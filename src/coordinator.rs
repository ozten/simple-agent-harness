/// Multi-agent coordinator: assigns beads to idle workers and manages their lifecycle.
///
/// When `workers.max > 1`, the coordinator replaces the serial runner loop.
/// It reads ready beads, uses the scheduler to find non-conflicting assignments,
/// spawns workers in git worktrees, and polls for completions.
/// Completed workers are queued for sequential integration into main.
use crate::config::HarnessConfig;
use crate::data_dir::DataDir;
use crate::db;
use crate::integrator::{CircuitBreaker, IntegrationQueue, TrippedFailure};
use crate::pool::{PoolError, WorkerPool};
use crate::scheduler::{self, InProgressAssignment, ReadyBead};
use crate::signals::SignalHandler;
use rusqlite::Connection;
use std::path::PathBuf;
use tokio::time::Duration;

/// Well-known filename that agents write to request affected set expansion.
const EXPAND_FILE_NAME: &str = ".blacksmith-expand";

/// Summary of a coordinator run, analogous to runner::RunSummary.
#[derive(Debug)]
pub struct CoordinatorSummary {
    /// Number of beads that were assigned and completed successfully.
    pub completed_beads: u32,
    /// Number of beads that were assigned but failed.
    pub failed_beads: u32,
    /// Why the coordinator stopped.
    pub exit_reason: CoordinatorExitReason,
}

/// Why the coordinator stopped.
#[derive(Debug, PartialEq)]
pub enum CoordinatorExitReason {
    /// No more ready beads to assign and no workers are active.
    NoWork,
    /// STOP file detected.
    StopFile,
    /// SIGINT or SIGTERM received.
    Signal,
    /// Fatal error (e.g., database failure).
    Error(String),
}

/// Run the multi-agent coordinator loop.
///
/// This is the entry point when `workers.max > 1`. It:
/// 1. Opens the metrics database
/// 2. Creates the worker pool
/// 3. Loops: schedule ready beads → spawn workers → poll completions → repeat
/// 4. Exits when no work remains or shutdown is requested
pub async fn run(
    config: &HarnessConfig,
    data_dir: &DataDir,
    signals: &SignalHandler,
    _quiet: bool,
) -> CoordinatorSummary {
    // Open metrics DB
    let db_conn = match db::open_or_create(&data_dir.db()) {
        Ok(conn) => conn,
        Err(e) => {
            tracing::error!(error = %e, "failed to open database for coordinator");
            return CoordinatorSummary {
                completed_beads: 0,
                failed_beads: 0,
                exit_reason: CoordinatorExitReason::Error(e.to_string()),
            };
        }
    };

    let repo_dir = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let worktrees_dir = data_dir.root().join(&config.workers.worktrees_dir);

    // Ensure worktrees directory exists
    if let Err(e) = std::fs::create_dir_all(&worktrees_dir) {
        tracing::error!(error = %e, "failed to create worktrees directory");
        return CoordinatorSummary {
            completed_beads: 0,
            failed_beads: 0,
            exit_reason: CoordinatorExitReason::Error(e.to_string()),
        };
    }

    let mut pool = WorkerPool::new(&config.workers, repo_dir.clone(), worktrees_dir);
    let output_dir = data_dir.sessions_dir();
    let integration_queue = IntegrationQueue::new(repo_dir, config.workers.base_branch.clone());
    let mut circuit_breaker = CircuitBreaker::new();

    // Ensure output directory exists
    if let Err(e) = std::fs::create_dir_all(&output_dir) {
        tracing::error!(error = %e, "failed to create sessions directory");
        return CoordinatorSummary {
            completed_beads: 0,
            failed_beads: 0,
            exit_reason: CoordinatorExitReason::Error(e.to_string()),
        };
    }

    let mut completed_beads = 0u32;
    let mut failed_beads = 0u32;
    let mut consecutive_no_work = 0u32;
    const MAX_CONSECUTIVE_NO_WORK: u32 = 3;

    tracing::info!(
        max_workers = config.workers.max,
        "coordinator starting multi-agent mode"
    );

    loop {
        // Check for shutdown signals
        if signals.shutdown_requested() {
            tracing::info!("shutdown requested, stopping coordinator");
            return CoordinatorSummary {
                completed_beads,
                failed_beads,
                exit_reason: CoordinatorExitReason::Signal,
            };
        }

        if signals
            .check_stop_file(&config.shutdown.stop_file)
            .is_detected()
        {
            tracing::info!("STOP file detected, stopping coordinator");
            return CoordinatorSummary {
                completed_beads,
                failed_beads,
                exit_reason: CoordinatorExitReason::StopFile,
            };
        }

        // Poll for completed workers
        let outcomes = pool.poll_completed().await;
        for outcome in &outcomes {
            if let Err(e) = pool.record_outcome(outcome, &db_conn) {
                tracing::warn!(error = %e, worker_id = outcome.worker_id, "failed to record outcome");
            }

            let succeeded = outcome.exit_code == Some(0);
            if succeeded {
                tracing::info!(
                    worker_id = outcome.worker_id,
                    "bead coding completed, queued for integration"
                );
                // Worker state is now Completed — will be picked up for integration below
            } else {
                failed_beads += 1;
                tracing::warn!(
                    worker_id = outcome.worker_id,
                    exit_code = ?outcome.exit_code,
                    "bead failed"
                );
                // Reset failed workers back to idle immediately
                if let Err(e) = pool.reset_worker(outcome.worker_id) {
                    tracing::warn!(error = %e, worker_id = outcome.worker_id, "failed to reset worker");
                }
            }
        }

        // Integration: process one completed worker at a time (sequential)
        // Only integrate if no other worker is currently integrating
        if !pool.has_integrating() {
            if let Some((worker_id, assignment_id, worktree_path, bead_id)) = pool.next_completed()
            {
                // Mark the worker as integrating
                pool.set_integrating(worker_id);

                tracing::info!(worker_id, bead_id = %bead_id, "starting integration");

                let resolved_integration = config.agent.resolved_integration();
                let result = integration_queue.integrate(
                    worker_id,
                    assignment_id,
                    &bead_id,
                    &worktree_path,
                    &db_conn,
                    Some(&resolved_integration),
                    &mut circuit_breaker,
                );

                if result.success {
                    completed_beads += 1;
                    tracing::info!(
                        worker_id,
                        bead_id = %bead_id,
                        commit = ?result.merge_commit,
                        "integration succeeded"
                    );
                    // Reset the worker back to idle after successful integration
                    if let Err(e) = pool.reset_worker(worker_id) {
                        tracing::warn!(error = %e, worker_id, "failed to reset worker after integration");
                    }
                } else {
                    let error_summary = result.failure_reason.as_deref().unwrap_or("unknown error");

                    // Check if the circuit breaker has tripped (integrate() already recorded attempts)
                    if let Some(tripped) =
                        circuit_breaker.check_tripped(&bead_id, error_summary, &worktree_path)
                    {
                        // Circuit breaker tripped — escalate to human review
                        failed_beads += 1;
                        handle_tripped_failure(&tripped);
                        // Do NOT reset the worker — worktree is preserved for inspection
                        // Do NOT clean up the worktree
                    } else {
                        tracing::warn!(
                            worker_id,
                            bead_id = %bead_id,
                            reason = ?result.failure_reason,
                            state = %circuit_breaker.state(&bead_id),
                            "integration failed, retries remain"
                        );
                        // Reset the worker back to idle so it can retry
                        if let Err(e) = pool.reset_worker(worker_id) {
                            tracing::warn!(error = %e, worker_id, "failed to reset worker after integration failure");
                        }
                    }
                }
            }
        }

        // Check for .blacksmith-expand files in worker worktrees
        check_and_process_expand_files(&pool, &db_conn);

        // If we have idle workers, try to schedule work
        if pool.idle_count() > 0 {
            // Gather in-progress assignments for the scheduler
            let in_progress = build_in_progress_list(&pool, &db_conn);

            // Query ready beads (stubbed — reads from `bd ready` equivalent)
            let ready_beads = query_ready_beads();

            if ready_beads.is_empty() && pool.active_count() == 0 {
                consecutive_no_work += 1;
                if consecutive_no_work >= MAX_CONSECUTIVE_NO_WORK {
                    tracing::info!("no work available and no active workers, exiting");
                    return CoordinatorSummary {
                        completed_beads,
                        failed_beads,
                        exit_reason: CoordinatorExitReason::NoWork,
                    };
                }
            } else {
                consecutive_no_work = 0;
            }

            // Schedule assignments for idle workers
            let assignable = scheduler::next_assignable_tasks(&ready_beads, &in_progress);

            for bead_id in assignable.iter().take(pool.idle_count() as usize) {
                // Find the bead to get its info for prompting
                let bead = ready_beads.iter().find(|b| b.id == *bead_id);
                let prompt = match bead {
                    Some(b) => format!("Work on bead: {}", b.id),
                    None => continue,
                };

                let resolved_agent = config.agent.resolved_coding();
                match pool
                    .spawn_worker(bead_id, &resolved_agent, &prompt, &output_dir, &db_conn)
                    .await
                {
                    Ok((worker_id, assignment_id)) => {
                        tracing::info!(
                            worker_id,
                            assignment_id,
                            bead_id,
                            "assigned bead to worker"
                        );
                    }
                    Err(PoolError::NoIdleWorker) => break,
                    Err(e) => {
                        tracing::error!(error = %e, bead_id, "failed to spawn worker for bead");
                    }
                }
            }
        }

        // Sleep before next poll cycle
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// Build the list of in-progress assignments from the worker pool's current state.
///
/// Reads affected_globs from the database for each coding worker so the scheduler
/// can make accurate conflict decisions (including dynamically expanded sets).
fn build_in_progress_list(pool: &WorkerPool, db_conn: &Connection) -> Vec<InProgressAssignment> {
    pool.snapshot()
        .iter()
        .filter(|(_, state, bead_id)| {
            *state == crate::pool::WorkerState::Coding && bead_id.is_some()
        })
        .map(|(worker_id, _, bead_id)| {
            let bead_id_str = bead_id.unwrap().to_string();

            // Look up the assignment's affected_globs from the DB
            let affected_globs = lookup_affected_globs_for_worker(pool, *worker_id, db_conn);

            InProgressAssignment {
                bead_id: bead_id_str,
                affected_globs,
            }
        })
        .collect()
}

/// Look up affected_globs from the DB for a worker's current assignment.
fn lookup_affected_globs_for_worker(
    pool: &WorkerPool,
    worker_id: u32,
    db_conn: &Connection,
) -> Option<Vec<String>> {
    let assignment_id = pool.worker_assignment_id(worker_id)?;
    let wa = db::get_worker_assignment(db_conn, assignment_id).ok()??;
    let globs_str = wa.affected_globs?;
    Some(parse_comma_separated_globs(&globs_str))
}

/// Parse a comma-separated globs string into a vector.
fn parse_comma_separated_globs(s: &str) -> Vec<String> {
    s.split(',')
        .map(|g| g.trim().to_string())
        .filter(|g| !g.is_empty())
        .collect()
}

/// Check all coding workers for `.blacksmith-expand` files and process them.
///
/// When a `.blacksmith-expand` file is found in a worker's worktree:
/// 1. Read the new file globs from it (one per line)
/// 2. Merge them with the assignment's existing affected_globs in the DB
/// 3. Delete the file so it's not processed again
///
/// Per the spec, expansion is always granted (optimistic concurrency).
/// Conflicts with other in-progress tasks are resolved at integration time.
fn check_and_process_expand_files(pool: &WorkerPool, db_conn: &Connection) {
    for (worker_id, state, _bead_id) in pool.snapshot() {
        if state != crate::pool::WorkerState::Coding {
            continue;
        }

        let worktree_path = match pool.worker_worktree_path(worker_id) {
            Some(p) => p,
            None => continue,
        };

        let expand_path = worktree_path.join(EXPAND_FILE_NAME);
        if !expand_path.exists() {
            continue;
        }

        // Read the expand file
        let contents = match std::fs::read_to_string(&expand_path) {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(
                    worker_id,
                    path = %expand_path.display(),
                    error = %e,
                    "failed to read .blacksmith-expand file"
                );
                continue;
            }
        };

        // Parse new globs (one per line, ignoring empty lines and comments)
        let new_globs: Vec<String> = contents
            .lines()
            .map(|l| l.trim().to_string())
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .collect();

        if new_globs.is_empty() {
            // Empty expand file — just delete it
            let _ = std::fs::remove_file(&expand_path);
            continue;
        }

        // Get the assignment ID for this worker
        let assignment_id = match pool.worker_assignment_id(worker_id) {
            Some(id) => id,
            None => {
                tracing::warn!(worker_id, "expand file found but no assignment ID");
                let _ = std::fs::remove_file(&expand_path);
                continue;
            }
        };

        // Get existing affected_globs from DB and merge
        let existing = db::get_worker_assignment(db_conn, assignment_id)
            .ok()
            .flatten()
            .and_then(|wa| wa.affected_globs)
            .map(|g| parse_comma_separated_globs(&g))
            .unwrap_or_default();

        let mut merged = existing;
        for g in &new_globs {
            if !merged.contains(g) {
                merged.push(g.clone());
            }
        }

        let merged_str = merged.join(", ");

        // Update the DB
        match db::update_worker_assignment_affected_globs(db_conn, assignment_id, &merged_str) {
            Ok(true) => {
                tracing::info!(
                    worker_id,
                    assignment_id,
                    new_globs = ?new_globs,
                    merged = %merged_str,
                    "expanded affected set for worker"
                );
            }
            Ok(false) => {
                tracing::warn!(
                    worker_id,
                    assignment_id,
                    "affected set expansion: assignment not found in DB"
                );
            }
            Err(e) => {
                tracing::error!(
                    worker_id,
                    assignment_id,
                    error = %e,
                    "failed to update affected set in DB"
                );
            }
        }

        // Delete the expand file so it's not processed again
        if let Err(e) = std::fs::remove_file(&expand_path) {
            tracing::warn!(
                worker_id,
                path = %expand_path.display(),
                error = %e,
                "failed to delete .blacksmith-expand file after processing"
            );
        }
    }
}

/// Query ready beads from the beads system.
///
/// This is a stub that returns an empty list. The full implementation will
/// shell out to `bd ready --json` or read from the beads database directly.
/// This is sufficient for wiring — the coordinator loop and scheduling logic
/// are exercised, and the actual bead query will be implemented in a follow-up task.
fn query_ready_beads() -> Vec<ReadyBead> {
    // Stub: try running `bd ready --format=json` and parsing the output.
    // For now, return empty — the coordinator will exit with NoWork.
    // This keeps the wiring testable without requiring a beads database.
    match std::process::Command::new("bd")
        .args(["list", "--status=open", "--format=json"])
        .output()
    {
        Ok(output) if output.status.success() => {
            parse_ready_beads_json(&String::from_utf8_lossy(&output.stdout))
        }
        _ => {
            tracing::debug!("bd command not available or failed, no beads to schedule");
            Vec::new()
        }
    }
}

/// Parse the JSON output from `bd list --status=open --format=json` into ReadyBead structs.
fn parse_ready_beads_json(json_str: &str) -> Vec<ReadyBead> {
    // Parse as JSON array of objects with id, priority, design fields
    let parsed: Result<Vec<serde_json::Value>, _> = serde_json::from_str(json_str);
    match parsed {
        Ok(beads) => beads
            .iter()
            .filter_map(|b| {
                let id = b.get("id")?.as_str()?.to_string();
                let priority = b.get("priority").and_then(|p| p.as_u64()).unwrap_or(2) as u32;
                let design = b.get("design").and_then(|d| d.as_str()).unwrap_or("");
                let affected_globs = scheduler::parse_affected_set(design);

                Some(ReadyBead {
                    id,
                    priority,
                    affected_globs,
                })
            })
            .collect(),
        Err(e) => {
            tracing::debug!(error = %e, "failed to parse bd output as JSON");
            Vec::new()
        }
    }
}

/// Handle a tripped circuit breaker by escalating to human review.
///
/// Per the spec (SPEC-v3-agents.md lines 626-644):
/// 1. Update bead with failure notes
/// 2. Label for human review
/// 3. Worktree is preserved (caller must NOT clean up)
/// 4. Display prominent HUMAN REVIEW NEEDED message
fn handle_tripped_failure(tripped: &TrippedFailure) {
    // Print the red/bold HUMAN REVIEW NEEDED message to stderr
    eprintln!("{}", tripped.display_message());

    tracing::error!(
        bead_id = %tripped.bead_id,
        attempts = tripped.attempts,
        error = %tripped.error_summary,
        worktree = %tripped.worktree_path.display(),
        "circuit breaker tripped — human review needed"
    );

    // Update the bead with failure notes via `bd update`
    let notes = tripped.failure_notes();
    match std::process::Command::new("bd")
        .args([
            "update",
            &tripped.bead_id,
            "--status=needs_review",
            &format!("--notes={notes}"),
        ])
        .output()
    {
        Ok(output) if output.status.success() => {
            tracing::info!(bead_id = %tripped.bead_id, "bead updated with failure notes and needs_review status");
        }
        Ok(output) => {
            let stderr = String::from_utf8_lossy(&output.stderr);
            tracing::warn!(
                bead_id = %tripped.bead_id,
                stderr = %stderr.trim(),
                "failed to update bead via bd command"
            );
        }
        Err(e) => {
            tracing::warn!(
                bead_id = %tripped.bead_id,
                error = %e,
                "failed to run bd update command"
            );
        }
    }
}

/// Extension trait for StopFileStatus (same as in runner.rs).
trait StopFileStatusExt {
    fn is_detected(&self) -> bool;
}

impl StopFileStatusExt for crate::signals::StopFileStatus {
    fn is_detected(&self) -> bool {
        *self == crate::signals::StopFileStatus::Detected
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::*;
    use crate::data_dir::DataDir;
    use crate::signals::SignalHandler;
    use tempfile::tempdir;

    fn test_config(dir: &std::path::Path) -> HarnessConfig {
        HarnessConfig {
            session: SessionConfig {
                max_iterations: 3,
                prompt_file: dir.join("prompt.md"),
                output_dir: dir.to_path_buf(),
                output_prefix: "test-iter".to_string(),
                counter_file: dir.join(".counter"),
            },
            agent: AgentConfig {
                command: "echo".to_string(),
                args: vec!["hello from worker".to_string()],
                ..Default::default()
            },
            watchdog: WatchdogConfig {
                check_interval_secs: 60,
                stale_timeout_mins: 20,
                min_output_bytes: 1,
            },
            retry: RetryConfig {
                max_empty_retries: 1,
                retry_delay_secs: 0,
            },
            backoff: BackoffConfig {
                initial_delay_secs: 0,
                max_delay_secs: 10,
                max_consecutive_rate_limits: 3,
            },
            shutdown: ShutdownConfig {
                stop_file: dir.join("STOP"),
            },
            hooks: HooksConfig::default(),
            prompt: PromptConfig::default(),
            output: OutputConfig::default(),
            commit_detection: CommitDetectionConfig::default(),
            metrics: MetricsConfig::default(),
            storage: StorageConfig::default(),
            workers: WorkersConfig {
                max: 3,
                base_branch: "main".to_string(),
                worktrees_dir: "worktrees".to_string(),
            },
        }
    }

    fn test_data_dir(dir: &std::path::Path) -> DataDir {
        let dd = DataDir::new(dir.join(".blacksmith"));
        dd.init().unwrap();
        dd
    }

    #[test]
    fn test_parse_ready_beads_json_valid() {
        let json = r#"[
            {"id": "beads-abc", "priority": 1, "design": "affected: src/db.rs"},
            {"id": "beads-def", "priority": 2, "design": "Some description\naffected: tests/**"}
        ]"#;
        let beads = parse_ready_beads_json(json);
        assert_eq!(beads.len(), 2);
        assert_eq!(beads[0].id, "beads-abc");
        assert_eq!(beads[0].priority, 1);
        assert_eq!(beads[0].affected_globs, Some(vec!["src/db.rs".to_string()]));
        assert_eq!(beads[1].id, "beads-def");
        assert_eq!(beads[1].priority, 2);
        assert_eq!(beads[1].affected_globs, Some(vec!["tests/**".to_string()]));
    }

    #[test]
    fn test_parse_ready_beads_json_empty() {
        let beads = parse_ready_beads_json("[]");
        assert!(beads.is_empty());
    }

    #[test]
    fn test_parse_ready_beads_json_invalid() {
        let beads = parse_ready_beads_json("not json");
        assert!(beads.is_empty());
    }

    #[test]
    fn test_parse_ready_beads_json_missing_fields() {
        // Missing "id" field should be skipped
        let json = r#"[{"priority": 1}]"#;
        let beads = parse_ready_beads_json(json);
        assert!(beads.is_empty());
    }

    #[test]
    fn test_parse_ready_beads_json_no_design() {
        let json = r#"[{"id": "beads-abc", "priority": 2}]"#;
        let beads = parse_ready_beads_json(json);
        assert_eq!(beads.len(), 1);
        assert_eq!(beads[0].affected_globs, None); // no design = affects everything
    }

    #[test]
    fn test_build_in_progress_list_empty_pool() {
        let dir = tempdir().unwrap();
        let wt_dir = dir.path().join("worktrees");
        let config = WorkersConfig {
            max: 2,
            base_branch: "main".to_string(),
            worktrees_dir: "worktrees".to_string(),
        };
        let pool = WorkerPool::new(&config, dir.path().to_path_buf(), wt_dir);
        let db_path = dir.path().join("test.db");
        let db_conn = db::open_or_create(&db_path).unwrap();
        let in_progress = build_in_progress_list(&pool, &db_conn);
        assert!(in_progress.is_empty());
    }

    #[tokio::test]
    async fn test_coordinator_exits_with_no_work() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());
        let data_dir = test_data_dir(dir.path());
        let signals = SignalHandler::install();

        std::fs::write(&config.session.prompt_file, "test prompt").unwrap();

        let summary = run(&config, &data_dir, &signals, false).await;

        // With no beads available (bd command not present in test), should exit with NoWork
        assert_eq!(summary.exit_reason, CoordinatorExitReason::NoWork);
        assert_eq!(summary.completed_beads, 0);
        assert_eq!(summary.failed_beads, 0);
    }

    #[tokio::test]
    async fn test_coordinator_exits_on_signal() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());
        let data_dir = test_data_dir(dir.path());
        let signals = SignalHandler::install();

        // Request shutdown before starting
        signals.request_shutdown();

        let summary = run(&config, &data_dir, &signals, false).await;

        assert_eq!(summary.exit_reason, CoordinatorExitReason::Signal);
    }

    #[tokio::test]
    async fn test_coordinator_exits_on_stop_file() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());
        let data_dir = test_data_dir(dir.path());
        let signals = SignalHandler::install();

        // Create STOP file
        std::fs::write(&config.shutdown.stop_file, "").unwrap();

        let summary = run(&config, &data_dir, &signals, false).await;

        assert_eq!(summary.exit_reason, CoordinatorExitReason::StopFile);
    }

    #[test]
    fn test_handle_tripped_failure_does_not_panic() {
        // Verify handle_tripped_failure doesn't panic even when bd is unavailable
        let tripped = TrippedFailure {
            bead_id: "beads-test-abc".to_string(),
            error_summary: "type mismatch in src/foo.rs:10".to_string(),
            worktree_path: std::path::PathBuf::from("/tmp/nonexistent-worktree"),
            attempts: 3,
        };
        // Should not panic — bd command will fail gracefully
        handle_tripped_failure(&tripped);
    }

    #[test]
    fn test_circuit_breaker_integration_with_coordinator_logic() {
        use crate::integrator::{CircuitBreaker, CircuitState, MAX_INTEGRATION_ATTEMPTS};

        let mut cb = CircuitBreaker::new();
        let bead_id = "beads-xyz";
        let worktree_path = std::path::Path::new("/tmp/wt-0");

        // Simulate integration failures up to max attempts
        for i in 1..=MAX_INTEGRATION_ATTEMPTS {
            cb.record_attempt(bead_id);

            if i < MAX_INTEGRATION_ATTEMPTS {
                // Not yet tripped
                assert!(cb.check_tripped(bead_id, "error", worktree_path).is_none());
                assert!(cb.state(bead_id).can_retry());
            } else {
                // Should be tripped now
                let tripped = cb.check_tripped(bead_id, "final error", worktree_path);
                assert!(tripped.is_some());
                let t = tripped.unwrap();
                assert_eq!(t.bead_id, bead_id);
                assert_eq!(t.attempts, MAX_INTEGRATION_ATTEMPTS);
                assert!(!cb.state(bead_id).can_retry());
            }
        }

        // After reset, should be back to closed
        cb.reset(bead_id);
        assert_eq!(cb.state(bead_id), CircuitState::Closed);
        assert!(cb.check_tripped(bead_id, "error", worktree_path).is_none());
    }

    #[test]
    fn test_parse_comma_separated_globs() {
        assert_eq!(
            parse_comma_separated_globs("src/db.rs, src/config.rs"),
            vec!["src/db.rs", "src/config.rs"]
        );
        assert_eq!(parse_comma_separated_globs("src/db.rs"), vec!["src/db.rs"]);
        assert!(parse_comma_separated_globs("").is_empty());
        assert_eq!(
            parse_comma_separated_globs("  src/a.rs ,  src/b.rs  "),
            vec!["src/a.rs", "src/b.rs"]
        );
    }

    #[test]
    fn test_expand_file_processing() {
        use std::process::Command as StdCommand;
        use std::process::Stdio;

        // Set up a git repo with a worktree (needed for pool)
        let dir = tempdir().unwrap();
        let repo = dir.path();

        StdCommand::new("git")
            .args(["init"])
            .current_dir(repo)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();
        StdCommand::new("git")
            .args(["config", "user.email", "test@test.com"])
            .current_dir(repo)
            .status()
            .unwrap();
        StdCommand::new("git")
            .args(["config", "user.name", "Test"])
            .current_dir(repo)
            .status()
            .unwrap();
        std::fs::write(repo.join("README.md"), "test").unwrap();
        StdCommand::new("git")
            .args(["add", "."])
            .current_dir(repo)
            .status()
            .unwrap();
        StdCommand::new("git")
            .args(["commit", "-m", "init"])
            .current_dir(repo)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();
        StdCommand::new("git")
            .args(["branch", "-M", "main"])
            .current_dir(repo)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();

        // Create a fake worktree directory (simulate a worker's worktree)
        let wt_dir = repo.join("worktrees");
        std::fs::create_dir_all(&wt_dir).unwrap();
        let fake_worktree = wt_dir.join("worker-0");
        std::fs::create_dir_all(&fake_worktree).unwrap();

        // Set up DB
        let db_path = repo.join("test.db");
        let db_conn = db::open_or_create(&db_path).unwrap();

        // Insert a worker assignment with initial affected_globs
        let assignment_id = db::insert_worker_assignment(
            &db_conn,
            0,
            "beads-expand-test",
            &fake_worktree.to_string_lossy(),
            "coding",
            Some("src/db.rs"),
        )
        .unwrap();

        // Create a pool and manually set up a worker in Coding state
        let workers_config = WorkersConfig {
            max: 1,
            base_branch: "main".to_string(),
            worktrees_dir: "worktrees".to_string(),
        };
        let mut pool = WorkerPool::new(&workers_config, repo.to_path_buf(), wt_dir);

        // Use set_worker_for_test to set up the coding state
        pool.set_worker_state_for_test(
            0,
            crate::pool::WorkerState::Coding,
            Some(assignment_id),
            Some("beads-expand-test".to_string()),
            Some(fake_worktree.clone()),
        );

        // Write a .blacksmith-expand file
        std::fs::write(
            fake_worktree.join(EXPAND_FILE_NAME),
            "src/config.rs\nsrc/signals.rs\n",
        )
        .unwrap();

        // Process expand files
        check_and_process_expand_files(&pool, &db_conn);

        // Verify the affected_globs were updated in DB
        let wa = db::get_worker_assignment(&db_conn, assignment_id)
            .unwrap()
            .unwrap();
        let globs = wa.affected_globs.unwrap();
        assert!(globs.contains("src/db.rs"));
        assert!(globs.contains("src/config.rs"));
        assert!(globs.contains("src/signals.rs"));

        // Verify the expand file was deleted
        assert!(!fake_worktree.join(EXPAND_FILE_NAME).exists());
    }

    #[test]
    fn test_expand_file_no_duplicates() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db_conn = db::open_or_create(&db_path).unwrap();

        let fake_worktree = dir.path().join("worker-0");
        std::fs::create_dir_all(&fake_worktree).unwrap();

        let assignment_id = db::insert_worker_assignment(
            &db_conn,
            0,
            "beads-nodup",
            &fake_worktree.to_string_lossy(),
            "coding",
            Some("src/db.rs, src/config.rs"),
        )
        .unwrap();

        let workers_config = WorkersConfig {
            max: 1,
            base_branch: "main".to_string(),
            worktrees_dir: "worktrees".to_string(),
        };
        let mut pool = WorkerPool::new(
            &workers_config,
            dir.path().to_path_buf(),
            dir.path().join("wt"),
        );
        pool.set_worker_state_for_test(
            0,
            crate::pool::WorkerState::Coding,
            Some(assignment_id),
            Some("beads-nodup".to_string()),
            Some(fake_worktree.clone()),
        );

        // Expand file includes one existing and one new glob
        std::fs::write(
            fake_worktree.join(EXPAND_FILE_NAME),
            "src/db.rs\nsrc/new.rs\n",
        )
        .unwrap();

        check_and_process_expand_files(&pool, &db_conn);

        let wa = db::get_worker_assignment(&db_conn, assignment_id)
            .unwrap()
            .unwrap();
        let globs_str = wa.affected_globs.unwrap();
        let globs = parse_comma_separated_globs(&globs_str);

        // src/db.rs should appear only once
        assert_eq!(globs.iter().filter(|g| *g == "src/db.rs").count(), 1);
        assert!(globs.contains(&"src/config.rs".to_string()));
        assert!(globs.contains(&"src/new.rs".to_string()));
    }

    #[test]
    fn test_expand_file_empty_file_ignored() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db_conn = db::open_or_create(&db_path).unwrap();

        let fake_worktree = dir.path().join("worker-0");
        std::fs::create_dir_all(&fake_worktree).unwrap();

        let assignment_id = db::insert_worker_assignment(
            &db_conn,
            0,
            "beads-empty",
            &fake_worktree.to_string_lossy(),
            "coding",
            Some("src/db.rs"),
        )
        .unwrap();

        let workers_config = WorkersConfig {
            max: 1,
            base_branch: "main".to_string(),
            worktrees_dir: "worktrees".to_string(),
        };
        let mut pool = WorkerPool::new(
            &workers_config,
            dir.path().to_path_buf(),
            dir.path().join("wt"),
        );
        pool.set_worker_state_for_test(
            0,
            crate::pool::WorkerState::Coding,
            Some(assignment_id),
            Some("beads-empty".to_string()),
            Some(fake_worktree.clone()),
        );

        // Empty expand file
        std::fs::write(fake_worktree.join(EXPAND_FILE_NAME), "\n\n").unwrap();

        check_and_process_expand_files(&pool, &db_conn);

        // affected_globs should be unchanged
        let wa = db::get_worker_assignment(&db_conn, assignment_id)
            .unwrap()
            .unwrap();
        assert_eq!(wa.affected_globs, Some("src/db.rs".to_string()));

        // But expand file should still be deleted
        assert!(!fake_worktree.join(EXPAND_FILE_NAME).exists());
    }

    #[test]
    fn test_expand_file_comments_ignored() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db_conn = db::open_or_create(&db_path).unwrap();

        let fake_worktree = dir.path().join("worker-0");
        std::fs::create_dir_all(&fake_worktree).unwrap();

        let assignment_id = db::insert_worker_assignment(
            &db_conn,
            0,
            "beads-comments",
            &fake_worktree.to_string_lossy(),
            "coding",
            None,
        )
        .unwrap();

        let workers_config = WorkersConfig {
            max: 1,
            base_branch: "main".to_string(),
            worktrees_dir: "worktrees".to_string(),
        };
        let mut pool = WorkerPool::new(
            &workers_config,
            dir.path().to_path_buf(),
            dir.path().join("wt"),
        );
        pool.set_worker_state_for_test(
            0,
            crate::pool::WorkerState::Coding,
            Some(assignment_id),
            Some("beads-comments".to_string()),
            Some(fake_worktree.clone()),
        );

        // Expand file with comments
        std::fs::write(
            fake_worktree.join(EXPAND_FILE_NAME),
            "# Need these files too\nsrc/config.rs\n# Also this\nsrc/signals.rs\n",
        )
        .unwrap();

        check_and_process_expand_files(&pool, &db_conn);

        let wa = db::get_worker_assignment(&db_conn, assignment_id)
            .unwrap()
            .unwrap();
        let globs = parse_comma_separated_globs(&wa.affected_globs.unwrap());
        assert_eq!(globs, vec!["src/config.rs", "src/signals.rs"]);
    }

    #[test]
    fn test_no_expand_file_is_noop() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db_conn = db::open_or_create(&db_path).unwrap();

        let fake_worktree = dir.path().join("worker-0");
        std::fs::create_dir_all(&fake_worktree).unwrap();

        let assignment_id = db::insert_worker_assignment(
            &db_conn,
            0,
            "beads-noop",
            &fake_worktree.to_string_lossy(),
            "coding",
            Some("src/db.rs"),
        )
        .unwrap();

        let workers_config = WorkersConfig {
            max: 1,
            base_branch: "main".to_string(),
            worktrees_dir: "worktrees".to_string(),
        };
        let mut pool = WorkerPool::new(
            &workers_config,
            dir.path().to_path_buf(),
            dir.path().join("wt"),
        );
        pool.set_worker_state_for_test(
            0,
            crate::pool::WorkerState::Coding,
            Some(assignment_id),
            Some("beads-noop".to_string()),
            Some(fake_worktree.clone()),
        );

        // No expand file — should be a no-op
        check_and_process_expand_files(&pool, &db_conn);

        let wa = db::get_worker_assignment(&db_conn, assignment_id)
            .unwrap()
            .unwrap();
        assert_eq!(wa.affected_globs, Some("src/db.rs".to_string()));
    }
}
