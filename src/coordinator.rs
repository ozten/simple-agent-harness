/// Multi-agent coordinator: assigns beads to idle workers and manages their lifecycle.
///
/// When `workers.max > 1`, the coordinator replaces the serial runner loop.
/// It reads ready beads, uses the scheduler to find non-conflicting assignments,
/// spawns workers in git worktrees, and polls for completions.
/// Completed workers are queued for sequential integration into main.
use crate::config::HarnessConfig;
use crate::data_dir::DataDir;
use crate::db;
use crate::integrator::IntegrationQueue;
use crate::pool::{PoolError, WorkerPool};
use crate::scheduler::{self, InProgressAssignment, ReadyBead};
use crate::signals::SignalHandler;
use std::path::PathBuf;
use tokio::time::Duration;

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

                let result = integration_queue.integrate(
                    worker_id,
                    assignment_id,
                    &bead_id,
                    &worktree_path,
                    &db_conn,
                );

                if result.success {
                    completed_beads += 1;
                    tracing::info!(
                        worker_id,
                        bead_id = %bead_id,
                        commit = ?result.merge_commit,
                        "integration succeeded"
                    );
                } else {
                    failed_beads += 1;
                    tracing::warn!(
                        worker_id,
                        bead_id = %bead_id,
                        reason = ?result.failure_reason,
                        "integration failed"
                    );
                }

                // Reset the worker back to idle after integration
                if let Err(e) = pool.reset_worker(worker_id) {
                    tracing::warn!(error = %e, worker_id, "failed to reset worker after integration");
                }
            }
        }

        // If we have idle workers, try to schedule work
        if pool.idle_count() > 0 {
            // Gather in-progress assignments for the scheduler
            let in_progress = build_in_progress_list(&pool);

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

                match pool
                    .spawn_worker(bead_id, &config.agent, &prompt, &output_dir, &db_conn)
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
fn build_in_progress_list(pool: &WorkerPool) -> Vec<InProgressAssignment> {
    pool.snapshot()
        .iter()
        .filter(|(_, state, bead_id)| {
            *state == crate::pool::WorkerState::Coding && bead_id.is_some()
        })
        .map(|(_, _, bead_id)| {
            // For now, we don't have the affected globs in the pool snapshot.
            // Treat all in-progress tasks as affecting everything (conservative).
            // Future: store affected_globs in the worker or look them up from DB.
            InProgressAssignment {
                bead_id: bead_id.unwrap().to_string(),
                affected_globs: None,
            }
        })
        .collect()
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
        let in_progress = build_in_progress_list(&pool);
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
}
