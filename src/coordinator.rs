/// Multi-agent coordinator: assigns beads to idle workers and manages their lifecycle.
///
/// The coordinator is the main execution path for all configurations (single or
/// multi-agent). It reads ready beads, uses the scheduler to find non-conflicting
/// assignments, spawns workers in git worktrees (skipped for max=1), and polls
/// for completions. Completed workers are queued for sequential integration into
/// main (also skipped for max=1).
use crate::adapters;
use crate::config::HarnessConfig;
use crate::cycle_detect;
use crate::data_dir::DataDir;
use crate::db;
use crate::estimation::{self, BeadNode};
use crate::improve;
use crate::ingest;
use crate::integrator::{CircuitBreaker, IntegrationQueue, TrippedFailure};
use crate::pool::{PoolError, SessionOutcome, WorkerPool};
use crate::prompt;
use crate::scheduler::{self, InProgressAssignment, ReadyBead};
use crate::signals::SignalHandler;
use crate::status::{HarnessState, StatusTracker};
use crate::worktree;
use rusqlite::Connection;
use std::collections::HashSet;
use std::path::PathBuf;
use tokio::time::Duration;

/// Well-known filename that agents write to request affected set expansion.
const EXPAND_FILE_NAME: &str = ".blacksmith-expand";

/// Summary of a coordinator run.
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

    // Load global iteration counter so worker output files use numeric naming
    let counter_path = data_dir.counter();
    let initial_session_id = load_counter(&counter_path);
    let mut pool = WorkerPool::new(
        &config.workers,
        repo_dir.clone(),
        worktrees_dir,
        initial_session_id,
    );
    let output_dir = data_dir.sessions_dir();
    let integration_queue =
        IntegrationQueue::new(repo_dir.clone(), config.workers.base_branch.clone());
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

    // StatusTracker: write state transitions so `--status` works
    let status_path = data_dir.status();
    let mut status = StatusTracker::new(status_path, 0, initial_session_id);
    status.update(HarnessState::Starting);

    // Compile metrics extraction rules once at startup (same as runner.rs)
    let extraction_rules: Vec<crate::config::CompiledRule> = config
        .metrics
        .extract
        .rules
        .iter()
        .filter_map(|r| match r.compile() {
            Ok(compiled) => Some(compiled),
            Err(e) => {
                tracing::warn!(error = %e, "invalid extraction rule, skipping");
                None
            }
        })
        .collect();

    // Create adapter for JSONL metric extraction
    let resolved_agent = config.agent.resolved_coding();
    let adapter_name =
        adapters::resolve_adapter_name(resolved_agent.adapter.as_deref(), &resolved_agent.command);
    let adapter = adapters::create_adapter(adapter_name);

    tracing::info!(
        max_workers = config.workers.max,
        adapter = adapter_name,
        "coordinator starting multi-agent mode"
    );

    // Recover orphaned in_progress beads from previous crash/kill.
    // Since the singleton lock guarantees no other coordinator is running,
    // any in_progress beads without an active worker are guaranteed orphaned.
    recover_orphaned_beads();

    // Clean up stale worktrees from previous crash/kill.
    // No workers are active yet, so every existing worktree is orphaned.
    match worktree::cleanup_orphans(&repo_dir, pool.worktrees_dir(), &[]) {
        Ok(cleaned) if !cleaned.is_empty() => {
            tracing::info!(
                count = cleaned.len(),
                "cleaned up stale worktrees from previous run"
            );
        }
        Err(e) => {
            tracing::warn!(error = %e, "failed to clean up stale worktrees");
        }
        _ => {}
    }

    loop {
        // Check for shutdown signals
        if signals.shutdown_requested() {
            tracing::info!("shutdown requested, stopping coordinator");
            status.update(HarnessState::ShuttingDown);
            status.remove();
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
            status.update(HarnessState::ShuttingDown);
            status.remove();
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

            // Ingest JSONL metrics from the worker's output file
            let ingest_result =
                ingest_worker_metrics(outcome, &db_conn, &extraction_rules, adapter.as_ref());

            let succeeded = outcome.exit_code == Some(0);
            if succeeded {
                tracing::info!(
                    worker_id = outcome.worker_id,
                    "bead coding completed, queued for integration"
                );
                // Worker state is now Completed — will be picked up for integration below
            } else {
                failed_beads += 1;
                // Print failure progress line
                print_coordinator_progress(
                    outcome,
                    false,
                    completed_beads,
                    failed_beads,
                    ingest_result.as_ref(),
                    &db_conn,
                    config.workers.max,
                );
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
                // In single-agent mode, the agent committed directly to the main branch.
                // Skip the integration merge — just close the bead and reset the worker.
                if pool.is_single_agent() {
                    pool.set_integrating(worker_id);

                    tracing::info!(
                        worker_id,
                        bead_id = %bead_id,
                        "single-agent mode: closing bead directly (no merge)"
                    );

                    close_bead_direct(&bead_id);

                    completed_beads += 1;
                    print_coordinator_integration_progress(
                        worker_id,
                        &bead_id,
                        completed_beads,
                        failed_beads,
                        &db_conn,
                        config.workers.max,
                    );

                    run_auto_promotion(config, &db_conn, &data_dir.db(), completed_beads);

                    if let Err(e) = pool.reset_worker(worker_id) {
                        tracing::warn!(error = %e, worker_id, "failed to reset worker after direct close");
                    }
                } else {
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

                        // Print progress using DB state (metrics already ingested during poll phase)
                        print_coordinator_integration_progress(
                            worker_id,
                            &bead_id,
                            completed_beads,
                            failed_beads,
                            &db_conn,
                            config.workers.max,
                        );

                        tracing::info!(
                            worker_id,
                            bead_id = %bead_id,
                            commit = ?result.merge_commit,
                            "integration succeeded"
                        );

                        // Run auto-promotion cycle after successful integration
                        run_auto_promotion(config, &db_conn, &data_dir.db(), completed_beads);

                        // Reset the worker back to idle after successful integration
                        if let Err(e) = pool.reset_worker(worker_id) {
                            tracing::warn!(error = %e, worker_id, "failed to reset worker after integration");
                        }
                    } else {
                        let error_summary =
                            result.failure_reason.as_deref().unwrap_or("unknown error");

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
        }

        // Check for .blacksmith-expand files in worker worktrees
        check_and_process_expand_files(&pool, &db_conn);

        // If we have idle workers, try to schedule work
        if pool.idle_count() > 0 {
            // Gather in-progress assignments for the scheduler
            let in_progress = build_in_progress_list(&pool, &db_conn);

            // Query beads, detect cycles, and filter out cycled beads
            let bead_query = query_ready_beads();
            let ready_beads = bead_query.ready;

            if ready_beads.is_empty() && pool.active_count() == 0 {
                consecutive_no_work += 1;
                if !config.workers.persistent && consecutive_no_work >= MAX_CONSECUTIVE_NO_WORK {
                    tracing::info!("no work available and no active workers, exiting");
                    status.update(HarnessState::ShuttingDown);
                    status.remove();
                    return CoordinatorSummary {
                        completed_beads,
                        failed_beads,
                        exit_reason: CoordinatorExitReason::NoWork,
                    };
                }
                if config.workers.persistent {
                    tracing::info!("no work available, persistent mode — sleeping before re-poll");
                }
            } else {
                consecutive_no_work = 0;
            }

            // Schedule assignments for idle workers
            let assignable = scheduler::next_assignable_tasks(&ready_beads, &in_progress);

            // Assemble the base prompt once (brief + improvements + PROMPT.md).
            // Each worker gets this base prompt with a bead-specific suffix.
            let base_prompt = assemble_base_prompt(config, data_dir);

            for bead_id in assignable.iter().take(pool.idle_count() as usize) {
                // Find the bead to get its info for prompting and affected set
                let bead = ready_beads.iter().find(|b| b.id == *bead_id);
                let prompt = match bead {
                    Some(b) => format!("{}\n\nWork on bead: {}", base_prompt, b.id),
                    None => continue,
                };

                // Serialize affected globs as comma-separated string for the DB
                let affected_globs_str =
                    bead.and_then(|b| b.affected_globs.as_ref().map(|globs| globs.join(", ")));

                let resolved_agent = config.agent.resolved_coding();
                match pool
                    .spawn_worker(
                        bead_id,
                        affected_globs_str.as_deref(),
                        &resolved_agent,
                        &prompt,
                        &output_dir,
                        &db_conn,
                    )
                    .await
                {
                    Ok((worker_id, assignment_id)) => {
                        // Persist session counter so other subsystems see
                        // the updated value if the coordinator crashes/restarts.
                        save_counter(&counter_path, pool.next_session_id());
                        status.set_iteration(completed_beads);
                        status.set_global_iteration(pool.next_session_id());
                        status.update(HarnessState::SessionRunning);
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

        // Update status between poll cycles
        if pool.active_count() == 0 {
            status.update(HarnessState::Idle);
        }

        // Sleep before next poll cycle
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// Assemble the base prompt with brief (performance feedback + improvements) and PROMPT.md.
///
/// Falls back to an empty string if prompt assembly fails (e.g., missing prompt file).
fn assemble_base_prompt(config: &HarnessConfig, data_dir: &DataDir) -> String {
    // Compile adapter to determine supported metrics
    let resolved_agent = config.agent.resolved_coding();
    let adapter_name =
        adapters::resolve_adapter_name(resolved_agent.adapter.as_deref(), &resolved_agent.command);
    let adapter = adapters::create_adapter(adapter_name);
    let supported = adapter.supported_metrics();
    let supported_opt = if supported.is_empty() {
        None
    } else {
        Some(supported)
    };
    let targets_opt = if config.metrics.targets.rules.is_empty() {
        None
    } else {
        Some(&config.metrics.targets)
    };

    match prompt::assemble(
        &config.prompt,
        &config.session.prompt_file,
        &data_dir.db(),
        targets_opt,
        supported_opt,
    ) {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(error = %e, "failed to assemble prompt, using empty base");
            String::new()
        }
    }
}

/// Run the self-improvement auto-promotion cycle after a successful integration.
///
/// Checks if any open improvements have been active for at least `auto_promote_after`
/// successful sessions. If so, promotes them and appends to the configured prompt file.
fn run_auto_promotion(
    config: &HarnessConfig,
    db_conn: &Connection,
    db_path: &std::path::Path,
    completed_beads: u32,
) {
    let auto_after = config.improvements.auto_promote_after;
    if auto_after == 0 {
        return; // auto-promotion disabled
    }

    // Only check every `auto_promote_after` completions to avoid DB churn
    if !completed_beads.is_multiple_of(auto_after) {
        return;
    }

    let open = match db::list_improvements(db_conn, Some("open"), None) {
        Ok(list) => list,
        Err(e) => {
            tracing::warn!(error = %e, "failed to list open improvements for auto-promotion");
            return;
        }
    };

    if open.is_empty() {
        return;
    }

    for imp in &open {
        // An improvement is eligible for promotion if there have been at least
        // `auto_promote_after` successful sessions since it was created.
        let sessions_since = sessions_since_improvement(db_conn, &imp.created);
        if sessions_since < auto_after as i64 {
            continue;
        }

        tracing::info!(
            ref_id = %imp.ref_id,
            title = %imp.title,
            sessions_since,
            threshold = auto_after,
            "auto-promoting improvement"
        );

        // Promote in DB
        if let Err(e) = improve::handle_promote(db_path, &imp.ref_id) {
            tracing::warn!(
                error = %e,
                ref_id = %imp.ref_id,
                "failed to auto-promote improvement"
            );
            continue;
        }

        // Append to PROMPT.md
        if let Err(e) = append_promoted_to_prompt(&config.improvements.prompt_file, imp) {
            tracing::warn!(
                error = %e,
                ref_id = %imp.ref_id,
                "failed to append promoted improvement to prompt file"
            );
        }
    }
}

/// Count sessions (observations) recorded after the given ISO timestamp.
fn sessions_since_improvement(db_conn: &Connection, created_at: &str) -> i64 {
    db_conn
        .query_row(
            "SELECT COUNT(*) FROM observations WHERE ts > ?1",
            rusqlite::params![created_at],
            |row| row.get::<_, i64>(0),
        )
        .unwrap_or_default()
}

/// Append a promoted improvement's rule to the prompt file.
fn append_promoted_to_prompt(
    prompt_path: &std::path::Path,
    imp: &db::Improvement,
) -> Result<(), String> {
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(prompt_path)
        .map_err(|e| format!("failed to open {}: {}", prompt_path.display(), e))?;

    let body_text = imp.body.as_deref().unwrap_or(&imp.title);
    write!(
        file,
        "\n\n<!-- Promoted from {} [{}] -->\n- {}",
        imp.ref_id, imp.category, body_text
    )
    .map_err(|e| format!("failed to write to {}: {}", prompt_path.display(), e))?;

    tracing::info!(
        ref_id = %imp.ref_id,
        prompt_file = %prompt_path.display(),
        "appended promoted improvement to prompt file"
    );
    Ok(())
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

/// Close a bead directly without a merge step (for single-agent mode).
///
/// Runs `bd close` and `bd sync` since the agent already committed to the main branch.
fn close_bead_direct(bead_id: &str) {
    // bd close
    match std::process::Command::new("bd")
        .args(["close", bead_id, "--reason=single-agent direct commit"])
        .output()
    {
        Ok(out) if out.status.success() => {
            tracing::info!(bead_id, "bd close succeeded");
        }
        Ok(out) => {
            let stderr = String::from_utf8_lossy(&out.stderr);
            tracing::warn!(bead_id, stderr = %stderr.trim(), "bd close failed");
        }
        Err(e) => {
            tracing::warn!(bead_id, error = %e, "failed to run bd close");
        }
    }

    // bd sync
    match std::process::Command::new("bd").args(["sync"]).output() {
        Ok(out) if out.status.success() => {
            tracing::info!("bd sync succeeded");
        }
        Ok(out) => {
            let stderr = String::from_utf8_lossy(&out.stderr);
            tracing::warn!(stderr = %stderr.trim(), "bd sync failed");
        }
        Err(e) => {
            tracing::warn!(error = %e, "failed to run bd sync");
        }
    }
}

/// Recover orphaned in_progress beads on coordinator startup.
///
/// When blacksmith crashes or is killed, beads marked in_progress stay stuck.
/// Since the singleton lock guarantees no other coordinator is running at this point,
/// any in_progress bead is guaranteed orphaned. This function queries for all
/// in_progress beads and resets them to open so they become schedulable again.
fn recover_orphaned_beads() {
    let output = match std::process::Command::new("bd")
        .args(["list", "--status=in_progress", "--json"])
        .output()
    {
        Ok(output) if output.status.success() => output,
        Ok(_) => {
            tracing::debug!("bd list --status=in_progress returned non-zero, skipping recovery");
            return;
        }
        Err(e) => {
            tracing::debug!(error = %e, "bd command not available, skipping orphaned bead recovery");
            return;
        }
    };

    let stdout = String::from_utf8_lossy(&output.stdout);
    let orphaned_ids = parse_orphaned_bead_ids(&stdout);

    if orphaned_ids.is_empty() {
        tracing::debug!("no orphaned in_progress beads to recover");
        return;
    }

    let mut recovered = 0u32;
    for id in &orphaned_ids {
        match std::process::Command::new("bd")
            .args(["update", id, "--status=open"])
            .output()
        {
            Ok(result) if result.status.success() => {
                recovered += 1;
                tracing::info!(bead_id = %id, "recovered orphaned in_progress bead → open");
            }
            Ok(result) => {
                let stderr = String::from_utf8_lossy(&result.stderr);
                tracing::warn!(
                    bead_id = %id,
                    stderr = %stderr.trim(),
                    "failed to recover orphaned bead"
                );
            }
            Err(e) => {
                tracing::warn!(bead_id = %id, error = %e, "failed to run bd update for orphaned bead");
            }
        }
    }

    if recovered > 0 {
        tracing::info!(count = recovered, "recovered orphaned in_progress beads");
    }
}

/// Parse bead IDs from JSON output of `bd list --status=in_progress --json`.
fn parse_orphaned_bead_ids(json_str: &str) -> Vec<String> {
    let parsed: Result<Vec<serde_json::Value>, _> = serde_json::from_str(json_str);
    match parsed {
        Ok(beads) => beads
            .iter()
            .filter_map(|b| b.get("id").and_then(|v| v.as_str()).map(|s| s.to_string()))
            .collect(),
        Err(e) => {
            tracing::debug!(error = %e, "failed to parse in_progress beads JSON");
            Vec::new()
        }
    }
}

/// Result of querying beads: ready beads for scheduling and the full graph for cycle detection.
struct BeadQuery {
    /// Beads available for scheduling (after filtering out cycled ones).
    ready: Vec<ReadyBead>,
    /// Detected dependency cycles (each cycle is a list of bead IDs).
    /// Used by CLI status output (simple-agent-harness-cqf) and in tests.
    #[allow(dead_code)]
    cycles: Vec<Vec<String>>,
}

/// Query beads from the beads system, detect cycles, and return schedulable beads.
///
/// Shells out to `bd list --status=open --json` to get all open beads with dependencies.
/// Runs cycle detection on every scheduling pass (cycles can be created or broken mid-run).
/// Cycled beads are filtered out of the scheduling pool.
fn query_ready_beads() -> BeadQuery {
    match std::process::Command::new("bd")
        .args(["list", "--status=open", "--json"])
        .output()
    {
        Ok(output) if output.status.success() => {
            parse_and_filter_beads(&String::from_utf8_lossy(&output.stdout))
        }
        _ => {
            tracing::debug!("bd command not available or failed, no beads to schedule");
            BeadQuery {
                ready: Vec::new(),
                cycles: Vec::new(),
            }
        }
    }
}

/// Parse JSON bead data, detect cycles, filter out cycled beads, and return schedulable beads.
fn parse_and_filter_beads(json_str: &str) -> BeadQuery {
    let (ready_beads, bead_nodes) = parse_ready_beads_json(json_str);

    // Run cycle detection on the dependency graph
    let cycles = cycle_detect::detect_cycles(&bead_nodes);

    // --- Cycle filtering ---
    let after_cycle: Vec<ReadyBead>;

    if !cycles.is_empty() {
        // Log each detected cycle
        for cycle in &cycles {
            let path = format_cycle_path(cycle);
            tracing::warn!(
                cycle_size = cycle.len(),
                "dependency cycle detected — beads excluded from scheduling: {path}"
            );
        }

        // Build set of all cycled bead IDs for efficient filtering
        let cycled_ids: HashSet<&str> = cycles
            .iter()
            .flat_map(|c| c.iter().map(|s| s.as_str()))
            .collect();

        after_cycle = ready_beads
            .into_iter()
            .filter(|b| !cycled_ids.contains(b.id.as_str()))
            .collect();

        let excluded_count = cycled_ids.len();
        tracing::warn!(
            excluded = excluded_count,
            cycles = cycles.len(),
            remaining = after_cycle.len(),
            "dependency cycle detected — {excluded_count} beads excluded from scheduling"
        );
    } else {
        after_cycle = ready_beads;
    }

    // --- Dependency filtering ---
    // A bead is truly ready only if none of its depends_on IDs are still open.
    let open_ids: HashSet<String> = after_cycle.iter().map(|b| b.id.clone()).collect();
    let deps_map: std::collections::HashMap<&str, &[String]> = bead_nodes
        .iter()
        .map(|n| (n.id.as_str(), n.depends_on.as_slice()))
        .collect();

    let before_dep_count = after_cycle.len();
    let truly_ready: Vec<ReadyBead> = after_cycle
        .into_iter()
        .filter(|b| {
            deps_map
                .get(b.id.as_str())
                .map(|deps| deps.iter().all(|d| !open_ids.contains(d)))
                .unwrap_or(true)
        })
        .collect();

    let blocked_count = before_dep_count - truly_ready.len();
    if blocked_count > 0 {
        tracing::info!(
            blocked = blocked_count,
            ready = truly_ready.len(),
            "filtered out {blocked_count} beads with unresolved dependencies"
        );
    }

    BeadQuery {
        ready: truly_ready,
        cycles,
    }
}

/// Format a cycle as a readable path: "A -> B -> C -> A"
fn format_cycle_path(cycle: &[String]) -> String {
    if cycle.is_empty() {
        return String::new();
    }
    let mut path = cycle.join(" -> ");
    path.push_str(" -> ");
    path.push_str(&cycle[0]);
    path
}

/// Parse the JSON output from `bd list --status=open --json` into ReadyBead and BeadNode structs.
///
/// Returns both the scheduling-ready beads and the dependency graph nodes for cycle detection.
fn parse_ready_beads_json(json_str: &str) -> (Vec<ReadyBead>, Vec<BeadNode>) {
    let parsed: Result<Vec<serde_json::Value>, _> = serde_json::from_str(json_str);
    match parsed {
        Ok(beads) => {
            let mut ready = Vec::new();
            let mut nodes = Vec::new();

            for b in &beads {
                let id = match b.get("id").and_then(|v| v.as_str()) {
                    Some(id) => id.to_string(),
                    None => continue,
                };
                let priority = b.get("priority").and_then(|p| p.as_u64()).unwrap_or(2) as u32;
                let design = b.get("design").and_then(|d| d.as_str()).unwrap_or("");
                let affected_globs = scheduler::parse_affected_set(design);

                // Parse dependencies for cycle detection
                let depends_on: Vec<String> = b
                    .get("dependencies")
                    .and_then(|d| d.as_array())
                    .map(|deps| {
                        deps.iter()
                            .filter_map(|dep| {
                                dep.get("depends_on_id")
                                    .and_then(|v| v.as_str())
                                    .map(|s| s.to_string())
                            })
                            .collect()
                    })
                    .unwrap_or_default();

                ready.push(ReadyBead {
                    id: id.clone(),
                    priority,
                    affected_globs,
                });

                nodes.push(BeadNode { id, depends_on });
            }

            (ready, nodes)
        }
        Err(e) => {
            tracing::debug!(error = %e, "failed to parse bd output as JSON");
            (Vec::new(), Vec::new())
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

/// Load the global iteration counter from a file. Returns 0 if not found.
fn load_counter(path: &std::path::Path) -> u64 {
    match std::fs::read_to_string(path) {
        Ok(contents) => contents.trim().parse().unwrap_or(0),
        Err(_) => 0,
    }
}

/// Save the global iteration counter to a file.
fn save_counter(path: &std::path::Path, value: u64) {
    if let Err(e) = std::fs::write(path, value.to_string()) {
        tracing::error!(error = %e, path = %path.display(), "failed to save iteration counter");
    }
}

/// Ingest JSONL metrics from a worker's output file into the metrics DB.
///
/// Called after each worker completion (success or failure), same as the serial runner does.
/// Returns the IngestResult for use in the progress line.
fn ingest_worker_metrics(
    outcome: &SessionOutcome,
    db_conn: &Connection,
    extraction_rules: &[crate::config::CompiledRule],
    adapter: &dyn crate::adapters::AgentAdapter,
) -> Option<ingest::IngestResult> {
    match ingest::ingest_session_with_rules(
        db_conn,
        outcome.session_id as i64,
        &outcome.output_file,
        outcome.exit_code,
        extraction_rules,
        adapter,
    ) {
        Ok(m) => {
            tracing::info!(
                worker_id = outcome.worker_id,
                session = outcome.session_id,
                turns = m.turns_total,
                cost_usd = format!("{:.4}", m.cost_estimate_usd),
                "JSONL metrics ingested"
            );
            Some(m)
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                worker_id = outcome.worker_id,
                session = outcome.session_id,
                "failed to ingest JSONL metrics"
            );
            None
        }
    }
}

/// Print a progress line after a worker coding failure.
///
/// Format: `[bead beads-abc] worker 2 FAILED in 5m | Progress: 8/15 beads | avg 5m/bead | ETA: ~42m @ 3 workers`
fn print_coordinator_progress(
    outcome: &SessionOutcome,
    _succeeded: bool,
    completed_beads: u32,
    failed_beads: u32,
    ingest_result: Option<&ingest::IngestResult>,
    db_conn: &Connection,
    workers: u32,
) {
    let duration_str = format_duration_secs(outcome.duration.as_secs());

    let turns_str = ingest_result
        .map(|m| format!("{} turns", m.turns_total))
        .unwrap_or_default();

    // Build the session summary part
    let session_part = if turns_str.is_empty() {
        format!(
            "[worker {}] FAILED in {} ({}c/{}f)",
            outcome.worker_id, duration_str, completed_beads, failed_beads
        )
    } else {
        format!(
            "[worker {}] FAILED in {} ({}) ({}c/{}f)",
            outcome.worker_id, duration_str, turns_str, completed_beads, failed_beads
        )
    };

    // Build the progress + ETA part from bead metrics
    let progress_part = build_progress_string(db_conn, workers).unwrap_or_default();

    if progress_part.is_empty() {
        println!("{}", session_part);
    } else {
        println!("{} | {}", session_part, progress_part);
    }
}

/// Print a progress line after successful integration.
///
/// Format: `[bead beads-abc] worker 2 integrated | Progress: 8/15 beads | avg 5m/bead | ETA: ~42m @ 3 workers`
fn print_coordinator_integration_progress(
    worker_id: u32,
    bead_id: &str,
    completed_beads: u32,
    failed_beads: u32,
    db_conn: &Connection,
    workers: u32,
) {
    let session_part = format!(
        "[bead {}] worker {} integrated ({}c/{}f)",
        bead_id, worker_id, completed_beads, failed_beads
    );

    // Build the progress + ETA part from bead metrics
    let progress_part = build_progress_string(db_conn, workers).unwrap_or_default();

    if progress_part.is_empty() {
        println!("{}", session_part);
    } else {
        println!("{} | {}", session_part, progress_part);
    }
}

/// Format seconds as a compact human-readable duration (e.g., "45s", "5m", "1h 30m").
fn format_duration_secs(secs: u64) -> String {
    if secs < 60 {
        format!("{}s", secs)
    } else if secs < 3600 {
        format!("{}m", secs / 60)
    } else {
        let hours = secs / 3600;
        let mins = (secs % 3600) / 60;
        if mins > 0 {
            format!("{}h {}m", hours, mins)
        } else {
            format!("{}h", hours)
        }
    }
}

/// Build the "Progress: A/B beads | avg Xm/bead | ETA: ~Zm" string from DB metrics.
fn build_progress_string(conn: &rusqlite::Connection, workers: u32) -> Option<String> {
    let all_metrics = db::all_bead_metrics(conn).ok()?;
    let completed = all_metrics
        .iter()
        .filter(|m| m.completed_at.is_some())
        .count();
    let total = all_metrics.len();

    if total == 0 {
        return None;
    }

    let open_beads = estimation::query_open_beads();
    let est = estimation::estimate(conn, &open_beads, workers);

    let mut parts = Vec::new();
    let schedulable_total = completed + est.open_count;
    if !est.cycled_beads.is_empty() {
        parts.push(format!(
            "Progress: {}/{} schedulable beads",
            completed, schedulable_total
        ));
    } else {
        parts.push(format!(
            "Progress: {}/{} beads",
            completed, schedulable_total
        ));
    }

    if let Some(avg) = est.avg_time_per_bead {
        parts.push(format!("avg {}/bead", format_duration_secs(avg as u64)));
    }

    if workers > 1 {
        if let Some(parallel) = est.parallel_secs {
            parts.push(format!(
                "ETA: ~{} @ {} workers",
                format_duration_secs(parallel as u64),
                workers
            ));
        }
    } else if let Some(serial) = est.serial_secs {
        parts.push(format!("ETA: ~{}", format_duration_secs(serial as u64)));
    }

    if !est.cycled_beads.is_empty() {
        parts.push(format!(
            "\u{26a0} {} beads in dependency cycle \u{2014} run `bd dep cycles` to fix",
            est.cycled_beads.len()
        ));
    }

    Some(parts.join(" | "))
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
                persistent: false,
            },
            reconciliation: ReconciliationConfig::default(),
            architecture: ArchitectureConfig::default(),
            quality_gates: QualityGatesConfig::default(),
            improvements: ImprovementsConfig::default(),
            serve: ServeConfig::default(),
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
        let (beads, nodes) = parse_ready_beads_json(json);
        assert_eq!(beads.len(), 2);
        assert_eq!(beads[0].id, "beads-abc");
        assert_eq!(beads[0].priority, 1);
        assert_eq!(beads[0].affected_globs, Some(vec!["src/db.rs".to_string()]));
        assert_eq!(beads[1].id, "beads-def");
        assert_eq!(beads[1].priority, 2);
        assert_eq!(beads[1].affected_globs, Some(vec!["tests/**".to_string()]));
        assert_eq!(nodes.len(), 2);
        assert!(nodes[0].depends_on.is_empty());
        assert!(nodes[1].depends_on.is_empty());
    }

    #[test]
    fn test_parse_ready_beads_json_empty() {
        let (beads, nodes) = parse_ready_beads_json("[]");
        assert!(beads.is_empty());
        assert!(nodes.is_empty());
    }

    #[test]
    fn test_parse_ready_beads_json_invalid() {
        let (beads, nodes) = parse_ready_beads_json("not json");
        assert!(beads.is_empty());
        assert!(nodes.is_empty());
    }

    #[test]
    fn test_parse_ready_beads_json_missing_fields() {
        // Missing "id" field should be skipped
        let json = r#"[{"priority": 1}]"#;
        let (beads, _) = parse_ready_beads_json(json);
        assert!(beads.is_empty());
    }

    #[test]
    fn test_parse_ready_beads_json_no_design() {
        let json = r#"[{"id": "beads-abc", "priority": 2}]"#;
        let (beads, _) = parse_ready_beads_json(json);
        assert_eq!(beads.len(), 1);
        assert_eq!(beads[0].affected_globs, None); // no design = affects everything
    }

    #[test]
    fn test_parse_ready_beads_json_with_dependencies() {
        let json = r#"[
            {
                "id": "beads-abc",
                "priority": 1,
                "dependencies": [
                    {"depends_on_id": "beads-def", "type": "blocks"}
                ]
            },
            {
                "id": "beads-def",
                "priority": 2,
                "dependencies": []
            }
        ]"#;
        let (beads, nodes) = parse_ready_beads_json(json);
        assert_eq!(beads.len(), 2);
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].id, "beads-abc");
        assert_eq!(nodes[0].depends_on, vec!["beads-def"]);
        assert_eq!(nodes[1].id, "beads-def");
        assert!(nodes[1].depends_on.is_empty());
    }

    #[test]
    fn test_parse_and_filter_beads_no_cycles_no_deps() {
        let json = r#"[
            {"id": "a", "priority": 1, "dependencies": []},
            {"id": "b", "priority": 2, "dependencies": []}
        ]"#;
        let result = parse_and_filter_beads(json);
        assert_eq!(result.ready.len(), 2);
        assert!(result.cycles.is_empty());
    }

    #[test]
    fn test_parse_and_filter_beads_blocked_by_dependency() {
        // "a" depends on "b" (both open) — only "b" is ready
        let json = r#"[
            {"id": "a", "priority": 1, "dependencies": [{"depends_on_id": "b", "type": "blocks"}]},
            {"id": "b", "priority": 2, "dependencies": []}
        ]"#;
        let result = parse_and_filter_beads(json);
        assert_eq!(result.ready.len(), 1);
        assert_eq!(result.ready[0].id, "b");
        assert!(result.cycles.is_empty());
    }

    #[test]
    fn test_parse_and_filter_beads_dep_on_closed_bead() {
        // "a" depends on "c" which is NOT in the open list — "a" is ready
        let json = r#"[
            {"id": "a", "priority": 1, "dependencies": [{"depends_on_id": "c", "type": "blocks"}]},
            {"id": "b", "priority": 2, "dependencies": []}
        ]"#;
        let result = parse_and_filter_beads(json);
        assert_eq!(result.ready.len(), 2);
        assert!(result.cycles.is_empty());
    }

    #[test]
    fn test_parse_and_filter_beads_chain_deps() {
        // "a" depends on "b", "b" depends on "c" — only "c" is ready
        let json = r#"[
            {"id": "a", "priority": 1, "dependencies": [{"depends_on_id": "b", "type": "blocks"}]},
            {"id": "b", "priority": 2, "dependencies": [{"depends_on_id": "c", "type": "blocks"}]},
            {"id": "c", "priority": 3, "dependencies": []}
        ]"#;
        let result = parse_and_filter_beads(json);
        assert_eq!(result.ready.len(), 1);
        assert_eq!(result.ready[0].id, "c");
        assert!(result.cycles.is_empty());
    }

    #[test]
    fn test_parse_and_filter_beads_with_cycle() {
        let json = r#"[
            {"id": "a", "priority": 1, "dependencies": [{"depends_on_id": "b", "type": "blocks"}]},
            {"id": "b", "priority": 2, "dependencies": [{"depends_on_id": "a", "type": "blocks"}]},
            {"id": "c", "priority": 3, "dependencies": []}
        ]"#;
        let result = parse_and_filter_beads(json);
        // a and b are in a cycle, only c should remain
        assert_eq!(result.ready.len(), 1);
        assert_eq!(result.ready[0].id, "c");
        assert_eq!(result.cycles.len(), 1);
        assert_eq!(result.cycles[0], vec!["a", "b"]);
    }

    #[test]
    fn test_format_cycle_path() {
        assert_eq!(
            format_cycle_path(&["a".into(), "b".into(), "c".into()]),
            "a -> b -> c -> a"
        );
        assert_eq!(format_cycle_path(&["x".into()]), "x -> x");
        assert_eq!(format_cycle_path(&[]), "");
    }

    #[test]
    fn test_build_in_progress_list_empty_pool() {
        let dir = tempdir().unwrap();
        let wt_dir = dir.path().join("worktrees");
        let config = WorkersConfig {
            max: 2,
            base_branch: "main".to_string(),
            worktrees_dir: "worktrees".to_string(),
            persistent: false,
        };
        let pool = WorkerPool::new(&config, dir.path().to_path_buf(), wt_dir, 0);
        let db_path = dir.path().join("test.db");
        let db_conn = db::open_or_create(&db_path).unwrap();
        let in_progress = build_in_progress_list(&pool, &db_conn);
        assert!(in_progress.is_empty());
    }

    #[tokio::test]
    #[ignore] // Slow test: sleep loop takes >60s, stalls CI and agent iterations
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
            persistent: false,
        };
        let mut pool = WorkerPool::new(&workers_config, repo.to_path_buf(), wt_dir, 0);

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
            persistent: false,
        };
        let mut pool = WorkerPool::new(
            &workers_config,
            dir.path().to_path_buf(),
            dir.path().join("wt"),
            0,
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
            persistent: false,
        };
        let mut pool = WorkerPool::new(
            &workers_config,
            dir.path().to_path_buf(),
            dir.path().join("wt"),
            0,
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
            persistent: false,
        };
        let mut pool = WorkerPool::new(
            &workers_config,
            dir.path().to_path_buf(),
            dir.path().join("wt"),
            0,
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
            persistent: false,
        };
        let mut pool = WorkerPool::new(
            &workers_config,
            dir.path().to_path_buf(),
            dir.path().join("wt"),
            0,
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

    #[test]
    fn test_parse_orphaned_bead_ids_valid() {
        let json = r#"[
            {"id": "beads-abc", "status": "in_progress"},
            {"id": "beads-def", "status": "in_progress"}
        ]"#;
        let ids = parse_orphaned_bead_ids(json);
        assert_eq!(ids, vec!["beads-abc", "beads-def"]);
    }

    #[test]
    fn test_parse_orphaned_bead_ids_empty() {
        let ids = parse_orphaned_bead_ids("[]");
        assert!(ids.is_empty());
    }

    #[test]
    fn test_parse_orphaned_bead_ids_invalid_json() {
        let ids = parse_orphaned_bead_ids("not json");
        assert!(ids.is_empty());
    }

    #[test]
    fn test_parse_orphaned_bead_ids_missing_id() {
        let json = r#"[{"status": "in_progress"}]"#;
        let ids = parse_orphaned_bead_ids(json);
        assert!(ids.is_empty());
    }

    #[test]
    fn test_recover_orphaned_beads_does_not_panic() {
        // When bd is not available, recover_orphaned_beads should
        // gracefully handle the error without panicking
        recover_orphaned_beads();
    }

    // ── Auto-promotion tests ───────────────────────────────────────────

    #[test]
    fn test_run_auto_promotion_disabled_when_zero() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db_conn = db::open_or_create(&db_path).unwrap();

        db::insert_improvement(&db_conn, "workflow", "Test", None, None, None).unwrap();

        let mut config = test_config(dir.path());
        config.improvements.auto_promote_after = 0; // disabled

        // Should be a no-op even with completed beads
        run_auto_promotion(&config, &db_conn, &db_path, 10);

        let imp = db::get_improvement(&db_conn, "R1").unwrap().unwrap();
        assert_eq!(imp.status, "open");
    }

    #[test]
    fn test_run_auto_promotion_not_ready_yet() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db_conn = db::open_or_create(&db_path).unwrap();

        db::insert_improvement(&db_conn, "workflow", "Test improvement", None, None, None).unwrap();

        // Only 2 observations — below threshold of 5
        for i in 1..=2 {
            db::upsert_observation(
                &db_conn,
                i,
                "2026-02-16T10:00:00Z",
                None,
                None,
                r#"{"turns.total": 50}"#,
            )
            .unwrap();
        }

        let config = test_config(dir.path());
        run_auto_promotion(&config, &db_conn, &db_path, 5);

        let imp = db::get_improvement(&db_conn, "R1").unwrap().unwrap();
        assert_eq!(imp.status, "open"); // not promoted yet
    }

    #[test]
    fn test_run_auto_promotion_promotes_after_threshold() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db_conn = db::open_or_create(&db_path).unwrap();

        // Create improvement with an early timestamp
        db::insert_improvement(
            &db_conn,
            "workflow",
            "Batch file reads",
            Some("Always batch independent file reads"),
            None,
            None,
        )
        .unwrap();

        // Verify improvement was created
        let _imp = db::get_improvement(&db_conn, "R1").unwrap().unwrap();

        // Add 6 observations after the improvement was created (threshold is 5)
        for i in 1..=6 {
            // Use a timestamp after creation
            db::upsert_observation(
                &db_conn,
                i,
                "2099-01-01T00:00:00Z",
                None,
                None,
                r#"{"turns.total": 50}"#,
            )
            .unwrap();
        }

        let prompt_file = dir.path().join("PROMPT.md");
        std::fs::write(&prompt_file, "# Original Prompt\n").unwrap();

        let mut config = test_config(dir.path());
        config.improvements.auto_promote_after = 5;
        config.improvements.prompt_file = prompt_file.clone();

        run_auto_promotion(&config, &db_conn, &db_path, 5);

        // Check the improvement was promoted
        let imp = db::get_improvement(&db_conn, "R1").unwrap().unwrap();
        assert_eq!(imp.status, "promoted");

        // Check PROMPT.md was updated with the body text
        let prompt_content = std::fs::read_to_string(&prompt_file).unwrap();
        assert!(prompt_content.contains("Always batch independent file reads"));
        assert!(prompt_content.contains("Promoted from R1"));
    }

    #[test]
    fn test_run_auto_promotion_only_runs_on_interval() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db_conn = db::open_or_create(&db_path).unwrap();

        db::insert_improvement(&db_conn, "workflow", "Test", None, None, None).unwrap();

        // Add enough observations
        for i in 1..=10 {
            db::upsert_observation(
                &db_conn,
                i,
                "2099-01-01T00:00:00Z",
                None,
                None,
                r#"{"turns.total": 50}"#,
            )
            .unwrap();
        }

        let prompt_file = dir.path().join("PROMPT.md");
        std::fs::write(&prompt_file, "").unwrap();

        let mut config = test_config(dir.path());
        config.improvements.auto_promote_after = 5;
        config.improvements.prompt_file = prompt_file;

        // completed_beads=3 is not a multiple of 5, so no promotion
        run_auto_promotion(&config, &db_conn, &db_path, 3);

        let imp = db::get_improvement(&db_conn, "R1").unwrap().unwrap();
        assert_eq!(imp.status, "open"); // not promoted
    }

    #[test]
    fn test_append_promoted_to_prompt_creates_file() {
        let dir = tempdir().unwrap();
        let prompt_path = dir.path().join("PROMPT.md");

        let imp = db::Improvement {
            ref_id: "R1".to_string(),
            created: "2026-02-16".to_string(),
            category: "workflow".to_string(),
            status: "open".to_string(),
            title: "Batch reads".to_string(),
            body: Some("Always batch independent file reads".to_string()),
            context: None,
            tags: None,
        };

        append_promoted_to_prompt(&prompt_path, &imp).unwrap();

        let content = std::fs::read_to_string(&prompt_path).unwrap();
        assert!(content.contains("Promoted from R1 [workflow]"));
        assert!(content.contains("Always batch independent file reads"));
    }

    #[test]
    fn test_append_promoted_to_prompt_uses_title_when_no_body() {
        let dir = tempdir().unwrap();
        let prompt_path = dir.path().join("PROMPT.md");

        let imp = db::Improvement {
            ref_id: "R2".to_string(),
            created: "2026-02-16".to_string(),
            category: "cost".to_string(),
            status: "open".to_string(),
            title: "Skip redundant lints".to_string(),
            body: None,
            context: None,
            tags: None,
        };

        append_promoted_to_prompt(&prompt_path, &imp).unwrap();

        let content = std::fs::read_to_string(&prompt_path).unwrap();
        assert!(content.contains("Skip redundant lints"));
    }

    #[test]
    fn test_sessions_since_improvement() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db_conn = db::open_or_create(&db_path).unwrap();

        // Add observations at various timestamps
        db::upsert_observation(
            &db_conn,
            1,
            "2026-01-01T00:00:00Z",
            None,
            None,
            r#"{"turns.total": 50}"#,
        )
        .unwrap();
        db::upsert_observation(
            &db_conn,
            2,
            "2026-01-15T00:00:00Z",
            None,
            None,
            r#"{"turns.total": 50}"#,
        )
        .unwrap();
        db::upsert_observation(
            &db_conn,
            3,
            "2026-02-01T00:00:00Z",
            None,
            None,
            r#"{"turns.total": 50}"#,
        )
        .unwrap();

        // Improvement created on Jan 10 — should see 2 sessions after it
        assert_eq!(
            sessions_since_improvement(&db_conn, "2026-01-10T00:00:00Z"),
            2
        );

        // Improvement created on Feb 01 — 0 sessions after it
        assert_eq!(
            sessions_since_improvement(&db_conn, "2026-02-01T00:00:00Z"),
            0
        );

        // Improvement created before all sessions — 3 sessions after it
        assert_eq!(
            sessions_since_improvement(&db_conn, "2025-12-01T00:00:00Z"),
            3
        );
    }

    #[test]
    fn test_close_bead_direct_does_not_panic() {
        // When bd is not available, close_bead_direct should handle gracefully
        close_bead_direct("beads-test-nonexistent");
    }

    #[test]
    fn test_coordinator_single_agent_direct_commit() {
        // Verify that in single-agent mode (max=1), the pool reports is_single_agent()
        // which the coordinator uses to skip integration and call close_bead_direct() instead.
        let dir = tempdir().unwrap();
        let wt_dir = dir.path().join("worktrees");

        let workers_config = WorkersConfig {
            max: 1,
            base_branch: "main".to_string(),
            worktrees_dir: "worktrees".to_string(),
            persistent: false,
        };
        let pool = WorkerPool::new(&workers_config, dir.path().to_path_buf(), wt_dir, 0);

        // Single-agent mode should be detected
        assert!(pool.is_single_agent());

        // Multi-agent mode should NOT be single-agent
        let multi_config = WorkersConfig {
            max: 3,
            base_branch: "main".to_string(),
            worktrees_dir: "worktrees".to_string(),
            persistent: false,
        };
        let multi_pool = WorkerPool::new(
            &multi_config,
            dir.path().to_path_buf(),
            dir.path().join("wt2"),
            0,
        );
        assert!(!multi_pool.is_single_agent());
    }

    #[test]
    fn test_assemble_base_prompt_graceful_on_missing_prompt_file() {
        let dir = tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.session.prompt_file = dir.path().join("nonexistent.md");
        let data_dir = test_data_dir(dir.path());

        // Should return empty string, not panic
        let prompt = assemble_base_prompt(&config, &data_dir);
        assert!(prompt.is_empty());
    }

    #[test]
    fn test_assemble_base_prompt_includes_prompt_file() {
        let dir = tempdir().unwrap();
        let mut config = test_config(dir.path());
        let prompt_path = dir.path().join("prompt.md");
        std::fs::write(&prompt_path, "You are a coding agent.").unwrap();
        config.session.prompt_file = prompt_path;
        let data_dir = test_data_dir(dir.path());

        let prompt = assemble_base_prompt(&config, &data_dir);
        assert!(prompt.contains("You are a coding agent."));
    }

    #[test]
    fn test_assemble_base_prompt_includes_open_improvements() {
        let dir = tempdir().unwrap();
        let mut config = test_config(dir.path());
        let prompt_path = dir.path().join("prompt.md");
        std::fs::write(&prompt_path, "Main prompt").unwrap();
        config.session.prompt_file = prompt_path;
        let data_dir = test_data_dir(dir.path());

        // Add an open improvement to the DB
        let db_path = data_dir.db();
        let conn = db::open_or_create(&db_path).unwrap();
        db::insert_improvement(&conn, "workflow", "Batch file reads", None, None, None).unwrap();
        drop(conn);

        let prompt = assemble_base_prompt(&config, &data_dir);
        assert!(prompt.contains("OPEN IMPROVEMENTS"));
        assert!(prompt.contains("Batch file reads"));
        assert!(prompt.contains("Main prompt"));
    }

    // ── format_duration_secs tests ──

    #[test]
    fn test_format_duration_secs_seconds() {
        assert_eq!(format_duration_secs(0), "0s");
        assert_eq!(format_duration_secs(45), "45s");
        assert_eq!(format_duration_secs(59), "59s");
    }

    #[test]
    fn test_format_duration_secs_minutes() {
        assert_eq!(format_duration_secs(60), "1m");
        assert_eq!(format_duration_secs(300), "5m");
        assert_eq!(format_duration_secs(3599), "59m");
    }

    #[test]
    fn test_format_duration_secs_hours() {
        assert_eq!(format_duration_secs(3600), "1h");
        assert_eq!(format_duration_secs(5400), "1h 30m");
        assert_eq!(format_duration_secs(7200), "2h");
    }

    // ── build_progress_string tests ──

    #[test]
    fn test_build_progress_string_empty_db() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let conn = crate::db::open_or_create(&db_path).unwrap();
        let result = build_progress_string(&conn, 1);
        assert!(result.is_none());
    }

    #[test]
    fn test_build_progress_string_with_completed() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let conn = crate::db::open_or_create(&db_path).unwrap();

        for i in 0..3 {
            crate::db::upsert_bead_metrics(
                &conn,
                &format!("bead-{}", i),
                1,
                300.0,
                50,
                None,
                None,
                Some("2026-01-01T00:00:00Z"),
            )
            .unwrap();
        }

        let result = build_progress_string(&conn, 1);
        assert!(result.is_some());
        let s = result.unwrap();
        assert!(s.contains("Progress:"));
    }
}
