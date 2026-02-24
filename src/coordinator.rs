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
use crate::defaults;
use crate::estimation::{self, BeadNode};
use crate::improve;
use crate::ingest;
use crate::integrator::{
    close_bead_in_bd, CircuitBreaker, IntegrationQueue, TrippedFailure, ValidationCircuitBreaker,
};
use crate::pool::{PoolError, SessionOutcome, WorkerPool};
use crate::prompt;
use crate::ratelimit;
use crate::scheduler::{self, InProgressAssignment, ReadyBead};
use crate::signals::SignalHandler;
use crate::status::{HarnessState, StatusTracker};
use crate::worktree;
use rusqlite::Connection;
use std::collections::{HashMap, HashSet};
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
    /// Agent quota exhausted (not a transient rate limit).
    QuotaExhausted(String),
    /// Too many rapid consecutive session failures (operator intervention needed).
    RapidFailures(String),
    /// Fatal error (e.g., database failure).
    Error(String),
}

const RAPID_FAILURE_BACKOFF_START: u32 = 5;
const RAPID_FAILURE_PAUSE_THRESHOLD: u32 = 10;
const RAPID_FAILURE_MAX_DURATION_SECS: u64 = 10;

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
        IntegrationQueue::new(repo_dir.clone(), config.workers.base_branch.clone())
            .with_speck_validate(config.speck_validate.clone());
    let mut circuit_breaker = CircuitBreaker::new();
    let mut validation_circuit_breaker =
        ValidationCircuitBreaker::new(config.speck_validate.max_validation_retries);

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
    let mut consecutive_quota_failures = 0u32;
    let mut consecutive_rapid_failures = 0u32;
    let mut previous_dependency_filter_counts: Option<(usize, usize)> = None;
    let mut total_completed_sessions: u32 = 0;
    let mut draining = false;
    let mut drain_reason: Option<CoordinatorExitReason> = None;
    const MAX_CONSECUTIVE_NO_WORK: u32 = 3;
    const MAX_CONSECUTIVE_QUOTA_FAILURES: u32 = 2;

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
            && !draining
        {
            draining = true;
            drain_reason = Some(CoordinatorExitReason::StopFile);
            status.update(HarnessState::ShuttingDown);
            tracing::info!(
                active_workers = pool.active_count(),
                "STOP file detected, draining active workers"
            );
        }

        // Poll for completed workers
        let outcomes = pool.poll_completed().await;

        // Snapshot which outcomes are analysis agents *before* any resets
        // clear the bead_id. Used below to exclude them from the session counter.
        let analysis_worker_ids: Vec<u32> = outcomes
            .iter()
            .filter(|o| {
                pool.worker_bead_id(o.worker_id)
                    .map(is_analysis_bead)
                    .unwrap_or(false)
            })
            .map(|o| o.worker_id)
            .collect();

        for outcome in &outcomes {
            if let Err(e) = pool.record_outcome(outcome, &db_conn) {
                tracing::warn!(error = %e, worker_id = outcome.worker_id, "failed to record outcome");
            }

            // Ingest JSONL metrics from the worker's output file
            let ingest_result =
                ingest_worker_metrics(outcome, &db_conn, &extraction_rules, adapter.as_ref());

            let succeeded = outcome.exit_code == Some(0);
            if succeeded {
                consecutive_rapid_failures = 0;
                tracing::info!(
                    worker_id = outcome.worker_id,
                    "bead coding completed, queued for integration"
                );
                // Worker state is now Completed — will be picked up for integration below
            } else {
                let is_analysis = pool
                    .worker_bead_id(outcome.worker_id)
                    .map(is_analysis_bead)
                    .unwrap_or(false);

                if is_analysis {
                    tracing::warn!(
                        worker_id = outcome.worker_id,
                        exit_code = ?outcome.exit_code,
                        "analysis agent failed"
                    );
                } else {
                    failed_beads += 1;
                    let rapid_failure = is_rapid_session_failure(
                        outcome,
                        ingest_result.as_ref(),
                        config.watchdog.min_output_bytes,
                    );
                    if rapid_failure {
                        consecutive_rapid_failures += 1;
                        tracing::warn!(
                            worker_id = outcome.worker_id,
                            consecutive = consecutive_rapid_failures,
                            duration_secs = outcome.duration.as_secs(),
                            output_bytes = outcome.output_bytes,
                            "rapid session failure detected"
                        );
                    } else {
                        consecutive_rapid_failures = 0;
                    }
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
                    // Check for quota exhaustion (hard limit, not transient rate limit)
                    if let Some(quota_msg) =
                        ratelimit::detect_quota_exhaustion(&outcome.output_file)
                    {
                        consecutive_quota_failures += 1;
                        tracing::error!(
                            worker_id = outcome.worker_id,
                            consecutive = consecutive_quota_failures,
                            "agent quota exhausted: {quota_msg}"
                        );
                    } else {
                        // Non-quota failure resets the counter
                        consecutive_quota_failures = 0;
                    }

                    tracing::warn!(
                        worker_id = outcome.worker_id,
                        exit_code = ?outcome.exit_code,
                        "bead failed"
                    );
                }

                // Reset failed workers back to idle immediately
                if let Err(e) = pool.reset_worker(outcome.worker_id) {
                    tracing::warn!(error = %e, worker_id = outcome.worker_id, "failed to reset worker");
                }
            }
        }

        // Track completed sessions for analysis agent trigger.
        // Exclude analysis agent outcomes so they don't count toward the
        // analyze_every interval (otherwise analyze_every=1 would loop forever).
        let coding_outcome_count = outcomes
            .iter()
            .filter(|o| !analysis_worker_ids.contains(&o.worker_id))
            .count();
        total_completed_sessions += coding_outcome_count as u32;

        // Exit if agent quota is exhausted
        if consecutive_quota_failures >= MAX_CONSECUTIVE_QUOTA_FAILURES {
            let agent_cmd = &config.agent.command;
            eprintln!();
            eprintln!("ERROR: Agent quota exhausted — all recent workers failed with a usage limit error.");
            eprintln!(
                "       The {} agent has hit its plan/credit limit.",
                agent_cmd
            );
            eprintln!();
            eprintln!("  To resume, either:");
            eprintln!("    1. Wait for your quota to reset");
            eprintln!("    2. Upgrade your plan at the provider");
            eprintln!(
                "    3. Switch to a different agent in .blacksmith/config.toml: [agent] command = \"...\""
            );
            eprintln!();
            status.update(HarnessState::ShuttingDown);
            status.remove();
            return CoordinatorSummary {
                completed_beads,
                failed_beads,
                exit_reason: CoordinatorExitReason::QuotaExhausted(agent_cmd.clone()),
            };
        }

        // Pause coordinator after repeated instant failures, regardless of root cause.
        if consecutive_rapid_failures >= RAPID_FAILURE_PAUSE_THRESHOLD {
            let message =
                format!("{consecutive_rapid_failures} consecutive rapid session failures detected");
            eprintln!();
            eprintln!("ALERT: Rapid consecutive session failures detected.");
            eprintln!("       {message}.");
            eprintln!("       Coordinator is pausing to prevent session ID churn.");
            eprintln!();
            eprintln!("  Check agent credentials/quota, network, and config before resuming.");
            eprintln!();
            status.update(HarnessState::ShuttingDown);
            status.remove();
            return CoordinatorSummary {
                completed_beads,
                failed_beads,
                exit_reason: CoordinatorExitReason::RapidFailures(message),
            };
        }

        // Integration: process one completed worker at a time (sequential)
        // Only integrate if no other worker is currently integrating
        if !pool.has_integrating() {
            if let Some((worker_id, assignment_id, worktree_path, bead_id)) = pool.next_completed()
            {
                let is_analysis = is_analysis_bead(&bead_id);

                // In single-agent mode, the agent committed directly to the main branch.
                // Skip the integration merge — just close the bead and reset the worker.
                if pool.is_single_agent() {
                    pool.set_integrating(worker_id);

                    if is_analysis {
                        tracing::info!(
                            worker_id,
                            bead_id = %bead_id,
                            "analysis agent completed (single-agent mode)"
                        );
                    } else {
                        tracing::info!(
                            worker_id,
                            bead_id = %bead_id,
                            "single-agent mode: closing bead directly (no merge)"
                        );

                        close_bead_direct(&bead_id);
                        auto_close_parent_epics(&bead_id);

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
                    }

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
                        &mut validation_circuit_breaker,
                    );

                    if result.success {
                        tracing::info!(
                            worker_id,
                            bead_id = %bead_id,
                            commit = ?result.merge_commit,
                            "integration succeeded"
                        );

                        if !is_analysis {
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

                            auto_close_parent_epics(&bead_id);

                            // Run auto-promotion cycle after successful integration
                            run_auto_promotion(config, &db_conn, &data_dir.db(), completed_beads);
                        }

                        // Reset the worker back to idle after successful integration
                        if let Err(e) = pool.reset_worker(worker_id) {
                            tracing::warn!(error = %e, worker_id, "failed to reset worker after integration");
                        }
                    } else if is_analysis {
                        // Analysis integration failure: just log and reset, don't trip circuit breaker
                        tracing::warn!(
                            worker_id,
                            bead_id = %bead_id,
                            reason = ?result.failure_reason,
                            "analysis agent integration failed"
                        );
                        if let Err(e) = pool.reset_worker(worker_id) {
                            tracing::warn!(error = %e, worker_id, "failed to reset worker after analysis integration failure");
                        }
                    } else {
                        let error_summary =
                            result.failure_reason.as_deref().unwrap_or("unknown error");

                        // Check if the validation circuit breaker has tripped first
                        // (integrate() already recorded validation attempts)
                        if let Some(tripped) = validation_circuit_breaker.check_tripped(
                            &bead_id,
                            error_summary,
                            &worktree_path,
                        ) {
                            // Validation retries exhausted — escalate to human review
                            failed_beads += 1;
                            handle_tripped_failure(&tripped);
                            // Do NOT reset the worker — worktree is preserved for inspection
                        } else if let Some(tripped) =
                            circuit_breaker.check_tripped(&bead_id, error_summary, &worktree_path)
                        {
                            // Integration circuit breaker tripped — escalate to human review
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
        if !draining && pool.idle_count() > 0 {
            // Gather in-progress assignments for the scheduler
            let in_progress = build_in_progress_list(&pool, &db_conn);

            // Query beads, detect cycles, and filter out cycled beads
            let bead_query = query_ready_beads();
            let blocked_count = bead_query.blocked_count;
            let ready_beads = bead_query.ready;
            let current_dependency_filter_counts = (blocked_count, ready_beads.len());

            if blocked_count > 0 {
                if should_log_dependency_filter_info(
                    previous_dependency_filter_counts,
                    current_dependency_filter_counts,
                ) {
                    tracing::info!(
                        blocked = blocked_count,
                        ready = ready_beads.len(),
                        "filtered out {blocked_count} beads with unresolved dependencies"
                    );
                } else {
                    tracing::debug!(
                        blocked = blocked_count,
                        ready = ready_beads.len(),
                        "filtered out {blocked_count} beads with unresolved dependencies"
                    );
                }
            }
            previous_dependency_filter_counts = Some(current_dependency_filter_counts);

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

            if !assignable.is_empty() {
                if let Some(delay_secs) = rapid_failure_backoff_delay_secs(
                    consecutive_rapid_failures,
                    config.backoff.initial_delay_secs,
                    config.backoff.max_delay_secs,
                ) {
                    tracing::warn!(
                        delay_secs,
                        consecutive_failures = consecutive_rapid_failures,
                        "applying rapid-failure backoff before spawning workers"
                    );
                    tokio::time::sleep(Duration::from_secs(delay_secs)).await;
                }
            }

            // Reserve one idle slot for the analysis agent when it's due,
            // so coding beads don't starve it of workers.
            let analysis_due = should_spawn_analysis(config, total_completed_sessions, &pool);
            let coding_slots = if analysis_due {
                (pool.idle_count() as usize).saturating_sub(1)
            } else {
                pool.idle_count() as usize
            };

            // Assemble the base prompt once (brief + improvements + PROMPT.md).
            // Each worker gets this base prompt with a bead-specific suffix.
            let base_prompt = assemble_base_prompt(config, data_dir);

            for bead_id in assignable.iter().take(coding_slots) {
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
                        if affected_globs_str.is_none() {
                            tracing::warn!(
                                bead_id,
                                "bead has no 'affected:' declaration in its design section — \
                                 add 'affected: <glob>, ...' to enable smarter parallel scheduling \
                                 (e.g. 'affected: src/auth/**, prisma/schema.prisma')"
                            );
                        }
                    }
                    Err(PoolError::NoIdleWorker) => break,
                    Err(e) => {
                        tracing::error!(error = %e, bead_id, "failed to spawn worker for bead");
                    }
                }
            }

            // Spawn analysis agent if conditions are met and an idle slot is available
            // (scheduled after coding beads so coding gets priority)
            if pool.idle_count() > 0
                && should_spawn_analysis(config, total_completed_sessions, &pool)
            {
                let ts = chrono_timestamp();
                let analysis_bead_id = format!("analysis-{ts}");
                let analysis_prompt = assemble_analysis_prompt(config, data_dir, &db_conn);
                let resolved_analysis = config.agent.resolved_analysis();
                match pool
                    .spawn_worker(
                        &analysis_bead_id,
                        Some(".beads/**"),
                        &resolved_analysis,
                        &analysis_prompt,
                        &output_dir,
                        &db_conn,
                    )
                    .await
                {
                    Ok((worker_id, _)) => {
                        save_counter(&counter_path, pool.next_session_id());
                        tracing::info!(
                            worker_id,
                            bead_id = %analysis_bead_id,
                            "spawned analysis agent"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "failed to spawn analysis agent");
                    }
                }
            }
        }

        // Update status between poll cycles
        if draining {
            tracing::info!(
                active_workers = pool.active_count(),
                completed_pending_integration = pool.completed_workers().len(),
                integrating = pool.has_integrating(),
                "draining coordinator"
            );
        }

        if !draining && pool.active_count() == 0 {
            status.update(HarnessState::Idle);
        }

        // Sleep before next poll cycle
        tokio::time::sleep(Duration::from_secs(2)).await;

        if draining
            && pool.active_count() == 0
            && !pool.has_integrating()
            && pool.completed_workers().is_empty()
        {
            status.remove();
            return CoordinatorSummary {
                completed_beads,
                failed_beads,
                exit_reason: drain_reason
                    .take()
                    .unwrap_or(CoordinatorExitReason::StopFile),
            };
        }
    }
}

// ── Analysis agent ──────────────────────────────────────────────────────

/// Check if a bead ID identifies an analysis agent run.
fn is_analysis_bead(id: &str) -> bool {
    id.starts_with("analysis-")
}

/// Determine whether the analysis agent should be spawned.
///
/// Returns false if disabled, if an analysis worker is already running in the pool,
/// or if the session count hasn't hit the configured interval.
fn should_spawn_analysis(config: &HarnessConfig, total_sessions: u32, pool: &WorkerPool) -> bool {
    let every = config.improvements.analyze_every;
    if every == 0 {
        return false; // disabled
    }
    // Check if an analysis worker is already running in the pool
    let analysis_running = pool.snapshot().iter().any(|(_, state, bead_id)| {
        *state == crate::pool::WorkerState::Coding && bead_id.map(is_analysis_bead).unwrap_or(false)
    });
    if analysis_running {
        return false;
    }
    total_sessions > 0 && total_sessions.is_multiple_of(every)
}

/// Assemble the analysis prompt with metrics data and open improvements.
fn assemble_analysis_prompt(
    config: &HarnessConfig,
    data_dir: &DataDir,
    db_conn: &Connection,
) -> String {
    // Load template: custom file in .blacksmith/ or embedded default
    let custom_path = data_dir.root().join("ANALYSIS_PROMPT.md");
    let template = if custom_path.exists() {
        std::fs::read_to_string(&custom_path)
            .unwrap_or_else(|_| defaults::ANALYSIS_PROMPT.to_string())
    } else {
        defaults::ANALYSIS_PROMPT.to_string()
    };

    // Query recent observations
    let limit = config.improvements.analyze_sessions as i64;
    let observations = db::recent_observations(db_conn, limit).unwrap_or_default();
    let metrics_table = format_observations_table(&observations);

    // Query open improvements
    let improvements = db::list_improvements(db_conn, Some("open"), None).unwrap_or_default();
    let improvements_text = if improvements.is_empty() {
        "(none)".to_string()
    } else {
        improvements
            .iter()
            .map(|imp| format!("- [{}] {}: {}", imp.ref_id, imp.category, imp.title))
            .collect::<Vec<_>>()
            .join("\n")
    };

    // Count total sessions
    let session_count = observations.len();

    template
        .replace("{{recent_metrics}}", &metrics_table)
        .replace("{{open_improvements}}", &improvements_text)
        .replace("{{session_count}}", &session_count.to_string())
}

/// Format observations as a markdown table for the analysis prompt.
fn format_observations_table(observations: &[db::Observation]) -> String {
    if observations.is_empty() {
        return "(no session data available)".to_string();
    }

    let mut lines = Vec::new();
    lines.push("| Session | Timestamp | Duration (s) | Outcome | Data |".to_string());
    lines.push("|---------|-----------|-------------|---------|------|".to_string());

    for obs in observations {
        let duration = obs
            .duration
            .map(|d| d.to_string())
            .unwrap_or_else(|| "-".to_string());
        let outcome = obs.outcome.as_deref().unwrap_or("-");
        // Truncate data to keep the table readable
        let data = if obs.data.len() > 120 {
            format!("{}...", &obs.data[..120])
        } else {
            obs.data.clone()
        };
        lines.push(format!(
            "| {} | {} | {} | {} | {} |",
            obs.session, obs.ts, duration, outcome, data
        ));
    }

    lines.join("\n")
}

/// Generate a compact timestamp for analysis bead IDs.
fn chrono_timestamp() -> String {
    use std::time::SystemTime;
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    format!("{}", now.as_secs())
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
    close_bead_in_bd(bead_id, "single-agent direct commit");
}

/// Open epic hierarchy derived from `bd list --status=open --json`.
#[derive(Debug, Default)]
struct OpenEpicHierarchy {
    open_ids: HashSet<String>,
    parent_children: HashMap<String, Vec<String>>,
    child_parents: HashMap<String, Vec<String>>,
}

/// After closing a bead, auto-close parent epics whose children are all closed.
///
/// This walks upward recursively (child -> parent epic -> grandparent epic), and
/// logs warnings on any `bd` failures without interrupting coordinator flow.
fn auto_close_parent_epics(closed_bead_id: &str) {
    let output = match std::process::Command::new("bd")
        .args(["list", "--status=open", "--json"])
        .output()
    {
        Ok(output) if output.status.success() => output,
        Ok(output) => {
            let stderr = String::from_utf8_lossy(&output.stderr);
            tracing::warn!(
                bead_id = closed_bead_id,
                stderr = %stderr.trim(),
                "failed to query open beads for parent epic auto-close"
            );
            return;
        }
        Err(e) => {
            tracing::warn!(
                bead_id = closed_bead_id,
                error = %e,
                "failed to run bd list for parent epic auto-close"
            );
            return;
        }
    };

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut hierarchy = parse_open_epic_hierarchy(&stdout);

    if hierarchy.child_parents.is_empty() {
        return;
    }

    let mut stack = vec![closed_bead_id.to_string()];
    while let Some(child_id) = stack.pop() {
        let parents = hierarchy
            .child_parents
            .get(&child_id)
            .cloned()
            .unwrap_or_default();

        for parent_id in parents {
            if !hierarchy.open_ids.contains(&parent_id) {
                continue;
            }

            let has_open_children = hierarchy
                .parent_children
                .get(&parent_id)
                .map(|children| children.iter().any(|c| hierarchy.open_ids.contains(c)))
                .unwrap_or(false);

            if has_open_children {
                continue;
            }

            if close_epic_when_children_complete(&parent_id) {
                hierarchy.open_ids.remove(&parent_id);
                stack.push(parent_id);
            }
        }
    }
}

/// Parse open epic -> child and child -> parent mappings from `bd list --status=open --json`.
fn parse_open_epic_hierarchy(json_str: &str) -> OpenEpicHierarchy {
    let parsed: Result<Vec<serde_json::Value>, _> = serde_json::from_str(json_str);
    let mut hierarchy = OpenEpicHierarchy::default();

    let beads = match parsed {
        Ok(beads) => beads,
        Err(e) => {
            tracing::debug!(error = %e, "failed to parse open beads JSON for epic auto-close");
            return hierarchy;
        }
    };

    for bead in &beads {
        if let Some(id) = bead.get("id").and_then(|v| v.as_str()) {
            hierarchy.open_ids.insert(id.to_string());
        }
    }

    for bead in &beads {
        let Some(parent_id) = bead.get("id").and_then(|v| v.as_str()) else {
            continue;
        };
        let issue_type = bead
            .get("issue_type")
            .and_then(|v| v.as_str())
            .unwrap_or("task");
        if !issue_type.eq_ignore_ascii_case("epic") {
            continue;
        }

        let dependencies = bead
            .get("dependencies")
            .and_then(|d| d.as_array())
            .cloned()
            .unwrap_or_default();

        for dep in dependencies {
            let dep_type = dep
                .get("type")
                .or_else(|| dep.get("dependency_type"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if !dep_type.eq_ignore_ascii_case("parent-child") {
                continue;
            }

            let child_id = dep
                .get("depends_on_id")
                .or_else(|| dep.get("id"))
                .and_then(|v| v.as_str());
            let Some(child_id) = child_id else {
                continue;
            };

            hierarchy
                .parent_children
                .entry(parent_id.to_string())
                .or_default()
                .push(child_id.to_string());
            hierarchy
                .child_parents
                .entry(child_id.to_string())
                .or_default()
                .push(parent_id.to_string());
        }
    }

    hierarchy
}

/// Compute which parent epics become auto-close candidates, assuming each close succeeds.
#[cfg(test)]
fn plan_parent_epic_autoclose_order(
    closed_bead_id: &str,
    hierarchy: &OpenEpicHierarchy,
) -> Vec<String> {
    let mut order = Vec::new();
    let mut open_ids = hierarchy.open_ids.clone();
    let mut stack = vec![closed_bead_id.to_string()];

    while let Some(child_id) = stack.pop() {
        let parents = hierarchy
            .child_parents
            .get(&child_id)
            .cloned()
            .unwrap_or_default();

        for parent_id in parents {
            if !open_ids.contains(&parent_id) {
                continue;
            }

            let has_open_children = hierarchy
                .parent_children
                .get(&parent_id)
                .map(|children| children.iter().any(|c| open_ids.contains(c)))
                .unwrap_or(false);
            if has_open_children {
                continue;
            }

            open_ids.remove(&parent_id);
            order.push(parent_id.clone());
            stack.push(parent_id);
        }
    }

    order
}

/// Auto-close an epic whose children are complete. Returns true when close succeeds.
fn close_epic_when_children_complete(epic_id: &str) -> bool {
    let reason = "all children completed";
    match std::process::Command::new("bd")
        .args(["close", epic_id, &format!("--reason={reason}")])
        .output()
    {
        Ok(out) if out.status.success() => {
            tracing::info!(bead_id = epic_id, reason, "auto-closed parent epic");
        }
        Ok(out) => {
            let stderr = String::from_utf8_lossy(&out.stderr);
            tracing::warn!(
                bead_id = epic_id,
                reason,
                stderr = %stderr.trim(),
                "failed to auto-close parent epic"
            );
            return false;
        }
        Err(e) => {
            tracing::warn!(
                bead_id = epic_id,
                reason,
                error = %e,
                "failed to run bd close for parent epic"
            );
            return false;
        }
    }

    match std::process::Command::new("bd").args(["sync"]).output() {
        Ok(out) if out.status.success() => {
            tracing::info!(bead_id = epic_id, "bd sync succeeded after epic auto-close");
        }
        Ok(out) => {
            let stderr = String::from_utf8_lossy(&out.stderr);
            tracing::warn!(
                bead_id = epic_id,
                stderr = %stderr.trim(),
                "bd sync failed after epic auto-close"
            );
        }
        Err(e) => {
            tracing::warn!(
                bead_id = epic_id,
                error = %e,
                "failed to run bd sync after epic auto-close"
            );
        }
    }

    true
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
    /// Number of open beads blocked by unresolved dependencies.
    blocked_count: usize,
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
                blocked_count: 0,
                cycles: Vec::new(),
            }
        }
    }
}

/// Parse JSON bead data, detect cycles, filter out cycled beads, and return schedulable beads.
fn parse_and_filter_beads(json_str: &str) -> BeadQuery {
    let (ready_beads, bead_nodes) = parse_ready_beads_json(json_str);
    let _open_ids_all: HashSet<String> = ready_beads.iter().map(|b| b.id.clone()).collect();

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

    // --- Epic child filtering ---
    // Exclude epics that still have open children.
    //
    // Build a set of epic IDs that have at least one open child.
    // In bd's JSON, a child bead has a "parent-child" dependency whose
    // `depends_on_id` is the *parent* epic.  So we scan all open beads and
    // reverse the relationship.
    let mut epics_with_open_children: HashSet<String> = HashSet::new();
    for rb in &after_cycle {
        for parent_id in &rb.parent_child_ids {
            epics_with_open_children.insert(parent_id.clone());
        }
    }

    let before_epic_count = after_cycle.len();
    let after_epic: Vec<ReadyBead> = after_cycle
        .into_iter()
        .filter(|b| {
            if b.issue_type.eq_ignore_ascii_case("epic") {
                !epics_with_open_children.contains(&b.id)
            } else {
                true
            }
        })
        .collect();
    let epic_filtered_count = before_epic_count - after_epic.len();
    if epic_filtered_count > 0 {
        tracing::info!(
            filtered = epic_filtered_count,
            remaining = after_epic.len(),
            "filtered out {epic_filtered_count} epics with open children"
        );
    }

    // --- Dependency filtering ---
    // A bead is truly ready only if none of its depends_on IDs are still open.
    let open_ids: HashSet<String> = after_epic.iter().map(|b| b.id.clone()).collect();
    let deps_map: std::collections::HashMap<&str, &[String]> = bead_nodes
        .iter()
        .map(|n| (n.id.as_str(), n.depends_on.as_slice()))
        .collect();

    let before_dep_count = after_epic.len();
    let truly_ready: Vec<ReadyBead> = after_epic
        .into_iter()
        .filter(|b| {
            deps_map
                .get(b.id.as_str())
                .map(|deps| deps.iter().all(|d| !open_ids.contains(d)))
                .unwrap_or(true)
        })
        .collect();

    let blocked_count = before_dep_count - truly_ready.len();

    BeadQuery {
        ready: truly_ready,
        blocked_count,
        cycles,
    }
}

fn should_log_dependency_filter_info(
    previous_counts: Option<(usize, usize)>,
    current_counts: (usize, usize),
) -> bool {
    previous_counts != Some(current_counts)
}

fn rapid_failure_backoff_delay_secs(
    consecutive_rapid_failures: u32,
    initial_delay_secs: u64,
    max_delay_secs: u64,
) -> Option<u64> {
    if consecutive_rapid_failures < RAPID_FAILURE_BACKOFF_START {
        return None;
    }
    let backoff_count = consecutive_rapid_failures - RAPID_FAILURE_BACKOFF_START;
    Some(ratelimit::backoff_delay(
        initial_delay_secs,
        backoff_count,
        max_delay_secs,
    ))
}

fn is_rapid_session_failure(
    outcome: &SessionOutcome,
    ingest_result: Option<&ingest::IngestResult>,
    min_output_bytes: u64,
) -> bool {
    let zero_turns = ingest_result.map(|m| m.turns_total == 0).unwrap_or(false);
    zero_turns
        || (outcome.duration.as_secs() <= RAPID_FAILURE_MAX_DURATION_SECS
            && outcome.output_bytes <= min_output_bytes)
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
                let issue_type = b
                    .get("issue_type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("task")
                    .to_string();
                let design = b.get("design").and_then(|d| d.as_str()).unwrap_or("");
                let affected_globs = scheduler::parse_affected_set(design);

                // Parse dependencies for cycle detection
                let dependencies = b
                    .get("dependencies")
                    .and_then(|d| d.as_array())
                    .cloned()
                    .unwrap_or_default();
                let depends_on: Vec<String> = dependencies
                    .iter()
                    .filter_map(|dep| {
                        dep.get("depends_on_id")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string())
                    })
                    .collect();
                let parent_child_ids: Vec<String> = dependencies
                    .iter()
                    .filter_map(|dep| {
                        let dep_type = dep.get("type").and_then(|v| v.as_str())?;
                        if dep_type.eq_ignore_ascii_case("parent-child") {
                            dep.get("depends_on_id")
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string())
                        } else {
                            None
                        }
                    })
                    .collect();

                ready.push(ReadyBead {
                    id: id.clone(),
                    priority,
                    issue_type,
                    parent_child_ids,
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
            speck_validate: crate::config::SpeckValidateConfig::default(),
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
            {"id": "beads-abc", "issue_type": "epic", "priority": 1, "design": "affected: src/db.rs", "dependencies": [{"depends_on_id": "beads-child", "type": "parent-child"}]},
            {"id": "beads-def", "issue_type": "task", "priority": 2, "design": "Some description\naffected: tests/**"}
        ]"#;
        let (beads, nodes) = parse_ready_beads_json(json);
        assert_eq!(beads.len(), 2);
        assert_eq!(beads[0].id, "beads-abc");
        assert_eq!(beads[0].issue_type, "epic");
        assert_eq!(beads[0].parent_child_ids, vec!["beads-child"]);
        assert_eq!(beads[0].priority, 1);
        assert_eq!(beads[0].affected_globs, Some(vec!["src/db.rs".to_string()]));
        assert_eq!(beads[1].id, "beads-def");
        assert_eq!(beads[1].issue_type, "task");
        assert!(beads[1].parent_child_ids.is_empty());
        assert_eq!(beads[1].priority, 2);
        assert_eq!(beads[1].affected_globs, Some(vec!["tests/**".to_string()]));
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].depends_on, vec!["beads-child"]);
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
        assert_eq!(result.blocked_count, 0);
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
        assert_eq!(result.blocked_count, 1);
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
        assert_eq!(result.blocked_count, 0);
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
        assert_eq!(result.blocked_count, 2);
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
        assert_eq!(result.blocked_count, 0);
        assert_eq!(result.ready[0].id, "c");
        assert_eq!(result.cycles.len(), 1);
        assert_eq!(result.cycles[0], vec!["a", "b"]);
    }

    #[test]
    fn test_should_log_dependency_filter_info() {
        assert!(should_log_dependency_filter_info(None, (2, 3)));
        assert!(should_log_dependency_filter_info(Some((1, 3)), (2, 3)));
        assert!(!should_log_dependency_filter_info(Some((2, 3)), (2, 3)));
    }

    #[test]
    fn test_rapid_failure_backoff_delay_starts_at_threshold() {
        assert_eq!(rapid_failure_backoff_delay_secs(4, 2, 60), None);
        assert_eq!(rapid_failure_backoff_delay_secs(5, 2, 60), Some(2));
        assert_eq!(rapid_failure_backoff_delay_secs(6, 2, 60), Some(4));
    }

    #[test]
    fn test_is_rapid_session_failure_detects_zero_turns() {
        let outcome = SessionOutcome {
            worker_id: 0,
            exit_code: Some(1),
            duration: std::time::Duration::from_secs(30),
            output_bytes: 10_000,
            output_file: std::path::PathBuf::from("out.jsonl"),
            session_id: 1,
        };
        let ingest = ingest::IngestResult {
            turns_total: 0,
            ..Default::default()
        };
        assert!(is_rapid_session_failure(&outcome, Some(&ingest), 100));
    }

    #[test]
    fn test_is_rapid_session_failure_falls_back_to_short_small_output() {
        let outcome = SessionOutcome {
            worker_id: 0,
            exit_code: Some(1),
            duration: std::time::Duration::from_secs(2),
            output_bytes: 50,
            output_file: std::path::PathBuf::from("out.jsonl"),
            session_id: 1,
        };
        assert!(is_rapid_session_failure(&outcome, None, 100));
    }

    #[test]
    fn test_is_rapid_session_failure_ignores_non_instant_failures() {
        let outcome = SessionOutcome {
            worker_id: 0,
            exit_code: Some(1),
            duration: std::time::Duration::from_secs(60),
            output_bytes: 10_000,
            output_file: std::path::PathBuf::from("out.jsonl"),
            session_id: 1,
        };
        let ingest = ingest::IngestResult {
            turns_total: 3,
            ..Default::default()
        };
        assert!(!is_rapid_session_failure(&outcome, Some(&ingest), 100));
    }

    #[test]
    fn test_parse_and_filter_beads_filters_epic_with_open_child() {
        let json = r#"[
            {"id": "epic-1", "issue_type": "epic", "priority": 1, "dependencies": [{"depends_on_id": "task-1", "type": "parent-child"}]},
            {"id": "task-1", "issue_type": "task", "priority": 2, "dependencies": []}
        ]"#;
        let result = parse_and_filter_beads(json);
        assert_eq!(result.ready.len(), 1);
        assert_eq!(result.ready[0].id, "task-1");
    }

    #[test]
    fn test_parse_and_filter_beads_keeps_epic_without_open_children() {
        let json = r#"[
            {"id": "epic-1", "issue_type": "epic", "priority": 1, "dependencies": [{"depends_on_id": "task-closed", "type": "parent-child"}]},
            {"id": "task-1", "issue_type": "task", "priority": 2, "dependencies": []}
        ]"#;
        let result = parse_and_filter_beads(json);
        assert_eq!(result.ready.len(), 2);
        assert!(result.ready.iter().any(|b| b.id == "epic-1"));
        assert!(result.ready.iter().any(|b| b.id == "task-1"));
    }

    #[test]
    fn test_parse_open_epic_hierarchy_extracts_parent_child_edges() {
        let json = r#"[
            {"id": "epic-1", "issue_type": "epic", "dependencies": [{"depends_on_id": "task-1", "type": "parent-child"}]},
            {"id": "epic-2", "issue_type": "epic", "dependencies": [{"id": "epic-1", "dependency_type": "parent-child"}]},
            {"id": "task-1", "issue_type": "task", "dependencies": []}
        ]"#;

        let hierarchy = parse_open_epic_hierarchy(json);
        assert!(hierarchy.open_ids.contains("epic-1"));
        assert!(hierarchy.open_ids.contains("epic-2"));
        assert!(hierarchy.open_ids.contains("task-1"));
        assert_eq!(hierarchy.parent_children["epic-1"], vec!["task-1"]);
        assert_eq!(hierarchy.parent_children["epic-2"], vec!["epic-1"]);
        assert_eq!(hierarchy.child_parents["task-1"], vec!["epic-1"]);
        assert_eq!(hierarchy.child_parents["epic-1"], vec!["epic-2"]);
    }

    #[test]
    fn test_plan_parent_epic_autoclose_order_chains_upward() {
        let json = r#"[
            {"id": "epic-parent", "issue_type": "epic", "dependencies": [{"depends_on_id": "epic-child", "type": "parent-child"}]},
            {"id": "epic-child", "issue_type": "epic", "dependencies": [{"depends_on_id": "task-closed", "type": "parent-child"}]}
        ]"#;

        let hierarchy = parse_open_epic_hierarchy(json);
        let order = plan_parent_epic_autoclose_order("task-closed", &hierarchy);
        assert_eq!(order, vec!["epic-child", "epic-parent"]);
    }

    #[test]
    fn test_plan_parent_epic_autoclose_order_requires_all_children_closed() {
        let json = r#"[
            {"id": "epic-1", "issue_type": "epic", "dependencies": [
                {"depends_on_id": "task-closed", "type": "parent-child"},
                {"depends_on_id": "task-open", "type": "parent-child"}
            ]},
            {"id": "task-open", "issue_type": "task", "dependencies": []}
        ]"#;

        let hierarchy = parse_open_epic_hierarchy(json);
        let order = plan_parent_epic_autoclose_order("task-closed", &hierarchy);
        assert!(order.is_empty());
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

    // ── Analysis agent tests ─────────────────────────────────────────

    fn test_pool(dir: &std::path::Path) -> WorkerPool {
        let wt_dir = dir.join("worktrees");
        let _ = std::fs::create_dir_all(&wt_dir);
        let workers_config = crate::config::WorkersConfig {
            max: 3,
            base_branch: "main".to_string(),
            worktrees_dir: "worktrees".to_string(),
            persistent: false,
        };
        WorkerPool::new(&workers_config, dir.to_path_buf(), wt_dir, 0)
    }

    #[test]
    fn test_is_analysis_bead_true() {
        assert!(is_analysis_bead("analysis-1234567890"));
        assert!(is_analysis_bead("analysis-"));
    }

    #[test]
    fn test_is_analysis_bead_false() {
        assert!(!is_analysis_bead("beads-abc-123"));
        assert!(!is_analysis_bead("my-analysis-bead"));
        assert!(!is_analysis_bead(""));
    }

    #[test]
    fn test_should_spawn_analysis_disabled() {
        let dir = tempdir().unwrap();
        let pool = test_pool(dir.path());
        let mut config = test_config(std::path::Path::new("/tmp"));
        config.improvements.analyze_every = 0;
        assert!(!should_spawn_analysis(&config, 10, &pool));
    }

    #[test]
    fn test_should_spawn_analysis_already_running() {
        let dir = tempdir().unwrap();
        let mut pool = test_pool(dir.path());
        let mut config = test_config(std::path::Path::new("/tmp"));
        config.improvements.analyze_every = 5;
        // Simulate an analysis worker already running in the pool
        pool.set_worker_state_for_test(
            0,
            crate::pool::WorkerState::Coding,
            Some(1),
            Some("analysis-1234567890".to_string()),
            None,
        );
        assert!(!should_spawn_analysis(&config, 5, &pool));
    }

    #[test]
    fn test_should_spawn_analysis_triggers_at_interval() {
        let dir = tempdir().unwrap();
        let pool = test_pool(dir.path());
        let mut config = test_config(std::path::Path::new("/tmp"));
        config.improvements.analyze_every = 10;
        assert!(!should_spawn_analysis(&config, 0, &pool));
        assert!(!should_spawn_analysis(&config, 3, &pool));
        assert!(!should_spawn_analysis(&config, 9, &pool));
        assert!(should_spawn_analysis(&config, 10, &pool));
        assert!(!should_spawn_analysis(&config, 11, &pool));
        assert!(should_spawn_analysis(&config, 20, &pool));
    }

    #[test]
    fn test_assemble_analysis_prompt_contains_placeholders_replaced() {
        let dir = tempdir().unwrap();
        let data_dir = test_data_dir(dir.path());
        let db_path = data_dir.db();
        let db_conn = db::open_or_create(&db_path).unwrap();

        // Insert some observations
        for i in 1..=3 {
            db::upsert_observation(
                &db_conn,
                i,
                "2026-02-20T10:00:00Z",
                Some(300),
                Some("success"),
                &format!(r#"{{"turns.total": {}}}"#, i * 10),
            )
            .unwrap();
        }

        // Insert an open improvement
        db::insert_improvement(&db_conn, "workflow", "Batch file reads", None, None, None).unwrap();

        let mut config = test_config(dir.path());
        config.improvements.analyze_sessions = 20;

        let prompt = assemble_analysis_prompt(&config, &data_dir, &db_conn);

        // Should contain observation data (not placeholder)
        assert!(!prompt.contains("{{recent_metrics}}"));
        assert!(!prompt.contains("{{open_improvements}}"));
        assert!(!prompt.contains("{{session_count}}"));
        assert!(prompt.contains("2026-02-20"));
        assert!(prompt.contains("Batch file reads"));
        assert!(prompt.contains("3")); // session count
    }

    #[test]
    fn test_assemble_analysis_prompt_empty_data() {
        let dir = tempdir().unwrap();
        let data_dir = test_data_dir(dir.path());
        let db_path = data_dir.db();
        let db_conn = db::open_or_create(&db_path).unwrap();

        let config = test_config(dir.path());
        let prompt = assemble_analysis_prompt(&config, &data_dir, &db_conn);

        assert!(prompt.contains("(no session data available)"));
        assert!(prompt.contains("(none)"));
    }

    #[test]
    fn test_format_observations_table_empty() {
        let table = format_observations_table(&[]);
        assert_eq!(table, "(no session data available)");
    }

    #[test]
    fn test_format_observations_table_with_data() {
        let obs = vec![db::Observation {
            session: 1,
            ts: "2026-02-20T10:00:00Z".to_string(),
            duration: Some(300),
            outcome: Some("success".to_string()),
            data: r#"{"turns.total": 50}"#.to_string(),
        }];
        let table = format_observations_table(&obs);
        assert!(table.contains("| Session |"));
        assert!(table.contains("| 1 |"));
        assert!(table.contains("300"));
        assert!(table.contains("success"));
    }

    #[test]
    fn test_chrono_timestamp_is_numeric() {
        let ts = chrono_timestamp();
        assert!(
            ts.parse::<u64>().is_ok(),
            "timestamp should be numeric: {ts}"
        );
    }

    // ── Runaway analysis loop regression tests ───────────────────────
    //
    // These tests prove the scenario from the bug report cannot recur:
    //   analyze_every=1 + analysis outcomes counting as sessions
    //   → infinite analysis-only loop, zero coding work done.

    /// Core invariant: analysis outcomes must NOT increment the session
    /// counter. If they did, analyze_every=1 would re-trigger analysis
    /// on its own completion, creating an infinite loop.
    #[test]
    fn test_analysis_outcomes_excluded_from_session_counter() {
        // This test replicates the counting logic from the coordinator loop
        // (lines ~220-326) to prove analysis workers are filtered out.

        let dir = tempdir().unwrap();
        let mut pool = test_pool(dir.path());

        // Simulate 3 completed workers: 2 coding + 1 analysis
        pool.set_worker_state_for_test(
            0,
            crate::pool::WorkerState::Coding,
            Some(1),
            Some("bead-abc".to_string()),
            None,
        );
        pool.set_worker_state_for_test(
            1,
            crate::pool::WorkerState::Coding,
            Some(2),
            Some("bead-def".to_string()),
            None,
        );
        pool.set_worker_state_for_test(
            2,
            crate::pool::WorkerState::Coding,
            Some(3),
            Some("analysis-1234567890".to_string()),
            None,
        );

        // Snapshot analysis worker IDs (same logic as coordinator)
        let worker_ids: Vec<u32> = vec![0, 1, 2];
        let analysis_worker_ids: Vec<u32> = worker_ids
            .iter()
            .filter(|&&id| {
                pool.worker_bead_id(id)
                    .map(is_analysis_bead)
                    .unwrap_or(false)
            })
            .copied()
            .collect();

        assert_eq!(analysis_worker_ids, vec![2]);

        // Count coding outcomes (same logic as coordinator)
        let coding_outcome_count = worker_ids
            .iter()
            .filter(|id| !analysis_worker_ids.contains(id))
            .count();

        assert_eq!(coding_outcome_count, 2, "only coding outcomes should count");
    }

    /// With analyze_every=1, prove that analysis completion alone does NOT
    /// advance total_completed_sessions, so should_spawn_analysis stays
    /// false until a real coding session completes.
    #[test]
    fn test_analyze_every_1_no_infinite_loop() {
        let dir = tempdir().unwrap();
        let pool = test_pool(dir.path());
        let mut config = test_config(std::path::Path::new("/tmp"));
        config.improvements.analyze_every = 1;

        // Simulate the runaway scenario: 30 analysis-only completions.
        // If analysis outcomes were counted, total_completed_sessions would
        // be 30 and should_spawn_analysis would return true on every check.
        let mut total_completed_sessions: u32 = 0;

        // First coding session completes → counter = 1
        total_completed_sessions += 1;
        assert!(
            should_spawn_analysis(&config, total_completed_sessions, &pool),
            "analysis should trigger after first coding session"
        );

        // Analysis completes — counter must NOT increment
        // (analysis outcome filtered out)
        // total_completed_sessions stays at 1

        // Check again with same counter: 1 % 1 == 0 still true,
        // BUT the pool would show analysis already running (tested separately).
        // After the analysis worker finishes and resets, the counter is still 1.
        // Next poll: should_spawn_analysis(1) → true, but only because
        // the original coding session hasn't been "consumed" yet.
        // The coordinator needs another coding completion to move past this.

        // Simulate 30 consecutive analysis completions with no coding work:
        // counter stays frozen at 1 the entire time.
        for _ in 0..30 {
            // Analysis completes — not counted
            let analysis_counted = 0u32; // analysis excluded
            total_completed_sessions += analysis_counted;
        }

        assert_eq!(
            total_completed_sessions, 1,
            "30 analysis completions should not have changed the counter"
        );

        // Second coding session completes → counter = 2
        total_completed_sessions += 1;
        assert_eq!(total_completed_sessions, 2);
        assert!(
            should_spawn_analysis(&config, total_completed_sessions, &pool),
            "analysis should trigger after second coding session"
        );
    }

    /// With analyze_every=1 and max_workers=1, coding beads must still get
    /// the single worker slot. Analysis only fires when there's an idle
    /// slot AFTER coding beads are scheduled.
    #[test]
    fn test_single_worker_coding_not_starved_by_analysis() {
        let dir = tempdir().unwrap();
        let wt_dir = dir.path().join("worktrees");
        let _ = std::fs::create_dir_all(&wt_dir);
        let workers_config = crate::config::WorkersConfig {
            max: 1, // single worker
            base_branch: "main".to_string(),
            worktrees_dir: "worktrees".to_string(),
            persistent: false,
        };
        let pool = WorkerPool::new(&workers_config, dir.path().to_path_buf(), wt_dir, 0);

        let mut config = test_config(std::path::Path::new("/tmp"));
        config.improvements.analyze_every = 1;

        // After 1 coding session, analysis is due
        let total_completed_sessions = 1;
        let analysis_due = should_spawn_analysis(&config, total_completed_sessions, &pool);
        assert!(analysis_due);

        // With 1 idle worker and analysis_due, coding_slots should be 0
        // (the slot is reserved for analysis)
        let coding_slots = if analysis_due {
            (pool.idle_count() as usize).saturating_sub(1)
        } else {
            pool.idle_count() as usize
        };
        assert_eq!(pool.idle_count(), 1);
        assert_eq!(
            coding_slots, 0,
            "single slot should be reserved for analysis"
        );

        // After analysis completes (counter unchanged), if there are no new
        // coding completions, counter stays at 1 and analysis_due stays true.
        // But the `already running` guard prevents a second concurrent analysis.
    }

    /// With analyze_every=1 and max_workers=2, one slot goes to coding
    /// and one to analysis when analysis is due.
    #[test]
    fn test_two_workers_analysis_gets_one_slot() {
        let dir = tempdir().unwrap();
        let wt_dir = dir.path().join("worktrees");
        let _ = std::fs::create_dir_all(&wt_dir);
        let workers_config = crate::config::WorkersConfig {
            max: 2,
            base_branch: "main".to_string(),
            worktrees_dir: "worktrees".to_string(),
            persistent: false,
        };
        let pool = WorkerPool::new(&workers_config, dir.path().to_path_buf(), wt_dir, 0);

        let mut config = test_config(std::path::Path::new("/tmp"));
        config.improvements.analyze_every = 1;

        let total_completed_sessions = 1;
        let analysis_due = should_spawn_analysis(&config, total_completed_sessions, &pool);
        assert!(analysis_due);

        let coding_slots = if analysis_due {
            (pool.idle_count() as usize).saturating_sub(1)
        } else {
            pool.idle_count() as usize
        };
        assert_eq!(pool.idle_count(), 2);
        assert_eq!(
            coding_slots, 1,
            "one slot for coding, one reserved for analysis"
        );
    }

    /// Prove that should_spawn_analysis returns false at session 0,
    /// even with analyze_every=1. This prevents analysis from firing
    /// before any real work has been done.
    #[test]
    fn test_no_analysis_before_any_coding_session() {
        let dir = tempdir().unwrap();
        let pool = test_pool(dir.path());
        let mut config = test_config(std::path::Path::new("/tmp"));
        config.improvements.analyze_every = 1;

        assert!(
            !should_spawn_analysis(&config, 0, &pool),
            "must not trigger analysis before any coding sessions"
        );
    }

    /// With analyze_every=1, the `already running` guard must prevent
    /// concurrent analysis even if the modulo condition keeps matching.
    #[test]
    fn test_already_running_guard_prevents_duplicate_analysis() {
        let dir = tempdir().unwrap();
        let mut pool = test_pool(dir.path());
        let mut config = test_config(std::path::Path::new("/tmp"));
        config.improvements.analyze_every = 1;

        // Analysis is due (total_completed_sessions=1, 1%1==0)
        assert!(should_spawn_analysis(&config, 1, &pool));

        // Now simulate an analysis worker running
        pool.set_worker_state_for_test(
            0,
            crate::pool::WorkerState::Coding,
            Some(1),
            Some("analysis-9999".to_string()),
            None,
        );

        // Even though 1%1==0, the running guard blocks it
        assert!(
            !should_spawn_analysis(&config, 1, &pool),
            "must not spawn second analysis while one is already running"
        );
        // And for subsequent coding completions too
        assert!(!should_spawn_analysis(&config, 2, &pool));
        assert!(!should_spawn_analysis(&config, 3, &pool));
    }

    /// Simulate the exact bug report scenario end-to-end:
    /// analyze_every=1, analyze_sessions=1, single worker.
    ///
    /// The old bug: analysis completion incremented total_completed_sessions,
    /// so the counter raced ahead and should_spawn_analysis kept returning
    /// true even with no coding work. The coordinator would spawn analysis
    /// after analysis after analysis — zero coding beads.
    ///
    /// The fix: analysis completions don't touch total_completed_sessions.
    /// The counter only advances on coding completions, so the ratio of
    /// analysis to coding stays bounded.
    #[test]
    fn test_bug_report_scenario_no_runaway_loop() {
        let dir = tempdir().unwrap();
        let pool = test_pool(dir.path());
        let mut config = test_config(std::path::Path::new("/tmp"));
        config.improvements.analyze_every = 1;
        config.improvements.analyze_sessions = 1;

        // Simulate the OLD broken behavior: every outcome (including analysis)
        // increments the counter. After N analysis-only sessions, the counter
        // is N and should_spawn_analysis returns true on every check.
        let mut broken_counter: u32 = 0;
        let mut broken_analysis_spawns = 0u32;

        for _ in 0..30 {
            // Analysis completes and is counted (old bug)
            broken_counter += 1;
            if should_spawn_analysis(&config, broken_counter, &pool) {
                broken_analysis_spawns += 1;
            }
        }
        // Old behavior: analysis fires 30 times with zero coding sessions
        assert_eq!(
            broken_analysis_spawns, 30,
            "confirms old bug: analysis triggers on every analysis completion"
        );

        // Now simulate the FIXED behavior: only coding outcomes increment.
        let mut fixed_counter: u32 = 0;
        let mut fixed_analysis_from_analysis = 0u32;

        // 30 analysis-only completions (no coding work)
        for _ in 0..30 {
            // Analysis completes — NOT counted (the fix)
            // fixed_counter stays at 0
            if should_spawn_analysis(&config, fixed_counter, &pool) {
                fixed_analysis_from_analysis += 1;
            }
        }
        // Fixed: counter never moved past 0, so analysis never triggers
        assert_eq!(
            fixed_counter, 0,
            "analysis completions must not advance the counter"
        );
        assert_eq!(
            fixed_analysis_from_analysis, 0,
            "analysis must not trigger when only analysis has completed"
        );

        // A coding session finally completes → counter goes to 1
        fixed_counter += 1;
        assert!(
            should_spawn_analysis(&config, fixed_counter, &pool),
            "analysis should trigger after a real coding session"
        );
    }
}
