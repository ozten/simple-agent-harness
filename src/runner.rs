/// Core loop: dispatch sessions, monitor, collect metrics, repeat.
///
/// The runner orchestrates the main iteration loop, coordinating
/// session spawning, watchdog monitoring, retry logic, and signal handling.
use crate::adapters;
use crate::commit;
use crate::compress;
use crate::config::HarnessConfig;
use crate::data_dir::DataDir;
use crate::db;
use crate::estimation;
use crate::hooks::{HookEnv, HookRunner};
use crate::ingest;
use crate::metrics::{EventLog, SessionEvent};
use crate::prompt;
use crate::ratelimit;
use crate::retention;
use crate::retry::{RetryDecision, RetryPolicy};
use crate::session::{self, SessionResult};
use crate::signals::SignalHandler;
use crate::status::{HarnessState, StatusTracker};
use crate::watchdog::{self, WatchdogOutcome};
use std::path::PathBuf;
use tokio::time::Duration;

/// Summary of the entire loop run, returned to main.
#[derive(Debug)]
pub struct RunSummary {
    /// Number of productive iterations completed.
    pub productive_iterations: u32,
    /// Global iteration counter at exit (persisted).
    pub global_iteration: u64,
    /// Why the loop stopped.
    pub exit_reason: ExitReason,
}

/// Why the loop stopped.
#[derive(Debug, PartialEq)]
pub enum ExitReason {
    /// Reached max_iterations productive iterations.
    MaxIterations,
    /// STOP file detected.
    StopFile,
    /// SIGINT or SIGTERM received.
    Signal,
    /// Prompt file could not be read.
    PromptError,
    /// Too many consecutive rate-limited sessions.
    RateLimited,
}

/// Run the main iteration loop.
///
/// Returns a summary of what happened, or an error if setup fails.
pub async fn run(
    config: &HarnessConfig,
    data_dir: &DataDir,
    signals: &SignalHandler,
    quiet: bool,
) -> RunSummary {
    let max_iterations = config.session.max_iterations;

    // Load or initialize the global iteration counter
    let counter_path = data_dir.counter();
    let mut global_iteration = load_counter(&counter_path);
    let mut productive = 0u32;
    let mut exit_reason = ExitReason::MaxIterations;

    let mut retry_policy = RetryPolicy::new(
        config.retry.max_empty_retries,
        config.watchdog.min_output_bytes,
    );
    let mut consecutive_errors = 0u32;
    const MAX_CONSECUTIVE_ERRORS: u32 = 5;
    let mut consecutive_rate_limits = 0u32;

    // Status file: write state transitions atomically
    let status_path = data_dir.status();
    let mut status = StatusTracker::new(status_path, max_iterations, global_iteration);
    status.update(HarnessState::Starting);

    // Event log: optional JSONL append-only log for external tooling
    let event_log = config
        .output
        .event_log
        .as_ref()
        .map(|p| EventLog::new(p.clone()));
    let mut retries_this_session = 0u32;

    // Metrics DB: open blacksmith.db for JSONL ingestion (non-fatal if it fails)
    let metrics_db = {
        let db_path = data_dir.db();
        match db::open_or_create(&db_path) {
            Ok(conn) => {
                tracing::debug!(path = %db_path.display(), "metrics database opened");
                Some(conn)
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to open metrics database, ingestion disabled");
                None
            }
        }
    };

    // Hook runner: pre/post-session shell commands
    let hook_runner = HookRunner::new(
        config.hooks.pre_session.clone(),
        config.hooks.post_session.clone(),
    );

    // Compile commit detection patterns once at startup
    let commit_patterns = match commit::compile_patterns(&config.commit_detection.patterns) {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(error = %e, "invalid commit detection pattern, disabling commit detection");
            vec![]
        }
    };

    // Compile metrics extraction rules once at startup
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

    // Resolve the coding agent config (used for the main loop)
    let resolved_agent = config.agent.resolved_coding();

    // Create adapter for JSONL metric extraction
    let adapter_name =
        adapters::resolve_adapter_name(resolved_agent.adapter.as_deref(), &resolved_agent.command);
    let adapter = adapters::create_adapter(adapter_name);
    tracing::info!(adapter = adapter_name, "using agent adapter");

    tracing::info!(max_iterations, global_iteration, "starting iteration loop");

    while productive < max_iterations {
        // 1. Check STOP file
        if signals
            .check_stop_file(&config.shutdown.stop_file)
            .is_detected()
        {
            tracing::info!("STOP file detected, exiting loop");
            exit_reason = ExitReason::StopFile;
            status.update(HarnessState::ShuttingDown);
            break;
        }

        // Check for signal-based shutdown
        if signals.shutdown_requested() {
            tracing::info!("shutdown requested, exiting loop");
            exit_reason = ExitReason::Signal;
            status.update(HarnessState::ShuttingDown);
            break;
        }

        // 2. Cleanup stale sessions (retention policy enforcement)
        retention::enforce_retention(&data_dir.sessions_dir(), &config.storage.retention);

        // 3. Assemble prompt (read file + run prepend_commands + brief injection)
        let targets = &config.metrics.targets;
        let targets_opt = if targets.rules.is_empty() {
            None
        } else {
            Some(targets)
        };
        let supported = adapter.supported_metrics();
        let supported_opt: Option<&[&str]> = if supported.is_empty() {
            None
        } else {
            Some(supported)
        };
        let prompt =
            match prompt::assemble(&config.prompt, &config.session.prompt_file, &data_dir.db(), targets_opt, supported_opt) {
                Ok(p) => p,
                Err(e) => {
                    tracing::error!(error = %e, "failed to assemble prompt");
                    exit_reason = ExitReason::PromptError;
                    status.update(HarnessState::ShuttingDown);
                    break;
                }
            };

        // 3. Compute output file path for this global iteration
        let output_path = data_dir.session_file(global_iteration as u32);

        // Ensure output directory exists
        if let Some(parent) = output_path.parent() {
            if !parent.exists() {
                if let Err(e) = std::fs::create_dir_all(parent) {
                    tracing::error!(error = %e, path = %parent.display(), "failed to create output directory");
                    exit_reason = ExitReason::PromptError;
                    status.update(HarnessState::ShuttingDown);
                    break;
                }
            }
        }

        tracing::info!(
            iteration = productive,
            global = global_iteration,
            output = %output_path.display(),
            "starting iteration"
        );

        // Run pre-session hooks
        if !config.hooks.pre_session.is_empty() {
            status.set_iteration(productive);
            status.set_global_iteration(global_iteration);
            status.update(HarnessState::PreHooks);

            let pre_env = HookEnv::pre_session(
                productive,
                global_iteration,
                &config.session.prompt_file.display().to_string(),
            );
            if let Err(e) = hook_runner.run_pre_session(&pre_env) {
                tracing::error!(error = %e, "pre-session hook failed, skipping iteration");
                consecutive_errors += 1;
                global_iteration += 1;
                save_counter(&counter_path, global_iteration);
                status.set_global_iteration(global_iteration);
                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                    tracing::error!(
                        consecutive_errors,
                        "too many consecutive hook failures, exiting"
                    );
                    exit_reason = ExitReason::PromptError;
                    status.update(HarnessState::ShuttingDown);
                    break;
                }
                continue;
            }
        }

        // Update status: session running
        status.set_iteration(productive);
        status.set_global_iteration(global_iteration);
        status.set_output_file(&output_path.display().to_string());
        status.set_output_bytes(0);
        status.set_session_start();
        status.update(HarnessState::SessionRunning);

        // 4. Spawn session + watchdog
        let session_result =
            run_session_with_watchdog(config, &output_path, &prompt, signals).await;

        let result = match session_result {
            Ok(r) => {
                consecutive_errors = 0;
                r
            }
            Err(e) => {
                tracing::error!(error = %e, "session failed");
                consecutive_errors += 1;
                global_iteration += 1;
                save_counter(&counter_path, global_iteration);
                status.set_global_iteration(global_iteration);
                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                    tracing::error!(
                        consecutive_errors,
                        "too many consecutive session errors, exiting"
                    );
                    exit_reason = ExitReason::PromptError;
                    status.update(HarnessState::ShuttingDown);
                    break;
                }
                retry_policy.reset();
                continue;
            }
        };

        // Update output bytes in status
        status.set_output_bytes(result.output_bytes);

        tracing::info!(
            exit_code = ?result.exit_code,
            output_bytes = result.output_bytes,
            duration_secs = result.duration.as_secs(),
            pid = result.pid,
            "session completed"
        );

        // 5. Evaluate retry policy
        let decision = retry_policy.evaluate(result.output_bytes);
        match decision {
            RetryDecision::Proceed => {
                // Check for rate limiting in session output
                let rate_limited = ratelimit::detect_rate_limit(&output_path);

                // Check for commit indicators in session output
                let committed = commit::detect_commit(&output_path, &commit_patterns);
                if committed {
                    tracing::info!("commit detected in session output");
                }

                // Emit event log entry
                if let Some(ref log) = event_log {
                    let event = SessionEvent::session_complete(
                        productive,
                        global_iteration,
                        result.output_bytes,
                        result.exit_code,
                        result.duration.as_secs(),
                        committed,
                        retries_this_session,
                        rate_limited,
                    );
                    if let Err(e) = log.append(&event) {
                        tracing::warn!(error = %e, "failed to write event log");
                    }
                }

                // Ingest JSONL metrics into database
                let mut ingest_result: Option<ingest::IngestResult> = None;
                if let Some(ref conn) = metrics_db {
                    match ingest::ingest_session_with_rules(
                        conn,
                        global_iteration as i64,
                        &output_path,
                        result.exit_code,
                        &extraction_rules,
                        adapter.as_ref(),
                    ) {
                        Ok(m) => {
                            tracing::info!(
                                session = global_iteration,
                                turns = m.turns_total,
                                cost_usd = format!("{:.4}", m.cost_estimate_usd),
                                "JSONL metrics ingested"
                            );
                            ingest_result = Some(m);
                        }
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                session = global_iteration,
                                "failed to ingest JSONL metrics"
                            );
                        }
                    }
                }

                // After-ingest retention: delete immediately, skip compression
                if config.storage.retention == crate::config::RetentionPolicy::AfterIngest {
                    retention::delete_after_ingest(&output_path);
                } else {
                    // Compress old session files after ingestion
                    compress::compress_old_sessions(
                        &data_dir.sessions_dir(),
                        global_iteration,
                        config.storage.compress_after,
                    );
                }

                // Run post-session hooks
                if !config.hooks.post_session.is_empty() {
                    status.update(HarnessState::PostHooks);
                    let post_env = HookEnv::post_session(
                        productive,
                        global_iteration,
                        &config.session.prompt_file.display().to_string(),
                        &output_path.display().to_string(),
                        result.exit_code,
                        result.output_bytes,
                        result.duration.as_secs(),
                        committed,
                    );
                    hook_runner.run_post_session(&post_env);
                }

                if rate_limited {
                    // Rate-limited: do NOT increment productive counter
                    consecutive_rate_limits += 1;
                    global_iteration += 1;
                    retry_policy.reset();
                    retries_this_session = 0;
                    save_counter(&counter_path, global_iteration);

                    status.set_global_iteration(global_iteration);
                    status.set_consecutive_rate_limits(consecutive_rate_limits);

                    let delay = ratelimit::backoff_delay(
                        config.backoff.initial_delay_secs,
                        consecutive_rate_limits,
                        config.backoff.max_delay_secs,
                    );

                    tracing::warn!(
                        consecutive = consecutive_rate_limits,
                        max = config.backoff.max_consecutive_rate_limits,
                        backoff_secs = delay,
                        "rate limit detected in session output"
                    );

                    if consecutive_rate_limits >= config.backoff.max_consecutive_rate_limits {
                        tracing::error!(
                            consecutive_rate_limits,
                            "max consecutive rate limits reached, exiting"
                        );
                        exit_reason = ExitReason::RateLimited;
                        status.update(HarnessState::ShuttingDown);
                        break;
                    }

                    // Apply exponential backoff
                    status.update(HarnessState::RateLimitedBackoff);
                    if delay > 0 {
                        tracing::info!(delay_secs = delay, "rate limit backoff delay");
                        tokio::time::sleep(Duration::from_secs(delay)).await;
                    }
                    continue;
                }

                // Successful (non-rate-limited) session: reset rate limit counter
                consecutive_rate_limits = 0;
                status.set_consecutive_rate_limits(0);

                productive += 1;
                global_iteration += 1;
                retry_policy.reset();
                retries_this_session = 0;
                save_counter(&counter_path, global_iteration);

                status.set_iteration(productive);
                status.set_global_iteration(global_iteration);
                status.set_last_completed(global_iteration - 1);
                status.set_last_committed(committed);
                status.update(HarnessState::Idle);

                // Print progress line after productive iteration
                print_progress_line(
                    global_iteration - 1,
                    &result,
                    ingest_result.as_ref(),
                    metrics_db.as_ref(),
                    config.workers.max,
                );

                tracing::info!(
                    productive = productive,
                    max = max_iterations,
                    global = global_iteration,
                    committed,
                    "productive iteration completed"
                );
            }
            RetryDecision::Retry { attempt } => {
                tracing::warn!(
                    attempt,
                    max = config.retry.max_empty_retries,
                    "retrying empty session"
                );
                retries_this_session += 1;
                global_iteration += 1;
                save_counter(&counter_path, global_iteration);
                status.set_global_iteration(global_iteration);
                status.update(HarnessState::Retrying);

                // Delay before retry
                if config.retry.retry_delay_secs > 0 {
                    tokio::time::sleep(Duration::from_secs(config.retry.retry_delay_secs)).await;
                }
                continue; // Skip the inter-iteration delay
            }
            RetryDecision::Skip => {
                // Emit event log entry for skipped (empty) session
                if let Some(ref log) = event_log {
                    let event = SessionEvent::session_complete(
                        productive,
                        global_iteration,
                        result.output_bytes,
                        result.exit_code,
                        result.duration.as_secs(),
                        false,
                        retries_this_session,
                        false,
                    );
                    if let Err(e) = log.append(&event) {
                        tracing::warn!(error = %e, "failed to write event log");
                    }
                }

                // Ingest JSONL metrics for skipped session (still has data)
                if let Some(ref conn) = metrics_db {
                    if let Err(e) = ingest::ingest_session_with_rules(
                        conn,
                        global_iteration as i64,
                        &output_path,
                        result.exit_code,
                        &extraction_rules,
                        adapter.as_ref(),
                    ) {
                        tracing::warn!(
                            error = %e,
                            session = global_iteration,
                            "failed to ingest JSONL metrics for skipped session"
                        );
                    }
                }

                // Run post-session hooks (even for skip — session ran, just empty)
                if !config.hooks.post_session.is_empty() {
                    status.update(HarnessState::PostHooks);
                    let post_env = HookEnv::post_session(
                        productive,
                        global_iteration,
                        &config.session.prompt_file.display().to_string(),
                        &output_path.display().to_string(),
                        result.exit_code,
                        result.output_bytes,
                        result.duration.as_secs(),
                        false, // empty session — no commit possible
                    );
                    hook_runner.run_post_session(&post_env);
                }

                tracing::warn!("skipping iteration after exhausting retries");
                productive += 1;
                global_iteration += 1;
                retry_policy.reset();
                retries_this_session = 0;
                save_counter(&counter_path, global_iteration);

                status.set_iteration(productive);
                status.set_global_iteration(global_iteration);
                status.set_last_completed(global_iteration - 1);
                status.update(HarnessState::Idle);
            }
        }

        // 6. Check for shutdown before delay
        if signals.shutdown_requested() {
            tracing::info!("shutdown requested after session, exiting loop");
            exit_reason = ExitReason::Signal;
            status.update(HarnessState::ShuttingDown);
            break;
        }

        // 7. Inter-iteration delay (only between productive iterations)
        if productive < max_iterations && config.backoff.initial_delay_secs > 0 {
            tracing::debug!(
                delay_secs = config.backoff.initial_delay_secs,
                "inter-iteration delay"
            );
            status.update(HarnessState::Idle);
            tokio::time::sleep(Duration::from_secs(config.backoff.initial_delay_secs)).await;
        }
    }

    tracing::info!(
        productive,
        global = global_iteration,
        reason = ?exit_reason,
        "loop finished"
    );

    // In quiet mode, print a one-line summary to stdout (bypasses tracing filter)
    if quiet {
        println!(
            "blacksmith: {productive}/{max_iterations} productive, global={global_iteration}, exit={exit_reason:?}"
        );
    }

    // Clean up status file on exit
    status.remove();

    RunSummary {
        productive_iterations: productive,
        global_iteration,
        exit_reason,
    }
}

/// Print a progress line after each productive iteration.
///
/// Format: `[iter N] completed in Xm (Y turns) | Progress: A/B beads | avg Xm/bead | ETA: ~Zm`
fn print_progress_line(
    iteration: u64,
    result: &SessionResult,
    ingest_result: Option<&ingest::IngestResult>,
    conn: Option<&rusqlite::Connection>,
    workers: u32,
) {
    let duration_secs = result.duration.as_secs();
    let duration_str = format_duration_secs(duration_secs);

    let turns_str = ingest_result
        .map(|m| format!("{} turns", m.turns_total))
        .unwrap_or_default();

    // Build the session summary part
    let session_part = if turns_str.is_empty() {
        format!("[iter {}] completed in {}", iteration, duration_str)
    } else {
        format!(
            "[iter {}] completed in {} ({})",
            iteration, duration_str, turns_str
        )
    };

    // Build the progress + ETA part from bead metrics
    let progress_part = conn
        .and_then(|c| build_progress_string(c, workers))
        .unwrap_or_default();

    if progress_part.is_empty() {
        println!("{}", session_part);
    } else {
        println!("{} | {}", session_part, progress_part);
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

/// Spawn a session and its watchdog concurrently, handling force-kill.
async fn run_session_with_watchdog(
    config: &HarnessConfig,
    output_path: &PathBuf,
    prompt: &str,
    signals: &SignalHandler,
) -> Result<SessionResult, session::SessionError> {
    let resolved_agent = config.agent.resolved_coding();

    // We need to create the output file first for the watchdog to monitor
    let session_fut = session::run_session(&config.agent, output_path, prompt);

    // Pin the session future so we can use it in select!
    tokio::pin!(session_fut);

    // We can't start the watchdog until we know the child PID, but run_session
    // internally spawns and waits. So we run session and watchdog concurrently
    // using select! — the watchdog monitors the output file for growth.
    //
    // However, we don't know the PID before run_session starts internally.
    // The simplest approach: run_session handles spawn+wait, watchdog monitors
    // the file. If watchdog kills the process group, run_session's wait() will
    // return with a signal exit code.
    //
    // We use a dummy PID for the watchdog since the child creates its own
    // process group. We need to get the PID from the session...
    //
    // Actually, let's just run the session. If the output grows, the watchdog
    // resets. If stale, the watchdog kills the process group.
    // Since run_session spawns with process_group(0), the child PID IS the PGID.
    //
    // For the MVP, we run session in a way where we start monitoring the output
    // file after spawn. But run_session encapsulates the whole lifecycle.
    // The correct approach: tokio::select! between session completion and
    // watchdog kill, using the output file as the shared state.

    // Start a separate task that will begin watching as soon as the file exists
    let watchdog_config = config.watchdog.clone();
    let watchdog_path = output_path.clone();

    // We need to know when the session is done to stop the watchdog.
    // Use select!: session finishes → cancel watchdog, or watchdog kills → session returns with signal.

    // First, actually start session and watchdog concurrently.
    // The challenge: we need child PID for watchdog, but run_session encapsulates spawning.
    // Solution: use a "file-only" watchdog that doesn't need the PID to monitor,
    // but does need it to kill. Since run_session spawns with process_group(0),
    // the child is its own PGID.
    //
    // For now, we accept that the watchdog monitors via the output file and kills
    // via the PID. We'll refactor session to return PID early if needed.
    //
    // Pragmatic approach for MVP: run session, let the watchdog run alongside
    // using a spawned task that reads the output file. The session future completes
    // when the child exits (naturally or killed by watchdog).

    // Create output file so watchdog can start monitoring immediately
    if !output_path.exists() {
        let _ = std::fs::File::create(output_path);
    }

    // We need to run session and get PID. Let's refactor to a simpler approach:
    // Just run the session. The watchdog needs a PID. We'll spawn the child
    // ourselves here instead of delegating entirely to session::run_session.
    //
    // Actually, the simplest correct MVP: run_session blocks until done.
    // We wrap it with a timeout based on stale_timeout_mins.
    // But that's not the same as monitoring growth — it's just a hard timeout.
    //
    // Let's take the correct approach: spawn child, get PID, then select!
    // between child.wait() and watchdog::monitor().

    // Spawn the child directly (duplicating a bit of session logic for control)
    let output_file =
        std::fs::File::create(output_path).map_err(|e| session::SessionError::OutputFile {
            path: output_path.to_path_buf(),
            source: e,
        })?;
    let output_file_stderr =
        output_file
            .try_clone()
            .map_err(|e| session::SessionError::OutputFile {
                path: output_path.to_path_buf(),
                source: e,
            })?;

    // For file mode: write prompt to a temp file
    let prompt_file = if resolved_agent.prompt_via == crate::config::PromptVia::File {
        let tmp =
            tempfile::NamedTempFile::new().map_err(|e| session::SessionError::Io { source: e })?;
        std::fs::write(tmp.path(), prompt).map_err(|e| session::SessionError::Io { source: e })?;
        Some(tmp)
    } else {
        None
    };

    let args = resolved_agent
        .args
        .iter()
        .map(|arg| {
            let mut result = arg.replace("{prompt}", prompt);
            if let Some(ref pf) = prompt_file {
                result = result.replace("{prompt_file}", &pf.path().display().to_string());
            }
            result
        })
        .collect::<Vec<_>>();

    // For stdin mode, pipe stdin instead of null
    let stdin_mode = if resolved_agent.prompt_via == crate::config::PromptVia::Stdin {
        std::process::Stdio::piped()
    } else {
        std::process::Stdio::null()
    };

    let start = std::time::Instant::now();

    let mut child = tokio::process::Command::new(&resolved_agent.command)
        .args(&args)
        .stdin(stdin_mode)
        .stdout(std::process::Stdio::from(output_file))
        .stderr(std::process::Stdio::from(output_file_stderr))
        .process_group(0)
        .spawn()
        .map_err(|e| session::SessionError::Spawn { source: e })?;

    // For stdin mode: write the prompt to the child's stdin, then close it
    if resolved_agent.prompt_via == crate::config::PromptVia::Stdin {
        if let Some(mut stdin) = child.stdin.take() {
            use tokio::io::AsyncWriteExt;
            stdin
                .write_all(prompt.as_bytes())
                .await
                .map_err(|e| session::SessionError::Io { source: e })?;
            // Dropping stdin closes the pipe, signaling EOF to the child
        }
    }

    let pid = child.id().unwrap_or(0);
    tracing::info!(pid, "agent subprocess started");

    // Register PID with signal handler for force-kill
    signals.set_child_pid(pid as i32);

    // Race: child completion vs watchdog vs force-kill signal
    let watchdog_fut = watchdog::monitor(&watchdog_config, &watchdog_path, pid);

    let exit_status;
    let was_killed;

    tokio::select! {
        status = child.wait() => {
            // Child exited naturally (or was killed externally)
            exit_status = status.map_err(|e| session::SessionError::Io { source: e })?;
            was_killed = false;
        }
        outcome = watchdog_fut => {
            // Watchdog triggered — child should be dead or dying
            was_killed = outcome == WatchdogOutcome::Killed;
            if was_killed {
                tracing::warn!(pid, "watchdog killed session");
            }
            // Collect the child's exit status
            exit_status = child.wait().await.map_err(|e| session::SessionError::Io { source: e })?;
        }
        _ = signals.wait_for_force_kill() => {
            // Force-kill requested (double SIGINT)
            tracing::warn!(pid, "force-kill requested, session terminated");
            exit_status = child.wait().await.map_err(|e| session::SessionError::Io { source: e })?;
            was_killed = true;
        }
    }

    signals.clear_child_pid();

    // Clean up temp prompt file if created
    drop(prompt_file);

    let duration = start.elapsed();
    let output_bytes = std::fs::metadata(output_path).map(|m| m.len()).unwrap_or(0);

    let exit_code = if was_killed && exit_status.code().is_none() {
        Some(124) // Timeout convention
    } else {
        exit_status.code()
    };

    tracing::info!(
        exit_code = ?exit_code,
        output_bytes,
        duration_secs = duration.as_secs(),
        was_killed,
        "session finished"
    );

    Ok(SessionResult {
        exit_code,
        output_bytes,
        duration,
        output_file: output_path.to_path_buf(),
        pid,
    })
}

/// Load the global iteration counter from a file. Returns 0 if the file
/// doesn't exist or can't be parsed.
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

/// Extension trait to check StopFileStatus in a readable way.
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
    use std::path::PathBuf;
    use tempfile::tempdir;

    /// Helper to create a minimal test config with a fast command.
    fn test_config(dir: &std::path::Path, command: &str, args: Vec<String>) -> HarnessConfig {
        HarnessConfig {
            session: SessionConfig {
                max_iterations: 3,
                prompt_file: dir.join("prompt.md"),
                output_dir: dir.to_path_buf(),
                output_prefix: "test-iter".to_string(),
                counter_file: dir.join(".counter"),
            },
            agent: AgentConfig {
                command: command.to_string(),
                args,
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
            workers: WorkersConfig::default(),
            reconciliation: ReconciliationConfig::default(),
            architecture: ArchitectureConfig::default(),
        }
    }

    /// Helper to create a DataDir for tests, using a subdirectory of the temp dir.
    fn test_data_dir(dir: &std::path::Path) -> DataDir {
        let dd = DataDir::new(dir.join(".blacksmith"));
        dd.init().unwrap();
        dd
    }

    /// Helper to create a SignalHandler for tests.
    fn make_signals() -> SignalHandler {
        SignalHandler::install()
    }

    #[test]
    fn test_load_counter_nonexistent() {
        assert_eq!(load_counter(&PathBuf::from("/nonexistent/.counter")), 0);
    }

    #[test]
    fn test_load_counter_valid() {
        let dir = tempdir().unwrap();
        let path = dir.path().join(".counter");
        std::fs::write(&path, "42").unwrap();
        assert_eq!(load_counter(&path), 42);
    }

    #[test]
    fn test_load_counter_with_whitespace() {
        let dir = tempdir().unwrap();
        let path = dir.path().join(".counter");
        std::fs::write(&path, "  7\n").unwrap();
        assert_eq!(load_counter(&path), 7);
    }

    #[test]
    fn test_load_counter_invalid() {
        let dir = tempdir().unwrap();
        let path = dir.path().join(".counter");
        std::fs::write(&path, "not_a_number").unwrap();
        assert_eq!(load_counter(&path), 0);
    }

    #[test]
    fn test_save_counter() {
        let dir = tempdir().unwrap();
        let path = dir.path().join(".counter");
        save_counter(&path, 99);
        let contents = std::fs::read_to_string(&path).unwrap();
        assert_eq!(contents, "99");
    }

    #[test]
    fn test_save_and_load_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join(".counter");
        save_counter(&path, 12345);
        assert_eq!(load_counter(&path), 12345);
    }

    #[tokio::test]
    async fn test_run_three_productive_iterations() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path(), "echo", vec!["hello from iteration".to_string()]);

        // Write prompt file
        std::fs::write(&config.session.prompt_file, "test prompt").unwrap();

        let signals = make_signals();
        let data_dir = test_data_dir(dir.path());
        let summary = run(&config, &data_dir, &signals, false).await;

        assert_eq!(summary.productive_iterations, 3);
        assert_eq!(summary.exit_reason, ExitReason::MaxIterations);
        assert_eq!(summary.global_iteration, 3);

        // Verify counter file was persisted
        assert_eq!(load_counter(&data_dir.counter()), 3);

        // Verify output files were created
        for i in 0..3 {
            let path = dir.path().join(format!(".blacksmith/sessions/{}.jsonl", i));
            assert!(path.exists(), "output file {} should exist", i);
        }
    }

    #[tokio::test]
    async fn test_run_stops_on_stop_file() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path(), "echo", vec!["hello".to_string()]);
        std::fs::write(&config.session.prompt_file, "test prompt").unwrap();

        // Create STOP file before starting
        std::fs::write(&config.shutdown.stop_file, "").unwrap();

        let signals = make_signals();
        let data_dir = test_data_dir(dir.path());
        let summary = run(&config, &data_dir, &signals, false).await;

        assert_eq!(summary.productive_iterations, 0);
        assert_eq!(summary.exit_reason, ExitReason::StopFile);
    }

    #[tokio::test]
    async fn test_run_stops_on_signal() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path(), "echo", vec!["hello".to_string()]);
        std::fs::write(&config.session.prompt_file, "test prompt").unwrap();

        let signals = make_signals();
        // Request shutdown before starting
        signals.request_shutdown();

        let data_dir = test_data_dir(dir.path());
        let summary = run(&config, &data_dir, &signals, false).await;

        assert_eq!(summary.productive_iterations, 0);
        assert_eq!(summary.exit_reason, ExitReason::Signal);
    }

    #[tokio::test]
    async fn test_run_fails_on_missing_prompt() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path(), "echo", vec!["hello".to_string()]);
        // Don't create prompt file

        let signals = make_signals();
        let data_dir = test_data_dir(dir.path());
        let summary = run(&config, &data_dir, &signals, false).await;

        assert_eq!(summary.productive_iterations, 0);
        assert_eq!(summary.exit_reason, ExitReason::PromptError);
    }

    #[tokio::test]
    async fn test_run_retries_empty_sessions() {
        let dir = tempdir().unwrap();
        // Use 'true' which exits 0 but produces no output
        let mut config = test_config(dir.path(), "true", vec![]);
        config.session.max_iterations = 1;
        config.retry.max_empty_retries = 2;
        config.watchdog.min_output_bytes = 100; // 'true' produces 0 bytes

        std::fs::write(&config.session.prompt_file, "test prompt").unwrap();

        let signals = make_signals();
        let data_dir = test_data_dir(dir.path());
        let summary = run(&config, &data_dir, &signals, false).await;

        // Should have exhausted retries (2 retries) then skipped, counting as 1 productive
        assert_eq!(summary.productive_iterations, 1);
        assert_eq!(summary.exit_reason, ExitReason::MaxIterations);
        // global counter: 1 original + 2 retries + 1 skip = 3? No:
        // attempt 0 (empty) -> retry, global++ (1)
        // attempt 1 (empty) -> retry, global++ (2)
        // attempt 2 (empty) -> skip, productive++, global++ (3)
        assert_eq!(summary.global_iteration, 3);
    }

    #[tokio::test]
    async fn test_run_resumes_from_existing_counter() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path(), "echo", vec!["hello".to_string()]);
        std::fs::write(&config.session.prompt_file, "test prompt").unwrap();

        let data_dir = test_data_dir(dir.path());

        // Pre-set counter to 100 (in the data dir)
        std::fs::write(data_dir.counter(), "100").unwrap();

        let signals = make_signals();
        let summary = run(&config, &data_dir, &signals, false).await;

        assert_eq!(summary.productive_iterations, 3);
        assert_eq!(summary.global_iteration, 103);

        // Verify output files use global counter
        for i in 100..103 {
            let path = dir.path().join(format!(".blacksmith/sessions/{}.jsonl", i));
            assert!(path.exists(), "output file {} should exist", i);
        }
    }

    #[tokio::test]
    async fn test_run_with_single_iteration() {
        let dir = tempdir().unwrap();
        let mut config = test_config(dir.path(), "echo", vec!["single run".to_string()]);
        config.session.max_iterations = 1;

        std::fs::write(&config.session.prompt_file, "test prompt").unwrap();

        let signals = make_signals();
        let data_dir = test_data_dir(dir.path());
        let summary = run(&config, &data_dir, &signals, false).await;

        assert_eq!(summary.productive_iterations, 1);
        assert_eq!(summary.exit_reason, ExitReason::MaxIterations);
        assert_eq!(summary.global_iteration, 1);
    }

    #[tokio::test]
    async fn test_run_writes_event_log() {
        let dir = tempdir().unwrap();
        let event_log_path = dir.path().join("harness-events.jsonl");
        let mut config = test_config(dir.path(), "echo", vec!["hello from iteration".to_string()]);
        config.output.event_log = Some(event_log_path.clone());

        std::fs::write(&config.session.prompt_file, "test prompt").unwrap();

        let signals = make_signals();
        let data_dir = test_data_dir(dir.path());
        let summary = run(&config, &data_dir, &signals, false).await;

        assert_eq!(summary.productive_iterations, 3);

        // Verify event log was created with 3 entries
        assert!(event_log_path.exists(), "event log should be created");
        let contents = std::fs::read_to_string(&event_log_path).unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(
            lines.len(),
            3,
            "should have one event per productive iteration"
        );

        // Each line should be valid JSON with expected fields
        for (i, line) in lines.iter().enumerate() {
            let parsed: serde_json::Value = serde_json::from_str(line).unwrap();
            assert_eq!(parsed["event"], "session_complete");
            assert_eq!(parsed["iteration"], i as u64);
            assert_eq!(parsed["global"], i as u64);
            assert!(parsed["output_bytes"].as_u64().unwrap() > 0);
            assert_eq!(parsed["exit_code"], 0);
            assert!(parsed["ts"].is_string());
            assert_eq!(parsed["retries"], 0);
        }
    }

    #[tokio::test]
    async fn test_run_no_event_log_when_not_configured() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path(), "echo", vec!["hello".to_string()]);
        // output.event_log is None by default

        std::fs::write(&config.session.prompt_file, "test prompt").unwrap();

        let signals = make_signals();
        let data_dir = test_data_dir(dir.path());
        let summary = run(&config, &data_dir, &signals, false).await;

        assert_eq!(summary.productive_iterations, 3);

        // No event log file should exist
        let event_log_path = dir.path().join("harness-events.jsonl");
        assert!(
            !event_log_path.exists(),
            "no event log should be created when not configured"
        );
    }

    #[tokio::test]
    async fn test_pre_session_hooks_run_with_env_vars() {
        let dir = tempdir().unwrap();
        let marker = dir.path().join("pre_hook_ran");
        let mut config = test_config(dir.path(), "echo", vec!["hello".to_string()]);
        config.hooks.pre_session = vec![format!(
            "echo $HARNESS_ITERATION:$HARNESS_GLOBAL_ITERATION:$HARNESS_PROMPT_FILE > {}",
            marker.display()
        )];
        config.session.max_iterations = 1;

        std::fs::write(&config.session.prompt_file, "test prompt").unwrap();

        let signals = make_signals();
        let data_dir = test_data_dir(dir.path());
        let summary = run(&config, &data_dir, &signals, false).await;

        assert_eq!(summary.productive_iterations, 1);
        assert!(marker.exists(), "pre-session hook should have run");
        let contents = std::fs::read_to_string(&marker).unwrap();
        let parts: Vec<&str> = contents.trim().split(':').collect();
        assert_eq!(parts[0], "0"); // HARNESS_ITERATION
        assert_eq!(parts[1], "0"); // HARNESS_GLOBAL_ITERATION
        assert!(parts[2].contains("prompt.md")); // HARNESS_PROMPT_FILE
    }

    #[tokio::test]
    async fn test_pre_session_hook_failure_skips_iteration() {
        let dir = tempdir().unwrap();
        let mut config = test_config(dir.path(), "echo", vec!["hello".to_string()]);
        // Pre-session hook always fails
        config.hooks.pre_session = vec!["false".to_string()];
        config.session.max_iterations = 3;

        std::fs::write(&config.session.prompt_file, "test prompt").unwrap();

        let signals = make_signals();
        let data_dir = test_data_dir(dir.path());
        let summary = run(&config, &data_dir, &signals, false).await;

        // All iterations skipped due to hook failure; exits after MAX_CONSECUTIVE_ERRORS (5)
        assert_eq!(summary.productive_iterations, 0);
        // Exits due to consecutive errors (same exit path as repeated session errors)
        assert_eq!(summary.exit_reason, ExitReason::PromptError);
        assert_eq!(summary.global_iteration, 5); // 5 consecutive hook failures
    }

    #[tokio::test]
    async fn test_post_session_hooks_run_with_env_vars() {
        let dir = tempdir().unwrap();
        let marker = dir.path().join("post_hook_ran");
        let mut config = test_config(dir.path(), "echo", vec!["hello".to_string()]);
        config.hooks.post_session = vec![format!(
            "echo $HARNESS_OUTPUT_FILE:$HARNESS_EXIT_CODE:$HARNESS_OUTPUT_BYTES > {}",
            marker.display()
        )];
        config.session.max_iterations = 1;

        std::fs::write(&config.session.prompt_file, "test prompt").unwrap();

        let signals = make_signals();
        let data_dir = test_data_dir(dir.path());
        let summary = run(&config, &data_dir, &signals, false).await;

        assert_eq!(summary.productive_iterations, 1);
        assert!(marker.exists(), "post-session hook should have run");
        let contents = std::fs::read_to_string(&marker).unwrap();
        let parts: Vec<&str> = contents.trim().split(':').collect();
        assert!(parts[0].contains("sessions/0.jsonl")); // HARNESS_OUTPUT_FILE
        assert_eq!(parts[1], "0"); // HARNESS_EXIT_CODE
        assert!(parts[2].parse::<u64>().unwrap() > 0); // HARNESS_OUTPUT_BYTES
    }

    #[tokio::test]
    async fn test_post_session_hook_failure_does_not_stop_loop() {
        let dir = tempdir().unwrap();
        let mut config = test_config(dir.path(), "echo", vec!["hello".to_string()]);
        // Post-session hook always fails
        config.hooks.post_session = vec!["false".to_string()];
        config.session.max_iterations = 2;

        std::fs::write(&config.session.prompt_file, "test prompt").unwrap();

        let signals = make_signals();
        let data_dir = test_data_dir(dir.path());
        let summary = run(&config, &data_dir, &signals, false).await;

        // Should complete all iterations despite post-hook failures
        assert_eq!(summary.productive_iterations, 2);
        assert_eq!(summary.exit_reason, ExitReason::MaxIterations);
    }

    #[tokio::test]
    async fn test_rate_limited_session_not_counted_as_productive() {
        let dir = tempdir().unwrap();
        // Script outputs a JSONL result event with is_error=true and rate_limit text
        let script = dir.path().join("rate_limit_echo.sh");
        std::fs::write(
            &script,
            r#"#!/bin/sh
printf '{"type":"result","subtype":"error","is_error":true,"result":"rate_limit: too many requests"}\n'
"#,
        )
        .unwrap();
        std::fs::set_permissions(&script, std::os::unix::fs::PermissionsExt::from_mode(0o755))
            .unwrap();

        let mut config = test_config(dir.path(), script.to_str().unwrap(), vec![]);
        config.session.max_iterations = 3;
        config.backoff.initial_delay_secs = 0;
        config.backoff.max_delay_secs = 0;
        config.backoff.max_consecutive_rate_limits = 5;

        std::fs::write(&config.session.prompt_file, "test prompt").unwrap();

        let signals = make_signals();
        let data_dir = test_data_dir(dir.path());
        let summary = run(&config, &data_dir, &signals, false).await;

        // All sessions are rate-limited, should exit after max_consecutive_rate_limits
        assert_eq!(summary.productive_iterations, 0);
        assert_eq!(summary.exit_reason, ExitReason::RateLimited);
        assert_eq!(summary.global_iteration, 5); // 5 rate-limited sessions
    }

    #[tokio::test]
    async fn test_rate_limit_resets_on_success() {
        let dir = tempdir().unwrap();
        // Script: first call outputs rate-limited error result, subsequent calls output success
        let script = dir.path().join("rate_limit_script.sh");
        let marker = dir.path().join("call_count");
        std::fs::write(
            &script,
            format!(
                r#"#!/bin/sh
count=0
if [ -f "{marker}" ]; then
    count=$(cat "{marker}")
fi
count=$((count + 1))
echo $count > "{marker}"
if [ $count -le 1 ]; then
    printf '{{"type":"result","subtype":"error","is_error":true,"result":"rate_limit exceeded"}}\n'
else
    printf '{{"type":"result","subtype":"success","is_error":false,"result":"productive output"}}\n'
fi
"#,
                marker = marker.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&script, std::os::unix::fs::PermissionsExt::from_mode(0o755))
            .unwrap();

        let mut config = test_config(dir.path(), script.to_str().unwrap(), vec![]);
        config.session.max_iterations = 2;
        config.backoff.initial_delay_secs = 0;
        config.backoff.max_delay_secs = 0;
        config.backoff.max_consecutive_rate_limits = 5;

        std::fs::write(&config.session.prompt_file, "test prompt").unwrap();

        let signals = make_signals();
        let data_dir = test_data_dir(dir.path());
        let summary = run(&config, &data_dir, &signals, false).await;

        // First session: rate-limited (not productive)
        // Second session: normal (productive)
        // Third session: normal (productive) — max_iterations reached
        assert_eq!(summary.productive_iterations, 2);
        assert_eq!(summary.exit_reason, ExitReason::MaxIterations);
        // global: 1 (rate-limited) + 2 (productive) = 3
        assert_eq!(summary.global_iteration, 3);
    }

    #[tokio::test]
    async fn test_rate_limited_event_log_shows_rate_limited_true() {
        let dir = tempdir().unwrap();
        let event_log_path = dir.path().join("events.jsonl");
        // Script outputs a JSONL error result with rate limit text
        let script = dir.path().join("rate_limit_echo.sh");
        std::fs::write(
            &script,
            r#"#!/bin/sh
printf '{"type":"result","subtype":"error","is_error":true,"result":"hit your limit"}\n'
"#,
        )
        .unwrap();
        std::fs::set_permissions(&script, std::os::unix::fs::PermissionsExt::from_mode(0o755))
            .unwrap();

        let mut config = test_config(dir.path(), script.to_str().unwrap(), vec![]);
        config.session.max_iterations = 1;
        config.backoff.initial_delay_secs = 0;
        config.backoff.max_delay_secs = 0;
        config.backoff.max_consecutive_rate_limits = 2;
        config.output.event_log = Some(event_log_path.clone());

        std::fs::write(&config.session.prompt_file, "test prompt").unwrap();

        let signals = make_signals();
        let data_dir = test_data_dir(dir.path());
        let summary = run(&config, &data_dir, &signals, false).await;

        assert_eq!(summary.exit_reason, ExitReason::RateLimited);

        // Verify event log entries have rate_limited=true
        let contents = std::fs::read_to_string(&event_log_path).unwrap();
        for line in contents.lines() {
            let parsed: serde_json::Value = serde_json::from_str(line).unwrap();
            assert_eq!(
                parsed["rate_limited"], true,
                "event should show rate_limited=true"
            );
        }
    }

    #[tokio::test]
    async fn test_successful_session_with_rate_limit_text_not_detected() {
        let dir = tempdir().unwrap();
        // Script outputs tool output with rate limit text, but the session result is success
        let script = dir.path().join("success_with_rate_text.sh");
        std::fs::write(
            &script,
            r#"#!/bin/sh
printf '{"type":"tool_result","content":"rate_limit detection code: usage limit"}\n'
printf '{"type":"result","subtype":"success","is_error":false,"result":"Done."}\n'
"#,
        )
        .unwrap();
        std::fs::set_permissions(&script, std::os::unix::fs::PermissionsExt::from_mode(0o755))
            .unwrap();

        let mut config = test_config(dir.path(), script.to_str().unwrap(), vec![]);
        config.session.max_iterations = 1;
        config.backoff.initial_delay_secs = 0;

        std::fs::write(&config.session.prompt_file, "test prompt").unwrap();

        let signals = make_signals();
        let data_dir = test_data_dir(dir.path());
        let summary = run(&config, &data_dir, &signals, false).await;

        // Session should be counted as productive (not rate-limited)
        assert_eq!(summary.productive_iterations, 1);
        assert_eq!(summary.exit_reason, ExitReason::MaxIterations);
    }

    #[tokio::test]
    async fn test_empty_retry_does_not_trigger_post_hooks() {
        let dir = tempdir().unwrap();
        let marker = dir.path().join("post_hook_count");
        let mut config = test_config(dir.path(), "true", vec![]);
        config.session.max_iterations = 1;
        config.retry.max_empty_retries = 2;
        config.watchdog.min_output_bytes = 100;
        // Post hook appends a line each time it runs
        config.hooks.post_session = vec![format!("echo ran >> {}", marker.display())];

        std::fs::write(&config.session.prompt_file, "test prompt").unwrap();

        let signals = make_signals();
        let data_dir = test_data_dir(dir.path());
        let summary = run(&config, &data_dir, &signals, false).await;

        assert_eq!(summary.productive_iterations, 1);
        // Post hook should only run once (on Skip), not during retries
        assert!(marker.exists(), "post hook should have run at least once");
        let contents = std::fs::read_to_string(&marker).unwrap();
        let count = contents.lines().count();
        assert_eq!(
            count, 1,
            "post hook should run once (on skip), not during retries"
        );
    }

    #[tokio::test]
    async fn test_commit_detected_in_event_log() {
        let dir = tempdir().unwrap();
        let event_log_path = dir.path().join("events.jsonl");
        // Script outputs text containing "bd-finish" which matches the default commit pattern
        let script = dir.path().join("commit_echo.sh");
        std::fs::write(
            &script,
            r#"#!/bin/sh
printf '{"type":"assistant","message":"running bd-finish.sh bead-123"}\n'
printf '{"type":"result","subtype":"success","is_error":false,"result":"Done."}\n'
"#,
        )
        .unwrap();
        std::fs::set_permissions(&script, std::os::unix::fs::PermissionsExt::from_mode(0o755))
            .unwrap();

        let mut config = test_config(dir.path(), script.to_str().unwrap(), vec![]);
        config.session.max_iterations = 1;
        config.backoff.initial_delay_secs = 0;
        config.output.event_log = Some(event_log_path.clone());

        std::fs::write(&config.session.prompt_file, "test prompt").unwrap();

        let signals = make_signals();
        let data_dir = test_data_dir(dir.path());
        let summary = run(&config, &data_dir, &signals, false).await;

        assert_eq!(summary.productive_iterations, 1);

        // Verify event log has committed=true
        let contents = std::fs::read_to_string(&event_log_path).unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 1);
        let parsed: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(
            parsed["committed"], true,
            "committed should be true when bd-finish detected"
        );
    }

    #[tokio::test]
    async fn test_no_commit_detected_in_event_log() {
        let dir = tempdir().unwrap();
        let event_log_path = dir.path().join("events.jsonl");
        let mut config = test_config(dir.path(), "echo", vec!["just regular output".to_string()]);
        config.session.max_iterations = 1;
        config.backoff.initial_delay_secs = 0;
        config.output.event_log = Some(event_log_path.clone());

        std::fs::write(&config.session.prompt_file, "test prompt").unwrap();

        let signals = make_signals();
        let data_dir = test_data_dir(dir.path());
        let summary = run(&config, &data_dir, &signals, false).await;

        assert_eq!(summary.productive_iterations, 1);

        // Verify event log has committed=false
        let contents = std::fs::read_to_string(&event_log_path).unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 1);
        let parsed: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(
            parsed["committed"], false,
            "committed should be false when no commit pattern matched"
        );
    }

    #[tokio::test]
    async fn test_commit_detected_in_post_hook_env() {
        let dir = tempdir().unwrap();
        let marker = dir.path().join("hook_output");
        // Script outputs text containing "git commit" which matches a default commit pattern
        let script = dir.path().join("commit_echo.sh");
        std::fs::write(
            &script,
            r#"#!/bin/sh
echo "Changes committed via git commit -m fix"
"#,
        )
        .unwrap();
        std::fs::set_permissions(&script, std::os::unix::fs::PermissionsExt::from_mode(0o755))
            .unwrap();

        let mut config = test_config(dir.path(), script.to_str().unwrap(), vec![]);
        config.session.max_iterations = 1;
        config.backoff.initial_delay_secs = 0;
        config.hooks.post_session = vec![format!("echo $HARNESS_COMMITTED > {}", marker.display())];

        std::fs::write(&config.session.prompt_file, "test prompt").unwrap();

        let signals = make_signals();
        let data_dir = test_data_dir(dir.path());
        let summary = run(&config, &data_dir, &signals, false).await;

        assert_eq!(summary.productive_iterations, 1);
        assert!(marker.exists(), "post hook should have run");
        let contents = std::fs::read_to_string(&marker).unwrap();
        assert_eq!(contents.trim(), "true", "HARNESS_COMMITTED should be true");
    }

    #[tokio::test]
    async fn test_run_ingests_jsonl_metrics_to_db() {
        let dir = tempdir().unwrap();
        // Script outputs valid JSONL with assistant turns and a result event
        let script = dir.path().join("metrics_echo.sh");
        std::fs::write(
            &script,
            r#"#!/bin/sh
printf '{"type":"assistant","message":{"content":[{"type":"text","text":"hello"}]}}\n'
printf '{"type":"assistant","message":{"content":[{"type":"tool_use","name":"Read","input":{}},{"type":"tool_use","name":"Grep","input":{}}]}}\n'
printf '{"type":"result","duration_ms":5000,"total_cost_usd":0.42,"num_turns":2,"modelUsage":{"opus":{"inputTokens":100,"outputTokens":50,"cacheReadInputTokens":0,"cacheCreationInputTokens":0}}}\n'
"#,
        )
        .unwrap();
        std::fs::set_permissions(&script, std::os::unix::fs::PermissionsExt::from_mode(0o755))
            .unwrap();

        let mut config = test_config(dir.path(), script.to_str().unwrap(), vec![]);
        config.session.max_iterations = 1;
        config.backoff.initial_delay_secs = 0;
        config.agent.adapter = Some("claude".to_string()); // Script outputs Claude JSONL format

        std::fs::write(&config.session.prompt_file, "test prompt").unwrap();

        let signals = make_signals();
        let data_dir = test_data_dir(dir.path());
        let summary = run(&config, &data_dir, &signals, false).await;

        assert_eq!(summary.productive_iterations, 1);

        // Verify blacksmith.db was created and has ingested data
        let db_path = dir.path().join(".blacksmith/blacksmith.db");
        assert!(db_path.exists(), "blacksmith.db should be created");

        let conn = crate::db::open_or_create(&db_path).unwrap();

        // Check events were written for session 0
        let events = crate::db::events_by_session(&conn, 0).unwrap();
        assert!(!events.is_empty(), "events should be ingested");

        let turns = events.iter().find(|e| e.kind == "turns.total").unwrap();
        assert_eq!(turns.value.as_deref(), Some("2"));

        let parallel = events.iter().find(|e| e.kind == "turns.parallel").unwrap();
        assert_eq!(parallel.value.as_deref(), Some("1"));

        let cost = events
            .iter()
            .find(|e| e.kind == "cost.estimate_usd")
            .unwrap();
        assert_eq!(cost.value.as_deref(), Some("0.420000"));

        // Check observation was written
        let obs = crate::db::get_observation(&conn, 0).unwrap().unwrap();
        let data: serde_json::Value = serde_json::from_str(&obs.data).unwrap();
        assert_eq!(data["turns.total"], 2);
        assert_eq!(data["turns.parallel"], 1);
        assert_eq!(data["cost.estimate_usd"], 0.42);
    }

    #[tokio::test]
    async fn test_run_ingestion_does_not_block_on_failure() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path(), "echo", vec!["hello".to_string()]);
        config.session.max_iterations;

        std::fs::write(&config.session.prompt_file, "test prompt").unwrap();

        // Make blacksmith.db a directory to cause open_or_create to fail
        // test_data_dir creates .blacksmith/, so we create .blacksmith/blacksmith.db as a dir
        let data_dir = test_data_dir(dir.path());
        std::fs::remove_dir_all(dir.path().join(".blacksmith/blacksmith.db")).ok();
        std::fs::create_dir_all(dir.path().join(".blacksmith/blacksmith.db")).unwrap();

        let signals = make_signals();
        let summary = run(&config, &data_dir, &signals, false).await;

        // Should still complete despite DB failure
        assert_eq!(summary.productive_iterations, 3);
        assert_eq!(summary.exit_reason, ExitReason::MaxIterations);
    }

    #[tokio::test]
    async fn test_quiet_mode_completes_successfully() {
        let dir = tempdir().unwrap();
        let mut config = test_config(dir.path(), "echo", vec!["hello".to_string()]);
        config.session.max_iterations = 1;

        std::fs::write(&config.session.prompt_file, "test prompt").unwrap();

        let signals = make_signals();
        let data_dir = test_data_dir(dir.path());
        let summary = run(&config, &data_dir, &signals, true).await;

        // Quiet mode should still complete normally
        assert_eq!(summary.productive_iterations, 1);
        assert_eq!(summary.exit_reason, ExitReason::MaxIterations);
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

    // ── print_progress_line tests ──

    #[test]
    fn test_print_progress_line_no_db() {
        let result = SessionResult {
            exit_code: Some(0),
            output_bytes: 1000,
            duration: std::time::Duration::from_secs(300),
            output_file: PathBuf::from("test.jsonl"),
            pid: 1,
        };
        // Should print without panicking even with no DB
        print_progress_line(42, &result, None, None, 1);
    }

    #[test]
    fn test_print_progress_line_with_ingest() {
        let result = SessionResult {
            exit_code: Some(0),
            output_bytes: 1000,
            duration: std::time::Duration::from_secs(300),
            output_file: PathBuf::from("test.jsonl"),
            pid: 1,
        };
        let ingest = crate::ingest::IngestResult {
            turns_total: 65,
            cost_estimate_usd: 1.85,
            session_duration_ms: 300000,
            bead_id: None,
        };
        // Should include turns in output
        print_progress_line(42, &result, Some(&ingest), None, 1);
    }

    #[test]
    fn test_build_progress_string_empty_db() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let conn = crate::db::open_or_create(&db_path).unwrap();
        // Empty DB — no bead_metrics
        let result = build_progress_string(&conn, 1);
        assert!(result.is_none());
    }

    #[test]
    fn test_build_progress_string_with_completed() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let conn = crate::db::open_or_create(&db_path).unwrap();

        // Add 3 completed beads
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
        // Should produce a progress string (exact content depends on `bd list` availability)
        // In test env, `bd` may not be available, so open_beads will be empty
        assert!(result.is_some());
        let s = result.unwrap();
        assert!(s.contains("Progress:"));
    }
}
