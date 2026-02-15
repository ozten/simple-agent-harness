/// Core loop: dispatch sessions, monitor, collect metrics, repeat.
///
/// The runner orchestrates the main iteration loop, coordinating
/// session spawning, watchdog monitoring, retry logic, and signal handling.
use crate::config::HarnessConfig;
use crate::retry::{RetryDecision, RetryPolicy};
use crate::session::{self, SessionResult};
use crate::signals::SignalHandler;
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
}

/// Run the main iteration loop.
///
/// Returns a summary of what happened, or an error if setup fails.
pub async fn run(config: &HarnessConfig, signals: &SignalHandler) -> RunSummary {
    let max_iterations = config.session.max_iterations;

    // Load or initialize the global iteration counter
    let mut global_iteration = load_counter(&config.session.counter_file);
    let mut productive = 0u32;
    let mut exit_reason = ExitReason::MaxIterations;

    let mut retry_policy = RetryPolicy::new(
        config.retry.max_empty_retries,
        config.watchdog.min_output_bytes,
    );
    let mut consecutive_errors = 0u32;
    const MAX_CONSECUTIVE_ERRORS: u32 = 5;

    tracing::info!(max_iterations, global_iteration, "starting iteration loop");

    while productive < max_iterations {
        // 1. Check STOP file
        if signals
            .check_stop_file(&config.shutdown.stop_file)
            .is_detected()
        {
            tracing::info!("STOP file detected, exiting loop");
            exit_reason = ExitReason::StopFile;
            break;
        }

        // Check for signal-based shutdown
        if signals.shutdown_requested() {
            tracing::info!("shutdown requested, exiting loop");
            exit_reason = ExitReason::Signal;
            break;
        }

        // 2. Read prompt
        let prompt = match read_prompt(&config.session.prompt_file) {
            Ok(p) => p,
            Err(e) => {
                tracing::error!(error = %e, path = %config.session.prompt_file.display(), "failed to read prompt file");
                exit_reason = ExitReason::PromptError;
                break;
            }
        };

        // 3. Compute output file path for this global iteration
        let output_path = session::output_file_path(&config.session, global_iteration);

        // Ensure output directory exists
        if let Some(parent) = output_path.parent() {
            if !parent.exists() {
                if let Err(e) = std::fs::create_dir_all(parent) {
                    tracing::error!(error = %e, path = %parent.display(), "failed to create output directory");
                    exit_reason = ExitReason::PromptError;
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
                save_counter(&config.session.counter_file, global_iteration);
                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                    tracing::error!(
                        consecutive_errors,
                        "too many consecutive session errors, exiting"
                    );
                    exit_reason = ExitReason::PromptError;
                    break;
                }
                retry_policy.reset();
                continue;
            }
        };

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
                productive += 1;
                global_iteration += 1;
                retry_policy.reset();
                save_counter(&config.session.counter_file, global_iteration);

                tracing::info!(
                    productive = productive,
                    max = max_iterations,
                    global = global_iteration,
                    "productive iteration completed"
                );
            }
            RetryDecision::Retry { attempt } => {
                tracing::warn!(
                    attempt,
                    max = config.retry.max_empty_retries,
                    "retrying empty session"
                );
                global_iteration += 1;
                save_counter(&config.session.counter_file, global_iteration);

                // Delay before retry
                if config.retry.retry_delay_secs > 0 {
                    tokio::time::sleep(Duration::from_secs(config.retry.retry_delay_secs)).await;
                }
                continue; // Skip the inter-iteration delay
            }
            RetryDecision::Skip => {
                tracing::warn!("skipping iteration after exhausting retries");
                productive += 1;
                global_iteration += 1;
                retry_policy.reset();
                save_counter(&config.session.counter_file, global_iteration);
            }
        }

        // 6. Check for shutdown before delay
        if signals.shutdown_requested() {
            tracing::info!("shutdown requested after session, exiting loop");
            exit_reason = ExitReason::Signal;
            break;
        }

        // 7. Inter-iteration delay (only between productive iterations)
        if productive < max_iterations && config.backoff.initial_delay_secs > 0 {
            tracing::debug!(
                delay_secs = config.backoff.initial_delay_secs,
                "inter-iteration delay"
            );
            tokio::time::sleep(Duration::from_secs(config.backoff.initial_delay_secs)).await;
        }
    }

    tracing::info!(
        productive,
        global = global_iteration,
        reason = ?exit_reason,
        "loop finished"
    );

    RunSummary {
        productive_iterations: productive,
        global_iteration,
        exit_reason,
    }
}

/// Spawn a session and its watchdog concurrently, handling force-kill.
async fn run_session_with_watchdog(
    config: &HarnessConfig,
    output_path: &PathBuf,
    prompt: &str,
    signals: &SignalHandler,
) -> Result<SessionResult, session::SessionError> {
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

    let args = config
        .agent
        .args
        .iter()
        .map(|arg| arg.replace("{prompt}", prompt))
        .collect::<Vec<_>>();

    let start = std::time::Instant::now();

    let mut child = tokio::process::Command::new(&config.agent.command)
        .args(&args)
        .stdout(std::process::Stdio::from(output_file))
        .stderr(std::process::Stdio::from(output_file_stderr))
        .process_group(0)
        .spawn()
        .map_err(|e| session::SessionError::Spawn { source: e })?;

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

/// Read the prompt file contents.
fn read_prompt(path: &std::path::Path) -> Result<String, std::io::Error> {
    std::fs::read_to_string(path)
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
        }
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
        let summary = run(&config, &signals).await;

        assert_eq!(summary.productive_iterations, 3);
        assert_eq!(summary.exit_reason, ExitReason::MaxIterations);
        assert_eq!(summary.global_iteration, 3);

        // Verify counter file was persisted
        assert_eq!(load_counter(&config.session.counter_file), 3);

        // Verify output files were created
        for i in 0..3 {
            let path = dir.path().join(format!("test-iter-{}.jsonl", i));
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
        let summary = run(&config, &signals).await;

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

        let summary = run(&config, &signals).await;

        assert_eq!(summary.productive_iterations, 0);
        assert_eq!(summary.exit_reason, ExitReason::Signal);
    }

    #[tokio::test]
    async fn test_run_fails_on_missing_prompt() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path(), "echo", vec!["hello".to_string()]);
        // Don't create prompt file

        let signals = make_signals();
        let summary = run(&config, &signals).await;

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
        let summary = run(&config, &signals).await;

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

        // Pre-set counter to 100
        std::fs::write(&config.session.counter_file, "100").unwrap();

        let signals = make_signals();
        let summary = run(&config, &signals).await;

        assert_eq!(summary.productive_iterations, 3);
        assert_eq!(summary.global_iteration, 103);

        // Verify output files use global counter
        for i in 100..103 {
            let path = dir.path().join(format!("test-iter-{}.jsonl", i));
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
        let summary = run(&config, &signals).await;

        assert_eq!(summary.productive_iterations, 1);
        assert_eq!(summary.exit_reason, ExitReason::MaxIterations);
        assert_eq!(summary.global_iteration, 1);
    }
}
