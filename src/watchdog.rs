/// Output-growth monitor for agent sessions.
///
/// Runs alongside the agent process, periodically checking the output file size.
/// If no growth is detected for `stale_timeout_mins`, kills the agent process group.
use crate::config::WatchdogConfig;
use nix::sys::signal::{killpg, Signal};
use nix::unistd::Pid;
use std::path::Path;
use tokio::time::{interval, Duration};

/// Outcome of a watchdog monitoring session.
#[derive(Debug, PartialEq)]
pub enum WatchdogOutcome {
    /// The process exited on its own before any timeout.
    #[allow(dead_code)]
    ProcessExited,
    /// The watchdog killed the process group due to stale output.
    Killed,
}

/// Monitor an output file for growth, killing the process group if stale.
///
/// This function should be run concurrently with the child process (e.g., via
/// `tokio::select!`). It returns `Killed` if it had to terminate the process
/// group, or `ProcessExited` if cancelled externally (meaning the process
/// finished on its own).
///
/// `child_pid` is the PID of the child process, used as the process group ID
/// (since the child was spawned with `process_group(0)`).
pub async fn monitor(
    config: &WatchdogConfig,
    output_path: &Path,
    child_pid: u32,
) -> WatchdogOutcome {
    let check_interval = Duration::from_secs(config.check_interval_secs);
    let stale_limit = Duration::from_secs(config.stale_timeout_mins * 60);
    monitor_inner(check_interval, stale_limit, output_path, child_pid).await
}

async fn monitor_inner(
    check_interval: Duration,
    stale_limit: Duration,
    output_path: &Path,
    child_pid: u32,
) -> WatchdogOutcome {
    let pgid = Pid::from_raw(child_pid as i32);

    let mut last_size = file_size(output_path);
    let mut stale_duration = Duration::ZERO;

    let mut ticker = interval(check_interval);
    // First tick fires immediately; skip it since we just recorded the initial size.
    ticker.tick().await;

    loop {
        ticker.tick().await;

        let current_size = file_size(output_path);
        if current_size > last_size {
            // Growth detected — reset stale timer
            tracing::debug!(
                output_bytes = current_size,
                growth = current_size - last_size,
                "watchdog: output growth detected"
            );
            last_size = current_size;
            stale_duration = Duration::ZERO;
        } else {
            stale_duration += check_interval;
            tracing::warn!(
                stale_secs = stale_duration.as_secs(),
                limit_secs = stale_limit.as_secs(),
                "watchdog: no output growth"
            );
        }

        if stale_duration >= stale_limit {
            tracing::error!(
                stale_secs = stale_limit.as_secs(),
                pid = child_pid,
                "watchdog: killing stale process group"
            );
            kill_process_group(pgid).await;
            return WatchdogOutcome::Killed;
        }
    }
}

/// Send SIGTERM, wait 5s, then SIGKILL if still alive.
async fn kill_process_group(pgid: Pid) {
    // SIGTERM the process group
    if let Err(e) = killpg(pgid, Signal::SIGTERM) {
        tracing::warn!(error = %e, "watchdog: SIGTERM failed (process may have already exited)");
        return;
    }

    // Wait 5 seconds for graceful shutdown
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Check if still alive by sending signal 0, then SIGKILL if needed
    if killpg(pgid, None).is_ok() {
        tracing::warn!("watchdog: process group still alive after SIGTERM, sending SIGKILL");
        if let Err(e) = killpg(pgid, Signal::SIGKILL) {
            tracing::warn!(error = %e, "watchdog: SIGKILL failed");
        }
    }
}

/// Get the file size, returning 0 if the file doesn't exist or can't be read.
fn file_size(path: &Path) -> u64 {
    std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::process::Stdio;
    use tempfile::NamedTempFile;
    use tokio::process::Command;

    #[test]
    fn test_file_size_existing_file() {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(b"hello").unwrap();
        f.flush().unwrap();
        assert_eq!(file_size(f.path()), 5);
    }

    #[test]
    fn test_file_size_nonexistent() {
        assert_eq!(file_size(Path::new("/nonexistent/file.txt")), 0);
    }

    #[test]
    fn test_file_size_empty_file() {
        let f = NamedTempFile::new().unwrap();
        assert_eq!(file_size(f.path()), 0);
    }

    #[tokio::test]
    async fn test_monitor_kills_stale_process() {
        // Spawn a long-sleeping process that produces no output
        let dir = tempfile::tempdir().unwrap();
        let output_path = dir.path().join("watchdog-test.jsonl");
        std::fs::File::create(&output_path).unwrap();

        let mut child = Command::new("sleep")
            .arg("300") // sleep for a long time
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0)
            .spawn()
            .unwrap();

        let pid = child.id().unwrap();

        let config = WatchdogConfig {
            check_interval_secs: 1,
            stale_timeout_mins: 0, // 0 minutes = immediate (0 seconds)
            min_output_bytes: 100,
        };

        // The watchdog should kill the process after the first check (no growth)
        // With stale_timeout_mins=0, stale_limit=0s, so first stale check triggers kill
        let outcome = monitor(&config, &output_path, pid).await;
        assert_eq!(outcome, WatchdogOutcome::Killed);

        // Reap the child to avoid zombie; the watchdog already sent signals
        let _ = child.wait().await;

        let pgid = Pid::from_raw(pid as i32);
        assert!(killpg(pgid, None).is_err(), "process group should be dead");
    }

    #[tokio::test]
    async fn test_monitor_resets_on_growth() {
        // Spawn a process that writes to the output file every 0.5s for ~3s,
        // then stops writing but keeps running. The watchdog has a 2s stale
        // timeout with 1s check interval, so it should survive through the
        // writing period then kill after 2s of silence.
        let dir = tempfile::tempdir().unwrap();
        let output_path = dir.path().join("growth-test.jsonl");

        let child = Command::new("sh")
            .arg("-c")
            .arg(format!(
                "for i in 1 2 3 4 5 6; do echo line-$i >> {}; sleep 0.5; done; sleep 300",
                output_path.display()
            ))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0)
            .spawn()
            .unwrap();

        let pid = child.id().unwrap();

        // Use monitor_inner with second-level precision for fast tests
        let start = std::time::Instant::now();
        let outcome = monitor_inner(
            Duration::from_secs(1), // check every 1s
            Duration::from_secs(2), // kill after 2s of no growth
            &output_path,
            pid,
        )
        .await;

        assert_eq!(outcome, WatchdogOutcome::Killed);
        // Should have survived through the ~3s writing period, then killed ~2s later
        let elapsed = start.elapsed();
        assert!(
            elapsed.as_secs() >= 4,
            "should survive writing period + stale timeout, took {:?}",
            elapsed
        );
    }

    #[tokio::test]
    async fn test_monitor_cancelled_returns_process_exited() {
        // When the process exits naturally, the caller should cancel the monitor.
        // We simulate this with tokio::select! — the monitor gets cancelled, and the
        // caller knows the process exited naturally.
        let dir = tempfile::tempdir().unwrap();
        let output_path = dir.path().join("cancel-test.jsonl");
        std::fs::File::create(&output_path).unwrap();

        let config = WatchdogConfig {
            check_interval_secs: 60, // Long interval so it doesn't fire
            stale_timeout_mins: 20,
            min_output_bytes: 100,
        };

        // Spawn a process that exits quickly
        let mut child = Command::new("echo")
            .arg("done")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0)
            .spawn()
            .unwrap();

        let pid = child.id().unwrap();

        // Use select! to race the monitor against the child exiting
        tokio::select! {
            _ = child.wait() => {
                // Process exited first — this is the expected path
            }
            outcome = monitor(&config, &output_path, pid) => {
                panic!("monitor should not have finished, got {:?}", outcome);
            }
        }
        // If we get here, the process exited before the watchdog triggered — correct behavior.
    }
}
