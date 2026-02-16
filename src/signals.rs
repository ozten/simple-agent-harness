/// Signal handling for graceful shutdown.
///
/// Handles SIGINT (Ctrl-C), SIGTERM, and STOP file detection.
/// First SIGINT: finish current session then exit (SIGTERM to child).
/// Second SIGINT within 3s OR 3s timeout: SIGKILL child immediately.
/// SIGTERM: same as single SIGINT.
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

/// Shared shutdown state, accessible from signal handlers and the main loop.
#[derive(Clone)]
pub struct SignalHandler {
    inner: Arc<SignalState>,
}

struct SignalState {
    /// Set on first SIGINT or SIGTERM — finish current session, then exit.
    shutdown_requested: AtomicBool,
    /// Set on second SIGINT or 3s timeout — kill child immediately.
    force_kill: AtomicBool,
    /// PID of the current child process (0 = no child running).
    child_pid: AtomicI32,
    /// Notified when force_kill is set, so waiters can react immediately.
    force_kill_notify: Notify,
}

/// What the STOP-file check found.
#[derive(Debug, PartialEq)]
pub enum StopFileStatus {
    /// No STOP file present.
    NotPresent,
    /// STOP file was present and has been deleted.
    Detected,
}

impl SignalHandler {
    /// Create a new handler and spawn background tasks that listen for
    /// SIGINT and SIGTERM. Call this once at startup.
    pub fn install() -> Self {
        let handler = Self {
            inner: Arc::new(SignalState {
                shutdown_requested: AtomicBool::new(false),
                force_kill: AtomicBool::new(false),
                child_pid: AtomicI32::new(0),
                force_kill_notify: Notify::new(),
            }),
        };

        handler.spawn_sigint_listener();
        handler.spawn_sigterm_listener();

        handler
    }

    /// Returns `true` if a graceful shutdown has been requested
    /// (first SIGINT, SIGTERM, or STOP file detected).
    pub fn shutdown_requested(&self) -> bool {
        self.inner.shutdown_requested.load(Ordering::SeqCst)
    }

    /// Returns `true` if immediate force-kill was requested
    /// (second SIGINT within 3s, or 3s timeout after first SIGINT).
    pub fn force_kill_requested(&self) -> bool {
        self.inner.force_kill.load(Ordering::SeqCst)
    }

    /// Set the PID of the currently-running child process so that
    /// a force-kill signal can terminate it.
    pub fn set_child_pid(&self, pid: i32) {
        self.inner.child_pid.store(pid, Ordering::SeqCst);
    }

    /// Clear the child PID (call when session ends).
    pub fn clear_child_pid(&self) {
        self.inner.child_pid.store(0, Ordering::SeqCst);
    }

    /// Wait until force-kill is requested. Returns immediately if already set.
    pub async fn wait_for_force_kill(&self) {
        if self.force_kill_requested() {
            return;
        }
        self.inner.force_kill_notify.notified().await;
    }

    /// Mark graceful shutdown (used by STOP file detection and signals).
    pub fn request_shutdown(&self) {
        self.inner.shutdown_requested.store(true, Ordering::SeqCst);
    }

    /// Check for a STOP file. If present, delete it, mark shutdown, and
    /// return `Detected`. Otherwise return `NotPresent`.
    pub fn check_stop_file(&self, stop_path: &Path) -> StopFileStatus {
        if stop_path.exists() {
            tracing::info!(path = %stop_path.display(), "STOP file detected, requesting shutdown");
            if let Err(e) = std::fs::remove_file(stop_path) {
                tracing::warn!(path = %stop_path.display(), error = %e, "failed to delete STOP file");
            }
            self.request_shutdown();
            StopFileStatus::Detected
        } else {
            StopFileStatus::NotPresent
        }
    }

    /// Spawn a tokio task that listens for SIGINT.
    /// First SIGINT → graceful shutdown + SIGTERM child so it can exit cleanly.
    /// Second SIGINT within 3s OR 3s timeout → SIGKILL the child process group.
    fn spawn_sigint_listener(&self) {
        let state = self.inner.clone();
        tokio::spawn(async move {
            let mut sigint =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
                    .expect("failed to install SIGINT handler");

            // Wait for first SIGINT
            sigint.recv().await;
            tracing::warn!(
                "caught SIGINT, finishing current session (Ctrl+C again or 3s to force-kill)"
            );
            state.shutdown_requested.store(true, Ordering::SeqCst);

            // Send SIGTERM to child so it can exit gracefully
            let pid = state.child_pid.load(Ordering::SeqCst);
            if pid > 0 {
                term_process_group(pid);
            }

            // Wait up to 3 seconds for a second SIGINT, then auto-escalate
            tokio::select! {
                _ = sigint.recv() => {
                    tracing::warn!("double SIGINT: force-killing child process group");
                }
                _ = tokio::time::sleep(std::time::Duration::from_secs(3)) => {
                    tracing::warn!("3s timeout after SIGINT: force-killing child process group");
                }
            }

            state.force_kill.store(true, Ordering::SeqCst);
            state.force_kill_notify.notify_waiters();

            let pid = state.child_pid.load(Ordering::SeqCst);
            if pid > 0 {
                kill_process_group(pid);
            }
        });
    }

    /// Spawn a tokio task that listens for SIGTERM (same as single SIGINT).
    fn spawn_sigterm_listener(&self) {
        let state = self.inner.clone();
        tokio::spawn(async move {
            let mut sigterm =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("failed to install SIGTERM handler");

            sigterm.recv().await;
            tracing::warn!("caught SIGTERM, finishing current session");
            state.shutdown_requested.store(true, Ordering::SeqCst);
        });
    }
}

/// Send SIGTERM to an entire process group (graceful shutdown).
fn term_process_group(pid: i32) {
    use nix::sys::signal::{killpg, Signal};
    use nix::unistd::Pid;

    let pgid = Pid::from_raw(pid);
    tracing::info!(%pid, "sending SIGTERM to process group");
    if let Err(e) = killpg(pgid, Signal::SIGTERM) {
        tracing::warn!(%pid, error = %e, "failed to SIGTERM process group");
    }
}

/// Send SIGKILL to an entire process group (force kill).
fn kill_process_group(pid: i32) {
    use nix::sys::signal::{killpg, Signal};
    use nix::unistd::Pid;

    let pgid = Pid::from_raw(pid);
    tracing::info!(%pid, "sending SIGKILL to process group");
    if let Err(e) = killpg(pgid, Signal::SIGKILL) {
        tracing::warn!(%pid, error = %e, "failed to kill process group");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_initial_state() {
        // Can't call install() without a tokio runtime, so test the state directly
        let state = Arc::new(SignalState {
            shutdown_requested: AtomicBool::new(false),
            force_kill: AtomicBool::new(false),
            child_pid: AtomicI32::new(0),
            force_kill_notify: Notify::new(),
        });
        let handler = SignalHandler { inner: state };
        assert!(!handler.shutdown_requested());
        assert!(!handler.force_kill_requested());
    }

    #[test]
    fn test_request_shutdown() {
        let state = Arc::new(SignalState {
            shutdown_requested: AtomicBool::new(false),
            force_kill: AtomicBool::new(false),
            child_pid: AtomicI32::new(0),
            force_kill_notify: Notify::new(),
        });
        let handler = SignalHandler { inner: state };
        assert!(!handler.shutdown_requested());
        handler.request_shutdown();
        assert!(handler.shutdown_requested());
    }

    #[test]
    fn test_child_pid_management() {
        let state = Arc::new(SignalState {
            shutdown_requested: AtomicBool::new(false),
            force_kill: AtomicBool::new(false),
            child_pid: AtomicI32::new(0),
            force_kill_notify: Notify::new(),
        });
        let handler = SignalHandler {
            inner: state.clone(),
        };
        assert_eq!(state.child_pid.load(Ordering::SeqCst), 0);
        handler.set_child_pid(12345);
        assert_eq!(state.child_pid.load(Ordering::SeqCst), 12345);
        handler.clear_child_pid();
        assert_eq!(state.child_pid.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn test_stop_file_not_present() {
        let handler = make_handler();
        let dir = tempdir().unwrap();
        let stop_path = dir.path().join("STOP");
        assert_eq!(
            handler.check_stop_file(&stop_path),
            StopFileStatus::NotPresent
        );
        assert!(!handler.shutdown_requested());
    }

    #[test]
    fn test_stop_file_detected_and_deleted() {
        let handler = make_handler();
        let dir = tempdir().unwrap();
        let stop_path = dir.path().join("STOP");
        fs::write(&stop_path, "").unwrap();
        assert!(stop_path.exists());

        assert_eq!(
            handler.check_stop_file(&stop_path),
            StopFileStatus::Detected
        );
        assert!(handler.shutdown_requested());
        assert!(!stop_path.exists(), "STOP file should be deleted");
    }

    #[test]
    fn test_stop_file_sets_shutdown_flag() {
        let handler = make_handler();
        let dir = tempdir().unwrap();
        let stop_path = dir.path().join("STOP");
        fs::write(&stop_path, "stop please").unwrap();

        assert!(!handler.shutdown_requested());
        handler.check_stop_file(&stop_path);
        assert!(handler.shutdown_requested());
    }

    #[test]
    fn test_force_kill_flag() {
        let handler = make_handler();
        assert!(!handler.force_kill_requested());
        handler.inner.force_kill.store(true, Ordering::SeqCst);
        assert!(handler.force_kill_requested());
    }

    #[test]
    fn test_handler_is_clone() {
        let handler = make_handler();
        let cloned = handler.clone();
        handler.request_shutdown();
        // Cloned handler should see the same state (Arc-shared).
        assert!(cloned.shutdown_requested());
    }

    #[tokio::test]
    async fn test_wait_for_force_kill_returns_immediately_when_set() {
        let handler = make_handler();
        handler.inner.force_kill.store(true, Ordering::SeqCst);
        // Should return immediately, not hang.
        handler.wait_for_force_kill().await;
        assert!(handler.force_kill_requested());
    }

    #[tokio::test]
    async fn test_wait_for_force_kill_wakes_on_notify() {
        let handler = make_handler();
        let handler2 = handler.clone();

        let join = tokio::spawn(async move {
            handler2.wait_for_force_kill().await;
            true
        });

        // Give the spawned task a moment to start waiting.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        handler.inner.force_kill.store(true, Ordering::SeqCst);
        handler.inner.force_kill_notify.notify_waiters();

        let result = tokio::time::timeout(std::time::Duration::from_secs(1), join)
            .await
            .expect("timed out waiting for force_kill notify")
            .expect("task panicked");
        assert!(result);
    }

    /// Helper to construct a SignalHandler without spawning signal listeners.
    fn make_handler() -> SignalHandler {
        SignalHandler {
            inner: Arc::new(SignalState {
                shutdown_requested: AtomicBool::new(false),
                force_kill: AtomicBool::new(false),
                child_pid: AtomicI32::new(0),
                force_kill_notify: Notify::new(),
            }),
        }
    }
}
