/// Singleton lock: prevents multiple blacksmith instances from running concurrently.
///
/// Uses `flock(LOCK_EX | LOCK_NB)` on `.blacksmith/lock` for atomic mutual exclusion.
/// The lock file also stores the owning PID for diagnostics.
use fs2::FileExt;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;

/// A held singleton lock. The lock is released when this value is dropped.
#[derive(Debug)]
pub struct SingletonLock {
    _file: File,
}

/// Error returned when the lock cannot be acquired.
#[derive(Debug)]
pub enum LockError {
    /// Another blacksmith process holds the lock.
    AlreadyRunning(Option<u32>),
    /// I/O error opening or locking the file.
    Io(std::io::Error),
}

impl std::fmt::Display for LockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LockError::AlreadyRunning(Some(pid)) => {
                write!(
                    f,
                    "another blacksmith instance is already running (PID {pid})"
                )
            }
            LockError::AlreadyRunning(None) => {
                write!(f, "another blacksmith instance is already running")
            }
            LockError::Io(e) => write!(f, "failed to acquire singleton lock: {e}"),
        }
    }
}

/// Try to acquire the singleton lock.
///
/// On success, returns a `SingletonLock` guard that holds the lock until dropped.
/// On failure, returns a `LockError` indicating why the lock couldn't be acquired.
pub fn try_acquire(lock_path: &Path) -> Result<SingletonLock, LockError> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(lock_path)
        .map_err(LockError::Io)?;

    match file.try_lock_exclusive() {
        Ok(()) => {
            // Write our PID for diagnostics
            let mut f = file;
            f.set_len(0).map_err(LockError::Io)?;
            write!(f, "{}", std::process::id()).map_err(LockError::Io)?;
            Ok(SingletonLock { _file: f })
        }
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
            // Lock is held by another process — try to read the PID
            let existing_pid = read_pid_from_lock(lock_path);
            Err(LockError::AlreadyRunning(existing_pid))
        }
        Err(e) => Err(LockError::Io(e)),
    }
}

/// Try to read the PID stored in the lock file.
fn read_pid_from_lock(lock_path: &Path) -> Option<u32> {
    std::fs::read_to_string(lock_path).ok()?.trim().parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_acquire_and_release() {
        let dir = tempfile::tempdir().unwrap();
        let lock_path = dir.path().join("lock");

        let guard = try_acquire(&lock_path).unwrap();

        // Lock file should contain our PID
        let contents = std::fs::read_to_string(&lock_path).unwrap();
        assert_eq!(contents, std::process::id().to_string());

        drop(guard);

        // After dropping, we should be able to re-acquire
        let guard2 = try_acquire(&lock_path).unwrap();
        drop(guard2);
    }

    #[test]
    fn test_double_acquire_fails() {
        let dir = tempfile::tempdir().unwrap();
        let lock_path = dir.path().join("lock");

        let _guard = try_acquire(&lock_path).unwrap();

        // Second acquire should fail with AlreadyRunning
        match try_acquire(&lock_path) {
            Err(LockError::AlreadyRunning(pid)) => {
                assert_eq!(pid, Some(std::process::id()));
            }
            other => panic!("expected AlreadyRunning, got {other:?}"),
        }
    }

    #[test]
    fn test_lock_released_on_drop() {
        let dir = tempfile::tempdir().unwrap();
        let lock_path = dir.path().join("lock");

        {
            let _guard = try_acquire(&lock_path).unwrap();
        }

        // Should succeed after drop
        let _guard = try_acquire(&lock_path).unwrap();
    }

    #[test]
    fn test_read_pid_from_lock_missing_file() {
        let result = read_pid_from_lock(Path::new("/nonexistent/lock"));
        assert!(result.is_none());
    }

    #[test]
    fn test_read_pid_from_lock_garbage() {
        let dir = tempfile::tempdir().unwrap();
        let lock_path = dir.path().join("lock");
        std::fs::write(&lock_path, "not-a-pid").unwrap();
        assert!(read_pid_from_lock(&lock_path).is_none());
    }
}
