//! Retention policy enforcement for session files.
//!
//! Runs at the start of each loop iteration (step 2) to delete session files
//! beyond the configured retention window. Supports `last-N`, `Nd`, `all`,
//! and `after-ingest` policies.

use crate::config::RetentionPolicy;
use std::path::Path;
use std::time::{Duration, SystemTime};

/// Enforce the retention policy by deleting session files beyond the retention window.
///
/// Scans the sessions directory for `.jsonl` and `.jsonl.zst` files and removes
/// those that fall outside the configured retention window.
///
/// - `last-N`: keep the N most recent sessions by iteration number, delete older ones.
/// - `Nd`: keep sessions with mtime within the last N days, delete older ones.
/// - `all`: do nothing (never delete).
/// - `after-ingest`: handled separately at ingestion time, not here.
pub fn enforce_retention(sessions_dir: &Path, retention: &RetentionPolicy) {
    match retention {
        RetentionPolicy::All | RetentionPolicy::AfterIngest => {
            // All: never delete. AfterIngest: deletion happens at ingestion time.
        }
        RetentionPolicy::LastN(n) => enforce_last_n(sessions_dir, *n),
        RetentionPolicy::Days(n) => enforce_days(sessions_dir, *n),
    }
}

/// Delete a single session file after successful ingestion (for `after-ingest` policy).
///
/// Removes the JSONL file at the given path. Called immediately after ingestion
/// succeeds. Does not compress — just deletes.
pub fn delete_after_ingest(path: &Path) {
    if path.exists() {
        if let Err(e) = std::fs::remove_file(path) {
            tracing::warn!(
                error = %e,
                file = %path.display(),
                "failed to delete session file after ingestion"
            );
        } else {
            tracing::debug!(
                file = %path.display(),
                "deleted session file after ingestion (after-ingest policy)"
            );
        }
    }
}

/// Keep only the N most recent sessions by iteration number.
fn enforce_last_n(sessions_dir: &Path, n: u64) {
    let mut sessions = list_session_files(sessions_dir);
    if sessions.len() as u64 <= n {
        return;
    }

    // Sort by iteration number descending so we keep the highest N
    sessions.sort_unstable_by(|a, b| b.0.cmp(&a.0));

    // Delete everything beyond the first N
    for (iteration, path) in sessions.into_iter().skip(n as usize) {
        if let Err(e) = std::fs::remove_file(&path) {
            tracing::warn!(
                error = %e,
                file = %path.display(),
                iteration,
                "failed to delete session file (retention: last-N)"
            );
        } else {
            tracing::debug!(
                file = %path.display(),
                iteration,
                "deleted session file (retention: last-N)"
            );
        }
    }
}

/// Keep sessions with mtime within the last N days.
fn enforce_days(sessions_dir: &Path, days: u64) {
    let cutoff = SystemTime::now() - Duration::from_secs(days * 86400);

    let entries = match std::fs::read_dir(sessions_dir) {
        Ok(e) => e,
        Err(e) => {
            tracing::warn!(error = %e, "failed to read sessions directory for retention");
            return;
        }
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if !is_session_file(&path) {
            continue;
        }

        let mtime = match std::fs::metadata(&path).and_then(|m| m.modified()) {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    file = %path.display(),
                    "failed to read mtime for retention check"
                );
                continue;
            }
        };

        if mtime < cutoff {
            if let Err(e) = std::fs::remove_file(&path) {
                tracing::warn!(
                    error = %e,
                    file = %path.display(),
                    "failed to delete session file (retention: Nd)"
                );
            } else {
                tracing::debug!(
                    file = %path.display(),
                    "deleted session file (retention: Nd)"
                );
            }
        }
    }
}

/// List all session files in the directory with their iteration numbers.
/// Returns (iteration_number, path) for each `.jsonl` or `.jsonl.zst` file.
fn list_session_files(sessions_dir: &Path) -> Vec<(u64, std::path::PathBuf)> {
    let entries = match std::fs::read_dir(sessions_dir) {
        Ok(e) => e,
        Err(e) => {
            tracing::warn!(error = %e, "failed to read sessions directory for retention");
            return vec![];
        }
    };

    let mut result = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if let Some(iteration) = parse_session_iteration(&path) {
            result.push((iteration, path));
        }
    }
    result
}

/// Check if a path is a session file (`.jsonl` or `.jsonl.zst` with numeric prefix).
fn is_session_file(path: &Path) -> bool {
    parse_session_iteration(path).is_some()
}

/// Extract the iteration number from a session filename like "42.jsonl" or "42.jsonl.zst".
fn parse_session_iteration(path: &Path) -> Option<u64> {
    let file_name = path.file_name()?.to_str()?;
    let stem = file_name
        .strip_suffix(".jsonl.zst")
        .or_else(|| file_name.strip_suffix(".jsonl"))?;
    stem.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_parse_session_iteration_jsonl() {
        let path = Path::new("/tmp/sessions/42.jsonl");
        assert_eq!(parse_session_iteration(path), Some(42));
    }

    #[test]
    fn test_parse_session_iteration_zst() {
        let path = Path::new("/tmp/sessions/100.jsonl.zst");
        assert_eq!(parse_session_iteration(path), Some(100));
    }

    #[test]
    fn test_parse_session_iteration_non_numeric() {
        let path = Path::new("/tmp/sessions/notes.jsonl");
        assert_eq!(parse_session_iteration(path), None);
    }

    #[test]
    fn test_parse_session_iteration_other_ext() {
        let path = Path::new("/tmp/sessions/42.txt");
        assert_eq!(parse_session_iteration(path), None);
    }

    #[test]
    fn test_enforce_last_n_deletes_oldest() {
        let dir = tempdir().unwrap();
        let sessions = dir.path();

        // Create 10 session files (mix of .jsonl and .jsonl.zst)
        for i in 0..7 {
            std::fs::write(sessions.join(format!("{i}.jsonl.zst")), "compressed").unwrap();
        }
        for i in 7..10 {
            std::fs::write(sessions.join(format!("{i}.jsonl")), "raw").unwrap();
        }

        // Keep last 5
        enforce_retention(sessions, &RetentionPolicy::LastN(5));

        // Sessions 0-4 should be deleted
        for i in 0..5 {
            assert!(
                !sessions.join(format!("{i}.jsonl.zst")).exists(),
                "session {i} should be deleted"
            );
        }
        // Sessions 5-9 should remain
        for i in 5..7 {
            assert!(
                sessions.join(format!("{i}.jsonl.zst")).exists(),
                "session {i} should remain"
            );
        }
        for i in 7..10 {
            assert!(
                sessions.join(format!("{i}.jsonl")).exists(),
                "session {i} should remain"
            );
        }
    }

    #[test]
    fn test_enforce_last_n_no_deletion_when_under_limit() {
        let dir = tempdir().unwrap();
        let sessions = dir.path();

        for i in 0..3 {
            std::fs::write(sessions.join(format!("{i}.jsonl")), "data").unwrap();
        }

        enforce_retention(sessions, &RetentionPolicy::LastN(5));

        // All 3 should remain (under the limit of 5)
        for i in 0..3 {
            assert!(sessions.join(format!("{i}.jsonl")).exists());
        }
    }

    #[test]
    fn test_enforce_days_deletes_old_files() {
        let dir = tempdir().unwrap();
        let sessions = dir.path();

        // Create a session file and backdate its mtime to 10 days ago
        let old_path = sessions.join("0.jsonl");
        std::fs::write(&old_path, "old data").unwrap();
        let ten_days_ago = SystemTime::now() - Duration::from_secs(10 * 86400);
        filetime::set_file_mtime(
            &old_path,
            filetime::FileTime::from_system_time(ten_days_ago),
        )
        .unwrap();

        // Create a recent session file
        let new_path = sessions.join("1.jsonl");
        std::fs::write(&new_path, "new data").unwrap();

        // Retain sessions from last 7 days
        enforce_retention(sessions, &RetentionPolicy::Days(7));

        assert!(!old_path.exists(), "old session should be deleted");
        assert!(new_path.exists(), "new session should remain");
    }

    #[test]
    fn test_enforce_all_does_nothing() {
        let dir = tempdir().unwrap();
        let sessions = dir.path();

        for i in 0..5 {
            std::fs::write(sessions.join(format!("{i}.jsonl")), "data").unwrap();
        }

        enforce_retention(sessions, &RetentionPolicy::All);

        for i in 0..5 {
            assert!(sessions.join(format!("{i}.jsonl")).exists());
        }
    }

    #[test]
    fn test_enforce_after_ingest_does_nothing_in_cleanup() {
        let dir = tempdir().unwrap();
        let sessions = dir.path();

        for i in 0..5 {
            std::fs::write(sessions.join(format!("{i}.jsonl")), "data").unwrap();
        }

        enforce_retention(sessions, &RetentionPolicy::AfterIngest);

        for i in 0..5 {
            assert!(sessions.join(format!("{i}.jsonl")).exists());
        }
    }

    #[test]
    fn test_delete_after_ingest() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("42.jsonl");
        std::fs::write(&path, "session data").unwrap();

        delete_after_ingest(&path);

        assert!(!path.exists(), "file should be deleted after ingestion");
    }

    #[test]
    fn test_delete_after_ingest_nonexistent_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("nonexistent.jsonl");

        // Should not panic
        delete_after_ingest(&path);
    }

    #[test]
    fn test_non_session_files_ignored() {
        let dir = tempdir().unwrap();
        let sessions = dir.path();

        std::fs::write(sessions.join("notes.txt"), "some notes").unwrap();
        std::fs::write(sessions.join("0.jsonl"), "session").unwrap();
        std::fs::write(sessions.join("1.jsonl"), "session").unwrap();

        enforce_retention(sessions, &RetentionPolicy::LastN(1));

        // notes.txt should be untouched
        assert!(sessions.join("notes.txt").exists());
        // Only session 1 should remain
        assert!(!sessions.join("0.jsonl").exists());
        assert!(sessions.join("1.jsonl").exists());
    }

    #[test]
    fn test_missing_sessions_directory() {
        let path = Path::new("/nonexistent/sessions");
        // Should not panic
        enforce_retention(path, &RetentionPolicy::LastN(5));
    }
}
