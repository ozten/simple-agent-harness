//! Manual garbage collection for session files.
//!
//! `blacksmith gc` runs session cleanup immediately, reusing the retention
//! and compression logic. Supports `--dry-run` and `--aggressive` modes.

use crate::compress;
use crate::config::RetentionPolicy;
use crate::data_dir::DataDir;
use std::path::Path;
use std::time::{Duration, SystemTime};

/// Results of a gc run (or dry-run).
#[derive(Debug, Default)]
pub struct GcReport {
    /// Files that would be or were compressed.
    pub compressed: Vec<String>,
    /// Files that would be or were deleted.
    pub deleted: Vec<String>,
}

/// Run garbage collection.
///
/// In normal mode, runs retention + compression with configured values.
/// In aggressive mode, compresses ALL uncompressed files and deletes beyond retention.
pub fn run_gc(
    data_dir: &DataDir,
    retention: &RetentionPolicy,
    compress_after: u32,
    dry_run: bool,
    aggressive: bool,
) -> GcReport {
    let sessions_dir = data_dir.sessions_dir();
    let mut report = GcReport::default();

    if !sessions_dir.exists() {
        return report;
    }

    // Find the current iteration from the counter file
    let current_iteration = load_current_iteration(data_dir);

    // Step 1: Identify files to delete (retention policy)
    let delete_candidates = find_retention_candidates(&sessions_dir, retention);
    for path in &delete_candidates {
        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default();
        report.deleted.push(name);
    }

    // Step 2: Identify files to compress
    let compress_candidates = if aggressive {
        // Aggressive: compress ALL uncompressed .jsonl files (except those about to be deleted)
        find_all_uncompressed(&sessions_dir, &delete_candidates)
    } else {
        // Normal: compress files older than compress_after
        find_compress_candidates(
            &sessions_dir,
            current_iteration,
            compress_after,
            &delete_candidates,
        )
    };
    for path in &compress_candidates {
        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default();
        report.compressed.push(name);
    }

    if dry_run {
        return report;
    }

    // Execute deletions
    for path in &delete_candidates {
        if let Err(e) = std::fs::remove_file(path) {
            tracing::warn!(error = %e, file = %path.display(), "gc: failed to delete");
        }
    }

    // Execute compressions (reuse compress module logic)
    if aggressive {
        // Compress all remaining uncompressed files
        for path in &compress_candidates {
            if let Err(e) = compress_single(path) {
                tracing::warn!(error = %e, file = %path.display(), "gc: failed to compress");
            }
        }
    } else {
        compress::compress_old_sessions(&sessions_dir, current_iteration, compress_after);
    }

    report
}

/// Load the current iteration counter from the data dir.
fn load_current_iteration(data_dir: &DataDir) -> u64 {
    let counter_path = data_dir.counter();
    match std::fs::read_to_string(counter_path) {
        Ok(contents) => contents.trim().parse().unwrap_or(0),
        Err(_) => 0,
    }
}

/// Find session files that should be deleted per the retention policy.
fn find_retention_candidates(
    sessions_dir: &Path,
    retention: &RetentionPolicy,
) -> Vec<std::path::PathBuf> {
    match retention {
        RetentionPolicy::All | RetentionPolicy::AfterIngest => vec![],
        RetentionPolicy::LastN(n) => find_last_n_candidates(sessions_dir, *n),
        RetentionPolicy::Days(days) => find_days_candidates(sessions_dir, *days),
    }
}

/// Find files to delete under the last-N policy.
fn find_last_n_candidates(sessions_dir: &Path, n: u64) -> Vec<std::path::PathBuf> {
    let mut sessions = list_session_files(sessions_dir);
    if sessions.len() as u64 <= n {
        return vec![];
    }

    // Sort by iteration number descending
    sessions.sort_unstable_by(|a, b| b.0.cmp(&a.0));

    // Everything beyond the first N
    sessions
        .into_iter()
        .skip(n as usize)
        .map(|(_, path)| path)
        .collect()
}

/// Find files to delete under the N-days policy.
fn find_days_candidates(sessions_dir: &Path, days: u64) -> Vec<std::path::PathBuf> {
    let cutoff = SystemTime::now() - Duration::from_secs(days * 86400);
    let mut result = vec![];

    let entries = match std::fs::read_dir(sessions_dir) {
        Ok(e) => e,
        Err(_) => return result,
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if parse_session_iteration(&path).is_none() {
            continue;
        }

        if let Ok(mtime) = std::fs::metadata(&path).and_then(|m| m.modified()) {
            if mtime < cutoff {
                result.push(path);
            }
        }
    }

    result
}

/// Find uncompressed .jsonl files eligible for compression (beyond compress_after threshold),
/// excluding those that will be deleted.
fn find_compress_candidates(
    sessions_dir: &Path,
    current_iteration: u64,
    compress_after: u32,
    exclude: &[std::path::PathBuf],
) -> Vec<std::path::PathBuf> {
    if compress_after == 0 || current_iteration < compress_after as u64 {
        return vec![];
    }

    let threshold = current_iteration.saturating_sub(compress_after as u64);
    let mut result = vec![];

    let entries = match std::fs::read_dir(sessions_dir) {
        Ok(e) => e,
        Err(_) => return result,
    };

    for entry in entries.flatten() {
        let path = entry.path();
        let file_name = match path.file_name().and_then(|n| n.to_str()) {
            Some(n) => n.to_string(),
            None => continue,
        };

        // Only uncompressed .jsonl
        if !file_name.ends_with(".jsonl") || file_name.ends_with(".jsonl.zst") {
            continue;
        }

        let iteration: u64 = match file_name
            .strip_suffix(".jsonl")
            .and_then(|s| s.parse().ok())
        {
            Some(n) => n,
            None => continue,
        };

        if iteration <= threshold && !exclude.contains(&path) {
            result.push(path);
        }
    }

    result
}

/// Find ALL uncompressed .jsonl files, excluding those that will be deleted.
fn find_all_uncompressed(
    sessions_dir: &Path,
    exclude: &[std::path::PathBuf],
) -> Vec<std::path::PathBuf> {
    let mut result = vec![];

    let entries = match std::fs::read_dir(sessions_dir) {
        Ok(e) => e,
        Err(_) => return result,
    };

    for entry in entries.flatten() {
        let path = entry.path();
        let file_name = match path.file_name().and_then(|n| n.to_str()) {
            Some(n) => n.to_string(),
            None => continue,
        };

        if !file_name.ends_with(".jsonl") || file_name.ends_with(".jsonl.zst") {
            continue;
        }

        // Must be a valid session file (numeric prefix)
        if file_name
            .strip_suffix(".jsonl")
            .and_then(|s| s.parse::<u64>().ok())
            .is_none()
        {
            continue;
        }

        if !exclude.contains(&path) {
            result.push(path);
        }
    }

    result
}

/// List all session files with their iteration numbers.
fn list_session_files(sessions_dir: &Path) -> Vec<(u64, std::path::PathBuf)> {
    let entries = match std::fs::read_dir(sessions_dir) {
        Ok(e) => e,
        Err(_) => return vec![],
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

/// Extract iteration number from a session filename.
fn parse_session_iteration(path: &Path) -> Option<u64> {
    let file_name = path.file_name()?.to_str()?;
    let stem = file_name
        .strip_suffix(".jsonl.zst")
        .or_else(|| file_name.strip_suffix(".jsonl"))?;
    stem.parse().ok()
}

/// Compress a single .jsonl file to .jsonl.zst and remove the original.
fn compress_single(path: &Path) -> std::io::Result<()> {
    let dest = path.with_extension("jsonl.zst");
    let input = std::fs::read(path)?;
    let compressed = zstd::encode_all(input.as_slice(), 3)?;
    std::fs::write(&dest, compressed)?;
    std::fs::remove_file(path)?;
    Ok(())
}

/// Handle the `blacksmith gc` CLI command.
pub fn handle_gc(
    data_dir: &DataDir,
    retention: &RetentionPolicy,
    compress_after: u32,
    dry_run: bool,
    aggressive: bool,
) {
    let report = run_gc(data_dir, retention, compress_after, dry_run, aggressive);

    if dry_run {
        println!("Dry run — no changes made.");
        println!();
    }

    if report.deleted.is_empty() && report.compressed.is_empty() {
        println!("Nothing to clean up.");
        return;
    }

    if !report.deleted.is_empty() {
        let action = if dry_run { "Would delete" } else { "Deleted" };
        println!("{action} {} session file(s):", report.deleted.len());
        for f in &report.deleted {
            println!("  {f}");
        }
    }

    if !report.compressed.is_empty() {
        let action = if dry_run {
            "Would compress"
        } else {
            "Compressed"
        };
        println!("{action} {} session file(s):", report.compressed.len());
        for f in &report.compressed {
            println!("  {f}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn make_data_dir(tmp: &Path) -> DataDir {
        let dd = DataDir::new(tmp.join(".blacksmith"));
        dd.init().unwrap();
        dd
    }

    fn write_counter(dd: &DataDir, val: u64) {
        std::fs::write(dd.counter(), val.to_string()).unwrap();
    }

    #[test]
    fn test_gc_dry_run_no_changes() {
        let tmp = tempdir().unwrap();
        let dd = make_data_dir(tmp.path());
        let sessions = dd.sessions_dir();
        write_counter(&dd, 20);

        // Create 10 session files (0-9)
        for i in 0..10 {
            std::fs::write(sessions.join(format!("{i}.jsonl")), "data").unwrap();
        }

        let report = run_gc(&dd, &RetentionPolicy::LastN(5), 5, true, false);

        // Should identify files to delete and compress
        assert_eq!(report.deleted.len(), 5); // 0-4 deleted (keep 5-9)
                                             // compress threshold = 20 - 5 = 15, all remaining (5-9) are <= 15
        assert_eq!(report.compressed.len(), 5); // 5-9 would be compressed

        // But files should still exist (dry run)
        for i in 0..10 {
            assert!(
                sessions.join(format!("{i}.jsonl")).exists(),
                "file {i} should still exist in dry run"
            );
        }
    }

    #[test]
    fn test_gc_normal_mode() {
        let tmp = tempdir().unwrap();
        let dd = make_data_dir(tmp.path());
        let sessions = dd.sessions_dir();
        write_counter(&dd, 20);

        for i in 0..10 {
            std::fs::write(sessions.join(format!("{i}.jsonl")), "data").unwrap();
        }

        let report = run_gc(&dd, &RetentionPolicy::LastN(5), 5, false, false);

        assert_eq!(report.deleted.len(), 5);
        assert_eq!(report.compressed.len(), 5);

        // Deleted files should be gone
        for i in 0..5 {
            assert!(!sessions.join(format!("{i}.jsonl")).exists());
            assert!(!sessions.join(format!("{i}.jsonl.zst")).exists());
        }

        // Remaining files should be compressed
        for i in 5..10 {
            assert!(!sessions.join(format!("{i}.jsonl")).exists());
            assert!(sessions.join(format!("{i}.jsonl.zst")).exists());
        }
    }

    #[test]
    fn test_gc_aggressive_compresses_all() {
        let tmp = tempdir().unwrap();
        let dd = make_data_dir(tmp.path());
        let sessions = dd.sessions_dir();
        write_counter(&dd, 5);

        // Create files 0-4, compress_after=10 means normally nothing would compress
        for i in 0..5 {
            std::fs::write(sessions.join(format!("{i}.jsonl")), "data").unwrap();
        }

        let report = run_gc(&dd, &RetentionPolicy::All, 10, false, true);

        assert!(report.deleted.is_empty());
        assert_eq!(report.compressed.len(), 5);

        // All should be compressed
        for i in 0..5 {
            assert!(!sessions.join(format!("{i}.jsonl")).exists());
            assert!(sessions.join(format!("{i}.jsonl.zst")).exists());
        }
    }

    #[test]
    fn test_gc_nothing_to_clean() {
        let tmp = tempdir().unwrap();
        let dd = make_data_dir(tmp.path());
        write_counter(&dd, 0);

        let report = run_gc(&dd, &RetentionPolicy::All, 5, false, false);

        assert!(report.deleted.is_empty());
        assert!(report.compressed.is_empty());
    }

    #[test]
    fn test_gc_already_compressed_skipped() {
        let tmp = tempdir().unwrap();
        let dd = make_data_dir(tmp.path());
        let sessions = dd.sessions_dir();
        write_counter(&dd, 20);

        // Only .jsonl.zst files — nothing to compress
        for i in 0..5 {
            std::fs::write(sessions.join(format!("{i}.jsonl.zst")), "compressed").unwrap();
        }

        let report = run_gc(&dd, &RetentionPolicy::LastN(10), 5, false, false);

        // .zst files still count for retention
        assert!(report.deleted.is_empty());
        assert!(report.compressed.is_empty());
    }

    #[test]
    fn test_gc_missing_sessions_dir() {
        let tmp = tempdir().unwrap();
        let dd = DataDir::new(tmp.path().join("nonexistent"));
        // Don't init — sessions dir doesn't exist

        let report = run_gc(&dd, &RetentionPolicy::LastN(5), 5, false, false);

        assert!(report.deleted.is_empty());
        assert!(report.compressed.is_empty());
    }

    #[test]
    fn test_gc_days_retention() {
        let tmp = tempdir().unwrap();
        let dd = make_data_dir(tmp.path());
        let sessions = dd.sessions_dir();
        write_counter(&dd, 10);

        // Create an old file (10 days ago)
        let old_path = sessions.join("0.jsonl");
        std::fs::write(&old_path, "old").unwrap();
        let ten_days_ago = SystemTime::now() - Duration::from_secs(10 * 86400);
        filetime::set_file_mtime(
            &old_path,
            filetime::FileTime::from_system_time(ten_days_ago),
        )
        .unwrap();

        // Create a recent file
        std::fs::write(sessions.join("9.jsonl"), "new").unwrap();

        let report = run_gc(&dd, &RetentionPolicy::Days(7), 5, false, false);

        assert_eq!(report.deleted.len(), 1);
        assert!(!old_path.exists());
        assert!(sessions.join("9.jsonl").exists());
    }

    #[test]
    fn test_gc_aggressive_with_retention() {
        let tmp = tempdir().unwrap();
        let dd = make_data_dir(tmp.path());
        let sessions = dd.sessions_dir();
        write_counter(&dd, 10);

        for i in 0..10 {
            std::fs::write(sessions.join(format!("{i}.jsonl")), "data").unwrap();
        }

        // Aggressive + last-5: delete 0-4, compress 5-9
        let report = run_gc(&dd, &RetentionPolicy::LastN(5), 5, false, true);

        assert_eq!(report.deleted.len(), 5);
        assert_eq!(report.compressed.len(), 5);

        for i in 0..5 {
            assert!(!sessions.join(format!("{i}.jsonl")).exists());
            assert!(!sessions.join(format!("{i}.jsonl.zst")).exists());
        }
        for i in 5..10 {
            assert!(!sessions.join(format!("{i}.jsonl")).exists());
            assert!(sessions.join(format!("{i}.jsonl.zst")).exists());
        }
    }
}
