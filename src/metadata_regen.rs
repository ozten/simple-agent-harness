//! Metadata regeneration with lazy invalidation strategy.
//!
//! Implements the regeneration logic from the PRD:
//! - On scheduling: check if base_commit matches current main; if not, regenerate layer 2.
//! - On integration: mark all cached layer-2 data as stale (delete entries with old commit).
//! - On refactor integration: proactively regenerate layer 2 for all pending tasks.

use rusqlite::{Connection, Result};
use std::path::Path;

use crate::file_resolution::{self, FileResolution};
use crate::intent::{self, IntentAnalysis};

/// Outcome of an `ensure_fresh` call.
#[derive(Debug, PartialEq)]
pub enum RefreshOutcome {
    /// Cache was valid — no regeneration needed.
    CacheHit,
    /// Cache was stale or missing — regenerated from static analysis.
    Regenerated,
    /// No intent analysis exists for this task — cannot resolve.
    NoIntent,
}

/// Combined metadata for a task: intent analysis (Layer 1) + file resolution (Layer 2).
///
/// This is the value returned by `ensure_fresh_metadata()` and represents everything
/// the scheduler needs to know about a task's impact on the codebase.
#[derive(Debug, Clone)]
pub struct TaskMetadata {
    /// The task identifier.
    pub task_id: String,
    /// Layer 1: LLM-derived intent analysis (concepts + reasoning).
    pub intent: IntentAnalysis,
    /// Layer 2: File resolution mapping concepts to concrete files/modules.
    pub resolution: FileResolution,
}

impl TaskMetadata {
    /// Extract affected file globs suitable for the scheduler's conflict detection.
    ///
    /// Converts the resolved files from file resolution into glob patterns.
    /// Individual files become exact paths; modules with multiple files become
    /// directory globs (e.g. `src/auth/**`).
    pub fn affected_globs(&self) -> Vec<String> {
        let mut globs = Vec::new();
        for mapping in &self.resolution.mappings {
            for file in &mapping.resolved_files {
                if !globs.contains(file) {
                    globs.push(file.clone());
                }
            }
        }
        globs.sort();
        globs
    }
}

/// Ensure both intent analysis and file resolution are fresh for a task.
///
/// This is the main entry point for the scheduler integration. It:
/// 1. Looks up or regenerates the intent analysis (Layer 1)
/// 2. Looks up or regenerates the file resolution (Layer 2)
/// 3. Returns combined `TaskMetadata` with both layers
///
/// Returns `None` if the task has no intent analysis (e.g., LLM command not configured
/// and no prior analysis exists).
pub fn ensure_fresh_metadata(
    conn: &Connection,
    repo_root: &Path,
    task_id: &str,
    current_commit: &str,
) -> Result<Option<TaskMetadata>> {
    let (outcome, resolution) = ensure_fresh(conn, repo_root, task_id, current_commit)?;

    match outcome {
        RefreshOutcome::NoIntent => Ok(None),
        _ => {
            // Intent must exist if we got CacheHit or Regenerated
            let intent = intent::get_by_task_id(conn, task_id)?
                .expect("intent must exist after successful ensure_fresh");
            let resolution =
                resolution.expect("resolution must exist after successful ensure_fresh");

            Ok(Some(TaskMetadata {
                task_id: task_id.to_string(),
                intent,
                resolution,
            }))
        }
    }
}

/// Ensure the file resolution for a task is fresh relative to `current_commit`.
///
/// If the cache has a valid entry for (task_id, current_commit, intent_hash),
/// returns `CacheHit`. Otherwise, regenerates layer 2 via static analysis
/// and stores the result.
pub fn ensure_fresh(
    conn: &Connection,
    repo_root: &Path,
    task_id: &str,
    current_commit: &str,
) -> Result<(RefreshOutcome, Option<FileResolution>)> {
    // Step 1: Look up intent analysis (Layer 1) for the task
    let intent = match intent::get_by_task_id(conn, task_id)? {
        Some(i) => i,
        None => return Ok((RefreshOutcome::NoIntent, None)),
    };

    // Step 2: Check if we have a fresh resolution
    if file_resolution::is_fresh(conn, task_id, current_commit, &intent.content_hash)? {
        let cached = file_resolution::get(conn, task_id, current_commit, &intent.content_hash)?;
        return Ok((RefreshOutcome::CacheHit, cached));
    }

    // Step 3: Regenerate layer 2
    let resolution = file_resolution::resolve(
        repo_root,
        task_id,
        current_commit,
        &intent.content_hash,
        &intent.target_areas,
    );
    file_resolution::store(conn, &resolution)?;

    Ok((RefreshOutcome::Regenerated, Some(resolution)))
}

/// Mark all cached layer-2 data as stale after main advances.
///
/// Called after any integration to main. Deletes all file_resolution entries
/// whose base_commit doesn't match the new commit. Regeneration happens lazily
/// when `ensure_fresh` is called for individual tasks.
pub fn invalidate_on_integration(conn: &Connection, new_commit: &str) -> Result<usize> {
    file_resolution::invalidate_stale(conn, new_commit)
}

/// Proactively regenerate layer 2 for a list of pending tasks after a refactor integration.
///
/// Unlike normal integration (lazy), refactor integrations are more likely to
/// invalidate metadata, so we regenerate eagerly for all pending tasks that
/// have intent analyses.
pub fn regenerate_after_refactor(
    conn: &Connection,
    repo_root: &Path,
    new_commit: &str,
    pending_task_ids: &[&str],
) -> Result<RegenerationReport> {
    // First invalidate everything stale
    let invalidated = file_resolution::invalidate_stale(conn, new_commit)?;

    let mut regenerated = 0;
    let mut skipped_no_intent = 0;
    let mut already_fresh = 0;

    for task_id in pending_task_ids {
        match ensure_fresh(conn, repo_root, task_id, new_commit)? {
            (RefreshOutcome::Regenerated, _) => regenerated += 1,
            (RefreshOutcome::CacheHit, _) => already_fresh += 1,
            (RefreshOutcome::NoIntent, _) => skipped_no_intent += 1,
        }
    }

    Ok(RegenerationReport {
        invalidated,
        regenerated,
        already_fresh,
        skipped_no_intent,
    })
}

/// Summary of a bulk regeneration operation.
#[derive(Debug, Clone, PartialEq)]
pub struct RegenerationReport {
    /// Number of stale entries deleted.
    pub invalidated: usize,
    /// Number of tasks whose layer-2 was regenerated.
    pub regenerated: usize,
    /// Number of tasks that already had fresh data.
    pub already_fresh: usize,
    /// Number of tasks skipped because they lack intent analysis.
    pub skipped_no_intent: usize,
}

/// Default multiplier threshold: flag drift when new file count >= 3x old count.
const DEFAULT_DRIFT_THRESHOLD: f64 = 3.0;

/// A single concept whose resolved file count changed significantly.
#[derive(Debug, Clone, PartialEq)]
pub struct ConceptDrift {
    /// The concept name (e.g. `"auth_endpoints"`).
    pub concept: String,
    /// Number of resolved files in the previous resolution.
    pub old_file_count: usize,
    /// Number of resolved files in the new resolution.
    pub new_file_count: usize,
}

/// Result of comparing two file resolutions for the same task.
#[derive(Debug, Clone, PartialEq)]
pub struct DriftReport {
    /// The task that was checked.
    pub task_id: String,
    /// The multiplier threshold that was used.
    pub threshold: f64,
    /// Concepts that drifted above the threshold.
    pub drifted_concepts: Vec<ConceptDrift>,
    /// Overall file count in the old resolution.
    pub old_total_files: usize,
    /// Overall file count in the new resolution.
    pub new_total_files: usize,
}

impl DriftReport {
    /// Whether any drift was detected (at concept or total level).
    pub fn has_drift(&self) -> bool {
        !self.drifted_concepts.is_empty()
    }

    /// Human-readable summary suitable for logging or architecture agent input.
    pub fn summary(&self) -> String {
        if !self.has_drift() {
            return format!("{}: no metadata drift detected", self.task_id);
        }
        let mut parts = vec![format!(
            "{}: metadata drift detected (total files {} → {})",
            self.task_id, self.old_total_files, self.new_total_files,
        )];
        for cd in &self.drifted_concepts {
            parts.push(format!(
                "  concept '{}': {} → {} files ({}x)",
                cd.concept,
                cd.old_file_count,
                cd.new_file_count,
                if cd.old_file_count > 0 {
                    format!("{:.1}", cd.new_file_count as f64 / cd.old_file_count as f64)
                } else {
                    "∞".to_string()
                },
            ));
        }
        parts.join("\n")
    }
}

/// Detect metadata drift between the previous and current file resolution for a task.
///
/// Compares the most recent cached resolution (any commit) against `new_resolution`.
/// A concept is flagged as drifted if its resolved file count grew by >= `threshold`
/// (default 3x). A concept going from 0 files to any positive count is always flagged.
///
/// Returns `None` if there is no previous resolution to compare against (first time).
pub fn detect_drift(
    conn: &Connection,
    new_resolution: &file_resolution::FileResolution,
    threshold: Option<f64>,
) -> Result<Option<DriftReport>> {
    let threshold = threshold.unwrap_or(DEFAULT_DRIFT_THRESHOLD);

    // Get the most recent previous resolution for this task (any commit)
    let old = match file_resolution::get_latest_for_task(conn, &new_resolution.task_id)? {
        Some(old) => {
            // Skip if the "latest" is actually the same entry we just stored
            if old.base_commit == new_resolution.base_commit
                && old.intent_hash == new_resolution.intent_hash
            {
                return Ok(None);
            }
            old
        }
        None => return Ok(None),
    };

    // Build concept → file count map for old resolution
    let old_counts: std::collections::HashMap<&str, usize> = old
        .mappings
        .iter()
        .map(|m| (m.concept.as_str(), m.resolved_files.len()))
        .collect();

    let mut drifted_concepts = Vec::new();

    for new_mapping in &new_resolution.mappings {
        let new_count = new_mapping.resolved_files.len();
        let old_count = old_counts
            .get(new_mapping.concept.as_str())
            .copied()
            .unwrap_or(0);

        let drifted = if old_count == 0 {
            // 0 → N is always drift (if N > 0)
            new_count > 0
        } else {
            new_count as f64 >= old_count as f64 * threshold
        };

        if drifted {
            drifted_concepts.push(ConceptDrift {
                concept: new_mapping.concept.clone(),
                old_file_count: old_count,
                new_file_count: new_count,
            });
        }
    }

    // Count total unique files in each resolution
    let old_total: usize = count_unique_files(&old);
    let new_total: usize = count_unique_files(new_resolution);

    Ok(Some(DriftReport {
        task_id: new_resolution.task_id.clone(),
        threshold,
        drifted_concepts,
        old_total_files: old_total,
        new_total_files: new_total,
    }))
}

/// Count unique files across all mappings in a resolution.
fn count_unique_files(resolution: &file_resolution::FileResolution) -> usize {
    let mut files = std::collections::HashSet::new();
    for mapping in &resolution.mappings {
        for file in &mapping.resolved_files {
            files.insert(file.as_str());
        }
    }
    files.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_resolution::{self, DerivedFields, FileResolution, FileResolutionMapping};
    use crate::intent::{IntentAnalysis, TargetArea};

    fn setup_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        intent::create_table(&conn).unwrap();
        file_resolution::create_table(&conn).unwrap();
        conn
    }

    fn store_intent(conn: &Connection, task_id: &str, content_hash: &str) {
        let analysis = IntentAnalysis {
            task_id: task_id.to_string(),
            content_hash: content_hash.to_string(),
            target_areas: vec![TargetArea {
                concept: "test_concept".to_string(),
                reasoning: "testing".to_string(),
            }],
        };
        intent::store(conn, &analysis).unwrap();
    }

    fn store_resolution(conn: &Connection, task_id: &str, base_commit: &str, intent_hash: &str) {
        let res = FileResolution {
            task_id: task_id.to_string(),
            base_commit: base_commit.to_string(),
            intent_hash: intent_hash.to_string(),
            mappings: vec![FileResolutionMapping {
                concept: "test".to_string(),
                resolved_files: vec!["src/test.rs".to_string()],
                resolved_modules: vec!["test".to_string()],
            }],
            derived: DerivedFields::default(),
        };
        file_resolution::store(conn, &res).unwrap();
    }

    // --- ensure_fresh tests ---

    #[test]
    fn ensure_fresh_no_intent_returns_no_intent() {
        let conn = setup_db();
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("src")).unwrap();
        std::fs::write(tmp.path().join("src/main.rs"), "fn main() {}").unwrap();

        let (outcome, resolution) =
            ensure_fresh(&conn, tmp.path(), "nonexistent-task", "commit1").unwrap();
        assert_eq!(outcome, RefreshOutcome::NoIntent);
        assert!(resolution.is_none());
    }

    #[test]
    fn ensure_fresh_cache_hit() {
        let conn = setup_db();
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("src")).unwrap();
        std::fs::write(tmp.path().join("src/main.rs"), "fn main() {}").unwrap();

        // Store intent and a matching resolution
        store_intent(&conn, "task-1", "hash1");
        store_resolution(&conn, "task-1", "commit-a", "hash1");

        let (outcome, resolution) = ensure_fresh(&conn, tmp.path(), "task-1", "commit-a").unwrap();
        assert_eq!(outcome, RefreshOutcome::CacheHit);
        assert!(resolution.is_some());
        assert_eq!(resolution.unwrap().base_commit, "commit-a");
    }

    #[test]
    fn ensure_fresh_stale_commit_regenerates() {
        let conn = setup_db();
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("src")).unwrap();
        std::fs::write(tmp.path().join("src/main.rs"), "fn main() {}").unwrap();

        // Store intent but resolution is at old commit
        store_intent(&conn, "task-1", "hash1");
        store_resolution(&conn, "task-1", "old-commit", "hash1");

        let (outcome, resolution) =
            ensure_fresh(&conn, tmp.path(), "task-1", "new-commit").unwrap();
        assert_eq!(outcome, RefreshOutcome::Regenerated);
        assert!(resolution.is_some());
        let res = resolution.unwrap();
        assert_eq!(res.base_commit, "new-commit");
        assert_eq!(res.intent_hash, "hash1");
    }

    #[test]
    fn ensure_fresh_no_cached_resolution_regenerates() {
        let conn = setup_db();
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("src")).unwrap();
        std::fs::write(tmp.path().join("src/main.rs"), "fn main() {}").unwrap();

        // Intent exists but no resolution at all
        store_intent(&conn, "task-1", "hash1");

        let (outcome, resolution) = ensure_fresh(&conn, tmp.path(), "task-1", "commit-a").unwrap();
        assert_eq!(outcome, RefreshOutcome::Regenerated);
        assert!(resolution.is_some());
    }

    #[test]
    fn ensure_fresh_stores_result_for_next_call() {
        let conn = setup_db();
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("src")).unwrap();
        std::fs::write(tmp.path().join("src/main.rs"), "fn main() {}").unwrap();

        store_intent(&conn, "task-1", "hash1");

        // First call: regenerate
        let (outcome1, _) = ensure_fresh(&conn, tmp.path(), "task-1", "commit-a").unwrap();
        assert_eq!(outcome1, RefreshOutcome::Regenerated);

        // Second call: cache hit
        let (outcome2, _) = ensure_fresh(&conn, tmp.path(), "task-1", "commit-a").unwrap();
        assert_eq!(outcome2, RefreshOutcome::CacheHit);
    }

    // --- invalidate_on_integration tests ---

    #[test]
    fn invalidate_on_integration_removes_old_entries() {
        let conn = setup_db();
        store_resolution(&conn, "task-1", "old-commit", "h1");
        store_resolution(&conn, "task-2", "old-commit", "h2");
        store_resolution(&conn, "task-3", "current", "h3");

        let deleted = invalidate_on_integration(&conn, "current").unwrap();
        assert_eq!(deleted, 2);

        assert!(file_resolution::get(&conn, "task-3", "current", "h3")
            .unwrap()
            .is_some());
        assert!(file_resolution::get(&conn, "task-1", "old-commit", "h1")
            .unwrap()
            .is_none());
    }

    #[test]
    fn invalidate_on_integration_noop_when_all_fresh() {
        let conn = setup_db();
        store_resolution(&conn, "task-1", "current", "h1");

        let deleted = invalidate_on_integration(&conn, "current").unwrap();
        assert_eq!(deleted, 0);
    }

    // --- regenerate_after_refactor tests ---

    #[test]
    fn regenerate_after_refactor_full_workflow() {
        let conn = setup_db();
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("src")).unwrap();
        std::fs::write(tmp.path().join("src/main.rs"), "fn main() {}").unwrap();

        // Setup: 3 tasks with intents, resolutions at old commit
        store_intent(&conn, "task-1", "h1");
        store_intent(&conn, "task-2", "h2");
        store_intent(&conn, "task-3", "h3");
        store_resolution(&conn, "task-1", "old-commit", "h1");
        store_resolution(&conn, "task-2", "old-commit", "h2");
        // task-3 has no existing resolution

        let report = regenerate_after_refactor(
            &conn,
            tmp.path(),
            "new-commit",
            &["task-1", "task-2", "task-3"],
        )
        .unwrap();

        // All 3 should be regenerated (old ones invalidated, task-3 had none)
        assert_eq!(report.invalidated, 2); // task-1 and task-2 old entries
        assert_eq!(report.regenerated, 3);
        assert_eq!(report.already_fresh, 0);
        assert_eq!(report.skipped_no_intent, 0);
    }

    #[test]
    fn regenerate_after_refactor_skips_no_intent() {
        let conn = setup_db();
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("src")).unwrap();
        std::fs::write(tmp.path().join("src/main.rs"), "fn main() {}").unwrap();

        // Only task-1 has intent
        store_intent(&conn, "task-1", "h1");

        let report = regenerate_after_refactor(
            &conn,
            tmp.path(),
            "new-commit",
            &["task-1", "task-no-intent"],
        )
        .unwrap();

        assert_eq!(report.regenerated, 1);
        assert_eq!(report.skipped_no_intent, 1);
    }

    #[test]
    fn regenerate_after_refactor_already_fresh() {
        let conn = setup_db();
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("src")).unwrap();
        std::fs::write(tmp.path().join("src/main.rs"), "fn main() {}").unwrap();

        // task-1 already has fresh resolution at current commit
        store_intent(&conn, "task-1", "h1");
        store_resolution(&conn, "task-1", "current-commit", "h1");

        let report =
            regenerate_after_refactor(&conn, tmp.path(), "current-commit", &["task-1"]).unwrap();

        assert_eq!(report.invalidated, 0);
        assert_eq!(report.regenerated, 0);
        assert_eq!(report.already_fresh, 1);
    }

    #[test]
    fn regenerate_after_refactor_empty_task_list() {
        let conn = setup_db();
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("src")).unwrap();
        std::fs::write(tmp.path().join("src/main.rs"), "fn main() {}").unwrap();

        // Old resolution exists but no tasks to regenerate
        store_resolution(&conn, "task-1", "old-commit", "h1");

        let report = regenerate_after_refactor(&conn, tmp.path(), "new-commit", &[]).unwrap();

        assert_eq!(report.invalidated, 1); // old entry still cleaned up
        assert_eq!(report.regenerated, 0);
        assert_eq!(report.already_fresh, 0);
        assert_eq!(report.skipped_no_intent, 0);
    }

    #[test]
    fn regenerate_after_refactor_mixed_states() {
        let conn = setup_db();
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("src")).unwrap();
        std::fs::write(tmp.path().join("src/main.rs"), "fn main() {}").unwrap();

        // task-1: has intent + stale resolution → regenerated
        store_intent(&conn, "task-1", "h1");
        store_resolution(&conn, "task-1", "old", "h1");

        // task-2: has intent + fresh resolution → already_fresh
        store_intent(&conn, "task-2", "h2");
        store_resolution(&conn, "task-2", "new-commit", "h2");

        // task-3: no intent → skipped
        // task-4: has intent, no resolution → regenerated
        store_intent(&conn, "task-4", "h4");

        let report = regenerate_after_refactor(
            &conn,
            tmp.path(),
            "new-commit",
            &["task-1", "task-2", "task-3", "task-4"],
        )
        .unwrap();

        assert_eq!(report.invalidated, 1); // task-1's old entry
        assert_eq!(report.regenerated, 2); // task-1 and task-4
        assert_eq!(report.already_fresh, 1); // task-2
        assert_eq!(report.skipped_no_intent, 1); // task-3
    }

    // --- ensure_fresh_metadata tests ---

    #[test]
    fn ensure_fresh_metadata_returns_none_without_intent() {
        let conn = setup_db();
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("src")).unwrap();
        std::fs::write(tmp.path().join("src/main.rs"), "fn main() {}").unwrap();

        let result = ensure_fresh_metadata(&conn, tmp.path(), "no-task", "commit1").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn ensure_fresh_metadata_returns_combined_on_cache_hit() {
        let conn = setup_db();
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("src")).unwrap();
        std::fs::write(tmp.path().join("src/main.rs"), "fn main() {}").unwrap();

        store_intent(&conn, "task-1", "hash1");
        store_resolution(&conn, "task-1", "commit-a", "hash1");

        let meta = ensure_fresh_metadata(&conn, tmp.path(), "task-1", "commit-a")
            .unwrap()
            .unwrap();
        assert_eq!(meta.task_id, "task-1");
        assert_eq!(meta.intent.task_id, "task-1");
        assert_eq!(meta.intent.content_hash, "hash1");
        assert_eq!(meta.resolution.base_commit, "commit-a");
    }

    #[test]
    fn ensure_fresh_metadata_returns_combined_on_regeneration() {
        let conn = setup_db();
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("src")).unwrap();
        std::fs::write(tmp.path().join("src/main.rs"), "fn main() {}").unwrap();

        store_intent(&conn, "task-1", "hash1");
        // No resolution stored → will regenerate

        let meta = ensure_fresh_metadata(&conn, tmp.path(), "task-1", "commit-new")
            .unwrap()
            .unwrap();
        assert_eq!(meta.task_id, "task-1");
        assert_eq!(meta.intent.content_hash, "hash1");
        assert_eq!(meta.resolution.base_commit, "commit-new");
    }

    #[test]
    fn task_metadata_affected_globs_from_resolution() {
        let conn = setup_db();
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("src")).unwrap();
        std::fs::write(tmp.path().join("src/main.rs"), "mod config;\nfn main() {}").unwrap();
        std::fs::write(tmp.path().join("src/config.rs"), "pub struct Config;").unwrap();

        // Store intent with concept "config" so resolution finds config.rs
        let analysis = IntentAnalysis {
            task_id: "task-globs".to_string(),
            content_hash: "hg".to_string(),
            target_areas: vec![TargetArea {
                concept: "config".to_string(),
                reasoning: "config changes".to_string(),
            }],
        };
        intent::store(&conn, &analysis).unwrap();

        let meta = ensure_fresh_metadata(&conn, tmp.path(), "task-globs", "commit-x")
            .unwrap()
            .unwrap();

        let globs = meta.affected_globs();
        assert!(!globs.is_empty());
        assert!(globs.iter().any(|g| g.contains("config")));
    }

    // --- detect_drift tests ---

    fn make_resolution(
        task_id: &str,
        base_commit: &str,
        intent_hash: &str,
        mappings: Vec<(&str, Vec<&str>)>,
    ) -> FileResolution {
        FileResolution {
            task_id: task_id.to_string(),
            base_commit: base_commit.to_string(),
            intent_hash: intent_hash.to_string(),
            mappings: mappings
                .into_iter()
                .map(|(concept, files)| FileResolutionMapping {
                    concept: concept.to_string(),
                    resolved_files: files.into_iter().map(|f| f.to_string()).collect(),
                    resolved_modules: vec![],
                })
                .collect(),
            derived: DerivedFields::default(),
        }
    }

    #[test]
    fn drift_no_previous_resolution_returns_none() {
        let conn = setup_db();
        let new_res = make_resolution("task-1", "commit-b", "hash1", vec![("auth", vec!["a.rs"])]);
        let report = detect_drift(&conn, &new_res, None).unwrap();
        assert!(report.is_none());
    }

    #[test]
    fn drift_same_commit_and_hash_returns_none() {
        let conn = setup_db();
        // Store a resolution then compare against one with same commit+hash
        let res = make_resolution("task-1", "commit-a", "hash1", vec![("auth", vec!["a.rs"])]);
        file_resolution::store(&conn, &res).unwrap();

        let new_res = make_resolution("task-1", "commit-a", "hash1", vec![("auth", vec!["a.rs"])]);
        let report = detect_drift(&conn, &new_res, None).unwrap();
        assert!(report.is_none()); // same entry, skip
    }

    #[test]
    fn drift_below_threshold_no_drift() {
        let conn = setup_db();
        // Old: auth -> 3 files
        let old = make_resolution(
            "task-1",
            "commit-a",
            "hash1",
            vec![("auth", vec!["a.rs", "b.rs", "c.rs"])],
        );
        file_resolution::store(&conn, &old).unwrap();

        // New: auth -> 5 files (1.67x, below 3x threshold)
        let new_res = make_resolution(
            "task-1",
            "commit-b",
            "hash1",
            vec![("auth", vec!["a.rs", "b.rs", "c.rs", "d.rs", "e.rs"])],
        );
        let report = detect_drift(&conn, &new_res, None).unwrap().unwrap();
        assert!(!report.has_drift());
        assert_eq!(report.old_total_files, 3);
        assert_eq!(report.new_total_files, 5);
    }

    #[test]
    fn drift_above_threshold_flags_concept() {
        let conn = setup_db();
        // Old: auth -> 3 files
        let old = make_resolution(
            "task-1",
            "commit-a",
            "hash1",
            vec![("auth", vec!["a.rs", "b.rs", "c.rs"])],
        );
        file_resolution::store(&conn, &old).unwrap();

        // New: auth -> 11 files (3.67x, above 3x threshold)
        let new_res = make_resolution(
            "task-1",
            "commit-b",
            "hash1",
            vec![(
                "auth",
                vec![
                    "a.rs", "b.rs", "c.rs", "d.rs", "e.rs", "f.rs", "g.rs", "h.rs", "i.rs", "j.rs",
                    "k.rs",
                ],
            )],
        );
        let report = detect_drift(&conn, &new_res, None).unwrap().unwrap();
        assert!(report.has_drift());
        assert_eq!(report.drifted_concepts.len(), 1);
        assert_eq!(report.drifted_concepts[0].concept, "auth");
        assert_eq!(report.drifted_concepts[0].old_file_count, 3);
        assert_eq!(report.drifted_concepts[0].new_file_count, 11);
    }

    #[test]
    fn drift_exactly_at_threshold() {
        let conn = setup_db();
        // Old: auth -> 2 files, New: auth -> 6 files (3.0x, exactly at threshold)
        let old = make_resolution(
            "task-1",
            "commit-a",
            "hash1",
            vec![("auth", vec!["a.rs", "b.rs"])],
        );
        file_resolution::store(&conn, &old).unwrap();

        let new_res = make_resolution(
            "task-1",
            "commit-b",
            "hash1",
            vec![("auth", vec!["a.rs", "b.rs", "c.rs", "d.rs", "e.rs", "f.rs"])],
        );
        let report = detect_drift(&conn, &new_res, None).unwrap().unwrap();
        assert!(report.has_drift()); // >= 3x triggers
    }

    #[test]
    fn drift_from_zero_to_positive_always_flags() {
        let conn = setup_db();
        // Old: auth -> 0 files (concept existed but no matches)
        let old = make_resolution("task-1", "commit-a", "hash1", vec![("auth", vec![])]);
        file_resolution::store(&conn, &old).unwrap();

        // New: auth -> 2 files
        let new_res = make_resolution(
            "task-1",
            "commit-b",
            "hash1",
            vec![("auth", vec!["a.rs", "b.rs"])],
        );
        let report = detect_drift(&conn, &new_res, None).unwrap().unwrap();
        assert!(report.has_drift());
        assert_eq!(report.drifted_concepts[0].old_file_count, 0);
        assert_eq!(report.drifted_concepts[0].new_file_count, 2);
    }

    #[test]
    fn drift_new_concept_not_in_old_flags() {
        let conn = setup_db();
        // Old: only "auth" concept
        let old = make_resolution("task-1", "commit-a", "hash1", vec![("auth", vec!["a.rs"])]);
        file_resolution::store(&conn, &old).unwrap();

        // New: has "auth" + "db" (db is new, was 0 → 3)
        let new_res = make_resolution(
            "task-1",
            "commit-b",
            "hash2",
            vec![("auth", vec!["a.rs"]), ("db", vec!["d.rs", "e.rs", "f.rs"])],
        );
        let report = detect_drift(&conn, &new_res, None).unwrap().unwrap();
        assert!(report.has_drift());
        assert_eq!(report.drifted_concepts.len(), 1);
        assert_eq!(report.drifted_concepts[0].concept, "db");
    }

    #[test]
    fn drift_custom_threshold() {
        let conn = setup_db();
        // Old: auth -> 2 files
        let old = make_resolution(
            "task-1",
            "commit-a",
            "hash1",
            vec![("auth", vec!["a.rs", "b.rs"])],
        );
        file_resolution::store(&conn, &old).unwrap();

        // New: auth -> 3 files (1.5x)
        let new_res = make_resolution(
            "task-1",
            "commit-b",
            "hash1",
            vec![("auth", vec!["a.rs", "b.rs", "c.rs"])],
        );

        // At 1.5x threshold: 3/2 = 1.5 which is >= 1.5, so flags
        let report = detect_drift(&conn, &new_res, Some(1.5)).unwrap().unwrap();
        assert!(report.has_drift());

        // At 2.0x threshold: 3/2 = 1.5 which is < 2.0, so no drift
        // Need to re-store old since it was consumed
        file_resolution::store(&conn, &old).unwrap();
        let report = detect_drift(&conn, &new_res, Some(2.0)).unwrap().unwrap();
        assert!(!report.has_drift());
    }

    #[test]
    fn drift_multiple_concepts_mixed() {
        let conn = setup_db();
        // Old: auth -> 2, config -> 5
        let old = make_resolution(
            "task-1",
            "commit-a",
            "hash1",
            vec![
                ("auth", vec!["a1.rs", "a2.rs"]),
                ("config", vec!["c1.rs", "c2.rs", "c3.rs", "c4.rs", "c5.rs"]),
            ],
        );
        file_resolution::store(&conn, &old).unwrap();

        // New: auth -> 8 (4x, drifted), config -> 7 (1.4x, not drifted)
        let new_res = make_resolution(
            "task-1",
            "commit-b",
            "hash1",
            vec![
                (
                    "auth",
                    vec![
                        "a1.rs", "a2.rs", "a3.rs", "a4.rs", "a5.rs", "a6.rs", "a7.rs", "a8.rs",
                    ],
                ),
                (
                    "config",
                    vec![
                        "c1.rs", "c2.rs", "c3.rs", "c4.rs", "c5.rs", "c6.rs", "c7.rs",
                    ],
                ),
            ],
        );
        let report = detect_drift(&conn, &new_res, None).unwrap().unwrap();
        assert!(report.has_drift());
        assert_eq!(report.drifted_concepts.len(), 1);
        assert_eq!(report.drifted_concepts[0].concept, "auth");
        assert_eq!(report.old_total_files, 7); // 2 + 5
        assert_eq!(report.new_total_files, 15); // 8 + 7
    }

    #[test]
    fn drift_summary_no_drift() {
        let report = DriftReport {
            task_id: "task-1".to_string(),
            threshold: 3.0,
            drifted_concepts: vec![],
            old_total_files: 3,
            new_total_files: 4,
        };
        assert!(report.summary().contains("no metadata drift detected"));
    }

    #[test]
    fn drift_summary_with_drift() {
        let report = DriftReport {
            task_id: "task-1".to_string(),
            threshold: 3.0,
            drifted_concepts: vec![ConceptDrift {
                concept: "auth".to_string(),
                old_file_count: 3,
                new_file_count: 11,
            }],
            old_total_files: 3,
            new_total_files: 11,
        };
        let s = report.summary();
        assert!(s.contains("metadata drift detected"));
        assert!(s.contains("3 → 11"));
        assert!(s.contains("auth"));
    }

    #[test]
    fn task_metadata_affected_globs_empty_when_no_matches() {
        let conn = setup_db();
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("src")).unwrap();
        std::fs::write(tmp.path().join("src/main.rs"), "fn main() {}").unwrap();

        // Intent with concept that won't match any files
        let analysis = IntentAnalysis {
            task_id: "task-noglobs".to_string(),
            content_hash: "hng".to_string(),
            target_areas: vec![TargetArea {
                concept: "nonexistent_module".to_string(),
                reasoning: "nothing".to_string(),
            }],
        };
        intent::store(&conn, &analysis).unwrap();

        let meta = ensure_fresh_metadata(&conn, tmp.path(), "task-noglobs", "commit-y")
            .unwrap()
            .unwrap();

        let globs = meta.affected_globs();
        assert!(globs.is_empty());
    }
}
