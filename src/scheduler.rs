//! Scheduler for assigning beads to idle workers.
//!
//! Beads declare their affected set in the `design` field:
//!   `affected: src/analytics/**/*.rs, src/lib.rs`
//!
//! The scheduler uses these sets to determine which tasks can run in parallel.
//! Two tasks overlap if any of their globs could match the same files.
//! A bead without an affected set is treated as affecting everything.
//!
//! The scheduler runs when a worker becomes idle. It queries for ready beads
//! (open status, all deps closed), filters by affected set overlap with
//! in-progress tasks, and assigns the highest-priority non-conflicting task.

/// Parse the affected set from a bead's design field.
///
/// Looks for a line matching `affected: <glob>, <glob>, ...` (case-insensitive key).
/// Returns `None` if no affected line is found (meaning "affects everything").
/// Returns `Some(vec![])` if the line is present but empty (unusual but handled).
pub fn parse_affected_set(design: &str) -> Option<Vec<String>> {
    for line in design.lines() {
        let trimmed = line.trim();
        // Match "affected:" prefix case-insensitively
        if let Some(rest) = strip_prefix_ci(trimmed, "affected:") {
            let globs: Vec<String> = rest
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            return Some(globs);
        }
    }
    None
}

/// Check if two sets of file globs could potentially match the same files.
///
/// Returns `true` if there is potential overlap (conservative — false positives are OK,
/// false negatives are not). Returns `true` if either set is empty (meaning "affects everything").
pub fn globs_overlap(a: &[String], b: &[String]) -> bool {
    // Empty set means "affects everything"
    if a.is_empty() || b.is_empty() {
        return true;
    }

    for ga in a {
        for gb in b {
            if pair_overlaps(ga, gb) {
                return true;
            }
        }
    }
    false
}

/// Check if two individual glob patterns could match the same file.
///
/// Uses a conservative heuristic: extract the literal prefix of each glob
/// (everything before the first wildcard character) and check if one prefix
/// is a prefix of the other. If so, they could overlap.
///
/// Examples:
/// - `src/analytics/**/*.rs` vs `src/analytics/mod.rs` → overlap (shared prefix)
/// - `src/db.rs` vs `src/config.rs` → no overlap (exact files, different)
/// - `src/**` vs `src/db.rs` → overlap (prefix containment)
/// - `tests/**` vs `src/**` → no overlap (different prefixes)
fn pair_overlaps(a: &str, b: &str) -> bool {
    let pa = literal_prefix(a);
    let pb = literal_prefix(b);

    // If either glob is just a wildcard with no prefix, it matches everything
    if pa.is_empty() || pb.is_empty() {
        return true;
    }

    // Two exact paths (no wildcards) only overlap if they're equal
    if !has_wildcard(a) && !has_wildcard(b) {
        return a == b;
    }

    // Check if one prefix starts with the other (either direction)
    pa.starts_with(pb) || pb.starts_with(pa)
}

/// Extract the literal prefix of a glob pattern — everything before the first
/// wildcard character (`*`, `?`, `[`).
///
/// For `src/analytics/**/*.rs` returns `src/analytics/`.
/// For `src/db.rs` returns `src/db.rs`.
/// For `**/*.rs` returns `` (empty).
fn literal_prefix(glob: &str) -> &str {
    let end = glob.find(['*', '?', '[']).unwrap_or(glob.len());
    &glob[..end]
}

/// Check if a pattern contains any wildcard characters.
fn has_wildcard(pattern: &str) -> bool {
    pattern.contains('*') || pattern.contains('?') || pattern.contains('[')
}

/// Case-insensitive prefix strip. Returns the remainder after the prefix if it matches.
fn strip_prefix_ci<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    if s.len() >= prefix.len() && s[..prefix.len()].eq_ignore_ascii_case(prefix) {
        Some(&s[prefix.len()..])
    } else {
        None
    }
}

// ── Scheduler types and logic ──────────────────────────────────────

/// A bead that is ready for assignment (open, all deps closed).
#[derive(Debug, Clone)]
pub struct ReadyBead {
    /// The bead identifier (e.g. "beads-abc-123").
    pub id: String,
    /// Priority (lower number = higher priority; 0 = critical, 4 = backlog).
    #[allow(dead_code)]
    pub priority: u32,
    /// Parsed affected globs from the bead's design field.
    /// `None` means the bead didn't declare an affected set (treats as "everything").
    pub affected_globs: Option<Vec<String>>,
}

/// An in-progress assignment with its locked affected set.
#[derive(Debug, Clone)]
pub struct InProgressAssignment {
    /// The bead identifier being worked on.
    #[allow(dead_code)]
    pub bead_id: String,
    /// Parsed affected globs for this assignment.
    /// `None` means it affects everything.
    pub affected_globs: Option<Vec<String>>,
}

/// Return the list of bead IDs from `ready_beads` whose affected sets
/// do not overlap with any in-progress task's affected set.
///
/// Beads are returned in their original order (assumed to be sorted by priority).
/// A bead with no affected set (`None`) is treated as affecting everything and
/// will conflict with all in-progress tasks (unless there are none in progress).
pub fn next_assignable_tasks(
    ready_beads: &[ReadyBead],
    in_progress: &[InProgressAssignment],
) -> Vec<String> {
    // If nothing is in progress, all ready beads are assignable.
    if in_progress.is_empty() {
        return ready_beads.iter().map(|b| b.id.clone()).collect();
    }

    // Collect all locked globs from in-progress assignments.
    // If any in-progress task has no affected set, it locks everything.
    let mut locked_globs: Vec<&str> = Vec::new();
    let mut any_in_progress_locks_all = false;

    for a in in_progress {
        match &a.affected_globs {
            None => {
                any_in_progress_locks_all = true;
                break;
            }
            Some(globs) => {
                for g in globs {
                    locked_globs.push(g.as_str());
                }
            }
        }
    }

    // If any in-progress task locks everything, no new tasks can be assigned.
    if any_in_progress_locks_all {
        return Vec::new();
    }

    let locked_owned: Vec<String> = locked_globs.iter().map(|s| s.to_string()).collect();

    ready_beads
        .iter()
        .filter(|b| {
            match &b.affected_globs {
                // No affected set = affects everything → conflicts with any lock.
                None => false,
                Some(bead_globs) => !globs_overlap(bead_globs, &locked_owned),
            }
        })
        .map(|b| b.id.clone())
        .collect()
}

/// Pick the single highest-priority bead that can be assigned without
/// conflicting with any in-progress task.
///
/// `ready_beads` should already be sorted by priority (ascending).
/// Returns `None` if no bead can be assigned.
#[allow(dead_code)]
pub fn schedule_next(
    ready_beads: &[ReadyBead],
    in_progress: &[InProgressAssignment],
) -> Option<String> {
    next_assignable_tasks(ready_beads, in_progress)
        .into_iter()
        .next()
}

/// Enrich a `ReadyBead`'s affected globs using fresh metadata from the database.
///
/// If a bead has no `affected_globs` (no `affected:` line in its design), this
/// function uses `ensure_fresh_metadata` to compute file resolution and derive
/// affected globs from the resolved files. This makes the scheduler's conflict
/// detection more precise — instead of treating the bead as "affects everything",
/// it uses the actual files the task touches.
///
/// If the bead already has explicit affected globs (from the design field), they
/// are preserved as-is (explicit declarations take precedence over inferred metadata).
///
/// Returns `true` if the bead's globs were updated from metadata.
pub fn enrich_with_metadata(
    bead: &mut ReadyBead,
    conn: &rusqlite::Connection,
    repo_root: &std::path::Path,
    current_commit: &str,
) -> bool {
    // Only enrich beads that don't already have explicit affected globs
    if bead.affected_globs.is_some() {
        return false;
    }

    match crate::metadata_regen::ensure_fresh_metadata(conn, repo_root, &bead.id, current_commit) {
        Ok(Some(metadata)) => {
            let globs = metadata.affected_globs();
            if !globs.is_empty() {
                bead.affected_globs = Some(globs);
                true
            } else {
                false
            }
        }
        Ok(None) | Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── parse_affected_set tests ──

    #[test]
    fn parse_single_glob() {
        let design = "affected: src/analytics/**/*.rs";
        let result = parse_affected_set(design);
        assert_eq!(result, Some(vec!["src/analytics/**/*.rs".to_string()]));
    }

    #[test]
    fn parse_multiple_globs() {
        let design = "affected: src/analytics/**/*.rs, src/lib.rs, tests/integration/**";
        let result = parse_affected_set(design).unwrap();
        assert_eq!(
            result,
            vec![
                "src/analytics/**/*.rs",
                "src/lib.rs",
                "tests/integration/**",
            ]
        );
    }

    #[test]
    fn parse_with_surrounding_text() {
        let design = "This task adds analytics.\naffected: src/analytics/**/*.rs, src/lib.rs\nSee also: docs/";
        let result = parse_affected_set(design).unwrap();
        assert_eq!(result, vec!["src/analytics/**/*.rs", "src/lib.rs"]);
    }

    #[test]
    fn parse_case_insensitive() {
        let design = "Affected: src/db.rs";
        assert_eq!(
            parse_affected_set(design),
            Some(vec!["src/db.rs".to_string()])
        );

        let design2 = "AFFECTED: src/db.rs";
        assert_eq!(
            parse_affected_set(design2),
            Some(vec!["src/db.rs".to_string()])
        );
    }

    #[test]
    fn parse_no_affected_line() {
        let design = "This is just a description with no affected set.";
        assert_eq!(parse_affected_set(design), None);
    }

    #[test]
    fn parse_empty_affected_line() {
        let design = "affected:";
        assert_eq!(parse_affected_set(design), Some(vec![]));
    }

    #[test]
    fn parse_whitespace_handling() {
        let design = "  affected:   src/foo.rs ,  src/bar.rs  ";
        let result = parse_affected_set(design).unwrap();
        assert_eq!(result, vec!["src/foo.rs", "src/bar.rs"]);
    }

    #[test]
    fn parse_empty_design() {
        assert_eq!(parse_affected_set(""), None);
    }

    // ── globs_overlap tests ──

    #[test]
    fn overlap_empty_a_matches_everything() {
        let a: Vec<String> = vec![];
        let b = vec!["src/foo.rs".to_string()];
        assert!(globs_overlap(&a, &b));
    }

    #[test]
    fn overlap_empty_b_matches_everything() {
        let a = vec!["src/foo.rs".to_string()];
        let b: Vec<String> = vec![];
        assert!(globs_overlap(&a, &b));
    }

    #[test]
    fn overlap_both_empty() {
        let a: Vec<String> = vec![];
        let b: Vec<String> = vec![];
        assert!(globs_overlap(&a, &b));
    }

    #[test]
    fn overlap_same_exact_file() {
        let a = vec!["src/db.rs".to_string()];
        let b = vec!["src/db.rs".to_string()];
        assert!(globs_overlap(&a, &b));
    }

    #[test]
    fn overlap_different_exact_files() {
        let a = vec!["src/db.rs".to_string()];
        let b = vec!["src/config.rs".to_string()];
        assert!(!globs_overlap(&a, &b));
    }

    #[test]
    fn overlap_glob_contains_file() {
        let a = vec!["src/analytics/**/*.rs".to_string()];
        let b = vec!["src/analytics/mod.rs".to_string()];
        assert!(globs_overlap(&a, &b));
    }

    #[test]
    fn overlap_disjoint_directories() {
        let a = vec!["src/analytics/**/*.rs".to_string()];
        let b = vec!["tests/integration/**".to_string()];
        assert!(!globs_overlap(&a, &b));
    }

    #[test]
    fn overlap_nested_globs() {
        let a = vec!["src/**".to_string()];
        let b = vec!["src/analytics/**/*.rs".to_string()];
        assert!(globs_overlap(&a, &b));
    }

    #[test]
    fn overlap_root_wildcard() {
        let a = vec!["**/*.rs".to_string()];
        let b = vec!["src/db.rs".to_string()];
        assert!(globs_overlap(&a, &b));
    }

    #[test]
    fn overlap_multiple_globs_one_overlaps() {
        let a = vec!["src/db.rs".to_string(), "src/config.rs".to_string()];
        let b = vec!["tests/**".to_string(), "src/db.rs".to_string()];
        assert!(globs_overlap(&a, &b));
    }

    #[test]
    fn overlap_multiple_globs_none_overlap() {
        let a = vec!["src/analytics/**".to_string(), "src/metrics.rs".to_string()];
        let b = vec!["tests/**".to_string(), "docs/**".to_string()];
        assert!(!globs_overlap(&a, &b));
    }

    // ── pair_overlaps tests ──

    #[test]
    fn pair_exact_same() {
        assert!(pair_overlaps("src/db.rs", "src/db.rs"));
    }

    #[test]
    fn pair_exact_different() {
        assert!(!pair_overlaps("src/db.rs", "src/config.rs"));
    }

    #[test]
    fn pair_glob_vs_file_overlap() {
        assert!(pair_overlaps("src/**", "src/db.rs"));
    }

    #[test]
    fn pair_glob_vs_file_no_overlap() {
        assert!(!pair_overlaps("tests/**", "src/db.rs"));
    }

    #[test]
    fn pair_both_wildcards_overlap() {
        assert!(pair_overlaps("src/**", "src/analytics/**"));
    }

    #[test]
    fn pair_both_wildcards_no_overlap() {
        assert!(!pair_overlaps("src/**", "tests/**"));
    }

    // ── literal_prefix tests ──

    #[test]
    fn prefix_no_wildcards() {
        assert_eq!(literal_prefix("src/db.rs"), "src/db.rs");
    }

    #[test]
    fn prefix_double_star() {
        assert_eq!(literal_prefix("src/analytics/**/*.rs"), "src/analytics/");
    }

    #[test]
    fn prefix_star_only() {
        assert_eq!(literal_prefix("**/*.rs"), "");
    }

    #[test]
    fn prefix_question_mark() {
        assert_eq!(literal_prefix("src/?.rs"), "src/");
    }

    #[test]
    fn prefix_bracket() {
        assert_eq!(literal_prefix("src/[abc].rs"), "src/");
    }

    // ── strip_prefix_ci tests ──

    #[test]
    fn strip_ci_match() {
        assert_eq!(strip_prefix_ci("Affected: foo", "affected:"), Some(" foo"));
    }

    #[test]
    fn strip_ci_no_match() {
        assert_eq!(strip_prefix_ci("something: foo", "affected:"), None);
    }

    #[test]
    fn strip_ci_exact() {
        assert_eq!(strip_prefix_ci("affected:", "affected:"), Some(""));
    }

    // ── next_assignable_tasks tests ──

    fn bead(id: &str, priority: u32, globs: Option<Vec<&str>>) -> ReadyBead {
        ReadyBead {
            id: id.to_string(),
            priority,
            affected_globs: globs.map(|g| g.into_iter().map(|s| s.to_string()).collect()),
        }
    }

    fn assignment(bead_id: &str, globs: Option<Vec<&str>>) -> InProgressAssignment {
        InProgressAssignment {
            bead_id: bead_id.to_string(),
            affected_globs: globs.map(|g| g.into_iter().map(|s| s.to_string()).collect()),
        }
    }

    #[test]
    fn assignable_no_in_progress_returns_all() {
        let beads = vec![
            bead("b1", 1, Some(vec!["src/db.rs"])),
            bead("b2", 2, Some(vec!["src/config.rs"])),
        ];
        let result = next_assignable_tasks(&beads, &[]);
        assert_eq!(result, vec!["b1", "b2"]);
    }

    #[test]
    fn assignable_no_ready_beads_returns_empty() {
        let in_progress = vec![assignment("x1", Some(vec!["src/**"]))];
        let result = next_assignable_tasks(&[], &in_progress);
        assert!(result.is_empty());
    }

    #[test]
    fn assignable_overlapping_filtered_out() {
        let beads = vec![
            bead("b1", 1, Some(vec!["src/db.rs"])),
            bead("b2", 2, Some(vec!["tests/**"])),
        ];
        let in_progress = vec![assignment("x1", Some(vec!["src/**"]))];
        let result = next_assignable_tasks(&beads, &in_progress);
        // b1 overlaps with src/**, b2 (tests/**) does not
        assert_eq!(result, vec!["b2"]);
    }

    #[test]
    fn assignable_no_affected_set_on_bead_conflicts_with_everything() {
        let beads = vec![
            bead("b1", 1, None), // no affected set → conflicts
            bead("b2", 2, Some(vec!["tests/**"])),
        ];
        let in_progress = vec![assignment("x1", Some(vec!["src/db.rs"]))];
        let result = next_assignable_tasks(&beads, &in_progress);
        assert_eq!(result, vec!["b2"]);
    }

    #[test]
    fn assignable_no_affected_set_on_in_progress_locks_all() {
        let beads = vec![
            bead("b1", 1, Some(vec!["src/db.rs"])),
            bead("b2", 2, Some(vec!["tests/**"])),
        ];
        let in_progress = vec![assignment("x1", None)]; // locks everything
        let result = next_assignable_tasks(&beads, &in_progress);
        assert!(result.is_empty());
    }

    #[test]
    fn assignable_multiple_in_progress_combined_locks() {
        let beads = vec![
            bead("b1", 1, Some(vec!["src/db.rs"])),
            bead("b2", 2, Some(vec!["docs/**"])),
            bead("b3", 3, Some(vec!["tests/**"])),
        ];
        let in_progress = vec![
            assignment("x1", Some(vec!["src/**"])),
            assignment("x2", Some(vec!["tests/**"])),
        ];
        let result = next_assignable_tasks(&beads, &in_progress);
        // b1 overlaps src/**, b3 overlaps tests/**, only b2 (docs/**) is free
        assert_eq!(result, vec!["b2"]);
    }

    #[test]
    fn assignable_preserves_order() {
        let beads = vec![
            bead("p1", 1, Some(vec!["src/a.rs"])),
            bead("p2", 2, Some(vec!["src/b.rs"])),
            bead("p3", 3, Some(vec!["src/c.rs"])),
        ];
        // Lock something unrelated
        let in_progress = vec![assignment("x1", Some(vec!["tests/**"]))];
        let result = next_assignable_tasks(&beads, &in_progress);
        assert_eq!(result, vec!["p1", "p2", "p3"]);
    }

    #[test]
    fn assignable_disjoint_globs_all_assignable() {
        let beads = vec![
            bead("b1", 1, Some(vec!["src/db.rs"])),
            bead("b2", 2, Some(vec!["src/config.rs"])),
        ];
        let in_progress = vec![assignment("x1", Some(vec!["tests/**"]))];
        let result = next_assignable_tasks(&beads, &in_progress);
        assert_eq!(result, vec!["b1", "b2"]);
    }

    #[test]
    fn assignable_all_overlap() {
        let beads = vec![
            bead("b1", 1, Some(vec!["src/**"])),
            bead("b2", 2, Some(vec!["src/db.rs"])),
        ];
        let in_progress = vec![assignment("x1", Some(vec!["src/**"]))];
        let result = next_assignable_tasks(&beads, &in_progress);
        assert!(result.is_empty());
    }

    // ── schedule_next tests ──

    #[test]
    fn schedule_next_picks_first_assignable() {
        let beads = vec![
            bead("b1", 1, Some(vec!["src/db.rs"])),
            bead("b2", 2, Some(vec!["tests/**"])),
        ];
        let in_progress = vec![assignment("x1", Some(vec!["src/**"]))];
        // b1 overlaps, b2 doesn't → picks b2
        assert_eq!(schedule_next(&beads, &in_progress), Some("b2".to_string()));
    }

    #[test]
    fn schedule_next_picks_highest_priority_when_all_free() {
        let beads = vec![
            bead("low", 3, Some(vec!["src/a.rs"])),
            bead("high", 1, Some(vec!["src/b.rs"])),
        ];
        // Beads should be pre-sorted by caller; schedule_next picks the first
        let result = schedule_next(&beads, &[]);
        assert_eq!(result, Some("low".to_string())); // first in list order
    }

    #[test]
    fn schedule_next_none_when_all_conflict() {
        let beads = vec![bead("b1", 1, Some(vec!["src/**"]))];
        let in_progress = vec![assignment("x1", Some(vec!["src/**"]))];
        assert_eq!(schedule_next(&beads, &in_progress), None);
    }

    #[test]
    fn schedule_next_none_when_no_ready_beads() {
        let in_progress = vec![assignment("x1", Some(vec!["src/**"]))];
        assert_eq!(schedule_next(&[], &in_progress), None);
    }

    #[test]
    fn schedule_next_none_when_empty() {
        assert_eq!(schedule_next(&[], &[]), None);
    }

    #[test]
    fn assignable_bead_with_no_globs_and_no_in_progress() {
        // A bead with no affected set is assignable when nothing is in progress
        let beads = vec![bead("b1", 1, None)];
        let result = next_assignable_tasks(&beads, &[]);
        assert_eq!(result, vec!["b1"]);
    }

    // ── enrich_with_metadata tests ──

    fn setup_enrich_db() -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        crate::intent::create_table(&conn).unwrap();
        crate::file_resolution::create_table(&conn).unwrap();
        conn
    }

    #[test]
    fn enrich_skips_bead_with_existing_globs() {
        let conn = setup_enrich_db();
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("src")).unwrap();
        std::fs::write(tmp.path().join("src/main.rs"), "fn main() {}").unwrap();

        let mut b = bead("b1", 1, Some(vec!["src/db.rs"]));
        let changed = enrich_with_metadata(&mut b, &conn, tmp.path(), "commit1");
        assert!(!changed);
        assert_eq!(b.affected_globs, Some(vec!["src/db.rs".to_string()]));
    }

    #[test]
    fn enrich_populates_globs_from_metadata() {
        let conn = setup_enrich_db();
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("src")).unwrap();
        std::fs::write(tmp.path().join("src/main.rs"), "mod config;\nfn main() {}").unwrap();
        std::fs::write(tmp.path().join("src/config.rs"), "pub struct Config;").unwrap();

        // Store intent analysis so ensure_fresh_metadata can resolve
        let analysis = crate::intent::IntentAnalysis {
            task_id: "b1".to_string(),
            content_hash: "h1".to_string(),
            target_areas: vec![crate::intent::TargetArea {
                concept: "config".to_string(),
                reasoning: "config changes".to_string(),
            }],
        };
        crate::intent::store(&conn, &analysis).unwrap();

        let mut b = bead("b1", 1, None);
        let changed = enrich_with_metadata(&mut b, &conn, tmp.path(), "commit1");
        assert!(changed);
        assert!(b.affected_globs.is_some());
        let globs = b.affected_globs.unwrap();
        assert!(globs.iter().any(|g| g.contains("config")));
    }

    #[test]
    fn enrich_no_intent_leaves_none() {
        let conn = setup_enrich_db();
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("src")).unwrap();
        std::fs::write(tmp.path().join("src/main.rs"), "fn main() {}").unwrap();

        let mut b = bead("b-unknown", 1, None);
        let changed = enrich_with_metadata(&mut b, &conn, tmp.path(), "commit1");
        assert!(!changed);
        assert!(b.affected_globs.is_none());
    }
}
