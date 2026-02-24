//! Scheduler for assigning beads to idle workers.
//!
//! Beads declare their affected set in the `design` field:
//!   `affected: src/analytics/**/*.rs, src/lib.rs`
//!
//! The scheduler uses these sets to determine which tasks can run in parallel.
//! Two tasks overlap if any of their globs could match the same files.
//! A bead without an affected set is treated optimistically — it is allowed to
//! run in parallel with other tasks, and conflicts are resolved at integration
//! time. Adding `affected:` lines to bead designs enables smarter scheduling.
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
    parse_markdown_affected_files(design)
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

fn parse_markdown_affected_files(design: &str) -> Option<Vec<String>> {
    let mut in_affected_section = false;
    let mut paths = Vec::new();

    for line in design.lines() {
        let trimmed = line.trim();

        if is_markdown_heading(trimmed) {
            let heading = trimmed.trim_start_matches('#').trim();
            in_affected_section = heading.eq_ignore_ascii_case("affected files");
            continue;
        }

        if !in_affected_section {
            continue;
        }

        if trimmed.is_empty() {
            continue;
        }

        if let Some(path) = parse_markdown_affected_bullet(trimmed) {
            paths.push(path);
        }
    }

    if paths.is_empty() {
        None
    } else {
        Some(paths)
    }
}

fn is_markdown_heading(line: &str) -> bool {
    if !line.starts_with('#') {
        return false;
    }
    let hashes = line.chars().take_while(|&c| c == '#').count();
    line.get(hashes..)
        .map(|rest| rest.starts_with(char::is_whitespace))
        .unwrap_or(false)
}

fn parse_markdown_affected_bullet(line: &str) -> Option<String> {
    let item = line
        .strip_prefix("- ")
        .or_else(|| line.strip_prefix("* "))
        .or_else(|| line.strip_prefix("• "))
        .map(str::trim)?;

    let candidate = item
        .trim_matches('`')
        .split_once(" (")
        .map(|(path, _)| path)
        .unwrap_or(item)
        .trim();

    if looks_like_file_path(candidate) {
        Some(candidate.to_string())
    } else {
        None
    }
}

fn looks_like_file_path(s: &str) -> bool {
    if s.contains('/') {
        return true;
    }

    const KNOWN_EXTENSIONS: [&str; 10] = [
        ".rs", ".toml", ".md", ".json", ".yaml", ".yml", ".sql", ".py", ".js", ".ts",
    ];

    KNOWN_EXTENSIONS.iter().any(|ext| s.ends_with(ext))
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
    /// Issue type from bd ("task", "epic", ...).
    #[allow(dead_code)]
    pub issue_type: String,
    /// Child IDs from parent-child dependencies (used to keep epics out of worker pool
    /// while children are still open).
    #[allow(dead_code)]
    pub parent_child_ids: Vec<String>,
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
/// A bead with no affected set (`None`) is treated optimistically — it is
/// allowed to run in parallel, with conflicts resolved at integration time.
pub fn next_assignable_tasks(
    ready_beads: &[ReadyBead],
    in_progress: &[InProgressAssignment],
) -> Vec<String> {
    // If nothing is in progress, all ready beads are assignable.
    if in_progress.is_empty() {
        return ready_beads.iter().map(|b| b.id.clone()).collect();
    }

    // Collect all locked globs from in-progress assignments.
    // In-progress tasks with no affected set are skipped (optimistic — they
    // don't block other work; conflicts are caught at integration time).
    let mut locked_globs: Vec<&str> = Vec::new();

    for a in in_progress {
        if let Some(globs) = &a.affected_globs {
            for g in globs {
                locked_globs.push(g.as_str());
            }
        }
    }

    // If no in-progress task has declared globs, everything is assignable.
    if locked_globs.is_empty() {
        return ready_beads.iter().map(|b| b.id.clone()).collect();
    }

    let locked_owned: Vec<String> = locked_globs.iter().map(|s| s.to_string()).collect();

    ready_beads
        .iter()
        .filter(|b| {
            match &b.affected_globs {
                // No affected set → optimistic: allow it, resolve at integration.
                None => true,
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

    #[test]
    fn parse_markdown_affected_files_fallback() {
        let design =
            "Task details\n\n## Affected files\n- src/scheduler.rs (modified)\n- Cargo.toml\n";
        let result = parse_affected_set(design).unwrap();
        assert_eq!(result, vec!["src/scheduler.rs", "Cargo.toml"]);
    }

    #[test]
    fn explicit_affected_line_takes_precedence_over_markdown_fallback() {
        let design = "affected: src/one.rs\n\n## Affected files\n- src/two.rs\n";
        let result = parse_affected_set(design).unwrap();
        assert_eq!(result, vec!["src/one.rs"]);
    }

    #[test]
    fn parse_markdown_heading_case_insensitive_and_multiple_bullets() {
        let design = "### Affected Files\n* src/lib.rs (new)\n• docs/spec.md (modified)\n";
        let result = parse_affected_set(design).unwrap();
        assert_eq!(result, vec!["src/lib.rs", "docs/spec.md"]);
    }

    #[test]
    fn parse_markdown_fallback_ignores_non_paths() {
        let design = "## Affected files\n- this is a note\n- changed parser behavior\n";
        assert_eq!(parse_affected_set(design), None);
    }

    #[test]
    fn parse_markdown_fallback_stops_at_next_heading() {
        let design = "## Affected files\n- src/lib.rs\n## Verify\n- Run: cargo test\n";
        let result = parse_affected_set(design).unwrap();
        assert_eq!(result, vec!["src/lib.rs"]);
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
            issue_type: "task".to_string(),
            parent_child_ids: Vec::new(),
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
    fn assignable_no_affected_set_on_bead_allowed_optimistically() {
        let beads = vec![
            bead("b1", 1, None), // no affected set → optimistic: allowed
            bead("b2", 2, Some(vec!["tests/**"])),
        ];
        let in_progress = vec![assignment("x1", Some(vec!["src/db.rs"]))];
        let result = next_assignable_tasks(&beads, &in_progress);
        assert_eq!(result, vec!["b1", "b2"]);
    }

    #[test]
    fn assignable_no_affected_set_on_in_progress_does_not_lock() {
        let beads = vec![
            bead("b1", 1, Some(vec!["src/db.rs"])),
            bead("b2", 2, Some(vec!["tests/**"])),
        ];
        let in_progress = vec![assignment("x1", None)]; // optimistic: doesn't lock
        let result = next_assignable_tasks(&beads, &in_progress);
        assert_eq!(result, vec!["b1", "b2"]);
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
}
