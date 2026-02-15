//! Affected-set parsing and glob overlap detection for multi-agent scheduling.
//!
//! Beads declare their affected set in the `design` field:
//!   `affected: src/analytics/**/*.rs, src/lib.rs`
//!
//! The scheduler uses these sets to determine which tasks can run in parallel.
//! Two tasks overlap if any of their globs could match the same files.
//! A bead without an affected set is treated as affecting everything.

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
}
