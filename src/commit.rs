/// Commit detection: scan session output for indicators that the agent committed code.
///
/// Scans the full session output (not just the result event) for configurable
/// patterns such as `bd-finish`, `git commit`, etc. This allows the harness to
/// report whether a session was productive in terms of code commits.
use regex::Regex;
use std::path::Path;

/// Scan the output file for any of the given commit indicator patterns.
///
/// Returns `true` if any pattern matches anywhere in the file content.
pub fn detect_commit(output_path: &Path, patterns: &[Regex]) -> bool {
    if patterns.is_empty() {
        return false;
    }

    let contents = match std::fs::read_to_string(output_path) {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(
                error = %e,
                path = %output_path.display(),
                "failed to read output file for commit detection"
            );
            return false;
        }
    };

    detect_commit_in_text(&contents, patterns)
}

/// Check text content for any of the given commit indicator patterns.
fn detect_commit_in_text(text: &str, patterns: &[Regex]) -> bool {
    for pattern in patterns {
        if pattern.is_match(text) {
            tracing::debug!(pattern = %pattern, "commit indicator detected in session output");
            return true;
        }
    }
    false
}

/// Compile a list of pattern strings into regex objects.
///
/// Returns an error message for any pattern that fails to compile.
pub fn compile_patterns(patterns: &[String]) -> Result<Vec<Regex>, String> {
    let mut compiled = Vec::with_capacity(patterns.len());
    for p in patterns {
        match Regex::new(p) {
            Ok(r) => compiled.push(r),
            Err(e) => return Err(format!("invalid commit detection pattern '{}': {}", p, e)),
        }
    }
    Ok(compiled)
}

/// Default commit detection patterns.
pub fn default_patterns() -> Vec<String> {
    vec![
        r"bd-finish".to_string(),
        r"(?i)git commit".to_string(),
        r"(?i)\bcommitted\b".to_string(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn test_patterns() -> Vec<Regex> {
        compile_patterns(&default_patterns()).unwrap()
    }

    #[test]
    fn test_detect_bd_finish() {
        let patterns = test_patterns();
        assert!(detect_commit_in_text(
            "running bd-finish.sh bead-123",
            &patterns
        ));
    }

    #[test]
    fn test_detect_git_commit() {
        let patterns = test_patterns();
        assert!(detect_commit_in_text(
            "$ git commit -m 'fix bug'",
            &patterns
        ));
    }

    #[test]
    fn test_detect_git_commit_case_insensitive() {
        let patterns = test_patterns();
        assert!(detect_commit_in_text("Git Commit completed", &patterns));
    }

    #[test]
    fn test_detect_committed() {
        let patterns = test_patterns();
        assert!(detect_commit_in_text(
            "Changes committed successfully",
            &patterns
        ));
    }

    #[test]
    fn test_no_commit_detected() {
        let patterns = test_patterns();
        assert!(!detect_commit_in_text(
            "just some regular output\nno commits here",
            &patterns
        ));
    }

    #[test]
    fn test_empty_output() {
        let patterns = test_patterns();
        assert!(!detect_commit_in_text("", &patterns));
    }

    #[test]
    fn test_empty_patterns() {
        let patterns: Vec<Regex> = vec![];
        assert!(!detect_commit_in_text("git commit -m 'test'", &patterns));
    }

    #[test]
    fn test_detect_commit_from_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("output.jsonl");
        std::fs::write(&path, "session output\nbd-finish.sh completed\n").unwrap();
        let patterns = test_patterns();
        assert!(detect_commit(&path, &patterns));
    }

    #[test]
    fn test_detect_no_commit_from_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("output.jsonl");
        std::fs::write(&path, "session output\nno commits\n").unwrap();
        let patterns = test_patterns();
        assert!(!detect_commit(&path, &patterns));
    }

    #[test]
    fn test_detect_commit_missing_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("nonexistent.jsonl");
        let patterns = test_patterns();
        assert!(!detect_commit(&path, &patterns));
    }

    #[test]
    fn test_compile_patterns_valid() {
        let patterns = vec!["bd-finish".to_string(), r"(?i)git commit".to_string()];
        assert!(compile_patterns(&patterns).is_ok());
        assert_eq!(compile_patterns(&patterns).unwrap().len(), 2);
    }

    #[test]
    fn test_compile_patterns_invalid() {
        let patterns = vec!["[invalid".to_string()];
        assert!(compile_patterns(&patterns).is_err());
    }

    #[test]
    fn test_compile_patterns_empty() {
        let patterns: Vec<String> = vec![];
        let compiled = compile_patterns(&patterns).unwrap();
        assert!(compiled.is_empty());
    }

    #[test]
    fn test_custom_patterns() {
        let patterns = compile_patterns(&["my-custom-commit-marker".to_string()]).unwrap();
        assert!(detect_commit_in_text(
            "output with my-custom-commit-marker here",
            &patterns
        ));
        assert!(!detect_commit_in_text("output without marker", &patterns));
    }

    #[test]
    fn test_default_patterns_list() {
        let defaults = default_patterns();
        assert_eq!(defaults.len(), 3);
        // All should compile
        assert!(compile_patterns(&defaults).is_ok());
    }
}
