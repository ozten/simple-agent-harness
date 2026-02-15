/// Rate limit detection: inspect the final result event in session JSONL output.
///
/// Only the last `"type":"result"` event is inspected. A successful session
/// (`is_error: false`, `subtype: "success"`) is never classified as rate-limited,
/// regardless of what text the agent may have read during the session.
///
/// Rate limit patterns checked (only within error result events):
/// - JSON: `"error":"rate_limit"` or `"error": "rate_limit"`
/// - Text: `rate limit`, `rate_limit`, `usage limit`, `hit your limit` (case-insensitive)
use regex::Regex;
use std::path::Path;
use std::sync::LazyLock;

/// Compiled regex patterns for rate limit detection within result events.
static RATE_LIMIT_PATTERNS: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    vec![
        Regex::new(r"(?i)rate[_.\s]limit").unwrap(),
        Regex::new(r"(?i)usage limit").unwrap(),
        Regex::new(r"(?i)hit your limit").unwrap(),
    ]
});

/// Inspect the final result event in a JSONL file for rate limit indicators.
///
/// Returns `true` only if the session ended with an error that contains
/// rate limit patterns. Successful sessions are never classified as rate-limited.
pub fn detect_rate_limit(output_path: &Path) -> bool {
    let contents = match std::fs::read_to_string(output_path) {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(
                error = %e,
                path = %output_path.display(),
                "failed to read output file for rate limit check"
            );
            return false;
        }
    };

    detect_rate_limit_in_result_event(&contents)
}

/// Find the last `"type":"result"` line in JSONL content and check for rate limiting.
///
/// Returns `false` if:
/// - No result event is found
/// - The result event has `is_error: false` (successful session)
/// - The result event is an error but contains no rate limit patterns
fn detect_rate_limit_in_result_event(jsonl_content: &str) -> bool {
    // Find the last line containing "type":"result"
    let result_line = jsonl_content
        .lines()
        .rev()
        .find(|line| line.contains("\"type\":\"result\""));

    let result_line = match result_line {
        Some(line) => line,
        None => {
            tracing::debug!("no result event found in output, not rate-limited");
            return false;
        }
    };

    // Parse the result event as JSON
    let parsed: serde_json::Value = match serde_json::from_str(result_line) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(error = %e, "failed to parse result event JSON");
            return false;
        }
    };

    // A successful session is never rate-limited
    let is_error = parsed["is_error"].as_bool().unwrap_or(false);
    let subtype = parsed["subtype"].as_str().unwrap_or("");

    if !is_error && subtype != "error" {
        tracing::debug!(
            is_error,
            subtype,
            "successful session result, not rate-limited"
        );
        return false;
    }

    // Session ended with an error — check if it's rate-limit related
    detect_rate_limit_in_text(result_line)
}

/// Check text content for rate limit patterns.
fn detect_rate_limit_in_text(text: &str) -> bool {
    for pattern in RATE_LIMIT_PATTERNS.iter() {
        if pattern.is_match(text) {
            tracing::debug!(pattern = %pattern, "rate limit pattern matched in result event");
            return true;
        }
    }
    false
}

/// Calculate exponential backoff delay for rate limiting.
///
/// Returns `initial_delay * 2^consecutive_count`, capped at `max_delay`.
pub fn backoff_delay(initial_delay_secs: u64, consecutive_count: u32, max_delay_secs: u64) -> u64 {
    let shift = 1u64.checked_shl(consecutive_count).unwrap_or(u64::MAX);
    let delay = initial_delay_secs.saturating_mul(shift);
    delay.min(max_delay_secs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    /// Helper: build a result event JSON line.
    fn result_event(is_error: bool, subtype: &str, result_text: &str) -> String {
        serde_json::json!({
            "type": "result",
            "subtype": subtype,
            "is_error": is_error,
            "result": result_text,
            "duration_ms": 1000,
            "session_id": "test-session"
        })
        .to_string()
    }

    /// Helper: build a JSONL file with tool output lines + a final result event.
    fn jsonl_with_result(
        tool_lines: &[&str],
        is_error: bool,
        subtype: &str,
        result_text: &str,
    ) -> String {
        let mut lines: Vec<String> = tool_lines.iter().map(|l| l.to_string()).collect();
        lines.push(result_event(is_error, subtype, result_text));
        lines.join("\n")
    }

    // --- Result event detection tests ---

    #[test]
    fn test_error_result_with_rate_limit_detected() {
        let jsonl = result_event(
            true,
            "error",
            "You have been rate limited. Please try again.",
        );
        assert!(detect_rate_limit_in_result_event(&jsonl));
    }

    #[test]
    fn test_error_result_with_rate_limit_json_error() {
        let line =
            r#"{"type":"result","subtype":"error","is_error":true,"result":"error: rate_limit"}"#;
        assert!(detect_rate_limit_in_result_event(line));
    }

    #[test]
    fn test_error_result_with_usage_limit() {
        let jsonl = result_event(
            true,
            "error",
            "You have exceeded your usage limit for this model.",
        );
        assert!(detect_rate_limit_in_result_event(&jsonl));
    }

    #[test]
    fn test_error_result_with_hit_your_limit() {
        let jsonl = result_event(true, "error", "You've hit your limit. Please wait.");
        assert!(detect_rate_limit_in_result_event(&jsonl));
    }

    #[test]
    fn test_error_result_with_rate_limit_case_insensitive() {
        let jsonl = result_event(true, "error", "RATE LIMIT exceeded");
        assert!(detect_rate_limit_in_result_event(&jsonl));
    }

    #[test]
    fn test_error_result_without_rate_limit_keywords() {
        let jsonl = result_event(true, "error", "Internal server error occurred");
        assert!(!detect_rate_limit_in_result_event(&jsonl));
    }

    // --- Successful session never rate-limited (the core false-positive fix) ---

    #[test]
    fn test_successful_session_never_rate_limited() {
        // Session output mentions rate limiting (e.g., agent read SPEC.md), but
        // the session itself succeeded — should NOT be classified as rate-limited.
        let jsonl = jsonl_with_result(
            &[
                r#"{"type":"assistant","message":"I read the file that says 'rate limit detection'"}"#,
            ],
            false,
            "success",
            "Done. Implemented rate limit detection feature.",
        );
        assert!(!detect_rate_limit_in_result_event(&jsonl));
    }

    #[test]
    fn test_successful_session_with_rate_limit_in_tool_output() {
        // Tool output contains rate limit keywords, but session succeeded
        let jsonl = jsonl_with_result(
            &[
                r#"{"type":"tool_result","content":"usage limit reached, resets at UTC midnight"}"#,
                r#"{"type":"assistant","message":"I found the rate limit code"}"#,
            ],
            false,
            "success",
            "Completed successfully.",
        );
        assert!(!detect_rate_limit_in_result_event(&jsonl));
    }

    #[test]
    fn test_successful_result_with_rate_limit_in_result_text() {
        // Even if the result text mentions rate limiting, a success is never rate-limited
        let jsonl = result_event(
            false,
            "success",
            "Implemented rate_limit feature with usage limit handling",
        );
        assert!(!detect_rate_limit_in_result_event(&jsonl));
    }

    // --- Edge cases ---

    #[test]
    fn test_no_result_event_not_rate_limited() {
        let jsonl = r#"{"type":"assistant","message":"hello"}
{"type":"tool_result","content":"some output"}"#;
        assert!(!detect_rate_limit_in_result_event(jsonl));
    }

    #[test]
    fn test_empty_content_not_rate_limited() {
        assert!(!detect_rate_limit_in_result_event(""));
    }

    #[test]
    fn test_malformed_result_line_not_rate_limited() {
        let jsonl = r#"{"type":"result" invalid json here"#;
        assert!(!detect_rate_limit_in_result_event(jsonl));
    }

    #[test]
    fn test_multiple_result_events_uses_last() {
        // First result is an error with rate limit, second (last) is success
        let line1 = result_event(true, "error", "rate_limit exceeded");
        let line2 = result_event(false, "success", "Session completed.");
        let jsonl = format!("{}\n{}", line1, line2);
        assert!(!detect_rate_limit_in_result_event(&jsonl));
    }

    // --- File-based detection ---

    #[test]
    fn test_detect_rate_limit_from_file_with_error_result() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("output.jsonl");
        let content = result_event(true, "error", "rate_limit: too many requests");
        std::fs::write(&path, content).unwrap();
        assert!(detect_rate_limit(&path));
    }

    #[test]
    fn test_detect_no_rate_limit_from_file_successful_session() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("output.jsonl");
        let content = jsonl_with_result(
            &[r#"{"type":"tool_result","content":"rate limit code here"}"#],
            false,
            "success",
            "Done.",
        );
        std::fs::write(&path, content).unwrap();
        assert!(!detect_rate_limit(&path));
    }

    #[test]
    fn test_detect_rate_limit_missing_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("nonexistent.jsonl");
        assert!(!detect_rate_limit(&path));
    }

    // --- Backoff tests (unchanged) ---

    #[test]
    fn test_backoff_delay_basic() {
        assert_eq!(backoff_delay(2, 0, 600), 2);
        assert_eq!(backoff_delay(2, 1, 600), 4);
        assert_eq!(backoff_delay(2, 2, 600), 8);
        assert_eq!(backoff_delay(2, 3, 600), 16);
    }

    #[test]
    fn test_backoff_delay_capped() {
        assert_eq!(backoff_delay(2, 10, 600), 600);
    }

    #[test]
    fn test_backoff_delay_overflow_safe() {
        assert_eq!(backoff_delay(2, 63, 600), 600);
    }

    #[test]
    fn test_backoff_delay_zero_initial() {
        assert_eq!(backoff_delay(0, 5, 600), 0);
    }
}
