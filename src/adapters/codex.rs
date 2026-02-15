use super::{AdapterError, AgentAdapter, ExtractionSource};
use serde_json::Value;
use std::io::BufRead;
use std::path::Path;

/// Adapter for OpenAI Codex CLI `--json` JSONL output.
///
/// Codex emits JSONL events: `turn.started`, `turn.completed`,
/// `item.started`, `item.completed`, etc.
///
/// Supported metrics: turns.total, turns.tool_calls,
/// session.output_bytes, session.exit_code, session.duration_secs.
///
/// Not available (gracefully skipped): turns.narration_only,
/// turns.parallel, cost.*.
pub struct CodexAdapter;

impl CodexAdapter {
    pub fn new() -> Self {
        CodexAdapter
    }
}

impl Default for CodexAdapter {
    fn default() -> Self {
        Self::new()
    }
}

/// Extracted metrics from a Codex JSONL session file.
#[derive(Debug, Default)]
struct RawMetrics {
    turns_total: u64,
    turns_tool_calls: u64,
    session_output_bytes: u64,
    session_exit_code: Option<i64>,
    session_duration_secs: f64,
}

/// Collected text from a session, separated by source type.
#[derive(Debug, Default)]
struct CollectedText {
    raw_lines: Vec<String>,
    text_blocks: Vec<String>,
    tool_commands: Vec<String>,
}

/// Parse a Codex JSONL file and extract metrics and text.
fn parse_codex_jsonl(path: &Path) -> Result<(RawMetrics, CollectedText), AdapterError> {
    let file = std::fs::File::open(path)?;
    let file_size = file.metadata()?.len();
    let reader = std::io::BufReader::new(file);

    let mut m = RawMetrics {
        session_output_bytes: file_size,
        ..Default::default()
    };
    let mut text = CollectedText::default();

    // Track timestamps for duration calculation
    let mut first_timestamp: Option<f64> = None;
    let mut last_timestamp: Option<f64> = None;

    for line in reader.lines() {
        let line = line?;
        if line.is_empty() {
            continue;
        }
        text.raw_lines.push(line.clone());

        let v: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => continue,
        };

        // Track timestamps from any event for duration calculation
        if let Some(ts) = v.get("timestamp").and_then(|t| t.as_f64()) {
            if first_timestamp.is_none() {
                first_timestamp = Some(ts);
            }
            last_timestamp = Some(ts);
        }

        match v.get("type").and_then(|t| t.as_str()) {
            Some("turn.completed") => {
                m.turns_total += 1;
            }
            Some("item.completed") | Some("item.started") => {
                if let Some(item) = v.get("item") {
                    collect_item(item, &mut text, &mut m);
                }
            }
            _ => {}
        }
    }

    // Calculate duration from first to last timestamp
    if let (Some(first), Some(last)) = (first_timestamp, last_timestamp) {
        m.session_duration_secs = (last - first).max(0.0);
    }

    Ok((m, text))
}

fn collect_item(item: &Value, text: &mut CollectedText, m: &mut RawMetrics) {
    let item_type = item.get("type").and_then(|t| t.as_str()).unwrap_or("");

    match item_type {
        "command_execution" => {
            if let Some(cmd) = item.get("command").and_then(|c| c.as_str()) {
                // Strip "bash -lc " prefix if present — Codex wraps commands this way
                let clean_cmd = cmd.strip_prefix("bash -lc ").unwrap_or(cmd);
                text.tool_commands.push(clean_cmd.to_string());
                m.turns_tool_calls += 1;
            }
            // Check for exit code
            if let Some(code) = item.get("exit_code").and_then(|c| c.as_i64()) {
                m.session_exit_code = Some(code);
            }
        }
        "agent_message" => {
            if let Some(t) = item.get("text").and_then(|t| t.as_str()) {
                text.text_blocks.push(t.to_string());
            }
        }
        "file_change" | "mcp_tool_call" | "web_search" => {
            // These are also tool-like invocations
            m.turns_tool_calls += 1;
        }
        _ => {}
    }
}

const SUPPORTED_METRICS: &[&str] = &[
    "turns.total",
    "turns.tool_calls",
    "session.output_bytes",
    "session.exit_code",
    "session.duration_secs",
];

impl AgentAdapter for CodexAdapter {
    fn name(&self) -> &str {
        "codex"
    }

    fn extract_builtin_metrics(
        &self,
        output_path: &Path,
    ) -> Result<Vec<(String, Value)>, AdapterError> {
        let (m, _) = parse_codex_jsonl(output_path)?;

        let mut metrics = vec![
            ("turns.total".into(), Value::from(m.turns_total)),
            ("turns.tool_calls".into(), Value::from(m.turns_tool_calls)),
            (
                "session.output_bytes".into(),
                Value::from(m.session_output_bytes),
            ),
            (
                "session.duration_secs".into(),
                serde_json::Number::from_f64(m.session_duration_secs)
                    .map(Value::Number)
                    .unwrap_or(Value::Null),
            ),
        ];

        if let Some(code) = m.session_exit_code {
            metrics.push(("session.exit_code".into(), Value::from(code)));
        }

        Ok(metrics)
    }

    fn supported_metrics(&self) -> &[&str] {
        SUPPORTED_METRICS
    }

    fn lines_for_source(
        &self,
        output_path: &Path,
        source: ExtractionSource,
    ) -> Result<Vec<String>, AdapterError> {
        let (_, text) = parse_codex_jsonl(output_path)?;
        Ok(match source {
            ExtractionSource::ToolCommands => text.tool_commands,
            ExtractionSource::Text => text.text_blocks,
            ExtractionSource::Raw => text.raw_lines,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    fn write_jsonl(dir: &Path, lines: &[&str]) -> std::path::PathBuf {
        let path = dir.join("codex-session.jsonl");
        let mut f = std::fs::File::create(&path).unwrap();
        for line in lines {
            writeln!(f, "{}", line).unwrap();
        }
        path
    }

    #[test]
    fn adapter_name() {
        let adapter = CodexAdapter::new();
        assert_eq!(adapter.name(), "codex");
    }

    #[test]
    fn extract_empty_file() {
        let dir = TempDir::new().unwrap();
        let path = write_jsonl(dir.path(), &[]);
        let adapter = CodexAdapter::new();
        let metrics = adapter.extract_builtin_metrics(&path).unwrap();
        let turns = metrics.iter().find(|(k, _)| k == "turns.total").unwrap();
        assert_eq!(turns.1, 0);
    }

    #[test]
    fn extract_turns_from_turn_completed() {
        let dir = TempDir::new().unwrap();
        let lines = &[
            r#"{"type":"turn.started","timestamp":1000.0}"#,
            r#"{"type":"turn.completed","timestamp":1001.0,"usage":{"input_tokens":100,"output_tokens":50}}"#,
            r#"{"type":"turn.started","timestamp":1002.0}"#,
            r#"{"type":"turn.completed","timestamp":1003.0,"usage":{"input_tokens":200,"output_tokens":80}}"#,
        ];
        let path = write_jsonl(dir.path(), lines);
        let adapter = CodexAdapter::new();
        let metrics = adapter.extract_builtin_metrics(&path).unwrap();
        let get = |k: &str| metrics.iter().find(|(key, _)| key == k).unwrap().1.clone();
        assert_eq!(get("turns.total"), 2);
    }

    #[test]
    fn extract_tool_calls_from_command_execution() {
        let dir = TempDir::new().unwrap();
        let lines = &[
            r#"{"type":"item.completed","timestamp":1000.0,"item":{"id":"i1","type":"command_execution","command":"bash -lc ls","status":"completed","exit_code":0}}"#,
            r#"{"type":"item.completed","timestamp":1001.0,"item":{"id":"i2","type":"command_execution","command":"bash -lc cargo test","status":"completed","exit_code":0}}"#,
        ];
        let path = write_jsonl(dir.path(), lines);
        let adapter = CodexAdapter::new();
        let metrics = adapter.extract_builtin_metrics(&path).unwrap();
        let get = |k: &str| metrics.iter().find(|(key, _)| key == k).unwrap().1.clone();
        assert_eq!(get("turns.tool_calls"), 2);
    }

    #[test]
    fn extract_exit_code() {
        let dir = TempDir::new().unwrap();
        let lines = &[
            r#"{"type":"item.completed","timestamp":1000.0,"item":{"id":"i1","type":"command_execution","command":"bash -lc exit 1","status":"completed","exit_code":1}}"#,
        ];
        let path = write_jsonl(dir.path(), lines);
        let adapter = CodexAdapter::new();
        let metrics = adapter.extract_builtin_metrics(&path).unwrap();
        let exit_code = metrics
            .iter()
            .find(|(k, _)| k == "session.exit_code")
            .unwrap();
        assert_eq!(exit_code.1, 1);
    }

    #[test]
    fn extract_duration_from_timestamps() {
        let dir = TempDir::new().unwrap();
        let lines = &[
            r#"{"type":"thread.started","timestamp":1000.0}"#,
            r#"{"type":"turn.started","timestamp":1001.0}"#,
            r#"{"type":"turn.completed","timestamp":1060.5}"#,
        ];
        let path = write_jsonl(dir.path(), lines);
        let adapter = CodexAdapter::new();
        let metrics = adapter.extract_builtin_metrics(&path).unwrap();
        let duration = metrics
            .iter()
            .find(|(k, _)| k == "session.duration_secs")
            .unwrap();
        let secs = duration.1.as_f64().unwrap();
        assert!((secs - 60.5).abs() < 0.01);
    }

    #[test]
    fn extract_output_bytes_is_file_size() {
        let dir = TempDir::new().unwrap();
        let lines = &[r#"{"type":"turn.completed","timestamp":1000.0}"#];
        let path = write_jsonl(dir.path(), lines);
        let expected_size = std::fs::metadata(&path).unwrap().len();
        let adapter = CodexAdapter::new();
        let metrics = adapter.extract_builtin_metrics(&path).unwrap();
        let bytes = metrics
            .iter()
            .find(|(k, _)| k == "session.output_bytes")
            .unwrap();
        assert_eq!(bytes.1, expected_size);
    }

    #[test]
    fn extract_skips_malformed_lines() {
        let dir = TempDir::new().unwrap();
        let lines = &[
            "not valid json",
            r#"{"type":"turn.completed","timestamp":1000.0}"#,
            "{broken",
        ];
        let path = write_jsonl(dir.path(), lines);
        let adapter = CodexAdapter::new();
        let metrics = adapter.extract_builtin_metrics(&path).unwrap();
        let turns = metrics.iter().find(|(k, _)| k == "turns.total").unwrap();
        assert_eq!(turns.1, 1);
    }

    #[test]
    fn file_not_found_returns_error() {
        let adapter = CodexAdapter::new();
        let result = adapter.extract_builtin_metrics(Path::new("/nonexistent/file.jsonl"));
        assert!(result.is_err());
    }

    #[test]
    fn supported_metrics_list() {
        let adapter = CodexAdapter::new();
        let supported = adapter.supported_metrics();
        assert!(supported.contains(&"turns.total"));
        assert!(supported.contains(&"turns.tool_calls"));
        assert!(supported.contains(&"session.output_bytes"));
        assert!(supported.contains(&"session.exit_code"));
        assert!(supported.contains(&"session.duration_secs"));
        assert_eq!(supported.len(), 5);
        // Should NOT contain cost or narration metrics
        assert!(!supported.contains(&"cost.input_tokens"));
        assert!(!supported.contains(&"turns.narration_only"));
        assert!(!supported.contains(&"turns.parallel"));
    }

    #[test]
    fn lines_for_source_tool_commands() {
        let dir = TempDir::new().unwrap();
        let lines = &[
            r#"{"type":"item.completed","item":{"id":"i1","type":"command_execution","command":"bash -lc cargo test --filter foo"}}"#,
            r#"{"type":"item.completed","item":{"id":"i2","type":"command_execution","command":"bash -lc cargo clippy"}}"#,
        ];
        let path = write_jsonl(dir.path(), lines);
        let adapter = CodexAdapter::new();
        let cmds = adapter
            .lines_for_source(&path, ExtractionSource::ToolCommands)
            .unwrap();
        assert_eq!(cmds.len(), 2);
        assert_eq!(cmds[0], "cargo test --filter foo");
        assert_eq!(cmds[1], "cargo clippy");
    }

    #[test]
    fn lines_for_source_tool_commands_without_bash_prefix() {
        let dir = TempDir::new().unwrap();
        let lines = &[
            r#"{"type":"item.completed","item":{"id":"i1","type":"command_execution","command":"ls -la"}}"#,
        ];
        let path = write_jsonl(dir.path(), lines);
        let adapter = CodexAdapter::new();
        let cmds = adapter
            .lines_for_source(&path, ExtractionSource::ToolCommands)
            .unwrap();
        assert_eq!(cmds.len(), 1);
        assert_eq!(cmds[0], "ls -la");
    }

    #[test]
    fn lines_for_source_text() {
        let dir = TempDir::new().unwrap();
        let lines = &[
            r#"{"type":"item.completed","item":{"id":"i1","type":"agent_message","text":"Hello, I'll help you."}}"#,
            r#"{"type":"item.completed","item":{"id":"i2","type":"agent_message","text":"Done."}}"#,
        ];
        let path = write_jsonl(dir.path(), lines);
        let adapter = CodexAdapter::new();
        let text = adapter
            .lines_for_source(&path, ExtractionSource::Text)
            .unwrap();
        assert_eq!(text.len(), 2);
        assert_eq!(text[0], "Hello, I'll help you.");
        assert_eq!(text[1], "Done.");
    }

    #[test]
    fn lines_for_source_raw() {
        let dir = TempDir::new().unwrap();
        let lines = &[
            r#"{"type":"thread.started"}"#,
            r#"{"type":"turn.completed"}"#,
        ];
        let path = write_jsonl(dir.path(), lines);
        let adapter = CodexAdapter::new();
        let raw = adapter
            .lines_for_source(&path, ExtractionSource::Raw)
            .unwrap();
        assert_eq!(raw.len(), 2);
    }

    #[test]
    fn file_change_and_mcp_count_as_tool_calls() {
        let dir = TempDir::new().unwrap();
        let lines = &[
            r#"{"type":"item.completed","item":{"id":"i1","type":"file_change"}}"#,
            r#"{"type":"item.completed","item":{"id":"i2","type":"mcp_tool_call"}}"#,
            r#"{"type":"item.completed","item":{"id":"i3","type":"web_search"}}"#,
        ];
        let path = write_jsonl(dir.path(), lines);
        let adapter = CodexAdapter::new();
        let metrics = adapter.extract_builtin_metrics(&path).unwrap();
        let get = |k: &str| metrics.iter().find(|(key, _)| key == k).unwrap().1.clone();
        assert_eq!(get("turns.tool_calls"), 3);
    }

    #[test]
    fn mixed_codex_session() {
        let dir = TempDir::new().unwrap();
        let lines = &[
            r#"{"type":"thread.started","timestamp":100.0}"#,
            r#"{"type":"turn.started","timestamp":100.5}"#,
            r#"{"type":"item.completed","timestamp":101.0,"item":{"id":"i1","type":"agent_message","text":"Let me check the code."}}"#,
            r#"{"type":"item.completed","timestamp":102.0,"item":{"id":"i2","type":"command_execution","command":"bash -lc cat src/main.rs","status":"completed","exit_code":0}}"#,
            r#"{"type":"turn.completed","timestamp":103.0,"usage":{"input_tokens":500,"output_tokens":100}}"#,
            r#"{"type":"turn.started","timestamp":104.0}"#,
            r#"{"type":"item.completed","timestamp":105.0,"item":{"id":"i3","type":"agent_message","text":"I'll fix the bug now."}}"#,
            r#"{"type":"item.completed","timestamp":106.0,"item":{"id":"i4","type":"command_execution","command":"bash -lc cargo test","status":"completed","exit_code":0}}"#,
            r#"{"type":"item.completed","timestamp":107.0,"item":{"id":"i5","type":"file_change"}}"#,
            r#"{"type":"turn.completed","timestamp":160.0,"usage":{"input_tokens":800,"output_tokens":200}}"#,
        ];
        let path = write_jsonl(dir.path(), lines);
        let adapter = CodexAdapter::new();
        let metrics = adapter.extract_builtin_metrics(&path).unwrap();

        let get = |k: &str| metrics.iter().find(|(key, _)| key == k).unwrap().1.clone();
        assert_eq!(get("turns.total"), 2);
        assert_eq!(get("turns.tool_calls"), 3); // 2 commands + 1 file_change
        let duration = get("session.duration_secs").as_f64().unwrap();
        assert!((duration - 60.0).abs() < 0.01);

        // Check text extraction
        let text = adapter
            .lines_for_source(&path, ExtractionSource::Text)
            .unwrap();
        assert_eq!(text.len(), 2);
        assert_eq!(text[0], "Let me check the code.");

        // Check tool commands
        let cmds = adapter
            .lines_for_source(&path, ExtractionSource::ToolCommands)
            .unwrap();
        assert_eq!(cmds.len(), 2);
        assert_eq!(cmds[0], "cat src/main.rs");
        assert_eq!(cmds[1], "cargo test");
    }

    #[test]
    fn no_exit_code_when_no_commands() {
        let dir = TempDir::new().unwrap();
        let lines = &[r#"{"type":"turn.completed","timestamp":1000.0}"#];
        let path = write_jsonl(dir.path(), lines);
        let adapter = CodexAdapter::new();
        let metrics = adapter.extract_builtin_metrics(&path).unwrap();
        // exit_code should not be present when no commands executed
        assert!(metrics
            .iter()
            .find(|(k, _)| k == "session.exit_code")
            .is_none());
    }
}
