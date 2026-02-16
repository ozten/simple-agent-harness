use super::{AdapterError, AgentAdapter, ExtractionSource};
use serde_json::Value;
use std::io::BufRead;
use std::path::Path;

/// Adapter for Claude's stream-json JSONL format.
pub struct ClaudeAdapter;

impl ClaudeAdapter {
    pub fn new() -> Self {
        ClaudeAdapter
    }
}

impl Default for ClaudeAdapter {
    fn default() -> Self {
        Self::new()
    }
}

/// Extracted metrics from a Claude JSONL session file.
#[derive(Debug, Default)]
struct RawMetrics {
    turns_total: u64,
    turns_narration_only: u64,
    turns_parallel: u64,
    turns_tool_calls: u64,
    cost_input_tokens: u64,
    cost_output_tokens: u64,
    cost_cache_read_tokens: u64,
    cost_cache_creation_tokens: u64,
    cost_estimate_usd: f64,
    session_duration_secs: f64,
    session_output_bytes: u64,
}

/// Collected text from a session, separated by source type.
#[derive(Debug, Default)]
struct CollectedText {
    raw_lines: Vec<String>,
    text_blocks: Vec<String>,
    tool_commands: Vec<String>,
}

/// Parse a Claude JSONL file and extract metrics and text.
fn parse_claude_jsonl(path: &Path) -> Result<(RawMetrics, CollectedText), AdapterError> {
    let file = std::fs::File::open(path)?;
    let file_size = file.metadata()?.len();
    let reader = std::io::BufReader::new(file);

    let mut m = RawMetrics {
        session_output_bytes: file_size,
        ..Default::default()
    };
    let mut text = CollectedText::default();

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

        match v.get("type").and_then(|t| t.as_str()) {
            Some("assistant") => {
                collect_assistant_text(&v, &mut text);
                count_assistant_turn(&v, &mut m);
            }
            Some("result") => extract_result(&v, &mut m),
            _ => {}
        }
    }

    Ok((m, text))
}

fn collect_assistant_text(v: &Value, text: &mut CollectedText) {
    let content = match v
        .get("message")
        .and_then(|msg| msg.get("content"))
        .and_then(|c| c.as_array())
    {
        Some(arr) => arr,
        None => return,
    };

    for item in content {
        match item.get("type").and_then(|t| t.as_str()) {
            Some("text") => {
                if let Some(t) = item.get("text").and_then(|t| t.as_str()) {
                    text.text_blocks.push(t.to_string());
                }
            }
            Some("tool_use") => {
                if let Some(cmd) = item
                    .get("input")
                    .and_then(|i| i.get("command"))
                    .and_then(|c| c.as_str())
                {
                    text.tool_commands.push(cmd.to_string());
                }
            }
            _ => {}
        }
    }
}

fn count_assistant_turn(v: &Value, m: &mut RawMetrics) {
    m.turns_total += 1;

    let content = match v
        .get("message")
        .and_then(|msg| msg.get("content"))
        .and_then(|c| c.as_array())
    {
        Some(arr) => arr,
        None => return,
    };

    let tool_use_count = content
        .iter()
        .filter(|c| c.get("type").and_then(|t| t.as_str()) == Some("tool_use"))
        .count();

    let has_text = content
        .iter()
        .any(|c| c.get("type").and_then(|t| t.as_str()) == Some("text"));

    m.turns_tool_calls += tool_use_count as u64;

    if tool_use_count == 0 && has_text {
        m.turns_narration_only += 1;
    }

    if tool_use_count >= 2 {
        m.turns_parallel += 1;
    }
}

fn extract_result(v: &Value, m: &mut RawMetrics) {
    if let Some(dur) = v.get("duration_ms").and_then(|d| d.as_u64()) {
        m.session_duration_secs = dur as f64 / 1000.0;
    }

    if let Some(cost) = v.get("total_cost_usd").and_then(|c| c.as_f64()) {
        m.cost_estimate_usd = cost;
    }

    if let Some(model_usage) = v.get("modelUsage").and_then(|u| u.as_object()) {
        for (_model, stats) in model_usage {
            m.cost_input_tokens += stats
                .get("inputTokens")
                .and_then(|t| t.as_u64())
                .unwrap_or(0);
            m.cost_output_tokens += stats
                .get("outputTokens")
                .and_then(|t| t.as_u64())
                .unwrap_or(0);
            m.cost_cache_read_tokens += stats
                .get("cacheReadInputTokens")
                .and_then(|t| t.as_u64())
                .unwrap_or(0);
            m.cost_cache_creation_tokens += stats
                .get("cacheCreationInputTokens")
                .and_then(|t| t.as_u64())
                .unwrap_or(0);
        }
    }
}

const SUPPORTED_METRICS: &[&str] = &[
    "turns.total",
    "turns.narration_only",
    "turns.parallel",
    "turns.tool_calls",
    "cost.input_tokens",
    "cost.output_tokens",
    "cost.cache_read_tokens",
    "cost.cache_creation_tokens",
    "cost.estimate_usd",
    "session.output_bytes",
    "session.duration_secs",
];

impl AgentAdapter for ClaudeAdapter {
    fn name(&self) -> &str {
        "claude"
    }

    fn extract_builtin_metrics(
        &self,
        output_path: &Path,
    ) -> Result<Vec<(String, Value)>, AdapterError> {
        let (m, _) = parse_claude_jsonl(output_path)?;

        let metrics = vec![
            ("turns.total".into(), Value::from(m.turns_total)),
            (
                "turns.narration_only".into(),
                Value::from(m.turns_narration_only),
            ),
            ("turns.parallel".into(), Value::from(m.turns_parallel)),
            ("turns.tool_calls".into(), Value::from(m.turns_tool_calls)),
            ("cost.input_tokens".into(), Value::from(m.cost_input_tokens)),
            (
                "cost.output_tokens".into(),
                Value::from(m.cost_output_tokens),
            ),
            (
                "cost.cache_read_tokens".into(),
                Value::from(m.cost_cache_read_tokens),
            ),
            (
                "cost.cache_creation_tokens".into(),
                Value::from(m.cost_cache_creation_tokens),
            ),
            (
                "cost.estimate_usd".into(),
                serde_json::Number::from_f64(m.cost_estimate_usd)
                    .map(Value::Number)
                    .unwrap_or(Value::Null),
            ),
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
        let (_, text) = parse_claude_jsonl(output_path)?;
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
        let path = dir.join("test-session.jsonl");
        let mut f = std::fs::File::create(&path).unwrap();
        for line in lines {
            writeln!(f, "{}", line).unwrap();
        }
        path
    }

    #[test]
    fn adapter_name() {
        let adapter = ClaudeAdapter::new();
        assert_eq!(adapter.name(), "claude");
    }

    #[test]
    fn extract_empty_file() {
        let dir = TempDir::new().unwrap();
        let path = write_jsonl(dir.path(), &[]);
        let adapter = ClaudeAdapter::new();
        let metrics = adapter.extract_builtin_metrics(&path).unwrap();
        let turns = metrics.iter().find(|(k, _)| k == "turns.total").unwrap();
        assert_eq!(turns.1, 0);
    }

    #[test]
    fn extract_single_assistant_text_only() {
        let dir = TempDir::new().unwrap();
        let line = r#"{"type":"assistant","message":{"content":[{"type":"text","text":"hello"}]}}"#;
        let path = write_jsonl(dir.path(), &[line]);
        let adapter = ClaudeAdapter::new();
        let metrics = adapter.extract_builtin_metrics(&path).unwrap();

        let get = |k: &str| metrics.iter().find(|(key, _)| key == k).unwrap().1.clone();
        assert_eq!(get("turns.total"), 1);
        assert_eq!(get("turns.narration_only"), 1);
        assert_eq!(get("turns.parallel"), 0);
        assert_eq!(get("turns.tool_calls"), 0);
    }

    #[test]
    fn extract_parallel_tools() {
        let dir = TempDir::new().unwrap();
        let line = r#"{"type":"assistant","message":{"content":[{"type":"tool_use","name":"Read","input":{}},{"type":"tool_use","name":"Grep","input":{}}]}}"#;
        let path = write_jsonl(dir.path(), &[line]);
        let adapter = ClaudeAdapter::new();
        let metrics = adapter.extract_builtin_metrics(&path).unwrap();

        let get = |k: &str| metrics.iter().find(|(key, _)| key == k).unwrap().1.clone();
        assert_eq!(get("turns.total"), 1);
        assert_eq!(get("turns.tool_calls"), 2);
        assert_eq!(get("turns.parallel"), 1);
    }

    #[test]
    fn extract_result_metrics() {
        let dir = TempDir::new().unwrap();
        let result = r#"{"type":"result","subtype":"success","duration_ms":180000,"num_turns":45,"total_cost_usd":1.234,"modelUsage":{"claude-opus-4-6":{"inputTokens":100,"outputTokens":200,"cacheReadInputTokens":300,"cacheCreationInputTokens":50}}}"#;
        let path = write_jsonl(dir.path(), &[result]);
        let adapter = ClaudeAdapter::new();
        let metrics = adapter.extract_builtin_metrics(&path).unwrap();

        let get = |k: &str| metrics.iter().find(|(key, _)| key == k).unwrap().1.clone();
        assert!((get("session.duration_secs").as_f64().unwrap() - 180.0).abs() < 0.001);
        assert_eq!(get("cost.input_tokens"), 100);
        assert_eq!(get("cost.output_tokens"), 200);
        assert_eq!(get("cost.cache_read_tokens"), 300);
        assert_eq!(get("cost.cache_creation_tokens"), 50);
        assert!((get("cost.estimate_usd").as_f64().unwrap() - 1.234).abs() < 0.001);
    }

    #[test]
    fn extract_multi_model_usage() {
        let dir = TempDir::new().unwrap();
        let result = r#"{"type":"result","total_cost_usd":2.0,"duration_ms":100,"modelUsage":{"claude-opus-4-6":{"inputTokens":100,"outputTokens":200,"cacheReadInputTokens":0,"cacheCreationInputTokens":0},"claude-haiku-4-5-20251001":{"inputTokens":50,"outputTokens":30,"cacheReadInputTokens":0,"cacheCreationInputTokens":0}}}"#;
        let path = write_jsonl(dir.path(), &[result]);
        let adapter = ClaudeAdapter::new();
        let metrics = adapter.extract_builtin_metrics(&path).unwrap();

        let get = |k: &str| metrics.iter().find(|(key, _)| key == k).unwrap().1.clone();
        assert_eq!(get("cost.input_tokens"), 150);
        assert_eq!(get("cost.output_tokens"), 230);
    }

    #[test]
    fn extract_mixed_events() {
        let dir = TempDir::new().unwrap();
        let lines = &[
            r#"{"type":"system","subtype":"init"}"#,
            r#"{"type":"assistant","message":{"content":[{"type":"text","text":"hi"}]}}"#,
            r#"{"type":"user","message":{"role":"user"}}"#,
            r#"{"type":"assistant","message":{"content":[{"type":"tool_use","name":"Bash","input":{}},{"type":"tool_use","name":"Read","input":{}}]}}"#,
            r#"{"type":"assistant","message":{"content":[{"type":"text","text":"done"},{"type":"tool_use","name":"Write","input":{}}]}}"#,
            r#"{"type":"result","duration_ms":5000,"total_cost_usd":0.5,"modelUsage":{"opus":{"inputTokens":1000,"outputTokens":500,"cacheReadInputTokens":0,"cacheCreationInputTokens":0}}}"#,
        ];
        let path = write_jsonl(dir.path(), lines);
        let adapter = ClaudeAdapter::new();
        let metrics = adapter.extract_builtin_metrics(&path).unwrap();

        let get = |k: &str| metrics.iter().find(|(key, _)| key == k).unwrap().1.clone();
        assert_eq!(get("turns.total"), 3);
        assert_eq!(get("turns.narration_only"), 1);
        assert_eq!(get("turns.parallel"), 1);
        assert_eq!(get("turns.tool_calls"), 3);
        assert_eq!(get("cost.input_tokens"), 1000);
        assert_eq!(get("cost.output_tokens"), 500);
    }

    #[test]
    fn extract_skips_malformed_lines() {
        let dir = TempDir::new().unwrap();
        let lines = &[
            "not valid json",
            r#"{"type":"assistant","message":{"content":[{"type":"text","text":"ok"}]}}"#,
            "{broken",
        ];
        let path = write_jsonl(dir.path(), lines);
        let adapter = ClaudeAdapter::new();
        let metrics = adapter.extract_builtin_metrics(&path).unwrap();
        let turns = metrics.iter().find(|(k, _)| k == "turns.total").unwrap();
        assert_eq!(turns.1, 1);
    }

    #[test]
    fn extract_output_bytes_is_file_size() {
        let dir = TempDir::new().unwrap();
        let lines =
            &[r#"{"type":"assistant","message":{"content":[{"type":"text","text":"hello"}]}}"#];
        let path = write_jsonl(dir.path(), lines);
        let expected_size = std::fs::metadata(&path).unwrap().len();
        let adapter = ClaudeAdapter::new();
        let metrics = adapter.extract_builtin_metrics(&path).unwrap();
        let bytes = metrics
            .iter()
            .find(|(k, _)| k == "session.output_bytes")
            .unwrap();
        assert_eq!(bytes.1, expected_size);
    }

    #[test]
    fn file_not_found_returns_error() {
        let adapter = ClaudeAdapter::new();
        let result = adapter.extract_builtin_metrics(Path::new("/nonexistent/file.jsonl"));
        assert!(result.is_err());
    }

    #[test]
    fn supported_metrics_list() {
        let adapter = ClaudeAdapter::new();
        let supported = adapter.supported_metrics();
        assert!(supported.contains(&"turns.total"));
        assert!(supported.contains(&"cost.estimate_usd"));
        assert!(supported.contains(&"session.output_bytes"));
        assert_eq!(supported.len(), 11);
    }

    #[test]
    fn lines_for_source_tool_commands() {
        let dir = TempDir::new().unwrap();
        let lines = &[
            r#"{"type":"assistant","message":{"content":[{"type":"tool_use","name":"Bash","input":{"command":"cargo test --filter foo"}}]}}"#,
            r#"{"type":"assistant","message":{"content":[{"type":"tool_use","name":"Bash","input":{"command":"cargo clippy"}}]}}"#,
        ];
        let path = write_jsonl(dir.path(), lines);
        let adapter = ClaudeAdapter::new();
        let cmds = adapter
            .lines_for_source(&path, ExtractionSource::ToolCommands)
            .unwrap();
        assert_eq!(cmds.len(), 2);
        assert_eq!(cmds[0], "cargo test --filter foo");
        assert_eq!(cmds[1], "cargo clippy");
    }

    #[test]
    fn lines_for_source_text() {
        let dir = TempDir::new().unwrap();
        let lines = &[
            r#"{"type":"assistant","message":{"content":[{"type":"text","text":"Hello world"},{"type":"tool_use","name":"Read","input":{}}]}}"#,
            r#"{"type":"assistant","message":{"content":[{"type":"text","text":"Done."}]}}"#,
        ];
        let path = write_jsonl(dir.path(), lines);
        let adapter = ClaudeAdapter::new();
        let text = adapter
            .lines_for_source(&path, ExtractionSource::Text)
            .unwrap();
        assert_eq!(text.len(), 2);
        assert_eq!(text[0], "Hello world");
        assert_eq!(text[1], "Done.");
    }

    #[test]
    fn lines_for_source_raw() {
        let dir = TempDir::new().unwrap();
        let lines = &[
            r#"{"type":"assistant","message":{"content":[{"type":"text","text":"hi"}]}}"#,
            r#"{"type":"result","duration_ms":1000,"total_cost_usd":0.0,"modelUsage":{}}"#,
        ];
        let path = write_jsonl(dir.path(), lines);
        let adapter = ClaudeAdapter::new();
        let raw = adapter
            .lines_for_source(&path, ExtractionSource::Raw)
            .unwrap();
        assert_eq!(raw.len(), 2);
    }

    #[test]
    fn real_file_format() {
        let dir = TempDir::new().unwrap();
        let lines = &[
            r#"{"type":"system","subtype":"hook_started","hook_id":"abc"}"#,
            r#"{"type":"system","subtype":"init","cwd":"/tmp","session_id":"test"}"#,
            r#"{"type":"assistant","message":{"model":"claude-opus-4-6","content":[{"type":"text","text":"Starting work."}]}}"#,
            r#"{"type":"user","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"x"}]}}"#,
            r#"{"type":"assistant","message":{"content":[{"type":"text","text":"Reading files"},{"type":"tool_use","name":"Read","id":"t1","input":{"file_path":"/tmp/a"}},{"type":"tool_use","name":"Read","id":"t2","input":{"file_path":"/tmp/b"}}]}}"#,
            r#"{"type":"assistant","message":{"content":[{"type":"tool_use","name":"Edit","id":"t3","input":{}}]}}"#,
            r#"{"type":"assistant","message":{"content":[{"type":"text","text":"Done."}]}}"#,
            r#"{"type":"result","subtype":"success","duration_ms":229857,"num_turns":49,"total_cost_usd":0.99,"modelUsage":{"claude-opus-4-6":{"inputTokens":24,"outputTokens":9407,"cacheReadInputTokens":939227,"cacheCreationInputTokens":37239},"claude-haiku-4-5-20251001":{"inputTokens":47934,"outputTokens":947,"cacheReadInputTokens":0,"cacheCreationInputTokens":0}}}"#,
        ];
        let path = write_jsonl(dir.path(), lines);
        let adapter = ClaudeAdapter::new();
        let metrics = adapter.extract_builtin_metrics(&path).unwrap();

        let get = |k: &str| metrics.iter().find(|(key, _)| key == k).unwrap().1.clone();
        assert_eq!(get("turns.total"), 4);
        assert_eq!(get("turns.narration_only"), 2);
        assert_eq!(get("turns.parallel"), 1);
        assert_eq!(get("turns.tool_calls"), 3);
        assert_eq!(get("cost.input_tokens"), 24 + 47934);
        assert_eq!(get("cost.output_tokens"), 9407 + 947);
        assert!((get("cost.estimate_usd").as_f64().unwrap() - 0.99).abs() < 0.001);
        assert!((get("session.duration_secs").as_f64().unwrap() - 229.857).abs() < 0.001);
    }
}
