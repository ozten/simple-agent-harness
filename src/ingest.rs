/// JSONL metric extraction: parse a Claude session output file and write
/// extracted events + observation to the database.
use crate::config::CompiledRule;
use crate::db;
use rusqlite::Connection;
use serde_json::Value;
use std::io::BufRead;
use std::path::Path;

/// Extracted metrics from a JSONL session file.
#[derive(Debug, Default)]
pub struct SessionMetrics {
    pub turns_total: u64,
    pub turns_narration_only: u64,
    pub turns_parallel: u64,
    pub turns_tool_calls: u64,
    pub cost_input_tokens: u64,
    pub cost_output_tokens: u64,
    pub cost_cache_read_tokens: u64,
    pub cost_cache_creation_tokens: u64,
    pub cost_estimate_usd: f64,
    pub session_duration_ms: u64,
    pub session_output_bytes: u64,
    pub session_exit_code: Option<i32>,
    pub session_num_turns: Option<u64>,
}

/// Collected text from a session for rule matching.
#[derive(Debug, Default)]
pub struct SessionText {
    /// Raw JSONL lines
    pub raw_lines: Vec<String>,
    /// Assistant text blocks
    pub text_blocks: Vec<String>,
    /// Tool command strings (tool_use input.command fields)
    pub tool_commands: Vec<String>,
}

/// Parse a JSONL file and extract built-in metrics.
pub fn extract_metrics(path: &Path) -> std::io::Result<SessionMetrics> {
    let (m, _) = extract_metrics_and_text(path)?;
    Ok(m)
}

/// Parse a JSONL file and extract both built-in metrics and session text
/// for configurable rule matching.
fn extract_metrics_and_text(path: &Path) -> std::io::Result<(SessionMetrics, SessionText)> {
    let file = std::fs::File::open(path)?;
    let file_size = file.metadata()?.len();
    let reader = std::io::BufReader::new(file);

    let mut m = SessionMetrics {
        session_output_bytes: file_size,
        ..Default::default()
    };
    let mut text = SessionText::default();

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

/// Collect text blocks and tool commands from an assistant message.
fn collect_assistant_text(v: &Value, text: &mut SessionText) {
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
                // Extract command from input.command if present
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

fn count_assistant_turn(v: &Value, m: &mut SessionMetrics) {
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

fn extract_result(v: &Value, m: &mut SessionMetrics) {
    if let Some(dur) = v.get("duration_ms").and_then(|d| d.as_u64()) {
        m.session_duration_ms = dur;
    }

    if let Some(turns) = v.get("num_turns").and_then(|t| t.as_u64()) {
        m.session_num_turns = Some(turns);
    }

    if let Some(cost) = v.get("total_cost_usd").and_then(|c| c.as_f64()) {
        m.cost_estimate_usd = cost;
    }

    // Aggregate token usage from modelUsage (covers all models)
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

    // Exit code: not in result directly, but stop_reason may indicate it
    // The harness tracks exit_code via SessionResult, not the JSONL.
    // We leave session_exit_code as None here; caller can set it.
}

/// Ingest a JSONL file for the given session: extract metrics, write events
/// and observation to the database. Returns the extracted metrics.
pub fn ingest_session(
    conn: &Connection,
    session: i64,
    jsonl_path: &Path,
    exit_code: Option<i32>,
) -> Result<SessionMetrics, IngestError> {
    ingest_session_with_rules(conn, session, jsonl_path, exit_code, &[])
}

/// Ingest a JSONL file with configurable extraction rules applied.
pub fn ingest_session_with_rules(
    conn: &Connection,
    session: i64,
    jsonl_path: &Path,
    exit_code: Option<i32>,
    rules: &[CompiledRule],
) -> Result<SessionMetrics, IngestError> {
    let (mut metrics, text) = extract_metrics_and_text(jsonl_path).map_err(IngestError::Io)?;
    metrics.session_exit_code = exit_code;

    let ts = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();

    // Write individual events for built-in metrics
    write_events(conn, session, &ts, &metrics).map_err(IngestError::Db)?;

    // Apply configurable extraction rules and write their events
    let rule_results = apply_rules(rules, &text);
    for (kind, value) in &rule_results {
        db::insert_event_with_ts(conn, &ts, session, kind, Some(value), None)
            .map_err(IngestError::Db)?;
    }

    // Build observation data JSON (includes both built-in and rule-extracted)
    let data = build_observation_data_with_rules(&metrics, &rule_results);
    let duration_secs = (metrics.session_duration_ms / 1000) as i64;

    db::upsert_observation(conn, session, &ts, Some(duration_secs), None, &data)
        .map_err(IngestError::Db)?;

    Ok(metrics)
}

fn write_events(
    conn: &Connection,
    session: i64,
    ts: &str,
    m: &SessionMetrics,
) -> rusqlite::Result<()> {
    db::insert_event_with_ts(
        conn,
        ts,
        session,
        "turns.total",
        Some(&m.turns_total.to_string()),
        None,
    )?;
    db::insert_event_with_ts(
        conn,
        ts,
        session,
        "turns.narration_only",
        Some(&m.turns_narration_only.to_string()),
        None,
    )?;
    db::insert_event_with_ts(
        conn,
        ts,
        session,
        "turns.parallel",
        Some(&m.turns_parallel.to_string()),
        None,
    )?;
    db::insert_event_with_ts(
        conn,
        ts,
        session,
        "turns.tool_calls",
        Some(&m.turns_tool_calls.to_string()),
        None,
    )?;
    db::insert_event_with_ts(
        conn,
        ts,
        session,
        "cost.input_tokens",
        Some(&m.cost_input_tokens.to_string()),
        None,
    )?;
    db::insert_event_with_ts(
        conn,
        ts,
        session,
        "cost.output_tokens",
        Some(&m.cost_output_tokens.to_string()),
        None,
    )?;
    db::insert_event_with_ts(
        conn,
        ts,
        session,
        "cost.cache_read_tokens",
        Some(&m.cost_cache_read_tokens.to_string()),
        None,
    )?;
    db::insert_event_with_ts(
        conn,
        ts,
        session,
        "cost.cache_creation_tokens",
        Some(&m.cost_cache_creation_tokens.to_string()),
        None,
    )?;
    db::insert_event_with_ts(
        conn,
        ts,
        session,
        "cost.estimate_usd",
        Some(&format!("{:.6}", m.cost_estimate_usd)),
        None,
    )?;
    db::insert_event_with_ts(
        conn,
        ts,
        session,
        "session.output_bytes",
        Some(&m.session_output_bytes.to_string()),
        None,
    )?;

    if let Some(code) = m.session_exit_code {
        db::insert_event_with_ts(
            conn,
            ts,
            session,
            "session.exit_code",
            Some(&code.to_string()),
            None,
        )?;
    }

    if let Some(dur) = m.session_duration_ms.checked_div(1) {
        db::insert_event_with_ts(
            conn,
            ts,
            session,
            "session.duration_ms",
            Some(&dur.to_string()),
            None,
        )?;
    }

    Ok(())
}

/// Apply configurable extraction rules against collected session text.
/// Returns a Vec of (kind, value) pairs for each rule that produced a match.
fn apply_rules(rules: &[CompiledRule], text: &SessionText) -> Vec<(String, String)> {
    let mut results = Vec::new();

    for rule in rules {
        let lines: &[String] = match rule.source.as_str() {
            "text" => &text.text_blocks,
            "raw" => &text.raw_lines,
            _ => &text.tool_commands, // "tool_commands" is the default
        };

        if let Some(ref emit_val) = rule.emit {
            // Emit mode: check if pattern matches anywhere, emit fixed value
            let found = lines.iter().any(|line| rule.pattern.is_match(line));
            if found {
                let val = toml_value_to_string(emit_val);
                results.push((rule.kind.clone(), val));
            }
        } else if rule.count {
            // Count mode: count matching lines (minus anti_pattern exclusions)
            let count = lines
                .iter()
                .filter(|line| {
                    if !rule.pattern.is_match(line) {
                        return false;
                    }
                    if let Some(ref anti) = rule.anti_pattern {
                        return !anti.is_match(line);
                    }
                    true
                })
                .count();
            results.push((rule.kind.clone(), count.to_string()));
        } else if rule.first_match {
            // First-match mode: find first capture group match and emit it
            for line in lines {
                if let Some(ref anti) = rule.anti_pattern {
                    if anti.is_match(line) {
                        continue;
                    }
                }
                if let Some(caps) = rule.pattern.captures(line) {
                    let matched = caps
                        .get(1)
                        .map(|m| m.as_str())
                        .unwrap_or_else(|| caps.get(0).unwrap().as_str());
                    let value = apply_transform(matched, rule.transform.as_deref());
                    results.push((rule.kind.clone(), value));
                    break;
                }
            }
        } else {
            // Default: collect all matches
            let mut matches = Vec::new();
            for line in lines {
                if let Some(ref anti) = rule.anti_pattern {
                    if anti.is_match(line) {
                        continue;
                    }
                }
                if let Some(caps) = rule.pattern.captures(line) {
                    let matched = caps
                        .get(1)
                        .map(|m| m.as_str())
                        .unwrap_or_else(|| caps.get(0).unwrap().as_str());
                    let value = apply_transform(matched, rule.transform.as_deref());
                    matches.push(value);
                }
            }
            if !matches.is_empty() {
                // Emit as JSON array if multiple, plain value if single
                if matches.len() == 1 {
                    results.push((rule.kind.clone(), matches.into_iter().next().unwrap()));
                } else {
                    let arr: Vec<Value> = matches.into_iter().map(Value::String).collect();
                    results.push((rule.kind.clone(), Value::Array(arr).to_string()));
                }
            }
        }
    }

    results
}

/// Apply a transform to a matched string.
fn apply_transform(input: &str, transform: Option<&str>) -> String {
    match transform {
        Some("last_segment") => input.rsplit('-').next().unwrap_or(input).to_string(),
        Some("int") => {
            // Extract first integer from the string
            input
                .chars()
                .filter(|c| c.is_ascii_digit())
                .collect::<String>()
        }
        Some("trim") => input.trim().to_string(),
        _ => input.to_string(),
    }
}

/// Convert a TOML value to a string for event storage.
fn toml_value_to_string(v: &toml::Value) -> String {
    match v {
        toml::Value::String(s) => s.clone(),
        toml::Value::Boolean(b) => b.to_string(),
        toml::Value::Integer(i) => i.to_string(),
        toml::Value::Float(f) => f.to_string(),
        _ => v.to_string(),
    }
}

/// Build observation data JSON including rule-extracted metrics.
fn build_observation_data_with_rules(
    m: &SessionMetrics,
    rule_results: &[(String, String)],
) -> String {
    let mut map = serde_json::Map::new();
    // Insert built-in metrics
    map.insert(
        "turns.total".to_string(),
        Value::Number(m.turns_total.into()),
    );
    map.insert(
        "turns.narration_only".to_string(),
        Value::Number(m.turns_narration_only.into()),
    );
    map.insert(
        "turns.parallel".to_string(),
        Value::Number(m.turns_parallel.into()),
    );
    map.insert(
        "turns.tool_calls".to_string(),
        Value::Number(m.turns_tool_calls.into()),
    );
    map.insert(
        "cost.input_tokens".to_string(),
        Value::Number(m.cost_input_tokens.into()),
    );
    map.insert(
        "cost.output_tokens".to_string(),
        Value::Number(m.cost_output_tokens.into()),
    );
    map.insert(
        "cost.cache_read_tokens".to_string(),
        Value::Number(m.cost_cache_read_tokens.into()),
    );
    map.insert(
        "cost.cache_creation_tokens".to_string(),
        Value::Number(m.cost_cache_creation_tokens.into()),
    );
    map.insert(
        "cost.estimate_usd".to_string(),
        serde_json::Number::from_f64(m.cost_estimate_usd)
            .map(Value::Number)
            .unwrap_or(Value::Null),
    );
    map.insert(
        "session.output_bytes".to_string(),
        Value::Number(m.session_output_bytes.into()),
    );
    map.insert(
        "session.duration_ms".to_string(),
        Value::Number(m.session_duration_ms.into()),
    );
    if let Some(code) = m.session_exit_code {
        map.insert("session.exit_code".to_string(), Value::Number(code.into()));
    }

    // Insert rule-extracted metrics
    for (kind, value) in rule_results {
        // Try to parse as number first for cleaner JSON
        if let Ok(n) = value.parse::<i64>() {
            map.insert(kind.clone(), Value::Number(n.into()));
        } else if let Ok(n) = value.parse::<f64>() {
            map.insert(
                kind.clone(),
                serde_json::Number::from_f64(n)
                    .map(Value::Number)
                    .unwrap_or(Value::String(value.clone())),
            );
        } else if value == "true" {
            map.insert(kind.clone(), Value::Bool(true));
        } else if value == "false" {
            map.insert(kind.clone(), Value::Bool(false));
        } else {
            // Try parsing as JSON (for arrays), fall back to string
            match serde_json::from_str::<Value>(value) {
                Ok(v) if v.is_array() => {
                    map.insert(kind.clone(), v);
                }
                _ => {
                    map.insert(kind.clone(), Value::String(value.clone()));
                }
            }
        }
    }

    Value::Object(map).to_string()
}

#[derive(Debug)]
pub enum IngestError {
    Io(std::io::Error),
    Db(rusqlite::Error),
}

impl std::fmt::Display for IngestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IngestError::Io(e) => write!(f, "I/O error during ingestion: {e}"),
            IngestError::Db(e) => write!(f, "database error during ingestion: {e}"),
        }
    }
}

impl std::error::Error for IngestError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            IngestError::Io(e) => Some(e),
            IngestError::Db(e) => Some(e),
        }
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

    fn test_db() -> (TempDir, Connection) {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("blacksmith.db");
        let conn = db::open_or_create(&db_path).unwrap();
        (dir, conn)
    }

    #[test]
    fn extract_empty_file() {
        let dir = TempDir::new().unwrap();
        let path = write_jsonl(dir.path(), &[]);
        let m = extract_metrics(&path).unwrap();
        assert_eq!(m.turns_total, 0);
        assert_eq!(m.cost_estimate_usd, 0.0);
    }

    #[test]
    fn extract_single_assistant_text_only() {
        let dir = TempDir::new().unwrap();
        let line = r#"{"type":"assistant","message":{"content":[{"type":"text","text":"hello"}]}}"#;
        let path = write_jsonl(dir.path(), &[line]);
        let m = extract_metrics(&path).unwrap();
        assert_eq!(m.turns_total, 1);
        assert_eq!(m.turns_narration_only, 1);
        assert_eq!(m.turns_parallel, 0);
        assert_eq!(m.turns_tool_calls, 0);
    }

    #[test]
    fn extract_assistant_with_one_tool() {
        let dir = TempDir::new().unwrap();
        let line = r#"{"type":"assistant","message":{"content":[{"type":"text","text":"let me check"},{"type":"tool_use","name":"Read","input":{}}]}}"#;
        let path = write_jsonl(dir.path(), &[line]);
        let m = extract_metrics(&path).unwrap();
        assert_eq!(m.turns_total, 1);
        assert_eq!(m.turns_narration_only, 0);
        assert_eq!(m.turns_tool_calls, 1);
        assert_eq!(m.turns_parallel, 0);
    }

    #[test]
    fn extract_assistant_with_parallel_tools() {
        let dir = TempDir::new().unwrap();
        let line = r#"{"type":"assistant","message":{"content":[{"type":"tool_use","name":"Read","input":{}},{"type":"tool_use","name":"Grep","input":{}}]}}"#;
        let path = write_jsonl(dir.path(), &[line]);
        let m = extract_metrics(&path).unwrap();
        assert_eq!(m.turns_total, 1);
        assert_eq!(m.turns_narration_only, 0);
        assert_eq!(m.turns_tool_calls, 2);
        assert_eq!(m.turns_parallel, 1);
    }

    #[test]
    fn extract_result_metrics() {
        let dir = TempDir::new().unwrap();
        let result = r#"{"type":"result","subtype":"success","duration_ms":180000,"num_turns":45,"total_cost_usd":1.234,"modelUsage":{"claude-opus-4-6":{"inputTokens":100,"outputTokens":200,"cacheReadInputTokens":300,"cacheCreationInputTokens":50}}}"#;
        let path = write_jsonl(dir.path(), &[result]);
        let m = extract_metrics(&path).unwrap();
        assert_eq!(m.session_duration_ms, 180000);
        assert_eq!(m.session_num_turns, Some(45));
        assert!((m.cost_estimate_usd - 1.234).abs() < 0.001);
        assert_eq!(m.cost_input_tokens, 100);
        assert_eq!(m.cost_output_tokens, 200);
        assert_eq!(m.cost_cache_read_tokens, 300);
        assert_eq!(m.cost_cache_creation_tokens, 50);
    }

    #[test]
    fn extract_multi_model_usage() {
        let dir = TempDir::new().unwrap();
        let result = r#"{"type":"result","total_cost_usd":2.0,"duration_ms":100,"modelUsage":{"claude-opus-4-6":{"inputTokens":100,"outputTokens":200,"cacheReadInputTokens":0,"cacheCreationInputTokens":0},"claude-haiku-4-5-20251001":{"inputTokens":50,"outputTokens":30,"cacheReadInputTokens":0,"cacheCreationInputTokens":0}}}"#;
        let path = write_jsonl(dir.path(), &[result]);
        let m = extract_metrics(&path).unwrap();
        assert_eq!(m.cost_input_tokens, 150); // 100 + 50
        assert_eq!(m.cost_output_tokens, 230); // 200 + 30
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
        let m = extract_metrics(&path).unwrap();
        assert_eq!(m.turns_total, 3);
        assert_eq!(m.turns_narration_only, 1);
        assert_eq!(m.turns_parallel, 1);
        assert_eq!(m.turns_tool_calls, 3); // 2 + 1
        assert_eq!(m.cost_input_tokens, 1000);
        assert_eq!(m.cost_output_tokens, 500);
        assert!((m.cost_estimate_usd - 0.5).abs() < 0.001);
        assert_eq!(m.session_duration_ms, 5000);
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
        let m = extract_metrics(&path).unwrap();
        assert_eq!(m.turns_total, 1);
    }

    #[test]
    fn extract_output_bytes_is_file_size() {
        let dir = TempDir::new().unwrap();
        let lines =
            &[r#"{"type":"assistant","message":{"content":[{"type":"text","text":"hello"}]}}"#];
        let path = write_jsonl(dir.path(), lines);
        let expected_size = std::fs::metadata(&path).unwrap().len();
        let m = extract_metrics(&path).unwrap();
        assert_eq!(m.session_output_bytes, expected_size);
    }

    #[test]
    fn ingest_session_writes_events() {
        let (_db_dir, conn) = test_db();
        let data_dir = TempDir::new().unwrap();
        let lines = &[
            r#"{"type":"assistant","message":{"content":[{"type":"text","text":"hi"}]}}"#,
            r#"{"type":"assistant","message":{"content":[{"type":"tool_use","name":"Read","input":{}}]}}"#,
            r#"{"type":"result","duration_ms":10000,"total_cost_usd":0.25,"modelUsage":{"opus":{"inputTokens":500,"outputTokens":100,"cacheReadInputTokens":0,"cacheCreationInputTokens":0}}}"#,
        ];
        let path = write_jsonl(data_dir.path(), lines);

        let m = ingest_session(&conn, 42, &path, Some(0)).unwrap();
        assert_eq!(m.turns_total, 2);
        assert_eq!(m.turns_narration_only, 1);
        assert_eq!(m.session_exit_code, Some(0));

        // Verify events were written
        let events = db::events_by_session(&conn, 42).unwrap();
        // Should have: turns.total, turns.narration_only, turns.parallel, turns.tool_calls,
        // cost.input_tokens, cost.output_tokens, cost.cache_read_tokens, cost.cache_creation_tokens,
        // cost.estimate_usd, session.output_bytes, session.exit_code, session.duration_ms = 12
        assert_eq!(events.len(), 12);

        // Verify specific event values
        let turns_total = events.iter().find(|e| e.kind == "turns.total").unwrap();
        assert_eq!(turns_total.value.as_deref(), Some("2"));

        let cost = events
            .iter()
            .find(|e| e.kind == "cost.estimate_usd")
            .unwrap();
        assert_eq!(cost.value.as_deref(), Some("0.250000"));
    }

    #[test]
    fn ingest_session_writes_observation() {
        let (_db_dir, conn) = test_db();
        let data_dir = TempDir::new().unwrap();
        let lines = &[
            r#"{"type":"assistant","message":{"content":[{"type":"text","text":"hi"}]}}"#,
            r#"{"type":"result","duration_ms":60000,"total_cost_usd":1.0,"modelUsage":{"opus":{"inputTokens":100,"outputTokens":50,"cacheReadInputTokens":0,"cacheCreationInputTokens":0}}}"#,
        ];
        let path = write_jsonl(data_dir.path(), lines);

        ingest_session(&conn, 7, &path, None).unwrap();

        let obs = db::get_observation(&conn, 7).unwrap().unwrap();
        assert_eq!(obs.session, 7);
        assert_eq!(obs.duration, Some(60));

        // Verify observation data JSON
        let data: Value = serde_json::from_str(&obs.data).unwrap();
        assert_eq!(data["turns.total"], 1);
        assert_eq!(data["cost.output_tokens"], 50);
    }

    #[test]
    fn ingest_session_no_exit_code_omits_event() {
        let (_db_dir, conn) = test_db();
        let data_dir = TempDir::new().unwrap();
        let lines =
            &[r#"{"type":"result","duration_ms":1000,"total_cost_usd":0.0,"modelUsage":{}}"#];
        let path = write_jsonl(data_dir.path(), lines);

        ingest_session(&conn, 1, &path, None).unwrap();

        let events = db::events_by_session(&conn, 1).unwrap();
        // No exit_code event when None
        assert!(events.iter().all(|e| e.kind != "session.exit_code"));
    }

    #[test]
    fn ingest_session_idempotent_observation() {
        let (_db_dir, conn) = test_db();
        let data_dir = TempDir::new().unwrap();
        let lines =
            &[r#"{"type":"result","duration_ms":1000,"total_cost_usd":0.5,"modelUsage":{}}"#];
        let path = write_jsonl(data_dir.path(), lines);

        // Ingest twice — observation should be replaced, not duplicated
        ingest_session(&conn, 1, &path, Some(0)).unwrap();
        ingest_session(&conn, 1, &path, Some(0)).unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM observations", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn extract_file_not_found() {
        let result = extract_metrics(Path::new("/nonexistent/file.jsonl"));
        assert!(result.is_err());
    }

    #[test]
    fn build_observation_data_roundtrip() {
        let m = SessionMetrics {
            turns_total: 42,
            turns_narration_only: 3,
            turns_parallel: 5,
            turns_tool_calls: 80,
            cost_input_tokens: 10000,
            cost_output_tokens: 5000,
            cost_cache_read_tokens: 200,
            cost_cache_creation_tokens: 100,
            cost_estimate_usd: 1.5,
            session_duration_ms: 120000,
            session_output_bytes: 50000,
            session_exit_code: Some(0),
            session_num_turns: Some(42),
        };
        let json_str = build_observation_data_with_rules(&m, &[]);
        let v: Value = serde_json::from_str(&json_str).unwrap();
        assert_eq!(v["turns.total"], 42);
        assert_eq!(v["turns.parallel"], 5);
        assert_eq!(v["cost.estimate_usd"], 1.5);
        assert_eq!(v["session.exit_code"], 0);
        assert_eq!(v["session.duration_ms"], 120000);
    }

    #[test]
    fn extract_assistant_no_content() {
        let dir = TempDir::new().unwrap();
        // Assistant message with no content array
        let line = r#"{"type":"assistant","message":{}}"#;
        let path = write_jsonl(dir.path(), &[line]);
        let m = extract_metrics(&path).unwrap();
        assert_eq!(m.turns_total, 1);
        assert_eq!(m.turns_narration_only, 0);
        assert_eq!(m.turns_tool_calls, 0);
    }

    #[test]
    fn ingest_real_file_format() {
        // Test with realistic multi-line JSONL mimicking real Claude output
        let (_db_dir, conn) = test_db();
        let data_dir = TempDir::new().unwrap();
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
        let path = write_jsonl(data_dir.path(), lines);

        let m = ingest_session(&conn, 10, &path, Some(0)).unwrap();
        assert_eq!(m.turns_total, 4); // 4 assistant messages
        assert_eq!(m.turns_narration_only, 2); // "Starting work." and "Done."
        assert_eq!(m.turns_parallel, 1); // the one with 2 Read tool_use
        assert_eq!(m.turns_tool_calls, 3); // 2 Read + 1 Edit
        assert_eq!(m.cost_input_tokens, 24 + 47934);
        assert_eq!(m.cost_output_tokens, 9407 + 947);
        assert!((m.cost_estimate_usd - 0.99).abs() < 0.001);
        assert_eq!(m.session_duration_ms, 229857);
        assert_eq!(m.session_num_turns, Some(49));
    }

    // --- Extraction rule tests ---

    use crate::config::ExtractionRule;

    fn make_rule(kind: &str, pattern: &str) -> ExtractionRule {
        ExtractionRule {
            kind: kind.to_string(),
            pattern: pattern.to_string(),
            anti_pattern: None,
            source: "tool_commands".to_string(),
            transform: None,
            first_match: false,
            count: false,
            emit: None,
        }
    }

    #[test]
    fn rule_count_tool_commands() {
        let text = SessionText {
            raw_lines: vec![],
            text_blocks: vec![],
            tool_commands: vec![
                "cargo test".to_string(),
                "cargo clippy".to_string(),
                "cargo test --filter foo".to_string(),
            ],
        };
        let mut rule = make_rule("extract.test_runs", "cargo test");
        rule.count = true;
        let compiled = rule.compile().unwrap();
        let results = apply_rules(&[compiled], &text);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "extract.test_runs");
        assert_eq!(results[0].1, "2"); // matches "cargo test" and "cargo test --filter foo"
    }

    #[test]
    fn rule_count_with_anti_pattern() {
        let text = SessionText {
            raw_lines: vec![],
            text_blocks: vec![],
            tool_commands: vec![
                "cargo test".to_string(),
                "cargo test --filter foo".to_string(),
                "cargo test --filter bar".to_string(),
            ],
        };
        let mut rule = make_rule("extract.full_suite", "cargo test");
        rule.count = true;
        rule.anti_pattern = Some("--filter".to_string());
        let compiled = rule.compile().unwrap();
        let results = apply_rules(&[compiled], &text);
        assert_eq!(results[0].1, "1"); // only "cargo test" without --filter
    }

    #[test]
    fn rule_emit_boolean() {
        let text = SessionText {
            raw_lines: vec![],
            text_blocks: vec![],
            tool_commands: vec!["./bd-finish.sh bd-xyz".to_string()],
        };
        let mut rule = make_rule("commit.detected", "bd-finish");
        rule.emit = Some(toml::Value::Boolean(true));
        let compiled = rule.compile().unwrap();
        let results = apply_rules(&[compiled], &text);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1, "true");
    }

    #[test]
    fn rule_emit_no_match() {
        let text = SessionText {
            raw_lines: vec![],
            text_blocks: vec![],
            tool_commands: vec!["cargo build".to_string()],
        };
        let mut rule = make_rule("commit.detected", "bd-finish");
        rule.emit = Some(toml::Value::Boolean(true));
        let compiled = rule.compile().unwrap();
        let results = apply_rules(&[compiled], &text);
        assert!(results.is_empty());
    }

    #[test]
    fn rule_first_match_with_capture_group() {
        let text = SessionText {
            raw_lines: vec![],
            text_blocks: vec![],
            tool_commands: vec![
                "bd update simple-agent-harness-xyz --status in_progress".to_string(),
                "bd update simple-agent-harness-abc --status in_progress".to_string(),
            ],
        };
        let mut rule = make_rule("extract.bead_id", r"bd update (\S+) --status.?in.?progress");
        rule.first_match = true;
        rule.transform = Some("last_segment".to_string());
        let compiled = rule.compile().unwrap();
        let results = apply_rules(&[compiled], &text);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1, "xyz"); // last segment of "simple-agent-harness-xyz"
    }

    #[test]
    fn rule_source_text() {
        let text = SessionText {
            raw_lines: vec![],
            text_blocks: vec![
                "Let me check the tests.".to_string(),
                "All tests passed!".to_string(),
            ],
            tool_commands: vec![],
        };
        let mut rule = make_rule("extract.mentions_tests", "tests");
        rule.source = "text".to_string();
        rule.count = true;
        let compiled = rule.compile().unwrap();
        let results = apply_rules(&[compiled], &text);
        assert_eq!(results[0].1, "2");
    }

    #[test]
    fn rule_source_raw() {
        let text = SessionText {
            raw_lines: vec![
                r#"{"type":"assistant","message":{"content":[{"type":"tool_use","name":"Read","input":{}}]}}"#.to_string(),
                r#"{"type":"assistant","message":{"content":[{"type":"tool_use","name":"Read","input":{}}]}}"#.to_string(),
                r#"{"type":"assistant","message":{"content":[{"type":"text","text":"hello"}]}}"#.to_string(),
            ],
            text_blocks: vec![],
            tool_commands: vec![],
        };
        let mut rule = make_rule("extract.file_reads", r#""name":\s*"Read""#);
        rule.source = "raw".to_string();
        rule.count = true;
        let compiled = rule.compile().unwrap();
        let results = apply_rules(&[compiled], &text);
        assert_eq!(results[0].1, "2");
    }

    #[test]
    fn rule_transform_int() {
        let text = SessionText {
            raw_lines: vec![],
            text_blocks: vec!["Found 42 errors".to_string()],
            tool_commands: vec![],
        };
        let mut rule = make_rule("extract.errors", r"Found (\d+) errors");
        rule.source = "text".to_string();
        rule.first_match = true;
        rule.transform = Some("int".to_string());
        let compiled = rule.compile().unwrap();
        let results = apply_rules(&[compiled], &text);
        assert_eq!(results[0].1, "42");
    }

    #[test]
    fn rule_transform_trim() {
        let text = SessionText {
            raw_lines: vec![],
            text_blocks: vec!["status:  done  ".to_string()],
            tool_commands: vec![],
        };
        let mut rule = make_rule("extract.status", r"status:\s+(.+)");
        rule.source = "text".to_string();
        rule.first_match = true;
        rule.transform = Some("trim".to_string());
        let compiled = rule.compile().unwrap();
        let results = apply_rules(&[compiled], &text);
        assert_eq!(results[0].1, "done");
    }

    #[test]
    fn rule_no_matches_returns_empty() {
        let text = SessionText {
            raw_lines: vec![],
            text_blocks: vec![],
            tool_commands: vec!["cargo build".to_string()],
        };
        let rule = make_rule("extract.missing", "nonexistent_pattern");
        let compiled = rule.compile().unwrap();
        let results = apply_rules(&[compiled], &text);
        assert!(results.is_empty());
    }

    #[test]
    fn rule_count_zero_still_emitted() {
        let text = SessionText {
            raw_lines: vec![],
            text_blocks: vec![],
            tool_commands: vec!["cargo build".to_string()],
        };
        let mut rule = make_rule("extract.test_runs", "cargo test");
        rule.count = true;
        let compiled = rule.compile().unwrap();
        let results = apply_rules(&[compiled], &text);
        // Count mode always emits (even 0)
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1, "0");
    }

    #[test]
    fn multiple_rules_applied() {
        let text = SessionText {
            raw_lines: vec![],
            text_blocks: vec![],
            tool_commands: vec![
                "cargo test".to_string(),
                "./bd-finish.sh bd-abc".to_string(),
            ],
        };

        let mut r1 = make_rule("extract.test_runs", "cargo test");
        r1.count = true;
        let mut r2 = make_rule("commit.detected", "bd-finish");
        r2.emit = Some(toml::Value::Boolean(true));

        let c1 = r1.compile().unwrap();
        let c2 = r2.compile().unwrap();
        let results = apply_rules(&[c1, c2], &text);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, "extract.test_runs");
        assert_eq!(results[0].1, "1");
        assert_eq!(results[1].0, "commit.detected");
        assert_eq!(results[1].1, "true");
    }

    #[test]
    fn ingest_with_rules_writes_events_and_observation() {
        let (_db_dir, conn) = test_db();
        let data_dir = TempDir::new().unwrap();
        let lines = &[
            r#"{"type":"assistant","message":{"content":[{"type":"tool_use","name":"Bash","input":{"command":"cargo test"}}]}}"#,
            r#"{"type":"assistant","message":{"content":[{"type":"tool_use","name":"Bash","input":{"command":"./bd-finish.sh bd-xyz"}}]}}"#,
            r#"{"type":"result","duration_ms":5000,"total_cost_usd":0.5,"modelUsage":{}}"#,
        ];
        let path = write_jsonl(data_dir.path(), lines);

        let mut r1 = make_rule("extract.test_runs", "cargo test");
        r1.count = true;
        let mut r2 = make_rule("commit.detected", "bd-finish");
        r2.emit = Some(toml::Value::Boolean(true));

        let c1 = r1.compile().unwrap();
        let c2 = r2.compile().unwrap();

        ingest_session_with_rules(&conn, 1, &path, Some(0), &[c1, c2]).unwrap();

        // Check events include rule-extracted ones
        let events = db::events_by_session(&conn, 1).unwrap();
        let test_runs = events
            .iter()
            .find(|e| e.kind == "extract.test_runs")
            .unwrap();
        assert_eq!(test_runs.value.as_deref(), Some("1"));

        let commit = events.iter().find(|e| e.kind == "commit.detected").unwrap();
        assert_eq!(commit.value.as_deref(), Some("true"));

        // Check observation includes rule-extracted data
        let obs = db::get_observation(&conn, 1).unwrap().unwrap();
        let data: Value = serde_json::from_str(&obs.data).unwrap();
        assert_eq!(data["extract.test_runs"], 1);
        assert_eq!(data["commit.detected"], true);
        // Built-in metrics still present
        assert_eq!(data["turns.total"], 2);
    }

    #[test]
    fn collect_tool_commands_from_input() {
        let dir = TempDir::new().unwrap();
        let lines = &[
            r#"{"type":"assistant","message":{"content":[{"type":"tool_use","name":"Bash","input":{"command":"cargo test --filter foo"}}]}}"#,
            r#"{"type":"assistant","message":{"content":[{"type":"tool_use","name":"Bash","input":{"command":"cargo clippy"}}]}}"#,
        ];
        let path = write_jsonl(dir.path(), lines);
        let (_, text) = extract_metrics_and_text(&path).unwrap();
        assert_eq!(text.tool_commands.len(), 2);
        assert_eq!(text.tool_commands[0], "cargo test --filter foo");
        assert_eq!(text.tool_commands[1], "cargo clippy");
    }

    #[test]
    fn collect_text_blocks() {
        let dir = TempDir::new().unwrap();
        let lines = &[
            r#"{"type":"assistant","message":{"content":[{"type":"text","text":"Hello world"},{"type":"tool_use","name":"Read","input":{}}]}}"#,
            r#"{"type":"assistant","message":{"content":[{"type":"text","text":"Done."}]}}"#,
        ];
        let path = write_jsonl(dir.path(), lines);
        let (_, text) = extract_metrics_and_text(&path).unwrap();
        assert_eq!(text.text_blocks.len(), 2);
        assert_eq!(text.text_blocks[0], "Hello world");
        assert_eq!(text.text_blocks[1], "Done.");
    }

    #[test]
    fn compile_invalid_pattern_returns_error() {
        let rule = make_rule("bad", "[invalid");
        assert!(rule.compile().is_err());
    }

    #[test]
    fn compile_invalid_anti_pattern_returns_error() {
        let mut rule = make_rule("test", "valid");
        rule.anti_pattern = Some("[invalid".to_string());
        assert!(rule.compile().is_err());
    }
}
