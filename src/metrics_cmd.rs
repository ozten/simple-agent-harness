use crate::config::MetricsTargetsConfig;
use crate::db;
use crate::ingest;
use rusqlite::Connection;
use std::path::Path;

/// Handle the `metrics log <file>` subcommand.
///
/// Ingests a JSONL session file into the database. The session number is
/// auto-assigned as max(session)+1 from the observations table.
pub fn handle_log(db_path: &Path, file: &Path) -> Result<(), String> {
    let conn = db::open_or_create(db_path).map_err(|e| format!("Failed to open database: {e}"))?;

    // Auto-assign session number: max existing + 1
    let session: i64 = conn
        .query_row(
            "SELECT COALESCE(MAX(session), 0) + 1 FROM observations",
            [],
            |row| row.get(0),
        )
        .map_err(|e| format!("Failed to determine session number: {e}"))?;

    let metrics = ingest::ingest_session(&conn, session, file, None)
        .map_err(|e| format!("Ingestion failed: {e}"))?;

    println!("Ingested session {session} from {}", file.display());
    println!(
        "  turns: {}  cost: ${:.2}  duration: {}s",
        metrics.turns_total,
        metrics.cost_estimate_usd,
        metrics.session_duration_ms / 1000
    );

    Ok(())
}

/// Handle the `metrics status [--last N]` subcommand.
///
/// Displays a dashboard of recent session observations with key metrics.
pub fn handle_status(db_path: &Path, last: i64) -> Result<(), String> {
    if !db_path.exists() {
        println!("No metrics database found. Run some sessions first.");
        return Ok(());
    }

    let conn = db::open_or_create(db_path).map_err(|e| format!("Failed to open database: {e}"))?;

    let observations = db::recent_observations(&conn, last)
        .map_err(|e| format!("Failed to query observations: {e}"))?;

    if observations.is_empty() {
        println!("No session observations recorded yet.");
        return Ok(());
    }

    // Print header
    println!(
        "{:<8} {:<12} {:>6} {:>8} {:>10} {:>6} {:>8}",
        "SESSION", "DATE", "TURNS", "COST", "DURATION", "NARR%", "PARALL%"
    );
    println!("{}", "-".repeat(64));

    // Collect for summary stats
    let mut total_turns: u64 = 0;
    let mut total_cost: f64 = 0.0;
    let mut total_duration: u64 = 0;
    let mut total_narration: u64 = 0;
    let mut total_parallel: u64 = 0;
    let count = observations.len();

    // Print rows (observations come in DESC order, reverse for chronological display)
    for obs in observations.iter().rev() {
        let data: serde_json::Value = serde_json::from_str(&obs.data).unwrap_or_default();

        let turns = data["turns.total"].as_u64().unwrap_or(0);
        let narration = data["turns.narration_only"].as_u64().unwrap_or(0);
        let parallel = data["turns.parallel"].as_u64().unwrap_or(0);
        let cost = data["cost.estimate_usd"].as_f64().unwrap_or(0.0);
        let duration_ms = data["session.duration_ms"].as_u64().unwrap_or(0);
        let duration_s = duration_ms / 1000;

        total_turns += turns;
        total_cost += cost;
        total_duration += duration_s;
        total_narration += narration;
        total_parallel += parallel;

        let narr_pct = if turns > 0 {
            (narration as f64 / turns as f64 * 100.0) as u64
        } else {
            0
        };
        let par_pct = if turns > 0 {
            (parallel as f64 / turns as f64 * 100.0) as u64
        } else {
            0
        };

        // Truncate timestamp to date
        let date = if obs.ts.len() >= 10 {
            &obs.ts[..10]
        } else {
            &obs.ts
        };

        let duration_str = format_duration(duration_s);

        println!(
            "{:<8} {:<12} {:>6} {:>8} {:>10} {:>5}% {:>7}%",
            obs.session,
            date,
            turns,
            format!("${:.2}", cost),
            duration_str,
            narr_pct,
            par_pct
        );
    }

    // Summary
    println!("{}", "-".repeat(64));
    let avg_turns = total_turns as f64 / count as f64;
    let avg_cost = total_cost / count as f64;
    let avg_duration = total_duration / count as u64;
    let avg_narr_pct = if total_turns > 0 {
        (total_narration as f64 / total_turns as f64 * 100.0) as u64
    } else {
        0
    };
    let avg_par_pct = if total_turns > 0 {
        (total_parallel as f64 / total_turns as f64 * 100.0) as u64
    } else {
        0
    };

    println!(
        "{:<8} {:<12} {:>6} {:>8} {:>10} {:>5}% {:>7}%",
        "AVG",
        "",
        format!("{:.0}", avg_turns),
        format!("${:.2}", avg_cost),
        format_duration(avg_duration),
        avg_narr_pct,
        avg_par_pct
    );
    println!(
        "{:<8} {:<12} {:>6} {:>8}",
        "TOTAL",
        "",
        total_turns,
        format!("${:.2}", total_cost)
    );

    println!("\n{count} session(s) shown");

    Ok(())
}

/// Handle the `metrics targets` subcommand.
///
/// Evaluates configured target rules against recent observations and
/// displays pass/fail status for each.
pub fn handle_targets(
    db_path: &Path,
    last: i64,
    targets_config: &MetricsTargetsConfig,
) -> Result<(), String> {
    if targets_config.rules.is_empty() {
        println!("No target rules configured in [metrics.targets.rules].");
        println!("Add rules to your blacksmith.toml to track performance targets.");
        return Ok(());
    }

    if !db_path.exists() {
        println!("No metrics database found. Run some sessions first.");
        return Ok(());
    }

    let conn = db::open_or_create(db_path).map_err(|e| format!("Failed to open database: {e}"))?;

    let observations = db::recent_observations(&conn, last)
        .map_err(|e| format!("Failed to query observations: {e}"))?;

    if observations.is_empty() {
        println!("No session observations recorded yet.");
        return Ok(());
    }

    // Parse all observation data into JSON values
    let data_values: Vec<serde_json::Value> = observations
        .iter()
        .map(|obs| serde_json::from_str(&obs.data).unwrap_or_default())
        .collect();

    let session_count = data_values.len();
    let mut pass_count = 0;
    let mut total_rules = 0;

    println!(
        "Targets (evaluated over last {} session{}):\n",
        session_count,
        if session_count == 1 { "" } else { "s" }
    );

    for rule in &targets_config.rules {
        total_rules += 1;
        let result = evaluate_target(rule, &data_values);

        let (status_str, passed) = match &result {
            TargetResult::Pass { actual, .. } => {
                pass_count += 1;
                (format_target_line(rule, *actual, true), true)
            }
            TargetResult::Fail { actual, .. } => (format_target_line(rule, *actual, false), false),
            TargetResult::NoData => (format!("  {}: no data", rule.label), false),
        };

        let _ = passed; // used via pass_count
        println!("{status_str}");
    }

    println!();
    if pass_count == total_rules {
        println!("All {total_rules} target(s) met.");
    } else {
        println!(
            "{pass_count}/{total_rules} target(s) met, {} missed.",
            total_rules - pass_count
        );
    }

    Ok(())
}

/// Handle the `metrics migrate --from <path>` subcommand.
///
/// Imports data from a V1 self-improvement.db into the V2 database.
/// Maps V1 sessions table columns to V2 event kinds and observations.
/// Copies improvements rows, mapping severity to category.
pub fn handle_migrate(db_path: &Path, from: &Path) -> Result<(), String> {
    if !from.exists() {
        return Err(format!("V1 database not found: {}", from.display()));
    }

    // Open V1 database (read-only)
    let v1_conn = Connection::open_with_flags(from, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(|e| format!("Failed to open V1 database: {e}"))?;

    // Open V2 database
    let v2_conn =
        db::open_or_create(db_path).map_err(|e| format!("Failed to open V2 database: {e}"))?;

    // Check what tables exist in V1
    let v1_tables = list_tables(&v1_conn).map_err(|e| format!("Failed to list V1 tables: {e}"))?;

    let mut sessions_imported = 0u64;
    let mut improvements_imported = 0u64;

    // Import sessions if the table exists
    if v1_tables.contains(&"sessions".to_string()) {
        sessions_imported = migrate_sessions(&v1_conn, &v2_conn)
            .map_err(|e| format!("Failed to migrate sessions: {e}"))?;
    }

    // Import improvements if the table exists
    if v1_tables.contains(&"improvements".to_string()) {
        improvements_imported = migrate_improvements(&v1_conn, &v2_conn)
            .map_err(|e| format!("Failed to migrate improvements: {e}"))?;
    }

    if sessions_imported == 0 && improvements_imported == 0 {
        println!("No data found to import from {}", from.display());
    } else {
        println!(
            "Migrated from {}: {} session(s), {} improvement(s)",
            from.display(),
            sessions_imported,
            improvements_imported
        );
    }

    Ok(())
}

/// List table names in a SQLite database.
fn list_tables(conn: &Connection) -> rusqlite::Result<Vec<String>> {
    let mut stmt =
        conn.prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")?;
    let rows = stmt
        .query_map([], |row| row.get(0))?
        .collect::<rusqlite::Result<Vec<String>>>()?;
    Ok(rows)
}

/// Migrate V1 sessions table to V2 events + observations.
///
/// V1 sessions schema (expected columns):
///   id, timestamp, assistant_turns, cost_usd, duration_secs,
///   input_tokens, output_tokens, cache_read_tokens, cache_creation_tokens,
///   num_tool_calls, narration_turns, parallel_turns, output_bytes, exit_code
///
/// Mapping to V2 event kinds:
///   assistant_turns -> turns.total
///   narration_turns -> turns.narration_only
///   parallel_turns -> turns.parallel
///   num_tool_calls -> turns.tool_calls
///   input_tokens -> cost.input_tokens
///   output_tokens -> cost.output_tokens
///   cache_read_tokens -> cost.cache_read_tokens
///   cache_creation_tokens -> cost.cache_creation_tokens
///   cost_usd -> cost.estimate_usd
///   output_bytes -> session.output_bytes
///   duration_secs -> session.duration_ms (* 1000)
///   exit_code -> session.exit_code
fn migrate_sessions(v1: &Connection, v2: &Connection) -> rusqlite::Result<u64> {
    // Discover available columns in V1 sessions table
    let columns = list_columns(v1, "sessions")?;

    // Build a query that selects available columns
    let mut select_cols = vec!["id".to_string()];
    let optional_cols = [
        "timestamp",
        "assistant_turns",
        "cost_usd",
        "duration_secs",
        "input_tokens",
        "output_tokens",
        "cache_read_tokens",
        "cache_creation_tokens",
        "num_tool_calls",
        "narration_turns",
        "parallel_turns",
        "output_bytes",
        "exit_code",
    ];
    for col in &optional_cols {
        if columns.contains(&col.to_string()) {
            select_cols.push(col.to_string());
        }
    }

    let sql = format!(
        "SELECT {} FROM sessions ORDER BY id ASC",
        select_cols.join(", ")
    );
    let mut stmt = v1.prepare(&sql)?;

    let col_index = |name: &str| -> Option<usize> { select_cols.iter().position(|c| c == name) };

    let mut count = 0u64;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let session_id: i64 = row.get(0)?;

        let ts: String = col_index("timestamp")
            .and_then(|i| row.get::<_, String>(i).ok())
            .unwrap_or_else(|| chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string());

        let turns_total: i64 = col_index("assistant_turns")
            .and_then(|i| row.get(i).ok())
            .unwrap_or(0);
        let narration_turns: i64 = col_index("narration_turns")
            .and_then(|i| row.get(i).ok())
            .unwrap_or(0);
        let parallel_turns: i64 = col_index("parallel_turns")
            .and_then(|i| row.get(i).ok())
            .unwrap_or(0);
        let tool_calls: i64 = col_index("num_tool_calls")
            .and_then(|i| row.get(i).ok())
            .unwrap_or(0);
        let input_tokens: i64 = col_index("input_tokens")
            .and_then(|i| row.get(i).ok())
            .unwrap_or(0);
        let output_tokens: i64 = col_index("output_tokens")
            .and_then(|i| row.get(i).ok())
            .unwrap_or(0);
        let cache_read: i64 = col_index("cache_read_tokens")
            .and_then(|i| row.get(i).ok())
            .unwrap_or(0);
        let cache_creation: i64 = col_index("cache_creation_tokens")
            .and_then(|i| row.get(i).ok())
            .unwrap_or(0);
        let cost_usd: f64 = col_index("cost_usd")
            .and_then(|i| row.get(i).ok())
            .unwrap_or(0.0);
        let output_bytes: i64 = col_index("output_bytes")
            .and_then(|i| row.get(i).ok())
            .unwrap_or(0);
        let duration_secs: i64 = col_index("duration_secs")
            .and_then(|i| row.get(i).ok())
            .unwrap_or(0);
        let exit_code: Option<i64> = col_index("exit_code").and_then(|i| row.get(i).ok());
        let duration_ms = duration_secs * 1000;

        // Write events
        let event_mappings: Vec<(&str, String)> = vec![
            ("turns.total", turns_total.to_string()),
            ("turns.narration_only", narration_turns.to_string()),
            ("turns.parallel", parallel_turns.to_string()),
            ("turns.tool_calls", tool_calls.to_string()),
            ("cost.input_tokens", input_tokens.to_string()),
            ("cost.output_tokens", output_tokens.to_string()),
            ("cost.cache_read_tokens", cache_read.to_string()),
            ("cost.cache_creation_tokens", cache_creation.to_string()),
            ("cost.estimate_usd", format!("{cost_usd:.6}")),
            ("session.output_bytes", output_bytes.to_string()),
            ("session.duration_ms", duration_ms.to_string()),
        ];

        for (kind, value) in &event_mappings {
            db::insert_event_with_ts(v2, &ts, session_id, kind, Some(value), None)?;
        }

        if let Some(code) = exit_code {
            db::insert_event_with_ts(
                v2,
                &ts,
                session_id,
                "session.exit_code",
                Some(&code.to_string()),
                None,
            )?;
        }

        // Build observation data JSON
        let mut map = serde_json::Map::new();
        map.insert("turns.total".into(), serde_json::json!(turns_total));
        map.insert(
            "turns.narration_only".into(),
            serde_json::json!(narration_turns),
        );
        map.insert("turns.parallel".into(), serde_json::json!(parallel_turns));
        map.insert("turns.tool_calls".into(), serde_json::json!(tool_calls));
        map.insert("cost.input_tokens".into(), serde_json::json!(input_tokens));
        map.insert(
            "cost.output_tokens".into(),
            serde_json::json!(output_tokens),
        );
        map.insert(
            "cost.cache_read_tokens".into(),
            serde_json::json!(cache_read),
        );
        map.insert(
            "cost.cache_creation_tokens".into(),
            serde_json::json!(cache_creation),
        );
        map.insert(
            "cost.estimate_usd".into(),
            serde_json::Number::from_f64(cost_usd)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
        );
        map.insert(
            "session.output_bytes".into(),
            serde_json::json!(output_bytes),
        );
        map.insert("session.duration_ms".into(), serde_json::json!(duration_ms));
        if let Some(code) = exit_code {
            map.insert("session.exit_code".into(), serde_json::json!(code));
        }

        let data = serde_json::Value::Object(map).to_string();
        db::upsert_observation(v2, session_id, &ts, Some(duration_secs), None, &data)?;

        count += 1;
    }

    Ok(count)
}

/// List column names for a table.
fn list_columns(conn: &Connection, table: &str) -> rusqlite::Result<Vec<String>> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({})", table))?;
    let rows = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<String>>>()?;
    Ok(rows)
}

/// Migrate V1 improvements table to V2.
///
/// V1 improvements may have a `severity` column that maps to V2 `category`.
/// Other columns (ref, title, body, context, tags, status, created, resolved, meta)
/// are copied directly if they exist.
fn migrate_improvements(v1: &Connection, v2: &Connection) -> rusqlite::Result<u64> {
    let columns = list_columns(v1, "improvements")?;

    let has_severity = columns.contains(&"severity".to_string());
    let has_category = columns.contains(&"category".to_string());

    // Build select list from available columns
    let needed = [
        "ref", "created", "status", "title", "body", "context", "tags", "meta",
    ];
    let mut select_cols = Vec::new();
    for col in &needed {
        if columns.contains(&col.to_string()) {
            select_cols.push(col.to_string());
        }
    }

    // Add category source column
    if has_severity {
        select_cols.push("severity".to_string());
    } else if has_category && !select_cols.contains(&"category".to_string()) {
        select_cols.push("category".to_string());
    }

    if select_cols.is_empty() || !columns.contains(&"title".to_string()) {
        // No useful data to import
        return Ok(0);
    }

    let sql = format!(
        "SELECT {} FROM improvements ORDER BY rowid ASC",
        select_cols.join(", ")
    );
    let mut stmt = v1.prepare(&sql)?;

    let col_idx = |name: &str| -> Option<usize> { select_cols.iter().position(|c| c == name) };

    let mut count = 0u64;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let title: String = match col_idx("title") {
            Some(i) => row.get(i)?,
            None => continue,
        };

        // Determine category: map severity -> category if needed
        let category = if has_severity {
            let severity: String = col_idx("severity")
                .and_then(|i| row.get::<_, String>(i).ok())
                .unwrap_or_else(|| "workflow".to_string());
            map_severity_to_category(&severity)
        } else {
            col_idx("category")
                .and_then(|i| row.get::<_, String>(i).ok())
                .unwrap_or_else(|| "workflow".to_string())
        };

        let body: Option<String> = col_idx("body").and_then(|i| row.get(i).ok());
        let context: Option<String> = col_idx("context").and_then(|i| row.get(i).ok());
        let tags: Option<String> = col_idx("tags").and_then(|i| row.get(i).ok());

        db::insert_improvement(
            v2,
            &category,
            &title,
            body.as_deref(),
            context.as_deref(),
            tags.as_deref(),
        )
        .map_err(|e| {
            // If a duplicate ref, skip it gracefully
            tracing::debug!("Skipping improvement '{}': {}", title, e);
            e
        })
        .ok();

        count += 1;
    }

    Ok(count)
}

/// Map V1 severity levels to V2 category names.
fn map_severity_to_category(severity: &str) -> String {
    match severity.to_lowercase().as_str() {
        "critical" | "high" => "reliability".to_string(),
        "medium" | "normal" => "workflow".to_string(),
        "low" | "minor" => "code-quality".to_string(),
        "cost" => "cost".to_string(),
        "performance" => "performance".to_string(),
        other => other.to_string(),
    }
}

enum TargetResult {
    Pass { actual: f64 },
    Fail { actual: f64 },
    NoData,
}

fn evaluate_target(
    rule: &crate::config::TargetRule,
    data_values: &[serde_json::Value],
) -> TargetResult {
    let actual = match rule.compare.as_str() {
        "avg" => {
            let values: Vec<f64> = data_values
                .iter()
                .filter_map(|d| d[&rule.kind].as_f64())
                .collect();
            if values.is_empty() {
                return TargetResult::NoData;
            }
            values.iter().sum::<f64>() / values.len() as f64
        }
        "pct_of" => {
            let relative_to = match &rule.relative_to {
                Some(r) => r,
                None => return TargetResult::NoData,
            };
            let mut numerator_sum = 0.0;
            let mut denominator_sum = 0.0;
            for d in data_values {
                let num = d[&rule.kind].as_f64().unwrap_or(0.0);
                let den = d[relative_to.as_str()].as_f64().unwrap_or(0.0);
                numerator_sum += num;
                denominator_sum += den;
            }
            if denominator_sum == 0.0 {
                return TargetResult::NoData;
            }
            (numerator_sum / denominator_sum) * 100.0
        }
        "pct_sessions" => {
            let sessions_with_event = data_values
                .iter()
                .filter(|d| d.get(&rule.kind).is_some() && !d[&rule.kind].is_null())
                .count();
            if data_values.is_empty() {
                return TargetResult::NoData;
            }
            (sessions_with_event as f64 / data_values.len() as f64) * 100.0
        }
        _ => return TargetResult::NoData,
    };

    let passed = match rule.direction.as_str() {
        "below" => actual <= rule.threshold,
        "above" => actual >= rule.threshold,
        _ => false,
    };

    if passed {
        TargetResult::Pass { actual }
    } else {
        TargetResult::Fail { actual }
    }
}

fn format_target_line(rule: &crate::config::TargetRule, actual: f64, passed: bool) -> String {
    let unit = rule.unit.as_deref().unwrap_or("");
    let dir_symbol = match rule.direction.as_str() {
        "below" => "<",
        "above" => ">",
        _ => "?",
    };
    let status = if passed { "OK" } else { "MISS" };

    // Format actual value: if unit is "$", prefix; otherwise suffix
    let actual_str = if unit == "$" {
        format!("${:.2}", actual)
    } else if unit == "%" {
        format!("{:.0}%", actual)
    } else {
        format!("{:.1}", actual)
    };

    let target_str = if unit == "$" {
        format!("{}{:.2}", dir_symbol, rule.threshold)
    } else if unit == "%" {
        format!("{}{:.0}%", dir_symbol, rule.threshold)
    } else {
        format!("{}{:.0}", dir_symbol, rule.threshold)
    };

    format!(
        "  {:<30} {:>8} (target: {:>6}) — {}",
        rule.label, actual_str, target_str, status
    )
}

fn format_duration(seconds: u64) -> String {
    let mins = seconds / 60;
    let secs = seconds % 60;
    if mins > 0 {
        format!("{}m{:02}s", mins, secs)
    } else {
        format!("{}s", secs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;
    use tempfile::TempDir;

    fn test_db_path() -> (TempDir, std::path::PathBuf) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("blacksmith.db");
        (dir, path)
    }

    #[test]
    fn format_duration_seconds_only() {
        assert_eq!(format_duration(45), "45s");
    }

    #[test]
    fn format_duration_with_minutes() {
        assert_eq!(format_duration(125), "2m05s");
    }

    #[test]
    fn format_duration_zero() {
        assert_eq!(format_duration(0), "0s");
    }

    #[test]
    fn status_no_database() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("nonexistent.db");
        handle_status(&path, 10).unwrap();
    }

    #[test]
    fn status_empty_database() {
        let (_dir, path) = test_db_path();
        db::open_or_create(&path).unwrap();
        handle_status(&path, 10).unwrap();
    }

    #[test]
    fn status_with_observations() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();

        let data = r#"{"turns.total":42,"turns.narration_only":3,"turns.parallel":5,"cost.estimate_usd":1.5,"session.duration_ms":120000}"#;
        db::upsert_observation(
            &conn,
            1,
            "2026-02-15T10:00:00Z",
            Some(120),
            Some("completed"),
            data,
        )
        .unwrap();
        db::upsert_observation(
            &conn,
            2,
            "2026-02-15T11:00:00Z",
            Some(90),
            Some("completed"),
            data,
        )
        .unwrap();

        drop(conn);
        handle_status(&path, 10).unwrap();
    }

    #[test]
    fn status_respects_last_limit() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();

        for i in 1..=5 {
            let data = format!(
                r#"{{"turns.total":{},"turns.narration_only":0,"turns.parallel":0,"cost.estimate_usd":0.5,"session.duration_ms":60000}}"#,
                i * 10
            );
            db::upsert_observation(&conn, i, "2026-02-15T10:00:00Z", Some(60), None, &data)
                .unwrap();
        }

        drop(conn);
        // Should only show last 2
        handle_status(&path, 2).unwrap();
    }

    #[test]
    fn log_ingests_jsonl() {
        let (_dir, path) = test_db_path();
        let data_dir = TempDir::new().unwrap();

        // Create a test JSONL file
        let jsonl_path = data_dir.path().join("test.jsonl");
        std::fs::write(
            &jsonl_path,
            r#"{"type":"assistant","message":{"content":[{"type":"text","text":"hello"}]}}
{"type":"result","duration_ms":10000,"total_cost_usd":0.5,"modelUsage":{"opus":{"inputTokens":100,"outputTokens":50,"cacheReadInputTokens":0,"cacheCreationInputTokens":0}}}
"#,
        )
        .unwrap();

        handle_log(&path, &jsonl_path).unwrap();

        // Verify it was ingested
        let conn = db::open_or_create(&path).unwrap();
        let obs = db::get_observation(&conn, 1).unwrap();
        assert!(obs.is_some());
        assert_eq!(obs.unwrap().session, 1);
    }

    #[test]
    fn log_auto_increments_session() {
        let (_dir, path) = test_db_path();
        let data_dir = TempDir::new().unwrap();

        let jsonl_path = data_dir.path().join("test.jsonl");
        std::fs::write(
            &jsonl_path,
            r#"{"type":"result","duration_ms":1000,"total_cost_usd":0.1,"modelUsage":{}}
"#,
        )
        .unwrap();

        handle_log(&path, &jsonl_path).unwrap();
        handle_log(&path, &jsonl_path).unwrap();

        let conn = db::open_or_create(&path).unwrap();
        let obs = db::recent_observations(&conn, 10).unwrap();
        assert_eq!(obs.len(), 2);
        // Sessions should be 1 and 2
        let sessions: Vec<i64> = obs.iter().map(|o| o.session).collect();
        assert!(sessions.contains(&1));
        assert!(sessions.contains(&2));
    }

    #[test]
    fn log_file_not_found() {
        let (_dir, path) = test_db_path();
        let result = handle_log(&path, Path::new("/nonexistent/file.jsonl"));
        assert!(result.is_err());
    }

    // ── Targets tests ──────────────────────────────────────────────────

    fn make_targets_config(
        rules: Vec<crate::config::TargetRule>,
    ) -> crate::config::MetricsTargetsConfig {
        crate::config::MetricsTargetsConfig {
            rules,
            streak_threshold: 3,
        }
    }

    fn make_rule(
        kind: &str,
        compare: &str,
        threshold: f64,
        direction: &str,
        label: &str,
    ) -> crate::config::TargetRule {
        crate::config::TargetRule {
            kind: kind.to_string(),
            compare: compare.to_string(),
            relative_to: None,
            threshold,
            direction: direction.to_string(),
            label: label.to_string(),
            unit: None,
        }
    }

    fn make_pct_of_rule(
        kind: &str,
        relative_to: &str,
        threshold: f64,
        direction: &str,
        label: &str,
    ) -> crate::config::TargetRule {
        crate::config::TargetRule {
            kind: kind.to_string(),
            compare: "pct_of".to_string(),
            relative_to: Some(relative_to.to_string()),
            threshold,
            direction: direction.to_string(),
            label: label.to_string(),
            unit: Some("%".to_string()),
        }
    }

    #[test]
    fn targets_no_rules_configured() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("blacksmith.db");
        let config = make_targets_config(vec![]);
        handle_targets(&path, 10, &config).unwrap();
    }

    #[test]
    fn targets_no_database() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("nonexistent.db");
        let config = make_targets_config(vec![make_rule(
            "turns.total",
            "avg",
            80.0,
            "below",
            "Avg turns",
        )]);
        handle_targets(&path, 10, &config).unwrap();
    }

    #[test]
    fn targets_empty_database() {
        let (_dir, path) = test_db_path();
        db::open_or_create(&path).unwrap();
        let config = make_targets_config(vec![make_rule(
            "turns.total",
            "avg",
            80.0,
            "below",
            "Avg turns",
        )]);
        handle_targets(&path, 10, &config).unwrap();
    }

    #[test]
    fn targets_avg_below_pass() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();

        // Two sessions: turns 60 and 70 → avg 65, threshold 80 below → pass
        db::upsert_observation(
            &conn,
            1,
            "2026-01-15T10:00:00Z",
            None,
            None,
            r#"{"turns.total": 60}"#,
        )
        .unwrap();
        db::upsert_observation(
            &conn,
            2,
            "2026-01-15T11:00:00Z",
            None,
            None,
            r#"{"turns.total": 70}"#,
        )
        .unwrap();
        drop(conn);

        let config = make_targets_config(vec![make_rule(
            "turns.total",
            "avg",
            80.0,
            "below",
            "Avg turns",
        )]);
        // Should not error (pass case)
        handle_targets(&path, 10, &config).unwrap();
    }

    #[test]
    fn targets_avg_below_fail() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();

        // avg 90, threshold 80 below → fail
        db::upsert_observation(
            &conn,
            1,
            "2026-01-15T10:00:00Z",
            None,
            None,
            r#"{"turns.total": 90}"#,
        )
        .unwrap();
        db::upsert_observation(
            &conn,
            2,
            "2026-01-15T11:00:00Z",
            None,
            None,
            r#"{"turns.total": 90}"#,
        )
        .unwrap();
        drop(conn);

        let config = make_targets_config(vec![make_rule(
            "turns.total",
            "avg",
            80.0,
            "below",
            "Avg turns",
        )]);
        handle_targets(&path, 10, &config).unwrap();
    }

    #[test]
    fn targets_pct_of_pass() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();

        // narration 5 of 100 turns = 5%, threshold 20% below → pass
        db::upsert_observation(
            &conn,
            1,
            "2026-01-15T10:00:00Z",
            None,
            None,
            r#"{"turns.narration_only": 5, "turns.total": 100}"#,
        )
        .unwrap();
        drop(conn);

        let config = make_targets_config(vec![make_pct_of_rule(
            "turns.narration_only",
            "turns.total",
            20.0,
            "below",
            "Narration-only turns",
        )]);
        handle_targets(&path, 10, &config).unwrap();
    }

    #[test]
    fn targets_pct_sessions_pass() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();

        // 3 out of 4 sessions have commit.detected → 75%, threshold 70% above → pass
        db::upsert_observation(
            &conn,
            1,
            "2026-01-15T10:00:00Z",
            None,
            None,
            r#"{"commit.detected": true}"#,
        )
        .unwrap();
        db::upsert_observation(
            &conn,
            2,
            "2026-01-15T11:00:00Z",
            None,
            None,
            r#"{"commit.detected": true}"#,
        )
        .unwrap();
        db::upsert_observation(
            &conn,
            3,
            "2026-01-15T12:00:00Z",
            None,
            None,
            r#"{"commit.detected": true}"#,
        )
        .unwrap();
        db::upsert_observation(&conn, 4, "2026-01-15T13:00:00Z", None, None, r#"{}"#).unwrap();
        drop(conn);

        let config = make_targets_config(vec![{
            let mut r = make_rule(
                "commit.detected",
                "pct_sessions",
                70.0,
                "above",
                "Completion rate",
            );
            r.unit = Some("%".to_string());
            r
        }]);
        handle_targets(&path, 10, &config).unwrap();
    }

    #[test]
    fn targets_multiple_rules_mixed_results() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();

        db::upsert_observation(
            &conn,
            1,
            "2026-01-15T10:00:00Z",
            None,
            None,
            r#"{"turns.total": 90, "cost.estimate_usd": 15.0}"#,
        )
        .unwrap();
        db::upsert_observation(
            &conn,
            2,
            "2026-01-15T11:00:00Z",
            None,
            None,
            r#"{"turns.total": 85, "cost.estimate_usd": 20.0}"#,
        )
        .unwrap();
        drop(conn);

        let config = make_targets_config(vec![
            make_rule("turns.total", "avg", 80.0, "below", "Avg turns"), // avg 87.5 > 80 → MISS
            {
                let mut r = make_rule("cost.estimate_usd", "avg", 30.0, "below", "Avg cost");
                r.unit = Some("$".to_string());
                r
            }, // avg 17.50 < 30 → OK
        ]);
        handle_targets(&path, 10, &config).unwrap();
    }

    #[test]
    fn evaluate_target_avg() {
        let data: Vec<serde_json::Value> = vec![
            serde_json::json!({"turns.total": 60}),
            serde_json::json!({"turns.total": 80}),
        ];
        let rule = make_rule("turns.total", "avg", 80.0, "below", "Avg turns");
        let result = evaluate_target(&rule, &data);
        match result {
            TargetResult::Pass { actual } => assert!((actual - 70.0).abs() < 0.01),
            _ => panic!("Expected Pass"),
        }
    }

    #[test]
    fn evaluate_target_pct_of() {
        let data: Vec<serde_json::Value> = vec![
            serde_json::json!({"turns.narration_only": 10, "turns.total": 100}),
            serde_json::json!({"turns.narration_only": 20, "turns.total": 100}),
        ];
        let rule = make_pct_of_rule(
            "turns.narration_only",
            "turns.total",
            20.0,
            "below",
            "Narration",
        );
        let result = evaluate_target(&rule, &data);
        match result {
            TargetResult::Pass { actual } => assert!((actual - 15.0).abs() < 0.01),
            _ => panic!("Expected Pass, narration is 15% which is below 20%"),
        }
    }

    #[test]
    fn evaluate_target_pct_sessions() {
        let data: Vec<serde_json::Value> = vec![
            serde_json::json!({"commit.detected": true}),
            serde_json::json!({"commit.detected": true}),
            serde_json::json!({}),
        ];
        let mut rule = make_rule(
            "commit.detected",
            "pct_sessions",
            50.0,
            "above",
            "Completion",
        );
        rule.unit = Some("%".to_string());
        let result = evaluate_target(&rule, &data);
        match result {
            TargetResult::Pass { actual } => {
                assert!((actual - 66.67).abs() < 1.0); // 2/3 ≈ 66.67%
            }
            _ => panic!("Expected Pass"),
        }
    }

    #[test]
    fn evaluate_target_no_data() {
        let data: Vec<serde_json::Value> = vec![serde_json::json!({"other.metric": 42})];
        let rule = make_rule("turns.total", "avg", 80.0, "below", "Avg turns");
        let result = evaluate_target(&rule, &data);
        assert!(matches!(result, TargetResult::NoData));
    }

    #[test]
    fn evaluate_target_unknown_compare_mode() {
        let data: Vec<serde_json::Value> = vec![serde_json::json!({"turns.total": 50})];
        let rule = make_rule("turns.total", "unknown_mode", 80.0, "below", "Avg turns");
        let result = evaluate_target(&rule, &data);
        assert!(matches!(result, TargetResult::NoData));
    }

    #[test]
    fn format_target_line_dollar_unit() {
        let mut rule = make_rule("cost.estimate_usd", "avg", 30.0, "below", "Avg cost");
        rule.unit = Some("$".to_string());
        let line = format_target_line(&rule, 17.50, true);
        assert!(line.contains("$17.50"));
        assert!(line.contains("OK"));
        assert!(line.contains("<30.00"));
    }

    #[test]
    fn format_target_line_percent_unit() {
        let mut rule = make_rule(
            "turns.narration_only",
            "pct_of",
            20.0,
            "below",
            "Narration-only",
        );
        rule.unit = Some("%".to_string());
        let line = format_target_line(&rule, 15.0, true);
        assert!(line.contains("15%"));
        assert!(line.contains("OK"));
        assert!(line.contains("<20%"));
    }

    #[test]
    fn format_target_line_miss() {
        let rule = make_rule("turns.total", "avg", 80.0, "below", "Avg turns");
        let line = format_target_line(&rule, 90.0, false);
        assert!(line.contains("MISS"));
        assert!(line.contains("90.0"));
    }

    // ── Migrate tests ──────────────────────────────────────────────────

    #[test]
    fn migrate_v1_not_found() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("v2.db");
        let result = handle_migrate(&db_path, Path::new("/nonexistent/v1.db"));
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not found"));
    }

    #[test]
    fn migrate_empty_v1_database() {
        let dir = TempDir::new().unwrap();
        let v1_path = dir.path().join("v1.db");
        let v2_path = dir.path().join("v2.db");

        // Create an empty V1 database with no tables
        let v1 = rusqlite::Connection::open(&v1_path).unwrap();
        v1.execute_batch("CREATE TABLE other (id INTEGER PRIMARY KEY)")
            .unwrap();
        drop(v1);

        // Should succeed but report no data
        handle_migrate(&v2_path, &v1_path).unwrap();
    }

    #[test]
    fn migrate_sessions_basic() {
        let dir = TempDir::new().unwrap();
        let v1_path = dir.path().join("v1.db");
        let v2_path = dir.path().join("v2.db");

        // Create V1 database with sessions table
        let v1 = rusqlite::Connection::open(&v1_path).unwrap();
        v1.execute_batch(
            "CREATE TABLE sessions (
                id INTEGER PRIMARY KEY,
                timestamp TEXT,
                assistant_turns INTEGER,
                cost_usd REAL,
                duration_secs INTEGER,
                input_tokens INTEGER,
                output_tokens INTEGER,
                cache_read_tokens INTEGER,
                cache_creation_tokens INTEGER,
                num_tool_calls INTEGER,
                narration_turns INTEGER,
                parallel_turns INTEGER,
                output_bytes INTEGER,
                exit_code INTEGER
            )",
        )
        .unwrap();
        v1.execute(
            "INSERT INTO sessions VALUES (1, '2026-01-10T10:00:00Z', 42, 1.5, 120, 10000, 5000, 200, 100, 80, 3, 5, 50000, 0)",
            [],
        ).unwrap();
        v1.execute(
            "INSERT INTO sessions VALUES (2, '2026-01-11T10:00:00Z', 30, 0.8, 90, 8000, 3000, 100, 50, 60, 2, 4, 30000, 0)",
            [],
        ).unwrap();
        drop(v1);

        handle_migrate(&v2_path, &v1_path).unwrap();

        // Verify V2 database
        let v2 = db::open_or_create(&v2_path).unwrap();

        // Check observations
        let obs1 = db::get_observation(&v2, 1).unwrap().unwrap();
        let data: serde_json::Value = serde_json::from_str(&obs1.data).unwrap();
        assert_eq!(data["turns.total"], 42);
        assert_eq!(data["turns.narration_only"], 3);
        assert_eq!(data["turns.parallel"], 5);
        assert_eq!(data["turns.tool_calls"], 80);
        assert_eq!(data["cost.input_tokens"], 10000);
        assert_eq!(data["cost.output_tokens"], 5000);
        assert_eq!(data["cost.estimate_usd"], 1.5);
        assert_eq!(data["session.duration_ms"], 120000);
        assert_eq!(data["session.output_bytes"], 50000);
        assert_eq!(data["session.exit_code"], 0);
        assert_eq!(obs1.duration, Some(120));

        let obs2 = db::get_observation(&v2, 2).unwrap().unwrap();
        let data2: serde_json::Value = serde_json::from_str(&obs2.data).unwrap();
        assert_eq!(data2["turns.total"], 30);
        assert_eq!(data2["cost.estimate_usd"], 0.8);

        // Check events
        let events = db::events_by_session(&v2, 1).unwrap();
        // Should have: turns.total, turns.narration_only, turns.parallel, turns.tool_calls,
        // cost.input_tokens, cost.output_tokens, cost.cache_read_tokens, cost.cache_creation_tokens,
        // cost.estimate_usd, session.output_bytes, session.duration_ms, session.exit_code = 12
        assert_eq!(events.len(), 12);
    }

    #[test]
    fn migrate_sessions_partial_columns() {
        let dir = TempDir::new().unwrap();
        let v1_path = dir.path().join("v1.db");
        let v2_path = dir.path().join("v2.db");

        // V1 database with only some columns
        let v1 = rusqlite::Connection::open(&v1_path).unwrap();
        v1.execute_batch(
            "CREATE TABLE sessions (
                id INTEGER PRIMARY KEY,
                assistant_turns INTEGER,
                cost_usd REAL
            )",
        )
        .unwrap();
        v1.execute("INSERT INTO sessions VALUES (1, 42, 1.5)", [])
            .unwrap();
        drop(v1);

        handle_migrate(&v2_path, &v1_path).unwrap();

        let v2 = db::open_or_create(&v2_path).unwrap();
        let obs = db::get_observation(&v2, 1).unwrap().unwrap();
        let data: serde_json::Value = serde_json::from_str(&obs.data).unwrap();
        assert_eq!(data["turns.total"], 42);
        assert_eq!(data["cost.estimate_usd"], 1.5);
        // Missing columns should default to 0
        assert_eq!(data["turns.narration_only"], 0);
        assert_eq!(data["turns.parallel"], 0);
    }

    #[test]
    fn migrate_improvements_with_severity() {
        let dir = TempDir::new().unwrap();
        let v1_path = dir.path().join("v1.db");
        let v2_path = dir.path().join("v2.db");

        let v1 = rusqlite::Connection::open(&v1_path).unwrap();
        v1.execute_batch(
            "CREATE TABLE improvements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ref TEXT,
                severity TEXT,
                title TEXT,
                body TEXT,
                context TEXT,
                tags TEXT,
                status TEXT DEFAULT 'open',
                created TEXT
            )",
        )
        .unwrap();
        v1.execute(
            "INSERT INTO improvements (ref, severity, title, body) VALUES ('R1', 'critical', 'Fix retry logic', 'Details here')",
            [],
        ).unwrap();
        v1.execute(
            "INSERT INTO improvements (ref, severity, title) VALUES ('R2', 'low', 'Clean up imports')",
            [],
        ).unwrap();
        v1.execute(
            "INSERT INTO improvements (ref, severity, title) VALUES ('R3', 'medium', 'Better logging')",
            [],
        ).unwrap();
        drop(v1);

        handle_migrate(&v2_path, &v1_path).unwrap();

        let v2 = db::open_or_create(&v2_path).unwrap();
        let imps = db::list_improvements(&v2, None, None).unwrap();
        assert_eq!(imps.len(), 3);

        // Check severity -> category mapping
        let r1 = imps.iter().find(|i| i.title == "Fix retry logic").unwrap();
        assert_eq!(r1.category, "reliability"); // critical -> reliability
        assert_eq!(r1.body.as_deref(), Some("Details here"));

        let r2 = imps.iter().find(|i| i.title == "Clean up imports").unwrap();
        assert_eq!(r2.category, "code-quality"); // low -> code-quality

        let r3 = imps.iter().find(|i| i.title == "Better logging").unwrap();
        assert_eq!(r3.category, "workflow"); // medium -> workflow
    }

    #[test]
    fn migrate_improvements_with_category() {
        let dir = TempDir::new().unwrap();
        let v1_path = dir.path().join("v1.db");
        let v2_path = dir.path().join("v2.db");

        // V1 database already has category (not severity)
        let v1 = rusqlite::Connection::open(&v1_path).unwrap();
        v1.execute_batch(
            "CREATE TABLE improvements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category TEXT,
                title TEXT
            )",
        )
        .unwrap();
        v1.execute(
            "INSERT INTO improvements (category, title) VALUES ('cost', 'Reduce tokens')",
            [],
        )
        .unwrap();
        drop(v1);

        handle_migrate(&v2_path, &v1_path).unwrap();

        let v2 = db::open_or_create(&v2_path).unwrap();
        let imps = db::list_improvements(&v2, None, None).unwrap();
        assert_eq!(imps.len(), 1);
        assert_eq!(imps[0].category, "cost");
    }

    #[test]
    fn migrate_both_sessions_and_improvements() {
        let dir = TempDir::new().unwrap();
        let v1_path = dir.path().join("v1.db");
        let v2_path = dir.path().join("v2.db");

        let v1 = rusqlite::Connection::open(&v1_path).unwrap();
        v1.execute_batch(
            "CREATE TABLE sessions (
                id INTEGER PRIMARY KEY,
                assistant_turns INTEGER,
                cost_usd REAL,
                duration_secs INTEGER
            );
            CREATE TABLE improvements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                severity TEXT,
                title TEXT
            )",
        )
        .unwrap();
        v1.execute("INSERT INTO sessions VALUES (1, 50, 2.0, 60)", [])
            .unwrap();
        v1.execute(
            "INSERT INTO improvements (severity, title) VALUES ('high', 'Improve throughput')",
            [],
        )
        .unwrap();
        drop(v1);

        handle_migrate(&v2_path, &v1_path).unwrap();

        let v2 = db::open_or_create(&v2_path).unwrap();
        let obs = db::recent_observations(&v2, 10).unwrap();
        assert_eq!(obs.len(), 1);
        let imps = db::list_improvements(&v2, None, None).unwrap();
        assert_eq!(imps.len(), 1);
        assert_eq!(imps[0].category, "reliability"); // high -> reliability
    }

    #[test]
    fn map_severity_to_category_mappings() {
        assert_eq!(map_severity_to_category("critical"), "reliability");
        assert_eq!(map_severity_to_category("high"), "reliability");
        assert_eq!(map_severity_to_category("medium"), "workflow");
        assert_eq!(map_severity_to_category("normal"), "workflow");
        assert_eq!(map_severity_to_category("low"), "code-quality");
        assert_eq!(map_severity_to_category("minor"), "code-quality");
        assert_eq!(map_severity_to_category("cost"), "cost");
        assert_eq!(map_severity_to_category("performance"), "performance");
        assert_eq!(map_severity_to_category("unknown"), "unknown");
    }

    #[test]
    fn migrate_sessions_no_exit_code() {
        let dir = TempDir::new().unwrap();
        let v1_path = dir.path().join("v1.db");
        let v2_path = dir.path().join("v2.db");

        let v1 = rusqlite::Connection::open(&v1_path).unwrap();
        v1.execute_batch(
            "CREATE TABLE sessions (
                id INTEGER PRIMARY KEY,
                assistant_turns INTEGER,
                duration_secs INTEGER
            )",
        )
        .unwrap();
        v1.execute("INSERT INTO sessions VALUES (1, 20, 60)", [])
            .unwrap();
        drop(v1);

        handle_migrate(&v2_path, &v1_path).unwrap();

        let v2 = db::open_or_create(&v2_path).unwrap();
        let events = db::events_by_session(&v2, 1).unwrap();
        // Should NOT have session.exit_code event when column doesn't exist
        assert!(events.iter().all(|e| e.kind != "session.exit_code"));
    }

    #[test]
    fn targets_config_from_toml() {
        let toml_str = r#"
[[targets.rules]]
kind = "turns.narration_only"
compare = "pct_of"
relative_to = "turns.total"
threshold = 20
direction = "below"
label = "Narration-only turns"
unit = "%"

[[targets.rules]]
kind = "turns.total"
compare = "avg"
threshold = 80
direction = "below"
label = "Avg turns per session"
"#;
        let config: crate::config::MetricsConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.targets.rules.len(), 2);

        let r0 = &config.targets.rules[0];
        assert_eq!(r0.kind, "turns.narration_only");
        assert_eq!(r0.compare, "pct_of");
        assert_eq!(r0.relative_to, Some("turns.total".to_string()));
        assert_eq!(r0.threshold, 20.0);
        assert_eq!(r0.direction, "below");
        assert_eq!(r0.unit, Some("%".to_string()));

        let r1 = &config.targets.rules[1];
        assert_eq!(r1.kind, "turns.total");
        assert_eq!(r1.compare, "avg");
        assert!(r1.relative_to.is_none());
        assert_eq!(r1.threshold, 80.0);
        assert_eq!(r1.direction, "below");
        assert!(r1.unit.is_none());
    }
}
