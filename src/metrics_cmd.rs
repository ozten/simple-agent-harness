use crate::adapters;
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

    // Use Claude adapter by default for `metrics log` (most common format)
    let adapter = adapters::claude::ClaudeAdapter::new();
    let result = ingest::ingest_session(&conn, session, file, None, &adapter)
        .map_err(|e| format!("Ingestion failed: {e}"))?;

    println!("Ingested session {session} from {}", file.display());
    println!(
        "  turns: {}  cost: ${:.2}  duration: {}s",
        result.turns_total,
        result.cost_estimate_usd,
        result.session_duration_secs as u64
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
        let duration_s = data["session.duration_secs"].as_f64().unwrap_or(0.0) as u64;

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
    adapter_name: &str,
    supported_metrics: &[&str],
) -> Result<(), String> {
    if targets_config.rules.is_empty() {
        println!("No target rules configured in [metrics.targets.rules].");
        println!("Add rules to your .blacksmith/config.toml to track performance targets.");
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
    let mut na_count = 0;

    println!(
        "Targets (evaluated over last {} session{}):\n",
        session_count,
        if session_count == 1 { "" } else { "s" }
    );

    for rule in &targets_config.rules {
        // Check if the metric is supported by the current adapter
        if !is_metric_available(rule, supported_metrics) {
            na_count += 1;
            println!(
                "  {:<30} N/A (not available from {} adapter)",
                rule.label, adapter_name
            );
            continue;
        }

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
    if total_rules == 0 && na_count > 0 {
        println!("No targets available for the {adapter_name} adapter ({na_count} skipped).");
    } else if pass_count == total_rules {
        if na_count > 0 {
            println!("All {total_rules} target(s) met ({na_count} not available).");
        } else {
            println!("All {total_rules} target(s) met.");
        }
    } else {
        let missed = total_rules - pass_count;
        if na_count > 0 {
            println!("{pass_count}/{total_rules} target(s) met, {missed} missed ({na_count} not available).");
        } else {
            println!("{pass_count}/{total_rules} target(s) met, {missed} missed.",);
        }
    }

    Ok(())
}

/// Check if a target rule's metrics are supported by the adapter.
fn is_metric_available(rule: &crate::config::TargetRule, supported_metrics: &[&str]) -> bool {
    if !supported_metrics.contains(&rule.kind.as_str()) {
        return false;
    }
    if let Some(ref rel) = rule.relative_to {
        if !supported_metrics.contains(&rel.as_str()) {
            return false;
        }
    }
    true
}

/// Handle the `metrics reingest` subcommand.
///
/// Re-ingests historical session files through the adapter, updating events and
/// observations. Decompresses `.jsonl.zst` files as needed (to a temp file).
/// Supports `--last N` (reingest N most recent) and `--all` (reingest everything).
pub fn handle_reingest(
    db_path: &Path,
    sessions_dir: &Path,
    last: Option<u64>,
    all: bool,
    rules: &[crate::config::CompiledRule],
    adapter: &dyn crate::adapters::AgentAdapter,
) -> Result<(), String> {
    if !all && last.is_none() {
        return Err("Specify --last N or --all".to_string());
    }

    if !sessions_dir.exists() {
        return Err(format!(
            "Sessions directory not found: {}",
            sessions_dir.display()
        ));
    }

    // List all session files (both .jsonl and .jsonl.zst)
    let mut session_files = list_session_files(sessions_dir);
    if session_files.is_empty() {
        println!("No session files found.");
        return Ok(());
    }

    // Sort by iteration number ascending
    session_files.sort_by_key(|(iter, _)| *iter);

    // Apply --last N filter
    if let Some(n) = last {
        let n = n as usize;
        if n < session_files.len() {
            session_files = session_files.split_off(session_files.len() - n);
        }
    }

    let conn = db::open_or_create(db_path).map_err(|e| format!("Failed to open database: {e}"))?;

    let total = session_files.len();
    let mut success_count = 0u64;
    let mut error_count = 0u64;

    for (iteration, path) in &session_files {
        let session = *iteration as i64;

        // Decompress if needed
        let (jsonl_path, _temp_file) = match decompress_if_needed(path) {
            Ok(result) => result,
            Err(e) => {
                eprintln!("  session {session}: decompress error: {e}");
                error_count += 1;
                continue;
            }
        };

        // Delete old events for this session
        if let Err(e) = db::delete_events_by_session(&conn, session) {
            eprintln!("  session {session}: failed to clear old events: {e}");
            error_count += 1;
            continue;
        }

        // Re-ingest
        match crate::ingest::ingest_session_with_rules(
            &conn,
            session,
            &jsonl_path,
            None,
            rules,
            adapter,
        ) {
            Ok(result) => {
                success_count += 1;
                println!(
                    "  session {session}: turns={} cost=${:.2} duration={}s",
                    result.turns_total,
                    result.cost_estimate_usd,
                    result.session_duration_secs as u64
                );
            }
            Err(e) => {
                eprintln!("  session {session}: ingest error: {e}");
                error_count += 1;
            }
        }
    }

    println!(
        "\nReingested {success_count}/{total} session(s){}",
        if error_count > 0 {
            format!(" ({error_count} error(s))")
        } else {
            String::new()
        }
    );

    Ok(())
}

/// List all session files (.jsonl and .jsonl.zst) in the sessions directory.
/// Returns (iteration_number, path) pairs.
fn list_session_files(sessions_dir: &Path) -> Vec<(u64, std::path::PathBuf)> {
    let entries = match std::fs::read_dir(sessions_dir) {
        Ok(e) => e,
        Err(_) => return vec![],
    };

    let mut result = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if let Some(iteration) = parse_session_iteration(&path) {
            result.push((iteration, path));
        }
    }
    result
}

/// Extract the iteration number from a session filename like "42.jsonl" or "42.jsonl.zst".
fn parse_session_iteration(path: &Path) -> Option<u64> {
    let file_name = path.file_name()?.to_str()?;
    let stem = file_name
        .strip_suffix(".jsonl.zst")
        .or_else(|| file_name.strip_suffix(".jsonl"))?;
    stem.parse().ok()
}

/// If the path is a .jsonl.zst file, decompress to a temp file and return its path.
/// If it's already .jsonl, return the path directly.
/// Returns (path_to_use, optional_temp_file_to_keep_alive).
fn decompress_if_needed(
    path: &Path,
) -> Result<(std::path::PathBuf, Option<tempfile::NamedTempFile>), String> {
    let file_name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_default();

    if file_name.ends_with(".jsonl.zst") {
        let compressed =
            std::fs::read(path).map_err(|e| format!("Failed to read {}: {e}", path.display()))?;
        let decompressed = zstd::decode_all(compressed.as_slice())
            .map_err(|e| format!("Failed to decompress {}: {e}", path.display()))?;

        let mut temp = tempfile::NamedTempFile::new()
            .map_err(|e| format!("Failed to create temp file: {e}"))?;
        std::io::Write::write_all(&mut temp, &decompressed)
            .map_err(|e| format!("Failed to write temp file: {e}"))?;

        let temp_path = temp.path().to_path_buf();
        Ok((temp_path, Some(temp)))
    } else {
        Ok((path.to_path_buf(), None))
    }
}

/// Handle the `metrics rebuild` subcommand.
///
/// Drops all observations and regenerates them from the events table.
/// Useful after manual event edits or migration.
pub fn handle_rebuild(db_path: &Path) -> Result<(), String> {
    if !db_path.exists() {
        println!("No metrics database found. Nothing to rebuild.");
        return Ok(());
    }

    let conn = db::open_or_create(db_path).map_err(|e| format!("Failed to open database: {e}"))?;

    let count = db::rebuild_observations(&conn)
        .map_err(|e| format!("Failed to rebuild observations: {e}"))?;

    println!("Rebuilt {count} observation(s) from events.");

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
///   duration_secs -> session.duration_secs
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
            ("session.duration_secs", duration_secs.to_string()),
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
        map.insert("session.duration_secs".into(), serde_json::json!(duration_secs));
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

/// Handle the `metrics export [--format csv|json]` subcommand.
///
/// Exports all observations. JSON format outputs a JSON array of observation objects.
/// CSV format outputs a header row followed by one row per observation with flattened data.
pub fn handle_export(db_path: &Path, format: &str) -> Result<(), String> {
    if !db_path.exists() {
        println!("No metrics database found. Run some sessions first.");
        return Ok(());
    }

    let conn = db::open_or_create(db_path).map_err(|e| format!("Failed to open database: {e}"))?;
    let observations =
        db::all_observations(&conn).map_err(|e| format!("Failed to query observations: {e}"))?;

    if observations.is_empty() {
        println!("No observations to export.");
        return Ok(());
    }

    match format {
        "json" => {
            let mut items: Vec<serde_json::Value> = Vec::new();
            for obs in &observations {
                let data: serde_json::Value = serde_json::from_str(&obs.data).unwrap_or_default();
                let mut obj = serde_json::Map::new();
                obj.insert("session".into(), serde_json::json!(obs.session));
                obj.insert("ts".into(), serde_json::json!(obs.ts));
                obj.insert(
                    "duration".into(),
                    obs.duration
                        .map(serde_json::Value::from)
                        .unwrap_or(serde_json::Value::Null),
                );
                obj.insert(
                    "outcome".into(),
                    obs.outcome
                        .as_ref()
                        .map(|s| serde_json::Value::String(s.clone()))
                        .unwrap_or(serde_json::Value::Null),
                );
                obj.insert("data".into(), data);
                items.push(serde_json::Value::Object(obj));
            }
            let output = serde_json::to_string_pretty(&items)
                .map_err(|e| format!("JSON serialization failed: {e}"))?;
            println!("{output}");
        }
        "csv" => {
            // Collect all unique data keys across observations
            let mut all_keys: Vec<String> = Vec::new();
            let parsed: Vec<serde_json::Value> = observations
                .iter()
                .map(|obs| serde_json::from_str(&obs.data).unwrap_or_default())
                .collect();

            for data in &parsed {
                if let Some(obj) = data.as_object() {
                    for key in obj.keys() {
                        if !all_keys.contains(key) {
                            all_keys.push(key.clone());
                        }
                    }
                }
            }
            all_keys.sort();

            // Print header
            let mut header = vec![
                "session".to_string(),
                "ts".to_string(),
                "duration".to_string(),
                "outcome".to_string(),
            ];
            header.extend(all_keys.iter().cloned());
            println!("{}", header.join(","));

            // Print rows
            for (obs, data) in observations.iter().zip(parsed.iter()) {
                let mut row = vec![
                    obs.session.to_string(),
                    obs.ts.clone(),
                    obs.duration.map(|d| d.to_string()).unwrap_or_default(),
                    obs.outcome.clone().unwrap_or_default(),
                ];
                for key in &all_keys {
                    let val = data.get(key);
                    let cell = match val {
                        Some(serde_json::Value::Number(n)) => n.to_string(),
                        Some(serde_json::Value::Bool(b)) => b.to_string(),
                        Some(serde_json::Value::String(s)) => s.clone(),
                        Some(serde_json::Value::Null) | None => String::new(),
                        Some(v) => v.to_string(),
                    };
                    row.push(cell);
                }
                println!("{}", row.join(","));
            }
        }
        other => {
            return Err(format!(
                "Unknown export format '{other}'. Use 'json' or 'csv'."
            ));
        }
    }

    Ok(())
}

/// Handle the `metrics query <kind> [--last N] [--aggregate avg|trend]` subcommand.
///
/// Queries event values for a specific kind. With --aggregate avg, computes the average.
/// With --aggregate trend, shows recent values to reveal direction.
pub fn handle_query(
    db_path: &Path,
    kind: &str,
    last: Option<i64>,
    aggregate: Option<&str>,
) -> Result<(), String> {
    if !db_path.exists() {
        println!("No metrics database found. Run some sessions first.");
        return Ok(());
    }

    let conn = db::open_or_create(db_path).map_err(|e| format!("Failed to open database: {e}"))?;
    let events = db::events_by_kind_last(&conn, kind, last)
        .map_err(|e| format!("Failed to query events: {e}"))?;

    if events.is_empty() {
        println!("No events found for kind '{kind}'.");
        return Ok(());
    }

    match aggregate.unwrap_or("list") {
        "avg" => {
            let values: Vec<f64> = events
                .iter()
                .filter_map(|e| e.value.as_ref())
                .filter_map(|v| v.parse::<f64>().ok())
                .collect();

            if values.is_empty() {
                println!("No numeric values found for kind '{kind}'.");
                return Ok(());
            }

            let avg = values.iter().sum::<f64>() / values.len() as f64;
            println!("{kind} avg: {avg:.2} (over {} value(s))", values.len());
        }
        "trend" => {
            println!("{:<8} {:<22} {:>12}", "SESSION", "TIMESTAMP", "VALUE");
            println!("{}", "-".repeat(44));
            for event in &events {
                let val = event.value.as_deref().unwrap_or("(none)");
                let date = if event.ts.len() >= 19 {
                    &event.ts[..19]
                } else {
                    &event.ts
                };
                println!("{:<8} {:<22} {:>12}", event.session, date, val);
            }

            // Show direction indicator
            let values: Vec<f64> = events
                .iter()
                .filter_map(|e| e.value.as_ref())
                .filter_map(|v| v.parse::<f64>().ok())
                .collect();

            if values.len() >= 2 {
                let first_half_avg =
                    values[..values.len() / 2].iter().sum::<f64>() / (values.len() / 2) as f64;
                let second_half_avg = values[values.len() / 2..].iter().sum::<f64>()
                    / (values.len() - values.len() / 2) as f64;

                let direction = if second_half_avg > first_half_avg * 1.05 {
                    "trending UP"
                } else if second_half_avg < first_half_avg * 0.95 {
                    "trending DOWN"
                } else {
                    "STABLE"
                };
                println!(
                    "\nTrend: {direction} ({:.2} -> {:.2})",
                    first_half_avg, second_half_avg
                );
            }
        }
        "list" => {
            println!("{:<8} {:<22} {:>12}", "SESSION", "TIMESTAMP", "VALUE");
            println!("{}", "-".repeat(44));
            for event in &events {
                let val = event.value.as_deref().unwrap_or("(none)");
                let date = if event.ts.len() >= 19 {
                    &event.ts[..19]
                } else {
                    &event.ts
                };
                println!("{:<8} {:<22} {:>12}", event.session, date, val);
            }
            println!("\n{} event(s)", events.len());
        }
        other => {
            return Err(format!(
                "Unknown aggregate mode '{other}'. Use 'avg' or 'trend'."
            ));
        }
    }

    Ok(())
}

/// Handle the `metrics events [--session N]` subcommand.
///
/// Dumps raw events, optionally filtered to a single session.
pub fn handle_events(db_path: &Path, session: Option<i64>) -> Result<(), String> {
    if !db_path.exists() {
        println!("No metrics database found. Run some sessions first.");
        return Ok(());
    }

    let conn = db::open_or_create(db_path).map_err(|e| format!("Failed to open database: {e}"))?;
    let events =
        db::all_events(&conn, session).map_err(|e| format!("Failed to query events: {e}"))?;

    if events.is_empty() {
        if let Some(s) = session {
            println!("No events found for session {s}.");
        } else {
            println!("No events in database.");
        }
        return Ok(());
    }

    println!(
        "{:<6} {:<8} {:<22} {:<28} {:<20} TAGS",
        "ID", "SESSION", "TIMESTAMP", "KIND", "VALUE"
    );
    println!("{}", "-".repeat(90));

    for event in &events {
        let val = event.value.as_deref().unwrap_or("");
        let tags = event.tags.as_deref().unwrap_or("");
        let date = if event.ts.len() >= 19 {
            &event.ts[..19]
        } else {
            &event.ts
        };
        println!(
            "{:<6} {:<8} {:<22} {:<28} {:<20} {}",
            event.id, event.session, date, event.kind, val, tags
        );
    }

    println!("\n{} event(s)", events.len());

    Ok(())
}

/// Handle the `metrics beads` subcommand.
///
/// Displays a per-bead timing report from the bead_metrics table.
/// Shows each bead with its ID, status, sessions, wall time, turns, and title.
/// Includes totals, averages, and fastest/slowest beads.
pub fn handle_beads(db_path: &Path) -> Result<(), String> {
    if !db_path.exists() {
        println!("No metrics database found. Run some sessions first.");
        return Ok(());
    }

    let conn = db::open_or_create(db_path).map_err(|e| format!("Failed to open database: {e}"))?;

    let mut all =
        db::all_bead_metrics(&conn).map_err(|e| format!("Failed to query bead metrics: {e}"))?;

    if all.is_empty() {
        println!("No bead metrics recorded yet.");
        return Ok(());
    }

    // Reconcile stale completed_at values with bd
    let reconciled = reconcile_bead_status(&conn, &all);
    if reconciled > 0 {
        all = db::all_bead_metrics(&conn)
            .map_err(|e| format!("Failed to re-query bead metrics: {e}"))?;
    }

    // Print header
    println!(
        "{:<28} {:<10} {:>8} {:>10} {:>6}",
        "BEAD", "STATUS", "SESSIONS", "WALL TIME", "TURNS"
    );
    println!("{}", "-".repeat(66));

    let mut closed_count = 0u64;
    let mut open_count = 0u64;
    let mut total_wall_secs: f64 = 0.0;
    let mut total_turns: i64 = 0;
    let mut total_sessions: i64 = 0;
    let mut fastest: Option<(&str, f64)> = None;
    let mut slowest: Option<(&str, f64)> = None;

    for bm in &all {
        let status = if bm.completed_at.is_some() {
            closed_count += 1;
            "closed"
        } else {
            open_count += 1;
            "open"
        };

        total_wall_secs += bm.wall_time_secs;
        total_turns += bm.total_turns;
        total_sessions += bm.sessions;

        // Track fastest/slowest among completed beads
        if bm.completed_at.is_some() && bm.wall_time_secs > 0.0 {
            match fastest {
                None => fastest = Some((&bm.bead_id, bm.wall_time_secs)),
                Some((_, t)) if bm.wall_time_secs < t => {
                    fastest = Some((&bm.bead_id, bm.wall_time_secs));
                }
                _ => {}
            }
            match slowest {
                None => slowest = Some((&bm.bead_id, bm.wall_time_secs)),
                Some((_, t)) if bm.wall_time_secs > t => {
                    slowest = Some((&bm.bead_id, bm.wall_time_secs));
                }
                _ => {}
            }
        }

        let wall_str = format_duration(bm.wall_time_secs as u64);

        println!(
            "{:<28} {:<10} {:>8} {:>10} {:>6}",
            truncate_bead_id(&bm.bead_id, 27),
            status,
            bm.sessions,
            wall_str,
            bm.total_turns
        );
    }

    // Totals
    println!("{}", "-".repeat(66));
    let closed_wall_secs: f64 = all
        .iter()
        .filter(|b| b.completed_at.is_some())
        .map(|b| b.wall_time_secs)
        .sum();
    println!(
        "{} closed ({}), {} open",
        closed_count,
        format_duration(closed_wall_secs as u64),
        open_count
    );

    // Averages (over completed beads only)
    if closed_count > 0 {
        let avg_wall = closed_wall_secs / closed_count as f64;
        let closed_turns: i64 = all
            .iter()
            .filter(|b| b.completed_at.is_some())
            .map(|b| b.total_turns)
            .sum();
        let avg_turns = closed_turns as f64 / closed_count as f64;
        println!(
            "Avg: {}/bead, {:.0} turns/bead",
            format_duration(avg_wall as u64),
            avg_turns
        );
    }

    // Fastest / slowest
    if let Some((id, secs)) = fastest {
        println!("Fastest: {} ({})", id, format_duration(secs as u64));
    }
    if let Some((id, secs)) = slowest {
        // Only show slowest if different from fastest
        if fastest.map(|(fid, _)| fid) != Some(id) {
            println!("Slowest: {} ({})", id, format_duration(secs as u64));
        }
    }

    // Overall totals
    if total_sessions > 0 {
        println!(
            "\nTotal: {} session(s), {}, {} turns",
            total_sessions,
            format_duration(total_wall_secs as u64),
            total_turns
        );
    }

    Ok(())
}

/// Truncate a bead ID for display, adding "..." if it exceeds max_len.
fn truncate_bead_id(id: &str, max_len: usize) -> String {
    if id.len() <= max_len {
        id.to_string()
    } else {
        format!("{}...", &id[..max_len - 3])
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

/// Parse bead IDs from JSON output of `bd list --status=closed --json`.
fn parse_closed_bead_ids(json_str: &str) -> Vec<String> {
    let parsed: Result<Vec<serde_json::Value>, _> = serde_json::from_str(json_str);
    match parsed {
        Ok(beads) => beads
            .iter()
            .filter_map(|b| b.get("id").and_then(|v| v.as_str()).map(|s| s.to_string()))
            .collect(),
        Err(_) => Vec::new(),
    }
}

/// Reconcile bead status by cross-referencing with `bd list --status=closed --json`.
///
/// For any bead in `all` that has `completed_at == None`, checks if `bd` reports it
/// as closed and calls `mark_bead_completed()` to set the timestamp. Returns the
/// number of beads updated. Gracefully returns 0 if `bd` is not available.
fn reconcile_bead_status(conn: &Connection, all: &[db::BeadMetrics]) -> usize {
    let stale: Vec<&str> = all
        .iter()
        .filter(|bm| bm.completed_at.is_none())
        .map(|bm| bm.bead_id.as_str())
        .collect();

    if stale.is_empty() {
        return 0;
    }

    let closed_ids = match std::process::Command::new("bd")
        .args(["list", "--status=closed", "--json"])
        .output()
    {
        Ok(output) if output.status.success() => {
            parse_closed_bead_ids(&String::from_utf8_lossy(&output.stdout))
        }
        _ => return 0,
    };

    let mut updated = 0;
    for id in &closed_ids {
        if stale.contains(&id.as_str()) {
            if let Ok(true) = db::mark_bead_completed(conn, id) {
                updated += 1;
            }
        }
    }
    updated
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;
    use tempfile::TempDir;

    /// All metrics supported by the Claude adapter, used in tests.
    const ALL_METRICS: &[&str] = &[
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
        "commit.detected",
    ];

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

        let data = r#"{"turns.total":42,"turns.narration_only":3,"turns.parallel":5,"cost.estimate_usd":1.5,"session.duration_secs":120}"#;
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
                r#"{{"turns.total":{},"turns.narration_only":0,"turns.parallel":0,"cost.estimate_usd":0.5,"session.duration_secs":60}}"#,
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
        handle_targets(&path, 10, &config, "claude", ALL_METRICS).unwrap();
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
        handle_targets(&path, 10, &config, "claude", ALL_METRICS).unwrap();
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
        handle_targets(&path, 10, &config, "claude", ALL_METRICS).unwrap();
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
        handle_targets(&path, 10, &config, "claude", ALL_METRICS).unwrap();
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
        handle_targets(&path, 10, &config, "claude", ALL_METRICS).unwrap();
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
        handle_targets(&path, 10, &config, "claude", ALL_METRICS).unwrap();
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
        handle_targets(&path, 10, &config, "claude", ALL_METRICS).unwrap();
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
        handle_targets(&path, 10, &config, "claude", ALL_METRICS).unwrap();
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
        assert_eq!(data["session.duration_secs"], 120);
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
        // cost.estimate_usd, session.output_bytes, session.duration_secs, session.exit_code = 12
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

    // ── Rebuild tests ─────────────────────────────────────────────────

    #[test]
    fn rebuild_no_database() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("nonexistent.db");
        handle_rebuild(&path).unwrap();
    }

    #[test]
    fn rebuild_empty_database() {
        let (_dir, path) = test_db_path();
        db::open_or_create(&path).unwrap();
        handle_rebuild(&path).unwrap();
    }

    #[test]
    fn rebuild_regenerates_observations() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();

        // Insert events for two sessions
        db::insert_event_with_ts(
            &conn,
            "2026-01-15T10:00:00Z",
            1,
            "turns.total",
            Some("50"),
            None,
        )
        .unwrap();
        db::insert_event_with_ts(
            &conn,
            "2026-01-15T10:00:00Z",
            1,
            "cost.estimate_usd",
            Some("2.500000"),
            None,
        )
        .unwrap();
        db::insert_event_with_ts(
            &conn,
            "2026-01-15T10:00:00Z",
            1,
            "session.duration_secs",
            Some("60"),
            None,
        )
        .unwrap();

        db::insert_event_with_ts(
            &conn,
            "2026-01-15T11:00:00Z",
            2,
            "turns.total",
            Some("30"),
            None,
        )
        .unwrap();

        drop(conn);
        handle_rebuild(&path).unwrap();

        // Verify observations were created
        let conn = db::open_or_create(&path).unwrap();
        let obs = db::recent_observations(&conn, 10).unwrap();
        assert_eq!(obs.len(), 2);

        let obs1 = db::get_observation(&conn, 1).unwrap().unwrap();
        let data: serde_json::Value = serde_json::from_str(&obs1.data).unwrap();
        assert_eq!(data["turns.total"], 50);
        assert_eq!(data["cost.estimate_usd"], 2.5);
        assert_eq!(obs1.duration, Some(60));
    }

    #[test]
    fn rebuild_replaces_stale_observations() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();

        // Insert a stale observation with no matching events
        db::upsert_observation(
            &conn,
            99,
            "2026-01-01T00:00:00Z",
            Some(100),
            None,
            r#"{"stale": true}"#,
        )
        .unwrap();

        // Insert events for session 1
        db::insert_event_with_ts(
            &conn,
            "2026-01-15T10:00:00Z",
            1,
            "turns.total",
            Some("20"),
            None,
        )
        .unwrap();

        drop(conn);
        handle_rebuild(&path).unwrap();

        let conn = db::open_or_create(&path).unwrap();
        // Stale observation should be gone
        assert!(db::get_observation(&conn, 99).unwrap().is_none());
        // New observation should exist
        assert!(db::get_observation(&conn, 1).unwrap().is_some());
    }

    // ── Export tests ──────────────────────────────────────────────────

    #[test]
    fn export_no_database() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("nonexistent.db");
        handle_export(&path, "json").unwrap();
    }

    #[test]
    fn export_empty_database() {
        let (_dir, path) = test_db_path();
        db::open_or_create(&path).unwrap();
        handle_export(&path, "json").unwrap();
    }

    #[test]
    fn export_json_format() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();

        db::upsert_observation(
            &conn,
            1,
            "2026-02-15T10:00:00Z",
            Some(120),
            Some("completed"),
            r#"{"turns.total":42,"cost.estimate_usd":1.5}"#,
        )
        .unwrap();
        drop(conn);

        handle_export(&path, "json").unwrap();
    }

    #[test]
    fn export_csv_format() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();

        db::upsert_observation(
            &conn,
            1,
            "2026-02-15T10:00:00Z",
            Some(120),
            Some("completed"),
            r#"{"turns.total":42,"cost.estimate_usd":1.5}"#,
        )
        .unwrap();
        db::upsert_observation(
            &conn,
            2,
            "2026-02-15T11:00:00Z",
            Some(90),
            None,
            r#"{"turns.total":30}"#,
        )
        .unwrap();
        drop(conn);

        handle_export(&path, "csv").unwrap();
    }

    #[test]
    fn export_unknown_format() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();
        db::upsert_observation(&conn, 1, "2026-02-15T10:00:00Z", None, None, "{}").unwrap();
        drop(conn);

        let result = handle_export(&path, "xml");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unknown export format"));
    }

    // ── Query tests ──────────────────────────────────────────────────

    #[test]
    fn query_no_database() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("nonexistent.db");
        handle_query(&path, "turns.total", None, None).unwrap();
    }

    #[test]
    fn query_no_events() {
        let (_dir, path) = test_db_path();
        db::open_or_create(&path).unwrap();
        handle_query(&path, "turns.total", None, None).unwrap();
    }

    #[test]
    fn query_list_mode() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();

        db::insert_event_with_ts(
            &conn,
            "2026-02-15T10:00:00Z",
            1,
            "turns.total",
            Some("42"),
            None,
        )
        .unwrap();
        db::insert_event_with_ts(
            &conn,
            "2026-02-15T11:00:00Z",
            2,
            "turns.total",
            Some("55"),
            None,
        )
        .unwrap();
        drop(conn);

        handle_query(&path, "turns.total", None, None).unwrap();
    }

    #[test]
    fn query_avg_mode() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();

        db::insert_event_with_ts(
            &conn,
            "2026-02-15T10:00:00Z",
            1,
            "turns.total",
            Some("40"),
            None,
        )
        .unwrap();
        db::insert_event_with_ts(
            &conn,
            "2026-02-15T11:00:00Z",
            2,
            "turns.total",
            Some("60"),
            None,
        )
        .unwrap();
        drop(conn);

        // avg of 40 and 60 = 50
        handle_query(&path, "turns.total", None, Some("avg")).unwrap();
    }

    #[test]
    fn query_trend_mode() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();

        for i in 1..=6 {
            db::insert_event_with_ts(
                &conn,
                "2026-02-15T10:00:00Z",
                i,
                "turns.total",
                Some(&(i * 10).to_string()),
                None,
            )
            .unwrap();
        }
        drop(conn);

        handle_query(&path, "turns.total", None, Some("trend")).unwrap();
    }

    #[test]
    fn query_with_last_limit() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();

        for i in 1..=10 {
            db::insert_event_with_ts(
                &conn,
                "2026-02-15T10:00:00Z",
                i,
                "turns.total",
                Some(&(i * 10).to_string()),
                None,
            )
            .unwrap();
        }
        drop(conn);

        // Should only show last 3 sessions
        handle_query(&path, "turns.total", Some(3), None).unwrap();
    }

    #[test]
    fn query_unknown_aggregate() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();
        db::insert_event_with_ts(
            &conn,
            "2026-02-15T10:00:00Z",
            1,
            "turns.total",
            Some("42"),
            None,
        )
        .unwrap();
        drop(conn);

        let result = handle_query(&path, "turns.total", None, Some("median"));
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unknown aggregate mode"));
    }

    // ── Events dump tests ────────────────────────────────────────────

    #[test]
    fn events_no_database() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("nonexistent.db");
        handle_events(&path, None).unwrap();
    }

    #[test]
    fn events_empty_database() {
        let (_dir, path) = test_db_path();
        db::open_or_create(&path).unwrap();
        handle_events(&path, None).unwrap();
    }

    #[test]
    fn events_all_sessions() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();

        db::insert_event_with_ts(
            &conn,
            "2026-02-15T10:00:00Z",
            1,
            "turns.total",
            Some("42"),
            None,
        )
        .unwrap();
        db::insert_event_with_ts(
            &conn,
            "2026-02-15T10:00:00Z",
            1,
            "cost.estimate_usd",
            Some("1.5"),
            None,
        )
        .unwrap();
        db::insert_event_with_ts(
            &conn,
            "2026-02-15T11:00:00Z",
            2,
            "turns.total",
            Some("30"),
            None,
        )
        .unwrap();
        drop(conn);

        handle_events(&path, None).unwrap();
    }

    #[test]
    fn events_filtered_by_session() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();

        db::insert_event_with_ts(
            &conn,
            "2026-02-15T10:00:00Z",
            1,
            "turns.total",
            Some("42"),
            None,
        )
        .unwrap();
        db::insert_event_with_ts(
            &conn,
            "2026-02-15T11:00:00Z",
            2,
            "turns.total",
            Some("30"),
            None,
        )
        .unwrap();
        drop(conn);

        // Only session 1
        handle_events(&path, Some(1)).unwrap();
    }

    #[test]
    fn events_nonexistent_session() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();
        db::insert_event_with_ts(
            &conn,
            "2026-02-15T10:00:00Z",
            1,
            "turns.total",
            Some("42"),
            None,
        )
        .unwrap();
        drop(conn);

        handle_events(&path, Some(999)).unwrap();
    }

    // ── Metric degradation tests ────────────────────────────────────

    #[test]
    fn targets_unsupported_metric_shows_na() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();

        db::upsert_observation(
            &conn,
            1,
            "2026-02-15T10:00:00Z",
            None,
            None,
            r#"{"turns.total": 50}"#,
        )
        .unwrap();
        drop(conn);

        let config = make_targets_config(vec![
            make_rule("turns.total", "avg", 80.0, "below", "Avg turns"),
            {
                let mut r = make_rule("cost.estimate_usd", "avg", 30.0, "below", "Avg cost");
                r.unit = Some("$".to_string());
                r
            },
        ]);

        // Only support turns.total — cost should show N/A
        let limited_metrics: &[&str] = &["turns.total"];
        handle_targets(&path, 10, &config, "codex", limited_metrics).unwrap();
    }

    #[test]
    fn targets_all_unsupported_shows_summary() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();

        db::upsert_observation(
            &conn,
            1,
            "2026-02-15T10:00:00Z",
            None,
            None,
            r#"{"turns.total": 50}"#,
        )
        .unwrap();
        drop(conn);

        let config = make_targets_config(vec![{
            let mut r = make_rule("cost.estimate_usd", "avg", 30.0, "below", "Avg cost");
            r.unit = Some("$".to_string());
            r
        }]);

        // No metrics supported
        let no_metrics: &[&str] = &[];
        handle_targets(&path, 10, &config, "codex", no_metrics).unwrap();
    }

    #[test]
    fn is_metric_available_tests() {
        let rule = make_rule("cost.estimate_usd", "avg", 30.0, "below", "Cost");

        assert!(is_metric_available(
            &rule,
            &["cost.estimate_usd", "turns.total"]
        ));
        assert!(!is_metric_available(&rule, &["turns.total"]));

        let pct_rule = make_pct_of_rule(
            "turns.narration_only",
            "turns.total",
            20.0,
            "below",
            "Narration",
        );
        assert!(is_metric_available(
            &pct_rule,
            &["turns.narration_only", "turns.total"]
        ));
        assert!(!is_metric_available(&pct_rule, &["turns.narration_only"]));
    }

    // ── Reingest tests ──────────────────────────────────────────────────

    #[test]
    fn reingest_requires_last_or_all() {
        let (_dir, path) = test_db_path();
        let sessions_dir = TempDir::new().unwrap();
        let adapter = crate::adapters::claude::ClaudeAdapter::new();
        let result = handle_reingest(&path, sessions_dir.path(), None, false, &[], &adapter);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("--last N or --all"));
    }

    #[test]
    fn reingest_no_sessions_dir() {
        let (_dir, path) = test_db_path();
        let adapter = crate::adapters::claude::ClaudeAdapter::new();
        let result = handle_reingest(
            &path,
            Path::new("/nonexistent/sessions"),
            None,
            true,
            &[],
            &adapter,
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not found"));
    }

    #[test]
    fn reingest_empty_sessions_dir() {
        let (_dir, path) = test_db_path();
        let sessions_dir = TempDir::new().unwrap();
        let adapter = crate::adapters::claude::ClaudeAdapter::new();
        handle_reingest(&path, sessions_dir.path(), None, true, &[], &adapter).unwrap();
    }

    #[test]
    fn reingest_all_jsonl_files() {
        let (_dir, path) = test_db_path();
        let sessions_dir = TempDir::new().unwrap();

        // Create two session files
        for i in 0..2 {
            let jsonl = sessions_dir.path().join(format!("{i}.jsonl"));
            std::fs::write(
                &jsonl,
                format!(
                    r#"{{"type":"assistant","message":{{"content":[{{"type":"text","text":"turn {i}"}}]}}}}
{{"type":"result","duration_ms":{},"total_cost_usd":{},"modelUsage":{{}}}}"#,
                    (i + 1) * 1000,
                    (i + 1) as f64 * 0.1
                ),
            )
            .unwrap();
        }

        let adapter = crate::adapters::claude::ClaudeAdapter::new();
        handle_reingest(&path, sessions_dir.path(), None, true, &[], &adapter).unwrap();

        // Verify observations were created for both sessions
        let conn = db::open_or_create(&path).unwrap();
        let obs = db::recent_observations(&conn, 10).unwrap();
        assert_eq!(obs.len(), 2);
    }

    #[test]
    fn reingest_last_n_only() {
        let (_dir, path) = test_db_path();
        let sessions_dir = TempDir::new().unwrap();

        // Create 5 session files
        for i in 0..5 {
            let jsonl = sessions_dir.path().join(format!("{i}.jsonl"));
            std::fs::write(
                &jsonl,
                r#"{"type":"result","duration_ms":1000,"total_cost_usd":0.1,"modelUsage":{}}"#,
            )
            .unwrap();
        }

        let adapter = crate::adapters::claude::ClaudeAdapter::new();
        handle_reingest(&path, sessions_dir.path(), Some(2), false, &[], &adapter).unwrap();

        // Only last 2 sessions (3, 4) should be ingested
        let conn = db::open_or_create(&path).unwrap();
        let obs = db::recent_observations(&conn, 10).unwrap();
        assert_eq!(obs.len(), 2);
        let sessions: Vec<i64> = obs.iter().map(|o| o.session).collect();
        assert!(sessions.contains(&3));
        assert!(sessions.contains(&4));
    }

    #[test]
    fn reingest_clears_old_events() {
        let (_dir, path) = test_db_path();
        let sessions_dir = TempDir::new().unwrap();

        // Create one session file
        let jsonl = sessions_dir.path().join("0.jsonl");
        std::fs::write(
            &jsonl,
            r#"{"type":"assistant","message":{"content":[{"type":"text","text":"hello"}]}}
{"type":"result","duration_ms":5000,"total_cost_usd":0.5,"modelUsage":{}}"#,
        )
        .unwrap();

        // Pre-populate with fake events for session 0
        let conn = db::open_or_create(&path).unwrap();
        db::insert_event_with_ts(
            &conn,
            "2026-01-01T00:00:00Z",
            0,
            "fake.event",
            Some("old"),
            None,
        )
        .unwrap();
        drop(conn);

        let adapter = crate::adapters::claude::ClaudeAdapter::new();
        handle_reingest(&path, sessions_dir.path(), None, true, &[], &adapter).unwrap();

        // Verify old fake event was removed
        let conn = db::open_or_create(&path).unwrap();
        let events = db::events_by_session(&conn, 0).unwrap();
        assert!(events.iter().all(|e| e.kind != "fake.event"));
        // But new events exist
        assert!(events.iter().any(|e| e.kind == "turns.total"));
    }

    #[test]
    fn reingest_compressed_zst_file() {
        let (_dir, path) = test_db_path();
        let sessions_dir = TempDir::new().unwrap();

        // Create a compressed session file
        let jsonl_content = r#"{"type":"assistant","message":{"content":[{"type":"text","text":"hi"}]}}
{"type":"result","duration_ms":3000,"total_cost_usd":0.3,"modelUsage":{}}"#;
        let compressed = zstd::encode_all(jsonl_content.as_bytes(), 3).unwrap();
        std::fs::write(sessions_dir.path().join("0.jsonl.zst"), compressed).unwrap();

        let adapter = crate::adapters::claude::ClaudeAdapter::new();
        handle_reingest(&path, sessions_dir.path(), None, true, &[], &adapter).unwrap();

        // Verify session was ingested from compressed data
        let conn = db::open_or_create(&path).unwrap();
        let obs = db::get_observation(&conn, 0).unwrap().unwrap();
        let data: serde_json::Value = serde_json::from_str(&obs.data).unwrap();
        assert_eq!(data["turns.total"], 1);
        assert!((data["session.duration_secs"].as_f64().unwrap() - 3.0).abs() < 0.001);
    }

    #[test]
    fn parse_session_iteration_jsonl() {
        let path = Path::new("/tmp/sessions/42.jsonl");
        assert_eq!(parse_session_iteration(path), Some(42));
    }

    #[test]
    fn parse_session_iteration_zst() {
        let path = Path::new("/tmp/sessions/100.jsonl.zst");
        assert_eq!(parse_session_iteration(path), Some(100));
    }

    #[test]
    fn parse_session_iteration_non_numeric() {
        let path = Path::new("/tmp/sessions/notes.jsonl");
        assert_eq!(parse_session_iteration(path), None);
    }

    #[test]
    fn parse_session_iteration_other_ext() {
        let path = Path::new("/tmp/sessions/42.txt");
        assert_eq!(parse_session_iteration(path), None);
    }

    // ── Beads report tests ──────────────────────────────────────────────

    #[test]
    fn beads_no_database() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("nonexistent.db");
        handle_beads(&path).unwrap();
    }

    #[test]
    fn beads_empty_database() {
        let (_dir, path) = test_db_path();
        db::open_or_create(&path).unwrap();
        handle_beads(&path).unwrap();
    }

    #[test]
    fn beads_with_completed_and_open() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();

        db::upsert_bead_metrics(
            &conn,
            "proj-abc",
            2,
            600.0,
            80,
            None,
            None,
            Some("2026-02-15T12:00:00Z"),
        )
        .unwrap();
        db::upsert_bead_metrics(&conn, "proj-def", 1, 300.0, 40, None, None, None).unwrap();

        drop(conn);
        handle_beads(&path).unwrap();
    }

    #[test]
    fn beads_fastest_slowest() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();

        // Three completed beads with different times
        db::upsert_bead_metrics(
            &conn,
            "proj-fast",
            1,
            120.0,
            20,
            None,
            None,
            Some("2026-01-01T00:00:00Z"),
        )
        .unwrap();
        db::upsert_bead_metrics(
            &conn,
            "proj-mid",
            2,
            600.0,
            50,
            None,
            None,
            Some("2026-01-02T00:00:00Z"),
        )
        .unwrap();
        db::upsert_bead_metrics(
            &conn,
            "proj-slow",
            3,
            1800.0,
            100,
            None,
            None,
            Some("2026-01-03T00:00:00Z"),
        )
        .unwrap();

        drop(conn);
        handle_beads(&path).unwrap();
    }

    #[test]
    fn beads_single_completed() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();

        db::upsert_bead_metrics(
            &conn,
            "proj-only",
            1,
            300.0,
            40,
            None,
            None,
            Some("2026-02-15T00:00:00Z"),
        )
        .unwrap();

        drop(conn);
        // Should not show "Slowest" when fastest == slowest
        handle_beads(&path).unwrap();
    }

    #[test]
    fn truncate_bead_id_short() {
        assert_eq!(truncate_bead_id("proj-abc", 27), "proj-abc");
    }

    // ── parse_closed_bead_ids / reconcile tests ────────────────────────

    #[test]
    fn test_query_closed_bead_ids_parses_json() {
        let json = r#"[
            {"id": "beads-abc", "title": "Do thing", "status": "closed"},
            {"id": "beads-def", "title": "Another", "status": "closed"},
            {"id": "beads-ghi"}
        ]"#;
        let ids = parse_closed_bead_ids(json);
        assert_eq!(ids, vec!["beads-abc", "beads-def", "beads-ghi"]);
    }

    #[test]
    fn test_query_closed_bead_ids_empty_array() {
        let ids = parse_closed_bead_ids("[]");
        assert!(ids.is_empty());
    }

    #[test]
    fn test_query_closed_bead_ids_invalid_json() {
        let ids = parse_closed_bead_ids("not json");
        assert!(ids.is_empty());
    }

    #[test]
    fn test_reconcile_updates_stale_beads() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("blacksmith.db");
        let conn = db::open_or_create(&path).unwrap();

        // Insert two beads: one with completed_at=None, one already completed
        db::upsert_bead_metrics(&conn, "beads-stale", 1, 60.0, 10, None, None, None).unwrap();
        db::upsert_bead_metrics(
            &conn,
            "beads-done",
            1,
            30.0,
            5,
            None,
            None,
            Some("2026-02-16T10:00:00Z"),
        )
        .unwrap();

        // Simulate reconciliation by calling mark_bead_completed directly
        // (since we can't control the bd command in tests)
        let result = db::mark_bead_completed(&conn, "beads-stale").unwrap();
        assert!(result, "stale bead should be marked as completed");

        let bm = db::get_bead_metrics(&conn, "beads-stale").unwrap().unwrap();
        assert!(
            bm.completed_at.is_some(),
            "completed_at should now be set for stale bead"
        );

        // The already-completed bead should remain unchanged
        let bm2 = db::get_bead_metrics(&conn, "beads-done").unwrap().unwrap();
        assert_eq!(bm2.completed_at.as_deref(), Some("2026-02-16T10:00:00Z"));
    }

    #[test]
    fn truncate_bead_id_exact() {
        let id = "a".repeat(27);
        assert_eq!(truncate_bead_id(&id, 27), id);
    }

    #[test]
    fn truncate_bead_id_long() {
        let id = "a".repeat(30);
        let result = truncate_bead_id(&id, 27);
        assert_eq!(result.len(), 27);
        assert!(result.ends_with("..."));
    }
}
