use crate::db;
use crate::ingest;
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
}
