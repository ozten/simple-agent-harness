use crate::config::MetricsTargetsConfig;
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
        crate::config::MetricsTargetsConfig { rules }
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
