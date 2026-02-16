use crate::config::{MetricsTargetsConfig, TargetRule};
use crate::db;
use std::path::Path;

/// Handle the `brief` subcommand.
///
/// Generates a performance feedback snippet for prompt injection.
/// Queries recent metrics and open improvements from DB, formats as markdown.
/// Returns empty string (no output) if no DB file exists.
pub fn handle_brief(
    db_path: &Path,
    targets_config: Option<&MetricsTargetsConfig>,
    supported_metrics: Option<&[&str]>,
) -> Result<(), String> {
    let text = generate_brief(db_path, targets_config, supported_metrics)?;
    if !text.is_empty() {
        print!("{text}");
    }
    Ok(())
}

/// Generate the brief snippet as a string for prompt injection.
///
/// Returns an empty string when:
/// - The database file doesn't exist (first run)
/// - The database has no improvements and no observations
///
/// Otherwise returns the formatted brief with performance feedback,
/// target warnings, and/or improvements.
pub fn generate_brief(
    db_path: &Path,
    targets_config: Option<&MetricsTargetsConfig>,
    supported_metrics: Option<&[&str]>,
) -> Result<String, String> {
    // If no database file exists, silently produce no output
    if !db_path.exists() {
        return Ok(String::new());
    }

    let conn = db::open_or_create(db_path).map_err(|e| format!("Failed to open database: {e}"))?;

    let mut output = String::new();

    // Performance feedback from most recent observation
    let observations = db::recent_observations(&conn, 1)
        .map_err(|e| format!("Failed to query observations: {e}"))?;

    if let Some(obs) = observations.first() {
        if let Ok(data) = serde_json::from_str::<serde_json::Value>(&obs.data) {
            output.push_str(&format_performance_feedback(obs.session, &data));
        }
    }

    // Target warnings section
    if let Some(config) = targets_config {
        if !config.rules.is_empty() {
            let streak_threshold = config.streak_threshold;
            // Load enough observations for streak detection
            let streak_window = std::cmp::max(streak_threshold as i64, 10);
            let recent = db::recent_observations(&conn, streak_window)
                .map_err(|e| format!("Failed to query observations for targets: {e}"))?;

            if !recent.is_empty() {
                let warnings = format_target_warnings(
                    &config.rules,
                    &recent,
                    streak_threshold,
                    supported_metrics,
                );
                if !warnings.is_empty() {
                    if !output.is_empty() {
                        output.push_str("\n\n");
                    }
                    output.push_str(&warnings);
                }
            }
        }
    }

    // Open improvements section
    let open = db::list_improvements(&conn, Some("open"), None)
        .map_err(|e| format!("Failed to query improvements: {e}"))?;

    let total =
        db::count_improvements(&conn).map_err(|e| format!("Failed to count improvements: {e}"))?;

    if total > 0 {
        if !output.is_empty() {
            output.push_str("\n\n");
        }
        output.push_str(&format!(
            "## OPEN IMPROVEMENTS ({} of {})\n",
            open.len(),
            total
        ));

        for imp in &open {
            output.push_str(&format!(
                "\n{} [{}] {}",
                imp.ref_id, imp.category, imp.title
            ));
        }
    }

    Ok(output)
}

/// Evaluate a single target rule against one observation's data.
/// Returns Some(actual_value) if the target is missed, None if it passes or has no data.
fn evaluate_single_observation(rule: &TargetRule, data: &serde_json::Value) -> Option<f64> {
    let actual = match rule.compare.as_str() {
        "avg" => data[&rule.kind].as_f64()?,
        "pct_of" => {
            let relative_to = rule.relative_to.as_ref()?;
            let num = data[&rule.kind].as_f64().unwrap_or(0.0);
            let den = data[relative_to.as_str()].as_f64()?;
            if den == 0.0 {
                return None;
            }
            (num / den) * 100.0
        }
        "pct_sessions" => {
            // pct_sessions doesn't make sense for a single observation
            // Skip for per-observation evaluation
            return None;
        }
        _ => return None,
    };

    let passed = match rule.direction.as_str() {
        "below" => actual <= rule.threshold,
        "above" => actual >= rule.threshold,
        _ => false,
    };

    if passed {
        None
    } else {
        Some(actual)
    }
}

/// Format the actual value for display based on the rule's unit.
fn format_actual_value(actual: f64, unit: Option<&str>) -> String {
    match unit {
        Some("$") => format!("${:.2}", actual),
        Some("%") => format!("{:.0}%", actual),
        _ => format!("{:.1}", actual),
    }
}

/// Format the target threshold for display based on the rule's unit and direction.
fn format_threshold(rule: &TargetRule) -> String {
    let dir_symbol = match rule.direction.as_str() {
        "below" => "<",
        "above" => ">",
        _ => "?",
    };
    let unit = rule.unit.as_deref().unwrap_or("");
    if unit == "$" {
        format!("{}{:.2}", dir_symbol, rule.threshold)
    } else if unit == "%" {
        format!("{}{:.0}%", dir_symbol, rule.threshold)
    } else {
        format!("{}{:.0}", dir_symbol, rule.threshold)
    }
}

/// Generate the TARGET WARNINGS section for the brief.
///
/// Checks each target rule against the most recent observation. For misses,
/// also counts the consecutive miss streak (working backwards through history).
/// If the streak >= streak_threshold, the warning is escalated to an ALERT.
/// Check if a target rule's metrics are supported by the current adapter.
/// Returns true if supported_metrics is None (no filtering) or if the rule's
/// kind (and relative_to for pct_of) are in the supported list.
fn is_rule_supported(rule: &TargetRule, supported_metrics: Option<&[&str]>) -> bool {
    let supported = match supported_metrics {
        Some(s) => s,
        None => return true,
    };
    if !supported.contains(&rule.kind.as_str()) {
        return false;
    }
    if let Some(ref rel) = rule.relative_to {
        if !supported.contains(&rel.as_str()) {
            return false;
        }
    }
    true
}

fn format_target_warnings(
    rules: &[TargetRule],
    observations: &[db::Observation],
    streak_threshold: u32,
    supported_metrics: Option<&[&str]>,
) -> String {
    if observations.is_empty() {
        return String::new();
    }

    // Parse observation data into JSON values
    // observations come in DESC order (most recent first)
    let data_values: Vec<serde_json::Value> = observations
        .iter()
        .map(|obs| serde_json::from_str(&obs.data).unwrap_or_default())
        .collect();

    let mut lines: Vec<String> = Vec::new();

    for rule in rules {
        // Skip rules for metrics the current adapter doesn't support
        if !is_rule_supported(rule, supported_metrics) {
            continue;
        }

        // Check most recent observation
        let most_recent = &data_values[0];
        if let Some(actual) = evaluate_single_observation(rule, most_recent) {
            // Count consecutive miss streak
            let mut streak: u32 = 0;
            for d in &data_values {
                if evaluate_single_observation(rule, d).is_some() {
                    streak += 1;
                } else {
                    break;
                }
            }

            let actual_str = format_actual_value(actual, rule.unit.as_deref());
            let target_str = format_threshold(rule);

            if streak >= streak_threshold {
                lines.push(format!(
                    "ALERT: {} {} (target: {}) — missed {} consecutive sessions",
                    rule.label, actual_str, target_str, streak
                ));
            } else {
                lines.push(format!(
                    "WARNING: {} {} (target: {})",
                    rule.label, actual_str, target_str
                ));
            }
        }
    }

    if lines.is_empty() {
        return String::new();
    }

    let mut output = String::from("## TARGET WARNINGS\n");
    for line in &lines {
        output.push_str(&format!("\n{line}"));
    }
    output
}

/// Format the PERFORMANCE FEEDBACK section from a single observation's data.
fn format_performance_feedback(session: i64, data: &serde_json::Value) -> String {
    let mut output = String::from("## PERFORMANCE FEEDBACK (auto-generated)\n");
    output.push_str(&format!("\nLast session #{}:\n", session));

    let turns = data["turns.total"].as_u64().unwrap_or(0);
    let narration = data["turns.narration_only"].as_u64().unwrap_or(0);
    let parallel = data["turns.parallel"].as_u64().unwrap_or(0);
    let cost = data["cost.estimate_usd"].as_f64().unwrap_or(0.0);
    let duration_s = data["session.duration_secs"].as_f64().unwrap_or(0.0) as u64;

    // Narration percentage
    let narr_pct = if turns > 0 {
        (narration as f64 / turns as f64 * 100.0).round() as u64
    } else {
        0
    };

    // Parallel percentage
    let par_pct = if turns > 0 {
        (parallel as f64 / turns as f64 * 100.0).round() as u64
    } else {
        0
    };

    // Duration formatting
    let mins = duration_s / 60;
    let secs = duration_s % 60;
    let duration_str = if mins > 0 {
        format!("{}m{:02}s", mins, secs)
    } else {
        format!("{}s", secs)
    };

    output.push_str(&format!("  Turns: {}\n", turns));
    output.push_str(&format!("  Narration-only turns: {}%\n", narr_pct));
    output.push_str(&format!("  Parallel tool calls: {}%\n", par_pct));
    output.push_str(&format!("  Cost: ${:.2}\n", cost));
    output.push_str(&format!("  Duration: {}", duration_str));

    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_db_path() -> (TempDir, std::path::PathBuf) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("blacksmith.db");
        (dir, path)
    }

    #[test]
    fn brief_no_database_file_produces_no_output() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("nonexistent.db");
        handle_brief(&path, None, None).unwrap();
    }

    #[test]
    fn brief_empty_database_produces_no_output() {
        let (_dir, path) = test_db_path();
        db::open_or_create(&path).unwrap();
        let text = generate_brief(&path, None, None).unwrap();
        assert!(text.is_empty());
    }

    #[test]
    fn brief_shows_open_improvements() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();

        db::insert_improvement(&conn, "workflow", "Batch file reads", None, None, None).unwrap();
        db::insert_improvement(
            &conn,
            "cost",
            "Skip full test suite for CSS",
            None,
            None,
            None,
        )
        .unwrap();

        drop(conn);
        let text = generate_brief(&path, None, None).unwrap();
        assert!(text.contains("## OPEN IMPROVEMENTS (2 of 2)"));
        assert!(text.contains("R1 [workflow] Batch file reads"));
        assert!(text.contains("R2 [cost] Skip full test suite for CSS"));
    }

    #[test]
    fn brief_excludes_non_open_improvements_from_listing() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();

        db::insert_improvement(&conn, "workflow", "Open item", None, None, None).unwrap();
        conn.execute(
            "INSERT INTO improvements (ref, category, status, title) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params!["R2", "cost", "promoted", "Promoted item"],
        )
        .unwrap();

        drop(conn);
        let text = generate_brief(&path, None, None).unwrap();
        assert!(text.contains("## OPEN IMPROVEMENTS (1 of 2)"));
        assert!(text.contains("R1 [workflow] Open item"));
        assert!(!text.contains("Promoted item"));
    }

    #[test]
    fn brief_format_matches_prd() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();

        db::insert_improvement(
            &conn,
            "code-quality",
            "Use ESLint --fix in pre-commit hook",
            None,
            None,
            None,
        )
        .unwrap();
        db::insert_improvement(
            &conn,
            "workflow",
            "Batch file reads when exploring",
            None,
            None,
            None,
        )
        .unwrap();
        conn.execute(
            "INSERT INTO improvements (ref, category, status, title) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params!["R3", "cost", "promoted", "Skip full test suite for CSS"],
        )
        .unwrap();

        drop(conn);

        let text = generate_brief(&path, None, None).unwrap();
        assert!(text.contains("## OPEN IMPROVEMENTS (2 of 3)"));
        assert!(text.contains("R1 [code-quality] Use ESLint --fix in pre-commit hook"));
    }

    // ── Performance feedback tests ──────────────────────────────────────

    #[test]
    fn brief_shows_performance_feedback_with_observation() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();

        let data = r#"{"turns.total":67,"turns.narration_only":4,"turns.parallel":8,"cost.estimate_usd":24.57,"session.duration_secs":1847}"#;
        db::upsert_observation(
            &conn,
            42,
            "2026-02-15T10:30:00Z",
            Some(1847),
            Some("completed"),
            data,
        )
        .unwrap();

        drop(conn);

        let text = generate_brief(&path, None, None).unwrap();
        assert!(text.contains("## PERFORMANCE FEEDBACK (auto-generated)"));
        assert!(text.contains("Last session #42:"));
        assert!(text.contains("Turns: 67"));
        assert!(text.contains("Narration-only turns: 6%"));
        assert!(text.contains("Parallel tool calls: 12%"));
        assert!(text.contains("Cost: $24.57"));
        assert!(text.contains("Duration: 30m47s"));
    }

    #[test]
    fn brief_performance_feedback_uses_most_recent_session() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();

        let data1 = r#"{"turns.total":50,"turns.narration_only":0,"turns.parallel":0,"cost.estimate_usd":10.0,"session.duration_secs":60}"#;
        let data2 = r#"{"turns.total":80,"turns.narration_only":5,"turns.parallel":10,"cost.estimate_usd":30.0,"session.duration_secs":120}"#;
        db::upsert_observation(&conn, 1, "2026-02-15T10:00:00Z", None, None, data1).unwrap();
        db::upsert_observation(&conn, 2, "2026-02-15T11:00:00Z", None, None, data2).unwrap();

        drop(conn);

        let text = generate_brief(&path, None, None).unwrap();
        // Should show session 2 (most recent)
        assert!(text.contains("Last session #2:"));
        assert!(text.contains("Turns: 80"));
    }

    #[test]
    fn brief_performance_feedback_and_improvements_together() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();

        let data = r#"{"turns.total":42,"turns.narration_only":2,"turns.parallel":5,"cost.estimate_usd":1.5,"session.duration_secs":60}"#;
        db::upsert_observation(&conn, 1, "2026-02-15T10:00:00Z", None, None, data).unwrap();
        db::insert_improvement(&conn, "workflow", "Batch reads", None, None, None).unwrap();

        drop(conn);

        let text = generate_brief(&path, None, None).unwrap();
        // Both sections present
        assert!(text.contains("## PERFORMANCE FEEDBACK"));
        assert!(text.contains("## OPEN IMPROVEMENTS (1 of 1)"));
        // Feedback comes before improvements
        let fb_pos = text.find("## PERFORMANCE FEEDBACK").unwrap();
        let imp_pos = text.find("## OPEN IMPROVEMENTS").unwrap();
        assert!(fb_pos < imp_pos);
    }

    #[test]
    fn brief_observation_only_no_improvements() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();

        let data = r#"{"turns.total":30,"turns.narration_only":0,"turns.parallel":3,"cost.estimate_usd":5.0,"session.duration_secs":45}"#;
        db::upsert_observation(&conn, 1, "2026-02-15T10:00:00Z", None, None, data).unwrap();

        drop(conn);

        let text = generate_brief(&path, None, None).unwrap();
        assert!(text.contains("## PERFORMANCE FEEDBACK"));
        assert!(!text.contains("## OPEN IMPROVEMENTS"));
    }

    #[test]
    fn brief_performance_feedback_zero_turns() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();

        let data = r#"{"turns.total":0,"turns.narration_only":0,"turns.parallel":0,"cost.estimate_usd":0.0,"session.duration_secs":0}"#;
        db::upsert_observation(&conn, 1, "2026-02-15T10:00:00Z", None, None, data).unwrap();

        drop(conn);

        let text = generate_brief(&path, None, None).unwrap();
        assert!(text.contains("Turns: 0"));
        assert!(text.contains("Narration-only turns: 0%"));
        assert!(text.contains("Parallel tool calls: 0%"));
    }

    #[test]
    fn brief_performance_feedback_duration_seconds_only() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();

        let data = r#"{"turns.total":10,"turns.narration_only":0,"turns.parallel":0,"cost.estimate_usd":0.5,"session.duration_secs":45}"#;
        db::upsert_observation(&conn, 1, "2026-02-15T10:00:00Z", None, None, data).unwrap();

        drop(conn);

        let text = generate_brief(&path, None, None).unwrap();
        assert!(text.contains("Duration: 45s"));
    }

    #[test]
    fn format_performance_feedback_unit() {
        let data: serde_json::Value = serde_json::from_str(
            r#"{"turns.total":100,"turns.narration_only":10,"turns.parallel":20,"cost.estimate_usd":15.99,"session.duration_secs":300}"#,
        )
        .unwrap();

        let result = format_performance_feedback(5, &data);
        assert!(result.contains("Last session #5:"));
        assert!(result.contains("Turns: 100"));
        assert!(result.contains("Narration-only turns: 10%"));
        assert!(result.contains("Parallel tool calls: 20%"));
        assert!(result.contains("Cost: $15.99"));
        assert!(result.contains("Duration: 5m00s"));
    }

    // ── Target warnings tests ──────────────────────────────────────────

    fn make_rule(
        kind: &str,
        compare: &str,
        threshold: f64,
        direction: &str,
        label: &str,
    ) -> TargetRule {
        TargetRule {
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
    ) -> TargetRule {
        TargetRule {
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
    fn target_warnings_single_miss() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();

        // High cost exceeds threshold
        db::upsert_observation(
            &conn,
            1,
            "2026-02-15T10:00:00Z",
            None,
            None,
            r#"{"cost.estimate_usd": 35.0}"#,
        )
        .unwrap();
        drop(conn);

        let mut rule = make_rule("cost.estimate_usd", "avg", 30.0, "below", "Avg cost");
        rule.unit = Some("$".to_string());
        let config = MetricsTargetsConfig {
            rules: vec![rule],
            streak_threshold: 3,
        };

        let text = generate_brief(&path, Some(&config), None).unwrap();
        assert!(text.contains("## TARGET WARNINGS"));
        assert!(text.contains("WARNING: Avg cost $35.00 (target: <30.00)"));
        assert!(!text.contains("ALERT"));
    }

    #[test]
    fn target_warnings_no_miss_no_section() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();

        // Cost within target
        db::upsert_observation(
            &conn,
            1,
            "2026-02-15T10:00:00Z",
            None,
            None,
            r#"{"cost.estimate_usd": 20.0}"#,
        )
        .unwrap();
        drop(conn);

        let mut rule = make_rule("cost.estimate_usd", "avg", 30.0, "below", "Avg cost");
        rule.unit = Some("$".to_string());
        let config = MetricsTargetsConfig {
            rules: vec![rule],
            streak_threshold: 3,
        };

        let text = generate_brief(&path, Some(&config), None).unwrap();
        assert!(!text.contains("## TARGET WARNINGS"));
    }

    #[test]
    fn target_warnings_streak_escalates_to_alert() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();

        // 4 consecutive sessions with high cost (streak threshold = 3)
        for i in 1..=4 {
            db::upsert_observation(
                &conn,
                i,
                "2026-02-15T10:00:00Z",
                None,
                None,
                r#"{"cost.estimate_usd": 40.0}"#,
            )
            .unwrap();
        }
        drop(conn);

        let mut rule = make_rule("cost.estimate_usd", "avg", 30.0, "below", "Avg cost");
        rule.unit = Some("$".to_string());
        let config = MetricsTargetsConfig {
            rules: vec![rule],
            streak_threshold: 3,
        };

        let text = generate_brief(&path, Some(&config), None).unwrap();
        assert!(text
            .contains("ALERT: Avg cost $40.00 (target: <30.00) — missed 4 consecutive sessions"));
    }

    #[test]
    fn target_warnings_streak_broken_by_pass() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();

        // Session 1: miss, session 2: pass, session 3: miss
        // Most recent first (session 3 is most recent)
        db::upsert_observation(
            &conn,
            1,
            "2026-02-15T10:00:00Z",
            None,
            None,
            r#"{"cost.estimate_usd": 40.0}"#,
        )
        .unwrap();
        db::upsert_observation(
            &conn,
            2,
            "2026-02-15T11:00:00Z",
            None,
            None,
            r#"{"cost.estimate_usd": 10.0}"#, // pass
        )
        .unwrap();
        db::upsert_observation(
            &conn,
            3,
            "2026-02-15T12:00:00Z",
            None,
            None,
            r#"{"cost.estimate_usd": 35.0}"#, // miss (most recent)
        )
        .unwrap();
        drop(conn);

        let mut rule = make_rule("cost.estimate_usd", "avg", 30.0, "below", "Avg cost");
        rule.unit = Some("$".to_string());
        let config = MetricsTargetsConfig {
            rules: vec![rule],
            streak_threshold: 3,
        };

        let text = generate_brief(&path, Some(&config), None).unwrap();
        // Streak = 1 (only session 3 misses, session 2 passes breaks the streak)
        assert!(text.contains("WARNING: Avg cost"));
        assert!(!text.contains("ALERT"));
    }

    #[test]
    fn target_warnings_pct_of_miss() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();

        // 25% narration (25/100), target is <20%
        db::upsert_observation(
            &conn,
            1,
            "2026-02-15T10:00:00Z",
            None,
            None,
            r#"{"turns.narration_only": 25, "turns.total": 100}"#,
        )
        .unwrap();
        drop(conn);

        let rule = make_pct_of_rule(
            "turns.narration_only",
            "turns.total",
            20.0,
            "below",
            "Narration-only turns",
        );
        let config = MetricsTargetsConfig {
            rules: vec![rule],
            streak_threshold: 3,
        };

        let text = generate_brief(&path, Some(&config), None).unwrap();
        assert!(text.contains("WARNING: Narration-only turns 25% (target: <20%)"));
    }

    #[test]
    fn target_warnings_above_direction() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();

        // 5% parallel, target is >10% (miss)
        db::upsert_observation(
            &conn,
            1,
            "2026-02-15T10:00:00Z",
            None,
            None,
            r#"{"turns.parallel": 5, "turns.total": 100}"#,
        )
        .unwrap();
        drop(conn);

        let rule = make_pct_of_rule(
            "turns.parallel",
            "turns.total",
            10.0,
            "above",
            "Parallel tool calls",
        );
        let config = MetricsTargetsConfig {
            rules: vec![rule],
            streak_threshold: 3,
        };

        let text = generate_brief(&path, Some(&config), None).unwrap();
        assert!(text.contains("WARNING: Parallel tool calls 5% (target: >10%)"));
    }

    #[test]
    fn target_warnings_multiple_rules_mixed() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();

        db::upsert_observation(
            &conn,
            1,
            "2026-02-15T10:00:00Z",
            None,
            None,
            r#"{"cost.estimate_usd": 35.0, "turns.total": 50}"#,
        )
        .unwrap();
        drop(conn);

        let config = MetricsTargetsConfig {
            rules: vec![
                {
                    let mut r = make_rule("cost.estimate_usd", "avg", 30.0, "below", "Avg cost");
                    r.unit = Some("$".to_string());
                    r
                },
                // turns.total=50, target <80 → pass (no warning)
                make_rule("turns.total", "avg", 80.0, "below", "Avg turns"),
            ],
            streak_threshold: 3,
        };

        let text = generate_brief(&path, Some(&config), None).unwrap();
        assert!(text.contains("WARNING: Avg cost"));
        assert!(!text.contains("Avg turns")); // passes, so no warning
    }

    #[test]
    fn target_warnings_position_between_feedback_and_improvements() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();

        let data = r#"{"turns.total":80,"turns.narration_only":5,"turns.parallel":8,"cost.estimate_usd":35.0,"session.duration_secs":60}"#;
        db::upsert_observation(&conn, 1, "2026-02-15T10:00:00Z", None, None, data).unwrap();
        db::insert_improvement(&conn, "workflow", "Test improvement", None, None, None).unwrap();
        drop(conn);

        let config = MetricsTargetsConfig {
            rules: vec![{
                let mut r = make_rule("cost.estimate_usd", "avg", 30.0, "below", "Avg cost");
                r.unit = Some("$".to_string());
                r
            }],
            streak_threshold: 3,
        };

        let text = generate_brief(&path, Some(&config), None).unwrap();
        let perf_pos = text.find("## PERFORMANCE FEEDBACK").unwrap();
        let warn_pos = text.find("## TARGET WARNINGS").unwrap();
        let imp_pos = text.find("## OPEN IMPROVEMENTS").unwrap();
        assert!(perf_pos < warn_pos);
        assert!(warn_pos < imp_pos);
    }

    #[test]
    fn target_warnings_no_rules_no_section() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();
        db::upsert_observation(
            &conn,
            1,
            "2026-02-15T10:00:00Z",
            None,
            None,
            r#"{"cost.estimate_usd": 100.0}"#,
        )
        .unwrap();
        drop(conn);

        let config = MetricsTargetsConfig {
            rules: vec![],
            streak_threshold: 3,
        };

        let text = generate_brief(&path, Some(&config), None).unwrap();
        assert!(!text.contains("## TARGET WARNINGS"));
    }

    #[test]
    fn target_warnings_none_config_no_section() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();
        db::upsert_observation(
            &conn,
            1,
            "2026-02-15T10:00:00Z",
            None,
            None,
            r#"{"cost.estimate_usd": 100.0}"#,
        )
        .unwrap();
        drop(conn);

        let text = generate_brief(&path, None, None).unwrap();
        assert!(!text.contains("## TARGET WARNINGS"));
    }

    #[test]
    fn evaluate_single_observation_pass() {
        let rule = make_rule("cost.estimate_usd", "avg", 30.0, "below", "Cost");
        let data = serde_json::json!({"cost.estimate_usd": 20.0});
        assert!(evaluate_single_observation(&rule, &data).is_none());
    }

    #[test]
    fn evaluate_single_observation_miss() {
        let rule = make_rule("cost.estimate_usd", "avg", 30.0, "below", "Cost");
        let data = serde_json::json!({"cost.estimate_usd": 35.0});
        let result = evaluate_single_observation(&rule, &data);
        assert!((result.unwrap() - 35.0).abs() < 0.01);
    }

    #[test]
    fn evaluate_single_observation_pct_of() {
        let rule = make_pct_of_rule(
            "turns.narration_only",
            "turns.total",
            20.0,
            "below",
            "Narration",
        );
        let data = serde_json::json!({"turns.narration_only": 25, "turns.total": 100});
        let result = evaluate_single_observation(&rule, &data);
        assert!((result.unwrap() - 25.0).abs() < 0.01);
    }

    #[test]
    fn evaluate_single_observation_pct_sessions_returns_none() {
        let rule = make_rule(
            "commit.detected",
            "pct_sessions",
            70.0,
            "above",
            "Completions",
        );
        let data = serde_json::json!({"commit.detected": true});
        // pct_sessions doesn't apply to single observation
        assert!(evaluate_single_observation(&rule, &data).is_none());
    }

    #[test]
    fn evaluate_single_observation_missing_data() {
        let rule = make_rule("cost.estimate_usd", "avg", 30.0, "below", "Cost");
        let data = serde_json::json!({"turns.total": 50});
        assert!(evaluate_single_observation(&rule, &data).is_none());
    }

    #[test]
    fn format_actual_value_dollar() {
        assert_eq!(format_actual_value(35.5, Some("$")), "$35.50");
    }

    #[test]
    fn format_actual_value_percent() {
        assert_eq!(format_actual_value(25.0, Some("%")), "25%");
    }

    #[test]
    fn format_actual_value_no_unit() {
        assert_eq!(format_actual_value(42.3, None), "42.3");
    }

    #[test]
    fn streak_threshold_exactly_met_is_alert() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();

        // Exactly 3 consecutive misses, threshold = 3 → should be ALERT
        for i in 1..=3 {
            db::upsert_observation(
                &conn,
                i,
                "2026-02-15T10:00:00Z",
                None,
                None,
                r#"{"cost.estimate_usd": 40.0}"#,
            )
            .unwrap();
        }
        drop(conn);

        let mut rule = make_rule("cost.estimate_usd", "avg", 30.0, "below", "Avg cost");
        rule.unit = Some("$".to_string());
        let config = MetricsTargetsConfig {
            rules: vec![rule],
            streak_threshold: 3,
        };

        let text = generate_brief(&path, Some(&config), None).unwrap();
        assert!(text
            .contains("ALERT: Avg cost $40.00 (target: <30.00) — missed 3 consecutive sessions"));
    }

    #[test]
    fn target_warnings_streak_threshold_configurable() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();

        // 2 consecutive misses
        for i in 1..=2 {
            db::upsert_observation(
                &conn,
                i,
                "2026-02-15T10:00:00Z",
                None,
                None,
                r#"{"cost.estimate_usd": 40.0}"#,
            )
            .unwrap();
        }
        drop(conn);

        let mut rule = make_rule("cost.estimate_usd", "avg", 30.0, "below", "Avg cost");
        rule.unit = Some("$".to_string());

        // With threshold=2, 2 misses → ALERT
        let config = MetricsTargetsConfig {
            rules: vec![rule],
            streak_threshold: 2,
        };

        let text = generate_brief(&path, Some(&config), None).unwrap();
        assert!(text.contains("ALERT"));
    }

    // ── Metric degradation tests ──────────────────────────────────────

    #[test]
    fn target_warnings_skip_unsupported_metric() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();

        db::upsert_observation(
            &conn,
            1,
            "2026-02-15T10:00:00Z",
            None,
            None,
            r#"{"cost.estimate_usd": 35.0, "turns.total": 80}"#,
        )
        .unwrap();
        drop(conn);

        let config = MetricsTargetsConfig {
            rules: vec![
                {
                    let mut r = make_rule("cost.estimate_usd", "avg", 30.0, "below", "Avg cost");
                    r.unit = Some("$".to_string());
                    r
                },
                make_rule("turns.total", "avg", 90.0, "below", "Avg turns"),
            ],
            streak_threshold: 3,
        };

        // Only support turns.total, not cost.estimate_usd
        let supported: &[&str] = &["turns.total"];
        let text = generate_brief(&path, Some(&config), Some(supported)).unwrap();
        // Cost target should be silently skipped — no warning about it
        assert!(!text.contains("Avg cost"));
        // Turns target should still appear (passes, so no warning)
        assert!(!text.contains("WARNING"));
    }

    #[test]
    fn target_warnings_skip_unsupported_relative_to_metric() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();

        db::upsert_observation(
            &conn,
            1,
            "2026-02-15T10:00:00Z",
            None,
            None,
            r#"{"turns.narration_only": 25, "turns.total": 100}"#,
        )
        .unwrap();
        drop(conn);

        let config = MetricsTargetsConfig {
            rules: vec![make_pct_of_rule(
                "turns.narration_only",
                "turns.total",
                20.0,
                "below",
                "Narration-only turns",
            )],
            streak_threshold: 3,
        };

        // Support narration_only but NOT turns.total (used as relative_to)
        let supported: &[&str] = &["turns.narration_only"];
        let text = generate_brief(&path, Some(&config), Some(supported)).unwrap();
        // Rule should be skipped because relative_to metric is unsupported
        // No TARGET WARNINGS section should appear
        assert!(!text.contains("TARGET WARNINGS"));
    }

    #[test]
    fn target_warnings_none_supported_metrics_means_no_filtering() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();

        db::upsert_observation(
            &conn,
            1,
            "2026-02-15T10:00:00Z",
            None,
            None,
            r#"{"cost.estimate_usd": 35.0}"#,
        )
        .unwrap();
        drop(conn);

        let mut rule = make_rule("cost.estimate_usd", "avg", 30.0, "below", "Avg cost");
        rule.unit = Some("$".to_string());
        let config = MetricsTargetsConfig {
            rules: vec![rule],
            streak_threshold: 3,
        };

        // None means no filtering — all metrics available
        let text = generate_brief(&path, Some(&config), None).unwrap();
        assert!(text.contains("WARNING: Avg cost"));
    }

    #[test]
    fn is_rule_supported_tests() {
        let rule = make_rule("cost.estimate_usd", "avg", 30.0, "below", "Cost");

        // None supported_metrics means all supported
        assert!(is_rule_supported(&rule, None));

        // Kind in supported list
        assert!(is_rule_supported(
            &rule,
            Some(&["cost.estimate_usd", "turns.total"])
        ));

        // Kind not in supported list
        assert!(!is_rule_supported(&rule, Some(&["turns.total"])));

        // pct_of rule: both kind and relative_to must be supported
        let pct_rule = make_pct_of_rule(
            "turns.narration_only",
            "turns.total",
            20.0,
            "below",
            "Narration",
        );
        assert!(is_rule_supported(
            &pct_rule,
            Some(&["turns.narration_only", "turns.total"])
        ));
        assert!(!is_rule_supported(
            &pct_rule,
            Some(&["turns.narration_only"])
        ));
    }
}
