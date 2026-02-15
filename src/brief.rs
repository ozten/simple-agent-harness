use crate::db;
use std::path::Path;

/// Handle the `brief` subcommand.
///
/// Generates a performance feedback snippet for prompt injection.
/// Queries recent metrics and open improvements from DB, formats as markdown.
/// Returns empty string (no output) if no DB file exists.
pub fn handle_brief(db_path: &Path) -> Result<(), String> {
    let text = generate_brief(db_path)?;
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
/// Otherwise returns the formatted brief with performance feedback and/or improvements.
pub fn generate_brief(db_path: &Path) -> Result<String, String> {
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

/// Format the PERFORMANCE FEEDBACK section from a single observation's data.
fn format_performance_feedback(session: i64, data: &serde_json::Value) -> String {
    let mut output = String::from("## PERFORMANCE FEEDBACK (auto-generated)\n");
    output.push_str(&format!("\nLast session #{}:\n", session));

    let turns = data["turns.total"].as_u64().unwrap_or(0);
    let narration = data["turns.narration_only"].as_u64().unwrap_or(0);
    let parallel = data["turns.parallel"].as_u64().unwrap_or(0);
    let cost = data["cost.estimate_usd"].as_f64().unwrap_or(0.0);
    let duration_ms = data["session.duration_ms"].as_u64().unwrap_or(0);

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
    let duration_s = duration_ms / 1000;
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
        handle_brief(&path).unwrap();
    }

    #[test]
    fn brief_empty_database_produces_no_output() {
        let (_dir, path) = test_db_path();
        db::open_or_create(&path).unwrap();
        let text = generate_brief(&path).unwrap();
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
        let text = generate_brief(&path).unwrap();
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
        let text = generate_brief(&path).unwrap();
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

        let text = generate_brief(&path).unwrap();
        assert!(text.contains("## OPEN IMPROVEMENTS (2 of 3)"));
        assert!(text.contains("R1 [code-quality] Use ESLint --fix in pre-commit hook"));
    }

    // ── Performance feedback tests ──────────────────────────────────────

    #[test]
    fn brief_shows_performance_feedback_with_observation() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();

        let data = r#"{"turns.total":67,"turns.narration_only":4,"turns.parallel":8,"cost.estimate_usd":24.57,"session.duration_ms":1847000}"#;
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

        let text = generate_brief(&path).unwrap();
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

        let data1 = r#"{"turns.total":50,"turns.narration_only":0,"turns.parallel":0,"cost.estimate_usd":10.0,"session.duration_ms":60000}"#;
        let data2 = r#"{"turns.total":80,"turns.narration_only":5,"turns.parallel":10,"cost.estimate_usd":30.0,"session.duration_ms":120000}"#;
        db::upsert_observation(&conn, 1, "2026-02-15T10:00:00Z", None, None, data1).unwrap();
        db::upsert_observation(&conn, 2, "2026-02-15T11:00:00Z", None, None, data2).unwrap();

        drop(conn);

        let text = generate_brief(&path).unwrap();
        // Should show session 2 (most recent)
        assert!(text.contains("Last session #2:"));
        assert!(text.contains("Turns: 80"));
    }

    #[test]
    fn brief_performance_feedback_and_improvements_together() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();

        let data = r#"{"turns.total":42,"turns.narration_only":2,"turns.parallel":5,"cost.estimate_usd":1.5,"session.duration_ms":60000}"#;
        db::upsert_observation(&conn, 1, "2026-02-15T10:00:00Z", None, None, data).unwrap();
        db::insert_improvement(&conn, "workflow", "Batch reads", None, None, None).unwrap();

        drop(conn);

        let text = generate_brief(&path).unwrap();
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

        let data = r#"{"turns.total":30,"turns.narration_only":0,"turns.parallel":3,"cost.estimate_usd":5.0,"session.duration_ms":45000}"#;
        db::upsert_observation(&conn, 1, "2026-02-15T10:00:00Z", None, None, data).unwrap();

        drop(conn);

        let text = generate_brief(&path).unwrap();
        assert!(text.contains("## PERFORMANCE FEEDBACK"));
        assert!(!text.contains("## OPEN IMPROVEMENTS"));
    }

    #[test]
    fn brief_performance_feedback_zero_turns() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();

        let data = r#"{"turns.total":0,"turns.narration_only":0,"turns.parallel":0,"cost.estimate_usd":0.0,"session.duration_ms":0}"#;
        db::upsert_observation(&conn, 1, "2026-02-15T10:00:00Z", None, None, data).unwrap();

        drop(conn);

        let text = generate_brief(&path).unwrap();
        assert!(text.contains("Turns: 0"));
        assert!(text.contains("Narration-only turns: 0%"));
        assert!(text.contains("Parallel tool calls: 0%"));
    }

    #[test]
    fn brief_performance_feedback_duration_seconds_only() {
        let (_dir, path) = test_db_path();
        let conn = db::open_or_create(&path).unwrap();

        let data = r#"{"turns.total":10,"turns.narration_only":0,"turns.parallel":0,"cost.estimate_usd":0.5,"session.duration_ms":45000}"#;
        db::upsert_observation(&conn, 1, "2026-02-15T10:00:00Z", None, None, data).unwrap();

        drop(conn);

        let text = generate_brief(&path).unwrap();
        assert!(text.contains("Duration: 45s"));
    }

    #[test]
    fn format_performance_feedback_unit() {
        let data: serde_json::Value = serde_json::from_str(
            r#"{"turns.total":100,"turns.narration_only":10,"turns.parallel":20,"cost.estimate_usd":15.99,"session.duration_ms":300000}"#,
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
}
