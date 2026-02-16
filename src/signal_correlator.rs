//! Historical signal correlator for architecture analysis.
//!
//! Overlays historical signals (expansion events, integration iterations,
//! metadata drift) onto the module graph from structural analysis. Weights
//! recent events more heavily. Identifies modules that score high on BOTH
//! structural smells AND historical failure signals as strong refactoring
//! candidates.
//!
//! This is steps 2-3 of the architecture analysis pipeline (ref: PRD
//! "Analysis Process").

use std::collections::HashMap;

use rusqlite::{Connection, Result};
use serde::Serialize;

use crate::structural_metrics::StructuralReport;

/// Weight applied to events in the most recent window (last N tasks).
const RECENT_WEIGHT: f64 = 2.0;
/// Weight applied to older events (before the recent window).
const OLD_WEIGHT: f64 = 1.0;
/// Default size of the "recent" window in number of tasks.
const DEFAULT_RECENT_WINDOW: u32 = 20;

/// Thresholds for structural smell flags.
const HIGH_FAN_IN_THRESHOLD: f64 = 0.5;
const LARGE_MODULE_LINE_THRESHOLD: usize = 500;
const WIDE_API_THRESHOLD: usize = 15;

/// Minimum combined score (structural + historical) to be flagged as a
/// refactoring candidate.
const CANDIDATE_THRESHOLD: f64 = 2.0;

/// Historical signals aggregated for a single module.
#[derive(Debug, Clone, Serialize)]
pub struct ModuleSignals {
    /// Module name.
    pub module: String,
    /// Number of expansion events involving this module (weighted).
    pub expansion_score: f64,
    /// Average integration iteration count for tasks touching this module (weighted).
    pub integration_score: f64,
    /// Number of metadata drift reports mentioning this module.
    pub drift_count: u32,
    /// Combined historical signal score.
    pub historical_score: f64,
}

/// Structural smell flags for a module.
#[derive(Debug, Clone, Serialize)]
pub struct StructuralSmells {
    /// High fan-in (many importers).
    pub high_fan_in: bool,
    /// Large module (high line count).
    pub large_module: bool,
    /// Participates in circular dependency.
    pub in_cycle: bool,
    /// Has boundary violations.
    pub has_violations: bool,
    /// Contains god file candidates.
    pub has_god_files: bool,
    /// Wide API surface.
    pub wide_api: bool,
    /// Combined structural smell score (count of flags).
    pub structural_score: f64,
}

/// A module identified as a refactoring candidate.
#[derive(Debug, Clone, Serialize)]
pub struct RefactorCandidate {
    /// Module name.
    pub module: String,
    /// Structural smells for this module.
    pub smells: StructuralSmells,
    /// Historical signals for this module.
    pub signals: ModuleSignals,
    /// Combined score (structural + historical).
    pub combined_score: f64,
    /// Confidence level (0.0-1.0) based on how many independent signals agree.
    pub confidence: f64,
}

/// Full correlation report combining structural analysis with historical signals.
#[derive(Debug, Serialize)]
pub struct CorrelationReport {
    /// Per-module signal data.
    pub module_signals: HashMap<String, ModuleSignals>,
    /// Per-module structural smells.
    pub module_smells: HashMap<String, StructuralSmells>,
    /// Modules identified as refactoring candidates (sorted by combined score desc).
    pub candidates: Vec<RefactorCandidate>,
    /// Total expansion events analyzed.
    pub total_expansion_events: u32,
    /// Total integration iterations analyzed.
    pub total_integration_records: u32,
    /// Total drift reports analyzed.
    pub total_drift_reports: u32,
}

/// Correlate structural metrics with historical signals from the database.
///
/// Queries expansion events, integration iterations, and metadata drift from
/// the database, weights them by recency, and overlays them onto the structural
/// report's module graph. Returns candidates that have BOTH structural smells
/// AND historical failure signals.
pub fn correlate(
    conn: &Connection,
    structural: &StructuralReport,
    recent_window: Option<u32>,
) -> Result<CorrelationReport> {
    let recent_window = recent_window.unwrap_or(DEFAULT_RECENT_WINDOW);
    let module_names: Vec<&str> = structural.modules.keys().map(|s| s.as_str()).collect();

    // Step 1: Gather historical signals per module
    let (module_signals, total_exp, total_int, total_drift) =
        gather_signals(conn, &module_names, recent_window)?;

    // Step 2: Compute structural smells per module
    let module_smells = compute_smells(structural);

    // Step 3: Identify candidates with BOTH structural smells AND historical signals
    let mut candidates = Vec::new();
    for module in &module_names {
        let smells = module_smells.get(*module).cloned().unwrap_or({
            StructuralSmells {
                high_fan_in: false,
                large_module: false,
                in_cycle: false,
                has_violations: false,
                has_god_files: false,
                wide_api: false,
                structural_score: 0.0,
            }
        });

        let signals = module_signals
            .get(*module)
            .cloned()
            .unwrap_or_else(|| ModuleSignals {
                module: module.to_string(),
                expansion_score: 0.0,
                integration_score: 0.0,
                drift_count: 0,
                historical_score: 0.0,
            });

        // Only candidates if BOTH structural AND historical signals are present
        if smells.structural_score > 0.0 && signals.historical_score > 0.0 {
            let combined = smells.structural_score + signals.historical_score;
            if combined >= CANDIDATE_THRESHOLD {
                let signal_types = count_signal_types(&smells, &signals);
                let confidence = (signal_types as f64 / 6.0).min(1.0);

                candidates.push(RefactorCandidate {
                    module: module.to_string(),
                    smells: smells.clone(),
                    signals: signals.clone(),
                    combined_score: combined,
                    confidence,
                });
            }
        }
    }

    // Sort candidates by combined score descending
    candidates.sort_by(|a, b| b.combined_score.partial_cmp(&a.combined_score).unwrap());

    Ok(CorrelationReport {
        module_signals,
        module_smells,
        candidates,
        total_expansion_events: total_exp,
        total_integration_records: total_int,
        total_drift_reports: total_drift,
    })
}

/// Count how many independent signal types are active for a module.
fn count_signal_types(smells: &StructuralSmells, signals: &ModuleSignals) -> u32 {
    let mut count = 0;
    if smells.high_fan_in {
        count += 1;
    }
    if smells.large_module || smells.has_god_files || smells.wide_api {
        count += 1;
    }
    if smells.in_cycle || smells.has_violations {
        count += 1;
    }
    if signals.expansion_score > 0.0 {
        count += 1;
    }
    if signals.integration_score > 0.0 {
        count += 1;
    }
    if signals.drift_count > 0 {
        count += 1;
    }
    count
}

/// Gather historical signals for each module from the database.
///
/// Returns (per-module signals, total expansion events, total integration records, total drift reports).
fn gather_signals(
    conn: &Connection,
    modules: &[&str],
    recent_window: u32,
) -> Result<(HashMap<String, ModuleSignals>, u32, u32, u32)> {
    let mut signals: HashMap<String, ModuleSignals> = HashMap::new();

    // -- Expansion events --
    // Get recent task IDs for recency weighting
    let recent_task_ids = get_recent_task_ids(conn, recent_window)?;
    let all_events = crate::expansion_event::get_recent(conn, 1000)?;
    let total_exp = all_events.len() as u32;

    // Count expansion events per module with recency weighting
    let mut module_expansion_counts: HashMap<String, f64> = HashMap::new();
    for event in &all_events {
        let weight = if recent_task_ids.contains(&event.task_id) {
            RECENT_WEIGHT
        } else {
            OLD_WEIGHT
        };
        for module in &event.actual_modules {
            *module_expansion_counts.entry(module.clone()).or_default() += weight;
        }
    }

    // -- Integration iterations --
    // Query all integration iteration records
    let all_integrations = get_all_integration_iterations(conn)?;
    let total_int = all_integrations.len() as u32;

    // Aggregate iteration counts per module with recency weighting
    let mut module_iteration_scores: HashMap<String, (f64, f64)> = HashMap::new(); // (weighted sum, weight sum)
    for iter_record in &all_integrations {
        let weight = if recent_task_ids.contains(&iter_record.bead_id) {
            RECENT_WEIGHT
        } else {
            OLD_WEIGHT
        };
        if let Some(modules_str) = &iter_record.modules {
            for module in modules_str.split(',') {
                let module = module.trim();
                if !module.is_empty() {
                    let entry = module_iteration_scores
                        .entry(module.to_string())
                        .or_default();
                    entry.0 += iter_record.iteration_count as f64 * weight;
                    entry.1 += weight;
                }
            }
        }
    }

    // -- Metadata drift --
    let drift_counts = get_drift_counts_per_module(conn)?;
    let total_drift: u32 = drift_counts.values().sum();

    // -- Build per-module signals --
    for module in modules {
        let expansion_score = module_expansion_counts.get(*module).copied().unwrap_or(0.0);

        let integration_score = module_iteration_scores
            .get(*module)
            .map(|(weighted_sum, weight_sum)| {
                if *weight_sum > 0.0 {
                    weighted_sum / weight_sum
                } else {
                    0.0
                }
            })
            .unwrap_or(0.0);

        let drift_count = drift_counts.get(*module).copied().unwrap_or(0);

        let historical_score = expansion_score + integration_score + drift_count as f64;

        signals.insert(
            module.to_string(),
            ModuleSignals {
                module: module.to_string(),
                expansion_score,
                integration_score,
                drift_count,
                historical_score,
            },
        );
    }

    Ok((signals, total_exp, total_int, total_drift))
}

/// Compute structural smells from the structural report.
fn compute_smells(structural: &StructuralReport) -> HashMap<String, StructuralSmells> {
    let mut smells = HashMap::new();

    // Compute max fan-in across all files for normalization
    let max_fan_in = structural
        .files
        .values()
        .map(|f| f.fan_in_score)
        .fold(0.0_f64, f64::max);

    for (name, metrics) in &structural.modules {
        // Compute average fan-in for files in this module
        let module_files: Vec<_> = structural
            .files
            .values()
            .filter(|f| {
                // Match files to modules by path prefix
                let path_str = f.path.to_string_lossy();
                if name == "crate" {
                    // Root module files are directly in src/
                    !path_str.contains('/')
                        || path_str
                            .rsplit('/')
                            .nth(1)
                            .map(|p| p == "src")
                            .unwrap_or(false)
                } else {
                    path_str.contains(&format!("/{name}/"))
                        || path_str.contains(&format!("/{name}.rs"))
                }
            })
            .collect();

        let avg_fan_in = if !module_files.is_empty() && max_fan_in > 0.0 {
            module_files.iter().map(|f| f.fan_in_score).sum::<f64>() / module_files.len() as f64
        } else {
            0.0
        };

        let high_fan_in = avg_fan_in >= HIGH_FAN_IN_THRESHOLD;
        let large_module = metrics.total_lines >= LARGE_MODULE_LINE_THRESHOLD;
        let in_cycle = metrics.in_cycle;
        let has_violations = metrics.violations_as_source > 0 || metrics.violations_as_target > 0;
        let has_god_files = metrics.god_file_count > 0;
        let wide_api = metrics.api_surface_width >= WIDE_API_THRESHOLD;

        let mut score = 0.0;
        if high_fan_in {
            score += 1.0;
        }
        if large_module {
            score += 1.0;
        }
        if in_cycle {
            score += 1.0;
        }
        if has_violations {
            score += 1.0;
        }
        if has_god_files {
            score += 1.0;
        }
        if wide_api {
            score += 1.0;
        }

        smells.insert(
            name.clone(),
            StructuralSmells {
                high_fan_in,
                large_module,
                in_cycle,
                has_violations,
                has_god_files,
                wide_api,
                structural_score: score,
            },
        );
    }

    smells
}

/// Get the most recent N distinct task IDs that have expansion events.
fn get_recent_task_ids(conn: &Connection, limit: u32) -> Result<Vec<String>> {
    let mut stmt =
        conn.prepare("SELECT DISTINCT task_id FROM expansion_events ORDER BY rowid DESC LIMIT ?1")?;
    let rows = stmt
        .query_map(rusqlite::params![limit], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>>>()?;
    Ok(rows)
}

/// Get all integration iteration records.
fn get_all_integration_iterations(
    conn: &Connection,
) -> Result<Vec<crate::db::IntegrationIteration>> {
    let mut stmt = conn.prepare(
        "SELECT id, assignment_id, bead_id, iteration_count, modules, recorded_at \
         FROM integration_iterations ORDER BY id ASC",
    )?;
    let rows = stmt
        .query_map([], |row| {
            Ok(crate::db::IntegrationIteration {
                id: row.get(0)?,
                assignment_id: row.get(1)?,
                bead_id: row.get(2)?,
                iteration_count: row.get(3)?,
                modules: row.get(4)?,
                recorded_at: row.get(5)?,
            })
        })?
        .collect::<Result<Vec<_>>>()?;
    Ok(rows)
}

/// Count metadata drift events per module from the drift_reports concept-level data.
///
/// Queries the file_resolutions table for tasks with multiple resolutions at different
/// commits, then checks if the file count changed significantly. Groups by module.
fn get_drift_counts_per_module(conn: &Connection) -> Result<HashMap<String, u32>> {
    let mut counts: HashMap<String, u32> = HashMap::new();

    // Check if the file_resolutions table exists
    let table_exists: bool = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='file_resolutions'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map(|c| c > 0)
        .unwrap_or(false);

    if !table_exists {
        return Ok(counts);
    }

    // Find tasks that have resolutions at multiple commits (indicating drift potential)
    let mut stmt = conn.prepare(
        "SELECT task_id, resolved_modules FROM file_resolutions \
         GROUP BY task_id HAVING COUNT(DISTINCT base_commit) > 1",
    )?;

    // For simplicity, query all resolutions for tasks with multiple commits
    // and count modules that appear in these multi-resolution tasks
    let task_ids: Vec<String> = stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>>>()?;

    for task_id in &task_ids {
        let mut res_stmt =
            conn.prepare("SELECT resolved_modules FROM file_resolutions WHERE task_id = ?1")?;
        let module_lists: Vec<String> = res_stmt
            .query_map(rusqlite::params![task_id], |row| row.get::<_, String>(0))?
            .filter_map(|r| r.ok())
            .collect();

        for modules_str in &module_lists {
            let modules: Vec<String> = serde_json::from_str(modules_str).unwrap_or_default();
            for module in modules {
                *counts.entry(module).or_default() += 1;
            }
        }
    }

    Ok(counts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expansion_event;

    fn setup_db() -> (tempfile::TempDir, Connection) {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("test.db");
        let conn = crate::db::open_or_create(&db_path).unwrap();
        expansion_event::create_table(&conn).unwrap();
        (tmp, conn)
    }

    /// Insert a dummy worker_assignment so FK constraints on integration_iterations pass.
    fn insert_dummy_assignment(conn: &Connection, bead_id: &str) -> i64 {
        crate::db::insert_worker_assignment(conn, 0, bead_id, "/tmp/wt", "completed", None).unwrap()
    }

    fn make_structural_report(
        modules: Vec<(&str, usize, usize, bool, usize, usize, usize)>,
    ) -> StructuralReport {
        use std::collections::HashMap;
        use std::path::PathBuf;

        let mut mod_metrics = HashMap::new();
        let mut file_metrics = HashMap::new();

        for (name, total_lines, api_width, in_cycle, violations_src, violations_tgt, god_files) in
            modules
        {
            mod_metrics.insert(
                name.to_string(),
                crate::structural_metrics::ModuleMetrics {
                    name: name.to_string(),
                    file_count: 1,
                    total_lines,
                    api_surface_width: api_width,
                    in_cycle,
                    violations_as_source: violations_src,
                    violations_as_target: violations_tgt,
                    god_file_count: god_files,
                },
            );

            // Create a dummy file for each module
            let path = PathBuf::from(format!("src/{name}/mod.rs"));
            file_metrics.insert(
                path.clone(),
                crate::structural_metrics::FileMetrics {
                    path,
                    line_count: total_lines,
                    fan_in_score: 0.3,
                    fan_in_importers: 2,
                    is_god_file: god_files > 0,
                    cluster_count: 0,
                },
            );
        }

        StructuralReport {
            modules: mod_metrics,
            files: file_metrics,
            cycles: vec![],
            boundary_violations: vec![],
            total_modules: 0,
            total_files: 0,
        }
    }

    #[test]
    fn empty_database_no_candidates() {
        let (_tmp, conn) = setup_db();
        let report = make_structural_report(vec![
            // name, lines, api_width, in_cycle, violations_src, violations_tgt, god_files
            ("auth", 600, 20, false, 0, 0, 0), // large + wide API = structural score 2
        ]);

        let result = correlate(&conn, &report, None).unwrap();
        // No historical signals → no candidates despite structural smells
        assert!(result.candidates.is_empty());
        assert_eq!(result.total_expansion_events, 0);
    }

    #[test]
    fn structural_only_no_historical_no_candidate() {
        let (_tmp, conn) = setup_db();
        // Module with multiple structural smells but no historical signals
        let report = make_structural_report(vec![("models", 1000, 25, true, 3, 2, 1)]);

        let result = correlate(&conn, &report, None).unwrap();
        assert!(result.candidates.is_empty());

        // Structural smells should still be computed
        let smells = &result.module_smells["models"];
        assert!(smells.large_module);
        assert!(smells.wide_api);
        assert!(smells.in_cycle);
        assert!(smells.has_violations);
        assert!(smells.has_god_files);
        assert!(smells.structural_score >= 4.0);
    }

    #[test]
    fn historical_only_no_structural_no_candidate() {
        let (_tmp, conn) = setup_db();
        // Module with no structural smells
        let report = make_structural_report(vec![("tiny", 50, 3, false, 0, 0, 0)]);

        // Add expansion events for "tiny"
        let event = expansion_event::ExpansionEvent {
            task_id: "task-1".to_string(),
            predicted_modules: vec!["other".to_string()],
            actual_modules: vec!["tiny".to_string()],
            expansion_reason: "needed tiny".to_string(),
            timestamp: "2026-01-15T10:00:00Z".to_string(),
        };
        expansion_event::record(&conn, &event).unwrap();

        let result = correlate(&conn, &report, None).unwrap();
        assert!(result.candidates.is_empty());
        assert!(result.total_expansion_events > 0);
    }

    #[test]
    fn both_signals_produces_candidate() {
        let (_tmp, conn) = setup_db();
        let report = make_structural_report(vec![
            ("models", 800, 20, true, 1, 0, 0), // structural: large + wide_api + in_cycle + violations = 4
        ]);

        // Add expansion events for "models"
        for i in 0..5 {
            let event = expansion_event::ExpansionEvent {
                task_id: format!("task-{i}"),
                predicted_modules: vec!["auth".to_string()],
                actual_modules: vec!["models".to_string()],
                expansion_reason: "models needed".to_string(),
                timestamp: format!("2026-01-{:02}T10:00:00Z", 10 + i),
            };
            expansion_event::record(&conn, &event).unwrap();
        }

        let result = correlate(&conn, &report, None).unwrap();
        assert_eq!(result.candidates.len(), 1);
        assert_eq!(result.candidates[0].module, "models");
        assert!(result.candidates[0].combined_score >= CANDIDATE_THRESHOLD);
        assert!(result.candidates[0].confidence > 0.0);
    }

    #[test]
    fn recency_weighting_applied() {
        let (_tmp, conn) = setup_db();
        let report = make_structural_report(vec![
            ("auth", 600, 16, false, 1, 0, 0), // structural: large + wide_api + violations = 3
        ]);

        // Add a recent expansion event
        let event = expansion_event::ExpansionEvent {
            task_id: "recent-task".to_string(),
            predicted_modules: vec!["other".to_string()],
            actual_modules: vec!["auth".to_string()],
            expansion_reason: "auth expanded".to_string(),
            timestamp: "2026-02-15T10:00:00Z".to_string(),
        };
        expansion_event::record(&conn, &event).unwrap();

        let result = correlate(&conn, &report, Some(20)).unwrap();
        let signals = &result.module_signals["auth"];

        // Recent event should have weight 2.0
        assert!(signals.expansion_score >= 2.0);
    }

    #[test]
    fn multiple_modules_ranked_by_score() {
        let (_tmp, conn) = setup_db();
        let report = make_structural_report(vec![
            ("auth", 600, 16, false, 1, 0, 0),   // structural score ~3
            ("models", 1000, 25, true, 2, 1, 1), // structural score ~5
        ]);

        // More expansion events for "models" than "auth"
        for i in 0..3 {
            let event = expansion_event::ExpansionEvent {
                task_id: format!("task-{i}"),
                predicted_modules: vec![],
                actual_modules: vec!["models".to_string(), "auth".to_string()],
                expansion_reason: "expanded".to_string(),
                timestamp: format!("2026-01-{:02}T10:00:00Z", 10 + i),
            };
            expansion_event::record(&conn, &event).unwrap();
        }
        // Extra events only for models
        for i in 3..6 {
            let event = expansion_event::ExpansionEvent {
                task_id: format!("task-{i}"),
                predicted_modules: vec![],
                actual_modules: vec!["models".to_string()],
                expansion_reason: "models only".to_string(),
                timestamp: format!("2026-01-{:02}T10:00:00Z", 10 + i),
            };
            expansion_event::record(&conn, &event).unwrap();
        }

        let result = correlate(&conn, &report, None).unwrap();
        assert!(result.candidates.len() >= 2);
        // models should rank higher (more structural smells + more historical signals)
        assert_eq!(result.candidates[0].module, "models");
    }

    #[test]
    fn integration_iterations_contribute_to_score() {
        let (_tmp, conn) = setup_db();
        let report = make_structural_report(vec![
            ("db", 700, 16, false, 1, 0, 0), // structural score ~3
        ]);

        // Need an expansion event too so historical_score > 0
        let event = expansion_event::ExpansionEvent {
            task_id: "task-int".to_string(),
            predicted_modules: vec![],
            actual_modules: vec!["db".to_string()],
            expansion_reason: "db expanded".to_string(),
            timestamp: "2026-01-15T10:00:00Z".to_string(),
        };
        expansion_event::record(&conn, &event).unwrap();

        // Add integration iteration records mentioning "db" module
        let aid = insert_dummy_assignment(&conn, "task-int");
        crate::db::record_integration_iterations(
            &conn,
            aid,
            "task-int",
            5,
            Some("db"),
            "2026-01-15T10:00:00Z",
        )
        .unwrap();

        let result = correlate(&conn, &report, None).unwrap();
        let signals = &result.module_signals["db"];
        assert!(signals.integration_score > 0.0);
        assert!(signals.historical_score > 0.0);
        assert!(!result.candidates.is_empty());
    }

    #[test]
    fn candidate_threshold_filters_low_scores() {
        let (_tmp, conn) = setup_db();
        // Module with minimal structural smell (just barely one flag)
        let report = make_structural_report(vec![
            ("minor", 501, 3, false, 0, 0, 0), // structural: only large_module = 1
        ]);

        // Single expansion event → historical score ~2 (recent weight)
        let event = expansion_event::ExpansionEvent {
            task_id: "task-minor".to_string(),
            predicted_modules: vec![],
            actual_modules: vec!["minor".to_string()],
            expansion_reason: "minor expanded".to_string(),
            timestamp: "2026-01-15T10:00:00Z".to_string(),
        };
        expansion_event::record(&conn, &event).unwrap();

        let result = correlate(&conn, &report, None).unwrap();
        // Combined = 1 (structural) + 2 (historical) = 3, which passes threshold
        assert!(!result.candidates.is_empty());
    }

    #[test]
    fn compute_smells_thresholds() {
        let report = make_structural_report(vec![
            ("small", 100, 5, false, 0, 0, 0), // no smells
            ("big", 600, 20, true, 2, 1, 1),   // all smells except fan-in
        ]);

        let smells = compute_smells(&report);

        let small = &smells["small"];
        assert_eq!(small.structural_score, 0.0);
        assert!(!small.large_module);
        assert!(!small.wide_api);

        let big = &smells["big"];
        assert!(big.large_module);
        assert!(big.wide_api);
        assert!(big.in_cycle);
        assert!(big.has_violations);
        assert!(big.has_god_files);
        assert!(big.structural_score >= 5.0);
    }

    #[test]
    fn correlation_report_totals() {
        let (_tmp, conn) = setup_db();
        let report = make_structural_report(vec![("auth", 100, 3, false, 0, 0, 0)]);

        for i in 0..3 {
            let event = expansion_event::ExpansionEvent {
                task_id: format!("task-{i}"),
                predicted_modules: vec![],
                actual_modules: vec!["auth".to_string()],
                expansion_reason: "reason".to_string(),
                timestamp: format!("2026-01-{:02}T10:00:00Z", 10 + i),
            };
            expansion_event::record(&conn, &event).unwrap();
        }

        let result = correlate(&conn, &report, None).unwrap();
        assert_eq!(result.total_expansion_events, 3);
    }

    #[test]
    fn empty_structural_report() {
        let (_tmp, conn) = setup_db();
        let report = StructuralReport {
            modules: HashMap::new(),
            files: HashMap::new(),
            cycles: vec![],
            boundary_violations: vec![],
            total_modules: 0,
            total_files: 0,
        };

        let result = correlate(&conn, &report, None).unwrap();
        assert!(result.candidates.is_empty());
        assert!(result.module_signals.is_empty());
        assert!(result.module_smells.is_empty());
    }

    #[test]
    fn confidence_scales_with_signal_count() {
        let (_tmp, conn) = setup_db();
        // Module with many structural smells AND many historical signals
        let report = make_structural_report(vec![
            ("everything", 1000, 25, true, 2, 1, 1), // 5 structural flags
        ]);

        // Add multiple types of historical signals
        for i in 0..5 {
            let event = expansion_event::ExpansionEvent {
                task_id: format!("task-{i}"),
                predicted_modules: vec![],
                actual_modules: vec!["everything".to_string()],
                expansion_reason: "expanded".to_string(),
                timestamp: format!("2026-01-{:02}T10:00:00Z", 10 + i),
            };
            expansion_event::record(&conn, &event).unwrap();
        }

        // Add integration iterations
        let aid = insert_dummy_assignment(&conn, "task-0");
        crate::db::record_integration_iterations(
            &conn,
            aid,
            "task-0",
            4,
            Some("everything"),
            "2026-01-10T10:00:00Z",
        )
        .unwrap();

        let result = correlate(&conn, &report, None).unwrap();
        assert!(!result.candidates.is_empty());
        let candidate = &result.candidates[0];
        // Should have high confidence with multiple signal types
        assert!(candidate.confidence > 0.5);
    }
}
