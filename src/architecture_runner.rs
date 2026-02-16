//! Architecture agent runner with trigger logic.
//!
//! Monitors task completion events and decides when to run the architecture
//! analysis pipeline. Three trigger modes:
//!
//! 1. **Periodic** — after every N task completions (configurable, default 10).
//! 2. **Threshold** — when rolling average of expansion events or integration
//!    iterations exceeds a configurable threshold.
//! 3. **Pre-planning** — explicit call with a feature description before
//!    decomposing a large feature.
//!
//! PRD ref: 'Analysis Process' triggers section.

use std::path::Path;

use rusqlite::Connection;

use crate::architecture_pipeline::{self, PipelineReport};
use crate::config::ArchitectureConfig;
use crate::expansion_event;

/// Context passed to `run_if_needed` describing what triggered the check.
#[derive(Debug)]
pub enum TriggerContext {
    /// A task was successfully integrated. Contains the total completed count
    /// since the runner was created.
    TaskCompleted { completed_count: u32 },
    /// Explicit pre-planning trigger with a feature description.
    PrePlanning { feature_description: String },
}

/// Result from the architecture runner indicating what happened.
#[derive(Debug)]
pub enum RunOutcome {
    /// Pipeline was triggered and ran.
    Ran(PipelineReport),
    /// No trigger condition was met; pipeline was not run.
    Skipped { reason: String },
}

/// Architecture agent runner that decides when to trigger the analysis pipeline.
pub struct ArchitectureRunner {
    /// Number of task completions since the last architecture review.
    completions_since_last_run: u32,
    /// How often (in task completions) to run the periodic trigger.
    review_interval: u32,
    /// Expansion event threshold: if recent expansions in the window exceed
    /// this count, trigger a review.
    expansion_threshold: u32,
    /// Window size (in tasks) over which expansion events are counted.
    expansion_window: u32,
    /// Integration iteration threshold: if average iterations across recent
    /// tasks exceed this value, trigger a review.
    integration_iteration_threshold: f64,
}

impl ArchitectureRunner {
    /// Create a new runner from architecture config.
    pub fn new(config: &ArchitectureConfig) -> Self {
        Self {
            completions_since_last_run: 0,
            review_interval: config.arch_review_interval,
            expansion_threshold: config.expansion_event_threshold,
            expansion_window: config.expansion_event_window,
            integration_iteration_threshold: config.integration_iteration_threshold,
        }
    }

    /// Check whether the architecture pipeline should run, and run it if so.
    ///
    /// Called by the coordinator after each successful task integration.
    pub fn run_if_needed(
        &mut self,
        trigger: TriggerContext,
        repo_root: &Path,
        db_conn: &Connection,
        config: &ArchitectureConfig,
    ) -> RunOutcome {
        match trigger {
            TriggerContext::TaskCompleted { completed_count } => {
                self.completions_since_last_run = completed_count;
                self.evaluate_and_run(repo_root, db_conn, config)
            }
            TriggerContext::PrePlanning {
                feature_description,
            } => {
                tracing::info!(
                    feature = %feature_description,
                    "architecture runner: pre-planning trigger, running pipeline"
                );
                self.completions_since_last_run = 0;
                let report =
                    architecture_pipeline::run_architecture_pipeline(repo_root, db_conn, config);
                log_report_summary(&report);
                RunOutcome::Ran(report)
            }
        }
    }

    /// Evaluate periodic and threshold triggers, running the pipeline if any fire.
    fn evaluate_and_run(
        &mut self,
        repo_root: &Path,
        db_conn: &Connection,
        config: &ArchitectureConfig,
    ) -> RunOutcome {
        // Check periodic trigger
        if self.completions_since_last_run >= self.review_interval && self.review_interval > 0 {
            tracing::info!(
                completions = self.completions_since_last_run,
                interval = self.review_interval,
                "architecture runner: periodic trigger fired"
            );
            self.completions_since_last_run = 0;
            let report =
                architecture_pipeline::run_architecture_pipeline(repo_root, db_conn, config);
            log_report_summary(&report);
            return RunOutcome::Ran(report);
        }

        // Check threshold trigger: expansion events
        if let Ok(recent_expansions) =
            expansion_event::count_recent_expansions(db_conn, self.expansion_window)
        {
            if recent_expansions >= self.expansion_threshold {
                tracing::info!(
                    recent_expansions,
                    threshold = self.expansion_threshold,
                    window = self.expansion_window,
                    "architecture runner: expansion event threshold trigger fired"
                );
                self.completions_since_last_run = 0;
                let report =
                    architecture_pipeline::run_architecture_pipeline(repo_root, db_conn, config);
                log_report_summary(&report);
                return RunOutcome::Ran(report);
            }
        }

        // Check threshold trigger: integration iteration average
        if let Ok(avg) = average_recent_iterations(db_conn, self.expansion_window) {
            if avg >= self.integration_iteration_threshold {
                tracing::info!(
                    avg_iterations = avg,
                    threshold = self.integration_iteration_threshold,
                    "architecture runner: integration iteration threshold trigger fired"
                );
                self.completions_since_last_run = 0;
                let report =
                    architecture_pipeline::run_architecture_pipeline(repo_root, db_conn, config);
                log_report_summary(&report);
                return RunOutcome::Ran(report);
            }
        }

        RunOutcome::Skipped {
            reason: format!(
                "no trigger fired (completions: {}/{}, below thresholds)",
                self.completions_since_last_run, self.review_interval
            ),
        }
    }
}

/// Compute average integration iterations across the most recent N tasks.
fn average_recent_iterations(conn: &Connection, window: u32) -> rusqlite::Result<f64> {
    // Get the average iteration count from the most recent `window` integration records
    let avg: f64 = conn.query_row(
        "SELECT COALESCE(AVG(iteration_count), 0.0) FROM (
             SELECT iteration_count FROM integration_iterations
             ORDER BY id DESC LIMIT ?1
         )",
        rusqlite::params![window],
        |row| row.get(0),
    )?;
    Ok(avg)
}

/// Log a summary of the pipeline report.
fn log_report_summary(report: &PipelineReport) {
    let passed = report
        .validated_proposals
        .iter()
        .filter(|v| v.passed)
        .count();
    let failed = report.validated_proposals.len() - passed;
    tracing::info!(
        proposals = report.proposals.len(),
        validated_passed = passed,
        validated_failed = failed,
        candidates = report.correlation_report.candidates.len(),
        "architecture runner: pipeline complete"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ArchitectureConfig;

    fn setup_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS integration_iterations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                assignment_id INTEGER,
                bead_id TEXT,
                iteration_count INTEGER,
                modules TEXT,
                recorded_at TEXT
            );
            CREATE TABLE IF NOT EXISTS expansion_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id TEXT NOT NULL,
                predicted_modules TEXT NOT NULL,
                actual_modules TEXT NOT NULL,
                expansion_reason TEXT NOT NULL,
                timestamp TEXT NOT NULL
            );",
        )
        .unwrap();
        conn
    }

    fn test_config() -> ArchitectureConfig {
        let mut config = ArchitectureConfig::default();
        config.arch_review_interval = 3;
        config.expansion_event_threshold = 2;
        config.expansion_event_window = 10;
        config.integration_iteration_threshold = 3.0;
        config
    }

    #[test]
    fn periodic_trigger_fires_at_threshold() {
        let conn = setup_db();
        let dir = tempfile::tempdir().unwrap();
        let config = test_config();
        let mut runner = ArchitectureRunner::new(&config);

        // Below threshold — should skip
        let outcome = runner.run_if_needed(
            TriggerContext::TaskCompleted { completed_count: 2 },
            dir.path(),
            &conn,
            &config,
        );
        assert!(matches!(outcome, RunOutcome::Skipped { .. }));

        // At threshold — should run
        let outcome = runner.run_if_needed(
            TriggerContext::TaskCompleted { completed_count: 3 },
            dir.path(),
            &conn,
            &config,
        );
        assert!(matches!(outcome, RunOutcome::Ran(_)));
    }

    #[test]
    fn threshold_trigger_expansion_events() {
        let conn = setup_db();
        let dir = tempfile::tempdir().unwrap();
        let config = test_config();
        let mut runner = ArchitectureRunner::new(&config);

        // Insert enough expansion events to trigger
        for i in 0..3 {
            conn.execute(
                "INSERT INTO expansion_events (task_id, predicted_modules, actual_modules, expansion_reason, timestamp)
                 VALUES (?1, '[]', '[\"mod_a\"]', 'reason', '2026-01-01T00:00:00Z')",
                rusqlite::params![format!("task-{i}")],
            )
            .unwrap();
        }

        // Completed count below periodic threshold but expansion events above threshold
        let outcome = runner.run_if_needed(
            TriggerContext::TaskCompleted { completed_count: 1 },
            dir.path(),
            &conn,
            &config,
        );
        assert!(matches!(outcome, RunOutcome::Ran(_)));
    }

    #[test]
    fn threshold_trigger_integration_iterations() {
        let conn = setup_db();
        let dir = tempfile::tempdir().unwrap();
        let config = test_config();
        let mut runner = ArchitectureRunner::new(&config);

        // Insert high iteration counts
        for i in 0..5 {
            conn.execute(
                "INSERT INTO integration_iterations (assignment_id, bead_id, iteration_count, modules, recorded_at)
                 VALUES (?1, 'bead-x', 5, 'mod_a', '2026-01-01T00:00:00Z')",
                rusqlite::params![i],
            )
            .unwrap();
        }

        // Average iterations = 5.0, threshold = 3.0 — should trigger
        let outcome = runner.run_if_needed(
            TriggerContext::TaskCompleted { completed_count: 1 },
            dir.path(),
            &conn,
            &config,
        );
        assert!(matches!(outcome, RunOutcome::Ran(_)));
    }

    #[test]
    fn no_trigger_when_below_all_thresholds() {
        let conn = setup_db();
        let dir = tempfile::tempdir().unwrap();
        let config = test_config();
        let mut runner = ArchitectureRunner::new(&config);

        let outcome = runner.run_if_needed(
            TriggerContext::TaskCompleted { completed_count: 1 },
            dir.path(),
            &conn,
            &config,
        );
        assert!(matches!(outcome, RunOutcome::Skipped { .. }));

        if let RunOutcome::Skipped { reason } = outcome {
            assert!(reason.contains("no trigger fired"));
        }
    }

    #[test]
    fn pre_planning_trigger_always_runs() {
        let conn = setup_db();
        let dir = tempfile::tempdir().unwrap();
        let config = test_config();
        let mut runner = ArchitectureRunner::new(&config);

        let outcome = runner.run_if_needed(
            TriggerContext::PrePlanning {
                feature_description: "Add user authentication".to_string(),
            },
            dir.path(),
            &conn,
            &config,
        );
        assert!(matches!(outcome, RunOutcome::Ran(_)));
    }

    #[test]
    fn average_recent_iterations_empty_db() {
        let conn = setup_db();
        let avg = average_recent_iterations(&conn, 20).unwrap();
        assert!((avg - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn average_recent_iterations_with_data() {
        let conn = setup_db();
        // Insert iterations: 2, 4, 6 → avg = 4.0
        for (i, count) in [2, 4, 6].iter().enumerate() {
            conn.execute(
                "INSERT INTO integration_iterations (assignment_id, bead_id, iteration_count, modules, recorded_at)
                 VALUES (?1, 'bead-y', ?2, NULL, '2026-01-01T00:00:00Z')",
                rusqlite::params![i, count],
            )
            .unwrap();
        }
        let avg = average_recent_iterations(&conn, 20).unwrap();
        assert!((avg - 4.0).abs() < f64::EPSILON);
    }
}
