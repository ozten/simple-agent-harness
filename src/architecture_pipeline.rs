//! End-to-end architecture analysis pipeline.
//!
//! Connects the analysis steps into a single `run_architecture_pipeline()` function:
//!   1. Structural analysis (`structural_metrics::analyze`)
//!   2. Signal correlation (`signal_correlator::correlate`)
//!   3. Proposal generation (`proposal_generation::generate_proposals`)
//!   4. Proposal validation (`proposal_validation::validate_proposal`)
//!
//! Callable from both the CLI analyze command and the architecture agent runner.
//! PRD ref: 'Analysis Process' stages 1-4 and 'Proposal Validation' section.

use std::collections::HashMap;
use std::path::Path;

use rusqlite::Connection;

use crate::config::ArchitectureConfig;
use crate::import_graph;
use crate::module_detect;
use crate::proposal_generation::{self, RefactorProposal as GenProposal};
use crate::proposal_validation::{
    self, ProposalKind as ValKind, RefactorProposal as ValProposal, ValidationResult,
};
use crate::public_api;
use crate::signal_correlator::{self, CorrelationReport};
use crate::structural_metrics::{self, StructuralReport};

/// The complete output of an architecture analysis pipeline run.
#[derive(Debug)]
pub struct PipelineReport {
    /// Structural analysis results (fan-in, god files, cycles, violations).
    pub structural_report: StructuralReport,
    /// Signal correlation results (historical signals overlaid on structure).
    pub correlation_report: CorrelationReport,
    /// Generated refactoring proposals (before validation).
    pub proposals: Vec<GenProposal>,
    /// Validation results for each proposal.
    pub validated_proposals: Vec<ValidationResult>,
}

/// Run the full architecture analysis pipeline.
///
/// Steps:
/// 1. Structural analysis on the repo
/// 2. Correlate structural metrics with historical signals from the DB
/// 3. Generate refactoring proposals from candidates
/// 4. Validate each proposal
///
/// Failed validations are logged but do not halt the pipeline.
pub fn run_architecture_pipeline(
    repo_root: &Path,
    db_conn: &Connection,
    config: &ArchitectureConfig,
) -> PipelineReport {
    // Step 1: Structural analysis
    tracing::info!("architecture pipeline: running structural analysis");
    let structural_report = structural_metrics::analyze(repo_root);
    tracing::info!(
        modules = structural_report.total_modules,
        files = structural_report.total_files,
        cycles = structural_report.cycles.len(),
        violations = structural_report.boundary_violations.len(),
        "architecture pipeline: structural analysis complete"
    );

    // Step 2: Signal correlation
    tracing::info!("architecture pipeline: correlating signals");
    let correlation_report = match signal_correlator::correlate(
        db_conn,
        &structural_report,
        Some(config.expansion_event_window),
    ) {
        Ok(report) => {
            tracing::info!(
                candidates = report.candidates.len(),
                expansion_events = report.total_expansion_events,
                "architecture pipeline: signal correlation complete"
            );
            report
        }
        Err(e) => {
            tracing::warn!(error = %e, "architecture pipeline: signal correlation failed, using empty report");
            CorrelationReport {
                module_signals: HashMap::new(),
                module_smells: HashMap::new(),
                candidates: Vec::new(),
                total_expansion_events: 0,
                total_integration_records: 0,
                total_drift_reports: 0,
            }
        }
    };

    // Step 3: Generate proposals
    tracing::info!("architecture pipeline: generating proposals");
    let proposals = proposal_generation::generate_proposals(&correlation_report);
    tracing::info!(
        count = proposals.len(),
        "architecture pipeline: proposal generation complete"
    );

    // Step 4: Validate proposals
    tracing::info!("architecture pipeline: validating proposals");
    let validated_proposals =
        validate_all_proposals(repo_root, db_conn, &proposals, &correlation_report);
    tracing::info!(
        total = validated_proposals.len(),
        "architecture pipeline: validation complete"
    );

    PipelineReport {
        structural_report,
        correlation_report,
        proposals,
        validated_proposals,
    }
}

/// Run proposal validation on all proposals, logging failures but not halting.
fn validate_all_proposals(
    repo_root: &Path,
    db_conn: &Connection,
    proposals: &[GenProposal],
    correlation_report: &CorrelationReport,
) -> Vec<ValidationResult> {
    if proposals.is_empty() {
        return Vec::new();
    }

    // Build the foundational data structures needed for validation
    let import_graph_data = import_graph::build_import_graph(repo_root);
    let modules = module_detect::detect_modules_from_repo(repo_root);
    let apis = public_api::extract_public_apis(&modules);

    let mut results = Vec::new();

    for gen_proposal in proposals {
        // Convert proposal_generation::RefactorProposal → proposal_validation::RefactorProposal
        let candidate = match correlation_report
            .candidates
            .iter()
            .find(|c| c.module == gen_proposal.target)
        {
            Some(c) => c.clone(),
            None => continue, // Should not happen, but skip gracefully
        };

        let val_proposal = ValProposal {
            kind: convert_kind(&gen_proposal.kind),
            target_module: gen_proposal.target.clone(),
            candidate,
            proposed_modules: Vec::new(), // Auto-populated by validation
            affected_files: modules
                .get(&gen_proposal.target)
                .map(|m| m.files.clone())
                .unwrap_or_default(),
        };

        let result = proposal_validation::validate_proposal(
            &val_proposal,
            &import_graph_data,
            &modules,
            &apis,
            db_conn,
            repo_root,
        );

        let errors = result.error_count();
        let warnings = result.warning_count();
        if errors > 0 {
            tracing::warn!(
                target_module = %gen_proposal.target,
                errors,
                warnings,
                "architecture pipeline: proposal has validation errors"
            );
        } else {
            tracing::info!(
                target_module = %gen_proposal.target,
                warnings,
                passed = result.passed,
                "architecture pipeline: proposal validated"
            );
        }

        results.push(result);
    }

    results
}

/// Convert from proposal_generation::RefactorKind to proposal_validation::ProposalKind.
fn convert_kind(kind: &crate::proposal_generation::RefactorKind) -> ValKind {
    match kind {
        crate::proposal_generation::RefactorKind::SplitModule => ValKind::SplitModule,
        crate::proposal_generation::RefactorKind::ExtractInterface => ValKind::ExtractInterface,
        crate::proposal_generation::RefactorKind::MoveFiles => ValKind::MoveFiles,
        crate::proposal_generation::RefactorKind::BreakCycle => ValKind::BreakCycle,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn architecture_pipeline_empty_repo() {
        let dir = TempDir::new().unwrap();
        let conn = Connection::open_in_memory().unwrap();
        // Set up tables needed by signal_correlator
        conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS integration_iterations (
                id INTEGER PRIMARY KEY, assignment_id INTEGER, bead_id TEXT,
                iteration_count INTEGER, modules TEXT, recorded_at TEXT);
            CREATE TABLE IF NOT EXISTS expansion_events (
                id INTEGER PRIMARY KEY, task_id TEXT, predicted_modules TEXT,
                actual_modules TEXT, expansion_reason TEXT, timestamp TEXT);",
            )
            .unwrap();

        let config = ArchitectureConfig::default();
        let report = run_architecture_pipeline(dir.path(), &conn, &config);

        // Empty repo should produce empty results
        assert!(report.proposals.is_empty());
        assert!(report.validated_proposals.is_empty());
    }

    #[test]
    fn convert_kind_mapping() {
        use crate::proposal_generation::RefactorKind;
        assert_eq!(convert_kind(&RefactorKind::SplitModule), ValKind::SplitModule);
        assert_eq!(
            convert_kind(&RefactorKind::ExtractInterface),
            ValKind::ExtractInterface
        );
        assert_eq!(convert_kind(&RefactorKind::MoveFiles), ValKind::MoveFiles);
        assert_eq!(convert_kind(&RefactorKind::BreakCycle), ValKind::BreakCycle);
    }
}
