//! Refactor proposal generation from architecture analysis.
//!
//! For each candidate module identified by the signal correlator, generates a
//! specific refactor proposal. Each proposal includes the kind of refactoring,
//! priority, confidence score, target module with current metrics, evidence
//! from historical signals, and a concrete recommendation with suggested new
//! structure.
//!
//! This is step 4 of the architecture analysis pipeline (ref: PRD section
//! "Proposal Format").

use crate::signal_correlator::{CorrelationReport, RefactorCandidate};

/// Minimum confidence score (0.0–1.0) for a candidate to produce a proposal.
const CANDIDATE_THRESHOLD: f64 = 0.6;

/// The kind of refactoring being proposed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RefactorKind {
    /// Split a large module into two or more smaller modules.
    SplitModule,
    /// Extract a common interface / trait from a module.
    ExtractInterface,
    /// Move files between modules to fix boundary violations.
    MoveFiles,
    /// Break a circular dependency by restructuring imports.
    BreakCycle,
}

/// Evidence supporting a refactoring proposal, drawn from historical signals
/// and structural analysis.
#[derive(Debug, Clone)]
pub struct ProposalEvidence {
    /// Number of expansion events involving this module (weighted by recency).
    pub expansion_events: f64,
    /// Average integration iteration count for tasks touching this module.
    pub avg_integration_iterations: f64,
    /// Number of entangled rollbacks (approximated by drift count).
    pub entangled_rollbacks: u32,
    /// Number of metadata drift reports mentioning this module.
    pub metadata_drift: u32,
}

/// A refactoring proposal generated from architecture analysis.
#[derive(Debug, Clone)]
pub struct RefactorProposal {
    /// The kind of refactoring being proposed.
    pub kind: RefactorKind,
    /// Priority level (lower = higher priority). Derived from combined score.
    pub priority: u8,
    /// Confidence score (0.0–1.0) based on how many independent signals agree.
    pub confidence: f64,
    /// Target file/module path.
    pub target: String,
    /// Evidence from historical signals supporting this proposal.
    pub evidence: ProposalEvidence,
    /// Human-readable recommendation with suggested new structure.
    pub recommendation: String,
}

/// Generate refactor proposals from a correlation report.
///
/// For each candidate in the report whose confidence score meets or exceeds
/// `CANDIDATE_THRESHOLD` (0.6), produces a `RefactorProposal` with the
/// appropriate kind, priority, evidence, and a concrete recommendation.
///
/// Returns proposals sorted by confidence score descending.
pub fn generate_proposals(report: &CorrelationReport) -> Vec<RefactorProposal> {
    let mut proposals: Vec<RefactorProposal> = report
        .candidates
        .iter()
        .filter(|c| c.confidence >= CANDIDATE_THRESHOLD)
        .map(|candidate| {
            let kind = determine_kind(candidate);
            let priority = determine_priority(candidate.combined_score);
            let evidence = build_evidence(candidate);
            let recommendation = build_recommendation(candidate, &kind);

            RefactorProposal {
                kind,
                priority,
                confidence: candidate.confidence,
                target: candidate.module.clone(),
                evidence,
                recommendation,
            }
        })
        .collect();

    proposals.sort_by(|a, b| b.confidence.partial_cmp(&a.confidence).unwrap());
    proposals
}

/// Determine the most appropriate refactoring kind based on the candidate's
/// structural smells and historical signals.
fn determine_kind(candidate: &RefactorCandidate) -> RefactorKind {
    // Circular dependency takes highest priority — must be resolved first
    if candidate.smells.in_cycle {
        return RefactorKind::BreakCycle;
    }
    // Boundary violations suggest files are in the wrong module
    if candidate.smells.has_violations {
        return RefactorKind::MoveFiles;
    }
    // Wide API surface suggests the module needs a cleaner interface
    if candidate.smells.wide_api && !candidate.smells.large_module {
        return RefactorKind::ExtractInterface;
    }
    // Large module or god file — split it
    RefactorKind::SplitModule
}

/// Map a combined score to a priority level (0 = critical, 4 = backlog).
fn determine_priority(combined_score: f64) -> u8 {
    if combined_score >= 8.0 {
        0 // critical
    } else if combined_score >= 6.0 {
        1 // high
    } else if combined_score >= 4.0 {
        2 // medium
    } else if combined_score >= 2.0 {
        3 // low
    } else {
        4 // backlog
    }
}

/// Build the evidence record from a candidate's historical signals.
fn build_evidence(candidate: &RefactorCandidate) -> ProposalEvidence {
    ProposalEvidence {
        expansion_events: candidate.signals.expansion_score,
        avg_integration_iterations: candidate.signals.integration_score,
        entangled_rollbacks: candidate.signals.drift_count,
        metadata_drift: candidate.signals.drift_count,
    }
}

/// Build a human-readable recommendation for the candidate.
fn build_recommendation(candidate: &RefactorCandidate, kind: &RefactorKind) -> String {
    let module = &candidate.module;
    let smells = &candidate.smells;

    match kind {
        RefactorKind::SplitModule => {
            let mut reasons = Vec::new();
            if smells.large_module {
                reasons.push("high line count");
            }
            if smells.has_god_files {
                reasons.push("god file detected");
            }
            if smells.high_fan_in {
                reasons.push("high fan-in");
            }
            let reason_str = if reasons.is_empty() {
                "structural complexity".to_string()
            } else {
                reasons.join(", ")
            };
            format!(
                "Split module '{module}' into smaller, focused submodules. \
                 Reason: {reason_str}. Suggested approach: identify cohesive \
                 clusters of types and functions, extract each cluster into \
                 its own submodule, and re-export public items from the parent."
            )
        }
        RefactorKind::ExtractInterface => {
            format!(
                "Extract a trait or interface from module '{module}' to narrow \
                 its public API surface ({} public items). Define a trait \
                 capturing the core contract, implement it on the existing \
                 types, and update consumers to depend on the trait.",
                smells.structural_score as usize
            )
        }
        RefactorKind::MoveFiles => {
            format!(
                "Move misplaced files out of module '{module}' to resolve \
                 boundary violations ({} as source, {} as target). Relocate \
                 files to the module that best matches their dependency pattern.",
                candidate.smells.has_violations as u8, candidate.smells.has_violations as u8
            )
        }
        RefactorKind::BreakCycle => {
            format!(
                "Break circular dependency involving module '{module}'. \
                 Introduce an intermediary module or invert the dependency \
                 direction by extracting shared types into a common crate/module \
                 that both sides can depend on without forming a cycle."
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signal_correlator::{
        CorrelationReport, ModuleSignals, RefactorCandidate, StructuralSmells,
    };
    use std::collections::HashMap;

    fn make_candidate(module: &str, confidence: f64, combined_score: f64) -> RefactorCandidate {
        RefactorCandidate {
            module: module.to_string(),
            smells: StructuralSmells {
                high_fan_in: false,
                large_module: true,
                in_cycle: false,
                has_violations: false,
                has_god_files: true,
                wide_api: false,
                structural_score: 3.0,
            },
            signals: ModuleSignals {
                module: module.to_string(),
                expansion_score: 5.0,
                integration_score: 2.0,
                drift_count: 1,
                historical_score: 8.0,
            },
            combined_score,
            confidence,
        }
    }

    fn make_report(candidates: Vec<RefactorCandidate>) -> CorrelationReport {
        CorrelationReport {
            module_signals: HashMap::new(),
            module_smells: HashMap::new(),
            candidates,
            total_expansion_events: 10,
            total_integration_records: 5,
            total_drift_reports: 2,
        }
    }

    #[test]
    fn filters_below_threshold() {
        let report = make_report(vec![
            make_candidate("high", 0.8, 6.0),
            make_candidate("low", 0.5, 4.0),
            make_candidate("very_low", 0.3, 3.0),
        ]);

        let proposals = generate_proposals(&report);
        assert_eq!(proposals.len(), 1);
        assert_eq!(proposals[0].target, "high");
    }

    #[test]
    fn sorted_by_confidence_descending() {
        let report = make_report(vec![
            make_candidate("medium", 0.7, 5.0),
            make_candidate("high", 0.9, 7.0),
            make_candidate("also_high", 0.8, 6.0),
        ]);

        let proposals = generate_proposals(&report);
        assert_eq!(proposals.len(), 3);
        assert_eq!(proposals[0].target, "high");
        assert_eq!(proposals[1].target, "also_high");
        assert_eq!(proposals[2].target, "medium");
    }

    #[test]
    fn proposal_has_all_fields() {
        let report = make_report(vec![make_candidate("auth", 0.8, 6.0)]);

        let proposals = generate_proposals(&report);
        assert_eq!(proposals.len(), 1);
        let p = &proposals[0];

        assert_eq!(p.kind, RefactorKind::SplitModule);
        assert!(p.priority <= 4);
        assert!((p.confidence - 0.8).abs() < f64::EPSILON);
        assert_eq!(p.target, "auth");
        assert!(p.evidence.expansion_events > 0.0);
        assert!(p.evidence.avg_integration_iterations > 0.0);
        assert!(!p.recommendation.is_empty());
    }

    #[test]
    fn empty_report_produces_no_proposals() {
        let report = make_report(vec![]);
        let proposals = generate_proposals(&report);
        assert!(proposals.is_empty());
    }

    #[test]
    fn priority_mapping() {
        assert_eq!(determine_priority(10.0), 0);
        assert_eq!(determine_priority(8.0), 0);
        assert_eq!(determine_priority(7.0), 1);
        assert_eq!(determine_priority(6.0), 1);
        assert_eq!(determine_priority(5.0), 2);
        assert_eq!(determine_priority(4.0), 2);
        assert_eq!(determine_priority(3.0), 3);
        assert_eq!(determine_priority(2.0), 3);
        assert_eq!(determine_priority(1.0), 4);
    }

    #[test]
    fn kind_break_cycle_when_in_cycle() {
        let mut candidate = make_candidate("cyclic", 0.8, 6.0);
        candidate.smells.in_cycle = true;
        assert_eq!(determine_kind(&candidate), RefactorKind::BreakCycle);
    }

    #[test]
    fn kind_move_files_when_violations() {
        let mut candidate = make_candidate("violated", 0.8, 6.0);
        candidate.smells.has_violations = true;
        assert_eq!(determine_kind(&candidate), RefactorKind::MoveFiles);
    }

    #[test]
    fn kind_extract_interface_when_wide_api_not_large() {
        let mut candidate = make_candidate("wide", 0.8, 6.0);
        candidate.smells.wide_api = true;
        candidate.smells.large_module = false;
        assert_eq!(determine_kind(&candidate), RefactorKind::ExtractInterface);
    }

    #[test]
    fn kind_split_module_default() {
        let candidate = make_candidate("big", 0.8, 6.0);
        assert_eq!(determine_kind(&candidate), RefactorKind::SplitModule);
    }

    #[test]
    fn evidence_populated_from_signals() {
        let candidate = make_candidate("mod1", 0.8, 6.0);
        let evidence = build_evidence(&candidate);
        assert!((evidence.expansion_events - 5.0).abs() < f64::EPSILON);
        assert!((evidence.avg_integration_iterations - 2.0).abs() < f64::EPSILON);
        assert_eq!(evidence.entangled_rollbacks, 1);
        assert_eq!(evidence.metadata_drift, 1);
    }

    #[test]
    fn recommendation_contains_module_name() {
        let candidate = make_candidate("my_module", 0.8, 6.0);
        let kind = determine_kind(&candidate);
        let rec = build_recommendation(&candidate, &kind);
        assert!(rec.contains("my_module"));
    }

    #[test]
    fn proposal_generation_with_mock_three_candidates() {
        // As specified in the bead's verification: 3 candidates with scores 0.8, 0.5, 0.3
        // Only the 0.8 candidate should produce a proposal
        let report = make_report(vec![
            make_candidate("above_threshold", 0.8, 6.0),
            make_candidate("below_threshold_1", 0.5, 4.0),
            make_candidate("below_threshold_2", 0.3, 3.0),
        ]);

        let proposals = generate_proposals(&report);
        assert_eq!(
            proposals.len(),
            1,
            "Only the 0.8 candidate should produce a proposal"
        );
        assert_eq!(proposals[0].target, "above_threshold");
        assert!((proposals[0].confidence - 0.8).abs() < f64::EPSILON);

        // Verify all required fields are present
        let p = &proposals[0];
        assert_eq!(p.kind, RefactorKind::SplitModule);
        assert!(p.priority <= 4);
        assert!(!p.target.is_empty());
        assert!(p.evidence.expansion_events > 0.0);
        assert!(!p.recommendation.is_empty());
    }

    #[test]
    fn threshold_boundary_exactly_0_6() {
        // A candidate with exactly 0.6 confidence should produce a proposal
        let report = make_report(vec![make_candidate("boundary", 0.6, 5.0)]);
        let proposals = generate_proposals(&report);
        assert_eq!(proposals.len(), 1);
        assert_eq!(proposals[0].target, "boundary");
    }

    #[test]
    fn threshold_boundary_just_below() {
        // A candidate with 0.59 confidence should NOT produce a proposal
        let report = make_report(vec![make_candidate("just_below", 0.59, 5.0)]);
        let proposals = generate_proposals(&report);
        assert!(proposals.is_empty());
    }
}
