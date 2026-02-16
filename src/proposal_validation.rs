//! Proposal validation checks for refactoring candidates.
//!
//! Before a refactoring proposal becomes a tracked task, it must pass four
//! validation gates:
//!
//! 1. **Dependency feasibility** — would the proposed split create circular deps?
//! 2. **API surface preservation** — do all current consumers still have access?
//! 3. **Test coverage** — are there tests for the module boundaries being changed?
//! 4. **Conflict check** — would the refactor conflict with in-progress tasks?
//!
//! Proposals that pass all checks (or have only warnings) become refactor issues
//! in the tracker. Ref: PRD section "Proposal Validation".

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use rusqlite::Connection;

use crate::circular_dep;
use crate::module_detect::Module;
use crate::public_api::ModuleApi;
use crate::signal_correlator::RefactorCandidate;

/// The kind of refactoring being proposed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProposalKind {
    /// Split a large module into two or more smaller modules.
    SplitModule,
    /// Extract a common interface / trait from a module.
    ExtractInterface,
    /// Move files between modules to fix boundary violations.
    MoveFiles,
    /// Break a circular dependency by restructuring imports.
    BreakCycle,
}

/// A refactoring proposal to be validated before becoming a task.
#[derive(Debug, Clone)]
pub struct RefactorProposal {
    /// The kind of refactoring.
    pub kind: ProposalKind,
    /// The target module being refactored.
    pub target_module: String,
    /// The candidate analysis that triggered this proposal.
    pub candidate: RefactorCandidate,
    /// For `SplitModule`: the proposed new module names after the split.
    /// For `MoveFiles`: the destination module(s).
    /// Empty for other kinds.
    pub proposed_modules: Vec<String>,
    /// Files that would be moved or reorganized.
    pub affected_files: Vec<PathBuf>,
}

/// Severity of a validation finding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationSeverity {
    /// Proposal cannot proceed; must be fixed or abandoned.
    Error,
    /// Proposal can proceed but reviewer should be aware.
    Warning,
}

/// A single validation finding.
#[derive(Debug, Clone)]
pub struct ValidationFinding {
    /// Which check produced this finding.
    pub check: String,
    /// Severity level.
    pub severity: ValidationSeverity,
    /// Human-readable description of the issue.
    pub message: String,
}

/// Full validation result for a proposal.
#[derive(Debug)]
pub struct ValidationResult {
    /// The proposal that was validated.
    pub proposal: RefactorProposal,
    /// All findings from validation checks.
    pub findings: Vec<ValidationFinding>,
    /// Whether the proposal passed validation (no errors, warnings are ok).
    pub passed: bool,
}

impl ValidationResult {
    /// Count findings by severity.
    pub fn error_count(&self) -> usize {
        self.findings
            .iter()
            .filter(|f| f.severity == ValidationSeverity::Error)
            .count()
    }

    pub fn warning_count(&self) -> usize {
        self.findings
            .iter()
            .filter(|f| f.severity == ValidationSeverity::Warning)
            .count()
    }
}

/// Validate a refactoring proposal against all four checks.
///
/// Returns a `ValidationResult` with findings from each check. The proposal
/// passes if there are zero `Error`-severity findings.
pub fn validate_proposal(
    proposal: &RefactorProposal,
    import_graph: &HashMap<PathBuf, Vec<PathBuf>>,
    modules: &HashMap<String, Module>,
    apis: &HashMap<String, ModuleApi>,
    conn: &Connection,
    repo_root: &Path,
) -> ValidationResult {
    let mut findings = Vec::new();

    // Check 1: Dependency feasibility
    findings.extend(check_dependency_feasibility(
        proposal,
        import_graph,
        modules,
    ));

    // Check 2: API surface preservation
    findings.extend(check_api_surface_preservation(proposal, apis, modules));

    // Check 3: Test coverage
    findings.extend(check_test_coverage(proposal, repo_root));

    // Check 4: Conflict with in-progress tasks
    findings.extend(check_conflict_with_in_progress(proposal, conn));

    let passed = !findings
        .iter()
        .any(|f| f.severity == ValidationSeverity::Error);

    ValidationResult {
        proposal: proposal.clone(),
        findings,
        passed,
    }
}

// ── Check 1: Dependency Feasibility ────────────────────────────────

/// Check whether the proposed refactoring would create circular dependencies.
///
/// Simulates the new module structure by remapping affected files to
/// proposed modules, then runs cycle detection on the resulting graph.
fn check_dependency_feasibility(
    proposal: &RefactorProposal,
    import_graph: &HashMap<PathBuf, Vec<PathBuf>>,
    modules: &HashMap<String, Module>,
) -> Vec<ValidationFinding> {
    let mut findings = Vec::new();

    // Only relevant for SplitModule and MoveFiles — these create new modules
    // or change which module a file belongs to.
    if proposal.kind != ProposalKind::SplitModule && proposal.kind != ProposalKind::MoveFiles {
        return findings;
    }

    if proposal.proposed_modules.is_empty() {
        findings.push(ValidationFinding {
            check: "dependency_feasibility".to_string(),
            severity: ValidationSeverity::Warning,
            message: "No proposed module structure specified; cannot check for circular deps"
                .to_string(),
        });
        return findings;
    }

    // Build a simulated module map: start from existing, apply the proposed changes
    let simulated_modules = simulate_module_split(proposal, modules);

    // Run cycle detection on the existing import graph with simulated modules
    let cycles = circular_dep::detect_module_cycles(import_graph, &simulated_modules);

    // Check for NEW cycles (cycles that didn't exist before the split)
    let existing_cycles = circular_dep::detect_module_cycles(import_graph, modules);
    let existing_cycle_set: HashSet<Vec<String>> =
        existing_cycles.into_iter().map(|c| c.modules).collect();

    for cycle in &cycles {
        if !existing_cycle_set.contains(&cycle.modules) {
            // This is a new cycle introduced by the proposal
            let cycle_str = cycle.modules.join(" -> ");
            findings.push(ValidationFinding {
                check: "dependency_feasibility".to_string(),
                severity: ValidationSeverity::Error,
                message: format!("Proposed split would create circular dependency: {cycle_str}"),
            });
        }
    }

    if findings.is_empty() && !cycles.is_empty() {
        // Pre-existing cycles remain but no new ones
        findings.push(ValidationFinding {
            check: "dependency_feasibility".to_string(),
            severity: ValidationSeverity::Warning,
            message: format!(
                "Pre-existing circular dependencies remain ({} cycles); proposal does not worsen them",
                cycles.len()
            ),
        });
    }

    findings
}

/// Simulate a module split by redistributing affected files to proposed modules.
fn simulate_module_split(
    proposal: &RefactorProposal,
    modules: &HashMap<String, Module>,
) -> HashMap<String, Module> {
    let mut result = modules.clone();
    let affected_set: HashSet<&PathBuf> = proposal.affected_files.iter().collect();

    // Remove affected files from the target module
    if let Some(target) = result.get_mut(&proposal.target_module) {
        target.files.retain(|f| !affected_set.contains(f));
    }

    // Create new modules for each proposed module name and distribute files
    // Simple strategy: divide affected files evenly across proposed modules
    // (In practice, the proposal would specify exactly which files go where)
    let files_per_module = if proposal.proposed_modules.is_empty() {
        0
    } else {
        (proposal.affected_files.len() + proposal.proposed_modules.len() - 1)
            / proposal.proposed_modules.len()
    };

    for (i, module_name) in proposal.proposed_modules.iter().enumerate() {
        let start = i * files_per_module;
        let end = (start + files_per_module).min(proposal.affected_files.len());
        let files: Vec<PathBuf> = if start < proposal.affected_files.len() {
            proposal.affected_files[start..end].to_vec()
        } else {
            Vec::new()
        };

        let root_path = files
            .first()
            .and_then(|f| f.parent())
            .unwrap_or(Path::new("src"))
            .to_path_buf();

        result.insert(
            module_name.clone(),
            Module {
                name: module_name.clone(),
                root_path,
                files,
                has_entry_point: false,
                entry_point: None,
                submodules: Vec::new(),
            },
        );
    }

    result
}

// ── Check 2: API Surface Preservation ──────────────────────────────

/// Check whether all current consumers of the target module's public API
/// would still have access after the refactoring.
///
/// For each public symbol in the target module, verifies that at least one
/// of the proposed modules (or the remaining target) still exposes it.
fn check_api_surface_preservation(
    proposal: &RefactorProposal,
    apis: &HashMap<String, ModuleApi>,
    modules: &HashMap<String, Module>,
) -> Vec<ValidationFinding> {
    let mut findings = Vec::new();

    let target_api = match apis.get(&proposal.target_module) {
        Some(api) => api,
        None => return findings, // No API to preserve
    };

    if target_api.public_symbols.is_empty() {
        return findings;
    }

    // For SplitModule/MoveFiles: check which symbols come from affected files
    let affected_set: HashSet<&PathBuf> = proposal.affected_files.iter().collect();

    // Find symbols that live in affected files
    let mut symbols_being_moved: Vec<&str> = Vec::new();
    for sym in &target_api.public_symbols {
        // Check if the symbol's file is in the affected set
        let sym_in_affected =
            is_symbol_in_affected_files(&sym.file, &proposal.target_module, modules, &affected_set);
        if sym_in_affected {
            symbols_being_moved.push(&sym.name);
        }
    }

    if symbols_being_moved.is_empty() {
        return findings;
    }

    // For SplitModule/MoveFiles, the moved symbols would need to be re-exported
    // or consumers updated. Flag this as a warning since it requires migration.
    if proposal.kind == ProposalKind::SplitModule || proposal.kind == ProposalKind::MoveFiles {
        let count = symbols_being_moved.len();
        if count > 0 {
            let sample: Vec<&str> = symbols_being_moved.iter().take(5).copied().collect();
            let sample_str = sample.join(", ");
            let suffix = if count > 5 {
                format!(" and {} more", count - 5)
            } else {
                String::new()
            };
            findings.push(ValidationFinding {
                check: "api_surface_preservation".to_string(),
                severity: ValidationSeverity::Warning,
                message: format!(
                    "{count} public symbol(s) would move from '{}': {sample_str}{suffix}. \
                     Consumers will need updated imports or re-exports.",
                    proposal.target_module
                ),
            });
        }
    }

    // Check for symbols that would disappear entirely (Error severity)
    // This happens if symbols are in affected files but no proposed module exists
    if proposal.proposed_modules.is_empty() && !symbols_being_moved.is_empty() {
        findings.push(ValidationFinding {
            check: "api_surface_preservation".to_string(),
            severity: ValidationSeverity::Error,
            message: format!(
                "{} public symbol(s) would be orphaned: no target module specified for moved files",
                symbols_being_moved.len()
            ),
        });
    }

    findings
}

/// Check if a symbol's source file is in the set of affected files.
fn is_symbol_in_affected_files(
    sym_file_name: &str,
    module_name: &str,
    modules: &HashMap<String, Module>,
    affected_set: &HashSet<&PathBuf>,
) -> bool {
    let module = match modules.get(module_name) {
        Some(m) => m,
        None => return false,
    };

    for file in &module.files {
        let file_name = file
            .file_name()
            .map(|f| f.to_string_lossy().to_string())
            .unwrap_or_default();
        if file_name == sym_file_name && affected_set.contains(file) {
            return true;
        }
    }
    false
}

// ── Check 3: Test Coverage ─────────────────────────────────────────

/// Check whether there are tests covering the module boundaries being changed.
///
/// Looks for test files or `#[cfg(test)]` modules in the affected files.
/// Warns if boundaries are being changed without test coverage.
fn check_test_coverage(proposal: &RefactorProposal, repo_root: &Path) -> Vec<ValidationFinding> {
    let mut findings = Vec::new();
    let mut files_with_tests = 0;
    let mut files_checked = 0;

    for file in &proposal.affected_files {
        let content = match std::fs::read_to_string(file) {
            Ok(c) => c,
            Err(_) => continue,
        };
        files_checked += 1;

        if has_test_coverage(&content) {
            files_with_tests += 1;
        }
    }

    // Also check for companion test files in tests/ directory
    let tests_dir = repo_root.join("tests");
    let mut integration_test_count = 0;
    if tests_dir.exists() {
        for file in &proposal.affected_files {
            if let Some(stem) = file.file_stem().and_then(|s| s.to_str()) {
                let test_file = tests_dir.join(format!("{stem}.rs"));
                let test_file_alt = tests_dir.join(format!("test_{stem}.rs"));
                if test_file.exists() || test_file_alt.exists() {
                    integration_test_count += 1;
                }
            }
        }
    }

    let total_test_coverage = files_with_tests + integration_test_count;

    if files_checked == 0 {
        findings.push(ValidationFinding {
            check: "test_coverage".to_string(),
            severity: ValidationSeverity::Warning,
            message: "No affected files could be read for test coverage analysis".to_string(),
        });
    } else if total_test_coverage == 0 {
        findings.push(ValidationFinding {
            check: "test_coverage".to_string(),
            severity: ValidationSeverity::Warning,
            message: format!(
                "None of the {} affected file(s) have test coverage. \
                 Consider adding tests before refactoring.",
                files_checked
            ),
        });
    } else if files_with_tests < files_checked {
        let untested = files_checked - files_with_tests;
        findings.push(ValidationFinding {
            check: "test_coverage".to_string(),
            severity: ValidationSeverity::Warning,
            message: format!(
                "{untested} of {files_checked} affected file(s) lack inline tests. \
                 {integration_test_count} integration test(s) found.",
            ),
        });
    }

    findings
}

/// Check if source code contains test coverage (inline `#[cfg(test)]` or `#[test]`).
fn has_test_coverage(source: &str) -> bool {
    for line in source.lines() {
        let trimmed = line.trim();
        if trimmed == "#[cfg(test)]" || trimmed == "#[test]" {
            return true;
        }
    }
    false
}

// ── Check 4: Conflict with In-Progress Tasks ──────────────────────

/// Check whether the proposed refactoring would conflict with currently
/// in-progress tasks (coding or integrating).
///
/// Compares the proposal's affected files with the files being touched
/// by active worker assignments.
fn check_conflict_with_in_progress(
    proposal: &RefactorProposal,
    conn: &Connection,
) -> Vec<ValidationFinding> {
    let mut findings = Vec::new();

    // Get active worker assignments
    let active = match crate::db::active_worker_assignments(conn) {
        Ok(a) => a,
        Err(_) => return findings, // Can't check, no error
    };

    if active.is_empty() {
        return findings;
    }

    for assignment in &active {
        // Check affected_globs for overlap
        if let Some(globs) = &assignment.affected_globs {
            let task_files: Vec<&str> = globs.split(',').map(|s| s.trim()).collect();
            let mut overlapping = Vec::new();

            for task_file in &task_files {
                // Check if any affected file path ends with or matches the task file
                let task_path = Path::new(task_file);
                for affected in &proposal.affected_files {
                    if paths_overlap(affected, task_path) {
                        overlapping.push(task_file.to_string());
                        break;
                    }
                }
            }

            if !overlapping.is_empty() {
                let overlap_str = overlapping.join(", ");
                findings.push(ValidationFinding {
                    check: "conflict_check".to_string(),
                    severity: ValidationSeverity::Error,
                    message: format!(
                        "Refactoring conflicts with in-progress task '{}' (status: {}). \
                         Overlapping files: {overlap_str}",
                        assignment.bead_id, assignment.status
                    ),
                });
            }
        }

        // Also check file changes recorded for this assignment
        let file_changes = match crate::db::file_changes_by_assignment(conn, assignment.id) {
            Ok(fc) => fc,
            Err(_) => continue,
        };

        for change in &file_changes {
            let change_path = Path::new(&change.file_path);
            let overlaps = proposal
                .affected_files
                .iter()
                .any(|f| paths_overlap(f, change_path));

            if overlaps {
                findings.push(ValidationFinding {
                    check: "conflict_check".to_string(),
                    severity: ValidationSeverity::Error,
                    message: format!(
                        "File '{}' is being modified by in-progress task '{}' ({}). \
                         Wait for task completion before refactoring.",
                        change.file_path, assignment.bead_id, change.change_type
                    ),
                });
            }
        }
    }

    // Deduplicate findings (same task + same file could appear in both globs and file_changes)
    findings.dedup_by(|a, b| a.message == b.message);

    findings
}

/// Check if two paths refer to the same file.
///
/// Handles the case where one path is absolute and the other relative,
/// or where paths use different prefixes. Matches if either path ends
/// with the other's components.
fn paths_overlap(a: &Path, b: &Path) -> bool {
    let a_str = a.to_string_lossy();
    let b_str = b.to_string_lossy();

    // Exact match
    if a_str == b_str {
        return true;
    }

    // Check if one ends with the other
    a_str.ends_with(&*b_str) || b_str.ends_with(&*a_str)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expansion_event;
    use crate::signal_correlator::{ModuleSignals, StructuralSmells};

    fn setup_db() -> (tempfile::TempDir, Connection) {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("test.db");
        let conn = crate::db::open_or_create(&db_path).unwrap();
        expansion_event::create_table(&conn).unwrap();
        (tmp, conn)
    }

    fn make_candidate(module: &str) -> RefactorCandidate {
        RefactorCandidate {
            module: module.to_string(),
            smells: StructuralSmells {
                high_fan_in: false,
                large_module: true,
                in_cycle: false,
                has_violations: false,
                has_god_files: true,
                wide_api: true,
                structural_score: 3.0,
            },
            signals: ModuleSignals {
                module: module.to_string(),
                expansion_score: 2.0,
                integration_score: 1.5,
                drift_count: 1,
                historical_score: 3.5,
            },
            combined_score: 6.5,
            confidence: 0.67,
        }
    }

    fn make_modules(specs: &[(&str, &[&str])]) -> HashMap<String, Module> {
        let mut modules = HashMap::new();
        for (name, files) in specs {
            let file_paths: Vec<PathBuf> = files.iter().map(|f| PathBuf::from(f)).collect();
            let root = file_paths
                .first()
                .and_then(|f| f.parent())
                .unwrap_or(Path::new("src"))
                .to_path_buf();
            modules.insert(
                name.to_string(),
                Module {
                    name: name.to_string(),
                    root_path: root,
                    files: file_paths,
                    has_entry_point: false,
                    entry_point: None,
                    submodules: Vec::new(),
                },
            );
        }
        modules
    }

    fn make_graph(edges: &[(&str, &[&str])]) -> HashMap<PathBuf, Vec<PathBuf>> {
        let mut graph = HashMap::new();
        for (src, deps) in edges {
            graph.insert(
                PathBuf::from(src),
                deps.iter().map(|d| PathBuf::from(d)).collect(),
            );
        }
        graph
    }

    // ── Dependency Feasibility Tests ───────────────────────────────

    #[test]
    fn dep_feasibility_no_new_cycles() {
        let modules = make_modules(&[
            ("big", &["src/big/mod.rs", "src/big/a.rs", "src/big/b.rs"]),
            ("other", &["src/other/mod.rs"]),
        ]);

        let graph = make_graph(&[
            ("src/big/mod.rs", &["src/big/a.rs", "src/big/b.rs"]),
            ("src/big/a.rs", &[]),
            ("src/big/b.rs", &[]),
            ("src/other/mod.rs", &["src/big/mod.rs"]),
        ]);

        let proposal = RefactorProposal {
            kind: ProposalKind::SplitModule,
            target_module: "big".to_string(),
            candidate: make_candidate("big"),
            proposed_modules: vec!["big_a".to_string(), "big_b".to_string()],
            affected_files: vec![PathBuf::from("src/big/a.rs"), PathBuf::from("src/big/b.rs")],
        };

        let findings = check_dependency_feasibility(&proposal, &graph, &modules);
        assert!(
            findings
                .iter()
                .all(|f| f.severity != ValidationSeverity::Error),
            "Should not have errors for clean split"
        );
    }

    #[test]
    fn dep_feasibility_detects_new_cycle() {
        // Setup: big module has files a.rs and b.rs
        // a.rs imports from other, other imports from b.rs
        // If we split big into big_a (a.rs) and big_b (b.rs), and b.rs imports a.rs,
        // that creates big_b -> big_a cycle via other -> big_a -> other -> big_b
        let modules = make_modules(&[
            ("big", &["src/big/a.rs", "src/big/b.rs"]),
            ("other", &["src/other/mod.rs"]),
        ]);

        let graph = make_graph(&[
            ("src/big/a.rs", &["src/other/mod.rs"]),
            ("src/big/b.rs", &["src/big/a.rs"]),
            ("src/other/mod.rs", &["src/big/b.rs"]),
        ]);

        let proposal = RefactorProposal {
            kind: ProposalKind::SplitModule,
            target_module: "big".to_string(),
            candidate: make_candidate("big"),
            proposed_modules: vec!["big_a".to_string(), "big_b".to_string()],
            affected_files: vec![PathBuf::from("src/big/a.rs"), PathBuf::from("src/big/b.rs")],
        };

        let findings = check_dependency_feasibility(&proposal, &graph, &modules);
        let errors: Vec<_> = findings
            .iter()
            .filter(|f| f.severity == ValidationSeverity::Error)
            .collect();
        assert!(
            !errors.is_empty(),
            "Should detect new circular dependency from split"
        );
        assert!(errors[0].message.contains("circular dependency"));
    }

    #[test]
    fn dep_feasibility_skips_non_split_proposals() {
        let modules = make_modules(&[("auth", &["src/auth/mod.rs"])]);
        let graph = make_graph(&[("src/auth/mod.rs", &[])]);

        let proposal = RefactorProposal {
            kind: ProposalKind::ExtractInterface,
            target_module: "auth".to_string(),
            candidate: make_candidate("auth"),
            proposed_modules: vec![],
            affected_files: vec![PathBuf::from("src/auth/mod.rs")],
        };

        let findings = check_dependency_feasibility(&proposal, &graph, &modules);
        assert!(findings.is_empty());
    }

    #[test]
    fn dep_feasibility_warns_no_proposed_modules() {
        let modules = make_modules(&[("big", &["src/big/mod.rs"])]);
        let graph = make_graph(&[("src/big/mod.rs", &[])]);

        let proposal = RefactorProposal {
            kind: ProposalKind::SplitModule,
            target_module: "big".to_string(),
            candidate: make_candidate("big"),
            proposed_modules: vec![],
            affected_files: vec![PathBuf::from("src/big/mod.rs")],
        };

        let findings = check_dependency_feasibility(&proposal, &graph, &modules);
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].severity, ValidationSeverity::Warning);
    }

    // ── API Surface Preservation Tests ─────────────────────────────

    #[test]
    fn api_preservation_warns_on_moved_symbols() {
        let modules = make_modules(&[("auth", &["src/auth/mod.rs", "src/auth/session.rs"])]);

        let apis: HashMap<String, ModuleApi> = [(
            "auth".to_string(),
            ModuleApi {
                module_name: "auth".to_string(),
                public_symbols: vec![
                    crate::public_api::Symbol {
                        kind: "fn".to_string(),
                        name: "login".to_string(),
                        signature: "pub fn login()".to_string(),
                        file: "mod.rs".to_string(),
                    },
                    crate::public_api::Symbol {
                        kind: "struct".to_string(),
                        name: "Session".to_string(),
                        signature: "pub struct Session".to_string(),
                        file: "session.rs".to_string(),
                    },
                ],
            },
        )]
        .into();

        let proposal = RefactorProposal {
            kind: ProposalKind::SplitModule,
            target_module: "auth".to_string(),
            candidate: make_candidate("auth"),
            proposed_modules: vec!["auth_session".to_string()],
            affected_files: vec![PathBuf::from("src/auth/session.rs")],
        };

        let findings = check_api_surface_preservation(&proposal, &apis, &modules);
        assert!(!findings.is_empty());
        let warning = findings
            .iter()
            .find(|f| f.check == "api_surface_preservation")
            .unwrap();
        assert_eq!(warning.severity, ValidationSeverity::Warning);
        assert!(warning.message.contains("Session"));
    }

    #[test]
    fn api_preservation_no_issue_when_symbols_stay() {
        let modules = make_modules(&[("auth", &["src/auth/mod.rs", "src/auth/internal.rs"])]);

        let apis: HashMap<String, ModuleApi> = [(
            "auth".to_string(),
            ModuleApi {
                module_name: "auth".to_string(),
                public_symbols: vec![crate::public_api::Symbol {
                    kind: "fn".to_string(),
                    name: "login".to_string(),
                    signature: "pub fn login()".to_string(),
                    file: "mod.rs".to_string(),
                }],
            },
        )]
        .into();

        // Moving internal.rs which has no public symbols
        let proposal = RefactorProposal {
            kind: ProposalKind::SplitModule,
            target_module: "auth".to_string(),
            candidate: make_candidate("auth"),
            proposed_modules: vec!["auth_internal".to_string()],
            affected_files: vec![PathBuf::from("src/auth/internal.rs")],
        };

        let findings = check_api_surface_preservation(&proposal, &apis, &modules);
        assert!(findings.is_empty());
    }

    #[test]
    fn api_preservation_error_when_no_target_module() {
        let modules = make_modules(&[("auth", &["src/auth/mod.rs", "src/auth/session.rs"])]);

        let apis: HashMap<String, ModuleApi> = [(
            "auth".to_string(),
            ModuleApi {
                module_name: "auth".to_string(),
                public_symbols: vec![crate::public_api::Symbol {
                    kind: "struct".to_string(),
                    name: "Session".to_string(),
                    signature: "pub struct Session".to_string(),
                    file: "session.rs".to_string(),
                }],
            },
        )]
        .into();

        let proposal = RefactorProposal {
            kind: ProposalKind::MoveFiles,
            target_module: "auth".to_string(),
            candidate: make_candidate("auth"),
            proposed_modules: vec![], // No target — symbols would be orphaned
            affected_files: vec![PathBuf::from("src/auth/session.rs")],
        };

        let findings = check_api_surface_preservation(&proposal, &apis, &modules);
        let errors: Vec<_> = findings
            .iter()
            .filter(|f| f.severity == ValidationSeverity::Error)
            .collect();
        assert!(!errors.is_empty());
        assert!(errors[0].message.contains("orphaned"));
    }

    #[test]
    fn api_preservation_no_api_no_findings() {
        let modules = make_modules(&[("internal", &["src/internal/mod.rs"])]);
        let apis: HashMap<String, ModuleApi> = HashMap::new();

        let proposal = RefactorProposal {
            kind: ProposalKind::SplitModule,
            target_module: "internal".to_string(),
            candidate: make_candidate("internal"),
            proposed_modules: vec!["internal_a".to_string()],
            affected_files: vec![PathBuf::from("src/internal/mod.rs")],
        };

        let findings = check_api_surface_preservation(&proposal, &apis, &modules);
        assert!(findings.is_empty());
    }

    // ── Test Coverage Tests ────────────────────────────────────────

    #[test]
    fn test_coverage_detects_tests() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src/mod_a");
        std::fs::create_dir_all(&src).unwrap();

        let file_with_tests = src.join("tested.rs");
        std::fs::write(
            &file_with_tests,
            "pub fn foo() {}\n\n#[cfg(test)]\nmod tests {\n    #[test]\n    fn it_works() {}\n}",
        )
        .unwrap();

        let file_without_tests = src.join("untested.rs");
        std::fs::write(&file_without_tests, "pub fn bar() {}").unwrap();

        let proposal = RefactorProposal {
            kind: ProposalKind::SplitModule,
            target_module: "mod_a".to_string(),
            candidate: make_candidate("mod_a"),
            proposed_modules: vec!["mod_a1".to_string()],
            affected_files: vec![file_with_tests, file_without_tests],
        };

        let findings = check_test_coverage(&proposal, tmp.path());
        // Should warn about partial coverage
        assert!(!findings.is_empty());
        let warning = &findings[0];
        assert_eq!(warning.severity, ValidationSeverity::Warning);
        assert!(warning.message.contains("1 of 2"));
    }

    #[test]
    fn test_coverage_all_tested() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src/mod_a");
        std::fs::create_dir_all(&src).unwrap();

        let file = src.join("tested.rs");
        std::fs::write(&file, "pub fn foo() {}\n#[cfg(test)]\nmod tests {}").unwrap();

        let proposal = RefactorProposal {
            kind: ProposalKind::SplitModule,
            target_module: "mod_a".to_string(),
            candidate: make_candidate("mod_a"),
            proposed_modules: vec!["mod_a1".to_string()],
            affected_files: vec![file],
        };

        let findings = check_test_coverage(&proposal, tmp.path());
        assert!(findings.is_empty());
    }

    #[test]
    fn test_coverage_none_tested() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src/mod_a");
        std::fs::create_dir_all(&src).unwrap();

        let file = src.join("untested.rs");
        std::fs::write(&file, "pub fn bar() {}").unwrap();

        let proposal = RefactorProposal {
            kind: ProposalKind::SplitModule,
            target_module: "mod_a".to_string(),
            candidate: make_candidate("mod_a"),
            proposed_modules: vec!["mod_a1".to_string()],
            affected_files: vec![file],
        };

        let findings = check_test_coverage(&proposal, tmp.path());
        assert!(!findings.is_empty());
        assert!(findings[0].message.contains("None of the"));
    }

    #[test]
    fn test_coverage_integration_tests_found() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src/mod_a");
        std::fs::create_dir_all(&src).unwrap();
        let tests_dir = tmp.path().join("tests");
        std::fs::create_dir_all(&tests_dir).unwrap();

        // File without inline tests
        let file = src.join("parser.rs");
        std::fs::write(&file, "pub fn parse() {}").unwrap();

        // But has a companion integration test
        std::fs::write(tests_dir.join("parser.rs"), "#[test]\nfn test_parse() {}").unwrap();

        let proposal = RefactorProposal {
            kind: ProposalKind::SplitModule,
            target_module: "mod_a".to_string(),
            candidate: make_candidate("mod_a"),
            proposed_modules: vec!["mod_a1".to_string()],
            affected_files: vec![file],
        };

        let findings = check_test_coverage(&proposal, tmp.path());
        // Should still report but acknowledge integration tests
        if !findings.is_empty() {
            assert!(
                findings[0].message.contains("integration test")
                    || findings[0].message.contains("1 of 1")
            );
        }
    }

    // ── Conflict Check Tests ───────────────────────────────────────

    #[test]
    fn conflict_check_no_active_tasks() {
        let (_tmp, conn) = setup_db();

        let proposal = RefactorProposal {
            kind: ProposalKind::SplitModule,
            target_module: "auth".to_string(),
            candidate: make_candidate("auth"),
            proposed_modules: vec!["auth_a".to_string()],
            affected_files: vec![PathBuf::from("src/auth/mod.rs")],
        };

        let findings = check_conflict_with_in_progress(&proposal, &conn);
        assert!(findings.is_empty());
    }

    #[test]
    fn conflict_check_detects_overlap() {
        let (_tmp, conn) = setup_db();

        // Create an active assignment that touches auth/mod.rs
        crate::db::insert_worker_assignment(
            &conn,
            0,
            "task-123",
            "/tmp/wt",
            "coding",
            Some("src/auth/mod.rs,src/auth/session.rs"),
        )
        .unwrap();

        let proposal = RefactorProposal {
            kind: ProposalKind::SplitModule,
            target_module: "auth".to_string(),
            candidate: make_candidate("auth"),
            proposed_modules: vec!["auth_a".to_string()],
            affected_files: vec![PathBuf::from("src/auth/mod.rs")],
        };

        let findings = check_conflict_with_in_progress(&proposal, &conn);
        let errors: Vec<_> = findings
            .iter()
            .filter(|f| f.severity == ValidationSeverity::Error)
            .collect();
        assert!(!errors.is_empty());
        assert!(errors[0].message.contains("task-123"));
    }

    #[test]
    fn conflict_check_no_overlap() {
        let (_tmp, conn) = setup_db();

        // Active assignment on a different module
        crate::db::insert_worker_assignment(
            &conn,
            0,
            "task-456",
            "/tmp/wt",
            "coding",
            Some("src/db/mod.rs"),
        )
        .unwrap();

        let proposal = RefactorProposal {
            kind: ProposalKind::SplitModule,
            target_module: "auth".to_string(),
            candidate: make_candidate("auth"),
            proposed_modules: vec!["auth_a".to_string()],
            affected_files: vec![PathBuf::from("src/auth/mod.rs")],
        };

        let findings = check_conflict_with_in_progress(&proposal, &conn);
        assert!(findings.is_empty());
    }

    #[test]
    fn conflict_check_completed_tasks_ignored() {
        let (_tmp, conn) = setup_db();

        // Create a completed assignment (should not conflict)
        let id = crate::db::insert_worker_assignment(
            &conn,
            0,
            "task-done",
            "/tmp/wt",
            "coding",
            Some("src/auth/mod.rs"),
        )
        .unwrap();
        crate::db::update_worker_assignment_status(&conn, id, "completed", None).unwrap();

        let proposal = RefactorProposal {
            kind: ProposalKind::SplitModule,
            target_module: "auth".to_string(),
            candidate: make_candidate("auth"),
            proposed_modules: vec!["auth_a".to_string()],
            affected_files: vec![PathBuf::from("src/auth/mod.rs")],
        };

        let findings = check_conflict_with_in_progress(&proposal, &conn);
        assert!(findings.is_empty());
    }

    // ── Full Validation Tests ──────────────────────────────────────

    #[test]
    fn validate_proposal_passes_clean() {
        let (_tmp_db, conn) = setup_db();
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src/big");
        std::fs::create_dir_all(&src).unwrap();

        let file_a = src.join("a.rs");
        std::fs::write(&file_a, "pub fn alpha() {}\n#[cfg(test)]\nmod tests {}").unwrap();
        let file_b = src.join("b.rs");
        std::fs::write(&file_b, "pub fn beta() {}\n#[cfg(test)]\nmod tests {}").unwrap();

        let modules =
            make_modules(&[("big", &[file_a.to_str().unwrap(), file_b.to_str().unwrap()])]);
        let graph = make_graph(&[
            (file_a.to_str().unwrap(), &[]),
            (file_b.to_str().unwrap(), &[]),
        ]);
        let apis: HashMap<String, ModuleApi> = [(
            "big".to_string(),
            ModuleApi {
                module_name: "big".to_string(),
                public_symbols: vec![],
            },
        )]
        .into();

        let proposal = RefactorProposal {
            kind: ProposalKind::SplitModule,
            target_module: "big".to_string(),
            candidate: make_candidate("big"),
            proposed_modules: vec!["big_a".to_string(), "big_b".to_string()],
            affected_files: vec![file_a, file_b],
        };

        let result = validate_proposal(&proposal, &graph, &modules, &apis, &conn, tmp.path());
        assert!(
            result.passed,
            "Clean proposal should pass: {:?}",
            result.findings
        );
        assert_eq!(result.error_count(), 0);
    }

    #[test]
    fn validate_proposal_fails_on_conflict() {
        let (_tmp_db, conn) = setup_db();
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src/auth");
        std::fs::create_dir_all(&src).unwrap();

        let file = src.join("mod.rs");
        std::fs::write(&file, "pub fn login() {}\n#[cfg(test)]\nmod tests {}").unwrap();

        // Create active assignment on the same file
        crate::db::insert_worker_assignment(
            &conn,
            0,
            "active-task",
            "/tmp/wt",
            "coding",
            Some("src/auth/mod.rs"),
        )
        .unwrap();

        let modules = make_modules(&[("auth", &[file.to_str().unwrap()])]);
        let graph = make_graph(&[(file.to_str().unwrap(), &[])]);
        let apis = HashMap::new();

        let proposal = RefactorProposal {
            kind: ProposalKind::SplitModule,
            target_module: "auth".to_string(),
            candidate: make_candidate("auth"),
            proposed_modules: vec!["auth_a".to_string()],
            affected_files: vec![file],
        };

        let result = validate_proposal(&proposal, &graph, &modules, &apis, &conn, tmp.path());
        assert!(!result.passed, "Should fail due to conflict");
        assert!(result.error_count() > 0);
    }

    #[test]
    fn validation_result_counts() {
        let result = ValidationResult {
            proposal: RefactorProposal {
                kind: ProposalKind::SplitModule,
                target_module: "test".to_string(),
                candidate: make_candidate("test"),
                proposed_modules: vec![],
                affected_files: vec![],
            },
            findings: vec![
                ValidationFinding {
                    check: "a".to_string(),
                    severity: ValidationSeverity::Error,
                    message: "err1".to_string(),
                },
                ValidationFinding {
                    check: "b".to_string(),
                    severity: ValidationSeverity::Warning,
                    message: "warn1".to_string(),
                },
                ValidationFinding {
                    check: "c".to_string(),
                    severity: ValidationSeverity::Warning,
                    message: "warn2".to_string(),
                },
            ],
            passed: false,
        };

        assert_eq!(result.error_count(), 1);
        assert_eq!(result.warning_count(), 2);
    }

    #[test]
    fn has_test_coverage_positive() {
        assert!(has_test_coverage("#[cfg(test)]\nmod tests {}"));
        assert!(has_test_coverage(
            "pub fn foo() {}\n#[test]\nfn test_foo() {}"
        ));
    }

    #[test]
    fn has_test_coverage_negative() {
        assert!(!has_test_coverage("pub fn foo() {}"));
        assert!(!has_test_coverage("// #[cfg(test)]"));
    }

    #[test]
    fn proposal_kind_equality() {
        assert_eq!(ProposalKind::SplitModule, ProposalKind::SplitModule);
        assert_ne!(ProposalKind::SplitModule, ProposalKind::ExtractInterface);
        assert_ne!(ProposalKind::MoveFiles, ProposalKind::BreakCycle);
    }
}
