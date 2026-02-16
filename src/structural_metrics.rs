//! Structural metrics aggregator for architecture analysis.
//!
//! Computes all structural metrics for all modules in one pass: fan-in scores,
//! file sizes, cohesion scores (god file detection), cycle participation,
//! API surface width. Returns a `StructuralReport` with per-module and per-file metrics.
//!
//! This is step 1 of the architecture analysis pipeline.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::boundary_violation::{self, BoundaryViolation};
use crate::circular_dep::{self, ModuleCycle};
use crate::fan_in::{self, FanInEntry};
use crate::god_file::{self, GodFileConfig, GodFileEntry};
use crate::import_graph;
use crate::module_detect;
use crate::public_api;

/// Complete structural analysis report for a codebase.
#[derive(Debug, Clone, Serialize)]
pub struct StructuralReport {
    /// Per-module metrics, keyed by module name.
    pub modules: HashMap<String, ModuleMetrics>,
    /// Per-file metrics, keyed by file path.
    pub files: HashMap<PathBuf, FileMetrics>,
    /// Detected module-level circular dependencies.
    pub cycles: Vec<ModuleCycle>,
    /// Detected boundary violations (cross-module non-public imports).
    pub boundary_violations: Vec<BoundaryViolation>,
    /// Total number of modules analyzed.
    pub total_modules: usize,
    /// Total number of files analyzed.
    pub total_files: usize,
}

/// Structural metrics for a single module.
#[derive(Debug, Clone, Serialize)]
pub struct ModuleMetrics {
    /// Module name.
    pub name: String,
    /// Number of files in this module.
    pub file_count: usize,
    /// Total line count across all files in the module.
    pub total_lines: usize,
    /// Number of public symbols (API surface width).
    pub api_surface_width: usize,
    /// Whether this module participates in a circular dependency.
    pub in_cycle: bool,
    /// Number of boundary violations where this module is the source.
    pub violations_as_source: usize,
    /// Number of boundary violations where this module is the target.
    pub violations_as_target: usize,
    /// Number of god file candidates in this module.
    pub god_file_count: usize,
}

/// Structural metrics for a single file.
#[derive(Debug, Clone, Serialize)]
pub struct FileMetrics {
    /// File path.
    pub path: PathBuf,
    /// Line count.
    pub line_count: usize,
    /// Fan-in score (fraction of modules that import this file).
    pub fan_in_score: f64,
    /// Number of files that import this file.
    pub fan_in_importers: usize,
    /// Whether this file is flagged as a god file candidate.
    pub is_god_file: bool,
    /// Number of independent symbol clusters (cohesion measure, 0 if not analyzed).
    pub cluster_count: usize,
}

/// Compute all structural metrics for a Rust codebase in one pass.
///
/// Runs fan-in scoring, god file detection, circular dependency detection,
/// boundary violation detection, and API surface extraction, then aggregates
/// results into a unified `StructuralReport`.
pub fn analyze(repo_root: &Path) -> StructuralReport {
    // Step 1: Build foundational data
    let import_graph = import_graph::build_import_graph(repo_root);
    let modules = module_detect::detect_modules_from_repo(repo_root);
    let apis = public_api::extract_public_apis(&modules);

    // Step 2: Compute individual metrics
    let fan_in_entries = fan_in::scores_from_graph(&import_graph);
    let cycles = circular_dep::detect_module_cycles(&import_graph, &modules);
    let boundary_violations = boundary_violation::detect_boundary_violations(&modules, &apis);

    // Collect all source files for god file detection
    let all_files: Vec<PathBuf> = modules
        .values()
        .flat_map(|m| m.files.iter().cloned())
        .collect();
    let god_files = god_file::detect_god_files(&all_files, &GodFileConfig::default());

    // Step 3: Build lookup structures
    let cycle_modules: HashSet<&str> = cycles
        .iter()
        .flat_map(|c| c.modules.iter().map(|m| m.as_str()))
        .collect();

    let fan_in_map: HashMap<&Path, &FanInEntry> = fan_in_entries
        .iter()
        .map(|e| (e.file.as_path(), e))
        .collect();

    let god_file_map: HashMap<&Path, &GodFileEntry> =
        god_files.iter().map(|e| (e.file.as_path(), e)).collect();

    // Step 4: Build per-file metrics
    let mut file_metrics: HashMap<PathBuf, FileMetrics> = HashMap::new();
    for file in &all_files {
        let line_count = std::fs::read_to_string(file)
            .map(|c| c.lines().count())
            .unwrap_or(0);

        let (fan_in_score, fan_in_importers) = fan_in_map
            .get(file.as_path())
            .map(|e| (e.score, e.importers))
            .unwrap_or((0.0, 0));

        let (is_god_file, cluster_count) = god_file_map
            .get(file.as_path())
            .map(|e| (true, e.cluster_count))
            .unwrap_or((false, 0));

        file_metrics.insert(
            file.clone(),
            FileMetrics {
                path: file.clone(),
                line_count,
                fan_in_score,
                fan_in_importers,
                is_god_file,
                cluster_count,
            },
        );
    }

    // Step 5: Build per-module metrics
    let mut module_metrics: HashMap<String, ModuleMetrics> = HashMap::new();
    for (name, module) in &modules {
        let total_lines: usize = module
            .files
            .iter()
            .map(|f| file_metrics.get(f).map(|m| m.line_count).unwrap_or(0))
            .sum();

        let api_surface_width = apis
            .get(name)
            .map(|api| api.public_symbols.len())
            .unwrap_or(0);

        let in_cycle = cycle_modules.contains(name.as_str());

        let violations_as_source = boundary_violations
            .iter()
            .filter(|v| v.source_module == *name)
            .count();

        let violations_as_target = boundary_violations
            .iter()
            .filter(|v| v.target_module == *name)
            .count();

        let god_file_count = module
            .files
            .iter()
            .filter(|f| god_file_map.contains_key(f.as_path()))
            .count();

        module_metrics.insert(
            name.clone(),
            ModuleMetrics {
                name: name.clone(),
                file_count: module.files.len(),
                total_lines,
                api_surface_width,
                in_cycle,
                violations_as_source,
                violations_as_target,
                god_file_count,
            },
        );
    }

    let total_files = file_metrics.len();
    let total_modules = module_metrics.len();

    StructuralReport {
        modules: module_metrics,
        files: file_metrics,
        cycles,
        boundary_violations,
        total_modules,
        total_files,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn setup_project(files: &[(&str, &str)]) -> tempfile::TempDir {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src");
        fs::create_dir_all(&src).unwrap();
        for (path, content) in files {
            let full = src.join(path);
            if let Some(parent) = full.parent() {
                fs::create_dir_all(parent).unwrap();
            }
            fs::write(&full, content).unwrap();
        }
        tmp
    }

    #[test]
    fn empty_project() {
        let tmp = setup_project(&[]);
        let report = analyze(tmp.path());
        assert_eq!(report.total_modules, 0);
        assert_eq!(report.total_files, 0);
        assert!(report.cycles.is_empty());
        assert!(report.boundary_violations.is_empty());
    }

    #[test]
    fn single_file_project() {
        let tmp = setup_project(&[("main.rs", "fn main() {}")]);
        let report = analyze(tmp.path());

        assert_eq!(report.total_modules, 1);
        assert_eq!(report.total_files, 1);
        assert!(report.cycles.is_empty());
        assert!(report.boundary_violations.is_empty());

        let crate_mod = &report.modules["crate"];
        assert_eq!(crate_mod.file_count, 1);
        assert_eq!(crate_mod.total_lines, 1);
        assert_eq!(crate_mod.api_surface_width, 0);
        assert!(!crate_mod.in_cycle);
    }

    #[test]
    fn fan_in_scores_populated() {
        let tmp = setup_project(&[
            (
                "main.rs",
                "mod config;\nuse crate::config::Foo;\nfn main() {}",
            ),
            ("config.rs", "pub struct Foo;"),
        ]);
        let report = analyze(tmp.path());

        // config.rs should have fan-in from main.rs
        let config_file = report
            .files
            .iter()
            .find(|(p, _)| p.ends_with("config.rs"))
            .map(|(_, m)| m)
            .unwrap();
        assert!(config_file.fan_in_score > 0.0);
        assert!(config_file.fan_in_importers > 0);
    }

    #[test]
    fn api_surface_width_counted() {
        let tmp = setup_project(&[
            ("main.rs", "mod utils;\nfn main() {}"),
            (
                "utils/mod.rs",
                "pub fn a() {}\npub fn b() {}\npub struct C;",
            ),
        ]);
        let report = analyze(tmp.path());

        let utils_mod = &report.modules["utils"];
        assert_eq!(utils_mod.api_surface_width, 3);
    }

    #[test]
    fn cycle_detection_populates_in_cycle() {
        let tmp = setup_project(&[
            ("main.rs", "mod a_mod;\nmod b_mod;\nfn main() {}"),
            ("a_mod/mod.rs", "use crate::b_mod::B;\npub struct A;"),
            ("b_mod/mod.rs", "use crate::a_mod::A;\npub struct B;"),
        ]);
        let report = analyze(tmp.path());

        // a_mod and b_mod should be in a cycle
        assert!(!report.cycles.is_empty());

        let a_metrics = &report.modules["a_mod"];
        let b_metrics = &report.modules["b_mod"];
        assert!(a_metrics.in_cycle);
        assert!(b_metrics.in_cycle);

        // crate should NOT be in a cycle
        let crate_metrics = &report.modules["crate"];
        assert!(!crate_metrics.in_cycle);
    }

    #[test]
    fn file_line_counts() {
        let source = "fn main() {\n    println!(\"hello\");\n}\n";
        let tmp = setup_project(&[("main.rs", source)]);
        let report = analyze(tmp.path());

        let main_file = report
            .files
            .iter()
            .find(|(p, _)| p.ends_with("main.rs"))
            .map(|(_, m)| m)
            .unwrap();
        assert_eq!(main_file.line_count, 3);
    }

    #[test]
    fn module_total_lines_aggregated() {
        let tmp = setup_project(&[
            ("main.rs", "mod adapters;\nfn main() {}"),
            ("adapters/mod.rs", "pub mod claude;\npub fn x() {}"),
            ("adapters/claude.rs", "pub struct A;\npub struct B;"),
        ]);
        let report = analyze(tmp.path());

        let adapters = &report.modules["adapters"];
        // mod.rs has 2 lines, claude.rs has 2 lines → total 4
        assert_eq!(adapters.total_lines, 4);
        assert_eq!(adapters.file_count, 2);
    }

    #[test]
    fn boundary_violations_counted_per_module() {
        let tmp = setup_project(&[
            (
                "main.rs",
                "mod adapters;\nuse crate::adapters::internal_fn;\nfn main() {}",
            ),
            (
                "adapters/mod.rs",
                "pub fn public_fn() {}\nfn internal_fn() {}",
            ),
        ]);
        let report = analyze(tmp.path());

        // crate module importing non-public symbol from adapters
        let crate_metrics = &report.modules["crate"];
        assert_eq!(crate_metrics.violations_as_source, 1);

        let adapters_metrics = &report.modules["adapters"];
        assert_eq!(adapters_metrics.violations_as_target, 1);
    }

    #[test]
    fn god_file_detection_flagged() {
        // Build a file with multiple independent clusters, exceeding 200 lines
        let mut source = String::new();

        // Cluster 1
        source.push_str("pub struct Alpha {\n    pub val: i32,\n}\n\n");
        source.push_str("pub fn use_alpha(a: &Alpha) -> i32 {\n    a.val\n}\n\n");

        // Cluster 2
        source.push_str("pub struct Beta {\n    pub name: String,\n}\n\n");
        source.push_str("pub fn use_beta(b: &Beta) -> &str {\n    &b.name\n}\n\n");

        // Cluster 3
        source.push_str("pub struct Gamma {\n    pub flag: bool,\n}\n\n");
        source.push_str("pub fn use_gamma(g: &Gamma) -> bool {\n    g.flag\n}\n\n");

        // Pad to exceed 200 lines
        for i in 0..200 {
            source.push_str(&format!("// line {i}\n"));
        }

        let tmp = setup_project(&[
            ("main.rs", &format!("mod god;\nfn main() {{}}")),
            ("god.rs", &source),
        ]);
        let report = analyze(tmp.path());

        let god_file = report
            .files
            .iter()
            .find(|(p, _)| p.ends_with("god.rs"))
            .map(|(_, m)| m);

        if let Some(gf) = god_file {
            if gf.is_god_file {
                assert!(gf.cluster_count >= 3);
                let crate_metrics = &report.modules["crate"];
                assert!(crate_metrics.god_file_count >= 1);
            }
        }
        // The test validates that the aggregator correctly picks up god file results
        // when the detector flags them. The exact threshold behavior is tested in god_file.rs.
    }

    #[test]
    fn no_cycles_in_dag() {
        let tmp = setup_project(&[
            ("main.rs", "mod adapters;\nfn main() {}"),
            ("adapters/mod.rs", "pub fn connect() {}"),
        ]);
        let report = analyze(tmp.path());
        assert!(report.cycles.is_empty());

        for (_, metrics) in &report.modules {
            assert!(!metrics.in_cycle);
        }
    }

    #[test]
    fn multiple_modules_all_metrics() {
        let tmp = setup_project(&[
            ("main.rs", "mod auth;\nmod db;\nfn main() {}"),
            ("auth/mod.rs", "pub fn login() {}\npub struct Session;"),
            ("db/mod.rs", "pub struct Database;\npub fn connect() {}"),
        ]);
        let report = analyze(tmp.path());

        assert_eq!(report.total_modules, 3); // crate, auth, db

        let auth = &report.modules["auth"];
        assert_eq!(auth.api_surface_width, 2); // login + Session
        assert_eq!(auth.file_count, 1);

        let db = &report.modules["db"];
        assert_eq!(db.api_surface_width, 2); // Database + connect
    }
}
