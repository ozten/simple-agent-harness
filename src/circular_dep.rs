//! Circular dependency detection at the module level.
//!
//! Given a file-level import graph and module boundaries, lifts edges to module
//! granularity and detects cycles. Module A importing Module B while B imports A
//! makes affected sets transitive in both directions.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use crate::module_detect::Module;

/// A detected circular dependency between modules.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ModuleCycle {
    /// Module names participating in the cycle, sorted alphabetically.
    pub modules: Vec<String>,
}

/// Detect circular dependencies at the module level.
///
/// Lifts file-level import edges to module-level edges, then finds strongly
/// connected components (cycles) in the module graph.
///
/// # Arguments
/// * `import_graph` - File-level import graph (from `import_graph::build_import_graph`)
/// * `modules` - Detected module boundaries (from `module_detect::detect_modules`)
///
/// # Returns
/// A list of `ModuleCycle`s. Each cycle contains 2+ module names that form a
/// circular dependency. Returns empty vec if no cycles exist.
pub fn detect_module_cycles(
    import_graph: &HashMap<PathBuf, Vec<PathBuf>>,
    modules: &HashMap<String, Module>,
) -> Vec<ModuleCycle> {
    // Build file-to-module lookup
    let file_to_module = build_file_to_module_map(modules);

    // Lift file edges to module edges
    let module_adj = build_module_adjacency(import_graph, &file_to_module);

    // Find SCCs in the module graph
    let all_modules: HashSet<&str> = module_adj.keys().map(|s| s.as_str()).collect();
    if all_modules.is_empty() {
        return Vec::new();
    }

    let adj_refs: HashMap<&str, Vec<&str>> = module_adj
        .iter()
        .map(|(k, v)| (k.as_str(), v.iter().map(|s| s.as_str()).collect()))
        .collect();

    let sccs = tarjan_scc(&all_modules, &adj_refs);

    let mut cycles: Vec<ModuleCycle> = sccs
        .into_iter()
        .map(|mut scc| {
            scc.sort();
            ModuleCycle { modules: scc }
        })
        .collect();
    cycles.sort_by(|a, b| a.modules.cmp(&b.modules));
    cycles
}

/// Map each file path to its owning module name.
fn build_file_to_module_map(modules: &HashMap<String, Module>) -> HashMap<&Path, &str> {
    let mut map: HashMap<&Path, &str> = HashMap::new();
    for (name, module) in modules {
        for file in &module.files {
            map.insert(file.as_path(), name.as_str());
        }
    }
    map
}

/// Lift file-level import edges to module-level edges.
///
/// For each file→dep edge in the import graph, if the file and dep belong to
/// different modules, record a module-level edge. Self-edges (within the same
/// module) are excluded.
fn build_module_adjacency(
    import_graph: &HashMap<PathBuf, Vec<PathBuf>>,
    file_to_module: &HashMap<&Path, &str>,
) -> HashMap<String, HashSet<String>> {
    let mut adj: HashMap<String, HashSet<String>> = HashMap::new();

    // Ensure all modules that appear in file_to_module have entries
    for &module_name in file_to_module.values() {
        adj.entry(module_name.to_string()).or_default();
    }

    for (source_file, deps) in import_graph {
        let source_module = match file_to_module.get(source_file.as_path()) {
            Some(m) => *m,
            None => continue,
        };

        for dep_file in deps {
            let dep_module = match file_to_module.get(dep_file.as_path()) {
                Some(m) => *m,
                None => continue,
            };

            // Skip self-edges (imports within the same module)
            if source_module != dep_module {
                adj.entry(source_module.to_string())
                    .or_default()
                    .insert(dep_module.to_string());
            }
        }
    }

    adj
}

/// Tarjan's strongly connected components algorithm.
///
/// Returns SCCs with size > 1 (true cycles) plus self-loops.
fn tarjan_scc<'a>(
    nodes: &HashSet<&'a str>,
    adj: &HashMap<&'a str, Vec<&'a str>>,
) -> Vec<Vec<String>> {
    struct State<'a> {
        adj: &'a HashMap<&'a str, Vec<&'a str>>,
        index_counter: usize,
        stack: Vec<&'a str>,
        on_stack: HashSet<&'a str>,
        index: HashMap<&'a str, usize>,
        lowlink: HashMap<&'a str, usize>,
        result: Vec<Vec<String>>,
    }

    impl<'a> State<'a> {
        fn strongconnect(&mut self, v: &'a str) {
            self.index.insert(v, self.index_counter);
            self.lowlink.insert(v, self.index_counter);
            self.index_counter += 1;
            self.stack.push(v);
            self.on_stack.insert(v);

            if let Some(neighbors) = self.adj.get(v) {
                let neighbors = neighbors.clone();
                for w in &neighbors {
                    if !self.index.contains_key(w) {
                        self.strongconnect(w);
                        let w_low = self.lowlink[w];
                        let v_low = self.lowlink.get_mut(v).unwrap();
                        if w_low < *v_low {
                            *v_low = w_low;
                        }
                    } else if self.on_stack.contains(w) {
                        let w_idx = self.index[w];
                        let v_low = self.lowlink.get_mut(v).unwrap();
                        if w_idx < *v_low {
                            *v_low = w_idx;
                        }
                    }
                }
            }

            if self.lowlink[v] == self.index[v] {
                let mut component = Vec::new();
                loop {
                    let w = self.stack.pop().unwrap();
                    self.on_stack.remove(w);
                    component.push(w.to_string());
                    if w == v {
                        break;
                    }
                }
                if component.len() > 1 {
                    self.result.push(component);
                } else if component.len() == 1 {
                    // Check for self-loop
                    let node = component[0].as_str();
                    if let Some(neighbors) = self.adj.get(node) {
                        if neighbors.contains(&node) {
                            self.result.push(component);
                        }
                    }
                }
            }
        }
    }

    let mut state = State {
        adj,
        index_counter: 0,
        stack: Vec::new(),
        on_stack: HashSet::new(),
        index: HashMap::new(),
        lowlink: HashMap::new(),
        result: Vec::new(),
    };

    let mut sorted_nodes: Vec<&str> = nodes.iter().copied().collect();
    sorted_nodes.sort();

    for &node in &sorted_nodes {
        if !state.index.contains_key(node) {
            state.strongconnect(node);
        }
    }

    state.result.sort();
    state.result
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: build modules map from a simple spec.
    /// Each entry: (module_name, root_path, &[file_paths], entry_point)
    fn make_modules(specs: &[(&str, &str, &[&str], Option<&str>)]) -> HashMap<String, Module> {
        let mut modules = HashMap::new();
        for (name, root, files, entry) in specs {
            modules.insert(
                name.to_string(),
                Module {
                    name: name.to_string(),
                    root_path: PathBuf::from(root),
                    files: files.iter().map(|f| PathBuf::from(f)).collect(),
                    has_entry_point: entry.is_some(),
                    entry_point: entry.map(|e| PathBuf::from(e)),
                    submodules: Vec::new(),
                },
            );
        }
        modules
    }

    /// Helper: build import graph from edges.
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

    #[test]
    fn no_cycles_empty() {
        let graph = HashMap::new();
        let modules = HashMap::new();
        let cycles = detect_module_cycles(&graph, &modules);
        assert!(cycles.is_empty());
    }

    #[test]
    fn no_cycles_dag() {
        // crate(main.rs) -> adapters(mod.rs, claude.rs) -> db(db.rs)
        let modules = make_modules(&[
            ("crate", "src", &["src/main.rs"], Some("src/main.rs")),
            (
                "adapters",
                "src/adapters",
                &["src/adapters/mod.rs", "src/adapters/claude.rs"],
                Some("src/adapters/mod.rs"),
            ),
            ("db", "src/db", &["src/db/mod.rs"], Some("src/db/mod.rs")),
        ]);

        let graph = make_graph(&[
            ("src/main.rs", &["src/adapters/mod.rs"]),
            ("src/adapters/mod.rs", &["src/adapters/claude.rs"]),
            ("src/adapters/claude.rs", &["src/db/mod.rs"]),
            ("src/db/mod.rs", &[]),
        ]);

        let cycles = detect_module_cycles(&graph, &modules);
        assert!(cycles.is_empty());
    }

    #[test]
    fn two_module_cycle() {
        // adapters -> db, db -> adapters
        let modules = make_modules(&[
            (
                "adapters",
                "src/adapters",
                &["src/adapters/mod.rs"],
                Some("src/adapters/mod.rs"),
            ),
            ("db", "src/db", &["src/db/mod.rs"], Some("src/db/mod.rs")),
        ]);

        let graph = make_graph(&[
            ("src/adapters/mod.rs", &["src/db/mod.rs"]),
            ("src/db/mod.rs", &["src/adapters/mod.rs"]),
        ]);

        let cycles = detect_module_cycles(&graph, &modules);
        assert_eq!(cycles.len(), 1);
        assert_eq!(cycles[0].modules, vec!["adapters", "db"]);
    }

    #[test]
    fn three_module_cycle() {
        // a -> b -> c -> a
        let modules = make_modules(&[
            ("a", "src/a", &["src/a/mod.rs"], Some("src/a/mod.rs")),
            ("b", "src/b", &["src/b/mod.rs"], Some("src/b/mod.rs")),
            ("c", "src/c", &["src/c/mod.rs"], Some("src/c/mod.rs")),
        ]);

        let graph = make_graph(&[
            ("src/a/mod.rs", &["src/b/mod.rs"]),
            ("src/b/mod.rs", &["src/c/mod.rs"]),
            ("src/c/mod.rs", &["src/a/mod.rs"]),
        ]);

        let cycles = detect_module_cycles(&graph, &modules);
        assert_eq!(cycles.len(), 1);
        assert_eq!(cycles[0].modules, vec!["a", "b", "c"]);
    }

    #[test]
    fn multiple_independent_cycles() {
        // Cycle 1: a <-> b
        // Cycle 2: c <-> d
        // e is independent
        let modules = make_modules(&[
            ("a", "src/a", &["src/a/mod.rs"], Some("src/a/mod.rs")),
            ("b", "src/b", &["src/b/mod.rs"], Some("src/b/mod.rs")),
            ("c", "src/c", &["src/c/mod.rs"], Some("src/c/mod.rs")),
            ("d", "src/d", &["src/d/mod.rs"], Some("src/d/mod.rs")),
            ("e", "src/e", &["src/e/mod.rs"], Some("src/e/mod.rs")),
        ]);

        let graph = make_graph(&[
            ("src/a/mod.rs", &["src/b/mod.rs"]),
            ("src/b/mod.rs", &["src/a/mod.rs"]),
            ("src/c/mod.rs", &["src/d/mod.rs"]),
            ("src/d/mod.rs", &["src/c/mod.rs"]),
            ("src/e/mod.rs", &[]),
        ]);

        let cycles = detect_module_cycles(&graph, &modules);
        assert_eq!(cycles.len(), 2);
        assert_eq!(cycles[0].modules, vec!["a", "b"]);
        assert_eq!(cycles[1].modules, vec!["c", "d"]);
    }

    #[test]
    fn intra_module_imports_not_cycles() {
        // Two files in the same module importing each other is NOT a module-level cycle
        let modules = make_modules(&[(
            "adapters",
            "src/adapters",
            &["src/adapters/mod.rs", "src/adapters/claude.rs"],
            Some("src/adapters/mod.rs"),
        )]);

        let graph = make_graph(&[
            ("src/adapters/mod.rs", &["src/adapters/claude.rs"]),
            ("src/adapters/claude.rs", &["src/adapters/mod.rs"]),
        ]);

        let cycles = detect_module_cycles(&graph, &modules);
        assert!(cycles.is_empty());
    }

    #[test]
    fn mixed_dag_and_cycle() {
        // DAG: crate -> a, crate -> b
        // Cycle: a -> b -> a
        let modules = make_modules(&[
            ("crate", "src", &["src/main.rs"], Some("src/main.rs")),
            ("a", "src/a", &["src/a/mod.rs"], Some("src/a/mod.rs")),
            ("b", "src/b", &["src/b/mod.rs"], Some("src/b/mod.rs")),
        ]);

        let graph = make_graph(&[
            ("src/main.rs", &["src/a/mod.rs", "src/b/mod.rs"]),
            ("src/a/mod.rs", &["src/b/mod.rs"]),
            ("src/b/mod.rs", &["src/a/mod.rs"]),
        ]);

        let cycles = detect_module_cycles(&graph, &modules);
        assert_eq!(cycles.len(), 1);
        assert_eq!(cycles[0].modules, vec!["a", "b"]);
    }

    #[test]
    fn files_not_in_any_module_ignored() {
        // File "orphan.rs" not in any module — should not cause issues
        let modules = make_modules(&[("a", "src/a", &["src/a/mod.rs"], Some("src/a/mod.rs"))]);

        let graph = make_graph(&[
            ("src/a/mod.rs", &["src/orphan.rs"]),
            ("src/orphan.rs", &["src/a/mod.rs"]),
        ]);

        let cycles = detect_module_cycles(&graph, &modules);
        assert!(cycles.is_empty());
    }

    #[test]
    fn single_module_no_cycle() {
        let modules = make_modules(&[("crate", "src", &["src/main.rs"], Some("src/main.rs"))]);

        let graph = make_graph(&[("src/main.rs", &[])]);

        let cycles = detect_module_cycles(&graph, &modules);
        assert!(cycles.is_empty());
    }

    #[test]
    fn multiple_files_create_one_module_edge() {
        // Multiple files in module A importing files in module B — still one edge A->B
        let modules = make_modules(&[
            (
                "a",
                "src/a",
                &["src/a/mod.rs", "src/a/helper.rs"],
                Some("src/a/mod.rs"),
            ),
            (
                "b",
                "src/b",
                &["src/b/mod.rs", "src/b/util.rs"],
                Some("src/b/mod.rs"),
            ),
        ]);

        let graph = make_graph(&[
            ("src/a/mod.rs", &["src/b/mod.rs"]),
            ("src/a/helper.rs", &["src/b/util.rs"]),
            ("src/b/mod.rs", &[]),
            ("src/b/util.rs", &[]),
        ]);

        let cycles = detect_module_cycles(&graph, &modules);
        assert!(cycles.is_empty()); // one-way, no cycle
    }

    #[test]
    fn cycle_via_multiple_file_edges() {
        // a/mod.rs -> b/mod.rs, b/util.rs -> a/helper.rs
        // Creates module cycle a <-> b even though it's through different files
        let modules = make_modules(&[
            (
                "a",
                "src/a",
                &["src/a/mod.rs", "src/a/helper.rs"],
                Some("src/a/mod.rs"),
            ),
            (
                "b",
                "src/b",
                &["src/b/mod.rs", "src/b/util.rs"],
                Some("src/b/mod.rs"),
            ),
        ]);

        let graph = make_graph(&[
            ("src/a/mod.rs", &["src/b/mod.rs"]),
            ("src/a/helper.rs", &[]),
            ("src/b/mod.rs", &[]),
            ("src/b/util.rs", &["src/a/helper.rs"]),
        ]);

        let cycles = detect_module_cycles(&graph, &modules);
        assert_eq!(cycles.len(), 1);
        assert_eq!(cycles[0].modules, vec!["a", "b"]);
    }

    #[test]
    fn integration_with_real_structures() {
        // Simulate a real-ish project:
        // crate -> adapters, crate -> db, crate -> config
        // adapters -> db (ok, one-way)
        // db -> config (ok)
        // config -> adapters (creates cycle: adapters -> db -> config -> adapters)
        let modules = make_modules(&[
            ("crate", "src", &["src/main.rs"], Some("src/main.rs")),
            (
                "adapters",
                "src/adapters",
                &["src/adapters/mod.rs"],
                Some("src/adapters/mod.rs"),
            ),
            ("db", "src/db", &["src/db/mod.rs"], Some("src/db/mod.rs")),
            (
                "config",
                "src/config",
                &["src/config/mod.rs"],
                Some("src/config/mod.rs"),
            ),
        ]);

        let graph = make_graph(&[
            (
                "src/main.rs",
                &["src/adapters/mod.rs", "src/db/mod.rs", "src/config/mod.rs"],
            ),
            ("src/adapters/mod.rs", &["src/db/mod.rs"]),
            ("src/db/mod.rs", &["src/config/mod.rs"]),
            ("src/config/mod.rs", &["src/adapters/mod.rs"]),
        ]);

        let cycles = detect_module_cycles(&graph, &modules);
        assert_eq!(cycles.len(), 1);
        assert_eq!(cycles[0].modules, vec!["adapters", "config", "db"]);
    }
}
