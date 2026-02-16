//! Codebase import graph parser for Rust projects.
//!
//! Walks all `.rs` files under a repo root, extracts `use crate::`, `mod`,
//! and `pub use` statements, and builds a directed graph of file-to-file imports
//! as an adjacency list.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Build a directed import graph for a Rust codebase.
///
/// Given a `repo_root`, walks all `.rs` files, parses import statements,
/// and returns an adjacency list where each key is a source file and the
/// value is the list of files it imports from.
///
/// Handles:
/// - `use crate::module::item` → resolves to `src/module.rs` or `src/module/mod.rs`
/// - `mod name;` declarations (non-inline) → resolves to sibling `.rs` or subdir `mod.rs`
/// - `pub use crate::module::item` re-exports
pub fn build_import_graph(repo_root: &Path) -> HashMap<PathBuf, Vec<PathBuf>> {
    let src_root = repo_root.join("src");
    let rs_files = collect_rs_files(&src_root);
    let mut graph: HashMap<PathBuf, Vec<PathBuf>> = HashMap::new();

    for file in &rs_files {
        let deps = extract_imports(file, &src_root);
        // Deduplicate and only keep deps that exist in our file set
        let mut unique_deps: Vec<PathBuf> =
            deps.into_iter().filter(|d| rs_files.contains(d)).collect();
        unique_deps.sort();
        unique_deps.dedup();
        graph.insert(file.clone(), unique_deps);
    }

    graph
}

/// Recursively collect all `.rs` files under a directory.
fn collect_rs_files(dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    collect_rs_files_recursive(dir, &mut files);
    files.sort();
    files
}

fn collect_rs_files_recursive(dir: &Path, files: &mut Vec<PathBuf>) {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_rs_files_recursive(&path, files);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            files.push(path);
        }
    }
}

/// Extract import targets from a single `.rs` file.
///
/// Returns resolved file paths for each import statement found.
fn extract_imports(file: &Path, src_root: &Path) -> Vec<PathBuf> {
    let content = match std::fs::read_to_string(file) {
        Ok(c) => c,
        Err(_) => return Vec::new(),
    };

    let mut deps = Vec::new();

    for line in content.lines() {
        let trimmed = line.trim();

        // Skip comments
        if trimmed.starts_with("//") {
            continue;
        }

        // `use crate::module::...` or `pub use crate::module::...`
        if let Some(path_str) = parse_use_crate(trimmed) {
            if let Some(resolved) = resolve_module_path(src_root, &path_str) {
                deps.push(resolved);
            }
        }

        // `mod name;` (external mod declaration, not inline block)
        if let Some(mod_name) = parse_mod_declaration(trimmed) {
            if let Some(resolved) = resolve_mod_declaration(file, src_root, &mod_name) {
                deps.push(resolved);
            }
        }
    }

    deps
}

/// Parse `use crate::foo::bar` or `pub use crate::foo::bar` and return the first
/// path segment (the module name) as a module path.
///
/// Examples:
/// - `use crate::config::HarnessConfig;` → `["config"]`
/// - `use crate::adapters::claude::ClaudeAdapter;` → `["adapters", "claude"]`
/// - `pub use crate::db::Database;` → `["db"]`
/// - `use crate::estimation::BeadNode;` → `["estimation"]`
fn parse_use_crate(line: &str) -> Option<Vec<String>> {
    // Match: (pub )? use crate:: <path>
    let rest = if let Some(r) = line.strip_prefix("pub use crate::") {
        r
    } else if let Some(r) = line.strip_prefix("use crate::") {
        r
    } else if let Some(r) = line.strip_prefix("pub(crate) use crate::") {
        r
    } else {
        return None;
    };

    // Extract path segments before the final item or group
    // e.g., "config::HarnessConfig;" → ["config"]
    // e.g., "adapters::claude::ClaudeAdapter;" → ["adapters", "claude"]
    // e.g., "db::{Database, something};" → ["db"]
    let path_part = rest.trim_end_matches(';');
    let segments: Vec<&str> = path_part.split("::").collect();

    if segments.is_empty() {
        return None;
    }

    // We want all segments up to but not including the final item/group.
    // The final segment is either a type name (starts with uppercase), a glob (*),
    // a group ({...}), or a module name. If there's only one segment, it IS the module.
    let module_segments: Vec<String> = if segments.len() == 1 {
        vec![segments[0].trim().to_string()]
    } else {
        // Take all segments except the last one (which is the imported item)
        // But if the last segment looks like a module (lowercase, no braces/star),
        // we still want the path up to it as the module path
        let last = segments.last().unwrap().trim();
        if last.starts_with('{') || last == "*" || last.starts_with(|c: char| c.is_uppercase()) {
            segments[..segments.len() - 1]
                .iter()
                .map(|s| s.trim().to_string())
                .collect()
        } else {
            // All segments are module paths (e.g., `use crate::adapters::claude`)
            segments.iter().map(|s| s.trim().to_string()).collect()
        }
    };

    if module_segments.is_empty() {
        None
    } else {
        Some(module_segments)
    }
}

/// Parse `mod name;` (not `mod name { ... }`) and return the module name.
fn parse_mod_declaration(line: &str) -> Option<String> {
    let rest = if let Some(r) = line.strip_prefix("pub mod ") {
        r
    } else if let Some(r) = line.strip_prefix("pub(crate) mod ") {
        r
    } else if let Some(r) = line.strip_prefix("mod ") {
        r
    } else {
        return None;
    };

    // Must end with `;` (not `{` which is an inline module)
    let rest = rest.trim();
    if rest.ends_with(';') && !rest.contains('{') {
        let name = rest.trim_end_matches(';').trim();
        // Skip `mod tests` — it's an inline test module, not an external file
        if name == "tests" {
            return None;
        }
        if name.chars().all(|c| c.is_alphanumeric() || c == '_') {
            return Some(name.to_string());
        }
    }
    None
}

/// Resolve a crate module path (e.g., `["config"]` or `["adapters", "claude"]`)
/// to a file path under `src_root`.
fn resolve_module_path(src_root: &Path, segments: &[String]) -> Option<PathBuf> {
    if segments.is_empty() {
        return None;
    }

    // Build the path: src/seg1/seg2/...
    let mut path = src_root.to_path_buf();
    for seg in &segments[..segments.len() - 1] {
        path.push(seg);
    }
    let last = &segments[segments.len() - 1];

    // Try: path/last.rs
    let as_file = path.join(format!("{last}.rs"));
    if as_file.exists() {
        return Some(as_file);
    }

    // Try: path/last/mod.rs
    let as_mod = path.join(last).join("mod.rs");
    if as_mod.exists() {
        return Some(as_mod);
    }

    None
}

/// Resolve a `mod name;` declaration relative to the declaring file.
fn resolve_mod_declaration(
    declaring_file: &Path,
    src_root: &Path,
    mod_name: &str,
) -> Option<PathBuf> {
    let parent = declaring_file.parent()?;

    // If the declaring file is `mod.rs` or `main.rs` or `lib.rs`,
    // look for sibling `name.rs` or `name/mod.rs` in the same directory.
    let file_stem = declaring_file.file_stem()?.to_str()?;

    if file_stem == "mod" || file_stem == "main" || file_stem == "lib" {
        // Look in same directory
        let as_file = parent.join(format!("{mod_name}.rs"));
        if as_file.exists() {
            return Some(as_file);
        }
        let as_mod = parent.join(mod_name).join("mod.rs");
        if as_mod.exists() {
            return Some(as_mod);
        }
    } else {
        // For `src/foo.rs` declaring `mod bar;`, look in `src/foo/bar.rs` or `src/foo/bar/mod.rs`
        let dir = parent.join(file_stem);
        let as_file = dir.join(format!("{mod_name}.rs"));
        if as_file.exists() {
            return Some(as_file);
        }
        let as_mod = dir.join(mod_name).join("mod.rs");
        if as_mod.exists() {
            return Some(as_mod);
        }
    }

    // Fallback: check from src_root (for top-level modules referenced from non-standard locations)
    let _ = src_root; // already tried via parent logic
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    /// Helper: create a temp Rust project with given files.
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
        let graph = build_import_graph(tmp.path());
        assert!(graph.is_empty());
    }

    #[test]
    fn single_file_no_imports() {
        let tmp = setup_project(&[("main.rs", "fn main() {}")]);
        let graph = build_import_graph(tmp.path());
        assert_eq!(graph.len(), 1);
        let deps = graph.values().next().unwrap();
        assert!(deps.is_empty());
    }

    #[test]
    fn use_crate_import() {
        let tmp = setup_project(&[
            (
                "main.rs",
                "mod config;\nuse crate::config::HarnessConfig;\nfn main() {}",
            ),
            ("config.rs", "pub struct HarnessConfig;"),
        ]);
        let graph = build_import_graph(tmp.path());
        let src = tmp.path().join("src");

        let main_deps = &graph[&src.join("main.rs")];
        assert!(main_deps.contains(&src.join("config.rs")));

        let config_deps = &graph[&src.join("config.rs")];
        assert!(config_deps.is_empty());
    }

    #[test]
    fn mod_declaration_resolves() {
        let tmp = setup_project(&[
            ("main.rs", "mod utils;\nfn main() {}"),
            ("utils.rs", "pub fn helper() {}"),
        ]);
        let graph = build_import_graph(tmp.path());
        let src = tmp.path().join("src");

        let main_deps = &graph[&src.join("main.rs")];
        assert!(main_deps.contains(&src.join("utils.rs")));
    }

    #[test]
    fn mod_declaration_subdir() {
        let tmp = setup_project(&[
            ("main.rs", "mod adapters;\nfn main() {}"),
            ("adapters/mod.rs", "pub mod claude;\npub fn adapt() {}"),
            ("adapters/claude.rs", "pub struct ClaudeAdapter;"),
        ]);
        let graph = build_import_graph(tmp.path());
        let src = tmp.path().join("src");

        // main.rs → adapters/mod.rs
        let main_deps = &graph[&src.join("main.rs")];
        assert!(main_deps.contains(&src.join("adapters/mod.rs")));

        // adapters/mod.rs → adapters/claude.rs
        let adapters_deps = &graph[&src.join("adapters/mod.rs")];
        assert!(adapters_deps.contains(&src.join("adapters/claude.rs")));
    }

    #[test]
    fn pub_use_reexport() {
        let tmp = setup_project(&[
            (
                "main.rs",
                "mod db;\npub use crate::db::Database;\nfn main() {}",
            ),
            ("db.rs", "pub struct Database;"),
        ]);
        let graph = build_import_graph(tmp.path());
        let src = tmp.path().join("src");

        let main_deps = &graph[&src.join("main.rs")];
        // Both `mod db;` and `pub use crate::db::Database` resolve to db.rs
        assert!(main_deps.contains(&src.join("db.rs")));
    }

    #[test]
    fn nested_crate_path() {
        let tmp = setup_project(&[
            (
                "main.rs",
                "mod adapters;\nuse crate::adapters::claude::ClaudeAdapter;\nfn main() {}",
            ),
            ("adapters/mod.rs", "pub mod claude;"),
            ("adapters/claude.rs", "pub struct ClaudeAdapter;"),
        ]);
        let graph = build_import_graph(tmp.path());
        let src = tmp.path().join("src");

        let main_deps = &graph[&src.join("main.rs")];
        // use crate::adapters::claude::ClaudeAdapter → adapters/claude.rs
        assert!(main_deps.contains(&src.join("adapters/claude.rs")));
    }

    #[test]
    fn comments_ignored() {
        let tmp = setup_project(&[
            ("main.rs", "// use crate::config::Foo;\nfn main() {}"),
            ("config.rs", "pub struct Foo;"),
        ]);
        let graph = build_import_graph(tmp.path());
        let src = tmp.path().join("src");

        let main_deps = &graph[&src.join("main.rs")];
        assert!(main_deps.is_empty());
    }

    #[test]
    fn deduplicates_deps() {
        let tmp = setup_project(&[
            (
                "main.rs",
                "mod config;\nuse crate::config::Foo;\nuse crate::config::Bar;\nfn main() {}",
            ),
            ("config.rs", "pub struct Foo;\npub struct Bar;"),
        ]);
        let graph = build_import_graph(tmp.path());
        let src = tmp.path().join("src");

        let main_deps = &graph[&src.join("main.rs")];
        // config.rs should appear only once despite multiple imports
        assert_eq!(
            main_deps
                .iter()
                .filter(|d| **d == src.join("config.rs"))
                .count(),
            1
        );
    }

    #[test]
    fn use_group_import() {
        let tmp = setup_project(&[
            (
                "main.rs",
                "mod db;\nuse crate::db::{Database, Connection};\nfn main() {}",
            ),
            ("db.rs", "pub struct Database;\npub struct Connection;"),
        ]);
        let graph = build_import_graph(tmp.path());
        let src = tmp.path().join("src");

        let main_deps = &graph[&src.join("main.rs")];
        assert!(main_deps.contains(&src.join("db.rs")));
    }

    #[test]
    fn glob_import() {
        let tmp = setup_project(&[
            ("main.rs", "mod utils;\nuse crate::utils::*;\nfn main() {}"),
            ("utils.rs", "pub fn helper() {}"),
        ]);
        let graph = build_import_graph(tmp.path());
        let src = tmp.path().join("src");

        let main_deps = &graph[&src.join("main.rs")];
        assert!(main_deps.contains(&src.join("utils.rs")));
    }

    #[test]
    fn parse_use_crate_basic() {
        assert_eq!(
            parse_use_crate("use crate::config::HarnessConfig;"),
            Some(vec!["config".to_string()])
        );
    }

    #[test]
    fn parse_use_crate_nested() {
        assert_eq!(
            parse_use_crate("use crate::adapters::claude::ClaudeAdapter;"),
            Some(vec!["adapters".to_string(), "claude".to_string()])
        );
    }

    #[test]
    fn parse_use_crate_group() {
        assert_eq!(
            parse_use_crate("use crate::db::{Database, Connection};"),
            Some(vec!["db".to_string()])
        );
    }

    #[test]
    fn parse_use_crate_pub() {
        assert_eq!(
            parse_use_crate("pub use crate::db::Database;"),
            Some(vec!["db".to_string()])
        );
    }

    #[test]
    fn parse_use_crate_glob() {
        assert_eq!(
            parse_use_crate("use crate::utils::*;"),
            Some(vec!["utils".to_string()])
        );
    }

    #[test]
    fn parse_use_crate_not_crate() {
        assert_eq!(parse_use_crate("use std::collections::HashMap;"), None);
    }

    #[test]
    fn parse_mod_declaration_basic() {
        assert_eq!(
            parse_mod_declaration("mod config;"),
            Some("config".to_string())
        );
    }

    #[test]
    fn parse_mod_declaration_pub() {
        assert_eq!(
            parse_mod_declaration("pub mod config;"),
            Some("config".to_string())
        );
    }

    #[test]
    fn parse_mod_declaration_inline_skipped() {
        assert_eq!(parse_mod_declaration("mod tests {"), None);
    }

    #[test]
    fn parse_mod_declaration_tests_skipped() {
        assert_eq!(parse_mod_declaration("mod tests;"), None);
    }

    #[test]
    fn nonexistent_import_filtered() {
        let tmp = setup_project(&[("main.rs", "use crate::nonexistent::Foo;\nfn main() {}")]);
        let graph = build_import_graph(tmp.path());
        let src = tmp.path().join("src");

        let main_deps = &graph[&src.join("main.rs")];
        assert!(main_deps.is_empty());
    }
}
