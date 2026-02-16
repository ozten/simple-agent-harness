//! Boundary violation detector for module imports.
//!
//! Detects modules reaching into each other's internals rather than using
//! public APIs. For each cross-module `use crate::` import, checks whether
//! the imported symbol is part of the target module's public API surface.
//! Non-public symbol accesses are reported as boundary violations.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use crate::module_detect::Module;
use crate::public_api::ModuleApi;

/// A boundary violation: a cross-module import of a non-public symbol.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct BoundaryViolation {
    /// The module that contains the violating import.
    pub source_module: String,
    /// The module whose internals are being accessed.
    pub target_module: String,
    /// The non-public symbol being accessed.
    pub symbol: String,
    /// The file containing the violating import.
    pub source_file: String,
    /// The import line that caused the violation.
    pub import_line: String,
}

/// Detect boundary violations across all modules.
///
/// For each file in each module, parses `use crate::` imports, resolves the
/// target module, and checks whether the imported symbol appears in the target
/// module's public API. If not, it is a boundary violation.
pub fn detect_boundary_violations(
    modules: &HashMap<String, Module>,
    apis: &HashMap<String, ModuleApi>,
) -> Vec<BoundaryViolation> {
    let file_to_module = build_file_to_module(modules);
    let mut violations = Vec::new();

    for (module_name, module) in modules {
        for file_path in &module.files {
            let content = match std::fs::read_to_string(file_path) {
                Ok(c) => c,
                Err(_) => continue,
            };

            let file_name = file_path
                .file_name()
                .map(|f| f.to_string_lossy().to_string())
                .unwrap_or_default();

            let imports = extract_cross_module_imports(&content);

            for import in imports {
                // Resolve which module this import targets
                let target_module_name =
                    match resolve_target_module(&import.module_path, modules, &file_to_module) {
                        Some(name) => name,
                        None => continue,
                    };

                // Skip intra-module imports
                if target_module_name == *module_name {
                    continue;
                }

                // Check each imported symbol against the target's public API
                let target_api = match apis.get(&target_module_name) {
                    Some(api) => api,
                    None => continue,
                };

                let public_names: HashSet<&str> = target_api
                    .public_symbols
                    .iter()
                    .map(|s| s.name.as_str())
                    .collect();

                for sym_name in &import.symbols {
                    // Glob imports and group-level imports without specific symbols can't be checked
                    if sym_name == "*" || sym_name.starts_with('{') {
                        continue;
                    }

                    if !public_names.contains(sym_name.as_str()) {
                        violations.push(BoundaryViolation {
                            source_module: module_name.clone(),
                            target_module: target_module_name.clone(),
                            symbol: sym_name.clone(),
                            source_file: file_name.clone(),
                            import_line: import.raw_line.clone(),
                        });
                    }
                }
            }
        }
    }

    violations.sort_by(|a, b| {
        a.source_module
            .cmp(&b.source_module)
            .then(a.target_module.cmp(&b.target_module))
            .then(a.symbol.cmp(&b.symbol))
    });

    violations
}

/// A parsed cross-module import statement.
#[derive(Debug, Clone)]
struct CrossModuleImport {
    /// The module path segments (e.g., `["adapters", "claude"]` from `use crate::adapters::claude::Foo`).
    module_path: Vec<String>,
    /// The specific symbols being imported (e.g., `["Foo", "Bar"]`).
    symbols: Vec<String>,
    /// The raw import line.
    raw_line: String,
}

/// Extract cross-module import statements from source text.
///
/// Parses `use crate::...` lines and extracts the module path and imported symbols.
fn extract_cross_module_imports(source: &str) -> Vec<CrossModuleImport> {
    let mut imports = Vec::new();
    let mut in_block_comment = false;

    for line in source.lines() {
        let trimmed = line.trim();

        // Track block comments
        if in_block_comment {
            if trimmed.contains("*/") {
                in_block_comment = false;
            }
            continue;
        }
        if trimmed.starts_with("/*") {
            in_block_comment = true;
            if trimmed.contains("*/") {
                in_block_comment = false;
            }
            continue;
        }

        // Skip line comments and attributes
        if trimmed.starts_with("//") || trimmed.starts_with('#') {
            continue;
        }

        // Parse `use crate::...` or `pub use crate::...`
        let rest = if let Some(r) = trimmed.strip_prefix("pub use crate::") {
            r
        } else if let Some(r) = trimmed.strip_prefix("use crate::") {
            r
        } else if let Some(r) = trimmed.strip_prefix("pub(crate) use crate::") {
            r
        } else {
            continue;
        };

        let rest = rest.trim_end_matches(';');
        let segments: Vec<&str> = rest.split("::").collect();

        if segments.is_empty() {
            continue;
        }

        // Determine module path vs imported symbols
        // E.g., `config::HarnessConfig` → module_path=["config"], symbols=["HarnessConfig"]
        // E.g., `adapters::claude::ClaudeAdapter` → module_path=["adapters", "claude"], symbols=["ClaudeAdapter"]
        // E.g., `db::{Database, Connection}` → module_path=["db"], symbols=["Database", "Connection"]
        // E.g., `utils::*` → module_path=["utils"], symbols=["*"]

        let (module_path, symbols) = split_module_and_symbols(&segments);

        if module_path.is_empty() || symbols.is_empty() {
            continue;
        }

        imports.push(CrossModuleImport {
            module_path,
            symbols,
            raw_line: trimmed.to_string(),
        });
    }

    imports
}

/// Split parsed path segments into module path and imported symbol names.
fn split_module_and_symbols(segments: &[&str]) -> (Vec<String>, Vec<String>) {
    if segments.is_empty() {
        return (Vec::new(), Vec::new());
    }

    if segments.len() == 1 {
        let seg = segments[0].trim();
        // Single segment: could be a module-level import like `use crate::config;`
        // This means importing the module itself, not a symbol from it.
        // We can't check this against public API, so skip.
        return (vec![seg.to_string()], Vec::new());
    }

    let last = segments.last().unwrap().trim();

    if last.starts_with('{') {
        // Group import: `db::{Database, Connection}`
        let module_path: Vec<String> = segments[..segments.len() - 1]
            .iter()
            .map(|s| s.trim().to_string())
            .collect();
        let symbols = parse_group_import(last);
        (module_path, symbols)
    } else if last == "*" {
        // Glob import
        let module_path: Vec<String> = segments[..segments.len() - 1]
            .iter()
            .map(|s| s.trim().to_string())
            .collect();
        (module_path, vec!["*".to_string()])
    } else if last.starts_with(|c: char| c.is_uppercase()) {
        // Type/symbol import: `config::HarnessConfig`
        let module_path: Vec<String> = segments[..segments.len() - 1]
            .iter()
            .map(|s| s.trim().to_string())
            .collect();
        (module_path, vec![last.to_string()])
    } else {
        // Lowercase last segment could be a nested module or a function.
        // E.g., `adapters::claude` — this might be accessing module `adapters::claude`.
        // Or `config::load_config` — accessing function `load_config` from module `config`.
        // Heuristic: if the segment contains underscores or is all lowercase, treat as a
        // function/const name being imported from the preceding module path.
        if segments.len() >= 2 {
            let module_path: Vec<String> = segments[..segments.len() - 1]
                .iter()
                .map(|s| s.trim().to_string())
                .collect();
            (module_path, vec![last.to_string()])
        } else {
            (
                segments.iter().map(|s| s.trim().to_string()).collect(),
                Vec::new(),
            )
        }
    }
}

/// Parse a group import like `{Database, Connection}` into individual symbol names.
fn parse_group_import(group: &str) -> Vec<String> {
    let inner = group.trim_start_matches('{').trim_end_matches('}').trim();

    inner
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Build a map from file path to owning module name.
fn build_file_to_module(modules: &HashMap<String, Module>) -> HashMap<PathBuf, String> {
    let mut map = HashMap::new();
    for (name, module) in modules {
        for file in &module.files {
            map.insert(file.clone(), name.clone());
        }
    }
    map
}

/// Resolve a module path (e.g., `["adapters", "claude"]`) to a module name.
///
/// First tries to match the full path as a module name (e.g., `"adapters::claude"`),
/// then tries progressively shorter prefixes (e.g., `"adapters"`).
fn resolve_target_module(
    module_path: &[String],
    modules: &HashMap<String, Module>,
    _file_to_module: &HashMap<PathBuf, String>,
) -> Option<String> {
    // Try full path: "adapters::claude"
    let full_name = module_path.join("::");
    if modules.contains_key(&full_name) {
        return Some(full_name);
    }

    // Try progressively shorter prefixes
    for i in (1..module_path.len()).rev() {
        let prefix = module_path[..i].join("::");
        if modules.contains_key(&prefix) {
            return Some(prefix);
        }
    }

    // Check if first segment matches a file in crate root (flat module)
    // e.g., `use crate::config::Foo` where config.rs is in src/ (part of "crate" module)
    if modules.contains_key("crate") {
        let crate_mod = &modules["crate"];
        let first = &module_path[0];
        for file in &crate_mod.files {
            if let Some(stem) = file.file_stem() {
                if stem.to_string_lossy() == *first {
                    return Some("crate".to_string());
                }
            }
        }
    }

    None
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
    fn no_violations_in_empty_project() {
        let modules = HashMap::new();
        let apis = HashMap::new();
        let violations = detect_boundary_violations(&modules, &apis);
        assert!(violations.is_empty());
    }

    #[test]
    fn no_violations_when_using_public_api() {
        let tmp = setup_project(&[
            (
                "main.rs",
                "mod adapters;\nuse crate::adapters::Adapter;\nfn main() {}",
            ),
            (
                "adapters/mod.rs",
                "pub struct Adapter;\npub fn connect() {}",
            ),
        ]);

        let modules = crate::module_detect::detect_modules_from_repo(tmp.path());
        let apis = crate::public_api::extract_public_apis(&modules);
        let violations = detect_boundary_violations(&modules, &apis);

        assert!(violations.is_empty());
    }

    #[test]
    fn detects_non_public_symbol_access() {
        // adapters module only has `connect` as pub, but main.rs tries to import `internal_helper`
        let tmp = setup_project(&[
            (
                "main.rs",
                "mod adapters;\nuse crate::adapters::internal_helper;\nfn main() {}",
            ),
            (
                "adapters/mod.rs",
                "pub fn connect() {}\nfn internal_helper() {}",
            ),
        ]);

        let modules = crate::module_detect::detect_modules_from_repo(tmp.path());
        let apis = crate::public_api::extract_public_apis(&modules);
        let violations = detect_boundary_violations(&modules, &apis);

        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].source_module, "crate");
        assert_eq!(violations[0].target_module, "adapters");
        assert_eq!(violations[0].symbol, "internal_helper");
    }

    #[test]
    fn no_violation_for_intra_module_imports() {
        // Files in the same module importing each other's items shouldn't be violations
        let tmp = setup_project(&[
            (
                "main.rs",
                "mod config;\nuse crate::config::load;\nfn main() {}",
            ),
            ("config.rs", "pub fn load() {}"),
        ]);

        let modules = crate::module_detect::detect_modules_from_repo(tmp.path());
        let apis = crate::public_api::extract_public_apis(&modules);
        let violations = detect_boundary_violations(&modules, &apis);

        // config.rs is in the "crate" module along with main.rs, so this is intra-module
        assert!(violations.is_empty());
    }

    #[test]
    fn group_import_multiple_symbols() {
        let tmp = setup_project(&[
            (
                "main.rs",
                "mod adapters;\nuse crate::adapters::{Adapter, Secret};\nfn main() {}",
            ),
            ("adapters/mod.rs", "pub struct Adapter;\nstruct Secret;"),
        ]);

        let modules = crate::module_detect::detect_modules_from_repo(tmp.path());
        let apis = crate::public_api::extract_public_apis(&modules);
        let violations = detect_boundary_violations(&modules, &apis);

        // Adapter is pub, Secret is not
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].symbol, "Secret");
    }

    #[test]
    fn nested_module_violation() {
        let tmp = setup_project(&[
            (
                "main.rs",
                "mod adapters;\nuse crate::adapters::claude::InternalClient;\nfn main() {}",
            ),
            ("adapters/mod.rs", "pub mod claude;"),
            (
                "adapters/claude.rs",
                "pub struct ClaudeAdapter;\nstruct InternalClient;",
            ),
        ]);

        let modules = crate::module_detect::detect_modules_from_repo(tmp.path());
        let apis = crate::public_api::extract_public_apis(&modules);
        let violations = detect_boundary_violations(&modules, &apis);

        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].symbol, "InternalClient");
        assert_eq!(violations[0].target_module, "adapters");
    }

    #[test]
    fn pub_use_reexport_not_violation() {
        let tmp = setup_project(&[
            (
                "main.rs",
                "mod adapters;\npub use crate::adapters::Adapter;\nfn main() {}",
            ),
            ("adapters/mod.rs", "pub struct Adapter;"),
        ]);

        let modules = crate::module_detect::detect_modules_from_repo(tmp.path());
        let apis = crate::public_api::extract_public_apis(&modules);
        let violations = detect_boundary_violations(&modules, &apis);

        assert!(violations.is_empty());
    }

    #[test]
    fn multiple_violations_across_modules() {
        let tmp = setup_project(&[
            ("main.rs", "mod auth;\nmod db;\nuse crate::auth::secret_key;\nuse crate::db::internal_pool;\nfn main() {}"),
            ("auth/mod.rs", "pub fn login() {}\nfn secret_key() {}"),
            ("db/mod.rs", "pub struct Database;\nfn internal_pool() {}"),
        ]);

        let modules = crate::module_detect::detect_modules_from_repo(tmp.path());
        let apis = crate::public_api::extract_public_apis(&modules);
        let violations = detect_boundary_violations(&modules, &apis);

        assert_eq!(violations.len(), 2);
        let symbols: Vec<&str> = violations.iter().map(|v| v.symbol.as_str()).collect();
        assert!(symbols.contains(&"secret_key"));
        assert!(symbols.contains(&"internal_pool"));
    }

    #[test]
    fn glob_import_not_checked() {
        // Glob imports can't be checked symbol-by-symbol
        let tmp = setup_project(&[
            (
                "main.rs",
                "mod adapters;\nuse crate::adapters::*;\nfn main() {}",
            ),
            ("adapters/mod.rs", "pub fn connect() {}\nfn secret() {}"),
        ]);

        let modules = crate::module_detect::detect_modules_from_repo(tmp.path());
        let apis = crate::public_api::extract_public_apis(&modules);
        let violations = detect_boundary_violations(&modules, &apis);

        // Glob imports are skipped
        assert!(violations.is_empty());
    }

    #[test]
    fn comments_not_parsed_as_imports() {
        let tmp = setup_project(&[
            (
                "main.rs",
                "mod adapters;\n// use crate::adapters::Secret;\nfn main() {}",
            ),
            ("adapters/mod.rs", "pub fn connect() {}\nstruct Secret;"),
        ]);

        let modules = crate::module_detect::detect_modules_from_repo(tmp.path());
        let apis = crate::public_api::extract_public_apis(&modules);
        let violations = detect_boundary_violations(&modules, &apis);

        assert!(violations.is_empty());
    }

    #[test]
    fn violations_sorted_by_source_target_symbol() {
        let tmp = setup_project(&[
            ("main.rs", "mod z_mod;\nmod a_mod;\nuse crate::z_mod::z_private;\nuse crate::a_mod::a_private;\nfn main() {}"),
            ("z_mod/mod.rs", "pub fn public_z() {}"),
            ("a_mod/mod.rs", "pub fn public_a() {}"),
        ]);

        let modules = crate::module_detect::detect_modules_from_repo(tmp.path());
        let apis = crate::public_api::extract_public_apis(&modules);
        let violations = detect_boundary_violations(&modules, &apis);

        assert_eq!(violations.len(), 2);
        // Should be sorted: a_mod violation before z_mod violation
        assert_eq!(violations[0].target_module, "a_mod");
        assert_eq!(violations[1].target_module, "z_mod");
    }

    #[test]
    fn extract_cross_module_imports_basic() {
        let imports = extract_cross_module_imports("use crate::config::HarnessConfig;");
        assert_eq!(imports.len(), 1);
        assert_eq!(imports[0].module_path, vec!["config"]);
        assert_eq!(imports[0].symbols, vec!["HarnessConfig"]);
    }

    #[test]
    fn extract_cross_module_imports_group() {
        let imports = extract_cross_module_imports("use crate::db::{Database, Connection};");
        assert_eq!(imports.len(), 1);
        assert_eq!(imports[0].module_path, vec!["db"]);
        assert_eq!(imports[0].symbols, vec!["Database", "Connection"]);
    }

    #[test]
    fn extract_cross_module_imports_nested() {
        let imports = extract_cross_module_imports("use crate::adapters::claude::ClaudeAdapter;");
        assert_eq!(imports.len(), 1);
        assert_eq!(imports[0].module_path, vec!["adapters", "claude"]);
        assert_eq!(imports[0].symbols, vec!["ClaudeAdapter"]);
    }

    #[test]
    fn extract_cross_module_imports_pub_use() {
        let imports = extract_cross_module_imports("pub use crate::db::Database;");
        assert_eq!(imports.len(), 1);
        assert_eq!(imports[0].module_path, vec!["db"]);
        assert_eq!(imports[0].symbols, vec!["Database"]);
    }

    #[test]
    fn extract_cross_module_imports_skips_std() {
        let imports = extract_cross_module_imports("use std::collections::HashMap;");
        assert!(imports.is_empty());
    }

    #[test]
    fn extract_cross_module_imports_skips_comments() {
        let imports = extract_cross_module_imports("// use crate::config::Foo;");
        assert!(imports.is_empty());
    }

    #[test]
    fn parse_group_import_basic() {
        let symbols = parse_group_import("{Foo, Bar, Baz}");
        assert_eq!(symbols, vec!["Foo", "Bar", "Baz"]);
    }

    #[test]
    fn parse_group_import_single() {
        let symbols = parse_group_import("{Foo}");
        assert_eq!(symbols, vec!["Foo"]);
    }

    #[test]
    fn extract_lowercase_function_import() {
        let imports = extract_cross_module_imports("use crate::config::load_config;");
        assert_eq!(imports.len(), 1);
        assert_eq!(imports[0].module_path, vec!["config"]);
        assert_eq!(imports[0].symbols, vec!["load_config"]);
    }
}
