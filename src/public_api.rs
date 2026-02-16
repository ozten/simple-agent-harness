//! Public API surface extractor for Rust modules.
//!
//! Given detected modules (from `module_detect`), parses each `.rs` file to extract
//! public symbols: `pub fn`, `pub struct`, `pub enum`, `pub trait`, `pub const`,
//! `pub static`, `pub type`. Returns a structured representation per module.
//!
//! Used to compute API surface width (a structural smell — modules exporting many
//! symbols create large blast radii) and to detect boundary violations.

use std::collections::HashMap;
#[cfg(test)]
use std::path::Path;

use crate::module_detect::Module;

/// A public symbol extracted from a Rust source file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Symbol {
    /// The kind of symbol: "fn", "struct", "enum", "trait", "const", "static", "type".
    pub kind: String,
    /// The symbol name (e.g., "Config", "open_or_create").
    pub name: String,
    /// The full signature line (trimmed, up to the opening brace or semicolon).
    pub signature: String,
    /// The file this symbol was found in (relative to module root).
    pub file: String,
}

/// Public API surface for a single module.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModuleApi {
    /// Module name (matches `Module::name`, e.g., "adapters", "crate").
    pub module_name: String,
    /// All public symbols exported by this module.
    pub public_symbols: Vec<Symbol>,
}

/// Extract public API surfaces for all detected modules.
///
/// For each module, reads all its `.rs` files and extracts `pub` declarations.
/// Returns a map from module name to `ModuleApi`.
pub fn extract_public_apis(modules: &HashMap<String, Module>) -> HashMap<String, ModuleApi> {
    let mut result = HashMap::new();

    for (name, module) in modules {
        let mut symbols = Vec::new();

        for file_path in &module.files {
            let content = match std::fs::read_to_string(file_path) {
                Ok(c) => c,
                Err(_) => continue,
            };

            let file_name = file_path
                .file_name()
                .map(|f| f.to_string_lossy().to_string())
                .unwrap_or_default();

            let mut file_symbols = extract_symbols_from_source(&content, &file_name);
            symbols.append(&mut file_symbols);
        }

        symbols.sort_by(|a, b| a.kind.cmp(&b.kind).then(a.name.cmp(&b.name)));

        result.insert(
            name.clone(),
            ModuleApi {
                module_name: name.clone(),
                public_symbols: symbols,
            },
        );
    }

    result
}

/// Extract public API surface for a single module.
#[cfg(test)]
pub fn extract_module_api(module: &Module) -> ModuleApi {
    let mut symbols = Vec::new();

    for file_path in &module.files {
        let content = match std::fs::read_to_string(file_path) {
            Ok(c) => c,
            Err(_) => continue,
        };

        let file_name = file_path
            .file_name()
            .map(|f| f.to_string_lossy().to_string())
            .unwrap_or_default();

        let mut file_symbols = extract_symbols_from_source(&content, &file_name);
        symbols.append(&mut file_symbols);
    }

    symbols.sort_by(|a, b| a.kind.cmp(&b.kind).then(a.name.cmp(&b.name)));

    ModuleApi {
        module_name: module.name.clone(),
        public_symbols: symbols,
    }
}

/// Format a `ModuleApi` into boundary signature strings suitable for
/// `DerivedFields::boundary_signatures`.
///
/// Returns strings like `"pub fn module::file::name(args) -> Ret"`.
#[cfg(test)]
pub fn format_boundary_signatures(api: &ModuleApi) -> Vec<String> {
    api.public_symbols
        .iter()
        .map(|s| s.signature.clone())
        .collect()
}

/// Extract public API surfaces from a set of source files (by path).
///
/// Convenience for extracting without needing full module detection.
#[cfg(test)]
pub fn extract_from_files(files: &[&Path]) -> Vec<Symbol> {
    let mut symbols = Vec::new();
    for file_path in files {
        let content = match std::fs::read_to_string(file_path) {
            Ok(c) => c,
            Err(_) => continue,
        };
        let file_name = file_path
            .file_name()
            .map(|f| f.to_string_lossy().to_string())
            .unwrap_or_default();
        let mut file_symbols = extract_symbols_from_source(&content, &file_name);
        symbols.append(&mut file_symbols);
    }
    symbols.sort_by(|a, b| a.kind.cmp(&b.kind).then(a.name.cmp(&b.name)));
    symbols
}

/// Parse Rust source text and extract public symbol declarations.
///
/// Recognizes: `pub fn`, `pub struct`, `pub enum`, `pub trait`,
/// `pub const`, `pub static`, `pub type`, and `pub async fn`.
/// Also handles `pub(crate)` visibility — these are *not* included since
/// they are not part of the external API surface.
fn extract_symbols_from_source(source: &str, file_name: &str) -> Vec<Symbol> {
    let mut symbols = Vec::new();
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

        // Must start with "pub " but not "pub(crate)" or "pub(super)" or "pub(in ...)"
        if !trimmed.starts_with("pub ") && !trimmed.starts_with("pub(") {
            continue;
        }

        // Filter out restricted visibility
        if trimmed.starts_with("pub(crate)")
            || trimmed.starts_with("pub(super)")
            || trimmed.starts_with("pub(in ")
        {
            continue;
        }

        // At this point we have a "pub " item (unrestricted visibility)
        // Strip "pub " prefix to get the rest
        let rest = if let Some(stripped) = trimmed.strip_prefix("pub ") {
            stripped
        } else {
            continue;
        };

        if let Some(sym) = parse_pub_item(rest, trimmed, file_name) {
            symbols.push(sym);
        }
    }

    symbols
}

/// Parse a public item declaration after the "pub " prefix.
fn parse_pub_item(rest: &str, full_line: &str, file_name: &str) -> Option<Symbol> {
    // pub async fn
    if let Some(after) = rest.strip_prefix("async fn ") {
        // skip "async fn "
        let name = extract_identifier(after)?;
        return Some(Symbol {
            kind: "fn".to_string(),
            name,
            signature: clean_signature(full_line),
            file: file_name.to_string(),
        });
    }

    let item_kinds = [
        ("fn ", "fn"),
        ("struct ", "struct"),
        ("enum ", "enum"),
        ("trait ", "trait"),
        ("const ", "const"),
        ("static ", "static"),
        ("type ", "type"),
    ];

    for (prefix, kind) in &item_kinds {
        if let Some(after) = rest.strip_prefix(prefix) {
            let name = extract_identifier(after)?;
            return Some(Symbol {
                kind: kind.to_string(),
                name,
                signature: clean_signature(full_line),
                file: file_name.to_string(),
            });
        }
    }

    None
}

/// Extract the first identifier from a string (alphanumeric + underscore).
fn extract_identifier(s: &str) -> Option<String> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    let end = s
        .find(|c: char| !c.is_alphanumeric() && c != '_')
        .unwrap_or(s.len());

    if end == 0 {
        return None;
    }

    Some(s[..end].to_string())
}

/// Clean up a signature line: trim, remove trailing `{`, and normalize whitespace.
fn clean_signature(line: &str) -> String {
    let mut sig = line.trim().to_string();

    // Remove trailing opening brace
    if sig.ends_with('{') {
        sig = sig[..sig.len() - 1].trim_end().to_string();
    }

    // Collapse multiple spaces
    let mut result = String::with_capacity(sig.len());
    let mut prev_space = false;
    for c in sig.chars() {
        if c == ' ' || c == '\t' {
            if !prev_space {
                result.push(' ');
                prev_space = true;
            }
        } else {
            result.push(c);
            prev_space = false;
        }
    }

    result
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
    fn extract_pub_fn() {
        let syms = extract_symbols_from_source(
            "pub fn hello(name: &str) -> String {\n    format!(\"hi {name}\")\n}",
            "lib.rs",
        );
        assert_eq!(syms.len(), 1);
        assert_eq!(syms[0].kind, "fn");
        assert_eq!(syms[0].name, "hello");
        assert_eq!(syms[0].signature, "pub fn hello(name: &str) -> String");
        assert_eq!(syms[0].file, "lib.rs");
    }

    #[test]
    fn extract_pub_struct() {
        let syms = extract_symbols_from_source(
            "pub struct Config {\n    pub name: String,\n}",
            "config.rs",
        );
        assert_eq!(syms.len(), 1);
        assert_eq!(syms[0].kind, "struct");
        assert_eq!(syms[0].name, "Config");
        assert_eq!(syms[0].signature, "pub struct Config");
    }

    #[test]
    fn extract_pub_enum() {
        let syms =
            extract_symbols_from_source("pub enum Color {\n    Red,\n    Blue,\n}", "types.rs");
        assert_eq!(syms.len(), 1);
        assert_eq!(syms[0].kind, "enum");
        assert_eq!(syms[0].name, "Color");
    }

    #[test]
    fn extract_pub_trait() {
        let syms =
            extract_symbols_from_source("pub trait Adapter {\n    fn run(&self);\n}", "adapter.rs");
        assert_eq!(syms.len(), 1);
        assert_eq!(syms[0].kind, "trait");
        assert_eq!(syms[0].name, "Adapter");
    }

    #[test]
    fn extract_pub_const_and_static() {
        let syms = extract_symbols_from_source(
            "pub const MAX_RETRIES: u32 = 3;\npub static VERSION: &str = \"1.0\";",
            "lib.rs",
        );
        assert_eq!(syms.len(), 2);
        assert!(syms
            .iter()
            .any(|s| s.kind == "const" && s.name == "MAX_RETRIES"));
        assert!(syms
            .iter()
            .any(|s| s.kind == "static" && s.name == "VERSION"));
    }

    #[test]
    fn extract_pub_type_alias() {
        let syms = extract_symbols_from_source(
            "pub type Result<T> = std::result::Result<T, Error>;",
            "lib.rs",
        );
        assert_eq!(syms.len(), 1);
        assert_eq!(syms[0].kind, "type");
        assert_eq!(syms[0].name, "Result");
    }

    #[test]
    fn extract_pub_async_fn() {
        let syms =
            extract_symbols_from_source("pub async fn fetch(url: &str) -> Response {", "client.rs");
        assert_eq!(syms.len(), 1);
        assert_eq!(syms[0].kind, "fn");
        assert_eq!(syms[0].name, "fetch");
        assert_eq!(
            syms[0].signature,
            "pub async fn fetch(url: &str) -> Response"
        );
    }

    #[test]
    fn skip_pub_crate() {
        let syms = extract_symbols_from_source(
            "pub(crate) fn internal() {}\npub fn external() {}",
            "lib.rs",
        );
        assert_eq!(syms.len(), 1);
        assert_eq!(syms[0].name, "external");
    }

    #[test]
    fn skip_pub_super() {
        let syms = extract_symbols_from_source(
            "pub(super) fn parent_only() {}\npub fn visible() {}",
            "child.rs",
        );
        assert_eq!(syms.len(), 1);
        assert_eq!(syms[0].name, "visible");
    }

    #[test]
    fn skip_pub_in_path() {
        let syms = extract_symbols_from_source(
            "pub(in crate::parent) fn restricted() {}\npub fn open() {}",
            "child.rs",
        );
        assert_eq!(syms.len(), 1);
        assert_eq!(syms[0].name, "open");
    }

    #[test]
    fn skip_private_items() {
        let syms = extract_symbols_from_source(
            "fn private_fn() {}\nstruct PrivateStruct;\nenum PrivateEnum {}",
            "lib.rs",
        );
        assert!(syms.is_empty());
    }

    #[test]
    fn skip_comments() {
        let syms = extract_symbols_from_source(
            "// pub fn commented_out() {}\n/// doc comment\npub fn real() {}",
            "lib.rs",
        );
        assert_eq!(syms.len(), 1);
        assert_eq!(syms[0].name, "real");
    }

    #[test]
    fn skip_block_comments() {
        let syms = extract_symbols_from_source(
            "/* pub fn in_block_comment() {} */\npub fn after() {}",
            "lib.rs",
        );
        assert_eq!(syms.len(), 1);
        assert_eq!(syms[0].name, "after");
    }

    #[test]
    fn skip_attributes() {
        let syms = extract_symbols_from_source("#[derive(Debug)]\npub struct Tagged;", "lib.rs");
        assert_eq!(syms.len(), 1);
        assert_eq!(syms[0].name, "Tagged");
    }

    #[test]
    fn multiple_symbols_one_file() {
        let source = "\
pub fn alpha() {}
pub struct Beta;
pub enum Gamma {}
pub trait Delta {}
pub const EPSILON: u32 = 1;
fn private() {}
";
        let syms = extract_symbols_from_source(source, "multi.rs");
        assert_eq!(syms.len(), 5);
        let names: Vec<&str> = syms.iter().map(|s| s.name.as_str()).collect();
        assert!(names.contains(&"alpha"));
        assert!(names.contains(&"Beta"));
        assert!(names.contains(&"Gamma"));
        assert!(names.contains(&"Delta"));
        assert!(names.contains(&"EPSILON"));
    }

    #[test]
    fn extract_module_api_from_files() {
        let tmp = setup_project(&[
            ("main.rs", "fn main() {}"),
            (
                "config.rs",
                "pub struct Config;\npub fn load() -> Config { Config }",
            ),
        ]);

        let modules = crate::module_detect::detect_modules_from_repo(tmp.path());
        let crate_mod = modules.get("crate").unwrap();
        let api = extract_module_api(crate_mod);

        assert_eq!(api.module_name, "crate");
        // Should find Config struct and load fn from config.rs
        assert!(api
            .public_symbols
            .iter()
            .any(|s| s.name == "Config" && s.kind == "struct"));
        assert!(api
            .public_symbols
            .iter()
            .any(|s| s.name == "load" && s.kind == "fn"));
    }

    #[test]
    fn extract_public_apis_multiple_modules() {
        let tmp = setup_project(&[
            ("main.rs", "mod adapters;\nfn main() {}"),
            ("adapters/mod.rs", "pub mod claude;"),
            (
                "adapters/claude.rs",
                "pub struct ClaudeAdapter;\npub fn connect() {}",
            ),
        ]);

        let modules = crate::module_detect::detect_modules_from_repo(tmp.path());
        let apis = extract_public_apis(&modules);

        assert!(apis.contains_key("crate"));
        assert!(apis.contains_key("adapters"));

        let adapters_api = &apis["adapters"];
        assert!(adapters_api
            .public_symbols
            .iter()
            .any(|s| s.name == "ClaudeAdapter"));
        assert!(adapters_api
            .public_symbols
            .iter()
            .any(|s| s.name == "connect"));
    }

    #[test]
    fn empty_module_no_symbols() {
        let tmp = setup_project(&[("main.rs", "fn main() {}")]);
        let modules = crate::module_detect::detect_modules_from_repo(tmp.path());
        let apis = extract_public_apis(&modules);

        let crate_api = &apis["crate"];
        // main.rs has no pub items
        assert!(crate_api.public_symbols.is_empty());
    }

    #[test]
    fn format_boundary_signatures_output() {
        let api = ModuleApi {
            module_name: "auth".to_string(),
            public_symbols: vec![
                Symbol {
                    kind: "fn".to_string(),
                    name: "login".to_string(),
                    signature: "pub fn login(req: Request) -> Response".to_string(),
                    file: "auth.rs".to_string(),
                },
                Symbol {
                    kind: "struct".to_string(),
                    name: "Session".to_string(),
                    signature: "pub struct Session".to_string(),
                    file: "auth.rs".to_string(),
                },
            ],
        };

        let sigs = format_boundary_signatures(&api);
        assert_eq!(sigs.len(), 2);
        assert_eq!(sigs[0], "pub fn login(req: Request) -> Response");
        assert_eq!(sigs[1], "pub struct Session");
    }

    #[test]
    fn extract_identifier_basic() {
        assert_eq!(extract_identifier("hello("), Some("hello".to_string()));
        assert_eq!(extract_identifier("Config {"), Some("Config".to_string()));
        assert_eq!(
            extract_identifier("MAX_VAL: u32"),
            Some("MAX_VAL".to_string())
        );
        assert_eq!(extract_identifier(""), None);
        assert_eq!(extract_identifier("(bad"), None);
    }

    #[test]
    fn clean_signature_removes_brace() {
        assert_eq!(clean_signature("pub fn foo() {"), "pub fn foo()");
        assert_eq!(clean_signature("pub struct Bar {"), "pub struct Bar");
    }

    #[test]
    fn clean_signature_preserves_semicolon() {
        assert_eq!(
            clean_signature("pub const X: u32 = 5;"),
            "pub const X: u32 = 5;"
        );
    }

    #[test]
    fn pub_struct_with_semicolon() {
        let syms = extract_symbols_from_source("pub struct Unit;", "lib.rs");
        assert_eq!(syms.len(), 1);
        assert_eq!(syms[0].kind, "struct");
        assert_eq!(syms[0].name, "Unit");
    }

    #[test]
    fn pub_struct_tuple() {
        let syms = extract_symbols_from_source("pub struct Wrapper(pub i32);", "lib.rs");
        assert_eq!(syms.len(), 1);
        assert_eq!(syms[0].name, "Wrapper");
    }

    #[test]
    fn extract_from_files_convenience() {
        let tmp = setup_project(&[("a.rs", "pub fn alpha() {}"), ("b.rs", "pub fn beta() {}")]);
        let src = tmp.path().join("src");
        let a = src.join("a.rs");
        let b = src.join("b.rs");
        let syms = extract_from_files(&[a.as_path(), b.as_path()]);
        assert_eq!(syms.len(), 2);
        assert!(syms.iter().any(|s| s.name == "alpha"));
        assert!(syms.iter().any(|s| s.name == "beta"));
    }

    #[test]
    fn multiline_block_comment_skips_pub() {
        let source = "/*\npub fn hidden() {}\n*/\npub fn visible() {}";
        let syms = extract_symbols_from_source(source, "lib.rs");
        assert_eq!(syms.len(), 1);
        assert_eq!(syms[0].name, "visible");
    }

    #[test]
    fn symbols_sorted_by_kind_then_name() {
        let source = "\
pub fn zebra() {}
pub struct Alpha;
pub fn apple() {}
pub enum Banana {}
";
        let tmp = setup_project(&[("lib.rs", source)]);
        let modules = crate::module_detect::detect_modules_from_repo(tmp.path());
        let apis = extract_public_apis(&modules);
        let api = &apis["crate"];

        let kinds: Vec<&str> = api.public_symbols.iter().map(|s| s.kind.as_str()).collect();
        let names: Vec<&str> = api.public_symbols.iter().map(|s| s.name.as_str()).collect();

        // Should be sorted: enum Banana, fn apple, fn zebra, struct Alpha
        assert_eq!(kinds, vec!["enum", "fn", "fn", "struct"]);
        assert_eq!(names, vec!["Banana", "apple", "zebra", "Alpha"]);
    }
}
