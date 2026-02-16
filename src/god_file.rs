//! God file detection via cohesion analysis.
//!
//! A "god file" is a large file containing multiple unrelated concerns.
//! Detection works by:
//! 1. Filtering files above a line-count threshold (default: 200 lines).
//! 2. Extracting public symbols from each candidate file.
//! 3. Building a symbol reference graph within the file (which symbols mention each other).
//! 4. Computing connected components (independent symbol clusters).
//! 5. Flagging files with 3+ independent clusters as god file candidates.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

/// A god file candidate with its cohesion analysis.
#[derive(Debug, Clone)]
pub struct GodFileEntry {
    /// Path to the file.
    pub file: PathBuf,
    /// Total line count.
    pub line_count: usize,
    /// Number of independent symbol clusters (connected components).
    pub cluster_count: usize,
}

/// Configuration for god file detection.
#[derive(Debug, Clone)]
pub struct GodFileConfig {
    /// Minimum line count to consider a file (default: 200).
    pub min_lines: usize,
    /// Minimum independent clusters to flag as god file (default: 3).
    pub min_clusters: usize,
}

impl Default for GodFileConfig {
    fn default() -> Self {
        Self {
            min_lines: 200,
            min_clusters: 3,
        }
    }
}

/// Detect god files in a set of source files.
///
/// Takes file paths, reads each, extracts symbols, computes cohesion clusters,
/// and returns entries for files that exceed the thresholds.
pub fn detect_god_files(files: &[PathBuf], config: &GodFileConfig) -> Vec<GodFileEntry> {
    let mut results = Vec::new();

    for file in files {
        let content = match std::fs::read_to_string(file) {
            Ok(c) => c,
            Err(_) => continue,
        };

        let line_count = content.lines().count();
        if line_count < config.min_lines {
            continue;
        }

        let entry = analyze_file(file, &content);
        if entry.cluster_count >= config.min_clusters {
            results.push(entry);
        }
    }

    // Sort by cluster count descending, then by line count descending, then path for determinism
    results.sort_by(|a, b| {
        b.cluster_count
            .cmp(&a.cluster_count)
            .then_with(|| b.line_count.cmp(&a.line_count))
            .then_with(|| a.file.cmp(&b.file))
    });

    results
}

/// Analyze a single file for cohesion.
///
/// Extracts symbols, builds a reference graph between them,
/// and computes connected components.
pub fn analyze_file(file: &Path, content: &str) -> GodFileEntry {
    let line_count = content.lines().count();

    let file_name = file
        .file_name()
        .map(|f| f.to_string_lossy().to_string())
        .unwrap_or_default();

    // Extract all symbols (pub + private) for cohesion analysis
    let all_symbols = extract_all_defined_symbols(content, &file_name);

    if all_symbols.is_empty() {
        return GodFileEntry {
            file: file.to_path_buf(),
            line_count,
            cluster_count: 0,
        };
    }

    // Build a reference graph: symbol A references symbol B if A's "body"
    // mentions B by name. We approximate by scanning lines between definitions.
    let clusters = compute_symbol_clusters(content, &all_symbols);

    GodFileEntry {
        file: file.to_path_buf(),
        line_count,
        cluster_count: clusters.len(),
    }
}

/// A symbol definition with its line range for body scanning.
#[derive(Debug, Clone)]
struct DefinedSymbol {
    name: String,
    start_line: usize,
    end_line: usize,
}

/// Extract all top-level symbol definitions (pub and private) with their line ranges.
fn extract_all_defined_symbols(source: &str, _file_name: &str) -> Vec<DefinedSymbol> {
    let lines: Vec<&str> = source.lines().collect();
    let mut symbols = Vec::new();
    let mut in_block_comment = false;

    let item_prefixes = [
        "pub fn ",
        "pub async fn ",
        "pub struct ",
        "pub enum ",
        "pub trait ",
        "pub const ",
        "pub static ",
        "pub type ",
        "fn ",
        "async fn ",
        "struct ",
        "enum ",
        "trait ",
        "const ",
        "static ",
        "type ",
        "pub(crate) fn ",
        "pub(crate) async fn ",
        "pub(crate) struct ",
        "pub(crate) enum ",
        "pub(crate) trait ",
        "pub(crate) const ",
        "pub(crate) static ",
        "pub(crate) type ",
    ];

    for (i, line) in lines.iter().enumerate() {
        let trimmed = line.trim();

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

        if trimmed.starts_with("//") || trimmed.starts_with('#') {
            continue;
        }

        // Check if this line starts a top-level item definition
        for prefix in &item_prefixes {
            if let Some(after) = trimmed.strip_prefix(prefix) {
                if let Some(name) = extract_ident(after) {
                    // Skip `mod` declarations, `use` statements, and `impl` blocks
                    if name == "mod" || name == "use" || name == "impl" {
                        continue;
                    }
                    let end_line = find_definition_end(&lines, i);
                    symbols.push(DefinedSymbol {
                        name,
                        start_line: i,
                        end_line,
                    });
                }
                break;
            }
        }

        // Also handle `impl` blocks — they contain methods referencing types
        if trimmed.starts_with("impl ") || trimmed.starts_with("impl<") {
            let end_line = find_definition_end(&lines, i);
            // Extract the type name being implemented
            let rest = if let Some(r) = trimmed.strip_prefix("impl ") {
                r
            } else if let Some(r) = trimmed.strip_prefix("impl<") {
                // Skip generic params
                if let Some(close) = r.find('>') {
                    r[close + 1..].trim().trim_start_matches(' ')
                } else {
                    r
                }
            } else {
                continue;
            };

            // impl TypeName ... or impl Trait for TypeName ...
            if let Some(name) = extract_ident(rest) {
                if name != "for" {
                    symbols.push(DefinedSymbol {
                        name: format!("impl_{name}"),
                        start_line: i,
                        end_line,
                    });
                } else {
                    // impl Trait for TypeName
                    let after_for = rest.strip_prefix("for ").unwrap_or(rest);
                    if let Some(type_name) = extract_ident(after_for.trim()) {
                        symbols.push(DefinedSymbol {
                            name: format!("impl_{type_name}"),
                            start_line: i,
                            end_line,
                        });
                    }
                }
            }
        }
    }

    symbols
}

/// Extract the first identifier from text.
fn extract_ident(s: &str) -> Option<String> {
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

/// Find the end line of a brace-delimited definition (or semicolon-terminated).
fn find_definition_end(lines: &[&str], start: usize) -> usize {
    let first_line = lines[start].trim();

    // If it ends with `;` it's a single-line definition
    if first_line.ends_with(';') {
        return start;
    }

    // Track brace depth
    let mut depth: i32 = 0;
    for (i, line) in lines.iter().enumerate().skip(start) {
        for ch in line.chars() {
            match ch {
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        return i;
                    }
                }
                _ => {}
            }
        }
    }

    // Fallback: couldn't find closing brace, use the start line
    start
}

/// Compute connected components of symbols based on cross-references in their bodies.
///
/// Two symbols are connected if one's body text mentions the other's name.
fn compute_symbol_clusters(source: &str, symbols: &[DefinedSymbol]) -> Vec<Vec<String>> {
    let lines: Vec<&str> = source.lines().collect();
    let n = symbols.len();

    if n == 0 {
        return Vec::new();
    }

    // Build adjacency: symbol i references symbol j if j's name appears
    // within i's body lines (and vice versa).
    let mut adj: Vec<HashSet<usize>> = vec![HashSet::new(); n];

    // Common Rust names that cause false-positive cross-references
    let noise_names: HashSet<&str> = [
        "new", "default", "from", "into", "get", "set", "len", "is_empty", "fmt", "clone", "drop",
        "next", "run", "init", "main",
    ]
    .iter()
    .copied()
    .collect();

    for (i, sym_i) in symbols.iter().enumerate() {
        let body = body_text(&lines, sym_i.start_line, sym_i.end_line);

        for (j, sym_j) in symbols.iter().enumerate() {
            if i == j {
                continue;
            }
            let check_name = sym_j.name.strip_prefix("impl_").unwrap_or(&sym_j.name);
            // Skip common Rust method/function names to avoid false positives
            if noise_names.contains(check_name) {
                continue;
            }
            if contains_word(&body, check_name) {
                adj[i].insert(j);
                adj[j].insert(i);
            }
        }
    }

    // Also merge symbols with the same base name (e.g., struct Foo and impl_Foo)
    let mut name_groups: HashMap<String, Vec<usize>> = HashMap::new();
    for (i, sym) in symbols.iter().enumerate() {
        let base = sym
            .name
            .strip_prefix("impl_")
            .unwrap_or(&sym.name)
            .to_string();
        name_groups.entry(base).or_default().push(i);
    }
    for indices in name_groups.values() {
        for pair in indices.windows(2) {
            adj[pair[0]].insert(pair[1]);
            adj[pair[1]].insert(pair[0]);
        }
    }

    // Find connected components via BFS
    let mut visited = vec![false; n];
    let mut components = Vec::new();

    for start in 0..n {
        if visited[start] {
            continue;
        }
        let mut component = Vec::new();
        let mut queue = vec![start];
        visited[start] = true;

        while let Some(node) = queue.pop() {
            component.push(symbols[node].name.clone());
            for &neighbor in &adj[node] {
                if !visited[neighbor] {
                    visited[neighbor] = true;
                    queue.push(neighbor);
                }
            }
        }

        component.sort();
        components.push(component);
    }

    components.sort_by(|a, b| b.len().cmp(&a.len()).then_with(|| a.cmp(b)));
    components
}

/// Extract the body text for a symbol definition (the lines between start and end).
fn body_text(lines: &[&str], start: usize, end: usize) -> String {
    if start >= lines.len() {
        return String::new();
    }
    let end = end.min(lines.len() - 1);
    lines[start..=end].join("\n")
}

/// Check if `text` contains `word` as a whole word (bounded by non-alphanumeric chars).
fn contains_word(text: &str, word: &str) -> bool {
    if word.is_empty() {
        return false;
    }
    let mut start = 0;
    while let Some(pos) = text[start..].find(word) {
        let abs_pos = start + pos;
        let before_ok = abs_pos == 0
            || !text.as_bytes()[abs_pos - 1].is_ascii_alphanumeric()
                && text.as_bytes()[abs_pos - 1] != b'_';
        let after_pos = abs_pos + word.len();
        let after_ok = after_pos >= text.len()
            || !text.as_bytes()[after_pos].is_ascii_alphanumeric()
                && text.as_bytes()[after_pos] != b'_';
        if before_ok && after_ok {
            return true;
        }
        start = abs_pos + 1;
    }
    false
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
    fn small_file_not_flagged() {
        let tmp = setup_project(&[("main.rs", "fn main() {}\npub fn a() {}\npub fn b() {}")]);
        let src = tmp.path().join("src");
        let results = detect_god_files(
            &[src.join("main.rs")],
            &GodFileConfig {
                min_lines: 100,
                min_clusters: 3,
            },
        );
        assert!(results.is_empty());
    }

    #[test]
    fn single_concern_file_not_flagged() {
        // A file that's large but has one cohesive cluster
        let mut source = String::new();
        source.push_str("pub struct Config {\n");
        source.push_str("    pub name: String,\n");
        source.push_str("}\n\n");
        source.push_str("pub fn create_config() -> Config {\n");
        source.push_str("    Config { name: String::new() }\n");
        source.push_str("}\n\n");
        source.push_str("pub fn validate_config(c: &Config) -> bool {\n");
        source.push_str("    !c.name.is_empty()\n");
        source.push_str("}\n\n");
        // Pad to exceed min_lines
        for i in 0..50 {
            source.push_str(&format!("// padding line {i}\n"));
        }

        let tmp = setup_project(&[("config.rs", &source)]);
        let src = tmp.path().join("src");
        let results = detect_god_files(
            &[src.join("config.rs")],
            &GodFileConfig {
                min_lines: 10,
                min_clusters: 3,
            },
        );
        assert!(
            results.is_empty(),
            "Single cohesive file should not be flagged"
        );
    }

    #[test]
    fn multiple_concerns_flagged() {
        // A file with 3+ independent clusters of symbols
        let mut source = String::new();

        // Cluster 1: Config-related
        source.push_str("pub struct Config {\n");
        source.push_str("    pub name: String,\n");
        source.push_str("}\n\n");
        source.push_str("pub fn load_config() -> Config {\n");
        source.push_str("    Config { name: String::new() }\n");
        source.push_str("}\n\n");

        // Cluster 2: Database-related (no reference to Config)
        source.push_str("pub struct Database {\n");
        source.push_str("    pub url: String,\n");
        source.push_str("}\n\n");
        source.push_str("pub fn connect_database() -> Database {\n");
        source.push_str("    Database { url: String::new() }\n");
        source.push_str("}\n\n");

        // Cluster 3: Logger-related (no reference to Config or Database)
        source.push_str("pub struct Logger {\n");
        source.push_str("    pub level: u8,\n");
        source.push_str("}\n\n");
        source.push_str("pub fn create_logger() -> Logger {\n");
        source.push_str("    Logger { level: 0 }\n");
        source.push_str("}\n\n");

        // Pad to exceed min_lines
        for i in 0..50 {
            source.push_str(&format!("// padding line {i}\n"));
        }

        let tmp = setup_project(&[("god.rs", &source)]);
        let src = tmp.path().join("src");
        let results = detect_god_files(
            &[src.join("god.rs")],
            &GodFileConfig {
                min_lines: 10,
                min_clusters: 3,
            },
        );
        assert_eq!(results.len(), 1);
        assert!(results[0].file.ends_with("god.rs"));
        assert!(results[0].cluster_count >= 3);
    }

    #[test]
    fn analyze_file_reports_clusters() {
        let source = "\
pub struct Alpha {
    pub val: i32,
}

pub fn use_alpha(a: &Alpha) -> i32 {
    a.val
}

pub struct Beta {
    pub name: String,
}

pub fn use_beta(b: &Beta) -> &str {
    &b.name
}

pub struct Gamma;

pub fn use_gamma(_g: Gamma) {}
";
        let tmp = setup_project(&[("multi.rs", source)]);
        let src = tmp.path().join("src");
        let entry = analyze_file(&src.join("multi.rs"), source);

        // Alpha + use_alpha form a cluster; Beta + use_beta form another; Gamma + use_gamma form a third
        assert!(
            entry.cluster_count >= 3,
            "Expected 3+ clusters, got {}",
            entry.cluster_count
        );
    }

    #[test]
    fn connected_symbols_same_cluster() {
        let source = "\
pub struct Foo {
    pub x: i32,
}

pub fn make_foo() -> Foo {
    Foo { x: 0 }
}

pub fn process_foo(f: &Foo) -> i32 {
    f.x + 1
}
";
        let tmp = setup_project(&[("connected.rs", source)]);
        let src = tmp.path().join("src");
        let entry = analyze_file(&src.join("connected.rs"), source);

        // All three symbols reference Foo → one cluster
        assert_eq!(
            entry.cluster_count, 1,
            "All connected symbols should form one cluster"
        );
    }

    #[test]
    fn impl_block_merges_with_struct() {
        let source = "\
pub struct Widget {
    pub size: u32,
}

impl Widget {
    pub fn new() -> Self {
        Widget { size: 0 }
    }
}

pub struct Gadget {
    pub name: String,
}

impl Gadget {
    pub fn default_gadget() -> Self {
        Gadget { name: String::new() }
    }
}
";
        let tmp = setup_project(&[("impls.rs", source)]);
        let src = tmp.path().join("src");
        let entry = analyze_file(&src.join("impls.rs"), source);

        // Widget + impl_Widget → one cluster; Gadget + impl_Gadget → another
        assert_eq!(
            entry.cluster_count, 2,
            "Struct and impl should merge: got {}",
            entry.cluster_count
        );
    }

    #[test]
    fn empty_file_zero_clusters() {
        let source = "// just a comment\n";
        let tmp = setup_project(&[("empty.rs", source)]);
        let src = tmp.path().join("src");
        let entry = analyze_file(&src.join("empty.rs"), source);
        assert_eq!(entry.cluster_count, 0);
    }

    #[test]
    fn contains_word_basic() {
        assert!(contains_word("foo bar baz", "bar"));
        assert!(!contains_word("foobar baz", "bar"));
        assert!(!contains_word("foo barbaz", "bar"));
        assert!(contains_word("bar", "bar"));
        assert!(contains_word("(bar)", "bar"));
        assert!(!contains_word("bar_ext", "bar"));
    }

    #[test]
    fn detect_god_files_sorted_by_severity() {
        let make_god_source = |clusters: usize| {
            let mut s = String::new();
            for c in 0..clusters {
                s.push_str(&format!(
                    "pub struct Thing{c} {{\n    pub val: i32,\n}}\n\n"
                ));
                s.push_str(&format!(
                    "pub fn use_thing{c}(t: &Thing{c}) -> i32 {{\n    t.val\n}}\n\n"
                ));
            }
            for i in 0..50 {
                s.push_str(&format!("// padding {i}\n"));
            }
            s
        };

        let source3 = make_god_source(3);
        let source5 = make_god_source(5);

        let tmp = setup_project(&[("mild.rs", &source3), ("severe.rs", &source5)]);
        let src = tmp.path().join("src");
        let results = detect_god_files(
            &[src.join("mild.rs"), src.join("severe.rs")],
            &GodFileConfig {
                min_lines: 10,
                min_clusters: 3,
            },
        );

        assert_eq!(results.len(), 2);
        // More severe (more clusters) should come first
        assert!(results[0].cluster_count >= results[1].cluster_count);
        assert!(results[0].file.ends_with("severe.rs"));
    }

    #[test]
    fn default_config_values() {
        let cfg = GodFileConfig::default();
        assert_eq!(cfg.min_lines, 200);
        assert_eq!(cfg.min_clusters, 3);
    }

    #[test]
    fn find_definition_end_single_line() {
        let lines = vec!["pub const X: u32 = 5;"];
        assert_eq!(find_definition_end(&lines, 0), 0);
    }

    #[test]
    fn find_definition_end_braced_block() {
        let lines = vec!["pub fn foo() {", "    let x = 1;", "}"];
        assert_eq!(find_definition_end(&lines, 0), 2);
    }

    #[test]
    fn extract_all_symbols_includes_private() {
        let source = "fn private_fn() {}\npub fn public_fn() {}";
        let symbols = extract_all_defined_symbols(source, "test.rs");
        let names: Vec<&str> = symbols.iter().map(|s| s.name.as_str()).collect();
        assert!(names.contains(&"private_fn"));
        assert!(names.contains(&"public_fn"));
    }
}
