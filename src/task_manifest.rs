use serde::Deserialize;
use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::path::Path;

/// Errors that can occur when parsing or applying a task manifest.
#[derive(Debug)]
pub enum ManifestError {
    Io(std::io::Error),
    Parse(toml::de::Error),
    CargoTomlParse(String),
}

impl fmt::Display for ManifestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ManifestError::Io(e) => write!(f, "IO error: {e}"),
            ManifestError::Parse(e) => write!(f, "TOML parse error: {e}"),
            ManifestError::CargoTomlParse(e) => write!(f, "Cargo.toml parse error: {e}"),
        }
    }
}

impl From<std::io::Error> for ManifestError {
    fn from(e: std::io::Error) -> Self {
        ManifestError::Io(e)
    }
}

impl From<toml::de::Error> for ManifestError {
    fn from(e: toml::de::Error) -> Self {
        ManifestError::Parse(e)
    }
}

/// A parsed task_manifest.toml file.
#[derive(Debug, Deserialize, Default, PartialEq)]
pub struct TaskManifest {
    #[serde(default)]
    pub lib_rs_additions: Vec<LibRsAddition>,
    #[serde(default)]
    pub cargo_toml_additions: Vec<CargoTomlAddition>,
    #[serde(default)]
    pub mod_rs_additions: Vec<ModRsAddition>,
    #[serde(default)]
    pub ts_index_additions: Vec<TsIndexAddition>,
}

/// An entry to be appended to lib.rs (or main.rs).
#[derive(Debug, Deserialize, PartialEq)]
pub struct LibRsAddition {
    pub kind: LibRsKind,
    pub content: String,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum LibRsKind {
    ModDeclaration,
    Reexport,
}

/// An entry for Cargo.toml dependency additions.
#[derive(Debug, Deserialize, PartialEq)]
pub struct CargoTomlAddition {
    pub kind: CargoTomlKind,
    pub content: String,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum CargoTomlKind {
    Dependency,
    DevDependency,
}

/// An entry for mod.rs submodule registration.
#[derive(Debug, Deserialize, PartialEq)]
pub struct ModRsAddition {
    /// Relative path to the mod.rs file (e.g. "src/adapters/mod.rs")
    pub target: String,
    pub content: String,
}

/// An entry for TypeScript barrel export additions.
#[derive(Debug, Deserialize, PartialEq)]
pub struct TsIndexAddition {
    /// Relative path to the index.ts file (e.g. "src/index.ts")
    pub target: String,
    pub content: String,
}

/// Parse a task_manifest.toml file from the given path.
pub fn parse(path: &Path) -> Result<TaskManifest, ManifestError> {
    let content = fs::read_to_string(path)?;
    parse_str(&content)
}

/// Parse a task manifest from a TOML string.
pub fn parse_str(content: &str) -> Result<TaskManifest, ManifestError> {
    let manifest: TaskManifest = toml::from_str(content)?;
    Ok(manifest)
}

/// Apply all manifest entries to the worktree rooted at `root`.
/// Returns the number of entries applied.
pub fn apply(manifest: &TaskManifest, root: &Path) -> Result<usize, ManifestError> {
    let mut count = 0;

    // Apply lib.rs additions
    if !manifest.lib_rs_additions.is_empty() {
        let lib_rs = root.join("src/lib.rs");
        let main_rs = root.join("src/main.rs");
        let target = if lib_rs.exists() { lib_rs } else { main_rs };
        count += apply_lib_rs_additions(&target, &manifest.lib_rs_additions)?;
    }

    // Apply mod.rs additions
    for addition in &manifest.mod_rs_additions {
        let target = root.join(&addition.target);
        count += apply_append_if_missing(&target, &addition.content)?;
    }

    // Apply Cargo.toml additions
    if !manifest.cargo_toml_additions.is_empty() {
        let cargo_toml = root.join("Cargo.toml");
        count += apply_cargo_toml_additions(&cargo_toml, &manifest.cargo_toml_additions)?;
    }

    // Apply TypeScript index.ts additions
    for addition in &manifest.ts_index_additions {
        let target = root.join(&addition.target);
        count += apply_append_if_missing(&target, &addition.content)?;
    }

    Ok(count)
}

/// Apply lib.rs additions, grouping by kind and appending in order.
fn apply_lib_rs_additions(
    path: &Path,
    additions: &[LibRsAddition],
) -> Result<usize, ManifestError> {
    let mut content = if path.exists() {
        fs::read_to_string(path)?
    } else {
        String::new()
    };

    let mut count = 0;
    for addition in additions {
        let line = addition.content.trim();
        if !content.contains(line) {
            if !content.ends_with('\n') && !content.is_empty() {
                content.push('\n');
            }
            content.push_str(line);
            content.push('\n');
            count += 1;
        }
    }

    if count > 0 {
        fs::write(path, &content)?;
    }
    Ok(count)
}

/// Append a line to a file if it doesn't already exist. Returns 1 if appended, 0 if already present.
fn apply_append_if_missing(path: &Path, line: &str) -> Result<usize, ManifestError> {
    let mut content = if path.exists() {
        fs::read_to_string(path)?
    } else {
        String::new()
    };

    let trimmed = line.trim();
    if content.contains(trimmed) {
        return Ok(0);
    }

    if !content.ends_with('\n') && !content.is_empty() {
        content.push('\n');
    }
    content.push_str(trimmed);
    content.push('\n');
    fs::write(path, &content)?;
    Ok(1)
}

/// Apply Cargo.toml dependency additions by merging into [dependencies] or [dev-dependencies].
fn apply_cargo_toml_additions(
    path: &Path,
    additions: &[CargoTomlAddition],
) -> Result<usize, ManifestError> {
    let content = if path.exists() {
        fs::read_to_string(path)?
    } else {
        return Err(ManifestError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Cargo.toml not found",
        )));
    };

    // Parse the existing Cargo.toml
    let mut doc: toml::Value = toml::from_str(&content)
        .map_err(|e| ManifestError::CargoTomlParse(format!("failed to parse Cargo.toml: {e}")))?;

    let mut count = 0;

    for addition in additions {
        let section_key = match addition.kind {
            CargoTomlKind::Dependency => "dependencies",
            CargoTomlKind::DevDependency => "dev-dependencies",
        };

        // Parse the addition content as "name = value"
        let dep_entry: BTreeMap<String, toml::Value> =
            toml::from_str(&addition.content).map_err(|e| {
                ManifestError::CargoTomlParse(format!(
                    "failed to parse dependency '{}': {e}",
                    addition.content
                ))
            })?;

        // Ensure the section exists
        let table = doc
            .as_table_mut()
            .ok_or_else(|| ManifestError::CargoTomlParse("Cargo.toml is not a table".into()))?;

        if !table.contains_key(section_key) {
            table.insert(
                section_key.to_string(),
                toml::Value::Table(toml::map::Map::new()),
            );
        }

        let section = table
            .get_mut(section_key)
            .and_then(|v| v.as_table_mut())
            .ok_or_else(|| {
                ManifestError::CargoTomlParse(format!("[{section_key}] is not a table"))
            })?;

        for (name, value) in &dep_entry {
            if !section.contains_key(name) {
                section.insert(name.clone(), value.clone());
                count += 1;
            }
        }
    }

    if count > 0 {
        let serialized = toml::to_string_pretty(&doc)
            .map_err(|e| ManifestError::CargoTomlParse(format!("failed to serialize: {e}")))?;
        fs::write(path, serialized)?;
    }

    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_parse_empty() {
        let manifest = parse_str("").unwrap();
        assert!(manifest.lib_rs_additions.is_empty());
        assert!(manifest.cargo_toml_additions.is_empty());
        assert!(manifest.mod_rs_additions.is_empty());
        assert!(manifest.ts_index_additions.is_empty());
    }

    #[test]
    fn test_parse_full_manifest() {
        let toml = r#"
[[lib_rs_additions]]
kind = "mod_declaration"
content = "pub mod analytics;"

[[lib_rs_additions]]
kind = "reexport"
content = "pub use analytics::AnalyticsEngine;"

[[cargo_toml_additions]]
kind = "dependency"
content = 'serde = { version = "1.0", features = ["derive"] }'

[[cargo_toml_additions]]
kind = "dev_dependency"
content = 'tempfile = "3"'

[[mod_rs_additions]]
target = "src/adapters/mod.rs"
content = "pub mod newadapter;"

[[ts_index_additions]]
target = "src/index.ts"
content = "export { Foo } from './foo';"
"#;
        let manifest = parse_str(toml).unwrap();
        assert_eq!(manifest.lib_rs_additions.len(), 2);
        assert_eq!(manifest.lib_rs_additions[0].kind, LibRsKind::ModDeclaration);
        assert_eq!(manifest.lib_rs_additions[0].content, "pub mod analytics;");
        assert_eq!(manifest.lib_rs_additions[1].kind, LibRsKind::Reexport);
        assert_eq!(manifest.cargo_toml_additions.len(), 2);
        assert_eq!(
            manifest.cargo_toml_additions[0].kind,
            CargoTomlKind::Dependency
        );
        assert_eq!(
            manifest.cargo_toml_additions[1].kind,
            CargoTomlKind::DevDependency
        );
        assert_eq!(manifest.mod_rs_additions.len(), 1);
        assert_eq!(manifest.mod_rs_additions[0].target, "src/adapters/mod.rs");
        assert_eq!(manifest.ts_index_additions.len(), 1);
    }

    #[test]
    fn test_parse_invalid_kind() {
        let toml = r#"
[[lib_rs_additions]]
kind = "invalid_kind"
content = "something"
"#;
        assert!(parse_str(toml).is_err());
    }

    #[test]
    fn test_apply_lib_rs_mod_declarations() {
        let dir = TempDir::new().unwrap();
        let src = dir.path().join("src");
        fs::create_dir_all(&src).unwrap();
        fs::write(src.join("lib.rs"), "mod existing;\n").unwrap();

        let manifest = TaskManifest {
            lib_rs_additions: vec![
                LibRsAddition {
                    kind: LibRsKind::ModDeclaration,
                    content: "pub mod analytics;".into(),
                },
                LibRsAddition {
                    kind: LibRsKind::Reexport,
                    content: "pub use analytics::AnalyticsEngine;".into(),
                },
            ],
            ..Default::default()
        };

        let count = apply(&manifest, dir.path()).unwrap();
        assert_eq!(count, 2);

        let content = fs::read_to_string(src.join("lib.rs")).unwrap();
        assert!(content.contains("mod existing;"));
        assert!(content.contains("pub mod analytics;"));
        assert!(content.contains("pub use analytics::AnalyticsEngine;"));
    }

    #[test]
    fn test_apply_lib_rs_idempotent() {
        let dir = TempDir::new().unwrap();
        let src = dir.path().join("src");
        fs::create_dir_all(&src).unwrap();
        fs::write(src.join("lib.rs"), "pub mod analytics;\n").unwrap();

        let manifest = TaskManifest {
            lib_rs_additions: vec![LibRsAddition {
                kind: LibRsKind::ModDeclaration,
                content: "pub mod analytics;".into(),
            }],
            ..Default::default()
        };

        let count = apply(&manifest, dir.path()).unwrap();
        assert_eq!(count, 0); // Already present, no change
    }

    #[test]
    fn test_apply_falls_back_to_main_rs() {
        let dir = TempDir::new().unwrap();
        let src = dir.path().join("src");
        fs::create_dir_all(&src).unwrap();
        // No lib.rs, only main.rs
        fs::write(src.join("main.rs"), "fn main() {}\n").unwrap();

        let manifest = TaskManifest {
            lib_rs_additions: vec![LibRsAddition {
                kind: LibRsKind::ModDeclaration,
                content: "mod foo;".into(),
            }],
            ..Default::default()
        };

        let count = apply(&manifest, dir.path()).unwrap();
        assert_eq!(count, 1);

        let content = fs::read_to_string(src.join("main.rs")).unwrap();
        assert!(content.contains("mod foo;"));
    }

    #[test]
    fn test_apply_cargo_toml_new_dep() {
        let dir = TempDir::new().unwrap();
        fs::write(
            dir.path().join("Cargo.toml"),
            r#"[package]
name = "test"
version = "0.1.0"

[dependencies]
serde = "1"
"#,
        )
        .unwrap();

        let manifest = TaskManifest {
            cargo_toml_additions: vec![CargoTomlAddition {
                kind: CargoTomlKind::Dependency,
                content: r#"tokio = { version = "1", features = ["full"] }"#.into(),
            }],
            ..Default::default()
        };

        let count = apply(&manifest, dir.path()).unwrap();
        assert_eq!(count, 1);

        let content = fs::read_to_string(dir.path().join("Cargo.toml")).unwrap();
        assert!(content.contains("tokio"));
        // serde still present
        assert!(content.contains("serde"));
    }

    #[test]
    fn test_apply_cargo_toml_existing_dep_skipped() {
        let dir = TempDir::new().unwrap();
        fs::write(
            dir.path().join("Cargo.toml"),
            r#"[package]
name = "test"
version = "0.1.0"

[dependencies]
serde = "1"
"#,
        )
        .unwrap();

        let manifest = TaskManifest {
            cargo_toml_additions: vec![CargoTomlAddition {
                kind: CargoTomlKind::Dependency,
                content: r#"serde = "2""#.into(),
            }],
            ..Default::default()
        };

        let count = apply(&manifest, dir.path()).unwrap();
        assert_eq!(count, 0); // Already exists, not overwritten
    }

    #[test]
    fn test_apply_cargo_toml_dev_dependency() {
        let dir = TempDir::new().unwrap();
        fs::write(
            dir.path().join("Cargo.toml"),
            r#"[package]
name = "test"
version = "0.1.0"

[dependencies]
serde = "1"
"#,
        )
        .unwrap();

        let manifest = TaskManifest {
            cargo_toml_additions: vec![CargoTomlAddition {
                kind: CargoTomlKind::DevDependency,
                content: r#"tempfile = "3""#.into(),
            }],
            ..Default::default()
        };

        let count = apply(&manifest, dir.path()).unwrap();
        assert_eq!(count, 1);

        let content = fs::read_to_string(dir.path().join("Cargo.toml")).unwrap();
        assert!(content.contains("tempfile"));
        assert!(content.contains("dev-dependencies"));
    }

    #[test]
    fn test_apply_mod_rs_additions() {
        let dir = TempDir::new().unwrap();
        let adapters = dir.path().join("src/adapters");
        fs::create_dir_all(&adapters).unwrap();
        fs::write(adapters.join("mod.rs"), "pub mod claude;\n").unwrap();

        let manifest = TaskManifest {
            mod_rs_additions: vec![ModRsAddition {
                target: "src/adapters/mod.rs".into(),
                content: "pub mod newadapter;".into(),
            }],
            ..Default::default()
        };

        let count = apply(&manifest, dir.path()).unwrap();
        assert_eq!(count, 1);

        let content = fs::read_to_string(adapters.join("mod.rs")).unwrap();
        assert!(content.contains("pub mod claude;"));
        assert!(content.contains("pub mod newadapter;"));
    }

    #[test]
    fn test_apply_ts_index_additions() {
        let dir = TempDir::new().unwrap();
        let src = dir.path().join("src");
        fs::create_dir_all(&src).unwrap();
        fs::write(src.join("index.ts"), "export { Bar } from './bar';\n").unwrap();

        let manifest = TaskManifest {
            ts_index_additions: vec![TsIndexAddition {
                target: "src/index.ts".into(),
                content: "export { Foo } from './foo';".into(),
            }],
            ..Default::default()
        };

        let count = apply(&manifest, dir.path()).unwrap();
        assert_eq!(count, 1);

        let content = fs::read_to_string(src.join("index.ts")).unwrap();
        assert!(content.contains("export { Bar } from './bar';"));
        assert!(content.contains("export { Foo } from './foo';"));
    }

    #[test]
    fn test_apply_full_manifest() {
        let dir = TempDir::new().unwrap();
        let src = dir.path().join("src");
        fs::create_dir_all(&src).unwrap();
        fs::write(src.join("lib.rs"), "mod existing;\n").unwrap();
        fs::write(
            dir.path().join("Cargo.toml"),
            r#"[package]
name = "test"
version = "0.1.0"

[dependencies]
"#,
        )
        .unwrap();

        let manifest = TaskManifest {
            lib_rs_additions: vec![LibRsAddition {
                kind: LibRsKind::ModDeclaration,
                content: "pub mod analytics;".into(),
            }],
            cargo_toml_additions: vec![CargoTomlAddition {
                kind: CargoTomlKind::Dependency,
                content: r#"chrono = "0.4""#.into(),
            }],
            mod_rs_additions: vec![],
            ts_index_additions: vec![],
        };

        let count = apply(&manifest, dir.path()).unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_parse_file() {
        let dir = TempDir::new().unwrap();
        let manifest_path = dir.path().join("task_manifest.toml");
        fs::write(
            &manifest_path,
            r#"
[[lib_rs_additions]]
kind = "mod_declaration"
content = "pub mod foo;"
"#,
        )
        .unwrap();

        let manifest = parse(&manifest_path).unwrap();
        assert_eq!(manifest.lib_rs_additions.len(), 1);
    }

    #[test]
    fn test_parse_file_not_found() {
        let result = parse(Path::new("/nonexistent/task_manifest.toml"));
        assert!(result.is_err());
    }

    #[test]
    fn test_cargo_toml_not_found() {
        let dir = TempDir::new().unwrap();
        let manifest = TaskManifest {
            cargo_toml_additions: vec![CargoTomlAddition {
                kind: CargoTomlKind::Dependency,
                content: r#"foo = "1""#.into(),
            }],
            ..Default::default()
        };

        let result = apply(&manifest, dir.path());
        assert!(result.is_err());
    }
}
