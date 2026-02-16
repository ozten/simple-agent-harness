//! Migration map: describes what moved during a refactor integration.
//!
//! When a refactor task completes, `generate_migration_map` compares the before/after
//! commits to identify file renames and symbol import path relocations. In-progress
//! worktrees consume this map to update their `use`/`mod` paths after main advances.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::process::Command;

/// A migration map produced by a refactor integration.
///
/// Contains the set of file moves and symbol relocations that occurred.
/// Consumed by `migration_apply::apply_migration_map` to update in-progress worktrees.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MigrationMap {
    /// Files that were renamed/moved (old_path, new_path) relative to repo root.
    pub file_moves: Vec<(PathBuf, PathBuf)>,
    /// Import path relocations: (old_crate_path, new_crate_path).
    ///
    /// Example: `("crate::old_module::Foo", "crate::new_module::Foo")`
    /// These are applied as literal string replacements in `use`/`pub use` statements.
    pub symbol_relocations: Vec<(String, String)>,
}

impl MigrationMap {
    /// Whether this map contains any changes to apply.
    pub fn is_empty(&self) -> bool {
        self.file_moves.is_empty() && self.symbol_relocations.is_empty()
    }
}

/// Generate a migration map by diffing two commits in a repository.
///
/// Detects file renames via `git diff --name-status` (R-type entries) and derives
/// symbol relocations by converting file paths to crate module paths.
///
/// Returns an empty map (not an error) if no renames are detected.
pub fn generate_migration_map(
    repo_dir: &Path,
    before_commit: &str,
    after_commit: &str,
) -> Result<MigrationMap, String> {
    let output = Command::new("git")
        .args([
            "diff",
            "--name-status",
            "--diff-filter=R",
            "-M",
            before_commit,
            after_commit,
        ])
        .current_dir(repo_dir)
        .output()
        .map_err(|e| format!("failed to run git diff: {e}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("git diff failed: {}", stderr.trim()));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut file_moves = Vec::new();
    let mut symbol_relocations = Vec::new();

    for line in stdout.lines() {
        // Format: R100\told_path\tnew_path (tab-separated)
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() >= 3 && parts[0].starts_with('R') {
            let old_path = PathBuf::from(parts[1]);
            let new_path = PathBuf::from(parts[2]);

            // Derive symbol relocations from .rs file moves
            if old_path.extension().and_then(|e| e.to_str()) == Some("rs")
                && new_path.extension().and_then(|e| e.to_str()) == Some("rs")
            {
                if let (Some(old_mod), Some(new_mod)) =
                    (file_path_to_crate_path(&old_path), file_path_to_crate_path(&new_path))
                {
                    symbol_relocations.push((old_mod, new_mod));
                }
            }

            file_moves.push((old_path, new_path));
        }
    }

    Ok(MigrationMap {
        file_moves,
        symbol_relocations,
    })
}

/// Convert a file path like `src/old_module.rs` or `src/foo/bar.rs` to a crate
/// module path like `crate::old_module` or `crate::foo::bar`.
///
/// Returns `None` if the path is not under `src/` or is `main.rs`/`lib.rs`.
fn file_path_to_crate_path(path: &Path) -> Option<String> {
    let path_str = path.to_str()?;

    // Must be under src/
    let rest = path_str.strip_prefix("src/")?;

    // Strip .rs extension
    let without_ext = rest.strip_suffix(".rs")?;

    // Skip main.rs, lib.rs
    if without_ext == "main" || without_ext == "lib" {
        return None;
    }

    // Convert path separators to :: and handle mod.rs
    let module_path = if without_ext.ends_with("/mod") {
        without_ext.strip_suffix("/mod")?
    } else {
        without_ext
    };

    let crate_path = format!("crate::{}", module_path.replace('/', "::"));
    Some(crate_path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command as StdCommand;
    use std::process::Stdio;
    use tempfile::TempDir;

    fn init_test_repo() -> TempDir {
        let dir = TempDir::new().unwrap();
        let repo = dir.path();

        StdCommand::new("git")
            .args(["init"])
            .current_dir(repo)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();
        StdCommand::new("git")
            .args(["config", "user.email", "test@test.com"])
            .current_dir(repo)
            .status()
            .unwrap();
        StdCommand::new("git")
            .args(["config", "user.name", "Test"])
            .current_dir(repo)
            .status()
            .unwrap();

        dir
    }

    #[test]
    fn test_file_path_to_crate_path_simple() {
        assert_eq!(
            file_path_to_crate_path(Path::new("src/config.rs")),
            Some("crate::config".to_string())
        );
    }

    #[test]
    fn test_file_path_to_crate_path_nested() {
        assert_eq!(
            file_path_to_crate_path(Path::new("src/adapters/claude.rs")),
            Some("crate::adapters::claude".to_string())
        );
    }

    #[test]
    fn test_file_path_to_crate_path_mod_rs() {
        assert_eq!(
            file_path_to_crate_path(Path::new("src/adapters/mod.rs")),
            Some("crate::adapters".to_string())
        );
    }

    #[test]
    fn test_file_path_to_crate_path_main_rs() {
        assert_eq!(file_path_to_crate_path(Path::new("src/main.rs")), None);
    }

    #[test]
    fn test_file_path_to_crate_path_lib_rs() {
        assert_eq!(file_path_to_crate_path(Path::new("src/lib.rs")), None);
    }

    #[test]
    fn test_file_path_to_crate_path_not_src() {
        assert_eq!(file_path_to_crate_path(Path::new("tests/foo.rs")), None);
    }

    #[test]
    fn test_migration_map_is_empty() {
        let map = MigrationMap::default();
        assert!(map.is_empty());

        let map = MigrationMap {
            file_moves: vec![(PathBuf::from("a"), PathBuf::from("b"))],
            symbol_relocations: vec![],
        };
        assert!(!map.is_empty());
    }

    #[test]
    fn test_migration_map_json_roundtrip() {
        let map = MigrationMap {
            file_moves: vec![(
                PathBuf::from("src/old.rs"),
                PathBuf::from("src/new.rs"),
            )],
            symbol_relocations: vec![(
                "crate::old::Foo".to_string(),
                "crate::new::Foo".to_string(),
            )],
        };
        let json = serde_json::to_string(&map).unwrap();
        let restored: MigrationMap = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.file_moves.len(), 1);
        assert_eq!(restored.symbol_relocations.len(), 1);
        assert_eq!(restored.symbol_relocations[0].0, "crate::old::Foo");
    }

    #[test]
    fn test_generate_migration_map_empty_diff() {
        let dir = init_test_repo();
        let repo = dir.path();

        // Create initial commit
        std::fs::create_dir_all(repo.join("src")).unwrap();
        std::fs::write(repo.join("src/main.rs"), "fn main() {}").unwrap();
        StdCommand::new("git")
            .args(["add", "."])
            .current_dir(repo)
            .status()
            .unwrap();
        StdCommand::new("git")
            .args(["commit", "-m", "init"])
            .current_dir(repo)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();

        let head = String::from_utf8(
            StdCommand::new("git")
                .args(["rev-parse", "HEAD"])
                .current_dir(repo)
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap()
        .trim()
        .to_string();

        let map = generate_migration_map(repo, &head, &head).unwrap();
        assert!(map.is_empty());
    }

    #[test]
    fn test_generate_migration_map_detects_rename() {
        let dir = init_test_repo();
        let repo = dir.path();

        // Create initial commit with a file
        std::fs::create_dir_all(repo.join("src")).unwrap();
        std::fs::write(repo.join("src/old_module.rs"), "pub struct Foo;").unwrap();
        std::fs::write(repo.join("src/main.rs"), "mod old_module;\nfn main() {}").unwrap();
        StdCommand::new("git")
            .args(["add", "."])
            .current_dir(repo)
            .status()
            .unwrap();
        StdCommand::new("git")
            .args(["commit", "-m", "before"])
            .current_dir(repo)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();

        let before = String::from_utf8(
            StdCommand::new("git")
                .args(["rev-parse", "HEAD"])
                .current_dir(repo)
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap()
        .trim()
        .to_string();

        // Rename the file
        StdCommand::new("git")
            .args(["mv", "src/old_module.rs", "src/new_module.rs"])
            .current_dir(repo)
            .status()
            .unwrap();
        // Update main.rs
        std::fs::write(repo.join("src/main.rs"), "mod new_module;\nfn main() {}").unwrap();
        StdCommand::new("git")
            .args(["add", "."])
            .current_dir(repo)
            .status()
            .unwrap();
        StdCommand::new("git")
            .args(["commit", "-m", "rename"])
            .current_dir(repo)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();

        let after = String::from_utf8(
            StdCommand::new("git")
                .args(["rev-parse", "HEAD"])
                .current_dir(repo)
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap()
        .trim()
        .to_string();

        let map = generate_migration_map(repo, &before, &after).unwrap();
        assert_eq!(map.file_moves.len(), 1);
        assert_eq!(
            map.file_moves[0],
            (
                PathBuf::from("src/old_module.rs"),
                PathBuf::from("src/new_module.rs")
            )
        );
        assert_eq!(map.symbol_relocations.len(), 1);
        assert_eq!(map.symbol_relocations[0].0, "crate::old_module");
        assert_eq!(map.symbol_relocations[0].1, "crate::new_module");
    }

    #[test]
    fn test_generate_migration_map_non_rs_file_no_relocation() {
        let dir = init_test_repo();
        let repo = dir.path();

        // Create and rename a non-Rust file
        std::fs::write(repo.join("docs.md"), "# Docs").unwrap();
        StdCommand::new("git")
            .args(["add", "."])
            .current_dir(repo)
            .status()
            .unwrap();
        StdCommand::new("git")
            .args(["commit", "-m", "init"])
            .current_dir(repo)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();

        let before = String::from_utf8(
            StdCommand::new("git")
                .args(["rev-parse", "HEAD"])
                .current_dir(repo)
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap()
        .trim()
        .to_string();

        StdCommand::new("git")
            .args(["mv", "docs.md", "documentation.md"])
            .current_dir(repo)
            .status()
            .unwrap();
        StdCommand::new("git")
            .args(["commit", "-m", "rename md"])
            .current_dir(repo)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();

        let after = String::from_utf8(
            StdCommand::new("git")
                .args(["rev-parse", "HEAD"])
                .current_dir(repo)
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap()
        .trim()
        .to_string();

        let map = generate_migration_map(repo, &before, &after).unwrap();
        // File move is recorded, but no symbol relocation (not .rs)
        assert_eq!(map.file_moves.len(), 1);
        assert!(map.symbol_relocations.is_empty());
    }
}
