//! Applies a migration map to an in-progress worktree.
//!
//! When a refactor integrates to main and another agent has an in-progress worktree,
//! this module:
//! 1. Applies symbol relocations (find-and-replace on `use`/`mod` import paths in .rs files)
//! 2. Merges main into the worktree branch
//! 3. Runs a cargo check fix loop (up to MAX_FIX_ITERATIONS)
//! 4. If the fix loop exceeds the limit, returns an error indicating abort+re-queue

use crate::migration_map::MigrationMap;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Maximum fix iterations after migration application.
pub const MAX_FIX_ITERATIONS: u32 = 3;

/// Result of applying a migration map to a worktree.
#[derive(Debug)]
pub struct MigrationApplyResult {
    /// Number of files that had relocations applied.
    pub files_modified: usize,
    /// Total number of individual replacements made.
    pub replacements_made: usize,
    /// Whether cargo check passed after migration.
    pub cargo_check_passed: bool,
    /// Number of fix iterations used (0 if passed on first try).
    pub fix_iterations: u32,
}

/// Errors from migration application.
#[derive(Debug)]
pub enum MigrationApplyError {
    /// Failed to read/write files in the worktree.
    Io(std::io::Error),
    /// Git merge failed.
    MergeFailed(String),
    /// Cargo check fix loop exceeded MAX_FIX_ITERATIONS.
    FixLoopExhausted {
        iterations: u32,
        last_errors: String,
    },
}

impl std::fmt::Display for MigrationApplyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MigrationApplyError::Io(e) => write!(f, "I/O error: {e}"),
            MigrationApplyError::MergeFailed(msg) => write!(f, "merge failed: {msg}"),
            MigrationApplyError::FixLoopExhausted {
                iterations,
                last_errors,
            } => write!(
                f,
                "fix loop exhausted after {iterations} iterations: {last_errors}"
            ),
        }
    }
}

impl std::error::Error for MigrationApplyError {}

impl From<std::io::Error> for MigrationApplyError {
    fn from(e: std::io::Error) -> Self {
        MigrationApplyError::Io(e)
    }
}

/// Apply a migration map to an in-progress worktree.
///
/// Steps:
/// 1. Apply symbol relocations to all `.rs` files in the worktree
/// 2. Merge the base branch into the worktree
/// 3. Run cargo check fix loop (up to `MAX_FIX_ITERATIONS`)
///
/// Returns `Ok(result)` if migration succeeds, or `Err(FixLoopExhausted)` if
/// the fix loop exceeds the maximum, indicating the worktree should be aborted
/// and the bead re-queued.
pub fn apply_migration_map(
    migration_map: &MigrationMap,
    worktree_path: &Path,
    base_branch: &str,
) -> Result<MigrationApplyResult, MigrationApplyError> {
    if migration_map.is_empty() {
        return Ok(MigrationApplyResult {
            files_modified: 0,
            replacements_made: 0,
            cargo_check_passed: true,
            fix_iterations: 0,
        });
    }

    // Step 1: Apply symbol relocations to .rs files
    let (files_modified, replacements_made) =
        apply_symbol_relocations(&migration_map.symbol_relocations, worktree_path)?;

    tracing::info!(
        files_modified,
        replacements_made,
        worktree = %worktree_path.display(),
        "applied symbol relocations"
    );

    // Commit relocation changes if any were made
    if replacements_made > 0 {
        git_add_and_commit(
            worktree_path,
            &format!(
                "migration: apply {} symbol relocations across {} files",
                replacements_made, files_modified
            ),
        )?;
    }

    // Step 2: Merge base branch into worktree
    merge_branch_into_worktree(worktree_path, base_branch)?;

    // Step 3: Cargo check fix loop
    let mut fix_iterations = 0u32;
    #[allow(unused_assignments)]
    let mut last_errors = String::new();

    loop {
        match run_cargo_check(worktree_path) {
            Ok(()) => {
                return Ok(MigrationApplyResult {
                    files_modified,
                    replacements_made,
                    cargo_check_passed: true,
                    fix_iterations,
                });
            }
            Err(errors) => {
                fix_iterations += 1;
                last_errors = errors;

                if fix_iterations >= MAX_FIX_ITERATIONS {
                    return Err(MigrationApplyError::FixLoopExhausted {
                        iterations: fix_iterations,
                        last_errors,
                    });
                }

                tracing::warn!(
                    iteration = fix_iterations,
                    max = MAX_FIX_ITERATIONS,
                    "cargo check failed after migration, attempting fix"
                );

                // Attempt automatic fix: cargo fix
                let fix_output = Command::new("cargo")
                    .args(["fix", "--allow-dirty", "--allow-staged"])
                    .current_dir(worktree_path)
                    .output();

                match fix_output {
                    Ok(output) if output.status.success() => {
                        tracing::info!(iteration = fix_iterations, "cargo fix succeeded");
                    }
                    Ok(output) => {
                        let stderr = String::from_utf8_lossy(&output.stderr);
                        tracing::warn!(
                            iteration = fix_iterations,
                            "cargo fix did not fully succeed: {}",
                            stderr.trim()
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            iteration = fix_iterations,
                            "cargo fix failed to run: {e}"
                        );
                    }
                }
            }
        }
    }
}

/// Apply symbol relocations to all `.rs` files under `worktree_path`.
///
/// Only replaces within `use` and `pub use` lines (not in comments or string literals).
/// Returns (files_modified, total_replacements).
fn apply_symbol_relocations(
    relocations: &[(String, String)],
    worktree_path: &Path,
) -> Result<(usize, usize), std::io::Error> {
    if relocations.is_empty() {
        return Ok((0, 0));
    }

    let rs_files = collect_rs_files(worktree_path);
    let mut total_files_modified = 0usize;
    let mut total_replacements = 0usize;

    for file_path in &rs_files {
        let content = std::fs::read_to_string(file_path)?;
        let mut modified = false;
        let mut new_lines = Vec::new();

        for line in content.lines() {
            let trimmed = line.trim();

            // Only apply relocations to use/pub use lines, not comments or strings
            if is_use_or_mod_statement(trimmed) {
                let mut new_line = line.to_string();
                for (old_path, new_path) in relocations {
                    // Strip "crate::" prefix for matching in use statements,
                    // since use statements have `use crate::` as a prefix already
                    let old_suffix = old_path.strip_prefix("crate::").unwrap_or(old_path);
                    let new_suffix = new_path.strip_prefix("crate::").unwrap_or(new_path);

                    if new_line.contains(old_suffix) {
                        let before = new_line.clone();
                        new_line = new_line.replace(old_suffix, new_suffix);
                        if new_line != before {
                            total_replacements += 1;
                            modified = true;
                        }
                    }
                }
                new_lines.push(new_line);
            } else {
                new_lines.push(line.to_string());
            }
        }

        if modified {
            // Preserve trailing newline if original had one
            let mut output = new_lines.join("\n");
            if content.ends_with('\n') {
                output.push('\n');
            }
            std::fs::write(file_path, output)?;
            total_files_modified += 1;
        }
    }

    Ok((total_files_modified, total_replacements))
}

/// Check if a trimmed line is a `use`/`pub use` or `mod` statement (not a comment).
fn is_use_or_mod_statement(trimmed: &str) -> bool {
    if trimmed.starts_with("//") {
        return false;
    }
    trimmed.starts_with("use ")
        || trimmed.starts_with("pub use ")
        || trimmed.starts_with("pub(crate) use ")
        || trimmed.starts_with("mod ")
        || trimmed.starts_with("pub mod ")
        || trimmed.starts_with("pub(crate) mod ")
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
        // Skip .git directories and target directories
        if path.is_dir() {
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if name == ".git" || name == "target" || name == ".blacksmith" {
                continue;
            }
            collect_rs_files_recursive(&path, files);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            files.push(path);
        }
    }
}

/// Merge a branch into the worktree.
fn merge_branch_into_worktree(
    worktree_path: &Path,
    branch: &str,
) -> Result<(), MigrationApplyError> {
    let output = Command::new("git")
        .args(["merge", branch, "--no-edit"])
        .current_dir(worktree_path)
        .output()
        .map_err(|e| MigrationApplyError::MergeFailed(format!("failed to run git merge: {e}")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        // Abort the merge if it left conflicts
        let _ = Command::new("git")
            .args(["merge", "--abort"])
            .current_dir(worktree_path)
            .output();
        return Err(MigrationApplyError::MergeFailed(format!(
            "git merge {branch} failed: {}",
            stderr.trim()
        )));
    }

    Ok(())
}

/// Run `cargo check` in the worktree. Returns `Ok(())` if it passes,
/// or `Err(error_output)` with compiler diagnostics.
fn run_cargo_check(worktree_path: &Path) -> Result<(), String> {
    let cargo_toml = worktree_path.join("Cargo.toml");
    if !cargo_toml.exists() {
        // No Cargo.toml — skip check, assume success
        return Ok(());
    }

    let output = Command::new("cargo")
        .args(["check", "--message-format=short"])
        .current_dir(worktree_path)
        .output()
        .map_err(|e| format!("failed to run cargo check: {e}"))?;

    if output.status.success() {
        Ok(())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        Err(stderr.trim().to_string())
    }
}

/// Stage all changes and commit.
fn git_add_and_commit(worktree_path: &Path, message: &str) -> Result<(), MigrationApplyError> {
    let add_output = Command::new("git")
        .args(["add", "-A"])
        .current_dir(worktree_path)
        .output()
        .map_err(|e| MigrationApplyError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;

    if !add_output.status.success() {
        let stderr = String::from_utf8_lossy(&add_output.stderr);
        return Err(MigrationApplyError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("git add failed: {}", stderr.trim()),
        )));
    }

    let commit_output = Command::new("git")
        .args(["commit", "-m", message, "--allow-empty"])
        .current_dir(worktree_path)
        .output()
        .map_err(|e| MigrationApplyError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;

    if !commit_output.status.success() {
        let stderr = String::from_utf8_lossy(&commit_output.stderr);
        if !stderr.contains("nothing to commit") {
            return Err(MigrationApplyError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("git commit failed: {}", stderr.trim()),
            )));
        }
    }

    Ok(())
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

        std::fs::write(repo.join("README.md"), "init").unwrap();
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
        StdCommand::new("git")
            .args(["branch", "-M", "main"])
            .current_dir(repo)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();

        dir
    }

    #[test]
    fn test_is_use_or_mod_statement() {
        assert!(is_use_or_mod_statement("use crate::config::Foo;"));
        assert!(is_use_or_mod_statement("pub use crate::db::Bar;"));
        assert!(is_use_or_mod_statement("pub(crate) use crate::pool::Baz;"));
        assert!(is_use_or_mod_statement("use std::path::Path;"));
        assert!(is_use_or_mod_statement("mod config;"));
        assert!(is_use_or_mod_statement("pub mod adapters;"));
        assert!(is_use_or_mod_statement("pub(crate) mod internal;"));

        assert!(!is_use_or_mod_statement("// use crate::config::Foo;"));
        assert!(!is_use_or_mod_statement("// mod old_module;"));
        assert!(!is_use_or_mod_statement("fn use_something() {}"));
        assert!(!is_use_or_mod_statement("let x = 42;"));
    }

    #[test]
    fn test_collect_rs_files() {
        let dir = TempDir::new().unwrap();
        let src = dir.path().join("src");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(src.join("main.rs"), "fn main() {}").unwrap();
        std::fs::write(src.join("config.rs"), "pub struct Cfg;").unwrap();
        std::fs::write(dir.path().join("README.md"), "# readme").unwrap();

        let files = collect_rs_files(dir.path());
        assert_eq!(files.len(), 2);
        assert!(files.iter().any(|f| f.ends_with("main.rs")));
        assert!(files.iter().any(|f| f.ends_with("config.rs")));
    }

    #[test]
    fn test_collect_rs_files_skips_git_and_target() {
        let dir = TempDir::new().unwrap();
        let root = dir.path();
        std::fs::create_dir_all(root.join(".git")).unwrap();
        std::fs::write(root.join(".git/config.rs"), "// git internal").unwrap();
        std::fs::create_dir_all(root.join("target/debug")).unwrap();
        std::fs::write(root.join("target/debug/build.rs"), "// build").unwrap();
        std::fs::write(root.join("real.rs"), "fn real() {}").unwrap();

        let files = collect_rs_files(root);
        assert_eq!(files.len(), 1);
        assert!(files[0].ends_with("real.rs"));
    }

    #[test]
    fn test_apply_symbol_relocations_basic() {
        let dir = TempDir::new().unwrap();
        let src = dir.path().join("src");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(
            src.join("main.rs"),
            "use crate::old_module::Foo;\nuse crate::old_module::Bar;\nfn main() {}\n",
        )
        .unwrap();

        let relocations = vec![(
            "crate::old_module".to_string(),
            "crate::new_module".to_string(),
        )];

        let (files_modified, replacements) =
            apply_symbol_relocations(&relocations, dir.path()).unwrap();

        assert_eq!(files_modified, 1);
        assert_eq!(replacements, 2);

        let content = std::fs::read_to_string(src.join("main.rs")).unwrap();
        assert!(content.contains("use crate::new_module::Foo;"));
        assert!(content.contains("use crate::new_module::Bar;"));
        assert!(!content.contains("old_module"));
    }

    #[test]
    fn test_apply_symbol_relocations_leaves_non_use_lines() {
        let dir = TempDir::new().unwrap();
        let src = dir.path().join("src");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(
            src.join("lib.rs"),
            concat!(
                "use crate::old_module::Foo;\n",
                "// use crate::old_module::Bar; // commented out\n",
                "fn do_stuff() {\n",
                "    let s = \"old_module\";\n",
                "    println!(\"using old_module\");\n",
                "}\n",
            ),
        )
        .unwrap();

        let relocations = vec![(
            "crate::old_module".to_string(),
            "crate::new_module".to_string(),
        )];

        let (files_modified, replacements) =
            apply_symbol_relocations(&relocations, dir.path()).unwrap();

        assert_eq!(files_modified, 1);
        assert_eq!(replacements, 1); // Only the `use` line, not comment or string

        let content = std::fs::read_to_string(src.join("lib.rs")).unwrap();
        assert!(content.contains("use crate::new_module::Foo;"));
        // Comment should be untouched
        assert!(content.contains("// use crate::old_module::Bar;"));
        // String literal should be untouched
        assert!(content.contains("\"old_module\""));
        assert!(content.contains("\"using old_module\""));
    }

    #[test]
    fn test_apply_symbol_relocations_untouched_files() {
        let dir = TempDir::new().unwrap();
        let src = dir.path().join("src");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(
            src.join("unrelated.rs"),
            "use crate::config::Config;\nfn setup() {}\n",
        )
        .unwrap();

        let relocations = vec![(
            "crate::old_module".to_string(),
            "crate::new_module".to_string(),
        )];

        let (files_modified, replacements) =
            apply_symbol_relocations(&relocations, dir.path()).unwrap();

        assert_eq!(files_modified, 0);
        assert_eq!(replacements, 0);

        // File should be unchanged
        let content = std::fs::read_to_string(src.join("unrelated.rs")).unwrap();
        assert!(content.contains("use crate::config::Config;"));
    }

    #[test]
    fn test_apply_symbol_relocations_empty() {
        let dir = TempDir::new().unwrap();
        let relocations: Vec<(String, String)> = vec![];
        let (files_modified, replacements) =
            apply_symbol_relocations(&relocations, dir.path()).unwrap();
        assert_eq!(files_modified, 0);
        assert_eq!(replacements, 0);
    }

    #[test]
    fn test_apply_symbol_relocations_pub_use() {
        let dir = TempDir::new().unwrap();
        let src = dir.path().join("src");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(
            src.join("lib.rs"),
            "pub use crate::old_module::Foo;\npub(crate) use crate::old_module::Bar;\n",
        )
        .unwrap();

        let relocations = vec![(
            "crate::old_module".to_string(),
            "crate::new_module".to_string(),
        )];

        let (files_modified, replacements) =
            apply_symbol_relocations(&relocations, dir.path()).unwrap();

        assert_eq!(files_modified, 1);
        assert_eq!(replacements, 2);

        let content = std::fs::read_to_string(src.join("lib.rs")).unwrap();
        assert!(content.contains("pub use crate::new_module::Foo;"));
        assert!(content.contains("pub(crate) use crate::new_module::Bar;"));
    }

    #[test]
    fn test_apply_symbol_relocations_nested_path() {
        let dir = TempDir::new().unwrap();
        let src = dir.path().join("src");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(
            src.join("main.rs"),
            "use crate::adapters::old_adapter::Client;\n",
        )
        .unwrap();

        let relocations = vec![(
            "crate::adapters::old_adapter".to_string(),
            "crate::adapters::new_adapter".to_string(),
        )];

        let (files_modified, replacements) =
            apply_symbol_relocations(&relocations, dir.path()).unwrap();

        assert_eq!(files_modified, 1);
        assert_eq!(replacements, 1);

        let content = std::fs::read_to_string(src.join("main.rs")).unwrap();
        assert!(content.contains("use crate::adapters::new_adapter::Client;"));
    }

    #[test]
    fn test_apply_migration_map_empty_map() {
        let dir = init_test_repo();
        let map = MigrationMap::default();
        let result = apply_migration_map(&map, dir.path(), "main").unwrap();
        assert_eq!(result.files_modified, 0);
        assert_eq!(result.replacements_made, 0);
        assert!(result.cargo_check_passed);
        assert_eq!(result.fix_iterations, 0);
    }

    #[test]
    fn test_apply_migration_map_with_worktree() {
        let dir = init_test_repo();
        let repo = dir.path();

        // Create src files in the repo and commit
        std::fs::create_dir_all(repo.join("src")).unwrap();
        std::fs::write(
            repo.join("src/main.rs"),
            "mod old_module;\nuse crate::old_module::Foo;\nfn main() {}\n",
        )
        .unwrap();
        std::fs::write(repo.join("src/old_module.rs"), "pub struct Foo;\n").unwrap();
        StdCommand::new("git")
            .args(["add", "."])
            .current_dir(repo)
            .status()
            .unwrap();
        StdCommand::new("git")
            .args(["commit", "-m", "add old_module"])
            .current_dir(repo)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();

        // Create a worktree on a named branch (simulates real worker flow)
        let wt_dir = repo.join("worktrees");
        std::fs::create_dir_all(&wt_dir).unwrap();
        let wt_path = wt_dir.join("worker-0-test");
        StdCommand::new("git")
            .args([
                "worktree",
                "add",
                "-b",
                "worker-branch",
                &wt_path.to_string_lossy(),
                "main",
            ])
            .current_dir(repo)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();

        // Simulate the agent making a change in the worktree (unrelated file)
        std::fs::write(wt_path.join("agent_work.txt"), "agent output").unwrap();
        StdCommand::new("git")
            .args(["add", "agent_work.txt"])
            .current_dir(&wt_path)
            .status()
            .unwrap();
        StdCommand::new("git")
            .args(["commit", "-m", "agent work"])
            .current_dir(&wt_path)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();

        // Make a change on main (rename old_module to new_module)
        StdCommand::new("git")
            .args(["mv", "src/old_module.rs", "src/new_module.rs"])
            .current_dir(repo)
            .status()
            .unwrap();
        std::fs::write(
            repo.join("src/main.rs"),
            "mod new_module;\nuse crate::new_module::Foo;\nfn main() {}\n",
        )
        .unwrap();
        StdCommand::new("git")
            .args(["add", "."])
            .current_dir(repo)
            .status()
            .unwrap();
        StdCommand::new("git")
            .args(["commit", "-m", "rename module"])
            .current_dir(repo)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();

        // The worktree still has old_module references
        let wt_content = std::fs::read_to_string(wt_path.join("src/main.rs")).unwrap();
        assert!(wt_content.contains("old_module"));

        // Build migration map
        let map = MigrationMap {
            file_moves: vec![(
                PathBuf::from("src/old_module.rs"),
                PathBuf::from("src/new_module.rs"),
            )],
            symbol_relocations: vec![(
                "crate::old_module".to_string(),
                "crate::new_module".to_string(),
            )],
        };

        // Apply migration map to worktree
        let result = apply_migration_map(&map, &wt_path, "main").unwrap();

        assert_eq!(result.files_modified, 1);
        // Both `mod old_module;` and `use crate::old_module::Foo;` are relocated
        assert_eq!(result.replacements_made, 2);

        // Verify the worktree's main.rs was updated
        let updated_content = std::fs::read_to_string(wt_path.join("src/main.rs")).unwrap();
        assert!(
            updated_content.contains("use crate::new_module::Foo;"),
            "import should be updated: {updated_content}"
        );

        // Clean up worktree
        StdCommand::new("git")
            .args(["worktree", "remove", "--force", &wt_path.to_string_lossy()])
            .current_dir(repo)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();
    }

    #[test]
    fn test_migration_apply_error_display() {
        let e = MigrationApplyError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "file not found",
        ));
        assert!(e.to_string().contains("file not found"));

        let e = MigrationApplyError::MergeFailed("conflict in main.rs".to_string());
        assert!(e.to_string().contains("conflict in main.rs"));

        let e = MigrationApplyError::FixLoopExhausted {
            iterations: 3,
            last_errors: "type mismatch".to_string(),
        };
        assert!(e.to_string().contains("3 iterations"));
        assert!(e.to_string().contains("type mismatch"));
    }

    #[test]
    fn test_apply_symbol_relocations_preserves_trailing_newline() {
        let dir = TempDir::new().unwrap();
        let src = dir.path().join("src");
        std::fs::create_dir_all(&src).unwrap();
        let original = "use crate::old_module::Foo;\nfn main() {}\n";
        std::fs::write(src.join("main.rs"), original).unwrap();

        let relocations = vec![(
            "crate::old_module".to_string(),
            "crate::new_module".to_string(),
        )];

        apply_symbol_relocations(&relocations, dir.path()).unwrap();

        let content = std::fs::read_to_string(src.join("main.rs")).unwrap();
        assert!(content.ends_with('\n'), "trailing newline should be preserved");
    }

    #[test]
    fn test_apply_symbol_relocations_multiple_relocations() {
        let dir = TempDir::new().unwrap();
        let src = dir.path().join("src");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(
            src.join("main.rs"),
            concat!(
                "use crate::module_a::Foo;\n",
                "use crate::module_b::Bar;\n",
                "use crate::module_c::Baz;\n",
            ),
        )
        .unwrap();

        let relocations = vec![
            (
                "crate::module_a".to_string(),
                "crate::renamed_a".to_string(),
            ),
            (
                "crate::module_b".to_string(),
                "crate::renamed_b".to_string(),
            ),
        ];

        let (files_modified, replacements) =
            apply_symbol_relocations(&relocations, dir.path()).unwrap();

        assert_eq!(files_modified, 1);
        assert_eq!(replacements, 2);

        let content = std::fs::read_to_string(src.join("main.rs")).unwrap();
        assert!(content.contains("use crate::renamed_a::Foo;"));
        assert!(content.contains("use crate::renamed_b::Bar;"));
        // module_c should be untouched
        assert!(content.contains("use crate::module_c::Baz;"));
    }
}
