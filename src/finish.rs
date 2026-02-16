//! `blacksmith finish` — quality-gated bead closure.
//!
//! Replaces `bd-finish.sh` with a compiled subcommand that runs configurable
//! quality gates before allowing a bead to be closed. This prevents agents
//! from closing beads without actually completing the work.

use crate::config::QualityGatesConfig;
use crate::db;
use std::path::Path;
use std::process::Command;

/// Result of running a single gate command.
#[derive(Debug)]
struct GateResult {
    command: String,
    success: bool,
    output: String,
}

/// Result of the entire finish operation.
#[derive(Debug)]
pub struct FinishResult {
    pub success: bool,
    pub message: String,
}

/// Run a single shell command, returning its success status and combined output.
fn run_gate_command(cmd: &str, working_dir: &Path) -> GateResult {
    let result = Command::new("sh")
        .args(["-c", cmd])
        .current_dir(working_dir)
        .output();

    match result {
        Ok(output) => {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            let combined = format!("{}{}", stdout, stderr);
            GateResult {
                command: cmd.to_string(),
                success: output.status.success(),
                output: combined,
            }
        }
        Err(e) => GateResult {
            command: cmd.to_string(),
            success: false,
            output: format!("Failed to execute: {e}"),
        },
    }
}

/// Run all commands in a gate. All must succeed for the gate to pass.
fn run_gate(name: &str, commands: &[String], working_dir: &Path) -> Result<(), String> {
    if commands.is_empty() {
        return Ok(());
    }

    for cmd in commands {
        eprintln!("  Running {name} gate: {cmd}");
        let result = run_gate_command(cmd, working_dir);
        if !result.success {
            return Err(format!(
                "{name} gate failed: {}\n{}",
                result.command,
                result
                    .output
                    .lines()
                    .take(50)
                    .collect::<Vec<_>>()
                    .join("\n")
            ));
        }
    }
    Ok(())
}

/// Verify bead deliverables by checking affected files and running verify commands.
///
/// Parses the bead description for:
/// - `## Affected files` section: checks that files marked `(new)` exist
/// - `## Verify` section: runs `Run:` commands
fn verify_deliverables(bead_id: &str, working_dir: &Path) -> Result<(), String> {
    // Fetch bead description via bd show --json
    let output = Command::new("bd")
        .args(["show", bead_id, "--allow-stale", "--json"])
        .current_dir(working_dir)
        .output();

    let description = match output {
        Ok(out) if out.status.success() => {
            let json_str = String::from_utf8_lossy(&out.stdout);
            // Parse JSON array, extract description from first element
            match serde_json::from_str::<serde_json::Value>(&json_str) {
                Ok(serde_json::Value::Array(arr)) if !arr.is_empty() => {
                    arr[0]["description"].as_str().unwrap_or("").to_string()
                }
                _ => String::new(),
            }
        }
        _ => {
            eprintln!("  Could not fetch bead description — skipping deliverable verification");
            return Ok(());
        }
    };

    if description.is_empty() {
        return Ok(());
    }

    let mut failures = Vec::new();

    // Check ## Affected files section for (new) files that must exist
    if let Some(affected) = extract_section(&description, "## Affected files") {
        for line in affected.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            // Match lines like "- src/foo.rs (new)" or "- src/foo.rs (new — description)"
            if let Some(path) = extract_new_file_path(trimmed) {
                let full = working_dir.join(&path);
                if !full.exists() {
                    failures.push(format!(
                        "MISSING: Affected file marked (new) does not exist: {path}"
                    ));
                } else {
                    eprintln!("  OK: {path} exists");
                }
            }
            // Check (modified) files exist
            if let Some(path) = extract_modified_file_path(trimmed) {
                let full = working_dir.join(&path);
                if !full.exists() {
                    failures.push(format!(
                        "MISSING: Affected file marked (modified) does not exist: {path}"
                    ));
                }
            }
        }
    }

    // Run ## Verify commands
    if let Some(verify) = extract_section(&description, "## Verify") {
        for line in verify.lines() {
            let trimmed = line.trim().trim_start_matches('-').trim();
            if let Some(cmd) = trimmed.strip_prefix("Run:") {
                let cmd = cmd.trim();
                if cmd.is_empty() {
                    continue;
                }
                eprintln!("  Running verify command: {cmd}");
                let result = run_gate_command(cmd, working_dir);
                if !result.success {
                    failures.push(format!("Verify command failed: {cmd}"));
                } else {
                    eprintln!("  Verify command passed");
                }
            }
        }
    }

    if failures.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "Bead deliverable verification failed:\n  {}",
            failures.join("\n  ")
        ))
    }
}

/// Extract a markdown section (from header to next ## header or end).
fn extract_section(text: &str, header: &str) -> Option<String> {
    let mut in_section = false;
    let mut lines = Vec::new();

    for line in text.lines() {
        if line.starts_with(header) {
            in_section = true;
            continue;
        }
        if in_section {
            if line.starts_with("## ") {
                break;
            }
            lines.push(line);
        }
    }

    if lines.is_empty() {
        None
    } else {
        Some(lines.join("\n"))
    }
}

/// Extract file path from a line matching `- path/to/file (new...)`.
fn extract_new_file_path(line: &str) -> Option<String> {
    let line = line.trim_start_matches('-').trim();
    // Check if line contains "(new" (case-insensitive)
    let lower = line.to_lowercase();
    if !lower.contains("(new") {
        return None;
    }
    // Extract path: everything before " (new"
    let idx = lower.find("(new")?;
    let path = line[..idx].trim();
    if path.is_empty() {
        None
    } else {
        Some(path.to_string())
    }
}

/// Extract file path from a line matching `- path/to/file (modified...)`.
fn extract_modified_file_path(line: &str) -> Option<String> {
    let line = line.trim_start_matches('-').trim();
    let lower = line.to_lowercase();
    if !lower.contains("(modified") {
        return None;
    }
    let idx = lower.find("(modified")?;
    let path = line[..idx].trim();
    if path.is_empty() {
        None
    } else {
        Some(path.to_string())
    }
}

/// Run the full finish protocol:
/// 1. Quality gates (check, test, lint, format)
/// 2. Deliverable verification
/// 3. Stage files + git commit
/// 4. bd close + bd sync
/// 5. Auto-commit .beads/ changes
/// 6. git push
pub fn handle_finish(
    bead_id: &str,
    commit_msg: &str,
    files: &[String],
    gates_config: &QualityGatesConfig,
    db_path: Option<&Path>,
) -> FinishResult {
    let working_dir = match std::env::current_dir() {
        Ok(d) => d,
        Err(e) => {
            return FinishResult {
                success: false,
                message: format!("Cannot determine working directory: {e}"),
            }
        }
    };

    // --- Step 0: Quality gates ---
    eprintln!("=== blacksmith finish: closing {bead_id} ===\n");

    // 0a. Check gate
    eprintln!("[0a] Running check gate...");
    if let Err(e) = run_gate("check", &gates_config.check, &working_dir) {
        eprintln!("\n=== CHECK GATE FAILED ===");
        eprintln!("Bead {bead_id} will NOT be closed. Fix compilation errors first.");
        return FinishResult {
            success: false,
            message: e,
        };
    }
    eprintln!("[0a] Check gate passed\n");

    // 0b. Test gate
    eprintln!("[0b] Running test gate...");
    if let Err(e) = run_gate("test", &gates_config.test, &working_dir) {
        eprintln!("\n=== TEST GATE FAILED ===");
        eprintln!("Bead {bead_id} will NOT be closed. Fix failing tests first.");
        return FinishResult {
            success: false,
            message: e,
        };
    }
    eprintln!("[0b] Test gate passed\n");

    // 0c. Lint gate
    eprintln!("[0c] Running lint gate...");
    if let Err(e) = run_gate("lint", &gates_config.lint, &working_dir) {
        eprintln!("\n=== LINT GATE FAILED ===");
        eprintln!("Bead {bead_id} will NOT be closed. Fix lint errors first.");
        return FinishResult {
            success: false,
            message: e,
        };
    }
    eprintln!("[0c] Lint gate passed\n");

    // 0d. Format gate
    eprintln!("[0d] Running format gate...");
    if let Err(e) = run_gate("format", &gates_config.format, &working_dir) {
        eprintln!("\n=== FORMAT GATE FAILED ===");
        eprintln!("Bead {bead_id} will NOT be closed. Fix formatting first.");
        return FinishResult {
            success: false,
            message: e,
        };
    }
    eprintln!("[0d] Format gate passed\n");

    // 0e. Deliverable verification
    eprintln!("[0e] Verifying bead deliverables...");
    if let Err(e) = verify_deliverables(bead_id, &working_dir) {
        eprintln!("\n=== DELIVERABLE VERIFICATION FAILED ===");
        eprintln!("Bead {bead_id} will NOT be closed.");
        return FinishResult {
            success: false,
            message: e,
        };
    }
    eprintln!("[0e] Deliverable verification passed\n");

    // --- Step 1: Append PROGRESS.txt to PROGRESS_LOG.txt ---
    let progress_path = working_dir.join("PROGRESS.txt");
    let log_path = working_dir.join("PROGRESS_LOG.txt");
    if progress_path.exists() {
        if let Ok(progress_content) = std::fs::read_to_string(&progress_path) {
            let timestamp = chrono::Local::now().format("%Y-%m-%d %H:%M:%S");
            let entry = format!("\n--- {timestamp} | {bead_id} ---\n{progress_content}");
            let _ = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&log_path)
                .and_then(|mut f| std::io::Write::write_all(&mut f, entry.as_bytes()));
            eprintln!("[1] Appended PROGRESS.txt to PROGRESS_LOG.txt");
        }
    } else {
        eprintln!("[1] No PROGRESS.txt found, skipping log append");
    }

    // --- Step 2: Stage files ---
    let stage_result = if files.is_empty() {
        // Stage all tracked modified files
        Command::new("git")
            .args(["add", "-u"])
            .current_dir(&working_dir)
            .output()
    } else {
        let mut args = vec!["add".to_string()];
        args.extend(files.iter().cloned());
        Command::new("git")
            .args(&args)
            .current_dir(&working_dir)
            .output()
    };

    match stage_result {
        Ok(out) if out.status.success() => {
            if files.is_empty() {
                eprintln!("[2] Staged all tracked modified files (git add -u)");
            } else {
                eprintln!("[2] Staged {} specified files", files.len());
            }
        }
        Ok(out) => {
            let stderr = String::from_utf8_lossy(&out.stderr);
            return FinishResult {
                success: false,
                message: format!("git add failed: {stderr}"),
            };
        }
        Err(e) => {
            return FinishResult {
                success: false,
                message: format!("git add failed: {e}"),
            };
        }
    }

    // Always include progress files if they exist
    let _ = Command::new("git")
        .args(["add", "-f", "PROGRESS.txt", "PROGRESS_LOG.txt"])
        .current_dir(&working_dir)
        .output();

    // --- Step 3: Git commit ---
    let full_msg = format!("{bead_id}: {commit_msg}");
    let commit_result = Command::new("git")
        .args(["commit", "-m", &full_msg, "--no-verify"])
        .current_dir(&working_dir)
        .output();

    match commit_result {
        Ok(out) if out.status.success() => {
            eprintln!("[3] Committed: {full_msg}");
        }
        Ok(out) => {
            let stderr = String::from_utf8_lossy(&out.stderr);
            // "nothing to commit" is OK — may happen if files were already committed
            if stderr.contains("nothing to commit") {
                eprintln!("[3] Nothing to commit (changes already committed)");
            } else {
                return FinishResult {
                    success: false,
                    message: format!("git commit failed: {stderr}"),
                };
            }
        }
        Err(e) => {
            return FinishResult {
                success: false,
                message: format!("git commit failed: {e}"),
            };
        }
    }

    // --- Step 4: bd close ---
    let close_result = Command::new("bd")
        .args(["close", bead_id, &format!("--reason={commit_msg}")])
        .current_dir(&working_dir)
        .output();

    match close_result {
        Ok(out) if out.status.success() => {
            eprintln!("[4] Closed bead {bead_id}");
        }
        Ok(out) => {
            let stderr = String::from_utf8_lossy(&out.stderr);
            return FinishResult {
                success: false,
                message: format!("bd close failed: {stderr}"),
            };
        }
        Err(e) => {
            return FinishResult {
                success: false,
                message: format!("bd close failed: {e}"),
            };
        }
    }

    // --- Step 4b: Mark bead completed in SQLite ---
    if let Some(path) = db_path {
        match db::open_or_create(path) {
            Ok(conn) => match db::mark_bead_completed(&conn, bead_id) {
                Ok(true) => eprintln!("[4b] Marked bead {bead_id} completed in DB"),
                Ok(false) => eprintln!("[4b] Bead {bead_id} not found or already completed in DB"),
                Err(e) => eprintln!("[4b] Warning: failed to mark bead completed in DB: {e}"),
            },
            Err(e) => eprintln!("[4b] Warning: could not open database: {e}"),
        }
    }

    // --- Step 5: bd sync ---
    let _ = Command::new("bd")
        .args(["sync"])
        .current_dir(&working_dir)
        .output();
    eprintln!("[5] Synced beads");

    // --- Step 6: Auto-commit .beads/ if dirty ---
    let beads_dirty = Command::new("git")
        .args(["diff", "--quiet", ".beads/"])
        .current_dir(&working_dir)
        .status()
        .map(|s| !s.success())
        .unwrap_or(false);

    let beads_staged_dirty = Command::new("git")
        .args(["diff", "--cached", "--quiet", ".beads/"])
        .current_dir(&working_dir)
        .status()
        .map(|s| !s.success())
        .unwrap_or(false);

    if beads_dirty || beads_staged_dirty {
        let _ = Command::new("git")
            .args(["add", ".beads/"])
            .current_dir(&working_dir)
            .output();
        let timestamp = chrono::Local::now().format("%Y-%m-%d %H:%M:%S");
        let _ = Command::new("git")
            .args([
                "commit",
                "-m",
                &format!("bd sync: {timestamp}"),
                "--no-verify",
            ])
            .current_dir(&working_dir)
            .output();
        eprintln!("[6] Committed .beads/ changes");
    } else {
        eprintln!("[6] .beads/ already clean");
    }

    // --- Step 7: Git push ---
    let push_result = Command::new("git")
        .args(["push"])
        .current_dir(&working_dir)
        .output();

    match push_result {
        Ok(out) if out.status.success() => {
            eprintln!("[7] Pushed to remote");
        }
        Ok(out) => {
            let stderr = String::from_utf8_lossy(&out.stderr);
            eprintln!("[7] Warning: git push failed: {stderr}");
            // Don't fail the whole operation for a push failure
        }
        Err(e) => {
            eprintln!("[7] Warning: git push failed: {e}");
        }
    }

    eprintln!("\n=== Done. {bead_id} closed and pushed. ===");

    FinishResult {
        success: true,
        message: format!("Bead {bead_id} closed successfully"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_section_found() {
        let text = "## Affected files\n- src/main.rs (modified)\n- src/new.rs (new)\n\n## Verify\n- Run: cargo test\n";
        let section = extract_section(text, "## Affected files").unwrap();
        assert!(section.contains("src/main.rs"));
        assert!(section.contains("src/new.rs"));
    }

    #[test]
    fn test_extract_section_not_found() {
        let text = "## Description\nSome text\n";
        assert!(extract_section(text, "## Affected files").is_none());
    }

    #[test]
    fn test_extract_section_stops_at_next_header() {
        let text = "## Affected files\n- src/main.rs\n## Done When\n- tests pass\n";
        let section = extract_section(text, "## Affected files").unwrap();
        assert!(section.contains("src/main.rs"));
        assert!(!section.contains("tests pass"));
    }

    #[test]
    fn test_extract_new_file_path() {
        assert_eq!(
            extract_new_file_path("- src/finish.rs (new)"),
            Some("src/finish.rs".to_string())
        );
        assert_eq!(
            extract_new_file_path("- src/finish.rs (new — implements finish)"),
            Some("src/finish.rs".to_string())
        );
        assert_eq!(extract_new_file_path("- src/main.rs (modified)"), None);
        assert_eq!(extract_new_file_path("- src/main.rs"), None);
    }

    #[test]
    fn test_extract_modified_file_path() {
        assert_eq!(
            extract_modified_file_path("- src/main.rs (modified)"),
            Some("src/main.rs".to_string())
        );
        assert_eq!(
            extract_modified_file_path("- src/main.rs (modified — added finish cmd)"),
            Some("src/main.rs".to_string())
        );
        assert_eq!(extract_modified_file_path("- src/new.rs (new)"), None);
    }

    #[test]
    fn test_run_gate_command_success() {
        let dir = tempfile::tempdir().unwrap();
        let result = run_gate_command("echo hello", dir.path());
        assert!(result.success);
        assert!(result.output.contains("hello"));
    }

    #[test]
    fn test_run_gate_command_failure() {
        let dir = tempfile::tempdir().unwrap();
        let result = run_gate_command("exit 1", dir.path());
        assert!(!result.success);
    }

    #[test]
    fn test_run_gate_empty_commands() {
        let dir = tempfile::tempdir().unwrap();
        assert!(run_gate("empty", &[], dir.path()).is_ok());
    }

    #[test]
    fn test_run_gate_passes() {
        let dir = tempfile::tempdir().unwrap();
        let commands = vec!["true".to_string(), "echo ok".to_string()];
        assert!(run_gate("test", &commands, dir.path()).is_ok());
    }

    #[test]
    fn test_run_gate_fails_on_first_failure() {
        let dir = tempfile::tempdir().unwrap();
        let commands = vec!["exit 1".to_string(), "echo should-not-run".to_string()];
        let err = run_gate("test", &commands, dir.path()).unwrap_err();
        assert!(err.contains("test gate failed"));
    }

    #[test]
    fn test_handle_finish_check_gate_failure() {
        let gates = QualityGatesConfig {
            check: vec!["exit 1".to_string()],
            test: vec![],
            lint: vec![],
            format: vec![],
        };
        // This tests the gate failure path (we can't test the full flow without git/bd)
        let result = handle_finish("test-bead", "test message", &[], &gates, None);
        assert!(!result.success);
        assert!(result.message.contains("check gate failed"));
    }

    #[test]
    fn test_handle_finish_test_gate_failure() {
        let gates = QualityGatesConfig {
            check: vec!["true".to_string()],
            test: vec!["exit 1".to_string()],
            lint: vec![],
            format: vec![],
        };
        let result = handle_finish("test-bead", "test message", &[], &gates, None);
        assert!(!result.success);
        assert!(result.message.contains("test gate failed"));
    }

    #[test]
    fn test_handle_finish_lint_gate_failure() {
        let gates = QualityGatesConfig {
            check: vec!["true".to_string()],
            test: vec!["true".to_string()],
            lint: vec!["exit 1".to_string()],
            format: vec![],
        };
        let result = handle_finish("test-bead", "test message", &[], &gates, None);
        assert!(!result.success);
        assert!(result.message.contains("lint gate failed"));
    }

    #[test]
    fn test_handle_finish_format_gate_failure() {
        let gates = QualityGatesConfig {
            check: vec!["true".to_string()],
            test: vec!["true".to_string()],
            lint: vec!["true".to_string()],
            format: vec!["exit 1".to_string()],
        };
        let result = handle_finish("test-bead", "test message", &[], &gates, None);
        assert!(!result.success);
        assert!(result.message.contains("format gate failed"));
    }

    #[test]
    fn test_handle_finish_marks_bead_completed() {
        // Create a temp DB with a bead_metrics entry
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("blacksmith.db");
        let conn = db::open_or_create(&db_path).unwrap();
        db::upsert_bead_metrics(&conn, "test-bead", 1, 60.0, 10, Some(100), None, None).unwrap();

        // Verify completed_at is initially NULL
        let bm = db::get_bead_metrics(&conn, "test-bead").unwrap().unwrap();
        assert!(
            bm.completed_at.is_none(),
            "completed_at should start as None"
        );
        drop(conn);

        // Call handle_finish with all gates passing but it will fail at git stage
        // (since we're not in a git repo in the temp dir). The DB marking happens
        // after bd close (step 4), so we can't reach it through handle_finish in a
        // unit test without a full git+bd setup. Instead, test the DB marking path
        // directly to verify the integration logic works.
        let conn = db::open_or_create(&db_path).unwrap();
        let result = db::mark_bead_completed(&conn, "test-bead").unwrap();
        assert!(result, "should return true when marking bead completed");

        let bm = db::get_bead_metrics(&conn, "test-bead").unwrap().unwrap();
        assert!(bm.completed_at.is_some(), "completed_at should now be set");
        assert!(
            bm.completed_at.unwrap().contains('T'),
            "should be an ISO timestamp"
        );
    }
}
