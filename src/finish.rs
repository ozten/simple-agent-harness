//! `blacksmith finish` — quality gate and deliverable verification.
//!
//! Replaces `bd-finish.sh` with a compiled subcommand that runs configurable
//! quality gates and bead verification before integration is allowed.

use crate::config::QualityGatesConfig;
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
                let cmd = strip_verify_prose(cmd);
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

/// Strip trailing prose from a verify `Run:` line.
///
/// Agents sometimes write lines like `Run: cargo test -- --test-threads=1 — all tests pass`.
/// The em-dash (—) and everything after it is prose, not part of the command.
fn strip_verify_prose(cmd: &str) -> &str {
    cmd.split('\u{2014}') // em-dash
        .next()
        .unwrap_or(cmd)
        .trim()
}

/// Run finish verification:
/// 1. Quality gates (check, test)
/// 2. Deliverable verification
pub fn handle_finish(
    bead_id: &str,
    _commit_msg: &str,
    _files: &[String],
    gates_config: &QualityGatesConfig,
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

    // 0c. Deliverable verification
    eprintln!("[0c] Verifying bead deliverables...");
    if let Err(e) = verify_deliverables(bead_id, &working_dir) {
        eprintln!("\n=== DELIVERABLE VERIFICATION FAILED ===");
        eprintln!("Bead {bead_id} will NOT be closed.");
        return FinishResult {
            success: false,
            message: e,
        };
    }
    eprintln!("[0c] Deliverable verification passed\n");

    eprintln!("\n=== Done. {bead_id} passed quality gates and verification. ===");

    FinishResult {
        success: true,
        message: format!("Bead {bead_id} passed finish verification"),
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
    fn test_strip_verify_prose() {
        // Plain command — no change
        assert_eq!(
            strip_verify_prose(" cargo test --release "),
            "cargo test --release"
        );
        // Em-dash followed by prose
        assert_eq!(
            strip_verify_prose(" cargo test --release — all tests pass"),
            "cargo test --release"
        );
        // Double-dash flags should be preserved (not em-dash)
        assert_eq!(
            strip_verify_prose(" cargo test -- --test-threads=1 "),
            "cargo test -- --test-threads=1"
        );
        // Em-dash after double-dash flags
        assert_eq!(
            strip_verify_prose(" cargo test -- --test-threads=1 — should pass"),
            "cargo test -- --test-threads=1"
        );
        // Empty after stripping
        assert_eq!(strip_verify_prose(" — just prose"), "");
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
        let result = handle_finish("test-bead", "test message", &[], &gates);
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
        let result = handle_finish("test-bead", "test message", &[], &gates);
        assert!(!result.success);
        assert!(result.message.contains("test gate failed"));
    }
}
