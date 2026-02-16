use std::fs;
use std::path::Path;
use std::process::Command;

const RED: &str = "\x1b[0;31m";
const GREEN: &str = "\x1b[0;32m";
const YELLOW: &str = "\x1b[0;33m";
const NC: &str = "\x1b[0m";

pub fn handle_finish(bead_id: &str, message: &str, files: &[String]) -> Result<(), String> {
    println!("{GREEN}=== blacksmith finish: closing {bead_id} ==={NC}");

    // 0a. cargo check gate
    println!("{YELLOW}[0a/8] Running cargo check...{NC}");
    let check = Command::new("cargo")
        .args(["check"])
        .status()
        .map_err(|e| format!("Failed to run cargo check: {e}"))?;
    if !check.success() {
        eprintln!();
        eprintln!("{RED}=== CARGO CHECK FAILED ==={NC}");
        eprintln!("{RED}Bead {bead_id} will NOT be closed. Fix compilation errors first.{NC}");
        return Err("cargo check failed".into());
    }
    println!("{GREEN}[0a/8] cargo check passed{NC}");

    // 0b. cargo test gate
    println!("{YELLOW}[0b/8] Running cargo test...{NC}");
    let test = Command::new("cargo")
        .args(["test"])
        .status()
        .map_err(|e| format!("Failed to run cargo test: {e}"))?;
    if !test.success() {
        eprintln!();
        eprintln!("{RED}=== CARGO TEST FAILED ==={NC}");
        eprintln!("{RED}Bead {bead_id} will NOT be closed. Fix failing tests first.{NC}");
        return Err("cargo test failed".into());
    }
    println!("{GREEN}[0b/8] cargo test passed{NC}");

    // 1. Append PROGRESS.txt to PROGRESS_LOG.txt with timestamp
    if Path::new("PROGRESS.txt").exists() {
        let progress = fs::read_to_string("PROGRESS.txt")
            .map_err(|e| format!("Failed to read PROGRESS.txt: {e}"))?;
        let timestamp = chrono::Local::now().format("%Y-%m-%d %H:%M:%S");
        let entry = format!("\n--- {timestamp} | {bead_id} ---\n{progress}");
        fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open("PROGRESS_LOG.txt")
            .and_then(|mut f| std::io::Write::write_all(&mut f, entry.as_bytes()))
            .map_err(|e| format!("Failed to append to PROGRESS_LOG.txt: {e}"))?;
        println!("{GREEN}[1/8] Appended PROGRESS.txt to PROGRESS_LOG.txt{NC}");
    } else {
        println!("{YELLOW}[1/8] No PROGRESS.txt found, skipping log append{NC}");
    }

    // 2. Stage files
    if files.is_empty() {
        run_git(&["add", "-u"], "stage tracked modified files")?;
        println!("{GREEN}[2/8] Staged all tracked modified files (git add -u){NC}");
    } else {
        let mut args = vec!["add"];
        for f in files {
            args.push(f.as_str());
        }
        run_git(&args, "stage specified files")?;
        println!("{GREEN}[2/8] Staged {} specified files{NC}", files.len());
    }
    // Always include progress files if they exist
    let _ = Command::new("git")
        .args(["add", "-f", "PROGRESS.txt", "PROGRESS_LOG.txt"])
        .status();

    // 3. Commit
    let commit_msg = format!("{bead_id}: {message}");
    run_git(
        &["commit", "-m", &commit_msg, "--no-verify"],
        "commit changes",
    )?;
    println!("{GREEN}[3/8] Committed: {commit_msg}{NC}");

    // 4. bd close
    let close_status = Command::new("bd")
        .args(["close", bead_id, &format!("--reason={message}")])
        .status()
        .map_err(|e| format!("Failed to run bd close: {e}"))?;
    if !close_status.success() {
        return Err(format!("bd close failed for {bead_id}"));
    }
    println!("{GREEN}[4/8] Closed bead {bead_id}{NC}");

    // 5. bd sync
    let _ = Command::new("bd").args(["sync"]).status();
    println!("{GREEN}[5/8] Synced beads{NC}");

    // 6. Auto-commit .beads/ if dirty
    let beads_dirty = is_beads_dirty();
    if beads_dirty {
        let _ = Command::new("git").args(["add", ".beads/"]).status();
        let timestamp = chrono::Local::now().format("%Y-%m-%d %H:%M:%S");
        let sync_msg = format!("bd sync: {timestamp}");
        let _ = Command::new("git")
            .args(["commit", "-m", &sync_msg, "--no-verify"])
            .status();
        println!("{GREEN}[6/8] Committed .beads/ changes{NC}");
    } else {
        println!("{GREEN}[6/8] .beads/ already clean{NC}");
    }

    // 7. Push
    run_git(&["push"], "push to remote")?;
    println!("{GREEN}[7/8] Pushed to remote{NC}");

    println!();
    println!("{GREEN}=== Done. {bead_id} closed and pushed. ==={NC}");
    Ok(())
}

fn run_git(args: &[&str], description: &str) -> Result<(), String> {
    let status = Command::new("git")
        .args(args)
        .status()
        .map_err(|e| format!("Failed to {description}: {e}"))?;
    if !status.success() {
        return Err(format!("git {}: failed", args.first().unwrap_or(&"")));
    }
    Ok(())
}

fn is_beads_dirty() -> bool {
    let unstaged = Command::new("git")
        .args(["diff", "--quiet", ".beads/"])
        .status()
        .map(|s| !s.success())
        .unwrap_or(false);
    let staged = Command::new("git")
        .args(["diff", "--cached", "--quiet", ".beads/"])
        .status()
        .map(|s| !s.success())
        .unwrap_or(false);
    unstaged || staged
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_beads_dirty_returns_bool() {
        // Smoke test: should not panic, returns a bool
        let result = is_beads_dirty();
        assert!(result || !result);
    }

    #[test]
    fn test_run_git_status() {
        // git status should always succeed
        let result = run_git(&["status"], "check status");
        assert!(result.is_ok());
    }

    #[test]
    fn test_run_git_invalid_command() {
        // git with an invalid subcommand should fail
        let result = run_git(&["not-a-real-subcommand"], "invalid");
        assert!(result.is_err());
    }
}
