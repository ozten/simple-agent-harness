use std::path::Path;

use crate::config::{command_in_path, HarnessConfig};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    Fatal,
    Warn,
}

#[derive(Debug, Clone)]
pub struct CheckResult {
    pub name: &'static str,
    pub severity: Severity,
    pub message: String,
}

#[derive(Debug, Default)]
pub struct PreflightReport {
    pub checks: Vec<CheckResult>,
}

impl PreflightReport {
    pub fn has_fatal(&self) -> bool {
        self.checks.iter().any(|c| c.severity == Severity::Fatal)
    }

    pub fn is_clean(&self) -> bool {
        self.checks.is_empty()
    }

    pub fn warnings(&self) -> Vec<&CheckResult> {
        self.checks
            .iter()
            .filter(|c| c.severity == Severity::Warn)
            .collect()
    }

    pub fn fatals(&self) -> Vec<&CheckResult> {
        self.checks
            .iter()
            .filter(|c| c.severity == Severity::Fatal)
            .collect()
    }
}

pub fn run_preflight(project_root: &Path, config: &HarnessConfig) -> PreflightReport {
    let mut report = PreflightReport::default();

    // Check: git_repo — .git/ exists in project_root
    if !project_root.join(".git").exists() {
        report.checks.push(CheckResult {
            name: "git_repo",
            severity: Severity::Fatal,
            message: format!("No .git directory found in '{}'", project_root.display()),
        });
    }

    // Check: git_remote — git remote -v returns non-empty output
    {
        let output = std::process::Command::new("git")
            .args(["remote", "-v"])
            .current_dir(project_root)
            .output();
        match output {
            Ok(o) if o.stdout.is_empty() => {
                report.checks.push(CheckResult {
                    name: "git_remote",
                    severity: Severity::Warn,
                    message: "No git remote configured".to_string(),
                });
            }
            Err(e) => {
                report.checks.push(CheckResult {
                    name: "git_remote",
                    severity: Severity::Warn,
                    message: format!("Could not run 'git remote -v': {e}"),
                });
            }
            _ => {}
        }
    }

    // Check: agent_binary — resolved coding command found in PATH
    {
        let coding = config.agent.resolved_coding();
        if !coding.command.is_empty() && !command_in_path(&coding.command) {
            report.checks.push(CheckResult {
                name: "agent_binary",
                severity: Severity::Fatal,
                message: format!("Agent binary '{}' not found in PATH", coding.command),
            });
        }
    }

    // Check: bd_cli — bd found in PATH
    if !command_in_path("bd") {
        report.checks.push(CheckResult {
            name: "bd_cli",
            severity: Severity::Warn,
            message: "bd CLI not found in PATH".to_string(),
        });
    }

    // Check: claude_verbose_flag — if args contain --output-format + stream-json, also check --verbose
    {
        let coding = config.agent.resolved_coding();
        let args = &coding.args;
        let has_stream_json = args
            .windows(2)
            .any(|w| w[0] == "--output-format" && w[1] == "stream-json");
        if has_stream_json && !args.iter().any(|a| a == "--verbose") {
            report.checks.push(CheckResult {
                name: "claude_verbose_flag",
                severity: Severity::Warn,
                message: "Agent uses --output-format stream-json but --verbose is missing"
                    .to_string(),
            });
        }
    }

    // Check: prompt_file — config.session.prompt_file exists
    if !config.session.prompt_file.exists() {
        report.checks.push(CheckResult {
            name: "prompt_file",
            severity: Severity::Warn,
            message: format!(
                "Prompt file '{}' does not exist",
                config.session.prompt_file.display()
            ),
        });
    }

    report
}

pub fn print_report(report: &PreflightReport) {
    if report.is_clean() {
        eprintln!("Preflight: all checks passed");
        return;
    }
    for check in &report.checks {
        let prefix = match check.severity {
            Severity::Fatal => "[ERROR]",
            Severity::Warn => "[WARN ]",
        };
        eprintln!("{prefix} {}: {}", check.name, check.message);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tempfile::tempdir;

    fn default_config() -> HarnessConfig {
        HarnessConfig::default()
    }

    #[test]
    fn test_no_git_repo() {
        let dir = tempdir().unwrap();
        let config = default_config();
        let report = run_preflight(dir.path(), &config);
        assert!(report
            .checks
            .iter()
            .any(|c| c.name == "git_repo" && c.severity == Severity::Fatal));
    }

    #[test]
    fn test_git_repo_present() {
        let dir = tempdir().unwrap();
        std::process::Command::new("git")
            .args(["init"])
            .current_dir(dir.path())
            .output()
            .unwrap();
        let config = default_config();
        let report = run_preflight(dir.path(), &config);
        assert!(!report.checks.iter().any(|c| c.name == "git_repo"));
    }

    #[test]
    fn test_no_git_remote() {
        let dir = tempdir().unwrap();
        std::process::Command::new("git")
            .args(["init"])
            .current_dir(dir.path())
            .output()
            .unwrap();
        let config = default_config();
        let report = run_preflight(dir.path(), &config);
        assert!(report
            .checks
            .iter()
            .any(|c| c.name == "git_remote" && c.severity == Severity::Warn));
    }

    #[test]
    fn test_stream_json_without_verbose() {
        let dir = tempdir().unwrap();
        std::process::Command::new("git")
            .args(["init"])
            .current_dir(dir.path())
            .output()
            .unwrap();
        let mut config = default_config();
        config.agent.args = vec!["--output-format".to_string(), "stream-json".to_string()];
        // Ensure prompt_file exists so we don't get that check
        let prompt = dir.path().join("PROMPT.md");
        std::fs::write(&prompt, "test").unwrap();
        config.session.prompt_file = prompt;

        let report = run_preflight(dir.path(), &config);
        assert!(report
            .checks
            .iter()
            .any(|c| c.name == "claude_verbose_flag" && c.severity == Severity::Warn));
    }

    #[test]
    fn test_stream_json_with_verbose() {
        let dir = tempdir().unwrap();
        std::process::Command::new("git")
            .args(["init"])
            .current_dir(dir.path())
            .output()
            .unwrap();
        let mut config = default_config();
        config.agent.args = vec![
            "--output-format".to_string(),
            "stream-json".to_string(),
            "--verbose".to_string(),
        ];
        let prompt = dir.path().join("PROMPT.md");
        std::fs::write(&prompt, "test").unwrap();
        config.session.prompt_file = prompt;

        let report = run_preflight(dir.path(), &config);
        assert!(!report
            .checks
            .iter()
            .any(|c| c.name == "claude_verbose_flag"));
    }

    #[test]
    fn test_missing_prompt_file() {
        let dir = tempdir().unwrap();
        std::process::Command::new("git")
            .args(["init"])
            .current_dir(dir.path())
            .output()
            .unwrap();
        let mut config = default_config();
        config.session.prompt_file = PathBuf::from("/nonexistent/prompt.md");

        let report = run_preflight(dir.path(), &config);
        assert!(report
            .checks
            .iter()
            .any(|c| c.name == "prompt_file" && c.severity == Severity::Warn));
    }

    #[test]
    fn test_has_fatal() {
        let mut report = PreflightReport::default();
        report.checks.push(CheckResult {
            name: "test",
            severity: Severity::Fatal,
            message: "fatal error".to_string(),
        });
        assert!(report.has_fatal());
        assert!(!report.is_clean());
        assert_eq!(report.fatals().len(), 1);
        assert_eq!(report.warnings().len(), 0);
    }

    #[test]
    fn test_is_clean() {
        let report = PreflightReport::default();
        assert!(report.is_clean());
        assert!(!report.has_fatal());
        assert_eq!(report.fatals().len(), 0);
        assert_eq!(report.warnings().len(), 0);
    }
}
