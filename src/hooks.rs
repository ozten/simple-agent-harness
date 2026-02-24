/// Pre/post-session hook execution.
///
/// Hooks are shell commands executed synchronously before or after each session.
/// They receive environment variables with session context (iteration counts,
/// output file paths, exit codes, etc.).
use std::collections::HashMap;
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};

/// Executes pre- and post-session shell hooks with appropriate environment variables.
pub struct HookRunner {
    pre_session: Vec<String>,
    post_session: Vec<String>,
    post_integration: Vec<String>,
    post_integration_timeout_secs: u64,
}

/// Environment variables available to hooks.
pub struct HookEnv {
    vars: HashMap<String, String>,
}

impl HookEnv {
    /// Create environment for pre-session hooks.
    pub fn pre_session(iteration: u32, global_iteration: u64, prompt_file: &str) -> Self {
        let mut vars = HashMap::new();
        vars.insert("HARNESS_ITERATION".to_string(), iteration.to_string());
        vars.insert(
            "HARNESS_GLOBAL_ITERATION".to_string(),
            global_iteration.to_string(),
        );
        vars.insert("HARNESS_PROMPT_FILE".to_string(), prompt_file.to_string());
        Self { vars }
    }

    /// Create environment for post-session hooks.
    #[allow(clippy::too_many_arguments)]
    pub fn post_session(
        iteration: u32,
        global_iteration: u64,
        prompt_file: &str,
        output_file: &str,
        exit_code: Option<i32>,
        output_bytes: u64,
        session_duration_secs: u64,
        committed: bool,
    ) -> Self {
        let mut vars = HashMap::new();
        vars.insert("HARNESS_ITERATION".to_string(), iteration.to_string());
        vars.insert(
            "HARNESS_GLOBAL_ITERATION".to_string(),
            global_iteration.to_string(),
        );
        vars.insert("HARNESS_PROMPT_FILE".to_string(), prompt_file.to_string());
        vars.insert("HARNESS_OUTPUT_FILE".to_string(), output_file.to_string());
        vars.insert(
            "HARNESS_EXIT_CODE".to_string(),
            exit_code.map_or("".to_string(), |c| c.to_string()),
        );
        vars.insert("HARNESS_OUTPUT_BYTES".to_string(), output_bytes.to_string());
        vars.insert(
            "HARNESS_SESSION_DURATION".to_string(),
            session_duration_secs.to_string(),
        );
        vars.insert("HARNESS_COMMITTED".to_string(), committed.to_string());
        Self { vars }
    }

    /// Create environment for post-integration hooks.
    pub fn post_integration(bead_id: &str, main_commit: &str) -> Self {
        let mut vars = HashMap::new();
        vars.insert(
            "BLACKSMITH_INTEGRATED_BEAD".to_string(),
            bead_id.to_string(),
        );
        vars.insert(
            "BLACKSMITH_MAIN_COMMIT".to_string(),
            main_commit.to_string(),
        );
        Self { vars }
    }
}

/// Error from hook execution.
#[derive(Debug)]
pub struct HookError {
    pub command: String,
    pub exit_code: Option<i32>,
    pub kind: HookErrorKind,
}

#[derive(Debug)]
pub enum HookErrorKind {
    /// Hook process exited with non-zero status.
    NonZeroExit,
    /// Failed to spawn the hook process.
    SpawnFailed(std::io::Error),
    /// Hook process timed out and was killed.
    Timeout { timeout_secs: u64 },
}

impl std::fmt::Display for HookError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.kind {
            HookErrorKind::NonZeroExit => write!(
                f,
                "hook command failed with exit code {:?}: {}",
                self.exit_code, self.command
            ),
            HookErrorKind::SpawnFailed(e) => {
                write!(f, "failed to spawn hook command '{}': {}", self.command, e)
            }
            HookErrorKind::Timeout { timeout_secs } => write!(
                f,
                "hook command timed out after {}s and was killed: {}",
                timeout_secs, self.command
            ),
        }
    }
}

impl std::error::Error for HookError {}

impl HookRunner {
    /// Create a new HookRunner from config hook lists.
    pub fn new(pre_session: Vec<String>, post_session: Vec<String>) -> Self {
        Self {
            pre_session,
            post_session,
            post_integration: Vec::new(),
            post_integration_timeout_secs: 60,
        }
    }

    /// Configure post-integration hooks and timeout.
    pub fn with_post_integration(
        mut self,
        post_integration: Vec<String>,
        post_integration_timeout_secs: u64,
    ) -> Self {
        self.post_integration = post_integration;
        self.post_integration_timeout_secs = post_integration_timeout_secs;
        self
    }

    /// Run all pre-session hooks. Returns Err on first failure (non-zero exit or spawn error).
    /// The caller should skip the iteration on error.
    pub fn run_pre_session(&self, env: &HookEnv) -> Result<(), HookError> {
        for cmd in &self.pre_session {
            tracing::info!(hook = "pre_session", command = %cmd, "running hook");
            run_hook_command(cmd, env)?;
        }
        Ok(())
    }

    /// Run all post-session hooks. Logs errors but does not return them —
    /// post-session hook failures should not affect the iteration.
    pub fn run_post_session(&self, env: &HookEnv) {
        for cmd in &self.post_session {
            tracing::info!(hook = "post_session", command = %cmd, "running hook");
            if let Err(e) = run_hook_command(cmd, env) {
                tracing::error!(hook = "post_session", error = %e, "post-session hook failed");
            }
        }
    }

    /// Run all post-integration hooks. Failures and timeouts are warnings and non-fatal.
    pub fn run_post_integration(&self, env: &HookEnv) {
        for cmd in &self.post_integration {
            tracing::info!(hook = "post_integration", command = %cmd, "running hook");
            if let Err(e) =
                run_hook_command_with_timeout(cmd, env, self.post_integration_timeout_secs)
            {
                tracing::warn!(
                    hook = "post_integration",
                    error = %e,
                    "post-integration hook failed"
                );
            }
        }
    }
}

/// Execute a single shell command with the given environment variables.
fn run_hook_command(command: &str, env: &HookEnv) -> Result<(), HookError> {
    let result = Command::new("sh")
        .arg("-c")
        .arg(command)
        .envs(&env.vars)
        .status();

    match result {
        Ok(status) => {
            if status.success() {
                tracing::debug!(command = %command, "hook completed successfully");
                Ok(())
            } else {
                let exit_code = status.code();
                tracing::error!(command = %command, exit_code = ?exit_code, "hook exited with error");
                Err(HookError {
                    command: command.to_string(),
                    exit_code,
                    kind: HookErrorKind::NonZeroExit,
                })
            }
        }
        Err(e) => Err(HookError {
            command: command.to_string(),
            exit_code: None,
            kind: HookErrorKind::SpawnFailed(e),
        }),
    }
}

fn run_hook_command_with_timeout(
    command: &str,
    env: &HookEnv,
    timeout_secs: u64,
) -> Result<(), HookError> {
    let mut child = Command::new("sh")
        .arg("-c")
        .arg(command)
        .envs(&env.vars)
        .spawn()
        .map_err(|e| HookError {
            command: command.to_string(),
            exit_code: None,
            kind: HookErrorKind::SpawnFailed(e),
        })?;

    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                if status.success() {
                    tracing::debug!(command = %command, "hook completed successfully");
                    return Ok(());
                }
                let exit_code = status.code();
                tracing::error!(command = %command, exit_code = ?exit_code, "hook exited with error");
                return Err(HookError {
                    command: command.to_string(),
                    exit_code,
                    kind: HookErrorKind::NonZeroExit,
                });
            }
            Ok(None) => {
                if Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(HookError {
                        command: command.to_string(),
                        exit_code: None,
                        kind: HookErrorKind::Timeout { timeout_secs },
                    });
                }
                thread::sleep(Duration::from_millis(25));
            }
            Err(e) => {
                return Err(HookError {
                    command: command.to_string(),
                    exit_code: None,
                    kind: HookErrorKind::SpawnFailed(e),
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hook_env_pre_session() {
        let env = HookEnv::pre_session(5, 100, "/path/to/prompt.md");
        assert_eq!(env.vars["HARNESS_ITERATION"], "5");
        assert_eq!(env.vars["HARNESS_GLOBAL_ITERATION"], "100");
        assert_eq!(env.vars["HARNESS_PROMPT_FILE"], "/path/to/prompt.md");
        assert!(!env.vars.contains_key("HARNESS_OUTPUT_FILE"));
        assert!(!env.vars.contains_key("HARNESS_EXIT_CODE"));
    }

    #[test]
    fn test_hook_env_post_session() {
        let env = HookEnv::post_session(
            3,
            50,
            "/prompt.md",
            "/output.jsonl",
            Some(0),
            1024,
            60,
            true,
        );
        assert_eq!(env.vars["HARNESS_ITERATION"], "3");
        assert_eq!(env.vars["HARNESS_GLOBAL_ITERATION"], "50");
        assert_eq!(env.vars["HARNESS_PROMPT_FILE"], "/prompt.md");
        assert_eq!(env.vars["HARNESS_OUTPUT_FILE"], "/output.jsonl");
        assert_eq!(env.vars["HARNESS_EXIT_CODE"], "0");
        assert_eq!(env.vars["HARNESS_OUTPUT_BYTES"], "1024");
        assert_eq!(env.vars["HARNESS_SESSION_DURATION"], "60");
        assert_eq!(env.vars["HARNESS_COMMITTED"], "true");
    }

    #[test]
    fn test_hook_env_post_session_no_exit_code() {
        let env = HookEnv::post_session(0, 0, "", "", None, 0, 0, false);
        assert_eq!(env.vars["HARNESS_EXIT_CODE"], "");
        assert_eq!(env.vars["HARNESS_COMMITTED"], "false");
    }

    #[test]
    fn test_hook_env_post_integration() {
        let env = HookEnv::post_integration("bd-123", "abc123");
        assert_eq!(env.vars["BLACKSMITH_INTEGRATED_BEAD"], "bd-123");
        assert_eq!(env.vars["BLACKSMITH_MAIN_COMMIT"], "abc123");
        assert_eq!(env.vars.len(), 2);
    }

    #[test]
    fn test_pre_session_success() {
        let runner = HookRunner::new(vec!["true".to_string()], vec![]);
        let env = HookEnv::pre_session(0, 0, "prompt.md");
        assert!(runner.run_pre_session(&env).is_ok());
    }

    #[test]
    fn test_pre_session_failure() {
        let runner = HookRunner::new(vec!["false".to_string()], vec![]);
        let env = HookEnv::pre_session(0, 0, "prompt.md");
        let err = runner.run_pre_session(&env).unwrap_err();
        assert!(matches!(err.kind, HookErrorKind::NonZeroExit));
    }

    #[test]
    fn test_pre_session_stops_on_first_failure() {
        // First command succeeds, second fails — third should not run
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("marker");
        let runner = HookRunner::new(
            vec![
                "true".to_string(),
                "false".to_string(),
                format!("touch {}", marker.display()),
            ],
            vec![],
        );
        let env = HookEnv::pre_session(0, 0, "prompt.md");
        assert!(runner.run_pre_session(&env).is_err());
        assert!(!marker.exists(), "third hook should not have run");
    }

    #[test]
    fn test_pre_session_multiple_success() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("marker");
        let runner = HookRunner::new(
            vec!["true".to_string(), format!("touch {}", marker.display())],
            vec![],
        );
        let env = HookEnv::pre_session(0, 0, "prompt.md");
        assert!(runner.run_pre_session(&env).is_ok());
        assert!(marker.exists(), "second hook should have run");
    }

    #[test]
    fn test_post_session_logs_error_but_continues() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("marker");
        // First post hook fails, second should still run
        let runner = HookRunner::new(
            vec![],
            vec!["false".to_string(), format!("touch {}", marker.display())],
        );
        let env = HookEnv::post_session(0, 0, "", "", Some(0), 0, 0, false);
        runner.run_post_session(&env); // Should not panic
        assert!(
            marker.exists(),
            "second post hook should have run despite first failure"
        );
    }

    #[test]
    fn test_hook_receives_env_vars() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("env_output");
        let runner = HookRunner::new(
            vec![format!(
                "echo $HARNESS_ITERATION-$HARNESS_GLOBAL_ITERATION > {}",
                output.display()
            )],
            vec![],
        );
        let env = HookEnv::pre_session(7, 42, "prompt.md");
        runner.run_pre_session(&env).unwrap();
        let contents = std::fs::read_to_string(&output).unwrap();
        assert_eq!(contents.trim(), "7-42");
    }

    #[test]
    fn test_post_hook_receives_all_env_vars() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("env_output");
        let runner = HookRunner::new(
            vec![],
            vec![format!(
                "echo $HARNESS_OUTPUT_FILE:$HARNESS_EXIT_CODE:$HARNESS_OUTPUT_BYTES:$HARNESS_SESSION_DURATION:$HARNESS_COMMITTED > {}",
                output.display()
            )],
        );
        let env = HookEnv::post_session(0, 0, "p.md", "/out.jsonl", Some(42), 9999, 120, true);
        runner.run_post_session(&env);
        let contents = std::fs::read_to_string(&output).unwrap();
        assert_eq!(contents.trim(), "/out.jsonl:42:9999:120:true");
    }

    #[test]
    fn test_post_integration_hook_receives_env_vars() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("env_output");
        let runner = HookRunner::new(vec![], vec![]).with_post_integration(
            vec![format!(
                "echo $BLACKSMITH_INTEGRATED_BEAD:$BLACKSMITH_MAIN_COMMIT > {}",
                output.display()
            )],
            1,
        );
        let env = HookEnv::post_integration("simple-agent-harness-m7xy", "deadbeef");
        runner.run_post_integration(&env);
        let contents = std::fs::read_to_string(&output).unwrap();
        assert_eq!(contents.trim(), "simple-agent-harness-m7xy:deadbeef");
    }

    #[test]
    fn test_post_integration_timeout_kills_process_and_continues() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("marker");
        let runner = HookRunner::new(vec![], vec![]).with_post_integration(
            vec!["sleep 2".to_string(), format!("touch {}", marker.display())],
            1,
        );
        let env = HookEnv::post_integration("bd-1", "abc");
        runner.run_post_integration(&env);
        assert!(
            marker.exists(),
            "later post-integration hook should still run"
        );
    }

    #[test]
    fn test_hook_error_display() {
        let err = HookError {
            command: "bad-cmd".to_string(),
            exit_code: Some(1),
            kind: HookErrorKind::NonZeroExit,
        };
        assert!(err.to_string().contains("bad-cmd"));
        assert!(err.to_string().contains("exit code"));
    }
}
