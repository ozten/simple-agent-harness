/// Single session lifecycle: spawn agent subprocess, capture output to file,
/// report results (exit code, output bytes, duration).
use crate::config::{AgentConfig, PromptVia, SessionConfig};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Instant;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

/// Result of a completed session.
#[derive(Debug)]
pub struct SessionResult {
    /// Process exit code (None if killed by signal).
    pub exit_code: Option<i32>,
    /// Total bytes written to the output file.
    pub output_bytes: u64,
    /// Wall-clock duration of the session.
    pub duration: std::time::Duration,
    /// Path to the output JSONL file.
    #[allow(dead_code)]
    pub output_file: PathBuf,
    /// Child PID (for logging/diagnostics).
    pub pid: u32,
}

/// Errors that can occur during session execution.
#[derive(Debug)]
pub enum SessionError {
    /// Failed to create the output file.
    OutputFile {
        path: PathBuf,
        source: std::io::Error,
    },
    /// Failed to spawn the agent subprocess.
    Spawn { source: std::io::Error },
    /// Failed to read from child stdout/stderr.
    Io { source: std::io::Error },
}

impl std::fmt::Display for SessionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SessionError::OutputFile { path, source } => {
                write!(
                    f,
                    "failed to create output file {}: {}",
                    path.display(),
                    source
                )
            }
            SessionError::Spawn { source } => {
                write!(f, "failed to spawn agent subprocess: {}", source)
            }
            SessionError::Io { source } => {
                write!(f, "I/O error during session: {}", source)
            }
        }
    }
}

impl std::error::Error for SessionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            SessionError::OutputFile { source, .. } => Some(source),
            SessionError::Spawn { source } => Some(source),
            SessionError::Io { source } => Some(source),
        }
    }
}

/// Build the output file path: {output_dir}/{output_prefix}-{global_iteration}.jsonl
///
/// Deprecated: use `DataDir::session_file()` instead. Retained for backwards compatibility
/// with existing session tests.
#[allow(dead_code)]
pub fn output_file_path(session_config: &SessionConfig, global_iteration: u64) -> PathBuf {
    let filename = format!(
        "{}-{}.jsonl",
        session_config.output_prefix, global_iteration
    );
    session_config.output_dir.join(filename)
}

/// Build the command arguments, replacing `{prompt}` and `{prompt_file}` placeholders.
///
/// - `{prompt}` is replaced with the inline prompt text (used with `prompt_via = "arg"`)
/// - `{prompt_file}` is replaced with the path to a temp file containing the prompt
///   (used with `prompt_via = "file"`)
fn build_args(agent_config: &AgentConfig, prompt: &str, prompt_file: Option<&Path>) -> Vec<String> {
    agent_config
        .args
        .iter()
        .map(|arg| {
            let mut result = arg.replace("{prompt}", prompt);
            if let Some(pf) = prompt_file {
                result = result.replace("{prompt_file}", &pf.display().to_string());
            }
            result
        })
        .collect()
}

/// Spawn the agent subprocess, capture stdout+stderr to a file, and return the result.
///
/// The subprocess is spawned in its own process group (via `process_group(0)`)
/// so the watchdog can later kill the entire group if needed.
///
/// Prompt delivery depends on `agent_config.prompt_via`:
/// - `Arg`: substitute `{prompt}` in args (default, existing behavior)
/// - `Stdin`: write prompt to the agent's stdin
/// - `File`: write prompt to a temp file, substitute `{prompt_file}` in args
pub async fn run_session(
    agent_config: &AgentConfig,
    output_path: &Path,
    prompt: &str,
) -> Result<SessionResult, SessionError> {
    // Create/truncate the output file
    let output_file = std::fs::File::create(output_path).map_err(|e| SessionError::OutputFile {
        path: output_path.to_path_buf(),
        source: e,
    })?;
    // We need a second handle for stderr since File doesn't impl Clone
    let output_file_stderr = output_file
        .try_clone()
        .map_err(|e| SessionError::OutputFile {
            path: output_path.to_path_buf(),
            source: e,
        })?;

    // For file mode: write prompt to a temp file
    let prompt_file = if agent_config.prompt_via == PromptVia::File {
        let tmp = tempfile::NamedTempFile::new().map_err(|e| SessionError::Io { source: e })?;
        std::fs::write(tmp.path(), prompt).map_err(|e| SessionError::Io { source: e })?;
        Some(tmp)
    } else {
        None
    };

    let args = build_args(agent_config, prompt, prompt_file.as_ref().map(|f| f.path()));

    // For stdin mode, don't pipe stdin from null
    let stdin_mode = if agent_config.prompt_via == PromptVia::Stdin {
        Stdio::piped()
    } else {
        Stdio::null()
    };

    tracing::info!(
        command = %agent_config.command,
        args = ?args,
        prompt_via = %agent_config.prompt_via,
        output = %output_path.display(),
        "spawning agent session"
    );

    let start = Instant::now();

    let mut child = Command::new(&agent_config.command)
        .args(&args)
        .stdin(stdin_mode)
        .stdout(Stdio::from(output_file))
        .stderr(Stdio::from(output_file_stderr))
        .process_group(0) // New process group for clean kill
        .spawn()
        .map_err(|e| SessionError::Spawn { source: e })?;

    // For stdin mode: write the prompt to the child's stdin, then close it
    if agent_config.prompt_via == PromptVia::Stdin {
        if let Some(mut stdin) = child.stdin.take() {
            stdin
                .write_all(prompt.as_bytes())
                .await
                .map_err(|e| SessionError::Io { source: e })?;
            // Dropping stdin closes the pipe, signaling EOF to the child
        }
    }

    let pid = child.id().unwrap_or(0);
    tracing::info!(pid, "agent subprocess started");

    // Wait for the child to exit
    let status = child
        .wait()
        .await
        .map_err(|e| SessionError::Io { source: e })?;

    let duration = start.elapsed();

    // Clean up temp file (NamedTempFile auto-deletes on drop, but be explicit)
    drop(prompt_file);

    // Read the output file size
    let output_bytes = std::fs::metadata(output_path).map(|m| m.len()).unwrap_or(0);

    let exit_code = status.code();
    tracing::info!(
        exit_code = ?exit_code,
        output_bytes,
        duration_secs = duration.as_secs(),
        "agent session completed"
    );

    Ok(SessionResult {
        exit_code,
        output_bytes,
        duration,
        output_file: output_path.to_path_buf(),
        pid,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AgentConfig;

    #[test]
    fn test_output_file_path() {
        let config = SessionConfig {
            output_dir: PathBuf::from("/tmp/output"),
            output_prefix: "claude-iteration".to_string(),
            ..Default::default()
        };
        let path = output_file_path(&config, 42);
        assert_eq!(path, PathBuf::from("/tmp/output/claude-iteration-42.jsonl"));
    }

    #[test]
    fn test_output_file_path_default_dir() {
        let config = SessionConfig::default();
        let path = output_file_path(&config, 0);
        assert_eq!(path, PathBuf::from("./claude-iteration-0.jsonl"));
    }

    #[test]
    fn test_build_args_replaces_prompt_placeholder() {
        let agent = AgentConfig {
            command: "claude".to_string(),
            args: vec![
                "-p".to_string(),
                "{prompt}".to_string(),
                "--verbose".to_string(),
            ],
            ..Default::default()
        };
        let args = build_args(&agent, "hello world", None);
        assert_eq!(args, vec!["-p", "hello world", "--verbose"]);
    }

    #[test]
    fn test_build_args_no_placeholder() {
        let agent = AgentConfig {
            command: "echo".to_string(),
            args: vec!["hello".to_string()],
            ..Default::default()
        };
        let args = build_args(&agent, "anything", None);
        assert_eq!(args, vec!["hello"]);
    }

    #[test]
    fn test_build_args_multiple_placeholders() {
        let agent = AgentConfig {
            command: "test".to_string(),
            args: vec![
                "{prompt}".to_string(),
                "mid".to_string(),
                "{prompt}".to_string(),
            ],
            ..Default::default()
        };
        let args = build_args(&agent, "X", None);
        assert_eq!(args, vec!["X", "mid", "X"]);
    }

    #[test]
    fn test_build_args_prompt_file_placeholder() {
        let agent = AgentConfig {
            command: "aider".to_string(),
            args: vec!["--message-file".to_string(), "{prompt_file}".to_string()],
            ..Default::default()
        };
        let args = build_args(&agent, "unused", Some(Path::new("/tmp/prompt.txt")));
        assert_eq!(args, vec!["--message-file", "/tmp/prompt.txt"]);
    }

    #[tokio::test]
    async fn test_run_session_echo_command() {
        let dir = tempfile::tempdir().unwrap();
        let output_path = dir.path().join("test-output.jsonl");

        let agent = AgentConfig {
            command: "echo".to_string(),
            args: vec!["hello".to_string(), "{prompt}".to_string()],
            ..Default::default()
        };

        let result = run_session(&agent, &output_path, "world").await.unwrap();

        assert_eq!(result.exit_code, Some(0));
        assert!(result.output_bytes > 0);
        assert!(result.output_file == output_path);
        assert!(result.pid > 0);

        // Check file contents
        let contents = std::fs::read_to_string(&output_path).unwrap();
        assert_eq!(contents.trim(), "hello world");
    }

    #[tokio::test]
    async fn test_run_session_captures_stderr() {
        let dir = tempfile::tempdir().unwrap();
        let output_path = dir.path().join("stderr-test.jsonl");

        let agent = AgentConfig {
            command: "sh".to_string(),
            args: vec![
                "-c".to_string(),
                "echo stdout-line; echo stderr-line >&2".to_string(),
            ],
            ..Default::default()
        };

        let result = run_session(&agent, &output_path, "unused").await.unwrap();

        assert_eq!(result.exit_code, Some(0));
        let contents = std::fs::read_to_string(&output_path).unwrap();
        assert!(contents.contains("stdout-line"));
        assert!(contents.contains("stderr-line"));
    }

    #[tokio::test]
    async fn test_run_session_nonzero_exit() {
        let dir = tempfile::tempdir().unwrap();
        let output_path = dir.path().join("fail-test.jsonl");

        let agent = AgentConfig {
            command: "sh".to_string(),
            args: vec!["-c".to_string(), "exit 42".to_string()],
            ..Default::default()
        };

        let result = run_session(&agent, &output_path, "unused").await.unwrap();
        assert_eq!(result.exit_code, Some(42));
    }

    #[tokio::test]
    async fn test_run_session_spawn_failure() {
        let dir = tempfile::tempdir().unwrap();
        let output_path = dir.path().join("spawn-fail.jsonl");

        let agent = AgentConfig {
            command: "nonexistent-binary-xyz".to_string(),
            args: vec![],
            ..Default::default()
        };

        let err = run_session(&agent, &output_path, "unused")
            .await
            .unwrap_err();
        assert!(matches!(err, SessionError::Spawn { .. }));
        assert!(err.to_string().contains("failed to spawn"));
    }

    #[tokio::test]
    async fn test_run_session_reports_correct_byte_count() {
        let dir = tempfile::tempdir().unwrap();
        let output_path = dir.path().join("bytes-test.jsonl");

        // Write exactly 5 bytes ("ABCDE") + newline from echo = 6 bytes
        let agent = AgentConfig {
            command: "printf".to_string(),
            args: vec!["ABCDE".to_string()],
            ..Default::default()
        };

        let result = run_session(&agent, &output_path, "unused").await.unwrap();
        assert_eq!(result.output_bytes, 5);
    }

    #[tokio::test]
    async fn test_run_session_duration_is_reasonable() {
        let dir = tempfile::tempdir().unwrap();
        let output_path = dir.path().join("duration-test.jsonl");

        let agent = AgentConfig {
            command: "sleep".to_string(),
            args: vec!["0.1".to_string()],
            ..Default::default()
        };

        let result = run_session(&agent, &output_path, "unused").await.unwrap();
        // Should take at least ~100ms
        assert!(result.duration.as_millis() >= 80);
        // But not more than a few seconds
        assert!(result.duration.as_secs() < 5);
    }

    #[tokio::test]
    async fn test_run_session_bad_output_path() {
        let agent = AgentConfig {
            command: "echo".to_string(),
            args: vec!["hello".to_string()],
            ..Default::default()
        };

        let err = run_session(
            &agent,
            Path::new("/nonexistent-dir/impossible/output.jsonl"),
            "unused",
        )
        .await
        .unwrap_err();
        assert!(matches!(err, SessionError::OutputFile { .. }));
    }

    #[tokio::test]
    async fn test_run_session_prompt_via_stdin() {
        let dir = tempfile::tempdir().unwrap();
        let output_path = dir.path().join("stdin-test.jsonl");

        let agent = AgentConfig {
            command: "cat".to_string(),
            args: vec![],
            prompt_via: PromptVia::Stdin,
            ..Default::default()
        };

        let result = run_session(&agent, &output_path, "hello from stdin")
            .await
            .unwrap();

        assert_eq!(result.exit_code, Some(0));
        let contents = std::fs::read_to_string(&output_path).unwrap();
        assert_eq!(contents, "hello from stdin");
    }

    #[tokio::test]
    async fn test_run_session_prompt_via_file() {
        let dir = tempfile::tempdir().unwrap();
        let output_path = dir.path().join("file-test.jsonl");

        let agent = AgentConfig {
            command: "sh".to_string(),
            args: vec!["-c".to_string(), "cat {prompt_file}".to_string()],
            prompt_via: PromptVia::File,
            ..Default::default()
        };

        let result = run_session(&agent, &output_path, "hello from file")
            .await
            .unwrap();

        assert_eq!(result.exit_code, Some(0));
        let contents = std::fs::read_to_string(&output_path).unwrap();
        assert_eq!(contents, "hello from file");
    }
}
