/// Prompt assembly: read prompt file, run prepend_commands, inject brief, combine.
///
/// Supports [prompt] config section with `file` and `prepend_commands` fields.
/// Runs each prepend command, captures stdout, generates the brief from the
/// blacksmith database, and prepends non-empty outputs to the prompt content
/// separated by "\n---\n". Empty command output and empty briefs are silently skipped.
use crate::brief;
use crate::config::{MetricsTargetsConfig, PromptConfig};
use std::path::Path;
use std::process::Command;

/// Errors that can occur during prompt assembly.
#[derive(Debug)]
pub enum PromptError {
    /// Failed to read the prompt file.
    ReadFile {
        path: std::path::PathBuf,
        source: std::io::Error,
    },
    /// A prepend command failed to execute.
    CommandFailed {
        command: String,
        source: std::io::Error,
    },
}

impl std::fmt::Display for PromptError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PromptError::ReadFile { path, source } => {
                write!(
                    f,
                    "failed to read prompt file {}: {}",
                    path.display(),
                    source
                )
            }
            PromptError::CommandFailed { command, source } => {
                write!(
                    f,
                    "prepend command '{}' failed to execute: {}",
                    command, source
                )
            }
        }
    }
}

impl std::error::Error for PromptError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PromptError::ReadFile { source, .. } => Some(source),
            PromptError::CommandFailed { source, .. } => Some(source),
        }
    }
}

/// Assemble the final prompt from the prompt file, prepend commands, and brief.
///
/// The prompt file path is determined by:
/// - `prompt_config.file` if set
/// - `fallback_prompt_file` otherwise (from session config)
///
/// Assembly sequence:
/// 1. Run prepend_commands and collect non-empty outputs
/// 2. Generate brief from the metrics database (silently skipped if no DB)
/// 3. Read prompt file
/// 4. Join all non-empty parts with `\n---\n` separators
///
/// Commands that fail to spawn return an error; commands that run but exit
/// non-zero have their stdout captured normally (may be empty, thus skipped).
pub fn assemble(
    prompt_config: &PromptConfig,
    fallback_prompt_file: &Path,
    db_path: &Path,
    targets_config: Option<&MetricsTargetsConfig>,
    supported_metrics: Option<&[&str]>,
) -> Result<String, PromptError> {
    // Determine prompt file path
    let prompt_path = prompt_config
        .file
        .as_deref()
        .unwrap_or(fallback_prompt_file);

    // Read prompt file
    let prompt_content =
        std::fs::read_to_string(prompt_path).map_err(|e| PromptError::ReadFile {
            path: prompt_path.to_path_buf(),
            source: e,
        })?;

    // Collect all prefix parts (prepend commands + brief)
    let mut prefix_parts: Vec<String> = Vec::new();

    // 1. Run each prepend command and collect non-empty outputs
    for cmd in &prompt_config.prepend_commands {
        let output = run_prepend_command(cmd)?;
        let trimmed = output.trim();
        if !trimmed.is_empty() {
            prefix_parts.push(trimmed.to_string());
            tracing::debug!(command = %cmd, bytes = trimmed.len(), "prepend command produced output");
        } else {
            tracing::debug!(command = %cmd, "prepend command produced empty output, skipping");
        }
    }

    // 2. Generate brief from blacksmith.db (silently skipped if no DB or no improvements)
    match brief::generate_brief(db_path, targets_config, supported_metrics) {
        Ok(text) if !text.is_empty() => {
            tracing::debug!(bytes = text.len(), "brief injected into prompt");
            prefix_parts.push(text);
        }
        Ok(_) => {
            tracing::debug!("brief produced no output, skipping");
        }
        Err(e) => {
            tracing::warn!(error = %e, "brief generation failed, skipping");
        }
    }

    // 3. Build final prompt
    if prefix_parts.is_empty() {
        Ok(prompt_content)
    } else {
        prefix_parts.push(prompt_content);
        Ok(prefix_parts.join("\n---\n"))
    }
}

/// Run a single prepend command via `sh -c` and return its stdout.
fn run_prepend_command(cmd: &str) -> Result<String, PromptError> {
    let output = Command::new("sh")
        .arg("-c")
        .arg(cmd)
        .output()
        .map_err(|e| PromptError::CommandFailed {
            command: cmd.to_string(),
            source: e,
        })?;

    if !output.status.success() {
        tracing::warn!(
            command = %cmd,
            exit_code = ?output.status.code(),
            "prepend command exited with non-zero status"
        );
    }

    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PromptConfig;

    #[test]
    fn test_assemble_no_prepend_commands() {
        let dir = tempfile::tempdir().unwrap();
        let prompt_path = dir.path().join("prompt.md");
        std::fs::write(&prompt_path, "Hello, world!").unwrap();

        let config = PromptConfig {
            file: None,
            prepend_commands: vec![],
        };

        let result = assemble(&config, &prompt_path, &dir.path().join("blacksmith.db"), None, None).unwrap();
        assert_eq!(result, "Hello, world!");
    }

    #[test]
    fn test_assemble_with_prompt_file_override() {
        let dir = tempfile::tempdir().unwrap();
        let fallback = dir.path().join("fallback.md");
        let custom = dir.path().join("custom.md");
        std::fs::write(&fallback, "fallback content").unwrap();
        std::fs::write(&custom, "custom content").unwrap();

        let config = PromptConfig {
            file: Some(custom.clone()),
            prepend_commands: vec![],
        };

        let result = assemble(&config, &fallback, dir.path(), None, None).unwrap();
        assert_eq!(result, "custom content");
    }

    #[test]
    fn test_assemble_uses_fallback_when_no_file() {
        let dir = tempfile::tempdir().unwrap();
        let fallback = dir.path().join("fallback.md");
        std::fs::write(&fallback, "fallback content").unwrap();

        let config = PromptConfig {
            file: None,
            prepend_commands: vec![],
        };

        let result = assemble(&config, &fallback, dir.path(), None, None).unwrap();
        assert_eq!(result, "fallback content");
    }

    #[test]
    fn test_assemble_with_single_prepend_command() {
        let dir = tempfile::tempdir().unwrap();
        let prompt_path = dir.path().join("prompt.md");
        std::fs::write(&prompt_path, "main prompt").unwrap();

        let config = PromptConfig {
            file: None,
            prepend_commands: vec!["echo 'prepended text'".to_string()],
        };

        let result = assemble(&config, &prompt_path, &dir.path().join("blacksmith.db"), None, None).unwrap();
        assert_eq!(result, "prepended text\n---\nmain prompt");
    }

    #[test]
    fn test_assemble_with_multiple_prepend_commands() {
        let dir = tempfile::tempdir().unwrap();
        let prompt_path = dir.path().join("prompt.md");
        std::fs::write(&prompt_path, "main prompt").unwrap();

        let config = PromptConfig {
            file: None,
            prepend_commands: vec!["echo 'first'".to_string(), "echo 'second'".to_string()],
        };

        let result = assemble(&config, &prompt_path, &dir.path().join("blacksmith.db"), None, None).unwrap();
        assert_eq!(result, "first\n---\nsecond\n---\nmain prompt");
    }

    #[test]
    fn test_assemble_skips_empty_command_output() {
        let dir = tempfile::tempdir().unwrap();
        let prompt_path = dir.path().join("prompt.md");
        std::fs::write(&prompt_path, "main prompt").unwrap();

        let config = PromptConfig {
            file: None,
            prepend_commands: vec![
                "echo 'visible'".to_string(),
                "true".to_string(), // produces no output
                "echo 'also visible'".to_string(),
            ],
        };

        let result = assemble(&config, &prompt_path, &dir.path().join("blacksmith.db"), None, None).unwrap();
        assert_eq!(result, "visible\n---\nalso visible\n---\nmain prompt");
    }

    #[test]
    fn test_assemble_all_commands_empty_returns_raw_prompt() {
        let dir = tempfile::tempdir().unwrap();
        let prompt_path = dir.path().join("prompt.md");
        std::fs::write(&prompt_path, "main prompt").unwrap();

        let config = PromptConfig {
            file: None,
            prepend_commands: vec!["true".to_string(), "true".to_string()],
        };

        let result = assemble(&config, &prompt_path, &dir.path().join("blacksmith.db"), None, None).unwrap();
        assert_eq!(result, "main prompt");
    }

    #[test]
    fn test_assemble_nonzero_exit_still_captures_output() {
        let dir = tempfile::tempdir().unwrap();
        let prompt_path = dir.path().join("prompt.md");
        std::fs::write(&prompt_path, "main prompt").unwrap();

        let config = PromptConfig {
            file: None,
            prepend_commands: vec!["echo 'partial'; exit 1".to_string()],
        };

        let result = assemble(&config, &prompt_path, &dir.path().join("blacksmith.db"), None, None).unwrap();
        assert_eq!(result, "partial\n---\nmain prompt");
    }

    #[test]
    fn test_assemble_missing_prompt_file() {
        let dir = tempfile::tempdir().unwrap();
        let config = PromptConfig {
            file: None,
            prepend_commands: vec![],
        };

        let result = assemble(
            &config,
            Path::new("/nonexistent/prompt.md"),
            &dir.path().join("blacksmith.db"),
            None,
            None,
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, PromptError::ReadFile { .. }));
        assert!(err.to_string().contains("failed to read prompt file"));
    }

    #[test]
    fn test_assemble_whitespace_only_output_is_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let prompt_path = dir.path().join("prompt.md");
        std::fs::write(&prompt_path, "main prompt").unwrap();

        let config = PromptConfig {
            file: None,
            prepend_commands: vec!["echo ''".to_string()],
        };

        let result = assemble(&config, &prompt_path, &dir.path().join("blacksmith.db"), None, None).unwrap();
        assert_eq!(result, "main prompt");
    }

    #[test]
    fn test_assemble_multiline_command_output() {
        let dir = tempfile::tempdir().unwrap();
        let prompt_path = dir.path().join("prompt.md");
        std::fs::write(&prompt_path, "main prompt").unwrap();

        let config = PromptConfig {
            file: None,
            prepend_commands: vec!["printf 'line1\\nline2\\nline3'".to_string()],
        };

        let result = assemble(&config, &prompt_path, &dir.path().join("blacksmith.db"), None, None).unwrap();
        assert_eq!(result, "line1\nline2\nline3\n---\nmain prompt");
    }

    #[test]
    fn test_run_prepend_command_success() {
        let output = run_prepend_command("echo hello").unwrap();
        assert_eq!(output.trim(), "hello");
    }

    #[test]
    fn test_run_prepend_command_empty() {
        let output = run_prepend_command("true").unwrap();
        assert_eq!(output.trim(), "");
    }

    #[test]
    fn test_assemble_no_db_skips_brief() {
        let dir = tempfile::tempdir().unwrap();
        let prompt_path = dir.path().join("prompt.md");
        std::fs::write(&prompt_path, "main prompt").unwrap();
        // No blacksmith.db exists — brief should be silently skipped

        let config = PromptConfig {
            file: None,
            prepend_commands: vec![],
        };

        let result = assemble(&config, &prompt_path, &dir.path().join("blacksmith.db"), None, None).unwrap();
        assert_eq!(result, "main prompt");
    }

    #[test]
    fn test_assemble_empty_db_skips_brief() {
        let dir = tempfile::tempdir().unwrap();
        let prompt_path = dir.path().join("prompt.md");
        std::fs::write(&prompt_path, "main prompt").unwrap();
        // Create empty DB (no improvements)
        let db_path = dir.path().join("blacksmith.db");
        crate::db::open_or_create(&db_path).unwrap();

        let config = PromptConfig {
            file: None,
            prepend_commands: vec![],
        };

        let result = assemble(&config, &prompt_path, &dir.path().join("blacksmith.db"), None, None).unwrap();
        assert_eq!(result, "main prompt");
    }

    #[test]
    fn test_assemble_injects_brief_after_prepend() {
        let dir = tempfile::tempdir().unwrap();
        let prompt_path = dir.path().join("prompt.md");
        std::fs::write(&prompt_path, "main prompt").unwrap();

        // Create DB with an open improvement
        let db_path = dir.path().join("blacksmith.db");
        let conn = crate::db::open_or_create(&db_path).unwrap();
        crate::db::insert_improvement(&conn, "workflow", "Batch file reads", None, None, None)
            .unwrap();
        drop(conn);

        let config = PromptConfig {
            file: None,
            prepend_commands: vec!["echo 'prepend output'".to_string()],
        };

        let result = assemble(&config, &prompt_path, &dir.path().join("blacksmith.db"), None, None).unwrap();
        // Sequence: prepend output, then brief, then prompt
        assert!(result.starts_with("prepend output\n---\n## OPEN IMPROVEMENTS"));
        assert!(result.contains("R1 [workflow] Batch file reads"));
        assert!(result.ends_with("\n---\nmain prompt"));
    }

    #[test]
    fn test_assemble_brief_without_prepend_commands() {
        let dir = tempfile::tempdir().unwrap();
        let prompt_path = dir.path().join("prompt.md");
        std::fs::write(&prompt_path, "main prompt").unwrap();

        // Create DB with an open improvement
        let db_path = dir.path().join("blacksmith.db");
        let conn = crate::db::open_or_create(&db_path).unwrap();
        crate::db::insert_improvement(&conn, "cost", "Reduce API calls", None, None, None).unwrap();
        drop(conn);

        let config = PromptConfig {
            file: None,
            prepend_commands: vec![],
        };

        let result = assemble(&config, &prompt_path, &dir.path().join("blacksmith.db"), None, None).unwrap();
        // Brief should be prepended to prompt
        assert!(result.starts_with("## OPEN IMPROVEMENTS (1 of 1)"));
        assert!(result.contains("R1 [cost] Reduce API calls"));
        assert!(result.ends_with("\n---\nmain prompt"));
    }

    #[test]
    fn test_assemble_passes_targets_config_to_brief() {
        use crate::config::{MetricsTargetsConfig, TargetRule};

        let dir = tempfile::tempdir().unwrap();
        let prompt_path = dir.path().join("prompt.md");
        std::fs::write(&prompt_path, "main prompt").unwrap();

        // Create DB with an observation that misses a target
        let db_path = dir.path().join("blacksmith.db");
        let conn = crate::db::open_or_create(&db_path).unwrap();
        let data = serde_json::json!({"cost.estimate_usd": 5.0});
        crate::db::upsert_observation(&conn, 1, "2026-01-01T00:00:00Z", None, None, &data.to_string()).unwrap();
        drop(conn);

        let targets = MetricsTargetsConfig {
            rules: vec![TargetRule {
                kind: "cost.estimate_usd".to_string(),
                compare: "avg".to_string(),
                relative_to: None,
                threshold: 1.0,
                direction: "below".to_string(),
                label: "Cost per session".to_string(),
                unit: Some("$".to_string()),
            }],
            streak_threshold: 3,
        };

        let config = PromptConfig {
            file: None,
            prepend_commands: vec![],
        };

        let result = assemble(&config, &prompt_path, &db_path, Some(&targets), None).unwrap();
        assert!(result.contains("TARGET WARNING"), "Expected target warning in brief, got: {result}");
        assert!(result.contains("Cost per session"));
        assert!(result.ends_with("\n---\nmain prompt"));
    }
}
