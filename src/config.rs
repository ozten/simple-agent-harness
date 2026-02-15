use serde::Deserialize;
use std::path::{Path, PathBuf};

/// Top-level configuration loaded from harness.toml.
#[derive(Debug, Deserialize)]
#[serde(default)]
#[derive(Default, Clone)]
pub struct HarnessConfig {
    pub session: SessionConfig,
    pub agent: AgentConfig,
    pub watchdog: WatchdogConfig,
    pub retry: RetryConfig,
    pub backoff: BackoffConfig,
    pub shutdown: ShutdownConfig,
    pub hooks: HooksConfig,
    pub prompt: PromptConfig,
    pub output: OutputConfig,
    pub commit_detection: CommitDetectionConfig,
}

impl HarnessConfig {
    /// Load configuration from a TOML file. If the file doesn't exist,
    /// returns compiled defaults. Returns an error only if the file exists
    /// but can't be read or parsed.
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        match std::fs::read_to_string(path) {
            Ok(contents) => {
                let config: HarnessConfig =
                    toml::from_str(&contents).map_err(|e| ConfigError::Parse {
                        path: path.to_path_buf(),
                        source: e,
                    })?;
                Ok(config)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(ConfigError::Read {
                path: path.to_path_buf(),
                source: e,
            }),
        }
    }

    /// Apply CLI overrides to this config. CLI values take precedence
    /// over file/default values when present (Some).
    pub fn apply_cli_overrides(&mut self, overrides: &CliOverrides) {
        if let Some(max) = overrides.max_iterations {
            self.session.max_iterations = max;
        }
        if let Some(ref p) = overrides.prompt {
            self.session.prompt_file = p.clone();
        }
        if let Some(ref d) = overrides.output_dir {
            self.session.output_dir = d.clone();
        }
        if let Some(t) = overrides.timeout {
            self.watchdog.stale_timeout_mins = t;
        }
        if let Some(r) = overrides.retries {
            self.retry.max_empty_retries = r;
        }
    }
}

/// CLI values that can override config file settings.
/// All fields are Option so only explicitly-provided flags apply.
#[derive(Debug, Default)]
pub struct CliOverrides {
    pub max_iterations: Option<u32>,
    pub prompt: Option<PathBuf>,
    pub output_dir: Option<PathBuf>,
    pub timeout: Option<u64>,
    pub retries: Option<u32>,
}

/// Errors that can occur during configuration loading.
#[derive(Debug)]
pub enum ConfigError {
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    Parse {
        path: PathBuf,
        source: toml::de::Error,
    },
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::Read { path, source } => {
                write!(
                    f,
                    "failed to read config file {}: {}",
                    path.display(),
                    source
                )
            }
            ConfigError::Parse { path, source } => {
                write!(
                    f,
                    "failed to parse config file {}: {}",
                    path.display(),
                    source
                )
            }
        }
    }
}

impl std::error::Error for ConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ConfigError::Read { source, .. } => Some(source),
            ConfigError::Parse { source, .. } => Some(source),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct SessionConfig {
    pub max_iterations: u32,
    pub prompt_file: PathBuf,
    pub output_dir: PathBuf,
    pub output_prefix: String,
    pub counter_file: PathBuf,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct AgentConfig {
    pub command: String,
    pub args: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct WatchdogConfig {
    pub check_interval_secs: u64,
    pub stale_timeout_mins: u64,
    pub min_output_bytes: u64,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct RetryConfig {
    pub max_empty_retries: u32,
    pub retry_delay_secs: u64,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct BackoffConfig {
    pub initial_delay_secs: u64,
    pub max_delay_secs: u64,
    pub max_consecutive_rate_limits: u32,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct ShutdownConfig {
    pub stop_file: PathBuf,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
#[derive(Default)]
pub struct HooksConfig {
    pub pre_session: Vec<String>,
    pub post_session: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
#[derive(Default)]
pub struct PromptConfig {
    pub file: Option<PathBuf>,
    pub prepend_commands: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
#[derive(Default)]
pub struct OutputConfig {
    pub event_log: Option<PathBuf>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct CommitDetectionConfig {
    pub patterns: Vec<String>,
}

impl Default for CommitDetectionConfig {
    fn default() -> Self {
        Self {
            patterns: crate::commit::default_patterns(),
        }
    }
}

// --- Default implementations ---

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            max_iterations: 25,
            prompt_file: PathBuf::from("PROMPT.md"),
            output_dir: PathBuf::from("."),
            output_prefix: "claude-iteration".to_string(),
            counter_file: PathBuf::from(".iteration_counter"),
        }
    }
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            command: "claude".to_string(),
            args: vec![
                "-p".to_string(),
                "{prompt}".to_string(),
                "--dangerously-skip-permissions".to_string(),
                "--verbose".to_string(),
                "--output-format".to_string(),
                "stream-json".to_string(),
            ],
        }
    }
}

impl Default for WatchdogConfig {
    fn default() -> Self {
        Self {
            check_interval_secs: 60,
            stale_timeout_mins: 20,
            min_output_bytes: 100,
        }
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_empty_retries: 2,
            retry_delay_secs: 5,
        }
    }
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            initial_delay_secs: 2,
            max_delay_secs: 600,
            max_consecutive_rate_limits: 5,
        }
    }
}

impl Default for ShutdownConfig {
    fn default() -> Self {
        Self {
            stop_file: PathBuf::from("STOP"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config_values() {
        let config = HarnessConfig::default();
        assert_eq!(config.session.max_iterations, 25);
        assert_eq!(config.session.prompt_file, PathBuf::from("PROMPT.md"));
        assert_eq!(config.session.output_dir, PathBuf::from("."));
        assert_eq!(config.session.output_prefix, "claude-iteration");
        assert_eq!(
            config.session.counter_file,
            PathBuf::from(".iteration_counter")
        );
        assert_eq!(config.agent.command, "claude");
        assert_eq!(config.agent.args.len(), 6);
        assert_eq!(config.watchdog.check_interval_secs, 60);
        assert_eq!(config.watchdog.stale_timeout_mins, 20);
        assert_eq!(config.watchdog.min_output_bytes, 100);
        assert_eq!(config.retry.max_empty_retries, 2);
        assert_eq!(config.retry.retry_delay_secs, 5);
        assert_eq!(config.backoff.initial_delay_secs, 2);
        assert_eq!(config.backoff.max_delay_secs, 600);
        assert_eq!(config.backoff.max_consecutive_rate_limits, 5);
        assert_eq!(config.shutdown.stop_file, PathBuf::from("STOP"));
        assert!(config.hooks.pre_session.is_empty());
        assert!(config.hooks.post_session.is_empty());
        assert!(config.prompt.file.is_none());
        assert!(config.prompt.prepend_commands.is_empty());
        assert!(config.output.event_log.is_none());
        assert_eq!(config.commit_detection.patterns.len(), 3);
    }

    #[test]
    fn test_load_missing_file_returns_defaults() {
        let config = HarnessConfig::load(Path::new("/nonexistent/harness.toml")).unwrap();
        assert_eq!(config.session.max_iterations, 25);
        assert_eq!(config.watchdog.stale_timeout_mins, 20);
    }

    #[test]
    fn test_load_empty_file_returns_defaults() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("harness.toml");
        std::fs::write(&path, "").unwrap();
        let config = HarnessConfig::load(&path).unwrap();
        assert_eq!(config.session.max_iterations, 25);
        assert_eq!(config.agent.command, "claude");
    }

    #[test]
    fn test_load_partial_toml_merges_with_defaults() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("harness.toml");
        std::fs::write(
            &path,
            r#"
[session]
max_iterations = 50

[watchdog]
stale_timeout_mins = 30
"#,
        )
        .unwrap();
        let config = HarnessConfig::load(&path).unwrap();
        // Overridden values
        assert_eq!(config.session.max_iterations, 50);
        assert_eq!(config.watchdog.stale_timeout_mins, 30);
        // Default values preserved
        assert_eq!(config.session.prompt_file, PathBuf::from("PROMPT.md"));
        assert_eq!(config.retry.max_empty_retries, 2);
        assert_eq!(config.agent.command, "claude");
    }

    #[test]
    fn test_load_full_toml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("harness.toml");
        std::fs::write(
            &path,
            r#"
[session]
max_iterations = 10
prompt_file = "my-prompt.md"
output_dir = "/tmp/output"
output_prefix = "test-run"
counter_file = ".counter"

[agent]
command = "my-agent"
args = ["--flag"]

[watchdog]
check_interval_secs = 30
stale_timeout_mins = 10
min_output_bytes = 50

[retry]
max_empty_retries = 5
retry_delay_secs = 10

[backoff]
initial_delay_secs = 5
max_delay_secs = 300
max_consecutive_rate_limits = 3

[shutdown]
stop_file = "HALT"

[hooks]
pre_session = ["echo pre"]
post_session = ["echo post"]

[prompt]
file = "CUSTOM.md"
prepend_commands = ["date"]

[output]
event_log = "harness-events.jsonl"
"#,
        )
        .unwrap();
        let config = HarnessConfig::load(&path).unwrap();
        assert_eq!(config.session.max_iterations, 10);
        assert_eq!(config.session.prompt_file, PathBuf::from("my-prompt.md"));
        assert_eq!(config.session.output_dir, PathBuf::from("/tmp/output"));
        assert_eq!(config.session.output_prefix, "test-run");
        assert_eq!(config.agent.command, "my-agent");
        assert_eq!(config.agent.args, vec!["--flag"]);
        assert_eq!(config.watchdog.check_interval_secs, 30);
        assert_eq!(config.retry.max_empty_retries, 5);
        assert_eq!(config.backoff.initial_delay_secs, 5);
        assert_eq!(config.backoff.max_delay_secs, 300);
        assert_eq!(config.shutdown.stop_file, PathBuf::from("HALT"));
        assert_eq!(config.hooks.pre_session, vec!["echo pre"]);
        assert_eq!(config.hooks.post_session, vec!["echo post"]);
        assert_eq!(config.prompt.file, Some(PathBuf::from("CUSTOM.md")));
        assert_eq!(config.prompt.prepend_commands, vec!["date"]);
        assert_eq!(
            config.output.event_log,
            Some(PathBuf::from("harness-events.jsonl"))
        );
    }

    #[test]
    fn test_load_invalid_toml_returns_parse_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("harness.toml");
        std::fs::write(&path, "this is not valid toml [[[").unwrap();
        let err = HarnessConfig::load(&path).unwrap_err();
        assert!(matches!(err, ConfigError::Parse { .. }));
        // Display impl works
        let msg = err.to_string();
        assert!(msg.contains("failed to parse"));
    }

    #[test]
    fn test_cli_overrides_all_fields() {
        let mut config = HarnessConfig::default();
        let overrides = CliOverrides {
            max_iterations: Some(99),
            prompt: Some(PathBuf::from("cli-prompt.md")),
            output_dir: Some(PathBuf::from("/cli/output")),
            timeout: Some(45),
            retries: Some(7),
        };
        config.apply_cli_overrides(&overrides);
        assert_eq!(config.session.max_iterations, 99);
        assert_eq!(config.session.prompt_file, PathBuf::from("cli-prompt.md"));
        assert_eq!(config.session.output_dir, PathBuf::from("/cli/output"));
        assert_eq!(config.watchdog.stale_timeout_mins, 45);
        assert_eq!(config.retry.max_empty_retries, 7);
    }

    #[test]
    fn test_cli_overrides_none_preserves_config() {
        let mut config = HarnessConfig::default();
        let overrides = CliOverrides::default();
        config.apply_cli_overrides(&overrides);
        // Everything should remain at defaults
        assert_eq!(config.session.max_iterations, 25);
        assert_eq!(config.session.prompt_file, PathBuf::from("PROMPT.md"));
        assert_eq!(config.session.output_dir, PathBuf::from("."));
        assert_eq!(config.watchdog.stale_timeout_mins, 20);
        assert_eq!(config.retry.max_empty_retries, 2);
    }

    #[test]
    fn test_cli_overrides_partial() {
        let mut config = HarnessConfig::default();
        let overrides = CliOverrides {
            max_iterations: Some(42),
            timeout: Some(10),
            ..Default::default()
        };
        config.apply_cli_overrides(&overrides);
        assert_eq!(config.session.max_iterations, 42);
        assert_eq!(config.watchdog.stale_timeout_mins, 10);
        // Non-overridden values stay at defaults
        assert_eq!(config.session.prompt_file, PathBuf::from("PROMPT.md"));
        assert_eq!(config.retry.max_empty_retries, 2);
    }

    #[test]
    fn test_full_precedence_chain() {
        // File overrides defaults, CLI overrides file
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("harness.toml");
        std::fs::write(
            &path,
            r#"
[session]
max_iterations = 50

[watchdog]
stale_timeout_mins = 30

[retry]
max_empty_retries = 4
"#,
        )
        .unwrap();
        let mut config = HarnessConfig::load(&path).unwrap();
        // File values
        assert_eq!(config.session.max_iterations, 50);
        assert_eq!(config.watchdog.stale_timeout_mins, 30);
        assert_eq!(config.retry.max_empty_retries, 4);

        // CLI overrides only max_iterations
        let overrides = CliOverrides {
            max_iterations: Some(100),
            ..Default::default()
        };
        config.apply_cli_overrides(&overrides);
        assert_eq!(config.session.max_iterations, 100); // CLI wins
        assert_eq!(config.watchdog.stale_timeout_mins, 30); // file value kept
        assert_eq!(config.retry.max_empty_retries, 4); // file value kept
        assert_eq!(config.session.prompt_file, PathBuf::from("PROMPT.md")); // default kept
    }
}
