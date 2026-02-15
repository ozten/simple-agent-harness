use regex::Regex;
use serde::Deserialize;
use std::path::{Path, PathBuf};

/// Top-level configuration loaded from blacksmith.toml (or harness.toml for backwards compat).
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
    pub metrics: MetricsConfig,
}

impl HarnessConfig {
    /// Load configuration from a TOML file. If the file doesn't exist,
    /// falls back to harness.toml for backwards compatibility.
    /// If neither file exists, returns compiled defaults.
    /// Returns an error only if a file exists but can't be read or parsed.
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
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // Try harness.toml fallback for backwards compatibility
                let fallback = Path::new("harness.toml");
                if path
                    .file_name()
                    .map(|f| f == "blacksmith.toml")
                    .unwrap_or(false)
                    && fallback.exists()
                {
                    tracing::info!("blacksmith.toml not found, falling back to harness.toml");
                    return Self::load(fallback);
                }
                Ok(Self::default())
            }
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

#[derive(Debug, Deserialize, Clone, Default)]
#[serde(default)]
pub struct MetricsConfig {
    pub extract: MetricsExtractConfig,
    pub targets: MetricsTargetsConfig,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct MetricsTargetsConfig {
    pub rules: Vec<TargetRule>,
    /// Number of consecutive misses before escalating to an ALERT in brief output.
    /// Default: 3
    #[serde(default = "default_streak_threshold")]
    pub streak_threshold: u32,
}

fn default_streak_threshold() -> u32 {
    3
}

impl Default for MetricsTargetsConfig {
    fn default() -> Self {
        Self {
            rules: Vec::new(),
            streak_threshold: default_streak_threshold(),
        }
    }
}

/// A configurable target rule that defines a performance threshold.
#[derive(Debug, Deserialize, Clone)]
pub struct TargetRule {
    /// Event kind to evaluate (e.g. "turns.narration_only", "cost.estimate_usd")
    pub kind: String,
    /// Comparison mode: "pct_of", "pct_sessions", "avg"
    pub compare: String,
    /// For pct_of: the denominator metric kind (e.g. "turns.total")
    #[serde(default)]
    pub relative_to: Option<String>,
    /// Numeric threshold value
    pub threshold: f64,
    /// Direction: "above" (good when >= threshold) or "below" (good when <= threshold)
    pub direction: String,
    /// Human-readable label for display
    pub label: String,
    /// Unit for display (e.g. "%", "$")
    #[serde(default)]
    pub unit: Option<String>,
}

#[derive(Debug, Deserialize, Clone, Default)]
#[serde(default)]
pub struct MetricsExtractConfig {
    pub rules: Vec<ExtractionRule>,
}

/// A configurable extraction rule that scans session output for a pattern
/// and emits a metric event.
#[derive(Debug, Deserialize, Clone)]
pub struct ExtractionRule {
    /// Event kind to emit (e.g. "extract.bead_id", "commit.detected")
    pub kind: String,
    /// Regex pattern to search for
    pub pattern: String,
    /// Exclude matches that also match this pattern (optional)
    pub anti_pattern: Option<String>,
    /// Where to search: "tool_commands", "text", "raw". Default: "tool_commands"
    #[serde(default = "default_source")]
    pub source: String,
    /// Post-processing: "last_segment", "int", "trim". Default: raw capture group.
    pub transform: Option<String>,
    /// Stop after first match, emit the value. Default: false
    #[serde(default)]
    pub first_match: bool,
    /// Emit count of matches instead of match content. Default: false
    #[serde(default)]
    pub count: bool,
    /// Emit a fixed value if pattern is found. Default: not set.
    pub emit: Option<toml::Value>,
}

fn default_source() -> String {
    "tool_commands".to_string()
}

/// A compiled extraction rule ready for use during ingestion.
#[derive(Debug)]
pub struct CompiledRule {
    pub kind: String,
    pub pattern: Regex,
    pub anti_pattern: Option<Regex>,
    pub source: String,
    pub transform: Option<String>,
    pub first_match: bool,
    pub count: bool,
    pub emit: Option<toml::Value>,
}

impl ExtractionRule {
    /// Compile this rule's patterns into regexes.
    /// Returns an error if the pattern or anti_pattern is invalid.
    pub fn compile(&self) -> Result<CompiledRule, String> {
        let pattern = Regex::new(&self.pattern)
            .map_err(|e| format!("invalid pattern '{}': {}", self.pattern, e))?;
        let anti_pattern = self
            .anti_pattern
            .as_ref()
            .map(|ap| Regex::new(ap).map_err(|e| format!("invalid anti_pattern '{}': {}", ap, e)))
            .transpose()?;
        Ok(CompiledRule {
            kind: self.kind.clone(),
            pattern,
            anti_pattern,
            source: self.source.clone(),
            transform: self.transform.clone(),
            first_match: self.first_match,
            count: self.count,
            emit: self.emit.clone(),
        })
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

    #[test]
    fn test_default_config_has_no_extraction_rules() {
        let config = HarnessConfig::default();
        assert!(config.metrics.extract.rules.is_empty());
    }

    #[test]
    fn test_load_extraction_rules_from_toml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blacksmith.toml");
        std::fs::write(
            &path,
            r#"
[[metrics.extract.rules]]
kind = "extract.test_runs"
pattern = "cargo test"
count = true

[[metrics.extract.rules]]
kind = "commit.detected"
pattern = "bd-finish"
emit = true

[[metrics.extract.rules]]
kind = "extract.bead_id"
pattern = 'bd update (\S+) --status.?in.?progress'
transform = "last_segment"
first_match = true
"#,
        )
        .unwrap();
        let config = HarnessConfig::load(&path).unwrap();
        assert_eq!(config.metrics.extract.rules.len(), 3);

        let r0 = &config.metrics.extract.rules[0];
        assert_eq!(r0.kind, "extract.test_runs");
        assert_eq!(r0.pattern, "cargo test");
        assert!(r0.count);
        assert!(!r0.first_match);
        assert_eq!(r0.source, "tool_commands"); // default

        let r1 = &config.metrics.extract.rules[1];
        assert_eq!(r1.kind, "commit.detected");
        assert_eq!(r1.emit, Some(toml::Value::Boolean(true)));

        let r2 = &config.metrics.extract.rules[2];
        assert_eq!(r2.kind, "extract.bead_id");
        assert_eq!(r2.transform, Some("last_segment".to_string()));
        assert!(r2.first_match);
    }

    #[test]
    fn test_load_extraction_rule_with_anti_pattern() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blacksmith.toml");
        std::fs::write(
            &path,
            r#"
[[metrics.extract.rules]]
kind = "extract.full_suite"
pattern = "cargo test"
anti_pattern = "--filter"
source = "tool_commands"
count = true
"#,
        )
        .unwrap();
        let config = HarnessConfig::load(&path).unwrap();
        assert_eq!(config.metrics.extract.rules.len(), 1);
        let rule = &config.metrics.extract.rules[0];
        assert_eq!(rule.anti_pattern, Some("--filter".to_string()));
        assert_eq!(rule.source, "tool_commands");
    }

    #[test]
    fn test_load_extraction_rule_source_raw() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blacksmith.toml");
        std::fs::write(
            &path,
            r#"
[[metrics.extract.rules]]
kind = "extract.file_reads"
pattern = '"name":\s*"Read"'
source = "raw"
count = true
"#,
        )
        .unwrap();
        let config = HarnessConfig::load(&path).unwrap();
        let rule = &config.metrics.extract.rules[0];
        assert_eq!(rule.source, "raw");
    }

    #[test]
    fn test_compile_extraction_rule() {
        let rule = ExtractionRule {
            kind: "test".to_string(),
            pattern: r"(\d+)".to_string(),
            anti_pattern: Some(r"skip".to_string()),
            source: "text".to_string(),
            transform: Some("int".to_string()),
            first_match: true,
            count: false,
            emit: None,
        };
        let compiled = rule.compile().unwrap();
        assert_eq!(compiled.kind, "test");
        assert!(compiled.pattern.is_match("42"));
        assert!(compiled
            .anti_pattern
            .as_ref()
            .unwrap()
            .is_match("skip this"));
    }

    #[test]
    fn test_compile_invalid_regex_returns_error() {
        let rule = ExtractionRule {
            kind: "bad".to_string(),
            pattern: "[invalid".to_string(),
            anti_pattern: None,
            source: "tool_commands".to_string(),
            transform: None,
            first_match: false,
            count: false,
            emit: None,
        };
        let result = rule.compile();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("invalid pattern"));
    }

    #[test]
    fn test_existing_config_with_no_metrics_section_still_works() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blacksmith.toml");
        std::fs::write(
            &path,
            r#"
[session]
max_iterations = 50
"#,
        )
        .unwrap();
        let config = HarnessConfig::load(&path).unwrap();
        assert_eq!(config.session.max_iterations, 50);
        assert!(config.metrics.extract.rules.is_empty());
    }

    #[test]
    fn test_default_streak_threshold() {
        let config = HarnessConfig::default();
        assert_eq!(config.metrics.targets.streak_threshold, 3);
    }

    #[test]
    fn test_load_streak_threshold_from_toml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blacksmith.toml");
        std::fs::write(
            &path,
            r#"
[metrics.targets]
streak_threshold = 5

[[metrics.targets.rules]]
kind = "turns.total"
compare = "avg"
threshold = 80
direction = "below"
label = "Avg turns"
"#,
        )
        .unwrap();
        let config = HarnessConfig::load(&path).unwrap();
        assert_eq!(config.metrics.targets.streak_threshold, 5);
        assert_eq!(config.metrics.targets.rules.len(), 1);
    }
}
