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
    pub storage: StorageConfig,
    pub workers: WorkersConfig,
    pub reconciliation: ReconciliationConfig,
    pub architecture: ArchitectureConfig,
    pub quality_gates: QualityGatesConfig,
    pub improvements: ImprovementsConfig,
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
        // output_dir override removed — session output now uses storage.data_dir
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
    #[allow(dead_code)]
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
    /// Which adapter to use for parsing session output.
    /// If omitted, auto-detected from command name.
    pub adapter: Option<String>,
    /// How to deliver the assembled prompt to the agent process.
    /// - "arg" (default): substitute `{prompt}` in args
    /// - "stdin": write prompt to the agent's stdin
    /// - "file": write prompt to a temp file, substitute `{prompt_file}` in args
    #[serde(default)]
    pub prompt_via: PromptVia,
    /// Phase-specific config for coding tasks. If set, takes precedence over
    /// the flat [agent] fields for coding sessions.
    pub coding: Option<AgentPhaseConfig>,
    /// Phase-specific config for integration tasks. Falls back to coding if omitted.
    pub integration: Option<AgentPhaseConfig>,
}

/// Phase-specific agent configuration (used in [agent.coding] and [agent.integration]).
/// All fields are optional; unset fields inherit from the parent [agent] section.
#[derive(Debug, Deserialize, Clone, Default)]
#[serde(default)]
pub struct AgentPhaseConfig {
    pub command: Option<String>,
    pub args: Option<Vec<String>>,
    pub adapter: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_prompt_via")]
    pub prompt_via: Option<PromptVia>,
}

impl AgentConfig {
    /// Resolve the effective agent config for coding tasks.
    /// If [agent.coding] is set, its fields override the flat [agent] fields.
    /// If [agent.coding] is not set, returns the flat [agent] fields.
    pub fn resolved_coding(&self) -> ResolvedAgentConfig {
        match &self.coding {
            Some(phase) => ResolvedAgentConfig {
                command: phase
                    .command
                    .as_deref()
                    .unwrap_or(&self.command)
                    .to_string(),
                args: phase.args.clone().unwrap_or_else(|| self.args.clone()),
                adapter: phase.adapter.clone().or_else(|| self.adapter.clone()),
                prompt_via: phase
                    .prompt_via
                    .clone()
                    .unwrap_or_else(|| self.prompt_via.clone()),
            },
            None => ResolvedAgentConfig {
                command: self.command.clone(),
                args: self.args.clone(),
                adapter: self.adapter.clone(),
                prompt_via: self.prompt_via.clone(),
            },
        }
    }

    /// Resolve the effective agent config for integration tasks.
    /// Falls back: [agent.integration] → [agent.coding] → flat [agent].
    pub fn resolved_integration(&self) -> ResolvedAgentConfig {
        match &self.integration {
            Some(phase) => {
                let coding = self.resolved_coding();
                ResolvedAgentConfig {
                    command: phase
                        .command
                        .as_deref()
                        .unwrap_or(&coding.command)
                        .to_string(),
                    args: phase.args.clone().unwrap_or(coding.args),
                    adapter: phase.adapter.clone().or(coding.adapter),
                    prompt_via: phase.prompt_via.clone().unwrap_or(coding.prompt_via),
                }
            }
            None => self.resolved_coding(),
        }
    }

    /// Returns true if the legacy flat [agent] section is being used
    /// (i.e., no [agent.coding] section is configured).
    #[allow(dead_code)]
    pub fn uses_legacy_flat_config(&self) -> bool {
        self.coding.is_none() && self.integration.is_none()
    }
}

/// Fully resolved agent configuration with no optional fields.
/// Produced by `resolved_coding()` / `resolved_integration()`.
#[derive(Debug, Clone)]
pub struct ResolvedAgentConfig {
    pub command: String,
    pub args: Vec<String>,
    pub adapter: Option<String>,
    pub prompt_via: PromptVia,
}

fn deserialize_optional_prompt_via<'de, D>(deserializer: D) -> Result<Option<PromptVia>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: Option<String> = Option::deserialize(deserializer)?;
    match s {
        None => Ok(None),
        Some(s) => match s.as_str() {
            "arg" => Ok(Some(PromptVia::Arg)),
            "stdin" => Ok(Some(PromptVia::Stdin)),
            "file" => Ok(Some(PromptVia::File)),
            other => Err(serde::de::Error::custom(format!(
                "invalid prompt_via '{}': expected 'arg', 'stdin', or 'file'",
                other
            ))),
        },
    }
}

/// How the assembled prompt is delivered to the agent subprocess.
#[derive(Debug, Clone, PartialEq, Default)]
pub enum PromptVia {
    /// Substitute `{prompt}` placeholders in the agent args (default, existing behavior).
    #[default]
    Arg,
    /// Write the prompt to the agent's stdin.
    Stdin,
    /// Write the prompt to a temp file and substitute `{prompt_file}` in args.
    File,
}

impl std::fmt::Display for PromptVia {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PromptVia::Arg => write!(f, "arg"),
            PromptVia::Stdin => write!(f, "stdin"),
            PromptVia::File => write!(f, "file"),
        }
    }
}

impl<'de> serde::Deserialize<'de> for PromptVia {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "arg" => Ok(PromptVia::Arg),
            "stdin" => Ok(PromptVia::Stdin),
            "file" => Ok(PromptVia::File),
            other => Err(serde::de::Error::custom(format!(
                "invalid prompt_via '{}': expected 'arg', 'stdin', or 'file'",
                other
            ))),
        }
    }
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

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct StorageConfig {
    pub data_dir: PathBuf,
    /// Number of iterations to keep uncompressed. Sessions older than
    /// `current - compress_after` are compressed with zstd. Default: 5.
    pub compress_after: u32,
    /// Retention policy controlling when session files are deleted.
    /// Accepts: "last-N", "Nd", "all", "after-ingest". Default: "last-50".
    #[serde(deserialize_with = "deserialize_retention")]
    pub retention: RetentionPolicy,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from(".blacksmith"),
            compress_after: 5,
            retention: RetentionPolicy::LastN(50),
        }
    }
}

/// Controls when compressed/ingested session files are deleted.
#[derive(Debug, Clone, PartialEq)]
pub enum RetentionPolicy {
    /// Keep the N most recent sessions (compressed or not). Delete older ones.
    LastN(u64),
    /// Keep sessions from the last N days based on file modification time.
    Days(u64),
    /// Never delete. Files still get compressed per `compress_after`.
    All,
    /// Delete the JSONL immediately after successful ingestion. No compression stage.
    AfterIngest,
}

impl std::fmt::Display for RetentionPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RetentionPolicy::LastN(n) => write!(f, "last-{n}"),
            RetentionPolicy::Days(n) => write!(f, "{n}d"),
            RetentionPolicy::All => write!(f, "all"),
            RetentionPolicy::AfterIngest => write!(f, "after-ingest"),
        }
    }
}

impl RetentionPolicy {
    /// Parse a retention policy from a string value.
    pub fn parse(s: &str) -> Result<Self, String> {
        let s = s.trim();
        if s == "all" {
            return Ok(RetentionPolicy::All);
        }
        if s == "after-ingest" {
            return Ok(RetentionPolicy::AfterIngest);
        }
        if let Some(n_str) = s.strip_prefix("last-") {
            let n: u64 = n_str
                .parse()
                .map_err(|_| format!("invalid retention 'last-N': '{n_str}' is not a number"))?;
            if n == 0 {
                return Err("retention 'last-N' requires N > 0".to_string());
            }
            return Ok(RetentionPolicy::LastN(n));
        }
        if let Some(n_str) = s.strip_suffix('d') {
            let n: u64 = n_str
                .parse()
                .map_err(|_| format!("invalid retention 'Nd': '{n_str}' is not a number"))?;
            if n == 0 {
                return Err("retention 'Nd' requires N > 0".to_string());
            }
            return Ok(RetentionPolicy::Days(n));
        }
        Err(format!(
            "invalid retention policy '{s}': expected 'last-N', 'Nd', 'all', or 'after-ingest'"
        ))
    }
}

fn deserialize_retention<'de, D>(deserializer: D) -> Result<RetentionPolicy, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    RetentionPolicy::parse(&s).map_err(serde::de::Error::custom)
}

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct WorkersConfig {
    /// Maximum number of concurrent worker sessions.
    pub max: u32,
    /// Base branch to create worktrees from.
    pub base_branch: String,
    /// Directory under storage.data_dir for worktrees.
    pub worktrees_dir: String,
}

impl Default for WorkersConfig {
    fn default() -> Self {
        Self {
            max: 1,
            base_branch: "main".to_string(),
            worktrees_dir: "worktrees".to_string(),
        }
    }
}

/// Configuration for periodic reconciliation.
///
/// After every N successful integrations, run the full test suite on main.
/// If failures are detected, flag the last N integrated tasks for human review.
#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct ReconciliationConfig {
    /// Run the full test suite every N successful integrations. Default: 3.
    pub every: u32,
}

impl Default for ReconciliationConfig {
    fn default() -> Self {
        Self { every: 3 }
    }
}

/// Configuration knobs for the architecture agent.
///
/// These control how aggressively the architecture agent flags issues
/// like high fan-in modules, integration loops, expansion storms,
/// and metadata drift.
#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct ArchitectureConfig {
    /// Fan-in ratio threshold (0.0–1.0). Modules with fan-in above this
    /// fraction of total modules are flagged for potential refactoring.
    /// Default: 0.3
    pub fan_in_threshold: f64,
    /// Maximum number of integration loop iterations before a task is
    /// flagged as a potential architecture problem. Default: 5
    pub integration_loop_max: u32,
    /// Number of expansion events within `expansion_event_window` tasks
    /// that triggers an architecture review. Default: 5
    pub expansion_event_threshold: u32,
    /// Window size (in tasks) over which expansion events are counted.
    /// Default: 20
    pub expansion_event_window: u32,
    /// Metadata drift sensitivity as a multiplier. A module's metadata
    /// change rate exceeding `metadata_drift_sensitivity` times the
    /// baseline average triggers a drift alert. Default: 3.0
    pub metadata_drift_sensitivity: f64,
    /// Whether refactoring suggestions from the architecture agent are
    /// automatically approved and queued, or require human confirmation.
    /// Default: false
    pub refactor_auto_approve: bool,
}

impl Default for ArchitectureConfig {
    fn default() -> Self {
        Self {
            fan_in_threshold: 0.3,
            integration_loop_max: 5,
            expansion_event_threshold: 5,
            expansion_event_window: 20,
            metadata_drift_sensitivity: 3.0,
            refactor_auto_approve: false,
        }
    }
}

#[allow(dead_code)]
impl ArchitectureConfig {
    /// Conservative preset: higher thresholds, fewer alerts, no auto-approve.
    pub fn conservative() -> Self {
        Self {
            fan_in_threshold: 0.5,
            integration_loop_max: 10,
            expansion_event_threshold: 10,
            expansion_event_window: 30,
            metadata_drift_sensitivity: 5.0,
            refactor_auto_approve: false,
        }
    }

    /// Aggressive preset: lower thresholds, more alerts, auto-approve enabled.
    pub fn aggressive() -> Self {
        Self {
            fan_in_threshold: 0.2,
            integration_loop_max: 3,
            expansion_event_threshold: 3,
            expansion_event_window: 15,
            metadata_drift_sensitivity: 2.0,
            refactor_auto_approve: true,
        }
    }
}

/// Configuration for quality gates run by `blacksmith finish`.
///
/// Each gate is a list of shell commands. All commands in a gate must succeed
/// for the gate to pass. If any gate fails, the bead is NOT closed.
///
/// Defaults are Rust-oriented but can be overridden for any language.
#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct QualityGatesConfig {
    /// Commands to verify the code compiles. Default: `["cargo check"]`
    pub check: Vec<String>,
    /// Commands to run the test suite. Default: `["cargo test"]`
    pub test: Vec<String>,
    /// Commands to run the linter. Default: `["cargo clippy --fix --allow-dirty"]`
    pub lint: Vec<String>,
    /// Commands to check formatting. Default: `["cargo fmt --check"]`
    pub format: Vec<String>,
}

impl Default for QualityGatesConfig {
    fn default() -> Self {
        Self {
            check: vec!["cargo check".to_string()],
            test: vec!["cargo test".to_string()],
            lint: vec!["cargo clippy --fix --allow-dirty".to_string()],
            format: vec!["cargo fmt --check".to_string()],
        }
    }
}

/// Configuration for the self-improvement promotion cycle.
///
/// Controls how improvements are auto-promoted after a configurable number
/// of successful sessions and where promoted rules are written.
#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct ImprovementsConfig {
    /// Number of successful sessions before an open improvement is auto-promoted.
    /// Set to 0 to disable auto-promotion. Default: 5
    pub auto_promote_after: u32,
    /// File to append promoted improvement rules to. Default: "PROMPT.md"
    pub prompt_file: PathBuf,
}

impl Default for ImprovementsConfig {
    fn default() -> Self {
        Self {
            auto_promote_after: 5,
            prompt_file: PathBuf::from("PROMPT.md"),
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

impl HarnessConfig {
    /// Emit deprecation warnings for config keys that have been superseded by storage.data_dir.
    /// Call this after loading config to inform users about migration.
    pub fn warn_deprecated_paths(&self) {
        let defaults = SessionConfig::default();
        if self.session.output_prefix != defaults.output_prefix {
            tracing::warn!(
                "session.output_prefix is deprecated; session output now goes to <storage.data_dir>/sessions/. Ignoring configured value '{}'",
                self.session.output_prefix
            );
        }
        if self.session.counter_file != defaults.counter_file {
            tracing::warn!(
                "session.counter_file is deprecated; counter now lives at <storage.data_dir>/counter. Ignoring configured value '{}'",
                self.session.counter_file.display()
            );
        }
        if self.session.output_dir != defaults.output_dir {
            tracing::warn!(
                "session.output_dir is deprecated; all artifacts now live under <storage.data_dir>/. Ignoring configured value '{}'",
                self.session.output_dir.display()
            );
        }
        // Warn about legacy flat [agent] section when phase-specific sections exist
        if self.agent.coding.is_some() || self.agent.integration.is_some() {
            let agent_defaults = AgentConfig::default();
            if self.agent.command != agent_defaults.command
                || self.agent.args != agent_defaults.args
            {
                tracing::warn!(
                    "flat [agent] command/args are deprecated when [agent.coding] or [agent.integration] is set; \
                     migrate all agent settings to [agent.coding]/[agent.integration]"
                );
            }
        }
    }

    /// Validate the resolved configuration, returning a list of actionable error messages.
    /// Call this after loading config and applying CLI overrides so errors surface immediately.
    pub fn validate(&self) -> Vec<String> {
        let mut errors = Vec::new();

        // prompt_file must exist
        if !self.session.prompt_file.exists() {
            errors.push(format!(
                "session.prompt_file: file '{}' does not exist",
                self.session.prompt_file.display()
            ));
        }

        // output_dir validation removed — session output now uses storage.data_dir

        // Validate resolved agent configs (coding + integration phases)
        let coding = self.agent.resolved_coding();
        let integration = self.agent.resolved_integration();
        for (label, resolved) in [("coding", &coding), ("integration", &integration)] {
            if !resolved.command.is_empty() {
                let cmd = Path::new(&resolved.command);
                if cmd.is_absolute() || resolved.command.contains('/') {
                    if !cmd.exists() {
                        errors.push(format!(
                            "agent.{label}: binary '{}' not found at specified path",
                            resolved.command
                        ));
                    }
                } else if !command_in_path(&resolved.command) {
                    errors.push(format!(
                        "agent.{label}: binary '{}' not found in PATH",
                        resolved.command
                    ));
                }
            } else {
                errors.push(format!("agent.{label}: command must not be empty"));
            }
        }

        // Timeout values must be positive
        if self.watchdog.check_interval_secs == 0 {
            errors.push("watchdog.check_interval_secs: must be greater than 0".to_string());
        }
        if self.watchdog.stale_timeout_mins == 0 {
            errors.push("watchdog.stale_timeout_mins: must be greater than 0".to_string());
        }
        if self.retry.retry_delay_secs == 0 {
            errors.push("retry.retry_delay_secs: must be greater than 0".to_string());
        }
        if self.backoff.initial_delay_secs == 0 {
            errors.push("backoff.initial_delay_secs: must be greater than 0".to_string());
        }
        if self.backoff.max_delay_secs == 0 {
            errors.push("backoff.max_delay_secs: must be greater than 0".to_string());
        }

        // max_iterations must be positive
        if self.session.max_iterations == 0 {
            errors.push("session.max_iterations: must be greater than 0".to_string());
        }

        // backoff.initial_delay should not exceed max_delay
        if self.backoff.initial_delay_secs > self.backoff.max_delay_secs {
            errors.push(format!(
                "backoff.initial_delay_secs ({}) must not exceed backoff.max_delay_secs ({})",
                self.backoff.initial_delay_secs, self.backoff.max_delay_secs
            ));
        }

        // commit_detection.patterns must be valid regexes
        for (i, pat) in self.commit_detection.patterns.iter().enumerate() {
            if let Err(e) = Regex::new(pat) {
                errors.push(format!(
                    "commit_detection.patterns[{}]: invalid regex '{}': {}",
                    i, pat, e
                ));
            }
        }

        // metrics.extract.rules must compile
        for (i, rule) in self.metrics.extract.rules.iter().enumerate() {
            if let Err(e) = rule.compile() {
                errors.push(format!("metrics.extract.rules[{}]: {}", i, e));
            }
        }

        // metrics.targets.rules must have valid compare and direction values
        let valid_compare = ["pct_of", "pct_sessions", "avg"];
        let valid_direction = ["above", "below"];
        for (i, rule) in self.metrics.targets.rules.iter().enumerate() {
            if !valid_compare.contains(&rule.compare.as_str()) {
                errors.push(format!(
                    "metrics.targets.rules[{}]: invalid compare '{}', expected one of: {}",
                    i,
                    rule.compare,
                    valid_compare.join(", ")
                ));
            }
            if !valid_direction.contains(&rule.direction.as_str()) {
                errors.push(format!(
                    "metrics.targets.rules[{}]: invalid direction '{}', expected 'above' or 'below'",
                    i, rule.direction
                ));
            }
            if rule.compare == "pct_of" && rule.relative_to.is_none() {
                errors.push(format!(
                    "metrics.targets.rules[{}]: compare='pct_of' requires 'relative_to' field",
                    i
                ));
            }
        }

        // Architecture config validation
        if self.architecture.fan_in_threshold < 0.0 || self.architecture.fan_in_threshold > 1.0 {
            errors.push(format!(
                "architecture.fan_in_threshold: must be between 0.0 and 1.0, got {}",
                self.architecture.fan_in_threshold
            ));
        }
        if self.architecture.integration_loop_max == 0 {
            errors.push("architecture.integration_loop_max: must be greater than 0".to_string());
        }
        if self.architecture.expansion_event_threshold == 0 {
            errors
                .push("architecture.expansion_event_threshold: must be greater than 0".to_string());
        }
        if self.architecture.expansion_event_window == 0 {
            errors.push("architecture.expansion_event_window: must be greater than 0".to_string());
        }
        if self.architecture.metadata_drift_sensitivity <= 0.0 {
            errors.push(format!(
                "architecture.metadata_drift_sensitivity: must be positive, got {}",
                self.architecture.metadata_drift_sensitivity
            ));
        }

        errors
    }
}

/// Check if a command name exists on the system PATH.
fn command_in_path(cmd: &str) -> bool {
    if let Some(path_var) = std::env::var_os("PATH") {
        for dir in std::env::split_paths(&path_var) {
            let full = dir.join(cmd);
            if full.is_file() {
                return true;
            }
        }
    }
    false
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
            adapter: None,
            prompt_via: PromptVia::default(),
            coding: None,
            integration: None,
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
        assert_eq!(config.storage.data_dir, PathBuf::from(".blacksmith"));
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
        // output_dir override is deprecated and no longer applied
        assert_eq!(config.session.output_dir, PathBuf::from("."));
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

    #[test]
    fn test_default_storage_data_dir() {
        let config = HarnessConfig::default();
        assert_eq!(config.storage.data_dir, PathBuf::from(".blacksmith"));
        assert_eq!(config.storage.compress_after, 5);
        assert_eq!(config.storage.retention, RetentionPolicy::LastN(50));
    }

    #[test]
    fn test_load_storage_data_dir_from_toml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blacksmith.toml");
        std::fs::write(
            &path,
            r#"
[storage]
data_dir = ".my-data"
"#,
        )
        .unwrap();
        let config = HarnessConfig::load(&path).unwrap();
        assert_eq!(config.storage.data_dir, PathBuf::from(".my-data"));
        // compress_after should use default when not specified
        assert_eq!(config.storage.compress_after, 5);
    }

    #[test]
    fn test_load_compress_after_from_toml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blacksmith.toml");
        std::fs::write(
            &path,
            r#"
[storage]
compress_after = 10
"#,
        )
        .unwrap();
        let config = HarnessConfig::load(&path).unwrap();
        assert_eq!(config.storage.compress_after, 10);
    }

    // --- Validation tests ---

    /// Helper: create a valid config with real paths so validation passes.
    fn valid_config() -> HarnessConfig {
        let dir = tempfile::tempdir().unwrap();
        let prompt = dir.path().join("PROMPT.md");
        std::fs::write(&prompt, "test prompt").unwrap();
        let mut config = HarnessConfig::default();
        config.session.prompt_file = prompt;
        config.session.output_dir = dir.path().to_path_buf();
        // Use a command known to exist
        config.agent.command = "sh".to_string();
        // Leak the tempdir so it lives through validation
        std::mem::forget(dir);
        config
    }

    #[test]
    fn test_validate_valid_config_no_errors() {
        let config = valid_config();
        let errors = config.validate();
        assert!(errors.is_empty(), "expected no errors, got: {:?}", errors);
    }

    #[test]
    fn test_validate_missing_prompt_file() {
        let mut config = valid_config();
        config.session.prompt_file = PathBuf::from("/nonexistent/prompt.md");
        let errors = config.validate();
        assert!(errors
            .iter()
            .any(|e| e.contains("session.prompt_file") && e.contains("does not exist")));
    }

    // test_validate_uncreatable_output_dir removed — output_dir is deprecated

    #[test]
    fn test_validate_empty_command() {
        let mut config = valid_config();
        config.agent.command = String::new();
        let errors = config.validate();
        assert!(errors
            .iter()
            .any(|e| e.contains("command must not be empty")));
    }

    #[test]
    fn test_validate_command_not_in_path() {
        let mut config = valid_config();
        config.agent.command = "nonexistent_binary_xyz_12345".to_string();
        let errors = config.validate();
        assert!(errors.iter().any(|e| e.contains("not found in PATH")));
    }

    #[test]
    fn test_validate_command_absolute_path_missing() {
        let mut config = valid_config();
        config.agent.command = "/nonexistent/path/to/binary".to_string();
        let errors = config.validate();
        assert!(errors
            .iter()
            .any(|e| e.contains("not found at specified path")));
    }

    #[test]
    fn test_validate_zero_timeout_values() {
        let mut config = valid_config();
        config.watchdog.check_interval_secs = 0;
        config.watchdog.stale_timeout_mins = 0;
        config.retry.retry_delay_secs = 0;
        config.backoff.initial_delay_secs = 0;
        config.backoff.max_delay_secs = 0;
        let errors = config.validate();
        assert!(errors.iter().any(|e| e.contains("check_interval_secs")));
        assert!(errors.iter().any(|e| e.contains("stale_timeout_mins")));
        assert!(errors.iter().any(|e| e.contains("retry_delay_secs")));
        assert!(errors.iter().any(|e| e.contains("initial_delay_secs")));
        assert!(errors.iter().any(|e| e.contains("max_delay_secs")));
    }

    #[test]
    fn test_validate_zero_max_iterations() {
        let mut config = valid_config();
        config.session.max_iterations = 0;
        let errors = config.validate();
        assert!(errors.iter().any(|e| e.contains("session.max_iterations")));
    }

    #[test]
    fn test_validate_initial_delay_exceeds_max_delay() {
        let mut config = valid_config();
        config.backoff.initial_delay_secs = 1000;
        config.backoff.max_delay_secs = 100;
        let errors = config.validate();
        assert!(errors
            .iter()
            .any(|e| e.contains("initial_delay_secs") && e.contains("must not exceed")));
    }

    #[test]
    fn test_validate_invalid_commit_detection_pattern() {
        let mut config = valid_config();
        config.commit_detection.patterns = vec!["[invalid".to_string()];
        let errors = config.validate();
        assert!(errors
            .iter()
            .any(|e| e.contains("commit_detection.patterns")));
    }

    #[test]
    fn test_validate_invalid_extraction_rule() {
        let mut config = valid_config();
        config.metrics.extract.rules.push(ExtractionRule {
            kind: "bad".to_string(),
            pattern: "[invalid".to_string(),
            anti_pattern: None,
            source: "tool_commands".to_string(),
            transform: None,
            first_match: false,
            count: false,
            emit: None,
        });
        let errors = config.validate();
        assert!(errors.iter().any(|e| e.contains("metrics.extract.rules")));
    }

    #[test]
    fn test_validate_invalid_target_compare() {
        let mut config = valid_config();
        config.metrics.targets.rules.push(TargetRule {
            kind: "test".to_string(),
            compare: "invalid_compare".to_string(),
            relative_to: None,
            threshold: 1.0,
            direction: "above".to_string(),
            label: "test".to_string(),
            unit: None,
        });
        let errors = config.validate();
        assert!(errors.iter().any(|e| e.contains("invalid compare")));
    }

    #[test]
    fn test_validate_invalid_target_direction() {
        let mut config = valid_config();
        config.metrics.targets.rules.push(TargetRule {
            kind: "test".to_string(),
            compare: "avg".to_string(),
            relative_to: None,
            threshold: 1.0,
            direction: "sideways".to_string(),
            label: "test".to_string(),
            unit: None,
        });
        let errors = config.validate();
        assert!(errors.iter().any(|e| e.contains("invalid direction")));
    }

    #[test]
    fn test_validate_pct_of_missing_relative_to() {
        let mut config = valid_config();
        config.metrics.targets.rules.push(TargetRule {
            kind: "test".to_string(),
            compare: "pct_of".to_string(),
            relative_to: None,
            threshold: 50.0,
            direction: "below".to_string(),
            label: "test".to_string(),
            unit: None,
        });
        let errors = config.validate();
        assert!(errors.iter().any(|e| e.contains("requires 'relative_to'")));
    }

    #[test]
    fn test_validate_multiple_errors_collected() {
        let mut config = valid_config();
        config.session.max_iterations = 0;
        config.watchdog.check_interval_secs = 0;
        config.agent.command = "nonexistent_binary_xyz_12345".to_string();
        let errors = config.validate();
        assert!(
            errors.len() >= 3,
            "expected at least 3 errors, got {}: {:?}",
            errors.len(),
            errors
        );
    }

    #[test]
    fn test_default_agent_adapter_is_none() {
        let config = HarnessConfig::default();
        assert!(config.agent.adapter.is_none());
    }

    #[test]
    fn test_load_agent_adapter_from_toml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blacksmith.toml");
        std::fs::write(
            &path,
            r#"
[agent]
command = "my-agent"
args = ["--flag"]
adapter = "raw"
"#,
        )
        .unwrap();
        let config = HarnessConfig::load(&path).unwrap();
        assert_eq!(config.agent.adapter, Some("raw".to_string()));
    }

    #[test]
    fn test_load_agent_without_adapter_field() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blacksmith.toml");
        std::fs::write(
            &path,
            r#"
[agent]
command = "claude"
args = ["-p", "{prompt}"]
"#,
        )
        .unwrap();
        let config = HarnessConfig::load(&path).unwrap();
        assert!(config.agent.adapter.is_none());
    }

    #[test]
    fn test_command_in_path_finds_sh() {
        assert!(command_in_path("sh"));
    }

    #[test]
    fn test_command_in_path_missing_binary() {
        assert!(!command_in_path("nonexistent_binary_xyz_12345"));
    }

    // --- Retention policy tests ---

    #[test]
    fn test_retention_parse_last_n() {
        assert_eq!(
            RetentionPolicy::parse("last-50").unwrap(),
            RetentionPolicy::LastN(50)
        );
        assert_eq!(
            RetentionPolicy::parse("last-1").unwrap(),
            RetentionPolicy::LastN(1)
        );
        assert_eq!(
            RetentionPolicy::parse("last-1000").unwrap(),
            RetentionPolicy::LastN(1000)
        );
    }

    #[test]
    fn test_retention_parse_days() {
        assert_eq!(
            RetentionPolicy::parse("30d").unwrap(),
            RetentionPolicy::Days(30)
        );
        assert_eq!(
            RetentionPolicy::parse("7d").unwrap(),
            RetentionPolicy::Days(7)
        );
        assert_eq!(
            RetentionPolicy::parse("1d").unwrap(),
            RetentionPolicy::Days(1)
        );
    }

    #[test]
    fn test_retention_parse_all() {
        assert_eq!(RetentionPolicy::parse("all").unwrap(), RetentionPolicy::All);
    }

    #[test]
    fn test_retention_parse_after_ingest() {
        assert_eq!(
            RetentionPolicy::parse("after-ingest").unwrap(),
            RetentionPolicy::AfterIngest
        );
    }

    #[test]
    fn test_retention_parse_invalid() {
        assert!(RetentionPolicy::parse("").is_err());
        assert!(RetentionPolicy::parse("keep-forever").is_err());
        assert!(RetentionPolicy::parse("last-").is_err());
        assert!(RetentionPolicy::parse("last-0").is_err());
        assert!(RetentionPolicy::parse("0d").is_err());
        assert!(RetentionPolicy::parse("d").is_err());
        assert!(RetentionPolicy::parse("last-abc").is_err());
    }

    #[test]
    fn test_retention_display() {
        assert_eq!(RetentionPolicy::LastN(50).to_string(), "last-50");
        assert_eq!(RetentionPolicy::Days(30).to_string(), "30d");
        assert_eq!(RetentionPolicy::All.to_string(), "all");
        assert_eq!(RetentionPolicy::AfterIngest.to_string(), "after-ingest");
    }

    #[test]
    fn test_load_retention_from_toml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blacksmith.toml");
        std::fs::write(
            &path,
            r#"
[storage]
retention = "last-100"
"#,
        )
        .unwrap();
        let config = HarnessConfig::load(&path).unwrap();
        assert_eq!(config.storage.retention, RetentionPolicy::LastN(100));
    }

    #[test]
    fn test_load_retention_days_from_toml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blacksmith.toml");
        std::fs::write(
            &path,
            r#"
[storage]
retention = "14d"
"#,
        )
        .unwrap();
        let config = HarnessConfig::load(&path).unwrap();
        assert_eq!(config.storage.retention, RetentionPolicy::Days(14));
    }

    #[test]
    fn test_load_retention_all_from_toml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blacksmith.toml");
        std::fs::write(
            &path,
            r#"
[storage]
retention = "all"
"#,
        )
        .unwrap();
        let config = HarnessConfig::load(&path).unwrap();
        assert_eq!(config.storage.retention, RetentionPolicy::All);
    }

    #[test]
    fn test_load_retention_after_ingest_from_toml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blacksmith.toml");
        std::fs::write(
            &path,
            r#"
[storage]
retention = "after-ingest"
"#,
        )
        .unwrap();
        let config = HarnessConfig::load(&path).unwrap();
        assert_eq!(config.storage.retention, RetentionPolicy::AfterIngest);
    }

    #[test]
    fn test_load_invalid_retention_from_toml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blacksmith.toml");
        std::fs::write(
            &path,
            r#"
[storage]
retention = "invalid-policy"
"#,
        )
        .unwrap();
        let err = HarnessConfig::load(&path).unwrap_err();
        assert!(matches!(err, ConfigError::Parse { .. }));
    }

    #[test]
    fn test_default_retention_when_not_specified() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blacksmith.toml");
        std::fs::write(
            &path,
            r#"
[storage]
data_dir = ".my-data"
"#,
        )
        .unwrap();
        let config = HarnessConfig::load(&path).unwrap();
        assert_eq!(config.storage.retention, RetentionPolicy::LastN(50));
    }

    // --- prompt_via tests ---

    #[test]
    fn test_default_prompt_via_is_arg() {
        let config = HarnessConfig::default();
        assert_eq!(config.agent.prompt_via, PromptVia::Arg);
    }

    #[test]
    fn test_prompt_via_display() {
        assert_eq!(PromptVia::Arg.to_string(), "arg");
        assert_eq!(PromptVia::Stdin.to_string(), "stdin");
        assert_eq!(PromptVia::File.to_string(), "file");
    }

    #[test]
    fn test_load_prompt_via_stdin_from_toml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blacksmith.toml");
        std::fs::write(
            &path,
            r#"
[agent]
command = "opencode"
args = ["--non-interactive"]
prompt_via = "stdin"
"#,
        )
        .unwrap();
        let config = HarnessConfig::load(&path).unwrap();
        assert_eq!(config.agent.prompt_via, PromptVia::Stdin);
    }

    #[test]
    fn test_load_prompt_via_file_from_toml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blacksmith.toml");
        std::fs::write(
            &path,
            r#"
[agent]
command = "aider"
args = ["--message-file", "{prompt_file}"]
prompt_via = "file"
"#,
        )
        .unwrap();
        let config = HarnessConfig::load(&path).unwrap();
        assert_eq!(config.agent.prompt_via, PromptVia::File);
    }

    #[test]
    fn test_load_prompt_via_arg_from_toml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blacksmith.toml");
        std::fs::write(
            &path,
            r#"
[agent]
command = "claude"
args = ["-p", "{prompt}"]
prompt_via = "arg"
"#,
        )
        .unwrap();
        let config = HarnessConfig::load(&path).unwrap();
        assert_eq!(config.agent.prompt_via, PromptVia::Arg);
    }

    #[test]
    fn test_load_prompt_via_default_when_not_specified() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blacksmith.toml");
        std::fs::write(
            &path,
            r#"
[agent]
command = "claude"
args = ["-p", "{prompt}"]
"#,
        )
        .unwrap();
        let config = HarnessConfig::load(&path).unwrap();
        assert_eq!(config.agent.prompt_via, PromptVia::Arg);
    }

    #[test]
    fn test_load_invalid_prompt_via_from_toml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blacksmith.toml");
        std::fs::write(
            &path,
            r#"
[agent]
command = "claude"
prompt_via = "invalid"
"#,
        )
        .unwrap();
        let err = HarnessConfig::load(&path).unwrap_err();
        assert!(matches!(err, ConfigError::Parse { .. }));
    }

    // --- agent.coding / agent.integration tests ---

    #[test]
    fn test_default_agent_has_no_phase_configs() {
        let config = HarnessConfig::default();
        assert!(config.agent.coding.is_none());
        assert!(config.agent.integration.is_none());
    }

    #[test]
    fn test_resolved_coding_falls_back_to_flat_agent() {
        let config = HarnessConfig::default();
        let resolved = config.agent.resolved_coding();
        assert_eq!(resolved.command, "claude");
        assert_eq!(resolved.args.len(), 6);
        assert!(resolved.adapter.is_none());
        assert_eq!(resolved.prompt_via, PromptVia::Arg);
    }

    #[test]
    fn test_resolved_integration_falls_back_to_coding_then_flat() {
        let config = HarnessConfig::default();
        let resolved = config.agent.resolved_integration();
        // With no coding or integration, falls back to flat agent
        assert_eq!(resolved.command, "claude");
        assert_eq!(resolved.args.len(), 6);
    }

    #[test]
    fn test_load_agent_coding_from_toml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blacksmith.toml");
        std::fs::write(
            &path,
            r#"
[agent.coding]
command = "claude"
args = ["-p", "{prompt}", "--verbose", "--output-format", "stream-json"]
adapter = "claude"
"#,
        )
        .unwrap();
        let config = HarnessConfig::load(&path).unwrap();
        assert!(config.agent.coding.is_some());
        let coding = config.agent.coding.unwrap();
        assert_eq!(coding.command, Some("claude".to_string()));
        assert_eq!(coding.args.unwrap().len(), 5);
        assert_eq!(coding.adapter, Some("claude".to_string()));
    }

    #[test]
    fn test_load_agent_coding_and_integration_from_toml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blacksmith.toml");
        std::fs::write(
            &path,
            r#"
[agent.coding]
command = "claude"
args = ["-p", "{prompt}", "--verbose", "--output-format", "stream-json"]
adapter = "claude"

[agent.integration]
command = "claude"
args = ["-p", "{prompt}", "--output-format", "stream-json"]
adapter = "claude"
"#,
        )
        .unwrap();
        let config = HarnessConfig::load(&path).unwrap();
        assert!(config.agent.coding.is_some());
        assert!(config.agent.integration.is_some());

        let resolved_coding = config.agent.resolved_coding();
        assert_eq!(resolved_coding.command, "claude");
        assert_eq!(resolved_coding.args.len(), 5);

        let resolved_integration = config.agent.resolved_integration();
        assert_eq!(resolved_integration.command, "claude");
        assert_eq!(resolved_integration.args.len(), 4); // fewer args
    }

    #[test]
    fn test_resolved_coding_overrides_flat_fields() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blacksmith.toml");
        std::fs::write(
            &path,
            r#"
[agent]
command = "legacy-agent"
args = ["--legacy"]

[agent.coding]
command = "claude"
args = ["-p", "{prompt}"]
"#,
        )
        .unwrap();
        let config = HarnessConfig::load(&path).unwrap();
        let resolved = config.agent.resolved_coding();
        assert_eq!(resolved.command, "claude");
        assert_eq!(resolved.args, vec!["-p", "{prompt}"]);
    }

    #[test]
    fn test_resolved_integration_inherits_from_coding() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blacksmith.toml");
        std::fs::write(
            &path,
            r#"
[agent.coding]
command = "claude"
args = ["-p", "{prompt}", "--verbose"]
adapter = "claude"
prompt_via = "stdin"
"#,
        )
        .unwrap();
        let config = HarnessConfig::load(&path).unwrap();
        // No [agent.integration] set — should fall back to coding
        let resolved = config.agent.resolved_integration();
        assert_eq!(resolved.command, "claude");
        assert_eq!(resolved.args, vec!["-p", "{prompt}", "--verbose"]);
        assert_eq!(resolved.adapter, Some("claude".to_string()));
        assert_eq!(resolved.prompt_via, PromptVia::Stdin);
    }

    #[test]
    fn test_resolved_integration_partial_override() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blacksmith.toml");
        std::fs::write(
            &path,
            r#"
[agent.coding]
command = "claude"
args = ["-p", "{prompt}", "--verbose"]
adapter = "claude"

[agent.integration]
args = ["-p", "{prompt}"]
"#,
        )
        .unwrap();
        let config = HarnessConfig::load(&path).unwrap();
        let resolved = config.agent.resolved_integration();
        // command falls back to coding
        assert_eq!(resolved.command, "claude");
        // args overridden by integration
        assert_eq!(resolved.args, vec!["-p", "{prompt}"]);
        // adapter falls back to coding
        assert_eq!(resolved.adapter, Some("claude".to_string()));
    }

    #[test]
    fn test_uses_legacy_flat_config() {
        let config = HarnessConfig::default();
        assert!(config.agent.uses_legacy_flat_config());

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blacksmith.toml");
        std::fs::write(
            &path,
            r#"
[agent.coding]
command = "claude"
"#,
        )
        .unwrap();
        let config = HarnessConfig::load(&path).unwrap();
        assert!(!config.agent.uses_legacy_flat_config());
    }

    // --- Reconciliation config tests ---

    #[test]
    fn test_default_reconciliation_every() {
        let config = HarnessConfig::default();
        assert_eq!(config.reconciliation.every, 3);
    }

    #[test]
    fn test_load_reconciliation_from_toml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blacksmith.toml");
        std::fs::write(
            &path,
            r#"
[reconciliation]
every = 5
"#,
        )
        .unwrap();
        let config = HarnessConfig::load(&path).unwrap();
        assert_eq!(config.reconciliation.every, 5);
    }

    #[test]
    fn test_reconciliation_default_when_not_specified() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blacksmith.toml");
        std::fs::write(
            &path,
            r#"
[session]
max_iterations = 10
"#,
        )
        .unwrap();
        let config = HarnessConfig::load(&path).unwrap();
        assert_eq!(config.reconciliation.every, 3);
    }

    #[test]
    fn test_reconciliation_zero_disables() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blacksmith.toml");
        std::fs::write(
            &path,
            r#"
[reconciliation]
every = 0
"#,
        )
        .unwrap();
        let config = HarnessConfig::load(&path).unwrap();
        assert_eq!(config.reconciliation.every, 0);
    }

    #[test]
    fn test_validate_resolves_agent_for_both_phases() {
        let mut config = valid_config();
        // Set coding to a valid command, integration inherits
        config.agent.coding = Some(AgentPhaseConfig {
            command: Some("sh".to_string()),
            ..Default::default()
        });
        let errors = config.validate();
        // Should validate both coding and integration (both resolve to "sh")
        assert!(
            !errors
                .iter()
                .any(|e| e.contains("agent.coding") || e.contains("agent.integration")),
            "expected no agent errors, got: {:?}",
            errors
        );
    }

    #[test]
    fn test_validate_catches_bad_integration_command() {
        let mut config = valid_config();
        config.agent.coding = Some(AgentPhaseConfig {
            command: Some("sh".to_string()),
            ..Default::default()
        });
        config.agent.integration = Some(AgentPhaseConfig {
            command: Some("nonexistent_binary_xyz_12345".to_string()),
            ..Default::default()
        });
        let errors = config.validate();
        assert!(errors
            .iter()
            .any(|e| e.contains("agent.integration") && e.contains("not found")));
        // coding should still be valid
        assert!(!errors.iter().any(|e| e.contains("agent.coding")));
    }

    // --- Architecture config tests ---

    #[test]
    fn test_default_architecture_config() {
        let config = HarnessConfig::default();
        assert!((config.architecture.fan_in_threshold - 0.3).abs() < f64::EPSILON);
        assert_eq!(config.architecture.integration_loop_max, 5);
        assert_eq!(config.architecture.expansion_event_threshold, 5);
        assert_eq!(config.architecture.expansion_event_window, 20);
        assert!((config.architecture.metadata_drift_sensitivity - 3.0).abs() < f64::EPSILON);
        assert!(!config.architecture.refactor_auto_approve);
    }

    #[test]
    fn test_architecture_conservative_preset() {
        let preset = ArchitectureConfig::conservative();
        assert!((preset.fan_in_threshold - 0.5).abs() < f64::EPSILON);
        assert_eq!(preset.integration_loop_max, 10);
        assert_eq!(preset.expansion_event_threshold, 10);
        assert_eq!(preset.expansion_event_window, 30);
        assert!((preset.metadata_drift_sensitivity - 5.0).abs() < f64::EPSILON);
        assert!(!preset.refactor_auto_approve);
    }

    #[test]
    fn test_architecture_aggressive_preset() {
        let preset = ArchitectureConfig::aggressive();
        assert!((preset.fan_in_threshold - 0.2).abs() < f64::EPSILON);
        assert_eq!(preset.integration_loop_max, 3);
        assert_eq!(preset.expansion_event_threshold, 3);
        assert_eq!(preset.expansion_event_window, 15);
        assert!((preset.metadata_drift_sensitivity - 2.0).abs() < f64::EPSILON);
        assert!(preset.refactor_auto_approve);
    }

    #[test]
    fn test_load_architecture_from_toml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blacksmith.toml");
        std::fs::write(
            &path,
            r#"
[architecture]
fan_in_threshold = 0.4
integration_loop_max = 8
expansion_event_threshold = 7
expansion_event_window = 25
metadata_drift_sensitivity = 4.0
refactor_auto_approve = true
"#,
        )
        .unwrap();
        let config = HarnessConfig::load(&path).unwrap();
        assert!((config.architecture.fan_in_threshold - 0.4).abs() < f64::EPSILON);
        assert_eq!(config.architecture.integration_loop_max, 8);
        assert_eq!(config.architecture.expansion_event_threshold, 7);
        assert_eq!(config.architecture.expansion_event_window, 25);
        assert!((config.architecture.metadata_drift_sensitivity - 4.0).abs() < f64::EPSILON);
        assert!(config.architecture.refactor_auto_approve);
    }

    #[test]
    fn test_architecture_defaults_when_not_specified() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blacksmith.toml");
        std::fs::write(
            &path,
            r#"
[session]
max_iterations = 10
"#,
        )
        .unwrap();
        let config = HarnessConfig::load(&path).unwrap();
        assert!((config.architecture.fan_in_threshold - 0.3).abs() < f64::EPSILON);
        assert_eq!(config.architecture.integration_loop_max, 5);
        assert!(!config.architecture.refactor_auto_approve);
    }

    #[test]
    fn test_architecture_partial_override() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blacksmith.toml");
        std::fs::write(
            &path,
            r#"
[architecture]
fan_in_threshold = 0.1
refactor_auto_approve = true
"#,
        )
        .unwrap();
        let config = HarnessConfig::load(&path).unwrap();
        assert!((config.architecture.fan_in_threshold - 0.1).abs() < f64::EPSILON);
        assert!(config.architecture.refactor_auto_approve);
        // Other fields should remain at defaults
        assert_eq!(config.architecture.integration_loop_max, 5);
        assert_eq!(config.architecture.expansion_event_window, 20);
    }

    #[test]
    fn test_validate_architecture_fan_in_out_of_range() {
        let mut config = valid_config();
        config.architecture.fan_in_threshold = 1.5;
        let errors = config.validate();
        assert!(errors
            .iter()
            .any(|e| e.contains("architecture.fan_in_threshold")));

        config.architecture.fan_in_threshold = -0.1;
        let errors = config.validate();
        assert!(errors
            .iter()
            .any(|e| e.contains("architecture.fan_in_threshold")));
    }

    #[test]
    fn test_validate_architecture_zero_values() {
        let mut config = valid_config();
        config.architecture.integration_loop_max = 0;
        config.architecture.expansion_event_threshold = 0;
        config.architecture.expansion_event_window = 0;
        let errors = config.validate();
        assert!(errors
            .iter()
            .any(|e| e.contains("architecture.integration_loop_max")));
        assert!(errors
            .iter()
            .any(|e| e.contains("architecture.expansion_event_threshold")));
        assert!(errors
            .iter()
            .any(|e| e.contains("architecture.expansion_event_window")));
    }

    #[test]
    fn test_validate_architecture_negative_drift_sensitivity() {
        let mut config = valid_config();
        config.architecture.metadata_drift_sensitivity = -1.0;
        let errors = config.validate();
        assert!(errors
            .iter()
            .any(|e| e.contains("architecture.metadata_drift_sensitivity")));
    }

    #[test]
    fn test_validate_architecture_valid_config() {
        let config = valid_config();
        let errors = config.validate();
        assert!(
            !errors.iter().any(|e| e.contains("architecture")),
            "unexpected architecture errors: {:?}",
            errors
        );
    }

    // --- Quality gates config tests ---

    #[test]
    fn test_default_quality_gates() {
        let config = HarnessConfig::default();
        assert_eq!(config.quality_gates.check, vec!["cargo check"]);
        assert_eq!(config.quality_gates.test, vec!["cargo test"]);
        assert_eq!(
            config.quality_gates.lint,
            vec!["cargo clippy --fix --allow-dirty"]
        );
        assert_eq!(config.quality_gates.format, vec!["cargo fmt --check"]);
    }

    #[test]
    fn test_load_quality_gates_from_toml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blacksmith.toml");
        std::fs::write(
            &path,
            r#"
[quality_gates]
check = ["npm run build"]
test = ["npm test", "npm run e2e"]
lint = ["eslint ."]
format = []
"#,
        )
        .unwrap();
        let config = HarnessConfig::load(&path).unwrap();
        assert_eq!(config.quality_gates.check, vec!["npm run build"]);
        assert_eq!(config.quality_gates.test, vec!["npm test", "npm run e2e"]);
        assert_eq!(config.quality_gates.lint, vec!["eslint ."]);
        assert!(config.quality_gates.format.is_empty());
    }

    #[test]
    fn test_quality_gates_defaults_when_not_specified() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blacksmith.toml");
        std::fs::write(
            &path,
            r#"
[session]
max_iterations = 10
"#,
        )
        .unwrap();
        let config = HarnessConfig::load(&path).unwrap();
        assert_eq!(config.quality_gates.check, vec!["cargo check"]);
        assert_eq!(config.quality_gates.test, vec!["cargo test"]);
    }

    // --- Improvements config tests ---

    #[test]
    fn test_default_improvements_config() {
        let config = HarnessConfig::default();
        assert_eq!(config.improvements.auto_promote_after, 5);
        assert_eq!(config.improvements.prompt_file, PathBuf::from("PROMPT.md"));
    }

    #[test]
    fn test_load_improvements_from_toml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blacksmith.toml");
        std::fs::write(
            &path,
            r#"
[improvements]
auto_promote_after = 10
prompt_file = "AGENTS.md"
"#,
        )
        .unwrap();
        let config = HarnessConfig::load(&path).unwrap();
        assert_eq!(config.improvements.auto_promote_after, 10);
        assert_eq!(config.improvements.prompt_file, PathBuf::from("AGENTS.md"));
    }

    #[test]
    fn test_improvements_defaults_when_not_specified() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blacksmith.toml");
        std::fs::write(
            &path,
            r#"
[session]
max_iterations = 10
"#,
        )
        .unwrap();
        let config = HarnessConfig::load(&path).unwrap();
        assert_eq!(config.improvements.auto_promote_after, 5);
        assert_eq!(config.improvements.prompt_file, PathBuf::from("PROMPT.md"));
    }

    #[test]
    fn test_improvements_auto_promote_disabled() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blacksmith.toml");
        std::fs::write(
            &path,
            r#"
[improvements]
auto_promote_after = 0
"#,
        )
        .unwrap();
        let config = HarnessConfig::load(&path).unwrap();
        assert_eq!(config.improvements.auto_promote_after, 0);
    }
}
