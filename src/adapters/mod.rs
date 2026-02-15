pub mod claude;
pub mod codex;
pub mod raw;

use serde_json::Value;
use std::path::Path;

/// Source type for configurable extraction rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtractionSource {
    /// Tool invocation commands (e.g., tool_use input.command fields).
    ToolCommands,
    /// Assistant text output blocks.
    Text,
    /// Raw file lines, unprocessed.
    Raw,
}

/// Errors produced by adapter operations.
#[derive(Debug)]
pub enum AdapterError {
    Io(std::io::Error),
    Parse(String),
}

impl std::fmt::Display for AdapterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AdapterError::Io(e) => write!(f, "I/O error: {e}"),
            AdapterError::Parse(msg) => write!(f, "parse error: {msg}"),
        }
    }
}

impl std::error::Error for AdapterError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            AdapterError::Io(e) => Some(e),
            AdapterError::Parse(_) => None,
        }
    }
}

impl From<std::io::Error> for AdapterError {
    fn from(e: std::io::Error) -> Self {
        AdapterError::Io(e)
    }
}

/// Normalizes agent-specific output into blacksmith events.
pub trait AgentAdapter: Send + Sync {
    /// Human-readable adapter name (e.g., "claude", "codex").
    fn name(&self) -> &str;

    /// Extract built-in metrics from a session output file.
    ///
    /// Returns a Vec of (kind, value) pairs representing the metrics
    /// this adapter knows how to extract from its native format.
    fn extract_builtin_metrics(
        &self,
        output_path: &Path,
    ) -> Result<Vec<(String, Value)>, AdapterError>;

    /// Which built-in event kinds this adapter can produce.
    ///
    /// Used by the brief/targets system to know which metrics are
    /// available. Metrics not in this list are simply skipped in
    /// dashboards rather than showing as errors.
    fn supported_metrics(&self) -> &[&str];

    /// Provide raw text lines for configurable extraction rules to scan.
    ///
    /// The adapter controls what "source" means for each source type.
    /// For example, the Claude adapter maps "tool_commands" to
    /// tool_use input.command fields, while the Codex adapter maps
    /// it to function_call arguments.
    fn lines_for_source(
        &self,
        output_path: &Path,
        source: ExtractionSource,
    ) -> Result<Vec<String>, AdapterError>;
}

/// Known adapter names returned by auto-detection.
const KNOWN_ADAPTERS: &[(&str, &str)] = &[
    ("claude", "claude"),
    ("codex", "codex"),
    ("opencode", "opencode"),
    ("aider", "aider"),
];

/// Detect the adapter name from the agent command.
///
/// Checks whether the command (basename only) contains a known agent name.
/// Falls back to "raw" if no known name matches.
pub fn detect_adapter_name(command: &str) -> &'static str {
    // Extract the basename (last path component) for matching
    let basename = command.rsplit('/').next().unwrap_or(command);
    for &(keyword, adapter_name) in KNOWN_ADAPTERS {
        if basename.contains(keyword) {
            return adapter_name;
        }
    }
    "raw"
}

/// Resolve the adapter name: use explicit override if provided, else auto-detect from command.
pub fn resolve_adapter_name(explicit: Option<&str>, command: &str) -> &'static str {
    if let Some(name) = explicit {
        // Return a 'static str for known names, or leak for unknown (config is loaded once)
        for &(_, adapter_name) in KNOWN_ADAPTERS {
            if name == adapter_name {
                return adapter_name;
            }
        }
        if name == "raw" {
            return "raw";
        }
        // Unknown adapter name — fall through to auto-detect
        // (validation should catch this before we get here)
        return detect_adapter_name(command);
    }
    detect_adapter_name(command)
}

/// Create an adapter instance for the given adapter name.
///
/// Returns a boxed trait object. Unknown names fall back to the raw adapter.
pub fn create_adapter(adapter_name: &str) -> Box<dyn AgentAdapter> {
    match adapter_name {
        "claude" => Box::new(claude::ClaudeAdapter::new()),
        "codex" => Box::new(codex::CodexAdapter::new()),
        _ => Box::new(raw::RawAdapter::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- detect_adapter_name tests ---

    #[test]
    fn test_detect_claude_from_bare_command() {
        assert_eq!(detect_adapter_name("claude"), "claude");
    }

    #[test]
    fn test_detect_claude_from_absolute_path() {
        assert_eq!(detect_adapter_name("/usr/local/bin/claude"), "claude");
    }

    #[test]
    fn test_detect_claude_from_variant_name() {
        // e.g. "claude-code" or "claude3" should still match
        assert_eq!(detect_adapter_name("claude-code"), "claude");
    }

    #[test]
    fn test_detect_codex() {
        assert_eq!(detect_adapter_name("codex"), "codex");
        assert_eq!(detect_adapter_name("/opt/bin/codex"), "codex");
    }

    #[test]
    fn test_detect_opencode() {
        assert_eq!(detect_adapter_name("opencode"), "opencode");
    }

    #[test]
    fn test_detect_aider() {
        assert_eq!(detect_adapter_name("aider"), "aider");
        assert_eq!(detect_adapter_name("/home/user/.local/bin/aider"), "aider");
    }

    #[test]
    fn test_detect_unknown_falls_back_to_raw() {
        assert_eq!(detect_adapter_name("my-custom-agent"), "raw");
        assert_eq!(detect_adapter_name("python"), "raw");
        assert_eq!(detect_adapter_name("/usr/bin/node"), "raw");
    }

    #[test]
    fn test_detect_empty_command_falls_back_to_raw() {
        assert_eq!(detect_adapter_name(""), "raw");
    }

    // --- resolve_adapter_name tests ---

    #[test]
    fn test_resolve_explicit_overrides_detection() {
        // Explicit "raw" even though command is "claude"
        assert_eq!(resolve_adapter_name(Some("raw"), "claude"), "raw");
    }

    #[test]
    fn test_resolve_explicit_known_adapter() {
        assert_eq!(resolve_adapter_name(Some("claude"), "anything"), "claude");
        assert_eq!(resolve_adapter_name(Some("codex"), "anything"), "codex");
        assert_eq!(resolve_adapter_name(Some("aider"), "anything"), "aider");
        assert_eq!(
            resolve_adapter_name(Some("opencode"), "anything"),
            "opencode"
        );
    }

    #[test]
    fn test_resolve_none_uses_auto_detect() {
        assert_eq!(resolve_adapter_name(None, "claude"), "claude");
        assert_eq!(resolve_adapter_name(None, "my-agent"), "raw");
    }

    #[test]
    fn test_resolve_unknown_explicit_falls_back_to_detect() {
        // Unknown adapter name "foo" — falls through to auto-detect from command
        assert_eq!(resolve_adapter_name(Some("foo"), "claude"), "claude");
        assert_eq!(resolve_adapter_name(Some("foo"), "my-agent"), "raw");
    }

    // --- create_adapter tests ---

    #[test]
    fn test_create_adapter_claude() {
        let adapter = create_adapter("claude");
        assert_eq!(adapter.name(), "claude");
    }

    #[test]
    fn test_create_adapter_raw() {
        let adapter = create_adapter("raw");
        assert_eq!(adapter.name(), "raw");
    }

    #[test]
    fn test_create_adapter_codex() {
        let adapter = create_adapter("codex");
        assert_eq!(adapter.name(), "codex");
    }

    #[test]
    fn test_create_adapter_unknown_falls_back_to_raw() {
        let adapter = create_adapter("unknown-agent");
        assert_eq!(adapter.name(), "raw");
    }

    // --- Integration: config -> adapter pipeline ---

    #[test]
    fn test_resolve_then_create_from_default_config() {
        let config = crate::config::AgentConfig::default();
        let name = resolve_adapter_name(config.adapter.as_deref(), &config.command);
        let adapter = create_adapter(name);
        // Default command is "claude", so should create claude adapter
        assert_eq!(adapter.name(), "claude");
    }

    #[test]
    fn test_resolve_then_create_with_explicit_override() {
        let config = crate::config::AgentConfig {
            command: "claude".to_string(),
            args: vec![],
            adapter: Some("raw".to_string()),
            ..Default::default()
        };
        let name = resolve_adapter_name(config.adapter.as_deref(), &config.command);
        let adapter = create_adapter(name);
        assert_eq!(adapter.name(), "raw");
    }
}
