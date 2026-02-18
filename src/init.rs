use std::io::BufRead;
use std::path::Path;
use std::process::{Command, Stdio};

use crate::config::command_in_path;

/// Profile for a supported AI coding agent.
#[derive(Debug, Clone)]
pub struct AgentProfile {
    pub name: &'static str,
    pub command: &'static str,
    pub args: Vec<String>,
    pub prompt_via: &'static str, // "arg", "stdin", or "file"
}

/// Return all known agent profiles in priority order.
pub fn agent_profiles() -> Vec<AgentProfile> {
    vec![
        AgentProfile {
            name: "claude",
            command: "claude",
            args: vec![
                "-p".into(),
                "{prompt}".into(),
                "--dangerously-skip-permissions".into(),
                "--verbose".into(),
                "--output-format".into(),
                "stream-json".into(),
            ],
            prompt_via: "arg",
        },
        AgentProfile {
            name: "codex",
            command: "codex",
            args: vec![
                "exec".into(),
                "--json".into(),
                "--full-auto".into(),
                "{prompt}".into(),
            ],
            prompt_via: "arg",
        },
        AgentProfile {
            name: "opencode",
            command: "opencode",
            args: vec!["run".into(), "{prompt}".into()],
            prompt_via: "arg",
        },
        AgentProfile {
            name: "aider",
            command: "aider",
            args: vec![
                "--message-file".into(),
                "{prompt_file}".into(),
                "--yes-always".into(),
                "--no-auto-commits".into(),
            ],
            prompt_via: "file",
        },
    ]
}

/// Detect the first available agent on PATH, in priority order.
/// Returns `None` if no known agent is found.
pub fn detect_agent() -> Option<AgentProfile> {
    agent_profiles()
        .into_iter()
        .find(|p| command_in_path(p.command))
}

/// Generate a complete config.toml string for the given project type and agent.
pub fn generate_config_toml(
    project_type: &ProjectType,
    agent: &AgentProfile,
    _commands: &[DetectedCommand],
) -> String {
    let args_toml: Vec<String> = agent.args.iter().map(|a| format!("\"{a}\"")).collect();
    let args_str = args_toml.join(", ");

    let (check, test, lint, format) = match project_type {
        ProjectType::Rust => (
            "\"cargo check --release\"",
            "\"cargo test --release\"",
            "\"cargo clippy --fix --allow-dirty\"",
            "\"cargo fmt --check\"",
        ),
        ProjectType::Node => (
            "\"npm run build\"",
            "\"npm test\"",
            "\"npm run lint\"",
            "[]",
        ),
        ProjectType::Python => (
            "[]",
            "\"pytest\"",
            "\"ruff check .\"",
            "\"ruff format --check .\"",
        ),
        ProjectType::Go => (
            "\"go build ./...\"",
            "\"go test ./...\"",
            "\"golangci-lint run\"",
            "[]",
        ),
        ProjectType::Unknown => ("[]", "[]", "[]", "[]"),
    };

    format!(
        r#"# Blacksmith configuration
# See documentation for all available options.

[agent]
command = "{command}"
args = [{args}]
prompt_via = "{prompt_via}"

[session]
max_iterations = 100

[storage]
compress_after = 5
retention = "last-50"

[workers]
max = 1

[quality_gates]
check = {check}
test = {test}
lint = {lint}
format = {format}
"#,
        command = agent.command,
        args = args_str,
        prompt_via = agent.prompt_via,
        check = check,
        test = test,
        lint = lint,
        format = format,
    )
}

/// Detected project type based on marker files.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectType {
    Rust,
    Node,
    Python,
    Go,
    Unknown,
}

/// A detected verification command with its purpose.
#[derive(Debug, Clone)]
pub struct DetectedCommand {
    pub label: &'static str,
    pub command: String,
}

/// Detect the project type by checking for marker files in `project_root`.
pub fn detect_project_type(project_root: &Path) -> ProjectType {
    if project_root.join("Cargo.toml").exists() {
        ProjectType::Rust
    } else if project_root.join("package.json").exists() {
        ProjectType::Node
    } else if project_root.join("pyproject.toml").exists()
        || project_root.join("setup.py").exists()
        || project_root.join("requirements.txt").exists()
    {
        ProjectType::Python
    } else if project_root.join("go.mod").exists() {
        ProjectType::Go
    } else {
        ProjectType::Unknown
    }
}

/// Return default verification commands for the given project type.
pub fn default_commands(project_type: &ProjectType) -> Vec<DetectedCommand> {
    match project_type {
        ProjectType::Rust => vec![
            DetectedCommand {
                label: "test",
                command: "cargo test --release".into(),
            },
            DetectedCommand {
                label: "lint",
                command: "cargo clippy --fix --allow-dirty".into(),
            },
            DetectedCommand {
                label: "format",
                command: "cargo fmt --check".into(),
            },
        ],
        ProjectType::Node => vec![
            DetectedCommand {
                label: "test",
                command: "npm test".into(),
            },
            DetectedCommand {
                label: "lint",
                command: "npm run lint".into(),
            },
            DetectedCommand {
                label: "build",
                command: "npm run build".into(),
            },
        ],
        ProjectType::Python => vec![
            DetectedCommand {
                label: "test",
                command: "pytest".into(),
            },
            DetectedCommand {
                label: "lint",
                command: "ruff check .".into(),
            },
            DetectedCommand {
                label: "format",
                command: "ruff format --check .".into(),
            },
        ],
        ProjectType::Go => vec![
            DetectedCommand {
                label: "test",
                command: "go test ./...".into(),
            },
            DetectedCommand {
                label: "lint",
                command: "golangci-lint run".into(),
            },
            DetectedCommand {
                label: "build",
                command: "go build ./...".into(),
            },
        ],
        ProjectType::Unknown => vec![],
    }
}

const PROMPT_TEMPLATE: &str = include_str!("../templates/PROMPT.md.tmpl");

/// Marker header present in every valid PROMPT.md, used to validate LLM output.
const PROMPT_MD_MARKER: &str = "# Task Execution Instructions";

/// Look up a command by label from the detected commands, returning a fallback if not found.
fn find_command<'a>(commands: &'a [DetectedCommand], label: &str, fallback: &'a str) -> &'a str {
    commands
        .iter()
        .find(|c| c.label == label)
        .map(|c| c.command.as_str())
        .unwrap_or(fallback)
}

/// Generate a PROMPT.md from the full template with verification commands populated.
pub fn generate_prompt_md(commands: &[DetectedCommand]) -> String {
    let test_cmd = find_command(commands, "test", "# TODO: add test command");
    let lint_cmd = find_command(commands, "lint", "# TODO: add lint command");
    let format_cmd = find_command(commands, "format", "# TODO: add format command");

    let mut verification_lines = String::new();
    if commands.is_empty() {
        verification_lines.push_str("# TODO: Add your verification commands here, for example:\n");
        verification_lines.push_str("# - test: npm test\n");
        verification_lines.push_str("# - lint: npm run lint\n");
    } else {
        for cmd in commands {
            verification_lines.push_str(&format!("- {}: `{}`\n", cmd.label, cmd.command));
        }
    }

    PROMPT_TEMPLATE
        .replace("{{test_command}}", test_cmd)
        .replace("{{lint_command}}", lint_cmd)
        .replace("{{format_command}}", format_cmd)
        .replace("{{verification_commands}}", verification_lines.trim_end())
}

/// Write PROMPT.md to `project_root` if it doesn't already exist.
/// Returns `true` if the file was created, `false` if it already existed.
pub fn write_prompt_md_if_missing(project_root: &Path, content: &str) -> std::io::Result<bool> {
    let path = project_root.join("PROMPT.md");
    if path.exists() {
        return Ok(false);
    }
    std::fs::write(&path, content)?;
    Ok(true)
}

const LLM_CUSTOMIZATION_PROMPT: &str = r#"You are customizing a PROMPT.md file for an AI coding agent managed by blacksmith.

The PROMPT.md at ./PROMPT.md contains a template with generic verification commands.
Your job is to analyze THIS specific project and update ONLY the verification/commands
sections to match the actual project setup.

Steps:
1. Read ./PROMPT.md (the current template)
2. Detect the project type and tooling:
   - For Node.js: Read package.json. Check for npm/yarn/pnpm/bun lock files.
     Find actual "test", "lint", "build", "format" scripts. Detect framework (Next.js, etc.)
   - For Python: Read pyproject.toml/setup.cfg. Check for poetry/pip/uv.
     Find test runner (pytest/unittest), linter (ruff/flake8/pylint), formatter (black/ruff)
   - For Rust: Read Cargo.toml. Standard cargo commands are usually correct.
   - For Go: Read go.mod. Standard go commands are usually correct.
3. Update the Verification section (step 4b in the Execution Protocol) with the
   actual commands for this project
4. Update any inline command examples that reference test/lint/format commands
5. Do NOT change the structure, workflow logic, turn budgets, or bead protocol sections

Output the complete updated PROMPT.md file content, nothing else."#;

/// Run `claude -p` to customize PROMPT.md for the specific project.
/// Returns `Ok(true)` if customization succeeded, `Ok(false)` if claude was not available.
pub fn customize_prompt_with_llm(project_root: &Path) -> Result<bool, std::io::Error> {
    // Check if `claude` is in PATH
    let which_result = Command::new("which").arg("claude").output();
    match which_result {
        Ok(output) if output.status.success() => {}
        _ => {
            return Ok(false);
        }
    }

    // Save original content so we can restore it if the agent produces invalid output
    let prompt_path = project_root.join("PROMPT.md");
    let original_content = std::fs::read_to_string(&prompt_path)?;

    eprintln!("Customizing PROMPT.md with Claude (this may take a moment)...");

    let mut child = Command::new("claude")
        .arg("-p")
        .arg(LLM_CUSTOMIZATION_PROMPT)
        .arg("--dangerously-skip-permissions")
        .arg("--allowedTools")
        .arg("Read Glob Grep")
        .arg("--verbose")
        .arg("--output-format")
        .arg("stream-json")
        .current_dir(project_root)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit()) // let user see progress
        .spawn()?;

    // Read streaming JSONL from stdout, extract the final result text
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| std::io::Error::other("failed to capture stdout"))?;
    let reader = std::io::BufReader::new(stdout);

    let mut result_text: Option<String> = None;
    let mut saw_generating = false;
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        // Parse each JSONL event; print progress to stderr, capture final result
        if let Ok(obj) = serde_json::from_str::<serde_json::Value>(&line) {
            match obj.get("type").and_then(|t| t.as_str()) {
                Some("assistant") => {
                    if let Some(content) = obj
                        .get("message")
                        .and_then(|m| m.get("content"))
                        .and_then(|c| c.as_array())
                    {
                        let mut has_tool = false;
                        let mut has_text = false;
                        for block in content {
                            match block.get("type").and_then(|t| t.as_str()) {
                                Some("tool_use") => {
                                    has_tool = true;
                                    if let Some(name) = block.get("name").and_then(|n| n.as_str()) {
                                        eprint!("  → {name}");
                                        if let Some(input) = block.get("input") {
                                            if let Some(path) = input
                                                .get("file_path")
                                                .or_else(|| input.get("pattern"))
                                                .and_then(|p| p.as_str())
                                            {
                                                eprint!(" {path}");
                                            }
                                        }
                                        eprintln!();
                                    }
                                }
                                Some("text") => {
                                    has_text = true;
                                }
                                _ => {}
                            }
                        }
                        // When assistant produces text without tools, it's generating the final output
                        if has_text && !has_tool && !saw_generating {
                            eprintln!("  → Generating customized PROMPT.md...");
                            saw_generating = true;
                        }
                    }
                }
                Some("result") => {
                    if let Some(text) = obj.get("result").and_then(|r| r.as_str()) {
                        result_text = Some(text.to_string());
                    }
                }
                _ => {}
            }
        }
    }

    let status = child.wait()?;
    if !status.success() {
        return Err(std::io::Error::other(
            "claude -p exited with non-zero status",
        ));
    }

    apply_llm_prompt_result(&prompt_path, &original_content, result_text.as_deref())?;

    Ok(true)
}

/// Validate and apply the LLM result to PROMPT.md.
///
/// If `result_text` contains the PROMPT.md marker, it's used as the new content.
/// If not, the on-disk file is checked (agent may have edited in-place).
/// If neither is valid, the original content is restored and an error is returned.
fn apply_llm_prompt_result(
    prompt_path: &Path,
    original_content: &str,
    result_text: Option<&str>,
) -> Result<(), std::io::Error> {
    let trimmed = result_text.map(|s| s.trim()).unwrap_or("");

    if trimmed.contains(PROMPT_MD_MARKER) {
        // Agent output complete PROMPT.md content — use it
        std::fs::write(prompt_path, trimmed)?;
    } else {
        // Agent likely edited in-place; verify the on-disk file is valid
        let on_disk = std::fs::read_to_string(prompt_path).unwrap_or_default();
        if !on_disk.contains(PROMPT_MD_MARKER) {
            // Neither result nor file is valid — restore original and report failure
            std::fs::write(prompt_path, original_content)?;
            return Err(std::io::Error::other(
                "claude -p did not produce valid PROMPT.md content",
            ));
        }
        // on-disk file is valid — agent edited in-place, nothing more to do
    }

    Ok(())
}

/// Format a guidance message for the user after init.
pub fn guidance_message(
    project_type: &ProjectType,
    commands: &[DetectedCommand],
    prompt_created: bool,
    llm_customized: bool,
    agent: Option<&AgentProfile>,
) -> String {
    let mut msg = String::new();

    if let Some(agent) = agent {
        msg.push_str(&format!("Detected agent: {}\n", agent.name));
    } else {
        msg.push_str("No supported agent found on PATH — defaulting to claude profile.\n");
        msg.push_str("Install an agent (claude, codex, opencode, aider) and edit config.toml.\n");
    }

    if prompt_created {
        msg.push('\n');

        let type_name = match project_type {
            ProjectType::Rust => "Rust (Cargo)",
            ProjectType::Node => "Node.js (npm)",
            ProjectType::Python => "Python",
            ProjectType::Go => "Go",
            ProjectType::Unknown => "unknown",
        };

        if *project_type != ProjectType::Unknown {
            msg.push_str(&format!("Detected project type: {type_name}\n"));
            msg.push_str("Auto-populated verification commands:\n");
            for cmd in commands {
                msg.push_str(&format!("  - {}: {}\n", cmd.label, cmd.command));
            }
            msg.push('\n');
        }

        if llm_customized {
            msg.push_str(
                "PROMPT.md was auto-customized for your project using Claude.\n\
                 Review it to ensure the detected commands are correct.\n",
            );
        } else {
            msg.push_str(
                "PROMPT.md created with defaults — customize it for your project.\n\
                 Tip: install Claude CLI (`claude`) to auto-customize PROMPT.md on init.\n",
            );
        }
    } else {
        msg.push_str("\nPROMPT.md already exists — skipping generation.\n");
    }

    msg
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn detect_rust_project() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("Cargo.toml"), "[package]").unwrap();
        assert_eq!(detect_project_type(dir.path()), ProjectType::Rust);
    }

    #[test]
    fn detect_node_project() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("package.json"), "{}").unwrap();
        assert_eq!(detect_project_type(dir.path()), ProjectType::Node);
    }

    #[test]
    fn detect_python_project_pyproject() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("pyproject.toml"), "").unwrap();
        assert_eq!(detect_project_type(dir.path()), ProjectType::Python);
    }

    #[test]
    fn detect_python_project_requirements() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("requirements.txt"), "").unwrap();
        assert_eq!(detect_project_type(dir.path()), ProjectType::Python);
    }

    #[test]
    fn detect_go_project() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("go.mod"), "module foo").unwrap();
        assert_eq!(detect_project_type(dir.path()), ProjectType::Go);
    }

    #[test]
    fn detect_unknown_project() {
        let dir = TempDir::new().unwrap();
        assert_eq!(detect_project_type(dir.path()), ProjectType::Unknown);
    }

    #[test]
    fn rust_takes_priority_over_node() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("Cargo.toml"), "").unwrap();
        std::fs::write(dir.path().join("package.json"), "").unwrap();
        assert_eq!(detect_project_type(dir.path()), ProjectType::Rust);
    }

    #[test]
    fn default_commands_rust() {
        let cmds = default_commands(&ProjectType::Rust);
        assert_eq!(cmds.len(), 3);
        assert_eq!(cmds[0].label, "test");
        assert!(cmds[0].command.contains("cargo test"));
    }

    #[test]
    fn default_commands_unknown_is_empty() {
        let cmds = default_commands(&ProjectType::Unknown);
        assert!(cmds.is_empty());
    }

    #[test]
    fn generate_prompt_md_with_commands() {
        let cmds = default_commands(&ProjectType::Rust);
        let content = generate_prompt_md(&cmds);
        assert!(content.contains("cargo test --release"));
        assert!(content.contains("cargo clippy"));
        assert!(content.contains("cargo fmt --check"));
        assert!(content.contains("## Verification"));
        // Full template sections should be present
        assert!(content.contains("# Task Execution Instructions"));
        assert!(content.contains("## Execution Protocol"));
        assert!(content.contains("## Turn Budget"));
        assert!(content.contains("## Improvement Recording"));
        assert!(content.contains("blacksmith finish"));
    }

    #[test]
    fn generate_prompt_md_without_commands() {
        let content = generate_prompt_md(&[]);
        assert!(content.contains("TODO: Add your verification commands"));
        assert!(content.contains("TODO: add test command"));
        assert!(content.contains("TODO: add lint command"));
    }

    #[test]
    fn generate_prompt_md_node_commands() {
        let cmds = default_commands(&ProjectType::Node);
        let content = generate_prompt_md(&cmds);
        assert!(content.contains("npm test"));
        assert!(content.contains("npm run lint"));
        // Node has "build" not "format", so format should fall back to TODO
        assert!(content.contains("TODO: add format command"));
    }

    #[test]
    fn write_prompt_md_creates_file() {
        let dir = TempDir::new().unwrap();
        let created = write_prompt_md_if_missing(dir.path(), "test content").unwrap();
        assert!(created);
        let content = std::fs::read_to_string(dir.path().join("PROMPT.md")).unwrap();
        assert_eq!(content, "test content");
    }

    #[test]
    fn write_prompt_md_skips_existing() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("PROMPT.md"), "existing").unwrap();
        let created = write_prompt_md_if_missing(dir.path(), "new content").unwrap();
        assert!(!created);
        let content = std::fs::read_to_string(dir.path().join("PROMPT.md")).unwrap();
        assert_eq!(content, "existing");
    }

    #[test]
    fn guidance_message_with_detected_commands_no_llm() {
        let cmds = default_commands(&ProjectType::Rust);
        let agent = agent_profiles().into_iter().find(|a| a.name == "claude");
        let msg = guidance_message(&ProjectType::Rust, &cmds, true, false, agent.as_ref());
        assert!(msg.contains("Detected project type: Rust (Cargo)"));
        assert!(msg.contains("cargo test --release"));
        assert!(msg.contains("created with defaults"));
        assert!(msg.contains("install Claude CLI"));
        assert!(msg.contains("Detected agent: claude"));
    }

    #[test]
    fn guidance_message_with_llm_customized() {
        let cmds = default_commands(&ProjectType::Rust);
        let agent = agent_profiles().into_iter().find(|a| a.name == "claude");
        let msg = guidance_message(&ProjectType::Rust, &cmds, true, true, agent.as_ref());
        assert!(msg.contains("Detected project type: Rust (Cargo)"));
        assert!(msg.contains("auto-customized"));
        assert!(!msg.contains("created with defaults"));
    }

    #[test]
    fn guidance_message_unknown_project() {
        let msg = guidance_message(&ProjectType::Unknown, &[], true, false, None);
        assert!(!msg.contains("Detected project type"));
        assert!(msg.contains("created with defaults"));
        assert!(msg.contains("No supported agent found"));
    }

    #[test]
    fn guidance_message_existing_prompt() {
        let msg = guidance_message(&ProjectType::Rust, &[], false, false, None);
        assert!(msg.contains("already exists"));
        assert!(!msg.contains("created with defaults"));
    }

    #[test]
    fn test_agent_profiles_all_unique_names() {
        let profiles = agent_profiles();
        let mut names: Vec<&str> = profiles.iter().map(|p| p.name).collect();
        let original_len = names.len();
        names.sort();
        names.dedup();
        assert_eq!(
            names.len(),
            original_len,
            "agent profile names must be unique"
        );
    }

    #[test]
    fn test_generate_config_toml_rust_claude() {
        let agent = agent_profiles()
            .into_iter()
            .find(|a| a.name == "claude")
            .unwrap();
        let cmds = default_commands(&ProjectType::Rust);
        let toml = generate_config_toml(&ProjectType::Rust, &agent, &cmds);
        assert!(toml.contains("command = \"claude\""));
        assert!(toml.contains("\"--dangerously-skip-permissions\""));
        assert!(toml.contains("\"--verbose\""));
        assert!(toml.contains("prompt_via = \"arg\""));
        assert!(toml.contains("check = \"cargo check --release\""));
        assert!(toml.contains("test = \"cargo test --release\""));
        assert!(toml.contains("lint = \"cargo clippy --fix --allow-dirty\""));
        assert!(toml.contains("format = \"cargo fmt --check\""));
        assert!(toml.contains("[workers]"));
        assert!(toml.contains("max = 1"));
    }

    #[test]
    fn test_generate_config_toml_node_claude() {
        let agent = agent_profiles()
            .into_iter()
            .find(|a| a.name == "claude")
            .unwrap();
        let cmds = default_commands(&ProjectType::Node);
        let toml = generate_config_toml(&ProjectType::Node, &agent, &cmds);
        assert!(toml.contains("check = \"npm run build\""));
        assert!(toml.contains("test = \"npm test\""));
        assert!(toml.contains("lint = \"npm run lint\""));
        assert!(toml.contains("format = []"));
    }

    #[test]
    fn test_generate_config_toml_unknown_project() {
        let agent = agent_profiles()
            .into_iter()
            .find(|a| a.name == "claude")
            .unwrap();
        let toml = generate_config_toml(&ProjectType::Unknown, &agent, &[]);
        assert!(toml.contains("check = []"));
        assert!(toml.contains("test = []"));
        assert!(toml.contains("lint = []"));
        assert!(toml.contains("format = []"));
    }

    #[test]
    fn test_generate_config_toml_aider() {
        let agent = agent_profiles()
            .into_iter()
            .find(|a| a.name == "aider")
            .unwrap();
        let cmds = default_commands(&ProjectType::Rust);
        let toml = generate_config_toml(&ProjectType::Rust, &agent, &cmds);
        assert!(toml.contains("command = \"aider\""));
        assert!(toml.contains("prompt_via = \"file\""));
        assert!(toml.contains("\"--message-file\""));
        assert!(toml.contains("\"--yes-always\""));
    }

    #[test]
    fn test_apply_llm_prompt_result_valid_output() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("PROMPT.md");
        let original = "original content";
        std::fs::write(&path, original).unwrap();

        let valid_output = "# Task Execution Instructions\n\nCustomized content here";
        apply_llm_prompt_result(&path, original, Some(valid_output)).unwrap();

        let result = std::fs::read_to_string(&path).unwrap();
        assert!(result.contains("# Task Execution Instructions"));
        assert!(result.contains("Customized content here"));
    }

    #[test]
    fn test_apply_llm_prompt_result_agent_edited_in_place() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("PROMPT.md");
        let original = "original content";
        // Simulate agent editing the file in-place with valid content
        let on_disk = "# Task Execution Instructions\n\nEdited in place";
        std::fs::write(&path, on_disk).unwrap();

        // Agent output is just a summary, not valid PROMPT.md
        let summary = "I updated the verification commands in PROMPT.md.";
        apply_llm_prompt_result(&path, original, Some(summary)).unwrap();

        // File should remain as the agent edited it
        let result = std::fs::read_to_string(&path).unwrap();
        assert_eq!(result, on_disk);
    }

    #[test]
    fn test_apply_llm_prompt_result_neither_valid_restores_original() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("PROMPT.md");
        let original = "original valid content";
        // On-disk was corrupted by the agent
        std::fs::write(&path, "garbage content").unwrap();

        // Agent output is also not valid
        let summary = "Done!";
        let err = apply_llm_prompt_result(&path, original, Some(summary)).unwrap_err();
        assert!(err.to_string().contains("did not produce valid PROMPT.md"));

        // Original content should be restored
        let result = std::fs::read_to_string(&path).unwrap();
        assert_eq!(result, original);
    }

    #[test]
    fn test_apply_llm_prompt_result_none_with_valid_disk() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("PROMPT.md");
        let original = "original content";
        let on_disk = "# Task Execution Instructions\n\nValid on disk";
        std::fs::write(&path, on_disk).unwrap();

        // No result text at all
        apply_llm_prompt_result(&path, original, None).unwrap();

        // File should remain valid
        let result = std::fs::read_to_string(&path).unwrap();
        assert_eq!(result, on_disk);
    }

    #[test]
    fn test_apply_llm_prompt_result_empty_with_invalid_disk() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("PROMPT.md");
        let original = "original content";
        std::fs::write(&path, "broken").unwrap();

        // Empty result text
        let err = apply_llm_prompt_result(&path, original, Some("")).unwrap_err();
        assert!(err.to_string().contains("did not produce valid PROMPT.md"));

        // Original content should be restored
        let result = std::fs::read_to_string(&path).unwrap();
        assert_eq!(result, original);
    }
}
