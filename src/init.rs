use std::path::Path;
use std::process::{Command, Stdio};

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

    eprintln!("Customizing PROMPT.md with Claude (this may take a moment)...");

    let child = Command::new("claude")
        .arg("-p")
        .arg(LLM_CUSTOMIZATION_PROMPT)
        .arg("--verbose")
        .arg("--output-format")
        .arg("text")
        .current_dir(project_root)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit()) // let user see progress
        .spawn()?;

    let output = child.wait_with_output()?;

    if !output.status.success() {
        return Err(std::io::Error::other(
            "claude -p exited with non-zero status",
        ));
    }

    let customized = String::from_utf8_lossy(&output.stdout);
    let trimmed = customized.trim();
    if trimmed.is_empty() {
        return Err(std::io::Error::other("claude -p returned empty output"));
    }

    // Overwrite PROMPT.md with the customized content
    let prompt_path = project_root.join("PROMPT.md");
    std::fs::write(&prompt_path, trimmed)?;

    Ok(true)
}

/// Format a guidance message for the user after init.
pub fn guidance_message(
    project_type: &ProjectType,
    commands: &[DetectedCommand],
    prompt_created: bool,
    llm_customized: bool,
) -> String {
    let mut msg = String::new();

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
        let msg = guidance_message(&ProjectType::Rust, &cmds, true, false);
        assert!(msg.contains("Detected project type: Rust (Cargo)"));
        assert!(msg.contains("cargo test --release"));
        assert!(msg.contains("created with defaults"));
        assert!(msg.contains("install Claude CLI"));
    }

    #[test]
    fn guidance_message_with_llm_customized() {
        let cmds = default_commands(&ProjectType::Rust);
        let msg = guidance_message(&ProjectType::Rust, &cmds, true, true);
        assert!(msg.contains("Detected project type: Rust (Cargo)"));
        assert!(msg.contains("auto-customized"));
        assert!(!msg.contains("created with defaults"));
    }

    #[test]
    fn guidance_message_unknown_project() {
        let msg = guidance_message(&ProjectType::Unknown, &[], true, false);
        assert!(!msg.contains("Detected project type"));
        assert!(msg.contains("created with defaults"));
    }

    #[test]
    fn guidance_message_existing_prompt() {
        let msg = guidance_message(&ProjectType::Rust, &[], false, false);
        assert!(msg.contains("already exists"));
        assert!(!msg.contains("created with defaults"));
    }
}
