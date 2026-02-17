use std::path::Path;

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

/// Generate a PROMPT.md template with verification commands populated.
pub fn generate_prompt_md(commands: &[DetectedCommand]) -> String {
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

    format!(
        "# Agent Instructions\n\
         \n\
         ## Verification\n\
         \n\
         Before closing a task, run these commands and ensure they pass:\n\
         \n\
         {verification_lines}\
         \n\
         ## Guidelines\n\
         \n\
         - Make small, focused changes\n\
         - Run verification commands before committing\n\
         - Follow existing code patterns and conventions\n"
    )
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

/// Format a guidance message for the user after init.
pub fn guidance_message(
    project_type: &ProjectType,
    commands: &[DetectedCommand],
    prompt_created: bool,
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

        msg.push_str(
            "Please READ and CUSTOMIZE PROMPT.md — review the verification section \
             and add any additional quality guards relevant to your project.\n",
        );
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
        assert!(content.contains("## Verification"));
    }

    #[test]
    fn generate_prompt_md_without_commands() {
        let content = generate_prompt_md(&[]);
        assert!(content.contains("TODO: Add your verification commands"));
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
    fn guidance_message_with_detected_commands() {
        let cmds = default_commands(&ProjectType::Rust);
        let msg = guidance_message(&ProjectType::Rust, &cmds, true);
        assert!(msg.contains("Detected project type: Rust (Cargo)"));
        assert!(msg.contains("cargo test --release"));
        assert!(msg.contains("READ and CUSTOMIZE PROMPT.md"));
    }

    #[test]
    fn guidance_message_unknown_project() {
        let msg = guidance_message(&ProjectType::Unknown, &[], true);
        assert!(!msg.contains("Detected project type"));
        assert!(msg.contains("READ and CUSTOMIZE PROMPT.md"));
    }

    #[test]
    fn guidance_message_existing_prompt() {
        let msg = guidance_message(&ProjectType::Rust, &[], false);
        assert!(msg.contains("already exists"));
        assert!(!msg.contains("READ and CUSTOMIZE"));
    }
}
