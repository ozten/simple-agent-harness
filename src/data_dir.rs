use crate::defaults;
use std::path::{Path, PathBuf};

/// Manages the `.blacksmith/` directory layout.
///
/// All blacksmith artifacts live under a single data directory (default `.blacksmith/`).
/// This struct provides accessors for each well-known path and handles initialization.
#[derive(Debug, Clone)]
pub struct DataDir {
    root: PathBuf,
}

impl DataDir {
    /// Create a new DataDir referencing the given root path.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// The root directory (e.g. `.blacksmith/`).
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Path to the SQLite database.
    pub fn db(&self) -> PathBuf {
        self.root.join("blacksmith.db")
    }

    /// Path to the harness status file.
    pub fn status(&self) -> PathBuf {
        self.root.join("status")
    }

    /// Path to the global iteration counter file.
    pub fn counter(&self) -> PathBuf {
        self.root.join("counter")
    }

    /// Path to the sessions directory.
    pub fn sessions_dir(&self) -> PathBuf {
        self.root.join("sessions")
    }

    /// Path to the worktrees directory.
    pub fn worktrees_dir(&self) -> PathBuf {
        self.root.join("worktrees")
    }

    /// Path to the singleton lock file.
    pub fn lock(&self) -> PathBuf {
        self.root.join("lock")
    }

    /// Path to a specific session file (e.g. `sessions/42.jsonl`).
    pub fn session_file(&self, iteration: u32) -> PathBuf {
        self.sessions_dir().join(format!("{iteration}.jsonl"))
    }

    /// Path to the skills directory (e.g. `.blacksmith/skills/`).
    pub fn skills_dir(&self) -> PathBuf {
        self.root.join("skills")
    }

    /// Path to the PROMPT.md file (e.g. `.blacksmith/PROMPT.md`).
    pub fn prompt_file(&self) -> PathBuf {
        self.root.join("PROMPT.md")
    }

    /// Initialize the full directory structure.
    /// Creates root, sessions/, and worktrees/ directories.
    /// Returns Ok(true) if directories were created, Ok(false) if they already existed.
    pub fn init(&self) -> std::io::Result<bool> {
        let created = !self.root.exists();
        std::fs::create_dir_all(&self.root)?;
        std::fs::create_dir_all(self.sessions_dir())?;
        std::fs::create_dir_all(self.worktrees_dir())?;
        Ok(created)
    }

    /// Ensure the data directory is initialized, creating it if missing.
    /// Extracts embedded default files on first run (never overwrites existing).
    /// Also appends the data_dir to .gitignore if a .gitignore exists
    /// and doesn't already contain the entry.
    pub fn ensure_initialized(&self) -> std::io::Result<()> {
        self.init()?;
        self.extract_defaults()?;
        self.update_gitignore()?;
        Ok(())
    }

    /// Default files to extract: (relative path within data dir, content).
    fn default_files() -> Vec<(&'static str, &'static str)> {
        vec![
            ("PROMPT.md", defaults::PROMPT_MD),
            ("blacksmith.toml", defaults::DEFAULT_CONFIG),
            ("bd-finish.sh", defaults::FINISH_SCRIPT),
            ("skills/prd-to-beads.md", defaults::SKILL_PRD_TO_BEADS),
            ("skills/break-down-issue.md", defaults::SKILL_BREAK_DOWN),
            ("skills/self-improvement.md", defaults::SKILL_SELF_IMPROVE),
        ]
    }

    /// Extract embedded default files into the data directory.
    /// Only writes files that don't already exist (never overwrites).
    fn extract_defaults(&self) -> std::io::Result<()> {
        std::fs::create_dir_all(self.skills_dir())?;

        for (rel_path, content) in Self::default_files() {
            let path = self.root.join(rel_path);
            if !path.exists() {
                std::fs::write(&path, content)?;
            }
        }
        Ok(())
    }

    /// Force-extract embedded defaults, overwriting existing files.
    /// Returns the list of files that were overwritten (already existed).
    pub fn force_extract(&self) -> std::io::Result<Vec<PathBuf>> {
        std::fs::create_dir_all(self.skills_dir())?;

        let mut overwritten = Vec::new();
        for (rel_path, content) in Self::default_files() {
            let path = self.root.join(rel_path);
            if path.exists() {
                overwritten.push(path.clone());
            }
            std::fs::write(&path, content)?;
        }
        Ok(overwritten)
    }

    /// Export PROMPT.md and skills/ to the project root (parent of the data dir).
    /// Returns the list of files exported.
    pub fn export_to_root(&self) -> std::io::Result<Vec<PathBuf>> {
        let project_root = self.root.parent().unwrap_or_else(|| Path::new("."));
        let mut exported = Vec::new();

        // Export PROMPT.md
        let prompt_src = self.prompt_file();
        if prompt_src.exists() {
            let dest = project_root.join("PROMPT.md");
            std::fs::copy(&prompt_src, &dest)?;
            exported.push(dest);
        }

        // Export skills/
        let skills_src = self.skills_dir();
        if skills_src.exists() {
            let dest_dir = project_root.join("skills");
            std::fs::create_dir_all(&dest_dir)?;

            for entry in std::fs::read_dir(&skills_src)? {
                let entry = entry?;
                if entry.file_type()?.is_file() {
                    let dest = dest_dir.join(entry.file_name());
                    std::fs::copy(entry.path(), &dest)?;
                    exported.push(dest);
                }
            }
        }

        Ok(exported)
    }

    /// Append the data directory path to .gitignore if:
    /// 1. A .gitignore file exists in the current directory or parent of data_dir
    /// 2. It doesn't already contain the entry
    fn update_gitignore(&self) -> std::io::Result<()> {
        // Determine where .gitignore should be (parent of data_dir, or cwd)
        let gitignore_dir = self.root.parent().unwrap_or_else(|| Path::new("."));
        let gitignore_path = gitignore_dir.join(".gitignore");

        // Get the directory name to add to .gitignore
        let dir_name = self
            .root
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| self.root.to_string_lossy().to_string());
        let entry = format!("{dir_name}/");

        if gitignore_path.exists() {
            let contents = std::fs::read_to_string(&gitignore_path)?;
            // Check if already present (exact line match)
            let already_present = contents.lines().any(|line| {
                let trimmed = line.trim();
                trimmed == entry || trimmed == dir_name
            });
            if !already_present {
                // Append with a newline separator if file doesn't end with one
                let prefix = if contents.ends_with('\n') || contents.is_empty() {
                    ""
                } else {
                    "\n"
                };
                let mut file = std::fs::OpenOptions::new()
                    .append(true)
                    .open(&gitignore_path)?;
                use std::io::Write;
                writeln!(file, "{prefix}{entry}")?;
            }
        }
        // If no .gitignore exists, don't create one
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_dir_paths() {
        let dd = DataDir::new(".blacksmith");
        assert_eq!(dd.root(), Path::new(".blacksmith"));
        assert_eq!(dd.db(), PathBuf::from(".blacksmith/blacksmith.db"));
        assert_eq!(dd.status(), PathBuf::from(".blacksmith/status"));
        assert_eq!(dd.counter(), PathBuf::from(".blacksmith/counter"));
        assert_eq!(dd.sessions_dir(), PathBuf::from(".blacksmith/sessions"));
        assert_eq!(dd.worktrees_dir(), PathBuf::from(".blacksmith/worktrees"));
        assert_eq!(dd.skills_dir(), PathBuf::from(".blacksmith/skills"));
        assert_eq!(dd.prompt_file(), PathBuf::from(".blacksmith/PROMPT.md"));
        assert_eq!(
            dd.session_file(42),
            PathBuf::from(".blacksmith/sessions/42.jsonl")
        );
    }

    #[test]
    fn test_init_creates_directories() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join(".blacksmith");
        let dd = DataDir::new(&root);

        assert!(!root.exists());
        let created = dd.init().unwrap();
        assert!(created);
        assert!(root.exists());
        assert!(dd.sessions_dir().exists());
        assert!(dd.worktrees_dir().exists());
    }

    #[test]
    fn test_init_idempotent() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join(".blacksmith");
        let dd = DataDir::new(&root);

        let created1 = dd.init().unwrap();
        assert!(created1);
        let created2 = dd.init().unwrap();
        assert!(!created2);
    }

    #[test]
    fn test_ensure_initialized_creates_and_updates_gitignore() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join(".blacksmith");
        let gitignore = tmp.path().join(".gitignore");

        // Create a .gitignore with existing content
        std::fs::write(&gitignore, "node_modules/\n").unwrap();

        let dd = DataDir::new(&root);
        dd.ensure_initialized().unwrap();

        assert!(root.exists());
        let contents = std::fs::read_to_string(&gitignore).unwrap();
        assert!(contents.contains(".blacksmith/"));
        assert!(contents.contains("node_modules/"));
    }

    #[test]
    fn test_gitignore_not_duplicated() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join(".blacksmith");
        let gitignore = tmp.path().join(".gitignore");

        std::fs::write(&gitignore, ".blacksmith/\n").unwrap();

        let dd = DataDir::new(&root);
        dd.ensure_initialized().unwrap();

        let contents = std::fs::read_to_string(&gitignore).unwrap();
        assert_eq!(contents.matches(".blacksmith/").count(), 1);
    }

    #[test]
    fn test_gitignore_not_created_if_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join(".blacksmith");
        let gitignore = tmp.path().join(".gitignore");

        let dd = DataDir::new(&root);
        dd.ensure_initialized().unwrap();

        assert!(!gitignore.exists());
    }

    #[test]
    fn test_gitignore_append_no_trailing_newline() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join(".blacksmith");
        let gitignore = tmp.path().join(".gitignore");

        // Write without trailing newline
        std::fs::write(&gitignore, "node_modules/").unwrap();

        let dd = DataDir::new(&root);
        dd.ensure_initialized().unwrap();

        let contents = std::fs::read_to_string(&gitignore).unwrap();
        assert_eq!(contents, "node_modules/\n.blacksmith/\n");
    }

    #[test]
    fn test_first_run_extracts_defaults() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join(".blacksmith");
        let dd = DataDir::new(&root);

        dd.ensure_initialized().unwrap();

        // All default files should exist
        assert!(dd.prompt_file().exists(), "PROMPT.md should be extracted");
        assert!(
            root.join("blacksmith.toml").exists(),
            "blacksmith.toml should be extracted"
        );
        assert!(
            root.join("bd-finish.sh").exists(),
            "bd-finish.sh should be extracted"
        );
        assert!(dd.skills_dir().exists(), "skills/ dir should exist");
        assert!(dd.skills_dir().join("prd-to-beads.md").exists());
        assert!(dd.skills_dir().join("break-down-issue.md").exists());
        assert!(dd.skills_dir().join("self-improvement.md").exists());

        // Verify content is non-empty
        let prompt = std::fs::read_to_string(dd.prompt_file()).unwrap();
        assert!(!prompt.is_empty());
        assert!(prompt.contains("Task Execution"));
    }

    #[test]
    fn test_existing_files_not_overwritten() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join(".blacksmith");
        let dd = DataDir::new(&root);

        // First init to create defaults
        dd.ensure_initialized().unwrap();

        // Overwrite PROMPT.md with custom content
        let custom = "My custom prompt";
        std::fs::write(dd.prompt_file(), custom).unwrap();

        // Re-init should not overwrite
        dd.ensure_initialized().unwrap();

        let content = std::fs::read_to_string(dd.prompt_file()).unwrap();
        assert_eq!(content, custom, "Custom content should survive re-init");
    }

    #[test]
    fn test_force_extract_overwrites() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join(".blacksmith");
        let dd = DataDir::new(&root);

        dd.ensure_initialized().unwrap();

        // Write custom content
        let custom = "My custom prompt";
        std::fs::write(dd.prompt_file(), custom).unwrap();

        // Force extract should overwrite
        let overwritten = dd.force_extract().unwrap();
        assert!(!overwritten.is_empty(), "Should report overwritten files");
        assert!(overwritten.contains(&dd.prompt_file()));

        let content = std::fs::read_to_string(dd.prompt_file()).unwrap();
        assert_ne!(
            content, custom,
            "Force extract should overwrite custom content"
        );
        assert!(content.contains("Task Execution"));
    }

    #[test]
    fn test_export_to_root() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join(".blacksmith");
        let dd = DataDir::new(&root);

        dd.ensure_initialized().unwrap();

        let exported = dd.export_to_root().unwrap();
        assert!(!exported.is_empty());

        // PROMPT.md should be in project root
        let prompt_in_root = tmp.path().join("PROMPT.md");
        assert!(
            prompt_in_root.exists(),
            "PROMPT.md should be exported to project root"
        );

        // Skills should be in project root/skills/
        let skills_in_root = tmp.path().join("skills");
        assert!(
            skills_in_root.exists(),
            "skills/ should be exported to project root"
        );
        assert!(skills_in_root.join("prd-to-beads.md").exists());
        assert!(skills_in_root.join("break-down-issue.md").exists());
        assert!(skills_in_root.join("self-improvement.md").exists());
    }
}
