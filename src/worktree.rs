use std::path::{Path, PathBuf};
use std::process::Command;

/// Errors that can occur during worktree operations.
#[derive(Debug)]
pub enum WorktreeError {
    /// The worktree path already exists on disk.
    AlreadyExists(PathBuf),
    /// The specified branch does not exist in the repository.
    BranchNotFound(String),
    /// A git command failed.
    GitError(String),
    /// An I/O error occurred.
    Io(std::io::Error),
}

impl std::fmt::Display for WorktreeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorktreeError::AlreadyExists(p) => {
                write!(f, "worktree already exists: {}", p.display())
            }
            WorktreeError::BranchNotFound(b) => write!(f, "branch not found: {b}"),
            WorktreeError::GitError(msg) => write!(f, "git error: {msg}"),
            WorktreeError::Io(e) => write!(f, "I/O error: {e}"),
        }
    }
}

impl std::error::Error for WorktreeError {}

impl From<std::io::Error> for WorktreeError {
    fn from(e: std::io::Error) -> Self {
        WorktreeError::Io(e)
    }
}

/// Build the worktree directory name for a given worker and bead.
///
/// Format: `worker-{worker_id}-{bead_id}`
pub fn worktree_name(worker_id: u32, bead_id: &str) -> String {
    format!("worker-{worker_id}-{bead_id}")
}

/// Build the full worktree path under the given worktrees directory.
pub fn worktree_path(worktrees_dir: &Path, worker_id: u32, bead_id: &str) -> PathBuf {
    worktrees_dir.join(worktree_name(worker_id, bead_id))
}

/// Check whether a git branch exists in the repository at `repo_dir`.
pub fn branch_exists(repo_dir: &Path, branch: &str) -> Result<bool, WorktreeError> {
    let output = Command::new("git")
        .args(["rev-parse", "--verify", &format!("refs/heads/{branch}")])
        .current_dir(repo_dir)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()?;
    Ok(output.success())
}

/// Create a new git worktree branching from `base_branch`.
///
/// Runs: `git worktree add <path> <base_branch>`
///
/// Returns the path to the created worktree.
pub fn create(
    repo_dir: &Path,
    worktrees_dir: &Path,
    worker_id: u32,
    bead_id: &str,
    base_branch: &str,
) -> Result<PathBuf, WorktreeError> {
    let wt_path = worktree_path(worktrees_dir, worker_id, bead_id);

    // Check if already exists on disk
    if wt_path.exists() {
        return Err(WorktreeError::AlreadyExists(wt_path));
    }

    // Verify base branch exists
    if !branch_exists(repo_dir, base_branch)? {
        return Err(WorktreeError::BranchNotFound(base_branch.to_string()));
    }

    // Create the worktree (detached HEAD from base_branch)
    let output = Command::new("git")
        .args([
            "worktree",
            "add",
            "--detach",
            &wt_path.to_string_lossy(),
            base_branch,
        ])
        .current_dir(repo_dir)
        .output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(WorktreeError::GitError(format!(
            "worktree add failed: {stderr}"
        )));
    }

    Ok(wt_path)
}

/// Remove a git worktree by its path.
///
/// Runs: `git worktree remove --force <path>`
pub fn remove(repo_dir: &Path, wt_path: &Path) -> Result<(), WorktreeError> {
    if !wt_path.exists() {
        // Already gone, nothing to do
        return Ok(());
    }

    let output = Command::new("git")
        .args(["worktree", "remove", "--force", &wt_path.to_string_lossy()])
        .current_dir(repo_dir)
        .output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(WorktreeError::GitError(format!(
            "worktree remove failed: {stderr}"
        )));
    }

    Ok(())
}

/// List all git worktrees known to the repository.
///
/// Returns a list of worktree paths (excluding the main worktree).
#[allow(dead_code)]
pub fn list(repo_dir: &Path) -> Result<Vec<PathBuf>, WorktreeError> {
    let output = Command::new("git")
        .args(["worktree", "list", "--porcelain"])
        .current_dir(repo_dir)
        .output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(WorktreeError::GitError(format!(
            "worktree list failed: {stderr}"
        )));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut paths = Vec::new();
    let mut is_first = true;

    for line in stdout.lines() {
        if let Some(path_str) = line.strip_prefix("worktree ") {
            if is_first {
                // Skip the main worktree (first entry)
                is_first = false;
                continue;
            }
            paths.push(PathBuf::from(path_str));
        }
    }

    Ok(paths)
}

/// Prune stale worktree metadata for worktrees whose directories no longer exist.
///
/// Runs: `git worktree prune`
#[allow(dead_code)]
pub fn prune(repo_dir: &Path) -> Result<(), WorktreeError> {
    let output = Command::new("git")
        .args(["worktree", "prune"])
        .current_dir(repo_dir)
        .output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(WorktreeError::GitError(format!(
            "worktree prune failed: {stderr}"
        )));
    }

    Ok(())
}

/// Provision a worktree with skills from the data directory.
///
/// Copies all files from `skills_src` into `<worktree>/.claude/skills/`.
/// No-op if `skills_src` does not exist or is empty (graceful degradation).
pub fn provision(wt_path: &Path, skills_src: &Path) -> Result<(), WorktreeError> {
    if !skills_src.exists() {
        return Ok(());
    }

    let dest = wt_path.join(".claude").join("skills");
    copy_dir_recursive(skills_src, &dest)?;
    Ok(())
}

/// Remove the `.claude/` directory from a worktree before removal.
///
/// No-op if the directory does not exist.
pub fn deprovision(wt_path: &Path) -> Result<(), WorktreeError> {
    let claude_dir = wt_path.join(".claude");
    if claude_dir.exists() {
        std::fs::remove_dir_all(&claude_dir)?;
    }
    Ok(())
}

/// Recursively copy a directory tree from `src` to `dest`.
fn copy_dir_recursive(src: &Path, dest: &Path) -> Result<(), WorktreeError> {
    std::fs::create_dir_all(dest)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let target = dest.join(entry.file_name());
        if file_type.is_dir() {
            copy_dir_recursive(&entry.path(), &target)?;
        } else {
            std::fs::copy(entry.path(), &target)?;
        }
    }
    Ok(())
}

/// Clean up orphaned worktrees: worktrees that exist on disk under `worktrees_dir`
/// but are not tracked by any active worker assignment.
///
/// `active_paths` is the set of worktree paths currently assigned to active workers.
/// Any worktree under `worktrees_dir` not in this set will be removed.
///
/// Returns the paths that were cleaned up.
pub fn cleanup_orphans(
    repo_dir: &Path,
    worktrees_dir: &Path,
    active_paths: &[PathBuf],
) -> Result<Vec<PathBuf>, WorktreeError> {
    if !worktrees_dir.exists() {
        return Ok(Vec::new());
    }

    let mut cleaned = Vec::new();

    let entries = std::fs::read_dir(worktrees_dir)?;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        if !active_paths.contains(&path) {
            remove(repo_dir, &path)?;
            cleaned.push(path);
        }
    }

    // Prune any stale git worktree metadata
    if !cleaned.is_empty() {
        prune(repo_dir)?;
    }

    Ok(cleaned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// Create a minimal git repo for testing.
    fn init_test_repo() -> TempDir {
        let dir = TempDir::new().unwrap();
        let repo = dir.path();

        // git init
        Command::new("git")
            .args(["init"])
            .current_dir(repo)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap();

        // Configure git user for commits
        Command::new("git")
            .args(["config", "user.email", "test@test.com"])
            .current_dir(repo)
            .status()
            .unwrap();
        Command::new("git")
            .args(["config", "user.name", "Test"])
            .current_dir(repo)
            .status()
            .unwrap();

        // Create initial commit on main
        std::fs::write(repo.join("README.md"), "test").unwrap();
        Command::new("git")
            .args(["add", "."])
            .current_dir(repo)
            .status()
            .unwrap();
        Command::new("git")
            .args(["commit", "-m", "init"])
            .current_dir(repo)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap();

        // Ensure we're on "main" branch
        Command::new("git")
            .args(["branch", "-M", "main"])
            .current_dir(repo)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap();

        dir
    }

    #[test]
    fn test_worktree_name() {
        assert_eq!(worktree_name(0, "beads-abc"), "worker-0-beads-abc");
        assert_eq!(worktree_name(3, "beads-xyz"), "worker-3-beads-xyz");
    }

    #[test]
    fn test_worktree_path() {
        let dir = Path::new("/tmp/worktrees");
        let path = worktree_path(dir, 1, "beads-abc");
        assert_eq!(path, PathBuf::from("/tmp/worktrees/worker-1-beads-abc"));
    }

    #[test]
    fn test_branch_exists_true() {
        let dir = init_test_repo();
        assert!(branch_exists(dir.path(), "main").unwrap());
    }

    #[test]
    fn test_branch_exists_false() {
        let dir = init_test_repo();
        assert!(!branch_exists(dir.path(), "nonexistent").unwrap());
    }

    #[test]
    fn test_create_and_remove_worktree() {
        let dir = init_test_repo();
        let wt_dir = dir.path().join("worktrees");
        std::fs::create_dir_all(&wt_dir).unwrap();

        let wt_path = create(dir.path(), &wt_dir, 0, "beads-abc", "main").unwrap();
        assert!(wt_path.exists());
        assert!(wt_path.join("README.md").exists());

        remove(dir.path(), &wt_path).unwrap();
        assert!(!wt_path.exists());
    }

    #[test]
    fn test_create_already_exists() {
        let dir = init_test_repo();
        let wt_dir = dir.path().join("worktrees");
        std::fs::create_dir_all(&wt_dir).unwrap();

        create(dir.path(), &wt_dir, 0, "beads-abc", "main").unwrap();

        let result = create(dir.path(), &wt_dir, 0, "beads-abc", "main");
        assert!(matches!(result, Err(WorktreeError::AlreadyExists(_))));
    }

    #[test]
    fn test_create_branch_not_found() {
        let dir = init_test_repo();
        let wt_dir = dir.path().join("worktrees");
        std::fs::create_dir_all(&wt_dir).unwrap();

        let result = create(dir.path(), &wt_dir, 0, "beads-abc", "nonexistent");
        assert!(matches!(result, Err(WorktreeError::BranchNotFound(_))));
    }

    #[test]
    fn test_remove_nonexistent_is_ok() {
        let dir = init_test_repo();
        let fake_path = dir.path().join("worktrees/worker-99-nope");
        // Should succeed (already gone)
        remove(dir.path(), &fake_path).unwrap();
    }

    #[test]
    fn test_list_worktrees() {
        let dir = init_test_repo();
        let wt_dir = dir.path().join("worktrees");
        std::fs::create_dir_all(&wt_dir).unwrap();

        // No extra worktrees initially
        let initial = list(dir.path()).unwrap();
        assert!(initial.is_empty());

        // Create two worktrees
        create(dir.path(), &wt_dir, 0, "beads-a", "main").unwrap();
        create(dir.path(), &wt_dir, 1, "beads-b", "main").unwrap();

        let wts = list(dir.path()).unwrap();
        assert_eq!(wts.len(), 2);
    }

    #[test]
    fn test_prune() {
        let dir = init_test_repo();
        let wt_dir = dir.path().join("worktrees");
        std::fs::create_dir_all(&wt_dir).unwrap();

        let wt_path = create(dir.path(), &wt_dir, 0, "beads-abc", "main").unwrap();

        // Manually delete the worktree directory (simulating orphan)
        std::fs::remove_dir_all(&wt_path).unwrap();

        // Prune should clean up git's internal tracking
        prune(dir.path()).unwrap();

        // After prune, list should be empty
        let wts = list(dir.path()).unwrap();
        assert!(wts.is_empty());
    }

    #[test]
    fn test_cleanup_orphans() {
        let dir = init_test_repo();
        let wt_dir = dir.path().join("worktrees");
        std::fs::create_dir_all(&wt_dir).unwrap();

        let path_a = create(dir.path(), &wt_dir, 0, "beads-a", "main").unwrap();
        let path_b = create(dir.path(), &wt_dir, 1, "beads-b", "main").unwrap();

        // Only path_a is active; path_b should be cleaned up
        let cleaned = cleanup_orphans(dir.path(), &wt_dir, &[path_a.clone()]).unwrap();
        assert_eq!(cleaned.len(), 1);
        assert_eq!(cleaned[0], path_b);

        assert!(path_a.exists());
        assert!(!path_b.exists());
    }

    #[test]
    fn test_cleanup_orphans_empty_dir() {
        let dir = init_test_repo();
        let wt_dir = dir.path().join("worktrees");
        std::fs::create_dir_all(&wt_dir).unwrap();

        let cleaned = cleanup_orphans(dir.path(), &wt_dir, &[]).unwrap();
        assert!(cleaned.is_empty());
    }

    #[test]
    fn test_cleanup_orphans_nonexistent_dir() {
        let dir = init_test_repo();
        let wt_dir = dir.path().join("no-such-dir");

        let cleaned = cleanup_orphans(dir.path(), &wt_dir, &[]).unwrap();
        assert!(cleaned.is_empty());
    }

    #[test]
    fn test_worktree_error_display() {
        let e = WorktreeError::AlreadyExists(PathBuf::from("/tmp/wt"));
        assert!(e.to_string().contains("already exists"));

        let e = WorktreeError::BranchNotFound("develop".to_string());
        assert!(e.to_string().contains("develop"));

        let e = WorktreeError::GitError("something broke".to_string());
        assert!(e.to_string().contains("something broke"));

        let e = WorktreeError::Io(std::io::Error::new(std::io::ErrorKind::NotFound, "gone"));
        assert!(e.to_string().contains("gone"));
    }

    #[test]
    fn test_provision_copies_skills() {
        let dir = init_test_repo();
        let wt_dir = dir.path().join("worktrees");
        std::fs::create_dir_all(&wt_dir).unwrap();

        let wt_path = create(dir.path(), &wt_dir, 0, "beads-prov", "main").unwrap();

        // Create a fake skills source directory with files and a subdirectory
        let skills_src = dir.path().join("skills");
        std::fs::create_dir_all(skills_src.join("subskill")).unwrap();
        std::fs::write(skills_src.join("skill-a.md"), "# Skill A").unwrap();
        std::fs::write(skills_src.join("subskill").join("SKILL.md"), "# Sub").unwrap();

        provision(&wt_path, &skills_src).unwrap();

        // Verify skills are in the worktree
        let dest = wt_path.join(".claude").join("skills");
        assert!(dest.exists());
        assert!(dest.join("skill-a.md").exists());
        assert_eq!(
            std::fs::read_to_string(dest.join("skill-a.md")).unwrap(),
            "# Skill A"
        );
        assert!(dest.join("subskill").join("SKILL.md").exists());
        assert_eq!(
            std::fs::read_to_string(dest.join("subskill").join("SKILL.md")).unwrap(),
            "# Sub"
        );
    }

    #[test]
    fn test_provision_noop_if_no_source() {
        let dir = init_test_repo();
        let wt_dir = dir.path().join("worktrees");
        std::fs::create_dir_all(&wt_dir).unwrap();

        let wt_path = create(dir.path(), &wt_dir, 0, "beads-noop", "main").unwrap();

        // Non-existent skills dir should be a no-op
        let skills_src = dir.path().join("no-such-skills");
        provision(&wt_path, &skills_src).unwrap();

        assert!(!wt_path.join(".claude").exists());
    }

    #[test]
    fn test_deprovision_cleans_up() {
        let dir = init_test_repo();
        let wt_dir = dir.path().join("worktrees");
        std::fs::create_dir_all(&wt_dir).unwrap();

        let wt_path = create(dir.path(), &wt_dir, 0, "beads-deprov", "main").unwrap();

        // Provision first
        let skills_src = dir.path().join("skills");
        std::fs::create_dir_all(&skills_src).unwrap();
        std::fs::write(skills_src.join("skill-a.md"), "# Skill A").unwrap();
        provision(&wt_path, &skills_src).unwrap();
        assert!(wt_path.join(".claude").exists());

        // Deprovision should remove .claude/
        deprovision(&wt_path).unwrap();
        assert!(!wt_path.join(".claude").exists());
    }

    #[test]
    fn test_deprovision_noop_if_no_claude_dir() {
        let dir = init_test_repo();
        let wt_dir = dir.path().join("worktrees");
        std::fs::create_dir_all(&wt_dir).unwrap();

        let wt_path = create(dir.path(), &wt_dir, 0, "beads-depnoop", "main").unwrap();

        // No .claude/ dir — deprovision should be a no-op
        deprovision(&wt_path).unwrap();
        assert!(!wt_path.join(".claude").exists());
    }
}
