/// Worker pool manager for spawning and tracking concurrent agent sessions.
///
/// Each worker runs in its own git worktree and progresses through states:
/// idle -> coding -> completed/failed. Completed workers are queued for integration.
use crate::config::{AgentConfig, WorkersConfig};
use crate::db;
use crate::worktree;
use rusqlite::Connection;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Instant;
use tokio::process::Command;

/// The state a worker can be in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerState {
    /// Worker slot is available for assignment.
    Idle,
    /// Worker is actively running an agent session.
    Coding,
    /// Worker's agent session completed successfully; queued for integration.
    Completed,
    /// Worker's agent session failed.
    Failed,
    /// Worker's completed work is being integrated into main.
    Integrating,
}

impl WorkerState {
    pub fn as_str(&self) -> &'static str {
        match self {
            WorkerState::Idle => "idle",
            WorkerState::Coding => "coding",
            WorkerState::Completed => "completed",
            WorkerState::Failed => "failed",
            WorkerState::Integrating => "integrating",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "idle" => Some(WorkerState::Idle),
            "coding" => Some(WorkerState::Coding),
            "completed" => Some(WorkerState::Completed),
            "failed" => Some(WorkerState::Failed),
            "integrating" => Some(WorkerState::Integrating),
            _ => None,
        }
    }
}

/// Tracks a single worker's assignment and runtime state.
#[derive(Debug)]
pub struct Worker {
    /// Worker slot index (0-based).
    pub id: u32,
    /// Current state.
    pub state: WorkerState,
    /// Database assignment ID (set when coding).
    pub assignment_id: Option<i64>,
    /// Bead ID being worked on (set when coding).
    pub bead_id: Option<String>,
    /// Path to the worker's worktree (set when coding).
    pub worktree_path: Option<PathBuf>,
    /// Tokio JoinHandle for the agent process (set when coding).
    child_handle: Option<tokio::task::JoinHandle<SessionOutcome>>,
}

/// Outcome of a worker's agent session.
#[derive(Debug)]
pub struct SessionOutcome {
    /// Worker slot ID.
    pub worker_id: u32,
    /// Process exit code (None if killed by signal).
    pub exit_code: Option<i32>,
    /// Wall-clock duration.
    pub duration: std::time::Duration,
    /// Total bytes written to the output file.
    pub output_bytes: u64,
}

/// The worker pool manages up to `max` concurrent agent sessions.
pub struct WorkerPool {
    workers: Vec<Worker>,
    repo_dir: PathBuf,
    worktrees_dir: PathBuf,
    base_branch: String,
}

/// Errors from worker pool operations.
#[derive(Debug)]
pub enum PoolError {
    /// No idle workers available.
    NoIdleWorker,
    /// Worker not found or not in expected state.
    InvalidWorker(String),
    /// Worktree operation failed.
    Worktree(worktree::WorktreeError),
    /// Database error.
    Db(rusqlite::Error),
    /// Session spawn error.
    Spawn(std::io::Error),
}

impl std::fmt::Display for PoolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PoolError::NoIdleWorker => write!(f, "no idle worker available"),
            PoolError::InvalidWorker(msg) => write!(f, "invalid worker: {msg}"),
            PoolError::Worktree(e) => write!(f, "worktree error: {e}"),
            PoolError::Db(e) => write!(f, "database error: {e}"),
            PoolError::Spawn(e) => write!(f, "spawn error: {e}"),
        }
    }
}

impl std::error::Error for PoolError {}

impl From<worktree::WorktreeError> for PoolError {
    fn from(e: worktree::WorktreeError) -> Self {
        PoolError::Worktree(e)
    }
}

impl From<rusqlite::Error> for PoolError {
    fn from(e: rusqlite::Error) -> Self {
        PoolError::Db(e)
    }
}

impl WorkerPool {
    /// Create a new worker pool with `max` slots.
    pub fn new(config: &WorkersConfig, repo_dir: PathBuf, worktrees_base: PathBuf) -> Self {
        let workers = (0..config.max)
            .map(|id| Worker {
                id,
                state: WorkerState::Idle,
                assignment_id: None,
                bead_id: None,
                worktree_path: None,
                child_handle: None,
            })
            .collect();

        Self {
            workers,
            repo_dir,
            worktrees_dir: worktrees_base,
            base_branch: config.base_branch.clone(),
        }
    }

    /// Number of total worker slots.
    pub fn capacity(&self) -> u32 {
        self.workers.len() as u32
    }

    /// Number of idle workers.
    pub fn idle_count(&self) -> u32 {
        self.workers
            .iter()
            .filter(|w| w.state == WorkerState::Idle)
            .count() as u32
    }

    /// Number of actively coding workers.
    pub fn active_count(&self) -> u32 {
        self.workers
            .iter()
            .filter(|w| w.state == WorkerState::Coding)
            .count() as u32
    }

    /// Get a snapshot of all worker states.
    pub fn snapshot(&self) -> Vec<(u32, WorkerState, Option<&str>)> {
        self.workers
            .iter()
            .map(|w| (w.id, w.state, w.bead_id.as_deref()))
            .collect()
    }

    /// Assign a bead to the next idle worker, creating a worktree and spawning the agent.
    ///
    /// Returns the worker_id and assignment_id on success.
    pub async fn spawn_worker(
        &mut self,
        bead_id: &str,
        agent_config: &AgentConfig,
        prompt: &str,
        output_dir: &Path,
        db_conn: &Connection,
    ) -> Result<(u32, i64), PoolError> {
        // Find an idle worker
        let worker_idx = self
            .workers
            .iter()
            .position(|w| w.state == WorkerState::Idle)
            .ok_or(PoolError::NoIdleWorker)?;

        let worker_id = self.workers[worker_idx].id;

        // Create worktree
        let wt_path = worktree::create(
            &self.repo_dir,
            &self.worktrees_dir,
            worker_id,
            bead_id,
            &self.base_branch,
        )?;

        // Insert assignment into DB
        let assignment_id = db::insert_worker_assignment(
            db_conn,
            worker_id as i64,
            bead_id,
            &wt_path.to_string_lossy(),
            "coding",
            None,
        )?;

        // Build the output file path for this worker
        let output_file = output_dir.join(format!("worker-{}-{}.jsonl", worker_id, bead_id));

        // Spawn the agent process in the worktree
        let handle =
            spawn_agent_in_worktree(worker_id, agent_config, &wt_path, &output_file, prompt)?;

        // Update worker state
        let worker = &mut self.workers[worker_idx];
        worker.state = WorkerState::Coding;
        worker.assignment_id = Some(assignment_id);
        worker.bead_id = Some(bead_id.to_string());
        worker.worktree_path = Some(wt_path);
        worker.child_handle = Some(handle);

        tracing::info!(worker_id, bead_id, assignment_id, "worker spawned");

        Ok((worker_id, assignment_id))
    }

    /// Poll all coding workers for completion. Returns a list of completed/failed outcomes.
    ///
    /// Only collects workers whose JoinHandle has already resolved (is_finished).
    pub async fn poll_completed(&mut self) -> Vec<SessionOutcome> {
        let mut outcomes = Vec::new();

        for worker in &mut self.workers {
            if worker.state != WorkerState::Coding {
                continue;
            }

            let is_done = worker
                .child_handle
                .as_ref()
                .map(|h| h.is_finished())
                .unwrap_or(false);

            if is_done {
                let handle = worker.child_handle.take().unwrap();
                match handle.await {
                    Ok(outcome) => {
                        let succeeded = outcome.exit_code == Some(0);
                        worker.state = if succeeded {
                            WorkerState::Completed
                        } else {
                            WorkerState::Failed
                        };
                        tracing::info!(
                            worker_id = worker.id,
                            exit_code = ?outcome.exit_code,
                            state = worker.state.as_str(),
                            "worker finished"
                        );
                        outcomes.push(outcome);
                    }
                    Err(e) => {
                        tracing::error!(
                            worker_id = worker.id,
                            error = %e,
                            "worker task panicked"
                        );
                        worker.state = WorkerState::Failed;
                        outcomes.push(SessionOutcome {
                            worker_id: worker.id,
                            exit_code: None,
                            duration: std::time::Duration::ZERO,
                            output_bytes: 0,
                        });
                    }
                }
            }
        }

        outcomes
    }

    /// Update the database for a completed/failed worker and record the outcome.
    pub fn record_outcome(
        &self,
        outcome: &SessionOutcome,
        db_conn: &Connection,
    ) -> Result<(), PoolError> {
        let worker = &self.workers[outcome.worker_id as usize];
        if let Some(assignment_id) = worker.assignment_id {
            let status = worker.state.as_str();
            let failure_notes = if worker.state == WorkerState::Failed {
                Some(format!(
                    "exit_code={:?}, duration={}s",
                    outcome.exit_code,
                    outcome.duration.as_secs()
                ))
            } else {
                None
            };
            db::update_worker_assignment_status(
                db_conn,
                assignment_id,
                status,
                failure_notes.as_deref(),
            )?;
        }
        Ok(())
    }

    /// Return a list of workers in the Completed state (ready for integration).
    pub fn completed_workers(&self) -> Vec<(u32, i64, PathBuf)> {
        self.workers
            .iter()
            .filter(|w| w.state == WorkerState::Completed)
            .filter_map(|w| Some((w.id, w.assignment_id?, w.worktree_path.clone()?)))
            .collect()
    }

    /// Check if any worker is currently in the Integrating state.
    pub fn has_integrating(&self) -> bool {
        self.workers
            .iter()
            .any(|w| w.state == WorkerState::Integrating)
    }

    /// Get the next completed worker for integration.
    /// Returns (worker_id, assignment_id, worktree_path, bead_id).
    pub fn next_completed(&self) -> Option<(u32, i64, PathBuf, String)> {
        self.workers
            .iter()
            .find(|w| w.state == WorkerState::Completed)
            .and_then(|w| {
                Some((
                    w.id,
                    w.assignment_id?,
                    w.worktree_path.clone()?,
                    w.bead_id.clone()?,
                ))
            })
    }

    /// Mark a worker as currently integrating.
    pub fn set_integrating(&mut self, worker_id: u32) {
        if let Some(worker) = self.workers.get_mut(worker_id as usize) {
            worker.state = WorkerState::Integrating;
        }
    }

    /// Reset a worker back to idle, cleaning up its worktree.
    pub fn reset_worker(&mut self, worker_id: u32) -> Result<(), PoolError> {
        let worker = self
            .workers
            .get_mut(worker_id as usize)
            .ok_or_else(|| PoolError::InvalidWorker(format!("worker {worker_id} not found")))?;

        if worker.state == WorkerState::Coding {
            return Err(PoolError::InvalidWorker(format!(
                "worker {worker_id} is still coding, cannot reset"
            )));
        }

        // Clean up worktree
        if let Some(ref wt_path) = worker.worktree_path {
            if let Err(e) = worktree::remove(&self.repo_dir, wt_path) {
                tracing::warn!(
                    worker_id,
                    error = %e,
                    "failed to remove worktree during reset"
                );
            }
        }

        worker.state = WorkerState::Idle;
        worker.assignment_id = None;
        worker.bead_id = None;
        worker.worktree_path = None;
        worker.child_handle = None;

        Ok(())
    }

    /// Get active worktree paths (for orphan cleanup).
    pub fn active_worktree_paths(&self) -> Vec<PathBuf> {
        self.workers
            .iter()
            .filter(|w| w.state == WorkerState::Coding)
            .filter_map(|w| w.worktree_path.clone())
            .collect()
    }
}

/// Spawn an agent process inside a worktree directory, returning a JoinHandle
/// that resolves to the session outcome.
fn spawn_agent_in_worktree(
    worker_id: u32,
    agent_config: &AgentConfig,
    worktree_path: &Path,
    output_path: &Path,
    prompt: &str,
) -> Result<tokio::task::JoinHandle<SessionOutcome>, PoolError> {
    // Create/truncate the output file
    let output_file = std::fs::File::create(output_path).map_err(PoolError::Spawn)?;
    let output_file_stderr = output_file.try_clone().map_err(PoolError::Spawn)?;

    let args: Vec<String> = agent_config
        .args
        .iter()
        .map(|arg| arg.replace("{prompt}", prompt))
        .collect();

    let start = Instant::now();

    let mut child = Command::new(&agent_config.command)
        .args(&args)
        .current_dir(worktree_path)
        .stdout(Stdio::from(output_file))
        .stderr(Stdio::from(output_file_stderr))
        .process_group(0)
        .spawn()
        .map_err(PoolError::Spawn)?;

    let pid = child.id().unwrap_or(0);
    tracing::info!(worker_id, pid, "agent process spawned in worktree");

    let output_path_owned = output_path.to_path_buf();

    let handle = tokio::spawn(async move {
        let status = child.wait().await;
        let duration = start.elapsed();

        let exit_code = match status {
            Ok(s) => s.code(),
            Err(e) => {
                tracing::error!(worker_id, error = %e, "failed to wait on agent process");
                None
            }
        };

        let output_bytes = std::fs::metadata(&output_path_owned)
            .map(|m| m.len())
            .unwrap_or(0);

        SessionOutcome {
            worker_id,
            exit_code,
            duration,
            output_bytes,
        }
    });

    Ok(handle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AgentConfig, WorkersConfig};
    use std::process::Command as StdCommand;
    use tempfile::TempDir;

    fn init_test_repo() -> TempDir {
        let dir = TempDir::new().unwrap();
        let repo = dir.path();

        StdCommand::new("git")
            .args(["init"])
            .current_dir(repo)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();

        StdCommand::new("git")
            .args(["config", "user.email", "test@test.com"])
            .current_dir(repo)
            .status()
            .unwrap();
        StdCommand::new("git")
            .args(["config", "user.name", "Test"])
            .current_dir(repo)
            .status()
            .unwrap();

        std::fs::write(repo.join("README.md"), "test").unwrap();
        StdCommand::new("git")
            .args(["add", "."])
            .current_dir(repo)
            .status()
            .unwrap();
        StdCommand::new("git")
            .args(["commit", "-m", "init"])
            .current_dir(repo)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();

        StdCommand::new("git")
            .args(["branch", "-M", "main"])
            .current_dir(repo)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();

        dir
    }

    fn test_agent_config() -> AgentConfig {
        AgentConfig {
            command: "echo".to_string(),
            args: vec!["hello from worker".to_string()],
            ..Default::default()
        }
    }

    fn test_workers_config(max: u32) -> WorkersConfig {
        WorkersConfig {
            max,
            base_branch: "main".to_string(),
            worktrees_dir: "worktrees".to_string(),
        }
    }

    #[test]
    fn test_worker_state_roundtrip() {
        for state in [
            WorkerState::Idle,
            WorkerState::Coding,
            WorkerState::Completed,
            WorkerState::Failed,
            WorkerState::Integrating,
        ] {
            assert_eq!(WorkerState::from_str(state.as_str()), Some(state));
        }
        assert_eq!(WorkerState::from_str("unknown"), None);
    }

    #[test]
    fn test_new_pool_all_idle() {
        let dir = init_test_repo();
        let wt_dir = dir.path().join("worktrees");
        let config = test_workers_config(3);
        let pool = WorkerPool::new(&config, dir.path().to_path_buf(), wt_dir);

        assert_eq!(pool.capacity(), 3);
        assert_eq!(pool.idle_count(), 3);
        assert_eq!(pool.active_count(), 0);

        let snap = pool.snapshot();
        assert_eq!(snap.len(), 3);
        for (id, state, bead) in &snap {
            assert_eq!(*state, WorkerState::Idle);
            assert!(bead.is_none());
            assert!(*id < 3);
        }
    }

    #[test]
    fn test_completed_workers_empty_initially() {
        let dir = init_test_repo();
        let wt_dir = dir.path().join("worktrees");
        let config = test_workers_config(2);
        let pool = WorkerPool::new(&config, dir.path().to_path_buf(), wt_dir);

        assert!(pool.completed_workers().is_empty());
    }

    #[test]
    fn test_active_worktree_paths_empty_initially() {
        let dir = init_test_repo();
        let wt_dir = dir.path().join("worktrees");
        let config = test_workers_config(2);
        let pool = WorkerPool::new(&config, dir.path().to_path_buf(), wt_dir);

        assert!(pool.active_worktree_paths().is_empty());
    }

    #[tokio::test]
    async fn test_spawn_worker_and_poll_completion() {
        let dir = init_test_repo();
        let wt_dir = dir.path().join("worktrees");
        std::fs::create_dir_all(&wt_dir).unwrap();
        let output_dir = dir.path().join("output");
        std::fs::create_dir_all(&output_dir).unwrap();

        let workers_config = test_workers_config(2);
        let mut pool = WorkerPool::new(&workers_config, dir.path().to_path_buf(), wt_dir);

        let db_path = dir.path().join("test.db");
        let conn = db::open_or_create(&db_path).unwrap();
        let agent = test_agent_config();

        // Spawn a worker
        let (worker_id, assignment_id) = pool
            .spawn_worker("beads-test-abc", &agent, "test prompt", &output_dir, &conn)
            .await
            .unwrap();

        assert_eq!(worker_id, 0);
        assert!(assignment_id > 0);
        assert_eq!(pool.idle_count(), 1);
        assert_eq!(pool.active_count(), 1);

        // Wait a bit for `echo` to complete
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // Poll for completions
        let outcomes = pool.poll_completed().await;
        assert_eq!(outcomes.len(), 1);
        assert_eq!(outcomes[0].worker_id, 0);
        assert_eq!(outcomes[0].exit_code, Some(0));
        assert!(outcomes[0].output_bytes > 0);

        // Worker should now be Completed
        assert_eq!(pool.active_count(), 0);
        let completed = pool.completed_workers();
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].0, 0);

        // Record outcome in DB
        pool.record_outcome(&outcomes[0], &conn).unwrap();
        let wa = db::get_worker_assignment(&conn, assignment_id)
            .unwrap()
            .unwrap();
        assert_eq!(wa.status, "completed");
        assert!(wa.completed_at.is_some());
    }

    #[tokio::test]
    async fn test_spawn_worker_failed_exit() {
        let dir = init_test_repo();
        let wt_dir = dir.path().join("worktrees");
        std::fs::create_dir_all(&wt_dir).unwrap();
        let output_dir = dir.path().join("output");
        std::fs::create_dir_all(&output_dir).unwrap();

        let workers_config = test_workers_config(1);
        let mut pool = WorkerPool::new(&workers_config, dir.path().to_path_buf(), wt_dir);

        let db_path = dir.path().join("test.db");
        let conn = db::open_or_create(&db_path).unwrap();

        let agent = AgentConfig {
            command: "sh".to_string(),
            args: vec!["-c".to_string(), "exit 1".to_string()],
            ..Default::default()
        };

        let (worker_id, assignment_id) = pool
            .spawn_worker("beads-fail", &agent, "test", &output_dir, &conn)
            .await
            .unwrap();

        assert_eq!(worker_id, 0);

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let outcomes = pool.poll_completed().await;
        assert_eq!(outcomes.len(), 1);
        assert_eq!(outcomes[0].exit_code, Some(1));

        // Worker should be Failed
        let snap = pool.snapshot();
        assert_eq!(snap[0].1, WorkerState::Failed);

        // Record and verify in DB
        pool.record_outcome(&outcomes[0], &conn).unwrap();
        let wa = db::get_worker_assignment(&conn, assignment_id)
            .unwrap()
            .unwrap();
        assert_eq!(wa.status, "failed");
        assert!(wa.failure_notes.is_some());
    }

    #[tokio::test]
    async fn test_reset_worker_after_completion() {
        let dir = init_test_repo();
        let wt_dir = dir.path().join("worktrees");
        std::fs::create_dir_all(&wt_dir).unwrap();
        let output_dir = dir.path().join("output");
        std::fs::create_dir_all(&output_dir).unwrap();

        let workers_config = test_workers_config(1);
        let mut pool = WorkerPool::new(&workers_config, dir.path().to_path_buf(), wt_dir);

        let db_path = dir.path().join("test.db");
        let conn = db::open_or_create(&db_path).unwrap();
        let agent = test_agent_config();

        pool.spawn_worker("beads-reset", &agent, "test", &output_dir, &conn)
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        pool.poll_completed().await;

        // Reset the worker
        pool.reset_worker(0).unwrap();
        assert_eq!(pool.idle_count(), 1);
        assert_eq!(pool.active_count(), 0);
        assert!(pool.completed_workers().is_empty());
    }

    #[tokio::test]
    async fn test_no_idle_worker_error() {
        let dir = init_test_repo();
        let wt_dir = dir.path().join("worktrees");
        std::fs::create_dir_all(&wt_dir).unwrap();
        let output_dir = dir.path().join("output");
        std::fs::create_dir_all(&output_dir).unwrap();

        // Pool with 1 worker
        let workers_config = test_workers_config(1);
        let mut pool = WorkerPool::new(&workers_config, dir.path().to_path_buf(), wt_dir);

        let db_path = dir.path().join("test.db");
        let conn = db::open_or_create(&db_path).unwrap();

        // Use sleep so it doesn't finish immediately
        let agent = AgentConfig {
            command: "sleep".to_string(),
            args: vec!["10".to_string()],
            ..Default::default()
        };

        // First spawn succeeds
        pool.spawn_worker("beads-first", &agent, "test", &output_dir, &conn)
            .await
            .unwrap();

        // Second spawn fails — no idle worker
        let result = pool
            .spawn_worker("beads-second", &agent, "test", &output_dir, &conn)
            .await;

        assert!(matches!(result, Err(PoolError::NoIdleWorker)));
    }

    #[tokio::test]
    async fn test_reset_coding_worker_fails() {
        let dir = init_test_repo();
        let wt_dir = dir.path().join("worktrees");
        std::fs::create_dir_all(&wt_dir).unwrap();
        let output_dir = dir.path().join("output");
        std::fs::create_dir_all(&output_dir).unwrap();

        let workers_config = test_workers_config(1);
        let mut pool = WorkerPool::new(&workers_config, dir.path().to_path_buf(), wt_dir);

        let db_path = dir.path().join("test.db");
        let conn = db::open_or_create(&db_path).unwrap();
        let agent = AgentConfig {
            command: "sleep".to_string(),
            args: vec!["10".to_string()],
            ..Default::default()
        };

        pool.spawn_worker("beads-active", &agent, "test", &output_dir, &conn)
            .await
            .unwrap();

        // Cannot reset a coding worker
        let result = pool.reset_worker(0);
        assert!(matches!(result, Err(PoolError::InvalidWorker(_))));
    }

    #[tokio::test]
    async fn test_multiple_workers_concurrent() {
        let dir = init_test_repo();
        let wt_dir = dir.path().join("worktrees");
        std::fs::create_dir_all(&wt_dir).unwrap();
        let output_dir = dir.path().join("output");
        std::fs::create_dir_all(&output_dir).unwrap();

        let workers_config = test_workers_config(3);
        let mut pool = WorkerPool::new(&workers_config, dir.path().to_path_buf(), wt_dir);

        let db_path = dir.path().join("test.db");
        let conn = db::open_or_create(&db_path).unwrap();
        let agent = test_agent_config();

        // Spawn 3 workers
        for i in 0..3 {
            let bead_id = format!("beads-concurrent-{i}");
            let (wid, _) = pool
                .spawn_worker(&bead_id, &agent, "test", &output_dir, &conn)
                .await
                .unwrap();
            assert_eq!(wid, i);
        }

        assert_eq!(pool.idle_count(), 0);
        assert_eq!(pool.active_count(), 3);

        // Wait for all to finish
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let outcomes = pool.poll_completed().await;
        assert_eq!(outcomes.len(), 3);

        // All should have succeeded
        for outcome in &outcomes {
            assert_eq!(outcome.exit_code, Some(0));
        }

        assert_eq!(pool.completed_workers().len(), 3);
    }

    #[test]
    fn test_pool_error_display() {
        let e = PoolError::NoIdleWorker;
        assert_eq!(e.to_string(), "no idle worker available");

        let e = PoolError::InvalidWorker("bad".to_string());
        assert!(e.to_string().contains("bad"));

        let e = PoolError::Spawn(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "not found",
        ));
        assert!(e.to_string().contains("not found"));
    }
}
