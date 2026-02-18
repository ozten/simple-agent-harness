/// Integration queue: processes completed worktrees one at a time.
///
/// After a coding agent finishes successfully in a worktree, the integrator:
/// 1. Commits any uncommitted worker changes (safety net)
/// 2. Merges main into the branch (pull main into branch, NOT push branch to main)
/// 3. Applies manifest entries from task_manifest.toml
/// 4. Runs compiler checks (cargo check / tsc --noEmit)
/// 5. If errors, spawns integration agent to fix them (up to 3 retries)
/// 6. On success, fast-forwards main to the branch tip
/// 7. Closes/syncs bead metadata and pushes main
/// 8. Records the integration in the database
///
/// Only one integration runs at a time to keep main's history linear.
/// Workers continue coding while one task integrates.
use crate::config::ResolvedAgentConfig;
use crate::db;
use crate::task_manifest;
use crate::worktree;
use rusqlite::Connection;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::process::Command;

/// Information about a tripped circuit breaker, used to escalate to human review.
#[derive(Debug, Clone)]
pub struct TrippedFailure {
    /// Bead ID that exhausted its integration attempts.
    pub bead_id: String,
    /// Human-readable error summary from the last failure.
    pub error_summary: String,
    /// Path to the preserved worktree for inspection.
    pub worktree_path: PathBuf,
    /// Total number of attempts made.
    pub attempts: u32,
}

impl TrippedFailure {
    /// Format the prominent HUMAN REVIEW NEEDED message for terminal display.
    ///
    /// Uses ANSI escape codes for red/bold output as specified in the spec.
    pub fn display_message(&self) -> String {
        format!(
            "\x1b[1;31m[ERROR] HUMAN REVIEW NEEDED: {} \x1b[0m\n\
             \x1b[1;31m        Integration failed after {} attempts.\x1b[0m\n\
             \x1b[1;31m        Error: {}\x1b[0m\n\
             \x1b[1;31m        Worktree preserved: {}\x1b[0m\n\
             \x1b[1;31m        Run `bd show {}` for details.\x1b[0m",
            self.bead_id,
            self.attempts,
            self.error_summary,
            self.worktree_path.display(),
            self.bead_id,
        )
    }

    /// Notes string suitable for `bd update <id> --notes="..."`.
    pub fn failure_notes(&self) -> String {
        format!("Integration failed: {}", self.error_summary)
    }
}

/// Maximum number of fix iterations before the circuit breaker trips.
pub const MAX_INTEGRATION_ATTEMPTS: u32 = 3;

/// State of the circuit breaker for a single bead's integration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// No attempts yet — integration hasn't started.
    Closed,
    /// At least one attempt failed, but retries remain.
    Retrying { attempt: u32 },
    /// Max attempts exceeded — escalate to human review.
    Tripped { attempts: u32 },
}

impl CircuitState {
    /// Whether further retry attempts are allowed.
    pub fn can_retry(&self) -> bool {
        matches!(self, CircuitState::Closed | CircuitState::Retrying { .. })
    }

    /// Whether the breaker has tripped (exceeded max attempts).
    pub fn is_tripped(&self) -> bool {
        matches!(self, CircuitState::Tripped { .. })
    }

    /// Current attempt number (0 if no attempts yet).
    pub fn attempt_count(&self) -> u32 {
        match self {
            CircuitState::Closed => 0,
            CircuitState::Retrying { attempt } => *attempt,
            CircuitState::Tripped { attempts } => *attempts,
        }
    }
}

impl std::fmt::Display for CircuitState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CircuitState::Closed => write!(f, "closed"),
            CircuitState::Retrying { attempt } => {
                write!(f, "retrying (attempt {attempt}/{MAX_INTEGRATION_ATTEMPTS})")
            }
            CircuitState::Tripped { attempts } => {
                write!(f, "tripped after {attempts} attempts")
            }
        }
    }
}

/// Circuit breaker tracking integration fix attempts per bead.
///
/// Each bead gets up to `MAX_INTEGRATION_ATTEMPTS` fix iterations during
/// integration. After that, the breaker trips and the task is escalated
/// for human review.
#[derive(Debug, Default)]
pub struct CircuitBreaker {
    /// Attempt counts keyed by bead ID.
    attempts: HashMap<String, u32>,
}

impl CircuitBreaker {
    pub fn new() -> Self {
        Self {
            attempts: HashMap::new(),
        }
    }

    /// Get the current circuit state for a bead.
    pub fn state(&self, bead_id: &str) -> CircuitState {
        match self.attempts.get(bead_id).copied() {
            None | Some(0) => CircuitState::Closed,
            Some(n) if n >= MAX_INTEGRATION_ATTEMPTS => CircuitState::Tripped { attempts: n },
            Some(n) => CircuitState::Retrying { attempt: n },
        }
    }

    /// Record a failed fix attempt for a bead. Returns the new state.
    pub fn record_attempt(&mut self, bead_id: &str) -> CircuitState {
        let count = self.attempts.entry(bead_id.to_string()).or_insert(0);
        *count += 1;
        tracing::info!(
            bead_id,
            attempt = *count,
            max = MAX_INTEGRATION_ATTEMPTS,
            "circuit breaker: recorded attempt"
        );
        self.state(bead_id)
    }

    /// Reset the circuit breaker for a bead (e.g., after successful integration).
    pub fn reset(&mut self, bead_id: &str) {
        self.attempts.remove(bead_id);
    }

    /// Get attempt count for a bead.
    pub fn attempt_count(&self, bead_id: &str) -> u32 {
        self.attempts.get(bead_id).copied().unwrap_or(0)
    }

    /// Check if a bead's circuit breaker has tripped and build escalation info.
    ///
    /// Returns `Some(TrippedFailure)` if the breaker is tripped, `None` otherwise.
    pub fn check_tripped(
        &self,
        bead_id: &str,
        error_summary: &str,
        worktree_path: &Path,
    ) -> Option<TrippedFailure> {
        match self.state(bead_id) {
            CircuitState::Tripped { attempts } => Some(TrippedFailure {
                bead_id: bead_id.to_string(),
                error_summary: error_summary.to_string(),
                worktree_path: worktree_path.to_path_buf(),
                attempts,
            }),
            _ => None,
        }
    }
}

/// Tracks successful integrations and triggers periodic reconciliation.
///
/// After every N successful integrations (configurable via `reconciliation.every`),
/// the full test suite is run on main. If failures are detected, the last N
/// integrated tasks are flagged for human review.
#[derive(Debug)]
#[allow(dead_code)]
pub struct ReconciliationTracker {
    /// How many successful integrations between reconciliation runs.
    every: u32,
    /// Count of successful integrations since the last reconciliation.
    since_last: u32,
    /// Bead IDs integrated since the last reconciliation, in order.
    recent_beads: Vec<String>,
}

#[allow(dead_code)]
impl ReconciliationTracker {
    /// Create a new tracker with the given reconciliation interval.
    pub fn new(every: u32) -> Self {
        Self {
            every,
            since_last: 0,
            recent_beads: Vec::new(),
        }
    }

    /// Record a successful integration. Returns `true` if reconciliation should run now.
    pub fn record_success(&mut self, bead_id: &str) -> bool {
        self.since_last += 1;
        self.recent_beads.push(bead_id.to_string());
        self.every > 0 && self.since_last >= self.every
    }

    /// Reset the counter after a reconciliation run (pass or fail).
    pub fn reset(&mut self) {
        self.since_last = 0;
        self.recent_beads.clear();
    }

    /// Get the bead IDs integrated since the last reconciliation.
    pub fn recent_beads(&self) -> &[String] {
        &self.recent_beads
    }

    /// Current count of integrations since last reconciliation.
    pub fn count(&self) -> u32 {
        self.since_last
    }
}

/// Result of a reconciliation run on main.
#[derive(Debug)]
#[allow(dead_code)]
pub struct ReconciliationResult {
    /// Whether the full test suite passed.
    pub passed: bool,
    /// Test output (stdout + stderr).
    pub output: String,
    /// Bead IDs that were integrated since the last reconciliation
    /// (flagged for review if tests failed).
    pub flagged_beads: Vec<String>,
}

/// Result of a single integration attempt.
#[derive(Debug)]
#[allow(dead_code)]
pub struct IntegrationResult {
    /// Worker slot ID that was integrated.
    pub worker_id: u32,
    /// Database assignment ID.
    pub assignment_id: i64,
    /// Bead ID that was integrated.
    pub bead_id: String,
    /// Whether integration succeeded.
    pub success: bool,
    /// Merge commit hash (if successful).
    pub merge_commit: Option<String>,
    /// Failure reason (if failed).
    pub failure_reason: Option<String>,
}

/// Close a bead in bd and sync metadata. Failures are logged as warnings and
/// do not propagate to callers.
pub(crate) fn close_bead_in_bd(bead_id: &str, reason: &str) {
    let close_succeeded = close_issue_in_bd(bead_id, reason);
    if close_succeeded {
        auto_close_parent_epics(bead_id);
    }
    sync_bd_metadata();
    commit_bead_metadata_changes();
}

/// Try to close a single issue via `bd close`. Returns true on success.
fn close_issue_in_bd(issue_id: &str, reason: &str) -> bool {
    match Command::new("bd")
        .args(["close", issue_id, &format!("--reason={reason}")])
        .output()
    {
        Ok(out) if out.status.success() => {
            tracing::info!(issue_id, reason, "bd close succeeded");
            true
        }
        Ok(out) => {
            let stderr = String::from_utf8_lossy(&out.stderr);
            tracing::warn!(
                issue_id,
                reason,
                stderr = %stderr.trim(),
                "bd close failed"
            );
            false
        }
        Err(e) => {
            tracing::warn!(issue_id, reason, error = %e, "failed to run bd close");
            false
        }
    }
}

/// Run `bd sync` and log failures as warnings.
fn sync_bd_metadata() {
    match Command::new("bd").args(["sync"]).output() {
        Ok(out) if out.status.success() => {
            tracing::info!("bd sync succeeded");
        }
        Ok(out) => {
            let stderr = String::from_utf8_lossy(&out.stderr);
            tracing::warn!(stderr = %stderr.trim(), "bd sync failed");
        }
        Err(e) => {
            tracing::warn!(error = %e, "failed to run bd sync");
        }
    }
}

/// Commit `.beads/` metadata changes from bd close/sync (if any).
fn commit_bead_metadata_changes() {
    let dirty = Command::new("git")
        .args(["diff", "--quiet", ".beads/"])
        .status()
        .map(|s| !s.success())
        .unwrap_or(false);
    let staged_dirty = Command::new("git")
        .args(["diff", "--cached", "--quiet", ".beads/"])
        .status()
        .map(|s| !s.success())
        .unwrap_or(false);

    if !dirty && !staged_dirty {
        return;
    }

    let _ = Command::new("git").args(["add", ".beads/"]).output();
    let ts = chrono_now_utc();
    let msg = format!("bd sync: {ts}");
    match Command::new("git")
        .args(["commit", "-m", &msg, "--no-verify"])
        .output()
    {
        Ok(out) if out.status.success() => tracing::info!("committed .beads/ metadata updates"),
        Ok(out) => {
            let stderr = String::from_utf8_lossy(&out.stderr);
            tracing::warn!(stderr = %stderr.trim(), "failed to commit .beads/ metadata updates");
        }
        Err(e) => tracing::warn!(error = %e, "failed to run git commit for .beads/ metadata"),
    }
}

/// Cascade-close parent epics when all of their children are closed.
fn auto_close_parent_epics(closed_bead_id: &str) {
    let mut stack = vec![closed_bead_id.to_string()];
    let mut visited = HashSet::new();

    while let Some(child_id) = stack.pop() {
        let Some(child_issue) = fetch_issue_from_bd(&child_id) else {
            continue;
        };
        let parent_ids = parent_ids_from_issue(&child_issue);

        for parent_id in parent_ids {
            if !visited.insert(parent_id.clone()) {
                continue;
            }

            let Some(parent_issue) = fetch_issue_from_bd(&parent_id) else {
                continue;
            };

            if !issue_is_epic(&parent_issue) {
                continue;
            }

            if issue_has_open_parent_child_dependencies(&parent_issue) {
                tracing::debug!(
                    parent_id,
                    child_id,
                    "parent epic still has open children, skipping auto-close"
                );
                continue;
            }

            if close_issue_in_bd(&parent_id, "all children completed") {
                tracing::info!(parent_id, child_id, "auto-closed parent epic");
                stack.push(parent_id);
            }
        }
    }
}

/// Fetch a single issue from `bd show <id> --json`.
fn fetch_issue_from_bd(issue_id: &str) -> Option<serde_json::Value> {
    let output = match Command::new("bd")
        .args(["show", issue_id, "--json"])
        .output()
    {
        Ok(output) => output,
        Err(e) => {
            tracing::warn!(issue_id, error = %e, "failed to run bd show --json");
            return None;
        }
    };

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        tracing::warn!(
            issue_id,
            stderr = %stderr.trim(),
            "bd show --json failed"
        );
        return None;
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let parsed: Result<Vec<serde_json::Value>, _> = serde_json::from_str(&stdout);
    match parsed {
        Ok(mut issues) => issues.drain(..).next(),
        Err(e) => {
            tracing::warn!(issue_id, error = %e, "failed to parse bd show JSON");
            None
        }
    }
}

/// Extract parent issue IDs from parent-child dependencies.
fn parent_ids_from_issue(issue: &serde_json::Value) -> Vec<String> {
    issue
        .get("dependencies")
        .and_then(|deps| deps.as_array())
        .map(|deps| {
            deps.iter()
                .filter_map(|dep| {
                    let dep_type = dep
                        .get("dependency_type")
                        .or_else(|| dep.get("type"))
                        .and_then(|v| v.as_str())?;
                    if !dep_type.eq_ignore_ascii_case("parent-child") {
                        return None;
                    }

                    dep.get("id")
                        .or_else(|| dep.get("depends_on_id"))
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                })
                .collect()
        })
        .unwrap_or_default()
}

fn issue_is_epic(issue: &serde_json::Value) -> bool {
    issue
        .get("issue_type")
        .and_then(|v| v.as_str())
        .map(|s| s.eq_ignore_ascii_case("epic"))
        .unwrap_or(false)
}

/// True when any parent-child dependency is not closed (or unknown status).
fn issue_has_open_parent_child_dependencies(issue: &serde_json::Value) -> bool {
    issue
        .get("dependencies")
        .and_then(|deps| deps.as_array())
        .map(|deps| {
            deps.iter().any(|dep| {
                let dep_type = dep
                    .get("dependency_type")
                    .or_else(|| dep.get("type"))
                    .and_then(|v| v.as_str());
                if dep_type != Some("parent-child") {
                    return false;
                }

                dep.get("status")
                    .and_then(|v| v.as_str())
                    .map(|status| !status.eq_ignore_ascii_case("closed"))
                    .unwrap_or(true)
            })
        })
        .unwrap_or(false)
}

/// Result of a rollback operation.
#[derive(Debug)]
pub struct RollbackResult {
    /// Whether rollback succeeded.
    pub success: bool,
    /// The revert commit hash (if successful).
    pub reverted_commit: Option<String>,
    /// Number of manifest entries reversed.
    pub manifest_entries_reversed: usize,
    /// Bead IDs that block this rollback (entanglement).
    pub blocked_by: Vec<String>,
}

/// Errors that can occur during integration.
#[derive(Debug)]
pub enum IntegrationError {
    /// A git command failed.
    Git(String),
    /// Database error.
    Db(rusqlite::Error),
    /// Worktree error.
    Worktree(worktree::WorktreeError),
}

impl std::fmt::Display for IntegrationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IntegrationError::Git(msg) => write!(f, "git error: {msg}"),
            IntegrationError::Db(e) => write!(f, "database error: {e}"),
            IntegrationError::Worktree(e) => write!(f, "worktree error: {e}"),
        }
    }
}

impl std::error::Error for IntegrationError {}

impl From<rusqlite::Error> for IntegrationError {
    fn from(e: rusqlite::Error) -> Self {
        IntegrationError::Db(e)
    }
}

impl From<worktree::WorktreeError> for IntegrationError {
    fn from(e: worktree::WorktreeError) -> Self {
        IntegrationError::Worktree(e)
    }
}

/// The integration queue manages sequential integration of completed worktrees.
pub struct IntegrationQueue {
    /// Path to the main repository.
    repo_dir: PathBuf,
    /// Base branch name (e.g., "main").
    base_branch: String,
}

impl IntegrationQueue {
    /// Create a new integration queue.
    pub fn new(repo_dir: PathBuf, base_branch: String) -> Self {
        Self {
            repo_dir,
            base_branch,
        }
    }

    /// Integrate a single completed worktree into main.
    ///
    /// Steps:
    /// 0. Commit uncommitted worker changes (safety net)
    /// 1. Merge main into the worktree's branch
    /// 2. Apply manifest entries from task_manifest.toml
    /// 3. Run compiler check (cargo check / tsc --noEmit)
    /// 4. If errors, spawn integration agent to fix; retry up to MAX_INTEGRATION_ATTEMPTS
    /// 5. Fast-forward main to the worktree's HEAD
    /// 6. Close/sync bead metadata and push main
    /// 7. Record integration in the database
    /// 8. Clean up the worktree
    #[allow(clippy::too_many_arguments)]
    pub fn integrate(
        &self,
        worker_id: u32,
        assignment_id: i64,
        bead_id: &str,
        worktree_path: &Path,
        db_conn: &Connection,
        integration_agent: Option<&ResolvedAgentConfig>,
        circuit_breaker: &mut CircuitBreaker,
    ) -> IntegrationResult {
        tracing::info!(
            worker_id,
            assignment_id,
            bead_id,
            worktree = %worktree_path.display(),
            "starting integration"
        );

        // Step 0: Safety net — commit any uncommitted worker changes before merge/integration.
        if let Err(e) = self.commit_uncommitted_changes(worktree_path, bead_id) {
            let reason = format!("failed to commit uncommitted changes: {e}");
            tracing::warn!(worker_id, bead_id, error = %e, "failed to capture worktree changes");
            self.record_failure(assignment_id, db_conn, &reason);
            return IntegrationResult {
                worker_id,
                assignment_id,
                bead_id: bead_id.to_string(),
                success: false,
                merge_commit: None,
                failure_reason: Some(reason),
            };
        }

        // Step 1: Merge main into the worktree's branch
        match self.merge_main_into_branch(worktree_path) {
            Ok(()) => {
                tracing::info!(worker_id, bead_id, "merge main into branch succeeded");
            }
            Err(e) => {
                let reason = format!("merge failed: {e}");
                tracing::warn!(worker_id, bead_id, error = %e, "merge main into branch failed");

                // Abort the merge if it's in a conflicted state
                let _ = self.abort_merge(worktree_path);

                self.record_failure(assignment_id, db_conn, &reason);

                return IntegrationResult {
                    worker_id,
                    assignment_id,
                    bead_id: bead_id.to_string(),
                    success: false,
                    merge_commit: None,
                    failure_reason: Some(reason),
                };
            }
        }

        // Step 2: Apply manifest entries from task_manifest.toml (if present)
        let manifest_path = worktree_path.join("task_manifest.toml");
        let manifest_entries = if manifest_path.exists() {
            match task_manifest::parse(&manifest_path) {
                Ok(manifest) => match task_manifest::apply(&manifest, worktree_path) {
                    Ok(count) => {
                        tracing::info!(worker_id, bead_id, count, "applied manifest entries");
                        // Stage and commit manifest changes
                        if count > 0 {
                            let _ = self.git_add_and_commit(
                                worktree_path,
                                &format!("integration: apply task manifest for {bead_id}"),
                            );
                        }
                        Some(count as i64)
                    }
                    Err(e) => {
                        tracing::warn!(worker_id, bead_id, error = %e, "failed to apply manifest");
                        None
                    }
                },
                Err(e) => {
                    tracing::warn!(worker_id, bead_id, error = %e, "failed to parse task manifest");
                    None
                }
            }
        } else {
            None
        };

        // Step 3-4: Compiler check + integration agent fix loop
        let mut integration_agent_used = false;
        if let Some(agent_config) = integration_agent {
            loop {
                // Check if circuit breaker allows retry
                if !circuit_breaker.state(bead_id).can_retry() {
                    let reason = "circuit breaker tripped during compiler fix loop".to_string();
                    self.record_failure(assignment_id, db_conn, &reason);
                    return IntegrationResult {
                        worker_id,
                        assignment_id,
                        bead_id: bead_id.to_string(),
                        success: false,
                        merge_commit: None,
                        failure_reason: Some(reason),
                    };
                }

                // Run compiler check
                match self.run_compiler_check(worktree_path) {
                    Ok(()) => {
                        tracing::info!(worker_id, bead_id, "compiler check passed");
                        break; // All good, proceed to fast-forward
                    }
                    Err(compiler_errors) => {
                        tracing::warn!(
                            worker_id,
                            bead_id,
                            state = %circuit_breaker.state(bead_id),
                            "compiler check failed, spawning integration agent"
                        );

                        // Record the attempt
                        let state = circuit_breaker.record_attempt(bead_id);
                        if state.is_tripped() {
                            let reason = format!(
                                "compiler fix failed after {} attempts: {}",
                                state.attempt_count(),
                                compiler_errors
                            );
                            self.record_failure(assignment_id, db_conn, &reason);
                            return IntegrationResult {
                                worker_id,
                                assignment_id,
                                bead_id: bead_id.to_string(),
                                success: false,
                                merge_commit: None,
                                failure_reason: Some(reason),
                            };
                        }

                        // Spawn integration agent to fix
                        let fix_prompt = format!(
                            "Fix the following compiler errors in this codebase. \
                             Apply minimal, surgical fixes (add missing imports, resolve name collisions, fix type mismatches). \
                             Do NOT refactor or change logic.\n\nCompiler output:\n{}",
                            compiler_errors
                        );

                        match self.spawn_integration_agent_sync(
                            agent_config,
                            worktree_path,
                            &fix_prompt,
                        ) {
                            Ok(exit_code) => {
                                integration_agent_used = true;
                                if exit_code != Some(0) {
                                    tracing::warn!(
                                        worker_id,
                                        bead_id,
                                        exit_code = ?exit_code,
                                        "integration agent exited with non-zero status"
                                    );
                                }
                                // Loop back to re-check compiler
                            }
                            Err(e) => {
                                let reason = format!("failed to spawn integration agent: {e}");
                                tracing::error!(worker_id, bead_id, error = %e, "integration agent spawn failed");
                                self.record_failure(assignment_id, db_conn, &reason);
                                return IntegrationResult {
                                    worker_id,
                                    assignment_id,
                                    bead_id: bead_id.to_string(),
                                    success: false,
                                    merge_commit: None,
                                    failure_reason: Some(reason),
                                };
                            }
                        }
                    }
                }
            }
        }

        // Step 5: Get the HEAD commit of the worktree (the merge/fix result)
        let worktree_head = match self.get_head_commit(worktree_path) {
            Ok(head) => head,
            Err(e) => {
                let reason = format!("failed to get worktree HEAD: {e}");
                tracing::warn!(worker_id, bead_id, error = %e, "failed to get worktree HEAD");
                self.record_failure(assignment_id, db_conn, &reason);
                return IntegrationResult {
                    worker_id,
                    assignment_id,
                    bead_id: bead_id.to_string(),
                    success: false,
                    merge_commit: None,
                    failure_reason: Some(reason),
                };
            }
        };

        // Step 6: Fast-forward main to the worktree's HEAD
        match self.fast_forward_main(&worktree_head) {
            Ok(()) => {
                tracing::info!(
                    worker_id,
                    bead_id,
                    commit = %worktree_head,
                    "fast-forwarded main"
                );
            }
            Err(e) => {
                let reason = format!("fast-forward failed: {e}");
                tracing::warn!(worker_id, bead_id, error = %e, "fast-forward main failed");
                self.record_failure(assignment_id, db_conn, &reason);
                return IntegrationResult {
                    worker_id,
                    assignment_id,
                    bead_id: bead_id.to_string(),
                    success: false,
                    merge_commit: None,
                    failure_reason: Some(reason),
                };
            }
        }

        // Mark the bead as closed in bd so progress metrics and dependency
        // unblocking reflect successful integration.
        let close_reason = match self.get_head_commit_subject(worktree_path) {
            Ok(subject) if !subject.trim().is_empty() => {
                format!("integration: {subject} ({bead_id})")
            }
            _ => format!("integration: merged {bead_id} at {worktree_head}"),
        };
        close_bead_in_bd(bead_id, &close_reason);

        if let Err(e) = self.push_base_branch() {
            tracing::warn!(
                worker_id,
                bead_id,
                error = %e,
                "failed to push integrated main branch"
            );
        }

        // Sync working tree to match the updated ref
        if let Err(e) = self.sync_working_tree() {
            tracing::warn!(error = %e, "working tree sync failed (non-fatal)");
        }

        // Step 7: Record integration in DB
        let merged_at = chrono_now_utc();
        let manifest_str = manifest_entries.map(|n| format!("{n} entries applied"));
        let cross_task_str = if integration_agent_used {
            Some(format!(
                "{} fix attempts",
                circuit_breaker.attempt_count(bead_id)
            ))
        } else {
            None
        };
        if let Err(e) = db::insert_integration_log(
            db_conn,
            assignment_id,
            &merged_at,
            &worktree_head,
            manifest_str.as_deref(),
            cross_task_str.as_deref(),
            integration_agent_used,
        ) {
            tracing::warn!(error = %e, "failed to record integration log");
        }

        // Record integration iteration count
        let iteration_count = circuit_breaker.attempt_count(bead_id);
        if let Err(e) = db::record_integration_iterations(
            db_conn,
            assignment_id,
            bead_id,
            iteration_count,
            cross_task_str.as_deref(),
            &merged_at,
        ) {
            tracing::warn!(error = %e, "failed to record integration iterations");
        }

        // Update assignment status to integrated
        if let Err(e) =
            db::update_worker_assignment_status(db_conn, assignment_id, "integrated", None)
        {
            tracing::warn!(error = %e, "failed to update assignment status to integrated");
        }

        // Reset circuit breaker on success
        circuit_breaker.reset(bead_id);

        // Step 8: Clean up the worktree
        if let Err(e) = worktree::remove(&self.repo_dir, worktree_path) {
            tracing::warn!(
                worker_id,
                error = %e,
                "failed to remove worktree after integration"
            );
        }

        IntegrationResult {
            worker_id,
            assignment_id,
            bead_id: bead_id.to_string(),
            success: true,
            merge_commit: Some(worktree_head),
            failure_reason: None,
        }
    }

    /// Merge main into the branch in the worktree.
    ///
    /// This is step 1 of the spec: "In the worker's worktree, merge main into the branch
    /// (pull main into branch, NOT push branch to main)".
    fn merge_main_into_branch(&self, worktree_path: &Path) -> Result<(), IntegrationError> {
        // First, fetch the latest main from the main repo into the worktree.
        // Since the worktree shares the same .git, we can reference the base_branch directly.
        let output = Command::new("git")
            .args(["merge", &self.base_branch, "--no-edit"])
            .current_dir(worktree_path)
            .output()
            .map_err(|e| IntegrationError::Git(format!("failed to run git merge: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(IntegrationError::Git(format!(
                "git merge {} failed: {}",
                self.base_branch,
                stderr.trim()
            )));
        }

        Ok(())
    }

    /// Commit uncommitted changes in the worktree before merge.
    ///
    /// This prevents agent-authored changes from being lost when the worker exits
    /// without an explicit commit.
    fn commit_uncommitted_changes(
        &self,
        worktree_path: &Path,
        bead_id: &str,
    ) -> Result<(), IntegrationError> {
        if !self.worktree_has_uncommitted_changes(worktree_path)? {
            return Ok(());
        }

        self.git_add_and_commit(
            worktree_path,
            &format!("integration: capture uncommitted work for {bead_id}"),
        )
    }

    /// Return true when a worktree has staged, unstaged, or untracked changes.
    fn worktree_has_uncommitted_changes(
        &self,
        worktree_path: &Path,
    ) -> Result<bool, IntegrationError> {
        let output = Command::new("git")
            .args(["status", "--porcelain"])
            .current_dir(worktree_path)
            .output()
            .map_err(|e| IntegrationError::Git(format!("failed to check worktree status: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(IntegrationError::Git(format!(
                "git status --porcelain failed: {}",
                stderr.trim()
            )));
        }

        Ok(!String::from_utf8_lossy(&output.stdout).trim().is_empty())
    }

    /// Abort a merge that's in a conflicted state.
    fn abort_merge(&self, worktree_path: &Path) -> Result<(), IntegrationError> {
        let output = Command::new("git")
            .args(["merge", "--abort"])
            .current_dir(worktree_path)
            .output()
            .map_err(|e| IntegrationError::Git(format!("failed to run git merge --abort: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(IntegrationError::Git(format!(
                "git merge --abort failed: {}",
                stderr.trim()
            )));
        }

        Ok(())
    }

    /// Get the HEAD commit hash from a worktree.
    fn get_head_commit(&self, worktree_path: &Path) -> Result<String, IntegrationError> {
        let output = Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(worktree_path)
            .output()
            .map_err(|e| IntegrationError::Git(format!("failed to run git rev-parse: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(IntegrationError::Git(format!(
                "git rev-parse HEAD failed: {}",
                stderr.trim()
            )));
        }

        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }

    /// Get the HEAD commit subject from a worktree.
    fn get_head_commit_subject(&self, worktree_path: &Path) -> Result<String, IntegrationError> {
        let output = Command::new("git")
            .args(["log", "-1", "--pretty=%s", "HEAD"])
            .current_dir(worktree_path)
            .output()
            .map_err(|e| IntegrationError::Git(format!("failed to run git log: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(IntegrationError::Git(format!(
                "git log -1 failed: {}",
                stderr.trim()
            )));
        }

        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }

    /// Sync the working tree to match HEAD after an update-ref.
    ///
    /// `git update-ref` moves the ref without updating the index or working tree.
    /// This leaves the main checkout stale — any subsequent commit from the working
    /// directory would silently revert the integrated changes. Reset to HEAD to fix.
    fn sync_working_tree(&self) -> Result<(), IntegrationError> {
        let output = Command::new("git")
            .args(["reset", "--hard", "HEAD"])
            .current_dir(&self.repo_dir)
            .output()
            .map_err(|e| IntegrationError::Git(format!("failed to sync working tree: {e}")))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            tracing::warn!("working tree sync failed: {}", stderr.trim());
        }
        Ok(())
    }

    /// Fast-forward main to the given commit.
    ///
    /// This is step 7 of the spec: "Fast-forward main to this branch".
    /// Uses `git update-ref` in the main repo to advance main's tip.
    fn fast_forward_main(&self, commit: &str) -> Result<(), IntegrationError> {
        // Use update-ref to advance main without checking out
        let output = Command::new("git")
            .args([
                "update-ref",
                &format!("refs/heads/{}", self.base_branch),
                commit,
            ])
            .current_dir(&self.repo_dir)
            .output()
            .map_err(|e| IntegrationError::Git(format!("failed to run git update-ref: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(IntegrationError::Git(format!(
                "git update-ref failed: {}",
                stderr.trim()
            )));
        }

        Ok(())
    }

    /// Push the updated base branch to origin (best-effort).
    fn push_base_branch(&self) -> Result<(), IntegrationError> {
        let output = Command::new("git")
            .args(["push", "origin", &self.base_branch])
            .current_dir(&self.repo_dir)
            .output()
            .map_err(|e| IntegrationError::Git(format!("failed to run git push: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(IntegrationError::Git(format!(
                "git push origin {} failed: {}",
                self.base_branch,
                stderr.trim()
            )));
        }

        Ok(())
    }

    /// Run a compiler check in the worktree.
    ///
    /// Tries `cargo check` first (for Rust projects), then `tsc --noEmit` (for TypeScript).
    /// Returns `Ok(())` if the check passes, or `Err(error_output)` with compiler diagnostics.
    fn run_compiler_check(&self, worktree_path: &Path) -> Result<(), String> {
        // Try cargo check first (Rust projects)
        let cargo_toml = worktree_path.join("Cargo.toml");
        if cargo_toml.exists() {
            let output = Command::new("cargo")
                .args(["check", "--release", "--message-format=short"])
                .current_dir(worktree_path)
                .output()
                .map_err(|e| format!("failed to run cargo check: {e}"))?;

            if output.status.success() {
                return Ok(());
            }

            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(stderr.trim().to_string());
        }

        // Try tsc --noEmit (TypeScript projects)
        let tsconfig = worktree_path.join("tsconfig.json");
        if tsconfig.exists() {
            let output = Command::new("npx")
                .args(["tsc", "--noEmit"])
                .current_dir(worktree_path)
                .output()
                .map_err(|e| format!("failed to run tsc --noEmit: {e}"))?;

            if output.status.success() {
                return Ok(());
            }

            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(stdout.trim().to_string());
        }

        // No recognized build system — skip check, assume success
        tracing::debug!("no Cargo.toml or tsconfig.json found, skipping compiler check");
        Ok(())
    }

    /// Spawn an integration agent synchronously in the worktree.
    ///
    /// The integration agent runs with the given prompt (containing compiler errors)
    /// and is expected to apply surgical fixes. Runs in the same worktree to
    /// preserve build artifacts.
    ///
    /// Returns the agent's exit code.
    fn spawn_integration_agent_sync(
        &self,
        agent_config: &ResolvedAgentConfig,
        worktree_path: &Path,
        prompt: &str,
    ) -> Result<Option<i32>, IntegrationError> {
        let args: Vec<String> = agent_config
            .args
            .iter()
            .map(|arg| arg.replace("{prompt}", prompt))
            .collect();

        tracing::info!(
            command = %agent_config.command,
            worktree = %worktree_path.display(),
            "spawning integration agent"
        );

        let output = Command::new(&agent_config.command)
            .args(&args)
            .envs(&agent_config.env)
            .current_dir(worktree_path)
            .output()
            .map_err(|e| {
                IntegrationError::Git(format!("failed to spawn integration agent: {e}"))
            })?;

        Ok(output.status.code())
    }

    /// Stage all changes and commit in the worktree.
    fn git_add_and_commit(
        &self,
        worktree_path: &Path,
        message: &str,
    ) -> Result<(), IntegrationError> {
        let add_output = Command::new("git")
            .args(["add", "-A"])
            .current_dir(worktree_path)
            .output()
            .map_err(|e| IntegrationError::Git(format!("git add failed: {e}")))?;

        if !add_output.status.success() {
            let stderr = String::from_utf8_lossy(&add_output.stderr);
            return Err(IntegrationError::Git(format!(
                "git add failed: {}",
                stderr.trim()
            )));
        }

        let commit_output = Command::new("git")
            .args(["commit", "-m", message, "--allow-empty"])
            .current_dir(worktree_path)
            .output()
            .map_err(|e| IntegrationError::Git(format!("git commit failed: {e}")))?;

        if !commit_output.status.success() {
            let stderr = String::from_utf8_lossy(&commit_output.stderr);
            // "nothing to commit" is OK
            if !stderr.contains("nothing to commit") {
                return Err(IntegrationError::Git(format!(
                    "git commit failed: {}",
                    stderr.trim()
                )));
            }
        }

        Ok(())
    }

    /// Run the full test suite on main as a reconciliation check.
    ///
    /// This catches cross-task semantic bugs that pass individual integration
    /// but fail when combined. If the test suite fails, the recently-integrated
    /// bead IDs are returned as flagged for human review.
    #[allow(dead_code)]
    pub fn run_reconciliation(&self, tracker: &mut ReconciliationTracker) -> ReconciliationResult {
        tracing::info!(
            count = tracker.count(),
            beads = ?tracker.recent_beads(),
            "running periodic reconciliation on main"
        );

        let flagged_beads = tracker.recent_beads().to_vec();

        // Run the test suite in the main repo
        let (passed, output) = self.run_test_suite(&self.repo_dir);

        if passed {
            tracing::info!("reconciliation passed — full test suite OK");
        } else {
            tracing::warn!(
                flagged = ?flagged_beads,
                "reconciliation FAILED — flagging {} beads for review",
                flagged_beads.len()
            );
        }

        // Always reset the tracker after a reconciliation run
        tracker.reset();

        ReconciliationResult {
            passed,
            output,
            flagged_beads: if passed { Vec::new() } else { flagged_beads },
        }
    }

    /// Run the project's test suite. Returns (passed, output).
    #[allow(dead_code)]
    fn run_test_suite(&self, dir: &Path) -> (bool, String) {
        // Try cargo test first (Rust projects)
        let cargo_toml = dir.join("Cargo.toml");
        if cargo_toml.exists() {
            match Command::new("cargo")
                .args(["test", "--release"])
                .current_dir(dir)
                .output()
            {
                Ok(output) => {
                    let combined = format!(
                        "{}\n{}",
                        String::from_utf8_lossy(&output.stdout),
                        String::from_utf8_lossy(&output.stderr)
                    );
                    return (output.status.success(), combined);
                }
                Err(e) => {
                    return (false, format!("failed to run cargo test: {e}"));
                }
            }
        }

        // Try npm test (Node.js projects)
        let package_json = dir.join("package.json");
        if package_json.exists() {
            match Command::new("npm").args(["test"]).current_dir(dir).output() {
                Ok(output) => {
                    let combined = format!(
                        "{}\n{}",
                        String::from_utf8_lossy(&output.stdout),
                        String::from_utf8_lossy(&output.stderr)
                    );
                    return (output.status.success(), combined);
                }
                Err(e) => {
                    return (false, format!("failed to run npm test: {e}"));
                }
            }
        }

        // No recognized test system — assume pass
        tracing::debug!("no Cargo.toml or package.json found, skipping reconciliation test suite");
        (true, "no test runner detected".to_string())
    }

    /// Rollback a previously integrated task.
    ///
    /// 1. Check entanglement — if other integrated tasks import from this task's module,
    ///    rollback is blocked unless `force` is true.
    /// 2. `git revert` the merge commit on the base branch.
    /// 3. Reverse manifest entries (remove `pub mod`, Cargo.toml deps, re-exports).
    /// 4. Commit the manifest reversal.
    /// 5. Update the assignment status to "rolled_back".
    pub fn rollback(
        &self,
        bead_id: &str,
        merge_commit: &str,
        assignment_id: i64,
        manifest_entries: Option<&str>,
        db_conn: &Connection,
        force: bool,
    ) -> Result<RollbackResult, IntegrationError> {
        tracing::info!(bead_id, merge_commit, "starting rollback");

        // Step 1: Check entanglement
        let entangled = db::find_entangled_beads(db_conn, bead_id)?;
        if !entangled.is_empty() && !force {
            let entangled_ids: Vec<String> = entangled.iter().map(|(id, _)| id.clone()).collect();
            tracing::warn!(
                bead_id,
                entangled = ?entangled_ids,
                "rollback blocked: other tasks depend on this bead"
            );
            return Ok(RollbackResult {
                success: false,
                reverted_commit: None,
                manifest_entries_reversed: 0,
                blocked_by: entangled_ids,
            });
        }

        // Step 2: Ensure the working tree matches HEAD before reverting.
        // Integration uses update-ref which moves the ref without updating the worktree,
        // so we need to sync the working tree to HEAD first.
        self.sync_working_tree()?;

        // git revert the merge commit
        let output = Command::new("git")
            .args(["revert", "--no-edit", merge_commit])
            .current_dir(&self.repo_dir)
            .output()
            .map_err(|e| IntegrationError::Git(format!("failed to run git revert: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            // Abort the revert if it left conflicts
            let _ = Command::new("git")
                .args(["revert", "--abort"])
                .current_dir(&self.repo_dir)
                .output();
            return Err(IntegrationError::Git(format!(
                "git revert {merge_commit} failed: {}",
                stderr.trim()
            )));
        }

        // Get the revert commit hash
        let revert_commit = self.get_head_commit(&self.repo_dir)?;
        tracing::info!(bead_id, revert_commit = %revert_commit, "git revert succeeded");

        // Step 3: Reverse manifest entries if a manifest was applied
        let manifest_reversed = if manifest_entries.is_some() {
            // Try to find and parse task_manifest.toml from the original worktree
            // Since the worktree may be gone, we reverse based on what's currently
            // in the repo after the revert. The revert already undid the code changes,
            // but manifest entries (pub mod lines, Cargo.toml deps) may still be present
            // if they were committed in a separate commit.
            //
            // For now, manifest reversal is handled by the git revert itself since
            // manifest changes are committed as part of integration. If the manifest
            // entries were in a separate commit, we note it in the result.
            0usize
        } else {
            0
        };

        // Step 4: Update assignment status to rolled_back
        if let Err(e) = db::update_worker_assignment_status(
            db_conn,
            assignment_id,
            "rolled_back",
            Some(&format!("Rolled back: reverted commit {merge_commit}")),
        ) {
            tracing::warn!(error = %e, "failed to update assignment status to rolled_back");
        }

        Ok(RollbackResult {
            success: true,
            reverted_commit: Some(revert_commit),
            manifest_entries_reversed: manifest_reversed,
            blocked_by: Vec::new(),
        })
    }

    /// Record a failed integration in the database.
    fn record_failure(&self, assignment_id: i64, db_conn: &Connection, reason: &str) {
        if let Err(e) = db::update_worker_assignment_status(
            db_conn,
            assignment_id,
            "integration_failed",
            Some(reason),
        ) {
            tracing::warn!(error = %e, "failed to update assignment status to integration_failed");
        }
    }
}

/// Get current UTC time as ISO 8601 string.
fn chrono_now_utc() -> String {
    // Use std::time to avoid adding chrono dependency
    let now = std::time::SystemTime::now();
    let duration = now
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = duration.as_secs();

    // Simple UTC timestamp formatting
    let days = secs / 86400;
    let time_secs = secs % 86400;
    let hours = time_secs / 3600;
    let minutes = (time_secs % 3600) / 60;
    let seconds = time_secs % 60;

    // Days since epoch to date (simplified algorithm)
    let mut y = 1970i64;
    let mut remaining_days = days as i64;

    loop {
        let days_in_year = if is_leap_year(y) { 366 } else { 365 };
        if remaining_days < days_in_year {
            break;
        }
        remaining_days -= days_in_year;
        y += 1;
    }

    let month_days = if is_leap_year(y) {
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };

    let mut m = 0usize;
    for (i, &md) in month_days.iter().enumerate() {
        if remaining_days < md as i64 {
            m = i;
            break;
        }
        remaining_days -= md as i64;
    }

    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        y,
        m + 1,
        remaining_days + 1,
        hours,
        minutes,
        seconds
    )
}

fn is_leap_year(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::process::Command as StdCommand;
    use std::process::Stdio;
    use tempfile::TempDir;

    /// Create a minimal git repo for testing.
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

        std::fs::write(repo.join("README.md"), "initial").unwrap();
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

    /// Create a worktree from the test repo and make a commit in it.
    fn create_worktree_with_commit(
        repo_dir: &Path,
        worktrees_dir: &Path,
        worker_id: u32,
        bead_id: &str,
    ) -> PathBuf {
        let wt_path =
            worktree::create(repo_dir, worktrees_dir, worker_id, bead_id, "main").unwrap();

        // Make a change in the worktree
        std::fs::write(wt_path.join("feature.txt"), format!("work from {bead_id}")).unwrap();
        StdCommand::new("git")
            .args(["add", "feature.txt"])
            .current_dir(&wt_path)
            .status()
            .unwrap();
        StdCommand::new("git")
            .args(["commit", "-m", &format!("implement {bead_id}")])
            .current_dir(&wt_path)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();

        wt_path
    }

    #[test]
    fn test_chrono_now_utc_format() {
        let ts = chrono_now_utc();
        // Should be ISO 8601 format: YYYY-MM-DDTHH:MM:SSZ
        assert!(ts.contains('T'));
        assert!(ts.ends_with('Z'));
        assert_eq!(ts.len(), 20);
    }

    #[test]
    fn test_integration_queue_new() {
        let queue = IntegrationQueue::new(PathBuf::from("/tmp/repo"), "main".to_string());
        assert_eq!(queue.repo_dir, PathBuf::from("/tmp/repo"));
        assert_eq!(queue.base_branch, "main");
    }

    #[test]
    fn test_parent_ids_from_issue_supports_show_and_list_shapes() {
        let issue = serde_json::json!({
            "dependencies": [
                {"id": "epic-from-show", "dependency_type": "parent-child"},
                {"depends_on_id": "epic-from-list", "type": "parent-child"},
                {"id": "other-dep", "dependency_type": "blocks"}
            ]
        });

        let mut parent_ids = parent_ids_from_issue(&issue);
        parent_ids.sort();
        assert_eq!(
            parent_ids,
            vec!["epic-from-list".to_string(), "epic-from-show".to_string()]
        );
    }

    #[test]
    fn test_issue_is_epic_case_insensitive() {
        let epic = serde_json::json!({ "issue_type": "EPIC" });
        let task = serde_json::json!({ "issue_type": "task" });

        assert!(issue_is_epic(&epic));
        assert!(!issue_is_epic(&task));
    }

    #[test]
    fn test_issue_has_open_parent_child_dependencies() {
        let all_closed = serde_json::json!({
            "dependencies": [
                {"id": "child-1", "dependency_type": "parent-child", "status": "closed"},
                {"id": "child-2", "dependency_type": "parent-child", "status": "CLOSED"}
            ]
        });
        assert!(!issue_has_open_parent_child_dependencies(&all_closed));

        let has_open = serde_json::json!({
            "dependencies": [
                {"id": "child-1", "dependency_type": "parent-child", "status": "open"},
                {"id": "child-2", "dependency_type": "parent-child", "status": "closed"}
            ]
        });
        assert!(issue_has_open_parent_child_dependencies(&has_open));

        let unknown_status = serde_json::json!({
            "dependencies": [
                {"id": "child-1", "dependency_type": "parent-child"}
            ]
        });
        assert!(issue_has_open_parent_child_dependencies(&unknown_status));
    }

    #[test]
    fn test_successful_integration() {
        let dir = init_test_repo();
        let repo_dir = dir.path();
        let wt_dir = repo_dir.join("worktrees");
        std::fs::create_dir_all(&wt_dir).unwrap();

        let db_path = repo_dir.join("test.db");
        let conn = db::open_or_create(&db_path).unwrap();

        // Insert a worker assignment
        let assignment_id =
            db::insert_worker_assignment(&conn, 0, "beads-abc", "/tmp/wt-0", "completed", None)
                .unwrap();

        // Create a worktree with a commit
        let wt_path = create_worktree_with_commit(repo_dir, &wt_dir, 0, "beads-abc");

        // Get main's HEAD before integration
        let main_before = StdCommand::new("git")
            .args(["rev-parse", "main"])
            .current_dir(repo_dir)
            .output()
            .unwrap();
        let main_before = String::from_utf8_lossy(&main_before.stdout)
            .trim()
            .to_string();

        let queue = IntegrationQueue::new(repo_dir.to_path_buf(), "main".to_string());
        let mut cb = CircuitBreaker::new();
        let result = queue.integrate(
            0,
            assignment_id,
            "beads-abc",
            &wt_path,
            &conn,
            None,
            &mut cb,
        );

        assert!(
            result.success,
            "integration should succeed: {:?}",
            result.failure_reason
        );
        assert!(result.merge_commit.is_some());
        assert!(result.failure_reason.is_none());
        assert_eq!(result.worker_id, 0);
        assert_eq!(result.bead_id, "beads-abc");

        // Main should have advanced
        let main_after = StdCommand::new("git")
            .args(["rev-parse", "main"])
            .current_dir(repo_dir)
            .output()
            .unwrap();
        let main_after = String::from_utf8_lossy(&main_after.stdout)
            .trim()
            .to_string();
        assert_ne!(main_before, main_after, "main should have advanced");

        // Integration log should be recorded
        let log = db::integration_log_by_assignment(&conn, assignment_id).unwrap();
        assert_eq!(log.len(), 1);
        assert_eq!(log[0].merge_commit, result.merge_commit.unwrap());

        // Assignment status should be updated
        let wa = db::get_worker_assignment(&conn, assignment_id)
            .unwrap()
            .unwrap();
        assert_eq!(wa.status, "integrated");
    }

    #[test]
    fn test_integration_commits_uncommitted_worker_changes() {
        let dir = init_test_repo();
        let repo_dir = dir.path();
        let wt_dir = repo_dir.join("worktrees");
        std::fs::create_dir_all(&wt_dir).unwrap();

        let db_path = repo_dir.join("test.db");
        let conn = db::open_or_create(&db_path).unwrap();
        let assignment_id = db::insert_worker_assignment(
            &conn,
            0,
            "beads-uncommitted",
            "/tmp/wt-uncommitted",
            "completed",
            None,
        )
        .unwrap();

        let wt_path = worktree::create(repo_dir, &wt_dir, 0, "beads-uncommitted", "main").unwrap();

        // Leave a tracked modification uncommitted in the worker worktree.
        std::fs::write(
            wt_path.join("README.md"),
            "captured by integration safety net",
        )
        .unwrap();

        let queue = IntegrationQueue::new(repo_dir.to_path_buf(), "main".to_string());
        let mut cb = CircuitBreaker::new();
        let result = queue.integrate(
            0,
            assignment_id,
            "beads-uncommitted",
            &wt_path,
            &conn,
            None,
            &mut cb,
        );

        assert!(
            result.success,
            "integration should succeed for uncommitted worker changes: {:?}",
            result.failure_reason
        );

        let subject = StdCommand::new("git")
            .args(["log", "-1", "--pretty=%s", "main"])
            .current_dir(repo_dir)
            .output()
            .unwrap();
        let subject = String::from_utf8_lossy(&subject.stdout);
        assert!(
            subject.contains("capture uncommitted work for beads-uncommitted"),
            "expected safety-net commit on main, got subject: {subject}"
        );
    }

    #[test]
    fn test_integration_with_merge_conflict() {
        let dir = init_test_repo();
        let repo_dir = dir.path();
        let wt_dir = repo_dir.join("worktrees");
        std::fs::create_dir_all(&wt_dir).unwrap();

        let db_path = repo_dir.join("test.db");
        let conn = db::open_or_create(&db_path).unwrap();

        let assignment_id = db::insert_worker_assignment(
            &conn,
            0,
            "beads-conflict",
            "/tmp/wt-0",
            "completed",
            None,
        )
        .unwrap();

        // Create a worktree with changes to README.md
        let wt_path = worktree::create(repo_dir, &wt_dir, 0, "beads-conflict", "main").unwrap();

        std::fs::write(wt_path.join("README.md"), "worktree change").unwrap();
        StdCommand::new("git")
            .args(["add", "README.md"])
            .current_dir(&wt_path)
            .status()
            .unwrap();
        StdCommand::new("git")
            .args(["commit", "-m", "worktree edit"])
            .current_dir(&wt_path)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();

        // Now make a conflicting change on main (in the main repo)
        std::fs::write(repo_dir.join("README.md"), "main change").unwrap();
        StdCommand::new("git")
            .args(["add", "README.md"])
            .current_dir(repo_dir)
            .status()
            .unwrap();
        StdCommand::new("git")
            .args(["commit", "-m", "main edit"])
            .current_dir(repo_dir)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();

        let queue = IntegrationQueue::new(repo_dir.to_path_buf(), "main".to_string());
        let mut cb = CircuitBreaker::new();
        let result = queue.integrate(
            0,
            assignment_id,
            "beads-conflict",
            &wt_path,
            &conn,
            None,
            &mut cb,
        );

        assert!(!result.success, "integration should fail due to conflict");
        assert!(result.merge_commit.is_none());
        assert!(result.failure_reason.is_some());
        assert!(result
            .failure_reason
            .as_ref()
            .unwrap()
            .contains("merge failed"));

        // Assignment status should reflect failure
        let wa = db::get_worker_assignment(&conn, assignment_id)
            .unwrap()
            .unwrap();
        assert_eq!(wa.status, "integration_failed");
        assert!(wa.failure_notes.is_some());
    }

    #[test]
    fn test_integration_no_changes_trivial_merge() {
        // When worktree has no changes beyond main, merge is a no-op (already up to date)
        let dir = init_test_repo();
        let repo_dir = dir.path();
        let wt_dir = repo_dir.join("worktrees");
        std::fs::create_dir_all(&wt_dir).unwrap();

        let db_path = repo_dir.join("test.db");
        let conn = db::open_or_create(&db_path).unwrap();

        let assignment_id =
            db::insert_worker_assignment(&conn, 0, "beads-noop", "/tmp/wt-0", "completed", None)
                .unwrap();

        // Create worktree but add a new file (no conflict with main)
        let wt_path = create_worktree_with_commit(repo_dir, &wt_dir, 0, "beads-noop");

        let queue = IntegrationQueue::new(repo_dir.to_path_buf(), "main".to_string());
        let mut cb = CircuitBreaker::new();
        let result = queue.integrate(
            0,
            assignment_id,
            "beads-noop",
            &wt_path,
            &conn,
            None,
            &mut cb,
        );

        assert!(
            result.success,
            "trivial merge should succeed: {:?}",
            result.failure_reason
        );
        assert!(result.merge_commit.is_some());
    }

    #[test]
    fn test_integration_error_display() {
        let e = IntegrationError::Git("something broke".to_string());
        assert!(e.to_string().contains("something broke"));

        let e = IntegrationError::Worktree(worktree::WorktreeError::BranchNotFound(
            "develop".to_string(),
        ));
        assert!(e.to_string().contains("develop"));
    }

    #[test]
    fn test_get_head_commit() {
        let dir = init_test_repo();
        let queue = IntegrationQueue::new(dir.path().to_path_buf(), "main".to_string());

        let head = queue.get_head_commit(dir.path()).unwrap();
        assert!(!head.is_empty());
        // SHA-1 hashes are 40 hex characters
        assert_eq!(head.len(), 40);
        assert!(head.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_is_leap_year() {
        assert!(is_leap_year(2000));
        assert!(is_leap_year(2024));
        assert!(!is_leap_year(1900));
        assert!(!is_leap_year(2023));
    }

    #[test]
    fn test_merge_main_into_branch_no_divergence() {
        // When main hasn't changed, merge is "Already up to date"
        let dir = init_test_repo();
        let repo_dir = dir.path();
        let wt_dir = repo_dir.join("worktrees");
        std::fs::create_dir_all(&wt_dir).unwrap();

        let wt_path = create_worktree_with_commit(repo_dir, &wt_dir, 0, "beads-nomerge");

        let queue = IntegrationQueue::new(repo_dir.to_path_buf(), "main".to_string());
        let result = queue.merge_main_into_branch(&wt_path);
        assert!(result.is_ok(), "merge should succeed when no divergence");
    }

    #[test]
    fn test_fast_forward_main() {
        let dir = init_test_repo();
        let repo_dir = dir.path();
        let wt_dir = repo_dir.join("worktrees");
        std::fs::create_dir_all(&wt_dir).unwrap();

        let wt_path = create_worktree_with_commit(repo_dir, &wt_dir, 0, "beads-ff");

        let queue = IntegrationQueue::new(repo_dir.to_path_buf(), "main".to_string());

        // Get the worktree HEAD
        let wt_head = queue.get_head_commit(&wt_path).unwrap();
        let main_before = queue.get_head_commit(repo_dir).unwrap();

        // Fast-forward main
        queue.fast_forward_main(&wt_head).unwrap();

        let main_after = queue.get_head_commit(repo_dir).unwrap();
        assert_eq!(main_after, wt_head, "main should point to worktree HEAD");
        assert_ne!(main_before, main_after, "main should have advanced");
    }

    #[test]
    fn test_circuit_state_closed_initial() {
        let cb = CircuitBreaker::new();
        let state = cb.state("beads-abc");
        assert_eq!(state, CircuitState::Closed);
        assert!(state.can_retry());
        assert!(!state.is_tripped());
        assert_eq!(state.attempt_count(), 0);
    }

    #[test]
    fn test_circuit_state_retrying_after_one_attempt() {
        let mut cb = CircuitBreaker::new();
        let state = cb.record_attempt("beads-abc");
        assert_eq!(state, CircuitState::Retrying { attempt: 1 });
        assert!(state.can_retry());
        assert!(!state.is_tripped());
        assert_eq!(state.attempt_count(), 1);
    }

    #[test]
    fn test_circuit_state_retrying_after_two_attempts() {
        let mut cb = CircuitBreaker::new();
        cb.record_attempt("beads-abc");
        let state = cb.record_attempt("beads-abc");
        assert_eq!(state, CircuitState::Retrying { attempt: 2 });
        assert!(state.can_retry());
        assert!(!state.is_tripped());
        assert_eq!(state.attempt_count(), 2);
    }

    #[test]
    fn test_circuit_state_tripped_after_max_attempts() {
        let mut cb = CircuitBreaker::new();
        cb.record_attempt("beads-abc");
        cb.record_attempt("beads-abc");
        let state = cb.record_attempt("beads-abc");
        assert_eq!(state, CircuitState::Tripped { attempts: 3 });
        assert!(!state.can_retry());
        assert!(state.is_tripped());
        assert_eq!(state.attempt_count(), 3);
    }

    #[test]
    fn test_circuit_breaker_reset() {
        let mut cb = CircuitBreaker::new();
        cb.record_attempt("beads-abc");
        cb.record_attempt("beads-abc");
        cb.reset("beads-abc");
        let state = cb.state("beads-abc");
        assert_eq!(state, CircuitState::Closed);
        assert_eq!(cb.attempt_count("beads-abc"), 0);
    }

    #[test]
    fn test_circuit_breaker_independent_beads() {
        let mut cb = CircuitBreaker::new();
        cb.record_attempt("beads-abc");
        cb.record_attempt("beads-abc");
        cb.record_attempt("beads-def");

        assert_eq!(cb.attempt_count("beads-abc"), 2);
        assert_eq!(cb.attempt_count("beads-def"), 1);
        assert_eq!(cb.state("beads-abc"), CircuitState::Retrying { attempt: 2 });
        assert_eq!(cb.state("beads-def"), CircuitState::Retrying { attempt: 1 });
    }

    #[test]
    fn test_circuit_breaker_exceeds_max() {
        let mut cb = CircuitBreaker::new();
        for _ in 0..5 {
            cb.record_attempt("beads-abc");
        }
        let state = cb.state("beads-abc");
        assert_eq!(state, CircuitState::Tripped { attempts: 5 });
        assert!(!state.can_retry());
        assert_eq!(state.attempt_count(), 5);
    }

    #[test]
    fn test_circuit_state_display() {
        assert_eq!(format!("{}", CircuitState::Closed), "closed");
        assert_eq!(
            format!("{}", CircuitState::Retrying { attempt: 2 }),
            "retrying (attempt 2/3)"
        );
        assert_eq!(
            format!("{}", CircuitState::Tripped { attempts: 3 }),
            "tripped after 3 attempts"
        );
    }

    #[test]
    fn test_circuit_breaker_default() {
        let cb = CircuitBreaker::default();
        assert_eq!(cb.attempt_count("nonexistent"), 0);
        assert_eq!(cb.state("nonexistent"), CircuitState::Closed);
    }

    #[test]
    fn test_check_tripped_returns_none_when_not_tripped() {
        let mut cb = CircuitBreaker::new();
        cb.record_attempt("beads-abc");
        let result = cb.check_tripped("beads-abc", "some error", Path::new("/tmp/wt"));
        assert!(result.is_none(), "should not be tripped after 1 attempt");
    }

    #[test]
    fn test_check_tripped_returns_none_when_closed() {
        let cb = CircuitBreaker::new();
        let result = cb.check_tripped("beads-abc", "some error", Path::new("/tmp/wt"));
        assert!(result.is_none(), "should not be tripped when closed");
    }

    #[test]
    fn test_check_tripped_returns_some_when_tripped() {
        let mut cb = CircuitBreaker::new();
        for _ in 0..MAX_INTEGRATION_ATTEMPTS {
            cb.record_attempt("beads-abc");
        }
        let result = cb.check_tripped(
            "beads-abc",
            "type mismatch in foo.rs:42",
            Path::new("/tmp/wt"),
        );
        assert!(result.is_some(), "should be tripped after max attempts");

        let tripped = result.unwrap();
        assert_eq!(tripped.bead_id, "beads-abc");
        assert_eq!(tripped.error_summary, "type mismatch in foo.rs:42");
        assert_eq!(tripped.worktree_path, Path::new("/tmp/wt"));
        assert_eq!(tripped.attempts, MAX_INTEGRATION_ATTEMPTS);
    }

    #[test]
    fn test_tripped_failure_display_message() {
        let tripped = TrippedFailure {
            bead_id: "beads-abc".to_string(),
            error_summary: "type mismatch in src/metrics.rs:42".to_string(),
            worktree_path: PathBuf::from(".blacksmith/worktrees/worker-0-beads-abc"),
            attempts: 3,
        };
        let msg = tripped.display_message();
        assert!(msg.contains("HUMAN REVIEW NEEDED"));
        assert!(msg.contains("beads-abc"));
        assert!(msg.contains("3 attempts"));
        assert!(msg.contains("type mismatch in src/metrics.rs:42"));
        assert!(msg.contains(".blacksmith/worktrees/worker-0-beads-abc"));
        assert!(msg.contains("bd show beads-abc"));
        // Should contain ANSI escape codes for red/bold
        assert!(msg.contains("\x1b[1;31m"));
        assert!(msg.contains("\x1b[0m"));
    }

    #[test]
    fn test_tripped_failure_notes() {
        let tripped = TrippedFailure {
            bead_id: "beads-abc".to_string(),
            error_summary: "merge conflict in main.rs".to_string(),
            worktree_path: PathBuf::from("/tmp/wt"),
            attempts: 3,
        };
        let notes = tripped.failure_notes();
        assert_eq!(notes, "Integration failed: merge conflict in main.rs");
    }

    #[test]
    fn test_compiler_check_no_build_system_passes() {
        // When no Cargo.toml or tsconfig.json exists, compiler check should pass
        let dir = TempDir::new().unwrap();
        let queue = IntegrationQueue::new(dir.path().to_path_buf(), "main".to_string());
        let result = queue.run_compiler_check(dir.path());
        assert!(result.is_ok(), "should pass when no build system detected");
    }

    #[test]
    fn test_compiler_check_valid_rust_project() {
        // Create a minimal valid Rust project
        let dir = TempDir::new().unwrap();
        let root = dir.path();
        std::fs::write(
            root.join("Cargo.toml"),
            r#"[package]
name = "test-project"
version = "0.1.0"
edition = "2021"
"#,
        )
        .unwrap();
        let src = root.join("src");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(
            src.join("lib.rs"),
            "pub fn hello() -> &'static str { \"hello\" }\n",
        )
        .unwrap();

        let queue = IntegrationQueue::new(root.to_path_buf(), "main".to_string());
        let result = queue.run_compiler_check(root);
        assert!(
            result.is_ok(),
            "valid Rust project should pass: {:?}",
            result
        );
    }

    #[test]
    fn test_compiler_check_invalid_rust_project() {
        // Create a Rust project with a compile error
        let dir = TempDir::new().unwrap();
        let root = dir.path();
        std::fs::write(
            root.join("Cargo.toml"),
            r#"[package]
name = "test-bad"
version = "0.1.0"
edition = "2021"
"#,
        )
        .unwrap();
        let src = root.join("src");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(src.join("lib.rs"), "fn hello() -> i32 { \"not an int\" }\n").unwrap();

        let queue = IntegrationQueue::new(root.to_path_buf(), "main".to_string());
        let result = queue.run_compiler_check(root);
        assert!(result.is_err(), "should fail with compile error");
        let errors = result.unwrap_err();
        assert!(
            errors.contains("mismatched types") || errors.contains("error"),
            "error output should contain compiler diagnostics: {errors}"
        );
    }

    #[test]
    fn test_git_add_and_commit() {
        let dir = init_test_repo();
        let repo_dir = dir.path();

        let queue = IntegrationQueue::new(repo_dir.to_path_buf(), "main".to_string());

        // Add a new file
        std::fs::write(repo_dir.join("newfile.txt"), "content").unwrap();

        // Stage and commit
        let result = queue.git_add_and_commit(repo_dir, "test commit");
        assert!(
            result.is_ok(),
            "git add and commit should succeed: {:?}",
            result
        );

        // Verify the commit was made
        let log = StdCommand::new("git")
            .args(["log", "--oneline", "-1"])
            .current_dir(repo_dir)
            .output()
            .unwrap();
        let log_output = String::from_utf8_lossy(&log.stdout);
        assert!(log_output.contains("test commit"));
    }

    #[test]
    fn test_spawn_integration_agent_sync() {
        let dir = init_test_repo();
        let repo_dir = dir.path();

        let queue = IntegrationQueue::new(repo_dir.to_path_buf(), "main".to_string());
        let agent = crate::config::ResolvedAgentConfig {
            command: "echo".to_string(),
            args: vec!["fixing: {prompt}".to_string()],
            adapter: None,
            prompt_via: crate::config::PromptVia::Arg,
            env: HashMap::new(),
        };

        let result = queue.spawn_integration_agent_sync(&agent, repo_dir, "test errors");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(0));
    }

    #[test]
    fn test_spawn_integration_agent_nonexistent_command() {
        let dir = init_test_repo();
        let repo_dir = dir.path();

        let queue = IntegrationQueue::new(repo_dir.to_path_buf(), "main".to_string());
        let agent = crate::config::ResolvedAgentConfig {
            command: "nonexistent-agent-xyz-12345".to_string(),
            args: vec![],
            adapter: None,
            prompt_via: crate::config::PromptVia::Arg,
            env: HashMap::new(),
        };

        let result = queue.spawn_integration_agent_sync(&agent, repo_dir, "test");
        assert!(result.is_err(), "should fail with nonexistent command");
    }

    #[test]
    fn test_integration_with_manifest_application() {
        let dir = init_test_repo();
        let repo_dir = dir.path();
        let wt_dir = repo_dir.join("worktrees");
        std::fs::create_dir_all(&wt_dir).unwrap();

        let db_path = repo_dir.join("test.db");
        let conn = db::open_or_create(&db_path).unwrap();

        let assignment_id = db::insert_worker_assignment(
            &conn,
            0,
            "beads-manifest",
            "/tmp/wt-0",
            "completed",
            None,
        )
        .unwrap();

        let wt_path = create_worktree_with_commit(repo_dir, &wt_dir, 0, "beads-manifest");

        // Create a task_manifest.toml in the worktree (but no lib.rs so it won't modify anything)
        std::fs::write(wt_path.join("task_manifest.toml"), "").unwrap();

        let queue = IntegrationQueue::new(repo_dir.to_path_buf(), "main".to_string());
        let mut cb = CircuitBreaker::new();
        let result = queue.integrate(
            0,
            assignment_id,
            "beads-manifest",
            &wt_path,
            &conn,
            None,
            &mut cb,
        );

        assert!(
            result.success,
            "integration with empty manifest should succeed: {:?}",
            result.failure_reason
        );
    }

    // --- Reconciliation tracker tests ---

    #[test]
    fn test_reconciliation_tracker_new() {
        let tracker = ReconciliationTracker::new(3);
        assert_eq!(tracker.count(), 0);
        assert!(tracker.recent_beads().is_empty());
    }

    #[test]
    fn test_reconciliation_tracker_record_below_threshold() {
        let mut tracker = ReconciliationTracker::new(3);
        assert!(!tracker.record_success("beads-a"));
        assert_eq!(tracker.count(), 1);
        assert!(!tracker.record_success("beads-b"));
        assert_eq!(tracker.count(), 2);
        assert_eq!(tracker.recent_beads(), &["beads-a", "beads-b"]);
    }

    #[test]
    fn test_reconciliation_tracker_triggers_at_threshold() {
        let mut tracker = ReconciliationTracker::new(3);
        tracker.record_success("beads-a");
        tracker.record_success("beads-b");
        let should_reconcile = tracker.record_success("beads-c");
        assert!(should_reconcile);
        assert_eq!(tracker.count(), 3);
        assert_eq!(tracker.recent_beads(), &["beads-a", "beads-b", "beads-c"]);
    }

    #[test]
    fn test_reconciliation_tracker_reset() {
        let mut tracker = ReconciliationTracker::new(3);
        tracker.record_success("beads-a");
        tracker.record_success("beads-b");
        tracker.record_success("beads-c");
        tracker.reset();
        assert_eq!(tracker.count(), 0);
        assert!(tracker.recent_beads().is_empty());
    }

    #[test]
    fn test_reconciliation_tracker_zero_every_never_triggers() {
        let mut tracker = ReconciliationTracker::new(0);
        assert!(!tracker.record_success("beads-a"));
        assert!(!tracker.record_success("beads-b"));
        assert!(!tracker.record_success("beads-c"));
    }

    #[test]
    fn test_reconciliation_tracker_every_one_always_triggers() {
        let mut tracker = ReconciliationTracker::new(1);
        assert!(tracker.record_success("beads-a"));
        tracker.reset();
        assert!(tracker.record_success("beads-b"));
    }

    #[test]
    fn test_reconciliation_no_build_system_passes() {
        // When no Cargo.toml or package.json exists, reconciliation should pass
        let dir = TempDir::new().unwrap();
        StdCommand::new("git")
            .args(["init"])
            .current_dir(dir.path())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();

        let queue = IntegrationQueue::new(dir.path().to_path_buf(), "main".to_string());
        let mut tracker = ReconciliationTracker::new(1);
        tracker.record_success("beads-a");
        let result = queue.run_reconciliation(&mut tracker);
        assert!(result.passed);
        assert!(result.flagged_beads.is_empty());
        assert_eq!(tracker.count(), 0); // should be reset
    }

    #[test]
    fn test_reconciliation_valid_rust_project_passes() {
        let dir = TempDir::new().unwrap();
        let root = dir.path();

        // Create a minimal valid Rust project with a passing test
        std::fs::write(
            root.join("Cargo.toml"),
            r#"[package]
name = "reconcile-test"
version = "0.1.0"
edition = "2021"
"#,
        )
        .unwrap();
        let src = root.join("src");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(
            src.join("lib.rs"),
            r#"
pub fn add(a: i32, b: i32) -> i32 { a + b }

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_add() { assert_eq!(add(1, 2), 3); }
}
"#,
        )
        .unwrap();

        let queue = IntegrationQueue::new(root.to_path_buf(), "main".to_string());
        let mut tracker = ReconciliationTracker::new(2);
        tracker.record_success("beads-x");
        tracker.record_success("beads-y");
        let result = queue.run_reconciliation(&mut tracker);
        assert!(result.passed);
        assert!(result.flagged_beads.is_empty());
    }

    #[test]
    fn test_reconciliation_failing_rust_project_flags_beads() {
        let dir = TempDir::new().unwrap();
        let root = dir.path();

        // Create a Rust project with a failing test
        std::fs::write(
            root.join("Cargo.toml"),
            r#"[package]
name = "reconcile-fail-test"
version = "0.1.0"
edition = "2021"
"#,
        )
        .unwrap();
        let src = root.join("src");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(
            src.join("lib.rs"),
            r#"
#[cfg(test)]
mod tests {
    #[test]
    fn test_fail() { assert_eq!(1, 2); }
}
"#,
        )
        .unwrap();

        let queue = IntegrationQueue::new(root.to_path_buf(), "main".to_string());
        let mut tracker = ReconciliationTracker::new(3);
        tracker.record_success("beads-a");
        tracker.record_success("beads-b");
        tracker.record_success("beads-c");
        let result = queue.run_reconciliation(&mut tracker);
        assert!(!result.passed);
        assert_eq!(result.flagged_beads, vec!["beads-a", "beads-b", "beads-c"]);
        // Tracker should still be reset after failure
        assert_eq!(tracker.count(), 0);
    }

    #[test]
    fn test_integration_with_compiler_check_no_build_system() {
        // When there's no build system, compiler check is skipped and integration succeeds
        let dir = init_test_repo();
        let repo_dir = dir.path();
        let wt_dir = repo_dir.join("worktrees");
        std::fs::create_dir_all(&wt_dir).unwrap();

        let db_path = repo_dir.join("test.db");
        let conn = db::open_or_create(&db_path).unwrap();

        let assignment_id =
            db::insert_worker_assignment(&conn, 0, "beads-nocheck", "/tmp/wt-0", "completed", None)
                .unwrap();

        let wt_path = create_worktree_with_commit(repo_dir, &wt_dir, 0, "beads-nocheck");

        // Provide an integration agent config — but since there's no Cargo.toml/tsconfig,
        // the compiler check should be skipped
        let agent = crate::config::ResolvedAgentConfig {
            command: "echo".to_string(),
            args: vec!["should-not-run".to_string()],
            adapter: None,
            prompt_via: crate::config::PromptVia::Arg,
            env: HashMap::new(),
        };

        let queue = IntegrationQueue::new(repo_dir.to_path_buf(), "main".to_string());
        let mut cb = CircuitBreaker::new();
        let result = queue.integrate(
            0,
            assignment_id,
            "beads-nocheck",
            &wt_path,
            &conn,
            Some(&agent),
            &mut cb,
        );

        assert!(
            result.success,
            "integration should succeed when no build system: {:?}",
            result.failure_reason
        );
        // Circuit breaker should not have been triggered
        assert_eq!(cb.attempt_count("beads-nocheck"), 0);
    }

    // --- Rollback tests ---

    #[test]
    fn test_rollback_successful() {
        let dir = init_test_repo();
        let repo_dir = dir.path();
        let wt_dir = repo_dir.join("worktrees");
        std::fs::create_dir_all(&wt_dir).unwrap();

        let db_path = repo_dir.join("test.db");
        let conn = db::open_or_create(&db_path).unwrap();

        // Integrate a task first
        let assignment_id =
            db::insert_worker_assignment(&conn, 0, "beads-rb", "/tmp/wt-0", "completed", None)
                .unwrap();
        let wt_path = create_worktree_with_commit(repo_dir, &wt_dir, 0, "beads-rb");

        let queue = IntegrationQueue::new(repo_dir.to_path_buf(), "main".to_string());
        let mut cb = CircuitBreaker::new();
        let result = queue.integrate(0, assignment_id, "beads-rb", &wt_path, &conn, None, &mut cb);
        assert!(result.success);
        let merge_commit = result.merge_commit.unwrap();

        // Now rollback
        let rollback = queue
            .rollback("beads-rb", &merge_commit, assignment_id, None, &conn, false)
            .unwrap();

        assert!(rollback.success);
        assert!(rollback.reverted_commit.is_some());
        assert!(rollback.blocked_by.is_empty());

        // Verify the assignment is now rolled_back
        let wa = db::get_worker_assignment(&conn, assignment_id)
            .unwrap()
            .unwrap();
        assert_eq!(wa.status, "rolled_back");
    }

    #[test]
    fn test_rollback_blocked_by_entanglement() {
        let dir = init_test_repo();
        let repo_dir = dir.path();
        let wt_dir = repo_dir.join("worktrees");
        std::fs::create_dir_all(&wt_dir).unwrap();

        let db_path = repo_dir.join("test.db");
        let conn = db::open_or_create(&db_path).unwrap();

        // Integrate task A
        let aid_a =
            db::insert_worker_assignment(&conn, 0, "beads-a", "/tmp/wt-0", "integrated", None)
                .unwrap();
        db::insert_integration_log(
            &conn,
            aid_a,
            "2026-02-15T10:00:00Z",
            "commit_a",
            None,
            None,
            false,
        )
        .unwrap();

        // Integrate task B that imports from task A
        let aid_b =
            db::insert_worker_assignment(&conn, 1, "beads-b", "/tmp/wt-1", "integrated", None)
                .unwrap();
        db::insert_integration_log(
            &conn,
            aid_b,
            "2026-02-15T11:00:00Z",
            "commit_b",
            None,
            Some("imports from beads-a::module"),
            false,
        )
        .unwrap();

        let queue = IntegrationQueue::new(repo_dir.to_path_buf(), "main".to_string());

        // Try to rollback A — should be blocked
        let rollback = queue
            .rollback("beads-a", "commit_a", aid_a, None, &conn, false)
            .unwrap();

        assert!(!rollback.success);
        assert!(rollback.reverted_commit.is_none());
        assert_eq!(rollback.blocked_by, vec!["beads-b"]);
    }

    #[test]
    fn test_rollback_forced_despite_entanglement() {
        let dir = init_test_repo();
        let repo_dir = dir.path();
        let wt_dir = repo_dir.join("worktrees");
        std::fs::create_dir_all(&wt_dir).unwrap();

        let db_path = repo_dir.join("test.db");
        let conn = db::open_or_create(&db_path).unwrap();

        // Integrate task A first
        let aid_a =
            db::insert_worker_assignment(&conn, 0, "beads-a", "/tmp/wt-0", "completed", None)
                .unwrap();
        let wt_path = create_worktree_with_commit(repo_dir, &wt_dir, 0, "beads-a");

        let queue = IntegrationQueue::new(repo_dir.to_path_buf(), "main".to_string());
        let mut cb = CircuitBreaker::new();
        let result = queue.integrate(0, aid_a, "beads-a", &wt_path, &conn, None, &mut cb);
        assert!(result.success);
        let merge_commit_a = result.merge_commit.unwrap();

        // Create an entangled task B
        let aid_b =
            db::insert_worker_assignment(&conn, 1, "beads-b", "/tmp/wt-1", "integrated", None)
                .unwrap();
        db::insert_integration_log(
            &conn,
            aid_b,
            "2026-02-15T11:00:00Z",
            "commit_b",
            None,
            Some("imports from beads-a"),
            false,
        )
        .unwrap();

        // Force rollback A — should succeed despite entanglement
        let rollback = queue
            .rollback(
                "beads-a",
                &merge_commit_a,
                aid_a,
                None,
                &conn,
                true, // force
            )
            .unwrap();

        assert!(rollback.success);
        assert!(rollback.reverted_commit.is_some());
    }

    #[test]
    fn test_rollback_result_fields() {
        let result = RollbackResult {
            success: true,
            reverted_commit: Some("abc123".to_string()),
            manifest_entries_reversed: 3,
            blocked_by: Vec::new(),
        };
        assert!(result.success);
        assert_eq!(result.reverted_commit.as_deref(), Some("abc123"));
        assert_eq!(result.manifest_entries_reversed, 3);
        assert!(result.blocked_by.is_empty());
    }
}
