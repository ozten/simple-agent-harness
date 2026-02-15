/// Status file: writes `blacksmith.status` as JSON on every state transition.
///
/// Uses atomic write pattern: write to temp file then rename.
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Harness states written to the status file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HarnessState {
    Starting,
    PreHooks,
    SessionRunning,
    WatchdogKill,
    Retrying,
    PostHooks,
    RateLimitedBackoff,
    Idle,
    ShuttingDown,
}

/// The JSON payload written to `blacksmith.status`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusData {
    pub pid: u32,
    pub state: HarnessState,
    pub iteration: u32,
    pub max_iterations: u32,
    pub global_iteration: u64,
    pub output_file: String,
    pub output_bytes: u64,
    pub session_start: Option<DateTime<Utc>>,
    pub last_update: DateTime<Utc>,
    pub last_completed_iteration: Option<u64>,
    pub last_committed: bool,
    pub consecutive_rate_limits: u32,
}

/// Manages the status file lifecycle.
pub struct StatusFile {
    path: PathBuf,
}

impl StatusFile {
    /// Create a new StatusFile writer for the given path.
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    /// Atomically write status data to the status file.
    ///
    /// Writes to a temporary file in the same directory, then renames
    /// to ensure readers never see a partial write.
    pub fn write(&self, data: &StatusData) -> Result<(), StatusError> {
        let json =
            serde_json::to_string_pretty(data).map_err(|e| StatusError::Serialize { source: e })?;

        let dir = self.path.parent().unwrap_or(Path::new("."));
        let tmp_path = dir.join(format!(".blacksmith.status.tmp.{}", std::process::id()));

        std::fs::write(&tmp_path, json.as_bytes()).map_err(|e| StatusError::Write {
            path: tmp_path.clone(),
            source: e,
        })?;

        std::fs::rename(&tmp_path, &self.path).map_err(|e| StatusError::Rename {
            from: tmp_path,
            to: self.path.clone(),
            source: e,
        })?;

        Ok(())
    }

    /// Remove the status file (on clean shutdown).
    pub fn remove(&self) {
        let _ = std::fs::remove_file(&self.path);
    }

    /// Path to the status file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Read and deserialize the status file.
    /// Returns None if the file does not exist.
    pub fn read(&self) -> Result<Option<StatusData>, StatusError> {
        match std::fs::read_to_string(&self.path) {
            Ok(contents) => {
                let data = serde_json::from_str(&contents)
                    .map_err(|e| StatusError::Deserialize { source: e })?;
                Ok(Some(data))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(StatusError::Read {
                path: self.path.clone(),
                source: e,
            }),
        }
    }
}

/// Mutable state tracker that builds StatusData for each update.
pub struct StatusTracker {
    file: StatusFile,
    pid: u32,
    max_iterations: u32,
    iteration: u32,
    global_iteration: u64,
    output_file: String,
    output_bytes: u64,
    session_start: Option<DateTime<Utc>>,
    last_completed_iteration: Option<u64>,
    last_committed: bool,
    consecutive_rate_limits: u32,
}

impl StatusTracker {
    /// Create a new tracker.
    pub fn new(status_path: PathBuf, max_iterations: u32, global_iteration: u64) -> Self {
        Self {
            file: StatusFile::new(status_path),
            pid: std::process::id(),
            max_iterations,
            iteration: 0,
            global_iteration,
            output_file: String::new(),
            output_bytes: 0,
            session_start: None,
            last_completed_iteration: None,
            last_committed: false,
            consecutive_rate_limits: 0,
        }
    }

    /// Update and write the status file with the given state.
    pub fn update(&self, state: HarnessState) {
        let data = StatusData {
            pid: self.pid,
            state,
            iteration: self.iteration,
            max_iterations: self.max_iterations,
            global_iteration: self.global_iteration,
            output_file: self.output_file.clone(),
            output_bytes: self.output_bytes,
            session_start: self.session_start,
            last_update: Utc::now(),
            last_completed_iteration: self.last_completed_iteration,
            last_committed: self.last_committed,
            consecutive_rate_limits: self.consecutive_rate_limits,
        };

        if let Err(e) = self.file.write(&data) {
            tracing::warn!(error = %e, "failed to write status file");
        }
    }

    /// Set the current productive iteration count.
    pub fn set_iteration(&mut self, iteration: u32) {
        self.iteration = iteration;
    }

    /// Set the global iteration counter.
    pub fn set_global_iteration(&mut self, global: u64) {
        self.global_iteration = global;
    }

    /// Set the current output file path.
    pub fn set_output_file(&mut self, path: &str) {
        self.output_file = path.to_string();
    }

    /// Set the current output size.
    pub fn set_output_bytes(&mut self, bytes: u64) {
        self.output_bytes = bytes;
    }

    /// Mark the start of a new session.
    pub fn set_session_start(&mut self) {
        self.session_start = Some(Utc::now());
    }

    /// Record the last completed iteration.
    pub fn set_last_completed(&mut self, global: u64) {
        self.last_completed_iteration = Some(global);
    }

    /// Set whether the last session committed.
    pub fn set_last_committed(&mut self, committed: bool) {
        self.last_committed = committed;
    }

    /// Set consecutive rate limit count.
    pub fn set_consecutive_rate_limits(&mut self, count: u32) {
        self.consecutive_rate_limits = count;
    }

    /// Remove the status file.
    pub fn remove(&self) {
        self.file.remove();
    }
}

/// Format bytes into a human-readable string (e.g., "48.2 KB").
fn format_bytes(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{} B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    }
}

/// Format a duration into human-readable uptime (e.g., "3h 42m").
fn format_uptime(duration: chrono::Duration) -> String {
    let total_secs = duration.num_seconds().max(0);
    let hours = total_secs / 3600;
    let mins = (total_secs % 3600) / 60;
    if hours > 0 {
        format!("{}h {}m", hours, mins)
    } else {
        format!("{}m", mins)
    }
}

impl HarnessState {
    /// Human-friendly label for display.
    fn display_label(self) -> &'static str {
        match self {
            HarnessState::Starting => "starting",
            HarnessState::PreHooks => "pre-hooks",
            HarnessState::SessionRunning => "running",
            HarnessState::WatchdogKill => "watchdog-kill",
            HarnessState::Retrying => "retrying",
            HarnessState::PostHooks => "post-hooks",
            HarnessState::RateLimitedBackoff => "rate-limited",
            HarnessState::Idle => "idle",
            HarnessState::ShuttingDown => "shutting-down",
        }
    }
}

/// Display the status of a running harness.
/// Reads the status file and prints formatted output to stdout.
/// Returns Ok(true) if status was displayed, Ok(false) if no harness is running.
pub fn display_status(
    status_path: &Path,
    db_path: Option<&Path>,
    workers: u32,
) -> Result<bool, StatusError> {
    let file = StatusFile::new(status_path.to_path_buf());
    let data = match file.read()? {
        Some(data) => data,
        None => return Ok(false),
    };

    // Check if the PID is still alive
    let pid_alive =
        nix::sys::signal::kill(nix::unistd::Pid::from_raw(data.pid as i32), None).is_ok();
    let state_label = if pid_alive {
        data.state.display_label()
    } else {
        "not running (stale status file)"
    };

    let worker_label = if workers > 1 {
        format!("{} workers active", workers)
    } else {
        format!("PID {}", data.pid)
    };

    println!("Status: {} ({})", state_label, worker_label);
    println!(
        "Current iteration: {}/{} (global: {})",
        data.iteration, data.max_iterations, data.global_iteration
    );

    // Output info
    let size_str = format_bytes(data.output_bytes);
    if !data.output_file.is_empty() {
        println!("Session output: {} ({})", size_str, data.output_file);
    } else {
        println!("Session output: {}", size_str);
    }

    // Uptime from session_start
    if let Some(start) = data.session_start {
        let uptime = Utc::now() - start;
        println!("Uptime: {}", format_uptime(uptime));
    }

    // Bead progress and ETA
    if let Some(db_path) = db_path {
        display_bead_progress(db_path, workers);
    }

    // Last completed iteration
    if let Some(last) = data.last_completed_iteration {
        let commit_info = if data.last_committed {
            " — committed"
        } else {
            ""
        };
        println!("Last completed: iteration {}{}", last, commit_info);
    }

    // Rate limits
    if data.consecutive_rate_limits > 0 {
        println!("Consecutive rate limits: {}", data.consecutive_rate_limits);
    }

    Ok(true)
}

/// Display bead progress, ETA, and failed beads count.
fn display_bead_progress(db_path: &Path, workers: u32) {
    use crate::db;
    use crate::estimation;

    let conn = match db::open_or_create(db_path) {
        Ok(c) => c,
        Err(_) => return,
    };

    let all_metrics = match db::all_bead_metrics(&conn) {
        Ok(m) => m,
        Err(_) => return,
    };

    if all_metrics.is_empty() {
        return;
    }

    let completed = all_metrics
        .iter()
        .filter(|m| m.completed_at.is_some())
        .count();

    let open_beads = estimation::query_open_beads();
    let total_schedulable = completed + open_beads.len();

    if total_schedulable == 0 {
        return;
    }

    let pct = if total_schedulable > 0 {
        (completed as f64 / total_schedulable as f64 * 100.0) as u32
    } else {
        0
    };

    let est = estimation::estimate(&conn, &open_beads, workers);

    // Count failed beads (bead_metrics entries with no completed_at, indicating in-progress/failed)
    let failed_count = count_failed_beads();

    println!(
        "Progress: {}/{} beads ({}%)",
        completed, total_schedulable, pct
    );

    if est.open_count > 0 {
        let schedulable_label = if !est.cycled_beads.is_empty() {
            format!(
                "  Schedulable: {} beads ({} excluded — dependency cycle)",
                total_schedulable - est.cycled_beads.len(),
                est.cycled_beads.len()
            )
        } else {
            String::new()
        };

        if !schedulable_label.is_empty() {
            println!("{}", schedulable_label);
        }

        if let Some(avg) = est.avg_time_per_bead {
            let total_completed_time: f64 = all_metrics
                .iter()
                .filter(|m| m.completed_at.is_some())
                .map(|m| m.wall_time_secs)
                .sum();
            println!(
                "  Completed:   {} beads in {}",
                completed,
                format_duration_f64(total_completed_time)
            );
            println!("  Remaining:   {} beads", est.open_count);

            if workers > 1 {
                if let Some(serial) = est.serial_secs {
                    println!("  Serial ETA:    ~{}", format_duration_f64(serial));
                }
                if let Some(parallel) = est.parallel_secs {
                    println!(
                        "  Parallel ETA:  ~{} @ {} workers",
                        format_duration_f64(parallel),
                        workers
                    );
                }
            } else if let Some(serial) = est.serial_secs {
                println!(
                    "  ETA: ~{} (avg {}/bead)",
                    format_duration_f64(serial),
                    format_duration_f64(avg)
                );
            }
        }

        if !est.cycled_beads.is_empty() {
            println!(
                "  \u{26a0} Cycle:    {} beads (run `bd dep cycles`)",
                est.cycled_beads.len()
            );
        }
    }

    if failed_count > 0 {
        println!("  Failed: {} beads", failed_count);
    }
}

/// Count beads with failed status from `bd list`.
fn count_failed_beads() -> usize {
    match std::process::Command::new("bd")
        .args(["list", "--status=in_progress", "--json"])
        .output()
    {
        Ok(output) if output.status.success() => {
            // Count beads that have [FAILED-ATTEMPT] in notes
            let json_str = String::from_utf8_lossy(&output.stdout);
            let parsed: Result<Vec<serde_json::Value>, _> = serde_json::from_str(&json_str);
            match parsed {
                Ok(beads) => beads
                    .iter()
                    .filter(|b| {
                        b.get("notes")
                            .and_then(|n| n.as_str())
                            .map(|n| n.contains("[FAILED-ATTEMPT]"))
                            .unwrap_or(false)
                    })
                    .count(),
                Err(_) => 0,
            }
        }
        _ => 0,
    }
}

/// Format seconds (f64) as a compact human-readable duration.
fn format_duration_f64(secs: f64) -> String {
    let total_secs = secs as u64;
    if total_secs < 60 {
        format!("{}s", total_secs)
    } else if total_secs < 3600 {
        let mins = total_secs / 60;
        let remaining = total_secs % 60;
        if remaining > 0 {
            format!("{}.{}m", mins, remaining * 10 / 60)
        } else {
            format!("{}m", mins)
        }
    } else {
        let hours = total_secs / 3600;
        let mins = (total_secs % 3600) / 60;
        if mins > 0 {
            format!("{}h {}m", hours, mins)
        } else {
            format!("{}h", hours)
        }
    }
}

/// Errors from status file operations.
#[derive(Debug)]
pub enum StatusError {
    Serialize {
        source: serde_json::Error,
    },
    Deserialize {
        source: serde_json::Error,
    },
    Write {
        path: PathBuf,
        source: std::io::Error,
    },
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    Rename {
        from: PathBuf,
        to: PathBuf,
        source: std::io::Error,
    },
}

impl std::fmt::Display for StatusError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StatusError::Serialize { source } => write!(f, "failed to serialize status: {source}"),
            StatusError::Deserialize { source } => {
                write!(f, "failed to deserialize status: {source}")
            }
            StatusError::Write { path, source } => {
                write!(
                    f,
                    "failed to write temp status file {}: {source}",
                    path.display()
                )
            }
            StatusError::Read { path, source } => {
                write!(f, "failed to read status file {}: {source}", path.display())
            }
            StatusError::Rename { from, to, source } => {
                write!(
                    f,
                    "failed to rename {} -> {}: {source}",
                    from.display(),
                    to.display()
                )
            }
        }
    }
}

impl std::error::Error for StatusError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            StatusError::Serialize { source } => Some(source),
            StatusError::Deserialize { source } => Some(source),
            StatusError::Write { source, .. } => Some(source),
            StatusError::Read { source, .. } => Some(source),
            StatusError::Rename { source, .. } => Some(source),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_status_file_atomic_write() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("blacksmith.status");
        let sf = StatusFile::new(path.clone());

        let data = StatusData {
            pid: 12345,
            state: HarnessState::SessionRunning,
            iteration: 3,
            max_iterations: 25,
            global_iteration: 103,
            output_file: "claude-iteration-103.jsonl".to_string(),
            output_bytes: 49331,
            session_start: Some(Utc::now()),
            last_update: Utc::now(),
            last_completed_iteration: Some(102),
            last_committed: true,
            consecutive_rate_limits: 0,
        };

        sf.write(&data).unwrap();

        // Verify the file exists and is valid JSON
        let contents = std::fs::read_to_string(&path).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&contents).unwrap();
        assert_eq!(parsed["pid"], 12345);
        assert_eq!(parsed["state"], "session_running");
        assert_eq!(parsed["iteration"], 3);
        assert_eq!(parsed["max_iterations"], 25);
        assert_eq!(parsed["global_iteration"], 103);
        assert_eq!(parsed["output_file"], "claude-iteration-103.jsonl");
        assert_eq!(parsed["output_bytes"], 49331);
        assert_eq!(parsed["last_committed"], true);
        assert_eq!(parsed["consecutive_rate_limits"], 0);

        // Verify no temp file left behind
        let tmp_path = dir
            .path()
            .join(format!(".blacksmith.status.tmp.{}", std::process::id()));
        assert!(
            !tmp_path.exists(),
            "temp file should be cleaned up by rename"
        );
    }

    #[test]
    fn test_status_file_overwrite() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("blacksmith.status");
        let sf = StatusFile::new(path.clone());

        let mut data = StatusData {
            pid: 1,
            state: HarnessState::Starting,
            iteration: 0,
            max_iterations: 10,
            global_iteration: 0,
            output_file: String::new(),
            output_bytes: 0,
            session_start: None,
            last_update: Utc::now(),
            last_completed_iteration: None,
            last_committed: false,
            consecutive_rate_limits: 0,
        };

        sf.write(&data).unwrap();

        // Write again with updated state
        data.state = HarnessState::SessionRunning;
        data.iteration = 1;
        sf.write(&data).unwrap();

        let contents = std::fs::read_to_string(&path).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&contents).unwrap();
        assert_eq!(parsed["state"], "session_running");
        assert_eq!(parsed["iteration"], 1);
    }

    #[test]
    fn test_status_file_remove() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("blacksmith.status");
        let sf = StatusFile::new(path.clone());

        let data = StatusData {
            pid: 1,
            state: HarnessState::Starting,
            iteration: 0,
            max_iterations: 10,
            global_iteration: 0,
            output_file: String::new(),
            output_bytes: 0,
            session_start: None,
            last_update: Utc::now(),
            last_completed_iteration: None,
            last_committed: false,
            consecutive_rate_limits: 0,
        };

        sf.write(&data).unwrap();
        assert!(path.exists());

        sf.remove();
        assert!(!path.exists());
    }

    #[test]
    fn test_all_harness_states_serialize() {
        let states = vec![
            (HarnessState::Starting, "starting"),
            (HarnessState::PreHooks, "pre_hooks"),
            (HarnessState::SessionRunning, "session_running"),
            (HarnessState::WatchdogKill, "watchdog_kill"),
            (HarnessState::Retrying, "retrying"),
            (HarnessState::PostHooks, "post_hooks"),
            (HarnessState::RateLimitedBackoff, "rate_limited_backoff"),
            (HarnessState::Idle, "idle"),
            (HarnessState::ShuttingDown, "shutting_down"),
        ];

        for (state, expected_str) in states {
            let json = serde_json::to_string(&state).unwrap();
            assert_eq!(json, format!("\"{}\"", expected_str));
        }
    }

    #[test]
    fn test_status_tracker_lifecycle() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("blacksmith.status");

        let mut tracker = StatusTracker::new(path.clone(), 25, 100);

        // Starting state
        tracker.update(HarnessState::Starting);
        let contents = std::fs::read_to_string(&path).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&contents).unwrap();
        assert_eq!(parsed["state"], "starting");
        assert_eq!(parsed["global_iteration"], 100);
        assert_eq!(parsed["max_iterations"], 25);

        // Session running
        tracker.set_iteration(1);
        tracker.set_global_iteration(100);
        tracker.set_output_file("claude-iteration-100.jsonl");
        tracker.set_session_start();
        tracker.update(HarnessState::SessionRunning);

        let contents = std::fs::read_to_string(&path).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&contents).unwrap();
        assert_eq!(parsed["state"], "session_running");
        assert_eq!(parsed["iteration"], 1);
        assert_eq!(parsed["output_file"], "claude-iteration-100.jsonl");
        assert!(parsed["session_start"].is_string());

        // Idle after completion
        tracker.set_output_bytes(50000);
        tracker.set_last_completed(100);
        tracker.set_global_iteration(101);
        tracker.update(HarnessState::Idle);

        let contents = std::fs::read_to_string(&path).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&contents).unwrap();
        assert_eq!(parsed["state"], "idle");
        assert_eq!(parsed["output_bytes"], 50000);
        assert_eq!(parsed["last_completed_iteration"], 100);
        assert_eq!(parsed["global_iteration"], 101);

        // Clean shutdown
        tracker.remove();
        assert!(!path.exists());
    }

    #[test]
    fn test_status_file_write_to_nonexistent_dir_fails() {
        let sf = StatusFile::new(PathBuf::from("/nonexistent/dir/blacksmith.status"));
        let data = StatusData {
            pid: 1,
            state: HarnessState::Starting,
            iteration: 0,
            max_iterations: 10,
            global_iteration: 0,
            output_file: String::new(),
            output_bytes: 0,
            session_start: None,
            last_update: Utc::now(),
            last_completed_iteration: None,
            last_committed: false,
            consecutive_rate_limits: 0,
        };

        let result = sf.write(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_status_error_display() {
        let err = StatusError::Write {
            path: PathBuf::from("/tmp/test"),
            source: std::io::Error::new(std::io::ErrorKind::PermissionDenied, "no perms"),
        };
        let msg = err.to_string();
        assert!(msg.contains("failed to write temp status file"));
        assert!(msg.contains("no perms"));
    }

    #[test]
    fn test_status_file_read_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("blacksmith.status");
        let sf = StatusFile::new(path.clone());

        let now = Utc::now();
        let data = StatusData {
            pid: 42,
            state: HarnessState::SessionRunning,
            iteration: 5,
            max_iterations: 25,
            global_iteration: 105,
            output_file: "claude-iteration-105.jsonl".to_string(),
            output_bytes: 12345,
            session_start: Some(now),
            last_update: now,
            last_completed_iteration: Some(104),
            last_committed: true,
            consecutive_rate_limits: 0,
        };

        sf.write(&data).unwrap();
        let read_back = sf.read().unwrap().unwrap();

        assert_eq!(read_back.pid, 42);
        assert_eq!(read_back.state, HarnessState::SessionRunning);
        assert_eq!(read_back.iteration, 5);
        assert_eq!(read_back.max_iterations, 25);
        assert_eq!(read_back.global_iteration, 105);
        assert_eq!(read_back.output_file, "claude-iteration-105.jsonl");
        assert_eq!(read_back.output_bytes, 12345);
        assert_eq!(read_back.last_completed_iteration, Some(104));
        assert!(read_back.last_committed);
    }

    #[test]
    fn test_status_file_read_missing_returns_none() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("nonexistent.status");
        let sf = StatusFile::new(path);

        assert!(sf.read().unwrap().is_none());
    }

    #[test]
    fn test_status_file_read_invalid_json() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("blacksmith.status");
        std::fs::write(&path, "not valid json").unwrap();
        let sf = StatusFile::new(path);

        let result = sf.read();
        assert!(result.is_err());
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1023), "1023 B");
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(49331), "48.2 KB");
        assert_eq!(format_bytes(1048576), "1.0 MB");
        assert_eq!(format_bytes(1572864), "1.5 MB");
    }

    #[test]
    fn test_format_uptime() {
        assert_eq!(format_uptime(chrono::Duration::seconds(0)), "0m");
        assert_eq!(format_uptime(chrono::Duration::seconds(59)), "0m");
        assert_eq!(format_uptime(chrono::Duration::seconds(60)), "1m");
        assert_eq!(format_uptime(chrono::Duration::seconds(3600)), "1h 0m");
        assert_eq!(
            format_uptime(chrono::Duration::seconds(3 * 3600 + 42 * 60)),
            "3h 42m"
        );
    }

    #[test]
    fn test_harness_state_display_labels() {
        assert_eq!(HarnessState::Starting.display_label(), "starting");
        assert_eq!(HarnessState::SessionRunning.display_label(), "running");
        assert_eq!(HarnessState::Idle.display_label(), "idle");
        assert_eq!(HarnessState::ShuttingDown.display_label(), "shutting-down");
        assert_eq!(
            HarnessState::RateLimitedBackoff.display_label(),
            "rate-limited"
        );
        assert_eq!(HarnessState::WatchdogKill.display_label(), "watchdog-kill");
        assert_eq!(HarnessState::PreHooks.display_label(), "pre-hooks");
        assert_eq!(HarnessState::PostHooks.display_label(), "post-hooks");
        assert_eq!(HarnessState::Retrying.display_label(), "retrying");
    }

    #[test]
    fn test_display_status_no_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("blacksmith.status");
        let result = display_status(&path, None, 1).unwrap();
        assert!(!result);
    }

    #[test]
    fn test_display_status_with_data() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("blacksmith.status");
        let sf = StatusFile::new(path.clone());

        // Use current PID so the "alive" check passes
        let data = StatusData {
            pid: std::process::id(),
            state: HarnessState::SessionRunning,
            iteration: 14,
            max_iterations: 25,
            global_iteration: 362,
            output_file: "claude-iteration-362.jsonl".to_string(),
            output_bytes: 49331,
            session_start: Some(Utc::now()),
            last_update: Utc::now(),
            last_completed_iteration: Some(361),
            last_committed: true,
            consecutive_rate_limits: 0,
        };

        sf.write(&data).unwrap();
        let result = display_status(&path, None, 1).unwrap();
        assert!(result);
    }

    #[test]
    fn test_deserialize_error_display() {
        let err = StatusError::Deserialize {
            source: serde_json::from_str::<StatusData>("bad").unwrap_err(),
        };
        let msg = err.to_string();
        assert!(msg.contains("failed to deserialize status"));
    }

    #[test]
    fn test_read_error_display() {
        let err = StatusError::Read {
            path: PathBuf::from("/tmp/test"),
            source: std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied"),
        };
        let msg = err.to_string();
        assert!(msg.contains("failed to read status file"));
        assert!(msg.contains("denied"));
    }

    // ── format_duration_f64 tests ──

    #[test]
    fn test_format_duration_f64_seconds() {
        assert_eq!(format_duration_f64(0.0), "0s");
        assert_eq!(format_duration_f64(45.0), "45s");
        assert_eq!(format_duration_f64(59.0), "59s");
    }

    #[test]
    fn test_format_duration_f64_minutes() {
        assert_eq!(format_duration_f64(60.0), "1m");
        assert_eq!(format_duration_f64(300.0), "5m");
        assert_eq!(format_duration_f64(90.0), "1.5m");
    }

    #[test]
    fn test_format_duration_f64_hours() {
        assert_eq!(format_duration_f64(3600.0), "1h");
        assert_eq!(format_duration_f64(5400.0), "1h 30m");
    }

    #[test]
    fn test_display_status_with_db() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("blacksmith.status");
        let db_path = dir.path().join("test.db");
        let sf = StatusFile::new(path.clone());

        // Create a DB with some bead_metrics
        let conn = crate::db::open_or_create(&db_path).unwrap();
        for i in 0..3 {
            crate::db::upsert_bead_metrics(
                &conn,
                &format!("bead-{}", i),
                1,
                300.0,
                50,
                None,
                None,
                Some("2026-01-01T00:00:00Z"),
            )
            .unwrap();
        }
        drop(conn);

        let data = StatusData {
            pid: std::process::id(),
            state: HarnessState::SessionRunning,
            iteration: 3,
            max_iterations: 25,
            global_iteration: 103,
            output_file: "claude-iteration-103.jsonl".to_string(),
            output_bytes: 49331,
            session_start: Some(Utc::now()),
            last_update: Utc::now(),
            last_completed_iteration: Some(102),
            last_committed: true,
            consecutive_rate_limits: 0,
        };

        sf.write(&data).unwrap();
        let result = display_status(&path, Some(&db_path), 2).unwrap();
        assert!(result);
    }

    #[test]
    fn test_display_status_multi_worker_label() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("blacksmith.status");
        let sf = StatusFile::new(path.clone());

        let data = StatusData {
            pid: std::process::id(),
            state: HarnessState::SessionRunning,
            iteration: 0,
            max_iterations: 10,
            global_iteration: 0,
            output_file: String::new(),
            output_bytes: 0,
            session_start: None,
            last_update: Utc::now(),
            last_completed_iteration: None,
            last_committed: false,
            consecutive_rate_limits: 0,
        };

        sf.write(&data).unwrap();
        // With workers > 1, label should indicate worker count
        let result = display_status(&path, None, 3).unwrap();
        assert!(result);
    }
}
