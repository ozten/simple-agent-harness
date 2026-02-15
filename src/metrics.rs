/// JSONL event log: appends one JSON line per session completion.
///
/// Configured via `[output] event_log = "harness-events.jsonl"`.
/// Each line is a self-contained JSON object with session metadata.
use chrono::Utc;
use serde::Serialize;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

/// A single event written to the JSONL event log.
#[derive(Debug, Serialize)]
pub struct SessionEvent {
    pub ts: String,
    pub event: String,
    pub iteration: u32,
    pub global: u64,
    pub output_bytes: u64,
    pub exit_code: Option<i32>,
    pub duration_secs: u64,
    pub committed: bool,
    pub retries: u32,
    pub rate_limited: bool,
}

/// Append-only JSONL event log writer.
pub struct EventLog {
    path: PathBuf,
}

impl EventLog {
    /// Create a new EventLog writer for the given path.
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    /// Append a session event as a single JSON line.
    pub fn append(&self, event: &SessionEvent) -> Result<(), EventLogError> {
        let json =
            serde_json::to_string(event).map_err(|e| EventLogError::Serialize { source: e })?;

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|e| EventLogError::Write {
                path: self.path.clone(),
                source: e,
            })?;

        writeln!(file, "{}", json).map_err(|e| EventLogError::Write {
            path: self.path.clone(),
            source: e,
        })?;

        Ok(())
    }

    /// Path to the event log file.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl SessionEvent {
    /// Create a session_complete event with the given fields.
    #[allow(clippy::too_many_arguments)]
    pub fn session_complete(
        iteration: u32,
        global: u64,
        output_bytes: u64,
        exit_code: Option<i32>,
        duration_secs: u64,
        committed: bool,
        retries: u32,
        rate_limited: bool,
    ) -> Self {
        Self {
            ts: Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            event: "session_complete".to_string(),
            iteration,
            global,
            output_bytes,
            exit_code,
            duration_secs,
            committed,
            retries,
            rate_limited,
        }
    }
}

/// Errors from event log operations.
#[derive(Debug)]
pub enum EventLogError {
    Serialize {
        source: serde_json::Error,
    },
    Write {
        path: PathBuf,
        source: std::io::Error,
    },
}

impl std::fmt::Display for EventLogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventLogError::Serialize { source } => {
                write!(f, "failed to serialize event: {source}")
            }
            EventLogError::Write { path, source } => {
                write!(f, "failed to write event log {}: {source}", path.display())
            }
        }
    }
}

impl std::error::Error for EventLogError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            EventLogError::Serialize { source } => Some(source),
            EventLogError::Write { source, .. } => Some(source),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_session_event_serializes_to_json() {
        let event = SessionEvent::session_complete(3, 103, 128456, Some(0), 1800, true, 0, false);
        let json = serde_json::to_string(&event).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["event"], "session_complete");
        assert_eq!(parsed["iteration"], 3);
        assert_eq!(parsed["global"], 103);
        assert_eq!(parsed["output_bytes"], 128456);
        assert_eq!(parsed["exit_code"], 0);
        assert_eq!(parsed["duration_secs"], 1800);
        assert_eq!(parsed["committed"], true);
        assert_eq!(parsed["retries"], 0);
        assert_eq!(parsed["rate_limited"], false);
        assert!(parsed["ts"].is_string());
    }

    #[test]
    fn test_session_event_null_exit_code() {
        let event = SessionEvent::session_complete(0, 0, 0, None, 60, false, 1, true);
        let json = serde_json::to_string(&event).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(parsed["exit_code"].is_null());
        assert_eq!(parsed["rate_limited"], true);
        assert_eq!(parsed["retries"], 1);
    }

    #[test]
    fn test_event_log_append_creates_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        let log = EventLog::new(path.clone());

        let event = SessionEvent::session_complete(0, 100, 5000, Some(0), 300, true, 0, false);
        log.append(&event).unwrap();

        assert!(path.exists());
        let contents = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 1);

        let parsed: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(parsed["event"], "session_complete");
        assert_eq!(parsed["global"], 100);
    }

    #[test]
    fn test_event_log_append_multiple_events() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        let log = EventLog::new(path.clone());

        for i in 0..5 {
            let event = SessionEvent::session_complete(
                i,
                100 + i as u64,
                1000 * i as u64,
                Some(0),
                60,
                true,
                0,
                false,
            );
            log.append(&event).unwrap();
        }

        let contents = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 5);

        // Each line is valid JSON
        for (i, line) in lines.iter().enumerate() {
            let parsed: serde_json::Value = serde_json::from_str(line).unwrap();
            assert_eq!(parsed["iteration"], i as u64);
            assert_eq!(parsed["global"], 100 + i as u64);
        }
    }

    #[test]
    fn test_event_log_append_to_nonexistent_dir_fails() {
        let log = EventLog::new(PathBuf::from("/nonexistent/dir/events.jsonl"));
        let event = SessionEvent::session_complete(0, 0, 0, Some(0), 0, false, 0, false);
        let result = log.append(&event);
        assert!(result.is_err());
    }

    #[test]
    fn test_event_log_error_display() {
        let err = EventLogError::Write {
            path: PathBuf::from("/tmp/test.jsonl"),
            source: std::io::Error::new(std::io::ErrorKind::PermissionDenied, "no write"),
        };
        let msg = err.to_string();
        assert!(msg.contains("failed to write event log"));
        assert!(msg.contains("no write"));
    }

    #[test]
    fn test_event_log_path() {
        let log = EventLog::new(PathBuf::from("/tmp/events.jsonl"));
        assert_eq!(log.path(), Path::new("/tmp/events.jsonl"));
    }
}
