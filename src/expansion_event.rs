//! Expansion event tracking for affected-set mismatches.
//!
//! Records when a coding agent discovers it needs modules not in its original
//! metadata. Each expansion is a data point: "the derivation engine predicted
//! these modules, but the agent actually needed these." Frequent expansions
//! around the same module indicate its boundaries don't contain changes well.

use rusqlite::{params, Connection, Result};
use serde::{Deserialize, Serialize};

/// A single expansion event: predicted vs actual modules for a task.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExpansionEvent {
    pub task_id: String,
    pub predicted_modules: Vec<String>,
    pub actual_modules: Vec<String>,
    pub expansion_reason: String,
    pub timestamp: String,
}

/// Create the expansion_events table if it doesn't exist.
pub fn create_table(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS expansion_events (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id             TEXT NOT NULL,
            predicted_modules   TEXT NOT NULL,
            actual_modules      TEXT NOT NULL,
            expansion_reason    TEXT NOT NULL,
            timestamp           TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
        );

        CREATE INDEX IF NOT EXISTS idx_expansion_events_task
            ON expansion_events(task_id);
        CREATE INDEX IF NOT EXISTS idx_expansion_events_timestamp
            ON expansion_events(timestamp);",
    )
}

/// Get the most recent N expansion events across all tasks.
pub fn get_recent(conn: &Connection, limit: u32) -> Result<Vec<ExpansionEvent>> {
    let mut stmt = conn.prepare(
        "SELECT task_id, predicted_modules, actual_modules, expansion_reason, timestamp
         FROM expansion_events
         ORDER BY id DESC
         LIMIT ?1",
    )?;

    let rows = stmt.query_map(params![limit], |row| {
        let task_id: String = row.get(0)?;
        let predicted_json: String = row.get(1)?;
        let actual_json: String = row.get(2)?;
        let expansion_reason: String = row.get(3)?;
        let timestamp: String = row.get(4)?;
        Ok((
            task_id,
            predicted_json,
            actual_json,
            expansion_reason,
            timestamp,
        ))
    })?;

    let mut events = Vec::new();
    for row in rows {
        let (task_id, predicted_json, actual_json, expansion_reason, timestamp) = row?;
        let predicted_modules: Vec<String> =
            serde_json::from_str(&predicted_json).unwrap_or_default();
        let actual_modules: Vec<String> = serde_json::from_str(&actual_json).unwrap_or_default();
        events.push(ExpansionEvent {
            task_id,
            predicted_modules,
            actual_modules,
            expansion_reason,
            timestamp,
        });
    }
    Ok(events)
}

/// Record an expansion event (test-only; no production callers yet).
#[cfg(test)]
pub fn record(conn: &Connection, event: &ExpansionEvent) -> Result<()> {
    let predicted_json =
        serde_json::to_string(&event.predicted_modules).unwrap_or_else(|_| "[]".to_string());
    let actual_json =
        serde_json::to_string(&event.actual_modules).unwrap_or_else(|_| "[]".to_string());

    conn.execute(
        "INSERT INTO expansion_events (task_id, predicted_modules, actual_modules, expansion_reason, timestamp)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            event.task_id,
            predicted_json,
            actual_json,
            event.expansion_reason,
            event.timestamp,
        ],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        create_table(&conn).unwrap();
        conn
    }

    fn sample_event() -> ExpansionEvent {
        ExpansionEvent {
            task_id: "task-47".to_string(),
            predicted_modules: vec!["auth".to_string()],
            actual_modules: vec![
                "auth".to_string(),
                "models".to_string(),
                "middleware".to_string(),
            ],
            expansion_reason: "Auth changes required updating shared model types".to_string(),
            timestamp: "2025-03-15T10:30:00Z".to_string(),
        }
    }

    /// Get all expansion events for a specific task (test helper).
    fn get_by_task(conn: &Connection, task_id: &str) -> Result<Vec<ExpansionEvent>> {
        let mut stmt = conn.prepare(
            "SELECT task_id, predicted_modules, actual_modules, expansion_reason, timestamp
             FROM expansion_events
             WHERE task_id = ?1
             ORDER BY id ASC",
        )?;

        let rows = stmt.query_map(params![task_id], |row| {
            let task_id: String = row.get(0)?;
            let predicted_json: String = row.get(1)?;
            let actual_json: String = row.get(2)?;
            let expansion_reason: String = row.get(3)?;
            let timestamp: String = row.get(4)?;
            Ok((
                task_id,
                predicted_json,
                actual_json,
                expansion_reason,
                timestamp,
            ))
        })?;

        let mut events = Vec::new();
        for row in rows {
            let (task_id, predicted_json, actual_json, expansion_reason, timestamp) = row?;
            let predicted_modules: Vec<String> =
                serde_json::from_str(&predicted_json).unwrap_or_default();
            let actual_modules: Vec<String> =
                serde_json::from_str(&actual_json).unwrap_or_default();
            events.push(ExpansionEvent {
                task_id,
                predicted_modules,
                actual_modules,
                expansion_reason,
                timestamp,
            });
        }
        Ok(events)
    }

    /// Count expansion events involving a specific module (test helper).
    fn count_by_module(conn: &Connection, module: &str) -> Result<u32> {
        let pattern = format!("%\"{module}\"%");
        let count: u32 = conn.query_row(
            "SELECT COUNT(*) FROM expansion_events WHERE actual_modules LIKE ?1",
            params![pattern],
            |row| row.get(0),
        )?;
        Ok(count)
    }

    /// Count total expansion events within a recent window of tasks (test helper).
    fn count_recent_expansions(conn: &Connection, task_window: u32) -> Result<u32> {
        let count: u32 = conn.query_row(
            "SELECT COUNT(*) FROM expansion_events
             WHERE task_id IN (
                 SELECT DISTINCT task_id FROM expansion_events
                 ORDER BY rowid DESC
                 LIMIT ?1
             )",
            params![task_window],
            |row| row.get(0),
        )?;
        Ok(count)
    }

    #[test]
    fn record_and_retrieve_by_task() {
        let conn = setup_db();
        let event = sample_event();
        record(&conn, &event).unwrap();

        let events = get_by_task(&conn, "task-47").unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].task_id, "task-47");
        assert_eq!(events[0].predicted_modules, vec!["auth"]);
        assert_eq!(
            events[0].actual_modules,
            vec!["auth", "models", "middleware"]
        );
        assert_eq!(
            events[0].expansion_reason,
            "Auth changes required updating shared model types"
        );
        assert_eq!(events[0].timestamp, "2025-03-15T10:30:00Z");
    }

    #[test]
    fn no_events_returns_empty() {
        let conn = setup_db();
        let events = get_by_task(&conn, "nonexistent").unwrap();
        assert!(events.is_empty());
    }

    #[test]
    fn multiple_events_same_task() {
        let conn = setup_db();
        let event1 = sample_event();
        let event2 = ExpansionEvent {
            task_id: "task-47".to_string(),
            predicted_modules: vec!["auth".to_string()],
            actual_modules: vec!["auth".to_string(), "config".to_string()],
            expansion_reason: "Also needed config changes".to_string(),
            timestamp: "2025-03-15T11:00:00Z".to_string(),
        };
        record(&conn, &event1).unwrap();
        record(&conn, &event2).unwrap();

        let events = get_by_task(&conn, "task-47").unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(
            events[0].expansion_reason,
            "Auth changes required updating shared model types"
        );
        assert_eq!(events[1].expansion_reason, "Also needed config changes");
    }

    #[test]
    fn get_recent_returns_most_recent() {
        let conn = setup_db();
        for i in 0..5 {
            let event = ExpansionEvent {
                task_id: format!("task-{i}"),
                predicted_modules: vec!["mod_a".to_string()],
                actual_modules: vec!["mod_a".to_string(), "mod_b".to_string()],
                expansion_reason: format!("reason {i}"),
                timestamp: format!("2025-03-15T10:{i:02}:00Z"),
            };
            record(&conn, &event).unwrap();
        }

        let recent = get_recent(&conn, 3).unwrap();
        assert_eq!(recent.len(), 3);
        // Most recent first (DESC order)
        assert_eq!(recent[0].task_id, "task-4");
        assert_eq!(recent[1].task_id, "task-3");
        assert_eq!(recent[2].task_id, "task-2");
    }

    #[test]
    fn get_recent_with_fewer_than_limit() {
        let conn = setup_db();
        let event = sample_event();
        record(&conn, &event).unwrap();

        let recent = get_recent(&conn, 10).unwrap();
        assert_eq!(recent.len(), 1);
    }

    #[test]
    fn count_by_module_counts_correctly() {
        let conn = setup_db();
        // Event 1: actual = [auth, models, middleware]
        record(&conn, &sample_event()).unwrap();
        // Event 2: actual = [auth, config]
        let event2 = ExpansionEvent {
            task_id: "task-48".to_string(),
            predicted_modules: vec!["auth".to_string()],
            actual_modules: vec!["auth".to_string(), "config".to_string()],
            expansion_reason: "another reason".to_string(),
            timestamp: "2025-03-15T11:00:00Z".to_string(),
        };
        record(&conn, &event2).unwrap();

        assert_eq!(count_by_module(&conn, "auth").unwrap(), 2);
        assert_eq!(count_by_module(&conn, "models").unwrap(), 1);
        assert_eq!(count_by_module(&conn, "config").unwrap(), 1);
        assert_eq!(count_by_module(&conn, "nonexistent").unwrap(), 0);
    }

    #[test]
    fn count_recent_expansions_within_window() {
        let conn = setup_db();
        // 3 events across 2 tasks
        record(&conn, &sample_event()).unwrap();
        let event2 = ExpansionEvent {
            task_id: "task-47".to_string(),
            predicted_modules: vec!["x".to_string()],
            actual_modules: vec!["x".to_string(), "y".to_string()],
            expansion_reason: "second".to_string(),
            timestamp: "2025-03-15T11:00:00Z".to_string(),
        };
        record(&conn, &event2).unwrap();
        let event3 = ExpansionEvent {
            task_id: "task-48".to_string(),
            predicted_modules: vec!["a".to_string()],
            actual_modules: vec!["a".to_string(), "b".to_string()],
            expansion_reason: "third".to_string(),
            timestamp: "2025-03-15T12:00:00Z".to_string(),
        };
        record(&conn, &event3).unwrap();

        // Window of 2 tasks: task-47 (2 events) + task-48 (1 event) = 3
        assert_eq!(count_recent_expansions(&conn, 2).unwrap(), 3);
        // Window of 1 task: only the most recent distinct task
        assert_eq!(count_recent_expansions(&conn, 1).unwrap(), 1);
    }

    #[test]
    fn empty_modules_handled() {
        let conn = setup_db();
        let event = ExpansionEvent {
            task_id: "task-empty".to_string(),
            predicted_modules: vec![],
            actual_modules: vec!["new_module".to_string()],
            expansion_reason: "Entirely new module needed".to_string(),
            timestamp: "2025-03-15T10:00:00Z".to_string(),
        };
        record(&conn, &event).unwrap();

        let events = get_by_task(&conn, "task-empty").unwrap();
        assert_eq!(events.len(), 1);
        assert!(events[0].predicted_modules.is_empty());
        assert_eq!(events[0].actual_modules, vec!["new_module"]);
    }

    #[test]
    fn events_across_multiple_tasks() {
        let conn = setup_db();
        for i in 0..3 {
            let event = ExpansionEvent {
                task_id: format!("task-{i}"),
                predicted_modules: vec!["mod_a".to_string()],
                actual_modules: vec!["mod_a".to_string(), format!("extra_{i}")],
                expansion_reason: format!("reason {i}"),
                timestamp: format!("2025-03-{:02}T10:00:00Z", 15 + i),
            };
            record(&conn, &event).unwrap();
        }

        // Each task has exactly 1 event
        for i in 0..3 {
            let events = get_by_task(&conn, &format!("task-{i}")).unwrap();
            assert_eq!(events.len(), 1);
        }

        // get_recent returns all 3
        let recent = get_recent(&conn, 10).unwrap();
        assert_eq!(recent.len(), 3);
    }
}
