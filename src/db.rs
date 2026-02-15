use rusqlite::{Connection, Result};
use std::path::Path;

/// Opens (or creates) the blacksmith SQLite database at the given path.
///
/// Creates the improvements table and indexes if they don't already exist.
/// Returns an open connection ready for use.
pub fn open_or_create(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path)?;

    // Enable WAL mode for better concurrent read performance
    conn.execute_batch("PRAGMA journal_mode=WAL;")?;

    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS improvements (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            ref        TEXT UNIQUE,
            created    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
            resolved   TEXT,
            category   TEXT NOT NULL,
            status     TEXT NOT NULL DEFAULT 'open',
            title      TEXT NOT NULL,
            body       TEXT,
            context    TEXT,
            tags       TEXT,
            meta       TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_improvements_status ON improvements(status);
        CREATE INDEX IF NOT EXISTS idx_improvements_category ON improvements(category);

        CREATE TABLE IF NOT EXISTS events (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            ts        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
            session   INTEGER NOT NULL,
            kind      TEXT NOT NULL,
            value     TEXT,
            tags      TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_events_session ON events(session);
        CREATE INDEX IF NOT EXISTS idx_events_kind ON events(kind);
        CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts);

        CREATE TABLE IF NOT EXISTS observations (
            session   INTEGER PRIMARY KEY,
            ts        TEXT NOT NULL,
            duration  INTEGER,
            outcome   TEXT,
            data      TEXT NOT NULL
        );

        -- Coordinator tables for multi-agent state

        CREATE TABLE IF NOT EXISTS worker_assignments (
            id             INTEGER PRIMARY KEY,
            worker_id      INTEGER NOT NULL,
            bead_id        TEXT NOT NULL,
            worktree_path  TEXT NOT NULL,
            status         TEXT NOT NULL,
            affected_globs TEXT,
            started_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
            completed_at   TEXT,
            failure_notes  TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_worker_assignments_status ON worker_assignments(status);
        CREATE INDEX IF NOT EXISTS idx_worker_assignments_bead_id ON worker_assignments(bead_id);

        CREATE TABLE IF NOT EXISTS task_file_changes (
            assignment_id  INTEGER NOT NULL REFERENCES worker_assignments(id),
            file_path      TEXT NOT NULL,
            change_type    TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_task_file_changes_assignment ON task_file_changes(assignment_id);

        CREATE TABLE IF NOT EXISTS integration_log (
            id                        INTEGER PRIMARY KEY,
            assignment_id             INTEGER NOT NULL REFERENCES worker_assignments(id),
            merged_at                 TEXT NOT NULL,
            merge_commit              TEXT NOT NULL,
            manifest_entries_applied  TEXT,
            cross_task_imports        TEXT,
            reconciliation_run        BOOLEAN DEFAULT 0
        );

        CREATE INDEX IF NOT EXISTS idx_integration_log_assignment ON integration_log(assignment_id);

        CREATE TABLE IF NOT EXISTS integration_iterations (
            id              INTEGER PRIMARY KEY,
            assignment_id   INTEGER NOT NULL REFERENCES worker_assignments(id),
            bead_id         TEXT NOT NULL,
            iteration_count INTEGER NOT NULL,
            modules         TEXT,
            recorded_at     TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_integration_iterations_bead ON integration_iterations(bead_id);

        CREATE TABLE IF NOT EXISTS bead_metrics (
            bead_id               TEXT PRIMARY KEY,
            sessions              INTEGER NOT NULL DEFAULT 0,
            wall_time_secs        REAL NOT NULL DEFAULT 0,
            total_turns           INTEGER NOT NULL DEFAULT 0,
            total_output_tokens   INTEGER DEFAULT 0,
            integration_time_secs REAL DEFAULT 0,
            completed_at          TEXT
        );",
    )?;

    crate::intent::create_table(&conn)?;
    crate::expansion_event::create_table(&conn)?;

    Ok(conn)
}

/// Assigns the next auto-increment ref (R1, R2, ...) for a new improvement.
///
/// Reads the current max ref number from the table and returns the next one.
pub fn next_ref(conn: &Connection) -> Result<String> {
    let max_num: Option<i64> = conn.query_row(
        "SELECT MAX(CAST(SUBSTR(ref, 2) AS INTEGER)) FROM improvements WHERE ref LIKE 'R%'",
        [],
        |row| row.get(0),
    )?;
    let next = max_num.unwrap_or(0) + 1;
    Ok(format!("R{next}"))
}

/// Insert a new improvement record, auto-assigning the next ref.
/// Returns the assigned ref (e.g. "R1").
pub fn insert_improvement(
    conn: &Connection,
    category: &str,
    title: &str,
    body: Option<&str>,
    context: Option<&str>,
    tags: Option<&str>,
) -> Result<String> {
    let ref_id = next_ref(conn)?;
    conn.execute(
        "INSERT INTO improvements (ref, category, title, body, context, tags) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        rusqlite::params![ref_id, category, title, body, context, tags],
    )?;
    Ok(ref_id)
}

/// A row from the improvements table.
#[derive(Debug)]
pub struct Improvement {
    pub ref_id: String,
    pub created: String,
    pub category: String,
    pub status: String,
    pub title: String,
    pub body: Option<String>,
    pub context: Option<String>,
    pub tags: Option<String>,
}

/// List improvements with optional status and category filters.
pub fn list_improvements(
    conn: &Connection,
    status: Option<&str>,
    category: Option<&str>,
) -> Result<Vec<Improvement>> {
    let mut sql =
        "SELECT ref, created, category, status, title, body, context, tags FROM improvements"
            .to_string();
    let mut conditions = Vec::new();

    if status.is_some() {
        conditions.push("status = ?1");
    }
    if category.is_some() {
        conditions.push(if status.is_some() {
            "category = ?2"
        } else {
            "category = ?1"
        });
    }

    if !conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conditions.join(" AND "));
    }
    sql.push_str(" ORDER BY id ASC");

    let mut stmt = conn.prepare(&sql)?;

    let rows = match (status, category) {
        (Some(s), Some(c)) => {
            let iter = stmt.query_map(rusqlite::params![s, c], map_improvement)?;
            iter.collect::<Result<Vec<_>>>()?
        }
        (Some(s), None) => {
            let iter = stmt.query_map(rusqlite::params![s], map_improvement)?;
            iter.collect::<Result<Vec<_>>>()?
        }
        (None, Some(c)) => {
            let iter = stmt.query_map(rusqlite::params![c], map_improvement)?;
            iter.collect::<Result<Vec<_>>>()?
        }
        (None, None) => {
            let iter = stmt.query_map([], map_improvement)?;
            iter.collect::<Result<Vec<_>>>()?
        }
    };

    Ok(rows)
}

/// Count total improvements (all statuses).
pub fn count_improvements(conn: &Connection) -> Result<i64> {
    conn.query_row("SELECT COUNT(*) FROM improvements", [], |row| row.get(0))
}

/// Fetch a single improvement by its ref (e.g. "R1").
/// Returns None if no matching ref exists.
pub fn get_improvement(conn: &Connection, ref_id: &str) -> Result<Option<Improvement>> {
    let mut stmt = conn.prepare(
        "SELECT ref, created, category, status, title, body, context, tags FROM improvements WHERE ref = ?1",
    )?;
    let mut rows = stmt.query_map(rusqlite::params![ref_id], map_improvement)?;
    match rows.next() {
        Some(row) => Ok(Some(row?)),
        None => Ok(None),
    }
}

/// Fetch the meta JSON field for an improvement by ref.
pub fn get_improvement_meta(conn: &Connection, ref_id: &str) -> Result<Option<String>> {
    conn.query_row(
        "SELECT meta FROM improvements WHERE ref = ?1",
        rusqlite::params![ref_id],
        |row| row.get(0),
    )
}

/// Update an improvement's fields by ref. Only non-None values are updated.
pub fn update_improvement(
    conn: &Connection,
    ref_id: &str,
    status: Option<&str>,
    body: Option<&str>,
    context: Option<&str>,
    meta: Option<&str>,
) -> Result<bool> {
    let mut sets = Vec::new();
    let mut params: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
    let mut idx = 1;

    if let Some(s) = status {
        sets.push(format!("status = ?{idx}"));
        params.push(Box::new(s.to_string()));
        idx += 1;
    }
    if let Some(b) = body {
        sets.push(format!("body = ?{idx}"));
        params.push(Box::new(b.to_string()));
        idx += 1;
    }
    if let Some(c) = context {
        sets.push(format!("context = ?{idx}"));
        params.push(Box::new(c.to_string()));
        idx += 1;
    }
    if let Some(m) = meta {
        sets.push(format!("meta = ?{idx}"));
        params.push(Box::new(m.to_string()));
        idx += 1;
    }

    if sets.is_empty() {
        return Ok(false);
    }

    // Add resolved timestamp when moving to a terminal status
    if let Some(s) = status {
        if s == "promoted" || s == "dismissed" {
            sets.push("resolved = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')".to_string());
        }
    }

    let sql = format!(
        "UPDATE improvements SET {} WHERE ref = ?{idx}",
        sets.join(", ")
    );
    params.push(Box::new(ref_id.to_string()));

    let param_refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|p| p.as_ref()).collect();
    let rows = conn.execute(&sql, param_refs.as_slice())?;
    Ok(rows > 0)
}

/// Full-text search across title, body, and context fields.
/// Returns improvements where the query appears in any of these fields (case-insensitive).
pub fn search_improvements(conn: &Connection, query: &str) -> Result<Vec<Improvement>> {
    let pattern = format!("%{query}%");
    let mut stmt = conn.prepare(
        "SELECT ref, created, category, status, title, body, context, tags \
         FROM improvements \
         WHERE title LIKE ?1 OR body LIKE ?1 OR context LIKE ?1 \
         ORDER BY id ASC",
    )?;
    let rows = stmt
        .query_map(rusqlite::params![pattern], map_improvement)?
        .collect::<Result<Vec<_>>>()?;
    Ok(rows)
}

// ── Events ──────────────────────────────────────────────────────────────

/// A row from the events table.
#[derive(Debug)]
pub struct Event {
    pub id: i64,
    pub ts: String,
    pub session: i64,
    pub kind: String,
    pub value: Option<String>,
    pub tags: Option<String>,
}

/// Insert a single event into the events table.
/// Returns the rowid of the inserted event.
#[allow(dead_code)]
pub fn insert_event(
    conn: &Connection,
    session: i64,
    kind: &str,
    value: Option<&str>,
    tags: Option<&str>,
) -> Result<i64> {
    conn.execute(
        "INSERT INTO events (session, kind, value, tags) VALUES (?1, ?2, ?3, ?4)",
        rusqlite::params![session, kind, value, tags],
    )?;
    Ok(conn.last_insert_rowid())
}

/// Insert a single event with an explicit timestamp.
/// Returns the rowid of the inserted event.
pub fn insert_event_with_ts(
    conn: &Connection,
    ts: &str,
    session: i64,
    kind: &str,
    value: Option<&str>,
    tags: Option<&str>,
) -> Result<i64> {
    conn.execute(
        "INSERT INTO events (ts, session, kind, value, tags) VALUES (?1, ?2, ?3, ?4, ?5)",
        rusqlite::params![ts, session, kind, value, tags],
    )?;
    Ok(conn.last_insert_rowid())
}

/// Query events for a specific session, ordered by id.
pub fn events_by_session(conn: &Connection, session: i64) -> Result<Vec<Event>> {
    let mut stmt = conn.prepare(
        "SELECT id, ts, session, kind, value, tags FROM events WHERE session = ?1 ORDER BY id ASC",
    )?;
    let rows = stmt
        .query_map(rusqlite::params![session], map_event)?
        .collect::<Result<Vec<_>>>()?;
    Ok(rows)
}

/// Delete all events for a specific session. Returns the number of deleted rows.
pub fn delete_events_by_session(conn: &Connection, session: i64) -> Result<usize> {
    let count = conn.execute(
        "DELETE FROM events WHERE session = ?1",
        rusqlite::params![session],
    )?;
    Ok(count)
}

/// Query events by kind, ordered by id.
pub fn events_by_kind(conn: &Connection, kind: &str) -> Result<Vec<Event>> {
    let mut stmt = conn.prepare(
        "SELECT id, ts, session, kind, value, tags FROM events WHERE kind = ?1 ORDER BY id ASC",
    )?;
    let rows = stmt
        .query_map(rusqlite::params![kind], map_event)?
        .collect::<Result<Vec<_>>>()?;
    Ok(rows)
}

fn map_event(row: &rusqlite::Row) -> Result<Event> {
    Ok(Event {
        id: row.get(0)?,
        ts: row.get(1)?,
        session: row.get(2)?,
        kind: row.get(3)?,
        value: row.get(4)?,
        tags: row.get(5)?,
    })
}

// ── Observations ────────────────────────────────────────────────────────

/// A row from the observations table (per-session materialized summary).
#[derive(Debug)]
pub struct Observation {
    pub session: i64,
    pub ts: String,
    pub duration: Option<i64>,
    pub outcome: Option<String>,
    pub data: String,
}

/// Insert or replace an observation for a session.
pub fn upsert_observation(
    conn: &Connection,
    session: i64,
    ts: &str,
    duration: Option<i64>,
    outcome: Option<&str>,
    data: &str,
) -> Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO observations (session, ts, duration, outcome, data) VALUES (?1, ?2, ?3, ?4, ?5)",
        rusqlite::params![session, ts, duration, outcome, data],
    )?;
    Ok(())
}

/// Get the observation for a specific session.
#[allow(dead_code)]
pub fn get_observation(conn: &Connection, session: i64) -> Result<Option<Observation>> {
    let mut stmt = conn.prepare(
        "SELECT session, ts, duration, outcome, data FROM observations WHERE session = ?1",
    )?;
    let mut rows = stmt.query_map(rusqlite::params![session], map_observation)?;
    match rows.next() {
        Some(row) => Ok(Some(row?)),
        None => Ok(None),
    }
}

/// List recent observations, ordered by session descending, limited to `limit` rows.
pub fn recent_observations(conn: &Connection, limit: i64) -> Result<Vec<Observation>> {
    let mut stmt = conn.prepare(
        "SELECT session, ts, duration, outcome, data FROM observations ORDER BY session DESC LIMIT ?1",
    )?;
    let rows = stmt
        .query_map(rusqlite::params![limit], map_observation)?
        .collect::<Result<Vec<_>>>()?;
    Ok(rows)
}

/// Rebuild all observations from events.
///
/// Drops all rows from the observations table, then for each distinct session
/// in the events table, aggregates event (kind, value) pairs into a JSON object
/// and upserts an observation. Returns the number of sessions rebuilt.
pub fn rebuild_observations(conn: &Connection) -> Result<u64> {
    // Delete all existing observations
    conn.execute("DELETE FROM observations", [])?;

    // Get distinct sessions from events
    let mut sessions_stmt =
        conn.prepare("SELECT DISTINCT session FROM events ORDER BY session ASC")?;
    let sessions: Vec<i64> = sessions_stmt
        .query_map([], |row| row.get(0))?
        .collect::<Result<Vec<_>>>()?;

    let mut count = 0u64;
    for session in sessions {
        // Get all events for this session
        let events = events_by_session(conn, session)?;
        if events.is_empty() {
            continue;
        }

        // Use the timestamp from the first event
        let ts = &events[0].ts;

        // Build observation data JSON from events
        let mut map = serde_json::Map::new();
        for event in &events {
            if let Some(ref value) = event.value {
                // Try to parse as number first for cleaner JSON
                if let Ok(n) = value.parse::<i64>() {
                    map.insert(event.kind.clone(), serde_json::Value::Number(n.into()));
                } else if let Ok(n) = value.parse::<f64>() {
                    map.insert(
                        event.kind.clone(),
                        serde_json::Number::from_f64(n)
                            .map(serde_json::Value::Number)
                            .unwrap_or(serde_json::Value::String(value.clone())),
                    );
                } else if value == "true" {
                    map.insert(event.kind.clone(), serde_json::Value::Bool(true));
                } else if value == "false" {
                    map.insert(event.kind.clone(), serde_json::Value::Bool(false));
                } else {
                    // Try parsing as JSON (for arrays), fall back to string
                    match serde_json::from_str::<serde_json::Value>(value) {
                        Ok(v) if v.is_array() => {
                            map.insert(event.kind.clone(), v);
                        }
                        _ => {
                            map.insert(
                                event.kind.clone(),
                                serde_json::Value::String(value.clone()),
                            );
                        }
                    }
                }
            }
        }

        let data = serde_json::Value::Object(map).to_string();

        // Extract duration from session.duration_ms event if present
        let duration_secs = events
            .iter()
            .find(|e| e.kind == "session.duration_ms")
            .and_then(|e| e.value.as_ref())
            .and_then(|v| v.parse::<i64>().ok())
            .map(|ms| ms / 1000);

        upsert_observation(conn, session, ts, duration_secs, None, &data)?;
        count += 1;
    }

    Ok(count)
}

/// List all observations, ordered by session ascending.
pub fn all_observations(conn: &Connection) -> Result<Vec<Observation>> {
    let mut stmt = conn.prepare(
        "SELECT session, ts, duration, outcome, data FROM observations ORDER BY session ASC",
    )?;
    let rows = stmt
        .query_map([], map_observation)?
        .collect::<Result<Vec<_>>>()?;
    Ok(rows)
}

/// List all events, optionally filtered by session, ordered by id ascending.
pub fn all_events(conn: &Connection, session: Option<i64>) -> Result<Vec<Event>> {
    match session {
        Some(s) => events_by_session(conn, s),
        None => {
            let mut stmt = conn
                .prepare("SELECT id, ts, session, kind, value, tags FROM events ORDER BY id ASC")?;
            let rows = stmt.query_map([], map_event)?.collect::<Result<Vec<_>>>()?;
            Ok(rows)
        }
    }
}

/// Query event values for a specific kind, with an optional session limit.
/// Returns events ordered by session descending, limited to `last` most recent sessions.
pub fn events_by_kind_last(conn: &Connection, kind: &str, last: Option<i64>) -> Result<Vec<Event>> {
    match last {
        Some(limit) => {
            let mut stmt = conn.prepare(
                "SELECT id, ts, session, kind, value, tags FROM events \
                 WHERE kind = ?1 AND session IN \
                 (SELECT DISTINCT session FROM events ORDER BY session DESC LIMIT ?2) \
                 ORDER BY session ASC",
            )?;
            let rows = stmt
                .query_map(rusqlite::params![kind, limit], map_event)?
                .collect::<Result<Vec<_>>>()?;
            Ok(rows)
        }
        None => events_by_kind(conn, kind),
    }
}

// ── Worker Assignments ──────────────────────────────────────────────

/// A row from the worker_assignments table.
#[derive(Debug)]
#[allow(dead_code)]
pub struct WorkerAssignment {
    pub id: i64,
    pub worker_id: i64,
    pub bead_id: String,
    pub worktree_path: String,
    pub status: String,
    pub affected_globs: Option<String>,
    pub started_at: String,
    pub completed_at: Option<String>,
    pub failure_notes: Option<String>,
}

/// Insert a new worker assignment.
/// Returns the id of the inserted row.
pub fn insert_worker_assignment(
    conn: &Connection,
    worker_id: i64,
    bead_id: &str,
    worktree_path: &str,
    status: &str,
    affected_globs: Option<&str>,
) -> Result<i64> {
    conn.execute(
        "INSERT INTO worker_assignments (worker_id, bead_id, worktree_path, status, affected_globs) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
        rusqlite::params![worker_id, bead_id, worktree_path, status, affected_globs],
    )?;
    Ok(conn.last_insert_rowid())
}

/// Update a worker assignment's status and optionally set completed_at/failure_notes.
pub fn update_worker_assignment_status(
    conn: &Connection,
    id: i64,
    status: &str,
    failure_notes: Option<&str>,
) -> Result<bool> {
    let rows = if status == "completed" || status == "failed" {
        conn.execute(
            "UPDATE worker_assignments SET status = ?1, completed_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now'), \
             failure_notes = ?2 WHERE id = ?3",
            rusqlite::params![status, failure_notes, id],
        )?
    } else {
        conn.execute(
            "UPDATE worker_assignments SET status = ?1, failure_notes = ?2 WHERE id = ?3",
            rusqlite::params![status, failure_notes, id],
        )?
    };
    Ok(rows > 0)
}

/// Update a worker assignment's affected_globs field (for dynamic expansion).
pub fn update_worker_assignment_affected_globs(
    conn: &Connection,
    id: i64,
    affected_globs: &str,
) -> Result<bool> {
    let rows = conn.execute(
        "UPDATE worker_assignments SET affected_globs = ?1 WHERE id = ?2",
        rusqlite::params![affected_globs, id],
    )?;
    Ok(rows > 0)
}

/// Get a worker assignment by id.
pub fn get_worker_assignment(conn: &Connection, id: i64) -> Result<Option<WorkerAssignment>> {
    let mut stmt = conn.prepare(
        "SELECT id, worker_id, bead_id, worktree_path, status, affected_globs, \
         started_at, completed_at, failure_notes FROM worker_assignments WHERE id = ?1",
    )?;
    let mut rows = stmt.query_map(rusqlite::params![id], map_worker_assignment)?;
    match rows.next() {
        Some(row) => Ok(Some(row?)),
        None => Ok(None),
    }
}

/// List worker assignments by status.
#[allow(dead_code)]
pub fn worker_assignments_by_status(
    conn: &Connection,
    status: &str,
) -> Result<Vec<WorkerAssignment>> {
    let mut stmt = conn.prepare(
        "SELECT id, worker_id, bead_id, worktree_path, status, affected_globs, \
         started_at, completed_at, failure_notes FROM worker_assignments WHERE status = ?1 ORDER BY id ASC",
    )?;
    let rows = stmt
        .query_map(rusqlite::params![status], map_worker_assignment)?
        .collect::<Result<Vec<_>>>()?;
    Ok(rows)
}

/// List all active worker assignments (status = 'coding' or 'integrating').
pub fn active_worker_assignments(conn: &Connection) -> Result<Vec<WorkerAssignment>> {
    let mut stmt = conn.prepare(
        "SELECT id, worker_id, bead_id, worktree_path, status, affected_globs, \
         started_at, completed_at, failure_notes FROM worker_assignments \
         WHERE status IN ('coding', 'integrating') ORDER BY worker_id ASC",
    )?;
    let rows = stmt
        .query_map([], map_worker_assignment)?
        .collect::<Result<Vec<_>>>()?;
    Ok(rows)
}

fn map_worker_assignment(row: &rusqlite::Row) -> Result<WorkerAssignment> {
    Ok(WorkerAssignment {
        id: row.get(0)?,
        worker_id: row.get(1)?,
        bead_id: row.get(2)?,
        worktree_path: row.get(3)?,
        status: row.get(4)?,
        affected_globs: row.get(5)?,
        started_at: row.get(6)?,
        completed_at: row.get(7)?,
        failure_notes: row.get(8)?,
    })
}

// ── Task File Changes ──────────────────────────────────────────────

/// A row from the task_file_changes table.
#[derive(Debug)]
#[allow(dead_code)]
pub struct TaskFileChange {
    pub assignment_id: i64,
    pub file_path: String,
    pub change_type: String,
}

/// Insert a file change record for a worker assignment.
#[allow(dead_code)]
pub fn insert_task_file_change(
    conn: &Connection,
    assignment_id: i64,
    file_path: &str,
    change_type: &str,
) -> Result<()> {
    conn.execute(
        "INSERT INTO task_file_changes (assignment_id, file_path, change_type) VALUES (?1, ?2, ?3)",
        rusqlite::params![assignment_id, file_path, change_type],
    )?;
    Ok(())
}

/// Get all file changes for a worker assignment.
#[allow(dead_code)]
pub fn file_changes_by_assignment(
    conn: &Connection,
    assignment_id: i64,
) -> Result<Vec<TaskFileChange>> {
    let mut stmt = conn.prepare(
        "SELECT assignment_id, file_path, change_type FROM task_file_changes WHERE assignment_id = ?1 ORDER BY file_path ASC",
    )?;
    let rows = stmt
        .query_map(rusqlite::params![assignment_id], |row| {
            Ok(TaskFileChange {
                assignment_id: row.get(0)?,
                file_path: row.get(1)?,
                change_type: row.get(2)?,
            })
        })?
        .collect::<Result<Vec<_>>>()?;
    Ok(rows)
}

// ── Integration Log ────────────────────────────────────────────────

/// A row from the integration_log table.
#[derive(Debug)]
#[allow(dead_code)]
pub struct IntegrationLogEntry {
    pub id: i64,
    pub assignment_id: i64,
    pub merged_at: String,
    pub merge_commit: String,
    pub manifest_entries_applied: Option<String>,
    pub cross_task_imports: Option<String>,
    pub reconciliation_run: bool,
}

/// Insert an integration log entry.
/// Returns the id of the inserted row.
pub fn insert_integration_log(
    conn: &Connection,
    assignment_id: i64,
    merged_at: &str,
    merge_commit: &str,
    manifest_entries_applied: Option<&str>,
    cross_task_imports: Option<&str>,
    reconciliation_run: bool,
) -> Result<i64> {
    conn.execute(
        "INSERT INTO integration_log (assignment_id, merged_at, merge_commit, \
         manifest_entries_applied, cross_task_imports, reconciliation_run) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        rusqlite::params![
            assignment_id,
            merged_at,
            merge_commit,
            manifest_entries_applied,
            cross_task_imports,
            reconciliation_run
        ],
    )?;
    Ok(conn.last_insert_rowid())
}

/// Get integration log entries for a worker assignment.
#[allow(dead_code)]
pub fn integration_log_by_assignment(
    conn: &Connection,
    assignment_id: i64,
) -> Result<Vec<IntegrationLogEntry>> {
    let mut stmt = conn.prepare(
        "SELECT id, assignment_id, merged_at, merge_commit, manifest_entries_applied, \
         cross_task_imports, reconciliation_run FROM integration_log WHERE assignment_id = ?1 ORDER BY id ASC",
    )?;
    let rows = stmt
        .query_map(rusqlite::params![assignment_id], map_integration_log)?
        .collect::<Result<Vec<_>>>()?;
    Ok(rows)
}

/// An integration log entry joined with worker_assignment info for display.
#[derive(Debug)]
#[allow(dead_code)]
pub struct IntegrationLogView {
    pub id: i64,
    pub assignment_id: i64,
    pub bead_id: String,
    pub merged_at: String,
    pub merge_commit: String,
    pub manifest_entries_applied: Option<String>,
    pub cross_task_imports: Option<String>,
    pub reconciliation_run: bool,
    pub status: String,
}

/// Query recent integration log entries joined with worker_assignments.
/// If `last` is Some, limits to the N most recent entries.
/// Returns entries ordered by integration_log.id DESC (most recent first).
pub fn recent_integration_log(
    conn: &Connection,
    last: Option<i64>,
) -> Result<Vec<IntegrationLogView>> {
    let sql = match last {
        Some(_) => {
            "SELECT il.id, il.assignment_id, wa.bead_id, il.merged_at, il.merge_commit, \
             il.manifest_entries_applied, il.cross_task_imports, il.reconciliation_run, wa.status \
             FROM integration_log il \
             JOIN worker_assignments wa ON il.assignment_id = wa.id \
             ORDER BY il.id DESC LIMIT ?1"
        }
        None => {
            "SELECT il.id, il.assignment_id, wa.bead_id, il.merged_at, il.merge_commit, \
             il.manifest_entries_applied, il.cross_task_imports, il.reconciliation_run, wa.status \
             FROM integration_log il \
             JOIN worker_assignments wa ON il.assignment_id = wa.id \
             ORDER BY il.id DESC"
        }
    };

    let mut stmt = conn.prepare(sql)?;
    let rows = match last {
        Some(n) => stmt
            .query_map(rusqlite::params![n], map_integration_log_view)?
            .collect::<Result<Vec<_>>>()?,
        None => stmt
            .query_map([], map_integration_log_view)?
            .collect::<Result<Vec<_>>>()?,
    };
    Ok(rows)
}

/// Find the most recent failed worker assignment for a given bead_id.
/// Returns the assignment if it exists and is in a failed/integration_failed state.
pub fn find_failed_assignment_by_bead(
    conn: &Connection,
    bead_id: &str,
) -> Result<Option<WorkerAssignment>> {
    let mut stmt = conn.prepare(
        "SELECT id, worker_id, bead_id, worktree_path, status, affected_globs, \
         started_at, completed_at, failure_notes FROM worker_assignments \
         WHERE bead_id = ?1 AND status IN ('failed', 'integration_failed') \
         ORDER BY id DESC LIMIT 1",
    )?;
    let mut rows = stmt.query_map(rusqlite::params![bead_id], map_worker_assignment)?;
    match rows.next() {
        Some(row) => Ok(Some(row?)),
        None => Ok(None),
    }
}

fn map_integration_log_view(row: &rusqlite::Row) -> Result<IntegrationLogView> {
    Ok(IntegrationLogView {
        id: row.get(0)?,
        assignment_id: row.get(1)?,
        bead_id: row.get(2)?,
        merged_at: row.get(3)?,
        merge_commit: row.get(4)?,
        manifest_entries_applied: row.get(5)?,
        cross_task_imports: row.get(6)?,
        reconciliation_run: row.get(7)?,
        status: row.get(8)?,
    })
}

/// Find the most recent successful integration log entry for a bead_id.
/// Joins with worker_assignments to find entries by bead_id.
pub fn find_integration_by_bead(
    conn: &Connection,
    bead_id: &str,
) -> Result<Option<IntegrationLogView>> {
    let mut stmt = conn.prepare(
        "SELECT il.id, il.assignment_id, wa.bead_id, il.merged_at, il.merge_commit, \
         il.manifest_entries_applied, il.cross_task_imports, il.reconciliation_run, wa.status \
         FROM integration_log il \
         JOIN worker_assignments wa ON il.assignment_id = wa.id \
         WHERE wa.bead_id = ?1 \
         ORDER BY il.id DESC LIMIT 1",
    )?;
    let mut rows = stmt.query_map(rusqlite::params![bead_id], map_integration_log_view)?;
    match rows.next() {
        Some(row) => Ok(Some(row?)),
        None => Ok(None),
    }
}

/// Find beads that are entangled with the given bead — i.e., other beads
/// whose cross_task_imports reference this bead's modules.
/// Returns a list of (bead_id, cross_task_imports) for entangled beads.
pub fn find_entangled_beads(conn: &Connection, bead_id: &str) -> Result<Vec<(String, String)>> {
    let pattern = format!("%{bead_id}%");
    let mut stmt = conn.prepare(
        "SELECT wa.bead_id, il.cross_task_imports \
         FROM integration_log il \
         JOIN worker_assignments wa ON il.assignment_id = wa.id \
         WHERE il.cross_task_imports LIKE ?1 \
         AND wa.bead_id != ?2 \
         AND wa.status = 'integrated' \
         ORDER BY il.id DESC",
    )?;
    let rows = stmt
        .query_map(rusqlite::params![pattern, bead_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<Result<Vec<_>>>()?;
    Ok(rows)
}

// ── Integration Iterations ─────────────────────────────────────────

/// A row from the integration_iterations table.
#[derive(Debug)]
#[allow(dead_code)]
pub struct IntegrationIteration {
    pub id: i64,
    pub assignment_id: i64,
    pub bead_id: String,
    pub iteration_count: u32,
    pub modules: Option<String>,
    pub recorded_at: String,
}

/// Record integration loop iteration count for a task.
///
/// `iteration_count` is the number of fix-loop iterations during integration
/// (0 means the compiler check passed on the first try).
/// `modules` is a comma-separated list of modules involved in the fix loop.
pub fn record_integration_iterations(
    conn: &Connection,
    assignment_id: i64,
    bead_id: &str,
    iteration_count: u32,
    modules: Option<&str>,
    recorded_at: &str,
) -> Result<i64> {
    conn.execute(
        "INSERT INTO integration_iterations (assignment_id, bead_id, iteration_count, modules, recorded_at) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
        rusqlite::params![assignment_id, bead_id, iteration_count, modules, recorded_at],
    )?;
    Ok(conn.last_insert_rowid())
}

/// Get integration iteration records for a bead.
#[allow(dead_code)]
pub fn integration_iterations_by_bead(
    conn: &Connection,
    bead_id: &str,
) -> Result<Vec<IntegrationIteration>> {
    let mut stmt = conn.prepare(
        "SELECT id, assignment_id, bead_id, iteration_count, modules, recorded_at \
         FROM integration_iterations WHERE bead_id = ?1 ORDER BY id ASC",
    )?;
    let rows = stmt
        .query_map(rusqlite::params![bead_id], |row| {
            Ok(IntegrationIteration {
                id: row.get(0)?,
                assignment_id: row.get(1)?,
                bead_id: row.get(2)?,
                iteration_count: row.get(3)?,
                modules: row.get(4)?,
                recorded_at: row.get(5)?,
            })
        })?
        .collect::<Result<Vec<_>>>()?;
    Ok(rows)
}

/// Get all integration iterations with high iteration counts (> threshold).
/// Useful for the architecture agent to identify problematic interfaces.
#[allow(dead_code)]
pub fn high_iteration_integrations(
    conn: &Connection,
    threshold: u32,
) -> Result<Vec<IntegrationIteration>> {
    let mut stmt = conn.prepare(
        "SELECT id, assignment_id, bead_id, iteration_count, modules, recorded_at \
         FROM integration_iterations WHERE iteration_count > ?1 ORDER BY iteration_count DESC",
    )?;
    let rows = stmt
        .query_map(rusqlite::params![threshold], |row| {
            Ok(IntegrationIteration {
                id: row.get(0)?,
                assignment_id: row.get(1)?,
                bead_id: row.get(2)?,
                iteration_count: row.get(3)?,
                modules: row.get(4)?,
                recorded_at: row.get(5)?,
            })
        })?
        .collect::<Result<Vec<_>>>()?;
    Ok(rows)
}

#[allow(dead_code)]
fn map_integration_log(row: &rusqlite::Row) -> Result<IntegrationLogEntry> {
    Ok(IntegrationLogEntry {
        id: row.get(0)?,
        assignment_id: row.get(1)?,
        merged_at: row.get(2)?,
        merge_commit: row.get(3)?,
        manifest_entries_applied: row.get(4)?,
        cross_task_imports: row.get(5)?,
        reconciliation_run: row.get(6)?,
    })
}

// ── Bead Metrics ───────────────────────────────────────────────────

/// A row from the bead_metrics table.
#[derive(Debug)]
pub struct BeadMetrics {
    pub bead_id: String,
    pub sessions: i64,
    pub wall_time_secs: f64,
    pub total_turns: i64,
    pub total_output_tokens: Option<i64>,
    pub integration_time_secs: Option<f64>,
    pub completed_at: Option<String>,
}

/// Upsert bead metrics — inserts or updates cumulative metrics for a bead.
#[allow(clippy::too_many_arguments)]
pub fn upsert_bead_metrics(
    conn: &Connection,
    bead_id: &str,
    sessions: i64,
    wall_time_secs: f64,
    total_turns: i64,
    total_output_tokens: Option<i64>,
    integration_time_secs: Option<f64>,
    completed_at: Option<&str>,
) -> Result<()> {
    conn.execute(
        "INSERT INTO bead_metrics (bead_id, sessions, wall_time_secs, total_turns, \
         total_output_tokens, integration_time_secs, completed_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) \
         ON CONFLICT(bead_id) DO UPDATE SET \
         sessions = ?2, wall_time_secs = ?3, total_turns = ?4, \
         total_output_tokens = ?5, integration_time_secs = ?6, completed_at = ?7",
        rusqlite::params![
            bead_id,
            sessions,
            wall_time_secs,
            total_turns,
            total_output_tokens,
            integration_time_secs,
            completed_at
        ],
    )?;
    Ok(())
}

/// Get bead metrics by bead_id.
pub fn get_bead_metrics(conn: &Connection, bead_id: &str) -> Result<Option<BeadMetrics>> {
    let mut stmt = conn.prepare(
        "SELECT bead_id, sessions, wall_time_secs, total_turns, total_output_tokens, \
         integration_time_secs, completed_at FROM bead_metrics WHERE bead_id = ?1",
    )?;
    let mut rows = stmt.query_map(rusqlite::params![bead_id], map_bead_metrics)?;
    match rows.next() {
        Some(row) => Ok(Some(row?)),
        None => Ok(None),
    }
}

/// List all bead metrics, ordered by bead_id.
pub fn all_bead_metrics(conn: &Connection) -> Result<Vec<BeadMetrics>> {
    let mut stmt = conn.prepare(
        "SELECT bead_id, sessions, wall_time_secs, total_turns, total_output_tokens, \
         integration_time_secs, completed_at FROM bead_metrics ORDER BY bead_id ASC",
    )?;
    let rows = stmt
        .query_map([], map_bead_metrics)?
        .collect::<Result<Vec<_>>>()?;
    Ok(rows)
}

/// List completed bead metrics (where completed_at IS NOT NULL), ordered by bead_id.
pub fn completed_bead_metrics(conn: &Connection) -> Result<Vec<BeadMetrics>> {
    let mut stmt = conn.prepare(
        "SELECT bead_id, sessions, wall_time_secs, total_turns, total_output_tokens, \
         integration_time_secs, completed_at FROM bead_metrics \
         WHERE completed_at IS NOT NULL ORDER BY bead_id ASC",
    )?;
    let rows = stmt
        .query_map([], map_bead_metrics)?
        .collect::<Result<Vec<_>>>()?;
    Ok(rows)
}

fn map_bead_metrics(row: &rusqlite::Row) -> Result<BeadMetrics> {
    Ok(BeadMetrics {
        bead_id: row.get(0)?,
        sessions: row.get(1)?,
        wall_time_secs: row.get(2)?,
        total_turns: row.get(3)?,
        total_output_tokens: row.get(4)?,
        integration_time_secs: row.get(5)?,
        completed_at: row.get(6)?,
    })
}

fn map_observation(row: &rusqlite::Row) -> Result<Observation> {
    Ok(Observation {
        session: row.get(0)?,
        ts: row.get(1)?,
        duration: row.get(2)?,
        outcome: row.get(3)?,
        data: row.get(4)?,
    })
}

fn map_improvement(row: &rusqlite::Row) -> Result<Improvement> {
    Ok(Improvement {
        ref_id: row.get(0)?,
        created: row.get(1)?,
        category: row.get(2)?,
        status: row.get(3)?,
        title: row.get(4)?,
        body: row.get(5)?,
        context: row.get(6)?,
        tags: row.get(7)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::params;
    use tempfile::TempDir;

    fn test_db() -> (TempDir, Connection) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("blacksmith.db");
        let conn = open_or_create(&path).unwrap();
        (dir, conn)
    }

    #[test]
    fn creates_database_and_table() {
        let (_dir, conn) = test_db();

        // Verify improvements table exists by querying it
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM improvements", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn idempotent_creation() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("blacksmith.db");

        // Open twice — should not error
        let conn1 = open_or_create(&path).unwrap();
        drop(conn1);
        let conn2 = open_or_create(&path).unwrap();

        let count: i64 = conn2
            .query_row("SELECT COUNT(*) FROM improvements", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn insert_and_query_improvement() {
        let (_dir, conn) = test_db();

        conn.execute(
            "INSERT INTO improvements (ref, category, title) VALUES (?1, ?2, ?3)",
            params!["R1", "workflow", "Use parallel tool calls"],
        )
        .unwrap();

        let title: String = conn
            .query_row(
                "SELECT title FROM improvements WHERE ref = 'R1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(title, "Use parallel tool calls");
    }

    #[test]
    fn default_status_is_open() {
        let (_dir, conn) = test_db();

        conn.execute(
            "INSERT INTO improvements (ref, category, title) VALUES (?1, ?2, ?3)",
            params!["R1", "cost", "Reduce token usage"],
        )
        .unwrap();

        let status: String = conn
            .query_row(
                "SELECT status FROM improvements WHERE ref = 'R1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(status, "open");
    }

    #[test]
    fn created_timestamp_auto_set() {
        let (_dir, conn) = test_db();

        conn.execute(
            "INSERT INTO improvements (ref, category, title) VALUES (?1, ?2, ?3)",
            params!["R1", "reliability", "Add retry logic"],
        )
        .unwrap();

        let created: String = conn
            .query_row(
                "SELECT created FROM improvements WHERE ref = 'R1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        // Should be a valid ISO timestamp
        assert!(created.contains('T'));
        assert!(created.ends_with('Z'));
    }

    #[test]
    fn ref_uniqueness_enforced() {
        let (_dir, conn) = test_db();

        conn.execute(
            "INSERT INTO improvements (ref, category, title) VALUES (?1, ?2, ?3)",
            params!["R1", "workflow", "First"],
        )
        .unwrap();

        let result = conn.execute(
            "INSERT INTO improvements (ref, category, title) VALUES (?1, ?2, ?3)",
            params!["R1", "cost", "Duplicate"],
        );
        assert!(result.is_err());
    }

    #[test]
    fn next_ref_empty_table() {
        let (_dir, conn) = test_db();
        assert_eq!(next_ref(&conn).unwrap(), "R1");
    }

    #[test]
    fn next_ref_after_inserts() {
        let (_dir, conn) = test_db();

        conn.execute(
            "INSERT INTO improvements (ref, category, title) VALUES (?1, ?2, ?3)",
            params!["R1", "workflow", "First"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO improvements (ref, category, title) VALUES (?1, ?2, ?3)",
            params!["R3", "cost", "Third"],
        )
        .unwrap();

        // Should be R4 (max is R3)
        assert_eq!(next_ref(&conn).unwrap(), "R4");
    }

    #[test]
    fn next_ref_handles_gaps() {
        let (_dir, conn) = test_db();

        conn.execute(
            "INSERT INTO improvements (ref, category, title) VALUES (?1, ?2, ?3)",
            params!["R5", "performance", "Skip ahead"],
        )
        .unwrap();

        assert_eq!(next_ref(&conn).unwrap(), "R6");
    }

    #[test]
    fn all_columns_insertable() {
        let (_dir, conn) = test_db();

        conn.execute(
            "INSERT INTO improvements (ref, category, status, title, body, context, tags, meta)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                "R1",
                "code-quality",
                "promoted",
                "Enforce clippy lints",
                "Run clippy --fix as part of CI",
                "Sessions 10-15 had repeated lint warnings",
                "ci,lint,quality",
                r#"{"bead_id": "beads-abc"}"#,
            ],
        )
        .unwrap();

        let (body, context, tags, meta): (String, String, String, String) = conn
            .query_row(
                "SELECT body, context, tags, meta FROM improvements WHERE ref = 'R1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();

        assert_eq!(body, "Run clippy --fix as part of CI");
        assert_eq!(context, "Sessions 10-15 had repeated lint warnings");
        assert_eq!(tags, "ci,lint,quality");
        assert!(meta.contains("bead_id"));
    }

    #[test]
    fn index_on_status_works() {
        let (_dir, conn) = test_db();

        for i in 1..=5 {
            let status = if i <= 3 { "open" } else { "promoted" };
            conn.execute(
                "INSERT INTO improvements (ref, category, status, title) VALUES (?1, ?2, ?3, ?4)",
                params![format!("R{i}"), "workflow", status, format!("Item {i}")],
            )
            .unwrap();
        }

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM improvements WHERE status = 'open'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 3);
    }

    #[test]
    fn resolved_timestamp_nullable() {
        let (_dir, conn) = test_db();

        conn.execute(
            "INSERT INTO improvements (ref, category, title) VALUES (?1, ?2, ?3)",
            params!["R1", "workflow", "Test"],
        )
        .unwrap();

        let resolved: Option<String> = conn
            .query_row(
                "SELECT resolved FROM improvements WHERE ref = 'R1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(resolved.is_none());

        // Set resolved
        conn.execute(
            "UPDATE improvements SET resolved = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE ref = 'R1'",
            [],
        )
        .unwrap();

        let resolved: Option<String> = conn
            .query_row(
                "SELECT resolved FROM improvements WHERE ref = 'R1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(resolved.is_some());
    }

    #[test]
    fn opens_existing_database() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("blacksmith.db");

        // Create and insert
        {
            let conn = open_or_create(&path).unwrap();
            conn.execute(
                "INSERT INTO improvements (ref, category, title) VALUES (?1, ?2, ?3)",
                params!["R1", "workflow", "Persisted"],
            )
            .unwrap();
        }

        // Reopen and verify data persisted
        {
            let conn = open_or_create(&path).unwrap();
            let title: String = conn
                .query_row(
                    "SELECT title FROM improvements WHERE ref = 'R1'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(title, "Persisted");
        }
    }

    // ── Events table tests ──────────────────────────────────────────────

    // ── get_improvement tests ────────────────────────────────────────

    #[test]
    fn get_improvement_found() {
        let (_dir, conn) = test_db();
        insert_improvement(
            &conn,
            "workflow",
            "Test item",
            Some("body"),
            Some("ctx"),
            Some("t1"),
        )
        .unwrap();

        let imp = get_improvement(&conn, "R1").unwrap().unwrap();
        assert_eq!(imp.ref_id, "R1");
        assert_eq!(imp.title, "Test item");
        assert_eq!(imp.body.as_deref(), Some("body"));
        assert_eq!(imp.context.as_deref(), Some("ctx"));
        assert_eq!(imp.tags.as_deref(), Some("t1"));
    }

    #[test]
    fn get_improvement_not_found() {
        let (_dir, conn) = test_db();
        let imp = get_improvement(&conn, "R999").unwrap();
        assert!(imp.is_none());
    }

    #[test]
    fn get_improvement_meta_found() {
        let (_dir, conn) = test_db();
        conn.execute(
            "INSERT INTO improvements (ref, category, title, meta) VALUES (?1, ?2, ?3, ?4)",
            params!["R1", "workflow", "Test", r#"{"key": "val"}"#],
        )
        .unwrap();

        let meta = get_improvement_meta(&conn, "R1").unwrap();
        assert!(meta.is_some());
        assert!(meta.unwrap().contains("key"));
    }

    // ── update_improvement tests ─────────────────────────────────────

    #[test]
    fn update_improvement_status() {
        let (_dir, conn) = test_db();
        insert_improvement(&conn, "workflow", "Item", None, None, None).unwrap();

        let updated = update_improvement(&conn, "R1", Some("validated"), None, None, None).unwrap();
        assert!(updated);

        let imp = get_improvement(&conn, "R1").unwrap().unwrap();
        assert_eq!(imp.status, "validated");
    }

    #[test]
    fn update_improvement_body_context() {
        let (_dir, conn) = test_db();
        insert_improvement(&conn, "workflow", "Item", None, None, None).unwrap();

        update_improvement(&conn, "R1", None, Some("new body"), Some("new ctx"), None).unwrap();

        let imp = get_improvement(&conn, "R1").unwrap().unwrap();
        assert_eq!(imp.body.as_deref(), Some("new body"));
        assert_eq!(imp.context.as_deref(), Some("new ctx"));
    }

    #[test]
    fn update_improvement_sets_resolved_on_promote() {
        let (_dir, conn) = test_db();
        insert_improvement(&conn, "workflow", "Item", None, None, None).unwrap();

        update_improvement(&conn, "R1", Some("promoted"), None, None, None).unwrap();

        let resolved: Option<String> = conn
            .query_row(
                "SELECT resolved FROM improvements WHERE ref = 'R1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(resolved.is_some());
    }

    #[test]
    fn update_improvement_sets_resolved_on_dismiss() {
        let (_dir, conn) = test_db();
        insert_improvement(&conn, "workflow", "Item", None, None, None).unwrap();

        update_improvement(&conn, "R1", Some("dismissed"), None, None, None).unwrap();

        let resolved: Option<String> = conn
            .query_row(
                "SELECT resolved FROM improvements WHERE ref = 'R1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(resolved.is_some());
    }

    #[test]
    fn update_improvement_no_resolved_on_other_status() {
        let (_dir, conn) = test_db();
        insert_improvement(&conn, "workflow", "Item", None, None, None).unwrap();

        update_improvement(&conn, "R1", Some("validated"), None, None, None).unwrap();

        let resolved: Option<String> = conn
            .query_row(
                "SELECT resolved FROM improvements WHERE ref = 'R1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(resolved.is_none());
    }

    #[test]
    fn update_improvement_nonexistent() {
        let (_dir, conn) = test_db();
        let updated = update_improvement(&conn, "R999", Some("open"), None, None, None).unwrap();
        assert!(!updated);
    }

    #[test]
    fn update_improvement_nothing_to_update() {
        let (_dir, conn) = test_db();
        insert_improvement(&conn, "workflow", "Item", None, None, None).unwrap();
        let updated = update_improvement(&conn, "R1", None, None, None, None).unwrap();
        assert!(!updated);
    }

    #[test]
    fn update_improvement_meta() {
        let (_dir, conn) = test_db();
        insert_improvement(&conn, "workflow", "Item", None, None, None).unwrap();

        update_improvement(&conn, "R1", None, None, None, Some(r#"{"reason": "test"}"#)).unwrap();

        let meta = get_improvement_meta(&conn, "R1").unwrap();
        assert!(meta.is_some());
        assert!(meta.unwrap().contains("test"));
    }

    // ── search_improvements tests ────────────────────────────────────

    #[test]
    fn search_improvements_by_title() {
        let (_dir, conn) = test_db();
        insert_improvement(&conn, "workflow", "Reduce token usage", None, None, None).unwrap();
        insert_improvement(&conn, "cost", "Fix retry logic", None, None, None).unwrap();

        let results = search_improvements(&conn, "token").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].title, "Reduce token usage");
    }

    #[test]
    fn search_improvements_by_body() {
        let (_dir, conn) = test_db();
        insert_improvement(
            &conn,
            "workflow",
            "Title",
            Some("Use parallel tool calls"),
            None,
            None,
        )
        .unwrap();

        let results = search_improvements(&conn, "parallel").unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn search_improvements_by_context() {
        let (_dir, conn) = test_db();
        insert_improvement(
            &conn,
            "workflow",
            "Title",
            None,
            Some("sessions 340-348"),
            None,
        )
        .unwrap();

        let results = search_improvements(&conn, "340").unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn search_improvements_no_match() {
        let (_dir, conn) = test_db();
        insert_improvement(&conn, "workflow", "Something", None, None, None).unwrap();

        let results = search_improvements(&conn, "nonexistent").unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn search_improvements_case_insensitive() {
        let (_dir, conn) = test_db();
        insert_improvement(&conn, "cost", "Token Usage", None, None, None).unwrap();

        let results = search_improvements(&conn, "token").unwrap();
        assert_eq!(results.len(), 1);
    }

    // ── Events table tests ──────────────────────────────────────────────

    #[test]
    fn events_table_exists() {
        let (_dir, conn) = test_db();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn insert_event_basic() {
        let (_dir, conn) = test_db();
        let id = insert_event(&conn, 1, "turns.total", Some("67"), None).unwrap();
        assert!(id > 0);

        let events = events_by_session(&conn, 1).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].session, 1);
        assert_eq!(events[0].kind, "turns.total");
        assert_eq!(events[0].value.as_deref(), Some("67"));
        assert!(events[0].tags.is_none());
    }

    #[test]
    fn insert_event_with_tags() {
        let (_dir, conn) = test_db();
        insert_event(&conn, 1, "commit.detected", Some("true"), Some("ci,deploy")).unwrap();

        let events = events_by_session(&conn, 1).unwrap();
        assert_eq!(events[0].tags.as_deref(), Some("ci,deploy"));
    }

    #[test]
    fn insert_event_null_value() {
        let (_dir, conn) = test_db();
        insert_event(&conn, 1, "session.start", None, None).unwrap();

        let events = events_by_session(&conn, 1).unwrap();
        assert_eq!(events.len(), 1);
        assert!(events[0].value.is_none());
    }

    #[test]
    fn events_timestamp_auto_set() {
        let (_dir, conn) = test_db();
        insert_event(&conn, 1, "session.start", None, None).unwrap();

        let events = events_by_session(&conn, 1).unwrap();
        assert!(events[0].ts.contains('T'));
        assert!(events[0].ts.ends_with('Z'));
    }

    #[test]
    fn insert_event_with_explicit_ts() {
        let (_dir, conn) = test_db();
        let ts = "2026-01-15T10:30:00Z";
        insert_event_with_ts(&conn, ts, 42, "turns.total", Some("55"), None).unwrap();

        let events = events_by_session(&conn, 42).unwrap();
        assert_eq!(events[0].ts, ts);
    }

    #[test]
    fn multiple_events_per_session() {
        let (_dir, conn) = test_db();
        insert_event(&conn, 5, "turns.total", Some("67"), None).unwrap();
        insert_event(&conn, 5, "cost.estimate_usd", Some("24.57"), None).unwrap();
        insert_event(&conn, 5, "session.outcome", Some("\"completed\""), None).unwrap();

        let events = events_by_session(&conn, 5).unwrap();
        assert_eq!(events.len(), 3);
        // Should be ordered by id (insertion order)
        assert_eq!(events[0].kind, "turns.total");
        assert_eq!(events[1].kind, "cost.estimate_usd");
        assert_eq!(events[2].kind, "session.outcome");
    }

    #[test]
    fn events_by_kind_query() {
        let (_dir, conn) = test_db();
        insert_event(&conn, 1, "turns.total", Some("50"), None).unwrap();
        insert_event(&conn, 2, "turns.total", Some("67"), None).unwrap();
        insert_event(&conn, 2, "cost.estimate_usd", Some("20"), None).unwrap();
        insert_event(&conn, 3, "turns.total", Some("80"), None).unwrap();

        let turns = events_by_kind(&conn, "turns.total").unwrap();
        assert_eq!(turns.len(), 3);
        assert_eq!(turns[0].session, 1);
        assert_eq!(turns[1].session, 2);
        assert_eq!(turns[2].session, 3);
    }

    #[test]
    fn events_session_index_works() {
        let (_dir, conn) = test_db();
        // Insert events across multiple sessions
        for session in 1..=10 {
            insert_event(&conn, session, "turns.total", Some("50"), None).unwrap();
        }
        // Query specific session — index should be used
        let events = events_by_session(&conn, 5).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].session, 5);
    }

    #[test]
    fn events_empty_session_returns_empty() {
        let (_dir, conn) = test_db();
        let events = events_by_session(&conn, 999).unwrap();
        assert!(events.is_empty());
    }

    // ── Observations table tests ────────────────────────────────────────

    #[test]
    fn observations_table_exists() {
        let (_dir, conn) = test_db();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM observations", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn upsert_observation_insert() {
        let (_dir, conn) = test_db();
        let data = r#"{"turns.total": 67, "cost.estimate_usd": 24.57}"#;
        upsert_observation(
            &conn,
            1,
            "2026-01-15T10:30:00Z",
            Some(1847),
            Some("completed"),
            data,
        )
        .unwrap();

        let obs = get_observation(&conn, 1).unwrap().unwrap();
        assert_eq!(obs.session, 1);
        assert_eq!(obs.ts, "2026-01-15T10:30:00Z");
        assert_eq!(obs.duration, Some(1847));
        assert_eq!(obs.outcome.as_deref(), Some("completed"));
        assert_eq!(obs.data, data);
    }

    #[test]
    fn upsert_observation_replaces() {
        let (_dir, conn) = test_db();
        let data1 = r#"{"turns.total": 50}"#;
        let data2 = r#"{"turns.total": 67, "cost.estimate_usd": 24.57}"#;

        upsert_observation(
            &conn,
            1,
            "2026-01-15T10:30:00Z",
            Some(1000),
            Some("failed"),
            data1,
        )
        .unwrap();
        upsert_observation(
            &conn,
            1,
            "2026-01-15T10:30:00Z",
            Some(1847),
            Some("completed"),
            data2,
        )
        .unwrap();

        let obs = get_observation(&conn, 1).unwrap().unwrap();
        assert_eq!(obs.duration, Some(1847));
        assert_eq!(obs.outcome.as_deref(), Some("completed"));
        assert_eq!(obs.data, data2);

        // Should still be just one row
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM observations", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn observation_nullable_fields() {
        let (_dir, conn) = test_db();
        upsert_observation(&conn, 1, "2026-01-15T10:30:00Z", None, None, "{}").unwrap();

        let obs = get_observation(&conn, 1).unwrap().unwrap();
        assert!(obs.duration.is_none());
        assert!(obs.outcome.is_none());
    }

    #[test]
    fn get_observation_nonexistent() {
        let (_dir, conn) = test_db();
        let obs = get_observation(&conn, 999).unwrap();
        assert!(obs.is_none());
    }

    #[test]
    fn recent_observations_ordering() {
        let (_dir, conn) = test_db();
        for session in 1..=5 {
            let data = format!(r#"{{"session": {session}}}"#);
            upsert_observation(
                &conn,
                session,
                "2026-01-15T10:30:00Z",
                Some(1000),
                Some("completed"),
                &data,
            )
            .unwrap();
        }

        let recent = recent_observations(&conn, 3).unwrap();
        assert_eq!(recent.len(), 3);
        // Should be descending by session
        assert_eq!(recent[0].session, 5);
        assert_eq!(recent[1].session, 4);
        assert_eq!(recent[2].session, 3);
    }

    #[test]
    fn recent_observations_limit() {
        let (_dir, conn) = test_db();
        for session in 1..=10 {
            upsert_observation(&conn, session, "2026-01-15T10:30:00Z", None, None, "{}").unwrap();
        }

        let recent = recent_observations(&conn, 5).unwrap();
        assert_eq!(recent.len(), 5);
    }

    #[test]
    fn observations_session_is_primary_key() {
        let (_dir, conn) = test_db();
        // Insert two different sessions — both should succeed
        upsert_observation(&conn, 1, "2026-01-15T10:30:00Z", None, None, "{}").unwrap();
        upsert_observation(&conn, 2, "2026-01-15T11:30:00Z", None, None, "{}").unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM observations", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn events_and_observations_coexist() {
        let (_dir, conn) = test_db();

        // Insert events for session 1
        insert_event(&conn, 1, "turns.total", Some("67"), None).unwrap();
        insert_event(&conn, 1, "cost.estimate_usd", Some("24.57"), None).unwrap();

        // Insert observation for session 1
        let data = r#"{"turns.total": 67, "cost.estimate_usd": 24.57}"#;
        upsert_observation(
            &conn,
            1,
            "2026-01-15T10:30:00Z",
            Some(1847),
            Some("completed"),
            data,
        )
        .unwrap();

        // Both should be queryable
        let events = events_by_session(&conn, 1).unwrap();
        assert_eq!(events.len(), 2);

        let obs = get_observation(&conn, 1).unwrap().unwrap();
        assert_eq!(obs.data, data);
    }

    #[test]
    fn all_three_tables_created() {
        let (_dir, conn) = test_db();

        // Verify all three tables exist
        let tables: Vec<String> = {
            let mut stmt = conn
                .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
                .unwrap();
            stmt.query_map([], |row| row.get(0))
                .unwrap()
                .collect::<Result<Vec<_>>>()
                .unwrap()
        };

        assert!(tables.contains(&"improvements".to_string()));
        assert!(tables.contains(&"events".to_string()));
        assert!(tables.contains(&"observations".to_string()));
    }

    #[test]
    fn events_indexes_created() {
        let (_dir, conn) = test_db();

        let indexes: Vec<String> = {
            let mut stmt = conn
                .prepare(
                    "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='events' ORDER BY name",
                )
                .unwrap();
            stmt.query_map([], |row| row.get(0))
                .unwrap()
                .collect::<Result<Vec<_>>>()
                .unwrap()
        };

        assert!(indexes.contains(&"idx_events_session".to_string()));
        assert!(indexes.contains(&"idx_events_kind".to_string()));
        assert!(indexes.contains(&"idx_events_ts".to_string()));
    }

    // ── Rebuild observations tests ─────────────────────────────────────

    #[test]
    fn rebuild_observations_empty_events() {
        let (_dir, conn) = test_db();
        let count = rebuild_observations(&conn).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn rebuild_observations_from_events() {
        let (_dir, conn) = test_db();

        // Insert events for session 1
        insert_event_with_ts(
            &conn,
            "2026-01-15T10:00:00Z",
            1,
            "turns.total",
            Some("42"),
            None,
        )
        .unwrap();
        insert_event_with_ts(
            &conn,
            "2026-01-15T10:00:00Z",
            1,
            "cost.estimate_usd",
            Some("1.500000"),
            None,
        )
        .unwrap();
        insert_event_with_ts(
            &conn,
            "2026-01-15T10:00:00Z",
            1,
            "session.duration_ms",
            Some("120000"),
            None,
        )
        .unwrap();

        // Insert events for session 2
        insert_event_with_ts(
            &conn,
            "2026-01-15T11:00:00Z",
            2,
            "turns.total",
            Some("30"),
            None,
        )
        .unwrap();
        insert_event_with_ts(
            &conn,
            "2026-01-15T11:00:00Z",
            2,
            "session.duration_ms",
            Some("90000"),
            None,
        )
        .unwrap();

        let count = rebuild_observations(&conn).unwrap();
        assert_eq!(count, 2);

        // Verify session 1 observation
        let obs1 = get_observation(&conn, 1).unwrap().unwrap();
        let data1: serde_json::Value = serde_json::from_str(&obs1.data).unwrap();
        assert_eq!(data1["turns.total"], 42);
        assert_eq!(data1["cost.estimate_usd"], 1.5);
        assert_eq!(data1["session.duration_ms"], 120000);
        assert_eq!(obs1.duration, Some(120)); // 120000ms / 1000

        // Verify session 2 observation
        let obs2 = get_observation(&conn, 2).unwrap().unwrap();
        let data2: serde_json::Value = serde_json::from_str(&obs2.data).unwrap();
        assert_eq!(data2["turns.total"], 30);
        assert_eq!(obs2.duration, Some(90));
    }

    #[test]
    fn rebuild_observations_clears_old() {
        let (_dir, conn) = test_db();

        // Insert a stale observation
        upsert_observation(
            &conn,
            99,
            "2026-01-01T00:00:00Z",
            Some(100),
            None,
            r#"{"old": true}"#,
        )
        .unwrap();

        // Insert events for session 1 only
        insert_event_with_ts(
            &conn,
            "2026-01-15T10:00:00Z",
            1,
            "turns.total",
            Some("10"),
            None,
        )
        .unwrap();

        let count = rebuild_observations(&conn).unwrap();
        assert_eq!(count, 1);

        // Old observation should be gone
        let old = get_observation(&conn, 99).unwrap();
        assert!(old.is_none());

        // New observation should exist
        let new = get_observation(&conn, 1).unwrap();
        assert!(new.is_some());
    }

    #[test]
    fn rebuild_observations_boolean_values() {
        let (_dir, conn) = test_db();

        insert_event_with_ts(
            &conn,
            "2026-01-15T10:00:00Z",
            1,
            "commit.detected",
            Some("true"),
            None,
        )
        .unwrap();

        rebuild_observations(&conn).unwrap();

        let obs = get_observation(&conn, 1).unwrap().unwrap();
        let data: serde_json::Value = serde_json::from_str(&obs.data).unwrap();
        assert_eq!(data["commit.detected"], true);
    }

    #[test]
    fn rebuild_observations_no_duration_event() {
        let (_dir, conn) = test_db();

        // Session with no session.duration_ms event
        insert_event_with_ts(
            &conn,
            "2026-01-15T10:00:00Z",
            1,
            "turns.total",
            Some("5"),
            None,
        )
        .unwrap();

        rebuild_observations(&conn).unwrap();

        let obs = get_observation(&conn, 1).unwrap().unwrap();
        assert!(obs.duration.is_none());
    }

    // ── Coordinator tables tests ────────────────────────────────────────

    #[test]
    fn coordinator_tables_created() {
        let (_dir, conn) = test_db();

        let tables: Vec<String> = {
            let mut stmt = conn
                .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
                .unwrap();
            stmt.query_map([], |row| row.get(0))
                .unwrap()
                .collect::<Result<Vec<_>>>()
                .unwrap()
        };

        assert!(tables.contains(&"worker_assignments".to_string()));
        assert!(tables.contains(&"task_file_changes".to_string()));
        assert!(tables.contains(&"integration_log".to_string()));
        assert!(tables.contains(&"bead_metrics".to_string()));
    }

    #[test]
    fn coordinator_indexes_created() {
        let (_dir, conn) = test_db();

        let indexes: Vec<String> = {
            let mut stmt = conn
                .prepare("SELECT name FROM sqlite_master WHERE type='index' ORDER BY name")
                .unwrap();
            stmt.query_map([], |row| row.get(0))
                .unwrap()
                .collect::<Result<Vec<_>>>()
                .unwrap()
        };

        assert!(indexes.contains(&"idx_worker_assignments_status".to_string()));
        assert!(indexes.contains(&"idx_worker_assignments_bead_id".to_string()));
        assert!(indexes.contains(&"idx_task_file_changes_assignment".to_string()));
        assert!(indexes.contains(&"idx_integration_log_assignment".to_string()));
    }

    // ── Worker Assignments tests ────────────────────────────────────────

    #[test]
    fn insert_and_get_worker_assignment() {
        let (_dir, conn) = test_db();

        let id = insert_worker_assignment(
            &conn,
            0,
            "beads-abc",
            "/tmp/worktree-0",
            "coding",
            Some(r#"["src/db.rs", "src/lib.rs"]"#),
        )
        .unwrap();
        assert!(id > 0);

        let wa = get_worker_assignment(&conn, id).unwrap().unwrap();
        assert_eq!(wa.worker_id, 0);
        assert_eq!(wa.bead_id, "beads-abc");
        assert_eq!(wa.worktree_path, "/tmp/worktree-0");
        assert_eq!(wa.status, "coding");
        assert_eq!(
            wa.affected_globs.as_deref(),
            Some(r#"["src/db.rs", "src/lib.rs"]"#)
        );
        assert!(wa.started_at.contains('T'));
        assert!(wa.completed_at.is_none());
        assert!(wa.failure_notes.is_none());
    }

    #[test]
    fn update_worker_assignment_to_completed() {
        let (_dir, conn) = test_db();

        let id =
            insert_worker_assignment(&conn, 1, "beads-xyz", "/tmp/wt-1", "coding", None).unwrap();
        let updated = update_worker_assignment_status(&conn, id, "completed", None).unwrap();
        assert!(updated);

        let wa = get_worker_assignment(&conn, id).unwrap().unwrap();
        assert_eq!(wa.status, "completed");
        assert!(wa.completed_at.is_some());
        assert!(wa.failure_notes.is_none());
    }

    #[test]
    fn update_worker_assignment_to_failed() {
        let (_dir, conn) = test_db();

        let id =
            insert_worker_assignment(&conn, 2, "beads-fail", "/tmp/wt-2", "coding", None).unwrap();
        let updated =
            update_worker_assignment_status(&conn, id, "failed", Some("tests failing")).unwrap();
        assert!(updated);

        let wa = get_worker_assignment(&conn, id).unwrap().unwrap();
        assert_eq!(wa.status, "failed");
        assert!(wa.completed_at.is_some());
        assert_eq!(wa.failure_notes.as_deref(), Some("tests failing"));
    }

    #[test]
    fn update_worker_assignment_nonexistent() {
        let (_dir, conn) = test_db();
        let updated = update_worker_assignment_status(&conn, 999, "completed", None).unwrap();
        assert!(!updated);
    }

    #[test]
    fn worker_assignments_by_status_query() {
        let (_dir, conn) = test_db();

        insert_worker_assignment(&conn, 0, "beads-a", "/tmp/wt-0", "coding", None).unwrap();
        insert_worker_assignment(&conn, 1, "beads-b", "/tmp/wt-1", "coding", None).unwrap();
        insert_worker_assignment(&conn, 2, "beads-c", "/tmp/wt-2", "completed", None).unwrap();

        let coding = worker_assignments_by_status(&conn, "coding").unwrap();
        assert_eq!(coding.len(), 2);

        let completed = worker_assignments_by_status(&conn, "completed").unwrap();
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].bead_id, "beads-c");
    }

    #[test]
    fn get_worker_assignment_nonexistent() {
        let (_dir, conn) = test_db();
        let wa = get_worker_assignment(&conn, 999).unwrap();
        assert!(wa.is_none());
    }

    #[test]
    fn active_worker_assignments_query() {
        let (_dir, conn) = test_db();

        insert_worker_assignment(&conn, 0, "beads-a", "/tmp/wt-0", "coding", None).unwrap();
        insert_worker_assignment(&conn, 1, "beads-b", "/tmp/wt-1", "integrating", None).unwrap();
        insert_worker_assignment(&conn, 2, "beads-c", "/tmp/wt-2", "completed", None).unwrap();
        insert_worker_assignment(&conn, 3, "beads-d", "/tmp/wt-3", "failed", None).unwrap();

        let active = active_worker_assignments(&conn).unwrap();
        assert_eq!(active.len(), 2);
        assert_eq!(active[0].worker_id, 0);
        assert_eq!(active[0].status, "coding");
        assert_eq!(active[1].worker_id, 1);
        assert_eq!(active[1].status, "integrating");
    }

    #[test]
    fn active_worker_assignments_empty() {
        let (_dir, conn) = test_db();

        insert_worker_assignment(&conn, 0, "beads-a", "/tmp/wt-0", "completed", None).unwrap();

        let active = active_worker_assignments(&conn).unwrap();
        assert!(active.is_empty());
    }

    // ── Task File Changes tests ─────────────────────────────────────────

    #[test]
    fn insert_and_query_task_file_changes() {
        let (_dir, conn) = test_db();

        let id =
            insert_worker_assignment(&conn, 0, "beads-a", "/tmp/wt-0", "coding", None).unwrap();

        insert_task_file_change(&conn, id, "src/db.rs", "modified").unwrap();
        insert_task_file_change(&conn, id, "src/new_file.rs", "added").unwrap();
        insert_task_file_change(&conn, id, "src/old.rs", "deleted").unwrap();

        let changes = file_changes_by_assignment(&conn, id).unwrap();
        assert_eq!(changes.len(), 3);
        // Ordered by file_path ASC
        assert_eq!(changes[0].file_path, "src/db.rs");
        assert_eq!(changes[0].change_type, "modified");
        assert_eq!(changes[1].file_path, "src/new_file.rs");
        assert_eq!(changes[1].change_type, "added");
        assert_eq!(changes[2].file_path, "src/old.rs");
        assert_eq!(changes[2].change_type, "deleted");
    }

    #[test]
    fn file_changes_empty_for_no_assignment() {
        let (_dir, conn) = test_db();
        let changes = file_changes_by_assignment(&conn, 999).unwrap();
        assert!(changes.is_empty());
    }

    // ── Integration Log tests ───────────────────────────────────────────

    #[test]
    fn insert_and_query_integration_log() {
        let (_dir, conn) = test_db();

        let assign_id =
            insert_worker_assignment(&conn, 0, "beads-a", "/tmp/wt-0", "completed", None).unwrap();

        let log_id = insert_integration_log(
            &conn,
            assign_id,
            "2026-02-15T12:00:00Z",
            "abc123def",
            Some(r#"["entry1", "entry2"]"#),
            Some(r#"["import_x"]"#),
            false,
        )
        .unwrap();
        assert!(log_id > 0);

        let entries = integration_log_by_assignment(&conn, assign_id).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].assignment_id, assign_id);
        assert_eq!(entries[0].merged_at, "2026-02-15T12:00:00Z");
        assert_eq!(entries[0].merge_commit, "abc123def");
        assert_eq!(
            entries[0].manifest_entries_applied.as_deref(),
            Some(r#"["entry1", "entry2"]"#)
        );
        assert_eq!(
            entries[0].cross_task_imports.as_deref(),
            Some(r#"["import_x"]"#)
        );
        assert!(!entries[0].reconciliation_run);
    }

    #[test]
    fn integration_log_with_reconciliation() {
        let (_dir, conn) = test_db();

        let assign_id =
            insert_worker_assignment(&conn, 0, "beads-b", "/tmp/wt-0", "completed", None).unwrap();

        insert_integration_log(
            &conn,
            assign_id,
            "2026-02-15T12:00:00Z",
            "def456",
            None,
            None,
            true,
        )
        .unwrap();

        let entries = integration_log_by_assignment(&conn, assign_id).unwrap();
        assert_eq!(entries.len(), 1);
        assert!(entries[0].reconciliation_run);
        assert!(entries[0].manifest_entries_applied.is_none());
        assert!(entries[0].cross_task_imports.is_none());
    }

    #[test]
    fn integration_log_empty_for_no_assignment() {
        let (_dir, conn) = test_db();
        let entries = integration_log_by_assignment(&conn, 999).unwrap();
        assert!(entries.is_empty());
    }

    // ── Bead Metrics tests ──────────────────────────────────────────────

    #[test]
    fn upsert_and_get_bead_metrics() {
        let (_dir, conn) = test_db();

        upsert_bead_metrics(
            &conn,
            "beads-abc",
            3,
            1200.5,
            150,
            Some(50000),
            Some(45.0),
            Some("2026-02-15T12:00:00Z"),
        )
        .unwrap();

        let bm = get_bead_metrics(&conn, "beads-abc").unwrap().unwrap();
        assert_eq!(bm.bead_id, "beads-abc");
        assert_eq!(bm.sessions, 3);
        assert!((bm.wall_time_secs - 1200.5).abs() < f64::EPSILON);
        assert_eq!(bm.total_turns, 150);
        assert_eq!(bm.total_output_tokens, Some(50000));
        assert!((bm.integration_time_secs.unwrap() - 45.0).abs() < f64::EPSILON);
        assert_eq!(bm.completed_at.as_deref(), Some("2026-02-15T12:00:00Z"));
    }

    #[test]
    fn upsert_bead_metrics_updates_existing() {
        let (_dir, conn) = test_db();

        upsert_bead_metrics(&conn, "beads-abc", 1, 400.0, 50, None, None, None).unwrap();
        upsert_bead_metrics(&conn, "beads-abc", 2, 800.0, 100, Some(30000), None, None).unwrap();

        let bm = get_bead_metrics(&conn, "beads-abc").unwrap().unwrap();
        assert_eq!(bm.sessions, 2);
        assert!((bm.wall_time_secs - 800.0).abs() < f64::EPSILON);
        assert_eq!(bm.total_turns, 100);
        assert_eq!(bm.total_output_tokens, Some(30000));

        // Only one row should exist
        let all = all_bead_metrics(&conn).unwrap();
        assert_eq!(all.len(), 1);
    }

    #[test]
    fn get_bead_metrics_nonexistent() {
        let (_dir, conn) = test_db();
        let bm = get_bead_metrics(&conn, "beads-nope").unwrap();
        assert!(bm.is_none());
    }

    #[test]
    fn all_bead_metrics_ordering() {
        let (_dir, conn) = test_db();

        upsert_bead_metrics(&conn, "beads-zzz", 1, 100.0, 10, None, None, None).unwrap();
        upsert_bead_metrics(&conn, "beads-aaa", 2, 200.0, 20, None, None, None).unwrap();
        upsert_bead_metrics(&conn, "beads-mmm", 3, 300.0, 30, None, None, None).unwrap();

        let all = all_bead_metrics(&conn).unwrap();
        assert_eq!(all.len(), 3);
        // Ordered by bead_id ASC
        assert_eq!(all[0].bead_id, "beads-aaa");
        assert_eq!(all[1].bead_id, "beads-mmm");
        assert_eq!(all[2].bead_id, "beads-zzz");
    }

    #[test]
    fn bead_metrics_nullable_fields() {
        let (_dir, conn) = test_db();

        upsert_bead_metrics(&conn, "beads-null", 0, 0.0, 0, None, None, None).unwrap();

        let bm = get_bead_metrics(&conn, "beads-null").unwrap().unwrap();
        assert!(bm.total_output_tokens.is_none());
        assert!(bm.integration_time_secs.is_none());
        assert!(bm.completed_at.is_none());
    }

    #[test]
    fn completed_bead_metrics_filters_correctly() {
        let (_dir, conn) = test_db();

        // Two completed, one not
        upsert_bead_metrics(
            &conn,
            "beads-done1",
            1,
            300.0,
            50,
            None,
            None,
            Some("2026-01-01T00:00:00Z"),
        )
        .unwrap();
        upsert_bead_metrics(
            &conn,
            "beads-done2",
            2,
            400.0,
            60,
            None,
            None,
            Some("2026-01-02T00:00:00Z"),
        )
        .unwrap();
        upsert_bead_metrics(&conn, "beads-open", 1, 100.0, 10, None, None, None).unwrap();

        let completed = completed_bead_metrics(&conn).unwrap();
        assert_eq!(completed.len(), 2);
        assert_eq!(completed[0].bead_id, "beads-done1");
        assert_eq!(completed[1].bead_id, "beads-done2");
    }

    #[test]
    fn completed_bead_metrics_empty_when_none_completed() {
        let (_dir, conn) = test_db();

        upsert_bead_metrics(&conn, "beads-open1", 1, 100.0, 10, None, None, None).unwrap();
        upsert_bead_metrics(&conn, "beads-open2", 1, 200.0, 20, None, None, None).unwrap();

        let completed = completed_bead_metrics(&conn).unwrap();
        assert!(completed.is_empty());
    }

    // ── Recent Integration Log (view) tests ─────────────────────────────

    #[test]
    fn recent_integration_log_empty() {
        let (_dir, conn) = test_db();
        let entries = recent_integration_log(&conn, None).unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn recent_integration_log_returns_entries() {
        let (_dir, conn) = test_db();

        let a1 = insert_worker_assignment(&conn, 0, "beads-abc", "/tmp/wt-0", "integrated", None)
            .unwrap();
        let a2 = insert_worker_assignment(&conn, 1, "beads-def", "/tmp/wt-1", "integrated", None)
            .unwrap();

        insert_integration_log(
            &conn,
            a1,
            "2026-02-15T10:00:00Z",
            "aaa111",
            Some("2 entries applied"),
            None,
            false,
        )
        .unwrap();
        insert_integration_log(
            &conn,
            a2,
            "2026-02-15T11:00:00Z",
            "bbb222",
            None,
            None,
            true,
        )
        .unwrap();

        let entries = recent_integration_log(&conn, None).unwrap();
        assert_eq!(entries.len(), 2);
        // Most recent first (DESC)
        assert_eq!(entries[0].bead_id, "beads-def");
        assert_eq!(entries[0].merge_commit, "bbb222");
        assert!(entries[0].reconciliation_run);
        assert_eq!(entries[0].status, "integrated");
        assert_eq!(entries[1].bead_id, "beads-abc");
        assert_eq!(entries[1].merge_commit, "aaa111");
        assert_eq!(
            entries[1].manifest_entries_applied.as_deref(),
            Some("2 entries applied")
        );
    }

    #[test]
    fn recent_integration_log_with_limit() {
        let (_dir, conn) = test_db();

        for i in 0..5 {
            let aid = insert_worker_assignment(
                &conn,
                i,
                &format!("beads-{i}"),
                &format!("/tmp/wt-{i}"),
                "integrated",
                None,
            )
            .unwrap();
            insert_integration_log(
                &conn,
                aid,
                &format!("2026-02-15T1{i}:00:00Z"),
                &format!("commit{i}"),
                None,
                None,
                false,
            )
            .unwrap();
        }

        let entries = recent_integration_log(&conn, Some(3)).unwrap();
        assert_eq!(entries.len(), 3);
        // Most recent first
        assert_eq!(entries[0].bead_id, "beads-4");
        assert_eq!(entries[1].bead_id, "beads-3");
        assert_eq!(entries[2].bead_id, "beads-2");
    }

    // ── Find Failed Assignment tests ────────────────────────────────────

    #[test]
    fn find_failed_assignment_none() {
        let (_dir, conn) = test_db();
        let result = find_failed_assignment_by_bead(&conn, "beads-nope").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn find_failed_assignment_finds_integration_failed() {
        let (_dir, conn) = test_db();

        let aid = insert_worker_assignment(
            &conn,
            0,
            "beads-fail",
            "/tmp/wt-0",
            "integration_failed",
            None,
        )
        .unwrap();

        let result = find_failed_assignment_by_bead(&conn, "beads-fail").unwrap();
        assert!(result.is_some());
        let wa = result.unwrap();
        assert_eq!(wa.id, aid);
        assert_eq!(wa.status, "integration_failed");
    }

    #[test]
    fn find_failed_assignment_finds_failed_status() {
        let (_dir, conn) = test_db();

        insert_worker_assignment(&conn, 0, "beads-fail2", "/tmp/wt-0", "failed", None).unwrap();

        let result = find_failed_assignment_by_bead(&conn, "beads-fail2").unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().status, "failed");
    }

    #[test]
    fn find_failed_assignment_ignores_completed() {
        let (_dir, conn) = test_db();

        insert_worker_assignment(&conn, 0, "beads-ok", "/tmp/wt-0", "completed", None).unwrap();

        let result = find_failed_assignment_by_bead(&conn, "beads-ok").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn find_failed_assignment_returns_most_recent() {
        let (_dir, conn) = test_db();

        let aid1 = insert_worker_assignment(
            &conn,
            0,
            "beads-multi",
            "/tmp/wt-0",
            "integration_failed",
            None,
        )
        .unwrap();
        let aid2 = insert_worker_assignment(
            &conn,
            1,
            "beads-multi",
            "/tmp/wt-1",
            "integration_failed",
            None,
        )
        .unwrap();

        let result = find_failed_assignment_by_bead(&conn, "beads-multi").unwrap();
        assert!(result.is_some());
        let wa = result.unwrap();
        // Should return the most recent (highest id)
        assert_eq!(wa.id, aid2);
        assert_ne!(wa.id, aid1);
    }

    // ── Find Integration by Bead tests ──────────────────────────────────

    #[test]
    fn find_integration_by_bead_none() {
        let (_dir, conn) = test_db();
        let result = find_integration_by_bead(&conn, "beads-nope").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn find_integration_by_bead_found() {
        let (_dir, conn) = test_db();

        let aid = insert_worker_assignment(&conn, 0, "beads-abc", "/tmp/wt-0", "integrated", None)
            .unwrap();
        insert_integration_log(
            &conn,
            aid,
            "2026-02-15T12:00:00Z",
            "abc123def",
            Some("2 entries"),
            Some("beads-xyz"),
            false,
        )
        .unwrap();

        let result = find_integration_by_bead(&conn, "beads-abc").unwrap();
        assert!(result.is_some());
        let entry = result.unwrap();
        assert_eq!(entry.bead_id, "beads-abc");
        assert_eq!(entry.merge_commit, "abc123def");
        assert_eq!(entry.assignment_id, aid);
        assert_eq!(entry.status, "integrated");
    }

    #[test]
    fn find_integration_by_bead_returns_most_recent() {
        let (_dir, conn) = test_db();

        let aid1 = insert_worker_assignment(&conn, 0, "beads-abc", "/tmp/wt-0", "integrated", None)
            .unwrap();
        insert_integration_log(
            &conn,
            aid1,
            "2026-02-15T10:00:00Z",
            "old_commit",
            None,
            None,
            false,
        )
        .unwrap();

        let aid2 = insert_worker_assignment(&conn, 1, "beads-abc", "/tmp/wt-1", "integrated", None)
            .unwrap();
        insert_integration_log(
            &conn,
            aid2,
            "2026-02-15T12:00:00Z",
            "new_commit",
            None,
            None,
            false,
        )
        .unwrap();

        let result = find_integration_by_bead(&conn, "beads-abc").unwrap();
        let entry = result.unwrap();
        assert_eq!(entry.merge_commit, "new_commit");
        assert_eq!(entry.assignment_id, aid2);
    }

    // ── Find Entangled Beads tests ──────────────────────────────────────

    #[test]
    fn find_entangled_beads_empty() {
        let (_dir, conn) = test_db();
        let result = find_entangled_beads(&conn, "beads-abc").unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn find_entangled_beads_finds_dependents() {
        let (_dir, conn) = test_db();

        // beads-abc was integrated first
        let aid1 = insert_worker_assignment(&conn, 0, "beads-abc", "/tmp/wt-0", "integrated", None)
            .unwrap();
        insert_integration_log(
            &conn,
            aid1,
            "2026-02-15T10:00:00Z",
            "commit_a",
            None,
            None,
            false,
        )
        .unwrap();

        // beads-def depends on beads-abc (imports from it)
        let aid2 = insert_worker_assignment(&conn, 1, "beads-def", "/tmp/wt-1", "integrated", None)
            .unwrap();
        insert_integration_log(
            &conn,
            aid2,
            "2026-02-15T11:00:00Z",
            "commit_b",
            None,
            Some("imports from beads-abc::module"),
            false,
        )
        .unwrap();

        let result = find_entangled_beads(&conn, "beads-abc").unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, "beads-def");
        assert!(result[0].1.contains("beads-abc"));
    }

    #[test]
    fn find_entangled_beads_excludes_self() {
        let (_dir, conn) = test_db();

        let aid = insert_worker_assignment(&conn, 0, "beads-abc", "/tmp/wt-0", "integrated", None)
            .unwrap();
        insert_integration_log(
            &conn,
            aid,
            "2026-02-15T10:00:00Z",
            "commit_a",
            None,
            Some("references beads-abc self-import"),
            false,
        )
        .unwrap();

        let result = find_entangled_beads(&conn, "beads-abc").unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn find_entangled_beads_excludes_non_integrated() {
        let (_dir, conn) = test_db();

        let aid1 = insert_worker_assignment(&conn, 0, "beads-abc", "/tmp/wt-0", "integrated", None)
            .unwrap();
        insert_integration_log(
            &conn,
            aid1,
            "2026-02-15T10:00:00Z",
            "commit_a",
            None,
            None,
            false,
        )
        .unwrap();

        // beads-def depends on beads-abc but is already rolled_back
        let aid2 =
            insert_worker_assignment(&conn, 1, "beads-def", "/tmp/wt-1", "rolled_back", None)
                .unwrap();
        insert_integration_log(
            &conn,
            aid2,
            "2026-02-15T11:00:00Z",
            "commit_b",
            None,
            Some("imports from beads-abc"),
            false,
        )
        .unwrap();

        let result = find_entangled_beads(&conn, "beads-abc").unwrap();
        assert!(result.is_empty());
    }

    // ── Integration Iterations tests ─────────────────────────────────────

    #[test]
    fn record_and_query_integration_iterations() {
        let (_dir, conn) = test_db();
        let aid = insert_worker_assignment(&conn, 0, "beads-iter", "/tmp/wt-0", "integrated", None)
            .unwrap();

        let row_id = record_integration_iterations(
            &conn,
            aid,
            "beads-iter",
            2,
            Some("auth,models"),
            "2026-02-15T12:00:00Z",
        )
        .unwrap();
        assert!(row_id > 0);

        let entries = integration_iterations_by_bead(&conn, "beads-iter").unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].bead_id, "beads-iter");
        assert_eq!(entries[0].iteration_count, 2);
        assert_eq!(entries[0].modules.as_deref(), Some("auth,models"));
        assert_eq!(entries[0].recorded_at, "2026-02-15T12:00:00Z");
    }

    #[test]
    fn integration_iterations_empty_for_unknown_bead() {
        let (_dir, conn) = test_db();
        let entries = integration_iterations_by_bead(&conn, "beads-unknown").unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn integration_iterations_zero_count() {
        let (_dir, conn) = test_db();
        let aid = insert_worker_assignment(&conn, 0, "beads-zero", "/tmp/wt-0", "integrated", None)
            .unwrap();

        record_integration_iterations(&conn, aid, "beads-zero", 0, None, "2026-02-15T12:00:00Z")
            .unwrap();

        let entries = integration_iterations_by_bead(&conn, "beads-zero").unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].iteration_count, 0);
        assert!(entries[0].modules.is_none());
    }

    #[test]
    fn high_iteration_integrations_filters_by_threshold() {
        let (_dir, conn) = test_db();

        let aid1 = insert_worker_assignment(&conn, 0, "beads-low", "/tmp/wt-0", "integrated", None)
            .unwrap();
        let aid2 =
            insert_worker_assignment(&conn, 1, "beads-high", "/tmp/wt-1", "integrated", None)
                .unwrap();
        let aid3 = insert_worker_assignment(&conn, 2, "beads-mid", "/tmp/wt-2", "integrated", None)
            .unwrap();

        record_integration_iterations(&conn, aid1, "beads-low", 1, None, "2026-02-15T12:00:00Z")
            .unwrap();
        record_integration_iterations(
            &conn,
            aid2,
            "beads-high",
            3,
            Some("auth,db"),
            "2026-02-15T12:01:00Z",
        )
        .unwrap();
        record_integration_iterations(
            &conn,
            aid3,
            "beads-mid",
            2,
            Some("models"),
            "2026-02-15T12:02:00Z",
        )
        .unwrap();

        // Threshold=1: should return beads-high (3) and beads-mid (2)
        let results = high_iteration_integrations(&conn, 1).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].bead_id, "beads-high"); // highest first
        assert_eq!(results[0].iteration_count, 3);
        assert_eq!(results[1].bead_id, "beads-mid");
        assert_eq!(results[1].iteration_count, 2);

        // Threshold=2: should return only beads-high (3)
        let results = high_iteration_integrations(&conn, 2).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].bead_id, "beads-high");

        // Threshold=5: should return nothing
        let results = high_iteration_integrations(&conn, 5).unwrap();
        assert!(results.is_empty());
    }
}
