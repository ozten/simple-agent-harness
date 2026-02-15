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
        CREATE INDEX IF NOT EXISTS idx_improvements_category ON improvements(category);",
    )?;

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
}
