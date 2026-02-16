//! Standalone CLI for parsing session JSONL files and storing per-session
//! metrics in a local SQLite database.
//!
//! Usage:
//!   self-improvement log <jsonl-file>
//!   self-improvement backfill [--from N] [--to N]
//!   self-improvement seed
use clap::{Parser, Subcommand};
use rusqlite::Connection;
use serde_json::Value;
use std::io::BufRead;
use std::path::{Path, PathBuf};

/// Default database location relative to the repo root.
const DEFAULT_DB_PATH: &str = "tools/self-improvement.db";

/// Default sessions directory.
const DEFAULT_SESSIONS_DIR: &str = ".blacksmith/sessions";

// ── CLI ──────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(
    name = "self-improvement",
    about = "Parse session JSONL files and store per-session metrics in SQLite"
)]
struct Cli {
    /// Database file path (default: tools/self-improvement.db)
    #[arg(long, default_value = DEFAULT_DB_PATH)]
    db: PathBuf,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Parse one session JSONL file and store metrics
    Log {
        /// Path to the JSONL session file
        file: PathBuf,
    },
    /// Bulk-parse .blacksmith/sessions/*.jsonl files
    Backfill {
        /// Only process sessions >= this number
        #[arg(long)]
        from: Option<u64>,
        /// Only process sessions <= this number
        #[arg(long)]
        to: Option<u64>,
        /// Sessions directory (default: .blacksmith/sessions)
        #[arg(long)]
        sessions_dir: Option<PathBuf>,
    },
    /// Populate improvements table with historical records from the main DB
    Seed {
        /// Path to the main blacksmith.db to import from
        #[arg(long)]
        from: Option<PathBuf>,
    },
}

// ── Schema ───────────────────────────────────────────────────────────────

fn ensure_schema(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch("PRAGMA journal_mode=WAL;")?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS sessions (
            session_id       INTEGER PRIMARY KEY,
            file             TEXT NOT NULL,
            timestamp        TEXT NOT NULL,
            turns            INTEGER NOT NULL DEFAULT 0,
            tool_calls       INTEGER NOT NULL DEFAULT 0,
            narration_only_turns INTEGER NOT NULL DEFAULT 0,
            parallel_tool_calls  INTEGER NOT NULL DEFAULT 0,
            duration_secs    REAL NOT NULL DEFAULT 0,
            output_bytes     INTEGER NOT NULL DEFAULT 0,
            completion       BOOLEAN NOT NULL DEFAULT 0,
            exit_code        INTEGER,
            cost_usd         REAL,
            input_tokens     INTEGER,
            output_tokens    INTEGER
        );

        CREATE TABLE IF NOT EXISTS improvements (
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

        CREATE INDEX IF NOT EXISTS idx_sessions_timestamp ON sessions(timestamp);
        CREATE INDEX IF NOT EXISTS idx_improvements_status ON improvements(status);
        CREATE INDEX IF NOT EXISTS idx_improvements_category ON improvements(category);",
    )?;
    Ok(())
}

// ── Session JSONL parsing ────────────────────────────────────────────────

#[derive(Debug, Default)]
struct SessionMetrics {
    turns: u64,
    tool_calls: u64,
    narration_only_turns: u64,
    parallel_tool_calls: u64,
    duration_secs: f64,
    output_bytes: u64,
    completion: bool,
    exit_code: Option<i32>,
    cost_usd: Option<f64>,
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    timestamp: String,
}

fn parse_session_jsonl(path: &Path) -> Result<SessionMetrics, String> {
    let file =
        std::fs::File::open(path).map_err(|e| format!("Cannot open {}: {e}", path.display()))?;
    let file_size = file.metadata().map(|m| m.len()).unwrap_or(0);
    let reader = std::io::BufReader::new(file);

    let mut m = SessionMetrics {
        output_bytes: file_size,
        ..Default::default()
    };

    // Use file modification time as fallback timestamp
    let file_mtime = std::fs::metadata(path)
        .and_then(|meta| meta.modified())
        .ok()
        .map(|t| {
            let dt: chrono::DateTime<chrono::Utc> = t.into();
            dt.format("%Y-%m-%dT%H:%M:%SZ").to_string()
        })
        .unwrap_or_else(|| chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string());
    m.timestamp = file_mtime;

    for line_result in reader.lines() {
        let line = line_result.map_err(|e| format!("Read error: {e}"))?;
        if line.is_empty() {
            continue;
        }

        let v: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => continue,
        };

        match v.get("type").and_then(|t| t.as_str()) {
            Some("assistant") => count_assistant_turn(&v, &mut m),
            Some("result") => extract_result(&v, &mut m),
            _ => {}
        }
    }

    Ok(m)
}

fn count_assistant_turn(v: &Value, m: &mut SessionMetrics) {
    m.turns += 1;

    let content = match v
        .get("message")
        .and_then(|msg| msg.get("content"))
        .and_then(|c| c.as_array())
    {
        Some(arr) => arr,
        None => return,
    };

    let tool_use_count = content
        .iter()
        .filter(|c| c.get("type").and_then(|t| t.as_str()) == Some("tool_use"))
        .count() as u64;

    let has_text = content
        .iter()
        .any(|c| c.get("type").and_then(|t| t.as_str()) == Some("text"));

    m.tool_calls += tool_use_count;

    if tool_use_count == 0 && has_text {
        m.narration_only_turns += 1;
    }

    if tool_use_count >= 2 {
        m.parallel_tool_calls += 1;
    }
}

fn extract_result(v: &Value, m: &mut SessionMetrics) {
    if let Some(dur_ms) = v.get("duration_ms").and_then(|d| d.as_u64()) {
        m.duration_secs = dur_ms as f64 / 1000.0;
    }

    if let Some(cost) = v.get("total_cost_usd").and_then(|c| c.as_f64()) {
        m.cost_usd = Some(cost);
    }

    // Completion: subtype == "success" and not is_error
    let is_success = v.get("subtype").and_then(|s| s.as_str()) == Some("success");
    let is_error = v.get("is_error").and_then(|e| e.as_bool()).unwrap_or(false);
    m.completion = is_success && !is_error;

    // Extract token counts from modelUsage
    if let Some(model_usage) = v.get("modelUsage").and_then(|u| u.as_object()) {
        let mut input_total: u64 = 0;
        let mut output_total: u64 = 0;
        for (_model, stats) in model_usage {
            input_total += stats
                .get("inputTokens")
                .and_then(|t| t.as_u64())
                .unwrap_or(0);
            output_total += stats
                .get("outputTokens")
                .and_then(|t| t.as_u64())
                .unwrap_or(0);
        }
        m.input_tokens = Some(input_total);
        m.output_tokens = Some(output_total);
    }
}

// ── Database operations ──────────────────────────────────────────────────

fn upsert_session(
    conn: &Connection,
    session_id: u64,
    file: &str,
    m: &SessionMetrics,
) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO sessions (session_id, file, timestamp, turns, tool_calls,
            narration_only_turns, parallel_tool_calls, duration_secs,
            output_bytes, completion, exit_code, cost_usd, input_tokens, output_tokens)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
         ON CONFLICT(session_id) DO UPDATE SET
            file = excluded.file,
            timestamp = excluded.timestamp,
            turns = excluded.turns,
            tool_calls = excluded.tool_calls,
            narration_only_turns = excluded.narration_only_turns,
            parallel_tool_calls = excluded.parallel_tool_calls,
            duration_secs = excluded.duration_secs,
            output_bytes = excluded.output_bytes,
            completion = excluded.completion,
            exit_code = excluded.exit_code,
            cost_usd = excluded.cost_usd,
            input_tokens = excluded.input_tokens,
            output_tokens = excluded.output_tokens",
        rusqlite::params![
            session_id,
            file,
            m.timestamp,
            m.turns,
            m.tool_calls,
            m.narration_only_turns,
            m.parallel_tool_calls,
            m.duration_secs,
            m.output_bytes,
            m.completion,
            m.exit_code,
            m.cost_usd,
            m.input_tokens,
            m.output_tokens,
        ],
    )?;
    Ok(())
}

// ── Session ID extraction from filename ──────────────────────────────────

/// Extract the session number from a filename like "42.jsonl" or "42.jsonl.zst".
fn session_id_from_path(path: &Path) -> Option<u64> {
    let stem = path.file_name()?.to_str()?;
    // Strip known extensions: .jsonl.zst or .jsonl
    let num_str = stem
        .strip_suffix(".jsonl.zst")
        .or_else(|| stem.strip_suffix(".jsonl"))?;
    num_str.parse().ok()
}

// ── Subcommand handlers ──────────────────────────────────────────────────

fn handle_log(conn: &Connection, file: &Path) -> Result<(), String> {
    let session_id = session_id_from_path(file).ok_or_else(|| {
        format!(
            "Cannot extract session ID from filename: {}",
            file.display()
        )
    })?;

    // Handle zstd-compressed files
    let metrics = if file.extension().and_then(|e| e.to_str()) == Some("zst") {
        parse_zst_session(file)?
    } else {
        parse_session_jsonl(file)?
    };

    upsert_session(conn, session_id, &file.display().to_string(), &metrics)
        .map_err(|e| format!("Database error: {e}"))?;

    println!(
        "Logged session {session_id}: {} turns, {:.1}s, {} tool calls, completion={}",
        metrics.turns, metrics.duration_secs, metrics.tool_calls, metrics.completion
    );
    Ok(())
}

fn handle_backfill(
    conn: &Connection,
    sessions_dir: &Path,
    from: Option<u64>,
    to: Option<u64>,
) -> Result<(), String> {
    if !sessions_dir.is_dir() {
        return Err(format!(
            "Sessions directory not found: {}",
            sessions_dir.display()
        ));
    }

    // Collect all .jsonl and .jsonl.zst files
    let mut entries: Vec<(u64, PathBuf)> = Vec::new();

    for entry in
        std::fs::read_dir(sessions_dir).map_err(|e| format!("Cannot read directory: {e}"))?
    {
        let entry = entry.map_err(|e| format!("Dir entry error: {e}"))?;
        let path = entry.path();

        if let Some(session_id) = session_id_from_path(&path) {
            if let Some(lo) = from {
                if session_id < lo {
                    continue;
                }
            }
            if let Some(hi) = to {
                if session_id > hi {
                    continue;
                }
            }
            entries.push((session_id, path));
        }
    }

    entries.sort_by_key(|(id, _)| *id);

    if entries.is_empty() {
        println!("No session files found in {}", sessions_dir.display());
        return Ok(());
    }

    let total = entries.len();
    let mut success = 0u64;
    let mut errors = 0u64;

    for (session_id, path) in &entries {
        let metrics_result = if path.extension().and_then(|e| e.to_str()) == Some("zst") {
            parse_zst_session(path)
        } else {
            parse_session_jsonl(path)
        };

        match metrics_result {
            Ok(metrics) => {
                match upsert_session(conn, *session_id, &path.display().to_string(), &metrics) {
                    Ok(()) => success += 1,
                    Err(e) => {
                        eprintln!("  DB error for session {session_id}: {e}");
                        errors += 1;
                    }
                }
            }
            Err(e) => {
                eprintln!("  Parse error for {}: {e}", path.display());
                errors += 1;
            }
        }
    }

    println!("Backfill complete: {success}/{total} sessions ingested ({errors} errors)");
    Ok(())
}

fn handle_seed(conn: &Connection, source_db_path: &Path) -> Result<(), String> {
    if !source_db_path.exists() {
        return Err(format!(
            "Source database not found: {}",
            source_db_path.display()
        ));
    }

    let source =
        Connection::open(source_db_path).map_err(|e| format!("Cannot open source DB: {e}"))?;

    // Check if the source has an improvements table
    let has_table: bool = source
        .query_row(
            "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name='improvements'",
            [],
            |row| row.get(0),
        )
        .unwrap_or(false);

    if !has_table {
        return Err("Source database has no improvements table".to_string());
    }

    let mut stmt = source
        .prepare(
            "SELECT ref, created, resolved, category, status, title, body, context, tags, meta
             FROM improvements ORDER BY id",
        )
        .map_err(|e| format!("Query error: {e}"))?;

    let mut count = 0u64;
    let mut skipped = 0u64;

    let rows = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, Option<String>>(6)?,
                row.get::<_, Option<String>>(7)?,
                row.get::<_, Option<String>>(8)?,
                row.get::<_, Option<String>>(9)?,
            ))
        })
        .map_err(|e| format!("Query error: {e}"))?;

    for row in rows {
        let (ref_id, created, resolved, category, status, title, body, context, tags, meta) =
            row.map_err(|e| format!("Row error: {e}"))?;

        let result = conn.execute(
            "INSERT OR IGNORE INTO improvements (ref, created, resolved, category, status, title, body, context, tags, meta)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            rusqlite::params![ref_id, created, resolved, category, status, title, body, context, tags, meta],
        );

        match result {
            Ok(n) if n > 0 => count += 1,
            Ok(_) => skipped += 1,
            Err(e) => {
                eprintln!("  Error inserting {ref_id}: {e}");
                skipped += 1;
            }
        }
    }

    println!("Seeded {count} improvements ({skipped} skipped/duplicates)");
    Ok(())
}

// ── Zstd decompression ───────────────────────────────────────────────────

fn parse_zst_session(path: &Path) -> Result<SessionMetrics, String> {
    let file =
        std::fs::File::open(path).map_err(|e| format!("Cannot open {}: {e}", path.display()))?;
    let decoder = zstd::Decoder::new(file).map_err(|e| format!("Zstd decode error: {e}"))?;
    let reader = std::io::BufReader::new(decoder);

    // Get output_bytes from compressed file size
    let file_size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);

    let file_mtime = std::fs::metadata(path)
        .and_then(|meta| meta.modified())
        .ok()
        .map(|t| {
            let dt: chrono::DateTime<chrono::Utc> = t.into();
            dt.format("%Y-%m-%dT%H:%M:%SZ").to_string()
        })
        .unwrap_or_else(|| chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string());

    let mut m = SessionMetrics {
        output_bytes: file_size,
        timestamp: file_mtime,
        ..Default::default()
    };

    for line_result in reader.lines() {
        let line = line_result.map_err(|e| format!("Read error: {e}"))?;
        if line.is_empty() {
            continue;
        }

        let v: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => continue,
        };

        match v.get("type").and_then(|t| t.as_str()) {
            Some("assistant") => count_assistant_turn(&v, &mut m),
            Some("result") => extract_result(&v, &mut m),
            _ => {}
        }
    }

    Ok(m)
}

// ── Main ─────────────────────────────────────────────────────────────────

fn main() {
    let cli = Cli::parse();

    // Ensure parent directory for DB file exists
    if let Some(parent) = cli.db.parent() {
        if !parent.as_os_str().is_empty() && !parent.exists() {
            std::fs::create_dir_all(parent).unwrap_or_else(|e| {
                eprintln!("Cannot create directory {}: {e}", parent.display());
                std::process::exit(1);
            });
        }
    }

    let conn = Connection::open(&cli.db).unwrap_or_else(|e| {
        eprintln!("Cannot open database {}: {e}", cli.db.display());
        std::process::exit(1);
    });

    ensure_schema(&conn).unwrap_or_else(|e| {
        eprintln!("Schema error: {e}");
        std::process::exit(1);
    });

    let result = match &cli.command {
        Commands::Log { file } => handle_log(&conn, file),
        Commands::Backfill {
            from,
            to,
            sessions_dir,
        } => {
            let dir = sessions_dir
                .clone()
                .unwrap_or_else(|| PathBuf::from(DEFAULT_SESSIONS_DIR));
            handle_backfill(&conn, &dir, *from, *to)
        }
        Commands::Seed { from } => {
            let source = from
                .clone()
                .unwrap_or_else(|| PathBuf::from(".blacksmith/blacksmith.db"));
            handle_seed(&conn, &source)
        }
    };

    if let Err(e) = result {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}

// ── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    fn write_jsonl(dir: &Path, name: &str, lines: &[&str]) -> PathBuf {
        let path = dir.join(name);
        let mut f = std::fs::File::create(&path).unwrap();
        for line in lines {
            writeln!(f, "{}", line).unwrap();
        }
        path
    }

    fn test_conn() -> (TempDir, Connection) {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let conn = Connection::open(&db_path).unwrap();
        ensure_schema(&conn).unwrap();
        (dir, conn)
    }

    // ── session_id_from_path ─────────────────────────────────────────

    #[test]
    fn session_id_from_jsonl() {
        assert_eq!(session_id_from_path(Path::new("100.jsonl")), Some(100));
    }

    #[test]
    fn session_id_from_zst() {
        assert_eq!(session_id_from_path(Path::new("42.jsonl.zst")), Some(42));
    }

    #[test]
    fn session_id_from_bad_name() {
        assert_eq!(session_id_from_path(Path::new("readme.txt")), None);
    }

    #[test]
    fn session_id_with_directory() {
        assert_eq!(
            session_id_from_path(Path::new("/some/path/7.jsonl")),
            Some(7)
        );
    }

    // ── parse_session_jsonl ──────────────────────────────────────────

    #[test]
    fn parse_empty_file() {
        let dir = TempDir::new().unwrap();
        let path = write_jsonl(dir.path(), "0.jsonl", &[]);
        let m = parse_session_jsonl(&path).unwrap();
        assert_eq!(m.turns, 0);
        assert_eq!(m.tool_calls, 0);
        assert!(!m.completion);
    }

    #[test]
    fn parse_minimal_session() {
        let dir = TempDir::new().unwrap();
        let lines = &[
            r#"{"type":"assistant","message":{"content":[{"type":"text","text":"hello"}]}}"#,
            r#"{"type":"result","subtype":"success","is_error":false,"duration_ms":10000,"num_turns":1,"total_cost_usd":0.5,"modelUsage":{"opus":{"inputTokens":100,"outputTokens":50,"cacheReadInputTokens":0,"cacheCreationInputTokens":0}}}"#,
        ];
        let path = write_jsonl(dir.path(), "1.jsonl", lines);
        let m = parse_session_jsonl(&path).unwrap();

        assert_eq!(m.turns, 1);
        assert_eq!(m.narration_only_turns, 1);
        assert_eq!(m.tool_calls, 0);
        assert_eq!(m.parallel_tool_calls, 0);
        assert!((m.duration_secs - 10.0).abs() < 0.01);
        assert!(m.completion);
        assert!((m.cost_usd.unwrap() - 0.5).abs() < 0.001);
        assert_eq!(m.input_tokens, Some(100));
        assert_eq!(m.output_tokens, Some(50));
    }

    #[test]
    fn parse_parallel_tools() {
        let dir = TempDir::new().unwrap();
        let lines = &[
            r#"{"type":"assistant","message":{"content":[{"type":"tool_use","name":"Read","input":{}},{"type":"tool_use","name":"Grep","input":{}}]}}"#,
            r#"{"type":"result","subtype":"success","is_error":false,"duration_ms":5000}"#,
        ];
        let path = write_jsonl(dir.path(), "2.jsonl", lines);
        let m = parse_session_jsonl(&path).unwrap();

        assert_eq!(m.turns, 1);
        assert_eq!(m.tool_calls, 2);
        assert_eq!(m.parallel_tool_calls, 1);
        assert_eq!(m.narration_only_turns, 0);
    }

    #[test]
    fn parse_error_result() {
        let dir = TempDir::new().unwrap();
        let lines = &[r#"{"type":"result","subtype":"error","is_error":true,"duration_ms":1000}"#];
        let path = write_jsonl(dir.path(), "3.jsonl", lines);
        let m = parse_session_jsonl(&path).unwrap();
        assert!(!m.completion);
    }

    #[test]
    fn parse_skips_malformed() {
        let dir = TempDir::new().unwrap();
        let lines = &[
            "not json",
            r#"{"type":"assistant","message":{"content":[{"type":"text","text":"ok"}]}}"#,
            "{broken",
        ];
        let path = write_jsonl(dir.path(), "4.jsonl", lines);
        let m = parse_session_jsonl(&path).unwrap();
        assert_eq!(m.turns, 1);
    }

    #[test]
    fn parse_multi_model_tokens() {
        let dir = TempDir::new().unwrap();
        let lines = &[
            r#"{"type":"result","subtype":"success","is_error":false,"duration_ms":100,"total_cost_usd":2.0,"modelUsage":{"opus":{"inputTokens":100,"outputTokens":200},"haiku":{"inputTokens":50,"outputTokens":30}}}"#,
        ];
        let path = write_jsonl(dir.path(), "5.jsonl", lines);
        let m = parse_session_jsonl(&path).unwrap();
        assert_eq!(m.input_tokens, Some(150));
        assert_eq!(m.output_tokens, Some(230));
    }

    // ── handle_log ───────────────────────────────────────────────────

    #[test]
    fn log_inserts_session() {
        let (_dir, conn) = test_conn();
        let tmp = TempDir::new().unwrap();
        let lines = &[
            r#"{"type":"assistant","message":{"content":[{"type":"text","text":"hi"}]}}"#,
            r#"{"type":"result","subtype":"success","is_error":false,"duration_ms":60000,"total_cost_usd":1.0,"modelUsage":{"opus":{"inputTokens":500,"outputTokens":200}}}"#,
        ];
        let path = write_jsonl(tmp.path(), "10.jsonl", lines);

        handle_log(&conn, &path).unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM sessions", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);

        let turns: i64 = conn
            .query_row(
                "SELECT turns FROM sessions WHERE session_id = 10",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(turns, 1);
    }

    #[test]
    fn log_upserts_on_conflict() {
        let (_dir, conn) = test_conn();
        let tmp = TempDir::new().unwrap();

        // First insert
        let lines1 = &[
            r#"{"type":"assistant","message":{"content":[{"type":"text","text":"v1"}]}}"#,
            r#"{"type":"result","subtype":"success","is_error":false,"duration_ms":1000}"#,
        ];
        let path = write_jsonl(tmp.path(), "20.jsonl", lines1);
        handle_log(&conn, &path).unwrap();

        // Re-log same session
        let lines2 = &[
            r#"{"type":"assistant","message":{"content":[{"type":"text","text":"v2"}]}}"#,
            r#"{"type":"assistant","message":{"content":[{"type":"text","text":"v2b"}]}}"#,
            r#"{"type":"result","subtype":"success","is_error":false,"duration_ms":2000}"#,
        ];
        let path2 = write_jsonl(tmp.path(), "20.jsonl", lines2);
        handle_log(&conn, &path2).unwrap();

        let turns: i64 = conn
            .query_row(
                "SELECT turns FROM sessions WHERE session_id = 20",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(turns, 2);
    }

    #[test]
    fn log_bad_filename() {
        let (_dir, conn) = test_conn();
        let tmp = TempDir::new().unwrap();
        let path = write_jsonl(tmp.path(), "readme.txt", &[]);
        let result = handle_log(&conn, &path);
        assert!(result.is_err());
    }

    // ── handle_backfill ──────────────────────────────────────────────

    #[test]
    fn backfill_empty_dir() {
        let (_dir, conn) = test_conn();
        let sessions = TempDir::new().unwrap();
        handle_backfill(&conn, sessions.path(), None, None).unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM sessions", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn backfill_multiple_sessions() {
        let (_dir, conn) = test_conn();
        let sessions = TempDir::new().unwrap();

        for i in 0..3 {
            let lines = &[
                r#"{"type":"assistant","message":{"content":[{"type":"text","text":"hi"}]}}"#,
                &format!(
                    r#"{{"type":"result","subtype":"success","is_error":false,"duration_ms":{}}}"#,
                    (i + 1) * 1000
                ),
            ];
            write_jsonl(sessions.path(), &format!("{i}.jsonl"), lines);
        }

        handle_backfill(&conn, sessions.path(), None, None).unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM sessions", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 3);
    }

    #[test]
    fn backfill_with_range() {
        let (_dir, conn) = test_conn();
        let sessions = TempDir::new().unwrap();

        for i in 0..10 {
            write_jsonl(
                sessions.path(),
                &format!("{i}.jsonl"),
                &[r#"{"type":"result","subtype":"success","is_error":false,"duration_ms":1000}"#],
            );
        }

        handle_backfill(&conn, sessions.path(), Some(3), Some(7)).unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM sessions", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 5); // sessions 3,4,5,6,7
    }

    #[test]
    fn backfill_nonexistent_dir() {
        let (_dir, conn) = test_conn();
        let result = handle_backfill(&conn, Path::new("/nonexistent/dir"), None, None);
        assert!(result.is_err());
    }

    // ── handle_seed ──────────────────────────────────────────────────

    #[test]
    fn seed_from_source_db() {
        let (_dir, conn) = test_conn();
        let source_dir = TempDir::new().unwrap();
        let source_path = source_dir.path().join("source.db");
        let source = Connection::open(&source_path).unwrap();
        source
            .execute_batch(
                "CREATE TABLE improvements (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ref TEXT UNIQUE,
                    created TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
                    resolved TEXT,
                    category TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'open',
                    title TEXT NOT NULL,
                    body TEXT,
                    context TEXT,
                    tags TEXT,
                    meta TEXT
                );
                INSERT INTO improvements (ref, category, title) VALUES ('R1', 'workflow', 'Test 1');
                INSERT INTO improvements (ref, category, title, body) VALUES ('R2', 'cost', 'Test 2', 'details');",
            )
            .unwrap();
        drop(source);

        handle_seed(&conn, &source_path).unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM improvements", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn seed_skips_duplicates() {
        let (_dir, conn) = test_conn();
        let source_dir = TempDir::new().unwrap();
        let source_path = source_dir.path().join("source.db");
        let source = Connection::open(&source_path).unwrap();
        source
            .execute_batch(
                "CREATE TABLE improvements (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ref TEXT UNIQUE,
                    created TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
                    resolved TEXT,
                    category TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'open',
                    title TEXT NOT NULL,
                    body TEXT, context TEXT, tags TEXT, meta TEXT
                );
                INSERT INTO improvements (ref, category, title) VALUES ('R1', 'workflow', 'Test');",
            )
            .unwrap();
        drop(source);

        // Seed twice
        handle_seed(&conn, &source_path).unwrap();
        handle_seed(&conn, &source_path).unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM improvements", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn seed_nonexistent_source() {
        let (_dir, conn) = test_conn();
        let result = handle_seed(&conn, Path::new("/nonexistent/db"));
        assert!(result.is_err());
    }

    #[test]
    fn seed_no_improvements_table() {
        let (_dir, conn) = test_conn();
        let source_dir = TempDir::new().unwrap();
        let source_path = source_dir.path().join("empty.db");
        let source = Connection::open(&source_path).unwrap();
        source
            .execute_batch("CREATE TABLE other (id INTEGER);")
            .unwrap();
        drop(source);

        let result = handle_seed(&conn, &source_path);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("no improvements table"));
    }

    // ── Schema ───────────────────────────────────────────────────────

    #[test]
    fn schema_is_idempotent() {
        let (_dir, conn) = test_conn();
        // Call ensure_schema again — should not error
        ensure_schema(&conn).unwrap();
    }
}
