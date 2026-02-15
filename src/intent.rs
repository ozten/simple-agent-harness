use rusqlite::{params, Connection, Result};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::process::Command;

/// A single concept identified by intent analysis, with reasoning.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TargetArea {
    pub concept: String,
    pub reasoning: String,
}

/// The result of LLM-based intent analysis for a task.
///
/// This is the stable, expensive layer (Layer 1) that only invalidates
/// when issue content changes. Concepts are abstract (e.g. "auth_endpoints")
/// rather than concrete file paths, so they survive refactors.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntentAnalysis {
    pub task_id: String,
    pub content_hash: String,
    pub target_areas: Vec<TargetArea>,
}

/// Compute a content hash from the issue title, description, and acceptance criteria.
///
/// Uses a simple deterministic hash. The same inputs always produce the same hash,
/// so analysis is only re-run when the issue content actually changes.
pub fn content_hash(title: &str, description: &str, acceptance_criteria: &str) -> String {
    let mut hasher = DefaultHasher::new();
    title.hash(&mut hasher);
    description.hash(&mut hasher);
    acceptance_criteria.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// Create the intent_analyses table if it doesn't exist.
pub fn create_table(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS intent_analyses (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id       TEXT NOT NULL,
            content_hash  TEXT NOT NULL,
            target_areas  TEXT NOT NULL,
            created_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
            UNIQUE(task_id, content_hash)
        );

        CREATE INDEX IF NOT EXISTS idx_intent_analyses_content_hash
            ON intent_analyses(content_hash);
        CREATE INDEX IF NOT EXISTS idx_intent_analyses_task_id
            ON intent_analyses(task_id);",
    )
}

/// Look up a cached intent analysis by content_hash.
///
/// Returns the most recent analysis matching this hash, if any.
/// Since the hash covers title+description+acceptance criteria,
/// a cache hit means the issue content hasn't changed.
pub fn get_by_content_hash(
    conn: &Connection,
    content_hash: &str,
) -> Result<Option<IntentAnalysis>> {
    let mut stmt = conn.prepare(
        "SELECT task_id, content_hash, target_areas
         FROM intent_analyses
         WHERE content_hash = ?1
         ORDER BY id DESC
         LIMIT 1",
    )?;

    let mut rows = stmt.query_map(params![content_hash], |row| {
        let task_id: String = row.get(0)?;
        let hash: String = row.get(1)?;
        let areas_json: String = row.get(2)?;
        Ok((task_id, hash, areas_json))
    })?;

    match rows.next() {
        Some(Ok((task_id, hash, areas_json))) => {
            let target_areas: Vec<TargetArea> =
                serde_json::from_str(&areas_json).unwrap_or_default();
            Ok(Some(IntentAnalysis {
                task_id,
                content_hash: hash,
                target_areas,
            }))
        }
        Some(Err(e)) => Err(e),
        None => Ok(None),
    }
}

/// Look up a cached intent analysis by task_id.
///
/// Returns the most recent analysis for this task, regardless of content hash.
pub fn get_by_task_id(conn: &Connection, task_id: &str) -> Result<Option<IntentAnalysis>> {
    let mut stmt = conn.prepare(
        "SELECT task_id, content_hash, target_areas
         FROM intent_analyses
         WHERE task_id = ?1
         ORDER BY id DESC
         LIMIT 1",
    )?;

    let mut rows = stmt.query_map(params![task_id], |row| {
        let tid: String = row.get(0)?;
        let hash: String = row.get(1)?;
        let areas_json: String = row.get(2)?;
        Ok((tid, hash, areas_json))
    })?;

    match rows.next() {
        Some(Ok((tid, hash, areas_json))) => {
            let target_areas: Vec<TargetArea> =
                serde_json::from_str(&areas_json).unwrap_or_default();
            Ok(Some(IntentAnalysis {
                task_id: tid,
                content_hash: hash,
                target_areas,
            }))
        }
        Some(Err(e)) => Err(e),
        None => Ok(None),
    }
}

/// Store an intent analysis result, replacing any existing entry for the same
/// (task_id, content_hash) pair.
pub fn store(conn: &Connection, analysis: &IntentAnalysis) -> Result<()> {
    let areas_json =
        serde_json::to_string(&analysis.target_areas).unwrap_or_else(|_| "[]".to_string());

    conn.execute(
        "INSERT OR REPLACE INTO intent_analyses (task_id, content_hash, target_areas)
         VALUES (?1, ?2, ?3)",
        params![analysis.task_id, analysis.content_hash, areas_json],
    )?;
    Ok(())
}

/// Errors that can occur during intent analysis.
#[derive(Debug)]
pub enum AnalysisError {
    /// Database error during cache lookup or storage.
    Db(rusqlite::Error),
    /// LLM subprocess failed to spawn or exited with an error.
    LlmFailed(String),
    /// LLM output could not be parsed as valid JSON target areas.
    ParseFailed(String),
}

impl std::fmt::Display for AnalysisError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AnalysisError::Db(e) => write!(f, "database error: {e}"),
            AnalysisError::LlmFailed(msg) => write!(f, "LLM call failed: {msg}"),
            AnalysisError::ParseFailed(msg) => write!(f, "failed to parse LLM output: {msg}"),
        }
    }
}

impl std::error::Error for AnalysisError {}

impl From<rusqlite::Error> for AnalysisError {
    fn from(e: rusqlite::Error) -> Self {
        AnalysisError::Db(e)
    }
}

/// Build the prompt sent to the LLM for intent analysis.
fn build_prompt(title: &str, description: &str, acceptance_criteria: &str) -> String {
    format!(
        r#"You are analyzing a software task to identify which semantic areas of a codebase it touches.

Task title: {title}
Task description: {description}
Acceptance criteria: {acceptance_criteria}

Identify the abstract concept areas this task involves. Use short snake_case names like "auth_endpoints", "middleware_stack", "config", "database_schema", "api_routes", "error_handling", etc. These are semantic concepts, NOT file paths.

Respond with ONLY a JSON array of objects, each with "concept" and "reasoning" fields. Example:
[
  {{"concept": "auth_endpoints", "reasoning": "Task modifies authentication API surface"}},
  {{"concept": "config", "reasoning": "New settings need to be configurable"}}
]

Respond with ONLY the JSON array. No markdown, no explanation, no code fences."#
    )
}

/// Extract the JSON array from LLM output, handling common formatting issues
/// like markdown code fences or extra text around the JSON.
fn extract_json_array(raw: &str) -> std::result::Result<Vec<TargetArea>, String> {
    let trimmed = raw.trim();

    // Try parsing directly first
    if let Ok(areas) = serde_json::from_str::<Vec<TargetArea>>(trimmed) {
        return Ok(areas);
    }

    // Strip markdown code fences if present
    let stripped = if trimmed.contains("```") {
        let mut in_fence = false;
        let mut json_lines = Vec::new();
        for line in trimmed.lines() {
            let lt = line.trim();
            if lt.starts_with("```") {
                in_fence = !in_fence;
                continue;
            }
            if in_fence {
                json_lines.push(line);
            }
        }
        json_lines.join("\n")
    } else {
        trimmed.to_string()
    };

    // Try parsing the stripped content
    if let Ok(areas) = serde_json::from_str::<Vec<TargetArea>>(stripped.trim()) {
        return Ok(areas);
    }

    // Try to find a JSON array substring via bracket matching
    if let Some(start) = stripped.find('[') {
        if let Some(end) = stripped.rfind(']') {
            let slice = &stripped[start..=end];
            if let Ok(areas) = serde_json::from_str::<Vec<TargetArea>>(slice) {
                return Ok(areas);
            }
        }
    }

    Err(format!(
        "could not parse as JSON array of {{concept, reasoning}}: {}",
        &trimmed[..trimmed.len().min(200)]
    ))
}

/// Run LLM-based intent analysis for a task, with caching.
///
/// Checks the cache first using the content hash of title+description+acceptance_criteria.
/// If a cached result exists with the same hash, returns it immediately.
/// Otherwise, calls the LLM via the specified command, parses the response,
/// stores it in the cache, and returns it.
///
/// `llm_command` is the command to invoke (e.g. "claude"). The prompt is passed
/// via `-p` flag. If the command is empty, analysis is skipped and an empty
/// target_areas list is returned.
pub fn analyze(
    conn: &Connection,
    task_id: &str,
    title: &str,
    description: &str,
    acceptance_criteria: &str,
    llm_command: &str,
) -> std::result::Result<IntentAnalysis, AnalysisError> {
    let hash = content_hash(title, description, acceptance_criteria);

    // Check cache
    if let Some(cached) = get_by_content_hash(conn, &hash)? {
        tracing::debug!(task_id, content_hash = %hash, "intent analysis cache hit");
        return Ok(cached);
    }

    // Skip LLM call if no command configured
    if llm_command.is_empty() {
        tracing::warn!(
            task_id,
            "no LLM command configured, returning empty intent analysis"
        );
        let analysis = IntentAnalysis {
            task_id: task_id.to_string(),
            content_hash: hash,
            target_areas: vec![],
        };
        store(conn, &analysis)?;
        return Ok(analysis);
    }

    let prompt = build_prompt(title, description, acceptance_criteria);

    tracing::info!(task_id, llm_command, "running LLM intent analysis");

    let output = Command::new(llm_command)
        .args(["-p", &prompt, "--output-format", "text"])
        .output()
        .map_err(|e| AnalysisError::LlmFailed(format!("failed to spawn '{llm_command}': {e}")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(AnalysisError::LlmFailed(format!(
            "'{llm_command}' exited with {}: {}",
            output.status,
            stderr.trim()
        )));
    }

    let raw_output = String::from_utf8_lossy(&output.stdout);
    let target_areas = extract_json_array(&raw_output).map_err(AnalysisError::ParseFailed)?;

    tracing::info!(
        task_id,
        areas = target_areas.len(),
        "intent analysis complete"
    );

    let analysis = IntentAnalysis {
        task_id: task_id.to_string(),
        content_hash: hash,
        target_areas,
    };

    store(conn, &analysis)?;
    Ok(analysis)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        create_table(&conn).unwrap();
        conn
    }

    #[test]
    fn test_content_hash_deterministic() {
        let h1 = content_hash("title", "desc", "criteria");
        let h2 = content_hash("title", "desc", "criteria");
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_content_hash_changes_with_input() {
        let h1 = content_hash("title", "desc", "criteria");
        let h2 = content_hash("title", "desc", "different criteria");
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_content_hash_format() {
        let h = content_hash("a", "b", "c");
        assert_eq!(h.len(), 16);
        assert!(h.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_store_and_retrieve_by_content_hash() {
        let conn = setup_db();
        let analysis = IntentAnalysis {
            task_id: "task-1".to_string(),
            content_hash: "abc123".to_string(),
            target_areas: vec![
                TargetArea {
                    concept: "auth".to_string(),
                    reasoning: "handles login".to_string(),
                },
                TargetArea {
                    concept: "config".to_string(),
                    reasoning: "rate limits configurable".to_string(),
                },
            ],
        };

        store(&conn, &analysis).unwrap();

        let retrieved = get_by_content_hash(&conn, "abc123").unwrap().unwrap();
        assert_eq!(retrieved.task_id, "task-1");
        assert_eq!(retrieved.content_hash, "abc123");
        assert_eq!(retrieved.target_areas.len(), 2);
        assert_eq!(retrieved.target_areas[0].concept, "auth");
        assert_eq!(retrieved.target_areas[1].concept, "config");
    }

    #[test]
    fn test_retrieve_by_task_id() {
        let conn = setup_db();
        let analysis = IntentAnalysis {
            task_id: "task-42".to_string(),
            content_hash: "hash1".to_string(),
            target_areas: vec![TargetArea {
                concept: "middleware".to_string(),
                reasoning: "rate limiting".to_string(),
            }],
        };

        store(&conn, &analysis).unwrap();

        let retrieved = get_by_task_id(&conn, "task-42").unwrap().unwrap();
        assert_eq!(retrieved.task_id, "task-42");
        assert_eq!(retrieved.target_areas.len(), 1);
    }

    #[test]
    fn test_cache_miss_returns_none() {
        let conn = setup_db();
        assert!(get_by_content_hash(&conn, "nonexistent").unwrap().is_none());
        assert!(get_by_task_id(&conn, "nonexistent").unwrap().is_none());
    }

    #[test]
    fn test_upsert_replaces_on_same_task_and_hash() {
        let conn = setup_db();

        let v1 = IntentAnalysis {
            task_id: "task-1".to_string(),
            content_hash: "hash1".to_string(),
            target_areas: vec![TargetArea {
                concept: "old".to_string(),
                reasoning: "old reason".to_string(),
            }],
        };
        store(&conn, &v1).unwrap();

        let v2 = IntentAnalysis {
            task_id: "task-1".to_string(),
            content_hash: "hash1".to_string(),
            target_areas: vec![TargetArea {
                concept: "new".to_string(),
                reasoning: "new reason".to_string(),
            }],
        };
        store(&conn, &v2).unwrap();

        let retrieved = get_by_content_hash(&conn, "hash1").unwrap().unwrap();
        assert_eq!(retrieved.target_areas[0].concept, "new");
    }

    #[test]
    fn test_new_hash_creates_new_entry() {
        let conn = setup_db();

        let v1 = IntentAnalysis {
            task_id: "task-1".to_string(),
            content_hash: "hash1".to_string(),
            target_areas: vec![TargetArea {
                concept: "v1".to_string(),
                reasoning: "first".to_string(),
            }],
        };
        store(&conn, &v1).unwrap();

        let v2 = IntentAnalysis {
            task_id: "task-1".to_string(),
            content_hash: "hash2".to_string(),
            target_areas: vec![TargetArea {
                concept: "v2".to_string(),
                reasoning: "second".to_string(),
            }],
        };
        store(&conn, &v2).unwrap();

        // Both entries exist
        let r1 = get_by_content_hash(&conn, "hash1").unwrap().unwrap();
        assert_eq!(r1.target_areas[0].concept, "v1");

        let r2 = get_by_content_hash(&conn, "hash2").unwrap().unwrap();
        assert_eq!(r2.target_areas[0].concept, "v2");

        // get_by_task_id returns the latest (hash2)
        let latest = get_by_task_id(&conn, "task-1").unwrap().unwrap();
        assert_eq!(latest.content_hash, "hash2");
    }

    #[test]
    fn test_empty_target_areas() {
        let conn = setup_db();
        let analysis = IntentAnalysis {
            task_id: "task-empty".to_string(),
            content_hash: "emptyhash".to_string(),
            target_areas: vec![],
        };

        store(&conn, &analysis).unwrap();

        let retrieved = get_by_content_hash(&conn, "emptyhash").unwrap().unwrap();
        assert!(retrieved.target_areas.is_empty());
    }

    #[test]
    fn test_build_prompt_contains_task_fields() {
        let prompt = build_prompt("Add auth", "Implement OAuth flow", "Users can log in");
        assert!(prompt.contains("Add auth"));
        assert!(prompt.contains("Implement OAuth flow"));
        assert!(prompt.contains("Users can log in"));
        assert!(prompt.contains("JSON array"));
    }

    #[test]
    fn test_extract_json_array_valid() {
        let input = r#"[{"concept": "auth", "reasoning": "handles login"}]"#;
        let areas = extract_json_array(input).unwrap();
        assert_eq!(areas.len(), 1);
        assert_eq!(areas[0].concept, "auth");
        assert_eq!(areas[0].reasoning, "handles login");
    }

    #[test]
    fn test_extract_json_array_with_code_fences() {
        let input = r#"```json
[{"concept": "db", "reasoning": "schema changes"}]
```"#;
        let areas = extract_json_array(input).unwrap();
        assert_eq!(areas.len(), 1);
        assert_eq!(areas[0].concept, "db");
    }

    #[test]
    fn test_extract_json_array_with_surrounding_text() {
        let input = r#"Here is the analysis:
[{"concept": "api", "reasoning": "new endpoint"}]
Hope this helps!"#;
        let areas = extract_json_array(input).unwrap();
        assert_eq!(areas.len(), 1);
        assert_eq!(areas[0].concept, "api");
    }

    #[test]
    fn test_extract_json_array_multiple_items() {
        let input = r#"[
            {"concept": "auth", "reasoning": "login flow"},
            {"concept": "config", "reasoning": "new settings"},
            {"concept": "middleware", "reasoning": "rate limiting"}
        ]"#;
        let areas = extract_json_array(input).unwrap();
        assert_eq!(areas.len(), 3);
    }

    #[test]
    fn test_extract_json_array_invalid() {
        let input = "This is not JSON at all";
        let result = extract_json_array(input);
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_json_array_empty_array() {
        let input = "[]";
        let areas = extract_json_array(input).unwrap();
        assert!(areas.is_empty());
    }

    #[test]
    fn test_analyze_cache_hit() {
        let conn = setup_db();
        // Pre-populate cache
        let hash = content_hash("title", "desc", "criteria");
        let analysis = IntentAnalysis {
            task_id: "task-1".to_string(),
            content_hash: hash.clone(),
            target_areas: vec![TargetArea {
                concept: "cached".to_string(),
                reasoning: "from cache".to_string(),
            }],
        };
        store(&conn, &analysis).unwrap();

        // analyze should return cached result without calling LLM
        let result = analyze(
            &conn,
            "task-1",
            "title",
            "desc",
            "criteria",
            "nonexistent-cmd",
        )
        .unwrap();
        assert_eq!(result.content_hash, hash);
        assert_eq!(result.target_areas[0].concept, "cached");
    }

    #[test]
    fn test_analyze_empty_command_returns_empty() {
        let conn = setup_db();
        let result = analyze(&conn, "task-1", "title", "desc", "criteria", "").unwrap();
        assert!(result.target_areas.is_empty());
        // Should also be stored in cache
        let cached = get_by_content_hash(&conn, &result.content_hash)
            .unwrap()
            .unwrap();
        assert!(cached.target_areas.is_empty());
    }

    #[test]
    fn test_analyze_with_echo_command() {
        let conn = setup_db();
        // Use sh -c to echo valid JSON — we pass it via the command mechanism
        // Since analyze uses Command::new(llm_command).args(["-p", ...]),
        // we use "sh" won't work directly. Instead test with a script.
        let dir = tempfile::tempdir().unwrap();
        let script = dir.path().join("mock-llm.sh");
        std::fs::write(
            &script,
            r#"#!/bin/sh
echo '[{"concept": "test_area", "reasoning": "from mock"}]'
"#,
        )
        .unwrap();
        std::fs::set_permissions(&script, std::os::unix::fs::PermissionsExt::from_mode(0o755))
            .unwrap();

        let result = analyze(
            &conn,
            "task-mock",
            "title",
            "desc",
            "criteria",
            script.to_str().unwrap(),
        )
        .unwrap();
        assert_eq!(result.target_areas.len(), 1);
        assert_eq!(result.target_areas[0].concept, "test_area");

        // Verify it was cached
        let cached = get_by_content_hash(&conn, &result.content_hash)
            .unwrap()
            .unwrap();
        assert_eq!(cached.target_areas[0].concept, "test_area");
    }

    #[test]
    fn test_analyze_llm_spawn_failure() {
        let conn = setup_db();
        let result = analyze(
            &conn,
            "task-fail",
            "title",
            "desc",
            "criteria",
            "nonexistent-binary-xyz-12345",
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, AnalysisError::LlmFailed(_)));
    }

    #[test]
    fn test_analyze_llm_bad_exit_code() {
        let conn = setup_db();
        let dir = tempfile::tempdir().unwrap();
        let script = dir.path().join("fail-llm.sh");
        std::fs::write(&script, "#!/bin/sh\necho 'error' >&2\nexit 1\n").unwrap();
        std::fs::set_permissions(&script, std::os::unix::fs::PermissionsExt::from_mode(0o755))
            .unwrap();

        let result = analyze(
            &conn,
            "task-fail",
            "title",
            "desc",
            "criteria",
            script.to_str().unwrap(),
        );
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), AnalysisError::LlmFailed(_)));
    }

    #[test]
    fn test_analyze_llm_invalid_json() {
        let conn = setup_db();
        let dir = tempfile::tempdir().unwrap();
        let script = dir.path().join("bad-json-llm.sh");
        std::fs::write(&script, "#!/bin/sh\necho 'not json at all'\n").unwrap();
        std::fs::set_permissions(&script, std::os::unix::fs::PermissionsExt::from_mode(0o755))
            .unwrap();

        let result = analyze(
            &conn,
            "task-bad",
            "title",
            "desc",
            "criteria",
            script.to_str().unwrap(),
        );
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), AnalysisError::ParseFailed(_)));
    }
}
