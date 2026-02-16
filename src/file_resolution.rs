//! Layer 2: File Resolution data model and cache.
//!
//! Maps abstract concepts from intent analysis (Layer 1) onto concrete files
//! and modules at a specific commit. Invalidates every time main advances
//! (keyed by base_commit).

use rusqlite::{params, Connection, Result};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use crate::import_graph;
use crate::intent::TargetArea;
use crate::module_detect;

/// A single concept-to-files mapping: which files and modules correspond to a concept.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FileResolutionMapping {
    pub concept: String,
    pub resolved_files: Vec<String>,
    pub resolved_modules: Vec<String>,
}

/// Derived analysis fields computed from the mappings and import graph.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct DerivedFields {
    /// All modules directly touched by this task's concepts.
    pub affected_modules: Vec<String>,
    /// Transitive dependents — modules that import from affected_modules.
    pub blast_radius: Vec<String>,
    /// Public API signatures at module boundaries that this task may affect.
    pub boundary_signatures: Vec<String>,
}

/// The result of file resolution for a task at a specific commit.
///
/// This is Layer 2 — volatile, cheap to regenerate via static analysis.
/// Invalidates whenever main advances (base_commit changes).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileResolution {
    pub task_id: String,
    pub base_commit: String,
    pub intent_hash: String,
    pub mappings: Vec<FileResolutionMapping>,
    pub derived: DerivedFields,
}

/// Create the file_resolutions table if it doesn't exist.
pub fn create_table(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS file_resolutions (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id      TEXT NOT NULL,
            base_commit  TEXT NOT NULL,
            intent_hash  TEXT NOT NULL,
            mappings     TEXT NOT NULL,
            derived      TEXT NOT NULL,
            created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
            UNIQUE(task_id, base_commit, intent_hash)
        );

        CREATE INDEX IF NOT EXISTS idx_file_resolutions_task_commit
            ON file_resolutions(task_id, base_commit);
        CREATE INDEX IF NOT EXISTS idx_file_resolutions_commit
            ON file_resolutions(base_commit);",
    )
}

/// Look up cached file resolution for a task at a specific commit.
///
/// Returns the resolution if the cache has a valid entry for this
/// (task_id, base_commit, intent_hash) triple. A cache hit means
/// the resolution is still valid — same commit, same intent analysis.
pub fn get(
    conn: &Connection,
    task_id: &str,
    base_commit: &str,
    intent_hash: &str,
) -> Result<Option<FileResolution>> {
    let mut stmt = conn.prepare(
        "SELECT task_id, base_commit, intent_hash, mappings, derived
         FROM file_resolutions
         WHERE task_id = ?1 AND base_commit = ?2 AND intent_hash = ?3
         ORDER BY id DESC
         LIMIT 1",
    )?;

    let mut rows = stmt.query_map(params![task_id, base_commit, intent_hash], |row| {
        let task_id: String = row.get(0)?;
        let base_commit: String = row.get(1)?;
        let intent_hash: String = row.get(2)?;
        let mappings_json: String = row.get(3)?;
        let derived_json: String = row.get(4)?;
        Ok((
            task_id,
            base_commit,
            intent_hash,
            mappings_json,
            derived_json,
        ))
    })?;

    match rows.next() {
        Some(Ok((task_id, base_commit, intent_hash, mappings_json, derived_json))) => {
            let mappings: Vec<FileResolutionMapping> =
                serde_json::from_str(&mappings_json).unwrap_or_default();
            let derived: DerivedFields = serde_json::from_str(&derived_json).unwrap_or_default();
            Ok(Some(FileResolution {
                task_id,
                base_commit,
                intent_hash,
                mappings,
                derived,
            }))
        }
        Some(Err(e)) => Err(e),
        None => Ok(None),
    }
}

/// Look up the most recent file resolution for a task (any commit).
///
/// Useful for checking whether a task has ever been resolved, even if
/// the cached entry is stale (base_commit doesn't match current HEAD).
pub fn get_latest_for_task(conn: &Connection, task_id: &str) -> Result<Option<FileResolution>> {
    let mut stmt = conn.prepare(
        "SELECT task_id, base_commit, intent_hash, mappings, derived
         FROM file_resolutions
         WHERE task_id = ?1
         ORDER BY id DESC
         LIMIT 1",
    )?;

    let mut rows = stmt.query_map(params![task_id], |row| {
        let task_id: String = row.get(0)?;
        let base_commit: String = row.get(1)?;
        let intent_hash: String = row.get(2)?;
        let mappings_json: String = row.get(3)?;
        let derived_json: String = row.get(4)?;
        Ok((
            task_id,
            base_commit,
            intent_hash,
            mappings_json,
            derived_json,
        ))
    })?;

    match rows.next() {
        Some(Ok((task_id, base_commit, intent_hash, mappings_json, derived_json))) => {
            let mappings: Vec<FileResolutionMapping> =
                serde_json::from_str(&mappings_json).unwrap_or_default();
            let derived: DerivedFields = serde_json::from_str(&derived_json).unwrap_or_default();
            Ok(Some(FileResolution {
                task_id,
                base_commit,
                intent_hash,
                mappings,
                derived,
            }))
        }
        Some(Err(e)) => Err(e),
        None => Ok(None),
    }
}

/// Store a file resolution result, replacing any existing entry for the
/// same (task_id, base_commit, intent_hash) triple.
pub fn store(conn: &Connection, resolution: &FileResolution) -> Result<()> {
    let mappings_json =
        serde_json::to_string(&resolution.mappings).unwrap_or_else(|_| "[]".to_string());
    let derived_json =
        serde_json::to_string(&resolution.derived).unwrap_or_else(|_| "{}".to_string());

    conn.execute(
        "INSERT OR REPLACE INTO file_resolutions (task_id, base_commit, intent_hash, mappings, derived)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            resolution.task_id,
            resolution.base_commit,
            resolution.intent_hash,
            mappings_json,
            derived_json,
        ],
    )?;
    Ok(())
}

/// Invalidate all cached file resolutions that were computed against
/// a different commit than `current_commit`.
///
/// This is called when main advances. Rather than eagerly regenerating,
/// we just delete stale entries — regeneration happens lazily when the
/// scheduler next needs the data.
pub fn invalidate_stale(conn: &Connection, current_commit: &str) -> Result<usize> {
    let count = conn.execute(
        "DELETE FROM file_resolutions WHERE base_commit != ?1",
        params![current_commit],
    )?;
    Ok(count)
}

/// Check whether a cached resolution exists and is fresh (matches current commit).
pub fn is_fresh(
    conn: &Connection,
    task_id: &str,
    current_commit: &str,
    intent_hash: &str,
) -> Result<bool> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM file_resolutions
         WHERE task_id = ?1 AND base_commit = ?2 AND intent_hash = ?3",
        params![task_id, current_commit, intent_hash],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

/// Resolve intent concepts to concrete files and modules using static analysis.
///
/// For each `TargetArea` concept, scans file names and module names for matches.
/// A concept like `"auth_endpoints"` matches files/modules containing `"auth"` or
/// `"endpoint"` in their name. Concepts are split on `_` into keywords.
///
/// After resolving, computes:
/// - `affected_modules`: all modules touched by any concept
/// - `blast_radius`: transitive dependents (files that import from affected files)
pub fn resolve(
    repo_root: &Path,
    task_id: &str,
    base_commit: &str,
    intent_hash: &str,
    target_areas: &[TargetArea],
) -> FileResolution {
    let import_graph = import_graph::build_import_graph(repo_root);
    let modules = module_detect::detect_modules_from_repo(repo_root);
    let src_root = repo_root.join("src");

    let mut mappings = Vec::new();
    let mut all_affected_modules: HashSet<String> = HashSet::new();
    let mut all_resolved_files: HashSet<PathBuf> = HashSet::new();

    for area in target_areas {
        let keywords = concept_to_keywords(&area.concept);
        let mut resolved_files: Vec<String> = Vec::new();
        let mut resolved_modules: Vec<String> = Vec::new();

        // Match against module names
        for (mod_name, module) in &modules {
            if keywords_match_name(&keywords, mod_name) {
                resolved_modules.push(mod_name.clone());
                all_affected_modules.insert(mod_name.clone());
                for file in &module.files {
                    let rel = file_relative_path(&src_root, file);
                    if !resolved_files.contains(&rel) {
                        resolved_files.push(rel);
                    }
                    all_resolved_files.insert(file.clone());
                }
            }
        }

        // Match against individual file names (stem without extension)
        for file in import_graph.keys() {
            let stem = file.file_stem().and_then(|s| s.to_str()).unwrap_or("");
            if stem == "mod" || stem == "main" || stem == "lib" {
                continue; // Skip generic entry points, they match too broadly
            }
            if keywords_match_name(&keywords, stem) {
                let rel = file_relative_path(&src_root, file);
                if !resolved_files.contains(&rel) {
                    resolved_files.push(rel);
                    all_resolved_files.insert(file.clone());
                }
                // Also mark the containing module
                if let Some(parent) = file.parent() {
                    let mod_name = if parent == src_root {
                        "crate".to_string()
                    } else {
                        module_detect_name(&src_root, parent)
                    };
                    if !resolved_modules.contains(&mod_name) {
                        resolved_modules.push(mod_name.clone());
                    }
                    all_affected_modules.insert(mod_name);
                }
            }
        }

        resolved_files.sort();
        resolved_modules.sort();

        mappings.push(FileResolutionMapping {
            concept: area.concept.clone(),
            resolved_files,
            resolved_modules,
        });
    }

    // Compute blast radius: find all files that transitively depend on resolved files
    let blast_radius_files = compute_blast_radius(&import_graph, &all_resolved_files);

    // Convert blast radius files to module names
    let mut blast_modules: HashSet<String> = HashSet::new();
    for file in &blast_radius_files {
        if let Some(parent) = file.parent() {
            let mod_name = if parent == src_root {
                "crate".to_string()
            } else {
                module_detect_name(&src_root, parent)
            };
            blast_modules.insert(mod_name);
        }
    }
    // Include the affected modules themselves
    for m in &all_affected_modules {
        blast_modules.insert(m.clone());
    }
    let mut blast_radius: Vec<String> = blast_modules.into_iter().collect();
    blast_radius.sort();

    let mut affected_modules: Vec<String> = all_affected_modules.into_iter().collect();
    affected_modules.sort();

    FileResolution {
        task_id: task_id.to_string(),
        base_commit: base_commit.to_string(),
        intent_hash: intent_hash.to_string(),
        mappings,
        derived: DerivedFields {
            affected_modules,
            blast_radius,
            boundary_signatures: Vec::new(), // Populated by the public API extractor (separate bead)
        },
    }
}

/// Split a concept name like `"auth_endpoints"` into keywords `["auth", "endpoints"]`.
fn concept_to_keywords(concept: &str) -> Vec<String> {
    concept
        .split('_')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_lowercase())
        .collect()
}

/// Check if any keyword matches within a name (case-insensitive substring match).
fn keywords_match_name(keywords: &[String], name: &str) -> bool {
    let lower = name.to_lowercase();
    keywords.iter().any(|kw| lower.contains(kw.as_str()))
}

/// Compute the relative path of a file from the src root (e.g., `"src/auth/handlers.rs"`).
fn file_relative_path(src_root: &Path, file: &Path) -> String {
    // Return path relative to src_root's parent (the repo root)
    if let Some(repo_root) = src_root.parent() {
        file.strip_prefix(repo_root)
            .unwrap_or(file)
            .to_string_lossy()
            .to_string()
    } else {
        file.to_string_lossy().to_string()
    }
}

/// Derive a module name from a directory path relative to src_root.
fn module_detect_name(src_root: &Path, dir: &Path) -> String {
    match dir.strip_prefix(src_root) {
        Ok(rel) => rel
            .components()
            .map(|c| c.as_os_str().to_string_lossy().to_string())
            .collect::<Vec<_>>()
            .join("::"),
        Err(_) => dir
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| "unknown".to_string()),
    }
}

/// Compute the transitive dependents (blast radius) of a set of files.
///
/// Returns all files that directly or transitively import from any file in `sources`.
/// Uses a reverse graph (dependents → dependency) and BFS.
fn compute_blast_radius(
    import_graph: &HashMap<PathBuf, Vec<PathBuf>>,
    sources: &HashSet<PathBuf>,
) -> HashSet<PathBuf> {
    // Build reverse graph: for each file, who imports it?
    let mut reverse: HashMap<&PathBuf, Vec<&PathBuf>> = HashMap::new();
    for (importer, deps) in import_graph {
        for dep in deps {
            reverse.entry(dep).or_default().push(importer);
        }
    }

    // BFS from all source files
    let mut visited: HashSet<PathBuf> = HashSet::new();
    let mut queue: Vec<&PathBuf> = sources.iter().collect();

    while let Some(file) = queue.pop() {
        if let Some(dependents) = reverse.get(file) {
            for dep in dependents {
                if visited.insert((*dep).clone()) {
                    queue.push(dep);
                }
            }
        }
    }

    // Remove the source files themselves from the blast radius
    // (blast radius is *other* files affected, not the files themselves)
    for source in sources {
        visited.remove(source);
    }

    visited
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        create_table(&conn).unwrap();
        conn
    }

    fn sample_resolution() -> FileResolution {
        FileResolution {
            task_id: "task-13".to_string(),
            base_commit: "abc123".to_string(),
            intent_hash: "a8f3c1".to_string(),
            mappings: vec![
                FileResolutionMapping {
                    concept: "auth_endpoints".to_string(),
                    resolved_files: vec![
                        "src/auth/handlers.rs".to_string(),
                        "src/auth/routes.rs".to_string(),
                    ],
                    resolved_modules: vec!["auth".to_string()],
                },
                FileResolutionMapping {
                    concept: "config".to_string(),
                    resolved_files: vec!["src/config/mod.rs".to_string()],
                    resolved_modules: vec!["config".to_string()],
                },
            ],
            derived: DerivedFields {
                affected_modules: vec!["auth".to_string(), "config".to_string()],
                blast_radius: vec!["auth".to_string(), "config".to_string(), "api".to_string()],
                boundary_signatures: vec![
                    "pub fn auth::handlers::login(req: Request) -> Response".to_string()
                ],
            },
        }
    }

    #[test]
    fn store_and_retrieve() {
        let conn = setup_db();
        let res = sample_resolution();
        store(&conn, &res).unwrap();

        let retrieved = get(&conn, "task-13", "abc123", "a8f3c1").unwrap().unwrap();
        assert_eq!(retrieved.task_id, "task-13");
        assert_eq!(retrieved.base_commit, "abc123");
        assert_eq!(retrieved.intent_hash, "a8f3c1");
        assert_eq!(retrieved.mappings.len(), 2);
        assert_eq!(retrieved.mappings[0].concept, "auth_endpoints");
        assert_eq!(retrieved.mappings[0].resolved_files.len(), 2);
        assert_eq!(retrieved.mappings[1].concept, "config");
        assert_eq!(retrieved.derived.affected_modules.len(), 2);
        assert_eq!(retrieved.derived.blast_radius.len(), 3);
        assert_eq!(retrieved.derived.boundary_signatures.len(), 1);
    }

    #[test]
    fn cache_miss_returns_none() {
        let conn = setup_db();
        assert!(get(&conn, "no-task", "no-commit", "no-hash")
            .unwrap()
            .is_none());
    }

    #[test]
    fn different_commit_is_cache_miss() {
        let conn = setup_db();
        let res = sample_resolution();
        store(&conn, &res).unwrap();

        // Same task+intent but different commit → miss
        assert!(get(&conn, "task-13", "different-commit", "a8f3c1")
            .unwrap()
            .is_none());
    }

    #[test]
    fn different_intent_hash_is_cache_miss() {
        let conn = setup_db();
        let res = sample_resolution();
        store(&conn, &res).unwrap();

        // Same task+commit but different intent → miss
        assert!(get(&conn, "task-13", "abc123", "different-intent")
            .unwrap()
            .is_none());
    }

    #[test]
    fn upsert_replaces_existing() {
        let conn = setup_db();
        let mut res = sample_resolution();
        store(&conn, &res).unwrap();

        // Update mappings
        res.mappings = vec![FileResolutionMapping {
            concept: "updated".to_string(),
            resolved_files: vec!["src/new.rs".to_string()],
            resolved_modules: vec!["new".to_string()],
        }];
        store(&conn, &res).unwrap();

        let retrieved = get(&conn, "task-13", "abc123", "a8f3c1").unwrap().unwrap();
        assert_eq!(retrieved.mappings.len(), 1);
        assert_eq!(retrieved.mappings[0].concept, "updated");
    }

    #[test]
    fn get_latest_for_task_returns_most_recent() {
        let conn = setup_db();

        let res1 = FileResolution {
            task_id: "task-1".to_string(),
            base_commit: "commit-old".to_string(),
            intent_hash: "hash1".to_string(),
            mappings: vec![FileResolutionMapping {
                concept: "old".to_string(),
                resolved_files: vec![],
                resolved_modules: vec![],
            }],
            derived: DerivedFields::default(),
        };
        store(&conn, &res1).unwrap();

        let res2 = FileResolution {
            task_id: "task-1".to_string(),
            base_commit: "commit-new".to_string(),
            intent_hash: "hash2".to_string(),
            mappings: vec![FileResolutionMapping {
                concept: "new".to_string(),
                resolved_files: vec![],
                resolved_modules: vec![],
            }],
            derived: DerivedFields::default(),
        };
        store(&conn, &res2).unwrap();

        let latest = get_latest_for_task(&conn, "task-1").unwrap().unwrap();
        assert_eq!(latest.base_commit, "commit-new");
        assert_eq!(latest.mappings[0].concept, "new");
    }

    #[test]
    fn get_latest_for_task_miss() {
        let conn = setup_db();
        assert!(get_latest_for_task(&conn, "nonexistent").unwrap().is_none());
    }

    #[test]
    fn invalidate_stale_removes_old_commits() {
        let conn = setup_db();

        // Store entries at two different commits
        let res1 = FileResolution {
            task_id: "task-1".to_string(),
            base_commit: "old-commit".to_string(),
            intent_hash: "h1".to_string(),
            mappings: vec![],
            derived: DerivedFields::default(),
        };
        let res2 = FileResolution {
            task_id: "task-2".to_string(),
            base_commit: "current-commit".to_string(),
            intent_hash: "h2".to_string(),
            mappings: vec![],
            derived: DerivedFields::default(),
        };
        let res3 = FileResolution {
            task_id: "task-3".to_string(),
            base_commit: "another-old".to_string(),
            intent_hash: "h3".to_string(),
            mappings: vec![],
            derived: DerivedFields::default(),
        };
        store(&conn, &res1).unwrap();
        store(&conn, &res2).unwrap();
        store(&conn, &res3).unwrap();

        let deleted = invalidate_stale(&conn, "current-commit").unwrap();
        assert_eq!(deleted, 2);

        // Only current-commit entry survives
        assert!(get(&conn, "task-2", "current-commit", "h2")
            .unwrap()
            .is_some());
        assert!(get(&conn, "task-1", "old-commit", "h1").unwrap().is_none());
        assert!(get(&conn, "task-3", "another-old", "h3").unwrap().is_none());
    }

    #[test]
    fn invalidate_stale_noop_when_all_fresh() {
        let conn = setup_db();

        let res = FileResolution {
            task_id: "task-1".to_string(),
            base_commit: "current".to_string(),
            intent_hash: "h1".to_string(),
            mappings: vec![],
            derived: DerivedFields::default(),
        };
        store(&conn, &res).unwrap();

        let deleted = invalidate_stale(&conn, "current").unwrap();
        assert_eq!(deleted, 0);
    }

    #[test]
    fn is_fresh_returns_true_for_matching_entry() {
        let conn = setup_db();
        let res = sample_resolution();
        store(&conn, &res).unwrap();

        assert!(is_fresh(&conn, "task-13", "abc123", "a8f3c1").unwrap());
    }

    #[test]
    fn is_fresh_returns_false_for_stale_commit() {
        let conn = setup_db();
        let res = sample_resolution();
        store(&conn, &res).unwrap();

        assert!(!is_fresh(&conn, "task-13", "new-commit", "a8f3c1").unwrap());
    }

    #[test]
    fn is_fresh_returns_false_for_missing_task() {
        let conn = setup_db();
        assert!(!is_fresh(&conn, "no-task", "any", "any").unwrap());
    }

    #[test]
    fn empty_mappings_and_derived() {
        let conn = setup_db();
        let res = FileResolution {
            task_id: "task-empty".to_string(),
            base_commit: "commit".to_string(),
            intent_hash: "hash".to_string(),
            mappings: vec![],
            derived: DerivedFields::default(),
        };
        store(&conn, &res).unwrap();

        let retrieved = get(&conn, "task-empty", "commit", "hash").unwrap().unwrap();
        assert!(retrieved.mappings.is_empty());
        assert!(retrieved.derived.affected_modules.is_empty());
        assert!(retrieved.derived.blast_radius.is_empty());
        assert!(retrieved.derived.boundary_signatures.is_empty());
    }

    #[test]
    fn multiple_tasks_same_commit() {
        let conn = setup_db();

        let res1 = FileResolution {
            task_id: "task-a".to_string(),
            base_commit: "same-commit".to_string(),
            intent_hash: "ha".to_string(),
            mappings: vec![FileResolutionMapping {
                concept: "auth".to_string(),
                resolved_files: vec!["src/auth.rs".to_string()],
                resolved_modules: vec!["auth".to_string()],
            }],
            derived: DerivedFields::default(),
        };
        let res2 = FileResolution {
            task_id: "task-b".to_string(),
            base_commit: "same-commit".to_string(),
            intent_hash: "hb".to_string(),
            mappings: vec![FileResolutionMapping {
                concept: "db".to_string(),
                resolved_files: vec!["src/db.rs".to_string()],
                resolved_modules: vec!["db".to_string()],
            }],
            derived: DerivedFields::default(),
        };

        store(&conn, &res1).unwrap();
        store(&conn, &res2).unwrap();

        let r1 = get(&conn, "task-a", "same-commit", "ha").unwrap().unwrap();
        let r2 = get(&conn, "task-b", "same-commit", "hb").unwrap().unwrap();
        assert_eq!(r1.mappings[0].concept, "auth");
        assert_eq!(r2.mappings[0].concept, "db");
    }

    // --- Resolution tests ---

    fn setup_project(files: &[(&str, &str)]) -> tempfile::TempDir {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src");
        std::fs::create_dir_all(&src).unwrap();
        for (path, content) in files {
            let full = src.join(path);
            if let Some(parent) = full.parent() {
                std::fs::create_dir_all(parent).unwrap();
            }
            std::fs::write(&full, content).unwrap();
        }
        tmp
    }

    #[test]
    fn concept_keywords_split() {
        assert_eq!(
            concept_to_keywords("auth_endpoints"),
            vec!["auth", "endpoints"]
        );
        assert_eq!(concept_to_keywords("config"), vec!["config"]);
        assert_eq!(
            concept_to_keywords("database_schema"),
            vec!["database", "schema"]
        );
        assert_eq!(concept_to_keywords(""), Vec::<String>::new());
    }

    #[test]
    fn keywords_match_basic() {
        let kw = vec!["auth".to_string()];
        assert!(keywords_match_name(&kw, "auth"));
        assert!(keywords_match_name(&kw, "auth_handlers"));
        assert!(!keywords_match_name(&kw, "config"));
    }

    #[test]
    fn keywords_match_case_insensitive() {
        let kw = vec!["auth".to_string()];
        assert!(keywords_match_name(&kw, "Auth"));
        assert!(keywords_match_name(&kw, "AUTH_HANDLERS"));
    }

    #[test]
    fn keywords_match_any_keyword() {
        let kw = vec!["auth".to_string(), "endpoints".to_string()];
        assert!(keywords_match_name(&kw, "auth")); // matches first
        assert!(keywords_match_name(&kw, "endpoints")); // matches second
        assert!(keywords_match_name(&kw, "api_endpoints")); // substring match
        assert!(!keywords_match_name(&kw, "config"));
    }

    #[test]
    fn resolve_empty_concepts() {
        let tmp = setup_project(&[("main.rs", "fn main() {}")]);
        let res = resolve(tmp.path(), "task-1", "commit", "hash", &[]);
        assert!(res.mappings.is_empty());
        assert!(res.derived.affected_modules.is_empty());
        assert!(res.derived.blast_radius.is_empty());
    }

    #[test]
    fn resolve_matches_file_by_name() {
        let tmp = setup_project(&[
            ("main.rs", "mod config;\nfn main() {}"),
            ("config.rs", "pub struct Config;"),
        ]);
        let areas = vec![TargetArea {
            concept: "config".to_string(),
            reasoning: "needs config".to_string(),
        }];
        let res = resolve(tmp.path(), "task-1", "commit", "hash", &areas);
        assert_eq!(res.mappings.len(), 1);
        assert!(!res.mappings[0].resolved_files.is_empty());
        assert!(res.mappings[0]
            .resolved_files
            .iter()
            .any(|f| f.contains("config.rs")));
    }

    #[test]
    fn resolve_matches_module_directory() {
        let tmp = setup_project(&[
            ("main.rs", "mod adapters;\nfn main() {}"),
            ("adapters/mod.rs", "pub mod claude;"),
            ("adapters/claude.rs", "pub struct ClaudeAdapter;"),
        ]);
        let areas = vec![TargetArea {
            concept: "adapters".to_string(),
            reasoning: "adapter changes".to_string(),
        }];
        let res = resolve(tmp.path(), "task-1", "commit", "hash", &areas);
        assert_eq!(res.mappings.len(), 1);
        // Should match the adapters module and its files
        assert!(res.mappings[0]
            .resolved_modules
            .contains(&"adapters".to_string()));
        assert!(res.mappings[0]
            .resolved_files
            .iter()
            .any(|f| f.contains("adapters")));
        assert!(res
            .derived
            .affected_modules
            .contains(&"adapters".to_string()));
    }

    #[test]
    fn resolve_compound_concept_matches_any_keyword() {
        let tmp = setup_project(&[
            ("main.rs", "mod auth;\nmod config;\nfn main() {}"),
            ("auth.rs", "pub fn login() {}"),
            ("config.rs", "pub struct Config;"),
        ]);
        let areas = vec![TargetArea {
            concept: "auth_config".to_string(),
            reasoning: "auth config".to_string(),
        }];
        let res = resolve(tmp.path(), "task-1", "commit", "hash", &areas);
        assert_eq!(res.mappings.len(), 1);
        // Should match both auth.rs and config.rs (keywords "auth" and "config")
        let files = &res.mappings[0].resolved_files;
        assert!(files.iter().any(|f| f.contains("auth.rs")));
        assert!(files.iter().any(|f| f.contains("config.rs")));
    }

    #[test]
    fn resolve_no_match_returns_empty_mapping() {
        let tmp = setup_project(&[
            ("main.rs", "mod config;\nfn main() {}"),
            ("config.rs", "pub struct Config;"),
        ]);
        let areas = vec![TargetArea {
            concept: "database".to_string(),
            reasoning: "needs db".to_string(),
        }];
        let res = resolve(tmp.path(), "task-1", "commit", "hash", &areas);
        assert_eq!(res.mappings.len(), 1);
        assert!(res.mappings[0].resolved_files.is_empty());
        assert!(res.mappings[0].resolved_modules.is_empty());
    }

    #[test]
    fn resolve_blast_radius_transitive() {
        // main imports config, config imports db
        // If we resolve "db", blast radius should include config and main (transitively)
        let tmp = setup_project(&[
            (
                "main.rs",
                "mod config;\nmod db;\nuse crate::config::Config;\nfn main() {}",
            ),
            ("config.rs", "use crate::db::Database;\npub struct Config;"),
            ("db.rs", "pub struct Database;"),
        ]);
        let areas = vec![TargetArea {
            concept: "db".to_string(),
            reasoning: "db changes".to_string(),
        }];
        let res = resolve(tmp.path(), "task-1", "commit", "hash", &areas);

        // blast_radius should include "crate" (main.rs and config.rs are both in crate module)
        assert!(!res.derived.blast_radius.is_empty());
        assert!(res.derived.blast_radius.contains(&"crate".to_string()));
    }

    #[test]
    fn resolve_multiple_concepts() {
        let tmp = setup_project(&[
            ("main.rs", "mod auth;\nmod db;\nfn main() {}"),
            ("auth.rs", "pub fn login() {}"),
            ("db.rs", "pub struct Database;"),
        ]);
        let areas = vec![
            TargetArea {
                concept: "auth".to_string(),
                reasoning: "auth".to_string(),
            },
            TargetArea {
                concept: "db".to_string(),
                reasoning: "db".to_string(),
            },
        ];
        let res = resolve(tmp.path(), "task-1", "commit", "hash", &areas);
        assert_eq!(res.mappings.len(), 2);
        assert!(res.mappings[0]
            .resolved_files
            .iter()
            .any(|f| f.contains("auth.rs")));
        assert!(res.mappings[1]
            .resolved_files
            .iter()
            .any(|f| f.contains("db.rs")));
    }

    #[test]
    fn resolve_sets_task_and_commit_fields() {
        let tmp = setup_project(&[("main.rs", "fn main() {}")]);
        let res = resolve(tmp.path(), "task-42", "abc123", "hash-xyz", &[]);
        assert_eq!(res.task_id, "task-42");
        assert_eq!(res.base_commit, "abc123");
        assert_eq!(res.intent_hash, "hash-xyz");
    }

    #[test]
    fn compute_blast_radius_empty_sources() {
        let graph: HashMap<PathBuf, Vec<PathBuf>> = HashMap::new();
        let sources: HashSet<PathBuf> = HashSet::new();
        let result = compute_blast_radius(&graph, &sources);
        assert!(result.is_empty());
    }

    #[test]
    fn compute_blast_radius_direct_dependent() {
        let a = PathBuf::from("a.rs");
        let b = PathBuf::from("b.rs");
        let mut graph = HashMap::new();
        graph.insert(b.clone(), vec![a.clone()]); // b imports a
        graph.insert(a.clone(), vec![]);

        let mut sources = HashSet::new();
        sources.insert(a);
        let result = compute_blast_radius(&graph, &sources);
        assert!(result.contains(&b));
    }

    #[test]
    fn compute_blast_radius_transitive() {
        let a = PathBuf::from("a.rs");
        let b = PathBuf::from("b.rs");
        let c = PathBuf::from("c.rs");
        let mut graph = HashMap::new();
        graph.insert(c.clone(), vec![b.clone()]); // c → b → a
        graph.insert(b.clone(), vec![a.clone()]);
        graph.insert(a.clone(), vec![]);

        let mut sources = HashSet::new();
        sources.insert(a);
        let result = compute_blast_radius(&graph, &sources);
        assert!(result.contains(&b));
        assert!(result.contains(&c));
    }

    #[test]
    fn compute_blast_radius_excludes_sources() {
        let a = PathBuf::from("a.rs");
        let b = PathBuf::from("b.rs");
        let mut graph = HashMap::new();
        graph.insert(b.clone(), vec![a.clone()]);
        graph.insert(a.clone(), vec![]);

        let mut sources = HashSet::new();
        sources.insert(a.clone());
        let result = compute_blast_radius(&graph, &sources);
        assert!(!result.contains(&a)); // source not in blast radius
        assert!(result.contains(&b));
    }

    #[test]
    fn resolve_skips_generic_entry_points() {
        // "main", "mod", "lib" stems should not match on keywords
        let tmp = setup_project(&[
            ("main.rs", "fn main() {}"),
            ("lib.rs", "pub fn lib_thing() {}"),
        ]);
        let areas = vec![TargetArea {
            concept: "main".to_string(),
            reasoning: "main".to_string(),
        }];
        let res = resolve(tmp.path(), "task-1", "commit", "hash", &areas);
        // main.rs should NOT match because we skip "main" stems
        assert!(res.mappings[0].resolved_files.is_empty());
    }
}
