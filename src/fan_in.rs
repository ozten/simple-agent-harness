//! Fan-in hotspot scoring for files.
//!
//! Computes `fan_in_score(file) = count(modules importing file) / total_modules`
//! for every file in the codebase. Files above a configurable threshold (default 0.3)
//! are splitting candidates.

use std::collections::HashMap;
use std::path::PathBuf;

#[cfg(test)]
use crate::import_graph;
#[cfg(test)]
use std::path::Path;

/// A file with its computed fan-in score.
#[derive(Debug, Clone)]
pub struct FanInEntry {
    pub file: PathBuf,
    pub importers: usize,
    pub score: f64,
}

/// Compute fan-in scores for all files in a Rust codebase.
///
/// Returns entries sorted by score descending (highest fan-in first).
#[cfg(test)]
pub fn compute_fan_in_scores(repo_root: &Path) -> Vec<FanInEntry> {
    let graph = import_graph::build_import_graph(repo_root);
    scores_from_graph(&graph)
}

/// Compute fan-in scores from a pre-built import graph.
///
/// The graph maps each file to the list of files it imports.
/// We invert it to count how many files import each target.
pub fn scores_from_graph(graph: &HashMap<PathBuf, Vec<PathBuf>>) -> Vec<FanInEntry> {
    let total_modules = graph.len();
    if total_modules == 0 {
        return Vec::new();
    }

    // Count how many distinct files import each target
    let mut fan_in_counts: HashMap<&PathBuf, usize> = HashMap::new();
    for deps in graph.values() {
        for dep in deps {
            *fan_in_counts.entry(dep).or_insert(0) += 1;
        }
    }

    // Build entries for ALL files in the graph (including those with zero fan-in)
    let mut entries: Vec<FanInEntry> = graph
        .keys()
        .map(|file| {
            let importers = fan_in_counts.get(file).copied().unwrap_or(0);
            let score = importers as f64 / total_modules as f64;
            FanInEntry {
                file: file.clone(),
                importers,
                score,
            }
        })
        .collect();

    // Sort by score descending, then by file path for determinism
    entries.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.file.cmp(&b.file))
    });

    entries
}

/// Filter entries to only those above the given threshold.
#[cfg(test)]
pub fn hotspots(entries: &[FanInEntry], threshold: f64) -> Vec<&FanInEntry> {
    entries.iter().filter(|e| e.score > threshold).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn setup_project(files: &[(&str, &str)]) -> tempfile::TempDir {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src");
        fs::create_dir_all(&src).unwrap();
        for (path, content) in files {
            let full = src.join(path);
            if let Some(parent) = full.parent() {
                fs::create_dir_all(parent).unwrap();
            }
            fs::write(&full, content).unwrap();
        }
        tmp
    }

    #[test]
    fn empty_project_returns_empty() {
        let tmp = setup_project(&[]);
        let scores = compute_fan_in_scores(tmp.path());
        assert!(scores.is_empty());
    }

    #[test]
    fn single_file_zero_fan_in() {
        let tmp = setup_project(&[("main.rs", "fn main() {}")]);
        let scores = compute_fan_in_scores(tmp.path());
        assert_eq!(scores.len(), 1);
        assert_eq!(scores[0].importers, 0);
        assert!((scores[0].score - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn one_importer() {
        let tmp = setup_project(&[
            (
                "main.rs",
                "mod config;\nuse crate::config::Foo;\nfn main() {}",
            ),
            ("config.rs", "pub struct Foo;"),
        ]);
        let scores = compute_fan_in_scores(tmp.path());
        assert_eq!(scores.len(), 2);

        // config.rs is imported by main.rs → fan_in = 1/2 = 0.5
        let config_entry = scores
            .iter()
            .find(|e| e.file.ends_with("config.rs"))
            .unwrap();
        assert_eq!(config_entry.importers, 1);
        assert!((config_entry.score - 0.5).abs() < f64::EPSILON);

        // main.rs is not imported by anyone → fan_in = 0/2 = 0.0
        let main_entry = scores.iter().find(|e| e.file.ends_with("main.rs")).unwrap();
        assert_eq!(main_entry.importers, 0);
        assert!((main_entry.score - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn multiple_importers_high_fan_in() {
        let tmp = setup_project(&[
            (
                "main.rs",
                "mod config;\nmod a;\nmod b;\nmod c;\nfn main() {}",
            ),
            ("a.rs", "use crate::config::Foo;\npub fn a() {}"),
            ("b.rs", "use crate::config::Bar;\npub fn b() {}"),
            ("c.rs", "use crate::config::Baz;\npub fn c() {}"),
            (
                "config.rs",
                "pub struct Foo;\npub struct Bar;\npub struct Baz;",
            ),
        ]);
        let scores = compute_fan_in_scores(tmp.path());

        // config.rs imported by main.rs (mod), a.rs, b.rs, c.rs → 4/5 = 0.8
        let config_entry = scores
            .iter()
            .find(|e| e.file.ends_with("config.rs"))
            .unwrap();
        assert_eq!(config_entry.importers, 4);
        assert!((config_entry.score - 0.8).abs() < f64::EPSILON);
    }

    #[test]
    fn sorted_descending_by_score() {
        let tmp = setup_project(&[
            ("main.rs", "mod a;\nmod b;\nmod c;\nfn main() {}"),
            (
                "a.rs",
                "use crate::b::Foo;\nuse crate::c::Bar;\npub fn a() {}",
            ),
            ("b.rs", "use crate::c::Baz;\npub struct Foo;"),
            ("c.rs", "pub struct Bar;\npub struct Baz;"),
        ]);
        let scores = compute_fan_in_scores(tmp.path());

        // c.rs imported by a.rs and b.rs → 2/4 = 0.5
        // b.rs imported by a.rs → 1/4 = 0.25
        // a.rs imported by main.rs (via mod) → 1/4 = 0.25
        // main.rs → 0/4 = 0.0

        // First entry should be highest score
        assert!(scores[0].score >= scores[1].score);
        assert!(scores[1].score >= scores[2].score);
        assert!(scores[2].score >= scores[3].score);
    }

    #[test]
    fn hotspots_filters_above_threshold() {
        let tmp = setup_project(&[
            (
                "main.rs",
                "mod config;\nmod a;\nmod b;\nmod c;\nfn main() {}",
            ),
            ("a.rs", "use crate::config::Foo;\npub fn a() {}"),
            ("b.rs", "use crate::config::Bar;\npub fn b() {}"),
            ("c.rs", "use crate::config::Baz;\npub fn c() {}"),
            (
                "config.rs",
                "pub struct Foo;\npub struct Bar;\npub struct Baz;",
            ),
        ]);
        let scores = compute_fan_in_scores(tmp.path());

        // config.rs has score 0.8 (imported by main+a+b+c), threshold 0.3 → should be a hotspot
        // a.rs, b.rs, c.rs each imported by main (via mod) → score 0.2, below threshold
        let hot = hotspots(&scores, 0.3);
        assert_eq!(hot.len(), 1);
        assert!(hot[0].file.ends_with("config.rs"));

        // threshold 0.9 → no hotspots (config is 0.8)
        let hot_high = hotspots(&scores, 0.9);
        assert!(hot_high.is_empty());
    }

    #[test]
    fn hotspots_default_threshold() {
        let tmp = setup_project(&[
            ("main.rs", "mod a;\nmod b;\nmod c;\nmod d;\nfn main() {}"),
            ("a.rs", "use crate::d::Foo;\npub fn a() {}"),
            ("b.rs", "use crate::d::Bar;\npub fn b() {}"),
            ("c.rs", "pub fn c() {}"),
            ("d.rs", "pub struct Foo;\npub struct Bar;"),
        ]);
        let scores = compute_fan_in_scores(tmp.path());

        // d.rs imported by a.rs, b.rs → 2/5 = 0.4, above 0.3
        let hot = hotspots(&scores, 0.3);
        assert_eq!(hot.len(), 1);
        assert!(hot[0].file.ends_with("d.rs"));
    }

    #[test]
    fn scores_from_graph_directly() {
        let mut graph: HashMap<PathBuf, Vec<PathBuf>> = HashMap::new();
        let a = PathBuf::from("a.rs");
        let b = PathBuf::from("b.rs");
        let c = PathBuf::from("c.rs");

        graph.insert(a.clone(), vec![b.clone(), c.clone()]);
        graph.insert(b.clone(), vec![c.clone()]);
        graph.insert(c.clone(), vec![]);

        let scores = scores_from_graph(&graph);
        assert_eq!(scores.len(), 3);

        // c.rs imported by a and b → 2/3
        let c_entry = scores.iter().find(|e| e.file == c).unwrap();
        assert_eq!(c_entry.importers, 2);
        assert!((c_entry.score - 2.0 / 3.0).abs() < f64::EPSILON);

        // b.rs imported by a → 1/3
        let b_entry = scores.iter().find(|e| e.file == b).unwrap();
        assert_eq!(b_entry.importers, 1);
        assert!((b_entry.score - 1.0 / 3.0).abs() < f64::EPSILON);

        // a.rs not imported → 0/3
        let a_entry = scores.iter().find(|e| e.file == a).unwrap();
        assert_eq!(a_entry.importers, 0);
    }

    #[test]
    fn deterministic_ordering_same_score() {
        let mut graph: HashMap<PathBuf, Vec<PathBuf>> = HashMap::new();
        let a = PathBuf::from("a.rs");
        let b = PathBuf::from("b.rs");
        let c = PathBuf::from("c.rs");

        // a and b both have 0 fan-in, c has fan-in 2
        graph.insert(a.clone(), vec![c.clone()]);
        graph.insert(b.clone(), vec![c.clone()]);
        graph.insert(c.clone(), vec![]);

        let scores = scores_from_graph(&graph);

        // First entry: c (highest score)
        assert_eq!(scores[0].file, c);
        // a and b tied at 0 — sorted by path
        assert_eq!(scores[1].file, a);
        assert_eq!(scores[2].file, b);
    }

    #[test]
    fn mod_declaration_counts_as_import() {
        let tmp = setup_project(&[
            ("main.rs", "mod utils;\nfn main() {}"),
            ("utils.rs", "pub fn helper() {}"),
        ]);
        let scores = compute_fan_in_scores(tmp.path());

        // utils.rs is imported by main.rs via mod → 1/2 = 0.5
        let utils_entry = scores
            .iter()
            .find(|e| e.file.ends_with("utils.rs"))
            .unwrap();
        assert_eq!(utils_entry.importers, 1);
        assert!((utils_entry.score - 0.5).abs() < f64::EPSILON);
    }
}
