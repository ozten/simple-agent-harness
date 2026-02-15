//! Dependency cycle detection for bead graphs.
//!
//! Two-phase approach:
//! 1. Kahn's algorithm (BFS topological sort) identifies nodes involved in cycles.
//! 2. Tarjan's SCC decomposes remaining nodes into individual strongly connected components.

use crate::estimation::BeadNode;
use std::collections::{HashMap, HashSet, VecDeque};

/// Detect dependency cycles in the bead graph.
///
/// Returns a list of strongly connected components (cycles), where each
/// cycle is a list of bead IDs. Returns an empty vec if the graph is a DAG.
///
/// Phase 1: Kahn's algorithm identifies nodes involved in cycles.
/// Phase 2: Tarjan's SCC decomposes them into individual cycles.
pub fn detect_cycles(beads: &[BeadNode]) -> Vec<Vec<String>> {
    if beads.is_empty() {
        return Vec::new();
    }

    let bead_ids: HashSet<&str> = beads.iter().map(|b| b.id.as_str()).collect();

    // Build adjacency list and in-degree map (only edges within the bead set)
    let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
    let mut in_degree: HashMap<&str, usize> = HashMap::new();

    for bead in beads {
        adj.entry(bead.id.as_str()).or_default();
        in_degree.entry(bead.id.as_str()).or_insert(0);
        for dep in &bead.depends_on {
            if bead_ids.contains(dep.as_str()) {
                // Edge: dep -> bead.id (bead depends on dep)
                adj.entry(dep.as_str()).or_default().push(bead.id.as_str());
                *in_degree.entry(bead.id.as_str()).or_insert(0) += 1;
            }
        }
    }

    // Phase 1: Kahn's algorithm — drain nodes with in-degree 0
    let mut queue: VecDeque<&str> = in_degree
        .iter()
        .filter(|(_, &deg)| deg == 0)
        .map(|(&id, _)| id)
        .collect();

    let mut visited: HashSet<&str> = HashSet::new();
    let mut remaining_in_degree = in_degree.clone();

    while let Some(node) = queue.pop_front() {
        visited.insert(node);
        if let Some(neighbors) = adj.get(node) {
            for &neighbor in neighbors {
                if let Some(deg) = remaining_in_degree.get_mut(neighbor) {
                    *deg -= 1;
                    if *deg == 0 {
                        queue.push_back(neighbor);
                    }
                }
            }
        }
    }

    // Nodes not visited are involved in cycles
    let cycled: HashSet<&str> = bead_ids.difference(&visited).copied().collect();

    if cycled.is_empty() {
        return Vec::new();
    }

    // Phase 2: Tarjan's SCC on the subgraph of cycled nodes
    // Build the subgraph adjacency list restricted to cycled nodes
    let mut sub_adj: HashMap<&str, Vec<&str>> = HashMap::new();
    for &node in &cycled {
        sub_adj.entry(node).or_default();
        if let Some(neighbors) = adj.get(node) {
            for &neighbor in neighbors {
                if cycled.contains(neighbor) {
                    sub_adj.entry(node).or_default().push(neighbor);
                }
            }
        }
    }

    tarjan_scc(&cycled, &sub_adj)
}

/// Tarjan's strongly connected components algorithm.
///
/// Returns SCCs with size > 1 (true cycles) plus self-loops (size 1 where node has edge to itself).
fn tarjan_scc<'a>(
    nodes: &HashSet<&'a str>,
    adj: &HashMap<&'a str, Vec<&'a str>>,
) -> Vec<Vec<String>> {
    struct State<'a> {
        adj: &'a HashMap<&'a str, Vec<&'a str>>,
        index_counter: usize,
        stack: Vec<&'a str>,
        on_stack: HashSet<&'a str>,
        index: HashMap<&'a str, usize>,
        lowlink: HashMap<&'a str, usize>,
        result: Vec<Vec<String>>,
    }

    impl<'a> State<'a> {
        fn strongconnect(&mut self, v: &'a str) {
            self.index.insert(v, self.index_counter);
            self.lowlink.insert(v, self.index_counter);
            self.index_counter += 1;
            self.stack.push(v);
            self.on_stack.insert(v);

            if let Some(neighbors) = self.adj.get(v) {
                let neighbors = neighbors.clone();
                for w in &neighbors {
                    if !self.index.contains_key(w) {
                        self.strongconnect(w);
                        let w_low = self.lowlink[w];
                        let v_low = self.lowlink.get_mut(v).unwrap();
                        if w_low < *v_low {
                            *v_low = w_low;
                        }
                    } else if self.on_stack.contains(w) {
                        let w_idx = self.index[w];
                        let v_low = self.lowlink.get_mut(v).unwrap();
                        if w_idx < *v_low {
                            *v_low = w_idx;
                        }
                    }
                }
            }

            // If v is a root node, pop the SCC
            if self.lowlink[v] == self.index[v] {
                let mut component = Vec::new();
                loop {
                    let w = self.stack.pop().unwrap();
                    self.on_stack.remove(w);
                    component.push(w.to_string());
                    if w == v {
                        break;
                    }
                }
                if component.len() > 1 {
                    component.sort();
                    self.result.push(component);
                } else if component.len() == 1 {
                    let node = component[0].as_str();
                    if let Some(neighbors) = self.adj.get(node) {
                        if neighbors.contains(&node) {
                            self.result.push(component);
                        }
                    }
                }
            }
        }
    }

    let mut state = State {
        adj,
        index_counter: 0,
        stack: Vec::new(),
        on_stack: HashSet::new(),
        index: HashMap::new(),
        lowlink: HashMap::new(),
        result: Vec::new(),
    };

    // Sort nodes for deterministic output
    let mut sorted_nodes: Vec<&str> = nodes.iter().copied().collect();
    sorted_nodes.sort();

    for &node in &sorted_nodes {
        if !state.index.contains_key(node) {
            state.strongconnect(node);
        }
    }

    state.result.sort();
    state.result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::estimation::BeadNode;

    #[test]
    fn no_cycles_empty_graph() {
        let result = detect_cycles(&[]);
        assert!(result.is_empty());
    }

    #[test]
    fn no_cycles_dag() {
        // a -> b -> c (linear DAG)
        let beads = vec![
            BeadNode {
                id: "a".into(),
                depends_on: vec![],
            },
            BeadNode {
                id: "b".into(),
                depends_on: vec!["a".into()],
            },
            BeadNode {
                id: "c".into(),
                depends_on: vec!["b".into()],
            },
        ];
        let result = detect_cycles(&beads);
        assert!(result.is_empty());
    }

    #[test]
    fn no_cycles_diamond_dag() {
        // a -> b, a -> c, b -> d, c -> d
        let beads = vec![
            BeadNode {
                id: "a".into(),
                depends_on: vec![],
            },
            BeadNode {
                id: "b".into(),
                depends_on: vec!["a".into()],
            },
            BeadNode {
                id: "c".into(),
                depends_on: vec!["a".into()],
            },
            BeadNode {
                id: "d".into(),
                depends_on: vec!["b".into(), "c".into()],
            },
        ];
        let result = detect_cycles(&beads);
        assert!(result.is_empty());
    }

    #[test]
    fn single_cycle_two_nodes() {
        // a <-> b (a depends on b, b depends on a)
        let beads = vec![
            BeadNode {
                id: "a".into(),
                depends_on: vec!["b".into()],
            },
            BeadNode {
                id: "b".into(),
                depends_on: vec!["a".into()],
            },
        ];
        let result = detect_cycles(&beads);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], vec!["a", "b"]);
    }

    #[test]
    fn single_cycle_three_nodes() {
        // a -> b -> c -> a
        let beads = vec![
            BeadNode {
                id: "a".into(),
                depends_on: vec!["c".into()],
            },
            BeadNode {
                id: "b".into(),
                depends_on: vec!["a".into()],
            },
            BeadNode {
                id: "c".into(),
                depends_on: vec!["b".into()],
            },
        ];
        let result = detect_cycles(&beads);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], vec!["a", "b", "c"]);
    }

    #[test]
    fn multiple_independent_cycles() {
        // Cycle 1: a <-> b
        // Cycle 2: c <-> d
        // e is independent (no cycle)
        let beads = vec![
            BeadNode {
                id: "a".into(),
                depends_on: vec!["b".into()],
            },
            BeadNode {
                id: "b".into(),
                depends_on: vec!["a".into()],
            },
            BeadNode {
                id: "c".into(),
                depends_on: vec!["d".into()],
            },
            BeadNode {
                id: "d".into(),
                depends_on: vec!["c".into()],
            },
            BeadNode {
                id: "e".into(),
                depends_on: vec![],
            },
        ];
        let result = detect_cycles(&beads);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], vec!["a", "b"]);
        assert_eq!(result[1], vec!["c", "d"]);
    }

    #[test]
    fn self_loop() {
        // a depends on itself
        let beads = vec![
            BeadNode {
                id: "a".into(),
                depends_on: vec!["a".into()],
            },
            BeadNode {
                id: "b".into(),
                depends_on: vec![],
            },
        ];
        let result = detect_cycles(&beads);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], vec!["a"]);
    }

    #[test]
    fn complex_graph_mixed_cycles_and_dag() {
        // DAG part: x -> y -> z
        // Cycle 1: a -> b -> c -> a
        // Cycle 2: d <-> e
        // f depends on both z (DAG) and a (cycle) — f itself is not in a cycle
        let beads = vec![
            BeadNode {
                id: "x".into(),
                depends_on: vec![],
            },
            BeadNode {
                id: "y".into(),
                depends_on: vec!["x".into()],
            },
            BeadNode {
                id: "z".into(),
                depends_on: vec!["y".into()],
            },
            BeadNode {
                id: "a".into(),
                depends_on: vec!["c".into()],
            },
            BeadNode {
                id: "b".into(),
                depends_on: vec!["a".into()],
            },
            BeadNode {
                id: "c".into(),
                depends_on: vec!["b".into()],
            },
            BeadNode {
                id: "d".into(),
                depends_on: vec!["e".into()],
            },
            BeadNode {
                id: "e".into(),
                depends_on: vec!["d".into()],
            },
            BeadNode {
                id: "f".into(),
                depends_on: vec!["z".into(), "a".into()],
            },
        ];
        let result = detect_cycles(&beads);
        // f depends on a (which is in a cycle), but f itself has in-degree from
        // z (non-cycled) and a (cycled). Since a never gets drained by Kahn's,
        // f's in-degree never reaches 0, so f ends up in the "cycled" set too.
        // But Tarjan's SCC will show f is NOT in an SCC > 1 (f doesn't have a
        // path back to itself). Let me reconsider...
        //
        // Actually: f depends on z and a. z is in the DAG portion (gets drained).
        // a is in the cycle, so it never gets drained. f's in-degree starts at 2
        // (from z and a). When z is drained, f's in-degree drops to 1. But a is
        // never drained, so f's in-degree stays at 1 and f is never drained either.
        // So f ends up in the "remaining" set. But Tarjan's will find that f is
        // in its own SCC of size 1 with no self-loop, so it won't be reported.
        //
        // The cycles reported should be: {a, b, c} and {d, e}
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], vec!["a", "b", "c"]);
        assert_eq!(result[1], vec!["d", "e"]);
    }

    #[test]
    fn external_deps_ignored() {
        // a depends on "external" which is not in the bead set
        // b depends on a — this is a normal DAG
        let beads = vec![
            BeadNode {
                id: "a".into(),
                depends_on: vec!["external".into()],
            },
            BeadNode {
                id: "b".into(),
                depends_on: vec!["a".into()],
            },
        ];
        let result = detect_cycles(&beads);
        assert!(result.is_empty());
    }

    #[test]
    fn single_node_no_cycle() {
        let beads = vec![BeadNode {
            id: "a".into(),
            depends_on: vec![],
        }];
        let result = detect_cycles(&beads);
        assert!(result.is_empty());
    }

    #[test]
    fn all_nodes_in_one_big_cycle() {
        // a -> b -> c -> d -> a
        let beads = vec![
            BeadNode {
                id: "a".into(),
                depends_on: vec!["d".into()],
            },
            BeadNode {
                id: "b".into(),
                depends_on: vec!["a".into()],
            },
            BeadNode {
                id: "c".into(),
                depends_on: vec!["b".into()],
            },
            BeadNode {
                id: "d".into(),
                depends_on: vec!["c".into()],
            },
        ];
        let result = detect_cycles(&beads);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], vec!["a", "b", "c", "d"]);
    }
}
