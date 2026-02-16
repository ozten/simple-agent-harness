# Multi-Agent Coordination (V3)

When `workers.max > 1`, blacksmith runs a multi-agent coordinator that executes tasks in parallel using git worktrees for isolation.

## Overview

```
Coordinator
├── Scheduler (assigns tasks to workers)
├── Worker Pool (parallel agents in worktrees)
├── Integration Queue (sequential merge to main)
└── Reconciliation (periodic full test suite)
```

## Configuration

```toml
[workers]
max = 3                    # Max concurrent workers
base_branch = "main"       # Branch to create worktrees from
worktrees_dir = "worktrees" # Subdirectory under .blacksmith/
```

## Git Worktree Isolation

Each worker gets its own git worktree branched from `base_branch`. Workers operate in complete isolation — no shared working directory, no merge conflicts during coding.

```
.blacksmith/worktrees/
├── worker-0-beads-abc/    # Worker 0's isolated checkout
├── worker-1-beads-def/    # Worker 1's isolated checkout
└── worker-2-beads-ghi/    # Worker 2's isolated checkout
```

## Scheduling

The scheduler:
1. Reads ready beads from the issue tracker (no unmet dependencies)
2. Checks for affected set conflicts with currently running workers
3. Assigns non-conflicting tasks to available workers
4. Detects dependency cycles (Kahn's algorithm + Tarjan's SCC) and excludes cycled beads

### Conflict Detection

**Static** — Before assignment, the scheduler compares the new task's expected affected file globs against all running workers' affected sets. Overlapping tasks are not assigned in parallel.

**Dynamic** — During execution, an agent can signal mid-task file set expansion by writing to `.blacksmith-expand`. The coordinator updates the affected set and checks for newly-introduced conflicts.

## Task Manifests

For shared files (like `lib.rs`, `Cargo.toml`), agents don't edit them directly. Instead, they produce a `task_manifest.toml` describing the changes needed:

```toml
[[entries]]
file = "src/lib.rs"
action = "add_mod"
module = "my_new_module"

[[entries]]
file = "Cargo.toml"
action = "add_dep"
name = "serde"
version = "1.0"
features = ["derive"]
```

Manifest entries are applied mechanically during integration, avoiding merge conflicts on shared files.

## Integration Queue

Completed tasks are integrated one at a time:

1. **Merge main → branch** (not branch → main) — brings branch up to date
2. **Apply manifest entries** — mechanical changes to shared files
3. **Compile check** — run configured check command
4. **Integration agent** — if compilation fails, spawn an agent to fix errors
5. **Run tests** — verify the integrated code passes
6. **Fast-forward main** — advance main to the branch head
7. **Close bead** — mark the task complete
8. **Clean up** — remove the worktree

### Circuit Breaker

If the integration agent can't fix compilation errors within `integration_loop_max` iterations (default: 5), the circuit breaker trips:
- Bead is marked `needs_review`
- Worktree is preserved for manual inspection
- Coordinator continues scheduling other tasks

## Reconciliation

Every N integrations (configured by `reconciliation.every`), run the full test suite to catch semantic bugs that individual task tests might miss — especially cross-task interaction issues.

```toml
[reconciliation]
every = 3
```

## Rollback

If an integration needs to be undone:

```bash
blacksmith integration rollback beads-abc
```

This performs:
1. `git revert` of the merge commit
2. Reversal of manifest entries
3. Entanglement check — warns if other tasks imported from this bead's module

Use `--force` to rollback even when entangled (may break dependent tasks).

## Per-Bead Metrics

The coordinator tracks per-bead timing:
- Sessions spent on each bead
- Wall time and turn counts
- Integration time
- Completion timestamp

View with `blacksmith metrics beads`.

## Time Estimation

```bash
blacksmith estimate
blacksmith estimate --workers 4
```

Uses the dependency DAG and historical timing data to compute serial and parallel ETAs for remaining work.

## CLI Commands

```bash
# Worker pool status
blacksmith workers status

# Kill a stuck worker
blacksmith workers kill 2

# Integration history
blacksmith integration log

# Retry failed integration
blacksmith integration retry beads-abc

# Rollback
blacksmith integration rollback beads-abc
```
