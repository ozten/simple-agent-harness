# Blacksmith V3 — Agent-Agnostic Architecture, Storage & Multi-Agent Coordination

Consolidates all blacksmith artifacts under a `.blacksmith/` data directory, adds session lifecycle management (retention, compression, cleanup), introduces an adapter layer so blacksmith works with any AI coding agent, and adds multi-agent coordination with worktree isolation, conflict-aware scheduling, and a mechanical integration loop.

Builds on V2 (evolvable metrics). V2 must land first.

---

## Problem

V2 solves the *metrics* problem but leaves four structural issues:

1. **Scattered artifacts.** Session output files (`claude-iteration-*.jsonl`), the counter file, status file, and database all live in the project root or a flat output directory. A 50-iteration run produces 50+ files mixed in with source code. There's no `.gitignore`-friendly single directory to exclude.

2. **Unbounded growth.** Session JSONL files accumulate forever. A single file is 100KB-2MB. After V2 ingestion, the structured data lives in the database — the raw JSONL is rarely needed but never cleaned up. Over hundreds of sessions this becomes tens of gigabytes of redundant data.

3. **Claude-specific parsing.** V2's built-in metric extraction assumes Claude's `stream-json` JSONL format — `type: "assistant"` events, `tool_use` blocks, `usage` fields. Codex, OpenCode, Aider, and other agents produce different output formats. Blacksmith can *spawn* any agent (the command is configurable), but it can only *understand* Claude's output.

4. **Single-agent bottleneck.** V2 runs one agent at a time in a serial loop. Independent tasks that could run in parallel are serialized, wasting wall-clock time. When multiple agents work concurrently in the same repo, three classes of conflict arise: physical (same file edits), semantic (API changes), and logical (incompatible design decisions). There's no coordination layer.

## Design Principles

- **Convention over configuration**: `.blacksmith/` is the default data directory, like `.git/`. Zero config needed for the common case.
- **Ingest then forget**: Raw session files are intermediate artifacts. Once ingested into the database, they can be compressed and eventually deleted.
- **Adapter, not abstraction**: Each agent's output format is different enough that a common parser is impractical. Instead, each agent gets a dedicated adapter that normalizes output into blacksmith's event model. No lowest-common-denominator compromises.
- **Graceful degradation**: If an adapter can't extract a metric (e.g., Codex doesn't report token counts), that metric is simply absent. Targets and briefs skip missing metrics rather than erroring.
- **Isolation by default**: Each worker gets its own git worktree. Workers never share a working directory. Conflicts are resolved mechanically at integration time, not during coding.
- **Main is always green**: No code reaches main without passing compilation and tests. The integration loop merges main into the branch, never the reverse, until the code is verified.
- **Escalate, don't block**: When something fails, label it for human review and continue scheduling unblocked work. The system should never stall waiting for a human.

---

## Data Directory

All blacksmith artifacts move under a single `.blacksmith/` directory at the project root.

### Structure

```
.blacksmith/
├── blacksmith.db           # SQLite database (events, observations, improvements, coordinator state)
├── status                  # harness status file (running/idle/etc)
├── counter                 # global iteration counter
├── worktrees/              # managed git worktrees (V3 multi-agent)
│   ├── worker-0-beads-abc/ # worktree for worker 0, task beads-abc
│   ├── worker-1-beads-def/ # worktree for worker 1, task beads-def
│   └── ...
└── sessions/
    ├── 142.jsonl           # recent: raw output, uncompressed
    ├── 141.jsonl
    ├── 140.jsonl.zst       # older: compressed with zstd
    ├── 139.jsonl.zst
    └── ...
```

### Changes from V2

| V2 | V3 |
|---|---|
| `blacksmith.db` in output_dir | `.blacksmith/blacksmith.db` |
| `blacksmith.status` in output_dir | `.blacksmith/status` |
| `.iteration_counter` in project root | `.blacksmith/counter` |
| `claude-iteration-{N}.jsonl` in output_dir | `.blacksmith/sessions/{N}.jsonl` |
| `session.output_prefix` config key | Removed — filename is always `{N}.jsonl` |
| `session.output_dir` config key | Replaced by `storage.data_dir` |
| `session.counter_file` config key | Removed — always `.blacksmith/counter` |
| No worktree management | `.blacksmith/worktrees/` managed by coordinator |

### Config

```toml
[storage]
data_dir = ".blacksmith"    # default; rarely needs changing
```

The `data_dir` is relative to the project root (where `blacksmith.toml` lives). Blacksmith creates it on first run.

### Initialization

On first run, blacksmith creates the directory structure:

```
blacksmith init    # explicit, or auto-created on first `blacksmith run`
```

Creates `.blacksmith/`, `.blacksmith/sessions/`, `.blacksmith/worktrees/`, initializes the database (including coordinator tables), and appends `.blacksmith/` to `.gitignore` if a `.gitignore` exists and doesn't already contain the entry.

### Migration from V2 layout

```
blacksmith migrate --consolidate
```

Moves existing files into the `.blacksmith/` structure:
- Finds `blacksmith.db` / `harness.db` in the old output directory
- Moves `{output_prefix}-*.jsonl` files into `.blacksmith/sessions/`, renaming to `{N}.jsonl`
- Moves counter and status files
- Prints a summary of what moved

This is non-destructive — files are moved, not copied-and-deleted. If anything fails, it stops and reports.

---

## Session Lifecycle

Session files progress through three stages: **active -> compressed -> deleted**.

### Stages

```
Active       Compressed       Deleted
(raw JSONL)  (zstd)           (removed)
   |              |                |
   v              v                v
142.jsonl -> 142.jsonl.zst ->  (gone)
```

1. **Active**: The current session's output file. Written to by the agent process. After the session completes and is ingested into the database, it becomes eligible for compression.

2. **Compressed**: The JSONL is compressed with zstd (fast, good ratio). Compressed files can still be re-ingested if extraction rules change — just decompress first. Keeps the audit trail at ~10-20% of original size.

3. **Deleted**: Beyond the retention window, compressed files are removed. The data lives on in the events/observations tables.

### Retention Policy

```toml
[storage]
retention = "last-50"        # keep the last N sessions (default)
# retention = "30d"          # keep sessions from the last N days
# retention = "all"          # never delete (V2 behavior)
# retention = "after-ingest" # aggressive: delete immediately after ingestion

compress_after = 5           # compress sessions older than N iterations (default: 5)
```

**`retention`** controls when compressed files are deleted:
- `last-N` — keep the N most recent sessions (compressed or not). Delete older ones.
- `Nd` — keep sessions from the last N days based on file modification time.
- `all` — never delete. Files still get compressed per `compress_after`.
- `after-ingest` — delete the JSONL immediately after successful ingestion. No compression stage. Most aggressive; saves the most space but loses the raw audit trail.

**`compress_after`** controls when active files are compressed. Default 5 means the 5 most recent sessions stay as raw JSONL for easy inspection; older ones are compressed.

### When cleanup runs

Cleanup is part of the loop lifecycle, running at the **start** of each iteration (before prompt assembly). This means:

```
┌─ Loop iteration ──────────────────────────────────────────────┐
│                                                               │
│  1. Check STOP file / signals                                 │
│  2. Cleanup stale sessions           (built-in, V3)           │
│  3. Assemble prompt                                           │
│     a. Run prepend_commands                                   │
│     b. Generate brief from blacksmith.db                      │
│     c. Read prompt file                                       │
│  4. Run pre-session hooks                                     │
│  5. Spawn agent session + watchdog                            │
│  6. Evaluate output (retry logic)                             │
│  7. Ingest session JSONL into database                        │
│  8. Compress previous session if eligible      (V3)           │
│  9. Check rate limit + backoff                                │
│  10. Run post-session hooks                                   │
│  11. Save iteration counter                                   │
│                                                               │
└───────────────────────────────────────────────────────────────┘
```

Step 2 handles deletion of files beyond the retention window. Step 8 compresses the *previous* session (not the one just completed — it might need re-reading for post-hooks).

### Manual cleanup

```
blacksmith gc                    # run cleanup immediately
blacksmith gc --dry-run          # show what would be cleaned up
blacksmith gc --aggressive       # compress all, delete beyond retention
```

### Re-ingestion

If extraction rules change (new patterns added in `[metrics.extract]`), you can re-ingest historical sessions:

```
blacksmith metrics reingest [--last N] [--all]
```

This decompresses `.jsonl.zst` files as needed, re-runs extraction, and updates the events table. Only works for sessions that haven't been deleted.

---

## Agent Adapters

An adapter is a module that knows how to parse a specific agent's output format and normalize it into blacksmith's event model.

### Configuration

```toml
[agent.coding]
command = "claude"
args = ["-p", "{prompt}", "--verbose", "--output-format", "stream-json"]
adapter = "claude"           # default: auto-detect from command name

[agent.integration]
command = "claude"
args = ["-p", "{prompt}", "--output-format", "stream-json"]
adapter = "claude"
# Integration can use a cheaper/faster model — it only reads compiler
# errors and applies mechanical fixes, no creative design work needed.
```

The `adapter` field selects which parser to use for the session output. If omitted, blacksmith infers it from the command name:

| Command contains | Adapter |
|---|---|
| `claude` | `claude` |
| `codex` | `codex` |
| `opencode` | `opencode` |
| `aider` | `aider` |
| (anything else) | `raw` |

Explicit `adapter = "..."` overrides auto-detection.

**Phase-specific models.** The `[agent.coding]` section configures the agent used for coding tasks — the creative work of implementing features, fixing bugs, writing tests. The `[agent.integration]` section configures the agent used for the integration loop — merging, fixing compiler errors, resolving manifest conflicts. Integration doesn't require design reasoning, so a faster/cheaper model is sufficient. If `[agent.integration]` is omitted, it falls back to `[agent.coding]`. If neither is set, the legacy `[agent]` section is used.

### Adapter trait

```rust
/// Normalizes agent-specific output into blacksmith events.
pub trait AgentAdapter: Send + Sync {
    /// Human-readable adapter name (e.g., "claude", "codex").
    fn name(&self) -> &str;

    /// Extract events from a session output file.
    ///
    /// Returns a Vec of (kind, value) pairs to be written as events.
    /// The adapter handles all format-specific parsing internally.
    fn extract_builtin_metrics(
        &self,
        output_path: &Path,
        session: u64,
    ) -> Result<Vec<(String, serde_json::Value)>, AdapterError>;

    /// Which built-in event kinds this adapter can produce.
    ///
    /// Used by the brief/targets system to know which metrics are
    /// available. Metrics not in this list are simply skipped in
    /// dashboards rather than showing as errors.
    fn supported_metrics(&self) -> &[&str];

    /// Provide raw text lines for configurable extraction rules to scan.
    ///
    /// The adapter controls what "source" means for each source type.
    /// For example, the Claude adapter maps "tool_commands" to
    /// tool_use input.command fields, while the Codex adapter maps
    /// it to function_call arguments.
    fn lines_for_source(
        &self,
        output_path: &Path,
        source: ExtractionSource,
    ) -> Result<Vec<String>, AdapterError>;
}

pub enum ExtractionSource {
    ToolCommands,   // tool invocation commands
    Text,           // assistant text output
    Raw,            // raw file lines, unprocessed
}
```

### Built-in adapters

#### `claude` (default)

Parses Claude's `stream-json` JSONL format. This is what V2's ingest module already does — V3 just wraps it in the adapter trait.

**Supported built-in metrics:**
- `turns.total`, `turns.narration_only`, `turns.parallel`, `turns.tool_calls`
- `cost.input_tokens`, `cost.output_tokens`, `cost.estimate_usd`
- `session.output_bytes`, `session.exit_code`, `session.duration_secs`

**Source mapping:**
- `tool_commands` -> `tool_use` block `input.command` fields
- `text` -> assistant message `text` blocks
- `raw` -> raw JSONL lines

#### `codex`

Parses Codex CLI output.

**Supported built-in metrics:**
- `turns.total`, `turns.tool_calls`
- `session.output_bytes`, `session.exit_code`, `session.duration_secs`

**Not available** (gracefully skipped):
- `turns.narration_only`, `turns.parallel` — Codex doesn't report parallel tool calls
- `cost.*` — Codex doesn't expose token usage in CLI output

**Source mapping:**
- `tool_commands` -> shell command invocations in function_call blocks
- `text` -> assistant response text
- `raw` -> raw output lines

#### `opencode`

Parses OpenCode's session output.

**Supported built-in metrics:**
- `turns.total`, `turns.tool_calls`
- `cost.input_tokens`, `cost.output_tokens` (if available in output)
- `session.output_bytes`, `session.exit_code`, `session.duration_secs`

#### `aider`

Parses Aider's session output / chat log.

**Supported built-in metrics:**
- `turns.total`
- `cost.estimate_usd` (Aider reports cost)
- `session.output_bytes`, `session.exit_code`, `session.duration_secs`

#### `raw`

No format-specific parsing. Zero built-in metrics extracted. All metrics come from configurable extraction rules (`[metrics.extract]`).

This is the escape hatch for unsupported agents or non-AI use cases. If your agent isn't recognized and you don't set `adapter`, this is what you get.

**Supported built-in metrics:**
- `session.output_bytes`, `session.exit_code`, `session.duration_secs` (from the harness, not the output)

**Source mapping:**
- All sources (`tool_commands`, `text`, `raw`) -> raw file lines (no semantic separation)

### Universal metrics

Some metrics come from the harness itself, not from parsing agent output. These are available regardless of adapter:

- `session.duration_secs` — wall clock time (measured by the harness)
- `session.output_bytes` — output file size
- `session.exit_code` — agent process exit code
- `session.outcome` — determined by retry/watchdog logic

### Graceful degradation

When a target or brief references a metric that the current adapter doesn't support:

```toml
[[metrics.targets.rules]]
kind = "cost.estimate_usd"      # Codex adapter can't provide this
compare = "avg"
threshold = 30
direction = "below"
```

Behavior: the target is **silently skipped** in dashboard and brief output. No error. The `metrics targets` command shows it as "N/A (not available from codex adapter)" so the user understands why.

This means a single `blacksmith.toml` can define targets for all metrics, and whichever agent is running will report on the subset it supports.

### Prompt template

The `{prompt}` placeholder in agent args works for all agents. But agents have different conventions for how prompts are passed:

```toml
# Claude: prompt as argument
[agent.coding]
command = "claude"
args = ["-p", "{prompt}", "--output-format", "stream-json"]

# Codex: prompt as argument
[agent.coding]
command = "codex"
args = ["--quiet", "{prompt}"]

# OpenCode: prompt via stdin
[agent.coding]
command = "opencode"
args = ["--non-interactive"]
prompt_via = "stdin"         # write prompt to stdin instead of arg substitution

# Aider: prompt via file
[agent.coding]
command = "aider"
args = ["--message-file", "{prompt_file}"]  # {prompt_file} = path to temp file
```

New placeholders:
- `{prompt}` — inline prompt text (existing, V1)
- `{prompt_file}` — path to a temp file containing the prompt (V3). Blacksmith writes the assembled prompt to a temp file and substitutes the path. Cleaned up after session.

New config:
- `prompt_via = "arg"` (default) — substitute `{prompt}` in args
- `prompt_via = "stdin"` — write prompt to the agent's stdin
- `prompt_via = "file"` — write prompt to temp file, substitute `{prompt_file}` in args

---

## Multi-Agent Coordination

When `workers.max > 1`, blacksmith transitions from a serial loop to a coordinated multi-agent system. Multiple workers execute independent tasks in parallel, each in its own git worktree, with a mechanical integration loop that merges completed work back to main.

### Architecture Overview

```
                  ┌──────────────────────────────────────────────┐
                  │              COORDINATOR                      │
                  │                                              │
  beads ─────────►│  Scheduler ──► Worker Pool (up to N)         │
  (task source)   │       │            │  │  │                   │
                  │       │         ┌──┘  │  └──┐                │
                  │       │         ▼     ▼     ▼                │
                  │       │       wt-0  wt-1  wt-2               │
                  │       │      (code) (code) (idle)            │
                  │       │         │     │                      │
                  │       │         ▼     ▼                      │
                  │       │     Integrator (sequential)          │
                  │       │         │                             │
                  │       │         ▼                             │
                  │       │       main (always green)             │
                  │       │         │                             │
                  │       │    [every N integrations]             │
                  │       │         ▼                             │
                  │       │    Reconciliation                     │
                  │       │    (full test suite)                  │
                  └───────┴──────────────────────────────────────┘
```

The coordinator reads tasks from beads, assigns them to idle workers, manages worktrees, and runs the integration loop when a worker completes. Tasks are created externally (e.g., by a human using `prd-to-beads`). Blacksmith is the execution engine, not the planner.

### Configuration

```toml
[workers]
max = 3                      # maximum concurrent workers (default: 3)

[reconciliation]
every = 3                    # run full test suite every N integrations (default: 3)
```

### Task Model

Tasks come from beads. Each bead that blacksmith should execute must have an **affected set** — file globs declaring what the task is expected to touch.

```bash
bd create --title="Add analytics module" --type=feature --priority=2
bd update beads-abc --design="affected: src/analytics/**/*.rs, src/lib.rs"
```

The affected set is stored in the bead's `design` field using a simple convention:

```
affected: <glob>, <glob>, ...
```

Globs follow standard gitignore-style patterns:
- `src/analytics/**/*.rs` — all Rust files under analytics
- `src/db.rs` — a specific file
- `tests/integration/**` — all integration test files

A bead without an affected set is treated as affecting everything — it will not be scheduled in parallel with any other task.

### Scheduling

The scheduler runs whenever a worker becomes idle. It queries beads for ready work and assigns tasks respecting conflict constraints.

A task is **assignable** when:

1. Its bead status is `open` (not `in_progress`, not `closed`).
2. All its bead dependencies are `closed`.
3. Its affected set does not overlap with any in-progress task's affected set.

```rust
fn next_assignable_tasks(
    ready_beads: &[Bead],
    in_progress: &[Assignment],
) -> Vec<BeadId> {
    let locked_globs: Vec<&str> = in_progress.iter()
        .flat_map(|a| a.affected_globs.iter().map(String::as_str))
        .collect();

    ready_beads.iter()
        .filter(|b| !globs_overlap(&b.affected_globs, &locked_globs))
        .map(|b| b.id.clone())
        .collect()
}
```

When a task is assigned:
1. Blacksmith runs `bd update <id> --status=in_progress`.
2. A git worktree is created from current `main`: `git worktree add .blacksmith/worktrees/worker-{N}-{bead-id} main`.
3. The worker is spawned in that worktree with a prompt assembled from the bead's description, design notes, and the standard brief.

### Dynamic Affected Set Expansion

An agent may discover mid-task that it needs files outside its declared affected set. The agent signals this by writing to a well-known file in the worktree:

```
# .blacksmith-expand
src/config.rs
src/signals.rs
```

The coordinator watches for this file. When it appears:

1. The new globs are added to the task's affected set in the coordinator database.
2. Future scheduling decisions account for the expanded set.
3. The request is always granted — no blocking. If the expansion creates an overlap with another in-progress task, the conflict is resolved at integration time.

This is optimistic concurrency: assume expansions are fine, handle conflicts mechanically later.

### Coding Phase

Each worker operates in its own git worktree with full autonomy. There is one critical constraint: **agents do not edit manifest files**.

#### Manifest Files

Manifest files are files that primarily register things rather than implement things:

- `lib.rs` / `main.rs` — module declarations (`mod foo;`, `pub use foo::Bar;`)
- `mod.rs` — submodule registration
- `Cargo.toml` — dependency additions
- TypeScript `index.ts` barrel files — re-exports
- `package.json` — dependency additions

Most edits to these files are commutative (order-independent) and append-only. Instead of editing them directly, the agent records what it needs as structured metadata:

```toml
# task_manifest.toml — agent produces this alongside its code

[[lib_rs_additions]]
kind = "mod_declaration"
content = "pub mod analytics;"

[[lib_rs_additions]]
kind = "reexport"
content = "pub use analytics::AnalyticsEngine;"

[[cargo_toml_additions]]
kind = "dependency"
content = 'serde = { version = "1.0", features = ["derive"] }'
```

This separation is the key to avoiding merge conflicts on shared files. The agent does the creative work; the integrator does the bookkeeping.

#### What the Worker Produces

On completion, the worktree contains:

1. A branch with all code changes (excluding manifest files).
2. A `task_manifest.toml` describing required manifest entries.
3. Passing tests within the worktree's isolated state.

The worker signals completion by exiting successfully. The coordinator detects this and queues the worktree for integration.

### Integration Loop

The integrator takes over **the same worktree** the coding agent used. This preserves build artifacts, installed dependencies, and compilation cache (critical for Rust incremental builds).

Integration uses the `[agent.integration]` config, which can be a cheaper/faster model since integration requires reading compiler errors and applying patterns, not design reasoning.

```
┌─ Integration Loop ────────────────────────────────────────┐
│                                                           │
│  1. In the worker's worktree, merge main into the branch  │
│     (pull main into branch, NOT push branch to main)      │
│                                                           │
│  2. Apply manifest entries from task_manifest.toml        │
│     - mod declarations appended to lib.rs                 │
│     - re-exports appended to appropriate module           │
│     - Cargo.toml deps merged into [dependencies]          │
│     - TypeScript barrel exports appended to index.ts      │
│                                                           │
│  3. cargo check / tsc --noEmit                            │
│                                                           │
│  4. If errors:                                            │
│     - Spawn integration agent to apply surgical fix       │
│       (add missing import, resolve name collision)        │
│     - Go to 3                                             │
│     - Circuit breaker: if > 3 iterations, ABORT           │
│                                                           │
│  5. cargo test / npm test                                 │
│                                                           │
│  6. If test failures:                                     │
│     - Is this a conflict the coding agent needs to        │
│       revisit? Yes -> abort, label for human review       │
│       No -> spawn integration agent to fix, go to 5       │
│                                                           │
│  7. Fast-forward main to this branch                      │
│                                                           │
│  8. Run bd close <bead-id>                                │
│                                                           │
│  9. Clean up worktree                                     │
│                                                           │
└───────────────────────────────────────────────────────────┘
```

#### Why This Works

**Step 1 is the key inversion.** By merging main into the branch (not the branch into main), main is untouched during the entire fix loop. Main is only updated at step 7, which is a fast-forward of already-tested code. This gives you the always-green-main invariant without a separate integration branch.

**Step 2 is mechanical.** Manifest entries are commutative — two tasks adding different entries never conflict. The integrator is doing code generation, not conflict resolution.

**Step 4 is bounded.** Errors from merging and applying manifests fall into a small, enumerable category: missing imports, duplicate names, type mismatches at module boundaries. These are compiler errors with clear diagnostics and local fixes. The integration agent handles these reliably in a single shot.

**The circuit breaker matters.** If step 4 takes more than 3 iterations, the problem isn't integration — it's the task decomposition. Escalate to human review.

#### Integration is Sequential

Only one integration runs at a time. This is intentional — it keeps main's history linear and avoids merge races. With up to 3 workers, the integration queue is short. Workers continue coding while one task integrates.

### Reconciliation

Individual tasks can integrate cleanly, pass all tests independently, and still produce a semantically wrong result together. Agent A adds a caching layer assuming immutable data. Agent B adds a mutation endpoint. Both correct in isolation; together they produce stale reads.

Reconciliation catches this by running the **full** test suite periodically:

```toml
[reconciliation]
every = 3    # run full test suite every N successful integrations
```

After every Nth integration:
1. Run the complete test suite on main.
2. If failures are detected, the last N integrated tasks are flagged for human review.

This is expensive but catches the nastiest multi-agent bugs — the ones that live in the gaps between task boundaries.

### Failure Handling

#### Circuit Breaker (Integration Failure)

When integration exceeds 3 fix iterations:

1. The task's bead is updated with failure notes: `bd update <id> --notes="Integration failed: <error summary>"`.
2. The bead is labeled for human review (custom label or status convention — e.g., `needs_review`).
3. The worktree is preserved for inspection (not cleaned up).
4. The coordinator **continues scheduling and executing unblocked tasks**. Only tasks whose affected sets overlap with the failed task are held.
5. The CLI displays a prominent `HUMAN REVIEW NEEDED` message (red/bold in terminal).

```
[ERROR] HUMAN REVIEW NEEDED: beads-abc "Add analytics module"
        Integration failed after 3 attempts.
        Error: type mismatch in src/metrics.rs:42
        Worktree preserved: .blacksmith/worktrees/worker-0-beads-abc/
        Run `bd show beads-abc` for details.
```

#### Future: Notifications

When human review is needed, blacksmith can optionally notify:

```toml
[notifications]
# Future V3.x — not in initial release
slack_webhook = "https://hooks.slack.com/..."
email = "team@example.com"
```

The notification interface is designed but not implemented in the first V3 release. The CLI output and bead labeling are the V3 notification mechanism.

#### Rollback

When an integrated task must be undone (post-reconciliation failure, human decision):

- `git revert` handles the code changes.
- Manifest entries must also be reversed — remove the `pub mod` line, the `Cargo.toml` dep, the re-export.
- **Entanglement**: If task B was integrated after task A, and task B's code imports from task A's module, you can't revert A without breaking B. The coordinator tracks dependencies created during integration (imports added by the integration agent that cross task boundaries) for safe rollback ordering.

### Dependency Cycle Detection

Beads allows arbitrary dependency graphs. Today, `bd dep add` inserts a dependency row without validating the graph — a cycle can be created silently. Cycled tasks never appear in `bd ready`, are never scheduled, and produce no error. They just disappear from the system's view of available work.

This is tolerable when a human is managing a handful of tasks. It's catastrophic for the multi-agent coordinator, which relies on the dependency DAG for scheduling, critical path estimation, and progress tracking. A cycle means:

- **Silent deadlock.** Cycled tasks are permanently blocked. No error, no warning — work simply stops being scheduled. The coordinator reports "0 ready beads" and idles.
- **Broken time estimates.** Critical path estimation traverses the DAG. A cycle creates an infinite path, producing nonsense ETAs or crashing the estimator.
- **Invisible progress stall.** The status line shows "Progress: 18/24 beads" and never changes. The remaining 6 beads are in a cycle, but nothing tells you that.

The fix is detection at schedule time and loud surfacing when found.

#### Out of Scope: Prevention in `bd dep add`

Ideally, `bd dep add A B` would reject the insert if it creates a cycle. However, beads is an external tool (`bd v0.49.x`, installed via Homebrew) — we don't control it. Beads already has `bd dep cycles` for after-the-fact detection and `bd doctor` includes cycle checking, but neither prevents creation.

We should file an upstream issue requesting cycle prevention on `bd dep add` (DFS reachability check before insert). Until that ships, blacksmith must be resilient to cycles in the graph it reads.

#### Detection: Coordinator Scheduling Check

Cycles can exist in the beads graph from:
- Accidental `bd dep add` (no prevention today)
- Concurrent `bd dep add` calls (race condition even if prevention is added)
- Bugs in beads sync/merge
- Manual JSONL or database edits

The coordinator runs a cycle check on **every scheduling pass** (not just startup — cycles can be created or broken mid-run):

```
┌─ Coordinator scheduling pass ──────────────────────────────┐
│                                                             │
│  1. Read all open beads and their dependencies              │
│     (already fetched by query_ready_beads)                  │
│  2. Build in-memory dependency graph                        │
│  3. Run topological sort (Kahn's algorithm)                 │
│     - If sort completes: graph is a DAG, proceed            │
│     - If nodes remain: cycles exist, report and continue    │
│  4. Filter cycled beads out of scheduling pool              │
│  5. Pass clean DAG to next_assignable_tasks()               │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

When cycles are detected:

1. **Log each cycle** with the full path (e.g., `A → B → C → A`).
2. **Exclude cycled beads** from the scheduling pool. They are treated as if they don't exist — the coordinator schedules everything else.
3. **Surface prominently** in CLI output:

```
[WARN] Dependency cycle detected — 3 beads excluded from scheduling:
       simple-agent-harness-abc → simple-agent-harness-def → simple-agent-harness-abc
       Run `bd dep cycles` to inspect, `bd dep rm` to break the cycle.
```

4. **Self-healing.** Since the check runs every scheduling pass, if the human breaks the cycle mid-run (via `bd dep rm`), the next pass picks up the now-unblocked beads automatically. No restart needed.

#### Detection Algorithm

All cycle detection is deterministic graph algorithms — no LLM reasoning, no heuristics. The dependency graph is small (tens to low hundreds of nodes), so even naive implementations are sub-millisecond.

**Data source.** The coordinator already shells out to `bd list --status=open --format=json` (see `query_ready_beads()` in `coordinator.rs`). The JSON includes a `dependencies` array per bead. We build the adjacency list from this — no additional subprocess needed.

**Two-phase approach:**

1. **Kahn's algorithm** (BFS topological sort, ~40 lines) to cheaply determine if cycles exist. Nodes remaining after the sort drains are exactly the nodes involved in cycles. O(V+E).

2. **Tarjan's SCC** (~50 lines, only runs on remaining nodes) to decompose into individual cycles for reporting. This matters when there are multiple independent cycles — the human needs to know which edges to cut.

```rust
/// Detect dependency cycles in the bead graph.
///
/// Returns a list of strongly connected components (cycles), where each
/// cycle is a list of bead IDs. Returns an empty vec if the graph is a DAG.
///
/// Phase 1: Kahn's algorithm identifies nodes involved in cycles.
/// Phase 2: Tarjan's SCC decomposes them into individual cycles.
pub fn detect_cycles(beads: &[BeadNode]) -> Vec<Vec<String>> {
    // Build adjacency list and in-degree map from beads + dependencies
    // Run Kahn's — drain nodes with in-degree 0
    // If all nodes drained: no cycles, return empty
    // Otherwise: run Tarjan's SCC on remaining subgraph
    // Return SCCs with size > 1 (self-loops are size 1)
}

/// A bead node in the dependency graph, parsed from bd JSON output.
pub struct BeadNode {
    pub id: String,
    pub depends_on: Vec<String>,  // bead IDs this node depends on
}
```

**Implementation sizing:** ~200 lines of Rust total (algorithms + wiring + CLI formatting + tests). Zero external dependencies. Pure `std` library code.

#### Impact on Time Estimation

The parallel time estimator (critical path heuristic) must handle cycles gracefully:

- **Exclude cycled beads** from the DAG before computing the critical path.
- **Report them separately** in the ETA output:

```
Progress: 18/24 beads | ETA: ~8m @ 3 workers
⚠ 3 beads stuck in dependency cycle (not included in estimate)
```

- If the cycle is broken during the run, the next estimation pass includes the freed beads.

#### CLI Surface

**`blacksmith run` output** (when cycles exist):

```
[iter 25] worker-0: beads-xyz "Add analytics module" (coding, 2.1m elapsed)
          worker-1: idle
          ─────────────────────────────────────────────────
          Progress: 18/21 schedulable beads | ETA: ~6m @ 2 workers
          ⚠ 3 beads in dependency cycle — run `bd dep cycles` to fix
```

**`blacksmith --status` output**:

```
Status: running (2 workers active)
Progress: 18/24 beads (75%)
  Schedulable: 21 beads (3 excluded — dependency cycle)
  Completed:   18 beads in 54.2m
  Remaining:   3 beads
  ⚠ Cycle:    3 beads (run `bd dep cycles`)
```

### Coordinator State (SQLite)

The coordinator stores its state in `blacksmith.db` alongside metrics tables. New tables:

```sql
-- Active and historical worker assignments
CREATE TABLE worker_assignments (
    id INTEGER PRIMARY KEY,
    worker_id INTEGER NOT NULL,          -- 0, 1, 2, ...
    bead_id TEXT NOT NULL,               -- beads issue ID
    worktree_path TEXT NOT NULL,
    status TEXT NOT NULL,                 -- coding, integrating, completed, failed
    affected_globs TEXT NOT NULL,         -- JSON array of glob strings
    started_at TEXT NOT NULL,
    completed_at TEXT,
    failure_notes TEXT
);

-- Tracks which files each task actually modified (post-hoc)
CREATE TABLE task_file_changes (
    assignment_id INTEGER NOT NULL REFERENCES worker_assignments(id),
    file_path TEXT NOT NULL,
    change_type TEXT NOT NULL             -- added, modified, deleted
);

-- Integration history for rollback/entanglement tracking
CREATE TABLE integration_log (
    id INTEGER PRIMARY KEY,
    assignment_id INTEGER NOT NULL REFERENCES worker_assignments(id),
    merged_at TEXT NOT NULL,
    merge_commit TEXT NOT NULL,           -- git commit hash on main
    manifest_entries_applied TEXT,        -- JSON array of applied entries
    cross_task_imports TEXT,              -- JSON array of imports added during integration
    reconciliation_run BOOLEAN DEFAULT 0
);

-- Per-bead timing metrics (populated on bead close or session attribution)
CREATE TABLE bead_metrics (
    bead_id TEXT PRIMARY KEY,
    sessions INTEGER NOT NULL DEFAULT 0, -- number of sessions spent on this bead
    wall_time_secs REAL NOT NULL DEFAULT 0, -- total wall clock time across sessions
    total_turns INTEGER NOT NULL DEFAULT 0,
    total_output_tokens INTEGER DEFAULT 0,
    integration_time_secs REAL DEFAULT 0,-- time spent in integration loop
    completed_at TEXT                     -- NULL if still open
);
```

---

## Task Metrics & Time Estimation

Blacksmith tracks how long each bead takes to complete and uses historical data to estimate remaining work. This works in both single-worker and multi-worker modes.

### Session-to-Bead Attribution

Every session must be linked to the bead it worked on. The attribution method depends on the execution mode.

**Multi-agent mode (explicit).** The `worker_assignments` table directly records which bead each session worked on. Attribution is free — the coordinator knows the mapping at assignment time.

**Single-agent mode (inferred).** When running in serial mode without the coordinator, blacksmith infers attribution using file metadata and git history:

1. **File timestamps.** The session output file's birth time (`ctime`) marks session start; modification time (`mtime`) marks session end. These bracket the session's time window.

2. **Git commit correlation.** Commits whose timestamps fall between `ctime` and `mtime` typically reference a bead ID in their message (e.g., `beads-abc: Implement feature`). Blacksmith scans `git log --after={ctime} --before={mtime}` and extracts bead IDs using the pattern `{project}-{id}` from commit subjects.

3. **Fallback: JSONL content.** If git correlation finds nothing, blacksmith scans the session's `result` event text for bead ID mentions.

The attributed bead ID is stored as a `session.bead_id` event during ingestion. The `bead_metrics` table is updated with cumulative timing: if a bead required multiple sessions (retries, continuation), all session durations are summed.

### Estimation Model

Blacksmith maintains running statistics on completed beads in the `bead_metrics` table. On each bead completion, it updates:

- `sessions` — how many iterations this bead consumed
- `wall_time_secs` — total wall clock time (sum of `session.duration_secs` across attributed sessions)
- `total_turns` — sum of turns across sessions
- `integration_time_secs` — time spent in the integration loop (multi-agent only)

These feed the time estimation.

#### Serial Estimate

Simple average extrapolation:

```
avg_time = sum(wall_time_secs for closed beads) / count(closed beads)
remaining_serial = count(open beads) * avg_time
```

If there are fewer than 3 completed beads, blacksmith shows "insufficient data" instead of a potentially misleading estimate.

#### Parallel Estimate

A heuristic based on the dependency graph and worker count:

```
1. Build the dependency DAG of open beads (from beads dependencies)
2. Find the critical path — the longest chain of sequentially-dependent beads
3. critical_path_time = count(beads on critical path) * avg_time
4. serial_time = count(open beads) * avg_time
5. integration_overhead = avg_integration_time * count(open beads)
6. parallel_time = max(critical_path_time, serial_time / N) + integration_overhead
```

Where `N` is `workers.max`. This is deliberately approximate — it doesn't model dynamic conflicts, scheduling contention, or failed integrations. The estimate is a lower bound on wall-clock time, not a prediction. It's useful for order-of-magnitude planning ("minutes vs hours"), not for precise ETAs.

**Accuracy notes:**
- The critical path dominates when there are deep dependency chains that can't be parallelized.
- `serial_time / N` dominates when work is mostly independent.
- `integration_overhead` accounts for the sequential integration queue being a bottleneck.
- The estimate improves as more beads complete and `avg_time` stabilizes.

### CLI Output

#### During `blacksmith run`

The status line includes progress and ETA after every session:

```
[iter 25] worker-0: beads-xyz "Add analytics module" (coding, 2.1m elapsed)
          worker-1: beads-def "Fix auth flow" (integrating)
          worker-2: idle
          ─────────────────────────────────────────────────
          Progress: 18/24 beads | ETA: ~18m serial, ~8m @ 3 workers
```

In single-worker mode:

```
[iter 12] beads-abc "Implement retry logic" completed in 3.2m (27 turns)
          Progress: 8/14 beads | avg 2.9m/bead | ETA: ~17m
```

#### `blacksmith --status`

```
Status: running (3 workers active)
Progress: 18/24 beads (75%)
  Completed:  18 beads in 54.2m (avg 3.0m/bead)
  In progress: 2 beads (worker-0: beads-xyz, worker-1: beads-def)
  Remaining:   4 beads
  ETA (serial):   ~12m
  ETA (parallel): ~6m (3 workers, 2 beads on critical path)
  Failed: 1 bead awaiting human review
```

#### `blacksmith metrics beads`

Detailed per-bead timing report:

```
Bead     Status   Sessions  WallTime  Turns  Title
─────────────────────────────────────────────────────────────────
ewc      closed        1      3.8m      49   Scaffold Rust project
utz      closed        1      3.3m      33   Implement config loading
d3v      closed        1      2.7m      29   Implement session spawning
i9s      closed        1      6.2m      53   Implement extraction rules
...
─────────────────────────────────────────────────────────────────
Total: 18 closed (54.2m), 6 open
Average: 3.0m/bead, 29 turns/bead
Fastest: 5x6 (1.6m)  Slowest: i9s (6.2m)
```

---

## CLI Changes

### New commands

```
blacksmith init                              # create .blacksmith/ directory structure
blacksmith gc [--dry-run] [--aggressive]     # manual session cleanup
blacksmith metrics reingest [--last N] [--all]  # re-extract from raw session files
blacksmith migrate --consolidate             # move V2 artifacts into .blacksmith/
blacksmith workers status                    # show worker pool state
blacksmith workers kill <worker-id>          # stop a specific worker
blacksmith integration log [--last N]        # show integration history
blacksmith integration retry <bead-id>       # retry a failed integration
blacksmith metrics beads                     # per-bead timing report
```

### Updated commands

```
blacksmith run                               # uses .blacksmith/ for all artifacts
                                             # with workers.max > 1, runs multi-agent
                                             # shows progress + ETA after each session
blacksmith --status                          # reads .blacksmith/status, shows worker states,
                                             # progress, and time estimates
```

### Adapter inspection

```
blacksmith adapter info                      # show detected/configured adapter
blacksmith adapter list                      # list available adapters
blacksmith adapter test <file>               # try parsing a file with the configured adapter
```

---

## Implementation Scope

### Milestone 5: Data Directory & Lifecycle

- Create `.blacksmith/` directory structure on init/first-run
- Move all file path construction to use `storage.data_dir`
- Remove `session.output_prefix` and `session.counter_file` config keys
- Implement `blacksmith init` command
- Implement session compression (zstd) after `compress_after` iterations
- Implement retention policy enforcement (`last-N`, `Nd`, `all`, `after-ingest`)
- Integrate cleanup into loop lifecycle (steps 2 and 8)
- Implement `blacksmith gc` command
- Implement `blacksmith migrate --consolidate` for V2->V3 file migration
- Auto-append `.blacksmith/` to `.gitignore`

### Milestone 6: Agent Adapters

- Define `AgentAdapter` trait
- Extract V2's Claude JSONL parser into `claude` adapter
- Implement `raw` adapter (no built-in metrics, pass-through for extraction rules)
- Implement adapter auto-detection from command name
- Add `adapter` config field
- Wire adapter into ingest pipeline (replaces hardcoded Claude parsing)
- Graceful degradation: skip unavailable metrics in targets/brief
- Implement `blacksmith adapter info/list/test` commands

### Milestone 7: Prompt Delivery & Additional Adapters

- Implement `prompt_via` config (`arg`, `stdin`, `file`)
- Implement `{prompt_file}` placeholder
- Implement `codex` adapter
- Implement `opencode` adapter
- Implement `aider` adapter
- Implement `blacksmith metrics reingest` command
- Add `[agent.coding]` and `[agent.integration]` config sections
- Deprecate flat `[agent]` section (falls back gracefully)

### Milestone 8: Multi-Agent Coordinator & Worktrees

- Add coordinator SQLite tables (`worker_assignments`, `task_file_changes`, `integration_log`, `bead_metrics`)
- Implement worker pool manager (spawn/track/reap workers)
- Implement git worktree lifecycle (create from main, cleanup after integration)
- Implement scheduler: read beads, check affected set overlaps, assign to idle workers
- Implement affected set parsing from bead `design` field
- Implement dynamic affected set expansion (`.blacksmith-expand` file watch)
- Implement beads lifecycle sync (in_progress on start, close on integration success)
- Implement `blacksmith workers status` and `blacksmith workers kill` commands
- Wire `workers.max` config into `blacksmith run`
- Single-worker mode (`workers.max = 1`) behaves identically to V2 serial loop
- Implement session-to-bead attribution (multi-agent: from assignment, single-agent: file metadata + git log)
- Populate `bead_metrics` table on bead close (wall time, turns, sessions, tokens)

### Milestone 8b: Dependency Cycle Detection (~200 lines, all deterministic)

- Add `BeadNode` struct and `detect_cycles()` function in new `src/cycle_detect.rs` module
  - Kahn's algorithm (~40 lines) to identify nodes in cycles
  - Tarjan's SCC (~50 lines) to decompose into individual cycles for reporting
- Parse dependency edges from `bd list --format=json` output (already fetched by `query_ready_beads`)
- Wire cycle detection into coordinator scheduling pass (runs every pass, not just startup)
- Exclude cycled beads from scheduling pool and from `next_assignable_tasks` input
- Exclude cycled beads from critical path estimation; report separately in ETA
- Surface cycle warnings in `blacksmith run` status line and `blacksmith --status` output
- Self-healing: cycles broken mid-run (via `bd dep rm`) are picked up on next pass
- **Upstream (out of scope):** File issue on beads requesting cycle prevention on `bd dep add`

### Milestone 9: Integration Loop, Reconciliation & Time Estimation

- Implement integration queue (sequential processing of completed worktrees)
- Implement merge-main-into-branch step
- Implement `task_manifest.toml` parsing and mechanical manifest application
- Spawn integration agent (`[agent.integration]`) for compiler error fixes
- Implement circuit breaker (max 3 integration fix iterations)
- Implement failure handling: bead labeling, worktree preservation, CLI error display
- Implement `HUMAN REVIEW NEEDED` prominent CLI output
- Implement periodic reconciliation (full test suite every N integrations)
- Implement `blacksmith integration log` and `blacksmith integration retry` commands
- Track cross-task imports for entanglement-aware rollback
- Implement `git revert` + manifest reversal for rollback
- Record `integration_time_secs` in `bead_metrics` after each integration
- Implement serial time estimation (avg extrapolation from `bead_metrics`)
- Implement parallel time estimation (critical path heuristic over dependency DAG)
- Add progress + ETA to `blacksmith run` status line output
- Add progress + ETA to `blacksmith --status` output
- Implement `blacksmith metrics beads` command (per-bead timing report)

---

## Migration Path

### V2 -> V3

1. User updates blacksmith binary
2. Runs `blacksmith migrate --consolidate` (one-time)
3. Old files move into `.blacksmith/`, config keys are flagged as deprecated
4. `.blacksmith/` added to `.gitignore`
5. Loop runs as before, now writing to new paths
6. Optionally add `[workers]` config to enable multi-agent mode

Deprecated config keys (`session.output_dir`, `session.output_prefix`, `session.counter_file`, flat `[agent]`) continue to work with a warning for one major version, then are removed.

### Breaking changes

- `session.output_prefix` removed (always `{N}.jsonl`)
- `session.counter_file` removed (always `.blacksmith/counter`)
- Output file path format changes (affects any external tools reading iteration files)
- Post-session hook `HARNESS_OUTPUT_FILE` env var will point to `.blacksmith/sessions/{N}.jsonl`
- `[agent]` section renamed to `[agent.coding]` (old name deprecated, not removed)

### Backwards compatibility

- `workers.max = 1` (or omitting `[workers]`) gives identical behavior to V2's serial loop. Multi-agent is opt-in.
- Beads without affected sets work in single-worker mode. Multi-worker mode treats them as "affects everything" (not parallelizable) and logs a warning.
- Projects without beads can still use `blacksmith run` in single-worker mode with the existing prompt-file workflow.
