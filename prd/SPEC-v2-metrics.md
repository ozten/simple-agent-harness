# Blacksmith V2 — Evolvable Metrics & Continuous Improvement

Replaces the rigid `self-improvement` Python tool with an evolvable metrics and institutional memory system built into `blacksmith` (renamed from `simple-agent-harness`).

## Rename: simple-agent-harness → blacksmith

The project, binary, and all references are renamed:

- Cargo package name: `blacksmith`
- Binary: `blacksmith`
- Config file: `blacksmith.toml` (falls back to `harness.toml` for backwards compat)
- Status file: `blacksmith.status`
- CLI help/version strings: `blacksmith`

The `simple-agent-harness` name was a placeholder. `blacksmith` is the permanent name.

---

## Problem

The current system has three evolvability failures:

1. **Fixed-column sessions table.** Every new metric requires `ALTER TABLE`, updating INSERT/UPDATE/SELECT queries, updating the parser, updating the dashboard, and updating the brief generator. Adding "context window utilization" today would touch 6 functions across 200 lines.

2. **Hardcoded extraction rules.** The parser greps for `phpunit`, `lint:fix`, `bd-finish` — all specific to one project. Moving to a different project means rewriting the parser.

3. **Enum constraints baked into schema.** Severity, status, and outcome are CHECK-constrained strings. Adding a new category means a migration, which SQLite doesn't handle gracefully.

And one architectural gap:

4. **No institutional memory.** Qualitative observations — "we should use a linter", "parallel tool calls reduce turn count" — have nowhere to live persistently. Low-priority ideas get forgotten and rediscovered. The system tracks *what happened* but not *what we learned*.

## Design Principles

- **Qualitative first**: The primary purpose is continuous improvement — noticing patterns, recording observations, tracking whether ideas helped. Quantitative metrics serve this qualitative loop.
- **Schema-last**: The system accepts arbitrary key-value metrics. Schema (types, thresholds, display rules) is defined in config, not in the database.
- **Extract-via-config**: What to look for in session output is defined in pattern rules, not in code.
- **Append-only core**: The fundamental storage is an append-only event log. Materialized views and aggregations are derived and rebuildable.
- **Project-agnostic**: Blacksmith knows nothing about WordPress, PHPUnit, or beads. It knows about "sessions that produce output files."

---

## Storage Model

All tables live in a single SQLite database (`blacksmith.db` in the output directory).

### Improvements Table (institutional memory)

The core of the continuous improvement system. Records qualitative observations, process improvement ideas, and lessons learned. This is *not* a task tracker — actionable work graduates to beads. This is where "we noticed X" lives so we don't rediscover it.

```sql
CREATE TABLE improvements (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    ref        TEXT UNIQUE,              -- human-friendly ID: R1, R15, etc.
    created    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    resolved   TEXT,                     -- timestamp when promoted/dismissed
    category   TEXT NOT NULL,            -- free-form: code-quality, workflow, cost, reliability, performance
    status     TEXT NOT NULL DEFAULT 'open',
    title      TEXT NOT NULL,
    body       TEXT,                     -- markdown description
    context    TEXT,                     -- evidence: "sessions 340-348 showed high narration turns"
    tags       TEXT,                     -- comma-separated
    meta       TEXT                      -- JSON blob for anything else
);

CREATE INDEX idx_improvements_status ON improvements(status);
CREATE INDEX idx_improvements_category ON improvements(category);
```

**Status values** (convention, not constraint):
- `open` — observed, not yet acted on
- `promoted` — graduated to a bead / actionable task
- `dismissed` — considered and rejected (with reason in `meta`)
- `revisit` — not now, but check back later
- `validated` — implemented and confirmed effective

**No CHECK constraints.** The CLI validates against a configurable set of allowed values, but the database accepts anything. New statuses and categories can be added without schema changes.

The `context` field links an observation to its evidence — session numbers, metric trends, specific incidents. This lets future sessions verify whether an improvement actually helped.

### Events Table (append-only)

The single source of truth for quantitative data. One row per significant occurrence.

```sql
CREATE TABLE events (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    ts        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    session   INTEGER NOT NULL,          -- global iteration number
    kind      TEXT NOT NULL,             -- event type (free-form, dotted namespace)
    value     TEXT,                      -- JSON value (number, string, object, array)
    tags      TEXT                       -- comma-separated free-form tags
);

CREATE INDEX idx_events_session ON events(session);
CREATE INDEX idx_events_kind ON events(kind);
CREATE INDEX idx_events_ts ON events(ts);
```

**No enums. No CHECK constraints.** The `kind` field is a free-form dotted string. Convention, not enforcement.

### Event Kinds (conventions, not schema)

Namespace pattern: `category.metric`

```
session.start              value: {"prompt_file": "PROMPT.md", "config": {...}}
session.end                value: {"exit_code": 0, "duration_secs": 1847, "output_bytes": 128456}
session.outcome            value: "completed" | "cutoff" | "failed" | "timeout" | "empty" | "rate_limited"
session.retry              value: {"attempt": 2, "reason": "empty", "prev_bytes": 0}

turns.total                value: 67
turns.narration_only       value: 4
turns.parallel             value: 8
turns.tool_calls           value: 142

cost.input_tokens          value: 1450000
cost.output_tokens         value: 38000
cost.estimate_usd          value: 24.57

watchdog.check             value: {"stale_secs": 60, "output_bytes": 48200, "growing": true}
watchdog.kill              value: {"stale_mins": 20, "final_bytes": 48200}

commit.detected            value: {"method": "bd-finish", "message": "FORM-01c: ..."}
commit.none                value: {"reason": "cutoff"}

# Project-specific (extracted via configurable patterns)
extract.test_runs          value: 3
extract.lint_runs          value: 1
extract.bead_id            value: "udgd"
```

New metrics appear by emitting new event kinds. No migration needed.

### Observations Table (derived, rebuildable)

A materialized per-session summary for fast dashboard queries. Rebuilt from events on demand.

```sql
CREATE TABLE observations (
    session   INTEGER PRIMARY KEY,       -- global iteration number
    ts        TEXT NOT NULL,
    duration  INTEGER,                   -- seconds
    outcome   TEXT,
    data      TEXT NOT NULL              -- JSON object: all metrics for this session
);
```

The `data` column is a JSON object assembled from all events for that session:

```json
{
  "turns.total": 67,
  "turns.narration_only": 4,
  "turns.parallel": 8,
  "cost.estimate_usd": 24.57,
  "session.duration_secs": 1847,
  "commit.detected": true
}
```

Rebuild command: `blacksmith metrics rebuild` — drops and recreates from `events`.

---

## Loop Integration

Metrics and improvements are first-class parts of the blacksmith session loop, not optional hook configurations. The harness handles these automatically.

### Pre-session: Brief Injection

Before each session, blacksmith automatically generates the brief and prepends it to the agent's prompt. This is built into the prompt assembly step — not a `prepend_commands` entry.

The sequence:
1. Run `prepend_commands` (user-configured, optional)
2. **Generate brief** (built-in, always runs when `blacksmith.db` exists)
3. Read prompt file
4. Assemble: `[prepend output]\n---\n[brief]\n---\n[prompt file]`

The brief includes open improvements and (once Milestone 2+ lands) quantitative target status. This means every session starts with institutional context — the agent knows what patterns have been observed, what's been tried, and what to watch for.

If no database exists yet (first run), the brief step is silently skipped.

### Post-session: Automatic Ingestion

After each session completes (and passes retry evaluation), blacksmith automatically ingests the session's JSONL output file into the database. This is built into the loop — not a post-session hook.

The sequence:
1. Session completes, retry logic decides Proceed or Skip
2. **Ingest session output** (built-in, always runs when metrics are configured)
   - Extract built-in metrics (turns, cost, duration)
   - Run configurable extraction rules
   - Write events to `events` table
   - Update `observations` table
3. Run post-session hooks (user-configured, optional)

Post-session hooks receive `HARNESS_SESSION_NUMBER` so they can query the database for the just-ingested session data if needed.

### Agent-side: Creating Improvements

The agent creates improvements by calling `blacksmith improve add` during sessions. This is not enforced by the harness — it depends on prompt instructions. The recommended pattern is to include guidance in `PROMPT.md`:

```markdown
## Continuous Improvement

When you notice a recurring pattern, inefficiency, or potential process improvement:
1. Check existing improvements: `blacksmith improve list`
2. If it's new, record it: `blacksmith improve add "description" --category <cat>`
3. If you're acting on one, note it: `blacksmith improve update <ref> --status promoted`
```

The agent has access to `blacksmith improve` as a regular CLI tool during its session. The brief injection ensures it sees existing improvements at the start of every session without needing to query manually.

### Loop Lifecycle Summary

```
┌─ Loop iteration ──────────────────────────────────────────────┐
│                                                               │
│  1. Check STOP file / signals                                 │
│  2. Assemble prompt                                           │
│     a. Run prepend_commands              (user-configured)    │
│     b. Generate brief from blacksmith.db (built-in)           │
│     c. Read prompt file                                       │
│  3. Run pre-session hooks                (user-configured)    │
│  4. Spawn agent session + watchdog                            │
│  5. Evaluate output (retry logic)                             │
│  6. Ingest session JSONL into database   (built-in)           │
│  7. Check rate limit + backoff                                │
│  8. Run post-session hooks               (user-configured)    │
│  9. Save iteration counter                                    │
│                                                               │
└───────────────────────────────────────────────────────────────┘
```

Steps 2b and 6 are the integration points. They're always active when `blacksmith.db` is present and require no user configuration.

---

## Configurable Extraction Rules

The parser is driven by rules in `blacksmith.toml`, not hardcoded logic.

```toml
[metrics.extract]
# Each rule: scan session output for a pattern, emit an event

[[metrics.extract.rules]]
kind = "extract.bead_id"
pattern = 'bd update (\S+) --status.?in.?progress'
transform = "last_segment"   # split on "-", take last segment
first_match = true           # stop after first match

[[metrics.extract.rules]]
kind = "commit.detected"
pattern = 'bd-finish'
emit = true                  # emit boolean true if pattern found anywhere

[[metrics.extract.rules]]
kind = "extract.test_runs"
pattern = "phpunit"
source = "tool_commands"     # only scan tool_use command fields
count = true                 # emit count of matches, not the match itself

[[metrics.extract.rules]]
kind = "extract.full_suite_runs"
pattern = "phpunit"
anti_pattern = "--filter"    # only count matches that DON'T also match this
source = "tool_commands"
count = true

[[metrics.extract.rules]]
kind = "extract.lint_runs"
pattern = "lint:fix|lint.*composer"
source = "tool_commands"
count = true

[[metrics.extract.rules]]
kind = "extract.file_reads"
pattern = '"name":\s*"Read"'
source = "raw"               # scan raw JSONL lines
count = true
```

**Rule fields:**
- `kind` — event kind to emit
- `pattern` — regex to search for
- `anti_pattern` — exclude matches that also match this (optional)
- `source` — where to search: `tool_commands` (tool_use input.command fields), `text` (assistant text blocks), `raw` (raw JSONL lines). Default: `tool_commands`.
- `transform` — post-processing: `last_segment` (split on `-`, take last), `int` (parse as integer), `trim` (strip whitespace). Default: raw capture group.
- `first_match` — stop after first match, emit the value. Default: false.
- `count` — emit count of matches instead of match content. Default: false.
- `emit` — emit a fixed value (true/false/string) if pattern is found. Default: not set.

Adding a new metric = adding 3-4 lines of TOML. No code changes.

### Built-in Metrics (not configurable)

Always extracted because they come from the JSONL structure, not pattern matching:

- `turns.total` — count of `type: "assistant"` events
- `turns.narration_only` — assistant events with text blocks but no tool_use blocks
- `turns.parallel` — assistant events with 2+ tool_use blocks
- `turns.tool_calls` — total tool_use blocks across all assistant events
- `cost.input_tokens` / `cost.output_tokens` / `cost.estimate_usd` — from usage data
- `session.duration_secs` — wall clock time (from harness, not JSONL)
- `session.output_bytes` — output file size
- `session.exit_code` — agent process exit code

---

## Targets & Thresholds (configurable)

Replace hardcoded thresholds with config:

```toml
[[metrics.targets.rules]]
kind = "turns.narration_only"
compare = "pct_of"           # compute as percentage of another metric
relative_to = "turns.total"
threshold = 20
direction = "below"          # "below" = good when under threshold
label = "Narration-only turns"
unit = "%"

[[metrics.targets.rules]]
kind = "turns.parallel"
compare = "pct_of"
relative_to = "turns.total"
threshold = 10
direction = "above"
label = "Parallel tool calls"
unit = "%"

[[metrics.targets.rules]]
kind = "commit.detected"
compare = "pct_sessions"     # percentage of sessions where this event exists
threshold = 85
direction = "above"
label = "Completion rate"
unit = "%"

[[metrics.targets.rules]]
kind = "turns.total"
compare = "avg"
threshold = 80
direction = "below"
label = "Avg turns per session"

[[metrics.targets.rules]]
kind = "cost.estimate_usd"
compare = "avg"
threshold = 30
direction = "below"
label = "Avg cost per session"
unit = "$"
```

The dashboard and brief generator iterate over these rules dynamically. Adding a target = adding TOML. Removing one = deleting TOML. No code changes.

---

## CLI Interface

### Improvement commands (institutional memory)

```
blacksmith improve add <title> --category <cat> [--body "..."] [--context "sessions 340-348"] [--tags "tag1,tag2"]
blacksmith improve list [--status open|promoted|dismissed|revisit|validated] [--category <cat>]
blacksmith improve show <ref>
blacksmith improve update <ref> --status <status> [--body "..."] [--context "..."]
blacksmith improve promote <ref>          # sets status=promoted, records timestamp
blacksmith improve dismiss <ref> [--reason "..."]
blacksmith improve search <query>         # full-text search across title, body, context
```

### Metrics commands

```
blacksmith metrics log <file>              # Parse JSONL file, emit events, update observations
blacksmith metrics status [--last N]       # Dashboard: recent sessions, target status, trends
blacksmith metrics brief [--last N]        # Performance snippet for prompt injection
blacksmith metrics targets                 # Show configured targets vs recent performance
blacksmith metrics query <kind> [--last N] [--aggregate avg|trend]
blacksmith metrics events [--session N]    # Dump raw events
blacksmith metrics rebuild                 # Rebuild observations from events
blacksmith metrics export [--format csv|json]
blacksmith metrics migrate --from <path>   # Import V1 self-improvement.db
```

### Brief command output

Includes both quantitative targets and qualitative context:

```
## PERFORMANCE FEEDBACK (auto-generated)

Last session #348:
  Narration-only turns: 6% (target: <20%) — OK
  Parallel tool calls:  12% (target: >10%) — OK
  Avg turns: 67 (target: <80) — OK
  Lint runs: 1 (target: <2) — OK

All targets met.

## OPEN IMPROVEMENTS (3 of 7)

R12 [code-quality] Use ESLint --fix in pre-commit hook to reduce lint iterations
R15 [workflow] Batch file reads when exploring unfamiliar directories
R18 [cost] Skip full test suite when only touching CSS files
```

The brief surfaces the most relevant open improvements so the agent has institutional context without manual prompting.

---

## Migration from V1

The V1 `self-improvement.db` has ~350 session rows. Migration path:

1. Read all rows from V1 `sessions` table
2. For each row, emit events into the new `events` table:
   - `turns.total` = `assistant_turns`
   - `turns.narration_only` = `narration_only_turns`
   - `turns.parallel` = `parallel_turns`
   - `turns.tool_calls` = `tool_calls`
   - `cost.estimate_usd` = `cost_estimate`
   - `extract.test_runs` = `test_runs`
   - `extract.lint_runs` = `lint_runs`
   - `extract.full_suite_runs` = `full_suite_runs`
   - `extract.bead_id` = `bead_id`
   - `extract.bead_title` = `bead_title`
   - `session.outcome` = `outcome`
   - `commit.detected` = `committed` (as boolean)
3. Rebuild `observations` table
4. Copy `improvements` rows (map `severity` → `category`, `status` values remain compatible)

Command: `blacksmith metrics migrate --from tools/self-improvement.db`

---

## Implementation Scope

All features are part of the `blacksmith` Rust binary.

### Milestone 0: Rename

- Rename Cargo package to `blacksmith`
- Rename binary to `blacksmith`
- Config file: `blacksmith.toml` (fall back to `harness.toml`)
- Status file: `blacksmith.status`
- Update all log messages, help text, version strings

### Milestone 1: Institutional Memory + Loop Integration

- SQLite database setup (`blacksmith.db`)
- `improvements` table creation
- `improve add/list/show/update/promote/dismiss/search` commands
- Auto-assign `ref` (R1, R2, ...) on creation
- `brief` command generates improvement context
- **Loop: pre-session brief injection** — brief output prepended to prompt automatically
- Brief silently skipped when no database exists

### Milestone 2: Session Metrics + Automatic Ingestion

- `events` table + `observations` table creation
- Built-in metric extraction from JSONL (turns, cost, duration)
- **Loop: post-session automatic ingestion** — JSONL parsed and written to database after each session
- `metrics log` command (manual ingestion for ad-hoc / historical files)
- `metrics status` command (dashboard from observations)
- `brief` command adds quantitative summary alongside improvements

### Milestone 3: Configurable Extraction & Targets

- `[metrics.extract]` rules in `blacksmith.toml`
- `[metrics.targets]` thresholds in `blacksmith.toml`
- `brief` auto-generates warnings from target misses
- `metrics targets` command
- Streak detection: escalate warnings after N consecutive misses (configurable)

### Milestone 4: Migration & Polish

- `metrics migrate` command for V1 data
- `metrics rebuild` command
- `metrics export` command
- `metrics query` command with aggregation
- `metrics events` dump command
