# Metrics & Improvements (V2)

Blacksmith collects per-session metrics and maintains an institutional memory of process improvements.

## Architecture

```
Session JSONL → Adapter Parser → Events (SQLite) → Observations (materialized)
                                                  → Brief (injected into prompt)
                                                  → Targets (performance alerts)
```

## Event Storage

Events are stored in an append-only SQLite table:

```sql
events(id, ts, session, kind, value, tags)
```

Events use a convention-based `kind` namespace:

| Kind | Description |
|---|---|
| `session.start` | Session began |
| `session.end` | Session ended |
| `session.outcome` | productive / empty / rate_limited |
| `session.duration_secs` | Wall-clock session time |
| `session.output_bytes` | Bytes written to output file |
| `session.exit_code` | Agent process exit code |
| `turns.total` | Total agent turns |
| `turns.narration_only` | Turns with no tool use |
| `turns.parallel` | Turns with parallel tool calls |
| `turns.tool_calls` | Total tool call count |
| `cost.input_tokens` | Input token count |
| `cost.output_tokens` | Output token count |
| `cost.estimate_usd` | Estimated session cost |
| `commit.detected` | Commit patterns found in output |
| `extract.*` | Custom extraction rule results |

## Observations

Observations are a materialized view of per-session summaries:

```sql
observations(session, ts, duration, outcome, data)
```

The `data` column is a JSON object containing all metrics for that session. Observations are rebuildable from events via `blacksmith metrics rebuild`.

## Metric Extraction

### Built-in (Adapter)

Each agent adapter extracts metrics natively from session output. See [Agent Adapters](adapters.md) for per-adapter capabilities.

### Custom Rules

Define extraction rules in config to derive additional metrics:

```toml
[[metrics.extract.rules]]
kind = "extract.test_runs"
pattern = 'cargo test'
source = "tool_commands"
count = true                  # Emit count of matches
```

See [Configuration — metrics.extract](configuration.md#metricsextract) for all rule options.

## Performance Brief

The brief system generates a markdown summary injected into agent prompts via `prepend_commands`:

```toml
[prompt]
prepend_commands = ["blacksmith brief"]
```

The brief includes:
- Last N sessions' key metrics (turns, cost, duration)
- Target compliance status (pass/miss/streak alerts)
- Open improvements the agent should act on

Metrics unavailable for the configured adapter are automatically excluded from the brief.

## Targets

Define performance targets to track over time:

```toml
[metrics.targets]
streak_threshold = 3

[[metrics.targets.rules]]
kind = "turns.narration_only"
compare = "pct_of"
relative_to = "turns.total"
threshold = 20
direction = "below"
label = "Narration-only turns"
unit = "%"
```

**Comparison modes:**
- `pct_of` — Value as percentage of another metric
- `pct_sessions` — Percentage of sessions where metric is present
- `avg` — Average value across sessions

Check targets with `blacksmith metrics targets`.

## Improvements (Institutional Memory)

Improvements are process observations stored in the database — things the agent or operator noticed that could make sessions more effective.

### Lifecycle

```
open → promoted (proven useful, injected into brief)
     → dismissed (not useful, with optional reason)
     → revisit (needs more data)
     → validated (confirmed working)
```

### Two-Speed Feedback

1. **Fast loop** — Database improvements shown in the performance brief each session. Agent sees them immediately.
2. **Slow loop** — Promoted improvements can be manually added to PROMPT.md for permanent inclusion.

### Commands

```bash
# Add
blacksmith improve add "Use parallel tool calls for independent reads" \
  --category workflow \
  --context "Sessions 140-145 all sequential"

# List/filter
blacksmith improve list --status open

# Promote when proven
blacksmith improve promote R3

# Dismiss when disproven
blacksmith improve dismiss R5 --reason "No measurable impact"

# Search
blacksmith improve search "parallel"
```

## Automatic Ingestion

After each session, blacksmith automatically parses the output file using the configured adapter and stores events in the database. The `metrics log` command can also manually ingest files.

## Re-ingestion

When extraction rules change, re-ingest historical sessions to apply new rules:

```bash
blacksmith metrics reingest --last 20   # Recent sessions
blacksmith metrics reingest --all       # Everything
```

## Per-Bead Timing

Track time spent per bead across multiple sessions:

```bash
blacksmith metrics beads
```

Shows sessions, wall time, total turns, output tokens, and integration time per bead.
