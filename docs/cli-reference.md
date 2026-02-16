# CLI Reference

## Main Command

```
blacksmith [OPTIONS] [MAX_ITERATIONS] [COMMAND]
```

### Arguments

| Argument | Description |
|---|---|
| `MAX_ITERATIONS` | Override max productive iterations (default: from config) |

### Options

| Flag | Description |
|---|---|
| `-c, --config <PATH>` | Config file path (default: `.blacksmith/config.toml`) |
| `-p, --prompt <PATH>` | Prompt file (overrides config) |
| `-o, --output-dir <PATH>` | Output directory (deprecated) |
| `--timeout <MINUTES>` | Stale timeout in minutes (overrides config) |
| `--retries <N>` | Max empty retries (overrides config) |
| `--dry-run` | Validate config and print resolved settings, don't run |
| `-v, --verbose` | Debug-level logging |
| `-q, --quiet` | Suppress per-iteration banners |
| `--status` | Print current loop state and exit |

**Precedence:** Defaults < Config file < CLI flags

### Examples

```bash
# Run with defaults
blacksmith

# Run 10 iterations with verbose logging
blacksmith -v 10

# Custom config and prompt
blacksmith -c my-config.toml -p my-prompt.md

# Validate config without running
blacksmith --dry-run

# Check if a harness is running
blacksmith --status
```

## Subcommands

### `init`

Initialize the `.blacksmith/` data directory.

```bash
blacksmith init
```

Creates the directory structure, database, and default files if they don't exist.

### `brief`

Generate a performance brief for prompt injection.

```bash
blacksmith brief
```

Outputs a markdown summary of recent session performance and open improvements, suitable for prepending to agent prompts.

### `improve`

Manage improvement records (institutional memory).

```bash
# Add a new improvement
blacksmith improve add "Reduce narration-only turns" \
  --category workflow \
  --body "Agent spends too many turns narrating instead of coding" \
  --context "Sessions 340-348 showed >30% narration" \
  --tags "narration,efficiency"

# List improvements
blacksmith improve list
blacksmith improve list --status open
blacksmith improve list --category cost

# Show details
blacksmith improve show R1

# Update fields
blacksmith improve update R1 --status validated --body "Confirmed fix works"

# Promote (shorthand for --status=promoted)
blacksmith improve promote R1

# Dismiss
blacksmith improve dismiss R2 --reason "No longer relevant"

# Search
blacksmith improve search "narration"
```

**Improvement statuses:** `open`, `promoted`, `dismissed`, `revisit`, `validated`

**Categories:** `workflow`, `cost`, `reliability`, `performance`, `code-quality` (convention, not enforced)

### `metrics`

View and manage session metrics.

```bash
# Ingest a session file
blacksmith metrics log path/to/session.jsonl

# Dashboard of recent sessions
blacksmith metrics status
blacksmith metrics status --last 20

# Check targets vs performance
blacksmith metrics targets
blacksmith metrics targets --last 20

# Query specific metric
blacksmith metrics query turns.total --last 10
blacksmith metrics query cost.estimate_usd --aggregate avg

# Dump raw events
blacksmith metrics events
blacksmith metrics events --session 142

# Re-ingest after changing extraction rules
blacksmith metrics reingest --last 10
blacksmith metrics reingest --all

# Rebuild observations from events
blacksmith metrics rebuild

# Export observations
blacksmith metrics export
blacksmith metrics export --format csv

# Migrate from V1 database
blacksmith metrics migrate --from old-self-improvement.db

# Per-bead timing report
blacksmith metrics beads
```

### `gc`

Run session file cleanup (compression + retention).

```bash
blacksmith gc              # Apply retention policy and compress old sessions
blacksmith gc --dry-run    # Show what would be cleaned
blacksmith gc --aggressive # Compress all eligible + delete beyond retention
```

### `migrate`

Migrate legacy V2 files into `.blacksmith/` structure.

```bash
blacksmith migrate --consolidate
```

Moves `claude-iteration-*.jsonl`, `.iteration_counter`, and database files into the `.blacksmith/` directory.

### `workers`

Manage concurrent agent workers.

```bash
# Show worker pool state
blacksmith workers status

# Kill a specific worker
blacksmith workers kill 2
```

### `integration`

View integration history and manage failed integrations.

```bash
# Show integration history
blacksmith integration log
blacksmith integration log --last 5

# Retry a failed integration
blacksmith integration retry beads-abc

# Rollback an integrated task
blacksmith integration rollback beads-abc
blacksmith integration rollback beads-abc --force  # Even if entangled
```

### `estimate`

Show time estimates for completing remaining beads.

```bash
blacksmith estimate             # Uses configured workers.max
blacksmith estimate --workers 4 # Override worker count
```

Computes serial and parallel ETAs using the dependency DAG and historical session timing.

### `adapter`

Inspect and test agent adapters.

```bash
# Show detected/configured adapter
blacksmith adapter info

# List all adapters with supported metrics
blacksmith adapter list

# Parse a file and show extracted metrics
blacksmith adapter test path/to/session.jsonl
```

### `arch`

Run architecture analysis on the codebase.

```bash
# Human-readable structural analysis
blacksmith arch

# JSON output for programmatic consumption
blacksmith arch --json
```

Computes structural metrics (fan-in hotspots, god files, circular dependencies, boundary violations, API surface width) and correlates them with historical operational signals from the metrics database if available. See [Architecture Analysis](architecture-analysis.md).
