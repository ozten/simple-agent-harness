# Getting Started

## Prerequisites

- Git repository
- [beads](https://github.com/steveyegge/beads) (`bd`) installed and configured
- An AI coding agent installed (Claude Code by default)

## Install

```bash
curl -fsSL https://raw.githubusercontent.com/ozten/blacksmith/main/scripts/install.sh | bash
```

To install a specific version:

```bash
BLACKSMITH_VERSION=0.1.0 curl -fsSL https://raw.githubusercontent.com/ozten/blacksmith/main/scripts/install.sh | bash
```

Or build from source:

```bash
cargo build --release
# Binary at target/release/blacksmith
```

## Initialize

```bash
cd your-project
blacksmith init
```

This creates the `.blacksmith/` directory with default config, prompt template, skills, and database. See [Initialization](deployment/initialization.md) for details on what gets created.

## Edit Your Prompt

Open `.blacksmith/PROMPT.md` and write instructions for your agent. This is what the agent sees each session — project context, coding conventions, what to work on.

## Validate Config

```bash
blacksmith --dry-run
```

Prints the fully resolved configuration (defaults + file + CLI overrides) without running.

## Run

```bash
blacksmith
```

Blacksmith runs `claude` by default for up to 25 productive iterations, monitoring for stale sessions, retrying empty outputs, and handling rate limits with exponential backoff.

## Check Status

From another terminal:

```bash
blacksmith --status
```

Shows whether a harness is running, current iteration, uptime, and worker status.

## View Metrics

After sessions complete:

```bash
blacksmith metrics status
```

See [Metrics Overview](metrics/overview.md) for the full metrics system.

## Stop

Three ways to stop:

| Method | Behavior |
|---|---|
| `touch STOP` | Clean exit after current session |
| `Ctrl+C` | First press finishes current session, second force-kills |
| `SIGTERM` | Same as first `Ctrl+C` |

See [Graceful Shutdown](core/graceful-shutdown.md) for details.

## Using a Different Agent

Blacksmith works with any AI coding agent. Change the command in `.blacksmith/config.toml`:

```toml
# Codex
[agent]
command = "codex"
args = ["exec", "--json", "--yolo", "{prompt}"]

# Aider
[agent]
command = "aider"
args = ["--message", "{prompt}", "--yes-always"]

# OpenCode
[agent]
command = "opencode"
args = ["run", "{prompt}"]
```

The adapter is auto-detected from the command name. See [Adapters Overview](adapters/overview.md) for details.

## Enable Multi-Agent Mode

To run multiple agents in parallel:

```toml
[workers]
max = 3
```

See [Multi-Agent Overview](multi-agent/overview.md).

## Launch the Dashboard

Optional web UI for monitoring:

```bash
# Start the API server in each project
blacksmith serve &

# Start the dashboard (once)
blacksmith-ui
```

Open http://localhost:8080. See [Dashboard Overview](dashboard/overview.md).

## Next Steps

- [Overview](overview.md) — Key concepts and architecture
- [Session Lifecycle](core/session-lifecycle.md) — How each iteration works
- [Configuration Reference](reference/configuration.md) — All config options
- [CLI Reference](reference/cli.md) — All commands and flags
