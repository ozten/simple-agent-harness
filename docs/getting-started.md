# Getting Started

## Install

```bash
cargo build --release
```

The binary is at `target/release/blacksmith`.

## First Run

1. Create a `PROMPT.md` with instructions for your agent.
2. Run:

```bash
blacksmith
```

That's it. With no config file, sensible defaults apply: runs `claude` for up to 25 productive iterations, monitors for stale sessions, retries empty outputs, and handles rate limits with exponential backoff.

## Initialize Data Directory

```bash
blacksmith init
```

Creates `.blacksmith/` with default config, prompt, skills, and database. This happens automatically on first run, but you can run it explicitly to inspect the defaults.

## Configuration

Create a `blacksmith.toml` to override defaults:

```toml
[session]
max_iterations = 10
prompt_file = "PROMPT.md"

[watchdog]
stale_timeout_mins = 15

[hooks]
pre_session = ["git pull --rebase"]
post_session = ["./notify.sh"]
```

See [Configuration Reference](configuration.md) for all options.

## Validate Config

```bash
blacksmith --dry-run
```

Prints the fully resolved configuration (defaults + file + CLI overrides) without running.

## Check Status

```bash
blacksmith --status
```

Shows whether a harness is running, current iteration, uptime, and worker status.

## Key Concepts

### Productive Iterations

Only sessions that produce meaningful output (above `min_output_bytes`) count as productive iterations toward `max_iterations`. Empty sessions trigger retries, and rate-limited sessions trigger backoff — neither counts.

### Graceful Shutdown

- `touch STOP` — exits after the current session
- `Ctrl+C` — first press finishes the current session, second press (within 3s) force-kills

### Metrics

Blacksmith automatically collects per-session metrics (turns, cost, duration). View them with:

```bash
blacksmith metrics status
```

### Multi-Agent Mode

To run multiple agents in parallel:

```toml
[workers]
max = 3
```

See [Multi-Agent Coordination](multi-agent.md).

## Using a Different Agent

Blacksmith works with any AI coding agent. Configure the command and adapter:

```toml
[agent]
command = "codex"
args = ["--prompt", "{prompt}"]
adapter = "codex"
```

See [Agent Adapters](adapters.md) for supported agents.

## Project Structure

```
your-project/
├── blacksmith.toml     # Configuration (optional)
├── PROMPT.md           # Agent instructions
├── STOP                # Touch to stop gracefully
└── .blacksmith/        # Data directory (auto-created)
    ├── blacksmith.db   # Metrics database
    ├── sessions/       # Session output files
    └── ...
```

## Next Steps

- [CLI Reference](cli-reference.md) — All commands and flags
- [Core Loop](core-loop.md) — How sessions work
- [Metrics & Improvements](metrics.md) — Track and improve agent performance
- [Configuration Reference](configuration.md) — Every config option
