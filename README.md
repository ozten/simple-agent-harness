# Blacksmith

> [!NOTE]
> Blacksmith is under active development and the API surface is not yet stable. We're iterating quickly — expect breaking changes in early releases. That said, it's fully functional today and we'd love for you to try it out.

A supervised agent harness that runs AI coding agents in a loop — dispatching prompts, monitoring sessions, enforcing health invariants, collecting metrics, and repeating.

Currently, blacksmith depends on your using `bd` to record tasks you would like accomplished.

beads - https://github.com/steveyegge/beads

`blacksmith init` currently depends on `claude` existing and being setup.

## Install

```bash
curl -fsSL https://raw.githubusercontent.com/ozten/blacksmith/main/scripts/install.sh | bash
```

To install a specific version:

```bash
BLACKSMITH_VERSION=0.1.0 curl -fsSL https://raw.githubusercontent.com/ozten/blacksmith/main/scripts/install.sh | bash
```

## Quick Start

Initialize blacksmith in your project:

```bash
cd your-project
blacksmith init
```

This creates a `.blacksmith/` directory with a default `config.toml` and a `PROMPT.md` template. Edit `PROMPT.md` with instructions for your agent, then start the loop:

```bash
blacksmith
```

Blacksmith runs `claude` by default for up to 25 productive iterations, monitoring for stale sessions, retrying empty outputs, and handling rate limits with exponential backoff.

See [docs/getting-started.md](docs/getting-started.md) for a full walkthrough.

## Configuration

Edit `.blacksmith/config.toml` to customize behavior:

```toml
[agent]
command = "claude"           # Or "codex", "aider", "opencode", etc.

[workers]
max = 3                      # Concurrent workers (1 = serial mode)

[session]
max_iterations = 25
```

Blacksmith supports multiple AI agents — see [Agent Adapters](docs/adapters.md) for Claude, Codex, OpenCode, Aider, and Raw adapter details.

For the full configuration reference, see [docs/configuration.md](docs/configuration.md).

## Dashboard

Launch a metrics server in each project directory:

```bash
blacksmith serve &
```

Then launch the multi-project dashboard once:

```bash
blacksmith-ui
```

This opens a browser dashboard that monitors all running blacksmith projects — workers, progress, metrics, and session health — from a single view.

## Features

- **Supervised loop** — session lifecycle, watchdog, retry, exponential backoff, graceful shutdown
- **Multi-agent** — parallel workers in git worktrees with conflict-aware scheduling and sequential integration
- **Metrics** — per-session event storage, custom extraction rules, performance briefs, targets
- **Institutional memory** — improvement tracking with two-speed feedback (DB → prompt promotion)
- **Agent adapters** — Claude, Codex, OpenCode, Aider, Raw — with graceful metric degradation
- **Architecture analysis** — fan-in detection, god files, circular deps, automated refactor proposals
- **Deployment model** — embedded defaults, `blacksmith init`, quality-gated `bd-finish.sh`
- **Language-agnostic** — configurable quality gates for Rust, TypeScript, Go, Python, etc.

## Documentation

Full documentation lives in [`docs/`](docs/README.md):

- [Getting Started](docs/getting-started.md)
- [CLI Reference](docs/cli-reference.md)
- [Configuration Reference](docs/configuration.md)
- [Core Loop](docs/core-loop.md)
- [Metrics & Improvements](docs/metrics.md)
- [Agent Adapters](docs/adapters.md)
- [Multi-Agent Coordination](docs/multi-agent.md)
- [Deployment Model](docs/deployment.md)
- [Architecture Analysis](docs/architecture-analysis.md)
- [Troubleshooting](docs/troubleshooting.md)

## Development

```bash
# Build from source
cargo build --release --workspace

# Run tests
cargo test
```
