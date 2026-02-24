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

Blacksmith supports multiple AI agents — see [Agent Adapters](docs/adapters/overview.md) for Claude, Codex, OpenCode, Aider, and Raw adapter details.

For the full configuration reference, see [docs/reference/configuration.md](docs/reference/configuration.md).

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

**Start Here**
- [Getting Started](docs/getting-started.md) — Installation, first run, basic configuration
- [Overview](docs/overview.md) — What blacksmith is, key concepts, architecture

**Core Loop**
- [Session Lifecycle](docs/core/session-lifecycle.md) — The full iteration cycle
- [Watchdog](docs/core/watchdog.md) — Stale session detection
- [Retry & Backoff](docs/core/retry-and-backoff.md) — Empty retries and rate limits
- [Graceful Shutdown](docs/core/graceful-shutdown.md) — STOP files and signals
- [Hooks](docs/core/hooks.md) — Pre/post-session commands
- [Prompt Assembly](docs/core/prompt-assembly.md) — How prompts are built
- [Commit Detection](docs/core/commit-detection.md) — Detecting commits in output

**Multi-Agent**
- [Multi-Agent Overview](docs/multi-agent/overview.md) — Parallel execution
- [Workers & Worktrees](docs/multi-agent/workers-and-worktrees.md) — Git isolation
- [Scheduling](docs/multi-agent/scheduling.md) — Conflict-aware assignment
- [Integration](docs/multi-agent/integration.md) — Merge queue and circuit breaker
- [Task Manifests](docs/multi-agent/task-manifests.md) — Shared file changes
- [Reconciliation](docs/multi-agent/reconciliation.md) — Periodic test suite

**Metrics & Observability**
- [Metrics Overview](docs/metrics/overview.md) — Architecture and event model
- [Event Storage](docs/metrics/event-storage.md) — SQLite events and observations
- [Extraction Rules](docs/metrics/extraction-rules.md) — Custom metric derivation
- [Performance Brief](docs/metrics/performance-brief.md) — Injecting stats into prompts
- [Targets](docs/metrics/targets.md) — Performance alerting
- [Per-Bead Timing](docs/metrics/per-bead-timing.md) — Time tracking per task
- [Improvements](docs/improvements.md) — Institutional memory

**Adapters**
- [Adapters Overview](docs/adapters/overview.md) — How adapters work
- [Claude](docs/adapters/claude.md) · [Codex](docs/adapters/codex.md) · [OpenCode](docs/adapters/opencode.md) · [Aider](docs/adapters/aider.md) · [Raw](docs/adapters/raw.md)

**Architecture Analysis**
- [Analysis Overview](docs/architecture-analysis/overview.md)
- [Structural Metrics](docs/architecture-analysis/structural-metrics.md)
- [Refactor Proposals](docs/architecture-analysis/refactor-proposals.md)

**Deployment**
- [Initialization](docs/deployment/initialization.md) — `blacksmith init`
- [Quality Gates](docs/deployment/quality-gates.md) — `bd-finish.sh`
- [Self-Improvement](docs/deployment/self-improvement.md) — Feedback loops

**Dashboard**
- [Dashboard Overview](docs/dashboard/overview.md) — Multi-project monitoring
- [API Reference](docs/dashboard/api-reference.md) — HTTP endpoints
- [Smoke Testing](docs/dashboard/smoke-testing.md) — Endpoint validation

**Reference**
- [CLI Reference](docs/reference/cli.md) — All commands and flags
- [Configuration Reference](docs/reference/configuration.md) — Every config option
- [Troubleshooting](docs/troubleshooting.md) — Common issues and recovery

## Development

```bash
# Build from source
cargo build --release --workspace

# Run tests
cargo test
```
