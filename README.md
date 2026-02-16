# Blacksmith

A supervised agent harness that runs AI coding agents in a loop — dispatching prompts, monitoring sessions, enforcing health invariants, collecting metrics, and repeating.

## Install

```bash
cargo build --release
```

## Quick Start

1. Create a `PROMPT.md` with instructions for your agent.
2. Run:

```bash
blacksmith
```

With no config file, sensible defaults apply: runs `claude` for up to 25 productive iterations, monitors for stale sessions, retries empty outputs, and handles rate limits with exponential backoff.

See [docs/getting-started.md](docs/getting-started.md) for a full walkthrough.

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

## Specs

Design specifications in [`prd/`](prd/):

- [V1: Core Loop](prd/SPEC.md)
- [V2: Metrics & Institutional Memory](prd/SPEC-v2-metrics.md)
- [V3: Multi-Agent Coordination](prd/SPEC-v3-agents.md)
- [V3.5: Deployment Model](prd/SPEC-v4-deploy-self-improvement.md)
- [V5: Architecture Analysis](prd/SPEC-v5-automated-architecture-and-metadata.md)

## Testing

```bash
cargo test
```
