# Blacksmith Documentation

A supervised agent harness that runs AI coding agents in a loop — dispatching prompts, monitoring sessions, enforcing health invariants, collecting metrics, and repeating.

## Start Here

| Document | Description |
|---|---|
| [Getting Started](getting-started.md) | Installation, first run, basic configuration |
| [Overview](overview.md) | What blacksmith is, key concepts, architecture |

## Core Loop

How a single agent session works — from prompt to commit.

| Document | Description |
|---|---|
| [Session Lifecycle](core/session-lifecycle.md) | The full iteration cycle from start to finish |
| [Watchdog](core/watchdog.md) | Monitoring output and killing stale sessions |
| [Retry & Backoff](core/retry-and-backoff.md) | Empty session retries and rate limit handling |
| [Graceful Shutdown](core/graceful-shutdown.md) | STOP files, signals, and clean exits |
| [Hooks](core/hooks.md) | Pre/post-session shell commands |
| [Prompt Assembly](core/prompt-assembly.md) | How prompts are built from commands + files |
| [Commit Detection](core/commit-detection.md) | Detecting commits in agent output |

## Multi-Agent Coordination

Running multiple agents in parallel with conflict-free scheduling.

| Document | Description |
|---|---|
| [Multi-Agent Overview](multi-agent/overview.md) | How parallel execution works |
| [Workers & Worktrees](multi-agent/workers-and-worktrees.md) | Git worktree isolation per worker |
| [Scheduling](multi-agent/scheduling.md) | Conflict-aware task assignment |
| [Integration](multi-agent/integration.md) | Sequential merge queue and circuit breaker |
| [Task Manifests](multi-agent/task-manifests.md) | Mechanical changes to shared files |
| [Reconciliation](multi-agent/reconciliation.md) | Periodic full test suite runs |

## Metrics & Observability

Collecting, querying, and acting on session data.

| Document | Description |
|---|---|
| [Metrics Overview](metrics/overview.md) | Architecture and event model |
| [Event Storage](metrics/event-storage.md) | SQLite events and observations |
| [Extraction Rules](metrics/extraction-rules.md) | Custom regex-based metric derivation |
| [Performance Brief](metrics/performance-brief.md) | Injecting session stats into prompts |
| [Targets](metrics/targets.md) | Performance alerting with streak detection |
| [Per-Bead Timing](metrics/per-bead-timing.md) | Tracking time spent per task |

## Institutional Memory

| Document | Description |
|---|---|
| [Improvements](improvements.md) | Two-speed feedback loop for process learning |

## Agent Adapters

Support for multiple AI coding agents.

| Document | Description |
|---|---|
| [Adapters Overview](adapters/overview.md) | How adapters work and what they provide |
| [Claude](adapters/claude.md) | Claude Code adapter (default) |
| [Codex](adapters/codex.md) | OpenAI Codex CLI adapter |
| [OpenCode](adapters/opencode.md) | OpenCode adapter |
| [Aider](adapters/aider.md) | Aider chat adapter |
| [Raw](adapters/raw.md) | Fallback adapter for any CLI tool |

## Architecture Analysis

Automated structural analysis and refactor proposals.

| Document | Description |
|---|---|
| [Analysis Overview](architecture-analysis/overview.md) | How architecture analysis works |
| [Structural Metrics](architecture-analysis/structural-metrics.md) | Fan-in, god files, circular deps, boundary violations |
| [Refactor Proposals](architecture-analysis/refactor-proposals.md) | Automated refactor suggestions |

## Deployment

Setting up blacksmith in any project.

| Document | Description |
|---|---|
| [Initialization](deployment/initialization.md) | `blacksmith init` and the `.blacksmith/` directory |
| [Quality Gates](deployment/quality-gates.md) | The `bd-finish.sh` script and pre-commit checks |
| [Self-Improvement](deployment/self-improvement.md) | Fast/slow feedback loops |

## Dashboard

Web-based monitoring across projects.

| Document | Description |
|---|---|
| [Dashboard Overview](dashboard/overview.md) | Multi-project monitoring UI |
| [API Reference](dashboard/api-reference.md) | Instance and dashboard HTTP endpoints |
| [Smoke Testing](dashboard/smoke-testing.md) | Automated endpoint validation |

## Reference

| Document | Description |
|---|---|
| [CLI Reference](reference/cli.md) | All commands, flags, and subcommands |
| [Configuration Reference](reference/configuration.md) | Complete `config.toml` option reference |
| [Troubleshooting](troubleshooting.md) | Common issues, error codes, recovery |

## Specs

Design specifications live in [`/prd/`](../prd/):

- [SPEC.md](../prd/SPEC.md) — V1: Core loop
- [SPEC-v2-metrics.md](../prd/SPEC-v2-metrics.md) — V2: Metrics & institutional memory
- [SPEC-v3-agents.md](../prd/SPEC-v3-agents.md) — V3: Multi-agent coordination & storage
- [SPEC-v4-deploy-self-improvement.md](../prd/SPEC-v4-deploy-self-improvement.md) — V3.5: Deployment model
- [SPEC-v5-automated-architecture-and-metadata.md](../prd/SPEC-v5-automated-architecture-and-metadata.md) — V5: Architecture analysis
