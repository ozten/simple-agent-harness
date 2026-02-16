# Blacksmith Documentation

A supervised agent harness that runs AI coding agents in a loop — dispatching prompts, monitoring sessions, enforcing health invariants, collecting metrics, and repeating.

## Quick Navigation

| Document | Description |
|---|---|
| [Getting Started](getting-started.md) | Installation, first run, basic configuration |
| [CLI Reference](cli-reference.md) | All commands, flags, and subcommands |
| [Configuration Reference](configuration.md) | Complete `blacksmith.toml` option reference |
| [Core Loop](core-loop.md) | Session lifecycle, watchdog, retry, backoff, hooks |
| [Metrics & Improvements](metrics.md) | Event storage, extraction rules, briefs, improvements |
| [Agent Adapters](adapters.md) | Claude, Codex, OpenCode, Aider, Raw — setup and capabilities |
| [Multi-Agent Coordination](multi-agent.md) | Workers, scheduling, integration, manifests |
| [Deployment Model](deployment.md) | Embedded defaults, `init`, `finish`, self-improvement |
| [Architecture Analysis](architecture-analysis.md) | Intent analysis, structural metrics, refactor proposals |
| [Troubleshooting](troubleshooting.md) | Common issues, error codes, recovery procedures |

## Specs

Design specifications live in [`/prd/`](../prd/):

- [SPEC.md](../prd/SPEC.md) — V1: Core loop
- [SPEC-v2-metrics.md](../prd/SPEC-v2-metrics.md) — V2: Metrics & institutional memory
- [SPEC-v3-agents.md](../prd/SPEC-v3-agents.md) — V3: Multi-agent coordination & storage
- [SPEC-v4-deploy-self-improvement.md](../prd/SPEC-v4-deploy-self-improvement.md) — V3.5: Deployment model
- [SPEC-v5-automated-architecture-and-metadata.md](../prd/SPEC-v5-automated-architecture-and-metadata.md) — V5: Architecture analysis
