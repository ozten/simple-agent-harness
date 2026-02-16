# Architecture Analysis (V5)

Blacksmith includes an automated architecture analysis system that detects structural issues, correlates historical signals, and proposes refactors.

## Two-Phase Task Metadata

### Layer 1: Intent Analysis (Slow, Stable)

An LLM reads the issue description and produces a semantic understanding of affected areas — concepts, not file paths. Results are cached against the issue content hash and survive refactors.

### Layer 2: File Resolution (Fast, Volatile)

Static analysis maps concepts from Layer 1 to concrete files at the current HEAD. Results are cached against the commit hash and regenerate when main advances.

This two-layer approach means metadata doesn't break when files move, and file resolution stays current as the codebase evolves.

## Structural Metrics

The architecture agent computes:

| Metric | Description |
|---|---|
| **Fan-in** | Files imported by a high percentage of modules. Threshold: `architecture.fan_in_threshold` |
| **God files** | Large files with low cohesion (many unrelated responsibilities) |
| **Circular dependencies** | Modules that form import cycles |
| **Boundary violations** | Non-public imports crossing module boundaries |
| **API surface width** | Public API surface area per module |

## Historical Signal Correlation

The architecture agent analyzes patterns in blacksmith's own operational data:

| Signal | What It Indicates |
|---|---|
| **Affected-set expansions** | Prediction misses — task touched more files than expected |
| **Integration loop iterations** | Interface clarity — more iterations = harder to integrate |
| **Entangled rollbacks** | Missing dependency edges between tasks |
| **Metadata drift** | Boundary expansion over time — modules growing beyond their intended scope |

## Configuration

```toml
[architecture]
fan_in_threshold = 0.3              # Fan-in ratio triggering refactor candidates
integration_loop_max = 5            # Max fix iterations before circuit breaker
expansion_event_threshold = 5       # Expansions within window triggering review
expansion_event_window = 20         # Window size (in tasks)
metadata_drift_sensitivity = 3.0    # Drift multiplier triggering alert
refactor_auto_approve = false       # Auto-execute proposals without human review
```

## Refactor Proposals

When issues are detected, the architecture agent generates proposals:

1. **Splitting recommendations** — Which module to split, where to draw boundaries
2. **Evidence** — Structural metrics + historical signals supporting the proposal
3. **Validation** — Dependency feasibility, API preservation, test coverage analysis
4. **Execution** — Auto-execute if `refactor_auto_approve = true`, otherwise flag for human review

## Convergence Loop

The system creates a feedback loop:

```
Bad module boundaries
  → Integration failures & affected-set expansions
    → Architecture agent detects patterns
      → Proposes module splits
        → Smaller, focused modules
          → Better parallelism & fewer integration failures
            → Equilibrium
```

## Drift Detection

Metadata drift detection provides early warning before integration failures occur. When a module's actual file footprint grows beyond what metadata predicts (by `metadata_drift_sensitivity` times the historical average), an alert is raised.
