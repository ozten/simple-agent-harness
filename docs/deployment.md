# Deployment Model (V3.5)

Blacksmith is designed to be deployed onto any repository. The deployment model handles initialization, embedded defaults, and self-improvement.

## Initialization

```bash
blacksmith init
```

Creates the `.blacksmith/` directory structure with default files:

```
.blacksmith/
├── blacksmith.db       # SQLite database
├── status              # Harness status file
├── counter             # Global iteration counter
├── lock                # Singleton lock (PID + flock)
├── PROMPT.md           # Default agent instructions
├── blacksmith.toml     # Default configuration
├── skills/             # Claude skills
│   ├── prd-to-beads/
│   ├── break-down-issue/
│   └── self-improvement/
├── sessions/           # Session output files
│   ├── 142.jsonl       # Recent (uncompressed)
│   ├── 140.jsonl.zst   # Older (zstd compressed)
│   └── ...
└── worktrees/          # Git worktrees (multi-agent mode)
    └── ...
```

The directory is automatically added to `.gitignore`.

## Embedded Defaults

Defaults for `PROMPT.md`, skills, and config are embedded in the binary. On first run (or `blacksmith init`), they're extracted to `.blacksmith/`.

## Worktree Provisioning

When a worker is spawned, blacksmith copies skills from `.blacksmith/skills/` to the worktree's `.claude/skills/` directory so the agent has access to all configured skills.

## The `finish` Command

`blacksmith finish` replaces manual commit workflows with quality-gated completion:

```bash
blacksmith finish beads-abc "Implement feature X" src/feature.rs src/lib.rs
```

This runs the configured quality gates in order:

```toml
[finish]
check = "cargo check"      # Compilation
test = "cargo test"         # Tests
lint = "cargo clippy --fix" # Lint
format = "cargo fmt"        # Format
```

Then commits, closes the bead, and signals completion.

## Self-Improvement Architecture

Two-speed feedback loop:

1. **Fast loop** — Improvements stored in the database are shown in the performance brief every session. The agent sees them immediately and can act on them.

2. **Slow loop** — Promoted improvements are manually incorporated into `.blacksmith/PROMPT.md` for permanent inclusion across all future sessions.

The brief is injected via prepend commands:

```toml
[prompt]
prepend_commands = ["blacksmith brief"]
```

## Language-Agnostic Design

Blacksmith works with any language. Configure quality gates per project:

```toml
# Rust
[finish]
check = "cargo check"
test = "cargo test"

# TypeScript
[finish]
check = "tsc --noEmit"
test = "npm test"

# Go
[finish]
check = "go build ./..."
test = "go test ./..."

# Python
[finish]
check = "python -m py_compile src/*.py"
test = "pytest"
```

## Template Variables

Prompts support template variable substitution for dynamic content injection at session start.
