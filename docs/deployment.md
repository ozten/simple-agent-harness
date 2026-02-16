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
├── config.toml         # Default configuration
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

## The `bd-finish.sh` Script

The `bd-finish.sh` shell script replaces manual commit workflows with quality-gated completion:

```bash
./bd-finish.sh beads-abc "Implement feature X" src/feature.rs src/lib.rs
```

If no files are specified, it stages all tracked modified files (`git add -u`).

The script runs these steps in order:
1. `cargo check` — abort if code doesn't compile
2. `cargo test` — abort if tests fail
3. Append `PROGRESS.txt` to `PROGRESS_LOG.txt` (timestamped)
4. Stage specified files (or `git add -u`)
5. `git commit`
6. `bd close <bead-id>`
7. `bd sync`
8. Auto-commit `.beads/` changes
9. `git push`

The script is extracted to the project root by `blacksmith init`.

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

Blacksmith works with any language. To customize quality gates, edit `bd-finish.sh` to replace the `cargo check` and `cargo test` commands with your project's equivalents (e.g. `tsc --noEmit` / `npm test` for TypeScript, `go build ./...` / `go test ./...` for Go).
