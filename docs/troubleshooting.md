# Troubleshooting

## Common Issues

### Agent Session Produces No Output

**Symptoms:** Session ends with 0 bytes, retried up to `max_empty_retries` times.

**Causes:**
- Agent command not found or not in PATH
- Prompt file missing or empty
- Agent failing silently

**Fix:**
1. Run `blacksmith --dry-run` to verify config
2. Run the agent command manually to check it works
3. Check `blacksmith metrics events --session N` for the exit code

### Session Killed by Watchdog

**Symptoms:** Log shows "watchdog: killing stale session" after `stale_timeout_mins`.

**Causes:**
- Agent stuck (infinite loop, waiting for input)
- Agent writing to unexpected location
- Timeout too short for the task

**Fix:**
- Increase `watchdog.stale_timeout_mins` for long-running tasks
- Verify agent output goes to the expected file
- Use `-v` to see watchdog check details

See [Watchdog](core/watchdog.md).

### Rate Limit Loop

**Symptoms:** Repeated rate-limit backoffs, exits after `max_consecutive_rate_limits`.

**Causes:**
- API quota exhausted
- Too many concurrent agents hitting the same API

**Fix:**
- Reduce `workers.max` to lower concurrency
- Increase `backoff.max_delay_secs`
- Increase `backoff.max_consecutive_rate_limits`

See [Retry & Backoff](core/retry-and-backoff.md).

### "Another blacksmith instance is already running"

**Symptoms:** Exit on startup with singleton lock error.

**Causes:**
- Another blacksmith process is running
- Previous process crashed without releasing the lock

**Fix:**
1. Check with `blacksmith --status` or `ps aux | grep blacksmith`
2. If no process exists, delete `.blacksmith/lock`

### Integration Failures (Multi-Agent)

**Symptoms:** Bead marked `needs_review`, circuit breaker tripped.

**Causes:**
- Merge conflicts the integration agent can't resolve
- Test failures from the integration
- Manifest entry conflicts

**Fix:**
1. Inspect the worktree: `cd .blacksmith/worktrees/worker-N-beads-xxx/`
2. Check the error: `blacksmith integration log`
3. Fix manually, then: `blacksmith integration retry beads-xxx`
4. Or rollback: `blacksmith integration rollback beads-xxx`

See [Integration](multi-agent/integration.md).

### Config Validation Errors

**Symptoms:** "Configuration validation failed" on startup.

**Causes:**
- Invalid regex in extraction rules or commit detection
- Missing prompt file
- Invalid values (negative timeouts, etc.)

**Fix:**
- Run `blacksmith --dry-run` to see validation errors
- Check regex syntax (Rust regex, not PCRE)
- Ensure prompt file exists

### Dashboard Smoke Test Failures

See [Smoke Testing](dashboard/smoke-testing.md) for detailed failure signatures.

## Error Codes

| Exit Code | Meaning |
|---|---|
| 0 | Success — all iterations completed |
| 1 | Configuration or runtime error |
| 124 | Watchdog timeout (convention) |

## Diagnostic Commands

```bash
blacksmith --dry-run                          # Full config dump
blacksmith --status                           # Current state
blacksmith metrics status                     # Recent sessions
blacksmith metrics events --session 142       # Raw events
blacksmith workers status                     # Worker pool
blacksmith integration log                    # Integration history
blacksmith adapter info                       # Adapter detection
blacksmith adapter test path/to/session.jsonl # Test parsing
```

## Log Levels

| Flag | Level | What You See |
|---|---|---|
| `-q` | error | Errors only |
| (default) | info | Iteration summaries, key events |
| `-v` | debug | Watchdog checks, retry decisions, config details |

Fine-grained control via `RUST_LOG`:

```bash
RUST_LOG=blacksmith::watchdog=debug blacksmith
```

## Recovery Procedures

### Rebuild Metrics Database

```bash
blacksmith metrics rebuild
```

Drops and recreates observations from events.

### Re-ingest Sessions

```bash
blacksmith metrics reingest --all
```

### Migrate from Legacy Layout

```bash
blacksmith migrate --consolidate
```

### Force-Clean Worktrees

```bash
git worktree remove .blacksmith/worktrees/worker-N-beads-xxx --force
```

The coordinator creates a fresh worktree on next assignment.
