# Troubleshooting

## Common Issues

### Agent Session Produces No Output

**Symptoms:** Session ends with 0 bytes, retried up to `max_empty_retries` times.

**Causes:**
- Agent command not found or not in PATH
- Prompt file missing or empty
- Agent failing silently (check exit code in event log)

**Fix:**
1. Run `blacksmith --dry-run` to verify config
2. Run the agent command manually to check it works
3. Check `blacksmith metrics events --session N` for the failed session's exit code

### Session Killed by Watchdog

**Symptoms:** Log shows "watchdog: killing stale session" after `stale_timeout_mins`.

**Causes:**
- Agent is stuck (infinite loop, waiting for input)
- Agent writing to a different location than expected
- Timeout too short for the task

**Fix:**
- Increase `watchdog.stale_timeout_mins` for long-running tasks
- Verify agent output goes to the expected file
- Use `--verbose` to see watchdog check details

### Rate Limit Loop

**Symptoms:** Repeated rate-limit backoffs, eventually exits after `max_consecutive_rate_limits`.

**Causes:**
- API quota exhausted
- Too many concurrent agents hitting the same API

**Fix:**
- Reduce `workers.max` to lower concurrency
- Increase `backoff.max_delay_secs` to wait longer
- Increase `backoff.max_consecutive_rate_limits` to wait it out

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
- Test failures introduced by the integration
- Manifest entry conflicts

**Fix:**
1. Check the worktree: `cd .blacksmith/worktrees/worker-N-beads-xxx/`
2. Inspect the error in `blacksmith integration log`
3. Fix manually, then `blacksmith integration retry beads-xxx`
4. Or rollback: `blacksmith integration rollback beads-xxx`

### Config Validation Errors

**Symptoms:** "Configuration validation failed" on startup.

**Causes:**
- Invalid regex in extraction rules or commit detection patterns
- Missing prompt file
- Invalid values (negative timeouts, etc.)

**Fix:**
- Run `blacksmith --dry-run` to see validation errors
- Check regex syntax (uses Rust regex, not PCRE)
- Ensure prompt file exists at the configured path

### Dashboard Smoke Test Failures

**Symptoms:** `./scripts/smoke-test-ui.sh` reports FAIL on one or more endpoints.

**Common causes and fixes:**

| Failure | Likely cause | Fix |
|---|---|---|
| TIMEOUT waiting for serve | Binary not built with `--features serve`, or port 18420 in use | Rebuild with `cargo build --release --features serve`; kill conflicting process |
| TIMEOUT waiting for UI | `blacksmith-ui` binary missing, or port 18080 in use | Rebuild with `cargo build --release -p blacksmith-ui`; kill conflicting process |
| FAIL on a specific serve endpoint | Route removed or renamed in `src/serve.rs` | Check git diff for route changes, restore the route |
| FAIL on a dashboard endpoint | Route removed or renamed in `blacksmith-ui/src/main.rs` | Check git diff for route changes, restore the route |
| FAIL on proxied transcript | Serve session/transcript handler broken | Check `api_session_transcript` and `api_session_stream` in `src/serve.rs` |
| FAIL on poll-data | Poller hasn't fetched yet or serve unreachable | Increase the sleep before dashboard checks, or check serve is healthy |

See also: [Dashboard docs — Smoke Testing](dashboard.md#smoke-testing)

## Error Codes

| Exit Code | Meaning |
|---|---|
| 0 | Success — all iterations completed |
| 1 | Configuration or runtime error |
| 124 | Watchdog timeout (convention) |

## Diagnostic Commands

```bash
# Full config dump
blacksmith --dry-run

# Current state
blacksmith --status

# Recent session metrics
blacksmith metrics status

# Raw events for a session
blacksmith metrics events --session 142

# Worker pool state
blacksmith workers status

# Integration history
blacksmith integration log

# Check adapter detection
blacksmith adapter info

# Test adapter parsing
blacksmith adapter test path/to/session.jsonl
```

## Log Levels

| Flag | Level | What You See |
|---|---|---|
| `-q` | error | Errors only |
| (default) | info | Iteration summaries, key events |
| `-v` | debug | Watchdog checks, retry decisions, config details |

Set `RUST_LOG` environment variable for fine-grained control:

```bash
RUST_LOG=blacksmith::watchdog=debug blacksmith
```

## Recovery Procedures

### Rebuild Metrics Database

If observations seem wrong or corrupted:

```bash
blacksmith metrics rebuild
```

Drops and recreates observations from the events table.

### Re-ingest Sessions

After changing extraction rules:

```bash
blacksmith metrics reingest --all
```

### Migrate from Legacy Layout

If upgrading from V2 (files in project root):

```bash
blacksmith migrate --consolidate
```

Moves `claude-iteration-*.jsonl`, `.iteration_counter`, and database into `.blacksmith/`.

### Force-Clean Worktrees

If a worktree is in a bad state:

```bash
git worktree remove .blacksmith/worktrees/worker-N-beads-xxx --force
```

Then retry the bead — the coordinator will create a fresh worktree.
