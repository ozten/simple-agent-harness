# Core Loop (V1)

Blacksmith's core loop runs an AI coding agent repeatedly: dispatch a prompt, monitor the session, enforce health invariants, collect output, and repeat.

## Session Lifecycle

Each iteration:

1. **Pre-session hooks** run (if configured). Failure skips the iteration.
2. **Prompt assembly** — prepend command outputs + prompt file contents.
3. **Agent spawned** — the configured command runs with the assembled prompt.
4. **Watchdog monitors** — checks output file growth at regular intervals.
5. **Session ends** — agent exits, is killed by watchdog, or interrupted by signal.
6. **Post-session hooks** run. Failures are logged but don't stop the loop.
7. **Outcome classification** — productive, empty (retry), or rate-limited (backoff).
8. **Metrics ingested** — session output parsed, events stored in database.

The loop exits when:
- `max_iterations` productive iterations complete
- `max_consecutive_rate_limits` hit
- STOP file detected
- SIGINT/SIGTERM received

## Watchdog

Monitors the agent's output file for growth. If no new bytes appear for `stale_timeout_mins`, the process is killed (SIGTERM, then SIGKILL after 5 seconds).

```toml
[watchdog]
check_interval_secs = 60   # How often to check
stale_timeout_mins = 20     # Kill after this long with no output
min_output_bytes = 100      # Below this = "empty" session
```

## Retry Logic

Sessions producing fewer than `min_output_bytes` are retried up to `max_empty_retries` times. Retries don't count as productive iterations and don't trigger post-session hooks.

```toml
[retry]
max_empty_retries = 2
retry_delay_secs = 5
```

## Rate Limit Detection

Inspects the final result event in session JSONL output for `is_error: true` with rate-limit keywords. Successful sessions are never classified as rate-limited, even if the agent discussed rate limiting in its output.

## Exponential Backoff

On rate limits: `initial_delay * 2^consecutive_count`, capped at `max_delay_secs`. Resets after a successful session. Exits the loop after `max_consecutive_rate_limits` consecutive hits.

```toml
[backoff]
initial_delay_secs = 2
max_delay_secs = 600
max_consecutive_rate_limits = 5
```

## Commit Detection

Scans session output for configurable regex patterns. Results are reported in event logs and passed to post-session hooks via `HARNESS_COMMITTED`.

```toml
[commit_detection]
patterns = ["bd-finish", "(?i)git commit", "(?i)\\bcommitted\\b"]
```

## Hooks

Shell commands that run before/after each session:

```toml
[hooks]
pre_session = ["git pull --rebase"]
post_session = ["./notify.sh"]
```

**Pre-session environment variables:**

| Variable | Description |
|---|---|
| `HARNESS_ITERATION` | Current productive iteration |
| `HARNESS_GLOBAL_ITERATION` | Total iterations including retries |
| `HARNESS_PROMPT_FILE` | Path to the prompt file |

**Post-session environment variables** (all of the above, plus):

| Variable | Description |
|---|---|
| `HARNESS_OUTPUT_FILE` | Path to the session output file |
| `HARNESS_EXIT_CODE` | Agent process exit code |
| `HARNESS_OUTPUT_BYTES` | Bytes written to output |
| `HARNESS_SESSION_DURATION` | Session wall-clock time in seconds |
| `HARNESS_COMMITTED` | `true` if commit patterns were detected |

Pre-hook failure skips the iteration. Post-hook failures are logged but don't stop the loop.

## Prompt Assembly

The prompt sent to the agent is assembled from:
1. Output of `prepend_commands` (separated by `---`)
2. Contents of the prompt file

```toml
[prompt]
prepend_commands = ["git diff --stat", "bd ready"]
```

## Graceful Shutdown

| Method | Behavior |
|---|---|
| **STOP file** | `touch STOP` — clean exit after current session (file deleted on detection) |
| **SIGINT** | First signal finishes current session then exits |
| **Double SIGINT** | Second signal within 3s force-kills the agent immediately |
| **SIGTERM** | Same as first SIGINT |

## Status File

A `status` JSON file is maintained in `.blacksmith/` with current state, iteration counts, PID, uptime, and output info. Query with `blacksmith --status`.

## Event Log

Optional append-only JSONL log with one entry per session:

```toml
[output]
event_log = "harness-events.jsonl"
```

Each entry includes: timestamp, iteration numbers, output bytes, exit code, duration, committed flag, retry count, and rate-limit flag.
