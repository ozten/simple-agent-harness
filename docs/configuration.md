# Configuration Reference

All configuration is optional. Blacksmith uses sensible defaults and looks for `blacksmith.toml` in the current directory (falls back to `harness.toml` for backwards compatibility).

**Precedence:** Defaults < Config file < CLI flags

## `[session]`

```toml
[session]
max_iterations = 25          # Productive iterations before exiting
prompt_file = "PROMPT.md"    # Agent prompt file path
```

`output_dir`, `output_prefix`, and `counter_file` are deprecated — use `[storage]` instead.

## `[agent]`

Top-level `[agent]` sets defaults. Use `[agent.coding]` and `[agent.integration]` for phase-specific config.

```toml
[agent]
command = "claude"
args = ["-p", "{prompt}", "--dangerously-skip-permissions", "--verbose", "--output-format", "stream-json"]
adapter = "claude"           # Auto-detected from command name if omitted
prompt_via = "arg"           # How to pass prompt: arg | stdin | file
```

**Placeholders in args:**
- `{prompt}` — replaced with assembled prompt text
- `{prompt_file}` — replaced with path to a temp file containing the prompt

### Phase-Specific Agent Config

```toml
[agent.coding]
command = "claude"           # Used for creative coding work
args = ["-p", "{prompt}", "--dangerously-skip-permissions", "--verbose", "--output-format", "stream-json"]
adapter = "claude"
prompt_via = "arg"

[agent.integration]
command = "claude"           # Used for mechanical integration fixes
args = ["-p", "{prompt}", "--dangerously-skip-permissions", "--output-format", "stream-json"]
```

If `[agent.integration]` is omitted, the coding agent config is used for integration too.

## `[watchdog]`

```toml
[watchdog]
check_interval_secs = 60    # How often to check for output growth
stale_timeout_mins = 20      # Kill session after no output for this long
min_output_bytes = 100       # Below this = "empty" session
```

## `[retry]`

```toml
[retry]
max_empty_retries = 2        # Retry attempts for empty sessions
retry_delay_secs = 5         # Delay between retries
```

## `[backoff]`

```toml
[backoff]
initial_delay_secs = 2      # Initial rate-limit backoff
max_delay_secs = 600         # Max backoff cap (10 minutes)
max_consecutive_rate_limits = 5  # Exit loop after N consecutive rate limits
```

## `[shutdown]`

```toml
[shutdown]
stop_file = "STOP"           # Touch this file to trigger graceful shutdown
```

## `[hooks]`

```toml
[hooks]
pre_session = ["git pull --rebase"]
post_session = ["./notify.sh"]
```

See [Core Loop — Hooks](core-loop.md#hooks) for environment variables.

## `[prompt]`

```toml
[prompt]
file = "PROMPT.md"                          # Alternative to session.prompt_file
prepend_commands = ["git diff --stat", "bd ready"]  # Stdout prepended to prompt
```

## `[output]`

```toml
[output]
event_log = "harness-events.jsonl"  # Optional append-only JSONL event log
```

## `[commit_detection]`

```toml
[commit_detection]
patterns = ["bd-finish", "(?i)git commit", "(?i)\\bcommitted\\b"]
```

## `[storage]`

```toml
[storage]
data_dir = ".blacksmith"     # Root for all blacksmith artifacts
compress_after = 5           # Compress sessions older than N iterations
retention = "last-50"        # Retention policy (see below)
```

**Retention policies:**
- `last-N` — Keep the N most recent sessions
- `Nd` — Keep sessions from the last N days
- `all` — Keep everything
- `after-ingest` — Delete raw session files after metrics ingestion

## `[workers]`

```toml
[workers]
max = 1                      # Max concurrent workers (1 = serial mode)
base_branch = "main"         # Branch to create worktrees from
worktrees_dir = "worktrees"  # Subdirectory under data_dir
```

When `max > 1`, blacksmith runs in multi-agent coordinator mode. See [Multi-Agent Coordination](multi-agent.md).

## `[reconciliation]`

```toml
[reconciliation]
every = 3                    # Run full test suite every N integrations
```

## `[architecture]`

```toml
[architecture]
fan_in_threshold = 0.3              # Module fan-in ratio (0.0–1.0) for refactor candidates
integration_loop_max = 5            # Max integration fix iterations before circuit breaker
expansion_event_threshold = 5       # Expansion events within window triggering review
expansion_event_window = 20         # Window size (in tasks) for expansion counting
metadata_drift_sensitivity = 3.0    # Drift multiplier triggering alert
refactor_auto_approve = false       # Auto-execute refactor proposals without human review
```

## `[metrics.extract]`

Custom extraction rules to derive metrics from session output.

```toml
[[metrics.extract.rules]]
kind = "extract.bead_id"             # Event kind to emit
pattern = 'bd update (\S+) --status' # Regex pattern
anti_pattern = "--filter"            # Exclude matches containing this (optional)
source = "tool_commands"             # Where to search: tool_commands | text | raw
transform = "last_segment"           # Post-processing: last_segment | int | trim
first_match = true                   # Stop after first match
count = false                        # Emit count of matches instead of content
emit = true                          # Emit fixed value if pattern found
```

**Sources:**
- `tool_commands` — Tool use input commands (adapter-specific)
- `text` — Assistant text blocks
- `raw` — Raw session file content

**Transforms:**
- `last_segment` — Extract the last path segment
- `int` — Parse as integer
- `trim` — Trim whitespace

## `[metrics.targets]`

Performance targets with streak-based alerting.

```toml
[metrics.targets]
streak_threshold = 3             # Consecutive misses before ALERT

[[metrics.targets.rules]]
kind = "turns.narration_only"
compare = "pct_of"               # Comparison: pct_of | pct_sessions | avg
relative_to = "turns.total"      # Denominator (for pct_of)
threshold = 20
direction = "below"              # Target direction: above | below
label = "Narration-only turns"
unit = "%"
```

## `[finish]`

Quality gates for the `blacksmith finish` command.

```toml
[finish]
check = "cargo check"
test = "cargo test"
lint = "cargo clippy --fix"
format = "cargo fmt"
```
