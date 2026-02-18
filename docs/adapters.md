# Agent Adapters

Blacksmith supports multiple AI coding agents through an adapter system. Each adapter knows how to parse the agent's output format and extract structured metrics.

## Supported Adapters

| Adapter | Auto-detect | Output Format | Cost Metrics | Turn Metrics |
|---|---|---|---|---|
| `claude` | command contains "claude" | stream-json JSONL | Yes | Full |
| `codex` | command contains "codex" | Codex CLI output | No | Partial |
| `opencode` | command contains "opencode" | OpenCode output | Partial | Partial |
| `aider` | command contains "aider" | Aider chat log | Cost only | Partial |
| `raw` | fallback | Any | No | No |

## Auto-Detection

The adapter is selected by matching the agent command name:

1. Command contains `claude` → `claude`
2. Command contains `codex` → `codex`
3. Command contains `opencode` → `opencode`
4. Command contains `aider` → `aider`
5. Anything else → `raw`

Override auto-detection with an explicit adapter:

```toml
[agent]
command = "my-custom-claude-wrapper"
adapter = "claude"    # Force Claude adapter
```

## Claude Adapter

The default adapter. Parses Claude Code's `stream-json` JSONL format.

**Config:**

```toml
[agent]
command = "claude"
args = ["-p", "{prompt}", "--dangerously-skip-permissions", "--verbose", "--output-format", "stream-json"]
```

Key flags:
- `-p {prompt}` — Non-interactive ("print") mode; reads the prompt as an argument and exits when done.
- `--output-format stream-json` — Emits JSONL events to stdout (required for metric extraction).
- `--verbose` — Includes tool-use detail in the JSONL stream.
- `--dangerously-skip-permissions` — Skip all permission prompts (required for headless / unattended use).
- `--allowedTools "Bash Read Write Edit Glob Grep"` — Optional whitelist of tools the agent may use (alternative to `--dangerously-skip-permissions`; use one or the other).
- `--model <model-id>` — Override the default model (e.g. `claude-sonnet-4-5-20250929`).
- `--max-turns <N>` — Cap the number of agentic turns.

**Built-in metrics:**
- `turns.total` — Total conversation turns
- `turns.narration_only` — Turns with no tool use
- `turns.parallel` — Turns with parallel tool calls
- `turns.tool_calls` — Total tool call count
- `cost.input_tokens` — Input token count
- `cost.output_tokens` — Output token count
- `cost.estimate_usd` — Session cost from result event

**Source mapping for extraction rules:**
- `tool_commands` → Tool use `input.command` fields
- `text` → Assistant text blocks

## Codex Adapter

Parses OpenAI Codex CLI `--json` JSONL output.

**Config:**

```toml
[agent]
command = "codex"
args = ["exec", "--json", "--yolo", "{prompt}"]
```

Key flags:
- `exec` — Non-interactive subcommand (alias: `e`). Required for headless / CI use.
- `--json` — Emits JSONL events to stdout (required for metric extraction). Event types: `thread.started`, `turn.started`, `turn.completed`, `item.started`, `item.completed`, `error`.
- `--dangerously-bypass-approvals-and-sandbox` / `--yolo` — Skip all approval prompts and sandbox restrictions (required for headless / unattended use). Without this, `--full-auto` sets `--ask-for-approval on-request` which can still prompt interactively, hanging a headless process.
- `--full-auto` — Lighter alternative: allows edits but sets `--ask-for-approval on-request` and workspace-write sandbox. **Not recommended** for headless use since the agent may still prompt.
- `--ephemeral` — Skip persisting Codex session files to disk.
- `-m <model>` / `--model <model>` — Override the configured model.
- `-s <policy>` / `--sandbox <policy>` — Sandbox policy: `read-only` (default), `workspace-write`, or `danger-full-access`.
- `--skip-git-repo-check` — Allow execution outside a Git repository.
- `-o <path>` / `--output-last-message <path>` — Write the final agent message to a file.

Environment:
- `CODEX_API_KEY` — API key for CI (only supported in `codex exec`).

Prompt delivery uses `prompt_via = "arg"` by default — the `{prompt}` placeholder is replaced inline in the args.

**Built-in metrics:**
- `turns.total`
- `turns.tool_calls`
- `session.output_bytes`, `session.exit_code`, `session.duration_secs`

**Not available:** Cost metrics, `turns.narration_only`, `turns.parallel`

**Example — headless with custom model:**

```toml
[agent]
command = "codex"
args = ["exec", "--json", "--yolo", "-m", "o3", "{prompt}"]
adapter = "codex"
```

## OpenCode Adapter

Parses OpenCode JSONL or single-JSON session output.

**Config:**

```toml
[agent]
command = "opencode"
args = ["run", "{prompt}"]
prompt_via = "arg"
```

Key flags:
- `run "<prompt>"` — Non-interactive mode; executes the prompt and exits.
- `-q` / `--quiet` — Disables the spinner (useful in automation).
- `--log-level <LEVEL>` — Set log verbosity: `DEBUG`, `INFO`, `WARN`, `ERROR`.

OpenCode can also run in server mode (`opencode serve`) with `--attach` for reuse across invocations, but the `run` subcommand is what blacksmith invokes.

> **Note:** OpenCode does not currently have a `--json` flag for structured JSONL output. The adapter parses whatever format the session file contains (JSONL lines or a single JSON object with a `messages` array). Token metrics are extracted when the output includes `usage`, `prompt_tokens`, or `completion_tokens` fields; otherwise they are silently skipped.

**Built-in metrics:**
- `turns.total`
- `turns.tool_calls`
- `cost.input_tokens`, `cost.output_tokens` (when available in output)
- `session.output_bytes`, `session.exit_code`, `session.duration_secs`

## Aider Adapter

Parses Aider's plain-text chat log format.

**Config:**

```toml
[agent]
command = "aider"
args = ["--message", "{prompt}", "--yes-always"]
```

Key flags:
- `--message` / `-m` — Non-interactive mode; sends one message and exits.
- `--message-file` / `-f` — Read the prompt from a file instead of an argument.
- `--yes-always` — Automatically confirm all prompts (required for unattended use).
- `--yes` — Lighter variant; confirms most prompts.
- `--auto-commits` / `--no-auto-commits` — Control whether aider auto-commits changes (default: on).
- `--model <model>` — Override the model.
- `--no-stream` — Disable streaming (can simplify log parsing).

Environment:
- `AIDER_YES=true` — Equivalent to `--yes`.
- `AIDER_MESSAGE=...` — Equivalent to `--message`.

Prompt delivery: use `prompt_via = "arg"` (default) with the `{prompt}` placeholder inside `--message`, or use `prompt_via = "file"` with `--message-file {prompt_file}` for very long prompts.

**Built-in metrics:**
- `turns.total`
- `cost.estimate_usd` (extracted from Aider's "Cost: $X.XX session" lines)
- `session.output_bytes`

**Not available:** Token counts, `turns.tool_calls`, `turns.narration_only`, `turns.parallel`

**Example — aider with prompt file and no auto-commits:**

```toml
[agent]
command = "aider"
args = ["--message-file", "{prompt_file}", "--yes-always", "--no-auto-commits"]
prompt_via = "file"
```

## Raw Adapter

No format-specific parsing. Only session metadata (from the harness, not agent output) is available. All other metrics must come from custom [extraction rules](configuration.md#metricsextract).

Use this for any agent not supported by a built-in adapter.

## Graceful Degradation

Metrics unavailable for the current adapter are silently skipped. The [performance brief](metrics.md#performance-brief) and [targets](metrics.md#targets) automatically exclude metrics the adapter can't provide.

## Prompt Delivery

Control how the prompt reaches the agent:

```toml
[agent]
prompt_via = "arg"     # Default: passed as argument (via {prompt} placeholder)
# prompt_via = "stdin" # Piped to agent's stdin
# prompt_via = "file"  # Written to temp file, path passed via {prompt_file}
```

## CLI Commands

```bash
# Show which adapter is active
blacksmith adapter info

# List all adapters and their metrics
blacksmith adapter list

# Test adapter parsing on a file
blacksmith adapter test path/to/session.jsonl
```
