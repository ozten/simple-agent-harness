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

The default adapter. Parses Claude's stream-json JSONL format.

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

Parses Codex CLI output format.

**Built-in metrics:**
- `turns.total`
- `turns.tool_calls`
- Session metadata (duration, output bytes)

**Not available:** Cost metrics, `turns.narration_only`, `turns.parallel`

## OpenCode Adapter

Parses OpenCode output format.

**Built-in metrics:**
- `turns.total`
- `turns.tool_calls`
- Token counts (if available in output)
- Session metadata

## Aider Adapter

Parses Aider chat log format.

**Built-in metrics:**
- `turns.total`
- `cost.estimate_usd`
- Session metadata

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
