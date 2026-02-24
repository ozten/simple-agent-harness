# Task Execution Instructions

## CRITICAL: Execution Efficiency Rules (MUST FOLLOW)

### Session mode check (run before Rule A/B/C)
Before applying the coding-bead workflow below, determine whether this session is an analysis/bookkeeping/meta-process task (for example: prompt tuning, reviewing metrics, documenting process changes, or other non-code implementation work).

If the session is analysis/bookkeeping/meta-process and code implementation is NOT explicitly requested, skip coding-session Task Selection/Execution Protocol requirements (including `bd ready`, task claiming, cargo quality gates, and `blacksmith finish`). Use only the commands needed to inspect evidence and record process artifacts for the requested analysis. Analysis/bookkeeping/meta-process sessions are exempt from Rule A mandatory parallel patterns, any zero-parallel failure rule, and the 5+ parallel turns target.

If the session includes code implementation (or explicitly asks you to work a bead), follow the full Rule A/B/C + Task Selection + Execution Protocol workflow.

These two rules are NON-NEGOTIABLE. Violating them wastes 25-35% of your turn budget.

### Rule A: ALWAYS batch independent tool calls in the SAME turn.
Every time you are about to call a tool, ask: "Is there another independent call I can make at the same time?" If yes, emit BOTH tool calls in the SAME message.
Bounded batching rule: Cap each assistant message at 2-4 tool calls (hard max 6), and count inner calls inside multi-tool/parallel wrappers (for example `multi_tool_use.parallel`) toward that 2-4 target and hard max 6 in the same message. Do NOT fan out dozens of speculative reads/greps/commands in one turn. Batch only calls you are certain you need right now, then inspect results before issuing the next batch.

**Mandatory parallel patterns — use these EVERY session:**
- Session start: `bd ready` + `blacksmith progress show --bead-id <id>` → ONE turn, TWO tool calls
- Reading source + test: `Read foo.rs` + `Read foo_test.rs` → ONE turn
- Multiple greps: `Grep("pattern1")` + `Grep("pattern2")` → ONE turn
- Session end: `Bash(cargo clippy --fix)` + `Bash(cargo test --release)` → ONE turn (if they don't depend on each other's output)
- Reading multiple related files: `Read config.rs` + `Read main.rs` → ONE turn

Use parallel calls when they reduce waiting, but do not optimize for parallel-call quotas. Prefer small batches (usually 2 calls) and keep decision points between batches so you can adapt to results.

### Rule B: Prefer narration + needed tool calls in the same turn (avoid filler tool calls).
WRONG: "Let me check the tests." (turn 1) → `Grep(tests/)` (turn 2)
RIGHT: "Let me check the tests." + `Grep(tests/)` (turn 1 — one message, includes both text AND tool call)
ALSO RIGHT: Short planning/synthesis message with no tool call when choosing the next bounded batch or summarizing results before the next action.

If you want to narrate what you're doing, combine the narration with the tool call you were already going to make. Do NOT add extra or unrelated tool calls just to satisfy this rule, and do not force a tool call into a turn that is just planning the next batch. Use brief text-only messages only when they reduce churn (for example, choosing the next batch after inspecting results).

### Rule C: After closing your bead, EXIT IMMEDIATELY.
Do NOT triage other beads. Do NOT run `bd ready` to find more work. Do NOT explore what to do next.
The sequence after closing is: `blacksmith progress add --bead-id <id> --stdin` -> run `blacksmith finish` -> STOP.
Each session handles exactly ONE bead. The loop script handles picking the next one.

---

## Context Loading

The project architecture is documented in MEMORY.md — do NOT re-explore the codebase.
Only read files you are about to modify. Do NOT launch explore subagents (this means NO `Task` tool with `subagent_type: Explore`).

1. Run `bd ready` AND `blacksmith progress show --bead-id <id>` in the SAME turn (Rule A — two parallel tool calls)

## Task Selection
Pick ONE task from the ready queue. **Always pick the highest-priority (lowest number) ready task.** Only deviate if recent `blacksmith progress list --bead-id <id>` entries explain why a specific lower-priority task should go next (e.g., it's a quick follow-up to the last session's work).

**Remember Rule C**: You will work on exactly ONE task this session. After closing it, exit immediately.

### Failed-Attempt Detection
Before claiming a task, run `bd show <id>` and check its notes for `[FAILED-ATTEMPT]` markers.

- **0 prior failures**: Proceed normally.
- **1 prior failure**: Proceed, but read the failure reason carefully. If the reason mentions "too large" or "ran out of turns," consider whether you can realistically finish in 55 turns. If not, skip to the decomposition step below.
- **2+ prior failures**: Do NOT attempt implementation. Instead, decompose the bead into smaller sub-beads:
  1. Analyze the bead description and failure notes to understand why it keeps failing
  2. Break it into 2-5 smaller sub-beads (follow the break-down-issue workflow: create children, wire deps, make original blocked-by children)
  3. Record decomposition with `blacksmith progress add --bead-id <id> --stdin`, then exit cleanly via `blacksmith finish`
  4. The next session will pick up the newly-unblocked child beads

If ALL top-priority ready beads have 2+ failures and you've decomposed them, move to the next priority level.

### No Work Available
If `bd ready` returns no tasks, exit immediately:
1. Do NOT create any git commits
2. Do NOT write a progress entry
3. Simply exit — the harness will handle retry/shutdown

## Execution Protocol
For the selected task (e.g., bd-X):

1. **Claim**: `bd update bd-X --status in_progress`

2. **Understand**: Run `bd show bd-X` for full task description. If the task references a PRD section, read it with an offset (see PRD index in AGENTS.md).

3. **Implement**: Complete the task fully
   - Only read files you need to modify — architecture is in MEMORY.md
   - Follow existing code patterns (see MEMORY.md for architecture and testing conventions)

4. **Verify** (use parallel calls per Rule A):

   **4a. Bead-specific verification:**
   Run `bd show bd-X` and look for a "## Verify" section in the description. If it exists, execute those exact steps. If any verification step fails, fix the issue before proceeding.

   If the bead has NO "## Verify" section, add one now:
   ```bash
   bd update bd-X --notes="## Verify\n- Run: <command you used to test>\n- Expect: <what you observed>"
   ```
   **Format rules for ## Verify:** Each `Run:` line must contain ONLY the shell command — no prose, no expected output, no comments after the command. Put expectations on a separate `Expect:` line.

   **4b. Code quality gates:**
   ```bash
   # Run full test suite FIRST, then lint in parallel:
   cargo test --release
   # Then in ONE turn with TWO parallel Bash calls:
   cargo clippy --fix --allow-dirty
   cargo fmt --check
   ```
   Run clippy and fmt exactly ONCE each. Do not repeat them.

   **4c. Integration check:**
   Before closing, verify your changes don't break existing callers. Grep for the function/struct names you changed or renamed. If other code references them, confirm those references still work.

5. **Finish** — record progress and call `blacksmith finish`, then STOP (Rule C):
   - **Write a progress entry** with `blacksmith progress add --bead-id bd-X --stdin` and include a short handoff note:
     - What you completed this session
     - Current state of the codebase
     - Suggested next tasks for the next session
   - **Run the finish command**:
     ```bash
     blacksmith finish bd-X "<brief description>" src/file1.rs src/file2.rs
     ```
     This runs quality gates (check + test), verifies bead deliverables, then handles: staging, committing, bd close, bd sync, auto-committing .beads/, recording bead closure metadata, and git push — all in one command.
     **If quality gates fail, the bead is NOT closed.** Fix the issues and re-run.
   - If no specific files to stage, omit the file list and it will stage all tracked modified files.
   - **After `blacksmith finish` completes, STOP. Do not triage more work. Do not run bd ready. Session is done.**

## Turn Budget (R1)

You have a **hard budget of 80 assistant turns** per session. Track your turn count.

- **Turns 1-55**: Normal implementation. Write code, run targeted tests (`--filter`).
- **Turns 56-65**: **Wrap-up phase.** Stop new feature work. Run the full test suite + `lint:fix` + `analyze`. If passing, commit and close.
- **Turns 66-75**: **Emergency wrap-up.** If tests/lint are failing, make minimal fixes. If you can't fix in 10 turns, revert your changes (`git checkout -- .`), mark the failure (see below), record a progress entry, and exit cleanly.
- **Turn 76+**: **Hard stop.** Do NOT start any new work. If you haven't committed yet: revert, mark the failure, record a progress entry, and exit immediately. An uncommitted session is worse than a cleanly abandoned one.

If you realize before turn 40 that the task is too large to complete in the remaining budget, STOP immediately. Mark the failure, and exit. Do not burn 40 more turns on a doomed session.

### Marking a Failed Attempt
When bailing out of a task for any reason, always run:
```bash
bd update <id> --status=open --notes="[FAILED-ATTEMPT] <YYYY-MM-DD> <reason>"
```
Use a specific reason: `too-large`, `tests-failing`, `lint-unfixable`, `missing-dependency`, `context-overflow`, or a brief custom description. This marker is read by future sessions to detect beads that need decomposition (see Task Selection).

## Stop Conditions
- Complete exactly ONE task per iteration, then STOP (Rule C)
- After calling `blacksmith finish`, do NOT continue. Do NOT triage. Do NOT run bd ready again.
- If task cannot be completed, mark the failure (see above), record progress with `blacksmith progress add`, exit cleanly
- If tests fail, debug and fix within this iteration

## Improvement Recording

Record institutional lessons using `blacksmith improve add` when you encounter reusable insights during your session. This builds the project's knowledge base so future sessions avoid repeated mistakes and adopt proven patterns.

**When to record** (pick at most 2 per session — don't spend turns on this):
- You discover a non-obvious debugging technique or root cause
- You find a code pattern that should be followed (or avoided) project-wide
- You notice a workflow inefficiency (e.g., unnecessary file reads, redundant test runs)
- A test failure reveals a subtle invariant that isn't documented

**When NOT to record:**
- Routine task completion (closing a bead is not an insight)
- Obvious things already in MEMORY.md or PROMPT.md
- Session-specific context that won't help future sessions

**How to record:**
```bash
blacksmith improve add "Short descriptive title" \
  --category <workflow|cost|reliability|performance|code-quality> \
  --body "What you learned and why it matters" \
  --context "Evidence: session number, file, or error message"
```

**Example:**
```bash
blacksmith improve add "Always check Cargo.toml when adding new modules" \
  --category reliability \
  --body "New module files need their crate dependencies added to Cargo.toml. Cargo check catches this but only if run before bead closure." \
  --context "Session 50 closed a bead with uncompilable code because Cargo.toml was missing the fs2 dependency"
```

### Evidence threshold for process-improvement analyses

With fewer than 3 recent sessions, also cap evidence collection to the minimum commands needed to inspect the cited metrics, review existing open improvements, and verify the specific PROMPT.md text related to any candidate change. Avoid broad repo scans or large multi-command fanout unless a severe anomaly cannot be validated from that minimal evidence.

### Metrics noise filter for self-improvement analyses

When analyzing recent sessions for prompt/process changes, treat sessions with turns.total = 1 and session.duration_secs = 0 as low-confidence evidence (likely harness/setup/fast-exit artifacts) unless corroborated by at least 2 additional sessions showing the same pattern. Do not file a new PROMPT.md improvement based solely on a single low-confidence session; use it only to review already-open improvements or request more data.

Record improvements as you work — don't batch them to the end of the session.

## Important
- Do not ask for clarification — make reasonable decisions
- Do NOT launch explore/research subagents (NO `Task` with `subagent_type: Explore`) — the architecture is in MEMORY.md
- Do NOT re-read files you already know from MEMORY.md
- Prefer small, atomic changes over large refactors
- Always run `cargo test --release` before committing
- Always run `cargo clippy --fix --allow-dirty` then `cargo fmt` before committing — exactly ONCE each
- Always use `blacksmith finish` to close out — do NOT manually run git add/commit/push/bd close/bd sync
- **NEVER call `bd close` directly** — always go through `blacksmith finish` which enforces quality gates
- **EFFICIENCY**: Re-read Rules A, B, C above. Minimize avoidable text-only turns and sequential-when-parallel tool calls. Aim for 5+ parallel turns per session, while using short planning/synthesis text-only turns only when they help choose the next batch cleanly.
