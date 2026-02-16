---
name: break-down-issue
description: Decompose a large beads issue into smaller sub-issues. Use when the user says 'break down this issue,' 'decompose bead,' 'split this bead,' 'this bead is too large,' or passes a bead ID that needs decomposition. Also use when the user invokes /break-down-issue.
---

# Break Down Issue

Decompose a large bead into smaller, independently actionable sub-beads. The original bead becomes an epic blocked-by its children.

## Workflow

### 1. Read the issue

Run `bd show $ARGUMENTS` to get the issue details. If no ID given, ask the user.

If the issue description is sparse, investigate further:
- Check if a PRD section is referenced — read that section
- Check if relevant source files are mentioned — skim them to understand scope
- Ask the user for clarification only if the title + description + code give insufficient context

### 2. Identify sub-tasks

Break the issue into pieces that are each completable in one coding session (~60-80 turns).

**Split along natural boundaries:**
- Data model changes (new meta keys, CPT modifications)
- Backend logic (business rules, validation, capacity enforcement)
- Admin UI (meta boxes, list tables, admin pages)
- Frontend display (templates, CSS, client-side JS)
- REST API (new endpoints or modifications)
- Tests (can bundle with implementation or separate if large)

**Each sub-bead must:**
- Be independently testable
- Have a clear "done" definition using the structured template (see below)
- Not require holding context from another sub-bead to implement

### Sub-bead description template

Every sub-bead description MUST include these sections:

```
<What to build — 2-4 sentences with context copied from parent so implementer doesn't need to read the parent.>

## Done when
- [ ] <specific, testable condition 1>
- [ ] <specific, testable condition 2>
- [ ] cargo check passes with no new errors
- [ ] cargo test passes (all existing + any new tests)

## Verify
- Run: <exact command to verify>
- Expect: <exact output or behavior>

## Affected files
- <file.rs> (new|modified)
```

Each "Done when" item must be independently verifiable. Each "Verify" must be a concrete command, not "check that it works". "Affected files" lists files the implementer will create or modify — this feeds the parallel agent scheduler.

### 3. Map dependencies between sub-beads

Common patterns:
- Data model sub-bead blocks all others (can't build UI without data)
- Backend logic blocks frontend display
- Core feature blocks enhancement/polish sub-beads

### 4. Create sub-beads

For each sub-bead, run:

```bash
bd create --title="<title>" --type=<type> --priority=<priority>
```

Capture returned IDs. Then add descriptions:

```bash
bd update <id> --description="<description>"
```

Inherit the parent's type and priority unless a sub-bead clearly differs (e.g., a test task from a feature parent).

### 5. Wire dependencies

Between sub-beads:
```bash
bd dep add <blocked-child> <blocker-child>
```

Then make the original bead blocked-by ALL its children:
```bash
bd dep add <original-bead> <child-1>
bd dep add <original-bead> <child-2>
bd dep add <original-bead> <child-N>
```

This makes the original bead an epic that auto-resolves when all children complete.

### 6. Output summary

Print the decomposition:

```
Original: <id> - <title> (now epic, blocked by N children)

Children:
| ID   | Title                          | Type    | Pri | Blocked By |
|------|--------------------------------|---------|-----|------------|
| abc1 | Add data model for X           | task    | 1   | -          |
| abc2 | Implement backend logic for X  | feature | 1   | abc1       |
| abc3 | Add admin UI for X             | feature | 2   | abc2       |
| abc4 | Add frontend display for X     | feature | 2   | abc2       |
```

Run `bd show <original-bead>` to confirm it shows as blocked-by the children.

## Guidelines

- Aim for 2-5 sub-beads per decomposition. More than 6 suggests the split is too granular.
- If a sub-bead itself looks large, note it but don't recursively decompose — the user or a future session can run this skill again on that sub-bead.
- Copy relevant context from the parent description into each child so implementers don't need to read the parent.
- Keep the parent bead's original title and description intact — don't modify it beyond what bd dep commands do.
