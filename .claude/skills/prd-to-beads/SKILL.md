---
name: prd-to-beads
description: Break down a PRD (Product Requirements Document) into beads issues with dependencies. Use when the user says 'break down this PRD,' 'file beads from PRD,' 'PRD to beads,' 'create issues from PRD,' or provides a PRD file path and wants it decomposed into trackable work items. Also use when the user invokes /prd-to-beads.
---

# PRD to Beads

Break a PRD markdown file into independently actionable beads issues, wire up dependencies, and output a summary.

## Workflow

### 1. Read and parse the PRD

Read the PRD file passed as `$ARGUMENTS`. If no path given, ask the user.

Identify discrete work items by scanning for:
- Numbered features or requirements sections
- Subsections with distinct deliverables
- Tables of requirements (each row = potential bead)
- Bullet lists of capabilities under a feature heading

### 2. Decompose into beads

For each work item, determine:

- **title**: Imperative, concise (e.g., "Add RSVP capacity enforcement")
- **type**: `feature` (new capability), `task` (infrastructure/refactor/tests), or `bug` (fix)
- **priority**: 0-4 (0=critical, 1=high, 2=medium, 3=low, 4=backlog). Match the PRD's own priority signals — words like "must have" = 0-1, "should have" = 2, "nice to have" = 3-4
- **description**: Structured description with three required sections (see template below)
- **affected_globs**: List of file glob patterns this bead will likely touch (e.g., `src/pool.rs`, `src/coordinator.rs`). Critical for parallel agent scheduling — without this, the scheduler treats beads as "affects everything" and serializes them.

Each bead must be completable in a single coding session (~60-80 turns). If a PRD feature is too large, split it into multiple beads right now — don't create L/XL beads that will need decomposition later.

### Bead description template

Every bead description MUST include these three sections:

```
<What to build — 2-4 sentences covering the implementation and relevant PRD section reference.>

## Done when
- [ ] <specific, testable condition 1>
- [ ] <specific, testable condition 2>
- [ ] cargo check passes with no new errors
- [ ] cargo test passes (all existing + any new tests)

## Verify
- Run: <exact command to verify the feature works>
- Expect: <exact output or behavior>

## Affected files
- <file1.rs> (new|modified)
- <file2.rs> (modified)
```

**Rules for "Done when":**
- Each item must be independently verifiable (not "works correctly" — HOW do you check?)
- Include at least one behavioral assertion (not just "code exists")
- Always include cargo check + cargo test as final items

**Rules for "Verify":**
- Must be a concrete command or sequence, not "verify it works"
- If the feature is a CLI command, show the exact invocation and expected output
- If the feature is internal, specify a test name or manual check

**Rules for "Affected files":**
- List every file the implementer will need to create or modify
- Mark each as `new` or `modified`
- This feeds the parallel agent scheduler's conflict detection

**Splitting heuristic**: If a feature touches 3+ files AND requires both backend + frontend work AND needs new tests, it's likely too large for one bead. Split along these boundaries:
- Data model / CPT changes (backend)
- Admin UI / meta boxes (backend)
- Frontend display / templates (frontend)
- REST API endpoints (backend)
- Tests for each of the above (can bundle with its implementation bead)

### 3. Identify dependencies

Map which beads block others:
- Data model beads block UI beads that display that data
- Backend API beads block frontend beads that consume them
- Infrastructure beads (new CPT, taxonomy, config) block feature beads that use them
- Shared components block features that depend on them

### 4. Create all beads

Run `bd create` for each bead:

```bash
bd create --title="<title>" --type=<type> --priority=<N>
```

Capture the returned bead ID from each create command.

Then update each bead with its description:

```bash
bd update <id> --description="<description>"
```

### 5. Wire dependencies

For each dependency relationship, run:

```bash
bd dep add <blocked-issue> <blocker-issue>
```

The first argument is the issue that is BLOCKED, the second is the issue it DEPENDS ON.

### 6. Output summary

Print a table showing all created beads:

```
| ID   | Title                          | Type    | Pri | Blocked By |
|------|--------------------------------|---------|-----|------------|
| abc1 | Add event CPT                  | feature | 1   | -          |
| abc2 | Add ticket meta box            | feature | 1   | abc1       |
| abc3 | Add RSVP frontend form         | feature | 2   | abc1       |
```

Then run `bd blocked` to verify the dependency graph looks correct.

## Guidelines

- Prefer more smaller beads over fewer large ones
- Every bead title starts with a verb: Add, Implement, Create, Fix, Update, Refactor, Remove
- Don't create beads for documentation, README updates, or changelog entries unless the PRD explicitly requires them
- If the PRD references existing code/features, note the relevant files in the bead description so the implementer doesn't have to search
- Group related beads by creating them sequentially (the bd IDs will be adjacent)
