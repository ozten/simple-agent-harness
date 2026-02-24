#!/usr/bin/env bash
# Session close script — replaces the 12-step shutdown ritual
# Usage: ./bd-finish.sh <bead-id> "<commit-message>" [files...]
#
# If no files are given, stages all tracked modified files (git add -u).
# Always includes PROGRESS.txt and PROGRESS_LOG.txt in the commit.
#
# Steps:
#   0a. Run cargo check — abort if code doesn't compile
#   0b. Run cargo test — abort if tests fail
#   0c. Verify bead deliverables — check affected files exist, run verify commands
#   1. Append PROGRESS.txt to PROGRESS_LOG.txt (timestamped)
#   2. Stage specified files (or git add -u)
#   3. git commit
#   4. bd close
#   5. bd sync
#   6. Auto-commit any .beads/ changes
#   7. git push

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'

if [ $# -lt 2 ]; then
  echo -e "${RED}Usage: ./bd-finish.sh <bead-id> \"<commit-message>\" [files...]${NC}"
  echo "  If no files given, stages all tracked modified files (git add -u)"
  exit 1
fi

BEAD_ID="$1"
COMMIT_MSG="$2"
shift 2
FILES=("$@")

echo -e "${GREEN}=== bd-finish: closing ${BEAD_ID} ===${NC}"

# 0a. Cargo check gate — abort if code doesn't compile
echo -e "${YELLOW}[0a] Running cargo check...${NC}"
if ! cargo check --release 2>&1; then
  echo ""
  echo -e "${RED}=== CARGO CHECK FAILED ===${NC}"
  echo -e "${RED}Bead ${BEAD_ID} will NOT be closed. Fix compilation errors first.${NC}"
  exit 1
fi
echo -e "${GREEN}[0a] cargo check passed${NC}"

# 0b. Cargo test gate — abort if tests fail
echo -e "${YELLOW}[0b] Running cargo test...${NC}"
if ! cargo test --release 2>&1; then
  echo ""
  echo -e "${RED}=== CARGO TEST FAILED ===${NC}"
  echo -e "${RED}Bead ${BEAD_ID} will NOT be closed. Fix failing tests first.${NC}"
  exit 1
fi
echo -e "${GREEN}[0b] cargo test passed${NC}"

# 0c. Lint gate
echo -e "${YELLOW}[0c] Running lint gate...${NC}"
if ! cargo clippy --fix --allow-dirty 2>&1; then
  echo ""
  echo -e "${RED}=== LINT GATE FAILED ===${NC}"
  echo -e "${RED}Bead ${BEAD_ID} will NOT be closed. Fix lint errors first.${NC}"
  exit 1
fi
echo -e "${GREEN}[0c] Lint gate passed${NC}"

# 0d. Format gate
echo -e "${YELLOW}[0d] Running format gate...${NC}"
if ! cargo fmt --check 2>&1; then
  echo ""
  echo -e "${RED}=== FORMAT GATE FAILED ===${NC}"
  echo -e "${RED}Bead ${BEAD_ID} will NOT be closed. Fix formatting errors first.${NC}"
  exit 1
fi
echo -e "${GREEN}[0d] Format gate passed${NC}"

# 0e. Verify bead deliverables — check affected files exist, run verify commands
echo -e "${YELLOW}[0e] Verifying bead deliverables...${NC}"
VERIFY_FAILED=0

# Extract bead description via bd show --json
BEAD_DESC=""
if BEAD_JSON=$(bd show "$BEAD_ID" --allow-stale --json 2>/dev/null); then
  BEAD_DESC=$(printf '%s' "$BEAD_JSON" | jq -r '.[0].description // ""' 2>/dev/null || true)
fi

if [ -z "$BEAD_DESC" ]; then
  echo -e "${YELLOW}  Could not fetch bead description — skipping deliverable verification${NC}"
else
  # --- Check ## Affected files: verify files marked (new) actually exist ---
  AFFECTED_SECTION=$(printf '%s\n' "$BEAD_DESC" | sed -n '/^## Affected files/,/^## /p' | sed '1d;/^## /d')
  if [ -n "$AFFECTED_SECTION" ]; then
    while IFS= read -r line; do
      # Match lines like "- src/foo.rs (new)" or "- src/foo.rs (new — description)"
      if echo "$line" | grep -qiE '\(new\b'; then
        # Extract the file path: strip leading "- " and trailing " (new...)"
        FILE_PATH=$(echo "$line" | sed 's/^[[:space:]]*-[[:space:]]*//' | sed 's/[[:space:]]*(new.*//')
        if [ ! -f "$FILE_PATH" ]; then
          echo -e "${RED}  MISSING: Affected file marked (new) does not exist: ${FILE_PATH}${NC}"
          VERIFY_FAILED=1
        else
          echo -e "${GREEN}  OK: ${FILE_PATH} exists${NC}"
        fi
      fi
    done <<< "$AFFECTED_SECTION"
  fi

  # --- Check ## Affected files: verify files marked (modified) actually changed ---
  if [ -n "$AFFECTED_SECTION" ]; then
    while IFS= read -r line; do
      if echo "$line" | grep -qiE '\(modified\b'; then
        FILE_PATH=$(echo "$line" | sed 's/^[[:space:]]*-[[:space:]]*//' | sed 's/[[:space:]]*(modified.*//')
        if [ ! -f "$FILE_PATH" ]; then
          echo -e "${RED}  MISSING: Affected file marked (modified) does not exist: ${FILE_PATH}${NC}"
          VERIFY_FAILED=1
        elif git diff --quiet HEAD -- "$FILE_PATH" 2>/dev/null && git diff --quiet -- "$FILE_PATH" 2>/dev/null; then
          echo -e "${YELLOW}  WARNING: ${FILE_PATH} listed as (modified) but has no changes${NC}"
          # Warn but don't fail — the agent may have changed and reverted it
        else
          echo -e "${GREEN}  OK: ${FILE_PATH} modified${NC}"
        fi
      fi
    done <<< "$AFFECTED_SECTION"
  fi

  # --- Run ## Verify commands ---
  VERIFY_SECTION=$(printf '%s\n' "$BEAD_DESC" | sed -n '/^## Verify$/,/^## /p' | sed '1d;/^## /d')
  if [ -n "$VERIFY_SECTION" ]; then
    # Extract "Run:" lines and execute them
    while IFS= read -r line; do
      if echo "$line" | grep -qiE '^[[:space:]]*-?[[:space:]]*Run:'; then
        VERIFY_CMD=$(echo "$line" | sed 's/^[[:space:]]*-[[:space:]]*Run:[[:space:]]*//' | sed 's/^[[:space:]]*Run:[[:space:]]*//')
        if [ -n "$VERIFY_CMD" ]; then
          echo -e "${YELLOW}  Running verify command: ${VERIFY_CMD}${NC}"
          if eval "$VERIFY_CMD" 2>&1; then
            echo -e "${GREEN}  Verify command passed${NC}"
          else
            echo -e "${RED}  FAILED: Verify command exited non-zero: ${VERIFY_CMD}${NC}"
            VERIFY_FAILED=1
          fi
        fi
      fi
    done <<< "$VERIFY_SECTION"
  else
    echo -e "${YELLOW}  No ## Verify section found in bead description — skipping command verification${NC}"
  fi
fi

if [ "$VERIFY_FAILED" -eq 1 ]; then
  echo ""
  echo -e "${RED}=== BEAD DELIVERABLE VERIFICATION FAILED ===${NC}"
  echo -e "${RED}Bead ${BEAD_ID} will NOT be closed. Fix the issues above first.${NC}"
  echo -e "${RED}If the bead description is outdated, update it with:${NC}"
  echo -e "${RED}  bd update ${BEAD_ID} --description=\"...\"${NC}"
  exit 1
fi
echo -e "${GREEN}[0e] Bead deliverable verification passed${NC}"

# 1. Append PROGRESS.txt to PROGRESS_LOG.txt with timestamp
if [ -f PROGRESS.txt ]; then
  echo "" >> PROGRESS_LOG.txt
  echo "--- $(date '+%Y-%m-%d %H:%M:%S') | ${BEAD_ID} ---" >> PROGRESS_LOG.txt
  cat PROGRESS.txt >> PROGRESS_LOG.txt
  echo -e "${GREEN}[1] Appended PROGRESS.txt to PROGRESS_LOG.txt${NC}"
else
  echo -e "${YELLOW}[1] No PROGRESS.txt found, skipping log append${NC}"
fi

# 2. Stage files
if [ ${#FILES[@]} -gt 0 ]; then
  git add "${FILES[@]}"
  echo -e "${GREEN}[2] Staged ${#FILES[@]} specified files${NC}"
else
  git add -u
  echo -e "${GREEN}[2] Staged all tracked modified files (git add -u)${NC}"
fi
# Always include the progress files if they exist
git add -f PROGRESS.txt PROGRESS_LOG.txt 2>/dev/null || true

# 3. Commit
git commit -m "${BEAD_ID}: ${COMMIT_MSG}" --no-verify
echo -e "${GREEN}[3] Committed: ${BEAD_ID}: ${COMMIT_MSG}${NC}"

# 4. bd close
bd close "$BEAD_ID" --reason="$COMMIT_MSG"
echo -e "${GREEN}[4] Closed bead ${BEAD_ID}${NC}"

# 5. bd sync
bd sync 2>/dev/null || true
echo -e "${GREEN}[5] Synced beads${NC}"

# 6. Auto-commit .beads/ if dirty
if ! git diff --quiet .beads/ 2>/dev/null || ! git diff --cached --quiet .beads/ 2>/dev/null; then
  git add .beads/
  git commit -m "bd sync: $(date '+%Y-%m-%d %H:%M:%S')" --no-verify
  echo -e "${GREEN}[6] Committed .beads/ changes${NC}"
else
  echo -e "${GREEN}[6] .beads/ already clean${NC}"
fi

# 7. Push
git push
echo -e "${GREEN}[7] Pushed to remote${NC}"

echo ""
echo -e "${GREEN}=== Done. ${BEAD_ID} closed and pushed. ===${NC}"
