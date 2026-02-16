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
echo -e "${YELLOW}[0a/8] Running cargo check...${NC}"
if ! cargo check 2>&1; then
  echo ""
  echo -e "${RED}=== CARGO CHECK FAILED ===${NC}"
  echo -e "${RED}Bead ${BEAD_ID} will NOT be closed. Fix compilation errors first.${NC}"
  exit 1
fi
echo -e "${GREEN}[0a/8] cargo check passed${NC}"

# 0b. Cargo test gate — abort if tests fail
echo -e "${YELLOW}[0b/8] Running cargo test...${NC}"
if ! cargo test 2>&1; then
  echo ""
  echo -e "${RED}=== CARGO TEST FAILED ===${NC}"
  echo -e "${RED}Bead ${BEAD_ID} will NOT be closed. Fix failing tests first.${NC}"
  exit 1
fi
echo -e "${GREEN}[0b/8] cargo test passed${NC}"

# 1. Append PROGRESS.txt to PROGRESS_LOG.txt with timestamp
if [ -f PROGRESS.txt ]; then
  echo "" >> PROGRESS_LOG.txt
  echo "--- $(date '+%Y-%m-%d %H:%M:%S') | ${BEAD_ID} ---" >> PROGRESS_LOG.txt
  cat PROGRESS.txt >> PROGRESS_LOG.txt
  echo -e "${GREEN}[1/8] Appended PROGRESS.txt to PROGRESS_LOG.txt${NC}"
else
  echo -e "${YELLOW}[1/8] No PROGRESS.txt found, skipping log append${NC}"
fi

# 2. Stage files
if [ ${#FILES[@]} -gt 0 ]; then
  git add "${FILES[@]}"
  echo -e "${GREEN}[2/8] Staged ${#FILES[@]} specified files${NC}"
else
  git add -u
  echo -e "${GREEN}[2/8] Staged all tracked modified files (git add -u)${NC}"
fi
# Always include the progress files if they exist
git add -f PROGRESS.txt PROGRESS_LOG.txt 2>/dev/null || true

# 3. Commit
git commit -m "${BEAD_ID}: ${COMMIT_MSG}" --no-verify
echo -e "${GREEN}[3/8] Committed: ${BEAD_ID}: ${COMMIT_MSG}${NC}"

# 4. bd close
bd close "$BEAD_ID" --reason="$COMMIT_MSG"
echo -e "${GREEN}[4/8] Closed bead ${BEAD_ID}${NC}"

# 5. bd sync
bd sync 2>/dev/null || true
echo -e "${GREEN}[5/8] Synced beads${NC}"

# 6. Auto-commit .beads/ if dirty
if ! git diff --quiet .beads/ 2>/dev/null || ! git diff --cached --quiet .beads/ 2>/dev/null; then
  git add .beads/
  git commit -m "bd sync: $(date '+%Y-%m-%d %H:%M:%S')" --no-verify
  echo -e "${GREEN}[6/8] Committed .beads/ changes${NC}"
else
  echo -e "${GREEN}[6/8] .beads/ already clean${NC}"
fi

# 7. Push
git push
echo -e "${GREEN}[7/8] Pushed to remote${NC}"

echo ""
echo -e "${GREEN}=== Done. ${BEAD_ID} closed and pushed. ===${NC}"
