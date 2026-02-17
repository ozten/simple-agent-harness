#!/usr/bin/env bash
#
# smoke-test-ui.sh — End-to-end smoke test for blacksmith serve + blacksmith-ui
#
# Usage:
#   ./scripts/smoke-test-ui.sh [PROJECT_DIR]
#
# Arguments:
#   PROJECT_DIR  Path to a blacksmith project (default: ../cantrip)
#
# Prerequisites:
#   - cargo build --release --features serve
#   - cargo build --release -p blacksmith-ui
#
# What it does:
#   1. Starts `blacksmith serve` in PROJECT_DIR
#   2. Validates all serve API endpoints return 200
#   3. Starts `blacksmith-ui` pointing at the serve instance
#   4. Validates all dashboard API endpoints return 200
#   5. Validates proxied transcript/stream endpoints
#   6. Tears down both processes
#
# Exit codes:
#   0 — all checks passed
#   1 — one or more checks failed

set -euo pipefail

PROJECT_DIR="${1:-../cantrip}"
SERVE_PORT=18420
UI_PORT=18080
SERVE_PID=""
UI_PID=""
PASS=0
FAIL=0
FAILED_CHECKS=()

# Colors (disabled if not a tty)
if [ -t 1 ]; then
  GREEN='\033[0;32m'
  RED='\033[0;31m'
  YELLOW='\033[0;33m'
  BOLD='\033[1m'
  RESET='\033[0m'
else
  GREEN='' RED='' YELLOW='' BOLD='' RESET=''
fi

cleanup() {
  echo ""
  echo -e "${BOLD}Cleaning up...${RESET}"
  [ -n "$UI_PID" ] && kill "$UI_PID" 2>/dev/null && wait "$UI_PID" 2>/dev/null || true
  [ -n "$SERVE_PID" ] && kill "$SERVE_PID" 2>/dev/null && wait "$SERVE_PID" 2>/dev/null || true
}
trap cleanup EXIT

check_endpoint() {
  local label="$1"
  local url="$2"
  local expected_status="${3:-200}"

  local status
  status=$(curl -s -o /dev/null -w "%{http_code}" --max-time 5 "$url" 2>/dev/null) || status="000"

  if [ "$status" = "$expected_status" ]; then
    echo -e "  ${GREEN}PASS${RESET}  $label (HTTP $status)"
    PASS=$((PASS + 1))
  else
    echo -e "  ${RED}FAIL${RESET}  $label (expected HTTP $expected_status, got $status)"
    FAIL=$((FAIL + 1))
    FAILED_CHECKS+=("$label")
  fi
}

check_json_field() {
  local label="$1"
  local url="$2"
  local jq_filter="$3"

  local body
  body=$(curl -s --max-time 5 "$url" 2>/dev/null) || body=""
  local value
  value=$(echo "$body" | jq -r "$jq_filter" 2>/dev/null) || value=""

  if [ -n "$value" ] && [ "$value" != "null" ] && [ "$value" != "" ]; then
    echo -e "  ${GREEN}PASS${RESET}  $label ($jq_filter = $value)"
    PASS=$((PASS + 1))
  else
    echo -e "  ${RED}FAIL${RESET}  $label ($jq_filter was empty/null)"
    FAIL=$((FAIL + 1))
    FAILED_CHECKS+=("$label")
  fi
}

wait_for_port() {
  local port="$1"
  local label="$2"
  local max_wait=15
  local waited=0

  echo -n "  Waiting for $label on port $port..."
  while ! curl -s -o /dev/null --max-time 1 "http://127.0.0.1:$port/api/health" 2>/dev/null; do
    sleep 1
    waited=$((waited + 1))
    if [ $waited -ge $max_wait ]; then
      echo -e " ${RED}TIMEOUT${RESET} (${max_wait}s)"
      return 1
    fi
  done
  echo -e " ${GREEN}OK${RESET} (${waited}s)"
  return 0
}

# ─── Preflight ───────────────────────────────────────────────────────────────

echo -e "${BOLD}Smoke Test: blacksmith serve + blacksmith-ui${RESET}"
echo ""

# Check project dir
if [ ! -d "$PROJECT_DIR/.blacksmith" ]; then
  echo -e "${RED}ERROR${RESET}: $PROJECT_DIR is not a blacksmith project (no .blacksmith/ dir)"
  exit 1
fi
echo -e "  Project: ${BOLD}$PROJECT_DIR${RESET}"

# Check binaries
BLACKSMITH_BIN="$(cd "$(dirname "$0")/.." && pwd)/target/release/blacksmith"
UI_BIN="$(cd "$(dirname "$0")/.." && pwd)/target/release/blacksmith-ui"

if [ ! -x "$BLACKSMITH_BIN" ]; then
  echo -e "${RED}ERROR${RESET}: blacksmith binary not found at $BLACKSMITH_BIN"
  echo "  Run: cargo build --release --features serve"
  exit 1
fi

if [ ! -x "$UI_BIN" ]; then
  echo -e "${RED}ERROR${RESET}: blacksmith-ui binary not found at $UI_BIN"
  echo "  Run: cargo build --release -p blacksmith-ui"
  exit 1
fi

echo ""

# ─── Phase 1: blacksmith serve ──────────────────────────────────────────────

echo -e "${BOLD}Phase 1: blacksmith serve${RESET}"

(cd "$PROJECT_DIR" && "$BLACKSMITH_BIN" serve --port "$SERVE_PORT" &>/dev/null) &
SERVE_PID=$!

if ! wait_for_port "$SERVE_PORT" "blacksmith serve"; then
  echo -e "${RED}FATAL${RESET}: blacksmith serve did not start"
  exit 1
fi

SERVE_BASE="http://127.0.0.1:$SERVE_PORT"

echo ""
echo -e "  ${BOLD}Checking serve endpoints:${RESET}"
check_endpoint "GET /api/health"          "$SERVE_BASE/api/health"
check_endpoint "GET /api/status"          "$SERVE_BASE/api/status"
check_endpoint "GET /api/project"         "$SERVE_BASE/api/project"
check_endpoint "GET /api/beads"           "$SERVE_BASE/api/beads"
check_endpoint "GET /api/sessions"        "$SERVE_BASE/api/sessions"
check_endpoint "GET /api/metrics/summary" "$SERVE_BASE/api/metrics/summary"
check_endpoint "GET /api/improvements"    "$SERVE_BASE/api/improvements"
check_endpoint "GET /api/estimate"        "$SERVE_BASE/api/estimate"

# Validate non-trivial JSON fields
echo ""
echo -e "  ${BOLD}Checking response payloads:${RESET}"
check_json_field "health.ok"      "$SERVE_BASE/api/health"  ".ok"
check_json_field "project.name"   "$SERVE_BASE/api/project" ".name"

# Check a session endpoint if sessions exist
SESSION_ID=$(curl -s --max-time 5 "$SERVE_BASE/api/sessions" 2>/dev/null | jq -r '.[0].id // empty' 2>/dev/null) || SESSION_ID=""
if [ -n "$SESSION_ID" ]; then
  echo ""
  echo -e "  ${BOLD}Checking session/transcript endpoints (session $SESSION_ID):${RESET}"
  check_endpoint "GET /api/sessions/$SESSION_ID"             "$SERVE_BASE/api/sessions/$SESSION_ID"
  check_endpoint "GET /api/sessions/$SESSION_ID/transcript"  "$SERVE_BASE/api/sessions/$SESSION_ID/transcript"
  # SSE stream: just check it connects (returns 200), don't wait for events
  check_endpoint "GET /api/sessions/$SESSION_ID/stream"      "$SERVE_BASE/api/sessions/$SESSION_ID/stream"
else
  echo -e "  ${YELLOW}SKIP${RESET}  No sessions found — skipping session/transcript checks"
fi

echo ""

# ─── Phase 2: blacksmith-ui ─────────────────────────────────────────────────

echo -e "${BOLD}Phase 2: blacksmith-ui${RESET}"

# Create a temporary config pointing at our serve instance
UI_WORKDIR=$(mktemp -d)
cat > "$UI_WORKDIR/blacksmith-ui.toml" <<EOF
[dashboard]
port = $UI_PORT
bind = "127.0.0.1"
poll_interval_secs = 2

[[projects]]
name = "cantrip"
url = "http://127.0.0.1:$SERVE_PORT"
EOF

(cd "$UI_WORKDIR" && "$UI_BIN" &>/dev/null) &
UI_PID=$!

if ! wait_for_port "$UI_PORT" "blacksmith-ui"; then
  echo -e "${RED}FATAL${RESET}: blacksmith-ui did not start"
  exit 1
fi

UI_BASE="http://127.0.0.1:$UI_PORT"

# Give the poller a moment to fetch from the serve instance
sleep 3

ENCODED_URL=$(python3 -c "import urllib.parse; print(urllib.parse.quote('http://127.0.0.1:$SERVE_PORT', safe=''))")

echo ""
echo -e "  ${BOLD}Checking dashboard endpoints:${RESET}"
check_endpoint "GET /api/health"                          "$UI_BASE/api/health"
check_endpoint "GET /api/instances"                       "$UI_BASE/api/instances"
check_endpoint "GET /api/aggregate"                       "$UI_BASE/api/aggregate"
check_endpoint "GET /api/global-metrics"                  "$UI_BASE/api/global-metrics"
check_endpoint "GET /api/instances/{url}/poll-data"       "$UI_BASE/api/instances/$ENCODED_URL/poll-data"
check_endpoint "GET /api/instances/{url}/estimate"        "$UI_BASE/api/instances/$ENCODED_URL/estimate"

# Proxied transcript endpoints (if sessions exist)
if [ -n "$SESSION_ID" ]; then
  echo ""
  echo -e "  ${BOLD}Checking proxied transcript endpoints:${RESET}"
  check_endpoint "GET /api/instances/{url}/sessions/{id}/transcript" \
    "$UI_BASE/api/instances/$ENCODED_URL/sessions/$SESSION_ID/transcript"
  check_endpoint "GET /api/instances/{url}/sessions/{id}/stream" \
    "$UI_BASE/api/instances/$ENCODED_URL/sessions/$SESSION_ID/stream"
else
  echo -e "  ${YELLOW}SKIP${RESET}  No sessions — skipping proxied transcript checks"
fi

# Clean up temp dir
rm -rf "$UI_WORKDIR"

echo ""

# ─── Summary ────────────────────────────────────────────────────────────────

echo -e "${BOLD}════════════════════════════════════════${RESET}"
echo -e "  ${GREEN}Passed${RESET}: $PASS"
echo -e "  ${RED}Failed${RESET}: $FAIL"

if [ ${#FAILED_CHECKS[@]} -gt 0 ]; then
  echo ""
  echo -e "  ${RED}Failed checks:${RESET}"
  for check in "${FAILED_CHECKS[@]}"; do
    echo -e "    - $check"
  done
fi

echo -e "${BOLD}════════════════════════════════════════${RESET}"

if [ "$FAIL" -gt 0 ]; then
  echo ""
  echo -e "${RED}SMOKE TEST FAILED${RESET}"
  exit 1
else
  echo ""
  echo -e "${GREEN}ALL CHECKS PASSED${RESET}"
  exit 0
fi
