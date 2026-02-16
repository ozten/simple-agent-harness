# Blacksmith V6: Multi-Project Dashboard

## Motivation

Blacksmith runs autonomously — that's the point. But the operator still needs
visibility. Today, that means SSH into the machine, run `blacksmith --status`,
`bd list`, `blacksmith metrics status`, one project at a time. With N
blacksmiths running across Y projects on Z machines, this doesn't scale.

The goal: a single web dashboard that shows every blacksmith instance across
every project and machine, with live session transcripts, bead status,
metrics, and one-click controls. Open a browser tab, see everything.

---

## Design Principles

1. **Pull model.** The dashboard polls instances; instances don't need to know
   the dashboard exists. This means zero config on the blacksmith side beyond
   `blacksmith serve`, and the dashboard can be started/stopped/moved without
   affecting running loops.

2. **Separate crate.** The dashboard is `blacksmith-ui`, a separate binary in
   a Cargo workspace. The main `blacksmith` binary gains a `serve` subcommand
   (feature-gated) but no frontend code. Default `cargo build` stays fast.

3. **Lightweight frontend.** This is a dashboard, not an app. Mostly read-only
   with a few actions. No WASM framework (Leptos, Yew). Static HTML + vanilla
   JS or a lightweight framework (Preact/Svelte), bundled into the binary.

4. **Discovery, not configuration.** LAN instances are auto-discovered via UDP
   multicast. Remote instances can be added manually. The operator shouldn't
   maintain a registry.

---

## Architecture

```
blacksmith (per-project, per-machine)
├── blacksmith serve --port 8420       # JSON API + SSE
│   ├── GET  /api/status               # Coordinator state, workers, iterations
│   ├── GET  /api/project              # Project name, repo path, config summary
│   ├── GET  /api/beads                # Bead listing with filters
│   ├── GET  /api/beads/:id            # Single bead detail
│   ├── GET  /api/sessions             # Session list with metadata
│   ├── GET  /api/sessions/:id         # Session metadata + metrics
│   ├── GET  /api/sessions/:id/stream  # SSE: live pretty-printed transcript
│   ├── GET  /api/metrics/summary      # Aggregated stats, averages, estimates
│   ├── GET  /api/metrics/timeseries   # Cost, tokens, duration over time
│   ├── GET  /api/improvements         # Self-improvement records
│   ├── POST /api/stop                 # Touch STOP file
│   └── GET  /api/health               # Liveness probe (returns 200)
└── UDP multicast heartbeat (optional) # Auto-discovery beacon

blacksmith-ui (single instance, the dashboard)
├── HTTP server on :8080               # Serves the dashboard
├── UDP multicast listener             # Auto-discovers LAN instances
├── Poller                             # Polls each instance's API
├── blacksmith-ui.toml                 # Manual instance registry + settings
└── Bundled static frontend assets     # HTML/JS/CSS
```

### Data Flow

```
                      UDP heartbeat (every 30s)
  blacksmith A  ─────────────────────────────────►  blacksmith-ui
  blacksmith B  ─────────────────────────────────►     │
  blacksmith C  ─────────────────────────────────►     │
                                                       │
                      HTTP polling (every 5s)          │
  blacksmith A  ◄─────────────────────────────────     │
  blacksmith B  ◄─────────────────────────────────     │
  blacksmith C  ◄─────────────────────────────────     │
                                                       │
                      SSE (persistent connection)      │
  blacksmith A  ─────────────────────────────────►     │
                (only for actively-viewed transcript)  │
                                                       │
                      Browser                          │
                  ◄────────────────────────────────    │
                  (WebSocket or polling from browser)
```

---

## Milestone 1: `blacksmith serve` — Per-Project API

Add a `serve` subcommand to the main blacksmith binary behind a `serve`
Cargo feature flag. This is the foundation everything else builds on.

### 1a. HTTP API Server

**Implementation:** Add `axum` and `tower-http` (CORS) as optional
dependencies behind `features = ["serve"]`. The server reads from the same
data sources the CLI commands use (SQLite DB, status file, session files,
beads CLI).

```bash
blacksmith serve                  # Default port 8420
blacksmith serve --port 9000      # Custom port
blacksmith serve --bind 0.0.0.0   # Bind address (default: 0.0.0.0)
```

**Endpoints:**

| Endpoint | Method | Description | Data Source |
|---|---|---|---|
| `/api/health` | GET | Returns `{"ok": true}` | — |
| `/api/project` | GET | Project name, repo root, config summary | `blacksmith.toml`, git |
| `/api/status` | GET | Loop state, iteration counts, PID, uptime, workers | `.blacksmith/status` |
| `/api/beads` | GET | Bead listing; `?status=open\|in_progress\|closed` | `bd list` / `bd ready` |
| `/api/beads/:id` | GET | Single bead with deps, notes, design | `bd show :id` |
| `/api/sessions` | GET | Session list; `?last=N` | `.blacksmith/sessions/` |
| `/api/sessions/:id` | GET | Session metadata + extracted metrics | SQLite `events` table |
| `/api/sessions/:id/stream` | GET | SSE stream of pretty-printed transcript lines | Session JSONL file |
| `/api/metrics/summary` | GET | Averages, totals, estimates, target compliance | SQLite `observations` |
| `/api/metrics/timeseries` | GET | Per-session cost, tokens, duration; `?last=N` | SQLite `events` |
| `/api/improvements` | GET | All improvements; `?status=open\|promoted\|...` | SQLite `improvements` |
| `/api/stop` | POST | Touches STOP file, returns `{"stopped": true}` | Filesystem |

**Session transcript SSE format:**

```
event: turn
data: {"role": "assistant", "type": "text", "content": "I'll read the file..."}

event: turn
data: {"role": "assistant", "type": "tool_use", "tool": "Read", "input": {"path": "src/main.rs"}}

event: turn
data: {"role": "tool", "tool": "Read", "output": "fn main() { ... }"}

event: done
data: {"session_id": 142, "outcome": "productive"}
```

For live (in-progress) sessions, the SSE endpoint tails the JSONL file and
emits events as lines are appended. For completed sessions, it replays the
full file and closes. Compressed `.jsonl.zst` files are transparently
decompressed.

**SQLite access:** The serve process opens the database in read-only mode
(`SQLITE_OPEN_READ_ONLY`). The main blacksmith loop is the only writer.
WAL mode allows concurrent readers.

#### Config

```toml
[serve]
port = 8420
bind = "0.0.0.0"
```

#### Verification

- **V1a-1:** `blacksmith serve` starts, `curl localhost:8420/api/health`
  returns `{"ok": true}`.
- **V1a-2:** `curl localhost:8420/api/status` returns JSON matching
  `blacksmith --status` output while a loop is running.
- **V1a-3:** `curl localhost:8420/api/beads` returns JSON matching
  `bd list --format=json` output.
- **V1a-4:** `curl localhost:8420/api/sessions/142/stream` returns SSE
  events for a completed session, including all turns.
- **V1a-5:** `curl localhost:8420/api/sessions/142/stream` for a
  `.jsonl.zst` file returns decompressed content.
- **V1a-6:** `curl -X POST localhost:8420/api/stop` creates the STOP file;
  a running loop exits gracefully.
- **V1a-7:** Running `blacksmith serve` while a loop is running on the same
  project works (read-only DB access, no lock conflict).
- **V1a-8:** Default `cargo build` (without `--features serve`) does not
  pull in `axum` or any serve-related dependencies.

#### Done when

- All endpoints return correct JSON for a project with active sessions,
  beads, metrics, and improvements.
- SSE streaming works for both live and completed sessions, including
  zstd-compressed historical sessions.
- Feature gate works: `cargo build` is unaffected, `cargo build --features
  serve` builds the serve subcommand.

---

### 1b. UDP Multicast Heartbeat

Each `blacksmith serve` instance broadcasts a heartbeat packet on a known
multicast group so dashboards can discover it without configuration.

**Multicast group:** `239.66.83.77:8421` (BS = 66.83 in ASCII)

**Heartbeat payload (JSON, <512 bytes to fit one UDP packet):**

```json
{
  "v": 1,
  "project": "blacksmith",
  "api": "http://10.0.1.5:8420",
  "status": "running",
  "workers_active": 2,
  "workers_max": 3,
  "iteration": 47,
  "max_iterations": 100,
  "pid": 12345
}
```

**Broadcast interval:** Every 30 seconds.

**Implementation:** Use `socket2` crate for multicast UDP. Spawn a background
tokio task in the serve process that sends the heartbeat. The `api` field is
auto-detected from the bind address and port (or configurable override for
NAT/proxy scenarios).

```toml
[serve]
heartbeat = true                # Default: true when serve is running
heartbeat_address = "239.66.83.77:8421"
api_advertise = "http://myhost:8420"  # Override for NAT (optional)
```

#### Verification

- **V1b-1:** Start `blacksmith serve`, listen on the multicast group with
  `socat`, confirm heartbeat packets arrive every ~30s.
- **V1b-2:** Heartbeat `api` field reflects the actual bound address and port.
- **V1b-3:** `heartbeat = false` in config suppresses heartbeat.
- **V1b-4:** `api_advertise` overrides the auto-detected URL.
- **V1b-5:** Two `blacksmith serve` instances on the same machine (different
  ports/projects) both broadcast without conflict.

#### Done when

- Heartbeat packets are receivable on the multicast group from another
  machine on the same LAN.
- Payload contains all fields needed for the dashboard to display the
  project in its list and probe the API.

---

### 1c. Port Allocation

Multiple blacksmith instances on the same machine need distinct ports.

**Behavior:**

1. If `[serve] port` is set in config, use it. Fail if occupied.
2. Otherwise, try 8420. If occupied, try 8421, 8422, ... up to 8430.
3. The actual bound port is advertised in the UDP heartbeat.

#### Verification

- **V1c-1:** Two instances on the same machine without explicit port config
  bind to 8420 and 8421 respectively.
- **V1c-2:** An instance with `port = 9000` in config always binds to 9000.
- **V1c-3:** Heartbeat advertises the actual bound port, not the configured
  default.

#### Done when

- N instances on one machine each get a unique port without manual config.

---

## Milestone 2: `blacksmith-ui` — The Dashboard Binary

A separate crate in a Cargo workspace that aggregates data from multiple
`blacksmith serve` instances and serves a web dashboard.

### 2a. Project Discovery and Registry

The dashboard discovers instances through three mechanisms (layered):

**1. UDP multicast listener.** Listens on `239.66.83.77:8421`, parses
heartbeat packets, adds/updates instances in an in-memory registry. Instances
that haven't sent a heartbeat in 90 seconds are marked `offline`.

**2. Manual registry in config.**

```toml
# blacksmith-ui.toml

[dashboard]
port = 8080
bind = "0.0.0.0"
poll_interval_secs = 5

[[projects]]
name = "blacksmith"
url = "http://devbox-1:8420"

[[projects]]
name = "webapp"
url = "http://devbox-2:8420"
```

**3. Runtime add via UI.** A text field in the dashboard: enter a URL, it
probes `/api/health`, adds to the registry if it responds. Persisted to a
local file so it survives restart.

**Merged registry:** All three sources merge. UDP-discovered instances are
matched to manual entries by URL. Duplicates are deduplicated. Manual entries
that are also UDP-discovered get the richer heartbeat metadata.

#### Verification

- **V2a-1:** Start `blacksmith-ui` on the same LAN as a `blacksmith serve`
  instance. The project appears in the dashboard within 60s without any
  configuration.
- **V2a-2:** A manually configured project appears immediately on dashboard
  start.
- **V2a-3:** Adding a URL via the UI probes the health endpoint and adds the
  project. It persists across dashboard restart.
- **V2a-4:** An instance that stops sending heartbeats transitions to
  `offline` after 90s.
- **V2a-5:** A UDP-discovered instance and a manually configured instance
  with the same URL appear as one entry, not two.

#### Done when

- Projects are discoverable via all three mechanisms.
- The merged registry correctly deduplicates and tracks liveness.

---

### 2b. Polling and Aggregation

The dashboard polls each known instance's API at a configurable interval
(default: 5s) and aggregates the results.

**Per-instance polling:**

| Endpoint | Frequency | Purpose |
|---|---|---|
| `/api/health` | Every poll | Detect online/offline transitions |
| `/api/status` | Every poll | Live state for project list |
| `/api/project` | Once on discovery, then on reconnect | Static project metadata |
| `/api/beads` | Every poll | Bead counts and status |
| `/api/metrics/summary` | Every poll | Stats for project card |
| `/api/improvements` | Every 30s | Lower priority, less volatile |

**Cross-project aggregation (computed in the dashboard):**

| Metric | Computation |
|---|---|
| Total beads open | Sum of open beads across all projects |
| Total beads in flight | Sum of in_progress beads across all projects |
| Aggregate cost today | Sum of cost.estimate_usd for sessions started today |
| Attention queue | Union of needs_review beads + circuit breaker trips |
| Global worker utilization | Active workers / max workers across all projects |

#### Verification

- **V2b-1:** Dashboard shows updated iteration counts within 10s of a
  session completing on any instance.
- **V2b-2:** Cross-project aggregate metrics are correct when verified
  against manual sums of individual instance data.
- **V2b-3:** An instance going offline mid-poll doesn't crash the dashboard;
  it shows the last known state with an offline indicator.
- **V2b-4:** Poll interval is configurable and respected.

#### Done when

- The dashboard has a live, aggregated view of all instances.
- Offline instances degrade gracefully (stale data shown, not crash).

---

### 2c. Dashboard Frontend

The frontend is bundled static assets served by the `blacksmith-ui` binary.
No separate build step required to run the dashboard (though a build step
is needed during development).

**Technology:** Preact + HTM (no build step needed for v1; can add Vite later)
or Svelte (small bundle, good DX). Decision deferred to implementation.
The key constraint: the final output is static files that can be embedded
via `include_dir` or `rust-embed`.

**Layout:**

```
┌──────────────────────────────────────────────────────────────────┐
│  BLACKSMITH DASHBOARD                              [Add Project] │
├──────────────┬───────────────────────────────────────────────────┤
│              │                                                   │
│  PROJECTS    │   PROJECT DETAIL (selected project)              │
│              │                                                   │
│  ● blacksmith│   Status: Running │ Iteration 47/100             │
│  ● webapp    │   Workers: 2/3 active │ Uptime: 4h 23m          │
│  ○ ml-pipe   │                                                   │
│  ◌ data-tools│   ┌─ Beads ──────────────────────────────────┐   │
│              │   │ ● 3 in progress  ○ 7 ready  ✓ 24 closed │   │
│  ── Summary ─│   │ beads-abc  In Progress  "Add auth..."    │   │
│  12 open     │   │ beads-def  In Progress  "Fix rate..."    │   │
│  4 in flight │   │ beads-ghi  Ready        "Update docs..." │   │
│  $16.10 today│   └──────────────────────────────────────────┘   │
│  2 need attn │                                                   │
│              │   ┌─ Active Sessions ─────────────────────────┐   │
│              │   │ Worker 0: Session 142 (beads-abc) 12m     │   │
│              │   │ Worker 1: Session 143 (beads-def) 3m      │   │
│              │   │ [Click to view transcript]                │   │
│              │   └──────────────────────────────────────────┘   │
│              │                                                   │
│              │   ┌─ Metrics ─────────────────────────────────┐   │
│              │   │ Avg session: 8.2 turns │ $0.34 │ 6m 12s  │   │
│              │   │ ETA: ~2h 15m (3 workers)                  │   │
│              │   │ [Cost trend chart] [Session duration chart]│   │
│              │   └──────────────────────────────────────────┘   │
│              │                                                   │
│              │   [STOP]                                          │
│              │                                                   │
└──────────────┴───────────────────────────────────────────────────┘
```

#### Views

**1. Project List (left sidebar)**

| Element | Source |
|---|---|
| Project name | `/api/project` |
| Status indicator (●/○/◌) | Heartbeat presence + `/api/status` |
| Worker count | `/api/status` |
| Iteration progress | `/api/status` |
| Cost today | `/api/metrics/summary` |

**2. Project Detail (main area, shown when a project is selected)**

| Section | Source | Features |
|---|---|---|
| Status bar | `/api/status` | Live iteration, workers, uptime |
| Bead list | `/api/beads` | Filter by status, click to expand |
| Active sessions | `/api/status` | Click to open transcript viewer |
| Metrics summary | `/api/metrics/summary` | Averages, ETA, target compliance |
| Cost/duration charts | `/api/metrics/timeseries` | Sparkline or small line chart |
| Improvements | `/api/improvements` | Status, priority, copy button |
| Stop button | `POST /api/stop` | Confirmation dialog first |

**3. Transcript Viewer (overlay/panel, opened from session list)**

| Feature | Implementation |
|---|---|
| Pretty-printed turns | Parse SSE events, render role/tool/content |
| Live tailing | SSE connection stays open for in-progress sessions |
| Syntax highlighting | Highlight code blocks in agent output |
| Search | Client-side text search within transcript |
| Auto-scroll | Follow new output, pause on manual scroll-up |

**4. Cross-Project Summary (sidebar footer or dedicated view)**

| Metric | Source |
|---|---|
| Total beads open / in flight / closed | Sum across all `/api/beads` |
| Aggregate cost today | Sum across all `/api/metrics/summary` |
| Attention queue | Merged needs_review + circuit breaker from all instances |
| Global worker utilization | Sum active / sum max across all instances |

**5. Improvements Panel**

| Feature | Implementation |
|---|---|
| List with status/priority/category | `/api/improvements` |
| Copy button | Copies full improvement context (title + body + context + evidence) to clipboard |
| Filter by status | open, promoted, dismissed, revisit, validated |
| Sort by priority | P0 first |

#### Verification

- **V2c-1:** Opening `http://localhost:8080` in a browser shows the project
  list with all discovered/configured instances.
- **V2c-2:** Clicking a project shows its detail view with current status,
  beads, metrics, and improvements.
- **V2c-3:** Opening a transcript for an in-progress session shows live
  output appearing in real-time.
- **V2c-4:** Opening a transcript for a completed (including compressed)
  session shows the full conversation.
- **V2c-5:** The STOP button shows a confirmation dialog, and after
  confirmation, the target instance's loop exits.
- **V2c-6:** The copy button on an improvement copies its full context to
  the system clipboard.
- **V2c-7:** The cross-project summary numbers match manual sums of
  individual project data.
- **V2c-8:** The dashboard is usable on a 1280x800 screen (no horizontal
  scroll, readable text).
- **V2c-9:** `blacksmith-ui` starts and serves the dashboard with zero
  frontend build steps (assets are embedded in the binary).

#### Done when

- A single browser tab shows live status of all connected blacksmith
  instances.
- Live transcript viewing works for both in-progress and historical sessions.
- Cross-project aggregation is visible and correct.
- The binary is self-contained (no external static files needed at runtime).

---

## Milestone 3: Visualizations

Beyond the basic status/list views, specific visualizations that provide
insight not available from raw numbers.

### 3a. Dependency DAG

A visual graph of beads and their dependency edges for a selected project.

**Rendering:** Force-directed or dagre (layered) layout. Nodes are beads,
edges are dependency arrows. Color-coded by status:

| Status | Color |
|---|---|
| Closed | Gray |
| In Progress | Blue |
| Ready (no blockers) | Green |
| Blocked | Orange |
| Needs Review | Red |

**Interaction:** Click a node to see bead details. Hover to highlight
dependency chain (ancestors + descendants). Toggle to hide closed beads.

**Implementation:** Client-side rendering with a lightweight graph library
(e.g., `d3-force`, `dagre-d3`, or `elkjs`). Data from `/api/beads` which
includes dependency edges.

#### Verification

- **V3a-1:** The DAG renders with correct edges matching `bd show` dependency
  data.
- **V3a-2:** Clicking a node shows bead title, status, assignee, and
  blockers.
- **V3a-3:** The graph handles 50+ beads without performance degradation
  (< 1s render).
- **V3a-4:** Hiding closed beads filters the graph and re-layouts.

#### Done when

- The dependency DAG is visually correct, interactive, and performs well
  with realistic bead counts.

---

### 3b. Worker Timeline

A Gantt-style horizontal bar chart showing worker activity over time.

```
Worker 0  ████ beads-abc ████████ beads-ghi ██
Worker 1  ██████████ beads-def ██████████████████
Worker 2  ░░░░░░░░░░░░░░░ idle ░░░░░░░░░░░░░░░
          ├──────────────────────────────────────┤
          10:00        11:00        12:00    now
```

Color-coded by bead. Gaps show idle time. Hover for session details (turns,
cost, outcome).

**Data source:** `/api/sessions` with worker ID, bead ID, start/end times.
The sessions endpoint needs to include `worker_id` and `bead_id` fields.

#### Verification

- **V3b-1:** Timeline matches actual session start/end times as recorded
  in the database.
- **V3b-2:** Idle gaps are visible between sessions on the same worker.
- **V3b-3:** Hovering a bar shows session ID, bead, turns, cost, duration.
- **V3b-4:** The timeline auto-scrolls to follow "now" with a visible
  right edge.

#### Done when

- Worker utilization is visually obvious at a glance.
- Time spent per bead and idle gaps are clearly visible.

---

### 3c. Cost and Performance Trends

Line charts showing key metrics over time (per session or per day).

**Charts:**

| Chart | X-axis | Y-axis | Purpose |
|---|---|---|---|
| Cost per session | Session number | USD | Spot cost anomalies |
| Tokens per session | Session number | Input + output tokens | Spot prompt bloat |
| Session duration | Session number | Minutes | Spot stale sessions |
| Turns per session | Session number | Turn count | Spot efficiency changes |
| Beads closed per day | Date | Count | Velocity tracking |
| Outcome distribution | Session number | Stacked (productive/empty/rate-limited) | Health overview |

**Implementation:** Lightweight charting library (e.g., `uPlot` — tiny,
fast, no dependencies; or `Chart.js`). Data from `/api/metrics/timeseries`.

#### Verification

- **V3c-1:** Cost chart values match `blacksmith metrics query cost.estimate_usd`.
- **V3c-2:** Charts render within 500ms for 500 data points.
- **V3c-3:** Hovering a data point shows the exact value and session ID.
- **V3c-4:** Outcome distribution stacked chart correctly sums to total
  sessions.

#### Done when

- All six charts render with correct data from the metrics API.
- Charts are responsive and readable at dashboard widths.

---

### 3d. Rate Limit Heatmap

A time-of-day heatmap showing when rate limits are hit, to help operators
plan scheduling windows.

```
         00  03  06  09  12  15  18  21
   Mon   ░░  ░░  ░░  ██  ██  ██  ░░  ░░
   Tue   ░░  ░░  ░░  ██  ██  ░░  ░░  ░░
   Wed   ░░  ░░  ░░  ██  ██  ██  ██  ░░
   ...
```

**Data source:** Sessions with `outcome = rate_limited` from
`/api/metrics/timeseries`, bucketed by hour-of-day and day-of-week.

#### Verification

- **V3d-1:** Heatmap cells reflect actual rate-limit events from the
  database.
- **V3d-2:** Hovering a cell shows the count and specific session IDs.

#### Done when

- The heatmap renders correctly and reveals time-of-day patterns.

---

## Milestone 4: Cross-Project Features

Features that span multiple blacksmith instances.

### 4a. Attention Queue

A unified view of items needing human attention across all projects.

**Items included:**

| Item | Source | Severity |
|---|---|---|
| Circuit breaker tripped | `/api/status` — bead in `needs_review` | High |
| Consecutive rate limits | `/api/status` — approaching max | Medium |
| Stale session (watchdog about to kill) | `/api/status` — session age | Medium |
| Target streak alert | `/api/metrics/summary` — target misses | Low |
| Offline instance | Heartbeat timeout | Info |

Sorted by severity, then recency. Each item links to the relevant project
detail view.

#### Verification

- **V4a-1:** A circuit breaker trip on project A appears in the attention
  queue within one poll interval.
- **V4a-2:** Items link to the correct project and relevant section (e.g.,
  clicking a needs_review bead opens that project's bead detail).
- **V4a-3:** Resolved items disappear on next poll.

#### Done when

- The attention queue shows all cross-project issues in one list.
- Each item is actionable (links to the right place).

---

### 4b. Global Metrics

Aggregated metrics across all projects, shown in the cross-project summary.

| Metric | Aggregation |
|---|---|
| Total cost today / this week | Sum of per-project costs |
| Beads velocity (closed/day) | Sum across projects, trended |
| Worker utilization | Active / max, shown as percentage |
| Aggregate session outcomes | Stacked chart across all projects |

#### Verification

- **V4b-1:** Global cost matches sum of per-project costs.
- **V4b-2:** Velocity trend shows data from all projects combined.
- **V4b-3:** Worker utilization percentage is correct against manual count.

#### Done when

- Cross-project metrics are visible and numerically correct.

---

## Milestone 5: Time Estimation

Surface the existing `blacksmith estimate` functionality through the API
and display it prominently in the dashboard.

### API Extension

```
GET /api/estimate
GET /api/estimate?workers=4
```

Returns:

```json
{
  "remaining_beads": 12,
  "serial_eta_minutes": 480,
  "parallel_eta_minutes": 165,
  "workers": 3,
  "avg_session_minutes": 8.2,
  "avg_sessions_per_bead": 1.4,
  "critical_path_beads": ["beads-abc", "beads-def", "beads-xyz"],
  "bottleneck": "beads-abc blocks 4 downstream beads"
}
```

**Display:** Show the ETA prominently in the project detail view. Include
a "what-if" slider: "How fast with N workers?" that re-queries the estimate
endpoint with different worker counts.

#### Verification

- **V5-1:** API estimate matches `blacksmith estimate` CLI output.
- **V5-2:** Changing the worker count slider updates the ETA.
- **V5-3:** Critical path beads are highlighted in the DAG view (M3a).

#### Done when

- Time estimates are visible per-project and respond to worker count changes.
- Critical path is identified and visually linked to the DAG.

---

## Non-Goals (V6)

These are explicitly out of scope for this version:

| Non-Goal | Rationale |
|---|---|
| Authentication / authorization | Operator tool on trusted networks; can add later |
| Write operations beyond STOP | Dashboard is read-only + stop; no bead creation or editing |
| Mobile-responsive layout | Desktop dashboard for operators |
| Persistent history in dashboard | Dashboard is stateless; all data comes from instances |
| Alerting (email, Slack, PagerDuty) | Deferred to V7; attention queue is the v6 answer |
| Multi-user / role-based access | Single operator tool |

---

## Dependency Impact

### `blacksmith` (main crate)

**New optional dependencies (behind `features = ["serve"]`):**

| Crate | Purpose | Weight |
|---|---|---|
| `axum` | HTTP server | ~15 transitive crates (shares `tokio`, `hyper`) |
| `tower-http` | CORS middleware | Small, part of tower ecosystem |
| `socket2` | UDP multicast | Minimal, no transitive deps |

**Impact on default build:** Zero. Feature-gated. `cargo build` unchanged.
**Impact on `--features serve` build:** ~8s additional clean compile time.
Incremental: negligible (axum compiles once).

### `blacksmith-ui` (new crate)

| Crate | Purpose |
|---|---|
| `axum` | HTTP server for dashboard |
| `tower-http` | Static file serving, CORS |
| `reqwest` | HTTP client for polling instances |
| `socket2` | UDP multicast listener |
| `rust-embed` or `include_dir` | Embed static frontend assets |
| `serde` / `serde_json` | JSON handling |
| `tokio` | Async runtime |
| `toml` | Config parsing |

**Frontend dependencies (bundled, not Rust):**

| Library | Purpose | Size |
|---|---|---|
| Preact + HTM (or Svelte) | UI rendering | ~4KB gzipped |
| uPlot | Charts | ~8KB gzipped |
| dagre-d3 or elkjs | DAG layout | ~15KB gzipped |

**Total frontend bundle:** <50KB gzipped. Embedded in the binary.

---

## Implementation Order

```
M1a  blacksmith serve (API)         ← foundation, unblocks everything
 │
 ├── M1b  UDP heartbeat             ← can parallel with M1a (independent)
 ├── M1c  port allocation           ← can parallel with M1a (independent)
 │
 ▼
M2a  blacksmith-ui discovery        ← needs M1a + M1b
 │
 ├── M2b  polling + aggregation     ← needs M2a
 │
 ▼
M2c  dashboard frontend             ← needs M2b
 │
 ├── M3a  dependency DAG            ← can parallel with M3b/M3c/M3d
 ├── M3b  worker timeline           ← can parallel
 ├── M3c  cost/performance charts   ← can parallel
 ├── M3d  rate limit heatmap        ← can parallel
 │
 ▼
M4a  attention queue                ← needs M2b (cross-project polling)
M4b  global metrics                 ← needs M2b
 │
 ▼
M5   time estimation                ← needs M2c (frontend) + M3a (DAG)
```

**Critical path:** M1a → M2a → M2b → M2c → M5

**Parallelizable:** M1b, M1c, M3a-M3d, M4a, M4b

---

## Open Questions

1. **Frontend framework decision.** Preact+HTM (zero build step for v1,
   add Vite later) vs Svelte (better DX, requires build step from day 1).
   Leaning Preact for faster bootstrapping.

2. **Session transcript pagination.** Large sessions can have thousands of
   turns. Should the SSE endpoint support `?from_turn=N` for pagination,
   or is client-side virtual scrolling sufficient?

3. **Dashboard state persistence.** Should the dashboard remember which
   project was selected, which view was open, etc. across browser refreshes?
   LocalStorage is the obvious answer but adds scope.

4. **Multi-dashboard.** If two operators open the dashboard simultaneously,
   everything works (it's read-only polling). But should we show "someone
   hit STOP" as a notification to the other viewer?

5. **Workspace structure.** Should `blacksmith-ui` live in this repo as a
   workspace member, or in a separate repository? Same repo is simpler for
   shared types; separate repo avoids bloating the main repo's CI.
