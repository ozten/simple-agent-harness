import {
  h,
  render,
  Component,
} from "https://esm.sh/preact@10.19.3";
import htm from "https://esm.sh/htm@3.1.1";
import {
  useState,
  useEffect,
  useCallback,
  useRef,
} from "https://esm.sh/preact@10.19.3/hooks";

const html = htm.bind(h);

function StatusDot({ online }) {
  return html`<span class="status-dot ${online ? "online" : "offline"}" />`;
}

function ProjectItem({ instance, active, onClick }) {
  const name = instance.name || instance.url;
  const workerCount = instance.worker_count || 0;
  return html`
    <div
      class="project-item ${active ? "active" : ""}"
      onClick=${onClick}
    >
      <${StatusDot} online=${instance.online} />
      <div class="project-info">
        <div class="project-name">${name}</div>
        <div class="project-meta">
          ${instance.online ? `${workerCount} worker${workerCount !== 1 ? "s" : ""}` : "offline"}
        </div>
      </div>
    </div>
  `;
}

function AddProjectForm({ onAdd, onCancel }) {
  const [url, setUrl] = useState("");
  const [name, setName] = useState("");
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(false);

  const submit = async (e) => {
    e.preventDefault();
    if (!url.trim()) return;
    setLoading(true);
    setError(null);
    try {
      const resp = await fetch("/api/instances", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ url: url.trim(), name: name.trim() || null }),
      });
      const data = await resp.json();
      if (!resp.ok) {
        setError(data.error || "Failed to add");
      } else {
        onAdd();
      }
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  return html`
    <form class="add-form" onSubmit=${submit}>
      <input
        type="text"
        placeholder="http://localhost:3000"
        value=${url}
        onInput=${(e) => setUrl(e.target.value)}
        autofocus
      />
      <input
        type="text"
        placeholder="Project name (optional)"
        value=${name}
        onInput=${(e) => setName(e.target.value)}
      />
      <div class="add-form-actions">
        <button type="submit" class="submit-btn" disabled=${loading}>
          ${loading ? "Adding..." : "Add"}
        </button>
        <button type="button" class="cancel-btn" onClick=${onCancel}>
          Cancel
        </button>
      </div>
      ${error && html`<div class="error-msg">${error}</div>`}
    </form>
  `;
}

function Sidebar({ instances, selected, onSelect, onRefresh }) {
  const [adding, setAdding] = useState(false);

  return html`
    <div class="sidebar">
      <div class="sidebar-header">
        <h1>BLACKSMITH</h1>
        <div class="subtitle">Multi-Project Dashboard</div>
      </div>
      <div class="project-list">
        ${instances.map(
          (inst) => html`
            <${ProjectItem}
              key=${inst.url}
              instance=${inst}
              active=${selected === inst.url}
              onClick=${() => onSelect(inst.url)}
            />
          `
        )}
        ${instances.length === 0 &&
        html`<div class="project-meta" style="padding: 12px; text-align: center;">
          No projects discovered
        </div>`}
      </div>
      <div class="sidebar-footer">
        ${adding
          ? html`<${AddProjectForm}
              onAdd=${() => {
                setAdding(false);
                onRefresh();
              }}
              onCancel=${() => setAdding(false)}
            />`
          : html`<button class="add-btn" onClick=${() => setAdding(true)}>
              + Add Project
            </button>`}
      </div>
    </div>
  `;
}

function AggregateCards({ aggregate }) {
  if (!aggregate) return null;
  return html`
    <div class="aggregate-cards">
      <div class="card">
        <div class="label">Open Beads</div>
        <div class="value">${aggregate.total_beads_open ?? "-"}</div>
      </div>
      <div class="card">
        <div class="label">In Progress</div>
        <div class="value">${aggregate.total_beads_in_progress ?? "-"}</div>
      </div>
      <div class="card">
        <div class="label">Workers</div>
        <div class="value">
          ${aggregate.worker_utilization ?? 0}/${aggregate.instances_total ?? 0}
        </div>
      </div>
      <div class="card">
        <div class="label">Instances Online</div>
        <div class="value">
          ${aggregate.instances_online ?? 0}/${aggregate.instances_total ?? 0}
        </div>
      </div>
      <div class="card">
        <div class="label">Cost Today</div>
        <div class="value">$${(aggregate.total_cost_today ?? 0).toFixed(2)}</div>
      </div>
    </div>
  `;
}

function GlobalMetricsPanel({ metrics }) {
  if (!metrics) return null;

  const utilPct = (metrics.worker_utilization * 100).toFixed(0);
  const outcomeTotal = metrics.outcomes?.total || 0;
  const successPct =
    outcomeTotal > 0
      ? ((metrics.outcomes.success / outcomeTotal) * 100).toFixed(0)
      : 0;
  const failedPct =
    outcomeTotal > 0
      ? ((metrics.outcomes.failed / outcomeTotal) * 100).toFixed(0)
      : 0;
  const timedOutPct =
    outcomeTotal > 0
      ? ((metrics.outcomes.timed_out / outcomeTotal) * 100).toFixed(0)
      : 0;

  return html`
    <div class="global-metrics">
      <h3>Global Metrics</h3>
      <div class="global-metrics-grid">
        <div class="card">
          <div class="label">Cost Today</div>
          <div class="value">$${metrics.total_cost_today.toFixed(2)}</div>
        </div>
        <div class="card">
          <div class="label">Cost This Week</div>
          <div class="value">$${metrics.total_cost_this_week.toFixed(2)}</div>
        </div>
        <div class="card">
          <div class="label">Beads Velocity</div>
          <div class="value">${metrics.beads_velocity.toFixed(1)}<span class="unit">/day</span></div>
        </div>
        <div class="card">
          <div class="label">Worker Utilization</div>
          <div class="value">${utilPct}<span class="unit">%</span></div>
          <div class="progress-bar">
            <div class="progress-fill" style="width: ${utilPct}%" />
          </div>
          <div class="sub-label">${metrics.workers_active}/${metrics.workers_max} active</div>
        </div>
      </div>
      ${outcomeTotal > 0 && html`
        <div class="outcomes-section">
          <div class="label">Session Outcomes</div>
          <div class="stacked-bar">
            ${metrics.outcomes.success > 0 && html`
              <div class="bar-segment success" style="width: ${successPct}%" title="Success: ${metrics.outcomes.success}" />
            `}
            ${metrics.outcomes.failed > 0 && html`
              <div class="bar-segment failed" style="width: ${failedPct}%" title="Failed: ${metrics.outcomes.failed}" />
            `}
            ${metrics.outcomes.timed_out > 0 && html`
              <div class="bar-segment timed-out" style="width: ${timedOutPct}%" title="Timed out: ${metrics.outcomes.timed_out}" />
            `}
          </div>
          <div class="outcomes-legend">
            <span class="legend-item"><span class="legend-dot success"></span> Success ${metrics.outcomes.success}</span>
            <span class="legend-item"><span class="legend-dot failed"></span> Failed ${metrics.outcomes.failed}</span>
            <span class="legend-item"><span class="legend-dot timed-out"></span> Timed out ${metrics.outcomes.timed_out}</span>
          </div>
        </div>
      `}
    </div>
  `;
}

function StatusBar({ instance, statusData }) {
  const iteration = instance?.iteration ?? statusData?.iteration ?? 0;
  const maxIterations = instance?.max_iterations ?? statusData?.max_iterations ?? 0;
  const workersActive = instance?.workers_active ?? statusData?.workers_active ?? 0;
  const workersMax = instance?.workers_max ?? statusData?.workers_max ?? 0;
  const uptime = statusData?.uptime_secs;

  const formatUptime = (secs) => {
    if (secs == null) return "-";
    const h = Math.floor(secs / 3600);
    const m = Math.floor((secs % 3600) / 60);
    if (h > 0) return `${h}h ${m}m`;
    return `${m}m`;
  };

  return html`
    <div class="status-bar">
      <div class="status-bar-item">
        <${StatusDot} online=${instance?.online !== false} />
        <span>${instance?.online !== false ? "Online" : "Offline"}</span>
      </div>
      <div class="status-bar-item">
        <span class="status-label">Iteration</span>
        <span class="status-value">${iteration}${maxIterations ? ` / ${maxIterations}` : ""}</span>
      </div>
      <div class="status-bar-item">
        <span class="status-label">Workers</span>
        <span class="status-value">${workersActive} / ${workersMax}</span>
      </div>
      <div class="status-bar-item">
        <span class="status-label">Uptime</span>
        <span class="status-value">${formatUptime(uptime)}</span>
      </div>
    </div>
  `;
}

function BeadList({ beadsData }) {
  const [filter, setFilter] = useState("all");
  const [expanded, setExpanded] = useState(null);

  const beads = beadsData?.items || beadsData?.beads || [];
  const counts = {
    all: beads.length,
    open: beads.filter((b) => b.status === "open").length,
    in_progress: beads.filter((b) => b.status === "in_progress").length,
    closed: beads.filter((b) => b.status === "closed").length,
  };

  const filtered = filter === "all" ? beads : beads.filter((b) => b.status === filter);

  const statusClass = (status) => {
    if (status === "open") return "bead-open";
    if (status === "in_progress") return "bead-in-progress";
    if (status === "closed") return "bead-closed";
    return "";
  };

  return html`
    <div class="detail-section">
      <div class="section-header">
        <h3>Beads</h3>
        <div class="filter-tabs">
          ${["all", "open", "in_progress", "closed"].map(
            (f) => html`
              <button
                key=${f}
                class="filter-tab ${filter === f ? "active" : ""}"
                onClick=${() => setFilter(f)}
              >
                ${f === "in_progress" ? "In Progress" : f.charAt(0).toUpperCase() + f.slice(1)}
                <span class="filter-count">${counts[f]}</span>
              </button>
            `
          )}
        </div>
      </div>
      <div class="bead-list">
        ${filtered.length === 0
          ? html`<div class="bead-empty">No beads matching filter</div>`
          : filtered.map(
              (b) => html`
                <div
                  key=${b.id}
                  class="bead-item ${expanded === b.id ? "expanded" : ""}"
                  onClick=${() => setExpanded(expanded === b.id ? null : b.id)}
                >
                  <div class="bead-row">
                    <span class="bead-status ${statusClass(b.status)}">${b.status}</span>
                    <span class="bead-id">${b.id}</span>
                    <span class="bead-title">${b.title}</span>
                  </div>
                  ${expanded === b.id &&
                  html`
                    <div class="bead-details">
                      ${b.type && html`<div><strong>Type:</strong> ${b.type}</div>`}
                      ${b.priority != null && html`<div><strong>Priority:</strong> P${b.priority}</div>`}
                      ${b.assignee && html`<div><strong>Assignee:</strong> ${b.assignee}</div>`}
                      ${b.description && html`<div class="bead-description">${b.description}</div>`}
                    </div>
                  `}
                </div>
              `
            )}
      </div>
    </div>
  `;
}

function ActiveSessions({ statusData, onViewTranscript }) {
  const workers = statusData?.workers || [];

  return html`
    <div class="detail-section">
      <div class="section-header">
        <h3>Active Sessions</h3>
      </div>
      ${workers.length === 0
        ? html`<div class="bead-empty">No active sessions</div>`
        : html`
            <div class="sessions-list">
              ${workers.map(
                (w) => html`
                  <div key=${w.id || w.worker_id} class="session-item">
                    <div class="session-worker">
                      <span class="session-worker-id">${w.id || w.worker_id || "Worker"}</span>
                      ${w.status && html`<span class="session-status">${w.status}</span>`}
                    </div>
                    <div class="session-meta">
                      ${w.bead_id && html`<span class="session-bead">Bead: ${w.bead_id}</span>`}
                      ${w.duration_secs != null &&
                      html`<span class="session-duration">${Math.floor(w.duration_secs / 60)}m ${w.duration_secs % 60}s</span>`}
                      <button
                        class="session-transcript-btn"
                        onClick=${() => onViewTranscript(w.session_id || w.id || w.worker_id, w.status)}
                      >View Transcript</button>
                    </div>
                  </div>
                `
              )}
            </div>
          `}
    </div>
  `;
}

function MetricsSummary({ metricsData }) {
  if (!metricsData) return null;

  const fmt = (v, decimals = 2) => (v != null ? Number(v).toFixed(decimals) : "-");
  const fmtDur = (secs) => {
    if (secs == null) return "-";
    const m = Math.floor(secs / 60);
    const s = Math.round(secs % 60);
    return `${m}m ${s}s`;
  };

  return html`
    <div class="detail-section">
      <div class="section-header">
        <h3>Metrics</h3>
      </div>
      <div class="metrics-grid">
        <div class="card">
          <div class="label">Avg Cost</div>
          <div class="value">$${fmt(metricsData.avg_cost)}</div>
        </div>
        <div class="card">
          <div class="label">Avg Tokens</div>
          <div class="value">${metricsData.avg_tokens != null ? Math.round(metricsData.avg_tokens).toLocaleString() : "-"}</div>
        </div>
        <div class="card">
          <div class="label">Avg Duration</div>
          <div class="value">${fmtDur(metricsData.avg_duration_secs)}</div>
        </div>
        <div class="card">
          <div class="label">Avg Turns</div>
          <div class="value">${fmt(metricsData.avg_turns, 1)}</div>
        </div>
        <div class="card">
          <div class="label">Cost Today</div>
          <div class="value">$${fmt(metricsData.cost_today)}</div>
        </div>
        <div class="card">
          <div class="label">Beads Closed Today</div>
          <div class="value">${metricsData.beads_closed_today ?? "-"}</div>
        </div>
      </div>
    </div>
  `;
}

function StopButton({ instanceUrl }) {
  const [confirming, setConfirming] = useState(false);
  const [stopping, setStopping] = useState(false);
  const [result, setResult] = useState(null);

  const doStop = async () => {
    setStopping(true);
    setResult(null);
    try {
      const resp = await fetch(`/api/instances/${encodeURIComponent(instanceUrl)}/stop`, {
        method: "POST",
      });
      if (resp.ok) {
        setResult("success");
      } else {
        const data = await resp.json().catch(() => ({}));
        setResult(data.error || "Failed to stop");
      }
    } catch (err) {
      setResult(err.message);
    } finally {
      setStopping(false);
      setConfirming(false);
    }
  };

  return html`
    <div class="detail-section stop-section">
      ${confirming
        ? html`
            <div class="stop-confirm">
              <p>Are you sure you want to stop this project? This will create a STOP file on the instance.</p>
              <div class="stop-actions">
                <button class="stop-confirm-btn" onClick=${doStop} disabled=${stopping}>
                  ${stopping ? "Stopping..." : "Confirm Stop"}
                </button>
                <button class="stop-cancel-btn" onClick=${() => setConfirming(false)}>Cancel</button>
              </div>
            </div>
          `
        : html`<button class="stop-btn" onClick=${() => setConfirming(true)}>Stop Project</button>`}
      ${result === "success" && html`<div class="stop-result success">Stop signal sent</div>`}
      ${result && result !== "success" && html`<div class="stop-result error">${result}</div>`}
    </div>
  `;
}

function TranscriptViewer({ instanceUrl, sessionId, sessionStatus, onClose }) {
  const [turns, setTurns] = useState([]);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(true);
  const [searchQuery, setSearchQuery] = useState("");
  const [autoScroll, setAutoScroll] = useState(true);
  const containerRef = useRef(null);
  const userScrolledRef = useRef(false);

  // Auto-scroll logic
  useEffect(() => {
    if (autoScroll && containerRef.current) {
      containerRef.current.scrollTop = containerRef.current.scrollHeight;
    }
  }, [turns, autoScroll]);

  const handleScroll = useCallback(() => {
    if (!containerRef.current) return;
    const el = containerRef.current;
    const atBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 50;
    if (!atBottom) {
      userScrolledRef.current = true;
      setAutoScroll(false);
    } else if (userScrolledRef.current) {
      userScrolledRef.current = false;
      setAutoScroll(true);
    }
  }, []);

  // For live sessions, connect via SSE
  useEffect(() => {
    const isLive = sessionStatus === "running" || sessionStatus === "in_progress";

    if (isLive) {
      const url = `/api/instances/${encodeURIComponent(instanceUrl)}/sessions/${encodeURIComponent(sessionId)}/stream`;
      const evtSource = new EventSource(url);

      evtSource.onmessage = (event) => {
        try {
          const turn = JSON.parse(event.data);
          setTurns((prev) => [...prev, turn]);
          setLoading(false);
        } catch (_) {}
      };

      evtSource.addEventListener("turn", (event) => {
        try {
          const turn = JSON.parse(event.data);
          setTurns((prev) => [...prev, turn]);
          setLoading(false);
        } catch (_) {}
      });

      evtSource.onerror = () => {
        setLoading(false);
        // SSE might close when session ends — not necessarily an error
      };

      return () => evtSource.close();
    } else {
      // Completed session: fetch full transcript
      const fetchTranscript = async () => {
        try {
          const resp = await fetch(
            `/api/instances/${encodeURIComponent(instanceUrl)}/sessions/${encodeURIComponent(sessionId)}/transcript`
          );
          if (resp.ok) {
            const data = await resp.json();
            setTurns(data.turns || data.messages || (Array.isArray(data) ? data : []));
          } else {
            setError("Failed to load transcript");
          }
        } catch (err) {
          setError(err.message);
        } finally {
          setLoading(false);
        }
      };
      fetchTranscript();
    }
  }, [instanceUrl, sessionId, sessionStatus]);

  const roleClass = (role) => {
    if (role === "assistant") return "turn-assistant";
    if (role === "tool" || role === "tool_result") return "turn-tool";
    if (role === "user") return "turn-user";
    return "turn-system";
  };

  const roleLabel = (role) => {
    if (role === "assistant") return "Assistant";
    if (role === "tool" || role === "tool_result") return "Tool";
    if (role === "user") return "User";
    return role || "System";
  };

  const highlightSearch = (text, query) => {
    if (!query || !text) return text;
    const parts = text.split(new RegExp(`(${query.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")})`, "gi"));
    return parts.map((part, i) =>
      part.toLowerCase() === query.toLowerCase()
        ? html`<mark class="search-highlight" key=${i}>${part}</mark>`
        : part
    );
  };

  const renderContent = (turn) => {
    const content = turn.content || turn.text || turn.message || "";
    if (typeof content === "string") {
      // Detect code blocks
      const parts = content.split(/(```[\s\S]*?```)/g);
      return parts.map((part, i) => {
        if (part.startsWith("```") && part.endsWith("```")) {
          const lines = part.slice(3, -3);
          const firstNewline = lines.indexOf("\n");
          const code = firstNewline >= 0 ? lines.slice(firstNewline + 1) : lines;
          return html`<pre class="code-block" key=${i}><code>${highlightSearch(code, searchQuery)}</code></pre>`;
        }
        return html`<span key=${i}>${highlightSearch(part, searchQuery)}</span>`;
      });
    }
    // If content is an array (multi-part messages)
    if (Array.isArray(content)) {
      return content.map((block, i) => {
        if (typeof block === "string") {
          return html`<span key=${i}>${highlightSearch(block, searchQuery)}</span>`;
        }
        if (block.type === "text") {
          return html`<span key=${i}>${highlightSearch(block.text, searchQuery)}</span>`;
        }
        if (block.type === "tool_use") {
          return html`<pre class="code-block" key=${i}><code>${highlightSearch(JSON.stringify(block.input, null, 2), searchQuery)}</code></pre>`;
        }
        if (block.type === "tool_result") {
          const text = typeof block.content === "string" ? block.content : JSON.stringify(block.content, null, 2);
          return html`<pre class="code-block" key=${i}><code>${highlightSearch(text, searchQuery)}</code></pre>`;
        }
        return html`<pre class="code-block" key=${i}><code>${highlightSearch(JSON.stringify(block, null, 2), searchQuery)}</code></pre>`;
      });
    }
    return html`<pre class="code-block"><code>${highlightSearch(JSON.stringify(content, null, 2), searchQuery)}</code></pre>`;
  };

  const matchesSearch = (turn) => {
    if (!searchQuery) return true;
    const q = searchQuery.toLowerCase();
    const content = turn.content || turn.text || turn.message || "";
    const text = typeof content === "string" ? content : JSON.stringify(content);
    return text.toLowerCase().includes(q);
  };

  const filteredTurns = searchQuery ? turns.filter(matchesSearch) : turns;

  return html`
    <div class="transcript-overlay" onClick=${(e) => { if (e.target.classList.contains("transcript-overlay")) onClose(); }}>
      <div class="transcript-panel">
        <div class="transcript-header">
          <h3>Transcript — ${sessionId}</h3>
          <div class="transcript-controls">
            <input
              type="text"
              class="transcript-search"
              placeholder="Search transcript..."
              value=${searchQuery}
              onInput=${(e) => setSearchQuery(e.target.value)}
            />
            <button
              class="transcript-autoscroll ${autoScroll ? "active" : ""}"
              onClick=${() => { setAutoScroll(!autoScroll); userScrolledRef.current = !autoScroll ? false : true; }}
              title=${autoScroll ? "Auto-scroll ON" : "Auto-scroll OFF"}
            >
              ${autoScroll ? "Following" : "Paused"}
            </button>
            <button class="transcript-close" onClick=${onClose}>Close</button>
          </div>
        </div>
        <div class="transcript-body" ref=${containerRef} onScroll=${handleScroll}>
          ${loading && html`<div class="transcript-loading">Loading transcript...</div>`}
          ${error && html`<div class="transcript-error">${error}</div>`}
          ${!loading && !error && filteredTurns.length === 0 && html`
            <div class="transcript-empty">${searchQuery ? "No matching turns" : "No turns yet"}</div>
          `}
          ${filteredTurns.map((turn, i) => html`
            <div key=${i} class="transcript-turn ${roleClass(turn.role)}">
              <div class="turn-header">
                <span class="turn-role">${roleLabel(turn.role)}</span>
                ${turn.timestamp && html`<span class="turn-time">${new Date(turn.timestamp).toLocaleTimeString()}</span>`}
              </div>
              <div class="turn-content">${renderContent(turn)}</div>
            </div>
          `)}
        </div>
      </div>
    </div>
  `;
}

function ProjectDetail({ instance, instanceUrl }) {
  const [pollData, setPollData] = useState(null);
  const [viewingTranscript, setViewingTranscript] = useState(null);

  const fetchPollData = useCallback(async () => {
    try {
      const resp = await fetch(`/api/instances/${encodeURIComponent(instanceUrl)}/poll-data`);
      if (resp.ok) {
        const data = await resp.json();
        setPollData(data);
      }
    } catch (_) {}
  }, [instanceUrl]);

  useEffect(() => {
    fetchPollData();
    const interval = setInterval(fetchPollData, 10000);
    return () => clearInterval(interval);
  }, [fetchPollData]);

  const name = instance?.name || instanceUrl;

  const handleViewTranscript = useCallback((sessionId, status) => {
    setViewingTranscript({ sessionId, status });
  }, []);

  return html`
    <div class="project-detail">
      <h2>${name}</h2>
      <${StatusBar} instance=${instance} statusData=${pollData?.status_data} />
      <${BeadList} beadsData=${pollData?.beads_data} />
      <${ActiveSessions} statusData=${pollData?.status_data} onViewTranscript=${handleViewTranscript} />
      <${MetricsSummary} metricsData=${pollData?.metrics_data} />
      <${StopButton} instanceUrl=${instanceUrl} />
      ${viewingTranscript && html`
        <${TranscriptViewer}
          instanceUrl=${instanceUrl}
          sessionId=${viewingTranscript.sessionId}
          sessionStatus=${viewingTranscript.status}
          onClose=${() => setViewingTranscript(null)}
        />
      `}
    </div>
  `;
}

function App() {
  const [instances, setInstances] = useState([]);
  const [aggregate, setAggregate] = useState(null);
  const [globalMetrics, setGlobalMetrics] = useState(null);
  const [selected, setSelected] = useState(null);

  const fetchData = useCallback(async () => {
    try {
      const [instResp, aggResp, gmResp] = await Promise.all([
        fetch("/api/instances"),
        fetch("/api/aggregate"),
        fetch("/api/global-metrics"),
      ]);
      if (instResp.ok) {
        const data = await instResp.json();
        setInstances(data);
      }
      if (aggResp.ok) {
        const data = await aggResp.json();
        setAggregate(data);
      }
      if (gmResp.ok) {
        const data = await gmResp.json();
        setGlobalMetrics(data);
      }
    } catch (_) {
      // Silently handle fetch errors
    }
  }, []);

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 10000);
    return () => clearInterval(interval);
  }, [fetchData]);

  const selectedInstance = instances.find((i) => i.url === selected);

  return html`
    <${Sidebar}
      instances=${instances}
      selected=${selected}
      onSelect=${setSelected}
      onRefresh=${fetchData}
    />
    <div class="main-content">
      ${selected
        ? html`<${ProjectDetail} instance=${selectedInstance} instanceUrl=${selected} />`
        : html`
            <h2>Overview</h2>
            <${AggregateCards} aggregate=${aggregate} />
            <${GlobalMetricsPanel} metrics=${globalMetrics} />
            <div class="empty-state">
              Select a project from the sidebar to view details
            </div>
          `}
    </div>
  `;
}

render(html`<${App} />`, document.getElementById("app"));
