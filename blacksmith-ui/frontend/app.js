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

  return html`
    <${Sidebar}
      instances=${instances}
      selected=${selected}
      onSelect=${setSelected}
      onRefresh=${fetchData}
    />
    <div class="main-content">
      <h2>Overview</h2>
      <${AggregateCards} aggregate=${aggregate} />
      <${GlobalMetricsPanel} metrics=${globalMetrics} />
      ${selected
        ? html`<div class="card" style="margin-top: 16px;">
            <div class="label">Selected Project</div>
            <div class="value" style="font-size: 16px; margin-top: 8px;">
              ${instances.find((i) => i.url === selected)?.name || selected}
            </div>
          </div>`
        : html`<div class="empty-state">
            Select a project from the sidebar to view details
          </div>`}
    </div>
  `;
}

render(html`<${App} />`, document.getElementById("app"));
