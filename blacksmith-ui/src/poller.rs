use crate::discovery::{Instance, Registry};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

/// Cross-project aggregate metrics.
#[derive(Debug, Clone, Serialize, Default)]
pub struct Aggregate {
    pub total_beads_open: u64,
    pub total_beads_in_progress: u64,
    pub total_cost_today: f64,
    pub worker_utilization: f64,
    pub instances_online: u64,
    pub instances_total: u64,
}

/// Polled data per instance, keyed by normalized URL.
#[derive(Debug, Clone, Serialize, Default)]
pub struct InstancePollData {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_info: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status_data: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub beads_data: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metrics_data: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub improvements_data: Option<serde_json::Value>,
}

pub type PollStore = Arc<RwLock<PollDataStore>>;

pub struct PollDataStore {
    /// Per-instance polled data, keyed by normalized URL
    pub data: HashMap<String, InstancePollData>,
    /// Cached aggregate
    pub aggregate: Aggregate,
}

impl PollDataStore {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
            aggregate: Aggregate::default(),
        }
    }
}

/// Response shapes for polled endpoints (loosely typed to handle missing fields).
#[derive(Debug, Deserialize, Default)]
struct BeadsSummary {
    #[serde(default)]
    open: u64,
    #[serde(default)]
    in_progress: u64,
}

#[derive(Debug, Deserialize, Default)]
struct MetricsSummary {
    #[serde(default)]
    cost_today: f64,
    #[serde(default)]
    workers_active: u64,
    #[serde(default)]
    workers_max: u64,
}

/// Spawn the main polling loop.
pub fn spawn_poller(registry: Registry, poll_store: PollStore, poll_interval_secs: u64) {
    let poll_interval = Duration::from_secs(poll_interval_secs);
    let improvements_interval = Duration::from_secs(30);

    tokio::spawn(async move {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();

        let mut tick_count: u64 = 0;
        loop {
            let instances = {
                let reg = registry.read().await;
                reg.list()
            };

            let poll_improvements = tick_count == 0
                || (tick_count * poll_interval_secs)
                    .is_multiple_of(improvements_interval.as_secs());

            // Poll all instances concurrently
            let mut handles = Vec::new();
            for inst in &instances {
                let client = client.clone();
                let url = inst.url.clone();
                let poll_impr = poll_improvements;
                let needs_project = {
                    let store = poll_store.read().await;
                    store
                        .data
                        .get(&normalize_url(&url))
                        .and_then(|d| d.project_info.as_ref())
                        .is_none()
                };

                handles.push(tokio::spawn(async move {
                    poll_instance(&client, &url, needs_project, poll_impr).await
                }));
            }

            // Collect results
            let mut results: Vec<(String, bool, InstancePollData)> = Vec::new();
            for (i, handle) in handles.into_iter().enumerate() {
                if let Ok(result) = handle.await {
                    let url = instances[i].url.clone();
                    results.push((url, result.0, result.1));
                }
            }

            // Update registry online status and poll store
            {
                let mut reg = registry.write().await;
                let mut store = poll_store.write().await;

                for (url, is_online, poll_data) in &results {
                    reg.set_online_status(url, *is_online);

                    let key = normalize_url(url);
                    let entry = store.data.entry(key).or_default();

                    // Only update fields that were polled (preserve last known on failure)
                    if *is_online {
                        if poll_data.status_data.is_some() {
                            entry.status_data = poll_data.status_data.clone();
                        }
                        if poll_data.beads_data.is_some() {
                            entry.beads_data = poll_data.beads_data.clone();
                        }
                        if poll_data.metrics_data.is_some() {
                            entry.metrics_data = poll_data.metrics_data.clone();
                        }
                        if poll_data.project_info.is_some() {
                            entry.project_info = poll_data.project_info.clone();
                        }
                        if poll_data.improvements_data.is_some() {
                            entry.improvements_data = poll_data.improvements_data.clone();
                        }
                    }
                    // If offline, preserve last known state (don't clear)
                }

                // Recompute aggregate
                store.aggregate = compute_aggregate(&reg.list(), &store.data);
            }

            tick_count += 1;
            tokio::time::sleep(poll_interval).await;
        }
    });
}

/// Poll a single instance. Returns (is_online, poll_data).
async fn poll_instance(
    client: &reqwest::Client,
    base_url: &str,
    fetch_project: bool,
    fetch_improvements: bool,
) -> (bool, InstancePollData) {
    let mut data = InstancePollData::default();

    // Health check first
    let health_url = format!("{base_url}/api/health");
    let is_online = match client.get(&health_url).send().await {
        Ok(resp) => resp.status().is_success(),
        Err(_) => false,
    };

    if !is_online {
        return (false, data);
    }

    // Build URLs
    let status_url = format!("{base_url}/api/status");
    let beads_url = format!("{base_url}/api/beads");
    let metrics_url = format!("{base_url}/api/metrics/summary");
    let project_url = format!("{base_url}/api/project");
    let improvements_url = format!("{base_url}/api/improvements");

    // Poll all endpoints concurrently
    let status_fut = fetch_json(client, &status_url);
    let beads_fut = fetch_json(client, &beads_url);
    let metrics_fut = fetch_json(client, &metrics_url);

    let project_fut = if fetch_project {
        Some(fetch_json(client, &project_url))
    } else {
        None
    };

    let improvements_fut = if fetch_improvements {
        Some(fetch_json(client, &improvements_url))
    } else {
        None
    };

    let (status, beads, metrics) = tokio::join!(status_fut, beads_fut, metrics_fut);
    data.status_data = status;
    data.beads_data = beads;
    data.metrics_data = metrics;

    if let Some(fut) = project_fut {
        data.project_info = fut.await;
    }

    if let Some(fut) = improvements_fut {
        data.improvements_data = fut.await;
    }

    (true, data)
}

/// Fetch JSON from a URL, returning None on any error (including 404).
async fn fetch_json(client: &reqwest::Client, url: &str) -> Option<serde_json::Value> {
    match client.get(url).send().await {
        Ok(resp) if resp.status().is_success() => resp.json().await.ok(),
        _ => None,
    }
}

/// Compute cross-project aggregates from all instances.
fn compute_aggregate(
    instances: &[Instance],
    data: &HashMap<String, InstancePollData>,
) -> Aggregate {
    let mut agg = Aggregate::default();
    let mut total_workers_active: u64 = 0;
    let mut total_workers_max: u64 = 0;

    agg.instances_total = instances.len() as u64;
    agg.instances_online = instances.iter().filter(|i| i.online).count() as u64;

    for inst in instances {
        let key = normalize_url(&inst.url);
        if let Some(poll_data) = data.get(&key) {
            // Beads aggregation
            if let Some(beads_val) = &poll_data.beads_data {
                if let Ok(summary) = serde_json::from_value::<BeadsSummary>(beads_val.clone()) {
                    agg.total_beads_open += summary.open;
                    agg.total_beads_in_progress += summary.in_progress;
                }
            }

            // Metrics aggregation
            if let Some(metrics_val) = &poll_data.metrics_data {
                if let Ok(summary) = serde_json::from_value::<MetricsSummary>(metrics_val.clone()) {
                    agg.total_cost_today += summary.cost_today;
                    total_workers_active += summary.workers_active;
                    total_workers_max += summary.workers_max;
                }
            }
        }

        // Also use heartbeat data for worker counts if available
        if let (Some(active), Some(max)) = (inst.workers_active, inst.workers_max) {
            if total_workers_max == 0 {
                // Only use heartbeat data if no metrics endpoint data
                total_workers_active += active;
                total_workers_max += max;
            }
        }
    }

    agg.worker_utilization = if total_workers_max > 0 {
        total_workers_active as f64 / total_workers_max as f64
    } else {
        0.0
    };

    agg
}

fn normalize_url(url: &str) -> String {
    url.trim_end_matches('/').to_lowercase()
}
