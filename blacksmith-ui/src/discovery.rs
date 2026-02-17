use crate::config::{self, ProjectEntry};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

const OFFLINE_TIMEOUT: Duration = Duration::from_secs(90);
const MULTICAST_ADDR: &str = "239.66.83.77";
const MULTICAST_PORT: u16 = 8421;

#[derive(Debug, Clone, Serialize)]
pub struct Instance {
    pub url: String,
    pub name: String,
    pub source: InstanceSource,
    pub online: bool,
    /// Extra metadata from heartbeat
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workers_active: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workers_max: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub iteration: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_iterations: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pid: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum InstanceSource {
    Manual,
    Udp,
    Runtime,
}

#[derive(Debug, Deserialize)]
struct HeartbeatPacket {
    #[serde(default)]
    v: Option<String>,
    #[serde(default)]
    project: Option<String>,
    #[serde(default)]
    api: Option<String>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    workers_active: Option<u64>,
    #[serde(default)]
    workers_max: Option<u64>,
    #[serde(default)]
    iteration: Option<u64>,
    #[serde(default)]
    max_iterations: Option<u64>,
    #[serde(default)]
    pid: Option<u64>,
}

struct RegistryEntry {
    instance: Instance,
    last_seen: Instant,
}

pub type Registry = Arc<RwLock<InstanceRegistry>>;

pub struct InstanceRegistry {
    /// Keyed by normalized URL
    entries: HashMap<String, RegistryEntry>,
    /// Runtime-added URLs (for persistence)
    runtime_urls: Vec<ProjectEntry>,
}

impl InstanceRegistry {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
            runtime_urls: Vec::new(),
        }
    }

    /// Add manual entries from config. Called once at startup.
    pub fn add_manual_entries(&mut self, projects: &[ProjectEntry]) {
        for p in projects {
            let key = normalize_url(&p.url);
            self.entries
                .entry(key)
                .and_modify(|e| {
                    // Enrich existing entry with manual name
                    e.instance.name = p.name.clone();
                    if e.instance.source == InstanceSource::Udp {
                        // Keep source as UDP but update name
                    }
                })
                .or_insert_with(|| RegistryEntry {
                    instance: Instance {
                        url: p.url.clone(),
                        name: p.name.clone(),
                        source: InstanceSource::Manual,
                        online: false,
                        project: None,
                        status: None,
                        workers_active: None,
                        workers_max: None,
                        iteration: None,
                        max_iterations: None,
                        pid: None,
                        version: None,
                    },
                    last_seen: Instant::now() - OFFLINE_TIMEOUT, // not yet seen
                });
        }
    }

    /// Add or update from a heartbeat packet.
    fn update_from_heartbeat(&mut self, packet: &HeartbeatPacket) {
        let url = match &packet.api {
            Some(u) => u.clone(),
            None => return,
        };
        let key = normalize_url(&url);
        let name = packet
            .project
            .clone()
            .unwrap_or_else(|| "unknown".to_string());

        self.entries
            .entry(key)
            .and_modify(|e| {
                e.last_seen = Instant::now();
                e.instance.online = true;
                e.instance.project = packet.project.clone();
                e.instance.status = packet.status.clone();
                e.instance.workers_active = packet.workers_active;
                e.instance.workers_max = packet.workers_max;
                e.instance.iteration = packet.iteration;
                e.instance.max_iterations = packet.max_iterations;
                e.instance.pid = packet.pid;
                e.instance.version = packet.v.clone();
            })
            .or_insert_with(|| RegistryEntry {
                instance: Instance {
                    url: url.clone(),
                    name,
                    source: InstanceSource::Udp,
                    online: true,
                    project: packet.project.clone(),
                    status: packet.status.clone(),
                    workers_active: packet.workers_active,
                    workers_max: packet.workers_max,
                    iteration: packet.iteration,
                    max_iterations: packet.max_iterations,
                    pid: packet.pid,
                    version: packet.v.clone(),
                },
                last_seen: Instant::now(),
            });
    }

    /// Add a runtime instance (after health check passes). Returns true if new.
    pub fn add_runtime(&mut self, url: &str, name: &str) -> bool {
        let key = normalize_url(url);
        let is_new = !self.entries.contains_key(&key);

        self.entries
            .entry(key)
            .and_modify(|e| {
                e.instance.online = true;
                e.last_seen = Instant::now();
            })
            .or_insert_with(|| RegistryEntry {
                instance: Instance {
                    url: url.to_string(),
                    name: name.to_string(),
                    source: InstanceSource::Runtime,
                    online: true,
                    project: None,
                    status: None,
                    workers_active: None,
                    workers_max: None,
                    iteration: None,
                    max_iterations: None,
                    pid: None,
                    version: None,
                },
                last_seen: Instant::now(),
            });

        if is_new {
            // Track for persistence
            if !self
                .runtime_urls
                .iter()
                .any(|p| normalize_url(&p.url) == normalize_url(url))
            {
                self.runtime_urls.push(ProjectEntry {
                    name: name.to_string(),
                    url: url.to_string(),
                });
                let _ = config::save_runtime_instances(&self.runtime_urls);
            }
        }
        is_new
    }

    /// Mark stale instances offline.
    pub fn sweep_stale(&mut self) {
        let now = Instant::now();
        for entry in self.entries.values_mut() {
            if entry.instance.online && now.duration_since(entry.last_seen) > OFFLINE_TIMEOUT {
                entry.instance.online = false;
            }
        }
    }

    /// Set online/offline status for an instance by URL.
    pub fn set_online_status(&mut self, url: &str, online: bool) {
        let key = normalize_url(url);
        if let Some(entry) = self.entries.get_mut(&key) {
            entry.instance.online = online;
            if online {
                entry.last_seen = Instant::now();
            }
        }
    }

    /// Get all instances as a sorted list.
    pub fn list(&self) -> Vec<Instance> {
        let mut instances: Vec<Instance> =
            self.entries.values().map(|e| e.instance.clone()).collect();
        instances.sort_by(|a, b| a.name.cmp(&b.name));
        instances
    }
}

fn normalize_url(url: &str) -> String {
    url.trim_end_matches('/').to_lowercase()
}

/// Spawn the UDP multicast listener that receives heartbeat packets.
pub fn spawn_udp_listener(registry: Registry) {
    tokio::spawn(async move {
        if let Err(e) = udp_listener_loop(registry).await {
            tracing::warn!("UDP listener exited: {e}");
        }
    });
}

async fn udp_listener_loop(
    registry: Registry,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use socket2::{Domain, Protocol, Socket, Type};

    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_reuse_address(true)?;
    socket.set_nonblocking(true)?;

    let bind_addr: std::net::SocketAddr = format!("0.0.0.0:{MULTICAST_PORT}").parse()?;
    socket.bind(&bind_addr.into())?;

    let multicast_addr: std::net::Ipv4Addr = MULTICAST_ADDR.parse()?;
    socket.join_multicast_v4(&multicast_addr, &std::net::Ipv4Addr::UNSPECIFIED)?;

    let std_socket: std::net::UdpSocket = socket.into();
    let udp = tokio::net::UdpSocket::from_std(std_socket)?;

    tracing::info!("UDP multicast listener active on {MULTICAST_ADDR}:{MULTICAST_PORT}");

    let mut buf = [0u8; 4096];
    loop {
        match udp.recv_from(&mut buf).await {
            Ok((len, _addr)) => {
                if let Ok(text) = std::str::from_utf8(&buf[..len]) {
                    if let Ok(packet) = serde_json::from_str::<HeartbeatPacket>(text) {
                        let mut reg = registry.write().await;
                        reg.update_from_heartbeat(&packet);
                    }
                }
            }
            Err(e) => {
                tracing::debug!("UDP recv error: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

/// Spawn a task that periodically sweeps stale instances.
pub fn spawn_sweep_task(registry: Registry) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(15)).await;
            let mut reg = registry.write().await;
            reg.sweep_stale();
        }
    });
}
