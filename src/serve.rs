use crate::config::{HarnessConfig, ServeConfig};
use crate::data_dir::DataDir;

#[cfg(feature = "serve")]
#[derive(Clone)]
struct AppState {
    db_path: std::path::PathBuf,
    beads_dir: std::path::PathBuf,
    stop_file: std::path::PathBuf,
}

#[cfg(feature = "serve")]
pub async fn run(config: &HarnessConfig) -> Result<(), Box<dyn std::error::Error>> {
    use axum::{
        routing::{get, post},
        Router,
    };
    use tower_http::cors::CorsLayer;

    let dd = DataDir::new(&config.storage.data_dir);
    let beads_dir = std::path::PathBuf::from(".beads");
    let state = AppState {
        db_path: dd.db(),
        beads_dir,
        stop_file: config.shutdown.stop_file.clone(),
    };

    let app = Router::new()
        .route("/api/health", get(health))
        .route("/api/improvements", get(api_improvements))
        .route("/api/beads", get(api_beads))
        .route("/api/stop", post(api_stop))
        .route("/api/estimate", get(api_estimate))
        .with_state(state)
        .layer(CorsLayer::permissive());

    let serve_config = &config.serve;
    let addr = format!("{}:{}", serve_config.bind, serve_config.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    let local_addr = listener.local_addr()?;
    tracing::info!("serve listening on {local_addr}");

    if serve_config.heartbeat {
        let heartbeat_config = HeartbeatConfig::from_serve_config(serve_config, local_addr);
        tokio::spawn(heartbeat_loop(heartbeat_config));
    }

    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(feature = "serve")]
async fn health() -> axum::Json<serde_json::Value> {
    axum::Json(serde_json::json!({"ok": true}))
}

#[cfg(feature = "serve")]
async fn api_stop(
    axum::extract::State(state): axum::extract::State<AppState>,
) -> Result<axum::Json<serde_json::Value>, axum::http::StatusCode> {
    std::fs::write(&state.stop_file, "")
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(axum::Json(serde_json::json!({"ok": true})))
}

#[cfg(feature = "serve")]
async fn api_improvements(
    axum::extract::State(state): axum::extract::State<AppState>,
) -> Result<axum::Json<Vec<crate::db::Improvement>>, axum::http::StatusCode> {
    let conn = crate::db::open_or_create(&state.db_path)
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    let improvements = crate::db::list_improvements(&conn, None, None)
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(axum::Json(improvements))
}

#[cfg(feature = "serve")]
#[derive(serde::Deserialize)]
struct BeadsQuery {
    status: Option<String>,
}

#[cfg(feature = "serve")]
#[derive(serde::Serialize)]
struct BeadItem {
    id: String,
    title: String,
    status: String,
    #[serde(rename = "type")]
    issue_type: String,
    priority: u8,
    assignee: Option<String>,
    description: String,
}

#[cfg(feature = "serve")]
#[derive(serde::Serialize)]
struct BeadsResponse {
    open_count: usize,
    in_progress_count: usize,
    items: Vec<BeadItem>,
}

#[cfg(feature = "serve")]
async fn api_beads(
    axum::extract::State(state): axum::extract::State<AppState>,
    axum::extract::Query(query): axum::extract::Query<BeadsQuery>,
) -> Result<axum::Json<BeadsResponse>, axum::http::StatusCode> {
    let issues_path = state.beads_dir.join("issues.jsonl");
    let content = std::fs::read_to_string(&issues_path)
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut all_items: Vec<BeadItem> = Vec::new();
    let mut open_count: usize = 0;
    let mut in_progress_count: usize = 0;

    for line in content.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let v: serde_json::Value = serde_json::from_str(line)
            .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

        let status = v["status"].as_str().unwrap_or("unknown").to_string();

        if status == "open" {
            open_count += 1;
        } else if status == "in_progress" {
            in_progress_count += 1;
        }

        // Apply status filter if provided
        if let Some(ref filter) = query.status {
            if &status != filter {
                continue;
            }
        }

        all_items.push(BeadItem {
            id: v["id"].as_str().unwrap_or("").to_string(),
            title: v["title"].as_str().unwrap_or("").to_string(),
            status,
            issue_type: v["issue_type"].as_str().unwrap_or("").to_string(),
            priority: v["priority"].as_u64().unwrap_or(4) as u8,
            assignee: v["owner"].as_str().map(|s| s.to_string()),
            description: v["description"].as_str().unwrap_or("").to_string(),
        });
    }

    Ok(axum::Json(BeadsResponse {
        open_count,
        in_progress_count,
        items: all_items,
    }))
}

#[cfg(feature = "serve")]
#[derive(serde::Deserialize)]
struct EstimateQuery {
    workers: Option<u32>,
}

#[cfg(feature = "serve")]
#[derive(serde::Serialize)]
struct EstimateResponse {
    beads_remaining: usize,
    serial_eta_secs: Option<f64>,
    parallel_eta_secs: Option<f64>,
    critical_path: usize,
    workers: u32,
    completed_count: usize,
    avg_time_per_bead: Option<f64>,
}

#[cfg(feature = "serve")]
async fn api_estimate(
    axum::extract::State(state): axum::extract::State<AppState>,
    axum::extract::Query(query): axum::extract::Query<EstimateQuery>,
) -> Result<axum::Json<EstimateResponse>, axum::http::StatusCode> {
    let workers = query.workers.unwrap_or(1);

    let conn = crate::db::open_or_create(&state.db_path)
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let open_beads = crate::estimation::query_open_beads();

    let est = crate::estimation::estimate(&conn, &open_beads, workers);

    Ok(axum::Json(EstimateResponse {
        beads_remaining: est.open_count,
        serial_eta_secs: est.serial_secs,
        parallel_eta_secs: est.parallel_secs,
        critical_path: est.critical_path_len,
        workers: est.workers,
        completed_count: est.completed_count,
        avg_time_per_bead: est.avg_time_per_bead,
    }))
}

#[cfg(feature = "serve")]
struct HeartbeatConfig {
    multicast_addr: std::net::SocketAddr,
    api_url: String,
    project: String,
}

#[cfg(feature = "serve")]
impl HeartbeatConfig {
    fn from_serve_config(config: &ServeConfig, local_addr: std::net::SocketAddr) -> Self {
        let multicast_addr: std::net::SocketAddr = config
            .heartbeat_address
            .parse()
            .unwrap_or_else(|_| "239.66.83.77:8421".parse().unwrap());

        let api_url = config.api_advertise.clone().unwrap_or_else(|| {
            let host = if local_addr.ip().is_unspecified() {
                "127.0.0.1".to_string()
            } else {
                local_addr.ip().to_string()
            };
            format!("http://{}:{}", host, local_addr.port())
        });

        let project = std::env::current_dir()
            .ok()
            .and_then(|p| p.file_name().map(|n| n.to_string_lossy().into_owned()))
            .unwrap_or_else(|| "unknown".to_string());

        Self {
            multicast_addr,
            api_url,
            project,
        }
    }
}

#[cfg(feature = "serve")]
async fn heartbeat_loop(config: HeartbeatConfig) {
    use socket2::SockAddr;

    let socket = match create_multicast_socket(&config.multicast_addr) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!("heartbeat: failed to create multicast socket: {e}");
            return;
        }
    };

    let dest = SockAddr::from(config.multicast_addr);
    let pid = std::process::id();
    let version = env!("CARGO_PKG_VERSION");

    tracing::info!(
        "heartbeat: broadcasting to {} every 30s",
        config.multicast_addr
    );

    loop {
        let payload = serde_json::json!({
            "v": version,
            "project": config.project,
            "api": config.api_url,
            "status": "serving",
            "workers_active": 0,
            "workers_max": 0,
            "iteration": 0,
            "max_iterations": 0,
            "pid": pid,
        });

        let bytes = payload.to_string();
        if let Err(e) = socket.send_to(bytes.as_bytes(), &dest) {
            tracing::debug!("heartbeat: send failed: {e}");
        }

        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
    }
}

#[cfg(feature = "serve")]
fn create_multicast_socket(
    _addr: &std::net::SocketAddr,
) -> Result<socket2::Socket, Box<dyn std::error::Error>> {
    use socket2::{Domain, Protocol, Socket, Type};

    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_multicast_ttl_v4(2)?;
    socket.set_multicast_loop_v4(true)?;
    socket.set_nonblocking(true)?;
    // Bind to ephemeral port — sender only needs to send, not receive
    let bind_addr: std::net::SocketAddr = "0.0.0.0:0".parse()?;
    socket.bind(&bind_addr.into())?;
    Ok(socket)
}
