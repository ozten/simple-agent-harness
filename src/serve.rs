use crate::config::{HarnessConfig, ServeConfig};
use crate::data_dir::DataDir;

#[cfg(feature = "serve")]
#[derive(Clone)]
struct AppState {
    db_path: std::path::PathBuf,
    beads_dir: std::path::PathBuf,
    sessions_dir: std::path::PathBuf,
    stop_file: std::path::PathBuf,
    status_path: std::path::PathBuf,
    project_name: String,
    workers_max: u32,
    max_iterations: u32,
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
    let project_name = std::env::current_dir()
        .ok()
        .and_then(|p| p.file_name().map(|n| n.to_string_lossy().into_owned()))
        .unwrap_or_else(|| "unknown".to_string());
    let state = AppState {
        db_path: dd.db(),
        beads_dir,
        sessions_dir: dd.sessions_dir(),
        stop_file: config.shutdown.stop_file.clone(),
        status_path: dd.status(),
        project_name,
        workers_max: config.workers.max,
        max_iterations: config.session.max_iterations,
    };

    let app = Router::new()
        .route("/api/health", get(health))
        .route("/api/status", get(api_status))
        .route("/api/project", get(api_project))
        .route("/api/metrics/summary", get(api_metrics_summary))
        .route("/api/improvements", get(api_improvements))
        .route("/api/beads", get(api_beads))
        .route("/api/sessions", get(api_sessions))
        .route("/api/sessions/{id}", get(api_session_detail))
        .route("/api/sessions/{id}/transcript", get(api_session_transcript))
        .route("/api/sessions/{id}/stream", get(api_session_stream))
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
        let dd = DataDir::new(&config.storage.data_dir);
        let heartbeat_ctx = HeartbeatContext {
            status_path: dd.status(),
            workers_max: config.workers.max,
            max_iterations: config.session.max_iterations,
        };
        tokio::spawn(heartbeat_loop(heartbeat_config, heartbeat_ctx));
    }

    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(feature = "serve")]
async fn health() -> axum::Json<serde_json::Value> {
    axum::Json(serde_json::json!({"ok": true}))
}

#[cfg(feature = "serve")]
async fn api_status(
    axum::extract::State(state): axum::extract::State<AppState>,
) -> axum::Json<serde_json::Value> {
    use crate::status::StatusFile;

    let sf = StatusFile::new(state.status_path.clone());
    match sf.read() {
        Ok(Some(data)) => {
            // Serialize the StatusData directly — it derives Serialize
            match serde_json::to_value(&data) {
                Ok(v) => axum::Json(v),
                Err(_) => axum::Json(serde_json::json!({"state": "unknown"})),
            }
        }
        _ => axum::Json(serde_json::json!({"state": "idle"})),
    }
}

#[cfg(feature = "serve")]
async fn api_project(
    axum::extract::State(state): axum::extract::State<AppState>,
) -> axum::Json<serde_json::Value> {
    axum::Json(serde_json::json!({
        "name": state.project_name,
        "workers_max": state.workers_max,
        "max_iterations": state.max_iterations,
    }))
}

#[cfg(feature = "serve")]
async fn api_metrics_summary(
    axum::extract::State(state): axum::extract::State<AppState>,
) -> Result<axum::Json<serde_json::Value>, axum::http::StatusCode> {
    let conn = crate::db::open_or_create(&state.db_path)
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    // Sum cost from observations data (cost.estimate_usd field in JSON data)
    let observations = crate::db::all_observations(&conn)
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    use chrono::Datelike;

    let now = chrono::Utc::now();
    let today_start = now.format("%Y-%m-%d").to_string();
    let week_start = (now - chrono::Duration::days(now.weekday().num_days_from_monday() as i64))
        .format("%Y-%m-%d")
        .to_string();

    let mut cost_today = 0.0_f64;
    let mut cost_this_week = 0.0_f64;
    let mut beads_closed_today = 0_u64;
    let mut outcomes_success = 0_u64;
    let mut outcomes_failed = 0_u64;
    let mut outcomes_timed_out = 0_u64;

    for obs in &observations {
        let obs_date = &obs.ts[..10]; // "YYYY-MM-DD"
        let data: serde_json::Value = serde_json::from_str(&obs.data).unwrap_or_default();
        let cost = data["cost.estimate_usd"].as_f64().unwrap_or(0.0);

        if obs_date >= week_start.as_str() {
            cost_this_week += cost;
        }
        if obs_date >= today_start.as_str() {
            cost_today += cost;

            // Count outcomes for today
            match obs.outcome.as_deref() {
                Some("success") => outcomes_success += 1,
                Some("failed") => outcomes_failed += 1,
                Some("timed_out") => outcomes_timed_out += 1,
                _ => {}
            }
        }
    }

    // Count beads closed today from bead_metrics
    let all_bead_metrics = crate::db::all_bead_metrics(&conn)
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    for bm in &all_bead_metrics {
        if let Some(ref completed) = bm.completed_at {
            if completed.len() >= 10 && &completed[..10] >= today_start.as_str() {
                beads_closed_today += 1;
            }
        }
    }

    // Read live worker counts from status file
    let (workers_active, workers_max) = {
        use crate::status::StatusFile;
        let sf = StatusFile::new(state.status_path.clone());
        match sf.read() {
            Ok(Some(data)) => {
                let active = match data.state {
                    crate::status::HarnessState::SessionRunning
                    | crate::status::HarnessState::PreHooks
                    | crate::status::HarnessState::PostHooks => state.workers_max,
                    _ => 0,
                };
                (active as u64, state.workers_max as u64)
            }
            _ => (0, state.workers_max as u64),
        }
    };

    Ok(axum::Json(serde_json::json!({
        "cost_today": cost_today,
        "cost_this_week": cost_this_week,
        "workers_active": workers_active,
        "workers_max": workers_max,
        "beads_closed_today": beads_closed_today,
        "session_outcomes": {
            "success": outcomes_success,
            "failed": outcomes_failed,
            "timed_out": outcomes_timed_out,
        },
    })))
}

#[cfg(feature = "serve")]
#[derive(serde::Deserialize)]
struct SessionsQuery {
    last: Option<i64>,
}

#[cfg(feature = "serve")]
#[derive(serde::Serialize)]
struct SessionItem {
    id: i64,
    ts: String,
    duration_secs: Option<i64>,
    outcome: Option<String>,
    #[serde(flatten)]
    data: serde_json::Value,
}

#[cfg(feature = "serve")]
fn observation_to_session_item(obs: &crate::db::Observation) -> SessionItem {
    let data: serde_json::Value = serde_json::from_str(&obs.data).unwrap_or_default();
    SessionItem {
        id: obs.session,
        ts: obs.ts.clone(),
        duration_secs: obs.duration,
        outcome: obs.outcome.clone(),
        data,
    }
}

#[cfg(feature = "serve")]
async fn api_sessions(
    axum::extract::State(state): axum::extract::State<AppState>,
    axum::extract::Query(query): axum::extract::Query<SessionsQuery>,
) -> Result<axum::Json<Vec<SessionItem>>, axum::http::StatusCode> {
    let conn = crate::db::open_or_create(&state.db_path)
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let limit = query.last.unwrap_or(50);
    let observations = crate::db::recent_observations(&conn, limit)
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let items: Vec<SessionItem> = observations
        .iter()
        .map(observation_to_session_item)
        .collect();
    Ok(axum::Json(items))
}

#[cfg(feature = "serve")]
async fn api_session_detail(
    axum::extract::State(state): axum::extract::State<AppState>,
    axum::extract::Path(id): axum::extract::Path<i64>,
) -> Result<axum::Json<SessionItem>, (axum::http::StatusCode, axum::Json<serde_json::Value>)> {
    let conn = crate::db::open_or_create(&state.db_path).map_err(|_| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(serde_json::json!({"error": "database error"})),
        )
    })?;

    match crate::db::get_observation(&conn, id) {
        Ok(Some(obs)) => Ok(axum::Json(observation_to_session_item(&obs))),
        Ok(None) => Err((
            axum::http::StatusCode::NOT_FOUND,
            axum::Json(serde_json::json!({"error": "session not found", "id": id})),
        )),
        Err(_) => Err((
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(serde_json::json!({"error": "database error"})),
        )),
    }
}

/// Read a session JSONL file (plain or zstd-compressed) and return its lines.
#[cfg(feature = "serve")]
fn read_session_lines(
    sessions_dir: &std::path::Path,
    id: &str,
) -> Result<Vec<String>, (axum::http::StatusCode, axum::Json<serde_json::Value>)> {
    let plain = sessions_dir.join(format!("{id}.jsonl"));
    let compressed = sessions_dir.join(format!("{id}.jsonl.zst"));

    let content = if plain.exists() {
        std::fs::read(&plain).map_err(|_| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(serde_json::json!({"error": "failed to read session file"})),
            )
        })?
    } else if compressed.exists() {
        let data = std::fs::read(&compressed).map_err(|_| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(serde_json::json!({"error": "failed to read compressed session file"})),
            )
        })?;
        zstd::decode_all(std::io::Cursor::new(data)).map_err(|_| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(serde_json::json!({"error": "failed to decompress session file"})),
            )
        })?
    } else {
        return Err((
            axum::http::StatusCode::NOT_FOUND,
            axum::Json(serde_json::json!({"error": "session not found", "id": id})),
        ));
    };

    let text = String::from_utf8_lossy(&content);
    Ok(text
        .lines()
        .filter(|l| !l.is_empty())
        .map(String::from)
        .collect())
}

/// Parse JSONL lines into transcript turns for the UI.
///
/// Each JSONL line has a `type` field (system, assistant, user, result).
/// We extract meaningful turns with role, content, and timestamp.
#[cfg(feature = "serve")]
fn parse_transcript_turns(lines: &[String]) -> Vec<serde_json::Value> {
    let mut turns = Vec::new();

    for line in lines {
        let v: serde_json::Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) => continue,
        };

        let event_type = v["type"].as_str().unwrap_or("");

        match event_type {
            "assistant" => {
                let role = v["message"]["role"].as_str().unwrap_or("assistant");
                let content = &v["message"]["content"];
                if content.is_null() {
                    continue;
                }
                turns.push(serde_json::json!({
                    "role": role,
                    "content": content,
                }));
            }
            "user" => {
                let role = v["message"]["role"].as_str().unwrap_or("user");
                let content = &v["message"]["content"];
                if content.is_null() {
                    continue;
                }
                turns.push(serde_json::json!({
                    "role": role,
                    "content": content,
                }));
            }
            "system" => {
                let subtype = v["subtype"].as_str().unwrap_or("");
                if subtype == "init" {
                    turns.push(serde_json::json!({
                        "role": "system",
                        "content": format!("Session initialized (model: {})", v["model"].as_str().unwrap_or("unknown")),
                    }));
                }
            }
            "result" => {
                if let Some(result) = v.get("result") {
                    turns.push(serde_json::json!({
                        "role": "system",
                        "content": format!("Session ended: {}", v["subtype"].as_str().unwrap_or("complete")),
                        "result": result,
                    }));
                }
            }
            _ => {}
        }
    }

    turns
}

#[cfg(feature = "serve")]
async fn api_session_transcript(
    axum::extract::State(state): axum::extract::State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Result<axum::Json<serde_json::Value>, (axum::http::StatusCode, axum::Json<serde_json::Value>)>
{
    let lines = read_session_lines(&state.sessions_dir, &id)?;
    let turns = parse_transcript_turns(&lines);
    Ok(axum::Json(serde_json::json!({ "turns": turns })))
}

#[cfg(feature = "serve")]
async fn api_session_stream(
    axum::extract::State(state): axum::extract::State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Result<
    axum::response::sse::Sse<
        impl tokio_stream::Stream<Item = Result<axum::response::sse::Event, std::convert::Infallible>>,
    >,
    (axum::http::StatusCode, axum::Json<serde_json::Value>),
> {
    use axum::response::sse::{Event, KeepAlive};
    use tokio_stream::StreamExt;

    let lines = read_session_lines(&state.sessions_dir, &id)?;
    let turns = parse_transcript_turns(&lines);

    let stream = tokio_stream::iter(
        turns
            .into_iter()
            .map(|turn| Ok(Event::default().event("turn").data(turn.to_string()))),
    );

    // Append a final "done" event so the client knows replay is complete
    let done = tokio_stream::once(Ok(Event::default().event("done").data("{}")));
    let full_stream = stream.chain(done);

    Ok(axum::response::sse::Sse::new(full_stream).keep_alive(KeepAlive::default()))
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
struct HeartbeatContext {
    status_path: std::path::PathBuf,
    workers_max: u32,
    max_iterations: u32,
}

#[cfg(feature = "serve")]
async fn heartbeat_loop(config: HeartbeatConfig, ctx: HeartbeatContext) {
    use crate::status::StatusFile;
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
    let status_file = StatusFile::new(ctx.status_path);

    // Adaptive heartbeat: fast burst (2s) for first 30s, then settle to 10s
    const BURST_INTERVAL: std::time::Duration = std::time::Duration::from_secs(2);
    const SETTLED_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);
    const BURST_DURATION: std::time::Duration = std::time::Duration::from_secs(30);

    let start = tokio::time::Instant::now();

    tracing::info!(
        "heartbeat: broadcasting to {} (2s burst → 10s settled)",
        config.multicast_addr
    );

    loop {
        // Read live state from the status file written by the coordinator/runner
        let (state, iteration) = match status_file.read() {
            Ok(Some(data)) => (
                format!("{:?}", data.state).to_lowercase(),
                data.global_iteration,
            ),
            _ => ("idle".to_string(), 0),
        };

        let payload = serde_json::json!({
            "v": version,
            "project": config.project,
            "api": config.api_url,
            "status": state,
            "workers_active": ctx.workers_max,
            "workers_max": ctx.workers_max,
            "iteration": iteration,
            "max_iterations": ctx.max_iterations,
            "pid": pid,
        });

        let bytes = payload.to_string();
        if let Err(e) = socket.send_to(bytes.as_bytes(), &dest) {
            tracing::debug!("heartbeat: send failed: {e}");
        }

        let interval = if start.elapsed() < BURST_DURATION {
            BURST_INTERVAL
        } else {
            SETTLED_INTERVAL
        };
        tokio::time::sleep(interval).await;
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

#[cfg(all(test, feature = "serve"))]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Request, StatusCode},
        routing::get,
        Router,
    };
    use tower::ServiceExt;

    fn test_state(dir: &std::path::Path) -> AppState {
        let db_path = dir.join("test.db");
        // Ensure DB exists
        let _conn = crate::db::open_or_create(&db_path).unwrap();

        let beads_dir = dir.join("beads");
        std::fs::create_dir_all(&beads_dir).unwrap();
        // Create a minimal issues.jsonl
        std::fs::write(
            beads_dir.join("issues.jsonl"),
            r#"{"id":"b-1","title":"Test","status":"open","issue_type":"task","priority":2,"owner":null,"description":"desc"}"#,
        )
        .unwrap();

        let sessions_dir = dir.join("sessions");
        std::fs::create_dir_all(&sessions_dir).unwrap();

        AppState {
            db_path,
            beads_dir,
            sessions_dir,
            stop_file: dir.join("stop"),
            status_path: dir.join("status"),
            project_name: "test-project".to_string(),
            workers_max: 2,
            max_iterations: 25,
        }
    }

    fn test_app(state: AppState) -> Router {
        Router::new()
            .route("/api/status", get(api_status))
            .route("/api/project", get(api_project))
            .route("/api/metrics/summary", get(api_metrics_summary))
            .route("/api/sessions", get(api_sessions))
            .route("/api/sessions/{id}", get(api_session_detail))
            .route("/api/sessions/{id}/transcript", get(api_session_transcript))
            .route("/api/sessions/{id}/stream", get(api_session_stream))
            .with_state(state)
    }

    #[tokio::test]
    async fn test_api_status_no_file() {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state(dir.path());
        let app = test_app(state);

        let resp = app
            .oneshot(Request::get("/api/status").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["state"], "idle");
    }

    #[tokio::test]
    async fn test_api_status_with_file() {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state(dir.path());

        // Write a status file
        let sf = crate::status::StatusFile::new(state.status_path.clone());
        let data = crate::status::StatusData {
            pid: std::process::id(),
            state: crate::status::HarnessState::SessionRunning,
            iteration: 3,
            max_iterations: 25,
            global_iteration: 103,
            output_file: "test.jsonl".to_string(),
            output_bytes: 5000,
            session_start: None,
            last_update: chrono::Utc::now(),
            last_completed_iteration: Some(102),
            last_committed: true,
            consecutive_rate_limits: 0,
        };
        sf.write(&data).unwrap();

        let app = test_app(state);
        let resp = app
            .oneshot(Request::get("/api/status").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["state"], "session_running");
        assert_eq!(json["iteration"], 3);
        assert_eq!(json["global_iteration"], 103);
    }

    #[tokio::test]
    async fn test_api_project() {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state(dir.path());
        let app = test_app(state);

        let resp = app
            .oneshot(Request::get("/api/project").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["name"], "test-project");
        assert_eq!(json["workers_max"], 2);
        assert_eq!(json["max_iterations"], 25);
    }

    #[tokio::test]
    async fn test_api_metrics_summary() {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state(dir.path());
        let app = test_app(state);

        let resp = app
            .oneshot(
                Request::get("/api/metrics/summary")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        // Should have all expected fields
        assert!(json["cost_today"].is_number());
        assert!(json["cost_this_week"].is_number());
        assert!(json["workers_active"].is_number());
        assert!(json["workers_max"].is_number());
        assert!(json["beads_closed_today"].is_number());
        assert!(json["session_outcomes"]["success"].is_number());
        assert!(json["session_outcomes"]["failed"].is_number());
        assert!(json["session_outcomes"]["timed_out"].is_number());
    }

    #[tokio::test]
    async fn test_api_sessions_empty() {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state(dir.path());
        let app = test_app(state);

        let resp = app
            .oneshot(Request::get("/api/sessions").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json.as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_api_sessions_with_data() {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state(dir.path());

        // Insert some observations
        let conn = crate::db::open_or_create(&state.db_path).unwrap();
        crate::db::upsert_observation(
            &conn,
            0,
            "2026-02-17T10:00:00Z",
            Some(120),
            Some("success"),
            r#"{"cost.estimate_usd":0.5}"#,
        )
        .unwrap();
        crate::db::upsert_observation(
            &conn,
            1,
            "2026-02-17T11:00:00Z",
            Some(90),
            Some("failed"),
            r#"{"cost.estimate_usd":0.3}"#,
        )
        .unwrap();
        drop(conn);

        let app = test_app(state);
        let resp = app
            .oneshot(Request::get("/api/sessions").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let arr = json.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        // recent_observations returns descending order
        assert_eq!(arr[0]["id"], 1);
        assert_eq!(arr[0]["outcome"], "failed");
        assert_eq!(arr[0]["duration_secs"], 90);
        assert_eq!(arr[1]["id"], 0);
        assert_eq!(arr[1]["outcome"], "success");
    }

    #[tokio::test]
    async fn test_api_sessions_last_param() {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state(dir.path());

        let conn = crate::db::open_or_create(&state.db_path).unwrap();
        for i in 0..5 {
            crate::db::upsert_observation(
                &conn,
                i,
                &format!("2026-02-17T1{}:00:00Z", i),
                Some(60),
                Some("success"),
                "{}",
            )
            .unwrap();
        }
        drop(conn);

        let app = test_app(state);
        let resp = app
            .oneshot(
                Request::get("/api/sessions?last=2")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json.as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_api_session_detail() {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state(dir.path());

        let conn = crate::db::open_or_create(&state.db_path).unwrap();
        crate::db::upsert_observation(
            &conn,
            42,
            "2026-02-17T12:00:00Z",
            Some(300),
            Some("success"),
            r#"{"cost.estimate_usd":1.2,"bead_id":"bd-5"}"#,
        )
        .unwrap();
        drop(conn);

        let app = test_app(state);
        let resp = app
            .oneshot(
                Request::get("/api/sessions/42")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["id"], 42);
        assert_eq!(json["outcome"], "success");
        assert_eq!(json["duration_secs"], 300);
        assert_eq!(json["cost.estimate_usd"], 1.2);
        assert_eq!(json["bead_id"], "bd-5");
    }

    #[tokio::test]
    async fn test_api_session_detail_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state(dir.path());
        let app = test_app(state);

        let resp = app
            .oneshot(
                Request::get("/api/sessions/999")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"], "session not found");
        assert_eq!(json["id"], 999);
    }

    fn write_test_session(sessions_dir: &std::path::Path, id: &str) {
        let content = format!(
            r#"{{"type":"system","subtype":"init","model":"test-model","session_id":"abc"}}
{{"type":"assistant","message":{{"role":"assistant","content":[{{"type":"text","text":"Hello, I will help."}}]}}}}
{{"type":"user","message":{{"role":"user","content":[{{"tool_use_id":"t1","type":"tool_result","content":"file contents"}}]}}}}
{{"type":"assistant","message":{{"role":"assistant","content":[{{"type":"text","text":"Done."}}]}}}}
{{"type":"result","subtype":"success","result":{{"cost":0.5}}}}"#
        );
        std::fs::write(sessions_dir.join(format!("{id}.jsonl")), content).unwrap();
    }

    #[tokio::test]
    async fn test_transcript_returns_turns() {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state(dir.path());
        write_test_session(&state.sessions_dir, "0");

        let app = test_app(state);
        let resp = app
            .oneshot(
                Request::get("/api/sessions/0/transcript")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let turns = json["turns"].as_array().unwrap();
        // system init + 2 assistant + 1 user + 1 result = 5 turns
        assert_eq!(turns.len(), 5);
        assert_eq!(turns[0]["role"], "system");
        assert_eq!(turns[1]["role"], "assistant");
        assert_eq!(turns[2]["role"], "user");
        assert_eq!(turns[3]["role"], "assistant");
        assert_eq!(turns[4]["role"], "system");
    }

    #[tokio::test]
    async fn test_transcript_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state(dir.path());
        let app = test_app(state);

        let resp = app
            .oneshot(
                Request::get("/api/sessions/999/transcript")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"], "session not found");
    }

    #[tokio::test]
    async fn test_stream_completed_session_replays_turns() {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state(dir.path());
        write_test_session(&state.sessions_dir, "0");

        let app = test_app(state);
        let resp = app
            .oneshot(
                Request::get("/api/sessions/0/stream")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers()
                .get("content-type")
                .unwrap()
                .to_str()
                .unwrap(),
            "text/event-stream"
        );

        let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let text = String::from_utf8_lossy(&body);

        // Should contain turn events
        let turn_count = text.matches("event: turn").count();
        assert_eq!(turn_count, 5);

        // Should end with done event
        assert!(text.contains("event: done"));
    }

    #[tokio::test]
    async fn test_stream_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state(dir.path());
        let app = test_app(state);

        let resp = app
            .oneshot(
                Request::get("/api/sessions/999/stream")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_transcript_compressed_session() {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state(dir.path());

        // Write a compressed session file
        let content = r#"{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"compressed turn"}]}}"#;
        let compressed = zstd::encode_all(std::io::Cursor::new(content), 3).unwrap();
        std::fs::write(state.sessions_dir.join("5.jsonl.zst"), compressed).unwrap();

        let app = test_app(state);
        let resp = app
            .oneshot(
                Request::get("/api/sessions/5/transcript")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let turns = json["turns"].as_array().unwrap();
        assert_eq!(turns.len(), 1);
        assert_eq!(turns[0]["role"], "assistant");
    }
}
