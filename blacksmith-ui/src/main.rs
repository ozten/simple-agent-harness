mod config;
mod discovery;
mod poller;

use axum::{
    body::Body,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use discovery::{Instance, InstanceRegistry, Registry};
use poller::{Aggregate, GlobalMetrics, PollDataStore, PollStore};
use rust_embed::Embed;
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::RwLock;
use tower_http::cors::CorsLayer;

#[derive(Embed)]
#[folder = "frontend/"]
struct FrontendAssets;

#[derive(Clone)]
struct AppState {
    registry: Registry,
    poll_store: PollStore,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "blacksmith_ui=info".parse().unwrap()),
        )
        .init();

    let cwd = std::env::current_dir().unwrap_or_default();
    let cfg = config::load_config(&cwd);

    // Build registry with manual + runtime-persisted entries
    let mut registry = InstanceRegistry::new();
    registry.add_manual_entries(&cfg.projects);

    let runtime_instances = config::load_runtime_instances();
    for ri in &runtime_instances {
        registry.add_runtime(&ri.url, &ri.name);
    }

    let registry = Arc::new(RwLock::new(registry));
    let poll_store = Arc::new(RwLock::new(PollDataStore::new()));

    // Spawn UDP listener and sweep task
    discovery::spawn_udp_listener(Arc::clone(&registry));
    discovery::spawn_sweep_task(Arc::clone(&registry));

    // Spawn polling loop
    poller::spawn_poller(
        Arc::clone(&registry),
        Arc::clone(&poll_store),
        cfg.dashboard.poll_interval_secs,
    );

    let state = AppState {
        registry,
        poll_store,
    };

    let app = Router::new()
        .route("/api/health", get(health))
        .route("/api/instances", get(list_instances))
        .route("/api/instances", post(add_instance))
        .route("/api/aggregate", get(get_aggregate))
        .route("/api/global-metrics", get(get_global_metrics))
        .route("/api/instances/:url/poll-data", get(get_instance_poll_data))
        .route("/api/instances/:url/stop", post(stop_instance))
        .route(
            "/api/instances/:url/sessions/:session_id/stream",
            get(proxy_session_stream),
        )
        .route(
            "/api/instances/:url/sessions/:session_id/transcript",
            get(proxy_session_transcript),
        )
        .fallback(get(static_handler))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let addr = format!("{}:{}", cfg.dashboard.bind, cfg.dashboard.port);
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    let local_addr = listener.local_addr().unwrap();
    tracing::info!("blacksmith-ui listening on {local_addr}");

    axum::serve(listener, app).await.unwrap();
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({"ok": true}))
}

async fn list_instances(State(state): State<AppState>) -> Json<Vec<Instance>> {
    let reg = state.registry.read().await;
    Json(reg.list())
}

async fn get_aggregate(State(state): State<AppState>) -> Json<Aggregate> {
    let store = state.poll_store.read().await;
    Json(store.aggregate.clone())
}

async fn get_global_metrics(State(state): State<AppState>) -> Json<GlobalMetrics> {
    let store = state.poll_store.read().await;
    Json(store.global_metrics.clone())
}

async fn get_instance_poll_data(
    State(state): State<AppState>,
    axum::extract::Path(url): axum::extract::Path<String>,
) -> Json<serde_json::Value> {
    let key = url.trim_end_matches('/').to_lowercase();
    let store = state.poll_store.read().await;
    match store.data.get(&key) {
        Some(data) => Json(serde_json::to_value(data).unwrap_or_default()),
        None => Json(serde_json::json!({})),
    }
}

#[derive(Deserialize)]
struct AddInstanceRequest {
    url: String,
    #[serde(default)]
    name: Option<String>,
}

async fn static_handler(uri: axum::http::Uri) -> Response {
    let path = uri.path().trim_start_matches('/');
    let path = if path.is_empty() { "index.html" } else { path };

    match FrontendAssets::get(path) {
        Some(file) => {
            let mime = mime_guess::from_path(path).first_or_octet_stream();
            (
                [(axum::http::header::CONTENT_TYPE, mime.as_ref())],
                file.data,
            )
                .into_response()
        }
        None => {
            // SPA fallback: serve index.html for non-file paths
            match FrontendAssets::get("index.html") {
                Some(file) => {
                    ([(axum::http::header::CONTENT_TYPE, "text/html")], file.data).into_response()
                }
                None => (StatusCode::NOT_FOUND, "not found").into_response(),
            }
        }
    }
}

async fn stop_instance(
    State(state): State<AppState>,
    axum::extract::Path(url): axum::extract::Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let instance_url = resolve_instance_url(&state.registry, &url)
        .await
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": "Instance not found"})),
            )
        })?;

    let stop_url = format!("{instance_url}/api/stop");
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": format!("http client error: {e}")})),
            )
        })?;

    let resp = client.post(&stop_url).send().await.map_err(|e| {
        (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({"error": format!("stop request failed: {e}")})),
        )
    })?;

    if resp.status().is_success() {
        Ok(Json(serde_json::json!({"ok": true})))
    } else {
        Err((
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({"error": format!("stop returned {}", resp.status())})),
        ))
    }
}

async fn resolve_instance_url(registry: &Registry, url: &str) -> Option<String> {
    let reg = registry.read().await;
    let instances = reg.list();
    let key = url.trim_end_matches('/').to_lowercase();
    instances
        .into_iter()
        .find(|i| i.url.trim_end_matches('/').to_lowercase() == key)
        .map(|i| i.url)
}

async fn proxy_session_stream(
    State(state): State<AppState>,
    axum::extract::Path((url, session_id)): axum::extract::Path<(String, String)>,
) -> Result<Response, (StatusCode, Json<serde_json::Value>)> {
    let instance_url = resolve_instance_url(&state.registry, &url)
        .await
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": "Instance not found"})),
            )
        })?;

    let stream_url = format!("{instance_url}/api/sessions/{session_id}/stream");
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(3600))
        .build()
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": format!("http client error: {e}")})),
            )
        })?;

    let resp = client.get(&stream_url).send().await.map_err(|e| {
        (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({"error": format!("stream request failed: {e}")})),
        )
    })?;

    if !resp.status().is_success() {
        return Err((
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({"error": format!("stream returned {}", resp.status())})),
        ));
    }

    let body = Body::from_stream(resp.bytes_stream());
    Ok(Response::builder()
        .header("Content-Type", "text/event-stream")
        .header("Cache-Control", "no-cache")
        .header("Connection", "keep-alive")
        .body(body)
        .unwrap())
}

async fn proxy_session_transcript(
    State(state): State<AppState>,
    axum::extract::Path((url, session_id)): axum::extract::Path<(String, String)>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let instance_url = resolve_instance_url(&state.registry, &url)
        .await
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": "Instance not found"})),
            )
        })?;

    let transcript_url = format!("{instance_url}/api/sessions/{session_id}/transcript");
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": format!("http client error: {e}")})),
            )
        })?;

    let resp = client.get(&transcript_url).send().await.map_err(|e| {
        (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({"error": format!("transcript request failed: {e}")})),
        )
    })?;

    if !resp.status().is_success() {
        return Err((
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({"error": format!("transcript returned {}", resp.status())})),
        ));
    }

    let data: serde_json::Value = resp.json().await.map_err(|e| {
        (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({"error": format!("invalid transcript json: {e}")})),
        )
    })?;

    Ok(Json(data))
}

async fn add_instance(
    State(state): State<AppState>,
    Json(req): Json<AddInstanceRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let url = req.url.trim_end_matches('/').to_string();

    // Probe health endpoint
    let health_url = format!("{url}/api/health");
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": format!("http client error: {e}")})),
            )
        })?;

    let resp = client.get(&health_url).send().await.map_err(|e| {
        (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({"error": format!("health check failed: {e}")})),
        )
    })?;

    if !resp.status().is_success() {
        return Err((
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({"error": format!("health check returned {}", resp.status())})),
        ));
    }

    let name = req.name.unwrap_or_else(|| {
        // Try to extract name from URL
        url.split("://")
            .nth(1)
            .unwrap_or(&url)
            .split(':')
            .next()
            .unwrap_or("unknown")
            .to_string()
    });

    let mut reg = state.registry.write().await;
    reg.add_runtime(&url, &name);

    Ok(Json(
        serde_json::json!({"ok": true, "url": url, "name": name}),
    ))
}
