use crate::config::ServeConfig;

#[cfg(feature = "serve")]
pub async fn run(config: &ServeConfig) -> Result<(), Box<dyn std::error::Error>> {
    use axum::{routing::get, Router};
    use tower_http::cors::CorsLayer;

    let app = Router::new()
        .route("/api/health", get(health))
        .layer(CorsLayer::permissive());

    let addr = format!("{}:{}", config.bind, config.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    let local_addr = listener.local_addr()?;
    tracing::info!("serve listening on {local_addr}");

    if config.heartbeat {
        let heartbeat_config = HeartbeatConfig::from_serve_config(config, local_addr);
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
    addr: &std::net::SocketAddr,
) -> Result<socket2::Socket, Box<dyn std::error::Error>> {
    use socket2::{Domain, Protocol, Socket, Type};

    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_reuse_address(true)?;
    socket.set_multicast_ttl_v4(2)?;
    socket.set_nonblocking(true)?;
    // Bind to any address on the multicast port so multiple instances can coexist
    let bind_addr: std::net::SocketAddr = format!("0.0.0.0:{}", addr.port()).parse()?;
    socket.bind(&bind_addr.into())?;
    Ok(socket)
}
