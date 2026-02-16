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
    tracing::info!("serve listening on {addr}");
    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(feature = "serve")]
async fn health() -> axum::Json<serde_json::Value> {
    axum::Json(serde_json::json!({"ok": true}))
}
