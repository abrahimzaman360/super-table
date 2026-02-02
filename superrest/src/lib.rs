//! # SuperTable REST Catalog API
//!
//! Implementation of the Iceberg REST Catalog specification for SuperTable.

use axum::{
    Json, Router,
    extract::Request,
    http::StatusCode,
    middleware::{self, Next},
    response::Response,
    routing::get,
};
use jsonwebtoken::{DecodingKey, Validation, decode};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;

/// JWT Claims structure
#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    sub: String,
    exp: usize,
}

/// Authentication middleware
async fn auth_middleware(req: Request, next: Next) -> Result<Response, StatusCode> {
    let auth_header = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;

    if !auth_header.starts_with("Bearer ") {
        return Err(StatusCode::UNAUTHORIZED);
    }

    let token = &auth_header[7..];
    let secret = "supersecret"; // In production, use environment variables

    decode::<Claims>(
        token,
        &DecodingKey::from_secret(secret.as_ref()),
        &Validation::default(),
    )
    .map_err(|_| StatusCode::UNAUTHORIZED)?;

    Ok(next.run(req).await)
}

/// Configuration for the REST catalog.
#[derive(Debug, Serialize, Deserialize)]
pub struct CatalogConfig {
    pub overrides: HashMap<String, String>,
    pub defaults: HashMap<String, String>,
}

/// GET /v1/config
pub async fn get_config() -> Json<CatalogConfig> {
    let mut overrides = HashMap::new();
    overrides.insert("warehouse".to_string(), "s3://bucket/warehouse".to_string());

    Json(CatalogConfig {
        overrides,
        defaults: HashMap::new(),
    })
}

/// GET /v1/namespaces
pub async fn list_namespaces() -> Json<Vec<String>> {
    Json(vec!["default".to_string()])
}

/// GET /v1/namespaces/:ns/tables
pub async fn list_tables(
    axum::extract::Path(_ns): axum::extract::Path<String>,
) -> Json<Vec<String>> {
    Json(vec!["my_table".to_string()])
}

/// GET /v1/namespaces/:ns/tables/:table
pub async fn get_table(
    axum::extract::Path((_ns, _table)): axum::extract::Path<(String, String)>,
) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "metadata-location": "s3://bucket/warehouse/default/my_table/metadata/v1.json",
        "metadata": {}
    }))
}

/// POST /v1/namespaces/:ns/tables
pub async fn create_table(
    axum::extract::Path(_ns): axum::extract::Path<String>,
    Json(_req): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    // Skeleton implementation
    Json(serde_json::json!({
        "metadata-location": "s3://bucket/warehouse/default/my_table/metadata/v1.json",
        "metadata": {}
    }))
}

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let app = Router::<()>::new()
        .route("/v1/config", get(get_config))
        .route("/v1/namespaces", get(list_namespaces))
        .route(
            "/v1/namespaces/:ns/tables",
            get(list_tables).post(create_table),
        )
        .route("/v1/namespaces/:ns/tables/:table", get(get_table))
        .layer(middleware::from_fn(auth_middleware));

    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    tracing::info!("SuperTable: Listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
