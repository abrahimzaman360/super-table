//! # SuperTable REST Catalog API
//!
//! Implementation of the Iceberg REST Catalog specification for SuperTable.

use axum::{Json, Router, routing::get};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;

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

/// GET /v1/namespaces/{ns}/tables
pub async fn list_tables(
    axum::extract::Path(_ns): axum::extract::Path<String>,
) -> Json<Vec<String>> {
    Json(vec!["my_table".to_string()])
}

/// GET /v1/namespaces/{ns}/tables/{table}
pub async fn get_table(
    axum::extract::Path((_ns, _table)): axum::extract::Path<(String, String)>,
) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "metadata-location": "s3://bucket/warehouse/default/my_table/metadata/v1.json",
        "metadata": {}
    }))
}

/// POST /v1/namespaces/{ns}/tables
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
            "/v1/namespaces/{ns}/tables",
            get(list_tables).post(create_table),
        )
        .route("/v1/namespaces/{ns}/tables/{table}", get(get_table));

    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    tracing::info!("SuperTable: Listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
