//! # SuperTable REST Catalog API
//!
//! Implementation of the Iceberg REST Catalog specification for SuperTable.
//! Provides dynamic table management with a shared catalog backend.

use axum::{
    Json, Router,
    extract::{Request, State, Path},
    http::StatusCode,
    middleware::{self, Next},
    response::{Response, IntoResponse},
    routing::get,
};
use jsonwebtoken::{DecodingKey, Validation, decode};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use supercore::prelude::*;
use utoipa::{OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;

/// OpenAPI documentation
#[derive(OpenApi)]
#[openapi(
    info(
        title = "SuperTable REST Catalog API",
        version = "1.0.0",
        description = "Iceberg-compatible REST Catalog for SuperTable"
    ),
    paths(
        health,
        get_config,
        list_namespaces,
        create_namespace,
        list_tables,
        create_table,
        get_table,
        drop_table
    ),
    components(schemas(
        CatalogConfig,
        CreateTableRequest,
        SchemaDto,
        FieldDto,
        HealthResponse,
        NamespaceResponse,
        TableListResponse
    ))
)]
pub struct ApiDoc;

/// Shared application state
pub struct AppState {
    pub catalog: InMemoryCatalog,
}

/// JWT Claims structure
#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    sub: String,
    exp: usize,
}

/// Authentication middleware
async fn auth_middleware(req: Request, next: Next) -> Result<Response, StatusCode> {
    // Allow unauthenticated access for health checks and swagger
    let path = req.uri().path();
    if path == "/health" || path.starts_with("/swagger-ui") || path.starts_with("/api-docs") {
        return Ok(next.run(req).await);
    }

    let auth_header = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok());

    // For development, allow unauthenticated access if no auth header
    if auth_header.is_none() {
        tracing::warn!("No auth header - allowing in dev mode");
        return Ok(next.run(req).await);
    }

    let auth_header = auth_header.unwrap();
    if !auth_header.starts_with("Bearer ") {
        return Err(StatusCode::UNAUTHORIZED);
    }

    let token = &auth_header[7..];
    let secret = std::env::var("JWT_SECRET").unwrap_or_else(|_| "supersecret".to_string());

    decode::<Claims>(
        token,
        &DecodingKey::from_secret(secret.as_ref()),
        &Validation::default(),
    )
    .map_err(|_| StatusCode::UNAUTHORIZED)?;

    Ok(next.run(req).await)
}

/// Configuration for the REST catalog.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CatalogConfig {
    /// Configuration overrides
    pub overrides: HashMap<String, String>,
    /// Default configuration values
    pub defaults: HashMap<String, String>,
}

/// Health check response
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct HealthResponse {
    pub status: String,
}

/// Namespace list response
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct NamespaceResponse {
    pub namespaces: Vec<Vec<String>>,
}

/// Table list response
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct TableListResponse {
    pub identifiers: Vec<TableIdentifierDto>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct TableIdentifierDto {
    pub namespace: Vec<String>,
    pub name: String,
}

/// Create table request
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateTableRequest {
    /// Table name
    pub name: String,
    /// Table schema definition
    pub schema: SchemaDto,
    /// Optional table properties
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

/// Schema DTO for API
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct SchemaDto {
    /// List of fields in the schema
    pub fields: Vec<FieldDto>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct FieldDto {
    /// Unique field ID
    pub id: i32,
    /// Field name
    pub name: String,
    /// Field type (long, int, string, boolean, float, double, date, binary)
    #[serde(rename = "type")]
    pub field_type: String,
    /// Whether the field is required
    pub required: bool,
}

impl From<SchemaDto> for Schema {
    fn from(dto: SchemaDto) -> Self {
        let mut builder = Schema::builder(0);
        for field in dto.fields {
            let field_type = match field.field_type.as_str() {
                "long" => Type::Long,
                "int" => Type::Int,
                "string" => Type::String,
                "boolean" => Type::Boolean,
                "float" => Type::Float,
                "double" => Type::Double,
                "date" => Type::Date,
                "binary" => Type::Binary,
                _ => Type::String,
            };
            builder = builder.with_field(field.id, &field.name, field_type, field.required);
        }
        builder.build()
    }
}

/// Health check endpoint
#[utoipa::path(
    get,
    path = "/health",
    responses(
        (status = 200, description = "Service is healthy", body = HealthResponse)
    ),
    tag = "Health"
)]
pub async fn health() -> impl IntoResponse {
    Json(HealthResponse { status: "ok".to_string() })
}

/// Get catalog configuration
#[utoipa::path(
    get,
    path = "/v1/config",
    responses(
        (status = 200, description = "Catalog configuration", body = CatalogConfig)
    ),
    tag = "Config"
)]
pub async fn get_config() -> Json<CatalogConfig> {
    let mut overrides = HashMap::new();
    overrides.insert("warehouse".to_string(), "s3://bucket/warehouse".to_string());

    Json(CatalogConfig {
        overrides,
        defaults: HashMap::new(),
    })
}

/// List all namespaces
#[utoipa::path(
    get,
    path = "/v1/namespaces",
    responses(
        (status = 200, description = "List of namespaces", body = NamespaceResponse)
    ),
    tag = "Namespaces"
)]
pub async fn list_namespaces(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match state.catalog.list_namespaces(None).await {
        Ok(namespaces) => Json(serde_json::json!({
            "namespaces": namespaces
        })),
        Err(_) => Json(serde_json::json!({
            "namespaces": []
        })),
    }
}

/// Create a new namespace
#[utoipa::path(
    post,
    path = "/v1/namespaces",
    request_body = serde_json::Value,
    responses(
        (status = 200, description = "Namespace created")
    ),
    tag = "Namespaces"
)]
pub async fn create_namespace(
    State(state): State<Arc<AppState>>,
    Json(req): Json<serde_json::Value>,
) -> impl IntoResponse {
    let ns = req.get("namespace")
        .and_then(|n| n.as_array())
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_str())
        .unwrap_or("default");
    
    let _ = state.catalog.create_namespace(&[ns.to_string()], HashMap::new()).await;
    
    Json(serde_json::json!({
        "namespace": [ns],
        "properties": {}
    }))
}

/// List tables in a namespace
#[utoipa::path(
    get,
    path = "/v1/namespaces/{ns}/tables",
    params(
        ("ns" = String, Path, description = "Namespace name")
    ),
    responses(
        (status = 200, description = "List of tables", body = TableListResponse)
    ),
    tag = "Tables"
)]
pub async fn list_tables(
    State(state): State<Arc<AppState>>,
    Path(ns): Path<String>,
) -> impl IntoResponse {
    match state.catalog.list_tables(&[ns.clone()]).await {
        Ok(tables) => Json(serde_json::json!({
            "identifiers": tables.iter().map(|t| {
                serde_json::json!({
                    "namespace": [ns.clone()],
                    "name": t.name.clone()
                })
            }).collect::<Vec<_>>()
        })),
        Err(_) => Json(serde_json::json!({"identifiers": []})),
    }
}

/// Create a new table
#[utoipa::path(
    post,
    path = "/v1/namespaces/{ns}/tables",
    params(
        ("ns" = String, Path, description = "Namespace name")
    ),
    request_body = CreateTableRequest,
    responses(
        (status = 201, description = "Table created"),
        (status = 400, description = "Table creation failed")
    ),
    tag = "Tables"
)]
pub async fn create_table(
    State(state): State<Arc<AppState>>,
    Path(ns): Path<String>,
    Json(req): Json<CreateTableRequest>,
) -> impl IntoResponse {
    let schema: Schema = req.schema.into();
    let location = format!("s3://bucket/warehouse/{}/{}", ns, req.name);
    let metadata = TableMetadata::builder(&location, schema)
        .with_properties(req.properties.into_iter())
        .build();

    let identifier = TableIdentifier::new([ns.clone()], &req.name);
    
    match state.catalog.create_table(&identifier, metadata.clone()).await {
        Ok(_) => {
            let metadata_location = format!("{}/metadata/v1.json", location);
            (StatusCode::CREATED, Json(serde_json::json!({
                "metadata-location": metadata_location,
                "metadata": {
                    "format-version": metadata.format_version,
                    "table-uuid": metadata.table_uuid.to_string(),
                    "location": metadata.location,
                    "schemas": metadata.schemas,
                    "current-schema-id": metadata.current_schema_id
                }
            })))
        }
        Err(e) => (StatusCode::BAD_REQUEST, Json(serde_json::json!({
            "error": "TableCreationFailed",
            "message": e.to_string()
        }))),
    }
}

/// Get table metadata
#[utoipa::path(
    get,
    path = "/v1/namespaces/{ns}/tables/{table}",
    params(
        ("ns" = String, Path, description = "Namespace name"),
        ("table" = String, Path, description = "Table name")
    ),
    responses(
        (status = 200, description = "Table metadata"),
        (status = 404, description = "Table not found")
    ),
    tag = "Tables"
)]
pub async fn get_table(
    State(state): State<Arc<AppState>>,
    Path((ns, table)): Path<(String, String)>,
) -> impl IntoResponse {
    let identifier = TableIdentifier::new([ns], &table);
    
    match state.catalog.load_table(&identifier).await {
        Ok(metadata) => {
            let metadata_location = format!("{}/metadata/v1.json", metadata.location);
            (StatusCode::OK, Json(serde_json::json!({
                "metadata-location": metadata_location,
                "metadata": {
                    "format-version": metadata.format_version,
                    "table-uuid": metadata.table_uuid.to_string(),
                    "location": metadata.location,
                    "schemas": metadata.schemas,
                    "current-schema-id": metadata.current_schema_id,
                    "current-snapshot-id": metadata.current_snapshot_id,
                    "snapshots": metadata.snapshots,
                    "properties": metadata.properties
                }
            })))
        }
        Err(e) => (StatusCode::NOT_FOUND, Json(serde_json::json!({
            "error": "TableNotFound",
            "message": e.to_string()
        }))),
    }
}

/// Drop a table
#[utoipa::path(
    delete,
    path = "/v1/namespaces/{ns}/tables/{table}",
    params(
        ("ns" = String, Path, description = "Namespace name"),
        ("table" = String, Path, description = "Table name")
    ),
    responses(
        (status = 204, description = "Table dropped"),
        (status = 404, description = "Table not found")
    ),
    tag = "Tables"
)]
pub async fn drop_table(
    State(state): State<Arc<AppState>>,
    Path((ns, table)): Path<(String, String)>,
) -> impl IntoResponse {
    let identifier = TableIdentifier::new([ns], &table);
    
    match state.catalog.drop_table(&identifier, false).await {
        Ok(_) => StatusCode::NO_CONTENT,
        Err(_) => StatusCode::NOT_FOUND,
    }
}

/// Create the router with all endpoints
pub fn create_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/config", get(get_config))
        .route("/v1/namespaces", get(list_namespaces).post(create_namespace))
        .route("/v1/namespaces/{ns}/tables", get(list_tables).post(create_table))
        .route("/v1/namespaces/{ns}/tables/{table}", get(get_table).delete(drop_table))
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .layer(middleware::from_fn(auth_middleware))
        .with_state(state)
}

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let state = Arc::new(AppState {
        catalog: InMemoryCatalog::new("supertable"),
    });

    let app = create_router(state);

    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    tracing::info!("SuperTable REST Catalog listening on {}", addr);
    tracing::info!("Swagger UI available at http://{}/swagger-ui/", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
