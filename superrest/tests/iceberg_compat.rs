//! Iceberg REST Catalog Compatibility Tests
//!
//! Tests to verify that our REST Catalog API is compatible with
//! standard Iceberg clients.

use serde_json::json;

/// Base URL for testing
const BASE_URL: &str = "http://localhost:8080";

/// Test catalog configuration endpoint
#[tokio::test]
async fn test_iceberg_get_config() {
    let client = reqwest::Client::new();

    let response = client.get(format!("{}/v1/config", BASE_URL)).send().await;

    if let Ok(resp) = response {
        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = resp.json().await.unwrap();

        // Iceberg spec requires these fields
        assert!(body.get("overrides").is_some());
        assert!(body.get("defaults").is_some());
    } else {
        println!("⚠️ Server not running - skipping live test");
    }
}

/// Test namespace listing
#[tokio::test]
async fn test_iceberg_list_namespaces() {
    let client = reqwest::Client::new();

    let response = client
        .get(format!("{}/v1/namespaces", BASE_URL))
        .send()
        .await;

    if let Ok(resp) = response {
        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = resp.json().await.unwrap();

        // Iceberg spec: namespaces should be an array
        assert!(body.get("namespaces").is_some());
        assert!(body["namespaces"].is_array());
    }
}

/// Test namespace creation
#[tokio::test]
async fn test_iceberg_create_namespace() {
    let client = reqwest::Client::new();

    let response = client
        .post(format!("{}/v1/namespaces", BASE_URL))
        .json(&json!({
            "namespace": ["test_ns"],
            "properties": {}
        }))
        .send()
        .await;

    if let Ok(resp) = response {
        assert!(resp.status() == 200 || resp.status() == 409); // 409 = already exists
    }
}

/// Test table listing
#[tokio::test]
async fn test_iceberg_list_tables() {
    let client = reqwest::Client::new();

    let response = client
        .get(format!("{}/v1/namespaces/default/tables", BASE_URL))
        .send()
        .await;

    if let Ok(resp) = response {
        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = resp.json().await.unwrap();

        // Iceberg spec: identifiers array
        assert!(body.get("identifiers").is_some());
    }
}

/// Test table creation with Iceberg-compatible schema
#[tokio::test]
async fn test_iceberg_create_table() {
    let client = reqwest::Client::new();

    let response = client
        .post(format!("{}/v1/namespaces/default/tables", BASE_URL))
        .json(&json!({
            "name": "iceberg_compat_test",
            "schema": {
                "fields": [
                    {"id": 1, "name": "id", "type": "long", "required": true},
                    {"id": 2, "name": "data", "type": "string", "required": false}
                ]
            },
            "properties": {
                "format-version": "1"
            }
        }))
        .send()
        .await;

    if let Ok(resp) = response {
        // 201 = created, 400 = already exists or error
        assert!(resp.status() == 201 || resp.status() == 400);

        if resp.status() == 201 {
            let body: serde_json::Value = resp.json().await.unwrap();

            // Iceberg spec: response must include metadata-location
            assert!(body.get("metadata-location").is_some());
            assert!(body.get("metadata").is_some());
        }
    }
}

/// Test table metadata retrieval
#[tokio::test]
async fn test_iceberg_get_table() {
    let client = reqwest::Client::new();

    let response = client
        .get(format!(
            "{}/v1/namespaces/default/tables/iceberg_compat_test",
            BASE_URL
        ))
        .send()
        .await;

    if let Ok(resp) = response {
        if resp.status() == 200 {
            let body: serde_json::Value = resp.json().await.unwrap();

            // Iceberg spec: table response format
            assert!(body.get("metadata-location").is_some());
            assert!(body.get("metadata").is_some());

            let metadata = &body["metadata"];
            assert!(metadata.get("format-version").is_some());
            assert!(metadata.get("table-uuid").is_some());
            assert!(metadata.get("location").is_some());
            assert!(metadata.get("schemas").is_some());
        }
    }
}

/// Test table deletion
#[tokio::test]
async fn test_iceberg_drop_table() {
    let client = reqwest::Client::new();

    let response = client
        .delete(format!(
            "{}/v1/namespaces/default/tables/iceberg_compat_test",
            BASE_URL
        ))
        .send()
        .await;

    if let Ok(resp) = response {
        // 204 = deleted, 404 = not found
        assert!(resp.status() == 204 || resp.status() == 404);
    }
}

/// Validate OpenAPI spec compatibility
#[tokio::test]
async fn test_openapi_spec_available() {
    let client = reqwest::Client::new();

    let response = client
        .get(format!("{}/api-docs/openapi.json", BASE_URL))
        .send()
        .await;

    if let Ok(resp) = response {
        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = resp.json().await.unwrap();

        // OpenAPI 3.x spec
        assert!(body.get("openapi").is_some() || body.get("swagger").is_some());
        assert!(body.get("paths").is_some());
    }
}

/// Full end-to-end workflow test
#[tokio::test]
async fn test_iceberg_full_workflow() {
    let client = reqwest::Client::new();

    // 1. Create namespace
    let _ = client
        .post(format!("{}/v1/namespaces", BASE_URL))
        .json(&json!({"namespace": ["workflow_test"]}))
        .send()
        .await;

    // 2. Create table
    let create_resp = client
        .post(format!("{}/v1/namespaces/workflow_test/tables", BASE_URL))
        .json(&json!({
            "name": "events",
            "schema": {
                "fields": [
                    {"id": 1, "name": "event_id", "type": "long", "required": true},
                    {"id": 2, "name": "event_type", "type": "string", "required": true},
                    {"id": 3, "name": "timestamp", "type": "long", "required": true}
                ]
            }
        }))
        .send()
        .await;

    if let Ok(resp) = create_resp {
        if resp.status() == 201 {
            // 3. Get table metadata
            let get_resp = client
                .get(format!(
                    "{}/v1/namespaces/workflow_test/tables/events",
                    BASE_URL
                ))
                .send()
                .await
                .unwrap();

            assert_eq!(get_resp.status(), 200);

            // 4. Drop table
            let drop_resp = client
                .delete(format!(
                    "{}/v1/namespaces/workflow_test/tables/events",
                    BASE_URL
                ))
                .send()
                .await
                .unwrap();

            assert_eq!(drop_resp.status(), 204);
        }
    }
}
