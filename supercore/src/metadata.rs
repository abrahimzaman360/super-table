//! # SuperTable Metadata
//!
//! This module defines the core metadata structures for SuperTable, a next-generation
//! open table format. The design is inspired by Apache Iceberg's metadata specification
//! while adding modern enhancements for better concurrency control and auditability.
//!
//! ## Key Concepts
//!
//! - **TableMetadata**: The root metadata object containing all table state
//! - **Snapshot**: An immutable point-in-time view of a table
//! - **Schema**: The table's column definitions with evolution support
//! - **PartitionSpec**: How the table is partitioned (planned)
//!
//! ## Concurrency Model
//!
//! SuperTable uses optimistic concurrency control (OCC) with compare-and-swap (CAS)
//! semantics. Each metadata update increments the `last_sequence_number`, enabling
//! conflict detection during concurrent writes.

use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

use crate::DataFile;
use crate::manifest::{Operation, Snapshot};
use crate::partition::PartitionSpec;
use crate::schema::Schema;

/// The format version of SuperTable metadata.
/// Increment this when making breaking changes to the metadata format.
pub const FORMAT_VERSION: i32 = 1;

/// The root metadata object for a SuperTable table.
///
/// This struct contains all information needed to understand the current state
/// of a table, including its schema history, partition specs, snapshots, and
/// properties.
///
/// # Concurrency
///
/// The `last_sequence_number` field is used for optimistic concurrency control.
/// Each operation that modifies the table increments this counter, allowing
/// the catalog to detect and reject conflicting concurrent updates.
///
/// # Example
///
/// ```rust,ignore
/// use supercore::metadata::TableMetadata;
/// use supercore::schema::{Schema, Field, Type};
///
/// let schema = Schema {
///     schema_id: 0,
///     fields: vec![
///         Field { id: 1, name: "id".into(), required: true, field_type: Type::Long },
///         Field { id: 2, name: "data".into(), required: false, field_type: Type::String },
///     ],
/// };
///
/// let metadata = TableMetadata::builder("s3://bucket/my_table", schema)
///     .with_property("write.format.default", "parquet")
///     .build();
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct TableMetadata {
    /// The unique identifier for this table (UUID v4).
    pub table_uuid: Uuid,

    /// The format version of this metadata file.
    /// Used for forward/backward compatibility checks.
    pub format_version: i32,

    /// The base location of the table (e.g., `s3://bucket/table`).
    /// All data and metadata files are stored relative to this location.
    pub location: String,

    /// Monotonically increasing sequence number for optimistic concurrency.
    /// Incremented on each metadata update operation.
    pub last_sequence_number: i64,

    /// Timestamp of the last update in milliseconds since epoch (UTC).
    pub last_updated_ms: i64,

    /// The highest assigned column ID across all schemas.
    /// Used to ensure new columns get unique IDs during schema evolution.
    pub last_column_id: i32,

    /// The ID of the current (active) schema.
    pub current_schema_id: i32,

    /// List of all schemas, forming the schema history.
    /// Schemas are immutable once added.
    #[serde(default)]
    pub schemas: Vec<Schema>,

    /// The ID of the default partition spec.
    #[serde(default)]
    pub default_spec_id: i32,

    /// List of partition specs.
    #[serde(default)]
    pub partition_specs: Vec<PartitionSpec>,

    /// The ID of the current snapshot, or `None` if the table is empty.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_snapshot_id: Option<i64>,

    /// List of all snapshots, forming the table's history.
    #[serde(default)]
    pub snapshots: Vec<Snapshot>,

    /// Log of snapshot changes for auditing and time travel.
    #[serde(default)]
    pub snapshot_log: Vec<SnapshotLogEntry>,

    /// User-defined table properties.
    /// Common properties include write format, compression, etc.
    #[serde(default)]
    pub properties: HashMap<String, String>,

    /// Aggregate table metrics.
    #[serde(default)]
    pub metrics: TableMetrics,
}

/// Aggregate metrics for a table.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub struct TableMetrics {
    pub total_records: i64,
    pub total_files: i64,
    pub total_size_bytes: i64,
}

/// A log entry recording a snapshot change.
///
/// This provides an audit trail of all operations that modified the table,
/// enabling debugging, compliance, and time-travel queries.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct SnapshotLogEntry {
    /// The ID of the snapshot that was added.
    pub snapshot_id: i64,

    /// Timestamp when this snapshot became current (ms since epoch).
    pub timestamp_ms: i64,

    /// The operation that created this snapshot change.
    pub operation: Operation,
}

/// Builder for constructing `TableMetadata` instances.
///
/// Provides a fluent API for creating new tables with sensible defaults.
pub struct TableMetadataBuilder {
    location: String,
    schema: Schema,
    partition_spec: Option<PartitionSpec>,
    properties: HashMap<String, String>,
}

impl TableMetadataBuilder {
    /// Creates a new builder with the required location and schema.
    pub fn new(location: impl Into<String>, schema: Schema) -> Self {
        Self {
            location: location.into(),
            schema,
            partition_spec: None,
            properties: HashMap::new(),
        }
    }

    /// Sets the partition spec.
    pub fn with_partition_spec(mut self, spec: PartitionSpec) -> Self {
        self.partition_spec = Some(spec);
        self
    }

    /// Adds a property to the table.
    pub fn with_property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }

    /// Adds multiple properties to the table.
    pub fn with_properties(mut self, props: impl IntoIterator<Item = (String, String)>) -> Self {
        self.properties.extend(props);
        self
    }

    /// Builds the `TableMetadata` instance.
    pub fn build(self) -> TableMetadata {
        let now = Utc::now();
        let last_column_id = self.schema.fields.iter().map(|f| f.id).max().unwrap_or(0);

        let mut partition_specs = Vec::new();
        if let Some(spec) = self.partition_spec {
            partition_specs.push(spec);
        }

        TableMetadata {
            table_uuid: Uuid::new_v4(),
            format_version: FORMAT_VERSION,
            location: self.location,
            last_sequence_number: 0,
            last_updated_ms: now.timestamp_millis(),
            last_column_id,
            current_schema_id: self.schema.schema_id,
            schemas: vec![self.schema],
            default_spec_id: 0,
            partition_specs,
            current_snapshot_id: None,
            snapshots: Vec::new(),
            snapshot_log: Vec::new(),
            properties: self.properties,
            metrics: TableMetrics::default(),
        }
    }
}

impl TableMetadata {
    /// Creates a new `TableMetadataBuilder`.
    pub fn builder(location: impl Into<String>, schema: Schema) -> TableMetadataBuilder {
        TableMetadataBuilder::new(location, schema)
    }

    /// Returns the current schema.
    ///
    /// # Panics
    ///
    /// Panics if the schema list is empty or the current schema ID is invalid.
    /// This should never happen with properly constructed metadata.
    pub fn current_schema(&self) -> &Schema {
        self.schemas
            .iter()
            .find(|s| s.schema_id == self.current_schema_id)
            .expect("current schema must exist")
    }

    /// Returns the current snapshot, if any.
    pub fn current_snapshot(&self) -> Option<&Snapshot> {
        self.current_snapshot_id
            .and_then(|id| self.snapshots.iter().find(|s| s.snapshot_id == id))
    }

    /// Returns a snapshot by its ID.
    pub fn snapshot(&self, snapshot_id: i64) -> Option<&Snapshot> {
        self.snapshots.iter().find(|s| s.snapshot_id == snapshot_id)
    }

    /// Returns the snapshot that was current at the given timestamp.
    ///
    /// This enables time-travel queries by finding the most recent snapshot
    /// that was committed before the specified timestamp.
    pub fn snapshot_at(&self, timestamp_ms: i64) -> Option<&Snapshot> {
        // Find the most recent snapshot log entry at or before the timestamp
        let entry = self
            .snapshot_log
            .iter()
            .rev()
            .find(|e| e.timestamp_ms <= timestamp_ms)?;

        self.snapshot(entry.snapshot_id)
    }

    /// Returns the schema with the given ID.
    pub fn schema(&self, schema_id: i32) -> Option<&Schema> {
        self.schemas.iter().find(|s| s.schema_id == schema_id)
    }

    /// Returns the current partition spec.
    pub fn current_partition_spec(&self) -> Option<&PartitionSpec> {
        if self.partition_specs.is_empty() {
            return None;
        }
        self.partition_specs
            .iter()
            .find(|s| s.spec_id == self.default_spec_id)
    }

    /// Adds a new partition spec and sets it as the default.
    pub fn add_partition_spec(&mut self, mut spec: PartitionSpec) {
        // Find next spec ID
        let next_id = self
            .partition_specs
            .iter()
            .map(|s| s.spec_id)
            .max()
            .unwrap_or(-1)
            + 1;
        spec.spec_id = next_id;

        self.default_spec_id = next_id;
        self.partition_specs.push(spec);
        self.increment_sequence();
    }

    /// Increments the sequence number and updates the timestamp.
    ///
    /// This should be called before committing any metadata update.
    pub fn increment_sequence(&mut self) {
        self.last_sequence_number += 1;
        self.last_updated_ms = Utc::now().timestamp_millis();
    }

    /// Adds a new snapshot to the table and makes it current.
    ///
    /// This also creates a snapshot log entry for auditing.
    pub fn add_snapshot(&mut self, snapshot: Snapshot) {
        let timestamp_ms = Utc::now().timestamp_millis();
        let snapshot_id = snapshot.snapshot_id;
        let operation = snapshot.operation;

        self.snapshots.push(snapshot);
        self.current_snapshot_id = Some(snapshot_id);
        self.snapshot_log.push(SnapshotLogEntry {
            snapshot_id,
            timestamp_ms,
            operation,
        });
        self.increment_sequence();
    }

    /// Adds a new schema to the table.
    ///
    /// The schema ID must be unique. Use `next_schema_id()` to generate one.
    pub fn add_schema(&mut self, schema: Schema) {
        // Update last_column_id if needed
        if let Some(max_id) = schema.fields.iter().map(|f| f.id).max() {
            self.last_column_id = self.last_column_id.max(max_id);
        }
        self.schemas.push(schema);
    }

    /// Returns the next available schema ID.
    pub fn next_schema_id(&self) -> i32 {
        self.schemas.iter().map(|s| s.schema_id).max().unwrap_or(-1) + 1
    }

    /// Returns the next available column ID.
    pub fn next_column_id(&self) -> i32 {
        self.last_column_id + 1
    }

    /// Returns the next available snapshot ID.
    pub fn next_snapshot_id(&self) -> i64 {
        self.snapshots
            .iter()
            .map(|s| s.snapshot_id)
            .max()
            .unwrap_or(0)
            + 1
    }

    /// Sets the current schema by ID.
    ///
    /// # Errors
    ///
    /// Returns an error if the schema ID doesn't exist.
    pub fn set_current_schema(&mut self, schema_id: i32) -> Result<(), MetadataError> {
        if self.schemas.iter().any(|s| s.schema_id == schema_id) {
            self.current_schema_id = schema_id;
            self.increment_sequence();
            Ok(())
        } else {
            Err(MetadataError::SchemaNotFound(schema_id))
        }
    }

    /// Rolls back the table to a previous snapshot.
    ///
    /// This creates a new snapshot log entry pointing to the old snapshot,
    /// effectively making it current again without removing history.
    ///
    /// # Errors
    ///
    /// Returns an error if the snapshot ID doesn't exist.
    pub fn rollback_to(&mut self, snapshot_id: i64) -> Result<(), MetadataError> {
        if self.snapshots.iter().any(|s| s.snapshot_id == snapshot_id) {
            self.current_snapshot_id = Some(snapshot_id);
            self.snapshot_log.push(SnapshotLogEntry {
                snapshot_id,
                timestamp_ms: Utc::now().timestamp_millis(),
                operation: Operation::Replace,
            });
            self.increment_sequence();
            Ok(())
        } else {
            Err(MetadataError::SnapshotNotFound(snapshot_id))
        }
    }

    /// Updates the aggregate metrics based on added and deleted files.
    pub fn update_metrics(&mut self, added: &[DataFile], deleted_paths: &HashSet<String>) {
        // This is a simplified incremental update.
        // In a real system, we might want to re-scan periodically to ensure accuracy.
        for file in added {
            self.metrics.total_records += file.record_count;
            self.metrics.total_files += 1;
            self.metrics.total_size_bytes += file.file_size_in_bytes;
        }

        // Deletion metrics are harder because we'd need to know the size/count of the deleted files.
        // For this prototype, we'll assume the caller doesn't need perfect deletion tracking
        // or we'd need to look up the old file metadata.
        // Let's just decrement file count at minimum.
        self.metrics.total_files -= deleted_paths.len() as i64;
    }
}

/// Errors that can occur during metadata operations.
#[derive(Debug, Clone, thiserror::Error)]
pub enum MetadataError {
    /// The requested schema was not found.
    #[error("schema not found: {0}")]
    SchemaNotFound(i32),

    /// The requested snapshot was not found.
    #[error("snapshot not found: {0}")]
    SnapshotNotFound(i64),

    /// A conflict occurred during a concurrent update.
    #[error("conflict: expected sequence {expected}, found {actual}")]
    ConflictError { expected: i64, actual: i64 },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Type;

    fn sample_schema() -> Schema {
        Schema::builder(0)
            .with_field(1, "id", Type::Long, true)
            .with_field(2, "data", Type::String, false)
            .build()
    }

    #[test]
    fn test_builder_creates_valid_metadata() {
        let schema = sample_schema();
        let metadata = TableMetadata::builder("s3://bucket/table", schema)
            .with_property("owner", "test")
            .build();

        assert_eq!(metadata.format_version, FORMAT_VERSION);
        assert_eq!(metadata.location, "s3://bucket/table");
        assert_eq!(metadata.last_sequence_number, 0);
        assert_eq!(metadata.current_schema_id, 0);
        assert_eq!(metadata.properties.get("owner"), Some(&"test".to_string()));
        assert!(metadata.current_snapshot_id.is_none());
    }

    #[test]
    fn test_increment_sequence() {
        let schema = sample_schema();
        let mut metadata = TableMetadata::builder("s3://bucket/table", schema).build();

        let initial_seq = metadata.last_sequence_number;
        metadata.increment_sequence();

        assert_eq!(metadata.last_sequence_number, initial_seq + 1);
    }

    #[test]
    fn test_rollback_to_nonexistent_snapshot() {
        let schema = sample_schema();
        let mut metadata = TableMetadata::builder("s3://bucket/table", schema).build();

        let result = metadata.rollback_to(999);
        assert!(matches!(result, Err(MetadataError::SnapshotNotFound(999))));
    }
}
