//! # SuperTable Manifest and Snapshot Management
//!
//! This module defines the structures for tracking table state and file inventory.
//! The design follows Apache Iceberg's snapshot-based model, where each snapshot
//! represents an immutable, point-in-time view of the table.
//!
//! ## Key Concepts
//!
//! - **Snapshot**: A complete view of the table at a point in time
//! - **Manifest List**: A file listing all manifest files for a snapshot
//! - **Manifest File**: A file listing data files with their metadata
//! - **Data File**: An actual Parquet file containing table data
//!
//! ## Immutability
//!
//! Once committed, snapshots are immutable. Updates create new snapshots that
//! reference both new and existing data files. This enables:
//! - Time travel (querying historical states)
//! - Atomic commits (all-or-nothing updates)
//! - Concurrent reads (readers see consistent snapshots)

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// The type of operation that created a snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Operation {
    /// New data was appended to the table.
    Append,
    /// Existing data was replaced (partition overwrite).
    Overwrite,
    /// Some data was deleted.
    Delete,
    /// An existing snapshot was restored.
    Replace,
}

impl std::fmt::Display for Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Operation::Append => write!(f, "append"),
            Operation::Overwrite => write!(f, "overwrite"),
            Operation::Delete => write!(f, "delete"),
            Operation::Replace => write!(f, "replace"),
        }
    }
}

/// A snapshot represents an immutable view of the table at a point in time.
///
/// Each snapshot contains a reference to a manifest list, which in turn
/// references all the data files that make up the table at that point.
///
/// # Lineage
///
/// Snapshots form a tree structure through parent references, allowing
/// tracking of table lineage and enabling features like rollback.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Snapshot {
    /// Unique identifier for this snapshot.
    pub snapshot_id: i64,

    /// The ID of the parent snapshot, if any.
    /// The first snapshot has no parent (None).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_snapshot_id: Option<i64>,

    /// Monotonically increasing sequence number.
    /// Used to order operations across snapshots.
    pub sequence_number: i64,

    /// Timestamp when this snapshot was created (ms since epoch).
    pub timestamp_ms: i64,

    /// The operation that created this snapshot.
    pub operation: Operation,

    /// Path to the manifest list file for this snapshot.
    pub manifest_list: String,

    /// Summary statistics and operation metadata.
    #[serde(default)]
    pub summary: HashMap<String, String>,

    /// The schema ID used for this snapshot's data.
    pub schema_id: i32,
}

impl Snapshot {
    /// Creates a new snapshot builder.
    pub fn builder(snapshot_id: i64, manifest_list: impl Into<String>) -> SnapshotBuilder {
        SnapshotBuilder::new(snapshot_id, manifest_list)
    }

    /// Returns the value of a summary property.
    pub fn summary_property(&self, key: &str) -> Option<&str> {
        self.summary.get(key).map(|s| s.as_str())
    }

    /// Returns the added files count from the summary.
    pub fn added_files_count(&self) -> Option<i64> {
        self.summary_property("added-data-files")
            .and_then(|s| s.parse().ok())
    }

    /// Returns the added records count from the summary.
    pub fn added_records_count(&self) -> Option<i64> {
        self.summary_property("added-records")
            .and_then(|s| s.parse().ok())
    }

    /// Returns the total files count from the summary.
    pub fn total_files_count(&self) -> Option<i64> {
        self.summary_property("total-data-files")
            .and_then(|s| s.parse().ok())
    }

    /// Returns the total records count from the summary.
    pub fn total_records_count(&self) -> Option<i64> {
        self.summary_property("total-records")
            .and_then(|s| s.parse().ok())
    }

    /// Loads all data and delete files for this snapshot.
    ///
    /// Returns a tuple of (data_files, delete_files).
    pub async fn all_files(
        &self,
        storage: &crate::storage::Storage,
    ) -> anyhow::Result<(Vec<DataFile>, Vec<DataFile>)> {
        let manifest_list = ManifestList::load(&self.manifest_list, storage).await?;
        let mut data_files = Vec::new();
        let mut delete_files = Vec::new();

        for entry in manifest_list.entries {
            let manifest = ManifestFile::load(&entry.manifest_path, storage).await?;
            for m_entry in manifest.entries {
                if m_entry.status != ManifestEntryStatus::Deleted {
                    match m_entry.data_file.content {
                        FileContent::Data => data_files.push(m_entry.data_file),
                        FileContent::PositionDeletes | FileContent::EqualityDeletes => {
                            delete_files.push(m_entry.data_file)
                        }
                    }
                }
            }
        }

        Ok((data_files, delete_files))
    }

    /// Loads all data files for this snapshot.
    pub async fn all_data_files(
        &self,
        storage: &crate::storage::Storage,
    ) -> anyhow::Result<Vec<DataFile>> {
        let (data_files, _) = self.all_files(storage).await?;
        Ok(data_files)
    }
}

/// Builder for constructing `Snapshot` instances.
pub struct SnapshotBuilder {
    snapshot_id: i64,
    manifest_list: String,
    parent_snapshot_id: Option<i64>,
    sequence_number: i64,
    timestamp_ms: i64,
    operation: Operation,
    summary: HashMap<String, String>,
    schema_id: i32,
}

impl SnapshotBuilder {
    /// Creates a new builder with required fields.
    pub fn new(snapshot_id: i64, manifest_list: impl Into<String>) -> Self {
        Self {
            snapshot_id,
            manifest_list: manifest_list.into(),
            parent_snapshot_id: None,
            sequence_number: 0,
            timestamp_ms: chrono::Utc::now().timestamp_millis(),
            operation: Operation::Append,
            summary: HashMap::new(),
            schema_id: 0,
        }
    }

    /// Sets the parent snapshot ID.
    pub fn with_parent(mut self, parent_id: i64) -> Self {
        self.parent_snapshot_id = Some(parent_id);
        self
    }

    /// Sets the sequence number.
    pub fn with_sequence_number(mut self, seq: i64) -> Self {
        self.sequence_number = seq;
        self
    }

    /// Sets the operation type.
    pub fn with_operation(mut self, op: Operation) -> Self {
        self.operation = op;
        self
    }

    /// Sets the schema ID.
    pub fn with_schema_id(mut self, schema_id: i32) -> Self {
        self.schema_id = schema_id;
        self
    }

    /// Adds a summary property.
    pub fn with_summary_property(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.summary.insert(key.into(), value.into());
        self
    }

    /// Builds the snapshot.
    pub fn build(self) -> Snapshot {
        Snapshot {
            snapshot_id: self.snapshot_id,
            parent_snapshot_id: self.parent_snapshot_id,
            sequence_number: self.sequence_number,
            timestamp_ms: self.timestamp_ms,
            operation: self.operation,
            manifest_list: self.manifest_list,
            summary: self.summary,
            schema_id: self.schema_id,
        }
    }
}

/// A manifest list contains references to all manifest files for a snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ManifestList {
    /// The list of manifest file entries.
    pub entries: Vec<ManifestFileEntry>,
}

impl ManifestList {
    /// Creates a new empty manifest list.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Adds a manifest file entry.
    pub fn add_entry(&mut self, entry: ManifestFileEntry) {
        self.entries.push(entry);
    }

    /// Returns the total number of data files across all manifests.
    pub fn total_data_files(&self) -> i64 {
        self.entries
            .iter()
            .map(|e| e.added_files_count + e.existing_files_count)
            .sum()
    }

    /// Loads a manifest list from storage.
    pub async fn load(path: &str, storage: &crate::storage::Storage) -> anyhow::Result<Self> {
        let data = storage.read(path).await?;
        Ok(serde_json::from_slice(&data)?)
    }

    /// Saves a manifest list to storage.
    pub async fn save(&self, path: &str, storage: &crate::storage::Storage) -> anyhow::Result<()> {
        let data = serde_json::to_vec(self)?;
        storage.write(path, data.into()).await?;
        Ok(())
    }
}

impl Default for ManifestList {
    fn default() -> Self {
        Self::new()
    }
}

/// An entry in a manifest list, referencing a single manifest file.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ManifestFileEntry {
    /// Path to the manifest file.
    pub manifest_path: String,

    /// Length of the manifest file in bytes.
    pub manifest_length: i64,

    /// The partition spec ID used to write the manifest.
    pub partition_spec_id: i32,

    /// The content type of files in this manifest.
    pub content: ManifestContent,

    /// Sequence number when this manifest was added.
    pub sequence_number: i64,

    /// Minimum sequence number of any file in this manifest.
    pub min_sequence_number: i64,

    /// The snapshot ID that added this manifest.
    pub added_snapshot_id: i64,

    /// Number of files added in this manifest.
    pub added_files_count: i64,

    /// Number of existing (unchanged) files in this manifest.
    pub existing_files_count: i64,

    /// Number of files deleted in this manifest.
    pub deleted_files_count: i64,

    /// Number of rows added across all files in this manifest.
    pub added_rows_count: i64,

    /// Number of existing rows in this manifest.
    pub existing_rows_count: i64,

    /// Number of rows deleted in this manifest.
    pub deleted_rows_count: i64,
}

/// The type of content in a manifest file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ManifestContent {
    /// Data files containing table records.
    Data,
    /// Delete files containing deleted record identifiers.
    Deletes,
}

/// A manifest file contains a list of data files.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ManifestFile {
    /// The schema used to write this manifest.
    pub schema: crate::schema::Schema,

    /// The partition spec ID.
    pub partition_spec_id: i32,

    /// The content type.
    pub content: ManifestContent,

    /// The entries in this manifest.
    pub entries: Vec<ManifestEntry>,
}

impl ManifestFile {
    /// Loads a manifest file from storage.
    pub async fn load(path: &str, storage: &crate::storage::Storage) -> anyhow::Result<Self> {
        let data = storage.read(path).await?;
        Ok(serde_json::from_slice(&data)?)
    }

    /// Saves a manifest file to storage.
    pub async fn save(&self, path: &str, storage: &crate::storage::Storage) -> anyhow::Result<()> {
        let data = serde_json::to_vec(self)?;
        storage.write(path, data.into()).await?;
        Ok(())
    }
}

/// An entry in a manifest file, representing a single data file.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ManifestEntry {
    /// The status of this entry.
    pub status: ManifestEntryStatus,

    /// The snapshot ID that added this file.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<i64>,

    /// The sequence number when this file was added.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sequence_number: Option<i64>,

    /// The data file metadata.
    pub data_file: DataFile,
}

/// The status of a manifest entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ManifestEntryStatus {
    /// The file exists in this snapshot.
    Existing,
    /// The file was added in this snapshot.
    Added,
    /// The file was deleted in this snapshot.
    Deleted,
}

/// Supported data file formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FileFormat {
    /// Apache Parquet format.
    Parquet,
    /// Apache ORC format (planned).
    Orc,
    /// Apache Avro format (planned).
    Avro,
}

/// Type of content stored in a file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum FileContent {
    /// Data files containing table records.
    Data,
    /// Position delete files containing file paths and row positions.
    PositionDeletes,
    /// Equality delete files containing column values for deleted rows.
    EqualityDeletes,
}

/// Metadata about a data or delete file.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct DataFile {
    /// Path to the file.
    pub file_path: String,

    /// The file format.
    pub file_format: FileFormat,

    /// The type of content in this file.
    pub content: FileContent,

    /// The partition data for this file.
    #[serde(default)]
    pub partition: HashMap<String, serde_json::Value>,

    /// Number of records in this file.
    pub record_count: i64,

    /// File size in bytes.
    pub file_size_in_bytes: i64,

    /// Column sizes (column_id -> size in bytes).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub column_sizes: HashMap<i32, i64>,

    /// Value counts (column_id -> non-null value count).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub value_counts: HashMap<i32, i64>,

    /// Null value counts (column_id -> null value count).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub null_value_counts: HashMap<i32, i64>,

    /// NaN value counts (column_id -> NaN count).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub nan_value_counts: HashMap<i32, i64>,

    /// Lower bounds (column_id -> serialized lower bound).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub lower_bounds: HashMap<i32, Vec<u8>>,

    /// Upper bounds (column_id -> serialized upper bound).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub upper_bounds: HashMap<i32, Vec<u8>>,

    /// Column-level statistics.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub statistics: HashMap<i32, crate::statistics::ColumnStats>,

    /// Sort order ID, if the file is sorted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort_order_id: Option<i32>,

    /// Key metadata for encryption.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_metadata: Option<Vec<u8>>,

    /// Split offsets for the file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub split_offsets: Option<Vec<i64>>,

    /// Field IDs used for equality comparison in equality delete files.
    /// This is required for content=EqualityDeletes and should be null otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub equality_ids: Option<Vec<i32>>,
}

impl DataFile {
    /// Creates a new data file with minimal required fields.
    pub fn new(
        file_path: impl Into<String>,
        file_format: FileFormat,
        record_count: i64,
        file_size_in_bytes: i64,
    ) -> Self {
        Self {
            file_path: file_path.into(),
            file_format,
            content: FileContent::Data,
            partition: HashMap::new(),
            record_count,
            file_size_in_bytes,
            column_sizes: HashMap::new(),
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
            nan_value_counts: HashMap::new(),
            lower_bounds: HashMap::new(),
            upper_bounds: HashMap::new(),
            statistics: HashMap::new(),
            sort_order_id: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
        }
    }

    /// Sets the partition data for this file.
    pub fn with_partition(mut self, partition: HashMap<String, serde_json::Value>) -> Self {
        self.partition = partition;
        self
    }

    /// Adds column statistics.
    pub fn with_column_stats(
        mut self,
        column_id: i32,
        size: i64,
        value_count: i64,
        null_count: i64,
    ) -> Self {
        self.column_sizes.insert(column_id, size);
        self.value_counts.insert(column_id, value_count);
        self.null_value_counts.insert(column_id, null_count);
        self
    }

    /// Sets the content type (e.g., Data, PositionDeletes, EqualityDeletes).
    pub fn with_content(mut self, content: FileContent) -> Self {
        self.content = content;
        self
    }

    /// Sets the equality IDs for equality delete files.
    pub fn with_equality_ids(mut self, ids: Vec<i32>) -> Self {
        self.equality_ids = Some(ids);
        self
    }

    /// Sets the key metadata.
    pub fn with_key_metadata(mut self, metadata: Vec<u8>) -> Self {
        self.key_metadata = Some(metadata);
        self
    }

    /// Sets the split offsets.
    pub fn with_split_offsets(mut self, offsets: Vec<i64>) -> Self {
        self.split_offsets = Some(offsets);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_builder() {
        let snapshot = Snapshot::builder(1, "/manifest-list.avro")
            .with_operation(Operation::Append)
            .with_sequence_number(5)
            .with_summary_property("added-data-files", "10")
            .with_summary_property("added-records", "1000")
            .build();

        assert_eq!(snapshot.snapshot_id, 1);
        assert_eq!(snapshot.operation, Operation::Append);
        assert_eq!(snapshot.sequence_number, 5);
        assert_eq!(snapshot.added_files_count(), Some(10));
        assert_eq!(snapshot.added_records_count(), Some(1000));
    }

    #[test]
    fn test_manifest_list() {
        let mut list = ManifestList::new();
        list.add_entry(ManifestFileEntry {
            manifest_path: "/manifest-1.avro".into(),
            manifest_length: 1024,
            partition_spec_id: 0,
            content: ManifestContent::Data,
            sequence_number: 1,
            min_sequence_number: 1,
            added_snapshot_id: 1,
            added_files_count: 5,
            existing_files_count: 0,
            deleted_files_count: 0,
            added_rows_count: 500,
            existing_rows_count: 0,
            deleted_rows_count: 0,
        });

        assert_eq!(list.total_data_files(), 5);
    }

    #[test]
    fn test_data_file() {
        let file = DataFile::new("/data/file.parquet", FileFormat::Parquet, 1000, 1024 * 1024)
            .with_column_stats(1, 512 * 1024, 1000, 0);

        assert_eq!(file.record_count, 1000);
        assert_eq!(file.column_sizes.get(&1), Some(&(512 * 1024)));
    }
}
