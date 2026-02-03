//! # SuperTable Transactions
//!
//! This module implements ACID transactions with optimistic concurrency control (OCC).
//! Transactions provide atomicity and isolation for table operations, enabling
//! multiple writers to work concurrently without conflicts.
//!
//! ## Optimistic Concurrency Control
//!
//! SuperTable uses OCC rather than pessimistic locking, which is better suited
//! for data lake workloads where:
//! - Reads vastly outnumber writes
//! - Writes typically occur in batches (not high frequency)
//! - Lock contention would be impractical across distributed systems
//!
//! ## Conflict Detection
//!
//! Conflicts are detected at commit time by comparing the transaction's base
//! sequence number with the current table state. If another writer committed
//! changes that overlap with this transaction's write set, the commit fails.
//!
//! ## Retry Strategy
//!
//! When a conflict is detected, the transaction can optionally retry with
//! configurable exponential backoff. The transaction re-reads the current
//! state, re-applies changes, and attempts to commit again.
//!
//! # Example
//!
//! ```rust,ignore
//! use supercore::transaction::Transaction;
//!
//! let tx = Transaction::new(table_metadata);
//! tx.add_data_files(vec![data_file]);
//! let result = tx.commit(&catalog).await?;
//! ```

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

// use async_trait::async_trait;
use thiserror::Error;

use crate::catalog::{Catalog, TableIdentifier};
use crate::manifest::{DataFile, Operation, Snapshot};
use crate::metadata::{MetadataError, TableMetadata};
/// Errors that can occur during transaction operations.
#[derive(Debug, Error)]
pub enum TransactionError {
    /// A conflict occurred - another transaction committed first.
    #[error(
        "commit conflict: base sequence {base_sequence} is stale, current is {current_sequence}"
    )]
    CommitConflict {
        base_sequence: i64,
        current_sequence: i64,
    },

    /// Maximum retry attempts exceeded.
    #[error("max retry attempts ({attempts}) exceeded")]
    MaxRetriesExceeded { attempts: u32 },

    /// The transaction was read-only (no changes to commit).
    #[error("transaction has no changes to commit")]
    NoChanges,

    /// A metadata error occurred.
    #[error("metadata error: {0}")]
    Metadata(#[from] MetadataError),

    /// An I/O error occurred.
    #[error("io error: {0}")]
    Io(String),

    /// A validation error occurred.
    #[error("validation error: {0}")]
    Validation(String),

    /// A generic commit failure.
    #[error("commit failed: {0}")]
    CommitFailed(String),
}

/// Result type for transaction operations.
pub type TransactionResult<T> = Result<T, TransactionError>;

/// Isolation level for transactions.
///
/// SuperTable currently supports snapshot isolation, which provides
/// a consistent view of the table as of the transaction start time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IsolationLevel {
    /// Each read sees the most recent committed data.
    /// This is the default and provides the best consistency.
    #[default]
    SnapshotIsolation,

    /// Reads can see uncommitted data from other transactions.
    /// Faster but less consistent. Use with caution.
    ReadUncommitted,
}

/// Configuration for transaction retry behavior.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts.
    pub max_attempts: u32,

    /// Initial delay between retries.
    pub initial_delay: Duration,

    /// Maximum delay between retries.
    pub max_delay: Duration,

    /// Multiplier for exponential backoff.
    pub backoff_multiplier: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            backoff_multiplier: 2.0,
        }
    }
}

impl RetryConfig {
    /// Creates a new config with no retries.
    pub fn no_retry() -> Self {
        Self {
            max_attempts: 1,
            ..Default::default()
        }
    }

    /// Calculates the delay for a given attempt number.
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        if attempt == 0 {
            return Duration::ZERO;
        }

        let delay_ms = self.initial_delay.as_millis() as f64
            * self.backoff_multiplier.powi(attempt as i32 - 1);

        Duration::from_millis(delay_ms.min(self.max_delay.as_millis() as f64) as u64)
    }
}

/// A transaction represents a unit of work against a table.
///
/// Transactions track:
/// - The base state (snapshot) at transaction start
/// - Files to be added
/// - Files to be deleted
/// - Schema changes (if any)
///
/// At commit time, the transaction validates that no conflicting changes
/// have been made and atomically updates the table metadata.
#[derive(Debug)]
pub struct Transaction {
    /// The table location.
    table_location: String,

    /// Base metadata at transaction start.
    base_metadata: TableMetadata,

    /// Sequence number at transaction start.
    base_sequence_number: i64,

    /// Files to be added.
    added_files: Vec<DataFile>,

    /// Paths of files to be deleted.
    deleted_file_paths: HashSet<String>,

    /// The operation type for this transaction.
    operation: Operation,

    /// Transaction isolation level.
    isolation_level: IsolationLevel,

    /// Retry configuration.
    retry_config: RetryConfig,

    /// Optional new schema to apply.
    new_schema: Option<crate::schema::Schema>,

    /// Custom summary properties.
    summary_properties: std::collections::HashMap<String, String>,
}

impl Transaction {
    /// Creates a new transaction based on the current table state.
    pub fn new(metadata: TableMetadata) -> Self {
        let base_sequence = metadata.last_sequence_number;
        let location = metadata.location.clone();

        Self {
            table_location: location,
            base_metadata: metadata,
            base_sequence_number: base_sequence,
            added_files: Vec::new(),
            deleted_file_paths: HashSet::new(),
            operation: Operation::Append,
            isolation_level: IsolationLevel::default(),
            retry_config: RetryConfig::default(),
            new_schema: None,
            summary_properties: std::collections::HashMap::new(),
        }
    }

    /// Sets the operation type.
    pub fn with_operation(mut self, operation: Operation) -> Self {
        self.operation = operation;
        self
    }

    /// Sets the isolation level.
    pub fn with_isolation_level(mut self, level: IsolationLevel) -> Self {
        self.isolation_level = level;
        self
    }

    /// Sets the retry configuration.
    pub fn with_retry_config(mut self, config: RetryConfig) -> Self {
        self.retry_config = config;
        self
    }

    /// Adds data files to be committed.
    pub fn add_files(&mut self, files: impl IntoIterator<Item = DataFile>) {
        self.added_files.extend(files);
    }

    /// Adds a single data file to be committed.
    pub fn add_file(&mut self, file: DataFile) {
        self.added_files.push(file);
    }

    /// Marks files for deletion.
    pub fn delete_files(&mut self, paths: impl IntoIterator<Item = String>) {
        self.deleted_file_paths.extend(paths);
    }

    /// Marks a single file for deletion.
    pub fn delete_file(&mut self, path: impl Into<String>) {
        self.deleted_file_paths.insert(path.into());
    }

    /// Sets a new schema to be applied.
    pub fn set_schema(&mut self, schema: crate::schema::Schema) {
        self.new_schema = Some(schema);
    }

    /// Adds a custom summary property.
    pub fn with_summary_property(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.summary_properties.insert(key.into(), value.into());
        self
    }

    /// Returns true if this transaction has changes to commit.
    pub fn has_changes(&self) -> bool {
        !self.added_files.is_empty()
            || !self.deleted_file_paths.is_empty()
            || self.new_schema.is_some()
    }

    /// Returns the number of files to be added.
    pub fn added_files_count(&self) -> usize {
        self.added_files.len()
    }

    /// Returns the number of files to be deleted.
    pub fn deleted_files_count(&self) -> usize {
        self.deleted_file_paths.len()
    }

    /// Returns the total records to be added.
    pub fn added_records_count(&self) -> i64 {
        self.added_files.iter().map(|f| f.record_count).sum()
    }

    /// Commits the transaction to the catalog.
    #[tracing::instrument(skip(self, catalog), fields(operation = ?self.operation))]
    pub async fn commit(mut self, catalog: Arc<dyn Catalog>) -> TransactionResult<Snapshot> {
        if !self.has_changes() {
            return Err(TransactionError::NoChanges);
        }

        let identifier = TableIdentifier::parse(&self.table_location);
        // Error handling simplified here - parse should probably return a Result

        let mut attempts = 0;
        loop {
            attempts += 1;

            let current_metadata = catalog
                .load_table(&identifier)
                .await
                .map_err(|e| TransactionError::Io(e.to_string()))?;

            // Detect conflicts
            self.detect_conflicts(&current_metadata)?;

            // Prepare the commit
            let prepared_commit = self.prepare_commit()?;

            // Attempt to commit
            match catalog
                .commit_table(
                    &identifier,
                    prepared_commit.base_sequence,
                    prepared_commit.new_metadata,
                )
                .await
            {
                Ok(committed_metadata) => {
                    // Commit successful, return the new snapshot
                    return committed_metadata.current_snapshot().cloned().ok_or(
                        TransactionError::CommitFailed("No current snapshot found".to_string()),
                    );
                }
                Err(e) => {
                    // In a real catalog, we'd check if e is a conflict.
                    // For now we assume if it fails it might be a conflict if it's not the first attempt.
                    if attempts >= self.retry_config.max_attempts {
                        return Err(TransactionError::Io(e.to_string()));
                    }

                    tracing::warn!(
                        "Commit might have failed or conflicted. Retrying (attempt {}/{})",
                        attempts,
                        self.retry_config.max_attempts
                    );
                    tokio::time::sleep(self.retry_config.delay_for_attempt(attempts)).await;
                    // Reload base metadata for next attempt
                    self.base_metadata = catalog
                        .load_table(&identifier)
                        .await
                        .map_err(|e| TransactionError::Io(e.to_string()))?;
                    self.base_sequence_number = self.base_metadata.last_sequence_number;
                }
            }
        }
    }

    /// Validates the transaction state before commit.
    fn validate(&self) -> TransactionResult<()> {
        if !self.has_changes() {
            return Err(TransactionError::NoChanges);
        }

        // Validate schema compatibility if changing schema
        if let Some(ref new_schema) = self.new_schema {
            let current_schema = self.base_metadata.current_schema();
            let validator =
                crate::validation::SchemaCompatibilityValidator::new(current_schema.clone());
            validator.validate(new_schema).map_err(|e| {
                TransactionError::Validation(format!("Schema evolution error: {}", e))
            })?;
        }

        // Validates that deleted files exist in the current snapshot
        // (This would require loading manifest files - simplified here)

        Ok(())
    }

    /// Detects conflicts between this transaction and updates that occurred
    /// since the transaction started.
    #[allow(unused)]
    fn detect_conflicts(&self, current_metadata: &TableMetadata) -> TransactionResult<()> {
        if current_metadata.last_sequence_number != self.base_sequence_number {
            // For now, we use a simple strategy: any concurrent modification is a conflict.
            // A more sophisticated approach would check:
            // 1. Partition overlap for append operations
            // 2. File overlap for delete operations
            // 3. Schema compatibility for schema changes

            return Err(TransactionError::CommitConflict {
                base_sequence: self.base_sequence_number,
                current_sequence: current_metadata.last_sequence_number,
            });
        }

        Ok(())
    }

    /// Builds the snapshot for this transaction.
    fn build_snapshot(&self, sequence_number: i64, manifest_list_path: &str) -> Snapshot {
        let added_records: i64 = self.added_files.iter().map(|f| f.record_count).sum();
        let added_bytes: i64 = self.added_files.iter().map(|f| f.file_size_in_bytes).sum();

        let mut builder = Snapshot::builder(sequence_number, manifest_list_path)
            .with_operation(self.operation)
            .with_sequence_number(sequence_number)
            .with_schema_id(self.base_metadata.current_schema_id)
            .with_summary_property("added-data-files", self.added_files.len().to_string())
            .with_summary_property("added-records", added_records.to_string())
            .with_summary_property("added-files-size", added_bytes.to_string())
            .with_summary_property(
                "deleted-data-files",
                self.deleted_file_paths.len().to_string(),
            );

        // Add custom summary properties
        for (key, value) in &self.summary_properties {
            builder = builder.with_summary_property(key.clone(), value.clone());
        }

        // Set parent snapshot if exists
        if let Some(current_snapshot_id) = self.base_metadata.current_snapshot_id {
            builder = builder.with_parent(current_snapshot_id);
        }

        builder.build()
    }

    /// Prepares the transaction for commit.
    ///
    /// This method:
    /// 1. Validates the transaction
    /// 2. Builds manifest files for new data
    /// 3. Creates the new snapshot
    /// 4. Returns the updated metadata ready for atomic commit
    pub fn prepare_commit(&self) -> TransactionResult<PreparedCommit> {
        self.validate()?;

        let new_sequence = self.base_metadata.last_sequence_number + 1;
        let manifest_list_path = format!(
            "{}/metadata/snap-{}-manifest-list.avro",
            self.table_location, new_sequence
        );

        let snapshot = self.build_snapshot(new_sequence, &manifest_list_path);

        let mut new_metadata = self.base_metadata.clone();

        // Apply schema changes if any
        if let Some(ref new_schema) = self.new_schema {
            new_metadata.add_schema(new_schema.clone());
            new_metadata.current_schema_id = new_schema.schema_id;
        }

        // Update table metrics
        new_metadata.update_metrics(&self.added_files, &self.deleted_file_paths);

        // Add the new snapshot
        new_metadata.add_snapshot(snapshot);

        Ok(PreparedCommit {
            base_sequence: self.base_sequence_number,
            new_metadata,
            added_files: self.added_files.clone(),
            deleted_files: self.deleted_file_paths.clone(),
            manifest_list_path,
        })
    }
}

/// A prepared commit ready for atomic application.
#[derive(Debug, Clone)]
pub struct PreparedCommit {
    /// The base sequence number this commit is based on.
    pub base_sequence: i64,

    /// The new metadata to be written.
    pub new_metadata: TableMetadata,

    /// Files added in this commit.
    pub added_files: Vec<DataFile>,

    /// Files deleted in this commit.
    pub deleted_files: HashSet<String>,

    /// Path where the manifest list should be written.
    pub manifest_list_path: String,
}

impl PreparedCommit {
    /// Returns the new snapshot ID.
    pub fn snapshot_id(&self) -> Option<i64> {
        self.new_metadata.current_snapshot_id
    }

    /// Returns the new sequence number.
    pub fn sequence_number(&self) -> i64 {
        self.new_metadata.last_sequence_number
    }
}

// Trait for catalog implementations that support atomic commits.
// (Catalog trait already provides this)

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::FileFormat;
    use crate::schema::{Schema, Type};

    fn sample_metadata() -> TableMetadata {
        let schema = Schema::builder(0)
            .with_field(1, "id", Type::Long, true)
            .with_field(2, "data", Type::String, false)
            .build();

        TableMetadata::builder("s3://bucket/table", schema).build()
    }

    #[test]
    fn test_transaction_creation() {
        let metadata = sample_metadata();
        let tx = Transaction::new(metadata);

        assert!(!tx.has_changes());
        assert_eq!(tx.added_files_count(), 0);
        assert_eq!(tx.deleted_files_count(), 0);
    }

    #[test]
    fn test_add_files() {
        let metadata = sample_metadata();
        let mut tx = Transaction::new(metadata);

        let file = DataFile::new("/data/file.parquet", FileFormat::Parquet, 1000, 1024 * 1024);
        tx.add_file(file);

        assert!(tx.has_changes());
        assert_eq!(tx.added_files_count(), 1);
        assert_eq!(tx.added_records_count(), 1000);
    }

    #[test]
    fn test_delete_files() {
        let metadata = sample_metadata();
        let mut tx = Transaction::new(metadata);

        tx.delete_file("/data/old-file.parquet");
        tx.delete_file("/data/another-file.parquet");

        assert!(tx.has_changes());
        assert_eq!(tx.deleted_files_count(), 2);
    }

    #[test]
    fn test_prepare_commit() {
        let metadata = sample_metadata();
        let mut tx = Transaction::new(metadata);

        let file = DataFile::new("/data/file.parquet", FileFormat::Parquet, 1000, 1024 * 1024);
        tx.add_file(file);

        let prepared = tx.prepare_commit().unwrap();

        assert_eq!(prepared.base_sequence, 0);
        assert_eq!(prepared.sequence_number(), 1);
        assert_eq!(prepared.added_files.len(), 1);
    }

    #[test]
    fn test_empty_transaction_fails() {
        let metadata = sample_metadata();
        let tx = Transaction::new(metadata);

        let result = tx.prepare_commit();
        assert!(matches!(result, Err(TransactionError::NoChanges)));
    }

    #[test]
    fn test_retry_config_delay() {
        let config = RetryConfig::default();

        assert_eq!(config.delay_for_attempt(0), Duration::ZERO);
        assert_eq!(config.delay_for_attempt(1), Duration::from_millis(100));
        assert_eq!(config.delay_for_attempt(2), Duration::from_millis(200));
        assert_eq!(config.delay_for_attempt(3), Duration::from_millis(400));
    }
}
