//! # SuperTable High-Level Table API
//!
//! This module provides the `Table` struct, which is the primary entry point
//! for interacting with SuperTable. It combines metadata management, storage access,
//! and transactional consistency.

use crate::catalog::{Catalog, TableIdentifier};
use crate::metadata::TableMetadata;
use crate::storage::Storage;
use crate::transaction::Transaction;
use std::sync::Arc;

/// A high-level representation of a SuperTable.
#[allow(unused)]
#[derive(Clone)]
pub struct Table {
    pub(crate) identifier: String,
    pub(crate) catalog: Arc<dyn Catalog>,
    pub(crate) metadata: TableMetadata,
    pub(crate) storage: Storage,
}

impl std::fmt::Debug for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Table")
            .field("identifier", &self.identifier)
            .field("metadata", &self.metadata)
            .finish()
    }
}

impl Table {
    /// Creates a new table instance.
    pub fn new(
        identifier: impl Into<String>,
        catalog: Arc<dyn Catalog>,
        metadata: TableMetadata,
        storage: Storage,
    ) -> Self {
        Self {
            identifier: identifier.into(),
            catalog,
            metadata,
            storage,
        }
    }

    /// Returns the table metadata.
    pub fn metadata(&self) -> &TableMetadata {
        &self.metadata
    }

    /// Returns the table storage.
    pub fn storage(&self) -> &Storage {
        &self.storage
    }

    /// Returns the table identifier.
    pub fn identifier(&self) -> &str {
        &self.identifier
    }

    /// Starts a new transaction on this table.
    pub fn new_transaction(&self) -> Transaction {
        Transaction::new(self.metadata.clone())
    }

    /// Returns a view of the table at a specific snapshot.
    pub fn at_snapshot(&self, snapshot_id: i64) -> anyhow::Result<Table> {
        let snapshot = self
            .metadata
            .snapshot(snapshot_id)
            .ok_or_else(|| anyhow::anyhow!("Snapshot {} not found", snapshot_id))?;

        let mut metadata = self.metadata.clone();
        metadata.current_snapshot_id = Some(snapshot.snapshot_id);

        Ok(Table::new(
            self.identifier.clone(),
            self.catalog.clone(),
            metadata,
            self.storage.clone(),
        ))
    }

    /// Returns a view of the table as of a specific timestamp.
    pub fn as_of(&self, timestamp_ms: i64) -> anyhow::Result<Table> {
        let snapshot = self
            .metadata
            .snapshot_at(timestamp_ms)
            .ok_or_else(|| anyhow::anyhow!("No snapshot found at or before {}", timestamp_ms))?;

        self.at_snapshot(snapshot.snapshot_id)
    }

    /// Refreshes the table metadata from the catalog.
    pub async fn refresh(&mut self) -> anyhow::Result<()> {
        let identifier = TableIdentifier::parse(&self.identifier);
        let new_metadata = self.catalog.load_table(&identifier).await?;
        self.metadata = new_metadata;
        Ok(())
    }

    /// Creates a new scan planner for this table's current snapshot.
    pub fn new_scan(&self) -> crate::scan::ScanPlanner<'_> {
        let snapshot = self
            .metadata
            .current_snapshot()
            .expect("Table must have a snapshot to scan");
        crate::scan::ScanPlanner::new(snapshot, &self.storage)
    }

    // TODO: Implement high-level refresh(), commit(), etc.
}
