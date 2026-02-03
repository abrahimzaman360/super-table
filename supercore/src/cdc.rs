//! # SuperTable Change Data Capture (CDC)
//!
//! This module provides tools for tracking and reading incremental changes
//! between table snapshots.

use crate::table::Table;
use anyhow::Result;

/// Operation type for a change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeOperation {
    Insert,
    Delete,
    Update,
}

/// A single row-level change.
#[derive(Debug, Clone)]
pub struct RowChange {
    pub operation: ChangeOperation,
    pub file_path: String,
    pub snapshot_id: i64,
}

/// A checkpoint for watermarking incremental progress.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Checkpoint {
    pub last_snapshot_id: i64,
    pub last_sequence_number: i64,
}

/// Change Data Capture engine.
pub struct ChangeDataCapture {
    table: Table,
}

impl ChangeDataCapture {
    pub fn new(table: Table) -> Self {
        Self { table }
    }

    /// Returns changes between two snapshots.
    pub async fn changes_between(
        &self,
        from_snapshot_id: i64,
        to_snapshot_id: i64,
    ) -> Result<Vec<RowChange>> {
        let from_snapshot = self
            .table
            .metadata
            .snapshot(from_snapshot_id)
            .ok_or_else(|| anyhow::anyhow!("Source snapshot not found: {}", from_snapshot_id))?;
        let to_snapshot = self
            .table
            .metadata
            .snapshot(to_snapshot_id)
            .ok_or_else(|| anyhow::anyhow!("Target snapshot not found: {}", to_snapshot_id))?;

        // In a real implementation, we'd compare manifest lists and manifests.
        // For the prototype, we compare the final data file sets.
        let from_files = from_snapshot.all_data_files(&self.table.storage).await?;
        let to_files = to_snapshot.all_data_files(&self.table.storage).await?;

        let mut changes = Vec::new();

        // Files in 'to' but not in 'from' are Inserts
        for file in &to_files {
            if !from_files
                .iter()
                .any(|f: &crate::manifest::DataFile| f.file_path == file.file_path)
            {
                changes.push(RowChange {
                    operation: ChangeOperation::Insert,
                    file_path: file.file_path.clone(),
                    snapshot_id: to_snapshot_id,
                });
            }
        }

        // Files in 'from' but not in 'to' are Deletes
        for file in &from_files {
            if !to_files
                .iter()
                .any(|f: &crate::manifest::DataFile| f.file_path == file.file_path)
            {
                changes.push(RowChange {
                    operation: ChangeOperation::Delete,
                    file_path: file.file_path.clone(),
                    snapshot_id: to_snapshot_id,
                });
            }
        }

        Ok(changes)
    }

    /// Returns changes since a given snapshot up to the current snapshot.
    pub async fn changes_since(&self, snapshot_id: i64) -> Result<Vec<RowChange>> {
        let current_id = self
            .table
            .metadata
            .current_snapshot_id
            .ok_or_else(|| anyhow::anyhow!("No current snapshot"))?;
        self.changes_between(snapshot_id, current_id).await
    }

    /// Saves a checkpoint for incremental reads.
    pub async fn save_checkpoint(&self, checkpoint: &Checkpoint, name: &str) -> Result<()> {
        let path = format!("{}/checkpoints/{}.json", self.table.metadata.location, name);
        let data = serde_json::to_vec(checkpoint)?;
        self.table.storage.write(&path, data.into()).await?;
        Ok(())
    }

    /// Loads a checkpoint for incremental reads.
    pub async fn load_checkpoint(&self, name: &str) -> Result<Checkpoint> {
        let path = format!("{}/checkpoints/{}.json", self.table.metadata.location, name);
        let data = self.table.storage.read(&path).await?;
        let checkpoint = serde_json::from_slice(&data)?;
        Ok(checkpoint)
    }

    /// Returns changes since the last checkpoint.
    pub async fn changes_since_checkpoint(&self, name: &str) -> Result<Vec<RowChange>> {
        let checkpoint = self.load_checkpoint(name).await?;
        self.changes_since(checkpoint.last_snapshot_id).await
    }
}
