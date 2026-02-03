//! # SuperTable Actions
//!
//! This module provides high-level table maintenance actions such as
//! snapshot expiration and orphan file removal.

use crate::manifest::{ManifestEntryStatus, ManifestFile, ManifestList, Snapshot};
use crate::metadata::TableMetadata;
use crate::storage::Storage;
use anyhow::Result;
use chrono::{Duration, Utc};
use std::collections::HashSet;

/// Action to expire old snapshots based on retention policies.
pub struct ExpireSnapshots<'a> {
    metadata: &'a mut TableMetadata,
    min_snapshots_to_keep: usize,
    max_snapshot_age_ms: Option<i64>,
}

impl<'a> ExpireSnapshots<'a> {
    pub fn new(metadata: &'a mut TableMetadata, _storage: &'a Storage) -> Self {
        Self {
            metadata,
            min_snapshots_to_keep: 1,
            max_snapshot_age_ms: None,
        }
    }

    pub fn with_min_snapshots_to_keep(mut self, min: usize) -> Self {
        self.min_snapshots_to_keep = min;
        self
    }

    pub fn with_max_snapshot_age(mut self, duration: Duration) -> Self {
        self.max_snapshot_age_ms = Some(duration.num_milliseconds());
        self
    }

    /// Executes the expiration action.
    ///
    /// Returns the IDs of the expired snapshots.
    pub async fn execute(self) -> Result<Vec<i64>> {
        if self.metadata.snapshots.is_empty() {
            return Ok(Vec::new());
        }

        let now = Utc::now().timestamp_millis();
        let mut expired_ids = Vec::new();

        // Snapshots are usually sorted by ID/Timestamp, but let's be sure
        let mut snapshots = self.metadata.snapshots.clone();
        snapshots.sort_by_key(|s| s.timestamp_ms);

        let current_snapshot_id = self.metadata.current_snapshot_id;

        // We never expire the current snapshot
        let to_expire_candidates: Vec<Snapshot> = snapshots
            .into_iter()
            .filter(|s| Some(s.snapshot_id) != current_snapshot_id)
            .collect();

        let num_to_keep = self
            .min_snapshots_to_keep
            .saturating_sub(if current_snapshot_id.is_some() { 1 } else { 0 });
        let mut kept_count = 0;

        for snapshot in to_expire_candidates.into_iter().rev() {
            let is_too_old = if let Some(max_age) = self.max_snapshot_age_ms {
                (now - snapshot.timestamp_ms) > max_age
            } else {
                false
            };

            if kept_count < num_to_keep || !is_too_old {
                kept_count += 1;
            } else {
                expired_ids.push(snapshot.snapshot_id);
            }
        }

        if expired_ids.is_empty() {
            return Ok(Vec::new());
        }

        // Update metadata
        self.metadata
            .snapshots
            .retain(|s| !expired_ids.contains(&s.snapshot_id));
        self.metadata
            .snapshot_log
            .retain(|e| !expired_ids.contains(&e.snapshot_id));
        self.metadata.increment_sequence();

        Ok(expired_ids)
    }
}

/// Action to delete files that are no longer referenced by any snapshot.
pub struct RemoveOrphanFiles<'a> {
    metadata: &'a TableMetadata,
    storage: &'a Storage,
}

impl<'a> RemoveOrphanFiles<'a> {
    pub fn new(metadata: &'a TableMetadata, storage: &'a Storage) -> Self {
        Self { metadata, storage }
    }

    /// Executes the removal action.
    ///
    /// WARNING: This scans all files in the table location.
    pub async fn execute(self) -> Result<Vec<String>> {
        // 1. Collect all referenced files
        let mut referenced_files = HashSet::new();

        for snapshot in &self.metadata.snapshots {
            // Add manifest list
            referenced_files.insert(snapshot.manifest_list.clone());

            // Load manifest list
            let manifest_list = ManifestList::load(&snapshot.manifest_list, self.storage).await?;
            for entry in &manifest_list.entries {
                referenced_files.insert(entry.manifest_path.clone());

                // Load manifest
                let manifest = ManifestFile::load(&entry.manifest_path, self.storage).await?;
                for m_entry in &manifest.entries {
                    if m_entry.status != ManifestEntryStatus::Deleted {
                        referenced_files.insert(m_entry.data_file.file_path.clone());
                    }
                }
            }
        }

        // 2. List all files in storage
        let all_files = self.storage.list_files(&self.metadata.location).await?;

        // 3. Find orphans
        let mut orphans = Vec::new();
        for file in all_files {
            if !referenced_files.contains(&file) {
                orphans.push(file.clone());
                // 4. Delete orphan (active)
                self.storage.delete(&file).await?;
            }
        }

        Ok(orphans)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::Operation;
    use crate::schema::Schema;
    use object_store::memory::InMemory;
    use std::sync::Arc;

    async fn setup() -> (TableMetadata, Storage) {
        let schema = Schema::builder(1).build();
        let metadata = TableMetadata::builder("test", schema).build();
        let storage = Storage::new(Arc::new(InMemory::new()));
        (metadata, storage)
    }

    #[tokio::test]
    async fn test_expire_snapshots() -> Result<()> {
        let (mut metadata, storage) = setup().await;

        // Add 5 snapshots
        for i in 1..=5 {
            let snapshot = Snapshot::builder(i as i64, format!("/m{}.json", i))
                .with_operation(Operation::Append)
                .build();
            // Manually set timestamp to simulate age
            let mut s = snapshot;
            s.timestamp_ms = Utc::now().timestamp_millis() - (i as i64 * 1000 * 3600 * 24); // days ago
            metadata.add_snapshot(s);
        }

        assert_eq!(metadata.snapshots.len(), 5);

        // Expire all but 2
        let expired = ExpireSnapshots::new(&mut metadata, &storage)
            .with_min_snapshots_to_keep(2)
            .with_max_snapshot_age(Duration::days(1))
            .execute()
            .await?;

        // Current snapshot is #5.
        // Snapshots #1, #2, #3, #4 are candidates.
        // #1, #2, #3, #4 are older than 1 day.
        // We keep min 2. Current is 1. So we keep #4.
        // Total kept: #5 (current), #4 (min).
        // Expired: #1, #2, #3.
        assert_eq!(expired.len(), 3);
        assert_eq!(metadata.snapshots.len(), 2);

        Ok(())
    }
}
