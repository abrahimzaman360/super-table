use crate::manifest::{DataFile, FileContent, Snapshot};
use crate::storage::Storage;
use anyhow::Result;

/// A single unit of work for reading a table.
/// Combines a data file with its relevant delete files.
#[derive(Debug, Clone)]
pub struct ScanTask {
    pub data_file: DataFile,
    pub delete_files: Vec<DataFile>,
}

/// A simple predicate for data pruning.
#[derive(Debug, Clone)]
pub enum Predicate {
    /// Equality predicate: column_id == value
    Eq { column_id: i32, value: Vec<u8> },
    /// Set membership: column_id IN (values)
    In {
        column_id: i32,
        values: Vec<Vec<u8>>,
    },
}

/// Plan scans for a table snapshot.
pub struct ScanPlanner<'a> {
    snapshot: &'a Snapshot,
    storage: &'a Storage,
    filter: Option<Predicate>,
}

impl<'a> ScanPlanner<'a> {
    pub fn new(snapshot: &'a Snapshot, storage: &'a Storage) -> Self {
        Self {
            snapshot,
            storage,
            filter: None,
        }
    }

    /// Adds a filter to the scan planner.
    pub fn with_filter(mut self, filter: Predicate) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Plans the scan by associating data files with relevant delete files.
    pub async fn plan(&self) -> Result<Vec<ScanTask>> {
        let (data_files, delete_files) = self.snapshot.all_files(self.storage).await?;

        // Group delete files by type
        let mut pos_deletes = Vec::new();
        let mut eq_deletes = Vec::new();

        for df in delete_files {
            match df.content {
                FileContent::PositionDeletes => pos_deletes.push(df),
                FileContent::EqualityDeletes => eq_deletes.push(df),
                _ => {}
            }
        }

        // For this prototype, we'll associate all equality deletes with all data files
        // and filter position deletes by file path if we were to read them here.
        // In a real implementation, we'd use partition pruning for deletes too.

        let tasks = data_files
            .into_iter()
            .filter(|df| self.should_keep_file(df))
            .map(|data_file| {
                // Find relevant position deletes for this specific file.
                // (Simplified: In a real system we'd use metadata to avoid searching all)
                let mut relevant_deletes = Vec::new();

                // Add all equality deletes (conservative)
                relevant_deletes.extend(eq_deletes.clone());

                // Add all position deletes (reader will filter)
                relevant_deletes.extend(pos_deletes.clone());

                ScanTask {
                    data_file,
                    delete_files: relevant_deletes,
                }
            })
            .collect();

        Ok(tasks)
    }

    fn should_keep_file(&self, data_file: &DataFile) -> bool {
        if let Some(ref filter) = self.filter {
            match filter {
                Predicate::Eq { column_id, value } => {
                    if let Some(stats) = data_file.statistics.get(column_id) {
                        if let Some(ref bf) = stats.bloom_filter {
                            return bf.contains(value);
                        }
                    }
                }
                Predicate::In { column_id, values } => {
                    if let Some(stats) = data_file.statistics.get(column_id) {
                        if let Some(ref bf) = stats.bloom_filter {
                            return values.iter().any(|v| bf.contains(v));
                        }
                    }
                }
            }
        }
        true
    }
}
