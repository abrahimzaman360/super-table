use crate::manifest::FileContent;
use crate::scan::ScanTask;
use arrow::array::RecordBatch;
use arrow::array::{Array, AsArray, BooleanArray};
use arrow::compute::filter_record_batch;
use arrow::datatypes::SchemaRef;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::HashSet;

use crate::storage::Storage;

/// Reads Parquet files from storage.
pub struct TableReader {
    storage: Storage,
}

impl TableReader {
    pub fn new(storage: Storage) -> Self {
        Self { storage }
    }

    /// Reads a Parquet file from storage and returns its RecordBatches.
    pub async fn read_file(&self, path: &str) -> anyhow::Result<Vec<RecordBatch>> {
        let data = self.storage.read(path).await?;

        // Bytes implements ChunkReader directly
        let builder = ParquetRecordBatchReaderBuilder::try_new(data)?;
        let reader = builder.build()?;

        let batches: Result<Vec<_>, _> = reader.collect();
        Ok(batches?)
    }

    /// Get the schema from a Parquet file.
    pub async fn read_schema(&self, path: &str) -> anyhow::Result<SchemaRef> {
        let data = self.storage.read(path).await?;

        let builder = ParquetRecordBatchReaderBuilder::try_new(data)?;
        Ok(builder.schema().clone())
    }

    /// Reads a scan task and applies relevant deletes.
    pub async fn read_task(&self, task: ScanTask) -> anyhow::Result<Vec<RecordBatch>> {
        let batches = self.read_file(&task.data_file.file_path).await?;

        if task.delete_files.is_empty() {
            return Ok(batches);
        }

        // 1. Process position deletes
        let mut deleted_positions = HashSet::new();
        for delete_file in &task.delete_files {
            if delete_file.content == FileContent::PositionDeletes {
                let del_batches = self.read_file(&delete_file.file_path).await?;
                for batch in del_batches {
                    // Position delete files MUST have at least:
                    // - file_path: UTF8
                    // - pos: Int64
                    let path_col = batch.column(0).as_string::<i32>();
                    let pos_col = batch
                        .column(1)
                        .as_primitive::<arrow::datatypes::Int64Type>();

                    for i in 0..batch.num_rows() {
                        if !path_col.is_null(i) && path_col.value(i) == task.data_file.file_path {
                            deleted_positions.insert(pos_col.value(i));
                        }
                    }
                }
            }
        }

        if deleted_positions.is_empty() {
            return Ok(batches);
        }

        // 2. Apply deletes to data batches
        let mut filtered_batches = Vec::new();
        let mut current_pos = 0;

        for batch in batches {
            let num_rows = batch.num_rows();
            let mut mask = Vec::with_capacity(num_rows);
            for i in 0..num_rows {
                let is_deleted = deleted_positions.contains(&(current_pos + i as i64));
                mask.push(!is_deleted);
            }

            let bool_mask = BooleanArray::from(mask);
            let filtered = filter_record_batch(&batch, &bool_mask)?;
            filtered_batches.push(filtered);
            current_pos += num_rows as i64;
        }

        Ok(filtered_batches)
    }
}
