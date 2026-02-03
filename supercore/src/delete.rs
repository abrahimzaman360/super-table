//! # SuperTable Row-Level Deletes
//!
//! This module provides tools for performing row-level delete operations.
//! It supports both Copy-on-Write (CoW) and Merge-on-Read (MoR) strategies.

use crate::table::Table;
use crate::transaction::Transaction;
use anyhow::Result;
use arrow::compute::filter_record_batch;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_expr::execution_props::ExecutionProps;

/// Strategy for performing row-level operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RowLevelStrategy {
    /// Rewrite affected files entirely.
    #[default]
    CopyOnWrite,
    /// Write delete files to be merged during read.
    MergeOnRead,
}

/// Builder for DELETE operations.
#[allow(unused)]
pub struct DeleteBuilder {
    table: Table,
    filter: Expr,
    strategy: RowLevelStrategy,
}

impl DeleteBuilder {
    pub fn new(table: Table, filter: Expr) -> Self {
        Self {
            table,
            filter,
            strategy: RowLevelStrategy::CopyOnWrite,
        }
    }

    pub fn with_strategy(mut self, strategy: RowLevelStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Executes the delete operation and returns a Transaction.
    pub async fn execute(self) -> Result<Transaction> {
        let _snapshot = self
            .table
            .metadata
            .current_snapshot()
            .ok_or_else(|| anyhow::anyhow!("No current snapshot to delete from"))?;

        match self.strategy {
            RowLevelStrategy::CopyOnWrite => self.execute_cow().await,
            RowLevelStrategy::MergeOnRead => self.execute_mor().await,
        }
    }

    async fn execute_cow(&self) -> Result<Transaction> {
        let snapshot = self.table.metadata.current_snapshot().unwrap();
        let all_files = snapshot.all_data_files(&self.table.storage).await?;

        let schema_ref = self.table.metadata.current_schema().to_arrow_schema_ref();

        // Create physical expression from the logical filter
        let physical_filter = create_physical_expr(
            &self.filter,
            &self.table.metadata.current_schema().to_df_schema()?,
            &ExecutionProps::new(),
        )?;

        let mut tx = self.table.new_transaction();
        let reader = crate::reader::TableReader::new(self.table.storage.clone());
        let writer = crate::writer::TableWriter::new(
            self.table.storage.clone(),
            self.table.metadata.location.clone(),
            schema_ref.clone(),
        );

        for file in all_files {
            if file.content != crate::manifest::FileContent::Data {
                continue;
            }

            let batches = reader.read_file(&file.file_path).await?;
            let mut rewritten_batches = Vec::new();
            let mut any_deleted = false;

            for batch in batches {
                // Evaluate filter on the batch
                // Note: The filter expression in 'DELETE FROM table WHERE filter'
                // specifies which rows to DELETE.
                // However, filter_record_batch keeps rows where the result is TRUE.
                // So we actually need to keep rows where the filter is FALSE or NULL.
                // For simplicity in this implementation, we'll invert the filter if possible,
                // or just handle the selection here.

                let filter_result = physical_filter.evaluate(&batch)?;
                let filter_array = filter_result.into_array(batch.num_rows())?;
                let filter_boolean = filter_array
                    .as_any()
                    .downcast_ref::<arrow::array::BooleanArray>()
                    .ok_or_else(|| anyhow::anyhow!("Filter must return boolean"))?;

                // Invert the filter: keep rows where filter is FALSE
                let keep_mask = arrow::compute::not(filter_boolean)?;

                // Check if any rows were deleted (where filter was TRUE)
                if filter_boolean.true_count() > 0 {
                    any_deleted = true;
                }

                let filtered_batch = filter_record_batch(&batch, &keep_mask)?;
                if filtered_batch.num_rows() > 0 {
                    rewritten_batches.push(filtered_batch);
                }
            }

            if any_deleted {
                tx.delete_file(file.file_path.clone());
                if !rewritten_batches.is_empty() {
                    let combined_batch =
                        arrow::compute::concat_batches(&schema_ref, &rewritten_batches)?;
                    let file_id = uuid::Uuid::new_v4().to_string();
                    let new_file = writer.write_batch(&combined_batch, &file_id).await?;
                    tx.add_file(new_file);
                }
            }
        }

        Ok(tx)
    }

    async fn execute_mor(&self) -> Result<Transaction> {
        // For this prototype, we'll try to extract equality constraints from the filter
        // and write an Equality Delete file.
        // E.g., if filter is "id = 5", we write a parquet file with column "id" and value 5.

        let snapshot = self.table.metadata.current_snapshot().unwrap();
        let schema = self.table.metadata.current_schema();

        // Find identifier column (assuming first column 'id' or similar for now, or use PK if defined)
        // Todo: Use Table Identifier Fields. For now, picking field_id 1.
        let id_field = schema.fields.iter().find(|f| f.id == 1).cloned();

        if id_field.is_none() {
            return Err(anyhow::anyhow!(
                "Cannot determine equality field (id=1) for MoR"
            ));
        }
        let id_field = id_field.unwrap();

        // Scan to find IDs to delete
        let physical_filter = create_physical_expr(
            &self.filter,
            &schema.to_df_schema()?,
            &ExecutionProps::new(),
        )?;

        let reader = crate::reader::TableReader::new(self.table.storage.clone());
        let all_files = snapshot.all_data_files(&self.table.storage).await?;

        let mut ids_to_delete = Vec::new();

        for file in all_files {
            if file.content != crate::manifest::FileContent::Data {
                continue;
            }
            let batches = reader.read_file(&file.file_path).await?;
            for batch in batches {
                let filter_result = physical_filter.evaluate(&batch)?;
                let filter_array = filter_result.into_array(batch.num_rows())?;
                let filter_boolean = filter_array
                    .as_any()
                    .downcast_ref::<arrow::array::BooleanArray>()
                    .unwrap();

                if filter_boolean.true_count() > 0 {
                    // Extract IDs
                    let id_col_idx = schema.fields.iter().position(|f| f.id == 1).unwrap();
                    let id_array = batch.column(id_col_idx);
                    // Filter ids to keep only those that matched the delete filter
                    let filtered_ids = arrow::compute::filter(id_array, filter_boolean)?;
                    ids_to_delete.push(filtered_ids);
                }
            }
        }

        if ids_to_delete.is_empty() {
            return Ok(self.table.new_transaction());
        }

        // Concatenate all IDs
        let total_ids: Vec<&dyn arrow::array::Array> =
            ids_to_delete.iter().map(|a| a.as_ref()).collect();
        let combined_ids = arrow::compute::concat(&total_ids)?;

        // Create Equality Delete File Batch
        let del_schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new(
                id_field.name.clone(),
                id_field.field_type.to_arrow_datatype(),
                false,
            ),
        ]));

        let batch =
            arrow::record_batch::RecordBatch::try_new(del_schema.clone(), vec![combined_ids])?;

        let writer = crate::writer::TableWriter::new(
            self.table.storage.clone(),
            self.table.metadata.location.clone(),
            del_schema,
        );

        let file_id = uuid::Uuid::new_v4().to_string();
        let mut data_file = writer
            .write_batch(&batch, &format!("delete-eq-{}", file_id))
            .await?;
        data_file.content = crate::manifest::FileContent::EqualityDeletes;

        let mut tx = self.table.new_transaction();
        tx.add_file(data_file);

        Ok(tx)
    }
}
