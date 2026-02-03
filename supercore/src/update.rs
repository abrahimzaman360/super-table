//! # SuperTable Row-Level Updates
//!
//! This module provides tools for performing row-level update operations.
//! It currently supports the Copy-on-Write (CoW) strategy.

use crate::delete::RowLevelStrategy;
use crate::table::Table;
use crate::transaction::Transaction;
use anyhow::Result;
use arrow::array::RecordBatch;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_expr::execution_props::ExecutionProps;
use datafusion::prelude::SessionContext;
use std::collections::HashMap;

/// Builder for UPDATE operations.
pub struct UpdateBuilder {
    table: Table,
    filter: Expr,
    assignments: HashMap<String, Expr>,
    strategy: RowLevelStrategy,
}

impl UpdateBuilder {
    pub fn new(table: Table, filter: Expr) -> Self {
        Self {
            table,
            filter,
            assignments: HashMap::new(),
            strategy: RowLevelStrategy::CopyOnWrite,
        }
    }

    pub fn set(mut self, column: impl Into<String>, value: Expr) -> Self {
        self.assignments.insert(column.into(), value);
        self
    }

    pub fn with_strategy(mut self, strategy: RowLevelStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Executes the update operation and returns a Transaction.
    pub async fn execute(self) -> Result<Transaction> {
        match self.strategy {
            RowLevelStrategy::CopyOnWrite => self.execute_cow().await,
            RowLevelStrategy::MergeOnRead => Err(anyhow::anyhow!(
                "Merge-on-Read for UPDATE is not yet implemented"
            )),
        }
    }

    async fn execute_cow(&self) -> Result<Transaction> {
        let snapshot = self
            .table
            .metadata
            .current_snapshot()
            .ok_or_else(|| anyhow::anyhow!("No current snapshot to update"))?;
        let all_files = snapshot.all_data_files(&self.table.storage).await?;

        let schema_ref = self.table.metadata.current_schema().to_arrow_schema_ref();
        let df_schema = self.table.metadata.current_schema().to_df_schema()?;
        let _context = SessionContext::new();

        // Create physical expression for the filter
        let physical_filter =
            create_physical_expr(&self.filter, &df_schema, &ExecutionProps::new())?;

        // Create physical expressions for each assignment
        let mut physical_assignments = HashMap::new();
        for (col, expr) in &self.assignments {
            let phys_expr = create_physical_expr(expr, &df_schema, &ExecutionProps::new())?;
            physical_assignments.insert(col.clone(), phys_expr);
        }

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
            let mut any_updated = false;

            for batch in batches {
                let filter_result = physical_filter.evaluate(&batch)?;
                let filter_array = filter_result.into_array(batch.num_rows())?;
                let filter_boolean = filter_array
                    .as_any()
                    .downcast_ref::<arrow::array::BooleanArray>()
                    .ok_or_else(|| anyhow::anyhow!("Filter must return boolean"))?;

                if filter_boolean.true_count() > 0 {
                    any_updated = true;

                    // For matching rows, apply assignments.
                    // This is complex to do row-by-row on Arrow arrays.
                    // A better way is to evaluate the assignment expr on the whole batch,
                    // then use the filter_boolean to 'zip' (choose) between old and new values.

                    let mut new_columns = Vec::new();
                    for (i, field) in schema_ref.fields().iter().enumerate() {
                        let original_col = batch.column(i);

                        if let Some(phys_assignment) = physical_assignments.get(field.name()) {
                            let assigned_val = phys_assignment.evaluate(&batch)?;
                            let assigned_array = assigned_val.into_array(batch.num_rows())?;

                            // Zip: if filter is true, use assigned_array, else use original_col
                            let updated_col = arrow::compute::kernels::zip::zip(
                                filter_boolean,
                                &assigned_array,
                                original_col,
                            )?;
                            new_columns.push(updated_col);
                        } else {
                            new_columns.push(original_col.clone());
                        }
                    }

                    let updated_batch = RecordBatch::try_new(schema_ref.clone(), new_columns)?;
                    rewritten_batches.push(updated_batch);
                } else {
                    rewritten_batches.push(batch);
                }
            }

            if any_updated {
                tx.delete_file(file.file_path.clone());
                let combined_batch =
                    arrow::compute::concat_batches(&schema_ref, &rewritten_batches)?;
                let file_id = uuid::Uuid::new_v4().to_string();
                let new_file = writer.write_batch(&combined_batch, &file_id).await?;
                tx.add_file(new_file);
            }
        }

        Ok(tx)
    }
}
