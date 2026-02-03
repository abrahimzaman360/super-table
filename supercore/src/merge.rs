//! # SuperTable MERGE Operation
//!
//! This module provides the `MergeBuilder` for performing UPSERT (MERGE) operations.

use crate::table::Table;
use crate::transaction::Transaction;
use anyhow::Result;
use arrow::record_batch::RecordBatch;
use datafusion::logical_expr::Expr;
use futures::stream::BoxStream;

/// Action to take during a MERGE operation.
#[derive(Debug, Clone)]
pub enum MergeAction {
    /// Update existing rows with assignments.
    Update(Vec<(String, Expr)>),
    /// Delete existing rows.
    Delete,
    /// Insert new rows with assignments.
    Insert(Vec<(String, Expr)>),
}

/// A single clause in a MERGE operation.
#[derive(Debug, Clone)]
pub struct MergeClause {
    /// Optional condition for this clause.
    pub condition: Option<Expr>,
    /// The action to take if the condition matches.
    pub action: MergeAction,
}

/// Builder for MERGE (UPSERT) operations.
#[allow(unused)]
pub struct MergeBuilder {
    table: Table,
    source: BoxStream<'static, Result<RecordBatch>>,
    on_condition: Expr,
    matched_clauses: Vec<MergeClause>,
    not_matched_clauses: Vec<MergeClause>,
}

impl MergeBuilder {
    pub fn new(
        table: Table,
        source: BoxStream<'static, Result<RecordBatch>>,
        on_condition: Expr,
    ) -> Self {
        Self {
            table,
            source,
            on_condition,
            matched_clauses: Vec::new(),
            not_matched_clauses: Vec::new(),
        }
    }

    /// Adds a clause to be applied when the ON condition matches.
    pub fn when_matched(mut self, condition: Option<Expr>, action: MergeAction) -> Self {
        self.matched_clauses.push(MergeClause { condition, action });
        self
    }

    /// Adds a clause to be applied when the ON condition does NOT match.
    pub fn when_not_matched(mut self, condition: Option<Expr>, action: MergeAction) -> Self {
        self.not_matched_clauses
            .push(MergeClause { condition, action });
        self
    }

    /// Executes the merge operation as a Copy-on-Write process.
    pub async fn execute(self) -> Result<Transaction> {
        use datafusion::prelude::*;
        use futures::StreamExt;

        // 1. Setup DataFusion Context
        let ctx = SessionContext::new();

        // 2. Register Source Table
        // We collect source into memory for now (Prototype limitation)
        let batches: Vec<Result<RecordBatch>> = self.source.collect().await;
        let source_batches: Vec<RecordBatch> =
            batches.into_iter().collect::<Result<Vec<RecordBatch>>>()?;

        // Check if source is empty
        if source_batches.is_empty() {
            return Ok(self.table.new_transaction());
        }

        let source_schema = source_batches[0].schema();
        // Use MemTable
        let source_provider = datafusion::datasource::MemTable::try_new(
            source_schema,
            vec![source_batches.clone()], // Partitions
        )?;
        ctx.register_table("source", std::sync::Arc::new(source_provider))?;
        let source_df = ctx.table("source").await?;

        // 3. Register Target Table
        // Use TableReader to load target data
        let storage = self.table.storage.clone();
        let reader = crate::reader::TableReader::new(storage.clone());
        let snapshot = self
            .table
            .metadata
            .current_snapshot()
            .ok_or_else(|| anyhow::anyhow!("No snapshot"))?;
        let (data_files, _) = snapshot.all_files(&storage).await?;

        let mut target_batches = Vec::new();
        for file in data_files {
            let batches = reader.read_file(&file.file_path).await?;
            target_batches.extend(batches);
        }

        // Register Target MemTable
        let target_schema = if !target_batches.is_empty() {
            target_batches[0].schema()
        } else {
            self.table.metadata.current_schema().to_arrow_schema_ref()
        };

        let target_provider =
            datafusion::datasource::MemTable::try_new(target_schema, vec![target_batches])?;
        ctx.register_table("target", std::sync::Arc::new(target_provider))?;
        let target_df = ctx.table("target").await?;

        // 4. Perform Join to identify Matched vs Not Matched
        // logic: source LEFT JOIN target ON condition
        // We need to alias tables to distinguish columns

        // Simplified Logic for Prototype:
        // 1. Identify PK column (assume field_id=1, "id")
        let schema = self.table.metadata.current_schema();
        let id_field = schema
            .fields
            .iter()
            .find(|f| f.id == 1)
            .ok_or_else(|| anyhow::anyhow!("PK not found"))?;
        let id_col = &id_field.name;

        // 2. Find IDs to Delete (Matched Rows)
        // Join Source and Target on ID (assuming on_condition is ID equality for now, or evaluating generic condition)
        // Let's assume on_condition is `source.id = target.id`

        let join_df = source_df.join(
            target_df,
            datafusion::logical_expr::JoinType::Inner,
            &[id_col],
            &[id_col],
            None,
        )?; // Simplified: assumes join on ID column name
        let matched_ids_df = join_df.select(vec![col(id_col)])?;
        let matched_batches = matched_ids_df.collect().await?;

        // Write Equality Deletes for matched IDs
        let mut ids_to_delete = Vec::new();
        for batch in &matched_batches {
            if batch.num_columns() > 0 {
                ids_to_delete.push(batch.column(0).clone());
            }
        }

        let mut tx = self.table.new_transaction();

        if !ids_to_delete.is_empty() {
            // Concatenate and Write Delete File
            let total_ids: Vec<&dyn arrow::array::Array> =
                ids_to_delete.iter().map(|a| a.as_ref()).collect();
            let combined_ids = arrow::compute::concat(&total_ids)?;

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
                .write_batch(&batch, &format!("delete-merge-{}", file_id))
                .await?;
            data_file.content = crate::manifest::FileContent::EqualityDeletes;
            tx.add_file(data_file);
        }

        // 3. Write New Data (Updates + Inserts)
        // For simple Upsert (Merge), we just write the whole Source batch as new data
        // (Assuming we deleted the old versions of matched rows above)
        // This covers both "Update" (Insert new version) and "Insert" (Insert new row).

        let writer = crate::writer::TableWriter::new(
            self.table.storage.clone(),
            self.table.metadata.location.clone(),
            self.table.metadata.current_schema().to_arrow_schema_ref(),
        );

        // Concatenate all source batches
        // Should validate against table schema first? Yes.
        // Assuming source matches table schema for now.
        for batch in source_batches {
            let file_id = uuid::Uuid::new_v4().to_string();
            let data_file = writer.write_batch(&batch, &file_id).await?;
            tx.add_file(data_file);
        }

        Ok(tx)
    }
}
