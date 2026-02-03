use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::datasource::TableProvider;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;

use supercore::TableMetadata;
use supercore::storage::Storage;

mod pruning;
use pruning::prune_data_files;

/// A DataFusion TableProvider for SuperTable.
#[derive(Debug)]
pub struct SuperTableProvider {
    metadata: TableMetadata,
    storage: Storage,
}

impl SuperTableProvider {
    pub fn new(metadata: TableMetadata, storage: Storage) -> Self {
        Self { metadata, storage }
    }
}

#[async_trait]
impl TableProvider for SuperTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.metadata.current_schema().to_arrow_schema_ref()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // 1. Get current snapshot
        let snapshot = self.metadata.current_snapshot().ok_or_else(|| {
            datafusion::error::DataFusionError::Internal("Table has no snapshots".into())
        })?;

        // 2. Get all files (data + deletes)
        let (data_files, delete_files) = snapshot
            .all_files(&self.storage)
            .await
            .map_err(|e| datafusion::error::DataFusionError::External(e.into()))?;

        // 3. Prune data files based on predicates
        let pruned_files = prune_data_files(data_files, filters);

        // 4. Convert to DataFusion PartitionedFile
        let df_files: Vec<datafusion::datasource::listing::PartitionedFile> = pruned_files
            .into_iter()
            .map(|f| datafusion::datasource::listing::PartitionedFile {
                object_meta: datafusion::object_store::ObjectMeta {
                    location: datafusion::object_store::path::Path::from(f.file_path.clone()),
                    last_modified: chrono::Utc::now(), // Placeholder
                    size: f.file_size_in_bytes as u64,
                    e_tag: None,
                    version: None,
                },
                partition_values: vec![], // TODO: Hidden partitioning values
                range: None,
                extensions: None,
                statistics: None,
                metadata_size_hint: None,
            })
            .collect();

        // 5. Create FileScanConfig
        let object_store_url =
            datafusion::execution::object_store::ObjectStoreUrl::parse("super://")?;

        let file_source = Arc::new(ParquetSource::new(self.schema()));

        // DataFusion 52.1.0/53.0.0 uses a builder for FileScanConfig
        let file_scan_config = FileScanConfigBuilder::new(object_store_url, file_source)
            .with_file_group(FileGroup::new(df_files))
            .with_projection_indices(projection.cloned())?
            .build();

        // 6. Base Scan Execution Plan
        let base_exec = Arc::new(DataSourceExec::new(Arc::new(file_scan_config)));

        // 7. Apply Merge-on-Read Equality Deletes
        // For this prototype, if we have Equality Deletes, we read them eagerly
        // and apply a NOT IN negotiation filter.
        let equality_deletes: Vec<_> = delete_files
            .into_iter()
            .filter(|f| f.content == supercore::manifest::FileContent::EqualityDeletes)
            .collect();

        if equality_deletes.is_empty() {
            return Ok(base_exec);
        }

        // Read all equality deletes to find IDs
        // Note: This is simplified. In production, we'd use an AntiJoinExec or similar.
        let storage = self.storage.clone();
        let reader = supercore::reader::TableReader::new(storage);
        let mut ids_to_exclude = Vec::new();

        // Assume ID field is field_id=1 for now, matching delete.rs assumption
        let schema = self.metadata.current_schema();
        let id_col_idx = schema.fields.iter().position(|f| f.id == 1);

        if let Some(_idx) = id_col_idx {
            for del_file in equality_deletes {
                let batches = reader
                    .read_file(&del_file.file_path)
                    .await
                    .map_err(|e| datafusion::error::DataFusionError::External(e.into()))?;

                for batch in batches {
                    if batch.num_columns() > 0 {
                        ids_to_exclude.push(batch.column(0).clone());
                    }
                }
            }
        }

        if ids_to_exclude.is_empty() {
            return Ok(base_exec);
        }

        // Construct NOT IN filter
        use datafusion::logical_expr::{col, lit};
        let ids_refs: Vec<&dyn arrow::array::Array> =
            ids_to_exclude.iter().map(|a| a.as_ref()).collect();
        let combined_ids = arrow::compute::concat(&ids_refs)
            .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;

        // Convert to ScalarValue list
        let mut scalars = Vec::new();
        // Assume Int64 for ID column for this prototype logic
        if let Some(int64_ids) = combined_ids
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
        {
            for i in 0..int64_ids.len() {
                scalars.push(lit(int64_ids.value(i)));
            }
        } else {
            // Fallback or skip if not Int64 for now
            return Ok(base_exec);
        }

        // Create Filter: id NOT IN (ids...)
        // We need the name of the ID column
        let id_col_name = schema
            .fields
            .iter()
            .find(|f| f.id == 1)
            .unwrap()
            .name
            .clone();
        let in_list = datafusion::logical_expr::expr::InList {
            expr: Box::new(col(id_col_name)),
            list: scalars,
            negated: true,
        };
        let predicate = Expr::InList(in_list);

        // Wrap in FilterExec
        // We need physical plan filter, not logical Expr.
        let df_schema = base_exec.schema();
        let df_schema_converted =
            datafusion::common::DFSchema::try_from(df_schema.as_ref().clone())
                .map_err(|e| datafusion::error::DataFusionError::External(e.into()))?;

        let physical_predicate = datafusion::physical_expr::create_physical_expr(
            &predicate,
            &df_schema_converted,
            &datafusion::physical_expr::execution_props::ExecutionProps::new(),
        )?;

        let filter_exec =
            datafusion::physical_plan::filter::FilterExec::try_new(physical_predicate, base_exec)?;

        Ok(Arc::new(filter_exec))
    }

    fn supports_filters_pushdown(
        &self,
        _filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        // For now, say we don't support pushdown to keep it simple,
        // but we'll implement it soon for "production grade".
        Ok(vec![TableProviderFilterPushDown::Inexact; _filters.len()])
    }
}
