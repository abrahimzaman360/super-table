use std::any::Any;
use std::sync::Arc;

use arrow::array::Array;
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
        let base_exec: Arc<dyn ExecutionPlan> =
            Arc::new(DataSourceExec::new(Arc::new(file_scan_config)));

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

        // Map of Column ID -> List of values to exclude
        use arrow::array::ArrayRef;
        use std::collections::HashMap;
        let mut exclusions: HashMap<i32, Vec<ArrayRef>> = HashMap::new();
        let schema = self.metadata.current_schema();

        for del_file in equality_deletes {
            // Use metadata if available, otherwise skip (or fallback to id=1 if we wanted backward compat)
            if let Some(ids) = &del_file.equality_ids {
                // For now, only support single-column equality deletes per file
                if ids.len() == 1 {
                    let col_id = ids[0];
                    // Find column name
                    if let Some(field) = schema.fields.iter().find(|f| f.id == col_id) {
                        match reader.read_file(&del_file.file_path).await {
                            Ok(batches) => {
                                for batch in batches {
                                    if let Some(col) = batch.column_by_name(&field.name) {
                                        exclusions.entry(col_id).or_default().push(col.clone());
                                    }
                                }
                            }
                            Err(e) => {
                                // Log error or fail? For now, we propagate
                                return Err(datafusion::error::DataFusionError::External(e.into()));
                            }
                        }
                    }
                }
            }
        }

        if exclusions.is_empty() {
            return Ok(base_exec);
        }

        // Apply filters
        // If we have filters for multiple columns, we can chain them (AND logic).
        // (id NOT IN (...)) AND (name NOT IN (...))

        let mut current_exec = base_exec;
        let df_schema = current_exec.schema();
        let df_schema_converted =
            datafusion::common::DFSchema::try_from(df_schema.as_ref().clone())
                .map_err(|e| datafusion::error::DataFusionError::External(e.into()))?;
        let execution_props = datafusion::physical_expr::execution_props::ExecutionProps::new();

        use datafusion::logical_expr::{col, lit};

        for (col_id, arrays) in exclusions {
            let field_name = schema
                .fields
                .iter()
                .find(|f| f.id == col_id)
                .unwrap()
                .name
                .clone();

            // Flatten recursion
            let arrays_refs: Vec<&dyn arrow::array::Array> =
                arrays.iter().map(|a| a.as_ref()).collect();
            let combined = arrow::compute::concat(&arrays_refs)
                .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;

            // Create InList list from array elements
            // Simple iteration for primitives.
            // supporting Int64, Int32, Utf8 for now
            let mut scalars = Vec::new();

            if let Some(arr) = combined.as_any().downcast_ref::<arrow::array::Int64Array>() {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        scalars.push(lit(arr.value(i)));
                    }
                }
            } else if let Some(arr) = combined.as_any().downcast_ref::<arrow::array::Int32Array>() {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        scalars.push(lit(arr.value(i)));
                    }
                }
            } else if let Some(arr) = combined
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
            {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        scalars.push(lit(arr.value(i)));
                    }
                }
            } else {
                // Unsupported type for this prototype
                continue;
            }

            if !scalars.is_empty() {
                let in_list = datafusion::logical_expr::expr::InList {
                    expr: Box::new(col(&field_name)),
                    list: scalars,
                    negated: true,
                };
                let predicate = Expr::InList(in_list);

                let physical_predicate = datafusion::physical_expr::create_physical_expr(
                    &predicate,
                    &df_schema_converted,
                    &execution_props,
                )?;

                current_exec = Arc::new(datafusion::physical_plan::filter::FilterExec::try_new(
                    physical_predicate,
                    current_exec,
                )?);
            }
        }

        Ok(current_exec)
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
