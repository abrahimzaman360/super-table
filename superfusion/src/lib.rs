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

        // 2. Get all data files
        let all_files = snapshot
            .all_data_files(&self.storage)
            .await
            .map_err(|e| datafusion::error::DataFusionError::External(e.into()))?;

        // 3. Prune files based on predicates
        let pruned_files = prune_data_files(all_files, filters);

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

        // 6. Return DataSourceExec
        let exec = DataSourceExec::new(Arc::new(file_scan_config));
        Ok(Arc::new(exec))
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
