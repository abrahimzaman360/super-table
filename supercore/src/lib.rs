// Core modules
pub mod action;
pub mod catalog;
pub mod cdc;
pub mod datafusion;
pub mod delete;
pub mod manifest;
pub mod merge;
pub mod metadata;
pub mod optimize;
pub mod partition; // Added
pub mod polars;
pub mod scan;
pub mod schema;
pub mod statistics;
pub mod storage;
pub mod table;
pub mod transaction;
pub mod validation;

// I/O modules
pub mod reader;
pub mod update;
pub mod writer;

// Re-exports for convenience
pub use catalog::{Catalog, CatalogError, CatalogResult, InMemoryCatalog, TableIdentifier};
pub use manifest::{DataFile, FileFormat, ManifestList, Operation, Snapshot};
pub use metadata::{MetadataError, TableMetadata};
pub use partition::{PartitionSpec, Transform}; // Added
pub use schema::{Field, Schema, Type};
pub use storage::Storage;
pub use transaction::{PreparedCommit, RetryConfig, Transaction, TransactionError};

/// Prelude module for convenient imports.
///
/// Use `use supercore::prelude::*;` to import commonly used types.
pub mod prelude {
    pub use crate::catalog::{
        Catalog, CatalogError, CatalogResult, InMemoryCatalog, TableIdentifier,
    };
    pub use crate::cdc::ChangeDataCapture;
    pub use crate::manifest::{DataFile, FileFormat, ManifestList, Operation, Snapshot};
    pub use crate::metadata::{MetadataError, TableMetadata};
    pub use crate::optimize::{CompactOptions, CompactionScheduler, Optimizer};
    pub use crate::partition::{PartitionSpec, Transform}; // Added
    pub use crate::polars::PolarsConnector;
    pub use crate::schema::{Field, Schema, Type};
    pub use crate::storage::Storage;
    pub use crate::transaction::{PreparedCommit, RetryConfig, Transaction, TransactionError};
}

/// Library version.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// The current format version.
pub const FORMAT_VERSION: i32 = 1;

#[cfg(test)]
mod tests {
    use super::prelude::*;

    #[test]
    fn test_prelude_imports() {
        // Verify that all prelude items are accessible
        let schema = Schema::builder(0)
            .with_field(1, "id", Type::Long, true)
            .build();

        let _spec = PartitionSpec::builder(&schema)
            .add_identity("id")
            .unwrap()
            .build();

        let metadata = TableMetadata::builder("s3://bucket/test", schema).build();
        assert_eq!(metadata.format_version, crate::FORMAT_VERSION);
    }
}
