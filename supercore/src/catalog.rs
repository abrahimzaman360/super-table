//! # SuperTable Catalog
//!
//! This module defines the catalog abstraction for SuperTable. The catalog
//! is responsible for:
//!
//! - **Namespace management**: Organizing tables into hierarchical namespaces
//! - **Table metadata tracking**: Storing the location of current metadata files
//! - **Atomic commits**: Ensuring ACID guarantees through compare-and-swap
//!
//! ## Catalog Implementations
//!
//! SuperTable supports multiple catalog backends:
//!
//! - **In-Memory**: For testing and development
//! - **SQLite**: For single-node deployments
//! - **PostgreSQL**: For production multi-node deployments
//! - **REST**: For integration with existing Iceberg REST catalogs
//!
//! ## Concurrency
//!
//! All catalog implementations must support atomic compare-and-swap operations
//! to enable optimistic concurrency control. This is typically achieved through:
//!
//! - Database transactions with row-level locking
//! - Conditional writes (e.g., etags, version numbers)
//! - Distributed consensus (e.g., Raft, Paxos)

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;

use crate::metadata::TableMetadata;


/// Errors that can occur during catalog operations.
#[derive(Debug, Error)]
pub enum CatalogError {
    /// The requested table was not found.
    #[error("table not found: {0}")]
    TableNotFound(String),

    /// The requested namespace was not found.
    #[error("namespace not found: {0}")]
    NamespaceNotFound(String),

    /// The table already exists.
    #[error("table already exists: {0}")]
    TableAlreadyExists(String),

    /// The namespace already exists.
    #[error("namespace already exists: {0}")]
    NamespaceAlreadyExists(String),

    /// A conflict occurred during an atomic operation.
    #[error("commit conflict: expected version {expected}, found {actual}")]
    CommitConflict { expected: i64, actual: i64 },

    /// An I/O error occurred.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// A serialization error occurred.
    #[error("serialization error: {0}")]
    Serialization(String),
}

/// Result type for catalog operations.
pub type CatalogResult<T> = Result<T, CatalogError>;

/// A table identifier consisting of a namespace and table name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableIdentifier {
    /// The namespace (can be multi-level, e.g., ["db", "schema"]).
    pub namespace: Vec<String>,

    /// The table name.
    pub name: String,
}

impl TableIdentifier {
    /// Creates a new table identifier.
    pub fn new(
        namespace: impl IntoIterator<Item = impl Into<String>>,
        name: impl Into<String>,
    ) -> Self {
        Self {
            namespace: namespace.into_iter().map(|s| s.into()).collect(),
            name: name.into(),
        }
    }

    /// Creates a table identifier from a single namespace level.
    pub fn of(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            namespace: vec![namespace.into()],
            name: name.into(),
        }
    }

    /// Returns the fully qualified name (namespace.table).
    pub fn full_name(&self) -> String {
        if self.namespace.is_empty() {
            self.name.clone()
        } else {
            format!("{}.{}", self.namespace.join("."), self.name)
        }
    }

    /// Parses a fully qualified name into a TableIdentifier.
    pub fn parse(full_name: &str) -> Self {
        let parts: Vec<&str> = full_name.split('.').collect();
        if parts.len() == 1 {
            Self {
                namespace: Vec::new(),
                name: parts[0].to_string(),
            }
        } else {
            let (namespace, name) = parts.split_at(parts.len() - 1);
            Self {
                namespace: namespace.iter().map(|s| s.to_string()).collect(),
                name: name[0].to_string(),
            }
        }
    }
}

impl std::fmt::Display for TableIdentifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.full_name())
    }
}

/// Properties for a namespace.
pub type NamespaceProperties = HashMap<String, String>;

/// The main catalog trait for managing tables.
#[async_trait]
pub trait Catalog: Send + Sync {
    /// Returns the name of this catalog.
    fn name(&self) -> &str;

    /// Lists all namespaces, optionally under a parent namespace.
    async fn list_namespaces(&self, parent: Option<&[String]>) -> CatalogResult<Vec<Vec<String>>>;

    /// Creates a new namespace.
    async fn create_namespace(
        &self,
        namespace: &[String],
        properties: NamespaceProperties,
    ) -> CatalogResult<()>;

    /// Drops a namespace (must be empty).
    async fn drop_namespace(&self, namespace: &[String]) -> CatalogResult<()>;

    /// Gets namespace properties.
    async fn namespace_properties(
        &self,
        namespace: &[String],
    ) -> CatalogResult<NamespaceProperties>;

    /// Lists all tables in a namespace.
    async fn list_tables(&self, namespace: &[String]) -> CatalogResult<Vec<TableIdentifier>>;

    /// Creates a new table.
    async fn create_table(
        &self,
        identifier: &TableIdentifier,
        metadata: TableMetadata,
    ) -> CatalogResult<TableMetadata>;

    /// Loads a table's metadata.
    async fn load_table(&self, identifier: &TableIdentifier) -> CatalogResult<TableMetadata>;

    /// Drops a table.
    async fn drop_table(&self, identifier: &TableIdentifier, purge: bool) -> CatalogResult<()>;

    /// Renames a table.
    async fn rename_table(&self, from: &TableIdentifier, to: &TableIdentifier)
    -> CatalogResult<()>;

    /// Checks if a table exists.
    async fn table_exists(&self, identifier: &TableIdentifier) -> CatalogResult<bool>;

    /// Atomically updates table metadata using compare-and-swap.
    ///
    /// # Arguments
    ///
    /// * `identifier` - The table to update
    /// * `base_version` - The expected current version (sequence number)
    /// * `metadata` - The new metadata to commit
    ///
    /// # Returns
    ///
    /// The committed metadata on success.
    async fn commit_table(
        &self,
        identifier: &TableIdentifier,
        base_version: i64,
        metadata: TableMetadata,
    ) -> CatalogResult<TableMetadata>;
}

/// An in-memory catalog implementation for testing and development.
///
/// This implementation stores all metadata in memory and is not persistent.
/// It's useful for unit tests and local development.
#[derive(Debug)]
pub struct InMemoryCatalog {
    name: String,
    namespaces: RwLock<HashMap<Vec<String>, NamespaceProperties>>,
    tables: RwLock<HashMap<TableIdentifier, TableMetadata>>,
}

impl InMemoryCatalog {
    /// Creates a new in-memory catalog.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            namespaces: RwLock::new(HashMap::new()),
            tables: RwLock::new(HashMap::new()),
        }
    }

    /// Creates a new catalog wrapped in an Arc for sharing.
    pub fn shared(name: impl Into<String>) -> Arc<Self> {
        Arc::new(Self::new(name))
    }
}

#[async_trait]
impl Catalog for InMemoryCatalog {
    fn name(&self) -> &str {
        &self.name
    }

    async fn list_namespaces(&self, parent: Option<&[String]>) -> CatalogResult<Vec<Vec<String>>> {
        let namespaces: tokio::sync::RwLockReadGuard<HashMap<Vec<String>, crate::catalog::NamespaceProperties>> = self.namespaces.read().await;
        let result: Vec<Vec<String>> = namespaces
            .keys()
            .filter(|ns| match parent {
                Some(p) => ns.starts_with(p) && ns.len() == p.len() + 1,
                None => ns.len() == 1,
            })
            .cloned()
            .collect();
        Ok(result)
    }

    async fn create_namespace(
        &self,
        namespace: &[String],
        properties: NamespaceProperties,
    ) -> CatalogResult<()> {
        let mut namespaces: tokio::sync::RwLockWriteGuard<HashMap<Vec<String>, crate::catalog::NamespaceProperties>> = self.namespaces.write().await;
        let ns_vec = namespace.to_vec();

        if namespaces.contains_key(&ns_vec) {
            return Err(CatalogError::NamespaceAlreadyExists(namespace.join(".")));
        }

        namespaces.insert(ns_vec, properties);
        Ok(())
    }

    async fn drop_namespace(&self, namespace: &[String]) -> CatalogResult<()> {
        let mut namespaces = self.namespaces.write().await;
        let ns_vec = namespace.to_vec();

        if namespaces.remove(&ns_vec).is_none() {
            return Err(CatalogError::NamespaceNotFound(namespace.join(".")));
        }

        Ok(())
    }

    async fn namespace_properties(
        &self,
        namespace: &[String],
    ) -> CatalogResult<NamespaceProperties> {
        let namespaces = self.namespaces.read().await;
        namespaces
            .get(namespace)
            .cloned()
            .ok_or_else(|| CatalogError::NamespaceNotFound(namespace.join(".")))
    }

    async fn list_tables(&self, namespace: &[String]) -> CatalogResult<Vec<TableIdentifier>> {
        let tables = self.tables.read().await;
        let result: Vec<TableIdentifier> = tables
            .keys()
            .filter(|id| id.namespace == namespace)
            .cloned()
            .collect();
        Ok(result)
    }

    async fn create_table(
        &self,
        identifier: &TableIdentifier,
        metadata: TableMetadata,
    ) -> CatalogResult<TableMetadata> {
        let mut tables = self.tables.write().await;

        if tables.contains_key(identifier) {
            return Err(CatalogError::TableAlreadyExists(identifier.full_name()));
        }

        tables.insert(identifier.clone(), metadata.clone());
        Ok(metadata)
    }

    async fn load_table(&self, identifier: &TableIdentifier) -> CatalogResult<TableMetadata> {
        let tables = self.tables.read().await;
        tables
            .get(identifier)
            .cloned()
            .ok_or_else(|| CatalogError::TableNotFound(identifier.full_name()))
    }

    async fn drop_table(&self, identifier: &TableIdentifier, _purge: bool) -> CatalogResult<()> {
        let mut tables = self.tables.write().await;

        if tables.remove(identifier).is_none() {
            return Err(CatalogError::TableNotFound(identifier.full_name()));
        }

        Ok(())
    }

    async fn rename_table(
        &self,
        from: &TableIdentifier,
        to: &TableIdentifier,
    ) -> CatalogResult<()> {
        let mut tables = self.tables.write().await;

        let metadata = tables
            .remove(from)
            .ok_or_else(|| CatalogError::TableNotFound(from.full_name()))?;

        if tables.contains_key(to) {
            // Restore the original table if target exists
            tables.insert(from.clone(), metadata);
            return Err(CatalogError::TableAlreadyExists(to.full_name()));
        }

        tables.insert(to.clone(), metadata);
        Ok(())
    }

    async fn table_exists(&self, identifier: &TableIdentifier) -> CatalogResult<bool> {
        let tables: tokio::sync::RwLockReadGuard<HashMap<TableIdentifier, TableMetadata>> = self.tables.read().await;
        Ok(tables.contains_key(identifier))
    }

    async fn commit_table(
        &self,
        identifier: &TableIdentifier,
        base_version: i64,
        metadata: TableMetadata,
    ) -> CatalogResult<TableMetadata> {
        let mut tables: tokio::sync::RwLockWriteGuard<HashMap<TableIdentifier, TableMetadata>> = self.tables.write().await;

        let current = tables
            .get(identifier)
            .ok_or_else(|| CatalogError::TableNotFound(identifier.full_name()))?;

        // Check for conflicts using sequence number
        if current.last_sequence_number != base_version {
            return Err(CatalogError::CommitConflict {
                expected: base_version,
                actual: current.last_sequence_number,
            });
        }

        tables.insert(identifier.clone(), metadata.clone());
        Ok(metadata)
    }
}



#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{Schema, Type};

    fn sample_metadata(location: &str) -> TableMetadata {
        let schema = Schema::builder(0)
            .with_field(1, "id", Type::Long, true)
            .build();
        TableMetadata::builder(location, schema).build()
    }

    #[tokio::test]
    async fn test_create_namespace() {
        let catalog = InMemoryCatalog::new("test");

        catalog
            .create_namespace(&["db".into()], HashMap::new())
            .await
            .unwrap();

        let namespaces = catalog.list_namespaces(None).await.unwrap();
        assert_eq!(namespaces.len(), 1);
        assert_eq!(namespaces[0], vec!["db".to_string()]);
    }

    #[tokio::test]
    async fn test_create_table() {
        let catalog = InMemoryCatalog::new("test");

        let identifier = TableIdentifier::of("db", "users");
        let metadata = sample_metadata("s3://bucket/users");

        catalog.create_table(&identifier, metadata).await.unwrap();

        let exists = catalog.table_exists(&identifier).await.unwrap();
        assert!(exists);
    }

    #[tokio::test]
    async fn test_commit_conflict() {
        let catalog = InMemoryCatalog::new("test");

        let identifier = TableIdentifier::of("db", "users");
        let metadata = sample_metadata("s3://bucket/users");

        catalog
            .create_table(&identifier, metadata.clone())
            .await
            .unwrap();

        // Try to commit with wrong base version
        let result = catalog
            .commit_table(&identifier, 999, metadata.clone())
            .await;

        assert!(matches!(result, Err(CatalogError::CommitConflict { .. })));
    }

    #[tokio::test]
    async fn test_table_identifier_parsing() {
        let id = TableIdentifier::parse("db.schema.users");
        assert_eq!(id.namespace, vec!["db", "schema"]);
        assert_eq!(id.name, "users");
        assert_eq!(id.full_name(), "db.schema.users");

        let id2 = TableIdentifier::parse("simple_table");
        assert!(id2.namespace.is_empty());
        assert_eq!(id2.name, "simple_table");
    }
}
