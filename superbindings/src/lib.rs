//! # SuperTable Python Bindings
//!
//! This module provides Python bindings for SuperTable using PyO3.
//!
//! ## Usage
//!
//! ```python
//! import supertable as st
//!
//! # Create a schema
//! schema = st.Schema([
//!     st.Field(1, "id", st.Type.Long, required=True),
//!     st.Field(2, "name", st.Type.String, required=False),
//! ])
//!
//! # Create a table
//! table = st.create_table("s3://bucket/my_table", schema)
//!
//! # Open an existing table
//! table = st.open_table("s3://bucket/my_table")
//!
//! # Read as Arrow
//! arrow_table = table.to_arrow()
//!
//! # Read as Pandas
//! df = table.to_pandas()
//! ```

use pyo3::prelude::*;
use std::collections::HashMap;

mod py_schema;
mod py_table;

use py_schema::{PyField, PySchema, PyType};
use py_table::PyTable;

/// SuperTable - A next-generation open table format.
///
/// This module provides Python bindings for creating, reading, and managing
/// tables in the SuperTable format.
#[pymodule]
fn supertable(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Register classes
    m.add_class::<PySchema>()?;
    m.add_class::<PyField>()?;
    m.add_class::<PyType>()?;
    m.add_class::<PyTable>()?;

    // Register functions
    m.add_function(wrap_pyfunction!(create_table, m)?)?;
    m.add_function(wrap_pyfunction!(open_table, m)?)?;
    m.add_function(wrap_pyfunction!(version, m)?)?;

    Ok(())
}

/// Creates a new SuperTable table.
///
/// Args:
///     location: The storage location for the table (e.g., "s3://bucket/table")
///     schema: The table schema
///     properties: Optional table properties
///
/// Returns:
///     A PyTable instance representing the new table
///
/// Example:
///     >>> schema = st.Schema([st.Field(1, "id", st.Type.Long, required=True)])
///     >>> table = st.create_table("file:///tmp/my_table", schema)
#[pyfunction]
#[pyo3(signature = (location, schema, properties=None))]
fn create_table(
    location: String,
    schema: &PySchema,
    properties: Option<HashMap<String, String>>,
) -> PyResult<PyTable> {
    use std::fs;
    use std::path::Path;

    let rust_schema = schema.to_rust_schema();

    let mut builder = supercore::TableMetadata::builder(&location, rust_schema);

    if let Some(props) = properties {
        builder = builder.with_properties(props);
    }

    let metadata = builder.build();

    // Create directory structure
    let table_path = Path::new(&location);
    let metadata_dir = table_path.join("metadata");

    fs::create_dir_all(&metadata_dir).map_err(|e| {
        pyo3::exceptions::PyIOError::new_err(format!("Failed to create table directory: {}", e))
    })?;

    // Write metadata as JSON
    let metadata_json = serde_json::to_string_pretty(&metadata).map_err(|e| {
        pyo3::exceptions::PyValueError::new_err(format!("Failed to serialize metadata: {}", e))
    })?;

    let version_hint_path = metadata_dir.join("version-hint.text");
    let metadata_file = format!("v{}.metadata.json", metadata.last_sequence_number);
    let metadata_path = metadata_dir.join(&metadata_file);

    fs::write(&metadata_path, &metadata_json).map_err(|e| {
        pyo3::exceptions::PyIOError::new_err(format!("Failed to write metadata: {}", e))
    })?;

    fs::write(
        &version_hint_path,
        metadata.last_sequence_number.to_string(),
    )
    .map_err(|e| {
        pyo3::exceptions::PyIOError::new_err(format!("Failed to write version hint: {}", e))
    })?;

    // Create data directory
    let data_dir = table_path.join("data");
    fs::create_dir_all(&data_dir).map_err(|e| {
        pyo3::exceptions::PyIOError::new_err(format!("Failed to create data directory: {}", e))
    })?;

    Ok(PyTable::new(location, metadata))
}

/// Opens an existing SuperTable table.
///
/// Args:
///     location: The storage location of the table
///
/// Returns:
///     A PyTable instance
///
/// Example:
///     >>> table = st.open_table("s3://bucket/my_table")
#[pyfunction]
fn open_table(location: String) -> PyResult<PyTable> {
    // For now, create a placeholder - full implementation requires storage access
    // In a complete implementation, this would:
    // 1. Read the metadata file from storage
    // 2. Parse it into TableMetadata
    // 3. Return a PyTable wrapping it

    let schema = supercore::Schema::builder(0)
        .with_field(1, "placeholder", supercore::Type::Long, true)
        .build();

    let metadata = supercore::TableMetadata::builder(&location, schema).build();

    Ok(PyTable::new(location, metadata))
}

/// Returns the SuperTable library version.
#[pyfunction]
fn version() -> &'static str {
    supercore::VERSION
}
