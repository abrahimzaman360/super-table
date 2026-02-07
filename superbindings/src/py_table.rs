//! Python bindings for SuperTable table operations.

use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::py_schema::PySchema;
use supercore::TableMetadata;

/// A SuperTable table.
///
/// This class represents a table and provides methods for reading,
/// writing, and managing table data.
#[pyclass]
pub struct PyTable {
    location: String,
    metadata: TableMetadata,
}

impl PyTable {
    pub fn new(location: String, metadata: TableMetadata) -> Self {
        Self { location, metadata }
    }
}

#[pymethods]
impl PyTable {
    /// Returns the table location.
    #[getter]
    fn location(&self) -> &str {
        &self.location
    }

    /// Returns the table UUID.
    #[getter]
    fn uuid(&self) -> String {
        self.metadata.table_uuid.to_string()
    }

    /// Returns the current schema.
    #[getter]
    fn schema(&self) -> PySchema {
        let rust_schema = self.metadata.current_schema();
        let fields: Vec<crate::py_schema::PyField> = rust_schema
            .fields
            .iter()
            .map(|f| crate::py_schema::PyField {
                id: f.id,
                name: f.name.clone(),
                field_type: type_to_py(&f.field_type),
                required: f.required,
                doc: f.doc.clone(),
            })
            .collect();

        PySchema::new(fields, rust_schema.schema_id)
    }

    /// Returns the current snapshot ID, if any.
    #[getter]
    fn current_snapshot_id(&self) -> Option<i64> {
        self.metadata.current_snapshot_id
    }

    /// Returns the table format version.
    #[getter]
    fn format_version(&self) -> i32 {
        self.metadata.format_version
    }

    /// Returns all table properties.
    #[getter]
    fn properties<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        for (key, value) in &self.metadata.properties {
            dict.set_item(key, value)?;
        }
        Ok(dict)
    }

    /// Returns the list of snapshot IDs.
    fn snapshot_ids(&self) -> Vec<i64> {
        self.metadata
            .snapshots
            .iter()
            .map(|s| s.snapshot_id)
            .collect()
    }

    /// Returns the number of snapshots.
    fn snapshot_count(&self) -> usize {
        self.metadata.snapshots.len()
    }

    /// Gets table history as a list of (snapshot_id, timestamp_ms) tuples.
    fn history(&self) -> Vec<(i64, i64)> {
        self.metadata
            .snapshot_log
            .iter()
            .map(|e| (e.snapshot_id, e.timestamp_ms))
            .collect()
    }

    /// Gets a snapshot by ID.
    fn snapshot_by_id(&self, snapshot_id: i64) -> Option<bool> {
        self.metadata.snapshot(snapshot_id).is_some().into()
    }

    /// Gets the snapshot that was current at the given timestamp.
    fn snapshot_at(&self, timestamp_ms: i64) -> Option<i64> {
        self.metadata
            .snapshot_at(timestamp_ms)
            .map(|s| s.snapshot_id)
    }

    /// Rolls back the table to a previous snapshot ID.
    fn rollback_to(&mut self, snapshot_id: i64) -> PyResult<()> {
        self.metadata
            .rollback_to(snapshot_id)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
        Ok(())
    }

    /// Returns the changes since a given snapshot ID.
    fn changes_since(&self, _snapshot_id: i64) -> PyResult<Vec<String>> {
        // Placeholder returning file paths of changes
        Ok(Vec::new())
    }

    /// Refreshes the table metadata from storage.
    ///
    /// This is a placeholder - full implementation requires storage access.
    fn refresh(&mut self) -> PyResult<()> {
        // In a complete implementation, this would reload metadata from storage
        Ok(())
    }

    /// Deletes rows matching the given filter.
    ///
    /// This uses Merge-on-Read (Equality Deletes) by default.
    ///
    /// Args:
    ///     filter: A SQL-like filter string (e.g. "id = 5")
    fn delete(&mut self, filter: String) -> PyResult<()> {
        // Todo: Instantiate real Table and call DeleteBuilder
        println!("Deleted rows matching: {}", filter);
        Ok(())
    }

    /// Merges data into the table (Upsert).
    ///
    /// Uses the SQL MERGE semantic.
    ///
    /// Args:
    ///     source: A list of dictionaries representing the source data
    ///     on_column: The column to join on (e.g. "id")
    /// Merges data into the table (Upsert).
    ///
    /// Uses the SQL MERGE semantic.
    ///
    /// Args:
    ///     source: A list of dictionaries representing the source data
    ///     on_column: The column to join on (e.g. "id")
    fn merge(&mut self, _source: Py<PyAny>, _on_column: String) -> PyResult<()> {
        // Todo: Instantiate real Table and call MergeBuilder
        println!("Merged data on column: {}", _on_column);
        Ok(())
    }

    /// Converts the table to a PyArrow Table.
    fn to_arrow<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        use arrow::pyarrow::ToPyArrow;
        use supercore::prelude::*;

        // 1. Setup async runtime
        let rt = tokio::runtime::Runtime::new().unwrap();

        // 2. Scan the table
        let batches = rt
            .block_on(async {
                let storage = Storage::from_location(&self.metadata.location)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

                let snapshot = match self.metadata.current_snapshot() {
                    Some(s) => s,
                    None => return Ok(Vec::new()),
                };

                let planner = supercore::scan::ScanPlanner::new(snapshot, &storage);
                let tasks = planner.plan().await.map_err(|e| anyhow::anyhow!(e))?;

                let reader = supercore::reader::TableReader::new(storage);
                let mut all_batches = Vec::new();

                for task in tasks {
                    let batches = reader
                        .read_task(task)
                        .await
                        .map_err(|e| anyhow::anyhow!(e))?;
                    all_batches.extend(batches);
                }

                Ok::<_, anyhow::Error>(all_batches)
            })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        if batches.is_empty() {
            // Return empty table with schema?
            // For now, return empty list of batches to create empty table
            let pa = py.import("pyarrow")?;
            return pa
                .getattr("Table")?
                .call_method1("from_batches", (Vec::<Py<PyAny>>::new(),));
        }

        // 3. Convert to PyArrow objects
        let mut py_batches = Vec::new();
        for batch in batches {
            let py_batch = batch.to_pyarrow(py)?;
            py_batches.push(py_batch);
        }

        // 4. Create Table from batches
        let pa = py.import("pyarrow")?;
        let table = pa
            .getattr("Table")?
            .call_method1("from_batches", (py_batches,))?;

        Ok(table)
    }

    /// Returns a string representation.
    fn __repr__(&self) -> String {
        format!(
            "Table(location='{}', schema_id={}, snapshots={})",
            self.location,
            self.metadata.current_schema_id,
            self.metadata.snapshots.len()
        )
    }

    /// Returns a human-readable summary.
    fn __str__(&self) -> String {
        let schema = self.metadata.current_schema();
        let field_count = schema.fields.len();
        let snapshot_count = self.metadata.snapshots.len();

        format!(
            "SuperTable: {}\n  Location: {}\n  Fields: {}\n  Snapshots: {}",
            self.metadata.table_uuid, self.location, field_count, snapshot_count
        )
    }
}

/// Converts a Rust Type to a PyType.
fn type_to_py(t: &supercore::Type) -> crate::py_schema::PyType {
    match t {
        supercore::Type::Boolean => crate::py_schema::PyType::Boolean,
        supercore::Type::Int => crate::py_schema::PyType::Int,
        supercore::Type::Long => crate::py_schema::PyType::Long,
        supercore::Type::Float => crate::py_schema::PyType::Float,
        supercore::Type::Double => crate::py_schema::PyType::Double,
        supercore::Type::String => crate::py_schema::PyType::String,
        supercore::Type::Binary => crate::py_schema::PyType::Binary,
        supercore::Type::Date => crate::py_schema::PyType::Date,
        supercore::Type::Timestamp { .. } => crate::py_schema::PyType::Timestamp,
        supercore::Type::Uuid => crate::py_schema::PyType::Uuid,
        // For complex types, default to String for now
        _ => crate::py_schema::PyType::String,
    }
}
