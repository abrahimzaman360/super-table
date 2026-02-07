//! Python bindings for SuperTable schema types.

use pyo3::prelude::*;

/// A data type for SuperTable columns.
#[pyclass(eq, eq_int)]
#[derive(Clone, Debug, PartialEq)]
pub enum PyType {
    Boolean,
    Int,
    Long,
    Float,
    Double,
    String,
    Binary,
    Date,
    Timestamp,
    Uuid,
}

impl PyType {
    /// Converts to the Rust Type enum.
    pub fn to_rust_type(&self) -> supercore::Type {
        match self {
            Self::Boolean => supercore::Type::Boolean,
            Self::Int => supercore::Type::Int,
            Self::Long => supercore::Type::Long,
            Self::Float => supercore::Type::Float,
            Self::Double => supercore::Type::Double,
            Self::String => supercore::Type::String,
            Self::Binary => supercore::Type::Binary,
            Self::Date => supercore::Type::Date,
            Self::Timestamp => supercore::Type::Timestamp {
                with_timezone: false,
            },
            Self::Uuid => supercore::Type::Uuid,
        }
    }
}

/// A field in a SuperTable schema.
#[pyclass]
#[derive(Clone)]
pub struct PyField {
    #[pyo3(get)]
    pub id: i32,
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub field_type: PyType,
    #[pyo3(get)]
    pub required: bool,
    #[pyo3(get)]
    pub doc: Option<String>,
}

#[pymethods]
impl PyField {
    /// Creates a new field.
    ///
    /// Args:
    ///     id: Unique field identifier
    ///     name: Field name
    ///     field_type: The data type
    ///     required: Whether the field is required (non-nullable)
    ///     doc: Optional documentation string
    #[new]
    #[pyo3(signature = (id, name, field_type, required=true, doc=None))]
    fn new(id: i32, name: String, field_type: PyType, required: bool, doc: Option<String>) -> Self {
        Self {
            id,
            name,
            field_type,
            required,
            doc,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "Field(id={}, name='{}', required={})",
            self.id, self.name, self.required
        )
    }
}

impl PyField {
    /// Converts to the Rust Field type.
    pub fn to_rust_field(&self) -> supercore::Field {
        supercore::Field {
            id: self.id,
            name: self.name.clone(),
            required: self.required,
            field_type: self.field_type.to_rust_type(),
            doc: self.doc.clone(),
            default: None,
        }
    }
}

/// A SuperTable schema defining the structure of records.
#[pyclass]
#[derive(Clone)]
pub struct PySchema {
    #[pyo3(get)]
    pub schema_id: i32,
    fields: Vec<PyField>,
}

#[pymethods]
impl PySchema {
    /// Creates a new schema.
    ///
    /// Args:
    ///     fields: List of Field objects
    ///     schema_id: Optional schema ID (default: 0)
    #[new]
    #[pyo3(signature = (fields, schema_id=0))]
    pub fn new(fields: Vec<PyField>, schema_id: i32) -> Self {
        Self { schema_id, fields }
    }

    /// Returns the list of fields in this schema.
    #[getter]
    fn fields(&self) -> Vec<PyField> {
        self.fields.clone()
    }

    /// Returns the number of fields.
    fn __len__(&self) -> usize {
        self.fields.len()
    }

    /// Gets a field by name.
    fn field(&self, name: &str) -> Option<PyField> {
        self.fields.iter().find(|f| f.name == name).cloned()
    }

    fn __repr__(&self) -> String {
        let field_names: Vec<_> = self.fields.iter().map(|f| f.name.as_str()).collect();
        format!(
            "Schema(id={}, fields=[{}])",
            self.schema_id,
            field_names.join(", ")
        )
    }
}

impl PySchema {
    /// Converts to the Rust Schema type.
    pub fn to_rust_schema(&self) -> supercore::Schema {
        let mut builder = supercore::Schema::builder(self.schema_id);

        for field in &self.fields {
            builder = builder.with_field(
                field.id,
                &field.name,
                field.field_type.to_rust_type(),
                field.required,
            );
        }

        builder.build()
    }
}
