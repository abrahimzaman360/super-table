//! # SuperTable Schema
//!
//! This module defines the schema representation for SuperTable, following
//! Iceberg's approach of using immutable field IDs that enable safe schema evolution.
//!
//! ## Design Principles
//!
//! 1. **Immutable Field IDs**: Each field has a unique ID that never changes,
//!    even if the field is renamed. This enables safe schema evolution.
//!
//! 2. **Nested Types**: Full support for complex nested structures including
//!    structs, lists, and maps.
//!
//! 3. **Rich Metadata**: Optional documentation and default values for fields.
//!
//! ## Schema Evolution
//!
//! SuperTable supports the following schema changes:
//! - Adding new optional columns
//! - Dropping columns (soft delete - ID is preserved)
//! - Renaming columns (ID stays the same)
//! - Widening types (e.g., int -> long)
//! - Making required columns optional

use serde::{Deserialize, Serialize};

/// A schema defines the structure of records in a table.
///
/// Schemas are immutable once created. Schema evolution is achieved by
/// creating new schemas and updating the table's current schema reference.
///
/// # Example
///
/// ```rust
/// use supercore::schema::{Schema, Field, Type};
///
/// let schema = Schema::builder(0)
///     .with_field(1, "id", Type::Long, true)
///     .with_field(2, "name", Type::String, true)
///     .with_field(3, "email", Type::String, false)
///     .build();
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub struct Schema {
    /// Unique identifier for this schema version.
    pub schema_id: i32,

    /// The list of fields in this schema.
    #[serde(default)]
    pub fields: Vec<Field>,

    /// Optional identifier field IDs (for tables with primary keys).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub identifier_field_ids: Vec<i32>,
}

/// A field in a schema.
///
/// Fields are identified by their `id`, which is immutable and unique within
/// a table's history. This enables safe schema evolution where fields can be
/// renamed without breaking existing data files.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub struct Field {
    /// Unique, immutable identifier for this field.
    /// IDs are never reused, even for dropped fields.
    pub id: i32,

    /// The field name. Can be changed during schema evolution.
    pub name: String,

    /// Whether the field is required (non-nullable).
    pub required: bool,

    /// The data type of this field.
    #[serde(rename = "type")]
    pub field_type: Type,

    /// Optional documentation for this field.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,

    /// Optional default value (JSON-encoded).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<serde_json::Value>,
}

impl Field {
    /// Converts this field to an Arrow field.
    pub fn to_arrow_field(&self) -> arrow::datatypes::Field {
        arrow::datatypes::Field::new(
            &self.name,
            self.field_type.to_arrow_datatype(),
            !self.required,
        )
    }
}

/// Data types supported by SuperTable.
///
/// These types are designed to be compatible with Apache Arrow and Parquet,
/// enabling zero-copy data access and efficient storage.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Type {
    /// A boolean value (true/false).
    Boolean,

    /// A 32-bit signed integer.
    Int,

    /// A 64-bit signed integer.
    Long,

    /// A 32-bit IEEE 754 floating point number.
    Float,

    /// A 64-bit IEEE 754 floating point number.
    Double,

    /// A date without time (days since 1970-01-01).
    Date,

    /// A time without date (microseconds since midnight).
    Time,

    /// A timestamp with microsecond precision.
    #[serde(rename_all = "kebab-case")]
    Timestamp {
        /// Whether the timestamp includes timezone information.
        with_timezone: bool,
    },

    /// An arbitrary-length Unicode string.
    String,

    /// A universally unique identifier (128-bit).
    Uuid,

    /// Arbitrary binary data.
    Binary,

    /// A fixed-length binary value.
    #[serde(rename_all = "kebab-case")]
    Fixed {
        /// The length in bytes.
        length: u32,
    },

    /// A fixed-precision decimal number.
    #[serde(rename_all = "kebab-case")]
    Decimal {
        /// Total number of digits.
        precision: u32,
        /// Number of digits after the decimal point.
        scale: u32,
    },

    /// A struct (nested record) type.
    #[serde(rename_all = "kebab-case")]
    Struct {
        /// The fields within this struct.
        fields: Vec<Field>,
    },

    /// A list (array) type.
    #[serde(rename_all = "kebab-case")]
    List {
        /// The element field (has its own ID).
        element: Box<Field>,
    },

    /// A map type with key-value pairs.
    #[serde(rename_all = "kebab-case")]
    Map {
        /// The key field (has its own ID).
        key: Box<Field>,
        /// The value field (has its own ID).
        value: Box<Field>,
    },
}

impl Type {
    /// Converts this type to an Arrow data type.
    pub fn to_arrow_datatype(&self) -> arrow::datatypes::DataType {
        match self {
            Type::Boolean => arrow::datatypes::DataType::Boolean,
            Type::Int => arrow::datatypes::DataType::Int32,
            Type::Long => arrow::datatypes::DataType::Int64,
            Type::Float => arrow::datatypes::DataType::Float32,
            Type::Double => arrow::datatypes::DataType::Float64,
            Type::Date => arrow::datatypes::DataType::Date32,
            Type::Time => {
                arrow::datatypes::DataType::Time64(arrow::datatypes::TimeUnit::Microsecond)
            }
            Type::Timestamp { with_timezone } => {
                let tz = if *with_timezone {
                    Some("UTC".into())
                } else {
                    None
                };
                arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, tz)
            }
            Type::String => arrow::datatypes::DataType::Utf8,
            Type::Uuid => arrow::datatypes::DataType::FixedSizeBinary(16),
            Type::Binary => arrow::datatypes::DataType::Binary,
            Type::Fixed { length } => arrow::datatypes::DataType::FixedSizeBinary(*length as i32),
            Type::Decimal { precision, scale } => {
                arrow::datatypes::DataType::Decimal128(*precision as u8, *scale as i8)
            }
            Type::Struct { fields } => {
                let arrow_fields = fields.iter().map(|f| f.to_arrow_field()).collect();
                arrow::datatypes::DataType::Struct(arrow_fields)
            }
            Type::List { element } => {
                arrow::datatypes::DataType::List(std::sync::Arc::new(element.to_arrow_field()))
            }
            Type::Map { key, value } => {
                let entries = arrow::datatypes::Field::new(
                    "entries",
                    arrow::datatypes::DataType::Struct(
                        vec![key.to_arrow_field(), value.to_arrow_field()].into(),
                    ),
                    false,
                );
                arrow::datatypes::DataType::Map(std::sync::Arc::new(entries), false)
            }
        }
    }
}

/// Builder for constructing `Schema` instances.
pub struct SchemaBuilder {
    schema_id: i32,
    fields: Vec<Field>,
    identifier_field_ids: Vec<i32>,
}

impl SchemaBuilder {
    /// Creates a new schema builder with the given schema ID.
    pub fn new(schema_id: i32) -> Self {
        Self {
            schema_id,
            fields: Vec::new(),
            identifier_field_ids: Vec::new(),
        }
    }

    /// Adds a simple field to the schema.
    pub fn with_field(
        mut self,
        id: i32,
        name: impl Into<String>,
        field_type: Type,
        required: bool,
    ) -> Self {
        self.fields.push(Field {
            id,
            name: name.into(),
            required,
            field_type,
            doc: None,
            default: None,
        });
        self
    }

    /// Adds a fully-specified field to the schema.
    pub fn with_field_full(mut self, field: Field) -> Self {
        self.fields.push(field);
        self
    }

    /// Marks fields as identifier (primary key) fields.
    pub fn with_identifier_fields(mut self, field_ids: impl IntoIterator<Item = i32>) -> Self {
        self.identifier_field_ids.extend(field_ids);
        self
    }

    /// Builds the schema.
    pub fn build(self) -> Schema {
        Schema {
            schema_id: self.schema_id,
            fields: self.fields,
            identifier_field_ids: self.identifier_field_ids,
        }
    }
}

impl Schema {
    /// Converts this schema to an Arrow schema.
    pub fn to_arrow_schema(&self) -> arrow::datatypes::Schema {
        let arrow_fields: Vec<arrow::datatypes::Field> =
            self.fields.iter().map(|f| f.to_arrow_field()).collect();
        arrow::datatypes::Schema::new(arrow_fields)
    }

    /// Converts this schema to an Arrow SchemaRef.
    pub fn to_arrow_schema_ref(&self) -> arrow::datatypes::SchemaRef {
        std::sync::Arc::new(self.to_arrow_schema())
    }

    /// Converts this schema to a DataFusion DFSchema.
    pub fn to_df_schema(&self) -> datafusion::error::Result<datafusion::common::DFSchema> {
        datafusion::common::DFSchema::try_from(self.to_arrow_schema())
    }
    /// Creates a new schema builder.
    pub fn builder(schema_id: i32) -> SchemaBuilder {
        SchemaBuilder::new(schema_id)
    }

    /// Finds a field by its ID.
    pub fn find_field(&self, field_id: i32) -> Option<&Field> {
        self.find_field_in_fields(&self.fields, field_id)
    }

    /// Finds a field by its name.
    pub fn find_field_by_name(&self, name: &str) -> Option<&Field> {
        self.fields.iter().find(|f| f.name == name)
    }

    /// Returns the highest field ID in this schema.
    pub fn highest_field_id(&self) -> i32 {
        self.highest_field_id_in(&self.fields)
    }

    /// Recursively finds a field by ID in a list of fields.
    fn find_field_in_fields<'a>(&self, fields: &'a [Field], field_id: i32) -> Option<&'a Field> {
        for field in fields {
            if field.id == field_id {
                return Some(field);
            }
            // Search nested types
            if let Some(nested) = self.find_in_nested_type(&field.field_type, field_id) {
                return Some(nested);
            }
        }
        None
    }

    /// Searches for a field ID within nested types.
    fn find_in_nested_type<'a>(&self, field_type: &'a Type, field_id: i32) -> Option<&'a Field> {
        match field_type {
            Type::Struct { fields } => self.find_field_in_fields(fields, field_id),
            Type::List { element } => {
                if element.id == field_id {
                    Some(element.as_ref())
                } else {
                    self.find_in_nested_type(&element.field_type, field_id)
                }
            }
            Type::Map { key, value } => {
                if key.id == field_id {
                    Some(key.as_ref())
                } else if value.id == field_id {
                    Some(value.as_ref())
                } else {
                    self.find_in_nested_type(&key.field_type, field_id)
                        .or_else(|| self.find_in_nested_type(&value.field_type, field_id))
                }
            }
            _ => None,
        }
    }

    /// Recursively finds the highest field ID.
    fn highest_field_id_in(&self, fields: &[Field]) -> i32 {
        let mut max_id = 0;
        for field in fields {
            max_id = max_id.max(field.id);
            max_id = max_id.max(self.highest_in_type(&field.field_type));
        }
        max_id
    }

    fn highest_in_type(&self, field_type: &Type) -> i32 {
        match field_type {
            Type::Struct { fields } => self.highest_field_id_in(fields),
            Type::List { element } => element.id.max(self.highest_in_type(&element.field_type)),
            Type::Map { key, value } => key
                .id
                .max(value.id)
                .max(self.highest_in_type(&key.field_type))
                .max(self.highest_in_type(&value.field_type)),
            _ => 0,
        }
    }
}

impl Type {
    /// Returns true if this type can be widened to the target type.
    ///
    /// Widening is allowed for:
    /// - int → long
    /// - float → double
    pub fn can_widen_to(&self, target: &Type) -> bool {
        matches!(
            (self, target),
            (Type::Int, Type::Long) | (Type::Float, Type::Double)
        )
    }

    /// Returns true if this is a primitive (non-nested) type.
    pub fn is_primitive(&self) -> bool {
        !matches!(
            self,
            Type::Struct { .. } | Type::List { .. } | Type::Map { .. }
        )
    }

    /// Returns true if this is a nested type.
    pub fn is_nested(&self) -> bool {
        !self.is_primitive()
    }
}

/// A pending schema update that can be applied to a schema to produce a new version.
pub struct SchemaUpdate {
    changes: Vec<SchemaChange>,
}

#[derive(Debug)]
enum SchemaChange {
    AddColumn {
        parent_id: Option<i32>,
        name: String,
        field_type: Type,
        required: bool,
        doc: Option<String>,
    },
    DeleteColumn {
        field_id: i32,
    },
    RenameColumn {
        field_id: i32,
        new_name: String,
    },
    UpdateType {
        field_id: i32,
        new_type: Type,
    },
    MakeOptional {
        field_id: i32,
    },
}

impl SchemaUpdate {
    pub fn new() -> Self {
        Self {
            changes: Vec::new(),
        }
    }

    /// adds a new column to the schema.
    pub fn add_column(
        mut self,
        parent_id: Option<i32>,
        name: impl Into<String>,
        field_type: Type,
        doc: Option<String>,
    ) -> Self {
        self.changes.push(SchemaChange::AddColumn {
            parent_id,
            name: name.into(),
            field_type,
            required: false, // New columns must be optional for compatibility
            doc,
        });
        self
    }

    /// Deletes a column from the schema.
    pub fn delete_column(mut self, field_id: i32) -> Self {
        self.changes.push(SchemaChange::DeleteColumn { field_id });
        self
    }

    /// Renames a column.
    pub fn rename_column(mut self, field_id: i32, new_name: impl Into<String>) -> Self {
        self.changes.push(SchemaChange::RenameColumn {
            field_id,
            new_name: new_name.into(),
        });
        self
    }

    /// Updates a column's type (must be compatible).
    pub fn update_type(mut self, field_id: i32, new_type: Type) -> Self {
        self.changes
            .push(SchemaChange::UpdateType { field_id, new_type });
        self
    }

    /// Makes a required column optional.
    pub fn make_optional(mut self, field_id: i32) -> Self {
        self.changes.push(SchemaChange::MakeOptional { field_id });
        self
    }

    /// Applies the changes to a base schema, producing a new schema.
    ///
    /// # Arguments
    ///
    /// * `base_schema` - The schema to start from
    /// * `new_schema_id` - The ID for the new schema
    /// * `next_column_id` - A mutable reference to the next available column ID setter
    pub fn apply(
        self,
        base_schema: &Schema,
        new_schema_id: i32,
        next_column_id: &mut i32,
    ) -> Result<Schema, String> {
        let mut fields = base_schema.fields.clone();

        for change in self.changes {
            match change {
                SchemaChange::AddColumn {
                    parent_id,
                    name,
                    field_type,
                    required,
                    doc,
                } => {
                    let id = *next_column_id;
                    *next_column_id += 1; // Increment for next use, including nested fields if any

                    // Note: For complex types we'd need to assign IDs to children too.
                    // Simplified here assuming primitive types for now.

                    let new_field = Field {
                        id,
                        name,
                        required,
                        field_type,
                        doc,
                        default: None,
                    };

                    if let Some(pid) = parent_id {
                        // Find parent and add to its children
                        Self::add_field_recursive(&mut fields, pid, new_field)?;
                    } else {
                        // Add to root
                        fields.push(new_field);
                    }
                }
                SchemaChange::DeleteColumn { field_id } => {
                    Self::delete_field_recursive(&mut fields, field_id);
                }
                SchemaChange::RenameColumn { field_id, new_name } => {
                    Self::rename_field_recursive(&mut fields, field_id, &new_name)?;
                }
                SchemaChange::UpdateType { field_id, new_type } => {
                    Self::update_type_recursive(&mut fields, field_id, new_type)?;
                }
                SchemaChange::MakeOptional { field_id } => {
                    Self::make_optional_recursive(&mut fields, field_id)?;
                }
            }
        }

        Ok(Schema {
            schema_id: new_schema_id,
            fields,
            identifier_field_ids: base_schema.identifier_field_ids.clone(),
        })
    }

    fn add_field_recursive(
        fields: &mut Vec<Field>,
        parent_id: i32,
        new_field: Field,
    ) -> Result<(), String> {
        for field in fields {
            if field.id == parent_id {
                match &mut field.field_type {
                    Type::Struct { fields: children } => {
                        children.push(new_field);
                        return Ok(());
                    }
                    _ => return Err(format!("Parent field {} is not a struct", parent_id)),
                }
            }
            // Recurse into struct fields
            if let Type::Struct { fields: children } = &mut field.field_type {
                if Self::add_field_recursive(children, parent_id, new_field.clone()).is_ok() {
                    return Ok(());
                }
            }
        }
        Err(format!("Parent field {} not found", parent_id))
    }

    fn delete_field_recursive(fields: &mut Vec<Field>, target_id: i32) {
        fields.retain(|f| f.id != target_id);
        for field in fields {
            if let Type::Struct { fields: children } = &mut field.field_type {
                Self::delete_field_recursive(children, target_id);
            }
        }
    }

    fn rename_field_recursive(
        fields: &mut [Field],
        target_id: i32,
        new_name: &str,
    ) -> Result<(), String> {
        for field in fields {
            if field.id == target_id {
                field.name = new_name.to_string();
                return Ok(());
            }
            if let Type::Struct { fields: children } = &mut field.field_type {
                if Self::rename_field_recursive(children, target_id, new_name).is_ok() {
                    return Ok(());
                }
            }
        }
        Err(format!("Field {} not found", target_id))
    }

    fn update_type_recursive(
        fields: &mut [Field],
        target_id: i32,
        new_type: Type,
    ) -> Result<(), String> {
        for field in fields {
            if field.id == target_id {
                if !field.field_type.can_widen_to(&new_type) {
                    return Err(format!(
                        "Cannot change type {:?} to {:?} for base column {}",
                        field.field_type, new_type, target_id
                    ));
                }
                field.field_type = new_type;
                return Ok(());
            }
            if let Type::Struct { fields: children } = &mut field.field_type {
                if Self::update_type_recursive(children, target_id, new_type.clone()).is_ok() {
                    return Ok(());
                }
            }
        }
        Err(format!("Field {} not found", target_id))
    }

    fn make_optional_recursive(fields: &mut [Field], target_id: i32) -> Result<(), String> {
        for field in fields {
            if field.id == target_id {
                field.required = false;
                return Ok(());
            }
            if let Type::Struct { fields: children } = &mut field.field_type {
                if Self::make_optional_recursive(children, target_id).is_ok() {
                    return Ok(());
                }
            }
        }
        Err(format!("Field {} not found", target_id))
    }
}

impl Default for SchemaUpdate {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_builder() {
        let schema = Schema::builder(0)
            .with_field(1, "id", Type::Long, true)
            .with_field(2, "name", Type::String, false)
            .build();

        assert_eq!(schema.schema_id, 0);
        assert_eq!(schema.fields.len(), 2);
        assert_eq!(schema.fields[0].name, "id");
        assert!(schema.fields[0].required);
    }

    #[test]
    fn test_find_field() {
        let schema = Schema::builder(0)
            .with_field(1, "id", Type::Long, true)
            .with_field(2, "name", Type::String, false)
            .build();

        let field = schema.find_field(2).unwrap();
        assert_eq!(field.name, "name");
    }

    #[test]
    fn test_type_widening() {
        assert!(Type::Int.can_widen_to(&Type::Long));
        assert!(Type::Float.can_widen_to(&Type::Double));
        assert!(!Type::Long.can_widen_to(&Type::Int));
        assert!(!Type::String.can_widen_to(&Type::Int));
    }

    #[test]
    fn test_nested_struct() {
        let address_fields = vec![
            Field {
                id: 10,
                name: "street".into(),
                required: true,
                field_type: Type::String,
                doc: None,
                default: None,
            },
            Field {
                id: 11,
                name: "city".into(),
                required: true,
                field_type: Type::String,
                doc: None,
                default: None,
            },
        ];

        let schema = Schema::builder(0)
            .with_field(1, "id", Type::Long, true)
            .with_field_full(Field {
                id: 2,
                name: "address".into(),
                required: false,
                field_type: Type::Struct {
                    fields: address_fields,
                },
                doc: None,
                default: None,
            })
            .build();

        // Should find nested field
        let street_field = schema.find_field(10).unwrap();
        assert_eq!(street_field.name, "street");

        // Highest ID should be 11
        assert_eq!(schema.highest_field_id(), 11);
    }

    #[test]
    fn test_serialization() {
        let schema = Schema::builder(0)
            .with_field(1, "id", Type::Long, true)
            .with_field(
                2,
                "amount",
                Type::Decimal {
                    precision: 10,
                    scale: 2,
                },
                false,
            )
            .build();

        let json = serde_json::to_string_pretty(&schema).unwrap();
        let deserialized: Schema = serde_json::from_str(&json).unwrap();

        assert_eq!(schema, deserialized);
    }

    #[test]
    fn test_schema_evolution() {
        let schema = Schema::builder(0)
            .with_field(1, "id", Type::Long, true)
            .with_field(2, "data", Type::String, false)
            .build();

        let mut next_col_id = 3;

        // Add a column
        let update = SchemaUpdate::new().add_column(None, "new_col", Type::Boolean, None);

        let new_schema = update.apply(&schema, 1, &mut next_col_id).unwrap();

        assert_eq!(new_schema.fields.len(), 3);
        assert_eq!(new_schema.fields[2].name, "new_col");
        assert_eq!(new_schema.fields[2].id, 3);
        assert_eq!(next_col_id, 4);

        // Rename a column
        let update = SchemaUpdate::new().rename_column(2, "renamed_data");

        let final_schema = update.apply(&new_schema, 2, &mut next_col_id).unwrap();

        assert_eq!(final_schema.fields[1].name, "renamed_data");
        assert_eq!(final_schema.fields[1].id, 2);
    }
}
