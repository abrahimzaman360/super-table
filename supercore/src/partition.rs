//! # SuperTable Partitioning
//!
//! This module defines the partitioning strategy for SuperTable, supporting
//! **Hidden Partitioning**.
//!
//! ## Hidden Partitioning
//!
//! Unlike Hive-style partitioning where users must explicitly query partition columns
//! (e.g., `WHERE year = 2024`), SuperTable tracks the relationship between
//! data columns and partition values. Users query the data column (e.g., `WHERE timestamp > ...`)
//! and the query engine automatically prunes partitions based on the transform.
//!
//! ## Transforms
//!
//! Supported transforms:
//! - **Identity**: Value is used as-is (e.g., `category`)
//! - **Bucket(N)**: Hash of value % N (e.g., `bucket(user_id, 16)`)
//! - **Truncate(W)**: Truncate string/binary to width W (e.g., `truncate(name, 1)`)
//! - **Year/Month/Day/Hour**: Extract date/time component

use serde::{Deserialize, Serialize};

use crate::schema::{Schema, Type};

/// A partition spec defines how a table is partitioned.
///
/// Use `PartitionSpec::builder()` to create new specs.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub struct PartitionSpec {
    /// Unique identifier for this partition spec.
    pub spec_id: i32,

    /// The list of fields that make up the partition tuple.
    pub fields: Vec<PartitionField>,
}

/// A field in a partition spec.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub struct PartitionField {
    /// The source column ID from the table schema.
    pub source_id: i32,

    /// A unique ID for this partition field within the spec.
    pub field_id: i32,

    /// A name for the partition field.
    pub name: String,

    /// The transform applied to the source column.
    pub transform: Transform,
}

/// Supported partition transforms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Transform {
    /// Use the value as-is.
    Identity,
    /// Hash implementation (murmur3 32-bit recommended) % N.
    Bucket(u32),
    /// Truncate to width W.
    Truncate(u32),
    /// Extract year from date/timestamp.
    Year,
    /// Extract month from date/timestamp.
    Month,
    /// Extract day from date/timestamp.
    Day,
    /// Extract hour from timestamp.
    Hour,
    /// Always null (used for evolution when dropping a partition field).
    Void,
}

impl PartitionSpec {
    pub fn builder(schema: &Schema) -> PartitionSpecBuilder<'_> {
        PartitionSpecBuilder::new(schema)
    }

    /// Returns true if the table is unpartitioned.
    pub fn is_unpartitioned(&self) -> bool {
        self.fields.is_empty()
    }
}

pub struct PartitionSpecBuilder<'a> {
    schema: &'a Schema,
    fields: Vec<PartitionField>,
    next_field_id: i32,
}

impl<'a> PartitionSpecBuilder<'a> {
    pub fn new(schema: &'a Schema) -> Self {
        Self {
            schema,
            fields: Vec::new(),
            next_field_id: 1000, // Partition field IDs usually start high to avoid collision
        }
    }

    pub fn add_identity(self, source_name: &str) -> Result<Self, String> {
        self.add_field(source_name, Transform::Identity, None)
    }

    pub fn add_bucket(self, source_name: &str, num_buckets: u32) -> Result<Self, String> {
        let name = format!("{}_bucket_{}", source_name, num_buckets);
        self.add_field(source_name, Transform::Bucket(num_buckets), Some(&name))
    }

    pub fn add_truncate(self, source_name: &str, width: u32) -> Result<Self, String> {
        let name = format!("{}_trunc_{}", source_name, width);
        self.add_field(source_name, Transform::Truncate(width), Some(&name))
    }

    pub fn add_year(self, source_name: &str) -> Result<Self, String> {
        let name = format!("{}_year", source_name);
        self.add_field(source_name, Transform::Year, Some(&name))
    }

    pub fn add_month(self, source_name: &str) -> Result<Self, String> {
        let name = format!("{}_month", source_name);
        self.add_field(source_name, Transform::Month, Some(&name))
    }

    pub fn add_day(self, source_name: &str) -> Result<Self, String> {
        let name = format!("{}_day", source_name);
        self.add_field(source_name, Transform::Day, Some(&name))
    }

    pub fn add_hour(self, source_name: &str) -> Result<Self, String> {
        let name = format!("{}_hour", source_name);
        self.add_field(source_name, Transform::Hour, Some(&name))
    }

    fn add_field(
        mut self,
        source_name: &str,
        transform: Transform,
        rename: Option<&str>,
    ) -> Result<Self, String> {
        let source_field = self
            .schema
            .find_field_by_name(source_name)
            .ok_or_else(|| format!("Column not found: {}", source_name))?;

        // Validate transform compatibility
        if !transform.can_apply_to(&source_field.field_type) {
            return Err(format!(
                "Cannot apply {:?} to column {} of type {:?}",
                transform, source_name, source_field.field_type
            ));
        }

        let name = rename.unwrap_or(&source_field.name).to_string();
        let field_id = self.next_field_id;
        self.next_field_id += 1;

        self.fields.push(PartitionField {
            source_id: source_field.id,
            field_id,
            name,
            transform,
        });

        Ok(self)
    }

    pub fn build(self) -> PartitionSpec {
        PartitionSpec {
            spec_id: 0, // Should be assigned by TableMetadata
            fields: self.fields,
        }
    }
}

impl Transform {
    /// Returns true if this transform can be applied to the given type.
    pub fn can_apply_to(&self, _type: &Type) -> bool {
        match self {
            Transform::Identity => true,  // Can partition by anything
            Transform::Bucket(_) => true, // Can hash anything
            Transform::Truncate(_) => matches!(_type, Type::String | Type::Binary),
            Transform::Year | Transform::Month | Transform::Day => {
                matches!(_type, Type::Date | Type::Timestamp { .. })
            }
            Transform::Hour => matches!(_type, Type::Timestamp { .. }),
            Transform::Void => true,
        }
    }

    /// Returns the result type of the transform.
    pub fn result_type(&self, source_type: &Type) -> Type {
        match self {
            Transform::Identity => source_type.clone(),
            Transform::Bucket(_) => Type::Int,
            Transform::Truncate(_) => source_type.clone(),
            Transform::Year | Transform::Month | Transform::Day | Transform::Hour => Type::Int,
            Transform::Void => source_type.clone(), // Or Null?
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_schema() -> Schema {
        Schema::builder(0)
            .with_field(1, "id", Type::Long, true)
            .with_field(
                2,
                "ts",
                Type::Timestamp {
                    with_timezone: true,
                },
                false,
            )
            .with_field(3, "category", Type::String, false)
            .build()
    }

    #[test]
    fn test_partition_builder() {
        let schema = sample_schema();
        let spec = PartitionSpec::builder(&schema)
            .add_day("ts")
            .unwrap()
            .add_identity("category")
            .unwrap()
            .add_bucket("id", 16)
            .unwrap()
            .build();

        assert_eq!(spec.fields.len(), 3);
        assert_eq!(spec.fields[0].transform, Transform::Day);
        assert_eq!(spec.fields[0].name, "ts_day");
        assert_eq!(spec.fields[1].transform, Transform::Identity);
        assert_eq!(spec.fields[2].transform, Transform::Bucket(16));
    }

    #[test]
    fn test_invalid_transform() {
        let schema = sample_schema();
        let result = PartitionSpec::builder(&schema).add_hour("category"); // String cannot be hour-partitioned

        assert!(result.is_err());
    }
}
