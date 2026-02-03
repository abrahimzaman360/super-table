use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use thiserror::Error;

/// Errors that can occur during validation.
#[derive(Debug, Error)]
pub enum ValidationError {
    #[error("schema mismatch: expected {expected}, found {found}")]
    SchemaMismatch { expected: String, found: String },

    #[error("null constraint violation: column {column} is not nullable but found nulls")]
    NotNullViolation { column: String },

    #[error("compatibility error: {message}")]
    CompatibilityError { message: String },
}

/// Validates record batches against a target schema.
pub struct SchemaValidator {
    target_schema: SchemaRef,
}

impl SchemaValidator {
    pub fn new(target_schema: SchemaRef) -> Self {
        Self { target_schema }
    }

    /// Validates a RecordBatch against the target schema.
    pub fn validate(&self, batch: &RecordBatch) -> Result<(), ValidationError> {
        // 1. Check if schemas are compatible
        if batch.schema() != self.target_schema {
            // Deep check: sometimes the metadata differs but fields are same
            if !self.schemas_are_compatible(&batch.schema(), &self.target_schema) {
                return Err(ValidationError::SchemaMismatch {
                    expected: format!("{:?}", self.target_schema),
                    found: format!("{:?}", batch.schema()),
                });
            }
        }

        // 2. Check for null constraints
        for field in self.target_schema.fields() {
            if !field.is_nullable() {
                let column = batch.column_by_name(field.name()).ok_or_else(|| {
                    ValidationError::SchemaMismatch {
                        expected: field.name().to_string(),
                        found: "missing column".to_string(),
                    }
                })?;

                if column.null_count() > 0 {
                    return Err(ValidationError::NotNullViolation {
                        column: field.name().to_string(),
                    });
                }
            }
        }

        Ok(())
    }

    fn schemas_are_compatible(&self, s1: &SchemaRef, s2: &SchemaRef) -> bool {
        if s1.fields().len() != s2.fields().len() {
            return false;
        }

        for (f1, f2) in s1.fields().iter().zip(s2.fields().iter()) {
            if f1.name() != f2.name() || f1.data_type() != f2.data_type() {
                return false;
            }
        }

        true
    }
}

/// Validates if a new schema is compatible with an old schema.
pub struct SchemaCompatibilityValidator {
    old_schema: crate::schema::Schema,
}

impl SchemaCompatibilityValidator {
    pub fn new(old_schema: crate::schema::Schema) -> Self {
        Self { old_schema }
    }

    /// Validates if new_schema can safely evolve from old_schema.
    pub fn validate(&self, new_schema: &crate::schema::Schema) -> Result<(), ValidationError> {
        for old_field in &self.old_schema.fields {
            let new_field = match new_schema.find_field(old_field.id) {
                Some(f) => f,
                None => continue, // Dropping columns is generally allowed in Iceberg
            };

            // 1. Type widening check
            if old_field.field_type != new_field.field_type {
                if !old_field.field_type.can_widen_to(&new_field.field_type) {
                    return Err(ValidationError::CompatibilityError {
                        message: format!(
                            "Cannot change type of field {} from {:?} to {:?}",
                            old_field.name, old_field.field_type, new_field.field_type
                        ),
                    });
                }
            }

            // 2. Nullability check: Cannot make an optional field required
            if !old_field.required && new_field.required {
                return Err(ValidationError::CompatibilityError {
                    message: format!("Cannot make optional field {} required", old_field.name),
                });
            }
        }

        // 3. New fields must be optional
        for new_field in &new_schema.fields {
            if self.old_schema.find_field(new_field.id).is_none() && new_field.required {
                return Err(ValidationError::CompatibilityError {
                    message: format!("New field {} must be optional", new_field.name),
                });
            }
        }

        Ok(())
    }
}
