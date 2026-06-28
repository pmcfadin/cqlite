//! Schema validation for discovery.
//!
//! Houses the [`SchemaValidator`] marker type and the validation/confidence
//! helpers used by the discovery engine when assembling a [`SchemaInfo`].

use crate::types::Value;

use super::model::{
    ColumnDefinition, TypeInfo, ValidationStatus, ValidationWarning, ValidationWarningType,
};
use super::SchemaDiscoveryEngine;

/// Schema validator for consistency checking
#[derive(Debug)]
pub struct SchemaValidator {
    // Implementation details for validation
}

impl SchemaValidator {
    pub(super) fn new() -> Self {
        Self {}
    }
}

// Validation helpers used while building schema info.
impl SchemaDiscoveryEngine {
    /// Calculate confidence score for type inference
    pub(super) fn calculate_type_confidence(&self, samples: &[Value], type_info: &TypeInfo) -> f64 {
        if samples.is_empty() {
            return 0.0;
        }

        let matching_samples = samples
            .iter()
            .filter(|sample| self.value_matches_type(sample, type_info))
            .count();

        matching_samples as f64 / samples.len() as f64
    }

    /// Check if a value matches the expected type
    fn value_matches_type(&self, value: &Value, type_info: &TypeInfo) -> bool {
        #[allow(clippy::match_like_matches_macro)]
        match (value, type_info.type_id.as_str()) {
            (Value::Text(_), "text") => true,
            (Value::Integer(_), "int") => true,
            (Value::BigInt(_), "bigint") => true,
            (Value::Float(_), "double") => true,
            (Value::Boolean(_), "boolean") => true,
            (Value::Uuid(_), "uuid") => true,
            (Value::Timestamp(_), "timestamp") => true,
            (Value::Blob(_), "blob") => true,
            (Value::List(_), "list") => true,
            (Value::Set(_), "set") => true,
            (Value::Map(_), "map") => true,
            (Value::Tuple(_), "tuple") => true,
            (Value::Udt(_), "udt") => true,
            _ => false,
        }
    }

    /// Determine overall validation status
    pub(super) fn determine_validation_status(
        &self,
        partition_key: &[ColumnDefinition],
        regular_columns: &[ColumnDefinition],
    ) -> ValidationStatus {
        // Check for basic requirements
        if partition_key.is_empty() {
            return ValidationStatus::Invalid;
        }

        // Check confidence levels
        let all_columns: Vec<_> = partition_key.iter().chain(regular_columns.iter()).collect();
        let low_confidence_count = all_columns
            .iter()
            .filter(|col| col.confidence < 0.7)
            .count();

        if low_confidence_count > all_columns.len() / 2 {
            ValidationStatus::Invalid
        } else if low_confidence_count > 0 {
            ValidationStatus::ValidWithWarnings
        } else {
            ValidationStatus::Valid
        }
    }

    /// Generate validation warnings
    pub(super) fn generate_validation_warnings(
        &self,
        _partition_key: &[ColumnDefinition],
        regular_columns: &[ColumnDefinition],
    ) -> Vec<ValidationWarning> {
        let mut warnings = Vec::new();

        // Check for low confidence type inference
        for column in regular_columns {
            if column.confidence < 0.7 {
                warnings.push(ValidationWarning {
                    warning_type: ValidationWarningType::LowConfidence,
                    message: format!(
                        "Low confidence type inference for column '{}': {:.2}",
                        column.name, column.confidence
                    ),
                    component: Some(column.name.clone()),
                });
            }
        }

        warnings
    }
}
