//! Type inference engine for schema discovery.
//!
//! Infers CQL type information for columns from sampled values, including
//! recursive handling of collections, tuples, and UDTs.

use std::collections::HashMap;

use crate::{types::Value, Result};

use super::model::{TypeInfo, UdtFieldInfo};

/// Type inference engine for complex type analysis
#[derive(Debug)]
pub struct TypeInferenceEngine {
    // Implementation details for type inference
}

impl TypeInferenceEngine {
    pub(super) fn new() -> Self {
        Self {}
    }

    /// Infer column type from sample values
    #[allow(dead_code)]
    pub(super) fn infer_column_type<'a>(
        &'a self,
        samples: &'a [Value],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<TypeInfo>> + Send + 'a>> {
        Box::pin(async move {
            if samples.is_empty() {
                return Ok(TypeInfo {
                    type_id: "text".to_string(),
                    type_params: vec![],
                    is_frozen: false,
                    element_type: None,
                    key_type: None,
                    value_type: None,
                    udt_fields: None,
                    tuple_elements: None,
                });
            }

            // Count occurrences of each type
            let mut type_counts = HashMap::new();
            let mut has_complex_types = false;

            for sample in samples {
                let type_name = self.get_value_type_name(sample);
                *type_counts.entry(type_name.clone()).or_insert(0) += 1;

                // Check for complex types that need special handling
                if matches!(
                    sample,
                    Value::List(_)
                        | Value::Set(_)
                        | Value::Map(_)
                        | Value::Tuple(_)
                        | Value::Udt(_)
                ) {
                    has_complex_types = true;
                }
            }

            // Find the most common type
            let most_common_type = type_counts
                .iter()
                .max_by_key(|(_, count)| *count)
                .map(|(type_name, _)| type_name.clone())
                .unwrap_or_else(|| "text".to_string());

            // Handle complex types
            if has_complex_types {
                return self.infer_complex_type(samples, &most_common_type).await;
            }

            // Handle simple types
            Ok(TypeInfo {
                type_id: self.normalize_type_name(&most_common_type),
                type_params: vec![],
                is_frozen: false,
                element_type: None,
                key_type: None,
                value_type: None,
                udt_fields: None,
                tuple_elements: None,
            })
        })
    }

    /// Get type name for a Value
    fn get_value_type_name(&self, value: &Value) -> String {
        match value {
            Value::Null => "null".to_string(),
            Value::Text(_) => "text".to_string(),
            Value::Integer(_) => "int".to_string(),
            Value::BigInt(_) => "bigint".to_string(),
            Value::Counter(_) => "counter".to_string(),
            Value::Float(_) => "double".to_string(),
            Value::Boolean(_) => "boolean".to_string(),
            Value::Uuid(_) => "uuid".to_string(),
            Value::Timestamp(_) => "timestamp".to_string(),
            Value::Date(_) => "date".to_string(),
            Value::Time(_) => "time".to_string(),
            Value::Inet(_) => "inet".to_string(),
            Value::Blob(_) => "blob".to_string(),
            Value::List(_) => "list".to_string(),
            Value::Set(_) => "set".to_string(),
            Value::Map(_) => "map".to_string(),
            Value::Json(_) => "text".to_string(), // JSON stored as text
            Value::TinyInt(_) => "tinyint".to_string(),
            Value::SmallInt(_) => "smallint".to_string(),
            Value::Float32(_) => "float".to_string(),
            Value::Tuple(_) => "tuple".to_string(),
            Value::Udt(_) => "udt".to_string(),
            Value::Frozen(_) => "frozen".to_string(),
            Value::Varint(_) => "varint".to_string(),
            Value::Decimal { .. } => "decimal".to_string(),
            Value::Duration { .. } => "duration".to_string(),
            Value::Tombstone(_) => "tombstone".to_string(),
            // The sentinel carries its declared type, so inference reports the
            // REAL CQL type name rather than a placeholder (issue #3805).
            Value::Empty(ty) => ty.cql_name().to_string(),
        }
    }

    /// Normalize type name to CQL standard
    fn normalize_type_name(&self, type_name: &str) -> String {
        match type_name.to_lowercase().as_str() {
            "int" | "integer" => "int".to_string(),
            "bigint" | "biginteger" => "bigint".to_string(),
            "double" | "float64" => "double".to_string(),
            "float" | "float32" => "float".to_string(),
            "text" | "varchar" | "string" => "text".to_string(),
            "bool" | "boolean" => "boolean".to_string(),
            "timestamp" | "datetime" => "timestamp".to_string(),
            "blob" | "bytes" => "blob".to_string(),
            "uuid" => "uuid".to_string(),
            "decimal" => "decimal".to_string(),
            "varint" => "varint".to_string(),
            "tinyint" => "tinyint".to_string(),
            "smallint" => "smallint".to_string(),
            "duration" => "duration".to_string(),
            _ => type_name.to_string(),
        }
    }

    /// Infer complex type information
    async fn infer_complex_type(&self, samples: &[Value], base_type: &str) -> Result<TypeInfo> {
        match base_type {
            "list" => self.infer_list_type(samples).await,
            "set" => self.infer_set_type(samples).await,
            "map" => self.infer_map_type(samples).await,
            "tuple" => self.infer_tuple_type(samples).await,
            "udt" => self.infer_udt_type(samples).await,
            _ => Ok(TypeInfo {
                type_id: self.normalize_type_name(base_type),
                type_params: vec![],
                is_frozen: false,
                element_type: None,
                key_type: None,
                value_type: None,
                udt_fields: None,
                tuple_elements: None,
            }),
        }
    }

    /// Infer list type from samples
    async fn infer_list_type(&self, samples: &[Value]) -> Result<TypeInfo> {
        let mut element_samples = Vec::new();

        for sample in samples {
            if let Value::List(elements) = sample {
                element_samples.extend(elements.iter().cloned());
            }
        }

        let element_type = if !element_samples.is_empty() {
            Box::new(self.infer_column_type(&element_samples).await?)
        } else {
            Box::new(TypeInfo {
                type_id: "text".to_string(),
                type_params: vec![],
                is_frozen: false,
                element_type: None,
                key_type: None,
                value_type: None,
                udt_fields: None,
                tuple_elements: None,
            })
        };

        Ok(TypeInfo {
            type_id: "list".to_string(),
            type_params: vec![element_type.type_id.clone()],
            is_frozen: false,
            element_type: Some(element_type),
            key_type: None,
            value_type: None,
            udt_fields: None,
            tuple_elements: None,
        })
    }

    /// Infer set type from samples
    async fn infer_set_type(&self, samples: &[Value]) -> Result<TypeInfo> {
        let mut element_samples = Vec::new();

        for sample in samples {
            if let Value::Set(elements) = sample {
                element_samples.extend(elements.iter().cloned());
            }
        }

        let element_type = if !element_samples.is_empty() {
            Box::new(self.infer_column_type(&element_samples).await?)
        } else {
            Box::new(TypeInfo {
                type_id: "text".to_string(),
                type_params: vec![],
                is_frozen: false,
                element_type: None,
                key_type: None,
                value_type: None,
                udt_fields: None,
                tuple_elements: None,
            })
        };

        Ok(TypeInfo {
            type_id: "set".to_string(),
            type_params: vec![element_type.type_id.clone()],
            is_frozen: false,
            element_type: Some(element_type),
            key_type: None,
            value_type: None,
            udt_fields: None,
            tuple_elements: None,
        })
    }

    /// Infer map type from samples
    async fn infer_map_type(&self, samples: &[Value]) -> Result<TypeInfo> {
        let mut key_samples = Vec::new();
        let mut value_samples = Vec::new();

        for sample in samples {
            if let Value::Map(map) = sample {
                for (key, value) in map {
                    key_samples.push(key.clone());
                    value_samples.push(value.clone());
                }
            }
        }

        let key_type = if !key_samples.is_empty() {
            Box::new(self.infer_column_type(&key_samples).await?)
        } else {
            Box::new(TypeInfo {
                type_id: "text".to_string(),
                type_params: vec![],
                is_frozen: false,
                element_type: None,
                key_type: None,
                value_type: None,
                udt_fields: None,
                tuple_elements: None,
            })
        };

        let value_type = if !value_samples.is_empty() {
            Box::new(self.infer_column_type(&value_samples).await?)
        } else {
            Box::new(TypeInfo {
                type_id: "text".to_string(),
                type_params: vec![],
                is_frozen: false,
                element_type: None,
                key_type: None,
                value_type: None,
                udt_fields: None,
                tuple_elements: None,
            })
        };

        Ok(TypeInfo {
            type_id: "map".to_string(),
            type_params: vec![key_type.type_id.clone(), value_type.type_id.clone()],
            is_frozen: false,
            element_type: None,
            key_type: Some(key_type),
            value_type: Some(value_type),
            udt_fields: None,
            tuple_elements: None,
        })
    }

    /// Infer tuple type from samples
    async fn infer_tuple_type(&self, samples: &[Value]) -> Result<TypeInfo> {
        let mut max_elements = 0;
        let mut element_positions: Vec<Vec<Value>> = Vec::new();

        // Collect samples by position
        for sample in samples {
            if let Value::Tuple(elements) = sample {
                max_elements = max_elements.max(elements.len());

                // Ensure we have enough position vectors
                while element_positions.len() < elements.len() {
                    element_positions.push(Vec::new());
                }

                // Add elements to their position vectors
                for (i, element) in elements.iter().enumerate() {
                    element_positions[i].push(element.clone());
                }
            }
        }

        // Infer type for each position
        let mut tuple_elements = Vec::new();
        for position_samples in element_positions {
            if !position_samples.is_empty() {
                let element_type = self.infer_column_type(&position_samples).await?;
                tuple_elements.push(element_type);
            }
        }

        let type_params: Vec<String> = tuple_elements.iter().map(|t| t.type_id.clone()).collect();

        Ok(TypeInfo {
            type_id: "tuple".to_string(),
            type_params,
            is_frozen: false,
            element_type: None,
            key_type: None,
            value_type: None,
            udt_fields: None,
            tuple_elements: Some(tuple_elements),
        })
    }

    /// Infer UDT type from samples (basic implementation)
    async fn infer_udt_type(&self, samples: &[Value]) -> Result<TypeInfo> {
        // UDT inference requires more sophisticated analysis
        // This is a basic implementation - would need expansion for production
        let mut field_map: HashMap<String, Vec<Value>> = HashMap::new();

        for sample in samples {
            if let Value::Udt(udt_value) = sample {
                for field in &udt_value.fields {
                    if let Some(field_value) = &field.value {
                        field_map
                            .entry(field.name.clone())
                            .or_default()
                            .push(field_value.clone());
                    }
                }
            }
        }

        let mut udt_fields = Vec::new();
        for (field_name, field_samples) in field_map {
            let field_type = self.infer_column_type(&field_samples).await?;
            udt_fields.push(UdtFieldInfo {
                name: field_name,
                field_type,
                nullable: true, // Assume nullable for safety
            });
        }

        Ok(TypeInfo {
            type_id: "udt".to_string(),
            type_params: vec![],
            is_frozen: false,
            element_type: None,
            key_type: None,
            value_type: None,
            udt_fields: Some(udt_fields),
            tuple_elements: None,
        })
    }
}
