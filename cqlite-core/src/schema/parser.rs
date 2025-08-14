//! Schema-driven parser for CQLite
//!
//! This module provides strictly schema-driven parsing without any type guessing or fallback
//! behavior. All parsing operations require explicit schema context.

use crate::{
    Error, Result,
    schema::{CqlType, registry::ParsingContext},
    types::{ComparatorType, Value},
};
use std::collections::HashMap;

/// Schema-driven parser that requires explicit schema context for all operations
#[derive(Debug)]
pub struct SchemaParser {
    /// Parsing context containing schema and comparators
    context: ParsingContext,
}

impl SchemaParser {
    /// Create a new schema-driven parser
    pub fn new(context: ParsingContext) -> Result<Self> {
        if !context.is_complete() {
            return Err(Error::Schema(
                "Incomplete parsing context: schema must be fully defined".to_string(),
            ));
        }
        Ok(Self { context })
    }

    /// Parse a partition key using the schema's partition key comparators
    pub fn parse_partition_key(&self, data: &[u8]) -> Result<Vec<Value>> {
        if self.context.partition_comparators.is_empty() {
            return Err(Error::Schema(
                "No partition key comparators defined in schema".to_string(),
            ));
        }

        let mut values = Vec::new();
        let mut offset = 0;

        for (idx, comparator) in self.context.partition_comparators.iter().enumerate() {
            let key_column = &self.context.schema.partition_keys[idx];
            let (value, consumed) = self.parse_value_with_comparator(
                &data[offset..],
                comparator,
                &key_column.data_type,
            )?;
            values.push(value);
            offset += consumed;
        }

        Ok(values)
    }

    /// Parse clustering keys using the schema's clustering key comparators
    pub fn parse_clustering_keys(&self, data: &[u8]) -> Result<Vec<Value>> {
        if self.context.clustering_comparators.is_empty() {
            return Ok(Vec::new()); // No clustering keys is valid
        }

        let mut values = Vec::new();
        let mut offset = 0;

        for (idx, comparator) in self.context.clustering_comparators.iter().enumerate() {
            if offset >= data.len() {
                break; // Partial clustering keys are valid
            }

            let key_column = &self.context.schema.clustering_keys[idx];
            let (value, consumed) = self.parse_value_with_comparator(
                &data[offset..],
                comparator,
                &key_column.data_type,
            )?;
            values.push(value);
            offset += consumed;
        }

        Ok(values)
    }

    /// Parse a column value using the schema's column type
    pub fn parse_column_value(&self, column_name: &str, data: &[u8]) -> Result<Value> {
        let comparator = self
            .context
            .get_column_comparator(column_name)
            .ok_or_else(|| {
                Error::Schema(format!(
                    "Column '{}' not found in schema for {}.{}",
                    column_name, self.context.schema.keyspace, self.context.schema.table
                ))
            })?;

        let column = self
            .context
            .schema
            .columns
            .iter()
            .find(|c| c.name == column_name)
            .ok_or_else(|| Error::Schema(format!("Column '{}' not found", column_name)))?;

        let (value, _) = self.parse_value_with_comparator(data, comparator, &column.data_type)?;
        Ok(value)
    }

    /// Parse a value using a specific comparator and type string
    fn parse_value_with_comparator(
        &self,
        data: &[u8],
        comparator: &ComparatorType,
        type_str: &str,
    ) -> Result<(Value, usize)> {
        let cql_type = CqlType::parse(type_str)?;
        self.parse_typed_value(data, &cql_type, comparator)
    }

    /// Parse a value with explicit CQL type and comparator
    fn parse_typed_value(
        &self,
        data: &[u8],
        cql_type: &CqlType,
        comparator: &ComparatorType,
    ) -> Result<(Value, usize)> {
        // This is where the actual parsing happens based on the CQL type
        // Each type has its specific binary format that we parse deterministically
        match cql_type {
            CqlType::Boolean => self.parse_boolean(data),
            CqlType::TinyInt => self.parse_tinyint(data),
            CqlType::SmallInt => self.parse_smallint(data),
            CqlType::Int => self.parse_int(data),
            CqlType::BigInt => self.parse_bigint(data),
            CqlType::Float => self.parse_float(data),
            CqlType::Double => self.parse_double(data),
            CqlType::Text | CqlType::Varchar | CqlType::Ascii => self.parse_text(data),
            CqlType::Blob => self.parse_blob(data),
            CqlType::Timestamp => self.parse_timestamp(data),
            CqlType::Uuid | CqlType::TimeUuid => self.parse_uuid(data),
            CqlType::List(elem_type) => self.parse_list(data, elem_type, comparator),
            CqlType::Set(elem_type) => self.parse_set(data, elem_type, comparator),
            CqlType::Map(key_type, val_type) => {
                self.parse_map(data, key_type, val_type, comparator)
            }
            CqlType::Tuple(field_types) => self.parse_tuple(data, field_types, comparator),
            CqlType::Udt(type_name, fields) => self.parse_udt(data, type_name, fields, comparator),
            CqlType::Frozen(inner_type) => self.parse_frozen(data, inner_type, comparator),
            _ => Err(Error::Schema(format!(
                "Unsupported type for schema-driven parsing: {:?}",
                cql_type
            ))),
        }
    }

    // Type-specific parsing methods (no guessing, strict format adherence)

    fn parse_boolean(&self, data: &[u8]) -> Result<(Value, usize)> {
        if data.is_empty() {
            return Err(Error::schema("Insufficient data for boolean".to_string()));
        }
        Ok((Value::Boolean(data[0] != 0), 1))
    }

    fn parse_tinyint(&self, data: &[u8]) -> Result<(Value, usize)> {
        if data.is_empty() {
            return Err(Error::schema("Insufficient data for tinyint".to_string()));
        }
        Ok((Value::TinyInt(data[0] as i8), 1))
    }

    fn parse_smallint(&self, data: &[u8]) -> Result<(Value, usize)> {
        if data.len() < 2 {
            return Err(Error::schema("Insufficient data for smallint".to_string()));
        }
        let value = i16::from_be_bytes([data[0], data[1]]);
        Ok((Value::SmallInt(value), 2))
    }

    fn parse_int(&self, data: &[u8]) -> Result<(Value, usize)> {
        if data.len() < 4 {
            return Err(Error::schema("Insufficient data for int".to_string()));
        }
        let value = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        Ok((Value::Integer(value), 4))
    }

    fn parse_bigint(&self, data: &[u8]) -> Result<(Value, usize)> {
        if data.len() < 8 {
            return Err(Error::schema("Insufficient data for bigint".to_string()));
        }
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&data[0..8]);
        let value = i64::from_be_bytes(bytes);
        Ok((Value::BigInt(value), 8))
    }

    fn parse_float(&self, data: &[u8]) -> Result<(Value, usize)> {
        if data.len() < 4 {
            return Err(Error::schema("Insufficient data for float".to_string()));
        }
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(&data[0..4]);
        let value = f32::from_be_bytes(bytes);
        Ok((Value::Float32(value), 4))
    }

    fn parse_double(&self, data: &[u8]) -> Result<(Value, usize)> {
        if data.len() < 8 {
            return Err(Error::schema("Insufficient data for double".to_string()));
        }
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&data[0..8]);
        let value = f64::from_be_bytes(bytes);
        Ok((Value::Float(value), 8))
    }

    fn parse_text(&self, data: &[u8]) -> Result<(Value, usize)> {
        // Text is typically length-prefixed
        if data.len() < 4 {
            return Err(Error::schema(
                "Insufficient data for text length".to_string(),
            ));
        }
        let len = i32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
        if data.len() < 4 + len {
            return Err(Error::schema(
                "Insufficient data for text content".to_string(),
            ));
        }
        let text = String::from_utf8(data[4..4 + len].to_vec())
            .map_err(|e| Error::schema(format!("Invalid UTF-8: {}", e)))?;
        Ok((Value::Text(text), 4 + len))
    }

    fn parse_blob(&self, data: &[u8]) -> Result<(Value, usize)> {
        // Blob is typically length-prefixed
        if data.len() < 4 {
            return Err(Error::schema(
                "Insufficient data for blob length".to_string(),
            ));
        }
        let len = i32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
        if data.len() < 4 + len {
            return Err(Error::schema(
                "Insufficient data for blob content".to_string(),
            ));
        }
        Ok((Value::Blob(data[4..4 + len].to_vec()), 4 + len))
    }

    fn parse_timestamp(&self, data: &[u8]) -> Result<(Value, usize)> {
        if data.len() < 8 {
            return Err(Error::schema("Insufficient data for timestamp".to_string()));
        }
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&data[0..8]);
        let millis = i64::from_be_bytes(bytes);
        Ok((Value::Timestamp(millis), 8))
    }

    fn parse_uuid(&self, data: &[u8]) -> Result<(Value, usize)> {
        if data.len() < 16 {
            return Err(Error::schema("Insufficient data for UUID".to_string()));
        }
        let uuid_bytes: [u8; 16] = data[0..16]
            .try_into()
            .map_err(|_| Error::schema("Invalid UUID bytes".to_string()))?;
        Ok((Value::Uuid(uuid_bytes), 16))
    }

    fn parse_list(
        &self,
        data: &[u8],
        elem_type: &CqlType,
        _comparator: &ComparatorType,
    ) -> Result<(Value, usize)> {
        let mut offset = 0;

        // Parse collection size
        if data.len() < 4 {
            return Err(Error::schema("Insufficient data for list size".to_string()));
        }
        let count = i32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
        offset += 4;

        let mut elements = Vec::with_capacity(count);
        let elem_comparator = ComparatorType::from_cql_type(elem_type)?;

        for _ in 0..count {
            let (value, consumed) =
                self.parse_typed_value(&data[offset..], elem_type, &elem_comparator)?;
            elements.push(value);
            offset += consumed;
        }

        Ok((Value::List(elements), offset))
    }

    fn parse_set(
        &self,
        data: &[u8],
        elem_type: &CqlType,
        _comparator: &ComparatorType,
    ) -> Result<(Value, usize)> {
        let mut offset = 0;

        // Parse collection size
        if data.len() < 4 {
            return Err(Error::schema("Insufficient data for set size".to_string()));
        }
        let count = i32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
        offset += 4;

        let mut elements = Vec::with_capacity(count);
        let elem_comparator = ComparatorType::from_cql_type(elem_type)?;

        for _ in 0..count {
            let (value, consumed) =
                self.parse_typed_value(&data[offset..], elem_type, &elem_comparator)?;
            elements.push(value);
            offset += consumed;
        }

        Ok((Value::Set(elements), offset))
    }

    fn parse_map(
        &self,
        data: &[u8],
        key_type: &CqlType,
        val_type: &CqlType,
        _comparator: &ComparatorType,
    ) -> Result<(Value, usize)> {
        let mut offset = 0;

        // Parse collection size
        if data.len() < 4 {
            return Err(Error::schema("Insufficient data for map size".to_string()));
        }
        let count = i32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
        offset += 4;

        let mut map = Vec::with_capacity(count);
        let key_comparator = ComparatorType::from_cql_type(key_type)?;
        let val_comparator = ComparatorType::from_cql_type(val_type)?;

        for _ in 0..count {
            let (key, key_consumed) =
                self.parse_typed_value(&data[offset..], key_type, &key_comparator)?;
            offset += key_consumed;

            let (value, val_consumed) =
                self.parse_typed_value(&data[offset..], val_type, &val_comparator)?;
            offset += val_consumed;

            map.push((key, value));
        }

        Ok((Value::Map(map), offset))
    }

    fn parse_tuple(
        &self,
        data: &[u8],
        field_types: &[CqlType],
        _comparator: &ComparatorType,
    ) -> Result<(Value, usize)> {
        let mut offset = 0;
        let mut values = Vec::with_capacity(field_types.len());

        for field_type in field_types {
            let field_comparator = ComparatorType::from_cql_type(field_type)?;
            let (value, consumed) =
                self.parse_typed_value(&data[offset..], field_type, &field_comparator)?;
            values.push(value);
            offset += consumed;
        }

        Ok((Value::Tuple(values), offset))
    }

    fn parse_udt(
        &self,
        data: &[u8],
        type_name: &str,
        fields: &[(String, CqlType)],
        _comparator: &ComparatorType,
    ) -> Result<(Value, usize)> {
        let mut offset = 0;
        let mut field_values = Vec::with_capacity(fields.len());

        for (field_name, field_type) in fields {
            let field_comparator = ComparatorType::from_cql_type(field_type)?;

            // Check for null field (length = -1)
            if data.len() >= offset + 4 {
                let field_len = i32::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]);

                if field_len < 0 {
                    // Null field
                    field_values.push(crate::types::UdtField {
                        name: field_name.clone(),
                        value: None,
                    });
                    offset += 4;
                    continue;
                }
            }

            let (value, consumed) =
                self.parse_typed_value(&data[offset..], field_type, &field_comparator)?;
            field_values.push(crate::types::UdtField {
                name: field_name.clone(),
                value: Some(value),
            });
            offset += consumed;
        }

        Ok((
            Value::Udt(crate::types::UdtValue {
                type_name: type_name.to_string(),
                keyspace: self.context.schema.keyspace.clone(),
                fields: field_values,
            }),
            offset,
        ))
    }

    fn parse_frozen(
        &self,
        data: &[u8],
        inner_type: &CqlType,
        _comparator: &ComparatorType,
    ) -> Result<(Value, usize)> {
        // Frozen types are serialized the same as their inner type
        let inner_comparator = ComparatorType::from_cql_type(inner_type)?;
        let (inner_value, consumed) =
            self.parse_typed_value(data, inner_type, &inner_comparator)?;
        Ok((Value::Frozen(Box::new(inner_value)), consumed))
    }

    /// Parse a row with all column values using schema
    pub fn parse_row(&self, data: &[u8]) -> Result<HashMap<String, Value>> {
        let mut row = HashMap::new();
        let mut offset = 0;

        for column in &self.context.schema.columns {
            if offset >= data.len() {
                // Remaining columns are null
                row.insert(column.name.clone(), Value::Null);
                continue;
            }

            // Check for null value (length = -1)
            if data.len() >= offset + 4 {
                let value_len = i32::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]);

                if value_len < 0 {
                    row.insert(column.name.clone(), Value::Null);
                    offset += 4;
                    continue;
                }
            }

            let value = self.parse_column_value(&column.name, &data[offset..])?;
            row.insert(column.name.clone(), value);

            // Calculate consumed bytes (this would need to be tracked in parse_column_value)
            // For now, we'll need to enhance parse_column_value to return consumed bytes
        }

        Ok(row)
    }
}

#[cfg(test)]
#[path = "parser_tests.rs"]
mod tests;
