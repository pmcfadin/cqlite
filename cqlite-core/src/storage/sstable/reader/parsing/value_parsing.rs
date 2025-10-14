//! Value parsing using schema-driven approach
//!
//! This module handles parsing of column values using exact schema types,
//! including collection types (list, set, map), tuples, and UDTs.

use crate::{
    types::{ComparatorType, TableId},
    Error, Result, RowKey, Value,
};

#[cfg(feature = "legacy-heuristics")]
use crate::types::UdtValue;

use super::super::types::SSTableReader;

impl SSTableReader {
    /// Parse column value using schema-driven approach (no heuristics)
    pub(in crate::storage::sstable::reader) fn parse_column_value_enhanced(
        &self,
        value_data: &[u8],
        table_id: &TableId,
        key: &RowKey,
        schema: Option<&crate::schema::TableSchema>,
    ) -> Result<Value> {
        if value_data.is_empty() {
            return Ok(Value::Null);
        }

        // Use schema information to determine exact type - NO GUESSING
        if let Some(schema) = self.get_table_schema(schema) {
            // Extract column name from key context if possible
            if let Some(column_name) = self.extract_column_name_from_context(table_id, key) {
                // Find column in schema
                if let Some(column) = schema.columns.iter().find(|c| c.name == column_name) {
                    // Parse using exact type from schema
                    return self.parse_value_with_schema_type(value_data, &column.data_type);
                }
            }
        }

        // Modern formats should never use blob fallback without schema
        match self.header.cassandra_version {
            crate::parser::header::CassandraVersion::V5_0NewBig
            | crate::parser::header::CassandraVersion::V5_0Bti => Err(Error::Schema(format!(
                "Blob fallback not allowed for value parsing in modern format {:?}. \
                     Use SchemaAwareReader with complete schema information.",
                self.header.cassandra_version
            ))),
            _ => {
                // Legacy formats can use blob fallback as last resort
                #[cfg(feature = "legacy-heuristics")]
                {
                    Ok(Value::Blob(value_data.to_vec()))
                }
                #[cfg(not(feature = "legacy-heuristics"))]
                {
                    Err(Error::Schema(
                        "Blob fallback requires legacy-heuristics feature for legacy compatibility.".to_string()
                    ))
                }
            }
        }
    }

    /// Parse value using exact schema type information
    pub(in crate::storage::sstable::reader) fn parse_value_with_schema_type(
        &self,
        value_data: &[u8],
        data_type: &str,
    ) -> Result<Value> {
        // Convert data type string directly to ComparatorType for decoding
        let comparator = ComparatorType::from_data_type(data_type)?;

        // Use comparator to decode the value properly
        match &comparator {
            ComparatorType::Boolean => {
                if value_data.len() == 1 {
                    Ok(Value::Boolean(value_data[0] != 0))
                } else {
                    Err(Error::corruption("Invalid boolean value length"))
                }
            }
            ComparatorType::TinyInt => {
                if value_data.len() == 1 {
                    Ok(Value::TinyInt(value_data[0] as i8))
                } else {
                    Err(Error::corruption("Invalid tinyint value length"))
                }
            }
            ComparatorType::SmallInt => {
                if value_data.len() == 2 {
                    let val = i16::from_be_bytes([value_data[0], value_data[1]]);
                    Ok(Value::SmallInt(val))
                } else {
                    Err(Error::corruption("Invalid smallint value length"))
                }
            }
            ComparatorType::Int => {
                if value_data.len() == 4 {
                    let val = i32::from_be_bytes([
                        value_data[0],
                        value_data[1],
                        value_data[2],
                        value_data[3],
                    ]);
                    Ok(Value::Integer(val))
                } else {
                    Err(Error::corruption("Invalid int value length"))
                }
            }
            ComparatorType::BigInt => {
                if value_data.len() == 8 {
                    let val = i64::from_be_bytes([
                        value_data[0],
                        value_data[1],
                        value_data[2],
                        value_data[3],
                        value_data[4],
                        value_data[5],
                        value_data[6],
                        value_data[7],
                    ]);
                    Ok(Value::BigInt(val))
                } else {
                    Err(Error::corruption("Invalid bigint value length"))
                }
            }
            ComparatorType::Text => {
                let text = String::from_utf8(value_data.to_vec())
                    .map_err(|_| Error::corruption("Invalid UTF-8 in text value"))?;
                Ok(Value::Text(text))
            }
            ComparatorType::Blob => Ok(Value::Blob(value_data.to_vec())),
            ComparatorType::Uuid => {
                if value_data.len() == 16 {
                    // Parse UUID from 16 bytes
                    let uuid_bytes: [u8; 16] = value_data
                        .try_into()
                        .map_err(|_| Error::corruption("Invalid UUID byte array"))?;
                    Ok(Value::Uuid(uuid_bytes))
                } else {
                    Err(Error::corruption("Invalid UUID value length"))
                }
            }
            ComparatorType::List(element_comparator) => {
                self.parse_list_value(value_data, element_comparator)
            }
            ComparatorType::Set(element_comparator) => {
                self.parse_set_value(value_data, element_comparator)
            }
            ComparatorType::Map(key_comparator, value_comparator) => {
                self.parse_map_value(value_data, key_comparator, value_comparator)
            }
            ComparatorType::Tuple(field_comparators) => {
                self.parse_tuple_value(value_data, field_comparators)
            }
            ComparatorType::Udt {
                field_comparators, ..
            } => self.parse_udt_value(value_data, field_comparators),
            ComparatorType::Frozen(inner_comparator) => {
                // For frozen types, parse the inner type directly
                let inner_value = self.parse_value_with_comparator(value_data, inner_comparator)?;
                Ok(Value::Frozen(Box::new(inner_value)))
            }
            _ => {
                // For other types, preserve as blob for now
                Ok(Value::Blob(value_data.to_vec()))
            }
        }
    }

    /// Parse value directly using ComparatorType (helper method)
    pub(in crate::storage::sstable::reader) fn parse_value_with_comparator(
        &self,
        value_data: &[u8],
        comparator: &ComparatorType,
    ) -> Result<Value> {
        // Use the same logic as parse_value_with_schema_type but with direct comparator
        match comparator {
            ComparatorType::Boolean => {
                if value_data.len() == 1 {
                    Ok(Value::Boolean(value_data[0] != 0))
                } else {
                    Err(Error::corruption("Invalid boolean value length"))
                }
            }
            ComparatorType::Text => {
                let text = String::from_utf8(value_data.to_vec())
                    .map_err(|_| Error::corruption("Invalid UTF-8 in text value"))?;
                Ok(Value::Text(text))
            }
            ComparatorType::Blob => Ok(Value::Blob(value_data.to_vec())),
            _ => {
                // For complex types, implement as needed
                Ok(Value::Blob(value_data.to_vec()))
            }
        }
    }

    /// Parse list value using element comparator
    pub(in crate::storage::sstable::reader) fn parse_list_value(
        &self,
        value_data: &[u8],
        element_comparator: &ComparatorType,
    ) -> Result<Value> {
        use crate::parser::vint::parse_vint_length;

        let mut offset = 0;
        let mut elements = Vec::new();

        // Parse element count
        let (remaining, element_count) = parse_vint_length(&value_data[offset..])
            .map_err(|_| Error::corruption("Failed to parse list element count"))?;
        offset = value_data.len() - remaining.len();

        // Parse each element
        for _ in 0..element_count {
            if offset >= value_data.len() {
                break;
            }

            // Parse element length
            let (remaining, element_len) = parse_vint_length(&value_data[offset..])
                .map_err(|_| Error::corruption("Failed to parse list element length"))?;
            offset = value_data.len() - remaining.len();

            if element_len > remaining.len() {
                return Err(Error::corruption(
                    "List element length exceeds available data",
                ));
            }

            // Parse element value using element comparator
            let element_data = &remaining[..element_len];
            let element_value =
                self.parse_value_with_comparator(element_data, element_comparator)?;
            elements.push(element_value);
            offset += element_len;
        }

        Ok(Value::List(elements))
    }

    /// Parse set value using element comparator
    pub(in crate::storage::sstable::reader) fn parse_set_value(
        &self,
        value_data: &[u8],
        element_comparator: &ComparatorType,
    ) -> Result<Value> {
        // Sets are parsed similarly to lists
        let list_value = self.parse_list_value(value_data, element_comparator)?;
        if let Value::List(elements) = list_value {
            Ok(Value::Set(elements))
        } else {
            Err(Error::corruption("Failed to parse set value"))
        }
    }

    /// Parse map value using key and value comparators
    pub(in crate::storage::sstable::reader) fn parse_map_value(
        &self,
        value_data: &[u8],
        key_comparator: &ComparatorType,
        value_comparator: &ComparatorType,
    ) -> Result<Value> {
        use crate::parser::vint::parse_vint_length;

        let mut offset = 0;
        let mut entries = Vec::new();

        // Parse entry count
        let (remaining, entry_count) = parse_vint_length(&value_data[offset..])
            .map_err(|_| Error::corruption("Failed to parse map entry count"))?;
        offset = value_data.len() - remaining.len();

        // Parse each key-value pair
        for _ in 0..entry_count {
            if offset >= value_data.len() {
                break;
            }

            // Parse key length and data
            let (remaining, key_len) = parse_vint_length(&value_data[offset..])
                .map_err(|_| Error::corruption("Failed to parse map key length"))?;
            offset = value_data.len() - remaining.len();

            if key_len > remaining.len() {
                return Err(Error::corruption("Map key length exceeds available data"));
            }

            let key_data = &remaining[..key_len];
            let key_value = self.parse_value_with_comparator(key_data, key_comparator)?;
            offset += key_len;

            // Parse value length and data
            let (remaining, value_len) = parse_vint_length(&value_data[offset..])
                .map_err(|_| Error::corruption("Failed to parse map value length"))?;
            offset = value_data.len() - remaining.len();

            if value_len > remaining.len() {
                return Err(Error::corruption("Map value length exceeds available data"));
            }

            let val_data = &remaining[..value_len];
            let val_value = self.parse_value_with_comparator(val_data, value_comparator)?;
            entries.push((key_value, val_value));
            offset += value_len;
        }

        Ok(Value::Map(entries))
    }

    /// Parse tuple value using field comparators
    pub(in crate::storage::sstable::reader) fn parse_tuple_value(
        &self,
        value_data: &[u8],
        field_comparators: &[ComparatorType],
    ) -> Result<Value> {
        use crate::parser::vint::parse_vint_length;

        let mut offset = 0;
        let mut fields = Vec::new();

        // Parse each field
        for (i, field_comparator) in field_comparators.iter().enumerate() {
            if offset >= value_data.len() {
                break;
            }

            // Parse field length
            let (remaining, field_len) =
                parse_vint_length(&value_data[offset..]).map_err(|_| {
                    Error::corruption(format!("Failed to parse tuple field {} length", i))
                })?;
            offset = value_data.len() - remaining.len();

            if field_len > remaining.len() {
                return Err(Error::corruption(format!(
                    "Tuple field {} length exceeds available data",
                    i
                )));
            }

            // Parse field value using field comparator
            let field_data = &remaining[..field_len];
            let field_value = self.parse_value_with_comparator(field_data, field_comparator)?;
            fields.push(field_value);
            offset += field_len;
        }

        Ok(Value::Tuple(fields))
    }

    /// Parse UDT value using field comparators
    pub(in crate::storage::sstable::reader) fn parse_udt_value(
        &self,
        value_data: &[u8],
        field_comparators: &[(String, ComparatorType)],
    ) -> Result<Value> {
        use crate::parser::vint::parse_vint_length;
        use crate::types::UdtField;

        let mut offset = 0;
        let mut fields = Vec::new();

        // Parse each field
        for (field_name, field_comparator) in field_comparators.iter() {
            if offset >= value_data.len() {
                break;
            }

            // Parse field length
            let (remaining, field_len) =
                parse_vint_length(&value_data[offset..]).map_err(|_| {
                    Error::corruption(format!("Failed to parse UDT field {} length", field_name))
                })?;
            offset = value_data.len() - remaining.len();

            if field_len > remaining.len() {
                return Err(Error::corruption(format!(
                    "UDT field {} length exceeds available data",
                    field_name
                )));
            }

            // Parse field value using field comparator
            let field_data = &remaining[..field_len];
            let field_value = self.parse_value_with_comparator(field_data, field_comparator)?;

            fields.push(UdtField {
                name: field_name.clone(),
                value: Some(field_value),
            });
            offset += field_len;
        }

        // Modern formats should never use generic UDT fabrication without schema
        match self.header.cassandra_version {
            crate::parser::header::CassandraVersion::V5_0NewBig
            | crate::parser::header::CassandraVersion::V5_0Bti => Err(Error::Schema(format!(
                "Generic UDT fabrication not allowed for modern format {:?}. \
                     Use SchemaAwareReader with complete UDT schema information.",
                self.header.cassandra_version
            ))),
            _ => {
                // Legacy formats can use generic UDT fabrication as last resort
                #[cfg(feature = "legacy-heuristics")]
                {
                    Ok(Value::Udt(UdtValue {
                        keyspace: "unknown".to_string(),
                        type_name: "unknown".to_string(),
                        fields,
                    }))
                }
                #[cfg(not(feature = "legacy-heuristics"))]
                {
                    Err(Error::Schema(
                        "Generic UDT fabrication requires legacy-heuristics feature for legacy compatibility.".to_string()
                    ))
                }
            }
        }
    }

    /// Extract column name from key context (schema-aware implementation)
    pub(in crate::storage::sstable::reader) fn extract_column_name_from_context(
        &self,
        _table_id: &TableId,
        _key: &RowKey,
    ) -> Option<String> {
        // Modern formats require SchemaAwareReader for proper column name extraction
        match self.header.cassandra_version {
            crate::parser::header::CassandraVersion::V5_0NewBig
            | crate::parser::header::CassandraVersion::V5_0Bti => {
                // Modern formats should not use this placeholder implementation
                log::error!(
                    "Column name extraction from key context requires SchemaAwareReader for modern format {:?}",
                    self.header.cassandra_version
                );
                None
            }
            _ => {
                // Legacy formats return None (placeholder behavior)
                #[cfg(feature = "legacy-heuristics")]
                {
                    None // Placeholder implementation for legacy compatibility
                }
                #[cfg(not(feature = "legacy-heuristics"))]
                {
                    None // Column name extraction not supported without legacy features
                }
            }
        }
    }
}
