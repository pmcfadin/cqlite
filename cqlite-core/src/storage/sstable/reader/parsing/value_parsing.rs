//! Value parsing using schema-driven approach
//!
//! This module handles parsing of column values using exact schema types,
//! including collection types (list, set, map), tuples, and UDTs.

use crate::{
    types::{ComparatorType, TableId, UdtField, UdtValue},
    Error, Result, RowKey, Value,
};

use super::super::types::SSTableReader;

// ============================================================================
// Standalone Pure Parsing Functions
// ============================================================================

/// Parse boolean value from raw bytes
pub(crate) fn parse_boolean_value(data: &[u8]) -> Result<Value> {
    if data.len() == 1 {
        Ok(Value::Boolean(data[0] != 0))
    } else {
        Err(Error::corruption("Invalid boolean value length"))
    }
}

/// Parse tinyint value from raw bytes
pub(crate) fn parse_tinyint_value(data: &[u8]) -> Result<Value> {
    if data.len() == 1 {
        Ok(Value::TinyInt(data[0] as i8))
    } else {
        Err(Error::corruption("Invalid tinyint value length"))
    }
}

/// Parse smallint value from raw bytes
pub(crate) fn parse_smallint_value(data: &[u8]) -> Result<Value> {
    if data.len() == 2 {
        let val = i16::from_be_bytes([data[0], data[1]]);
        Ok(Value::SmallInt(val))
    } else {
        Err(Error::corruption("Invalid smallint value length"))
    }
}

/// Parse int value from raw bytes
pub(crate) fn parse_int_value(data: &[u8]) -> Result<Value> {
    if data.len() == 4 {
        let val = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        Ok(Value::Integer(val))
    } else {
        Err(Error::corruption("Invalid int value length"))
    }
}

/// Parse bigint value from raw bytes
pub(crate) fn parse_bigint_value(data: &[u8]) -> Result<Value> {
    if data.len() == 8 {
        let val = i64::from_be_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ]);
        Ok(Value::BigInt(val))
    } else {
        Err(Error::corruption("Invalid bigint value length"))
    }
}

/// Parse counter value from raw bytes
///
/// Counter values at this stage are already extracted i64 values (8 bytes big-endian).
/// The CounterContext parsing happens earlier in V5CompressedLegacyParser.
pub(crate) fn parse_counter_value(data: &[u8]) -> Result<Value> {
    if data.len() == 8 {
        let val = i64::from_be_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ]);
        Ok(Value::Counter(val))
    } else {
        Err(Error::corruption("Invalid counter value length"))
    }
}

/// Parse text value from raw bytes
pub(crate) fn parse_text_value(data: &[u8]) -> Result<Value> {
    String::from_utf8(data.to_vec())
        .map(Value::Text)
        .map_err(|_| Error::corruption("Invalid UTF-8 in text value"))
}

/// Parse blob value from raw bytes
pub(crate) fn parse_blob_value(data: &[u8]) -> Result<Value> {
    Ok(Value::Blob(data.to_vec()))
}

/// Parse UUID value from raw bytes
pub(crate) fn parse_uuid_value(data: &[u8]) -> Result<Value> {
    if data.len() == 16 {
        let uuid_bytes: [u8; 16] = data
            .try_into()
            .map_err(|_| Error::corruption("Invalid UUID byte array"))?;
        Ok(Value::Uuid(uuid_bytes))
    } else {
        Err(Error::corruption("Invalid UUID value length"))
    }
}

/// Parse DATE value from raw bytes
pub(crate) fn parse_date_value(data: &[u8]) -> Result<Value> {
    if data.len() == 4 {
        // Cassandra DATE: 4-byte big-endian unsigned int with Integer.MIN_VALUE offset
        // for byte-order comparability. Decode by adding i32::MIN back.
        let stored = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        let days_since_epoch = stored.wrapping_add(i32::MIN as u32) as i32;
        Ok(Value::Date(days_since_epoch))
    } else {
        Err(Error::corruption("Invalid DATE value length"))
    }
}

/// Parse list value with recursive element parsing via closure
///
/// This allows testing the list parsing logic independently of SSTableReader.
pub(crate) fn parse_list_value_with<F>(
    data: &[u8],
    element_comparator: &ComparatorType,
    parse_element: F,
) -> Result<Value>
where
    F: Fn(&[u8], &ComparatorType) -> Result<Value>,
{
    use crate::parser::vint::parse_vint_length;

    let mut offset = 0;
    let mut elements = Vec::new();

    // Parse element count
    let (remaining, element_count) = parse_vint_length(&data[offset..])
        .map_err(|_| Error::corruption("Failed to parse list element count"))?;
    offset = data.len() - remaining.len();

    // Parse each element
    for _ in 0..element_count {
        if offset >= data.len() {
            break;
        }

        // Parse element length
        let (remaining, element_len) = parse_vint_length(&data[offset..])
            .map_err(|_| Error::corruption("Failed to parse list element length"))?;
        offset = data.len() - remaining.len();

        if element_len > remaining.len() {
            return Err(Error::corruption(
                "List element length exceeds available data",
            ));
        }

        // Parse element value using provided closure
        let element_data = &remaining[..element_len];
        let element_value = parse_element(element_data, element_comparator)?;
        elements.push(element_value);
        offset += element_len;
    }

    Ok(Value::List(elements))
}

/// Parse set value with recursive element parsing via closure
pub(crate) fn parse_set_value_with<F>(
    data: &[u8],
    element_comparator: &ComparatorType,
    parse_element: F,
) -> Result<Value>
where
    F: Fn(&[u8], &ComparatorType) -> Result<Value>,
{
    // Sets are parsed similarly to lists
    let list_value = parse_list_value_with(data, element_comparator, parse_element)?;
    if let Value::List(elements) = list_value {
        Ok(Value::Set(elements))
    } else {
        Err(Error::corruption("Failed to parse set value"))
    }
}

/// Parse map value with recursive key/value parsing via closure
pub(crate) fn parse_map_value_with<F>(
    data: &[u8],
    key_comparator: &ComparatorType,
    value_comparator: &ComparatorType,
    parse_element: F,
) -> Result<Value>
where
    F: Fn(&[u8], &ComparatorType) -> Result<Value>,
{
    use crate::parser::vint::parse_vint_length;

    let mut offset = 0;
    let mut entries = Vec::new();

    // Parse entry count
    let (remaining, entry_count) = parse_vint_length(&data[offset..])
        .map_err(|_| Error::corruption("Failed to parse map entry count"))?;
    offset = data.len() - remaining.len();

    // Parse each key-value pair
    for _ in 0..entry_count {
        if offset >= data.len() {
            break;
        }

        // Parse key length and data
        let (remaining, key_len) = parse_vint_length(&data[offset..])
            .map_err(|_| Error::corruption("Failed to parse map key length"))?;
        offset = data.len() - remaining.len();

        if key_len > remaining.len() {
            return Err(Error::corruption("Map key length exceeds available data"));
        }

        let key_data = &remaining[..key_len];
        let key_value = parse_element(key_data, key_comparator)?;
        offset += key_len;

        // Parse value length and data
        let (remaining, value_len) = parse_vint_length(&data[offset..])
            .map_err(|_| Error::corruption("Failed to parse map value length"))?;
        offset = data.len() - remaining.len();

        if value_len > remaining.len() {
            return Err(Error::corruption("Map value length exceeds available data"));
        }

        let val_data = &remaining[..value_len];
        let val_value = parse_element(val_data, value_comparator)?;
        entries.push((key_value, val_value));
        offset += value_len;
    }

    Ok(Value::Map(entries))
}

/// Parse tuple value with recursive field parsing via closure
pub(crate) fn parse_tuple_value_with<F>(
    data: &[u8],
    field_comparators: &[ComparatorType],
    parse_element: F,
) -> Result<Value>
where
    F: Fn(&[u8], &ComparatorType) -> Result<Value>,
{
    use crate::parser::vint::parse_vint_length;

    let mut offset = 0;
    let mut fields = Vec::new();

    // Parse each field
    for (i, field_comparator) in field_comparators.iter().enumerate() {
        if offset >= data.len() {
            break;
        }

        // Parse field length
        let (remaining, field_len) = parse_vint_length(&data[offset..])
            .map_err(|_| Error::corruption(format!("Failed to parse tuple field {} length", i)))?;
        offset = data.len() - remaining.len();

        if field_len > remaining.len() {
            return Err(Error::corruption(format!(
                "Tuple field {} length exceeds available data",
                i
            )));
        }

        // Parse field value using provided closure
        let field_data = &remaining[..field_len];
        let field_value = parse_element(field_data, field_comparator)?;
        fields.push(field_value);
        offset += field_len;
    }

    Ok(Value::Tuple(fields))
}

/// Parse UDT value with recursive field parsing via closure
#[allow(dead_code)] // Used in tests; may be used by future refactoring
pub(crate) fn parse_udt_value_with<F>(
    data: &[u8],
    field_comparators: &[(String, ComparatorType)],
    parse_element: F,
) -> Result<UdtValue>
where
    F: Fn(&[u8], &ComparatorType) -> Result<Value>,
{
    use crate::parser::vint::parse_vint_length;

    let mut offset = 0;
    let mut fields = Vec::new();

    // Parse each field
    for (field_name, field_comparator) in field_comparators.iter() {
        if offset >= data.len() {
            break;
        }

        // Parse field length
        let (remaining, field_len) = parse_vint_length(&data[offset..]).map_err(|_| {
            Error::corruption(format!("Failed to parse UDT field {} length", field_name))
        })?;
        offset = data.len() - remaining.len();

        if field_len > remaining.len() {
            return Err(Error::corruption(format!(
                "UDT field {} length exceeds available data",
                field_name
            )));
        }

        // Parse field value using provided closure
        let field_data = &remaining[..field_len];
        let field_value = parse_element(field_data, field_comparator)?;

        fields.push(UdtField {
            name: field_name.clone(),
            value: Some(field_value),
        });
        offset += field_len;
    }

    Ok(UdtValue {
        keyspace: "unknown".to_string(),
        type_name: "unknown".to_string(),
        fields,
    })
}

// ============================================================================
// SSTableReader Methods (Wrappers around pure functions)
// ============================================================================

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
            ComparatorType::Boolean => parse_boolean_value(value_data),
            ComparatorType::TinyInt => parse_tinyint_value(value_data),
            ComparatorType::SmallInt => parse_smallint_value(value_data),
            ComparatorType::Int => parse_int_value(value_data),
            ComparatorType::BigInt => parse_bigint_value(value_data),
            ComparatorType::Counter => parse_counter_value(value_data),
            ComparatorType::Text => parse_text_value(value_data),
            ComparatorType::Blob => parse_blob_value(value_data),
            ComparatorType::Uuid => parse_uuid_value(value_data),
            ComparatorType::Date => parse_date_value(value_data),
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
                parse_blob_value(value_data)
            }
        }
    }

    /// Parse value directly using ComparatorType (helper method for nested collection elements)
    ///
    /// This function provides complete recursive type parsing for collection elements,
    /// including UDTs, tuples, nested collections, and frozen types.
    pub(in crate::storage::sstable::reader) fn parse_value_with_comparator(
        &self,
        value_data: &[u8],
        comparator: &ComparatorType,
    ) -> Result<Value> {
        use crate::parser::vint::parse_vint_length;

        match comparator {
            ComparatorType::Boolean => parse_boolean_value(value_data),
            ComparatorType::TinyInt => parse_tinyint_value(value_data),
            ComparatorType::SmallInt => parse_smallint_value(value_data),
            ComparatorType::Int => parse_int_value(value_data),
            ComparatorType::BigInt => parse_bigint_value(value_data),
            ComparatorType::Counter => parse_counter_value(value_data),
            ComparatorType::Text => parse_text_value(value_data),
            ComparatorType::Blob => parse_blob_value(value_data),
            ComparatorType::Uuid => parse_uuid_value(value_data),
            ComparatorType::Date => parse_date_value(value_data),
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
                keyspace,
                type_name,
                field_comparators,
            } => {
                // Parse UDT fields inline with full type info (Issue #238 fix)
                // This avoids the V5 format check in parse_udt_value() which incorrectly
                // returns an error even when we have complete schema information.
                let mut offset = 0;
                let mut fields = Vec::new();

                for (field_name, field_comparator) in field_comparators.iter() {
                    if offset >= value_data.len() {
                        break;
                    }
                    let (remaining, field_len) =
                        parse_vint_length(&value_data[offset..]).map_err(|_| {
                            Error::corruption(format!(
                                "Failed to parse UDT field {} length",
                                field_name
                            ))
                        })?;
                    offset = value_data.len() - remaining.len();

                    if field_len > remaining.len() {
                        return Err(Error::corruption(format!(
                            "UDT field {} length exceeds available data",
                            field_name
                        )));
                    }

                    let field_data = &remaining[..field_len];
                    let field_value =
                        self.parse_value_with_comparator(field_data, field_comparator)?;

                    fields.push(UdtField {
                        name: field_name.clone(),
                        value: Some(field_value),
                    });
                    offset += field_len;
                }

                Ok(Value::Udt(UdtValue {
                    keyspace: keyspace.clone().unwrap_or_else(|| "unknown".to_string()),
                    type_name: type_name.clone(),
                    fields,
                }))
            }
            ComparatorType::Frozen(inner_comparator) => {
                let inner_value = self.parse_value_with_comparator(value_data, inner_comparator)?;
                Ok(Value::Frozen(Box::new(inner_value)))
            }
            _ => {
                // For other unsupported types (Custom, Counter, Timestamp, etc.),
                // preserve as blob. These types are handled at the top-level
                // by parse_value_with_schema_type but may appear in collections.
                parse_blob_value(value_data)
            }
        }
    }

    /// Parse list value using element comparator
    pub(in crate::storage::sstable::reader) fn parse_list_value(
        &self,
        value_data: &[u8],
        element_comparator: &ComparatorType,
    ) -> Result<Value> {
        parse_list_value_with(value_data, element_comparator, |data, comp| {
            self.parse_value_with_comparator(data, comp)
        })
    }

    /// Parse set value using element comparator
    pub(in crate::storage::sstable::reader) fn parse_set_value(
        &self,
        value_data: &[u8],
        element_comparator: &ComparatorType,
    ) -> Result<Value> {
        parse_set_value_with(value_data, element_comparator, |data, comp| {
            self.parse_value_with_comparator(data, comp)
        })
    }

    /// Parse map value using key and value comparators
    pub(in crate::storage::sstable::reader) fn parse_map_value(
        &self,
        value_data: &[u8],
        key_comparator: &ComparatorType,
        value_comparator: &ComparatorType,
    ) -> Result<Value> {
        parse_map_value_with(
            value_data,
            key_comparator,
            value_comparator,
            |data, comp| self.parse_value_with_comparator(data, comp),
        )
    }

    /// Parse tuple value using field comparators
    pub(in crate::storage::sstable::reader) fn parse_tuple_value(
        &self,
        value_data: &[u8],
        field_comparators: &[ComparatorType],
    ) -> Result<Value> {
        parse_tuple_value_with(value_data, field_comparators, |data, comp| {
            self.parse_value_with_comparator(data, comp)
        })
    }

    /// Parse UDT value using field comparators
    pub(in crate::storage::sstable::reader) fn parse_udt_value(
        &self,
        value_data: &[u8],
        field_comparators: &[(String, ComparatorType)],
    ) -> Result<Value> {
        use crate::parser::vint::parse_vint_length;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::vint::encode_vint;

    // ============================================================================
    // Primitive Type Tests
    // ============================================================================

    #[test]
    fn test_parse_boolean_true() {
        let data = [0x01];
        let result = parse_boolean_value(&data).unwrap();
        assert_eq!(result, Value::Boolean(true));
    }

    #[test]
    fn test_parse_boolean_false() {
        let data = [0x00];
        let result = parse_boolean_value(&data).unwrap();
        assert_eq!(result, Value::Boolean(false));
    }

    #[test]
    fn test_parse_tinyint() {
        let data = [0x2A]; // 42
        let result = parse_tinyint_value(&data).unwrap();
        assert_eq!(result, Value::TinyInt(42));
    }

    #[test]
    fn test_parse_tinyint_negative() {
        let data = [0xD6]; // -42 in two's complement
        let result = parse_tinyint_value(&data).unwrap();
        assert_eq!(result, Value::TinyInt(-42));
    }

    #[test]
    fn test_parse_smallint() {
        let data = [0x00, 0x2A]; // 42 big-endian
        let result = parse_smallint_value(&data).unwrap();
        assert_eq!(result, Value::SmallInt(42));
    }

    #[test]
    fn test_parse_smallint_negative() {
        let data = [0xFF, 0xD6]; // -42 big-endian
        let result = parse_smallint_value(&data).unwrap();
        assert_eq!(result, Value::SmallInt(-42));
    }

    #[test]
    fn test_parse_int() {
        let data = 42i32.to_be_bytes();
        let result = parse_int_value(&data).unwrap();
        assert_eq!(result, Value::Integer(42));
    }

    #[test]
    fn test_parse_int_negative() {
        let data = (-2i32).to_be_bytes();
        let result = parse_int_value(&data).unwrap();
        assert_eq!(result, Value::Integer(-2));
    }

    #[test]
    fn test_parse_bigint() {
        let data = 42i64.to_be_bytes();
        let result = parse_bigint_value(&data).unwrap();
        assert_eq!(result, Value::BigInt(42));
    }

    #[test]
    fn test_parse_bigint_negative() {
        let data = (-9223372036854775807i64).to_be_bytes();
        let result = parse_bigint_value(&data).unwrap();
        assert_eq!(result, Value::BigInt(-9223372036854775807));
    }

    #[test]
    fn test_parse_counter() {
        // Test with actual counter value from test data (Issue #272)
        let test_value: i64 = 422216548022666;
        let data = test_value.to_be_bytes();
        let result = parse_counter_value(&data).unwrap();
        assert_eq!(result, Value::Counter(test_value));
    }

    #[test]
    fn test_parse_counter_negative() {
        let data = (-1234567890i64).to_be_bytes();
        let result = parse_counter_value(&data).unwrap();
        assert_eq!(result, Value::Counter(-1234567890));
    }

    #[test]
    fn test_parse_counter_wrong_length() {
        let data = [0x00, 0x00, 0x00, 0x00]; // Only 4 bytes instead of 8
        let result = parse_counter_value(&data);
        assert!(result.is_err(), "Counter should reject 4-byte input");
    }

    #[test]
    fn test_parse_text() {
        let data = b"hello";
        let result = parse_text_value(data).unwrap();
        assert_eq!(result, Value::Text("hello".to_string()));
    }

    #[test]
    fn test_parse_text_unicode() {
        let data = "Hello 世界".as_bytes();
        let result = parse_text_value(data).unwrap();
        assert_eq!(result, Value::Text("Hello 世界".to_string()));
    }

    #[test]
    fn test_parse_blob() {
        let data = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let result = parse_blob_value(&data).unwrap();
        assert_eq!(result, Value::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]));
    }

    #[test]
    fn test_parse_uuid() {
        let uuid_bytes = [
            0x12, 0x34, 0x56, 0x78, 0x90, 0xAB, 0xCD, 0xEF, 0x12, 0x34, 0x56, 0x78, 0x90, 0xAB,
            0xCD, 0xEF,
        ];
        let result = parse_uuid_value(&uuid_bytes).unwrap();
        assert_eq!(result, Value::Uuid(uuid_bytes));
    }

    #[test]
    fn test_parse_date_epoch() {
        // Cassandra DATE for epoch (1970-01-01) is stored as i32::MIN (0x80000000)
        // which represents 0 days since epoch
        let stored = (0u32).wrapping_sub(i32::MIN as u32);
        let data = stored.to_be_bytes();
        let result = parse_date_value(&data).unwrap();
        assert_eq!(result, Value::Date(0));
    }

    #[test]
    fn test_parse_date_positive() {
        // 100 days after epoch
        let days = 100i32;
        let stored = (days as u32).wrapping_sub(i32::MIN as u32);
        let data = stored.to_be_bytes();
        let result = parse_date_value(&data).unwrap();
        assert_eq!(result, Value::Date(100));
    }

    #[test]
    fn test_parse_date_negative() {
        // 100 days before epoch
        let days = -100i32;
        let stored = (days as u32).wrapping_sub(i32::MIN as u32);
        let data = stored.to_be_bytes();
        let result = parse_date_value(&data).unwrap();
        assert_eq!(result, Value::Date(-100));
    }

    // ============================================================================
    // Collection Tests
    // ============================================================================

    #[test]
    fn test_parse_list_empty() {
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(0)); // 0 elements

        let result =
            parse_list_value_with(&data, &ComparatorType::Int, |d, _| parse_int_value(d)).unwrap();

        if let Value::List(elements) = result {
            assert_eq!(elements.len(), 0);
        } else {
            panic!("Expected List value");
        }
    }

    #[test]
    fn test_parse_list_single_int() {
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(1)); // 1 element
        data.extend_from_slice(&encode_vint(4)); // element length
        data.extend_from_slice(&42i32.to_be_bytes());

        let result =
            parse_list_value_with(&data, &ComparatorType::Int, |d, _| parse_int_value(d)).unwrap();

        if let Value::List(elements) = result {
            assert_eq!(elements.len(), 1);
            assert_eq!(elements[0], Value::Integer(42));
        } else {
            panic!("Expected List value");
        }
    }

    #[test]
    fn test_parse_list_multiple_ints() {
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(3)); // 3 elements
        for val in [1i32, 2, 3] {
            data.extend_from_slice(&encode_vint(4)); // each int is 4 bytes
            data.extend_from_slice(&val.to_be_bytes());
        }

        let result =
            parse_list_value_with(&data, &ComparatorType::Int, |d, _| parse_int_value(d)).unwrap();

        if let Value::List(elements) = result {
            assert_eq!(elements.len(), 3);
            assert_eq!(elements[0], Value::Integer(1));
            assert_eq!(elements[1], Value::Integer(2));
            assert_eq!(elements[2], Value::Integer(3));
        } else {
            panic!("Expected List value");
        }
    }

    #[test]
    fn test_parse_list_text_elements() {
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(2)); // 2 elements

        let str1 = "hello";
        data.extend_from_slice(&encode_vint(str1.len() as i64));
        data.extend_from_slice(str1.as_bytes());

        let str2 = "world";
        data.extend_from_slice(&encode_vint(str2.len() as i64));
        data.extend_from_slice(str2.as_bytes());

        let result =
            parse_list_value_with(&data, &ComparatorType::Text, |d, _| parse_text_value(d))
                .unwrap();

        if let Value::List(elements) = result {
            assert_eq!(elements.len(), 2);
            assert_eq!(elements[0], Value::Text("hello".to_string()));
            assert_eq!(elements[1], Value::Text("world".to_string()));
        } else {
            panic!("Expected List value");
        }
    }

    #[test]
    fn test_parse_set_int() {
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(2)); // 2 elements
        for val in [10i32, 20] {
            data.extend_from_slice(&encode_vint(4));
            data.extend_from_slice(&val.to_be_bytes());
        }

        let result =
            parse_set_value_with(&data, &ComparatorType::Int, |d, _| parse_int_value(d)).unwrap();

        if let Value::Set(elements) = result {
            assert_eq!(elements.len(), 2);
            assert_eq!(elements[0], Value::Integer(10));
            assert_eq!(elements[1], Value::Integer(20));
        } else {
            panic!("Expected Set value");
        }
    }

    #[test]
    fn test_parse_map_text_int() {
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(2)); // 2 entries

        // Entry 1: "key1" -> 100
        let key1 = "key1";
        data.extend_from_slice(&encode_vint(key1.len() as i64));
        data.extend_from_slice(key1.as_bytes());
        data.extend_from_slice(&encode_vint(4));
        data.extend_from_slice(&100i32.to_be_bytes());

        // Entry 2: "key2" -> 200
        let key2 = "key2";
        data.extend_from_slice(&encode_vint(key2.len() as i64));
        data.extend_from_slice(key2.as_bytes());
        data.extend_from_slice(&encode_vint(4));
        data.extend_from_slice(&200i32.to_be_bytes());

        let result = parse_map_value_with(
            &data,
            &ComparatorType::Text,
            &ComparatorType::Int,
            |d, comp| match comp {
                ComparatorType::Text => parse_text_value(d),
                ComparatorType::Int => parse_int_value(d),
                _ => panic!("Unexpected comparator"),
            },
        )
        .unwrap();

        if let Value::Map(entries) = result {
            assert_eq!(entries.len(), 2);
            assert_eq!(entries[0].0, Value::Text("key1".to_string()));
            assert_eq!(entries[0].1, Value::Integer(100));
            assert_eq!(entries[1].0, Value::Text("key2".to_string()));
            assert_eq!(entries[1].1, Value::Integer(200));
        } else {
            panic!("Expected Map value");
        }
    }

    #[test]
    fn test_parse_map_empty() {
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(0)); // 0 entries

        let result = parse_map_value_with(
            &data,
            &ComparatorType::Text,
            &ComparatorType::Int,
            |d, comp| match comp {
                ComparatorType::Text => parse_text_value(d),
                ComparatorType::Int => parse_int_value(d),
                _ => panic!("Unexpected comparator"),
            },
        )
        .unwrap();

        if let Value::Map(entries) = result {
            assert_eq!(entries.len(), 0);
        } else {
            panic!("Expected Map value");
        }
    }

    #[test]
    fn test_parse_map_nested() {
        // Map<text, list<int>>
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(1)); // 1 entry

        // Key: "numbers"
        let key = "numbers";
        data.extend_from_slice(&encode_vint(key.len() as i64));
        data.extend_from_slice(key.as_bytes());

        // Value: list<int> with [1, 2, 3]
        let mut list_data = Vec::new();
        list_data.extend_from_slice(&encode_vint(3)); // 3 elements
        for val in [1i32, 2, 3] {
            list_data.extend_from_slice(&encode_vint(4));
            list_data.extend_from_slice(&val.to_be_bytes());
        }
        data.extend_from_slice(&encode_vint(list_data.len() as i64));
        data.extend_from_slice(&list_data);

        let result = parse_map_value_with(
            &data,
            &ComparatorType::Text,
            &ComparatorType::List(Box::new(ComparatorType::Int)),
            |d, comp| match comp {
                ComparatorType::Text => parse_text_value(d),
                ComparatorType::List(inner) => {
                    parse_list_value_with(d, inner, |inner_d, inner_comp| match inner_comp {
                        ComparatorType::Int => parse_int_value(inner_d),
                        _ => panic!("Unexpected inner comparator"),
                    })
                }
                _ => panic!("Unexpected comparator"),
            },
        )
        .unwrap();

        if let Value::Map(entries) = result {
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].0, Value::Text("numbers".to_string()));
            if let Value::List(list_elements) = &entries[0].1 {
                assert_eq!(list_elements.len(), 3);
                assert_eq!(list_elements[0], Value::Integer(1));
                assert_eq!(list_elements[1], Value::Integer(2));
                assert_eq!(list_elements[2], Value::Integer(3));
            } else {
                panic!("Expected List value in map");
            }
        } else {
            panic!("Expected Map value");
        }
    }

    // ============================================================================
    // Complex Type Tests
    // ============================================================================

    #[test]
    fn test_parse_tuple_int_text() {
        let mut data = Vec::new();

        // Field 0: int = 42
        data.extend_from_slice(&encode_vint(4));
        data.extend_from_slice(&42i32.to_be_bytes());

        // Field 1: text = "hello"
        let text = "hello";
        data.extend_from_slice(&encode_vint(text.len() as i64));
        data.extend_from_slice(text.as_bytes());

        let field_comparators = vec![ComparatorType::Int, ComparatorType::Text];

        let result = parse_tuple_value_with(&data, &field_comparators, |d, comp| match comp {
            ComparatorType::Int => parse_int_value(d),
            ComparatorType::Text => parse_text_value(d),
            _ => panic!("Unexpected comparator"),
        })
        .unwrap();

        if let Value::Tuple(fields) = result {
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0], Value::Integer(42));
            assert_eq!(fields[1], Value::Text("hello".to_string()));
        } else {
            panic!("Expected Tuple value");
        }
    }

    #[test]
    fn test_parse_tuple_with_null() {
        // Note: In Cassandra tuples, null fields are represented by negative length
        // However, parse_vint_length will reject negative values
        // For this test, we'll test a tuple with a field that has 0-length data
        let mut data = Vec::new();

        // Field 0: int = 42
        data.extend_from_slice(&encode_vint(4));
        data.extend_from_slice(&42i32.to_be_bytes());

        // Field 1: empty text (0 bytes)
        data.extend_from_slice(&encode_vint(0));

        let field_comparators = vec![ComparatorType::Int, ComparatorType::Text];

        let result = parse_tuple_value_with(&data, &field_comparators, |d, comp| match comp {
            ComparatorType::Int => parse_int_value(d),
            ComparatorType::Text => {
                if d.is_empty() {
                    Ok(Value::Text(String::new()))
                } else {
                    parse_text_value(d)
                }
            }
            _ => panic!("Unexpected comparator"),
        })
        .unwrap();

        if let Value::Tuple(fields) = result {
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0], Value::Integer(42));
            assert_eq!(fields[1], Value::Text(String::new()));
        } else {
            panic!("Expected Tuple value");
        }
    }

    #[test]
    fn test_parse_udt_simple() {
        let mut data = Vec::new();

        // Field "id": int = 123
        data.extend_from_slice(&encode_vint(4));
        data.extend_from_slice(&123i32.to_be_bytes());

        // Field "name": text = "Alice"
        let name = "Alice";
        data.extend_from_slice(&encode_vint(name.len() as i64));
        data.extend_from_slice(name.as_bytes());

        let field_comparators = vec![
            ("id".to_string(), ComparatorType::Int),
            ("name".to_string(), ComparatorType::Text),
        ];

        let result = parse_udt_value_with(&data, &field_comparators, |d, comp| match comp {
            ComparatorType::Int => parse_int_value(d),
            ComparatorType::Text => parse_text_value(d),
            _ => panic!("Unexpected comparator"),
        })
        .unwrap();

        assert_eq!(result.fields.len(), 2);
        assert_eq!(result.fields[0].name, "id");
        assert_eq!(result.fields[0].value, Some(Value::Integer(123)));
        assert_eq!(result.fields[1].name, "name");
        assert_eq!(
            result.fields[1].value,
            Some(Value::Text("Alice".to_string()))
        );
    }

    #[test]
    fn test_parse_udt_with_collection() {
        let mut data = Vec::new();

        // Field "id": int = 456
        data.extend_from_slice(&encode_vint(4));
        data.extend_from_slice(&456i32.to_be_bytes());

        // Field "tags": list<text> = ["tag1", "tag2"]
        let mut list_data = Vec::new();
        list_data.extend_from_slice(&encode_vint(2)); // 2 elements
        for tag in ["tag1", "tag2"] {
            list_data.extend_from_slice(&encode_vint(tag.len() as i64));
            list_data.extend_from_slice(tag.as_bytes());
        }
        data.extend_from_slice(&encode_vint(list_data.len() as i64));
        data.extend_from_slice(&list_data);

        let field_comparators = vec![
            ("id".to_string(), ComparatorType::Int),
            (
                "tags".to_string(),
                ComparatorType::List(Box::new(ComparatorType::Text)),
            ),
        ];

        let result = parse_udt_value_with(&data, &field_comparators, |d, comp| match comp {
            ComparatorType::Int => parse_int_value(d),
            ComparatorType::List(inner) => {
                parse_list_value_with(d, inner, |inner_d, inner_comp| match inner_comp {
                    ComparatorType::Text => parse_text_value(inner_d),
                    _ => panic!("Unexpected inner comparator"),
                })
            }
            _ => panic!("Unexpected comparator"),
        })
        .unwrap();

        assert_eq!(result.fields.len(), 2);
        assert_eq!(result.fields[0].name, "id");
        assert_eq!(result.fields[0].value, Some(Value::Integer(456)));
        assert_eq!(result.fields[1].name, "tags");
        if let Some(Value::List(tags)) = &result.fields[1].value {
            assert_eq!(tags.len(), 2);
            assert_eq!(tags[0], Value::Text("tag1".to_string()));
            assert_eq!(tags[1], Value::Text("tag2".to_string()));
        } else {
            panic!("Expected List value in UDT");
        }
    }

    // ============================================================================
    // Error Cases
    // ============================================================================

    #[test]
    fn test_parse_int_wrong_length() {
        let data = [0x00, 0x00, 0x2A]; // Only 3 bytes instead of 4
        let result = parse_int_value(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_text_invalid_utf8() {
        let data = [0xFF, 0xFE, 0xFD]; // Invalid UTF-8 sequence
        let result = parse_text_value(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_uuid_wrong_length() {
        let data = [
            0x12, 0x34, 0x56, 0x78, 0x90, 0xAB, 0xCD, 0xEF, 0x12, 0x34, 0x56, 0x78, 0x90, 0xAB,
            0xCD,
        ]; // Only 15 bytes
        let result = parse_uuid_value(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_list_truncated() {
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(2)); // Claims 2 elements
        data.extend_from_slice(&encode_vint(4)); // First element length
        data.extend_from_slice(&42i32.to_be_bytes());
        // Missing second element

        let result =
            parse_list_value_with(&data, &ComparatorType::Int, |d, _| parse_int_value(d)).unwrap();

        // Should successfully parse 1 element (stops when no more data)
        if let Value::List(elements) = result {
            assert_eq!(elements.len(), 1);
        } else {
            panic!("Expected List value");
        }
    }

    #[test]
    fn test_parse_boolean_wrong_length() {
        let data = [0x01, 0x02]; // 2 bytes instead of 1
        let result = parse_boolean_value(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_date_wrong_length() {
        let data = [0x00, 0x00, 0x00]; // Only 3 bytes instead of 4
        let result = parse_date_value(&data);
        assert!(result.is_err());
    }

    // Issue #264: Blob fallback disabled test - type-specific parsers enforce strict validation
    #[test]
    fn test_value_parsing_blob_fallback_disabled() {
        // Test that type-specific parsers reject invalid data lengths
        // This validates strict type checking - no silent blob fallback

        // Smallint requires exactly 2 bytes
        let wrong_size_smallint = [0x00]; // 1 byte
        let result = parse_smallint_value(&wrong_size_smallint);
        assert!(result.is_err(), "Smallint should reject 1-byte input");

        // Bigint requires exactly 8 bytes
        let wrong_size_bigint = [0x00, 0x00, 0x00, 0x00]; // 4 bytes
        let result = parse_bigint_value(&wrong_size_bigint);
        assert!(result.is_err(), "Bigint should reject 4-byte input");

        // Tinyint requires exactly 1 byte
        let wrong_size_tinyint = [0x00, 0x01]; // 2 bytes
        let result = parse_tinyint_value(&wrong_size_tinyint);
        assert!(result.is_err(), "Tinyint should reject 2-byte input");

        // Note: Float and Double parsing is handled by the main parser (parse_value_by_type)
        // rather than standalone functions, so they are not tested here.
    }
}
