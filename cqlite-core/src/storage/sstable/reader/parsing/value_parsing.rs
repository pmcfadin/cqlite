//! Value parsing using schema-driven approach
//!
//! This module handles parsing of column values using exact schema types,
//! including collection types (list, set, map), tuples, and UDTs.

use crate::{
    types::{ComparatorType, TableId, UdtField, UdtValue},
    Error, Result, RowKey, Value,
};

use super::super::types::SSTableReader;
use super::comparator_value_parsing::parse_value_with_comparator as decode_scalar_comparator;
use super::custom_scalar::decode_custom_scalar;

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
///
/// Cassandra tuple field lengths are encoded as 4-byte big-endian signed int32
/// (TupleType.java uses `accessor.putInt`), with -1 meaning null.
/// This is NOT VInt encoding.
pub(crate) fn parse_tuple_value_with<F>(
    data: &[u8],
    field_comparators: &[ComparatorType],
    parse_element: F,
) -> Result<Value>
where
    F: Fn(&[u8], &ComparatorType) -> Result<Value>,
{
    let mut offset = 0;
    let mut fields = Vec::new();

    // Parse each field
    for (i, field_comparator) in field_comparators.iter().enumerate() {
        if offset >= data.len() {
            break;
        }

        // Parse field length as 4-byte big-endian signed int32 (Cassandra specification)
        // -1 = null field, 0 = empty field, >0 = byte count
        if offset + 4 > data.len() {
            return Err(Error::corruption(format!(
                "Tuple field {} length prefix truncated",
                i
            )));
        }
        let field_len_i32 =
            i32::from_be_bytes(data[offset..offset + 4].try_into().map_err(|_| {
                Error::corruption(format!("Tuple field {} length bytes invalid", i))
            })?);
        offset += 4;

        if field_len_i32 == -1 {
            // Null field
            fields.push(Value::Null);
            continue;
        }
        if field_len_i32 < 0 {
            return Err(Error::corruption(format!(
                "Tuple field {} has invalid negative length {}",
                i, field_len_i32
            )));
        }
        let field_len = field_len_i32 as usize;

        if offset + field_len > data.len() {
            return Err(Error::corruption(format!(
                "Tuple field {} length {} exceeds available data",
                i, field_len
            )));
        }

        // Parse field value using provided closure
        let field_data = &data[offset..offset + field_len];
        let field_value = parse_element(field_data, field_comparator)?;
        fields.push(field_value);
        offset += field_len;
    }

    Ok(Value::Tuple(fields))
}

/// Parse UDT value with recursive field parsing via closure
///
/// Cassandra UDT field lengths are encoded as 4-byte big-endian signed int32
/// (TupleType.java / UserType.java use `accessor.putInt`), with -1 meaning null.
/// This is NOT VInt encoding.
#[allow(dead_code)] // Used in tests; may be used by future refactoring
pub(crate) fn parse_udt_value_with<F>(
    data: &[u8],
    field_comparators: &[(String, ComparatorType)],
    parse_element: F,
) -> Result<UdtValue>
where
    F: Fn(&[u8], &ComparatorType) -> Result<Value>,
{
    let mut offset = 0;
    let mut fields = Vec::new();

    // Parse each field
    for (field_name, field_comparator) in field_comparators.iter() {
        if offset >= data.len() {
            break;
        }

        // Parse field length as 4-byte big-endian signed int32 (Cassandra specification)
        // -1 = null field, 0 = empty field, >0 = byte count
        if offset + 4 > data.len() {
            return Err(Error::corruption(format!(
                "UDT field {} length prefix truncated",
                field_name
            )));
        }
        let field_len_i32 =
            i32::from_be_bytes(data[offset..offset + 4].try_into().map_err(|_| {
                Error::corruption(format!("UDT field {} length bytes invalid", field_name))
            })?);
        offset += 4;

        if field_len_i32 == -1 {
            // Null field
            fields.push(UdtField {
                name: field_name.clone(),
                value: None,
            });
            continue;
        }
        if field_len_i32 < 0 {
            return Err(Error::corruption(format!(
                "UDT field {} has invalid negative length {}",
                field_name, field_len_i32
            )));
        }
        let field_len = field_len_i32 as usize;

        if offset + field_len > data.len() {
            return Err(Error::corruption(format!(
                "UDT field {} length {} exceeds available data",
                field_name, field_len
            )));
        }

        // Parse field value using provided closure
        let field_data = &data[offset..offset + field_len];
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
            // Scalar types with no self-recursion: delegate to the authoritative
            // standalone decoder (issue #1627 — these previously fell through to a
            // wrong-typed Value::Blob).
            ComparatorType::Float32
            | ComparatorType::Float
            | ComparatorType::Timestamp
            | ComparatorType::Varint
            | ComparatorType::Decimal
            | ComparatorType::Duration
            | ComparatorType::Json => decode_scalar_comparator(value_data, &comparator),
            // `time`/`inet` arrive as schema-derived Custom(name); genuinely-unknown
            // custom types remain the only legitimate blob fallback.
            ComparatorType::Custom(name) => decode_custom_scalar(name, value_data),
        }
    }

    /// Bench-only forwarder to the crate-private block-path decode entry
    /// [`parse_value_with_schema_type`](Self::parse_value_with_schema_type).
    ///
    /// `parse_value_with_schema_type` is `pub(in crate::storage::sstable::reader)`,
    /// so the `decode` bench (issue #1615, Epic H) — an external crate — cannot call
    /// it directly. This shim exposes the REAL dispatch (not a re-implemented copy)
    /// through a single `#[doc(hidden)]`, non-default, `bench-internals`-gated symbol,
    /// forwarding its arguments verbatim. It adds no behavior: with the feature off it
    /// does not exist and the public API is unchanged.
    #[cfg(feature = "bench-internals")]
    #[doc(hidden)]
    pub fn decode_value_for_bench(&self, value_data: &[u8], data_type: &str) -> Result<Value> {
        self.parse_value_with_schema_type(value_data, data_type)
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
                // Field lengths are 4-byte big-endian signed int32 per Cassandra specification.
                let mut offset = 0;
                let mut fields = Vec::new();

                for (field_name, field_comparator) in field_comparators.iter() {
                    if offset >= value_data.len() {
                        break;
                    }
                    // Parse field length as 4-byte big-endian signed int32 (Cassandra spec)
                    if offset + 4 > value_data.len() {
                        return Err(Error::corruption(format!(
                            "UDT field {} length prefix truncated",
                            field_name
                        )));
                    }
                    let field_len_i32 = i32::from_be_bytes(
                        value_data[offset..offset + 4].try_into().map_err(|_| {
                            Error::corruption(format!("UDT field {} length invalid", field_name))
                        })?,
                    );
                    offset += 4;

                    if field_len_i32 == -1 {
                        fields.push(UdtField {
                            name: field_name.clone(),
                            value: None,
                        });
                        continue;
                    }
                    if field_len_i32 < 0 {
                        return Err(Error::corruption(format!(
                            "UDT field {} has invalid negative length {}",
                            field_name, field_len_i32
                        )));
                    }
                    let field_len = field_len_i32 as usize;

                    if offset + field_len > value_data.len() {
                        return Err(Error::corruption(format!(
                            "UDT field {} length {} exceeds available data",
                            field_name, field_len
                        )));
                    }

                    let field_data = &value_data[offset..offset + field_len];
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
            // Scalar collection elements: delegate to the authoritative standalone
            // decoder rather than blob-decoding them (issue #1627, same defect
            // class as parse_value_with_schema_type).
            ComparatorType::Float32
            | ComparatorType::Float
            | ComparatorType::Timestamp
            | ComparatorType::Varint
            | ComparatorType::Decimal
            | ComparatorType::Duration
            | ComparatorType::Json => decode_scalar_comparator(value_data, comparator),
            // `time`/`inet` as schema-derived Custom(name); unknown custom → blob.
            ComparatorType::Custom(name) => decode_custom_scalar(name, value_data),
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
    ///
    /// Cassandra UDT field lengths are 4-byte big-endian signed int32 (not VInt).
    pub(in crate::storage::sstable::reader) fn parse_udt_value(
        &self,
        value_data: &[u8],
        field_comparators: &[(String, ComparatorType)],
    ) -> Result<Value> {
        let mut offset = 0;
        let mut fields = Vec::new();

        // Parse each field
        for (field_name, field_comparator) in field_comparators.iter() {
            if offset >= value_data.len() {
                break;
            }

            // Parse field length as 4-byte big-endian signed int32 (Cassandra specification)
            if offset + 4 > value_data.len() {
                return Err(Error::corruption(format!(
                    "UDT field {} length prefix truncated",
                    field_name
                )));
            }
            let field_len_i32 =
                i32::from_be_bytes(value_data[offset..offset + 4].try_into().map_err(|_| {
                    Error::corruption(format!("UDT field {} length invalid", field_name))
                })?);
            offset += 4;

            if field_len_i32 == -1 {
                fields.push(UdtField {
                    name: field_name.clone(),
                    value: None,
                });
                continue;
            }
            if field_len_i32 < 0 {
                return Err(Error::corruption(format!(
                    "UDT field {} has invalid negative length {}",
                    field_name, field_len_i32
                )));
            }
            let field_len = field_len_i32 as usize;

            if offset + field_len > value_data.len() {
                return Err(Error::corruption(format!(
                    "UDT field {} length {} exceeds available data",
                    field_name, field_len
                )));
            }

            // Parse field value using field comparator
            let field_data = &value_data[offset..offset + field_len];
            let field_value = self.parse_value_with_comparator(field_data, field_comparator)?;
            offset += field_len;

            fields.push(UdtField {
                name: field_name.clone(),
                value: Some(field_value),
            });
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
#[path = "value_parsing_tests.rs"]
mod tests;
