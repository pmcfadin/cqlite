//! Standalone comparator-based value parsing for V5 format
//!
//! This module provides schema-aware value parsing that can be used independently
//! of the SSTableReader, making it suitable for use in the row_cell_state_machine.
//!
//! Unlike the `parse_cql_value_raw` function which only uses CqlTypeId,
//! this module uses full ComparatorType information to properly parse nested
//! structures like UDTs inside collections.

use crate::{
    parser::vint::parse_vint_length,
    types::{ComparatorType, UdtField, UdtValue, Value},
    Error, Result,
};

/// Parse a value using a ComparatorType for schema-aware decoding
///
/// This function handles all CQL types including:
/// - Primitive types (int, text, boolean, etc.)
/// - Collections (list, set, map) with proper element type parsing
/// - UDTs with recursive field parsing
/// - Frozen types
/// - Nested combinations (e.g., list<frozen<udt>>)
///
/// # Arguments
/// * `value_data` - The raw bytes to parse
/// * `comparator` - The type information for parsing
///
/// # Returns
/// The parsed Value or an error if parsing fails
pub fn parse_value_with_comparator(
    value_data: &[u8],
    comparator: &ComparatorType,
) -> Result<Value> {
    match comparator {
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
        ComparatorType::Counter => {
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
                Ok(Value::Counter(val))
            } else {
                Err(Error::corruption("Invalid counter value length"))
            }
        }
        ComparatorType::Float32 => {
            if value_data.len() == 4 {
                let val = f32::from_be_bytes([
                    value_data[0],
                    value_data[1],
                    value_data[2],
                    value_data[3],
                ]);
                Ok(Value::Float32(val))
            } else {
                Err(Error::corruption("Invalid float value length"))
            }
        }
        ComparatorType::Float => {
            if value_data.len() == 8 {
                let val = f64::from_be_bytes([
                    value_data[0],
                    value_data[1],
                    value_data[2],
                    value_data[3],
                    value_data[4],
                    value_data[5],
                    value_data[6],
                    value_data[7],
                ]);
                Ok(Value::Float(val))
            } else {
                Err(Error::corruption("Invalid double value length"))
            }
        }
        ComparatorType::Text => {
            let text = String::from_utf8(value_data.to_vec())
                .map_err(|_| Error::corruption("Invalid UTF-8 in text value"))?;
            Ok(Value::Text(text))
        }
        ComparatorType::Blob => Ok(Value::Blob(value_data.to_vec())),
        ComparatorType::Timestamp => {
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
                Ok(Value::Timestamp(val))
            } else {
                Err(Error::corruption("Invalid timestamp value length"))
            }
        }
        ComparatorType::Uuid => {
            if value_data.len() == 16 {
                let uuid_bytes: [u8; 16] = value_data
                    .try_into()
                    .map_err(|_| Error::corruption("Invalid UUID bytes"))?;
                Ok(Value::Uuid(uuid_bytes))
            } else {
                Err(Error::corruption("Invalid UUID value length"))
            }
        }
        ComparatorType::Date => {
            if value_data.len() == 4 {
                // Cassandra DATE: 4-byte big-endian unsigned int with Integer.MIN_VALUE offset
                // for byte-order comparability. Decode by adding i32::MIN back.
                let stored = u32::from_be_bytes([
                    value_data[0],
                    value_data[1],
                    value_data[2],
                    value_data[3],
                ]);
                let days_since_epoch = stored.wrapping_add(i32::MIN as u32) as i32;
                Ok(Value::Date(days_since_epoch))
            } else {
                Err(Error::corruption("Invalid DATE value length"))
            }
        }
        ComparatorType::Varint => Ok(Value::Varint(value_data.to_vec())),
        ComparatorType::Decimal => {
            // Decimal format: 4-byte scale + variable-length unscaled value
            if value_data.len() < 4 {
                return Err(Error::corruption("Invalid decimal value length"));
            }
            let scale =
                i32::from_be_bytes([value_data[0], value_data[1], value_data[2], value_data[3]]);
            let unscaled = value_data[4..].to_vec();
            Ok(Value::Decimal { scale, unscaled })
        }
        ComparatorType::Duration => {
            // Duration format: 3 signed vints: months, days, nanoseconds
            // For now, store as blob until proper vint parsing is needed
            // TODO: Parse months, days, nanos as vints
            Ok(Value::Blob(value_data.to_vec()))
        }
        ComparatorType::Json => {
            let json_text = String::from_utf8(value_data.to_vec())
                .map_err(|_| Error::corruption("Invalid UTF-8 in JSON value"))?;
            let json_value: serde_json::Value = serde_json::from_str(&json_text)
                .map_err(|_| Error::corruption("Invalid JSON value"))?;
            Ok(Value::Json(json_value))
        }
        ComparatorType::List(element_comparator) => {
            parse_list_value(value_data, element_comparator)
        }
        ComparatorType::Set(element_comparator) => parse_set_value(value_data, element_comparator),
        ComparatorType::Map(key_comparator, value_comparator) => {
            parse_map_value(value_data, key_comparator, value_comparator)
        }
        ComparatorType::Tuple(field_comparators) => {
            parse_tuple_value(value_data, field_comparators)
        }
        ComparatorType::Udt {
            type_name,
            keyspace,
            field_comparators,
        } => parse_udt_value(value_data, type_name, keyspace, field_comparators),
        ComparatorType::Frozen(inner_comparator) => {
            let inner_value = parse_value_with_comparator(value_data, inner_comparator)?;
            Ok(Value::Frozen(Box::new(inner_value)))
        }
        ComparatorType::Custom(_) => {
            // Custom types are stored as blobs
            Ok(Value::Blob(value_data.to_vec()))
        }
    }
}

/// Parse a list value using the element comparator
fn parse_list_value(value_data: &[u8], element_comparator: &ComparatorType) -> Result<Value> {
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

        // Parse element value using element comparator (recursive)
        let element_data = &remaining[..element_len];
        let element_value = parse_value_with_comparator(element_data, element_comparator)?;
        elements.push(element_value);
        offset += element_len;
    }

    Ok(Value::List(elements))
}

/// Parse a set value using the element comparator
fn parse_set_value(value_data: &[u8], element_comparator: &ComparatorType) -> Result<Value> {
    // Sets are parsed similarly to lists
    let list_value = parse_list_value(value_data, element_comparator)?;
    if let Value::List(elements) = list_value {
        Ok(Value::Set(elements))
    } else {
        Err(Error::corruption("Failed to parse set value"))
    }
}

/// Parse a map value using key and value comparators
fn parse_map_value(
    value_data: &[u8],
    key_comparator: &ComparatorType,
    value_comparator: &ComparatorType,
) -> Result<Value> {
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
        let key_value = parse_value_with_comparator(key_data, key_comparator)?;
        offset += key_len;

        // Parse value length and data
        let (remaining, value_len) = parse_vint_length(&value_data[offset..])
            .map_err(|_| Error::corruption("Failed to parse map value length"))?;
        offset = value_data.len() - remaining.len();

        if value_len > remaining.len() {
            return Err(Error::corruption("Map value length exceeds available data"));
        }

        let value_data_slice = &remaining[..value_len];
        let value = parse_value_with_comparator(value_data_slice, value_comparator)?;
        offset += value_len;

        entries.push((key_value, value));
    }

    Ok(Value::Map(entries))
}

/// Parse a tuple value using field comparators
fn parse_tuple_value(value_data: &[u8], field_comparators: &[ComparatorType]) -> Result<Value> {
    let mut offset = 0;
    let mut fields = Vec::new();

    // Parse each field
    for field_comparator in field_comparators {
        if offset >= value_data.len() {
            // Remaining fields are null
            fields.push(Value::Null);
            continue;
        }

        // Parse field length
        let (remaining, field_len) = parse_vint_length(&value_data[offset..])
            .map_err(|_| Error::corruption("Failed to parse tuple field length"))?;
        offset = value_data.len() - remaining.len();

        if field_len > remaining.len() {
            return Err(Error::corruption(
                "Tuple field length exceeds available data",
            ));
        }

        // Parse field value using field comparator (recursive)
        let field_data = &remaining[..field_len];
        let field_value = parse_value_with_comparator(field_data, field_comparator)?;
        fields.push(field_value);
        offset += field_len;
    }

    Ok(Value::Tuple(fields))
}

/// Parse a UDT value using field comparators
fn parse_udt_value(
    value_data: &[u8],
    type_name: &str,
    keyspace: &Option<String>,
    field_comparators: &[(String, ComparatorType)],
) -> Result<Value> {
    let mut offset = 0;
    let mut fields = Vec::new();

    // Parse each field
    for (field_name, field_comparator) in field_comparators {
        if offset >= value_data.len() {
            // Remaining fields are null
            fields.push(UdtField {
                name: field_name.clone(),
                value: None,
            });
            continue;
        }

        // Parse field length
        let (remaining, field_len) = parse_vint_length(&value_data[offset..]).map_err(|_| {
            Error::corruption(format!("Failed to parse UDT field {} length", field_name))
        })?;
        offset = value_data.len() - remaining.len();

        if field_len > remaining.len() {
            return Err(Error::corruption(format!(
                "UDT field {} length exceeds available data",
                field_name
            )));
        }

        // Parse field value using field comparator (recursive)
        let field_data = &remaining[..field_len];
        let field_value = parse_value_with_comparator(field_data, field_comparator)?;

        fields.push(UdtField {
            name: field_name.clone(),
            value: Some(field_value),
        });
        offset += field_len;
    }

    Ok(Value::Udt(UdtValue {
        keyspace: keyspace.clone().unwrap_or_else(|| "unknown".to_string()),
        type_name: type_name.to_string(),
        fields,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_int() {
        let data = vec![0x00, 0x00, 0x00, 0x2A]; // 42 in big-endian
        let comparator = ComparatorType::Int;
        let result = parse_value_with_comparator(&data, &comparator).unwrap();
        assert_eq!(result, Value::Integer(42));
    }

    #[test]
    fn test_parse_int_value_1() {
        let data = vec![0x00, 0x00, 0x00, 0x01]; // 1 in big-endian
        let comparator = ComparatorType::Int;
        let result = parse_value_with_comparator(&data, &comparator).unwrap();
        assert_eq!(result, Value::Integer(1));
    }

    #[test]
    fn test_parse_text() {
        let data = b"hello".to_vec();
        let comparator = ComparatorType::Text;
        let result = parse_value_with_comparator(&data, &comparator).unwrap();
        assert_eq!(result, Value::Text("hello".to_string()));
    }

    #[test]
    fn test_parse_list_of_ints() {
        // List with 2 elements: [1, 2]
        // Format: element_count (VInt) + (element_length (VInt) + element_bytes)*
        // Note: VInt uses zigzag encoding, so value 4 is encoded as 8 (0x08)
        let mut data = vec![
            0x04, // count = 2 (zigzag_encode(2) = 4)
        ];
        // Element 1: length=4, value=0x00000001 (1 as big-endian i32)
        data.push(0x08); // length (zigzag_encode(4) = 8)
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]); // value
                                                           // Element 2: length=4, value=0x00000002 (2 as big-endian i32)
        data.push(0x08); // length (zigzag_encode(4) = 8)
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x02]); // value

        let comparator = ComparatorType::List(Box::new(ComparatorType::Int));
        let result = parse_value_with_comparator(&data, &comparator).unwrap();

        if let Value::List(elements) = result {
            assert_eq!(elements.len(), 2);
            assert_eq!(elements[0], Value::Integer(1));
            assert_eq!(elements[1], Value::Integer(2));
        } else {
            panic!("Expected List value");
        }
    }
}
