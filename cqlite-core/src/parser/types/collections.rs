//! Collection CQL type decoding: lists, sets, and maps.
//!
//! Covers the legacy collection layout, the Cassandra 5+ (`v5`) cell-aware
//! layout, registry-aware element decoding (for collections of UDTs), and the
//! schema-driven decoders used by the no-heuristics read path.

// Issue #1623: `*_v5_format` parsers read RAW Cassandra collection cells whose
// counts/lengths are unsigned (`parse_vint_length`). The legacy/schema/registry
// parsers (`parse_list`, `parse_map`, `parse_list_with_element_type`,
// `parse_map_with_types`, `parse_*_with_schema`) round-trip with CQLite's own
// ZigZag serializer (`serialize_cql_value`) and use signed element lengths
// (-1 = null via `parse_vint`); their COUNT reads therefore use the signed
// helper to stay self-consistent.
use super::super::vint::{parse_vint, parse_vint_length, parse_vint_length_signed};
// #3848: `take_vint_length` is the SINGLE place a signed VInt length is
// narrowed — it rejects a negative length AND one wider than `usize` on a
// 32-bit target, where `take(len as usize)` would silently truncate instead.
use super::super::vint_narrow::take_vint_length;
use super::udt::parse_cql_value_for_type_with_registry;
use super::{
    create_empty_value_for_cql_type, parse_cql_value, parse_cql_value_raw,
    parse_cql_value_with_schema, validate_element_consumption, CqlTypeId,
};
use crate::{
    error::Result,
    schema::{CqlType, UdtRegistry},
    types::Value,
};
use nom::{
    bytes::complete::take,
    number::complete::{be_i32, be_u8},
    IResult,
};

/// Maximum collection element/entry count, to bound memory on corrupt input.
const MAX_COLLECTION_SIZE: usize = 1_000_000;

/// Parse list using enhanced Cassandra 5+ parser with fallback to legacy format
pub fn parse_list_enhanced(input: &[u8]) -> IResult<&[u8], Value> {
    // Try Cassandra 5+ format first, fall back to legacy on failure
    match parse_list_v5_format(input) {
        Ok(result) => Ok(result),
        Err(_) => parse_list(input), // Fallback to legacy format
    }
}

/// Legacy list parser for backward compatibility
pub fn parse_list(input: &[u8]) -> IResult<&[u8], Value> {
    let (input, count) = parse_vint_length_signed(input)?;

    // Validate count to prevent memory exhaustion attacks
    if count > MAX_COLLECTION_SIZE {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::TooLarge,
        )));
    }

    if count == 0 {
        return Ok((input, Value::List(Vec::new())));
    }

    let (input, element_type) = super::parse_cql_type_id(input)?;

    let mut elements = Vec::with_capacity(count);
    let mut remaining = input;

    for _ in 0..count {
        // Parse element length prefix using VInt (which can be negative for null)
        let (new_remaining, element_length) = parse_vint(remaining)?;
        remaining = new_remaining;

        let element = if element_length == -1 {
            Value::Null // Null element
        } else {
            let (new_remaining, element_data) = take_vint_length(element_length)(remaining)?;
            remaining = new_remaining;

            // Handle nested collections correctly
            match element_type {
                CqlTypeId::List => parse_list(element_data)?.1,
                CqlTypeId::Set => parse_set(element_data)?.1,
                CqlTypeId::Map => parse_map(element_data)?.1,
                _ => parse_cql_value_raw(element_data, element_type)?.1,
            }
        };

        elements.push(element);
    }

    Ok((remaining, Value::List(elements)))
}

/// Parse set using enhanced Cassandra 5+ parser with fallback to legacy format
pub fn parse_set_enhanced(input: &[u8]) -> IResult<&[u8], Value> {
    // Try Cassandra 5+ format first, fall back to legacy on failure
    match parse_set_v5_format(input) {
        Ok(result) => Ok(result),
        Err(_) => parse_set(input), // Fallback to legacy format
    }
}

/// Legacy set parser for backward compatibility
pub fn parse_set(input: &[u8]) -> IResult<&[u8], Value> {
    let (remaining, list_value) = parse_list(input)?;

    if let Value::List(elements) = list_value {
        // Convert to Set - in Cassandra, sets maintain insertion order but enforce uniqueness
        // We preserve the order as read from the SSTable for compatibility
        Ok((remaining, Value::Set(elements)))
    } else {
        Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )))
    }
}

/// Parse map using enhanced Cassandra 5+ parser with fallback to legacy format
pub fn parse_map_enhanced(input: &[u8]) -> IResult<&[u8], Value> {
    // Try Cassandra 5+ format first, fall back to legacy on failure
    match parse_map_v5_format(input) {
        Ok(result) => Ok(result),
        Err(_) => parse_map(input), // Fallback to legacy format
    }
}

/// Legacy map parser for backward compatibility
pub fn parse_map(input: &[u8]) -> IResult<&[u8], Value> {
    let (input, count) = parse_vint_length_signed(input)?;

    // Validate count to prevent memory exhaustion
    if count > MAX_COLLECTION_SIZE {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::TooLarge,
        )));
    }

    if count == 0 {
        return Ok((input, Value::Map(Vec::new())));
    }

    let (input, key_type) = super::parse_cql_type_id(input)?;
    let (input, value_type) = super::parse_cql_type_id(input)?;

    let mut map = Vec::with_capacity(count);
    let mut remaining = input;

    for _ in 0..count {
        // Parse key length prefix
        let (new_remaining, key_length) = parse_vint(remaining)?;
        remaining = new_remaining;

        let key = if key_length == -1 {
            Value::Null // Null key (unusual but possible)
        } else {
            let (new_remaining, key_data) = take_vint_length(key_length)(remaining)?;
            remaining = new_remaining;
            parse_cql_value_raw(key_data, key_type)?.1
        };

        // Parse value length prefix
        let (new_remaining, value_length) = parse_vint(remaining)?;
        remaining = new_remaining;

        let value = if value_length == -1 {
            Value::Null // Null value
        } else {
            let (new_remaining, value_data) = take_vint_length(value_length)(remaining)?;
            remaining = new_remaining;
            parse_cql_value_raw(value_data, value_type)?.1
        };

        map.push((key, value));
    }

    Ok((remaining, Value::Map(map)))
}

/// Parse list with specific element type (including UDTs)
pub(super) fn parse_list_with_element_type<'a>(
    input: &'a [u8],
    element_type: &CqlType,
    keyspace: &str,
    registry: &UdtRegistry,
) -> IResult<&'a [u8], Value> {
    let (input, count) = parse_vint_length_signed(input)?;

    // Validate count to prevent memory exhaustion attacks
    if count > MAX_COLLECTION_SIZE {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::TooLarge,
        )));
    }

    if count == 0 {
        return Ok((input, Value::List(Vec::new())));
    }

    let mut elements = Vec::with_capacity(count);
    let mut remaining = input;

    for _ in 0..count {
        // Parse element length
        let (new_remaining, element_length) = be_i32(remaining)?;
        remaining = new_remaining;

        if element_length > 0 {
            let (new_remaining, element_data) = take(element_length as usize)(remaining)?;
            remaining = new_remaining;

            let element_value = parse_cql_value_for_type_with_registry(
                element_data,
                element_type,
                keyspace,
                registry,
            )
            .map_err(|_| {
                nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
            })?;
            elements.push(element_value);
        } else if element_length == 0 {
            // Empty element
            let empty_value = create_empty_value_for_cql_type(element_type).map_err(|_| {
                nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
            })?;
            elements.push(empty_value);
        } else {
            // Null element - skip for lists (null elements typically not allowed)
            // Note: Some Cassandra versions may handle this differently
        }
    }

    Ok((remaining, Value::List(elements)))
}

/// Parse set with specific element type (including UDTs)
pub(super) fn parse_set_with_element_type<'a>(
    input: &'a [u8],
    element_type: &CqlType,
    keyspace: &str,
    registry: &UdtRegistry,
) -> IResult<&'a [u8], Value> {
    let (remaining, list_value) =
        parse_list_with_element_type(input, element_type, keyspace, registry)?;

    if let Value::List(elements) = list_value {
        // Convert to Set - in Cassandra, sets maintain insertion order but enforce uniqueness
        Ok((remaining, Value::Set(elements)))
    } else {
        Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )))
    }
}

/// Parse map with specific key and value types (including UDTs)
pub(super) fn parse_map_with_types<'a>(
    input: &'a [u8],
    key_type: &CqlType,
    value_type: &CqlType,
    keyspace: &str,
    registry: &UdtRegistry,
) -> IResult<&'a [u8], Value> {
    let (input, count) = parse_vint_length_signed(input)?;

    // Validate count to prevent memory exhaustion
    if count > MAX_COLLECTION_SIZE {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::TooLarge,
        )));
    }

    if count == 0 {
        return Ok((input, Value::Map(Vec::new())));
    }

    let mut map = Vec::with_capacity(count);
    let mut remaining = input;

    for _ in 0..count {
        // Parse key
        let (new_remaining, key_length) = be_i32(remaining)?;
        remaining = new_remaining;

        let key = if key_length > 0 {
            let (new_remaining, key_data) = take(key_length as usize)(remaining)?;
            remaining = new_remaining;
            parse_cql_value_for_type_with_registry(key_data, key_type, keyspace, registry).map_err(
                |_| nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify)),
            )?
        } else {
            create_empty_value_for_cql_type(key_type).map_err(|_| {
                nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
            })?
        };

        // Parse value
        let (new_remaining, value_length) = be_i32(remaining)?;
        remaining = new_remaining;

        let value = if value_length > 0 {
            let (new_remaining, value_data) = take(value_length as usize)(remaining)?;
            remaining = new_remaining;
            parse_cql_value_for_type_with_registry(value_data, value_type, keyspace, registry)
                .map_err(|_| {
                    nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
                })?
        } else {
            create_empty_value_for_cql_type(value_type).map_err(|_| {
                nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
            })?
        };

        map.push((key, value));
    }

    Ok((remaining, Value::Map(map)))
}

/// Parse List with enhanced Cassandra 5+ format supporting mixed types and proper cell handling
pub fn parse_list_v5_format(input: &[u8]) -> IResult<&[u8], Value> {
    // Enhanced Cassandra 5+ format with proper cell metadata handling
    let (input, count) = parse_vint_length(input)?;

    // Validate count to prevent memory exhaustion attacks
    if count > MAX_COLLECTION_SIZE {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::TooLarge,
        )));
    }

    if count == 0 {
        return Ok((input, Value::List(Vec::new())));
    }

    // Check if this is a mixed-type collection (Cassandra 5.0 feature)
    let (mut remaining, format_flags) = be_u8(input)?;
    let has_mixed_types = (format_flags & 0x01) != 0;

    let mut elements = Vec::with_capacity(count);

    if has_mixed_types {
        // Each element has its own type information
        for _ in 0..count {
            let (new_remaining, element_type_id) = super::parse_cql_type_id(remaining)?;
            let (new_remaining, element_length) = parse_vint_length(new_remaining)?;

            if element_length > 0 {
                let (new_remaining, element_data) = take(element_length)(new_remaining)?;
                let element_value =
                    parse_cql_value_with_cell_metadata(element_data, element_type_id).map_err(
                        |_e| {
                            nom::Err::Error(nom::error::Error::new(
                                element_data,
                                nom::error::ErrorKind::Verify,
                            ))
                        },
                    )?;
                elements.push(element_value);
                remaining = new_remaining;
            } else {
                // Null element
                elements.push(Value::Null);
                remaining = new_remaining;
            }
        }
    } else {
        // Homogeneous collection with single type
        let (new_remaining, element_type_id) = super::parse_cql_type_id(remaining)?;
        remaining = new_remaining;

        for _ in 0..count {
            let (new_remaining, element_length) = parse_vint_length(remaining)?;

            if element_length > 0 {
                let (new_remaining, element_data) = take(element_length)(new_remaining)?;
                let element_value =
                    parse_cql_value_with_cell_metadata(element_data, element_type_id).map_err(
                        |_e| {
                            nom::Err::Error(nom::error::Error::new(
                                element_data,
                                nom::error::ErrorKind::Verify,
                            ))
                        },
                    )?;
                elements.push(element_value);
                remaining = new_remaining;
            } else {
                // Null element
                elements.push(Value::Null);
                remaining = new_remaining;
            }
        }
    }

    Ok((remaining, Value::List(elements)))
}

/// Parse CQL value with Cassandra 5.0 cell metadata handling
fn parse_cql_value_with_cell_metadata(input: &[u8], type_id: CqlTypeId) -> Result<Value> {
    if input.is_empty() {
        return Ok(Value::Null);
    }

    // Skip cell metadata if present (Cassandra 5.0 format)
    let mut offset = 0;
    if input.len() > 1 && (input[0] & 0x80) != 0 {
        offset += 1; // Skip cell flags

        // Skip timestamp if present (8 bytes)
        if offset + 8 <= input.len() {
            offset += 8;
        }

        // Skip TTL if present (4 bytes)
        if offset < input.len() && (input[0] & 0x40) != 0 && offset + 4 <= input.len() {
            offset += 4;
        }
    }

    let actual_data = &input[offset..];
    if actual_data.is_empty() {
        return Ok(Value::Null);
    }

    // Parse the actual value
    let (_, value) = parse_cql_value(actual_data, type_id)?;
    Ok(value)
}

/// Parse Set with Cassandra 5+ format
pub fn parse_set_v5_format(input: &[u8]) -> IResult<&[u8], Value> {
    // Sets use same binary format as lists in Cassandra 5+
    let (remaining, list_value) = parse_list_v5_format(input)?;

    if let Value::List(elements) = list_value {
        // Convert to Set - maintain insertion order for compatibility
        Ok((remaining, Value::Set(elements)))
    } else {
        Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )))
    }
}

/// Parse Map with Cassandra 5+ format (tuple-based serialization)
pub fn parse_map_v5_format(input: &[u8]) -> IResult<&[u8], Value> {
    // Cassandra 5+ format: [count:vint][key_type:u8][value_type:u8][pairs...]
    let (input, count) = parse_vint_length(input)?;

    // Validate count to prevent memory exhaustion
    if count > MAX_COLLECTION_SIZE {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::TooLarge,
        )));
    }

    if count == 0 {
        return Ok((input, Value::Map(Vec::new())));
    }

    let (input, key_type_id) = super::parse_cql_type_id(input)?;
    let (input, value_type_id) = super::parse_cql_type_id(input)?;

    let mut pairs = Vec::with_capacity(count);
    let mut remaining = input;

    for _ in 0..count {
        // Parse key with length prefix
        let (new_remaining, key_length) = parse_vint_length(remaining)?;
        let (new_remaining, key_data) = take(key_length)(new_remaining)?;
        let (_, key) = parse_cql_value(key_data, key_type_id)?;

        // Parse value with length prefix
        let (new_remaining, value_length) = parse_vint_length(new_remaining)?;
        let (new_remaining, value_data) = take(value_length)(new_remaining)?;
        let (_, value) = parse_cql_value(value_data, value_type_id)?;

        pairs.push((key, value));
        remaining = new_remaining;
    }

    Ok((remaining, Value::Map(pairs)))
}

/// Parse list with schema-driven element decoding
///
/// Uses provided schema to deterministically decode list elements, ensuring
/// no heuristic fallbacks per Issue #28. Validates full buffer consumption
/// for each element per Issue #61.
///
/// # Arguments
/// * `input` - Raw bytes to parse
/// * `element_schema` - Schema for list elements
///
/// # Returns
/// * Parsed list value with fully consumed input
pub fn parse_list_with_schema<'a>(
    input: &'a [u8],
    element_schema: &CqlType,
) -> IResult<&'a [u8], Value> {
    let (input, count) = parse_vint_length_signed(input)?;

    // Validate count to prevent memory exhaustion
    if count > MAX_COLLECTION_SIZE {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::TooLarge,
        )));
    }

    if count == 0 {
        return Ok((input, Value::List(Vec::new())));
    }

    let mut elements = Vec::with_capacity(count);
    let mut remaining = input;

    for i in 0..count {
        // Parse element length prefix (can be -1 for null)
        let (new_remaining, element_length) = parse_vint(remaining)?;
        remaining = new_remaining;

        let element = if element_length == -1 {
            Value::Null
        } else {
            let (new_remaining, element_data) = take_vint_length(element_length)(remaining)?;
            remaining = new_remaining;

            // Parse using schema-driven decoding (no heuristics)
            let (element_remaining, element_value) =
                parse_cql_value_with_schema(element_data, element_schema)?;

            // Validate element consumed its full buffer (Issue #61)
            validate_element_consumption(element_data, element_remaining, i, "List").map_err(
                |_e| {
                    nom::Err::Error(nom::error::Error::new(
                        element_data,
                        nom::error::ErrorKind::Verify,
                    ))
                },
            )?;

            element_value
        };

        elements.push(element);
    }

    Ok((remaining, Value::List(elements)))
}

/// Parse map with schema-driven key/value decoding
///
/// Uses provided schemas to deterministically decode map entries, ensuring
/// no heuristic fallbacks per Issue #28. Validates full buffer consumption
/// for each key and value per Issue #61.
pub fn parse_map_with_schema<'a>(
    input: &'a [u8],
    key_schema: &CqlType,
    value_schema: &CqlType,
) -> IResult<&'a [u8], Value> {
    let (input, count) = parse_vint_length_signed(input)?;

    // Validate count to prevent memory exhaustion
    if count > MAX_COLLECTION_SIZE {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::TooLarge,
        )));
    }

    if count == 0 {
        return Ok((input, Value::Map(Vec::new())));
    }

    let mut pairs = Vec::with_capacity(count);
    let mut remaining = input;

    for i in 0..count {
        // Parse key length
        let (new_remaining, key_length) = parse_vint(remaining)?;
        remaining = new_remaining;

        let key = if key_length == -1 {
            Value::Null
        } else {
            let (new_remaining, key_data) = take_vint_length(key_length)(remaining)?;
            remaining = new_remaining;

            let (key_remaining, key_value) = parse_cql_value_with_schema(key_data, key_schema)?;
            validate_element_consumption(key_data, key_remaining, i, "Map key").map_err(|_e| {
                nom::Err::Error(nom::error::Error::new(
                    key_data,
                    nom::error::ErrorKind::Verify,
                ))
            })?;

            key_value
        };

        // Parse value length
        let (new_remaining, value_length) = parse_vint(remaining)?;
        remaining = new_remaining;

        let value = if value_length == -1 {
            Value::Null
        } else {
            let (new_remaining, value_data) = take_vint_length(value_length)(remaining)?;
            remaining = new_remaining;

            let (value_remaining, value_value) =
                parse_cql_value_with_schema(value_data, value_schema)?;
            validate_element_consumption(value_data, value_remaining, i, "Map value").map_err(
                |_e| {
                    nom::Err::Error(nom::error::Error::new(
                        value_data,
                        nom::error::ErrorKind::Verify,
                    ))
                },
            )?;

            value_value
        };

        pairs.push((key, value));
    }

    Ok((remaining, Value::Map(pairs)))
}

#[cfg(test)]
mod tests {
    use super::super::CqlTypeId;
    use super::*;

    #[test]
    fn test_parse_list_v5_homogeneous() {
        let mut data = Vec::new();
        // Issue #1623: parse_list_v5_format reads count AND element lengths via
        // the unsigned parse_vint_length, so encode them unsigned.
        data.extend(super::super::super::vint::encode_vuint(2)); // two elements
        data.push(0x00); // homogeneous collection
        data.push(CqlTypeId::Int as u8);

        data.extend(super::super::super::vint::encode_vuint(4));
        data.extend_from_slice(&1i32.to_be_bytes());

        data.extend(super::super::super::vint::encode_vuint(4));
        data.extend_from_slice(&2i32.to_be_bytes());

        let (_, value) = parse_list_v5_format(&data).expect("parse list");
        assert_eq!(
            value,
            Value::List(vec![Value::Integer(1), Value::Integer(2)])
        );
    }

    #[test]
    fn test_parse_list_v5_mixed_types() {
        let mut data = Vec::new();
        // Issue #1623: parse_list_v5_format uses the unsigned parse_vint_length.
        data.extend(super::super::super::vint::encode_vuint(2)); // two elements
        data.push(0x01); // mixed-type flag

        // First element: text "alpha"
        data.push(CqlTypeId::Varchar as u8);
        data.extend(super::super::super::vint::encode_vuint(5));
        data.extend_from_slice(b"alpha");

        // Second element: integer 7
        data.push(CqlTypeId::Int as u8);
        data.extend(super::super::super::vint::encode_vuint(4));
        data.extend_from_slice(&7i32.to_be_bytes());

        let (_, value) = parse_list_v5_format(&data).expect("parse list");
        assert_eq!(
            value,
            Value::List(vec![Value::text("alpha".to_string()), Value::Integer(7)])
        );
    }

    #[test]
    fn test_parse_map_empty() {
        // Empty map: count = 0
        use super::super::super::vint::encode_vint;
        let data = encode_vint(0); // count = 0
        let (remaining, value) = parse_map(&data).unwrap();
        assert!(remaining.is_empty());
        assert_eq!(value, Value::Map(vec![]));
    }

    #[test]
    fn test_parse_map_with_entries() {
        // Map with 2 entries: int -> text
        use super::super::super::vint::encode_vint;
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(2)); // count = 2
        data.push(CqlTypeId::Int as u8); // key type
        data.push(CqlTypeId::Varchar as u8); // value type

        // Entry 1: key=1, value="one"
        data.extend_from_slice(&encode_vint(4)); // key length
        data.extend_from_slice(&1i32.to_be_bytes()); // key = 1
        data.extend_from_slice(&encode_vint(3)); // value length
        data.extend_from_slice(b"one"); // value = "one"

        // Entry 2: key=2, value="two"
        data.extend_from_slice(&encode_vint(4)); // key length
        data.extend_from_slice(&2i32.to_be_bytes()); // key = 2
        data.extend_from_slice(&encode_vint(3)); // value length
        data.extend_from_slice(b"two"); // value = "two"

        let (remaining, value) = parse_map(&data).unwrap();
        assert!(remaining.is_empty());
        assert_eq!(
            value,
            Value::Map(vec![
                (Value::Integer(1), Value::text("one".to_string())),
                (Value::Integer(2), Value::text("two".to_string())),
            ])
        );
    }

    #[test]
    fn test_parse_set_empty() {
        // Empty set: count = 0
        use super::super::super::vint::encode_vint;
        let data = encode_vint(0); // count = 0
        let (remaining, value) = parse_set(&data).unwrap();
        assert!(remaining.is_empty());
        assert_eq!(value, Value::Set(vec![]));
    }

    #[test]
    fn test_parse_set_with_entries() {
        // Set with 3 integer elements
        use super::super::super::vint::encode_vint;
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(3)); // count = 3
        data.push(CqlTypeId::Int as u8); // element type

        // Element 1: 10
        data.extend_from_slice(&encode_vint(4)); // length
        data.extend_from_slice(&10i32.to_be_bytes());

        // Element 2: 20
        data.extend_from_slice(&encode_vint(4)); // length
        data.extend_from_slice(&20i32.to_be_bytes());

        // Element 3: 30
        data.extend_from_slice(&encode_vint(4)); // length
        data.extend_from_slice(&30i32.to_be_bytes());

        let (remaining, value) = parse_set(&data).unwrap();
        assert!(remaining.is_empty());
        assert_eq!(
            value,
            Value::Set(vec![
                Value::Integer(10),
                Value::Integer(20),
                Value::Integer(30)
            ])
        );
    }

    #[test]
    fn test_parse_map_with_null_value() {
        // Map with null value (length = -1)
        use super::super::super::vint::encode_vint;
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(1)); // count = 1
        data.push(CqlTypeId::Int as u8); // key type
        data.push(CqlTypeId::Varchar as u8); // value type

        // Entry: key=1, value=null
        data.extend_from_slice(&encode_vint(4)); // key length
        data.extend_from_slice(&1i32.to_be_bytes()); // key = 1
        data.extend_from_slice(&encode_vint(-1)); // value = null

        let (remaining, value) = parse_map(&data).unwrap();
        assert!(remaining.is_empty());
        assert_eq!(value, Value::Map(vec![(Value::Integer(1), Value::Null)]));
    }

    #[test]
    fn test_parse_list_with_null_element() {
        // List with null element (length = -1)
        use super::super::super::vint::encode_vint;
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(2)); // count = 2
        data.push(CqlTypeId::Int as u8); // element type

        // Element 1: 42
        data.extend_from_slice(&encode_vint(4)); // length
        data.extend_from_slice(&42i32.to_be_bytes());

        // Element 2: null
        data.extend_from_slice(&encode_vint(-1)); // null marker

        let (remaining, value) = parse_list(&data).unwrap();
        assert!(remaining.is_empty());
        assert_eq!(value, Value::List(vec![Value::Integer(42), Value::Null]));
    }

    // Issue #264: Collection max elements limit test
    #[test]
    fn test_collection_max_elements_limit() {
        use super::super::super::vint::encode_vint;

        // Build list data claiming 1,000,001 elements (exceeds MAX_COLLECTION_SIZE)
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(1_000_001i64)); // Count > 1,000,000 limit
        data.push(CqlTypeId::Int as u8); // Element type

        let result = parse_list(&data);
        assert!(
            result.is_err(),
            "Should reject list with > 1,000,000 elements"
        );

        // nom error - check it's TooLarge
        if let Err(nom::Err::Error(e)) = result {
            assert_eq!(
                e.code,
                nom::error::ErrorKind::TooLarge,
                "Error should be TooLarge, got: {:?}",
                e.code
            );
        } else {
            panic!("Expected nom::Err::Error, got something else");
        }
    }

    // Issue #264: Map max elements limit test
    #[test]
    fn test_map_max_elements_limit() {
        use super::super::super::vint::encode_vint;

        // Build map data claiming 1,000,001 entries
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(1_000_001i64)); // Count > 1,000,000 limit
        data.push(CqlTypeId::Int as u8); // Key type
        data.push(CqlTypeId::Varchar as u8); // Value type

        let result = parse_map(&data);
        assert!(
            result.is_err(),
            "Should reject map with > 1,000,000 entries"
        );

        // nom error - check it's TooLarge (consistent with list test)
        if let Err(nom::Err::Error(e)) = result {
            assert_eq!(
                e.code,
                nom::error::ErrorKind::TooLarge,
                "Error should be TooLarge, got: {:?}",
                e.code
            );
        } else {
            panic!("Expected nom::Err::Error, got something else");
        }
    }
}
