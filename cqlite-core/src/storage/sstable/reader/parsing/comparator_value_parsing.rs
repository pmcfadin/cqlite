//! Standalone comparator-based value parsing for V5 format
//!
//! This module provides schema-aware value parsing that can be used independently
//! of the SSTableReader, making it suitable for use in the row_cell_state_machine.
//!
//! Unlike the `parse_cql_value_raw` function which only uses CqlTypeId,
//! this module uses full ComparatorType information to properly parse nested
//! structures like UDTs inside collections.
//!
//! **Element length encoding**: Uses VInt-encoded lengths for non-frozen
//! collection elements. This differs from frozen collections in
//! `v5_compressed_legacy.rs` which use i32 BE lengths per Cassandra's
//! `AbstractType.writeValue()`.

use crate::{
    parser::vint::{parse_vint, parse_vint_length},
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
        ComparatorType::Duration => parse_duration_value(value_data),
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
        ComparatorType::Custom(name) => {
            // Schema-derived custom scalars (`time`/`inet`/`json`) decode to their
            // typed `Value` via the shared decoder; genuinely-unknown custom types
            // fall back to `Value::Blob` inside `decode_custom_scalar`. Dispatch is
            // on the schema-derived name string (no-heuristics mandate, issue #28).
            // This is reached for schema-derived custom scalars used as collection
            // elements (e.g. `list<time>`, `set<inet>`, `map<text,json>`) on the
            // block-path `RowCellStateMachine`, which uses this standalone parser.
            super::custom_scalar::decode_custom_scalar(name, value_data)
        }
    }
}

/// Parse a CQL `DURATION` value body.
///
/// Cassandra's `DurationType` stores three consecutive signed (ZigZag) VInts:
/// `months` (i32), `days` (i32), and `nanoseconds` (i64). See
/// `org.apache.cassandra.cql3.Duration` / `DurationSerializer` and the
/// definitive guide Appendix B (VInt cheat sheet).
///
/// `value_data` is the cell value body only; any outer length prefix has
/// already been stripped by the caller (mirroring the `Decimal`/`Blob` arms).
fn parse_duration_value(value_data: &[u8]) -> Result<Value> {
    // months (signed VInt -> i32)
    let (remaining, months) = parse_vint(value_data)
        .map_err(|_| Error::corruption("Failed to parse duration months VInt"))?;

    // days (signed VInt -> i32)
    let (remaining, days) = parse_vint(remaining)
        .map_err(|_| Error::corruption("Failed to parse duration days VInt"))?;

    // nanoseconds (signed VInt -> i64)
    let (remaining, nanos) = parse_vint(remaining)
        .map_err(|_| Error::corruption("Failed to parse duration nanos VInt"))?;

    if !remaining.is_empty() {
        return Err(Error::corruption(
            "Duration value has trailing bytes after months/days/nanos",
        ));
    }

    // months and days are i32 in Cassandra's DurationType; reject (rather than
    // silently wrap) any encoded value outside the i32 range.
    let months =
        i32::try_from(months).map_err(|_| Error::corruption("Duration months out of i32 range"))?;
    let days =
        i32::try_from(days).map_err(|_| Error::corruption("Duration days out of i32 range"))?;

    Ok(Value::Duration {
        months,
        days,
        nanos,
    })
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

    /// Local signed-VInt encoder for tests — avoids depending on
    /// `crate::storage::serialization::vint`, which is gated behind the
    /// `write-support` feature (so the lib tests compile under minimal
    /// feature sets, e.g. `--no-default-features --features=all-compression`).
    /// Byte-identical to Cassandra's `VIntCoding` (zigzag + unsigned VInt),
    /// matching the writer's `encode_signed`.
    fn encode_signed(value: i64, buf: &mut Vec<u8>) {
        let zigzag = ((value << 1) ^ (value >> 63)) as u64;
        // Cassandra unsigned-VInt size: (639 - leading_zeros(v|1) * 9) >> 6
        let magnitude = (zigzag | 1).leading_zeros();
        let size = ((639 - magnitude * 9) >> 6) as usize;
        if size == 1 {
            buf.push(zigzag as u8);
        } else if size == 9 {
            buf.push(0xFF);
            buf.extend_from_slice(&zigzag.to_be_bytes());
        } else {
            let extra_bytes = size - 1;
            let mask: u8 = (0xFFu16 << (8 - extra_bytes)) as u8;
            let first_byte_data_bits = 8 - extra_bytes - 1;
            let data_shift = extra_bytes * 8;
            let first_byte_data =
                ((zigzag >> data_shift) & ((1u64 << first_byte_data_bits) - 1)) as u8;
            buf.push(mask | first_byte_data);
            for i in (0..extra_bytes).rev() {
                buf.push(((zigzag >> (i * 8)) & 0xFF) as u8);
            }
        }
    }

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

    /// Build a Duration value-body (three signed/zigzag VInts: months, days, nanos)
    /// using the writer's `encode_signed`, which is the matched encoder for
    /// `parse_vint`. This mirrors what `data_writer::serialize_value` emits for
    /// `Value::Duration`.
    fn build_duration_bytes(months: i32, days: i32, nanos: i64) -> Vec<u8> {
        let mut buf = Vec::new();
        encode_signed(months as i64, &mut buf);
        encode_signed(days as i64, &mut buf);
        encode_signed(nanos, &mut buf);
        buf
    }

    #[test]
    fn test_parse_duration_zero() {
        // All-zero duration: zigzag(0) == 0 -> three 0x00 bytes.
        let data = vec![0x00, 0x00, 0x00];
        let comparator = ComparatorType::Duration;
        let result = parse_value_with_comparator(&data, &comparator).unwrap();
        assert_eq!(
            result,
            Value::Duration {
                months: 0,
                days: 0,
                nanos: 0,
            }
        );
    }

    #[test]
    fn test_parse_duration_positive() {
        // 1 month, 2 days, 3 nanos.
        // zigzag(1)=2(0x02), zigzag(2)=4(0x04), zigzag(3)=6(0x06).
        let data = vec![0x02, 0x04, 0x06];
        assert_eq!(data, build_duration_bytes(1, 2, 3));
        let comparator = ComparatorType::Duration;
        let result = parse_value_with_comparator(&data, &comparator).unwrap();
        assert_eq!(
            result,
            Value::Duration {
                months: 1,
                days: 2,
                nanos: 3,
            }
        );
    }

    #[test]
    fn test_parse_duration_negative() {
        // -1 month, -1 day, -1 nano. zigzag(-1)=1(0x01) for each.
        let data = vec![0x01, 0x01, 0x01];
        assert_eq!(data, build_duration_bytes(-1, -1, -1));
        let comparator = ComparatorType::Duration;
        let result = parse_value_with_comparator(&data, &comparator).unwrap();
        assert_eq!(
            result,
            Value::Duration {
                months: -1,
                days: -1,
                nanos: -1,
            }
        );
    }

    #[test]
    fn test_parse_duration_mixed_sign() {
        // Negative months, positive days, large negative nanos.
        let months = -13_i32;
        let days = 200_i32;
        let nanos = -86_400_000_000_000_i64; // -1 day in nanos, spans multiple VInt bytes
        let data = build_duration_bytes(months, days, nanos);
        let comparator = ComparatorType::Duration;
        let result = parse_value_with_comparator(&data, &comparator).unwrap();
        assert_eq!(
            result,
            Value::Duration {
                months,
                days,
                nanos,
            }
        );
    }

    #[test]
    fn test_parse_duration_i32_extremes() {
        // The i32 boundary values must decode losslessly (they fit in i32).
        let data = build_duration_bytes(i32::MIN, i32::MAX, i64::MAX);
        let comparator = ComparatorType::Duration;
        let result = parse_value_with_comparator(&data, &comparator).unwrap();
        assert_eq!(
            result,
            Value::Duration {
                months: i32::MIN,
                days: i32::MAX,
                nanos: i64::MAX,
            }
        );
    }

    #[test]
    fn test_parse_duration_months_out_of_i32_range_errors() {
        // months/days are i32 in Cassandra; an encoded value outside the i32
        // range must be rejected as corruption, not silently wrapped.
        let comparator = ComparatorType::Duration;

        let mut over = Vec::new();
        encode_signed(i32::MAX as i64 + 1, &mut over); // months overflow
        encode_signed(0, &mut over);
        encode_signed(0, &mut over);
        assert!(parse_value_with_comparator(&over, &comparator).is_err());

        let mut under = Vec::new();
        encode_signed(0, &mut under);
        encode_signed(i32::MIN as i64 - 1, &mut under); // days underflow
        encode_signed(0, &mut under);
        assert!(parse_value_with_comparator(&under, &comparator).is_err());
    }

    #[test]
    fn test_parse_duration_truncated_errors() {
        // Only two VInts present where three are required -> corruption error.
        let mut data = Vec::new();
        encode_signed(1, &mut data);
        encode_signed(2, &mut data);
        let comparator = ComparatorType::Duration;
        assert!(parse_value_with_comparator(&data, &comparator).is_err());
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

    /// Encode a non-frozen collection element (VInt length prefix + body) using
    /// the same length encoding `parse_list_value`/`parse_map_value` expect.
    fn push_element(buf: &mut Vec<u8>, body: &[u8]) {
        encode_signed(body.len() as i64, buf);
        buf.extend_from_slice(body);
    }

    // Issue #1627 / roborev job 2738: the standalone `parse_value_with_comparator`
    // is the parser the block-path `RowCellStateMachine` uses. Its
    // `Custom(name)` arm must route schema-derived custom scalars through
    // `decode_custom_scalar` so collection elements decode to their typed
    // `Value` instead of `Value::Blob`. These tests drive the standalone parser
    // with collection-of-custom-scalar comparators and would fail under the old
    // `Custom(_) => Value::Blob` behavior.

    #[test]
    fn test_parse_list_of_time_custom_scalars() {
        let t0: i64 = 0;
        let t1: i64 = 4_500_000_000_000;
        let mut data = Vec::new();
        encode_signed(2, &mut data); // element count
        push_element(&mut data, &t0.to_be_bytes());
        push_element(&mut data, &t1.to_be_bytes());

        let comparator =
            ComparatorType::List(Box::new(ComparatorType::Custom("time".to_string())));
        let result = parse_value_with_comparator(&data, &comparator).unwrap();

        assert_eq!(result, Value::List(vec![Value::Time(t0), Value::Time(t1)]));
    }

    #[test]
    fn test_parse_list_of_inet_custom_scalars() {
        let v4 = vec![10u8, 0, 0, 1];
        let v6 = vec![0u8; 16];
        let mut data = Vec::new();
        encode_signed(2, &mut data); // element count
        push_element(&mut data, &v4);
        push_element(&mut data, &v6);

        let comparator =
            ComparatorType::List(Box::new(ComparatorType::Custom("inet".to_string())));
        let result = parse_value_with_comparator(&data, &comparator).unwrap();

        assert_eq!(
            result,
            Value::List(vec![Value::Inet(v4), Value::Inet(v6)])
        );
    }

    #[test]
    fn test_parse_map_text_to_json_custom_scalars() {
        // map<text, json>: keys are text, values are schema-derived Custom("json").
        let key = b"k";
        let json_body = br#"[1,2,3]"#;
        let mut data = Vec::new();
        encode_signed(1, &mut data); // entry count
        push_element(&mut data, key);
        push_element(&mut data, json_body);

        let comparator = ComparatorType::Map(
            Box::new(ComparatorType::Text),
            Box::new(ComparatorType::Custom("json".to_string())),
        );
        let result = parse_value_with_comparator(&data, &comparator).unwrap();

        assert_eq!(
            result,
            Value::Map(vec![(
                Value::Text("k".to_string()),
                Value::Json(serde_json::json!([1, 2, 3])),
            )])
        );
    }

    #[test]
    fn test_parse_set_of_inet_custom_scalars() {
        let v4a = vec![192u8, 168, 0, 1];
        let v4b = vec![10u8, 0, 0, 2];
        let mut data = Vec::new();
        encode_signed(2, &mut data); // element count
        push_element(&mut data, &v4a);
        push_element(&mut data, &v4b);

        let comparator =
            ComparatorType::Set(Box::new(ComparatorType::Custom("inet".to_string())));
        let result = parse_value_with_comparator(&data, &comparator).unwrap();

        assert_eq!(
            result,
            Value::Set(vec![Value::Inet(v4a), Value::Inet(v4b)])
        );
    }

    #[test]
    fn test_parse_list_of_unknown_custom_stays_blob() {
        // A genuinely-unknown custom marshaller name must still fall back to Blob
        // (the only legitimate blob fallback), even as a collection element.
        let body = vec![1u8, 2, 3];
        let mut data = Vec::new();
        encode_signed(1, &mut data);
        push_element(&mut data, &body);

        let comparator = ComparatorType::List(Box::new(ComparatorType::Custom(
            "some_udt_marshaller".to_string(),
        )));
        let result = parse_value_with_comparator(&data, &comparator).unwrap();

        assert_eq!(result, Value::List(vec![Value::Blob(body)]));
    }
}
