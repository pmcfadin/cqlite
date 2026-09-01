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
//! `row_decoder.rs` which use i32 BE lengths per Cassandra's
//! `AbstractType.writeValue()`.

use crate::{
    parser::vint::parse_vint,
    types::{ComparatorType, UdtValue, Value},
    Error, Result,
};

/// Maximum nesting depth for recursive value decoding, mirroring the V5 frozen
/// guard (`MAX_TYPE_NESTING_DEPTH` in `row_decoder::mod`). A corrupt or
/// adversarial deeply-nested type (e.g. `frozen<frozen<frozen<...>>>`) must
/// return `Err` rather than recurse until the stack overflows and aborts the
/// process (issue #1632). Shared by both exact-slice decoders — the standalone
/// block-path decoder here and `SSTableReader::parse_value_with_comparator`.
pub(crate) const MAX_VALUE_NESTING_DEPTH: usize = 10;

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
    parse_value_with_comparator_at_depth(value_data, comparator, 0)
}

/// Depth-tracking core of [`parse_value_with_comparator`]. Every recursive
/// descent into a nested element/field/frozen inner type increments `depth`;
/// exceeding [`MAX_VALUE_NESTING_DEPTH`] returns `Err` instead of recursing to a
/// stack overflow (issue #1632). Guard-only: successful decoding of any value
/// within the depth budget is byte-identical to before.
fn parse_value_with_comparator_at_depth(
    value_data: &[u8],
    comparator: &ComparatorType,
    depth: usize,
) -> Result<Value> {
    if depth > MAX_VALUE_NESTING_DEPTH {
        return Err(Error::corruption(format!(
            "Value decode recursion depth {} exceeds maximum {}",
            depth, MAX_VALUE_NESTING_DEPTH
        )));
    }
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
            std::str::from_utf8(value_data)
                .map_err(|_| Error::corruption("Invalid UTF-8 in text value"))?;
            Ok(Value::Text(
                crate::storage::sstable::reader::value_borrow::borrow_active(value_data),
            ))
        }
        ComparatorType::Blob => Ok(Value::Blob(
            crate::storage::sstable::reader::value_borrow::borrow_active(value_data),
        )),
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
        ComparatorType::Varint => Ok(Value::Varint(
            crate::storage::sstable::reader::value_borrow::borrow_active(value_data),
        )),
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
            Ok(Value::Json(Box::new(json_value)))
        }
        // Structural types route through the ONE shared structural body in
        // `value_parsing` (issue #1636 / J2). Non-frozen collections use VInt
        // element framing; tuple/UDT use Cassandra's 4-byte i32-BE field framing
        // (`TupleType`/`UserType` `putInt`, `-1` == null) — the same framing the
        // live block path (decoder #1) and the v5 frozen path already use.
        ComparatorType::List(element_comparator) => {
            super::value_parsing::parse_list_value_with(value_data, element_comparator, |d, c| {
                parse_value_with_comparator_at_depth(d, c, depth + 1)
            })
        }
        ComparatorType::Set(element_comparator) => {
            super::value_parsing::parse_set_value_with(value_data, element_comparator, |d, c| {
                parse_value_with_comparator_at_depth(d, c, depth + 1)
            })
        }
        ComparatorType::Map(key_comparator, value_comparator) => {
            super::value_parsing::parse_map_value_with(
                value_data,
                key_comparator,
                value_comparator,
                |d, c| parse_value_with_comparator_at_depth(d, c, depth + 1),
            )
        }
        ComparatorType::Tuple(field_comparators) => {
            super::value_parsing::parse_tuple_value_with(value_data, field_comparators, |d, c| {
                parse_value_with_comparator_at_depth(d, c, depth + 1)
            })
        }
        ComparatorType::Udt {
            type_name,
            keyspace,
            field_comparators,
        } => {
            let udt = super::value_parsing::parse_udt_value_with(
                value_data,
                field_comparators,
                |d, c| parse_value_with_comparator_at_depth(d, c, depth + 1),
            )?;
            // Preserve decoder #2's provenance (type_name/keyspace from the
            // comparator) rather than the helper's "unknown" placeholders.
            Ok(Value::Udt(Box::new(UdtValue {
                keyspace: keyspace.clone().unwrap_or_else(|| "unknown".to_string()),
                type_name: type_name.to_string(),
                fields: udt.fields,
            })))
        }
        ComparatorType::Frozen(inner_comparator) => {
            // Issue #2339: a frozen COLLECTION body uses i32-BE element framing, not
            // the VInt framing a NON-frozen collection cell uses, so the three
            // collection kinds dispatch to `frozen_value_parsing`; every other inner
            // type (tuple/UDT/scalar/nested frozen) decodes through this same body.
            let inner_value = super::frozen_value_parsing::parse_frozen_inner_with(
                value_data,
                inner_comparator,
                depth + 1,
                MAX_VALUE_NESTING_DEPTH,
                &|d, c, dep| parse_value_with_comparator_at_depth(d, c, dep),
            )?;
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

    /// Local unsigned-VInt encoder for tests (Cassandra `writeUnsignedVInt`, no
    /// zigzag). Cassandra writes collection element/entry counts and per-element
    /// length prefixes with the unsigned encoding, so fixtures for the
    /// unsigned-aware parser (issue #1623) must use this, not `encode_signed`.
    fn encode_unsigned(value: u64, buf: &mut Vec<u8>) {
        let magnitude = (value | 1).leading_zeros();
        let size = ((639 - magnitude * 9) >> 6) as usize;
        if size == 1 {
            buf.push(value as u8);
        } else if size == 9 {
            buf.push(0xFF);
            buf.extend_from_slice(&value.to_be_bytes());
        } else {
            let extra_bytes = size - 1;
            let mask: u8 = (0xFFu16 << (8 - extra_bytes)) as u8;
            let first_byte_data_bits = 8 - extra_bytes - 1;
            let data_shift = extra_bytes * 8;
            let first_byte_data =
                ((value >> data_shift) & ((1u64 << first_byte_data_bits) - 1)) as u8;
            buf.push(mask | first_byte_data);
            for i in (0..extra_bytes).rev() {
                buf.push(((value >> (i * 8)) & 0xFF) as u8);
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
        assert_eq!(result, Value::text("hello".to_string()));
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
        // Format: element_count (unsigned VInt) + (element_length (unsigned VInt)
        // + element_bytes)*
        // Issue #1623: Cassandra writes counts/lengths with writeUnsignedVInt, so
        // small values are their own byte (count 2 = 0x02, length 4 = 0x04).
        let mut data = vec![
            0x02, // count = 2 (unsigned VInt)
        ];
        // Element 1: length=4, value=0x00000001 (1 as big-endian i32)
        data.push(0x04); // length = 4 (unsigned VInt)
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]); // value
                                                           // Element 2: length=4, value=0x00000002 (2 as big-endian i32)
        data.push(0x04); // length = 4 (unsigned VInt)
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
        encode_unsigned(body.len() as u64, buf);
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
        encode_unsigned(2, &mut data); // element count
        push_element(&mut data, &t0.to_be_bytes());
        push_element(&mut data, &t1.to_be_bytes());

        let comparator = ComparatorType::List(Box::new(ComparatorType::Custom("time".to_string())));
        let result = parse_value_with_comparator(&data, &comparator).unwrap();

        assert_eq!(result, Value::List(vec![Value::Time(t0), Value::Time(t1)]));
    }

    #[test]
    fn test_parse_list_of_inet_custom_scalars() {
        let v4 = vec![10u8, 0, 0, 1];
        let v6 = vec![0u8; 16];
        let mut data = Vec::new();
        encode_unsigned(2, &mut data); // element count
        push_element(&mut data, &v4);
        push_element(&mut data, &v6);

        let comparator = ComparatorType::List(Box::new(ComparatorType::Custom("inet".to_string())));
        let result = parse_value_with_comparator(&data, &comparator).unwrap();

        assert_eq!(
            result,
            Value::List(vec![Value::Inet(v4.into()), Value::Inet(v6.into())])
        );
    }

    #[test]
    fn test_parse_map_text_to_json_custom_scalars() {
        // map<text, json>: keys are text, values are schema-derived Custom("json").
        let key = b"k";
        let json_body = br#"[1,2,3]"#;
        let mut data = Vec::new();
        encode_unsigned(1, &mut data); // entry count
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
                Value::text("k".to_string()),
                Value::Json(Box::new(serde_json::json!([1, 2, 3]))),
            )])
        );
    }

    #[test]
    fn test_parse_set_of_inet_custom_scalars() {
        let v4a = vec![192u8, 168, 0, 1];
        let v4b = vec![10u8, 0, 0, 2];
        let mut data = Vec::new();
        encode_unsigned(2, &mut data); // element count
        push_element(&mut data, &v4a);
        push_element(&mut data, &v4b);

        let comparator = ComparatorType::Set(Box::new(ComparatorType::Custom("inet".to_string())));
        let result = parse_value_with_comparator(&data, &comparator).unwrap();

        assert_eq!(
            result,
            Value::Set(vec![Value::Inet(v4a.into()), Value::Inet(v4b.into())])
        );
    }

    #[test]
    fn test_parse_list_of_unknown_custom_stays_blob() {
        // A genuinely-unknown custom marshaller name must still fall back to Blob
        // (the only legitimate blob fallback), even as a collection element.
        let body = vec![1u8, 2, 3];
        let mut data = Vec::new();
        encode_unsigned(1, &mut data); // element count
        push_element(&mut data, &body);

        let comparator = ComparatorType::List(Box::new(ComparatorType::Custom(
            "some_udt_marshaller".to_string(),
        )));
        let result = parse_value_with_comparator(&data, &comparator).unwrap();

        assert_eq!(result, Value::List(vec![Value::Blob(body.into())]));
    }

    // Issue #1632 (hardening a): a corrupt or adversarial deeply-nested type must
    // return `Err` via the recursion-depth guard rather than recursing until the
    // stack overflows and aborts the process. A `frozen<...>` chain recurses on
    // the SAME bytes at every level (no byte consumed), so without the guard it
    // would recurse unbounded.
    #[test]
    fn test_deeply_nested_frozen_type_errors_not_overflow() {
        // 12 levels of frozen wrapping an int — exceeds MAX_VALUE_NESTING_DEPTH (10).
        let mut comparator = ComparatorType::Int;
        for _ in 0..12 {
            comparator = ComparatorType::Frozen(Box::new(comparator));
        }
        let data = vec![0x00, 0x00, 0x00, 0x2A];
        let result = parse_value_with_comparator(&data, &comparator);
        assert!(
            result.is_err(),
            "12-level nested frozen type must Err, not stack-overflow/abort"
        );
    }

    /// A modestly-nested type (within the limit) must still decode correctly, so
    /// the guard does not regress successful-decode semantics (issue #1632).
    #[test]
    fn test_shallow_nested_frozen_still_decodes() {
        let comparator = ComparatorType::Frozen(Box::new(ComparatorType::Frozen(Box::new(
            ComparatorType::Int,
        ))));
        let data = vec![0x00, 0x00, 0x00, 0x2A]; // 42
        let result = parse_value_with_comparator(&data, &comparator).unwrap();
        assert_eq!(
            result,
            Value::Frozen(Box::new(Value::Frozen(Box::new(Value::Integer(42)))))
        );
    }

    // Issue #1632 (hardening c): a corrupt, huge declared element count must not
    // pre-allocate gigabytes. Capacity is clamped to REASONABLE_COLLECTION_CAPACITY
    // and the short buffer makes the first element length exceed the data, so the
    // decode returns `Err` promptly with bounded peak allocation (no OOM/panic).
    #[test]
    fn test_huge_declared_count_short_buffer_errors_bounded_alloc() {
        let mut data = Vec::new();
        encode_unsigned(1u64 << 30, &mut data); // declared count ~1 billion
        encode_unsigned(1000, &mut data); // first element claims 1000 bytes...
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]); // ...but only 4 remain

        let comparator = ComparatorType::List(Box::new(ComparatorType::Int));
        let result = parse_value_with_comparator(&data, &comparator);
        assert!(
            result.is_err(),
            "huge count + short buffer must Err without a huge pre-allocation"
        );
    }
}
