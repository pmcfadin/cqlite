//! Primitive CQL type parsers.
//!
//! Decoders for the scalar CQL types (boolean, integers, floats, text, blob,
//! uuid, timestamp/date/time, varint, decimal, duration, inet) as they appear
//! in the Cassandra SSTable cell format.

// Issue #1623: text/blob/varint/inet length prefixes here are read back from
// CQLite's own `serialize_cql_value` output, whose lengths are written with the
// ZigZag encoder `encode_vint`. This is a self-consistent CQLite-internal pair,
// so these length reads use `parse_vint_length_signed` (ZigZag), NOT the
// unsigned `parse_vint_length` used for raw Cassandra structural fields. Real
// Cassandra cell values are length-delimited by the cell header (decoded with
// the unsigned reader in `storage/sstable/reader/parsing`) before reaching here.
use super::super::vint::{parse_vint, parse_vint_length_signed};
use crate::types::Value;
use nom::{
    bytes::complete::take,
    combinator::map,
    number::complete::{be_f32, be_f64, be_i32, be_i64, be_u16, be_u32, be_u8},
    IResult,
};

/// Parse a boolean value (1 byte: 0x00 = false, 0x01 = true)
pub fn parse_boolean(input: &[u8]) -> IResult<&[u8], Value> {
    map(be_u8, |b| Value::Boolean(b != 0))(input)
}

/// Parse a tinyint (signed 8-bit integer)
pub fn parse_tinyint(input: &[u8]) -> IResult<&[u8], Value> {
    map(be_u8, |b| Value::Integer(b as i8 as i32))(input)
}

/// Parse a smallint (signed 16-bit integer)
pub fn parse_smallint(input: &[u8]) -> IResult<&[u8], Value> {
    map(be_u16, |b| Value::Integer(b as i16 as i32))(input)
}

/// Parse an int (signed 32-bit integer)
pub fn parse_int(input: &[u8]) -> IResult<&[u8], Value> {
    map(be_i32, Value::Integer)(input)
}

/// Parse a bigint (signed 64-bit integer)
pub fn parse_bigint(input: &[u8]) -> IResult<&[u8], Value> {
    map(be_i64, Value::BigInt)(input)
}

/// Parse a counter (signed 64-bit integer with counter semantics)
pub fn parse_counter(input: &[u8]) -> IResult<&[u8], Value> {
    map(be_i64, Value::Counter)(input)
}

/// Parse a float (32-bit floating point)
pub fn parse_float(input: &[u8]) -> IResult<&[u8], Value> {
    map(be_f32, Value::Float32)(input)
}

/// Parse a double (64-bit floating point)
pub fn parse_double(input: &[u8]) -> IResult<&[u8], Value> {
    map(be_f64, Value::Float)(input)
}

/// Parse text (length-prefixed UTF-8 string)
pub fn parse_text(input: &[u8]) -> IResult<&[u8], Value> {
    let (input, length) = parse_vint_length_signed(input)?;
    let (input, bytes) = take(length)(input)?;
    let text = String::from_utf8(bytes.to_vec()).map_err(|_| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
    })?;
    Ok((input, Value::Text(text)))
}

/// Parse blob (length-prefixed binary data)
pub fn parse_blob(input: &[u8]) -> IResult<&[u8], Value> {
    let (input, length) = parse_vint_length_signed(input)?;
    let (input, bytes) = take(length)(input)?;
    Ok((input, Value::Blob(bytes.to_vec())))
}

/// Parse UUID (16 bytes)
pub fn parse_uuid(input: &[u8]) -> IResult<&[u8], Value> {
    let (input, bytes) = take(16usize)(input)?;
    let mut uuid = [0u8; 16];
    uuid.copy_from_slice(bytes);
    Ok((input, Value::Uuid(uuid)))
}

/// Parse timestamp from binary format (milliseconds since Unix epoch)
pub fn parse_timestamp(input: &[u8]) -> IResult<&[u8], Value> {
    map(be_i64, Value::Timestamp)(input)
}

/// Parse date (32-bit days since epoch with Integer.MIN_VALUE offset)
///
/// Cassandra encodes DATE as an unsigned 32-bit integer shifted by Integer.MIN_VALUE
/// (2^31 = 2,147,483,648) for byte-order comparability. To decode, we add i32::MIN back.
pub fn parse_date(input: &[u8]) -> IResult<&[u8], Value> {
    map(be_u32, |stored| {
        // Cassandra DATE: decode by adding i32::MIN back
        let days_since_epoch = stored.wrapping_add(i32::MIN as u32) as i32;
        Value::Date(days_since_epoch)
    })(input)
}

/// Parse time (64-bit nanoseconds since midnight)
pub fn parse_time(input: &[u8]) -> IResult<&[u8], Value> {
    map(be_i64, Value::Time)(input)
}

/// Parse varint (variable-length integer)
pub fn parse_varint(input: &[u8]) -> IResult<&[u8], Value> {
    let (input, length) = parse_vint_length_signed(input)?;
    let (input, bytes) = take(length)(input)?;
    Ok((input, Value::Varint(bytes.to_vec())))
}

/// Parse decimal (scale + unscaled BigInteger bytes)
///
/// Cassandra SSTable format (DecimalType.java:275-278):
///   - 4 bytes: scale as big-endian signed int32
///   - remaining bytes: unscaled value as raw BigInteger two's-complement big-endian bytes
///
/// The unscaled bytes are NOT VInt-encoded; they are the raw Java BigInteger
/// byte array written by `ByteBuffer.putInt(scale)` then `unscaledValue.toByteArray()`.
pub fn parse_decimal(input: &[u8]) -> IResult<&[u8], Value> {
    let (input, scale) = be_i32(input)?;
    // All remaining bytes in this cell's data are the BigInteger unscaled bytes.
    // In the SSTable cell framing, each cell value is already length-delimited by
    // the surrounding cell header, so we consume all of `input` here.
    let unscaled = input.to_vec();
    Ok((
        &input[input.len()..], // empty remaining slice
        Value::Decimal { scale, unscaled },
    ))
}

/// Parse duration (months, days, nanoseconds)
///
/// Cassandra SSTable format (DurationType.java / DurationSerializer):
///   - months:  signed VInt (zigzag-encoded)
///   - days:    signed VInt (zigzag-encoded)
///   - nanos:   signed VInt (zigzag-encoded)
///
/// All three components are returned as-is in `Value::Duration` to preserve
/// calendar semantics (months ≠ 30 days; days ≠ 24 hours in all time zones).
pub fn parse_duration(input: &[u8]) -> IResult<&[u8], Value> {
    let (rest, months_raw) = parse_vint(input)?;
    let (rest, days_raw) = parse_vint(rest)?;
    let (rest, nanos) = parse_vint(rest)?;

    // months/days are i32 in Cassandra's DurationType. Reject (rather than
    // silently truncate via `as i32`) any encoded value outside the i32 range so
    // a corrupt encoding errors instead of wrapping around (issue #1632). Mirrors
    // the reader-side guard in `comparator_value_parsing::parse_duration_value`
    // (issue #765).
    let months = i32::try_from(months_raw).map_err(|_| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
    })?;
    let days = i32::try_from(days_raw).map_err(|_| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
    })?;

    Ok((
        rest,
        Value::Duration {
            months,
            days,
            nanos,
        },
    ))
}

/// Parse inet address (4 or 16 bytes)
pub fn parse_inet(input: &[u8]) -> IResult<&[u8], Value> {
    let (input, length) = parse_vint_length_signed(input)?;
    let (input, bytes) = take(length)(input)?;
    Ok((input, Value::Inet(bytes.to_vec())))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_boolean_parsing() {
        let data = vec![0x01];
        let (_, value) = parse_boolean(&data).unwrap();
        assert_eq!(value, Value::Boolean(true));

        let data = vec![0x00];
        let (_, value) = parse_boolean(&data).unwrap();
        assert_eq!(value, Value::Boolean(false));
    }

    #[test]
    fn test_int_parsing() {
        let data = vec![0x00, 0x00, 0x01, 0x00]; // 256 in big-endian
        let (_, value) = parse_int(&data).unwrap();
        assert_eq!(value, Value::Integer(256));
    }

    #[test]
    fn test_text_parsing() {
        use super::super::super::vint::encode_vint;

        let test_str = "hello";
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(test_str.len() as i64));
        data.extend_from_slice(test_str.as_bytes());

        let (_, value) = parse_text(&data).unwrap();
        assert_eq!(value, Value::Text("hello".to_string()));
    }

    #[test]
    fn test_uuid_parsing() {
        let uuid_bytes = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let (_, value) = parse_uuid(&uuid_bytes).unwrap();
        assert_eq!(value, Value::Uuid(uuid_bytes));
    }

    #[test]
    fn test_parse_timestamp() {
        // Timestamp is stored as milliseconds since epoch (i64 big-endian)
        let ts_ms: i64 = 1702900000000; // 2023-12-18 in milliseconds
        let data = ts_ms.to_be_bytes();
        let (remaining, value) = parse_timestamp(&data).unwrap();
        assert!(remaining.is_empty());
        assert_eq!(value, Value::Timestamp(ts_ms));
    }

    #[test]
    fn test_parse_timestamp_negative() {
        // Test negative timestamp (before epoch)
        let ts_ms: i64 = -86400000; // -1 day in milliseconds
        let data = ts_ms.to_be_bytes();
        let (remaining, value) = parse_timestamp(&data).unwrap();
        assert!(remaining.is_empty());
        assert_eq!(value, Value::Timestamp(ts_ms));
    }

    #[test]
    fn test_parse_varint_single_byte() {
        // Varint with 1-byte value
        use super::super::super::vint::encode_vint;
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(1)); // length = 1
        data.push(0x2A); // value 42
        let (remaining, value) = parse_varint(&data).unwrap();
        assert!(remaining.is_empty());
        assert_eq!(value, Value::Varint(vec![0x2A]));
    }

    #[test]
    fn test_parse_varint_multi_byte() {
        // Varint with multi-byte value (big number)
        use super::super::super::vint::encode_vint;
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(4)); // length = 4
        data.extend_from_slice(&[0x01, 0x02, 0x03, 0x04]);
        let (remaining, value) = parse_varint(&data).unwrap();
        assert!(remaining.is_empty());
        assert_eq!(value, Value::Varint(vec![0x01, 0x02, 0x03, 0x04]));
    }

    #[test]
    fn test_parse_decimal() {
        // Cassandra decimal: 4-byte BE i32 scale + raw BigInteger bytes (NOT VInt).
        // e.g., 1.23 = scale 2, unscaled raw bytes = [0x7B] (= 123)
        let mut data = Vec::new();
        data.extend_from_slice(&2i32.to_be_bytes()); // scale = 2
        data.push(0x7B); // unscaled BigInteger bytes = 123
        let (remaining, value) = parse_decimal(&data).unwrap();
        assert!(remaining.is_empty());
        match value {
            Value::Decimal {
                scale,
                ref unscaled,
            } => {
                assert_eq!(scale, 2, "scale should be 2");
                assert_eq!(unscaled, &[0x7B], "unscaled should be [0x7B]=123 (= 1.23)");
            }
            other => panic!("Expected Decimal value, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_decimal_negative_scale() {
        // scale = -2, unscaled = [0x05] = 5 → value is 500 (5 × 10^2)
        let mut data = Vec::new();
        data.extend_from_slice(&(-2i32).to_be_bytes()); // scale = -2
        data.push(0x05); // unscaled = 5
        let (remaining, value) = parse_decimal(&data).unwrap();
        assert!(remaining.is_empty());
        match value {
            Value::Decimal {
                scale,
                ref unscaled,
            } => {
                assert_eq!(scale, -2, "scale should be -2");
                assert_eq!(unscaled, &[0x05], "unscaled should be [0x05]=5");
            }
            other => panic!("Expected Decimal value, got {:?}", other),
        }
    }

    /// S2 regression: decimal unscaled bytes > 127 must NOT be VInt-decoded.
    /// If decoded as VInt, byte 0x80 would be misread (high bit set = multi-byte VInt).
    #[test]
    fn test_parse_decimal_large_unscaled_no_vint_misread() {
        // scale = 0, unscaled = [0x01, 0x00] = 256 (big-endian BigInteger)
        let mut data = Vec::new();
        data.extend_from_slice(&0i32.to_be_bytes()); // scale = 0
        data.push(0x01); // MSB of 256
        data.push(0x00); // LSB of 256
        let (remaining, value) = parse_decimal(&data).unwrap();
        assert!(remaining.is_empty());
        match value {
            Value::Decimal {
                scale,
                ref unscaled,
            } => {
                assert_eq!(scale, 0);
                assert_eq!(unscaled, &[0x01, 0x00], "unscaled [0x01,0x00] = 256");
            }
            other => panic!("Expected Decimal value, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_inet_ipv4() {
        // IPv4 address (4 bytes)
        use super::super::super::vint::encode_vint;
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(4)); // length = 4
        data.extend_from_slice(&[192, 168, 1, 1]); // 192.168.1.1
        let (remaining, value) = parse_inet(&data).unwrap();
        assert!(remaining.is_empty());
        assert_eq!(value, Value::Inet(vec![192, 168, 1, 1]));
    }

    #[test]
    fn test_parse_inet_ipv6() {
        // IPv6 address (16 bytes)
        use super::super::super::vint::encode_vint;
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(16)); // length = 16
        let ipv6_bytes: [u8; 16] = [
            0x20, 0x01, 0x0d, 0xb8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x01,
        ];
        data.extend_from_slice(&ipv6_bytes); // 2001:db8::1
        let (remaining, value) = parse_inet(&data).unwrap();
        assert!(remaining.is_empty());
        assert_eq!(value, Value::Inet(ipv6_bytes.to_vec()));
    }

    #[test]
    fn test_parse_duration() {
        // Duration: months (signed VInt) + days (signed VInt) + nanos (signed VInt)
        // Returns Value::Duration { months, days, nanos } preserving calendar semantics.
        use super::super::super::vint::encode_vint;
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(1)); // 1 month
        data.extend_from_slice(&encode_vint(15)); // 15 days
        data.extend_from_slice(&encode_vint(3_600_000_000_000_i64)); // 1 hour in nanos
        let (remaining, value) = parse_duration(&data).unwrap();
        assert!(remaining.is_empty());
        match value {
            Value::Duration {
                months,
                days,
                nanos,
            } => {
                assert_eq!(months, 1, "months should be 1");
                assert_eq!(days, 15, "days should be 15");
                assert_eq!(
                    nanos, 3_600_000_000_000,
                    "nanos should be 1 hour in nanoseconds"
                );
            }
            other => panic!("Expected Duration value, got {:?}", other),
        }
    }

    /// Issue #1632 (hardening b): months/days are i32 in Cassandra's
    /// DurationType. A corrupt encoding with a value outside the i32 range must be
    /// rejected as an error, NOT silently truncated via `as i32`. Fails on the
    /// pre-#1632 code where the cast wraps the value instead of erroring.
    #[test]
    fn test_parse_duration_months_out_of_i32_range_errors() {
        use super::super::super::vint::{encode_vint, parse_vint};

        let too_big = i32::MAX as i64 + 1;
        // Sanity: the VInt round-trips to the intended out-of-range value.
        let months_bytes = encode_vint(too_big);
        assert_eq!(parse_vint(&months_bytes).unwrap().1, too_big);

        let mut over = Vec::new();
        over.extend_from_slice(&months_bytes); // months overflow i32
        over.extend_from_slice(&encode_vint(0));
        over.extend_from_slice(&encode_vint(0));
        assert!(
            parse_duration(&over).is_err(),
            "months > i32::MAX must error, not truncate"
        );

        let too_small = i32::MIN as i64 - 1;
        let mut under = Vec::new();
        under.extend_from_slice(&encode_vint(0));
        under.extend_from_slice(&encode_vint(too_small)); // days underflow i32
        under.extend_from_slice(&encode_vint(0));
        assert!(
            parse_duration(&under).is_err(),
            "days < i32::MIN must error, not truncate"
        );
    }

    /// S2 regression: duration with all-zero components should produce Duration{{0,0,0}}.
    #[test]
    fn test_parse_duration_zero() {
        use super::super::super::vint::encode_vint;
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(0));
        data.extend_from_slice(&encode_vint(0));
        data.extend_from_slice(&encode_vint(0));
        let (remaining, value) = parse_duration(&data).unwrap();
        assert!(remaining.is_empty());
        assert_eq!(
            value,
            Value::Duration {
                months: 0,
                days: 0,
                nanos: 0
            },
            "zero duration should be Duration{{0,0,0}}"
        );
    }

    /// S2 regression: duration with negative nanos (nanoseconds can be negative).
    #[test]
    fn test_parse_duration_negative_nanos() {
        use super::super::super::vint::encode_vint;
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(0));
        data.extend_from_slice(&encode_vint(0));
        data.extend_from_slice(&encode_vint(-1_i64));
        let (remaining, value) = parse_duration(&data).unwrap();
        assert!(remaining.is_empty());
        assert_eq!(
            value,
            Value::Duration {
                months: 0,
                days: 0,
                nanos: -1
            },
            "duration with -1 nanos should be Duration{{0,0,-1}}"
        );
    }

    #[test]
    fn test_parse_date() {
        // Cassandra DATE format: stored as u32 with Integer.MIN_VALUE offset
        // For example, 2023-12-18 is 19710 days since epoch
        // Cassandra stores it as: 19710 - i32::MIN = 19710 + 2147483648 = 2147503358
        let days_since_epoch: i32 = 19710; // 2023-12-18
        let stored = (days_since_epoch as u32).wrapping_sub(i32::MIN as u32);
        let data = stored.to_be_bytes();
        let (remaining, value) = parse_date(&data).unwrap();
        assert!(remaining.is_empty());
        assert_eq!(value, Value::Date(days_since_epoch));
    }

    #[test]
    fn test_parse_time() {
        // Time: nanoseconds since midnight (i64)
        let nanos: i64 = 43_200_000_000_000; // 12:00:00 in nanoseconds
        let data = nanos.to_be_bytes();
        let (remaining, value) = parse_time(&data).unwrap();
        assert!(remaining.is_empty());
        assert_eq!(value, Value::Time(nanos));
    }

    #[test]
    fn test_parse_tinyint() {
        let data = [0x7F]; // 127
        let (remaining, value) = parse_tinyint(&data).unwrap();
        assert!(remaining.is_empty());
        assert_eq!(value, Value::Integer(127));

        let data = [0x80]; // -128 (signed)
        let (remaining, value) = parse_tinyint(&data).unwrap();
        assert!(remaining.is_empty());
        assert_eq!(value, Value::Integer(-128));
    }

    #[test]
    fn test_parse_smallint() {
        let data = 0x7FFFi16.to_be_bytes(); // 32767
        let (remaining, value) = parse_smallint(&data).unwrap();
        assert!(remaining.is_empty());
        assert_eq!(value, Value::Integer(32767));

        let data = (-32768i16).to_be_bytes(); // -32768
        let (remaining, value) = parse_smallint(&data).unwrap();
        assert!(remaining.is_empty());
        assert_eq!(value, Value::Integer(-32768));
    }

    #[test]
    fn test_parse_bigint() {
        let data = 0x7FFF_FFFF_FFFF_FFFFi64.to_be_bytes(); // i64::MAX
        let (remaining, value) = parse_bigint(&data).unwrap();
        assert!(remaining.is_empty());
        assert_eq!(value, Value::BigInt(i64::MAX));
    }

    #[test]
    fn test_parse_counter() {
        // Counter is stored as i64 big-endian
        let counter_value: i64 = 42;
        let data = counter_value.to_be_bytes();
        let (remaining, value) = parse_counter(&data).unwrap();
        assert!(remaining.is_empty());
        assert_eq!(value, Value::Counter(42));
    }

    #[test]
    fn test_parse_float() {
        let float_value: f32 = 42.5; // Use a non-constant value
        let data = float_value.to_be_bytes();
        let (remaining, value) = parse_float(&data).unwrap();
        assert!(remaining.is_empty());
        if let Value::Float32(f) = value {
            assert!((f - 42.5_f32).abs() < 0.0001);
        } else {
            panic!("Expected Float32, got {:?}", value);
        }
    }

    #[test]
    fn test_parse_double() {
        let double_value: f64 = std::f64::consts::E;
        let data = double_value.to_be_bytes();
        let (remaining, value) = parse_double(&data).unwrap();
        assert!(remaining.is_empty());
        if let Value::Float(f) = value {
            assert!((f - std::f64::consts::E).abs() < 0.0001);
        } else {
            panic!("Expected Float");
        }
    }
}
