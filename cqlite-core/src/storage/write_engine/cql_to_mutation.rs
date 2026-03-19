//! CQL AST to Mutation conversion.
//!
//! Converts parsed CQL literal values into `Value` types that the WriteEngine
//! can persist. The primary entry point is `literal_to_value`, which performs
//! schema-aware coercion from the CQL parser's AST types to the internal
//! `Value` enum.

#[cfg(feature = "write-support")]
use crate::cql::ast::{CqlCollectionLiteral, CqlLiteral, CqlUdtLiteral};
#[cfg(feature = "write-support")]
use crate::schema::CqlType;
#[cfg(feature = "write-support")]
use crate::types::{UdtField, UdtValue, Value};
#[cfg(feature = "write-support")]
use crate::Error;

/// Convert a CQL AST literal value to an internal `Value`, guided by the
/// target schema type.
///
/// # Errors
///
/// Returns `Error::InvalidInput` for type mismatches or overflow, and
/// `Error::Parse` for malformed UUID/blob/inet strings.
#[cfg(feature = "write-support")]
pub fn literal_to_value(literal: &CqlLiteral, target_type: &CqlType) -> Result<Value, Error> {
    // Unwrap Frozen – it doesn't affect value representation
    if let CqlType::Frozen(inner) = target_type {
        let inner_value = literal_to_value(literal, inner)?;
        return Ok(Value::Frozen(Box::new(inner_value)));
    }

    match literal {
        CqlLiteral::Null => Ok(Value::Null),

        CqlLiteral::Boolean(b) => match target_type {
            CqlType::Boolean => Ok(Value::Boolean(*b)),
            _ => Err(type_mismatch("boolean", target_type)),
        },

        CqlLiteral::Integer(i) => integer_to_value(*i, target_type),

        CqlLiteral::Float(f) => match target_type {
            CqlType::Double => Ok(Value::Float(*f)),
            CqlType::Float => Ok(Value::Float32(*f as f32)),
            _ => Err(type_mismatch("float", target_type)),
        },

        CqlLiteral::String(s) => match target_type {
            CqlType::Text | CqlType::Varchar | CqlType::Ascii => Ok(Value::Text(s.clone())),
            CqlType::Inet => parse_inet(s),
            _ => Err(type_mismatch("string", target_type)),
        },

        CqlLiteral::Uuid(s) => match target_type {
            CqlType::Uuid | CqlType::TimeUuid => parse_uuid(s),
            _ => Err(type_mismatch("uuid", target_type)),
        },

        CqlLiteral::Blob(s) => match target_type {
            CqlType::Blob => parse_blob(s),
            _ => Err(type_mismatch("blob", target_type)),
        },

        CqlLiteral::Collection(coll) => collection_to_value(coll, target_type),

        CqlLiteral::Tuple(elements) => tuple_to_value(elements, target_type),

        CqlLiteral::Udt(udt) => udt_to_value(udt, target_type),
    }
}

/// Coerce an integer literal to the requested numeric type.
#[cfg(feature = "write-support")]
fn integer_to_value(i: i64, target: &CqlType) -> Result<Value, Error> {
    match target {
        CqlType::TinyInt => {
            let v = i8::try_from(i).map_err(|_| overflow_error(i, "tinyint"))?;
            Ok(Value::TinyInt(v))
        }
        CqlType::SmallInt => {
            let v = i16::try_from(i).map_err(|_| overflow_error(i, "smallint"))?;
            Ok(Value::SmallInt(v))
        }
        CqlType::Int => {
            let v = i32::try_from(i).map_err(|_| overflow_error(i, "int"))?;
            Ok(Value::Integer(v))
        }
        CqlType::BigInt | CqlType::Counter => Ok(Value::BigInt(i)),
        CqlType::Timestamp => Ok(Value::Timestamp(i)),
        CqlType::Date => {
            let v = i32::try_from(i).map_err(|_| overflow_error(i, "date"))?;
            Ok(Value::Date(v))
        }
        CqlType::Time => Ok(Value::Time(i)),
        CqlType::Float => Ok(Value::Float32(i as f32)),
        CqlType::Double => Ok(Value::Float(i as f64)),
        CqlType::Varint => {
            // Store as big-endian two's complement bytes (minimal encoding)
            let bytes = varint_to_bytes(i);
            Ok(Value::Varint(bytes))
        }
        _ => Err(type_mismatch("integer", target)),
    }
}

/// Encode a signed 64-bit integer as a minimal big-endian two's-complement
/// byte sequence (Cassandra varint format).
#[cfg(feature = "write-support")]
fn varint_to_bytes(i: i64) -> Vec<u8> {
    if i == 0 {
        return vec![0];
    }
    let be = i.to_be_bytes();
    // Find the shortest representation: strip leading bytes that are the sign
    // extension (0x00 for positive, 0xFF for negative) as long as the following
    // byte has the same sign bit.
    let sign_byte = if i < 0 { 0xFF_u8 } else { 0x00_u8 };
    let mut start = 0usize;
    while start < 7 {
        if be[start] == sign_byte {
            // Check that the next byte would not flip the sign bit
            let next_is_same_sign = if i < 0 {
                be[start + 1] & 0x80 != 0
            } else {
                be[start + 1] & 0x80 == 0
            };
            if next_is_same_sign {
                start += 1;
                continue;
            }
        }
        break;
    }
    be[start..].to_vec()
}

/// Parse a UUID string (with or without dashes) into a 16-byte array.
#[cfg(feature = "write-support")]
fn parse_uuid(s: &str) -> Result<Value, Error> {
    // Strip dashes
    let hex: String = s.chars().filter(|c| *c != '-').collect();
    if hex.len() != 32 {
        return Err(Error::Parse(format!(
            "invalid UUID string (expected 32 hex chars after stripping dashes): {:?}",
            s
        )));
    }
    let mut bytes = [0u8; 16];
    for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
        let hi = hex_nibble(chunk[0])?;
        let lo = hex_nibble(chunk[1])?;
        bytes[i] = (hi << 4) | lo;
    }
    Ok(Value::Uuid(bytes))
}

/// Parse a blob hex string (with optional `0x` prefix) into bytes.
#[cfg(feature = "write-support")]
fn parse_blob(s: &str) -> Result<Value, Error> {
    let hex = s
        .strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))
        .unwrap_or(s);
    if hex.len() % 2 != 0 {
        return Err(Error::Parse(format!(
            "blob hex string has odd length: {:?}",
            s
        )));
    }
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    for chunk in hex.as_bytes().chunks(2) {
        let hi = hex_nibble(chunk[0])?;
        let lo = hex_nibble(chunk[1])?;
        bytes.push((hi << 4) | lo);
    }
    Ok(Value::Blob(bytes))
}

/// Parse an IP address string into 4 (IPv4) or 16 (IPv6) bytes.
#[cfg(feature = "write-support")]
fn parse_inet(s: &str) -> Result<Value, Error> {
    use std::net::IpAddr;
    let addr: IpAddr = s
        .parse()
        .map_err(|_| Error::Parse(format!("invalid inet address: {:?}", s)))?;
    let bytes = match addr {
        IpAddr::V4(a) => a.octets().to_vec(),
        IpAddr::V6(a) => a.octets().to_vec(),
    };
    Ok(Value::Inet(bytes))
}

/// Convert a single hex ASCII byte to its nibble value.
#[cfg(feature = "write-support")]
fn hex_nibble(b: u8) -> Result<u8, Error> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => Err(Error::Parse(format!(
            "invalid hex character: {:?}",
            b as char
        ))),
    }
}

/// Convert a collection literal given the target CQL collection type.
#[cfg(feature = "write-support")]
fn collection_to_value(coll: &CqlCollectionLiteral, target: &CqlType) -> Result<Value, Error> {
    // Unwrap Frozen so that frozen collections work transparently
    if let CqlType::Frozen(inner) = target {
        let inner_value = collection_to_value(coll, inner)?;
        return Ok(Value::Frozen(Box::new(inner_value)));
    }

    match (coll, target) {
        (CqlCollectionLiteral::List(items), CqlType::List(elem_type)) => {
            let values = items
                .iter()
                .map(|lit| literal_to_value(lit, elem_type))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Value::List(values))
        }
        (CqlCollectionLiteral::Set(items), CqlType::Set(elem_type)) => {
            let values = items
                .iter()
                .map(|lit| literal_to_value(lit, elem_type))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Value::Set(values))
        }
        (CqlCollectionLiteral::Map(pairs), CqlType::Map(key_type, val_type)) => {
            let pairs = pairs
                .iter()
                .map(|(k, v)| {
                    Ok((
                        literal_to_value(k, key_type)?,
                        literal_to_value(v, val_type)?,
                    ))
                })
                .collect::<Result<Vec<_>, Error>>()?;
            Ok(Value::Map(pairs))
        }
        _ => Err(type_mismatch("collection", target)),
    }
}

/// Convert a tuple literal to `Value::Tuple`, using the positional types from
/// `CqlType::Tuple`.
#[cfg(feature = "write-support")]
fn tuple_to_value(elements: &[CqlLiteral], target: &CqlType) -> Result<Value, Error> {
    // Unwrap Frozen
    if let CqlType::Frozen(inner) = target {
        let inner_value = tuple_to_value(elements, inner)?;
        return Ok(Value::Frozen(Box::new(inner_value)));
    }

    match target {
        CqlType::Tuple(field_types) => {
            if elements.len() != field_types.len() {
                return Err(Error::InvalidInput(format!(
                    "tuple has {} elements but schema expects {}",
                    elements.len(),
                    field_types.len()
                )));
            }
            let values = elements
                .iter()
                .zip(field_types.iter())
                .map(|(lit, ft)| literal_to_value(lit, ft))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Value::Tuple(values))
        }
        _ => Err(type_mismatch("tuple", target)),
    }
}

/// Convert a UDT literal to `Value::Udt`, looking up field types from
/// `CqlType::Udt`.
#[cfg(feature = "write-support")]
fn udt_to_value(udt: &CqlUdtLiteral, target: &CqlType) -> Result<Value, Error> {
    // Unwrap Frozen
    if let CqlType::Frozen(inner) = target {
        let inner_value = udt_to_value(udt, inner)?;
        return Ok(Value::Frozen(Box::new(inner_value)));
    }

    match target {
        CqlType::Udt(type_name, field_defs) => {
            let mut fields: Vec<UdtField> = Vec::with_capacity(udt.fields.len());
            for (field_id, field_lit) in &udt.fields {
                let field_name = field_id.name.as_str();
                // Find the schema type for this field
                let field_type = field_defs
                    .iter()
                    .find(|(n, _)| n.eq_ignore_ascii_case(field_name))
                    .map(|(_, t)| t)
                    .ok_or_else(|| {
                        Error::InvalidInput(format!(
                            "field {:?} not found in UDT {:?}",
                            field_name, type_name
                        ))
                    })?;
                let value = literal_to_value(field_lit, field_type)?;
                fields.push(UdtField {
                    name: field_name.to_string(),
                    value: Some(value),
                });
            }
            Ok(Value::Udt(UdtValue {
                type_name: type_name.clone(),
                keyspace: String::new(),
                fields,
            }))
        }
        _ => Err(type_mismatch("udt", target)),
    }
}

/// Build a type-mismatch error with a human-readable message.
#[cfg(feature = "write-support")]
fn type_mismatch(literal_type: &str, target: &CqlType) -> Error {
    Error::InvalidInput(format!(
        "cannot coerce {} literal to {:?}",
        literal_type, target
    ))
}

/// Build an overflow error message.
#[cfg(feature = "write-support")]
fn overflow_error(value: i64, target: &str) -> Error {
    Error::InvalidInput(format!(
        "integer value {} overflows target type {}",
        value, target
    ))
}

#[cfg(all(test, feature = "write-support"))]
mod tests {
    use super::*;
    use crate::cql::ast::CqlLiteral;
    use crate::schema::CqlType;
    use crate::types::Value;

    // ── Null ─────────────────────────────────────────────────────────────────

    #[test]
    fn test_null_literal() {
        let result = literal_to_value(&CqlLiteral::Null, &CqlType::Text).unwrap();
        assert_eq!(result, Value::Null);

        let result = literal_to_value(&CqlLiteral::Null, &CqlType::Int).unwrap();
        assert_eq!(result, Value::Null);
    }

    // ── Boolean ──────────────────────────────────────────────────────────────

    #[test]
    fn test_boolean_literal() {
        let t = literal_to_value(&CqlLiteral::Boolean(true), &CqlType::Boolean).unwrap();
        assert_eq!(t, Value::Boolean(true));

        let f = literal_to_value(&CqlLiteral::Boolean(false), &CqlType::Boolean).unwrap();
        assert_eq!(f, Value::Boolean(false));
    }

    // ── Integer coercions ────────────────────────────────────────────────────

    #[test]
    fn test_integer_to_int() {
        let v = literal_to_value(&CqlLiteral::Integer(42), &CqlType::Int).unwrap();
        assert_eq!(v, Value::Integer(42));
    }

    #[test]
    fn test_integer_to_bigint() {
        let v = literal_to_value(&CqlLiteral::Integer(i64::MAX), &CqlType::BigInt).unwrap();
        assert_eq!(v, Value::BigInt(i64::MAX));
    }

    #[test]
    fn test_integer_to_smallint() {
        let v = literal_to_value(&CqlLiteral::Integer(1000), &CqlType::SmallInt).unwrap();
        assert_eq!(v, Value::SmallInt(1000));
    }

    #[test]
    fn test_integer_to_tinyint() {
        let v = literal_to_value(&CqlLiteral::Integer(127), &CqlType::TinyInt).unwrap();
        assert_eq!(v, Value::TinyInt(127));
    }

    #[test]
    fn test_integer_overflow_tinyint() {
        let err = literal_to_value(&CqlLiteral::Integer(999), &CqlType::TinyInt).unwrap_err();
        match err {
            Error::InvalidInput(msg) => assert!(msg.contains("overflow")),
            other => panic!("expected InvalidInput, got {:?}", other),
        }
    }

    #[test]
    fn test_integer_to_timestamp() {
        let ts: i64 = 1_700_000_000_000;
        let v = literal_to_value(&CqlLiteral::Integer(ts), &CqlType::Timestamp).unwrap();
        assert_eq!(v, Value::Timestamp(ts));
    }

    // ── Float coercions ──────────────────────────────────────────────────────

    #[test]
    fn test_float_to_float() {
        // Use 1.5 (exactly representable in f32/f64, not an approximation of a named constant)
        let v = literal_to_value(&CqlLiteral::Float(1.5), &CqlType::Float).unwrap();
        // CqlType::Float → Value::Float32
        match v {
            Value::Float32(f) => assert!((f - 1.5_f32).abs() < f32::EPSILON),
            other => panic!("expected Value::Float32, got {:?}", other),
        }
    }

    #[test]
    fn test_float_to_double() {
        let v = literal_to_value(&CqlLiteral::Float(1.25), &CqlType::Double).unwrap();
        assert_eq!(v, Value::Float(1.25));
    }

    // ── String coercions ─────────────────────────────────────────────────────

    #[test]
    fn test_string_to_text() {
        let v = literal_to_value(&CqlLiteral::String("hello".to_string()), &CqlType::Text).unwrap();
        assert_eq!(v, Value::Text("hello".to_string()));
    }

    #[test]
    fn test_string_to_varchar() {
        let v =
            literal_to_value(&CqlLiteral::String("world".to_string()), &CqlType::Varchar).unwrap();
        assert_eq!(v, Value::Text("world".to_string()));
    }

    #[test]
    fn test_string_to_ascii() {
        let v =
            literal_to_value(&CqlLiteral::String("ascii".to_string()), &CqlType::Ascii).unwrap();
        assert_eq!(v, Value::Text("ascii".to_string()));
    }

    // ── UUID ─────────────────────────────────────────────────────────────────

    #[test]
    fn test_uuid_literal() {
        let uuid_str = "550e8400-e29b-41d4-a716-446655440000".to_string();
        let v = literal_to_value(&CqlLiteral::Uuid(uuid_str), &CqlType::Uuid).unwrap();
        match v {
            Value::Uuid(bytes) => {
                assert_eq!(bytes[0], 0x55);
                assert_eq!(bytes[1], 0x0e);
                assert_eq!(bytes[15], 0x00);
            }
            other => panic!("expected Value::Uuid, got {:?}", other),
        }
    }

    // ── Blob ─────────────────────────────────────────────────────────────────

    #[test]
    fn test_blob_literal() {
        let v =
            literal_to_value(&CqlLiteral::Blob("0xDEADBEEF".to_string()), &CqlType::Blob).unwrap();
        assert_eq!(v, Value::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]));
    }

    // ── Type mismatch ────────────────────────────────────────────────────────

    #[test]
    fn test_type_mismatch_error() {
        let err = literal_to_value(&CqlLiteral::Boolean(true), &CqlType::Int).unwrap_err();
        match err {
            Error::InvalidInput(msg) => assert!(msg.contains("boolean")),
            other => panic!("expected InvalidInput, got {:?}", other),
        }
    }

    // ── Frozen unwrapping ────────────────────────────────────────────────────

    #[test]
    fn test_frozen_unwraps_to_inner() {
        let frozen_text = CqlType::Frozen(Box::new(CqlType::Text));
        let v = literal_to_value(&CqlLiteral::String("frozen".to_string()), &frozen_text).unwrap();
        match v {
            Value::Frozen(inner) => assert_eq!(*inner, Value::Text("frozen".to_string())),
            other => panic!("expected Value::Frozen, got {:?}", other),
        }
    }

    // ── Inet ─────────────────────────────────────────────────────────────────

    #[test]
    fn test_inet_ipv4() {
        let v =
            literal_to_value(&CqlLiteral::String("127.0.0.1".to_string()), &CqlType::Inet).unwrap();
        assert_eq!(v, Value::Inet(vec![127, 0, 0, 1]));
    }

    #[test]
    fn test_inet_ipv6() {
        let v = literal_to_value(&CqlLiteral::String("::1".to_string()), &CqlType::Inet).unwrap();
        match v {
            Value::Inet(bytes) => assert_eq!(bytes.len(), 16),
            other => panic!("expected Value::Inet, got {:?}", other),
        }
    }

    // ── Collections ──────────────────────────────────────────────────────────

    #[test]
    fn test_list_conversion() {
        let list_lit = CqlLiteral::Collection(CqlCollectionLiteral::List(vec![
            CqlLiteral::Integer(1),
            CqlLiteral::Integer(2),
            CqlLiteral::Integer(3),
        ]));
        let target = CqlType::List(Box::new(CqlType::Int));
        let v = literal_to_value(&list_lit, &target).unwrap();
        assert_eq!(
            v,
            Value::List(vec![
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(3)
            ])
        );
    }

    #[test]
    fn test_set_conversion() {
        let set_lit = CqlLiteral::Collection(CqlCollectionLiteral::Set(vec![
            CqlLiteral::String("a".to_string()),
            CqlLiteral::String("b".to_string()),
        ]));
        let target = CqlType::Set(Box::new(CqlType::Text));
        let v = literal_to_value(&set_lit, &target).unwrap();
        assert_eq!(
            v,
            Value::Set(vec![
                Value::Text("a".to_string()),
                Value::Text("b".to_string())
            ])
        );
    }

    #[test]
    fn test_map_conversion() {
        let map_lit = CqlLiteral::Collection(CqlCollectionLiteral::Map(vec![(
            CqlLiteral::String("key".to_string()),
            CqlLiteral::Integer(99),
        )]));
        let target = CqlType::Map(Box::new(CqlType::Text), Box::new(CqlType::BigInt));
        let v = literal_to_value(&map_lit, &target).unwrap();
        assert_eq!(
            v,
            Value::Map(vec![(Value::Text("key".to_string()), Value::BigInt(99))])
        );
    }

    // ── Varint ───────────────────────────────────────────────────────────────

    #[test]
    fn test_integer_to_varint() {
        let v = literal_to_value(&CqlLiteral::Integer(256), &CqlType::Varint).unwrap();
        match v {
            Value::Varint(bytes) => assert!(!bytes.is_empty()),
            other => panic!("expected Value::Varint, got {:?}", other),
        }
    }
}
