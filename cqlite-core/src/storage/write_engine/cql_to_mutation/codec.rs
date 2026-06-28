//! CQL literal/expression value codec.
//!
//! Converts CQL parser AST literals and expressions into the internal `Value`
//! enum, guided by the target schema type. Also provides JSON-to-`Value`
//! conversion for the `INSERT ... JSON` form and the low-level scalar parsers
//! (UUID, blob, inet, varint).

#[cfg(feature = "write-support")]
use crate::cql::ast::{
    CqlCollectionLiteral, CqlExpression, CqlLiteral, CqlUdtLiteral, CqlUnaryOperator,
};
#[cfg(feature = "write-support")]
use crate::schema::CqlType;
#[cfg(feature = "write-support")]
use crate::types::{UdtField, UdtValue, Value};
#[cfg(feature = "write-support")]
use crate::Error;

/// Convert a `CqlExpression` to a `Value` for mutation purposes.
///
/// Only literal expressions and unary minus on literals are supported.
///
/// # Errors
///
/// Returns `Error::InvalidInput` for non-literal expressions, and propagates
/// type-coercion errors from `literal_to_value`.
#[cfg(feature = "write-support")]
pub(super) fn expression_to_value(
    expr: &CqlExpression,
    target_type: &CqlType,
) -> Result<Value, Error> {
    match expr {
        CqlExpression::Literal(lit) => literal_to_value(lit, target_type),
        CqlExpression::Unary {
            operator: CqlUnaryOperator::Minus,
            operand,
        } => {
            // Handle negative numeric literals: -(integer) or -(float)
            match operand.as_ref() {
                CqlExpression::Literal(CqlLiteral::Integer(i)) => {
                    let negated = i.checked_neg().ok_or_else(|| {
                        Error::InvalidInput(format!("Integer {} cannot be negated (overflow)", i))
                    })?;
                    literal_to_value(&CqlLiteral::Integer(negated), target_type)
                }
                CqlExpression::Literal(CqlLiteral::Float(f)) => {
                    literal_to_value(&CqlLiteral::Float(-f), target_type)
                }
                _ => Err(Error::InvalidInput(
                    "Unary minus is only supported on integer or float literals".to_string(),
                )),
            }
        }
        _ => Err(Error::InvalidInput(
            "Only literal values are supported in mutations".to_string(),
        )),
    }
}

/// Convert a CQL AST literal value to an internal `Value`, guided by the
/// target schema type.
///
/// # Errors
///
/// Returns `Error::InvalidInput` for type mismatches or overflow, and
/// `Error::Parse` for malformed UUID/blob/inet strings.
#[cfg(feature = "write-support")]
pub(crate) fn literal_to_value(
    literal: &CqlLiteral,
    target_type: &CqlType,
) -> Result<Value, Error> {
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
            CqlType::Decimal => Err(Error::InvalidInput(
                "Float-to-Decimal conversion not supported; use a string literal".to_string(),
            )),
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
        CqlType::BigInt => Ok(Value::BigInt(i)),
        CqlType::Counter => Ok(Value::Counter(i)),
        CqlType::Duration => Err(Error::InvalidInput(
            "Duration type requires a duration literal (e.g. '1h30m'), not an integer".to_string(),
        )),
        CqlType::Decimal => {
            let unscaled = varint_to_bytes(i);
            Ok(Value::Decimal { scale: 0, unscaled })
        }
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

/// Parse a UUID string in standard format (8-4-4-4-12) into a 16-byte array.
#[cfg(feature = "write-support")]
fn parse_uuid(s: &str) -> Result<Value, Error> {
    let b = s.as_bytes();
    if b.len() != 36 || b[8] != b'-' || b[13] != b'-' || b[18] != b'-' || b[23] != b'-' {
        return Err(Error::Parse(format!(
            "invalid UUID string (expected 8-4-4-4-12 format): {:?}",
            s
        )));
    }
    let mut bytes = [0u8; 16];
    let segments: [&[u8]; 5] = [&b[0..8], &b[9..13], &b[14..18], &b[19..23], &b[24..36]];
    let mut out = 0;
    for seg in segments {
        for pair in seg.chunks(2) {
            bytes[out] = (hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?;
            out += 1;
        }
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
///
/// Expects a non-Frozen target type (Frozen is unwrapped by caller).
#[cfg(feature = "write-support")]
fn collection_to_value(coll: &CqlCollectionLiteral, target: &CqlType) -> Result<Value, Error> {
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
///
/// Expects a non-Frozen target type (Frozen is unwrapped by caller).
#[cfg(feature = "write-support")]
fn tuple_to_value(elements: &[CqlLiteral], target: &CqlType) -> Result<Value, Error> {
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
///
/// Expects a non-Frozen target type (Frozen is unwrapped by caller).
#[cfg(feature = "write-support")]
fn udt_to_value(udt: &CqlUdtLiteral, target: &CqlType) -> Result<Value, Error> {
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

/// Convert a `serde_json::Value` to an internal `Value`, guided by the target CQL type.
///
/// Supports boolean, numeric, string, array, and object JSON types. Null values
/// should be filtered out before calling this function.
#[cfg(feature = "write-support")]
pub(super) fn json_value_to_cql_value(
    json_val: &serde_json::Value,
    target_type: &CqlType,
) -> Result<Value, Error> {
    use serde_json::Value as JV;

    // Unwrap Frozen – it does not affect value representation
    if let CqlType::Frozen(inner) = target_type {
        return json_value_to_cql_value(json_val, inner);
    }

    match (json_val, target_type) {
        // Null — should be filtered before reaching here
        (JV::Null, _) => Err(Error::InvalidInput(
            "Unexpected null value in JSON conversion".to_string(),
        )),

        // Boolean
        (JV::Bool(b), CqlType::Boolean) => Ok(Value::Boolean(*b)),

        // Integer numbers
        (JV::Number(n), CqlType::Int) => {
            let v = n
                .as_i64()
                .ok_or_else(|| Error::InvalidInput(format!("Cannot convert {} to int", n)))?;
            let v = i32::try_from(v)
                .map_err(|_| Error::InvalidInput(format!("Value {} out of range for int", v)))?;
            Ok(Value::Integer(v))
        }
        (JV::Number(n), CqlType::BigInt) => {
            let v = n
                .as_i64()
                .ok_or_else(|| Error::InvalidInput(format!("Cannot convert {} to bigint", n)))?;
            Ok(Value::BigInt(v))
        }
        (JV::Number(n), CqlType::SmallInt) => {
            let v = n
                .as_i64()
                .ok_or_else(|| Error::InvalidInput(format!("Cannot convert {} to smallint", n)))?;
            let v = i16::try_from(v).map_err(|_| {
                Error::InvalidInput(format!("Value {} out of range for smallint", v))
            })?;
            Ok(Value::SmallInt(v))
        }
        (JV::Number(n), CqlType::TinyInt) => {
            let v = n
                .as_i64()
                .ok_or_else(|| Error::InvalidInput(format!("Cannot convert {} to tinyint", n)))?;
            let v = i8::try_from(v).map_err(|_| {
                Error::InvalidInput(format!("Value {} out of range for tinyint", v))
            })?;
            Ok(Value::TinyInt(v))
        }
        (JV::Number(n), CqlType::Timestamp) => {
            let v = n
                .as_i64()
                .ok_or_else(|| Error::InvalidInput(format!("Cannot convert {} to timestamp", n)))?;
            Ok(Value::Timestamp(v))
        }
        (JV::Number(n), CqlType::Float) => {
            let v = n
                .as_f64()
                .ok_or_else(|| Error::InvalidInput(format!("Cannot convert {} to float", n)))?;
            Ok(Value::Float32(v as f32))
        }
        (JV::Number(n), CqlType::Double) => {
            let v = n
                .as_f64()
                .ok_or_else(|| Error::InvalidInput(format!("Cannot convert {} to double", n)))?;
            Ok(Value::Float(v))
        }
        (JV::Number(n), CqlType::Varint) => {
            let v = n
                .as_i64()
                .ok_or_else(|| Error::InvalidInput(format!("Cannot convert {} to varint", n)))?;
            Ok(Value::Varint(varint_to_bytes(v)))
        }

        // String types
        (JV::String(s), CqlType::Text | CqlType::Varchar | CqlType::Ascii) => {
            Ok(Value::Text(s.clone()))
        }
        (JV::String(s), CqlType::Uuid | CqlType::TimeUuid) => parse_uuid(s),
        (JV::String(s), CqlType::Blob) => parse_blob(s),
        (JV::String(s), CqlType::Inet) => parse_inet(s),
        (JV::String(s), CqlType::Int) => {
            let v: i32 = s
                .parse()
                .map_err(|_| Error::InvalidInput(format!("Cannot parse '{}' as int", s)))?;
            Ok(Value::Integer(v))
        }
        (JV::String(s), CqlType::BigInt) => {
            let v: i64 = s
                .parse()
                .map_err(|_| Error::InvalidInput(format!("Cannot parse '{}' as bigint", s)))?;
            Ok(Value::BigInt(v))
        }
        (JV::String(s), CqlType::Boolean) => match s.to_lowercase().as_str() {
            "true" => Ok(Value::Boolean(true)),
            "false" => Ok(Value::Boolean(false)),
            _ => Err(Error::InvalidInput(format!(
                "Cannot parse '{}' as boolean",
                s
            ))),
        },
        (JV::String(s), CqlType::Timestamp) => {
            if let Ok(v) = s.parse::<i64>() {
                return Ok(Value::Timestamp(v));
            }
            Err(Error::InvalidInput(format!(
                "Cannot parse '{}' as timestamp",
                s
            )))
        }
        (JV::String(s), CqlType::Float) => {
            let v: f32 = s
                .parse()
                .map_err(|_| Error::InvalidInput(format!("Cannot parse '{}' as float", s)))?;
            Ok(Value::Float32(v))
        }
        (JV::String(s), CqlType::Double) => {
            let v: f64 = s
                .parse()
                .map_err(|_| Error::InvalidInput(format!("Cannot parse '{}' as double", s)))?;
            Ok(Value::Float(v))
        }

        // Arrays → Lists, Sets, Tuples
        (JV::Array(arr), CqlType::List(element_type)) => {
            let elements = arr
                .iter()
                .map(|item| json_value_to_cql_value(item, element_type))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Value::List(elements))
        }
        (JV::Array(arr), CqlType::Set(element_type)) => {
            let elements = arr
                .iter()
                .map(|item| json_value_to_cql_value(item, element_type))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Value::Set(elements))
        }
        (JV::Array(arr), CqlType::Tuple(types)) => {
            if arr.len() != types.len() {
                return Err(Error::InvalidInput(format!(
                    "JSON array has {} elements but tuple expects {}",
                    arr.len(),
                    types.len()
                )));
            }
            let elements = arr
                .iter()
                .zip(types.iter())
                .map(|(item, t)| json_value_to_cql_value(item, t))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Value::Tuple(elements))
        }

        // Objects → Maps
        (JV::Object(map), CqlType::Map(key_type, val_type)) => {
            let entries = map
                .iter()
                .map(|(k, v)| {
                    let key_json = JV::String(k.clone());
                    let key = json_value_to_cql_value(&key_json, key_type)?;
                    let val = json_value_to_cql_value(v, val_type)?;
                    Ok((key, val))
                })
                .collect::<Result<Vec<_>, Error>>()?;
            Ok(Value::Map(entries))
        }

        // Type mismatch
        _ => Err(Error::InvalidInput(format!(
            "Cannot convert JSON {} to CQL type {:?}",
            json_type_name(json_val),
            target_type
        ))),
    }
}

/// Return a human-readable name for a `serde_json::Value` variant.
#[cfg(feature = "write-support")]
fn json_type_name(val: &serde_json::Value) -> &'static str {
    match val {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
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
    use crate::cql::ast::{CqlCollectionLiteral, CqlExpression, CqlLiteral, CqlUnaryOperator};
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

    // ── Varint ───────────────────────────────────────────────────────────────

    #[test]
    fn test_integer_to_varint() {
        let v = literal_to_value(&CqlLiteral::Integer(256), &CqlType::Varint).unwrap();
        match v {
            Value::Varint(bytes) => assert!(!bytes.is_empty()),
            other => panic!("expected Value::Varint, got {:?}", other),
        }
    }

    // ── Counter ──────────────────────────────────────────────────────────────

    #[test]
    fn test_integer_to_counter() {
        let v = literal_to_value(&CqlLiteral::Integer(100), &CqlType::Counter).unwrap();
        assert_eq!(v, Value::Counter(100));
    }

    // ── Decimal ──────────────────────────────────────────────────────────────

    #[test]
    fn test_integer_to_decimal() {
        let v = literal_to_value(&CqlLiteral::Integer(42), &CqlType::Decimal).unwrap();
        match v {
            Value::Decimal { scale, unscaled } => {
                assert_eq!(scale, 0);
                assert!(!unscaled.is_empty());
            }
            other => panic!("expected Value::Decimal, got {:?}", other),
        }
    }

    #[test]
    fn test_float_to_decimal_returns_error() {
        let err = literal_to_value(&CqlLiteral::Float(1.23), &CqlType::Decimal).unwrap_err();
        match err {
            Error::InvalidInput(msg) => assert!(msg.contains("Float-to-Decimal")),
            other => panic!("expected InvalidInput, got {:?}", other),
        }
    }

    // ── Duration ─────────────────────────────────────────────────────────────

    #[test]
    fn test_integer_to_duration_returns_error() {
        let err = literal_to_value(&CqlLiteral::Integer(42), &CqlType::Duration).unwrap_err();
        match err {
            Error::InvalidInput(msg) => assert!(msg.contains("Duration")),
            other => panic!("expected InvalidInput, got {:?}", other),
        }
    }

    // ── varint_to_bytes correctness ───────────────────────────────────────────

    #[test]
    fn test_varint_bytes_correctness() {
        // 256 as i64 big-endian is [0,0,0,0,0,0,1,0]
        // varint_to_bytes should produce the minimal representation: [1, 0]
        let bytes = varint_to_bytes(256);
        assert_eq!(bytes, vec![0x01, 0x00]);
    }

    // ── Additional collection tests ───────────────────────────────────────────

    #[test]
    fn test_list_of_int() {
        let lit = CqlLiteral::Collection(CqlCollectionLiteral::List(vec![
            CqlLiteral::Integer(1),
            CqlLiteral::Integer(2),
            CqlLiteral::Integer(3),
        ]));
        let result = literal_to_value(&lit, &CqlType::List(Box::new(CqlType::Int)));
        assert_eq!(
            result.unwrap(),
            Value::List(vec![
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(3),
            ])
        );
    }

    #[test]
    fn test_set_of_text() {
        let lit = CqlLiteral::Collection(CqlCollectionLiteral::Set(vec![
            CqlLiteral::String("a".into()),
            CqlLiteral::String("b".into()),
        ]));
        let result = literal_to_value(&lit, &CqlType::Set(Box::new(CqlType::Text)));
        assert_eq!(
            result.unwrap(),
            Value::Set(vec![Value::Text("a".into()), Value::Text("b".into()),])
        );
    }

    #[test]
    fn test_map_of_text_to_int() {
        let lit = CqlLiteral::Collection(CqlCollectionLiteral::Map(vec![(
            CqlLiteral::String("a".into()),
            CqlLiteral::Integer(1),
        )]));
        let result = literal_to_value(
            &lit,
            &CqlType::Map(Box::new(CqlType::Text), Box::new(CqlType::Int)),
        );
        assert_eq!(
            result.unwrap(),
            Value::Map(vec![(Value::Text("a".into()), Value::Integer(1)),])
        );
    }

    #[test]
    fn test_frozen_list() {
        let lit = CqlLiteral::Collection(CqlCollectionLiteral::List(vec![CqlLiteral::Integer(1)]));
        let result = literal_to_value(
            &lit,
            &CqlType::Frozen(Box::new(CqlType::List(Box::new(CqlType::Int)))),
        );
        // literal_to_value wraps the inner value in Value::Frozen
        match result.unwrap() {
            Value::Frozen(inner) => {
                assert_eq!(*inner, Value::List(vec![Value::Integer(1)]));
            }
            other => panic!("expected Value::Frozen, got {:?}", other),
        }
    }

    #[test]
    fn test_tuple() {
        let lit = CqlLiteral::Tuple(vec![
            CqlLiteral::Integer(1),
            CqlLiteral::String("hello".into()),
        ]);
        let result = literal_to_value(&lit, &CqlType::Tuple(vec![CqlType::Int, CqlType::Text]));
        assert_eq!(
            result.unwrap(),
            Value::Tuple(vec![Value::Integer(1), Value::Text("hello".into()),])
        );
    }

    #[test]
    fn test_tuple_wrong_arity() {
        let lit = CqlLiteral::Tuple(vec![CqlLiteral::Integer(1)]);
        let result = literal_to_value(&lit, &CqlType::Tuple(vec![CqlType::Int, CqlType::Text]));
        assert!(result.is_err());
    }

    #[test]
    fn test_smallint_overflow() {
        let result = literal_to_value(&CqlLiteral::Integer(40000), &CqlType::SmallInt);
        assert!(result.is_err());
    }

    #[test]
    fn test_int_overflow() {
        let result = literal_to_value(&CqlLiteral::Integer(3_000_000_000), &CqlType::Int);
        assert!(result.is_err());
    }

    #[test]
    fn test_timeuuid() {
        let result = literal_to_value(
            &CqlLiteral::Uuid("550e8400-e29b-11d4-a716-446655440000".into()),
            &CqlType::TimeUuid,
        );
        assert!(result.is_ok());
        if let Value::Uuid(bytes) = result.unwrap() {
            assert_eq!(bytes.len(), 16);
        } else {
            panic!("expected Uuid");
        }
    }

    #[test]
    fn test_empty_list() {
        let lit = CqlLiteral::Collection(CqlCollectionLiteral::List(vec![]));
        let result = literal_to_value(&lit, &CqlType::List(Box::new(CqlType::Int)));
        assert_eq!(result.unwrap(), Value::List(vec![]));
    }

    #[test]
    fn test_collection_type_mismatch() {
        // List literal but Map target type
        let lit = CqlLiteral::Collection(CqlCollectionLiteral::List(vec![CqlLiteral::Integer(1)]));
        let result = literal_to_value(
            &lit,
            &CqlType::Map(Box::new(CqlType::Text), Box::new(CqlType::Int)),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_nested_collection() {
        // list<frozen<list<int>>>
        let inner_list = CqlLiteral::Collection(CqlCollectionLiteral::List(vec![
            CqlLiteral::Integer(1),
            CqlLiteral::Integer(2),
        ]));
        let outer_list = CqlLiteral::Collection(CqlCollectionLiteral::List(vec![inner_list]));
        let target = CqlType::List(Box::new(CqlType::Frozen(Box::new(CqlType::List(
            Box::new(CqlType::Int),
        )))));
        let result = literal_to_value(&outer_list, &target);
        assert!(result.is_ok());
    }

    // ── expression_to_value ───────────────────────────────────────────────────

    #[test]
    fn test_expression_negative_integer() {
        // expression_to_value with unary minus on an integer literal
        let expr = CqlExpression::Unary {
            operator: CqlUnaryOperator::Minus,
            operand: Box::new(CqlExpression::Literal(CqlLiteral::Integer(42))),
        };
        let result = expression_to_value(&expr, &CqlType::Int).unwrap();
        assert_eq!(result, Value::Integer(-42));
    }
}
