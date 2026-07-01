//! Value conversion from cqlite_core to JavaScript types.
//!
//! This module handles conversion of all CQL data types to their JavaScript equivalents.
//! The mapping follows Issue #302 specification for type fidelity.
//!
//! ## Type Mapping
//!
//! | CQL Type | JavaScript Type |
//! |----------|-----------------|
//! | Null | `null` |
//! | Boolean | `boolean` |
//! | TinyInt/SmallInt/Int | `number` |
//! | BigInt/Counter | `bigint` |
//! | Float/Float32 | `number` |
//! | Text | `string` |
//! | Blob | `Buffer` |
//! | Timestamp | `Date` |
//! | Date | `Date` |
//! | Time | `bigint` (nanoseconds) |
//! | Uuid | `string` (formatted) |
//! | Varint | `bigint` |
//! | Decimal | `string` (preserves precision) |
//! | Duration | `{ months, days, nanos }` |
//! | Inet | `string` (IP format) |
//! | List | `Array` |
//! | Set | `Set` |
//! | Map | `Map` |
//! | Tuple | `Array` |
//! | Udt | `object` with `_type`, `_keyspace`, and field properties |

use cqlite_core::types::Value;
use napi::{Env, JsFunction, JsObject, JsUnknown, Result};

/// Convert a CQL Value to a JavaScript value with native types.
///
/// This function creates proper JavaScript native types:
/// - BigInt for i64 values (preserves precision)
/// - Buffer for blob data
/// - Date for timestamps
/// - Set for CQL sets
/// - Map for CQL maps
///
/// # Arguments
///
/// * `env` - The napi environment
/// * `value` - The CQL value to convert
///
/// # Returns
///
/// A `JsUnknown` representing the JavaScript value
pub fn value_to_napi(env: &Env, value: &Value) -> Result<JsUnknown> {
    match value {
        // Null
        Value::Null => env.get_null().map(|v| v.into_unknown()),

        // Boolean
        Value::Boolean(b) => env.get_boolean(*b).map(|v| v.into_unknown()),

        // Integer types that fit in JavaScript number without precision loss
        Value::TinyInt(i) => env.create_int32(*i as i32).map(|n| n.into_unknown()),
        Value::SmallInt(i) => env.create_int32(*i as i32).map(|n| n.into_unknown()),
        Value::Integer(i) => env.create_int32(*i).map(|n| n.into_unknown()),

        // 64-bit integers use BigInt to preserve precision
        Value::BigInt(i) => env.create_bigint_from_i64(*i)?.into_unknown(),
        Value::Counter(i) => env.create_bigint_from_i64(*i)?.into_unknown(),

        // Floating point
        Value::Float32(f) => env.create_double(*f as f64).map(|n| n.into_unknown()),
        Value::Float(f) => env.create_double(*f).map(|n| n.into_unknown()),

        // Text
        Value::Text(s) => env.create_string(s).map(|s| s.into_unknown()),

        // Blob -> Buffer
        Value::Blob(bytes) => env.create_buffer_copy(bytes).map(|b| b.into_unknown()),

        // Timestamp -> Date (milliseconds since epoch)
        Value::Timestamp(millis) => env.create_date(*millis as f64).map(|d| d.into_unknown()),

        // Date -> Date (days since epoch converted to milliseconds at midnight UTC)
        Value::Date(days) => {
            // CQL date is days since epoch (1970-01-01)
            // Convert to milliseconds: days * 24 * 60 * 60 * 1000 = days * 86400000
            // Note: JavaScript Date has no date-only type; time component will be midnight UTC
            let millis = (*days as i64)
                .checked_mul(86_400_000)
                .ok_or_else(|| napi::Error::from_reason("Date value overflow"))?;
            env.create_date(millis as f64).map(|d| d.into_unknown())
        }

        // Time -> BigInt (nanoseconds since midnight)
        Value::Time(nanos) => env.create_bigint_from_i64(*nanos)?.into_unknown(),

        // UUID -> formatted string
        Value::Uuid(bytes) => {
            let uuid = uuid::Uuid::from_bytes(*bytes);
            env.create_string(&uuid.to_string())
                .map(|s| s.into_unknown())
        }

        // Varint -> BigInt
        Value::Varint(bytes) => varint_to_bigint(env, bytes),

        // Decimal -> string (preserves arbitrary precision)
        Value::Decimal { scale, unscaled } => {
            let decimal_str = decimal_to_string(*scale, unscaled);
            env.create_string(&decimal_str).map(|s| s.into_unknown())
        }

        // Duration -> object { months, days, nanos }
        Value::Duration {
            months,
            days,
            nanos,
        } => duration_to_object(env, *months, *days, *nanos),

        // Inet -> IP address string
        Value::Inet(bytes) => inet_to_string_js(env, bytes),

        // JSON -> recursive conversion
        Value::Json(json) => json_to_napi(env, json),

        // List -> Array
        Value::List(items) => list_to_array(env, items),

        // Set -> JavaScript Set
        Value::Set(items) => set_to_js_set(env, items),

        // Map -> JavaScript Map
        Value::Map(pairs) => map_to_js_map(env, pairs),

        // Tuple -> Array
        Value::Tuple(items) => list_to_array(env, items),

        // UDT -> object with fields
        Value::Udt(udt) => udt_to_object(env, udt),

        // Frozen -> unwrap inner value
        Value::Frozen(inner) => value_to_napi(env, inner),

        // Tombstone -> null (deleted data)
        Value::Tombstone(_) => env.get_null().map(|v| v.into_unknown()),

        // Transient row carrier (issue #1334): expose as a name→value object. It
        // is disassembled into `QueryRow.values` before FFI, so this is defensive.
        Value::Row(cells) => {
            let mut obj = env.create_object()?;
            for (name, v) in cells {
                obj.set_named_property(name.as_ref(), value_to_napi(env, v)?)?;
            }
            Ok(obj.into_unknown())
        }
    }
}

/// Convert variable-length integer bytes to JavaScript BigInt.
///
/// Varint is stored as big-endian two's complement bytes.
fn varint_to_bigint(env: &Env, bytes: &[u8]) -> Result<JsUnknown> {
    if bytes.is_empty() {
        return env.create_bigint_from_i64(0)?.into_unknown();
    }

    // Determine sign from high bit
    let is_negative = (bytes[0] & 0x80) != 0;

    // For small varints that fit in i64, use the direct method
    if bytes.len() <= 8 {
        let mut value: i64 = 0;
        for &byte in bytes {
            value = (value << 8) | (byte as i64);
        }
        // Sign extend if negative
        if is_negative && bytes.len() < 8 {
            let sign_bits = !0i64 << (bytes.len() * 8);
            value |= sign_bits;
        }
        return env.create_bigint_from_i64(value)?.into_unknown();
    }

    // For larger varints, convert to u64 words for BigInt creation
    // napi's create_bigint_from_words expects little-endian u64 words
    let mut words: Vec<u64> = Vec::new();

    // Pad bytes to multiple of 8 for processing
    let padded_len = bytes.len().div_ceil(8) * 8;
    let mut padded = vec![if is_negative { 0xFF } else { 0x00 }; padded_len];
    padded[padded_len - bytes.len()..].copy_from_slice(bytes);

    // Convert to little-endian u64 words
    for chunk in padded.chunks(8).rev() {
        let word = u64::from_be_bytes(
            chunk
                .try_into()
                .map_err(|_| napi::Error::from_reason("Invalid varint chunk size"))?,
        );
        words.push(word);
    }

    // For negative numbers in two's complement, napi expects the magnitude
    // with a sign flag, not raw two's complement
    if is_negative {
        // Negate: invert all bits and add 1
        let mut carry = 1u64;
        for word in &mut words {
            *word = !*word;
            let (new_val, new_carry) = word.overflowing_add(carry);
            *word = new_val;
            carry = if new_carry { 1 } else { 0 };
        }
    }

    env.create_bigint_from_words(is_negative, words)?
        .into_unknown()
}

/// Convert decimal to string representation for arbitrary precision.
///
/// Format: Represents the decimal as an exact string.
/// For example: scale=2, unscaled=[1, 23] (123) -> "1.23"
fn decimal_to_string(scale: i32, unscaled: &[u8]) -> String {
    if unscaled.is_empty() {
        return "0".to_string();
    }

    // Determine sign from high bit (two's complement)
    let is_negative = (unscaled[0] & 0x80) != 0;

    // Convert bytes to absolute magnitude
    let mut magnitude = unscaled.to_vec();
    if is_negative {
        // Two's complement negation
        let mut carry = true;
        for byte in magnitude.iter_mut().rev() {
            *byte = !*byte;
            if carry {
                let (new_val, new_carry) = byte.overflowing_add(1);
                *byte = new_val;
                carry = new_carry;
            }
        }
    }

    // Convert bytes to decimal string using repeated division
    let mut digits = String::new();
    while !magnitude.is_empty() && magnitude.iter().any(|&b| b != 0) {
        let mut remainder = 0u32;
        for byte in &mut magnitude {
            let dividend = remainder * 256 + (*byte as u32);
            *byte = (dividend / 10) as u8;
            remainder = dividend % 10;
        }
        digits.push(char::from_digit(remainder, 10).unwrap());
        // Remove leading zeros from magnitude
        while magnitude.first() == Some(&0) {
            magnitude.remove(0);
        }
    }

    if digits.is_empty() {
        digits = "0".to_string();
    } else {
        // Reverse since we built it backwards
        digits = digits.chars().rev().collect();
    }

    // Apply scale
    let result = if scale == 0 {
        digits
    } else if scale > 0 {
        // Positive scale means decimal point moves left
        let scale_usize = scale as usize;
        if digits.len() <= scale_usize {
            // Need leading zeros: 123 with scale 5 -> 0.00123
            format!("0.{digits:0>scale_usize$}")
        } else {
            // Insert decimal point
            let split_point = digits.len() - scale_usize;
            let int_part = &digits[..split_point];
            let frac_part = &digits[split_point..];
            format!("{int_part}.{frac_part}")
        }
    } else {
        // Negative scale means multiply by power of 10
        let neg_scale = -scale;
        format!("{digits}e{neg_scale}")
    };

    if is_negative {
        format!("-{result}")
    } else {
        result
    }
}

/// Convert duration to JavaScript object { months, days, nanos }.
fn duration_to_object(env: &Env, months: i32, days: i32, nanos: i64) -> Result<JsUnknown> {
    let mut obj = env.create_object()?;
    obj.set_named_property("months", env.create_int32(months)?)?;
    obj.set_named_property("days", env.create_int32(days)?)?;
    let nanos_bigint = env.create_bigint_from_i64(nanos)?;
    obj.set_named_property("nanos", nanos_bigint)?;
    Ok(obj.into_unknown())
}

/// Convert inet bytes to IP address string.
fn inet_to_string_js(env: &Env, bytes: &[u8]) -> Result<JsUnknown> {
    let ip_str = match bytes.len() {
        4 => {
            let ip = std::net::Ipv4Addr::new(bytes[0], bytes[1], bytes[2], bytes[3]);
            ip.to_string()
        }
        16 => {
            let mut arr = [0u8; 16];
            arr.copy_from_slice(bytes);
            std::net::Ipv6Addr::from(arr).to_string()
        }
        _ => {
            return Err(napi::Error::from_reason(format!(
                "Invalid inet address length: {} (expected 4 or 16)",
                bytes.len()
            )))
        }
    };
    env.create_string(&ip_str).map(|s| s.into_unknown())
}

/// Convert serde_json::Value to JavaScript value.
fn json_to_napi(env: &Env, json: &serde_json::Value) -> Result<JsUnknown> {
    match json {
        serde_json::Value::Null => env.get_null().map(|v| v.into_unknown()),
        serde_json::Value::Bool(b) => env.get_boolean(*b).map(|v| v.into_unknown()),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                // Check if it fits in i32 for JavaScript number
                if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                    env.create_int32(i as i32).map(|v| v.into_unknown())
                } else {
                    // Use BigInt for large integers
                    env.create_bigint_from_i64(i)?.into_unknown()
                }
            } else if let Some(f) = n.as_f64() {
                env.create_double(f).map(|v| v.into_unknown())
            } else {
                env.get_null().map(|v| v.into_unknown())
            }
        }
        serde_json::Value::String(s) => env.create_string(s).map(|v| v.into_unknown()),
        serde_json::Value::Array(arr) => {
            let mut js_arr = env.create_array_with_length(arr.len())?;
            for (i, item) in arr.iter().enumerate() {
                js_arr.set_element(i as u32, json_to_napi(env, item)?)?;
            }
            Ok(js_arr.into_unknown())
        }
        serde_json::Value::Object(obj) => {
            let mut js_obj = env.create_object()?;
            for (k, v) in obj {
                js_obj.set_named_property(k, json_to_napi(env, v)?)?;
            }
            Ok(js_obj.into_unknown())
        }
    }
}

/// Convert CQL List to JavaScript Array.
fn list_to_array(env: &Env, items: &[Value]) -> Result<JsUnknown> {
    let mut arr = env.create_array_with_length(items.len())?;
    for (i, item) in items.iter().enumerate() {
        let js_value = value_to_napi(env, item)?;
        arr.set_element(i as u32, js_value)?;
    }
    Ok(arr.into_unknown())
}

/// Convert CQL Set to JavaScript Set.
///
/// Uses the global Set constructor to create a native JavaScript Set.
fn set_to_js_set(env: &Env, items: &[Value]) -> Result<JsUnknown> {
    // Get the Set constructor from global
    let global = env.get_global()?;
    let set_constructor: JsFunction = global.get_named_property("Set")?;

    // Create an array of items first
    let mut arr = env.create_array_with_length(items.len())?;
    for (i, item) in items.iter().enumerate() {
        let js_value = value_to_napi(env, item)?;
        arr.set_element(i as u32, js_value)?;
    }

    // Create new Set from array: new Set(array)
    let set_instance = set_constructor.new_instance(&[arr])?;
    Ok(set_instance.into_unknown())
}

/// Convert CQL Map to JavaScript Map.
///
/// Uses the global Map constructor to create a native JavaScript Map.
fn map_to_js_map(env: &Env, pairs: &[(Value, Value)]) -> Result<JsUnknown> {
    // Get the Map constructor from global
    let global = env.get_global()?;
    let map_constructor: JsFunction = global.get_named_property("Map")?;

    // Create an array of [key, value] pairs
    let mut entries = env.create_array_with_length(pairs.len())?;
    for (i, (key, value)) in pairs.iter().enumerate() {
        let mut pair = env.create_array_with_length(2)?;
        pair.set_element(0, value_to_napi(env, key)?)?;
        pair.set_element(1, value_to_napi(env, value)?)?;
        entries.set_element(i as u32, pair)?;
    }

    // Create new Map from entries: new Map([[k1, v1], [k2, v2], ...])
    let map_instance = map_constructor.new_instance(&[entries])?;
    Ok(map_instance.into_unknown())
}

/// Convert UDT to JavaScript object.
///
/// Creates an object with:
/// - `_type`: The UDT type name
/// - `_keyspace`: The keyspace containing the UDT
/// - All field names as properties
fn udt_to_object(env: &Env, udt: &cqlite_core::UdtValue) -> Result<JsUnknown> {
    let mut obj = env.create_object()?;

    // Add type metadata
    obj.set_named_property("_type", env.create_string(&udt.type_name)?)?;
    obj.set_named_property("_keyspace", env.create_string(&udt.keyspace)?)?;

    // Add fields
    for field in &udt.fields {
        let value = match &field.value {
            Some(v) => value_to_napi(env, v)?,
            None => env.get_null()?.into_unknown(),
        };
        obj.set_named_property(&field.name, value)?;
    }

    Ok(obj.into_unknown())
}

/// Convert row values to a JavaScript object.
///
/// This is a convenience function for converting query result rows.
pub fn row_to_object(
    env: &Env,
    values: &std::collections::HashMap<String, Value>,
) -> Result<JsObject> {
    let mut obj = env.create_object()?;
    for (col_name, value) in values {
        let js_value = value_to_napi(env, value)?;
        obj.set_named_property(col_name, js_value)?;
    }
    Ok(obj)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decimal_to_string_positive() {
        // 123 with scale 2 = 1.23
        let unscaled = vec![123];
        assert_eq!(decimal_to_string(2, &unscaled), "1.23");
    }

    #[test]
    fn test_decimal_to_string_no_scale() {
        // 123 with scale 0 = 123
        let unscaled = vec![123];
        assert_eq!(decimal_to_string(0, &unscaled), "123");
    }

    #[test]
    fn test_decimal_to_string_negative_scale() {
        // 123 with scale -2 = 12300 (123e2)
        let unscaled = vec![123];
        assert_eq!(decimal_to_string(-2, &unscaled), "123e2");
    }

    #[test]
    fn test_decimal_to_string_large_scale() {
        // 123 with scale 5 = 0.00123
        let unscaled = vec![123];
        assert_eq!(decimal_to_string(5, &unscaled), "0.00123");
    }

    #[test]
    fn test_decimal_to_string_empty() {
        assert_eq!(decimal_to_string(0, &[]), "0");
    }

    #[test]
    fn test_decimal_to_string_negative() {
        // -123 in two's complement (single byte) = 0x85 = 133, but need proper encoding
        // For -123: 256 - 123 = 133 = 0x85
        let unscaled = vec![0x85]; // -123 as two's complement byte
        assert_eq!(decimal_to_string(2, &unscaled), "-1.23");
    }
}
