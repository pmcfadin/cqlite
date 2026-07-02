//! Decoding for CQL scalar types that map to `ComparatorType::Custom` (issue #1627).
//!
//! `CqlType::Time`, `CqlType::Inet`, and the `json` schema type are represented as
//! `ComparatorType::Custom("time")` / `Custom("inet")` / `Custom("json")` (see
//! `ComparatorType::from_cql_type` and `CqlType::parse("json")`). Because the
//! `Custom(name)` string is derived authoritatively from the schema, dispatching
//! on it here is schema-driven — NOT a byte-pattern heuristic (no-heuristics
//! mandate, issue #28).
//!
//! Wire formats mirror the authoritative decoders (`raw_value.rs` and the free
//! `parse_value_with_comparator` in `comparator_value_parsing.rs`):
//! * `time` — 8-byte big-endian `i64` nanoseconds-since-midnight → [`Value::Time`].
//! * `inet` — raw address bytes, 4 for IPv4 or 16 for IPv6 (any other length is
//!   rejected as corruption) → [`Value::Inet`].
//! * `json` — UTF-8 text parsed into a `serde_json::Value` (invalid UTF-8 or
//!   invalid JSON is rejected as corruption) → [`Value::Json`].
//!
//! Any genuinely-unknown custom type is preserved verbatim as [`Value::Blob`] —
//! the only legitimate blob fallback.

use crate::{types::Value, Error, Result};

/// Decode a `ComparatorType::Custom(name)` value body into its typed `Value`.
pub(super) fn decode_custom_scalar(name: &str, value_data: &[u8]) -> Result<Value> {
    match name {
        "time" => {
            if value_data.len() == 8 {
                let nanos = i64::from_be_bytes([
                    value_data[0],
                    value_data[1],
                    value_data[2],
                    value_data[3],
                    value_data[4],
                    value_data[5],
                    value_data[6],
                    value_data[7],
                ]);
                Ok(Value::Time(nanos))
            } else {
                Err(Error::corruption("Invalid time value length"))
            }
        }
        "inet" => {
            // CQL `inet` is valid only as a 4-byte IPv4 or 16-byte IPv6 address
            // (Cassandra `InetAddressType`); reject any other length as corruption
            // rather than surfacing a malformed address (mirrors the `time` arm).
            if value_data.len() == 4 || value_data.len() == 16 {
                Ok(Value::Inet(value_data.to_vec()))
            } else {
                Err(Error::corruption("Invalid inet value length"))
            }
        }
        "json" => {
            // Mirrors the authoritative `ComparatorType::Json` arm in
            // `comparator_value_parsing::parse_value_with_comparator`: parse the
            // body as UTF-8, then as a JSON document.
            let json_text = std::str::from_utf8(value_data)
                .map_err(|_| Error::corruption("Invalid UTF-8 in JSON value"))?;
            let json_value: serde_json::Value = serde_json::from_str(json_text)
                .map_err(|_| Error::corruption("Invalid JSON value"))?;
            Ok(Value::Json(json_value))
        }
        // Genuinely-unknown custom type: preserve raw bytes verbatim.
        _ => Ok(Value::Blob(value_data.to_vec())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn time_decodes_8_byte_be_nanos() {
        let t: i64 = 4_500_000_000_000;
        assert_eq!(
            decode_custom_scalar("time", &t.to_be_bytes()).unwrap(),
            Value::Time(t)
        );
    }

    #[test]
    fn time_wrong_length_errors() {
        assert!(decode_custom_scalar("time", &[0u8; 4]).is_err());
    }

    #[test]
    fn inet_ipv4_and_ipv6_preserved() {
        assert_eq!(
            decode_custom_scalar("inet", &[10, 0, 0, 1]).unwrap(),
            Value::Inet(vec![10, 0, 0, 1])
        );
        let v6 = vec![0u8; 16];
        assert_eq!(
            decode_custom_scalar("inet", &v6).unwrap(),
            Value::Inet(v6.clone())
        );
    }

    #[test]
    fn inet_wrong_length_errors() {
        // Neither 4 (IPv4) nor 16 (IPv6) bytes: must be rejected as corruption.
        assert!(decode_custom_scalar("inet", &[0u8; 5]).is_err());
        assert!(decode_custom_scalar("inet", &[]).is_err());
        assert!(decode_custom_scalar("inet", &[0u8; 15]).is_err());
    }

    #[test]
    fn json_decodes_to_typed_value() {
        assert_eq!(
            decode_custom_scalar("json", br#"{"a":1}"#).unwrap(),
            Value::Json(serde_json::json!({"a": 1}))
        );
        assert_eq!(
            decode_custom_scalar("json", b"[1,2,3]").unwrap(),
            Value::Json(serde_json::json!([1, 2, 3]))
        );
    }

    #[test]
    fn json_invalid_errors() {
        // Not valid JSON.
        assert!(decode_custom_scalar("json", b"{not json").is_err());
        // Valid UTF-8 but empty is not a JSON document.
        assert!(decode_custom_scalar("json", b"").is_err());
        // Invalid UTF-8 bytes.
        assert!(decode_custom_scalar("json", &[0xff, 0xfe]).is_err());
    }

    #[test]
    fn unknown_custom_falls_back_to_blob() {
        assert_eq!(
            decode_custom_scalar("some_udt_marshaller", &[1, 2, 3]).unwrap(),
            Value::Blob(vec![1, 2, 3])
        );
    }
}
