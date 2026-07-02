//! Decoding for CQL scalar types that map to `ComparatorType::Custom` (issue #1627).
//!
//! `CqlType::Time` and `CqlType::Inet` are represented as
//! `ComparatorType::Custom("time")` / `Custom("inet")` (see
//! `ComparatorType::from_cql_type`). Because the `Custom(name)` string is derived
//! authoritatively from the schema, dispatching on it here is schema-driven — NOT
//! a byte-pattern heuristic (no-heuristics mandate, issue #28).
//!
//! Wire formats mirror the V5CompressedLegacy decoder (`raw_value.rs`):
//! * `time` — 8-byte big-endian `i64` nanoseconds-since-midnight → [`Value::Time`].
//! * `inet` — raw address bytes (4 for IPv4, 16 for IPv6) → [`Value::Inet`].
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
        "inet" => Ok(Value::Inet(value_data.to_vec())),
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
    fn unknown_custom_falls_back_to_blob() {
        assert_eq!(
            decode_custom_scalar("some_udt_marshaller", &[1, 2, 3]).unwrap(),
            Value::Blob(vec![1, 2, 3])
        );
    }
}
