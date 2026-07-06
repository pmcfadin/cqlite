//! Value formatting shared by output writers
//! Implements the Value → String mapping per QUERY_RESULT_CONTRACT.md
//!
//! This module provides stable, cqlsh-compatible formatting for all CQL value
//! types.  It originally lived in `cqlite-cli/src/output/value_fmt.rs` and was
//! moved into core (Issue #683) so that the Parquet export writer — which uses
//! it for textual fallbacks such as inet and duration — can live in
//! `cqlite-core` together with its formatting helpers.  The CLI re-exports it
//! unchanged from `cqlite_cli::output::value_fmt`.

use crate::types::Value;
use chrono::DateTime;
use std::net::{Ipv4Addr, Ipv6Addr};

/// ValueFormatter provides cqlsh-compatible string formatting for CQL values
pub struct ValueFormatter;

/// Lowercase hex digits for the lookup-table UUID/hex encoders.
const HEX_LOWER: &[u8; 16] = b"0123456789abcdef";

impl ValueFormatter {
    /// Returns `true` when the value represents a genuine CQL `NULL`.
    ///
    /// This is the authoritative null predicate used by output writers to decide
    /// whether to emit an empty CSV field. It replaces the fragile
    /// `format_value(v) == "null"` sentinel, which wrongly collapsed a literal
    /// text value `"null"` to an empty field (issue #1499). Only `Value::Null`
    /// formats to the bare string `"null"`, so matching on the variant is exact.
    #[inline]
    pub fn is_null(value: &Value) -> bool {
        matches!(value, Value::Null)
    }

    /// Append the formatted string representation of `value` to `out`.
    ///
    /// Byte-for-byte identical to `format_value`, but writes into a caller-owned
    /// scratch buffer so hot loops (CSV rows) can reuse one allocation across all
    /// cells instead of allocating a fresh `String` per cell (issue #1499). Scalar
    /// hot paths are written directly; rarer complex types delegate to
    /// `format_value` for a single append.
    pub fn format_into(value: &Value, out: &mut String) {
        use std::fmt::Write as _;
        match value {
            Value::Null => out.push_str("null"),
            Value::Boolean(b) => out.push_str(if *b { "true" } else { "false" }),
            Value::TinyInt(i) => {
                let _ = write!(out, "{}", i);
            }
            Value::SmallInt(i) => {
                let _ = write!(out, "{}", i);
            }
            Value::Integer(i) => {
                let _ = write!(out, "{}", i);
            }
            Value::BigInt(i) => {
                let _ = write!(out, "{}", i);
            }
            Value::Counter(i) => {
                let _ = write!(out, "{}", i);
            }
            Value::Text(s) => out.push_str(s),
            Value::Uuid(bytes) => Self::format_uuid_into(bytes, out),
            // Complex / rarer types: single append via the owned formatter. This
            // keeps output byte-identical without duplicating their logic.
            other => out.push_str(&Self::format_value(other)),
        }
    }

    /// Encode a 16-byte UUID as lowercase hyphenated text into `out` using a hex
    /// lookup table (no per-cell `format!` machinery). Shared by `format_uuid`
    /// and the JSON writer (issue #1499).
    pub fn format_uuid_into(bytes: &[u8; 16], out: &mut String) {
        out.reserve(36);
        for (i, b) in bytes.iter().enumerate() {
            // Hyphens after bytes 4, 6, 8, and 10 (1-based).
            if matches!(i, 4 | 6 | 8 | 10) {
                out.push('-');
            }
            out.push(HEX_LOWER[(b >> 4) as usize] as char);
            out.push(HEX_LOWER[(b & 0x0f) as usize] as char);
        }
    }

    /// Format a Value to its string representation according to the contract specification
    ///
    /// # Contract Guarantees
    /// - UUID/TimeUUID: lowercase hyphenated (e.g., "a8f167f0-ebe7-4f20-a386-31ff138bec3b")
    /// - Timestamps: `YYYY-MM-DD HH:MM:SS[.fff][+0000]`, default UTC
    /// - Collections: list `[a, b]`, set `{a, b}`, map `{k: v}`
    /// - Blob: `0x`-prefixed lowercase hex
    /// - Boolean: `true`/`false`
    /// - Numbers: standard Rust formatting, avoid scientific notation unless necessary
    pub fn format_value(value: &Value) -> String {
        match value {
            Value::Null => "null".to_string(),

            // Boolean: lowercase true/false
            Value::Boolean(b) => b.to_string(),

            // Integer types: standard decimal formatting
            Value::TinyInt(i) => i.to_string(),
            Value::SmallInt(i) => i.to_string(),
            Value::Integer(i) => i.to_string(),
            Value::BigInt(i) => i.to_string(),
            Value::Counter(i) => i.to_string(),

            // Floating point: avoid scientific notation for reasonable ranges
            Value::Float32(f) => Self::format_float32(*f),
            Value::Float(f) => Self::format_float64(*f),

            // Text: output as-is (no quotes for CLI display)
            Value::Text(s) => s.clone(),

            // Blob: 0x-prefixed lowercase hex
            Value::Blob(bytes) => format!("0x{}", hex::encode(bytes)),

            // Timestamp: milliseconds since epoch → YYYY-MM-DD HH:MM:SS.fff+0000
            Value::Timestamp(millis) => Self::format_timestamp(*millis),

            // Date: days since epoch → YYYY-MM-DD
            Value::Date(days) => Self::format_date(*days),

            // Time: nanoseconds since midnight → HH:MM:SS.nnnnnnnnn
            Value::Time(nanos) => Self::format_time(*nanos),

            // UUID: lowercase hyphenated format
            Value::Uuid(bytes) => Self::format_uuid(bytes),

            // Varint: arbitrary precision integer as decimal string
            Value::Varint(bytes) => Self::format_varint(bytes),

            // Decimal: scale + unscaled value → decimal string
            Value::Decimal { scale, unscaled } => Self::format_decimal(*scale, unscaled),

            // Duration: months, days, nanoseconds → "XmoYdZns" format
            Value::Duration {
                months,
                days,
                nanos,
            } => Self::format_duration(*months, *days, *nanos),

            // JSON: serialize to JSON string
            Value::Json(json_value) => json_value.to_string(),

            // Collections
            Value::List(elements) => Self::format_list(elements),
            Value::Set(elements) => Self::format_set(elements),
            Value::Map(pairs) => Self::format_map(pairs),
            Value::Tuple(fields) => Self::format_tuple(fields),

            // User Defined Type
            Value::Udt(udt) => Self::format_udt(udt),

            // Frozen: unwrap and format inner value
            Value::Frozen(inner) => Self::format_value(inner),

            // Tombstone: special marker (should rarely appear in query results)
            Value::Tombstone(info) => format!("<deleted@{}>", info.deletion_time),

            // Inet: IPv4 or IPv6 address
            Value::Inet(bytes) => Self::format_inet(bytes),
        }
    }

    // ==================== Helper Methods ====================

    /// Format float32 avoiding scientific notation for reasonable ranges
    fn format_float32(f: f32) -> String {
        if f.is_nan() {
            "NaN".to_string()
        } else if f.is_infinite() {
            if f.is_sign_positive() {
                "Infinity".to_string()
            } else {
                "-Infinity".to_string()
            }
        } else if f.abs() < 1e-6 || f.abs() > 1e10 {
            format!("{:e}", f)
        } else {
            format!("{}", f)
        }
    }

    /// Format float64 avoiding scientific notation for reasonable ranges
    fn format_float64(f: f64) -> String {
        if f.is_nan() {
            "NaN".to_string()
        } else if f.is_infinite() {
            if f.is_sign_positive() {
                "Infinity".to_string()
            } else {
                "-Infinity".to_string()
            }
        } else if f.abs() < 1e-6 || f.abs() > 1e10 {
            format!("{:e}", f)
        } else {
            format!("{}", f)
        }
    }

    /// Format timestamp (milliseconds since epoch) as YYYY-MM-DD HH:MM:SS.fff+0000
    fn format_timestamp(millis: i64) -> String {
        // Use from_timestamp_millis to correctly handle pre-epoch timestamps
        // (truncating division was incorrect for negative values)
        if let Some(datetime) = DateTime::from_timestamp_millis(millis) {
            // Format with milliseconds: YYYY-MM-DD HH:MM:SS.fff+0000
            datetime.format("%Y-%m-%d %H:%M:%S%.3f+0000").to_string()
        } else {
            format!("<invalid-timestamp:{}>", millis)
        }
    }

    /// Format date (days since Unix epoch) as YYYY-MM-DD
    fn format_date(days: i32) -> String {
        // Unix epoch: 1970-01-01
        let epoch = DateTime::from_timestamp(0, 0)
            .map(|dt| dt.date_naive())
            .unwrap_or_else(|| {
                // Fallback: construct epoch date directly if timestamp fails
                // Ultimate fallback for Date value formatting
                chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap_or(chrono::NaiveDate::MIN)
            });

        if let Some(date) = epoch.checked_add_signed(chrono::Duration::days(days as i64)) {
            date.format("%Y-%m-%d").to_string()
        } else {
            format!("<invalid-date:{}>", days)
        }
    }

    /// Format time (nanoseconds since midnight) as HH:MM:SS.nnnnnnnnn
    fn format_time(nanos: i64) -> String {
        if nanos < 0 {
            return format!("<invalid-time:{}>", nanos);
        }

        let total_secs = nanos / 1_000_000_000;
        let hours = total_secs / 3600;
        let minutes = (total_secs % 3600) / 60;
        let seconds = total_secs % 60;
        let remaining_nanos = nanos % 1_000_000_000;

        if hours >= 24 {
            return format!("<invalid-time:{}>", nanos);
        }

        format!(
            "{:02}:{:02}:{:02}.{:09}",
            hours, minutes, seconds, remaining_nanos
        )
    }

    /// Format UUID as lowercase hyphenated format
    fn format_uuid(bytes: &[u8; 16]) -> String {
        let mut s = String::with_capacity(36);
        Self::format_uuid_into(bytes, &mut s);
        s
    }

    /// Format varint as decimal string
    fn format_varint(bytes: &[u8]) -> String {
        if bytes.is_empty() {
            return "0".to_string();
        }

        // Use from_signed_bytes_be to handle both positive and negative values correctly
        let result = num_bigint::BigInt::from_signed_bytes_be(bytes);
        result.to_string()
    }

    /// Format decimal (scale + unscaled value) as decimal string
    fn format_decimal(scale: i32, unscaled: &[u8]) -> String {
        if unscaled.is_empty() {
            return "0".to_string();
        }

        // Convert unscaled bytes to bigint
        let is_negative = (unscaled[0] & 0x80) != 0;
        let bigint = if is_negative {
            // Two's complement for negative
            num_bigint::BigInt::from_signed_bytes_be(unscaled)
        } else {
            num_bigint::BigInt::from_bytes_be(num_bigint::Sign::Plus, unscaled)
        };

        let mut decimal_str = bigint.to_string();
        let is_neg = decimal_str.starts_with('-');
        if is_neg {
            decimal_str = decimal_str[1..].to_string();
        }

        // Insert decimal point based on scale
        if scale <= 0 {
            // Scale <= 0: multiply by 10^(-scale)
            decimal_str.push_str(&"0".repeat((-scale) as usize));
        } else if scale as usize >= decimal_str.len() {
            // Need leading zeros
            let leading_zeros = scale as usize - decimal_str.len() + 1;
            decimal_str = format!("0.{}{}", "0".repeat(leading_zeros - 1), decimal_str);
        } else {
            // Insert decimal point
            let pos = decimal_str.len() - scale as usize;
            decimal_str.insert(pos, '.');
        }

        if is_neg {
            format!("-{}", decimal_str)
        } else {
            decimal_str
        }
    }

    /// Format duration as "XmoYdZns" (cqlsh format)
    fn format_duration(months: i32, days: i32, nanos: i64) -> String {
        let mut parts = Vec::new();

        if months != 0 {
            parts.push(format!("{}mo", months));
        }
        if days != 0 {
            parts.push(format!("{}d", days));
        }
        if nanos != 0 {
            parts.push(format!("{}ns", nanos));
        }

        if parts.is_empty() {
            "0ns".to_string()
        } else {
            parts.join("")
        }
    }

    /// Format list as [a, b, c]
    fn format_list(elements: &[Value]) -> String {
        let formatted_elements: Vec<String> = elements.iter().map(Self::format_value).collect();
        format!("[{}]", formatted_elements.join(", "))
    }

    /// Format set as {a, b, c}
    fn format_set(elements: &[Value]) -> String {
        let formatted_elements: Vec<String> = elements.iter().map(Self::format_value).collect();
        format!("{{{}}}", formatted_elements.join(", "))
    }

    /// Format map as {k1: v1, k2: v2}
    fn format_map(pairs: &[(Value, Value)]) -> String {
        let formatted_pairs: Vec<String> = pairs
            .iter()
            .map(|(k, v)| format!("{}: {}", Self::format_value(k), Self::format_value(v)))
            .collect();
        format!("{{{}}}", formatted_pairs.join(", "))
    }

    /// Format tuple as (a, b, c)
    fn format_tuple(fields: &[Value]) -> String {
        let formatted_fields: Vec<String> = fields.iter().map(Self::format_value).collect();
        format!("({})", formatted_fields.join(", "))
    }

    /// Format UDT as {field1: value1, field2: value2}
    fn format_udt(udt: &crate::types::UdtValue) -> String {
        let formatted_fields: Vec<String> = udt
            .fields
            .iter()
            .map(|field| {
                let value_str = field
                    .value
                    .as_ref()
                    .map(Self::format_value)
                    .unwrap_or_else(|| "null".to_string());
                format!("{}: {}", field.name, value_str)
            })
            .collect();
        format!("{{{}}}", formatted_fields.join(", "))
    }

    /// Format inet address (IPv4 or IPv6)
    fn format_inet(bytes: &[u8]) -> String {
        if bytes.len() == 4 {
            // IPv4
            let addr = Ipv4Addr::new(bytes[0], bytes[1], bytes[2], bytes[3]);
            addr.to_string()
        } else if bytes.len() == 16 {
            // IPv6
            let mut octets = [0u8; 16];
            octets.copy_from_slice(bytes);
            let addr = Ipv6Addr::from(octets);
            addr.to_string()
        } else {
            format!("<invalid-inet:{}-bytes>", bytes.len())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{UdtField, UdtValue};

    #[test]
    fn test_null() {
        assert_eq!(ValueFormatter::format_value(&Value::Null), "null");
    }

    #[test]
    fn test_boolean() {
        assert_eq!(ValueFormatter::format_value(&Value::Boolean(true)), "true");
        assert_eq!(
            ValueFormatter::format_value(&Value::Boolean(false)),
            "false"
        );
    }

    #[test]
    fn test_integers() {
        assert_eq!(ValueFormatter::format_value(&Value::TinyInt(127)), "127");
        assert_eq!(ValueFormatter::format_value(&Value::TinyInt(-128)), "-128");
        assert_eq!(
            ValueFormatter::format_value(&Value::SmallInt(32767)),
            "32767"
        );
        assert_eq!(
            ValueFormatter::format_value(&Value::Integer(2147483647)),
            "2147483647"
        );
        assert_eq!(
            ValueFormatter::format_value(&Value::BigInt(9223372036854775807)),
            "9223372036854775807"
        );
        assert_eq!(
            ValueFormatter::format_value(&Value::Counter(1000000)),
            "1000000"
        );
    }

    #[test]
    fn test_floats() {
        assert_eq!(ValueFormatter::format_value(&Value::Float32(3.25)), "3.25");
        assert_eq!(ValueFormatter::format_value(&Value::Float(2.75)), "2.75");

        // Special values
        assert_eq!(
            ValueFormatter::format_value(&Value::Float32(f32::NAN)),
            "NaN"
        );
        assert_eq!(
            ValueFormatter::format_value(&Value::Float32(f32::INFINITY)),
            "Infinity"
        );
        assert_eq!(
            ValueFormatter::format_value(&Value::Float32(f32::NEG_INFINITY)),
            "-Infinity"
        );

        // Scientific notation for very small/large numbers
        let small = Value::Float(1e-7);
        let formatted = ValueFormatter::format_value(&small);
        assert!(formatted.contains('e') || formatted.contains('E'));
    }

    #[test]
    fn test_text() {
        assert_eq!(
            ValueFormatter::format_value(&Value::Text("hello world".to_string())),
            "hello world"
        );
        assert_eq!(
            ValueFormatter::format_value(&Value::Text("".to_string())),
            ""
        );
    }

    #[test]
    fn test_blob() {
        let blob = Value::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(ValueFormatter::format_value(&blob), "0xdeadbeef");

        let empty_blob = Value::Blob(vec![]);
        assert_eq!(ValueFormatter::format_value(&empty_blob), "0x");
    }

    #[test]
    fn test_uuid() {
        // UUID: a8f167f0-ebe7-4f20-a386-31ff138bec3b
        let uuid = Value::Uuid([
            0xa8, 0xf1, 0x67, 0xf0, 0xeb, 0xe7, 0x4f, 0x20, 0xa3, 0x86, 0x31, 0xff, 0x13, 0x8b,
            0xec, 0x3b,
        ]);
        assert_eq!(
            ValueFormatter::format_value(&uuid),
            "a8f167f0-ebe7-4f20-a386-31ff138bec3b"
        );
    }

    #[test]
    fn test_timestamp() {
        // 2023-01-15 10:30:45.123 UTC = 1673778645123 milliseconds
        let timestamp = Value::Timestamp(1673778645123);
        let formatted = ValueFormatter::format_value(&timestamp);
        assert!(formatted.starts_with("2023-01-15"));
        assert!(formatted.contains("10:30:45"));
        assert!(formatted.ends_with("+0000"));
    }

    #[test]
    fn test_date() {
        // 2023-01-01 = 19358 days since 1970-01-01
        let date = Value::Date(19358);
        assert_eq!(ValueFormatter::format_value(&date), "2023-01-01");

        // Unix epoch
        let epoch = Value::Date(0);
        assert_eq!(ValueFormatter::format_value(&epoch), "1970-01-01");
    }

    #[test]
    fn test_time() {
        // 14:30:45.123456789
        let nanos =
            14 * 3600 * 1_000_000_000 + 30 * 60 * 1_000_000_000 + 45 * 1_000_000_000 + 123_456_789;
        let time = Value::Time(nanos);
        assert_eq!(ValueFormatter::format_value(&time), "14:30:45.123456789");

        // Midnight
        let midnight = Value::Time(0);
        assert_eq!(
            ValueFormatter::format_value(&midnight),
            "00:00:00.000000000"
        );
    }

    #[test]
    fn test_duration() {
        let duration = Value::Duration {
            months: 2,
            days: 15,
            nanos: 123456789,
        };
        assert_eq!(ValueFormatter::format_value(&duration), "2mo15d123456789ns");

        let zero_duration = Value::Duration {
            months: 0,
            days: 0,
            nanos: 0,
        };
        assert_eq!(ValueFormatter::format_value(&zero_duration), "0ns");

        let partial_duration = Value::Duration {
            months: 0,
            days: 5,
            nanos: 0,
        };
        assert_eq!(ValueFormatter::format_value(&partial_duration), "5d");
    }

    #[test]
    fn test_list() {
        let list = Value::List(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ]);
        assert_eq!(ValueFormatter::format_value(&list), "[1, 2, 3]");

        let empty_list = Value::List(vec![]);
        assert_eq!(ValueFormatter::format_value(&empty_list), "[]");
    }

    #[test]
    fn test_set() {
        let set = Value::Set(vec![
            Value::Text("apple".to_string()),
            Value::Text("banana".to_string()),
        ]);
        assert_eq!(ValueFormatter::format_value(&set), "{apple, banana}");

        let empty_set = Value::Set(vec![]);
        assert_eq!(ValueFormatter::format_value(&empty_set), "{}");
    }

    #[test]
    fn test_map() {
        let map = Value::Map(vec![
            (Value::Text("key1".to_string()), Value::Integer(100)),
            (Value::Text("key2".to_string()), Value::Integer(200)),
        ]);
        assert_eq!(ValueFormatter::format_value(&map), "{key1: 100, key2: 200}");

        let empty_map = Value::Map(vec![]);
        assert_eq!(ValueFormatter::format_value(&empty_map), "{}");
    }

    #[test]
    fn test_tuple() {
        let tuple = Value::Tuple(vec![
            Value::Integer(42),
            Value::Text("hello".to_string()),
            Value::Boolean(true),
        ]);
        assert_eq!(ValueFormatter::format_value(&tuple), "(42, hello, true)");
    }

    #[test]
    fn test_udt() {
        let udt = Value::Udt(UdtValue {
            type_name: "person".to_string(),
            keyspace: "test_ks".to_string(),
            fields: vec![
                UdtField {
                    name: "name".to_string(),
                    value: Some(Value::Text("Alice".to_string())),
                },
                UdtField {
                    name: "age".to_string(),
                    value: Some(Value::Integer(30)),
                },
                UdtField {
                    name: "email".to_string(),
                    value: None,
                },
            ],
        });
        assert_eq!(
            ValueFormatter::format_value(&udt),
            "{name: Alice, age: 30, email: null}"
        );
    }

    #[test]
    fn test_frozen() {
        let frozen = Value::Frozen(Box::new(Value::List(vec![
            Value::Integer(1),
            Value::Integer(2),
        ])));
        assert_eq!(ValueFormatter::format_value(&frozen), "[1, 2]");
    }

    #[test]
    fn test_inet() {
        // IPv4
        let ipv4 = Value::Inet(vec![192, 168, 1, 1]);
        assert_eq!(ValueFormatter::format_value(&ipv4), "192.168.1.1");

        // IPv6
        let ipv6 = Value::Inet(vec![
            0x20, 0x01, 0x0d, 0xb8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x01,
        ]);
        let formatted = ValueFormatter::format_value(&ipv6);
        assert!(formatted.contains("2001:db8"));
    }

    #[test]
    fn test_nested_collections() {
        // List of lists
        let nested = Value::List(vec![
            Value::List(vec![Value::Integer(1), Value::Integer(2)]),
            Value::List(vec![Value::Integer(3), Value::Integer(4)]),
        ]);
        assert_eq!(ValueFormatter::format_value(&nested), "[[1, 2], [3, 4]]");

        // Map with complex values
        let complex_map = Value::Map(vec![(
            Value::Text("data".to_string()),
            Value::Set(vec![Value::Integer(1), Value::Integer(2)]),
        )]);
        assert_eq!(ValueFormatter::format_value(&complex_map), "{data: {1, 2}}");
    }

    #[test]
    fn test_json() {
        let json = Value::Json(serde_json::json!({
            "name": "Alice",
            "age": 30
        }));
        let formatted = ValueFormatter::format_value(&json);
        assert!(formatted.contains("Alice"));
        assert!(formatted.contains("30"));
    }

    #[test]
    fn test_varint() {
        // Positive varint
        let varint = Value::Varint(vec![0x01, 0x00]);
        let formatted = ValueFormatter::format_value(&varint);
        assert_eq!(formatted, "256");

        // Zero
        let zero = Value::Varint(vec![]);
        assert_eq!(ValueFormatter::format_value(&zero), "0");
    }

    #[test]
    fn test_decimal() {
        // 123.45 (scale=2, unscaled=12345)
        let decimal = Value::Decimal {
            scale: 2,
            unscaled: vec![0x30, 0x39], // 12345 in big-endian
        };
        let formatted = ValueFormatter::format_value(&decimal);
        // Should contain decimal point
        assert!(formatted.contains('.'));
    }

    #[test]
    fn test_is_null_only_matches_null_variant() {
        // Issue #1499: is_null must be exact — a literal text "null" is NOT null.
        assert!(ValueFormatter::is_null(&Value::Null));
        assert!(!ValueFormatter::is_null(&Value::Text("null".to_string())));
        assert!(!ValueFormatter::is_null(&Value::Integer(0)));
        assert!(!ValueFormatter::is_null(&Value::Text(String::new())));
    }

    #[test]
    fn test_format_into_matches_format_value() {
        // Issue #1499: format_into must be byte-identical to format_value for every
        // representative variant, including scalar hot paths and complex fallbacks.
        let samples = vec![
            Value::Null,
            Value::Boolean(true),
            Value::Boolean(false),
            Value::TinyInt(-7),
            Value::SmallInt(1234),
            Value::Integer(-2147483648),
            Value::BigInt(9223372036854775807),
            Value::Counter(42),
            Value::Text("hello".to_string()),
            Value::Text("null".to_string()),
            Value::Text(String::new()),
            Value::Uuid([
                0xa8, 0xf1, 0x67, 0xf0, 0xeb, 0xe7, 0x4f, 0x20, 0xa3, 0x86, 0x31, 0xff, 0x13, 0x8b,
                0xec, 0x3b,
            ]),
            Value::Float32(3.25),
            Value::Float(2.75),
            Value::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]),
            Value::List(vec![Value::Integer(1), Value::Integer(2)]),
            Value::Set(vec![Value::Text("a".to_string())]),
            Value::Map(vec![(Value::Text("k".to_string()), Value::Integer(1))]),
            Value::Varint(vec![0x01, 0x00]),
        ];
        for v in &samples {
            let mut buf = String::new();
            ValueFormatter::format_into(v, &mut buf);
            assert_eq!(
                buf,
                ValueFormatter::format_value(v),
                "format_into mismatch for {v:?}"
            );
        }
    }

    #[test]
    fn test_format_into_reuses_buffer() {
        // Clearing and reusing the scratch buffer yields the same result as fresh.
        let mut buf = String::from("stale-contents");
        buf.clear();
        ValueFormatter::format_into(&Value::Integer(99), &mut buf);
        assert_eq!(buf, "99");
    }

    #[test]
    fn test_format_uuid_into_matches_reference() {
        let bytes = [
            0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66,
            0x77, 0x88,
        ];
        let mut s = String::new();
        ValueFormatter::format_uuid_into(&bytes, &mut s);
        assert_eq!(s, "12345678-9abc-def0-1122-334455667788");
        // And the owned formatter agrees.
        assert_eq!(ValueFormatter::format_value(&Value::Uuid(bytes)), s);
    }

    #[test]
    fn test_format_varint_negative() {
        // Test negative varint: -1 in big-endian two's complement
        let negative_bytes = vec![0xFF];
        let formatted = ValueFormatter::format_value(&Value::Varint(negative_bytes));
        assert_eq!(
            formatted, "-1",
            "Negative varint -1 should format correctly"
        );

        // Test larger negative number: -256
        let negative_256 = vec![0xFF, 0x00];
        let formatted_256 = ValueFormatter::format_value(&Value::Varint(negative_256));
        assert_eq!(
            formatted_256, "-256",
            "Negative varint -256 should format correctly"
        );

        // Ensure no debug markers like '<' or '>' in output
        assert!(!formatted.contains('<'), "Should not contain debug markers");
        assert!(!formatted.contains('>'), "Should not contain debug markers");
    }
}
