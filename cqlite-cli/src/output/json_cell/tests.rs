//! Unit tests for the JSON-egress rendering of one CQL value.
//!
//! Split out of `cqlite-cli/src/output/json.rs` with the code they cover (issue
//! #3644 item 3); the writer-level tests stayed there.

use super::*;
use cqlite_core::types::{UdtField, UdtValue};
use cqlite_core::Value;

/// The cell as a `serde_json::Value`, for assertions where the value's exact
/// TEXT is not the point.
///
/// NEVER use it for a `decimal`/`varint`: parsing a JSON number back into a
/// `serde_json::Value` puts it through an `f64`, which is exactly the precision
/// loss [`JsonCell::Raw`] exists to avoid. Those cases assert on the emitted
/// TEXT.
fn as_json(value: &Value) -> JsonValue {
    serde_json::from_str(&JsonCell::to_json_text(value)).expect("valid JSON")
}

#[test]
fn test_uuid_formatting() {
    let uuid_bytes = [
        0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
        0x88,
    ];

    let json_val = as_json(&Value::Uuid(uuid_bytes));
    let uuid_str = json_val.as_str().unwrap();

    // Should be formatted as hyphenated UUID
    assert_eq!(uuid_str, "12345678-9abc-def0-1122-334455667788");
}

#[test]
fn test_list_value() {
    let list_value = Value::List(vec![
        Value::Integer(1),
        Value::Integer(2),
        Value::Integer(3),
    ]);

    let json_val = as_json(&list_value);
    assert!(json_val.is_array());

    let array = json_val.as_array().unwrap();
    assert_eq!(array.len(), 3);
    assert_eq!(array[0], serde_json::json!(1));
    assert_eq!(array[1], serde_json::json!(2));
    assert_eq!(array[2], serde_json::json!(3));
}

#[test]
fn test_map_value() {
    let map_value = Value::Map(vec![
        (Value::text("key1".to_string()), Value::Integer(1)),
        (Value::text("key2".to_string()), Value::Integer(2)),
    ]);

    let json_val = as_json(&map_value);
    assert!(json_val.is_array());

    let array = json_val.as_array().unwrap();
    assert_eq!(array.len(), 2);

    // Each entry should have "key" and "value" fields
    let entry1 = array[0].as_object().unwrap();
    assert_eq!(entry1.get("key").unwrap().as_str().unwrap(), "key1");
    assert_eq!(entry1.get("value").unwrap().as_i64().unwrap(), 1);
}

// Issue #227: Tests for human-readable formatting of complex types

#[test]
fn test_blob_formatting() {
    let blob = Value::blob(vec![0xDE, 0xAD, 0xBE, 0xEF]);
    let json_val = as_json(&blob);
    // Should be 0x hex format, not base64
    assert_eq!(json_val.as_str().unwrap(), "0xdeadbeef");
}

#[test]
fn test_timestamp_formatting() {
    // 2023-01-15 10:30:45.123 UTC = 1673778645123 milliseconds
    let timestamp = Value::Timestamp(1673778645123);
    let json_val = as_json(&timestamp);
    let formatted = json_val.as_str().unwrap();
    // Should be human-readable format, not raw milliseconds
    assert!(formatted.starts_with("2023-01-15"));
    assert!(formatted.contains("10:30:45"));
    assert!(formatted.ends_with("+0000"));
}

#[test]
fn test_date_formatting() {
    // 2023-01-01 = 19358 days since 1970-01-01
    let date = Value::Date(19358);
    let json_val = as_json(&date);
    // Should be YYYY-MM-DD format, not raw days number
    assert_eq!(json_val.as_str().unwrap(), "2023-01-01");
}

#[test]
fn test_time_formatting() {
    // 14:30:45.123456789 in nanoseconds
    let nanos =
        14 * 3600 * 1_000_000_000 + 30 * 60 * 1_000_000_000 + 45 * 1_000_000_000 + 123_456_789;
    let time = Value::Time(nanos);
    let json_val = as_json(&time);
    // Should be HH:MM:SS.nnnnnnnnn format, not raw nanoseconds
    assert_eq!(json_val.as_str().unwrap(), "14:30:45.123456789");
}

#[test]
fn test_duration_formatting() {
    let duration = Value::Duration {
        months: 2,
        days: 15,
        nanos: 123456789,
    };
    let json_val = as_json(&duration);
    // Should be "XmoYdZns" format, not {months, days, nanos} object
    assert_eq!(json_val.as_str().unwrap(), "2mo15d123456789ns");
}

/// Issue #806: tombstoned cells must render as JSON null, not as an internal
/// metadata object.  This matches cqlsh and Python binding behaviour.
#[test]
fn test_cell_tombstone_renders_as_null() {
    use cqlite_core::types::{TombstoneInfo, TombstoneType};

    let tombstone = Value::Tombstone(Box::new(TombstoneInfo {
        deletion_time: 1673778645000000,
        tombstone_type: TombstoneType::CellTombstone,
        local_deletion_time: 0,
        ttl: None,
        range_start: None,
        range_end: None,
    }));

    let json_val = as_json(&tombstone);
    assert!(
        json_val.is_null(),
        "CellTombstone must render as JSON null, got: {json_val}"
    );
}

#[test]
fn test_row_tombstone_renders_as_null() {
    use cqlite_core::types::{TombstoneInfo, TombstoneType};

    let tombstone = Value::Tombstone(Box::new(TombstoneInfo {
        deletion_time: 1673778645000000,
        tombstone_type: TombstoneType::RowTombstone,
        local_deletion_time: 0,
        ttl: None,
        range_start: None,
        range_end: None,
    }));

    let json_val = as_json(&tombstone);
    assert!(
        json_val.is_null(),
        "RowTombstone must render as JSON null, got: {json_val}"
    );
}

// ============================================================================
// `decimal` / `varint` render as UNQUOTED JSON numbers (issue #3644 item 3)
// ============================================================================
//
// Oracle, at the pinned tag, for every case below:
//   cassandra-5.0.8:src/java/org/apache/cassandra/db/marshal/DecimalType.java:314-317
//     `toJSONString` → `Objects.toString(getSerializer().deserialize(buffer), "\"\"")`,
//     an UNQUOTED `BigDecimal.toString()`, deliberately overriding the quoting
//     form at `AbstractType.java:186-189`.
//   cassandra-5.0.8:src/java/org/apache/cassandra/db/marshal/IntegerType.java:488-491
//     the `varint` `toJSONString`, identical shape, also unquoted.
//   cassandra-5.0.8:tools/.../JsonTransformer.java:494 writes a cell VALUE with
//     `writeRawValue(cellType.toJSONString(...))`, so that text lands unquoted.
//
// The assertions are on the emitted TEXT, never on a re-parsed value: parsing a
// JSON number yields an `f64` and destroys the digits under test.

/// A 33-significant-digit `decimal` — the committed
/// `test_signed_coll.signed_special_collections` fixture's own `sd` member —
/// survives digit for digit, unquoted.
#[test]
fn decimal_renders_as_an_unquoted_number_with_every_digit() {
    // -999999999999999999999999999999.999 = unscaled -999999999999999999999999999999999
    // at scale 3.
    let unscaled = num_bigint::BigInt::parse_bytes(b"-999999999999999999999999999999999", 10)
        .expect("literal parses")
        .to_signed_bytes_be();
    let value = Value::Decimal { scale: 3, unscaled };

    let text = JsonCell::to_json_text(&value);
    assert_eq!(text, "-999999999999999999999999999999.999");
    // …and it is a NUMBER, not a string: no quotes anywhere in the cell.
    assert!(!text.contains('"'), "decimal must not be quoted: {text}");
}

/// The whole emitted document must still PARSE — a raw fragment that is not
/// valid JSON would be worse than a quoted number.
#[test]
fn a_document_carrying_a_raw_decimal_is_valid_json() {
    let unscaled = num_bigint::BigInt::parse_bytes(b"123456789012345678901234567890123", 10)
        .expect("literal parses")
        .to_signed_bytes_be();
    let cell = JsonCell::from_value(&Value::Decimal { scale: 3, unscaled });
    let doc = serde_json::to_string(&vec![cell]).expect("serializes");
    assert_eq!(doc, "[123456789012345678901234567890.123]");
    let raws: Vec<Box<serde_json::value::RawValue>> =
        serde_json::from_str(&doc).expect("document parses as JSON");
    assert_eq!(raws[0].get(), "123456789012345678901234567890.123");
}

/// A `varint` beyond `u64::MAX` — `IntegerType.toJSONString:488-491`.
#[test]
fn varint_renders_as_an_unquoted_number_beyond_u64() {
    let unscaled = num_bigint::BigInt::parse_bytes(b"170141183460469231731687303715884105727", 10)
        .expect("literal parses")
        .to_signed_bytes_be();
    let text = JsonCell::to_json_text(&Value::varint(unscaled));
    assert_eq!(text, "170141183460469231731687303715884105727");
}

/// The small cases the pre-#3644 `test_varint_formatting` /
/// `test_decimal_formatting` pinned as QUOTED strings. Same digits, no quotes.
#[test]
fn small_varint_and_decimal_are_unquoted_too() {
    assert_eq!(
        JsonCell::to_json_text(&Value::varint(vec![0x01, 0x00])),
        "256"
    );
    assert_eq!(
        JsonCell::to_json_text(&Value::Decimal {
            scale: 2,
            unscaled: vec![0x30, 0x39],
        }),
        "123.45"
    );
    // A negative one, and one whose scale exceeds its digit count.
    assert_eq!(
        JsonCell::to_json_text(&Value::Decimal {
            scale: 5,
            unscaled: vec![0xFF, 0xFF, 0xCF, 0xC7],
        }),
        "-0.12345"
    );
}

/// FAIL-SAFE. `ValueFormatter::format_value` is total and renders an over-bound
/// `decimal` as the marker `<corrupt-decimal:…>`
/// (`cqlite-core/src/util/value_fmt.rs`, the 32 KiB ceiling from issue #1754).
/// That is not a JSON number, so it MUST fall back to a quoted string — emitting
/// it raw would produce an unparseable document.
#[test]
fn a_non_numeric_rendering_falls_back_to_a_json_string() {
    let value = Value::Decimal {
        scale: 2,
        unscaled: vec![0x01; 32 * 1024 + 1],
    };
    let text = JsonCell::to_json_text(&value);
    assert!(
        text.starts_with("\"<corrupt-decimal:") && text.ends_with('"'),
        "the corrupt marker must be a quoted string, got: {text}"
    );
    let parsed: serde_json::Value = serde_json::from_str(&text).expect("document parses as JSON");
    assert!(parsed.is_string(), "fallback must be a JSON string");
}

/// The exponent form `format_decimal` uses for an over-bound but VALID magnitude
/// (issue #1754: `<digits>e<-scale>`) is legal JSON, so it stays a raw number.
/// Measured, not assumed — the fail-safe must not quietly quote a legitimate
/// value.
#[test]
fn the_bounded_exponent_form_stays_an_unquoted_number() {
    // 1025 unscaled bytes is over the positional bound (1024) and well under the
    // 32 KiB corruption ceiling.
    let mut unscaled = vec![0x01u8; 1025];
    unscaled[0] = 0x01;
    let text = JsonCell::to_json_text(&Value::Decimal { scale: 4, unscaled });
    assert!(
        !text.contains('"'),
        "a valid over-bound decimal must stay a number: {}",
        &text[..text.len().min(40)]
    );
    assert!(
        text.ends_with("e-4"),
        "expected the exponent form: …{}",
        &text[text.len() - 8..]
    );
    let raw: Box<serde_json::value::RawValue> =
        serde_json::from_str(&text).expect("exponent form is valid JSON");
    assert_eq!(raw.get(), text);
}

/// NESTING. The divergence is a property of the TYPE, not of the position — and
/// the fixture that exposed it (`set<decimal>`) is nested. A `decimal` inside a
/// set, a list, a tuple, a map value and a UDT field is unquoted in every one.
#[test]
fn a_nested_decimal_is_unquoted_at_every_position() {
    let d = || Value::Decimal {
        scale: 2,
        unscaled: vec![0x30, 0x39],
    };

    assert_eq!(JsonCell::to_json_text(&Value::Set(vec![d()])), "[123.45]");
    assert_eq!(JsonCell::to_json_text(&Value::List(vec![d()])), "[123.45]");
    assert_eq!(
        JsonCell::to_json_text(&Value::Tuple(vec![Value::Integer(1), d()])),
        "[1,123.45]"
    );
    assert_eq!(
        JsonCell::to_json_text(&Value::Map(vec![(Value::text("k".to_string()), d())])),
        r#"[{"key":"k","value":123.45}]"#
    );
    assert_eq!(
        JsonCell::to_json_text(&Value::Frozen(Box::new(d()))),
        "123.45"
    );
    let udt = Value::Udt(Box::new(UdtValue {
        type_name: "money".to_string(),
        keyspace: "ks".to_string(),
        fields: vec![
            UdtField {
                name: "amount".to_string(),
                value: Some(d()),
            },
            UdtField {
                name: "missing".to_string(),
                value: None,
            },
        ],
    }));
    // Declared fields and nothing else (issue #3629), an absent field as `null`,
    // and the decimal unquoted.
    assert_eq!(
        JsonCell::to_json_text(&udt),
        r#"{"amount":123.45,"missing":null}"#
    );
}

/// Item 2 of issue #3644, kept as a pinned CORRECT behaviour rather than a gap:
/// a non-finite `double`/`float` renders as JSON `null`, matching
/// `cassandra-5.0.8:.../marshal/DoubleType.java:114-123` and
/// `FloatType.java:115-124`, whose `toJSONString` returns the literal `null`
/// ("JSON does not support NaN, Infinity and -Infinity values. Most of the
/// parser convert them into null."). This is NOT a defect awaiting a fix.
#[test]
fn a_non_finite_float_renders_as_json_null_per_doubletype() {
    for v in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert_eq!(JsonCell::to_json_text(&Value::Float(v)), "null");
    }
    for v in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
        assert_eq!(JsonCell::to_json_text(&Value::Float32(v)), "null");
    }
    // A FINITE float is a number, so the null above is the format's limit and
    // not a blanket rule.
    assert_eq!(JsonCell::to_json_text(&Value::Float(2.5)), "2.5");
}
