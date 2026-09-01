//! Unit tests for [`super`] — the JSON output writer (`json.rs`).
//!
//! Split out of `json.rs` under the campsite rule (epic #1116 / #1135): the
//! inline `mod tests` had grown the source file past the 800-line target, so
//! adding the issue-3777 float-spelling cases would have tripped the `file-size`
//! ratchet.

use super::*;
use cqlite_core::query::ColumnInfo;
use cqlite_core::{RowKey, Value};
use std::collections::HashMap;

fn default_config() -> OutputConfig {
    OutputConfig::default()
}

#[test]
fn test_deterministic_key_ordering() {
    // Create QueryResult with columns in reverse alphabetical order: [c, b, a]
    let mut result = QueryResult::new();

    // Set metadata with columns in specific order
    result.metadata.columns = vec![
        ColumnInfo::new(
            "c".to_string(),
            cqlite_core::types::DataType::Integer,
            false,
            0,
        ),
        ColumnInfo::new(
            "b".to_string(),
            cqlite_core::types::DataType::Integer,
            false,
            1,
        ),
        ColumnInfo::new(
            "a".to_string(),
            cqlite_core::types::DataType::Integer,
            false,
            2,
        ),
    ];

    // Add a row
    let mut values = HashMap::new();
    values.insert("a".to_string(), Value::Integer(1));
    values.insert("b".to_string(), Value::Integer(2));
    values.insert("c".to_string(), Value::Integer(3));

    let row = QueryRow::with_values(RowKey::new(vec![1]), values);
    result.rows.push(row);

    // Write to JSON
    let json_str = JSONWriter::write(&result, &default_config()).unwrap();

    // Parse to verify structure
    let parsed: Vec<serde_json::Value> = serde_json::from_str(&json_str).unwrap();
    assert_eq!(parsed.len(), 1);

    let row_obj = parsed[0].as_object().unwrap();

    // CRITICAL: Verify key order matches column order [c, b, a], NOT [a, b, c]
    let keys: Vec<&String> = row_obj.keys().collect();
    assert_eq!(keys, vec!["c", "b", "a"], "Keys must be in column order");

    // Verify JSON string representation has keys in correct order
    assert!(
        json_str.find("\"c\"").unwrap() < json_str.find("\"b\"").unwrap(),
        "Key 'c' must appear before 'b' in JSON string"
    );
    assert!(
        json_str.find("\"b\"").unwrap() < json_str.find("\"a\"").unwrap(),
        "Key 'b' must appear before 'a' in JSON string"
    );
}

/// Issue #1499: the borrowed-key serializer must produce byte-identical pretty
/// JSON to the previous `serde_json::Map` + `to_string_pretty` path.
#[test]
fn test_borrowed_key_pretty_output_is_byte_identical() {
    let mut result = QueryResult::new();
    result.metadata.columns = vec![
        ColumnInfo::new(
            "id".to_string(),
            cqlite_core::types::DataType::Integer,
            false,
            0,
        ),
        ColumnInfo::new(
            "name".to_string(),
            cqlite_core::types::DataType::Text,
            false,
            1,
        ),
    ];
    let mut values = HashMap::new();
    values.insert("id".to_string(), Value::Integer(7));
    values.insert("name".to_string(), Value::text("null".to_string()));
    result
        .rows
        .push(QueryRow::with_values(RowKey::new(vec![7]), values));

    let json_str = JSONWriter::write(&result, &default_config()).unwrap();

    // Reference: what the old Map-based path produced.
    let mut map = serde_json::Map::new();
    map.insert("id".to_string(), serde_json::json!(7));
    map.insert("name".to_string(), serde_json::json!("null"));
    let expected = serde_json::to_string_pretty(&vec![serde_json::Value::Object(map)]).unwrap();

    assert_eq!(json_str, expected);
    // A literal text "null" is a JSON string, never dropped.
    assert!(json_str.contains("\"null\""));
}

/// Issue #1499: a result whose `metadata.columns` contains a duplicate output
/// column name (e.g. `SELECT a, a`) must render a SINGLE `"a"` key holding the
/// LAST value, byte-identical to the old `serde_json::Map::insert` (last-wins)
/// path — NOT two duplicate `"a"` keys.
#[test]
fn test_duplicate_column_names_collapse_last_wins_batch() {
    let mut result = QueryResult::new();
    result.metadata.columns = vec![
        ColumnInfo::new(
            "a".to_string(),
            cqlite_core::types::DataType::Integer,
            false,
            0,
        ),
        ColumnInfo::new(
            "a".to_string(),
            cqlite_core::types::DataType::Integer,
            false,
            1,
        ),
    ];
    // The row's HashMap holds a single value per name — the LAST written value.
    let mut values = HashMap::new();
    values.insert("a".to_string(), Value::Integer(2));
    result
        .rows
        .push(QueryRow::with_values(RowKey::new(vec![1]), values));

    let json_str = JSONWriter::write(&result, &default_config()).unwrap();

    // Reference: old Map-based path, inserting both duplicate columns in order
    // (first=1, then last=2) collapses to a single `"a"` key holding 2.
    let mut map = serde_json::Map::new();
    map.insert("a".to_string(), serde_json::json!(1));
    map.insert("a".to_string(), serde_json::json!(2));
    let expected = serde_json::to_string_pretty(&vec![serde_json::Value::Object(map)]).unwrap();

    assert_eq!(
        json_str, expected,
        "duplicate column name must collapse to a single last-wins key"
    );
    // Exactly one occurrence of the `"a"` key.
    assert_eq!(json_str.matches("\"a\"").count(), 1);
}

/// Issue #1499: the streaming writer must apply the same duplicate-key collapse
/// as the batch writer.
#[test]
fn test_duplicate_column_names_collapse_last_wins_streaming() {
    let metadata = {
        let mut m = QueryResult::new().metadata;
        m.columns = vec![
            ColumnInfo::new(
                "a".to_string(),
                cqlite_core::types::DataType::Integer,
                false,
                0,
            ),
            ColumnInfo::new(
                "a".to_string(),
                cqlite_core::types::DataType::Integer,
                false,
                1,
            ),
        ];
        m
    };

    let mut values = HashMap::new();
    values.insert("a".to_string(), Value::Integer(2));
    let row = QueryRow::with_values(RowKey::new(vec![1]), values);

    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = StreamingJSONWriter::new(&mut buf);
        writer.write_header(&metadata).unwrap();
        writer.write_chunk(std::slice::from_ref(&row)).unwrap();
        writer.finalize().unwrap();
    }
    let json_str = String::from_utf8(buf).unwrap();

    // Parsing into a Map (last-wins) proves there is exactly one `"a"` key.
    let parsed: Vec<serde_json::Value> = serde_json::from_str(&json_str).unwrap();
    assert_eq!(parsed.len(), 1);
    let obj = parsed[0].as_object().unwrap();
    assert_eq!(obj.len(), 1, "duplicate key must collapse to one entry");
    assert_eq!(obj.get("a").unwrap(), &serde_json::json!(2));
    // No duplicate `"a"` key in the raw bytes.
    assert_eq!(json_str.matches("\"a\"").count(), 1);
}

#[test]
fn test_empty_result_is_empty_array_bytes() {
    // serialize_seq(Some(0)) must still render exactly "[]".
    let result = QueryResult::new();
    let json_str = JSONWriter::write(&result, &default_config()).unwrap();
    assert_eq!(json_str, "[]");
}

#[test]
fn test_null_values() {
    let mut result = QueryResult::new();
    result.metadata.columns = vec![ColumnInfo::new(
        "nullable_col".to_string(),
        cqlite_core::types::DataType::Text,
        true,
        0,
    )];

    // Row with missing value (should be null)
    let values = HashMap::new(); // Empty - no value for nullable_col
    let row = QueryRow::with_values(RowKey::new(vec![1]), values);
    result.rows.push(row);

    let json_str = JSONWriter::write(&result, &default_config()).unwrap();
    assert!(
        json_str.contains("null"),
        "Missing values should be JSON null"
    );
}

#[test]
fn test_value_types() {
    let mut result = QueryResult::new();
    result.metadata.columns = vec![
        ColumnInfo::new(
            "int_col".to_string(),
            cqlite_core::types::DataType::Integer,
            false,
            0,
        ),
        ColumnInfo::new(
            "text_col".to_string(),
            cqlite_core::types::DataType::Text,
            false,
            1,
        ),
        ColumnInfo::new(
            "bool_col".to_string(),
            cqlite_core::types::DataType::Boolean,
            false,
            2,
        ),
    ];

    let mut values = HashMap::new();
    values.insert("int_col".to_string(), Value::Integer(42));
    values.insert("text_col".to_string(), Value::text("hello".to_string()));
    values.insert("bool_col".to_string(), Value::Boolean(true));

    let row = QueryRow::with_values(RowKey::new(vec![1]), values);
    result.rows.push(row);

    let json_str = JSONWriter::write(&result, &default_config()).unwrap();

    // Verify values are correctly represented
    assert!(json_str.contains("42"));
    assert!(json_str.contains("\"hello\""));
    assert!(json_str.contains("true"));
}

#[test]
fn test_empty_result() {
    let result = QueryResult::new();
    let json_str = JSONWriter::write(&result, &default_config()).unwrap();

    // Empty result should be empty array
    let parsed: Vec<serde_json::Value> = serde_json::from_str(&json_str).unwrap();
    assert_eq!(parsed.len(), 0);
}

#[test]
fn test_multiple_rows() {
    let mut result = QueryResult::new();
    result.metadata.columns = vec![ColumnInfo::new(
        "id".to_string(),
        cqlite_core::types::DataType::Integer,
        false,
        0,
    )];

    // Add multiple rows
    for i in 1..=3 {
        let mut values = HashMap::new();
        values.insert("id".to_string(), Value::Integer(i));
        let row = QueryRow::with_values(RowKey::new(vec![i as u8]), values);
        result.rows.push(row);
    }

    let json_str = JSONWriter::write(&result, &default_config()).unwrap();
    let parsed: Vec<serde_json::Value> = serde_json::from_str(&json_str).unwrap();
    assert_eq!(parsed.len(), 3);
}

#[test]
fn test_config_limit() {
    let mut result = QueryResult::new();
    result.metadata.columns = vec![ColumnInfo::new(
        "id".to_string(),
        cqlite_core::types::DataType::Integer,
        false,
        0,
    )];

    // Add 10 rows
    for i in 1..=10 {
        let mut values = HashMap::new();
        values.insert("id".to_string(), Value::Integer(i));
        let row = QueryRow::with_values(RowKey::new(vec![i as u8]), values);
        result.rows.push(row);
    }

    // Apply limit of 3 rows
    let config = OutputConfig {
        color_enabled: true,
        limit: Some(3),
        page_size: None,
        target: crate::output::OutputTarget::Stdout,
        overwrite: false,
    };
    let json_str = JSONWriter::write(&result, &config).unwrap();
    let parsed: Vec<serde_json::Value> = serde_json::from_str(&json_str).unwrap();

    // Should only have 3 rows, not 10
    assert_eq!(parsed.len(), 3, "Limit should restrict output to 3 rows");
}

#[test]
fn test_uuid_formatting() {
    let uuid_bytes = [
        0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
        0x88,
    ];

    let json_val = JSONWriter::value_to_json(&Value::Uuid(uuid_bytes));
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

    let json_val = JSONWriter::value_to_json(&list_value);
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

    let json_val = JSONWriter::value_to_json(&map_value);
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
    let json_val = JSONWriter::value_to_json(&blob);
    // Should be 0x hex format, not base64
    assert_eq!(json_val.as_str().unwrap(), "0xdeadbeef");
}

#[test]
fn test_timestamp_formatting() {
    // 2023-01-15 10:30:45.123 UTC = 1673778645123 milliseconds
    let timestamp = Value::Timestamp(1673778645123);
    let json_val = JSONWriter::value_to_json(&timestamp);
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
    let json_val = JSONWriter::value_to_json(&date);
    // Should be YYYY-MM-DD format, not raw days number
    assert_eq!(json_val.as_str().unwrap(), "2023-01-01");
}

#[test]
fn test_time_formatting() {
    // 14:30:45.123456789 in nanoseconds
    let nanos =
        14 * 3600 * 1_000_000_000 + 30 * 60 * 1_000_000_000 + 45 * 1_000_000_000 + 123_456_789;
    let time = Value::Time(nanos);
    let json_val = JSONWriter::value_to_json(&time);
    // Should be HH:MM:SS.nnnnnnnnn format, not raw nanoseconds
    assert_eq!(json_val.as_str().unwrap(), "14:30:45.123456789");
}

#[test]
fn test_varint_formatting() {
    let varint = Value::varint(vec![0x01, 0x00]); // 256
    let json_val = JSONWriter::value_to_json(&varint);
    // Should be decimal string, not base64
    assert_eq!(json_val.as_str().unwrap(), "256");
}

#[test]
fn test_decimal_formatting() {
    // 123.45 with scale=2, unscaled=12345 (big-endian: 0x30, 0x39)
    let decimal = Value::Decimal {
        scale: 2,
        unscaled: vec![0x30, 0x39],
    };
    let json_val = JSONWriter::value_to_json(&decimal);
    // Should be human-readable decimal string, not {scale, unscaled} object
    let formatted = json_val.as_str().unwrap();
    assert!(formatted.contains('.'));
}

#[test]
fn test_duration_formatting() {
    let duration = Value::Duration {
        months: 2,
        days: 15,
        nanos: 123456789,
    };
    let json_val = JSONWriter::value_to_json(&duration);
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

    let json_val = JSONWriter::value_to_json(&tombstone);
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

    let json_val = JSONWriter::value_to_json(&tombstone);
    assert!(
        json_val.is_null(),
        "RowTombstone must render as JSON null, got: {json_val}"
    );
}

#[test]
fn test_tombstone_column_in_result_is_null() {
    use cqlite_core::types::{TombstoneInfo, TombstoneType};

    let mut result = QueryResult::new();
    result.metadata.columns = vec![ColumnInfo::new(
        "deleted_col".to_string(),
        cqlite_core::types::DataType::Tombstone,
        true,
        0,
    )];

    let mut values = HashMap::new();
    values.insert(
        "deleted_col".to_string(),
        Value::Tombstone(Box::new(TombstoneInfo {
            deletion_time: 0,
            tombstone_type: TombstoneType::CellTombstone,
            local_deletion_time: 0,
            ttl: None,
            range_start: None,
            range_end: None,
        })),
    );
    let row = QueryRow::with_values(RowKey::new(vec![1]), values);
    result.rows.push(row);

    let json_str = JSONWriter::write(&result, &default_config()).unwrap();
    let parsed: Vec<serde_json::Value> = serde_json::from_str(&json_str).unwrap();

    let col_val = &parsed[0]["deleted_col"];
    assert!(
        col_val.is_null(),
        "Tombstoned column must be JSON null in output, got: {col_val}"
    );
    // Ensure NO internal metadata leaked
    assert!(
        !json_str.contains("tombstone_type"),
        "Internal tombstone metadata must not appear in output: {json_str}"
    );
}

// ============================================================================
// FLOAT (f32) spelling — issue #3777
// ============================================================================

/// Serialize one value the way the writer does, so the assertions are on the
/// BYTES the CLI emits rather than on an intermediate `JsonValue`.
fn json_text(value: &Value) -> String {
    serde_json::to_string(&JSONWriter::value_to_json(value)).expect("serialize")
}

/// Significant digits of a decimal rendering: sign, decimal point, exponent,
/// leading zeros and trailing zeros carry no precision.
fn significant_digits(s: &str) -> usize {
    let mantissa = s.split(['e', 'E']).next().unwrap_or(s);
    let digits: String = mantissa.chars().filter(|c| c.is_ascii_digit()).collect();
    digits.trim_start_matches('0').trim_end_matches('0').len()
}

/// Issue #3777: a CQL `float` must serialize as the shortest decimal that
/// round-trips the **f32**, which is what `sstabledump` prints (Cassandra
/// `FloatSerializer` → `Float.toString`).
///
/// The oracle is the committed dump, NOT CQLite's own prior output:
///
/// ```text
/// $ grep -o '{"name":"height","value":[^,}]*' \
///     test-data/datasets/sstables/test_basic/simple_table-*/nb-1-big-Data.db.jsonl
/// {"name":"height","value":1.84
/// {"name":"height","value":1.65
/// ```
///
/// and `test_timeseries.sensor_data`, whose `temperature`/`humidity` floats carry
/// the full 7–9 significant digits an f32 can hold.
#[test]
fn float32_renders_shortest_decimal_that_round_trips_the_f32() {
    // (f32 literal, the spelling the sstabledump golden carries)
    let cases: &[(f32, &str)] = &[
        // test_basic.simple_table `height FLOAT`
        (1.67, "1.67"),
        (1.84, "1.84"),
        (1.65, "1.65"),
        (1.56, "1.56"),
        (1.87, "1.87"),
        // test_timeseries.sensor_data `temperature`/`humidity` FLOAT
        (92.88221, "92.88221"),
        (-16.172066, "-16.172066"),
        (1.5052613, "1.5052613"),
        (8.8656225, "8.8656225"),
        // Integral and zero spellings.
        (0.0, "0.0"),
        (-0.0, "-0.0"),
        (1.0, "1.0"),
        (-2.5, "-2.5"),
    ];

    for (f, expected) in cases {
        assert_eq!(
            json_text(&Value::Float32(*f)),
            *expected,
            "FLOAT {f} must render as the shortest f32 round-trip, not its widened f64"
        );
    }
}

/// The property behind the case list above: whatever the writer emits must parse
/// back to the SAME f32, and must carry no more digits than the f32's own
/// shortest round-trip spelling.
#[test]
fn float32_json_round_trips_through_f32_for_a_spread_of_values() {
    let values: &[f32] = &[
        1.67,
        -16.172066,
        f32::MIN_POSITIVE,
        f32::MAX,
        f32::MIN,
        1e-7,
        1e10,
        1.0 / 3.0,
        core::f32::consts::PI,
        16_777_215.0,
        0.1,
        1234.5678,
    ];
    for &f in values {
        let text = json_text(&Value::Float32(f));
        let parsed: f32 = text
            .parse()
            .unwrap_or_else(|e| panic!("emitted {text} for {f} is not parseable as f32: {e}"));
        assert_eq!(
            parsed.to_bits(),
            f.to_bits(),
            "emitted {text} does not round-trip {f}"
        );
        // No more SIGNIFICANT digits than `f32`'s own shortest spelling. Counted
        // rather than string-compared because JSON float notation legitimately
        // differs from Rust's `Display` in ways that carry no precision: ryu
        // renders `1e10` as `10000000000.0` (a trailing `.0`) and `f32::MAX` in
        // exponent form (`3.4028235e38`) where `Display` writes it out in full.
        assert!(
            significant_digits(&text) <= significant_digits(&f.to_string()),
            "emitted {text} carries more significant digits than the f32 shortest form {f}"
        );
    }
}

/// PRESERVED, deliberately: a non-finite `float`/`double` renders as JSON `null`
/// because JSON has no literal for `NaN`/`±Infinity`. That is a DECLARED 3-way
/// asymmetry (CLAUDE.md, `bindings/parity` declared gap 4; AD2's
/// `Divergence::NonFiniteFloatRendersAsJsonNull`) and is NOT in scope for #3777 —
/// this test pins it so the preservation is visible rather than accidental.
#[test]
fn nonfinite_float_renders_as_json_null_unchanged() {
    for v in [
        Value::Float32(f32::NAN),
        Value::Float32(f32::INFINITY),
        Value::Float32(f32::NEG_INFINITY),
        Value::Float(f64::NAN),
        Value::Float(f64::INFINITY),
        Value::Float(f64::NEG_INFINITY),
    ] {
        assert!(
            JSONWriter::value_to_json(&v).is_null(),
            "non-finite {v:?} must stay JSON null (declared divergence, not #3777)"
        );
    }
}

/// `Value::Float` is a CQL `double` and is ALREADY correct: no widening happens,
/// so its shortest f64 round-trip is exactly what `sstabledump` prints (measured
/// against `test_timeseries.sensor_data`'s `pressure DOUBLE`). Pinned so the
/// #3777 fix to the `Float32` arm cannot drift the `Float` one.
#[test]
fn float64_double_is_not_widened_and_keeps_its_shortest_decimal() {
    let cases: &[(f64, &str)] = &[
        // test_timeseries.sensor_data `pressure DOUBLE`, from the golden.
        (1017.9518806690071, "1017.9518806690071"),
        (1002.1829379523564, "1002.1829379523564"),
        (1.67, "1.67"),
        (0.1, "0.1"),
    ];
    for (f, expected) in cases {
        assert_eq!(json_text(&Value::Float(*f)), *expected);
    }
}

/// MEASUREMENT for the route decision (#3777): serde_json cannot fix this arm
/// from the outside. `Number` stores an `f64` unconditionally — `Number::from_f32`
/// itself is `N::Float(f as f64)` — so `serde_json::Value::from(1.67f32)` carries
/// the widened f64 and prints the widened spelling. The `float_roundtrip` feature
/// (enabled for this crate's dev-dependencies) only touches DESERIALIZATION
/// (`src/de.rs`, `src/value/de.rs`); nothing in `ser.rs`/`number.rs` reads it. So
/// only the streaming `Serializer::serialize_f32` path preserves f32 shortest
/// form, and this writer builds a `JsonValue`.
#[test]
fn serde_json_value_from_f32_still_widens_so_the_fix_must_be_local() {
    assert_eq!(
        serde_json::to_string(&serde_json::Value::from(1.67f32)).expect("serialize"),
        "1.6699999570846558",
        "if this ever changes, the local shortest-form conversion can be dropped"
    );
}
