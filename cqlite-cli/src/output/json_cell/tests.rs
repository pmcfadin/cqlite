//! Unit tests for the JSON-egress rendering of one CQL value.
//!
//! Split out of `cqlite-cli/src/output/json.rs` with the code they cover (issue
//! #3644 item 3); the writer-level tests stayed there.

use super::*;
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
// Those cases are NOT here. They live in
// `cqlite-cli/tests/issue_3644_json_decimal_unquoted.rs`, an INTEGRATION target,
// because this file is a `--lib` unit module and `cqlite-cli`'s lib/bin unit
// tests execute in NO gate component and NO CI job
// (`scripts/tests/workspace-test-disposition.txt` records cqlite-cli as
// `PARTIAL / contradicts-doctrine`; the gate's `cli-tests` passes no `--lib`, and
// `.github/workflows/ci.yml` runs only `--test unit_tests`). The gate DOES derive
// its `--test` set from the `cqlite-cli/tests/*.rs` glob, so a case placed there
// enrols automatically.
//
// The cases above are in the same hole and are left where they are: they predate
// issue #3644 (they moved here with the code they cover) and moving them is not
// this change's subject. Closing that hole for the whole file is the
// disposition record's business, not this module's.
