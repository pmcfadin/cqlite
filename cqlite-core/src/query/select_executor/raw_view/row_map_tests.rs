//! Unit pins for the raw SSTable view's `CompactionRow` -> `QueryRow` mapper
//! — the `#[cfg(test)] mod tests` of `row_map.rs`, extracted VERBATIM to a
//! sibling file so the parent stays under the campsite-rule size threshold
//! (epic #1116 / #1135). Included via
//! `#[cfg(test)] #[path = "row_map_tests.rs"] mod tests;`, so `super` is still
//! the `row_map` module and every path resolves exactly as before.

use super::*;
use crate::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn};

fn schema() -> TableSchema {
    TableSchema {
        keyspace: "ks".to_string(),
        table: "t".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![
            ClusteringColumn {
                name: "ck1".to_string(),
                data_type: "int".to_string(),
                position: 0,
                order: ClusteringOrder::Asc,
            },
            ClusteringColumn {
                name: "ck2".to_string(),
                data_type: "text".to_string(),
                position: 1,
                order: ClusteringOrder::Asc,
            },
        ],
        columns: vec![
            Column {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "ck1".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "ck2".to_string(),
                data_type: "text".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "val".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: Default::default(),
        dropped_columns: Default::default(),
    }
}

fn pk_bytes(pk: i32) -> RowKey {
    RowKey::new(pk.to_be_bytes().to_vec())
}

fn source() -> RawViewSource {
    RawViewSource {
        sstable: "nb-1-big-Data.db".to_string(),
        generation: 1,
        format: "big",
        position: Some(42),
    }
}

/// A prefix-bound range tombstone (design.md D7 / spec: "the unspecified
/// component is NULL, not fabricated") becomes exactly two rows, start +
/// end, each carrying ONLY the clustering components the bound specifies.
#[test]
fn range_marker_becomes_two_bound_rows_with_prefix_clustering() {
    let row = CompactionRow {
        key: pk_bytes(1),
        row_timestamp: 0,
        row_data: CompactionRowData::RangeMarker {
            start: CompactionBound::Inclusive(vec![("ck1".to_string(), Value::Integer(2))]),
            end: CompactionBound::Inclusive(vec![("ck1".to_string(), Value::Integer(2))]),
            deletion_time: 1_700_000_000_000_000,
            local_deletion_time: 1_700_000_000,
        },
    };
    let rows = map_compaction_row(row, &schema(), &source()).expect("mapping must succeed");
    assert_eq!(rows.len(), 2, "a RangeMarker must become exactly 2 rows");

    let start = &rows[0];
    assert_eq!(
        start.values.get("row_kind").and_then(|v| match v {
            Value::Text(b) => Some(String::from_utf8_lossy(b).to_string()),
            _ => None,
        }),
        Some("range_tombstone_start".to_string())
    );
    assert_eq!(
        start.values.get("bound_inclusive"),
        Some(&Value::Boolean(true))
    );
    assert_eq!(start.values.get("ck1"), Some(&Value::Integer(2)));
    assert!(
        !start.values.contains_key("ck2"),
        "an unspecified clustering component must be ABSENT, never fabricated as NULL-or-zero"
    );
    assert_eq!(
        start.values.get("range_deletion_timestamp"),
        Some(&Value::BigInt(1_700_000_000_000_000))
    );
    assert_eq!(start.values.get("pk"), Some(&Value::Integer(1)));

    let end = &rows[1];
    assert_eq!(
        end.values.get("row_kind").and_then(|v| match v {
            Value::Text(b) => Some(String::from_utf8_lossy(b).to_string()),
            _ => None,
        }),
        Some("range_tombstone_end".to_string())
    );
}

/// Mixed open/closed inclusivity on the same range tombstone (spec
/// scenario, `test_deltas.range_tombstones` pk=3) is preserved per bound.
#[test]
fn range_marker_preserves_mixed_inclusivity_per_bound() {
    let row = CompactionRow {
        key: pk_bytes(3),
        row_timestamp: 0,
        row_data: CompactionRowData::RangeMarker {
            start: CompactionBound::Exclusive(vec![("ck1".to_string(), Value::Integer(1))]),
            end: CompactionBound::Inclusive(vec![("ck1".to_string(), Value::Integer(3))]),
            deletion_time: 1,
            local_deletion_time: 1,
        },
    };
    let rows = map_compaction_row(row, &schema(), &source()).expect("mapping must succeed");
    assert_eq!(
        rows[0].values.get("bound_inclusive"),
        Some(&Value::Boolean(false))
    );
    assert_eq!(
        rows[1].values.get("bound_inclusive"),
        Some(&Value::Boolean(true))
    );
}

/// A partition tombstone becomes exactly one row with every cell/clustering
/// column absent and both partition-deletion columns populated.
#[test]
fn partition_delete_becomes_one_row_with_no_cell_columns() {
    let row = CompactionRow {
        key: pk_bytes(7),
        row_timestamp: 0,
        row_data: CompactionRowData::PartitionDelete {
            deletion_time: 999,
            local_deletion_time: 5,
        },
    };
    let rows = map_compaction_row(row, &schema(), &source()).expect("mapping must succeed");
    assert_eq!(rows.len(), 1);
    let r = &rows[0];
    assert_eq!(
        r.values.get("partition_deletion_timestamp"),
        Some(&Value::BigInt(999))
    );
    assert_eq!(
        r.values.get("partition_deletion_time"),
        Some(&Value::BigInt(5))
    );
    assert!(!r.values.contains_key("ck1"));
    assert!(!r.values.contains_key("val"));
}

/// Roborev finding (issue #4222, round 7): a far-future
/// `local_deletion_time` — represented as a WRAPPED `i32` bit pattern of
/// a `u32` (`compaction_row.rs`'s convention, matching every other
/// widening site in the codebase) — must widen to `bigint` via
/// `i64::from(x as u32)`, never a bare `as i64`, which SIGN-EXTENDS the
/// wrapped bits into a fabricated large-negative epoch second.
/// `local_deletion_time: -1` represents `u32::MAX` (4294967295, a
/// genuinely far-future LDT); the buggy widening would render it as
/// `-1`.
#[test]
fn partition_delete_far_future_ldt_widens_without_sign_extension() {
    let row = CompactionRow {
        key: pk_bytes(7),
        row_timestamp: 0,
        row_data: CompactionRowData::PartitionDelete {
            deletion_time: 999,
            local_deletion_time: -1,
        },
    };
    let rows = map_compaction_row(row, &schema(), &source()).expect("mapping must succeed");
    assert_eq!(
        rows[0].values.get("partition_deletion_time"),
        Some(&Value::BigInt(4_294_967_295)),
        "a far-future LDT must widen to its TRUE u32 value, never sign-extend the \
         wrapped i32 bit pattern into a fabricated negative epoch second"
    );
}

/// A live cell tombstone reports its kind and NULLs the value; a live cell
/// carries its value plus the full metadata quad.
#[test]
fn live_row_reports_cell_tombstone_and_live_metadata() {
    let tombstoned = Value::Tombstone(Box::new(crate::types::TombstoneInfo {
        deletion_time: 55,
        tombstone_type: TombstoneType::CellTombstone,
        local_deletion_time: 66,
        ttl: None,
        range_start: None,
        range_end: None,
    }));
    let row = CompactionRow {
        key: pk_bytes(1),
        row_timestamp: 10,
        row_data: CompactionRowData::Live {
            simple: vec![
                SimpleCell {
                    column: "ck1".to_string(),
                    value: Value::Integer(1),
                    timestamp: 10,
                    ttl: None,
                    local_deletion_time: None,
                },
                SimpleCell {
                    column: "val".to_string(),
                    value: tombstoned,
                    timestamp: 10,
                    ttl: None,
                    local_deletion_time: None,
                },
            ],
            complex: vec![],
            row_deletion: None,
            row_liveness: Default::default(),
        },
    };
    let rows = map_compaction_row(row, &schema(), &source()).expect("mapping must succeed");
    assert_eq!(rows.len(), 1);
    let r = &rows[0];
    assert_eq!(r.values.get("val"), Some(&Value::Null));
    assert_eq!(
        r.values.get("val_tombstone").and_then(|v| match v {
            Value::Text(b) => Some(String::from_utf8_lossy(b).to_string()),
            _ => None,
        }),
        Some("cell".to_string())
    );
    assert_eq!(
        r.values.get("val_local_deletion_time"),
        Some(&Value::BigInt(66)),
        "val_local_deletion_time must be bigint (roborev finding, issue #4222 — round 8)"
    );
    // Clustering columns are plain data columns, never a metadata quad.
    assert_eq!(r.values.get("ck1"), Some(&Value::Integer(1)));
    assert!(!r.values.contains_key("ck1_timestamp"));
}

/// Roborev finding (issue #4222, round 8): `SimpleCell::local_deletion_time`
/// (a LIVE expiring cell, distinct from `TombstoneInfo`'s already-honest
/// `i64`) follows the SAME wrapped `as u32 as i32` convention as every
/// other LDT field in this module — `local_deletion_time: Some(-1)`
/// represents `u32::MAX` (4294967295, a genuinely far-future LDT).
#[test]
fn live_expiring_cell_far_future_ldt_widens_without_sign_extension() {
    let row = CompactionRow {
        key: pk_bytes(1),
        row_timestamp: 10,
        row_data: CompactionRowData::Live {
            simple: vec![SimpleCell {
                column: "val".to_string(),
                value: Value::text("x"),
                timestamp: 10,
                ttl: Some(60),
                local_deletion_time: Some(-1),
            }],
            complex: vec![],
            row_deletion: None,
            row_liveness: Default::default(),
        },
    };
    let rows = map_compaction_row(row, &schema(), &source()).expect("mapping must succeed");
    assert_eq!(
        rows[0].values.get("val_local_deletion_time"),
        Some(&Value::BigInt(4_294_967_295)),
        "a live expiring cell's far-future LDT must widen to its TRUE u32 value, never \
         sign-extend the wrapped i32 bit pattern"
    );
}

/// Roborev finding (issue #4222, round 8): `row_deletion`'s wrapped
/// `i32` LDT (the row-tombstone's own local-deletion-time — distinct
/// from `CompactionRowData::Tombstone`'s, already covered) must ALSO
/// widen without sign-extension.
#[test]
fn row_deletion_far_future_ldt_widens_without_sign_extension() {
    let row = CompactionRow {
        key: pk_bytes(1),
        row_timestamp: 10,
        row_data: CompactionRowData::Live {
            simple: vec![],
            complex: vec![],
            row_deletion: Some((99, -1)),
            row_liveness: Default::default(),
        },
    };
    let rows = map_compaction_row(row, &schema(), &source()).expect("mapping must succeed");
    assert_eq!(
        rows[0].values.get("row_local_deletion_time"),
        Some(&Value::BigInt(4_294_967_295)),
        "a row tombstone's far-future LDT must widen to its TRUE u32 value, never \
         sign-extend the wrapped i32 bit pattern"
    );
}

/// Roborev finding (issue #4222, round 8): `ComplexColumn::complex_deletion`'s
/// wrapped `i32` LDT element must ALSO widen without sign-extension.
#[test]
fn complex_deletion_far_future_ldt_widens_without_sign_extension() {
    let mut values: HashMap<String, Value> = HashMap::new();
    let col = ComplexColumn {
        column: "tags".to_string(),
        elements: vec![],
        collapsed_value: Value::Null,
        complex_deletion: Some((99, -1)),
    };
    insert_complex_column(&mut values, &col);
    assert_eq!(
        values.get("tags_complex_deletion_time"),
        Some(&Value::BigInt(4_294_967_295)),
        "a complex column's far-future LDT must widen to its TRUE u32 value, never \
         sign-extend the wrapped i32 bit pattern"
    );
}
