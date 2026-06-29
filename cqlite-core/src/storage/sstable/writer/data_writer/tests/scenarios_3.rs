//! data_writer tests, group 3/6 (issue #1118 split).
//! Relocated verbatim from the original inline `mod tests`.

#![allow(unused_imports)]

use super::super::*;
use super::support::*;
use crate::schema::{ClusteringColumn, ClusteringOrder, Column, CqlType, KeyColumn, TableSchema};
use crate::storage::serialization::types::TypeSerializer;
use crate::storage::write_engine::mutation::{CellOperation, ClusteringKey, PartitionKey, TableId};
use crate::types::UdtValue;
use std::collections::HashMap;

/// Large static-column subsets use the same delta encoding as regular columns.
#[test]
fn test_column_subset_65_static_columns_uses_missing_indexes_when_present_majority() {
    let stats = create_test_stats();
    let writer = DataWriter::new(stats);

    // Create schema with 65 static columns
    let columns: Vec<Column> = (0..65)
        .map(|i| Column {
            name: format!("scol_{:03}", i),
            data_type: "text".to_string(),
            nullable: true,
            default: None,
            is_static: true,
        })
        .collect();

    let schema = TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns,
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let table_id = TableId::new("test_ks", "test_table");
    let pk = PartitionKey::single("id", Value::Integer(1));

    // Write all but one static column so the encoding emits missing indexes.
    let mut operations = Vec::new();
    for i in 0..65 {
        if i == 17 {
            continue;
        }
        operations.push(CellOperation::Write {
            column: format!("scol_{:03}", i),
            value: Value::Text(format!("value-{}", i)),
        });
    }

    let mutation = Mutation::new(table_id, pk, None, operations, 1001000, None);
    let static_ops: Vec<StaticMergedOp> = mutation
        .operations
        .iter()
        .map(|op| StaticMergedOp {
            op: op.clone(),
            timestamp_micros: mutation.timestamp_micros,
            cell_local_deletion_time: mutation.effective_local_deletion_time(),
            row_ttl_seconds: mutation.ttl_seconds,
        })
        .collect();

    let mut buf = Vec::new();
    writer
        .write_static_column_bitmap(&mut buf, &static_ops, &schema)
        .unwrap();

    // missing_count=1, followed by the missing column index.
    assert_eq!(buf, vec![1, 17]);
}

/// Smaller subsets still use the missing-column bitmap.
#[test]
fn test_column_subset_under_64_regular_columns_uses_bitmap() {
    let stats = create_test_stats();
    let writer = DataWriter::new(stats);

    let columns: Vec<Column> = (0..4)
        .map(|i| Column {
            name: format!("col_{i}"),
            data_type: "text".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        })
        .collect();

    let schema = TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns,
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let table_id = TableId::new("test_ks", "test_table");
    let pk = PartitionKey::single("id", Value::Integer(1));

    // Only col_1 is present, so bits 0, 2, and 3 are set.
    let mutation = Mutation::new(
        table_id,
        pk,
        None,
        vec![CellOperation::Write {
            column: "col_1".to_string(),
            value: Value::Text("present".to_string()),
        }],
        1001000,
        None,
    );

    let mut buf = Vec::new();
    writer
        .write_column_bitmap(&mut buf, &mutation, &schema)
        .unwrap();

    assert_eq!(buf, vec![0b1101]);
}

#[test]
fn test_regular_columns_sort_simple_before_complex() {
    let stats = create_test_stats();
    let writer = DataWriter::new(stats);

    let schema = TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "z_simple".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "a_complex".to_string(),
                data_type: "set<text>".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "m_simple".to_string(),
                data_type: "int".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let ordered = writer.regular_columns(&schema);
    let names: Vec<_> = ordered.iter().map(|column| column.name.as_str()).collect();

    assert_eq!(names, vec!["m_simple", "z_simple", "a_complex"]);
}

#[test]
fn test_static_columns_sort_simple_before_complex() {
    let stats = create_test_stats();
    let writer = DataWriter::new(stats);

    let schema = TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: vec![
            Column {
                name: "z_static_simple".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: true,
            },
            Column {
                name: "a_static_complex".to_string(),
                data_type: "set<text>".to_string(),
                nullable: true,
                default: None,
                is_static: true,
            },
            Column {
                name: "m_static_simple".to_string(),
                data_type: "int".to_string(),
                nullable: true,
                default: None,
                is_static: true,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let ordered = writer.static_columns(&schema);
    let names: Vec<_> = ordered.iter().map(|column| column.name.as_str()).collect();

    assert_eq!(
        names,
        vec!["m_static_simple", "z_static_simple", "a_static_complex"]
    );
}

/// Build a one-static-column schema: `id` (pk) / `ck` (clustering) / `s` (static text).
fn single_static_column_schema() -> TableSchema {
    TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: vec![Column {
            name: "s".to_string(),
            data_type: "text".to_string(),
            nullable: true,
            default: None,
            is_static: true,
        }],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// Regression guard for the #1196 TTL regression (PR #1211, roborev HIGH).
///
/// A static column written with statement-level `USING TTL` arrives as a plain
/// `CellOperation::Write` with the TTL in `Mutation::ttl_seconds`. Cassandra
/// encodes that TTL on the static CELL (an expiring cell), never as row-level
/// liveness on the static block (#1196: the static row keeps flags `0xa0`, no
/// `ROW_HAS_TTL`). The original #1196 fix routed EVERY static `Write` through the
/// non-TTL `write_cell_explicit_ts`, silently DROPPING the statement TTL so the
/// cell lived forever — a data-liveness correctness regression.
///
/// This asserts the static `Write` + `row_ttl_seconds` now serializes as an
/// EXPIRING cell (`CELL_IS_EXPIRING` set). Fails before the routing fix (cell
/// flags `0x00`), passes after (`0x02`). BYTE-level static-cell TTL parity vs
/// Cassandra is the follow-up byte-golden (#1210); this is the functional guard.
#[test]
fn static_using_ttl_write_emits_expiring_cell_1196() {
    let mut stats = create_test_stats();
    stats.min_timestamp = 1_000_000;
    stats.min_local_deletion_time = 0;
    stats.min_ttl = 0;
    let writer = DataWriter::new(stats);
    let schema = single_static_column_schema();

    // Statement-level TTL: ttl_seconds carried on the Mutation, op is a plain
    // Write (this is exactly how the CQL builders shape `UPDATE ... USING TTL`).
    let ttl = 3_600u32;
    let static_op = StaticMergedOp {
        op: CellOperation::Write {
            column: "s".to_string(),
            value: Value::Text("v".to_string()),
        },
        timestamp_micros: 1_001_000,
        cell_local_deletion_time: 0,
        row_ttl_seconds: Some(ttl),
    };

    let mut buf = Vec::new();
    let cells = writer
        .write_static_cells(
            &mut buf,
            std::slice::from_ref(&static_op),
            1_001_000,
            &schema,
        )
        .unwrap();

    assert_eq!(cells, 1, "exactly one static cell written");
    assert!(!buf.is_empty(), "static cell must produce bytes");
    // First byte is the cell flags. The expiring-cell encoding sets
    // CELL_IS_EXPIRING and must NOT borrow row timestamp/TTL (the static block
    // has none).
    let flags = buf[0];
    assert_eq!(
        flags & CELL_IS_EXPIRING,
        CELL_IS_EXPIRING,
        "#1196 regression: static USING TTL write must be an EXPIRING cell, \
         not a non-expiring (lives-forever) cell (flags={flags:#04x})"
    );
    assert_eq!(
        flags & CELL_USE_ROW_TIMESTAMP,
        0,
        "#1196: static cell must carry its own timestamp, not USE_ROW_TIMESTAMP"
    );
}

/// Companion guard: a static `Write` with NO TTL stays a plain non-expiring
/// cell with its own explicit timestamp (flags `0x00`) — the #1196 fix that
/// removed the spurious row-level static HAS_TIMESTAMP must remain intact. This
/// is the unit-level mirror of the byte-parity test
/// `static_row_timestamp_flags_gap_pinned_1196` (which pins the `0xa0` block).
#[test]
fn static_write_without_ttl_stays_non_expiring_1196() {
    let mut stats = create_test_stats();
    stats.min_timestamp = 1_000_000;
    let writer = DataWriter::new(stats);
    let schema = single_static_column_schema();

    let static_op = StaticMergedOp {
        op: CellOperation::Write {
            column: "s".to_string(),
            value: Value::Text("v".to_string()),
        },
        timestamp_micros: 1_001_000,
        cell_local_deletion_time: 0,
        row_ttl_seconds: None,
    };

    let mut buf = Vec::new();
    writer
        .write_static_cells(
            &mut buf,
            std::slice::from_ref(&static_op),
            1_001_000,
            &schema,
        )
        .unwrap();

    let flags = buf[0];
    assert_eq!(
        flags & CELL_IS_EXPIRING,
        0,
        "#1196: a no-TTL static write must NOT be expiring (flags={flags:#04x})"
    );
    assert_eq!(
        flags & CELL_USE_ROW_TIMESTAMP,
        0,
        "#1196: static cell carries its own explicit timestamp (no row liveness)"
    );
}

/// Schema with two static columns (`s1`, `s2`) plus a clustering column, for the
/// per-cell static-writetime test below.
fn two_static_column_schema() -> TableSchema {
    TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: vec![
            Column {
                name: "s1".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: true,
            },
            Column {
                name: "s2".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: true,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// Issue #1018 (roborev finding 2): a compacted STATIC row whose surviving static
/// cells were last written at DIFFERENT writetimes must keep each cell's own
/// timestamp. The compaction merge→mutation path records those per-cell writetimes
/// in `Mutation::cell_write_timestamps`; `collect_static_operations` must thread
/// each into the `StaticMergedOp.timestamp_micros` (and use it for the LWW
/// comparison), mirroring the regular-row path. Before the fix, every static cell
/// inherited the row-level `timestamp_micros` (the row max), so an older static
/// sibling was rewritten to a higher writetime — the same bug class the
/// regular-row fix addressed.
#[test]
fn collect_static_operations_preserves_per_cell_writetimes_1018() {
    let schema = two_static_column_schema();
    let table_id = TableId::new("test_ks", "test_table");
    let pk = PartitionKey::single("id", Value::Integer(1));

    // Row marker timestamp is the MAX of the two static cells (5000). The cell `s1`
    // genuinely survives from an OLDER write at 3000; only `s2` was written at 5000.
    let row_ts = 5000i64;
    let s1_ts = 3000i64;
    let mut cell_ts = HashMap::new();
    cell_ts.insert("s1".to_string(), s1_ts);
    cell_ts.insert("s2".to_string(), row_ts);

    let mut mutation = Mutation::new(
        table_id,
        pk,
        None,
        vec![
            CellOperation::Write {
                column: "s1".to_string(),
                value: Value::Text("old".to_string()),
            },
            CellOperation::Write {
                column: "s2".to_string(),
                value: Value::Text("new".to_string()),
            },
        ],
        row_ts,
        None,
    );
    mutation.cell_write_timestamps = Some(cell_ts);

    let ops = collect_static_operations(std::slice::from_ref(&mutation), &schema, None);

    let find_ts = |col: &str| {
        ops.iter()
            .find(|o| matches!(&o.op, CellOperation::Write { column, .. } if column == col))
            .map(|o| o.timestamp_micros)
    };

    assert_eq!(
        find_ts("s1"),
        Some(s1_ts),
        "#1018: surviving older static cell must keep its OWN writetime, not the row max"
    );
    assert_eq!(
        find_ts("s2"),
        Some(row_ts),
        "#1018: newest static cell keeps its own (row-max) writetime"
    );
}

/// Schema with one clustering key and two REGULAR (non-static) text columns, used
/// by the #1018 per-cell shadow tests for `merge_row_group`.
fn two_regular_column_schema() -> TableSchema {
    TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: vec![
            Column {
                name: "c1".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "c2".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// Issue #1018 (roborev HIGH — data resurrection past a tombstone): the per-cell
/// write-timestamp work made each emitted simple `Write`/`WriteWithTtl` cell use
/// its OWN `cell_write_timestamp(col)` instead of the row max, but tombstone
/// SHADOWING in `merge_row_group` still gated by the row max (`shadow_floor` vs
/// `m.timestamp_micros`). So a reconciled row whose row max is ABOVE a tombstone
/// floor (a recent sibling keeps it live) could still emit another simple cell
/// whose own per-cell timestamp is `<= floor` — resurrecting data the tombstone
/// should shadow. The fix applies the `<= deletion_ts` shadow floor PER CELL using
/// the cell's resolved per-cell timestamp.
///
/// This test FAILS before the fix (the low-ts cell survives) and passes after.
#[test]
fn merge_row_group_shadows_low_per_cell_ts_simple_cell_1018() {
    let schema = two_regular_column_schema();
    let table_id = TableId::new("test_ks", "test_table");
    let pk = PartitionKey::single("id", Value::Integer(1));
    let ck = ClusteringKey::single("ck", Value::Integer(1));

    // Partition/range tombstone floor at 4000. The row's max writetime is 5000
    // (the `c2` sibling, written AFTER the tombstone) so the mutation as a whole
    // survives the floor. `c1`'s OWN per-cell writetime is 3000 — BELOW the floor —
    // so it is covered by the tombstone and must be shadowed.
    let shadow_floor = Some(4000i64);
    let row_ts = 5000i64;
    let c1_ts = 3000i64;
    let mut cell_ts = HashMap::new();
    cell_ts.insert("c1".to_string(), c1_ts);
    cell_ts.insert("c2".to_string(), row_ts);

    let mut mutation = Mutation::new(
        table_id,
        pk,
        Some(ck),
        vec![
            CellOperation::Write {
                column: "c1".to_string(),
                value: Value::Text("shadowed".to_string()),
            },
            CellOperation::Write {
                column: "c2".to_string(),
                value: Value::Text("survivor".to_string()),
            },
        ],
        row_ts,
        None,
    );
    mutation.cell_write_timestamps = Some(cell_ts);

    let row = DataWriter::merge_row_group(&[&mutation], &schema, false, shadow_floor)
        .expect("row with a surviving sibling must be emitted");

    let surviving: Vec<&str> = row
        .ops
        .iter()
        .filter_map(|mop| match mop.op {
            CellOperation::Write { column, .. } => Some(column.as_str()),
            _ => None,
        })
        .collect();

    assert!(
        surviving.contains(&"c2"),
        "#1018: the recent sibling (per-cell ts > floor) must survive"
    );
    assert!(
        !surviving.contains(&"c1"),
        "#1018: a simple cell whose per-cell ts ({c1_ts}) is <= the tombstone floor (4000) \
         must be SHADOWED, not resurrected"
    );
}

/// Issue #1018 static analogue: `collect_static_operations` gates a mutation by
/// the row max (`shadow_floor` vs `mutation.timestamp_micros`). A static row whose
/// row max is ABOVE a partition tombstone floor (a recent static sibling survives)
/// could still keep another static cell whose OWN per-cell timestamp is `<= floor`,
/// resurrecting data the partition tombstone covers. The fix applies the floor
/// per-cell to the resolved per-cell timestamp.
///
/// FAILS before the fix (the low-ts static cell survives), passes after.
#[test]
fn collect_static_operations_shadows_low_per_cell_ts_1018() {
    let schema = two_static_column_schema();
    let table_id = TableId::new("test_ks", "test_table");
    let pk = PartitionKey::single("id", Value::Integer(1));

    // Partition tombstone floor at 4000. Row max is 5000 (`s2`, written after the
    // tombstone) so the mutation survives the floor. `s1`'s OWN per-cell writetime
    // is 3000 (BELOW the floor), so it is shadowed by the partition tombstone.
    let shadow_floor = Some(4000i64);
    let row_ts = 5000i64;
    let s1_ts = 3000i64;
    let mut cell_ts = HashMap::new();
    cell_ts.insert("s1".to_string(), s1_ts);
    cell_ts.insert("s2".to_string(), row_ts);

    let mut mutation = Mutation::new(
        table_id,
        pk,
        None,
        vec![
            CellOperation::Write {
                column: "s1".to_string(),
                value: Value::Text("shadowed".to_string()),
            },
            CellOperation::Write {
                column: "s2".to_string(),
                value: Value::Text("survivor".to_string()),
            },
        ],
        row_ts,
        None,
    );
    mutation.cell_write_timestamps = Some(cell_ts);

    let ops = collect_static_operations(std::slice::from_ref(&mutation), &schema, shadow_floor);

    let surviving: Vec<&str> = ops
        .iter()
        .filter_map(|o| match &o.op {
            CellOperation::Write { column, .. } => Some(column.as_str()),
            _ => None,
        })
        .collect();

    assert!(
        surviving.contains(&"s2"),
        "#1018: the recent static sibling (per-cell ts > floor) must survive"
    );
    assert!(
        !surviving.contains(&"s1"),
        "#1018: a static cell whose per-cell ts ({s1_ts}) is <= the partition floor (4000) \
         must be SHADOWED, not resurrected"
    );
}

#[test]
fn test_write_column_bitmap_zero_when_all_columns_present() {
    let stats = create_test_stats();
    let writer = DataWriter::new(stats);

    let columns: Vec<Column> = (0..65)
        .map(|i| Column {
            name: format!("col_{:03}", i),
            data_type: "text".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        })
        .collect();

    let schema = TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns,
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let table_id = TableId::new("test_ks", "test_table");
    let pk = PartitionKey::single("id", Value::Integer(1));

    let operations: Vec<_> = (0..65)
        .map(|i| CellOperation::Write {
            column: format!("col_{:03}", i),
            value: Value::Text(format!("value-{}", i)),
        })
        .collect();

    let mutation = Mutation::new(table_id, pk, None, operations, 1001000, None);

    let mut buf = Vec::new();
    writer
        .write_column_bitmap(&mut buf, &mutation, &schema)
        .unwrap();

    assert_eq!(buf, vec![0]);
}

#[test]
fn test_serialize_list() {
    let list = Value::List(vec![
        Value::Integer(1),
        Value::Integer(2),
        Value::Integer(3),
    ]);
    let bytes = serialize_value(&list).unwrap();
    // 4 bytes count + 3 * (4 bytes len + 4 bytes i32)
    assert_eq!(bytes.len(), 4 + 3 * 8);
    // Count = 3
    assert_eq!(&bytes[0..4], &3i32.to_be_bytes());
    // First element length = 4
    assert_eq!(&bytes[4..8], &4i32.to_be_bytes());
    // First element value = 1
    assert_eq!(&bytes[8..12], &1i32.to_be_bytes());
}

#[test]
fn test_serialize_empty_list() {
    let list = Value::List(vec![]);
    let bytes = serialize_value(&list).unwrap();
    assert_eq!(bytes.len(), 4);
    assert_eq!(&bytes[0..4], &0i32.to_be_bytes());
}

#[test]
fn test_serialize_single_element_list() {
    let list = Value::List(vec![Value::Integer(42)]);
    let bytes = serialize_value(&list).unwrap();
    assert_eq!(
        bytes,
        vec![
            0x00, 0x00, 0x00, 0x01, // count = 1
            0x00, 0x00, 0x00, 0x04, // len = 4
            0x00, 0x00, 0x00, 0x2A, // value = 42
        ]
    );
}

#[test]
fn test_serialize_set() {
    let set = Value::Set(vec![
        Value::Text("alpha".to_string()),
        Value::Text("beta".to_string()),
    ]);
    let bytes = serialize_value(&set).unwrap();
    // Count = 2
    assert_eq!(&bytes[0..4], &2i32.to_be_bytes());
    // First element length = 5 ("alpha")
    assert_eq!(&bytes[4..8], &5i32.to_be_bytes());
    assert_eq!(&bytes[8..13], b"alpha");
}

#[test]
fn test_serialize_single_element_set() {
    let set = Value::Set(vec![Value::Text("alpha".to_string())]);
    let bytes = serialize_value(&set).unwrap();
    assert_eq!(
        bytes,
        vec![
            0x00, 0x00, 0x00, 0x01, // count = 1
            0x00, 0x00, 0x00, 0x05, // len = 5
            b'a', b'l', b'p', b'h', b'a', // value = "alpha"
        ]
    );
}

#[test]
fn test_serialize_empty_set() {
    let set = Value::Set(vec![]);
    let bytes = serialize_value(&set).unwrap();
    assert_eq!(bytes, 0i32.to_be_bytes().to_vec());
}

#[test]
fn test_serialize_map() {
    let map = Value::Map(vec![(Value::Text("key1".to_string()), Value::Integer(100))]);
    let bytes = serialize_value(&map).unwrap();
    // Count = 1
    assert_eq!(&bytes[0..4], &1i32.to_be_bytes());
    // Key length = 4 ("key1")
    assert_eq!(&bytes[4..8], &4i32.to_be_bytes());
    assert_eq!(&bytes[8..12], b"key1");
    // Value length = 4 (i32)
    assert_eq!(&bytes[12..16], &4i32.to_be_bytes());
    // Value = 100
    assert_eq!(&bytes[16..20], &100i32.to_be_bytes());
}

#[test]
fn test_serialize_empty_map() {
    let map = Value::Map(vec![]);
    let bytes = serialize_value(&map).unwrap();
    assert_eq!(bytes.len(), 4);
    assert_eq!(&bytes[0..4], &0i32.to_be_bytes());
}

#[test]
fn test_serialize_tuple() {
    let tuple = Value::Tuple(vec![
        Value::Integer(42),
        Value::Text("hello".to_string()),
        Value::Null,
    ]);
    let bytes = serialize_value(&tuple).unwrap();
    // Field 1: 4 bytes len + 4 bytes i32 = 8
    assert_eq!(&bytes[0..4], &4i32.to_be_bytes());
    assert_eq!(&bytes[4..8], &42i32.to_be_bytes());
    // Field 2: 4 bytes len + 5 bytes text = 9
    assert_eq!(&bytes[8..12], &5i32.to_be_bytes());
    assert_eq!(&bytes[12..17], b"hello");
    // Field 3: NULL = -1 as i32
    assert_eq!(&bytes[17..21], &(-1i32).to_be_bytes());
}

#[test]
fn test_serialize_single_element_tuple() {
    let tuple = Value::Tuple(vec![Value::Text("solo".to_string())]);
    let bytes = serialize_value(&tuple).unwrap();
    assert_eq!(
        bytes,
        vec![
            0x00, 0x00, 0x00, 0x04, // len = 4
            b's', b'o', b'l', b'o', // value = "solo"
        ]
    );
}

#[test]
fn test_serialize_frozen() {
    let frozen = Value::Frozen(Box::new(Value::List(vec![
        Value::Integer(10),
        Value::Integer(20),
    ])));
    let frozen_bytes = serialize_value(&frozen).unwrap();
    let list_bytes =
        serialize_value(&Value::List(vec![Value::Integer(10), Value::Integer(20)])).unwrap();
    // Frozen should produce identical bytes to inner value
    assert_eq!(frozen_bytes, list_bytes);
}

#[test]
fn test_serialize_single_element_frozen() {
    let frozen = Value::Frozen(Box::new(Value::List(vec![Value::Text("solo".to_string())])));
    let frozen_bytes = serialize_value(&frozen).unwrap();
    let list_bytes = serialize_value(&Value::List(vec![Value::Text("solo".to_string())])).unwrap();
    assert_eq!(frozen_bytes, list_bytes);
}

#[test]
fn test_serialize_nested_collection() {
    // MAP<TEXT, FROZEN<LIST<INT>>>
    let nested = Value::Map(vec![(
        Value::Text("nums".to_string()),
        Value::Frozen(Box::new(Value::List(vec![
            Value::Integer(1),
            Value::Integer(2),
        ]))),
    )]);
    let bytes = serialize_value(&nested).unwrap();
    // Should not error - validates nested serialization works
    assert!(!bytes.is_empty());
    // Count = 1
    assert_eq!(&bytes[0..4], &1i32.to_be_bytes());
}

#[test]
fn test_serialize_udt_with_nested_collections_matches_schema_aware_bytes() {
    let serializer = TypeSerializer::new();
    let company = phase3_company_value();

    let bytes = serialize_value(&Value::Udt(company.clone())).unwrap();
    let expected = serializer
        .serialize_udt(&Value::Udt(company), &phase3_company_schema())
        .unwrap();

    assert_eq!(bytes, expected);
}

#[test]
fn test_serialize_collection_containing_nested_udts() {
    let serializer = TypeSerializer::new();
    let company = phase3_company_value();
    let company_bytes = serializer
        .serialize_udt(&Value::Udt(company.clone()), &phase3_company_schema())
        .unwrap();

    let value = Value::Map(vec![(
        Value::Text("empresa_日本".to_string()),
        Value::Frozen(Box::new(Value::Udt(company))),
    )]);
    let bytes = serialize_value(&value).unwrap();

    let key = "empresa_日本".as_bytes();
    let mut expected = Vec::new();
    expected.extend_from_slice(&1i32.to_be_bytes());
    expected.extend_from_slice(&(key.len() as i32).to_be_bytes());
    expected.extend_from_slice(key);
    expected.extend_from_slice(&(company_bytes.len() as i32).to_be_bytes());
    expected.extend_from_slice(&company_bytes);

    assert_eq!(bytes, expected);
}

#[test]
fn test_serialize_tuple_with_collection_fields_and_udt() {
    let serializer = TypeSerializer::new();
    let address = phase3_address_value();
    let person = phase3_person_value("Tuple User");
    let address_bytes = serializer
        .serialize_udt(&Value::Udt(address.clone()), &phase3_address_schema())
        .unwrap();
    let person_bytes = serializer
        .serialize_udt(&Value::Udt(person.clone()), &phase3_person_schema())
        .unwrap();

    let tuple = Value::Tuple(vec![
        Value::Text("phase3".to_string()),
        Value::Frozen(Box::new(Value::List(vec![
            Value::Integer(3),
            Value::Integer(5),
            Value::Integer(8),
        ]))),
        Value::Frozen(Box::new(Value::Map(vec![(
            Value::Text("home".to_string()),
            Value::Frozen(Box::new(Value::Udt(address))),
        )]))),
        Value::Frozen(Box::new(Value::Udt(person))),
    ]);
    let bytes = serialize_value(&tuple).unwrap();

    let list_bytes = serialize_value(&Value::List(vec![
        Value::Integer(3),
        Value::Integer(5),
        Value::Integer(8),
    ]))
    .unwrap();
    let map_bytes = {
        let key = b"home";
        let mut encoded = Vec::new();
        encoded.extend_from_slice(&1i32.to_be_bytes());
        encoded.extend_from_slice(&(key.len() as i32).to_be_bytes());
        encoded.extend_from_slice(key);
        encoded.extend_from_slice(&(address_bytes.len() as i32).to_be_bytes());
        encoded.extend_from_slice(&address_bytes);
        encoded
    };

    let mut expected = Vec::new();
    expected.extend_from_slice(&6i32.to_be_bytes());
    expected.extend_from_slice(b"phase3");
    expected.extend_from_slice(&(list_bytes.len() as i32).to_be_bytes());
    expected.extend_from_slice(&list_bytes);
    expected.extend_from_slice(&(map_bytes.len() as i32).to_be_bytes());
    expected.extend_from_slice(&map_bytes);
    expected.extend_from_slice(&(person_bytes.len() as i32).to_be_bytes());
    expected.extend_from_slice(&person_bytes);

    assert_eq!(bytes, expected);
}

#[test]
fn test_serialize_high_complexity_nested_collection() {
    let nested = Value::Map(vec![(
        Value::Text("outer".to_string()),
        Value::Frozen(Box::new(Value::List(vec![Value::Frozen(Box::new(
            Value::Map(vec![(
                Value::Text("inner".to_string()),
                Value::Frozen(Box::new(Value::List(vec![
                    Value::Integer(1),
                    Value::Integer(2),
                ]))),
            )]),
        ))]))),
    )]);

    let bytes = serialize_value(&nested).unwrap();

    assert!(!bytes.is_empty());
    assert_eq!(&bytes[0..4], &1i32.to_be_bytes());
}
