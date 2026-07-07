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

/// BYTE-level static-cell TTL parity for issue #1210 (follow-up to the #1196
/// functional guard `static_using_ttl_write_emits_expiring_cell_1196`).
///
/// Ground truth (Cassandra 5.0 `BufferCell` / `Cell.Serializer.serialize`): a
/// static column written with statement-level `USING TTL` is an EXPIRING cell —
/// the TTL rides on the CELL (cell-level `CELL_IS_EXPIRING` flag, an unsigned
/// VInt `ttl` delta from `min_ttl`, and an unsigned VInt `localDeletionTime`
/// delta = `now + ttl` from `min_local_deletion_time`), NEVER as row-level
/// liveness/`ROW_HAS_TTL` on the static block. The cell carries its OWN explicit
/// timestamp (no `CELL_USE_ROW_TIMESTAMP`/`CELL_USE_ROW_TTL`), because a static
/// block emits no row liveness to borrow (#1196).
///
/// This decodes the FULL emitted cell wire-format and pins:
///   * flags = `CELL_IS_EXPIRING`, no row-timestamp/row-ttl borrowing,
///   * the on-wire `ttl` == the requested TTL (via `min_ttl == 0`),
///   * the on-wire `localDeletionTime` is consistent with `now + ttl`,
///   * the cell's explicit timestamp delta == `timestamp - min_timestamp`.
///
/// It also cross-checks byte-equivalence against the per-cell
/// `CellOperation::WriteWithTtl` reference path: a row-level `USING TTL` static
/// `Write` and an equivalent per-cell `WriteWithTtl` must produce equivalent
/// cell encodings (identical except for the wall-clock LDT, which is bounded).
///
/// Fail-before (pre-#1196 routing): the static `Write` was serialized via the
/// non-TTL `write_cell_explicit_ts`, so byte 0 would be `0x00` (non-expiring,
/// lives forever) and there would be no ttl/LDT fields — the decode below would
/// not find `CELL_IS_EXPIRING`. Pass-after: the cell is expiring with the TTL.
#[test]
fn static_using_ttl_write_byte_parity_1210() {
    let min_timestamp = 1_000_000i64;
    let mut stats = create_test_stats();
    stats.min_timestamp = min_timestamp;
    stats.min_local_deletion_time = 0; // so LDT delta == absolute localDeletionTime
    stats.min_ttl = 0; // so ttl delta == absolute ttl
    let writer = DataWriter::new(stats);
    let schema = single_static_column_schema();

    let ttl = 3_600u32;
    let timestamp = 1_001_000i64;
    let value = Value::Text("v".to_string());

    // Capture a tight wall-clock window around the write for the LDT bound.
    let before = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    // Path under test: row-level USING TTL arrives as a plain `Write` carrying
    // the statement TTL in `row_ttl_seconds` (how the CQL builders shape it).
    let row_ttl_op = StaticMergedOp {
        op: CellOperation::Write {
            column: "s".to_string(),
            value: value.clone(),
        },
        timestamp_micros: timestamp,
        cell_local_deletion_time: 0,
        row_ttl_seconds: Some(ttl),
    };
    let mut row_ttl_buf = Vec::new();
    let cells = writer
        .write_static_cells(
            &mut row_ttl_buf,
            std::slice::from_ref(&row_ttl_op),
            timestamp,
            &schema,
        )
        .unwrap();

    assert_eq!(cells, 1, "exactly one static cell written");

    // Decode the full cell wire format:
    //   [flags: u8][ts_delta: uvint][ldt_delta: uvint][ttl_delta: uvint]
    //   [value_len: uvint][value bytes]   (text is length-prefixed)
    let mut pos = 0usize;
    let flags = row_ttl_buf[pos];
    pos += 1;

    assert_eq!(
        flags & CELL_IS_EXPIRING,
        CELL_IS_EXPIRING,
        "#1210: static USING TTL write must be an EXPIRING cell (flags={flags:#04x})"
    );
    assert_eq!(
        flags & CELL_USE_ROW_TIMESTAMP,
        0,
        "#1210: static cell must carry its own timestamp, not USE_ROW_TIMESTAMP"
    );
    assert_eq!(
        flags & CELL_USE_ROW_TTL,
        0,
        "#1210: static cell must carry its own TTL, not USE_ROW_TTL"
    );

    let ts_delta = read_uvint_at(&row_ttl_buf, &mut pos);
    assert_eq!(
        ts_delta as i64,
        timestamp - min_timestamp,
        "#1210: cell timestamp delta must be (timestamp - min_timestamp)"
    );

    // min_local_deletion_time == 0, so the decoded delta IS the absolute
    // localDeletionTime; it must equal `now + ttl`. The bound is checked below
    // against a single `[before, after]` window captured around BOTH writes.
    let ldt = read_uvint_at(&row_ttl_buf, &mut pos) as i64;

    // min_ttl == 0, so the decoded delta IS the absolute ttl.
    let ttl_on_wire = read_uvint_at(&row_ttl_buf, &mut pos);
    assert_eq!(
        ttl_on_wire, ttl as u64,
        "#1210: on-wire ttl must equal the statement TTL"
    );

    // Cross-check: an equivalent per-cell `WriteWithTtl` static cell (the
    // reference TTL path) must produce equivalent cell encoding. The only
    // permitted difference is the wall-clock LDT field, so compare the prefix
    // up to and including the flags + ts_delta + ttl, and assert the LDT of the
    // reference cell falls in the same bound.
    let ref_op = StaticMergedOp {
        op: CellOperation::WriteWithTtl {
            column: "s".to_string(),
            value: value.clone(),
            ttl_seconds: ttl,
            local_deletion_time: None,
        },
        timestamp_micros: timestamp,
        cell_local_deletion_time: 0,
        row_ttl_seconds: None,
    };
    let mut ref_buf = Vec::new();
    writer
        .write_static_cells(
            &mut ref_buf,
            std::slice::from_ref(&ref_op),
            timestamp,
            &schema,
        )
        .unwrap();

    // Close the wall-clock window only after BOTH writes have run, so each
    // cell's LDT (now + ttl, sampled at its own write) is covered by the same
    // `[before, after]` bound regardless of one-second boundary crossings.
    let after = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    // Both cells' localDeletionTime must be now + ttl within the shared window.
    let lower = before + ttl as i64;
    let upper = after + ttl as i64;
    assert!(
        (lower..=upper).contains(&ldt),
        "#1210: localDeletionTime ({ldt}) must be now + ttl \
         (expected in [{lower}, {upper}])"
    );

    // Flags + timestamp delta must be byte-identical between the two paths.
    let mut rpos = 0usize;
    assert_eq!(
        ref_buf[rpos], flags,
        "#1210: WriteWithTtl flags must match row-TTL Write"
    );
    rpos += 1;
    let ref_ts_delta = read_uvint_at(&ref_buf, &mut rpos);
    assert_eq!(ref_ts_delta, ts_delta, "#1210: timestamp deltas must match");
    let ref_ldt = read_uvint_at(&ref_buf, &mut rpos) as i64;
    assert!(
        (lower..=upper).contains(&ref_ldt),
        "#1210: WriteWithTtl LDT must also be now + ttl"
    );
    let ref_ttl = read_uvint_at(&ref_buf, &mut rpos);
    assert_eq!(
        ref_ttl, ttl_on_wire,
        "#1210: on-wire ttl must match the reference path"
    );

    // The value tail (length-prefix + bytes) must be byte-identical.
    assert_eq!(
        &row_ttl_buf[pos..],
        &ref_buf[rpos..],
        "#1210: value encoding must match the WriteWithTtl reference path"
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

/// Read one unsigned VInt (Cassandra encoding) from `buf` at `pos`.
fn read_uvint_at(buf: &[u8], pos: &mut usize) -> u64 {
    let first = buf[*pos];
    *pos += 1;
    if first == 0xFF {
        let mut v = 0u64;
        for _ in 0..8 {
            v = (v << 8) | buf[*pos] as u64;
            *pos += 1;
        }
        return v;
    }
    let extra = first.leading_ones() as usize;
    let mask = 0xFF_u8.wrapping_shr((extra + 1) as u32);
    let mut v = (first & mask) as u64;
    for _ in 0..extra {
        v = (v << 8) | buf[*pos] as u64;
        *pos += 1;
    }
    v
}

/// Issue #1018 (roborev Medium): the PUBLIC `DataWriter::write_static_row` entry
/// point was the last writer path that ignored the per-cell write-timestamp
/// side-channel — it stamped EVERY static op with `mutation.timestamp_micros`
/// (the row max). A caller passing a compacted/static mutation that carries
/// per-cell overrides in `Mutation::cell_write_timestamps` would therefore
/// rewrite older surviving static cells (live OR cell tombstones) up to the row
/// max, the same over-deletion bug class already fixed in
/// `collect_static_operations`.
///
/// This drives `write_static_row` END-TO-END (not the lower-level
/// `write_static_cells`) and decodes the emitted static-cell timestamp deltas,
/// asserting each static cell keeps its OWN per-cell writetime — and a static
/// cell tombstone keeps its OWN `markedForDeleteAt` — rather than the row max.
///
/// Pre-fix: `write_static_row` did not consult `cell_write_timestamps`, so the
/// `s1` cell would be stamped at the row max (5000) instead of its own 3000 —
/// this test fails. Post-fix (resolving via `op_cell_write_timestamp`, exactly
/// as `collect_static_operations` does) it passes. The no-override case is
/// covered by the existing `static_write_without_ttl_stays_non_expiring_1196`
/// (single writetime, byte-identical).
#[test]
fn write_static_row_preserves_per_cell_writetimes_1018() {
    let mut stats = create_test_stats();
    stats.min_timestamp = 1_000;
    stats.min_local_deletion_time = 0;
    stats.min_ttl = 0;
    let min_ts = stats.min_timestamp;
    let mut writer = DataWriter::new(stats);
    let schema = two_static_column_schema();
    let table_id = TableId::new("test_ks", "test_table");
    let pk = PartitionKey::single("id", Value::Integer(1));

    // Row max is 5000 (`s2`); `s1` genuinely survives from an OLDER write at 3000.
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

    writer.write_static_row(&mutation, &schema).unwrap();
    let buf = &writer.buffer;

    // Decode the static-row body. Layout (#1196: no row-level liveness, ALL columns
    // present → no subset bitmap): [flags][ext=0x01][row_size uvint][prev_size uvint]
    // then cells in static-column order (s1, s2), each simple cell:
    // [cell_flags][ts_delta uvint][value_len uvint][value].
    assert_eq!(buf[0], ROW_HAS_EXTENDED_FLAGS | ROW_HAS_ALL_COLUMNS);
    assert_eq!(buf[1], EXTENDED_IS_STATIC);
    let mut pos = 2usize;
    let _row_size = read_uvint_at(buf, &mut pos);
    let _prev_size = read_uvint_at(buf, &mut pos);

    let read_cell_ts = |buf: &[u8], pos: &mut usize| -> i64 {
        let flags = buf[*pos];
        *pos += 1;
        assert_eq!(
            flags & CELL_USE_ROW_TIMESTAMP,
            0,
            "static cell must carry its own explicit timestamp (no row liveness)"
        );
        let ts_delta = read_uvint_at(buf, pos) as i64;
        // skip value (length-prefixed text)
        let len = read_uvint_at(buf, pos) as usize;
        *pos += len;
        min_ts + ts_delta
    };

    // Sorted static order: s1 then s2.
    let s1_decoded = read_cell_ts(buf, &mut pos);
    let s2_decoded = read_cell_ts(buf, &mut pos);

    assert_eq!(
        s1_decoded, s1_ts,
        "#1018: write_static_row must keep the surviving older static cell's OWN \
         writetime ({s1_ts}), not rewrite it to the row max ({row_ts})"
    );
    assert_eq!(
        s2_decoded, row_ts,
        "#1018: the newest static cell keeps its own (row-max) writetime"
    );
}

/// Issue #1018 (roborev Medium) companion: `write_static_row` must also preserve a
/// static CELL TOMBSTONE's own `markedForDeleteAt` (per-cell write timestamp), not
/// rewrite it to the row max. A compacted static row can hold a live static cell
/// alongside an older static cell tombstone at a DIFFERENT (lower) writetime.
///
/// Pre-fix the `Delete` op was stamped at the row max; post-fix it carries its own
/// per-cell timestamp resolved via `op_cell_write_timestamp`.
#[test]
fn write_static_row_preserves_cell_tombstone_writetime_1018() {
    let mut stats = create_test_stats();
    stats.min_timestamp = 1_000;
    stats.min_local_deletion_time = 0;
    stats.min_ttl = 0;
    let min_ts = stats.min_timestamp;
    let min_ldt = stats.min_local_deletion_time as i64;
    let mut writer = DataWriter::new(stats);
    let schema = two_static_column_schema();
    let table_id = TableId::new("test_ks", "test_table");
    let pk = PartitionKey::single("id", Value::Integer(1));

    // `s2` is a live write at the row max (5000); `s1` is an older static cell
    // tombstone whose own markedForDeleteAt is 3000 and explicit LDT is 1234.
    let row_ts = 5000i64;
    let tomb_ts = 3000i64;
    let tomb_ldt = 1234i32;
    let mut cell_ts = HashMap::new();
    cell_ts.insert("s1".to_string(), tomb_ts);
    cell_ts.insert("s2".to_string(), row_ts);

    let mut mutation = Mutation::new(
        table_id,
        pk,
        None,
        vec![
            CellOperation::Delete {
                column: "s1".to_string(),
                local_deletion_time: Some(tomb_ldt),
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

    writer.write_static_row(&mutation, &schema).unwrap();
    let buf = &writer.buffer;

    // NOT all-writes (one Delete) → ROW_HAS_ALL_COLUMNS unset, a column subset
    // bitmap precedes the cells. Both columns present → subset is a single 0x00.
    assert_eq!(buf[0] & ROW_HAS_ALL_COLUMNS, 0);
    assert_eq!(buf[1], EXTENDED_IS_STATIC);
    let mut pos = 2usize;
    let _row_size = read_uvint_at(buf, &mut pos);
    let _prev_size = read_uvint_at(buf, &mut pos);
    let subset = read_uvint_at(buf, &mut pos);
    assert_eq!(
        subset, 0,
        "both static columns present → empty missing subset"
    );

    // Sorted static order: s1 (tombstone) then s2 (live write).
    // Tombstone cell: [flags(CELL_IS_DELETED|CELL_HAS_EMPTY_VALUE)][ts_delta][ldt_delta]
    let s1_flags = buf[pos];
    pos += 1;
    assert_eq!(
        s1_flags & CELL_IS_DELETED,
        CELL_IS_DELETED,
        "s1 must be a cell tombstone"
    );
    assert_eq!(s1_flags & CELL_USE_ROW_TIMESTAMP, 0);
    let s1_ts_delta = read_uvint_at(buf, &mut pos) as i64;
    let s1_ldt_delta = read_uvint_at(buf, &mut pos) as i64;
    let s1_marked_for_delete = min_ts + s1_ts_delta;
    let s1_ldt = min_ldt + s1_ldt_delta;

    assert_eq!(
        s1_marked_for_delete, tomb_ts,
        "#1018: static cell tombstone must keep its OWN markedForDeleteAt ({tomb_ts}), \
         not the row max ({row_ts})"
    );
    assert_eq!(
        s1_ldt, tomb_ldt as i64,
        "#1018: static cell tombstone keeps its own explicit localDeletionTime"
    );

    // Live cell s2 keeps the row max.
    let s2_flags = buf[pos];
    pos += 1;
    assert_eq!(s2_flags & CELL_IS_DELETED, 0);
    let s2_ts_delta = read_uvint_at(buf, &mut pos) as i64;
    assert_eq!(
        min_ts + s2_ts_delta,
        row_ts,
        "#1018: the live static cell keeps its own (row-max) writetime"
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

    let bytes = serialize_value(&Value::Udt(Box::new(company.clone()))).unwrap();
    let expected = serializer
        .serialize_udt(&Value::Udt(Box::new(company)), &phase3_company_schema())
        .unwrap();

    assert_eq!(bytes, expected);
}

#[test]
fn test_serialize_collection_containing_nested_udts() {
    let serializer = TypeSerializer::new();
    let company = phase3_company_value();
    let company_bytes = serializer
        .serialize_udt(
            &Value::Udt(Box::new(company.clone())),
            &phase3_company_schema(),
        )
        .unwrap();

    let value = Value::Map(vec![(
        Value::Text("empresa_日本".to_string()),
        Value::Frozen(Box::new(Value::Udt(Box::new(company)))),
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
        .serialize_udt(
            &Value::Udt(Box::new(address.clone())),
            &phase3_address_schema(),
        )
        .unwrap();
    let person_bytes = serializer
        .serialize_udt(
            &Value::Udt(Box::new(person.clone())),
            &phase3_person_schema(),
        )
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
            Value::Frozen(Box::new(Value::Udt(Box::new(address)))),
        )]))),
        Value::Frozen(Box::new(Value::Udt(Box::new(person)))),
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
