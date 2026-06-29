//! Issue #764 (writer D2): an explicit `local_deletion_time` on a `Mutation`
//! must flow end-to-end to the row tombstone written in Data.db and to the
//! Statistics.db min/max localDeletionTime aggregates.
//!
//! Default behavior (`local_deletion_time: None`) MUST be identical to the
//! historical timestamp-derived value (`timestamp_micros / 1_000_000`).
//!
//! These tests assert the WRITTEN BYTES and the parsed Statistics.db, not a
//! pure CQLite round-trip, so a reader/writer that are wrong in the same way
//! cannot mask a regression.

#![cfg(feature = "write-support")]

use cqlite_core::parser::enhanced_statistics_parser::parse_statistics_with_fallback;
use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use std::collections::HashMap;
use tempfile::TempDir;

// 2024-01-01T00:00:00Z in microseconds. Derived LDT = 1_704_067_200 s.
const T0: i64 = 1_704_067_200_000_000;
const DERIVED_LDT: i32 = (T0 / 1_000_000) as i32;
// An explicit local deletion time deliberately DIFFERENT from the derived one.
const EXPLICIT_LDT: i32 = 1_650_000_000; // 2022-04-15-ish, well below DERIVED_LDT

const ROW_HAS_DELETION: u8 = 0x10;
const ROW_HAS_TIMESTAMP: u8 = 0x04;
const ROW_HAS_TTL: u8 = 0x08;
const ROW_HAS_ALL_COLUMNS: u8 = 0x20;
const ROW_HAS_COMPLEX_DELETION: u8 = 0x40;
const ROW_HAS_EXTENDED_FLAGS: u8 = 0x80;
const EXTENDED_IS_STATIC: u8 = 0x01;
// Cell flags must match the writer (data_writer.rs): IS_DELETED=0x01,
// HAS_EMPTY_VALUE=0x04, USE_ROW_TIMESTAMP=0x08.
const CELL_IS_DELETED: u8 = 0x01;

/// Single int PK, single regular text column.
fn test_schema() -> TableSchema {
    TableSchema {
        keyspace: "issue764".to_string(),
        table: "t".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![Column {
            name: "name".to_string(),
            data_type: "text".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        }],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// Flush the mutations to a one-SSTable directory and return (Data.db, Statistics.db) bytes.
async fn flush_mutations(schema: &TableSchema, mutations: Vec<Mutation>) -> (Vec<u8>, Vec<u8>) {
    let temp = TempDir::new().unwrap();
    let config = WriteEngineConfig::new(
        temp.path().join("data"),
        temp.path().join("wal"),
        schema.clone(),
    );
    let mut engine = WriteEngine::new(config).expect("engine");
    for m in mutations {
        engine.write_async(m).await.expect("write");
    }
    let info = engine
        .flush()
        .await
        .expect("flush")
        .expect("flush produced an sstable");
    let data = std::fs::read(&info.data_path).expect("read Data.db");
    let stats = std::fs::read(&info.stats_path).expect("read Statistics.db");
    (data, stats)
}

/// Read a Cassandra unsigned VInt; returns (value, new_pos).
fn read_vuint(data: &[u8], pos: usize) -> (u64, usize) {
    let first = data[pos];
    let extra = first.leading_ones() as usize;
    assert!(extra < 8, "vint with 8 extension bytes not expected");
    let mut value = (first as u64) & (0xFFu64 >> (extra + 1).min(8));
    for i in 0..extra {
        value = (value << 8) | data[pos + 1 + i] as u64;
    }
    (value, pos + 1 + extra)
}

/// Decode the row tombstone localDeletionTime from a single-partition,
/// single-row-tombstone Data.db file, returning (min_ldt_from_stats_baseline,
/// reconstructed_row_ldt).
///
/// Layout for an int PK with no partition tombstone:
///   keylen(2) + key(4) + partition deletion LDT(4) + MFDA(8) + row...
/// Row (pure DeleteRow): flags(=ROW_HAS_DELETION) + row_size(vint) +
///   prev_size(vint) + ts_delta(vint) + ldt_delta(vint) + columns subset(vint)
fn decode_row_tombstone_ldt(data: &[u8], stats_min_ldt: i32) -> i32 {
    assert_eq!(
        u16::from_be_bytes([data[0], data[1]]),
        4,
        "int partition key length"
    );
    let mut pos = 2 + 4;
    // Partition deletion must be LIVE (the deletion is at the ROW level here).
    assert_eq!(
        i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]),
        i32::MAX,
        "partition deletion must be LIVE for a row tombstone"
    );
    pos += 4 + 8; // skip partition LDT + markedForDeleteAt

    assert_eq!(
        data[pos] & ROW_HAS_DELETION,
        ROW_HAS_DELETION,
        "row flags must include HAS_DELETION (0x10)"
    );
    pos += 1;
    let (_row_size, p) = read_vuint(data, pos);
    pos = p;
    let (_prev, p) = read_vuint(data, pos); // prev_unfiltered_size
    pos = p;
    // HAS_DELETION (no HAS_TIMESTAMP for a pure row tombstone): markedForDeleteAt
    // delta then localDeletionTime delta, both unsigned VInts.
    let (_ts_delta, p) = read_vuint(data, pos);
    pos = p;
    let (ldt_delta, _p) = read_vuint(data, pos);
    // localDeletionTime = min_local_deletion_time + ldt_delta (wrapping add).
    (stats_min_ldt as i64 + ldt_delta as i64) as i32
}

/// Helper: parse Statistics.db and return min localDeletionTime
/// (exposed by the parser as `min_deletion_time`).
fn stats_min_ldt(stats_bytes: &[u8]) -> i32 {
    let (_rem, stats) =
        parse_statistics_with_fallback(stats_bytes, None).expect("parse Statistics.db");
    stats.timestamp_stats.min_deletion_time as i32
}

#[tokio::test]
async fn explicit_local_deletion_time_flows_to_data_and_statistics() {
    let schema = test_schema();
    let table_id = TableId::new("issue764", "t");
    let pk = PartitionKey::single("id", Value::Integer(7));

    let delete_row = Mutation::new(table_id, pk, None, vec![CellOperation::DeleteRow], T0, None)
        .with_local_deletion_time(EXPLICIT_LDT);

    let (data, stats) = flush_mutations(&schema, vec![delete_row]).await;

    // Statistics.db min localDeletionTime must reflect the EXPLICIT value,
    // not the timestamp-derived one.
    let min_ldt = stats_min_ldt(&stats);
    assert_eq!(
        min_ldt, EXPLICIT_LDT,
        "Statistics.db min localDeletionTime must equal the explicit value"
    );
    assert_ne!(
        min_ldt, DERIVED_LDT,
        "the explicit value must differ from the timestamp-derived value (test sanity)"
    );

    // Data.db row tombstone localDeletionTime must reconstruct to EXPLICIT_LDT.
    let row_ldt = decode_row_tombstone_ldt(&data, min_ldt);
    assert_eq!(
        row_ldt, EXPLICIT_LDT,
        "Data.db row tombstone localDeletionTime must equal the explicit value"
    );
}

#[tokio::test]
async fn default_none_preserves_timestamp_derived_local_deletion_time() {
    let schema = test_schema();
    let table_id = TableId::new("issue764", "t");
    let pk = PartitionKey::single("id", Value::Integer(7));

    // No explicit local_deletion_time: behavior must match the historical
    // timestamp-derived value.
    let delete_row = Mutation::new(table_id, pk, None, vec![CellOperation::DeleteRow], T0, None);
    assert_eq!(delete_row.local_deletion_time, None);

    let (data, stats) = flush_mutations(&schema, vec![delete_row]).await;

    let min_ldt = stats_min_ldt(&stats);
    assert_eq!(
        min_ldt, DERIVED_LDT,
        "with None, Statistics.db min localDeletionTime must be timestamp-derived"
    );

    let row_ldt = decode_row_tombstone_ldt(&data, min_ldt);
    assert_eq!(
        row_ldt, DERIVED_LDT,
        "with None, Data.db row tombstone localDeletionTime must be timestamp-derived"
    );
}

// ── Finding 1: complex/collection column deletion ──────────────────────────

/// Single int PK, single regular `set<int>` column (a complex/non-frozen
/// collection, so a column delete emits a complex-column deletion block).
fn complex_schema() -> TableSchema {
    TableSchema {
        keyspace: "issue764".to_string(),
        table: "tc".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![Column {
            name: "tags".to_string(),
            data_type: "set<int>".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        }],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// Decode the complex-column-deletion localDeletionTime from a single-partition
/// Data.db file whose only row deletes a complex (collection) column.
///
/// Row layout (flags = HAS_COMPLEX_DELETION only, no timestamp / no deletion):
///   flags(1) + row_size(vint) + prev_size(vint) + columns_subset(vint)
///   + complex_deletion( markedForDeleteAt:vint, localDeletionTime:vint )
///   + cell_count:vint(=0)
fn decode_complex_deletion_ldt(data: &[u8], stats_min_ldt: i32) -> i32 {
    assert_eq!(
        u16::from_be_bytes([data[0], data[1]]),
        4,
        "int partition key length"
    );
    let mut pos = 2 + 4;
    assert_eq!(
        i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]),
        i32::MAX,
        "partition deletion must be LIVE"
    );
    pos += 4 + 8; // skip partition LDT + markedForDeleteAt

    let flags = data[pos];
    assert_eq!(
        flags & ROW_HAS_COMPLEX_DELETION,
        ROW_HAS_COMPLEX_DELETION,
        "row flags must include HAS_COMPLEX_DELETION (0x40)"
    );
    assert_eq!(flags & ROW_HAS_DELETION, 0, "must not be a row tombstone");
    assert_eq!(
        flags & ROW_HAS_EXTENDED_FLAGS,
        0,
        "must not have extended flags"
    );
    pos += 1;
    let (_row_size, p) = read_vuint(data, pos);
    pos = p;
    let (_prev, p) = read_vuint(data, pos);
    pos = p;

    if flags & ROW_HAS_TIMESTAMP != 0 {
        let (_ts, p) = read_vuint(data, pos);
        pos = p;
    }
    assert_eq!(flags & ROW_HAS_TTL, 0, "no row TTL expected");
    // columns subset (single regular column → present): one VInt.
    if flags & ROW_HAS_ALL_COLUMNS == 0 {
        let (_subset, p) = read_vuint(data, pos);
        pos = p;
    }
    // complex column deletion: markedForDeleteAt delta, then localDeletionTime delta.
    let (_mfda, p) = read_vuint(data, pos);
    pos = p;
    let (ldt_delta, p) = read_vuint(data, pos);
    pos = p;
    let (cell_count, _p) = read_vuint(data, pos);
    assert_eq!(cell_count, 0, "complex deletion writes zero cells");
    (stats_min_ldt as i64 + ldt_delta as i64) as i32
}

#[tokio::test]
async fn explicit_local_deletion_time_flows_to_complex_column_deletion() {
    let schema = complex_schema();
    let table_id = TableId::new("issue764", "tc");
    let pk = PartitionKey::single("id", Value::Integer(7));

    // Delete the whole collection with an explicit LDT BELOW the
    // timestamp-derived one. The explicit value becomes the stats baseline.
    let delete_collection = Mutation::new(
        table_id,
        pk,
        None,
        vec![CellOperation::Delete {
            column: "tags".to_string(),
            local_deletion_time: None,
        }],
        T0,
        None,
    )
    .with_local_deletion_time(EXPLICIT_LDT);

    let (data, stats) = flush_mutations(&schema, vec![delete_collection]).await;

    let min_ldt = stats_min_ldt(&stats);
    assert_eq!(
        min_ldt, EXPLICIT_LDT,
        "Statistics.db min localDeletionTime must equal the explicit value"
    );

    let complex_ldt = decode_complex_deletion_ldt(&data, min_ldt);
    assert_eq!(
        complex_ldt, EXPLICIT_LDT,
        "complex-column-deletion localDeletionTime must equal the explicit value, \
         not the timestamp-derived one"
    );
    assert_ne!(complex_ldt, DERIVED_LDT, "test sanity: values must differ");
}

// ── Finding 2: static-column delete from an OLDER mutation ─────────────────

/// Single int PK, two static columns (`s_old`, `s_new`), no clustering.
fn static_schema() -> TableSchema {
    TableSchema {
        keyspace: "issue764".to_string(),
        table: "ts".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "s_old".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: true,
            },
            Column {
                name: "s_new".to_string(),
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

/// Decode the two static-cell tombstone localDeletionTimes from a
/// single-partition Data.db whose static row deletes BOTH static columns.
///
/// Static row layout (issue #1196: a static row carries NO row-level liveness,
/// so flags = HAS_EXTENDED_FLAGS only — no HAS_TIMESTAMP, no liveness_ts delta;
/// extended = IS_STATIC):
///   flags(1) + extended(1) + row_size(vint) + prev_size(vint)
///   + [columns_subset(vint) if !HAS_ALL_COLUMNS]
///   + two tombstone cells in static-column order.
///
/// Static columns sort simple-before-complex then by name, so the cell order is
/// `s_new`, then `s_old`. Each tombstone cell is:
///   flags(=CELL_IS_DELETED|HAS_EMPTY_VALUE), ts_delta(vint), ldt_delta(vint).
///
/// Returns (s_new_ldt, s_old_ldt).
fn decode_static_cell_tombstone_ldts(data: &[u8], stats_min_ldt: i32) -> (i32, i32) {
    assert_eq!(
        u16::from_be_bytes([data[0], data[1]]),
        4,
        "int partition key length"
    );
    let mut pos = 2 + 4;
    assert_eq!(
        i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]),
        i32::MAX,
        "partition deletion must be LIVE"
    );
    pos += 4 + 8; // skip partition LDT + markedForDeleteAt

    let flags = data[pos];
    assert_eq!(
        flags & ROW_HAS_EXTENDED_FLAGS,
        ROW_HAS_EXTENDED_FLAGS,
        "static row must have extended flags"
    );
    // Issue #1196: a static row carries NO row-level liveness timestamp.
    assert_eq!(
        flags & ROW_HAS_TIMESTAMP,
        0,
        "static row must NOT carry a row-level liveness timestamp (#1196)"
    );
    assert_eq!(flags & ROW_HAS_DELETION, 0, "this is not a row tombstone");
    pos += 1;
    assert_eq!(
        data[pos] & EXTENDED_IS_STATIC,
        EXTENDED_IS_STATIC,
        "extended flags must mark the row static"
    );
    pos += 1;
    let (_row_size, p) = read_vuint(data, pos);
    pos = p;
    let (_prev, p) = read_vuint(data, pos);
    pos = p;
    // No row-level liveness timestamp delta (#1196): the next bytes are the
    // columns subset (both columns deleted, none "all present").
    assert_eq!(
        flags & ROW_HAS_ALL_COLUMNS,
        0,
        "two deletes must not flag HAS_ALL_COLUMNS"
    );
    let (_subset, p) = read_vuint(data, pos);
    pos = p;

    // Two tombstone cells, in order s_new then s_old.
    let read_cell_ldt = |data: &[u8], mut p: usize| -> (i32, usize) {
        let cell_flags = data[p];
        assert_eq!(
            cell_flags & CELL_IS_DELETED,
            CELL_IS_DELETED,
            "static cell must be a tombstone"
        );
        p += 1;
        let (_cell_ts, q) = read_vuint(data, p);
        p = q;
        let (ldt_delta, q) = read_vuint(data, p);
        p = q;
        ((stats_min_ldt as i64 + ldt_delta as i64) as i32, p)
    };
    let (s_new_ldt, p) = read_cell_ldt(data, pos);
    let (s_old_ldt, _p) = read_cell_ldt(data, p);
    (s_new_ldt, s_old_ldt)
}

#[tokio::test]
async fn explicit_local_deletion_time_preserved_for_older_static_delete() {
    let schema = static_schema();
    let table_id = TableId::new("issue764", "ts");
    let pk = PartitionKey::single("id", Value::Integer(7));

    // OLDER mutation: delete static column s_old with an explicit LDT BELOW the
    // timestamp-derived baseline. This seeds the stats min localDeletionTime.
    let older_delete = Mutation::new(
        table_id.clone(),
        pk.clone(),
        None,
        vec![CellOperation::Delete {
            column: "s_old".to_string(),
            local_deletion_time: None,
        }],
        T0,
        None,
    )
    .with_local_deletion_time(EXPLICIT_LDT);

    // NEWER mutation: delete static column s_new with no explicit LDT (so its
    // LDT is timestamp-derived, much higher than EXPLICIT_LDT). The synthetic
    // static row historically stamped ITS LDT on ALL cells.
    let newer_delete = Mutation::new(
        table_id,
        pk,
        None,
        vec![CellOperation::Delete {
            column: "s_new".to_string(),
            local_deletion_time: None,
        }],
        T0 + 2_000_000, // 2 seconds later
        None,
    );

    // Order: older first, then newer.
    let (data, stats) = flush_mutations(&schema, vec![older_delete, newer_delete]).await;

    let min_ldt = stats_min_ldt(&stats);
    assert_eq!(
        min_ldt, EXPLICIT_LDT,
        "Statistics.db min localDeletionTime must be seeded from the older delete's explicit LDT"
    );

    let newer_derived = ((T0 + 2_000_000) / 1_000_000) as i32;
    let (s_new_ldt, s_old_ldt) = decode_static_cell_tombstone_ldts(&data, min_ldt);

    // The s_old tombstone must carry ITS OWN explicit LDT, not the newer
    // mutation's timestamp-derived LDT.
    assert_eq!(
        s_old_ldt, EXPLICIT_LDT,
        "older static-column delete must preserve its own explicit localDeletionTime"
    );
    assert_ne!(
        s_old_ldt, newer_derived,
        "must not inherit the newer mutation's timestamp-derived LDT"
    );
    // The s_new tombstone keeps its own timestamp-derived LDT.
    assert_eq!(
        s_new_ldt, newer_derived,
        "newer static-column delete keeps its timestamp-derived localDeletionTime"
    );
}

const CELL_USE_ROW_TIMESTAMP: u8 = 0x08;
const CELL_HAS_EMPTY_VALUE: u8 = 0x04;

/// Decode the cell flag bytes of the two static LIVE cells (`s_new`, `s_old`).
///
/// Returns `(s_new_flags, s_old_flags)`. A cell written with
/// `CELL_USE_ROW_TIMESTAMP` borrows the row liveness timestamp; one written
/// with explicit timestamp does not (and carries its own ts delta).
fn decode_static_live_cell_flags(data: &[u8]) -> (u8, u8) {
    assert_eq!(
        u16::from_be_bytes([data[0], data[1]]),
        4,
        "int partition key length"
    );
    let mut pos = 2 + 4 + 4 + 8; // pk + partition LDT + markedForDeleteAt

    let flags = data[pos];
    assert_eq!(flags & ROW_HAS_EXTENDED_FLAGS, ROW_HAS_EXTENDED_FLAGS);
    // Issue #1196: a static row carries NO row-level liveness timestamp.
    assert_eq!(
        flags & ROW_HAS_TIMESTAMP,
        0,
        "static row must NOT carry a row-level liveness timestamp (#1196)"
    );
    assert_eq!(
        flags & ROW_HAS_DELETION,
        0,
        "two live writes, not a tombstone"
    );
    pos += 1;
    assert_eq!(data[pos] & EXTENDED_IS_STATIC, EXTENDED_IS_STATIC);
    pos += 1;
    let (_row_size, p) = read_vuint(data, pos);
    pos = p;
    let (_prev, p) = read_vuint(data, pos);
    pos = p;
    // No row-level liveness timestamp delta (#1196).
    assert_eq!(
        flags & ROW_HAS_ALL_COLUMNS,
        ROW_HAS_ALL_COLUMNS,
        "both static columns present as writes ⇒ HAS_ALL_COLUMNS, no subset bytes"
    );

    // Each live cell: flags(1) + [ts_delta(vint) if !USE_ROW_TIMESTAMP]
    //   + [value_len(vint) + value_bytes  unless HAS_EMPTY_VALUE].
    let read_cell = |data: &[u8], mut p: usize| -> (u8, usize) {
        let cell_flags = data[p];
        assert_eq!(
            cell_flags & CELL_IS_DELETED,
            0,
            "live cell, not a tombstone"
        );
        p += 1;
        if (cell_flags & CELL_USE_ROW_TIMESTAMP) == 0 {
            let (_ts_delta, q) = read_vuint(data, p);
            p = q;
        }
        if (cell_flags & CELL_HAS_EMPTY_VALUE) == 0 {
            let (vlen, q) = read_vuint(data, p);
            p = q + vlen as usize;
        }
        (cell_flags, p)
    };
    let (s_new_flags, p) = read_cell(data, pos); // s_new sorts first
    let (s_old_flags, _p) = read_cell(data, p);
    (s_new_flags, s_old_flags)
}

#[tokio::test]
async fn older_static_live_write_keeps_its_own_timestamp() {
    // Finding (job 371): an older surviving static WRITE merged into a row whose
    // liveness timestamp comes from a NEWER static mutation must not be promoted
    // to the newer timestamp via CELL_USE_ROW_TIMESTAMP.
    let schema = static_schema();
    let table_id = TableId::new("issue764", "tslive");
    let pk = PartitionKey::single("id", Value::Integer(11));

    // OLDER write of s_old at T0.
    let older = Mutation::new(
        table_id.clone(),
        pk.clone(),
        None,
        vec![CellOperation::Write {
            column: "s_old".to_string(),
            value: Value::Text("old".to_string()),
        }],
        T0,
        None,
    );
    // NEWER write of s_new at T0+2s — this provides the row liveness timestamp.
    let newer = Mutation::new(
        table_id,
        pk,
        None,
        vec![CellOperation::Write {
            column: "s_new".to_string(),
            value: Value::Text("new".to_string()),
        }],
        T0 + 2_000_000,
        None,
    );

    let (data, _stats) = flush_mutations(&schema, vec![older, newer]).await;
    let (s_new_flags, s_old_flags) = decode_static_live_cell_flags(&data);

    // Issue #1196: a static row carries NO row-level liveness timestamp, so there
    // is no row timestamp for any static cell to borrow — EVERY static cell must
    // carry its OWN explicit timestamp (CELL_USE_ROW_TIMESTAMP clear). This still
    // satisfies the finding's intent: the older s_old write is never promoted to
    // the newer mutation's timestamp.
    assert_eq!(
        s_new_flags & CELL_USE_ROW_TIMESTAMP,
        0,
        "static cells carry their own timestamp; no row liveness to borrow (#1196)"
    );
    assert_eq!(
        s_old_flags & CELL_USE_ROW_TIMESTAMP,
        0,
        "older static write must carry its own timestamp, not be promoted (#1196)"
    );
}

#[test]
fn effective_local_deletion_time_prefers_explicit() {
    let m = Mutation::new(
        TableId::new("ks", "t"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![CellOperation::DeleteRow],
        T0,
        None,
    );
    assert_eq!(m.effective_local_deletion_time(), DERIVED_LDT);

    let m2 = m.with_local_deletion_time(EXPLICIT_LDT);
    assert_eq!(m2.effective_local_deletion_time(), EXPLICIT_LDT);
}
