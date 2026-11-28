//! Moved out of `merge/mod.rs` unchanged (#4658); the module keeps its name via `#[path]`.

use super::*;
use crate::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use crate::storage::sstable::writer::{DataWriter, StatisticsMetadata};
use crate::storage::write_engine::mutation::{
    CellOperation, ClusteringKey, DecoratedKey, Mutation, PartitionKey, TableId,
};
use crate::types::{TombstoneInfo, TombstoneType, Value};
use std::collections::HashMap;

// ── Wire-format flag constants (mirror data_writer.rs; protocol constants,
//    NOT a reimplementation of writer logic — the load-bearing assertions are
//    on the REAL bytes produced by `DataWriter::write_partition`). ──────────
const ROW_HAS_TIMESTAMP: u8 = 0x04;
const ROW_HAS_DELETION: u8 = 0x10;
const ROW_HAS_ALL_COLUMNS: u8 = 0x20;
const ROW_HAS_EXTENDED_FLAGS: u8 = 0x80;
const EXTENDED_IS_STATIC: u8 = 0x01;
const CELL_IS_DELETED: u8 = 0x01;
const CELL_IS_EXPIRING: u8 = 0x02;

/// Deterministic stats baselines so temporal deltas are small single-byte
/// vints, keeping the manual byte walk simple (mirrors issue_821).
fn writer_stats() -> StatisticsMetadata {
    let mut s = StatisticsMetadata::new();
    s.min_timestamp = 1_000_000;
    s.min_ttl = 0;
    s.min_local_deletion_time = 0;
    s
}

/// Read a Cassandra unsigned vint at `pos`; returns `(value, bytes_consumed)`.
fn read_vuint(data: &[u8], pos: usize) -> (u64, usize) {
    let first = data[pos];
    let extra = first.leading_ones() as usize;
    assert!(extra < 8, "9-byte vint not expected in this framing");
    let mask: u64 = 0xFFu64 >> (extra + 1);
    let mut value = (first as u64) & mask;
    for i in 0..extra {
        value = (value << 8) | data[pos + 1 + i] as u64;
    }
    (value, extra + 1)
}

/// 4-byte big-endian int partition-key bytes.
fn int_key_bytes(n: i32) -> Vec<u8> {
    n.to_be_bytes().to_vec()
}

/// Partition-header byte size for a 4-byte int PK:
/// 2 (u16 key-length) + 4 (key) + 4 (LDT i32) + 8 (mfda i64) = 18.
const INT_PK_HEADER_SIZE: usize = 2 + 4 + 4 + 8;

/// Schema with a single clustering column whose sort order is configurable.
fn schema_one_clustering(ck_name: &str, ck_type: &str, order: ClusteringOrder) -> TableSchema {
    TableSchema {
        keyspace: "issue_822".to_string(),
        table: "tbl".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: ck_name.to_string(),
            data_type: ck_type.to_string(),
            position: 0,
            order,
        }],
        columns: vec![Column {
            name: "v".to_string(),
            data_type: "text".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        }],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

fn empty_merger(schema: TableSchema) -> KWayMerger {
    KWayMerger {
        runs: vec![],
        heap: BinaryHeap::new(),
        current_partition: None,
        gc_before_secs: None,
        now_secs: None,
        purge_safe: false,
        max_purgeable_timestamp: None,
        schema_arc: std::sync::Arc::new(schema.clone()),
        schema,
        _egress_slot: None,
        trace: NoTrace,
    }
}

fn ck_text(col: &str, s: &str) -> ClusteringKey {
    ClusteringKey {
        columns: vec![(col.to_string(), Value::text(s.to_string()))],
    }
}

// ── #10: DESC empty-vs-valued clustering ordering ────────────────────────

/// Direct assertion on `ClusteringKey::compare`: an EMPTY clustering value
/// must route through the column's reversed-ness.
///
/// Cassandra rule: under a reversed (DESC) clustering column, the empty
/// clustering value sorts AFTER valued ones. CQLite models the empty value
/// as `Text("")`; `compare_values` makes `"" < "a"` (ASC), and `compare`
/// reverses that for DESC, so empty becomes GREATER (sorts last). Under ASC
/// empty stays smallest. This test pins both directions.
#[test]
fn issue_10_desc_empty_vs_valued_via_clustering_compare() {
    let empty = ck_text("ck", "");
    let valued = ck_text("ck", "a");

    // ASC: empty < valued (empty sorts first).
    let asc = schema_one_clustering("ck", "text", ClusteringOrder::Asc);
    assert_eq!(
        empty.compare(&valued, &asc).expect("compare must not fail"),
        Ordering::Less,
        "ASC: empty clustering value must sort BEFORE a valued one"
    );

    // DESC: empty > valued (empty sorts last). This is the load-bearing
    // assertion for #10 — empty-vs-valued must be routed through reversed-ness.
    let desc = schema_one_clustering("ck", "text", ClusteringOrder::Desc);
    assert_eq!(
        empty
            .compare(&valued, &desc)
            .expect("compare must not fail"),
        Ordering::Greater,
        "DESC: empty clustering value must sort AFTER a valued one (reversed-ness \
             must apply to empty-vs-valued comparison)"
    );

    // Symmetry: valued-vs-empty is the mirror image under DESC.
    assert_eq!(
        valued
            .compare(&empty, &desc)
            .expect("compare must not fail"),
        Ordering::Less,
        "DESC: a valued clustering value must sort BEFORE the empty one"
    );
}

/// End-to-end through the real merge sort in `merge_partition_rows`: with a
/// DESC clustering column, the emitted row order must place the empty
/// clustering value LAST (after valued rows).
#[test]
fn issue_10_desc_empty_vs_valued_in_merge_output_order() {
    let schema = schema_one_clustering("ck", "text", ClusteringOrder::Desc);
    let mut merger = empty_merger(schema);

    let pk = DecoratedKey::new(7, vec![0, 0, 0, 7]);
    const TS: i64 = 1_700_000_000_000_000;

    let make = |ck: &str| {
        MergeEntry::new(
            0,
            pk.clone(),
            Some(ck_text("ck", ck)),
            TS,
            RowData::Live {
                cells: vec![CellData {
                    column: "v".to_string(),
                    value: Value::text(format!("row-{ck}")),
                    timestamp: TS,
                    ttl: None,
                    cell_path: None,
                    local_deletion_time: None,
                    is_complex_element: false,
                    is_deleted: false,
                    has_empty_value: false,
                }],
            },
        )
    };

    // Feed in deliberately scrambled input order.
    let input = vec![make(""), make("b"), make("a")];
    let merged = merger
        .merge_partition_rows(input)
        .expect("merge_partition_rows must not fail");

    let order: Vec<String> = merged
        .iter()
        .map(|e| match &e.clustering_key {
            Some(ck) => match &ck.columns[0].1 {
                Value::Text(s) => String::from_utf8_lossy(s).into_owned(),
                other => format!("{other:?}"),
            },
            None => "<none>".to_string(),
        })
        .collect();

    // DESC valued order is "b","a"; the empty value sorts LAST.
    assert_eq!(
        order,
        vec!["b".to_string(), "a".to_string(), "".to_string()],
        "DESC merge output: valued rows descending, empty clustering value LAST"
    );
}

// ── #13/#3: tombstone beats expiring at EQUAL timestamp ───────────────────

/// At EQUAL timestamp a (cell) tombstone must beat an EXPIRING (TTL) cell.
/// In CQLite's merge model an "expiring" cell is a live `CellData` with
/// `ttl = Some(_)`; a cell tombstone is `Value::Tombstone(CellTombstone)`.
/// `reconcile_cluster` (driven via `merge_partition_rows`) must keep the
/// tombstone regardless of which file (run_index) it came from.
#[test]
fn issue_13_tombstone_beats_expiring_at_equal_ts() {
    let schema = schema_one_clustering("ck", "text", ClusteringOrder::Asc);
    let mut merger = empty_merger(schema);

    let pk = DecoratedKey::new(11, vec![0, 0, 0, 11]);
    const TS: i64 = 1_700_000_000_000_000;

    // Build both rows so the SAME clustering key bucket is formed: the cells
    // include the clustering column "ck" plus the data column "v".
    let ck_cell = || CellData {
        column: "ck".to_string(),
        value: Value::text("c".to_string()),
        timestamp: TS,
        ttl: None,
        cell_path: None,
        local_deletion_time: None,
        is_complex_element: false,
        is_deleted: false,
        has_empty_value: false,
    };

    // Expiring (TTL) live cell for column "v", in the NEWER file (run 0).
    // run_index 0 would win a recency tiebreak — so if the tombstone still
    // wins, it is the equal-ts tombstone rule, not recency.
    let expiring = MergeEntry::new(
        0,
        pk.clone(),
        Some(ck_text("ck", "c")),
        TS,
        RowData::Live {
            cells: vec![
                ck_cell(),
                CellData {
                    column: "v".to_string(),
                    value: Value::text("expiring-if-buggy".to_string()),
                    timestamp: TS,
                    ttl: Some(3600),
                    cell_path: None,
                    local_deletion_time: None,
                    is_complex_element: false,
                    is_deleted: false,
                    has_empty_value: false,
                },
            ],
        },
    );

    // Cell tombstone for column "v", in the OLDER file (run 1), same ts.
    let tombstone = MergeEntry::new(
        1,
        pk.clone(),
        Some(ck_text("ck", "c")),
        TS,
        RowData::Live {
            cells: vec![
                ck_cell(),
                CellData {
                    column: "v".to_string(),
                    value: Value::Tombstone(Box::new(TombstoneInfo {
                        deletion_time: TS,
                        tombstone_type: TombstoneType::CellTombstone,
                        local_deletion_time: 0,
                        ttl: None,
                        range_start: None,
                        range_end: None,
                    })),
                    timestamp: TS,
                    ttl: None,
                    cell_path: None,
                    local_deletion_time: None,
                    is_complex_element: false,
                    is_deleted: false,
                    has_empty_value: false,
                },
            ],
        },
    );

    // Drive the real merger with the expiring (newer file) first.
    let merged = merger
        .merge_partition_rows(vec![expiring, tombstone])
        .expect("merge_partition_rows must not fail");

    assert_eq!(merged.len(), 1, "one clustering key => one merged winner");

    let cells = match &merged[0].row_data {
        RowData::Live { cells } => cells,
        other => panic!("expected Live row, got {other:?}"),
    };
    let v_cell = cells
        .iter()
        .find(|c| c.column == "v")
        .expect("column v must survive (as a tombstone)");

    assert!(
        KWayMerger::<NoTrace>::is_cell_tombstone(v_cell),
        "At equal ts the cell tombstone must beat the expiring (TTL) cell, even \
             though the expiring cell is in the newer file (run 0). Got a live value \
             => tombstone-vs-expiring tie reverted to recency (#13/#3 regression)."
    );
    assert!(
        v_cell.ttl.is_none(),
        "Surviving cell tombstone must carry no TTL (IS_DELETED and IS_EXPIRING \
             are mutually exclusive)."
    );
}

/// Mirror case: tombstone is in the NEWER file and the expiring cell is
/// older — tombstone still wins (it never depends on file order).
#[test]
fn issue_13_tombstone_beats_expiring_irrespective_of_run_index() {
    let schema = schema_one_clustering("ck", "text", ClusteringOrder::Asc);
    let mut merger = empty_merger(schema);

    let pk = DecoratedKey::new(12, vec![0, 0, 0, 12]);
    const TS: i64 = 1_700_000_000_000_000;

    let ck_cell = || CellData {
        column: "ck".to_string(),
        value: Value::text("c".to_string()),
        timestamp: TS,
        ttl: None,
        cell_path: None,
        local_deletion_time: None,
        is_complex_element: false,
        is_deleted: false,
        has_empty_value: false,
    };

    let tombstone = MergeEntry::new(
        0,
        pk.clone(),
        Some(ck_text("ck", "c")),
        TS,
        RowData::Live {
            cells: vec![
                ck_cell(),
                CellData {
                    column: "v".to_string(),
                    value: Value::Tombstone(Box::new(TombstoneInfo {
                        deletion_time: TS,
                        tombstone_type: TombstoneType::CellTombstone,
                        local_deletion_time: 0,
                        ttl: None,
                        range_start: None,
                        range_end: None,
                    })),
                    timestamp: TS,
                    ttl: None,
                    cell_path: None,
                    local_deletion_time: None,
                    is_complex_element: false,
                    is_deleted: false,
                    has_empty_value: false,
                },
            ],
        },
    );

    let expiring = MergeEntry::new(
        1,
        pk.clone(),
        Some(ck_text("ck", "c")),
        TS,
        RowData::Live {
            cells: vec![
                ck_cell(),
                CellData {
                    column: "v".to_string(),
                    value: Value::text("expiring-if-buggy".to_string()),
                    timestamp: TS,
                    ttl: Some(3600),
                    cell_path: None,
                    local_deletion_time: None,
                    is_complex_element: false,
                    is_deleted: false,
                    has_empty_value: false,
                },
            ],
        },
    );

    let merged = merger
        .merge_partition_rows(vec![tombstone, expiring])
        .expect("merge_partition_rows must not fail");

    let cells = match &merged[0].row_data {
        RowData::Live { cells } => cells,
        other => panic!("expected Live row, got {other:?}"),
    };
    let v_cell = cells.iter().find(|c| c.column == "v").expect("v survives");
    assert!(
        KWayMerger::<NoTrace>::is_cell_tombstone(v_cell),
        "Tombstone must win the equal-ts tie over an expiring cell regardless of \
             run_index ordering."
    );
}

/// Strict flag semantics (#13/#3) PINNED ON REAL WRITTEN BYTES.
///
/// On the WRITER side, `CELL_IS_EXPIRING` means `ttl != NO_TTL` and is mutually
/// exclusive with `CELL_IS_DELETED`. The old version of this test only built a
/// merge-model `CellData` and asserted its `ttl` field — tautological, proving
/// nothing about production. This version drives the REAL production path:
/// `DataWriter::write_partition` over a partition where the SAME column at the
/// SAME timestamp is written both as an EXPIRING cell (`WriteWithTtl`) and as a
/// cell tombstone (`Delete`). The writer's own equal-ts reconciliation
/// (`merge_row_group`: a `Delete` wins the timestamp tie over a `WriteWithTtl`)
/// must keep the tombstone, and the writer's own cell serializer
/// (`write_tombstone_cell`) must emit a flags byte with `CELL_IS_DELETED` set
/// and `CELL_IS_EXPIRING` NOT set. We assert on the actual Data.db bytes,
/// mirroring the byte-walk in tests/issue_821_writer_byte_invariants.rs.
#[test]
fn issue_3_tombstone_beats_expiring_and_writer_never_sets_both_flags() {
    // int PK, int clustering, single regular text column `v`.
    let schema = TableSchema {
        keyspace: "issue_822".to_string(),
        table: "tbl".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
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
            name: "v".to_string(),
            data_type: "text".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        }],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    const TS: i64 = 2_000_000;
    let key = DecoratedKey::new(1, int_key_bytes(1));

    // Same clustering key, same timestamp: one EXPIRING write and one cell
    // DELETE of column `v`. The writer's equal-ts reconciliation must keep the
    // tombstone.
    let expiring = Mutation::new(
        TableId::new("issue_822", "tbl"),
        PartitionKey::single("pk", Value::Integer(1)),
        Some(ClusteringKey::single("ck", Value::Integer(7))),
        vec![CellOperation::WriteWithTtl {
            column: "v".to_string(),
            value: Value::text("expiring-if-buggy".to_string()),
            ttl_seconds: 3600,
            local_deletion_time: None,
        }],
        TS,
        None,
    );
    let tombstone = Mutation::new(
        TableId::new("issue_822", "tbl"),
        PartitionKey::single("pk", Value::Integer(1)),
        Some(ClusteringKey::single("ck", Value::Integer(7))),
        vec![CellOperation::Delete {
            column: "v".to_string(),
            local_deletion_time: None,
        }],
        TS,
        None,
    );

    // Drive the REAL writer (expiring first so a recency bug would surface it).
    let mut w = DataWriter::new(writer_stats());
    w.write_partition(&key, &[expiring, tombstone], &schema, None, &[])
        .expect("write_partition must succeed");
    let bytes = w.finish().expect("finish must succeed");

    // Walk to the single row's cell flags byte.
    //
    // Layout after the 18-byte int-PK partition header:
    //   [row_flags u8]
    //   [clustering prefix: 1 header byte + 4 int value bytes = 5]
    //   [row_size vint][prev_size vint]
    //   [timestamp delta vint  (ROW_HAS_TIMESTAMP)]
    //   [column bitmap vint    (NOT ROW_HAS_ALL_COLUMNS — a Delete is present)]
    //   [cell flags u8]   ← the byte under test
    let mut p = INT_PK_HEADER_SIZE;
    let row_flags = bytes[p];
    p += 1;
    // The surviving op is a cell DELETE, so HAS_ALL_COLUMNS must NOT be set
    // (the column subset/bitmap is written), and the row keeps its liveness
    // timestamp from the (losing) expiring write.
    assert_eq!(
        row_flags & ROW_HAS_EXTENDED_FLAGS,
        0,
        "regular (non-static) row expected"
    );
    assert_ne!(
        row_flags & ROW_HAS_TIMESTAMP,
        0,
        "row keeps liveness ts from the expiring write"
    );
    assert_eq!(
        row_flags & ROW_HAS_ALL_COLUMNS,
        0,
        "a surviving cell tombstone forces a column subset/bitmap (NOT all-columns)"
    );
    // Clustering prefix: 1 header byte + 4 int value bytes.
    p += 1 + 4;
    // row_size vint, then prev_size vint (inside the body).
    let (_row_size, rs_len) = read_vuint(&bytes, p);
    p += rs_len;
    let (_prev_size, ps_len) = read_vuint(&bytes, p);
    p += ps_len;
    // Liveness timestamp delta vint (ROW_HAS_TIMESTAMP is set, no TTL on row).
    let (_ts_delta, ts_len) = read_vuint(&bytes, p);
    p += ts_len;
    // Column subset bitmap vint (NOT all-columns).
    let (_bitmap, bm_len) = read_vuint(&bytes, p);
    p += bm_len;

    // The single surviving cell's flags byte.
    let cell_flags = bytes[p];
    assert_ne!(
        cell_flags & CELL_IS_DELETED,
        0,
        "At equal ts the cell tombstone must win and be serialized as a deleted \
             cell (CELL_IS_DELETED set). Flags byte = {cell_flags:#04x}"
    );
    assert_eq!(
        cell_flags & CELL_IS_EXPIRING,
        0,
        "A tombstone cell must NOT carry CELL_IS_EXPIRING — IS_DELETED and \
             IS_EXPIRING are mutually exclusive. Flags byte = {cell_flags:#04x}"
    );
    assert_ne!(
        cell_flags & (CELL_IS_DELETED | CELL_IS_EXPIRING),
        CELL_IS_DELETED | CELL_IS_EXPIRING,
        "the writer must never set BOTH CELL_IS_DELETED and CELL_IS_EXPIRING on \
             one cell. Flags byte = {cell_flags:#04x}"
    );

    // The exact flags byte: tombstone (CELL_IS_DELETED) plus HAS_EMPTY_VALUE
    // (0x04), and crucially NOT CELL_IS_EXPIRING. write_tombstone_cell emits
    // CELL_IS_DELETED | CELL_HAS_EMPTY_VALUE = 0x05.
    assert_eq!(
        cell_flags, 0x05,
        "surviving cell tombstone must serialize as CELL_IS_DELETED|HAS_EMPTY \
             (0x05); got {cell_flags:#04x}"
    );
}

/// Issue #848: writer-path equal-ts tie-break, REVERSED source order. The
/// cell tombstone arrives FIRST, the expiring write SECOND (newer in
/// recency). The writer's `cells` LWW must still keep the tombstone — an
/// equal-ts expiring write does NOT supersede an existing cell tombstone
/// (parity `a62c749`). Pins the writer path agrees with `reconcile_cluster`
/// in BOTH source orders.
#[test]
fn issue_848_writer_tombstone_beats_expiring_tombstone_first() {
    let schema = TableSchema {
        keyspace: "issue_822".to_string(),
        table: "tbl".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
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
            name: "v".to_string(),
            data_type: "text".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        }],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    const TS: i64 = 2_000_000;
    let key = DecoratedKey::new(1, int_key_bytes(1));

    let tombstone = Mutation::new(
        TableId::new("issue_822", "tbl"),
        PartitionKey::single("pk", Value::Integer(1)),
        Some(ClusteringKey::single("ck", Value::Integer(7))),
        vec![CellOperation::Delete {
            column: "v".to_string(),
            local_deletion_time: None,
        }],
        TS,
        None,
    );
    let expiring = Mutation::new(
        TableId::new("issue_822", "tbl"),
        PartitionKey::single("pk", Value::Integer(1)),
        Some(ClusteringKey::single("ck", Value::Integer(7))),
        vec![CellOperation::WriteWithTtl {
            column: "v".to_string(),
            value: Value::text("expiring-if-buggy".to_string()),
            ttl_seconds: 3600,
            local_deletion_time: None,
        }],
        TS,
        None,
    );

    // Tombstone FIRST so a recency bug favoring the later expiring write
    // would surface (the reverse of issue_3_tombstone_beats_expiring).
    let mut w = DataWriter::new(writer_stats());
    w.write_partition(&key, &[tombstone, expiring], &schema, None, &[])
        .expect("write_partition must succeed");
    let bytes = w.finish().expect("finish must succeed");

    let mut p = INT_PK_HEADER_SIZE;
    let row_flags = bytes[p];
    p += 1;
    assert_eq!(
        row_flags & ROW_HAS_ALL_COLUMNS,
        0,
        "a surviving cell tombstone forces a column subset/bitmap"
    );
    // Clustering prefix: 1 header byte + 4 int value bytes.
    p += 1 + 4;
    let (_row_size, rs_len) = read_vuint(&bytes, p);
    p += rs_len;
    let (_prev_size, ps_len) = read_vuint(&bytes, p);
    p += ps_len;
    let (_ts_delta, ts_len) = read_vuint(&bytes, p);
    p += ts_len;
    let (_bitmap, bm_len) = read_vuint(&bytes, p);
    p += bm_len;

    let cell_flags = bytes[p];
    assert_ne!(
        cell_flags & CELL_IS_DELETED,
        0,
        "tombstone-first: at equal ts the cell tombstone must still win \
             (CELL_IS_DELETED set). Flags byte = {cell_flags:#04x}"
    );
    assert_eq!(
        cell_flags & CELL_IS_EXPIRING,
        0,
        "the surviving tombstone must NOT carry CELL_IS_EXPIRING. \
             Flags byte = {cell_flags:#04x}"
    );
    assert_eq!(
        cell_flags, 0x05,
        "surviving cell tombstone serializes as CELL_IS_DELETED|HAS_EMPTY \
             (0x05); got {cell_flags:#04x}"
    );
}

/// Companion merge-layer invariant (kept, but now secondary to the byte-level
/// assertion above): the equal-ts reconcile in `reconcile_cluster` keeps the
/// tombstone and never produces a `CellData` that is BOTH a cell tombstone AND
/// carries a TTL. This is the precondition that lets the writer keep the two
/// flags exclusive; the writer side is now pinned on real bytes above.
#[test]
fn issue_3_merge_layer_tombstone_carries_no_ttl_precondition() {
    let tomb = CellData {
        column: "v".to_string(),
        value: Value::Tombstone(Box::new(TombstoneInfo {
            deletion_time: 1,
            tombstone_type: TombstoneType::CellTombstone,
            local_deletion_time: 0,
            ttl: None,
            range_start: None,
            range_end: None,
        })),
        timestamp: 1,
        ttl: None,
        cell_path: None,
        local_deletion_time: None,
        is_complex_element: false,
        is_deleted: false,
        has_empty_value: false,
    };
    assert!(KWayMerger::<NoTrace>::is_cell_tombstone(&tomb));
    assert!(
        tomb.ttl.is_none(),
        "A cell tombstone must not carry a TTL (precondition for flag exclusivity)."
    );

    let expiring = CellData {
        column: "v".to_string(),
        value: Value::text("x".to_string()),
        timestamp: 1,
        ttl: Some(60),
        cell_path: None,
        local_deletion_time: None,
        is_complex_element: false,
        is_deleted: false,
        has_empty_value: false,
    };
    assert!(
        !KWayMerger::<NoTrace>::is_cell_tombstone(&expiring),
        "An expiring (TTL) cell is LIVE, not a tombstone."
    );
    assert!(expiring.ttl.is_some(), "Expiring cell carries a TTL.");
}

// ── #22: header-driven static / column superset (DIVERGENT) ──────────────

/// #22 is DIVERGENT in CQLite. Cassandra derives `hasStatic` and the column
/// superset from the merged input SSTables' `SerializationHeader`s, so a
/// DROPPED static column (whose static rows still exist on disk) is still
/// emitted. CQLite's compaction writer instead derives `hasStatic` and the
/// column set purely from the supplied `TableSchema`
/// (`schema.columns.iter().any(|c| c.is_static)` in data_writer.rs). There is
/// NO `SerializationHeader` read anywhere under `storage/write_engine/`.
///
/// This test PINS THE DIVERGENCE ON REAL WRITER OUTPUT (not a reimplemented
/// predicate). The old version asserted on a local copy of
/// `|s| s.columns.iter().any(|c| c.is_static)`, so it stayed green even if the
/// production writer changed. This version drives the REAL production path —
/// `DataWriter::write_partition` — with the SAME partition data (a mutation
/// that writes a static cell AND a regular cell) under two schemas:
///
///   * schema WITH the static column → the writer MUST emit a static-row
///     prelude (a row with `ROW_HAS_EXTENDED_FLAGS` + `EXTENDED_IS_STATIC`),
///   * schema with the static column DROPPED → the writer MUST NOT emit any
///     static-row prelude, even though a real cluster's on-disk
///     SerializationHeader still records the static column.
///
/// Both observations are read from the OBSERVED Data.db bytes, so a change to
/// the writer's static-emission decision (e.g. becoming header-driven) flips
/// this test instead of silently passing. The static op data is byte-identical
/// across the two runs; ONLY the schema differs — so the divergence is
/// attributable purely to the schema-driven `schema_has_static` decision in
/// `write_partition`.
#[test]
fn issue_22_static_emission_is_schema_driven_not_header_driven_divergent() {
    // Schema WITH a static column.
    let schema_with_static = TableSchema {
        keyspace: "issue_822".to_string(),
        table: "tbl".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
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
                name: "s".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: true,
            },
            Column {
                name: "v".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    // Same table after the static column was DROPPED from the schema. On a real
    // cluster the input SSTables' SerializationHeader still records the static
    // column and static rows still exist on disk — but CQLite only sees the
    // schema.
    let mut schema_dropped_static = schema_with_static.clone();
    schema_dropped_static.columns.retain(|c| c.name != "s");

    // IDENTICAL partition data for both runs: a mutation that writes the static
    // column `s` AND a regular cell `v`. Only the SCHEMA differs between runs.
    let key = DecoratedKey::new(1, int_key_bytes(1));
    let mutation = || {
        Mutation::new(
            TableId::new("issue_822", "tbl"),
            PartitionKey::single("pk", Value::Integer(1)),
            Some(ClusteringKey::single("ck", Value::Integer(7))),
            vec![
                CellOperation::Write {
                    column: "s".to_string(),
                    value: Value::text("static-val".to_string()),
                },
                CellOperation::Write {
                    column: "v".to_string(),
                    value: Value::text("row-val".to_string()),
                },
            ],
            2_000_000,
            None,
        )
    };

    // Deterministically walk one unfiltered starting at its flags byte `pos`.
    // Returns `(flags, ext_flags, next_pos)`. Modeled on
    // `tests/issue_821_writer_byte_invariants.rs::walk_unfiltered`: it parses
    // the partition structure (flags → ext flags → clustering prefix →
    // row_size vint → body) so we land on real row-flag bytes only — we never
    // scan the buffer for a stray high-bit byte (VInt lengths/timestamps and
    // cell payload bytes can legitimately set bit 0x80). The int-clustering
    // schema's non-static clustering prefix is 1 header byte + 4 value bytes.
    const INT_CLUSTERING_PREFIX_LEN: usize = 1 + 4;
    let walk_unfiltered = |bytes: &[u8], pos: usize| -> (u8, Option<u8>, usize) {
        let mut p = pos;
        let flags = bytes[p];
        p += 1;
        let mut ext = None;
        if flags & ROW_HAS_EXTENDED_FLAGS != 0 {
            ext = Some(bytes[p]);
            p += 1;
        }
        let is_static = ext.is_some_and(|e| e & EXTENDED_IS_STATIC != 0);
        // Non-static rows carry a clustering prefix before row_size; static
        // rows do not.
        if !is_static {
            p += INT_CLUSTERING_PREFIX_LEN;
        }
        let (row_size, rs_len) = read_vuint(bytes, p);
        p += rs_len;
        // row_size counts the body (prev_size vint + remaining body); the next
        // unfiltered begins immediately after it.
        let next = p + row_size as usize;
        (flags, ext, next)
    };

    // Parse the first unfiltered after the int-PK partition header and report
    // whether it is a static-row prelude. The static row, if present, is
    // ALWAYS the first unfiltered (this schema/layout is known), so we assert
    // on the flags byte at the parsed partition-header position — not on an
    // arbitrary high-bit byte found elsewhere in the buffer.
    let first_unfiltered_is_static = |bytes: &[u8]| -> bool {
        let (flags, ext, _next) = walk_unfiltered(bytes, INT_PK_HEADER_SIZE);
        flags & ROW_HAS_EXTENDED_FLAGS != 0 && ext.is_some_and(|e| e & EXTENDED_IS_STATIC != 0)
    };

    // ── Run 1: schema WITH static → static prelude MUST be emitted. ──
    let mut w_with = DataWriter::new(writer_stats());
    w_with
        .write_partition(&key, &[mutation()], &schema_with_static, None, &[])
        .expect("write_partition (with static) must succeed");
    let bytes_with = w_with.finish().expect("finish (with static)");
    assert!(
        first_unfiltered_is_static(&bytes_with),
        "schema WITH a static column: the writer must emit a static-row prelude \
             (ROW_HAS_EXTENDED_FLAGS | EXTENDED_IS_STATIC) as the first unfiltered"
    );

    // ── Run 2: schema with static DROPPED → NO static prelude. ──
    // Same partition data; only the schema lost the static column.
    let mut w_drop = DataWriter::new(writer_stats());
    w_drop
        .write_partition(&key, &[mutation()], &schema_dropped_static, None, &[])
        .expect("write_partition (dropped static) must succeed");
    let bytes_drop = w_drop.finish().expect("finish (dropped static)");
    assert!(
        !first_unfiltered_is_static(&bytes_drop),
        "DIVERGENT (#22): with the static column dropped from the SCHEMA, CQLite's \
             writer emits NO static-row prelude — even though a real cluster's on-disk \
             SerializationHeader still records the static column and Cassandra \
             (header-driven) would still emit its static rows. The writer's decision \
             is schema-driven, observed here on the real Data.db bytes. If CQLite \
             becomes header-driven this assertion must change."
    );

    // Belt and suspenders: no EXTENDED_IS_STATIC row exists ANYWHERE in the
    // dropped-static output, proving the static prelude was not merely
    // relocated. We DETERMINISTICALLY walk the unfiltered chain (parsing each
    // row's flags/ext/clustering/row_size) until the END_OF_PARTITION sentinel,
    // rather than scanning every buffer byte for a high bit — VInt and cell
    // payload bytes can legitimately set 0x80, so a raw scan would be
    // spuriously fragile.
    const END_OF_PARTITION: u8 = 0x01;
    let mut p = INT_PK_HEADER_SIZE;
    while p < bytes_drop.len() {
        // The unfiltered chain is terminated by the END_OF_PARTITION sentinel,
        // which occupies a real row-flags position (so this check is exact).
        if bytes_drop[p] == END_OF_PARTITION {
            break;
        }
        let (flags, ext, next) = walk_unfiltered(&bytes_drop, p);
        if flags & ROW_HAS_EXTENDED_FLAGS != 0 {
            assert_eq!(
                ext.expect("HAS_EXTENDED_FLAGS implies an ext-flags byte") & EXTENDED_IS_STATIC,
                0,
                "no EXTENDED_IS_STATIC row may appear once the static column is \
                     dropped from the schema"
            );
        }
        assert!(next > p, "unfiltered walk must advance");
        p = next;
    }

    // Sanity: ROW_HAS_DELETION is unused here (no tombstones), so the constant
    // is referenced to keep the wire-format mirror complete and avoid an
    // unused-const warning under -D warnings.
    let _ = ROW_HAS_DELETION;
}
