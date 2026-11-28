//! Real-run `KWayMerger` reconcile tests (split out of `merge/mod.rs` `tests`, #4658).

use super::*;
use crate::storage::write_engine::mutation::DecoratedKey;

#[test]
fn test_real_merger_delete_wins_at_equal_timestamp() {
    // Issue #498: at EQUAL timestamp, a Delete (tombstone) must beat a Live
    // row regardless of file recency (Cassandra `Cells#reconcile`).
    //
    // We drive the REAL merger entry point (`merge_partition_rows`) with two
    // entries that share the SAME clustering key and the SAME timestamp:
    //   - A: Live, run_index 0  (the NEWER file — would win a run_index tiebreak)
    //   - B: Delete, run_index 1 (the OLDER file)
    //
    // The pre-fix merger sorted equal-timestamp ties by run_index only, so the
    // live row (run_index 0) would win and survive. With the fix the tombstone
    // wins. This test therefore FAILS if the tiebreak reverts to run_index.
    use crate::schema::{Column, KeyColumn, TableSchema};
    use std::collections::HashMap;

    let schema = TableSchema {
        keyspace: "reconcile_ks".to_string(),
        table: "reconcile_tbl".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![Column {
            name: "value".to_string(),
            data_type: "text".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        }],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    const EQUAL_TS: i64 = 1_700_000_000_000_000;

    let partition_key = DecoratedKey::new(100, vec![0, 0, 0, 1]);

    // A = Live, in the NEWER file (run_index 0).
    let live_entry = MergeEntry::new(
        0,
        partition_key.clone(),
        None,
        EQUAL_TS,
        RowData::Live {
            cells: vec![CellData {
                column: "value".to_string(),
                value: Value::text("survivor-if-buggy".to_string()),
                timestamp: EQUAL_TS,
                ttl: None,
                cell_path: None,
                local_deletion_time: None,
                is_complex_element: false,
                is_deleted: false,
                has_empty_value: false,
            }],
        },
    );

    // B = Delete (row tombstone), in the OLDER file (run_index 1).
    let tombstone_entry = MergeEntry::new(
        1,
        partition_key.clone(),
        None,
        EQUAL_TS,
        RowData::Tombstone {
            deletion_time: EQUAL_TS,
            local_deletion_time: 2_000_000,
        },
    );

    let mut merger = KWayMerger {
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
    };

    // Drive the real merger. Order the input so the live (newer-file) entry is
    // first — pre-fix this is exactly the entry that wins by run_index.
    let merged = merger
        .merge_partition_rows(vec![live_entry, tombstone_entry])
        .expect("merge_partition_rows must not fail");

    assert_eq!(merged.len(), 1, "one clustering key => one merged winner");

    assert!(
        matches!(merged[0].row_data, RowData::Tombstone { .. }),
        "At equal timestamp the tombstone must win even though the live row is in \
             the newer file (run_index 0). Got a live row => the equal-ts tiebreak \
             reverted to run_index (Issue #498 regression)."
    );
}

#[test]
fn test_real_merger_disjoint_columns_survive_compaction() {
    // Issue #533: when two SSTables share the same (pk, ck) but carry DISJOINT
    // columns, per-cell reconcile must keep cells from BOTH. The pre-fix merger
    // picked one whole winning row and DROPPED the loser's columns.
    //
    //   A (run_index 1, ts=100): {name: "alice"}
    //   B (run_index 0, ts=200): {score: 42}
    //
    // Cassandra `Cells#reconcile` => {name: "alice", score: 42}.
    // The old whole-row-wins code returned only {score: 42} (name LOST).
    use crate::schema::{ClusteringColumn, Column, KeyColumn, TableSchema};
    use std::collections::HashMap;

    let schema = TableSchema {
        keyspace: "disjoint_ks".to_string(),
        table: "disjoint_tbl".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: Default::default(),
        }],
        columns: vec![
            Column {
                name: "name".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "score".to_string(),
                data_type: "int".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let partition_key = DecoratedKey::new(100, vec![0, 0, 0, 1]);
    let ck = ClusteringKey {
        columns: vec![("ck".to_string(), Value::Integer(1))],
    };

    // A: older file (run_index 1), only `name` at ts=100.
    let entry_a = MergeEntry::new(
        1,
        partition_key.clone(),
        Some(ck.clone()),
        100,
        RowData::Live {
            cells: vec![CellData {
                column: "name".to_string(),
                value: Value::text("alice".to_string()),
                timestamp: 100,
                ttl: None,
                cell_path: None,
                local_deletion_time: None,
                is_complex_element: false,
                is_deleted: false,
                has_empty_value: false,
            }],
        },
    );

    // B: newer file (run_index 0), only `score` at ts=200.
    let entry_b = MergeEntry::new(
        0,
        partition_key.clone(),
        Some(ck.clone()),
        200,
        RowData::Live {
            cells: vec![CellData {
                column: "score".to_string(),
                value: Value::Integer(42),
                timestamp: 200,
                ttl: None,
                cell_path: None,
                local_deletion_time: None,
                is_complex_element: false,
                is_deleted: false,
                has_empty_value: false,
            }],
        },
    );

    let mut merger = KWayMerger {
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
    };

    // Pass in heap-routing order (run_index ascending): B then A.
    let merged = merger
        .merge_partition_rows(vec![entry_b, entry_a])
        .expect("merge_partition_rows must not fail");

    assert_eq!(merged.len(), 1, "one clustering key => one merged row");

    let cells = match &merged[0].row_data {
        RowData::Live { cells } => cells,
        other => panic!("expected a Live merged row, got {:?}", other),
    };

    let name = cells.iter().find(|c| c.column == "name");
    let score = cells.iter().find(|c| c.column == "score");

    assert!(
        name.is_some(),
        "disjoint column `name` from the older file was DROPPED — per-cell \
             reconcile regression (Issue #533). Old whole-row-wins code fails here."
    );
    assert!(
        score.is_some(),
        "disjoint column `score` from the newer file is missing"
    );
    assert_eq!(
        name.unwrap().value,
        Value::text("alice".to_string()),
        "`name` must carry A's value"
    );
    assert_eq!(
        score.unwrap().value,
        Value::Integer(42),
        "`score` must carry B's value"
    );

    // Row timestamp must be the max surviving cell timestamp.
    assert_eq!(
        merged[0].timestamp, 200,
        "merged row timestamp must be the max surviving cell timestamp"
    );
}

#[test]
fn test_real_merger_cell_tombstone_beats_live_at_equal_timestamp() {
    // Issue #533/#498 (per cell): when two SSTables write the SAME column at the
    // SAME timestamp, a cell tombstone (Delete) must beat the live value,
    // independent of file recency.
    //
    //   A (run_index 0, NEWER file, ts=100): {score: 42}            (live)
    //   B (run_index 1, OLDER file, ts=100): {score: <cell tombstone>}
    //
    // Cassandra `Cells#reconcile` => score is deleted. The adversarial part: A is
    // the newer file, so a recency-only tiebreak would wrongly keep the live 42.
    use crate::schema::{ClusteringColumn, Column, KeyColumn, TableSchema};
    use crate::types::{TombstoneInfo, TombstoneType};
    use std::collections::HashMap;

    let schema = TableSchema {
        keyspace: "ct_ks".to_string(),
        table: "ct_tbl".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: Default::default(),
        }],
        columns: vec![Column {
            name: "score".to_string(),
            data_type: "int".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        }],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let partition_key = DecoratedKey::new(100, vec![0, 0, 0, 1]);
    let ck = ClusteringKey {
        columns: vec![("ck".to_string(), Value::Integer(1))],
    };

    // A: newer file (run_index 0), live `score` = 42 at ts=100.
    let entry_a = MergeEntry::new(
        0,
        partition_key.clone(),
        Some(ck.clone()),
        100,
        RowData::Live {
            cells: vec![CellData {
                column: "score".to_string(),
                value: Value::Integer(42),
                timestamp: 100,
                ttl: None,
                cell_path: None,
                local_deletion_time: None,
                is_complex_element: false,
                is_deleted: false,
                has_empty_value: false,
            }],
        },
    );

    // B: older file (run_index 1), cell tombstone on `score` at the SAME ts=100.
    let entry_b = MergeEntry::new(
        1,
        partition_key.clone(),
        Some(ck.clone()),
        100,
        RowData::Live {
            cells: vec![CellData {
                column: "score".to_string(),
                value: Value::Tombstone(Box::new(TombstoneInfo {
                    deletion_time: 100,
                    tombstone_type: TombstoneType::CellTombstone,
                    local_deletion_time: 0,
                    ttl: None,
                    range_start: None,
                    range_end: None,
                })),
                timestamp: 100,
                ttl: None,
                cell_path: None,
                local_deletion_time: None,
                is_complex_element: false,
                is_deleted: false,
                has_empty_value: false,
            }],
        },
    );

    let mut merger = KWayMerger {
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
    };

    // Heap-routing order (run_index ascending): A then B.
    let merged = merger
        .merge_partition_rows(vec![entry_a, entry_b])
        .expect("merge_partition_rows must not fail");

    assert_eq!(merged.len(), 1, "one clustering key => one merged row");

    let cells = match &merged[0].row_data {
        RowData::Live { cells } => cells,
        other => panic!("expected a Live merged row, got {:?}", other),
    };
    let score = cells
        .iter()
        .find(|c| c.column == "score")
        .expect("score cell must be present (as a tombstone)");

    assert!(
        matches!(
            score.value,
            Value::Tombstone(ref info) if info.tombstone_type == TombstoneType::CellTombstone
        ),
        "at equal ts the cell tombstone must win over the live value (got {:?}) — \
             a recency-only tiebreak would have kept the newer file's live 42 (#498 per cell)",
        score.value
    );
}

#[test]
fn test_real_merger_value_tiebreak_converges_with_cassandra() {
    // Issue #4530 (was the cursor-compaction #4/#21 DIVERGENCE test): on an equal-timestamp
    // tie between two LIVE cells of the SAME column, Cassandra's `Cells.resolveRegular`
    // (cassandra-5.0.8 `Cells.java:128`, `compareValues(left, right) >= 0 ? left : right`)
    // keeps the cell whose raw value bytes are GREATER (unsigned lexicographic). File / run
    // order is NOT consulted, so the winner is the same in either arrival order.
    //
    // CQLite now implements that rule (`reconcile_rules::cell_wins`), so this test
    // asserts CONVERGENCE in both orders.
    //
    // Fixture (same pk, no clustering, SAME timestamp, DIFFERENT values):
    //   A (run_index 0, NEWER file): {v: "apple"}    raw bytes 0x61 70 70 6C 65
    //   B (run_index 1, OLDER file): {v: "banana"}   raw bytes 0x62 ...  (GREATER)
    //
    // Cassandra keeps "banana" whichever file CQLite sees first. The previous first-seen
    // rule kept "apple" (the newer file).
    use crate::schema::{Column, KeyColumn, TableSchema};
    use std::collections::HashMap;

    let schema = TableSchema {
        keyspace: "tiebreak_ks".to_string(),
        table: "tiebreak_tbl".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
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

    const EQUAL_TS: i64 = 1_700_000_000_000_000;
    let partition_key = DecoratedKey::new(100, vec![0, 0, 0, 1]);

    // For CQL `text`, the raw cell value Cassandra compares is exactly the UTF-8
    // bytes. "banana" > "apple" unsigned-lexicographically (0x62 > 0x61).
    let newer_file_value = "apple"; // run_index 0 (newer)
    let older_file_value = "banana"; // run_index 1 (older), greater raw bytes
    assert!(
        older_file_value.as_bytes() > newer_file_value.as_bytes(),
        "fixture invariant: the older file must hold the lexicographically GREATER \
             raw value, so the two tie-break rules pick different winners"
    );

    // The value Cassandra's rule (#4/#21) would keep: greater raw value bytes.
    let cassandra_winner = if older_file_value.as_bytes() > newer_file_value.as_bytes() {
        older_file_value
    } else {
        newer_file_value
    };

    let entry_newer = MergeEntry::new(
        0, // newer file
        partition_key.clone(),
        None,
        EQUAL_TS,
        RowData::Live {
            cells: vec![CellData {
                column: "v".to_string(),
                value: Value::text(newer_file_value.to_string()),
                timestamp: EQUAL_TS,
                ttl: None,
                cell_path: None,
                local_deletion_time: None,
                is_complex_element: false,
                is_deleted: false,
                has_empty_value: false,
            }],
        },
    );

    let entry_older = MergeEntry::new(
        1, // older file
        partition_key.clone(),
        None,
        EQUAL_TS,
        RowData::Live {
            cells: vec![CellData {
                column: "v".to_string(),
                value: Value::text(older_file_value.to_string()),
                timestamp: EQUAL_TS,
                ttl: None,
                cell_path: None,
                local_deletion_time: None,
                is_complex_element: false,
                is_deleted: false,
                has_empty_value: false,
            }],
        },
    );

    let mut merger = KWayMerger {
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
    };

    // Heap-routing order (run_index ascending): newer file first — exactly what
    // the real merge heap yields for equal (pk, ck).
    let merged = merger
        .merge_partition_rows(vec![entry_newer, entry_older])
        .expect("merge_partition_rows must not fail");

    assert_eq!(merged.len(), 1, "one (pk, ck) group => one merged winner");

    let cells = match &merged[0].row_data {
        RowData::Live { cells } => cells,
        other => panic!("expected a Live merged row, got {:?}", other),
    };
    let surviving = match &cells
        .iter()
        .find(|c| c.column == "v")
        .expect("column `v` must survive")
        .value
    {
        Value::Text(s) => String::from_utf8_lossy(s).into_owned(),
        other => panic!("expected Text value, got {:?}", other),
    };

    // The Cassandra winner (greater raw value bytes) survives, from the OLDER file.
    assert_eq!(
        surviving, cassandra_winner,
        "Cells.resolveRegular keeps the greater raw value {:?}; CQLite kept {:?}",
        cassandra_winner, surviving
    );
    assert_ne!(
        surviving, newer_file_value,
        "first-seen (newer file) must no longer win"
    );
}

#[test]
fn test_real_merger_same_column_conflict_resolves_by_timestamp() {
    // Issue #533: when both SSTables write the SAME column, the higher-timestamp
    // value wins (last-write-wins), but disjoint columns still survive.
    //
    //   A (run_index 1, ts=100): {name: "old", extra: "a-only"}
    //   B (run_index 0, ts=200): {name: "new"}
    // => {name: "new" (ts=200 wins), extra: "a-only" (survives)}
    use crate::schema::{Column, KeyColumn, TableSchema};
    use std::collections::HashMap;

    let schema = TableSchema {
        keyspace: "conflict_ks".to_string(),
        table: "conflict_tbl".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "name".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "extra".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let partition_key = DecoratedKey::new(100, vec![0, 0, 0, 1]);

    let entry_a = MergeEntry::new(
        1,
        partition_key.clone(),
        None,
        100,
        RowData::Live {
            cells: vec![
                CellData {
                    column: "name".to_string(),
                    value: Value::text("old".to_string()),
                    timestamp: 100,
                    ttl: None,
                    cell_path: None,
                    local_deletion_time: None,
                    is_complex_element: false,
                    is_deleted: false,
                    has_empty_value: false,
                },
                CellData {
                    column: "extra".to_string(),
                    value: Value::text("a-only".to_string()),
                    timestamp: 100,
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

    let entry_b = MergeEntry::new(
        0,
        partition_key.clone(),
        None,
        200,
        RowData::Live {
            cells: vec![CellData {
                column: "name".to_string(),
                value: Value::text("new".to_string()),
                timestamp: 200,
                ttl: None,
                cell_path: None,
                local_deletion_time: None,
                is_complex_element: false,
                is_deleted: false,
                has_empty_value: false,
            }],
        },
    );

    let mut merger = KWayMerger {
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
    };

    let merged = merger
        .merge_partition_rows(vec![entry_b, entry_a])
        .expect("merge_partition_rows must not fail");

    assert_eq!(merged.len(), 1);
    let cells = match &merged[0].row_data {
        RowData::Live { cells } => cells,
        other => panic!("expected Live, got {:?}", other),
    };

    let name = cells
        .iter()
        .find(|c| c.column == "name")
        .expect("name present");
    let extra = cells
        .iter()
        .find(|c| c.column == "extra")
        .expect("extra (disjoint) must survive");

    assert_eq!(
        name.value,
        Value::text("new".to_string()),
        "same-column conflict must resolve to the higher-timestamp value"
    );
    assert_eq!(
        extra.value,
        Value::text("a-only".to_string()),
        "disjoint column from the older file must survive the conflict merge"
    );
}

#[test]
fn test_real_merger_row_tombstone_shadows_old_cells_keeps_new() {
    // Issue #533 / #505: a row tombstone shadows cells with ts <= row_del but
    // a cell written strictly AFTER the tombstone survives.
    //
    //   A (ts=100): {name: "old"}          -> 100 <= 200 row_del => shadowed
    //   B (ts=200, row tombstone)
    //   C (ts=300): {score: 7}             -> 300 > 200            => survives
    use crate::schema::{Column, KeyColumn, TableSchema};
    use std::collections::HashMap;

    let schema = TableSchema {
        keyspace: "shadow_ks".to_string(),
        table: "shadow_tbl".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "name".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "score".to_string(),
                data_type: "int".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let pk = DecoratedKey::new(100, vec![0, 0, 0, 1]);

    let entry_a = MergeEntry::new(
        2,
        pk.clone(),
        None,
        100,
        RowData::Live {
            cells: vec![CellData {
                column: "name".to_string(),
                value: Value::text("old".to_string()),
                timestamp: 100,
                ttl: None,
                cell_path: None,
                local_deletion_time: None,
                is_complex_element: false,
                is_deleted: false,
                has_empty_value: false,
            }],
        },
    );
    let entry_b = MergeEntry::new(
        1,
        pk.clone(),
        None,
        200,
        RowData::Tombstone {
            deletion_time: 200,
            local_deletion_time: 0,
        },
    );
    let entry_c = MergeEntry::new(
        0,
        pk.clone(),
        None,
        300,
        RowData::Live {
            cells: vec![CellData {
                column: "score".to_string(),
                value: Value::Integer(7),
                timestamp: 300,
                ttl: None,
                cell_path: None,
                local_deletion_time: None,
                is_complex_element: false,
                is_deleted: false,
                has_empty_value: false,
            }],
        },
    );

    let mut merger = KWayMerger {
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
    };

    let merged = merger
        .merge_partition_rows(vec![entry_c, entry_b, entry_a])
        .expect("merge must not fail");

    assert_eq!(merged.len(), 1);
    let cells = match &merged[0].row_data {
        RowData::Live { cells } => cells,
        other => panic!(
            "expected Live (score survives the tombstone), got {:?}",
            other
        ),
    };

    assert!(
        cells.iter().all(|c| c.column != "name"),
        "`name` (ts=100 <= row_del=200) must be shadowed by the row tombstone"
    );
    let score = cells
        .iter()
        .find(|c| c.column == "score")
        .expect("`score` (ts=300 > row_del=200) must survive the row tombstone");
    assert_eq!(score.value, Value::Integer(7));
}

#[test]
fn test_real_merger_row_tombstone_only_emits_tombstone() {
    // When every cell is shadowed by a row tombstone (no later writes), the
    // merger must emit a Tombstone entry so the row stays deleted downstream
    // (preserves #505/#498 absence semantics).
    use crate::schema::{Column, KeyColumn, TableSchema};
    use std::collections::HashMap;

    let schema = TableSchema {
        keyspace: "ts_only_ks".to_string(),
        table: "ts_only_tbl".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
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
    };

    let pk = DecoratedKey::new(100, vec![0, 0, 0, 1]);

    let live = MergeEntry::new(
        1,
        pk.clone(),
        None,
        100,
        RowData::Live {
            cells: vec![CellData {
                column: "name".to_string(),
                value: Value::text("doomed".to_string()),
                timestamp: 100,
                ttl: None,
                cell_path: None,
                local_deletion_time: None,
                is_complex_element: false,
                is_deleted: false,
                has_empty_value: false,
            }],
        },
    );
    let tomb = MergeEntry::new(
        0,
        pk.clone(),
        None,
        300,
        RowData::Tombstone {
            deletion_time: 300,
            local_deletion_time: 0,
        },
    );

    let mut merger = KWayMerger {
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
    };

    let merged = merger
        .merge_partition_rows(vec![tomb, live])
        .expect("merge must not fail");

    assert_eq!(merged.len(), 1);
    match &merged[0].row_data {
        RowData::Tombstone { deletion_time, .. } => {
            assert_eq!(*deletion_time, 300, "tombstone deletion_time preserved");
        }
        other => panic!("expected a Tombstone entry, got {:?}", other),
    }
}
