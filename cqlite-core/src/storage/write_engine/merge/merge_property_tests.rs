//! Moved out of `merge/mod.rs` unchanged (#4658); the module keeps its name via `#[path]`.

use super::*;
use proptest::prelude::*;
use std::collections::HashMap;

// ─── Fixed "wall clock" used in all TTL expiry tests ─────────────────────
// Unix seconds; cells with local_deletion_time < MERGE_TIME_SECS are expired.
const MERGE_TIME_SECS: i32 = 1_000;

// ─── Cell operation model ─────────────────────────────────────────────────

/// The three kinds of cell operations that a compaction must resolve.
#[derive(Debug, Clone)]
enum CellOp {
    /// A live write: column <- value, recorded at `timestamp`.
    /// When `local_deletion_time` is Some(t) it is an expiring cell; the
    /// cell is considered dead when `t < MERGE_TIME_SECS`.
    Write {
        timestamp: i64,
        local_deletion_time: Option<i32>,
    },
    /// A cell tombstone (DELETE column): column is dead at `timestamp`.
    Delete { timestamp: i64 },
    /// A range tombstone covering the inclusive integer range
    /// `[start_ck, end_ck]`. Any row whose clustering key integer falls
    /// within the range and whose write-timestamp <= `marked_for_delete_at`
    /// is suppressed.
    RangeTombstone {
        start_ck: u8,
        end_ck: u8,
        marked_for_delete_at: i64,
    },
}

/// A single entry in the randomised cell stream.
///
/// We work with small integer partition/clustering/column spaces so
/// collisions occur frequently and the interesting merge cases arise.
#[derive(Debug, Clone)]
struct CellInput {
    /// 0..4
    partition: u8,
    /// 0..4
    clustering: u8,
    /// 0..3
    column: u8,
    op: CellOp,
}

// ─── Key type for the merged output map ──────────────────────────────────

/// (partition, clustering, column) triple identifying a unique cell slot.
type CellKey = (u8, u8, u8);

/// What the reference merge produces for a cell slot.
#[derive(Debug, Clone, PartialEq, Eq)]
enum MergedCell {
    /// The cell is alive with the given write timestamp.
    Live { timestamp: i64 },
    /// The cell is a tombstone (deleted) at the given timestamp.
    Dead { timestamp: i64 },
}

// ─── Reference implementation ─────────────────────────────────────────────

/// Reference merge over a flat cell-stream, applying full Cassandra rules.
///
/// Rules (applied in order):
/// 1. Per (partition, clustering, column), keep the op with the highest
///    `timestamp`. Ties: Delete wins over Write (Cassandra reconcile).
/// 2. A `RangeTombstone` with `marked_for_delete_at >= cell.timestamp`
///    covering a clustering key suppresses the live cell in that slot.
/// 3. A `Write` whose `local_deletion_time < MERGE_TIME_SECS` (TTL expired)
///    is dropped from the output even if it has the highest timestamp.
fn reference_merge(inputs: &[CellInput]) -> HashMap<CellKey, MergedCell> {
    // ── Step 1: per-slot LWW ──────────────────────────────────────────────
    let mut per_slot: HashMap<CellKey, MergedCell> = HashMap::new();

    // Collect range tombstones grouped by (partition, clustering range).
    let mut range_tombstones: Vec<CellInput> = Vec::new();

    for ci in inputs {
        match &ci.op {
            CellOp::RangeTombstone { .. } => {
                range_tombstones.push(ci.clone());
            }
            CellOp::Write {
                timestamp,
                local_deletion_time,
            } => {
                // TTL expiry: drop the write if its local_deletion_time has passed.
                if local_deletion_time
                    .map(|ldt| ldt < MERGE_TIME_SECS)
                    .unwrap_or(false)
                {
                    // Expired: treat as if this write never happened.
                    continue;
                }
                let key = (ci.partition, ci.clustering, ci.column);
                let candidate = MergedCell::Live {
                    timestamp: *timestamp,
                };
                per_slot
                    .entry(key)
                    .and_modify(|existing| {
                        match existing {
                            MergedCell::Live { timestamp: ex_ts } => {
                                if *timestamp > *ex_ts {
                                    *existing = candidate.clone();
                                }
                            }
                            MergedCell::Dead { timestamp: ex_ts } => {
                                // Dead wins over a live cell at the same timestamp;
                                // only replace if the write is strictly newer.
                                if *timestamp > *ex_ts {
                                    *existing = candidate.clone();
                                }
                            }
                        }
                    })
                    .or_insert(candidate);
            }
            CellOp::Delete { timestamp } => {
                let key = (ci.partition, ci.clustering, ci.column);
                let candidate = MergedCell::Dead {
                    timestamp: *timestamp,
                };
                per_slot
                    .entry(key)
                    .and_modify(|existing| {
                        match existing {
                            MergedCell::Live { timestamp: ex_ts } => {
                                if *timestamp >= *ex_ts {
                                    // Delete wins at equal timestamp (Cassandra rule).
                                    *existing = candidate.clone();
                                }
                            }
                            MergedCell::Dead { timestamp: ex_ts } => {
                                if *timestamp > *ex_ts {
                                    *existing = candidate.clone();
                                }
                            }
                        }
                    })
                    .or_insert(candidate);
            }
        }
    }

    // ── Step 2: apply range tombstones ────────────────────────────────────
    // A range tombstone suppresses a live cell when:
    //   - partition matches
    //   - clustering key is within [start_ck, end_ck]
    //   - marked_for_delete_at >= cell write timestamp
    per_slot.retain(|&(pk, ck, _col), cell| {
        for rt in &range_tombstones {
            if rt.partition != pk {
                continue;
            }
            if let CellOp::RangeTombstone {
                start_ck,
                end_ck,
                marked_for_delete_at,
            } = rt.op
            {
                if ck >= start_ck && ck <= end_ck {
                    if let MergedCell::Live { timestamp } = cell {
                        if marked_for_delete_at >= *timestamp {
                            return false; // suppressed
                        }
                    }
                }
            }
        }
        true
    });

    // Dead cells (tombstones) are kept in the output so callers can verify
    // they appear rather than a live cell with a lower timestamp.
    per_slot
}

// ─── Proptest strategies ──────────────────────────────────────────────────

fn arb_timestamp() -> impl Strategy<Value = i64> {
    1i64..=20i64
}

/// local_deletion_time: sometimes None, sometimes expired (<MERGE_TIME_SECS),
/// sometimes live (>=MERGE_TIME_SECS).
fn arb_local_deletion_time() -> impl Strategy<Value = Option<i32>> {
    prop_oneof![
        3 => Just(None),                         // no TTL
        1 => (990i32..=999i32).prop_map(Some),   // expired TTL
        1 => (1000i32..=1010i32).prop_map(Some), // live TTL
    ]
}

fn arb_cell_op() -> impl Strategy<Value = CellOp> {
    prop_oneof![
        5 => (arb_timestamp(), arb_local_deletion_time())
                .prop_map(|(ts, ldt)| CellOp::Write {
                    timestamp: ts,
                    local_deletion_time: ldt,
                }),
        3 => arb_timestamp().prop_map(|ts| CellOp::Delete { timestamp: ts }),
        2 => (0u8..=3u8, 0u8..=3u8, arb_timestamp()).prop_map(|(s, e, ts)| {
                let (start_ck, end_ck) = if s <= e { (s, e) } else { (e, s) };
                CellOp::RangeTombstone {
                    start_ck,
                    end_ck,
                    marked_for_delete_at: ts,
                }
            }),
    ]
}

fn arb_cell_input() -> impl Strategy<Value = CellInput> {
    (0u8..4u8, 0u8..4u8, 0u8..3u8, arb_cell_op()).prop_map(|(partition, clustering, column, op)| {
        CellInput {
            partition,
            clustering,
            column,
            op,
        }
    })
}

fn arb_cell_stream() -> impl Strategy<Value = Vec<CellInput>> {
    prop::collection::vec(arb_cell_input(), 4..=32)
}

// ─── Helper: sort merged output for stable comparison ─────────────────────
fn sorted_keys(m: &HashMap<CellKey, MergedCell>) -> Vec<(CellKey, MergedCell)> {
    let mut v: Vec<_> = m.iter().map(|(&k, v)| (k, v.clone())).collect();
    v.sort_by_key(|(k, _)| *k);
    v
}

// ─── Deterministic unit tests for reference semantics ────────────────────

#[test]
fn ref_tombstone_shadows_earlier_write() {
    // Write at ts=5, then Delete at ts=10 => cell must be Dead(10).
    let inputs = vec![
        CellInput {
            partition: 0,
            clustering: 0,
            column: 0,
            op: CellOp::Write {
                timestamp: 5,
                local_deletion_time: None,
            },
        },
        CellInput {
            partition: 0,
            clustering: 0,
            column: 0,
            op: CellOp::Delete { timestamp: 10 },
        },
    ];
    let result = reference_merge(&inputs);
    assert_eq!(
        result.get(&(0, 0, 0)),
        Some(&MergedCell::Dead { timestamp: 10 }),
        "Delete(ts=10) must shadow Write(ts=5)"
    );
}

#[test]
fn ref_write_not_shadowed_by_older_tombstone() {
    // Write at ts=10, Delete at ts=5 => cell must be Live(10).
    let inputs = vec![
        CellInput {
            partition: 0,
            clustering: 0,
            column: 0,
            op: CellOp::Write {
                timestamp: 10,
                local_deletion_time: None,
            },
        },
        CellInput {
            partition: 0,
            clustering: 0,
            column: 0,
            op: CellOp::Delete { timestamp: 5 },
        },
    ];
    let result = reference_merge(&inputs);
    assert_eq!(
        result.get(&(0, 0, 0)),
        Some(&MergedCell::Live { timestamp: 10 }),
        "Write(ts=10) must win over Delete(ts=5)"
    );
}

#[test]
fn ref_delete_wins_at_equal_timestamp() {
    // Write at ts=5, Delete at ts=5 => Delete must win (Cassandra reconcile).
    let inputs = vec![
        CellInput {
            partition: 0,
            clustering: 0,
            column: 0,
            op: CellOp::Write {
                timestamp: 5,
                local_deletion_time: None,
            },
        },
        CellInput {
            partition: 0,
            clustering: 0,
            column: 0,
            op: CellOp::Delete { timestamp: 5 },
        },
    ];
    let result = reference_merge(&inputs);
    assert_eq!(
        result.get(&(0, 0, 0)),
        Some(&MergedCell::Dead { timestamp: 5 }),
        "Delete must win at equal timestamp (Cassandra reconcile rule)"
    );
}

#[test]
fn ref_expired_ttl_drops_cell() {
    // Write at ts=5 with local_deletion_time=500 (< MERGE_TIME_SECS=1000).
    // No other ops => cell should be absent from merged output.
    let inputs = vec![CellInput {
        partition: 0,
        clustering: 0,
        column: 0,
        op: CellOp::Write {
            timestamp: 5,
            local_deletion_time: Some(500), // expired
        },
    }];
    let result = reference_merge(&inputs);
    assert!(
        !result.contains_key(&(0, 0, 0)),
        "Expired TTL cell must be absent from merged output"
    );
}

#[test]
fn ref_live_ttl_keeps_cell() {
    // Write at ts=5 with local_deletion_time=1500 (>= MERGE_TIME_SECS=1000).
    // Cell should still be present.
    let inputs = vec![CellInput {
        partition: 0,
        clustering: 0,
        column: 0,
        op: CellOp::Write {
            timestamp: 5,
            local_deletion_time: Some(1500), // not expired
        },
    }];
    let result = reference_merge(&inputs);
    assert_eq!(
        result.get(&(0, 0, 0)),
        Some(&MergedCell::Live { timestamp: 5 }),
        "Non-expired TTL cell must be present"
    );
}

#[test]
fn ref_range_tombstone_suppresses_row_in_range() {
    // Write at ts=5 for clustering key 2 in partition 0.
    // RangeTombstone covering [0, 5] with marked_for_delete_at=10 => suppressed.
    let inputs = vec![
        CellInput {
            partition: 0,
            clustering: 2,
            column: 0,
            op: CellOp::Write {
                timestamp: 5,
                local_deletion_time: None,
            },
        },
        CellInput {
            partition: 0,
            clustering: 0, // column/clustering fields ignored for RT; range is in op
            column: 0,
            op: CellOp::RangeTombstone {
                start_ck: 0,
                end_ck: 5,
                marked_for_delete_at: 10,
            },
        },
    ];
    let result = reference_merge(&inputs);
    assert!(
        !result.contains_key(&(0, 2, 0)),
        "Cell with ts=5 at clustering=2 must be suppressed by RangeTombstone(mfda=10, [0,5])"
    );
}

#[test]
fn ref_range_tombstone_does_not_suppress_newer_write() {
    // Write at ts=15, RangeTombstone with mfda=10 => not suppressed.
    let inputs = vec![
        CellInput {
            partition: 0,
            clustering: 2,
            column: 0,
            op: CellOp::Write {
                timestamp: 15,
                local_deletion_time: None,
            },
        },
        CellInput {
            partition: 0,
            clustering: 0,
            column: 0,
            op: CellOp::RangeTombstone {
                start_ck: 0,
                end_ck: 5,
                marked_for_delete_at: 10,
            },
        },
    ];
    let result = reference_merge(&inputs);
    assert_eq!(
        result.get(&(0, 2, 0)),
        Some(&MergedCell::Live { timestamp: 15 }),
        "Write(ts=15) must NOT be suppressed by RangeTombstone(mfda=10)"
    );
}

#[test]
fn ref_range_tombstone_only_applies_within_partition() {
    // Write in partition 1, RangeTombstone in partition 0 => not suppressed.
    let inputs = vec![
        CellInput {
            partition: 1,
            clustering: 2,
            column: 0,
            op: CellOp::Write {
                timestamp: 5,
                local_deletion_time: None,
            },
        },
        CellInput {
            partition: 0,
            clustering: 0,
            column: 0,
            op: CellOp::RangeTombstone {
                start_ck: 0,
                end_ck: 5,
                marked_for_delete_at: 10,
            },
        },
    ];
    let result = reference_merge(&inputs);
    assert_eq!(
        result.get(&(1, 2, 0)),
        Some(&MergedCell::Live { timestamp: 5 }),
        "RangeTombstone in partition 0 must not affect partition 1"
    );
}

// ─── Property tests ───────────────────────────────────────────────────────

proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        // Property A: Tombstone shadowing
        // After merging, for every (partition, clustering, column) cell slot, if
        // the reference says Dead(ts=T) then the highest-timestamp Delete in the
        // input stream for that slot must have timestamp T.
        #[test]
        fn prop_tombstone_shadowing_consistent(inputs in arb_cell_stream()) {
            let merged = reference_merge(&inputs);

            for (&(pk, ck, col), cell) in &merged {
                if let MergedCell::Dead { timestamp: dead_ts } = cell {
                    // Find the highest-timestamp Delete for this slot in the input.
                    let best_delete = inputs.iter()
                        .filter(|ci| ci.partition == pk && ci.clustering == ck && ci.column == col)
                        .filter_map(|ci| {
                            if let CellOp::Delete { timestamp } = ci.op {
                                Some(timestamp)
                            } else {
                                None
                            }
                        })
                        .max();

                    prop_assert!(
                        best_delete.is_some(),
                        "Dead cell at ({},{},{}) but no Delete in inputs",
                        pk, ck, col
                    );
                    prop_assert_eq!(
                        best_delete.unwrap(),
                        *dead_ts,
                        "Dead cell timestamp must equal best Delete timestamp for ({},{},{})",
                        pk, ck, col
                    );
                }
            }
        }

        // Property B: TTL expiry
        // After merging, no cell should be Live if all its Write ops are TTL-expired.
        #[test]
        fn prop_ttl_expiry_no_expired_live_cells(inputs in arb_cell_stream()) {
            let merged = reference_merge(&inputs);

            for (&(pk, ck, col), cell) in &merged {
                if let MergedCell::Live { .. } = cell {
                    // There must be at least one non-expired Write for this slot.
                    let has_live_write = inputs.iter()
                        .filter(|ci| ci.partition == pk && ci.clustering == ck && ci.column == col)
                        .any(|ci| {
                            if let CellOp::Write { local_deletion_time, .. } = &ci.op {
                                // A live write: no TTL, or TTL not expired.
                                local_deletion_time
                                    .map(|ldt| ldt >= MERGE_TIME_SECS)
                                    .unwrap_or(true)
                            } else {
                                false
                            }
                        });

                    prop_assert!(
                        has_live_write,
                        "Live cell at ({},{},{}) but all writes are expired",
                        pk, ck, col
                    );
                }
            }
        }

        // Property C: Range tombstone application
        // After merging, no Live cell should exist that is fully covered by a
        // range tombstone whose marked_for_delete_at >= the cell's write timestamp.
        #[test]
        fn prop_range_tombstone_suppresses_covered_live_cells(inputs in arb_cell_stream()) {
            let merged = reference_merge(&inputs);

            // Collect all range tombstones from the input stream.
            let range_tombstones: Vec<(u8, u8, u8, i64)> = inputs.iter()
                .filter_map(|ci| {
                    if let CellOp::RangeTombstone { start_ck, end_ck, marked_for_delete_at } = ci.op {
                        Some((ci.partition, start_ck, end_ck, marked_for_delete_at))
                    } else {
                        None
                    }
                })
                .collect();

            for (&(pk, ck, _col), cell) in &merged {
                if let MergedCell::Live { timestamp } = cell {
                    // Verify no range tombstone shadows this cell.
                    for &(rt_pk, start_ck, end_ck, mfda) in &range_tombstones {
                        if rt_pk == pk && ck >= start_ck && ck <= end_ck && mfda >= *timestamp {
                            prop_assert!(
                                false,
                                "Live cell at ({},{}) ts={} should be suppressed by \
                                 RangeTombstone(part={}, [{},{}], mfda={})",
                                pk, ck, timestamp, rt_pk, start_ck, end_ck, mfda
                            );
                        }
                    }
                }
            }
        }

        // Property D: LWW correctness
        // Every Live cell in the merged output must have a timestamp equal to
        // the maximum non-expired Write timestamp for that cell slot.
        #[test]
        fn prop_live_cell_has_max_write_timestamp(inputs in arb_cell_stream()) {
            let merged = reference_merge(&inputs);

            for (&(pk, ck, col), cell) in &merged {
                if let MergedCell::Live { timestamp: live_ts } = cell {
                    // Find the maximum non-expired Write timestamp for this slot.
                    let max_ts = inputs.iter()
                        .filter(|ci| ci.partition == pk && ci.clustering == ck && ci.column == col)
                        .filter_map(|ci| {
                            if let CellOp::Write { timestamp, local_deletion_time } = &ci.op {
                                // Only include non-expired writes.
                                let not_expired = local_deletion_time
                                    .map(|ldt| ldt >= MERGE_TIME_SECS)
                                    .unwrap_or(true);
                                if not_expired { Some(*timestamp) } else { None }
                            } else {
                                None
                            }
                        })
                        .max();

                    prop_assert_eq!(
                        max_ts,
                        Some(*live_ts),
                        "Live cell at ({},{},{}) must have max non-expired write timestamp",
                        pk, ck, col
                    );
                }
            }
        }

        // Property E: Output is deterministic (idempotent reference)
        // Calling reference_merge twice on the same input produces identical output.
        #[test]
        fn prop_reference_merge_is_deterministic(inputs in arb_cell_stream()) {
            let result_a = reference_merge(&inputs);
            let result_b = reference_merge(&inputs);
            prop_assert_eq!(
                sorted_keys(&result_a),
                sorted_keys(&result_b),
                "reference_merge must be deterministic"
            );
        }

        // Property F: Real merger LWW parity
        // For cell streams containing only non-expired Writes (no Deletes,
        // no RangeTombstones, no TTL), the real KWayMerger.merge_partition_rows
        // must agree with the reference on which row wins per clustering key.
        //
        // We drive merge_partition_rows directly with synthetic MergeEntry inputs
        // that represent the Write ops.
        #[test]
        fn prop_real_merger_lww_agrees_with_reference(
            entries in prop::collection::vec(
                // (clustering_key 0..4, run_index 0..2, timestamp 1..20)
                (0u8..4u8, 0usize..2usize, 1i64..=20i64),
                2..=12usize,
            )
        ) {
            use crate::schema::{Column, KeyColumn};
            use std::collections::HashMap as SchemaMap;

            let schema = TableSchema {
                keyspace: "prop_test_ks".to_string(),
                table: "prop_test_table".to_string(),
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
                comments: SchemaMap::new(),
                dropped_columns: SchemaMap::new(),
            };

            // Build MergeEntry stream — one per (ck, run_index, timestamp) tuple.
            // All entries share the same partition.
            let partition_key = DecoratedKey::new(100, vec![0, 0, 0, 1]);
            let merge_entries: Vec<MergeEntry> = entries.iter().map(|&(ck, run_index, ts)| {
                let ck_key = ClusteringKey {
                    columns: vec![("ck".to_string(), Value::TinyInt(ck as i8))],
                };
                MergeEntry::new(
                    run_index,
                    partition_key.clone(),
                    Some(ck_key),
                    ts,
                    RowData::Live {
                        cells: vec![CellData {
                            column: "value".to_string(),
                            value: Value::Integer(ts as i32),
                            timestamp: ts,
                            ttl: None,
                            cell_path: None,
                            local_deletion_time: None,
                                                    is_complex_element: false,
                            is_deleted: false,
                            has_empty_value: false,
}],
                    },
                )
            }).collect();

            // Drive the real merger.
            let mut merger = KWayMerger {
                runs: vec![],
                heap: std::collections::BinaryHeap::new(),
                current_partition: None,
                gc_before_secs: None,
                now_secs: None,
                purge_safe: false,
                max_purgeable_timestamp: None,
                schema: schema.clone(),
                schema_arc: std::sync::Arc::new(schema.clone()),
                _egress_slot: None,
                trace: NoTrace,
            };
            let real_merged = merger.merge_partition_rows(merge_entries.clone())
                .expect("merge_partition_rows must not fail");

            // Build the reference result: per clustering-key int, highest timestamp wins.
            // (run_index as tie-breaker: lower run_index wins at equal ts — same as merger)
            let mut ref_map: HashMap<u8, (i64, usize)> = HashMap::new();
            for &(ck, run_index, ts) in &entries {
                ref_map.entry(ck)
                    .and_modify(|(best_ts, best_run)| {
                        if ts > *best_ts || (ts == *best_ts && run_index < *best_run) {
                            *best_ts = ts;
                            *best_run = run_index;
                        }
                    })
                    .or_insert((ts, run_index));
            }

            // Verify each winner in the real output matches the reference.
            prop_assert_eq!(
                real_merged.len(),
                ref_map.len(),
                "real merger output row count must match reference"
            );

            for entry in &real_merged {
                let ck_byte = match entry.clustering_key.as_ref()
                    .and_then(|ck| ck.columns.first())
                    .map(|(_, v)| v)
                {
                    Some(Value::TinyInt(b)) => *b as u8,
                    _ => {
                        prop_assert!(false, "unexpected clustering key value");
                        unreachable!()
                    }
                };

                let (ref_ts, _ref_run) = ref_map[&ck_byte];
                prop_assert_eq!(
                    entry.timestamp,
                    ref_ts,
                    "real merger winner timestamp must match reference for ck={}",
                    ck_byte
                );
            }
        }

        // Property G: Tombstone wins over live row at same clustering key in real merger
        // When a Tombstone and a Live row have the same clustering key, the one with
        // the higher timestamp must win — and the real merger must reflect this.
        #[test]
        fn prop_real_merger_tombstone_vs_live(
            ts_write in 1i64..=10i64,
            ts_delete in 1i64..=20i64,
        ) {
            use crate::schema::{Column, KeyColumn};
            use std::collections::HashMap as SchemaMap;

            let schema = TableSchema {
                keyspace: "prop_test_ks".to_string(),
                table: "prop_test_table".to_string(),
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
                comments: SchemaMap::new(),
                dropped_columns: SchemaMap::new(),
            };

            let partition_key = DecoratedKey::new(100, vec![0, 0, 0, 1]);
            let ck = ClusteringKey {
                columns: vec![("ck".to_string(), Value::TinyInt(0))],
            };

            // Deliberately give the LIVE row the NEWER file (run_index 0) and the
            // tombstone the OLDER file (run_index 1). A run_index-only tiebreak at
            // equal timestamp would wrongly pick the live row; the Cassandra
            // liveness rule must pick the tombstone regardless of file recency.
            let live_entry = MergeEntry::new(
                0, // run_index 0 = newer file
                partition_key.clone(),
                Some(ck.clone()),
                ts_write,
                RowData::Live {
                    cells: vec![CellData {
                        column: "value".to_string(),
                        value: Value::Integer(42),
                        timestamp: ts_write,
                        ttl: None,
                        cell_path: None,
                        local_deletion_time: None,
                                            is_complex_element: false,
                        is_deleted: false,
                        has_empty_value: false,
}],
                },
            );
            let tombstone_entry = MergeEntry::new(
                1, // run_index 1 = older file
                partition_key.clone(),
                Some(ck.clone()),
                ts_delete,
                RowData::Tombstone {
                    deletion_time: ts_delete,
                    local_deletion_time: 2000,
                },
            );

            let mut merger = KWayMerger {
                runs: vec![],
                heap: std::collections::BinaryHeap::new(),
                current_partition: None,
                gc_before_secs: None,
                now_secs: None,
                purge_safe: false,
                max_purgeable_timestamp: None,
                schema: schema.clone(),
                schema_arc: std::sync::Arc::new(schema.clone()),
                _egress_slot: None,
                trace: NoTrace,
            };
            let merged = merger.merge_partition_rows(vec![live_entry, tombstone_entry])
                .expect("merge_partition_rows must not fail");

            prop_assert_eq!(merged.len(), 1, "one clustering key => one merged row");

            let winner = &merged[0];
            if ts_delete > ts_write {
                // Tombstone has higher timestamp => should win.
                prop_assert!(
                    matches!(winner.row_data, RowData::Tombstone { .. }),
                    "Tombstone(ts={}) must win over Live(ts={})",
                    ts_delete, ts_write
                );
            } else if ts_write > ts_delete {
                // Live write has higher timestamp => should win.
                prop_assert!(
                    matches!(winner.row_data, RowData::Live { .. }),
                    "Live(ts={}) must win over Tombstone(ts={})",
                    ts_write, ts_delete
                );
            } else {
                // Equal timestamps: the tombstone (Delete) ALWAYS wins, matching
                // Cassandra `Cells#reconcile`. This must hold regardless of file
                // recency — the assertion previously carved this case out, hiding
                // the run_index-only tiebreak bug (Issue #498).
                prop_assert!(
                    matches!(winner.row_data, RowData::Tombstone { .. }),
                    "At equal ts={}, Tombstone must win over Live (Cassandra reconcile rule)",
                    ts_delete
                );
            }
        }
    }
