//! Issue #1383 / #1668 regression: BACKGROUND auto-compaction (via the public
//! [`WriteEngine::maintenance_step`] surface) must SYNTHESIZE — never drop —
//! the range-tombstone boundary when two flush generations carry ranges that
//! OVERLAP across a clustering point, INCLUDING when a budget pause lands
//! mid-buffering.
//!
//! `issue_1383_rt_boundary_synthesis` pins this for the direct
//! `compact_sstables`/`KWayMerger::merge` path. This file drives the SAME
//! property through the streaming background-compaction state machine
//! (`maintenance.rs`'s `PartitionStreamState`), which had the IDENTICAL
//! late-marker-drop hazard before issue #1668's buffering fix: it opened the
//! writer session on the first `Some(ck)` row with an incomplete
//! range-tombstone set, then fed each late range-only marker mutation through
//! `feed_streaming_row`, which dropped it as a no-op — silently losing the
//! synthesized boundary (and its shadowing) from the compacted output.
//!
//! Generation placement is the WORST case for the streaming path (mirrors
//! `crit3_inverse_generation_ordering`): gen-1 (newer) holds `[incl(3), Top]`
//! @ts=20 and gen-2 (older) holds `[Bottom, excl(5)]` @ts=10, so the OLDER
//! range's coalesced marker is only surfaced by the merge stream AFTER the
//! rows it covers have already streamed.
//!
//! Two tests:
//!   1. unpaused single-drain correctness (the boundary + shadowing survive);
//!   2. a near-zero first-call budget that forces a pause to land WHILE the
//!      range-tombstone-bearing partition is still being BUFFERED (before the
//!      PartitionEnd write session opens) — the highest-risk path for the
//!      buffering struct's stash/resume — must produce byte-identical output
//!      to the unpaused drain (and the same correct boundary + survivals).

#![cfg(feature = "write-support")]

use std::path::Path;
use std::time::Duration;

use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::merge::{KWayMerger, MergeStep, RowData};
use cqlite_core::storage::write_engine::mutation::{
    CellOperation, ClusteringBound, ClusteringKey, Mutation, PartitionKey, RangeTombstone, TableId,
};
use cqlite_core::storage::write_engine::{STCSPolicy, WriteEngine, WriteEngineConfig};
use cqlite_core::types::Value;
use tempfile::TempDir;

const KS: &str = "rt_bound_ks";
const TBL: &str = "rt_bound_tbl";
const PID: i32 = 1;

// Far-future local-deletion-times (distinct per range) so gc-grace never
// purges a marker regardless of wall clock (no wall-clock race), AND the two
// boundary sides carry different LDTs so a swapped LDT would be caught —
// mirroring the issue_1383 suite.
const NEVER_PURGE_LDT_BASE: i32 = 2_000_000_000;

fn ldt_for(ts: i64) -> i32 {
    NEVER_PURGE_LDT_BASE + (ts as i32)
}

fn schema() -> TableSchema {
    TableSchema {
        keyspace: KS.to_string(),
        table: TBL.to_string(),
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
                name: "id".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "ck".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "name".to_string(),
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

fn write_row(ck: i32, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("id", Value::Integer(PID)),
        Some(ClusteringKey::single("ck", Value::Integer(ck))),
        vec![CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text(format!("r{ck}")),
        }],
        ts,
        None,
    )
}

fn range_delete(start: ClusteringBound, end: ClusteringBound, ts: i64) -> Mutation {
    let mut m = Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("id", Value::Integer(PID)),
        None,
        vec![],
        ts,
        None,
    );
    m.range_tombstones.push(RangeTombstone {
        start,
        end,
        deletion_time: ts,
        local_deletion_time: ldt_for(ts),
    });
    m
}

fn incl(ck: i32) -> ClusteringBound {
    ClusteringBound::Inclusive(ClusteringKey::single("ck", Value::Integer(ck)))
}

fn excl(ck: i32) -> ClusteringBound {
    ClusteringBound::Exclusive(ClusteringKey::single("ck", Value::Integer(ck)))
}

fn ck_of(k: &ClusteringKey) -> Option<i32> {
    match k.columns.first().map(|(_, v)| v) {
        Some(Value::Integer(n)) => Some(*n),
        _ => None,
    }
}

fn bound_ck(b: &ClusteringBound) -> Option<i32> {
    match b {
        ClusteringBound::Inclusive(k) | ClusteringBound::Exclusive(k) => ck_of(k),
        _ => None,
    }
}

/// A decoded range marker as it surfaces through the compaction read path.
/// `Eq` so two runs' marker sequences can be compared directly.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Marker {
    start: ClusteringBound,
    end: ClusteringBound,
    deletion_time: i64,
    local_deletion_time: i32,
}

/// One compacted output, as needed to compare/assert: its raw Data.db bytes
/// (for the paused-vs-unpaused byte-identity check) plus the surviving range
/// markers and live-row clustering keys decoded through the merge read path.
struct Compacted {
    bytes: Vec<u8>,
    markers: Vec<Marker>,
    live_rows: Vec<i32>,
}

/// Write the WORST-CASE RT-overlap-across-generations fixture into `engine`:
/// gen-1 (newer) holds `[incl(3), Top]` @ts=20; gen-2 (older) holds
/// `[Bottom, excl(5)]` @ts=10 — coalescing to the boundary `[Bottom, excl(3))`
/// @ts=10 then `[incl(3), Top]` @ts=20. Seven surviving rows (ck 0,1,2 from the
/// ts=10 side; ck 5,6,7,8 from the ts=20 side) give enough cluster groups that
/// a near-zero budget reliably pauses mid-buffering; ck 3,4 (@ts=15) are
/// shadowed by the ts=20 side.
fn write_fixture(engine: &mut WriteEngine, rt: &tokio::runtime::Runtime) {
    // gen-1 (newer): the ts=20 range + rows that postdate it (survive) and two
    // that predate it (shadowed).
    engine
        .write(range_delete(incl(3), ClusteringBound::Top, 20))
        .unwrap();
    for ck in [5, 6, 7, 8] {
        engine.write(write_row(ck, 25)).unwrap();
    }
    for ck in [3, 4] {
        engine.write(write_row(ck, 15)).unwrap();
    }
    rt.block_on(engine.flush()).unwrap().unwrap();

    // gen-2 (older): the ts=10 range + rows below the boundary that postdate it.
    engine
        .write(range_delete(ClusteringBound::Bottom, excl(5), 10))
        .unwrap();
    for ck in [0, 1, 2] {
        engine.write(write_row(ck, 15)).unwrap();
    }
    rt.block_on(engine.flush()).unwrap().unwrap();
}

/// Read a compacted output Data.db back through the merge read path.
fn read_output(path: &Path, schema: &TableSchema) -> Compacted {
    let bytes = std::fs::read(path).expect("read compacted Data.db");
    let mut merger = KWayMerger::new(vec![path.to_path_buf()], schema).expect("KWayMerger::new");
    let mut markers = Vec::new();
    let mut live_rows = Vec::new();
    loop {
        match merger.step().expect("merger step") {
            MergeStep::Complete => break,
            MergeStep::Partition { rows, .. } => {
                for entry in rows {
                    if let Some(rt) = &entry.range_deletion {
                        markers.push(Marker {
                            start: rt.start.clone(),
                            end: rt.end.clone(),
                            deletion_time: rt.deletion_time,
                            local_deletion_time: rt.local_deletion_time,
                        });
                        continue;
                    }
                    if let RowData::Live { cells } = &entry.row_data {
                        let has_data = cells.iter().any(|c| c.column != "ck" && c.column != "id");
                        if has_data {
                            if let Some(ck) = entry.clustering_key.as_ref().and_then(ck_of) {
                                live_rows.push(ck);
                            }
                        }
                    }
                }
            }
        }
    }
    live_rows.sort_unstable();
    Compacted {
        bytes,
        markers,
        live_rows,
    }
}

/// Assert the synthesized boundary at ck=3 is present and correct:
/// `[Bottom, Exclusive(3)) @ts=10` CLOSE then `[Inclusive(3), Top] @ts=20` OPEN,
/// each carrying its OWN distinct localDeletionTime.
fn assert_boundary(markers: &[Marker]) {
    assert_eq!(
        markers.len(),
        2,
        "background compaction must synthesize BOTH boundary halves (not drop them), got {markers:#?}"
    );
    assert!(matches!(markers[0].start, ClusteringBound::Bottom));
    assert!(
        matches!(&markers[0].end, ClusteringBound::Exclusive(_))
            && bound_ck(&markers[0].end) == Some(3)
    );
    assert_eq!(markers[0].deletion_time, 10);
    assert_eq!(markers[0].local_deletion_time, ldt_for(10));
    assert!(
        matches!(&markers[1].start, ClusteringBound::Inclusive(_))
            && bound_ck(&markers[1].start) == Some(3)
    );
    assert!(matches!(markers[1].end, ClusteringBound::Top));
    assert_eq!(markers[1].deletion_time, 20);
    assert_eq!(markers[1].local_deletion_time, ldt_for(20));
}

// The 7 surviving clustering keys: ck 0,1,2 postdate the ts=10 side; ck 5,6,7,8
// postdate the ts=20 side. ck 3,4 are shadowed by the ts=20 side.
const EXPECTED_LIVE_ROWS: [i32; 7] = [0, 1, 2, 5, 6, 7, 8];

/// Build a fresh engine over the fixture and compact it via `maintenance_step`,
/// using `first_budget` for the FIRST call and a generous budget for every
/// subsequent call. Returns the compacted output decoded for assertions plus
/// the number of `maintenance_step` calls the drain took.
///
/// The fixture is a SINGLE partition, and the streaming writer session opens
/// only at `PartitionEnd`, so the output Data.db is finalized only on the
/// final call — meaning any call count `> 1` proves the partition's drain
/// PAUSED and RESUMED while still buffering that partition (nothing had been
/// written yet), exercising `PartitionStreamState`'s stash/resume of the
/// buffered rows + partial carrier/range-tombstone accumulation.
fn compact_fixture(first_budget: Duration) -> (Compacted, u32) {
    let schema = schema();
    let temp = TempDir::new().unwrap();
    let config = WriteEngineConfig::new(
        temp.path().join("data"),
        temp.path().join("wal"),
        schema.clone(),
    );
    let mut engine = WriteEngine::new(config).unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    write_fixture(&mut engine, &rt);

    // min_sstable_size large enough that both tiny files bucket together into
    // one full (purge-safe) compaction.
    let policy = STCSPolicy::new(2, 32, 0.5, 1.5, 1024 * 1024).unwrap();
    engine.set_merge_policy(Box::new(policy)).unwrap();

    let mut report = engine.maintenance_step(first_budget).unwrap();
    // Until the drain completes, nothing is finalized (single partition, write
    // deferred to PartitionEnd) — so an incomplete first call is a genuine
    // mid-buffering pause.
    let mut outputs = report.completed_merges.clone();
    let mut calls = 1u32;
    while report.pending_compaction {
        report = engine.maintenance_step(Duration::from_secs(60)).unwrap();
        outputs.extend(report.completed_merges.clone());
        calls += 1;
        assert!(calls < 10_000, "compaction never completed");
    }
    assert_eq!(
        outputs.len(),
        1,
        "expected exactly one compacted output Data.db, got {outputs:?}"
    );
    (read_output(&outputs[0], &schema), calls)
}

/// Unpaused single-drain: the synthesized boundary and the correct row
/// survivals must appear in the compacted output.
#[test]
fn maintenance_step_synthesizes_range_tombstone_boundary_across_generations() {
    let (out, calls) = compact_fixture(Duration::from_secs(60));
    assert_eq!(calls, 1, "generous budget should drain in a single call");
    assert_eq!(
        out.live_rows,
        EXPECTED_LIVE_ROWS.to_vec(),
        "ck 0,1,2 (postdate the ts=10 side) and 5,6,7,8 (postdate the ts=20 side) survive; \
         ck 3,4 (shadowed by the ts=20 side) are gone"
    );
    assert_boundary(&out.markers);
}

/// A near-zero first-call budget forces a pause to land WHILE the
/// range-tombstone-bearing partition is still being BUFFERED (before the
/// PartitionEnd write session opens) — the highest-risk path for
/// `PartitionStreamState`'s stash/resume. The resumed, multi-call drain must
/// produce output BYTE-IDENTICAL to an unpaused single-call drain of the
/// identical input, AND still carry the correct synthesized boundary +
/// survivals (proving neither the buffered rows nor the partial
/// carrier/range-tombstone accumulation is lost or corrupted across the pause).
#[test]
fn maintenance_step_pause_mid_buffering_matches_unpaused_for_rt_partition() {
    // Near-zero budget on the first call: `budget * 1.1` is ~1ns, so the
    // between-cluster-group budget check trips right after the first cluster
    // group is buffered — long before PartitionEnd — forcing the RT partition's
    // accumulation to pause and resume across many calls.
    let (paused, calls) = compact_fixture(Duration::from_nanos(1));
    assert!(
        calls > 1,
        "a ~1ns first-call budget must force the single RT partition's drain to PAUSE and RESUME \
         across MULTIPLE maintenance_step calls (nothing is written until PartitionEnd, so >1 \
         call proves a mid-buffering pause) — otherwise this test would not exercise the \
         stash/resume path it exists to cover; got {calls} call(s)"
    );

    // Unpaused single-drain baseline over the IDENTICAL input.
    let (unpaused, unpaused_calls) = compact_fixture(Duration::from_secs(60));
    assert_eq!(
        unpaused_calls, 1,
        "generous budget should drain in one call"
    );

    assert_eq!(
        paused.bytes, unpaused.bytes,
        "paused/resumed compaction of an RT-overlap partition must produce BYTE-IDENTICAL \
         Data.db to the unpaused single-call drain (no buffered row or partial range-tombstone \
         accumulation lost/corrupted across the mid-buffering pause)"
    );
    assert_eq!(
        paused.markers, unpaused.markers,
        "the synthesized boundary markers must be identical across the pause"
    );
    assert_eq!(
        paused.live_rows, unpaused.live_rows,
        "the surviving rows must be identical across the pause"
    );
    // Not a vacuous equality: both must actually carry the correct boundary +
    // survivals, not merely agree with each other while both wrong.
    assert_eq!(paused.live_rows, EXPECTED_LIVE_ROWS.to_vec());
    assert_boundary(&paused.markers);
}
