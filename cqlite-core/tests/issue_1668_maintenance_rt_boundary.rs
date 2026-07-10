//! Issue #1383 / #1668 regression: BACKGROUND auto-compaction (via the public
//! [`WriteEngine::maintenance_step`] surface) must SYNTHESIZE — never drop —
//! the range-tombstone boundary when two flush generations carry ranges that
//! OVERLAP across a clustering point.
//!
//! `issue_1383_rt_boundary_synthesis` pins this for the direct
//! `compact_sstables`/`KWayMerger::merge` path. This test drives the SAME
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

#![cfg(feature = "write-support")]

use std::path::PathBuf;
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
struct Marker {
    start: ClusteringBound,
    end: ClusteringBound,
    deletion_time: i64,
    local_deletion_time: i32,
}

/// Read the compacted output back through the merge read path, collecting the
/// surviving range markers (in clustering order) and live-row clustering keys.
fn read_back(output: PathBuf, schema: &TableSchema) -> (Vec<Marker>, Vec<i32>) {
    let mut merger = KWayMerger::new(vec![output], schema).expect("KWayMerger::new");
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
    (markers, live_rows)
}

/// Compact two overlapping-range-tombstone generations via the public
/// background-compaction surface (`maintenance_step`) and assert the
/// synthesized boundary at ck=3 survives — both boundary halves AND the
/// correct row survivals.
#[test]
fn maintenance_step_synthesizes_range_tombstone_boundary_across_generations() {
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

    // gen-1 (newer): [incl(3), Top] @ts=20 + rows ck=2 (ts=15, survives — only
    // the ts=10 side covers ck<3, and 15>10) and ck=6 (ts=25, survives — in the
    // ts=20 side but 25>20).
    engine
        .write(range_delete(incl(3), ClusteringBound::Top, 20))
        .unwrap();
    engine.write(write_row(2, 15)).unwrap();
    engine.write(write_row(6, 25)).unwrap();
    rt.block_on(engine.flush()).unwrap().unwrap();
    // gen-2 (older): [Bottom, excl(5)] @ts=10 + row ck=4 (ts=15, shadowed by
    // the ts=20 side, 15<=20 → absent).
    engine
        .write(range_delete(ClusteringBound::Bottom, excl(5), 10))
        .unwrap();
    engine.write(write_row(4, 15)).unwrap();
    rt.block_on(engine.flush()).unwrap().unwrap();

    // min_sstable_size large enough that both tiny files bucket together into
    // one full (purge-safe) compaction.
    let policy = STCSPolicy::new(2, 32, 0.5, 1.5, 1024 * 1024).unwrap();
    engine.set_merge_policy(Box::new(policy)).unwrap();

    let mut report = engine.maintenance_step(Duration::from_secs(60)).unwrap();
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

    let (markers, live_rows) = read_back(outputs.into_iter().next().unwrap(), &schema);

    assert_eq!(
        live_rows,
        vec![2, 6],
        "ck=2 (postdates the ts=10 side) and ck=6 (postdates the ts=20 side) survive background \
         compaction; ck=4 (shadowed by the ts=20 side) is gone"
    );

    // The synthesized boundary at ck=3 must survive background compaction:
    // [Bottom, Exclusive(3)) @ts=10 CLOSE, then [Inclusive(3), Top] @ts=20 OPEN.
    assert_eq!(
        markers.len(),
        2,
        "background compaction must synthesize BOTH boundary halves (not drop them)"
    );
    assert!(
        matches!(markers[0].start, ClusteringBound::Bottom),
        "closing range opens from Bottom, got {:?}",
        markers[0].start
    );
    assert!(
        matches!(&markers[0].end, ClusteringBound::Exclusive(_))
            && bound_ck(&markers[0].end) == Some(3),
        "closing range ends Exclusive at the ck=3 boundary, got {:?}",
        markers[0].end
    );
    assert_eq!(
        markers[0].deletion_time, 10,
        "closing range carries the ts=10 deletion time"
    );
    assert_eq!(
        markers[0].local_deletion_time,
        ldt_for(10),
        "closing range carries the ts=10 side's OWN localDeletionTime"
    );
    assert!(
        matches!(&markers[1].start, ClusteringBound::Inclusive(_))
            && bound_ck(&markers[1].start) == Some(3),
        "opening range starts Inclusive at the SAME ck=3 boundary, got {:?}",
        markers[1].start
    );
    assert!(
        matches!(markers[1].end, ClusteringBound::Top),
        "opening range runs to Top, got {:?}",
        markers[1].end
    );
    assert_eq!(
        markers[1].deletion_time, 20,
        "opening range carries the ts=20 deletion time"
    );
    assert_eq!(
        markers[1].local_deletion_time,
        ldt_for(20),
        "opening range carries the ts=20 side's OWN localDeletionTime (distinct from the close \
         side's — a swapped LDT would be caught)"
    );
}
