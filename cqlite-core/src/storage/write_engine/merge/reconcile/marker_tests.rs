//! Issue #4581: the primary-key marker steps of the reconcile kernel
//! ([`ReconcileState::shadow_marker_by_row_deletion`] / [`ReconcileState::purge_marker`]) and the
//! `build` arms that depend on them, driven through the REAL
//! `reconcile_cluster_with_overlap_counted` kernel.
//!
//! Every expectation is Cassandra 5.0.8's (`cassandra-5.0.8`): `Row.Merger.merge` shadows the
//! marker by the active deletion BEFORE the purge (`Row.java:762`), `BTreeRow.purge` then drops an
//! expired marker whose gc grace has elapsed (`BTreeRow.java:464`, `DeletionPurger.java:31`), a
//! REGULAR row deletion survives a strictly newer marker (`Row.java:754`, `isShadowedBy` is for
//! shadowable deletions only). The same shapes are pinned against Cassandra-written and
//! Cassandra-COMPACTED bytes in `tests/issue_4581_compaction_marker_rows.rs`.

#![cfg(feature = "write-support")]

use std::collections::HashMap;

use super::super::{CellData, KWayMerger, MergeEntry, PurgeCounts, RowData};
use crate::storage::sstable::reader::compaction_row::RowLiveness;
use crate::storage::write_engine::mutation::{ClusteringKey, DecoratedKey};
use crate::types::Value;

const NOW: i64 = 1_000_000;
const GC_BEFORE: i64 = NOW - 100;

fn dk() -> DecoratedKey {
    DecoratedKey::from_key_bytes(vec![1]).expect("token")
}

fn marker(ts: i64, expires_at: Option<i64>) -> RowLiveness {
    RowLiveness {
        has_marker: true,
        expires_at_seconds: expires_at,
        marker_timestamp: Some(ts),
        ttl_seconds: expires_at.map(|_| 3),
    }
}

fn marker_only(ts: i64, expires_at: Option<i64>) -> MergeEntry {
    MergeEntry::new(0, dk(), None, ts, RowData::Live { cells: vec![] })
        .with_row_liveness(marker(ts, expires_at))
}

fn row_deletion(ts: i64, ldt: i32) -> MergeEntry {
    MergeEntry::new(
        1,
        dk(),
        None,
        ts,
        RowData::Tombstone {
            deletion_time: ts,
            local_deletion_time: ldt,
        },
    )
}

/// `(gc_before, now, max_purgeable)` → the reconciled entry.
fn reconcile(
    rows: Vec<MergeEntry>,
    gc_before: Option<i64>,
    now: Option<i64>,
    max_purgeable: i64,
) -> Option<MergeEntry> {
    KWayMerger::reconcile_cluster_with_overlap_counted(
        None,
        rows,
        &HashMap::new(),
        gc_before,
        max_purgeable,
        now,
        &mut PurgeCounts::default(),
    )
}

fn plain(rows: Vec<MergeEntry>) -> Option<MergeEntry> {
    reconcile(rows, None, None, i64::MAX)
}

/// A live marker-only row is kept with its real timestamp, TTL and expiry (never ts=0).
#[test]
fn a_live_marker_only_row_is_kept_verbatim() {
    let out = plain(vec![marker_only(500, Some(NOW + 10))]).expect("kept");
    assert!(matches!(&out.row_data, RowData::Live { cells } if cells.is_empty()));
    assert_eq!(out.timestamp, 500);
    assert_eq!(out.row_liveness, marker(500, Some(NOW + 10)));
}

/// `activeDeletion.deletes(rowInfo)` is `ts <= markedForDeleteAt`: an older marker and an EQUAL
/// one die with the deletion; the row is the deletion alone.
#[test]
fn a_row_deletion_shadows_an_older_or_equal_marker() {
    for marker_ts in [400, 500] {
        let out =
            plain(vec![marker_only(marker_ts, None), row_deletion(500, 7)]).expect("deletion");
        assert!(
            matches!(
                out.row_data,
                RowData::Tombstone {
                    deletion_time: 500,
                    ..
                }
            ),
            "marker@{marker_ts} must be shadowed by deletion@500"
        );
        assert!(!out.row_liveness.has_marker);
    }
}

/// Defect B: a marker STRICTLY newer than the row deletion keeps the row live and the (regular,
/// non-shadowable) deletion rides alongside it — the Tombstone arm must not run first.
#[test]
fn a_marker_newer_than_a_row_deletion_keeps_the_row_live_beside_the_deletion() {
    let out = plain(vec![marker_only(600, None), row_deletion(500, 7)]).expect("live row");
    assert!(matches!(&out.row_data, RowData::Live { cells } if cells.is_empty()));
    assert_eq!(
        out.row_deletion,
        Some((500, 7)),
        "the deletion is kept, not dropped"
    );
    assert_eq!(out.row_liveness.marker_timestamp, Some(600));
    assert_eq!(out.timestamp, 600);
}

/// An expired marker INSIDE gc_grace is kept verbatim (ts/TTL/LDT), not turned into a tombstone.
#[test]
fn an_expired_marker_inside_gc_grace_is_kept() {
    // expired (expiry <= now) but expiry >= gcBefore
    let expiry = NOW - 50;
    let out = reconcile(
        vec![marker_only(500, Some(expiry))],
        Some(GC_BEFORE),
        Some(NOW),
        i64::MAX,
    )
    .expect("kept");
    assert_eq!(out.row_liveness, marker(500, Some(expiry)));
}

/// `shouldPurge(LivenessInfo, now)`: expired AND `ldt < gcBefore` AND `ts < maxPurgeable`.
#[test]
fn an_expired_marker_past_gc_grace_is_purged_only_when_all_three_gates_hold() {
    let expiry = NOW - 200; // expired and < gcBefore
    let row = || vec![marker_only(500, Some(expiry))];
    assert!(
        reconcile(row(), Some(GC_BEFORE), Some(NOW), i64::MAX).is_none(),
        "purged"
    );
    // overlap gate: ts must be STRICTLY below maxPurgeable
    assert!(
        reconcile(row(), Some(GC_BEFORE), Some(NOW), 500).is_some(),
        "ts == bound is kept"
    );
    assert!(reconcile(row(), Some(GC_BEFORE), Some(NOW), 501).is_none());
    // purging / expiry disabled: strict no-ops
    assert!(reconcile(row(), None, Some(NOW), i64::MAX).is_some());
    assert!(reconcile(row(), Some(GC_BEFORE), None, i64::MAX).is_some());
    // a LIVE TTL'd marker (expiry in the future) is never purged
    let live = vec![marker_only(500, Some(NOW + 1))];
    assert!(reconcile(live, Some(GC_BEFORE), Some(NOW), i64::MAX).is_some());
    // expiry == now is already expired (`isLive` is `now < ldt`) but inside grace here
    let edge = vec![marker_only(500, Some(NOW))];
    assert!(reconcile(edge, Some(GC_BEFORE), Some(NOW), i64::MAX).is_some());
}

/// Shadow BEFORE purge: a row deletion past gc_grace is purged, but it must first have shadowed
/// the older marker — otherwise the purged deletion would leave a ghost row.
#[test]
fn a_purged_row_deletion_leaves_no_ghost_row() {
    let old_ldt = (NOW - 200) as i32; // < gcBefore: purgeable
    let out = reconcile(
        vec![marker_only(400, None), row_deletion(500, old_ldt)],
        Some(GC_BEFORE),
        Some(NOW),
        i64::MAX,
    );
    assert!(
        out.is_none(),
        "marker shadowed, deletion purged: nothing is left"
    );
}

/// A newer marker outlives its purged older deletion: the row is the marker alone.
#[test]
fn a_newer_marker_outlives_a_purged_row_deletion() {
    let old_ldt = (NOW - 200) as i32;
    let out = reconcile(
        vec![marker_only(600, None), row_deletion(500, old_ldt)],
        Some(GC_BEFORE),
        Some(NOW),
        i64::MAX,
    )
    .expect("marker row");
    assert_eq!(out.row_deletion, None);
    assert_eq!(out.row_liveness.marker_timestamp, Some(600));
}

/// `INSERT (id, v) VALUES (id, null)`: the cell tombstone is purged, the marker survives — the row
/// is a marker-only row (Cassandra keeps it), not dropped by the phantom-row guard.
#[test]
fn a_marker_outlives_its_purged_cell_tombstone() {
    let tomb_ldt = NOW - 200;
    let mut cell = CellData::new(
        "v".to_string(),
        Value::Tombstone(Box::new(crate::types::TombstoneInfo {
            deletion_time: 500,
            tombstone_type: crate::types::TombstoneType::CellTombstone,
            local_deletion_time: tomb_ldt,
            ttl: None,
            range_start: None,
            range_end: None,
        })),
        500,
    );
    cell.local_deletion_time = Some(tomb_ldt as i32);
    let entry = MergeEntry::new(0, dk(), None, 500, RowData::Live { cells: vec![cell] })
        .with_row_liveness(marker(500, None));
    let out = reconcile(vec![entry], Some(GC_BEFORE), Some(NOW), i64::MAX).expect("marker row");
    assert!(matches!(&out.row_data, RowData::Live { cells } if cells.is_empty()));
    assert_eq!(out.row_liveness.marker_timestamp, Some(500));
}

/// A CLUSTERED row carries its marker on its clustering pseudo-cell: once the expired marker is
/// purged the key-only row must go with it (no ghost row), while the same row with a live marker
/// is kept.
#[test]
fn an_expired_purged_marker_removes_a_clustered_key_only_row() {
    let ck = ClusteringKey::single("ck", Value::Integer(1));
    let entry = |m: RowLiveness| {
        MergeEntry::new(
            0,
            dk(),
            Some(ck.clone()),
            500,
            RowData::Live {
                cells: vec![CellData::new("ck".to_string(), Value::Integer(1), 500)],
            },
        )
        .with_row_liveness(m)
    };
    let past_grace = marker(500, Some(NOW - 200));
    let reconciled = |m| {
        KWayMerger::reconcile_cluster_with_overlap_counted(
            Some(ck.clone()),
            vec![entry(m)],
            &HashMap::new(),
            Some(GC_BEFORE),
            i64::MAX,
            Some(NOW),
            &mut PurgeCounts::default(),
        )
    };
    assert!(
        reconciled(past_grace).is_none(),
        "purged marker, no data: gone"
    );
    assert!(reconciled(marker(500, None)).is_some(), "live marker: kept");
}
