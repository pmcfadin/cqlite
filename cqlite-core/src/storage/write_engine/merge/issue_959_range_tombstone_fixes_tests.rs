//! Moved out of `merge/mod.rs` unchanged (#4658); the module keeps its name via `#[path]`.

use super::*;
use crate::schema::{Column, KeyColumn};
use crate::storage::write_engine::mutation::ClusteringBound;
use crate::types::Value;
use std::collections::HashMap;

fn dk(byte: u8) -> DecoratedKey {
    DecoratedKey::from_key_bytes(vec![byte]).expect("token")
}

/// Single-column `int` partition + single-column `int` clustering schema.
fn schema_int_ck(order: crate::schema::ClusteringOrder) -> TableSchema {
    TableSchema {
        keyspace: "ks".to_string(),
        table: "tbl".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![crate::schema::ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order,
        }],
        columns: vec![Column {
            name: "v".to_string(),
            data_type: "int".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        }],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

fn ck(n: i32) -> ClusteringKey {
    ClusteringKey {
        columns: vec![("ck".to_string(), Value::Integer(n))],
    }
}

fn rt(start: ClusteringBound, end: ClusteringBound, dt: i64) -> RangeTombstone {
    RangeTombstone {
        start,
        end,
        deletion_time: dt,
        local_deletion_time: (dt / 1_000_000) as i32,
    }
}

/// High #1: two OVERLAPPING ranges with different bounds split into three
/// disjoint segments, the inner (newer) deletion winning its overlap.
#[test]
fn overlapping_ranges_coalesce_to_disjoint_segments() {
    let schema = schema_int_ck(Default::default());
    // [1,5] @100 overlapped by [2,3] @200.
    let mut rts = vec![
        (
            dk(1),
            rt(
                ClusteringBound::Inclusive(ck(1)),
                ClusteringBound::Inclusive(ck(5)),
                100,
            ),
        ),
        (
            dk(1),
            rt(
                ClusteringBound::Inclusive(ck(2)),
                ClusteringBound::Inclusive(ck(3)),
                200,
            ),
        ),
    ];
    KWayMerger::<NoTrace>::coalesce_range_tombstones(&mut rts, &schema);

    assert_eq!(rts.len(), 3, "union splits into 3 disjoint segments");
    // [1, 2) @100
    assert_eq!(rts[0].1.start, ClusteringBound::Inclusive(ck(1)));
    assert_eq!(rts[0].1.end, ClusteringBound::Exclusive(ck(2)));
    assert_eq!(rts[0].1.deletion_time, 100);
    // [2, 3] @200 (the newer inner range wins the overlap)
    assert_eq!(rts[1].1.start, ClusteringBound::Inclusive(ck(2)));
    assert_eq!(rts[1].1.end, ClusteringBound::Inclusive(ck(3)));
    assert_eq!(rts[1].1.deletion_time, 200);
    // (3, 5] @100
    assert_eq!(rts[2].1.start, ClusteringBound::Exclusive(ck(3)));
    assert_eq!(rts[2].1.end, ClusteringBound::Inclusive(ck(5)));
    assert_eq!(rts[2].1.deletion_time, 100);

    // Every emitted segment is a valid, well-ordered, non-overlapping range:
    // each end is >= its start and each start is > the previous end.
    for i in 0..rts.len() {
        let s = KWayMerger::<NoTrace>::range_start_cut(&rts[i].1.start);
        let e = KWayMerger::<NoTrace>::range_end_cut(&rts[i].1.end);
        assert_ne!(
            KWayMerger::<NoTrace>::cut_cmp(&s, &e, &schema),
            Ordering::Greater,
            "segment {i} start must not exceed its end"
        );
        if i > 0 {
            // Non-overlap: the previous end cut must not exceed this start
            // cut. Equality is fine and expected — an exclusive end and an
            // inclusive start at the same value share one boundary cut while
            // covering complementary value sets (e.g. `< 2` then `>= 2`).
            let prev_e = KWayMerger::<NoTrace>::range_end_cut(&rts[i - 1].1.end);
            assert_ne!(
                KWayMerger::<NoTrace>::cut_cmp(&prev_e, &s, &schema),
                Ordering::Greater,
                "segment {i} must not overlap the previous segment"
            );
        }
    }
}

/// Identical bounds across inputs collapse to one range at the newest
/// deletion (the old `canonicalize` behavior is subsumed).
#[test]
fn identical_bounds_collapse_to_newest() {
    let schema = schema_int_ck(Default::default());
    let mut rts = vec![
        (
            dk(1),
            rt(
                ClusteringBound::Inclusive(ck(1)),
                ClusteringBound::Inclusive(ck(3)),
                100,
            ),
        ),
        (
            dk(1),
            rt(
                ClusteringBound::Inclusive(ck(1)),
                ClusteringBound::Inclusive(ck(3)),
                200,
            ),
        ),
    ];
    KWayMerger::<NoTrace>::coalesce_range_tombstones(&mut rts, &schema);
    assert_eq!(rts.len(), 1);
    assert_eq!(rts[0].1.deletion_time, 200);
    assert_eq!(rts[0].1.start, ClusteringBound::Inclusive(ck(1)));
    assert_eq!(rts[0].1.end, ClusteringBound::Inclusive(ck(3)));
}

/// Adjacent (touching) ranges at the SAME deletion merge into one.
#[test]
fn adjacent_same_deletion_ranges_merge() {
    let schema = schema_int_ck(Default::default());
    let mut rts = vec![
        (
            dk(1),
            rt(
                ClusteringBound::Inclusive(ck(1)),
                ClusteringBound::Inclusive(ck(3)),
                100,
            ),
        ),
        (
            dk(1),
            rt(
                ClusteringBound::Exclusive(ck(3)),
                ClusteringBound::Inclusive(ck(5)),
                100,
            ),
        ),
    ];
    KWayMerger::<NoTrace>::coalesce_range_tombstones(&mut rts, &schema);
    assert_eq!(rts.len(), 1, "touching equal-deletion ranges coalesce");
    assert_eq!(rts[0].1.start, ClusteringBound::Inclusive(ck(1)));
    assert_eq!(rts[0].1.end, ClusteringBound::Inclusive(ck(5)));
}

/// Distinct partition keys are coalesced independently (never merged).
#[test]
fn distinct_partitions_not_merged() {
    let schema = schema_int_ck(Default::default());
    let mut rts = vec![
        (
            dk(1),
            rt(ClusteringBound::Bottom, ClusteringBound::Top, 100),
        ),
        (
            dk(2),
            rt(ClusteringBound::Bottom, ClusteringBound::Top, 100),
        ),
    ];
    KWayMerger::<NoTrace>::coalesce_range_tombstones(&mut rts, &schema);
    assert_eq!(rts.len(), 2);
    assert_ne!(rts[0].0.key, rts[1].0.key);
}

/// High #2: a complex deletion NEWER than the covering range survives as a
/// metadata carrier when the row is otherwise fully shadowed.
#[test]
fn newer_complex_deletion_survives_full_range_shadow() {
    let schema = schema_int_ck(Default::default());
    // Row at ck=2 with only its clustering pseudo-cell (no live data) plus a
    // collection deletion @200; covered by a whole-partition range @100.
    let entry = MergeEntry::new(
        0,
        dk(1),
        Some(ck(2)),
        50,
        RowData::Live {
            cells: vec![CellData::new("ck".to_string(), Value::Integer(2), 50)],
        },
    )
    .with_complex_deletions(vec![ComplexDeletion {
        column: "v".to_string(),
        marked_for_delete_at: 200,
        local_deletion_time: 0,
    }]);
    let range = vec![(
        dk(1),
        rt(ClusteringBound::Bottom, ClusteringBound::Top, 100),
    )];

    let out = KWayMerger::<NoTrace>::apply_range_shadowing(entry, &range, &schema)
        .expect("a complex deletion newer than the range must survive");
    assert_eq!(
        out.complex_deletions.len(),
        1,
        "the newer collection deletion is preserved"
    );
    assert_eq!(out.complex_deletions[0].marked_for_delete_at, 200);
    match out.row_data {
        RowData::Live { cells } => assert!(cells.is_empty(), "carrier holds no data cells"),
        other => panic!("expected an empty Live carrier, got {other:?}"),
    }
}

/// A complex deletion OLDER than the covering range is subsumed and dropped,
/// leaving nothing for the re-emitted range marker to duplicate.
#[test]
fn older_complex_deletion_subsumed_by_range() {
    let schema = schema_int_ck(Default::default());
    let entry = MergeEntry::new(
        0,
        dk(1),
        Some(ck(2)),
        50,
        RowData::Live {
            cells: vec![CellData::new("ck".to_string(), Value::Integer(2), 50)],
        },
    )
    .with_complex_deletions(vec![ComplexDeletion {
        column: "v".to_string(),
        marked_for_delete_at: 50,
        local_deletion_time: 0,
    }]);
    let range = vec![(
        dk(1),
        rt(ClusteringBound::Bottom, ClusteringBound::Top, 100),
    )];

    assert!(
        KWayMerger::<NoTrace>::apply_range_shadowing(entry, &range, &schema).is_none(),
        "an older complex deletion is subsumed; nothing survives"
    );
}

/// Issue #2374/#2789 (rust-reviewer BLOCKER 2): a key-only LIVE row (an
/// `INSERT` with only its primary-key liveness marker, e.g. no regular
/// columns / all-null regulars) that coexists with an OLDER covering RANGE
/// tombstone survives shadowing (its marker timestamp beats the floor). The
/// surviving `RowData::Live` rebuild in `apply_range_shadowing` must carry
/// the marker forward via `with_row_liveness` — before the fix it dropped to
/// `RowLiveness::default()` (has_marker=false), so the READ-path visibility
/// rule (`marker_live_at`) then wrongly HID the row. Cassandra returns it.
#[test]
fn key_only_live_row_keeps_marker_through_range_shadow() {
    use crate::storage::sstable::reader::compaction_row::RowLiveness;
    let schema = schema_int_ck(Default::default());
    // Key-only live row at ck=2: only the clustering pseudo-cell (no data),
    // liveness marker @200 (live-forever); covered by a range @100.
    let entry = MergeEntry::new(
        0,
        dk(1),
        Some(ck(2)),
        200,
        RowData::Live {
            cells: vec![CellData::new("ck".to_string(), Value::Integer(2), 200)],
        },
    )
    .with_row_liveness(RowLiveness {
        has_marker: true,
        marker_timestamp: Some(200),
        ..Default::default()
    });
    let range = vec![(
        dk(1),
        rt(ClusteringBound::Bottom, ClusteringBound::Top, 100),
    )];

    let out = KWayMerger::<NoTrace>::apply_range_shadowing(entry, &range, &schema)
        .expect("a key-only live row newer than the range must survive");
    assert!(
        out.row_liveness.marker_live_at(1_000),
        "the surviving row must carry its liveness marker forward (BLOCKER 2, range)"
    );
}

/// BLOCKER 2, partition variant: same key-only-live-row invariant through
/// `apply_partition_shadowing`.
#[test]
fn key_only_live_row_keeps_marker_through_partition_shadow() {
    use crate::storage::sstable::reader::compaction_row::RowLiveness;
    let schema = schema_int_ck(Default::default());
    let _ = &schema; // schema unused by partition shadowing, kept for parity.
    let entry = MergeEntry::new(
        0,
        dk(1),
        Some(ck(2)),
        200,
        RowData::Live {
            cells: vec![CellData::new("ck".to_string(), Value::Integer(2), 200)],
        },
    )
    .with_row_liveness(RowLiveness {
        has_marker: true,
        marker_timestamp: Some(200),
        ..Default::default()
    });

    let out = KWayMerger::<NoTrace>::apply_partition_shadowing(entry, Some((100, 0)))
        .expect("a key-only live row newer than the partition floor must survive");
    assert!(
        out.row_liveness.marker_live_at(1_000),
        "the surviving row must carry its liveness marker forward (BLOCKER 2, partition)"
    );
}

/// BLOCKER 2 negative: a key-only row whose marker is OLDER-or-equal to the
/// covering floor does NOT survive as a phantom live row — the marker must
/// NOT be carried forward (marker_live stays false). Range case fully
/// shadows to `None`; the invariant is that no live-marker survivor leaks.
#[test]
fn key_only_expired_marker_does_not_survive_range_shadow() {
    use crate::storage::sstable::reader::compaction_row::RowLiveness;
    let schema = schema_int_ck(Default::default());
    // Marker @80 (older than the range @100) → fully shadowed, no survivor.
    let entry = MergeEntry::new(
        0,
        dk(1),
        Some(ck(2)),
        80,
        RowData::Live {
            cells: vec![CellData::new("ck".to_string(), Value::Integer(2), 80)],
        },
    )
    .with_row_liveness(RowLiveness {
        has_marker: true,
        marker_timestamp: Some(200),
        ..Default::default()
    });
    let range = vec![(
        dk(1),
        rt(ClusteringBound::Bottom, ClusteringBound::Top, 100),
    )];
    assert!(
        KWayMerger::<NoTrace>::apply_range_shadowing(entry, &range, &schema).is_none(),
        "a marker older than the covering range must not survive as a live row"
    );
}
