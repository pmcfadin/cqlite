//! Issue #1669 — range-shadowing binary search.
//!
//! `apply_range_shadowing` used to LINEAR-scan the coalesced range tombstones for
//! EVERY clustering key it shadowed: `range_tombstones.iter().filter(...).max()`,
//! invoked once per reconciled cluster ⇒ O(rows × ranges) coverage comparisons.
//!
//! `coalesce_range_tombstones` already yields, per partition key, a sequence
//! sorted by start bound and DISJOINT, and `apply_range_shadowing` is called from
//! the per-partition `merge_partition_rows`, so the whole slice is one
//! partition's sorted+disjoint ranges. Disjoint ⇒ at most ONE range covers a
//! given `ck`, so the fix binary-searches for that single candidate:
//! O(rows × log ranges + ranges).
//!
//! This sibling file (not inline in `merge/mod.rs`, per the #1116 campsite rule)
//! houses two guards:
//!   1. A WORK-COUNTER regression — drives R rows against T coalesced ranges
//!      inside a
//!      [`RangeCoverageScope`](crate::storage::sstable::work_counters::range_coverage_scope::RangeCoverageScope)
//!      and asserts the coverage-comparison count stays O(R + T), which only the
//!      binary-search code can meet (the linear scan spends ~R × T).
//!   2. A CORRECTNESS / byte-parity guard — the binary-search path shadows
//!      EXACTLY the rows an independent coverage oracle says are covered, across a
//!      full clustering sweep including bound edges and inter-range gaps, plus the
//!      multi-partition FALLBACK path (which keeps the exact linear scan).

use super::*;
use crate::schema::{Column, KeyColumn};
use crate::storage::sstable::work_counters::range_coverage_scope::RangeCoverageScope;
use crate::storage::write_engine::mutation::ClusteringBound;
use crate::types::Value;
use std::collections::HashMap;

/// Single-column `int` partition + single-column `int` clustering schema.
fn schema() -> TableSchema {
    TableSchema {
        keyspace: "i1669".to_string(),
        table: "range_shadow".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![crate::schema::ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: Default::default(),
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

fn dk(byte: u8) -> DecoratedKey {
    DecoratedKey::from_key_bytes(vec![byte]).expect("token")
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

/// A whole-row tombstone at `ck=n` with `deletion_time` BELOW any range floor, so
/// it is dropped (`None`) exactly when a range covers it — making coverage
/// observable through the return value alone.
fn shadowable_row(key: DecoratedKey, n: i32) -> MergeEntry {
    MergeEntry::new(
        0,
        key,
        Some(ck(n)),
        1,
        RowData::Tombstone {
            deletion_time: 1,
            local_deletion_time: 0,
        },
    )
}

/// A live row at `ck=n` (its clustering pseudo-cell only), likewise shadowed to
/// `None` when a covering range's floor is newer than its cell timestamp.
fn shadowable_live_row(key: DecoratedKey, n: i32) -> MergeEntry {
    MergeEntry::new(
        0,
        key,
        Some(ck(n)),
        1,
        RowData::Live {
            cells: vec![CellData::new("ck".to_string(), Value::Integer(n), 1)],
        },
    )
}

/// WORK-COUNTER regression for issue #1669. R clustering rows shadowed against T
/// coalesced (sorted + disjoint) range tombstones must cost O(R + T) coverage
/// comparisons — the binary search checks at most ONE candidate range per row.
///
// main today: the linear `filter().max()` scan calls `range_tombstone_covers_ck`
// once per (row × range) = R × T = 64 × 64 = 4096 comparisons — far above the
// C·(R + T) bound below (256). Post-fix: at most R = 64 (one candidate per row).
#[test]
fn range_shadowing_is_binary_search_not_linear_scan() {
    const R: usize = 64; // clustering rows to shadow
    const T: usize = 64; // coalesced range tombstones

    let schema = schema();

    // T disjoint inclusive ranges [10i, 10i+3] @100, one per block of 10. Feed
    // them through the production coalescer so `range_tombstones` is exactly the
    // sorted + disjoint per-partition slice `merge_partition_rows` would pass.
    let mut range_tombstones: Vec<(DecoratedKey, RangeTombstone)> = (0..T)
        .map(|i| {
            let base = (i as i32) * 10;
            (
                dk(1),
                rt(
                    ClusteringBound::Inclusive(ck(base)),
                    ClusteringBound::Inclusive(ck(base + 3)),
                    100,
                ),
            )
        })
        .collect();
    KWayMerger::coalesce_range_tombstones(&mut range_tombstones, &schema);
    assert_eq!(
        range_tombstones.len(),
        T,
        "the {T} disjoint ranges stay {T} after coalescing (no merges)"
    );

    // R rows spread across the clustering axis (some covered, some in gaps),
    // every one in the SAME partition as the ranges (single-partition fast path).
    let rows: Vec<MergeEntry> = (0..R)
        .map(|i| shadowable_row(dk(1), (i as i32) * 10 + 1))
        .collect();

    let scope = RangeCoverageScope::new();
    for row in rows {
        let _ = KWayMerger::apply_range_shadowing(row, &range_tombstones, &schema);
    }
    let comparisons = scope.count();
    drop(scope);

    // O(R + T): each row triggers at most one authoritative containment check
    // (the single binary-search candidate). C = 2 sits strictly between the
    // post-fix count (<= R = 64) and the linear-scan count (R × T = 4096).
    let bound = 2 * (R as u64 + T as u64); // 256
    assert!(
        comparisons <= bound,
        "range shadowing made {comparisons} coverage comparisons for R={R} rows \
         and T={T} ranges (bound {bound}); the #1669 linear scan (~R×T={}) regressed",
        R * T
    );
}

/// Independent coverage oracle: `ck=n` is covered iff it lands in one of the
/// inclusive `[base, base+3]` blocks the test builds. Deliberately NOT the
/// production `range_tombstone_covers_ck` — this is the reference truth.
fn oracle_covered(n: i32, blocks: &[i32]) -> bool {
    blocks.iter().any(|&base| (base..=base + 3).contains(&n))
}

/// CORRECTNESS / byte-parity guard: the binary-search path shadows EXACTLY the
/// rows the independent oracle marks covered, across a full clustering sweep that
/// hits every range's start/end edge and the gaps between ranges. A single
/// mismatch (a covered row surviving, or an uncovered row dropped) is a byte-parity
/// break the 33-table goldens would also catch — this pins it at the unit level.
#[test]
fn binary_search_shadows_exactly_the_covered_rows() {
    let schema = schema();
    let blocks: Vec<i32> = (0..8).map(|i| i * 10).collect(); // [0,3],[10,13],...

    let mut range_tombstones: Vec<(DecoratedKey, RangeTombstone)> = blocks
        .iter()
        .map(|&base| {
            (
                dk(1),
                rt(
                    ClusteringBound::Inclusive(ck(base)),
                    ClusteringBound::Inclusive(ck(base + 3)),
                    100,
                ),
            )
        })
        .collect();
    KWayMerger::coalesce_range_tombstones(&mut range_tombstones, &schema);

    // Sweep past every edge (-1 .. last_end + 2), across both a tombstone row and
    // a live row so both `apply_range_shadowing` arms are exercised.
    let last = *blocks.last().expect("blocks non-empty") + 3;
    for n in -1..=(last + 2) {
        let expected_covered = oracle_covered(n, &blocks);
        for row in [shadowable_row(dk(1), n), shadowable_live_row(dk(1), n)] {
            let survived =
                KWayMerger::apply_range_shadowing(row, &range_tombstones, &schema).is_some();
            assert_eq!(
                !survived, expected_covered,
                "ck={n}: covered={expected_covered} but survived={survived} \
                 (binary-search coverage diverged from the oracle)"
            );
        }
    }
}

/// The single-partition binary search and the multi-partition FALLBACK linear
/// scan agree on coverage. A slice holding TWO partitions' ranges (first.key !=
/// last.key) forces `apply_range_shadowing` down the defensive linear-scan branch;
/// it must still shadow each partition's rows by that partition's own ranges only.
#[test]
fn multi_partition_slice_uses_exact_fallback_scan() {
    let schema = schema();

    // Partition 1 covers [0,3]; partition 2 covers [10,13]. Coalesced per key,
    // the slice holds both partitions' ranges (contiguous but two groups), so the
    // first/last-key guard is false and the fallback linear scan runs.
    let mut range_tombstones = vec![
        (
            dk(1),
            rt(
                ClusteringBound::Inclusive(ck(0)),
                ClusteringBound::Inclusive(ck(3)),
                100,
            ),
        ),
        (
            dk(2),
            rt(
                ClusteringBound::Inclusive(ck(10)),
                ClusteringBound::Inclusive(ck(13)),
                100,
            ),
        ),
    ];
    KWayMerger::coalesce_range_tombstones(&mut range_tombstones, &schema);
    assert_ne!(
        range_tombstones.first().expect("non-empty").0.key,
        range_tombstones.last().expect("non-empty").0.key,
        "the slice must span two partitions to exercise the fallback"
    );

    // p1 row at ck=1 is covered by p1's range; ck=11 (p2's range) does NOT leak
    // into p1. Symmetrically for p2.
    let p1_hit =
        KWayMerger::apply_range_shadowing(shadowable_row(dk(1), 1), &range_tombstones, &schema);
    assert!(p1_hit.is_none(), "p1 ck=1 covered by p1 range");
    let p1_miss =
        KWayMerger::apply_range_shadowing(shadowable_row(dk(1), 11), &range_tombstones, &schema);
    assert!(
        p1_miss.is_some(),
        "p1 ck=11 must NOT be shadowed by p2's range (no cross-partition leak)"
    );

    let p2_hit =
        KWayMerger::apply_range_shadowing(shadowable_row(dk(2), 11), &range_tombstones, &schema);
    assert!(p2_hit.is_none(), "p2 ck=11 covered by p2 range");
    let p2_miss =
        KWayMerger::apply_range_shadowing(shadowable_row(dk(2), 1), &range_tombstones, &schema);
    assert!(
        p2_miss.is_some(),
        "p2 ck=1 must NOT be shadowed by p1's range (no cross-partition leak)"
    );
}
