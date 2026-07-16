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
//! houses these guards:
//!   1. A WORK-COUNTER regression — drives R rows against T coalesced ranges
//!      inside a
//!      [`RangeCoverageScope`](crate::storage::sstable::work_counters::range_coverage_scope::RangeCoverageScope)
//!      and asserts the coverage-comparison count stays O(R + T), which only the
//!      binary-search code can meet (the linear scan spends ~R × T).
//!   2. A CORRECTNESS / byte-parity guard — the binary-search path shadows
//!      EXACTLY the rows an independent coverage oracle says are covered, across a
//!      full clustering sweep including bound edges and inter-range gaps, plus the
//!      multi-partition FALLBACK path (which keeps the exact linear scan).
//!   3. INVARIANT sweeps over the HARD cases where the load-bearing monotonicity
//!      of `range_end_before_ck` is non-trivial — `Exclusive` bounds, multi-column
//!      PREFIX bounds (the prefix-truncation `cmp` closure), and DESC clustering
//!      order (the `cut_cmp` axis reversal). Each asserts BOTH oracle-exact
//!      shadowing AND that `range_end_before_ck` is monotone (all-true then
//!      all-false) over the coalescer's output — the precise precondition that
//!      makes the `partition_point` binary search sound. A drift in the coalescer's
//!      ordering or the `range_end_before_ck`/`range_tombstone_covers_ck` mirror
//!      (which would silently drop shadowed rows → a byte-parity break) trips these.
//!   4. A MIRROR guard — `range_end_before_ck(ck, rt)` equals the exact negation of
//!      the `before_end` test in `range_tombstone_covers_ck`, restated
//!      independently, across all four end-bound variants and a ck sweep. Pins the
//!      hand-mirrored negation against future divergence.

use super::*;
use crate::schema::{ClusteringOrder, Column, KeyColumn};
use crate::storage::sstable::work_counters::range_coverage_scope::RangeCoverageScope;
use crate::storage::write_engine::mutation::ClusteringBound;
use crate::types::Value;
use std::cmp::Ordering;
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

// ---------------------------------------------------------------------------
// Invariant sweeps over the HARD cases (roborev #1669 Medium): the monotonicity
// of `range_end_before_ck` that makes `partition_point` sound is trivial for the
// single-column int / ASC / Inclusive case above. Below it is exercised where it
// is NOT trivial — Exclusive bounds, multi-column PREFIX bounds, and DESC order.
// ---------------------------------------------------------------------------

/// Single `int` PK + single `int` clustering, DESC order. The clustering axis is
/// reversed, so the coalescer sorts high→low and the binary search must stay
/// monotone against the reversed `compare`.
fn schema_desc() -> TableSchema {
    let mut s = schema();
    s.clustering_keys[0].order = ClusteringOrder::Desc;
    s
}

/// Single `int` PK + two `int` clustering columns (c1, c2), both ASC. Range
/// bounds are built on a PREFIX (c1 only) to exercise the prefix-truncation `cmp`
/// closure shared by `range_end_before_ck` and `range_tombstone_covers_ck`.
fn schema_two_col() -> TableSchema {
    let mut s = schema();
    s.clustering_keys = vec![
        crate::schema::ClusteringColumn {
            name: "c1".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: Default::default(),
        },
        crate::schema::ClusteringColumn {
            name: "c2".to_string(),
            data_type: "int".to_string(),
            position: 1,
            order: Default::default(),
        },
    ];
    s
}

/// A full two-column clustering key `(c1, c2)`.
fn ck2(a: i32, b: i32) -> ClusteringKey {
    ClusteringKey {
        columns: vec![
            ("c1".to_string(), Value::Integer(a)),
            ("c2".to_string(), Value::Integer(b)),
        ],
    }
}

/// A PREFIX clustering bound holding only the first component `c1`.
fn ckp(a: i32) -> ClusteringKey {
    ClusteringKey {
        columns: vec![("c1".to_string(), Value::Integer(a))],
    }
}

/// A whole-row tombstone at an arbitrary clustering key, dropped exactly when a
/// covering range's floor (100) outranks its `deletion_time` (1).
fn entry_tomb(key: DecoratedKey, ck: ClusteringKey) -> MergeEntry {
    MergeEntry::new(
        0,
        key,
        Some(ck),
        1,
        RowData::Tombstone {
            deletion_time: 1,
            local_deletion_time: 0,
        },
    )
}

/// A live row (one data cell @ ts 1) at an arbitrary clustering key, likewise
/// shadowed to `None` when a covering range's floor outranks its cell timestamp.
fn entry_live(key: DecoratedKey, ck: ClusteringKey) -> MergeEntry {
    MergeEntry::new(
        0,
        key,
        Some(ck),
        1,
        RowData::Live {
            cells: vec![CellData::new("v".to_string(), Value::Integer(0), 1)],
        },
    )
}

/// The reusable HARD-case assertion. Given a single-partition, coalesced+sorted
/// range set, it checks for every swept `ck`:
///   1. `range_end_before_ck` is MONOTONE (all-true then all-false) over the
///      sorted ranges — the exact precondition `partition_point` relies on; a
///      coalescer ordering drift or a broken end-bound mirror makes it non-monotone
///      and the binary search would then pick the wrong (or no) candidate; and
///   2. the binary-search `apply_range_shadowing` shadows the row iff an
///      INDEPENDENT `oracle` says the key is covered — for both a tombstone row and
///      a live row.
fn assert_binsearch_matches_oracle<F>(
    schema: &TableSchema,
    range_tombstones: &[(DecoratedKey, RangeTombstone)],
    keys: &[ClusteringKey],
    oracle: F,
) where
    F: Fn(&ClusteringKey) -> bool,
{
    for w in range_tombstones.windows(2) {
        assert_eq!(
            w[0].0.key, w[1].0.key,
            "helper requires a single-partition slice (binary-search path)"
        );
    }
    let key = range_tombstones
        .first()
        .expect("range set non-empty")
        .0
        .clone();

    for ck in keys {
        // (1) monotone partition predicate.
        let flags: Vec<bool> = range_tombstones
            .iter()
            .map(|(_, rt)| KWayMerger::range_end_before_ck(ck, rt, schema))
            .collect();
        let first_false = flags.iter().position(|&b| !b).unwrap_or(flags.len());
        assert!(
            flags[first_false..].iter().all(|&b| !b),
            "range_end_before_ck not monotone for ck={ck:?}: {flags:?} — \
             partition_point precondition violated"
        );

        // (2) oracle-exact shadowing, both row arms.
        let expected = oracle(ck);
        for row in [
            entry_tomb(key.clone(), ck.clone()),
            entry_live(key.clone(), ck.clone()),
        ] {
            let survived =
                KWayMerger::apply_range_shadowing(row, range_tombstones, schema).is_some();
            assert_eq!(
                !survived, expected,
                "ck={ck:?}: oracle covered={expected} but survived={survived} \
                 (binary-search coverage diverged from the oracle)"
            );
        }
    }
}

/// EXCLUSIVE-bound sweep: the previous guards used only `Inclusive` bounds, so the
/// `Exclusive` arms of `range_end_before_ck` / `range_tombstone_covers_ck` (and
/// their monotonicity) were untested. Mixes all four start/end inclusivities.
#[test]
fn binary_search_exact_over_exclusive_bounds() {
    let schema = schema();

    // (10,20) excl-excl → 11..=19 ; (30,40] excl-incl → 31..=40 ;
    // [50,60) incl-excl → 50..=59. Disjoint, non-adjacent.
    let mut range_tombstones = vec![
        (
            dk(1),
            rt(
                ClusteringBound::Exclusive(ck(10)),
                ClusteringBound::Exclusive(ck(20)),
                100,
            ),
        ),
        (
            dk(1),
            rt(
                ClusteringBound::Exclusive(ck(30)),
                ClusteringBound::Inclusive(ck(40)),
                100,
            ),
        ),
        (
            dk(1),
            rt(
                ClusteringBound::Inclusive(ck(50)),
                ClusteringBound::Exclusive(ck(60)),
                100,
            ),
        ),
    ];
    KWayMerger::coalesce_range_tombstones(&mut range_tombstones, &schema);

    let keys: Vec<ClusteringKey> = (5..=65).map(ck).collect();
    assert_binsearch_matches_oracle(&schema, &range_tombstones, &keys, |k| {
        let n = col_i32(k, 0);
        (10 < n && n < 20) || (30 < n && n <= 40) || (50..60).contains(&n)
    });
}

/// MULTI-COLUMN PREFIX-bound sweep: ranges bounded on `c1` alone (a prefix of the
/// two-column key) plus one full `(c1,c2)` range. Exercises the prefix-truncation
/// `cmp` closure — a covered `(c1,*)` must be recognised regardless of `c2`.
#[test]
fn binary_search_exact_over_prefix_bounds() {
    let schema = schema_two_col();

    // c1==1 : whole prefix covered ; c1==3 : whole prefix covered ;
    // c1==5 & 10<=c2<=20 : full-key range. Gaps at c1 in {0,2,4,6}.
    let mut range_tombstones = vec![
        (
            dk(1),
            rt(
                ClusteringBound::Inclusive(ckp(1)),
                ClusteringBound::Inclusive(ckp(1)),
                100,
            ),
        ),
        (
            dk(1),
            rt(
                ClusteringBound::Inclusive(ckp(3)),
                ClusteringBound::Inclusive(ckp(3)),
                100,
            ),
        ),
        (
            dk(1),
            rt(
                ClusteringBound::Inclusive(ck2(5, 10)),
                ClusteringBound::Inclusive(ck2(5, 20)),
                100,
            ),
        ),
    ];
    KWayMerger::coalesce_range_tombstones(&mut range_tombstones, &schema);

    let mut keys: Vec<ClusteringKey> = Vec::new();
    for c1 in 0..=6 {
        for c2 in [0, 5, 10, 15, 20, 25] {
            keys.push(ck2(c1, c2));
        }
    }
    assert_binsearch_matches_oracle(&schema, &range_tombstones, &keys, |k| {
        let c1 = col_i32(k, 0);
        let c2 = col_i32(k, 1);
        c1 == 1 || c1 == 3 || (c1 == 5 && (10..=20).contains(&c2))
    });
}

/// DESC-order sweep: the clustering axis is reversed, so a range covering VALUES
/// `[lo,hi]` is stored with its axis-first (higher-value) bound as `start`. The
/// coalescer sorts high→low; `range_end_before_ck` must stay monotone against the
/// reversed `compare`. Oracle stays in value space.
#[test]
fn binary_search_exact_over_desc_order() {
    let schema = schema_desc();

    // Values [10,20] incl, [30,40] incl, (50,60) excl — on the DESC axis the
    // higher value is the START bound.
    let mut range_tombstones = vec![
        (
            dk(1),
            rt(
                ClusteringBound::Inclusive(ck(20)),
                ClusteringBound::Inclusive(ck(10)),
                100,
            ),
        ),
        (
            dk(1),
            rt(
                ClusteringBound::Inclusive(ck(40)),
                ClusteringBound::Inclusive(ck(30)),
                100,
            ),
        ),
        (
            dk(1),
            rt(
                ClusteringBound::Exclusive(ck(60)),
                ClusteringBound::Exclusive(ck(50)),
                100,
            ),
        ),
    ];
    KWayMerger::coalesce_range_tombstones(&mut range_tombstones, &schema);

    let keys: Vec<ClusteringKey> = (5..=65).map(ck).collect();
    assert_binsearch_matches_oracle(&schema, &range_tombstones, &keys, |k| {
        let n = col_i32(k, 0);
        (10..=20).contains(&n) || (30..=40).contains(&n) || (50 < n && n < 60)
    });
}

/// MIRROR guard: `range_end_before_ck` MUST equal the exact negation of the
/// `before_end` test inside `range_tombstone_covers_ck`. `ref_before_end` restates
/// that test INDEPENDENTLY (not by calling the function under test), so a future
/// edit that changes one negation arm without the other trips this — the Low nit
/// roborev flagged, promoted to an enforced invariant across all four end-bound
/// variants (Inclusive / Exclusive / Top / Bottom) and a full ck sweep, ASC + DESC.
#[test]
fn range_end_before_ck_mirrors_before_end() {
    // Independent restatement of the `before_end` arms of
    // `range_tombstone_covers_ck` (compare `ck` against the end bound over the
    // bound's component count). Must be kept semantically identical to that
    // function — this test exists to catch a drift between the two.
    fn ref_before_end(ck: &ClusteringKey, rt: &RangeTombstone, schema: &TableSchema) -> bool {
        let cmp = |bound: &ClusteringKey| -> Ordering {
            let n = bound.columns.len();
            let truncated = ClusteringKey {
                columns: ck.columns.iter().take(n).cloned().collect(),
            };
            truncated
                .compare(bound, schema)
                .unwrap_or_else(|_| truncated.cmp(bound))
        };
        match &rt.end {
            ClusteringBound::Inclusive(b) => cmp(b) != Ordering::Greater,
            ClusteringBound::Exclusive(b) => cmp(b) == Ordering::Less,
            ClusteringBound::Top => true,
            ClusteringBound::Bottom => false,
        }
    }

    for (schema, ends) in [
        (
            schema(),
            vec![
                ClusteringBound::Inclusive(ck(30)),
                ClusteringBound::Exclusive(ck(30)),
                ClusteringBound::Top,
                ClusteringBound::Bottom,
            ],
        ),
        (
            schema_desc(),
            vec![
                ClusteringBound::Inclusive(ck(30)),
                ClusteringBound::Exclusive(ck(30)),
                ClusteringBound::Top,
                ClusteringBound::Bottom,
            ],
        ),
    ] {
        for end in &ends {
            // The start bound is irrelevant to `range_end_before_ck`; use Bottom.
            let rt = rt(ClusteringBound::Bottom, end.clone(), 100);
            for n in 25..=35 {
                let k = ck(n);
                assert_eq!(
                    KWayMerger::range_end_before_ck(&k, &rt, &schema),
                    !ref_before_end(&k, &rt, &schema),
                    "range_end_before_ck must be the exact negation of before_end \
                     (ck={n}, end={end:?}, desc={})",
                    schema.clustering_keys[0].order == ClusteringOrder::Desc
                );
            }
        }
    }
}

/// Extract the `i32` at clustering column `i` for the oracles above.
fn col_i32(ck: &ClusteringKey, i: usize) -> i32 {
    match ck.columns.get(i).map(|(_, v)| v) {
        Some(Value::Integer(n)) => *n,
        other => panic!("expected Integer at clustering col {i}, got {other:?}"),
    }
}
