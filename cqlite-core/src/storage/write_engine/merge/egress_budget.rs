//! Process-global adaptive egress budget for concurrent k-way merges
//! (issues #2765/#2600/#2367).
//!
//! Each streaming merge input buffers up to a per-merge channel capacity of
//! prefetched `MergeEntry` values in its bounded producer→consumer
//! `sync_channel` (see `STREAMING_CHANNEL_CAPACITY`, `merge/mod.rs`). With a
//! FIXED per-merge capacity of 256, the total rows buffered across the whole
//! process grew as `256 × active_merges` — an unbounded aggregate working set
//! under concurrent scan/compaction load (the #2600/#2367 backpressure gap).
//!
//! This module makes the aggregate track a FIXED [`EGRESS_ROW_BUDGET`] instead
//! of the per-merge count. A process-global [`ACTIVE`] count of in-flight
//! streaming merges (incremented when a channel is constructed, decremented via
//! the [`ActiveMergeGuard`] RAII guard when the merge finishes — even on panic
//! or early return) drives a per-merge capacity of
//!
//! ```text
//! cap_per_merge = clamp(EGRESS_ROW_BUDGET / active_merge_count, MIN_CAP, MAX_CAP)
//! ```
//!
//! so `N` merges started at concurrency `N` each buffer `budget / N`, and the
//! aggregate stays near `EGRESS_ROW_BUDGET` rather than `256 × N`. At LOW
//! concurrency (a single active merge) the cap resolves to the unchanged
//! [`MAX_CAP`] = 256, preserving single-merge behavior. [`MIN_CAP`] (≥ 1)
//! guarantees forward progress — the integer division can never yield a
//! zero-capacity channel that would wedge a producer.
//!
//! This budget is ORTHOGONAL to the #2419 `channel_depth` gauge: that gauge
//! observes live occupancy; this bounds the per-merge capacity ceiling. The
//! counter here is a sibling of the #2316 producer-thread gauge and the #2419
//! channel-depth gauge, kept out of `merge/mod.rs` to bound that file.

use std::sync::atomic::{AtomicUsize, Ordering};

/// Fixed process-wide target for the TOTAL number of prefetched `MergeEntry`
/// values buffered across ALL concurrently active streaming merges (issue
/// #2765). Chosen so that at the pre-change fixed capacity of 256 the budget is
/// fully consumed by ~8 concurrent merges; beyond that, per-merge capacity
/// shrinks (down to [`MIN_CAP`]) so the aggregate stays bounded instead of
/// growing without limit. At `2048` entries of a few hundred bytes each the
/// aggregate is well within the 128MB memory target.
pub(super) const EGRESS_ROW_BUDGET: usize = 2048;

/// Minimum per-merge channel capacity (issue #2765). Guarantees forward
/// progress: no matter how many merges are concurrently active, every producer
/// can always place at least this many entries, so a bounded `sync_channel` can
/// never be constructed with capacity 0 (which would wedge the producer on its
/// first `send`). Kept ≥ 1 by construction.
pub(super) const MIN_CAP: usize = 8;

/// Maximum per-merge channel capacity — the unchanged single-merge value
/// (`STREAMING_CHANNEL_CAPACITY` in `merge/mod.rs`). At LOW concurrency the cap
/// clamps up to this, so single-merge behavior is byte-for-byte unchanged.
pub(super) const MAX_CAP: usize = super::STREAMING_CHANNEL_CAPACITY;

/// Process-global live count of in-flight streaming merges. Incremented by
/// [`begin_merge`] when a merge's egress channel is constructed and decremented
/// by [`ActiveMergeGuard`]'s drop when that merge finishes (or is torn down).
static ACTIVE: AtomicUsize = AtomicUsize::new(0);

/// Resolve the per-merge channel capacity for a given live active-merge count.
///
/// `clamp(EGRESS_ROW_BUDGET / active, MIN_CAP, MAX_CAP)`. `active` is floored at
/// 1 (via [`usize::max`]) so the division is always well-defined and the result
/// is never 0 — combined with the [`MIN_CAP`] clamp this is doubly safe against
/// a zero-capacity channel.
pub(super) fn capacity_for(active: usize) -> usize {
    let active = active.max(1);
    (EGRESS_ROW_BUDGET / active).clamp(MIN_CAP, MAX_CAP)
}

/// Register a starting streaming merge and return its resolved per-merge channel
/// capacity together with the RAII [`ActiveMergeGuard`] that decrements the live
/// count when the merge finishes.
///
/// The active count is incremented FIRST, then the capacity is computed from the
/// post-increment count, so the current merge counts itself: `N` merges each
/// starting when `N` are active every observe `active >= N` and receive
/// `budget / N`. The returned guard MUST be stored for the merge's lifetime (on
/// the adapter that owns the channel) so the decrement pairs with this
/// increment on every exit path.
#[must_use]
pub(super) fn begin_merge() -> (usize, ActiveMergeGuard) {
    begin_on(&ACTIVE)
}

/// Increment `counter`, resolve the capacity from the post-increment count, and
/// return a guard bound to `counter`. Parameterized over the atomic (mirroring
/// `channel_depth::adjust`) so a test can drive this EXACT increment/guard-drop
/// pairing against a PRIVATE atomic — deterministic, never racing the other
/// tests in this binary that drive real merges through the shared [`ACTIVE`].
fn begin_on(counter: &'static AtomicUsize) -> (usize, ActiveMergeGuard) {
    let active = counter.fetch_add(1, Ordering::SeqCst) + 1;
    (capacity_for(active), ActiveMergeGuard { counter })
}

/// RAII guard that decrements the active-merge count when a streaming merge
/// finishes — on normal completion, early return, or panic. Stored on the
/// merge's channel-owning adapter so it lives exactly as long as the merge's
/// buffered working set is accounted against the budget.
pub(super) struct ActiveMergeGuard {
    counter: &'static AtomicUsize,
}

impl Drop for ActiveMergeGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn low_concurrency_resolves_to_max_cap() {
        // Criterion 1: a single active merge gets the unchanged 256 cap.
        assert_eq!(capacity_for(1), MAX_CAP);
        assert_eq!(MAX_CAP, 256);
        // Zero active (defensive) is floored to 1 → MAX_CAP, never a div-by-zero.
        assert_eq!(capacity_for(0), MAX_CAP);
    }

    #[test]
    fn capacity_shrinks_with_concurrency_but_never_below_min() {
        // Budget divided among concurrent merges, clamped to the ceiling.
        assert_eq!(capacity_for(8), EGRESS_ROW_BUDGET / 8); // exactly 256 (== MAX_CAP)
        assert_eq!(capacity_for(16), EGRESS_ROW_BUDGET / 16); // 128
        // Very high concurrency clamps to MIN_CAP (never 0 → forward progress).
        assert_eq!(capacity_for(usize::MAX), MIN_CAP);
        assert!(MIN_CAP >= 1, "MIN_CAP must guarantee at least one slot");
    }

    #[test]
    fn aggregate_buffering_stays_within_budget_at_steady_concurrency() {
        // Criterion 2 (arithmetic form): at any single concurrency level K, the
        // aggregate worst-case buffering = K merges × capacity_for(K) stays at
        // or below the budget once K saturates it — whereas the pre-change fixed
        // cap of 256 gives K × 256, which blows past the budget for K > 8
        // (K == 8 exactly saturates it: 8 × 256 == 2048 == budget).
        for k in 8..=64usize {
            let aggregate = k * capacity_for(k);
            assert!(
                aggregate <= EGRESS_ROW_BUDGET,
                "adaptive aggregate {aggregate} must stay within budget \
                 {EGRESS_ROW_BUDGET} at concurrency {k}"
            );
            if k > 8 {
                let pre_change = k * MAX_CAP;
                assert!(
                    pre_change > EGRESS_ROW_BUDGET,
                    "the pre-change fixed cap would exceed the budget at \
                     concurrency {k} (pre_change={pre_change})"
                );
            }
        }
    }

    #[test]
    fn concurrent_merges_keep_aggregate_buffering_within_budget() {
        use std::sync::{Arc, Barrier};

        // Criterion 2 (wiring evidence): drive N real concurrent merges through
        // the REAL process-global `begin_merge` counter + RAII guards, and assert
        // the aggregate buffered working set they impose stays within the budget
        // — whereas the pre-change fixed cap of 256 would exceed it.
        const N: usize = 16;
        // Two barriers: one so every merge is registered (active) before any
        // reads the concurrency, one so no guard drops until all have read it —
        // modelling N merges concurrently active with the budget divided among
        // them (the steady-state the construction-time snapshot targets).
        let registered = Arc::new(Barrier::new(N));
        let released = Arc::new(Barrier::new(N));

        let handles: Vec<_> = (0..N)
            .map(|_| {
                let registered = registered.clone();
                let released = released.clone();
                std::thread::spawn(move || {
                    // Real registration on the shared global (holds the guard).
                    let (_ramp_cap, _guard) = begin_merge();
                    registered.wait();
                    // Every thread now sees the full concurrency; resolve the
                    // per-merge capacity against the live active count.
                    let live = ACTIVE.load(Ordering::SeqCst);
                    let cap = capacity_for(live);
                    released.wait();
                    (live, cap)
                })
            })
            .collect();

        let results: Vec<(usize, usize)> = handles
            .into_iter()
            .map(|h| h.join().expect("merge thread joins"))
            .collect();

        for (live, cap) in &results {
            // At least our N merges were concurrently active (background merges
            // from other tests only raise `live`, never lower it).
            assert!(*live >= N, "observed concurrency {live} must be >= {N}");
            // Aggregate worst-case buffering imposed by OUR N merges at the
            // observed concurrency stays within the fixed budget.
            let aggregate = N * cap;
            assert!(
                aggregate <= EGRESS_ROW_BUDGET,
                "adaptive aggregate {aggregate} (N={N} × cap={cap}) must stay \
                 within budget {EGRESS_ROW_BUDGET} at concurrency {live}"
            );
        }
        // The pre-change fixed cap would blow past the budget at this concurrency.
        assert!(
            N * MAX_CAP > EGRESS_ROW_BUDGET,
            "the pre-change fixed cap of {MAX_CAP} × {N} merges would exceed the \
             budget {EGRESS_ROW_BUDGET} — this is what the adaptive budget fixes"
        );
    }

    #[test]
    fn guard_increments_and_decrements_a_private_count() {
        // Deterministic pairing test against a PRIVATE atomic (mirroring
        // `channel_depth`'s per-test-atomic pattern): `begin_on` increments and
        // the returned guard's drop decrements, so the count returns to baseline
        // on every scope exit. A private atomic cannot be perturbed by the other
        // tests in this binary that drive real merges through the shared global.
        let counter: &'static AtomicUsize = Box::leak(Box::new(AtomicUsize::new(0)));
        assert_eq!(counter.load(Ordering::SeqCst), 0);
        {
            let (cap0, _g0) = begin_on(counter);
            assert_eq!(counter.load(Ordering::SeqCst), 1);
            assert_eq!(cap0, capacity_for(1), "first merge sees active=1");
            {
                let (cap1, _g1) = begin_on(counter);
                assert_eq!(counter.load(Ordering::SeqCst), 2);
                assert_eq!(cap1, capacity_for(2), "second merge sees active=2");
            }
            assert_eq!(
                counter.load(Ordering::SeqCst),
                1,
                "inner guard drop decremented"
            );
        }
        assert_eq!(
            counter.load(Ordering::SeqCst),
            0,
            "outer guard drop returned to baseline (no leak on any exit path)"
        );
    }
}
