//! Process-global adaptive egress budget for concurrent k-way merges
//! (issues #2765/#2600/#2367).
//!
//! A k-way merge streams each of its `K` input SSTables through a bounded
//! producer→consumer `sync_channel` that buffers up to a per-channel capacity of
//! prefetched `MergeEntry` values (see `STREAMING_CHANNEL_CAPACITY`,
//! `merge/mod.rs`). With a FIXED per-channel capacity of 256, the rows buffered
//! by a SINGLE merge grew as `256 × K`, and across the process as
//! `256 × K × active_merges` — an unbounded aggregate working set under
//! concurrent scan/compaction load (the #2600/#2367 backpressure gap; the field
//! signal was ~80 producer threads live at once).
//!
//! ## The unit is a MERGE, not a source channel
//!
//! The count keyed here is **concurrent k-way MERGE operations**, incremented
//! exactly ONCE per merge — when a [`KWayMerger`](super::KWayMerger) is
//! constructed — NOT once per source channel. (An earlier revision counted per
//! source adapter, so a solo `K`-way compaction registered `K` "merges" and its
//! own later source channels shrank below 256 — violating the "a single merge is
//! unchanged = 256 per source" contract regardless of `K`. Keying per merge
//! fixes that.) This is also why the count deliberately differs from
//! [`producer_gauge`](super::producer_gauge)'s `LIVE`, which counts per-SOURCE
//! producer THREADS (`O(K × active_merges)`): that gauge answers "how many
//! producer threads exist"; this counter answers "how many merge operations are
//! competing for the egress budget".
//!
//! ## Capacity snapshot
//!
//! At merge construction the active-merge count is incremented ONCE (via
//! [`begin_merge`], returning an [`ActiveMergeGuard`] stored on the
//! `KWayMerger`, decremented exactly once when the whole merge is dropped — even
//! on panic/early-return) and a single per-channel capacity is snapshotted:
//!
//! ```text
//! cap_per_channel = clamp(EGRESS_ROW_BUDGET / active_merge_count, MIN_CAP, MAX_CAP)
//! ```
//!
//! ALL `K` source channels of that merge use the SAME snapshot. So a SOLO merge
//! (active = 1) gives 256 per source for ANY `K` (single-merge behavior
//! unchanged); the cap shrinks only as CONCURRENT merges rise.
//!
//! ## Honest bound (NOT a strict global `≤ EGRESS_ROW_BUDGET`)
//!
//! [`EGRESS_ROW_BUDGET`] is a per-ACTIVE-MERGE-SLOT budget, not a hard global
//! ceiling. Because every source channel of a merge gets `cap_per_channel`, the
//! honest worst-case global working set is
//!
//! ```text
//! working_set ≈ active_merges × K × cap_per_channel
//! ```
//!
//! which for `active_merges ≥ EGRESS_ROW_BUDGET / MAX_CAP` is `≈ K × budget`
//! (the `/active` division cancels the outer `active` factor down to ONE budget
//! per source-fanout), and is floored by `active_merges × K × MIN_CAP` — the
//! deliberate cost of the forward-progress floor. The win vs. the fixed cap is
//! the removal of the `× 256` per-source constant: caps fall toward [`MIN_CAP`]
//! as concurrency climbs, instead of every channel holding a fixed 256. Do NOT
//! read this as a strict `≤ EGRESS_ROW_BUDGET` global bound.
//!
//! The snapshot is taken ONCE at construction and never revised, so it is
//! order-dependent, not fair: a long-lived merge that starts during a burst
//! stays PINNED at its low snapshot cap for its entire life, even after the
//! burst clears and concurrency drops back to one. Conversely a merge that
//! starts while the process is idle keeps the full 256 per channel even as later
//! merges arriving during ITS lifetime are squeezed toward [`MIN_CAP`] — the
//! throttle falls on the newcomers, not the incumbent.
//!
//! This budget is ORTHOGONAL to the #2419 `channel_depth` gauge: that gauge
//! observes live occupancy; this bounds the per-channel capacity ceiling. Kept
//! out of `merge/mod.rs` to bound that file.

use std::sync::atomic::{AtomicUsize, Ordering};

use crate::observability;

/// Per-active-merge-SLOT egress budget in prefetched `MergeEntry` values (issue
/// #2765): the per-channel capacity a merge receives is `budget / active_merges`
/// (clamped). Chosen so that at the pre-change fixed capacity of 256 the budget
/// is fully consumed by ~8 concurrent merges; beyond that, per-channel capacity
/// shrinks (down to [`MIN_CAP`]) instead of every channel holding a fixed 256.
/// See the module doc for why this is a per-slot budget, NOT a strict global
/// ceiling. `2048` entries of a few hundred bytes each keeps a solo merge's
/// `K × 256` footprint well within the 128MB memory target.
pub(super) const EGRESS_ROW_BUDGET: usize = 2048;

/// Minimum per-channel capacity (issue #2765). Guarantees forward progress: no
/// matter how many merges are concurrently active, every producer can always
/// place at least this many entries, so a bounded `sync_channel` can never be
/// constructed with capacity 0 (which would wedge the producer on its first
/// `send`). Kept ≥ 1 by construction.
pub(super) const MIN_CAP: usize = 8;

/// Maximum per-channel capacity — the unchanged single-merge value
/// (`STREAMING_CHANNEL_CAPACITY` in `merge/mod.rs`). At LOW concurrency the cap
/// clamps up to this, so single-merge behavior is byte-for-byte unchanged.
pub(super) const MAX_CAP: usize = super::STREAMING_CHANNEL_CAPACITY;

/// Process-global live count of in-flight streaming merges. Incremented by
/// [`begin_merge`] when a merge's egress channel is constructed and decremented
/// by [`ActiveMergeGuard`]'s drop when that merge finishes (or is torn down).
static ACTIVE: AtomicUsize = AtomicUsize::new(0);

/// Record the operator-facing `cqlite.merge.active_merges` gauge (issue #2765),
/// mirroring the `producer_gauge::record` pattern. Called ONLY for the real
/// global path (`begin_merge`), never for the per-test private-atomic path, so
/// the gauge always reflects the true process-wide concurrency.
fn record_active(active: usize) {
    observability::record_gauge(
        observability::catalog::MERGE_ACTIVE_MERGES,
        active as i64,
        &[],
    );
}

/// Current live process-global active-merge count (test/observability hook —
/// mirrors the [`MERGE_ACTIVE_MERGES`](crate::observability::catalog::MERGE_ACTIVE_MERGES)
/// gauge, exposed to integration tests via `merge::active_merge_count`).
pub(super) fn active_count() -> usize {
    ACTIVE.load(Ordering::SeqCst)
}

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

/// Register a starting k-way MERGE (called ONCE per merge, at `KWayMerger`
/// construction — NOT per source channel) and return the per-channel capacity
/// ALL its source channels share, together with the RAII [`ActiveMergeGuard`]
/// that decrements the live count once when the whole merge is dropped.
///
/// The active count is incremented FIRST, then the capacity is computed from the
/// post-increment count, so the current merge counts itself: `N` merges each
/// starting when `N` are active observe `active >= N` and receive `budget / N`
/// per channel. The returned guard MUST be stored for the merge's lifetime (on
/// the `KWayMerger`, dropped AFTER its runs/channels) so exactly one decrement
/// pairs with this increment on every exit path.
#[must_use]
pub(super) fn begin_merge() -> (usize, ActiveMergeGuard) {
    begin_on(&ACTIVE, true)
}

/// Increment `counter`, resolve the capacity from the post-increment count, and
/// return a guard bound to `counter`. Parameterized over the atomic (mirroring
/// `channel_depth::adjust`) so a test can drive this EXACT increment/guard-drop
/// pairing against a PRIVATE atomic — deterministic, never racing the other
/// tests in this binary that drive real merges through the shared [`ACTIVE`].
///
/// `record` gates the operator gauge: `true` ONLY for the real global path
/// ([`begin_merge`]), `false` for per-test private atomics, so a test can never
/// publish a bogus `cqlite.merge.active_merges` level from a private count.
fn begin_on(counter: &'static AtomicUsize, record: bool) -> (usize, ActiveMergeGuard) {
    let active = counter.fetch_add(1, Ordering::SeqCst) + 1;
    if record {
        record_active(active);
    }
    (capacity_for(active), ActiveMergeGuard { counter, record })
}

/// RAII guard that decrements the active-merge count when a streaming merge
/// finishes — on normal completion, early return, or panic. Stored on the
/// `KWayMerger` (dropped after its runs/channels) so exactly one decrement
/// pairs with the merge's single [`begin_merge`] increment.
#[derive(Debug)]
pub(super) struct ActiveMergeGuard {
    counter: &'static AtomicUsize,
    /// Whether to re-record the operator gauge on decrement — see [`begin_on`].
    record: bool,
}

impl Drop for ActiveMergeGuard {
    fn drop(&mut self) {
        let active = self.counter.fetch_sub(1, Ordering::SeqCst) - 1;
        if self.record {
            record_active(active);
        }
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
    fn per_slot_bound_holds_and_min_cap_floor_is_honest() {
        // The per-ACTIVE-MERGE-SLOT invariant (see module doc "Honest bound"):
        // at any concurrency `a`, one merge's per-channel share `capacity_for(a)`
        // times `a` stays within the budget — whereas the pre-change fixed cap
        // of 256 gives `a × 256`, blowing past the budget for a > 8 (a == 8
        // exactly saturates it: 8 × 256 == 2048). Beyond the clamp point the
        // per-slot product is BELOW budget because the cap floors at MIN_CAP —
        // the deliberate, explicit cost of guaranteeing forward progress.
        //
        // Low #6: exercise BOTH the divide region AND the MIN_CAP-floor region so
        // the floor's cost (per-slot product falling under budget) is explicit.
        let clamp_point = EGRESS_ROW_BUDGET / MAX_CAP; // 8: highest `a` with cap==MAX_CAP
        let floor_point = EGRESS_ROW_BUDGET / MIN_CAP; // 256: lowest `a` with cap==MIN_CAP
        assert!(floor_point > clamp_point);
        for a in 1..=(floor_point + 64) {
            let cap = capacity_for(a);
            assert!((MIN_CAP..=MAX_CAP).contains(&cap), "cap {cap} within clamp");
            // Per-slot product never exceeds the budget.
            assert!(
                a * cap <= EGRESS_ROW_BUDGET || cap == MIN_CAP,
                "per-slot product {} must stay within budget at a={a}",
                a * cap
            );
            if a > clamp_point {
                assert!(
                    a * MAX_CAP > EGRESS_ROW_BUDGET,
                    "pre-change fixed cap {MAX_CAP} × {a} exceeds budget"
                );
            }
            if a >= floor_point {
                // MIN_CAP-floor region: the cap has bottomed out; the per-slot
                // product now GROWS as `a × MIN_CAP` — the forward-progress cost.
                assert_eq!(cap, MIN_CAP, "cap floored at MIN_CAP for a={a}");
                assert_eq!(a * cap, a * MIN_CAP);
            }
        }
    }

    #[test]
    fn concurrent_begin_shrinks_per_channel_capacity() {
        use std::sync::{Arc, Barrier};

        // Drive N concurrent MERGE registrations through the EXACT production
        // increment/snapshot primitive (`begin_on`), but against a PRIVATE atomic
        // (issue #2451 isolation, mirroring the pairing test) so this test can
        // NEVER inflate the shared global `ACTIVE` and shrink a parallel
        // merger-building test's caps. Assert the per-channel capacity a merge is
        // HANDED shrinks below the fixed 256 as concurrent merges rise — and
        // never below MIN_CAP. (KWayMerger-level end-to-end wiring, on the real
        // global, lives in `egress_wiring_tests`.)
        let counter: &'static AtomicUsize = Box::leak(Box::new(AtomicUsize::new(0)));
        const N: usize = 16;
        let registered = Arc::new(Barrier::new(N));
        let released = Arc::new(Barrier::new(N));

        let handles: Vec<_> = (0..N)
            .map(|_| {
                let registered = registered.clone();
                let released = released.clone();
                std::thread::spawn(move || {
                    // Register FIRST (increments the PRIVATE count + snapshots the
                    // cap), THEN barrier — so all N are counted before any reads;
                    // the second barrier holds every guard alive until all have
                    // observed, modelling N concurrent merges. `record = false`:
                    // never touch the process-global operator gauge.
                    let (cap, _guard) = begin_on(counter, false);
                    registered.wait();
                    let live = counter.load(Ordering::SeqCst);
                    released.wait();
                    (live, cap)
                })
            })
            .collect();

        let results: Vec<(usize, usize)> = handles
            .into_iter()
            .map(|h| h.join().expect("merge thread joins"))
            .collect();

        // The private atomic is touched ONLY by this test's N threads, so the
        // observed concurrency is EXACTLY N (deterministic, not `>=`).
        let max_live = results.iter().map(|(l, _)| *l).max().unwrap_or(0);
        assert_eq!(
            max_live, N,
            "private-atomic concurrency must be exactly {N}"
        );
        // The merges that registered while the count was already high got a
        // capacity strictly below the pre-change fixed 256 — the shrink property.
        let min_cap_seen = results.iter().map(|(_, c)| *c).min().unwrap_or(MAX_CAP);
        assert!(
            min_cap_seen < MAX_CAP,
            "with {N} concurrent merges at least one per-channel cap must fall \
             below {MAX_CAP} (min seen = {min_cap_seen})"
        );
        // Forward progress: no merge is ever handed a zero/sub-MIN_CAP capacity.
        for (_, cap) in &results {
            assert!(
                *cap >= MIN_CAP,
                "per-channel cap {cap} >= MIN_CAP {MIN_CAP}"
            );
        }
        // Every guard dropped on join → the private count returns to zero.
        assert_eq!(
            counter.load(Ordering::SeqCst),
            0,
            "no leak: all guards dropped"
        );
    }

    #[test]
    fn guard_increments_and_decrements_a_private_count() {
        // Deterministic pairing test against a PRIVATE atomic (mirroring
        // `channel_depth`'s per-test-atomic pattern): `begin_on` increments and
        // the returned guard's drop decrements, so the count returns to baseline
        // on every scope exit. A private atomic cannot be perturbed by the other
        // tests in this binary that drive real merges through the shared global,
        // and `record = false` keeps it off the process-global operator gauge.
        let counter: &'static AtomicUsize = Box::leak(Box::new(AtomicUsize::new(0)));
        assert_eq!(counter.load(Ordering::SeqCst), 0);
        {
            let (cap0, _g0) = begin_on(counter, false);
            assert_eq!(counter.load(Ordering::SeqCst), 1);
            assert_eq!(cap0, capacity_for(1), "first merge sees active=1");
            {
                let (cap1, _g1) = begin_on(counter, false);
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
