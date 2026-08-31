//! Process-global adaptive egress budget for concurrent k-way merges
//! (issues #2765/#2600/#2367).
//!
//! A k-way merge streams each of its `K` input SSTables through a bounded
//! producer→consumer `sync_channel` that buffers prefetched `MergeEntry` values
//! (see `STREAMING_CHANNEL_CAPACITY`, `merge/mod.rs`). With a FIXED per-channel
//! capacity, the rows buffered by a SINGLE merge grew as `K × per_source`, and
//! across the process as `active_merges × K × per_source` — an unbounded
//! aggregate working set under concurrent scan/compaction load (the #2600/#2367
//! backpressure gap; the field signal was ~80 producer threads live at once).
//!
//! ## `per_source` — the ONE quantity every aggregate here multiplies (#2820)
//!
//! Every figure below is a PRODUCT of a per-source row bound, so it is stated in
//! terms of that quantity rather than a literal — the literals moved once
//! already. Since #2820 an egress channel carries BATCHES, and three populations
//! coexist at one instant (channel-resident, consumer-held, and the batch a
//! producer is PARKED holding in `send`), so:
//!
//! ```text
//! per_source = egress_batch::max_inflight_rows(cap_per_channel)
//!            = (msg_cap + 2) × batch_ceiling
//!            = 4 × cap_per_channel        (at every reachable setting)
//! ```
//!
//! of which `egress_batch::rows_resident_in_channel(cap_per_channel)` =
//! `2 × cap_per_channel` is the strictly smaller half the #2419 depth gauge can
//! observe, and a cold-started producer parks holding less again
//! (`egress_batch::rows_in_full_channel(cap) + batch_ceiling + 1`, the ramp sum).
//!
//! The pre-#2820 per-source figure was `cap_per_channel` ITSELF (one row per
//! channel slot), so **every product in this module is 4× what the same sentence
//! said before #2820**. That 4× is the envelope of record for #2820 — the
//! reconciliation that makes a ~256× reduction in cross-thread sends safe — not
//! an accidental regression, and it is measured, not argued (dhat peak FELL
//! against the pre-change baseline, because batching also shortens the interval
//! over which a producer holds rows at all). Two substitutions to refuse:
//! putting the channel-resident figure in for `per_source` understates the
//! MEMORY bound 2×, and putting `cap_per_channel` in understates it 4× — which
//! is exactly how the four aggregate sentences below went stale.
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
//! working_set ≈ active_merges × K × per_source
//!             = active_merges × K × max_inflight_rows(cap_per_channel)
//!             = 4 × active_merges × K × cap_per_channel
//! ```
//!
//! which for `active_merges ≥ EGRESS_ROW_BUDGET / MAX_CAP` is `≈ 4 × K × budget`
//! (the `/active` division cancels the outer `active` factor down to ONE budget
//! per source-fanout, and `per_source` keeps its 4×), and is floored by
//! `active_merges × K × max_inflight_rows(MIN_CAP)` = `4 × active_merges × K ×
//! MIN_CAP` — the deliberate cost of the forward-progress floor. Of that working
//! set, half is channel-RESIDENT (`2 ×` rather than `4 ×`); the other half is the
//! consumer-held and producer-parked batches, which are just as real to the
//! allocator and so belong in a MEMORY figure. The win vs. the fixed cap is
//! unchanged and is the removal of the per-source CONSTANT: `per_source` is a
//! multiple of `cap_per_channel` at every setting, so caps fall toward
//! [`MIN_CAP`] as concurrency climbs instead of every channel holding a fixed
//! `MAX_CAP` worth. Do NOT read this as a strict `≤ EGRESS_ROW_BUDGET` global
//! bound — it never was one, and since #2820 it is 4× further from one.
//!
//! Residual K-linear dimension: the budget divides by merge COUNT only, never by
//! per-merge fanout `K`, so a SINGLE wide merge still buffers up to
//! `K × max_inflight_rows(MAX_CAP)` = `4 × K × MAX_CAP` entries invariant to
//! concurrency — intended (the owner's "a solo merge is unchanged for any `K`"
//! contract), and 4× the pre-#2820 `K × MAX_CAP`, so the ~60 MB this doc used to
//! quote at `K = 100` is ~240 MB at the same ~2.4 KB/row. For any row shape fat
//! enough to matter the BYTE budget binds first and is the figure to reason with:
//! `K × egress_batch::max_inflight_bytes(cap, max_row_bytes)` =
//! `4 × K × (BATCH_EMIT_BYTES_MERGE + max_row_bytes)`, ≈ 4 MiB per source. Both
//! are worst-case row bounds against a STALLED consumer, not steady state. The
//! high-`K` envelope is validated by the #2895 loadgen sweep (deferred
//! follow-up).
//!
//! The snapshot is taken ONCE at construction and never revised, so it is
//! order-dependent, not fair: a long-lived merge that starts during a burst
//! stays PINNED at its low snapshot cap for its entire life, even after the
//! burst clears and concurrency drops back to one. Conversely a merge that
//! starts while the process is idle keeps the full 256 per channel even as later
//! merges arriving during ITS lifetime are squeezed toward [`MIN_CAP`] — the
//! throttle falls on the newcomers, not the incumbent.
//!
//! ## Operator knobs
//!
//! Both bounds are runtime-overridable env knobs (parsed ONCE per process — see
//! [`resolved`] — never on the per-merge hot path), defaulting to the shipped
//! values so behavior is unchanged when unset:
//!
//! * `CQLITE_EGRESS_ROW_BUDGET` → [`EGRESS_ROW_BUDGET`] (default `2048`): the
//!   per-active-merge-slot budget divided among a merge's channels.
//! * `CQLITE_EGRESS_MIN_CAP` → [`MIN_CAP`] (default `8`): the forward-progress
//!   floor, clamped to `[1, MAX_CAP]`; the budget is forced `≥ min_cap`.
//!
//! A missing/unparseable/zero value falls back to the default (never panics).
//!
//! ## AC#3 grounding (throughput evidence) — NOT "gap closed"
//!
//! With the DEFAULT budget `2048` / `MAX_CAP` `256` the throttle is INERT at ≤ 8
//! concurrent merges (`2048 / 8 = 256`, i.e. the cap stays at the pre-change
//! 256): it ENGAGES (per-channel cap falls below 256) only ABOVE 8 concurrent
//! merges, and the [`MIN_CAP`] floor only at `budget / min_cap` ≈ 256 concurrent
//! merges. So this change does NOT by itself "close" the #2600/#2367
//! backpressure gap — it installs the MECHANISM (and the operator knobs to tune
//! it); whether the DEFAULT bounds are the right ones at field concurrency is
//! validated/tuned by the **#2895** flight-loadgen sweep (deferred follow-up).
//!
//! The reverted `STREAMING_CHANNEL_CAPACITY = 32` experiment in #2765 measured
//! the egress-channel depth SHRINKING at FLAT qps and p99 — i.e. the buffering
//! was slack, not a throughput floor, so bounding it does not cost throughput at
//! the concurrency levels tested.
//!
//! This budget is ORTHOGONAL to the #2419 `channel_depth` gauge: that gauge
//! observes live occupancy; this bounds the per-channel capacity ceiling. Kept
//! out of `merge/mod.rs` to bound that file.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;

use crate::observability;

/// DEFAULT per-active-merge-SLOT egress budget in prefetched `MergeEntry` values
/// (issue #2765) — overridable at runtime by [`BUDGET_ENV`]. The per-channel
/// capacity a merge receives is `budget / active_merges` (clamped). Chosen so
/// that at the pre-change fixed capacity of 256 the budget is fully consumed by
/// ~8 concurrent merges; beyond that, per-channel capacity shrinks (down to
/// [`MIN_CAP`]) instead of every channel holding a fixed 256. See the module doc
/// for why this is a per-slot budget, NOT a strict global ceiling — and for
/// `per_source`, the quantity a solo merge's footprint actually multiplies:
/// `K × per_source` = `4 × K × MAX_CAP` rows since #2820, NOT `K × 256`. The
/// module doc carries the one derivation and the one per-row constant; do not
/// re-derive a second here, which is how these two figures drifted apart in the
/// first place. For a fat-row workload the BYTE term
/// (`egress_batch::max_inflight_bytes`, ≈4 MiB/source) binds first, and the
/// high-`K` envelope is #2895's to validate.
pub(super) const EGRESS_ROW_BUDGET: usize = 2048;

/// DEFAULT minimum per-channel capacity (issue #2765) — overridable at runtime
/// by [`MIN_CAP_ENV`]. Guarantees forward progress: no matter how many merges
/// are concurrently active, every producer can always place at least this many
/// entries, so a bounded `sync_channel` can never be constructed with capacity 0
/// (which would wedge the producer on its first `send`). Kept ≥ 1 by
/// construction, in both the default and the resolved-override path.
pub(super) const MIN_CAP: usize = 8;

/// Maximum per-channel capacity — the unchanged single-merge value
/// (`STREAMING_CHANNEL_CAPACITY` in `merge/mod.rs`). At LOW concurrency the cap
/// clamps up to this, so single-merge behavior is byte-for-byte unchanged. NOT
/// operator-overridable (it is the memory-bounded ceiling of one channel).
pub(super) const MAX_CAP: usize = super::STREAMING_CHANNEL_CAPACITY;

/// Operator env knob overriding [`EGRESS_ROW_BUDGET`] (issue #2765). Parsed ONCE
/// per process (see [`resolved`]); a missing/unparseable/zero value falls back
/// to the default, and the result is clamped `≥ resolved MIN_CAP`.
const BUDGET_ENV: &str = "CQLITE_EGRESS_ROW_BUDGET";

/// Operator env knob overriding [`MIN_CAP`] (issue #2765). Parsed ONCE per
/// process; a missing/unparseable/zero value falls back to the default, and the
/// result is clamped to `[1, MAX_CAP]` (the ≥1 forward-progress invariant).
const MIN_CAP_ENV: &str = "CQLITE_EGRESS_MIN_CAP";

/// The resolved `(budget, min_cap)` pair, read from the environment ONCE per
/// process into a `OnceLock` (mirroring `select_executor::forcing`'s cached-env
/// pattern) so env parsing is OFF the per-merge hot path. Every `capacity_for`
/// call reads this cached tuple, never `std::env`.
fn resolved() -> (usize, usize) {
    static RESOLVED: OnceLock<(usize, usize)> = OnceLock::new();
    *RESOLVED.get_or_init(|| {
        let (budget, min_cap) = resolve_budget(
            std::env::var(BUDGET_ENV).ok().as_deref(),
            std::env::var(MIN_CAP_ENV).ok().as_deref(),
        );
        // One-time operator signal (issue #2765): warn on the two inert-throttle
        // configs this predicate covers — the floor meets the ceiling
        // (`min_cap >= MAX_CAP`, so every channel is 256 at any concurrency) or
        // the range is degenerate (`budget < 2 × min_cap`, i.e. `budget / min_cap
        // <= 1`, so the cap is pinned at `min_cap` regardless of concurrency).
        // NOTE: this does NOT cover a very LARGE budget (e.g. 1_000_000), where
        // caps stay at 256 until ~`budget / MAX_CAP` concurrent merges with no
        // warn — that direction is intentionally un-warned here (no behavior
        // change). Runs once (OnceLock init).
        if min_cap >= MAX_CAP || budget / min_cap <= 1 {
            tracing::warn!(
                budget,
                min_cap,
                max_cap = MAX_CAP,
                "{BUDGET_ENV}/{MIN_CAP_ENV} leave the adaptive merge egress \
                 throttle INERT (per-channel capacity does not shrink with \
                 concurrency); see cqlite.merge.active_merges / #2765"
            );
        }
        (budget, min_cap)
    })
}

/// Pure resolver over the two raw env values (injectable seam for tests — the
/// #2451-safe alternative to mutating real process env in a shared-binary test).
/// Validates/clamps defensively and NEVER panics: a `None`/unparseable/zero
/// value falls back to the default; `min_cap` is clamped to `[1, MAX_CAP]` (the
/// forward-progress invariant) and `budget` to `≥ min_cap` (so `budget/1` can
/// never fall below the floor). Returns `(budget, min_cap)`.
fn resolve_budget(env_budget: Option<&str>, env_min_cap: Option<&str>) -> (usize, usize) {
    let parse = |raw: Option<&str>| {
        raw.and_then(|s| s.trim().parse::<usize>().ok())
            .filter(|&v| v > 0)
    };
    let min_cap = parse(env_min_cap).unwrap_or(MIN_CAP).clamp(1, MAX_CAP);
    let budget = parse(env_budget).unwrap_or(EGRESS_ROW_BUDGET).max(min_cap);
    (budget, min_cap)
}

/// Process-global live count of in-flight streaming merges. Incremented by
/// [`begin_merge`] when a merge's egress channel is constructed and decremented
/// by [`ActiveMergeGuard`]'s drop when that merge finishes (or is torn down).
static ACTIVE: AtomicUsize = AtomicUsize::new(0);

/// Record the operator-facing `cqlite.merge.active_merges` gauge (issue #2765),
/// mirroring the `producer_gauge::record` pattern: publish the post-transition
/// `level` the caller already computed from its own `fetch_add`/`fetch_sub`.
///
/// The gauge is EVENTUALLY-CONSISTENT, not strictly synchronized: the atomic
/// update and this record are two separate steps, so two concurrent transitions
/// can publish out of order and the gauge may briefly show a stale level until
/// the NEXT begin/drop re-publishes — the same lock-free convention as
/// `producer_gauge`. A lock/seqlock would be overkill for a diagnostic gauge.
fn record_active(level: usize) {
    observability::record_gauge(
        observability::catalog::MERGE_ACTIVE_MERGES,
        level as i64,
        &[],
    );
}

/// Current live process-global active-merge count (test/observability hook —
/// mirrors the [`MERGE_ACTIVE_MERGES`](crate::observability::catalog::MERGE_ACTIVE_MERGES)
/// gauge, exposed to integration tests via `merge::active_merge_count`).
pub(super) fn active_count() -> usize {
    ACTIVE.load(Ordering::SeqCst)
}

/// Resolve the per-merge channel capacity for a given live active-merge count,
/// using the runtime-[`resolved`] `(budget, min_cap)` (env-overridable).
///
/// `clamp(budget / active, min_cap, MAX_CAP)`. `active` is floored at 1 (via
/// [`usize::max`]) so the division is always well-defined and the result is
/// never 0 — combined with the `min_cap ≥ 1` clamp this is doubly safe against a
/// zero-capacity channel.
pub(super) fn capacity_for(active: usize) -> usize {
    let (budget, min_cap) = resolved();
    capacity_from(active, budget, min_cap)
}

/// The pure `clamp(budget / active, min_cap, MAX_CAP)` kernel — shared by
/// [`capacity_for`] (resolved values) and the override test (injected values).
/// `min_cap ≤ MAX_CAP` is guaranteed by [`resolve_budget`], so the clamp bounds
/// are always valid.
fn capacity_from(active: usize, budget: usize, min_cap: usize) -> usize {
    let active = active.max(1);
    (budget / active).clamp(min_cap, MAX_CAP)
}

/// The runtime-resolved minimum per-channel capacity (env-overridable). Used by
/// the wiring tests' clamp-range assertions so they track an override too.
#[cfg(test)]
pub(super) fn min_cap() -> usize {
    resolved().1
}

/// The runtime-resolved egress budget (env-overridable). Used by the wiring test
/// to size its concurrency against the SAME budget the production path resolves,
/// so it stays correct under an operator `CQLITE_EGRESS_ROW_BUDGET` override.
#[cfg(test)]
pub(super) fn budget() -> usize {
    resolved().0
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
///
/// Always targets the process-global [`ACTIVE`] and always publishes the gauge —
/// no injectable atomic and no `record` branch on this hot path (the per-test
/// private-atomic seam is the `#[cfg(test)]`-only [`begin_on_for_test`]).
#[must_use]
pub(super) fn begin_merge() -> (usize, ActiveMergeGuard) {
    let active = ACTIVE.fetch_add(1, Ordering::SeqCst) + 1;
    record_active(active);
    (capacity_for(active), ActiveMergeGuard)
}

/// RAII guard that decrements the process-global active-merge count (and
/// re-publishes the gauge) when a streaming merge finishes — on normal
/// completion, early return, or panic. Stored on the `KWayMerger` (dropped after
/// its runs/channels) so exactly one decrement pairs with the merge's single
/// [`begin_merge`] increment.
#[derive(Debug)]
pub(super) struct ActiveMergeGuard;

impl Drop for ActiveMergeGuard {
    fn drop(&mut self) {
        let level = ACTIVE.fetch_sub(1, Ordering::SeqCst) - 1;
        record_active(level);
    }
}

/// Test-only injectable seam: run the EXACT increment/capacity/guard-drop pairing
/// [`begin_merge`] uses, but against a PRIVATE `&'static AtomicUsize` (a
/// `Box::leak`ed per-test counter) and WITHOUT touching the process-global gauge
/// — so a test can drive deterministic concurrency without racing the shared
/// [`ACTIVE`] or publishing a bogus `cqlite.merge.active_merges` level (#2451
/// isolation). The returned guard decrements that private counter on drop.
#[cfg(test)]
pub(super) fn begin_on_for_test(counter: &'static AtomicUsize) -> (usize, TestMergeGuard) {
    let active = counter.fetch_add(1, Ordering::SeqCst) + 1;
    (capacity_for(active), TestMergeGuard { counter })
}

/// Test-only counterpart of [`ActiveMergeGuard`] bound to a private atomic; its
/// drop decrements that counter only (never the global gauge). See
/// [`begin_on_for_test`].
#[cfg(test)]
pub(super) struct TestMergeGuard {
    counter: &'static AtomicUsize,
}

#[cfg(test)]
impl Drop for TestMergeGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::SeqCst);
    }
}

impl super::KWayMerger {
    /// Attach the adaptive egress-budget slot guard (issue #2765) to a merger
    /// whose source channels were opened OUTSIDE its constructor — the point-read
    /// builders (`build_single_partition_merger*`) open their fail-safe adapters,
    /// with the shared capacity snapshot, before calling `from_row_iterators`,
    /// then move the matching guard onto the built merger here so it decrements
    /// exactly once at merge end. See [`begin_merge`].
    ///
    /// Takes the guard BY VALUE (not `Option`), so it is impossible to call this
    /// with `None` and silently un-register a live merge; the `debug_assert`
    /// additionally catches a double-attach that would drop a still-live guard
    /// early and under-count concurrency. Kept in this sibling module (not
    /// `merge/mod.rs`) to bound that over-threshold file (#1116). `pub(super)`
    /// (matching [`ActiveMergeGuard`]'s visibility) — uncallable outside `merge`.
    #[must_use]
    pub(super) fn with_egress_slot(mut self, egress_slot: ActiveMergeGuard) -> Self {
        debug_assert!(
            self._egress_slot.is_none(),
            "with_egress_slot must not overwrite a live active-merge slot"
        );
        self._egress_slot = Some(egress_slot);
        self
    }
}

/// Doc-hidden integration-test hook (issue #2765): the adaptive per-channel
/// egress capacity a NEW merge would receive at `active_merges` concurrent
/// merges — `clamp(EGRESS_ROW_BUDGET / active_merges, MIN_CAP, 256)`. Lets an
/// integration test derive an ADAPTIVE backpressure threshold instead of
/// hard-coding the pre-#2765 fixed 256. Re-exported from `merge`.
#[doc(hidden)]
pub fn egress_channel_capacity_for(active_merges: usize) -> usize {
    capacity_for(active_merges)
}

/// Doc-hidden integration-test hook (issue #2765): the live process-global count
/// of in-flight k-way merges (the `cqlite.merge.active_merges` gauge value).
/// Used with [`egress_channel_capacity_for`] to compute the current adaptive
/// per-channel capacity from an integration test. Re-exported from `merge`.
#[doc(hidden)]
pub fn active_merge_count() -> usize {
    active_count()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Evaluate the capacity kernel against the SHIPPED DEFAULTS, bypassing the
    /// env-reading `capacity_for` — so exporting `CQLITE_EGRESS_*` (an operator
    /// knob this change added) can never break these compile-time-constant
    /// assertions. The env plumbing itself is covered by
    /// `env_knobs_resolve_validate_and_drive_capacity`.
    fn cap_default(active: usize) -> usize {
        capacity_from(active, EGRESS_ROW_BUDGET, MIN_CAP)
    }

    #[test]
    fn low_concurrency_resolves_to_max_cap() {
        // Criterion 1: a single active merge gets the unchanged 256 cap.
        assert_eq!(cap_default(1), MAX_CAP);
        assert_eq!(MAX_CAP, 256);
        // Zero active (defensive) is floored to 1 → MAX_CAP, never a div-by-zero.
        assert_eq!(cap_default(0), MAX_CAP);
    }

    #[test]
    fn env_knobs_resolve_validate_and_drive_capacity() {
        // Inject via the PURE seam (`resolve_budget`), never real process env, so
        // this cannot perturb the OnceLock other tests observe (#2451-safe).
        // Unset → the shipped defaults (behavior unchanged when the knobs absent).
        assert_eq!(resolve_budget(None, None), (EGRESS_ROW_BUDGET, MIN_CAP));

        // A clean override flows through the SAME clamp kernel `capacity_for` uses.
        let (budget, min_cap) = resolve_budget(Some("4096"), Some("16"));
        assert_eq!((budget, min_cap), (4096, 16));
        assert_eq!(capacity_from(1, budget, min_cap), MAX_CAP); // 4096 clamps to 256
        assert_eq!(capacity_from(16, budget, min_cap), 256); // 4096/16 = 256
        assert_eq!(capacity_from(4096, budget, min_cap), 16); // floors at the raised min_cap

        // Defensive: missing / unparseable / zero each fall back to the default,
        // never a panic.
        assert_eq!(
            resolve_budget(Some(""), Some("nope")),
            (EGRESS_ROW_BUDGET, MIN_CAP)
        );
        assert_eq!(
            resolve_budget(Some("0"), Some("0")),
            (EGRESS_ROW_BUDGET, MIN_CAP)
        );
        assert_eq!(resolve_budget(Some("  1024  "), None), (1024, MIN_CAP)); // trims

        // Invariants: min_cap clamped to [1, MAX_CAP]; budget forced ≥ min_cap so
        // `budget/1` can never dip under the forward-progress floor.
        let (b, m) = resolve_budget(Some("10"), Some("100000"));
        assert_eq!(m, MAX_CAP, "min_cap clamped down to MAX_CAP");
        assert_eq!(b, MAX_CAP, "budget raised to ≥ min_cap");
        assert!(m >= 1, "forward-progress floor: min_cap ≥ 1");
        assert!(
            capacity_from(usize::MAX, b, m) >= 1,
            "never a zero-capacity channel"
        );
    }

    #[test]
    fn capacity_shrinks_with_concurrency_but_never_below_min() {
        // Budget divided among concurrent merges, clamped to the ceiling.
        assert_eq!(cap_default(8), EGRESS_ROW_BUDGET / 8); // exactly 256 (== MAX_CAP)
        assert_eq!(cap_default(16), EGRESS_ROW_BUDGET / 16); // 128
                                                             // Very high concurrency clamps to MIN_CAP (never 0 → forward progress).
        assert_eq!(cap_default(usize::MAX), MIN_CAP);
        assert!(MIN_CAP >= 1, "MIN_CAP must guarantee at least one slot");
    }

    #[test]
    fn per_slot_bound_holds_and_min_cap_floor_is_honest() {
        // The per-ACTIVE-MERGE-SLOT invariant (see module doc "Honest bound"):
        // at any concurrency `a`, one merge's per-channel share `cap_default(a)`
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
            let cap = cap_default(a);
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

        // Drive `n` concurrent MERGE registrations through the SAME
        // increment/snapshot logic `begin_merge` uses, via the test-only
        // `begin_on_for_test` seam against a PRIVATE atomic (issue #2451
        // isolation, mirroring the pairing test) so this test can
        // NEVER inflate the shared global `ACTIVE` and shrink a parallel
        // merger-building test's caps. Assert the per-channel capacity a merge is
        // HANDED shrinks below the fixed 256 as concurrent merges rise — and
        // never below the resolved floor. (KWayMerger-level end-to-end wiring, on
        // the real global, lives in `egress_wiring_tests`.)
        //
        // `n` is sized from the RESOLVED budget (not a fixed 16): `begin_on_for_test`
        // calls the env-resolved `capacity_for`, so under `CQLITE_EGRESS_ROW_BUDGET=8192`
        // a fixed 16 would leave `8192/16 == 512` clamped back to 256 and never
        // shrink. `budget/MAX_CAP + 8` makes the last-registering thread see
        // `capacity_for(n) < MAX_CAP` for any realistic budget — but is CLAMPED to
        // 64 so a pathological `CQLITE_EGRESS_ROW_BUDGET` can't spawn thousands of
        // threads. If the clamp defeats the shrink precondition (a huge budget
        // where even 64 concurrency stays at the 256 clamp), skip: the property is
        // unobservable within a bounded workload, not violated.
        // UNCONDITIONAL kernel-path proof (env-INDEPENDENT — uses the compile-time
        // DEFAULT budget/floor via `capacity_from`): at the crossover concurrency
        // the default kernel shrinks below the 256 ceiling. So even when the
        // threaded phase below is skipped under a large env override, this test
        // always proves the shrink math.
        let default_crossover = EGRESS_ROW_BUDGET / MAX_CAP + 1;
        assert!(
            capacity_from(default_crossover, EGRESS_ROW_BUDGET, MIN_CAP) < MAX_CAP,
            "default kernel must shrink below {MAX_CAP} at concurrency {default_crossover}"
        );

        let counter: &'static AtomicUsize = Box::leak(Box::new(AtomicUsize::new(0)));
        let floor = min_cap();
        let n = (budget() / MAX_CAP + 8).min(64);
        if capacity_for(n) >= MAX_CAP {
            eprintln!(
                "SKIP: concurrent_begin_shrinks_per_channel_capacity — budget={} \
                 min_cap={} defeats the bounded-workload shrink precondition \
                 (clamp={n}); default kernel path asserted above",
                budget(),
                floor
            );
            return;
        }
        let registered = Arc::new(Barrier::new(n));
        let released = Arc::new(Barrier::new(n));

        let handles: Vec<_> = (0..n)
            .map(|_| {
                let registered = registered.clone();
                let released = released.clone();
                std::thread::spawn(move || {
                    // Register FIRST (increments the PRIVATE count + snapshots the
                    // cap), THEN barrier — so all `n` are counted before any reads;
                    // the second barrier holds every guard alive until all have
                    // observed, modelling `n` concurrent merges. `begin_on_for_test`
                    // never publishes the global gauge (structural suppression).
                    let (cap, _guard) = begin_on_for_test(counter);
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

        // The private atomic is touched ONLY by this test's `n` threads, so the
        // observed concurrency is EXACTLY `n` (deterministic, not `>=`).
        let max_live = results.iter().map(|(l, _)| *l).max().unwrap_or(0);
        assert_eq!(
            max_live, n,
            "private-atomic concurrency must be exactly {n}"
        );
        // The merges that registered while the count was already high got a
        // capacity strictly below the pre-change fixed 256 — the shrink property.
        let min_cap_seen = results.iter().map(|(_, c)| *c).min().unwrap_or(MAX_CAP);
        assert!(
            min_cap_seen < MAX_CAP,
            "with {n} concurrent merges at least one per-channel cap must fall \
             below {MAX_CAP} (min seen = {min_cap_seen})"
        );
        // Forward progress: no merge is ever handed a sub-floor capacity (uses the
        // RESOLVED floor, so it holds under a `CQLITE_EGRESS_MIN_CAP` override).
        for (_, cap) in &results {
            assert!(
                *cap >= floor,
                "per-channel cap {cap} >= resolved floor {floor}"
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
        // `channel_depth`'s per-test-atomic pattern): `begin_on_for_test`
        // increments and the returned guard's drop decrements, so the count
        // returns to baseline on every scope exit. A private atomic cannot be
        // perturbed by the other tests in this binary that drive real merges
        // through the shared global, and `begin_on_for_test` never publishes the
        // process-global operator gauge (structural suppression).
        let counter: &'static AtomicUsize = Box::leak(Box::new(AtomicUsize::new(0)));
        assert_eq!(counter.load(Ordering::SeqCst), 0);
        {
            let (cap0, _g0) = begin_on_for_test(counter);
            assert_eq!(counter.load(Ordering::SeqCst), 1);
            assert_eq!(cap0, capacity_for(1), "first merge sees active=1");
            {
                let (cap1, _g1) = begin_on_for_test(counter);
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
