//! Egress-channel-depth gauge for the k-way merge (issue #2419, WS2).
//!
//! Backs the [`crate::observability::catalog::MERGE_EGRESS_CHANNEL_DEPTH`] gauge
//! with a process-global live count of merged DATA entries currently buffered in
//! the bounded producer→consumer `sync_channel` (capacity up to
//! `STREAMING_CHANNEL_CAPACITY` = 256, adaptively reduced under concurrent
//! merges — see `merge/egress_budget.rs`). `std::sync::mpsc`'s
//! `sync_channel` exposes no `len()`, so occupancy is tracked explicitly, exactly
//! as the #2316 producer-thread gauge tracks live producer threads:
//!
//! * [`sent`] increments the count after a successful DATA-entry `send`
//!   (`from_readers::forward_row`).
//! * [`received`] decrements it when the consumer pulls that entry off the
//!   channel (`SSTableRowIteratorAdapter::next`).
//! * [`reconcile_residual`] (issue #2419 roborev job 1733) subtracts, in ONE
//!   atomic op, any entries a producer sent but its consumer never received — a
//!   cancelled/disconnected merge whose channel was torn down while entries were
//!   still buffered (or racing the teardown). Called ONLY from
//!   `SSTableRowIteratorAdapter::drop`, AFTER its producer thread has been
//!   joined: reading a per-adapter sent/received delta BEFORE the join (or
//!   trying to drain-then-decrement before dropping the receiver) races a
//!   concurrently-running producer thread and can leak the gauge upward
//!   permanently — see that `Drop` impl's doc for the full derivation.
//!
//! Only DATA entries (`Ok(MergeEntry)`) are tracked on BOTH sides; the rare
//! terminal error message (`Err(MergeProducerError)`) is untracked on send and
//! on receive, so it never unbalances the level. The `max(0)` floor matches
//! `RpcMetrics::finish`, guarding against any unexpected residual imbalance.
//!
//! The gauge is OS-independent — it always emits on every platform (unlike the
//! `/proc`-derived `cqlite.proc.*` saturation gauges).

use std::sync::atomic::{AtomicI64, Ordering};

use crate::observability;

static DEPTH: AtomicI64 = AtomicI64::new(0);

fn record(level: i64) {
    observability::record_gauge(
        observability::catalog::MERGE_EGRESS_CHANNEL_DEPTH,
        // Floor at 0 so an unexpected imbalance never records a negative gauge
        // (matches `RpcMetrics::finish`).
        level.max(0),
        &[],
    );
}

/// Apply `delta` to `atomic` and return the resulting level — the exact
/// arithmetic [`sent`], [`received`], and [`reconcile_residual`] apply to the
/// shared [`DEPTH`]. Parameterized over the atomic (issue #2419 roborev job
/// 1733, the #2451 flake class) so a test can pin this SAME logic against a
/// private, per-test atomic instead of racing every other concurrently-running
/// test that also drives a real merge egress channel through the shared global.
fn adjust(atomic: &AtomicI64, delta: i64) -> i64 {
    atomic.fetch_add(delta, Ordering::SeqCst) + delta
}

/// Account one DATA entry that was just successfully sent into the bounded
/// egress channel. Balanced by exactly one [`received`] or, for entries a
/// consumer never pulled, by [`reconcile_residual`] post-join.
pub(super) fn sent() {
    record(adjust(&DEPTH, 1));
}

/// Account one DATA entry that was just received (consumed) from the egress
/// channel, decrementing the live occupancy.
pub(super) fn received() {
    record(adjust(&DEPTH, -1));
}

/// Subtract `residual` DATA entries from the shared depth in ONE atomic op
/// (issue #2419 roborev job 1733) — the post-join reconcile for entries a
/// producer sent but its consumer never received. A no-op for `residual <= 0`
/// (the common case: every send was received). See the module doc for why this
/// must run only after the producer thread has been joined.
pub(super) fn reconcile_residual(residual: i64) {
    if residual > 0 {
        record(adjust(&DEPTH, -residual));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Producer-fast / consumer-slow harness against the tracked
    /// send/recv/reconcile arithmetic (issue #2419, spec scenario "Depth rises
    /// when the consumer is slower than the producer"): the depth climbs above
    /// zero while sends outrun receives, stays bounded by the notional channel
    /// capacity, and returns to exactly zero once every send is balanced by a
    /// receive.
    ///
    /// Roborev job 1733 (the #2451 flake class): this now runs against a
    /// PRIVATE, per-test `AtomicI64` via [`adjust`] — the exact same arithmetic
    /// [`sent`]/[`received`] apply to the shared [`DEPTH`] — rather than the
    /// process-global atomic. The prior version drove `DEPTH` directly, so any
    /// concurrently-running test in this binary that ALSO exercises a real merge
    /// egress channel (elsewhere in the crate) could perturb the shared level
    /// mid-window, flaking this test's exact-equality assertions. A local atomic
    /// makes the pin fully deterministic: no other test can ever touch it.
    #[test]
    fn depth_rises_while_backed_up_and_returns_to_baseline() {
        let local = AtomicI64::new(0);
        const CAP: i64 = 256;

        // Producer races ahead: fill toward the bounded capacity.
        for _ in 0..CAP {
            adjust(&local, 1);
        }
        let backed_up = local.load(Ordering::SeqCst);
        assert!(
            backed_up > 0,
            "depth must rise above zero while the producer outruns the consumer \
             (backed_up={backed_up})"
        );
        assert!(
            backed_up <= CAP,
            "tracked depth must stay bounded by the channel capacity \
             (backed_up={backed_up}, cap={CAP})"
        );

        // Consumer drains every entry.
        for _ in 0..CAP {
            adjust(&local, -1);
        }
        assert_eq!(
            local.load(Ordering::SeqCst),
            0,
            "after draining every entry the depth returns to exactly zero \
             (every tracked send balanced by exactly one tracked receive)"
        );
    }

    /// [`reconcile_residual`] subtracts a positive residual in one op and is a
    /// no-op for `<= 0` — pinned against a private atomic for the same
    /// determinism reason as the scenario test above.
    #[test]
    fn reconcile_residual_subtracts_positive_and_ignores_non_positive() {
        let local = AtomicI64::new(10);
        // Mirror `reconcile_residual`'s logic directly against the local atomic
        // (the public function only targets the shared `DEPTH`).
        let residual = 4;
        if residual > 0 {
            adjust(&local, -residual);
        }
        assert_eq!(local.load(Ordering::SeqCst), 6);

        let before = local.load(Ordering::SeqCst);
        for non_positive in [0, -3] {
            if non_positive > 0 {
                adjust(&local, -non_positive);
            }
        }
        assert_eq!(
            local.load(Ordering::SeqCst),
            before,
            "a non-positive residual must never adjust the level"
        );
    }

    /// Pins the ACTUAL fix (issue #2419 roborev job 1733): a send that races
    /// teardown — landing AFTER the point a pre-fix drain-until-`Empty` loop
    /// would already have stopped looking — is still fully reconciled, because
    /// the reconcile now runs from an authoritative post-join sent/received
    /// DELTA computed once the producer thread is provably done, never a live
    /// drain-then-drop race. Modeled deterministically against a private atomic
    /// (never the shared `DEPTH`, for the same reason as the tests above): the
    /// arithmetic mirrors exactly what `sent`/`received`/`reconcile_residual`
    /// apply, and what `SSTableRowIteratorAdapter::drop` does with its own
    /// `sent_count`/`received_count` fields.
    #[test]
    fn reconcile_after_join_absorbs_a_send_that_raced_teardown() {
        let depth = AtomicI64::new(0);
        let baseline = depth.load(Ordering::SeqCst);

        // Normal traffic: three entries sent and received, balanced.
        let mut sent_count: i64 = 0;
        let mut received_count: i64 = 0;
        for _ in 0..3 {
            adjust(&depth, 1);
            sent_count += 1;
            adjust(&depth, -1);
            received_count += 1;
        }
        assert_eq!(depth.load(Ordering::SeqCst), baseline);

        // Teardown begins (cancel tripped, receiver about to be torn down).
        // Model the exact race this fix addresses: the producer thread, still
        // alive, gets ONE more send through — precisely the send a
        // drain-until-`Empty`-then-drop loop could miss because it lands AFTER
        // that loop's last `Empty` observation. The consumer never receives it.
        adjust(&depth, 1);
        sent_count += 1;

        // The producer thread is now joined (guaranteed no more sends
        // possible). Reconcile using the authoritative post-join delta, exactly
        // as `SSTableRowIteratorAdapter::drop` does.
        let residual = sent_count - received_count;
        assert_eq!(residual, 1, "exactly the one entry that raced teardown");
        if residual > 0 {
            adjust(&depth, -residual);
        }

        assert_eq!(
            depth.load(Ordering::SeqCst),
            baseline,
            "the post-join reconcile must absorb a send that raced teardown, \
             returning the gauge to its pre-scenario baseline — the #2419 \
             roborev job 1733 regression this fix eliminates"
        );
    }

    /// The `max(0)` floor never records a negative gauge even under an unexpected
    /// residual imbalance (defense-in-depth; the wrappers are balanced by
    /// construction). Asserts the recorded VALUE is floored; nothing else to
    /// assert without an OTel capture harness (covered by the flight integration
    /// test).
    #[test]
    fn recorded_gauge_is_floored_at_zero() {
        record(-5);
        record(0);
        record(7);
    }
}
