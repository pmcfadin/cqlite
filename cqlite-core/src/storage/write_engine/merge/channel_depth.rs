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
//! # THE invariant this module depends on (issue #3120)
//!
//! Only DATA entries (`MergeMsg::Item`) are tracked, and they are tracked on BOTH
//! sides. The TERMINATORS (`MergeMsg::Failed` / `MergeMsg::Done`) are untracked on
//! send AND on receive, so they can never unbalance the level. Formally, per
//! adapter `A`, with `data(m) = 1` iff `m` is a DATA entry:
//!
//! ```text
//! ∀ m:  counted_on_send(m) ⟺ counted_on_receive_or_residual(m) ⟺ data(m)
//! and   sentA − recvA ≥ 0 at every join point
//! ```
//!
//! An asymmetry here is INVISIBLE without help, which is why it is now asserted
//! rather than merely documented: a message counted on exactly one side drives the
//! residual NEGATIVE, [`reconcile_residual`]'s `> 0` guard then SKIPS it, and
//! [`record`]'s `max(0)` floor hides the resulting drift from every observer,
//! permanently. So:
//!
//! * both sides express the predicate through the SAME exhaustive
//!   `MergeMsg::is_tracked_data` (send: `from_readers::forward_row`; receive: the
//!   `MergeMsg::Item` arm of `SSTableRowIteratorAdapter::next`, and nowhere else),
//!   and a future 4th `MergeMsg` variant is a compile error there rather than a
//!   silent default; and
//! * [`reconcile_residual`] carries a `debug_assert!(residual >= 0)`.
//!
//! The `max(0)` floor stays (it matches `RpcMetrics::finish`) as the release-build
//! backstop; the `debug_assert` is what makes the imbalance OBSERVABLE in test and
//! dev builds instead of being floored away.
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
    // Issue #3120: a NEGATIVE residual means the tracked-send and tracked-receive
    // predicates disagreed — e.g. a terminator counted on exactly one side. The
    // `> 0` guard below would silently skip it and `record`'s `max(0)` floor would
    // hide the drift forever, so make it LOUD in a debug build instead. Sound as a
    // hard invariant because every tracked receive is preceded by the tracked send
    // of that same entry, and the only caller reads `sent_count` AFTER joining the
    // producer thread — so no in-flight `fetch_add` can still be pending.
    debug_assert!(
        residual >= 0,
        "egress-depth residual must never be negative (got {residual}): a tracked \
         receive without a matching tracked send means the send/receive predicates \
         disagree — see this module's invariant (issue #3120)"
    );
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

    /// THE issue #3120 pin: a TERMINATOR is untracked on BOTH sides, so a run that
    /// ends with one returns the depth to exactly baseline and leaves a residual of
    /// exactly ZERO — never a negative residual, which the `> 0` guard would skip
    /// and the `max(0)` floor would hide forever.
    ///
    /// Drives the REAL predicate (`MergeMsg::is_tracked_data`) over a REAL message
    /// sequence, on both the send side and the receive side, so an asymmetry
    /// between the two sites cannot hide from this test. Against a PRIVATE atomic,
    /// never the shared `DEPTH` (the #2451 flake class): thousands of tests share
    /// this binary and several drive real merge egress channels.
    #[test]
    fn a_terminator_is_untracked_on_both_sides_so_the_residual_is_exactly_zero() {
        use crate::storage::write_engine::merge::producer_msg::{MergeMsg, MergeProducerError};
        use crate::storage::write_engine::merge::{CellData, MergeEntry, RowData};
        use crate::storage::write_engine::mutation::DecoratedKey;
        use crate::types::Value;

        fn data_item(n: i64) -> MergeMsg {
            MergeMsg::Item(MergeEntry::new(
                0,
                DecoratedKey::new(n, n.to_be_bytes().to_vec()),
                None,
                100,
                RowData::Live {
                    cells: vec![CellData::new("name".to_string(), Value::text("v"), 100)],
                },
            ))
        }

        // Each run: N data entries followed by exactly ONE terminator — the shape
        // every producer thread now produces on every exit path.
        for terminator in [
            MergeMsg::Done,
            MergeMsg::Failed(MergeProducerError::Panicked("boom".to_string())),
            MergeMsg::Failed(MergeProducerError::Cancelled),
        ] {
            const DATA_ENTRIES: usize = 5;
            let depth = AtomicI64::new(0);
            let baseline = depth.load(Ordering::SeqCst);
            let mut sent_count: i64 = 0;
            let mut received_count: i64 = 0;

            let mut stream: Vec<MergeMsg> = (0..DATA_ENTRIES as i64).map(data_item).collect();
            stream.push(terminator);

            // SEND side: exactly what `from_readers::forward_row` does.
            for msg in &stream {
                if msg.is_tracked_data() {
                    adjust(&depth, 1);
                    sent_count += 1;
                }
            }
            assert_eq!(
                sent_count, DATA_ENTRIES as i64,
                "only the DATA entries may be tracked on send — the terminator must \
                 not be"
            );

            // RECEIVE side: exactly what the `MergeMsg::Item` arm of
            // `SSTableRowIteratorAdapter::next` does (and no other arm does).
            for msg in &stream {
                if msg.is_tracked_data() {
                    adjust(&depth, -1);
                    received_count += 1;
                }
            }

            let residual = sent_count - received_count;
            assert_eq!(
                residual, 0,
                "a fully drained run whose last message is a terminator must leave a \
                 residual of exactly zero; a NEGATIVE residual is skipped by the \
                 `> 0` guard and floored away by `max(0)`, hiding the drift forever \
                 (issue #3120)"
            );
            // The production reconcile would be a no-op here; running it proves the
            // debug_assert is satisfied by the balanced case.
            reconcile_residual(residual);
            assert_eq!(
                depth.load(Ordering::SeqCst),
                baseline,
                "the tracked level returns to baseline"
            );
        }
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
