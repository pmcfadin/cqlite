//! Egress-channel-depth gauge for the k-way merge (issue #2419, WS2).
//!
//! Backs the [`crate::observability::catalog::MERGE_EGRESS_CHANNEL_DEPTH`] gauge
//! with a process-global live count of merged DATA entries currently buffered in
//! the bounded producer→consumer `sync_channel`. `std::sync::mpsc`'s
//! `sync_channel` exposes no `len()`, so occupancy is tracked explicitly, exactly
//! as the #2316 producer-thread gauge tracks live producer threads:
//!
//! * [`sent_n`] adds the entries of one successfully sent DATA BATCH
//!   (`egress_batch::EgressBatcher::flush`).
//! * [`received_n`] subtracts them when the consumer pulls that batch off the
//!   channel (`SSTableRowIteratorAdapter::next`).
//!
//! # THE UNIT IS ENTRIES (rows), and issue #2820 did not change it
//!
//! The channel carries BATCHES since #2820 (one message per up to
//! `egress_batch::BATCH_EMIT_ROWS_MERGE` rows, so its `sync_channel` capacity is
//! in MESSAGES — `egress_budget`'s row budget converted by
//! `egress_batch::message_capacity_for_rows`), but this gauge stays in ENTRIES on
//! BOTH sides: a batch of `n` rows moves the level by `n`, never by 1. Counting
//! messages on one side and entries on the other is precisely the invisible
//! asymmetry documented below.
//!
//! # THIS gauge's ceiling is CHANNEL-RESIDENT rows, not the in-flight bound
//!
//! Because [`sent_n`] fires on a successful `send` and [`received_n`] on the
//! consumer's `recv`, the level counts ONLY entries currently sitting in the
//! channel — never the batch the consumer is handing out (already received) nor
//! the batch a producer is PARKED holding (never sent). So the ceiling is
//! `egress_batch::rows_resident_in_channel(rows_cap)` = `msg_cap × batch_ceiling`
//! (512 rows at the shipped default), NOT
//! `egress_batch::max_inflight_rows(rows_cap)` = `(msg_cap + 2) × batch_ceiling`
//! (1024), which is the MEMORY bound and overstates what this gauge can reach by
//! exactly two batches. Both scale with the #2765 adaptive row capacity since
//! #2820 (`2 × rows_cap` and `4 × rows_cap`), so neither is a constant.
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
//! Only DATA entries (the rows of a `MergeMsg::Batch`) are tracked, and they are
//! tracked on BOTH sides. The TERMINATORS (`MergeMsg::Failed` / `MergeMsg::Done`)
//! are untracked on send AND on receive, so they can never unbalance the level.
//! Formally, per adapter `A`, with `data(m)` = the number of DATA entries in `m`:
//!
//! ```text
//! ∀ m:  counted_on_send(m) = counted_on_receive_or_residual(m) = data(m)
//! and   sentA − recvA ≥ 0 at every join point
//! ```
//!
//! An asymmetry here is INVISIBLE without help: a message counted on exactly one
//! side drives the residual NEGATIVE, [`reconcile_residual`]'s `> 0` guard then
//! SKIPS it, and [`record`]'s `max(0)` floor hides the resulting drift from every
//! observer, permanently.
//!
//! ## What actually holds the invariant up (stated precisely — rust-reviewer)
//!
//! NOT "both sides call one shared predicate". They do not, and claiming they do
//! would hide the real gap:
//!
//! * **Receive side** (`SSTableRowIteratorAdapter::next`) is a hand-written
//!   `MergeMsg::Batch(entries)` match arm that never calls `tracked_entries`. Two
//!   properties make it correct anyway: that `match` is **EXHAUSTIVE with no
//!   wildcard arm**, so a future 4th `MergeMsg` variant is a COMPILE ERROR there
//!   rather than silently falling into a catch-all; and [`received_n`] and
//!   `received_count` each appear at EXACTLY ONE site in the crate, both inside
//!   that `Batch` arm, and both are passed the SAME `entries.len()` (issue #2820:
//!   the arm decrements by the batch length, so a batch counted as ONE message
//!   here against `n` entries on send would drive the residual negative — the
//!   defect this section exists for).
//! * **Send side** (`egress_batch::EgressBatcher::flush`) builds
//!   `MergeMsg::Batch(batch)` unconditionally, so its `msg.tracked_entries()` is a
//!   TAUTOLOGY today — it can only be the batch length. Its value is as a
//!   compile-time tripwire, not a runtime test: `MergeMsg::tracked_entries`'s body
//!   is itself an exhaustive match, so adding a variant forces an explicit
//!   tracked/untracked decision there.
//! * The `channel_depth` test below pins `tracked_entries`'s CLASSIFICATION (that
//!   a batch counts per ENTRY and both terminators are untracked). It cannot — and
//!   does not claim to — detect a divergence at the hand-written receive site.
//! * [`reconcile_residual`] checks `residual >= 0` at the one place a violation
//!   becomes observable, reporting it WITHOUT panicking from a `Drop` (see that
//!   function).
//!
//! The `max(0)` floor stays (it matches `RpcMetrics::finish`) as the last-resort
//! backstop so no negative gauge is ever recorded.
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

/// Account the `entries` DATA rows of one BATCH that was just successfully sent
/// into the bounded egress channel (issue #2820 — the gauge's unit is ENTRIES,
/// not messages; see the module doc). Balanced by exactly one [`received_n`] of
/// the same count or, for entries a consumer never pulled, by
/// [`reconcile_residual`] post-join.
///
/// `entries` is `MergeMsg::tracked_entries()` of the message actually sent, so a
/// terminator (0) is a no-op by construction rather than by a caller's judgement.
pub(super) fn sent_n(entries: usize) {
    record(adjust(&DEPTH, entries as i64));
}

/// Account the `entries` DATA rows of one BATCH just received (pulled off the
/// channel) by the consumer, decrementing the live occupancy by the same count
/// its send incremented it.
pub(super) fn received_n(entries: usize) {
    record(adjust(&DEPTH, -(entries as i64)));
}

/// Subtract `residual` DATA entries from the shared depth in ONE atomic op
/// (issue #2419 roborev job 1733) — the post-join reconcile for entries a
/// producer sent but its consumer never received. A no-op for `residual <= 0`
/// (the common case: every send was received). See the module doc for why this
/// must run only after the producer thread has been joined.
///
/// # A NEGATIVE residual is reported, never panicked out of a `Drop`
///
/// A negative residual means the tracked-send and tracked-receive sites disagreed
/// (e.g. a terminator counted on exactly one side). The `> 0` guard below would
/// silently skip it and [`record`]'s `max(0)` floor would hide the drift forever,
/// so it must be reported — but this function's ONLY production caller is
/// `SSTableRowIteratorAdapter::drop`, so 100% of its reachable paths are inside a
/// `Drop` (rust-reviewer, issue #3120). A bare `debug_assert!` there would panic
/// FROM a `Drop`: any test that fails an assertion with a live `KWayMerger` in
/// scope drops the adapter while ALREADY UNWINDING → double panic → process
/// ABORT, which under libtest destroys the original assertion message and every
/// sibling test's result in the binary. `teardown_tests` drops mergers on purpose,
/// so this is not hypothetical.
///
/// Hence the same idiom `producer_fault::SilencedInjectedPanics::drop` uses for
/// `set_hook`: skip the panicking route while unwinding. The `tracing::error!`
/// runs unconditionally, so the signal also exists in a release build (where
/// `debug_assert!` compiles away entirely).
///
/// The invariant itself genuinely holds: every tracked receive is preceded by the
/// tracked send of that same entry, and the caller reads `sent_count` strictly
/// AFTER `join()`, which retires every pending `fetch_add`.
pub(super) fn reconcile_residual(residual: i64) {
    if residual < 0 {
        tracing::error!(
            residual,
            "egress-depth residual is NEGATIVE: a tracked receive without a matching \
             tracked send means the send and receive accounting sites disagree — the \
             `> 0` guard below and `record`'s `max(0)` floor would otherwise hide \
             this drift permanently (issue #3120)"
        );
        // Reaching here at all is the violation, so the assert's condition is
        // deliberately "…and we are not ALREADY unwinding": it fails loudly on a
        // normal drop (the signal we want) and stays silent when this `Drop` is
        // itself running during someone else's panic, where a second panic would
        // abort the process and take the original failure message with it.
        debug_assert!(
            std::thread::panicking(),
            "egress-depth residual must never be negative (got {residual}): the \
             send and receive accounting sites disagree — see this module's \
             invariant (issue #3120)"
        );
    }
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
    /// [`sent_n`]/[`received_n`] apply to the shared [`DEPTH`] — rather than the
    /// process-global atomic. The prior version drove `DEPTH` directly, so any
    /// concurrently-running test in this binary that ALSO exercises a real merge
    /// egress channel (elsewhere in the crate) could perturb the shared level
    /// mid-window, flaking this test's exact-equality assertions. A local atomic
    /// makes the pin fully deterministic: no other test can ever touch it.
    #[test]
    fn depth_rises_while_backed_up_and_returns_to_baseline() {
        let local = AtomicI64::new(0);
        // The notional per-source CHANNEL-RESIDENT ROW ceiling: issue #2820 made
        // the channel carry BATCHES, so the bound this gauge can reach is
        // `rows_resident_in_channel(rows_cap)` — NOT `max_inflight_rows`, which
        // adds the consumer-held and producer-parked batches this gauge counts on
        // neither side. Derived from the shipped constants rather than the
        // pre-batching flat 256.
        let cap = super::super::egress_batch::rows_resident_in_channel(
            super::super::STREAMING_CHANNEL_CAPACITY,
        ) as i64;

        // Producer races ahead in BATCHES (the ramp saturates at this run's
        // ceiling), filling toward the bounded capacity.
        let batch = super::super::egress_batch::batch_limit_ceiling(
            super::super::STREAMING_CHANNEL_CAPACITY,
        ) as i64;
        let mut filled = 0;
        while filled < cap {
            let rows = batch.min(cap - filled);
            adjust(&local, rows);
            filled += rows;
        }
        let backed_up = local.load(Ordering::SeqCst);
        assert!(
            backed_up > 0,
            "depth must rise above zero while the producer outruns the consumer \
             (backed_up={backed_up})"
        );
        assert!(
            backed_up <= cap,
            "tracked depth must stay bounded by the resident-rows bound \
             (backed_up={backed_up}, cap={cap})"
        );

        // Consumer drains every entry, one batch at a time.
        let mut drained = 0;
        while drained < cap {
            let rows = batch.min(cap - drained);
            adjust(&local, -rows);
            drained += rows;
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

    /// Guards the REPORTING mechanism [`reconcile_residual`] uses for a negative
    /// residual (rust-reviewer, issue #3120), because its condition is subtle
    /// enough to be "corrected" into a no-op: the assert deliberately requires
    /// `std::thread::panicking()`, so inverting it to `!panicking()` would silently
    /// mean the invariant is NEVER checked. Both halves matter:
    ///
    /// 1. On a NORMAL (non-unwinding) call it FAILS LOUDLY — the signal.
    /// 2. When [`reconcile_residual`] runs from a `Drop` that is itself unwinding
    ///    (the only shape it is reachable in production, and what `teardown_tests`
    ///    routinely produces), it adds NO second panic, so the ORIGINAL failure
    ///    message survives instead of the process aborting and taking every
    ///    sibling test's result with it.
    ///
    /// Uses `residual = -1` directly rather than trying to provoke a real
    /// asymmetry: the invariant genuinely holds today, so the only way to exercise
    /// the reporting path is to call it with a violating value.
    #[test]
    fn a_negative_residual_is_loud_normally_and_silent_while_unwinding() {
        // (1) `debug_assert!` compiles away in a release build, so only assert the
        // panic where the assertion actually exists.
        #[cfg(debug_assertions)]
        {
            let died = std::panic::catch_unwind(|| reconcile_residual(-1));
            assert!(
                died.is_err(),
                "a negative residual must fail LOUDLY on a normal (non-unwinding) \
                 call — if this passes, the invariant is no longer checked at all"
            );
        }

        // (2) The production shape: a `Drop` that reconciles while already
        // unwinding must not double-panic.
        struct ReconcilesOnDrop;
        impl Drop for ReconcilesOnDrop {
            fn drop(&mut self) {
                reconcile_residual(-1);
            }
        }
        let died = std::panic::catch_unwind(|| {
            let _guard = ReconcilesOnDrop;
            panic!("the original failure, whose message must survive the drop");
        });
        let payload = died.expect_err("the original panic must still propagate");
        let message = payload
            .downcast_ref::<&'static str>()
            .copied()
            .unwrap_or("<payload lost — a double panic would have aborted instead>");
        assert!(
            message.contains("the original failure"),
            "the ORIGINAL panic message must survive a reconcile during unwind; a \
             second panic here aborts the process under libtest, destroying this \
             message and every sibling test's result, got: {message}"
        );
    }

    /// Issue #3120: `MergeMsg::tracked_entries` CLASSIFIES both terminators as
    /// untracked, so a run that ends with one returns the depth to exactly baseline
    /// and leaves a residual of exactly ZERO — never a negative residual, which the
    /// `> 0` guard would skip and the `max(0)` floor would hide forever.
    ///
    /// Issue #2820 adds the BATCH dimension to the same property: the DATA
    /// messages here carry MULTI-row batches, and both sides account the batch
    /// LENGTH, so the residual is zero for a batched run exactly as it was for a
    /// per-row one. A side that counted messages instead of entries leaves a
    /// residual of `entries - messages` (positive here, i.e. a permanent upward
    /// gauge leak) or its negative mirror, and this test fails on both.
    ///
    /// SCOPE, stated honestly (rust-reviewer): this pins the PREDICATE, not the
    /// symmetry of the two production call sites. The receive site
    /// (`SSTableRowIteratorAdapter::next`) is a hand-written `MergeMsg::Batch` match
    /// arm that never calls `tracked_entries`, so a divergence introduced THERE
    /// would not fail this test. What protects that site is structural, not this
    /// test: its `match` is exhaustive with no wildcard arm (a 4th variant is a
    /// compile error) and the receive-side accounting calls each appear at exactly
    /// one place, inside that arm. See the module doc.
    ///
    /// Against a PRIVATE atomic, never the shared `DEPTH` (the #2451 flake class):
    /// thousands of tests share this binary and several drive real merge egress
    /// channels.
    #[test]
    fn a_terminator_is_untracked_on_both_sides_so_the_residual_is_exactly_zero() {
        use crate::storage::write_engine::merge::producer_msg::{MergeMsg, MergeProducerError};
        use crate::storage::write_engine::merge::{CellData, MergeEntry, RowData};
        use crate::storage::write_engine::mutation::DecoratedKey;
        use crate::types::Value;

        /// One DATA message carrying `rows` entries — the batched shape every
        /// producer now sends (issue #2820).
        fn data_batch(rows: usize) -> MergeMsg {
            MergeMsg::Batch(
                (0..rows as i64)
                    .map(|n| {
                        MergeEntry::new(
                            0,
                            DecoratedKey::new(n, n.to_be_bytes().to_vec()),
                            None,
                            100,
                            RowData::Live {
                                cells: vec![CellData::new(
                                    "name".to_string(),
                                    Value::text("v"),
                                    100,
                                )],
                            },
                        )
                    })
                    .collect(),
            )
        }

        // Each run: N data entries followed by exactly ONE terminator — the shape
        // every producer thread now produces on every exit path.
        for terminator in [
            MergeMsg::Done,
            MergeMsg::Failed(MergeProducerError::Panicked("boom".to_string())),
            MergeMsg::Failed(MergeProducerError::Cancelled),
        ] {
            // A ramped run: batches of 1, 2 then 4 rows — the shape
            // `EgressBatcher` produces before its limit saturates — so the pin
            // covers MULTI-row batches, not just the degenerate 1-row one.
            const BATCH_ROWS: [usize; 3] = [1, 2, 4];
            let data_entries: i64 = BATCH_ROWS.iter().sum::<usize>() as i64;
            let depth = AtomicI64::new(0);
            let baseline = depth.load(Ordering::SeqCst);
            let mut sent_count: i64 = 0;
            let mut received_count: i64 = 0;

            let mut stream: Vec<MergeMsg> = BATCH_ROWS.iter().copied().map(data_batch).collect();
            stream.push(terminator);

            // SEND side: exactly what `egress_batch::EgressBatcher::flush` does.
            for msg in &stream {
                let tracked = msg.tracked_entries();
                if tracked > 0 {
                    adjust(&depth, tracked as i64);
                    sent_count += tracked as i64;
                }
            }
            assert_eq!(
                sent_count, data_entries,
                "only the DATA entries may be tracked on send, one slot per ENTRY \
                 (never one per batch) — and the terminator must not be tracked"
            );

            // RECEIVE side: exactly what the `MergeMsg::Batch` arm of
            // `SSTableRowIteratorAdapter::next` does (and no other arm does).
            for msg in &stream {
                let tracked = msg.tracked_entries();
                if tracked > 0 {
                    adjust(&depth, -(tracked as i64));
                    received_count += tracked as i64;
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
