//! BATCHED producer→consumer fan-in for the k-way merge egress channel (issue
//! #2820).
//!
//! # The defect this closes
//!
//! Every merge run is fed by a detached producer THREAD over a bounded
//! `sync_channel` (`producer_iter` for the path-based shape, `from_readers` for
//! the shared-reader shape). `from_readers::forward_row` used to `send` ONE
//! message per ROW, so a merged row cost one cross-thread park/wake pair.
//! Phase-0 profiling of the single-stream Flight scan measured that hand-off at
//! **49.9% of single-stream CPU, ~94% of it kernel park/wake** — the largest
//! single item in the profile, and pure overhead: nothing about the merge needs
//! per-row synchronisation.
//!
//! The fix is the one issue #1592/#1143 already applied to the windowed scan's
//! blocking→async boundary (`sstable::reader::scan_stream_windowed`): accumulate
//! rows into a `Vec` and send ONE message per batch, amortising the wake
//! ~[`BATCH_EMIT_ROWS_MERGE`]× while keeping every ordering, backpressure and
//! terminator guarantee. Order is preserved (rows are pushed and drained FIFO,
//! batches sent in order); the consumer holds ONE received batch and hands out
//! one entry per `next()`.
//!
//! # Capacity is a ROW budget; the channel counts MESSAGES (the #2765 seam)
//!
//! `egress_budget`'s whole vocabulary is in ROWS — `EGRESS_ROW_BUDGET`,
//! `MIN_CAP`, `MAX_CAP` (= `STREAMING_CHANNEL_CAPACITY`) — and stays that way:
//! it is what bounds the aggregate buffered working set across concurrent
//! merges. So the row budget is converted to a MESSAGE capacity at the
//! `sync_channel` call site by [`message_capacity_for_rows`]. Getting that
//! conversion wrong is the one catastrophic failure mode of this change: an
//! UNCONVERTED capacity would budget 256 *batches* — `256 × 256 = 65_536`
//! entries per source, times `K` sources, times every active merge — a 256×
//! blow-up of resident rows straight through the memory target.
//!
//! [`max_inflight_rows`] is the resulting explicit rows-resident bound, the
//! sibling of `scan_stream_windowed::MAX_INFLIGHT_BATCH_ROWS` and derived the
//! same way (channel-resident + consumer-held + producer-blocked-in-send).
//!
//! # First-row latency: the ramp, and the two alternatives rejected
//!
//! PURE batching regresses first-row latency — a sparse or sub-batch result set
//! would emit nothing until the walk ended. That is #1143's own recorded lesson
//! (`scan_stream_windowed`'s per-chunk flush comment), so it is designed for
//! here rather than rediscovered:
//!
//! * The windowed scan flushes its pending tail at every CHUNK boundary. That
//!   is NOT available here: this module's emit funnel is a per-ROW callback with
//!   no chunk/window event to hang a flush on.
//! * Flushing per PARTITION boundary is available (the callback carries the
//!   partition key) and was REJECTED: the dominant fixture/table shape in this
//!   repo is one row per partition (`test_basic/simple_table` = 999 partitions ×
//!   1 row), where a per-partition flush degenerates to one message per row —
//!   i.e. it would silently disable the entire optimisation for exactly the
//!   workload that motivated it.
//!
//! So the batch limit RAMPS: the first batch of a run carries
//! [`FIRST_BATCH_EMIT_ROWS`] = 1 row (the first row is therefore NEVER delayed
//! by batching), and each subsequent flush doubles the limit up to
//! [`BATCH_EMIT_ROWS_MERGE`]. A consumer that wants the first `K` rows waits for
//! `O(K)` rows to be produced, never for a full batch, while a full scan pays
//! only `log2(BATCH_EMIT_ROWS_MERGE)` = 8 extra messages per run before the
//! limit saturates. The pending tail is ALSO flushed before the run's terminator
//! (`from_readers`), so a sub-batch result set never waits for a full batch.

use std::ops::ControlFlow;
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::SyncSender;

use super::producer_msg::MergeMsg;
use super::{channel_depth, MergeEntry};

/// Rows accumulated into ONE egress channel message once the ramp has saturated
/// (issue #2820). Mirrors `scan_stream_windowed::BATCH_EMIT_ROWS`, and for the
/// same reason: a modest value keeps the per-batch heap small (a batch holds at
/// most this many already-owned `MergeEntry` values — the same values the
/// pre-change code handed over one at a time) while amortising the cross-thread
/// wake ~256×.
pub(super) const BATCH_EMIT_ROWS_MERGE: usize = 256;

/// Rows in the FIRST batch of a run; the limit doubles per flush up to
/// [`BATCH_EMIT_ROWS_MERGE`]. `1` so batching NEVER delays the first row of a
/// run — see the module doc's first-row-latency section.
pub(super) const FIRST_BATCH_EMIT_ROWS: usize = 1;

/// Minimum MESSAGE capacity of an egress `sync_channel`, whatever the row budget
/// converts to. `2` (matching `scan_stream_windowed::BATCH_CHANNEL_CAP`): a
/// capacity-1 bounded channel serialises producer and consumer — the producer
/// can hold no batch while the consumer drains one — which is the
/// producer/consumer overlap the whole streaming architecture exists for.
pub(super) const MIN_MSG_CAP: usize = 2;

/// Convert `egress_budget`'s per-channel ROW capacity into the MESSAGE capacity
/// the bounded `sync_channel` is actually constructed with (issue #2820).
///
/// THE conversion of this change — see the module doc: the channel now carries
/// batches, so passing a row budget straight to `sync_channel` would multiply
/// resident rows by [`BATCH_EMIT_ROWS_MERGE`].
pub(super) fn message_capacity_for_rows(rows_cap: usize) -> usize {
    rows_cap.div_ceil(BATCH_EMIT_ROWS_MERGE).max(MIN_MSG_CAP)
}

/// The next batch limit after a flush: double, capped at
/// [`BATCH_EMIT_ROWS_MERGE`] (the ramp — see the module doc).
fn next_batch_limit(current: usize) -> usize {
    current.saturating_mul(2).min(BATCH_EMIT_ROWS_MERGE)
}

/// Documented WORST-CASE rows the batching subsystem of ONE run may hold in
/// flight ahead of a stalled consumer, for a channel of `msg_cap` messages
/// (issue #2820) — the sibling of `scan_stream_windowed::MAX_INFLIGHT_BATCH_ROWS`
/// and derived exactly the same way. Three populations coexist at one instant,
/// each up to [`BATCH_EMIT_ROWS_MERGE`] rows:
///
/// 1. **Channel-resident** — the bounded channel is FULL: `msg_cap` messages.
/// 2. **Consumer-held** — the adapter `recv`'d one batch and is handing it out
///    one entry per `next()`; it has LEFT the channel (+1).
/// 3. **Producer-blocked-in-send** — the producer assembled its next full batch
///    and is PARKED in `SyncSender::send`, still OWNING it because `send` moves
///    the value only on success (+1).
///
/// The pending accumulator is NOT a fourth: once it reaches the limit it BECOMES
/// (3) via that same `send`, and a parked producer accumulates nothing further.
///
/// At the default row capacity (256 → `msg_cap` 2) this is `4 × 256 = 1024`
/// rows per source — 4× the pre-change 256 — a low-single-digit-MB term against
/// the ~15–17 MB Arrow egress buffer that dominates peak RSS per stream, which
/// is the sizing argument of record (`docs/architecture/throughput-program-2026-07.md`
/// §7 M3).
pub(super) fn max_inflight_rows(msg_cap: usize) -> usize {
    msg_cap
        .saturating_add(2)
        .saturating_mul(BATCH_EMIT_ROWS_MERGE)
}

/// Rows a run's producer can place into a FULL egress channel of `msg_cap`
/// messages FROM A COLD START (issue #2820).
///
/// Distinct from [`max_inflight_rows`], and both are needed: the ramp means the
/// first `msg_cap` batches of a run are NOT full ones, so a producer whose
/// consumer never steps (the backpressure fixtures of issues #2316/#2419/#2361)
/// parks in `send` holding only this many rows — far below the saturated worst
/// case. A test that derives its "the producer is genuinely backed up" threshold
/// from the worst-case bound instead would wait for rows that, by construction,
/// can never be sent.
pub(super) fn rows_in_full_channel(msg_cap: usize) -> usize {
    let mut limit = FIRST_BATCH_EMIT_ROWS;
    let mut rows = 0usize;
    for _ in 0..msg_cap {
        rows = rows.saturating_add(limit);
        limit = next_batch_limit(limit);
    }
    rows
}

/// DATA messages (batches) successfully sent into any merge egress channel since
/// process start.
static MESSAGES_SENT: AtomicU64 = AtomicU64::new(0);
/// DATA entries carried by those messages (issue #2820): `MESSAGES_SENT` counts
/// cross-thread wakes, this counts rows, and their ratio IS the fan-in
/// amortisation the change buys.
static ENTRIES_SENT: AtomicU64 = AtomicU64::new(0);
/// Peak rows in any single sent batch since process start — the observable half
/// of the per-batch cap invariant (`<= BATCH_EMIT_ROWS_MERGE`).
static PEAK_BATCH_ROWS: AtomicUsize = AtomicUsize::new(0);

/// Account one successfully sent batch of `entries` rows.
fn record_batch_sent(entries: usize) {
    MESSAGES_SENT.fetch_add(1, Ordering::Relaxed);
    ENTRIES_SENT.fetch_add(entries as u64, Ordering::Relaxed);
    PEAK_BATCH_ROWS.fetch_max(entries, Ordering::Relaxed);
}

/// Doc-hidden integration-test probe (issue #2820): a snapshot of the
/// process-global egress fan-in counters plus the batching constants they must
/// be read against. Re-exported from `merge`.
///
/// Process-global, exactly like `work_counters` and the `#2765` active-merge
/// hook: a DELTA assertion around one merge is valid only in a test binary with
/// no concurrent merge (the counters are monotonic, so a concurrent merge can
/// only ADD messages and entries — never make a batched run look per-row).
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EgressBatchProbe {
    /// DATA messages (batches) sent into merge egress channels. The pre-#2820
    /// code sent one per ROW, so `entries_sent` is exactly what this counter
    /// would have read then — which is what makes the reduction measurable
    /// without keeping a per-row code path alive to compare against.
    pub messages_sent: u64,
    /// DATA entries carried by those messages.
    pub entries_sent: u64,
    /// Peak rows observed in a single batch (must never exceed
    /// [`Self::batch_emit_rows`]).
    pub peak_batch_rows: usize,
    /// [`BATCH_EMIT_ROWS_MERGE`] — the saturated per-batch row cap.
    pub batch_emit_rows: usize,
    /// [`FIRST_BATCH_EMIT_ROWS`] — the ramp's first-batch row limit.
    pub first_batch_emit_rows: usize,
}

/// Doc-hidden integration-test hook (issue #2820): snapshot the egress fan-in
/// counters. Re-exported from `merge`.
#[doc(hidden)]
pub fn merge_egress_batch_probe() -> EgressBatchProbe {
    EgressBatchProbe {
        messages_sent: MESSAGES_SENT.load(Ordering::Relaxed),
        entries_sent: ENTRIES_SENT.load(Ordering::Relaxed),
        peak_batch_rows: PEAK_BATCH_ROWS.load(Ordering::Relaxed),
        batch_emit_rows: BATCH_EMIT_ROWS_MERGE,
        first_batch_emit_rows: FIRST_BATCH_EMIT_ROWS,
    }
}

impl EgressBatchProbe {
    /// The MESSAGE capacity a channel built from the ROW capacity `rows_cap` gets
    /// ([`message_capacity_for_rows`]).
    pub fn message_capacity_for_rows(&self, rows_cap: usize) -> usize {
        message_capacity_for_rows(rows_cap)
    }

    /// WORST-CASE resident rows of one run's batching subsystem for a channel
    /// built from the ROW capacity `rows_cap` ([`max_inflight_rows`]). Lets a
    /// memory/fixture-sizing test derive the bound from the shipped constants
    /// instead of a literal that re-rots the moment either constant moves.
    pub fn max_inflight_rows(&self, rows_cap: usize) -> usize {
        max_inflight_rows(message_capacity_for_rows(rows_cap))
    }

    /// Rows a producer can place into a FULL channel built from `rows_cap`, FROM
    /// A COLD START ([`rows_in_full_channel`]) — the threshold the #2316/#2419
    /// backed-up-merge fixtures must derive their "the producer is genuinely
    /// blocked" premise from.
    pub fn rows_in_full_channel(&self, rows_cap: usize) -> usize {
        rows_in_full_channel(message_capacity_for_rows(rows_cap))
    }

    /// The exact number of MESSAGES a run of `entries` rows sends: the ramp
    /// (`first_batch_emit_rows`, doubling to `batch_emit_rows`) followed by the
    /// pre-terminator tail flush. Deterministic, so a send-count oracle can
    /// assert an EXACT expected count rather than a hand-waved "fewer".
    pub fn expected_messages(&self, entries: u64) -> u64 {
        let mut left = entries;
        // `max(1)` on BOTH the seed and the step: a future `FIRST_BATCH_EMIT_ROWS`
        // (or `BATCH_EMIT_ROWS_MERGE`) of 0 would otherwise make `take` 0 and spin
        // this loop forever — a hang in a probe method used by tests.
        let mut limit = (self.first_batch_emit_rows as u64).max(1);
        let cap = (self.batch_emit_rows as u64).max(1);
        let mut messages = 0u64;
        while left > 0 {
            let take = limit.min(left);
            left -= take;
            messages += 1;
            limit = limit.saturating_mul(2).min(cap);
        }
        messages
    }
}

/// Producer-side row accumulator for ONE run's egress channel (issue #2820).
///
/// Owns the pending `Vec<MergeEntry>` and the ramping batch limit, and is the
/// ONLY place a `MergeMsg::Batch` is built — so the egress-depth accounting
/// (`channel_depth` + the adapter's own `local_sent`, both in ENTRIES, issue
/// #2419) happens at exactly one site, per successful send, for the whole batch.
///
/// Created per driven stream by `from_readers`, which MUST flush the pending
/// tail before its producer thread sends a terminator: a row still in this
/// accumulator when `Done` goes out is a LOST row (issue #3120's silent-short-read
/// class, reintroduced through the back door).
pub(super) struct EgressBatcher<'a> {
    sender: &'a SyncSender<MergeMsg>,
    /// This adapter's own sent-count in ENTRIES (issue #2419 roborev job 1733),
    /// read post-join by `SSTableRowIteratorAdapter::drop` for the exact
    /// reconcile residual — so it must be incremented by the BATCH LENGTH, never
    /// by one per message.
    local_sent: &'a AtomicI64,
    pending: Vec<MergeEntry>,
    limit: usize,
}

impl<'a> EgressBatcher<'a> {
    pub(super) fn new(sender: &'a SyncSender<MergeMsg>, local_sent: &'a AtomicI64) -> Self {
        Self {
            sender,
            local_sent,
            pending: Vec::with_capacity(FIRST_BATCH_EMIT_ROWS),
            limit: FIRST_BATCH_EMIT_ROWS,
        }
    }

    /// Accumulate one converted row, flushing when the current ramp limit is
    /// reached. `Break` means the consumer has dropped the channel.
    pub(super) fn push(&mut self, entry: MergeEntry) -> ControlFlow<()> {
        self.pending.push(entry);
        if self.pending.len() >= self.limit {
            self.flush()
        } else {
            ControlFlow::Continue(())
        }
    }

    /// Send the pending rows as ONE message (a no-op when empty). Called for the
    /// full-batch flush AND as the pre-terminator tail flush.
    ///
    /// The BLOCKING `send`, exactly as the pre-batching per-row code used: it is
    /// what carries the backpressure that keeps peak memory independent of total
    /// input size, now in units of batches (see [`max_inflight_rows`]).
    pub(super) fn flush(&mut self) -> ControlFlow<()> {
        if self.pending.is_empty() {
            return ControlFlow::Continue(());
        }
        let next_limit = next_batch_limit(self.limit);
        let batch = std::mem::replace(&mut self.pending, Vec::with_capacity(next_limit));
        let msg = MergeMsg::Batch(batch);
        // Issue #2419 (WS2) / #3120: only DATA entries are tracked on the
        // egress-depth gauge, and a TERMINATOR is untracked on both send and
        // receive so it can never unbalance the level. ONE predicate, shared with
        // the consumer's recv site — see `MergeMsg::tracked_entries`, whose
        // exhaustive match is the compile-time tripwire for a future variant.
        // Captured BEFORE `send` moves `msg`.
        let tracked = msg.tracked_entries();
        match self.sender.send(msg) {
            Ok(()) => {
                if tracked > 0 {
                    // These entries now occupy a channel slot; balanced by exactly
                    // one `channel_depth::received_n` of the same count at the
                    // consumer's recv site (or by the post-join reconcile in
                    // `Drop`) — see `channel_depth`.
                    channel_depth::sent_n(tracked);
                    self.local_sent.fetch_add(tracked as i64, Ordering::SeqCst);
                    record_batch_sent(tracked);
                }
                self.limit = next_limit;
                ControlFlow::Continue(())
            }
            // The consumer is gone; the batch is dropped with it. Same outcome as
            // the pre-batching per-row send failure: the walk stops via `Break`.
            Err(_) => ControlFlow::Break(()),
        }
    }
}

// Issue #2820 unit pins (constants, the rows→messages conversion, the resident
// bound, the ramp) in a `*_tests.rs` sibling so this file stays under the
// ~800-line campsite target (epic #1116 / #1135). Also the home of the channel
// capacity pin moved out of the 12.5k-line `merge/mod.rs`.
#[cfg(test)]
#[path = "egress_batch_tests.rs"]
mod tests;
