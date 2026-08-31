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
//! ## The batch size is bounded BY the row capacity, or #2765 becomes inert
//!
//! The conversion alone is not enough, and the first revision of this change got
//! it wrong. Over the whole REACHABLE range of `egress_budget` row capacities
//! (`[MIN_CAP, MAX_CAP]` = `[8, 256]`, `MAX_CAP` being
//! `STREAMING_CHANNEL_CAPACITY` itself) `rows_cap.div_ceil(256)` is `1`, so
//! [`message_capacity_for_rows`] is floored at [`MIN_MSG_CAP`] = 2 for EVERY
//! setting the adaptive budget can produce. If the per-batch limit were then a
//! flat [`BATCH_EMIT_ROWS_MERGE`], resident rows would be a CONSTANT `4 × 256 =
//! 1024` per source at every setting — i.e. at the documented throttled floor
//! (`rows_cap = 8`) a source channel would hold 1024 rows where the pre-change
//! code held 8, a **128×** increase, and `egress_channel_capacity_for` would have
//! ZERO effect on resident memory. That defeats the #2765/#2600/#2367 aggregate
//! bound this module is co-designed with.
//!
//! So the ramp's ceiling is [`batch_limit_ceiling`] = `min(rows_cap,
//! BATCH_EMIT_ROWS_MERGE)`, and every derived bound is a MULTIPLE of the row
//! capacity rather than a constant. Three DISTINCT quantities follow, and they
//! are routinely confused — each has its own function here, and callers must pick
//! the one that answers their question:
//!
//! | quantity | function | value |
//! |---|---|---|
//! | channel-resident (what the #2419 depth gauge can reach) | [`rows_resident_in_channel`] | `msg_cap × ceiling` = `2 × rows_cap` |
//! | total in-flight (the MEMORY bound) | [`max_inflight_rows`] | `(msg_cap + 2) × ceiling` = `4 × rows_cap` |
//! | cold-start fill (what a fixture must exceed to PARK a producer) | [`rows_in_full_channel`] `+ ceiling + 1` | ramp sum, far below either |
//!
//! At the default `rows_cap = 256` that is 512 / 1024; at the throttled floor
//! `rows_cap = 8` it is 16 / 32. The throttle therefore keeps a 32× dynamic
//! range instead of being a constant, and the 4× multiplier over the pre-change
//! flat capacity is uniform at EVERY setting.
//!
//! [`max_inflight_rows`] is the resulting explicit TOTAL IN-FLIGHT bound (never
//! called "resident": that word is reserved for the strictly smaller
//! channel-resident figure the gauge can reach), the
//! sibling of `scan_stream_windowed::MAX_INFLIGHT_BATCH_ROWS` and derived the
//! same way (channel-resident + consumer-held + producer-blocked-in-send).
//!
//! # A row bound is not a memory bound: the BYTE budget
//!
//! A row count says nothing about bytes — 1024 rows is a few hundred KiB of
//! Flight scan rows and ~49 MiB of 48 KiB blob rows. The `#827` merge memory
//! fixture is exactly that second shape (4 sources × 800 rows × 48 KiB), and a
//! purely row-bounded 4× would put `4 × 4 × 256 × 48 KiB ≈ 196 MiB` of
//! channel-resident payload against a 128 MiB budget. So the accumulator ALSO
//! carries a byte budget ([`BATCH_EMIT_BYTES_MERGE`]) and flushes on whichever
//! bound trips FIRST. Sizes come from `RunReader::estimate_entry_size`, the same
//! estimator the merge's own read-ahead buffer is bounded by — reused rather than
//! re-derived so the two cannot drift, and adequate for this purpose for the same
//! reason it is adequate there: it counts the heap actually owned by the
//! `MergeEntry` (key bytes, per-cell column names, per-value payload) with a
//! per-variant approximation for values whose size is not carried inline. It is
//! an ESTIMATE, not an exact `size_of` closure over the graph, so the byte bound
//! is approximate in the same direction the read-ahead bound already is; what it
//! buys is that the bound tracks row SIZE at all, which no row count can.
//!
//! The threshold is checked AFTER the push, so one batch may reach
//! `BATCH_EMIT_BYTES_MERGE + one entry` — a single row larger than the whole
//! budget is still sent (never split, never dropped). The honest per-source
//! byte bound is therefore `(msg_cap + 2) × (BATCH_EMIT_BYTES_MERGE +
//! max_row_bytes)`.
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

/// Estimated BYTES accumulated into ONE egress channel message before it is
/// flushed regardless of row count (issue #2820) — the row-size-independent half
/// of the bound, see the module doc.
///
/// `1 MiB`. Chosen so the byte bound is INERT for ordinary merge rows and
/// governs only for large ones: at a few hundred bytes per row a full
/// [`BATCH_EMIT_ROWS_MERGE`] batch is ~128 KiB, well under this, so the default
/// row-bounded amortisation is unchanged; it engages above ~4 KiB/row average,
/// where the row bound stops being a memory bound. The resulting per-source
/// worst case is `(msg_cap + 2) × (this + max_row_bytes)` ≈ 4 MiB at the default
/// message capacity — a bounded, row-size-independent term, versus the ~49 MiB
/// per source a flat 1024-row bound implies for 48 KiB rows.
pub(super) const BATCH_EMIT_BYTES_MERGE: usize = 1024 * 1024;

/// Convert `egress_budget`'s per-channel ROW capacity into the MESSAGE capacity
/// the bounded `sync_channel` is actually constructed with (issue #2820).
///
/// THE conversion of this change — see the module doc: the channel now carries
/// batches, so passing a row budget straight to `sync_channel` would multiply
/// resident rows by [`BATCH_EMIT_ROWS_MERGE`].
pub(super) fn message_capacity_for_rows(rows_cap: usize) -> usize {
    rows_cap.div_ceil(BATCH_EMIT_ROWS_MERGE).max(MIN_MSG_CAP)
}

/// The per-batch ROW ceiling for a run whose egress channel was budgeted
/// `rows_cap` ROWS (issue #2820): `min(rows_cap, BATCH_EMIT_ROWS_MERGE)`.
///
/// THE line that keeps #2765's adaptive throttle effective — see the module doc's
/// "bounded BY the row capacity" section. A flat [`BATCH_EMIT_ROWS_MERGE`]
/// ceiling would make every derived resident bound a CONSTANT across the whole
/// reachable capacity range.
///
/// Floored at `1` so a degenerate (0) row budget can never produce a 0-row batch
/// limit, which would make the accumulator flush an empty batch per push.
pub(super) fn batch_limit_ceiling(rows_cap: usize) -> usize {
    rows_cap.clamp(1, BATCH_EMIT_ROWS_MERGE)
}

/// The next batch limit after a flush: double, capped at this run's
/// `ceiling` ([`batch_limit_ceiling`]) — the ramp, see the module doc.
fn next_batch_limit(current: usize, ceiling: usize) -> usize {
    current.saturating_mul(2).min(ceiling)
}

/// Rows that can be resident IN the bounded channel itself for a run budgeted
/// `rows_cap` ROWS (issue #2820): `msg_cap × ceiling`, i.e. `2 × rows_cap` over
/// the reachable capacity range.
///
/// This — NOT [`max_inflight_rows`] — is the ceiling of the #2419
/// `cqlite.merge.egress_channel_depth` gauge: that gauge is incremented on a
/// successful `send` and decremented on the consumer's `recv`, so it counts
/// neither the consumer-HELD batch (already decremented) nor the batch a
/// producer is PARKED holding (never sent). Documenting the gauge's ceiling as
/// the in-flight bound overstates it by exactly those two batches (2× at the
/// shipped default), which is an operator-facing alert-threshold error.
pub(super) fn rows_resident_in_channel(rows_cap: usize) -> usize {
    message_capacity_for_rows(rows_cap).saturating_mul(batch_limit_ceiling(rows_cap))
}

/// Documented WORST-CASE rows the batching subsystem of ONE run may hold in
/// flight ahead of a stalled consumer, for a channel budgeted `rows_cap` ROWS
/// (issue #2820) — the sibling of `scan_stream_windowed::MAX_INFLIGHT_BATCH_ROWS`
/// and derived exactly the same way. Three populations coexist at one instant,
/// each up to [`batch_limit_ceiling`] rows:
///
/// 1. **Channel-resident** — the bounded channel is FULL: `msg_cap` messages
///    ([`rows_resident_in_channel`]).
/// 2. **Consumer-held** — the adapter `recv`'d one batch and is handing it out
///    one entry per `next()`; it has LEFT the channel (+1).
/// 3. **Producer-blocked-in-send** — the producer assembled its next full batch
///    and is PARKED in `SyncSender::send`, still OWNING it because `send` moves
///    the value only on success (+1).
///
/// The pending accumulator is NOT a fourth: once it reaches the limit it BECOMES
/// (3) via that same `send`, and a parked producer accumulates nothing further.
///
/// Because the ceiling is bounded by `rows_cap`, this is `4 × rows_cap` at EVERY
/// reachable setting — 1024 rows at the default 256, 32 at the throttled floor of
/// 8 — a uniform 4× over the pre-batching flat capacity rather than a constant
/// that would ignore the #2765 throttle entirely.
///
/// **This is a ROW bound and therefore NOT a memory bound on its own.** For a
/// 48 KiB-row workload (the `#827` fixture shape) `4 × 256` rows would be ~49 MiB
/// per source. The memory bound is the BYTE budget the accumulator enforces
/// alongside it ([`BATCH_EMIT_BYTES_MERGE`]): per source, whichever of
/// `(msg_cap + 2) × ceiling` rows and `(msg_cap + 2) × (BATCH_EMIT_BYTES_MERGE +
/// max_row_bytes)` bytes binds first — ≈4 MiB at the default message capacity.
pub(super) fn max_inflight_rows(rows_cap: usize) -> usize {
    message_capacity_for_rows(rows_cap)
        .saturating_add(2)
        .saturating_mul(batch_limit_ceiling(rows_cap))
}

/// Worst-case estimated BYTES one run's batching subsystem may hold in flight
/// (issue #2820) — the byte sibling of [`max_inflight_rows`], over the same three
/// populations, each holding at most `BATCH_EMIT_BYTES_MERGE + max_row_bytes`
/// (the threshold is checked after the push, so one oversized row can carry a
/// batch past the budget).
///
/// `max_row_bytes` is the caller's own knowledge of its workload; there is no
/// process-wide row-size bound to substitute for it.
pub(super) fn max_inflight_bytes(rows_cap: usize, max_row_bytes: usize) -> usize {
    message_capacity_for_rows(rows_cap)
        .saturating_add(2)
        .saturating_mul(BATCH_EMIT_BYTES_MERGE.saturating_add(max_row_bytes))
}

/// Rows a run's producer can place INTO a FULL egress channel budgeted
/// `rows_cap` ROWS, FROM A COLD START (issue #2820) — the ramp sum over the
/// channel's `msg_cap` slots.
///
/// Distinct from [`max_inflight_rows`], and both are needed: the ramp means the
/// first `msg_cap` batches of a run are NOT full ones, so a producer whose
/// consumer never steps (the backpressure fixtures of issues #2316/#2419/#2361)
/// parks in `send` holding only this many rows — far below the saturated worst
/// case. A test that derives its "the producer is genuinely backed up" threshold
/// from the worst-case bound instead would wait for rows that, by construction,
/// can never be sent.
///
/// **This is the CHANNEL's content only — a parked producer also OWNS the batch
/// it is trying to hand over.** So a fixture that must observe the producer
/// actually blocked needs
/// `rows_in_full_channel(rows_cap) + batch_limit_ceiling(rows_cap) + 1` rows
/// available, which is what every caller writes (`+ batch_emit_rows + 1` via the
/// probe). The in-flight batch term is NOT folded in here because the two are
/// separately meaningful: this is what the #2419 depth gauge can observe from a
/// cold start, the extra batch is what the producer holds off-gauge.
pub(super) fn rows_in_full_channel(rows_cap: usize) -> usize {
    let ceiling = batch_limit_ceiling(rows_cap);
    let mut limit = FIRST_BATCH_EMIT_ROWS.min(ceiling);
    let mut rows = 0usize;
    for _ in 0..message_capacity_for_rows(rows_cap) {
        rows = rows.saturating_add(limit);
        limit = next_batch_limit(limit, ceiling);
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
///
/// Called BEFORE the `SeqCst` egress-depth/`local_sent` stores at the send site,
/// deliberately: an integration test establishes "the producer has sent N rows"
/// by observing the SeqCst gauge and then asserts these counters by EXACT
/// equality (`tests/issue_2419_egress_depth_gauge.rs`). Storing them after the
/// gauge left no happens-before at all — the producer then parks in `send` with
/// no further synchronisation, so a reader could see the gauge move and the
/// counters still stale. Ordering these first makes the gauge store the release
/// that publishes them.
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
    /// [`BATCH_EMIT_BYTES_MERGE`] — the per-batch estimated-BYTE budget, the
    /// row-size-independent half of the bound (issue #2820).
    pub batch_emit_bytes: usize,
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
        batch_emit_bytes: BATCH_EMIT_BYTES_MERGE,
    }
}

impl EgressBatchProbe {
    /// The MESSAGE capacity a channel built from the ROW capacity `rows_cap` gets
    /// ([`message_capacity_for_rows`]).
    pub fn message_capacity_for_rows(&self, rows_cap: usize) -> usize {
        message_capacity_for_rows(rows_cap)
    }

    /// The per-batch ROW ceiling a run budgeted `rows_cap` ROWS uses
    /// ([`batch_limit_ceiling`]) — bounded BY `rows_cap`, so a throttled budget
    /// shrinks the batches too. Use this, not [`Self::batch_emit_rows`], wherever
    /// a bound must hold at a NON-default capacity.
    pub fn batch_limit_ceiling(&self, rows_cap: usize) -> usize {
        batch_limit_ceiling(rows_cap)
    }

    /// Rows that can be resident IN the bounded channel for a run budgeted
    /// `rows_cap` ROWS ([`rows_resident_in_channel`]) — the ceiling of the #2419
    /// egress-depth GAUGE, which counts neither the consumer-held nor the
    /// producer-parked batch. Strictly below [`Self::max_inflight_rows`].
    pub fn rows_resident_in_channel(&self, rows_cap: usize) -> usize {
        rows_resident_in_channel(rows_cap)
    }

    /// WORST-CASE resident rows of one run's batching subsystem for a channel
    /// built from the ROW capacity `rows_cap` ([`max_inflight_rows`]). Lets a
    /// memory/fixture-sizing test derive the bound from the shipped constants
    /// instead of a literal that re-rots the moment either constant moves.
    pub fn max_inflight_rows(&self, rows_cap: usize) -> usize {
        max_inflight_rows(rows_cap)
    }

    /// WORST-CASE in-flight estimated BYTES for a run budgeted `rows_cap` ROWS
    /// whose largest row estimates at `max_row_bytes`
    /// ([`max_inflight_bytes`]) — the row-size-independent bound a memory
    /// fixture must size against, since a row count alone bounds no memory.
    pub fn max_inflight_bytes(&self, rows_cap: usize, max_row_bytes: usize) -> usize {
        max_inflight_bytes(rows_cap, max_row_bytes)
    }

    /// Rows a producer can place into a FULL channel built from `rows_cap`, FROM
    /// A COLD START ([`rows_in_full_channel`]) — the threshold the #2316/#2419
    /// backed-up-merge fixtures must derive their "the producer is genuinely
    /// blocked" premise from. CHANNEL content only; add
    /// [`Self::batch_limit_ceiling`] `+ 1` for the batch a parked producer owns
    /// (see [`Self::rows_that_park_the_producer`]).
    pub fn rows_in_full_channel(&self, rows_cap: usize) -> usize {
        rows_in_full_channel(rows_cap)
    }

    /// Rows a fixture must make available before its producer is GUARANTEED to be
    /// parked in `send` on a channel budgeted `rows_cap` ROWS: the cold-start
    /// channel fill, PLUS the full batch the parked producer still owns, plus one
    /// more row it could not even accumulate.
    ///
    /// The one place this sum lives (issue #2820 review round 2): six #2316/#2419/
    /// #2370/#2361 fixtures each wrote it out by hand, so the premise the probe
    /// exists to make un-rottable was re-copied six times one level up.
    pub fn rows_that_park_the_producer(&self, rows_cap: usize) -> usize {
        rows_in_full_channel(rows_cap)
            .saturating_add(batch_limit_ceiling(rows_cap))
            .saturating_add(1)
    }

    /// The exact number of MESSAGES a run of `entries` rows sends at the ROW
    /// capacity `rows_cap`: the ramp (`first_batch_emit_rows`, doubling to
    /// [`Self::batch_limit_ceiling`]) followed by the pre-terminator tail flush.
    /// Deterministic, so a send-count oracle can assert an EXACT expected count
    /// rather than a hand-waved "fewer".
    ///
    /// Models the ROW bound only. A workload whose rows trip the BYTE budget
    /// ([`Self::batch_emit_bytes`]) sends MORE messages than this, so it is a
    /// lower bound there — never assert exact equality against it for large rows.
    pub fn expected_messages_at(&self, rows_cap: usize, entries: u64) -> u64 {
        let mut left = entries;
        // `max(1)` on BOTH the seed and the step: a future `FIRST_BATCH_EMIT_ROWS`
        // (or a zero ceiling) would otherwise make `take` 0 and spin this loop
        // forever — a hang in a probe method used by tests.
        let mut limit = (self.first_batch_emit_rows as u64).max(1);
        let cap = (batch_limit_ceiling(rows_cap) as u64).max(1);
        let mut messages = 0u64;
        while left > 0 {
            let take = limit.min(left);
            left -= take;
            messages += 1;
            limit = limit.saturating_mul(2).min(cap);
        }
        messages
    }

    /// [`Self::expected_messages_at`] at the SATURATED ceiling
    /// (`batch_emit_rows`), i.e. what a run at the DEFAULT/unthrottled row budget
    /// sends. A run whose adaptive budget was squeezed sends more.
    pub fn expected_messages(&self, entries: u64) -> u64 {
        self.expected_messages_at(self.batch_emit_rows, entries)
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
    /// Estimated heap bytes of `pending` (`RunReader::estimate_entry_size`),
    /// accumulated per push and reset with it — the byte half of the flush
    /// predicate, see the module doc.
    pending_bytes: usize,
    limit: usize,
    /// This run's per-batch ROW ceiling, [`batch_limit_ceiling`] of the ROW
    /// capacity its channel was budgeted. NOT a global constant: bounding the
    /// batch by the run's own capacity is what keeps #2765's adaptive throttle
    /// effective (module doc).
    ceiling: usize,
}

impl<'a> EgressBatcher<'a> {
    /// `rows_cap` is the merge-scoped adaptive ROW capacity
    /// (`egress_budget`'s snapshot) this run's channel was built from — the SAME
    /// value [`message_capacity_for_rows`] converted for the `sync_channel`. It
    /// bounds the batch size, so both halves of the resident bound scale with the
    /// throttle instead of one of them being a constant.
    pub(super) fn new(
        sender: &'a SyncSender<MergeMsg>,
        local_sent: &'a AtomicI64,
        rows_cap: usize,
    ) -> Self {
        let ceiling = batch_limit_ceiling(rows_cap);
        let limit = FIRST_BATCH_EMIT_ROWS.min(ceiling);
        Self {
            sender,
            local_sent,
            pending: Vec::with_capacity(limit),
            pending_bytes: 0,
            limit,
            ceiling,
        }
    }

    /// Accumulate one converted row, flushing when EITHER the current ramp row
    /// limit or the [`BATCH_EMIT_BYTES_MERGE`] byte budget is reached — whichever
    /// trips first. `Break` means the consumer has dropped the channel.
    ///
    /// The byte half is what makes the bound row-SIZE-independent: at a few
    /// hundred bytes per row it never trips and the row limit governs (so the
    /// default amortisation is unchanged), while a 48 KiB-row workload flushes on
    /// bytes long before 256 rows — the difference between a bounded ~4 MiB per
    /// source and ~49 MiB.
    pub(super) fn push(&mut self, entry: MergeEntry) -> ControlFlow<()> {
        // Reuses the merge's OWN read-ahead-buffer estimator rather than a second
        // size model, so the two accountings cannot drift (module doc).
        self.pending_bytes = self
            .pending_bytes
            .saturating_add(super::RunReader::estimate_entry_size(&entry));
        self.pending.push(entry);
        if self.pending.len() >= self.limit || self.pending_bytes >= BATCH_EMIT_BYTES_MERGE {
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
        let next_limit = next_batch_limit(self.limit, self.ceiling);
        let batch = std::mem::replace(&mut self.pending, Vec::with_capacity(next_limit));
        self.pending_bytes = 0;
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
                    // The probe counters are stored FIRST, the `SeqCst` gauge/
                    // `local_sent` stores second: a test that observes the gauge and
                    // then reads the probe by exact equality has no other
                    // happens-before to rely on (see `record_batch_sent`).
                    record_batch_sent(tracked);
                    // These entries now occupy a channel slot; balanced by exactly
                    // one `channel_depth::received_n` of the same count at the
                    // consumer's recv site (or by the post-join reconcile in
                    // `Drop`) — see `channel_depth`.
                    channel_depth::sent_n(tracked);
                    self.local_sent.fetch_add(tracked as i64, Ordering::SeqCst);
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
