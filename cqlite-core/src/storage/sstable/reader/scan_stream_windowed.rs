//! Sliding-window stitch+parse driver for the user-facing streaming scan
//! (issue #1143).
//!
//! # Design (issue #1143 regression fix)
//!
//! The V5CompressedLegacy full scan must reconcile two goals that PR #1156
//! treated as mutually exclusive:
//!
//!   1. **Bounded heap.** Keep only a sliding `window: Vec<u8>` of decompressed
//!      bytes (peak `max_partition_size + one_chunk`), not an O(file) stitched
//!      buffer per scan. (PR #1156's contribution — KEPT.)
//!   2. **CPU off the async worker pool.** Decompress +
//!      `parse_one_partition_with_timestamps` are CPU-bound; running them inline
//!      on the small async worker pool (as PR #1156 did, relying on
//!      `yield_now()`) lets a scan starve everything else scheduled there
//!      (writer flush/compaction in production), halving reader throughput under
//!      concurrent write load. The pre-#1156 path ran the whole parse under a
//!      dedicated `spawn_blocking` thread — restore that.
//!
//! Both are achievable together. This driver splits the work across THREE stages
//! of one bounded pipeline (issue #1143 contention fix):
//!
//!   - **Async I/O half** (`run_scan_stream_windowed`): the only thing that must
//!     touch the async runtime is the chunk read (`read_next_block().await`
//!     awaits the per-scan cursor's async file lock). It does ONLY I/O: read the
//!     next raw compressed chunk and hand it to the parse half over a small
//!     bounded channel (`raw` channel, capacity [`RAW_CHUNK_CHANNEL_CAP`] = 8 so
//!     the I/O half can read ahead instead of ping-ponging on every chunk).
//!   - **Blocking parse half** (`drain_scan_window_blocking`): a single
//!     `spawn_blocking` task owns the parser, the schema, and the sliding
//!     window. It pulls raw chunks with `blocking_recv`, decompresses, appends to
//!     the window, drains every confirmed partition, and accumulates surviving
//!     `(RowKey, ScanRow)` entries into a BATCH of up to [`BATCH_EMIT_ROWS`], which
//!     it hands across the blocking→async seam via `tx.blocking_send` as ONE
//!     `Vec` item. ALL decompress+parse CPU runs here, off the async worker pool.
//!     Batching is the key contention fix: PR #1156 `blocking_send`'d one row at
//!     a time, so a `SELECT *` full scan woke the consuming async task O(rows)
//!     times across the blocking-pool ↔ async-worker boundary; samply attributed
//!     ~31.5% of read wall time under `mixed.read_while_write` conc=8 to that one
//!     `parking_lot` condvar. Batching amortizes the wake ~`BATCH_EMIT_ROWS`×.
//!     To keep that win WITHOUT regressing incremental delivery, the pending
//!     batch is flushed at a BOUNDED CADENCE — at each chunk/window drain
//!     boundary (after the per-chunk drain returns NeedMore / empties the
//!     window) as well as on reaching `BATCH_EMIT_ROWS` — so a sparse or
//!     sub-`BATCH_EMIT_ROWS` scan (e.g. a `LIMIT`) still delivers its confirmed
//!     rows promptly instead of waiting for the whole scan to finish. The wake
//!     rate is thus ~one-per-chunk (plus one-per-full-batch in dense windows),
//!     far below PR #1156's one-per-row.
//!   - **Async forwarder task** (in `run_scan_stream_windowed`): adapts the SAME
//!     internal `Vec`-batched stream to whichever public surface the caller asked
//!     for (issue #1592, Epic F/F2), selected by [`WindowedOut`]. The
//!     [`WindowedOut::PerRow`] arm FLATTENS each `Vec` batch back into the caller's
//!     per-item `tx`, preserving the historical `scan_stream` contract (item type,
//!     order, backpressure) unchanged — so per-row is a thin flattening adapter
//!     over the batched internal stream. The [`WindowedOut::Batched`] arm FORWARDS
//!     each `Vec` batch straight through to the caller's batched `tx`, so the
//!     consumer is woken once per BATCH, not once per row (the F2 win: the internal
//!     batch is not re-flattened then re-batched onto the public channel). Runs on
//!     the async runtime concurrently with the I/O half (both are needed at once;
//!     running them sequentially would deadlock), so its wakes are cheap
//!     async-worker wakes, not blocking-pool condvar wakes.
//!
//! ## Backpressure (preserved end-to-end)
//!
//! A slow consumer blocks the forwarder's `tx.send().await`, which stops draining
//! the bounded batch channel, which blocks the parse half's `tx.blocking_send`,
//! which stops the parse loop, which stops draining the `raw` channel, which
//! (being bounded) blocks `raw_tx.send().await` in the I/O half, which stops
//! reading from disk. Nothing buffers the whole file; live heap stays
//! `window + RAW_CHUNK_CHANNEL_CAP` raw chunks + at most
//! [`MAX_INFLIGHT_BATCH_ROWS`] buffered rows ahead of the caller.
//!
//! ## Worst-case resident rows (issue #1143, roborev)
//!
//! Be honest and COMPLETE: against a stalled consumer the resident `(RowKey, ScanRow)`
//! count is the SUM of three inherent terms, not one constant —
//! `buffer_size + max_partition_size + MAX_INFLIGHT_BATCH_ROWS`:
//!
//! - **`buffer_size`** — the public per-item channel, sized from the caller's
//!   `StreamingConfig::buffer_size`.
//! - **`max_partition_size`** — INHERENT to any row-materializing partition scan and
//!   PRE-DATES this change: `drain_scan_window` parses one CONFIRMED partition fully
//!   into a reused `scratch: Vec<(RowKey, ScanRow)>` before batching (the parser's `FnMut`
//!   emit is synchronous, so a partition's rows cannot stream out mid-parse), so if
//!   `blocking_send` stalls mid-iteration the producer still owns that Vec's
//!   not-yet-batched tail. This is the pre-existing #1156 windowed-scan heap term
//!   (heap was always `~max_partition_size + one_chunk`); this PR neither introduced
//!   it nor restructures emission to observe backpressure mid-partition — that is a
//!   separate #1156 redesign, deliberately out of scope here.
//! - **`MAX_INFLIGHT_BATCH_ROWS`** — the *additional* bound this PR's batching adds:
//!   a SECOND in-flight pool NOT sized by `buffer_size`. The bounded batch channel
//!   (`BATCH_CHANNEL_CAP` batches), the one batch the forwarder `recv()`'d and is
//!   flattening, AND the one batch the producer is parked-in-`blocking_send` holding
//!   can ALL be live at once — even when `buffer_size == 1`: a deliberate, BOUNDED
//!   read-ahead, the price of amortizing the blocking->async wake. It bounds the
//!   BATCHING subsystem ALONE; it does NOT — and is not claimed to — cover the
//!   `max_partition_size` term. The public `scan_stream` doc states the full
//!   three-term sum so callers know the true worst case.
//!
//! ## Cancellation (issue #1143 finding 2)
//!
//! The outer scan runs inside the `tokio::spawn` in `scan_stream`; if that task
//! is dropped/cancelled at an `await`, the parse task's `JoinHandle` (held in
//! `run_scan_stream_windowed`) is dropped too. Dropping a `spawn_blocking`
//! `JoinHandle` DETACHES the task — it does NOT abort it (there is no portable
//! way to interrupt a running blocking thread). The detached parse task keeps
//! running: `raw_rx.blocking_recv()` observes the dropped `raw_tx`, and because a
//! clean cancellation leaves `io_failed == false`, it proceeds to a final drain
//! and `tx.blocking_send`s its trailing batch into the batch channel.
//!
//! This is harmless in practice and needs no abort machinery: on cancellation
//! the consumer drops the `mpsc::Receiver` (`rx`) returned by `scan_stream`, so
//! the forwarder's next `fwd_tx.send` fails, the forwarder returns and drops the
//! batch receiver, so the parse half's next `tx.blocking_send` fails
//! (`*broke = true`), the parse loop returns immediately, and the task — and the
//! `reader: Arc<Self>` it holds — are released. The only window in which it can
//! still emit is when a batch was already in flight AND the consumer's `rx` has
//! not yet dropped, i.e. the consumer is still reading; those rows are valid scan
//! output, not garbage. Worst case the task lingers for one partition's parse
//! before its `blocking_send` fails, keeping `Arc<Self>` alive only that long.
//! Pre-#1156 the parse ran inline on the cancellable async task, so this detach
//! window is new to the offload split; it is bounded and self-terminating, so we
//! document it rather than add abort plumbing.
//!
//! ## Parity
//!
//! The emitted set/order is byte-identical to the prior inline driver: same
//! schema resolution, same incompressible-chunk raw fallback, same
//! `table_ids_match` / key-range / `filter_tombstone` filters, same
//! NeedMore/Done straddle handling and final-chunk semantics, same
//! `READ_SCAN_WINDOW_REFILL` counter. The only change is WHERE the CPU runs.

use super::source::ScanCursor;
use super::window_cursor::WindowCursor;
use super::SSTableReader;
use crate::types::{ScanRow, TableId};
use crate::{Error, Result, RowKey};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

// Panic/early-exit drop-guard for the raw-chunk feed loops (roborev finding,
// issue #1593). In a sibling file to keep this driver under the campsite-rule
// size limit (epic #1116); re-exported privately so `use super::*` in the test
// module resolves it.
#[path = "scan_stream_windowed_guard.rs"]
mod guard;
use guard::FeedFailureGuard;

// Blocking-pool admission control for windowed scans (issue #1594, Epic F/F4).
// Always compiled (production `admit()` gates every scan's blocking offload); its
// test-only limit-override + in-flight probe surface is `pub` ONLY under the
// non-default `scan-offload-probe` feature, mirroring `probe` below. In a sibling
// file to keep this driver under the campsite-rule size limit (epic #1116).
#[cfg(not(feature = "scan-offload-probe"))]
#[path = "scan_admission.rs"]
pub(crate) mod scan_admission;
#[cfg(feature = "scan-offload-probe")]
#[path = "scan_admission.rs"]
pub mod scan_admission;

/// Bound on the raw-compressed-chunk channel feeding the blocking parse task.
///
/// This is the I/O-half → parse-half hand-off depth. It must stay small enough
/// to bound live heap (combined with the sliding window: live heap is
/// `max_partition_size + (RAW_CHUNK_CHANNEL_CAP + 1) * one_chunk`), but a value
/// of 2 (PR #1156's original) makes the two halves ping-pong: the I/O half fills
/// the channel, parks on `raw_tx.send().await`, and is unparked again after the
/// parse half drains essentially every chunk. Under concurrent write load (issue
/// #1143, `mixed.read_while_write` conc=8) samply showed ~42% of read wall time
/// parked on futexes, dominated by this channel's park/unpark, roughly doubling
/// read p99 vs the pre-windowing baseline.
///
/// Raising the depth to 8 lets the I/O half read ahead up to 8 compressed chunks
/// before it has to park, amortizing the wake round-trips ~4x while preserving
/// the bounded-heap intent of PR #1156. The chunks are RAW COMPRESSED data
/// (~`one_chunk` = the SSTable's compression chunk length, typically 16–64 KiB),
/// so the added buffer is at most `(8 - 2) * one_chunk` ≈ 384 KiB per scan at
/// 64 KiB chunks. At the issue #1143 concurrency of 8 readers that is < 3 MiB of
/// channel buffers total — negligible against the < 128 MiB budget, and far
/// smaller than the O(file) stitch buffer PR #1156 eliminated. Backpressure is
/// unchanged: a full channel still blocks the I/O half, which still blocks disk
/// reads, so nothing buffers the whole file.
const RAW_CHUNK_CHANNEL_CAP: usize = 8;

/// Number of surviving `(RowKey, ScanRow)` entries the blocking parse half buffers
/// before it hands a batch across the blocking→async seam.
///
/// PR #1156's blocking parse half `blocking_send`s ONE row at a time into the
/// async consumer's channel. Each such send parks/unparks across the
/// blocking-pool ↔ async-worker boundary (a `parking_lot` condvar wake under
/// tokio's blocking pool). For a `SELECT *` full scan that is O(rows) wakes per
/// scan — and samply attributed ~31.5% of read wall time to that one condvar
/// under `mixed.read_while_write` conc=8 (issue #1143). The cap-bump on the
/// raw-chunk channel alone did not move read p99 in the external harness, because
/// the cost is dominated by THIS per-row cross-thread wake, not the chunk hand-off.
///
/// Batching `BATCH_EMIT_ROWS` rows into a single `Vec` item amortizes that wake
/// ~`BATCH_EMIT_ROWS`× (one wake per batch instead of one per row) while keeping
/// the bounded-heap and backpressure guarantees: a batch holds at most this many
/// `(RowKey, ScanRow)` already-owned entries (the same entries the prior code held
/// transiently in `scratch`), and the batch channel is bounded
/// ([`BATCH_CHANNEL_CAP`]) so a slow consumer still stops the parse loop, which
/// still stops draining raw chunks, which still stops disk reads. Order is
/// preserved (entries are pushed and drained FIFO, batches sent in order). A
/// modest value keeps the per-batch heap small; tiny / sparse result sets do not
/// wait for a full batch because the pending tail is flushed at every chunk/window
/// drain boundary (and at stream end), preserving incremental first-row delivery.
pub(crate) const BATCH_EMIT_ROWS: usize = 256;

/// Bound on the batched-row channel feeding the async forwarder. Small: the
/// forwarder flattens batches into the public per-item channel roughly as fast as
/// the parse half produces them. Live heap on this channel is at most
/// `BATCH_CHANNEL_CAP * BATCH_EMIT_ROWS` entries; with the defaults that is
/// ~512 small `(RowKey, ScanRow)` entries — negligible against the <128MB budget.
const BATCH_CHANNEL_CAP: usize = 2;

/// Documented bound on the rows the windowed scan's BATCHING SUBSYSTEM may hold in
/// flight ahead of the caller, INDEPENDENT of `scan_stream`'s `buffer_size`
/// (roborev finding, issue #1143).
///
/// # Scope (read this first)
///
/// This bounds the BATCHING subsystem ALONE — the pending `batch`, the bounded batch
/// channel, and the parked-in-`blocking_send` batch — the *additional* in-flight pool
/// issue-#1143 batching adds, NOT sized by `buffer_size`. It is NOT the whole-pipeline
/// resident-row bound: the complete worst case (module doc) is
/// `buffer_size + max_partition_size + MAX_INFLIGHT_BATCH_ROWS`, and this constant
/// does NOT cover the inherent pre-existing-#1156 `max_partition_size` term (the one
/// confirmed partition `drain_scan_window` materializes in `scratch` before
/// batching). It is named/tested so a future `BATCH_EMIT_ROWS`/`BATCH_CHANNEL_CAP`
/// tweak cannot silently let batching run arbitrarily far ahead of a stalled consumer.
///
/// # The bound (batching subsystem)
///
/// When the public consumer is stalled, THREE full batches coexist in flight — not
/// two (each up to `BATCH_EMIT_ROWS` rows):
///   1. **Forwarder-held.** The async forwarder `recv()`'d one batch and is blocked
///      flattening it into the caller's full `tx`; it has LEFT the batch channel.
///   2. **Channel-resident.** The bounded batch channel is FULL —
///      `BATCH_CHANNEL_CAP` items — because the forwarder (stuck on (1)) stopped draining.
///   3. **Producer-blocked.** The `spawn_blocking` parse half assembled its NEXT
///      full batch and is PARKED in `tx.blocking_send`, still OWNING it because the
///      channel (full, per (2)) has no slot — `blocking_send` moves the value only
///      on success, so a parked send holds a third, distinct batch.
///
/// All three coexist at one instant; the pending tail in `drain_scan_window` is NOT
/// a fourth — once it REACHES `BATCH_EMIT_ROWS` it BECOMES (3) via `blocking_send`,
/// and a producer parked in that send accumulates no further tail. So bounding by
/// `(BATCH_CHANNEL_CAP + 2) * BATCH_EMIT_ROWS` covers channel + forwarder-held (+1)
/// + producer-blocked-in-send (+1).
///
/// With the defaults (`256 * 4 = 1024`) this equals — never exceeds — every real
/// caller's `buffer_size` (`StreamingConfig::buffer_size` defaults to 1024, the
/// text preset is 512 ≤ this), keeping this batching term comparable to the
/// channel term — yet the bound is a CONSTANT, not `buffer_size`-scaled, so it
/// holds even for `buffer_size = 1`. (The full resident-row worst case adds the
/// `max_partition_size` term on top; see the module doc.)
///
/// [`tests::batch_inflight_rows_are_bounded_independent_of_buffer_size`] asserts
/// the batching subsystem's runtime worst case never exceeds this; any sizing
/// change that breaks the bound must update this constant AND keep that test green.
pub(super) const MAX_INFLIGHT_BATCH_ROWS: usize = (BATCH_CHANNEL_CAP + 2) * BATCH_EMIT_ROWS;

/// Which public surface the windowed driver's forwarder adapts its single
/// internal `Vec`-batched stream to (issue #1592, Epic F/F2).
///
/// The driver ([`SSTableReader::run_scan_stream_windowed`]) always produces one
/// internal batched stream (the pre-existing #1143 batch channel). This selects
/// how [`SSTableReader::spawn_windowed_forwarder`] delivers it to the caller —
/// flattened to per-row (the historical contract, an adapter over the batches) or
/// forwarded straight through as `Vec` batches (one async wake per batch).
pub(super) enum WindowedOut {
    /// Historical per-item surface: each `Vec` batch is FLATTENED into
    /// `(RowKey, ScanRow)` items on `tx`, one send per row.
    PerRow(mpsc::Sender<Result<(RowKey, ScanRow)>>),
    /// Batched surface (F2): each internal `Vec` batch is forwarded straight
    /// through on `tx`, one send per batch — so the consumer is woken once per
    /// batch instead of once per row.
    Batched(mpsc::Sender<Result<Vec<(RowKey, ScanRow)>>>),
}

/// Decide whether the blocking parse half should run its terminal
/// (`at_final_chunk = true`) drain once the raw-chunk stream has ended.
///
/// The terminal drain parses the trailing window AS IF it were a complete final
/// partition (it collapses `NeedMore` → `Done`). That is correct only on a CLEAN
/// EOF. If the I/O half stopped on a mid-stream read error it sets `io_failed`
/// before dropping the raw-chunk sender, so the trailing window is a TRUNCATED
/// fragment; running the terminal drain on it would emit a spurious/garbage
/// partition through `tx` BEFORE the caller surfaces the I/O `Err` (issue #1143
/// finding 2). Skipping it makes a truncated/corrupt stream yield ONLY the error.
///
/// Pulled out as a tiny pure function so the drain-skip decision is unit-tested
/// directly (issue #1143 finding 1): a future refactor that drops the flag,
/// flips the load, or reorders the store-before-`drop(raw_tx)` must fail
/// [`tests::terminal_drain_skipped_iff_io_failed`].
#[inline]
fn should_run_terminal_drain(io_failed: bool) -> bool {
    !io_failed
}

/// Flush any rows already buffered in `batch` to the batched-row channel as one
/// item, draining `batch`. Used at EVERY error-exit of the blocking parse half
/// so confirmed rows produced before a mid-stream parse/decompress error are
/// delivered to the consumer ahead of the terminal `Err` — matching the
/// pre-#1156 per-row send contract (the consumer must see all confirmed rows up
/// to the failure, not just the earlier full batches).
///
/// Returns the send result; the caller propagates the original error regardless
/// (a failed flush means the consumer already dropped `rx`, so there is nobody
/// left to receive the rows OR the error — the scan is terminating either way).
/// A no-op when `batch` is empty. Roborev finding (issue #1143): the batching
/// change must not silently drop confirmed-up-to-error rows.
#[inline]
fn flush_pending(
    batch: &mut Vec<(RowKey, ScanRow)>,
    tx: &mpsc::Sender<Result<Vec<(RowKey, ScanRow)>>>,
) {
    if !batch.is_empty() {
        let _ = tx.blocking_send(Ok(std::mem::take(batch)));
    }
}

/// Finish the blocking parse half: deliver the rows still pending in `batch` and
/// return the drain outcome to the caller.
///
/// This is the load-bearing error-flush seam (roborev finding, issue #1143).
/// `drained` is the result of the fallible parse loop + terminal drain:
///
///   - `Err(e)`: a mid-stream decompress/parse error. FLUSH `batch` first so the
///     consumer receives every confirmed-up-to-error row ahead of the terminal
///     `Err` — matching the pre-batching per-row send contract — THEN propagate
///     `e`. Removing this flush silently drops the confirmed-but-unflushed pending
///     rows on an error, which is exactly the regression
///     [`tests::finish_blocking_drain_flushes_pending_before_error`] pins.
///   - `Ok(())` and the consumer is still attached (`!broke`): flush the trailing
///     partial batch so the last rows reach the consumer.
///   - `Ok(())` but `broke` (consumer dropped): nothing to deliver — drop `batch`.
///
/// Pulled out of [`SSTableReader::drain_scan_window_blocking`] so the error-path
/// flush is unit-tested in isolation with a genuinely NON-EMPTY pending batch. In
/// the live driver the in-stream per-chunk flush usually empties `batch` at a
/// chunk boundary BEFORE a separate-chunk decompress error fires, which would make
/// an end-to-end "corrupt trailing chunk" test vacuous; testing this seam directly
/// guarantees the error flush is exercised and FAILS if it is removed.
fn finish_blocking_drain(
    drained: Result<()>,
    batch: &mut Vec<(RowKey, ScanRow)>,
    broke: bool,
    tx: &mpsc::Sender<Result<Vec<(RowKey, ScanRow)>>>,
) -> Result<()> {
    match drained {
        Err(e) => {
            flush_pending(batch, tx);
            Err(e)
        }
        Ok(()) => {
            if !broke {
                flush_pending(batch, tx);
            }
            Ok(())
        }
    }
}

/// Inputs the blocking parse half needs that the I/O half resolves once up front
/// (so the blocking task does not have to touch the async runtime).
struct WindowParseCtx {
    // Issue #1578: no `table_id` — the stitch path does not filter by it (see the
    // parse closure in `run_scan_stream_windowed`).
    start_key: Option<RowKey>,
    end_key: Option<RowKey>,
    schema: Option<crate::schema::TableSchema>,
    /// Cassandra stores a chunk RAW when its compressed length would meet or
    /// exceed this (Bug #639, epic #970, issue #1104); honour the same rule as
    /// `stitch_all_chunks` so the windowed path decodes identically.
    max_compressed_length: usize,
}

impl SSTableReader {
    /// Async I/O half of the windowed streaming scan (issue #1143).
    ///
    /// Reads raw compressed chunks from `cursor` and forwards them to a single
    /// `spawn_blocking` parse task ([`drain_scan_window_blocking`]) over a
    /// bounded channel; the parse task owns all decompress+parse CPU and emits
    /// results through `tx`. See the module docs for the full rationale.
    ///
    /// Precondition: `cursor`'s file is seeked to the start of the data section.
    pub(super) async fn run_scan_stream_windowed(
        self: Arc<Self>,
        // Issue #1578: unused — the stitch path does not filter by table_id.
        _table_id: TableId,
        start_key: Option<RowKey>,
        end_key: Option<RowKey>,
        schema: Option<crate::schema::TableSchema>,
        cursor: &ScanCursor,
        // Which public surface to adapt the internal batched stream to (issue
        // #1592): per-row (flatten) or batched (straight-through). The rest of the
        // driver — I/O feed, blocking parse, join, backpressure — is identical for
        // both; only the forwarder arm differs.
        out: WindowedOut,
    ) -> Result<()> {
        // Admission control (issue #1594, F4) is applied by the CALLER at
        // top-level scan-OPERATION granularity, NOT here per sub-scan: a direct
        // scan acquires its permit in `run_scan_stream` (`ScanAdmission::Acquire`),
        // and a cross-generation fan-out merge acquires ONE permit for the whole
        // operation and opens each sub-scan `ScanAdmission::Exempt`. So this
        // windowed sub-scan does NOT admit — a fan-out to `N > cap` generations
        // would otherwise let `cap` sub-scans hold permits and park in backpressure
        // while the priming merge blocks forever waiting on the rest (the deadlock
        // the per-sub-scan design introduced). See `scan_admission`'s module docs.

        // Resolve everything the parser needs ONCE, here on the async side, so
        // the blocking task never touches the async runtime. Schema resolution
        // matches the previous `parse_stitched_stream` resolution exactly.
        let ctx = WindowParseCtx {
            start_key,
            end_key,
            schema: schema.or_else(|| self.get_table_schema(None)),
            max_compressed_length: self
                .compression_info
                .as_ref()
                .map(|ci| ci.max_compressed_length as usize)
                .unwrap_or(usize::MAX),
        };

        // Raw-chunk pipe: I/O half -> blocking parse half (bounded for heap +
        // backpressure). Output backpressure rides on the batch channel inside
        // the task.
        let (raw_tx, raw_rx) = mpsc::channel::<Vec<u8>>(RAW_CHUNK_CHANNEL_CAP);
        // Batched-row pipe: blocking parse half -> async forwarder task. The
        // parse half buffers up to `BATCH_EMIT_ROWS` surviving entries per item so
        // the expensive blocking→async wake happens once per batch instead of once
        // per row (issue #1143). The forwarder task flattens each batch into the
        // caller's per-item `tx`, preserving the public stream contract.
        let (batch_tx, batch_rx) =
            mpsc::channel::<Result<Vec<(RowKey, ScanRow)>>>(BATCH_CHANNEL_CAP);
        // The batching subsystem (this bounded channel + the forwarder-held batch +
        // the producer's parked-in-send batch) holds at most `MAX_INFLIGHT_BATCH_ROWS`
        // confirmed rows ahead of a stalled caller, INDEPENDENT of `scan_stream`'s
        // `buffer_size` (roborev finding, issue #1143). The assert ties the runtime
        // channel sizing to that named bound so a capacity change cannot exceed it.
        debug_assert!(
            (BATCH_CHANNEL_CAP + 2) * BATCH_EMIT_ROWS <= MAX_INFLIGHT_BATCH_ROWS,
            "batch channel sizing must stay within the documented MAX_INFLIGHT_BATCH_ROWS bound \
             (channel-resident BATCH_CHANNEL_CAP + forwarder-held 1 + producer-blocked-in-send 1)"
        );
        // Distinguishes a clean EOF (sender dropped after `Ok(None)`) from a
        // mid-stream read error (sender dropped after `Err`). On error the parse
        // half must NOT run its `at_final_chunk = true` terminal drain — a
        // truncated/partial trailing window would otherwise emit a spurious
        // partition through `tx` BEFORE this function returns the `Err` (issue
        // #1143 finding 2; the pre-#1156 path `?`-propagated the read error and
        // never ran a final drain). Set before `raw_tx` is dropped so the parse
        // half observes it via the channel-close happens-before.
        let io_failed = Arc::new(AtomicBool::new(false));
        let reader = Arc::clone(&self);
        let task_io_failed = Arc::clone(&io_failed);
        let parse_task = tokio::task::spawn_blocking(move || {
            reader.drain_scan_window_blocking(ctx, raw_rx, batch_tx, task_io_failed)
        });

        // Forwarder task: adapt the blocking parse half's batched stream to the
        // caller's chosen public surface (per-row flatten or batched
        // straight-through; issue #1592). Runs CONCURRENTLY with the I/O feed loop
        // below — both are needed at once (the I/O loop produces the raw chunks
        // the parse half consumes; the parse half produces the batches this task
        // drains), so running them sequentially would deadlock. On the async
        // runtime, so its wakes are cheap async-worker wakes, not blocking-pool
        // condvar wakes.
        //
        // Backpressure: a slow consumer blocks the `send().await` inside the
        // forwarder, which stops draining `batch_rx`, which (being bounded) blocks
        // the parse half's `blocking_send`, which stops the parse loop and
        // ultimately disk reads — the exact end-to-end backpressure shape PR #1156
        // had, just batched, on BOTH arms. If the consumer drops its receiver, the
        // send fails, the forwarder returns and drops `batch_rx`; the parse half's
        // next `blocking_send` then fails so it terminates (`*broke = true`).
        let forwarder = Self::spawn_windowed_forwarder(out, batch_rx);

        // Feed raw compressed chunks to the parse task. The bounded `raw_tx`
        // applies backpressure all the way back to disk reads when the consumer
        // (and thus the parse task) falls behind.
        //
        // Determine the ACTUAL scan backend once (the per-scan cursor mutex is
        // uncontended, so this lock is effectively free). A synchronously-faulting
        // backend (mmap page fault / `O_DIRECT` `pread`) does its disk I/O
        // *inside* `poll_read`, so reading it on an async worker blocks — and K
        // concurrent cold scans pin the whole pool, stalling every warm point read
        // (issue #1593, F3). Route such a backend's read loop off the async
        // workers; the buffered backend is genuinely async (`tokio::fs`, reactor-
        // driven) and stays inline on the runtime.
        let faulting_backend = cursor.file.lock().await.faults_synchronously();

        let io_err: Option<Error> = if faulting_backend {
            Self::feed_raw_chunks_blocking(Arc::clone(&self), cursor, raw_tx, &io_failed).await
        } else {
            self.feed_raw_chunks_async(cursor, raw_tx, &io_failed).await
        };

        // Join the parse task; its Result is the scan's Result. An I/O error
        // takes precedence (the task only saw a truncated stream). The parse task
        // owns `batch_tx`; when it returns, `batch_tx` drops, so the forwarder's
        // `batch_rx.recv()` will observe channel close after the last batch.
        let parse_result = match parse_task.await {
            Ok(r) => r,
            Err(join_err) => Err(Error::corruption(format!(
                "run_scan_stream_windowed: parse task failed: {join_err}"
            ))),
        };
        // Then await the forwarder so every batched row reaches the consumer
        // before this function returns (the public stream must not drop the
        // trailing batch). The forwarder cannot fail the scan — it only flattens
        // already-produced rows — so a join error there is non-fatal; the parse
        // task's `Result` plus `io_err` remain canonical.
        let _ = forwarder.await;
        if let Some(e) = io_err {
            return Err(e);
        }
        parse_result
    }

    /// Spawn the forwarder that drains the internal `Vec`-batched channel into the
    /// caller's public surface (issue #1592, Epic F/F2).
    ///
    /// This is the ONE place per-row and batched delivery diverge; everything
    /// upstream (I/O feed, blocking decompress+parse, the internal batch channel,
    /// backpressure) is shared. The per-row arm is a thin flattening adapter over
    /// the same batched stream the batched arm forwards straight through, so the
    /// two surfaces are guaranteed to yield identical rows in identical order (the
    /// batched arm's output flattened equals the per-row arm's output).
    ///
    /// Both arms preserve backpressure: a stalled consumer blocks the `send().await`
    /// here, stopping the drain of `batch_rx`, which (bounded) blocks the parse
    /// half's `blocking_send`, all the way back to disk reads. On consumer drop the
    /// `send` fails and the forwarder returns, dropping `batch_rx` so the parse
    /// half terminates.
    fn spawn_windowed_forwarder(
        out: WindowedOut,
        mut batch_rx: mpsc::Receiver<Result<Vec<(RowKey, ScanRow)>>>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            match out {
                // Per-row (historical) surface: FLATTEN each confirmed batch back
                // into single `(RowKey, ScanRow)` items. One send per row.
                WindowedOut::PerRow(tx) => {
                    while let Some(batch) = batch_rx.recv().await {
                        match batch {
                            Ok(rows) => {
                                for entry in rows {
                                    if tx.send(Ok(entry)).await.is_err() {
                                        return; // consumer dropped
                                    }
                                }
                            }
                            Err(e) => {
                                // Parse half surfaced a mid-stream error; forward it
                                // as a terminal item and stop. The parse half also
                                // returns the same error via its `Result`.
                                let _ = tx.send(Err(e)).await;
                                return;
                            }
                        }
                    }
                }
                // Batched (F2) surface: FORWARD each confirmed batch straight
                // through — one send per batch, not per row. A terminal error is
                // forwarded as one item then the stream ends.
                WindowedOut::Batched(tx) => {
                    while let Some(batch) = batch_rx.recv().await {
                        let terminal = batch.is_err();
                        if tx.send(batch).await.is_err() {
                            return; // consumer dropped
                        }
                        if terminal {
                            return;
                        }
                    }
                }
            }
        })
    }

    /// I/O feed loop for a genuinely-async (buffered) backend: read the next
    /// compressed chunk on the async runtime and forward it over the bounded
    /// `raw_tx`. `tokio::fs` reads are reactor-driven and non-blocking, so this
    /// belongs inline on the worker (issue #1593, F3). Returns the terminal read
    /// error, if any; consumes `raw_tx` so it drops on return, signalling EOF to
    /// the parse half (only when `io_failed` stayed false — see the parse half).
    async fn feed_raw_chunks_async(
        &self,
        cursor: &ScanCursor,
        raw_tx: mpsc::Sender<Vec<u8>>,
        io_failed: &Arc<AtomicBool>,
    ) -> Option<Error> {
        // Flip `io_failed` on any unwind so a panic in `read_next_block` cannot
        // masquerade as a clean EOF and make the parse half emit a truncated
        // trailing partition (roborev finding, issue #1593). Disarmed on the
        // clean-EOF / consumer-ended exit below so the happy path is unchanged.
        let mut panic_guard = FeedFailureGuard::armed(io_failed);
        loop {
            match self.read_next_block(cursor).await {
                Ok(Some(chunk)) => {
                    if raw_tx.send(chunk).await.is_err() {
                        // Parse task ended early (consumer dropped or parse error).
                        // Stop reading; the task's result is canonical.
                        break;
                    }
                }
                Ok(None) => break, // EOF
                Err(e) => {
                    // Tell the parse half to SKIP the terminal final drain: the
                    // stream is truncated, so the trailing window is partial and
                    // must not be emitted. The store is sequenced before the
                    // `drop(raw_tx)` (on return) the parse half synchronizes on.
                    io_failed.store(true, Ordering::SeqCst);
                    return Some(e);
                }
            }
        }
        // Clean EOF (or consumer ended early): leave `io_failed` false so the
        // parse half runs its terminal drain exactly as before.
        panic_guard.disarm();
        None
        // `raw_tx` drops here on return, signalling clean EOF to the parse half.
    }

    /// I/O feed loop for a synchronously-faulting backend (mmap page fault /
    /// `O_DIRECT` `pread`): run the WHOLE per-scan read loop on ONE
    /// `spawn_blocking` thread so the blocking disk I/O never pins an async worker
    /// (issue #1593, F3). One offload per scan (mirroring the parse half — never
    /// per chunk, which would over-spawn; per-scan admission is F4's scope). Feeds
    /// the same bounded `raw_tx` via `blocking_send` (backpressure preserved: a
    /// full channel parks this thread). Consumes `raw_tx` (moved into the task) so
    /// it drops when the task returns, signalling EOF exactly as the async loop.
    async fn feed_raw_chunks_blocking(
        reader: Arc<Self>,
        cursor: &ScanCursor,
        raw_tx: mpsc::Sender<Vec<u8>>,
        io_failed: &Arc<AtomicBool>,
    ) -> Option<Error> {
        let io_file = Arc::clone(&cursor.file);
        let io_chunk_index = Arc::clone(&cursor.chunk_index);
        let io_failed_feed = Arc::clone(io_failed);
        let feed = tokio::task::spawn_blocking(move || -> Option<Error> {
            // Panic/early-exit guard (roborev finding, issue #1593). `raw_tx` is
            // captured (moved) into this closure and drops when the closure
            // returns OR unwinds; the parse half reads a `raw_tx` close with
            // `io_failed == false` as a CLEAN EOF and runs its terminal drain. If
            // this closure PANICS, `raw_tx` drops during unwind while the only
            // other `io_failed = true` store (the `Err(join_err)` arm below) runs
            // LATER — so the parse half would spuriously terminal-drain a
            // truncated window. Arming this guard as a BODY-LOCAL flips
            // `io_failed = true` on unwind BEFORE the captured `raw_tx` drops
            // (body locals drop before a move closure's captured environment).
            // Disarmed on the clean-EOF / consumer-ended exit so the happy path is
            // byte-identical.
            let mut panic_guard = FeedFailureGuard::armed(&io_failed_feed);
            // Record the thread this scan's raw reads run on so a guard test can
            // prove it is a spawn_blocking thread, NOT an async worker (F3).
            // Compiled only under the non-default `scan-offload-probe` feature.
            #[cfg(feature = "scan-offload-probe")]
            probe::record_io_read_thread();
            loop {
                // SOUNDNESS (issue #1593): driving the async read under
                // `futures::executor::block_on` on this blocking thread is sound
                // ONLY because the mmap/`O_DIRECT` per-chunk read path touches no
                // tokio reactor/timer primitive — the per-scan cursor `Mutex` is
                // executor-agnostic (semaphore + wakers, no reactor);
                // `MmapCursor`/`DirectCursor` `poll_*` are pure synchronous
                // memory/`pread` ops; the CRC digest is preloaded in memory at
                // open (no per-chunk `tokio::fs`); and the transient-retry re-seek
                // uses no `tokio::time`. This branch is NEVER taken for the
                // buffered backend (which needs the reactor). A nested tokio
                // runtime or `block_in_place` would panic on a `spawn_blocking`
                // thread; `futures::executor::block_on` is a plain thread-parking
                // executor with no tokio involvement, so it does not.
                match futures::executor::block_on(
                    reader.read_next_block_parts(&io_file, &io_chunk_index),
                ) {
                    Ok(Some(chunk)) => {
                        if raw_tx.blocking_send(chunk).is_err() {
                            break; // consumer/parse ended early
                        }
                    }
                    Ok(None) => break, // EOF
                    Err(e) => {
                        io_failed_feed.store(true, Ordering::SeqCst);
                        return Some(e);
                    }
                }
            }
            // Clean EOF (or consumer ended early): leave `io_failed` false so the
            // parse half runs its terminal drain exactly as before.
            panic_guard.disarm();
            None
            // `raw_tx` drops here (moved into the closure), signalling clean EOF.
        });
        match feed.await {
            Ok(e) => e,
            Err(join_err) => {
                io_failed.store(true, Ordering::SeqCst);
                Some(Error::corruption(format!(
                    "run_scan_stream_windowed: I/O feed task failed: {join_err}"
                )))
            }
        }
    }

    /// Blocking parse half of the windowed streaming scan (issue #1143).
    ///
    /// Runs entirely on a `spawn_blocking` thread — NEVER on an async worker.
    /// Owns the sliding `window` (a [`WindowCursor`], issue #1589); for each raw chunk pulled from
    /// `raw_rx` it applies the incompressible-raw fallback or decompresses,
    /// appends to the window, and drains every confirmed partition via
    /// [`drain_scan_window`]. On a CLEAN `raw_rx` close (I/O EOF) it runs a final
    /// drain with `at_final_chunk = true`; on a close caused by a mid-stream read
    /// error (`io_failed` set by the I/O half before it dropped the sender) it
    /// SKIPS that terminal drain so a truncated window cannot emit a spurious
    /// trailing partition (issue #1143 finding 2). Surviving `(RowKey, ScanRow)`
    /// entries are accumulated into batches of up to [`BATCH_EMIT_ROWS`] and sent
    /// through `tx` (a `Vec`-batched channel) with `blocking_send`, mirroring the
    /// pre-#1156 `parse_stitched_stream` backpressure but amortizing the
    /// blocking→async wake one-per-batch instead of one-per-row (issue #1143).
    ///
    /// On a mid-stream decompress/parse `Err`, any rows already buffered in the
    /// pending batch are flushed to `tx` BEFORE the error is propagated (via
    /// [`flush_pending`]), so the consumer receives every confirmed-up-to-error
    /// row ahead of the terminal `Err` — preserving the pre-batching per-row
    /// send contract (roborev finding, issue #1143).
    fn drain_scan_window_blocking(
        &self,
        ctx: WindowParseCtx,
        mut raw_rx: mpsc::Receiver<Vec<u8>>,
        tx: mpsc::Sender<Result<Vec<(RowKey, ScanRow)>>>,
        io_failed: Arc<AtomicBool>,
    ) -> Result<()> {
        use crate::storage::sstable::compression::Compression;

        // Issue #1741: single-gen full-scan read path applies SELECT-semantic
        // read shadowing (partition/range tombstone + TTL), so build the parser
        // with read_shadowing = true.
        let parser = self.build_v5_parser(true);
        // Sliding front-cursor window (issue #1589): compacts once per refill.
        let mut window = WindowCursor::new();
        let mut broke = false;
        let mut chunk_count = 0usize;
        // Pending surviving rows not yet handed to the forwarder. Flushed as a
        // single batch item when it reaches `BATCH_EMIT_ROWS`, and once more at
        // stream end (the partial tail). Bounded by `BATCH_EMIT_ROWS` + one
        // partition's worth of rows, so it stays within the sliding-window heap
        // bound.
        let mut batch: Vec<(RowKey, ScanRow)> = Vec::with_capacity(BATCH_EMIT_ROWS);
        // Reused scratch buffer for ONE confirmed partition's surviving
        // `(RowKey, ScanRow)` entries. Hoisted OUT of the per-partition loop in
        // [`drain_scan_window`] (issue #1333, follow-up to #1046) and passed by
        // `&mut` so it is allocated ONCE for the whole scan and `.clear()`-reused
        // per partition. `clear()` drops the prior partition's entries but PRESERVES
        // capacity, so a warmed buffer performs ZERO per-partition backing
        // allocations — the #1046 "buffers should be reused, do not allocate as we
        // iterate" mandate, extended to the per-partition scratch. Its peak size is
        // still bounded by one partition's rows (`max_partition_size`), the same
        // bound the transient per-partition `Vec::new()` had.
        let mut scratch: Vec<(RowKey, ScanRow)> = Vec::new();

        // The parse loop + terminal drain are fallible; on ANY `Err` we must
        // first deliver the rows already buffered in `batch` (confirmed rows
        // produced before the failure) so the consumer sees them ahead of the
        // terminal error — the pre-#1156 per-row send delivered every
        // confirmed-up-to-error row, and batching must preserve that (roborev
        // finding, issue #1143). Run the fallible work in a closure and flush
        // `batch` before propagating any error it returns.
        let drained: Result<()> = (|| {
            // Single decode plane (issue #1598, G2): the windowed-scan blocking half
            // receives compressed bytes over the channel (already read by the async I/O
            // half), so it routes through `decode_and_cache` directly (no self-read).
            // Build a minimal CompressionInfo for max_compressed_length (the only field
            // the decode path needs); Compression is built once outside the loop.
            let compression_opt = self
                .compression_reader
                .as_ref()
                .map(|cr| Compression::new(*cr.algorithm()))
                .transpose()?;
            let comp_info_dummy = crate::storage::sstable::compression_info::CompressionInfo {
                algorithm: String::new(),
                option_pairs: vec![],
                chunk_length: 0,
                max_compressed_length: ctx.max_compressed_length as u32,
                data_length: 0,
                chunk_offsets: vec![],
            };
            let chunk_source = super::chunk_source::ChunkSource::new(
                self.point_source.as_ref(), // unused by decode_and_cache
                &comp_info_dummy,
                compression_opt.as_ref(),
                &self.chunk_cache,
                0, // unused
                0, // unused
                super::data_access::NS_WINDOWED_CHUNK,
                self.chunk_cache_id,
            );

            while let Some(compressed_chunk) = raw_rx.blocking_recv() {
                // Windowed scan: incompressible-raw chunks skip the cache (documented
                // D-choice, task 4.4). The key is ABSOLUTE chunk index (I/O half feeds
                // from data-section start in order).
                let incompressible = compressed_chunk.len() >= ctx.max_compressed_length;
                let chunk: std::sync::Arc<[u8]> = if incompressible {
                    // Raw passthrough: no cache, no decompress
                    std::sync::Arc::from(compressed_chunk.into_boxed_slice())
                } else {
                    let key = crate::storage::cache::ChunkKey::new(
                        self.chunk_cache_id ^ super::data_access::NS_WINDOWED_CHUNK,
                        chunk_count as u64,
                    );
                    // Check cache BEFORE decode_and_cache (issue #1598 roborev Medium):
                    // warm scans must take the hit, not re-decompress and overwrite.
                    // Mirror what chunk()/range() do: hit → Arc clone, miss → decode.
                    if let Some(hit) = self.chunk_cache.get(&key) {
                        hit
                    } else {
                        chunk_source.decode_and_cache(key, compressed_chunk, false)?
                    }
                };
                window.refill(&chunk);
                chunk_count += 1;

                // Not the final chunk yet: drain confirmed partitions; NeedMore
                // means "await more bytes" (a partition straddles this boundary).
                self.drain_scan_window(
                    &parser,
                    &ctx,
                    &mut window,
                    false,
                    &tx,
                    &mut batch,
                    &mut scratch,
                    &mut broke,
                )?;
                if broke {
                    return Ok(());
                }

                // Per-chunk flush cadence (roborev finding, issue #1143). The
                // drain above stopped because no further partition can be
                // confirmed until the NEXT raw chunk arrives (NeedMore / empty
                // window), so any rows still buffered in `batch` are FULLY
                // confirmed — they will not be amended or joined by a same-chunk
                // partition. Flushing the pending tail here (a no-op when empty)
                // restores `scan_stream`'s incremental-delivery contract and
                // first-row latency for sparse / sub-`BATCH_EMIT_ROWS` result
                // sets, which the pure-batching change regressed (those emitted
                // nothing until full scan completion). Dense windows still hit
                // the full-batch flush inside `drain_scan_window`, so we wake the
                // forwarder at most ~once per chunk PLUS once per full batch —
                // far below PR #1156's one-wake-per-ROW, preserving the perf win.
                if !batch.is_empty() {
                    if tx.blocking_send(Ok(std::mem::take(&mut batch))).is_err() {
                        broke = true; // consumer dropped
                        return Ok(());
                    }
                    batch.reserve(BATCH_EMIT_ROWS);
                }
            }

            // Stream end. On a CLEAN EOF run the final drain — a trailing
            // partition with no END_OF_PARTITION marker is now terminal (Done),
            // not a refill request that will never come. But if the I/O half
            // stopped on a read ERROR (`io_failed`), the trailing window is a
            // truncated fragment of a partition; running `at_final_chunk = true`
            // here would parse and emit it as if complete, surfacing a
            // partial/garbage row BEFORE the caller returns the I/O `Err`. Skip
            // the final drain so a truncated/corrupt stream yields ONLY the error
            // (issue #1143 finding 2). The store in the I/O half happens-before
            // the `drop(raw_tx)` that ended `blocking_recv`, so this load
            // observes it.
            if !broke && should_run_terminal_drain(io_failed.load(Ordering::SeqCst)) {
                self.drain_scan_window(
                    &parser,
                    &ctx,
                    &mut window,
                    true,
                    &tx,
                    &mut batch,
                    &mut scratch,
                    &mut broke,
                )?;
            }
            Ok(())
        })();

        // Finish: deliver any rows still in `batch` and propagate the drain
        // outcome. On `Err`, the pending rows are flushed BEFORE the error so the
        // consumer sees every confirmed-up-to-error row ahead of the terminal Err
        // (roborev finding, issue #1143); on success the trailing partial batch is
        // flushed (unless the consumer already dropped, `broke`). Extracted so the
        // error-path flush is unit-tested directly with a NON-EMPTY pending batch
        // (`tests::finish_blocking_drain_flushes_pending_before_error`) — that test
        // fails if the error flush is dropped, keeping the guard non-vacuous even
        // though the in-stream per-chunk flush usually empties `batch` first.
        finish_blocking_drain(drained, &mut batch, broke, &tx)?;

        // Test-only probe (issue #1143 regression guard): record the thread that
        // ran the parse so a guard test can prove it was NOT an async worker.
        // Compiled ONLY under the non-default `scan-offload-probe` feature; a
        // true no-op (not even referenced) in normal/release builds.
        #[cfg(feature = "scan-offload-probe")]
        probe::record_parse_thread();

        log::debug!(
            "drain_scan_window_blocking: drained {} chunks (final window {} bytes)",
            chunk_count,
            window.len()
        );
        Ok(())
    }

    /// Drain every confirmed partition from the front of the sliding `window`,
    /// accumulating each surviving `(RowKey, ScanRow)` into `batch` and flushing a
    /// full `BATCH_EMIT_ROWS`-sized batch through `tx` (issue #1143).
    ///
    /// Synchronous (runs on the `spawn_blocking` thread). Drives
    /// [`parse_one_partition_with_timestamps`], drops the per-row timestamp, and
    /// applies the same `table_ids_match` + key-range + `filter_tombstone`
    /// filters the prior driver applied. After each `Emitted(consumed)` the
    /// consumed prefix is removed, keeping the window's peak bounded by
    /// `max_partition_size + one_chunk`. Rows are pushed onto `batch` in scan
    /// order; whenever `batch` reaches `BATCH_EMIT_ROWS` it is flushed as ONE
    /// channel item (amortizing the blocking→async wake). The trailing partial
    /// batch is flushed by the caller at stream end. Stops at `NeedMore` / `Done`
    /// (await the next chunk / genuine end) or when the consumer is dropped
    /// (`*broke`).
    #[allow(clippy::too_many_arguments)]
    fn drain_scan_window(
        &self,
        parser: &crate::storage::sstable::reader::parsing::V5CompressedLegacyParser,
        ctx: &WindowParseCtx,
        window: &mut WindowCursor,
        at_final_chunk: bool,
        tx: &mpsc::Sender<Result<Vec<(RowKey, ScanRow)>>>,
        batch: &mut Vec<(RowKey, ScanRow)>,
        scratch: &mut Vec<(RowKey, ScanRow)>,
        broke: &mut bool,
    ) -> Result<()> {
        use crate::storage::sstable::reader::parsing::ParseStep;

        loop {
            if *broke || window.is_empty() {
                return Ok(());
            }

            // Buffer this partition's surviving entries in the caller's REUSED
            // `scratch` buffer, then forward them via `blocking_send` AFTER the
            // parser returns. `parse_one_partition_with_timestamps` takes a
            // synchronous `FnMut` emit, so we cannot send inside it; a partition's
            // rows are bounded by `max_partition_size`, so this stays within the
            // window bound. `clear()` drops the previous partition's entries but
            // PRESERVES the backing capacity, so after warmup this loop performs
            // NO per-partition allocation for `scratch` (issue #1333, follow-up to
            // #1046). NeedMore/Done leave `scratch` empty (the parser only invokes
            // the emit closure on a CONFIRMED `Emitted`), so nothing leaks across
            // partitions. Issue #1334: entries carry the `ScanRow` row carrier.
            scratch.clear();
            // Snapshot the (retained) capacity BEFORE this partition's pushes so
            // the offload probe can count how many times `scratch` actually grows
            // its backing store across the whole scan — the direct signal that the
            // buffer is reused, not reallocated per partition (issue #1333 guard).
            #[cfg(feature = "scan-offload-probe")]
            let scratch_cap_before = scratch.capacity();
            let step = parser.parse_one_partition_with_timestamps(
                window.as_slice(),
                ctx.schema.as_ref(),
                self,
                at_final_chunk,
                &mut |(_entry_table_id, key, value, _ts)| {
                    // Issue #1578: this stitching path deliberately does NOT filter
                    // by `table_ids_match` — it mirrors the authoritative
                    // materializing `sequential_scan` stitch path, which skips it
                    // because the nb parser may report header-default table_ids.
                    // Applying it here dropped EVERY row of an nb SSTable whose parsed
                    // table_id diverged from the query (e.g. CQLite-written output).
                    if let Some(start) = ctx.start_key.as_ref() {
                        if &key < start {
                            return Ok(std::ops::ControlFlow::Continue(()));
                        }
                    }
                    if let Some(end) = ctx.end_key.as_ref() {
                        if &key > end {
                            return Ok(std::ops::ControlFlow::Continue(()));
                        }
                    }
                    // Suppress row tombstones from user-facing scan output (#505).
                    if !self.filter_tombstone(&value) {
                        return Ok(std::ops::ControlFlow::Continue(()));
                    }
                    scratch.push((key, value));
                    Ok(std::ops::ControlFlow::Continue(()))
                },
            )?;

            // Record whether this partition forced `scratch` to (re)allocate its
            // backing store. With the hoist this happens only while the buffer
            // grows to its high-water mark (a small bounded number of times per
            // scan); if the buffer were freshly allocated per partition it would
            // grow from empty EVERY partition, so the count would scale with
            // partition count — exactly what the #1333 guard asserts it does not.
            #[cfg(feature = "scan-offload-probe")]
            probe::note_scratch_capacity(scratch_cap_before, scratch.capacity());

            match step {
                ParseStep::Emitted(consumed) => {
                    let take = if consumed == 0 { 1 } else { consumed };
                    // Advance the cursor (no memmove); `consume` clamps to remaining.
                    window.consume(take);
                    // Accumulate this partition's surviving entries in scan order,
                    // flushing a full batch as ONE channel item whenever it
                    // reaches `BATCH_EMIT_ROWS`. `blocking_send` carries the same
                    // backpressure as the prior per-row send (this runs on a
                    // spawn_blocking thread) but wakes the async forwarder once per
                    // batch instead of once per row (issue #1143).
                    for entry in scratch.drain(..) {
                        batch.push(entry);
                        if batch.len() >= BATCH_EMIT_ROWS {
                            let full =
                                std::mem::replace(batch, Vec::with_capacity(BATCH_EMIT_ROWS));
                            if tx.blocking_send(Ok(full)).is_err() {
                                *broke = true; // consumer dropped
                                return Ok(());
                            }
                        }
                    }
                }
                // NeedMore: the partition straddles this chunk boundary. The
                // per-partition parser buffers a partition's rows internally and
                // only invokes our emit closure on a CONFIRMED `Emitted` return,
                // so on `NeedMore` our `scratch` buffer is empty — nothing was
                // forwarded and nothing is dropped. The caller appends the next
                // chunk and we re-parse this partition from its start, so no row
                // is duplicated or lost across the boundary. Record the straddle
                // (issue #1143) so the boundary re-parse path is observable; it is
                // suppressed at the final chunk (parser collapses NeedMore→Done).
                ParseStep::NeedMore => {
                    crate::observability::add_counter(
                        crate::observability::catalog::READ_SCAN_WINDOW_REFILL,
                        1,
                        &[],
                    );
                    return Ok(());
                }
                // Done: genuine end of partitions / terminal truncation.
                ParseStep::Done => return Ok(()),
            }
        }
    }
}

/// Test-only probe (issues #1143, #1333) for the windowed scan — see
/// `scan_stream_windowed_probe.rs`. Compiled ONLY under the non-default
/// `scan-offload-probe` feature, so it adds zero cost and no public surface in
/// normal/release builds (issue #1143 finding 1); kept in a sibling file so this
/// source stays under the campsite-rule size limit (epic #1116).
#[cfg(feature = "scan-offload-probe")]
#[doc(hidden)]
#[path = "scan_stream_windowed_probe.rs"]
pub mod probe;

// Unit + dataset-dependent guards live in a sibling file to keep this source
// file under the campsite-rule size limit (issue #1143). `use super::*` in the
// included module resolves to this module's private items.
#[cfg(test)]
#[path = "scan_stream_windowed_tests.rs"]
mod tests;
