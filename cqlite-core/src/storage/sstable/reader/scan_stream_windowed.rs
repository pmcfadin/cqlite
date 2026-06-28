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
//! Both are achievable together. This driver splits the work across two halves
//! of one bounded pipeline:
//!
//!   - **Async I/O half** (`run_scan_stream_windowed`): the only thing that must
//!     touch the async runtime is the chunk read (`read_next_block().await`
//!     awaits the per-scan cursor's async file lock). It does ONLY I/O: read the
//!     next raw compressed chunk and hand it to the parse half over a small
//!     bounded channel (`raw` channel, capacity [`RAW_CHUNK_CHANNEL_CAP`]).
//!   - **Blocking parse half** (`drain_scan_window_blocking`): a single
//!     `spawn_blocking` task owns the parser, the schema, and the sliding
//!     window. It pulls raw chunks with `blocking_recv`, decompresses, appends
//!     to the window, drains every confirmed partition, and emits each surviving
//!     `(RowKey, Value)` via `tx.blocking_send` — exactly the pre-#1156
//!     backpressure shape. ALL decompress+parse CPU runs here, off the async
//!     worker pool.
//!
//! ## Backpressure (preserved end-to-end)
//!
//! A slow consumer blocks `tx.blocking_send` in the parse half, which stops the
//! parse loop, which stops draining the `raw` channel, which (being bounded)
//! blocks `raw_tx.send().await` in the I/O half, which stops reading from disk.
//! Nothing buffers the whole file; live heap stays `window +
//! RAW_CHUNK_CHANNEL_CAP` chunks.
//!
//! ## Parity
//!
//! The emitted set/order is byte-identical to the prior inline driver: same
//! schema resolution, same incompressible-chunk raw fallback, same
//! `table_ids_match` / key-range / `filter_tombstone` filters, same
//! NeedMore/Done straddle handling and final-chunk semantics, same
//! `READ_SCAN_WINDOW_REFILL` counter. The only change is WHERE the CPU runs.

use super::data_access::table_ids_match;
use super::source::ScanCursor;
use super::SSTableReader;
use crate::types::{TableId, Value};
use crate::{Error, Result, RowKey};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

/// Bound on the raw-compressed-chunk channel feeding the blocking parse task.
/// Small: the parse half consumes chunks roughly as fast as the I/O half
/// produces them, so a tiny buffer absorbs scheduling jitter without unbounding
/// heap. Combined with the sliding window this keeps live heap at
/// `max_partition_size + (RAW_CHUNK_CHANNEL_CAP + 1) * one_chunk`.
const RAW_CHUNK_CHANNEL_CAP: usize = 2;

/// Inputs the blocking parse half needs that the I/O half resolves once up front
/// (so the blocking task does not have to touch the async runtime).
struct WindowParseCtx {
    table_id: TableId,
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
        table_id: TableId,
        start_key: Option<RowKey>,
        end_key: Option<RowKey>,
        schema: Option<crate::schema::TableSchema>,
        cursor: &ScanCursor,
        tx: &mpsc::Sender<Result<(RowKey, Value)>>,
    ) -> Result<()> {
        // Resolve everything the parser needs ONCE, here on the async side, so
        // the blocking task never touches the async runtime. Schema resolution
        // matches the previous `parse_stitched_stream` resolution exactly.
        let ctx = WindowParseCtx {
            table_id,
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
        // backpressure). Output backpressure rides on `tx` inside the task.
        let (raw_tx, raw_rx) = mpsc::channel::<Vec<u8>>(RAW_CHUNK_CHANNEL_CAP);
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
        let out_tx = tx.clone();
        let task_io_failed = Arc::clone(&io_failed);
        let parse_task = tokio::task::spawn_blocking(move || {
            reader.drain_scan_window_blocking(ctx, raw_rx, out_tx, task_io_failed)
        });

        // Feed raw compressed chunks to the parse task. The bounded `raw_tx`
        // applies backpressure all the way back to disk reads when the consumer
        // (and thus the parse task) falls behind.
        let mut io_err: Option<Error> = None;
        loop {
            match self.read_next_block(cursor).await {
                Ok(Some(chunk)) => {
                    if raw_tx.send(chunk).await.is_err() {
                        // Parse task ended early (consumer dropped or parse
                        // error). Stop reading; the task's result is canonical.
                        break;
                    }
                }
                Ok(None) => break, // EOF
                Err(e) => {
                    // Tell the parse half to SKIP the terminal final drain: the
                    // stream is truncated, so the trailing window is partial and
                    // must not be emitted. The store is sequenced before the
                    // `drop(raw_tx)` the parse half synchronizes on.
                    io_failed.store(true, Ordering::SeqCst);
                    io_err = Some(e);
                    break;
                }
            }
        }
        // Drop the sender so the blocking task sees EOF and runs its final drain
        // (only if `io_failed` is still false — see the parse half).
        drop(raw_tx);

        // Join the parse task; its Result is the scan's Result. An I/O error
        // takes precedence (the task only saw a truncated stream).
        let parse_result = match parse_task.await {
            Ok(r) => r,
            Err(join_err) => Err(Error::corruption(format!(
                "run_scan_stream_windowed: parse task failed: {join_err}"
            ))),
        };
        if let Some(e) = io_err {
            return Err(e);
        }
        parse_result
    }

    /// Blocking parse half of the windowed streaming scan (issue #1143).
    ///
    /// Runs entirely on a `spawn_blocking` thread — NEVER on an async worker.
    /// Owns the sliding `window: Vec<u8>`; for each raw chunk pulled from
    /// `raw_rx` it applies the incompressible-raw fallback or decompresses,
    /// appends to the window, and drains every confirmed partition via
    /// [`drain_scan_window`]. On a CLEAN `raw_rx` close (I/O EOF) it runs a final
    /// drain with `at_final_chunk = true`; on a close caused by a mid-stream read
    /// error (`io_failed` set by the I/O half before it dropped the sender) it
    /// SKIPS that terminal drain so a truncated window cannot emit a spurious
    /// trailing partition (issue #1143 finding 2). Surviving `(RowKey, Value)`
    /// entries are sent through `tx` with `blocking_send`, mirroring the
    /// pre-#1156 `parse_stitched_stream` backpressure.
    fn drain_scan_window_blocking(
        &self,
        ctx: WindowParseCtx,
        mut raw_rx: mpsc::Receiver<Vec<u8>>,
        tx: mpsc::Sender<Result<(RowKey, Value)>>,
        io_failed: Arc<AtomicBool>,
    ) -> Result<()> {
        use crate::storage::sstable::compression::Compression;

        let parser = self.build_v5_parser();
        let mut window: Vec<u8> = Vec::new();
        let mut broke = false;
        let mut chunk_count = 0usize;

        while let Some(compressed_chunk) = raw_rx.blocking_recv() {
            let decompressed_chunk = if compressed_chunk.len() >= ctx.max_compressed_length {
                compressed_chunk
            } else if let Some(compression_reader) = &self.compression_reader {
                let compression = Compression::new(*compression_reader.algorithm())?;
                compression.decompress(&compressed_chunk).map_err(|e| {
                    Error::corruption(format!(
                        "drain_scan_window_blocking: Failed to decompress chunk {}: {}",
                        chunk_count, e
                    ))
                })?
            } else {
                compressed_chunk
            };
            window.extend_from_slice(&decompressed_chunk);
            chunk_count += 1;

            // Not the final chunk yet: drain confirmed partitions; NeedMore means
            // "await more bytes" (a partition straddles this chunk boundary).
            self.drain_scan_window(&parser, &ctx, &mut window, false, &tx, &mut broke)?;
            if broke {
                return Ok(());
            }
        }

        // Stream end. On a CLEAN EOF run the final drain — a trailing partition
        // with no END_OF_PARTITION marker is now terminal (Done), not a refill
        // request that will never come. But if the I/O half stopped on a read
        // ERROR (`io_failed`), the trailing window is a truncated fragment of a
        // partition; running `at_final_chunk = true` here would parse and emit it
        // as if complete, surfacing a partial/garbage row BEFORE the caller
        // returns the I/O `Err`. Skip the final drain so a truncated/corrupt
        // stream yields ONLY the error (issue #1143 finding 2). The store in the
        // I/O half happens-before the `drop(raw_tx)` that ended `blocking_recv`,
        // so this load observes it.
        if !broke && !io_failed.load(Ordering::SeqCst) {
            self.drain_scan_window(&parser, &ctx, &mut window, true, &tx, &mut broke)?;
        }

        // Test-only probe (issue #1143 regression guard): record the thread that
        // ran the parse so a guard test can prove it was NOT an async worker.
        probe::record_parse_thread();

        log::debug!(
            "drain_scan_window_blocking: drained {} chunks (final window {} bytes)",
            chunk_count,
            window.len()
        );
        Ok(())
    }

    /// Drain every confirmed partition from the front of the sliding `window`,
    /// emitting each surviving `(RowKey, Value)` through `tx` (issue #1143).
    ///
    /// Synchronous (runs on the `spawn_blocking` thread). Drives
    /// [`parse_one_partition_with_timestamps`], drops the per-row timestamp, and
    /// applies the same `table_ids_match` + key-range + `filter_tombstone`
    /// filters the prior driver applied. After each `Emitted(consumed)` the
    /// consumed prefix is removed, keeping the window's peak bounded by
    /// `max_partition_size + one_chunk`. Stops at `NeedMore` / `Done` (await the
    /// next chunk / genuine end) or when the consumer is dropped (`*broke`).
    fn drain_scan_window(
        &self,
        parser: &crate::storage::sstable::reader::parsing::V5CompressedLegacyParser,
        ctx: &WindowParseCtx,
        window: &mut Vec<u8>,
        at_final_chunk: bool,
        tx: &mpsc::Sender<Result<(RowKey, Value)>>,
        broke: &mut bool,
    ) -> Result<()> {
        use crate::storage::sstable::reader::parsing::ParseStep;

        loop {
            if *broke || window.is_empty() {
                return Ok(());
            }

            // Buffer this partition's surviving entries, then forward them via
            // `blocking_send` AFTER the parser returns.
            // `parse_one_partition_with_timestamps` takes a synchronous `FnMut`
            // emit, so we cannot send inside it; a partition's rows are bounded
            // by `max_partition_size`, so this stays within the window bound.
            let mut surviving: Vec<(RowKey, Value)> = Vec::new();
            let step = parser.parse_one_partition_with_timestamps(
                window.as_slice(),
                ctx.schema.as_ref(),
                self,
                at_final_chunk,
                &mut |(entry_table_id, key, value, _ts)| {
                    // Key-range + tombstone filters match the previous
                    // `parse_stitched_stream`; the `table_ids_match` guard is the
                    // ADDITIONAL filter the non-stitching `scan_stream` branch
                    // also applies (a no-op for single-table SSTables).
                    if !table_ids_match(&entry_table_id, &ctx.table_id) {
                        return Ok(std::ops::ControlFlow::Continue(()));
                    }
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
                    surviving.push((key, value));
                    Ok(std::ops::ControlFlow::Continue(()))
                },
            )?;

            match step {
                ParseStep::Emitted(consumed) => {
                    let take = if consumed == 0 { 1 } else { consumed };
                    window.drain(0..take.min(window.len()));
                    // Forward this partition's surviving entries with backpressure
                    // (blocking_send: this runs on a spawn_blocking thread).
                    for entry in surviving {
                        if tx.blocking_send(Ok(entry)).is_err() {
                            *broke = true; // consumer dropped
                            return Ok(());
                        }
                    }
                }
                // NeedMore: the partition straddles this chunk boundary. The
                // per-partition parser buffers a partition's rows internally and
                // only invokes our emit closure on a CONFIRMED `Emitted` return,
                // so on `NeedMore` our `surviving` buffer is empty — nothing was
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

/// Test-only probe (issue #1143): records the [`std::thread::ThreadId`] on which
/// the windowed scan's decompress+parse half actually ran, so a guard test can
/// deterministically prove that work executed on a `spawn_blocking` thread and
/// NOT on a tokio async worker. Disarmed by default and effectively zero cost
/// (one relaxed atomic load per scan); never observes anything in production.
#[doc(hidden)]
pub mod probe {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;
    use std::thread::ThreadId;

    static ARMED: AtomicBool = AtomicBool::new(false);
    static LAST_PARSE_THREAD: Mutex<Option<ThreadId>> = Mutex::new(None);

    /// Arm the probe and clear any previously recorded thread. Call from a test
    /// before driving a scan.
    pub fn arm() {
        if let Ok(mut g) = LAST_PARSE_THREAD.lock() {
            *g = None;
        }
        ARMED.store(true, Ordering::SeqCst);
    }

    /// Disarm the probe (restores the production no-op state).
    pub fn disarm() {
        ARMED.store(false, Ordering::SeqCst);
    }

    /// Record the current thread as the parse thread, if armed. Called from the
    /// blocking parse half after a scan's parse work completes.
    pub(super) fn record_parse_thread() {
        if ARMED.load(Ordering::Relaxed) {
            if let Ok(mut g) = LAST_PARSE_THREAD.lock() {
                *g = Some(std::thread::current().id());
            }
        }
    }

    /// The [`ThreadId`] recorded by the most recent parse, if any.
    pub fn recorded_parse_thread() -> Option<ThreadId> {
        LAST_PARSE_THREAD.lock().ok().and_then(|g| *g)
    }
}
