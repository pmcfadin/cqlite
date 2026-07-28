//! Single-generation, token-scoped, PULL-based query ROW stream (issue #3058).
//!
//! # Why this exists
//!
//! A `SELECT` served from exactly ONE SSTable generation needs no cross-generation
//! reconciliation: read-time SELECT semantics (partition deletions, range
//! tombstones, row/cell tombstones, TTL expiry, static-cell injection) are applied
//! inside the decoder by `PartitionShadow` when the parser is built with
//! `read_shadowing = true` (issue #1741). The k-way merge exists to reconcile
//! ACROSS generations; with one generation it is pure overhead — it materialises
//! full-fidelity `CompactionRow`s with per-cell write metadata that the read path
//! then throws away.
//!
//! This module exposes the already-shipped single-generation walk as a **pull**
//! surface a synchronous consumer can drive: the Flight `do_get` row loop
//! (`cqlite-flight`) runs on a blocking thread with no async runtime of its own,
//! and the underlying walks ([`SSTableReader::stream_partitions_summary_guided`],
//! [`SSTableReader::stream_all_partitions_via_full_index`]) are async with a
//! SYNCHRONOUS emit callback. So the walk is driven on a dedicated thread owning a
//! `current_thread` runtime and hands BATCHES to the consumer over a bounded
//! `sync_channel` — the same thread/bounded-channel shape the k-way merge's own
//! per-input adapter uses (`write_engine/merge/from_readers.rs`), minus the merge:
//! one thread instead of one-per-input, and one handoff per BATCH instead of one
//! per row.
//!
//! # Guarantees this surface makes to its caller
//!
//! * **Read shadowing is ON.** Every source this module drives builds its parser
//!   with `build_v5_parser(true)` — the Summary-guided walk
//!   ([`SSTableReader::stream_partitions_summary_guided`]), the full-`Index.db`
//!   walk ([`SSTableReader::stream_all_partitions_via_full_index`]) and the
//!   windowed batched scan (`scan_stream_windowed`'s `drain_scan_window_blocking`).
//!   That posture is asserted BEHAVIOURALLY, not by reading: the Flight
//!   forced-path differential (`cqlite-flight/tests/issue_3058_forced_path_differential.rs`)
//!   shows tombstoned/expired rows suppressed identically to the reconciling
//!   merge arm, which is only possible with shadowing on.
//! * **The TTL/expiry clock is the CALLER's.** `now_secs` is pinned onto the
//!   parser ([`V5CompressedLegacyParser::with_now_secs`]), never re-sampled from
//!   the wall clock, so a request that captured ONE reconciliation instant (and a
//!   test that PINS `now`) is honored.
//! * **Token pushdown is preserved.** `token_bound` is pushed into the
//!   Summary-guided walk (#2412/#2413), so out-of-range partition bodies are never
//!   decoded.
//! * **Fail-closed on an unservable reader.** If neither walk can prove it can
//!   stream this reader (no usable `Index.db`/`Summary.db`, a BTI reader, or a
//!   coverage gap), the stream reports [`QueryRowBatch::Unsupported`] as its FIRST
//!   and ONLY message — having emitted NOTHING — so the caller can fall back to
//!   the k-way merge path with no partial output. The degenerate materialising
//!   `sequential_scan` fallback is deliberately NOT taken here: it cannot honor a
//!   caller-pinned `now`, and silently serving a scan against the wall clock would
//!   break the pinned-`now` contract above.
//! * **Cancellation and backpressure.** The walk polls this stream's CHILD
//!   [`ScanCancel`] at its normal cadence, and the CALLER's flag is bridged into
//!   that child at every batch boundary — on the full-ring arm in
//!   [`drive_full_scan_rows`] and on the token-bounded arm in `BatchSink::flush`
//!   — so a client disconnect stops either walk without waiting for the consumer
//!   to drop the stream, and a cancelled scan terminates with `Cancelled` rather
//!   than a silently short stream. Only the child is ever cancelled; the caller's
//!   flag is left intact so a fallback to the merge arm still works. Every batch
//!   send also observes the bounded channel, and a dropped consumer breaks the
//!   walk at its next send.

use std::ops::ControlFlow;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::Arc;

use super::super::super::SSTableReader;
use super::super::full_index_stream::FullIndexStreamOutcome;
use super::ScanTokenBound;
use crate::storage::scan_cancel::ScanCancel;
use crate::storage::sstable::reader::scan_stream_windowed::scan_admission::ScanAdmission;
use crate::types::ScanRow;
use crate::{Error, Result, RowKey};

/// Rows accumulated before a batch is handed to the consumer. Matches the
/// batched scan surface's emit granularity (issue #1592): one cross-thread
/// handoff per batch instead of per row.
const QUERY_ROWS_PER_BATCH: usize = 128;

/// Batches the bounded handoff channel may hold. Resident rows are therefore
/// bounded by `QUERY_ROWS_PER_BATCH * (QUERY_ROWS_CHANNEL_BATCHES + 1)` plus the
/// partition currently being decoded — independent of table size.
const QUERY_ROWS_CHANNEL_BATCHES: usize = 4;

/// One message from a [`QueryRowStream`].
#[derive(Debug)]
pub enum QueryRowBatch {
    /// A batch of decoded, read-shadowed `(RowKey, ScanRow)` rows in
    /// token/partition order.
    Rows(Vec<(RowKey, ScanRow)>),
    /// This reader cannot be served by the single-generation streaming query
    /// walk. Guaranteed to arrive BEFORE any [`QueryRowBatch::Rows`] and to be
    /// the stream's only message, so the caller may fall back to another read
    /// path having emitted nothing.
    Unsupported,
}

/// A pull-based, single-generation query row stream (issue #3058).
///
/// Dropping it requests cancellation of the underlying walk; the producer thread
/// then observes the cancel (or a failed send into the dropped channel) and exits.
pub struct QueryRowStream {
    rx: Receiver<Result<QueryRowBatch>>,
    /// This stream's OWN cancellation flag — a CHILD of the caller's, never the
    /// caller's own clone (roborev, issue #3058).
    ///
    /// `ScanCancel` clones share one `Arc<AtomicBool>`, so cancelling the
    /// caller's flag on drop would POISON the whole request: the designed clean
    /// fallback (`QueryRowBatch::Unsupported` → drop the stream → build the
    /// k-way merger instead) would hand the merger an already-cancelled flag and
    /// yield `Cancelled`/zero rows instead of the full result set, and even a
    /// successful stream would leave the request's `CancelFlag` single-use. The
    /// caller's flag is bridged INTO this child (caller-cancel stops the walk)
    /// but never the other way round.
    child_cancel: ScanCancel,
}

impl QueryRowStream {
    /// Block until the next message is available. `None` = the walk finished
    /// (clean end of stream).
    ///
    /// KNOWN GAP — issue #3106: a `recv` error also means "the sender was
    /// dropped", which a producer-thread PANIC produces without any terminal
    /// message, so an unwinding walk currently reports a clean (silently
    /// truncated) end of stream rather than an error. The channel protocol needs
    /// an explicit `Done` sentinel (or `catch_unwind` → `Err`) for a disconnect
    /// without a terminator to be treated as corruption; #3106 owns that, for
    /// this stream AND the k-way merge adapter, whose stance this matches.
    pub fn next_batch(&mut self) -> Option<Result<QueryRowBatch>> {
        self.rx.recv().ok()
    }
}

impl Drop for QueryRowStream {
    fn drop(&mut self) {
        // Stop the walk promptly rather than letting it run to completion into a
        // channel nobody reads (the producer would otherwise only notice at its
        // next send). Cancels only THIS stream's child flag — the caller's flag
        // is untouched, so a caller that falls back to another read path gets an
        // un-cancelled one. The thread is deliberately NOT joined: it holds only
        // an `Arc<SSTableReader>` + its own runtime and exits on the next cancel
        // poll, so a dropped stream never blocks the consumer.
        self.child_cancel.cancel();
    }
}

impl SSTableReader {
    /// Open a [`QueryRowStream`] over this ONE reader (issue #3058).
    ///
    /// `schema` is the authoritative table schema for decoding, `token_bound` the
    /// split's `(start, end]` range to push into the Summary-guided walk (`None`
    /// = full ring), `now_secs` the caller's request-scoped read-time TTL clock,
    /// and `scan_cancel` the caller's cooperative cancellation flag.
    ///
    /// See the module docs for the guarantees (read shadowing on, pinned clock,
    /// token pushdown, fail-closed [`QueryRowBatch::Unsupported`]).
    pub fn open_query_row_stream(
        self: Arc<Self>,
        schema: crate::schema::TableSchema,
        token_bound: Option<ScanTokenBound>,
        now_secs: i64,
        scan_cancel: ScanCancel,
    ) -> Result<QueryRowStream> {
        let (tx, rx) = sync_channel::<Result<QueryRowBatch>>(QUERY_ROWS_CHANNEL_BATCHES);
        // This stream's own flag (see `QueryRowStream::child_cancel`): the walk
        // polls the CHILD, the caller's flag is bridged into it, and nothing ever
        // cancels the caller's.
        let child_cancel = ScanCancel::new();
        let bridge = CancelBridge {
            caller: scan_cancel,
            child: child_cancel.clone(),
        };
        std::thread::Builder::new()
            .name("cqlite-query-rows".to_string())
            .spawn(move || {
                let sender = tx.clone();
                let outcome = (|| -> Result<()> {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(|e| {
                            Error::Storage(format!(
                                "query row stream: failed to create runtime: {e}"
                            ))
                        })?;
                    rt.block_on(drive_query_rows(
                        self,
                        schema,
                        token_bound,
                        now_secs,
                        &bridge,
                        &tx,
                    ))
                })();
                if let Err(e) = outcome {
                    // Forward the terminal error (consumer may already be gone).
                    let _ = sender.send(Err(e));
                }
            })
            .map_err(|e| {
                Error::Storage(format!("query row stream: failed to spawn thread: {e}"))
            })?;
        Ok(QueryRowStream { rx, child_cancel })
    }

    /// Whether this reader has the components the single-generation streaming
    /// query walk needs (an `Index.db`, not a BTI `Partitions.db`).
    ///
    /// AUTHORITATIVE reader metadata (issue #28): the presence of parsed
    /// components, never a guess from file names, sizes, or byte content. It is a
    /// NECESSARY condition, not a sufficient one — a walk may still report
    /// [`QueryRowBatch::Unsupported`] when it cannot prove partition coverage, which
    /// is why that signal exists.
    pub fn supports_streaming_query_scan(&self) -> bool {
        self.index_reader.is_some() && self.bti_partitions_db.is_none()
    }

    /// The STATIC columns this SSTable's OWN serialization header declares
    /// (`Statistics.db`'s `SerializationHeader.staticColumns`) — authoritative
    /// ON-DISK metadata, independent of any caller-supplied schema.
    ///
    /// Exists because a caller schema is not a safe source for the static
    /// question (roborev, issue #3058): the Flight producer's schema comes from
    /// the ticket DDL, and an `nb` header carries no embedded schema to
    /// cross-check it against (#3097), so a DDL that predates an
    /// `ALTER TABLE ADD … STATIC` (or a hand-built ticket) would declare no
    /// static column for an SSTable that actually contains one. The
    /// single-generation read path emits NOTHING for a partition holding only a
    /// static row, where the merge arm emits one row, so a routing decision made
    /// on the stale DDL alone would change the row count.
    ///
    /// The serialization header enumerates EVERY static column the file can
    /// contain, so this is at least as strong as inspecting each decoded row's
    /// `EXTENDED_IS_STATIC` flag, and it is available BEFORE the first row (so a
    /// caller can fall back cleanly instead of aborting mid-stream).
    ///
    /// An empty result means "this file declares no static column" when the
    /// header was parsed. A file whose `Statistics.db` is absent/unparsed also
    /// yields an empty list — see [`Self::static_columns_are_known`], which a
    /// caller MUST consult to distinguish the two.
    pub fn on_disk_static_columns(&self) -> Vec<String> {
        let Some(stats) = self.statistics_reader.as_ref() else {
            return Vec::new();
        };
        stats
            .statistics()
            .serialization_header_columns
            .iter()
            .filter(|c| c.is_static)
            .map(|c| c.name.clone())
            .collect()
    }

    /// Whether [`Self::on_disk_static_columns`] is an ANSWER rather than an
    /// absence of information: `true` when the serialization header was parsed
    /// (so "no static columns" is authoritative), `false` when there is nothing
    /// to read (no `Statistics.db`, or a header with no columns at all), in which
    /// case a caller that needs the static question settled MUST fail closed.
    pub fn static_columns_are_known(&self) -> bool {
        self.statistics_reader
            .as_ref()
            .is_some_and(|s| !s.statistics().serialization_header_columns.is_empty())
    }
}

/// Drive the single-generation walk, batching rows into `tx`.
///
/// # Source selection (issue #3058)
///
/// Both sources are single-generation, read-shadowed and clock-pinned; they
/// differ in which cost they avoid, and the choice is made from the request's
/// own authoritative shape — the presence of a token bound — never from a guess:
///
/// * **No token bound (a full-ring scan).** The WINDOWED batched scan
///   (`scan_stream_batched_admitted`) — the exact path the bare `SELECT`
///   full scan uses. It walks the data section directly, so it pays NO
///   `partition_slice_fully_consumed` structural re-decode per partition and
///   builds ZERO per-row `CellWriteMetadata` maps.
/// * **A token bound (a Trino split).** The Summary-guided walk, so the split's
///   range is pushed INTO the per-SSTable walk (#2412/#2413) and out-of-range
///   partition bodies are never read. Its per-partition coverage check
///   (`partition_slice_fully_consumed`, Signal B) drives the framing in
///   STRUCTURE-ONLY mode (`parse_one_partition_structure_only`), so it builds no
///   `CompactionRow`, no per-row `CellWriteMetadata` map and no complex-element
///   map — this arm allocates ZERO metadata maps, exactly like the full-ring one,
///   and that is pinned by `cell_metadata_maps == 0` in
///   `cqlite-flight/tests/issue_3058_bypass_path_taken.rs`
///   (`token_pruning_to_one_source_still_selects_the_fast_path`) plus every
///   `assert_arms_agree` case's bypass leg. (Before that fix the check decoded
///   through the compaction parser and DID build one map per row; the merge arm's
///   own coverage check got the same saving, since those rows were always
///   discarded.)
///
/// On a pre-emit `FellBack` from the Summary-guided walk the full-`Index.db`
/// streaming walk is tried (full ring; the caller's downstream token filter
/// still bounds the result set); on a second pre-emit `FellBack`,
/// [`QueryRowBatch::Unsupported`] is reported and NOTHING has been emitted.
async fn drive_query_rows(
    reader: Arc<SSTableReader>,
    schema: crate::schema::TableSchema,
    token_bound: Option<ScanTokenBound>,
    now_secs: i64,
    cancel: &CancelBridge,
    tx: &SyncSender<Result<QueryRowBatch>>,
) -> Result<()> {
    if token_bound.is_none() {
        return drive_full_scan_rows(reader, schema, now_secs, cancel, tx).await;
    }

    let scan_cancel = cancel.child();
    let mut sink = BatchSink::new(tx, cancel);

    // Each walk gets its OWN short-lived emit closure so the `&mut sink` borrow
    // ends with the call — that is what lets the pre-emit guard below actually
    // READ `sink.emitted` between the two walks (roborev, issue #3058).
    let outcome = reader
        .stream_partitions_summary_guided(
            scan_cancel,
            token_bound,
            Some(now_secs),
            Some(&schema),
            &mut |row: (RowKey, ScanRow)| sink.push(row),
        )
        .await?;
    if matches!(outcome, FullIndexStreamOutcome::Streamed) {
        sink.finish()?;
        // A caller cancellation that broke the walk must surface as `Cancelled`,
        // never as a clean (silently short) end of stream.
        return cancel.caller_result();
    }

    // No usable Summary.db: fall back to the full-`Index.db` streaming walk. Both
    // walks CONTRACT to report `FellBack` only BEFORE their first emit — ENFORCED
    // here, not assumed (roborev, issue #3058): re-driving a second walk into a
    // sink that already holds rows (an under-full, still-buffered batch counts)
    // would flush both walks' output and silently DUPLICATE rows. Fail closed.
    assert_nothing_emitted(sink.emitted, "before the full-index fallback walk")?;
    let outcome = reader
        .stream_all_partitions_via_full_index(
            scan_cancel,
            Some(now_secs),
            Some(&schema),
            &mut |row: (RowKey, ScanRow)| sink.push(row),
        )
        .await?;
    if matches!(outcome, FullIndexStreamOutcome::Streamed) {
        sink.finish()?;
        return cancel.caller_result();
    }

    // Neither walk can serve this reader. Report it as the FIRST and ONLY
    // message, having emitted nothing — enforced, for the same reason.
    assert_nothing_emitted(sink.emitted, "before reporting Unsupported")?;
    let _ = tx.send(Ok(QueryRowBatch::Unsupported));
    Ok(())
}

/// Fail closed when a walk reported `FellBack` AFTER handing rows to the sink.
///
/// The fallback design rests on "`FellBack` is pre-emit only"; if that ever
/// stopped holding, continuing would emit some rows twice (once from the walk
/// that fell back, once from the fallback walk) with no error anywhere. A
/// corruption error is the only safe outcome — a partially-served stream must
/// never be silently topped up.
fn assert_nothing_emitted(emitted: u64, stage: &str) -> Result<()> {
    if emitted > 0 {
        return Err(Error::corruption(format!(
            "query row stream: a walk reported FellBack AFTER emitting {emitted} row(s) \
             ({stage}) — continuing would duplicate rows (issue #3058)"
        )));
    }
    Ok(())
}

/// The stream's cancellation pair: the caller's flag (observed, NEVER cancelled)
/// and this stream's own child flag (what the walks poll and what `Drop`
/// cancels). See [`QueryRowStream::child_cancel`].
struct CancelBridge {
    caller: ScanCancel,
    child: ScanCancel,
}

impl CancelBridge {
    /// The flag to hand the walks.
    fn child(&self) -> &ScanCancel {
        &self.child
    }

    /// Propagate a CALLER cancellation into the child (one-way), returning
    /// whether the scan should stop. Polled at batch boundaries, so a cancelled
    /// request stops the walk without waiting for the consumer to drop us.
    fn poll_caller(&self) -> bool {
        if self.caller.is_cancelled() {
            self.child.cancel();
            return true;
        }
        false
    }

    /// `Err(Cancelled)` when the CALLER cancelled, else `Ok(())` — so a walk that
    /// was broken by a cancellation terminates the stream with a cancellation
    /// rather than a clean, silently truncated end of stream.
    fn caller_result(&self) -> Result<()> {
        if self.caller.is_cancelled() {
            return Err(Error::Cancelled);
        }
        Ok(())
    }
}

/// Full-ring arm: forward the windowed batched scan's `(RowKey, ScanRow)` batches
/// straight through, re-using the batching the scan already did.
///
/// # Admission (issue #1594 / #2420, roborev #3058)
///
/// Opened [`ScanAdmission::Exempt`] — this scan does NOT take a core admission
/// permit. Three reasons, all load-bearing:
/// * The CALLER is already admitted. The Flight `do_get` this serves holds a
///   `--max-concurrent-scans` permit for its whole life (#2420), and the core
///   semaphore's default cap is `available_parallelism()` (≈ncpu), which is
///   SMALLER than that governor's default of 64 — acquiring here would silently
///   throttle single-source `do_get`s below the operator's configured
///   concurrency.
/// * The arm it replaces takes none. The k-way merge arm drives its inputs on
///   plain `std::thread`s and never admits, so admitting here would make the
///   fast path *less* concurrent than the slow path it replaces.
/// * The resource the core semaphore protects is not contended here. This scan's
///   `spawn_blocking` parse/feed tasks run on the query-row thread's OWN
///   `current_thread` runtime (and therefore its own blocking pool), not the
///   shared runtime pool whose starvation #1594 exists to prevent.
///
/// `Exempt` never blocks, so it cannot introduce the #1594 hold-and-wait cycle.
async fn drive_full_scan_rows(
    reader: Arc<SSTableReader>,
    schema: crate::schema::TableSchema,
    now_secs: i64,
    cancel: &CancelBridge,
    tx: &SyncSender<Result<QueryRowBatch>>,
) -> Result<()> {
    let table_id = reader.scan_table_id();
    let mut rx = reader.scan_stream_batched_admitted(
        table_id,
        None,
        None,
        Some(schema),
        QUERY_ROWS_PER_BATCH * QUERY_ROWS_CHANNEL_BATCHES,
        ScanAdmission::Exempt,
        Some(now_secs),
    );
    while let Some(msg) = rx.recv().await {
        // Cooperative cancellation: the caller dropping the stream also drops our
        // receiver (so the send below fails), but polling here stops a cancelled
        // scan without waiting for the next batch to be produced. A CALLER cancel
        // is bridged into the child flag; the caller's own flag is never touched.
        if cancel.poll_caller() {
            return Err(Error::Cancelled);
        }
        cancel.child().check()?;
        let rows = msg?;
        if tx.send(Ok(QueryRowBatch::Rows(rows))).is_err() {
            // Consumer dropped: stop pulling (not an error).
            return Ok(());
        }
    }
    Ok(())
}

/// Accumulates emitted rows into `QUERY_ROWS_PER_BATCH`-sized batches and pushes
/// them through the bounded channel, translating a dropped consumer — or a
/// CALLER cancellation — into `ControlFlow::Break` so the walk stops instead of
/// running to completion.
struct BatchSink<'a> {
    tx: &'a SyncSender<Result<QueryRowBatch>>,
    /// The caller/child cancellation pair. The token-bounded walk polls the CHILD
    /// internally at its own cadence, but nothing there sees the CALLER's flag —
    /// so a client disconnect would otherwise only stop this walk once the
    /// consumer noticed and dropped the stream (a whole batch of a wide partition
    /// later). Bridged here, at the batch boundary (roborev, issue #3058).
    cancel: &'a CancelBridge,
    batch: Vec<(RowKey, ScanRow)>,
    emitted: u64,
}

impl<'a> BatchSink<'a> {
    fn new(tx: &'a SyncSender<Result<QueryRowBatch>>, cancel: &'a CancelBridge) -> Self {
        Self {
            tx,
            cancel,
            batch: Vec::with_capacity(QUERY_ROWS_PER_BATCH),
            emitted: 0,
        }
    }

    fn push(&mut self, row: (RowKey, ScanRow)) -> Result<ControlFlow<()>> {
        self.batch.push(row);
        self.emitted += 1;
        if self.batch.len() >= QUERY_ROWS_PER_BATCH {
            return self.flush();
        }
        Ok(ControlFlow::Continue(()))
    }

    fn flush(&mut self) -> Result<ControlFlow<()>> {
        // Bridge a CALLER cancellation into the child (one-way — the caller's own
        // flag is never cancelled, which is what keeps the merge-arm fallback
        // usable) and stop the walk here rather than a batch later.
        if self.cancel.poll_caller() {
            return Ok(ControlFlow::Break(()));
        }
        if self.batch.is_empty() {
            return Ok(ControlFlow::Continue(()));
        }
        let batch = std::mem::take(&mut self.batch);
        self.batch.reserve(QUERY_ROWS_PER_BATCH);
        match self.tx.send(Ok(QueryRowBatch::Rows(batch))) {
            Ok(()) => Ok(ControlFlow::Continue(())),
            // Consumer dropped: stop the walk (not an error).
            Err(_) => Ok(ControlFlow::Break(())),
        }
    }

    fn finish(&mut self) -> Result<()> {
        // A `Break` here means the consumer went away with a partial tail
        // pending — a clean end of stream, not an error.
        let _ = self.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Roborev (issue #3058): the "`FellBack` is pre-emit only" contract is
    /// ENFORCED, not assumed. Rows already handed to the sink (including an
    /// under-full, still-buffered batch) must turn a second walk into a hard
    /// corruption error rather than a silently duplicated result set.
    #[test]
    fn a_post_emit_fallback_fails_closed_instead_of_duplicating_rows() {
        assert!(
            assert_nothing_emitted(0, "before the full-index fallback walk").is_ok(),
            "the pre-emit case is the normal fallback and must proceed"
        );
        let err = assert_nothing_emitted(1, "before the full-index fallback walk")
            .expect_err("a post-emit fallback must fail closed");
        let msg = err.to_string();
        assert!(
            msg.contains("FellBack AFTER emitting"),
            "the error must name the violated contract, got: {msg}"
        );
        assert!(
            assert_nothing_emitted(127, "before reporting Unsupported").is_err(),
            "a partially-filled batch (< QUERY_ROWS_PER_BATCH) still counts as emitted"
        );
    }

    /// Roborev (issue #3058): a CALLER cancellation must reach the TOKEN-BOUNDED
    /// walk too, at a batch boundary — not only once the consumer notices and
    /// drops the stream (which on a wide partition is a whole batch later). The
    /// caller's own flag must still be left un-cancelled (only the child is ever
    /// cancelled), or the merge-arm fallback would be poisoned again.
    #[test]
    fn a_caller_cancellation_breaks_the_token_bound_sink_at_a_batch_boundary() {
        let caller = ScanCancel::new();
        let child = ScanCancel::new();
        let bridge = CancelBridge {
            caller: caller.clone(),
            child: child.clone(),
        };
        let (tx, rx) = sync_channel::<Result<QueryRowBatch>>(QUERY_ROWS_CHANNEL_BATCHES);
        let mut sink = BatchSink::new(&tx, &bridge);
        let row = || (RowKey::new(vec![1]), ScanRow::Row(Vec::new()));

        // A full batch with no cancellation flows through.
        for _ in 0..QUERY_ROWS_PER_BATCH {
            assert!(matches!(
                sink.push(row()).expect("push"),
                ControlFlow::Continue(())
            ));
        }
        assert!(
            matches!(rx.try_recv(), Ok(Ok(QueryRowBatch::Rows(b))) if b.len() == QUERY_ROWS_PER_BATCH),
            "the first batch was handed to the consumer"
        );

        // Now the caller cancels. The next batch boundary must BREAK the walk.
        caller.cancel();
        let mut broke = false;
        for _ in 0..QUERY_ROWS_PER_BATCH {
            if matches!(sink.push(row()).expect("push"), ControlFlow::Break(())) {
                broke = true;
                break;
            }
        }
        assert!(
            broke,
            "a caller cancellation must stop the token-bounded walk at the batch \
             boundary, not only when the consumer drops the stream"
        );
        assert!(
            child.is_cancelled(),
            "the cancellation is propagated into the child so the walk's own poll \
             aborts it promptly too"
        );
        assert!(
            matches!(bridge.caller_result(), Err(Error::Cancelled)),
            "a cancelled scan terminates with Cancelled, never a clean short stream"
        );
    }

    /// The bridge is ONE-WAY: a caller cancellation stops the walk (via the
    /// child), but nothing this stream does may cancel the caller's flag — the
    /// fallback to the k-way merge arm depends on getting it back un-cancelled.
    #[test]
    fn the_cancel_bridge_is_one_way() {
        let caller = ScanCancel::new();
        let bridge = CancelBridge {
            caller: caller.clone(),
            child: ScanCancel::new(),
        };
        assert!(!bridge.poll_caller(), "no cancellation yet");
        bridge.child().cancel();
        assert!(
            !caller.is_cancelled(),
            "cancelling the CHILD must never reach the caller's flag"
        );
        assert!(!bridge.poll_caller(), "still no caller cancellation");

        let caller2 = ScanCancel::new();
        let bridge2 = CancelBridge {
            caller: caller2.clone(),
            child: ScanCancel::new(),
        };
        caller2.cancel();
        assert!(
            bridge2.poll_caller(),
            "a caller cancellation stops the scan"
        );
        assert!(
            bridge2.child().is_cancelled(),
            "and is propagated into the child so the walk aborts promptly"
        );
    }
}
