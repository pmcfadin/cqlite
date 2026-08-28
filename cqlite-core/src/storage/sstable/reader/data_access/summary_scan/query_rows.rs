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
//! * **A dead producer is an ERROR, never a clean end of stream (issue #3106).**
//!   Completion is EXPLICIT: the producer thread's last act is to send
//!   [`QueryRowMsg::Done`], and the consumer reports end-of-stream ONLY on that
//!   sentinel. A bare channel disconnect — which is what an UNWINDING producer
//!   leaves behind, since a panic drops the `SyncSender` without sending anything
//!   — is reported as a hard error instead of "the walk finished". Belt and
//!   braces: the thread body also runs under `catch_unwind`, so a panic is
//!   forwarded as an INFORMATIVE terminal error naming the panic message rather
//!   than a generic "the producer died". Before this, such a panic completed the
//!   request SUCCESSFULLY with a silently truncated result set.
//!
//!   The two halves cover different builds ON PURPOSE, which is why both are
//!   here: `catch_unwind` only fires under `panic = "unwind"` (dev/test, and the
//!   `release-unwind` profile the bindings ship — `[profile.release]` is
//!   `panic = "abort"`, where a panicking producer takes the process down instead,
//!   loudly). The `Done` sentinel needs no unwinding at all: it makes "the walk
//!   finished" an OBSERVED fact in every profile, so any way THIS THREAD can stop
//!   without reporting — a future exit path that forgets its terminator included —
//!   fails closed rather than being read as a complete scan.
//!
//!   SCOPE, stated precisely (roborev, issue #3106): `Done` proves only that the
//!   query-row THREAD ran to completion. It says nothing about that thread's own
//!   upstream, and on the full-ring arm the rows come from an INNER `tokio` task
//!   over a second channel — whose death this thread would report as a clean
//!   walk. That boundary is closed separately and by the same principle, in
//!   [`crate::storage::sstable::reader::BatchedScanStream`]: it owns the scan
//!   task's `JoinHandle` and joins it when its channel closes, so a task that
//!   died surfaces as an error there and reaches this thread as a normal `Err`
//!   (hence a `Failed` terminator). Both boundaries are needed for the end-to-end
//!   claim; neither alone suffices — and the claim is bounded to those two, NOT
//!   universal: on the chunk-stitching branch there is a THIRD hop, the windowed
//!   driver's batch forwarder, whose `JoinError` is deliberately discarded
//!   (`scan_stream_windowed`'s `let _ = forwarder.await;`), so a panic there is
//!   still invisible. Pre-existing and low-likelihood (the forwarder only moves
//!   already-decoded batches), tracked as a follow-up rather than fixed here.

use std::ops::ControlFlow;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::Arc;

use super::super::super::SSTableReader;
use super::super::full_index_stream::FullIndexStreamOutcome;
use super::query_rows_bounds::{
    QUERY_ROWS_CHANNEL_BATCHES, QUERY_ROWS_FULL_SCAN_BUFFER_ROWS, QUERY_ROWS_PER_BATCH,
};
use super::ScanTokenBound;
use crate::storage::producer_fault::ProducerFault;
use crate::storage::scan_cancel::ScanCancel;
use crate::storage::sstable::reader::scan_stream_windowed::scan_admission::ScanAdmission;
// Re-exported so `summary_scan::query_rows::QUERY_ROWS_MAX_READ_AHEAD` keeps
// resolving after the sizing constants moved to their own file (issue #3384).
#[allow(unused_imports)]
pub use super::query_rows_bounds::{QUERY_ROWS_MAX_READ_AHEAD, QUERY_ROWS_MAX_RESIDENT_ROWS};
use crate::types::ScanRow;
use crate::{Error, Result, RowKey};

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

/// One message on the producer→consumer channel (issue #3106).
///
/// INTERNAL: the terminator is deliberately NOT a [`QueryRowBatch`] variant, so
/// no consumer can observe (or forget to handle) it — the sentinel exists purely
/// to disambiguate "the walk finished" from "the producer died", which a bare
/// channel disconnect cannot do.
#[derive(Debug)]
enum QueryRowMsg {
    /// A batch. Carries only `Ok` BY CONSTRUCTION (issue #3106, roborev): a
    /// failure is [`QueryRowMsg::Failed`], so "this message ends the stream" is a
    /// structural property of the variant rather than an unenforced invariant
    /// about where an `Err` was built. Were a non-fatal mid-stream `Err` ever sent
    /// as an `Item`, the consumer would mark the stream terminated and a LATER
    /// genuine dead-producer disconnect would silently revert to a clean end of
    /// stream — #3106 reintroduced with no test failing.
    Item(QueryRowBatch),
    /// The walk failed. TERMINAL: the producer sends exactly one of this or
    /// [`QueryRowMsg::Done`], as its last act.
    Failed(Error),
    /// The producer finished its walk and is exiting normally. This is the ONLY
    /// thing that makes [`QueryRowStream::next_batch`] report a clean end of
    /// stream; a disconnect without it is a dead producer.
    Done,
}

/// A pull-based, single-generation query row stream (issue #3058).
///
/// Dropping it requests cancellation of the underlying walk; the producer thread
/// then observes the cancel (or a failed send into the dropped channel) and exits.
pub struct QueryRowStream {
    rx: Receiver<QueryRowMsg>,
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
    /// Whether an EXPLICIT terminator has been observed (issue #3106): either the
    /// [`QueryRowMsg::Done`] sentinel or a terminal `Err`.
    ///
    /// This is what makes a subsequent channel disconnect interpretable. Set →
    /// the producer is known to have finished/failed on purpose, so the
    /// disconnect is a clean end of stream. Unset → the sender was dropped
    /// WITHOUT a terminator, which only an unwinding (or otherwise dead)
    /// producer can do, so the stream fails closed rather than reporting a
    /// silently truncated result set as success.
    terminated: bool,
}

impl QueryRowStream {
    /// Block until the next message is available. `None` = the walk finished
    /// (clean end of stream), proven by the explicit [`QueryRowMsg::Done`]
    /// sentinel — never inferred from a channel disconnect.
    ///
    /// A disconnect WITHOUT that sentinel means the producer thread died mid-walk
    /// (issue #3106) and yields `Some(Err(..))`: the result set is truncated, and
    /// truncation must never be reported as success. A producer PANIC normally
    /// arrives as a terminal `Err` carrying the panic message (the thread body
    /// runs under `catch_unwind`), so this arm is the backstop for a producer that
    /// died in a way it could not report at all (e.g. an abort of the send itself).
    ///
    /// The blocking wait is timed into the per-request RECV-WAIT accumulator
    /// (`stream_subphase::time_recv`), exactly as the k-way merge's own recv site
    /// is (`write_engine/merge/mod.rs`). The drive loop SUBTRACTS that accumulator
    /// from its `stream_merge` bucket, so producer starvation / cold I/O on this
    /// arm is not billed as merge CPU — without this wrap the fast arm (now the
    /// DEFAULT path) would silently break #2819's "`stream_merge` is merge CPU
    /// only" contract and inflate the sub-phase profile that issue #3096 uses as
    /// its evidence base. Inert (no clock read) when no sub-phase sink is
    /// installed, and it never touches the value returned.
    pub fn next_batch(&mut self) -> Option<Result<QueryRowBatch>> {
        let received = crate::observability::stream_subphase::time_recv(|| self.rx.recv());
        match received {
            Ok(QueryRowMsg::Item(batch)) => Some(Ok(batch)),
            Ok(QueryRowMsg::Failed(e)) => {
                // Terminal by construction: a caller that keeps polling after it
                // gets a clean `None`, not a spurious dead-producer error.
                self.terminated = true;
                Some(Err(e))
            }
            Ok(QueryRowMsg::Done) => {
                self.terminated = true;
                None
            }
            Err(_disconnected) if self.terminated => None,
            Err(_disconnected) => {
                self.terminated = true;
                Some(Err(dead_producer_error()))
            }
        }
    }
}

/// The fail-closed error for a producer that disconnected without a terminator
/// (issue #3106).
///
/// [`Error::Internal`] — ONE variant for both halves of this event (here and
/// [`panicked_producer_error`]), chosen deliberately (roborev, issue #3106):
/// nothing suggests the `Data.db` is bad, so `Corruption` would send an operator
/// hunting a nonexistent bad file, and `Storage` is `is_recoverable() == true`
/// (surfaced to consumers, e.g. the Node bindings' error mapping) which would
/// advertise a deterministic internal failure as RETRYABLE. `Internal` is
/// `is_recoverable() == false` and is the honest variant for a violated internal
/// invariant.
fn dead_producer_error() -> Error {
    Error::internal(
        "query row stream: the producer thread disconnected WITHOUT its terminal \
         Done sentinel — it died mid-walk, so the result set is TRUNCATED and \
         cannot be reported as a complete scan (issue #3106)",
    )
}

/// Turn a caught producer-thread panic into an INFORMATIVE terminal error
/// (issue #3106).
///
/// The bare disconnect backstop can only say "the producer died"; catching the
/// unwind lets the client see WHERE/WHY, which is the difference between a
/// debuggable failure and a mystery. The panic payload is a `String`/`&str` for
/// every `panic!`/assertion (the only other shapes are hand-rolled
/// `panic_any`), so an unrecognized payload degrades to a named placeholder
/// rather than being dropped.
///
/// Same [`Error::Internal`] variant as [`dead_producer_error`] — one event, one
/// variant, and not the `is_recoverable() == true` `Storage` this first used.
fn panicked_producer_error(payload: &(dyn std::any::Any + Send)) -> Error {
    let message = payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&'static str>().copied())
        .unwrap_or("<non-string panic payload>");
    Error::internal(format!(
        "query row stream: the producer thread PANICKED mid-walk ({message}) — the \
         result set is TRUNCATED and cannot be reported as a complete scan \
         (issue #3106)"
    ))
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
        let (tx, rx) = sync_channel::<QueryRowMsg>(QUERY_ROWS_CHANNEL_BATCHES);
        // This stream's own flag (see `QueryRowStream::child_cancel`): the walk
        // polls the CHILD, the caller's flag is bridged into it, and nothing ever
        // cancels the caller's.
        let child_cancel = ScanCancel::new();
        let bridge = CancelBridge {
            caller: scan_cancel,
            child: child_cancel.clone(),
        };
        // Issue #3106: whatever fault a test armed FOR THIS READER is captured
        // HERE (never re-read mid-walk) and owned by the producer thread. Always
        // empty — and a zero-sized no-op whose scope closure is never called — in
        // a production build.
        let mut fault =
            crate::storage::producer_fault::ProducerFault::capture_for(|| self.file_path());
        std::thread::Builder::new()
            .name("cqlite-query-rows".to_string())
            .spawn(move || {
                let sender = tx.clone();
                // Issue #3106: an UNWINDING walk must not look like a finished
                // one. `AssertUnwindSafe` is sound here because nothing the
                // closure touched is observed after an unwind: the reader,
                // schema, parser buffers and cancel bridge are dropped, and the
                // only thing used afterwards is `sender` — an mpsc `SyncSender`,
                // which has no poisoning/broken-invariant state — to report the
                // failure. The walk's own output is already downstream.
                let outcome =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<()> {
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
                            &mut fault,
                        ))
                    }));
                // EXACTLY ONE terminal message on every exit path, so the
                // consumer never has to infer completion from a disconnect.
                let terminal = match outcome {
                    Ok(Ok(())) => QueryRowMsg::Done,
                    Ok(Err(e)) => QueryRowMsg::Failed(e),
                    Err(panic) => QueryRowMsg::Failed(panicked_producer_error(panic.as_ref())),
                };
                // CAUSAL completion signal (issue #3384), published BEFORE the
                // terminal message and never after (roborev): a consumer holding
                // the terminal message must be able to conclude this producer can
                // no longer publish, or a PRIOR case's producer could increment
                // into a LATER case's freshly-reset counter. Nothing below decodes.
                crate::storage::read_path_probe::mark_query_row_producer_finished();
                // The consumer may already be gone; a failed send is fine.
                let _ = sender.send(terminal);
            })
            .map_err(|e| {
                Error::Storage(format!("query row stream: failed to spawn thread: {e}"))
            })?;
        Ok(QueryRowStream {
            rx,
            child_cancel,
            terminated: false,
        })
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
    tx: &SyncSender<QueryRowMsg>,
    fault: &mut ProducerFault,
) -> Result<()> {
    if token_bound.is_none() {
        return drive_full_scan_rows(reader, schema, now_secs, cancel, tx, fault).await;
    }

    let scan_cancel = cancel.child();
    let mut sink = BatchSink::new(tx, cancel, fault);

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
    let _ = tx.send(QueryRowMsg::Item(QueryRowBatch::Unsupported));
    Ok(())
}

/// Hand ONE batch of rows to the consumer, returning `false` when the consumer
/// has dropped the channel.
///
/// The SINGLE batch-handoff funnel for BOTH walk arms — which is exactly why the
/// test-only producer-fault hook is consulted here (issue #3106): a fault armed
/// against this stream is observed identically on the full-ring and the
/// token-bounded arm, so neither can be fixed while the other silently truncates.
/// [`ProducerFault::before_batch_handoff`] is a no-op (and `fault` a ZST) in a
/// production build.
fn emit_rows(
    tx: &SyncSender<QueryRowMsg>,
    fault: &mut ProducerFault,
    rows: Vec<(RowKey, ScanRow)>,
) -> bool {
    fault.before_batch_handoff();
    tx.send(QueryRowMsg::Item(QueryRowBatch::Rows(rows)))
        .is_ok()
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
    tx: &SyncSender<QueryRowMsg>,
    fault: &mut ProducerFault,
) -> Result<()> {
    let table_id = reader.scan_table_id();
    let mut rx = reader.scan_stream_batched_admitted(
        table_id,
        None,
        None,
        Some(schema),
        QUERY_ROWS_FULL_SCAN_BUFFER_ROWS,
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
        if !emit_rows(tx, fault, rows) {
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
    tx: &'a SyncSender<QueryRowMsg>,
    /// The caller/child cancellation pair. The token-bounded walk polls the CHILD
    /// internally at its own cadence, but nothing there sees the CALLER's flag —
    /// so a client disconnect would otherwise only stop this walk once the
    /// consumer noticed and dropped the stream (a whole batch of a wide partition
    /// later). Bridged here, at the batch boundary (roborev, issue #3058).
    cancel: &'a CancelBridge,
    batch: Vec<(RowKey, ScanRow)>,
    emitted: u64,
    /// Test-only producer-fault state for this stream (issue #3106); a ZST with
    /// a no-op hook in a production build.
    fault: &'a mut ProducerFault,
}

impl<'a> BatchSink<'a> {
    fn new(
        tx: &'a SyncSender<QueryRowMsg>,
        cancel: &'a CancelBridge,
        fault: &'a mut ProducerFault,
    ) -> Self {
        Self {
            tx,
            cancel,
            batch: Vec::with_capacity(QUERY_ROWS_PER_BATCH),
            emitted: 0,
            fault,
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
        if emit_rows(self.tx, self.fault, batch) {
            Ok(ControlFlow::Continue(()))
        } else {
            // Consumer dropped: stop the walk (not an error).
            Ok(ControlFlow::Break(()))
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
#[path = "query_rows_tests.rs"]
mod tests;

// Issue #3106 END-TO-END pin: a real walk whose real producer thread PANICS. Needs
// the write engine to build its fixture, so it is gated on `write-support`.
#[cfg(all(test, feature = "write-support"))]
#[path = "query_rows_panic_tests.rs"]
mod panic_tests;
