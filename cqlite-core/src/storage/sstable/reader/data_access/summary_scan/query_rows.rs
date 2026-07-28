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
//! * **Read shadowing is ON.** Both walks build their parser with
//!   `build_v5_parser(true)` — asserted by `read_shadowing_is_enabled_on_the_query_walk`
//!   in the sibling tests, not assumed.
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
//! * **Cancellation and backpressure.** The walk polls the caller's [`ScanCancel`]
//!   at its normal cadence, and every batch send observes the bounded channel; a
//!   consumer that drops the stream breaks the walk at its next send.

use std::ops::ControlFlow;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::Arc;

use super::ScanTokenBound;
use crate::storage::scan_cancel::ScanCancel;
use super::super::full_index_stream::FullIndexStreamOutcome;
use super::super::super::SSTableReader;
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
    cancel: ScanCancel,
}

impl QueryRowStream {
    /// Block until the next message is available. `None` = the walk finished
    /// (clean end of stream).
    pub fn next_batch(&mut self) -> Option<Result<QueryRowBatch>> {
        self.rx.recv().ok()
    }
}

impl Drop for QueryRowStream {
    fn drop(&mut self) {
        // Stop the walk promptly rather than letting it run to completion into a
        // channel nobody reads (the producer would otherwise only notice at its
        // next send). The thread is deliberately NOT joined: it holds only an
        // `Arc<SSTableReader>` + its own runtime and exits on the next cancel
        // poll, so a dropped stream never blocks the consumer.
        self.cancel.cancel();
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
        let thread_cancel = scan_cancel.clone();
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
                        &self,
                        &schema,
                        token_bound,
                        now_secs,
                        &thread_cancel,
                        &tx,
                    ))
                })();
                if let Err(e) = outcome {
                    // Forward the terminal error (consumer may already be gone).
                    let _ = sender.send(Err(e));
                }
            })
            .map_err(|e| Error::Storage(format!("query row stream: failed to spawn thread: {e}")))?;
        Ok(QueryRowStream { rx, cancel: scan_cancel })
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
}

/// Drive the single-generation walk, batching rows into `tx`.
///
/// Routing: Summary-guided + token-scoped first (#2412/#2413, no out-of-range
/// partition body is read); on a pre-emit `FellBack`, the full-`Index.db`
/// streaming walk (full ring — the caller's downstream token filter still bounds
/// the result); on a second pre-emit `FellBack`, [`QueryRowBatch::Unsupported`].
async fn drive_query_rows(
    reader: &SSTableReader,
    schema: &crate::schema::TableSchema,
    token_bound: Option<ScanTokenBound>,
    now_secs: i64,
    scan_cancel: &ScanCancel,
    tx: &SyncSender<Result<QueryRowBatch>>,
) -> Result<()> {
    let mut sink = BatchSink::new(tx);
    let mut emit = |row: (RowKey, ScanRow)| sink.push(row);

    let outcome = reader
        .stream_partitions_summary_guided(
            scan_cancel,
            token_bound,
            Some(now_secs),
            Some(schema),
            &mut emit,
        )
        .await?;
    if matches!(outcome, FullIndexStreamOutcome::Streamed) {
        return sink.finish();
    }

    // No usable Summary.db: fall back to the full-`Index.db` streaming walk. Both
    // walks report `FellBack` only BEFORE their first emit, so nothing has been
    // handed to the consumer at this point.
    let outcome = reader
        .stream_all_partitions_via_full_index(scan_cancel, Some(now_secs), Some(schema), &mut emit)
        .await?;
    if matches!(outcome, FullIndexStreamOutcome::Streamed) {
        return sink.finish();
    }

    // Neither walk can serve this reader. Report it as the FIRST and ONLY
    // message, having emitted nothing.
    let _ = tx.send(Ok(QueryRowBatch::Unsupported));
    Ok(())
}

/// Accumulates emitted rows into `QUERY_ROWS_PER_BATCH`-sized batches and pushes
/// them through the bounded channel, translating a dropped consumer into
/// `ControlFlow::Break` so the walk stops instead of running to completion.
struct BatchSink<'a> {
    tx: &'a SyncSender<Result<QueryRowBatch>>,
    batch: Vec<(RowKey, ScanRow)>,
    emitted: u64,
}

impl<'a> BatchSink<'a> {
    fn new(tx: &'a SyncSender<Result<QueryRowBatch>>) -> Self {
        Self {
            tx,
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
