//! Warm/shared-reader k-way merge construction (issue #2346).
//!
//! The path-based [`KWayMerger::new`]/[`KWayMerger::new_cancellable`] family
//! (`mod.rs`) opens a fresh [`SSTableReader`] per input INSIDE its own detached
//! producer thread — necessary for the compaction/write-engine callers, whose
//! inputs may be deleted once the merge (and thus every producer thread) has
//! finished (issue #591). A cached-reader caller — the intended consumer is a
//! future Flight warm-handle registry (epic #2310) — instead wants to hand the
//! merger ALREADY-OPEN, possibly-SHARED `Arc<SSTableReader>`s it keeps parsed
//! across requests, paying the reader-open + Index/Summary/Statistics/bloom
//! parse cost once per SSTable generation instead of once per request.
//!
//! [`KWayMerger::new_from_readers`] is that seam. It reuses every other piece
//! of the k-way merge (heap, reconciliation, LWW tie-break by run index)
//! byte-identically — only WHO opens/owns the `SSTableReader` differs from the
//! path-based constructors.
//!
//! ## Delegation (no behavioural drift between the two producer shapes)
//!
//! [`drive_compaction_stream`] is the single streaming-emit helper BOTH producer
//! thread shapes call: the path-based `SSTableRowIteratorAdapter::producer_thread`
//! (private to `producer_iter`; unchanged opening/threading behaviour — still one fresh reader
//! opened per thread, in parallel, exactly as before this issue) and the new
//! [`SSTableRowIteratorAdapter::open_from_reader`]'s producer thread (this file,
//! never opens a reader). Factoring the conversion/backpressure/
//! cancellation-by-variant logic into one function means the two shapes cannot
//! silently diverge.
//!
//! `KWayMerger::new_with_gc_and_registry_cancellable` (the path-based
//! constructor) is intentionally NOT rewritten to open readers eagerly and then
//! call [`KWayMerger::new_from_readers`] — that would move every input's open
//! from N-parallel-producer-threads to one serial pass on the calling thread,
//! a real latency regression for a multi-SSTable merge with no benefit (the
//! path-based caller never has a reader to share in the first place). The
//! shared code is the STREAMING logic, not the opening timing.
//!
//! ## File-lifetime / open-config contract for caller-supplied readers
//!
//! The path-based adapter forces `use_mmap = false` + `DiskAccessMode::Buffered`
//! specifically because compaction inputs may be deleted by
//! `finalize_merge_async` once every producer thread has finished (issue #591) —
//! a reader opened any other way could hold a dangling mapping. A reader passed
//! into [`SSTableRowIteratorAdapter::open_from_reader`] is opened by the CALLER
//! (not by this seam), so that safety property is the CALLER's responsibility:
//! the backing `Data.db` MUST NOT be deleted while any `Arc<SSTableReader>`
//! clone (including ones held by other concurrent runs, or by a cache) is still
//! alive. This is a materially different contract from the path-based adapter's
//! self-contained guarantee, so it is called out explicitly rather than
//! silently inherited. A read-only warm-handle cache that never deletes/replaces
//! a generation's `Data.db` out from under a live `Arc` (evicting only its OWN
//! reference, per #1749's fail-closed model) satisfies this trivially.
//!
//! ## KNOWN GAP — a dead producer reads as end-of-input (issue #3120)
//!
//! Neither producer-thread shape sends an explicit terminator, and
//! `SSTableRowIteratorAdapter::next` (`producer_iter`) maps a channel DISCONNECT onto
//! `None` = "this run is exhausted". So a producer thread that UNWINDS makes its
//! run look finished, and the merge completes successfully having merged only the
//! rows that reached the channel — a silently short read result, or a short
//! rewritten SSTable on the compaction path. The query row stream's version of
//! this defect was fixed in issue #3106 (explicit `Done` sentinel +
//! `catch_unwind` forwarding the panic as a terminal error); the same treatment
//! here is issue #3120, held separately because it also changes compaction
//! semantics and needs the byte-parity write suite as its evidence base.
//!
//! UDT registry: [`SSTableRowIteratorAdapter::open`] (path-based) can call
//! `reader.set_udt_registry(..)` because it just opened its OWN exclusive
//! reader. `open_from_reader` CANNOT — the reader is shared (`Arc`), so no
//! `&mut self` is available. A caller needing UDT-aware decode over a shared
//! reader must open it WITH the registry already resolved (before wrapping it
//! in `Arc`); `open_from_reader` takes no `udt_registry` parameter for this
//! reason (an accepted-but-silently-ignored parameter would be a correctness
//! trap).

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::mpsc::SyncSender;
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::schema::TableSchema;
use crate::storage::scan_cancel::ScanCancel;
use crate::storage::sstable::reader::SSTableReader;

use super::{
    egress_budget, producer_gauge, KWayMerger, MergeEntry, MergeProducerError, RunReader,
    SSTableRowIterator, SSTableRowIteratorAdapter,
};

/// Drive `reader`'s compaction stream into `sender`, converting each row via
/// [`SSTableRowIteratorAdapter::build_merge_entry`].
///
/// Shared by BOTH producer-thread shapes (see the module doc): the path-based
/// adapter (which opens its own reader per thread) and the shared-reader
/// adapter this module adds. `scan_cancel` is the PER-CALL token
/// [`SSTableReader::stream_all_partitions_for_compaction`] now takes (issue
/// #2346) — never a field mutated onto `reader`, so two concurrent calls over
/// the SAME shared reader (two different producer threads, each with its own
/// token) cancel independently.
pub(super) async fn drive_compaction_stream(
    reader: &SSTableReader,
    run_index: usize,
    schema: &TableSchema,
    scan_cancel: &ScanCancel,
    sender: &SyncSender<std::result::Result<MergeEntry, MergeProducerError>>,
    local_sent: &AtomicI64,
) -> Result<()> {
    reader
        .stream_all_partitions_for_compaction(Some(schema), scan_cancel, |compaction_row| {
            forward_row(run_index, compaction_row, schema, sender, local_sent)
        })
        .await
}

/// Warm query-serve sibling of [`drive_compaction_stream`] (issue #2412 §C /
/// #2413 Option A): drives the reader's Summary-guided query stream
/// ([`SSTableReader::stream_all_partitions_for_query`]) with an optional token
/// pushdown, instead of the full-ring compaction stream.
///
/// A SEPARATE helper (not a flag on `drive_compaction_stream`) precisely because
/// the two must NOT drift toward each other: the path-based/compaction thread
/// (`mod.rs`) keeps its byte-parity full-ring materialising walk unchanged, while
/// ONLY the warm reader-based producer takes the streaming + token-scoped path.
/// The per-row conversion/backpressure (`forward_row`) is still shared, so the
/// emit contract cannot diverge.
pub(super) async fn drive_query_stream(
    reader: &SSTableReader,
    run_index: usize,
    schema: &TableSchema,
    scan_cancel: &ScanCancel,
    token_bound: Option<crate::storage::sstable::reader::ScanTokenBound>,
    sender: &SyncSender<std::result::Result<MergeEntry, MergeProducerError>>,
    local_sent: &AtomicI64,
) -> Result<()> {
    reader
        .stream_all_partitions_for_query(Some(schema), scan_cancel, token_bound, |compaction_row| {
            forward_row(run_index, compaction_row, schema, sender, local_sent)
        })
        .await
}

/// Convert one streamed row into a [`MergeEntry`] and push it into `sender`,
/// signalling `Break` when the consumer has dropped the channel. Shared by BOTH
/// [`drive_compaction_stream`] and [`drive_query_stream`] so their emit
/// contract is defined in exactly one place.
///
/// `local_sent` is THIS adapter's own sent-count (issue #2419 roborev job
/// 1733), incremented alongside the shared `channel_depth::sent()` gauge — it
/// is what lets `Drop` compute an exact post-join reconcile residual instead of
/// racing a pre-drop drain against a concurrently-sending producer.
fn forward_row(
    run_index: usize,
    compaction_row: crate::storage::sstable::reader::CompactionRow,
    schema: &TableSchema,
    sender: &SyncSender<std::result::Result<MergeEntry, MergeProducerError>>,
    local_sent: &AtomicI64,
) -> Result<std::ops::ControlFlow<()>> {
    let msg = SSTableRowIteratorAdapter::build_merge_entry(run_index, compaction_row, schema)
        .map_err(MergeProducerError::from);
    // Issue #2419 (WS2): only DATA entries are tracked on the egress-depth gauge
    // (a terminal `Err` message is untracked on both send and receive, so it
    // never unbalances the level). Captured BEFORE `send` moves `msg`.
    let is_data = msg.is_ok();
    match sender.send(msg) {
        Ok(()) => {
            if is_data {
                // A DATA entry now occupies a channel slot; balanced by exactly
                // one `channel_depth::received()` at the consumer's recv site (or
                // by the post-join reconcile in `Drop`) — see `channel_depth`.
                super::channel_depth::sent();
                local_sent.fetch_add(1, Ordering::SeqCst);
            }
            Ok(std::ops::ControlFlow::Continue(()))
        }
        Err(_) => Ok(std::ops::ControlFlow::Break(())),
    }
}

impl SSTableRowIteratorAdapter {
    /// Open a streaming run over an ALREADY-OPEN, possibly-SHARED
    /// [`SSTableReader`] (issue #2346), instead of opening a fresh reader from a
    /// path ([`SSTableRowIteratorAdapter::open`]).
    ///
    /// Still spawns a dedicated producer thread (preserving the O(M)
    /// thread-per-input / bounded-channel backpressure architecture — issues
    /// #827/#2316) but never opens/owns a reader itself; it drives the
    /// caller-supplied `Arc<SSTableReader>` directly via
    /// [`drive_compaction_stream`]. See the module doc for the file-lifetime
    /// and UDT-registry contract differences from the path-based `open`.
    ///
    /// `channel_capacity` (issue #2765) is the merge-scoped adaptive egress
    /// capacity snapshotted ONCE by the `KWayMerger` constructor and shared by
    /// every source channel of that merge.
    pub(crate) fn open_from_reader(
        reader: Arc<SSTableReader>,
        run_index: usize,
        schema: &TableSchema,
        scan_cancel: ScanCancel,
        token_bound: Option<crate::storage::sstable::reader::ScanTokenBound>,
        channel_capacity: usize,
    ) -> Result<Self> {
        let schema = schema.clone();
        // Held on the adapter for cancel-aware recv + Drop teardown (issue #2361).
        let adapter_cancel = scan_cancel.clone();
        let (sender, receiver) = std::sync::mpsc::sync_channel(channel_capacity);
        // Issue #2419 roborev job 1733: this adapter's own sent-count, shared
        // with the producer thread — see `SSTableRowIteratorAdapter`'s field doc.
        let sent_count = Arc::new(AtomicI64::new(0));
        let producer_sent_count = sent_count.clone();

        // Issue #2316: account this producer on the live-thread gauge BEFORE
        // spawning (see `SSTableRowIteratorAdapter::open`'s identical rationale).
        producer_gauge::spawned();

        // Issue #2819: thread-locals are NOT inherited across a spawn, so the
        // flight per-request sub-phase sink is propagated EXPLICITLY — capture it
        // on the CALLING (merge consumer) thread here and re-install it at the top
        // of the producer thread below, so this scan's page-in/decompress (which
        // run synchronously ON the producer thread — `stream_all_partitions_for_query`
        // → `compressed_offset.rs` / `compaction.rs`) reach the request's
        // accumulator. A deeper `spawn_blocking` feed thread (the windowed-scan
        // page-in) is NOT reached by this single-hop propagation and is not
        // covered. `None` (no-op) for every non-flight caller.
        let subphase_sink = crate::observability::stream_subphase::current();
        let producer = match std::thread::Builder::new().spawn(move || {
            let _subphase_guard = crate::observability::stream_subphase::install(subphase_sink);
            Self::producer_thread_from_reader(
                reader,
                run_index,
                schema,
                scan_cancel,
                token_bound,
                sender,
                producer_sent_count,
            );
        }) {
            Ok(handle) => handle,
            Err(e) => {
                producer_gauge::rollback();
                return Err(Error::Storage(format!(
                    "streaming producer (shared reader): failed to spawn thread: {}",
                    e
                )));
            }
        };

        Ok(Self {
            receiver: Some(receiver),
            producer: Some(producer),
            scan_cancel: adapter_cancel,
            sent_count,
            received_count: 0,
            #[cfg(test)]
            egress_channel_capacity: channel_capacity,
        })
    }

    /// Body of the shared-reader producer thread (issue #2346).
    ///
    /// Unlike `Self::producer_thread` (path-based, private to `producer_iter`),
    /// this NEVER opens a
    /// reader — it drives the caller-supplied `Arc<SSTableReader>` directly via
    /// [`drive_compaction_stream`], reusing the exact
    /// conversion/backpressure/cancellation-by-variant semantics. Still owns a
    /// dedicated `current_thread` runtime (issue #2316: zero extra worker
    /// threads) so the async `stream_all_partitions_for_compaction` call can
    /// run without a nested-runtime panic.
    fn producer_thread_from_reader(
        reader: Arc<SSTableReader>,
        run_index: usize,
        schema: TableSchema,
        scan_cancel: ScanCancel,
        token_bound: Option<crate::storage::sstable::reader::ScanTokenBound>,
        sender: SyncSender<std::result::Result<MergeEntry, MergeProducerError>>,
        sent_count: Arc<AtomicI64>,
    ) {
        let _thread_guard = producer_gauge::ProducerThreadGuard;
        let error_sender = sender.clone();

        let stream_result = (|| -> Result<()> {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| {
                    Error::Storage(format!(
                        "streaming producer (shared reader): failed to create runtime: {}",
                        e
                    ))
                })?;
            rt.block_on(drive_query_stream(
                &reader,
                run_index,
                &schema,
                &scan_cancel,
                token_bound,
                &sender,
                sent_count.as_ref(),
            ))
        })();

        if let Err(e) = stream_result {
            // Forward the error (preserving `Cancelled` distinctly, issue #2264);
            // ignore send failure (consumer may have dropped).
            let _ = error_sender.send(Err(MergeProducerError::from(e)));
        }
        // Channel closed naturally when `sender` is dropped here.
    }
}

impl KWayMerger {
    /// Build a k-way merger over already-open, possibly-SHARED `SSTableReader`s
    /// (issue #2346): a warm-handle cache can hand this constructor `Arc`
    /// clones of readers it keeps parsed across requests, instead of the
    /// per-request path-based open ([`KWayMerger::new_cancellable`]).
    ///
    /// `readers` must be ordered newest-to-oldest generation (run index = LWW
    /// tie-break rank), exactly as the path-based constructors' `input_paths`
    /// are. Reconciliation is byte-identical to the path-based merge — only WHO
    /// opens/owns the `SSTableReader` differs.
    ///
    /// `token_bound` (issue #2412 §C / #2413 Option A): when `Some`, each reader's
    /// Summary-guided walk is scoped to the split's `(start, end]` token range so
    /// out-of-range partition bodies are never read; `None` walks the full ring.
    /// Compaction never uses this seam, so it keeps full-ring parity walks.
    ///
    /// UDT-registry guard (issue #2346, WS1 #2345): this seam takes NO
    /// `udt_registry` parameter (see the module doc — a shared `Arc` reader has
    /// no `&mut self` for `set_udt_registry`). Each caller-supplied reader MUST
    /// therefore be opened WITH its UDT registry already resolved BEFORE it is
    /// wrapped in `Arc`. Wiring that resolution into the warm-handle registry
    /// caller is WS1 #2345's responsibility — if a warm caller opens a
    /// UDT-bearing table's reader WITHOUT the registry, frozen/nested UDT cells
    /// silently decode as `Blob` (the #1234 data-loss class), NOT an error. The
    /// merge layer cannot detect this here; #2345 owns that end-to-end guarantee.
    pub fn new_from_readers(
        readers: Vec<Arc<SSTableReader>>,
        schema: &TableSchema,
        scan_cancel: ScanCancel,
        token_bound: Option<crate::storage::sstable::reader::ScanTokenBound>,
    ) -> Result<Self> {
        if readers.is_empty() {
            return Err(Error::InvalidInput(
                "K-way merge requires at least one input reader".to_string(),
            ));
        }
        schema.validate_dropped_columns()?;
        // Issue #3058: explicit "the k-way merge arm was taken" marker (see
        // `storage::read_path_probe`), recorded once per constructed merger.
        crate::storage::read_path_probe::record_merger_built();

        // Issue #2765: register this k-way merge ONCE and snapshot the adaptive
        // per-channel egress capacity shared by every source channel below.
        let (channel_capacity, egress_slot) = egress_budget::begin_merge();

        let mut runs = Vec::with_capacity(readers.len());
        for (run_index, reader) in readers.into_iter().enumerate() {
            let adapter = SSTableRowIteratorAdapter::open_from_reader(
                reader,
                run_index,
                schema,
                scan_cancel.clone(),
                token_bound,
                channel_capacity,
            )?;
            runs.push(RunReader::new(
                Box::new(adapter) as Box<dyn SSTableRowIterator>
            ));
        }

        Ok(Self {
            runs,
            heap: std::collections::BinaryHeap::new(),
            current_partition: None,
            schema: schema.clone(),
            // Issue #1668, stage 5c-i: see the field doc in `mod.rs`.
            schema_arc: Arc::new(schema.clone()),
            gc_before_secs: None,
            now_secs: None,
            purge_safe: false,
            max_purgeable_timestamp: None,
            _egress_slot: Some(egress_slot),
        })
    }
}

// Issue #2346 parity tests, in a `*_tests.rs` sibling so this file stays under
// the ~800-line campsite target (epic #1116 / #1135) — see that file's header.
#[cfg(test)]
#[path = "from_readers_tests.rs"]
mod tests;
