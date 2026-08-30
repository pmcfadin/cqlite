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
//! ## CLOSED — a dead producer is an ERROR, never end-of-input (issue #3120)
//!
//! Neither producer-thread shape used to send an explicit terminator, and
//! `SSTableRowIteratorAdapter::next` (`producer_iter`) mapped a channel DISCONNECT
//! onto `None` = "this run is exhausted". So a producer thread that UNWOUND made
//! its run look finished, and the merge completed successfully having merged only
//! the rows that reached the channel — a silently short read result, or a short
//! rewritten SSTable on the compaction path (silent data loss at rest). The query
//! row stream's version of this defect was fixed in issue #3106; this is the same
//! treatment for the merge:
//!
//! * The channel item is [`MergeMsg`](super::producer_msg::MergeMsg), whose
//!   TERMINATORS make completion an observed fact — see that module's header for
//!   the full protocol rationale.
//! * BOTH producer thread bodies run under `catch_unwind` and send EXACTLY ONE
//!   terminal message on EVERY exit path, with the BLOCKING `SyncSender::send`
//!   (a `try_send` that dropped the terminator would recreate the ambiguity).
//! * [`forward_row`] no longer sends a row-conversion failure as a mid-walk
//!   channel message while RETURNING `Continue`. A non-terminal `Err` in the DATA
//!   slot, after which the walk keeps going, is precisely the shape that lets a
//!   later genuine dead-producer disconnect revert to a clean end-of-input. It
//!   now RETURNS the `Err` out of the emit callback and the thread body emits the
//!   single terminal `Failed`. Behaviour delta: the walk stops at the FIRST
//!   row-conversion failure instead of continuing. The outcome observed by the
//!   merge is unchanged, because `RunReader::refill_buffer` already returned that
//!   `Err` to its caller immediately.
//!
//! UDT registry: [`SSTableRowIteratorAdapter::open`] (path-based) can call
//! `reader.set_udt_registry(..)` because it just opened its OWN exclusive
//! reader. `open_from_reader` CANNOT — the reader is shared (`Arc`), so no
//! `&mut self` is available. A caller needing UDT-aware decode over a shared
//! reader must open it WITH the registry already resolved (before wrapping it
//! in `Arc`); `open_from_reader` takes no `udt_registry` parameter for this
//! reason (an accepted-but-silently-ignored parameter would be a correctness
//! trap).

use std::sync::atomic::AtomicI64;
use std::sync::mpsc::SyncSender;
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::schema::TableSchema;
use crate::storage::producer_fault::MergeProducerFault;
use crate::storage::scan_cancel::ScanCancel;
use crate::storage::sstable::reader::SSTableReader;

use super::egress_batch::EgressBatcher;
use super::producer_iter::RunState;
use super::producer_msg::{panicked_producer_error, MergeMsg, MergeProducerError};
use super::{
    egress_batch, egress_budget, producer_gauge, KWayMerger, RunReader, SSTableRowIterator,
    SSTableRowIteratorAdapter,
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
    sender: &SyncSender<MergeMsg>,
    local_sent: &AtomicI64,
    fault: &mut MergeProducerFault,
) -> Result<()> {
    let mut batcher = EgressBatcher::new(sender, local_sent);
    let walk = reader
        .stream_all_partitions_for_compaction(Some(schema), scan_cancel, |compaction_row| {
            forward_row(run_index, compaction_row, schema, &mut batcher, fault)
        })
        .await;
    finish_batched_walk(walk, &mut batcher)
}

/// Flush the pending tail of a batched walk on EVERY exit path, then report the
/// walk's own outcome (issue #2820).
///
/// LOAD-BEARING, and the one thing batching can silently get wrong: the caller
/// (a producer thread body) sends its single terminator immediately after this
/// returns, so a row still sitting in the accumulator when `Done` goes out is a
/// row the merge never sees — issue #3120's silent-short-read / short-rewritten-
/// SSTable class, reintroduced through the back door and invisible to any test
/// that only counts rows on a fixture whose size happens to be a batch multiple.
///
/// The tail is flushed on the ERROR path too, not just the clean one: the
/// terminator is queued BEHIND every data message, so the consumer still sees
/// those rows before the failure, and flushing can only ever deliver MORE of what
/// the producer already read. The walk's `Err` always wins — a flush that finds
/// the consumer gone (`Break`) is not an error, there is simply nobody left to
/// hand rows to.
fn finish_batched_walk(walk: Result<()>, batcher: &mut EgressBatcher<'_>) -> Result<()> {
    let _ = batcher.flush();
    walk
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
/// The row conversion + batched backpressure (`forward_row` into one
/// `EgressBatcher`, then [`finish_batched_walk`]) is still shared, so the emit
/// contract — including the pre-terminator tail flush — cannot diverge.
pub(super) async fn drive_query_stream(
    reader: &SSTableReader,
    run_index: usize,
    schema: &TableSchema,
    scan_cancel: &ScanCancel,
    token_bound: Option<crate::storage::sstable::reader::ScanTokenBound>,
    sender: &SyncSender<MergeMsg>,
    local_sent: &AtomicI64,
    fault: &mut MergeProducerFault,
) -> Result<()> {
    let mut batcher = EgressBatcher::new(sender, local_sent);
    let walk = reader
        .stream_all_partitions_for_query(Some(schema), scan_cancel, token_bound, |compaction_row| {
            forward_row(run_index, compaction_row, schema, &mut batcher, fault)
        })
        .await;
    finish_batched_walk(walk, &mut batcher)
}

/// Convert one streamed row into a [`MergeEntry`] and accumulate it into
/// `batcher`, signalling `Break` when the consumer has dropped the channel.
/// Shared by BOTH [`drive_compaction_stream`] and [`drive_query_stream`] so their
/// emit contract is defined in exactly one place.
///
/// Issue #2820: this used to `send` ONE channel message per ROW — measured at
/// 49.9% of single-stream CPU, ~94% of it kernel park/wake. It now hands the
/// entry to [`EgressBatcher`], which sends one message per BATCH; the caller
/// flushes the tail before its terminator (see [`finish_batched_walk`]).
///
/// A row-conversion failure is RETURNED as `Err` (issue #3120), never sent as a
/// channel message: only the thread body may put a terminator on the channel, and
/// it sends EXACTLY ONE. The pre-#3120 code sent an `Err` here and returned
/// `Continue`, i.e. a non-terminal error in the DATA slot after which the walk kept
/// going — the shape that lets a later genuine dead-producer disconnect revert to a
/// clean end-of-input. The outcome seen by the merge is unchanged: `RunReader::
/// refill_buffer` already propagated that `Err` to its caller immediately.
///
/// The egress-depth (issue #2419) and `local_sent` (roborev job 1733)
/// accounting lives in [`EgressBatcher::flush`], per successful batch send and in
/// ENTRIES — see `channel_depth`'s module doc for why the unit must stay entries
/// on both sides.
fn forward_row(
    run_index: usize,
    compaction_row: crate::storage::sstable::reader::CompactionRow,
    schema: &TableSchema,
    batcher: &mut EgressBatcher<'_>,
    fault: &mut MergeProducerFault,
) -> Result<std::ops::ControlFlow<()>> {
    // TEST-ONLY (issue #3120): an empty function in a production build. This is
    // the ONE emit funnel both stream shapes go through, and it sits ABOVE any
    // reader format branch, so a fault armed here cannot silently fail to fire
    // for a particular on-disk format.
    fault.before_row_forward();
    let entry = SSTableRowIteratorAdapter::build_merge_entry(run_index, compaction_row, schema)?;
    Ok(batcher.push(entry))
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
        // Issue #2820: `channel_capacity` is a ROW budget (`egress_budget`'s whole
        // vocabulary is rows); the channel carries BATCHES, so it is converted to a
        // MESSAGE capacity here. Passing the row budget straight through would
        // budget 256 BATCHES = 65_536 entries per source — a 256x resident-row
        // blow-up. See `egress_batch::message_capacity_for_rows`.
        let message_capacity = egress_batch::message_capacity_for_rows(channel_capacity);
        let (sender, receiver) = std::sync::mpsc::sync_channel(message_capacity);
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
        // Issue #1707: the read-PHASE sink is propagated the SAME way and for the
        // same reason. `generation_merge::stream_generations_for_read` installs it on
        // the merge/CONSUMER thread, but the chunk decode of a cross-generation read
        // happens HERE, on this per-input producer thread — so without this
        // capture/install pair the route recorded `merge` and nothing else. It reaches
        // the shared chunk-decode plane (`reader::chunk_source`), so DECOMPRESS is
        // measured through it; the io seam is a separate matter, see
        // `observability::read_phase`'s coverage boundary. `None` (no-op) for an
        // unmetered caller.
        let read_phase_sink = crate::observability::read_phase::current();
        // Issue #3120: whatever fault a test armed FOR THIS INPUT is captured HERE
        // (never re-read mid-walk) and owned by the producer thread. Always empty —
        // and a zero-sized no-op whose scope closure is never called — in a
        // production build.
        let fault = MergeProducerFault::capture_for(|| reader.file_path());
        let producer = match std::thread::Builder::new().spawn(move || {
            let _subphase_guard = crate::observability::stream_subphase::install(subphase_sink);
            let _read_phase_guard = crate::observability::read_phase::install(read_phase_sink);
            Self::producer_thread_from_reader(
                reader,
                run_index,
                schema,
                scan_cancel,
                token_bound,
                sender,
                producer_sent_count,
                fault,
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
            held: Vec::new().into_iter(),
            // Issue #3120: no terminator observed yet. `None` (end of input) is
            // reachable ONLY from a `Done`-proven `RunState::Finished`.
            state: RunState::Streaming,
            #[cfg(test)]
            egress_channel_capacity: message_capacity,
            #[cfg(test)]
            egress_rows_capacity: channel_capacity,
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
    ///
    /// Sends EXACTLY ONE terminal [`MergeMsg`] on EVERY exit path (issue #3120),
    /// so the consumer never has to infer completion from a channel disconnect —
    /// see the module header. `catch_unwind` covers the unwinding path; the
    /// `Done` terminator covers every other way this thread could stop.
    #[allow(clippy::too_many_arguments)]
    fn producer_thread_from_reader(
        reader: Arc<SSTableReader>,
        run_index: usize,
        schema: TableSchema,
        scan_cancel: ScanCancel,
        token_bound: Option<crate::storage::sstable::reader::ScanTokenBound>,
        sender: SyncSender<MergeMsg>,
        sent_count: Arc<AtomicI64>,
        mut fault: MergeProducerFault,
    ) {
        let _thread_guard = producer_gauge::ProducerThreadGuard;
        // Issue #3120: the terminal sender lives OUTSIDE the `catch_unwind`
        // closure, so an unwind cannot drop it before the terminator is sent.
        let terminal_sender = sender.clone();

        // Issue #3120: an UNWINDING walk must not look like a finished one.
        // `AssertUnwindSafe` is sound here because nothing the closure touched is
        // observed after an unwind: the reader `Arc`, schema, cancel token and
        // per-row fault state are dropped, and the only thing used afterwards is
        // `terminal_sender` — an mpsc `SyncSender`, which has no
        // poisoning/broken-invariant state. `sent_count` is an atomic whose value
        // stays meaningful (it counts sends that already succeeded), and `Drop`
        // reads it only after joining this thread.
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<()> {
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
                &mut fault,
            ))
        }));

        // EXACTLY ONE terminal message, on every exit path. `Cancelled` is
        // preserved distinctly (issue #2264) by `MergeProducerError::from`.
        let terminal = match outcome {
            Ok(Ok(())) => MergeMsg::Done,
            Ok(Err(e)) => MergeMsg::Failed(MergeProducerError::from(e)),
            Err(panic) => MergeMsg::Failed(panicked_producer_error(panic.as_ref())),
        };
        // The BLOCKING `send`, never `try_send`: dropping the terminator because
        // the bounded channel happened to be full would recreate exactly the
        // ambiguity this protocol removes. A failed send means the consumer is
        // already gone, which is fine — nobody is left to mislead.
        let _ = terminal_sender.send(terminal);
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
