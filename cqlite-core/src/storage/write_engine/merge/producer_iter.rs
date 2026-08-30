//! Streaming producer-thread row iterator for the k-way merge.
//!
//! PURE CODE MOTION out of `merge/mod.rs` (issue #3139): that file is ~13.5k
//! lines, far over the ~800-line source campsite target (epic #1116). Nothing
//! here changed behaviourally — the moved items are byte-identical to their
//! pre-move form apart from `use` paths and the `pub(super)` visibility the
//! move requires.
//!
//! This module owns the PATH-BASED streaming producer shape:
//! - [`SSTableRowIteratorAdapter`] — the bounded-channel adapter that turns an
//!   async `SSTableReader` into a sync [`SSTableRowIterator`],
//! - its constructor + producer-thread body (`open` / `producer_thread`),
//! - the consumer side ([`SSTableRowIterator`] impl) and the cancel-aware
//!   join-on-drop teardown ([`Drop`] impl).
//!
//! Siblings: the reader→[`MergeEntry`] conversion helpers live in
//! [`producer_iter_convert`](super::producer_iter_convert); the SHARED-READER
//! producer shape (`open_from_reader` / `producer_thread_from_reader`, which
//! adds its own inherent `impl` onto this adapter) lives in
//! [`from_readers`](super::from_readers); the CHANNEL PROTOCOL both producer
//! shapes speak lives in [`producer_msg`](super::producer_msg); the egress-depth
//! gauge accounting this module only *calls* lives in
//! [`channel_depth`](super::channel_depth).
//!
//! Issue #3120 CLOSED the defect this module's header used to warn about (a
//! producer thread that UNWINDS being indistinguishable from an exhausted run):
//! completion is now an explicit [`MergeMsg::Done`] terminator and this adapter's
//! [`RunState`] makes "receiver/verdict gone, so assume exhausted" unrepresentable.
//! See [`producer_msg`](super::producer_msg)'s header for the protocol and
//! [`from_readers`](super::from_readers)'s for the emit-side contract.

use super::producer_msg::{
    dead_producer_error, panicked_producer_error, MergeMsg, MergeProducerError,
};
use super::{channel_depth, from_readers, producer_gauge, MergeEntry, SSTableRowIterator};
use crate::error::{Error, Result};
use crate::schema::TableSchema;
use crate::storage::producer_fault::MergeProducerFault;
use std::path::{Path, PathBuf};

/// Whether this adapter's producer run has been PROVEN finished, proven failed,
/// or is of unknown standing (issue #3120).
///
/// The point of the type is that a verdict can never be DOWNGRADED to "clean end
/// of input" by accident. Before this issue there were two silent routes to
/// `None`: a channel disconnect (which an unwinding producer is
/// indistinguishable from) and a `self.receiver.as_ref()?`. Both are now folded
/// into this state, and `None` is reachable ONLY from [`RunState::Finished`].
#[cfg(feature = "write-support")]
pub(super) enum RunState {
    /// No terminator observed yet: the producer may still send.
    Streaming,
    /// The producer sent [`MergeMsg::Done`] — the ONLY proof of a clean,
    /// COMPLETE run, and therefore the only state from which
    /// [`SSTableRowIterator::next`] may report end of input.
    Finished,
    /// The producer sent a terminal [`MergeMsg::Failed`], already reported once.
    ///
    /// STICKY, and it keeps the payload so a repeat poll re-reports the IDENTICAL
    /// error instead of degrading to `None`. That matters because
    /// `RunReader::refill_buffer` propagates our `Err` but keeps NO sticky error
    /// of its own: a consumer that swallowed the error and advanced again would
    /// otherwise see a dead producer downgraded to a clean end of input — issue
    /// #3120 reintroduced inside its own fix, with no test failing.
    Failed(MergeProducerError),
    /// STICKY: the channel disconnected with NO terminator, or the receiver was
    /// gone when a poll arrived. The verdict is "unknown, therefore TRUNCATED",
    /// so every subsequent poll re-reports the error and never `None`.
    ///
    /// Deliberately NOT #3106's non-sticky `terminated` flag, which returns
    /// `None` on the SECOND poll.
    Died,
}

/// Adapter that wraps async SSTableReader into a true-streaming sync
/// [`SSTableRowIterator`].
///
/// ## Design (Issue #754 — remove 128MB buffer cap residue of #447)
///
/// The V5CompressedLegacy format requires chunk stitching: a partition may
/// straddle compression-chunk boundaries, so the decoder needs a contiguous
/// view spanning at least one whole partition. The reader's streaming path
/// keeps only a **sliding window** of that view — one chunk plus the partition
/// currently being decoded — rather than the whole decompressed file.
///
/// A background thread (the producer) opens the SSTable with its own Tokio
/// runtime and calls
/// [`stream_all_partitions_for_compaction`](crate::storage::sstable::reader::SSTableReader::stream_all_partitions_for_compaction),
/// which decompresses one chunk at a time, drains every fully-decoded partition
/// out of the window, and forwards each entry one at a time into a bounded
/// `sync_channel`. The channel capacity is up to [`STREAMING_CHANNEL_CAPACITY`](super::STREAMING_CHANNEL_CAPACITY)
/// entries, adaptively reduced under concurrent merges (see [`egress_budget`](super::egress_budget));
/// once the channel is full the producer blocks until the consumer (the main
/// merge thread) pulls the next entry.
///
/// The bounded window plus the bounded channel together make end-to-end peak
/// memory independent of total input size: a source's decompressed content is
/// never fully resident. Peak is roughly `max_partition_size + one_chunk +
/// channel_capacity` per source (issue #827).
///
/// ## Issue #591 safety (mmap vs file deletion)
///
/// `finalize_merge_async` deletes the input SSTable files once the merged output
/// is published. We require that no mmap outlives its backing file. The producer
/// thread opens the reader with `use_mmap = false`, and the thread *fully reads
/// all file data* (the stitching phase) before it can block on a channel send.
/// By the time `finalize_merge_async` runs, the merge is complete and all
/// channel entries have been consumed, so the producer thread has long since
/// finished and dropped its file handle. No mmap ever exists.
///
/// ## Issue #587 safety (async-from-sync bridge)
///
/// The producer thread creates its own Tokio runtime (never `Handle::block_on`),
/// so it cannot panic even when called from within an existing Tokio runtime.
/// This is the same strategy as [`block_on_async`](super::block_on_async).
///
/// ## Issue #2316 thread budget (O(M), no per-producer worker pool)
///
/// That per-producer runtime is a **`current_thread`** runtime, NOT a
/// multi-threaded `Runtime::new()`: it drives the producer's single sequential
/// scan (no internal `tokio::spawn`) on the producer thread itself, adding ZERO
/// worker threads. A merge over `M` inputs therefore costs `O(M)` OS threads, not
/// `M + M·num_cpus` — killing the context-switch storm under concurrent `do_get`.
#[cfg(feature = "write-support")]
pub(super) struct SSTableRowIteratorAdapter {
    /// Receiving end of the bounded channel fed by the producer thread. `Option`
    /// so [`Drop`] can drop it FIRST (issue #2361) — closing the channel wakes a
    /// producer blocked on a full `SyncSender::send` (its send returns `Err`), so
    /// the subsequent producer join is bounded and cannot deadlock.
    pub(super) receiver: Option<std::sync::mpsc::Receiver<MergeMsg>>,
    /// Producer thread handle. `Option` so [`Drop`] can `take()` it and JOIN the
    /// thread (issue #2361) rather than detach it — the pre-#2361 `_producer`
    /// field's "joined on drop" doc was WRONG (dropping a `JoinHandle` detaches
    /// the thread, leaving it to run to completion in the background; a cancelled
    /// `do_get` over a 1M-partition scan would keep burning CPU/IO). Joining after
    /// dropping the receiver + tripping `scan_cancel` guarantees prompt teardown.
    pub(super) producer: Option<std::thread::JoinHandle<()>>,
    /// The PER-CALL cancellation token this adapter's producer scan polls (issues
    /// #2264/#2346). Held so (a) [`Self::next`] can make its blocking channel
    /// `recv` cancel-aware and (b) [`Drop`] can trip it, waking a producer that is
    /// mid-scan (not blocked on send) so it exits promptly before the join.
    pub(super) scan_cancel: crate::storage::scan_cancel::ScanCancel,
    /// This adapter's OWN count of DATA entries its producer thread has
    /// successfully sent into the bounded channel (issue #2419 roborev job
    /// 1733), shared with the producer thread via `Arc` so `Drop` can read it.
    /// It only becomes STABLE — no further increments possible — once the
    /// producer thread has been joined (see `Drop`). Paired with
    /// [`Self::received_count`] to compute the exact post-join egress-depth
    /// reconcile residual: reading it BEFORE the join (as the pre-fix drain loop
    /// did) races a producer send that can slip in after the drain's last
    /// `Empty` and before the receiver is actually dropped, permanently leaking
    /// the shared `cqlite.merge.egress_channel_depth` gauge upward across
    /// repeated cancellations on a long-running server.
    pub(super) sent_count: std::sync::Arc<std::sync::atomic::AtomicI64>,
    /// This adapter's own count of DATA entries actually received/consumed,
    /// incremented alongside every [`channel_depth::received`] call this adapter
    /// makes ([`Self::next`]'s normal consumption). Only ever touched by the
    /// thread holding `&mut self` (never shared), so a plain `i64`.
    pub(super) received_count: i64,
    /// This run's verdict (issue #3120) — see [`RunState`]. The single place a
    /// "this run is exhausted" answer may come from.
    pub(super) state: RunState,
    /// Test-only (issue #2765): the exact `sync_channel` capacity this adapter's
    /// egress channel was built with — the merge-scoped adaptive snapshot the
    /// constructor threaded in. Observed via [`SSTableRowIterator::egress_channel_capacity`]
    /// so a wiring test proves the budget reaches BOTH construction sites.
    #[cfg(test)]
    pub(super) egress_channel_capacity: usize,
}

/// Poll interval for the cancel-aware blocking `recv` in
/// [`SSTableRowIteratorAdapter::next`] (issue #2361). A bounded wait so a
/// cancellation that lands while the consumer is blocked waiting for the next
/// producer entry is observed within one interval, without a busy-spin. This is
/// an INTERNAL cadence only — never asserted against wall-clock in tests.
#[cfg(feature = "write-support")]
const RECV_CANCEL_POLL: std::time::Duration = std::time::Duration::from_millis(50);

#[cfg(feature = "write-support")]
impl SSTableRowIteratorAdapter {
    /// Open an SSTable and start a streaming producer thread.
    ///
    /// Returns immediately; the producer thread runs concurrently and populates
    /// the channel as the consumer advances. The file handle is held only by
    /// the producer thread and is dropped when the thread finishes.
    ///
    /// Uses [`SSTableReader::iterate_all_partitions_for_compaction`] which
    /// returns actual per-row timestamps decoded from the on-disk row headers.
    /// This allows the k-way merger to perform timestamp-accurate last-write-wins
    /// ordering, which is essential for tombstone shadowing (Issue #505).
    ///
    /// When the schema has clustering columns, their values are extracted from
    /// the decoded cells (in the producer thread, by column name in schema order)
    /// and stored on `MergeEntry.clustering_key` so `merge_partition_rows` groups
    /// and reconciles distinct clustering rows correctly. The clustering columns
    /// are left in the cells as well, since the read-back path expects them there.
    ///
    /// `channel_capacity` (issue #2765) is the merge-scoped adaptive egress
    /// capacity snapshotted ONCE by the `KWayMerger` constructor and shared by
    /// every source channel of that merge — never derived per adapter here.
    pub(super) fn open(
        path: &Path,
        run_index: usize,
        schema: &TableSchema,
        udt_registry: Option<crate::schema::UdtRegistry>,
        scan_cancel: crate::storage::scan_cancel::ScanCancel,
        channel_capacity: usize,
        // Issue #1704: WHO counts a failed reopen. Stated by the caller because only
        // it knows whether its own seam records — see `merge::constructors`.
        reporting: crate::storage::sstable::reader::OpenErrorReporting,
    ) -> Result<Self> {
        let path_buf = path.to_path_buf();
        let schema = schema.clone();
        // Held on the adapter for cancel-aware recv + Drop teardown (issue #2361).
        let adapter_cancel = scan_cancel.clone();

        let (sender, receiver) = std::sync::mpsc::sync_channel(channel_capacity);
        // Issue #2419 roborev job 1733: this adapter's own sent-count, shared
        // with the producer thread so `Drop` can read its post-join-stable value
        // for the egress-depth reconcile (see the field doc).
        let sent_count = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0));
        let producer_sent_count = sent_count.clone();

        // Issue #2316: account this producer on the `cqlite.merge.producer_threads`
        // gauge BEFORE spawning, so the increment happens-before any possible
        // decrement. The decrementing `ProducerThreadGuard` is created first thing
        // inside the child thread (see `producer_thread`); a fast-exiting producer
        // (e.g. a reader-open error) could otherwise race its own guard's drop
        // against a post-spawn increment on the parent, transiently underflowing
        // the live count. Incrementing here first makes the pairing
        // correct-by-construction — no ordering race is possible.
        producer_gauge::spawned();

        // Issue #2819: propagate the sub-phase sink onto this path-based producer
        // thread too (thread-locals are not inherited across a spawn); `None`
        // (no-op) for non-flight callers. NOTE (roborev L2): this is a DEFENSIVE /
        // latent propagation, NOT a parity-covered do_get path — production
        // `do_get` is warm-only (`spawn_streaming_from_readers` → `open_from_reader`,
        // which has the tested propagation). The path-based `open`
        // (`MergeInput::Paths`) is the test-only byte-identity oracle, so its
        // cold_fault/decompress attribution has no e2e assertion (and would fire
        // only for a stitching/BTI fixture via the `read_next_block` loop). Kept so
        // a future path-based flight caller is correct by construction, not to
        // imply current parity coverage. (This file is far over the #1116 campsite
        // target; the +2 lines are the minimal propagation.)
        let subphase_sink = crate::observability::stream_subphase::current();
        // Issue #1707: the read-PHASE sink, propagated the SAME way and for the same
        // reason — and on THIS path it is exercised in production, unlike the
        // sub-phase sink above: `generation_merge::stream_generations_for_read`
        // reaches `KWayMerger::new` → `MergeInput::Paths` → here, so the chunk decode
        // of every cross-generation read runs on this thread. Installing the sink on
        // the merge/consumer thread alone left that route recording `merge` and
        // nothing else. It reaches the shared chunk-decode plane
        // (`reader::chunk_source`), so DECOMPRESS is measured through it; the io seam
        // lives only in the windowed scan's read helpers and is therefore still NOT
        // reached from here — see `observability::read_phase`'s coverage boundary.
        // `None` (no-op) for an unmetered caller.
        let read_phase_sink = crate::observability::read_phase::current();

        // Issue #3120: whatever fault a test armed FOR THIS INPUT is captured HERE
        // (never re-read mid-walk) and owned by the producer thread. Always empty —
        // and a zero-sized no-op whose scope closure is never called — in a
        // production build.
        let fault = MergeProducerFault::capture_for(|| path.to_path_buf());

        // Spawn the producer thread via `Builder::spawn` (rather than the
        // panic-on-failure `std::thread::spawn`) so an OS thread-creation failure
        // is a recoverable `Err`, not a process abort: the gauge increment above
        // must never leak for a thread that never actually started, so roll it
        // back on this path via `producer_gauge::rollback`. The thread owns a
        // fresh single-threaded (current_thread) Tokio runtime so it never
        // collides with any runtime on the calling thread (Issue #587) and adds no
        // worker threads beyond itself (Issue #2316).
        let producer = match std::thread::Builder::new().spawn(move || {
            let _subphase_guard = crate::observability::stream_subphase::install(subphase_sink);
            let _read_phase_guard = crate::observability::read_phase::install(read_phase_sink);
            Self::producer_thread(
                path_buf,
                run_index,
                schema,
                udt_registry,
                scan_cancel,
                sender,
                producer_sent_count,
                fault,
                reporting,
            );
        }) {
            Ok(handle) => handle,
            Err(e) => {
                producer_gauge::rollback();
                return Err(Error::Storage(format!(
                    "streaming producer: failed to spawn thread: {}",
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
            // Issue #3120: no terminator observed yet. `None` (end of input) is
            // reachable ONLY from a `Done`-proven `RunState::Finished`.
            state: RunState::Streaming,
            #[cfg(test)]
            egress_channel_capacity: channel_capacity,
        })
    }

    /// Body of the producer thread.
    ///
    /// Opens the SSTable with buffered I/O (Issue #591), then **streams** the
    /// source one partition at a time via
    /// [`stream_all_partitions_for_compaction`](crate::storage::sstable::reader::SSTableReader::stream_all_partitions_for_compaction),
    /// converting each entry to a [`MergeEntry`] (populating the clustering key
    /// from the decoded cells when the schema has clustering columns) and
    /// sending it through the bounded channel immediately (issue #827). The
    /// blocking `SyncSender::send` provides the backpressure that — together
    /// with the reader's sliding-window stitch+parse — keeps peak memory bounded
    /// by `max_partition_size + one_chunk + channel_capacity`, independent of
    /// the total source size.
    ///
    /// Sends EXACTLY ONE terminal [`MergeMsg`] on EVERY exit path (issue #3120) —
    /// `Done` on a completed walk, `Failed` on an error return, `Failed(Panicked)`
    /// on an unwind caught here — so the consumer never has to infer completion
    /// from a channel disconnect. See [`producer_msg`](super::producer_msg)'s
    /// header for why both the `catch_unwind` and the `Done` sentinel are needed.
    #[allow(clippy::too_many_arguments)]
    fn producer_thread(
        path_buf: PathBuf,
        run_index: usize,
        schema: TableSchema,
        udt_registry: Option<crate::schema::UdtRegistry>,
        scan_cancel: crate::storage::scan_cancel::ScanCancel,
        sender: std::sync::mpsc::SyncSender<MergeMsg>,
        sent_count: std::sync::Arc<std::sync::atomic::AtomicI64>,
        mut fault: MergeProducerFault,
        reporting: crate::storage::sstable::reader::OpenErrorReporting,
    ) {
        // Issue #2316: decrement the live producer-thread gauge when this thread
        // exits (even on panic). Created FIRST so the spawn-time increment in
        // `open` is always balanced exactly once.
        let _thread_guard = producer_gauge::ProducerThreadGuard;

        // Drive the streaming read on an owned Tokio runtime (Issue #587): the
        // producer owns its single-purpose runtime, so the blocking
        // `SyncSender::send` inside the emit callback never stalls a shared
        // runtime, and there is no nested `block_on` / `Handle::current`. The
        // runtime is `current_thread` (Issue #2316), so it adds no worker threads.
        // Buffered I/O (Issue #591): the file must not be memory-mapped OR read
        // via direct I/O because finalize_merge_async may delete it after the
        // merge completes. The default disk-access mode is `Auto`, which would
        // otherwise map (or direct-read) inputs above the size thresholds, so we
        // force `Buffered` explicitly here — clearing `use_mmap` alone is no
        // longer sufficient now that `Auto` ignores that legacy flag.
        // Clone the sender for the TERMINAL message: the streaming closure moves
        // one clone for per-entry sends, leaving this one to report the single
        // terminator. Issue #3120: it lives OUTSIDE the `catch_unwind` closure, so
        // an unwind cannot drop it before the terminator is sent.
        let terminal_sender = sender.clone();
        // Reborrowed into the closure (and on into the `async move` block) so the
        // per-row fault checkpoint is reached without the closure taking ownership.
        let fault = &mut fault;
        // Issue #3120: an UNWINDING walk must not look like a finished one.
        // `AssertUnwindSafe` is sound here because nothing the closure touched is
        // observed after an unwind: the reader, config, schema, cancel token and
        // fault state are dropped, and the only thing used afterwards is
        // `terminal_sender` — an mpsc `SyncSender`, which has no
        // poisoning/broken-invariant state. `sent_count` is an atomic whose value
        // stays meaningful (it counts sends that already succeeded) and `Drop`
        // reads it only after joining this thread.
        let stream_result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || -> Result<()> {
                use crate::config::DiskAccessMode;
                use crate::platform::Platform;
                use crate::Config;
                use std::sync::Arc;

                let mut config = Config::default();
                config.storage.use_mmap = false;
                config.storage.disk_access_mode = DiskAccessMode::Buffered;

                // Issue #2316: a `current_thread` runtime drives the scan ON this
                // producer thread with ZERO extra workers. A multi-threaded
                // `Runtime::new()` would spin up `num_cpus` workers per producer
                // (~M·num_cpus threads/merge) that the single sequential scan never
                // uses — pure overhead. This bounds the per-merge thread cost to O(M).
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| {
                        Error::Storage(format!(
                            "streaming producer: failed to create runtime: {}",
                            e
                        ))
                    })?;

                rt.block_on(async move {
                    let platform = Arc::new(Platform::new(&config).await?);
                    // `open_with_reporting`, NOT the self-reporting default (issue #1704): this reopen is an
                    // INNER STEP. A failure here surfaces mid-stream at `step()` and
                    // is counted ONCE by the enclosing operation's seam — the measured
                    // `JoinedStream` on the cross-generation streaming read path, or
                    // `record_result("compaction", ..)` in `maintenance_step` on the
                    // write path. `open`, which records its own failure, made one
                    // failed reopen report TWO increments, under two different
                    // categories (measured: `io` from the raw EACCES plus `storage`
                    // from the merge's rewrap).
                    let mut reader =
                        crate::storage::sstable::reader::SSTableReader::open_with_reporting(
                            &path_buf, &config, platform, reporting,
                        )
                        .await?;

                    // Issue #1234: wire the authoritative UDT registry onto the reader
                    // so the compaction read path decodes a top-level `frozen<UDT>`
                    // value structurally. Without it the value decode errors
                    // (`UDT not found in registry`), the row reconciles to empty, and
                    // the partition is dropped — silent data loss during compaction.
                    // This mutation requires `&mut self`, so it can only happen HERE
                    // — before the reader is ever shared — not on the shared-reader
                    // producer path (issue #2346, see `from_readers::open_from_reader`'s
                    // doc comment for that seam's UDT-registry contract).
                    if let Some(registry) = udt_registry {
                        reader.set_udt_registry(registry);
                    }

                    // Issue #2264/#2346: the cooperative-cancellation token is now a
                    // PER-CALL parameter to `stream_all_partitions_for_compaction`
                    // rather than mutated onto the reader (`set_scan_cancel` needed
                    // `&mut self`, which a SHARED reader cannot offer — see
                    // `from_readers::drive_compaction_stream`). This path-based
                    // producer still owns its freshly-opened `reader` exclusively, so
                    // passing `scan_cancel` by reference here is behaviourally
                    // identical to the old `set_scan_cancel` + field-read.
                    //
                    // Pass the schema so the parser uses the real clustering column
                    // names; the header-inferred fallback uses generic names like
                    // "clustering_key", which would defeat extract_clustering_key.
                    // `drive_compaction_stream` is the SAME streaming helper the
                    // shared-reader producer thread uses (issue #2346) — the
                    // conversion/backpressure/cancellation-by-variant semantics are
                    // defined in exactly one place.
                    from_readers::drive_compaction_stream(
                        &reader,
                        run_index,
                        &schema,
                        &scan_cancel,
                        &sender,
                        sent_count.as_ref(),
                        fault,
                    )
                    .await
                })
            }));

        // EXACTLY ONE terminal message, on every exit path (issue #3120).
        // `Cancelled` is preserved distinctly (issue #2264) by
        // `MergeProducerError::from`.
        let terminal = match stream_result {
            Ok(Ok(())) => MergeMsg::Done,
            Ok(Err(e)) => MergeMsg::Failed(MergeProducerError::from(e)),
            Err(panic) => MergeMsg::Failed(panicked_producer_error(panic.as_ref())),
        };
        // The BLOCKING `send`, never `try_send`: dropping the terminator because
        // the bounded channel happened to be full would recreate exactly the
        // ambiguity this protocol removes. A failed send means the consumer is
        // already gone, which is fine — nobody is left to mislead.
        let _ = terminal_sender.send(terminal);
        // Channel closed naturally when sender is dropped here.
    }
}

#[cfg(feature = "write-support")]
impl SSTableRowIterator for SSTableRowIteratorAdapter {
    /// Pull the next entry of this run.
    ///
    /// `None` means EXHAUSTED, and issue #3120 made that answer reachable from
    /// exactly ONE place: a [`RunState::Finished`] proven by the producer's
    /// [`MergeMsg::Done`] terminator. Every other way this adapter can stop —
    /// a bare channel disconnect, a torn-down receiver, a terminal failure — is
    /// an `Err`, because a run whose standing is unknown is a TRUNCATED run, and
    /// merging (or REWRITING) a truncated run as if it were complete is silent
    /// data loss.
    fn next(&mut self) -> Option<Result<MergeEntry>> {
        use std::sync::mpsc::RecvTimeoutError;
        // A terminal state is STICKY: re-poll it as many times as a consumer
        // likes, and it never decays into a clean end of input. `RunReader::
        // refill_buffer` propagates our `Err` but keeps no sticky error of its
        // own, so this is where the stickiness has to live (issue #3120).
        match &self.state {
            RunState::Finished => return None,
            RunState::Failed(e) => return Some(Err(e.to_error())),
            RunState::Died => return Some(Err(dead_producer_error())),
            RunState::Streaming => {}
        }
        // The receiver is torn down ONLY by `Drop`, so a live poll that finds it
        // gone has NO verdict at all. Pre-#3120 this was a `?`, i.e. a second
        // silent route to "exhausted"; fail closed instead.
        let receiver = match self.receiver.as_ref() {
            Some(receiver) => receiver,
            None => {
                self.state = RunState::Died;
                return Some(Err(dead_producer_error()));
            }
        };
        // Cancel-aware blocking recv (issue #2361): a plain `recv()` blocks
        // indefinitely inside `KWayMerger::step`, so a `do_get` cancellation that
        // lands while the consumer is waiting for the next producer entry was NOT
        // observed until an entry (or channel close) arrived. Poll `scan_cancel`
        // at a bounded interval so `drive_merge` sees `Error::Cancelled` promptly
        // even while blocked here.
        loop {
            // A CANCELLED scan must never be reported as a dead producer (issue
            // #3120): that is this fix's likeliest false positive. The check stays
            // at the TOP of the loop so it wins over a queued terminator, and
            // `ScanCancel` is set-once, so a repeat poll keeps answering
            // `Cancelled` — no state transition is needed or wanted here (the run
            // is abandoned, not truncated-and-misreported).
            if self.scan_cancel.is_cancelled() {
                return Some(Err(Error::Cancelled));
            }
            match receiver.recv_timeout(RECV_CANCEL_POLL) {
                Ok(MergeMsg::Item(entry)) => {
                    // Issue #2419 (WS2): this DATA entry just left the bounded
                    // egress channel — decrement the live occupancy gauge,
                    // balancing the `channel_depth::sent()` at its send site
                    // (`from_readers::forward_row`). `received_count` is this
                    // adapter's OWN mirror of that decrement (roborev job 1733),
                    // read by `Drop` post-join to compute the exact reconcile
                    // residual — see the field doc.
                    //
                    // Issue #3120 — the receive-side half of "a TERMINATOR is
                    // untracked on both sides". Note what enforces it HERE: this
                    // arm does NOT call `MergeMsg::is_tracked_data` (that is the
                    // send site's compile-time tripwire). It is correct because
                    // (a) the `match` it belongs to is EXHAUSTIVE with no wildcard
                    // arm, so a future 4th variant is a compile error rather than
                    // a silent catch-all, and (b) `channel_depth::received`,
                    // `received_count` and `add_merge_run_entry_decoded` each
                    // appear at exactly ONE site in the crate — all three right
                    // here. Counting a terminator on exactly one side drives the
                    // reconcile residual negative, which `reconcile_residual`'s
                    // `> 0` guard skips and `record`'s `max(0)` floor then hides
                    // from every observer, permanently.
                    channel_depth::received();
                    self.received_count += 1;
                    // Issue #2096: one merge entry decoded from `Data.db` by THIS
                    // adapter-driven run — a full scan, compaction, or (via the
                    // fail-safe `SinglePartitionFilterRun`) a point read all share
                    // this increment site, so `merge_run_entries_decoded` counts
                    // entries for any of them, not point reads alone. See
                    // `work_counters::merge_run_entries_decoded`'s doc for the
                    // process-global caveat when using it as a delta assertion.
                    crate::storage::sstable::work_counters::add_merge_run_entry_decoded();
                    return Some(Ok(entry));
                }
                // TERMINAL failure. Recorded so a repeat poll re-reports the
                // identical error. `Cancelled` stays a distinct `Error::Cancelled`
                // (issue #2264) — `drive_merge` matches on the variant, not on a
                // side-channel flag — via `MergeProducerError::to_error`.
                Ok(MergeMsg::Failed(e)) => {
                    let error = e.to_error();
                    self.state = RunState::Failed(e);
                    return Some(Err(error));
                }
                // The ONLY proof of a clean, COMPLETE run.
                Ok(MergeMsg::Done) => {
                    self.state = RunState::Finished;
                    return None;
                }
                // No entry yet: re-poll the cancel flag and keep waiting.
                Err(RecvTimeoutError::Timeout) => continue,
                // The sender was dropped WITHOUT a terminator. Only a dead
                // producer can do that (a healthy one sends `Done`; a failing one
                // sends `Failed`), so this run is TRUNCATED at an arbitrary point.
                // Pre-#3120 this returned `None` — "the run is exhausted" — which
                // is the defect: a short read result, or a short REWRITTEN SSTable.
                Err(RecvTimeoutError::Disconnected) => {
                    self.state = RunState::Died;
                    return Some(Err(dead_producer_error()));
                }
            }
        }
    }

    /// Issue #2765 wiring hook: the exact adaptive capacity this adapter's egress
    /// `sync_channel` was constructed with (the merge-scoped snapshot the
    /// constructor threaded in).
    #[cfg(test)]
    fn egress_channel_capacity(&self) -> Option<usize> {
        Some(self.egress_channel_capacity)
    }
}

/// Cancel-aware teardown (issue #2361): trip the scan token, close the channel,
/// then JOIN the producer thread — so a dropped merger (LIMIT satisfied, client
/// disconnect, error, panic) does not leave a detached producer streaming a
/// multi-million-partition scan in the background.
///
/// ## Step order is LOAD-BEARING and must not change
///
/// cancel → drop receiver → join → reconcile. (1)+(2) are what make (3) bounded,
/// and (4) is only race-free once (3) has proven the producer can no longer send.
///
/// ## The join is a DIAGNOSTIC, never the verdict (issue #3120)
///
/// `std::thread::JoinHandle::join(self)` CONSUMES the handle and BLOCKS, so —
/// unlike the `tokio::JoinHandle` that issue #3106 could poll in place — it can
/// never be the mechanism that decides whether a run finished. The verdict comes
/// entirely from the producer's explicit terminator (see
/// [`SSTableRowIterator::next`] above and [`producer_msg`](super::producer_msg)),
/// which is why `next()` performs no join at all and stays non-blocking.
///
/// What step 3 does now is stop DISCARDING the join outcome: a producer that
/// unwound is logged here rather than silently swallowed. That is deliberately a
/// log/metric only — this `Drop` must stay NON-PANICKING (`teardown_tests` drops
/// mergers on purpose; issue #2361 pins that), and by the time a `Drop` runs the
/// consumer is gone, so there is nobody left to return an error to. The
/// correctness answer was already delivered through the channel.
#[cfg(feature = "write-support")]
impl Drop for SSTableRowIteratorAdapter {
    fn drop(&mut self) {
        // 1. Trip the cooperative token so a producer that is mid-scan (polling
        //    `scan_cancel`, NOT blocked on a channel send) abandons its walk.
        self.scan_cancel.cancel();
        // 2. Drop the receiver so a producer BLOCKED on a full `SyncSender::send`
        //    wakes immediately (send returns `Err`, and `drive_compaction_stream`
        //    maps that to `ControlFlow::Break`). Without this the join below could
        //    block until the channel drained. Dropping it here (before the join)
        //    rather than relying on field-drop order (which runs AFTER this `Drop`)
        //    is what makes the join bounded.
        //
        //    Issue #2419 roborev job 1733: do NOT try to drain-and-decrement the
        //    egress-depth gauge HERE. A prior version looped `try_recv` until
        //    `Empty` and decremented per drained entry before dropping the
        //    receiver — but the producer runs on another OS thread, so a send can
        //    slip in and succeed (incrementing the shared gauge) in the window
        //    between that loop's last `Empty` and this `drop`, with no entry ever
        //    physically drained to balance it: a PERMANENT, monotonic upward leak
        //    of `cqlite.merge.egress_channel_depth` across every
        //    cancelled/disconnected scan on a long-running server. The gauge is
        //    reconciled instead in step 4, AFTER the join, using this adapter's
        //    own sent/received delta — which only becomes authoritative once the
        //    producer thread has provably stopped sending.
        drop(self.receiver.take());
        // 3. Join the producer — bounded, because after (1)+(2) the producer
        //    cannot block indefinitely: it either observes the cancel in its scan
        //    loop or its next send fails. Issue #3120: the join outcome is no
        //    longer DISCARDED. It is a diagnostic, not the verdict (see this
        //    impl's doc): a producer that unwound has already reported itself
        //    through the terminator protocol, and a `Drop` has no caller to return
        //    an error to, so an unwind is LOGGED here — never re-panicked, which
        //    would abort a teardown that is frequently itself running during an
        //    unwind.
        //
        //    NOT exercised by the injected-panic tests, on purpose: both producer
        //    bodies wrap their whole walk in `catch_unwind`, so under
        //    `panic = "unwind"` a caught panic never reaches the join as an `Err`.
        //    This is the residual backstop for an unwind that escapes the
        //    `catch_unwind` — a panic between it returning and the terminal `send`,
        //    or inside the `send` itself — which is also the shape the bare-
        //    disconnect arm of `next()` above reports (see
        //    `producer_panic_tests::a_producer_that_disconnects_without_a_terminator_*`).
        if let Some(handle) = self.producer.take() {
            if handle.join().is_err() {
                tracing::warn!(
                    "streaming merge producer thread PANICKED (issue #3120); its run \
                     was reported to the merge as a terminal error via the channel \
                     terminator, so no row was silently dropped"
                );
            }
        }
        // 4. Issue #2419 roborev job 1733: reconcile the shared egress-depth
        //    gauge AFTER the producer thread has definitively exited — `join`
        //    returning guarantees `self.sent_count` can never increase again, so
        //    this read is race-free (unlike reading it, or draining the channel,
        //    before the join). Any DATA entries this adapter's producer
        //    successfully sent but this consumer never received — abandoned when
        //    the channel was torn down while entries were still buffered —
        //    are exactly `sent_count - received_count`; reconcile the shared
        //    gauge by that residual in ONE atomic op so a cancelled/disconnected
        //    scan returns `cqlite.merge.egress_channel_depth` to baseline instead
        //    of drifting upward.
        let residual =
            self.sent_count.load(std::sync::atomic::Ordering::SeqCst) - self.received_count;
        channel_depth::reconcile_residual(residual);
    }
}
