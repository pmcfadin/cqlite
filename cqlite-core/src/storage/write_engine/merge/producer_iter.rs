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
//! [`from_readers`](super::from_readers); the egress-depth gauge accounting
//! this module only *calls* lives in
//! [`channel_depth`](super::channel_depth).

use super::{
    channel_depth, from_readers, producer_gauge, MergeEntry, MergeProducerError, SSTableRowIterator,
};
use crate::error::{Error, Result};
use crate::schema::TableSchema;
use std::path::{Path, PathBuf};

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
    pub(super) receiver:
        Option<std::sync::mpsc::Receiver<std::result::Result<MergeEntry, MergeProducerError>>>,
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
            Self::producer_thread(
                path_buf,
                run_index,
                schema,
                udt_registry,
                scan_cancel,
                sender,
                producer_sent_count,
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
    /// the total source size. Errors are forwarded as `Err(String)`.
    fn producer_thread(
        path_buf: PathBuf,
        run_index: usize,
        schema: TableSchema,
        udt_registry: Option<crate::schema::UdtRegistry>,
        scan_cancel: crate::storage::scan_cancel::ScanCancel,
        sender: std::sync::mpsc::SyncSender<std::result::Result<MergeEntry, MergeProducerError>>,
        sent_count: std::sync::Arc<std::sync::atomic::AtomicI64>,
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
        // Clone the sender for the error path: the streaming closure moves one
        // clone for per-entry sends, leaving this one to report a fatal error.
        let error_sender = sender.clone();
        let stream_result = (|| -> Result<()> {
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
                let mut reader = crate::storage::sstable::reader::SSTableReader::open(
                    &path_buf, &config, platform,
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
                )
                .await
            })
        })();

        if let Err(e) = stream_result {
            // Forward the error (preserving `Cancelled` distinctly, issue #2264);
            // ignore send failure (consumer may have dropped).
            let _ = error_sender.send(Err(MergeProducerError::from(e)));
        }
        // Channel closed naturally when sender is dropped here.
    }
}

#[cfg(feature = "write-support")]
impl SSTableRowIterator for SSTableRowIteratorAdapter {
    fn next(&mut self) -> Option<Result<MergeEntry>> {
        use std::sync::mpsc::RecvTimeoutError;
        // The receiver is only `None` after `Drop` has run — never during a live
        // merge — so treat a missing receiver as end-of-stream.
        let receiver = self.receiver.as_ref()?;
        // Cancel-aware blocking recv (issue #2361): a plain `recv()` blocks
        // indefinitely inside `KWayMerger::step`, so a `do_get` cancellation that
        // lands while the consumer is waiting for the next producer entry was NOT
        // observed until an entry (or channel close) arrived. Poll `scan_cancel`
        // at a bounded interval so `drive_merge` sees `Error::Cancelled` promptly
        // even while blocked here.
        loop {
            if self.scan_cancel.is_cancelled() {
                return Some(Err(Error::Cancelled));
            }
            match receiver.recv_timeout(RECV_CANCEL_POLL) {
                Ok(Ok(entry)) => {
                    // Issue #2419 (WS2): this DATA entry just left the bounded
                    // egress channel — decrement the live occupancy gauge,
                    // balancing the `channel_depth::sent()` at its send site
                    // (`from_readers::forward_row`). `received_count` is this
                    // adapter's OWN mirror of that decrement (roborev job 1733),
                    // read by `Drop` post-join to compute the exact reconcile
                    // residual — see the field doc.
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
                // Issue #2264: reconstruct `Error::Cancelled` distinctly so a
                // cancelled scan is never confused with a genuine I/O/corruption
                // error at the merge/producer boundary — `drive_merge` matches on
                // the variant, not on a side-channel flag.
                Ok(Err(MergeProducerError::Cancelled)) => return Some(Err(Error::Cancelled)),
                Ok(Err(MergeProducerError::Other(msg))) => {
                    return Some(Err(Error::Storage(format!(
                        "streaming merge producer error: {}",
                        msg
                    ))))
                }
                // No entry yet: re-poll the cancel flag and keep waiting.
                Err(RecvTimeoutError::Timeout) => continue,
                // Channel closed — producer finished normally.
                Err(RecvTimeoutError::Disconnected) => return None,
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
        //    loop or its next send fails. A failed join (producer panicked) is
        //    ignored; the panic was already surfaced through the error channel.
        if let Some(handle) = self.producer.take() {
            let _ = handle.join();
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
