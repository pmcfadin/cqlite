//! The batch-production seam the spike shares with production (issue #2605).
//!
//! Both benchmark arms and the DataFusion `ExecutionPlan` consume batches from
//! exactly ONE place: [`spawn_scan`], which drives the EXISTING
//! [`MergeProducer::produce_streaming`] loop — the same call the streaming
//! `do_get` route makes — and forwards each [`RecordBatch`] into a bounded
//! channel as it is produced.
//!
//! # Why streaming and not `produce()`
//!
//! [`MergeProducer::produce`] returns `Vec<RecordBatch>`, i.e. the WHOLE result
//! set resident at once. Over the spike corpus (millions of wide rows) that is
//! several GB and would blow the B4 512Mi pod budget before any engine
//! comparison could be made — the measurement would be of swapping, not of
//! execution. The streaming seam bounds resident payload to
//! `channel_capacity x max_batch_bytes` regardless of result size.
//!
//! # Resident-bytes bound
//!
//! The sink issues [`EgressReservation::inert`] rather than participating in the
//! `#2821` egress-credit pool: there is no gRPC egress here to govern, and the
//! bound is stated directly instead — at most [`CHANNEL_CAPACITY`] batches are
//! in flight, each capped at the producer's `max_batch_bytes` payload ceiling
//! (default [`crate::batch_bytes::DEFAULT_MAX_BATCH_BYTES`] = 4 MiB), plus the
//! one batch the consumer holds. The harness records peak RSS so the claim is
//! measured rather than asserted.
//!
//! # Which read arm runs (load-bearing, throughput-program M15 item 4)
//!
//! `produce_streaming`'s path-based (cold) route builds a `KWayMerger`
//! unconditionally — the single-generation `bypass` arm exists only on the WARM
//! reader-based route (`crate::bypass`). So a spike scan is always
//! post-reconciliation. That is an argument about code, not an observation, so
//! [`ScanOutcome`] additionally carries the authoritative post-prune source
//! count AND the `cqlite_core::storage::read_path_probe` counter delta, and the
//! harness FAILS when the merge arm did not demonstrably run. If it did not, the
//! two arms would be comparing different row sets and the benchmark would be
//! measuring a correctness difference rather than an engine difference.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;

use arrow::record_batch::RecordBatch;
use cqlite_core::observability::{StreamSubPhase, StreamSubPhaseTimings};
use cqlite_core::schema::TableSchema;
use cqlite_core::storage::read_path_probe::ReadPathProbe;

use crate::cancel::CancelFlag;
use crate::egress_credit::{CreditedBatch, EgressReservation};
use crate::filter::ScanSpec;
use crate::producer::{BatchSink, DirSource, MergeProducer, ProducerError, SstableSource};
use crate::scan_progress::ScanProgress;

/// In-flight batch ceiling for the spike's producer→consumer channel. Small on
/// purpose: it is half of the stated resident bound, and a deeper queue would
/// hide producer/consumer imbalance behind buffering, which is precisely the
/// thing the "shared batch-production floor" scenario is trying to measure.
pub const CHANNEL_CAPACITY: usize = 2;

/// One batch as it crosses the seam: a produced batch, or the terminal error
/// that ended the scan.
pub type BatchItem = Result<RecordBatch, ProducerError>;

/// What to scan: a resolved table directory plus the decode contract.
#[derive(Clone, Debug)]
pub struct ScanTarget {
    /// Authoritative table schema (parsed from the corpus DDL).
    pub schema: TableSchema,
    /// Directory holding the table's `*-Data.db` components.
    pub dir: PathBuf,
    /// Rows per Arrow batch (production default is 8192).
    pub batch_size: usize,
}

/// Per-sub-phase wall time for one scan, in nanoseconds, read back from the
/// ALWAYS-COMPILED `cqlite_core::observability::stream_subphase` accumulator —
/// the same instrument the `#2819` `cqlite.rpc.phase` sub-phase histograms are
/// emitted from, so the spike invents no new timing.
///
/// The sub-phases run on concurrent pipeline threads and OVERLAP in wall clock;
/// they are NOT expected to sum to the scan's elapsed time.
#[derive(Debug, Clone, Copy, Default)]
pub struct SubPhaseNanos {
    /// Cold body-chunk page-in, per-SSTable producer thread(s).
    pub cold_fault: u64,
    /// Chunk decompression (the corpus is LZ4-compressed), producer thread(s).
    pub decompress: u64,
    /// k-way merge + reconcile + per-row materialize, merge-consumer thread.
    pub merge: u64,
    /// Arrow array BUILD — the row→column transpose. This is the cost a columnar
    /// producer would eliminate, i.e. the decode-to-column figure M15 item 1 asks
    /// to be reported SEPARATELY from the vectorized-exec delta.
    pub encode: u64,
    /// Egress channel send incl. backpressure park — `None` when the sink that
    /// served the scan does not instrument it.
    ///
    /// The spike's own [`SpikeSink`] does NOT: the counter is fed by
    /// PRODUCTION's `ChannelSink`, so in this harness it is always unmeasured.
    /// Reporting the accumulator's `0` here would be a FABRICATED ZERO — "the
    /// channel send cost nothing" is a very different statement from "nobody
    /// measured the channel send", and the first one is false. Same rule as
    /// `rss.rs`: an absence is reported as an absence.
    pub grpc_write: Option<u64>,
}

impl SubPhaseNanos {
    /// Read every counter out of a finished scan's accumulator.
    fn read(timings: &StreamSubPhaseTimings) -> Self {
        Self {
            cold_fault: timings.nanos(StreamSubPhase::ColdFault),
            decompress: timings.nanos(StreamSubPhase::Decompress),
            merge: timings.nanos(StreamSubPhase::Merge),
            encode: timings.nanos(StreamSubPhase::Encode),
            // Unconditionally `None`, and NOT `Some(0)`-unless-nonzero: whether
            // this phase is instrumented is a STATIC property of the sink that
            // ran (the spike's never records it), not something to infer from
            // whether the counter happens to be zero on this run.
            grpc_write: None,
        }
    }
}

/// Everything the harness needs to characterise one finished scan.
#[derive(Debug)]
pub struct ScanOutcome {
    /// Rows emitted across every batch.
    pub rows: u64,
    /// Batches emitted.
    pub batches: u64,
    /// Sub-phase decomposition of this scan.
    pub subphase: SubPhaseNanos,
    /// `read_path_probe` counter DELTA across this scan. Non-zero
    /// `reconcile_entries` is a direct observation that the compaction
    /// reconciler ran, i.e. the merge arm served the scan.
    pub probe: ProbeDelta,
    /// Terminal result of the merge loop.
    pub result: Result<(), ProducerError>,
}

/// `read_path_probe` counter delta across one scan.
#[derive(Debug, Clone, Copy, Default)]
pub struct ProbeDelta {
    /// Entries into the compaction reconciler (merge arm only).
    pub reconcile_entries: u64,
    /// Per-row cell-write-metadata maps allocated (merge arm only).
    pub cell_metadata_maps: u64,
}

impl ProbeDelta {
    /// Whether this scan demonstrably ran the k-way MERGE arm rather than the
    /// single-generation bypass. Both counters are incremented only on the merge
    /// arm, so a non-zero reading is a direct observation, never a correlation.
    pub fn merge_arm_observed(self) -> bool {
        self.reconcile_entries > 0 || self.cell_metadata_maps > 0
    }
}

/// A live scan: the batch channel, the arm evidence available before the first
/// batch, and the handle that yields the [`ScanOutcome`] when the merge ends.
pub struct RunningScan {
    /// Batches as produced. Dropping the receiver stops the merge (the sink's
    /// send fails and reports [`ProducerError::Cancelled`]).
    pub batches: tokio::sync::mpsc::Receiver<BatchItem>,
    /// Post-prune `*-Data.db` count actually handed to the merger — the
    /// authoritative source count for the "≥ 2 generations" precondition.
    pub sources: usize,
    /// Joins the producer thread and yields its outcome.
    pub done: JoinHandle<ScanOutcome>,
}

/// Build the producer for `target` under `spec`, sharing production's defaults.
pub fn build_producer(target: &ScanTarget, spec: ScanSpec) -> Result<MergeProducer, ProducerError> {
    MergeProducer::with_spec(target.schema.clone(), target.batch_size, spec)
}

/// Resolve the post-prune `*-Data.db` paths for `target`.
///
/// Called from a SYNCHRONOUS context on purpose. `resolve_paths` performs
/// blocking filesystem I/O and — when a token filter is present — builds its own
/// Tokio runtime for the `Summary.db` prune, which would panic if it ran inside
/// an async task. Resolving once, up front, keeps every later step (including
/// DataFusion's `ExecutionPlan::execute`, which DataFusion calls from inside its
/// runtime) free of both hazards.
pub fn resolve_paths(
    producer: &MergeProducer,
    target: &ScanTarget,
) -> Result<Vec<PathBuf>, ProducerError> {
    let source: &dyn SstableSource = &DirSource::new(target.dir.clone());
    producer.resolve_paths(source)
}

/// Start streaming `paths` through `producer` on a dedicated OS thread.
///
/// The thread installs a fresh sub-phase accumulator (so the timings belong to
/// THIS scan and to no other), snapshots the read-path arm counters, drives the
/// production `produce_streaming` loop, and reports the totals back through the
/// join handle.
pub fn spawn_scan(producer: Arc<MergeProducer>, paths: Vec<PathBuf>) -> RunningScan {
    let sources = paths.len();
    let (tx, rx) = tokio::sync::mpsc::channel::<BatchItem>(CHANNEL_CAPACITY);
    let done = std::thread::spawn(move || run_scan(producer, paths, tx));
    RunningScan {
        batches: rx,
        sources,
        done,
    }
}

/// The producer thread body: install the timing sink, drive the merge, report.
fn run_scan(
    producer: Arc<MergeProducer>,
    paths: Vec<PathBuf>,
    tx: tokio::sync::mpsc::Sender<BatchItem>,
) -> ScanOutcome {
    let timings = Arc::new(StreamSubPhaseTimings::default());
    // RAII: restored on every exit path incl. a panic unwind, so this scan's
    // sink can never leak into a later one on a reused thread.
    let _sink_guard = cqlite_core::observability::stream_subphase::install(Some(timings.clone()));

    let before = ReadPathProbe::snapshot();
    let progress = ScanProgress::default();
    let cancel = CancelFlag::new();
    let mut sink = SpikeSink {
        tx: tx.clone(),
        rows: 0,
        batches: 0,
    };

    let result = producer.produce_streaming(paths, &cancel, &mut sink, &progress, || {});
    let delta = ReadPathProbe::snapshot().delta_since(&before);

    // Publish the terminal error to the consumer too, so a stream that ends
    // early is never mistaken for a complete result set.
    if let Err(e) = &result {
        let _ = tx.blocking_send(Err(clone_producer_error(e)));
    }

    ScanOutcome {
        rows: sink.rows,
        batches: sink.batches,
        subphase: SubPhaseNanos::read(&timings),
        probe: ProbeDelta {
            reconcile_entries: delta.reconcile_entries,
            cell_metadata_maps: delta.cell_metadata_maps,
        },
        result,
    }
}

/// `ProducerError` is not `Clone` (it wraps non-`Clone` sources), and the
/// terminal error must reach BOTH the consumer stream and the outcome record.
/// Re-express it as an `Other` carrying the original `Display` text rather than
/// dropping one of the two — a benchmark that silently ignores a scan error
/// would report a throughput number for a truncated result set.
fn clone_producer_error(e: &ProducerError) -> ProducerError {
    ProducerError::Merge(cqlite_core::Error::Internal(e.to_string()))
}

/// [`BatchSink`] forwarding each produced batch into the spike channel.
struct SpikeSink {
    tx: tokio::sync::mpsc::Sender<BatchItem>,
    rows: u64,
    batches: u64,
}

impl BatchSink for SpikeSink {
    /// Inert: there is no gRPC egress residency to govern here, and the resident
    /// bound is stated structurally (see the module docs) rather than metered.
    fn reserve(&mut self, _capacity_bytes: usize) -> Result<EgressReservation, ProducerError> {
        Ok(EgressReservation::inert())
    }

    fn emit(&mut self, batch: CreditedBatch) -> Result<(), ProducerError> {
        let batch = batch.into_batch();
        self.rows = self.rows.saturating_add(batch.num_rows() as u64);
        self.batches = self.batches.saturating_add(1);
        // A closed channel means the consumer is gone (an early-terminating
        // query, or a dropped stream). Report it as cancellation so the merge
        // stops promptly instead of scanning the rest of the corpus.
        self.tx
            .blocking_send(Ok(batch))
            .map_err(|_| ProducerError::Cancelled)
    }
}

/// Process-wide count of scans started by the spike, so a harness run can state
/// how many times it touched the corpus. Purely informational.
static SCANS_STARTED: AtomicU64 = AtomicU64::new(0);

/// Record and return the number of spike scans started so far.
pub fn note_scan_started() -> u64 {
    SCANS_STARTED.fetch_add(1, Ordering::Relaxed) + 1
}
