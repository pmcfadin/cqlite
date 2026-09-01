//! Streaming `do_get` egress (issue #1476, AB1).
//!
//! `do_get` no longer runs the whole compaction merge to completion and collects
//! every [`RecordBatch`] into a `Vec` before the first byte reaches the client.
//! Instead the merge runs on the blocking pool and sends each batch into a bounded
//! [`tokio::sync::mpsc`] channel as it is produced; the gRPC response wraps the
//! receiver. First batches reach the wire while the merge is still running, and
//! peak resident payload is bounded — independent of the total result size. This
//! mirrors the proven `cqlite-core` `delta_scan/scan.rs` bounded-channel pattern.
//!
//! Two independent governors bound that residency, and neither replaces the
//! other (issue #2821):
//!
//! * [`DO_GET_CHANNEL_CAPACITY`] — a batch **count**, which binds first at narrow
//!   row widths;
//! * [`crate::egress_credit`] — a per-stream in-flight **capacity-byte** credit
//!   pool (`--max-inflight-egress-bytes`), which binds first at wide row widths
//!   and is what makes residency independent of row width.
//!
//! The retained `produce`/`produce_cancellable` collect path remains the
//! byte-identity parity oracle and serves the aggregate route (bounded output).

use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use arrow::datatypes::Schema as ArrowSchema;
use arrow::record_batch::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::FlightData;
use futures::{Stream, StreamExt};
use tokio::sync::mpsc;
use tonic::Status;

use cqlite_core::storage::sstable::reader::SSTableReader;

use crate::cancel::CancelFlag;
use crate::egress_credit::{
    CreditedBatch, EgressBudget, EgressCredit, EgressObservation, EgressReservation,
};
use crate::metered_stream::{MeteredDoGetStream, ReceiverStream};
use crate::obs::RpcMetrics;
use crate::producer::{BatchSink, MergeProducer, ProducerError};

/// The merge input for the streaming row path (issue #2310): either the cold
/// per-request `Data.db` paths (opened fresh) or a WARM set of already-open,
/// shared `Arc<SSTableReader>`s handed over by the warm-handle registry. The two
/// drive byte-identical merges — only WHO opened the reader differs — so the
/// single `spawn_streaming` egress serves both.
pub(crate) enum MergeInput {
    /// Cold path: open a fresh reader per path. Production row reads now take the
    /// warm [`Self::Readers`] path (issue #2310), so this variant is retained as
    /// the byte-identity regression oracle exercised by the streaming test suite
    /// (`streaming_tests.rs`) — it proves the shared bounded-channel egress +
    /// cancellation machinery still behaves identically for a cold input.
    #[cfg_attr(not(test), allow(dead_code))]
    Paths(Vec<PathBuf>),
    /// Warm path: drive the merge over cached, shared readers.
    Readers(Vec<Arc<SSTableReader>>),
}

/// Boxed server response stream (matches `service::BoxStream<FlightData>`).
pub(crate) type DoGetStream =
    Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;

/// `do_get` channel capacity, in batches.
///
/// This is a batch **COUNT** governor, and a count is only half a memory bound.
/// Production peak resident rows per stream is approximately
/// `(DO_GET_CHANNEL_CAPACITY + 2) · batch_size` ≈ **49,152 rows** at the default
/// `batch_size` of 8192 — the `+2` being the two real production terms: one
/// send-in-flight batch (`ChannelSink::emit` counts a batch before its channel
/// slot is taken) and one encoder prefetch (`FlightDataEncoderBuilder` pulls a
/// batch ahead of yielding it). That figure is **row-width dependent**: multiply
/// it by an unbounded per-row width and the byte residency is unbounded, which is
/// exactly the gap issue #2821 closes.
///
/// The bytes are governed separately, and configurably, by the per-stream
/// in-flight capacity-byte ceiling — `--max-inflight-egress-bytes` /
/// `CQLITE_MAX_INFLIGHT_EGRESS_BYTES` /
/// [`crate::egress_credit::DEFAULT_MAX_INFLIGHT_EGRESS_BYTES`]. The two compose;
/// whichever binds first wins (narrow shapes: this count; wide shapes: the byte
/// ceiling). The depth itself stays a small named constant.
pub(crate) const DO_GET_CHANNEL_CAPACITY: usize = 4;

/// Structural slack beyond [`DO_GET_CHANNEL_CAPACITY`] that the merge's
/// `produced_batches` counter can legitimately reach, given the exact instant a
/// consumer observes it (roborev N1 — the doc and the test bound must share one
/// derivation so they cannot drift):
///
/// - **+1 send-in-flight**: [`ChannelSink::emit`] increments the counter BEFORE
///   `blocking_send`, so one produced batch can be counted while still blocked
///   trying to enter an already-full channel.
/// - **+1 encoder prefetch**: `FlightDataEncoderBuilder`'s stream can pull one
///   batch out of the channel into its own internal state ahead of yielding it
///   to the gRPC consumer, freeing a channel slot the producer immediately fills.
/// - **+1 scheduling slack**: absorbs Tokio scheduling nondeterminism between the
///   test reading its observation messages and reading the counter (e.g. via
///   `yield_now`), without which the bound would be exact-timing-dependent.
///
/// Test-only (`#[cfg(test)]`): this is a test-observation bound, not a value any
/// production code branches on — the doc comment above is the production-facing
/// derivation; this constant just keeps the tests from drifting from it.
#[cfg(test)]
pub(crate) const IN_FLIGHT_ALLOWANCE: usize = 3;

/// Test-only observability handle for the streaming path.
///
/// The counters are always maintained (the writes are cheap `Relaxed` atomics),
/// so production simply passes a throwaway [`StreamProbe::default`]. Tests inject
/// their own to assert the memory bound (batches produced), metrics attribution
/// (rows/bytes fed to [`RpcMetrics`]), and that a mid-stream error reaches the
/// error-observability hook (roborev B2) — without reaching into private state
/// or depending on the `observability` feature's OTel counters, which are a
/// genuine no-op when the feature is off (see `cqlite_core::observability::record_error`).
#[derive(Clone, Default)]
pub(crate) struct StreamProbe {
    /// Batches the merge has pushed toward the channel (bounded by the backpressure).
    produced_batches: Arc<AtomicUsize>,
    /// Rows attributed to metrics at stream end (== emitted prefix on cancel).
    rows: Arc<AtomicU64>,
    /// Payload bytes attributed to metrics at stream end.
    bytes: Arc<AtomicU64>,
    /// Running rows counted as each record batch passes through
    /// [`MeteredDoGetStream`] toward the client (issue #2162). Updated per batch
    /// BEFORE the stream ends, so a feature-independent slow-consumer test can
    /// observe `cqlite.rpc.rows` progress mid-stream (the OTel counter itself is a
    /// no-op when the `observability` feature is off). Distinct from [`Self::rows`],
    /// which is only stored at finalization.
    progressed_rows: Arc<AtomicU64>,
    /// Count of record batches that have passed through [`MeteredDoGetStream`]
    /// toward the client — i.e. the number of per-batch `cqlite.rpc.rows` /
    /// `cqlite.rpc.bytes` counter emissions (issue #2162). One emission per batch,
    /// never per row. Distinct from [`Self::produced_batches`], which counts
    /// batches the merge pushed toward the channel.
    emitted_batches: Arc<AtomicUsize>,
    /// Incremented once per surfaced egress error, alongside the
    /// `crate::obs::record_status_error` call — both by [`MeteredDoGetStream`]'s
    /// error arm for a merge-stage error (roborev B2) AND by
    /// [`record_encoder_error`] for an encoder-stage error (issue #2193). Proves
    /// every egress failure reaches the same error-observability hook `do_get`'s
    /// eager-setup errors always did, independent of whether OTel counters are
    /// compiled in.
    errors_recorded: Arc<AtomicUsize>,
    /// Blocking-task guards THIS stream's own `spawn_blocking` merge closure
    /// entered (issue #2896): exactly one per streaming merge. Makes the
    /// `cqlite.flight.blocking_tasks_in_use` wiring observable PER-STREAM rather
    /// than through the process-global gauge.
    blocking_entries: Arc<AtomicUsize>,
    /// The shared in-use level that closure's own
    /// [`crate::saturation::BlockingTaskGuard`] observed at ITS entry (issue
    /// #2896) — see [`crate::saturation::BlockingTaskGuard::entry_level`], and
    /// [`crate::saturation::blocking_tasks_in_use_level`] for the normative rule
    /// on bounding it soundly.
    blocking_entry_level: Arc<AtomicI64>,
    /// Egress credit accounting (issue #2821): charged permit bytes, REALIZED
    /// resident capacity bytes and their high-water marks, plus the
    /// reservations-granted / batches-materialized pair that makes
    /// reserve-before-materialize observable. Maintained feature-independently
    /// exactly like [`Self::produced_batches`]; no new OTel metric.
    pub(crate) egress: EgressObservation,
    /// Incremental core-scan progress seam (issue #2162): counts the
    /// `cqlite.query.rows_scanned` delta flushes the merge/scan loop emits and
    /// their summed total, feature-independently (like the rest of this probe).
    /// Threaded into `drive_merge` so a full-scan `do_get` records ≥ 2 flushes
    /// over a threshold-crossing scan, versus exactly 1 for a sub-threshold scan.
    pub(crate) scan_progress: crate::scan_progress::ScanProgress,
}

/// Sink that sends each merged batch into the bounded `do_get` channel.
///
/// A send failure means the receiver (client) is gone: report
/// [`ProducerError::Cancelled`] so the merge stops within a bounded number of
/// steps (composing with the #1473 `CancelFlag`).
struct ChannelSink {
    tx: mpsc::Sender<Result<CreditedBatch, ProducerError>>,
    produced: Arc<AtomicUsize>,
    /// The SAME cancel flag threaded through the merge (issue #2264). The
    /// backpressure send below races this flag's async cancellation, so a client
    /// disconnect wakes a producer otherwise parked forever in a full channel.
    cancel: CancelFlag,
    /// The per-stream in-flight capacity-byte credit pool (issue #2821). Credit
    /// is taken in [`Self::reserve`] BEFORE the batch is built, so no
    /// materialized-but-uncharged `RecordBatch` can exist on this path.
    credit: EgressCredit,
}

impl BatchSink for ChannelSink {
    fn reserve(&mut self, capacity_bytes: usize) -> Result<EgressReservation, ProducerError> {
        // Same cancellation-aware park as `emit` below (issue #2264): a producer
        // waiting for egress credit must be woken by a client disconnect exactly
        // as one waiting for a channel slot is, and must pin no blocking-pool
        // thread. `reserve` runs on the same `spawn_blocking` thread as `emit`,
        // which carries the runtime handle in TLS, so `Handle::block_on` is
        // permitted here for the same reason it is there.
        let handle = tokio::runtime::Handle::current();
        let cancelled = self.cancel.cancelled();
        let credit = self.credit.clone();
        handle.block_on(async move {
            tokio::select! {
                // Bias the credit arm so an immediately-available reservation is
                // taken deterministically even if cancellation fires in the same
                // poll — matching `emit`'s bias, keeping normal-path behaviour and
                // the stream/collect byte-parity identical.
                biased;
                // A pool that cannot charge the reservation fails the stream
                // CLOSED (issue #2821): an uncharged reservation would put a
                // batch on the egress path outside the published bound.
                reservation = credit.reserve(capacity_bytes) => Ok(reservation?),
                // Client disconnected while we were parked waiting for credit.
                // Nothing has been materialized, so nothing is abandoned.
                _ = cancelled => Err(ProducerError::Cancelled),
            }
        })
    }

    fn emit(&mut self, batch: CreditedBatch) -> Result<(), ProducerError> {
        self.produced.fetch_add(1, Ordering::Relaxed);
        // Cancellation-aware backpressure (issue #2264). `reserve()` still applies
        // backpressure — it only resolves once the bounded channel has a free slot,
        // so the merge never runs ahead of the consumer by more than the channel
        // capacity. But unlike a bare `blocking_send` (which wakes ONLY on a freed
        // permit or a dropped receiver, never on the cancel flag), this races the
        // reservation against the flag's async cancellation. When a client
        // disconnects mid-stream while the channel is full — the exact case where
        // tonic/h2 may not promptly drop the receiver — the cancel arm fires and the
        // producer stops instead of pinning a blocking-pool thread forever.
        //
        // `emit` runs on a `spawn_blocking` thread, which carries the runtime handle
        // in TLS; `Handle::block_on` there is permitted (it is a blocking thread, not
        // an async worker) and drives the `select!` to completion.
        let handle = tokio::runtime::Handle::current();
        let cancelled = self.cancel.cancelled();
        // Issue #2819: the `stream_grpc_write` sub-phase wraps ONLY the egress
        // channel `reserve()`/send here — INCLUDING the backpressure park while a
        // slow client is not draining. This runs on the merge/egress thread and
        // shares no code interval with the producer-thread `stream_cold_fault`
        // scope (`data_access/compressed_offset.rs` on the Summary-guided read,
        // `data_access/compaction.rs` on the full-ring fallback), so a client
        // stall inflates `stream_grpc_write` but can never inflate
        // `stream_cold_fault`.
        cqlite_core::observability::stream_subphase::timed(
            cqlite_core::observability::StreamSubPhase::GrpcWrite,
            || {
                handle.block_on(async {
                    tokio::select! {
                        // Bias the send arm so a ready permit is taken
                        // deterministically even if cancellation fires in the same
                        // poll — a batch that CAN be delivered without blocking is,
                        // keeping normal-path behaviour and the stream/collect
                        // byte-parity identical.
                        biased;
                        permit = self.tx.reserve() => match permit {
                            // Receiver still present: deliver this batch.
                            Ok(permit) => {
                                permit.send(Ok(batch));
                                Ok(())
                            }
                            // Receiver dropped (client gone): stop the merge.
                            Err(_) => Err(ProducerError::Cancelled),
                        },
                        // Client disconnected while parked waiting for a slot.
                        _ = cancelled => Err(ProducerError::Cancelled),
                    }
                })
            },
        )
    }
}

/// Run `run` (the merge body) under [`std::panic::catch_unwind`], forwarding
/// either the merge's own error or a caught panic as a terminal `Err` into `tx`
/// (issue #1476, roborev B1).
///
/// Without this, a panic inside `spawn_blocking` simply drops `tx` when the
/// blocking task unwinds; the receiver then sees a normal, clean channel close
/// — indistinguishable from "the merge finished successfully" — so the client
/// (and Trino) would silently read a TRUNCATED result as a complete one. Forcing
/// every panic through the channel as [`ProducerError::Panicked`] makes it
/// surface as a gRPC `Status::internal` on every profile (this is a correctness
/// fix; `panic=abort` release profiles mask the symptom only in production).
fn run_merge_catching_panics<F>(tx: &mpsc::Sender<Result<CreditedBatch, ProducerError>>, run: F)
where
    F: FnOnce() -> Result<(), ProducerError>,
{
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(run)) {
        Ok(Ok(())) => {}
        Ok(Err(ProducerError::Cancelled)) => {
            // Do NOT forward a terminal `Cancelled` (issue #2264): cancellation
            // means the client/receiver is already departing, so a `Cancelled`
            // status has no value to deliver — and a bare, cancel-unaware
            // `blocking_send` of it would park forever if this send raced ahead of
            // the receiver's drop. Skipping it here makes liveness EXPLICIT rather
            // than resting on `MeteredDoGetStream`'s implicit field-drop order (rx
            // before guard), which a future field reorder could silently break.
        }
        Ok(Err(e)) => {
            // Forward a genuine terminal error to the client (matches delta_scan
            // error forwarding). This applies normal bounded backpressure and
            // resolves on read/disconnect; ignored if the receiver is already gone.
            let _ = tx.blocking_send(Err(e));
        }
        Err(payload) => {
            let message = panic_message(payload.as_ref());
            let _ = tx.blocking_send(Err(ProducerError::Panicked { message }));
        }
    }
}

/// Best-effort extraction of a panic payload's message: the two payload shapes
/// `std::panic!`/`.unwrap()`/`.expect()` actually produce (`&'static str` and
/// `String`) are read verbatim; any other payload type gets a fixed placeholder
/// (never a guess at its structure).
fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

/// Spawn the row-merge of already-resolved `paths` streaming into a bounded
/// channel, returning the encoded `do_get` response stream and the merge task
/// handle (production drops the handle; tests await it to observe cancellation
/// deterministically).
///
/// `paths` MUST come from [`MergeProducer::resolve_paths`] (already token-pruned),
/// so the fallible discovery/prune has already surfaced upstream. `cancel` is
/// the SAME flag threaded through the caller's eager-setup phase (issue #1476,
/// roborev F1) — one flag, one cancellation story spanning setup + merge; the
/// caller's setup-phase guard is expected to already be disarmed by the time
/// this runs, so the fresh guard armed here is the only one live from this
/// point on.
/// Warm analogue of [`spawn_streaming`] (issue #2310): drive the streaming
/// row-merge over an already-open, shared warm reader set from the
/// [`crate::warm::WarmTableRegistry`] instead of cold per-request paths. A thin
/// wrapper that hands [`spawn_streaming`] a [`MergeInput::Readers`].
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_streaming_from_readers(
    producer: MergeProducer,
    readers: Vec<Arc<SSTableReader>>,
    schema_ref: Arc<ArrowSchema>,
    metrics: RpcMetrics,
    capacity: usize,
    budget: EgressBudget,
    probe: StreamProbe,
    cancel: CancelFlag,
    timer: crate::obs::PhaseTimer,
) -> (DoGetStream, tokio::task::JoinHandle<()>) {
    spawn_streaming(
        producer,
        MergeInput::Readers(readers),
        schema_ref,
        metrics,
        capacity,
        budget,
        probe,
        cancel,
        timer,
    )
}

/// Shared egress for the cold ([`MergeInput::Paths`]) and warm
/// ([`MergeInput::Readers`]) streaming row paths (issue #2310). The two drive
/// byte-identical merges; only the single merge-driving call inside the blocking
/// closure branches on the input shape. `paths`-shaped inputs MUST already be
/// token-pruned (as [`MergeProducer::resolve_paths`] returns); `readers`-shaped
/// inputs are the warm registry's pre-parsed set. `cancel` is the SAME flag
/// threaded through the caller's eager-setup phase (issue #1476, roborev F1).
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_streaming(
    producer: MergeProducer,
    input: MergeInput,
    schema_ref: Arc<ArrowSchema>,
    metrics: RpcMetrics,
    capacity: usize,
    budget: EgressBudget,
    probe: StreamProbe,
    cancel: CancelFlag,
    timer: crate::obs::PhaseTimer,
) -> (DoGetStream, tokio::task::JoinHandle<()>) {
    let (tx, rx) = mpsc::channel::<Result<CreditedBatch, ProducerError>>(capacity.max(1));
    // Issue #2821: the per-stream in-flight capacity-byte credit pool, publishing
    // its accounting through the probe's feature-independent observation seam.
    let credit = EgressCredit::new(budget, probe.egress.clone());
    // The shared cancel flag stops the merge when this response stream is dropped
    // (client disconnect); `blocking_send` failure is the second, independent stop
    // signal. AA3 machinery (#1473) is reused, not replaced.
    let guard = cancel.drop_guard();
    // The stream's egress-cancel race (issue #2680 / P0 #2782 hardening): an owned
    // future that resolves when the shared flag trips from ANY source. Threaded into
    // `MeteredDoGetStream` so a cancellation ends egress at the next poll even if the
    // merge is parked between steps. Taken before the flag moves into `merge_cancel`.
    let stream_cancelled = Box::pin(cancel.cancelled());
    // Clone for the sink's cancellation-aware backpressure race (issue #2264); the
    // remaining clone drives the between-step merge polling.
    let sink_cancel = cancel.clone();
    let merge_cancel = cancel;
    let produced = probe.produced_batches.clone();
    // The incremental scan-progress seam (issue #2162) is threaded into the merge
    // loop so `cqlite.query.rows_scanned` climbs while the scan is in progress.
    let scan_progress = probe.scan_progress.clone();
    // The blocking-task-guard observation seam (issue #2896): the closure below
    // publishes its OWN guard's entry through these, so the wiring assertion is
    // attributable to THIS stream and immune to peer streams' guard timing.
    let blocking_entries = probe.blocking_entries.clone();
    let blocking_entry_level = probe.blocking_entry_level.clone();

    // Issue #2819 (roborev L5): capture the `flight.do_get` RPC span HERE — this fn
    // runs synchronously inside the handler's `.instrument(span)` future, so
    // `Span::current()` is the live RPC span (same context `PhaseTimer::start`
    // captures it). It MUST be captured before the `spawn_blocking` below: tokio
    // does NOT propagate the caller's span across `spawn_blocking`, so
    // `Span::current()` on the blocking thread is the EMPTY span. Threaded into the
    // emitter so the sub-phase samples carry the same span/exemplar association as
    // the top-level phase samples.
    let rpc_span = tracing::Span::current();
    // Issue #3096: the IPC-framing accumulator lives OUT here, not inside the
    // blocking closure, because the arrow-flight encoder stage runs on the ASYNC
    // gRPC task that polls the response stream — a different thread, reached by no
    // thread-local the merge closure installs. Cloned for the framing wrapper
    // below; the span is cloned for its own teardown emitter.
    let framing_span = rpc_span.clone();

    // Run the CPU-bound merge off the async runtime; it sends batches as it goes.
    // `error_tx` is a clone kept OUTSIDE the (potentially unwound) merge closure so
    // a panic can still report through it — `sink` (holding the other clone) lives
    // inside the closure catch_unwind guards, and unwinding drops it.
    let handle = tokio::task::spawn_blocking(move || {
        // Issue #2419 (WS2): account this flight-managed blocking task on
        // `cqlite.flight.blocking_tasks_in_use` for the whole closure — the guard
        // is the FIRST act here and its Drop decrements on every exit path
        // (normal, error, cancel, panic).
        let blocking_guard = crate::saturation::BlockingTaskGuard::enter();
        // Issue #2896: publish THIS guard's own entry through the probe (see the
        // `StreamProbe` fields). These are plain statements on captured `Arc`s, not
        // ordering-relevant locals: the invariant they preserve is that the guard
        // is entered BEFORE any other statement in this closure, and that no local
        // declared after it can outlive it (so the accounting still spans the whole
        // closure body, including the merge below).
        blocking_entries.fetch_add(1, Ordering::Relaxed);
        blocking_entry_level.store(blocking_guard.entry_level(), Ordering::Relaxed);
        // Issue #2819: per-request in-`stream` sub-phase accumulator, installed on
        // THIS merge consumer thread; emitted ONCE at teardown. Roborev B1 — the
        // emitter MUST flush its samples BEFORE any egress sender is released, or a
        // client scraping metrics at end-of-stream races the recording. Locals drop
        // in REVERSE declaration order, so `_subphase_emit` is declared AFTER
        // `error_tx` to drop FIRST: emit → then `error_tx` drops (closing the
        // channel / ending the client stream) → then `_subphase_install`
        // uninstalls the thread-local (no leak onto a reused blocking-pool thread).
        // Roborev M1 — install the sink ONLY when metrics are actually collected;
        // with the meter off, `install(None)` makes every timing site inert (ZERO
        // `Instant::now()` on the hot loop for a meter-off build).
        let subphase = Arc::new(cqlite_core::observability::StreamSubPhaseTimings::default());
        let _subphase_install = cqlite_core::observability::stream_subphase::install(
            cqlite_core::observability::metrics_active().then(|| subphase.clone()),
        );
        let error_tx = tx.clone();
        let _subphase_emit = crate::obs::StreamSubPhaseEmitter::new(rpc_span, subphase);
        let mut sink = ChannelSink {
            tx,
            produced,
            cancel: sink_cancel,
            credit,
        };
        run_merge_catching_panics(&error_tx, move || {
            // `timer` enters `do_get` already in the `merge_setup` phase (the
            // caller transitioned `resolve` → `merge_setup` before spawning). The
            // `on_merger_built` hook fires the `merge_setup` → `stream` transition
            // right after `KWayMerger::new` opens the input SSTables; the timer
            // then drops at the end of this closure, recording the final `stream`
            // phase (or `merge_setup` if the merger build itself failed / there was
            // nothing to merge). Issue #2162.
            let mut timer = timer;
            let on_built = || {
                timer.transition(crate::obs::PHASE_STREAM);
            };
            match input {
                MergeInput::Paths(paths) => producer.produce_streaming(
                    paths,
                    &merge_cancel,
                    &mut sink,
                    &scan_progress,
                    on_built,
                ),
                MergeInput::Readers(readers) => producer.produce_streaming_from_readers(
                    readers,
                    &merge_cancel,
                    &mut sink,
                    &scan_progress,
                    on_built,
                ),
            }
        });
    });

    let inner = Box::pin(ReceiverStream { rx });
    // Belt-and-suspenders (issue #2264): hand the merge task's `AbortHandle` to the
    // response stream so dropping it (client disconnect) also aborts the detached
    // `spawn_blocking` at its next await point. The cancellation-aware send above
    // is the core fix (abort alone cannot unpark a `blocking_send`); this is
    // defense-in-depth for a task that has since moved past its send. The
    // `JoinHandle` is still returned so tests await completion deterministically.
    let metered = MeteredDoGetStream::new(
        inner,
        metrics,
        Some(guard),
        probe.clone(),
        Some(handle.abort_handle()),
        Some(stream_cancelled),
    );
    let encoded = encode_do_get(metered, schema_ref, probe);
    (time_encoder_framing(encoded, framing_span), handle)
}

/// Wrap the arrow-flight encoder's output stream so its poll time lands in the
/// `stream_encode_framing` sub-phase (issue #3096).
///
/// # The blind spot this closes
///
/// `StreamSubPhase::Encode` (`egress_flush.rs`) times ONLY the projected cells'
/// resolution and the Arrow ARRAY BUILD, on the merge-consumer thread (since issue
/// #3552 that is `flush_buffer` PLUS the push-time `StageEncodeAccum`). The arrow-flight encoder stage
/// built in [`encode_do_get`] runs LATER, on the async gRPC task, and does the IPC
/// framing, the dictionary hydration, and the re-slicing of any batch larger than
/// the encoder's own target. None of that was inside ANY sub-phase, so a change
/// aimed at it could not be attributed from in-process timings at all.
///
/// # Cost
///
/// Two `Instant::now()` per `poll_next` — and ONLY when a meter is installed. With
/// metrics off (the default, and the state every `perf`-based measurement runs in)
/// this function returns the stream UNWRAPPED, so the framing path is byte- and
/// instruction-identical to before: the cost is one `metrics_active()` branch per
/// RPC, not per poll.
fn time_encoder_framing(inner: DoGetStream, rpc_span: tracing::Span) -> DoGetStream {
    if !cqlite_core::observability::metrics_active() {
        return inner;
    }
    let timings = Arc::new(cqlite_core::observability::StreamSubPhaseTimings::default());
    Box::pin(FramingTimedStream {
        // Its OWN emitter, deliberately not the merge closure's: this accumulator
        // is still being written after that closure has ended (the client may poll
        // the last frames later), and reusing the merge emitter would either
        // truncate the framing total or move the merge samples' emission point,
        // breaking the #2819 roborev-B1 ordering invariant. Only the framing bucket
        // is ever non-zero here, so this emits exactly one extra sample per RPC.
        // Written first to MIRROR the load-bearing declaration order below.
        _emitter: crate::obs::StreamSubPhaseEmitter::new(rpc_span, timings.clone()),
        inner,
        timings,
    })
}

/// See [`time_encoder_framing`]. Constructed only when a meter is installed.
///
/// FIELD ORDER IS LOAD-BEARING: struct fields drop in DECLARATION order, so
/// `_emitter` is declared FIRST — it flushes the framing sample while `inner` (the
/// metered/encoded parent stream, whose own `Drop` finalizes the RPC's row/byte
/// accounting) is still alive. Declared AFTER `inner` the sample would land
/// OUTSIDE its parent stream's teardown, the ordering the #2819 roborev-B1
/// invariant forbids. Guarded by `streaming_framing_tests.rs`.
struct FramingTimedStream {
    /// Flushes the framing sample when the response stream is dropped — i.e. after
    /// the LAST poll, so a mid-stream client disconnect still records what it cost.
    /// Held purely for its `Drop`; underscore-prefixed so the lint knows that is
    /// deliberate rather than an unread field.
    _emitter: crate::obs::StreamSubPhaseEmitter,
    inner: DoGetStream,
    timings: Arc<cqlite_core::observability::StreamSubPhaseTimings>,
}

impl Stream for FramingTimedStream {
    type Item = Result<FlightData, Status>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        // Every field is `Unpin` (a `Pin<Box<dyn Stream>>`, an `Arc`, and an
        // emitter holding an `Arc` + a `Span`), so projecting through `get_mut` is
        // sound and needs no pin-projection macro.
        let this = self.get_mut();
        let start = std::time::Instant::now();
        let polled = this.inner.as_mut().poll_next(cx);
        this.timings.add_nanos(
            cqlite_core::observability::StreamSubPhase::EncodeFraming,
            cqlite_core::observability::stream_subphase::elapsed_nanos(start),
        );
        polled
    }
}

/// Materialize the (bounded) aggregate output and serve it as a stream, unchanged
/// in content (issue #1476: the aggregate route keeps materializing — one row per
/// group). The same accounting wrapper attributes rows/bytes at stream end.
///
/// `cancel` is the SAME flag threaded through the caller's eager-setup phase
/// (roborev F1) — see [`spawn_streaming`]'s doc for why one flag spans both
/// phases.
pub(crate) async fn build_aggregate_response(
    producer: MergeProducer,
    paths: Vec<PathBuf>,
    schema_ref: Arc<ArrowSchema>,
    metrics: RpcMetrics,
    cancel: CancelFlag,
    timer: crate::obs::PhaseTimer,
) -> Result<DoGetStream, (Status, RpcMetrics, crate::obs::AbortReason)> {
    // The aggregate route materializes its bounded per-group output — it never
    // enters a client `stream` phase (issue #2162). The caller transitioned
    // `resolve` → `merge_setup` before this call, so `timer` measures the merger
    // build + drain-to-accumulator under `merge_setup`; it is a local here, so it
    // drops (recording that `merge_setup` sample) on EVERY exit — success, a
    // materialization error, or a cancel — and records no `stream` sample.
    let _timer = timer;
    // Cancellation during materialization mirrors the pre-change guard-across-await.
    let mut guard = cancel.drop_guard();
    let merge_cancel = cancel;
    let result = tokio::task::spawn_blocking(move || {
        // Issue #2419 (WS2): account this flight-managed blocking task on
        // `cqlite.flight.blocking_tasks_in_use` for the whole closure (RAII drop
        // decrements on every exit path).
        let _blocking_guard = crate::saturation::BlockingTaskGuard::enter();
        producer.produce_from_resolved(paths, &merge_cancel)
    })
    .await;
    guard.disarm();

    let batches = match result {
        Ok(Ok(batches)) => batches,
        // Issue #2681: stamp the abort reason from the typed producer error
        // VARIANT (a cooperative cancel → client_cancel; a genuine merge/convert/
        // discovery fault → internal), never inferred from the gRPC code.
        Ok(Err(e)) => {
            let reason = crate::service::producer_error_abort_reason(&e);
            return Err((Status::from(e), metrics, reason));
        }
        Err(e) => {
            return Err((
                Status::internal(format!("aggregate merge task panicked: {e}")),
                metrics,
                crate::obs::AbortReason::Internal,
            ))
        }
    };

    // Issue #2821 non-goal, stated rather than implied: the aggregate route
    // materializes its bounded per-group output into a `Vec` and never passes
    // through a `BatchSink`, so no egress reservation applies to it. Its batches
    // therefore carry an INERT permit — bounded by GROUP count by construction.
    let iter = futures::stream::iter(
        batches
            .into_iter()
            .map(|b| Ok::<_, ProducerError>(CreditedBatch::uncredited(b))),
    );
    let probe = StreamProbe::default();
    let metered = MeteredDoGetStream::new(Box::pin(iter), metrics, None, probe.clone(), None, None);
    // DELIBERATE ASYMMETRY, not an omission (issue #3096 review): the encoder
    // stream is NOT wrapped in `time_encoder_framing` here, so the aggregate route
    // emits no `stream_encode_framing` sub-phase sample.
    //
    // `stream_encode_framing` is a sub-phase OF the `stream` phase, and this route
    // deliberately records no `stream` phase at all (issue #2162 — it materializes
    // one row per GROUP BY group under `merge_setup` and never enters a client
    // stream phase; the `_timer` above is what enforces that). Emitting a
    // sub-phase whose parent phase is absent would publish a sample no consumer of
    // `docs/reports/flight-metrics-reference.md` can attribute, and would break
    // the #2819 ordering invariant that a sub-phase's emission sits inside its
    // phase.
    //
    // The cost of the asymmetry is bounded and known: framing time on the
    // aggregate route is unattributed. That is acceptable because the route's
    // output is bounded by GROUP count — one row per group, so a handful of
    // batches — which is exactly why it is not a framing-throughput surface. If a
    // `stream` phase is ever introduced for aggregates, wire the framing timer at
    // the same time.
    Ok(encode_do_get(metered, schema_ref, probe))
}

/// Wrap a `RecordBatch` stream in the Flight encoder. `with_schema` emits the
/// Arrow schema as the first message even for an empty result, so a schema-only
/// response still carries the schema.
///
/// `probe` observes encoder-stage egress failures (issue #2193): an error raised
/// INSIDE the encoder is downstream of [`MeteredDoGetStream`], so it never hit
/// that stream's error arm — before this change it was neither logged nor
/// counted. `record_encoder_error` now routes it through the shared
/// error-observability hook and bumps `probe.errors_recorded`, so the failure is
/// visible and deterministically observable in tests.
// The stream item's `Err` is `tonic::Status`, whose size is fixed by the
// arrow-flight `FlightService`/`DoGetStream` contract; boxing it (clippy's
// suggestion) would violate the trait-mandated stream item type (#2856).
#[allow(clippy::result_large_err)]
fn encode_do_get(
    batch_stream: impl Stream<Item = Result<RecordBatch, FlightError>> + Send + 'static,
    schema_ref: Arc<ArrowSchema>,
    probe: StreamProbe,
) -> DoGetStream {
    // Issue #3096, lever 4 REVERTED (owner ruling A, deferred to #3281): the
    // encoder's re-slicing target is arrow-flight's own default again — no
    // `with_max_flight_data_size`, no wire-side partitioner, no serialized-message
    // guard. Lever 4 measured ZERO throughput (-0.03%), and the wire-safety
    // mechanism that was its only remaining justification conflated a TARGET with a
    // CEILING: a reserve that is safe to over-estimate against a target
    // false-REJECTS legal input against a ceiling.
    let encoded = FlightDataEncoderBuilder::new()
        .with_schema(schema_ref)
        .build(batch_stream)
        .map(move |res| res.map_err(|e| flight_error_to_status(e, &probe)));
    Box::pin(encoded)
}

/// Recover a [`Status`] from an encoder [`FlightError`], preserving the gRPC code
/// of a producer error surfaced mid-stream (e.g. `aborted` for a cancelled merge,
/// `not_found`, `invalid_argument`) instead of flattening everything to internal.
///
/// Issue #2193 — surface the swallowed egress-encode/send failure: an error
/// raised INSIDE the Flight encoder (Arrow IPC framing, batch encoding, or the
/// send itself) reached this mapping and went straight to the client as a gRPC
/// status with NO server-side log and NO entry in the flight error signal — the
/// exact silent path that let a `do_get` stream close with the client's
/// `Failed to read message` while the server logged nothing even at
/// `RUST_LOG=debug`. Route those encoder-stage errors through
/// [`crate::obs::record_status_error`] (which logs at error level and bumps the
/// error-rate signal). A producer/merge error is NOT re-recorded here: it
/// arrives as `ExternalError(Box<Status>)`, already logged + recorded by
/// [`MeteredDoGetStream`]'s error arm, so we only unwrap it — double-recording
/// would double-count the error signal.
fn flight_error_to_status(e: FlightError, probe: &StreamProbe) -> Status {
    match e {
        FlightError::ExternalError(boxed) => match boxed.downcast::<Status>() {
            // Already observed upstream (MeteredDoGetStream) — just recover it.
            Ok(status) => *status,
            // A non-`Status` external error out of the encoder — not yet observed.
            Err(other) => record_encoder_error(Status::internal(other.to_string()), probe),
        },
        // Every other `FlightError` variant is raised by the encoder itself
        // (Arrow/IPC/decode/… ) — the swallowed egress class. Observe it.
        other => record_encoder_error(Status::internal(other.to_string()), probe),
    }
}

/// Log + record an encoder-stage egress failure (issue #2193) and return the
/// same [`Status`] for propagation to the client, so the failure is both visible
/// server-side (via [`crate::obs::record_status_error`]'s error log + error
/// signal) AND delivered as a proper gRPC error status.
fn record_encoder_error(status: Status, probe: &StreamProbe) -> Status {
    // Issue #2681: an encoder-stage egress failure (Arrow IPC framing / encode /
    // send) is a genuine internal fault — stamp `internal` at the site.
    crate::obs::record_do_get_abort(
        &status,
        crate::obs::AbortReason::Internal,
        crate::obs::AbortContext::empty(),
    );
    probe.errors_recorded.fetch_add(1, Ordering::Relaxed);
    status
}

impl StreamProbe {
    /// Store the emitted-prefix totals at finalization (the drain side's
    /// terminal bookkeeping — see [`MeteredDoGetStream`]).
    pub(crate) fn store_terminal_totals(&self, rows: u64, bytes: u64) {
        self.rows.store(rows, Ordering::Relaxed);
        self.bytes.store(bytes, Ordering::Relaxed);
    }

    /// Publish mid-stream progress as one batch passes toward the client.
    pub(crate) fn record_batch_emitted(&self, running_rows: u64) {
        self.progressed_rows.store(running_rows, Ordering::Relaxed);
        self.emitted_batches.fetch_add(1, Ordering::Relaxed);
    }

    /// Count one surfaced egress error alongside the shared observability hook.
    pub(crate) fn record_error(&self) {
        self.errors_recorded.fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
#[path = "streaming_tests.rs"]
mod tests;

// Per-stream in-flight egress byte-budget behaviour (issue #2821), in its own
// test module so `streaming_tests.rs` stays under the campsite test threshold
// (epic #1135).
#[cfg(test)]
#[path = "egress_budget_tests.rs"]
mod egress_budget_tests;

// Teardown-ORDER guard for `FramingTimedStream`'s load-bearing field order
// (issue #3096); its own module so the two files above stay put (epic #1135).
#[cfg(test)]
#[path = "streaming_framing_tests.rs"]
mod streaming_framing_tests;
