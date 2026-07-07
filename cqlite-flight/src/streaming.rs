//! Streaming `do_get` egress (issue #1476, AB1).
//!
//! `do_get` no longer runs the whole compaction merge to completion and collects
//! every [`RecordBatch`] into a `Vec` before the first byte reaches the client.
//! Instead the merge runs on the blocking pool and sends each batch into a bounded
//! [`tokio::sync::mpsc`] channel as it is produced; the gRPC response wraps the
//! receiver. First batches reach the wire while the merge is still running, and
//! peak resident payload is bounded to the channel capacity + a small in-flight
//! allowance — independent of the total result size. This mirrors the proven
//! `cqlite-core` `delta_scan/scan.rs` bounded-channel pattern.
//!
//! The retained `produce`/`produce_cancellable` collect path remains the
//! byte-identity parity oracle and serves the aggregate route (bounded output).

use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::Schema as ArrowSchema;
use arrow::record_batch::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::FlightData;
use futures::{Stream, StreamExt};
use tokio::sync::mpsc;
use tonic::Status;

use crate::cancel::{CancelFlag, CancelGuard};
use crate::obs::RpcMetrics;
use crate::producer::{BatchSink, MergeProducer, ProducerError};

/// Boxed server response stream (matches `service::BoxStream<FlightData>`).
pub(crate) type DoGetStream =
    Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;

/// `do_get` channel capacity, in batches.
///
/// Peak resident record-batch payload is bounded to roughly
/// `(DO_GET_CHANNEL_CAPACITY + IN_FLIGHT_ALLOWANCE) · batch_size`, regardless of
/// the total result size. Deliberately a small named constant, not a config
/// knob: the 2026-07 platform audit's "don't add tunables without a consumer"
/// lesson applies; issue #2162 can motivate one later.
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
    /// Incremented once per mid-stream error, alongside the
    /// `crate::obs::record_status_error` call (roborev B2) — proves the error
    /// arm reaches the same error-observability hook `do_get`'s eager-setup
    /// errors always did, independent of whether OTel counters are compiled in.
    errors_recorded: Arc<AtomicUsize>,
}

/// Sink that sends each merged batch into the bounded `do_get` channel.
///
/// A send failure means the receiver (client) is gone: report
/// [`ProducerError::Cancelled`] so the merge stops within a bounded number of
/// steps (composing with the #1473 `CancelFlag`).
struct ChannelSink {
    tx: mpsc::Sender<Result<RecordBatch, ProducerError>>,
    produced: Arc<AtomicUsize>,
}

impl BatchSink for ChannelSink {
    fn emit(&mut self, batch: RecordBatch) -> Result<(), ProducerError> {
        self.produced.fetch_add(1, Ordering::Relaxed);
        // `blocking_send` applies backpressure: it parks this blocking-pool thread
        // while the channel is full, so the merge never runs ahead of the consumer
        // by more than the channel capacity. `Err` == receiver dropped == cancel.
        self.tx
            .blocking_send(Ok(batch))
            .map_err(|_| ProducerError::Cancelled)
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
fn run_merge_catching_panics<F>(tx: &mpsc::Sender<Result<RecordBatch, ProducerError>>, run: F)
where
    F: FnOnce() -> Result<(), ProducerError>,
{
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(run)) {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            // Forward a terminal error to the client (matches delta_scan error
            // forwarding). Ignored if the receiver is already gone (client left).
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
/// so the fallible discovery/prune has already surfaced upstream.
pub(crate) fn spawn_streaming(
    producer: MergeProducer,
    paths: Vec<PathBuf>,
    schema_ref: Arc<ArrowSchema>,
    metrics: RpcMetrics,
    capacity: usize,
    probe: StreamProbe,
) -> (DoGetStream, tokio::task::JoinHandle<()>) {
    let (tx, rx) = mpsc::channel::<Result<RecordBatch, ProducerError>>(capacity.max(1));
    // The shared cancel flag stops the merge when this response stream is dropped
    // (client disconnect); `blocking_send` failure is the second, independent stop
    // signal. AA3 machinery (#1473) is reused, not replaced.
    let cancel = CancelFlag::new();
    let guard = cancel.drop_guard();
    let merge_cancel = cancel;
    let produced = probe.produced_batches.clone();

    // Run the CPU-bound merge off the async runtime; it sends batches as it goes.
    // `error_tx` is a clone kept OUTSIDE the (potentially unwound) merge closure so
    // a panic can still report through it — `sink` (holding the other clone) lives
    // inside the closure catch_unwind guards, and unwinding drops it.
    let handle = tokio::task::spawn_blocking(move || {
        let error_tx = tx.clone();
        let mut sink = ChannelSink { tx, produced };
        run_merge_catching_panics(&error_tx, move || {
            producer.produce_streaming(paths, &merge_cancel, &mut sink)
        });
    });

    let inner = Box::pin(ReceiverStream { rx });
    let metered = MeteredDoGetStream::new(inner, metrics, Some(guard), probe);
    (encode_do_get(metered, schema_ref), handle)
}

/// Materialize the (bounded) aggregate output and serve it as a stream, unchanged
/// in content (issue #1476: the aggregate route keeps materializing — one row per
/// group). The same accounting wrapper attributes rows/bytes at stream end.
pub(crate) async fn build_aggregate_response(
    producer: MergeProducer,
    paths: Vec<PathBuf>,
    schema_ref: Arc<ArrowSchema>,
    metrics: RpcMetrics,
) -> Result<DoGetStream, (Status, RpcMetrics)> {
    // Cancellation during materialization mirrors the pre-change guard-across-await.
    let cancel = CancelFlag::new();
    let mut guard = cancel.drop_guard();
    let merge_cancel = cancel;
    let result =
        tokio::task::spawn_blocking(move || producer.produce_from_resolved(paths, &merge_cancel))
            .await;
    guard.disarm();

    let batches = match result {
        Ok(Ok(batches)) => batches,
        Ok(Err(e)) => return Err((Status::from(e), metrics)),
        Err(e) => {
            return Err((
                Status::internal(format!("aggregate merge task panicked: {e}")),
                metrics,
            ))
        }
    };

    let iter = futures::stream::iter(batches.into_iter().map(Ok::<_, ProducerError>));
    let metered = MeteredDoGetStream::new(Box::pin(iter), metrics, None, StreamProbe::default());
    Ok(encode_do_get(metered, schema_ref))
}

/// Wrap a `RecordBatch` stream in the Flight encoder. `with_schema` emits the
/// Arrow schema as the first message even for an empty result, so a schema-only
/// response still carries the schema.
fn encode_do_get(
    batch_stream: impl Stream<Item = Result<RecordBatch, FlightError>> + Send + 'static,
    schema_ref: Arc<ArrowSchema>,
) -> DoGetStream {
    let encoded = FlightDataEncoderBuilder::new()
        .with_schema(schema_ref)
        .build(batch_stream)
        .map(|res| res.map_err(flight_error_to_status));
    Box::pin(encoded)
}

/// Recover a [`Status`] from an encoder [`FlightError`], preserving the gRPC code
/// of a producer error surfaced mid-stream (e.g. `aborted` for a cancelled merge,
/// `not_found`, `invalid_argument`) instead of flattening everything to internal.
fn flight_error_to_status(e: FlightError) -> Status {
    match e {
        FlightError::ExternalError(boxed) => match boxed.downcast::<Status>() {
            Ok(status) => *status,
            Err(other) => Status::internal(other.to_string()),
        },
        other => Status::internal(other.to_string()),
    }
}

/// Minimal [`Stream`] adapter over a bounded [`mpsc::Receiver`] (the crate has no
/// `tokio-stream` dependency; `poll_recv` is all we need).
struct ReceiverStream<T> {
    rx: mpsc::Receiver<T>,
}

impl<T> Stream for ReceiverStream<T> {
    type Item = T;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        self.rx.poll_recv(cx)
    }
}

/// Accounting + cancellation wrapper around the `do_get` batch stream.
///
/// Accumulates rows/bytes per batch as they pass toward the encoder and attributes
/// them to [`RpcMetrics`] at stream end — a fully-consumed stream records the same
/// totals the collect path would; a stream dropped early (client disconnect)
/// attributes exactly the emitted prefix. It also owns the merge's [`CancelGuard`]
/// so dropping the response stream cancels the merge (issue #1476 / #1473).
struct MeteredDoGetStream {
    inner: Pin<Box<dyn Stream<Item = Result<RecordBatch, ProducerError>> + Send + 'static>>,
    /// Taken (and dropped, emitting terminal metrics) exactly once at finalization.
    metrics: Option<RpcMetrics>,
    /// Cancels the merge on drop unless disarmed; `None` for the aggregate route
    /// (already materialized — nothing to cancel).
    guard: Option<CancelGuard>,
    probe: StreamProbe,
    rows: u64,
    bytes: u64,
    errored: bool,
}

impl MeteredDoGetStream {
    fn new(
        inner: Pin<Box<dyn Stream<Item = Result<RecordBatch, ProducerError>> + Send + 'static>>,
        metrics: RpcMetrics,
        guard: Option<CancelGuard>,
        probe: StreamProbe,
    ) -> Self {
        Self {
            inner,
            metrics: Some(metrics),
            guard,
            probe,
            rows: 0,
            bytes: 0,
            errored: false,
        }
    }

    /// Attribute the accumulated rows/bytes to metrics (and the probe) and record
    /// the terminal RPC status. Idempotent: `metrics` is taken exactly once, so a
    /// normal end followed by drop (or vice versa) records only once.
    fn finalize(&mut self, ok: bool) {
        if let Some(mut metrics) = self.metrics.take() {
            metrics.add_rows_bytes(self.rows, self.bytes);
            if ok {
                metrics.ok();
            }
            self.probe.rows.store(self.rows, Ordering::Relaxed);
            self.probe.bytes.store(self.bytes, Ordering::Relaxed);
            // Dropping `metrics` here emits the terminal RPC counters/histogram.
        }
    }

    /// The merge is finished (completed or errored); disarm so we do not cancel a
    /// flag whose task has already exited.
    fn disarm_guard(&mut self) {
        if let Some(guard) = self.guard.as_mut() {
            guard.disarm();
        }
    }
}

impl Stream for MeteredDoGetStream {
    type Item = Result<RecordBatch, FlightError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                this.rows = this.rows.saturating_add(batch.num_rows() as u64);
                this.bytes = this
                    .bytes
                    .saturating_add(batch.get_array_memory_size() as u64);
                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(Some(Err(err))) => {
                this.errored = true;
                this.disarm_guard();
                this.finalize(false);
                let status = Status::from(err);
                // Roborev B2: pre-change `do_get` ran `record_status_error` for
                // EVERY error (service.rs `finish`); a mid-stream error must hit the
                // same error-observability path, not just the per-RPC OK/error flag.
                crate::obs::record_status_error(&status);
                this.probe.errors_recorded.fetch_add(1, Ordering::Relaxed);
                Poll::Ready(Some(Err(FlightError::ExternalError(Box::new(status)))))
            }
            Poll::Ready(None) => {
                this.disarm_guard();
                this.finalize(!this.errored);
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for MeteredDoGetStream {
    fn drop(&mut self) {
        // Early drop (client disconnect): attribute the emitted prefix, then the
        // still-armed guard cancels the merge. `finalize` is idempotent, so a
        // stream that already ended normally records nothing more here.
        self.finalize(false);
    }
}

#[cfg(test)]
#[path = "streaming_tests.rs"]
mod tests;
