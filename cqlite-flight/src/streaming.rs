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
/// Peak resident record-batch payload is bounded to roughly `(K + 2) · batch_size`
/// — `K` batches queued in the channel, one being built by the merge, and one held
/// in the encoder — regardless of the total result size. Deliberately a small
/// named constant, not a config knob: the 2026-07 platform audit's "don't add
/// tunables without a consumer" lesson applies; issue #2162 can motivate one later.
pub(crate) const DO_GET_CHANNEL_CAPACITY: usize = 4;

/// Test-only observability handle for the streaming path.
///
/// The counters are always maintained (the writes are cheap `Relaxed` atomics),
/// so production simply passes a throwaway [`StreamProbe::default`]. Tests inject
/// their own to assert the memory bound (batches produced) and metrics
/// attribution (rows/bytes fed to [`RpcMetrics`]) without reaching into private
/// state.
#[derive(Clone, Default)]
pub(crate) struct StreamProbe {
    /// Batches the merge has pushed toward the channel (bounded by the backpressure).
    produced_batches: Arc<AtomicUsize>,
    /// Rows attributed to metrics at stream end (== emitted prefix on cancel).
    rows: Arc<AtomicU64>,
    /// Payload bytes attributed to metrics at stream end.
    bytes: Arc<AtomicU64>,
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
    let handle = tokio::task::spawn_blocking(move || {
        let mut sink = ChannelSink { tx, produced };
        if let Err(e) = producer.produce_streaming(paths, &merge_cancel, &mut sink) {
            // Forward a terminal error to the client (matches delta_scan error
            // forwarding). Ignored if the receiver is already gone (client left).
            let _ = sink.tx.blocking_send(Err(e));
        }
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
mod tests {
    use super::*;
    use crate::producer::DirSource;
    use crate::testutil::{build_sstables, simple_schema, write_row};
    use cqlite_core::schema::TableSchema;

    /// Build a fixture with `n` single-row partitions across two SSTables so the
    /// merge has real per-partition work and, at `batch_size = 1`, yields `n`
    /// batches.
    fn many_partition_fixture(n: i32) -> (tempfile::TempDir, std::path::PathBuf, TableSchema) {
        let schema = simple_schema();
        let half = n / 2;
        let a: Vec<_> = (1..=half).map(|i| write_row(i, "a", i, 100)).collect();
        let b: Vec<_> = (half + 1..=n).map(|i| write_row(i, "b", i, 100)).collect();
        let (temp, _data, dir) = build_sstables(&schema, vec![a, b]);
        (temp, dir, schema)
    }

    /// Resolve the pruned paths for a producer over `dir` (mirrors the service's
    /// eager `resolve_paths` step).
    fn resolved(producer: &MergeProducer, dir: &std::path::Path) -> Vec<PathBuf> {
        producer.resolve_paths(&DirSource::new(dir)).unwrap()
    }

    fn probe() -> StreamProbe {
        StreamProbe::default()
    }

    /// Read exactly one item from the response stream (the first Flight message is
    /// the schema; the second carries the first record batch), proving a batch is
    /// available before the merge has run to completion.
    async fn read_one(stream: &mut DoGetStream) -> Option<Result<FlightData, Status>> {
        stream.next().await
    }

    // ---- Task 1.1: do_get emits batches incrementally --------------------------

    /// The first batch is available while the merge is still running: after pulling
    /// one message, the producer has emitted far fewer than the full-scan batch
    /// count (bounded by the channel capacity + in-flight allowance).
    ///
    /// FAILS on pre-change `main`: there is no `spawn_streaming`/streaming producer
    /// there — `do_get` materializes every batch before the first is available, so
    /// the produced count would equal the full result.
    #[test]
    fn first_batch_available_before_merge_completes() {
        let n = 40;
        let (_temp, dir, schema) = many_partition_fixture(n);
        let producer = MergeProducer::with_spec(
            schema,
            1, // batch_size = 1 → one batch per partition row
            crate::filter::ScanSpec::default(),
        )
        .unwrap();
        let paths = resolved(&producer, &dir);
        let schema_ref = Arc::new(producer.arrow_schema().unwrap());

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let pr = probe();
            let (mut stream, handle) = spawn_streaming(
                producer,
                paths,
                schema_ref,
                RpcMetrics::start("do_get"),
                DO_GET_CHANNEL_CAPACITY,
                pr.clone(),
            );
            // Pull the first message (schema) then the first batch.
            let _schema_msg = read_one(&mut stream).await.expect("schema message");
            let _first_batch = read_one(&mut stream).await.expect("first batch");

            let produced = pr.produced_batches.load(Ordering::Relaxed);
            assert!(
                produced < n as usize,
                "streaming must not materialize all {n} batches before batch 1 \
                 (produced={produced})"
            );
            assert!(
                produced <= DO_GET_CHANNEL_CAPACITY + 3,
                "producer must stay within the channel bound (produced={produced})"
            );

            drop(stream);
            let _ = handle.await;
        });
    }

    // ---- Task 1.2: peak resident payload is bounded ----------------------------

    /// A slow consumer that reads one batch and pauses does not let the producer
    /// run ahead: it blocks after at most capacity + in-flight allowance batches,
    /// independent of the total result size.
    ///
    /// FAILS on pre-change `main`: all batches materialize regardless of consumer
    /// progress (no bounded channel exists).
    #[test]
    fn slow_consumer_bounds_produced_batches() {
        let n = 60;
        let (_temp, dir, schema) = many_partition_fixture(n);
        let producer =
            MergeProducer::with_spec(schema, 1, crate::filter::ScanSpec::default()).unwrap();
        let paths = resolved(&producer, &dir);
        let schema_ref = Arc::new(producer.arrow_schema().unwrap());

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let pr = probe();
            let (mut stream, handle) = spawn_streaming(
                producer,
                paths,
                schema_ref,
                RpcMetrics::start("do_get"),
                DO_GET_CHANNEL_CAPACITY,
                pr.clone(),
            );
            let _schema_msg = read_one(&mut stream).await.expect("schema");
            let _first = read_one(&mut stream).await.expect("first batch");

            // Give the producer every opportunity to run ahead, then assert it is
            // still bounded — it physically cannot exceed the channel allowance.
            tokio::task::yield_now().await;
            let produced = pr.produced_batches.load(Ordering::Relaxed);
            assert!(
                produced <= DO_GET_CHANNEL_CAPACITY + 3,
                "slow consumer: produced {produced} exceeds the channel bound"
            );

            drop(stream);
            let _ = handle.await;
        });
    }

    // ---- Task 1.3: consumer disconnect stops the merge -------------------------

    /// Dropping the response stream after the first batch stops the merge: the
    /// producer never emits all partitions and the blocking task exits.
    ///
    /// FAILS on pre-change `main`: `do_get` runs the merge to completion before the
    /// stream exists, so a drop cannot cut it short.
    #[test]
    fn dropping_stream_cancels_merge() {
        let n = 60;
        let (_temp, dir, schema) = many_partition_fixture(n);
        let producer =
            MergeProducer::with_spec(schema, 1, crate::filter::ScanSpec::default()).unwrap();
        let paths = resolved(&producer, &dir);
        let schema_ref = Arc::new(producer.arrow_schema().unwrap());

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let pr = probe();
            let (mut stream, handle) = spawn_streaming(
                producer,
                paths,
                schema_ref,
                RpcMetrics::start("do_get"),
                DO_GET_CHANNEL_CAPACITY,
                pr.clone(),
            );
            let _schema_msg = read_one(&mut stream).await.expect("schema");
            let _first = read_one(&mut stream).await.expect("first batch");

            // Client disconnects: drop the stream, then await the merge task. It
            // must terminate (send failure + cancel flag) rather than run to
            // completion.
            drop(stream);
            handle.await.expect("merge task joins after cancellation");

            let produced = pr.produced_batches.load(Ordering::Relaxed);
            assert!(
                produced < n as usize,
                "merge must stop early on disconnect (produced={produced} of {n})"
            );
        });
    }

    // ---- Task 2.3 / Requirement 4: stream/collect byte-identity ----------------

    fn collect_batches(producer: &MergeProducer, dir: &std::path::Path) -> Vec<RecordBatch> {
        producer.produce(&DirSource::new(dir)).unwrap()
    }

    async fn drain_stream(stream: DoGetStream) -> Vec<RecordBatch> {
        use arrow_flight::decode::FlightRecordBatchStream;
        let mapped = stream.map(|r| r.map_err(|s| FlightError::ExternalError(Box::new(s))));
        let mut rb = FlightRecordBatchStream::new_from_flight_data(mapped);
        let mut out = Vec::new();
        while let Some(batch) = rb.next().await {
            out.push(batch.expect("decode batch"));
        }
        out
    }

    /// Drive the streaming producer path and collect the raw batches it emits into
    /// the channel (pre-encode). This is the true parity seam the spec names —
    /// `produce_streaming` vs the retained `produce` collect path — without the
    /// Flight encoder's response-schema injection (a uniform presentation concern
    /// applied identically to every `do_get`).
    fn stream_batches_raw(producer: MergeProducer, paths: Vec<PathBuf>) -> Vec<RecordBatch> {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let (tx, mut rx) =
                mpsc::channel::<Result<RecordBatch, ProducerError>>(DO_GET_CHANNEL_CAPACITY);
            let cancel = CancelFlag::new();
            let handle = tokio::task::spawn_blocking(move || {
                let mut sink = ChannelSink {
                    tx,
                    produced: Arc::new(AtomicUsize::new(0)),
                };
                if let Err(e) = producer.produce_streaming(paths, &cancel, &mut sink) {
                    let _ = sink.tx.blocking_send(Err(e));
                }
            });
            let mut out = Vec::new();
            while let Some(item) = rx.recv().await {
                out.push(item.expect("streamed batch is ok"));
            }
            let _ = handle.await;
            out
        })
    }

    /// The streamed batches are byte-identical to the collect-path batches for a
    /// given spec: same schema, batch boundaries, and row content.
    fn assert_stream_collect_parity(spec: crate::filter::ScanSpec, dir: &std::path::Path) {
        let schema = simple_schema();
        let collect_producer = MergeProducer::with_spec(schema.clone(), 4, spec.clone()).unwrap();
        let expected = collect_batches(&collect_producer, dir);

        let stream_producer = MergeProducer::with_spec(schema, 4, spec).unwrap();
        let paths = resolved(&stream_producer, dir);
        let streamed = stream_batches_raw(stream_producer, paths);

        assert_eq!(
            streamed.len(),
            expected.len(),
            "batch count (boundaries) must match"
        );
        for (s, e) in streamed.iter().zip(expected.iter()) {
            assert_eq!(
                s, e,
                "streamed batch must be byte-identical to collect batch"
            );
        }
    }

    #[test]
    fn stream_collect_parity_no_constraints() {
        let schema = simple_schema();
        let rows = (1..=10)
            .map(|i| write_row(i, &format!("n{i}"), i * 10, 100))
            .collect::<Vec<_>>();
        let (_temp, _data, dir) = build_sstables(&schema, vec![rows]);
        assert_stream_collect_parity(crate::filter::ScanSpec::default(), &dir);
    }

    #[test]
    fn stream_collect_parity_limit_mid_batch() {
        let schema = simple_schema();
        let rows = (1..=10)
            .map(|i| write_row(i, &format!("n{i}"), i * 10, 100))
            .collect::<Vec<_>>();
        let (_temp, _data, dir) = build_sstables(&schema, vec![rows]);
        let spec = crate::filter::ScanSpec {
            limit: Some(5),
            ..Default::default()
        };
        assert_stream_collect_parity(spec, &dir);
    }

    #[test]
    fn stream_collect_parity_predicate() {
        use crate::ticket::{FlightTicket, Predicate, PredicateOp};
        let schema = simple_schema();
        let rows = (1..=10)
            .map(|i| write_row(i, &format!("n{i}"), i * 10, 100))
            .collect::<Vec<_>>();
        let (_temp, _data, dir) = build_sstables(&schema, vec![rows]);
        let ticket = FlightTicket {
            predicates: vec![Predicate {
                column: "score".into(),
                op: PredicateOp::Gte,
                value: serde_json::json!(40),
            }],
            ..Default::default()
        };
        let spec = crate::filter::ScanSpec::from_ticket(&ticket, &schema).unwrap();
        assert_stream_collect_parity(spec, &dir);
    }

    #[test]
    fn stream_collect_parity_token_range() {
        use crate::ticket::FlightTicket;
        let schema = simple_schema();
        let rows = (1..=10)
            .map(|i| write_row(i, &format!("n{i}"), i * 10, 100))
            .collect::<Vec<_>>();
        let (_temp, _data, dir) = build_sstables(&schema, vec![rows]);
        let ticket = FlightTicket {
            token_start: Some(i64::MIN),
            token_end: Some(0),
            ..Default::default()
        };
        let spec = crate::filter::ScanSpec::from_ticket(&ticket, &schema).unwrap();
        assert_stream_collect_parity(spec, &dir);
    }

    // ---- Requirement 5: rows/bytes metrics reflect what was emitted ------------

    /// A fully-consumed stream attributes the same rows/bytes the collect path
    /// would for the same ticket.
    #[test]
    fn metrics_parity_on_full_consumption() {
        let schema = simple_schema();
        let rows = (1..=10)
            .map(|i| write_row(i, &format!("n{i}"), i * 10, 100))
            .collect::<Vec<_>>();
        let (_temp, _data, dir) = build_sstables(&schema, vec![rows]);

        let producer =
            MergeProducer::with_spec(schema.clone(), 4, crate::filter::ScanSpec::default())
                .unwrap();
        let expected = collect_batches(&producer, &dir);
        let expected_rows: u64 = expected.iter().map(|b| b.num_rows() as u64).sum();
        let expected_bytes: u64 = expected
            .iter()
            .map(|b| b.get_array_memory_size() as u64)
            .sum();

        let stream_producer =
            MergeProducer::with_spec(schema, 4, crate::filter::ScanSpec::default()).unwrap();
        let paths = resolved(&stream_producer, &dir);
        let schema_ref = Arc::new(stream_producer.arrow_schema().unwrap());

        let rt = tokio::runtime::Runtime::new().unwrap();
        let pr = probe();
        let pr_check = pr.clone();
        rt.block_on(async move {
            let (stream, handle) = spawn_streaming(
                stream_producer,
                paths,
                schema_ref,
                RpcMetrics::start("do_get"),
                DO_GET_CHANNEL_CAPACITY,
                pr,
            );
            let _ = drain_stream(stream).await;
            let _ = handle.await;
        });

        assert_eq!(
            pr_check.rows.load(Ordering::Relaxed),
            expected_rows,
            "fully-consumed rows must match the collect path"
        );
        assert_eq!(
            pr_check.bytes.load(Ordering::Relaxed),
            expected_bytes,
            "fully-consumed bytes must match the collect path"
        );
        assert!(expected_rows > 0, "fixture must produce rows");
    }

    // ---- Requirement 6: aggregate path keeps materializing (unchanged content) -

    /// An aggregation ticket over a multi-SSTable table serves its bounded
    /// per-group output as a stream, byte-identical to the retained collect path
    /// (`produce`). The aggregate route does NOT stream row-by-row — it keeps
    /// materializing — but is still wrapped in a stream.
    #[test]
    fn aggregate_path_matches_collect_content() {
        use crate::ticket::{AggFunc, AggregateSpec, Aggregation};
        let schema = simple_schema();
        // Two SSTables, 7 distinct partitions total after LWW (id=1 rewritten).
        let (_temp, dir, _schema) = {
            let a: Vec<_> = (1..=4).map(|i| write_row(i, "a", i, 100)).collect();
            let b: Vec<_> = (4..=7).map(|i| write_row(i, "b", i, 200)).collect();
            let (t, _d, dir) = build_sstables(&schema, vec![a, b]);
            (t, dir, schema.clone())
        };
        let agg = Aggregation {
            group_by: vec![],
            aggregates: vec![AggregateSpec {
                func: AggFunc::Count,
                column: None,
                output: "cnt".into(),
            }],
        };

        // Collect path (the oracle): produce the partial aggregate directly.
        let collect_producer = MergeProducer::with_spec(schema.clone(), 4, Default::default())
            .unwrap()
            .with_aggregation(&agg)
            .unwrap();
        let expected = collect_producer.produce(&DirSource::new(&dir)).unwrap();
        assert_eq!(
            expected.len(),
            1,
            "global aggregation emits one partial batch"
        );
        let expected_cnt = count_value(&expected[0]);

        // Streaming service path (build_aggregate_response) — same content.
        let stream_producer = MergeProducer::with_spec(schema, 4, Default::default())
            .unwrap()
            .with_aggregation(&agg)
            .unwrap();
        assert!(stream_producer.is_aggregating());
        let paths = resolved(&stream_producer, &dir);
        let schema_ref = Arc::new(stream_producer.arrow_schema().unwrap());

        let rt = tokio::runtime::Runtime::new().unwrap();
        let streamed = rt.block_on(async move {
            let stream = match build_aggregate_response(
                stream_producer,
                paths,
                schema_ref,
                RpcMetrics::start("do_get"),
            )
            .await
            {
                Ok(stream) => stream,
                Err((status, _metrics)) => panic!("aggregate response failed: {status}"),
            };
            drain_stream(stream).await
        });
        assert_eq!(streamed.len(), 1, "aggregate output stays one batch");
        assert_eq!(
            count_value(&streamed[0]),
            expected_cnt,
            "streamed aggregate content matches the collect path"
        );
    }

    /// Read the single global-`count(*)` partial value from a one-row batch.
    fn count_value(batch: &RecordBatch) -> i64 {
        use arrow::array::Array;
        let col = batch.column_by_name("cnt").expect("cnt column");
        col.as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("count is Int64")
            .value(0)
    }

    /// A cancelled stream (one batch read, then dropped) attributes exactly the
    /// emitted prefix — not the full table.
    #[test]
    fn metrics_attribute_emitted_prefix_on_cancel() {
        let n = 60;
        let (_temp, dir, schema) = many_partition_fixture(n);
        let producer =
            MergeProducer::with_spec(schema, 1, crate::filter::ScanSpec::default()).unwrap();
        let paths = resolved(&producer, &dir);
        let schema_ref = Arc::new(producer.arrow_schema().unwrap());

        let rt = tokio::runtime::Runtime::new().unwrap();
        let pr = probe();
        let pr_check = pr.clone();
        rt.block_on(async move {
            let (mut stream, handle) = spawn_streaming(
                producer,
                paths,
                schema_ref,
                RpcMetrics::start("do_get"),
                DO_GET_CHANNEL_CAPACITY,
                pr,
            );
            let _schema_msg = read_one(&mut stream).await.expect("schema");
            let _first = read_one(&mut stream).await.expect("first batch");
            drop(stream);
            let _ = handle.await;
        });

        // The metered stream yielded exactly one batch (one row at batch_size=1)
        // before the drop, so metrics attribute that single-row prefix — far below
        // the full table.
        let rows = pr_check.rows.load(Ordering::Relaxed);
        assert_eq!(
            rows, 1,
            "cancelled stream attributes only the emitted prefix"
        );
        assert!(rows < n as u64);
    }
}
