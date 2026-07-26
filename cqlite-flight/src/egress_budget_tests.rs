//! Per-stream in-flight egress byte-budget tests (issue #2821).
//!
//! Driven through the REAL streaming egress (`spawn_streaming` → `ChannelSink` →
//! the bounded channel → `MeteredDoGetStream`), over the merged synthetic
//! `wide_row_fixture` shapes built in process — never the fetched
//! `test_wide_rows` corpus, which would make every assertion here pass vacuously
//! in an unfetched checkout.
//!
//! **No assertion in this file compares an elapsed duration against a threshold**
//! (#2642 / `roborev-lints`). A slow consumer is simulated by withholding polls;
//! the two `sleep`/`timeout` uses are LIVENESS bounds on a parked blocking-pool
//! thread (the pattern the merged `cancelled_emit_under_backpressure_returns_cancelled`
//! already uses), never correctness thresholds.

use std::sync::atomic::AtomicUsize;

use super::*;
use crate::batch_bytes::worst_case_batch_capacity_bytes;
use crate::egress_credit::{DEFAULT_MAX_INFLIGHT_EGRESS_BYTES, EGRESS_CREDIT_QUANTUM_BYTES};
use crate::producer::DirSource;
use crate::testutil::build_sstables;
use crate::wide_row_fixture as fx;
use cqlite_core::schema::TableSchema;

/// 60 rows x 16 KiB of blob: ~1 MiB of payload, far more than any ceiling used
/// here, so every ceiling test streams many batches.
const WIDE_ROWS: i32 = 60;
const WIDE_PAYLOAD: usize = 16 * 1024;
/// Per-batch PAYLOAD cap (#2825 currency) — cuts the fixture into ~15 batches.
const WIDE_BATCH_CAP: usize = 64 * 1024;
/// Per-stream in-flight CAPACITY ceiling (#2821 currency) for the wide tests.
const WIDE_CEILING: usize = 192 * 1024;

fn timer() -> crate::obs::PhaseTimer {
    crate::obs::PhaseTimer::start("do_get")
}

/// Build the wide fixture as a real SSTable and return a producer over it.
fn wide_setup(
    batch_cap: usize,
) -> (
    tempfile::TempDir,
    MergeProducer,
    Vec<PathBuf>,
    Arc<ArrowSchema>,
) {
    let schema: TableSchema = fx::wide_row_schema();
    let (temp, _data, dir) = build_sstables(
        &schema,
        vec![fx::wide_row_mutations(WIDE_ROWS, WIDE_PAYLOAD)],
    );
    let producer = MergeProducer::with_spec(schema, 8192, crate::filter::ScanSpec::default())
        .expect("producer")
        .with_max_batch_bytes(batch_cap);
    let paths = producer
        .resolve_paths(&DirSource::new(&dir))
        .expect("resolve");
    let schema_ref = Arc::new(producer.arrow_schema().expect("arrow schema"));
    (temp, producer, paths, schema_ref)
}

/// Build the NARROW fixture (the non-regression shape) and a producer over it.
fn narrow_setup() -> (
    tempfile::TempDir,
    MergeProducer,
    Vec<PathBuf>,
    Arc<ArrowSchema>,
) {
    let schema: TableSchema = fx::narrow_row_schema();
    let (temp, _data, dir) = build_sstables(&schema, vec![fx::narrow_row_mutations(WIDE_ROWS * 4)]);
    // batch_size 1 makes the batch-count channel the observable governor, exactly
    // as `slow_consumer_bounds_produced_batches` does for the pre-#2821 bound.
    let producer =
        MergeProducer::with_spec(schema, 1, crate::filter::ScanSpec::default()).expect("producer");
    let paths = producer
        .resolve_paths(&DirSource::new(&dir))
        .expect("resolve");
    let schema_ref = Arc::new(producer.arrow_schema().expect("arrow schema"));
    (temp, producer, paths, schema_ref)
}

/// Decode every record batch off a real `do_get` response stream.
// The stream item's `Err` is `tonic::Status`, whose size is fixed by the
// arrow-flight `FlightService` contract (#2856).
#[allow(clippy::result_large_err)]
async fn decode_all(stream: DoGetStream) -> Vec<RecordBatch> {
    let mapped = stream.map(|r| r.map_err(|s| FlightError::ExternalError(Box::new(s))));
    let mut decoded = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(mapped);
    let mut out = Vec::new();
    while let Some(b) = decoded.next().await {
        out.push(b.expect("decode"));
    }
    out
}

/// The guaranteed contract, computed the SAME way the governor's doc states it.
fn contract_bytes(ceiling_bytes: u64, largest_observed_batch: u64) -> u64 {
    ceiling_bytes.max(largest_observed_batch)
}

// ---------------------------------------------------------------------------
// Requirement: peak resident payload is bounded, independent of row width
// ---------------------------------------------------------------------------

/// A slow consumer that reads ONE batch and stops polling cannot push per-stream
/// resident capacity past `max(ceiling, largest single batch)`. The wide fixture
/// is sized so the byte ceiling binds strictly before the 4-deep batch-count
/// channel does — asserted, so the test cannot go vacuous if the fixture drifts.
///
/// FAILS on pre-change `main`: there is no byte governor there at all, and
/// residency is bounded only by `DO_GET_CHANNEL_CAPACITY` batches of unbounded
/// width.
#[test]
fn slow_consumer_bounds_inflight_egress_capacity_bytes() {
    let (_temp, producer, paths, schema_ref) = wide_setup(WIDE_BATCH_CAP);

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        let pr = StreamProbe::default();
        let (mut stream, handle) = spawn_streaming(
            producer,
            MergeInput::Paths(paths),
            schema_ref,
            RpcMetrics::start("do_get"),
            DO_GET_CHANNEL_CAPACITY,
            EgressBudget::bytes(WIDE_CEILING),
            pr.clone(),
            CancelFlag::new(),
            timer(),
        );
        let _schema_msg = stream.next().await.expect("schema");
        let _first = stream.next().await.expect("first batch");
        // Give the producer every opportunity to run ahead, then observe.
        tokio::task::yield_now().await;

        let obs = &pr.egress;
        let largest = obs.largest_batch_capacity_bytes();
        let peak = obs.peak_resident_capacity_bytes();

        // Non-vacuity: real batches, and the byte ceiling — not the batch-count
        // channel — is what binds at this shape.
        assert!(largest > 0, "no batch was materialized (vacuous fixture)");
        assert!(peak > 0, "no residency was observed (vacuous fixture)");
        assert!(
            largest * DO_GET_CHANNEL_CAPACITY as u64 > WIDE_CEILING as u64,
            "fixture drift: {DO_GET_CHANNEL_CAPACITY} batches of {largest} B would fit \
             under the {WIDE_CEILING} B ceiling, so the count governor still binds first \
             and this assertion proves nothing"
        );

        // The contract.
        assert!(
            peak <= contract_bytes(WIDE_CEILING as u64, largest),
            "peak resident egress capacity {peak} B exceeds \
             max(ceiling {WIDE_CEILING}, largest batch {largest})"
        );
        // Reserve-before-materialize: no batch exists without credit held for it.
        assert!(
            obs.batches_materialized() <= obs.reservations_granted(),
            "materialized {} batches under {} reservations — a materialized \
             batch existed without credit",
            obs.batches_materialized(),
            obs.reservations_granted()
        );

        drop(stream);
        let _ = handle.await;
        assert_eq!(
            obs.charged_bytes(),
            0,
            "credit leaked after the stream ended"
        );
    });
}

/// The narrow shape is UNREGRESSED: with the default ceiling the batch-count
/// channel is still the binding governor, the produced-batch bound is exactly the
/// pre-#2821 structural one, and the byte ceiling never trips (peak resident
/// capacity stays far below it).
#[test]
fn narrow_rows_stay_channel_governed() {
    let (_temp, producer, paths, schema_ref) = narrow_setup();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        let pr = StreamProbe::default();
        let (mut stream, handle) = spawn_streaming(
            producer,
            MergeInput::Paths(paths),
            schema_ref,
            RpcMetrics::start("do_get"),
            DO_GET_CHANNEL_CAPACITY,
            EgressBudget::default(),
            pr.clone(),
            CancelFlag::new(),
            timer(),
        );
        let _schema_msg = stream.next().await.expect("schema");
        let _first = stream.next().await.expect("first batch");
        tokio::task::yield_now().await;

        let produced = pr.produced_batches.load(Ordering::Relaxed);
        assert!(
            produced <= DO_GET_CHANNEL_CAPACITY + IN_FLIGHT_ALLOWANCE,
            "narrow shape: produced {produced} exceeds the pre-#2821 structural bound"
        );
        let obs = &pr.egress;
        assert!(obs.largest_batch_capacity_bytes() > 0, "vacuous fixture");
        assert!(
            obs.peak_resident_capacity_bytes() < DEFAULT_MAX_INFLIGHT_EGRESS_BYTES as u64,
            "narrow shape must not come near the byte ceiling (peak {} B)",
            obs.peak_resident_capacity_bytes()
        );
        // The ceiling did not reduce how far the producer may run ahead: the
        // whole channel's worth of narrow batches costs a tiny fraction of it.
        let channel_worth = obs.largest_batch_capacity_bytes()
            * (DO_GET_CHANNEL_CAPACITY + IN_FLIGHT_ALLOWANCE) as u64;
        assert!(
            channel_worth < DEFAULT_MAX_INFLIGHT_EGRESS_BYTES as u64,
            "the byte ceiling would bind before the channel at this narrow shape"
        );

        drop(stream);
        let _ = handle.await;
    });
}

/// The bound holds independent of TOTAL result size, and the streamed content is
/// byte-identical to the collect path for the same input — a governor that
/// dropped, reordered or truncated a batch would fail here.
#[test]
fn a_full_drain_stays_bounded_and_matches_the_collect_path() {
    let (_temp, producer, paths, schema_ref) = wide_setup(WIDE_BATCH_CAP);
    let collected = producer
        .produce_from_resolved(paths.clone(), &CancelFlag::new())
        .expect("collect path");
    let expected_rows: usize = collected.iter().map(|b| b.num_rows()).sum();
    assert_eq!(expected_rows, WIDE_ROWS as usize, "vacuous fixture");
    let expected =
        arrow::compute::concat_batches(&collected[0].schema(), &collected).expect("concat collect");

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        let pr = StreamProbe::default();
        let (stream, handle) = spawn_streaming(
            producer,
            MergeInput::Paths(paths),
            schema_ref,
            RpcMetrics::start("do_get"),
            DO_GET_CHANNEL_CAPACITY,
            EgressBudget::bytes(WIDE_CEILING),
            pr.clone(),
            CancelFlag::new(),
            timer(),
        );
        let batches = decode_all(stream).await;
        let _ = handle.await;

        let obs = &pr.egress;
        let peak = obs.peak_resident_capacity_bytes();
        let largest = obs.largest_batch_capacity_bytes();
        assert!(
            peak <= contract_bytes(WIDE_CEILING as u64, largest),
            "peak resident {peak} B exceeds max({WIDE_CEILING}, {largest}) over a full drain"
        );
        assert!(
            obs.batches_materialized() > 4,
            "expected many batches over {WIDE_ROWS} wide rows, got {}",
            obs.batches_materialized()
        );
        assert_eq!(obs.charged_bytes(), 0, "credit leaked after a clean drain");

        let got = arrow::compute::concat_batches(&batches[0].schema(), &batches).expect("concat");
        assert_eq!(got.num_rows(), expected_rows);
        assert_eq!(
            got.columns(),
            expected.columns(),
            "streamed content differs from the collect path"
        );
    });
}

// ---------------------------------------------------------------------------
// Requirement: both producer loops reserve
// ---------------------------------------------------------------------------

/// Drive the SAME ceiling assertion through the partition-at-a-time loop
/// (`drive_merge`) and the row-granular one (`drive_merge_streaming`), against a
/// real `ChannelSink`. A governor wired into only one loop leaves the other
/// unbounded and fails here.
#[test]
fn both_producer_loops_reserve_before_materializing() {
    for streaming_loop in [false, true] {
        let (_temp, producer, paths, _schema_ref) = wide_setup(WIDE_BATCH_CAP);
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let pr = StreamProbe::default();
            let credit = EgressCredit::new(EgressBudget::bytes(WIDE_CEILING), pr.egress.clone());
            let (tx, mut rx) =
                mpsc::channel::<Result<CreditedBatch, ProducerError>>(DO_GET_CHANNEL_CAPACITY);
            let cancel = CancelFlag::new();
            let sink_cancel = cancel.clone();
            let handle = tokio::task::spawn_blocking(move || {
                let mut sink = ChannelSink {
                    tx,
                    produced: Arc::new(AtomicUsize::new(0)),
                    cancel: sink_cancel.clone(),
                    credit,
                };
                let progress = crate::scan_progress::ScanProgress::default();
                if streaming_loop {
                    producer.produce_streaming(paths, &sink_cancel, &mut sink, &progress, || {})
                } else {
                    let mut merger = producer
                        .open_cold_merger(paths, &sink_cancel)
                        .expect("merger");
                    producer.drive_merge(&mut merger, &sink_cancel, &mut sink, &progress, "test")
                }
            });

            let mut rows = 0usize;
            let mut n_batches = 0usize;
            while let Some(item) = rx.recv().await {
                let batch = item.expect("streamed batch").into_batch();
                rows += batch.num_rows();
                n_batches += 1;
            }
            handle.await.expect("join").expect("merge");

            let obs = &pr.egress;
            let which = if streaming_loop {
                "drive_merge_streaming"
            } else {
                "drive_merge"
            };
            assert_eq!(rows, WIDE_ROWS as usize, "{which}: vacuous fixture");
            assert!(n_batches > 4, "{which}: expected many batches");
            assert!(
                obs.reservations_granted() as usize >= n_batches,
                "{which}: {} reservations for {n_batches} batches — a build site \
                 materialized without reserving",
                obs.reservations_granted()
            );
            assert_eq!(
                obs.batches_materialized() as usize,
                n_batches,
                "{which}: every emitted batch must be accounted"
            );
            assert!(
                obs.peak_resident_capacity_bytes()
                    <= contract_bytes(WIDE_CEILING as u64, obs.largest_batch_capacity_bytes()),
                "{which}: peak resident {} B exceeds the contract",
                obs.peak_resident_capacity_bytes()
            );
            assert_eq!(obs.charged_bytes(), 0, "{which}: credit leaked");
        });
    }
}

// ---------------------------------------------------------------------------
// Requirement: the byte ceiling never deadlocks
// ---------------------------------------------------------------------------

/// A ceiling SMALLER than one batch still delivers every batch and terminates —
/// the deadlock-avoidance clamp. A naive non-clamping implementation hangs here
/// (the test would never return), and so would a release-on-yield deferred slot.
#[test]
fn a_batch_larger_than_the_whole_ceiling_is_still_delivered() {
    let (_temp, producer, paths, schema_ref) = wide_setup(WIDE_BATCH_CAP);

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        let pr = StreamProbe::default();
        // One quantum: every batch of this fixture is orders of magnitude bigger.
        let (stream, handle) = spawn_streaming(
            producer,
            MergeInput::Paths(paths),
            schema_ref,
            RpcMetrics::start("do_get"),
            DO_GET_CHANNEL_CAPACITY,
            EgressBudget::bytes(1),
            pr.clone(),
            CancelFlag::new(),
            timer(),
        );
        let rows: usize = decode_all(stream).await.iter().map(|b| b.num_rows()).sum();
        let _ = handle.await;

        assert_eq!(
            rows, WIDE_ROWS as usize,
            "every row must still be delivered"
        );
        let obs = &pr.egress;
        assert!(obs.batches_materialized() > 4, "vacuous fixture");
        assert!(
            obs.largest_batch_capacity_bytes() > EGRESS_CREDIT_QUANTUM_BYTES as u64,
            "the fixture's batches must exceed the ceiling for this to prove the clamp"
        );
        assert_eq!(obs.charged_bytes(), 0);
    });
}

// ---------------------------------------------------------------------------
// Requirement: the deferred (one-batch) release
// ---------------------------------------------------------------------------

/// A batch's credit is STILL charged while the consumer holds it, and is
/// released only when the consumer comes back for the next one.
///
/// This is the property that keeps the bound at `max(ceiling, one maximum batch)`
/// rather than `... + one maximum batch`: `MeteredDoGetStream` is upstream of the
/// Flight encoder, which holds the yielded `RecordBatch` while encoding it. A
/// "simplification" to release-on-yield fails this test.
#[test]
fn a_yielded_batch_keeps_its_credit_until_the_next_is_requested() {
    let schema = fx::wide_row_schema();
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        let producer = MergeProducer::new(schema, 8192).expect("producer");
        let arrow_schema = Arc::new(producer.arrow_schema().expect("schema"));
        let one_row = |fill: u8| {
            let cols: Vec<arrow::array::ArrayRef> = vec![
                Arc::new(arrow::array::Int32Array::from(vec![fill as i32])),
                Arc::new(arrow::array::BinaryArray::from(vec![Some(
                    vec![fill; 4096].as_slice(),
                )])),
                Arc::new(arrow::array::StringArray::from(vec![Some("row")])),
            ];
            RecordBatch::try_new(Arc::clone(&arrow_schema), cols).expect("batch")
        };

        let pr = StreamProbe::default();
        let credit = EgressCredit::new(EgressBudget::bytes(1024 * 1024), pr.egress.clone());
        let (tx, rx) = mpsc::channel::<Result<CreditedBatch, ProducerError>>(4);

        let mut caps = Vec::new();
        for fill in [1u8, 2u8] {
            let batch = one_row(fill);
            let actual = batch.get_array_memory_size();
            caps.push(actual as u64);
            let permit = credit
                .reserve(actual * 4)
                .await
                .materialize(actual)
                .expect("granted");
            tx.send(Ok(CreditedBatch::new(batch, permit)))
                .await
                .expect("send");
        }
        drop(tx);

        let mut metered = MeteredDoGetStream::new(
            Box::pin(ReceiverStream { rx }),
            RpcMetrics::start("do_get"),
            None,
            pr.clone(),
            None,
            None,
        );
        let obs = pr.egress.clone();
        assert_eq!(obs.resident_capacity_bytes(), caps[0] + caps[1]);

        let _a = metered.next().await.expect("batch A").expect("ok");
        assert_eq!(
            obs.resident_capacity_bytes(),
            caps[0] + caps[1],
            "A is deferred (still charged) and B is still queued"
        );

        let _b = metered.next().await.expect("batch B").expect("ok");
        assert_eq!(
            obs.resident_capacity_bytes(),
            caps[1],
            "asking for B released A's credit; B is now the deferred one"
        );

        drop(metered);
        assert_eq!(
            obs.resident_capacity_bytes(),
            0,
            "dropping the stream must release the deferred permit"
        );
        assert_eq!(obs.charged_bytes(), 0);
    });
}

// ---------------------------------------------------------------------------
// Requirement: credit is released on every termination path
// ---------------------------------------------------------------------------

/// Dropping the response stream mid-flight (client disconnect) returns ALL
/// charged credit: the deferred permit, every batch still queued in the channel,
/// and any reservation the producer abandoned.
#[test]
fn dropping_the_stream_mid_flight_releases_all_credit() {
    let (_temp, producer, paths, schema_ref) = wide_setup(WIDE_BATCH_CAP);

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        let pr = StreamProbe::default();
        let (mut stream, handle) = spawn_streaming(
            producer,
            MergeInput::Paths(paths),
            schema_ref,
            RpcMetrics::start("do_get"),
            DO_GET_CHANNEL_CAPACITY,
            EgressBudget::bytes(WIDE_CEILING),
            pr.clone(),
            CancelFlag::new(),
            timer(),
        );
        let _schema_msg = stream.next().await.expect("schema");
        let _first = stream.next().await.expect("first batch");
        assert!(
            pr.egress.charged_bytes() > 0,
            "expected live credit mid-stream (vacuous)"
        );

        drop(stream);
        handle.await.expect("merge task joins after disconnect");

        assert_eq!(
            pr.egress.charged_bytes(),
            0,
            "credit stranded on disconnect"
        );
        assert_eq!(pr.egress.resident_capacity_bytes(), 0);
    });
}

/// A terminal producer error surfaced mid-stream strands no credit: the `Err`
/// arm carries none and every queued `Ok` releases its own on drop.
#[test]
fn a_mid_stream_producer_error_does_not_strand_credit() {
    let schema = fx::narrow_row_schema();
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        let producer = MergeProducer::new(schema, 8192).expect("producer");
        let arrow_schema = Arc::new(producer.arrow_schema().expect("schema"));
        let batch = RecordBatch::try_new(
            Arc::clone(&arrow_schema),
            arrow_schema
                .fields()
                .iter()
                .map(|f| arrow::array::new_null_array(f.data_type(), 1))
                .collect(),
        )
        .expect("batch");

        let pr = StreamProbe::default();
        let credit = EgressCredit::new(EgressBudget::bytes(1024 * 1024), pr.egress.clone());
        let (tx, rx) = mpsc::channel::<Result<CreditedBatch, ProducerError>>(4);
        let actual = batch.get_array_memory_size();
        let permit = credit
            .reserve(actual * 4)
            .await
            .materialize(actual)
            .expect("granted");
        tx.send(Ok(CreditedBatch::new(batch, permit)))
            .await
            .expect("send");
        tx.send(Err(ProducerError::Panicked {
            message: "synthetic mid-merge panic (test)".into(),
        }))
        .await
        .expect("send err");
        drop(tx);
        assert!(pr.egress.charged_bytes() > 0);

        let mut metered = MeteredDoGetStream::new(
            Box::pin(ReceiverStream { rx }),
            RpcMetrics::start("do_get"),
            None,
            pr.clone(),
            None,
            None,
        );
        let _ok = metered.next().await.expect("batch").expect("ok");
        let err = metered.next().await.expect("terminal item");
        assert!(err.is_err(), "the terminal producer error must surface");
        drop(metered);

        assert_eq!(pr.egress.charged_bytes(), 0, "credit stranded on error");
        assert_eq!(
            pr.errors_recorded.load(Ordering::Relaxed),
            1,
            "the mid-stream error must still reach the error-observability hook"
        );
    });
}

/// A producer parked awaiting egress credit (pool exhausted, consumer stopped)
/// is woken by the shared cancel flag and stops promptly, having materialized
/// nothing — exactly as one parked on a full channel is.
///
/// The `sleep`/`timeout` here are LIVENESS bounds on a parked blocking-pool
/// thread (the merged `cancelled_emit_under_backpressure_returns_cancelled`
/// precedent), not correctness thresholds (#2642).
#[test]
fn a_producer_parked_on_credit_wakes_on_cancellation() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        let pr = StreamProbe::default();
        let credit = EgressCredit::new(EgressBudget::bytes(4096), pr.egress.clone());
        // Exhaust the pool and hold it for the whole test.
        let _held = credit.reserve(4096).await.materialize(4096).expect("held");

        let cancel = CancelFlag::new();
        let (tx, _rx) = mpsc::channel::<Result<CreditedBatch, ProducerError>>(4);
        let sink_cancel = cancel.clone();
        let reserve_task = tokio::task::spawn_blocking(move || {
            let mut sink = ChannelSink {
                tx,
                produced: Arc::new(AtomicUsize::new(0)),
                cancel: sink_cancel,
                credit,
            };
            sink.reserve(4096).map(|_| ())
        });

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        cancel.cancel();

        let joined = tokio::time::timeout(std::time::Duration::from_secs(3), reserve_task)
            .await
            .expect("a parked reservation must return once cancelled, not park forever")
            .expect("reserve task joins");
        match joined {
            Err(ProducerError::Cancelled) => {}
            Ok(()) => panic!("a cancelled reservation must return Cancelled, got Ok"),
            Err(other) => panic!("expected Cancelled, got {other:?}"),
        }
        // Nothing was built while the reservation was pending.
        assert_eq!(pr.egress.batches_materialized(), 1, "only the held permit");
    });
}

// ---------------------------------------------------------------------------
// Requirement: the embedder opt-out
// ---------------------------------------------------------------------------

/// An explicitly unbounded budget applies no ceiling: residency reverts to the
/// pre-change structural (batch-count) bound and nothing is charged.
#[test]
fn an_unbounded_budget_reverts_to_the_structural_bound() {
    let (_temp, producer, paths, schema_ref) = wide_setup(WIDE_BATCH_CAP);

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        let pr = StreamProbe::default();
        let (mut stream, handle) = spawn_streaming(
            producer,
            MergeInput::Paths(paths),
            schema_ref,
            RpcMetrics::start("do_get"),
            DO_GET_CHANNEL_CAPACITY,
            EgressBudget::unbounded(),
            pr.clone(),
            CancelFlag::new(),
            timer(),
        );
        let _schema_msg = stream.next().await.expect("schema");
        let _first = stream.next().await.expect("first batch");
        tokio::task::yield_now().await;

        assert_eq!(
            pr.egress.charged_bytes(),
            0,
            "an unbounded budget must charge nothing"
        );
        assert!(
            pr.egress.peak_charged_bytes() == 0,
            "an unbounded budget must never have charged anything"
        );
        // The structural bound is all that remains.
        let produced = pr.produced_batches.load(Ordering::Relaxed);
        assert!(produced <= DO_GET_CHANNEL_CAPACITY + IN_FLIGHT_ALLOWANCE);

        drop(stream);
        let _ = handle.await;
    });
}

// ---------------------------------------------------------------------------
// The composition against B4, restated at the wiring level
// ---------------------------------------------------------------------------

/// The service's DEFAULT egress budget composes inside B4 with #2825's merged
/// per-batch cap, computed from the imported constants at the surface a
/// deployment actually gets.
#[test]
fn the_default_service_budget_composes_inside_b4() {
    let budget = crate::service::CqliteFlightService::new(std::env::temp_dir(), 8192)
        .egress_budget()
        .ceiling_bytes()
        .expect("the ceiling is ON by default on every construction path");
    assert_eq!(budget, DEFAULT_MAX_INFLIGHT_EGRESS_BYTES);
    let one_max_batch =
        worst_case_batch_capacity_bytes(crate::batch_bytes::DEFAULT_MAX_BATCH_BYTES, 3, 0);
    assert!(budget.max(one_max_batch) <= 16 * 1024 * 1024);
}
