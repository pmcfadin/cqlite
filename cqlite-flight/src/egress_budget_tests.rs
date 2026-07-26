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

/// Wait until the producer has PARKED on the exhausted credit pool.
///
/// Saturation is observed as the park EVENT the governor publishes
/// (`EgressObservation::parked`), never as elapsed time and never as "one
/// `yield_now` should be enough": a peak-residency sample taken before the
/// producer has run up against the ceiling makes the bound assertion weaker than
/// it reads. Waiting for the event ALSO makes the test non-vacuous — a fixture
/// that stopped saturating the pool fails here instead of quietly asserting
/// nothing.
///
/// The `timeout` is a LIVENESS bound on a parked blocking-pool thread (the
/// merged `cancelled_emit_under_backpressure_returns_cancelled` precedent), not a
/// correctness threshold (#2642): the assertion is on the park counter, and the
/// timeout only converts a hang into a readable failure.
async fn await_pool_saturated(obs: &crate::egress_credit::EgressObservation) {
    tokio::time::timeout(std::time::Duration::from_secs(60), async {
        // `notify_one` stores a permit when no waiter is registered, so a park
        // that happens between the check and the await cannot be missed.
        while obs.reservations_parked() == 0 {
            obs.parked().await;
        }
    })
    .await
    .expect("the producer must park on the exhausted egress pool while the consumer is stopped");
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
        // Sample only once the producer is provably pressed against the ceiling.
        await_pool_saturated(&pr.egress).await;

        let obs = &pr.egress;
        let largest = obs.largest_batch_capacity_bytes();
        let peak = obs.peak_resident_capacity_bytes();

        // Non-vacuity: real batches, and the byte ceiling — not the batch-count
        // channel — is what binds at this shape.
        assert!(largest > 0, "no batch was materialized (vacuous fixture)");
        assert!(peak > 0, "no residency was observed (vacuous fixture)");
        assert!(
            obs.reservations_parked() > 0,
            "the byte ceiling never applied backpressure — the sample proves nothing"
        );
        assert!(
            largest.saturating_mul(DO_GET_CHANNEL_CAPACITY as u64) > WIDE_CEILING as u64,
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
        let channel_worth = obs
            .largest_batch_capacity_bytes()
            .saturating_mul((DO_GET_CHANNEL_CAPACITY + IN_FLIGHT_ALLOWANCE) as u64);
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
        // The safety valve is for a RETAINING consumer only: the real Flight
        // encoder drops each batch before asking for the next, so a full drain
        // through it must never need the valve (issue #2821 review R1).
        assert_eq!(
            obs.safety_valve_releases(),
            0,
            "the safety valve fired on the ORDINARY encoder drain — it is returning credit \
             for server-resident data and loosening the published bound"
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
                // Issue #2821 review R3: take the batch WITH its permit and keep
                // the permit alive for exactly as long as this test holds the
                // data. `into_batch()` drops the permit the instant the batch is
                // received, so residency would be UNCHARGED for the whole time
                // the batch is held and `peak_resident_capacity_bytes()` would
                // under-report — making the contract assertion below close to
                // self-fulfilling.
                let (batch, permit) = item.expect("streamed batch").split();
                // The SHARP form of that: asserted at the instant of the hold,
                // not inferred from a high-water mark. With the permit dropped at
                // receive time this FAILS the moment this batch is the only one
                // outstanding — which is exactly the hole the peak assertion had.
                let batch_bytes = batch.get_array_memory_size() as u64;
                assert!(
                    pr.egress.resident_capacity_bytes() >= batch_bytes,
                    "batch {n_batches}: this test holds {batch_bytes} B of Arrow data but the \
                     governor accounts only {} B as resident — residency is UNCHARGED while \
                     the consumer holds the batch",
                    pr.egress.resident_capacity_bytes()
                );
                rows += batch.num_rows();
                n_batches += 1;
                // Explicit order: the data goes first, then the credit that
                // accounts for it. Never the reverse.
                drop(batch);
                drop(permit);
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
        // The clamp really engaged here — the counter that the shipped-default
        // test asserts is ZERO is non-trivially observable on this path.
        assert_eq!(
            obs.reservations_clamped(),
            obs.reservations_granted(),
            "every reservation against a one-quantum pool must clamp"
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
///
/// Release is keyed on the CONSUMER DROPPING the batch, not on the consumer
/// asking for the next one (see `metered_stream::DeferredCredit`), so this test
/// drops A explicitly before asking for B.
#[test]
fn a_yielded_batch_keeps_its_credit_until_the_consumer_drops_it() {
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
                .expect("pool open")
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

        let a = metered.next().await.expect("batch A").expect("ok");
        assert_eq!(
            obs.resident_capacity_bytes(),
            caps[0] + caps[1],
            "A is deferred (still charged) and B is still queued — a release-on-yield \
             implementation returns A's credit here while the encoder still holds A"
        );

        drop(a);
        let _b = metered.next().await.expect("batch B").expect("ok");
        assert_eq!(
            obs.resident_capacity_bytes(),
            caps[1],
            "A was dropped by the consumer, so the next poll returned its credit; B is now \
             the deferred one"
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

/// A SPECULATIVE poll — one that returns `Pending` while the consumer is still
/// holding the previously yielded batch — must NOT return that batch's credit.
///
/// `MeteredDoGetStream` is `pub(crate)` and polled directly (a `select!` arm,
/// `futures::poll!`, this suite). Releasing at the top of every `poll_next` is
/// safe only for a consumer that drops batch N before asking for N+1 — true of
/// `FlightDataEncoder`, but a bound that rests on a downstream consumer's polling
/// discipline is not a bound. Credit is therefore keyed on the batch's Arrow data
/// actually being dropped.
#[test]
fn a_speculative_pending_poll_does_not_release_a_held_batchs_credit() {
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
        let actual = batch.get_array_memory_size();

        let pr = StreamProbe::default();
        let obs = pr.egress.clone();
        let credit = EgressCredit::new(EgressBudget::bytes(1024 * 1024), obs.clone());
        // `tx` stays ALIVE, so the channel is empty-but-open: a further poll
        // parks rather than terminating the stream.
        let (tx, rx) = mpsc::channel::<Result<CreditedBatch, ProducerError>>(4);
        let permit = credit
            .reserve(actual * 4)
            .await
            .expect("pool open")
            .materialize(actual)
            .expect("granted");
        tx.send(Ok(CreditedBatch::new(batch, permit)))
            .await
            .expect("send");

        let mut metered = MeteredDoGetStream::new(
            Box::pin(ReceiverStream { rx }),
            RpcMetrics::start("do_get"),
            None,
            pr.clone(),
            None,
            None,
        );
        let held = metered.next().await.expect("batch").expect("ok");
        let charged_while_held = obs.charged_bytes();
        assert!(charged_while_held > 0, "vacuous: nothing was charged");

        // The speculative poll: the consumer still owns `held`.
        {
            let mut speculative = metered.next();
            assert!(
                futures::poll!(&mut speculative).is_pending(),
                "the channel is empty-but-open, so this poll must park"
            );
        }
        assert_eq!(
            obs.charged_bytes(),
            charged_while_held,
            "a speculative Pending poll released the credit for a batch the consumer is \
             still holding — the memory bound is voided by a consumer that polls before it \
             is done with the data"
        );
        assert_eq!(obs.resident_capacity_bytes(), actual as u64);

        // Once the consumer really is done with it, the next poll reaps it — so
        // holding across `Pending` cannot wedge a producer parked on the pool.
        drop(held);
        {
            let mut after = metered.next();
            assert!(futures::poll!(&mut after).is_pending());
        }
        assert_eq!(
            obs.charged_bytes(),
            0,
            "credit must be returned once the consumer drops the batch"
        );
        assert_eq!(obs.resident_capacity_bytes(), 0);
        drop(tx);
        drop(metered);
    });
}

// ---------------------------------------------------------------------------
// Requirement: NO consumer behaviour can wedge the stream (the safety valve)
// ---------------------------------------------------------------------------

/// Drive the real streaming producer into a `MeteredDoGetStream` and drain it,
/// RETAINING every yielded batch across the await for the next one. Returns the
/// probe and the total rows delivered.
///
/// The `timeout` is a LIVENESS bound (the `await_pool_saturated` /
/// `cancelled_emit_under_backpressure_returns_cancelled` precedent), never a
/// correctness threshold (#2642): a wedged stream would otherwise hang the whole
/// test binary instead of failing readably.
async fn drain_metered_retaining(
    producer: MergeProducer,
    paths: Vec<PathBuf>,
    ceiling: usize,
) -> (StreamProbe, usize) {
    let pr = StreamProbe::default();
    let credit = EgressCredit::new(EgressBudget::bytes(ceiling), pr.egress.clone());
    let (tx, rx) = mpsc::channel::<Result<CreditedBatch, ProducerError>>(DO_GET_CHANNEL_CAPACITY);
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
        producer.produce_streaming(paths, &sink_cancel, &mut sink, &progress, || {})
    });

    let mut metered = MeteredDoGetStream::new(
        Box::pin(ReceiverStream { rx }),
        RpcMetrics::start("do_get"),
        None,
        pr.clone(),
        None,
        None,
    );

    // `held` is the whole point: batch N stays alive across the `.await` that
    // asks for N+1.
    let mut held: Vec<RecordBatch> = Vec::new();
    let mut rows = 0usize;
    let drained = tokio::time::timeout(std::time::Duration::from_secs(60), async {
        while let Some(item) = metered.next().await {
            let batch = item.expect("streamed batch");
            rows += batch.num_rows();
            held.push(batch);
        }
    })
    .await;
    assert!(
        drained.is_ok(),
        "the do_get stream WEDGED: the producer is parked on credit held by a batch the \
         consumer is still retaining, and the consumer is awaiting a batch that can never be \
         built — the safety valve did not fire"
    );
    handle.await.expect("join").expect("merge");
    assert!(!held.is_empty(), "vacuous: nothing was retained");
    drop(held);
    drop(metered);
    (pr, rows)
}

/// A RETAINING consumer — one that holds every yielded batch alive while awaiting
/// the next — still makes progress, and the safety valve is what makes that true.
///
/// This is the failure mode keying credit release on the batch's LIVENESS
/// introduces (issue #2821 review R1): the deferred permit holds the credit, the
/// producer parks in `EgressCredit::reserve`, and the batch the consumer is
/// waiting for can never be built. Without
/// `MeteredDoGetStream::open_safety_valve` this test HANGS rather than failing an
/// assertion — hence the liveness timeout in `drain_metered`.
///
/// Note what is being asserted about MEMORY: the retained batches are the
/// CONSUMER's memory. The governor correctly stops charging for bytes it no
/// longer controls, because the published bound governs SERVER-SIDE residency —
/// see `DEFAULT_MAX_INFLIGHT_EGRESS_BYTES`.
#[test]
fn a_retaining_consumer_still_makes_progress() {
    let (_temp, producer, paths, _schema_ref) = wide_setup(WIDE_BATCH_CAP);
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        let (pr, rows) = drain_metered_retaining(producer, paths, WIDE_CEILING).await;
        let obs = &pr.egress;
        assert_eq!(
            rows, WIDE_ROWS as usize,
            "vacuous fixture — no rows streamed"
        );
        assert!(
            obs.batches_materialized() > 4,
            "vacuous fixture: {} batch(es)",
            obs.batches_materialized()
        );
        // Non-vacuity of the SCENARIO: the ceiling really did press the producer
        // against the pool while the consumer was holding data.
        assert!(
            obs.reservations_parked() > 0,
            "the producer never parked, so this drain never reached the wedge state and \
             proves nothing about the safety valve"
        );
        assert!(
            obs.safety_valve_releases() > 0,
            "the stream drained without the valve firing — the retaining consumer never \
             actually wedged it, so this test is not exercising the R1 scenario"
        );
        assert_eq!(obs.charged_bytes(), 0, "credit leaked");
        assert_eq!(obs.resident_capacity_bytes(), 0);
    });
}

/// The valve fires ONLY in the wedge state, asserted on BOTH sides of the
/// predicate in one deterministic scenario — no reliance on a race being won.
///
/// This is what stops the valve from quietly loosening the bound. A valve that
/// fired on ordinary backpressure would return credit for a batch still resident
/// on the SERVER, which is precisely the uncharged-resident-batch class the whole
/// governor exists to eliminate.
///
/// Sequence:
/// 1. Two credited batches are queued; the consumer takes A and RETAINS it.
/// 2. A speculative `Pending` poll with **no producer parked** — the valve must
///    NOT fire even though a deferred permit holds credit.
/// 3. A reservation larger than the free pool is started and PARKS (awaited on
///    the governor's own park event, never on elapsed time — #2642). Now the
///    wedge predicate holds, and the next poll must fire the valve exactly once
///    and let the parked reservation through.
#[test]
fn the_safety_valve_fires_only_when_the_stream_is_wedged() {
    let schema = fx::wide_row_schema();
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        let producer = MergeProducer::new(schema, 8192).expect("producer");
        let arrow_schema = Arc::new(producer.arrow_schema().expect("schema"));
        let one_row = |fill: u8| {
            let cols: Vec<arrow::array::ArrayRef> = vec![
                Arc::new(arrow::array::Int32Array::from(vec![fill as i32])),
                Arc::new(arrow::array::BinaryArray::from(vec![Some(
                    vec![fill; 64 * 1024].as_slice(),
                )])),
                Arc::new(arrow::array::StringArray::from(vec![Some("row")])),
            ];
            RecordBatch::try_new(Arc::clone(&arrow_schema), cols).expect("batch")
        };

        let pr = StreamProbe::default();
        let obs = pr.egress.clone();
        // Sized so ONE batch's realized capacity (~65 KiB) fits without the
        // deadlock clamp, but TWO cannot: with A retained, a second same-sized
        // reservation must park. Both halves are asserted below, so a fixture
        // drift in either direction fails loudly instead of going vacuous.
        const CEILING: usize = 96 * 1024;
        let credit = EgressCredit::new(EgressBudget::bytes(CEILING), obs.clone());
        // `tx` stays alive, so the channel is empty-but-open and a further poll
        // parks rather than terminating the stream.
        let (tx, rx) = mpsc::channel::<Result<CreditedBatch, ProducerError>>(4);
        let batch = one_row(1);
        let actual = batch.get_array_memory_size();
        assert!(
            actual <= CEILING && actual * 2 > CEILING,
            "fixture drift: one batch is {actual} B against a {CEILING} B pool — the scenario \
             needs exactly one batch to fit (no deadlock clamp) and two not to (so the second \
             reservation parks)"
        );
        let permit = credit
            .reserve(actual)
            .await
            .expect("pool open")
            .materialize(actual)
            .expect("granted");
        tx.send(Ok(CreditedBatch::new(batch, permit)))
            .await
            .expect("send");

        let mut metered = MeteredDoGetStream::new(
            Box::pin(ReceiverStream { rx }),
            RpcMetrics::start("do_get"),
            None,
            pr.clone(),
            None,
            None,
        );

        // (1) Take A and RETAIN it.
        let held = metered.next().await.expect("batch A").expect("ok");
        assert_eq!(obs.charged_bytes(), permits_bytes(actual));
        assert_eq!(obs.safety_valve_releases(), 0);

        // (2) No producer is parked: the valve must NOT fire, even though a
        //     deferred permit holds the entire charge.
        for _ in 0..3 {
            let mut speculative = metered.next();
            assert!(
                futures::poll!(&mut speculative).is_pending(),
                "the channel is empty-but-open, so this poll must park"
            );
        }
        assert_eq!(
            obs.safety_valve_releases(),
            0,
            "the valve fired with NO producer parked — it is releasing credit for \
             server-resident data on the ordinary backpressure path"
        );
        assert_eq!(
            obs.charged_bytes(),
            permits_bytes(actual),
            "A's credit was returned while nothing was wedged"
        );

        // (3) Park a reservation that cannot fit beside A's charge.
        let parking = tokio::spawn({
            let credit = credit.clone();
            async move { credit.reserve(actual).await.map(|r| r.materialize(actual)) }
        });
        await_pool_saturated(&obs).await;
        assert_eq!(
            obs.parked_now(),
            1,
            "exactly one reservation must be parked"
        );

        // The wedge predicate now holds: poll, and the valve fires exactly once.
        {
            let mut wedged = metered.next();
            let _ = futures::poll!(&mut wedged);
        }
        assert_eq!(
            obs.safety_valve_releases(),
            1,
            "the stream is wedged (producer parked, channel empty, the whole charge held by \
             a batch the consumer retains) and the valve did not fire — this consumer hangs"
        );
        // The parked reservation gets through, which is the point.
        let unparked = tokio::time::timeout(std::time::Duration::from_secs(60), parking)
            .await
            .expect("the parked reservation must be admitted once the valve fires")
            .expect("join");
        drop(unparked.expect("pool open").expect("granted"));

        // The retained batch is still ALIVE — its bytes are the consumer's
        // residency now, deliberately outside the server-side bound.
        assert_eq!(held.num_rows(), 1);
        drop(held);
        drop(tx);
        drop(metered);
        assert_eq!(obs.charged_bytes(), 0, "credit leaked");
    });
}

/// Permit bytes charged for `actual` capacity bytes: permits round UP to the
/// quantum, so the charge is quantised, never the raw byte count.
fn permits_bytes(actual: usize) -> u64 {
    (actual.div_ceil(EGRESS_CREDIT_QUANTUM_BYTES) * EGRESS_CREDIT_QUANTUM_BYTES) as u64
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
            .expect("pool open")
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
        let _held = credit
            .reserve(4096)
            .await
            .expect("pool open")
            .materialize(4096)
            .expect("held");

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
// Requirement: the SHIPPED default does not make the deadlock clamp routine
// ---------------------------------------------------------------------------

/// At the shipped defaults — the 4 MiB per-batch payload cap and the default
/// egress ceiling — a real streamed `do_get` over the merged wide-row fixture
/// cuts on the byte-cap and NEVER trips the deadlock clamp.
///
/// The defect this pins: admission is gated on the pre-materialization
/// RESERVATION (the full published worst case), not on the trued-down realized
/// size. A ceiling that does not clear ONE worst-case reservation makes every
/// full-size batch take the entire pool, so the producer cannot start batch N+1
/// until batch N has completely left the stream — strict lock-step, with the
/// 4-deep batch-count channel as dead weight, on exactly the wide-row workload
/// this ceiling exists for. `egress_credit_tests` pins the arithmetic; this pins
/// that the shipped default reaches a real stream and that a real byte-cap cut
/// happens under it.
///
/// Sharpness, stated honestly: the SHARP guard is the arithmetic one
/// (`egress_credit_tests::a_worst_case_default_reservation_does_not_clamp`, which
/// FAILS at an 8 MiB default). This end-to-end case cannot be sharp — a batch cut
/// by the byte-cap accumulates at most `cap - (width of the crossing row)`, so its
/// reservation lands just UNDER the worst case unless the row width happens to
/// divide the cap exactly. It is wiring evidence that the default is the value a
/// real stream governs by, and it holds the clamp counter at zero over a genuine
/// multi-batch drain.
#[test]
fn the_default_ceiling_does_not_clamp_a_real_byte_cap_cut_stream() {
    // Comfortably over the DEFAULT 4 MiB payload cap, so the byte-cap cuts at
    // least once; far under the 8192-row row-cap, so the cut can only be the
    // byte one.
    const BIG_ROWS: i32 = 80;
    const BIG_PAYLOAD: usize = 64 * 1024;

    let schema: TableSchema = fx::wide_row_schema();
    let (_temp, _data, dir) =
        build_sstables(&schema, vec![fx::wide_row_mutations(BIG_ROWS, BIG_PAYLOAD)]);
    // NOTE: no `with_max_batch_bytes` — the SHIPPED default per-batch cap.
    let producer = MergeProducer::with_spec(schema, 8192, crate::filter::ScanSpec::default())
        .expect("producer");
    let paths = producer
        .resolve_paths(&DirSource::new(&dir))
        .expect("resolve");
    let schema_ref = Arc::new(producer.arrow_schema().expect("arrow schema"));

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        let pr = StreamProbe::default();
        let (stream, handle) = spawn_streaming(
            producer,
            MergeInput::Paths(paths),
            schema_ref,
            RpcMetrics::start("do_get"),
            DO_GET_CHANNEL_CAPACITY,
            // The SHIPPED default ceiling.
            EgressBudget::default(),
            pr.clone(),
            CancelFlag::new(),
            timer(),
        );
        let rows: usize = decode_all(stream).await.iter().map(|b| b.num_rows()).sum();
        let _ = handle.await;

        let obs = &pr.egress;
        assert_eq!(rows, BIG_ROWS as usize, "vacuous fixture");
        assert!(
            obs.batches_materialized() > 1,
            "the DEFAULT byte-cap must have cut this stream (row-cap is 8192 rows over \
             {BIG_ROWS} rows), got {} batches",
            obs.batches_materialized()
        );
        assert_eq!(
            obs.reservations_clamped(),
            0,
            "{} of {} reservations clamped to the WHOLE pool at the shipped defaults: every \
             byte-cap-cut batch then runs the stream lock-step",
            obs.reservations_clamped(),
            obs.reservations_granted()
        );
        assert_eq!(
            obs.safety_valve_releases(),
            0,
            "the safety valve fired at the SHIPPED defaults on an ordinary encoder drain"
        );
        assert_eq!(obs.charged_bytes(), 0, "credit leaked after a clean drain");
    });
}

// ---------------------------------------------------------------------------
// Requirement: the fail-closed path terminates THE STREAM
// ---------------------------------------------------------------------------

/// A sink whose reservation is deliberately one byte, whatever the producer asks
/// for — the "estimator-conservatism contract broke" simulation the fail-closed
/// scenario calls for, applied at a REAL producer batch boundary.
struct UnderReservingSink {
    tx: mpsc::Sender<Result<CreditedBatch, ProducerError>>,
    credit: EgressCredit,
}

impl BatchSink for UnderReservingSink {
    fn reserve(&mut self, _capacity_bytes: usize) -> Result<EgressReservation, ProducerError> {
        let credit = self.credit.clone();
        Ok(tokio::runtime::Handle::current().block_on(async move { credit.reserve(1).await })?)
    }

    fn emit(&mut self, batch: CreditedBatch) -> Result<(), ProducerError> {
        self.tx
            .blocking_send(Ok(batch))
            .map_err(|_| ProducerError::Cancelled)
    }
}

/// The fail-closed path proven END TO END, not just on the helper: a realized
/// capacity above its reservation at a real producer batch boundary terminates
/// the RESPONSE STREAM with `Status::internal` naming the violated invariant, and
/// strands no credit.
#[test]
fn an_under_reservation_terminates_the_response_stream_with_internal() {
    let (_temp, producer, paths, schema_ref) = wide_setup(WIDE_BATCH_CAP);

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        let pr = StreamProbe::default();
        let credit = EgressCredit::new(EgressBudget::bytes(WIDE_CEILING), pr.egress.clone());
        let (tx, rx) =
            mpsc::channel::<Result<CreditedBatch, ProducerError>>(DO_GET_CHANNEL_CAPACITY);
        let cancel = CancelFlag::new();
        let error_tx = tx.clone();
        let handle = tokio::task::spawn_blocking(move || {
            let mut sink = UnderReservingSink { tx, credit };
            let progress = crate::scan_progress::ScanProgress::default();
            // The REAL terminal-error forwarding used by `spawn_streaming`.
            run_merge_catching_panics(&error_tx, move || {
                producer.produce_streaming(paths, &cancel, &mut sink, &progress, || {})
            });
        });

        let metered = MeteredDoGetStream::new(
            Box::pin(ReceiverStream { rx }),
            RpcMetrics::start("do_get"),
            None,
            pr.clone(),
            None,
            None,
        );
        // The REAL encoded response stream, so the assertion is on the gRPC
        // `Status` a client would see.
        let mut stream = encode_do_get(metered, schema_ref, pr.clone());
        let mut terminal: Option<Status> = None;
        while let Some(item) = stream.next().await {
            if let Err(status) = item {
                terminal = Some(status);
                break;
            }
        }
        drop(stream);
        handle.await.expect("merge task joins");

        let status = terminal.expect("the stream must terminate with the invariant error");
        assert_eq!(
            status.code(),
            tonic::Code::Internal,
            "a violated credit invariant is an internal fault, got: {status:?}"
        );
        assert!(
            status.message().contains("estimator-conservatism")
                || status.message().contains("egress credit invariant"),
            "the status must name the violated invariant, got: {}",
            status.message()
        );
        assert_eq!(
            pr.egress.charged_bytes(),
            0,
            "the fail-closed path stranded credit"
        );
        assert_eq!(pr.egress.resident_capacity_bytes(), 0);
        assert_eq!(
            pr.egress.batches_materialized(),
            0,
            "no batch may be accounted on a false reservation"
        );
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
