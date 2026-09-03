//! Tests for `crate::streaming` (issue #1476, AB1).
//!
//! Split out of `streaming.rs` to keep the production module under the
//! campsite file-size threshold (`~800` source lines) — this file is a test
//! module (loaded via `#[path]` from `streaming.rs`), well under the `~1500`
//! test-file threshold. See epic #1116/#1135.

use super::*;
use crate::producer::DirSource;
use crate::testutil::{build_sstables, delete_row, simple_schema, write_row};
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

/// A throwaway phase timer (issue #2162) for the streaming-path tests: they
/// assert bounded-channel / metrics behaviour, not phase timing, so any timer is
/// fine — it records its (tiny) phase durations as no-ops when `observability` is
/// off. The dedicated phase assertions live in the `observability-testing` gated
/// integration test.
fn timer() -> crate::obs::PhaseTimer {
    crate::obs::PhaseTimer::start("do_get")
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
            MergeInput::Paths(paths),
            schema_ref,
            RpcMetrics::start("do_get"),
            DO_GET_CHANNEL_CAPACITY,
            EgressBudget::default(),
            pr.clone(),
            CancelFlag::new(),
            timer(),
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
            produced <= DO_GET_CHANNEL_CAPACITY + IN_FLIGHT_ALLOWANCE,
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
    let producer = MergeProducer::with_spec(schema, 1, crate::filter::ScanSpec::default()).unwrap();
    let paths = resolved(&producer, &dir);
    let schema_ref = Arc::new(producer.arrow_schema().unwrap());

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        let pr = probe();
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
        let _schema_msg = read_one(&mut stream).await.expect("schema");
        let _first = read_one(&mut stream).await.expect("first batch");

        // Give the producer every opportunity to run ahead, then assert it is
        // still bounded — it physically cannot exceed the channel allowance.
        tokio::task::yield_now().await;
        let produced = pr.produced_batches.load(Ordering::Relaxed);
        assert!(
            produced <= DO_GET_CHANNEL_CAPACITY + IN_FLIGHT_ALLOWANCE,
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
    let producer = MergeProducer::with_spec(schema, 1, crate::filter::ScanSpec::default()).unwrap();
    let paths = resolved(&producer, &dir);
    let schema_ref = Arc::new(producer.arrow_schema().unwrap());

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        let pr = probe();
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

// ---- Roborev B1: a panic mid-merge is a terminal error, not a silent EOF ---

/// [`run_merge_catching_panics`] must forward a caught panic as
/// [`ProducerError::Panicked`] into the channel, never let it close silently
/// (which would be indistinguishable downstream from "the merge finished
/// successfully" — the exact silent-truncation class roborev B1 flagged).
// No tokio runtime needed: `run_merge_catching_panics` uses `blocking_send`
// (mirroring how it actually runs, inside `spawn_blocking`), and
// `blocking_recv` panics only INSIDE an async context — a plain `#[test]`
// (no runtime at all) is the correct way to exercise it directly.
#[test]
fn panicking_merge_forwards_a_terminal_error_not_silent_close() {
    let (tx, mut rx) = mpsc::channel::<Result<CreditedBatch, ProducerError>>(1);
    run_merge_catching_panics(&tx, || -> Result<(), ProducerError> {
        panic!("synthetic mid-merge panic (test)");
    });
    drop(tx);

    match rx.blocking_recv() {
        Some(Err(ProducerError::Panicked { message })) => {
            assert!(
                message.contains("synthetic mid-merge panic"),
                "panic message must be forwarded, got: {message}"
            );
        }
        other => {
            panic!(
                "a panic must forward ProducerError::Panicked, not a silent close \
                 (got {})",
                match other {
                    Some(Ok(_)) => "a batch",
                    Some(Err(_)) => "a different error",
                    None => "a silent close",
                }
            )
        }
    }
    // No spurious extra item after the terminal error.
    assert!(rx.blocking_recv().is_none());
}

/// End-to-end through the actual `do_get` response-stream stack
/// (`ReceiverStream` → `MeteredDoGetStream` → `encode_do_get`): a panic mid-merge
/// must surface as `Status::internal`, never a clean, silently-truncated EOF —
/// this is the client-visible symptom roborev B1 named. Also proves roborev
/// B2: the error arm reaches the `record_status_error` hook exactly once
/// (observed via `StreamProbe`, which does not depend on the `observability`
/// feature's OTel counters).
#[test]
fn do_get_stream_surfaces_panic_as_internal_status_not_eof() {
    let schema = simple_schema();
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        let (tx, rx) =
            mpsc::channel::<Result<CreditedBatch, ProducerError>>(DO_GET_CHANNEL_CAPACITY);
        // Simulate the merge panicking on the blocking pool.
        let handle = tokio::task::spawn_blocking(move || {
            run_merge_catching_panics(&tx, || -> Result<(), ProducerError> {
                panic!("synthetic mid-merge panic (test)");
            });
        });

        let schema_ref = Arc::new(
            MergeProducer::new(schema, 4)
                .unwrap()
                .arrow_schema()
                .unwrap(),
        );
        let inner = Box::pin(ReceiverStream { rx });
        let pr = probe();
        let metered = MeteredDoGetStream::new(
            inner,
            RpcMetrics::start("do_get"),
            None,
            pr.clone(),
            None,
            None,
        );
        let mut stream = encode_do_get(metered, schema_ref, pr.clone());

        // First message is the schema; the panic must arrive as an `Err` Status
        // (not the stream simply ending after the schema).
        let _schema_msg = stream.next().await.expect("schema message");
        match stream.next().await {
            Some(Err(status)) => {
                assert_eq!(
                    status.code(),
                    tonic::Code::Internal,
                    "panic must map to Status::internal, got: {status:?}"
                );
            }
            other => {
                panic!("a mid-merge panic must surface as Status::internal, not {other:?}")
            }
        }
        let _ = handle.await;

        assert_eq!(
            pr.errors_recorded.load(Ordering::Relaxed),
            1,
            "the mid-stream error must reach record_status_error exactly once (B2)"
        );
    });
}

// ---- Issue #2193: a swallowed encoder-stage egress failure is now surfaced ---

/// An error raised INSIDE the Flight encoder (here: a batch whose column count
/// disagrees with the advertised schema, so `FlightDataEncoderBuilder` fails
/// while building the record-batch message — AFTER it has already emitted the
/// schema message, the exact "schema then abrupt failure" shape the field
/// client saw as `Failed to read message`) must (a) surface to the client as a
/// gRPC `Status`, and (b) be routed through the shared error-observability hook
/// (`record_status_error`, which logs at error level and bumps the flight
/// error-rate signal), observed here via `StreamProbe::errors_recorded`.
///
/// An encoder-stage error is downstream of [`MeteredDoGetStream`], so it never
/// reached that stream's error arm. FAILS on pre-change `encode_do_get`: the
/// error mapping produced the `Status` but never called `record_status_error`
/// (nor bumped the probe), so the failure was swallowed — invisible in the logs
/// even at `RUST_LOG=debug` and absent from the error signal.
#[test]
fn encoder_stage_error_is_surfaced_not_swallowed() {
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field};

    // Advertised (schema message) schema: two columns.
    let schema_ref = Arc::new(ArrowSchema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
    ]));
    // Produced batch: only ONE column — a schema/batch divergence the Flight
    // encoder rejects when building the record-batch message (after the schema
    // message is already on the wire).
    let batch_schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "a",
        DataType::Int32,
        false,
    )]));
    let batch = RecordBatch::try_new(
        batch_schema,
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap();

    let pr = probe();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let status = rt.block_on({
        let pr = pr.clone();
        async move {
            let stream = futures::stream::iter(vec![Ok::<_, FlightError>(batch)]);
            let mut encoded = encode_do_get(stream, schema_ref, pr);
            // Message 1: the schema (Ok). Then the encoder error.
            let _schema = encoded.next().await.expect("schema message");
            let mut err_status = None;
            while let Some(item) = encoded.next().await {
                if let Err(s) = item {
                    err_status = Some(s);
                    break;
                }
            }
            err_status
        }
    });

    let status = status.expect("encoder-stage failure must surface as a Status, not a clean EOF");
    assert_eq!(
        status.code(),
        tonic::Code::Internal,
        "an egress encode failure maps to Status::internal, got: {status:?}"
    );
    assert_eq!(
        pr.errors_recorded.load(Ordering::Relaxed),
        1,
        "the encoder-stage egress failure must reach record_status_error exactly \
         once (issue #2193: it was previously swallowed with no log/signal at all)"
    );
}

// ---- Task 2.3 / Requirement 4: stream/collect byte-identity ----------------

fn collect_batches(producer: &MergeProducer, dir: &std::path::Path) -> Vec<RecordBatch> {
    producer.produce(&DirSource::new(dir)).unwrap()
}

// arrow-flight's `FlightError` Err type has a framework-fixed large size; boxing
// it (clippy's suggestion) would break the flight decoder stream API (#2856).
#[allow(clippy::result_large_err)]
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
            mpsc::channel::<Result<CreditedBatch, ProducerError>>(DO_GET_CHANNEL_CAPACITY);
        let cancel = CancelFlag::new();
        let sink_cancel = cancel.clone();
        let handle = tokio::task::spawn_blocking(move || {
            let mut sink = ChannelSink {
                tx,
                produced: Arc::new(AtomicUsize::new(0)),
                cancel: sink_cancel,
                credit: EgressCredit::new(EgressBudget::default(), EgressObservation::default()),
            };
            if let Err(e) = producer.produce_streaming(
                paths,
                &cancel,
                &mut sink,
                &crate::scan_progress::ScanProgress::default(),
                || {},
            ) {
                let _ = sink.tx.blocking_send(Err(e));
            }
        });
        let mut out = Vec::new();
        while let Some(item) = rx.recv().await {
            out.push(item.expect("streamed batch is ok").into_batch());
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

/// Issue #2361: `LIMIT` over a multi-SSTable k-way merge (two overlapping-key
/// SSTables, forcing real cross-generation reconciliation) must return the SAME
/// result set as the collect path, and the streamed row count must equal the
/// cap exactly (non-vacuity) — proving `LIMIT` is enforced correctly end-to-end
/// through the streaming egress purely via the consumer's post-reconciliation
/// break (there is no producer-side budget, roborev round 2).
#[test]
fn stream_collect_parity_limit_multi_sstable() {
    let schema = simple_schema();
    let a = (1..=10)
        .map(|i| write_row(i, &format!("a{i}"), i * 10, 100))
        .collect::<Vec<_>>();
    let b = (5..=15)
        .map(|i| write_row(i, &format!("b{i}"), i * 10, 200))
        .collect::<Vec<_>>();
    let (_temp, _data, dir) = build_sstables(&schema, vec![a, b]);

    let spec = crate::filter::ScanSpec {
        limit: Some(6),
        ..Default::default()
    };
    // Streamed == collect over a real multi-producer merge.
    assert_stream_collect_parity(spec.clone(), &dir);

    // Non-vacuity: the streaming egress returns EXACTLY the cap (6), not fewer
    // (under-return) nor all 15 distinct partitions.
    let producer = MergeProducer::with_spec(schema, 4, spec).unwrap();
    let paths = resolved(&producer, &dir);
    let streamed = stream_batches_raw(producer, paths);
    let rows: usize = streamed.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        rows, 6,
        "LIMIT-6 streaming scan over two overlapping SSTables must return exactly 6 rows"
    );
}

/// Regression pin (issue #2361, roborev round 2, BLOCKER 1): a sparse predicate
/// combined with `LIMIT k` must return exactly `k` MATCHING rows even when every
/// match sits in the token-ORDER TAIL of the SSTable. A per-producer
/// PARTITION budget (the removed design) would let the producer stop after the
/// first `k` (non-matching) partitions, the consumer's filter would then reject
/// all of them, and the split would return FEWER rows than exist — violating
/// `drive_merge`'s own doc and `ScanSpec::limit`'s doc (`limitGuaranteed = false`
/// permits MORE rows than the cap, never fewer). With no producer-side budget
/// (current design) this passes by construction: the producer scans until
/// genuinely cancelled, so the consumer sees every candidate row regardless of
/// where the matches physically sit in token order.
#[test]
fn stream_limit_returns_k_matches_even_when_concentrated_past_limit_index() {
    use crate::ticket::{FlightTicket, Predicate, PredicateOp};
    use cqlite_core::storage::write_engine::mutation::Mutation;

    const N: i32 = 20;
    const MATCHES: usize = 3;
    let schema = simple_schema();

    // Determine the ACTUAL token order for ids 1..=N (token order need not match
    // numeric id order — Murmur3 hashes the encoded partition key).
    let mut by_token: Vec<(i64, i32)> = (1..=N)
        .map(|id| {
            let m: Mutation = write_row(id, &format!("n{id}"), 1, 100);
            let token = m.decorated_key(&schema).unwrap().token;
            (token, id)
        })
        .collect();
    by_token.sort_by_key(|(t, _)| *t);

    // The matching ids are the LAST `MATCHES` in token order — i.e. strictly
    // AFTER where a partition-budget of `MATCHES` would have already stopped.
    let matching_ids: std::collections::HashSet<i32> = by_token[by_token.len() - MATCHES..]
        .iter()
        .map(|(_, id)| *id)
        .collect();
    assert_eq!(
        matching_ids.len(),
        MATCHES,
        "test precondition: exactly {MATCHES} distinct matching ids"
    );

    // score = 999 for the matching (token-tail) ids, 1 for everyone else.
    let rows: Vec<Mutation> = (1..=N)
        .map(|id| {
            let score = if matching_ids.contains(&id) { 999 } else { 1 };
            write_row(id, &format!("n{id}"), score, 100)
        })
        .collect();
    let (_temp, _data, dir) = build_sstables(&schema, vec![rows]);

    let ticket = FlightTicket {
        predicates: vec![Predicate {
            column: "score".into(),
            op: PredicateOp::Gte,
            value: serde_json::json!(500),
        }],
        limit: Some(MATCHES as u64),
        ..Default::default()
    };
    let spec = crate::filter::ScanSpec::from_ticket(&ticket, &schema).unwrap();

    let producer = MergeProducer::with_spec(schema, 1024, spec).unwrap();
    let paths = resolved(&producer, &dir);
    let streamed = stream_batches_raw(producer, paths);
    let total: usize = streamed.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total, MATCHES,
        "LIMIT-{MATCHES} with a sparse predicate whose matches sit in the token-order \
         tail must return all {MATCHES} matching rows, got {total} \
         (a producer-side partition budget would under-return here)"
    );
}

/// Regression pin (issue #2361, roborev round 2): `LIMIT k` over an UNFILTERED
/// scan where surviving rows sit behind SHADOWED (tombstoned) partitions in
/// token order must still return `k` surviving rows — a partition consumed by a
/// tombstone contributes ZERO rows but would still have counted against a
/// producer-side partition budget (the removed design). Two SSTables: gen1
/// writes every id, gen2 (newer) row-tombstones the ids that sort FIRST in
/// token order, leaving only later-token ids alive.
#[test]
fn stream_limit_over_shadowed_data_returns_k_surviving_rows() {
    use cqlite_core::storage::write_engine::mutation::Mutation;

    const N: i32 = 20;
    const SHADOWED: usize = 12;
    const LIMIT: usize = 5;
    let schema = simple_schema();

    let mut by_token: Vec<(i64, i32)> = (1..=N)
        .map(|id| {
            let m: Mutation = write_row(id, &format!("n{id}"), 1, 100);
            let token = m.decorated_key(&schema).unwrap().token;
            (token, id)
        })
        .collect();
    by_token.sort_by_key(|(t, _)| *t);

    // Shadow the FIRST `SHADOWED` ids in token order — LIMIT walking in token
    // order encounters them before any surviving row.
    let shadowed_ids: Vec<i32> = by_token[..SHADOWED].iter().map(|(_, id)| *id).collect();
    let surviving_count = N as usize - SHADOWED;
    assert!(
        surviving_count >= LIMIT,
        "test precondition: at least {LIMIT} surviving rows exist beyond the shadowed prefix"
    );

    let gen1: Vec<Mutation> = (1..=N)
        .map(|id| write_row(id, &format!("n{id}"), 1, 100))
        .collect();
    let gen2: Vec<Mutation> = shadowed_ids.iter().map(|&id| delete_row(id, 200)).collect();
    let (_temp, _data, dir) = build_sstables(&schema, vec![gen1, gen2]);

    let spec = crate::filter::ScanSpec {
        limit: Some(LIMIT as u64),
        ..Default::default()
    };
    let producer = MergeProducer::with_spec(schema, 1024, spec).unwrap();
    let paths = resolved(&producer, &dir);
    let streamed = stream_batches_raw(producer, paths);
    let total: usize = streamed.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total, LIMIT,
        "LIMIT-{LIMIT} over data with {SHADOWED} tombstoned partitions ahead (in token \
         order) of the surviving rows must still return {LIMIT} surviving rows, got {total} \
         (a producer-side partition budget would under-return here)"
    );
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
        MergeProducer::with_spec(schema.clone(), 4, crate::filter::ScanSpec::default()).unwrap();
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
            MergeInput::Paths(paths),
            schema_ref,
            RpcMetrics::start("do_get"),
            DO_GET_CHANNEL_CAPACITY,
            EgressBudget::default(),
            pr,
            CancelFlag::new(),
            timer(),
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
    // Roborev round 4: a normal, fully-consumed completion must NOT record an
    // error — only an actual mid-stream error or an unclean early drop does.
    assert_eq!(
        pr_check.errors_recorded.load(Ordering::Relaxed),
        0,
        "normal completion must not record an error"
    );
}

// ---- Issue #2419 (WS2): blocking-task gauge wiring -------------------------

/// End-to-end wiring evidence (issue #2419): the `cqlite.flight.blocking_tasks_in_use`
/// [`crate::saturation::BlockingTaskGuard`] is entered by the REAL streaming
/// `spawn_blocking` merge closure — not merely a standalone unit of the guard.
///
/// The test holds a REAL guard of its own (the production
/// `BlockingTaskGuard::enter`, never the private-atomic `enter_on`) across the
/// whole observation, then asserts four links — each a lower bound of the SOUND
/// kind defined normatively on
/// [`crate::saturation::blocking_tasks_in_use_level`] (issue #2896), so peer
/// tests can only raise these values and none can flake:
///
/// 1. `blocking_tasks_in_use_level() >= 1` while our guard is live — a real
///    `enter()` reaches the shared atomic the gauge is published from;
/// 2. `blocking_entries == 1` — the REAL `spawn_blocking` merge closure entered
///    exactly one guard (not a standalone unit of the guard);
/// 3. `blocking_entry_level >= 2` — that guard's own post-increment reading
///    counts our live guard plus its own `+1`, tying it to the SAME atomic;
/// 4. `blocking_tasks_in_use_level() >= 2` while the merge is still producing —
///    the closure HOLDS its guard for the merge's duration (the actual production
///    signal). Without this, an entered-and-immediately-dropped guard — leaving
///    the gauge reading 0 for the whole merge — would satisfy 1-3.
///
/// After the merge is joined, a final `>= 1` bounds the closure's RAII decrement
/// against underflow past our own still-held contribution (NOT a claim that the
/// drop was exactly `-1`; the exact rise/balance arithmetic is pinned against a
/// private atomic by `saturation::tests::blocking_task_guard_rises_and_balances`).
#[test]
fn blocking_tasks_gauge_tracks_real_streaming_do_get() {
    let n = 40;
    let (_temp, dir, schema) = many_partition_fixture(n);
    let producer = MergeProducer::with_spec(schema, 1, crate::filter::ScanSpec::default()).unwrap();
    let paths = resolved(&producer, &dir);
    let schema_ref = Arc::new(producer.arrow_schema().unwrap());

    let rt = tokio::runtime::Runtime::new().unwrap();
    let pr = probe();
    let pr_check = pr.clone();
    // A REAL guard on the shared production atomic (never the private-atomic
    // `enter_on`), held for the rest of the test: it raises the floor the merge
    // closure's own guard must read, anchoring every assertion below to the SAME
    // atomic the gauge is published from. It emits one true gauge reading, as any
    // production entry does; no test asserts on recorded gauge VALUES for
    // `cqlite.flight.blocking_tasks_in_use` (the OTel capture harness runs in a
    // separate integration-test process), so nothing else is perturbed.
    let own_guard = crate::saturation::BlockingTaskGuard::enter();
    // Link 1: our end of the anchor.
    assert!(
        crate::saturation::blocking_tasks_in_use_level() >= 1,
        "a real BlockingTaskGuard::enter() must increment the shared atomic that \
         backs cqlite.flight.blocking_tasks_in_use"
    );
    rt.block_on(async move {
        let (mut stream, handle) = spawn_streaming(
            producer,
            MergeInput::Paths(paths),
            schema_ref,
            RpcMetrics::start("do_get"),
            DO_GET_CHANNEL_CAPACITY,
            EgressBudget::default(),
            pr,
            CancelFlag::new(),
            timer(),
        );
        // Pull the schema + first batch: to emit a batch the merge closure must
        // have entered its `BlockingTaskGuard` and started producing. That channel
        // recv is ALSO the release/acquire edge that makes the `Relaxed` probe loads
        // below sound: the closure's probe writes happen-before its batch send, so
        // they are visible here. Do NOT hoist these asserts above the batch read —
        // without that edge they would be racy.
        let _schema_msg = read_one(&mut stream).await.expect("schema message");
        let _first_batch = read_one(&mut stream).await.expect("first batch");
        assert_eq!(
            pr_check.blocking_entries.load(Ordering::Relaxed),
            1,
            "the real spawn_blocking merge closure must enter exactly one \
             BlockingTaskGuard (proves the gauge is wired into the production path)"
        );
        assert!(
            pr_check.blocking_entry_level.load(Ordering::Relaxed) >= 2,
            "the closure's guard must increment the SAME shared \
             cqlite.flight.blocking_tasks_in_use atomic this test holds a guard on: \
             its own post-increment reading counts our live guard plus its own +1"
        );
        // Link 4: the closure still HOLDS its guard here, so the live level counts
        // both guards. Deterministic, not probabilistic: `batch_size = 1` over
        // n = 40 single-row partitions means the merge owes 40 sends, while
        // DO_GET_CHANNEL_CAPACITY is 4 — so having consumed only the first batch,
        // the producer can be at most ~capacity + in-flight-send + encoder-prefetch
        // batches ahead (the bound `first_batch_available_before_merge_completes`
        // and `slow_consumer_bounds_produced_batches` pin at this same point) and
        // is parked in `send` on the full channel with >30 batches still to emit. It
        // cannot leave the closure (dropping the guard) until it has sent them all
        // or is cancelled, and nothing cancels before the `drop(stream)` below. The
        // egress credit pool cannot end it early either: 12 MiB of default budget
        // versus 1-row batches, so the count governor binds first. If
        // DO_GET_CHANNEL_CAPACITY ever rises to >= n, this premise dies — keep the
        // two numbers apart rather than weakening the assert.
        assert!(
            crate::saturation::blocking_tasks_in_use_level() >= 2,
            "the merge closure's guard must still be HELD while it produces (our \
             guard + its guard); an entered-and-immediately-dropped guard would \
             leave cqlite.flight.blocking_tasks_in_use reading 0 for the whole merge"
        );
        // Drop the stream to cancel + join the merge; the guard drops on exit.
        drop(stream);
        let _ = handle.await;
    });

    // The merge's guard has dropped (the closure returned): its decrement did not
    // take the shared atomic below the contribution of the guard THIS test still
    // holds. An underflow bound only — see the doc comment.
    assert!(
        crate::saturation::blocking_tasks_in_use_level() >= 1,
        "the blocking-task gauge must not underflow past this test's own live guard"
    );
    drop(own_guard);
}

// ---- Issue #2162 Stage 1: rpc.rows/rpc.bytes move per batch ----------------

/// The rpc-progress seam moves BEFORE the stream is drained: after pulling the
/// schema + first batch from a slow consumer, `progressed_rows` is non-zero and
/// strictly below the fixture total, and at least one — but not all — per-batch
/// emissions have happened. On pre-#2162 `main`, rows are attributed only at
/// stream end, so nothing has moved at this observation point.
///
/// This is the feature-independent proof (via `StreamProbe`, which does not
/// depend on the `observability` OTel counters) that emission moved from
/// stream-end to per-batch — one emission per batch, never per row.
#[test]
fn rpc_progress_moves_before_stream_completes() {
    let n = 40;
    let (_temp, dir, schema) = many_partition_fixture(n);
    // batch_size = 1 → one row per batch, so emitted_batches == rows emitted.
    let producer = MergeProducer::with_spec(schema, 1, crate::filter::ScanSpec::default()).unwrap();
    let paths = resolved(&producer, &dir);
    let schema_ref = Arc::new(producer.arrow_schema().unwrap());

    let rt = tokio::runtime::Runtime::new().unwrap();
    let pr = probe();
    let pr_check = pr.clone();
    rt.block_on(async move {
        let (mut stream, handle) = spawn_streaming(
            producer,
            MergeInput::Paths(paths),
            schema_ref,
            RpcMetrics::start("do_get"),
            DO_GET_CHANNEL_CAPACITY,
            EgressBudget::default(),
            pr,
            CancelFlag::new(),
            timer(),
        );
        let _schema_msg = read_one(&mut stream).await.expect("schema");
        let _first = read_one(&mut stream).await.expect("first batch");

        let progressed = pr_check.progressed_rows.load(Ordering::Relaxed);
        let emitted = pr_check.emitted_batches.load(Ordering::Relaxed);
        assert!(
            (1..n as u64).contains(&progressed),
            "rpc.rows must have moved before drain: progressed={progressed} of {n}"
        );
        assert!(
            (1..n as usize).contains(&emitted),
            "at least one, but not all, per-batch emissions before drain: emitted={emitted}"
        );

        drop(stream);
        let _ = handle.await;
    });
}

/// The per-batch deltas sum to the unchanged total: draining the stream to
/// completion, the running progress (== summed per-batch deltas) equals the
/// fixture's total row count, and the number of per-batch emissions equals the
/// number of record batches — proving emission is per-batch, not per-row, and
/// the monotonic total is byte-identical to the pre-#2162 single emission.
#[test]
fn per_batch_deltas_sum_to_unchanged_total() {
    let schema = simple_schema();
    let rows = (1..=10)
        .map(|i| write_row(i, &format!("n{i}"), i * 10, 100))
        .collect::<Vec<_>>();
    let (_temp, _data, dir) = build_sstables(&schema, vec![rows]);

    // batch_size = 4 → ceil(10/4) = 3 batches, so per-batch (not per-row).
    let producer =
        MergeProducer::with_spec(schema.clone(), 4, crate::filter::ScanSpec::default()).unwrap();
    let expected = collect_batches(&producer, &dir);
    let expected_rows: u64 = expected.iter().map(|b| b.num_rows() as u64).sum();
    let expected_batches = expected.len();

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
            MergeInput::Paths(paths),
            schema_ref,
            RpcMetrics::start("do_get"),
            DO_GET_CHANNEL_CAPACITY,
            EgressBudget::default(),
            pr,
            CancelFlag::new(),
            timer(),
        );
        let _ = drain_stream(stream).await;
        let _ = handle.await;
    });

    assert!(expected_rows > 0, "fixture must produce rows");
    assert_eq!(
        pr_check.progressed_rows.load(Ordering::Relaxed),
        expected_rows,
        "summed per-batch deltas equal the total the single emission produced"
    );
    assert_eq!(
        pr_check.emitted_batches.load(Ordering::Relaxed),
        expected_batches,
        "one per-batch emission per record batch (per-batch, not per-row)"
    );
    // The finalized total (used for the terminal probe) matches too.
    assert_eq!(pr_check.rows.load(Ordering::Relaxed), expected_rows);
}

// ---- Issue #2162 Stage 3: core scan counters flush incrementally -----------

/// Build a probe whose scan-progress seam flushes every `threshold` examined
/// rows, so a modest fixture exercises multiple incremental flushes without
/// building 16k rows.
fn probe_with_threshold(threshold: u64) -> StreamProbe {
    StreamProbe {
        scan_progress: crate::scan_progress::ScanProgress::with_threshold(threshold),
        ..Default::default()
    }
}

/// Over a threshold-crossing full scan through the public Flight merge surface,
/// the scan-progress seam records at least TWO `cqlite.query.rows_scanned` delta
/// flushes (threshold crossings + the final remainder), and the summed deltas
/// equal the scan's total examined-row count. On pre-#2162 `main` the count is
/// exactly one — the single end-of-scan emission.
#[test]
fn rows_scanned_flushes_incrementally_over_threshold() {
    let schema = simple_schema();
    let rows = (1..=10)
        .map(|i| write_row(i, &format!("n{i}"), i * 10, 100))
        .collect::<Vec<_>>();
    let (_temp, _data, dir) = build_sstables(&schema, vec![rows]);

    let producer =
        MergeProducer::with_spec(schema.clone(), 4, crate::filter::ScanSpec::default()).unwrap();
    let expected = collect_batches(&producer, &dir);
    let total_rows: u64 = expected.iter().map(|b| b.num_rows() as u64).sum();
    assert!(total_rows >= 8, "fixture must cross the test threshold");

    let stream_producer =
        MergeProducer::with_spec(schema, 4, crate::filter::ScanSpec::default()).unwrap();
    let paths = resolved(&stream_producer, &dir);
    let schema_ref = Arc::new(stream_producer.arrow_schema().unwrap());

    // threshold = 4 → 10 rows yields 2 threshold crossings + a remainder flush.
    let pr = probe_with_threshold(4);
    let pr_check = pr.clone();
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        let (stream, handle) = spawn_streaming(
            stream_producer,
            MergeInput::Paths(paths),
            schema_ref,
            RpcMetrics::start("do_get"),
            DO_GET_CHANNEL_CAPACITY,
            EgressBudget::default(),
            pr,
            CancelFlag::new(),
            timer(),
        );
        let _ = drain_stream(stream).await;
        let _ = handle.await;
    });

    assert!(
        pr_check.scan_progress.flush_count() >= 2,
        "threshold-crossing scan must flush ≥2 rows_scanned deltas, got {}",
        pr_check.scan_progress.flush_count()
    );
    assert_eq!(
        pr_check.scan_progress.flushed_rows(),
        total_rows,
        "summed incremental deltas must equal the total examined-row count"
    );
}

/// A sub-threshold scan flushes exactly once — the final remainder — matching the
/// pre-#2162 single end-of-scan emission (the counter total is unchanged; only a
/// long scan gains incremental cadence).
#[test]
fn rows_scanned_sub_threshold_flushes_once() {
    let schema = simple_schema();
    let rows = (1..=5)
        .map(|i| write_row(i, &format!("n{i}"), i, 100))
        .collect::<Vec<_>>();
    let (_temp, _data, dir) = build_sstables(&schema, vec![rows]);

    let stream_producer =
        MergeProducer::with_spec(schema, 4, crate::filter::ScanSpec::default()).unwrap();
    let paths = resolved(&stream_producer, &dir);
    let schema_ref = Arc::new(stream_producer.arrow_schema().unwrap());

    // Default (production) threshold: 5 rows never crosses it → one remainder flush.
    let pr = probe();
    let pr_check = pr.clone();
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        let (stream, handle) = spawn_streaming(
            stream_producer,
            MergeInput::Paths(paths),
            schema_ref,
            RpcMetrics::start("do_get"),
            DO_GET_CHANNEL_CAPACITY,
            EgressBudget::default(),
            pr,
            CancelFlag::new(),
            timer(),
        );
        let _ = drain_stream(stream).await;
        let _ = handle.await;
    });

    assert_eq!(
        pr_check.scan_progress.flush_count(),
        1,
        "a sub-threshold scan flushes exactly once (the remainder)"
    );
    assert_eq!(pr_check.scan_progress.flushed_rows(), 5);
}

/// Early termination still flushes the remainder (issue #2162): a LIMIT that
/// stops the merge mid-scan must NOT lose the in-flight progress — the seam's
/// summed deltas equal the rows actually examined up to the cap, via the
/// `ScanProgressMeter`'s `Drop`.
#[test]
fn rows_scanned_flushes_remainder_on_limit_break() {
    use crate::filter::ScanSpec;
    let schema = simple_schema();
    let rows = (1..=20)
        .map(|i| write_row(i, &format!("n{i}"), i, 100))
        .collect::<Vec<_>>();
    let (_temp, _data, dir) = build_sstables(&schema, vec![rows]);

    // LIMIT 6 stops the merge after emitting 6 rows; the meter's Drop must still
    // flush the examined remainder for the entered (unflushed) rows.
    let spec = ScanSpec {
        limit: Some(6),
        ..Default::default()
    };
    let stream_producer = MergeProducer::with_spec(schema, 4, spec).unwrap();
    let paths = resolved(&stream_producer, &dir);
    let schema_ref = Arc::new(stream_producer.arrow_schema().unwrap());

    let pr = probe_with_threshold(4);
    let pr_check = pr.clone();
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        let (stream, handle) = spawn_streaming(
            stream_producer,
            MergeInput::Paths(paths),
            schema_ref,
            RpcMetrics::start("do_get"),
            DO_GET_CHANNEL_CAPACITY,
            EgressBudget::default(),
            pr,
            CancelFlag::new(),
            timer(),
        );
        let _ = drain_stream(stream).await;
        let _ = handle.await;
    });

    // At least one flush happened and the summed deltas equal the examined rows
    // (≥ the 6 emitted; the meter counts examined-before-predicate, and with no
    // predicate that equals the rows built up to the LIMIT break).
    assert!(
        pr_check.scan_progress.flush_count() >= 1,
        "a LIMIT-terminated scan must still flush its progress"
    );
    assert_eq!(
        pr_check.scan_progress.flushed_rows(),
        6,
        "summed deltas equal the rows examined up to the LIMIT break — none lost"
    );
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
            CancelFlag::new(),
            timer(),
        )
        .await
        {
            Ok(stream) => stream,
            Err((status, _metrics, _reason)) => panic!("aggregate response failed: {status}"),
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

/// **Issue #2264 core regression.** `ChannelSink::emit` parked in the bounded
/// channel (a full channel whose Receiver is kept ALIVE, so no send failure fires)
/// must be UNPARKED by the shared cancel flag — the exact forever-park the bug
/// filed. Fill the single-slot channel, spawn the merge sink `emit` on a blocking
/// thread where it must park in `reserve()`, then cancel the flag. Within 3s the
/// emit must return `Err(ProducerError::Cancelled)`.
///
/// FAILS on the pre-fix `emit` (bare `tx.blocking_send`): `blocking_send` wakes
/// ONLY on a freed permit or a dropped receiver, NEVER on the cancel flag, so with
/// the receiver held alive the task parks forever and this times out. PASSES after
/// the fix because `emit` races `reserve()` against `cancel.cancelled()`.
#[test]
fn channel_sink_emit_unparks_on_cancel_when_receiver_alive() {
    let schema = simple_schema();
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        // Cap 1 so a single pre-filled batch fills the channel; keep `_rx` bound so
        // the channel never closes (the send can only be released by cancellation).
        let (tx, _rx) = mpsc::channel::<Result<CreditedBatch, ProducerError>>(1);
        let cancel = CancelFlag::new();

        // A minimal 1-row batch matching `simple_schema`'s Arrow schema.
        let batch = {
            let producer = MergeProducer::new(schema.clone(), 1).unwrap();
            let arrow_schema = Arc::new(producer.arrow_schema().unwrap());
            let ncols = arrow_schema.fields().len();
            let cols: Vec<arrow::array::ArrayRef> = arrow_schema
                .fields()
                .iter()
                .map(|f| arrow::array::new_null_array(f.data_type(), 1))
                .collect();
            assert_eq!(cols.len(), ncols);
            RecordBatch::try_new(arrow_schema, cols).unwrap()
        };

        // Fill the single slot so the sink's next emit must park in `reserve()`.
        tx.send(Ok(CreditedBatch::uncredited(batch.clone())))
            .await
            .unwrap();

        let sink_cancel = cancel.clone();
        let emit_task = tokio::task::spawn_blocking(move || {
            let mut sink = ChannelSink {
                tx,
                produced: Arc::new(AtomicUsize::new(0)),
                cancel: sink_cancel,
                // Ample credit: this test pins the CHANNEL-slot park, not the
                // credit park (which `egress_budget_tests` covers separately).
                credit: EgressCredit::new(EgressBudget::default(), EgressObservation::default()),
            };
            sink.emit(CreditedBatch::uncredited(batch))
        });

        // Let the emit park waiting for a permit, then cancel (client disconnect).
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        cancel.cancel();

        let outcome = tokio::time::timeout(std::time::Duration::from_secs(3), emit_task).await;
        let joined = outcome.expect("emit must return within 3s once cancelled, not park forever");
        match joined.expect("emit task joins") {
            Err(ProducerError::Cancelled) => {}
            Ok(()) => panic!("cancelled emit under backpressure must return Cancelled, got Ok"),
            Err(other) => {
                panic!("cancelled emit under backpressure must return Cancelled, got {other:?}")
            }
        }
    });
}

/// A cancelled stream (one batch read, then dropped) attributes exactly the
/// emitted prefix — not the full table, and records exactly ONE error (roborev
/// round 4: pre-change a disconnect surfaced as `aborted` through the handler's
/// `Err` path and hit the same error-observability hook as any other failure —
/// `Drop` must reproduce that, not let a disconnect vanish from the signal).
#[test]
fn metrics_attribute_emitted_prefix_on_cancel() {
    let n = 60;
    let (_temp, dir, schema) = many_partition_fixture(n);
    let producer = MergeProducer::with_spec(schema, 1, crate::filter::ScanSpec::default()).unwrap();
    let paths = resolved(&producer, &dir);
    let schema_ref = Arc::new(producer.arrow_schema().unwrap());

    let rt = tokio::runtime::Runtime::new().unwrap();
    let pr = probe();
    let pr_check = pr.clone();
    rt.block_on(async move {
        let (mut stream, handle) = spawn_streaming(
            producer,
            MergeInput::Paths(paths),
            schema_ref,
            RpcMetrics::start("do_get"),
            DO_GET_CHANNEL_CAPACITY,
            EgressBudget::default(),
            pr,
            CancelFlag::new(),
            timer(),
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
    assert_eq!(
        pr_check.errors_recorded.load(Ordering::Relaxed),
        1,
        "an early drop (client disconnect) must record exactly one error, \
         same as a returned Err used to before this rewrite"
    );
}

/// **Issue #2680 / P0 #2782 — server-side egress hardening.** A `do_get` response
/// stream whose inner merge is PARKED (bounded channel empty, no batch ready, the
/// receiver still alive so no send-failure fires) must terminate promptly when the
/// shared [`CancelFlag`] trips from ANY source — a half-closed peer, a transport
/// reset, or the drop guard — NOT wait for the merge to notice cancellation between
/// steps. The `select!` in `MeteredDoGetStream::poll_next` races the parked inner
/// poll against the flag's async cancellation and ends the stream cleanly (`None`).
///
/// FAILS on pre-hardening `poll_next` (a bare `Poll::Pending` passthrough): with the
/// channel empty and its sender held, the inner stream parks forever and the stream
/// never resolves, so this times out. PASSES with the fix because the cancellation
/// arm fires at the next poll after the flag trips.
#[test]
fn metered_stream_ends_when_cancel_flag_trips_while_inner_parked() {
    let schema = simple_schema();
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        let schema_ref = Arc::new(
            MergeProducer::new(schema, 4)
                .unwrap()
                .arrow_schema()
                .unwrap(),
        );
        // A channel whose sender is HELD (never sends, never drops): the inner
        // ReceiverStream polls Pending forever — the "merge parked, receiver alive"
        // shape that a bare Pending passthrough could never break out of.
        let (_tx, rx) = mpsc::channel::<Result<CreditedBatch, ProducerError>>(1);
        let inner = Box::pin(ReceiverStream { rx });

        let cancel = CancelFlag::new();
        let guard = cancel.drop_guard();
        let cancelled = Box::pin(cancel.cancelled());
        let pr = probe();
        let metered = MeteredDoGetStream::new(
            inner,
            RpcMetrics::start("do_get"),
            Some(guard),
            pr.clone(),
            None,
            Some(cancelled),
        );
        let mut stream = encode_do_get(metered, schema_ref, pr);

        // The schema message is emitted eagerly by the encoder even for an empty
        // result; pull it so the next poll reaches the parked inner stream.
        let _schema_msg =
            tokio::time::timeout(std::time::Duration::from_secs(3), read_one(&mut stream))
                .await
                .expect("schema message arrives without a hang")
                .expect("schema message present");

        // Trip the flag from "another thread" (a half-closed peer / transport reset).
        cancel.cancel();

        // Egress must end (schema-only stream, no batches) within a bound, not park.
        let next =
            tokio::time::timeout(std::time::Duration::from_secs(3), read_one(&mut stream)).await;
        let ended = next.expect("stream must resolve within 3s of the cancel, not park forever");
        assert!(
            ended.is_none(),
            "a cancelled parked stream ends cleanly (None), got a further message: {ended:?}"
        );
    });
}

/// Issue #2819 (M1 / L4b): the in-`stream` sub-phase sink is installed ONLY when
/// metrics are actually collected. `spawn_streaming` gates the install on
/// `cqlite_core::observability::metrics_active()`; with the meter OFF — the state
/// in a plain unit test, and in any deploy that never initialised OTel — the
/// gating installs `None`, so the merge thread's `RowSubPhaseAccum` is inert and
/// the hot row loop takes ZERO `Instant::now()`. This pins that decision:
/// meter-off ⇒ no sink installed (else the loop would pay per-row clock reads for
/// samples `record_histogram` discards anyway).
#[test]
fn meter_off_installs_no_subphase_sink() {
    use cqlite_core::observability::{metrics_active, stream_subphase, StreamSubPhaseTimings};

    assert!(
        !metrics_active(),
        "a plain unit test installs no meter, so metrics must be inactive"
    );
    // The EXACT expression `spawn_streaming` uses to build the install argument.
    let subphase = Arc::new(StreamSubPhaseTimings::default());
    let _guard = stream_subphase::install(metrics_active().then(|| subphase.clone()));
    assert!(
        stream_subphase::current().is_none(),
        "meter-off do_get must install NO sub-phase sink (spawn_streaming M1 gating)"
    );
}

// MEASUREMENT ONLY (issue #3742): does a zero-column `RecordBatch`'s explicit
// row count survive the real `do_get` encoder plus an arrow-flight client
// decode? Declared here rather than in `streaming.rs` so the production module
// stays under the campsite source threshold (epic #1116); a grandchild module
// still sees `crate::streaming`'s private `encode_do_get`, which is the encoder
// the real `do_get` response stream uses.
#[path = "issue_3742_zero_column_wire_tests.rs"]
mod issue_3742_zero_column_wire_tests;
