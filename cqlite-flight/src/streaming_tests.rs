//! Tests for `crate::streaming` (issue #1476, AB1).
//!
//! Split out of `streaming.rs` to keep the production module under the
//! campsite file-size threshold (`~800` source lines) — this file is a test
//! module (loaded via `#[path]` from `streaming.rs`), well under the `~1500`
//! test-file threshold. See epic #1116/#1135.

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
    let (tx, mut rx) = mpsc::channel::<Result<RecordBatch, ProducerError>>(1);
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
            panic!("a panic must forward ProducerError::Panicked, not a silent close: {other:?}")
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
        let (tx, rx) = mpsc::channel::<Result<RecordBatch, ProducerError>>(DO_GET_CHANNEL_CAPACITY);
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
        let metered = MeteredDoGetStream::new(inner, RpcMetrics::start("do_get"), None, pr.clone());
        let mut stream = encode_do_get(metered, schema_ref);

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
    let producer = MergeProducer::with_spec(schema, 1, crate::filter::ScanSpec::default()).unwrap();
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
