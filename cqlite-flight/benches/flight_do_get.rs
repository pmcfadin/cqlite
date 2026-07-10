//! End-to-end Flight `do_get` streaming-throughput bench (issue #1494, AD5).
//!
//! Tier-2 of the export/Flight perf net (epic #1469): the **wiring-evidence**
//! surface. Unlike the cqlite-core converter micro-benches (which isolate
//! per-cell conversion cost), this drives the **public tonic
//! `FlightService::do_get` RPC over a real loopback transport** — a real
//! `FlightServiceClient` → gRPC wire → server → `FlightRecordBatchStream` decode
//! — reusing the harness pattern proven by
//! `cqlite-flight/tests/do_get_transport_test.rs`. It catches a producer / merge
//! / transport regression the converter benches cannot see.
//!
//! It is an **ADVISORY** perf-gate entry (`flight/do_get` in
//! `cqlite-core/benches/perf-gate.json`): its wall time is Tokio-runtime +
//! tonic-transport + I/O dominated, so it is reported but never fails CI (the
//! `write/ingest_wal_on` precedent). The hard, load-deterministic signal for the
//! producer path is the dhat budget guard
//! (`tests/issue_1494_producer_mem_budget.rs`), not this wall clock.
//!
//! Wiring evidence / non-vacuity: setup runs one full `do_get` and **panics**
//! unless the decoded stream yields ≥ 1 row, so a broken server / empty fixture
//! can never record a fake measurement.
//!
//! Reproduce:
//! ```text
//! cargo bench -p cqlite-flight --bench flight_do_get
//! ```

use std::time::Duration;

use arrow::record_batch::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::Ticket;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use futures::StreamExt;
use tokio::runtime::Runtime;
use tonic::transport::server::TcpIncoming;
use tonic::transport::{Channel, Server};

use cqlite_core::storage::write_engine::{WriteEngine, WriteEngineConfig};
use cqlite_flight::service::CqliteFlightService;
use cqlite_flight::test_fixtures as fx;

/// Rows flushed into the single-SSTable fixture. Large enough that the do_get
/// streams a meaningful batch (the whole set lands in one `batch_size = 8192`
/// batch), so the measurement reflects producer + transport + decode cost rather
/// than fixed connect overhead alone.
const FIXTURE_ROWS: usize = 2_000;
const BATCH_SIZE: usize = 8192;

/// Flush `FIXTURE_ROWS` `keyvalue` rows into a fresh single-SSTable fixture and
/// return its data dir (temp dir kept alive by the returned guard).
fn build_fixture() -> (tempfile::TempDir, std::path::PathBuf) {
    let schema = fx::keyvalue_schema();
    let temp = tempfile::TempDir::new().expect("temp dir");
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");
    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, schema);
    let mut engine = WriteEngine::new(config).expect("write engine");
    for i in 0..FIXTURE_ROWS {
        engine
            .write(fx::keyvalue_write(&format!("k{i:06}"), &format!("v{i}")))
            .expect("write mutation");
    }
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(engine.flush())
        .expect("flush")
        .expect("flush info");
    (temp, data_dir)
}

/// The connector-shaped JSON ticket for the `keyvalue` fixture.
fn ticket_bytes() -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "keyspace": fx::KEYVALUE_KS,
        "table": fx::KEYVALUE_TBL,
        "ddl": fx::KEYVALUE_DDL,
    }))
    .expect("ticket json")
}

/// Stand up a real loopback tonic server serving a fresh `CqliteFlightService`
/// over `data_dir`, returning the server task handle, a connected real
/// `FlightServiceClient`, and the bound address.
async fn serve_and_connect(
    data_dir: std::path::PathBuf,
) -> (
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    FlightServiceClient<Channel>,
) {
    let svc = CqliteFlightService::new(data_dir, BATCH_SIZE);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("local addr");
    let incoming = TcpIncoming::from_listener(listener, true, None).expect("incoming");

    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(FlightServiceServer::new(svc))
            .serve_with_incoming(incoming)
            .await
    });

    let endpoint = Channel::from_shared(format!("http://{addr}"))
        .expect("endpoint")
        .connect_timeout(Duration::from_secs(5));
    let mut channel = None;
    for _ in 0..50 {
        if let Ok(c) = endpoint.connect().await {
            channel = Some(c);
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let channel = channel.expect("connect to loopback flight server");
    (server, FlightServiceClient::new(channel))
}

/// Run ONE `do_get` over the real transport with `client` and return the decoded
/// batches (the throughput unit is total rows decoded).
async fn do_get_batches(
    client: &mut FlightServiceClient<Channel>,
    ticket: Vec<u8>,
) -> Vec<RecordBatch> {
    let resp = client
        .do_get(Ticket::new(ticket))
        .await
        .expect("do_get rpc");
    let stream = resp.into_inner().map(|r| r.map_err(FlightError::Tonic));
    let mut rb = FlightRecordBatchStream::new_from_flight_data(stream);
    let mut batches = Vec::new();
    while let Some(batch) = rb.next().await {
        batches.push(batch.expect("decode flight batch"));
    }
    batches
}

fn bench_flight_do_get(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");
    let (_temp, data_dir) = build_fixture();
    let (server, mut client) = rt.block_on(serve_and_connect(data_dir));

    // Non-vacuity / wiring evidence: a full do_get must decode ≥ 1 row over the
    // real transport before we record any measurement.
    let warm = rt.block_on(do_get_batches(&mut client, ticket_bytes()));
    let rows: usize = warm.iter().map(|b| b.num_rows()).sum();
    assert!(
        rows >= 1,
        "flight_do_get: do_get over the real transport decoded 0 rows — refusing \
         to record a vacuous measurement (the public FlightService::do_get path \
         did not stream the fixture)"
    );
    eprintln!(
        "flight_do_get: do_get decoded {rows} rows across {} batch(es)",
        warm.len()
    );

    let mut group = c.benchmark_group("flight");
    group.throughput(Throughput::Elements(rows as u64));
    group.bench_function("do_get", |b| {
        b.iter(|| {
            let batches = rt.block_on(do_get_batches(&mut client, black_box(ticket_bytes())));
            let n: usize = batches.iter().map(|bb| bb.num_rows()).sum();
            black_box(n)
        });
    });
    group.finish();

    server.abort();
}

criterion_group!(benches, bench_flight_do_get);
criterion_main!(benches);
