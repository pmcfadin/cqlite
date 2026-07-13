//! Shared harness for the issue #2370 N-concurrent `do_get` coverage suite.
//!
//! Every integration binary in the suite (`issue_2370_concurrent_doget_test`,
//! `issue_2370_gauge_readback_test`, `issue_2370_single_flight_test`) compiles
//! this module independently, so unused helpers in any one binary are expected —
//! hence the crate-local `allow(dead_code)`. The helpers stand up ONE real
//! loopback tonic server serving a shared `CqliteFlightService` and connect N
//! real `FlightServiceClient`s over it, so the tests exercise the true gRPC
//! handler path (not the in-process `do_get`), the layer both #2316 and #2361
//! escaped through.
//!
//! Fixtures are built via the write engine (which emits UNCOMPRESSED `nb-big`
//! SSTables — the non-stitching read path), interleaving keys across two flushes
//! so any first-N result must span BOTH generations (the #2157 early-stop guard,
//! reused here so a concurrency bug that drops a generation is observable).

#![allow(dead_code)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use arrow::array::{Array, StringArray};
use arrow::record_batch::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::Ticket;
use futures::StreamExt;
use tonic::transport::server::TcpIncoming;
use tonic::transport::{Channel, Server};

use cqlite_core::storage::write_engine::{WriteEngine, WriteEngineConfig};
use cqlite_flight::service::CqliteFlightService;
use cqlite_flight::test_fixtures as fx;

/// A running loopback tonic server plus the address to dial it on. The caller
/// keeps this alive for the duration of the concurrent workload and `.abort()`s
/// `server` when done.
pub struct RunningServer {
    pub addr: SocketAddr,
    pub server: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

/// Flush `total` rows across TWO separate memtable flushes so the resulting
/// `data_dir` holds TWO `nb-*-big-Data.db` SSTables the server must k-way-merge.
/// Keys are interleaved (even indices → flush 1, odd indices → flush 2) so the
/// sorted first-N result for any `N >= 2` necessarily draws from BOTH
/// generations. Asserts the fixture really produced ≥2 SSTables.
pub fn build_multi_sstable_fixture(total: usize) -> (tempfile::TempDir, PathBuf) {
    assert!(total >= 4, "need enough rows to span two flushes");
    let schema = fx::keyvalue_schema();
    let temp = tempfile::TempDir::new().unwrap();
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");
    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, schema);
    let mut engine = WriteEngine::new(config).expect("engine");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for i in (0..total).step_by(2) {
        engine
            .write(fx::keyvalue_write(&format!("k{i:06}"), &format!("v{i}")))
            .expect("write");
    }
    rt.block_on(engine.flush())
        .expect("flush 1")
        .expect("info 1");
    for i in (1..total).step_by(2) {
        engine
            .write(fx::keyvalue_write(&format!("k{i:06}"), &format!("v{i}")))
            .expect("write");
    }
    rt.block_on(engine.flush())
        .expect("flush 2")
        .expect("info 2");

    let table_dir = data_dir.join(fx::KEYVALUE_KS).join(fx::KEYVALUE_TBL);
    let data_files = std::fs::read_dir(&table_dir)
        .expect("table dir")
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_str()
                .is_some_and(|n| n.ends_with("-Data.db"))
        })
        .count();
    assert!(
        data_files >= 2,
        "fixture must span ≥2 SSTables (found {data_files}) so a concurrency bug \
         that drops a generation is observable",
    );
    (temp, data_dir)
}

/// Stand up ONE real loopback tonic server serving `svc`. All N clients in a
/// concurrency test dial the SAME returned address, so they share one server
/// process exactly as the field (a single Flight endpoint under Trino load).
pub async fn start_server(svc: CqliteFlightService) -> RunningServer {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = TcpIncoming::from_listener(listener, true, None).unwrap();
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(FlightServiceServer::new(svc))
            .serve_with_incoming(incoming)
            .await
    });
    RunningServer { addr, server }
}

/// Connect a real Flight client to `addr` over loopback TCP, retrying until the
/// server accepts.
pub async fn connect(addr: SocketAddr) -> FlightServiceClient<Channel> {
    let endpoint = Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect_timeout(Duration::from_secs(5));
    let mut channel = None;
    let mut last_err = None;
    for _ in 0..100 {
        match endpoint.connect().await {
            Ok(c) => {
                channel = Some(c);
                break;
            }
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
    let channel = channel.unwrap_or_else(|| panic!("connect failed: {last_err:?}"));
    FlightServiceClient::new(channel)
}

/// A full-scan ticket (no filter, no limit).
pub fn scan_ticket() -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "keyspace": fx::KEYVALUE_KS,
        "table": fx::KEYVALUE_TBL,
        "ddl": fx::KEYVALUE_DDL,
    }))
    .unwrap()
}

/// A `LIMIT n` ticket over the multi-SSTable fixture.
pub fn limit_ticket(n: u64) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "keyspace": fx::KEYVALUE_KS,
        "table": fx::KEYVALUE_TBL,
        "ddl": fx::KEYVALUE_DDL,
        "limit": n,
    }))
    .unwrap()
}

/// A PK-equality point-read ticket for `key` (the single `text` partition key).
pub fn point_ticket(key: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "keyspace": fx::KEYVALUE_KS,
        "table": fx::KEYVALUE_TBL,
        "ddl": fx::KEYVALUE_DDL,
        "filter": {"type": "Compare", "column": "key", "op": "Equal", "value": key},
    }))
    .unwrap()
}

/// Run `do_get(ticket)` against `client`, fully draining the decoded stream, and
/// return every `RecordBatch`.
pub async fn do_get_batches(
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
        batches.push(batch.expect("decode flight batch over transport"));
    }
    batches
}

/// Collect the string values of `column` across all `batches` (used for
/// per-shape key-set correctness).
pub fn column_strings(batches: &[RecordBatch], column: &str) -> Vec<String> {
    let mut out = Vec::new();
    for batch in batches {
        let idx = batch
            .schema()
            .index_of(column)
            .unwrap_or_else(|_| panic!("column {column} present in output"));
        let arr = batch
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("text column decodes as StringArray");
        for i in 0..arr.len() {
            out.push(arr.value(i).to_string());
        }
    }
    out
}

/// Parse the numeric index `N` out of a `kNNNNNN` fixture key.
pub fn key_index(key: &str) -> usize {
    key.strip_prefix('k')
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| panic!("fixture key {key:?} must be of the form kNNNNNN"))
}

/// Find the real compressed corpus fixture's `nb-*-big-Data.db` binary under
/// `data_dir/<keyspace>/<table>-<uuid>/`, by GLOB rather than a hardcoded uuid +
/// generation number: scans `data_dir/<keyspace>` for a `<table>-*` directory
/// entry, then that directory for an `nb-*-big-Data.db` file. This is what keeps
/// the real-corpus concurrency arm from silently skipping forever after a
/// dataset regen changes the table's uuid or generation number (it would
/// instead just find the regenerated file under the same table-name prefix).
/// Returns `None` (never panics) when the keyspace dir, table dir, or Data.db
/// binary is absent — the caller treats that as "fixture not fetched, skip".
pub fn find_real_compressed_fixture(
    data_dir: &std::path::Path,
    keyspace: &str,
    table: &str,
) -> Option<PathBuf> {
    let ks_dir = data_dir.join(keyspace);
    let table_prefix = format!("{table}-");
    let table_dir = std::fs::read_dir(&ks_dir)
        .ok()?
        .flatten()
        .find(|e| {
            e.file_type().is_ok_and(|t| t.is_dir())
                && e.file_name().to_string_lossy().starts_with(&table_prefix)
        })?
        .path();
    std::fs::read_dir(&table_dir)
        .ok()?
        .flatten()
        .find(|e| {
            let name = e.file_name();
            let name = name.to_string_lossy();
            name.starts_with("nb-") && name.ends_with("-big-Data.db")
        })
        .map(|e| e.path())
}
