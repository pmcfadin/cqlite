//! End-to-end transport reproduction for issue #2193.
//!
//! The in-process `do_get` unit tests hand the caller `FlightData` structs
//! directly, so they never exercise the gRPC protobuf serialize → wire →
//! deframe → arrow-flight client decode path where the field failure
//! (`FlightRuntimeException: Failed to read message.`) actually occurs. This
//! test stands up a REAL tonic server + REAL `FlightServiceClient` over a
//! loopback TCP socket and decodes the response with the REAL
//! `FlightRecordBatchStream`, so a malformed egress message surfaces the same
//! way the Trino client saw it.

use std::collections::HashMap;
use std::time::Duration;

use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::Ticket;
use futures::StreamExt;
use tonic::transport::server::TcpIncoming;
use tonic::transport::{Channel, Server};

use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use cqlite_flight::service::CqliteFlightService;

const KS: &str = "cassandra_easy_stress";
const TBL: &str = "keyvalue";
// The exact cassandra-easy-stress KeyValue shape from the round-3 field run:
// a text partition key + a single text value column, no clustering key.
const DDL: &str = "CREATE TABLE cassandra_easy_stress.keyvalue (key text PRIMARY KEY, value text)";

fn simple_schema() -> TableSchema {
    let col = |name: &str, ty: &str, nullable: bool| Column {
        name: name.into(),
        data_type: ty.into(),
        nullable,
        default: None,
        is_static: false,
    };
    TableSchema {
        keyspace: KS.into(),
        table: TBL.into(),
        partition_keys: vec![KeyColumn {
            name: "key".into(),
            data_type: "text".into(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![col("key", "text", false), col("value", "text", true)],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

fn write_row(key: &str, value: &str, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("key", Value::Text(key.into())),
        None,
        vec![CellOperation::Write {
            column: "value".into(),
            value: Value::Text(value.into()),
        }],
        ts,
        None,
    )
}

/// Flush the 3-row single-SSTable fixture (matching the field `tiny` table:
/// a small flushed table that reads cleanly) and return its data dir.
fn build_fixture() -> (tempfile::TempDir, std::path::PathBuf) {
    let schema = simple_schema();
    let temp = tempfile::TempDir::new().unwrap();
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");
    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, schema);
    let mut engine = WriteEngine::new(config).expect("engine");
    for m in [
        write_row("k1", "1", 100),
        write_row("k2", "2", 100),
        write_row("k3", "3", 100),
    ] {
        engine.write(m).expect("write");
    }
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(engine.flush()).expect("flush").expect("info");
    (temp, data_dir)
}

/// The on-the-wire ticket is JSON (the `#[non_exhaustive]` `FlightTicket` is
/// only constructible inside the crate); build the same bytes the connector
/// would send. The server's `from_bytes` applies serde defaults for the rest.
fn ticket_bytes() -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "keyspace": KS,
        "table": TBL,
        "ddl": DDL,
    }))
    .unwrap()
}

/// Serve `svc` over a real loopback tonic server, run `do_get` with `ticket`
/// through a real `FlightServiceClient`, and decode the response with the real
/// `FlightRecordBatchStream`. Returns the decoded row count. Any malformed
/// egress surfaces exactly as it did for the Trino client (a decode `Err`).
async fn do_get_rows_over_transport(svc: CqliteFlightService, ticket: Vec<u8>) -> usize {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = TcpIncoming::from_listener(listener, true, None).unwrap();

    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(FlightServiceServer::new(svc))
            .serve_with_incoming(incoming)
            .await
    });

    // Connect a real Flight client over loopback TCP (retry until accepting).
    let endpoint = Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect_timeout(Duration::from_secs(5));
    let mut channel = None;
    let mut last_err = None;
    for _ in 0..50 {
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

    let mut client = FlightServiceClient::new(channel);
    let resp = client
        .do_get(Ticket::new(ticket))
        .await
        .expect("do_get rpc");
    let stream = resp.into_inner().map(|r| r.map_err(FlightError::Tonic));
    let mut rb = FlightRecordBatchStream::new_from_flight_data(stream);

    let mut rows = 0usize;
    while let Some(batch) = rb.next().await {
        let batch = batch.expect("decode flight batch over transport");
        rows += batch.num_rows();
    }
    server.abort();
    rows
}

/// Reproduces issue #2193: a 3-row nb-big table served over the real gRPC
/// transport must decode to 3 rows with the real arrow-flight client. Before
/// the fix the client fails with a "Failed to read message"-class decode error.
#[test]
fn do_get_over_real_transport_decodes_all_rows() {
    let (_temp, data_dir) = build_fixture();
    // `batch_size = 8192` matches the field flight image, so all 3 rows land in
    // one final-flush batch — the exact shape that failed in the round-3 run.
    let svc = CqliteFlightService::new(data_dir, 8192);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let rows = rt.block_on(do_get_rows_over_transport(svc, ticket_bytes()));
    assert_eq!(
        rows, 3,
        "all 3 rows must round-trip through the real transport"
    );
}

/// The field failure was against a REAL Cassandra 5.0 `nb-big` + compressed
/// SSTable (LZ4, 16KB chunks) read through the `V5CompressedLegacy` path — the
/// exact read path the synthetic write-engine fixture never exercises. Point
/// the service at a real compressed fixture from the corpus and decode it over
/// the real transport with the real client.
///
/// The decoded row count is asserted `> 0` so the test can never pass on an
/// empty dataset (the 0-rows-when-present failure mode).
#[test]
fn do_get_over_transport_real_compressed_fixture() {
    let Some(root) = std::env::var_os("CQLITE_DATASETS_ROOT") else {
        eprintln!("CQLITE_DATASETS_ROOT unset — skipping real-fixture repro");
        return;
    };
    // The service resolves `<data_dir>/<keyspace>/<table>[-<uuid>]`, so point
    // `data_dir` at `sstables/` and let the ticket carry keyspace `test_basic`.
    let data_dir = std::path::PathBuf::from(&root).join("sstables");
    // Skip when the gitignored `Data.db` BINARY is absent (the repo ships only
    // the JSONL references, so the fixture DIRECTORY exists even in a worktree
    // that never ran `fetch-datasets.sh`). Checking the actual `Data.db` file —
    // not just the dir — is what stops this from silently 0-row-passing on an
    // unfetched checkout while still asserting `> 0` whenever the binary is real.
    let data_db = data_dir
        .join("test_basic")
        .join("simple_table-6aa08200a25111f0a3fef1a551383fb9")
        .join("nb-1-big-Data.db");
    if !data_db.is_file() {
        eprintln!("real fixture Data.db binary absent (run fetch-datasets.sh) — skipping");
        return;
    }
    let ddl = "CREATE TABLE test_basic.simple_table (\
        id uuid PRIMARY KEY, name text, age int, salary bigint, height float, \
        weight double, active boolean, created timestamp, birth_date date, \
        work_time time, description blob, account_balance decimal, \
        session_id timeuuid, ip_address inet, small_number tinyint, \
        medium_number smallint, duration_val duration, varchar_field varchar, \
        ascii_field ascii)";
    let ticket = serde_json::to_vec(&serde_json::json!({
        "keyspace": "test_basic",
        "table": "simple_table",
        "ddl": ddl,
    }))
    .unwrap();

    let svc = CqliteFlightService::new(data_dir, 8192);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let rows = rt.block_on(do_get_rows_over_transport(svc, ticket));
    assert!(
        rows > 0,
        "real compressed nb-big fixture must decode to > 0 rows over transport"
    );
}
