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
use std::path::Path;
use std::time::Duration;

use arrow::array::{Array, StringArray};
use arrow::ipc::{root_as_message, MessageHeader};
use arrow::record_batch::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::{FlightData, Ticket};
use futures::StreamExt;
use prost::Message as _;
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
    do_get_batches_over_transport(svc, ticket)
        .await
        .iter()
        .map(|b| b.num_rows())
        .sum()
}

/// Same real-transport round-trip as [`do_get_rows_over_transport`] but returns
/// every decoded `RecordBatch` so a caller can assert on the actual column set
/// and cell values (not just the row count).
async fn do_get_batches_over_transport(
    svc: CqliteFlightService,
    ticket: Vec<u8>,
) -> Vec<RecordBatch> {
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

    let mut batches = Vec::new();
    while let Some(batch) = rb.next().await {
        batches.push(batch.expect("decode flight batch over transport"));
    }
    server.abort();
    batches
}

/// The SAME real gRPC round-trip as [`do_get_batches_over_transport`], but returns
/// the RAW `Vec<FlightData>` protobuf messages straight off the response stream —
/// i.e. the exact on-the-wire bytes, captured BEFORE they are wrapped into a
/// `FlightRecordBatchStream` (the point where the arrow-java client would decode
/// them). This lets a caller pin the server's message SEQUENCE at the Flight
/// protobuf level, not the decoded-`RecordBatch` level (issue #2193).
async fn do_get_raw_flight_data_over_transport(
    svc: CqliteFlightService,
    ticket: Vec<u8>,
) -> Vec<FlightData> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = TcpIncoming::from_listener(listener, true, None).unwrap();

    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(FlightServiceServer::new(svc))
            .serve_with_incoming(incoming)
            .await
    });

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
    // `resp.into_inner()` yields the raw `FlightData` protobuf messages exactly as
    // deframed off the wire — the pre-decode interception point.
    let mut inner = resp.into_inner();
    let mut messages = Vec::new();
    while let Some(msg) = inner.next().await {
        messages.push(msg.expect("raw FlightData message over transport"));
    }
    server.abort();
    messages
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

/// **Message-sequence pin (issue #2193).** For the 3-row field shape the server's
/// `do_get` must emit EXACTLY two `FlightData` messages on the wire — a Schema
/// message followed by a single RecordBatch message — and each message's
/// `data_header` must be the matching IPC `Message` flatbuffer. This pins the
/// on-the-wire shape the arrow-java Flight decoder consumes (the Java oracle
/// `FlightDataGoldenDecodeTest` decodes the committed form of exactly this
/// sequence), capturing at the protobuf-message level rather than the decoded
/// `RecordBatch` level so a framing/ordering regression surfaces here.
#[test]
fn do_get_over_transport_emits_schema_then_recordbatch() {
    let (_temp, data_dir) = build_fixture();
    let svc = CqliteFlightService::new(data_dir, 8192);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let messages = rt.block_on(do_get_raw_flight_data_over_transport(svc, ticket_bytes()));

    assert_eq!(
        messages.len(),
        2,
        "3-row field shape must emit [schema, record-batch], got {} messages",
        messages.len()
    );

    let header_type = |fd: &FlightData| {
        root_as_message(&fd.data_header)
            .expect("data_header must be a valid IPC Message flatbuffer")
            .header_type()
    };
    assert_eq!(
        header_type(&messages[0]),
        MessageHeader::Schema,
        "message[0] data_header must parse as a Schema flatbuffer"
    );
    assert_eq!(
        header_type(&messages[1]),
        MessageHeader::RecordBatch,
        "message[1] data_header must parse as a RecordBatch flatbuffer"
    );
}

/// **Golden cross-check (issue #2193 review).** The transport-captured raw
/// `FlightData` sequence for this SAME field-shape fixture must be
/// byte-identical to the committed `keyvalue.flightdata` golden that
/// `FlightDataGoldenDecodeTest` decodes on the Java side — closing the loop
/// between "what the golden contains" and "what the wire actually carries" so
/// a Java-side PASS against the golden is genuinely evidence about the real
/// transport, not just about a possibly-stale fixture.
#[test]
fn do_get_over_transport_matches_committed_golden() {
    let (_temp, data_dir) = build_fixture();
    let svc = CqliteFlightService::new(data_dir, 8192);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let wire = rt.block_on(do_get_raw_flight_data_over_transport(svc, ticket_bytes()));

    // `cqlite-flight` and `trino-connector` are sibling crates/dirs at the repo
    // root; the golden is committed there, not under `cqlite-flight`.
    let golden_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../trino-connector/src/test/resources/golden/keyvalue.flightdata");
    let golden_bytes = std::fs::read(&golden_path)
        .unwrap_or_else(|e| panic!("read committed golden {}: {e}", golden_path.display()));

    // Length-delimited protobuf: decode each `FlightData` message in turn,
    // consuming the shared cursor (`&mut buf`) as prost advances it.
    let mut buf: &[u8] = golden_bytes.as_slice();
    let mut golden = Vec::new();
    while !buf.is_empty() {
        golden.push(
            FlightData::decode_length_delimited(&mut buf)
                .expect("decode a FlightData message from the committed golden"),
        );
    }

    assert_eq!(
        wire.len(),
        golden.len(),
        "live-transport message count must match the committed golden's message count"
    );
    for (i, (w, g)) in wire.iter().zip(golden.iter()).enumerate() {
        assert_eq!(
            w.data_header, g.data_header,
            "message[{i}] data_header drifted from the committed golden"
        );
        assert_eq!(
            w.data_body, g.data_body,
            "message[{i}] data_body drifted from the committed golden"
        );
    }
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

/// Flush `total` rows across two separate memtable flushes so the resulting
/// `data_dir` holds TWO `nb-*-big-Data.db` SSTables the server must k-way-merge.
/// A single-SSTable fixture would let a broken merge pass the LIMIT test, so the
/// >N rows MUST span ≥2 SSTables to catch a #2157-class early-stop break.
///
/// Key assignment is **interleaved** across the two flushes — EVEN-numbered keys
/// (`k000000`, `k000002`, …) go to flush 1 and ODD-numbered keys (`k000001`,
/// `k000003`, …) to flush 2. This is what makes the LIMIT test meaningful: after
/// the merge sorts keys ascending, the first `N` rows for any `N >= 2`
/// necessarily draw from BOTH generations, so an implementation that reads only
/// one SSTable (or drops the second generation) can no longer return N rows and
/// pass. [`key_index`]/the LIMIT test assert that span directly.
fn build_multi_sstable_fixture(total: usize) -> (tempfile::TempDir, std::path::PathBuf) {
    assert!(
        total >= 4,
        "need enough rows to span two flushes above a cap"
    );
    let schema = simple_schema();
    let temp = tempfile::TempDir::new().unwrap();
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");
    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, schema);
    let mut engine = WriteEngine::new(config).expect("engine");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    // Distinct keys with a fixed 6-digit width so string order == numeric order.
    // Flush 1 = even indices, flush 2 = odd indices, so the sorted first-N result
    // interleaves rows from both SSTables (see the doc comment above).
    for i in (0..total).step_by(2) {
        engine
            .write(write_row(&format!("k{i:06}"), &format!("v{i}"), 100))
            .expect("write");
    }
    rt.block_on(engine.flush())
        .expect("flush 1")
        .expect("info 1");
    for i in (1..total).step_by(2) {
        engine
            .write(write_row(&format!("k{i:06}"), &format!("v{i}"), 100))
            .expect("write");
    }
    rt.block_on(engine.flush())
        .expect("flush 2")
        .expect("info 2");

    // Prove the fixture really produced ≥2 SSTables — otherwise the LIMIT test
    // is not exercising the multi-SSTable early-stop it claims to.
    let table_dir = data_dir.join(KS).join(TBL);
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
        "fixture must span ≥2 SSTables (found {data_files}) so LIMIT exercises the k-way merge",
    );
    (temp, data_dir)
}

/// Collect the string values of the named `column` across all decoded batches.
/// Used by the predicate/projection and LIMIT-span assertions.
fn keys_of(batches: &[RecordBatch], column: &str) -> Vec<String> {
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

/// Parse the numeric index `N` out of a `kNNNNNN` fixture key. Even indices were
/// flushed to SSTable 1, odd indices to SSTable 2 (see [`build_multi_sstable_fixture`]).
fn key_index(key: &str) -> usize {
    key.strip_prefix('k')
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| panic!("fixture key {key:?} must be of the form kNNNNNN"))
}

/// **LIMIT enforcement over the wire, PROVABLY across ≥2 SSTables.** A ticket with
/// `limit: Some(N)` against a fixture holding >N rows must decode to EXACTLY N rows
/// through the real gRPC transport. Because the fixture interleaves keys across two
/// flushes (even → SSTable 1, odd → SSTable 2), the sorted first-N result MUST draw
/// from BOTH generations — so we also assert the returned key set contains at least
/// one flush-1 key AND at least one flush-2 key. This catches a #2157-class early-stop
/// break where the k-way merge reads only one generation (which would still return N
/// contiguous rows and pass a naive count-only assertion).
#[test]
fn do_get_over_transport_enforces_limit() {
    let total = 20usize;
    let limit = 7u64;
    let (_temp, data_dir) = build_multi_sstable_fixture(total);
    let ticket = serde_json::to_vec(&serde_json::json!({
        "keyspace": KS,
        "table": TBL,
        "ddl": DDL,
        "limit": limit,
    }))
    .unwrap();

    let svc = CqliteFlightService::new(data_dir, 8192);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let batches = rt.block_on(do_get_batches_over_transport(svc, ticket));

    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        rows, limit as usize,
        "a LIMIT {limit} ticket over {total} rows across 2 SSTables must decode EXACTLY {limit} rows"
    );

    // The capped result must genuinely span both generations: at least one key
    // from flush 1 (even index) AND at least one from flush 2 (odd index).
    let keys = keys_of(&batches, "key");
    assert_eq!(keys.len(), limit as usize, "one key per returned row");
    let from_flush1 = keys.iter().any(|k| key_index(k) % 2 == 0);
    let from_flush2 = keys.iter().any(|k| key_index(k) % 2 == 1);
    assert!(
        from_flush1 && from_flush2,
        "capped LIMIT {limit} result must span BOTH SSTables (flush1={from_flush1}, \
         flush2={from_flush2}); keys={keys:?}"
    );
}

/// **Predicate + projection over the wire.** A ticket carrying a v2 filter tree
/// (`value = "v3"`) plus a single-column projection (`["value"]`) must decode to
/// exactly the matching row, with ONLY the projected column present. This closes
/// the gap where the transport tests previously asserted only `count > 0`.
#[test]
fn do_get_over_transport_applies_predicate_and_projection() {
    let (_temp, data_dir) = build_multi_sstable_fixture(20);
    // Filter on the `value` column for the row written as v3 (key k000003), and
    // project only `value` so the row-key column must be absent from the output.
    let ticket = serde_json::to_vec(&serde_json::json!({
        "keyspace": KS,
        "table": TBL,
        "ddl": DDL,
        "columns": ["value"],
        "filter": {"type": "Compare", "column": "value", "op": "Equal", "value": "v3"},
    }))
    .unwrap();

    let svc = CqliteFlightService::new(data_dir, 8192);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let batches = rt.block_on(do_get_batches_over_transport(svc, ticket));

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 1,
        "predicate value = 'v3' must select exactly one row over the wire"
    );
    let values = keys_of(&batches, "value");
    assert_eq!(values, vec!["v3".to_string()], "the surviving row is v3");
    // Projection: only `value` must be present, the `key` column projected out.
    let schema = batches.first().expect("one batch").schema();
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        field_names,
        vec!["value"],
        "projection [\"value\"] must emit ONLY the value column, not key"
    );
}

/// **live-mode file-set resolution.** A null-snapshot ticket against a table dir
/// that contains BOTH a `snapshots/<name>/` subdir AND live `Data.db` files must
/// read the LIVE set, never the snapshot. We build the live set with two rows and
/// a snapshot with three DIFFERENT rows, so the decoded row count disambiguates
/// which set the server actually read. This closes the read-mode server-behavior
/// gap that was only field-level-tested.
#[test]
fn do_get_over_transport_reads_live_set_not_snapshot() {
    // Live set: 2 rows in one SSTable.
    let (_temp_live, live_dir) = build_fixture_n(2);
    let live_table = live_dir.join(KS).join(TBL);

    // Snapshot set: 5 rows (a distinct count) copied into snapshots/pinned/.
    let (_temp_snap, snap_dir) = build_fixture_n(5);
    let snap_table = snap_dir.join(KS).join(TBL);
    let snapshot_dst = live_table.join("snapshots").join("pinned");
    std::fs::create_dir_all(&snapshot_dst).expect("mk snapshot dir");
    copy_sstable_components(&snap_table, &snapshot_dst);

    // A null-snapshot ticket (live mode).
    let ticket = serde_json::to_vec(&serde_json::json!({
        "keyspace": KS,
        "table": TBL,
        "ddl": DDL,
    }))
    .unwrap();

    let svc = CqliteFlightService::new(live_dir, 8192);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let rows = rt.block_on(do_get_rows_over_transport(svc, ticket));
    assert_eq!(
        rows, 2,
        "a null-snapshot (live) ticket must read the 2-row LIVE set, not the 5-row snapshot"
    );
}

/// Flush `n` distinct rows into a fresh single-SSTable fixture and return its
/// data dir. Like [`build_fixture`] but with a caller-chosen row count so two
/// fixtures can carry disambiguating counts.
fn build_fixture_n(n: usize) -> (tempfile::TempDir, std::path::PathBuf) {
    let schema = simple_schema();
    let temp = tempfile::TempDir::new().unwrap();
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");
    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, schema);
    let mut engine = WriteEngine::new(config).expect("engine");
    for i in 0..n {
        engine
            .write(write_row(&format!("k{i:06}"), &format!("v{i}"), 100))
            .expect("write");
    }
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(engine.flush()).expect("flush").expect("info");
    (temp, data_dir)
}

/// Copy every SSTable component file directly under `src` into `dst` (flat),
/// mirroring how a Cassandra snapshot hardlinks the component set into
/// `snapshots/<name>/`. Copies real bytes — never synthesizes SSTable content.
fn copy_sstable_components(src: &Path, dst: &Path) {
    for entry in std::fs::read_dir(src)
        .expect("read source table dir")
        .flatten()
    {
        let path = entry.path();
        if path.is_file() {
            let name = entry.file_name();
            std::fs::copy(&path, dst.join(&name)).expect("copy component");
        }
    }
}
