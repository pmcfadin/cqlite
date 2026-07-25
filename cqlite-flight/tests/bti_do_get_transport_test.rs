//! Issue #2372: end-to-end coverage of BTI (`da`) SSTables through the Flight
//! `do_get` path.
//!
//! Every other Flight integration test hardcodes a BIG (`nb-*-big`) fixture, so
//! no `da` SSTable ever reached `do_get` before this file. These tests drive a
//! REAL `do_get` over a REAL loopback tonic server + `FlightServiceClient`
//! against the committed `test_da` BTI corpus, decoding the response with the
//! REAL `FlightRecordBatchStream` — the same wire path a Trino client uses.
//!
//! Oracle: each table's committed `da-2-bti-Data.db.jsonl` sstabledump golden.
//! Assertions are SET-based (match each returned row to its golden counterpart
//! by key), so they are independent of merge/emit ordering.
//!
//! Skip-on-presence: gated on the `da-2-bti-Data.db` binary exactly like the BIG
//! tests gate on `nb-1-big-Data.db` — but because the binaries are force-committed
//! (`git add -f`, D1 of the change), the gate is satisfied in a stock CI checkout
//! and the tests EXECUTE. `rows > 0` (a present-but-empty result is a HARD
//! FAILURE, never a skip) guards against a silent 0-row false pass.
//!
//! Latent-risk pin (issue #2363 audit / spec Req 2): Flight token pruning derives
//! a `-Summary.db` sibling (BIG-only) and fail-opens when it is absent — a BTI
//! table is never token-pruned. `bti_do_get_full_ring_token_prune_returns_all_rows`
//! drives a full-ring token ticket (so `spec.token` is `Some` and the prune loop
//! runs its `None => true` fail-open branch) and asserts EVERY golden row is
//! still returned — pinning "fail-open must never become fail-closed" without
//! asserting that any pruning happened.

use std::collections::HashMap;
use std::time::Duration;

use arrow::array::{
    Array, BooleanArray, FixedSizeBinaryArray, Int32Array, Int64Array, StringArray,
};
use arrow::record_batch::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::Ticket;
use futures::StreamExt;
use tonic::transport::server::TcpIncoming;
use tonic::transport::{Channel, Server};

use cqlite_flight::service::CqliteFlightService;

mod fixture_support;

const SIMPLE_TABLE: &str = "simple_table-de1be8b064e711f19ad401a8c8227b11";
const WIDE_TABLE: &str = "wide_table-9099a7c06c1811f19864870fb8444786";
const BTI_TAG: &str = "da-2-bti";

/// DDL for `test_da.simple_table` — a `uuid` PK with all-scalar regular columns,
/// matching the on-disk cell names (`name`, `age`, `salary`, `active`, `created`)
/// so `entry_to_row` reassembles every cell.
const SIMPLE_DDL: &str = "CREATE TABLE test_da.simple_table (\
    id uuid PRIMARY KEY, name text, age int, salary bigint, active boolean, \
    created timestamp)";

/// DDL for `test_da.wide_table` — an `int` partition key + `int` clustering key,
/// one `text` `payload` regular column (the sole on-disk cell). 3 partitions ×
/// 300 clustering rows = 900 rows, so a LIMIT-k with k < 900 genuinely bounds.
const WIDE_DDL: &str = "CREATE TABLE test_da.wide_table (\
    pk int, ck int, payload text, PRIMARY KEY (pk, ck))";

/// Stand up a real loopback tonic server serving `svc`, connect a real
/// `FlightServiceClient` (retrying until it accepts), run `do_get(ticket)`, and
/// return every decoded `RecordBatch` through the real `FlightRecordBatchStream`.
// arrow-flight's `FlightError` Err type has a framework-fixed large size; boxing
// it (clippy's suggestion) would break the flight decoder stream API (#2856).
#[allow(clippy::result_large_err)]
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
        batches.push(batch.expect("decode BTI flight batch over transport"));
    }
    server.abort();
    batches
}

/// Run `do_get(ticket)` against a fresh service pointed at the corpus `sstables/`
/// root and return the decoded batches.
fn run_do_get(sstables_root: std::path::PathBuf, ticket: serde_json::Value) -> Vec<RecordBatch> {
    let bytes = serde_json::to_vec(&ticket).unwrap();
    let svc = CqliteFlightService::new(sstables_root, 8192);
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(do_get_batches_over_transport(svc, bytes))
}

/// Raw 16 bytes of a hyphenated UUID string.
fn uuid_bytes(s: &str) -> [u8; 16] {
    let hex: String = s.chars().filter(|c| *c != '-').collect();
    assert_eq!(hex.len(), 32, "not a 16-byte UUID: {s:?}");
    let mut out = [0u8; 16];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).expect("hex");
    }
    out
}

// ---- simple_table golden (uuid PK, scalar columns) ----

/// One `simple_table` row as decoded/expected: (name, age, salary, active).
#[derive(Debug, PartialEq, Clone)]
struct SimpleRow {
    name: String,
    age: i32,
    salary: i64,
    active: bool,
}

/// The `simple_table` golden, keyed by UUID (raw 16 bytes). Hand-transcribed
/// from the committed `da-2-bti-Data.db.jsonl` (3 partitions).
fn simple_golden() -> HashMap<[u8; 16], SimpleRow> {
    HashMap::from([
        (
            uuid_bytes("22222222-2222-2222-2222-222222222222"),
            SimpleRow {
                name: "Bob Johnson".into(),
                age: 45,
                salary: 95000,
                active: false,
            },
        ),
        (
            uuid_bytes("11111111-1111-1111-1111-111111111111"),
            SimpleRow {
                name: "Alice Smith".into(),
                age: 30,
                salary: 75000,
                active: true,
            },
        ),
        (
            uuid_bytes("33333333-3333-3333-3333-333333333333"),
            SimpleRow {
                name: "Carol Williams".into(),
                age: 28,
                salary: 65000,
                active: true,
            },
        ),
    ])
}

/// Extract every decoded `simple_table` row across `batches` as
/// (uuid_bytes -> SimpleRow), by column name (order-independent).
fn simple_rows(batches: &[RecordBatch]) -> HashMap<[u8; 16], SimpleRow> {
    let mut out = HashMap::new();
    for b in batches {
        let ids = col_fsb(b, "id");
        let names = col_str(b, "name");
        let ages = col_i32(b, "age");
        let salaries = col_i64(b, "salary");
        let actives = col_bool(b, "active");
        for r in 0..b.num_rows() {
            let mut key = [0u8; 16];
            key.copy_from_slice(ids.value(r));
            out.insert(
                key,
                SimpleRow {
                    name: names.value(r).to_string(),
                    age: ages.value(r),
                    salary: salaries.value(r),
                    active: actives.value(r),
                },
            );
        }
    }
    out
}

fn col_fsb<'a>(b: &'a RecordBatch, name: &str) -> &'a FixedSizeBinaryArray {
    let idx = b.schema().index_of(name).expect("column present");
    b.column(idx)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap_or_else(|| panic!("{name} is not FixedSizeBinary"))
}
fn col_str<'a>(b: &'a RecordBatch, name: &str) -> &'a StringArray {
    let idx = b.schema().index_of(name).expect("column present");
    b.column(idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap_or_else(|| panic!("{name} is not Utf8"))
}
fn col_i32<'a>(b: &'a RecordBatch, name: &str) -> &'a Int32Array {
    let idx = b.schema().index_of(name).expect("column present");
    b.column(idx)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap_or_else(|| panic!("{name} is not Int32"))
}
fn col_i64<'a>(b: &'a RecordBatch, name: &str) -> &'a Int64Array {
    let idx = b.schema().index_of(name).expect("column present");
    b.column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap_or_else(|| panic!("{name} is not Int64"))
}
fn col_bool<'a>(b: &'a RecordBatch, name: &str) -> &'a BooleanArray {
    let idx = b.schema().index_of(name).expect("column present");
    b.column(idx)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap_or_else(|| panic!("{name} is not Boolean"))
}

// ---- wide_table golden (int PK/CK, text payload) — parsed from the JSONL ----

/// The `wide_table` golden as (pk, ck) -> payload, parsed from the committed
/// `da-2-bti-Data.db.jsonl` (3 partitions × 300 clustering rows = 900).
fn wide_golden(table_dir: &std::path::Path) -> HashMap<(i32, i32), String> {
    let jsonl = table_dir.join(format!("{BTI_TAG}-Data.db.jsonl"));
    let text = std::fs::read_to_string(&jsonl)
        .unwrap_or_else(|e| panic!("read wide golden {}: {e}", jsonl.display()));
    let mut out = HashMap::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let obj: serde_json::Value = serde_json::from_str(line).expect("golden JSONL line");
        let pk: i32 = obj["partition"]["key"][0]
            .as_str()
            .expect("pk string")
            .parse()
            .expect("pk int");
        for row in obj["rows"].as_array().into_iter().flatten() {
            if row["type"].as_str() != Some("row") {
                continue;
            }
            let ck = row["clustering"][0].as_i64().expect("ck int") as i32;
            let mut payload = None;
            for cell in row["cells"].as_array().into_iter().flatten() {
                if cell["name"].as_str() == Some("payload") {
                    payload = cell["value"].as_str().map(str::to_string);
                }
            }
            out.insert((pk, ck), payload.expect("payload cell"));
        }
    }
    out
}

/// Extract every decoded `wide_table` row as (pk, ck) -> payload.
fn wide_rows(batches: &[RecordBatch]) -> HashMap<(i32, i32), String> {
    let mut out = HashMap::new();
    for b in batches {
        let pks = col_i32(b, "pk");
        let cks = col_i32(b, "ck");
        let payloads = col_str(b, "payload");
        for r in 0..b.num_rows() {
            out.insert((pks.value(r), cks.value(r)), payloads.value(r).to_string());
        }
    }
    out
}

// ============================ tests ============================

/// **Full scan over a BTI table (spec Req 1, scenario 1).** A `do_get` full scan
/// of `test_da.simple_table` over the real transport must return exactly the
/// rows in the `da-2-bti-Data.db.jsonl` golden (all 3, values matched by key),
/// with `rows > 0`.
#[test]
fn bti_do_get_full_scan_matches_golden() {
    let Some(_dir) = fixture_support::table_dir_if_present("test_da", SIMPLE_TABLE, BTI_TAG) else {
        eprintln!("test_da/simple_table {BTI_TAG}-Data.db absent — skipping (BTI corpus)");
        return;
    };
    let root = fixture_support::sstables_root().expect("root present when Data.db is");
    let batches = run_do_get(
        root,
        serde_json::json!({"keyspace": "test_da", "table": "simple_table", "ddl": SIMPLE_DDL}),
    );

    let rows = simple_rows(&batches);
    assert!(
        !rows.is_empty(),
        "BTI full scan must decode > 0 rows (never a 0-row false pass)"
    );
    assert_eq!(
        rows,
        simple_golden(),
        "BTI full-scan rows must equal the da-2-bti sstabledump golden (all keys + values)"
    );
}

/// **Point lookup over a BTI table (spec Req 1, scenario 2).** A `do_get` with a
/// full-PK equality (`id = 11111111-…`) must return exactly that partition's
/// golden row.
#[test]
fn bti_do_get_point_lookup_returns_addressed_partition() {
    let Some(_dir) = fixture_support::table_dir_if_present("test_da", SIMPLE_TABLE, BTI_TAG) else {
        eprintln!("test_da/simple_table {BTI_TAG}-Data.db absent — skipping (BTI corpus)");
        return;
    };
    let root = fixture_support::sstables_root().expect("root present when Data.db is");
    let target = "11111111-1111-1111-1111-111111111111";
    let batches = run_do_get(
        root,
        serde_json::json!({
            "keyspace": "test_da",
            "table": "simple_table",
            "ddl": SIMPLE_DDL,
            "filter": {"type": "Compare", "column": "id", "op": "Equal", "value": target},
        }),
    );

    let rows = simple_rows(&batches);
    assert_eq!(
        rows.len(),
        1,
        "point read id = {target} must decode to exactly its one partition, got {}",
        rows.len()
    );
    let key = uuid_bytes(target);
    assert_eq!(
        rows.get(&key),
        simple_golden().get(&key),
        "the addressed BTI partition must match its golden row"
    );
}

/// **LIMIT-k over a BTI table (spec Req 1, scenario 3).** A `do_get` with
/// `limit = k` over `wide_table` (900 golden rows) must decode at most `k` rows,
/// each matching its golden counterpart.
#[test]
fn bti_do_get_limit_bounds_result_and_matches_golden() {
    let Some(dir) = fixture_support::table_dir_if_present("test_da", WIDE_TABLE, BTI_TAG) else {
        eprintln!("test_da/wide_table {BTI_TAG}-Data.db absent — skipping (BTI corpus)");
        return;
    };
    let root = fixture_support::sstables_root().expect("root present when Data.db is");
    let k = 25u64;
    let golden = wide_golden(&dir);
    assert!(
        golden.len() as u64 > k,
        "wide_table golden ({}) must exceed the limit {k} for the bound to be meaningful",
        golden.len()
    );

    let batches = run_do_get(
        root,
        serde_json::json!({
            "keyspace": "test_da",
            "table": "wide_table",
            "ddl": WIDE_DDL,
            "limit": k,
        }),
    );

    let rows = wide_rows(&batches);
    assert!(!rows.is_empty(), "LIMIT-k BTI scan must decode > 0 rows");
    assert!(
        rows.len() as u64 <= k,
        "LIMIT {k} must bound the BTI result to <= {k} rows, got {}",
        rows.len()
    );
    for ((pk, ck), payload) in &rows {
        assert_eq!(
            golden.get(&(*pk, *ck)),
            Some(payload),
            "returned BTI row (pk={pk}, ck={ck}) must match its golden payload"
        );
    }
}

/// **Fail-open pruning does not drop BTI rows (spec Req 2).** A full-ring
/// token-range ticket makes `ScanSpec.token` `Some`, so the producer's
/// `prune_paths` loop runs — and because a BTI table has no `-Summary.db`, its
/// per-SSTable span resolves to `None`, exercising the `None => true` fail-open
/// branch. EVERY golden row must still be returned. The test deliberately does
/// NOT assert that any partition was token-pruned (BTI pruning is out of scope).
#[test]
fn bti_do_get_full_ring_token_prune_returns_all_rows() {
    let Some(dir) = fixture_support::table_dir_if_present("test_da", SIMPLE_TABLE, BTI_TAG) else {
        eprintln!("test_da/simple_table {BTI_TAG}-Data.db absent — skipping (BTI corpus)");
        return;
    };
    // Guard the premise: a BTI table has NO Summary.db sibling — that absence is
    // exactly what routes the prune through the fail-open branch under test.
    assert!(
        !dir.join(format!("{BTI_TAG}-Summary.db")).exists(),
        "a BTI (da) table must have no -Summary.db — that absence is what this test pins"
    );
    let root = fixture_support::sstables_root().expect("root present when Data.db is");
    let batches = run_do_get(
        root,
        serde_json::json!({
            "keyspace": "test_da",
            "table": "simple_table",
            "ddl": SIMPLE_DDL,
            // Full ring → spec.token is Some, so prune_paths runs its per-SSTable
            // Summary.db read + None => true fail-open branch for the BTI table.
            "token_start": i64::MIN,
            "token_end": i64::MAX,
        }),
    );

    let rows = simple_rows(&batches);
    assert_eq!(
        rows,
        simple_golden(),
        "fail-open token pruning must never drop BTI rows — every golden row must be returned"
    );
}
