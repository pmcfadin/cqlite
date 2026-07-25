//! Issue #2373: end-to-end coverage of COMPRESSED (chunk-stitching) BIG-`nb`
//! SSTables through the Flight `do_get` path, over REAL gRPC transport.
//!
//! Every pre-existing Flight transport test served either a WriteEngine-produced
//! fixture (uncompressed by construction — the write surface never emits a
//! `CompressionInfo.db`, issue #1406) or the BTI corpus, so the chunk-stitching
//! read path — the one the FIELD runs on, since every Cassandra table is LZ4 by
//! default — never reached `do_get` in-repo. These tests close that gap: a bound
//! socket, a tonic server, a real `FlightServiceClient`, and the real
//! `FlightRecordBatchStream` decode, against the real `test_comp` corpus.
//!
//! Oracle: each table's committed `nb-1-big-Data.db.jsonl` sstabledump golden —
//! row count AND `(pk, ck, <payload col>)` values. A present-but-empty result
//! (zero rows) is a HARD FAILURE, never a pass and never a skip.
//!
//! ## Routing evidence (spec Req 2)
//!
//! `SSTableReader::requires_chunk_stitching()` is `pub(super)`, so an integration
//! test in this crate cannot call it. Instead each compressed case brackets its
//! scan with the PUBLIC process-global counters
//! `SSTableReader::reset_decompress_calls()` / `decompress_call_count()` (bumped
//! once per real chunk decompression in `reader/chunk_source.rs`) and asserts a
//! non-zero count — positive evidence the decompress plane executed, rather than
//! inferring it from a green result.
//!
//! Those counters are PROCESS-GLOBAL, so every test here that reads them is
//! `#[serial]`: a concurrent scan in the same test binary would otherwise inflate
//! the count. What makes the non-zero counts *evidence* rather than noise is the
//! uncompressed control (`uncompressed_table`, which has no `CompressionInfo.db`):
//! it runs the same scan shape in the same serial group and must leave the counter
//! at exactly ZERO, so a compressed table's non-zero count cannot be explained by
//! unrelated decompression elsewhere in the process.
//!
//! ## Fixture pin
//!
//! The `test_comp` binaries and their `.jsonl` goldens are force-committed
//! (`git add -f`) and pinned to datasets-v3 **v3.5**. A future corpus
//! regeneration (#2222) MUST refresh the bytes and the goldens TOGETHER — the
//! assertions below compare one against the other. Fixture lookup is by table
//! NAME PREFIX (never a hardcoded generation UUID) so a regen that mints new
//! UUIDs does not silently turn these tests into skips.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use arrow::array::{Array, BinaryArray, Int32Array, StringArray};
use arrow::record_batch::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::Ticket;
use futures::StreamExt;
use serial_test::serial;
use tonic::transport::server::TcpIncoming;
use tonic::transport::{Channel, Server};

use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_flight::service::CqliteFlightService;

mod fixture_support;

/// SSTable generation tag shared by every `test_comp` table.
const BIG_TAG: &str = "nb-1-big";
const KEYSPACE: &str = "test_comp";

/// The six codec/edge tables all share one DDL shape except the incompressible
/// one, whose payload column is a `BLOB` (high-entropy bytes are what force
/// Cassandra's raw-chunk fallback).
fn text_ddl(table: &str) -> String {
    format!("CREATE TABLE {KEYSPACE}.{table} (pk int, ck int, body text, PRIMARY KEY (pk, ck))")
}

const BLOB_DDL: &str = "CREATE TABLE test_comp.incompressible_uncompressed_chunk \
     (pk int, ck int, payload blob, PRIMARY KEY (pk, ck))";

// ======================== fixture gating ========================

/// Whether an absent fixture must HARD-FAIL rather than skip.
///
/// Matches the repo-wide predicate (`cqlite-core/tests/decompressed_chunk_cache_tests.rs`):
/// only `1`/`true`/`TRUE` mean require-fixtures, so a lane that explicitly sets
/// `CQLITE_REQUIRE_FIXTURES=0` stays lenient instead of hard-failing.
///
/// BOTH fail-closed flags are honored, matching the repo's canonical pair
/// (`tools/cassandra-parity/src/workflow_check/command.rs`: `["CQLITE_REQUIRE_FIXTURES",
/// "CQLITE_PARITY_REQUIRE_DATASETS"]`). The lanes that actually arm fail-closed today
/// (`ci.yml`, `scripts/local/pre-merge.sh`, `sstabledump-parity-gate.yml`) set the
/// PARITY var, so honoring only the first would leave those lanes lenient (roborev 1977).
/// This is belt-and-suspenders: the load-bearing guarantee is the force-committed
/// `test_comp` corpus plus the per-lookup in-repo fallback.
fn require_fixtures() -> bool {
    ["CQLITE_REQUIRE_FIXTURES", "CQLITE_PARITY_REQUIRE_DATASETS"]
        .iter()
        .any(|var| {
            matches!(
                std::env::var(var).ok().as_deref(),
                Some("1") | Some("true") | Some("TRUE")
            )
        })
}

/// Resolve a `test_comp` table, or report why the case must be skipped.
///
/// The lookup walks every candidate `sstables/` root (env then in-repo) and
/// returns the root the fixture was ACTUALLY found under, so the service and the
/// golden always read the same corpus. Under `CQLITE_REQUIRE_FIXTURES=1` an
/// absent fixture is a HARD FAILURE naming the missing table instead of a skip,
/// so a lane that is supposed to have the corpus can never go green vacuously.
fn fixture_or_skip(table: &str) -> Option<fixture_support::ResolvedFixture> {
    match fixture_support::table_dir_by_prefix(KEYSPACE, table, BIG_TAG) {
        Some(found) => Some(found),
        None => {
            let roots: Vec<String> = fixture_support::candidate_sstables_roots()
                .iter()
                .map(|r| r.display().to_string())
                .collect();
            let msg = format!(
                "{KEYSPACE}.{table}: no <{table}-*>/{BIG_TAG}-Data.db under any of [{}] \
                 (compressed corpus absent)",
                roots.join(", ")
            );
            assert!(!require_fixtures(), "CQLITE_REQUIRE_FIXTURES=1: {msg}");
            eprintln!("SKIP: {msg}");
            None
        }
    }
}

/// Assert the fixture really is compressed before drawing any conclusion from a
/// non-zero decompress count. Structural precondition only — it proves the table
/// HAS chunked compression, not that the reader took the stitch arm (that is the
/// counter's job).
fn assert_has_compression_info(dir: &Path, table: &str) {
    let info = dir.join(format!("{BIG_TAG}-CompressionInfo.db"));
    assert!(
        info.is_file(),
        "{table} must ship a {BIG_TAG}-CompressionInfo.db for the compressed-path \
         assertions to mean anything (looked at {})",
        info.display()
    );
}

// ======================== transport harness ========================

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

    let mut client = connect_client(addr).await;
    let resp = client
        .do_get(Ticket::new(ticket))
        .await
        .expect("do_get rpc");
    let stream = resp.into_inner().map(|r| r.map_err(FlightError::Tonic));
    let mut rb = FlightRecordBatchStream::new_from_flight_data(stream);

    let mut batches = Vec::new();
    while let Some(batch) = rb.next().await {
        batches.push(batch.expect("decode compressed flight batch over transport"));
    }
    server.abort();
    batches
}

/// Connect a `FlightServiceClient` to `addr`, retrying while the server binds.
async fn connect_client(addr: std::net::SocketAddr) -> FlightServiceClient<Channel> {
    let endpoint = Channel::from_shared(format!("http://{addr}"))
        .expect("endpoint")
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
    FlightServiceClient::new(channel)
}

/// Run `do_get(ticket)` against a fresh service pointed at the corpus `sstables/`
/// root and return the decoded batches.
fn run_do_get(root: PathBuf, ticket: serde_json::Value) -> Vec<RecordBatch> {
    let bytes = serde_json::to_vec(&ticket).expect("ticket json");
    let svc = CqliteFlightService::new(root, 8192);
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    rt.block_on(do_get_batches_over_transport(svc, bytes))
}

// ======================== goldens + decode ========================

fn col_i32<'a>(b: &'a RecordBatch, name: &str) -> &'a Int32Array {
    let idx = b.schema().index_of(name).expect("column present");
    b.column(idx)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap_or_else(|| panic!("{name} is not Int32"))
}

/// Every `(pk, ck)` -> text-cell value in a table's committed sstabledump golden.
fn text_golden(dir: &Path, cell: &str) -> HashMap<(i32, i32), String> {
    golden_cells(dir, cell, |v| {
        v.as_str()
            .map(str::to_string)
            .unwrap_or_else(|| panic!("golden cell {cell} is not a string"))
    })
}

/// Every `(pk, ck)` -> blob-cell value (`0x…` hex in the golden) as raw bytes.
fn blob_golden(dir: &Path, cell: &str) -> HashMap<(i32, i32), Vec<u8>> {
    golden_cells(dir, cell, |v| {
        let s = v
            .as_str()
            .unwrap_or_else(|| panic!("golden cell {cell} is not a string"));
        let hex = s.strip_prefix("0x").unwrap_or(s);
        assert!(hex.len() % 2 == 0, "odd-length hex blob in golden: {s}");
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("hex byte"))
            .collect()
    })
}

/// Shared golden walk: every `type == "row"` entry across every JSONL line,
/// keyed by `(pk, ck)`, with the named cell mapped through `decode`.
fn golden_cells<T>(
    dir: &Path,
    cell: &str,
    decode: impl Fn(&serde_json::Value) -> T,
) -> HashMap<(i32, i32), T> {
    let jsonl = dir.join(format!("{BIG_TAG}-Data.db.jsonl"));
    let text = std::fs::read_to_string(&jsonl)
        .unwrap_or_else(|e| panic!("read golden {}: {e}", jsonl.display()));
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
            let ck = i32::try_from(row["clustering"][0].as_i64().expect("ck int")).expect("ck i32");
            let value = row["cells"]
                .as_array()
                .into_iter()
                .flatten()
                .find(|c| c["name"].as_str() == Some(cell))
                .map(|c| decode(&c["value"]))
                .unwrap_or_else(|| panic!("golden row (pk={pk}, ck={ck}) has no {cell} cell"));
            out.insert((pk, ck), value);
        }
    }
    assert!(
        !out.is_empty(),
        "golden {} yielded no rows — the oracle itself is empty",
        jsonl.display()
    );
    out
}

/// Rows the server ACTUALLY emitted, summed across batches before any keying.
///
/// The `(pk, ck)`-keyed decode maps below COLLAPSE duplicates, so a
/// chunk-stitching regression that re-emits a chunk (stitch offset restart, an
/// overlapping window) would produce identical tuples that dedup away and leave
/// the keyed count matching the golden. Asserting this raw total catches the
/// duplication BEFORE the dedup hides it.
fn raw_row_total(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

/// Decode every returned row as `(pk, ck) -> text cell`.
fn text_rows(batches: &[RecordBatch], cell: &str) -> HashMap<(i32, i32), String> {
    let mut out = HashMap::new();
    for b in batches {
        let pks = col_i32(b, "pk");
        let cks = col_i32(b, "ck");
        let idx = b.schema().index_of(cell).expect("cell column present");
        let vals = b
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap_or_else(|| panic!("{cell} is not Utf8"));
        for r in 0..b.num_rows() {
            out.insert((pks.value(r), cks.value(r)), vals.value(r).to_string());
        }
    }
    out
}

/// Decode every returned row as `(pk, ck) -> blob cell`.
fn blob_rows(batches: &[RecordBatch], cell: &str) -> HashMap<(i32, i32), Vec<u8>> {
    let mut out = HashMap::new();
    for b in batches {
        let pks = col_i32(b, "pk");
        let cks = col_i32(b, "ck");
        let idx = b.schema().index_of(cell).expect("cell column present");
        let vals = b
            .column(idx)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap_or_else(|| panic!("{cell} is not Binary"));
        for r in 0..b.num_rows() {
            out.insert((pks.value(r), cks.value(r)), vals.value(r).to_vec());
        }
    }
    out
}

// ======================== shared case bodies ========================

/// Full scan of a COMPRESSED text table over transport, bracketed by the
/// decompress counter: rows must equal the golden exactly, and the stitching
/// plane must have run (`decompress_call_count() >= 1`).
fn assert_compressed_text_scan_matches_golden(table: &str) {
    let Some(found) = fixture_or_skip(table) else {
        return;
    };
    assert_has_compression_info(&found.dir, table);
    let golden = text_golden(&found.dir, "body");

    SSTableReader::reset_decompress_calls();
    let batches = run_do_get(
        found.sstables_root.clone(),
        serde_json::json!({"keyspace": KEYSPACE, "table": table, "ddl": text_ddl(table)}),
    );
    let decompressed = SSTableReader::decompress_call_count();

    let rows = text_rows(&batches, "body");
    assert!(
        !rows.is_empty(),
        "{table}: a present fixture returning ZERO rows is a failure, never a pass"
    );
    assert_eq!(
        raw_row_total(&batches),
        golden.len(),
        "{table}: the server must EMIT exactly the golden's {} rows, got {} before dedup — \
         a mismatch here that the keyed count below misses is a duplicate emission",
        golden.len(),
        raw_row_total(&batches)
    );
    assert_eq!(
        rows.len(),
        golden.len(),
        "{table}: full scan must return exactly the golden's {} rows, got {}",
        golden.len(),
        rows.len()
    );
    assert_eq!(
        rows, golden,
        "{table}: every (pk, ck, body) must equal the {BIG_TAG} sstabledump golden"
    );
    assert!(
        decompressed >= 1,
        "{table}: the chunk-stitching plane must have decompressed at least one chunk \
         (decompress_call_count() == 0 means the scan never took the compressed arm); \
         the uncompressed control pins this counter at 0 for a non-compressed table"
    );
}

// ======================== per-codec full scans (spec Req 1) ========================

/// **LZ4 full scan (spec Req 1, scenario 1).** The field default codec.
#[test]
#[serial]
fn compressed_do_get_full_scan_lz4_matches_golden() {
    assert_compressed_text_scan_matches_golden("lz4_table");
}

/// **Snappy full scan (spec Req 1, scenario 1).**
#[test]
#[serial]
fn compressed_do_get_full_scan_snappy_matches_golden() {
    assert_compressed_text_scan_matches_golden("snappy_table");
}

/// **Deflate full scan (spec Req 1, scenario 1).**
#[test]
#[serial]
fn compressed_do_get_full_scan_deflate_matches_golden() {
    assert_compressed_text_scan_matches_golden("deflate_table");
}

/// **Zstd full scan (spec Req 1, scenario 1).**
#[test]
#[serial]
fn compressed_do_get_full_scan_zstd_matches_golden() {
    assert_compressed_text_scan_matches_golden("zstd_table");
}

// ======================== chunk-boundary edge tables (spec Req 1, scenario 2) ========================

/// **Short final chunk (spec Req 1, scenario 2).** LZ4 at 4 KB chunks with a row
/// volume chosen so the last chunk is SHORTER than 4096 bytes. A stitcher that
/// assumed a full-length final chunk would truncate or over-read here, so exact
/// golden equality on all 777 rows is the assertion that matters.
#[test]
#[serial]
fn compressed_do_get_short_final_chunk_matches_golden() {
    assert_compressed_text_scan_matches_golden("short_final_chunk");
}

/// **Raw-chunk fallback (spec Req 1, scenario 2).** LZ4 with
/// `min_compress_ratio = 1.0`, so high-entropy chunks whose "compressed" form is
/// >= the chunk length are stored RAW by Cassandra. The stitcher must pass those
/// through unchanged while still decompressing the chunks that did shrink — hence
/// the counter is still `>= 1` even though some chunks bypass the decompressor
/// (raw passthrough deliberately does not bump it).
#[test]
#[serial]
fn compressed_do_get_incompressible_raw_chunks_match_golden() {
    let table = "incompressible_uncompressed_chunk";
    let Some(found) = fixture_or_skip(table) else {
        return;
    };
    assert_has_compression_info(&found.dir, table);
    let golden = blob_golden(&found.dir, "payload");

    SSTableReader::reset_decompress_calls();
    let batches = run_do_get(
        found.sstables_root.clone(),
        serde_json::json!({"keyspace": KEYSPACE, "table": table, "ddl": BLOB_DDL}),
    );
    let decompressed = SSTableReader::decompress_call_count();

    let rows = blob_rows(&batches, "payload");
    assert!(
        !rows.is_empty(),
        "{table}: a present fixture returning ZERO rows is a failure, never a pass"
    );
    assert_eq!(
        raw_row_total(&batches),
        golden.len(),
        "{table}: the server must EMIT exactly the golden's {} rows, got {} before dedup — \
         a duplicate raw-chunk passthrough would collapse away in the keyed count below",
        golden.len(),
        raw_row_total(&batches)
    );
    assert_eq!(
        rows.len(),
        golden.len(),
        "{table}: full scan must return exactly the golden's {} rows, got {}",
        golden.len(),
        rows.len()
    );
    assert_eq!(
        rows, golden,
        "{table}: every (pk, ck, payload) byte must equal the {BIG_TAG} golden — a \
         raw-fallback chunk must be stitched through untouched"
    );
    assert!(
        decompressed >= 1,
        "{table}: at least one chunk of this table still compresses, so the decompress \
         plane must have run at least once"
    );
}

// ======================== LIMIT-k (spec Req 1, scenario 3) ========================

/// **LIMIT-k over the compressed path (spec Req 1, scenario 3).** A ticket
/// carrying `limit = k` against a 600-row compressed table must return EXACTLY
/// `k` rows, each a member of the golden.
///
/// The golden is verified to hold more than `k` rows, so `== k` (not `<= k`) is
/// the assertion that has teeth: a merely-bounded check would also pass an
/// implementation that truncated to one row or stopped early on the compressed
/// path. Asserting it on the RAW batch total additionally catches a duplicate
/// emission the keyed map would collapse.
#[test]
#[serial]
fn compressed_do_get_limit_bounds_result_and_matches_golden() {
    let table = "lz4_table";
    let Some(found) = fixture_or_skip(table) else {
        return;
    };
    let k = 25u64;
    let golden = text_golden(&found.dir, "body");
    assert!(
        golden.len() as u64 > k,
        "{table} golden ({}) must exceed the limit {k} for the bound to be meaningful",
        golden.len()
    );

    SSTableReader::reset_decompress_calls();
    let batches = run_do_get(
        found.sstables_root.clone(),
        serde_json::json!({
            "keyspace": KEYSPACE,
            "table": table,
            "ddl": text_ddl(table),
            "limit": k,
        }),
    );
    let decompressed = SSTableReader::decompress_call_count();

    let rows = text_rows(&batches, "body");
    let k_usize = usize::try_from(k).expect("limit fits usize");
    assert_eq!(
        raw_row_total(&batches),
        k_usize,
        "{table}: LIMIT {k} over a {}-row golden must EMIT exactly {k} rows, got {} — \
         fewer means the compressed path stopped early, more means it overran the limit",
        golden.len(),
        raw_row_total(&batches)
    );
    assert_eq!(
        rows.len(),
        k_usize,
        "{table}: LIMIT {k} must yield exactly {k} DISTINCT (pk, ck) rows, got {}",
        rows.len()
    );
    for (key, body) in &rows {
        assert_eq!(
            golden.get(key),
            Some(body),
            "{table}: returned row (pk={}, ck={}) must match its golden body",
            key.0,
            key.1
        );
    }
    assert!(
        decompressed >= 1,
        "{table}: even a LIMIT-bounded scan must decompress at least one chunk"
    );
}

// ======================== uncompressed control (spec Req 2) ========================

/// **Uncompressed control (spec Req 2, scenario 2).** `uncompressed_table` has NO
/// `CompressionInfo.db`, so the same full-scan shape must leave the decompress
/// counter at EXACTLY zero. This is what makes the `>= 1` assertions above
/// evidence of routing rather than an artifact of unrelated decompression
/// elsewhere in the process — and the rows must still match its golden, so a
/// zero count can never be explained by the scan having returned nothing.
#[test]
#[serial]
fn uncompressed_control_do_get_leaves_decompress_counter_at_zero() {
    let table = "uncompressed_table";
    let Some(found) = fixture_or_skip(table) else {
        return;
    };
    let info = found.dir.join(format!("{BIG_TAG}-CompressionInfo.db"));
    assert!(
        !info.exists(),
        "the control table must have NO CompressionInfo.db (found {})",
        info.display()
    );
    let golden = text_golden(&found.dir, "body");

    SSTableReader::reset_decompress_calls();
    let batches = run_do_get(
        found.sstables_root.clone(),
        serde_json::json!({"keyspace": KEYSPACE, "table": table, "ddl": text_ddl(table)}),
    );
    let decompressed = SSTableReader::decompress_call_count();

    let rows = text_rows(&batches, "body");
    assert_eq!(
        raw_row_total(&batches),
        golden.len(),
        "{table}: the control must EMIT exactly the golden's {} rows, got {} before dedup",
        golden.len(),
        raw_row_total(&batches)
    );
    assert_eq!(
        rows, golden,
        "{table}: the control must still return its golden rows exactly"
    );
    assert_eq!(
        decompressed, 0,
        "{table} has no CompressionInfo.db, so an identical scan shape must perform ZERO \
         chunk decompressions (got {decompressed}); a non-zero value here would mean the \
         compressed cases' counts are not attributable to the compressed path"
    );
}

// ======================== midstream drop (spec Req 1, scenario 4) ========================

/// Open a `do_get` over real transport, read `read_batches` batches, then DROP
/// the decode stream, the client and the channel WITHOUT draining — leaving the
/// server to observe the disconnect.
///
/// Returns the still-running server task AND the number of batches actually
/// read, which the caller MUST assert equals `read_batches`: a ticket/DDL/schema
/// regression that makes `do_get` error on the first poll reads zero batches, so
/// the producer never parks and the in-flight level never leaves its baseline —
/// a green "no leak" verdict from a stream that exercised nothing.
// arrow-flight's `FlightError` Err type has a framework-fixed large size; boxing
// it (clippy's suggestion) would break the flight decoder stream API (#2856).
#[allow(clippy::result_large_err)]
async fn do_get_drop_after(
    svc: CqliteFlightService,
    ticket: Vec<u8>,
    read_batches: usize,
) -> (
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    usize,
) {
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

    let mut client = connect_client(addr).await;
    let resp = client
        .do_get(Ticket::new(ticket))
        .await
        .expect("do_get rpc");
    let stream = resp.into_inner().map(|r| r.map_err(FlightError::Tonic));
    let mut rb = FlightRecordBatchStream::new_from_flight_data(stream);

    let mut read = 0usize;
    while read < read_batches {
        match rb.next().await {
            Some(Ok(_batch)) => read += 1,
            // EOF or a Status error before `read_batches`. NOT silently
            // tolerated: the caller asserts `read == read_batches`, so this
            // becomes a clean failure rather than an accidental green.
            _ => break,
        }
    }
    drop(rb);
    drop(client);
    (server, read)
}

/// Poll the process-wide `do_get` in-flight level until it drops to `<= baseline`
/// or `timeout` elapses; returns the final observed level. The timeout is a
/// generous liveness bound, not a latency assertion.
async fn await_in_flight_settled(baseline: i64, timeout: Duration) -> i64 {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let level = cqlite_flight::obs::in_flight_level("do_get");
        if level <= baseline {
            return level;
        }
        if std::time::Instant::now() >= deadline {
            return level;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// **Midstream client drop on the stitching path (spec Req 1, scenario 4).** LZ4
/// (the field default) served with `batch_size = 1`, so the 600-row table produces
/// far more batches than the bounded `do_get` channel holds and the merge producer
/// PARKS in a blocking send once the client stops reading. The client reads two
/// batches and drops the stream; the server-side `do_get` in-flight accounting
/// must return to its pre-RPC baseline, proving a mid-scan disconnect on the
/// COMPRESSED path releases the parked producer instead of leaking it.
///
/// The 30s bound is a generous liveness ceiling for a leak that never settles at
/// all — it is not a latency threshold (a healthy release settles in milliseconds).
#[test]
#[serial]
fn compressed_do_get_client_drop_midstream_releases_producer() {
    let table = "lz4_table";
    let Some(found) = fixture_or_skip(table) else {
        return;
    };
    assert_has_compression_info(&found.dir, table);

    let ticket = serde_json::to_vec(&serde_json::json!({
        "keyspace": KEYSPACE,
        "table": table,
        "ddl": text_ddl(table),
    }))
    .expect("ticket json");

    const WANT_BATCHES: usize = 2;

    // batch_size = 1 → one row per batch, so the producer fills the bounded
    // channel and parks while the client is still reading.
    let svc = CqliteFlightService::new(found.sstables_root.clone(), 1);
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let (baseline, level, read) = rt.block_on(async move {
        let baseline = cqlite_flight::obs::in_flight_level("do_get");
        let (server, read) = do_get_drop_after(svc, ticket, WANT_BATCHES).await;
        let level = await_in_flight_settled(baseline, Duration::from_secs(30)).await;
        server.abort();
        (baseline, level, read)
    });
    // Shut the runtime down BEFORE asserting. On a genuine producer leak — the
    // bug under test — a panic inside `block_on` would drop the Runtime during
    // unwind, and `Runtime::drop` waits on started blocking tasks: the leak
    // would surface as a HANG instead of a clean failure.
    rt.shutdown_timeout(Duration::from_secs(5));

    assert_eq!(
        read, WANT_BATCHES,
        "the client must have read exactly {WANT_BATCHES} batches before dropping (got {read}); \
         a stream that ended early never parked the producer, so the in-flight assertion below \
         would prove nothing"
    );
    assert!(
        level <= baseline,
        "do_get in-flight level must return to its {baseline} baseline after the client \
         drops midstream on the COMPRESSED (chunk-stitching) path (got {level}); a higher \
         level means the merge producer is still parked in its blocking send"
    );
}
