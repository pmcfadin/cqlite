//! Issue #2370 — N-concurrent `do_get` stress/coverage against ONE server.
//!
//! The exact escape class behind #2316 (thread collapse) and #2361 (phase gauge
//! / stall): every prior `do_get` integration test drives a SINGLE client stream,
//! so a failure that only manifests under real concurrency (N simultaneous
//! streams sharing one server + one warm registry + the k-way merge pool) has no
//! asserting coverage. This suite spawns N≥8 simultaneous `do_get` streams of
//! MIXED shapes (full scan, PK point-read, LIMIT-k, plus one client that drops
//! midstream) over the REAL loopback gRPC transport and asserts every stream
//! completes within a generous hang-detector timeout AND returns the correct
//! result for its shape.
//!
//! The fixture is written by the write engine, so it is UNCOMPRESSED `nb-big` —
//! the non-stitching read path (issue #2370 arm 4: concurrency × uncompressed).
//! A second test additionally drives the concurrent workload over a REAL
//! compressed corpus table (the stitching `V5CompressedLegacy` path) when the
//! gitignored `Data.db` binaries are present.
//!
//! These tests assert per-stream RESULT correctness under a generous timeout;
//! they deliberately avoid process-global gauge assertions so they stay robust
//! when the harness runs their sibling `#[test]`s concurrently (plain
//! `cargo test`). The precise mid-flight gauge read-back lives in its own
//! isolated binary (`issue_2370_gauge_readback_test`).
//!
//! Run with:
//! ```text
//! cargo test -p cqlite-flight --test issue_2370_concurrent_doget_test
//! ```

use std::sync::Arc;
use std::time::Duration;

use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::Ticket;
use futures::StreamExt;
use tokio::sync::Barrier;

use cqlite_flight::service::CqliteFlightService;

mod concurrent_support;
use concurrent_support as support;

/// Generous per-stream hang detector. On a loaded box a real concurrent `do_get`
/// completes in well under a second; this bound is only a fail-loud ceiling that
/// turns a #2316/#2361-class hang (a stream that never completes) into a clear
/// test failure, never a tight wall-clock margin that could flake under load.
const STREAM_TIMEOUT: Duration = Duration::from_secs(60);

/// The shape each concurrent client drives, plus its expected result.
enum Shape {
    /// Full scan — must return all `expect_rows` rows.
    Scan { expect_rows: usize },
    /// PK point-read for `key` — must return exactly that one partition.
    Point { key: String },
    /// `LIMIT n` — must return exactly `n` rows spanning BOTH generations.
    Limit { n: u64 },
    /// A client that reads one batch then drops the stream midstream (the Trino
    /// "stop reading once satisfied" shape). Must not hang; result unchecked.
    DropMidstream,
}

/// Drive one `do_get` of `shape` against a fresh client dialed at `addr`, fully
/// checking the per-shape result. Returns a human label for diagnostics.
///
/// `start` is a shared start barrier (roborev job 1655 LOW): every caller
/// connects FIRST, then rendezvous on `start` before issuing its `do_get`, so
/// all N clients genuinely fire their RPC together — no client can finish (or
/// even begin) before every other client has connected and is ready to go. This
/// strengthens the overlap guarantee the test exists to prove; without it, a
/// fast-connecting client could complete its whole `do_get` before a
/// slower-connecting sibling even issues its RPC, undermining the "N truly
/// concurrent" claim.
async fn run_shape(
    addr: std::net::SocketAddr,
    shape: Shape,
    total: usize,
    start: Arc<Barrier>,
) -> String {
    let mut client = support::connect(addr).await;
    start.wait().await;
    match shape {
        Shape::Scan { expect_rows } => {
            let batches = support::do_get_batches(&mut client, support::scan_ticket()).await;
            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(
                rows, expect_rows,
                "concurrent full scan must return all {expect_rows} rows, got {rows}"
            );
            let keys = support::column_strings(&batches, "key");
            let distinct: std::collections::BTreeSet<_> = keys.iter().collect();
            assert_eq!(
                distinct.len(),
                expect_rows,
                "concurrent full scan must return {expect_rows} distinct partitions"
            );
            "scan".into()
        }
        Shape::Point { key } => {
            let batches = support::do_get_batches(&mut client, support::point_ticket(&key)).await;
            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(
                rows, 1,
                "concurrent point read pk={key} must return exactly its one partition, got {rows}"
            );
            let keys = support::column_strings(&batches, "key");
            assert_eq!(
                keys,
                vec![key.clone()],
                "concurrent point read must return the target partition, not another"
            );
            format!("point({key})")
        }
        Shape::Limit { n } => {
            let batches = support::do_get_batches(&mut client, support::limit_ticket(n)).await;
            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(
                rows, n as usize,
                "concurrent LIMIT {n} over {total} rows must return exactly {n} rows, got {rows}"
            );
            // The capped result must genuinely span both interleaved generations.
            let keys = support::column_strings(&batches, "key");
            let from_flush1 = keys.iter().any(|k| support::key_index(k) % 2 == 0);
            let from_flush2 = keys.iter().any(|k| support::key_index(k) % 2 == 1);
            assert!(
                from_flush1 && from_flush2,
                "concurrent LIMIT {n} result must span BOTH SSTables \
                 (flush1={from_flush1}, flush2={from_flush2}); keys={keys:?}"
            );
            format!("limit({n})")
        }
        Shape::DropMidstream => {
            let resp = client
                .do_get(Ticket::new(support::scan_ticket()))
                .await
                .expect("do_get rpc");
            let stream = resp.into_inner().map(|r| r.map_err(FlightError::Tonic));
            let mut rb = FlightRecordBatchStream::new_from_flight_data(stream);
            // Read exactly one decoded batch, then drop everything without draining.
            let _ = rb.next().await;
            drop(rb);
            drop(client);
            "drop_midstream".into()
        }
    }
}

/// **N-concurrent mixed-shape do_get over UNCOMPRESSED (non-stitching) path.**
/// Eight simultaneous `do_get` streams — 3 full scans, 2 PK point-reads, 2
/// LIMIT-k, and 1 that drops midstream — against ONE server over the real gRPC
/// transport. Every stream must complete within the generous hang-detector
/// timeout and return the correct result for its shape. This is the standing net
/// for the #2316/#2361 concurrency escape class.
#[test]
fn eight_concurrent_mixed_shape_do_gets_all_complete_correctly() {
    let total = 40usize;
    let (_temp, data_dir) = support::build_multi_sstable_fixture(total);
    // Small batch size so each scan streams several record batches — real
    // interleaving/overlap across the concurrent streams, not one-shot replies.
    let svc = CqliteFlightService::new(data_dir, 4);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async move {
        let running = support::start_server(svc).await;
        let addr = running.addr;

        let shapes = vec![
            Shape::Scan { expect_rows: total },
            Shape::Point {
                key: "k000003".into(),
            },
            Shape::Limit { n: 7 },
            Shape::Scan { expect_rows: total },
            Shape::Point {
                key: "k000010".into(),
            },
            Shape::Limit { n: 5 },
            Shape::Scan { expect_rows: total },
            Shape::DropMidstream,
        ];
        let n = shapes.len();
        assert!(n >= 8, "issue #2370 requires N>=8 concurrent streams, have {n}");

        // Shared start barrier (roborev job 1655 LOW): every client connects, THEN
        // rendezvous here before issuing its `do_get`, so all N genuinely fire
        // together — no client can finish (or even start streaming) before every
        // other client is connected and ready, strengthening the overlap this
        // suite exists to prove.
        let start = Arc::new(Barrier::new(n));

        // Spawn every stream simultaneously; each is bounded by its own hang
        // detector so a single wedged stream fails loudly (naming its shape)
        // rather than the whole test timing out anonymously.
        let mut handles = Vec::new();
        for shape in shapes {
            let start = start.clone();
            handles.push(tokio::spawn(async move {
                tokio::time::timeout(STREAM_TIMEOUT, run_shape(addr, shape, total, start))
                    .await
                    .expect("a concurrent do_get stream did not complete within the hang-detector timeout")
            }));
        }

        let mut labels = Vec::new();
        for h in handles {
            labels.push(h.await.expect("concurrent stream task panicked"));
        }
        assert_eq!(labels.len(), n, "every concurrent stream produced a result");

        running.server.abort();
    });
}

/// **Concurrency × REAL compressed corpus (stitching path).** The same N
/// simultaneous full scans, but against a real Cassandra 5.0 compressed `nb-big`
/// corpus table (`V5CompressedLegacy`) — the concurrency arm of the stitching
/// read path. Skips cleanly when the gitignored `Data.db` binary is absent (the
/// repo ships only JSONL references), and asserts `> 0` rows so it can never
/// 0-row-pass on an unfetched checkout.
#[test]
fn concurrent_full_scans_over_real_compressed_corpus() {
    let Some(root) = std::env::var_os("CQLITE_DATASETS_ROOT") else {
        eprintln!("CQLITE_DATASETS_ROOT unset — skipping real compressed concurrency repro");
        return;
    };
    let data_dir = std::path::PathBuf::from(&root).join("sstables");
    // Discover the `simple_table-<uuid>/nb-*-big-Data.db` binary by GLOB rather
    // than a hardcoded uuid + generation, so a future dataset regen (a new table
    // uuid or generation number) can never silently skip this arm forever — it
    // would instead just find the regenerated file under the same table-name
    // prefix (see `find_real_compressed_fixture`).
    // The discovered path only proves the binary's presence; the service itself
    // is pointed at `data_dir` (it re-resolves the table dir from the ticket).
    if support::find_real_compressed_fixture(&data_dir, "test_basic", "simple_table").is_none() {
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

    let svc = CqliteFlightService::new(data_dir, 16);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async move {
        let running = support::start_server(svc).await;
        let addr = running.addr;

        // Eight concurrent full scans of the SAME compressed table — they share
        // the server + warm registry, exercising the stitching path under load.
        let mut handles = Vec::new();
        for _ in 0..8 {
            let ticket = ticket.clone();
            handles.push(tokio::spawn(async move {
                let mut client = support::connect(addr).await;
                let batches = tokio::time::timeout(
                    STREAM_TIMEOUT,
                    support::do_get_batches(&mut client, ticket),
                )
                .await
                .expect("a concurrent compressed-corpus scan did not complete in time");
                batches.iter().map(|b| b.num_rows()).sum::<usize>()
            }));
        }

        let mut first: Option<usize> = None;
        for h in handles {
            let rows = h.await.expect("concurrent compressed scan task panicked");
            assert!(
                rows > 0,
                "each concurrent scan of the real compressed corpus must decode > 0 rows"
            );
            // Every concurrent scan of the same immutable table must agree.
            match first {
                None => first = Some(rows),
                Some(f) => assert_eq!(
                    rows, f,
                    "all concurrent scans of the same table must return the same row count"
                ),
            }
        }

        running.server.abort();
    });
}
