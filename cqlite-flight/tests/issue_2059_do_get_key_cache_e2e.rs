//! Issue #2059 (Stage 5, spec Requirement "Exercised end-to-end through the flight
//! do_get path (cold and warm)") — the process-global key→partition-offset cache
//! proven END-TO-END through the flight `do_get` POINT-READ path.
//!
//! Two point-read `do_get`s (`WHERE id = <present uuid>`) against the SAME real
//! Cassandra-produced BIG table served over the lazy Summary-guided path (#2412):
//!
//! - **Cold** (first request): a cache MISS → resolves the partition through EXACTLY
//!   ONE bounded `Index.db` interval (`cqlite.sstable.index_interval_parses_total`
//!   climbs) and POPULATES the global key cache.
//! - **Warm** (second request, same service, unchanged generation): a cache HIT →
//!   the SAME row with ZERO further interval parses (the location is served from the
//!   global cache, skipping the interval read entirely).
//!
//! This is the wiring-evidence surface the spec requires: a named public API
//! (`FlightService::do_get`) + a real call chain (ticket → warm registry → point
//! lookup → global key cache → Arrow batches), not a helper-only unit test. Row
//! equality between cold and warm is the query-semantics correctness proof.
//!
//! The present uuid is harvested via an INDEPENDENT core `IndexReader` (NOT a
//! full-scan `do_get` on the service under test) so the reader the service opens
//! stays LAZY (a full scan would `ensure_materialized`, routing point reads through
//! the resident map instead of the Summary-guided interval path this pin measures).
//!
//! Separate integration-test process (process-global meter, roborev #2163).
//!
//! Run with:
//! ```text
//! cargo test -p cqlite-flight --features observability-testing \
//!   --test issue_2059_do_get_key_cache_e2e
//! ```

#![cfg(feature = "observability-testing")]

use std::path::PathBuf;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::Ticket;
use futures::StreamExt;
use tonic::Request;

use cqlite_core::observability::{catalog, testing};
use cqlite_core::platform::Platform;
use cqlite_core::storage::cache::GlobalKeyOffsetCache;
use cqlite_core::storage::sstable::index_reader::IndexReader;
use cqlite_core::Config;
use cqlite_flight::service::CqliteFlightService;

const DDL: &str = "CREATE TABLE test_basic.uncompressed_table (\
    id uuid PRIMARY KEY, data text, value int, timestamp_val timestamp)";

fn real_fixture_dirs() -> Option<(PathBuf, PathBuf)> {
    let root = std::env::var_os("CQLITE_DATASETS_ROOT")?;
    let data_dir = PathBuf::from(&root).join("sstables");
    let table_dir = data_dir
        .join("test_basic")
        .join("uncompressed_table-6aedb7a0a25111f0a3fef1a551383fb9");
    let data_db = table_dir.join("nb-1-big-Data.db");
    if !data_db.is_file() {
        return None;
    }
    Some((data_dir, table_dir))
}

/// Format 16 raw partition-key bytes as a canonical hyphenated UUID string (the
/// literal form the flight predicate parser expects for a `uuid` PK).
fn uuid_string(bytes: &[u8]) -> Option<String> {
    if bytes.len() != 16 {
        return None;
    }
    let h: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    Some(format!(
        "{}-{}-{}-{}-{}",
        &h[0..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..32]
    ))
}

fn point_ticket(uuid: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "keyspace": "test_basic",
        "table": "uncompressed_table",
        "ddl": DDL,
        "filter": {"type": "Compare", "column": "id", "op": "Equal", "value": uuid},
    }))
    .unwrap()
}

/// Total rows returned by a `do_get`.
async fn do_get_row_count(svc: &CqliteFlightService, ticket: Vec<u8>) -> usize {
    let resp = svc
        .do_get(Request::new(Ticket::new(ticket)))
        .await
        .expect("do_get");
    let stream = resp
        .into_inner()
        .map(|r| r.map_err(|s| FlightError::ExternalError(Box::new(s))));
    let mut rb = FlightRecordBatchStream::new_from_flight_data(stream);
    let mut rows = 0usize;
    while let Some(batch) = rb.next().await {
        let batch: RecordBatch = batch.expect("decode batch");
        rows += batch.num_rows();
    }
    rows
}

#[test]
fn cold_point_do_get_populates_warm_hit_skips_interval_parse() {
    let Some((data_dir, table_dir)) = real_fixture_dirs() else {
        eprintln!(
            "real fixture Data.db binary absent (set CQLITE_DATASETS_ROOT + run \
             fetch-datasets.sh) — skipping issue #2059 do_get key-cache e2e"
        );
        return;
    };
    let summary_db = table_dir.join("nb-1-big-Summary.db");
    assert!(
        summary_db.is_file(),
        "fixture precondition: the real table must ship a Summary.db (lazy open shape)"
    );

    let rt = tokio::runtime::Runtime::new().unwrap();

    // Harvest a present uuid via an INDEPENDENT core IndexReader.
    let uuid = {
        let config = Config::default();
        let platform = Arc::new(rt.block_on(Platform::new(&config)).expect("platform"));
        let index_path = table_dir.join("nb-1-big-Index.db");
        let ir = rt
            .block_on(IndexReader::open(&index_path, platform))
            .expect("Index.db open for present-key harvest");
        let entries = ir.get_partition_entries();
        assert!(
            !entries.is_empty(),
            "fixture must expose present Index.db entries (0-when-present is a failure)"
        );
        uuid_string(&entries[0].key_digest).expect("uuid PK is 16 raw bytes")
    };

    // Install the process-global meter and start from a COLD global key cache.
    let mc = testing::metrics_capture();
    GlobalKeyOffsetCache::global().invalidate_all();

    let svc = CqliteFlightService::new(data_dir, 8192);

    // ---- Cold point-read do_get: miss → populate (interval parse climbs) ----
    mc.reset();
    let cold_rows = rt.block_on(do_get_row_count(&svc, point_ticket(&uuid)));
    let cold_intervals = mc
        .flush_and_collect()
        .counter_sum(catalog::INDEX_INTERVAL_PARSES_TOTAL);

    assert_eq!(
        cold_rows, 1,
        "the cold point read must resolve exactly the one present partition (never a \
         silent 0-when-present pass)"
    );
    assert!(
        cold_intervals >= 1.0,
        "the cold point read (cache miss) must read at least one bounded Index.db \
         interval to resolve + populate; got {cold_intervals}"
    );

    // ---- Warm repeat point-read do_get: hit → skip the interval parse ----
    mc.reset();
    let warm_rows = rt.block_on(do_get_row_count(&svc, point_ticket(&uuid)));
    let warm_intervals = mc
        .flush_and_collect()
        .counter_sum(catalog::INDEX_INTERVAL_PARSES_TOTAL);

    assert_eq!(
        warm_rows, cold_rows,
        "the warm repeat point read must resolve the SAME row as the cold request \
         (query-semantics correctness)"
    );
    assert_eq!(
        warm_intervals, 0.0,
        "the warm point read must be served from the global key cache with ZERO \
         further Index.db interval parses over the unchanged generation; got {warm_intervals}"
    );
}
