//! Issue #2412 §G / spec Requirement 7 (Stage 6) — the COMPRESSED chunk-stitching
//! warm-query decode path (`stream_partitions_summary_guided_compaction`,
//! `requires_chunk_stitching() == true`) proven end-to-end through the flight
//! `do_get` path, cold and warm. Sibling of `issue_2412_do_get_cold_warm_e2e.rs`
//! (which deliberately covers the UNCOMPRESSED / `stream_partitions_summary_guided`
//! branch instead — the two decoders in `summary_scan.rs` share the walk but
//! decode differently, and until this file neither real-dataset e2e exercised the
//! compressed branch, roborev-flagged wiring-evidence gap).
//!
//! `test_basic.compression_test_table` is a REAL Cassandra-produced, Snappy-
//! compressed `nb-big` table (`CompressionInfo.db` present) — the shape that
//! routes `stream_all_partitions_for_query` through
//! `stream_partitions_summary_guided_compaction` (full-fidelity `CompactionRow`
//! decode over `read_compressed_offset_window`-mapped chunks), NOT the
//! uncompressed `ScanRow` decoder the sibling file covers.
//!
//! Same cold/warm assertions as the sibling: cold = 0 full `Index.db` parses + 1
//! real reader-open; warm repeat = 0 further parses + 0 further opens; identical
//! rows both times (query-semantics correctness, byte-identical across cold/warm).
//!
//! ## Separate integration-test process
//!
//! The OTel capture harness installs a PROCESS-GLOBAL meter provider (roborev
//! #2163 precedent), so this file holds exactly one `#[test]` in its own binary.
//!
//! Run with:
//! ```text
//! cargo test -p cqlite-flight --features observability-testing \
//!   --test issue_2412_do_get_cold_warm_e2e_compressed
//! ```

#![cfg(feature = "observability-testing")]

use std::path::PathBuf;

use arrow::array::{Array, StringArray};
use arrow::record_batch::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::Ticket;
use futures::StreamExt;
use tonic::Request;

use cqlite_core::observability::{catalog, testing};
use cqlite_flight::service::CqliteFlightService;

const DDL: &str = "CREATE TABLE test_basic.compression_test_table (\
    id uuid PRIMARY KEY, large_text text, repeated_data text, random_data blob, \
    compressed_json text)";

fn ticket_bytes() -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "keyspace": "test_basic",
        "table": "compression_test_table",
        "ddl": DDL,
    }))
    .unwrap()
}

/// Locate the real fixture's `Data.db` sibling directory, resolved the same way
/// the sibling (uncompressed) e2e test does. Returns `None` (never a hard
/// failure) when `CQLITE_DATASETS_ROOT` is unset or the gitignored binary is
/// absent — the repo ships only the JSONL references.
fn real_fixture_dirs() -> Option<(PathBuf, PathBuf)> {
    let root = std::env::var_os("CQLITE_DATASETS_ROOT")?;
    let data_dir = PathBuf::from(&root).join("sstables");
    let table_dir = data_dir
        .join("test_basic")
        .join("compression_test_table-6ad6ad30a25111f0a3fef1a551383fb9");
    let data_db = table_dir.join("nb-1-big-Data.db");
    if !data_db.is_file() {
        return None;
    }
    Some((data_dir, table_dir))
}

/// Drive `do_get` and decode every batch into sorted `compressed_json` values —
/// a value-level correctness check proving the cold and warm runs resolve
/// byte-identical content, not merely "some rows."
async fn do_get_sorted_values(svc: &CqliteFlightService, ticket: Vec<u8>) -> Vec<String> {
    let resp = svc
        .do_get(Request::new(Ticket::new(ticket)))
        .await
        .expect("do_get");
    let stream = resp
        .into_inner()
        .map(|r| r.map_err(|s| FlightError::ExternalError(Box::new(s))));
    let mut rb = FlightRecordBatchStream::new_from_flight_data(stream);
    let mut out = Vec::new();
    while let Some(batch) = rb.next().await {
        let batch: RecordBatch = batch.expect("decode batch");
        let values = batch
            .column_by_name("compressed_json")
            .expect("compressed_json column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("compressed_json is text");
        for i in 0..values.len() {
            out.push(values.value(i).to_string());
        }
    }
    out.sort();
    out
}

/// Spec Requirement 7's pinned scenario over the COMPRESSED chunk-stitching decode
/// path (both halves in one test — they share the one warm service instance /
/// process-global meter):
///
/// - Cold `do_get`: resolves real rows, 0 full `Index.db` parses.
/// - Warm repeat `do_get` (same generation): the SAME rows, 0 further reader
///   opens, 0 full `Index.db` parses.
#[test]
fn cold_and_warm_do_get_resolve_correct_rows_over_compressed_chunks() {
    let Some((data_dir, table_dir)) = real_fixture_dirs() else {
        eprintln!(
            "real fixture Data.db binary absent (set CQLITE_DATASETS_ROOT + run \
             fetch-datasets.sh) — skipping issue #2412 compressed cold/warm e2e"
        );
        return;
    };
    // Preconditions (never assumed): the real fixture ships a usable Summary.db
    // (the shape a lazy BIG open, design §A, applies to) AND a CompressionInfo.db
    // (proving this fixture genuinely takes the chunk-stitching decode branch —
    // the whole point of this file, distinct from the sibling uncompressed test).
    let summary_db = table_dir.join("nb-1-big-Summary.db");
    assert!(
        summary_db.is_file(),
        "fixture precondition: the real table must ship a Summary.db"
    );
    let compression_info = table_dir.join("nb-1-big-CompressionInfo.db");
    assert!(
        compression_info.is_file(),
        "fixture precondition: this table must be COMPRESSED (CompressionInfo.db \
         present) — the whole point of this file is exercising the \
         chunk-stitching decode branch the sibling (uncompressed) test cannot"
    );

    // Install the process-global in-memory meter BEFORE any parse in this process.
    let mc = testing::metrics_capture();

    let svc = CqliteFlightService::new(data_dir, 8192);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // ---- Cold do_get ----
    mc.reset();
    let cold_values = rt.block_on(do_get_sorted_values(&svc, ticket_bytes()));
    let cold_parses = mc
        .flush_and_collect()
        .counter_sum(catalog::INDEX_PARSES_TOTAL);
    let opens_after_cold = svc.warm_metrics().reader_opens;

    assert!(
        !cold_values.is_empty(),
        "cold do_get must resolve at least one row from the real compressed \
         fixture (never a silent 0-rows-when-present pass)"
    );
    assert_eq!(
        cold_parses, 0.0,
        "cold do_get over a generation with a usable Summary.db must perform ZERO \
         full Index.db parses (issue #2412 lazy open, spec Requirement 7), even \
         over the compressed chunk-stitching decode branch; got {cold_parses}"
    );
    assert_eq!(
        opens_after_cold, 1,
        "cold do_get must open exactly the one generation once (real work \
         happened — never a vacuous 0-open skip)"
    );

    // ---- Warm repeat do_get (same generation, unchanged) ----
    mc.reset();
    let warm_values = rt.block_on(do_get_sorted_values(&svc, ticket_bytes()));
    let warm_parses = mc
        .flush_and_collect()
        .counter_sum(catalog::INDEX_PARSES_TOTAL);
    let opens_after_warm = svc.warm_metrics().reader_opens;

    assert_eq!(
        warm_values, cold_values,
        "warm repeat do_get must resolve the SAME rows as the cold request over \
         the compressed chunk-stitching decode branch (query-semantics \
         correctness, byte-identical across cold/warm)"
    );
    assert_eq!(
        warm_parses, 0.0,
        "warm repeat do_get must perform ZERO full Index.db parses over the \
         unchanged compressed generation (spec Requirement 7); got {warm_parses}"
    );
    assert_eq!(
        opens_after_warm, opens_after_cold,
        "warm repeat do_get over the SAME compressed generation must perform \
         ZERO further reader opens (the cached warm reader is reused, not \
         re-opened); opens climbed from {opens_after_cold} to {opens_after_warm}"
    );
}
