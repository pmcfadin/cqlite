//! Issue #2412 §G / spec Requirement 7 (Stage 6) — the lazy Summary-guided BIG
//! index proven END-TO-END through the flight `do_get` path, both cold and warm.
//!
//! Two `do_get`s against the SAME real Cassandra-produced BIG table (a real
//! `Summary.db` + `Index.db` pair — the shape #2412's lazy open targets):
//!
//! - **Cold** (first request): resolves rows over ZERO full `Index.db` parses
//!   (open is lazy over the on-disk `Summary.db`, design §A) — open work is
//!   bounded by `Summary.db` size, not partition count.
//! - **Warm** (second request, same service, unchanged generation): the SAME
//!   rows, with ZERO further reader opens (the warm registry serves the cached
//!   reader) AND ZERO full `Index.db` parses (the streaming query path never
//!   materializes the resident map, Stage 4).
//!
//! This is the wiring-evidence surface the spec requires: a named public API
//! (`FlightService::do_get`) + a real call chain (ticket → warm registry →
//! Summary-guided streaming merge → Arrow batches), not a helper-only unit test.
//! Row-count equality between the cold and warm runs is the query-semantics
//! correctness proof (a `WHERE`-less full scan's post-reconciliation result is
//! whatever the golden JSONL enumerates, byte-identically reproduced on both
//! requests) — the same non-vacuity discipline
//! `do_get_over_transport_real_compressed_fixture` uses.
//!
//! ## Why a REAL dataset table, not a synthetic write-engine fixture
//!
//! A write-engine-produced (synthetic) `nb`-tagged Data.db is headerless, and
//! `CassandraVersion` detection for a headerless file falls back to a magic-byte
//! sniff of the FIRST few partition bytes (`reader/header.rs`): unless those
//! bytes coincidentally match a specific magic, it classifies as `V5_0NewBig`
//! (chunk-stitching), which decodes via `drain_compaction_window` and never
//! touches the `Index.db`-based enumeration paths at all — making a synthetic
//! fixture an unreliable, data-dependent discriminator for this scenario. A
//! REAL Cassandra-produced table (this file's fixture) always classifies
//! correctly and reliably exercises the Index.db-guided paths #2412 targets
//! (confirmed empirically: `issue_2385_index_single_parse` proves `Index.db` a
//! full materializing enumeration of this SAME table parses exactly once).
//!
//! ## Separate integration-test process
//!
//! The OTel capture harness installs a PROCESS-GLOBAL meter provider (roborev
//! #2163 precedent), so this file holds exactly one `#[test]` in its own binary.
//!
//! Run with:
//! ```text
//! cargo test -p cqlite-flight --features observability-testing \
//!   --test issue_2412_do_get_cold_warm_e2e
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

const DDL: &str = "CREATE TABLE test_basic.uncompressed_table (\
    id uuid PRIMARY KEY, data text, value int, timestamp_val timestamp)";

fn ticket_bytes() -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "keyspace": "test_basic",
        "table": "uncompressed_table",
        "ddl": DDL,
    }))
    .unwrap()
}

/// Locate the real fixture's `Data.db` sibling directory, resolved the same way
/// `do_get_transport_test.rs::do_get_over_transport_real_compressed_fixture`
/// does. `uncompressed_table` (no `CompressionInfo.db`) is deliberately chosen
/// over a compressed table: a REAL Cassandra-produced compressed `nb` table
/// takes the chunk-stitching read path (`requires_chunk_stitching() == true`),
/// which decodes via chunk decompression and never touches the `Index.db`
/// resident-map paths AT ALL regardless of #2412 — an uncompressed table is
/// what actually routes through the Summary-guided / full-index-enumeration
/// machinery this issue changes. Returns `None` (never a hard failure) when
/// `CQLITE_DATASETS_ROOT` is unset or the gitignored binary is absent — the repo
/// ships only the JSONL references, so a fresh worktree that never ran
/// `fetch-datasets.sh` must skip cleanly, not fail.
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

/// Drive `do_get` and decode every batch into sorted `data` values — a
/// value-level correctness check (matches the convention used across the
/// #2310/#2412 warm-hit wiring-evidence tests), proving the cold and warm runs
/// resolve byte-identical content, not merely "some rows."
async fn do_get_sorted_names(svc: &CqliteFlightService, ticket: Vec<u8>) -> Vec<String> {
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
        let names = batch
            .column_by_name("data")
            .expect("data column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name is text");
        for i in 0..names.len() {
            out.push(names.value(i).to_string());
        }
    }
    out.sort();
    out
}

/// Spec Requirement 7's pinned scenario, both halves in one test (they share
/// the one warm service instance / process-global meter):
///
/// - Cold `do_get`: resolves real rows, 0 full `Index.db` parses.
/// - Warm repeat `do_get` (same generation): the SAME rows, 0 further reader
///   opens, 0 full `Index.db` parses.
#[test]
fn cold_and_warm_do_get_resolve_correct_rows_with_bounded_work() {
    let Some((data_dir, table_dir)) = real_fixture_dirs() else {
        eprintln!(
            "real fixture Data.db binary absent (set CQLITE_DATASETS_ROOT + run \
             fetch-datasets.sh) — skipping issue #2412 cold/warm e2e"
        );
        return;
    };
    // Precondition (never assumed): the real fixture ships a usable Summary.db —
    // the shape a lazy BIG open (design §A) actually applies to.
    let summary_db = table_dir.join("nb-1-big-Summary.db");
    assert!(
        summary_db.is_file(),
        "fixture precondition: the real table must ship a Summary.db"
    );

    // Install the process-global in-memory meter BEFORE any parse in this process.
    let mc = testing::metrics_capture();

    let svc = CqliteFlightService::new(data_dir, 8192);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // ---- Cold do_get ----
    mc.reset();
    let cold_names = rt.block_on(do_get_sorted_names(&svc, ticket_bytes()));
    let cold_parses = mc
        .flush_and_collect()
        .counter_sum(catalog::INDEX_PARSES_TOTAL);
    let opens_after_cold = svc.warm_metrics().reader_opens;

    assert!(
        !cold_names.is_empty(),
        "cold do_get must resolve at least one row from the real fixture (never \
         a silent 0-rows-when-present pass)"
    );
    assert_eq!(
        cold_parses, 0.0,
        "cold do_get over a generation with a usable Summary.db must perform ZERO \
         full Index.db parses (issue #2412 lazy open, spec Requirement 7); got \
         {cold_parses}"
    );
    assert_eq!(
        opens_after_cold, 1,
        "cold do_get must open exactly the one generation once (real work \
         happened — never a vacuous 0-open skip)"
    );

    // ---- Warm repeat do_get (same generation, unchanged) ----
    mc.reset();
    let warm_names = rt.block_on(do_get_sorted_names(&svc, ticket_bytes()));
    let warm_parses = mc
        .flush_and_collect()
        .counter_sum(catalog::INDEX_PARSES_TOTAL);
    let opens_after_warm = svc.warm_metrics().reader_opens;

    assert_eq!(
        warm_names, cold_names,
        "warm repeat do_get must resolve the SAME rows as the cold request \
         (query-semantics correctness, byte-identical across cold/warm)"
    );
    assert_eq!(
        warm_parses, 0.0,
        "warm repeat do_get must perform ZERO full Index.db parses over the \
         unchanged generation (spec Requirement 7); got {warm_parses}"
    );
    assert_eq!(
        opens_after_warm, opens_after_cold,
        "warm repeat do_get over the SAME generation must perform ZERO further \
         reader opens (the cached warm reader is reused, not re-opened); opens \
         climbed from {opens_after_cold} to {opens_after_warm}"
    );
}
