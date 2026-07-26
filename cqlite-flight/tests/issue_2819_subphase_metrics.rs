//! Issue #2819 — end-to-end proof of the in-`stream` data-plane SUB-PHASE timers
//! over the real `CqliteFlightService::do_get` egress path, through the shared
//! `observability-testing` metrics-capture harness.
//!
//! The `stream` RPC phase is decomposed into five bounded `cqlite.rpc.phase`
//! values on the EXISTING `cqlite.rpc.phase.duration` histogram (no new metric
//! name / attribute key): `stream_cold_fault` + `stream_decompress` (feed thread),
//! `stream_merge` + `stream_encode` (merge consumer thread), `stream_grpc_write`
//! (egress thread). Per the amended (pipeline-correct) accounting model, the
//! sub-phases run on CONCURRENT threads and OVERLAP in wall-clock — they are NOT
//! asserted to sum to `stream`. These tests exercise the REAL production `do_get`
//! (warm reader path → `spawn_streaming_from_readers`), draining the whole stream,
//! and read back the emitted OTel series.
//!
//! Each test is `#[serial]` and resets the process-global capture first, so the
//! delta-temporality provider isolates one `do_get`'s metrics per test (mirroring
//! the `compressed_do_get_transport_test.rs` serial-isolation convention).
//!
//! Run with:
//! ```text
//! cargo test -p cqlite-flight --features observability-testing --test issue_2819_subphase_metrics
//! ```

#![cfg(feature = "observability-testing")]

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use arrow_flight::flight_service_server::FlightService;
use arrow_flight::Ticket;
use futures::StreamExt;
use serial_test::serial;
use tonic::Request;

use cqlite_core::observability::{catalog, testing};
use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use cqlite_flight::service::CqliteFlightService;

mod fixture_support;

const BIG_TAG: &str = "nb-1-big";
const COMP_KS: &str = "test_comp";

/// The five bounded in-`stream` sub-phase `cqlite.rpc.phase` values.
const SUBPHASES: [&str; 5] = [
    "stream_cold_fault",
    "stream_decompress",
    "stream_merge",
    "stream_encode",
    "stream_grpc_write",
];

/// The bounded attribute keys any phase.duration point may carry.
const BOUNDED_KEYS: &[&str] = &[
    catalog::attr::RPC_METHOD,
    catalog::attr::RPC_PHASE,
    catalog::attr::RPC_STATUS,
];

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

/// Resolve the compressed LZ4 corpus table, or skip (hard-fail under
/// `CQLITE_REQUIRE_FIXTURES=1`) — never a silent 0-row false pass.
fn lz4_fixture_or_skip() -> Option<fixture_support::ResolvedFixture> {
    match fixture_support::table_dir_by_prefix(COMP_KS, "lz4_table", BIG_TAG) {
        Some(found) => {
            let info = found.dir.join(format!("{BIG_TAG}-CompressionInfo.db"));
            assert!(
                info.is_file(),
                "lz4_table must ship a CompressionInfo.db for the compressed sub-phase \
                 assertions to mean anything (looked at {})",
                info.display()
            );
            Some(found)
        }
        None => {
            let msg = "test_comp.lz4_table compressed corpus absent";
            assert!(!require_fixtures(), "CQLITE_REQUIRE_FIXTURES=1: {msg}");
            eprintln!("SKIP: {msg}");
            None
        }
    }
}

fn lz4_ticket(limit: Option<u64>) -> Vec<u8> {
    let mut t = serde_json::json!({
        "keyspace": COMP_KS,
        "table": "lz4_table",
        "ddl": "CREATE TABLE test_comp.lz4_table (pk int, ck int, body text, PRIMARY KEY (pk, ck))",
    });
    if let Some(k) = limit {
        t["limit"] = serde_json::json!(k);
    }
    serde_json::to_vec(&t).expect("ticket json")
}

/// A `(phase_value -> (histogram_sum_seconds, sample_count))` map for the
/// sub-phase points recorded on `cqlite.rpc.phase.duration`.
fn subphase_points(m: &testing::CapturedMetrics) -> HashMap<String, (f64, u64)> {
    let mut out = HashMap::new();
    if let Some(entry) = m.find(catalog::RPC_PHASE_DURATION) {
        for p in &entry.points {
            for (k, v) in &p.attributes {
                if k == catalog::attr::RPC_PHASE && SUBPHASES.contains(&v.as_str()) {
                    out.insert(v.clone(), (p.value, p.count));
                }
            }
        }
    }
    out
}

/// Sum of the `stream` (top-level) phase duration recorded for this do_get.
fn stream_phase_seconds(m: &testing::CapturedMetrics) -> f64 {
    m.find(catalog::RPC_PHASE_DURATION)
        .map(|e| {
            e.points
                .iter()
                .filter(|p| {
                    p.attributes
                        .iter()
                        .any(|(k, v)| k == catalog::attr::RPC_PHASE && v == "stream")
                })
                .map(|p| p.value)
                .sum()
        })
        .unwrap_or(0.0)
}

/// Run one `do_get`, draining every stream item promptly. Returns once the whole
/// stream is consumed (so the merge completes and the sub-phase samples are
/// emitted at teardown).
async fn do_get_drain_all(svc: &CqliteFlightService, ticket: Vec<u8>) {
    let resp = svc
        .do_get(Request::new(Ticket::new(ticket)))
        .await
        .expect("do_get");
    let mut stream = resp.into_inner();
    let mut msgs = 0usize;
    while let Some(item) = stream.next().await {
        item.expect("stream item ok");
        msgs += 1;
    }
    assert!(msgs > 0, "do_get must yield at least the schema message");
}

/// Run one `do_get`, sleeping `per_item` between each stream poll so a
/// small-`batch_size` scan fills the bounded egress channel and the producer PARKS
/// in `sink.emit` — inflating `stream_grpc_write`.
async fn do_get_drain_slow(svc: &CqliteFlightService, ticket: Vec<u8>, per_item: Duration) {
    let resp = svc
        .do_get(Request::new(Ticket::new(ticket)))
        .await
        .expect("do_get");
    let mut stream = resp.into_inner();
    while let Some(item) = stream.next().await {
        item.expect("stream item ok");
        tokio::time::sleep(per_item).await;
    }
}

/// Flush a small uncompressed (WriteEngine — CQLite's own write surface, never a
/// `CompressionInfo.db`) fixture, for the "un-entered decompress" scenario.
fn build_uncompressed_fixture() -> (tempfile::TempDir, PathBuf) {
    let ks = "subphase_uncompressed_ks";
    let tbl = "items";
    let col = |name: &str, ty: &str, nullable: bool| Column {
        name: name.into(),
        data_type: ty.into(),
        nullable,
        default: None,
        is_static: false,
    };
    let schema = TableSchema {
        keyspace: ks.into(),
        table: tbl.into(),
        partition_keys: vec![KeyColumn {
            name: "id".into(),
            data_type: "int".into(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![col("id", "int", false), col("name", "text", true)],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };
    let temp = tempfile::TempDir::new().unwrap();
    let data_dir = temp.path().join("data");
    let config = WriteEngineConfig::new(data_dir.clone(), temp.path().join("wal"), schema);
    let mut engine = WriteEngine::new(config).expect("engine");
    for i in 1..=40 {
        engine
            .write(Mutation::new(
                TableId::new(ks, tbl),
                PartitionKey::single("id", Value::Integer(i)),
                None,
                vec![CellOperation::Write {
                    column: "name".into(),
                    value: Value::text(format!("n{i}")),
                }],
                100,
                None,
            ))
            .expect("write");
    }
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(engine.flush()).expect("flush").expect("info");
    (temp, data_dir)
}

fn uncompressed_ticket() -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "keyspace": "subphase_uncompressed_ks",
        "table": "items",
        "ddl": "CREATE TABLE subphase_uncompressed_ks.items (id int PRIMARY KEY, name text)",
    }))
    .unwrap()
}

// ============================ scenarios ============================

/// Requirement 1: a completed `do_get` over a real COMPRESSED fixture records at
/// least four distinct sub-phase samples, AND (positivity check) each recorded
/// sub-phase is > 0 and no greater than the RPC wall time — the sub-phases
/// attribute the in-`stream` cost across concurrent stages WITHOUT summing to it.
#[test]
#[serial]
fn compressed_do_get_records_at_least_four_positive_subphases() {
    let Some(found) = lz4_fixture_or_skip() else {
        return;
    };
    let mc = testing::metrics_capture();
    let svc = CqliteFlightService::new(found.sstables_root.clone(), 8);
    let rt = tokio::runtime::Runtime::new().unwrap();

    mc.reset();
    rt.block_on(do_get_drain_all(&svc, lz4_ticket(None)));
    let metrics = mc.flush_and_collect();

    let subs = subphase_points(&metrics);
    assert!(
        subs.len() >= 4,
        "a completed compressed do_get must record >= 4 distinct sub-phase samples \
         (got {}: {:?}) — cold-fault/decompress on the feed thread plus \
         merge/encode/grpc-write on the merge/egress thread",
        subs.len(),
        subs.keys().collect::<Vec<_>>()
    );
    // The compressed path must exercise BOTH the cold-IO and decompress sub-phases
    // — the whole point of the P1.3↔P1.5 split.
    assert!(
        subs.contains_key("stream_cold_fault") && subs.contains_key("stream_decompress"),
        "a compressed fixture must record both stream_cold_fault and stream_decompress, got {:?}",
        subs.keys().collect::<Vec<_>>()
    );

    let rpc_wall = metrics.counter_sum(catalog::RPC_DURATION);
    assert!(rpc_wall > 0.0, "cqlite.rpc.duration must be recorded");
    for (phase, (seconds, count)) in &subs {
        assert!(
            *seconds > 0.0,
            "recorded sub-phase {phase} must have a positive duration, got {seconds}"
        );
        // Each sub-phase is a sub-interval of the RPC, so its duration cannot
        // exceed the RPC wall time (small multiplicative slack for measurement).
        assert!(
            *seconds <= rpc_wall * 1.5 + 0.01,
            "sub-phase {phase} ({seconds}s) must not exceed the RPC wall time ({rpc_wall}s)"
        );
        assert!(
            *count >= 1,
            "sub-phase {phase} must have at least one recorded sample"
        );
    }
    // `stream` retains its meaning as the whole data-plane total (unchanged).
    assert!(
        stream_phase_seconds(&metrics) > 0.0,
        "the top-level `stream` phase sample must still be recorded"
    );
}

/// Requirement 1, scenario 3: an uncompressed-fixture run never invokes
/// decompression, so it records NO `stream_decompress` sample — while the other
/// sub-phases still record theirs.
#[test]
#[serial]
fn uncompressed_do_get_records_no_decompress_subphase() {
    let (_temp, data_dir) = build_uncompressed_fixture();
    let mc = testing::metrics_capture();
    let svc = CqliteFlightService::new(data_dir, 8);
    let rt = tokio::runtime::Runtime::new().unwrap();

    mc.reset();
    rt.block_on(do_get_drain_all(&svc, uncompressed_ticket()));
    let metrics = mc.flush_and_collect();

    let subs = subphase_points(&metrics);
    assert!(
        !subs.contains_key("stream_decompress"),
        "an uncompressed fixture (no CompressionInfo.db) must record NO stream_decompress \
         sample, got {:?}",
        subs.keys().collect::<Vec<_>>()
    );
    // The other data-plane sub-phases still record on the merge/egress thread:
    // merge (reconcile/materialize), encode (Arrow), and grpc-write (egress).
    for phase in ["stream_merge", "stream_encode", "stream_grpc_write"] {
        assert!(
            subs.contains_key(phase),
            "uncompressed do_get must still record {phase}, got {:?}",
            subs.keys().collect::<Vec<_>>()
        );
    }
}

/// Requirement 2: a slow client inflates `stream_grpc_write` (the egress park) but
/// not `stream_cold_fault` (feed-thread page-in) — the two are measured on
/// DISTINCT threads with no shared code interval, so client-drain speed cannot
/// leak into cold-IO latency.
#[test]
#[serial]
fn slow_client_inflates_grpc_write_but_not_cold_fault() {
    let Some(found) = lz4_fixture_or_skip() else {
        return;
    };
    let mc = testing::metrics_capture();
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Prompt drain: batch_size = 1 → many batches, but drained immediately so the
    // producer barely parks.
    mc.reset();
    let svc_prompt = CqliteFlightService::new(found.sstables_root.clone(), 1);
    rt.block_on(do_get_drain_all(&svc_prompt, lz4_ticket(Some(30))));
    let prompt = subphase_points(&mc.flush_and_collect());

    // Stalled drain: the client sleeps 40ms between polls, so with batch_size = 1
    // the bounded channel fills and the producer PARKS in sink.emit — grpc_write
    // accumulates far beyond the prompt run's.
    mc.reset();
    let svc_slow = CqliteFlightService::new(found.sstables_root.clone(), 1);
    rt.block_on(do_get_drain_slow(
        &svc_slow,
        lz4_ticket(Some(30)),
        Duration::from_millis(40),
    ));
    let stalled = subphase_points(&mc.flush_and_collect());

    let prompt_grpc = prompt.get("stream_grpc_write").map(|p| p.0).unwrap_or(0.0);
    let stalled_grpc = stalled.get("stream_grpc_write").map(|p| p.0).unwrap_or(0.0);
    let prompt_cold = prompt.get("stream_cold_fault").map(|p| p.0).unwrap_or(0.0);
    let stalled_cold = stalled.get("stream_cold_fault").map(|p| p.0).unwrap_or(0.0);

    // The stalled run's egress-write time is materially larger (the deliberate
    // 40ms-per-batch park dominates any scheduling noise). Comparing two METRIC
    // sums, not a host-latency threshold.
    assert!(
        stalled_grpc > prompt_grpc,
        "a stalled client must inflate stream_grpc_write (stalled {stalled_grpc}s vs \
         prompt {prompt_grpc}s) — the egress park is attributed to grpc_write"
    );
    // Cold-fault is feed-thread page-in of the SAME small fixture; the client stall
    // does not touch it, so it stays well below the stalled grpc_write. A send-side
    // stall can never inflate cold-IO latency (distinct threads, disjoint scopes).
    assert!(
        stalled_cold < stalled_grpc,
        "the client stall must NOT inflate stream_cold_fault: stalled cold_fault \
         ({stalled_cold}s) must stay below the inflated stream_grpc_write ({stalled_grpc}s)"
    );
    assert!(
        prompt_cold > 0.0 && stalled_cold > 0.0,
        "both runs must still record a readable stream_cold_fault sample"
    );
}

/// Requirement 2, scenario: the cold−warm delta on `stream_cold_fault` is
/// obtainable from the STANDING metric alone (no profiler). We assert the delta is
/// COMPUTABLE — both a cold (first-touch) and a warm (pages resident) run emit a
/// readable `stream_cold_fault` sample off `cqlite.rpc.phase.duration` — rather
/// than a wall-clock inequality (which would be a host-dependent flake, forbidden
/// in the correctness path).
#[test]
#[serial]
fn cold_warm_delta_on_cold_fault_is_readable_from_the_standing_metric() {
    let Some(found) = lz4_fixture_or_skip() else {
        return;
    };
    let mc = testing::metrics_capture();
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Cold run (first touch of the files in this process).
    mc.reset();
    let svc_cold = CqliteFlightService::new(found.sstables_root.clone(), 8);
    rt.block_on(do_get_drain_all(&svc_cold, lz4_ticket(None)));
    let cold = subphase_points(&mc.flush_and_collect());

    // Warm run (fresh service, but the OS page cache is now warm).
    mc.reset();
    let svc_warm = CqliteFlightService::new(found.sstables_root.clone(), 8);
    rt.block_on(do_get_drain_all(&svc_warm, lz4_ticket(None)));
    let warm = subphase_points(&mc.flush_and_collect());

    let cold_cf = cold.get("stream_cold_fault").map(|p| p.0);
    let warm_cf = warm.get("stream_cold_fault").map(|p| p.0);
    assert!(
        cold_cf.is_some() && warm_cf.is_some(),
        "both the cold and warm runs must emit a stream_cold_fault sample so the \
         cold-warm delta is computable from the standing metric alone (cold={cold_cf:?}, \
         warm={warm_cf:?})"
    );
    // The delta itself is a plain subtraction of two standing-metric values — the
    // point of the instrument (no profiler needed). We do not assert its sign to
    // avoid a host-dependent wall-clock flake.
    let _delta = cold_cf.unwrap() - warm_cf.unwrap();
}

/// Requirement 3: no new metric name or attribute key. The sub-phase samples ride
/// the pre-existing `cqlite.rpc.phase.duration` histogram, keyed by the
/// pre-existing `cqlite.rpc.phase` attribute — and the catalog gained no new
/// metric.
#[test]
#[serial]
fn subphases_add_no_new_metric_or_attribute_key() {
    let Some(found) = lz4_fixture_or_skip() else {
        return;
    };
    let mc = testing::metrics_capture();
    let svc = CqliteFlightService::new(found.sstables_root.clone(), 8);
    let rt = tokio::runtime::Runtime::new().unwrap();

    mc.reset();
    rt.block_on(do_get_drain_all(&svc, lz4_ticket(None)));
    let metrics = mc.flush_and_collect();

    // The ONLY histogram carrying sub-phase samples is the pre-existing one.
    let entry = metrics
        .find(catalog::RPC_PHASE_DURATION)
        .expect("cqlite.rpc.phase.duration must carry the sub-phase samples");
    let mut saw_subphase = false;
    for p in &entry.points {
        for (k, v) in &p.attributes {
            // Every attribute key stays in the bounded set — no new attribute key.
            assert!(
                BOUNDED_KEYS.contains(&k.as_str()),
                "phase.duration carries an unexpected attribute key {k:?}"
            );
            if k == catalog::attr::RPC_PHASE && SUBPHASES.contains(&v.as_str()) {
                saw_subphase = true;
                // Sub-phase values appear ONLY on the `do_get` method.
                let method = p
                    .attributes
                    .iter()
                    .find(|(mk, _)| mk == catalog::attr::RPC_METHOD)
                    .map(|(_, mv)| mv.as_str());
                assert_eq!(
                    method,
                    Some("do_get"),
                    "a {v} sub-phase sample must be tagged with the do_get method"
                );
            }
        }
    }
    assert!(
        saw_subphase,
        "at least one sub-phase sample must be present"
    );

    // The catalog contains no metric name resembling a new sub-phase metric.
    for name in catalog::ALL_METRICS {
        assert!(
            !name.contains("subphase") && !name.contains("sub_phase"),
            "no new sub-phase metric may be added to the catalog, found {name}"
        );
    }
    // No sub-phase value ever leaked onto the `cqlite.rpc.phase.active` gauge
    // (owner decision: sub-phases are duration-only).
    if let Some(active) = metrics.find(catalog::RPC_PHASE_ACTIVE) {
        for p in &active.points {
            for (k, v) in &p.attributes {
                if k == catalog::attr::RPC_PHASE {
                    assert!(
                        !SUBPHASES.contains(&v.as_str()),
                        "sub-phase value {v} must NOT appear on cqlite.rpc.phase.active"
                    );
                }
            }
        }
    }
}

/// Requirement 4: the sub-phase samples are emitted ONCE per RPC at teardown, not
/// once per row/batch — the sample count for each sub-phase is bounded by the
/// number of sub-phases, independent of the row/batch count. Proven via the
/// histogram sample COUNT: a full scan (many batches) records exactly ONE sample
/// per recorded sub-phase.
#[test]
#[serial]
fn subphase_samples_are_bounded_per_rpc_not_per_row() {
    let Some(found) = lz4_fixture_or_skip() else {
        return;
    };
    let mc = testing::metrics_capture();
    // batch_size = 1 over the full table → MANY batches/rows; if emission were
    // per-row/per-batch the sample count would scale with rows.
    let svc = CqliteFlightService::new(found.sstables_root.clone(), 1);
    let rt = tokio::runtime::Runtime::new().unwrap();

    mc.reset();
    rt.block_on(do_get_drain_all(&svc, lz4_ticket(None)));
    let metrics = mc.flush_and_collect();

    let subs = subphase_points(&metrics);
    assert!(
        !subs.is_empty(),
        "the many-batch scan must record sub-phase samples"
    );
    assert!(
        subs.len() <= SUBPHASES.len(),
        "at most one point per sub-phase value ({} <= {})",
        subs.len(),
        SUBPHASES.len()
    );
    for (phase, (_seconds, count)) in &subs {
        assert_eq!(
            *count, 1,
            "sub-phase {phase} must be emitted EXACTLY once per RPC (got {count} samples) — \
             bounded per RPC, never once per row/batch"
        );
    }
}
