//! Issue #3096 — the IPC-FRAMING sub-phase closes an attribution blind spot.
//!
//! # The blind spot
//!
//! `StreamSubPhase::Encode` (`egress_flush.rs`) wraps ONLY `flush_buffer` — the
//! Arrow ARRAY BUILD, on the merge-consumer thread. The arrow-flight encoder
//! stage built in `streaming.rs::encode_do_get` runs LATER and on a DIFFERENT
//! thread (the async gRPC task), and is where the IPC serialization, the
//! `DictionaryHandling::Hydrate` per-batch rebuild, and the re-slicing of any
//! batch larger than `GRPC_TARGET_MAX_FLIGHT_SIZE_BYTES` happen.
//!
//! None of that was inside ANY sub-phase. So the two levers this change ranks
//! against that stage — aligning the batch cap with the encoder's target, and
//! stopping the per-batch dictionary rebuild — could not be attributed from
//! in-process timings at all: the only bucket that could plausibly have moved
//! (`stream_encode`) does not span the code they change. "Lever 4 helped" was
//! literally unfalsifiable.
//!
//! # What this asserts
//!
//! 1. A streaming `do_get` records a `stream_encode_framing` sample.
//! 2. It is a sample DISTINCT from `stream_encode` — both are present, so the
//!    array build and the framing are separately readable rather than merged.
//! 3. Exactly ONE sample per RPC (teardown emission, never per batch/row), so the
//!    new bucket cannot become a cardinality or emission-rate regression.
//! 4. No new metric NAME and no new attribute KEY: the sample rides the existing
//!    `cqlite.rpc.phase.duration` histogram on the existing `cqlite.rpc.phase`
//!    attribute, exactly as the five #2819 values do.
//!
//! The fixture is the small `ws0.events` corpus built through the SAME
//! `ws0_corpus_gen::generate::generate` the measurement corpus uses, so no
//! fetched dataset is needed and the test never skips. SCOPE: that corpus is
//! CQLite-written + CQLite-read, a PERFORMANCE FIXTURE ONLY (#3042) — nothing
//! here is an on-disk framing correctness claim.

#![cfg(feature = "observability-testing")]

use std::collections::HashMap;

use arrow_flight::flight_service_server::FlightService;
use arrow_flight::Ticket;
use futures::StreamExt;
use serial_test::serial;
use tonic::Request;

use cqlite_core::observability::{catalog, testing};
use cqlite_flight::obs::{
    PHASE_STREAM_ENCODE, PHASE_STREAM_ENCODE_FRAMING, PHASE_STREAM_GRPC_WRITE, PHASE_STREAM_MERGE,
};
use cqlite_flight::service::CqliteFlightService;
use ws0_corpus_gen::generate::{generate, CorpusSpec};
use ws0_corpus_gen::schema::{DDL, KEYSPACE, TABLE};

/// Rows in the fixture. Small, but spanning several batches at the `batch_size`
/// below so the framing stage is polled repeatedly rather than exactly once.
const FIXTURE_ROWS: u64 = 500;
/// Deliberately BELOW the production default (8192): the committed assertion
/// "exactly one sample per RPC, never one per batch" is only meaningful over a
/// fixture that spans MANY batches.
const BATCH_SIZE: usize = 64;

/// Optional: point the test at the generated measurement corpus instead of
/// building a fixture, so the framing-vs-array-build split can be READ at corpus
/// scale during a perf run. Unset = build the small fixture (the CI path).
const CORPUS_DIR_ENV: &str = "CQLITE_WS0_CORPUS_DIR";
/// Optional: override the service `batch_size`, so the same split can be read at
/// the PRODUCTION default (8192) rather than only at the small committed value.
const BATCH_SIZE_ENV: &str = "CQLITE_WS0_BATCH_SIZE";

/// `(phase value -> [(seconds, sample_count)])` for every `stream_*` sub-phase
/// point on `cqlite.rpc.phase.duration`.
///
/// Deliberately filtered on the `stream_` PREFIX rather than a hardcoded list, so
/// a sub-phase this test does not know about still shows up here — the "exactly
/// one sample per value" assertion below then covers it too.
fn subphase_points(m: &testing::CapturedMetrics) -> HashMap<String, Vec<(f64, u64)>> {
    let mut out: HashMap<String, Vec<(f64, u64)>> = HashMap::new();
    if let Some(entry) = m.find(catalog::RPC_PHASE_DURATION) {
        for p in &entry.points {
            for (k, v) in &p.attributes {
                if k == catalog::attr::RPC_PHASE && v.starts_with("stream_") {
                    let count = p
                        .count
                        .expect("phase.duration is a histogram → sample count present");
                    out.entry(v.clone()).or_default().push((p.value, count));
                }
            }
        }
    }
    out
}

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

fn ticket_bytes() -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "keyspace": KEYSPACE, "table": TABLE, "ddl": DDL
    }))
    .expect("ticket json")
}

#[test]
#[serial]
fn do_get_attributes_ipc_framing_separately_from_the_array_build() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let temp = tempfile::tempdir().expect("tempdir");
    let (corpus_root, rows) = match std::env::var(CORPUS_DIR_ENV) {
        Ok(dir) => {
            let root = std::path::PathBuf::from(&dir);
            let identity: serde_json::Value = serde_json::from_str(
                &std::fs::read_to_string(root.join("corpus-identity.json")).unwrap_or_else(|e| {
                    panic!("{CORPUS_DIR_ENV}={dir} but its corpus-identity.json is unreadable: {e}")
                }),
            )
            .expect("corpus-identity.json");
            let rows = identity["rows"].as_u64().expect("recorded row count");
            assert!(rows > 0, "the measurement corpus must hold rows");
            (root, rows)
        }
        Err(_) => {
            let spec = CorpusSpec::small(temp.path().to_path_buf(), FIXTURE_ROWS);
            let identity = rt.block_on(generate(&spec)).expect("generate fixture");
            assert_eq!(
                identity.rows, FIXTURE_ROWS,
                "the fixture must be non-vacuous"
            );
            (temp.path().to_path_buf(), identity.rows)
        }
    };
    let batch_size: usize = std::env::var(BATCH_SIZE_ENV)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(BATCH_SIZE);

    let mc = testing::metrics_capture();
    let svc = CqliteFlightService::new(corpus_root, batch_size);
    mc.reset();
    rt.block_on(do_get_drain_all(&svc, ticket_bytes()));
    let metrics = mc.flush_and_collect();
    let subs = subphase_points(&metrics);

    // (1) + (2): the framing bucket exists AND the array-build bucket still does.
    // Asserting both is the point — a change that merely RENAMED `stream_encode`
    // would satisfy (1) alone while leaving the two costs indistinguishable.
    for phase in [
        PHASE_STREAM_ENCODE,
        PHASE_STREAM_ENCODE_FRAMING,
        PHASE_STREAM_MERGE,
        PHASE_STREAM_GRPC_WRITE,
    ] {
        assert!(
            subs.contains_key(phase),
            "a streaming do_get must record {phase}; got {:?}",
            subs.keys().collect::<Vec<_>>()
        );
    }
    assert_ne!(
        PHASE_STREAM_ENCODE, PHASE_STREAM_ENCODE_FRAMING,
        "the framing bucket must be a DISTINCT phase value from the array build"
    );
    let framing = &subs[PHASE_STREAM_ENCODE_FRAMING];
    assert!(
        framing[0].0 > 0.0,
        "stream_encode_framing must record POSITIVE wall time — a zero would mean \
         the encoder stage was never polled inside the timed region"
    );

    // (3) One sample per sub-phase per RPC: emitted at teardown, never per batch.
    // The fixture spans several batches, so a per-batch emission would show here.
    for (phase, points) in &subs {
        assert_eq!(
            points.len(),
            1,
            "sub-phase {phase} must record exactly ONE point per RPC, got {points:?}"
        );
        assert_eq!(
            points[0].1, 1,
            "sub-phase {phase} must be emitted EXACTLY once per RPC (got {} samples) \
             — a per-batch emission would scale with the batch count",
            points[0].1
        );
    }

    // (4) No new metric name, and no new attribute key.
    for name in catalog::ALL_METRICS {
        assert!(
            !name.contains("framing"),
            "the framing sub-phase must ride the existing phase.duration histogram, \
             never a new metric — found {name}"
        );
    }
    let entry = metrics
        .find(catalog::RPC_PHASE_DURATION)
        .expect("phase.duration must carry the sub-phase samples");
    for p in &entry.points {
        for (k, _) in &p.attributes {
            assert!(
                k == catalog::attr::RPC_METHOD || k == catalog::attr::RPC_PHASE,
                "phase.duration carries an unexpected attribute key {k:?}"
            );
        }
    }
    // …and the framing value is tagged with the `do_get` method, like its peers.
    let framing_method = entry
        .points
        .iter()
        .find(|p| {
            p.attributes
                .iter()
                .any(|(k, v)| k == catalog::attr::RPC_PHASE && v == PHASE_STREAM_ENCODE_FRAMING)
        })
        .and_then(|p| {
            p.attributes
                .iter()
                .find(|(k, _)| k == catalog::attr::RPC_METHOD)
                .map(|(_, v)| v.clone())
        });
    assert_eq!(
        framing_method.as_deref(),
        Some("do_get"),
        "the framing sample must be tagged with the do_get method"
    );

    // The framing sample must never leak onto the phase.ACTIVE gauge (the #2819
    // owner decision: sub-phases are duration-only).
    if let Some(active) = metrics.find(catalog::RPC_PHASE_ACTIVE) {
        for p in &active.points {
            for (k, v) in &p.attributes {
                assert!(
                    !(k == catalog::attr::RPC_PHASE && v == PHASE_STREAM_ENCODE_FRAMING),
                    "stream_encode_framing must NOT appear on cqlite.rpc.phase.active"
                );
            }
        }
    }

    let build_s = subs[PHASE_STREAM_ENCODE][0].0;
    let framing_s = framing[0].0;
    eprintln!(
        "framing attribution over {rows} rows at batch_size {batch_size}:\n  \
         stream_encode (array build) = {build_s:.6}s = {:.1} ns/row\n  \
         stream_encode_framing (IPC) = {framing_s:.6}s = {:.1} ns/row\n  \
         framing share of encode+framing = {:.1}%",
        build_s / rows as f64 * 1e9,
        framing_s / rows as f64 * 1e9,
        framing_s / (build_s + framing_s) * 100.0
    );
}
