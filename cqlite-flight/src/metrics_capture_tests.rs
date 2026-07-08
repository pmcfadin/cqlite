//! Issue #2162 — OTel-level assertions through the shared `observability-testing`
//! capture harness (`cqlite_core::observability::testing`).
//!
//! The feature-independent `StreamProbe` seam tests in `streaming_tests.rs` carry
//! the always-compiled wiring evidence (per-batch progress, incremental
//! rows_scanned flushes, remainder-on-early-exit). THIS module additionally
//! reads back the actual emitted OTel series to prove:
//!
//! * `cqlite.rpc.phase.duration` records a bounded `merge_setup` phase sample
//!   over a completed `do_get` (Stage 2),
//! * every `cqlite.rpc.phase` attribute value is one of the closed set and no
//!   phase sample carries an unbounded (ticket/key/query) attribute (Stage 2/4),
//! * `cqlite.rpc.rows` and `cqlite.query.rows_scanned` are emitted, carrying only
//!   their bounded attribute keys (Stage 1/3/4).
//!
//! All metric assertions live in ONE serial test because the production metric
//! helpers bind a single process-global `Meter`; the assertions use `>=` /
//! membership so they are robust to any other emitter in the process. Gated
//! behind `observability-testing`, so this is a distinct test binary from the
//! lib tests — the in-memory meter provider is installed before any metric is
//! recorded in THIS process.

use arrow_flight::flight_service_server::FlightService;
use arrow_flight::Ticket;
use futures::StreamExt;
use tonic::Request;

use cqlite_core::observability::{catalog, testing};

use crate::service::CqliteFlightService;
use crate::testutil::{build_sstables, simple_schema, write_row, KS, SIMPLE_DDL, TBL};
use crate::ticket::FlightTicket;

/// The bounded attribute keys any #2162 metric may carry — the invariant the
/// no-unbounded-attribute scenario asserts (Stage 4.1).
const BOUNDED_KEYS: &[&str] = &[
    catalog::attr::RPC_METHOD,
    catalog::attr::RPC_PHASE,
    catalog::attr::RPC_STATUS,
    catalog::attr::ACCESS_PATH,
    catalog::attr::SSTABLE_FORMAT,
];

fn ticket_bytes() -> Vec<u8> {
    let t = FlightTicket {
        keyspace: KS.into(),
        table: TBL.into(),
        ddl: SIMPLE_DDL.into(),
        ..Default::default()
    };
    t.to_bytes().unwrap()
}

/// Run a full `do_get` over a multi-row fixture and read back the emitted
/// metrics, asserting the #2162 incremental + phase series and the bounded-
/// attribute invariant.
#[test]
fn do_get_emits_bounded_phase_and_incremental_metrics() {
    // Install the in-memory meter provider BEFORE any metric is recorded in this
    // process (this is a dedicated test binary, so only this file's tests run).
    let mc = testing::metrics_capture();

    // A fixture large enough to exercise several batches.
    let schema = simple_schema();
    let rows = (1..=12)
        .map(|i| write_row(i, &format!("n{i}"), i * 10, 100))
        .collect::<Vec<_>>();
    let (_temp, data_dir, _dir) = build_sstables(&schema, vec![rows]);
    let svc = CqliteFlightService::new(data_dir, 4); // batch_size 4 → 3 batches

    mc.reset();
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let resp = svc
            .do_get(Request::new(Ticket::new(ticket_bytes())))
            .await
            .expect("do_get");
        // Drain the whole stream so the merge completes and every phase +
        // incremental counter is flushed (the merge task's PhaseTimer drops when
        // the channel closes, which the receiver observes as end-of-stream).
        let mut stream = resp.into_inner();
        let mut msgs = 0usize;
        while let Some(item) = stream.next().await {
            item.expect("stream item ok");
            msgs += 1;
        }
        assert!(msgs > 0, "do_get must yield at least the schema message");
    });

    let metrics = mc.flush_and_collect();

    // --- Stage 2: a bounded merge_setup phase sample is recorded ---------------
    let phase = metrics
        .find(catalog::RPC_PHASE_DURATION)
        .expect("cqlite.rpc.phase.duration must be recorded over a completed do_get");
    assert_eq!(
        metrics.unit(catalog::RPC_PHASE_DURATION),
        Some(catalog::unit::SECONDS)
    );
    let merge_setup_samples = phase
        .points
        .iter()
        .filter(|p| {
            p.attributes
                .iter()
                .any(|(k, v)| k == catalog::attr::RPC_PHASE && v == "merge_setup")
        })
        .count();
    assert!(
        merge_setup_samples >= 1,
        "a merge_setup-tagged phase sample must exist (the #2157 stall localizer)"
    );

    // Every phase value is one of the closed set; no ticket/key/query attribute.
    for p in &phase.points {
        let phase_val = p
            .attributes
            .iter()
            .find(|(k, _)| k == catalog::attr::RPC_PHASE)
            .map(|(_, v)| v.as_str());
        assert!(
            matches!(phase_val, Some("resolve" | "merge_setup" | "stream")),
            "phase value must be in the closed set, got {phase_val:?}"
        );
        assert_bounded_attrs(&p.attributes, catalog::RPC_PHASE_DURATION);
    }

    // --- Stage 1: rpc.rows emitted, bounded attrs -----------------------------
    assert!(
        metrics.counter_sum(catalog::RPC_ROWS) >= 12.0,
        "cqlite.rpc.rows must have accumulated the streamed rows, got {}",
        metrics.counter_sum(catalog::RPC_ROWS)
    );
    if let Some(rpc_rows) = metrics.find(catalog::RPC_ROWS) {
        for p in &rpc_rows.points {
            assert_bounded_attrs(&p.attributes, catalog::RPC_ROWS);
        }
    }

    // --- Stage 3: query.rows_scanned emitted, only the access_path attr --------
    let scanned = metrics
        .find(catalog::QUERY_ROWS_SCANNED)
        .expect("cqlite.query.rows_scanned must be emitted by the merge scan");
    assert!(
        metrics.counter_sum(catalog::QUERY_ROWS_SCANNED) >= 12.0,
        "rows_scanned must reflect the examined rows"
    );
    for p in &scanned.points {
        // Carries the bounded access_path (full_scan) and nothing unbounded.
        assert_bounded_attrs(&p.attributes, catalog::QUERY_ROWS_SCANNED);
        let ap = p
            .attributes
            .iter()
            .find(|(k, _)| k == catalog::attr::ACCESS_PATH)
            .map(|(_, v)| v.as_str());
        assert_eq!(
            ap,
            Some("full_scan"),
            "the Flight merge scan reports the bounded full_scan access path"
        );
    }
}

/// Assert every attribute key on a collected metric point is in the bounded set
/// (never a ticket, partition key, token, or query string).
fn assert_bounded_attrs(attrs: &[(String, String)], metric: &str) {
    for (k, _) in attrs {
        assert!(
            BOUNDED_KEYS.contains(&k.as_str()),
            "metric {metric} carries unbounded attribute key {k:?}"
        );
    }
}
