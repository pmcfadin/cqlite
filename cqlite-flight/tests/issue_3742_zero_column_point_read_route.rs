//! Issue #3742 — ROUTE OBSERVATION for the zero-column point-read measurement.
//!
//! `issue_3742_zero_column_projection.rs` records what a zero-column projection
//! DOES. This file answers the separate question roborev raised about it: was the
//! POINT-READ route actually selected for that request, or did it silently fall
//! back to a full scan (in which case the zero-column outcome recorded there is
//! not the point route's outcome at all)?
//!
//! The only route signal this crate publishes to an integration test is the
//! `access_path` attribute on `cqlite.query.rows_scanned`
//! (`cqlite-flight/src/scan_progress.rs` -> `catalog::attr::ACCESS_PATH`), which
//! needs the in-memory OTel capture — hence the module-level feature gate and
//! hence its OWN test binary (the capture's meter provider is process-global;
//! `point_read_metrics_test.rs` documents the same hazard).
//!
//! Its ungated sibling asserts the ROUTING DECISION instead, through the very
//! function the production path calls (`point_read::detect_route` over a
//! production-lowered `ScanSpec`). This file is the stronger, EMITTED-signal
//! half: the label is flushed by `ScanProgressMeter::drop`, so it is recorded
//! even for the request that then FAILS on arrow's zero-column refusal.
//!
//! ```text
//! cargo test -p cqlite-flight --features observability-testing \
//!   --test issue_3742_zero_column_point_read_route
//! ```

#![cfg(feature = "observability-testing")]

use arrow_flight::flight_service_server::FlightService;
use arrow_flight::Ticket;
use futures::StreamExt;
use tonic::Request;

use cqlite_core::observability::{catalog, testing};
use cqlite_core::storage::write_engine::{WriteEngine, WriteEngineConfig};
use cqlite_flight::service::CqliteFlightService;
use cqlite_flight::test_fixtures as fx;

fn build_fixture() -> (tempfile::TempDir, std::path::PathBuf) {
    let schema = fx::keyvalue_schema();
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");
    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, schema);
    let mut engine = WriteEngine::new(config).expect("engine");
    for m in fx::keyvalue_mutations() {
        engine.write(m).expect("write");
    }
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("rt");
    rt.block_on(engine.flush()).expect("flush").expect("info");
    (temp, data_dir)
}

/// A full-partition-key equality ticket, optionally with a `columns` projection.
fn ticket_bytes(columns: Option<serde_json::Value>) -> Vec<u8> {
    let mut t = serde_json::json!({
        "keyspace": fx::KEYVALUE_KS,
        "table": fx::KEYVALUE_TBL,
        "ddl": fx::KEYVALUE_DDL,
        "filter": {"type": "Compare", "column": "key", "op": "Equal", "value": "k1"},
    });
    if let Some(c) = columns {
        t["columns"] = c;
    }
    serde_json::to_vec(&t).expect("ticket json")
}

/// Drive `do_get` to completion (success OR terminal stream error) and return
/// the distinct `access_path` labels the read emitted, plus the error text.
fn drive_and_collect_access_paths(
    data_dir: &std::path::Path,
    ticket: Vec<u8>,
) -> (Vec<String>, Option<String>) {
    let mc = testing::metrics_capture();
    mc.reset();
    let rt = tokio::runtime::Runtime::new().expect("rt");
    let err = rt.block_on(async {
        let svc = CqliteFlightService::new(data_dir.to_path_buf(), 8192);
        let resp = match svc.do_get(Request::new(Ticket::new(ticket))).await {
            Ok(r) => r,
            Err(status) => return Some(status.to_string()),
        };
        let mut stream = resp.into_inner();
        let mut err = None;
        while let Some(item) = stream.next().await {
            if let Err(status) = item {
                err = Some(status.to_string());
                break;
            }
        }
        err
    });
    let metrics = mc.flush_and_collect();
    let mut labels: Vec<String> = Vec::new();
    if let Some(scanned) = metrics.find(catalog::QUERY_ROWS_SCANNED) {
        for p in &scanned.points {
            if let Some((_, v)) = p
                .attributes
                .iter()
                .find(|(k, _)| k == catalog::attr::ACCESS_PATH)
            {
                if !labels.iter().any(|l| l == v) {
                    labels.push(v.clone());
                }
            }
        }
    }
    (labels, err)
}

/// MEASUREMENT (#3742): the zero-column request on the point-read ticket shape
/// reports the POINT-READ access path — so the recorded zero-column outcome is
/// the point route's outcome, not a silent full-scan fallback.
#[test]
fn zero_column_point_read_request_reports_the_point_read_access_path() {
    let (_temp, data_dir) = build_fixture();

    // Control: the same ticket WITHOUT a projection.
    let (control_labels, control_err) = drive_and_collect_access_paths(&data_dir, ticket_bytes(None));
    eprintln!("MEASURE point-read control access_path={control_labels:?} err={control_err:?}");
    assert_eq!(
        control_err, None,
        "point-read control must complete without a stream error"
    );
    assert_eq!(
        control_labels,
        vec!["streaming_partition_lookup".to_string()],
        "the full-PK-equality control must report the point-read access path"
    );

    // The measurement: the SAME filter with `"columns": []`.
    let (labels, err) =
        drive_and_collect_access_paths(&data_dir, ticket_bytes(Some(serde_json::json!([]))));
    eprintln!("MEASURE point-read columns=[] access_path={labels:?} err={err:?}");
    let err = err.expect("zero-column point read must end in a terminal stream error");
    assert!(
        err.contains("must either specify a row count or at least one column"),
        "expected arrow's zero-column refusal, got: {err}"
    );
    assert_eq!(
        labels,
        vec!["streaming_partition_lookup".to_string()],
        "the zero-column request must be measured on the POINT-READ route \
         (never a silent full_scan fallback)"
    );
}
