//! Issue #3742 — ROUTE OBSERVATION, from the EMITTED signal, for the
//! zero-output-column admission contract.
//!
//! `issue_3742_zero_column_projection.rs` asserts that a zero-output-column
//! request is REFUSED at spec admission (`InvalidArgument`, no message). This
//! file answers the complementary question with a signal the server actually
//! emits rather than a decision recomputed beside it: was any partition read at
//! all?
//!
//! Since the admission check landed, the answer is the point: a refused request
//! emits NO `access_path` label, because it is refused before a producer, a
//! route or a scan exists. Its ungated sibling can no longer establish the route
//! for the refused ticket at all — `ScanSpec::from_ticket` rejects it, so there
//! is no `ScanSpec` to hand `detect_route` — so the route is pinned here the
//! only way left: the SAME full-PK-equality filter with a SERVED projection
//! reports `streaming_partition_lookup`, and the zero-column variant of that
//! same filter reports nothing.
//!
//! The only route signal this crate publishes to an integration test is the
//! `access_path` attribute on `cqlite.query.rows_scanned`
//! (`cqlite-flight/src/scan_progress.rs` -> `catalog::attr::ACCESS_PATH`), which
//! needs the in-memory OTel capture — hence the module-level feature gate and
//! hence its OWN test binary (the capture's meter provider is process-global;
//! `point_read_metrics_test.rs` documents the same hazard).
//!
//! NOTE (#3375): a module-level `#![cfg(feature = "observability-testing")]`
//! target executes in NO gate component and in NO CI lane. Nothing here is
//! merge-gating; the contract itself is pinned by the ungated sibling.
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

/// Drive `do_get` to completion (success, up-front refusal, OR terminal stream
/// error) and return the distinct `access_path` labels the read emitted, plus
/// the terminal `Status` itself — the STATUS, not its text, so the CODE can be
/// asserted rather than pattern-matched out of a message.
fn drive_and_collect_access_paths(
    data_dir: &std::path::Path,
    ticket: Vec<u8>,
) -> (Vec<String>, Option<tonic::Status>) {
    let mc = testing::metrics_capture();
    mc.reset();
    let rt = tokio::runtime::Runtime::new().expect("rt");
    let err = rt.block_on(async {
        let svc = CqliteFlightService::new(data_dir.to_path_buf(), 8192);
        let resp = match svc.do_get(Request::new(Ticket::new(ticket))).await {
            Ok(r) => r,
            Err(status) => return Some(status),
        };
        let mut stream = resp.into_inner();
        let mut err = None;
        while let Some(item) = stream.next().await {
            if let Err(status) = item {
                err = Some(status);
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

/// CONTRACT (#3742), from the emitted signal: a zero-output-column request on
/// the point-read ticket shape is refused BEFORE any partition is read, so it
/// emits no `access_path` at all — while the same filter with a served
/// projection reports the POINT-READ path, which is what establishes that this
/// ticket shape is a point read and not a full scan.
#[test]
fn a_refused_zero_column_point_read_emits_no_access_path() {
    let (_temp, data_dir) = build_fixture();

    // Control: the same ticket WITHOUT a projection.
    let (control_labels, control_err) =
        drive_and_collect_access_paths(&data_dir, ticket_bytes(None));
    assert!(
        control_err.is_none(),
        "point-read control must complete without an error: {control_err:?}"
    );
    assert_eq!(
        control_labels,
        vec!["streaming_partition_lookup".to_string()],
        "the full-PK-equality control must report the point-read access path"
    );

    // The SAME filter with a SERVED one-column projection: still the point-read
    // path. This is what pins the route for the refused ticket below, whose only
    // difference is a projection the routing decision never reads.
    let (served_labels, served_err) =
        drive_and_collect_access_paths(&data_dir, ticket_bytes(Some(serde_json::json!(["key"]))));
    assert!(
        served_err.is_none(),
        "a served projection on the point-read filter must complete: {served_err:?}"
    );
    assert_eq!(
        served_labels,
        vec!["streaming_partition_lookup".to_string()],
        "columns=[key] on the same filter must still report the point-read path"
    );

    // The contract: the SAME filter with `"columns": []`.
    let (labels, err) =
        drive_and_collect_access_paths(&data_dir, ticket_bytes(Some(serde_json::json!([]))));
    let status = err.expect("a zero-column request must be REFUSED");
    assert_eq!(
        status.code(),
        tonic::Code::InvalidArgument,
        "the refusal is a client error, got {:?} ({status})",
        status.code()
    );
    assert!(
        labels.is_empty(),
        "a request refused at spec admission reads NO partition, so it emits no \
         access_path at all, got {labels:?}"
    );
}
