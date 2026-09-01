//! Issue #3742 — MEASUREMENT: is a zero-column projection reachable, and what
//! happens when it is?
//!
//! This test asserts nothing about what CQLite *should* do. It records, from as
//! public a surface as this crate has (a `do_get` ticket, the JSON bytes a Trino
//! connector puts on the wire), what CQLite does TODAY for a projection that
//! resolves to zero columns.
//!
//! Two ticket shapes reach it, neither of which is rejected anywhere on the way
//! in:
//!   * `"columns": []`               — an explicitly empty projection list
//!   * `"columns": ["no_such_col"]`  — a projection naming only unknown columns
//!
//! `ScanSpec::from_ticket` copies `ticket.columns` verbatim
//! (`cqlite-flight/src/filter.rs:289`) and `MergeProducer::with_spec` narrows by
//! RETAIN (`cqlite-flight/src/producer.rs:456`), which cannot fail — so both
//! shapes produce an empty `columns` vec and the Arrow encode ends in
//! `RecordBatch::try_new(schema_with_no_fields, vec![])`.
//!
//! Contrast: the DataFusion `TableProvider` spike REFUSES to push an empty
//! projection (`cqlite-flight/src/df_spike/provider.rs:191-198`), narrowing to a
//! "count anchor" column instead — a guard that exists precisely because this
//! shape loses the row count.

use cqlite_core::storage::write_engine::{WriteEngine, WriteEngineConfig};
use cqlite_flight::service::CqliteFlightService;
use cqlite_flight::test_fixtures as fx;

use arrow_flight::flight_service_server::FlightService;
use arrow_flight::Ticket;
use futures::StreamExt;
use tonic::Request;

/// The same 3-row `keyvalue` fixture the transport tests use.
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

fn ticket_bytes(columns: Option<serde_json::Value>) -> Vec<u8> {
    let mut t = serde_json::json!({
        "keyspace": fx::KEYVALUE_KS,
        "table": fx::KEYVALUE_TBL,
        "ddl": fx::KEYVALUE_DDL,
    });
    if let Some(c) = columns {
        t["columns"] = c;
    }
    serde_json::to_vec(&t).expect("ticket json")
}

/// Drive `do_get` in process and report `(rpc_ok, message_count, first_error)`.
fn drive(data_dir: &std::path::Path, ticket: Vec<u8>) -> (bool, usize, Option<String>) {
    let rt = tokio::runtime::Runtime::new().expect("rt");
    rt.block_on(async {
        let svc = CqliteFlightService::new(data_dir.to_path_buf(), 8192);
        let resp = match svc.do_get(Request::new(Ticket::new(ticket))).await {
            Ok(r) => r,
            Err(status) => return (false, 0, Some(status.to_string())),
        };
        let mut stream = resp.into_inner();
        let mut msgs = 0usize;
        let mut err = None;
        while let Some(item) = stream.next().await {
            match item {
                Ok(_) => msgs += 1,
                Err(status) => {
                    err = Some(status.to_string());
                    break;
                }
            }
        }
        (true, msgs, err)
    })
}

/// MEASUREMENT (#3742): a `do_get` ticket whose projection resolves to zero
/// columns is accepted all the way in, and what comes back is recorded here.
#[test]
fn measure_zero_column_projection_over_do_get() {
    let (_temp, data_dir) = build_fixture();

    // Control: no projection at all — the 3 fixture rows must round-trip.
    let (ok, msgs, err) = drive(&data_dir, ticket_bytes(None));
    eprintln!("MEASURE control (no projection): rpc_ok={ok} msgs={msgs} err={err:?}");
    assert!(ok && err.is_none(), "control must succeed: {err:?}");

    // (a) explicitly empty projection list
    let (ok_a, msgs_a, err_a) = drive(&data_dir, ticket_bytes(Some(serde_json::json!([]))));
    eprintln!("MEASURE columns=[]: rpc_ok={ok_a} msgs={msgs_a} err={err_a:?}");

    // (b) projection naming only a column the table does not have
    let (ok_b, msgs_b, err_b) = drive(
        &data_dir,
        ticket_bytes(Some(serde_json::json!(["no_such_col"]))),
    );
    eprintln!("MEASURE columns=[no_such_col]: rpc_ok={ok_b} msgs={msgs_b} err={err_b:?}");

    // (c) a real column, as a sanity control that projection works at all.
    let (ok_c, msgs_c, err_c) = drive(&data_dir, ticket_bytes(Some(serde_json::json!(["key"]))));
    eprintln!("MEASURE columns=[key]: rpc_ok={ok_c} msgs={msgs_c} err={err_c:?}");
}
