//! Issue #3742 — MEASUREMENT: is a zero-column projection reachable, and what
//! happens when it is?
//!
//! This test asserts nothing about what CQLite *should* do. It records, from as
//! public a surface as this crate has (a `do_get` ticket, the JSON bytes a Trino
//! connector puts on the wire), what CQLite does TODAY for a projection that
//! resolves to zero columns.
//!
//! Three routes reach it (the third is measured in
//! `measure_zero_output_columns_via_an_empty_aggregation`, which needs no
//! `columns` field at all). Two ticket shapes reach it via `columns`, neither of
//! which is rejected anywhere on the way in:
//!   * `"columns": []`               — an explicitly empty projection list
//!   * `"columns": ["no_such_col"]`  — a projection naming only unknown columns
//!
//! `ScanSpec::from_ticket` copies `ticket.columns` verbatim
//! (`cqlite-flight/src/filter.rs:289`) and `MergeProducer::with_spec` narrows by
//! RETAIN (`cqlite-flight/src/producer.rs:456`), which cannot fail — so both
//! shapes produce an empty `columns` vec and the Arrow encode ends in
//! `RecordBatch::try_new(schema_with_no_fields, vec![])`.
//!
//! Route observation: each route measured here ASSERTS the route it names —
//! the scan/point-read split through the production routing decision
//! (`point_read::detect_route` over a production-lowered `ScanSpec`), and the
//! point route additionally through the EMITTED `access_path` label in the
//! `observability-testing`-gated sibling
//! `tests/issue_3742_zero_column_point_read_route.rs`.
//!
//! Contrast: the DataFusion `TableProvider` spike REFUSES to push an empty
//! projection (`cqlite-flight/src/df_spike/provider.rs:191-198`), narrowing to a
//! "count anchor" column instead — a guard that exists precisely because this
//! shape loses the row count.

use cqlite_core::schema::parse_cql_schema;
use cqlite_core::storage::write_engine::{WriteEngine, WriteEngineConfig};
use cqlite_flight::filter::ScanSpec;
use cqlite_flight::point_read::{detect_route, PointReadRoute};
use cqlite_flight::service::CqliteFlightService;
use cqlite_flight::test_fixtures as fx;
use cqlite_flight::ticket::FlightTicket;

use arrow::ipc::{root_as_message, MessageHeader};
use arrow::record_batch::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{FlightData, Ticket};
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

/// A ticket with an arbitrary extra JSON field set (point-read filter,
/// aggregation, ...).
fn ticket_with(extra: serde_json::Value) -> Vec<u8> {
    let mut t = serde_json::json!({
        "keyspace": fx::KEYVALUE_KS,
        "table": fx::KEYVALUE_TBL,
        "ddl": fx::KEYVALUE_DDL,
    });
    if let Some(map) = extra.as_object() {
        for (k, v) in map {
            t[k] = v.clone();
        }
    }
    serde_json::to_vec(&t).expect("ticket json")
}

/// Everything one `do_get` attempt produced, RETAINED rather than summarised
/// (roborev job 1, finding 2): the `FlightData` messages themselves and the
/// `tonic::Status` values, not their `to_string()`s — so a caller can assert the
/// message TYPE, the SCHEMA it carries and the status CODE, none of which a
/// `(bool, usize, Option<String>)` triple could distinguish.
struct DriveOutcome {
    /// `Err` = `do_get` itself refused; no stream was ever opened.
    rpc: Result<(), tonic::Status>,
    /// Every `FlightData` received, in order, before any terminal error.
    msgs: Vec<FlightData>,
    /// The terminal stream error, if the stream failed after being opened.
    stream_err: Option<tonic::Status>,
}

impl std::fmt::Debug for DriveOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "rpc_ok={} msgs={} terminal_err={:?}",
            self.rpc.is_ok(),
            self.msgs.len(),
            self.terminal_status().map(|s| s.to_string())
        )
    }
}

impl DriveOutcome {
    /// The terminal error, whichever side it came from (the aggregation route
    /// fails the RPC itself; the streaming routes fail the opened stream).
    fn terminal_status(&self) -> Option<&tonic::Status> {
        self.rpc.as_ref().err().or(self.stream_err.as_ref())
    }
}

/// Drive `do_get` in process, retaining every message and status.
fn drive(data_dir: &std::path::Path, ticket: Vec<u8>) -> DriveOutcome {
    let rt = tokio::runtime::Runtime::new().expect("rt");
    rt.block_on(async {
        let svc = CqliteFlightService::new(data_dir.to_path_buf(), 8192);
        let resp = match svc.do_get(Request::new(Ticket::new(ticket))).await {
            Ok(r) => r,
            Err(status) => {
                return DriveOutcome {
                    rpc: Err(status),
                    msgs: Vec::new(),
                    stream_err: None,
                }
            }
        };
        let mut stream = resp.into_inner();
        let mut msgs = Vec::new();
        let mut stream_err = None;
        while let Some(item) = stream.next().await {
            match item {
                Ok(fd) => msgs.push(fd),
                Err(status) => {
                    stream_err = Some(status);
                    break;
                }
            }
        }
        DriveOutcome {
            rpc: Ok(()),
            msgs,
            stream_err,
        }
    })
}

/// MEASUREMENT (#3742): a `do_get` ticket whose projection resolves to zero
/// columns is accepted all the way in, and what comes back is recorded here.
#[test]
fn measure_zero_column_projection_over_do_get() {
    let (_temp, data_dir) = build_fixture();

    // Control: no projection at all — the 3 fixture rows must round-trip.
    let control = drive(&data_dir, ticket_bytes(None));
    eprintln!("MEASURE control (no projection): {control:?}");
    assert!(
        control.terminal_status().is_none(),
        "control must succeed: {:?}",
        control.terminal_status().map(|s| s.to_string())
    );

    // (a) explicitly empty projection list
    let a = drive(&data_dir, ticket_bytes(Some(serde_json::json!([]))));
    eprintln!("MEASURE columns=[]: {a:?}");
    assert_zero_column_outcome("columns=[]", &a);

    // (b) projection naming only a column the table does not have. `retain`
    // cannot fail, so an unknown name is not a bad-request — it silently empties
    // the projection and lands in exactly the same place as (a).
    let b = drive(
        &data_dir,
        ticket_bytes(Some(serde_json::json!(["no_such_col"]))),
    );
    eprintln!("MEASURE columns=[no_such_col]: {b:?}");
    assert_zero_column_outcome("columns=[no_such_col]", &b);

    // (c) a real column, as a sanity control that projection works at all.
    let c = drive(&data_dir, ticket_bytes(Some(serde_json::json!(["key"]))));
    eprintln!("MEASURE columns=[key]: {c:?}");
    assert!(
        c.terminal_status().is_none(),
        "single-column control: {:?}",
        c.terminal_status().map(|s| s.to_string())
    );
}

/// The measured outcome of a zero-column projection on the streaming routes:
/// the RPC is ACCEPTED, the Arrow schema message (zero fields) is sent, and the
/// stream then FAILS with arrow's refusal wrapped as `Status::Internal`.
///
/// This is recorded, not endorsed. What it establishes for issue #3742 is that
/// the shape is REACHABLE and that it fails LOUDLY — it is not a silent
/// 0-rows-when-present answer on this path.
fn assert_zero_column_outcome(what: &str, outcome: &DriveOutcome) {
    if let Err(status) = &outcome.rpc {
        panic!("{what}: do_get itself must be accepted (no up-front rejection), got {status}");
    }
    assert_eq!(
        outcome.msgs.len(),
        1,
        "{what}: exactly one message arrives before the stream fails"
    );

    // The ONE message must really be a zero-field SCHEMA message (roborev job 1,
    // finding 2): a different message type, or a schema carrying fields, would
    // have satisfied a bare count. Both facts are read off the wire bytes —
    // the IPC `Message` header type, then the decoded `Schema` itself.
    let fd = &outcome.msgs[0];
    let message =
        root_as_message(&fd.data_header).expect("data_header must be a valid IPC Message");
    assert_eq!(
        message.header_type(),
        MessageHeader::Schema,
        "{what}: message[0] must be the SCHEMA message (never a record batch)"
    );
    // Decode from the flatbuffer header directly: `data_header` is the bare IPC
    // `Message`, NOT a length-prefixed IPC stream frame, so the stream-buffer
    // helper cannot read it.
    let ipc_schema = message
        .header_as_schema()
        .expect("a Schema-typed message must carry a Schema header");
    let schema = arrow::ipc::convert::fb_to_schema(ipc_schema);
    assert!(
        schema.fields().is_empty(),
        "{what}: the schema message must carry ZERO fields, got {:?}",
        schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
    );

    // The stream then fails. The STATUS is retained, so the code is asserted as
    // `Internal` rather than pattern-matched out of a stringified message.
    let status = outcome
        .stream_err
        .as_ref()
        .unwrap_or_else(|| panic!("{what}: expected a terminal stream error"));
    assert_eq!(
        status.code(),
        tonic::Code::Internal,
        "{what}: arrow's refusal must surface as Status::Internal, got {:?}",
        status.code()
    );
    assert!(
        status
            .message()
            .contains("must either specify a row count or at least one column"),
        "{what}: unexpected terminal error: {status}"
    );
}

/// Drive `do_get` in process and DECODE the response the way an arrow client
/// would, returning the `RecordBatch`es or the first error's text. Used for the
/// aggregation CONTROL, where the point is not just "a message arrived" but that
/// the route really aggregates.
// The `FlightError` the decoder stream requires as its item `Err` type has a
// framework-fixed large size; boxing it (clippy's suggestion) would violate the
// `FlightRecordBatchStream` item contract (#2856).
#[allow(clippy::result_large_err)]
fn drive_batches(data_dir: &std::path::Path, ticket: Vec<u8>) -> Result<Vec<RecordBatch>, String> {
    let rt = tokio::runtime::Runtime::new().expect("rt");
    rt.block_on(async {
        let svc = CqliteFlightService::new(data_dir.to_path_buf(), 8192);
        let resp = svc
            .do_get(Request::new(Ticket::new(ticket)))
            .await
            .map_err(|s| s.to_string())?;
        let stream = resp.into_inner().map(|r| r.map_err(FlightError::Tonic));
        let mut rb = FlightRecordBatchStream::new_from_flight_data(stream);
        let mut out = Vec::new();
        while let Some(item) = rb.next().await {
            out.push(item.map_err(|e| e.to_string())?);
        }
        Ok(out)
    })
}

/// Assert that the given ticket bytes SELECT the partition point-read route.
///
/// This runs the production routing decision on production inputs: the ticket is
/// deserialized as the service deserializes it, its DDL parsed by the same
/// `parse_cql_schema` the service calls (`service.rs:425`), lowered by the same
/// `ScanSpec::from_ticket`, and handed to `point_read::detect_route` — the ONE
/// function `MergeProducer::produce_streaming` consults to pick the route
/// (`producer.rs:784` -> `producer_point.rs:87`), which it takes unconditionally
/// whenever the decision is not `Scan`.
///
/// What this establishes: the zero-column ticket shape does NOT route to the
/// full-scan path, so the outcome recorded below is the point route's outcome.
/// What it does NOT establish, being a decision rather than an emitted signal:
/// that this particular request reached `produce_streaming` at all. That half is
/// covered by the EMITTED `access_path` label
/// (`tests/issue_3742_zero_column_point_read_route.rs`), which needs the
/// in-memory OTel capture and is therefore gated on `observability-testing`;
/// measured there, the zero-column request reports `streaming_partition_lookup`.
fn assert_ticket_routes_to_point_read(what: &str, ticket: &[u8]) {
    let parsed: FlightTicket = serde_json::from_slice(ticket).expect("ticket deserializes");
    let schema = parse_cql_schema(&parsed.ddl).expect("ticket ddl parses");
    let spec = ScanSpec::from_ticket(&parsed, &schema).expect("ticket lowers to a scan spec");
    let route = detect_route(spec.filter.as_ref(), &schema);
    assert!(
        matches!(route, PointReadRoute::PartitionPointRead(_)),
        "{what}: must select the partition point-read route, got {route:?}"
    );
}

/// MEASUREMENT (#3742): the POINT-READ route (a full-partition-key equality
/// filter) shares `MergeProducer::output_columns()`, so an empty projection
/// reaches it too.
///
/// The route is ASSERTED, not assumed (roborev job 1, finding 1): without it the
/// zero-column assertions below would pass identically had the request quietly
/// fallen back to a full scan, and the test would not measure what it names.
#[test]
fn measure_zero_column_projection_on_the_point_read_route() {
    let (_temp, data_dir) = build_fixture();
    let filter =
        serde_json::json!({"type": "Compare", "column": "key", "op": "Equal", "value": "k1"});

    // Control: the same point-read ticket with no projection. Asserted (not
    // printed) so the zero-column failure can never be mistaken for a point
    // route that is simply broken on this fixture: it must route to the point
    // path AND return exactly the one target partition's row.
    let control = ticket_with(serde_json::json!({"filter": filter.clone()}));
    assert_ticket_routes_to_point_read("point-read control", &control);
    let batches = drive_batches(&data_dir, control).expect("point-read control must succeed");
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        rows, 1,
        "point-read control: the `key = 'k1'` partition is exactly one row"
    );

    let zero = ticket_with(serde_json::json!({"filter": filter, "columns": []}));
    assert_ticket_routes_to_point_read("point-read columns=[]", &zero);
    let outcome = drive(&data_dir, zero);
    eprintln!("MEASURE point-read columns=[]: {outcome:?}");
    assert_zero_column_outcome("point-read columns=[]", &outcome);
}

/// MEASUREMENT (#3742): the AGGREGATION route builds its output columns from
/// `group_by + aggregates` (`agg.rs:239`). A ticket carrying an EMPTY
/// aggregates list with no group_by therefore yields zero output columns by a
/// route that has nothing to do with `columns`.
///
/// # MEASURED VERDICT — same arrow refusal, but EARLIER, and NO guard fires
///
/// Nothing rejects an empty aggregation spec on the way in. The route reaches
/// the SAME `RecordBatch::try_new` refusal as the scan/point-read routes, at
/// `rows_to_record_batch(columns, group)` over zero output columns
/// (`cqlite-flight/src/producer.rs:963`) — a global aggregation always emits one
/// partial row (`producer.rs:946-948`), so there IS a row and no column to put
/// it in.
///
/// It surfaces differently from routes 1-3, and that difference is the point of
/// this test: the aggregate route materializes its bounded per-group output
/// EAGERLY inside `do_get` (`build_aggregate_response` ->
/// `producer.produce_from_resolved`, `cqlite-flight/src/streaming.rs:623-649`,
/// dispatched at `service.rs:895`), so the failure aborts the RPC ITSELF —
/// `Status::Internal`, and NOT ONE message reaches the client, not even the
/// zero-field schema. The streaming routes instead send the zero-field schema
/// message and then fail the stream. Hence this test cannot reuse
/// `assert_zero_column_outcome`, which pins `msgs == 1`.
///
/// Recorded, not endorsed, and presupposing no fix posture.
#[test]
fn measure_zero_output_columns_via_an_empty_aggregation() {
    let (_temp, data_dir) = build_fixture();

    // Control: a real global count(*), DECODED — one batch, one row, one Int64
    // column carrying the 3 fixture rows. Asserted (not printed) so the
    // empty-aggregation failure below can never be mistaken for an aggregation
    // route that is simply broken on this fixture.
    let batches = drive_batches(
        &data_dir,
        ticket_with(serde_json::json!({
            "aggregation": {"group_by": [], "aggregates": [
                {"func": "Count", "column": null, "output": "c"}
            ]}
        })),
    )
    .expect("agg control count(*) must succeed");
    assert_eq!(batches.len(), 1, "agg control: one bounded aggregate batch");
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1, "agg control: one global aggregate row");
    assert_eq!(
        batch.schema().field(0).name(),
        "c",
        "agg control: the aggregate's declared output name"
    );
    let counts = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("agg control: count(*) is Int64");
    assert_eq!(
        counts.value(0),
        3,
        "agg control: count(*) over the 3-row keyvalue fixture"
    );

    // The measurement: an aggregation with NO group_by and NO aggregates.
    let outcome = drive(
        &data_dir,
        ticket_with(serde_json::json!({
            "aggregation": {"group_by": [], "aggregates": []}
        })),
    );
    eprintln!("MEASURE agg empty (no group_by, no aggregates): {outcome:?}");
    let rpc_status = outcome.rpc.as_ref().expect_err(
        "agg empty: the eager aggregate materialization must fail the do_get RPC \
         itself — no guard rejects the spec, and no stream is opened",
    );
    assert!(
        outcome.msgs.is_empty(),
        "agg empty: not one message reaches the client, not even the zero-field \
         schema (contrast the streaming routes, which send it first)"
    );
    assert_eq!(
        rpc_status.code(),
        tonic::Code::Internal,
        "agg empty: arrow's refusal surfaces as Status::Internal, got {:?}",
        rpc_status.code()
    );
    assert!(
        rpc_status
            .message()
            .contains("must either specify a row count or at least one column"),
        "agg empty: expected arrow's zero-column refusal, got: {rpc_status}"
    );
}
