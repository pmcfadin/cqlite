//! Issue #3742 — CONTRACT: a `do_get` request whose OUTPUT COLUMN SET is empty
//! is REFUSED AT SPEC ADMISSION with `InvalidArgument`, before any Arrow schema
//! message is produced.
//!
//! Four routes reach a zero-output-column request from the wire (the JSON bytes
//! a Trino connector puts on a `do_get` ticket), and all four are refused by the
//! ONE admission check `admit_output_columns`, called from
//! `ScanSpec::from_ticket` (`cqlite-flight/src/filter.rs`) — the function every
//! route passes through via `CqliteFlightService::build_producer`
//! (`cqlite-flight/src/service.rs:481`):
//!   * `"columns": []`               — an explicitly empty projection list
//!   * `"columns": ["no_such_col"]`  — a projection naming only unknown columns
//!   * the point-read route (a full-PK equality filter) with either shape
//!   * `"aggregation": {"group_by": [], "aggregates": []}` — no output columns
//!     from a route that has nothing to do with `columns`
//!
//! # What this REPLACED, and why the codes moved
//!
//! Before the admission check, `ScanSpec::from_ticket` copied `ticket.columns`
//! verbatim and `MergeProducer::with_spec` narrowed by RETAIN, which cannot
//! fail — so every shape above produced an empty column vec and reached
//! `RecordBatch::try_new(<zero-field schema>, vec![])`, whose `Err` surfaced as
//! `Status::Internal` ("must either specify a row count or at least one
//! column"). The streaming routes had already sent the zero-field SCHEMA message
//! by then and failed the stream mid-flight; the aggregation route materializes
//! eagerly inside `do_get` and aborted the RPC with zero messages. A client
//! error was reported as a server fault, in two different places.
//!
//! Both are gone BY CONSTRUCTION, which is what these tests pin: the state never
//! reaches arrow, so there is no internal error left to mis-label, and no
//! message — not even a schema — reaches the client. (Arrow still refuses a
//! zero-column batch; that arm is pinned at its own level in
//! `cqlite-core/src/export/arrow_row_accumulator_tests.rs`.)
//!
//! # The predicate is ZERO TOTAL OUTPUT COLUMNS
//!
//! Never "the `aggregates` list is empty". `SELECT DISTINCT c` reaches Trino's
//! `applyAggregation` with `groupingKeys=[c]`/`aggregations={}` and the
//! connector emits `{"group_by": ["c"], "aggregates": []}`
//! (`CqliteFlightMetadata.java:569`) — a live shape with ONE output column.
//! `a_distinct_shaped_aggregation_is_served` pins that it is SERVED, so the
//! narrower predicate cannot be reintroduced.
//!
//! Route observation: each route ASSERTS the route it names — the scan/point-read
//! split through the production routing decision (`point_read::detect_route` over
//! a production-lowered `ScanSpec`), and the point route additionally through the
//! EMITTED `access_path` label in the `observability-testing`-gated sibling
//! `tests/issue_3742_zero_column_point_read_route.rs`.
//!
//! Contrast: the DataFusion `TableProvider` spike REFUSES to push an empty
//! projection (`cqlite-flight/src/df_spike/provider.rs:191-198`), narrowing to a
//! "count anchor" column instead — the same refusal, one layer up.

use cqlite_core::schema::parse_cql_schema;
use cqlite_core::storage::write_engine::{WriteEngine, WriteEngineConfig};
use cqlite_flight::filter::ScanSpec;
use cqlite_flight::point_read::{detect_route, PointReadRoute};
use cqlite_flight::service::CqliteFlightService;
use cqlite_flight::test_fixtures as fx;
use cqlite_flight::ticket::FlightTicket;

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

/// CONTRACT (#3742): a `do_get` ticket whose projection resolves to zero
/// columns is REFUSED by the RPC itself — `InvalidArgument`, no stream, no
/// message.
#[test]
fn a_zero_column_projection_is_refused_up_front() {
    let (_temp, data_dir) = build_fixture();

    // Control: no projection at all — the 3 fixture rows must round-trip.
    let control = drive(&data_dir, ticket_bytes(None));
    assert!(
        control.terminal_status().is_none(),
        "control must succeed: {:?}",
        control.terminal_status().map(|s| s.to_string())
    );

    // (a) explicitly empty projection list.
    let a = drive(&data_dir, ticket_bytes(Some(serde_json::json!([]))));
    assert_refused_up_front("columns=[]", &a, &["projection selects no columns"]);

    // (b) a projection naming only a column the table does not have. This used
    // to be indistinguishable from (a) — `retain` cannot fail, so the unknown
    // name silently emptied the projection. It is now its own refusal, and the
    // error NAMES the offending column.
    let b = drive(
        &data_dir,
        ticket_bytes(Some(serde_json::json!(["no_such_col"]))),
    );
    assert_refused_up_front(
        "columns=[no_such_col]",
        &b,
        &["projection selects no columns", "'no_such_col'"],
    );

    // (c) a real column, as a control that projection still works at all.
    let c = drive(&data_dir, ticket_bytes(Some(serde_json::json!(["key"]))));
    assert!(
        c.terminal_status().is_none(),
        "single-column control: {:?}",
        c.terminal_status().map(|s| s.to_string())
    );

    // (d) a projection mixing a real and an unknown column still RESOLVES to one
    // output column, so it is served exactly as before: the admission rejects a
    // projection that resolves to NOTHING, never merely one carrying an unknown
    // name.
    let d = drive(
        &data_dir,
        ticket_bytes(Some(serde_json::json!(["key", "no_such_col"]))),
    );
    assert!(
        d.terminal_status().is_none(),
        "mixed known/unknown projection must still be served: {:?}",
        d.terminal_status().map(|s| s.to_string())
    );
}

/// The contract for every zero-output-column route: `do_get` REFUSES the request
/// itself with `Code::InvalidArgument`, and NOT ONE `FlightData` reaches the
/// client — not even the zero-field schema message the streaming routes used to
/// send before failing mid-stream.
///
/// `expect_in_message` are substrings the refusal must name (the reason, and for
/// an unknown-column projection the offending column), asserted so a refusal for
/// some unrelated reason cannot satisfy this helper.
fn assert_refused_up_front(what: &str, outcome: &DriveOutcome, expect_in_message: &[&str]) {
    let status = outcome
        .rpc
        .as_ref()
        .err()
        .unwrap_or_else(|| panic!("{what}: do_get must REFUSE the request up front"));
    assert_eq!(
        status.code(),
        tonic::Code::InvalidArgument,
        "{what}: a zero-output-column request is a CLIENT error, got {:?} ({status})",
        status.code()
    );
    assert!(
        outcome.msgs.is_empty(),
        "{what}: no message may reach the client, got {}",
        outcome.msgs.len()
    );
    assert!(
        outcome.stream_err.is_none(),
        "{what}: no stream is opened, so there is no mid-stream failure"
    );
    for expected in expect_in_message {
        assert!(
            status.message().contains(expected),
            "{what}: refusal must mention {expected:?}, got: {status}"
        );
    }
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
/// The ticket passed here must be one that ADMISSION accepts: since #3742 a
/// zero-output-column ticket is refused by `from_ticket` itself, so there is no
/// `ScanSpec` to route and the question "which route would it have taken" no
/// longer has an answer. That is not a gap in the measurement — `detect_route`
/// takes `(spec.filter, schema)` and never consults the projection, so the
/// FILTER decides the route, and probing it with a served projection establishes
/// the route for every projection of the same filter.
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

/// CONTRACT (#3742): the POINT-READ route (a full-partition-key equality filter)
/// shares `MergeProducer::output_columns()`, so an empty projection used to
/// reach arrow through it too. It is now refused at admission — BEFORE routing —
/// so the refusal is route-independent by construction.
///
/// The route is ASSERTED, not assumed (roborev job 1, finding 1): without it the
/// served control below could be a full scan, and the test would not cover what
/// it names.
#[test]
fn a_zero_column_projection_on_the_point_read_route_is_refused_up_front() {
    let (_temp, data_dir) = build_fixture();
    let filter =
        serde_json::json!({"type": "Compare", "column": "key", "op": "Equal", "value": "k1"});

    // Control: the same point-read ticket with no projection. It must route to
    // the point path AND return exactly the one target partition's row, so the
    // refusal below can never be mistaken for a point route that is simply
    // broken on this fixture.
    let control = ticket_with(serde_json::json!({"filter": filter.clone()}));
    assert_ticket_routes_to_point_read("point-read control", &control);
    let batches = drive_batches(&data_dir, control).expect("point-read control must succeed");
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        rows, 1,
        "point-read control: the `key = 'k1'` partition is exactly one row"
    );

    // The SAME filter with a one-column projection: still the point route, still
    // served. This is what pins the route for the refused ticket below, whose
    // only difference is a projection `detect_route` never reads.
    let projected = ticket_with(serde_json::json!({"filter": filter.clone(), "columns": ["key"]}));
    assert_ticket_routes_to_point_read("point-read columns=[key]", &projected);
    let projected_rows: usize = drive_batches(&data_dir, projected)
        .expect("point-read with a served projection must succeed")
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(projected_rows, 1, "point-read columns=[key]: one row");

    let zero = ticket_with(serde_json::json!({"filter": filter, "columns": []}));
    let outcome = drive(&data_dir, zero);
    assert_refused_up_front(
        "point-read columns=[]",
        &outcome,
        &["projection selects no columns"],
    );
}

/// CONTRACT (#3742): the AGGREGATION route builds its output columns from
/// `group_by + aggregates` (`agg.rs::partial_columns`). A ticket carrying an
/// empty aggregates list AND no group_by therefore has zero output columns by a
/// route that has nothing to do with `columns` — and is refused by the same
/// admission check, with the same `InvalidArgument`.
///
/// Before the check, this route reached `rows_to_record_batch(columns, group)`
/// over zero output columns (a global aggregation always emits one partial row),
/// and because it materializes EAGERLY inside `do_get`
/// (`build_aggregate_response`, dispatched at `service.rs:895`) it aborted the
/// RPC with `Status::Internal` and zero messages. The code changes; the
/// "zero messages" half does not.
#[test]
fn an_aggregation_with_no_output_columns_is_refused_up_front() {
    let (_temp, data_dir) = build_fixture();

    // Control: a real global count(*), DECODED — one batch, one row, one Int64
    // column carrying the 3 fixture rows.
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

    // An aggregation with NO group_by and NO aggregates: zero output columns.
    let outcome = drive(
        &data_dir,
        ticket_with(serde_json::json!({
            "aggregation": {"group_by": [], "aggregates": []}
        })),
    );
    assert_refused_up_front(
        "agg empty",
        &outcome,
        &["aggregation produces no output columns"],
    );
}

/// THE TRAP THIS TEST EXISTS TO PREVENT (#3742), pinned end to end over the real
/// `do_get` wire path: the admission predicate is **zero total OUTPUT columns**,
/// never "the `aggregates` list is empty".
///
/// `SELECT DISTINCT key` passes Trino's `PushAggregationIntoTableScan` guard
/// (which refuses only when grouping keys AND aggregations are both empty),
/// reaches `applyAggregation` with `groupingKeys=[key]`/`aggregations={}`, and
/// `CqliteFlightMetadata.java:569` emits `{"group_by": ["key"], "aggregates":
/// []}` verbatim. That is a LIVE wire shape with ONE output column: a predicate
/// keyed on an empty `aggregates` array would reject working `SELECT DISTINCT`
/// queries. It must be SERVED, and this asserts the served rows, not merely that
/// the RPC was accepted.
#[test]
fn a_distinct_shaped_aggregation_is_served() {
    let (_temp, data_dir) = build_fixture();

    let batches = drive_batches(
        &data_dir,
        ticket_with(serde_json::json!({
            "aggregation": {"group_by": ["key"], "aggregates": []}
        })),
    )
    .expect("`SELECT DISTINCT key` (group_by with empty aggregates) must be SERVED");

    let schema = batches
        .first()
        .map(|b| b.schema())
        .expect("at least one batch");
    assert_eq!(
        schema.fields().len(),
        1,
        "one output column (the group-by key), got {:?}",
        schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
    );
    assert_eq!(schema.field(0).name(), "key");

    let mut keys: Vec<String> = Vec::new();
    for batch in &batches {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("the group-by key is a text column");
        for i in 0..batch.num_rows() {
            keys.push(col.value(i).to_string());
        }
    }
    keys.sort();
    assert_eq!(
        keys,
        vec!["k1", "k2", "k3"],
        "the 3 distinct partition keys of the keyvalue fixture"
    );
}
