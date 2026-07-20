//! Issue #2681 (spec Req 2) — the `do_get` abort path emits a STRUCTURED
//! `tracing` EVENT, at the reason-appropriate LEVEL, carrying the attribution
//! context (`cqlite.flight.abort_reason`, `ticket_id`, `snapshot_generation`).
//!
//! The companion `do_get_abort_reason_test.rs` proves the METRIC side (the
//! bounded `cqlite.flight.abort_reason` attribute on `cqlite.errors.total`,
//! never `other`) via the OTel meter harness. But that harness captures metric
//! points, NOT the bare `tracing::debug!/warn!/error!` EVENT emitted by
//! `obs_abort::record_do_get_abort`. This test closes the remaining half of
//! Req 2: it installs an in-process, event-capturing `tracing_subscriber::Layer`
//! and asserts, for real service-surface aborts, that the emitted event
//!
//!   1. fires at the reason-appropriate LEVEL (benign teardown → DEBUG, a
//!      genuine internal fault → ERROR), and
//!   2. carries `cqlite.flight.abort_reason`, `ticket_id`, AND — crucially —
//!      `snapshot_generation` PRESENT and NON-None on a warm teardown, the exact
//!      resolve-time path that populates the generation (service.rs:787-788 via
//!      `WarmError::resolved_generation()`).
//!
//! Approach (a) from the task: a REAL `do_get` abort driven through the public
//! `CqliteFlightService` surface. The capturing layer is installed as the
//! THREAD default (`tracing::subscriber::set_default`) around a CURRENT-THREAD
//! tokio runtime's `block_on`, so the abort future — including the bare
//! `record_do_get_abort` event emission at `do_get`'s error arm — is polled on
//! the same thread that holds the subscriber, making event capture deterministic
//! (a multi-thread runtime would poll the emission on a worker thread that never
//! saw our thread-local default).
//!
//! Not feature-gated: the abort EVENT is emitted unconditionally (plain `tracing`
//! macros, no observability feature), so this test is meaningful in every build.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow_flight::flight_service_server::FlightService;
use arrow_flight::Ticket;
use futures::StreamExt;
use tonic::Request;
use tracing::field::{Field, Visit};
use tracing::Level;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::Layer;

use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use cqlite_flight::service::CqliteFlightService;

const KS: &str = "abort_event_ks";
const TBL: &str = "items";
const DDL: &str = "CREATE TABLE abort_event_ks.items (id int PRIMARY KEY, name text, score int)";
const ABORT_REASON_FIELD: &str = "cqlite.flight.abort_reason";

// ---------------------------------------------------------------------------
// Event-capturing tracing layer
// ---------------------------------------------------------------------------

/// One captured `tracing` event: its level plus the field values we assert on.
#[derive(Debug, Clone)]
struct CapturedEvent {
    level: Level,
    /// All non-numeric fields, keyed by field name (e.g. the dotted
    /// `cqlite.flight.abort_reason`, `ticket_id`, `code`, `message`).
    fields: HashMap<String, String>,
    /// The `snapshot_generation` field, PRESENT (`Some`) only when the event
    /// carried it as a `u64`. A production `None` snapshot generation records
    /// NOTHING (tracing's `Value for Option<T>` is a no-op on `None`), so an
    /// absent value here means the abort site had no resolved generation.
    snapshot_generation: Option<u64>,
}

impl CapturedEvent {
    fn abort_reason(&self) -> Option<&str> {
        self.fields.get(ABORT_REASON_FIELD).map(String::as_str)
    }
}

/// `Visit` implementation that harvests the fields we care about. Strings land
/// in `fields`; `snapshot_generation` is captured typed off `record_u64`.
#[derive(Default)]
struct FieldVisitor {
    fields: HashMap<String, String>,
    snapshot_generation: Option<u64>,
}

impl Visit for FieldVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == "snapshot_generation" {
            self.snapshot_generation = Some(value);
        }
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        // Fallback for `%code` (Display-wrapped) and the message format-args.
        self.fields
            .entry(field.name().to_string())
            .or_insert_with(|| format!("{value:?}"));
    }
}

/// A layer that records every abort-taxonomy event (any event carrying the
/// `cqlite.flight.abort_reason` field) into a shared vector.
struct CaptureLayer {
    events: Arc<Mutex<Vec<CapturedEvent>>>,
}

impl<S: tracing::Subscriber> Layer<S> for CaptureLayer {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let mut visitor = FieldVisitor::default();
        event.record(&mut visitor);
        // Only the abort-taxonomy events are of interest; ignore unrelated
        // debug chatter from other crates so assertions never see noise.
        if !visitor.fields.contains_key(ABORT_REASON_FIELD) {
            return;
        }
        self.events
            .lock()
            .expect("events lock")
            .push(CapturedEvent {
                level: *event.metadata().level(),
                fields: visitor.fields,
                snapshot_generation: visitor.snapshot_generation,
            });
    }
}

// ---------------------------------------------------------------------------
// Fixture helpers
// ---------------------------------------------------------------------------

fn simple_schema() -> TableSchema {
    let col = |name: &str, ty: &str, nullable: bool| Column {
        name: name.into(),
        data_type: ty.into(),
        nullable,
        default: None,
        is_static: false,
    };
    TableSchema {
        keyspace: KS.into(),
        table: TBL.into(),
        partition_keys: vec![KeyColumn {
            name: "id".into(),
            data_type: "int".into(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            col("id", "int", false),
            col("name", "text", true),
            col("score", "int", true),
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

fn write_row(id: i32, name: &str, score: i32, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("id", Value::Integer(id)),
        None,
        vec![
            CellOperation::Write {
                column: "name".into(),
                value: Value::text(name),
            },
            CellOperation::Write {
                column: "score".into(),
                value: Value::Integer(score),
            },
        ],
        ts,
        None,
    )
}

/// Flush a single-SSTable fixture and return its data dir.
fn build_fixture() -> (tempfile::TempDir, std::path::PathBuf) {
    let schema = simple_schema();
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");
    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, schema);
    let mut engine = WriteEngine::new(config).expect("engine");
    for i in 1..=8 {
        engine
            .write(write_row(i, &format!("n{i}"), i * 10, 100))
            .expect("write");
    }
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("rt");
    rt.block_on(engine.flush()).expect("flush").expect("info");
    (temp, data_dir)
}

fn ticket_bytes(snapshot: Option<&str>) -> Vec<u8> {
    let mut obj = serde_json::json!({ "keyspace": KS, "table": TBL, "ddl": DDL });
    if let Some(s) = snapshot {
        obj["snapshot"] = serde_json::json!(s);
    }
    serde_json::to_vec(&obj).expect("ticket json")
}

/// Cassandra-style snapshot: hardlink (or copy) every component into
/// `<table_dir>/snapshots/<name>/`.
fn make_snapshot(table_dir: &std::path::Path, name: &str) -> std::path::PathBuf {
    let dst = table_dir.join("snapshots").join(name);
    std::fs::create_dir_all(&dst).expect("mk snapshot dir");
    for entry in std::fs::read_dir(table_dir)
        .expect("read table dir")
        .flatten()
    {
        let path = entry.path();
        if path.is_file() {
            let target = dst.join(entry.file_name());
            std::fs::hard_link(&path, &target)
                .or_else(|_| std::fs::copy(&path, &target).map(|_| ()))
                .expect("snapshot link/copy");
        }
    }
    dst
}

/// Warm the registry cache from `snapshot`, then drive one more `do_get` under
/// the capturing layer and return the captured abort events.
fn warm_then_abort(
    svc: &CqliteFlightService,
    rt: &tokio::runtime::Runtime,
    snapshot: &str,
    mutate_snapshot: impl FnOnce(),
) -> Vec<CapturedEvent> {
    // First query warms the cache from the live snapshot dir.
    rt.block_on(async {
        let resp = svc
            .do_get(Request::new(Ticket::new(ticket_bytes(Some(snapshot)))))
            .await
            .expect("first snapshot do_get warms");
        let mut stream = resp.into_inner();
        while stream.next().await.is_some() {}
    });

    // Mutate the snapshot dir under the warm reader (plant the teardown/escape).
    mutate_snapshot();

    // Install the capturing layer as THIS thread's default subscriber, then
    // drive the aborting request on the SAME thread via a current-thread runtime
    // so the abort event is emitted under our subscriber.
    let events = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry().with(CaptureLayer {
        events: events.clone(),
    });
    let guard = tracing::subscriber::set_default(subscriber);
    rt.block_on(async {
        let _err = svc
            .do_get(Request::new(Ticket::new(ticket_bytes(Some(snapshot)))))
            .await
            .err()
            .expect("the mutated snapshot must abort the do_get");
    });
    drop(guard);

    let captured = events.lock().expect("events lock").clone();
    captured
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// A benign warm teardown of a SINGLE generation entry emits a DEBUG event
/// carrying `abort_reason=superseded_split`, the ticket identity, AND a PRESENT,
/// NON-None `snapshot_generation` — the resolve-time path that populates the
/// generation from `WarmError::resolved_generation()` (spec Req 2).
#[cfg(unix)]
#[test]
fn benign_teardown_emits_debug_event_with_nonnull_generation() {
    use std::os::unix::fs::symlink;

    // Current-thread runtime: `block_on` polls the abort future (and its bare
    // event emission) on THIS thread, where the capturing layer is installed.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("rt");

    let (_temp, data_dir) = build_fixture();
    let table_dir = data_dir.join(KS).join(TBL);
    let snap_dir = make_snapshot(&table_dir, "snap_teardown");
    let svc = CqliteFlightService::new(data_dir, 1024);

    let events = warm_then_abort(&svc, &rt, "snap_teardown", || {
        // Plant a DANGLING symlink named like a specific generation's Data.db,
        // pointing at a NON-EXISTENT target INSIDE the snapshot dir. It stays
        // contained (issue #1430 check passes — the parent dir resolves within
        // root), but `stat`/`GenerationId::resolve` fails on the dangling link,
        // so the warm probe classifies it as `ProbeEntrySuperseded` — the benign
        // teardown variant — while `resolved_generation()` parses `77` from the
        // name (spec Req 2: a non-None generation on a reachable benign abort).
        let phantom = snap_dir.join("phantom-nb-77-big-Data.db");
        symlink(&phantom, snap_dir.join("nb-77-big-Data.db")).expect("plant dangling symlink");
    });

    let abort = events
        .iter()
        .find(|e| e.abort_reason() == Some("superseded_split"))
        .unwrap_or_else(|| panic!("expected a superseded_split abort event, captured: {events:?}"));

    // (1) reason-appropriate LEVEL: a benign teardown logs at DEBUG.
    assert_eq!(
        abort.level,
        Level::DEBUG,
        "a benign superseded_split teardown must emit at DEBUG, got {:?}",
        abort.level
    );
    // (2a) attribution: the ticket/split identity is carried on the event.
    assert_eq!(
        abort.fields.get("ticket_id").map(String::as_str),
        Some("abort_event_ks/items/snap_teardown"),
        "the abort event must carry the ticket/split identity, fields: {:?}",
        abort.fields
    );
    // (2b) attribution: `snapshot_generation` is PRESENT and NON-None here — the
    // exact Req 2 property the metric-side test cannot observe. Parsed as `77`
    // from the torn-down `nb-77-big-Data.db` entry.
    assert_eq!(
        abort.snapshot_generation,
        Some(77),
        "a warm teardown must carry a PRESENT, NON-None snapshot_generation \
         (spec Req 2); event snapshot_generation={:?}, fields={:?}",
        abort.snapshot_generation,
        abort.fields
    );
}

/// A containment escape (issue #1430 security backstop) is a genuine internal
/// fault: its abort event fires at ERROR and still carries the attribution
/// context, including the parsed generation of the offending entry.
#[cfg(unix)]
#[test]
fn containment_escape_emits_error_event_with_context() {
    use std::os::unix::fs::symlink;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("rt");

    let (_temp, data_dir) = build_fixture();
    let table_dir = data_dir.join(KS).join(TBL);
    let snap_dir = make_snapshot(&table_dir, "snap_escape");
    let svc = CqliteFlightService::new(data_dir, 1024);

    // A target OUTSIDE the table dir; keep the tempdir alive for the request.
    let outside = tempfile::TempDir::new().expect("outside tempdir");
    let escapee = outside.path().join("nb-99-big-Data.db");
    std::fs::write(&escapee, b"x").expect("write escapee");

    let events = warm_then_abort(&svc, &rt, "snap_escape", || {
        // A symlink whose resolved target escapes the table dir → the warm
        // probe's per-entry containment check rejects it as
        // `ProbeEntryContainment` → `internal` (ERROR), never demoted to the
        // benign bucket. `resolved_generation()` parses `99` from the name.
        symlink(&escapee, snap_dir.join("nb-99-big-Data.db")).expect("plant escaping symlink");
    });

    let abort = events
        .iter()
        .find(|e| e.abort_reason() == Some("internal"))
        .unwrap_or_else(|| panic!("expected an internal abort event, captured: {events:?}"));

    // (1) reason-appropriate LEVEL: a genuine internal fault logs at ERROR.
    assert_eq!(
        abort.level,
        Level::ERROR,
        "a containment-escape internal fault must emit at ERROR, got {:?}",
        abort.level
    );
    // (2) attribution context is carried on the event.
    assert_eq!(
        abort.fields.get("ticket_id").map(String::as_str),
        Some("abort_event_ks/items/snap_escape"),
        "the abort event must carry the ticket/split identity, fields: {:?}",
        abort.fields
    );
    assert_eq!(
        abort.snapshot_generation,
        Some(99),
        "the internal abort must carry the offending entry's parsed generation, \
         snapshot_generation={:?}, fields={:?}",
        abort.snapshot_generation,
        abort.fields
    );
    // A containment escape is NEVER demoted to the benign teardown reason.
    assert_ne!(
        abort.abort_reason(),
        Some("superseded_split"),
        "a containment escape must not be demoted to superseded_split"
    );
}
