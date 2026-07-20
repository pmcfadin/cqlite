//! Issue #2681 — wiring evidence for the `do_get` abort taxonomy.
//!
//! Asserts, through the OTel capture harness, that a server-side `do_get`
//! failure increments `cqlite.errors.total` with the authoritative, site-stamped
//! `cqlite.flight.abort_reason` bounded attribute (never `other`), for each abort
//! class the spec pins:
//!
//! * a client that disconnects mid-stream → `client_cancel`,
//! * a snapshot torn down under a warm reader → `snapshot_retired`/`superseded_split`,
//! * an admission-ceiling shed → `admission_shed`.
//!
//! A SEPARATE integration-test binary/process (matching the #2162
//! `metrics_capture_test.rs` precedent): the capture harness installs a
//! PROCESS-GLOBAL meter provider on first use, and once installed EVERY metric
//! record in the process is captured. `do_get_transport_test.rs` runs several
//! parallel client-disconnect tests that would each increment `client_cancel`
//! into a shared DELTA window and corrupt this assertion, so these scenarios
//! live in their own binary and run inside a SINGLE serial test (each scenario
//! resets the capture, drives one abort, and reads back its own delta).
//!
//! Run with:
//! ```text
//! cargo test -p cqlite-flight --features observability-testing --test do_get_abort_reason_test
//! ```

#![cfg(feature = "observability-testing")]

use std::collections::HashMap;
use std::time::Duration;

use arrow_flight::flight_service_server::FlightService;
use arrow_flight::Ticket;
use futures::StreamExt;
use tonic::Request;

use cqlite_core::observability::{catalog, testing};
use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use cqlite_flight::admission::{Admission, AdmissionConfig, WaitBudget};
use cqlite_flight::service::CqliteFlightService;

const KS: &str = "abort_reason_ks";
const TBL: &str = "items";
const DDL: &str = "CREATE TABLE abort_reason_ks.items (id int PRIMARY KEY, name text, score int)";

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

/// Flush a multi-row single-SSTable fixture and return its data dir. Enough rows
/// (with a tiny batch size in the service) that a client can read one batch and
/// drop the stream with many batches still unstreamed.
fn build_fixture() -> (tempfile::TempDir, std::path::PathBuf) {
    let schema = simple_schema();
    let temp = tempfile::TempDir::new().unwrap();
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");
    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, schema);
    let mut engine = WriteEngine::new(config).expect("engine");
    for i in 1..=24 {
        engine
            .write(write_row(i, &format!("n{i}"), i * 10, 100))
            .expect("write");
    }
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(engine.flush()).expect("flush").expect("info");
    (temp, data_dir)
}

fn ticket_bytes(snapshot: Option<&str>) -> Vec<u8> {
    let mut obj = serde_json::json!({ "keyspace": KS, "table": TBL, "ddl": DDL });
    if let Some(s) = snapshot {
        obj["snapshot"] = serde_json::json!(s);
    }
    serde_json::to_vec(&obj).unwrap()
}

/// Make a Cassandra-style snapshot: hardlink (or copy) every SSTable component
/// under `<table_dir>` into `<table_dir>/snapshots/<name>/`.
fn make_snapshot(table_dir: &std::path::Path, name: &str) -> std::path::PathBuf {
    let dst = table_dir.join("snapshots").join(name);
    std::fs::create_dir_all(&dst).unwrap();
    for entry in std::fs::read_dir(table_dir).unwrap().flatten() {
        let path = entry.path();
        if path.is_file() {
            let target = dst.join(entry.file_name());
            std::fs::hard_link(&path, &target)
                .or_else(|_| std::fs::copy(&path, &target).map(|_| ()))
                .unwrap();
        }
    }
    dst
}

/// Sum of `cqlite.errors.total` points carrying `subsystem=flight` AND the given
/// `abort_reason`.
fn abort_count(metrics: &testing::CapturedMetrics, reason: &str) -> f64 {
    metrics.sum_where(
        catalog::ERRORS_TOTAL,
        &[
            (catalog::attr::SUBSYSTEM, "flight"),
            (catalog::attr::FLIGHT_ABORT_REASON, reason),
        ],
    )
}

/// Sum of `cqlite.errors.total` points carrying `subsystem=flight` AND the coarse
/// `error.category` — used to prove a benign abort is NOT counted as a genuine
/// fault (`other`).
fn category_count(metrics: &testing::CapturedMetrics, category: &str) -> f64 {
    metrics.sum_where(
        catalog::ERRORS_TOTAL,
        &[
            (catalog::attr::SUBSYSTEM, "flight"),
            (catalog::attr::ERROR_CATEGORY, category),
        ],
    )
}

/// All three abort-taxonomy wiring scenarios in ONE serial test (the harness's
/// meter provider is process-global; sequential reset/flush pairs isolate each
/// scenario's DELTA window — see the module doc).
#[test]
fn do_get_aborts_carry_site_stamped_abort_reason() {
    let mc = testing::metrics_capture();
    let rt = tokio::runtime::Runtime::new().unwrap();

    // ---- Scenario 1: client disconnect → client_cancel --------------------
    // A client that drops the response stream mid-read is a benign, expected
    // terminal state stamped at the MeteredDoGetStream::drop site. Driven via the
    // in-process service surface (the accepted public-surface fallback): dropping
    // the response stream before it is drained fires the exact drop path a real
    // transport disconnect fires, deterministically within one runtime.
    {
        let (_temp, data_dir) = build_fixture();
        // batch_size 1 → many batches; read one, then drop with the rest unstreamed.
        let svc = CqliteFlightService::new(data_dir, 1);
        mc.reset();
        rt.block_on(async {
            let resp = svc
                .do_get(Request::new(Ticket::new(ticket_bytes(None))))
                .await
                .expect("do_get opens");
            let mut stream = resp.into_inner();
            // Read a single message, then drop the stream WITHOUT draining — the
            // client-disconnect shape.
            let _first = stream.next().await;
            drop(stream);
            // Let the server-side drop/finalize run.
            tokio::time::sleep(Duration::from_millis(50)).await;
        });
        let metrics = mc.flush_and_collect();
        assert!(
            abort_count(&metrics, "client_cancel") >= 1.0,
            "a mid-stream client disconnect must increment \
             cqlite.errors.total{{abort_reason=client_cancel}}, got {}",
            abort_count(&metrics, "client_cancel")
        );
        assert_eq!(
            abort_count(&metrics, "internal"),
            0.0,
            "a client disconnect must NOT be attributed to internal"
        );
    }

    // ---- Scenario 2: snapshot torn down under a warm reader ---------------
    // Warm the cache from a snapshot dir, tear the snapshot down, then query the
    // same snapshot again: the warm staleness probe hits a dead dir and aborts
    // with a teardown reason (snapshot_retired / superseded_split), never internal.
    {
        let (_temp, data_dir) = build_fixture();
        let table_dir = data_dir.join(KS).join(TBL);
        make_snapshot(&table_dir, "snap1");
        let svc = CqliteFlightService::new(data_dir, 1024);
        // First query warms the cache from the live snapshot dir.
        rt.block_on(async {
            let resp = svc
                .do_get(Request::new(Ticket::new(ticket_bytes(Some("snap1")))))
                .await
                .expect("first snapshot do_get warms");
            let mut stream = resp.into_inner();
            while stream.next().await.is_some() {}
        });
        // Tear the snapshot down under the warm reader.
        std::fs::remove_dir_all(table_dir.join("snapshots").join("snap1"))
            .expect("tear down the snapshot dir");
        mc.reset();
        let code = rt.block_on(async {
            svc.do_get(Request::new(Ticket::new(ticket_bytes(Some("snap1")))))
                .await
                .err()
                .expect("a torn-down snapshot must abort the do_get")
                .code()
        });
        let metrics = mc.flush_and_collect();
        let teardown =
            abort_count(&metrics, "snapshot_retired") + abort_count(&metrics, "superseded_split");
        assert!(
            teardown >= 1.0,
            "a torn-down snapshot must increment cqlite.errors.total with \
             abort_reason ∈ {{snapshot_retired, superseded_split}} (got \
             retired={}, superseded={}, code={code:?})",
            abort_count(&metrics, "snapshot_retired"),
            abort_count(&metrics, "superseded_split"),
        );
        assert_eq!(
            abort_count(&metrics, "internal"),
            0.0,
            "a torn-down snapshot must NOT be attributed to internal"
        );
        assert_eq!(
            category_count(&metrics, "other"),
            0.0,
            "a benign teardown must NOT land in the coarse 'other' category"
        );
    }

    // ---- Scenario 2b: containment escape → internal (issue #1430 backstop) -
    // A directory entry whose resolved path ESCAPES the table dir is the #1430
    // security backstop, NOT a benign teardown. It MUST be attributed to
    // `internal` (ERROR / coarse `other`), never demoted to the benign
    // `superseded_split` / `cancelled` bucket — the exact signal this backstop
    // exists to surface (roborev BLOCKER 1, issue #2681).
    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;
        let (_temp, data_dir) = build_fixture();
        let table_dir = data_dir.join(KS).join(TBL);
        let snap_dir = make_snapshot(&table_dir, "snap_escape");
        let svc = CqliteFlightService::new(data_dir, 1024);
        // First query warms the cache from the snapshot dir.
        rt.block_on(async {
            let resp = svc
                .do_get(Request::new(Ticket::new(ticket_bytes(Some("snap_escape")))))
                .await
                .expect("first snapshot do_get warms");
            let mut stream = resp.into_inner();
            while stream.next().await.is_some() {}
        });
        // Plant an escaping symlink named like a Data.db INSIDE the snapshot dir:
        // its target lives outside the table dir, so the warm probe's per-entry
        // containment check (issue #1430) rejects it on the next request.
        let outside = tempfile::TempDir::new().unwrap();
        let escapee = outside.path().join("nb-99-big-Data.db");
        std::fs::write(&escapee, b"x").unwrap();
        symlink(&escapee, snap_dir.join("nb-99-big-Data.db")).unwrap();
        mc.reset();
        let code = rt.block_on(async {
            svc.do_get(Request::new(Ticket::new(ticket_bytes(Some("snap_escape")))))
                .await
                .err()
                .expect("a containment escape must abort the do_get")
                .code()
        });
        let metrics = mc.flush_and_collect();
        assert!(
            abort_count(&metrics, "internal") >= 1.0,
            "a containment escape (issue #1430 backstop) must increment \
             cqlite.errors.total{{abort_reason=internal}}, got {} (code={code:?})",
            abort_count(&metrics, "internal")
        );
        assert_eq!(
            abort_count(&metrics, "superseded_split"),
            0.0,
            "a containment escape must NOT be demoted to the benign superseded_split bucket"
        );
    }

    // ---- Scenario 3: admission shed → admission_shed ----------------------
    // Hold the sole admission permit, then drive a further do_get past the
    // ceiling: it sheds with UNAVAILABLE, stamped admission_shed at the site.
    // Capture this window's metrics for BOTH the scenario assertions AND the
    // bounded-attribute invariant below (it is a window that DOES carry error
    // points — a fresh empty DELTA window would assert the invariant vacuously).
    let shed_metrics = {
        let (_temp, data_dir) = build_fixture();
        let admission = Admission::new(AdmissionConfig {
            max_concurrent_scans: 1,
            wait_budget: WaitBudget::Timeout(Duration::from_millis(20)),
        });
        let svc = CqliteFlightService::with_admission(data_dir, 4, admission);
        mc.reset();
        let code = rt.block_on(async {
            // Hold the only permit so the next request cannot be admitted.
            let _held = svc
                .admission()
                .acquire()
                .await
                .expect("hold the sole permit");
            svc.do_get(Request::new(Ticket::new(ticket_bytes(None))))
                .await
                .err()
                .expect("a saturated admission ceiling must shed the do_get")
                .code()
        });
        assert_eq!(
            code,
            tonic::Code::Unavailable,
            "admission shed must surface as UNAVAILABLE"
        );
        let metrics = mc.flush_and_collect();
        assert!(
            abort_count(&metrics, "admission_shed") >= 1.0,
            "an admission shed must increment cqlite.errors.total{{abort_reason=admission_shed}}, \
             got {}",
            abort_count(&metrics, "admission_shed")
        );
        assert_eq!(
            abort_count(&metrics, "internal"),
            0.0,
            "an admission shed must NOT be attributed to internal"
        );
        metrics
    };

    // ---- Bounded-attribute invariant over every collected error point ------
    // Assert over scenario 3's window (captured above) — it carries ≥1 real
    // error point (the admission_shed abort). A fresh `flush_and_collect()` here
    // would be an EMPTY DELTA window (no metric-producing op since the scenario-3
    // collect), so the loop would never run and the invariant would be vacuous.
    let errs = shed_metrics
        .find(catalog::ERRORS_TOTAL)
        .expect("scenario 3 recorded at least one cqlite.errors.total point");
    assert!(
        !errs.points.is_empty(),
        "the bounded-attribute invariant must run over ≥1 real error point"
    );
    for p in &errs.points {
        for (k, _) in &p.attributes {
            assert!(
                [
                    catalog::attr::ERROR_CATEGORY,
                    catalog::attr::SUBSYSTEM,
                    catalog::attr::FLIGHT_ABORT_REASON,
                ]
                .contains(&k.as_str()),
                "cqlite.errors.total carries unbounded attribute key {k:?}"
            );
        }
    }
}
