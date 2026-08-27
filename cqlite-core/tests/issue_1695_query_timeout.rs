//! Issue #1695 (AH2, epic #1685 "config honesty"): `query.max_execution_time` is
//! ENFORCED at the query-engine chokepoint.
//!
//! # The defect
//!
//! `Config.query.max_execution_time` (default 300s) is set by the CLI's
//! `performance.query_timeout_ms` and, before this change, enforced NOWHERE. A
//! runaway query ran to completion however long it took; the operator's knob was
//! a placebo. The fix is ONE `tokio::time::timeout` per public engine entry point
//! (`cqlite-core/src/query/engine/deadline.rs`) — never ad-hoc clock checks in
//! the scan loop.
//!
//! # What each case pins
//!
//! * `budget_of_one_millisecond_times_out_a_large_scan` — THE RED CASE. With a
//!   1ms budget a real large-fixture full scan must return
//!   `Error::QueryTimeout`. On unenforced code it returns `Ok(rows)`.
//! * `default_budget_leaves_a_normal_query_unaffected` — the shipped 300s default
//!   changes nothing (and returns a NON-EMPTY result, so the case cannot pass
//!   vacuously on an empty fixture).
//! * `zero_sentinel_means_unbounded` — `Duration::ZERO` is "no timeout", NOT a
//!   zero deadline that trips instantly; it returns the same rows as the default.
//! * `config_validate_accepts_the_zero_sentinel` — the sentinel is explicitly
//!   LEGAL to `Config::validate`.
//! * `streaming_budget_covers_the_setup_future` — the streaming surface is
//!   bounded for the work done INSIDE `execute_streaming` (documented contract).
//! * `timed_out_streaming_query_leaves_no_live_producer` /
//!   `dropping_a_streaming_iterator_retires_its_producer` — cancellation is
//!   clean: nothing is left running after the timed-out future is dropped.
//! * `timeout_error_is_not_a_corruption` — the timeout is a DISTINCT variant with
//!   a distinct telemetry category, so an operator budget can never read as
//!   damaged data.
//!
//! # Fixture discipline
//!
//! The scan fixture is resolved per TABLE via the shared, table-granular resolver
//! (issue #3220) and its binaries are GIT-COMMITTED, so it is `must_run`:
//! fail-closed unconditionally, never a silent SKIP. No wall-clock threshold is
//! ever asserted (issue #2642) — the assertions are on the RESULT (a timeout
//! error vs rows), never on how long anything took.

// `cli-helpers` gates `cqlite_core::ingestion`, the surface this lane uses to open
// the fixture as a real `Database` — the same object the CLI builds. Mirrored by
// `required-features` in cqlite-core/Cargo.toml so the gate runs it WITH the
// feature rather than compiling an empty target.
#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

// `#[path]` because this file IS the integration target's crate root.
#[path = "support/datasets_root.rs"]
mod datasets_root;

use std::path::Path;
use std::time::Duration;

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::result::StreamingConfig;
use cqlite_core::{Config, Database, Error};

use datasets_root::{describe_search, schema_path, sstables_root_for_table};

/// A real Cassandra 5.0 fixture to full-scan.
struct ScanFixture {
    keyspace: &'static str,
    table: &'static str,
    schema: &'static str,
    /// `true` when the SSTable BINARIES are git-committed, so an unresolvable
    /// fixture is a harness defect and the case is fail-closed (issue #3220).
    committed: bool,
}

/// The largest GIT-COMMITTED single-table fixture (~260 KiB `Data.db` of
/// high-entropy BLOB rows). Present in every checkout, so every case that only
/// needs "a real scan" uses it and is `must_run`.
const COMMITTED: ScanFixture = ScanFixture {
    keyspace: "test_comp",
    table: "incompressible_uncompressed_chunk",
    schema: "compression-parity.cql",
    committed: true,
};

/// The largest FETCHED fixture (~650 KiB / 1000 partitions of mixed CQL types):
/// the only corpus table whose full scan is reliably MULTI-millisecond, so it is
/// the one that can demonstrate the AC's literal `1ms` budget interrupting a scan
/// ALREADY IN PROGRESS. Its binaries are gitignored (only the JSONL sidecar is
/// committed), so this case skips clean on a minimal checkout — and fails closed
/// under `CQLITE_REQUIRE_FIXTURES=1`, the same contract the sibling dataset lanes
/// use. Fetch: `bash test-data/scripts/fetch-datasets.sh`.
const FETCHED_LARGE: ScanFixture = ScanFixture {
    keyspace: "test_basic",
    table: "simple_table",
    schema: "basic-types.cql",
    committed: false,
};

/// A budget that is ALREADY EXPIRED by the time the query future is first polled,
/// so enforcement is observable on ANY host and ANY fixture size: the bound trips
/// at the read path's first cooperative checkpoint
/// (`ScanCancel::checkpoint`) rather than depending on how long a scan happens to
/// take. Distinct from `Duration::ZERO`, which is the "no timeout" sentinel.
const EXPIRED_BUDGET: Duration = Duration::from_nanos(1);

/// Full-table scan over a fixture — the runaway query shape the knob bounds.
fn scan_query(f: &ScanFixture) -> String {
    format!("SELECT * FROM {}.{}", f.keyspace, f.table)
}

/// An aggregate over the same fixture. The streaming surface routes any
/// aggregate through its execute-then-stream path, so the WHOLE scan runs inside
/// the `execute_streaming` future — which is exactly the future the documented
/// streaming bound covers.
fn streaming_scan_query(f: &ScanFixture) -> String {
    format!("SELECT COUNT(*) FROM {}.{}", f.keyspace, f.table)
}

/// `true` when the dataset-dependent lanes must fail rather than skip.
fn require_fixtures() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").ok().as_deref(),
        Some("1") | Some("true") | Some("TRUE")
    )
}

/// Open a `Database` over `f` with `max_execution_time` set to `budget`.
///
/// PER-CASE fail-closed (issue #3220): a `committed` fixture MUST resolve — its
/// binaries are in git, so absence is a harness defect, never a legitimate skip.
/// A fetched fixture returns `None` (skip-clean) unless `CQLITE_REQUIRE_FIXTURES=1`.
async fn open_db(f: &ScanFixture, budget: Duration) -> Option<Database> {
    let root = match sstables_root_for_table(f.keyspace, f.table) {
        Some(root) => root,
        None => {
            assert!(
                !f.committed && !require_fixtures(),
                "fixture must resolve (fail-closed): {}",
                describe_search(f.keyspace, f.table)
            );
            eprintln!(
                "SKIP: fetched fixture absent ({}.{}); {} — set CQLITE_REQUIRE_FIXTURES=1 to enforce.",
                f.keyspace,
                f.table,
                describe_search(f.keyspace, f.table)
            );
            return None;
        }
    };
    let schema = schema_path(f.schema)
        .unwrap_or_else(|| panic!("committed schema {} must be readable (#3148)", f.schema));
    Some(open_db_at(f, &root, &schema, budget).await)
}

/// The `committed` opener, which cannot skip.
async fn open_committed_db(budget: Duration) -> Database {
    open_db(&COMMITTED, budget)
        .await
        .expect("the COMMITTED fixture can never skip")
}

async fn open_db_at(f: &ScanFixture, root: &Path, schema: &Path, budget: Duration) -> Database {
    let mut core_config = Config::default();
    core_config.query.max_execution_time = budget;
    core_config
        .validate()
        .expect("a max_execution_time budget must be a VALID configuration");
    let cfg = IngestionConfig {
        schema_paths: vec![schema.to_path_buf()],
        data_dir: root.to_path_buf(),
        version_hint: None,
        core_config,
        table_directory_filter: Some(format!("/{}/", f.keyspace)),
    };
    let result = ingest(cfg).await.expect("ingestion of the fixture");
    assert!(
        result.schema_load_result.schemas_loaded > 0,
        "the committed schema must load, else the scan would decode schemaless"
    );
    result.database
}

/// Assert `outcome` is the timeout error for `entry_point` under `limit`.
fn assert_timed_out<T>(
    outcome: cqlite_core::Result<T>,
    entry_point: &str,
    limit: Duration,
    rows_of: impl FnOnce(T) -> String,
) {
    match outcome {
        Err(Error::QueryTimeout {
            operation,
            limit: reported,
            ..
        }) => {
            assert_eq!(
                reported, limit,
                "the reported limit must be the CONFIGURED budget"
            );
            assert_eq!(
                operation, entry_point,
                "the error must name the bounded entry point"
            );
        }
        Err(other) => panic!(
            "expected Error::QueryTimeout from {entry_point} under a {limit:?} budget, got a \
             DIFFERENT error (the budget must not surface as an unrelated failure): {other}"
        ),
        Ok(value) => panic!(
            "REGRESSION (issue #1695): a {limit:?} max_execution_time budget was IGNORED by \
             {entry_point} — the query ran to completion ({}). `query.max_execution_time` is a \
             placebo again; the chokepoint wrapper in query/engine/deadline.rs is gone.",
            rows_of(value)
        ),
    }
}

/// THE RED CASE (AC1), literal form: `max_execution_time = 1ms` + a large-scan
/// fixture ⇒ the timeout `Err`.
///
/// Uses the corpus's largest FETCHED table (~650 KiB / 1000 partitions), whose
/// unbounded full scan measures multiple milliseconds — so the 1ms budget elapses
/// while the scan is ALREADY IN PROGRESS, at one of the read path's cooperative
/// checkpoints. Pre-fix (no wrapper at the chokepoint) this returned `Ok(1000
/// rows)`.
///
/// Skip-clean when the fetched corpus is absent; the always-present proof of the
/// same property is `expired_budget_times_out_a_committed_scan` below.
/// Asserts on the OUTCOME only — never on elapsed wall clock (issue #2642).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn budget_of_one_millisecond_times_out_a_large_scan() {
    let limit = Duration::from_millis(1);
    let Some(db) = open_db(&FETCHED_LARGE, limit).await else {
        return;
    };
    let outcome = db.execute(&scan_query(&FETCHED_LARGE)).await;
    assert_timed_out(outcome, "query.execute", limit, |r| {
        format!("{} rows", r.rows.len())
    });
}

/// AC1, HOST-INDEPENDENT form: an already-expired budget over the COMMITTED
/// fixture. `must_run` (fail-closed) and deterministic everywhere — the bound
/// trips at the read path's first cooperative checkpoint rather than depending on
/// how long a particular machine takes to decode a particular fixture. This is
/// the case that guarantees the knob is never a placebo, even on a minimal
/// checkout where the multi-millisecond fetched fixture is unavailable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expired_budget_times_out_a_committed_scan() {
    let db = open_committed_db(EXPIRED_BUDGET).await;
    let outcome = db.execute(&scan_query(&COMMITTED)).await;
    assert_timed_out(outcome, "query.execute", EXPIRED_BUDGET, |r| {
        format!("{} rows", r.rows.len())
    });
}

/// The same bound must hold on the OTHER public entry points, so the knob is not
/// a placebo on three of four paths: `execute_with_params` (markerless and bound)
/// and `execute_prepared`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_public_entry_point_is_bounded() {
    let db = open_committed_db(EXPIRED_BUDGET).await;
    let query = scan_query(&COMMITTED);

    // Markerless: delegates to `execute`'s INNER body, so it reports the
    // entry point it was CALLED on, under one shared budget.
    let markerless = db.execute_with_params(&query, &[]).await;
    assert_timed_out(
        markerless,
        "query.execute_with_params",
        EXPIRED_BUDGET,
        |r| format!("{} rows", r.rows.len()),
    );

    // A handle from `prepare()` carries the engine's budget too (issue #1695):
    // `Database::prepare` hands back a `PreparedQuery` whose own `execute` would
    // otherwise be an unbounded back door into the same full scan.
    let prepared = db.prepare(&query).await.expect("prepare the scan");
    let via_handle = prepared.execute(&[]).await;
    assert_timed_out(via_handle, "prepared.execute", EXPIRED_BUDGET, |r| {
        format!("{} rows", r.rows.len())
    });

    // With a bind marker: the bound SELECT pipeline.
    let bound = db
        .execute_with_params(
            &format!(
                "SELECT * FROM {}.{} WHERE pk = ?",
                COMMITTED.keyspace, COMMITTED.table
            ),
            &[cqlite_core::Value::Integer(1)],
        )
        .await;
    assert_timed_out(bound, "query.execute_with_params", EXPIRED_BUDGET, |r| {
        format!("{} rows", r.rows.len())
    });
}

/// AC2 (first half): the shipped 300s default changes nothing for a normal query.
/// Anti-empty-pass: the result must be NON-EMPTY, so the case cannot pass on an
/// empty fixture.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_budget_leaves_a_normal_query_unaffected() {
    assert_eq!(
        Config::default().query.max_execution_time,
        Duration::from_secs(300),
        "the shipped default budget is 300s"
    );
    let db = open_committed_db(Config::default().query.max_execution_time).await;
    let result = db
        .execute(&scan_query(&COMMITTED))
        .await
        .expect("the default 300s budget must not interfere with a normal query");
    assert!(
        !result.rows.is_empty(),
        "the committed fixture must yield rows — a 0-row pass would make every other \
         case here vacuous (dataset doctrine)"
    );
}

/// AC2 (second half): `Duration::ZERO` is the documented "no timeout" sentinel —
/// unbounded execution, NOT a zero deadline that trips at the first yield. It
/// must return the same rows the default budget does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zero_sentinel_means_unbounded() {
    let bounded = open_committed_db(Config::default().query.max_execution_time).await;
    let expected = bounded
        .execute(&scan_query(&COMMITTED))
        .await
        .expect("default budget query")
        .rows
        .len();
    assert!(expected > 0, "fixture must yield rows");

    let unbounded = open_committed_db(Duration::ZERO).await;
    let rows = unbounded
        .execute(&scan_query(&COMMITTED))
        .await
        .expect(
            "Duration::ZERO is the NO-TIMEOUT sentinel: it must run unbounded, not \
             elapse instantly",
        )
        .rows
        .len();
    assert_eq!(
        rows, expected,
        "the ZERO sentinel must return the same result as a generous budget"
    );
}

/// AC2: the sentinel is explicitly LEGAL configuration — `Config::validate` must
/// never reject it (nor any ordinary budget).
#[test]
fn config_validate_accepts_the_zero_sentinel() {
    let mut config = Config::default();
    config.query.max_execution_time = Duration::ZERO;
    config
        .validate()
        .expect("Duration::ZERO is the documented no-timeout sentinel and must validate");

    for budget in [
        Duration::from_millis(1),
        Duration::from_secs(300),
        Duration::from_secs(86_400),
    ] {
        config.query.max_execution_time = budget;
        config
            .validate()
            .unwrap_or_else(|e| panic!("budget {budget:?} must validate, got {e}"));
    }
}

/// The streaming surface is bounded too (issue #1695 item 2). The documented
/// scope is the `execute_streaming` FUTURE: parse, plan, stream setup, and — on
/// the execute-then-stream paths an aggregate takes — the whole scan. A 1ms
/// budget over the large fixture must therefore fail the call itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn streaming_budget_covers_the_setup_future() {
    let db = open_committed_db(EXPIRED_BUDGET).await;
    let outcome = db
        .execute_streaming(
            &streaming_scan_query(&COMMITTED),
            StreamingConfig::default(),
        )
        .await;
    assert_timed_out(outcome, "query.execute_streaming", EXPIRED_BUDGET, |_| {
        "an iterator".to_string()
    });
}

/// AC3: cancellation is clean. After the timed-out streaming future is dropped,
/// no producer task is left alive, and the engine is still usable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timed_out_streaming_query_leaves_no_live_producer() {
    let db = open_committed_db(EXPIRED_BUDGET).await;
    let baseline = alive_tasks();

    let outcome = db
        .execute_streaming(
            &streaming_scan_query(&COMMITTED),
            StreamingConfig::default(),
        )
        .await;
    assert!(
        matches!(outcome, Err(Error::QueryTimeout { .. })),
        "precondition: the streaming call must have timed out"
    );
    drop(outcome);

    await_tasks_back_to(baseline, "post-timeout").await;

    // The engine is still USABLE after the cancellation: the timed-out future
    // released everything it held (readers, buffers, registry locks), so a second
    // query reaches the same clean typed outcome instead of hanging or failing
    // with a poisoned-lock / already-borrowed error.
    let again = db
        .execute_streaming(
            &streaming_scan_query(&COMMITTED),
            StreamingConfig::default(),
        )
        .await;
    assert!(
        matches!(again, Err(Error::QueryTimeout { .. })),
        "after a cancelled query the engine must still work (same clean timeout, not a \
         hang or a poisoned-state error): {:?}",
        again.err()
    );
}

/// The complementary half of drop-safety, with NO timing dependence: a streaming
/// producer that is definitely running (the consumer has taken a row) exits when
/// its iterator is dropped. This is the mechanism the timeout relies on — the
/// timed-out future's drop closes the receiver.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_a_streaming_iterator_retires_its_producer() {
    // Unbounded, so this case tests the DROP mechanism alone.
    let db = open_committed_db(Duration::ZERO).await;
    let baseline = alive_tasks();

    // A ONE-row channel buffer, deliberately: with the default 1024-row buffer the
    // producer can push this fixture's whole result set and EXIT before the check
    // below, making "a producer is alive" a race. With `buffer_size = 1` and a
    // fixture of many rows, taking one row leaves the producer parked on a full
    // channel — so it is still alive by construction, not by timing.
    let mut iter = db
        .execute_streaming(
            &scan_query(&COMMITTED),
            StreamingConfig {
                buffer_size: 1,
                ..StreamingConfig::default()
            },
        )
        .await
        .expect("unbounded streaming setup");
    let first = iter.next_async().await;
    assert!(
        matches!(first, Some(Ok(_))),
        "the fixture must yield at least one streamed row, else the producer was \
         never running and this case would be vacuous: {first:?}"
    );
    assert!(
        alive_tasks() > baseline,
        "precondition: a live producer task must exist while the iterator is held \
         (it is parked on the 1-row channel with rows left to send)"
    );

    drop(iter);
    await_tasks_back_to(baseline, "post-drop").await;
}

/// The timeout must be distinguishable from corruption at every layer a consumer
/// switches on (issue #1695: "do NOT let the timeout error be indistinguishable
/// from corruption").
#[test]
fn timeout_error_is_not_a_corruption() {
    let err = Error::QueryTimeout {
        operation: "query.execute".into(),
        elapsed: Duration::from_millis(2),
        limit: Duration::from_millis(1),
    };
    assert_eq!(
        err.obs_category(),
        cqlite_core::observability::ErrorCategory::Timeout,
        "the telemetry taxonomy must give the budget elapse its own bucket"
    );
    assert_ne!(
        err.obs_category(),
        cqlite_core::observability::ErrorCategory::Corruption
    );
    assert_ne!(
        err.category(),
        cqlite_core::error::ErrorCategory::Data,
        "a budget elapse is never a DATA fault"
    );
    assert!(
        !err.is_recoverable(),
        "retrying the same query under the same budget elapses again"
    );
    let text = err.to_string();
    for expected in ["max_execution_time", "query_timeout_ms", "LIMIT"] {
        assert!(
            text.contains(expected),
            "the operator-facing message must name the remedy {expected:?}: {text}"
        );
    }
}

// ---------------------------------------------------------------------------
// Task-liveness helpers (no wall-clock assertions — issue #2642)
// ---------------------------------------------------------------------------

/// Tasks alive on THIS test's own runtime (each `#[tokio::test]` builds its own),
/// so the count is isolated from every other test.
fn alive_tasks() -> usize {
    tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks()
}

/// Wait — with a bounded number of polls, never a wall-clock threshold — for the
/// alive-task count to come back to `baseline`. A producer that never exits is a
/// LEAK and fails the case; the loop bound only stops the test hanging forever.
async fn await_tasks_back_to(baseline: usize, phase: &str) {
    const MAX_POLLS: usize = 2_000;
    for _ in 0..MAX_POLLS {
        if alive_tasks() <= baseline {
            return;
        }
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    panic!(
        "LEAK ({phase}): {} task(s) still alive after {MAX_POLLS} polls (baseline {baseline}) — \
         a producer did not exit when its receiver was dropped, so cancellation is not clean",
        alive_tasks() - baseline
    );
}
