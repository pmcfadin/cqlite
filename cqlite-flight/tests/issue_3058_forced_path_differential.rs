//! Issue #3058 — the FORCED-PATH DIFFERENTIAL: the same bytes, the same ticket,
//! a PINNED `now`, both arms, byte-identical result sets.
//!
//! This is the primary proof for the one gap code-reading cannot settle (design
//! §gap (b)): the k-way merge arm decides row visibility with `entry_to_row`'s
//! `has_live_data_cell` / row-marker liveness rule (`producer.rs`, issues
//! #2374/#2789), while the single-generation fast arm lets the decoder's own
//! `PartitionShadow` + `build_row_from_scan_cached` suppression decide. Only a
//! differential over the SAME bytes can show the two agree.
//!
//! Every case runs `do_get` twice — once under `CQLITE_FLIGHT_MERGE_PATH=merge`
//! and once under `=bypass` — and asserts the two runs return identical rows,
//! identical column values, and identical ORDER. Each pair also asserts, on the
//! `read_path_probe` markers, that the two runs genuinely took DIFFERENT arms:
//! without that, a differential that silently ran the same arm twice would be a
//! vacuous pass.
//!
//! Covered shapes:
//!   * real Cassandra `nb` fixtures from the query-semantics oracle
//!     (`test_compaction_tombstone_ttl`): range tombstone, row deletion,
//!     expired-TTL cell + expired row-liveness marker, surviving live rows;
//!   * the STATIC-column fail-closed fallback, on both a CQLite-written fixture
//!     (including a static-ONLY partition) and the real Cassandra
//!     `test_writeparity.static_clustering_shape`: a static-bearing schema must
//!     take the MERGE arm and return exactly what it returns today (the two arms
//!     genuinely disagree there — see `BypassReason::StaticColumns`);
//!   * a CQLite-written fixture carrying a partition deletion, a range tombstone,
//!     a row deletion, an expired-TTL cell and a live-TTL cell,
//!     read at TWO pinned `now` values so the pinned clock itself is pinned;
//!   * feature parity on the fast arm: predicate pushdown, projection, a token
//!     range, and a `max_batch_bytes`-capped stream.
//!
//! ## Isolation
//!
//! `CQLITE_FLIGHT_MERGE_PATH`, `CQLITE_TTL_NOW_OVERRIDE_SECS` and the probe
//! counters are all PROCESS-GLOBAL, so this file holds exactly ONE `#[test]`
//! that runs every case sequentially — the same discipline
//! `query_semantics_flight_parity.rs` uses. Add a case to the list, not a second
//! `#[test]`.
//!
//! ## Fixture contract
//!
//! ## Residual (stated, not hidden)
//!
//! The gap-(b) shape with NO liveness marker AT ALL — a clustering row written by
//! `UPDATE t SET v=? WHERE pk=? AND ck=?` — is covered here only in its
//! EXPIRED-marker form (`ttl_expired_live`, whose surviving row's visibility comes
//! from a live data cell rather than a live marker, asserted under `SELECT *` AND
//! a PK-only projection). No committed fixture contains a marker-less clustering
//! row, and CQLite's own writer cannot produce one (`merge_row_group` derives
//! liveness from any mutation that writes cells), so that exact byte shape needs a
//! Cassandra-generated fixture — recorded as owed, not silently claimed.
//!
//! Dataset-backed cases SKIP cleanly when the fetched corpus is absent, UNLESS
//! `CQLITE_REQUIRE_FIXTURES=1` (which the gate sets), where an absent fixture is
//! a hard failure. The CQLite-written cases always run. A case that returns ZERO
//! rows where rows are expected is a failure, never a vacuous pass.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::Ticket;
use futures::StreamExt;
use tonic::Request;

use cqlite_core::schema::{ClusteringColumn, Column, KeyColumn, TableSchema};
use cqlite_core::storage::read_path_probe::ReadPathProbe;
use cqlite_core::storage::write_engine::mutation::{
    ClusteringBound, PartitionTombstone, RangeTombstone,
};
use cqlite_core::storage::write_engine::{
    CellOperation, ClusteringKey, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use cqlite_core::util::cassandra_murmur3::cassandra_murmur3_token;
use cqlite_flight::bypass::MERGE_PATH_ENV;
use cqlite_flight::service::CqliteFlightService;

/// Debug-only reader seam pinning the read-time TTL clock (see `now_clock.rs`).
const TTL_NOW_ENV: &str = "CQLITE_TTL_NOW_OVERRIDE_SECS";

/// One row rendered as an ordered `column -> value` map, so a mismatch prints a
/// readable diff and every column participates in the comparison.
type Row = BTreeMap<String, String>;

fn require_fixtures() -> bool {
    std::env::var("CQLITE_REQUIRE_FIXTURES")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate dir has a parent")
        .to_path_buf()
}

/// The fetched dataset root (`CQLITE_DATASETS_ROOT`, else the in-repo default).
fn datasets_root() -> PathBuf {
    std::env::var("CQLITE_DATASETS_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| repo_root().join("test-data/datasets"))
}

/// Resolve `<sstables>/<keyspace>/<table>-<uuid>/`, whose Cassandra-assigned
/// UUID suffix is discovered from the directory listing (never guessed).
fn fixture_dir(keyspace: &str, table: &str) -> Option<PathBuf> {
    let ks_dir = datasets_root().join("sstables").join(keyspace);
    let prefix = format!("{table}-");
    std::fs::read_dir(&ks_dir)
        .ok()?
        .flatten()
        .map(|e| e.path())
        .find(|p| {
            p.is_dir()
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with(&prefix))
        })
}

/// Whether the fixture's `Data.db` binaries were actually fetched.
fn fixture_data_present(dir: &Path) -> bool {
    std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .any(|e| {
            e.file_name()
                .to_str()
                .is_some_and(|n| n.ends_with("-Data.db"))
        })
}

fn ticket_json(keyspace: &str, table: &str, ddl: &str) -> serde_json::Value {
    serde_json::json!({ "keyspace": keyspace, "table": table, "ddl": ddl })
}

/// Drain `do_get` into ordered rows (the emit ORDER is part of the comparison)
/// plus each emitted batch's row count, so a batching-budget case can compare
/// the batch BOUNDARIES across arms too.
// arrow-flight's `FlightError` Err type has a framework-fixed large size; boxing
// it (clippy's suggestion) would break the flight decoder stream API (#2856).
#[allow(clippy::result_large_err)]
async fn do_get_rows_and_batches(
    svc: &CqliteFlightService,
    ticket: &serde_json::Value,
) -> (Vec<Row>, Vec<usize>) {
    let bytes = serde_json::to_vec(ticket).expect("ticket json");
    let resp = svc
        .do_get(Request::new(Ticket::new(bytes)))
        .await
        .expect("do_get")
        .into_inner();
    let mapped = resp.map(|r| r.map_err(|e| FlightError::ExternalError(Box::new(e))));
    let mut stream = FlightRecordBatchStream::new_from_flight_data(mapped);
    let mut rows = Vec::new();
    let mut sizes = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.expect("record batch");
        sizes.push(batch.num_rows());
        push_rows(&batch, &mut rows);
    }
    (rows, sizes)
}

/// Render every column of every row through Arrow's own display formatter, so
/// the comparison covers VALUES (and nullness) for every CQL type without this
/// test hardcoding a per-type downcast.
fn push_rows(batch: &RecordBatch, out: &mut Vec<Row>) {
    let schema = batch.schema();
    let formatters: Vec<_> = batch
        .columns()
        .iter()
        .map(|c| {
            arrow::util::display::ArrayFormatter::try_new(
                c.as_ref(),
                &arrow::util::display::FormatOptions::default(),
            )
            .expect("array formatter")
        })
        .collect();
    for r in 0..batch.num_rows() {
        let mut row = Row::new();
        for (c, field) in schema.fields().iter().enumerate() {
            let rendered = if batch.column(c).is_null(r) {
                "<null>".to_string()
            } else {
                formatters[c].value(r).to_string()
            };
            row.insert(field.name().clone(), rendered);
        }
        out.push(row);
    }
}

/// Run `ticket` under a forced arm, returning its rows, batch sizes and the
/// probe delta that proves which arm actually ran.
async fn run_forced(
    svc: &CqliteFlightService,
    ticket: &serde_json::Value,
    arm: &str,
) -> (Vec<Row>, Vec<usize>, ReadPathProbe) {
    std::env::set_var(MERGE_PATH_ENV, arm);
    let before = ReadPathProbe::snapshot();
    let (rows, sizes) = do_get_rows_and_batches(svc, ticket).await;
    let delta = ReadPathProbe::snapshot().delta_since(&before);
    std::env::remove_var(MERGE_PATH_ENV);
    (rows, sizes, delta)
}

/// The differential itself: run `ticket` on BOTH arms at `pinned_now` and assert
/// identical rows/values/order — and that the arms really differed.
///
/// `expect_rows` is the minimum number of rows the case must return; a case that
/// silently returns nothing cannot pass (anti-vacuity).
async fn assert_arms_agree(
    label: &str,
    svc: &CqliteFlightService,
    ticket: &serde_json::Value,
    pinned_now: i64,
    expect_rows: usize,
    failures: &mut Vec<String>,
) -> Option<Vec<Row>> {
    std::env::set_var(TTL_NOW_ENV, pinned_now.to_string());
    let (merge_rows, merge_sizes, merge_delta) = run_forced(svc, ticket, "merge").await;
    let (bypass_rows, bypass_sizes, bypass_delta) = run_forced(svc, ticket, "bypass").await;
    std::env::remove_var(TTL_NOW_ENV);

    if merge_rows != bypass_rows {
        failures.push(format!(
            "case {label}: FORCED-PATH DIFFERENTIAL MISMATCH at pinned now {pinned_now}\n  \
             merge  ({} rows): {:#?}\n  bypass ({} rows): {:#?}",
            merge_rows.len(),
            merge_rows,
            bypass_rows.len(),
            bypass_rows,
        ));
        return None;
    }
    if merge_rows.len() < expect_rows {
        failures.push(format!(
            "case {label}: expected at least {expect_rows} rows on BOTH arms, got {} \
             — a zero/short result is a failure, never a vacuous pass",
            merge_rows.len()
        ));
        return None;
    }
    if merge_delta.mergers_built == 0 || merge_delta.reconcile_entries == 0 {
        failures.push(format!(
            "case {label}: the forced-merge run did not actually take the merge arm \
             (mergers={}, reconciles={}) — the differential would be vacuous",
            merge_delta.mergers_built, merge_delta.reconcile_entries
        ));
        return None;
    }
    if bypass_delta.mergers_built != 0 || bypass_delta.reconcile_entries != 0 {
        failures.push(format!(
            "case {label}: the forced-bypass run still merged (mergers={}, \
             reconciles={}) — the differential compared the same arm twice",
            bypass_delta.mergers_built, bypass_delta.reconcile_entries
        ));
        return None;
    }
    if merge_sizes.iter().sum::<usize>() != bypass_sizes.iter().sum::<usize>() {
        failures.push(format!(
            "case {label}: batch row totals differ: merge {merge_sizes:?} vs bypass \
             {bypass_sizes:?}"
        ));
        return None;
    }
    eprintln!(
        "PASS {label} — {} rows identical on both arms (pinned now {pinned_now})",
        merge_rows.len()
    );
    Some(bypass_rows)
}

/// A static-bearing schema must fall back to the merge arm under BOTH forced
/// values, returning identical rows — i.e. the fast path cannot change those
/// results because it is never taken for them (fail-closed, see
/// `BypassReason::StaticColumns`). Also asserts the fallback returns a non-empty
/// result, so an empty read cannot make this pass vacuously.
async fn assert_static_falls_back(
    label: &str,
    svc: &CqliteFlightService,
    ticket: &serde_json::Value,
    pinned_now: i64,
    failures: &mut Vec<String>,
) {
    std::env::set_var(TTL_NOW_ENV, pinned_now.to_string());
    let (merge_rows, _, merge_delta) = run_forced(svc, ticket, "merge").await;
    let (bypass_rows, _, bypass_delta) = run_forced(svc, ticket, "bypass").await;
    std::env::remove_var(TTL_NOW_ENV);

    if bypass_delta.mergers_built == 0 || bypass_delta.reconcile_entries == 0 {
        failures.push(format!(
            "case {label}: a STATIC-column schema must fall back to the merge arm \
             even under CQLITE_FLIGHT_MERGE_PATH=bypass (mergers={}, reconciles={})",
            bypass_delta.mergers_built, bypass_delta.reconcile_entries
        ));
    }
    if merge_delta.mergers_built == 0 {
        failures.push(format!("case {label}: the forced-merge run did not merge"));
    }
    if merge_rows != bypass_rows {
        failures.push(format!(
            "case {label}: the static fallback must be behaviour-identical under both \
             forced values\n  merge: {merge_rows:#?}\n  bypass: {bypass_rows:#?}"
        ));
    }
    if merge_rows.is_empty() {
        failures.push(format!(
            "case {label}: the static fixture returned NO rows — a vacuous pass"
        ));
    }
    eprintln!(
        "PASS {label} — static schema fell back to the merge arm ({} rows, unchanged)",
        merge_rows.len()
    );
}

// ---------------------------------------------------------------------------
// CQLite-written tombstone/TTL fixture
// ---------------------------------------------------------------------------

const KS: &str = "diff_ks";
const TBL: &str = "shapes";
const DDL: &str =
    "CREATE TABLE diff_ks.shapes (pk int, ck int, v text, w text, PRIMARY KEY (pk, ck))";

fn shapes_schema() -> TableSchema {
    TableSchema {
        keyspace: KS.into(),
        table: TBL.into(),
        partition_keys: vec![KeyColumn {
            name: "pk".into(),
            data_type: "int".into(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".into(),
            data_type: "int".into(),
            position: 0,
            order: Default::default(),
        }],
        columns: vec![
            col("pk", "int", false),
            col("ck", "int", false),
            col("v", "text", true),
            col("w", "text", true),
        ],
        comments: Default::default(),
        dropped_columns: Default::default(),
    }
}

fn col(name: &str, ty: &str, nullable: bool) -> Column {
    Column {
        name: name.into(),
        data_type: ty.into(),
        nullable,
        default: None,
        is_static: false,
    }
}

fn base(pk: i32, ck: i32, ops: Vec<CellOperation>, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("pk", Value::Integer(pk)),
        Some(ClusteringKey::single("ck", Value::Integer(ck))),
        ops,
        ts,
        None,
    )
}

fn write_v(pk: i32, ck: i32, v: &str, ts: i64) -> Mutation {
    base(
        pk,
        ck,
        vec![CellOperation::Write {
            column: "v".into(),
            value: Value::text(v),
        }],
        ts,
    )
}

/// `v` live, `w` written with a TTL that expires at `write_secs + ttl`.
fn write_v_and_ttl_w(pk: i32, ck: i32, v: &str, w: &str, ts: i64, ttl: u32, ldt: i32) -> Mutation {
    base(
        pk,
        ck,
        vec![
            CellOperation::Write {
                column: "v".into(),
                value: Value::text(v),
            },
            CellOperation::WriteWithTtl {
                column: "w".into(),
                value: Value::text(w),
                ttl_seconds: ttl,
                local_deletion_time: Some(ldt),
            },
        ],
        ts,
    )
}

/// Base write timestamp (micros) and the derived pinned `now` values. Both
/// pinned instants are CONSTANTS, never a wall-clock read (issue #2642): the
/// fixture's TTL local-deletion-times are stamped explicitly, so the expiry
/// decision is a pure function of these constants.
const T_BASE_SECS: i64 = 1_700_000_000;
const T_BASE_MICROS: i64 = T_BASE_SECS * 1_000_000;
/// Before the TTL cell expires.
const NOW_BEFORE_EXPIRY: i64 = T_BASE_SECS + 100;
/// After it expires (its LDT is `T_BASE_SECS + 600`).
const NOW_AFTER_EXPIRY: i64 = T_BASE_SECS + 1_000;
const TTL_LDT: i32 = (T_BASE_SECS + 600) as i32;

/// One SSTable holding: a live row, a row with a live-TTL cell, a cell
/// tombstone, a whole-row deletion, a range tombstone and a partition deletion.
async fn build_shapes_fixture() -> (tempfile::TempDir, PathBuf) {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");
    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, shapes_schema());
    let mut engine = WriteEngine::new(config).expect("engine");

    // pk=1: a plain live row, a row whose `w` carries a TTL, a row whose `w` was
    // deleted (cell tombstone), and a row deleted outright (row tombstone).
    engine.write(write_v(1, 1, "live", T_BASE_MICROS)).unwrap();
    engine
        .write(write_v_and_ttl_w(
            1,
            2,
            "ttl-row",
            "expires",
            T_BASE_MICROS,
            600,
            TTL_LDT,
        ))
        .unwrap();
    // ck=3: two live columns (the multi-column shape). A CQLite-WRITTEN simple
    // cell tombstone is deliberately NOT exercised here: both arms surface it as
    // a raw `Value::Tombstone` that the Arrow encoder then rejects
    // ("expected Text value, got Tombstone(..)"). That reproduces with
    // `CQLITE_FLIGHT_MERGE_PATH=merge`, i.e. on the pre-#3058 merge path, so it
    // is a PRE-EXISTING defect of the CQLite write/read round-trip, identical on
    // both arms and out of this change's scope — recorded for a follow-up rather
    // than papered over here. Cassandra-written tombstones ARE covered, by the
    // dataset cases below and by the query-semantics oracle.
    engine
        .write(base(
            1,
            3,
            vec![
                CellOperation::Write {
                    column: "v".into(),
                    value: Value::text("two-column"),
                },
                CellOperation::Write {
                    column: "w".into(),
                    value: Value::text("also-live"),
                },
            ],
            T_BASE_MICROS,
        ))
        .unwrap();
    engine
        .write(write_v(1, 4, "doomed", T_BASE_MICROS))
        .unwrap();
    engine
        .write(base(
            1,
            4,
            vec![CellOperation::DeleteRow],
            T_BASE_MICROS + 10,
        ))
        .unwrap();

    // pk=2: rows covered by a RANGE tombstone plus one outside it.
    engine
        .write(write_v(2, 10, "rt-covered", T_BASE_MICROS))
        .unwrap();
    engine
        .write(write_v(2, 11, "rt-covered", T_BASE_MICROS))
        .unwrap();
    engine
        .write(write_v(2, 99, "rt-survivor", T_BASE_MICROS))
        .unwrap();
    let mut rt = Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("pk", Value::Integer(2)),
        None,
        vec![],
        T_BASE_MICROS + 10,
        None,
    );
    rt.range_tombstones = vec![RangeTombstone {
        start: ClusteringBound::Inclusive(ClusteringKey::single("ck", Value::Integer(10))),
        end: ClusteringBound::Inclusive(ClusteringKey::single("ck", Value::Integer(11))),
        deletion_time: T_BASE_MICROS + 10,
        local_deletion_time: T_BASE_SECS as i32,
    }];
    engine.write(rt).unwrap();

    // pk=3: entirely covered by a PARTITION deletion.
    engine.write(write_v(3, 1, "gone", T_BASE_MICROS)).unwrap();
    let mut pt = Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("pk", Value::Integer(3)),
        None,
        vec![],
        T_BASE_MICROS + 10,
        None,
    );
    pt.partition_tombstone = Some(PartitionTombstone {
        deletion_time: T_BASE_MICROS + 10,
        local_deletion_time: T_BASE_SECS as i32,
    });
    engine.write(pt).unwrap();

    engine.flush().await.expect("flush").expect("flush info");
    (temp, data_dir)
}

const STATIC_TBL: &str = "statics";
const STATIC_DDL: &str = "CREATE TABLE diff_ks.statics \
     (pk int, ck int, s text static, v text, PRIMARY KEY (pk, ck))";

fn statics_schema() -> TableSchema {
    let mut s = col("s", "text", true);
    s.is_static = true;
    TableSchema {
        keyspace: KS.into(),
        table: STATIC_TBL.into(),
        partition_keys: vec![KeyColumn {
            name: "pk".into(),
            data_type: "int".into(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".into(),
            data_type: "int".into(),
            position: 0,
            order: Default::default(),
        }],
        columns: vec![
            col("pk", "int", false),
            col("ck", "int", false),
            s,
            col("v", "text", true),
        ],
        comments: Default::default(),
        dropped_columns: Default::default(),
    }
}

/// One SSTable with (a) a partition holding a static cell AND clustering rows,
/// and (b) a partition holding ONLY a static cell (no clustering row).
async fn build_statics_fixture() -> (tempfile::TempDir, PathBuf) {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");
    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, statics_schema());
    let mut engine = WriteEngine::new(config).expect("engine");
    let static_write = |pk: i32, val: &str| {
        Mutation::new(
            TableId::new(KS, STATIC_TBL),
            PartitionKey::single("pk", Value::Integer(pk)),
            None,
            vec![CellOperation::Write {
                column: "s".into(),
                value: Value::text(val),
            }],
            T_BASE_MICROS,
            None,
        )
    };
    let row_write = |pk: i32, ck: i32, v: &str| {
        Mutation::new(
            TableId::new(KS, STATIC_TBL),
            PartitionKey::single("pk", Value::Integer(pk)),
            Some(ClusteringKey::single("ck", Value::Integer(ck))),
            vec![CellOperation::Write {
                column: "v".into(),
                value: Value::text(v),
            }],
            T_BASE_MICROS,
            None,
        )
    };
    engine.write(static_write(1, "s1")).unwrap();
    engine.write(row_write(1, 1, "v11")).unwrap();
    engine.write(row_write(1, 2, "v12")).unwrap();
    engine.write(static_write(2, "s2-only")).unwrap();
    engine.flush().await.expect("flush").expect("flush info");
    (temp, data_dir)
}

/// The whole differential (one test — see the module doc's isolation note).
#[tokio::test]
async fn forced_path_differential_agrees_on_every_shape() {
    let mut failures: Vec<String> = Vec::new();

    // ---- CQLite-written tombstone/TTL/range/partition shapes ---------------
    let (_temp, shapes_dir) = build_shapes_fixture().await;
    let svc = CqliteFlightService::new(shapes_dir.clone(), 8192);
    let full = ticket_json(KS, TBL, DDL);

    // (1) Every reconciliation shape, at a `now` BEFORE the TTL cell expires.
    let before = assert_arms_agree(
        "shapes/select-star@before-expiry",
        &svc,
        &full,
        NOW_BEFORE_EXPIRY,
        1,
        &mut failures,
    )
    .await;
    // (2) The SAME bytes at a `now` AFTER it expires — pinning that the pinned
    //     clock reaches the fast path's shadow decoder rather than the wall clock.
    let after = assert_arms_agree(
        "shapes/select-star@after-expiry",
        &svc,
        &full,
        NOW_AFTER_EXPIRY,
        1,
        &mut failures,
    )
    .await;
    if let (Some(before), Some(after)) = (before.as_ref(), after.as_ref()) {
        let w_before = cell(before, "ck", "2", "w");
        let w_after = cell(after, "ck", "2", "w");
        if w_before.as_deref() == Some("<null>") || w_before.is_none() {
            failures.push(format!(
                "the live-TTL cell must be PRESENT at the earlier pinned now, got {w_before:?}"
            ));
        }
        if w_after.as_deref() != Some("<null>") {
            failures.push(format!(
                "the TTL cell must be EXPIRED at the later pinned now (the fast path \
                 read the request's now, not the wall clock), got {w_after:?}"
            ));
        }
        // Row/partition/range/cell tombstone shapes, asserted on the bypass output
        // (already proven identical to the merge arm above).
        assert_semantics(after, &mut failures);
    }

    // (3) Predicate pushdown + projection + a token range on the fast arm.
    let mut projected = ticket_json(KS, TBL, DDL);
    projected["columns"] = serde_json::json!(["pk", "ck"]);
    projected["predicates"] =
        serde_json::json!([{ "column": "v", "op": "Equal", "value": "live" }]);
    let _ = assert_arms_agree(
        "shapes/predicate+projection",
        &svc,
        &projected,
        NOW_BEFORE_EXPIRY,
        1,
        &mut failures,
    )
    .await;

    // A token range derived from the fixture's REAL tokens: `(t1 - 1, t1]` holds
    // exactly partition pk=1, whose three surviving clustering rows give the case
    // a non-zero floor. Deriving the range (rather than guessing a half-ring)
    // is what stops this from passing vacuously when the range holds nothing —
    // and `assert_arms_agree` additionally requires the merge run to have really
    // merged and the bypass run to have really bypassed.
    let t1 = cassandra_murmur3_token(&1_i32.to_be_bytes());
    let mut token_scoped = ticket_json(KS, TBL, DDL);
    token_scoped["token_start"] = serde_json::json!(t1.saturating_sub(1));
    token_scoped["token_end"] = serde_json::json!(t1);
    let _ = assert_arms_agree(
        "shapes/token-range(single partition)",
        &svc,
        &token_scoped,
        NOW_BEFORE_EXPIRY,
        3,
        &mut failures,
    )
    .await;

    // (4) A byte-capped stream: the cap must hold on the fast arm and the rows
    //     must still match the merge arm's.
    let capped = CqliteFlightService::new(shapes_dir.clone(), 8192).with_max_batch_bytes(512);
    let _ = assert_arms_agree(
        "shapes/max-batch-bytes",
        &capped,
        &full,
        NOW_BEFORE_EXPIRY,
        1,
        &mut failures,
    )
    .await;

    // ---- Static columns: the fail-closed fallback --------------------------
    // A static-bearing schema must NOT take the fast path: the arms disagree on
    // static-row shape (the merge arm emits a `ck = null` static row and injects
    // nothing; the single-generation decoder injects statics into the clustering
    // rows but drops a static-ONLY partition entirely). Both are wrong in
    // DIFFERENT ways versus Cassandra, so this change keeps today's behaviour
    // exactly and the divergence is a documented follow-up.
    let (_stemp, statics_dir) = build_statics_fixture().await;
    let ssvc = CqliteFlightService::new(statics_dir, 8192);
    let sticket = ticket_json(KS, STATIC_TBL, STATIC_DDL);
    assert_static_falls_back(
        "statics/select-star",
        &ssvc,
        &sticket,
        NOW_BEFORE_EXPIRY,
        &mut failures,
    )
    .await;

    // ---- Real Cassandra fixtures ------------------------------------------
    for case in dataset_cases() {
        let Some(dir) = fixture_dir(case.keyspace, case.table) else {
            handle_missing(&case, "fixture dir absent", &mut failures);
            continue;
        };
        if !fixture_data_present(&dir) {
            handle_missing(&case, "Data.db not fetched", &mut failures);
            continue;
        }
        let root = datasets_root().join("sstables");
        let svc = CqliteFlightService::new(root, 8192);
        let mut ticket = ticket_json(case.keyspace, case.table, &case.ddl);
        if !case.columns.is_empty() {
            ticket["columns"] = serde_json::json!(case.columns);
        }
        if case.static_fallback {
            assert_static_falls_back(case.label, &svc, &ticket, case.pinned_now, &mut failures)
                .await;
            continue;
        }
        let _ = assert_arms_agree(
            case.label,
            &svc,
            &ticket,
            case.pinned_now,
            case.min_rows,
            &mut failures,
        )
        .await;

        // The gap-(b) shape read under a PK-ONLY projection too: a row whose
        // visibility comes from a live data cell rather than a liveness marker
        // must survive a projection that drops that very cell.
        if case.pk_only_projection.is_empty() {
            continue;
        }
        let mut pk_only = ticket.clone();
        pk_only["columns"] = serde_json::json!(case.pk_only_projection);
        let _ = assert_arms_agree(
            case.pk_only_label,
            &svc,
            &pk_only,
            case.pinned_now,
            case.min_rows,
            &mut failures,
        )
        .await;
    }

    assert!(
        failures.is_empty(),
        "forced-path differential failures:\n{}",
        failures.join("\n\n")
    );
}

/// A dataset-backed differential case.
struct DatasetCase {
    label: &'static str,
    pk_only_label: &'static str,
    keyspace: &'static str,
    table: &'static str,
    ddl: String,
    pinned_now: i64,
    min_rows: usize,
    pk_only_projection: Vec<&'static str>,
    /// `true` for a schema the predicate refuses (a STATIC column): the case
    /// asserts the fail-closed FALLBACK instead of an arm differential.
    static_fallback: bool,
    /// Optional projection for the MAIN case (empty = `SELECT *`).
    columns: Vec<&'static str>,
}

/// The pinned `now` the query-semantics oracle records for the
/// `test_compaction_tombstone_ttl` fixtures.
const ORACLE_PINNED_NOW: i64 = 1_782_950_400;

fn dataset_cases() -> Vec<DatasetCase> {
    let tombstone_ddl = "CREATE TABLE test_compaction_tombstone_ttl.{TBL} \
         (id int, ck int, v text, PRIMARY KEY (id, ck))";
    vec![
        DatasetCase {
            label: "cassandra/rt_cross_gen",
            pk_only_label: "cassandra/rt_cross_gen@pk-only",
            keyspace: "test_compaction_tombstone_ttl",
            table: "rt_cross_gen",
            ddl: tombstone_ddl.replace("{TBL}", "rt_cross_gen"),
            pinned_now: ORACLE_PINNED_NOW,
            min_rows: 2,
            pk_only_projection: vec!["id", "ck"],
            static_fallback: false,
            columns: vec![],
        },
        DatasetCase {
            label: "cassandra/ttl_expired_live",
            pk_only_label: "cassandra/ttl_expired_live@pk-only",
            keyspace: "test_compaction_tombstone_ttl",
            table: "ttl_expired_live",
            ddl: tombstone_ddl.replace("{TBL}", "ttl_expired_live"),
            pinned_now: ORACLE_PINNED_NOW,
            min_rows: 1,
            pk_only_projection: vec!["id", "ck"],
            static_fallback: false,
            columns: vec![],
        },
        DatasetCase {
            label: "cassandra/shadow_row_delete",
            pk_only_label: "cassandra/shadow_row_delete@pk-only",
            keyspace: "test_compaction_tombstone_ttl",
            table: "shadow_row_delete",
            ddl: tombstone_ddl.replace("{TBL}", "shadow_row_delete"),
            pinned_now: ORACLE_PINNED_NOW,
            min_rows: 3,
            pk_only_projection: vec!["id", "ck"],
            static_fallback: false,
            columns: vec![],
        },
        // REAL Cassandra static shape: `sdata` was written by
        // `UPDATE static_clustering_shape SET sdata='static-val' WHERE id=1`
        // alongside an INSERTed clustering row. Its schema declares a STATIC
        // column, so the predicate REFUSES the fast path (the arms disagree on
        // static-row shape) — this case pins that fail-closed fallback on real
        // Cassandra bytes.
        // Spec R7: a `frozen<UDT>` INSIDE a collection must still decode
        // structurally on the fast arm — the warm reader's resolved UDT registry
        // is threaded identically, so the arms must agree cell-for-cell.
        DatasetCase {
            label: "cassandra/collections_with_udts(frozen UDT in collection)",
            pk_only_label: "cassandra/collections_with_udts@pk-only",
            keyspace: "test_collections",
            table: "collections_with_udts",
            // The ticket DDL is parsed as ONE `CREATE TABLE`
            // (`service::parse_schema` -> `parse_cql_schema`), while the UDT
            // registry is resolved by scanning the SAME string for `CREATE TYPE`
            // (`udt_registry_from_cql`) — so the table statement comes FIRST and
            // the type statements follow it.
            ddl: [
                "CREATE TABLE collections_with_udts (user_id uuid PRIMARY KEY, addresses list<frozen<address_type>>, contacts set<frozen<contact_info>>, locations_visited map<date, frozen<address_type>>, emergency_contacts map<text, frozen<contact_info>>);",
                "CREATE TYPE address_type (street text, city text, state text, zip_code text, country text);",
                "CREATE TYPE contact_info (email text, phone text, address frozen<address_type>);",
            ]
            .join(" "),
            pinned_now: ORACLE_PINNED_NOW,
            min_rows: 1,
            pk_only_projection: vec!["user_id"],
            static_fallback: false,
            // `contacts` / `emergency_contacts` are composite-keyed collections
            // of frozen UDTs, which the MERGED-read assembler fails closed on
            // (issue #2339) — so they cannot be part of an arm-vs-arm comparison.
            // `addresses` is `list<frozen<address_type>>`, the frozen-UDT-inside-a
            // -collection shape this case exists to compare.
            columns: vec!["user_id", "addresses"],
        },
        DatasetCase {
            label: "cassandra/static_clustering_shape(fail-closed static fallback)",
            pk_only_label: "cassandra/static_clustering_shape@pk-only",
            keyspace: "test_writeparity",
            table: "static_clustering_shape",
            ddl: "CREATE TABLE test_writeparity.static_clustering_shape \
                  (id int, ck int, sdata text static, rdata text, PRIMARY KEY (id, ck))"
                .to_string(),
            pinned_now: ORACLE_PINNED_NOW,
            min_rows: 1,
            pk_only_projection: vec![],
            static_fallback: true,
            columns: vec![],
        },
    ]
}

fn handle_missing(case: &DatasetCase, why: &str, failures: &mut Vec<String>) {
    let msg = format!("case {}: {why}", case.label);
    if require_fixtures() {
        failures.push(format!("REQUIRE_FIXTURES: {msg}"));
    } else {
        eprintln!("SKIP {msg}");
    }
}

/// The value of `column` in the row whose `key_col` renders as `key_val`.
fn cell(rows: &[Row], key_col: &str, key_val: &str, column: &str) -> Option<String> {
    rows.iter()
        .find(|r| r.get(key_col).map(String::as_str) == Some(key_val))
        .and_then(|r| r.get(column).cloned())
}

/// The tombstone semantics the fixture encodes, asserted on the (already
/// arm-identical) output so a shared misreading of the fixture cannot pass.
fn assert_semantics(rows: &[Row], failures: &mut Vec<String>) {
    let pks: Vec<&str> = rows
        .iter()
        .filter_map(|r| r.get("pk").map(String::as_str))
        .collect();
    if pks.contains(&"3") {
        failures.push("a partition-deleted partition must not surface any row".into());
    }
    let cks: Vec<&str> = rows
        .iter()
        .filter_map(|r| r.get("ck").map(String::as_str))
        .collect();
    if cks.contains(&"4") {
        failures.push("a row-deleted row must not surface".into());
    }
    if cks.contains(&"10") || cks.contains(&"11") {
        failures.push("range-tombstoned rows must not surface".into());
    }
    if !cks.contains(&"99") {
        failures.push("the row outside the range tombstone must survive".into());
    }
    if cell(rows, "ck", "3", "v").as_deref() != Some("two-column") {
        failures.push("a live multi-column row must surface both of its cells".into());
    }
    if cell(rows, "ck", "3", "w").as_deref() != Some("also-live") {
        failures.push("a live multi-column row must surface both of its cells".into());
    }
}
