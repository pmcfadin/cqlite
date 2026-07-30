//! Issue #3094 — a DELETED CELL must read as NULL, on both Flight read arms.
//!
//! A simple cell tombstone (`CellOperation::Delete` on one regular column of an
//! otherwise-live clustering row) used to survive the read path as a raw
//! `Value::Tombstone(CellTombstone)` in the row carrier. The Arrow encoder then
//! (correctly, #1485) refused to coerce it and `do_get` failed the WHOLE stream
//! with `column 'w': expected Text value, got Tombstone(..)`.
//!
//! The correct semantics are Cassandra's: a deleted cell is ABSENT from the
//! reconciled row, so `SELECT *` renders it NULL. That is a read-time
//! RECONCILIATION property (not an on-disk framing property), so a
//! CQLite-written fixture is a legitimate oracle here (see #3042's
//! symmetric-round-trip caveat: framing/encoding properties would NOT be).
//!
//! The Cassandra-WRITTEN side of the same shape is pinned separately, against a
//! real Apache Cassandra 5.0 `nb` fixture, by the query-semantics oracle case
//! `static_with_tombstones__deleted_cell_null`
//! (`test-data/query-semantics-oracle.json`), which runs on both the in-core
//! lane and the Flight lane. That fixture's committed sstabledump golden
//! ENUMERATES the tombstone, which is precisely why a physical-dump oracle
//! cannot catch this bug and a semantic one must.
//!
//! Both arms are exercised at a PINNED `now`: `CQLITE_FLIGHT_MERGE_PATH=merge`
//! (the k-way merge path) and `=bypass` (the #3058 single-generation fast path).
//! Neither may error, and both must render the deleted column NULL while the
//! sibling live column keeps its value.
//!
//! ## Isolation
//!
//! `CQLITE_FLIGHT_MERGE_PATH` and `CQLITE_TTL_NOW_OVERRIDE_SECS` are
//! PROCESS-GLOBAL, so this file holds exactly ONE `#[test]` that runs every case
//! sequentially — the same discipline `issue_3058_forced_path_differential.rs`
//! uses. Add a case to the list, not a second `#[test]`.

use std::collections::BTreeMap;
use std::path::PathBuf;

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
use cqlite_core::storage::write_engine::{
    CellOperation, ClusteringKey, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use cqlite_flight::bypass::MERGE_PATH_ENV;
use cqlite_flight::service::CqliteFlightService;

const TTL_NOW_ENV: &str = "CQLITE_TTL_NOW_OVERRIDE_SECS";

/// Pinned write timestamp / read clock — CONSTANTS, never a wall-clock read
/// (#2642). Nothing in this fixture carries a TTL, so the pin exists purely to
/// make the read deterministic.
const T_BASE_SECS: i64 = 1_700_000_000;
const T_BASE_MICROS: i64 = T_BASE_SECS * 1_000_000;
const PINNED_NOW: i64 = T_BASE_SECS + 100;

const KS: &str = "tomb_ks";
const TBL: &str = "deleted_cell";
const DDL: &str =
    "CREATE TABLE tomb_ks.deleted_cell (pk int, ck int, v text, w text, PRIMARY KEY (pk, ck))";

/// One row rendered as an ordered `column -> value` map.
type Row = BTreeMap<String, String>;

fn col(name: &str, ty: &str, nullable: bool) -> Column {
    Column {
        name: name.into(),
        data_type: ty.into(),
        nullable,
        default: None,
        is_static: false,
    }
}

fn schema() -> TableSchema {
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

fn mutation(pk: i32, ck: i32, ops: Vec<CellOperation>, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("pk", Value::Integer(pk)),
        Some(ClusteringKey::single("ck", Value::Integer(ck))),
        ops,
        ts,
        None,
    )
}

/// A SINGLE SSTable holding one live row (`ck=1`, both columns live) and one row
/// whose `w` was deleted by a later-timestamp cell tombstone (`ck=2`).
async fn build_fixture() -> (tempfile::TempDir, PathBuf) {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");
    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, schema());
    let mut engine = WriteEngine::new(config).expect("engine");

    let both_live = |ck: i32, ts: i64| {
        mutation(
            1,
            ck,
            vec![
                CellOperation::Write {
                    column: "v".into(),
                    value: Value::text("v-live"),
                },
                CellOperation::Write {
                    column: "w".into(),
                    value: Value::text("w-doomed"),
                },
            ],
            ts,
        )
    };
    // ck=1: control row — nothing is deleted.
    engine.write(both_live(1, T_BASE_MICROS)).expect("write");
    // ck=2: `w` is deleted by a strictly-later cell tombstone; `v` stays live.
    engine.write(both_live(2, T_BASE_MICROS)).expect("write");
    engine
        .write(mutation(
            1,
            2,
            vec![CellOperation::Delete {
                column: "w".into(),
                local_deletion_time: Some(T_BASE_SECS as i32),
            }],
            T_BASE_MICROS + 10,
        ))
        .expect("write delete");

    engine.flush().await.expect("flush").expect("flush info");
    (temp, data_dir)
}

fn ticket_json(keyspace: &str, table: &str, ddl: &str) -> serde_json::Value {
    serde_json::json!({ "keyspace": keyspace, "table": table, "ddl": ddl })
}

/// Render every column through Arrow's own display formatter, so nullness and
/// values are compared without a per-type downcast in this test.
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

/// Drain `do_get`, returning the terminal error message instead of panicking —
/// the pre-fix behaviour of this shape was exactly such a terminal error, so it
/// has to be observable rather than a panic.
// arrow-flight's `FlightError` Err type has a framework-fixed large size; boxing
// it (clippy's suggestion) would break the flight decoder stream API (#2856).
#[allow(clippy::result_large_err)]
async fn do_get_outcome(
    svc: &CqliteFlightService,
    ticket: &serde_json::Value,
) -> Result<Vec<Row>, String> {
    let bytes = serde_json::to_vec(ticket).expect("ticket json");
    let resp = match svc.do_get(Request::new(Ticket::new(bytes))).await {
        Ok(r) => r.into_inner(),
        Err(status) => return Err(format!("do_get rpc: {}", status.message())),
    };
    let mapped = resp.map(|r| r.map_err(|e| FlightError::ExternalError(Box::new(e))));
    let mut stream = FlightRecordBatchStream::new_from_flight_data(mapped);
    let mut rows = Vec::new();
    while let Some(batch) = stream.next().await {
        match batch {
            Ok(batch) => push_rows(&batch, &mut rows),
            Err(e) => return Err(format!("stream: {e}")),
        }
    }
    Ok(rows)
}

/// Run one arm at the pinned `now`, returning its outcome plus the probe delta
/// that proves WHICH arm actually ran (without it a "both arms" claim could be
/// the same arm twice).
async fn run_arm(
    svc: &CqliteFlightService,
    ticket: &serde_json::Value,
    arm: &str,
) -> (Result<Vec<Row>, String>, ReadPathProbe) {
    std::env::set_var(TTL_NOW_ENV, PINNED_NOW.to_string());
    std::env::set_var(MERGE_PATH_ENV, arm);
    let before = ReadPathProbe::snapshot();
    let outcome = do_get_outcome(svc, ticket).await;
    let delta = ReadPathProbe::snapshot().delta_since(&before);
    std::env::remove_var(MERGE_PATH_ENV);
    std::env::remove_var(TTL_NOW_ENV);
    (outcome, delta)
}

/// Locate the row with `ck == want_ck` and return its rendered `column`.
fn cell(rows: &[Row], want_ck: &str, column: &str) -> Option<String> {
    rows.iter()
        .find(|r| r.get("ck").map(String::as_str) == Some(want_ck))
        .and_then(|r| r.get(column).cloned())
}

/// Assert one arm's outcome: the request SUCCEEDS, the deleted cell renders
/// NULL, the sibling live cell keeps its value, and the control row is intact.
fn assert_deleted_cell_is_null(
    label: &str,
    outcome: &Result<Vec<Row>, String>,
    failures: &mut Vec<String>,
) {
    let rows = match outcome {
        Ok(rows) => rows,
        Err(msg) => {
            failures.push(format!(
                "case {label}: do_get FAILED — a deleted cell must read as NULL, never \
                 error the stream (issue #3094): {msg}"
            ));
            return;
        }
    };
    // Anti-vacuity: the fixture has two rows; zero rows would pass every
    // null-assertion below trivially.
    if rows.len() != 2 {
        failures.push(format!(
            "case {label}: expected exactly 2 rows (ck=1 control, ck=2 deleted-cell), \
             got {} — {rows:#?}",
            rows.len()
        ));
        return;
    }
    // The control row proves the fixture/read really carries both columns, so
    // the NULL below is the tombstone's effect and not a blanket loss of `w`.
    if cell(rows, "1", "v").as_deref() != Some("v-live") {
        failures.push(format!(
            "case {label}: the control row's live `v` must survive, got {:?}",
            cell(rows, "1", "v")
        ));
    }
    if cell(rows, "1", "w").as_deref() != Some("w-doomed") {
        failures.push(format!(
            "case {label}: the control row's live `w` must survive, got {:?}",
            cell(rows, "1", "w")
        ));
    }
    // The row whose `w` was deleted: `v` live, `w` NULL.
    if cell(rows, "2", "v").as_deref() != Some("v-live") {
        failures.push(format!(
            "case {label}: the deleted-cell row's sibling `v` must stay live, got {:?}",
            cell(rows, "2", "v")
        ));
    }
    match cell(rows, "2", "w").as_deref() {
        Some("<null>") => {}
        other => failures.push(format!(
            "case {label}: the DELETED cell `w` must render NULL, got {other:?} \
             (issue #3094)"
        )),
    }
}

#[tokio::test]
async fn deleted_cell_reads_null_on_both_flight_arms() {
    let mut failures: Vec<String> = Vec::new();

    let (_temp, data_dir) = build_fixture().await;
    let svc = CqliteFlightService::new(data_dir, 8192);
    let ticket = ticket_json(KS, TBL, DDL);

    // ---- the merge arm (pre-#3058 k-way merge path) ------------------------
    let (merge_outcome, merge_delta) = run_arm(&svc, &ticket, "merge").await;
    if merge_delta.mergers_built == 0 {
        failures.push(
            "the forced-merge run did not take the merge arm (mergers=0) — the \
             'both arms' claim would be the same arm twice"
                .to_string(),
        );
    }
    assert_deleted_cell_is_null("cqlite-written/merge", &merge_outcome, &mut failures);

    // ---- the bypass arm (#3058 single-generation fast path) ----------------
    let (bypass_outcome, bypass_delta) = run_arm(&svc, &ticket, "bypass").await;
    if bypass_delta.mergers_built != 0 {
        failures.push(format!(
            "the forced-bypass run still merged (mergers={}) — the 'both arms' claim \
             would be the same arm twice",
            bypass_delta.mergers_built
        ));
    }
    assert_deleted_cell_is_null("cqlite-written/bypass", &bypass_outcome, &mut failures);

    // Both arms must agree exactly (same bytes, same ticket, same pinned now).
    if let (Ok(m), Ok(b)) = (&merge_outcome, &bypass_outcome) {
        if m != b {
            failures.push(format!(
                "the two arms disagree on the deleted-cell shape\n  merge: {m:#?}\n  \
                 bypass: {b:#?}"
            ));
        }
    }

    assert!(
        failures.is_empty(),
        "issue #3094 cell-tombstone-null failures:\n{}",
        failures.join("\n\n")
    );
}
