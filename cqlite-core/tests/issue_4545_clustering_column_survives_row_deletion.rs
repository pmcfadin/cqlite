//! Issue #4545 — a primary-key column is never shadowed: a row that a newer cell keeps alive across
//! a coexisting row deletion still returns its clustering column.
//!
//! Fixture `test-data/fixtures/issue_4543` (Cassandra 5.0.8; see its generator): pk=1 ck=2 has a
//! row deletion at ts 200 in the older generation and a cell `b` at ts 300 in the newer one.
//! Cassandra's own `SELECT` (captured in `cassandra-select.txt`) returns `(1, 2, null, b-gen2, null)`.
//!
//! `SELECT *` is compared COLUMN SET BOTH WAYS (#3890): no column the oracle row carries may be
//! missing, and none it lacks may appear. Paths: forced point, forced full, prepared, streaming.
//!
//! Root cause (verified): `ReconcileState::shadow_by_row_deletion` (`merge/reconcile.rs`) filtered
//! EVERY winner with `cell.timestamp > row_deletion`. The clustering pseudo-cell `ck` carries the row
//! marker's timestamp, or 0 for a marker-less row (the row was created by an `UPDATE`), so `0 <= 200`
//! dropped it while `b@300` survived, and the row was materialised without `ck`.

#![cfg(all(feature = "state_machine", feature = "cli-helpers", feature = "lz4"))]

use std::collections::BTreeSet;
use std::path::PathBuf;

use cqlite_core::config::ReadPathMode;
use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::result::StreamingConfig;
use cqlite_core::types::Value;
use cqlite_core::{Config, Database};

#[path = "support/datasets_root.rs"]
mod datasets_root;

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace parent")
        .join("test-data/fixtures/issue_4543")
}

async fn open(mode: ReadPathMode) -> Database {
    let mut core_config = Config::default();
    core_config.query.forced_read_path = Some(mode);
    ingest(IngestionConfig {
        schema_paths: vec![
            datasets_root::schema_path("issue-4543-percell-cross-gen.cql").expect("schema"),
        ],
        data_dir: fixture().join("ordered"),
        version_hint: None,
        core_config,
        table_directory_filter: None,
    })
    .await
    .expect("ingest")
    .database
}

type Cols = BTreeSet<String>;

/// The oracle's column set for the pk=1 ck=2 row: Cassandra returned a, c as null (absent here),
/// b-gen2, plus the key columns `pk` and `ck`.
fn want_cols() -> Cols {
    ["pk", "ck", "b"].iter().map(|s| s.to_string()).collect()
}

fn assert_ck2_row(path: &str, rows: &[std::collections::HashMap<String, Value>]) {
    let row = rows
        .iter()
        .find(|r| {
            matches!(r.get("b"), Some(Value::Text(b)) if b.as_ref() == b"b-gen2")
                && !matches!(r.get("ck"), Some(Value::Integer(1) | Value::Integer(3)))
        })
        .unwrap_or_else(|| panic!("[{path}] the ck=2 row (b-gen2) is missing: {rows:?}"));
    let got: Cols = row.keys().map(|k| k.to_string()).collect();
    let want = want_cols();
    let missing: Vec<_> = want.difference(&got).collect();
    let extra: Vec<_> = got.difference(&want).collect();
    assert!(
        missing.is_empty() && extra.is_empty(),
        "[{path}] column set: missing {missing:?}, extra {extra:?} in {row:?}"
    );
    assert_eq!(
        row.get("ck"),
        Some(&Value::Integer(2)),
        "[{path}] clustering value"
    );
    assert_eq!(
        row.get("pk"),
        Some(&Value::Integer(1)),
        "[{path}] partition value"
    );
}

fn maps(
    res: &cqlite_core::query::result::QueryResult,
) -> Vec<std::collections::HashMap<String, Value>> {
    res.rows
        .iter()
        .map(|r| {
            r.values
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect()
        })
        .collect()
}

#[tokio::test]
async fn point_path_keeps_the_clustering_column() {
    let db = open(ReadPathMode::Point).await;
    let res = db
        .execute("SELECT * FROM test_percell.pcell WHERE pk = 1")
        .await
        .expect("select");
    assert_ck2_row("point", &maps(&res));
}

#[tokio::test]
async fn full_path_keeps_the_clustering_column() {
    let db = open(ReadPathMode::Full).await;
    let res = db
        .execute("SELECT * FROM test_percell.pcell")
        .await
        .expect("select");
    assert_ck2_row("full", &maps(&res));
}

#[tokio::test]
async fn prepared_path_keeps_the_clustering_column() {
    let db = open(ReadPathMode::Point).await;
    let prepared = db
        .prepare("SELECT * FROM test_percell.pcell WHERE pk = ?")
        .await
        .expect("prepare");
    let res = prepared
        .execute(&[Value::Integer(1)])
        .await
        .expect("execute");
    assert_ck2_row("prepared", &maps(&res));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn streaming_path_keeps_the_clustering_column() {
    let db = open(ReadPathMode::Full).await;
    let mut iter = db
        .execute_streaming(
            "SELECT * FROM test_percell.pcell",
            StreamingConfig::default(),
        )
        .await
        .expect("execute_streaming");
    let mut rows = Vec::new();
    while let Some(row) = iter.next_async().await {
        let row = row.expect("row");
        rows.push(
            row.values
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
        );
    }
    assert_ck2_row("streaming", &rows);
}

/// The sibling rows must keep EXACTLY their own column sets too (no key column added or lost where
/// the row deletion did not apply): ck=1 carries a, b, c; ck=3 carries a only.
#[tokio::test]
async fn untouched_rows_keep_their_exact_column_sets() {
    let db = open(ReadPathMode::Point).await;
    let res = db
        .execute("SELECT * FROM test_percell.pcell WHERE pk = 1")
        .await
        .expect("select");
    for r in maps(&res) {
        let cols: Cols = r.keys().map(|k| k.to_string()).collect();
        let want: Cols = match r.get("ck") {
            Some(Value::Integer(1)) => ["pk", "ck", "a", "b", "c"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            Some(Value::Integer(2)) => want_cols(),
            Some(Value::Integer(3)) => ["pk", "ck", "a"].iter().map(|s| s.to_string()).collect(),
            other => panic!("unexpected row (ck = {other:?}): {r:?}"),
        };
        assert_eq!(cols, want, "{r:?}");
    }
    assert_eq!(
        maps(&res).len(),
        3,
        "ck=4 stays invisible (row deleted, only an older cell)"
    );
}
