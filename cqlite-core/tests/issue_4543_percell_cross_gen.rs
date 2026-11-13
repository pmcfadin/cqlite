//! Issue #4543 — MEASUREMENT: does a point read across generations reconcile PER CELL the way
//! Cassandra does?
//!
//! Fixture `test-data/fixtures/issue_4543` (Cassandra 5.0.8; generator
//! `test-data/scripts/generate-issue-4543-percell-cross-gen.sh`, case table in
//! `test-data/schemas/issue-4543-percell-cross-gen.cql`): two generations where the NEWER file
//! holds OLDER per-cell timestamps for some cells, plus a row deletion in the older file with a
//! timestamp between the cells'. Oracle: Cassandra's own `SELECT` over the two uncompacted
//! generations (`cassandra-select.txt`).
//!
//! Each query runs under forced `Point` and forced `Full` read paths and is compared with the oracle.

#![cfg(all(feature = "state_machine", feature = "cli-helpers", feature = "lz4"))]

use std::path::PathBuf;

use cqlite_core::config::ReadPathMode;
use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::types::Value;
use cqlite_core::{Config, Database};

#[path = "support/datasets_root.rs"]
mod datasets_root;

type Row = (i32, i32, Option<String>, Option<String>, Option<String>);

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace parent")
        .join("test-data/fixtures/issue_4543")
}

/// Cassandra's `SELECT pk, ck, a, b, c, ...` rows from the captured cqlsh output.
fn oracle() -> Vec<Row> {
    let text = std::fs::read_to_string(fixture().join("cassandra-select.txt")).expect("capture");
    let cell = |s: &str| (s != "null").then(|| s.to_string());
    let mut out = Vec::new();
    for line in text.lines() {
        let c: Vec<&str> = line.split('|').map(str::trim).collect();
        if c.len() == 8 {
            if let (Ok(pk), Ok(ck)) = (c[0].parse(), c[1].parse()) {
                out.push((pk, ck, cell(c[2]), cell(c[3]), cell(c[4])));
            }
        }
    }
    assert_eq!(out.len(), 5, "Cassandra returned 5 rows: {text}");
    out.sort();
    out
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

async fn select(db: &Database, where_clause: &str) -> Result<Vec<Row>, String> {
    let q = format!("SELECT pk, ck, a, b, c FROM test_percell.pcell{where_clause}");
    let res = db.execute(&q).await.map_err(|e| format!("{q}: {e}"))?;
    let text = |r: &cqlite_core::query::result::QueryRow, c: &str| match r.values.get(c) {
        Some(Value::Text(b)) => Some(String::from_utf8_lossy(b).into_owned()),
        _ => None,
    };
    // A missing `ck` is reported as -1 (and so FAILS the comparison) instead of panicking, so the
    // cell values of the same row stay visible in the verdict.
    let int = |r: &cqlite_core::query::result::QueryRow, c: &str| match r.values.get(c) {
        Some(Value::Integer(i)) => *i,
        None if c == "ck" => -1,
        other => panic!("{c}: {other:?}"),
    };
    if std::env::var("DBG4543").is_ok() {
        for r in &res.rows {
            eprintln!("RAW {q} -> {:?}", r.values);
        }
    }
    let mut rows: Vec<Row> = res
        .rows
        .iter()
        .map(|r| {
            (
                int(r, "pk"),
                int(r, "ck"),
                text(r, "a"),
                text(r, "b"),
                text(r, "c"),
            )
        })
        .collect();
    rows.sort();
    Ok(rows)
}

/// Run the matrix, print one verdict line per (path, query), return the failures.
async fn matrix(mode: ReadPathMode) -> Vec<String> {
    let db = open(mode).await;
    let all = oracle();
    let mut bad = Vec::new();
    for (label, clause, pk) in [
        ("whole table", "", None),
        ("pk = 1", " WHERE pk = 1", Some(1)),
        ("pk = 2", " WHERE pk = 2", Some(2)),
        ("pk = 3", " WHERE pk = 3", Some(3)),
    ] {
        if pk.is_none() && mode == ReadPathMode::Point {
            continue; // a whole-table scan cannot be served by the point path (by design)
        }
        let want: Vec<Row> = all
            .iter()
            .filter(|r| pk.is_none_or(|p| r.0 == p))
            .cloned()
            .collect();
        // Cell values only: the clustering column of the ck=2 row is lost on every path (see
        // `surviving_row_keeps_its_clustering_value`), which is a different defect from the one
        // this file measures (per-cell reconcile).
        let cells = |rows: &[Row]| {
            let mut v: Vec<_> = rows
                .iter()
                .map(|r| (r.0, r.2.clone(), r.3.clone(), r.4.clone()))
                .collect();
            v.sort();
            v
        };
        let verdict = match select(&db, clause).await {
            Ok(got) if cells(&got) == cells(&want) => "PASS".to_string(),
            Ok(got) => format!("FAIL\n    got  {got:?}\n    want {want:?}"),
            Err(e) => format!("FAIL {e}"),
        };
        eprintln!("[{mode:?}] {label:<12} {verdict}");
        if verdict != "PASS" {
            bad.push(format!("[{mode:?}] {label}: {verdict}"));
        }
    }
    bad
}

#[tokio::test]
async fn point_path_per_cell_reconcile_matches_cassandra() {
    let bad = matrix(ReadPathMode::Point).await;
    assert!(bad.is_empty(), "{}", bad.join("\n"));
}

#[tokio::test]
async fn full_path_per_cell_reconcile_matches_cassandra() {
    let bad = matrix(ReadPathMode::Full).await;
    assert!(bad.is_empty(), "{}", bad.join("\n"));
}

/// The prepared-statement route (`Database::prepare` -> `PreparedQuery::execute`), the only place
/// the legacy `QueryExecutor` -> `SSTableManager::get` -> `TombstoneMerger` chain could be reached
/// from the public API (ad-hoc SELECTs have used `SelectExecutor` since #1750). Cell values only
/// (see `surviving_row_keeps_its_clustering_value` for the `ck` finding).
#[tokio::test]
async fn prepared_point_lookup_per_cell_reconcile_matches_cassandra() {
    type Cells = (Option<String>, Option<String>, Option<String>);
    let db = open(ReadPathMode::Point).await;
    let all = oracle();
    for pk in [1, 2, 3] {
        let prepared = db
            .prepare("SELECT pk, ck, a, b, c FROM test_percell.pcell WHERE pk = ?")
            .await
            .expect("prepare");
        let res = prepared
            .execute(&[Value::Integer(pk)])
            .await
            .expect("execute");
        let t = |r: &cqlite_core::query::result::QueryRow, c: &str| match r.values.get(c) {
            Some(Value::Text(b)) => Some(String::from_utf8_lossy(b).into_owned()),
            _ => None,
        };
        let mut got: Vec<Cells> = res
            .rows
            .iter()
            .map(|r| (t(r, "a"), t(r, "b"), t(r, "c")))
            .collect();
        let mut want: Vec<Cells> = all
            .iter()
            .filter(|r| r.0 == pk)
            .map(|r| (r.2.clone(), r.3.clone(), r.4.clone()))
            .collect();
        got.sort();
        want.sort();
        assert_eq!(got, want, "[Prepared] pk = {pk}: cell values");
    }
}

/// FINDING (not #4543): when a row deletion coexists with a surviving NEWER cell of the same row
/// (here ck=2: older generation deletes the row at ts 200, newer generation writes `b` at ts 300),
/// the reconciled row comes back WITHOUT its clustering column - `ck` is missing - on the point,
/// full and prepared paths alike. Cassandra returns `(1, 2, null, b-gen2, null)`. The `ck`
/// pseudo-cell carries timestamp 0, so the row-deletion shadow filter drops it like any other cell
/// older than the deletion.
#[tokio::test]
async fn surviving_row_keeps_its_clustering_value() {
    for mode in [ReadPathMode::Point, ReadPathMode::Full] {
        let db = open(mode).await;
        let got = select(&db, " WHERE pk = 1").await.expect("select");
        let want: Vec<Row> = oracle().into_iter().filter(|r| r.0 == 1).collect();
        assert_eq!(got, want, "[{mode:?}] pk = 1");
    }
}
