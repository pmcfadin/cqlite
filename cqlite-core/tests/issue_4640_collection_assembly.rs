//! Issue #4640: a `SELECT` that merges more than one generation must return each
//! non-frozen collection COMPLETE, not collapsed to its last element.
//!
//! Oracle: the live Cassandra 5.0.8 CL=ALL read committed beside the Cassandra-written
//! `issue_4628` fixture (`live-select-consistency-all.txt`). Every key is read through the
//! partition-targeted path (`WHERE id = k`) and the whole-table scan (`SELECT *`) under
//! `ReadPathMode::Auto` and `ReadPathMode::Full`, and compared on the FULL row
//! (`id, c1, c2, v, l, m`). Per #3890 the column set is asserted in both directions.

#![cfg(all(feature = "write-support", feature = "cli-helpers"))]

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use cqlite_core::config::ReadPathMode;
use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::{Config, Database, Value};

const COLUMNS: [&str; 6] = ["id", "c1", "c2", "v", "l", "m"];

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../test-data/fixtures/issue_4628")
}

async fn open(mode: ReadPathMode) -> Database {
    let mut core_config = Config::default();
    core_config.query.forced_read_path = Some(mode);
    ingest(IngestionConfig {
        schema_paths: vec![fixture().join("schema.cql")],
        data_dir: fixture().join("sstables"),
        version_hint: None,
        core_config,
        table_directory_filter: None,
    })
    .await
    .expect("ingestion succeeds")
    .database
}

/// cqlsh rendering of a value (`[0, 0]`, `{1: 'a0'}`, `v300`, `null`).
fn render(v: Option<&Value>, nested: bool) -> String {
    match v {
        None | Some(Value::Null) => "null".to_string(),
        Some(Value::Integer(n)) => n.to_string(),
        Some(Value::Text(b)) => {
            let s = String::from_utf8_lossy(b).into_owned();
            if nested {
                format!("'{s}'")
            } else {
                s
            }
        }
        Some(Value::List(items)) => {
            let parts: Vec<String> = items.iter().map(|i| render(Some(i), true)).collect();
            format!("[{}]", parts.join(", "))
        }
        Some(Value::Map(entries)) => {
            let parts: Vec<String> = entries
                .iter()
                .map(|(k, v)| format!("{}: {}", render(Some(k), true), render(Some(v), true)))
                .collect();
            format!("{{{}}}", parts.join(", "))
        }
        Some(other) => format!("UNEXPECTED {other:?}"),
    }
}

/// `(id, c1, c2) -> [v, l, m]` rendered, straight from the CL=ALL oracle file.
fn oracle() -> BTreeMap<(i32, i32, i32), [String; 3]> {
    let text = std::fs::read_to_string(fixture().join("live-select-consistency-all.txt"))
        .expect("oracle file is committed (must_run)");
    let mut out = BTreeMap::new();
    for line in text.lines() {
        let cells: Vec<&str> = line.split('|').map(str::trim).collect();
        let Some(id) = cells.first().and_then(|c| c.parse::<i32>().ok()) else {
            continue; // header, rule, banner, row count
        };
        assert!(cells.len() >= 6, "oracle row has the six columns: {line}");
        let key = (id, cells[1].parse().unwrap(), cells[2].parse().unwrap());
        out.insert(
            key,
            [
                cells[3].to_string(),
                cells[4].to_string(),
                cells[5].to_string(),
            ],
        );
    }
    assert_eq!(out.len(), 22, "the CL=ALL oracle holds 22 rows");
    out
}

/// Render the result into `(id, c1, c2) -> [v, l, m]`, asserting the column set both ways.
fn rows_of(result: &cqlite_core::QueryResult, ctx: &str) -> BTreeMap<(i32, i32, i32), [String; 3]> {
    let mut out = BTreeMap::new();
    for row in &result.rows {
        let present: BTreeSet<&str> = row.values.keys().map(|k| &**k).collect();
        let key_cols: BTreeSet<&str> = ["id", "c1", "c2", "v"].into_iter().collect();
        assert!(
            key_cols.is_subset(&present),
            "{ctx}: key/scalar columns missing, got {present:?}"
        );
        for c in &present {
            assert!(COLUMNS.contains(c), "{ctx}: unexpected column {c}");
        }
        let int = |c: &str| match row.get(c) {
            Some(Value::Integer(n)) => *n,
            other => panic!("{ctx}: {c} is {other:?}"),
        };
        let key = (int("id"), int("c1"), int("c2"));
        let cells = [
            render(row.get("v"), false),
            render(row.get("l"), false),
            render(row.get("m"), false),
        ];
        assert!(
            out.insert(key, cells).is_none(),
            "{ctx}: duplicate row {key:?}"
        );
    }
    out
}

#[tokio::test]
async fn select_full_rows_match_live_cl_all_on_every_read_path() {
    let want = oracle();
    let keys: BTreeSet<i32> = want.keys().map(|k| k.0).collect();
    for (mode, name) in [(ReadPathMode::Auto, "auto"), (ReadPathMode::Full, "full")] {
        let db = open(mode).await;

        // Whole-table scan (streaming merge).
        let all = db.execute("SELECT * FROM fix4628.t").await.expect("scan");
        let got = rows_of(&all, &format!("{name} scan"));
        assert_eq!(
            got, want,
            "{name}: SELECT * differs from the live CL=ALL read"
        );

        // Partition-targeted reads, one per key (key 1 is wholly deleted: no rows).
        let mut union = BTreeMap::new();
        for key in keys.iter().copied().chain([1]) {
            let r = db
                .execute(&format!("SELECT * FROM fix4628.t WHERE id = {key}"))
                .await
                .unwrap_or_else(|e| panic!("{name} id={key}: {e}"));
            union.extend(rows_of(&r, &format!("{name} id={key}")));
        }
        assert_eq!(union, want, "{name}: partition reads differ from CL=ALL");

        // Fully-specified point reads.
        for ((id, c1, c2), cells) in &want {
            let r = db
                .execute(&format!(
                    "SELECT * FROM fix4628.t WHERE id = {id} AND c1 = {c1} AND c2 = {c2}"
                ))
                .await
                .unwrap_or_else(|e| panic!("{name} point {id},{c1},{c2}: {e}"));
            let got = rows_of(&r, &format!("{name} point {id},{c1},{c2}"));
            assert_eq!(got.len(), 1, "{name} point {id},{c1},{c2}: one row");
            assert_eq!(
                got.values().next(),
                Some(cells),
                "{name} point {id},{c1},{c2}"
            );
        }
    }
}
