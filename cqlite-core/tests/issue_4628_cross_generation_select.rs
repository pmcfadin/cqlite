//! Issue #4628: SELECT over a table whose tombstone and data live in different
//! generations, with an ABSENT generation in between, must match the live Cassandra 5.0.8
//! CL=ALL read (`test-data/fixtures/issue_4628/live-select-consistency-all.txt`).
//!
//! The merger refilled runs by candidate index while absent candidates contribute no run,
//! so the point-read path stopped reading the data generation after its first row. Runs
//! under `Auto` (the user-facing path) and `Full` (the scan reference), per #3890 both ways.

#![cfg(all(feature = "write-support", feature = "cli-helpers"))]

use std::collections::BTreeMap;
use std::path::PathBuf;

use cqlite_core::config::ReadPathMode;
use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::types::Value;
use cqlite_core::{Config, Database};

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

/// `id -> [(c1, c2, v, l)]` parsed from the committed CL=ALL oracle, in clustering order.
/// One live row: `(c1, c2, v, l)`.
type LiveRow = (i32, i32, String, Vec<i32>);

fn live_rows() -> BTreeMap<i32, Vec<LiveRow>> {
    let text = std::fs::read_to_string(fixture().join("live-select-consistency-all.txt"))
        .expect("committed live oracle");
    let mut rows: BTreeMap<i32, Vec<_>> = BTreeMap::new();
    for line in text.lines() {
        let cols: Vec<&str> = line.split('|').map(str::trim).collect();
        if cols.len() < 9 {
            continue;
        }
        let Ok(id) = cols[0].parse::<i32>() else {
            continue; // header / separator / footer
        };
        let list = cols[4].trim_matches(|c| c == '[' || c == ']');
        let l: Vec<i32> = if list.is_empty() || list == "null" {
            Vec::new()
        } else {
            list.split(',')
                .map(|x| x.trim().parse().expect("list element"))
                .collect()
        };
        rows.entry(id).or_default().push((
            cols[1].parse().expect("c1"),
            cols[2].parse().expect("c2"),
            cols[3].to_string(),
            l,
        ));
    }
    assert_eq!(
        rows.values().map(Vec::len).sum::<usize>(),
        22,
        "live oracle carries all 22 rows"
    );
    rows
}

/// Keys the oracle reads, including key 1 whose rows are all deleted.
const KEYS: [i32; 6] = [1, 3, 4, 5, 6, 9];

fn int(v: Option<&Value>, what: &str) -> i32 {
    match v {
        Some(Value::Integer(i)) => *i,
        other => panic!("{what}: expected int, got {other:?}"),
    }
}

fn text(v: Option<&Value>) -> String {
    match v {
        Some(Value::Text(b)) => String::from_utf8_lossy(b).into_owned(),
        Some(Value::Null) | None => "null".to_string(),
        other => panic!("v: expected text, got {other:?}"),
    }
}

fn list(v: Option<&Value>) -> Vec<i32> {
    match v {
        Some(Value::List(xs)) => xs.iter().map(|x| int(Some(x), "l element")).collect(),
        Some(Value::Null) | None => Vec::new(),
        other => panic!("l: expected list<int>, got {other:?}"),
    }
}

#[tokio::test]
async fn select_matches_live_cl_all_for_cross_generation_tombstones() {
    let live = live_rows();
    for (mode, name) in [(ReadPathMode::Auto, "auto"), (ReadPathMode::Full, "full")] {
        let db = open(mode).await;
        for key in KEYS {
            let result = db
                .execute(&format!("SELECT * FROM fix4628.t WHERE id = {key}"))
                .await
                .unwrap_or_else(|e| panic!("SELECT key {key} ({name}) failed: {e}"));
            let got: Vec<LiveRow> = result
                .rows
                .iter()
                .map(|row| {
                    (
                        int(row.get("c1"), "c1"),
                        int(row.get("c2"), "c2"),
                        text(row.get("v")),
                        list(row.get("l")),
                    )
                })
                .collect();
            let want = live.get(&key).cloned().unwrap_or_default();
            assert_eq!(
                got, want,
                "key {key} ({name}): rows (c1, c2, v, l) differ from the live CL=ALL read"
            );
        }
    }
}
