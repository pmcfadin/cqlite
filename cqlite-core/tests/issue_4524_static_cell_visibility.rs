//! Issue #4524 (roborev job 297) — a live STATIC cell must never keep a clustering row visible.
//!
//! Oracle: CASSANDRA 5.0.8's own `cqlsh SELECT *` output (`cqlsh-select.txt`, committed verbatim
//! beside the Cassandra-written two-generation SSTables; generator
//! `test-data/scripts/generate-issue-4524-static-multigen.sh`, which documents every row's
//! statements). The expected set is PARSED from that file, never authored here or taken from a
//! CQLite output (CLAUDE.md, #3042).
//!
//! The shape: `pk 2` has a clustering row written by `UPDATE` (NO liveness marker) in generation
//! 1 whose only regular cell is tombstoned in generation 2, beside a LIVE static cell. Cassandra
//! (`AbstractRow.hasLiveData` looks at the ROW's own liveness and its own cells; the static row is
//! a separate `Row`) returns NO `(2, 1)` clustering row — only the static row `(2, null, st2)`.
//! Controls: `pk 1` (liveness marker survives the tombstone -> the clustering row stays), `pk 3`
//! (live regular cell), `pk 4` (static-only partition -> the static row). Every read surface and
//! BOTH generation orders (the `nb-1`/`nb-2` file names swapped) must give Cassandra's answer.
//!
//! What is compared (the finding is about CLUSTERING-row visibility): the set of clustering rows
//! `(pk, ck, v)` must equal Cassandra's exactly, and every static row Cassandra returns must be
//! returned. cqlsh's display also PROJECTS a partition's static value onto each clustering row
//! and prints the static row only for a partition with no live clustering row; CQLite's read path
//! emits static rows as their own rows and does not project (a separate, pre-existing
//! presentation difference covered by the static-parity suites, not by this finding), so `s` on a
//! clustering row and the EXTRA static rows are deliberately not compared.

#![cfg(all(feature = "state_machine", feature = "cli-helpers", feature = "lz4"))]

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use cqlite_core::config::ReadPathMode;
use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::result::QueryRow;
use cqlite_core::query::StreamingConfig;
use cqlite_core::types::Value;
use cqlite_core::{Config, Database};

#[path = "support/datasets_root.rs"]
mod datasets_root;

const KEYSPACE: &str = "test_staticrow";
const TABLE: &str = "static_multigen";
const SCHEMA: &str = "issue-4524-static-multigen.cql";
const PARTITION_KEYS: std::ops::RangeInclusive<i64> = 1..=4;

/// `(pk, ck (None = the static row), s, v)`; absent values are `"null"` as cqlsh prints them.
type Key = (i64, Option<i64>, String, String);

fn fixture_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace parent")
        .join("test-data/fixtures/issue_4524")
}

fn generation_dir() -> PathBuf {
    let dirs = datasets_root::table_generation_dirs(&fixture_root(), KEYSPACE, TABLE);
    assert_eq!(
        dirs.len(),
        1,
        "expected ONE committed table dir, got {dirs:?}"
    );
    dirs.into_iter().next().unwrap()
}

/// Cassandra's committed answer.
fn cassandra_rows() -> BTreeSet<Key> {
    let text = std::fs::read_to_string(generation_dir().join("cqlsh-select.txt"))
        .expect("committed cqlsh oracle");
    let mut out = BTreeSet::new();
    for line in text.lines() {
        let cols: Vec<&str> = line.split('|').map(str::trim).collect();
        if cols.len() != 4 || cols[0].parse::<i64>().is_err() {
            continue; // header, separator, blank, "(N rows)"
        }
        out.insert((
            cols[0].parse().unwrap(),
            cols[1].parse::<i64>().ok(),
            cols[2].to_string(),
            cols[3].to_string(),
        ));
    }
    assert_eq!(
        out.len(),
        4,
        "the cqlsh oracle must parse to its 4 rows: {out:?}"
    );
    out
}

fn as_i64(v: &Value) -> Option<i64> {
    match v {
        Value::Integer(i) => Some(i64::from(*i)),
        Value::BigInt(i) => Some(*i),
        Value::Null => None,
        other => panic!("expected an integer column, got {other:?}"),
    }
}

fn text(row: &QueryRow, col: &str) -> String {
    match row.values.get(col) {
        None | Some(Value::Null) => "null".to_string(),
        Some(v @ Value::Text(_)) => v.as_str().expect("text").to_string(),
        other => panic!("unexpected {col} value {other:?}"),
    }
}

fn key_of(row: &QueryRow) -> Key {
    (
        as_i64(row.values.get("pk").expect("pk")).expect("pk is never null"),
        row.values.get("ck").and_then(as_i64),
        text(row, "s"),
        text(row, "v"),
    )
}

async fn open(root: &Path, mode: ReadPathMode) -> Database {
    let schema = datasets_root::schema_path(SCHEMA).expect("committed schema");
    let mut core_config = Config::default();
    core_config.query.forced_read_path = Some(mode);
    ingest(IngestionConfig {
        schema_paths: vec![schema],
        data_dir: root.to_path_buf(),
        version_hint: None,
        core_config,
        table_directory_filter: Some(format!("/{KEYSPACE}/")),
    })
    .await
    .expect("ingest")
    .database
}

async fn rows(db: &Database, sql: &str) -> BTreeSet<Key> {
    let r = db
        .execute(sql)
        .await
        .unwrap_or_else(|e| panic!("`{sql}`: {e}"));
    r.rows.iter().map(key_of).collect()
}

/// A copy of the fixture tree with the two generations' file names swapped
/// (`nb-1-big-*` <-> `nb-2-big-*`): same bytes, opposite generation order.
fn swapped_copy() -> tempfile::TempDir {
    let tmp = tempfile::tempdir().expect("tempdir");
    let src = generation_dir();
    let dst = tmp
        .path()
        .join(KEYSPACE)
        .join(src.file_name().expect("table dir name"));
    std::fs::create_dir_all(&dst).expect("mkdir");
    for e in std::fs::read_dir(&src).expect("dir").flatten() {
        let name = e.file_name().to_string_lossy().into_owned();
        if !e.path().is_file() {
            continue;
        }
        let swapped = if let Some(rest) = name.strip_prefix("nb-1-big-") {
            format!("nb-2-big-{rest}")
        } else if let Some(rest) = name.strip_prefix("nb-2-big-") {
            format!("nb-1-big-{rest}")
        } else {
            name
        };
        std::fs::copy(e.path(), dst.join(swapped)).expect("copy");
    }
    tmp
}

/// Cassandra's clustering rows, and its static rows, against what CQLite returned.
fn assert_matches_cassandra(got: &BTreeSet<Key>, want: &BTreeSet<Key>, label: &str) {
    let clustering = |set: &BTreeSet<Key>| -> BTreeSet<(i64, i64, String)> {
        set.iter()
            .filter_map(|(p, c, _, v)| c.map(|c| (*p, c, v.clone())))
            .collect()
    };
    assert_eq!(
        clustering(got),
        clustering(want),
        "{label}: clustering rows (got {got:?})"
    );
    for (p, c, s, v) in want.iter().filter(|(_, c, _, _)| c.is_none()) {
        assert!(
            got.contains(&(*p, *c, s.clone(), v.clone())),
            "{label}: Cassandra's static row for pk {p} is missing (got {got:?})"
        );
    }
}

async fn check_all_surfaces(root: &Path, label: &str) {
    let oracle = cassandra_rows();
    // The subject must be in the oracle, so the comparison cannot pass vacuously: pk 2 has a
    // static row and NO clustering row, pk 1 keeps its clustering row.
    assert!(
        oracle.contains(&(2, None, "st2".into(), "null".into())),
        "{oracle:?}"
    );
    assert!(
        !oracle.iter().any(|(p, c, _, _)| *p == 2 && c.is_some()),
        "{oracle:?}"
    );
    assert!(
        oracle.contains(&(1, Some(1), "st1".into(), "null".into())),
        "{oracle:?}"
    );

    let full_sql = format!("SELECT * FROM {KEYSPACE}.{TABLE}");
    for mode in [ReadPathMode::Auto, ReadPathMode::Full] {
        let db = open(root, mode).await;
        assert_matches_cassandra(
            &rows(&db, &full_sql).await,
            &oracle,
            &format!("{label} full scan [{mode:?}]"),
        );
        for k in PARTITION_KEYS {
            let want: BTreeSet<Key> = oracle.iter().filter(|(p, ..)| *p == k).cloned().collect();
            assert_matches_cassandra(
                &rows(&db, &format!("{full_sql} WHERE pk = {k}")).await,
                &want,
                &format!("{label} pk = {k} [{mode:?}]"),
            );
        }
        let list = PARTITION_KEYS
            .map(|k| k.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        assert_matches_cassandra(
            &rows(&db, &format!("{full_sql} WHERE pk IN ({list})")).await,
            &oracle,
            &format!("{label} IN [{mode:?}]"),
        );
        let mut it = db
            .execute_streaming(&full_sql, StreamingConfig::default())
            .await
            .expect("streaming SELECT");
        let mut streamed = BTreeSet::new();
        while let Some(item) = it.next_async().await {
            streamed.insert(key_of(&item.expect("streamed row")));
        }
        assert_matches_cassandra(&streamed, &oracle, &format!("{label} streaming [{mode:?}]"));
    }
    let point = open(root, ReadPathMode::Point).await;
    for k in PARTITION_KEYS {
        let want: BTreeSet<Key> = oracle.iter().filter(|(p, ..)| *p == k).cloned().collect();
        assert_matches_cassandra(
            &rows(&point, &format!("{full_sql} WHERE pk = {k}")).await,
            &want,
            &format!("{label} pk = {k} [forced point]"),
        );
    }
}

#[tokio::test]
async fn a_live_static_cell_does_not_keep_a_tombstone_only_clustering_row_visible() {
    check_all_surfaces(&fixture_root(), "generation order as written").await;
}

#[tokio::test]
async fn the_same_answer_with_the_generations_in_the_opposite_order() {
    let tmp = swapped_copy();
    check_all_surfaces(tmp.path(), "generations swapped").await;
}
