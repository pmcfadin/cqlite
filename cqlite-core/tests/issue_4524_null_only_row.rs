//! Issue #4524 — a `SELECT` must return a row that has live primary-key LIVENESS but no live
//! regular cell (a primary-key-only `INSERT`, an explicit `null`, a cell deleted over a live
//! insert) and must NOT return a row with no liveness whose only cell is deleted.
//!
//! Oracle: CASSANDRA 5.0.8's own `cqlsh SELECT *` output, committed verbatim as
//! `cqlsh-select.txt` beside each Cassandra-written SSTable
//! (`test-data/fixtures/issue_4524`, generator `generate-issue-4524-null-only-row.sh`; the
//! statement and expected outcome of every row are documented in the generator). The expected
//! key set is PARSED FROM THAT FILE, never authored here, and never from a CQLite output
//! (CLAUDE.md, #3042).
//!
//! Physical-dump parity (the `*-Data.db.jsonl` goldens) cannot see this defect: the dump lists
//! the dropped rows AND the wrongly-kept row. This target compares the post-reconciliation
//! result, through every read surface: a full scan (auto and forced-full), a per-partition point
//! read (auto, forced-point and forced-full), a multi-partition `IN`, and the streaming
//! executor. The `query_semantics_oracle_parity` cases `null_only_row__*` pin the full scan too.

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

const KEYSPACE: &str = "test_nullrow";
const SCHEMA: &str = "issue-4524-null-only-row.cql";
/// Partition keys written by the generator (1..=8): 5 and 7 are the NEGATIVE controls.
const PARTITION_KEYS: std::ops::RangeInclusive<i64> = 1..=8;

fn fixture_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace parent")
        .join("test-data/fixtures/issue_4524")
}

fn generation_dir(table: &str) -> PathBuf {
    let dirs = datasets_root::table_generation_dirs(&fixture_root(), KEYSPACE, table);
    assert_eq!(dirs.len(), 1, "{table}: expected ONE committed generation, got {dirs:?}");
    dirs.into_iter().next().unwrap()
}

/// One result row reduced to `(pk, ck, v)`; `ck` is 0 for the flat table; `v` is `"null"` for a
/// null/absent value (cqlsh prints `null`).
type Key = (i64, i64, String);

/// Parse Cassandra's committed `cqlsh-select.txt` table output into the expected row set.
fn cassandra_rows(table: &str, clustered: bool) -> BTreeSet<Key> {
    let text = std::fs::read_to_string(generation_dir(table).join("cqlsh-select.txt"))
        .expect("committed cqlsh oracle");
    let mut out = BTreeSet::new();
    for line in text.lines() {
        let cols: Vec<&str> = line.split('|').map(str::trim).collect();
        let want = if clustered { 3 } else { 2 };
        if cols.len() != want || cols[0].parse::<i64>().is_err() {
            continue; // header, separator, blank, "(N rows)"
        }
        let pk: i64 = cols[0].parse().unwrap();
        let (ck, v) = if clustered {
            (cols[1].parse().expect("ck"), cols[2])
        } else {
            (0, cols[1])
        };
        out.insert((pk, ck, v.to_string()));
    }
    assert!(!out.is_empty(), "{table}: the cqlsh oracle parsed to zero rows");
    out
}

fn as_i64(v: Option<&Value>) -> i64 {
    match v {
        Some(Value::Integer(i)) => i64::from(*i),
        Some(Value::BigInt(i)) => *i,
        other => panic!("expected an integer key column, got {other:?}"),
    }
}

fn key_of(row: &QueryRow, clustered: bool) -> Key {
    let pk = as_i64(row.values.get("pk").or_else(|| row.values.get("id")));
    let ck = if clustered { as_i64(row.values.get("ck")) } else { 0 };
    let v = match row.values.get("v") {
        None | Some(Value::Null) => "null".to_string(),
        Some(Value::Text(_)) => row.values["v"].as_str().expect("text").to_string(),
        other => panic!("unexpected v value {other:?}"),
    };
    (pk, ck, v)
}

async fn open(mode: ReadPathMode) -> Database {
    let schema = datasets_root::schema_path(SCHEMA).expect("committed schema");
    let mut core_config = Config::default();
    core_config.query.forced_read_path = Some(mode);
    let result = ingest(IngestionConfig {
        schema_paths: vec![schema],
        data_dir: fixture_root(),
        version_hint: None,
        core_config,
        table_directory_filter: Some(format!("/{KEYSPACE}/")),
    })
    .await
    .expect("ingest");
    result.database
}

async fn rows(db: &Database, sql: &str, clustered: bool) -> BTreeSet<Key> {
    let r = db.execute(sql).await.unwrap_or_else(|e| panic!("`{sql}`: {e}"));
    r.rows.iter().map(|row| key_of(row, clustered)).collect()
}

fn pk_col(clustered: bool) -> &'static str {
    if clustered {
        "pk"
    } else {
        "id"
    }
}

/// Every surface, both tables, against Cassandra's own answer.
async fn check_table(table: &str, clustered: bool) {
    let oracle = cassandra_rows(table, clustered);
    let pk = pk_col(clustered);
    let full_sql = format!("SELECT * FROM {KEYSPACE}.{table}");

    // The oracle must itself contain the subject (rows with no live regular cell) so the
    // comparison below cannot pass vacuously.
    assert!(
        oracle.iter().filter(|(_, _, v)| v == "null").count() >= 4,
        "{table}: the Cassandra oracle lost its null-only rows"
    );

    for mode in [ReadPathMode::Auto, ReadPathMode::Full] {
        let db = open(mode).await;
        // Full scan.
        assert_eq!(rows(&db, &full_sql, clustered).await, oracle, "{table} full scan [{mode:?}]");
        // Per-partition point read, including the partitions Cassandra returns NOTHING for.
        for k in PARTITION_KEYS {
            let want: BTreeSet<Key> = oracle.iter().filter(|(p, _, _)| *p == k).cloned().collect();
            let got = rows(&db, &format!("{full_sql} WHERE {pk} = {k}"), clustered).await;
            assert_eq!(got, want, "{table} {pk} = {k} [{mode:?}]");
        }
        // Multi-partition IN.
        let list = PARTITION_KEYS.map(|k| k.to_string()).collect::<Vec<_>>().join(", ");
        assert_eq!(
            rows(&db, &format!("{full_sql} WHERE {pk} IN ({list})"), clustered).await,
            oracle,
            "{table} IN [{mode:?}]"
        );
        // Streaming executor.
        let mut it = db
            .execute_streaming(&full_sql, StreamingConfig::default())
            .await
            .expect("streaming SELECT");
        let mut streamed = BTreeSet::new();
        while let Some(item) = it.next_async().await {
            streamed.insert(key_of(&item.expect("streamed row"), clustered));
        }
        assert_eq!(streamed, oracle, "{table} streaming [{mode:?}]");
    }

    // Forced POINT: every single-partition read must serve (no fallback) and match.
    let point = open(ReadPathMode::Point).await;
    for k in PARTITION_KEYS {
        let want: BTreeSet<Key> = oracle.iter().filter(|(p, _, _)| *p == k).cloned().collect();
        let got = rows(&point, &format!("{full_sql} WHERE {pk} = {k}"), clustered).await;
        assert_eq!(got, want, "{table} {pk} = {k} [forced point]");
    }
}

#[tokio::test]
async fn flat_table_returns_null_only_rows_and_hides_the_deleted_ones() {
    check_table("flat_rows", false).await;
}

#[tokio::test]
async fn clustered_table_returns_null_only_rows_and_hides_the_deleted_ones() {
    check_table("clustered_rows", true).await;
}
