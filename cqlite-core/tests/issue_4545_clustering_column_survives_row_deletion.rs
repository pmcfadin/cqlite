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

/// pk=1 returns EXACTLY ck 1, 2, 3: ck=4 (row deleted, only an older cell), ck=5 (row deleted at 200,
/// then only a newer cell TOMBSTONE at 300) and ck=6 (then only a row MARKER at 300 that has EXPIRED) are NOT rows in Cassandra's SELECT,
/// and must not materialise as phantom key-only rows.
fn assert_exact_rows(path: &str, rows: &[std::collections::HashMap<String, Value>]) {
    let mut cks: Vec<i32> = rows
        .iter()
        .filter(|r| r.get("pk") == Some(&Value::Integer(1)))
        .map(|r| match r.get("ck") {
            Some(Value::Integer(i)) => *i,
            _ => -1,
        })
        .collect();
    cks.sort();
    assert_eq!(
        cks,
        vec![1, 2, 3],
        "[{path}] pk=1 clustering keys (no phantom key-only rows)"
    );
}

fn assert_ck2_row(path: &str, rows: &[std::collections::HashMap<String, Value>]) {
    assert_exact_rows(path, rows);
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

/// The COMPACTION WRITE PATH (`compact_sstables` -> `merge_entry_to_mutation` -> writer) for both
/// shapes: compact the two Cassandra generations with CQLite, read the OUTPUT back, and compare with
/// Cassandra's SELECT - the surviving row keeps `ck`, the tombstone-only row stays hidden.
#[test]
fn compaction_output_keeps_the_clustering_column_and_hides_the_tombstone_only_row() {
    compact_and_check(None, None);
}

/// Round-3 (roborev): with gc-grace purging the row deletion is CLEARED before `build`; the key cells
/// of a row with no live data must still be shed (no phantom key-only row), live rows keep `ck`.
#[test]
fn compaction_with_gc_purge_leaves_no_key_only_phantom_row() {
    compact_and_check(Some(i64::from(i32::MAX)), Some(i64::from(i32::MAX)));
}

fn compact_and_check(gc_before: Option<i64>, now: Option<i64>) {
    use cqlite_core::schema::{ClusteringColumn, Column, KeyColumn, TableSchema};
    use cqlite_core::storage::write_engine::merge::compact_sstables;

    let schema = TableSchema {
        keyspace: "test_percell".into(),
        table: "pcell".into(),
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
        columns: ["a", "b", "c"]
            .iter()
            .map(|n| Column {
                name: (*n).into(),
                data_type: "text".into(),
                nullable: true,
                default: None,
                is_static: false,
            })
            .collect(),
        comments: Default::default(),
        dropped_columns: Default::default(),
    };
    let tmp = tempfile::TempDir::new().expect("tmp");
    let src =
        datasets_root::table_generation_dirs(&fixture().join("ordered"), "test_percell", "pcell");
    assert_eq!(src.len(), 1);
    let out = tmp
        .path()
        .join("test_percell")
        .join("pcell-00000000000000000000000000000000");
    std::fs::create_dir_all(&out).expect("out dir");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("rt");
    let inputs = vec![
        src[0].join("nb-2-big-Data.db"),
        src[0].join("nb-1-big-Data.db"),
    ];
    rt.block_on(compact_sstables(
        inputs, &out, &schema, 3, gc_before, now, true,
    ))
    .expect("compaction");

    // Read the OUTPUT back through the merge reader (the layout `<out>/<ks>/<table>/nb-3-big-*`).
    use cqlite_core::storage::write_engine::merge::{KWayMerger, MergeStep, RowData};
    let mut merger = KWayMerger::new(
        vec![out.join("test_percell/pcell/nb-3-big-Data.db")],
        &schema,
    )
    .expect("merger");
    let (mut live_cks, mut key_cell_cks) = (Vec::new(), Vec::new());
    loop {
        match merger.step().expect("step") {
            MergeStep::Complete => break,
            MergeStep::Partition { rows, .. } => {
                for e in rows {
                    let ck = match e.clustering_key.as_ref().and_then(|c| c.columns.first()) {
                        Some((_, Value::Integer(i))) => *i,
                        _ => continue,
                    };
                    let RowData::Live { cells } = &e.row_data else {
                        continue;
                    };
                    let has_key = cells.iter().any(|c| c.column == "ck");
                    let has_live = cells.iter().any(|c| {
                        c.column != "ck" && !matches!(c.value, Value::Tombstone(_)) && !c.is_deleted
                    });
                    if has_key {
                        key_cell_cks.push(ck);
                    }
                    if has_live {
                        live_cks.push(ck);
                    }
                }
            }
        }
    }
    live_cks.sort();
    // pk=1 rows with live data: ck 1, 2, 3 (pk 2/3 rows also appear, with ck=1 - filter by count below)
    assert!(
        live_cks.contains(&2),
        "ck=2 must survive compaction: {live_cks:?}"
    );
    assert!(
        !live_cks.contains(&4) && !live_cks.contains(&5),
        "tombstone-only rows are not live: {live_cks:?}"
    );
    assert!(
        key_cell_cks.contains(&2),
        "ck=2 must keep its clustering key cell: {key_cell_cks:?}"
    );
    assert!(
        !key_cell_cks.contains(&4),
        "ck=4 (row deleted, only an older cell) must not keep a key-only row: {key_cell_cks:?}"
    );
    assert!(!key_cell_cks.contains(&5), "ck=5 (row deleted, only a newer cell tombstone) must not keep a key-only row: {key_cell_cks:?}");
}
