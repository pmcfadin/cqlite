//! Issue #4645: a read of ONE generation must return non-frozen collections with element
//! tombstones the way Cassandra does: a deleted set/list member or map entry is omitted, the
//! survivors stay in cell-path order, a whole-collection delete reads null, and a collection
//! whose every element is deleted reads null.
//!
//! Oracles: (1) the live Cassandra 5.0.8 CL=ALL read beside `issue_4645/da-3-bti`, which is
//! Cassandra's OWN compacted single generation (map, set and list element deletes, a whole
//! collection overwrite, whole-collection deletes, all elements deleted); (2) `issue_4640/da-2-bti`
//! read ALONE, whose expected rows are taken from its `sstabledump -t` (the Cassandra-written
//! generation of the issue's repro). Scan, partition and point reads under Auto and Full; the
//! column set is asserted both ways (#3890).

#![cfg(all(feature = "write-support", feature = "cli-helpers"))]

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use cqlite_core::config::ReadPathMode;
use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::{Config, Database, Value};

const KEY_COLS: &[&str] = &["id", "c"];
const DATA_COLS: &[&str] = &["v", "s", "l", "m"];
/// The collection columns: absent or `Null` exactly when the oracle says `null`.
const COLLECTIONS: &[&str] = &["s", "l", "m"];

type Rows = BTreeMap<Vec<i32>, Vec<String>>;

fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../test-data/fixtures")
}

async fn open_at(schema: &Path, data_dir: &Path, mode: ReadPathMode) -> Database {
    let mut core_config = Config::default();
    core_config.query.forced_read_path = Some(mode);
    ingest(IngestionConfig {
        schema_paths: vec![schema.to_path_buf()],
        data_dir: data_dir.to_path_buf(),
        version_hint: None,
        core_config,
        table_directory_filter: None,
    })
    .await
    .expect("ingestion succeeds")
    .database
}

/// cqlsh rendering of a value (`[0, 0]`, `{1, 2}`, `{1: 'a0'}`, `v300`, `null`).
fn render(v: Option<&Value>, nested: bool) -> String {
    let join = |items: Vec<String>| items.join(", ");
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
            format!(
                "[{}]",
                join(items.iter().map(|i| render(Some(i), true)).collect())
            )
        }
        Some(Value::Set(items)) => {
            format!(
                "{{{}}}",
                join(items.iter().map(|i| render(Some(i), true)).collect())
            )
        }
        Some(Value::Map(entries)) => format!(
            "{{{}}}",
            join(
                entries
                    .iter()
                    .map(|(k, v)| format!("{}: {}", render(Some(k), true), render(Some(v), true)))
                    .collect()
            )
        ),
        Some(other) => format!("UNEXPECTED {other:?}"),
    }
}

/// The CL=ALL oracle file, parsed to `key columns -> rendered data columns` in `DATA_COLS`
/// order (the file's own column order is `l | m | s | v`).
fn oracle() -> Rows {
    oracle_at("issue_4645", [5, 4, 2, 3], 4)
}

/// `file`'s `<dir>/live-select-consistency-all.txt`; `order` = the file columns holding
/// `v, s, l, m`.
fn oracle_at(dir: &str, order: [usize; 4], rows: usize) -> Rows {
    let text =
        std::fs::read_to_string(fixtures().join(dir).join("live-select-consistency-all.txt"))
            .expect("oracle file is committed (must_run)");
    let mut out = Rows::new();
    for line in text.lines() {
        let cells: Vec<&str> = line.split('|').map(str::trim).collect();
        if cells.first().and_then(|c| c.parse::<i32>().ok()).is_none() {
            continue;
        }
        assert_eq!(cells.len(), 6, "oracle row: {line}");
        let key = vec![cells[0].parse().unwrap(), cells[1].parse().unwrap()];
        let data = order.iter().map(|&i| cells[i].to_string()).collect();
        out.insert(key, data);
    }
    assert_eq!(out.len(), rows, "the CL=ALL oracle row count");
    out
}

/// Render a result into `key -> data`, asserting the column set both ways (#3890): no
/// undeclared column, the key columns and `v` present, and a collection column present
/// (non-null) exactly when the oracle row has one.
fn rows_of(result: &cqlite_core::QueryResult, want: &Rows, ctx: &str) -> Rows {
    let declared: BTreeSet<&str> = KEY_COLS.iter().chain(DATA_COLS).copied().collect();
    let mut out = Rows::new();
    for row in &result.rows {
        let present: BTreeSet<&str> = row.values.keys().map(|k| &**k).collect();
        assert!(
            present.is_subset(&declared),
            "{ctx}: undeclared column in {present:?}"
        );
        for c in KEY_COLS {
            assert!(present.contains(c), "{ctx}: key column {c} missing");
        }
        let key: Vec<i32> = KEY_COLS
            .iter()
            .map(|c| match row.get(c) {
                Some(Value::Integer(n)) => *n,
                other => panic!("{ctx}: {c} is {other:?}"),
            })
            .collect();
        let data: Vec<String> = DATA_COLS
            .iter()
            .map(|c| render(row.get(c), false))
            .collect();
        if let Some(expected) = want.get(&key) {
            for (i, c) in DATA_COLS.iter().enumerate() {
                let live = expected[i] != "null";
                let has = row.get(c).is_some_and(|v| !matches!(v, Value::Null));
                assert_eq!(has, live, "{ctx} {key:?}: column {c} presence");
            }
        }
        assert!(
            out.insert(key.clone(), data).is_none(),
            "{ctx}: duplicate row {key:?}"
        );
    }
    let _ = COLLECTIONS;
    out
}

/// Scan, per-partition and fully-specified point reads, under Auto and Full.
async fn assert_every_read_path(
    schema: &Path,
    data_dir: &Path,
    table: &str,
    want: &Rows,
    absent_keys: &[i32],
    label: &str,
) {
    assert!(!want.is_empty(), "{label}: a vacuous oracle");
    for (mode, name) in [(ReadPathMode::Auto, "auto"), (ReadPathMode::Full, "full")] {
        let db = open_at(schema, data_dir, mode).await;
        let ctx = format!("{label} {name}");

        let all = db
            .execute(&format!("SELECT * FROM {table}"))
            .await
            .expect("scan");
        assert_eq!(
            &rows_of(&all, want, &ctx),
            want,
            "{ctx}: SELECT * differs from the oracle"
        );

        let ids: BTreeSet<i32> = want.keys().map(|k| k[0]).collect();
        let mut union = Rows::new();
        for id in ids.iter().chain(absent_keys) {
            let r = db
                .execute(&format!("SELECT * FROM {table} WHERE id = {id}"))
                .await
                .unwrap_or_else(|e| panic!("{ctx} id={id}: {e}"));
            union.extend(rows_of(&r, want, &format!("{ctx} id={id}")));
        }
        assert_eq!(
            &union, want,
            "{ctx}: partition reads differ from the oracle"
        );

        for (key, data) in want {
            let r = db
                .execute(&format!(
                    "SELECT * FROM {table} WHERE id = {} AND c = {}",
                    key[0], key[1]
                ))
                .await
                .unwrap_or_else(|e| panic!("{ctx} point {key:?}: {e}"));
            let got = rows_of(&r, want, &format!("{ctx} point {key:?}"));
            assert_eq!(got.len(), 1, "{ctx} point {key:?}: one row");
            assert_eq!(got.values().next(), Some(data), "{ctx} point {key:?}");
        }
    }
}

/// Map/set/list element deletes, a mid-list element delete (`[10, <dead>, 30, 40]` must read
/// `[10, 30, 40]`), a whole-map overwrite, a collection whose every element is deleted
/// (null) and whole-collection deletes, all in Cassandra's own compacted generation.
#[tokio::test]
async fn cassandra_compacted_single_generation_matches_live_cl_all() {
    let root = fixtures().join("issue_4645");
    let want = oracle();
    // Sanity that the oracle really exercises the cases (not a vacuous pass).
    assert_eq!(
        want[&vec![3, 0]],
        [
            "g1",
            "{1, 3, 4}",
            "[10, 30, 40]",
            "{1: 'a', 3: 'c', 4: 'd'}"
        ]
    );
    assert_eq!(want[&vec![3, 1]], ["g1", "{7, 8}", "[70, 80]", "{9: 'z'}"]);
    assert_eq!(want[&vec![4, 0]], ["g1", "null", "null", "null"]);
    assert_eq!(want[&vec![5, 0]], ["g1", "null", "[5, 6]", "null"]);
    assert_every_read_path(
        &root.join("schema.cql"),
        &root.join("sstables"),
        "fix4645.t",
        &want,
        &[],
        "4645",
    )
    .await;
}

/// `da-2-bti` of the #4640 fixture ALONE (Cassandra-written; the issue's repro returned
/// `m = {2: null, 4: d}`). Expected rows are its `sstabledump -t`: key 3 holds element
/// tombstones for `s{2}`, `m{2}` and a list element beside the live `4`/`d`/`40`; key 4 a
/// bare `s+{9}`; key 5 only an element tombstone (no row).
#[tokio::test]
async fn cassandra_written_da2_alone_omits_deleted_elements() {
    let root = fixtures().join("issue_4640");
    let table_dir = std::fs::read_dir(root.join("sstables/fix4640"))
        .expect("table dir")
        .flatten()
        .map(|e| e.path())
        .find(|p| p.is_dir())
        .expect("table dir");
    let tmp = tempfile::TempDir::new().unwrap();
    let dest = tmp
        .path()
        .join("sstables/fix4640")
        .join(table_dir.file_name().unwrap());
    std::fs::create_dir_all(&dest).unwrap();
    let mut copied = 0;
    for e in std::fs::read_dir(&table_dir).unwrap().flatten() {
        let name = e.file_name().to_string_lossy().into_owned();
        if name.starts_with("da-2-bti-") {
            std::fs::copy(e.path(), dest.join(&name)).unwrap();
            copied += 1;
        }
    }
    assert!(copied >= 7, "all da-2-bti components copied, got {copied}");

    let mut want = Rows::new();
    want.insert(
        vec![3, 0],
        ["null", "{4}", "[40]", "{4: 'd'}"]
            .map(String::from)
            .to_vec(),
    );
    want.insert(
        vec![4, 0],
        ["null", "{9}", "null", "null"].map(String::from).to_vec(),
    );
    assert_every_read_path(
        &root.join("schema.cql"),
        &tmp.path().join("sstables"),
        "fix4640.t",
        &want,
        &[5],
        "4640-da2",
    )
    .await;
}

/// CQLite's own compacted output of the #4640 fixture (a single BIG `nb-10-big`, which Cassandra
/// 5.0.8 `sstableverify` accepts and whose live CL=ALL read is the #4640 oracle). Its list cells
/// are NOT in comparator order on disk (`[40, 10, 30]` before #4645) and its map holds the
/// element tombstone for key 2: the read must give `[10, 30, 40]` and omit the entry.
#[tokio::test]
async fn cqlite_compacted_single_generation_reads_in_cell_path_order() {
    let root = fixtures().join("issue_4645/cqlite_compacted_4640");
    let want = oracle_at("issue_4640", [2, 3, 4, 5], 5);
    assert_eq!(
        want[&vec![3, 0]],
        [
            "g1",
            "{1, 3, 4, 5}",
            "[10, 30, 40]",
            "{1: 'a2', 3: 'c', 4: 'd'}"
        ]
    );
    assert_every_read_path(
        &fixtures().join("issue_4640/schema.cql"),
        &root.join("sstables"),
        "fix4640.t",
        &want,
        &[1],
        "4640-cqlite-compacted",
    )
    .await;
}
