//! Issue #4640: a `SELECT` that merges more than one generation must return each
//! non-frozen collection COMPLETE, not collapsed to its last element.
//!
//! Oracle: the live Cassandra 5.0.8 CL=ALL read committed beside each Cassandra-written
//! fixture (`live-select-consistency-all.txt`): `issue_4628` (list + map, three BTI
//! generations, `fix4628.t`) and `issue_4640` (set + list + map with element deletes and
//! overwrites across three generations, `fix4640.t`). Every key is read through the
//! partition-targeted path (`WHERE id = k`) and the whole-table scan (`SELECT *`) under
//! `ReadPathMode::Auto` and `ReadPathMode::Full`, and compared on the FULL row. Per #3890
//! the column set is asserted in both directions. A third test compacts the three
//! generations through the core API and re-reads the output.

#![cfg(all(feature = "write-support", feature = "cli-helpers"))]

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use cqlite_core::config::ReadPathMode;
use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::{Config, Database, Value};

/// One Cassandra-written fixture and its live CL=ALL oracle.
struct Fixture {
    dir: &'static str,
    /// `keyspace.table`
    table: &'static str,
    /// Names of the clustering+partition key columns, in oracle column order.
    key_cols: &'static [&'static str],
    /// The non-key columns, in oracle column order.
    data_cols: &'static [&'static str],
    /// Expected oracle row count (guards against an empty/mis-parsed oracle).
    rows: usize,
}

const F4628: Fixture = Fixture {
    dir: "issue_4628",
    table: "fix4628.t",
    key_cols: &["id", "c1", "c2"],
    data_cols: &["v", "l", "m"],
    rows: 22,
};
const F4640: Fixture = Fixture {
    dir: "issue_4640",
    table: "fix4640.t",
    key_cols: &["id", "c"],
    data_cols: &["v", "s", "l", "m"],
    rows: 5,
};

type Rows = BTreeMap<Vec<i32>, Vec<String>>;

fn fixture_root(f: &Fixture) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../test-data/fixtures")
        .join(f.dir)
}

async fn open_at(f: &Fixture, data_dir: &Path, mode: ReadPathMode) -> Database {
    let mut core_config = Config::default();
    core_config.query.forced_read_path = Some(mode);
    ingest(IngestionConfig {
        schema_paths: vec![fixture_root(f).join("schema.cql")],
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

/// The CL=ALL oracle file, parsed to `key columns -> rendered data columns`.
fn oracle(f: &Fixture) -> Rows {
    let text = std::fs::read_to_string(fixture_root(f).join("live-select-consistency-all.txt"))
        .expect("oracle file is committed (must_run)");
    let width = f.key_cols.len() + f.data_cols.len();
    let mut out = Rows::new();
    for line in text.lines() {
        let cells: Vec<&str> = line.split('|').map(str::trim).collect();
        if cells.first().and_then(|c| c.parse::<i32>().ok()).is_none() {
            continue; // banner, header, rule, row count
        }
        assert!(
            cells.len() >= width,
            "oracle row has {width} columns: {line}"
        );
        let key = cells[..f.key_cols.len()]
            .iter()
            .map(|c| c.parse().unwrap())
            .collect();
        let data = cells[f.key_cols.len()..width]
            .iter()
            .map(|c| c.to_string())
            .collect();
        out.insert(key, data);
    }
    assert_eq!(out.len(), f.rows, "the CL=ALL oracle row count");
    out
}

/// Render a result into `key -> data`, asserting the column set both ways (#3890): every
/// declared column is present except a null collection, and no undeclared column appears.
fn rows_of(f: &Fixture, result: &cqlite_core::QueryResult, ctx: &str) -> Rows {
    let declared: BTreeSet<&str> = f.key_cols.iter().chain(f.data_cols).copied().collect();
    let mut out = Rows::new();
    for row in &result.rows {
        let present: BTreeSet<&str> = row.values.keys().map(|k| &**k).collect();
        assert!(
            present.is_subset(&declared),
            "{ctx}: undeclared column in {present:?}"
        );
        for c in f.key_cols.iter().chain(&f.data_cols[..1]) {
            assert!(
                present.contains(c),
                "{ctx}: column {c} missing, got {present:?}"
            );
        }
        let key: Vec<i32> = f
            .key_cols
            .iter()
            .map(|c| match row.get(c) {
                Some(Value::Integer(n)) => *n,
                other => panic!("{ctx}: {c} is {other:?}"),
            })
            .collect();
        let data = f
            .data_cols
            .iter()
            .map(|c| render(row.get(c), false))
            .collect();
        assert!(
            out.insert(key.clone(), data).is_none(),
            "{ctx}: duplicate row {key:?}"
        );
    }
    out
}

fn partition_ids(want: &Rows) -> BTreeSet<i32> {
    want.keys().map(|k| k[0]).collect()
}

/// Scan, per-partition and fully-specified point reads, under Auto and Full.
async fn assert_every_read_path(f: &Fixture, data_dir: &Path, label: &str) {
    let want = oracle(f);
    for (mode, name) in [(ReadPathMode::Auto, "auto"), (ReadPathMode::Full, "full")] {
        let db = open_at(f, data_dir, mode).await;
        let ctx = format!("{label} {name}");

        let all = db
            .execute(&format!("SELECT * FROM {}", f.table))
            .await
            .expect("scan");
        assert_eq!(
            rows_of(f, &all, &ctx),
            want,
            "{ctx}: SELECT * differs from CL=ALL"
        );

        // Partition reads; key 1 (4628) is wholly deleted and must yield nothing.
        let mut union = Rows::new();
        for id in partition_ids(&want).into_iter().chain([1]) {
            let r = db
                .execute(&format!("SELECT * FROM {} WHERE id = {id}", f.table))
                .await
                .unwrap_or_else(|e| panic!("{ctx} id={id}: {e}"));
            union.extend(rows_of(f, &r, &format!("{ctx} id={id}")));
        }
        assert_eq!(union, want, "{ctx}: partition reads differ from CL=ALL");

        for (key, data) in &want {
            let pred: Vec<String> = f
                .key_cols
                .iter()
                .zip(key)
                .map(|(c, v)| format!("{c} = {v}"))
                .collect();
            let r = db
                .execute(&format!(
                    "SELECT * FROM {} WHERE {}",
                    f.table,
                    pred.join(" AND ")
                ))
                .await
                .unwrap_or_else(|e| panic!("{ctx} point {key:?}: {e}"));
            let got = rows_of(f, &r, &format!("{ctx} point {key:?}"));
            assert_eq!(got.len(), 1, "{ctx} point {key:?}: one row");
            assert_eq!(got.values().next(), Some(data), "{ctx} point {key:?}");
        }
    }
}

#[tokio::test]
async fn list_and_map_across_generations_match_live_cl_all() {
    assert_every_read_path(&F4628, &fixture_root(&F4628).join("sstables"), "4628").await;
}

/// Set, list and map with an element tombstone in a newer generation shadowing an older
/// element (`s - {2}`, `m - {2}`, `l - [20]`, `DELETE m[6]`), an element re-added later,
/// an element overwritten (`m[1]`), a whole-collection overwrite (`l = [99]`) and a whole
/// collection deleted (`DELETE s`).
#[tokio::test]
async fn set_list_map_element_tombstones_across_generations_match_live_cl_all() {
    assert_every_read_path(&F4640, &fixture_root(&F4640).join("sstables"), "4640").await;
    // Sanity that the oracle really exercises the cases (not a vacuous pass).
    let want = oracle(&F4640);
    assert_eq!(
        want[&vec![3, 0]],
        [
            "g1",
            "{1, 3, 4, 5}",
            "[10, 30, 40]",
            "{1: 'a2', 3: 'c', 4: 'd'}"
        ]
    );
}

fn count_data_files(dir: &Path) -> usize {
    std::fs::read_dir(dir)
        .unwrap()
        .flatten()
        .map(|e| {
            let p = e.path();
            if p.is_dir() {
                count_data_files(&p)
            } else {
                usize::from(p.to_string_lossy().ends_with("-Data.db"))
            }
        })
        .sum()
}

/// Compaction verdict (#4640): compact the three generations through the core API and
/// re-read the single output. The collections must be intact and equal the CL=ALL read.
#[tokio::test]
async fn compacted_multi_generation_collections_stay_intact() {
    use cqlite_core::storage::write_engine::merge::compact_sstables;

    for (f, label) in [(&F4640, "4640"), (&F4628, "4628")] {
        let src = fixture_root(f).join("sstables");
        let ks = f.table.split('.').next().unwrap();
        let table_dir = std::fs::read_dir(src.join(ks))
            .expect("keyspace dir")
            .flatten()
            .map(|e| e.path())
            .find(|p| p.is_dir())
            .expect("table dir");
        let mut inputs: Vec<(u64, PathBuf)> = std::fs::read_dir(&table_dir)
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.to_string_lossy().ends_with("-bti-Data.db"))
            .map(|p| {
                let n = p.file_name().unwrap().to_string_lossy().into_owned();
                (n.split('-').nth(1).unwrap().parse().unwrap(), p)
            })
            .collect();
        inputs.sort_by_key(|(g, _)| std::cmp::Reverse(*g)); // newest first
        assert!(
            inputs.len() >= 3,
            "{label}: the fixture has several generations"
        );

        let ddl = std::fs::read_to_string(fixture_root(f).join("schema.cql")).unwrap();
        let create = &ddl[ddl.find("CREATE TABLE").expect("CREATE TABLE")..];
        let schema = cqlite_core::schema::parse_cql_schema(create).expect("schema");

        let tmp = tempfile::TempDir::new().unwrap();
        let out_root = tmp.path().join("sstables");
        std::fs::create_dir_all(&out_root).unwrap();
        compact_sstables(
            inputs.into_iter().map(|(_, p)| p).collect(),
            &out_root,
            &schema,
            10,
            None,
            None,
            false,
        )
        .await
        .unwrap_or_else(|e| panic!("{label}: compaction failed: {e}"));

        let data_files = count_data_files(&out_root);
        assert_eq!(data_files, 1, "{label}: one compacted generation");

        // The compacted output is ONE generation: it takes the single-generation read path,
        // independent of the multi-generation merge fixed above.
        assert_every_read_path(
            f,
            &tmp.path().join("sstables"),
            &format!("{label} compacted"),
        )
        .await;
    }
}
