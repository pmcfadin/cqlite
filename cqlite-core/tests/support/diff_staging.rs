//! Shared staging for the issue #4201 `cqlite diff` tests: copy a Cassandra-written table
//! directory's generations into two temp directories (side A / side B), resolve schemas, and turn
//! a staged directory into a [`DiffSide`]. Nothing here computes an expected value — those come
//! from the committed `sstabledump` JSONL goldens or from `cassandra-5.0.8` source, cited at each
//! assertion.

#![allow(dead_code)]

use std::path::{Path, PathBuf};

use cqlite_core::schema::cql_parser::{
    classify_statement, parse_create_table, split_cql_statements, StatementType,
};
use cqlite_core::schema::TableSchema;
use cqlite_core::storage::write_engine::snapshot_diff::DiffSide;

// Declared by the including test target next to this module (`#[path = "support/datasets_root.rs"]
// mod datasets_root;`), the same convention `salvage_corpus.rs` documents.
use super::datasets_root;

/// `test_tomb.resurrection_gc_positive` (datasets-v4, Cassandra 5.0.8): two generations; every
/// literal below is read from `nb-2-big-Data.db.jsonl` (`local_delete_time` 2026-11-11T00:53:44Z)
/// and `nb-2-big-Statistics.db.txt` ("SSTable min/max local deletion time" 1794358424).
pub const GC_POSITIVE_LDT: i64 = 1_794_358_424;
/// `gc_grace_seconds` declared in `test-data/schemas/tombstone-parity.cql` table 5.
pub const GC_POSITIVE_GC_GRACE: i64 = 864_000;
/// `ldt + gc_grace`: at exactly this `now` the tombstones are NOT yet purgeable (strict `<`,
/// cassandra-5.0.8 `PurgeFunction.java:41-42`).
pub const GC_POSITIVE_BOUNDARY: i64 = GC_POSITIVE_LDT + GC_POSITIVE_GC_GRACE;

pub fn require_fixtures_strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

/// The generation directory of a fixture table, or — when the (fetched, not committed) dataset
/// is absent — a SKIP-clean `None`, which `CQLITE_REQUIRE_FIXTURES=1` turns into a panic.
pub fn fixture_dir(keyspace: &str, table: &str) -> Option<PathBuf> {
    let dir = datasets_root::sstables_root_for_table(keyspace, table).and_then(|root| {
        datasets_root::table_generation_dirs(&root, keyspace, table)
            .into_iter()
            .next()
    });
    if dir.is_none() {
        let reason = datasets_root::describe_search(keyspace, table);
        if require_fixtures_strict() {
            panic!("CQLITE_REQUIRE_FIXTURES=1 but {keyspace}.{table} is absent: {reason}");
        }
        eprintln!("[skip] {keyspace}.{table}: {reason}");
    }
    dir
}

pub fn load_schema(schema_file: &str, keyspace: &str, table: &str) -> TableSchema {
    let path = datasets_root::schema_path(schema_file)
        .unwrap_or_else(|| panic!("committed schema fixture {schema_file} not found"));
    let content = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    for statement in split_cql_statements(&content) {
        if !matches!(classify_statement(&statement), StatementType::CreateTable) {
            continue;
        }
        let (_, mut schema) = parse_create_table(&statement)
            .unwrap_or_else(|error| panic!("parse CREATE TABLE in {}: {error:?}", path.display()));
        if schema.table.eq_ignore_ascii_case(table) {
            schema.keyspace = keyspace.to_string();
            return schema;
        }
    }
    panic!("table {keyspace}.{table} not found in {}", path.display());
}

/// Copy every file of `src` whose name starts with one of `generation_prefixes`
/// (e.g. `"nb-1-big-"`) into a fresh subdirectory `name` of `root`, returning that directory.
pub fn stage(root: &Path, name: &str, src: &Path, generation_prefixes: &[&str]) -> PathBuf {
    let dir = root.join(name);
    std::fs::create_dir_all(&dir).expect("create staged side dir");
    let mut copied = 0;
    for entry in std::fs::read_dir(src).expect("read fixture dir") {
        let entry = entry.expect("dir entry");
        let file_name = entry.file_name().to_string_lossy().into_owned();
        if entry.path().is_file()
            && generation_prefixes
                .iter()
                .any(|prefix| file_name.starts_with(prefix))
        {
            std::fs::copy(entry.path(), dir.join(&file_name)).expect("copy fixture file");
            copied += 1;
        }
    }
    assert!(
        copied > 0,
        "staging {generation_prefixes:?} from {} copied nothing",
        src.display()
    );
    dir
}

/// A [`DiffSide`] from a staged flat directory: every `*-Data.db` with a sibling `*-TOC.txt`,
/// newest generation first (so `run_index` 0 is the last-write-wins newest, matching every other
/// merger caller).
pub fn side_of(dir: &Path) -> DiffSide {
    let mut found: Vec<(u64, PathBuf)> = Vec::new();
    for entry in std::fs::read_dir(dir).expect("read staged dir") {
        let path = entry.expect("dir entry").path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        let Some(stem) = name.strip_suffix("-Data.db") else {
            continue;
        };
        if !dir.join(format!("{stem}-TOC.txt")).is_file() {
            continue;
        }
        let generation = stem
            .split('-')
            .nth(1)
            .and_then(|g| g.parse::<u64>().ok())
            .unwrap_or(0);
        found.push((generation, path));
    }
    found.sort_by_key(|entry| std::cmp::Reverse(entry.0));
    assert!(
        !found.is_empty(),
        "no generations staged in {}",
        dir.display()
    );
    DiffSide {
        dir: dir.to_path_buf(),
        generations: found.into_iter().map(|(_, p)| p).collect(),
    }
}

/// `test_diff.risk_cases` (committed Cassandra 5.0.8 fixture, `test-data/fixtures/issue_4201`):
/// MUST be present — it is committed, so a miss is a failure, never a skip.
pub fn risk_cases_dir() -> PathBuf {
    let root = datasets_root::repo_root().join("test-data/fixtures/issue_4201");
    let mut dirs = datasets_root::table_generation_dirs(&root, "test_diff", "risk_cases");
    assert_eq!(
        dirs.len(),
        1,
        "committed risk_cases fixture under {}",
        root.display()
    );
    dirs.remove(0)
}

/// `test_explain.trace_decisions` (committed Cassandra 5.0.8 fixture, issue #4193): MUST be present.
pub fn trace_decisions_dir() -> PathBuf {
    let root = datasets_root::sstables_root_for_table("test_explain", "trace_decisions")
        .unwrap_or_else(|| {
            panic!(
                "{}",
                datasets_root::describe_search("test_explain", "trace_decisions")
            )
        });
    datasets_root::table_generation_dirs(&root, "test_explain", "trace_decisions")
        .into_iter()
        .next()
        .expect("committed test_explain.trace_decisions generation dir")
}

/// The raw partition-key bytes for an `int` partition key (4-byte big-endian).
pub fn int_key(value: i32) -> Vec<u8> {
    value.to_be_bytes().to_vec()
}

pub fn partition<'a>(report: &'a serde_json::Value, key: &str) -> &'a serde_json::Value {
    report["partitions"]
        .as_array()
        .expect("partitions array")
        .iter()
        .find(|p| p["partition_key"] == key)
        .unwrap_or_else(|| panic!("partition {key} not in report"))
}

/// `(clustering, column) -> cell entry` within one partition entry.
pub fn cell<'a>(
    partition: &'a serde_json::Value,
    clustering: &str,
    column: &str,
) -> &'a serde_json::Value {
    partition["cells"]
        .as_array()
        .expect("cells array")
        .iter()
        .find(|c| c["clustering"] == clustering && c["column"] == column)
        .unwrap_or_else(|| panic!("cell ({clustering}, {column}) not in partition"))
}

/// Tombstone entries of one kind within a partition entry.
pub fn tombstones_of<'a>(
    partition: &'a serde_json::Value,
    kind: &str,
) -> Vec<&'a serde_json::Value> {
    partition["tombstones"]
        .as_array()
        .expect("tombstones array")
        .iter()
        .filter(|t| t["kind"] == kind)
        .collect()
}

pub fn shadowed(tombstone: &serde_json::Value) -> Vec<(String, String)> {
    tombstone["resurrection_risk"]["shadowed_cells"]
        .as_array()
        .map(|cells| {
            cells
                .iter()
                .map(|c| {
                    (
                        c["clustering"].as_str().unwrap_or("<static>").to_string(),
                        c["column"].as_str().unwrap_or_default().to_string(),
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}
