//! Issue #4524 — the marker-only row is a READ-path rule; COMPACTION keeps its pre-change behaviour.
//!
//! A liveness-only row of a clustering-less table (a pk-only `INSERT`, an explicit `null`) is
//! RETURNED by a Cassandra read (`AbstractRow.hasLiveData`), so the read merge emits an empty
//! `Live` entry carrying the real marker in `row_liveness`. The compaction write path ignores
//! `row_liveness`, so the same entry there would be written as a ts=0, TTL-less pk-only row (an
//! expired TTL'd row resurrected as an immortal one). Compaction therefore must NOT emit it; a
//! separate issue owns compaction correctness for these rows.
//!
//! Inputs: real Cassandra 5.0.8-written SSTables (`test-data/fixtures/issue_4524/test_nullrow`).
//! The compaction expectations below are the OUTPUT OF `origin/main` (built in a separate worktree
//! and target dir) for the same inputs, pinned as PROPERTIES (partition count, no ts=0 liveness
//! row, no empty row) rather than raw bytes.

#![cfg(all(feature = "state_machine", feature = "write-support", feature = "lz4"))]

use std::path::{Path, PathBuf};

use cqlite_core::schema::TableSchema;
use cqlite_core::storage::write_engine::merge::{
    compact_sstables, KWayMerger, MergeEntry, MergePurpose, MergeStep, RowData,
};

#[path = "support/datasets_root.rs"]
mod datasets_root;

const KEYSPACE: &str = "test_nullrow";
const SCHEMA: &str = "issue-4524-null-only-row.cql";

fn fixture_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace parent")
        .join("test-data/fixtures/issue_4524")
}

fn schema_of(table: &str) -> TableSchema {
    let path = datasets_root::schema_path(SCHEMA).expect("committed CQL schema");
    let cql = std::fs::read_to_string(path).expect("read schema");
    let start = cql
        .find(&format!("CREATE TABLE IF NOT EXISTS {KEYSPACE}.{table}"))
        .expect("CREATE TABLE statement");
    let end = start + cql[start..].find(';').expect("terminator") + 1;
    let mut t = cqlite_core::schema::cql_parser::parse_cql_schema(&cql[start..end])
        .expect("parse CREATE TABLE");
    t.keyspace = KEYSPACE.to_string();
    t
}

/// Committed Cassandra-written `*-Data.db` inputs of `table`, NEWEST generation first.
fn inputs(table: &str) -> Vec<PathBuf> {
    let dirs = datasets_root::table_generation_dirs(&fixture_root(), KEYSPACE, table);
    assert_eq!(dirs.len(), 1, "{table}: expected ONE committed table dir");
    let mut data: Vec<PathBuf> = std::fs::read_dir(&dirs[0])
        .expect("fixture dir")
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.to_string_lossy().ends_with("-Data.db"))
        .collect();
    assert!(!data.is_empty(), "{table}: no committed Data.db");
    data.sort();
    data.reverse();
    data
}

fn drain(mut merger: KWayMerger) -> Vec<MergeEntry> {
    let mut out = Vec::new();
    loop {
        match merger.step().expect("merge step") {
            MergeStep::Complete => return out,
            MergeStep::Partition { rows, .. } => out.extend(rows),
        }
    }
}

fn is_marker_only(e: &MergeEntry) -> bool {
    matches!(&e.row_data, RowData::Live { cells } if cells.is_empty()) && e.row_liveness.has_marker
}

fn open(paths: &[PathBuf], schema: &TableSchema, purpose: MergePurpose) -> KWayMerger {
    KWayMerger::new(paths.to_vec(), schema)
        .expect("merger")
        .with_purpose(purpose)
}

/// `(marker_only entries from a READ merge, marker_only entries from a COMPACTION merge)`.
fn marker_only_counts(table: &str) -> (usize, usize) {
    let (schema, paths) = (schema_of(table), inputs(table));
    let count = |p| {
        drain(open(&paths, &schema, p))
            .iter()
            .filter(|e| is_marker_only(e))
            .count()
    };
    (count(MergePurpose::Read), count(MergePurpose::Compaction))
}

/// Compact `table`'s committed inputs and describe the output as `(partitions, rows)` after
/// asserting its row-level properties.
async fn compact_output(table: &str) -> (usize, usize) {
    let (schema, paths) = (schema_of(table), inputs(table));
    let out = tempfile::tempdir().expect("tempdir");
    let report = compact_sstables(paths, out.path(), &schema, 9, None, None, true)
        .await
        .expect("compaction");
    // Everything dropped => a zero-byte Data.db (no partitions to read back).
    let data = report.output.data_path;
    let rows = if std::fs::metadata(&data).expect("output Data.db").len() == 0 {
        Vec::new()
    } else {
        drain(KWayMerger::new(vec![data], &schema).expect("reopen"))
    };
    for r in &rows {
        // The pre-change output of a clustering-less table never holds a liveness-only row:
        // no empty row and no ts=0 marker (a clustered table's writer legitimately emits ts=0
        // liveness on rows WITH data, so that half is asserted for clustering-less tables only).
        assert!(
            !matches!(&r.row_data, RowData::Live { cells } if cells.is_empty()),
            "{table}: compaction wrote an empty (liveness-only) row"
        );
        if schema.clustering_keys.is_empty() {
            assert_ne!(
                r.row_liveness.marker_timestamp,
                Some(0),
                "{table}: compaction wrote a ts=0 liveness marker"
            );
        }
    }
    let partitions = rows
        .iter()
        .map(|r| r.key.token)
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    (partitions, rows.len())
}

/// The split on real Cassandra-written bytes: the READ merge emits liveness-only entries for the
/// null-only rows, the COMPACTION merge of the same inputs emits none.
#[test]
fn read_merge_emits_marker_only_rows_and_compaction_merge_does_not() {
    for table in ["flat_rows", "ttl_liveness"] {
        let (read, compaction) = marker_only_counts(table);
        assert!(
            read > 0,
            "{table}: the read merge must emit its liveness-only rows"
        );
        assert_eq!(
            compaction, 0,
            "{table}: the compaction merge must not emit them"
        );
    }
    // A table WITH clustering columns never takes the marker-only branch in either mode.
    let (read, compaction) = marker_only_counts("clustered_rows");
    assert_eq!((read, compaction), (0, 0), "clustered_rows");
}

/// End-to-end `compact_sstables` over real inputs: output pinned to what `origin/main` writes.
#[tokio::test]
async fn compaction_output_is_unchanged_for_liveness_only_rows() {
    for (table, partitions, rows) in EXPECTED_COMPACTION {
        let got = compact_output(table).await;
        assert_eq!(got, (*partitions, *rows), "{table}: (partitions, rows)");
    }
}

/// `(table, partitions, rows)` of the compaction output, as produced by `origin/main`.
const EXPECTED_COMPACTION: &[(&str, usize, usize)] = &[
    ("flat_rows", 6, 6),
    ("multigen_rows", 5, 5),
    // The lone pk-only TTL'd row: pre-change compaction writes an EMPTY Data.db, not an
    // immortal ts=0 row.
    ("ttl_liveness", 0, 0),
    ("clustered_rows", 8, 9),
];
