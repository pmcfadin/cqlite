//! Issues #4524 / #4581 — the marker-only row is ONE rule for reads AND compactions.
//!
//! A liveness-only row of a clustering-less table (a pk-only `INSERT`, an explicit `null`) is
//! RETURNED by a Cassandra read (`AbstractRow.hasLiveData`) and KEPT by a Cassandra compaction
//! (`Row.Merger.merge` returns null only when liveness, deletion and data are all empty;
//! `BTreeRow.purge` keeps a live marker). #4524 fixed the read path first and had to keep
//! compaction DROPPING such rows, because the compaction write path ignored `row_liveness` and
//! would have rewritten the row as ts=0 with no TTL (resurrecting an expired TTL'd row as an
//! immortal one). That split (`MergePurpose::{Read, Compaction}`) is gone as of #4581: the writer
//! now carries the marker's real timestamp, TTL and `localExpirationTime`
//! (`Mutation::row_marker`), so there is nothing left to gate and the merge no longer knows why it
//! runs.
//!
//! This target pins what that split used to pin, inverted: the merge emits the marker-only
//! entries, the compaction OUTPUT holds each of them with its input liveness VERBATIM (never
//! ts=0, never TTL-less), and the clustered control is unchanged. The Cassandra-compacted
//! oracle for these shapes is `issue_4581_compaction_marker_rows.rs`.
//!
//! Inputs: real Cassandra 5.0.8-written SSTables (`test-data/fixtures/issue_4524/test_nullrow`).

#![cfg(all(feature = "state_machine", feature = "write-support", feature = "lz4"))]

use std::path::{Path, PathBuf};

use cqlite_core::schema::TableSchema;
use cqlite_core::storage::sstable::reader::compaction_row::RowLiveness;
use cqlite_core::storage::write_engine::merge::{
    compact_sstables, KWayMerger, MergeEntry, MergeStep, RowData,
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

/// Marker-only entries (ordered by partition token) of a merge over `paths`, as
/// `(partition token, row_liveness)`.
fn marker_only_rows(paths: &[PathBuf], schema: &TableSchema) -> Vec<(i64, RowLiveness)> {
    drain(KWayMerger::new(paths.to_vec(), schema).expect("merger"))
        .iter()
        .filter(|e| is_marker_only(e))
        .map(|e| (e.key.token, e.row_liveness))
        .collect()
}

/// The merge emits the liveness-only rows of the null-only tables, and none for a table WITH
/// clustering columns (its marker rides the clustering pseudo-cells).
#[test]
fn the_merge_emits_marker_only_rows_of_clustering_less_tables_only() {
    for table in ["flat_rows", "ttl_liveness"] {
        let found = marker_only_rows(&inputs(table), &schema_of(table));
        assert!(
            !found.is_empty(),
            "{table}: the merge must emit its liveness-only rows"
        );
        assert!(
            found
                .iter()
                .all(|(_, l)| l.marker_timestamp.is_some_and(|ts| ts != 0)),
            "{table}: a marker-only entry carries the marker's REAL timestamp, never ts=0"
        );
    }
    let clustered = marker_only_rows(&inputs("clustered_rows"), &schema_of("clustered_rows"));
    assert!(clustered.is_empty(), "clustered_rows");
}

/// End-to-end `compact_sstables` over real inputs: every marker-only row of the merged inputs is
/// in the output with its liveness VERBATIM (ts, TTL, expiry). `gc_before`/`now` are unset, so
/// nothing is purged or expired (the purge rules are pinned against Cassandra in
/// `issue_4581_compaction_marker_rows.rs`).
#[tokio::test]
async fn compaction_keeps_marker_only_rows_with_their_input_liveness() {
    for table in ["flat_rows", "ttl_liveness"] {
        let (schema, paths) = (schema_of(table), inputs(table));
        let expected = marker_only_rows(&paths, &schema);
        assert!(
            !expected.is_empty(),
            "{table}: fixture must hold marker-only rows"
        );

        let out = tempfile::tempdir().expect("tempdir");
        let report = compact_sstables(paths, out.path(), &schema, 9, None, None, true)
            .await
            .expect("compaction");
        let got = marker_only_rows(std::slice::from_ref(&report.output.data_path), &schema);
        assert_eq!(
            got, expected,
            "{table}: compaction must keep every marker-only row verbatim"
        );
    }
}

/// `(table, partitions, rows)` of the compaction output. Unlike the pre-#4581 pins these include
/// the marker-only rows Cassandra keeps (`flat_rows`: 6 data rows + the null-only ones).
#[tokio::test]
async fn compaction_output_row_counts() {
    for (table, partitions, rows) in EXPECTED_COMPACTION {
        let (schema, paths) = (schema_of(table), inputs(table));
        let out = tempfile::tempdir().expect("tempdir");
        let report = compact_sstables(paths, out.path(), &schema, 9, None, None, true)
            .await
            .expect("compaction");
        let entries =
            drain(KWayMerger::new(vec![report.output.data_path.clone()], &schema).expect("reopen"));
        let parts = entries
            .iter()
            .map(|r| r.key.token)
            .collect::<std::collections::BTreeSet<_>>()
            .len();
        assert_eq!(
            (parts, entries.len()),
            (*partitions, *rows),
            "{table}: (partitions, rows)"
        );
    }
}

const EXPECTED_COMPACTION: &[(&str, usize, usize)] = &[
    ("flat_rows", 7, 7),
    ("multigen_rows", 5, 5),
    // Both pk-only rows (id 1 USING TTL, id 2 live): kept, the TTL'd one with its TTL and LDT (an
    // immortal ts=0 row would be the bug). Pre-#4581 compaction wrote an EMPTY Data.db.
    ("ttl_liveness", 2, 2),
    ("clustered_rows", 8, 9),
];
