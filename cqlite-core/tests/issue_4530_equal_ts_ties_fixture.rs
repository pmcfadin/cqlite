//! Issue #4530 — equal-timestamp reconcile ties against a CASSANDRA-WRITTEN fixture.
//!
//! `test-data/fixtures/issue_4530/` holds two SSTable generations written by Cassandra 5.0.8 at
//! the SAME client timestamp with conflicting content (generator
//! `test-data/scripts/generate-issue-4530-equal-ts-ties.sh`; the case table is in
//! `test-data/schemas/issue-4530-equal-ts-ties.cql`). Two on-disk arrangements of the same bytes:
//! `ordered/` (as written) and `swapped/` (generation numbers exchanged), so the file CQLite
//! treats as "newer" is the opposite half of every case. Cassandra's reconcile is independent of
//! that, CQLite's old first-seen tie-break was not.
//!
//! Oracles, all produced by Cassandra, none by CQLite:
//! * `cassandra-select.txt` — Cassandra's own `SELECT` over the two uncompacted generations;
//! * `expected.json` — the `localDeletionTime`s that survive Cassandra's `nodetool compact`,
//!   extracted by the generator from the compacted `sstabledump` golden (`compacted.jsonl`).
//!
//! Checked on BOTH arrangements: the public `SELECT` equals Cassandra's `SELECT`, and the
//! `KWayMerger` (the compaction merge) keeps the cell/row/partition-deletion LDTs Cassandra kept.

#![cfg(all(
    feature = "state_machine",
    feature = "cli-helpers",
    feature = "write-support"
))]

use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::schema::{ClusteringColumn, Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::merge::{KWayMerger, MergeStep, RowData};
use cqlite_core::types::Value;
use cqlite_core::Config;

#[path = "support/datasets_root.rs"]
mod datasets_root;

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace parent")
        .join("test-data/fixtures/issue_4530")
}

fn schema_path() -> PathBuf {
    datasets_root::schema_path("issue-4530-equal-ts-ties.cql").expect("committed schema")
}

fn table_dir(variant: &str) -> PathBuf {
    let root = fixture().join(variant);
    let dirs = datasets_root::table_generation_dirs(&root, "test_ties", "ties");
    assert_eq!(dirs.len(), 1, "{variant}: one table dir");
    dirs[0].clone()
}

/// Cassandra's `SELECT pk, ck, v` rows from the captured cqlsh output.
fn cassandra_select() -> Vec<(i32, i32, String)> {
    let text =
        std::fs::read_to_string(fixture().join("cassandra-select.txt")).expect("select capture");
    let mut out = Vec::new();
    for line in text.lines() {
        let cols: Vec<&str> = line.split('|').map(str::trim).collect();
        if cols.len() == 4 {
            if let (Ok(pk), Ok(ck)) = (cols[0].parse(), cols[1].parse()) {
                out.push((pk, ck, cols[2].to_string()));
            }
        }
    }
    assert_eq!(out.len(), 6, "Cassandra's SELECT returned 6 rows: {text}");
    out
}

fn expected_ldt(key: &str) -> i32 {
    let v: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(fixture().join("expected.json")).expect("expected.json"),
    )
    .expect("json");
    v[key]
        .as_i64()
        .unwrap_or_else(|| panic!("expected.json has no {key}")) as i32
}

async fn select_rows(variant: &str) -> Vec<(i32, i32, String)> {
    let result = ingest(IngestionConfig {
        schema_paths: vec![schema_path()],
        data_dir: fixture().join(variant),
        version_hint: None,
        core_config: Config::default(),
        table_directory_filter: None,
    })
    .await
    .expect("ingest");
    let res = result
        .database
        .execute("SELECT pk, ck, v FROM test_ties.ties")
        .await
        .expect("SELECT");
    let int = |r: &cqlite_core::query::result::QueryRow, c: &str| match r.values.get(c) {
        Some(Value::Integer(i)) => *i,
        other => panic!("{c}: {other:?}"),
    };
    let mut rows: Vec<_> = res
        .rows
        .iter()
        .map(|r| {
            let v = match r.values.get("v") {
                Some(Value::Text(b)) => Some(String::from_utf8_lossy(b).into_owned()),
                _ => None,
            };
            (int(r, "pk"), int(r, "ck"), v)
        })
        // A row whose only content is a cell TOMBSTONE (pk 1, ck 7) has no liveness and Cassandra's
        // SELECT omits it; CQLite returning it as a null-valued row is a separate read-completeness
        // question (see #4524), not a reconcile tie. It is excluded here so this test pins the tie.
        .filter_map(|(pk, ck, v)| v.map(|v| (pk, ck, v)))
        .collect();
    rows.sort();
    rows
}

fn data_files(dir: &Path) -> Vec<PathBuf> {
    // newest generation first, as the compactor feeds the merger
    vec![dir.join("nb-2-big-Data.db"), dir.join("nb-1-big-Data.db")]
}

#[tokio::test]
async fn select_over_two_equal_timestamp_generations_matches_cassandra() {
    let mut want = cassandra_select();
    want.sort();
    for variant in ["ordered", "swapped"] {
        assert_eq!(
            select_rows(variant).await,
            want,
            "{variant}: CQLite SELECT != Cassandra SELECT"
        );
    }
}

/// The compaction merge keeps the localDeletionTimes Cassandra's own compaction kept.
#[test]
fn compaction_merge_keeps_the_ldts_cassandra_kept() {
    // The committed CQL schema, spelled as a `TableSchema` (this file has no public CQL loader).
    let schema = TableSchema {
        keyspace: "test_ties".into(),
        table: "ties".into(),
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
        columns: vec![Column {
            name: "v".into(),
            data_type: "text".into(),
            nullable: true,
            default: None,
            is_static: false,
        }],
        comments: Default::default(),
        dropped_columns: Default::default(),
    };
    for variant in ["ordered", "swapped"] {
        let mut merger = KWayMerger::new(data_files(&table_dir(variant)), &schema).expect("merger");
        let (mut cell_tomb, mut row_del, mut part_del) = (None, None, None);
        let mut values = Vec::new();
        loop {
            match merger.step().expect("step") {
                MergeStep::Complete => break,
                MergeStep::Partition { rows, .. } => {
                    for e in rows {
                        let ck = e
                            .clustering_key
                            .as_ref()
                            .and_then(|c| match c.columns.first() {
                                Some((_, Value::Integer(i))) => Some(*i),
                                _ => None,
                            });
                        if let Some((_, ldt)) = e.partition_deletion {
                            part_del = Some(ldt);
                        }
                        match &e.row_data {
                            // (the partition-deletion carrier is also a `Tombstone` row; `pd` names it)
                            RowData::Tombstone {
                                local_deletion_time,
                                ..
                            } if e.partition_deletion.is_none() => {
                                row_del = Some(*local_deletion_time)
                            }
                            RowData::Tombstone { .. } => {}
                            RowData::Live { cells } => {
                                for c in cells {
                                    match (&c.value, ck) {
                                        (Value::Tombstone(info), Some(7)) => {
                                            cell_tomb = Some(info.local_deletion_time as i32)
                                        }
                                        (Value::Text(b), Some(k)) => values
                                            .push((k, String::from_utf8_lossy(b).into_owned())),
                                        _ => {}
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        values.sort();
        let want: Vec<(i32, String)> = cassandra_select()
            .into_iter()
            .filter(|(pk, _, _)| *pk == 1)
            .map(|(_, ck, v)| (ck, v))
            .collect();
        assert_eq!(values, want, "{variant}: merged cell values");
        assert_eq!(
            cell_tomb,
            Some(expected_ldt("cell_tombstone_ldt")),
            "{variant}: cell tombstone LDT"
        );
        assert_eq!(
            row_del,
            Some(expected_ldt("row_deletion_ldt")),
            "{variant}: row deletion LDT"
        );
        assert_eq!(
            part_del,
            Some(expected_ldt("partition_deletion_ldt")),
            "{variant}: partition deletion LDT"
        );
    }
}
