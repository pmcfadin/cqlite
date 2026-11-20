//! Issue #4193 review M2 — the decision trail NAMES the `Cells.resolveRegular` rule that decided an
//! equal-timestamp tie (cassandra-5.0.8 `db/rows/Cells.java:79-129`).
//!
//! Real data only: the `test-data/fixtures/issue_4530/` generations were written by Cassandra 5.0.8
//! at the SAME client timestamp with conflicting content, and `cassandra-select.txt` is Cassandra's
//! own `SELECT` over them. For each case the trail's winner must be the version Cassandra's SELECT
//! returned, and the loser's `decided_by` must carry the rule below. Checked on both on-disk
//! arrangements (`ordered`, `swapped`), because Cassandra's rule does not depend on file order.
//!
//! The plain-timestamp (`Timestamp`) and tombstone-over-live rules are pinned against the
//! Cassandra-written `test_explain.trace_decisions` fixture in `issue_4193_verdict_fixtures.rs`.

#![cfg(all(feature = "write-support", feature = "cli-helpers"))]

use std::path::{Path, PathBuf};

use cqlite_core::schema::cql_parser::{
    classify_statement, parse_create_table, split_cql_statements, StatementType,
};
use cqlite_core::schema::TableSchema;
use cqlite_core::storage::partition_key_codec::encode_partition_key_columns;
use cqlite_core::storage::scan_cancel::ScanCancel;
use cqlite_core::storage::write_engine::merge::trace::{
    CellDecision, DecidedBy, ReconcileRule, RecordingSink, Verdict,
};
use cqlite_core::storage::write_engine::merge::{
    build_single_partition_merger_with_trace, effective_compaction_schema, MergeStep,
};
use cqlite_core::storage::write_engine::ClusteringKey;
use cqlite_core::types::Value;

#[path = "support/datasets_root.rs"]
mod datasets_root;

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace parent")
        .join("test-data/fixtures/issue_4530")
}

fn schema() -> TableSchema {
    let path =
        datasets_root::schema_path("issue-4530-equal-ts-ties.cql").expect("committed schema");
    let content = std::fs::read_to_string(&path).expect("read schema");
    for statement in split_cql_statements(&content) {
        if matches!(classify_statement(&statement), StatementType::CreateTable) {
            let (_, mut schema) = parse_create_table(&statement).expect("parse CREATE TABLE");
            schema.keyspace = "test_ties".into();
            return schema;
        }
    }
    panic!("no CREATE TABLE in {}", path.display());
}

/// Cassandra's `SELECT pk, ck, v` rows from the captured cqlsh output: `ck -> v`.
fn cassandra_select_pk1() -> Vec<(i32, String)> {
    let text = std::fs::read_to_string(fixture().join("cassandra-select.txt")).expect("capture");
    let rows: Vec<(i32, String)> = text
        .lines()
        .filter_map(|line| {
            let cols: Vec<&str> = line.split('|').map(str::trim).collect();
            (cols.len() == 4 && cols[0] == "1")
                .then(|| Some((cols[1].parse().ok()?, cols[2].to_string())))
                .flatten()
        })
        .collect();
    assert_eq!(
        rows.len(),
        6,
        "Cassandra's SELECT returned 6 pk=1 rows: {text}"
    );
    rows
}

/// Newest generation first (`run_index` 0), as every production caller feeds the merger.
fn generations(variant: &str) -> Vec<PathBuf> {
    let root = fixture().join(variant);
    let dirs = datasets_root::table_generation_dirs(&root, "test_ties", "ties");
    assert_eq!(dirs.len(), 1, "{variant}: one table dir");
    let dir: &Path = &dirs[0];
    vec![dir.join("nb-2-big-Data.db"), dir.join("nb-1-big-Data.db")]
}

fn explain_pk1(variant: &str) -> Vec<CellDecision> {
    let paths = generations(variant);
    let effective = effective_compaction_schema(&schema(), &paths);
    let key = encode_partition_key_columns(&[Value::Integer(1)], &effective).expect("key");
    let mut merger = build_single_partition_merger_with_trace(
        paths,
        &[key],
        &effective,
        ScanCancel::default(),
        RecordingSink::new(),
    )
    .expect("build traced merger")
    .expect("pk=1 is held by a generation")
    .with_now_secs(Some(1_794_532_600))
    .with_gc_before_secs(None)
    .with_purge_safe(false);
    while !matches!(merger.step().expect("step"), MergeStep::Complete) {}
    let (cells, _tombstones, _probes) = merger.into_trace_sink().into_parts();
    cells
}

fn text(c: &CellDecision) -> Option<String> {
    match c.value.as_ref()? {
        Value::Text(b) => Some(String::from_utf8_lossy(b).into_owned()),
        _ => None,
    }
}

/// The case's clustering row decided as Cassandra decided it: exactly one `Winner` carrying
/// Cassandra's SELECT value, and exactly one shadowed version whose `decided_by` names `rule`.
fn assert_tie(
    cells: &[CellDecision],
    ck: i32,
    oracle_value: &str,
    loser_value: &str,
    rule: ReconcileRule,
    variant: &str,
) {
    let key = ClusteringKey::single("ck", Value::Integer(ck));
    let row: Vec<&CellDecision> = cells
        .iter()
        .filter(|c| c.column == "v" && c.clustering.as_ref() == Some(&key))
        .collect();
    assert_eq!(
        row.len(),
        2,
        "{variant} ck={ck}: two versions expected, got {row:?}"
    );
    let winner = row.iter().find(|c| c.verdict == Verdict::Winner);
    let winner = winner.unwrap_or_else(|| panic!("{variant} ck={ck}: no Winner in {row:?}"));
    assert_eq!(
        text(winner).as_deref(),
        Some(oracle_value),
        "{variant} ck={ck}: the trail's winner must be Cassandra's SELECT value"
    );
    let loser = row
        .iter()
        .find(|c| c.verdict != Verdict::Winner)
        .expect("a loser");
    assert_eq!(
        text(loser).as_deref(),
        Some(loser_value),
        "{variant} ck={ck}"
    );
    match &loser.decided_by {
        DecidedBy::Winner {
            rule: got,
            writetime,
            ..
        } => {
            assert_eq!(*writetime, 1000, "{variant} ck={ck}: equal timestamp");
            assert_eq!(*got, Some(rule), "{variant} ck={ck}: wrong deciding rule");
        }
        other => panic!("{variant} ck={ck}: expected DecidedBy::Winner, got {other:?}"),
    }
}

#[test]
fn each_equal_timestamp_tie_names_the_cassandra_rule() {
    let oracle = cassandra_select_pk1();
    let want = |ck: i32| {
        oracle
            .iter()
            .find(|(c, _)| *c == ck)
            .map(|(_, v)| v.clone())
            .unwrap_or_else(|| panic!("ck {ck} missing from Cassandra's SELECT"))
    };
    for variant in ["ordered", "swapped"] {
        let cells = explain_pk1(variant);
        // value bytes: 'zebra' > 'apple' regardless of which generation holds it (ck 1 and 2).
        assert_tie(
            &cells,
            1,
            &want(1),
            "apple",
            ReconcileRule::GreaterValue,
            variant,
        );
        assert_tie(
            &cells,
            2,
            &want(2),
            "apple",
            ReconcileRule::GreaterValue,
            variant,
        );
        // expiring beats live, either arrangement (ck 3 and 4).
        assert_tie(
            &cells,
            3,
            &want(3),
            "live",
            ReconcileRule::ExpiringOverLive,
            variant,
        );
        assert_tie(
            &cells,
            4,
            &want(4),
            "live",
            ReconcileRule::ExpiringOverLive,
            variant,
        );
        // two expiring cells, larger localDeletionTime (= larger TTL here) wins (ck 5 and 6).
        assert_tie(
            &cells,
            5,
            &want(5),
            "small",
            ReconcileRule::LaterLocalDeletionTime,
            variant,
        );
        assert_tie(
            &cells,
            6,
            &want(6),
            "small",
            ReconcileRule::LaterLocalDeletionTime,
            variant,
        );
    }
}
