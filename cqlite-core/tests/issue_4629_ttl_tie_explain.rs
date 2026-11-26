//! Issue #4629 (0.19 field round F9): `explain` listed MORE THAN ONE `winner` for a cell when the
//! partition's generations tie on timestamp and one generation does not hold the key.
//!
//! Root cause: the point-read builder dropped a generation that does not hold the key from the run
//! list, so every later run's `MergeEntry::run_index` (its position in the caller's generation list,
//! which the trace reports) no longer equalled its slot in `KWayMerger::runs`. `refill_heap` indexes
//! by `run_index`, so a displaced run was never refilled inside its partition step and its remaining
//! rows were reconciled alone in a later step, each as its own un-shadowed `winner`.
//!
//! Oracle (NOT CQLite's own output): `test-data/fixtures/issue_4629/` holds four `da` generations
//! written by Cassandra 5.0.8 and the live `SELECT ... WRITETIME, TTL` at CONSISTENCY ALL taken from
//! the same cluster (`oracle/live-select-cl-all.txt`); `README.md` there has the regen recipe. The
//! cases: live vs expiring both ways round (keys 1, 3), expiring vs expiring with different
//! `localDeletionTime` both ways round (keys 4, 5), tombstone vs expiring (key 6), and a plain cell
//! duplicated in two generations plus an expiring one with an unrelated generation between (key 11).

#![cfg(all(feature = "write-support", feature = "cli-helpers"))]

use std::collections::BTreeMap;
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
use cqlite_core::types::Value;

/// The `--now` the field round used; far before every cell's expiry (`1795600000 + 1_000_000 s`).
const NOW: i64 = 1_795_662_946;
const GC_GRACE_SECONDS: i64 = 864_000;

fn fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../test-data/fixtures/issue_4629")
}

fn schema() -> TableSchema {
    let path = fixture().join("schema.cql");
    let content = std::fs::read_to_string(&path).expect("read fixture schema");
    for statement in split_cql_statements(&content) {
        if !matches!(classify_statement(&statement), StatementType::CreateTable) {
            continue;
        }
        let (_, mut schema) = parse_create_table(&statement).expect("parse CREATE TABLE");
        schema.keyspace = "fix4629".to_string();
        return schema;
    }
    panic!("no CREATE TABLE in {}", path.display());
}

/// Newest generation first, the order `cqlite explain` passes (`discover_live_generations`).
fn generations_newest_first() -> Vec<PathBuf> {
    let mut found: Vec<(u64, PathBuf)> = std::fs::read_dir(fixture().join("sstables"))
        .expect("fixture sstables dir")
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter_map(|path| {
            let name = path.file_name()?.to_str()?.to_string();
            let generation = name
                .strip_prefix("da-")?
                .strip_suffix("-bti-Data.db")?
                .parse()
                .ok()?;
            Some((generation, path))
        })
        .collect();
    found.sort_by_key(|(generation, _)| std::cmp::Reverse(*generation));
    assert_eq!(found.len(), 4, "the fixture holds da-1..da-4");
    found.into_iter().map(|(_, path)| path).collect()
}

fn explain_cells(id: i32) -> Vec<CellDecision> {
    let schema = schema();
    let paths = generations_newest_first();
    let effective = effective_compaction_schema(&schema, &paths);
    let key = encode_partition_key_columns(&[Value::Integer(id)], &effective).expect("key");
    let mut merger = build_single_partition_merger_with_trace(
        paths,
        &[key],
        &effective,
        ScanCancel::default(),
        RecordingSink::new(),
    )
    .expect("build traced merger")
    .unwrap_or_else(|| panic!("partition {id} is held by a generation"))
    .with_now_secs(Some(NOW))
    .with_gc_before_secs(Some(NOW - GC_GRACE_SECONDS))
    .with_purge_safe(true);
    loop {
        match merger.step().expect("merge step") {
            MergeStep::Complete => break,
            MergeStep::Partition { .. } => {}
        }
    }
    merger.into_trace_sink().into_parts().0
}

/// `(id, c) -> (v, has_ttl)` from the live `SELECT` at CONSISTENCY ALL; `v = None` is a CQL null.
fn live_answer() -> BTreeMap<(i32, i32), (Option<String>, bool)> {
    let text = std::fs::read_to_string(fixture().join("oracle/live-select-cl-all.txt"))
        .expect("live oracle");
    let mut rows = BTreeMap::new();
    for line in text.lines() {
        let cols: Vec<&str> = line.split('|').map(str::trim).collect();
        let (Some(id), Some(c)) = (
            cols.first().and_then(|s| s.parse::<i32>().ok()),
            cols.get(1).and_then(|s| s.parse::<i32>().ok()),
        ) else {
            continue;
        };
        let v = (cols[2] != "null").then(|| cols[2].to_string());
        rows.insert((id, c), (v, cols[4] != "null"));
    }
    assert_eq!(rows.len(), 24, "8 partitions x 3 rows in the live oracle");
    rows
}

fn clustering_c(cell: &CellDecision) -> i32 {
    let key = cell.clustering.as_ref().expect("clustered cell");
    match key.columns.first().map(|(_, v)| v) {
        Some(Value::Integer(c)) => *c,
        other => panic!("unexpected clustering {other:?}"),
    }
}

fn rule_of(cell: &CellDecision) -> Option<ReconcileRule> {
    match &cell.decided_by {
        DecidedBy::Winner { rule, .. } | DecidedBy::Tombstone { rule, .. } => *rule,
        _ => None,
    }
}

fn text(value: &Option<Value>) -> String {
    match value {
        Some(Value::Text(s)) => String::from_utf8_lossy(s.as_ref()).into_owned(),
        other => panic!("expected a text cell, got {other:?}"),
    }
}

/// Exactly one `winner` per `(row, v)`, equal to the live answer (value and TTL presence); a
/// tombstone winner (live null) has none.
#[test]
fn one_winner_per_cell_equal_to_the_live_answer() {
    let live = live_answer();
    for id in [1, 3, 4, 5, 6, 11, 13] {
        let cells = explain_cells(id);
        for c in 0..3 {
            let winners: Vec<&CellDecision> = cells
                .iter()
                .filter(|cell| {
                    cell.column == "v" && clustering_c(cell) == c && cell.verdict == Verdict::Winner
                })
                .collect();
            let (want_v, want_ttl) = &live[&(id, c)];
            match want_v {
                None => assert!(
                    winners.is_empty(),
                    "id {id} c {c}: live v is null but explain has winners {winners:?}"
                ),
                Some(want) => {
                    assert_eq!(
                        winners.len(),
                        1,
                        "id {id} c {c}: exactly one winner for v, got {winners:?}"
                    );
                    assert_eq!(&text(&winners[0].value), want, "id {id} c {c}: winner value");
                    assert_eq!(
                        winners[0].ttl.is_some(),
                        *want_ttl,
                        "id {id} c {c}: winner TTL presence"
                    );
                }
            }
        }
    }
}

/// The rule that decided each tie is named as Cassandra's `Cells.resolveRegular` decided it.
#[test]
fn losers_cite_the_rule_that_decided_them() {
    let expected = [
        (1, ReconcileRule::ExpiringOverLive),
        (3, ReconcileRule::ExpiringOverLive),
        (11, ReconcileRule::ExpiringOverLive),
        (4, ReconcileRule::LaterLocalDeletionTime),
        (5, ReconcileRule::LaterLocalDeletionTime),
        (6, ReconcileRule::TombstoneOverExpiring),
    ];
    for (id, rule) in expected {
        let cells = explain_cells(id);
        let losers: Vec<&CellDecision> = cells
            .iter()
            .filter(|c| c.column == "v" && c.verdict != Verdict::Winner)
            .collect();
        assert!(!losers.is_empty(), "id {id}: a tie has at least one loser");
        for loser in losers {
            assert_eq!(rule_of(loser), Some(rule), "id {id}: {loser:?}");
        }
    }
}
