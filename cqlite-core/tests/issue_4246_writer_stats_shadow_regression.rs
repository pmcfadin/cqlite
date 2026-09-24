//! Issue #4246 regression: persisted `StatisticsMetadata` must be derived
//! from what the writer actually EMITS, never from every logical mutation it
//! receives.
//!
//! Before the fix, `WriteEngine::flush_internal_async`'s two-pass baseline
//! pre-scan (`SSTableWriter::compute_mutations_baseline_stats`, issue #729)
//! folded EVERY mutation's timestamp unconditionally — including a row fully
//! shadow-dropped by a covering partition/range tombstone and never written
//! to Data.db. That pre-scan LOCKS the encoding baseline before any
//! partition is written (`pre_seed_encoding_baselines`), and the baseline
//! can only ever be lowered further afterward, so the shadowed row's low
//! timestamp poisoned both the Data.db delta-VInt encoding and the persisted
//! `Statistics.db` minimum. This is the exact defect the #4243 Cassandra
//! open-ended range-boundary oracle caught (`issue_1383_rt_boundary_synthesis.rs`,
//! `oracle::crit5_cassandra_oracle_two_gen_open_ended_boundary`): CQLite
//! picked baseline 5 where Cassandra picks 10.
//!
//! This file is the PUBLIC, fixture-free writer regression the #4246
//! acceptance criteria calls for — exercised without any external Cassandra
//! fixture (unlike the oracle, this needs no `CQLITE_DATASETS_ROOT`), so it
//! always runs. Two properties, each with its own test:
//!   1. A row shadow-dropped by a covering tombstone must NOT lower
//!      persisted `min_timestamp` (`shadowed_old_row_does_not_lower_persisted_min_timestamp`).
//!   2. The **live-older-row control**: a genuinely LIVE row with an old
//!      timestamp (never covered by any tombstone) MUST still lower
//!      `min_timestamp` — proving the fix gates on actual shadow-drop, not on
//!      "old timestamp" as a heuristic (`live_older_row_retains_persisted_min_timestamp`).
//!
//! A third test combines both properties in one partition to prove
//! selectivity: the persisted minimum reflects the live older row, never the
//! (even lower) shadowed one.

#![cfg(feature = "write-support")]

use std::path::{Path, PathBuf};

use cqlite_core::parser::enhanced_statistics_parser::parse_statistics_with_fallback;
use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::merge::{KWayMerger, MergeStep, RowData};
use cqlite_core::storage::write_engine::mutation::{
    CellOperation, ClusteringBound, ClusteringKey, Mutation, PartitionKey, PartitionTombstone,
    RangeTombstone, TableId,
};
use cqlite_core::storage::write_engine::{WriteEngine, WriteEngineConfig};
use cqlite_core::types::Value;
use tempfile::TempDir;

const KS: &str = "issue_4246_ks";
const TBL: &str = "shadow_stats";

fn schema() -> TableSchema {
    TableSchema {
        keyspace: KS.to_string(),
        table: TBL.to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: vec![
            Column {
                name: "id".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "ck".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "name".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: Default::default(),
        dropped_columns: Default::default(),
    }
}

fn write_row(ck: i32, name: &str, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("id", Value::Integer(1)),
        Some(ClusteringKey::single("ck", Value::Integer(ck))),
        vec![CellOperation::Write {
            column: "name".to_string(),
            value: Value::text(name.to_string()),
        }],
        ts,
        None,
    )
}

/// A range-tombstone-only mutation for the pinned partition. A within-grace
/// (far-future) local deletion time so gc-grace never purges the marker.
fn range_delete(start: ClusteringBound, end: ClusteringBound, ts: i64) -> Mutation {
    let mut m = Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![],
        ts,
        None,
    );
    m.range_tombstones.push(RangeTombstone {
        start,
        end,
        deletion_time: ts,
        local_deletion_time: 2_000_000_000,
    });
    m
}

fn excl(ck: i32) -> ClusteringBound {
    ClusteringBound::Exclusive(ClusteringKey::single("ck", Value::Integer(ck)))
}

fn flush_batch(engine: &mut WriteEngine, rt: &tokio::runtime::Runtime, muts: Vec<Mutation>) {
    for m in muts {
        engine.write(m).expect("write");
    }
    rt.block_on(engine.flush())
        .expect("flush")
        .expect("sstable info");
}

/// Find the single `*-Statistics.db` under `dir` (recursively) and decode its
/// `min_timestamp`. Mirrors `issue_1385_gc_grace_boundary.rs`'s
/// `statistics_min_local_deletion_time` helper, one field over.
fn statistics_min_timestamp(dir: &Path) -> i64 {
    fn find_stats(dir: &Path, depth: usize) -> Option<PathBuf> {
        for entry in std::fs::read_dir(dir).ok()?.flatten() {
            let path = entry.path();
            if path
                .file_name()
                .map(|n| n.to_string_lossy().ends_with("-Statistics.db"))
                .unwrap_or(false)
            {
                return Some(path);
            }
            if depth > 0 && path.is_dir() {
                if let Some(p) = find_stats(&path, depth - 1) {
                    return Some(p);
                }
            }
        }
        None
    }
    let db = find_stats(dir, 8).expect("a *-Statistics.db under the flushed data dir");
    let bytes = std::fs::read(&db).expect("read Statistics.db");
    let (_, stats) = parse_statistics_with_fallback(&bytes, None).expect("decode Statistics.db");
    stats.timestamp_stats.min_timestamp
}

/// Total observation count across the `Statistics.db` tombstone-drop-time
/// histogram (Σ of every `(local_deletion_time, count)` bucket) — the exact
/// counter `StatisticsMetadata::update_local_deletion_time` increments once
/// per tombstone folded (issue #4246 roborev finding: NOT idempotent, so
/// folding the same tombstone twice inflates this sum).
fn statistics_tombstone_drop_count(dir: &Path) -> u64 {
    fn find_stats(dir: &Path, depth: usize) -> Option<PathBuf> {
        for entry in std::fs::read_dir(dir).ok()?.flatten() {
            let path = entry.path();
            if path
                .file_name()
                .map(|n| n.to_string_lossy().ends_with("-Statistics.db"))
                .unwrap_or(false)
            {
                return Some(path);
            }
            if depth > 0 && path.is_dir() {
                if let Some(p) = find_stats(&path, depth - 1) {
                    return Some(p);
                }
            }
        }
        None
    }
    let db = find_stats(dir, 8).expect("a *-Statistics.db under the flushed data dir");
    let bytes = std::fs::read(&db).expect("read Statistics.db");
    let (_, stats) = parse_statistics_with_fallback(&bytes, None).expect("decode Statistics.db");
    stats
        .tombstone_drop_times
        .iter()
        .map(|(_, count)| count)
        .sum()
}

fn discover_inputs(dir: &Path) -> Vec<PathBuf> {
    fn collect(dir: &Path, out: &mut Vec<PathBuf>, depth: usize) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let name = path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string();
            if name.starts_with("nb-") && name.ends_with("-big-Data.db") {
                out.push(path);
            } else if depth > 0 && path.is_dir() {
                collect(&path, out, depth - 1);
            }
        }
    }
    let mut found = Vec::new();
    collect(dir, &mut found, 8);
    found
}

/// Live clustering-key values read back through the real compaction read
/// path (`KWayMerger`) — the "serialization header remains correct" half of
/// the AC: proves Data.db actually decodes correctly post-fix, not merely
/// that Statistics.db reports a different number.
fn live_cks(inputs: Vec<PathBuf>, schema: &TableSchema) -> Vec<i32> {
    let mut merger = KWayMerger::new(inputs, schema).expect("KWayMerger::new");
    let mut live = Vec::new();
    loop {
        match merger.step().expect("merger step") {
            MergeStep::Complete => break,
            MergeStep::Partition { rows, .. } => {
                for entry in rows {
                    if entry.range_deletion.is_some() {
                        continue;
                    }
                    if let RowData::Live { cells } = &entry.row_data {
                        let has_data = cells.iter().any(|c| c.column != "ck" && c.column != "id");
                        if has_data {
                            if let Some(Value::Integer(ck)) = entry
                                .clustering_key
                                .as_ref()
                                .and_then(|k| k.columns.first().map(|(_, v)| v.clone()))
                            {
                                live.push(ck);
                            }
                        }
                    }
                }
            }
        }
    }
    live.sort_unstable();
    live
}

/// Property 1: a row fully shadow-dropped by a covering range tombstone must
/// NOT lower the persisted `Statistics.db` `min_timestamp` — the exact #4246
/// defect (mirrors the #4243 oracle's `ck1@5` shadowed by `[Bottom,5)@10`).
#[test]
fn shadowed_old_row_does_not_lower_persisted_min_timestamp() {
    let schema = schema();
    let temp = TempDir::new().unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let data_dir = temp.path().join("data");
    let mut engine = WriteEngine::new(WriteEngineConfig::new(
        data_dir.clone(),
        temp.path().join("wal"),
        schema.clone(),
    ))
    .unwrap();

    // One flush batch (exercises the buffered write_partition path AND the
    // two-pass baseline pre-scan #4246 fixes): ck=1@5 is shadow-dropped by
    // the range tombstone [Bottom, 5)@10; ck=6@25 survives untouched.
    flush_batch(
        &mut engine,
        &rt,
        vec![
            range_delete(ClusteringBound::Bottom, excl(5), 10),
            write_row(1, "shadowed", 5),
            write_row(6, "survivor", 25),
        ],
    );
    rt.block_on(engine.close()).unwrap();

    assert_eq!(
        statistics_min_timestamp(&data_dir),
        10,
        "the shadow-dropped ck=1@5 row must not lower persisted min_timestamp \
         below the covering tombstone's own deletion time (10)"
    );

    // Data.db itself must be correct, not just the reported statistic: ck=1
    // is genuinely absent (shadowed) and ck=6 genuinely present.
    let live = live_cks(discover_inputs(&data_dir), &schema);
    assert_eq!(
        live,
        vec![6],
        "ck=1 must be physically absent from Data.db (shadowed) and ck=6 present"
    );
}

/// Property 2 — the LIVE-OLDER-ROW CONTROL: a genuinely live row with an old
/// timestamp, never covered by any tombstone, MUST still lower persisted
/// `min_timestamp`. Proves the #4246 fix gates on the actual shadow
/// decision (`DataWriter::merge_row_group`), not on "is this timestamp old"
/// — a heuristic that would incorrectly exclude this row too.
#[test]
fn live_older_row_retains_persisted_min_timestamp() {
    let schema = schema();
    let temp = TempDir::new().unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let data_dir = temp.path().join("data");
    let mut engine = WriteEngine::new(WriteEngineConfig::new(
        data_dir.clone(),
        temp.path().join("wal"),
        schema.clone(),
    ))
    .unwrap();

    // ck=2@3 is old but LIVE (no tombstone anywhere in this partition); ck=6@25
    // is newer. No shadowing occurs at all.
    flush_batch(
        &mut engine,
        &rt,
        vec![write_row(2, "old-but-live", 3), write_row(6, "newer", 25)],
    );
    rt.block_on(engine.close()).unwrap();

    assert_eq!(
        statistics_min_timestamp(&data_dir),
        3,
        "a genuinely live old row must still set persisted min_timestamp — \
         the fix must not exclude it just for being old"
    );

    let live = live_cks(discover_inputs(&data_dir), &schema);
    assert_eq!(live, vec![2, 6], "both rows are live and must both survive");
}

/// Property 3 (combined, selectivity): one partition carrying BOTH a
/// shadow-dropped OLDER row (ck=1@5, shadowed) and a genuinely live OLDER
/// row (ck=4@8, not covered) alongside the covering tombstone (@10) and a
/// live survivor (ck=6@25). Persisted `min_timestamp` must reflect the live
/// older row (8), never the even-lower shadowed one (5) — proving the gate
/// is selective per row-group, not an all-or-nothing partition-level switch.
#[test]
fn shadow_gate_is_selective_per_row_not_partition_wide() {
    let schema = schema();
    let temp = TempDir::new().unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let data_dir = temp.path().join("data");
    let mut engine = WriteEngine::new(WriteEngineConfig::new(
        data_dir.clone(),
        temp.path().join("wal"),
        schema.clone(),
    ))
    .unwrap();

    // Range tombstone covers only ck < 3 (Bottom, Exclusive(3)) @ ts=10.
    // ck=1@5 falls inside it (shadowed); ck=4@8 is OUTSIDE it (chosen
    // instead of ck=2, which [Bottom, 3) would ALSO cover).
    flush_batch(
        &mut engine,
        &rt,
        vec![
            range_delete(ClusteringBound::Bottom, excl(3), 10),
            write_row(1, "shadowed", 5),
            write_row(4, "live-older", 8),
            write_row(6, "survivor", 25),
        ],
    );
    rt.block_on(engine.close()).unwrap();

    assert_eq!(
        statistics_min_timestamp(&data_dir),
        8,
        "min_timestamp must reflect the live older row (8), excluding the \
         shadowed row's lower timestamp (5) but NOT the tombstone's own \
         deletion time cap (10) either — the live row is the true minimum"
    );

    let live = live_cks(discover_inputs(&data_dir), &schema);
    assert_eq!(
        live,
        vec![4, 6],
        "ck=1 shadowed (absent), ck=4 and ck=6 live (present)"
    );
}

/// Property 4 (roborev finding, DOCUMENTED RESIDUAL — not a regression this
/// fix introduces, and deliberately NOT fixed here): a row deletion shadowing
/// an OLDER mutation for the SAME clustering key WITHIN ONE FLUSH BATCH — an
/// `INSERT` then a `DELETE` in a single flush, no range/partition tombstone
/// involved at all.
///
/// This LOOKS like the same defect class as the range-tombstone case above,
/// but it is architecturally different and this fix does not close it:
/// `compute_mutations_baseline_stats` computes the ENCODING baseline
/// (`pre_seed_encoding_baselines`'s input), which Cassandra's own
/// `EncodingStats` accumulates from EVERY memtable update APPLIED
/// (`SkipListMemtable.put`'s `statsCollector.update(update.stats())`,
/// cassandra-5.0.8) — UNCONDITIONALLY, never re-derived from the
/// post-reconciliation row the way the PERSISTED STATS component
/// (`MetadataCollector`) is. Verified directly against `issue_717_row_
/// tombstone_columns_subset.rs::row_tombstone_emits_columns_subset`, a
/// Cassandra-rejection-motivated byte-level test: excluding the shadowed
/// INSERT from the encoding baseline broke it (the emitted `mfda_delta`
/// must be relative to the INSERT's timestamp, not the DELETE's). Because
/// `pre_seed_encoding_baselines` seeds `self.stats` — the same struct that
/// becomes the PERSISTED Statistics.db STATS component — from THIS
/// function's return value directly, the persisted minimum inherits the
/// same (lower, INSERT-inclusive) value for this specific same-batch,
/// same-key shape. A cross-generation shadow (a range/partition tombstone,
/// or a shadowing DELETE in a LATER, separate flush/compaction — the shapes
/// the other tests in this file and the #4243 oracle cover) is NOT affected:
/// there, the shadowed clustering key's own GROUP returns `None` and is
/// correctly excluded.
#[test]
fn insert_then_delete_same_batch_baseline_residual_is_documented() {
    let schema = schema();
    let temp = TempDir::new().unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let data_dir = temp.path().join("data");
    let mut engine = WriteEngine::new(WriteEngineConfig::new(
        data_dir.clone(),
        temp.path().join("wal"),
        schema.clone(),
    ))
    .unwrap();

    // Both mutations target ck=1 in ONE flush batch: an INSERT at ts=5,
    // then a row DELETE at ts=10. No range/partition tombstone at all.
    let insert = write_row(1, "will-be-deleted", 5);
    let mut delete = Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("id", Value::Integer(1)),
        Some(ClusteringKey::single("ck", Value::Integer(1))),
        vec![CellOperation::DeleteRow],
        10,
        None,
    );
    delete.local_deletion_time = Some(2_000_000_000);
    flush_batch(&mut engine, &rt, vec![insert, delete]);
    rt.block_on(engine.close()).unwrap();

    assert_eq!(
        statistics_min_timestamp(&data_dir),
        5,
        "DOCUMENTED RESIDUAL: the encoding baseline (and, downstream, the \
         persisted min_timestamp) for a same-batch INSERT-then-DELETE still \
         includes the shadowed INSERT's timestamp, matching Cassandra's own \
         EncodingStats accumulation — this is intentional, not a bug this \
         assertion should ever need to change to fix. If this starts \
         failing, either a future fix has closed the residual (update this \
         test to match, and remove the doc comment above) or a regression \
         reintroduced the #717 columns-subset byte defect (do NOT change \
         this expected value \
         without also re-running \
         issue_717_row_tombstone_columns_subset::row_tombstone_emits_columns_subset)."
    );

    let live = live_cks(discover_inputs(&data_dir), &schema);
    assert!(
        live.is_empty(),
        "the row is deleted, not merely shadowed by a marker — no live row \
         must remain: {live:?}"
    );
}

/// Property 5 (roborev finding): a mutation carrying BOTH row content and a
/// partition tombstone in the SAME `Mutation` object must fold that
/// tombstone's local-deletion-time into the persisted tombstone-drop-time
/// histogram EXACTLY ONCE — `StatisticsMetadata::update_local_deletion_time`
/// is not idempotent (it increments a histogram bucket), so a caller that
/// folds row content and markers through two separate, unguarded paths for
/// the same mutation would double-count it, inflating the
/// `estimatedTombstoneDropTime` distribution Cassandra derives compaction
/// scheduling from.
#[test]
fn mixed_row_and_partition_tombstone_mutation_folds_tombstone_once() {
    let schema = schema();
    let temp = TempDir::new().unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let data_dir = temp.path().join("data");
    let mut engine = WriteEngine::new(WriteEngineConfig::new(
        data_dir.clone(),
        temp.path().join("wal"),
        schema.clone(),
    ))
    .unwrap();

    // A single Mutation carrying BOTH a surviving cell write (ts=25, well
    // above the tombstone) AND a partition_tombstone field directly.
    let mut mutation = write_row(1, "survivor", 25);
    mutation.partition_tombstone = Some(PartitionTombstone {
        deletion_time: 10,
        local_deletion_time: 2_000_000_000,
    });
    flush_batch(&mut engine, &rt, vec![mutation]);
    rt.block_on(engine.close()).unwrap();

    assert_eq!(
        statistics_tombstone_drop_count(&data_dir),
        1,
        "a mutation carrying both row content and a partition tombstone must \
         fold that tombstone's LDT into the drop-time histogram exactly \
         once, not twice"
    );
}
