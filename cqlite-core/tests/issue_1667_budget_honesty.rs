//! Issue #1667 — Q4 budget honesty: dropped-column pre-pass accounting.
//!
//! These are the TDD tests for #1667. They live OUT of
//! `write_engine/maintenance.rs` (already over the campsite file-size
//! threshold, epic #1116) and exercise only the PUBLIC surface
//! (`WriteEngine::maintenance_step` + `MaintenanceReport`), so they double as
//! wiring evidence that the honest accounting reaches the public API.
//!
//! Red-on-main: both tests reference `MaintenanceReport::pre_pass_time` /
//! `pre_pass_short_circuited`, which do not exist before #1667 (won't compile
//! against main). Test B's `pre_pass_short_circuited` assertion is additionally
//! red behaviorally — verified by disabling only the short-circuit branch, the
//! first over-budget-pre-pass step let the pre-pass precede partition-loop work
//! (`pre_pass_short_circuited == false`). The main-today observed numbers are
//! recorded inline below.
#![cfg(feature = "write-support")]

use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, STCSPolicy, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use std::collections::HashMap;
use std::time::Duration;
use tempfile::TempDir;

/// An unclustered schema (id -> name) optionally declaring a dropped column
/// `old` (int). The dropped column STAYS in `columns` (the decode contract,
/// mirroring the merge-crate survivor-pre-pass tests) but is listed in
/// `dropped_columns`, so `start_merge`'s
/// `effective_schema.dropped_columns.is_empty()` gate is FALSE and the
/// (unbudgeted, one-shot) survivor pre-pass runs. No row ever writes `old`, so
/// `for_compaction_output` strips it and the compaction output is
/// byte-identical to the no-dropped-column case (#921 preserved).
fn budget_honesty_schema(with_dropped: bool) -> TableSchema {
    let mut columns = vec![
        Column {
            name: "id".to_string(),
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
    ];
    let mut dropped_columns = HashMap::new();
    if with_dropped {
        columns.push(Column {
            name: "old".to_string(),
            data_type: "int".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        });
        // Drop time = 1s past the epoch, far below any row timestamp.
        dropped_columns.insert("old".to_string(), 1_i64);
    }
    TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns,
        comments: HashMap::new(),
        dropped_columns,
    }
}

/// Flush `ids` (one partition per id) into ONE SSTable, at `ts` micros.
fn flush_ids(engine: &mut WriteEngine, ids: &[i32], ts: i64) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let table_id = TableId::new("test_ks", "test_table");
    for &id in ids {
        let m = Mutation::new(
            table_id.clone(),
            PartitionKey::single("id", Value::Integer(id)),
            None,
            vec![CellOperation::Write {
                column: "name".to_string(),
                value: Value::Text(format!("row-{id}")),
            }],
            ts,
            None,
        );
        engine.write(m).unwrap();
    }
    rt.block_on(engine.flush()).unwrap().unwrap();
}

fn stcs_policy() -> STCSPolicy {
    // min_threshold=2 so two flushed SSTables bucket + compact together.
    STCSPolicy::new(2, 32, 0.5, 1.5, 1024 * 1024).unwrap()
}

/// Test A — wide/partition overshoot is REPORTED HONESTLY (issue #1667).
///
/// A budget is a target checked at boundaries, not a hard cap. With a
/// deterministically-tiny budget (`ZERO`), every step that does real work must
/// report the REAL elapsed time in `time_spent` (never a value silently clamped
/// to the budget), and a NO-dropped-column table must report
/// `pre_pass_time == ZERO` on every step (the survivor pre-pass did not run).
/// This exercises the PARTITION LOOP overshoot specifically: a resume step
/// (which does not short-circuit) drains one cluster group and honestly reports
/// it overshot the zero budget.
///
/// Main-today (pre-#1667) observed behavior: `MaintenanceReport` has NO
/// `pre_pass_time` field at all (so this test cannot even compile against main —
/// the honest field is exactly what #1667 adds). `time_spent` was already
/// computed as `start.elapsed()` on main, so overshoot was already reported
/// honestly there; #1667 additionally makes the pre-pass cost a first-class,
/// separately-visible figure and adds the budget short-circuit.
#[test]
fn budget_honesty_reports_overshoot_honestly_test_a() {
    let temp_dir = TempDir::new().unwrap();
    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        budget_honesty_schema(false),
    );
    let mut engine = WriteEngine::new(config).unwrap();

    // Two SSTables, 4 distinct partitions total -> STCS merges them.
    flush_ids(&mut engine, &[0, 1], 5_000);
    flush_ids(&mut engine, &[2, 3], 6_000);
    engine.set_merge_policy(Box::new(stcs_policy())).unwrap();

    // ZERO budget: any real work deterministically overshoots the boundary.
    let budget = Duration::ZERO;
    let mut total_rows = 0u64;
    let mut saw_working_step_overshoot = false;
    let mut calls = 0u32;
    loop {
        let report = engine.maintenance_step(budget).unwrap();
        // A no-dropped-column table NEVER runs the survivor pre-pass.
        assert_eq!(
            report.pre_pass_time,
            Duration::ZERO,
            "no dropped columns => pre_pass_time must be zero"
        );
        total_rows += report.rows_merged;
        // A step that merged rows ran the partition loop; its time_spent must
        // honestly reflect the (over-budget) real work, never be clamped to 0.
        if report.rows_merged > 0 {
            assert!(
                report.time_spent > budget,
                "partition-loop step overshot the zero budget but time_spent \
                 ({:?}) was not reported honestly (<= budget {:?})",
                report.time_spent,
                budget
            );
            saw_working_step_overshoot = true;
        }
        calls += 1;
        assert!(calls < 100_000, "compaction never completed");
        if !report.pending_compaction {
            break;
        }
    }

    assert_eq!(total_rows, 4, "all 4 partitions must be merged");
    assert!(
        saw_working_step_overshoot,
        "at least one partition-loop step must have honestly reported \
         overshooting the zero budget"
    );
}

/// Test B — dropped-column survivor pre-pass is ACCOUNTED against the budget
/// clock and cannot silently precede an unbudgeted partition loop (#1667).
///
/// On a dropped-column table the very first step runs the FULL, one-shot
/// survivor pre-pass before any partition is emitted. With a tiny budget
/// (`from_nanos(1)`) the pre-pass alone exhausts it, so the fixed code:
///   (1) surfaces the pre-pass cost distinctly (`pre_pass_time > ZERO`), and
///   (2) SHORT-CIRCUITS the partition loop for that step
///       (`pre_pass_short_circuited == true`, `rows_merged == 0`,
///       `direct_stream_partitions == 0`, merge still pending) — the pre-pass
///       never silently precedes a partition pass in the same step.
///
/// Main-today (pre-#1667) observed behavior: `MaintenanceReport` has NEITHER
/// `pre_pass_time` NOR `pre_pass_short_circuited` (this test cannot compile
/// against main — those honest fields are exactly what #1667 adds). The
/// behavioral difference the short-circuit adds: WITHOUT it (empirically
/// verified by disabling only the short-circuit branch), the first step ran the
/// pre-pass fully and THEN entered the partition loop, buffering one cluster
/// group before #1668's per-cluster-group budget check paused it — so the
/// pre-pass DID silently precede partition-loop work (`pre_pass_short_circuited`
/// would be `false`). Note `rows_merged` is 0 in BOTH cases here (that first
/// buffered cluster group pauses before its `PartitionEnd`, where the count is
/// applied), so the short-circuit flag — not `rows_merged` — is the
/// load-bearing discriminator.
#[test]
fn budget_honesty_accounts_dropped_column_pre_pass_test_b() {
    let temp_dir = TempDir::new().unwrap();
    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        budget_honesty_schema(true),
    );
    let mut engine = WriteEngine::new(config).unwrap();

    // Two SSTables (enough to trigger STCS), 4 distinct partitions total.
    flush_ids(&mut engine, &[0, 1], 5_000);
    flush_ids(&mut engine, &[2, 3], 6_000);
    engine.set_merge_policy(Box::new(stcs_policy())).unwrap();

    // Tiny budget: the (unbudgeted) survivor pre-pass alone blows it.
    let first = engine.maintenance_step(Duration::from_nanos(1)).unwrap();

    assert!(
        first.pre_pass_time > Duration::ZERO,
        "the dropped-column survivor pre-pass cost must be accounted in the \
         report (pre_pass_time), got {:?}",
        first.pre_pass_time
    );
    assert!(
        first.time_spent >= first.pre_pass_time,
        "time_spent ({:?}) must include the pre-pass ({:?})",
        first.time_spent,
        first.pre_pass_time
    );
    assert!(
        first.pre_pass_short_circuited,
        "an over-budget pre-pass must short-circuit the partition loop for this \
         step (the load-bearing discriminator: false on main-equivalent code \
         that lets the pre-pass precede partition-loop work)"
    );
    assert_eq!(
        first.rows_merged, 0,
        "a short-circuited step merges no rows in the same step as the pre-pass"
    );
    assert_eq!(
        first.direct_stream_partitions, 0,
        "no partition work may run in the same step as an over-budget pre-pass"
    );
    assert!(
        first.pending_compaction,
        "the merge was started (pre-pass ran) so it must remain pending"
    );

    // The merge resumes and completes on subsequent (generously budgeted)
    // steps, and every row is still merged (accounting only, no behavior
    // change): the short-circuit changed WHEN work happens, not WHAT.
    let mut total_rows = first.rows_merged;
    let mut calls = 1u32;
    let mut report = first;
    while report.pending_compaction {
        report = engine.maintenance_step(Duration::from_secs(60)).unwrap();
        total_rows += report.rows_merged;
        // The pre-pass ran only on the FIRST step; later steps must not
        // re-account it.
        assert_eq!(
            report.pre_pass_time,
            Duration::ZERO,
            "pre_pass_time must be zero on steps that resume an existing merge"
        );
        calls += 1;
        assert!(calls < 100_000, "compaction never completed");
    }
    assert_eq!(total_rows, 4, "all 4 partitions must eventually be merged");
}
