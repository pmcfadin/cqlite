//! Issue #2316 — the `cqlite.merge.producer_threads` gauge.
//!
//! Corroborates the O(M) producer-thread bound with an always-on observability
//! signal (independent of the direct `/proc` thread-count observation in
//! `issue_2316_merge_thread_budget.rs`): the gauge RISES to reflect the live
//! producer threads while a real merge holds them, and RETURNS to its baseline
//! once the merge's producers are joined/dropped.
//!
//! Runs only under `observability-testing` (the in-memory metric capture fixture):
//!   cargo test -p cqlite-core --features observability-testing \
//!     --test issue_2316_producer_gauge

#![cfg(all(feature = "write-support", feature = "observability-testing"))]

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use cqlite_core::observability::{catalog, testing};
use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::writer::SSTableWriter;
use cqlite_core::storage::write_engine::merge::compute_baseline_min;
use cqlite_core::storage::write_engine::{
    CellOperation, KWayMerger, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use tempfile::TempDir;

/// Rows per input SSTable. MUST exceed the merge's 256-entry channel capacity so
/// every producer blocks on `send` and stays alive (uncollapsed live count) until
/// the merge drains it — giving a deterministic window where the gauge reads `M`.
const ROWS_PER_INPUT: i32 = 400;
const NUM_INPUTS: usize = 4;

fn make_schema() -> TableSchema {
    TableSchema {
        keyspace: "gauge_ks".to_string(),
        table: "items".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "id".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "val".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

fn write_row(id: i32, val: &str, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new("gauge_ks", "items"),
        PartitionKey::single("id", Value::Integer(id)),
        None,
        vec![CellOperation::Write {
            column: "val".to_string(),
            value: Value::Text(val.to_string()),
        }],
        ts,
        None,
    )
}

fn collect_inputs(dir: &std::path::Path, out: &mut Vec<(u64, PathBuf)>, depth: usize) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = path.file_name().unwrap_or_default().to_string_lossy().to_string();
        if name.starts_with("nb-") && name.ends_with("-big-Data.db") {
            let base = name.trim_end_matches("-Data.db");
            if !path.with_file_name(format!("{base}-TOC.txt")).exists() {
                continue;
            }
            let generation = name
                .strip_prefix("nb-")
                .and_then(|s| s.split("-big-").next())
                .and_then(|g| g.parse::<u64>().ok())
                .unwrap_or(0);
            out.push((generation, path));
        } else if depth > 0 && path.is_dir() {
            collect_inputs(&path, out, depth - 1);
        }
    }
}

/// Build `NUM_INPUTS` REAL nb SSTables (each `ROWS_PER_INPUT` live rows over a
/// disjoint partition range). Never empty.
fn build_inputs() -> (TempDir, Vec<PathBuf>, TableSchema) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime");
    let temp = TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("inputs");
    let wal_dir = temp.path().join("wal");
    let schema = make_schema();

    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir.clone(), schema.clone());
    let mut engine = WriteEngine::new(config).expect("engine");
    for input in 0..NUM_INPUTS {
        let base = input as i32 * ROWS_PER_INPUT;
        for r in 0..ROWS_PER_INPUT {
            engine
                .write(write_row(base + r, &format!("v-{input}-{r}"), 100 + input as i64))
                .expect("write row");
        }
        rt.block_on(engine.flush()).expect("flush").expect("flush info");
    }
    rt.block_on(engine.close()).expect("close engine");

    let mut found = Vec::new();
    collect_inputs(&data_dir, &mut found, 8);
    found.sort_by(|a, b| b.0.cmp(&a.0));
    let inputs: Vec<PathBuf> = found.into_iter().map(|(_, p)| p).collect();
    assert!(
        inputs.len() >= NUM_INPUTS,
        "expected >= {NUM_INPUTS} real inputs, got {}",
        inputs.len()
    );
    drop(rt);
    (temp, inputs, schema)
}

#[test]
fn producer_threads_gauge_rises_and_returns_to_baseline() {
    let capture = testing::metrics_capture();
    capture.reset();

    let (_temp, inputs, schema) = build_inputs();
    let m = inputs.len();
    let out = TempDir::new().expect("out tempdir");
    let (baseline_ts, baseline_ldt, baseline_ttl) = compute_baseline_min(&inputs);

    // Construct the merger: all M producers spawn and — with ROWS_PER_INPUT > the
    // 256-entry channel capacity — block on `send`, so none decrements before the
    // snapshot below. The gauge was incremented once per producer at spawn.
    let merger = KWayMerger::new(inputs, &schema).expect("KWayMerger::new");

    // MID-MERGE: the gauge must reflect the M live producer threads.
    let mid = capture.flush_and_collect();
    let mid_val = mid.counter_sum(catalog::MERGE_PRODUCER_THREADS);
    assert_eq!(
        mid_val, m as f64,
        "gauge should read M={m} live producer threads mid-merge, got {mid_val}"
    );
    assert_eq!(
        mid.unit(catalog::MERGE_PRODUCER_THREADS),
        Some(catalog::unit::THREADS),
        "gauge must carry the {{thread}} unit"
    );

    // Drain the merge; its producers exit and decrement the gauge on the way out.
    let mut writer =
        SSTableWriter::new(out.path().to_path_buf(), 1, &schema).expect("SSTableWriter::new");
    writer.pre_seed_encoding_baselines(baseline_ts, baseline_ldt, baseline_ttl);
    let stats = merger.merge(&mut writer).expect("merge into writer");
    let finish_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("finish runtime");
    finish_rt.block_on(writer.finish()).expect("writer finish");
    assert!(
        stats.output_rows >= (NUM_INPUTS as u64 * ROWS_PER_INPUT as u64),
        "merge should emit all input rows; got {}",
        stats.output_rows
    );

    // AFTER: the merge's producers have exited (each guard decremented the live
    // count on the way out). The gauge returns to its baseline (0). Poll with a
    // per-iteration `reset` so each reading is an ISOLATED delta window (a single
    // `flush_and_collect` would otherwise SUM the still-buffered mid-merge `M`
    // snapshot). Under DELTA temporality a steady gauge with no new record in the
    // window reports absent (== 0.0). This is NON-vacuous: the mid assertion above
    // already proved the same gauge recorded the live count `M` during the merge,
    // so a settled 0 here is a genuine return-to-baseline, not a never-recorded
    // metric. Bounded → deterministic, never a wall-clock race.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut final_val = f64::NAN;
    while Instant::now() < deadline {
        capture.reset();
        std::thread::sleep(Duration::from_millis(20));
        final_val = capture.flush_and_collect().counter_sum(catalog::MERGE_PRODUCER_THREADS);
        if final_val == 0.0 {
            break;
        }
    }
    assert_eq!(
        final_val, 0.0,
        "gauge must return to its baseline (0) after the merge's producers are joined/dropped"
    );
}
