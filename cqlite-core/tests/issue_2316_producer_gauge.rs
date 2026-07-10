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
        let name = path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
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
                .write(write_row(
                    base + r,
                    &format!("v-{input}-{r}"),
                    100 + input as i64,
                ))
                .expect("write row");
        }
        rt.block_on(engine.flush())
            .expect("flush")
            .expect("flush info");
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

    // MID-MERGE: poll until the gauge POSITIVELY records M — synchronizing on the
    // producer LIFECYCLE rather than assuming a single immediate snapshot right
    // after `KWayMerger::new` already reflects every increment (issue #2316,
    // roborev job 1604 finding 2). Bounded + fail-loud: a broken or delayed
    // increment path exhausts the deadline and FAILS explicitly rather than
    // silently checking a possibly-incomplete snapshot. Scans ALL accumulated
    // matching points (not `find`'s first-match) for the same reason as the
    // teardown poll below: repeated `flush_and_collect` calls without an
    // intervening `reset` accumulate one batch per call, and `find` would get
    // stuck on the EARLIEST batch (e.g. "2" while producers 3 and 4 are still
    // incrementing) rather than ever observing the LATEST value.
    let mid_deadline = Instant::now() + Duration::from_secs(10);
    let mut mid_reached = false;
    let mut mid_last_seen: Option<f64> = None;
    let mut mid_unit: Option<String> = None;
    while Instant::now() < mid_deadline {
        let snap = capture.flush_and_collect();
        for entry in snap
            .entries()
            .iter()
            .filter(|e| e.name == catalog::MERGE_PRODUCER_THREADS)
        {
            mid_unit = Some(entry.unit.clone());
            for point in &entry.points {
                mid_last_seen = Some(point.value);
                if point.value == m as f64 {
                    mid_reached = true;
                }
            }
        }
        if mid_reached {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        mid_reached,
        "gauge should POSITIVELY record M={m} live producer threads mid-merge \
         within the bound (never inferred from an absent/stale window); last \
         observed value: {:?}",
        mid_last_seen
    );
    assert_eq!(
        mid_unit.as_deref(),
        Some(catalog::unit::THREADS),
        "gauge must carry the {{thread}} unit"
    );

    // Reset HERE — BEFORE the drain starts — so the fresh delta window opens
    // strictly before any producer thread can exit and decrement. The producer
    // threads that back the M live count are DETACHED (a `JoinHandle` drop does
    // not join), so some or all of the M decrements below can complete on their
    // own OS threads before this test thread even reaches the polling loop;
    // resetting AFTER the drain (as an earlier version of this test did) risks
    // clearing that evidence away before it is ever observed. Establishing the
    // window here, then NEVER resetting again before inspecting the accumulated
    // evidence below, is what makes the transition-to-0 observable at all.
    capture.reset();

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

    // AFTER: driven from an explicit completion event — the merge has already
    // been drained to completion (`merger.merge` + `writer.finish` above both
    // returned) — NOT a fixed sleep (issue #2316, roborev job 1604 finding 2).
    // Draining to completion guarantees every producer's channel closed (so its
    // scan loop has finished), but the producer THREAD itself may still be
    // mid-unwind: the channel closes when its `sync_channel` sender is dropped
    // (inside the async scan closure), while the gauge-decrementing
    // `ProducerThreadGuard` — declared FIRST in `producer_thread`, so it drops
    // LAST per Rust's reverse-declaration-order local drop — only fires once
    // that whole function body finishes unwinding and the OS thread actually
    // exits. That gap is normally microseconds but has no hard bound under
    // extreme scheduler contention, and the `JoinHandle` is never joined (by
    // design — see `SSTableRowIteratorAdapter`'s doc), so there is no stronger
    // "the thread is definitely gone" signal available from this test than
    // polling the gauge itself. So: drain first (already done), THEN poll for
    // the zero record — a generous, bounded, fail-loud timeout, never a fixed
    // sleep standing in for synchronization.
    //
    // The gauge should have recorded a genuine transition down to 0 WITHIN the
    // window opened by the `reset()` above (before the drain). Under DELTA
    // temporality a window with NO new `record_gauge` call reports the metric as
    // ABSENT — `CapturedMetrics::counter_sum` then defaults an absent metric to
    // `0.0`. Treating THAT default as "returned to baseline" is VACUOUS: it
    // would pass identically whether the decrement path fired or is entirely
    // broken. So this loop requires POSITIVE evidence: an actual collected data
    // point equal to `0.0`, never inferred from absence.
    //
    // Deliberately NO `reset()` inside this loop: the producer threads are
    // DETACHED (`JoinHandle` drop does not join), so the decrement(s) may have
    // already completed before this loop's first iteration — resetting again
    // here would risk clearing that very evidence before it is ever read.
    // Instead each poll calls `flush_and_collect` (which force-flushes any
    // pending record into the exporter's buffer WITHOUT clearing it) and scans
    // ALL accumulated matching entries/points for the given metric — not just
    // `find`'s first match, which would return only the earliest post-reset
    // batch and could get stuck on a stale intermediate (non-zero) reading while
    // later batches already show the settled 0. Bounded → deterministic, never a
    // wall-clock race; a broken decrement path (or a leaked increment) exhausts
    // the deadline and FAILS explicitly rather than passing vacuously.
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut observed_zero_record = false;
    let mut last_seen: Option<f64> = None;
    while Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
        let snap = capture.flush_and_collect();
        let matches: Vec<f64> = snap
            .entries()
            .iter()
            .filter(|e| e.name == catalog::MERGE_PRODUCER_THREADS)
            .flat_map(|e| e.points.iter().map(|p| p.value))
            .collect();
        if let Some(&v) = matches.last() {
            last_seen = Some(v);
        }
        if matches.iter().any(|&v| v == 0.0) {
            observed_zero_record = true;
            break;
        }
    }
    assert!(
        observed_zero_record,
        "gauge must POSITIVELY record a value of 0 after the merge's producers exit \
         (never inferred from an absent/un-updated window); last observed value: {:?}",
        last_seen
    );
}
