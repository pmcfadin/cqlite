//! Issue #2419 (WS2), C-audit Req 3 — the `cqlite.merge.egress_channel_depth`
//! gauge.
//!
//! Sibling of `issue_2316_producer_gauge.rs` (same capture infra, same
//! backed-up-merge fixture shape). The unit test in
//! `storage::write_engine::merge::channel_depth` pins the depth arithmetic
//! against a PRIVATE atomic via `adjust()` (deliberately, to stay immune to the
//! #2451 flake class — see that module's tests) — but that means nothing
//! public-surface would fail if the PRODUCTION `channel_depth::sent()` /
//! `received()` call sites (`from_readers::forward_row`,
//! `SSTableRowIteratorAdapter::next`) were ever unwired, since the private-atomic
//! pin never touches them. This test closes that gap: it drives a REAL k-way
//! merge whose per-input channels back up past capacity (so `sent()` must fire
//! to observe a positive reading), then drains it to completion (so
//! `received()` must fire to observe the return to baseline) — all through the
//! actual OTel capture surface, never a private/injected atomic.
//!
//! Runs only under `observability-testing` (the in-memory metric capture fixture):
//!   cargo test -p cqlite-core --features observability-testing \
//!     --test issue_2419_egress_depth_gauge

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

/// Rows per input SSTable. MUST exceed the merge's 256-entry channel capacity
/// (`STREAMING_CHANNEL_CAPACITY`, private to `merge/mod.rs`) so every producer
/// fills its own channel and blocks on `send`, staying backed up (none received)
/// until the merge is stepped — mirroring `issue_2316_producer_gauge.rs`'s
/// identical rationale for its own gauge.
const ROWS_PER_INPUT: i32 = 400;
const NUM_INPUTS: usize = 4;

fn make_schema() -> TableSchema {
    TableSchema {
        keyspace: "egress_ks".to_string(),
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
        TableId::new("egress_ks", "items"),
        PartitionKey::single("id", Value::Integer(id)),
        None,
        vec![CellOperation::Write {
            column: "val".to_string(),
            value: Value::text(val.to_string()),
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
    found.sort_by_key(|b| std::cmp::Reverse(b.0));
    let inputs: Vec<PathBuf> = found.into_iter().map(|(_, p)| p).collect();
    assert!(
        inputs.len() >= NUM_INPUTS,
        "expected >= {NUM_INPUTS} real inputs, got {}",
        inputs.len()
    );
    drop(rt);
    (temp, inputs, schema)
}

/// Sum every accumulated `cqlite.merge.egress_channel_depth` data point in one
/// captured snapshot (there may be several from repeated `flush_and_collect`
/// calls without an intervening `reset` — see the polling loops below).
fn depth_points(snap: &testing::CapturedMetrics) -> Vec<f64> {
    snap.entries()
        .iter()
        .filter(|e| e.name == catalog::MERGE_EGRESS_CHANNEL_DEPTH)
        .flat_map(|e| e.points.iter().map(|p| p.value))
        .collect()
}

/// The egress-depth gauge RISES to reflect real bounded-channel backpressure
/// while a k-way merge holds M producers backed up past capacity, and RETURNS
/// to baseline once the merge is drained to completion — driven ENTIRELY
/// through the production `channel_depth::sent()` / `received()` call sites
/// (issue #2419 C-audit Req 3): a lower-bound/positive-observation style
/// throughout (never an exact-equality assertion against the shared global),
/// so this stays immune to the #2451 flake class even though the gauge is a
/// process-wide atomic.
#[test]
fn egress_depth_gauge_rises_and_returns_to_baseline() {
    let capture = testing::metrics_capture();
    capture.reset();

    let (_temp, inputs, schema) = build_inputs();
    let (baseline_ts, baseline_ldt, baseline_ttl) = compute_baseline_min(&inputs);
    let out = TempDir::new().expect("out tempdir");

    // Construct the merger WITHOUT stepping: `KWayMerger::new` does not seed its
    // heap ("populated on first step"), so every producer races ahead filling
    // its OWN egress channel and blocks on `send` once full — none received yet.
    // Issue #2765: that channel's capacity is now ADAPTIVE — up to 256, but
    // `clamp(EGRESS_ROW_BUDGET / active_merges, MIN_CAP, 256)` under concurrent
    // merges — so the theoretical ceiling is `NUM_INPUTS * per_channel_cap`, NOT
    // a hard-coded `NUM_INPUTS * 256`. The threshold below is derived from the
    // LIVE adaptive capacity (a conservative fraction of that ceiling, never an
    // exact target) so a future concurrent merge in this binary shrinks the cap
    // WITHOUT pushing the threshold out of reach (the pre-#2765 fixed-256
    // assumption would 10s-timeout in that case).
    let merger = KWayMerger::new(inputs, &schema).expect("KWayMerger::new");

    // Read the adaptive per-channel capacity this merger actually got. Reading
    // AFTER construction only ever LOWERS the estimate (a concurrent merge would
    // raise `active_merge_count`, shrinking `capacity_for`), so the derived
    // threshold stays reachable — never above the true ceiling.
    let per_channel_cap = cqlite_core::storage::write_engine::merge::egress_channel_capacity_for(
        cqlite_core::storage::write_engine::merge::active_merge_count(),
    );

    // MID-MERGE: poll (bounded, fail-loud) until the gauge POSITIVELY records a
    // reading proving multiple channels are genuinely backed up concurrently —
    // never inferred from an absent/stale window. If `channel_depth::sent()`
    // were removed from `forward_row`, this loop would exhaust its deadline and
    // fail explicitly. Half the adaptive ceiling (`NUM_INPUTS * cap`) requires
    // more than one full channel's worth (for NUM_INPUTS >= 2), so it still
    // proves CONCURRENT multi-channel backpressure, adaptively.
    let backpressure_threshold = (NUM_INPUTS as f64) * (per_channel_cap as f64) * 0.5;
    let mid_deadline = Instant::now() + Duration::from_secs(10);
    let mut mid_reached = false;
    let mut mid_last_seen: Option<f64> = None;
    let mut mid_unit: Option<String> = None;
    while Instant::now() < mid_deadline {
        let snap = capture.flush_and_collect();
        for entry in snap
            .entries()
            .iter()
            .filter(|e| e.name == catalog::MERGE_EGRESS_CHANNEL_DEPTH)
        {
            mid_unit = Some(entry.unit.clone());
        }
        for v in depth_points(&snap) {
            mid_last_seen = Some(v);
            if v >= backpressure_threshold {
                mid_reached = true;
            }
        }
        if mid_reached {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        mid_reached,
        "gauge should POSITIVELY record a backed-up reading >= {backpressure_threshold} \
         while {NUM_INPUTS} producers are blocked on a full channel (never inferred \
         from an absent/stale window); last observed value: {:?}",
        mid_last_seen
    );
    assert_eq!(
        mid_unit.as_deref(),
        Some(catalog::unit::ENTRIES),
        "gauge must carry the {{entry}} unit"
    );

    // Reset HERE — BEFORE the drain — so the fresh delta window opens strictly
    // before any entry is received (mirrors `issue_2316_producer_gauge.rs`'s
    // identical ordering rationale).
    capture.reset();

    // Drain the merge to completion: every producer's remaining entries are
    // received (channel_depth::received() fires per entry via
    // `SSTableRowIteratorAdapter::next`), so the gauge should settle back to 0
    // through completely ordinary consumption — no cancellation/reconcile
    // needed for this scenario.
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

    // AFTER: poll (bounded, fail-loud) for a POSITIVE observation of 0.0 — never
    // inferred from an absent/un-updated window (see `issue_2316_producer_gauge.rs`'s
    // identical DELTA-temporality caveat: an absent metric defaults to 0.0 in
    // `counter_sum`, which would pass vacuously whether `received()` fired or is
    // entirely broken). If `channel_depth::received()` were removed, the gauge
    // would stay pinned at its backed-up level forever and this loop would
    // exhaust its deadline and fail explicitly.
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut observed_zero_record = false;
    let mut last_seen: Option<f64> = None;
    while Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
        let snap = capture.flush_and_collect();
        let matches = depth_points(&snap);
        if let Some(&v) = matches.last() {
            last_seen = Some(v);
        }
        if matches.contains(&0.0) {
            observed_zero_record = true;
            break;
        }
    }
    assert!(
        observed_zero_record,
        "gauge must POSITIVELY record a value of 0 after the merge fully drains \
         (never inferred from an absent/un-updated window); last observed value: {:?}",
        last_seen
    );
}
