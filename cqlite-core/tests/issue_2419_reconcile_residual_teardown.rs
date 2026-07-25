//! Issue #2419 (WS2), C-audit Req 3 — the PRODUCTION cancel/teardown reconcile
//! path for `cqlite.merge.egress_channel_depth`.
//!
//! Companion to `issue_2419_egress_depth_gauge.rs`. That test drives a real
//! k-way merge to FULL COMPLETION, so every buffered entry is `received()` and
//! the post-join residual (`sent_count - received_count`) is ZERO — which means
//! `SSTableRowIteratorAdapter::drop`'s reconcile step
//! (`channel_depth::reconcile_residual`, `merge/mod.rs`) is a pure no-op there
//! (`residual <= 0`). Nothing public-surface would fail if that reconcile were
//! ever unwired, sign-flipped, read pre-join, or dropped: the drain test would
//! stay green (roborev job 1735, wiring-evidence Medium).
//!
//! This test closes that gap by exercising the OTHER branch — a genuine
//! `residual > 0`. It builds a REAL backed-up cancellable k-way merge (producers
//! fill their bounded channels past capacity so `sent()` fires repeatedly), then
//! CANCELS + DROPS the merger WITHOUT ever draining it (client-disconnect /
//! LIMIT-satisfied analogue, mirroring
//! `merge::teardown_tests::dropping_merger_after_partial_consumption_joins`).
//! With nothing received, the abandoned buffered entries ARE the residual, and
//! the adapter's post-join `reconcile_residual(sent - received)` must return the
//! shared `cqlite.merge.egress_channel_depth` gauge to baseline instead of
//! leaking upward. A regression in the post-join subtraction (wrong sign, a
//! pre-join read, a dropped residual, or an unwired call site) would leave the
//! gauge pinned at its backed-up level forever and this test's baseline poll
//! would exhaust its deadline and fail explicitly — never pass vacuously.
//!
//! All assertions are positive-observation / lower-bound style throughout (never
//! an exact-equality assertion against the process-wide gauge atomic), so this
//! stays immune to the #2451 flake class — identical rationale to
//! `issue_2419_egress_depth_gauge.rs`.
//!
//! Runs only under `observability-testing` (the in-memory metric capture fixture):
//!   cargo test -p cqlite-core --features observability-testing \
//!     --test issue_2419_reconcile_residual_teardown

#![cfg(all(feature = "write-support", feature = "observability-testing"))]

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use cqlite_core::observability::{catalog, testing};
use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::scan_cancel::ScanCancel;
use cqlite_core::storage::write_engine::{
    CellOperation, KWayMerger, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use tempfile::TempDir;

/// Rows per input SSTable. MUST exceed the merge's 256-entry channel capacity
/// (`STREAMING_CHANNEL_CAPACITY`, private to `merge/mod.rs`) so every producer
/// fills its own channel and blocks on `send`, staying backed up (none received)
/// until teardown — mirroring `issue_2419_egress_depth_gauge.rs`'s identical
/// rationale.
const ROWS_PER_INPUT: i32 = 400;
const NUM_INPUTS: usize = 4;

fn make_schema() -> TableSchema {
    TableSchema {
        keyspace: "residual_ks".to_string(),
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
        TableId::new("residual_ks", "items"),
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
/// captured snapshot.
fn depth_points(snap: &testing::CapturedMetrics) -> Vec<f64> {
    snap.entries()
        .iter()
        .filter(|e| e.name == catalog::MERGE_EGRESS_CHANNEL_DEPTH)
        .flat_map(|e| e.points.iter().map(|p| p.value))
        .collect()
}

/// A cancelled/dropped k-way merge whose per-input channels are still backed up
/// (entries `sent()` but never `received()`) must reconcile the shared
/// `cqlite.merge.egress_channel_depth` gauge back to baseline on teardown — via
/// the production `SSTableRowIteratorAdapter::drop` post-join
/// `reconcile_residual(sent_count - received_count)` path (issue #2419 C-audit
/// Req 3; roborev job 1735). This exercises the `residual > 0` branch that the
/// drain-to-completion sibling test (`issue_2419_egress_depth_gauge.rs`) leaves
/// as a no-op.
#[test]
fn cancelled_backed_up_merge_reconciles_egress_depth_to_baseline_on_drop() {
    let capture = testing::metrics_capture();
    capture.reset();

    let (_temp, inputs, schema) = build_inputs();

    // Construct a CANCELLABLE merger WITHOUT stepping: `KWayMerger::new` seeds no
    // heap ("populated on first step"), so every producer races ahead filling its
    // own 256-entry channel and blocks on `send` once full — none received yet.
    // Because we never step, `received_count` stays 0 and the buffered entries
    // ARE the residual the teardown must reconcile. `new_cancellable` (vs `new`)
    // drives the cancel-aware Drop path used by the Flight `do_get` merge.
    let cancel = ScanCancel::default();
    let merger = KWayMerger::new_cancellable(inputs, &schema, cancel.clone())
        .expect("KWayMerger::new_cancellable");

    // MID-MERGE: poll (bounded, fail-loud) until the gauge POSITIVELY records a
    // reading proving multiple channels are genuinely backed up concurrently —
    // never inferred from an absent/stale window. If `channel_depth::sent()` were
    // unwired, this loop would exhaust its deadline and fail explicitly. This
    // reading is the proof that a genuine `residual > 0` exists BEFORE teardown.
    let backpressure_threshold = (NUM_INPUTS as f64) * 150.0;
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
         (a genuine residual > 0) while {NUM_INPUTS} producers are blocked on a full \
         channel, before teardown (never inferred from an absent/stale window); \
         last observed value: {mid_last_seen:?}"
    );
    assert_eq!(
        mid_unit.as_deref(),
        Some(catalog::unit::ENTRIES),
        "gauge must carry the {{entry}} unit"
    );

    // Reset HERE — BEFORE the teardown — so the fresh delta window opens strictly
    // before any residual is reconciled (mirrors the sibling test's ordering).
    capture.reset();

    // Client-disconnect / LIMIT-satisfied analogue: trip the cooperative cancel,
    // then DROP the still-backed-up merger WITHOUT draining. Each input adapter's
    // `Drop` closes its channel, JOINs its producer, then reconciles the shared
    // gauge by its own `sent_count - received_count` residual (all positive here,
    // since nothing was received). The teardown must be bounded (a regressed,
    // deadlocking teardown would hang here, per issue #2361) AND must return the
    // gauge to baseline (a regressed reconcile would leave it pinned).
    cancel.cancel();
    drop(merger);

    // AFTER: poll (bounded, fail-loud) for a POSITIVE observation of 0.0 — never
    // inferred from an absent/un-updated window (an absent metric defaults to 0.0
    // in `counter_sum`, which would pass vacuously whether `reconcile_residual`
    // fired or is entirely broken). If the reconcile were unwired, sign-flipped,
    // read pre-join, or dropped, the gauge would stay pinned at its backed-up
    // level and this loop would exhaust its deadline and fail explicitly.
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
        "gauge must POSITIVELY record a value of 0 after the cancelled/backed-up \
         merge is torn down (the post-join residual reconcile returns it to \
         baseline; never inferred from an absent/un-updated window); \
         last observed value: {last_seen:?}"
    );
}
