//! Issue #2765 — emission-level wiring evidence for the
//! `cqlite.merge.active_merges` gauge.
//!
//! Sibling of `issue_2419_egress_depth_gauge.rs` (same OTel capture infra). The
//! `egress_budget` unit tests drive the increment/guard-drop pairing against a
//! PRIVATE atomic (`begin_on_for_test`, deliberately #2451-isolated) and the
//! `KWayMerger`-level wiring tests assert on `active_merge_count()`, so NOTHING
//! public-surface would fail if the PRODUCTION `record_active()` calls (in
//! `egress_budget::begin_merge` and `ActiveMergeGuard::drop`) were unwired — the
//! metric would simply stop emitting. This test closes that gap: it holds `N`
//! REAL `KWayMerger`s and asserts the captured `cqlite.merge.active_merges`
//! series RISES to ≥ `N` (proving the begin-side `record_active` fires) and
//! RETURNS to 0 once they drop (proving the drop-side one fires) — all through
//! the actual capture surface, never a private/injected atomic.
//!
//! Removing EITHER `record_active()` call makes this test fail: without the
//! begin-side one nothing rises to `N`; without the drop-side one the series
//! never returns to 0.
//!
//! This test binary runs ONLY this test, so the process-global active-merge
//! count starts at 0 and is driven solely here — deterministic, no ambient.
//!
//! Runs only under `observability-testing`:
//!   cargo test -p cqlite-core --features observability-testing \
//!     --test issue_2765_active_merges_gauge

#![cfg(all(feature = "write-support", feature = "observability-testing"))]

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

/// Concurrent merges to hold. Small (each spawns a producer thread over a tiny
/// SSTable); enough to prove a rise clearly above the 0 baseline.
const N: usize = 4;

fn make_schema() -> TableSchema {
    TableSchema {
        keyspace: "am_ks".to_string(),
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
        comments: std::collections::HashMap::new(),
        dropped_columns: std::collections::HashMap::new(),
    }
}

fn write_row(id: i32) -> Mutation {
    Mutation::new(
        TableId::new("am_ks", "items"),
        PartitionKey::single("id", Value::Integer(id)),
        None,
        vec![CellOperation::Write {
            column: "val".to_string(),
            value: Value::text(format!("v-{id}")),
        }],
        100,
        None,
    )
}

/// Build ONE small real nb SSTable (a handful of live rows) and return its
/// `Data.db` path — enough for a `KWayMerger` to open and hold an egress slot.
fn build_one_input() -> (TempDir, PathBuf, TableSchema) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime");
    let temp = TempDir::new().expect("tempdir");
    let schema = make_schema();
    let config = WriteEngineConfig::new(
        temp.path().join("data"),
        temp.path().join("wal"),
        schema.clone(),
    );
    let mut engine = WriteEngine::new(config).expect("engine");
    for id in 0..8 {
        engine.write(write_row(id)).expect("write row");
    }
    let info = rt
        .block_on(engine.flush())
        .expect("flush")
        .expect("flush info");
    let path = info.data_path;
    rt.block_on(engine.close()).expect("close engine");
    drop(rt);
    (temp, path, schema)
}

/// Every accumulated `cqlite.merge.active_merges` data point in a snapshot.
fn active_points(snap: &testing::CapturedMetrics) -> Vec<f64> {
    snap.entries()
        .iter()
        .filter(|e| e.name == catalog::MERGE_ACTIVE_MERGES)
        .flat_map(|e| e.points.iter().map(|p| p.value))
        .collect()
}

/// The gauge RISES to ≥ N as N real mergers are held, and RETURNS to 0 once they
/// drop — the begin-side and drop-side `record_active()` calls, respectively.
#[test]
fn active_merges_gauge_rises_and_returns_to_baseline() {
    let capture = testing::metrics_capture();
    capture.reset();

    let (_temp, path, schema) = build_one_input();

    // Hold N real mergers; each `begin_merge` records the rising level.
    let mut held: Vec<KWayMerger> = Vec::with_capacity(N);
    for _ in 0..N {
        held.push(
            KWayMerger::new_cancellable(vec![path.clone()], &schema, ScanCancel::default())
                .expect("merger builds"),
        );
    }

    // RISE: poll (bounded, fail-loud) for a POSITIVE reading ≥ N. If the
    // begin-side `record_active` were unwired, no point would reach N and this
    // loop would exhaust its deadline.
    let rise_unit = {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut reached = false;
        let mut unit: Option<String> = None;
        let mut last: Option<f64> = None;
        while Instant::now() < deadline {
            let snap = capture.flush_and_collect();
            for e in snap
                .entries()
                .iter()
                .filter(|e| e.name == catalog::MERGE_ACTIVE_MERGES)
            {
                unit = Some(e.unit.clone());
            }
            for v in active_points(&snap) {
                last = Some(v);
                if v >= N as f64 {
                    reached = true;
                }
            }
            if reached {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        assert!(
            reached,
            "cqlite.merge.active_merges must rise to >= {N} while {N} merges are \
             held (last observed {last:?}) — the begin-side record_active is unwired?"
        );
        unit
    };
    assert_eq!(
        rise_unit.as_deref(),
        Some(catalog::unit::MERGES),
        "gauge must carry the {{merge}} unit"
    );

    // Reset so the return-to-baseline window opens strictly before any drop.
    capture.reset();
    drop(held);

    // RETURN: poll (bounded, fail-loud) for a POSITIVE 0.0 record — the drop-side
    // `record_active`. If it were unwired the gauge would stay pinned at N and
    // this loop would exhaust its deadline (never inferred from an absent window).
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut observed_zero = false;
    let mut last: Option<f64> = None;
    while Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
        let snap = capture.flush_and_collect();
        let pts = active_points(&snap);
        if let Some(&v) = pts.last() {
            last = Some(v);
        }
        if pts.contains(&0.0) {
            observed_zero = true;
            break;
        }
    }
    assert!(
        observed_zero,
        "cqlite.merge.active_merges must return to 0 after all mergers drop \
         (last observed {last:?}) — the drop-side record_active is unwired?"
    );
}
