//! Issue #1591 (F1): the `SSTableManager` must NOT hold the `table_readers`
//! read guard across a scan's I/O.
//!
//! `tokio::sync::RwLock` is FIFO-fair, so a single queued writer (reader reload,
//! schema set, generation removal) parks every later-arriving reader behind the
//! longest in-flight read guard. If a scan holds the read guard across its whole
//! multi-reader I/O, one slow scan plus one admin write stalls every subsequent
//! point read — bimodal tail latency. The fix snapshots the `Vec<Arc<Reader>>`
//! and drops the guard before any reader I/O.
//!
//! This test pins the invariant deterministically (no wall-clock sleeps): it
//! pauses a scan at its per-reader I/O via a per-manager gate, then asserts a
//! writer can immediately acquire the write guard. On the pre-fix code the scan
//! still holds the read guard at that point, so the write guard is unavailable
//! (a queued writer would FIFO-park every subsequent point read); after the fix
//! the guard is already dropped, so the writer — and therefore any following
//! point read — never parks.

#![cfg(all(test, feature = "write-support", feature = "state_machine"))]

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use super::SSTableManager;
use crate::schema::{Column, KeyColumn, TableSchema};
use crate::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId as WriteTableId, WriteEngine, WriteEngineConfig,
};
use crate::types::{TableId, Value};
use crate::{Config, Platform};
use tempfile::TempDir;

fn users_schema() -> TableSchema {
    TableSchema {
        keyspace: "ks_lock".to_string(),
        table: "users".to_string(),
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
                name: "value".to_string(),
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

async fn flush_one_partition(data_dir: &Path, wal_dir: &Path, id: i32) {
    let config = WriteEngineConfig::new(
        data_dir.to_path_buf(),
        wal_dir.to_path_buf(),
        users_schema(),
    );
    let mut engine = WriteEngine::new(config).expect("write engine");
    let table_id = WriteTableId::new("ks_lock", "users");
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ops = vec![CellOperation::Write {
        column: "value".to_string(),
        value: Value::Text(format!("v{}", id)),
    }];
    engine
        .write_async(Mutation::new(
            table_id,
            pk,
            None,
            ops,
            1_000 + id as i64,
            None,
        ))
        .await
        .expect("write");
    engine.flush().await.expect("flush");
}

/// Gated-scan test (issue #1591): a scan paused mid-flight must not be holding
/// the `table_readers` read guard, so a writer (and any point read behind it)
/// never parks.
///
/// Deterministic: the scan signals arrival on `gate.reached` and blocks on
/// `gate.release`; the assertions use `try_write()` (non-blocking) — no sleeps.
///
/// RED before the fix: the scan holds the read guard across the loop, so
/// `try_write()` returns `Err`. GREEN after the fix: the guard was dropped at
/// the snapshot boundary, so `try_write()` returns `Ok`.
#[tokio::test]
async fn scan_does_not_hold_read_guard_across_io() {
    let tmp = TempDir::new().expect("tmp");
    let data_dir = tmp.path().join("data");
    let wal_dir = tmp.path().join("wal");
    flush_one_partition(&data_dir, &wal_dir, 1).await;

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    let manager = Arc::new(
        SSTableManager::new(&data_dir, &config, platform, None)
            .await
            .expect("manager"),
    );

    let schema = users_schema();
    let table_id = TableId::new("ks_lock.users");

    // Warm, un-gated scan: confirms exactly one generation is served and yields a
    // real key for the concurrent point read below.
    let pre = manager
        .scan(&table_id, None, None, None, Some(&schema))
        .await
        .expect("pre scan");
    assert_eq!(pre.len(), 1, "one partition flushed → one row");
    let probe_key = pre[0].0.clone();

    // Arm the gate, then launch a scan that will pause at its per-reader I/O.
    let gate = manager.arm_scan_gate();
    let scan_handle = {
        let manager = Arc::clone(&manager);
        let schema = schema.clone();
        let table_id = table_id.clone();
        tokio::spawn(async move {
            manager
                .scan(&table_id, None, None, None, Some(&schema))
                .await
        })
    };

    // Rendezvous: the scan has reached the per-reader I/O and is now blocked on
    // `gate.release`. Whether it is still holding the read guard is exactly the
    // behaviour under test.
    gate.reached.notified().await;

    // Core assertion (RED before, GREEN after): a writer acquires the map guard
    // immediately. If the scan still held the read guard this would fail, and a
    // real queued writer would FIFO-park every later point read behind the scan.
    {
        let w = manager.table_readers.try_write();
        assert!(
            w.is_ok(),
            "scan held the table_readers read guard across its I/O; a queued \
             writer would park every subsequent point read (issue #1591)"
        );
        // Drop the write guard immediately so the point read below can proceed.
    }

    // Liveness: a point read completes while the scan is STILL parked at the
    // gate — it is not blocked behind the (now non-parking) writer.
    let _ = manager
        .get(&table_id, &probe_key)
        .await
        .expect("point read completes while scan is in flight");

    // Release the scan and confirm it still produces the correct result.
    gate.release.notify_one();
    let scanned = scan_handle
        .await
        .expect("scan task joins")
        .expect("scan ok");
    assert_eq!(scanned.len(), 1, "gated scan returns the same single row");
}
