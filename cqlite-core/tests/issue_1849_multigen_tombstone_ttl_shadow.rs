//! Issue #1849 (P0, split from #1741): a MULTI-generation `SELECT` (a table dir
//! with >1 SSTable generation, `candidates > 1`) routes through the write-support
//! `KWayMerger` for cross-generation reconciliation. That merger does last-write-wins
//! + tombstone reconciliation but does NOT apply read-time TTL expiry — so before
//! this fix a multi-gen read returned TTL-EXPIRED cells as live, even though the
//! single-gen path (#1741) already hides them.
//!
//! The fix runs the merger's reconciled output through the SAME single-gen
//! `PartitionShadow` per-cell decision (`cell_shadowed_or_expired`) POST-merge, so
//! there is ONE read-visibility implementation across single- and multi-gen.
//!
//! These tests drive the real `SSTableManager::scan` multi-generation path (the same
//! harness `issue_883_cross_generation_read_merge` uses): two generations are flushed
//! via the public `WriteEngine` API (no compaction), then scanned. Each generation
//! carries a row with a live-forever `name` cell alongside an EXPIRED `token` cell —
//! written with an explicit past `localDeletionTime` (`WriteWithTtl { local_deletion_time }`)
//! so expiry is DETERMINISTIC (no wall-clock/override dependency): its expiry instant
//! is ~1970, decades before any real read clock.
//!
//! Revert-verify: on pre-fix `main` the merged scan returns the `token` cell as live
//! (each assertion `token.is_none()` FAILS); the fix drops it while keeping `name`.
//!
//! WRITE surface note: this exercises UNCOMPRESSED SSTables written by `WriteEngine`
//! (the production write surface, issue #1406), guaranteeing the `KWayMerger` reads
//! them directly rather than falling back to per-reader concatenation — so the bug is
//! actually observed on the merger path, not masked by the fallback.

#![cfg(feature = "write-support")]

use cqlite_core::platform::Platform;
use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::SSTableManager;
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::TableId as CqlTableId;
use cqlite_core::types::Value;
use cqlite_core::Config;
use cqlite_core::ScanRow;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

const KS: &str = "gen_ttl_ks";
const TBL: &str = "sessions";

/// An expiration instant DECADES in the past (1970-01-12) so the cell is
/// unconditionally TTL-expired at any realistic read clock — no wall-clock or
/// `CQLITE_TTL_NOW_OVERRIDE_SECS` dependency, works identically in debug and release.
const PAST_EXPIRY_SECS: i32 = 1_000_000;

fn make_schema() -> TableSchema {
    TableSchema {
        keyspace: KS.to_string(),
        table: TBL.to_string(),
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
                name: "name".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "token".to_string(),
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

/// A row with a live-forever `name` plus an already-EXPIRED `token` (explicit past
/// `localDeletionTime`). The row-liveness marker itself is live-forever (no row TTL).
fn write_name_plus_expired_token(id: i32, name: &str, token: &str, ts: i64) -> Mutation {
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ops = vec![
        CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text(name.to_string()),
        },
        CellOperation::WriteWithTtl {
            column: "token".to_string(),
            value: Value::Text(token.to_string()),
            // Any positive TTL; expiry is pinned by the explicit past LDT below.
            ttl_seconds: 60,
            local_deletion_time: Some(PAST_EXPIRY_SECS),
        },
    ];
    Mutation::new(TableId::new(KS, TBL), pk, None, ops, ts, None)
}

/// A live row with only a `name` cell (no TTL anywhere) — must always survive.
fn write_name_only(id: i32, name: &str, ts: i64) -> Mutation {
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ops = vec![CellOperation::Write {
        column: "name".to_string(),
        value: Value::Text(name.to_string()),
    }];
    Mutation::new(TableId::new(KS, TBL), pk, None, ops, ts, None)
}

fn count_data_files(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir)
        .expect("read sstable dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with("-big-Data.db"))
        .count()
}

fn col<'a>(row: &'a ScanRow, name: &str) -> Option<&'a Value> {
    match row {
        ScanRow::Row(cells) => cells
            .iter()
            .find_map(|(k, v)| if k.as_ref() == name { Some(v) } else { None }),
        _ => None,
    }
}

#[test]
fn multigen_select_hides_ttl_expired_cells_across_generations() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let wal_dir = temp_dir.path().join("wal");
    let schema = make_schema();

    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir.clone(), schema.clone());
    let mut engine = WriteEngine::new(config).expect("engine creation");

    // ── Gen 1 (ts=100): PK1 name + EXPIRED token; PK2 name-only (live) ──────────
    engine
        .write(write_name_plus_expired_token(1, "keep1", "secret1", 100))
        .unwrap();
    engine.write(write_name_only(2, "keep2", 100)).unwrap();
    rt.block_on(engine.flush()).expect("flush 1").expect("gen1");

    // ── Gen 2 (ts=200): PK3 name + EXPIRED token ────────────────────────────────
    engine
        .write(write_name_plus_expired_token(3, "keep3", "secret3", 200))
        .unwrap();
    rt.block_on(engine.flush()).expect("flush 2").expect("gen2");

    rt.block_on(engine.close()).expect("close engine");

    // Precondition: a genuine multi-generation directory (candidates > 1 → merger).
    let sstable_dir = data_dir.join(KS).join(TBL);
    assert_eq!(
        count_data_files(&sstable_dir),
        2,
        "test must exercise a multi-generation directory (the KWayMerger path)"
    );

    // ── Reopen and scan through the manager's multi-gen merge path ──────────────
    let cqlite_config = Config::default();
    let manager = rt.block_on(async {
        let platform = Arc::new(Platform::new(&cqlite_config).await.expect("platform"));
        SSTableManager::new(
            &data_dir,
            &cqlite_config,
            platform,
            #[cfg(feature = "state_machine")]
            None,
        )
        .await
        .expect("SSTableManager open")
    });

    let table_id = CqlTableId::from(format!("{KS}.{TBL}").as_str());
    let results = rt
        .block_on(manager.scan(&table_id, None, None, None, Some(&schema)))
        .expect("scan must not error");

    let by_pk: HashMap<Vec<u8>, ScanRow> = results
        .iter()
        .map(|(k, v)| (k.as_bytes().to_vec(), v.clone()))
        .collect();
    let pk = |id: i32| -> Vec<u8> { id.to_be_bytes().to_vec() };

    // Three live rows (PK1, PK2, PK3); the expired `token` cells are hidden, the
    // live `name` cells survive. The rows themselves stay (live row-liveness marker).
    assert_eq!(
        results.len(),
        3,
        "expected 3 live rows after cross-generation merge, got {}",
        results.len()
    );

    let row1 = by_pk.get(&pk(1)).expect("PK1 present");
    assert_eq!(col(row1, "name"), Some(&Value::Text("keep1".to_string())));
    assert!(
        col(row1, "token").is_none(),
        "issue #1849: PK1 token expired (past localDeletionTime) — a multi-gen \
         SELECT must hide it, got {:?}",
        col(row1, "token")
    );

    let row2 = by_pk.get(&pk(2)).expect("PK2 present");
    assert_eq!(col(row2, "name"), Some(&Value::Text("keep2".to_string())));

    let row3 = by_pk.get(&pk(3)).expect("PK3 present");
    assert_eq!(col(row3, "name"), Some(&Value::Text("keep3".to_string())));
    assert!(
        col(row3, "token").is_none(),
        "issue #1849: PK3 token expired (past localDeletionTime) — a multi-gen \
         SELECT must hide it, got {:?}",
        col(row3, "token")
    );

    drop(temp_dir);
}
