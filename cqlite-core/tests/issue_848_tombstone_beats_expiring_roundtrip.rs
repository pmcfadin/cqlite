//! Issue #848 (Epic #921): at EQUAL timestamp a cell TOMBSTONE must beat an
//! EXPIRING (TTL) cell — decided before any `localDeletionTime` compare (parity
//! Cassandra `a62c749`, `Cells#reconcile`). Without the fix an expiring cell
//! written at the same timestamp as a cell delete could resurrect the value
//! until its TTL lapsed.
//!
//! This is the NON-DORMANT, round-trip proof: the V5 reader surfaces per-cell
//! `ttl` / `localDeletionTime` for SIMPLE cells (see
//! `v5_compressed_legacy.rs::parse_block_for_compaction_emit`), which the merge
//! threads onto `CellData`. Here we drive the REAL write→flush→read path: an
//! expiring write in one generation and a same-timestamp cell delete in another,
//! scanned back through `SSTableManager` after the cross-generation merge. The
//! deleted column must be ABSENT in both source orders.
//!
//! Run with:
//!   CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
//!     cargo test --package cqlite-core --features write-support \
//!     --test issue_848_tombstone_beats_expiring_roundtrip
//!
//! Gated `not(feature = "tombstones")`: with `tombstones` enabled,
//! `SSTableManager::scan` resolves to the tombstones-specific implementation
//! that does NOT route through the `KWayMerger` compaction merge path this test
//! is asserting (roborev #974). The #848 tie-break itself is feature-independent
//! and is pinned by the `reconcile_cluster` and writer-bytes unit tests; this
//! round-trip only exercises the merge scan path, so we skip it under
//! `tombstones`. (The `tombstones` read-path reconciliation is out of #848's
//! compaction scope.)

#![cfg(all(feature = "write-support", not(feature = "tombstones")))]

use cqlite_core::platform::Platform;
use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::SSTableManager;
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::TableId as CqlTableId;
use cqlite_core::types::Value;
use cqlite_core::Config;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

const KS: &str = "ttl_tomb_ks";
const TBL: &str = "items";
/// Identical write timestamp for the expiring write and the cell delete.
const TS: i64 = 1_700_000_000_000_000;

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
                name: "v".to_string(),
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

fn expiring_write(id: i32, ttl: u32, ts: i64) -> Mutation {
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ops = vec![
        // A plain `name` so the row stays live and is emitted even after `v` is
        // deleted (a row of only a cell tombstone would otherwise be a pure
        // tombstone-carrier).
        CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text(format!("row-{id}")),
        },
        CellOperation::WriteWithTtl {
            column: "v".to_string(),
            value: Value::Text("expiring-if-buggy".to_string()),
            ttl_seconds: ttl,
        },
    ];
    Mutation::new(TableId::new(KS, TBL), pk, None, ops, ts, None)
}

fn delete_v(id: i32, ts: i64) -> Mutation {
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ops = vec![CellOperation::Delete {
        column: "v".to_string(),
        local_deletion_time: None,
    }];
    Mutation::new(TableId::new(KS, TBL), pk, None, ops, ts, None)
}

fn col<'a>(row: &'a Value, name: &str) -> Option<&'a Value> {
    match row {
        // Issue #1334: rows decode to `Value::Row` keyed by `Arc<str>`.
        Value::Row(cells) => cells
            .iter()
            .find_map(|(k, v)| if k.as_ref() == name { Some(v) } else { None }),
        _ => None,
    }
}

/// Write the expiring `v` for PK `id` in gen1 and the cell delete in gen2 (or
/// the reverse when `tombstone_first`), flush each generation separately, then
/// scan the merged directory and assert `v` is ABSENT (the tombstone wins at
/// equal ts).
fn run_case(tombstone_first: bool) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let temp_dir = TempDir::new().expect("tempdir");
    let data_dir = temp_dir.path().join("data");
    let wal_dir = temp_dir.path().join("wal");
    let schema = make_schema();

    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir.clone(), schema.clone());
    let mut engine = WriteEngine::new(config).expect("engine creation");

    let id = 1;
    if tombstone_first {
        engine.write(delete_v(id, TS)).expect("write delete");
        rt.block_on(engine.flush()).expect("flush g1").expect("g1");
        engine
            .write(expiring_write(id, 3600, TS))
            .expect("write expiring");
        rt.block_on(engine.flush()).expect("flush g2").expect("g2");
    } else {
        engine
            .write(expiring_write(id, 3600, TS))
            .expect("write expiring");
        rt.block_on(engine.flush()).expect("flush g1").expect("g1");
        engine.write(delete_v(id, TS)).expect("write delete");
        rt.block_on(engine.flush()).expect("flush g2").expect("g2");
    }
    rt.block_on(engine.close()).expect("close engine");

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

    // The row is still live (it carries `name`), but `v` must be absent: at equal
    // ts the cell tombstone beats the expiring cell, so `v` is NOT resurrected.
    let pk_bytes = id.to_be_bytes().to_vec();
    let row = results
        .iter()
        .find(|(k, _)| k.0 == pk_bytes)
        .map(|(_, v)| v.clone())
        .expect("row must be present (kept live by the `name` cell)");

    assert_eq!(
        col(&row, "name"),
        Some(&Value::Text(format!("row-{id}"))),
        "the live `name` cell keeps the row present"
    );
    assert!(
        col(&row, "v").is_none(),
        "tombstone_first={tombstone_first}: at equal ts the cell tombstone must \
         beat the expiring cell — `v` must NOT be resurrected, got {:?}",
        col(&row, "v")
    );
}

#[test]
fn tombstone_beats_expiring_at_equal_ts_expiring_first_roundtrip() {
    run_case(false);
}

#[test]
fn tombstone_beats_expiring_at_equal_ts_tombstone_first_roundtrip() {
    run_case(true);
}
