//! Issue #1535: `WRITETIME(col)` / `TTL(col)` must resolve from the authoritative
//! per-cell metadata under `--features tombstones`, not return null.
//!
//! Before the fix, `SSTableManager::scan_with_cell_metadata` had a
//! `#[cfg(feature = "tombstones")]` stub that delegated to the plain `scan` and
//! returned an EMPTY per-cell metadata map ("WRITETIME/TTL will still return null
//! when tombstones are enabled"). So a live cell that carries a real write
//! timestamp / TTL reported `null` for `WRITETIME()` / `TTL()` under
//! `--features tombstones`, while the default / `write-support` build returned the
//! real values — a correctness/parity gap for advertised v0.12 features.
//!
//! The metadata comes from the AUTHORITATIVE per-cell timestamp/TTL the SSTable
//! carries (no-heuristics mandate) — never inferred. This test writes a
//! single-generation table via the public `WriteEngine` API with a KNOWN write
//! timestamp and a KNOWN per-cell TTL, reopens through `SSTableManager`, and asserts
//! the per-cell metadata surfaced by `scan_with_cell_metadata` is non-empty and
//! equals what was written.
//!
//! This test is gated on `write-support` (it uses the `WriteEngine`); the default
//! build enables `write-support` and so does `--features tombstones` (default
//! features stay on), so the SAME assertions run — and must PASS — in BOTH builds.
//! It is the pinned parity guard for the `tombstones` build specifically:
//!
//!   CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
//!     cargo test --package cqlite-core --features tombstones \
//!     --test issue_1535_writetime_ttl_tombstones

#![cfg(feature = "write-support")]

use cqlite_core::platform::Platform;
use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::SSTableManager;
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::TableId as CqlTableId;
use cqlite_core::types::{CellWriteMetadata, Value};
use cqlite_core::Config;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

const KS: &str = "wt_ks";
const TBL: &str = "items";

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
                name: "score".to_string(),
                data_type: "int".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// Write a row with a plain `name` cell and a `score` cell carrying a per-cell TTL,
/// both stamped with the same authoritative write timestamp `ts`.
fn write_row_with_ttl(id: i32, name: &str, score: i32, ttl_seconds: u32, ts: i64) -> Mutation {
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ops = vec![
        CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text(name.to_string()),
        },
        CellOperation::WriteWithTtl {
            column: "score".to_string(),
            value: Value::Integer(score),
            ttl_seconds,
            local_deletion_time: None,
        },
    ];
    Mutation::new(TableId::new(KS, TBL), pk, None, ops, ts, None)
}

#[test]
fn live_cell_writetime_and_ttl_resolve_under_tombstones() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let wal_dir = temp_dir.path().join("wal");
    let schema = make_schema();

    const TS: i64 = 1_700_000_000_000_000;
    const TTL: u32 = 3600;
    const TTL_I32: i32 = TTL as i32;

    // ── Single generation: write two live rows, flush once (no compaction) ──────
    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir.clone(), schema.clone());
    let mut engine = WriteEngine::new(config).expect("engine creation");
    engine
        .write(write_row_with_ttl(1, "alice", 11, TTL, TS))
        .unwrap();
    engine
        .write(write_row_with_ttl(2, "bob", 22, TTL, TS))
        .unwrap();
    rt.block_on(engine.flush()).expect("flush").expect("gen1");
    rt.block_on(engine.close()).expect("close engine");

    // ── Reopen and scan WITH cell metadata ─────────────────────────────────────
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
        .block_on(manager.scan_with_cell_metadata(&table_id, None, None, None, Some(&schema)))
        .expect("metadata scan must not error");

    assert_eq!(results.len(), 2, "expected the two live rows written");

    let by_pk: HashMap<Vec<u8>, HashMap<String, CellWriteMetadata>> = results
        .into_iter()
        .map(|(k, _v, m)| (k.as_bytes().to_vec(), m))
        .collect();

    for id in [1_i32, 2] {
        let meta = by_pk
            .get(id.to_be_bytes().as_slice())
            .unwrap_or_else(|| panic!("row for id={id} present"));

        // Core regression guard (Issue #1535): per-cell metadata is NOT empty under
        // `--features tombstones`. The old stub returned an empty map here.
        assert!(
            !meta.is_empty(),
            "id={id}: per-cell metadata must be surfaced under tombstones, not empty"
        );

        // WRITETIME(name): the authoritative write timestamp, NOT null.
        let name_meta = meta
            .get("name")
            .unwrap_or_else(|| panic!("id={id}: WRITETIME(name) must be present, not null"));
        assert_eq!(
            name_meta.write_timestamp_micros, TS,
            "id={id}: WRITETIME(name) must equal the written timestamp"
        );
        // Plain cell (no TTL) has no expiration.
        assert!(
            name_meta.expiration.is_none(),
            "id={id}: name was written without TTL; expiration must be None"
        );

        // WRITETIME(score): non-null and equal to the written timestamp.
        let score_meta = meta
            .get("score")
            .unwrap_or_else(|| panic!("id={id}: WRITETIME(score) must be present, not null"));
        assert_eq!(
            score_meta.write_timestamp_micros, TS,
            "id={id}: WRITETIME(score) must equal the written timestamp"
        );
        // TTL(score): the authoritative per-cell TTL the cell was written with.
        let exp = score_meta
            .expiration
            .as_ref()
            .unwrap_or_else(|| panic!("id={id}: TTL(score) must be present, not null"));
        assert_eq!(
            exp.ttl_seconds, TTL_I32,
            "id={id}: TTL(score) must equal the written per-cell TTL"
        );
    }

    drop(temp_dir);
}
