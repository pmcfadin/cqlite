//! Issue #2038: a NON-FROZEN collection (or UDT) column written with a TTL must
//! round-trip so that its per-cell metadata surfaces the expiry — so
//! `TTL(collection_col)` resolves to the authoritative value instead of `null`.
//!
//! This is the complex-column analogue of #1743, which fixed only the SCALAR
//! cell path. Root cause (read side): the complex-column per-cell metadata
//! builder in `v5_compressed_legacy/row_data.rs` hardcoded `expiration: None`,
//! so a non-frozen collection/UDT column written as expiring (each element cell
//! IS_EXPIRING with its own explicit TTL + localExpirationTime — exactly what a
//! `USING TTL` collection write emits) surfaced NO expiry, and `TTL(col)`
//! returned `null` even though the on-disk cells ARE expiring.
//!
//! The fix pairs the authoritative per-element aggregate that the reader already
//! computes (`max_element_expires_at` = the max localExpirationTime across the
//! collection's explicit-TTL elements) with its element's `max_element_ttl`
//! (added by this issue) into a `CellExpiration { ttl_seconds, expires_at_seconds }`.
//! No heuristics: both come from the decoded per-element cell fields.
//!
//! WRITE→READ regression guard: writes a row with a `set<int>` column under a
//! per-column TTL (`CellOperation::WriteWithTtl`, the whole-collection TTL write),
//! flushes to a single `nb` SSTable, reopens through `SSTableManager`, scans WITH
//! cell metadata, and asserts the collection column surfaces an expiring cell with
//! the written TTL.
//!
//! No wall-clock assertion race: the writer stamps each element's
//! localExpirationTime as `wallClockNowSeconds + ttl`. We bracket the write+flush
//! with a `[before, after]` wall-clock window and assert
//! `expires_at ∈ [before+ttl, after+ttl]`, plus `ttl_seconds == n` exactly.
//!
//!   CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
//!     cargo test --package cqlite-core --test issue_2038_collection_ttl_expiring_cell

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
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::TempDir;

const KS: &str = "ttl_coll_ks";
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
                name: "tags".to_string(),
                data_type: "set<int>".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// Build a row whose non-frozen `set<int>` column is written with a per-column
/// TTL — the whole-collection expiring write (`WriteWithTtl`), i.e. what the CQL
/// `INSERT ... USING TTL n` path produces for a collection column.
fn insert_collection_using_ttl(id: i32, elems: Vec<i32>, ttl_seconds: u32, ts: i64) -> Mutation {
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ops = vec![CellOperation::WriteWithTtl {
        column: "tags".to_string(),
        value: Value::Set(elems.into_iter().map(Value::Integer).collect()),
        ttl_seconds,
        local_deletion_time: None,
    }];
    Mutation::new(TableId::new(KS, TBL), pk, None, ops, ts, None)
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_secs() as i64
}

#[test]
fn collection_ttl_write_round_trips_as_expiring_cell() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let wal_dir = temp_dir.path().join("wal");
    let schema = make_schema();

    // Pinned reconciliation timestamp (USING TIMESTAMP is independent of the TTL
    // expiry clock, so this need not track wall-clock).
    const TS: i64 = 1_700_000_000_000_000;
    const TTL: u32 = 86_400;
    const TTL_I32: i32 = TTL as i32;

    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir.clone(), schema.clone());
    let mut engine = WriteEngine::new(config).expect("engine creation");

    // Bracket the write+flush with a wall-clock window: each element's on-disk
    // localExpirationTime is stamped as `wallClockNow + ttl`, so the surfaced
    // expiry must land inside `[before+ttl, after+ttl]`.
    let before = now_secs();
    engine
        .write(insert_collection_using_ttl(1, vec![10, 20, 30], TTL, TS))
        .unwrap();
    rt.block_on(engine.flush()).expect("flush").expect("gen1");
    let after = now_secs();
    rt.block_on(engine.close()).expect("close engine");

    // Reopen and scan WITH cell metadata (the authoritative surface `TTL()` reads).
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

    assert_eq!(results.len(), 1, "expected the single row written");

    let by_pk: HashMap<Vec<u8>, HashMap<String, CellWriteMetadata>> =
        results.into_iter().map(|(k, _v, m)| (k.0, m)).collect();

    let meta = by_pk
        .get(1_i32.to_be_bytes().as_slice())
        .expect("row for id=1 present");

    let tags_meta = meta
        .get("tags")
        .expect("per-cell metadata for the 'tags' collection must be surfaced");

    // Core regression (#2038): TTL(tags) must NOT be null — the collection's
    // expiring element cells carry an explicit per-element TTL + localExpirationTime.
    let exp = tags_meta.expiration.as_ref().expect(
        "Issue #2038: TTL(tags) must be present (expiring non-frozen collection), not null",
    );

    assert_eq!(
        exp.ttl_seconds, TTL_I32,
        "TTL(tags) must equal the written per-column TTL value"
    );

    // localExpirationTime = wallClockNow + ttl, bracketed by the flush window.
    let lo = before + TTL as i64;
    let hi = after + TTL as i64;
    assert!(
        (lo..=hi).contains(&exp.expires_at_seconds),
        "expires_at {} must be wallClockNow+ttl, in [{lo}, {hi}]",
        exp.expires_at_seconds
    );

    drop(temp_dir);
}
