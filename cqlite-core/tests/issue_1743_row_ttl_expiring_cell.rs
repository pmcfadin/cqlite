//! Issue #1743: a row written with `USING TTL n` (statement-level TTL,
//! `Mutation::ttl_seconds`) must round-trip as an EXPIRING cell whose per-cell
//! metadata surfaces the TTL — so `TTL(col)` resolves to the authoritative value
//! instead of `null`.
//!
//! Root cause (read side): a statement-level `USING TTL` INSERT emits each simple
//! cell with the `USE_ROW_TTL` (0x10) flag — the cell carries NO explicit TTL /
//! localExpirationTime and instead inherits the ROW's pk-liveness expiry
//! (`liveness_expires_at_seconds`, from the row `HAS_TTL` flag). The per-cell
//! metadata builder resolved the inherited expiry from `row_header.ttl` paired with
//! `row_header.local_deletion_time` — but `local_deletion_time` is the GC-grace
//! clock set ONLY by a row tombstone (`HAS_DELETION`) and is `None` for a plain TTL
//! INSERT. So the fallback produced `None`, and `TTL(col)` returned `null` even
//! though the on-disk cell IS expiring. The fix mirrors the #1741 shadow-path expiry
//! resolution, using `liveness_expires_at_seconds` for the inherited expiry.
//!
//! This is the WRITE→READ regression guard: it writes a row through the public
//! `WriteEngine` with a statement-level TTL (exactly what the CLI `USING TTL` path
//! builds), flushes to a single `nb` SSTable, reopens through `SSTableManager`, and
//! asserts the surfaced per-cell metadata is an expiring cell with the written TTL.
//!
//! No wall-clock assertion race: the writer stamps the on-disk localExpirationTime
//! as `wallClockNowSeconds + ttl` (matching Cassandra's `nowInSec + ttl`, which is
//! independent of `USING TIMESTAMP`). We capture a `[before, after]` wall-clock
//! window that BRACKETS the flush and assert `expires_at ∈ [before+ttl, after+ttl]`,
//! and assert `ttl_seconds == n` exactly.
//!
//!   CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
//!     cargo test --package cqlite-core --test issue_1743_row_ttl_expiring_cell

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

const KS: &str = "ttl_ks";
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
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// Build a row with a plain `Write` cell under a STATEMENT-LEVEL TTL — i.e. the
/// mutation the CLI/CQL `INSERT ... USING TTL n` path constructs (row-level
/// `Mutation::ttl_seconds`, NOT a per-cell `WriteWithTtl`).
fn insert_using_ttl(id: i32, name: &str, ttl_seconds: u32, ts: i64) -> Mutation {
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ops = vec![CellOperation::Write {
        column: "name".to_string(),
        value: Value::Text(name.to_string()),
    }];
    Mutation::new(TableId::new(KS, TBL), pk, None, ops, ts, Some(ttl_seconds))
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_secs() as i64
}

#[test]
fn row_ttl_insert_round_trips_as_expiring_cell() {
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

    // Bracket the write+flush with a wall-clock window: the on-disk
    // localExpirationTime is stamped as `wallClockNow + ttl`, so the surfaced
    // expiry must land inside `[before+ttl, after+ttl]`.
    let before = now_secs();
    engine.write(insert_using_ttl(1, "alice", TTL, TS)).unwrap();
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

    let by_pk: HashMap<Vec<u8>, HashMap<String, CellWriteMetadata>> = results
        .into_iter()
        .map(|(k, _v, m)| (k.as_bytes().to_vec(), m))
        .collect();

    let meta = by_pk
        .get(1_i32.to_be_bytes().as_slice())
        .expect("row for id=1 present");

    let name_meta = meta
        .get("name")
        .expect("per-cell metadata for 'name' must be surfaced");

    // WRITETIME(name): the authoritative pinned write timestamp.
    assert_eq!(
        name_meta.write_timestamp_micros, TS,
        "WRITETIME(name) must equal the written timestamp"
    );

    // Core regression: TTL(name) must NOT be null — the USE_ROW_TTL cell inherits
    // the row-liveness expiry.
    let exp = name_meta
        .expiration
        .as_ref()
        .expect("Issue #1743: TTL(name) must be present (expiring cell), not null");

    assert_eq!(
        exp.ttl_seconds, TTL_I32,
        "TTL(name) must equal the statement-level USING TTL value"
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
