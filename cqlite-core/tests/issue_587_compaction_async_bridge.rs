//! Regression test for Issue #587 — compaction async-to-sync bridge panic.
//!
//! `WriteEngine::maintenance_step()` is synchronous but bridges to async I/O to
//! read the input SSTables of a merge (`merge::block_on_async`, reached via
//! `KWayMerger::new` → `SSTableRowIteratorAdapter::open`, and again in
//! `finalize_merge_blocking`). The pre-fix bridge called
//! `tokio::runtime::Handle::current().block_on(future)` whenever a runtime was
//! already running on the thread, which panics with
//! *"Cannot start a runtime from within a runtime"*.
//!
//! Because the bridge is only reached once a merge has input SSTables to read,
//! STCS compaction worked in isolation (plain `#[test]`) but was **unreachable
//! from any `#[tokio::main]`/async caller** — exactly how the CLI (`maintenance`,
//! `export-sstable --compact`, both under `#[tokio::main]`) invokes it.
//!
//! These tests drive `maintenance_step()` **from inside a Tokio runtime** (the
//! real entry-point condition the CLI hits). They reproduce the panic before the
//! fix and assert correct compaction (N SSTables → 1, correct row count) after.
//!
//! - [`maintenance_step_from_multi_thread_runtime`] mirrors the CLI's default
//!   `#[tokio::main]` multi-thread runtime.
//! - [`maintenance_step_from_current_thread_runtime`] covers the current-thread
//!   flavor (the default `#[tokio::test]` flavor), which `block_in_place` cannot
//!   support — proving the scoped-thread bridge is runtime-flavor-agnostic.

#![cfg(feature = "write-support")]

use cqlite_core::platform::Platform;
use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::SSTableManager;
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, STCSPolicy, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::TableId as CqlTableId;
use cqlite_core::types::Value;
use cqlite_core::Config;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

const KEYSPACE: &str = "issue587_ks";
const TABLE: &str = "items";

/// 4 SSTables × 10 disjoint partition keys each = 40 rows total.
const SSTABLE_COUNT: i32 = 4;
const ROWS_PER_SSTABLE: i32 = 10;
const EXPECTED_ROWS: usize = (SSTABLE_COUNT * ROWS_PER_SSTABLE) as usize;

fn make_schema() -> TableSchema {
    TableSchema {
        keyspace: KEYSPACE.to_string(),
        table: TABLE.to_string(),
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

fn write_row(id: i32, name: &str, score: i32, timestamp: i64) -> Mutation {
    let table_id = TableId::new(KEYSPACE, TABLE);
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ops = vec![
        CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text(name.to_string()),
        },
        CellOperation::Write {
            column: "score".to_string(),
            value: Value::Integer(score),
        },
    ];
    Mutation::new(table_id, pk, None, ops, timestamp, None)
}

/// STCS policy that compacts groups of >= `SSTABLE_COUNT` SSTables.
/// `min_sstable_size = 0` buckets the tiny test files together.
fn make_policy() -> STCSPolicy {
    STCSPolicy::new(
        SSTABLE_COUNT as usize, // min_threshold — compact once all N exist
        32,                     // max_threshold
        0.5,                    // bucket_low
        1.5,                    // bucket_high
        0,                      // min_sstable_size — zero so small files group together
    )
    .expect("valid STCS parameters")
}

fn count_data_files(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir)
        .expect("read sstable dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with("-big-Data.db"))
        .count()
}

/// The shared scenario, executed from within an active Tokio runtime.
///
/// Writes `SSTABLE_COUNT` SSTables with disjoint partition keys, then runs
/// `maintenance_step()` (the **synchronous** API) directly from this async
/// context. Pre-fix this panics; post-fix it compacts N SSTables → 1 and the
/// merged output reads back all `EXPECTED_ROWS` rows.
async fn run_compaction_from_async_context() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let wal_dir = temp_dir.path().join("wal");
    let schema = make_schema();

    // ── Phase 1: write N SSTables, each with disjoint partition keys ──────────
    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir.clone(), schema.clone());
    let mut engine = WriteEngine::new(config).expect("engine creation");

    for table_idx in 0..SSTABLE_COUNT {
        let base = table_idx * ROWS_PER_SSTABLE + 1;
        for id in base..base + ROWS_PER_SSTABLE {
            let ts = 100 + (id as i64);
            engine
                .write(write_row(id, &format!("name-{id}"), id * 10, ts))
                .expect("write row");
        }
        // flush() is async — awaiting it here is fine; the bug is in the *sync*
        // maintenance_step bridge, not flush from an async context.
        let info = engine
            .flush()
            .await
            .expect("flush sstable")
            .expect("non-empty sstable");
        assert_eq!(
            info.partition_count, ROWS_PER_SSTABLE as usize,
            "each SSTable has {ROWS_PER_SSTABLE} partitions"
        );
    }

    let sstable_dir = data_dir.join(KEYSPACE).join(TABLE);
    assert_eq!(
        count_data_files(&sstable_dir),
        SSTABLE_COUNT as usize,
        "expected {SSTABLE_COUNT} Data.db files before compaction"
    );

    // ── Phase 2: compaction via the SYNC maintenance_step from async context ──
    engine
        .set_merge_policy(Box::new(make_policy()))
        .expect("set merge policy");

    // This is the Issue #587 reproduction: a synchronous method whose internal
    // async-to-sync bridge runs while a Tokio runtime is already active on this
    // thread. Pre-fix, the first call that starts a real merge panics here with
    // "Cannot start a runtime from within a runtime".
    let budget = Duration::from_secs(30);
    let mut compaction_completed = false;
    for _ in 0..5 {
        let report = engine
            .maintenance_step(budget)
            .expect("maintenance_step must not error");
        if !report.completed_merges.is_empty() {
            compaction_completed = true;
            break;
        }
        if !report.pending_compaction {
            break;
        }
    }
    assert!(
        compaction_completed,
        "compaction must complete within 5 maintenance_step calls"
    );

    let stats = engine.maintenance_stats();
    assert_eq!(
        stats.compactions_completed, 1,
        "exactly 1 compaction must have completed"
    );
    assert_eq!(
        stats.sstables_merged_in, SSTABLE_COUNT as u64,
        "all {SSTABLE_COUNT} input SSTables must have been consumed"
    );
    assert_eq!(
        stats.sstables_produced, 1,
        "exactly 1 output SSTable must have been produced"
    );
    assert_eq!(
        count_data_files(&sstable_dir),
        1,
        "after compaction exactly 1 Data.db must exist (N → 1)"
    );

    engine.close().await.expect("close engine");

    // ── Phase 3: read back the merged SSTable and assert the row count ────────
    let cqlite_config = Config::default();
    let platform = Arc::new(
        Platform::new(&cqlite_config)
            .await
            .expect("platform creation"),
    );
    let manager = SSTableManager::new(
        &data_dir,
        &cqlite_config,
        platform,
        #[cfg(feature = "state_machine")]
        None,
    )
    .await
    .expect("SSTableManager must open the merged output");

    let table_id = CqlTableId::from(format!("{KEYSPACE}.{TABLE}").as_str());
    let results = manager
        .scan(&table_id, None, None, None, Some(&schema))
        .await
        .expect("post-compaction scan must not error");

    assert_eq!(
        results.len(),
        EXPECTED_ROWS,
        "compacted output must contain all {EXPECTED_ROWS} disjoint rows (got {})",
        results.len()
    );

    // Spot-check that every original partition key survived the merge.
    let keys: std::collections::HashSet<Vec<u8>> = results
        .into_iter()
        .map(|(k, _)| k.as_bytes().to_vec())
        .collect();
    for id in 1..=(SSTABLE_COUNT * ROWS_PER_SSTABLE) {
        let key: Vec<u8> = id.to_be_bytes().into();
        assert!(
            keys.contains(&key),
            "partition key id={id} must survive compaction"
        );
    }

    // temp_dir kept alive until here.
    drop(temp_dir);
}

/// Multi-thread runtime — mirrors the CLI's default `#[tokio::main]`, which is
/// how `maintenance` / `export-sstable --compact` reach `maintenance_step`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn maintenance_step_from_multi_thread_runtime() {
    run_compaction_from_async_context().await;
}

/// Current-thread runtime — the default `#[tokio::test]` flavor. `block_in_place`
/// would panic here; the scoped-thread bridge handles it, proving the fix is
/// runtime-flavor-agnostic.
#[tokio::test(flavor = "current_thread")]
async fn maintenance_step_from_current_thread_runtime() {
    run_compaction_from_async_context().await;
}
