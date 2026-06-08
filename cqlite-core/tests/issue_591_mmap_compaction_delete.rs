//! Regression test for Issue #591 — mmap write-while-mapped guard + delete policy.
//!
//! The mmap read backend (opt-in, #589) aliases a Data.db file's bytes for the
//! reader's lifetime. The SAFETY contract is that mapped files are immutable for
//! that lifetime: truncating or deleting a mapped file out from under a live
//! reader can fault with `SIGBUS` on Unix (unrecoverable as an `io::Error`) and
//! can block deletion on Windows.
//!
//! Compaction deletes its input SSTables once the merged output is published, so
//! the two paths must not collide. The invariant enforced by the fix:
//!
//! 1. Compaction reads its inputs through **buffered I/O**, never a memory map,
//!    and drains every entry into memory before any delete — so the merger never
//!    holds a mapping over a file it deletes (no SIGBUS).
//! 2. Deletion removes `TOC.txt` **first** (the publication barrier), then the
//!    data components best-effort. The compaction candidate scan skips any
//!    Data.db lacking a sibling TOC.txt, so a not-yet-removable component (e.g.
//!    pinned by a mapped reader on Windows) is an invisible orphan, never a
//!    duplicate-row source and never re-fed to the merger.
//!
//! This test exercises the real cross-flow: a separate read-path
//! [`SSTableManager`] opens the inputs **with mmap enabled** and scans them
//! (mapping their Data.db files), and *then* the write engine compacts and
//! deletes those same inputs. It must complete without a panic/SIGBUS and the
//! merged output must read back correctly.

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

const KEYSPACE: &str = "issue591_ks";
const TABLE: &str = "items";
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
        ],
        comments: HashMap::new(),
    }
}

fn write_row(id: i32, name: &str, timestamp: i64) -> Mutation {
    let table_id = TableId::new(KEYSPACE, TABLE);
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ops = vec![CellOperation::Write {
        column: "name".to_string(),
        value: Value::Text(name.to_string()),
    }];
    Mutation::new(table_id, pk, None, ops, timestamp, None)
}

fn make_policy() -> STCSPolicy {
    STCSPolicy::new(SSTABLE_COUNT as usize, 32, 0.5, 1.5, 0).expect("valid STCS parameters")
}

/// Config with mmap forced ON for every Data.db (min size 0), so the read path
/// maps the compaction inputs.
fn mmap_config() -> Config {
    let mut cfg = Config::default();
    cfg.storage.use_mmap = true;
    cfg.storage.mmap_min_size_bytes = 0;
    cfg
}

fn count_data_files(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir)
        .expect("read sstable dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with("-big-Data.db"))
        .count()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_deletes_inputs_held_by_a_mapped_reader() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let wal_dir = temp_dir.path().join("wal");
    let schema = make_schema();

    // ── Phase 1: write N published SSTables (each flush writes a TOC.txt) ──────
    let cfg = WriteEngineConfig::new(data_dir.clone(), wal_dir.clone(), schema.clone());
    let mut engine = WriteEngine::new(cfg).expect("engine creation");

    for table_idx in 0..SSTABLE_COUNT {
        let base = table_idx * ROWS_PER_SSTABLE + 1;
        for id in base..base + ROWS_PER_SSTABLE {
            engine
                .write(write_row(id, &format!("name-{id}"), 100 + id as i64))
                .expect("write row");
        }
        engine
            .flush()
            .await
            .expect("flush")
            .expect("non-empty sstable");
    }

    let sstable_dir = data_dir.join(KEYSPACE).join(TABLE);
    assert_eq!(
        count_data_files(&sstable_dir),
        SSTABLE_COUNT as usize,
        "expected {SSTABLE_COUNT} input Data.db files before compaction"
    );

    // ── Phase 2: open a read-path manager WITH mmap and scan, mapping the inputs.
    // The manager is kept alive across the compaction below, so its memory maps
    // over the input Data.db files are live while the write engine deletes them.
    let read_cfg = mmap_config();
    let platform = Arc::new(Platform::new(&read_cfg).await.expect("platform"));
    let mapped_manager = SSTableManager::new(
        &data_dir,
        &read_cfg,
        platform,
        #[cfg(feature = "state_machine")]
        None,
    )
    .await
    .expect("open SSTableManager with mmap");

    let table_id = CqlTableId::from(format!("{KEYSPACE}.{TABLE}").as_str());
    let pre_rows = mapped_manager
        .scan(&table_id, None, None, None, Some(&schema))
        .await
        .expect("pre-compaction scan (forces the input files to be mapped)");
    assert_eq!(
        pre_rows.len(),
        EXPECTED_ROWS,
        "the mapped reader should see all {EXPECTED_ROWS} rows across the inputs"
    );

    // ── Phase 3: compact the same inputs the mapped reader holds. This deletes
    // files that Phase 2 mapped. With the fix it must NOT panic/SIGBUS and must
    // collapse N inputs → 1 output.
    engine
        .set_merge_policy(Box::new(make_policy()))
        .expect("set merge policy");

    let budget = Duration::from_secs(30);
    let mut completed = false;
    for _ in 0..5 {
        let report = engine
            .maintenance_step(budget)
            .expect("maintenance_step must not error or panic while inputs are mapped");
        if !report.completed_merges.is_empty() {
            completed = true;
            break;
        }
        if !report.pending_compaction {
            break;
        }
    }
    assert!(completed, "compaction must complete");

    let stats = engine.maintenance_stats();
    assert_eq!(stats.sstables_merged_in, SSTABLE_COUNT as u64);
    assert_eq!(stats.sstables_produced, 1);
    assert_eq!(
        count_data_files(&sstable_dir),
        1,
        "after compaction exactly 1 Data.db must exist (N → 1)"
    );

    // The mapped reader is still alive here; dropping it now releases the maps.
    // On Unix the deleted inputs' inodes were kept alive by these maps with no
    // ill effect; on Windows any input pinned by these maps would have been left
    // as a TOC-less orphan (unpublished, reclaimed on next startup).
    drop(mapped_manager);
    engine.close().await.expect("close engine");

    // ── Phase 4: a fresh mmap-enabled manager reads the merged output correctly.
    let read_cfg2 = mmap_config();
    let platform2 = Arc::new(Platform::new(&read_cfg2).await.expect("platform"));
    let manager2 = SSTableManager::new(
        &data_dir,
        &read_cfg2,
        platform2,
        #[cfg(feature = "state_machine")]
        None,
    )
    .await
    .expect("reopen SSTableManager");

    let post_rows = manager2
        .scan(&table_id, None, None, None, Some(&schema))
        .await
        .expect("post-compaction scan");
    assert_eq!(
        post_rows.len(),
        EXPECTED_ROWS,
        "merged output must contain all {EXPECTED_ROWS} rows with no duplicates"
    );

    drop(temp_dir);
}
