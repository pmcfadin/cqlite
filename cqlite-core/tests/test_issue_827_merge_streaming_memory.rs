//! Memory-bound regression test for Issue #827 (dhat-gated) — THE deliverable.
//!
//! **Problem**: the k-way merge producer (`merge::producer_thread`) read each
//! source via `iterate_all_partitions_for_compaction`, which fully materialised
//! the decompressed data section and parsed EVERY entry into a `Vec` before any
//! entry entered the bounded channel. End-to-end peak live heap therefore
//! scaled with total input size (≈O(sum of source sizes)), not with a bounded
//! read-ahead window — so a compaction whose inputs collectively exceed the
//! 128 MiB budget would blow past it.
//!
//! **Fix**: a sliding-window incremental stitch+parse
//! (`SSTableReader::stream_all_partitions_for_compaction`) feeding a streaming
//! producer. A source's decompressed content is never fully resident; peak per
//! source is bounded by roughly `max_partition_size + one_chunk +
//! channel_capacity`.
//!
//! **This test** writes several SSTables whose combined decompressed size
//! exceeds 128 MiB, runs an actual STCS compaction (k-way merge) under the dhat
//! heap profiler, and asserts the peak live heap stays within the 128 MiB
//! budget. On the pre-fix code the producer materialised each full source, so
//! the peak exceeded the budget; with the streaming read it stays bounded.
//!
//! The profiler is started immediately before the compaction so only the merge
//! workload is attributed (the write/flush setup that builds the large inputs
//! is intentionally outside the measured window).
//!
//! Gated on the `dhat-heap` feature; runs in the profiling job, not normal
//! `cargo test`. Run via:
//!
//! ```text
//! cargo test --package cqlite-core \
//!   --features write-support,cli-helpers,dhat-heap \
//!   --test test_issue_827_merge_streaming_memory --profile bench -- --nocapture
//! ```

#![cfg(all(
    feature = "write-support",
    feature = "cli-helpers",
    feature = "dhat-heap"
))]

use std::sync::Arc;
use std::time::Duration;

use cqlite_core::platform::Platform;
use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::SSTableManager;
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, STCSPolicy, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::TableId as CqlTableId;
use cqlite_core::types::Value;
use cqlite_core::Config;
use tempfile::TempDir;

// The dhat allocator must be the global allocator to observe every allocation.
// This test binary is separate from all others, so installing it here does not
// affect normal builds or other test binaries.
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

const HEAP_BUDGET_BYTES: usize = 128 * 1024 * 1024;
const KEYSPACE: &str = "issue827_mem_ks";
const TABLE: &str = "blobs";

// Workload sizing: payload bytes per row, rows per SSTable, number of SSTables.
// 4 SSTables × 800 rows × 48 KiB ≈ 150 MiB of decompressed source content —
// comfortably over the 128 MiB budget. The pre-fix producer held one whole
// source (~38 MiB parsed) plus the consumer's per-source channels; with several
// large sources the materialising path exceeded the budget. The streaming read
// keeps only one partition + one chunk per source resident.
const PAYLOAD_BYTES: usize = 48 * 1024;
const ROWS_PER_SSTABLE: i32 = 800;
const SSTABLE_COUNT: i32 = 4;

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
                name: "payload".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
    }
}

use std::collections::HashMap;

/// A distinct-enough payload per row so the data does not trivially compress to
/// nothing (we want a real decompressed window). The leading unique prefix
/// defeats run-length collapse while the bulk stays cheap to generate.
fn payload_for(id: i32) -> String {
    let mut s = format!("row-{id:08}-");
    s.push_str(&"abcdefghij".repeat((PAYLOAD_BYTES.saturating_sub(s.len())) / 10 + 1));
    s.truncate(PAYLOAD_BYTES);
    s
}

fn write_row(id: i32, timestamp: i64) -> Mutation {
    let table_id = TableId::new(KEYSPACE, TABLE);
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ops = vec![CellOperation::Write {
        column: "payload".to_string(),
        value: Value::Text(payload_for(id)),
    }];
    Mutation::new(table_id, pk, None, ops, timestamp, None)
}

fn make_policy() -> STCSPolicy {
    STCSPolicy::new(
        SSTABLE_COUNT as usize, // min_threshold — compact once all N exist
        32,                     // max_threshold
        0.5,                    // bucket_low
        1.5,                    // bucket_high
        0,                      // min_sstable_size — zero so files group together
    )
    .expect("valid STCS parameters")
}

fn count_data_files(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .filter(|e| e.file_name().to_string_lossy().ends_with("-big-Data.db"))
                .count()
        })
        .unwrap_or(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_merge_streaming_stays_within_heap_budget() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let wal_dir = temp_dir.path().join("wal");
    let schema = make_schema();

    // ── Phase 1 (UNMEASURED): write SSTABLE_COUNT large SSTables ──────────────
    // Disjoint partition-key ranges per SSTable so the merge does real work.
    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, schema.clone());
    let mut engine = WriteEngine::new(config).expect("engine creation");
    for table_idx in 0..SSTABLE_COUNT {
        let base = table_idx * ROWS_PER_SSTABLE;
        for id in base..base + ROWS_PER_SSTABLE {
            engine
                .write(write_row(id, 100 + id as i64))
                .expect("write row");
        }
        engine
            .flush()
            .await
            .expect("flush sstable")
            .expect("non-empty sstable");
    }

    let sstable_dir = data_dir.join(KEYSPACE).join(TABLE);
    assert_eq!(
        count_data_files(&sstable_dir),
        SSTABLE_COUNT as usize,
        "expected {SSTABLE_COUNT} Data.db files before compaction"
    );

    engine
        .set_merge_policy(Box::new(make_policy()))
        .expect("set merge policy");

    // ── Phase 2 (MEASURED): run the k-way-merge compaction under dhat ─────────
    // Start the profiler here so only the merge read/merge/write is attributed.
    let _profiler = dhat::Profiler::builder().testing().build();

    let budget = Duration::from_secs(120);
    let mut compaction_completed = false;
    for _ in 0..8 {
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
        "Issue #827: compaction must complete so the merge memory bound is exercised"
    );

    let stats = dhat::HeapStats::get();
    eprintln!(
        "Issue #827 merge streaming: {SSTABLE_COUNT} sources × {ROWS_PER_SSTABLE} rows × {} KiB \
         payload, peak heap {} bytes ({:.2} MiB) vs budget {} MiB",
        PAYLOAD_BYTES / 1024,
        stats.max_bytes,
        stats.max_bytes as f64 / (1024.0 * 1024.0),
        HEAP_BUDGET_BYTES / (1024 * 1024),
    );

    // ── Phase 3 (post-measurement sanity): N → 1 and rows preserved ───────────
    let merged_count = count_data_files(&sstable_dir);
    assert_eq!(
        merged_count, 1,
        "Issue #827: compaction must produce exactly 1 output SSTable (N → 1)"
    );

    assert!(
        stats.max_bytes <= HEAP_BUDGET_BYTES,
        "Issue #827: k-way-merge peak heap {} bytes ({:.2} MiB) exceeds the {} MiB budget — \
         the compaction read path regressed to materialising the whole source",
        stats.max_bytes,
        stats.max_bytes as f64 / (1024.0 * 1024.0),
        HEAP_BUDGET_BYTES / (1024 * 1024),
    );

    engine.close().await.expect("close engine");

    // Read back to confirm the merge preserved all partitions (correctness guard
    // alongside the memory assertion).
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
        (SSTABLE_COUNT * ROWS_PER_SSTABLE) as usize,
        "Issue #827: merged output must contain all rows"
    );

    drop(temp_dir);
}
