//! Memory-bound regression test for issue #2299 (uncompressed compaction
//! within-partition streaming, dhat-gated) — originally held back from #1668's
//! branch (the doc-comment below preserves that lineage). Measured 424 MiB peak
//! vs. a 128 MiB budget — root-caused to THIS issue (the uncompressed compaction
//! read path materialising a whole wide partition per source), not the merge
//! layer.
//!
//! CQLite's ENTIRE production write surface emits UNCOMPRESSED SSTables (no
//! `CompressionInfo.db`), so every real compaction of CQLite's own output takes
//! the non-stitching read path. This test writes four uncompressed Data.db
//! files, each covering a DISJOINT clustering-key range of the SAME wide
//! partition (id = 1), then runs a real STCS compaction (k-way merge) under the
//! dhat heap profiler and asserts the peak live heap stays within 128 MiB. On
//! the pre-fix code the uncompressed compaction read materialised the whole wide
//! partition per source, blowing the budget; with the bounded within-partition
//! streaming read it stays bounded.
//!
//! Run via:
//! ```text
//! cargo test --package cqlite-core --features write-support,dhat-heap \
//!   --test test_issue_2299_uncompressed_stream_memory --profile bench -- --nocapture
//! ```

#![cfg(all(feature = "write-support", feature = "dhat-heap"))]

use std::collections::HashMap;
use std::time::Duration;

use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::{
    CellOperation, ClusteringKey, Mutation, PartitionKey, STCSPolicy, TableId, WriteEngine,
    WriteEngineConfig,
};
use cqlite_core::types::Value;
use tempfile::TempDir;

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

const HEAP_BUDGET_BYTES: usize = 128 * 1024 * 1024;
const KEYSPACE: &str = "issue2299_mem_ks";
const TABLE: &str = "wide_partition";

// 4 files x 600 rows x 64 KiB payload ~= 150 MiB of combined row content in
// ONE partition (id = 1), split across the 4 files by disjoint clustering-key
// ranges so a real k-way merge reconciles them into one wide output
// partition. Zero tombstones/deletes.
const PAYLOAD_BYTES: usize = 64 * 1024;
const ROWS_PER_SSTABLE: i32 = 600;
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
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: vec![
            Column {
                name: "id".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "ck".to_string(),
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
        dropped_columns: HashMap::new(),
    }
}

fn payload_for(ck: i32) -> String {
    let mut s = format!("row-{ck:08}-");
    s.push_str(&"abcdefghij".repeat((PAYLOAD_BYTES.saturating_sub(s.len())) / 10 + 1));
    s.truncate(PAYLOAD_BYTES);
    s
}

fn write_row(ck: i32, timestamp: i64) -> Mutation {
    let table_id = TableId::new(KEYSPACE, TABLE);
    let pk = PartitionKey::single("id", Value::Integer(1));
    let ck_key = ClusteringKey::single("ck", Value::Integer(ck));
    let ops = vec![CellOperation::Write {
        column: "payload".to_string(),
        value: Value::Text(payload_for(ck)),
    }];
    Mutation::new(table_id, pk, Some(ck_key), ops, timestamp, None)
}

fn make_policy() -> STCSPolicy {
    STCSPolicy::new(SSTABLE_COUNT as usize, 32, 0.5, 1.5, 0).expect("valid STCS parameters")
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

#[test]
fn test_within_partition_streaming_merge_stays_within_heap_budget() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let wal_dir = temp_dir.path().join("wal");
    let schema = make_schema();

    // Phase 1 (UNMEASURED): write SSTABLE_COUNT files for the SAME partition,
    // each covering a disjoint clustering-key range.
    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, schema.clone());
    let mut engine = WriteEngine::new(config).expect("engine creation");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    for file_idx in 0..SSTABLE_COUNT {
        let base = file_idx * ROWS_PER_SSTABLE;
        for ck in base..base + ROWS_PER_SSTABLE {
            engine
                .write(write_row(ck, 100 + i64::from(ck)))
                .expect("write row");
        }
        rt.block_on(engine.flush())
            .expect("flush sstable")
            .expect("non-empty sstable");
    }

    let sstable_dir = data_dir.join(KEYSPACE).join(TABLE);
    assert_eq!(count_data_files(&sstable_dir), SSTABLE_COUNT as usize);

    engine
        .set_merge_policy(Box::new(make_policy()))
        .expect("set merge policy");

    // Phase 2 (MEASURED): run the k-way-merge compaction under dhat.
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
    assert!(compaction_completed);

    let stats = dhat::HeapStats::get();
    eprintln!(
        "issue #2299 uncompressed within-partition streaming merge: peak heap {} bytes \
         ({:.2} MiB) vs budget {} MiB",
        stats.max_bytes,
        stats.max_bytes as f64 / (1024.0 * 1024.0),
        HEAP_BUDGET_BYTES / (1024 * 1024)
    );

    assert_eq!(count_data_files(&sstable_dir), 1);

    // MEASURED RESULT (pre-fix): 424827541 bytes (~424.22 MiB) vs. the 128 MiB budget.
    assert!(
        stats.max_bytes <= HEAP_BUDGET_BYTES,
        "issue #2299: uncompressed within-partition k-way-merge peak heap {} bytes \
         ({:.2} MiB) exceeds the {} MiB budget — the compaction read path materialised \
         the whole wide partition per source",
        stats.max_bytes,
        stats.max_bytes as f64 / (1024.0 * 1024.0),
        HEAP_BUDGET_BYTES / (1024 * 1024)
    );

    rt.block_on(engine.close()).expect("close engine");
    drop(temp_dir);
}
