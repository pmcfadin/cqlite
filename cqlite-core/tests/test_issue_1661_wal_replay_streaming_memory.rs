//! Memory-bound regression test for Issue #1661 (dhat-gated) — THE deliverable.
//!
//! **Problem**: `WriteAheadLog::replay()` decoded EVERY WAL entry into a single
//! `Vec<Mutation>` (the whole log) and returned it; crash recovery in
//! `WriteEngine::new` then iterated that Vec to build the memtable. The memtable
//! *moves* each mutation in (it does not copy the payload heap), so the resident
//! payload is not literally doubled — but the whole-log Vec's M-slot backing
//! buffer stays allocated for the entire drain loop *on top of* the growing
//! memtable, so end-to-end reopen peak carries an extra `M × size_of::<Mutation>`
//! allocation above the memtable's own resident size.
//!
//! **Fix**: `WriteAheadLog::replay_each` streams one decoded mutation at a time
//! to a callback (reusing a single read buffer), and recovery inserts each
//! straight into the memtable. No whole-log Vec is ever materialised, so reopen
//! peak is bounded by the memtable itself plus a small constant.
//!
//! **Observed** (deterministic, dhat `testing()` mode; 60_000 × 256 B):
//! pre-change reopen peak 114.66 MiB, post-change 98.16 MiB — a reproducible
//! 16.5 MiB reduction (the eliminated whole-log Vec backing buffer). The budget
//! below sits in that gap so the streaming path passes and the whole-log path
//! fails.
//!
//! **This test** writes M mutations to a WAL (no flush, so they persist for
//! replay), drops the engine, then reopens it under the dhat heap profiler so
//! only the crash-recovery replay is attributed. It asserts the reopen peak
//! heap stays within a budget that the streaming path meets but the old
//! whole-log path exceeds.
//!
//! Gated on the `dhat-heap` feature; runs in the profiling job, not normal
//! `cargo test`. Run via:
//!
//! ```text
//! cargo test --package cqlite-core \
//!   --features write-support,cli-helpers,dhat-heap \
//!   --test test_issue_1661_wal_replay_streaming_memory --profile bench -- --nocapture
//! ```

#![cfg(all(
    feature = "write-support",
    feature = "cli-helpers",
    feature = "dhat-heap"
))]

use std::collections::HashMap;

use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use tempfile::TempDir;

// The dhat allocator must be the global allocator to observe every allocation.
// This test binary is separate from all others, so installing it here does not
// affect normal builds or other test binaries.
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

const KEYSPACE: &str = "issue1661_mem_ks";
const TABLE: &str = "blobs";

// Workload sizing. A large mutation count makes the eliminated whole-log Vec
// backing buffer (`M × size_of::<Mutation>`) a clearly measurable slice of the
// reopen peak, well above any constant overhead. The payload size is modest so
// the memtable resident set stays bounded while the count drives the signal.
const PAYLOAD_BYTES: usize = 256;
const MUTATION_COUNT: i32 = 60_000;

// Budget: sits in the deterministic gap between the streaming reopen peak
// (~98.16 MiB, PASS) and the pre-fix whole-log peak (~114.66 MiB, FAIL). dhat's
// `testing()` mode makes the measurement reproducible, so an ~8 MiB margin on
// each side is safe.
const HEAP_BUDGET_BYTES: usize = 106 * 1024 * 1024;

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
        dropped_columns: HashMap::new(),
    }
}

fn payload_for(id: i32) -> String {
    let mut s = format!("row-{id:08}-");
    s.push_str(&"abcdefghij".repeat(PAYLOAD_BYTES / 10 + 1));
    s.truncate(PAYLOAD_BYTES);
    s
}

fn write_row(id: i32) -> Mutation {
    let table_id = TableId::new(KEYSPACE, TABLE);
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ops = vec![CellOperation::Write {
        column: "payload".to_string(),
        value: Value::Text(payload_for(id)),
    }];
    Mutation::new(table_id, pk, None, ops, 100 + id as i64, None)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_wal_replay_streaming_stays_within_heap_budget() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let wal_dir = temp_dir.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();
    let schema = make_schema();

    // ── Phase 1 (UNMEASURED): fill the WAL, do NOT flush ──────────────────────
    // A high flush threshold + running inside the Tokio runtime guarantees
    // `write()` never auto-flushes, so every mutation stays in the WAL for
    // replay. Dropping the engine discards the memtable but leaves the WAL on
    // disk as the recovery source.
    {
        let config = WriteEngineConfig::new(data_dir.clone(), wal_dir.clone(), schema.clone())
            .with_flush_threshold(usize::MAX);
        let mut engine = WriteEngine::new(config).expect("engine creation");
        for id in 0..MUTATION_COUNT {
            engine.write(write_row(id)).expect("write row");
        }
        // Drop without flush: the WAL retains all MUTATION_COUNT entries.
        drop(engine);
    }

    // ── Phase 2 (MEASURED): reopen, which drives WAL crash-recovery replay ────
    let _profiler = dhat::Profiler::builder().testing().build();

    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir.clone(), schema.clone())
        .with_flush_threshold(usize::MAX);
    let engine = WriteEngine::new(config).expect("engine reopen (drives WAL replay)");

    let stats = dhat::HeapStats::get();
    eprintln!(
        "Issue #1661 WAL replay streaming: {MUTATION_COUNT} mutations × {PAYLOAD_BYTES} B payload, \
         reopen peak heap {} bytes ({:.2} MiB) vs budget {} MiB",
        stats.max_bytes,
        stats.max_bytes as f64 / (1024.0 * 1024.0),
        HEAP_BUDGET_BYTES / (1024 * 1024),
    );

    // Correctness guard: the reopened memtable must hold every replayed row.
    assert_eq!(
        engine.memtable_row_count(),
        MUTATION_COUNT as usize,
        "Issue #1661: replay must recover every WAL entry into the memtable"
    );

    assert!(
        stats.max_bytes <= HEAP_BUDGET_BYTES,
        "Issue #1661: WAL-replay reopen peak heap {} bytes ({:.2} MiB) exceeds the {} MiB budget — \
         recovery regressed to materialising the whole log as a Vec<Mutation>",
        stats.max_bytes,
        stats.max_bytes as f64 / (1024.0 * 1024.0),
        HEAP_BUDGET_BYTES / (1024 * 1024),
    );

    drop(temp_dir);
}
