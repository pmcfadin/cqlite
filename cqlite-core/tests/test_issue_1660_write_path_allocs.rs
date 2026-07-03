//! Allocation-count regression guard for Issue #1660 (write-path perf audit).
//!
//! Two steady-state `WriteEngine::write()` allocations were removable without
//! changing any on-disk bytes:
//!
//!   (a) `WriteAheadLog::append` allocated a fresh `Vec<u8>` per call via
//!       `bincode::serialize`. The fix serializes into a reusable
//!       `append_scratch` buffer (`serialize_into`, byte-identical output), so
//!       steady-state appends allocate zero serialization buffers.
//!   (b) `PartitionKey::to_bytes` for a single-component key allocated an empty
//!       `result` Vec and then copied the serialized value into it. The fix
//!       returns the serialized value Vec directly (one fewer alloc + copy),
//!       leaving the on-disk encoding byte-identical.
//!
//! This test installs a process-global counting allocator and measures the
//! number of heap allocations performed *inside* a batch of steady-state
//! `write()` calls (the engine, schema, and every mutation are all constructed
//! before the counting window opens, and a warm-up phase primes the WAL scratch
//! buffer and the memtable's per-partition Vec so amortized growth is not
//! attributed to the measured window).
//!
//! All writes target a SINGLE single-component TEXT partition key so the
//! per-write allocations under test (WAL serialize + PK `to_bytes` `result` Vec)
//! are isolated from BTreeMap node-split churn (a distinct key per write would
//! allocate a node/Vec and mask the signal). Measured on this workload:
//!
//!   * before this fix: ~5 allocations per `write()`
//!     WAL `bincode::serialize` (1) + PK `result` Vec (1) + PK value Vec (1) +
//!     `CqlType::parse` inside `serialize_value` (~2)
//!   * after this fix:  ~3 allocations per `write()` — the WAL serialize buffer
//!     and the PK `result` Vec are both gone.
//!
//! The residual ~3/write (PK value serialization + the type-string parse in
//! `serialize_value`) is OUT OF SCOPE for #1660. The guard therefore asserts
//! `< 4` allocations per write (`<= 7 * BATCH / 2`): `main` (~5/write) FAILS and
//! the post-fix path (~3/write) PASSES with margin, and re-introducing EITHER
//! removed allocation (→ ~4/write) trips the guard.
//!
//! This file is its own test binary with exactly one `#[test]`, so the global
//! counter observes allocations only from this test's thread.

#![cfg(feature = "write-support")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use std::collections::HashMap;
use tempfile::TempDir;

/// Counts every allocation (not deallocation) while `COUNTING` is enabled, so
/// the measurement window is scoped exactly to the batch of `write()` calls.
struct CountingAlloc;

static ALLOCS: AtomicUsize = AtomicUsize::new(0);
static COUNTING: AtomicBool = AtomicBool::new(false);

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        System.alloc(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        System.realloc(ptr, layout, new_size)
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

const KEYSPACE: &str = "issue1660_ks";
const TABLE: &str = "t";
const PK: &str = "the-one-partition-key";

fn make_schema() -> TableSchema {
    TableSchema {
        keyspace: KEYSPACE.to_string(),
        table: TABLE.to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "text".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "id".to_string(),
                data_type: "text".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "val".to_string(),
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

fn build_mutation(seq: i64) -> Mutation {
    let table_id = TableId::new(KEYSPACE, TABLE);
    // Same single-component partition key on every write (see module docs).
    let pk = PartitionKey::single("id", Value::Text(PK.to_string()));
    let ops = vec![CellOperation::Write {
        column: "val".to_string(),
        value: Value::Text(format!("value-{seq:08}")),
    }];
    Mutation::new(table_id, pk, None, ops, 1_000 + seq, None)
}

#[test]
fn write_path_alloc_budget_holds() {
    // Warm-up count is chosen to sit JUST PAST a memtable per-partition `Vec`
    // doubling boundary (all writes share one partition key, so its `Vec`
    // capacity doubles 1,2,4,…,128,256). After 130 pushes the capacity is 256,
    // so the 64-write measured window (pushes 131..194) triggers no reallocation
    // and the count is deterministic.
    const WARMUP: i64 = 130;
    const BATCH: usize = 64;

    let temp_dir = TempDir::new().expect("temp dir");
    let data_dir = temp_dir.path().join("data");
    let wal_dir = temp_dir.path().join("wal");
    let schema = make_schema();

    let config = WriteEngineConfig::new(data_dir, wal_dir, schema);
    let mut engine = WriteEngine::new(config).expect("engine creation");

    // Build every mutation BEFORE the counting window so only the work performed
    // inside `write()` is measured.
    let total = WARMUP as usize + BATCH;
    let mutations: Vec<Mutation> = (0..total as i64).map(build_mutation).collect();
    let mut it = mutations.into_iter();

    // Warm-up (unmeasured): prime the WAL scratch buffer to its steady-state
    // capacity and grow the memtable's per-partition Vec past a doubling
    // boundary so the measured window does not pay one-time growth costs.
    for _ in 0..WARMUP {
        engine
            .write(it.next().expect("warmup mutation"))
            .expect("warmup write");
    }

    // ── counting window ──────────────────────────────────────────────────────
    COUNTING.store(true, Ordering::Relaxed);
    let baseline = ALLOCS.load(Ordering::Relaxed);
    for _ in 0..BATCH {
        engine
            .write(it.next().expect("measured mutation"))
            .expect("measured write");
    }
    let allocs = ALLOCS.load(Ordering::Relaxed) - baseline;
    COUNTING.store(false, Ordering::Relaxed);
    // ─────────────────────────────────────────────────────────────────────────

    // "< 4 allocations per write": before the fix each write costs ~5 allocs
    // (~5 * BATCH), after the fix ~3 (~3 * BATCH). The 3.5/write budget fails on
    // `main` and passes after, tripping if either removed allocation returns.
    let budget = (7 * BATCH) / 2;
    assert!(
        allocs <= budget,
        "Issue #1660: {BATCH} steady-state write()s allocated {allocs} times \
         (> {budget}, i.e. >= 4/write) — WAL serialize scratch reuse or the \
         single-component PK to_bytes fast path regressed"
    );
}
