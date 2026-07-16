//! Allocation regression guard for Issue #1673 (Epic R, finding R2).
//!
//! Before R2, EVERY row written by `DataWriter` allocated **two throwaway
//! `Vec`s**:
//!   1. a per-row body `Vec` (`build_merged_row_body` / `build_static_row_body`
//!      returned a fresh `Vec<u8>`), then copied into `self.buffer`; and
//!   2. a gratuitous size-VInt `Vec` (`row_size_buf`) holding 1–9 bytes, also
//!      copied into `self.buffer`.
//!
//! R2 replaces (1) with a reusable `row_scratch` buffer on the writer (cleared,
//! not reallocated, per row) and (2) with a direct `encode_unsigned(_, &mut
//! self.buffer)`. After warmup neither the row body nor the size VInt allocates.
//!
//! ## How the guard isolates exactly those two allocations
//!
//! Both throwaway buffers are **freshly-empty `Vec<u8>`s**. A `Vec<u8>`'s FIRST
//! growth always allocates `RawVec::MIN_NON_ZERO_CAP == 8` bytes (the minimum
//! non-zero capacity for a 1-byte element type), regardless of the final body
//! size — a body larger than 8 bytes reallocates 8→16→… afterward, but its FIRST
//! allocation is always exactly 8. So on `main` each written row produces two
//! extra 8-byte allocations (body Vec + `row_size_buf`) that R2 removes.
//!
//! After R2 the reused `row_scratch` is already grown past 8 after the first row,
//! and `self.buffer` grows at power-of-two sizes far above 8, so neither the body
//! nor the size VInt allocates 8 bytes per row anymore. The merge path's
//! `HashMap`/`Vec<MergedOp>` allocations are larger than 8 bytes (and, for a
//! pure-PK row, are never created), so they do not pollute the 8-byte count.
//!
//! To cancel one-time warmup + fixed per-writer overhead the guard measures the
//! **slope** of 8-byte allocations across two sizes (the #1046 rigorous form):
//!   * regular path — one partition, M pure-PK-insert clustering rows, M ∈ {64, 256};
//!   * static path  — P static-only partitions (one static row each), P ∈ {32, 128}.
//!
//! Measured (Rust 1.88, this repo):
//!   * regular: `main` slope 576 (≈3 eight-byte allocs/row), post-R2 192 (≈1/row);
//!   * static:  `main` slope 288 (≈3/static-row),            post-R2 96 (≈1/row).
//!
//! Each path retains ONE pre-existing per-row 8-byte allocation that R2 does not
//! touch (an incidental writer-path `Vec<u8>` unrelated to the row body/size — the
//! regular residual is the clustering-prefix comparator path). The R2 signal is
//! the DROP of exactly the two removed buffers: 576→192 (−2/row) on the regular
//! path and 288→96 (−2/row) on the static path. `K` is set at the midpoint of the
//! `main` and post-R2 slopes on each path, so `main` FAILs and post-R2 PASSes with
//! a wide margin, and re-introducing either throwaway buffer trips the guard.
//!
//! Single `#[test]` in its own binary so the process-global counter observes only
//! this thread's allocations (regular then static, measured sequentially).

#![cfg(feature = "write-support")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::writer::SSTableWriter;
use cqlite_core::storage::write_engine::mutation::{
    CellOperation, ClusteringKey, Mutation, PartitionKey, TableId,
};
use cqlite_core::types::Value;
use tempfile::TempDir;

struct CountingAlloc;

/// Total allocations in the counting window.
static ALLOCS: AtomicUsize = AtomicUsize::new(0);
/// Allocations/reallocations of EXACTLY 8 bytes — a freshly-empty `Vec<u8>`'s
/// first growth. On `main` this includes the per-row body Vec + `row_size_buf`
/// pair (see module docs); R2 removes both, dropping the per-row count by 2.
static ALLOCS_8B: AtomicUsize = AtomicUsize::new(0);
static COUNTING: AtomicBool = AtomicBool::new(false);

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
            if layout.size() == 8 {
                ALLOCS_8B.fetch_add(1, Ordering::Relaxed);
            }
        }
        System.alloc(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
            if new_size == 8 {
                ALLOCS_8B.fetch_add(1, Ordering::Relaxed);
            }
        }
        System.realloc(ptr, layout, new_size)
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

const KEYSPACE: &str = "issue1673_ks";
const TABLE: &str = "t";

/// `pk int` partition key + `ck int` clustering key + one regular `v int`.
fn regular_schema() -> TableSchema {
    TableSchema {
        keyspace: KEYSPACE.to_string(),
        table: TABLE.to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
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
                name: "pk".to_string(),
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
                name: "v".to_string(),
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

/// `pk int` partition key + one static `s int` (no clustering).
fn static_schema() -> TableSchema {
    TableSchema {
        keyspace: KEYSPACE.to_string(),
        table: TABLE.to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "s".to_string(),
                data_type: "int".to_string(),
                nullable: true,
                default: None,
                is_static: true,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// Count total + 8-byte allocations while writing ONE partition of `m`
/// pure-primary-key-insert clustering rows (empty ops → a row marker, no cells,
/// tiny body). Everything is built before the counting window.
fn regular_allocs(m: i32) -> (usize, usize) {
    let dir = TempDir::new().expect("temp dir");
    let schema = regular_schema();
    let mut writer = SSTableWriter::new(dir.path().to_path_buf(), 1, &schema).expect("writer");

    let mutations: Vec<Mutation> = (0..m)
        .map(|i| {
            Mutation::new(
                TableId::new(KEYSPACE, TABLE),
                PartitionKey::single("pk", Value::Integer(1)),
                Some(ClusteringKey::single("ck", Value::Integer(i))),
                Vec::new(), // pure-PK insert: row marker only, no cells
                1_000_000,
                None,
            )
        })
        .collect();
    let key = mutations[0].decorated_key(&schema).expect("decorated key");

    let total_base = ALLOCS.load(Ordering::Relaxed);
    let eight_base = ALLOCS_8B.load(Ordering::Relaxed);
    COUNTING.store(true, Ordering::Relaxed);
    writer.write_partition(key, mutations).expect("write");
    COUNTING.store(false, Ordering::Relaxed);

    (
        ALLOCS.load(Ordering::Relaxed) - total_base,
        ALLOCS_8B.load(Ordering::Relaxed) - eight_base,
    )
}

/// Count total + 8-byte allocations while writing `p` static-only partitions
/// (one static row per partition, no clustering rows). Everything is built
/// before the counting window.
fn static_allocs(p: i32) -> (usize, usize) {
    let dir = TempDir::new().expect("temp dir");
    let schema = static_schema();
    let mut writer = SSTableWriter::new(dir.path().to_path_buf(), 1, &schema).expect("writer");

    let mut partitions: Vec<(_, Vec<Mutation>)> = (0..p)
        .map(|i| {
            let mutation = Mutation::new(
                TableId::new(KEYSPACE, TABLE),
                PartitionKey::single("pk", Value::Integer(i)),
                None,
                vec![CellOperation::Write {
                    column: "s".to_string(),
                    value: Value::Integer(i),
                }],
                1_000_000,
                None,
            );
            let key = mutation.decorated_key(&schema).expect("decorated key");
            (key, vec![mutation])
        })
        .collect();
    // `write_partition` requires partitions in token order.
    partitions.sort_by(|a, b| a.0.cmp(&b.0));

    let total_base = ALLOCS.load(Ordering::Relaxed);
    let eight_base = ALLOCS_8B.load(Ordering::Relaxed);
    COUNTING.store(true, Ordering::Relaxed);
    for (key, mutations) in partitions {
        writer.write_partition(key, mutations).expect("write");
    }
    COUNTING.store(false, Ordering::Relaxed);

    (
        ALLOCS.load(Ordering::Relaxed) - total_base,
        ALLOCS_8B.load(Ordering::Relaxed) - eight_base,
    )
}

#[test]
fn row_scratch_kills_per_row_body_and_size_allocs() {
    // ---- Regular clustering-row path ----
    const M1: i32 = 64;
    const M2: i32 = 256;
    let (reg_total_1, reg_8b_1) = regular_allocs(M1);
    let (_reg_total_2, reg_8b_2) = regular_allocs(M2);
    let reg_slope_8b = reg_8b_2.saturating_sub(reg_8b_1);

    eprintln!(
        "#1673 regular: M1={M1} 8b={reg_8b_1} total={reg_total_1} | M2={M2} 8b={reg_8b_2} | \
         8b-slope={reg_slope_8b}"
    );

    // Non-vacuous: writing rows must actually allocate SOMETHING.
    assert!(
        reg_total_1 > 0,
        "#1673: writing {M1} clustering rows allocated nothing — measurement is vacuous"
    );

    // Each row allocates a body Vec + a `row_size_buf` (two 8-byte first-growths)
    // on `main` PLUS one pre-existing per-row 8-byte alloc R2 does not touch, so
    // the measured slope is ~576 (3/row). R2 removes the two throwaway buffers,
    // dropping it to ~192 (1/row). K is the midpoint (384): `main` (576) FAILs,
    // post-R2 (192) PASSes, each with a ~192 margin.
    const K_REGULAR: usize = 384;
    assert!(
        reg_slope_8b <= K_REGULAR,
        "#1673: regular-row 8-byte alloc slope regressed — {M1}->{M2} rows added \
         {reg_slope_8b} eight-byte allocations (> K={K_REGULAR}). `main` measures ~576 \
         (per-row body Vec + row_size_buf + 1 pre-existing); after R2 the reusable \
         row_scratch + in-place size VInt remove the two throwaway buffers (~192)."
    );

    // ---- Static-row path (companion) ----
    const P1: i32 = 32;
    const P2: i32 = 128;
    let (stat_total_1, stat_8b_1) = static_allocs(P1);
    let (_stat_total_2, stat_8b_2) = static_allocs(P2);
    let stat_slope_8b = stat_8b_2.saturating_sub(stat_8b_1);

    eprintln!(
        "#1673 static: P1={P1} 8b={stat_8b_1} total={stat_total_1} | P2={P2} 8b={stat_8b_2} | \
         8b-slope={stat_slope_8b}"
    );

    assert!(
        stat_total_1 > 0,
        "#1673: writing {P1} static partitions allocated nothing — measurement is vacuous"
    );

    // Same shape as the regular path: `main` measures ~288 (3/static-row: body
    // Vec + row_size_buf + 1 pre-existing); R2 drops it to ~96 (1/row). K is the
    // midpoint (192): `main` (288) FAILs, post-R2 (96) PASSes, ~96 margin each.
    const K_STATIC: usize = 192;
    assert!(
        stat_slope_8b <= K_STATIC,
        "#1673: static-row 8-byte alloc slope regressed — {P1}->{P2} partitions added \
         {stat_slope_8b} eight-byte allocations (> K={K_STATIC}). `main` measures ~288 \
         (per-static-row body Vec + row_size_buf + 1 pre-existing); after R2 the reusable \
         row_scratch + in-place size VInt remove the two throwaway buffers (~96)."
    );
}
