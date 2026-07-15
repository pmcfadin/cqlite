//! Allocation-slope regression guard for Issue #1672 (Epic R, finding R1).
//!
//! Before R1, `serialize_value` allocated a **fresh `Vec<u8>` for every cell
//! value** and returned it by value; the writer then copied those bytes a second
//! time into the row buffer. For a 4-byte `int` that is 1 heap alloc + 2 memcpys
//! to move 4 bytes, once per cell, per row, per partition — the hottest write
//! loop. R1 adds `serialize_value_into(&Value, &mut Vec<u8>)` and routes the
//! fixed-width scalar cell path (int/bigint/float/uuid/timestamp/bool — the
//! types WITHOUT a length prefix) straight into the row buffer, so an `int` cell
//! allocates zero throwaway `Vec`s.
//!
//! This test measures the per-`int`-column allocation SLOPE of `write_partition`
//! (the #1046 rigorous form of "independent of N"): it counts heap allocations
//! while writing ONE partition / ONE row carrying N `int` columns, at two widths
//! (N1=16 and N2=64), into fresh temp dirs, with the schema, writer, decorated
//! key and mutation ALL constructed before the counting window. Subtracting the
//! two counts cancels the fixed `write_partition` overhead and leaves only the
//! N-scaling cost.
//!
//! * On `main`: each of the (N2-N1)=48 extra int cells allocates one
//!   `serialize_value` `Vec`, so the delta grows by >= 48.
//! * After R1: the fixed-width int path writes straight into the row buffer, so
//!   the delta collapses to a small constant (a handful of buffer-doubling
//!   reallocs as the row buffer grows with more columns).
//!
//! The guard asserts `delta(N2) - delta(N1) <= K` with K calibrated from the
//! post-fix run plus headroom. `main` (delta >= 48) FAILS; the post-fix path
//! PASSES; re-introducing the per-cell Vec trips it. A non-vacuous check asserts
//! the narrow (N1) window actually allocated, so a writer that silently no-ops
//! cannot pass.
//!
//! This file is its own test binary with a single `#[test]`, so the process-
//! global counter observes allocations only from this test's thread.

#![cfg(feature = "write-support")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::writer::SSTableWriter;
use cqlite_core::storage::write_engine::mutation::{CellOperation, Mutation, PartitionKey, TableId};
use cqlite_core::types::Value;
use tempfile::TempDir;

struct CountingAlloc;

/// Total allocations in the counting window.
static ALLOCS: AtomicUsize = AtomicUsize::new(0);
/// Allocations of EXACTLY 4 bytes in the counting window. `serialize_value` for
/// `Value::Integer` does `n.to_be_bytes().to_vec()` — a 4-byte heap allocation
/// per int cell. The fixed-width R1 path writes those 4 bytes straight into the
/// row buffer via `extend_from_slice` (which reallocs the buffer at power-of-two
/// sizes, never 4), so this counter isolates the per-int-cell Vec precisely.
static ALLOCS_4B: AtomicUsize = AtomicUsize::new(0);
static COUNTING: AtomicBool = AtomicBool::new(false);

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
            if layout.size() == 4 {
                ALLOCS_4B.fetch_add(1, Ordering::Relaxed);
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
            if new_size == 4 {
                ALLOCS_4B.fetch_add(1, Ordering::Relaxed);
            }
        }
        System.realloc(ptr, layout, new_size)
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

const KEYSPACE: &str = "issue1672_ks";
const TABLE: &str = "t";

/// `pk int` partition key plus `n` regular `int` columns `c0..c{n-1}`.
fn make_schema(n: usize) -> TableSchema {
    let mut columns = vec![Column {
        name: "pk".to_string(),
        data_type: "int".to_string(),
        nullable: false,
        default: None,
        is_static: false,
    }];
    for i in 0..n {
        columns.push(Column {
            name: format!("c{i}"),
            data_type: "int".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        });
    }
    TableSchema {
        keyspace: KEYSPACE.to_string(),
        table: TABLE.to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns,
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// A single mutation writing `Value::Integer` into every `c{i}` column.
fn make_mutation(n: usize) -> Mutation {
    let ops = (0..n)
        .map(|i| CellOperation::Write {
            column: format!("c{i}"),
            value: Value::Integer(i as i32),
        })
        .collect();
    Mutation::new(
        TableId::new(KEYSPACE, TABLE),
        PartitionKey::single("pk", Value::Integer(1)),
        None,
        ops,
        1_000_000,
        None,
    )
}

/// Count heap allocations performed *inside* one `write_partition` for a row of
/// `n` int columns. Everything (schema, writer, decorated key, mutation) is
/// built before the counting window opens, so only serialization work is
/// measured.
fn allocs_for_width(n: usize) -> (usize, usize) {
    let dir = TempDir::new().expect("temp dir");
    let schema = make_schema(n);
    let mut writer = SSTableWriter::new(dir.path().to_path_buf(), 1, &schema).expect("writer");
    let mutation = make_mutation(n);
    let key = mutation.decorated_key(&schema).expect("decorated key");
    let mutations = vec![mutation];

    let total_base = ALLOCS.load(Ordering::Relaxed);
    let small_base = ALLOCS_4B.load(Ordering::Relaxed);
    COUNTING.store(true, Ordering::Relaxed);
    writer
        .write_partition(key, mutations)
        .expect("write_partition");
    COUNTING.store(false, Ordering::Relaxed);
    let total = ALLOCS.load(Ordering::Relaxed) - total_base;
    let small = ALLOCS_4B.load(Ordering::Relaxed) - small_base;

    (total, small)
}

#[test]
fn serialize_value_into_kills_per_int_cell_alloc() {
    const N1: usize = 16;
    const N2: usize = 64;

    let (narrow_total, narrow_4b) = allocs_for_width(N1);
    let (wide_total, wide_4b) = allocs_for_width(N2);
    let slope_4b = wide_4b.saturating_sub(narrow_4b);

    eprintln!(
        "#1672 alloc window: N1={N1} total={narrow_total} 4b={narrow_4b} | \
         N2={N2} total={wide_total} 4b={wide_4b} | 4b-slope={slope_4b}"
    );

    // Non-vacuous: the writer must actually do work (allocate) writing a row.
    assert!(
        narrow_total > 0,
        "#1672: writing {N1} int columns allocated nothing — the measurement is vacuous"
    );

    // Isolate the per-int-cell serialization Vec via the 4-byte-allocation slope.
    // `serialize_value(Value::Integer)` allocates exactly one 4-byte Vec per int
    // cell (`n.to_be_bytes().to_vec()`), so on `main` the 4-byte-alloc count grows
    // by one per extra column: slope >= (N2-N1) = 48. After R1 the fixed-width int
    // path writes those 4 bytes straight into the row buffer (which reallocs at
    // power-of-two sizes, never exactly 4), so the 4-byte-alloc slope collapses to
    // ~0. K is calibrated from the post-fix run + headroom and sits FAR below the
    // >=48 `main` slope, so re-introducing the per-cell Vec trips the guard.
    const K: usize = 8;
    assert!(
        slope_4b <= K,
        "#1672: per-int-cell 4-byte alloc slope regressed — {N1}->{N2} int columns added \
         {slope_4b} four-byte allocations (> K={K}). On `main` this is >= {} (one throwaway \
         `serialize_value` Vec per int cell); after R1 the fixed-width int path writes straight \
         into the row buffer. (narrow_4b={narrow_4b}, wide_4b={wide_4b})",
        N2 - N1
    );
}
