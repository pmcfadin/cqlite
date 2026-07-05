//! Bounded-worklist regression guard for Issue #1625 (roborev finding 2).
//!
//! The iterative size estimator (`Memtable::estimate_value_size`) fails closed
//! at `MAX_ESTIMATE_NODES` nodes. The finding: the node cap was checked only when
//! POPPING a node, but every container arm enqueued ALL of its children up front
//! (`worklist.extend(...)` / per-entry `push`). A single flat collection with far
//! more than `MAX_ESTIMATE_NODES` elements therefore allocated a reference
//! worklist proportional to its element count BEFORE the estimator failed closed
//! — weakening the DoS guard.
//!
//! The fix checks `visited + pending + incoming` against the cap BEFORE enqueuing
//! any children and returns `usize::MAX` (fail closed) without growing the
//! worklist. This test proves the worklist stays bounded by comparing two inserts
//! that differ ONLY in how many children the estimator would enqueue:
//!
//!   * UNDER-cap flat list (fully enqueued): the estimator grows its worklist to
//!     ~N references (log2(N) reallocations).
//!   * OVER-cap flat list (fails closed at the enqueue check): the estimator
//!     never grows its worklist.
//!
//! `Memtable::insert_with_key` performs NO serialization, so the estimator's
//! worklist is the only per-insert allocation that scales with the collection.
//! The per-insert BTreeMap/Vec/tracing overhead is constant and cancels in the
//! comparison, so the over-cap insert must allocate STRICTLY FEWER times than the
//! under-cap insert. Pre-fix, both grow the worklist and the counts match.
//!
//! This file is its own test binary with exactly one `#[test]`, so the global
//! counter observes allocations only from this test's thread.

#![cfg(feature = "write-support")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use cqlite_core::storage::write_engine::{
    CellOperation, DecoratedKey, Memtable, Mutation, PartitionKey, TableId,
};
use cqlite_core::types::Value;

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

/// Mirror of the private `Memtable::MAX_ESTIMATE_NODES` (1_000_000).
const MAX_ESTIMATE_NODES: usize = 1_000_000;

fn make_mutation(list_len: usize) -> Mutation {
    let value = Value::List((0..list_len as i32).map(Value::Integer).collect());
    let ops = vec![CellOperation::Write {
        column: "big".to_string(),
        value,
    }];
    Mutation::new(
        TableId::new("ks", "t"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        ops,
        1,
        None,
    )
}

/// Insert one mutation into a fresh single-partition memtable and return the
/// number of heap allocations performed inside `insert_with_key`.
fn allocs_for_insert(m: Mutation) -> (usize, usize) {
    // Build everything (key, memtable) BEFORE the counting window so only the
    // work inside `insert_with_key` — including the estimator — is measured.
    let mut memtable = Memtable::new();
    let key = DecoratedKey::from_key_bytes(vec![0, 0, 0, 1]).expect("key");

    COUNTING.store(true, Ordering::Relaxed);
    let baseline = ALLOCS.load(Ordering::Relaxed);
    memtable.insert_with_key(key, m).expect("insert");
    let allocs = ALLOCS.load(Ordering::Relaxed) - baseline;
    COUNTING.store(false, Ordering::Relaxed);

    (allocs, memtable.size_bytes())
}

#[test]
fn estimator_worklist_bounded_at_node_cap() {
    // UNDER cap: the estimator enqueues all ~1M children (worklist grows).
    let (under_allocs, under_size) = allocs_for_insert(make_mutation(MAX_ESTIMATE_NODES - 1));
    // A finite (non-saturated) estimate: the value fit under the cap.
    assert_ne!(
        under_size,
        usize::MAX,
        "under-cap insert should produce a finite ledger size"
    );

    // OVER cap: the estimator fails closed at the enqueue check WITHOUT growing
    // the worklist, so the ledger saturates to usize::MAX.
    let (over_allocs, over_size) = allocs_for_insert(make_mutation(MAX_ESTIMATE_NODES + 5));
    assert_eq!(
        over_size,
        usize::MAX,
        "over-cap insert must fail closed (usize::MAX)"
    );

    // The ONLY allocation difference between the two inserts is the estimator's
    // worklist growth. Failing closed BEFORE enqueuing must allocate strictly
    // fewer times than fully enqueuing ~1M children. Pre-fix these are equal
    // (both grow the worklist); post-fix the over-cap path grows it not at all.
    assert!(
        over_allocs < under_allocs,
        "issue #1625: over-cap insert allocated {over_allocs} times, not fewer \
         than the under-cap insert's {under_allocs} — the estimator is still \
         growing its worklist past the node cap before failing closed"
    );
}
