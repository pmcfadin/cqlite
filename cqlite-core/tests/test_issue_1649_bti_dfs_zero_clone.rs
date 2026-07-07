//! Issue #1649 (L2): zero-clone BTI DFS — reusable path buffer + visitor.
//!
//! The BTI in-order DFS core ([`dfs_visit_in_order`]) hands each emitted node's
//! reconstructed key to the visitor as a **borrowed slice** into a single reusable
//! path buffer. Owned-key callers pay one `to_vec()` **per emitted result**
//! ([`iterate_partitions_in_bti_file`]); offset-only callers
//! ([`iterate_partition_locations_in_bti_file`]) drop the key and allocate
//! **nothing per node**.
//!
//! This file installs a process-global counting allocator and proves, over a
//! synthetic 500-leaf `Partitions.db` trie, that:
//!
//!   * the OWNED path allocates ~one key `Vec` per result (O(results)); and
//!   * the OFFSET-ONLY path allocates a small CONSTANT independent of the leaf
//!     count (only the trie load, the visited bitset, and log-bounded growth of
//!     the stack + locations `Vec`).
//!
//! It also asserts byte-identical equivalence between the two paths (identical
//! ordered `BtiPartitionLocation` sequences) on the synthetic trie AND on the
//! real `test_da` `Partitions.db` fixtures.
//!
//! `#[global_allocator]` counts allocations from **every** thread in the
//! process, not just the current test's. This file has two `#[test]`s, and
//! under the default multi-threaded test runner they can run concurrently in
//! the SAME process — so the second test's file I/O / Vec growth could
//! allocate inside the first test's counting window and inflate
//! `offset_allocs`, flaking the `<= MAX_OFFSET_ONLY_ALLOCS` bound. `TEST_LOCK`
//! serializes the two test bodies (each holds it for its FULL body, not just
//! the counting window) so the measurement is race-free by construction, not
//! by luck.

use std::alloc::{GlobalAlloc, Layout, System};
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use cqlite_core::storage::sstable::bti::{
    iterate_partition_locations_in_bti_file, iterate_partitions_in_bti_file, BtiPartitionLocation,
};

/// Counts every allocation (not deallocation) over a tightly scoped window.
struct CountingAlloc;

static ALLOCS: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        System.alloc(layout)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        System.realloc(ptr, layout, new_size)
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

/// Serializes the two `#[test]`s in this file so neither's allocations can
/// pollute the other's counting window (see the module doc comment). Poisoned
/// (panicked-while-held) is treated as still-lockable — a prior test's panic
/// must not spuriously fail an unrelated test via lock poisoning.
static TEST_LOCK: Mutex<()> = Mutex::new(());

/// A `Partitions.db` PayloadOnly leaf: `[0x08(payloadBits=8), hash, position]`.
/// `position = -1` (0xFF) → `DataOffset(0)`; a `PayloadOnly` leaf has no children,
/// so it contributes ZERO heap allocations to the DFS.
fn leaf() -> [u8; 3] {
    [0x08, 0x00, (-1i8) as u8]
}

/// Build a two-level wide `Partitions.db` trie:
///   root Sparse16 → 2 mid Sparse16 nodes → `leaves_per_branch` PayloadOnly leaves.
///
/// Total nodes = `1 + 2 + 2*leaves_per_branch`; total emitted results =
/// `2*leaves_per_branch` (only the leaves carry a payload). Returns the complete
/// in-memory file (trie bytes + 8-byte big-endian root-offset footer).
///
/// Sparse16 (ordinal 7) uses 2-byte backward deltas (reach ≤ 65535) so a branch's
/// far leaves are reachable; the count byte caps a branch at 255 leaves.
fn build_wide_partitions_db(leaves_per_branch: usize) -> Vec<u8> {
    assert!(
        (1..=255).contains(&leaves_per_branch),
        "Sparse16 count is one byte (1..=255)"
    );

    let mut trie: Vec<u8> = Vec::new();

    // Emit one branch: `leaves_per_branch` leaves followed by a Sparse16 mid node
    // pointing back at each. Returns the mid node's absolute offset.
    let emit_branch = |trie: &mut Vec<u8>| -> usize {
        let mut leaf_offsets = Vec::with_capacity(leaves_per_branch);
        for _ in 0..leaves_per_branch {
            leaf_offsets.push(trie.len());
            trie.extend_from_slice(&leaf());
        }
        let mid_off = trie.len();
        // Sparse16 header + count.
        trie.push(0x70);
        trie.push(leaves_per_branch as u8);
        // Transition bytes: distinct, ascending 0..leaves_per_branch (≤ 255).
        for i in 0..leaves_per_branch {
            trie.push(i as u8);
        }
        // Backward 2-byte deltas to each leaf.
        for &leaf_off in &leaf_offsets {
            let delta = (mid_off - leaf_off) as u16;
            trie.extend_from_slice(&delta.to_be_bytes());
        }
        mid_off
    };

    let mid_a = emit_branch(&mut trie);
    let mid_b = emit_branch(&mut trie);

    // Root Sparse16 with two children (mid_a via 0x01, mid_b via 0x02).
    let root_off = trie.len();
    trie.push(0x70);
    trie.push(0x02);
    trie.push(0x01);
    trie.push(0x02);
    trie.extend_from_slice(&((root_off - mid_a) as u16).to_be_bytes());
    trie.extend_from_slice(&((root_off - mid_b) as u16).to_be_bytes());

    // 8-byte big-endian root-offset footer.
    trie.extend_from_slice(&(root_off as u64).to_be_bytes());
    trie
}

/// Upper bound on allocations for the OFFSET-ONLY DFS over the synthetic trie:
/// trie load + visited bitset + a few Sparse `parse`/`ordered_children` Vecs +
/// log-bounded growth of the DFS stack and the locations Vec. Crucially this does
/// NOT scale with the 500 leaves. On `main` (no offset-only path, forced through
/// the owned collector) this window allocates ~one key Vec per leaf (~500+),
/// blowing this bound — the red→green guard.
const MAX_OFFSET_ONLY_ALLOCS: usize = 64;

#[test]
fn offset_only_dfs_allocates_constant_not_per_leaf() {
    // Hold for the WHOLE body: the counting window below must not race against
    // the other test in this file (see module doc comment).
    let _guard = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());

    const LEAVES_PER_BRANCH: usize = 250; // 500 leaves, 503 nodes total.
    const RESULTS: usize = 2 * LEAVES_PER_BRANCH;

    let file = build_wide_partitions_db(LEAVES_PER_BRANCH);

    // ── OWNED path (baseline: O(results) key allocations) ────────────────────
    // A fresh Cursor is built before each window so only the enumeration allocs
    // are measured.
    let mut owned_cursor = Cursor::new(file.clone());
    let owned_baseline = ALLOCS.load(Ordering::Relaxed);
    let entries = iterate_partitions_in_bti_file(&mut owned_cursor)
        .expect("owned Partitions.db traversal must succeed");
    let owned_allocs = ALLOCS.load(Ordering::Relaxed) - owned_baseline;

    // ── OFFSET-ONLY path (the fix: constant allocations) ─────────────────────
    let mut offset_cursor = Cursor::new(file);
    let offset_baseline = ALLOCS.load(Ordering::Relaxed);
    let locations = iterate_partition_locations_in_bti_file(&mut offset_cursor)
        .expect("offset-only Partitions.db traversal must succeed");
    let offset_allocs = ALLOCS.load(Ordering::Relaxed) - offset_baseline;
    // ─────────────────────────────────────────────────────────────────────────

    assert_eq!(entries.len(), RESULTS, "owned path must emit every leaf");
    assert_eq!(
        locations.len(),
        RESULTS,
        "offset-only path must emit every leaf"
    );

    // The owned path pays ~one key `Vec` per result — it scales with the leaf
    // count. This documents the O(results) baseline the fix improves upon.
    assert!(
        owned_allocs >= RESULTS,
        "owned path is expected to allocate >= {RESULTS} (one key Vec per result); got {owned_allocs}"
    );

    // The fix: the offset-only path allocates a small CONSTANT, independent of the
    // 500 leaves. On `main` this is forced through the owned collector and would
    // allocate ~{RESULTS}+, failing this bound.
    assert!(
        offset_allocs <= MAX_OFFSET_ONLY_ALLOCS,
        "offset-only DFS over {RESULTS} leaves must allocate <= {MAX_OFFSET_ONLY_ALLOCS} \
         (O(results) key clones regressed); got {offset_allocs}"
    );

    // The improvement is a large multiple, not a constant-factor tweak.
    assert!(
        offset_allocs * 4 < owned_allocs,
        "offset-only ({offset_allocs}) must be far below owned ({owned_allocs})"
    );

    // Byte-identical equivalence on the synthetic trie: same ordered locations.
    let owned_locations: Vec<BtiPartitionLocation> =
        entries.into_iter().map(|(_key, loc)| loc).collect();
    assert_eq!(
        owned_locations, locations,
        "offset-only and owned DFS must yield identical ordered location sequences"
    );
}

/// Byte-identical equivalence on REAL `test_da` `Partitions.db` fixtures: the
/// offset-only path yields exactly the owned path's ordered locations.
///
/// Guarded by `CQLITE_DATASETS_ROOT`; skips cleanly when the binary fixtures are
/// absent so it never blocks CI running without test data.
#[test]
fn offset_only_matches_owned_on_real_test_da_fixtures() {
    // Hold for the WHOLE body: this test's own allocations (file reads, Vec
    // growth) must not run concurrently with the other test's counting window
    // (see module doc comment). This test does not itself measure allocation
    // counts, so simply excluding overlap with the other test's window suffices.
    let _guard = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());

    let Some(root) = std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
    else {
        eprintln!("SKIP: CQLITE_DATASETS_ROOT not set; needs real BTI fixtures");
        return;
    };

    let rels = [
        "sstables/test_da/simple_table-de1be8b064e711f19ad401a8c8227b11/da-2-bti-Partitions.db",
        "sstables/test_da/collection_table-de2c155064e711f19ad401a8c8227b11/da-2-bti-Partitions.db",
        "sstables/test_da/wide_table-9099a7c06c1811f19864870fb8444786/da-2-bti-Partitions.db",
    ];

    let mut checked = 0usize;
    for rel in rels {
        let path = root.join(rel);
        let Ok(bytes) = std::fs::read(&path) else {
            eprintln!("SKIP: BTI fixture not found at {path:?}");
            continue;
        };

        let owned = iterate_partitions_in_bti_file(&mut Cursor::new(bytes.clone()))
            .expect("owned Partitions.db traversal must succeed");
        let offset_only = iterate_partition_locations_in_bti_file(&mut Cursor::new(bytes))
            .expect("offset-only Partitions.db traversal must succeed");

        let owned_locations: Vec<BtiPartitionLocation> =
            owned.into_iter().map(|(_key, loc)| loc).collect();
        assert_eq!(
            owned_locations, offset_only,
            "offset-only and owned DFS must match on {rel}"
        );
        assert!(!offset_only.is_empty(), "{rel} must yield >= 1 partition");
        checked += 1;
    }

    if checked == 0 {
        eprintln!("SKIP: no test_da Partitions.db fixtures were present");
    }
}
