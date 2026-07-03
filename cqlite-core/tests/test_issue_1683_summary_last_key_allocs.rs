//! Allocation-count regression guard for Issue #1683 (write-path perf audit S6).
//!
//! `SummaryWriter::note_partition` is called once per partition, but only the
//! FINAL `last_key` (the SSTable's highest key) is ever serialized. The previous
//! implementation did `self.last_key = Some(key_bytes.clone())` on every call,
//! allocating a fresh `Vec<u8>` for each of N partitions and discarding all but
//! the last. The fix reuses the single existing buffer in place (`clear` +
//! `extend_from_slice`), so N partitions cost at most a constant number of
//! key-buffer allocations.
//!
//! This test installs a process-global counting allocator and measures the exact
//! number of allocations performed *inside* the `note_partition` loop (all keys
//! and the writer are constructed before the counting window opens). On `main`
//! the count scales with N (~1 alloc per partition, ~1000+); with the fix it is a
//! small constant (first_key clone + one last_key buffer). It also asserts the
//! final serialized `last_key` region is byte-identical to the last partition's
//! key, proving the perf refactor did not change `Summary.db` output.
//!
//! This file is its own test binary containing exactly one `#[test]`, so the
//! global counter observes allocations only from this test's thread — no parallel
//! test pollutes the count.

#![cfg(feature = "write-support")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use cqlite_core::storage::sstable::writer::SummaryWriter;
use cqlite_core::storage::write_engine::mutation::DecoratedKey;

/// Counts every allocation (not deallocation) so the test can measure allocation
/// churn over a tightly scoped window.
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

/// Upper bound on key-buffer allocations for N partitions: the first-key clone
/// plus the single reused last-key buffer. On `main` (fresh clone per call) this
/// window allocates ~N; the fix keeps it to this constant.
const MAX_ALLOCS: usize = 2;

#[test]
fn summary_last_key_allocates_at_most_twice_over_many_partitions() {
    const N: u64 = 1000;

    // Build every input BEFORE opening the counting window so that only the
    // work performed inside `note_partition` is measured. Keys are all the same
    // length (8 bytes) so the reused last-key buffer never has to grow.
    let keys: Vec<DecoratedKey> = (0..N)
        .map(|i| DecoratedKey::new(i as i64, i.to_be_bytes().to_vec()))
        .collect();

    let mut writer = SummaryWriter::new(128);
    // Add one sampled entry (outside the window) so `finish` writes the full
    // first-key/last-key region instead of taking the empty-summary early return.
    writer
        .add_entry(&keys[0], 0)
        .expect("add_entry should succeed");

    // ── counting window ──────────────────────────────────────────────────────
    let baseline = ALLOCS.load(Ordering::Relaxed);
    for key in &keys {
        writer.note_partition(key);
    }
    let allocs = ALLOCS.load(Ordering::Relaxed) - baseline;
    // ─────────────────────────────────────────────────────────────────────────

    assert!(
        allocs <= MAX_ALLOCS,
        "expected <= {MAX_ALLOCS} key-buffer allocations noting {N} partitions, got {allocs} \
         (per-partition last_key clone regressed)"
    );

    // Correctness: the FINAL serialized last_key must be byte-identical to the
    // last partition's key — the perf refactor must not change Summary.db output.
    let last_key_bytes = &keys[(N - 1) as usize].key;
    let bytes = writer.finish().expect("finish should succeed");

    assert!(
        bytes.ends_with(last_key_bytes),
        "Summary.db must end with the last partition's key bytes"
    );
    // The 4 bytes preceding the trailing key are its big-endian length prefix.
    let len_pos = bytes.len() - last_key_bytes.len() - 4;
    let written_len = u32::from_be_bytes(
        bytes[len_pos..len_pos + 4]
            .try_into()
            .expect("length prefix slice is 4 bytes"),
    );
    assert_eq!(
        written_len as usize,
        last_key_bytes.len(),
        "last_key length prefix must match the last partition's key length"
    );
}
