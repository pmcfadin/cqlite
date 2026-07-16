//! Allocation regression-lock for Issue #1676 (Epic R, finding R5).
//!
//! The write-path audit (`docs/reports/write-path-performance-audit-2026-07-01.md`)
//! flagged that `StatisticsMetadata` was **cloned per partition** just to seed the
//! `DataWriter`'s delta-encoding baselines, even though the writer reads only three
//! `Copy` scalars from it (`min_timestamp`, `min_ttl`, `min_local_deletion_time`).
//!
//! ## Verified state (this is a verify + regression-lock task, not a fresh fix)
//!
//! The optimization is **already implemented on `main`**: `DataWriter` stores a
//! `#[derive(Clone, Copy)] EncodingStatsBaselines` (the three scalars only), and the
//! per-partition baseline-update path
//! (`SSTableWriter::write_partition` → `DataWriter::update_stats_from_metadata`)
//! takes `&StatisticsMetadata` **by reference** and copies just those scalars:
//!
//! ```ignore
//! pub fn update_stats_from_metadata(&mut self, stats: &StatisticsMetadata) {
//!     self.stats = EncodingStatsBaselines::from(stats); // Copy, stack-only
//! }
//! ```
//!
//! No per-partition `StatisticsMetadata` clone exists; the only clones are one-time
//! (`DataWriter::with_sink(stats.clone(), …)` at open, `StatisticsWriter::write`'s
//! finalize clone at SSTable finish). So the strict "red on main" property is
//! ALREADY GREEN — this test locks that state so the finding can never regress.
//!
//! ## What this test measures
//!
//! `update_stats_from_metadata` is the *exact* operation `SSTableWriter` runs once
//! per partition. We drive it 1,000 times (the "1,000 single-row partitions"
//! workload from the issue) against a fully-populated `StatisticsMetadata` under a
//! process-global counting allocator and assert **zero** heap allocations.
//!
//! The positive control up front clones the same `StatisticsMetadata` **once** and
//! asserts the clone DOES allocate (its two `EstimatedHistogram`s own `Vec`s, its
//! `TombstoneHistogram` a `BTreeMap`, and `first_key`/`last_key` own `Vec`s). This
//! proves both that the counting allocator works AND that a reintroduced
//! per-partition clone would allocate ≥1 block/partition — i.e. this guard would
//! then observe ≥1,000 allocations and fail, catching the regression.
//!
//! ## Single-test-binary invariant (why the strict `== 0` is not flaky)
//!
//! `CountingAlloc` is the process-global allocator and increments while `COUNTING`
//! is on regardless of which thread allocates. This file therefore contains
//! **exactly one `#[test]`**, so libtest spawns no sibling test threads that could
//! allocate concurrently, and the measured window is a tight synchronous loop that
//! spawns no threads and has no `.await` points — between the two `ALLOCS` loads
//! only the measuring thread is live. Under that invariant the global counter
//! observes only this thread's allocations, so the exact `== 0` assertion is
//! deterministic (the same guarantee the precedent guard
//! `tests/test_issue_1660_write_path_allocs.rs` relies on). Do NOT add a second
//! `#[test]` to this binary without making the counter thread-scoped first.

#![cfg(feature = "write-support")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use cqlite_core::storage::sstable::writer::data_writer::DataWriter;
use cqlite_core::storage::sstable::writer::stats_writer::StatisticsMetadata;

/// Counts every allocation/reallocation (never deallocation) while `COUNTING` is
/// on, so the window is scoped exactly to the operation under test. Process-global:
/// it counts on any thread, which is safe here only because this binary holds
/// exactly one `#[test]` (see the module-level "Single-test-binary invariant").
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

/// Number of partitions in the simulated many-small-partition SSTable.
const PARTITIONS: usize = 1_000;

/// Build a `StatisticsMetadata` shaped like the accumulator a 1,000-single-row-partition
/// flush produces: both estimated histograms populated and a non-empty key range, so
/// its heap footprint (the state a per-partition clone would have to copy) is realistic.
fn populated_stats() -> StatisticsMetadata {
    let mut stats = StatisticsMetadata::new();
    stats.min_timestamp = 1_000_000;
    stats.min_ttl = 0;
    stats.min_local_deletion_time = 0;
    for i in 0..PARTITIONS as u64 {
        // Vary the observations so the histogram buckets hold real spread.
        stats.record_partition(64 + (i % 512), 1 + (i % 8));
        stats.update_key_range(&(i as u32).to_be_bytes());
    }
    stats
}

#[test]
fn per_partition_baseline_update_allocates_zero() {
    let stats = populated_stats();
    // Construct the DataWriter (and its one-time construction copy) BEFORE the
    // counting window so only the per-partition baseline-update work is measured.
    let mut writer = DataWriter::new(StatisticsMetadata::new());

    // ── positive control ──────────────────────────────────────────────────────
    // A per-partition `StatisticsMetadata` clone (the pre-R5 behaviour) MUST
    // allocate; this both proves the counting allocator is live and documents the
    // regression signal this guard trips on.
    COUNTING.store(true, Ordering::Relaxed);
    let control_baseline = ALLOCS.load(Ordering::Relaxed);
    let cloned = std::hint::black_box(stats.clone());
    let control_allocs = ALLOCS.load(Ordering::Relaxed) - control_baseline;
    COUNTING.store(false, Ordering::Relaxed);
    drop(cloned); // deallocations are never counted
    assert!(
        control_allocs > 0,
        "positive control: cloning a populated StatisticsMetadata must allocate \
         (got {control_allocs}); a per-partition clone would cost this PER PARTITION"
    );

    // ── measured window: the real per-partition baseline-update path ───────────
    COUNTING.store(true, Ordering::Relaxed);
    let baseline = ALLOCS.load(Ordering::Relaxed);
    for _ in 0..PARTITIONS {
        // Exactly what SSTableWriter::write_partition runs once per partition.
        writer.update_stats_from_metadata(std::hint::black_box(&stats));
    }
    let allocs = ALLOCS.load(Ordering::Relaxed) - baseline;
    COUNTING.store(false, Ordering::Relaxed);
    // ───────────────────────────────────────────────────────────────────────────

    assert_eq!(
        allocs, 0,
        "Issue #1676 (R5): {PARTITIONS} per-partition baseline updates allocated \
         {allocs} times — expected 0. `update_stats_from_metadata` must keep copying \
         the three Copy scalars via `EncodingStatsBaselines`; a reintroduced \
         per-partition `StatisticsMetadata` clone would allocate ~{control_allocs} \
         blocks per partition"
    );
}
