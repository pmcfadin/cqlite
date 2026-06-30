//! Issue #1143 (regression guard for #964): the DEFAULT disk-access backend
//! must not silently memory-map an ordinary SSTable under read-while-write load.
//!
//! ## What regressed
//!
//! Commit `1021577` / `e26df3f` ("feat(reader): size-aware disk-access backend
//! with configurable prefetch", #964) added `StorageConfig::disk_access_mode`,
//! defaulting to [`DiskAccessMode::Auto`], and `prefetch`, defaulting to
//! [`PrefetchMode::Auto`]. For an ordinary-sized SSTable (above one page, below
//! `direct_io_memory_fraction` of system RAM — the common benchmark/production
//! case) `Auto` resolved to **mmap with `madvise(MADV_SEQUENTIAL)`** read-ahead.
//!
//! Before #964 the default read backend was buffered I/O (`use_mmap` defaulted to
//! `false`). So #964 silently flipped the default from buffered to
//! mmap-with-aggressive-sequential-readahead.
//!
//! `MADV_SEQUENTIAL` enables aggressive read-ahead AND drop-behind: the kernel
//! evicts pages behind each reader's cursor. With several readers concurrently
//! scanning the same Data.db while a writer flushes (the `mixed.read_while_write`
//! workload, conc=8), each reader's drop-behind evicts pages the *other* readers
//! still need (re-fault storms / page-cache thrash) and the eager read-ahead
//! competes with the concurrent flush for the same I/O queue and page cache.
//! Isolated single-reader throughput *improved* (sequential read-ahead is ideal
//! with no contention) but read p99 under concurrent write roughly doubled
//! (~200µs → ~371µs in the external A/B) — the classic "faster isolated, worse
//! tail under contention" backend regression.
//!
//! ## Why a structural backend-selection guard (not a p99 threshold)
//!
//! Absolute p99 latency is machine-dependent, flaky, and the regression only
//! surfaces under sustained multi-reader + writer load against large production
//! data — a criterion micro-bench went green and missed it. The true root cause
//! is purely STRUCTURAL and fully deterministic: the *default* `Auto` config
//! selecting the mmap+SEQUENTIAL backend for an ordinary file. This guard pins
//! that decision directly.
//!
//! It asserts the load-bearing invariant via the probe-gated
//! `probe_resolved_disk_access_mode` (which runs the real
//! `resolve_disk_access_mode` against actual system memory, exactly as the reader
//! does at `open()`): for a representative ordinary SSTable size, the default
//! `Auto` config must resolve to **Buffered** (or, only when the file genuinely
//! exceeds the RAM fraction, page-cache-bypassing **Direct**) — but NEVER
//! **Mmap**. Mmap stays available only as an explicit opt-in.
//!
//! Pre-fix (#964 heuristic): `Auto` → `Mmap` for a sub-RAM file → FAIL.
//! Post-fix: `Auto` → `Buffered` for a sub-RAM file → PASS.

// Requires the non-default `scan-offload-probe` feature, which exposes
// `probe_resolved_disk_access_mode` (the regression-guard probe). The agent gate
// runs this binary with that feature enabled.
#![cfg(feature = "scan-offload-probe")]

use cqlite_core::config::{DiskAccessMode, PrefetchMode};
use cqlite_core::storage::sstable::reader::probe_resolved_disk_access_mode as resolve;

/// One page (the `mmap_min_size_bytes` default) and the `direct_io_memory_fraction`
/// default, mirroring `StorageConfig::default()`.
const ONE_PAGE: u64 = 4096;
const RAM_FRACTION: f64 = 0.5;

/// A representative ordinary SSTable size: well above one page, well below any
/// plausible machine's `RAM_FRACTION` of RAM. 64 MiB is the default
/// `max_sstable_size`, so a single generation never exceeds it; the
/// `read_while_write` corpus (a static 100k-row lz4 table) is in this ballpark.
const ORDINARY_SSTABLE_BYTES: u64 = 64 * 1024 * 1024;

/// The default-config (`Auto`) backend for an ordinary sub-RAM SSTable must NOT
/// be memory-mapped. This is the exact structural decision the read-while-write
/// p99 regression turned on (issue #1143 / #964).
#[test]
fn default_auto_backend_is_not_mmap_for_ordinary_sstable() {
    let resolved = resolve(
        DiskAccessMode::Auto,
        ORDINARY_SSTABLE_BYTES,
        ONE_PAGE,
        RAM_FRACTION,
    );

    eprintln!(
        "issue #1143 backend guard: Auto for a {ORDINARY_SSTABLE_BYTES}-byte SSTable \
         resolved to {resolved:?}"
    );

    assert_ne!(
        resolved,
        DiskAccessMode::Mmap,
        "Issue #1143 REGRESSION: the default `Auto` disk-access mode selected the \
         mmap backend for an ordinary sub-RAM SSTable. #964 made `Auto` pick \
         mmap+madvise(SEQUENTIAL), whose drop-behind thrashes the shared page cache \
         under concurrent write load and roughly doubled read p99 \
         (mixed.read_while_write). `Auto` must default to buffered I/O; mmap is an \
         explicit opt-in only."
    );

    // The only acceptable Auto outcomes are Buffered (the contention-safe
    // default) or Direct (a genuinely > RAM-fraction one-shot scan; uses its own
    // per-cursor aligned buffer, so no shared-page drop-behind). An ordinary
    // 64 MiB file is far below the RAM fraction on any real host, so this should
    // be Buffered — but accept Direct defensively for tiny-RAM CI containers.
    assert!(
        matches!(resolved, DiskAccessMode::Buffered | DiskAccessMode::Direct),
        "Auto must resolve to Buffered (or Direct for > RAM-fraction files), got {resolved:?}"
    );
}

/// Tiny files are buffered under `Auto` (mapping a tiny file is pointless and
/// mmap is the contention hazard regardless).
#[test]
fn default_auto_backend_is_buffered_for_tiny_file() {
    assert_eq!(
        resolve(DiskAccessMode::Auto, 1024, ONE_PAGE, RAM_FRACTION),
        DiskAccessMode::Buffered,
        "Auto must use buffered I/O for a sub-page file"
    );
}

/// An explicit `Mmap` request is still honored (the opt-in is preserved); only
/// the silent `Auto` default changed. Guards against an over-broad fix that
/// removes mmap entirely.
#[test]
fn explicit_mmap_is_still_honored() {
    assert_eq!(
        resolve(
            DiskAccessMode::Mmap,
            ORDINARY_SSTABLE_BYTES,
            ONE_PAGE,
            RAM_FRACTION,
        ),
        DiskAccessMode::Mmap,
        "explicit Mmap mode must still select the mmap backend"
    );
}

/// `PrefetchMode::Auto` remains the configured default — this guard is about the
/// *backend* selection, not prefetch; assert the default is unchanged so the two
/// stay decoupled (a future change that flips the prefetch default should be a
/// deliberate, separately-reviewed decision).
#[test]
fn prefetch_default_unchanged() {
    assert_eq!(PrefetchMode::default(), PrefetchMode::Auto);
}
