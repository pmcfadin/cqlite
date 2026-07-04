//! Cfg-gated read-work counters for the read-path optimization program
//! (Issue #1566, Epic A / A5).
//!
//! # Why this exists
//!
//! The July 2026 read-path audit (`docs/reports/read-path-performance-audit-2026-07-01.md`
//! §Epic A) is measurement-first: land the gauges before the optimizations so every
//! later claim (epics B–G) is pinned. Correct result rows never prove *how much
//! work* a read did — a regression can return the right answer while decompressing
//! the whole file, walking the trie twice, or reopening `Data.db` per lookup. These
//! counters make that work observable so a later epic can assert on it the
//! no-heuristics way: observe the *work*, not just the result.
//!
//! # Why cfg-gated (unlike [`work_counters`](super::work_counters))
//!
//! The existing [`work_counters`](super::work_counters) sit on *cold per-lookup
//! boundaries* (once per candidate SSTable / once per returned partition) and are
//! therefore always-on. A5's counters sit on *per-chunk / per-seek / per-open*
//! paths that are much hotter, and the issue's DoD demands "zero overhead in
//! release builds". So the pattern here (design.md Decision 1) is:
//!
//! - The `record_*()` functions are **unconditional public functions** — the read
//!   path calls e.g. [`record_decompress`] with no `#[cfg]` at the call site, so
//!   production code reads identically in every build.
//! - Their **body** is `#[cfg(any(test, feature = "work-counters"))]`. In a
//!   default/release build (no `work-counters`, no `cfg(test)`) the body is empty
//!   and the `#[inline(always)]` no-op is optimized away → **zero overhead**. No
//!   atomic is even linked (the static and its module are compiled only under the
//!   same cfg), so the release read path pays literally nothing.
//! - The **getters + [`reset`]** are `#[cfg(any(test, feature = "work-counters"))]`:
//!   only test/feature builds read the values. In-crate unit tests get them via the
//!   `test` cfg; integration tests in `tests/` and benches enable the
//!   `work-counters` feature (they compile the lib WITHOUT its `test` cfg — the same
//!   reason the `SCAN_FOR_KEY_CALLS` probe exists).
//!
//! A local-[`Counters`]-instance unit test (per issue #1071) gives deterministic
//! absolute-value assertions immune to parallel tests mutating the process-global.
//!
//! # Counters and their consuming epic-children
//!
//! Grep a counter name to find its consumer, the same discoverability
//! [`work_counters`](super::work_counters) provides:
//!
//! - [`record_trie_walk`] / [`trie_walks`] — **`TRIE_WALKS`**: one per BTI trie
//!   descent (`lookup_partition_via_bti_trie`). Consumers **C3** (single-walk point
//!   lookup) and **C4** (hoist the trie rehash out of the loop): both must prove a
//!   point lookup descends the trie exactly once.
//! - [`record_decompress`] / [`decompress_calls`] — **`DECOMPRESS_CALLS`**: one per
//!   compression-chunk decompress (`Compressor::decompress`). Consumers **B1**
//!   (chunk cache — a cache hit must skip the decompress) and **E3** (copy-chain —
//!   count the decompress invocations a read triggers).
//! - [`record_seek`] / [`seek_calls`] — **`SEEK_CALLS`**: one per production
//!   read-path seek. Wired across every read-path seek site: the compressed
//!   chunk-read seek in `reader/block_io.rs` and the point-lookup / single-partition
//!   / whole-section / scan seeks in the `reader/data_access` modules
//!   (`bti.rs` BTI target-chunk + fallback + scan, `big_promoted.rs` BIG
//!   reverse-block target-chunk + last-partition seek-to-end). Consumer **E4** (drop
//!   redundant seeks): a sequential chunk walk must not re-seek to the position it is
//!   already at.
//! - [`record_file_open`] / [`file_opens`] — **`FILE_OPENS`**: one per `open(2)` that
//!   mints a reader `BlockSource` file descriptor (`reader/source.rs` scan opens +
//!   the reader's cold-open sites). Consumer **C2** (pread / kill the per-lookup
//!   open): a point lookup must not `open(2)` `Data.db` on every call.
//! - [`fd_high_water`] — the **fd high-water helper**: samples the process's current
//!   open-fd count (`/dev/fd` on macOS, `/proc/self/fd` on Linux, `None` elsewhere).
//!   Consumer **C2** (fd-exhaustion guard): a test brackets an operation with this
//!   to bound fd growth. It is a *sample*, not an atomic — wrapping every
//!   `open`/`close` to keep a live count is invasive and racy; sampling at a test
//!   checkpoint is what the guard actually needs.

// The atomics, the process-global, the struct methods, the getters, and `reset`
// exist ONLY in test/feature builds — a release build links none of them, which is
// what makes the counters zero-overhead there.
#[cfg(any(test, feature = "work-counters"))]
use std::sync::atomic::{AtomicU64, Ordering};

/// The four read-work counters as one value (test/feature builds only).
///
/// Production code shares a single process-global instance ([`COUNTERS`]) reached
/// through the unconditional `record_*` free functions; the increment sites and the
/// integration probes all operate on that instance. Bundling the atomics in a
/// struct also lets a unit test exercise the add/get/reset contract against a
/// *local* instance, immune to other tests concurrently mutating the global
/// (issue #1071) — the global is shared with read-path code that any parallel test
/// can drive.
#[cfg(any(test, feature = "work-counters"))]
struct Counters {
    /// BTI trie descents (`TRIE_WALKS`) — consumers C3/C4.
    trie_walks: AtomicU64,
    /// Compression-chunk decompress invocations (`DECOMPRESS_CALLS`) — consumers B1/E3.
    decompress_calls: AtomicU64,
    /// Block-read seeks in the chunk read path (`SEEK_CALLS`) — consumer E4.
    seek_calls: AtomicU64,
    /// `open(2)` calls minting a reader `BlockSource` fd (`FILE_OPENS`) — consumer C2.
    file_opens: AtomicU64,
    /// `read_exact` reads in the compressed-chunk read path (`READ_CALLS`) —
    /// consumer E3. One per logical read of a compression chunk's bytes. Before
    /// E3 a chunk cost TWO reads (payload then trailing CRC as separate
    /// `read_exact` calls); E3 folds them into one `read_exact` into a single
    /// `payload+CRC` buffer, so a steady-state chunk read records exactly one.
    read_calls: AtomicU64,
    /// Heap allocations in the per-chunk read→decompress→window-fill copy chain
    /// (`CHUNK_PATH_ALLOCS`) — consumer E3. Bumped at each genuine heap allocation
    /// the copy chain performs for one chunk: the compressed-bytes buffer
    /// (`block_io`), the decompression output buffer (`compression`), and the
    /// cached decompressed `Arc<[u8]>` (`DecompressedChunkCache`). Before E3 a
    /// steady-state windowed-scan chunk allocated all three (≥3); after E3 the
    /// compressed buffer is a recycled per-scan scratch and decompression writes
    /// into a reused scratch, so only the cached `Arc` (issue B1, kept) allocates
    /// — exactly one per chunk.
    chunk_path_allocs: AtomicU64,
}

#[cfg(any(test, feature = "work-counters"))]
impl Counters {
    const fn new() -> Self {
        Self {
            trie_walks: AtomicU64::new(0),
            decompress_calls: AtomicU64::new(0),
            seek_calls: AtomicU64::new(0),
            file_opens: AtomicU64::new(0),
            read_calls: AtomicU64::new(0),
            chunk_path_allocs: AtomicU64::new(0),
        }
    }

    fn record_trie_walk(&self) {
        self.trie_walks.fetch_add(1, Ordering::Relaxed);
    }

    fn record_decompress(&self) {
        self.decompress_calls.fetch_add(1, Ordering::Relaxed);
    }

    fn record_seek(&self) {
        self.seek_calls.fetch_add(1, Ordering::Relaxed);
    }

    fn record_file_open(&self) {
        self.file_opens.fetch_add(1, Ordering::Relaxed);
    }

    fn record_read(&self) {
        self.read_calls.fetch_add(1, Ordering::Relaxed);
    }

    fn record_chunk_path_alloc(&self) {
        self.chunk_path_allocs.fetch_add(1, Ordering::Relaxed);
    }

    fn trie_walks(&self) -> u64 {
        self.trie_walks.load(Ordering::Relaxed)
    }

    fn decompress_calls(&self) -> u64 {
        self.decompress_calls.load(Ordering::Relaxed)
    }

    fn seek_calls(&self) -> u64 {
        self.seek_calls.load(Ordering::Relaxed)
    }

    fn file_opens(&self) -> u64 {
        self.file_opens.load(Ordering::Relaxed)
    }

    fn read_calls(&self) -> u64 {
        self.read_calls.load(Ordering::Relaxed)
    }

    fn chunk_path_allocs(&self) -> u64 {
        self.chunk_path_allocs.load(Ordering::Relaxed)
    }

    fn reset(&self) {
        self.trie_walks.store(0, Ordering::Relaxed);
        self.decompress_calls.store(0, Ordering::Relaxed);
        self.seek_calls.store(0, Ordering::Relaxed);
        self.file_opens.store(0, Ordering::Relaxed);
        self.read_calls.store(0, Ordering::Relaxed);
        self.chunk_path_allocs.store(0, Ordering::Relaxed);
    }
}

/// The process-global counters every read-path increment site and integration
/// probe shares (test/feature builds only). Unit tests that assert absolute values
/// use a local [`Counters`] instead (issue #1071).
#[cfg(any(test, feature = "work-counters"))]
static COUNTERS: Counters = Counters::new();

/// Record one BTI trie descent (`TRIE_WALKS`; consumers C3/C4).
///
/// Called unconditionally at the single trie-descent entry
/// ([`lookup_partition_via_bti_trie`](super::reader)); the body compiles to a no-op
/// in a release build (design.md Decision 1).
#[inline(always)]
pub fn record_trie_walk() {
    #[cfg(any(test, feature = "work-counters"))]
    COUNTERS.record_trie_walk();
}

/// Record one compression-chunk decompress (`DECOMPRESS_CALLS`; consumers B1/E3).
///
/// Called unconditionally at the single decompress entry
/// ([`Compressor::decompress`](super::compression)); the body compiles to a no-op
/// in a release build (design.md Decision 1).
#[inline(always)]
pub fn record_decompress() {
    #[cfg(any(test, feature = "work-counters"))]
    COUNTERS.record_decompress();
}

/// Record one production read-path seek (`SEEK_CALLS`; consumer E4).
///
/// Called unconditionally at every production read-path seek site
/// (`reader/block_io.rs` chunk-read seek plus the `reader/data_access` BTI/BIG
/// point-lookup, single-partition, whole-section, and scan seeks); the body
/// compiles to a no-op in a release build (design.md Decision 1).
#[inline(always)]
pub fn record_seek() {
    #[cfg(any(test, feature = "work-counters"))]
    COUNTERS.record_seek();
}

/// Record one `open(2)` that mints a reader `BlockSource` fd (`FILE_OPENS`;
/// consumer C2).
///
/// Called unconditionally at each `BlockSource` fd-mint (the reader's cold-open
/// sites and the per-scan opens in `reader/source.rs`); the body compiles to a
/// no-op in a release build (design.md Decision 1).
#[inline(always)]
pub fn record_file_open() {
    #[cfg(any(test, feature = "work-counters"))]
    COUNTERS.record_file_open();
}

/// Record one logical `read_exact` of a compression chunk's bytes (`READ_CALLS`;
/// consumer E3).
///
/// Called unconditionally at the single compressed-chunk read site in
/// `reader/block_io.rs`; the body compiles to a no-op in a release build
/// (design.md Decision 1).
#[inline(always)]
pub fn record_read() {
    #[cfg(any(test, feature = "work-counters"))]
    COUNTERS.record_read();
}

/// Record one heap allocation in the per-chunk read→decompress→window-fill copy
/// chain (`CHUNK_PATH_ALLOCS`; consumer E3).
///
/// Called unconditionally at each genuine chunk-path allocation site (the
/// compressed-bytes buffer in `block_io`, the decompression output buffer in
/// `compression`, and the cached `Arc<[u8]>` in `DecompressedChunkCache`); the
/// body compiles to a no-op in a release build (design.md Decision 1).
#[inline(always)]
pub fn record_chunk_path_alloc() {
    #[cfg(any(test, feature = "work-counters"))]
    COUNTERS.record_chunk_path_alloc();
}

/// Number of BTI trie descents since the last [`reset`] (`TRIE_WALKS`).
#[cfg(any(test, feature = "work-counters"))]
pub fn trie_walks() -> u64 {
    COUNTERS.trie_walks()
}

/// Number of compression-chunk decompress invocations since the last [`reset`]
/// (`DECOMPRESS_CALLS`).
#[cfg(any(test, feature = "work-counters"))]
pub fn decompress_calls() -> u64 {
    COUNTERS.decompress_calls()
}

/// Number of block-read seeks since the last [`reset`] (`SEEK_CALLS`).
#[cfg(any(test, feature = "work-counters"))]
pub fn seek_calls() -> u64 {
    COUNTERS.seek_calls()
}

/// Number of `open(2)` reader-fd mints since the last [`reset`] (`FILE_OPENS`).
#[cfg(any(test, feature = "work-counters"))]
pub fn file_opens() -> u64 {
    COUNTERS.file_opens()
}

/// Number of compressed-chunk `read_exact` reads since the last [`reset`]
/// (`READ_CALLS`).
#[cfg(any(test, feature = "work-counters"))]
pub fn read_calls() -> u64 {
    COUNTERS.read_calls()
}

/// Number of per-chunk copy-chain heap allocations since the last [`reset`]
/// (`CHUNK_PATH_ALLOCS`).
#[cfg(any(test, feature = "work-counters"))]
pub fn chunk_path_allocs() -> u64 {
    COUNTERS.chunk_path_allocs()
}

/// Clear all four process-global counters. Integration tests call this before a
/// measured operation so a stale value cannot satisfy a later assertion. Because
/// the global is shared, callers serialize on the shared test mutex (per the
/// existing counter-test convention); the in-crate unit test sidesteps this by
/// asserting against a local [`Counters`] instance (issue #1071).
#[cfg(any(test, feature = "work-counters"))]
pub fn reset() {
    COUNTERS.reset();
}

/// Sample the process's current open file-descriptor count (the fd high-water
/// helper; consumer C2).
///
/// Returns `Some(count)` by counting entries in `/dev/fd` (macOS) or
/// `/proc/self/fd` (Linux), and `None` on any other platform so a test can `skip`
/// rather than fail. This is a point-in-time sample, not a running maximum: a test
/// captures it before/after an operation to bound fd growth (C2's fd-exhaustion
/// guard). Best-effort — an unreadable fd directory yields `None`.
#[cfg(any(test, feature = "work-counters"))]
pub fn fd_high_water() -> Option<u64> {
    #[cfg(target_os = "macos")]
    {
        count_fd_dir("/dev/fd")
    }
    #[cfg(target_os = "linux")]
    {
        count_fd_dir("/proc/self/fd")
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        None
    }
}

/// Count the directory entries under an fd directory (`/dev/fd` | `/proc/self/fd`).
/// Returns `None` if the directory cannot be read.
#[cfg(all(
    any(test, feature = "work-counters"),
    any(target_os = "macos", target_os = "linux")
))]
fn count_fd_dir(path: &str) -> Option<u64> {
    std::fs::read_dir(path)
        .ok()
        .map(|rd| rd.filter_map(|e| e.ok()).count() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    // Exercises the add/get/reset contract against a *local* [`Counters`] rather
    // than the process-global reached through the free functions. The global is
    // shared with read-path increment sites any concurrent test in this binary can
    // drive, so absolute-value assertions on it race nondeterministically
    // (issue #1071). A local instance is owned by this test alone, so the
    // exact-equality checks below are deterministic. Serialized so the local test
    // never overlaps a wiring test that mutates the global (shared-mutex convention).
    #[test]
    #[serial]
    fn counters_round_trip_and_reset() {
        let c = Counters::new();
        c.reset();
        assert_eq!(c.trie_walks(), 0);
        assert_eq!(c.decompress_calls(), 0);
        assert_eq!(c.seek_calls(), 0);
        assert_eq!(c.file_opens(), 0);

        c.record_trie_walk();
        c.record_decompress();
        c.record_decompress();
        c.record_seek();
        c.record_seek();
        c.record_seek();
        c.record_file_open();
        c.record_file_open();
        c.record_file_open();
        c.record_file_open();
        c.record_read();
        c.record_read();
        c.record_read();
        c.record_read();
        c.record_read();
        c.record_chunk_path_alloc();
        c.record_chunk_path_alloc();
        c.record_chunk_path_alloc();
        c.record_chunk_path_alloc();
        c.record_chunk_path_alloc();
        c.record_chunk_path_alloc();

        assert_eq!(c.trie_walks(), 1);
        assert_eq!(c.decompress_calls(), 2);
        assert_eq!(c.seek_calls(), 3);
        assert_eq!(c.file_opens(), 4);
        assert_eq!(c.read_calls(), 5);
        assert_eq!(c.chunk_path_allocs(), 6);

        c.reset();
        assert_eq!(c.trie_walks(), 0);
        assert_eq!(c.decompress_calls(), 0);
        assert_eq!(c.seek_calls(), 0);
        assert_eq!(c.file_opens(), 0);
        assert_eq!(c.read_calls(), 0);
        assert_eq!(c.chunk_path_allocs(), 0);
    }

    // The fd high-water helper returns a positive count on the supported platforms
    // (the process always holds stdin/stdout/stderr) and `None` elsewhere — a
    // skip, never a failure.
    #[test]
    #[serial]
    fn fd_high_water_is_positive_or_unsupported() {
        match fd_high_water() {
            Some(n) => assert!(
                n > 0,
                "a running process holds at least stdio fds, so the sample must be > 0"
            ),
            None => { /* unsupported platform: skip, not fail */ }
        }
    }
}
