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
//! - [`record_key_hash`] / [`key_hash_calls`] — **`KEY_HASH_CALLS`**: one per
//!   Murmur3 hash + BTI byte-comparable encoding of a query partition key
//!   (`encode_partition_key_for_bti_trie`). Consumer **C4** (hoist per-candidate
//!   rehashing): a multi-generation `WHERE pk = ?` point read must hash+encode the
//!   query key exactly ONCE per read (the encoding is identical for every candidate
//!   SSTable), not once per candidate. On `main`/pre-C4 the candidate-prune loop
//!   re-encoded per candidate (N hashes for N generations); after C4 it is 1.
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
//!
//! # Parser work counters (Issue #1618, Epic H / H5)
//!
//! H5 lands the *parser* gauges the audit (`§Epic H`, block 2) needs so epics
//! J1/K2/K3/L1/L3 can assert their claims ("zero `to_lowercase` per cell", "one
//! header try-parse per partition", "<40 BTI nodes visited"). Same cfg-gated,
//! zero-in-release pattern as the A5 counters above:
//!
//! - [`record_type_normalize`] / [`type_normalize_calls`] — **`TYPE_NORMALIZE_CALLS`**:
//!   the per-cell decode-path type-normalization gauge. **J1 (issue #1635) delivered**:
//!   dispatch is now resolved ONCE per column at `RowColumnResolution::build`
//!   (cached on `ColumnToParse.kind`/`is_complex`), so the per-cell `to_lowercase`
//!   sites — the value-parse normalization (`v5_compressed_legacy/cell_value.rs`) and
//!   the per-row complex-check (`v5_compressed_legacy/udt.rs::is_complex_column`) —
//!   are gone. A full fixture scan therefore records `0` (on `main`/pre-J1 it was
//!   ≥2/cell). No production site calls [`record_type_normalize`] anymore; the counter
//!   is retained as a regression tripwire — any reintroduced per-cell normalization
//!   must record it, flipping the J1 `== 0` assertions red.
//! - [`record_partition_header_try_parse`] / [`partition_header_try_parses`] —
//!   **`PARTITION_HEADER_TRY_PARSES`**: one per speculative partition-header parse
//!   (`v5_compressed_legacy/row_framing.rs::parse_partition_header_full`, the single
//!   boundary-peek/try-parse primitive every emit path routes through). Consumer
//!   **K2/K3** (one try-parse per partition): flips to an exact per-partition bound.
//! - [`record_bti_node_visited`] / [`bti_nodes_visited`] — **`BTI_NODES_VISITED`**:
//!   one per node the BTI DFS enters (`bti/parser/traversal.rs`). Consumer **L1/L3**
//!   (<40 BTI nodes visited): flips to the bounded-descent count.
//! - [`record_bti_pointer_decode`] / [`bti_pointer_decodes`] — **`BTI_POINTER_DECODES`**:
//!   one per BTI node/pointer decode (`bti/parser/node_decode.rs::parse_bti_node`).
//!   Consumer **L1/L3**: pairs with `BTI_NODES_VISITED` to prove the descent does not
//!   re-decode nodes.
//! - [`record_row_sort`] / [`row_sort_invocations`] — **`ROW_SORT_INVOCATIONS`**:
//!   the per-row cell `sort_by` gauge at the shared display-row builder
//!   (`v5_compressed_legacy/mod.rs::build_display_row`, which #1334 consolidated the
//!   former `block_emit`/`block_emit_windowed` sort sites into). **K3 (issue #1642)
//!   delivered**: the decoder now emits cells positionally in serialization-header
//!   column order (deterministic by CONSTRUCTION), so `build_display_row` performs
//!   NO per-row sort and a full scan records `0` (on `main`/pre-K3 it was one per
//!   returned live row). No production site calls [`record_row_sort`] anymore; the
//!   counter is retained as a regression tripwire — any reintroduced per-row cell
//!   sort must record it, flipping the K3 `== 0` scan assertions red.

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
    /// (`CHUNK_PATH_ALLOCS`) — consumer E3/#1940. **Instrumented today at ONE site
    /// only**: the compressed-bytes `payload+CRC` buffer allocated in the cursor
    /// chunk-read path (`block_io`). The other two copy-chain allocation sites —
    /// the decompression output buffer (`compression`) and the cached decompressed
    /// `Arc<[u8]>` (`DecompressedChunkCache`) — are NOT yet instrumented, so this
    /// counter currently reflects the compressed-buffer allocation alone and
    /// undercounts the full copy chain. Wiring those sites (and the ≤1-alloc/chunk
    /// reduction they measure) is the A4 work deferred to issue #1940.
    chunk_path_allocs: AtomicU64,
    /// `data_type.to_lowercase()` calls in the per-cell decode path
    /// (`TYPE_NORMALIZE_CALLS`, Issue #1618) — consumer J1.
    type_normalize_calls: AtomicU64,
    /// Speculative partition-header parses (`PARTITION_HEADER_TRY_PARSES`,
    /// Issue #1618) — consumers K2/K3.
    partition_header_try_parses: AtomicU64,
    /// BTI DFS node entries (`BTI_NODES_VISITED`, Issue #1618) — consumers L1/L3.
    bti_nodes_visited: AtomicU64,
    /// BTI node/pointer decodes (`BTI_POINTER_DECODES`, Issue #1618) — consumers L1/L3.
    bti_pointer_decodes: AtomicU64,
    /// Per-row cell `sort_by` invocations (`ROW_SORT_INVOCATIONS`, Issue #1618) —
    /// consumers K2/L.
    row_sort_invocations: AtomicU64,
    /// `CompressionInfo.db` parses (`COMPRESSION_INFO_PARSES`, Issue #1597 / G1) —
    /// one per `compression_info::CompressionInfo::parse`. Consumer G1: a reader
    /// open must parse `CompressionInfo.db` exactly once (was 2 — a legacy
    /// `parse_binary` plus the modern `parse`).
    compression_info_parses: AtomicU64,
    /// BIG `Index.db` partition probes (`INDEX_PROBES`, Issue #1570 / B4) — one per
    /// real `index_reader.lookup_partition` in `lookup_partition_with_index`.
    /// Consumer B4 (key→partition-offset cache): a repeated point read served from
    /// the cache must skip the `Index.db` probe, so `INDEX_PROBES` stays 0 on a hit —
    /// the BIG analogue of `TRIE_WALKS == 0` for BTI.
    index_probes: AtomicU64,
    /// Query-key Murmur3 hash + BTI byte-comparable encodings (`KEY_HASH_CALLS`,
    /// Issue #1575 / C4) — one per `encode_partition_key_for_bti_trie`. Consumer C4:
    /// a multi-candidate point read hashes+encodes the key ONCE (hoisted out of the
    /// candidate-prune loop), not once per candidate generation.
    key_hash_calls: AtomicU64,
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
            type_normalize_calls: AtomicU64::new(0),
            partition_header_try_parses: AtomicU64::new(0),
            bti_nodes_visited: AtomicU64::new(0),
            bti_pointer_decodes: AtomicU64::new(0),
            row_sort_invocations: AtomicU64::new(0),
            compression_info_parses: AtomicU64::new(0),
            index_probes: AtomicU64::new(0),
            key_hash_calls: AtomicU64::new(0),
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

    fn record_type_normalize(&self) {
        self.type_normalize_calls.fetch_add(1, Ordering::Relaxed);
    }

    fn record_partition_header_try_parse(&self) {
        self.partition_header_try_parses
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_bti_node_visited(&self) {
        self.bti_nodes_visited.fetch_add(1, Ordering::Relaxed);
    }

    fn record_bti_pointer_decode(&self) {
        self.bti_pointer_decodes.fetch_add(1, Ordering::Relaxed);
    }

    fn record_row_sort(&self) {
        self.row_sort_invocations.fetch_add(1, Ordering::Relaxed);
    }

    fn record_compression_info_parse(&self) {
        self.compression_info_parses.fetch_add(1, Ordering::Relaxed);
    }

    fn record_index_probe(&self) {
        self.index_probes.fetch_add(1, Ordering::Relaxed);
    }

    fn record_key_hash(&self) {
        self.key_hash_calls.fetch_add(1, Ordering::Relaxed);
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

    fn type_normalize_calls(&self) -> u64 {
        self.type_normalize_calls.load(Ordering::Relaxed)
    }

    fn partition_header_try_parses(&self) -> u64 {
        self.partition_header_try_parses.load(Ordering::Relaxed)
    }

    fn bti_nodes_visited(&self) -> u64 {
        self.bti_nodes_visited.load(Ordering::Relaxed)
    }

    fn bti_pointer_decodes(&self) -> u64 {
        self.bti_pointer_decodes.load(Ordering::Relaxed)
    }

    fn row_sort_invocations(&self) -> u64 {
        self.row_sort_invocations.load(Ordering::Relaxed)
    }

    fn compression_info_parses(&self) -> u64 {
        self.compression_info_parses.load(Ordering::Relaxed)
    }

    fn index_probes(&self) -> u64 {
        self.index_probes.load(Ordering::Relaxed)
    }

    fn key_hash_calls(&self) -> u64 {
        self.key_hash_calls.load(Ordering::Relaxed)
    }

    fn reset(&self) {
        self.trie_walks.store(0, Ordering::Relaxed);
        self.decompress_calls.store(0, Ordering::Relaxed);
        self.seek_calls.store(0, Ordering::Relaxed);
        self.file_opens.store(0, Ordering::Relaxed);
        self.read_calls.store(0, Ordering::Relaxed);
        self.chunk_path_allocs.store(0, Ordering::Relaxed);
        self.type_normalize_calls.store(0, Ordering::Relaxed);
        self.partition_header_try_parses.store(0, Ordering::Relaxed);
        self.bti_nodes_visited.store(0, Ordering::Relaxed);
        self.bti_pointer_decodes.store(0, Ordering::Relaxed);
        self.row_sort_invocations.store(0, Ordering::Relaxed);
        self.compression_info_parses.store(0, Ordering::Relaxed);
        self.index_probes.store(0, Ordering::Relaxed);
        self.key_hash_calls.store(0, Ordering::Relaxed);
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
/// chain (`CHUNK_PATH_ALLOCS`; consumer E3/#1940).
///
/// Called today at ONE site: the compressed-bytes `payload+CRC` buffer in the
/// cursor chunk-read path (`block_io`). The remaining copy-chain allocation sites
/// — the decompression output buffer (`compression`) and the cached `Arc<[u8]>`
/// in `DecompressedChunkCache` — are NOT yet instrumented; wiring them is the A4
/// ≤1-alloc/chunk work deferred to issue #1940. The body compiles to a no-op in a
/// release build (design.md Decision 1).
#[inline(always)]
pub fn record_chunk_path_alloc() {
    #[cfg(any(test, feature = "work-counters"))]
    COUNTERS.record_chunk_path_alloc();
}

/// Record one `data_type.to_lowercase()` in the per-cell decode path
/// (`TYPE_NORMALIZE_CALLS`; consumer J1, Issue #1618 / #1635).
///
/// **J1 (issue #1635) removed every per-cell caller**: dispatch is resolved once per
/// column at bind time, so the value-parse normalization and the per-row
/// `is_complex_column` check no longer run per cell and no production site calls this.
/// It is kept (unconditional, zero-overhead in release per design.md Decision 1) as a
/// regression tripwire — any future per-cell normalization must call it, which would
/// flip the J1 `TYPE_NORMALIZE_CALLS == 0` scan assertions red.
#[inline(always)]
pub fn record_type_normalize() {
    #[cfg(any(test, feature = "work-counters"))]
    COUNTERS.record_type_normalize();
}

/// Record one speculative partition-header parse (`PARTITION_HEADER_TRY_PARSES`;
/// consumers K2/K3, Issue #1618).
///
/// Called unconditionally at the single boundary-peek/try-parse primitive
/// (`v5_compressed_legacy/row_framing.rs::parse_partition_header_full`) every emit
/// path routes through; the body compiles to a no-op in a release build.
#[inline(always)]
pub fn record_partition_header_try_parse() {
    #[cfg(any(test, feature = "work-counters"))]
    COUNTERS.record_partition_header_try_parse();
}

/// Record one BTI DFS node entry (`BTI_NODES_VISITED`; consumers L1/L3, Issue #1618).
///
/// Called unconditionally once per node the BTI depth-first walk enters
/// (`bti/parser/traversal.rs`); the body compiles to a no-op in a release build.
#[inline(always)]
pub fn record_bti_node_visited() {
    #[cfg(any(test, feature = "work-counters"))]
    COUNTERS.record_bti_node_visited();
}

/// Record one BTI node/pointer decode (`BTI_POINTER_DECODES`; consumers L1/L3,
/// Issue #1618).
///
/// Called unconditionally at the single node-decode entry
/// (`bti/parser/node_decode.rs::parse_bti_node`); the body compiles to a no-op in a
/// release build.
#[inline(always)]
pub fn record_bti_pointer_decode() {
    #[cfg(any(test, feature = "work-counters"))]
    COUNTERS.record_bti_pointer_decode();
}

/// Record one per-row cell `sort_by` (`ROW_SORT_INVOCATIONS`; consumer K3,
/// Issue #1618 / #1642).
///
/// **K3 (issue #1642) removed the caller**: the decoder emits cells positionally
/// in serialization-header column order, so `build_display_row` no longer sorts and
/// no production site calls this. It is kept (unconditional, zero-overhead in
/// release per design.md Decision 1) as a regression tripwire — any future per-row
/// cell sort must call it, which would flip the K3 `ROW_SORT_INVOCATIONS == 0` scan
/// assertions red.
#[inline(always)]
pub fn record_row_sort() {
    #[cfg(any(test, feature = "work-counters"))]
    COUNTERS.record_row_sort();
}

/// Record one `CompressionInfo.db` parse (`COMPRESSION_INFO_PARSES`; consumer G1,
/// Issue #1597).
///
/// Called unconditionally at the single surviving `CompressionInfo.db` parser
/// (`compression_info::CompressionInfo::parse`); the body compiles to a no-op in a
/// release build (design.md Decision 3).
#[inline(always)]
pub fn record_compression_info_parse() {
    #[cfg(any(test, feature = "work-counters"))]
    COUNTERS.record_compression_info_parse();
}

/// Record one real BIG `Index.db` partition probe (`INDEX_PROBES`; consumer B4,
/// Issue #1570).
///
/// Called unconditionally at the single `index_reader.lookup_partition` probe in
/// `lookup_partition_with_index`; the body compiles to a no-op in a release build
/// (design.md Decision 7). A key→partition-offset cache hit returns before the
/// probe, so a repeated point read records zero probes.
#[inline(always)]
pub fn record_index_probe() {
    #[cfg(any(test, feature = "work-counters"))]
    COUNTERS.record_index_probe();
}

/// Record one query-key Murmur3 hash + BTI byte-comparable encoding
/// (`KEY_HASH_CALLS`; consumer C4, Issue #1575).
///
/// Called unconditionally at the single BTI key-encoding site
/// (`bti::parser::partitions::encode_partition_key_for_bti_trie`, which computes the
/// Murmur3 token); the body compiles to a no-op in a release build (design.md
/// Decision 1). C4 hoists this out of the candidate-prune loop so a multi-generation
/// point read records exactly 1 (not one per candidate).
#[inline(always)]
pub fn record_key_hash() {
    #[cfg(any(test, feature = "work-counters"))]
    COUNTERS.record_key_hash();
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

/// Number of per-cell `data_type.to_lowercase()` calls since the last [`reset`]
/// (`TYPE_NORMALIZE_CALLS`, Issue #1618).
#[cfg(any(test, feature = "work-counters"))]
pub fn type_normalize_calls() -> u64 {
    COUNTERS.type_normalize_calls()
}

/// Number of speculative partition-header parses since the last [`reset`]
/// (`PARTITION_HEADER_TRY_PARSES`, Issue #1618).
#[cfg(any(test, feature = "work-counters"))]
pub fn partition_header_try_parses() -> u64 {
    COUNTERS.partition_header_try_parses()
}

/// Number of BTI DFS node entries since the last [`reset`] (`BTI_NODES_VISITED`,
/// Issue #1618).
#[cfg(any(test, feature = "work-counters"))]
pub fn bti_nodes_visited() -> u64 {
    COUNTERS.bti_nodes_visited()
}

/// Number of BTI node/pointer decodes since the last [`reset`]
/// (`BTI_POINTER_DECODES`, Issue #1618).
#[cfg(any(test, feature = "work-counters"))]
pub fn bti_pointer_decodes() -> u64 {
    COUNTERS.bti_pointer_decodes()
}

/// Number of per-row cell `sort_by` invocations since the last [`reset`]
/// (`ROW_SORT_INVOCATIONS`, Issue #1618).
#[cfg(any(test, feature = "work-counters"))]
pub fn row_sort_invocations() -> u64 {
    COUNTERS.row_sort_invocations()
}

/// Number of `CompressionInfo.db` parses since the last [`reset`]
/// (`COMPRESSION_INFO_PARSES`, Issue #1597).
#[cfg(any(test, feature = "work-counters"))]
pub fn compression_info_parses() -> u64 {
    COUNTERS.compression_info_parses()
}

/// Number of BIG `Index.db` partition probes since the last [`reset`]
/// (`INDEX_PROBES`, Issue #1570).
#[cfg(any(test, feature = "work-counters"))]
pub fn index_probes() -> u64 {
    COUNTERS.index_probes()
}

/// Number of query-key Murmur3 hash + BTI encodings since the last [`reset`]
/// (`KEY_HASH_CALLS`, Issue #1575 / C4).
#[cfg(any(test, feature = "work-counters"))]
pub fn key_hash_calls() -> u64 {
    COUNTERS.key_hash_calls()
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
        // Issue #1618 parser counters: distinct multiplicities so a mis-wired
        // getter/field cross-up is caught.
        for _ in 0..7 {
            c.record_type_normalize();
        }
        for _ in 0..8 {
            c.record_partition_header_try_parse();
        }
        for _ in 0..9 {
            c.record_bti_node_visited();
        }
        for _ in 0..10 {
            c.record_bti_pointer_decode();
        }
        for _ in 0..11 {
            c.record_row_sort();
        }
        for _ in 0..12 {
            c.record_compression_info_parse();
        }
        for _ in 0..13 {
            c.record_index_probe();
        }
        for _ in 0..14 {
            c.record_key_hash();
        }

        assert_eq!(c.trie_walks(), 1);
        assert_eq!(c.decompress_calls(), 2);
        assert_eq!(c.seek_calls(), 3);
        assert_eq!(c.file_opens(), 4);
        assert_eq!(c.read_calls(), 5);
        assert_eq!(c.chunk_path_allocs(), 6);
        assert_eq!(c.type_normalize_calls(), 7);
        assert_eq!(c.partition_header_try_parses(), 8);
        assert_eq!(c.bti_nodes_visited(), 9);
        assert_eq!(c.bti_pointer_decodes(), 10);
        assert_eq!(c.row_sort_invocations(), 11);
        assert_eq!(c.compression_info_parses(), 12);
        assert_eq!(c.index_probes(), 13);
        assert_eq!(c.key_hash_calls(), 14);

        c.reset();
        assert_eq!(c.trie_walks(), 0);
        assert_eq!(c.decompress_calls(), 0);
        assert_eq!(c.seek_calls(), 0);
        assert_eq!(c.file_opens(), 0);
        assert_eq!(c.read_calls(), 0);
        assert_eq!(c.chunk_path_allocs(), 0);
        assert_eq!(c.type_normalize_calls(), 0);
        assert_eq!(c.partition_header_try_parses(), 0);
        assert_eq!(c.bti_nodes_visited(), 0);
        assert_eq!(c.bti_pointer_decodes(), 0);
        assert_eq!(c.row_sort_invocations(), 0);
        assert_eq!(c.compression_info_parses(), 0);
        assert_eq!(c.index_probes(), 0);
        assert_eq!(c.key_hash_calls(), 0);
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
