## Why

CQLite has **no functioning read cache**. Every query re-reads and re-decompresses every
compression chunk it touches, every time it touches it. The one pre-existing cache
(`memory::MemoryManager.block_cache`, `SSTableReader.block_cache`) is initialized empty and
never inserted into, and its lookup is a `RwLock<LruCache>` whose `get()` mutates recency —
so every "read" takes the *write* lock and the `RwLock` degrades to a global `Mutex`
(`cqlite-core/src/memory/mod.rs:129`). The July 2026 read-path audit
(`docs/reports/read-path-performance-audit-2026-07-01.md` §Epic B) identifies a shared,
bytes-bounded, concurrent decompressed-chunk cache as the single biggest lever for point-read
latency and for p99 under concurrent load.

This is **Epic B / child B1 (#1567)**, capstone Wave 3 of the read-path audit; OWNER DECISION #1
(2026-07-01) locked the approach: **build this cache**. It is **design-driven** (a new subsystem
with real latitude in structure, concurrency strategy, and eviction — no external oracle dictates
the cache shape), so it goes through OpenSpec. Its *correctness guardrail* is oracle-driven: the
cache MUST NOT change any read result — the 33-table `sstabledump` parity harness stays green
byte-for-byte.

The A-series measurement benches are on `main` and validate this change: A1 (#1562) real point-read,
A2 (#1563) tail latency, A3 (#1564) concurrent scaling, A4 (#1565) dhat memory budget.

Milestone: **M7 (perf validation)**. Target: reduce repeated-read latency without breaching the
<128MB memory budget.

## What Changes

- **New shared, bytes-bounded, sharded decompressed-chunk cache** (`cqlite-core/src/storage/cache/`,
  new module). Value = `Arc<[u8]>` of the *decompressed* chunk; a cache hit is a refcount bump
  (`Arc::clone`), **never a memcpy**. Key = authoritative `(sstable identity, chunk_index)` derived
  from the reader's `file_path` + `generation` and the chunk index computed from the authoritative
  `CompressionInfo` chunk length / promoted-index offset — never guessed from byte content.
- **Bytes-bounded eviction, not entry-count.** Entries are weighed by decompressed length; the cache
  evicts LRU-order under a total-bytes budget. Internally **sharded** (fixed shard count), each shard
  a `Mutex<LruCache<…>>` with its own byte accounting, so the hit path takes only a per-shard lock —
  **no global lock**. (Reusing the tested `lru` crate internals, hand-sharded, is explicitly blessed
  by owner decision #1; no new external cache dependency.)
- **Wired into all three decompressed-chunk read sites**, each with the lookup order
  `cache get → on miss: read + CRC-verify + decompress → insert Arc → return`:
  1. the BIG point-read chunk fetch (`data_access/mod.rs` `get_cached_data`),
  2. the windowed streaming scan chunk fill (`reader/scan_stream_windowed.rs`),
  3. the BTI target-chunk read (`data_access/bti.rs`).
- **A decompress-work counter** (extending the `work_counters` pattern) that increments on every
  actual chunk decompression at those sites, so a test proves a cache hit performed **zero**
  decompressions and **zero** underlying reads. If the A5 counter is insufficient (the existing
  `chunks_decompressed` is seek-path-only and `tombstones`-gated), this change adds an unconditional
  counter using the same process-global atomic pattern.
- **Byte budget** is a constructor parameter with a sane default (256 MB) until B2 (#… config knob)
  lands; the shared instance is process-global (mirroring `work_counters`), and the cache *type* is
  independently constructible so eviction/byte-budget/zero-copy behavior is unit-testable with a
  local instance (deterministic, immune to cross-test global races — the #1071 lesson).
- **A `read/point_lookup_repeated` bench** (second identical point read ≥10× faster than cold) added
  to `cqlite-core/benches/perf-gate.json`.

## Non-goals

- **No change to any read RESULT.** Byte-for-byte 33-table parity is a hard guardrail, not a goal to
  be traded off. The cache is a pure performance layer over an immutable data source.
- **Not caching compressed bytes.** The win is skipping *decompress*; the OS page cache already holds
  compressed bytes. Only decompressed chunks are cached.
- **Not removing the dead `block_cache`/`block_meta_cache`/`MemoryManager` subsystems** — that is B2
  (#…), a separate issue. This change adds the new cache alongside; B2 retires the old one.
- **Not the key/partition-offset cache** (B4) or the honest observability surface (B5) — separate
  children. This change may add minimal hit/miss counters for its own tests; the full `DatabaseStats`
  surface is B5.
- **No new external crate dependency** for the cache (uses existing `lru` + `bytes`/`Arc<[u8]>`).
- **No change to the no-heuristics posture** — keying is from authoritative reader identity + chunk
  offsets, never inferred from byte patterns.

## Impact

- **Memory budget (<128MB):** the cache is bytes-bounded and evicts under budget; the A4 dhat lane
  validates a scan of a table larger than the cache completes with bounded memory. Default budget is
  a knob, sized conservatively for the budget.
- **No-heuristics mandate:** unaffected — authoritative keying only.
- **Public binding surfaces (Python/Node/CLI):** unchanged behavior; reads get faster on repeat.
  No API signature changes required (the shared cache is internal).
- **Concurrency:** a shared sharded cache is concurrency-sensitive — the design forbids poisoned-lock
  propagation, unbounded growth, and any global lock on the hit path; correctness under concurrent
  scan pressure is a tested requirement.
