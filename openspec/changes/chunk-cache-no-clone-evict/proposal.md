## Why

The `ChunkDecompressor` on the CLI read path (`cqlite-core/src/storage/sstable/chunk_decompressor.rs`)
is the one cache that IS live today, and the July 2026 read-path audit
(`docs/reports/read-path-performance-audit-2026-07-01.md` §Epic B, child **B3 / #1569**) found it has
two defects:

1. **Clone-on-hit.** A cache hit `clone()`s the entire decompressed chunk (16–64KB) out of the cache
   (`chunk_decompressor.rs` `get_decompressed_chunk`), so every repeated access pays a full chunk-sized
   memcpy.
2. **Fake LRU.** Eviction removes an *arbitrary* key (`FxHashMap` iteration order via
   `self.chunk_cache.keys().next()`) despite being commented "LRU eviction". Recency is never tracked,
   so a hot chunk can be evicted while a cold one survives.

This is **design-driven** (a cache-internals refactor with real latitude in value type and eviction
policy — no external oracle dictates the cache shape), so it goes through OpenSpec. Its *correctness
guardrail* is oracle-driven: the fix MUST NOT change any read result — the 33-table CLI smoke / parity
suite stays green. Routing = **design**; the design is already **owner-approved** via the read-path
performance audit (standing owner Seam-1 approval, 2026-07-06 drain directive).

Milestone: **v0.14 perf wave** (M7 perf validation). Target: eliminate the per-hit chunk memcpy and make
eviction honest, on the interim end-state (the CLI is still on `BulletproofReader`; D5 has not landed).

## What Changes

- **Cache value becomes `Arc<[u8]>`.** `ChunkDecompressor`'s internal chunk cache stores each
  decompressed chunk as a reference-counted `Arc<[u8]>`. A cache hit is an `Arc::clone` (refcount bump),
  **never** a chunk-sized allocation or memcpy. The decompressed `Vec<u8>` is converted to `Arc<[u8]>`
  exactly once, on insert.
- **Real recency-tracked eviction** replaces the arbitrary-key removal. The internal cache becomes a
  capacity-bounded `lru::LruCache<usize, Arc<[u8]>>` (the CLI path is single-threaded — the decompressor
  is owned `&mut` by one `BulletproofReader` — so no sharding is needed here; contrast B1's shared cache).
  Accessing a chunk updates its recency; inserting a new chunk at capacity evicts the genuinely
  least-recently-used entry.
- **A local decompress-work counter** increments on every actual chunk decompression (miss path only), so
  a test can prove that reading the same chunk twice performs exactly one decompression.
- **No public API signature changes.** `read_data`, `read_all_data`, and `decompress_chunk_by_index`
  keep their `Result<Vec<u8>>` return types; `cache_stats()` keeps `(len, capacity)`. Only the private
  `get_decompressed_chunk` internal return type changes (`Vec<u8>` → `Arc<[u8]>`), invisible to callers.

## Non-goals

- **No change to any read RESULT.** CLI output parity is a hard guardrail; the CLI smoke suite
  (`bash test-data/scripts/smoke-test-all-tables.sh`) stays green. This is a pure performance layer over
  an immutable data source.
- **Not folding into B1's shared cache.** The audit permits a fold only once D5 (CLI off
  `BulletproofReader`) has landed; it has not. This is the interim contain-and-fix, keyed to the
  `ChunkDecompressor` type only. Growing this into the shared cache is B1's scope.
- **No sharding / concurrency machinery.** This cache is exercised single-threaded on the CLI path
  (owned `&mut` by one reader). Sharding, poison-tolerance, and byte-budget are B1's concerns.
- **No new external crate dependency** — reuses the existing `lru` crate and `Arc<[u8]>`.
- **No change to the no-heuristics posture** — the cache key is the authoritative chunk index derived
  from `CompressionInfo` offsets, never inferred from byte content.

## Impact

- **Memory:** the cache stays capacity-bounded (same 16-chunk cap as today) and now evicts by true
  recency; peak memory is unchanged, hits no longer allocate.
- **Public binding surfaces (Python/Node/CLI):** unchanged behavior; repeated chunk reads on the CLI path
  get faster and stop allocating.
- **No-heuristics mandate:** unaffected — authoritative chunk-index keying only.
