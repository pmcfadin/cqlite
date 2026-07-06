## Context

`ChunkDecompressor` (`cqlite-core/src/storage/sstable/chunk_decompressor.rs`) decompresses SSTable
compression chunks on demand for the CLI read path (via `BulletproofReader`, which owns an
`Option<ChunkDecompressor>` by value and calls it `&mut`). It keeps a small hot-chunk cache. Today that
cache is `FxHashMap<usize, Vec<u8>>` with two defects: a hit clones the whole `Vec<u8>` out, and eviction
at capacity removes `keys().next()` (arbitrary hash-map order), not the least-recently-used entry.

B1 (#1567) already built a separate, shared, sharded, byte-bounded `DecompressedChunkCache`
(`storage/cache/mod.rs`) with `Arc<[u8]>` values and real LRU. That is the *query-engine* cache. B3 is
scoped to the *CLI-path* `ChunkDecompressor` cache only — the audit says fold into B1 only after D5, which
has not landed, so this is the interim fix.

## Goals / Non-Goals

- **Goal:** a cache hit is a refcount bump, never a chunk-sized copy.
- **Goal:** eviction removes the genuinely least-recently-used chunk.
- **Goal:** prove both with deterministic, single-threaded tests (no wall-clock, no shared globals).
- **Non-goal:** sharding, byte-budget, poison-tolerance, concurrency (B1's shared cache owns those).
- **Non-goal:** any change to public method signatures or read results.

## Decisions

### D1 — Value type: `Arc<[u8]>`

The cache stores `Arc<[u8]>`. On insert, the decompressed `Vec<u8>` is converted **once** via
`Arc::from(vec.into_boxed_slice())`. `get_decompressed_chunk` returns `Arc<[u8]>`; a hit returns
`Arc::clone(&cached)` (a refcount bump). Callers that need an owned `Vec` (the cold
`decompress_chunk_by_index` parity path) call `.to_vec()` explicitly, so the copy is opt-in and off the
hot hit path. `read_data` reads the requested sub-range straight off the `Arc<[u8]>` slice (`Deref` to
`[u8]`) — it copies only the caller-requested bytes into the output, never the whole cache entry.

**Rationale:** `Arc<[u8]>` is the minimal shared, immutable, heap-backed buffer; matches B1's chosen value
type; no `Bytes` dependency needed. The chunk is immutable once decompressed, so sharing is sound.

### D2 — Eviction: `lru::LruCache<usize, Arc<[u8]>>` (capacity-bounded, real recency)

Replace the `FxHashMap` + arbitrary-key removal with `lru::LruCache` at a fixed capacity (the existing
16-chunk cap). `LruCache::get(&mut self, k)` bumps recency; `LruCache::put` evicts the true LRU entry when
at capacity. `lru` is already a `cqlite-core` dependency (used by B1). Capacity is a compile-time constant
converted to `NonZeroUsize` in a `const` context (no runtime `unwrap()`/`expect()` — library code must be
panic-free): `const CHUNK_CACHE_CAPACITY: NonZeroUsize = match NonZeroUsize::new(16) { Some(n) => n, None
=> NonZeroUsize::MIN };`.

**Single-threaded justification:** `ChunkDecompressor` is owned by one `BulletproofReader` and every entry
point (`read_data`, `read_all_data`, `decompress_chunk_by_index`) takes `&mut self`. `LruCache::get`
requiring `&mut self` is therefore free here — no `Mutex`/`RwLock` needed, unlike B1's shared cache. This
is explicitly the interim, contained end-state (not the shared cache).

### D3 — Decompress-work counter

Add a private `decompress_calls: u64` field, incremented at the top of `decompress_chunk` (the miss path
that performs an actual decode). It is not incremented on a cache hit. The in-module test reads it
directly to assert "same chunk read twice → exactly one decompression". No process-global atomic is needed
(the counter is per-decompressor and the test owns the instance) — this avoids the cross-test global-race
class (#1071 lesson).

## Test strategy (TDD — red on current `main`)

All tests are in-module (`chunk_decompressor.rs` `mod tests`), so they can call the private
`get_decompressed_chunk` and read the private counter directly. They reuse the existing
`build_multichunk_lz4` synthetic-fixture helper and are fully deterministic (single-threaded, no
wall-clock).

1. **Decompress-once (real read path):** build a multi-chunk LZ4 fixture, `read_data` the same chunk's
   byte range twice; assert the decompress counter delta is exactly 1 (hit on the second read). *Red
   today:* the counter does not exist / the hit clones but still the second read is a hit — the new
   assertion is the counter existence + delta.
2. **Zero-copy hit (Arc pointer identity):** call `get_decompressed_chunk` twice for the same index;
   assert `Arc::ptr_eq` of the two handles. *Red today:* the fn returns `Vec<u8>` clones (no `Arc`,
   different allocations).
3. **Eviction order (A,B,A,C at capacity 2 evicts B):** construct a decompressor with capacity 2, access
   A, B, then A again (bump A's recency), then C; assert B is gone (a subsequent read of B re-decompresses
   → counter increments) while A and C are hits (counter unchanged). *Red today:* arbitrary-key eviction
   may drop A instead of B.

## Risks / Trade-offs

- **`decompress_chunk_by_index` cold copy:** it now does one `.to_vec()` to preserve its public `Vec<u8>`
  signature. This path is per-chunk parity/verification only (not the hot query path), so the extra copy
  is acceptable and keeps the public API unchanged. Accepted.
- **File-size ratchet:** `chunk_decompressor.rs` is near the source threshold; the added tests grow it.
  Acknowledged via `CQLITE_ALLOW_FILE_GROWTH=1` for this contained fix (a full split is out of scope; the
  file is one cohesive decompressor). Noted for #1116.
