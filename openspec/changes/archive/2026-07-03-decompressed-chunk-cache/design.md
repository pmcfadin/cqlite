# Design — decompressed-chunk-cache (issue #1567, Epic B / B1)

## Context

Anchors verified on `main` via the read-path map (2026-07-03):
- BIG point-read fetch: `cqlite-core/src/storage/sstable/reader/data_access/mod.rs` `get_cached_data`
  (lines ~470–511). Reads `(block_offset, size)` from `self.file` (Arc<Mutex<BlockSource>>), then
  `Compression::new(*compression_reader.algorithm())?.decompress(&buffer)`. Keyed by raw `block_offset`
  (NOT chunk-aligned — this path reads an index-resolved byte region, decompresses it whole).
- Windowed scan fill: `reader/scan_stream_windowed.rs` `drain_scan_window_blocking` (lines ~554–570),
  runs on the blocking parse task; incompressible-raw fallback when `len >= max_compressed_length`;
  chunk index via `ScanCursor.chunk_index`.
- BTI target-chunk read: `data_access/bti.rs` `bti_decompress_and_parse_target` (lines ~638–668);
  `target_chunk = offset / chunk_length` (`bti_chunk_target`, ~524–530).
- Decompress primitive all three funnel through: `compression.rs` `Compression::decompress(&self, &[u8])
  -> Result<Vec<u8>>` (line ~243).
- No A5 `DECOMPRESS_CALLS` counter has landed. The blessed idiom to copy is
  `SCAN_FOR_KEY_CALLS: AtomicU64` (`data_access/model.rs:167`) with an `SSTableReader` accessor
  (`bti.rs:31-33`) — NOT `cfg(test)`-gated (integration tests compile the lib without the `test` cfg).
- Existing dead caches (B2's problem, out of scope here): per-reader `block_cache: HashMap` (never
  inserted) and `MemoryManager.block_cache: Arc<RwLock<BlockCache>>` whose `get_block` takes the WRITE
  lock on every read (`memory/mod.rs:129`) — the exact anti-pattern to avoid.
- Config budget already exists: `config.memory.block_cache.max_size` defaults to 256MB (`config.rs:297`).
- Tightest owner of all readers: `SSTableManager` (`storage/sstable/mod.rs:222-253`) —
  `readers: Arc<RwLock<HashMap<SSTableId, Arc<SSTableReader>>>>`, holds `config: Config`.

## Goals / non-goals

Goals: a shared, bytes-bounded, sharded, zero-copy-hit decompressed-chunk cache wired into all three
read sites; wiring-evidence via a decompress counter; parity unchanged; <128MB respected.

Non-goals: removing the dead caches (B2), the key/offset cache (B4), the full observability surface
(B5), a new external cache crate, any change to read results.

## Decisions

### D1 — Cache scope: per-`Database`/per-`SSTableManager`, not process-global
The issue allows "process-wide (or per-`Database`)". For a **library** consumed by embedders (Python,
Node, Flight, multiple `Database` instances in one process), a process-global cache is an anti-pattern:
independent datasets would fight over one budget and one lock namespace, and tests would race on shared
mutable budget state (the #1071 lesson). Decision: own one `Arc<DecompressedChunkCache>` on
`SSTableManager`, sized from `config.memory.block_cache.max_size` (256MB default), and clone the `Arc`
into every `SSTableReader` at construction so all readers of a dataset share it.

Wiring minimization: add `SSTableReader::open_with_cache(path, config, platform, cache)` and make the
existing `open(...)` delegate with a freshly-constructed cache (back-compat for existing callers and
sibling crates); `SSTableManager` calls `open_with_cache` with its shared instance so the production
read path shares. The reader stores `chunk_cache: Arc<DecompressedChunkCache>`.

The decompress-work **counter** stays process-global (it is pure instrumentation, like
`SCAN_FOR_KEY_CALLS`); unit tests that assert absolute cache behavior use a *local* cache instance for
determinism, while the wiring/integration test resets the global counter around its two reads.

### D2 — Concurrency: hand-sharded `Mutex<LruCache>` over the tested `lru` crate (no new dep)
Owner decision #1 blesses reusing the tested LRU internals, hand-sharded. Structure:
`shards: [Mutex<Shard>; N]` (N a fixed power of two, e.g. 16). Each `Shard` holds an
`lru::LruCache<ChunkKey, Arc<[u8]>>` used **unbounded by count** plus a running `current_bytes: usize`
and the per-shard `budget_bytes = total_budget / N`. Shard selection = `hash(key) & (N-1)`. The hit path
locks exactly one shard, calls `lru.get` (which mutates recency — hence a `Mutex`, not an `RwLock`; but
sharded so contention is 1/N, never global). This satisfies "sharded, no global lock on the hit path"
while avoiding a new dependency and reusing code the repo already trusts.

Rejected: `RwLock<LruCache>` (degrades to a global Mutex — the `memory/mod.rs:129` bug). Rejected:
adding `moka`/`quick_cache` (new external dep; the hand-shard is sufficient and blessed).

Lock hygiene: use `Mutex` and handle poisoning by recovering the guard (`lock().unwrap_or_else(|e|
e.into_inner())`) so one panicking thread cannot poison the whole cache into panics for everyone — no
`unwrap()`/`expect()` in library code; the read path degrades to a correct recompute at worst, never a
panic.

### D3 — Value type: `Arc<[u8]>` for true zero-copy hits
Value is `Arc<[u8]>`. Insert converts the decompressed `Vec<u8>` once (`Arc::from(vec)` /
`vec.into()`); a hit is `Arc::clone` (refcount bump). The three sites currently return/consume
`Vec<u8>` — on a hit they get an `Arc<[u8]>` and use it by slice (`&chunk[..]`); where a site truly
needs an owned `Vec` (e.g. `window.extend_from_slice(&chunk)` copies anyway into the sliding window),
the cache still saved the *decompress*, which is the dominant cost. The zero-copy guarantee is asserted
at the cache API boundary (pointer identity), which is where "hit = refcount bump, never memcpy" is
meaningful.

### D4 — Key: authoritative `(sstable id, chunk_index)`
`ChunkKey { sstable: u64 /* stable hash of file_path (+generation) */, chunk_index: u64 }`. The two
chunk-aligned sites (windowed scan, BTI) key by their real `chunk_index`. The BIG point-read
`get_cached_data` reads an index-resolved `(block_offset, size)` region that is not chunk-aligned; it
keys by `block_offset` in the same `chunk_index` field slot (an authoritative, index-derived offset —
not guessed). Per-site key namespaces are acceptable: the acceptance criterion is that each site
*consults the shared cache* and repeat reads hit (proven per-site by the counter), not that a physical
chunk shares one key across differently-granular sites. All key components come from authoritative
reader state / `CompressionInfo`, never from decompressed byte content — no-heuristics preserved.

### D5 — Decompress counter (wiring evidence)
Add `DECOMPRESS_CALLS: AtomicU64` (mirror `SCAN_FOR_KEY_CALLS`) incremented once per *actual*
`Compression::decompress` call at the three wired sites (i.e. only on a miss, after the cache lookup
fails), with `reset()` + a getter. Reused as the TDD oracle: `delta == 0` on a repeat read proves the
hit skipped decompression. For "0 underlying reads", wrap the byte source (or count at the read site)
so the test proves the cached read touched the file zero times.

## Risks / mitigations
- **Correctness (wrong bytes on hit):** keys include full sstable identity + chunk index; SSTables are
  immutable so a resident entry is always valid until evicted. Parity harness is the guardrail.
- **Memory blowup under scan:** bytes-bounded eviction per shard; A4 dhat lane + the "scan larger than
  cache" test guard it.
- **Concurrency:** poison-tolerant per-shard `Mutex`; no global lock; bounded growth by construction.
- **CRC ordering:** insert the decompressed buffer only after the site's existing CRC verification runs
  on a miss; a hit returns already-verified bytes (verified when first inserted) — do not skip a
  verification the site performs today.

## Migration / rollout
Additive. No config schema change (reuses `block_cache.max_size`). No public API change. Dead caches
untouched (B2). `open` back-compat preserved via the delegating constructor.
