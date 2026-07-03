# Tasks — decompressed-chunk-cache (issue #1567, B1)

## 1. TDD tests first (write RED against current main, paste red run in PR)
- [ ] 1.1 Unit test (local cache instance, deterministic): **eviction order** — cache budget holds 2
      equal chunks; access A, B, A, insert C → assert B evicted, A + C resident.
- [ ] 1.2 Unit test: **byte budget** — insert N distinct chunks exceeding the budget → assert resident
      bytes never exceed budget after each insert; total entries bounded.
- [ ] 1.3 Unit test: **zero-copy hit** — insert a chunk, get it twice → assert `Arc`/pointer identity
      equal to the inserted buffer (no chunk-sized allocation on hit).
- [ ] 1.4 Unit test: **concurrency soundness** — many threads read overlapping + disjoint keys under
      eviction pressure → every buffer complete/correct, resident bytes ≤ budget, no panic (drive a
      poisoned-lock recovery path if feasible).
- [ ] 1.5 Integration test (real fixture, `CQLITE_DATASETS_ROOT`): **zero-work repeat read** — open a
      multi-chunk fixture, do the same point read twice; reset `DECOMPRESS_CALLS` + the byte-source read
      counter before the 2nd read → assert decompress delta == 0 AND underlying reads == 0 AND identical
      result. Skip-not-fail when fixture absent; never a silent 0-row pass. Must FAIL on current main.
- [ ] 1.6 Integration test: **per-site wiring** — prove BIG point-read, BTI point-read, and windowed
      scan each serve a repeat read from the cache (decompress delta == 0 per site).
- [ ] 1.7 Integration test: **scan larger than cache** — scan a table whose decompressed size exceeds a
      small configured budget → all rows correct AND cache resident bytes stay within budget (eviction
      under scan pressure).

## 2. Cache module (new)
- [ ] 2.1 New module `cqlite-core/src/storage/cache/mod.rs` (or `chunk_cache.rs`): `DecompressedChunkCache`
      — sharded `[Mutex<Shard>; N]`, each `Shard { lru: lru::LruCache<ChunkKey, Arc<[u8]>>, current_bytes,
      budget_bytes }`; `ChunkKey { sstable: u64, chunk_index: u64 }`. API: `with_budget_bytes(usize)`,
      `get(&ChunkKey) -> Option<Arc<[u8]>>`, `get_or_insert_with(key, || Result<Vec<u8>>) ->
      Result<Arc<[u8]>>` (or `get` + `insert`), `resident_bytes()`, minimal hit/miss counters for tests.
- [ ] 2.2 Bytes-bounded LRU eviction per shard (`pop_lru` until within `budget_bytes`); recency updated
      on `get`. Poison-tolerant locking (`lock().unwrap_or_else(|e| e.into_inner())`) — NO
      `unwrap()`/`expect()` in library code.
- [ ] 2.3 Zero-copy: values `Arc<[u8]>`; insert converts `Vec<u8>` once; hit is `Arc::clone`.

## 3. Decompress-work counter
- [ ] 3.1 Add `DECOMPRESS_CALLS: AtomicU64` mirroring `SCAN_FOR_KEY_CALLS` (`data_access/model.rs:167`)
      + `reset()` + getter accessor on `SSTableReader` (or a free fn). NOT `cfg(test)`-gated.
      Incremented once per actual `Compression::decompress` at the wired sites (miss only).
- [ ] 3.2 A counting byte-source test double (or a read counter at the site) so the repeat-read test can
      assert 0 underlying reads.

## 4. Ownership + wiring (production read path)
- [ ] 4.1 `SSTableManager` holds `chunk_cache: Arc<DecompressedChunkCache>`, sized from
      `config.memory.block_cache.max_size` (256MB default). Construct in `SSTableManager::new`.
- [ ] 4.2 Add `SSTableReader::open_with_cache(path, config, platform, cache)`; make existing `open(...)`
      delegate with a fresh cache (back-compat). Store `chunk_cache` on the reader. `SSTableManager` uses
      `open_with_cache` with its shared instance.
- [ ] 4.3 Wire `get_cached_data` (`data_access/mod.rs` ~470): key `(sstable, block_offset)` → cache get →
      on miss read+decompress (+count) → insert → return.
- [ ] 4.4 Wire windowed scan fill (`scan_stream_windowed.rs` ~554): key `(sstable, chunk_index)`; preserve
      the incompressible-raw fallback (`len >= max_compressed_length` chunks are stored/returned as-is —
      decide whether to cache raw-passthrough chunks; document the choice).
- [ ] 4.5 Wire BTI target-chunk read (`bti.rs` ~638): key `(sstable, target_chunk)`.
- [ ] 4.6 CRC ordering: keep each site's existing CRC verification on the miss path before insert; a hit
      returns already-verified bytes.

## 5. Bench
- [ ] 5.1 Add `read/point_lookup_repeated` to `cqlite-core/benches/read.rs`: cold read then repeated read
      of the same key; assert (or document) 2nd-read median ≥10× cold. Add to `benches/perf-gate.json`.

## 6. Validation
- [ ] 6.1 33-table parity + smoke green (`env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets`) — byte-for-byte
      unchanged.
- [ ] 6.2 `RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets` clean (sibling crates too).
- [ ] 6.3 `scripts/agent-gate.sh` PASS — paste the AGENT-GATE SUMMARY block verbatim.
- [ ] 6.4 A4 dhat memory lane within the <128MB budget with the cache enabled.
- [ ] 6.5 Update `cqlite-core/benches/README.md` / module docs to mention the repeated-read bench and the
      cache. (Doctrine: no user-facing behavior change; the cache is internal.)
