## 1. TDD — write failing tests first (in `chunk_decompressor.rs` `mod tests`)

- [ ] 1.1 Zero-copy hit: `get_decompressed_chunk` twice for one index → `Arc::ptr_eq` handles
- [ ] 1.2 Decompress-once via `read_data`: same chunk range read twice → decompress counter delta == 1
- [ ] 1.3 Eviction order: capacity 2, access A, B, A, insert C → B re-decompresses, A & C are hits
- [ ] 1.4 Capacity bound: reading more distinct chunks than capacity keeps `cache_stats().0 <= capacity`
- [ ] Confirm 1.1–1.3 are RED on current code (clone-on-hit / arbitrary eviction / no counter)

## 2. Implement (`cqlite-core/src/storage/sstable/chunk_decompressor.rs`)

- [ ] 2.1 Cache field: `FxHashMap<usize, Vec<u8>>` → `lru::LruCache<usize, Arc<[u8]>>` at fixed capacity
      (const `NonZeroUsize`, no runtime `unwrap()`/`expect()`)
- [ ] 2.2 `get_decompressed_chunk` returns `Arc<[u8]>`; hit = `Arc::clone`; insert converts `Vec` once via
      `Arc::from(v.into_boxed_slice())` and uses `LruCache::put` (real LRU eviction)
- [ ] 2.3 Add private `decompress_calls` counter incremented in `decompress_chunk`; expose read accessor
      for tests
- [ ] 2.4 Update in-file callers: `read_data` reads slice off `Arc<[u8]>`; `decompress_chunk_by_index`
      keeps `Vec<u8>` via explicit `.to_vec()`; `cache_stats()` uses `LruCache::len`/`cap`
- [ ] 2.5 No public signature changes (`read_data`/`read_all_data`/`decompress_chunk_by_index`/`cache_stats`)

## 3. Validate

- [ ] 3.1 Tests green; no `unwrap()`/`expect()` in library code
- [ ] 3.2 FAST iteration gate (`scripts/agent-gate.sh --lite`) → RESULT: PASS on each fix round
- [ ] 3.3 CLI smoke read parity unchanged (spot-check; full smoke in the gate of record)

## 4. Sign-off gates

- [ ] 4.1 Full `scripts/agent-gate.sh` PASS (run once by the lead/orchestrator, not the implementer)
- [ ] 4.2 Intent audit **C** (`spec-auditor`) PASS against `specs/chunk-decompressor-cache/spec.md`
- [ ] 4.3 roborev clean (`--base origin/main`)
