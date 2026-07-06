# Tasks — key-partition-offset-cache (issue #1570, B4)

## 1. TDD tests first (write RED, then implement to green)
- [ ] 1.1 Unit test (local instance, single shard): eviction order — capacity 2; insert A, insert B,
      access A, insert C → assert B evicted, A + C resident.
- [ ] 1.2 Unit test: entry-count bound — insert N > capacity distinct keys → resident count never
      exceeds capacity after each insert.
- [ ] 1.3 Unit test: hit returns the stored `(offset,size)`; a different key never aliases; a
      never-inserted key misses (parity / no-fabrication).
- [ ] 1.4 Unit test: `disabled()` is a genuine no-op (get misses, insert retains nothing, occupancy
      + counters zero).
- [ ] 1.5 Unit test: poison recovery — panic while holding a shard lock; subsequent get/insert still
      work.
- [ ] 1.6 Counter unit test: `INDEX_PROBES` round-trips through the local `Counters` add/get/reset
      contract (distinct multiplicity vs the other counters).

## 2. Implement the cache
- [ ] 2.1 New `cqlite-core/src/storage/cache/key_offset.rs`: `PartitionLoc { data_offset, data_size }`,
      `KeyOffsetCache` (sharded `Mutex<LruCache<Box<[u8]>, PartitionLoc>>`, power-of-two shards,
      poison-tolerant lock, `with_capacity` / `with_capacity_and_shards` / `disabled`, `get` / `insert`
      / `len` / `is_empty` / `capacity` / hit+miss counters). `DEFAULT_KEY_CACHE_ENTRIES`,
      `DEFAULT_KEY_CACHE_SHARDS`. Declare `pub mod key_offset;` + re-export in `cache/mod.rs`.
- [ ] 2.2 Add `INDEX_PROBES` to `read_work_counters.rs` (field + `record_index_probe` + `index_probes`
      getter + reset + round-trip test line), same cfg-gated zero-in-release pattern.

## 3. Wire into the point-read path
- [ ] 3.1 Reader field `key_offset_cache: Arc<KeyOffsetCache>` (`types.rs`), built at `open_inner`
      honoring `config.memory.block_cache.enabled` via a `build_key_offset_cache(config)` helper in
      `sstable/mod.rs`.
- [ ] 3.2 BIG — `lookup_partition_with_index`: cache get → on miss `record_index_probe()` + probe →
      insert on a present hit → return. A cache hit skips the probe.
- [ ] 3.3 BTI — `lookup_partition_via_bti_trie`: cache get → on miss descend (`record_trie_walk`) →
      insert offset on a present resolution → return. A cache hit skips the descent and still emits
      the presence counters.

## 4. Wiring-evidence integration tests (`work-counters` feature)
- [ ] 4.1 `tests/issue_1570_key_offset_cache.rs`: BTI (`test_da/simple_table`) repeated interleaved
      point read → `TRIE_WALKS == 0` on the second read of the first key; BIG
      (`test_basic/simple_table`) `lookup_partition_with_index` twice → `INDEX_PROBES == 0` on the
      second call and identical `(offset,size)`. Data.db-present skip guard, `#[serial]`.
- [ ] 4.2 Add `--test issue_1570_key_offset_cache` to the `work-counters-guard` component in
      `scripts/agent-gate.sh`.

## 5. Gate + validate
- [ ] 5.1 `openspec validate key-partition-offset-cache --strict` clean.
- [ ] 5.2 `cargo +1.88.0 fmt` + `fmt --check` clean.
- [ ] 5.3 `scripts/agent-gate.sh --lite` PASS each fix round; minimal-features build compiles.
