# Design — Global bounded key→partition-offset cache (#2059)

Replace the per-reader `KeyOffsetCache` (#1570) with a single process-global, byte-bounded, sharded
cache keyed on `(generation identity, raw partition key)` — Cassandra's key-cache model — so aggregate
resident memory is bounded by ONE global cap regardless of open-reader count, and a hit skips the
Summary-guided `Index.db` interval parse (#2412).

## Context / measured anchors (in-tree, this worktree)
- **Per-reader cache (today):** `KeyOffsetCache`
  (`cqlite-core/src/storage/cache/key_offset.rs`) — sharded `Mutex<Shard>`, byte-bounded per reader
  (`DEFAULT_KEY_CACHE_BYTES = 512 KiB`), keyed on `Box<[u8]>` raw partition key → `PartitionLoc { data_offset, data_size }`.
  Built per reader by `build_key_offset_cache` and stored on `SSTableReader.key_offset_cache`
  (`reader/types.rs`, `reader/mod.rs`). Its own doc-comment (D2 + the `DEFAULT_KEY_CACHE_BYTES` note)
  already names the global `(sstable_id, key)` cache as the deferred follow-up — this change.
- **Consult/populate site (BIG):** `SSTableReader::lookup_partition_with_index`
  (`reader/partition_lookup.rs`) — `key_offset_cache.get(key)` fast path → on miss, a resident raw-key
  `index_reader.lookup_partition(key)` (O(1) map), then `key_offset_cache.insert(...)`. This is where a
  hit is near-zero value TODAY (in front of an O(1) map) and real value AFTER #2412 (in front of an
  interval I/O). `data_access/big_point.rs` (`big_get_with_resolution`) owns the point path #2412 rewrites.
- **Reader identity:** `SSTableReader::open` derives `chunk_cache_id` = hash(path + generation)
  (`reader/mod.rs:820`) for the chunk cache's namespace; the flight `WarmTableRegistry`
  (`cqlite-flight/src/warm/identity.rs`, #2345) keys the authoritative generation identity on
  `(device, inode, size, generation)`, valid across snapshot hardlink dirs, never path- or TTL-keyed,
  with #2383 rebind-by-inode swapping the backing path over a byte-identical generation.
- **Warm budget (#2343, WS4):** `cqlite-flight/src/warm/budget.rs` — a fixed named byte budget inside
  `<128MB`, LRU by `(table, generation)`, removed-on-disk evicts immediately; metrics in `warm/metrics.rs`.
- **Chunk cache sibling:** `DecompressedChunkCache` (`cache/mod.rs`) is ALREADY a single shared global
  cache keyed on `ChunkKey { sstable, chunk_index, aux }` with a global byte budget — the exact shape
  this change gives the key cache. Reuse its proven sharded-`Mutex`, byte-bounded, poison-tolerant pattern.
- **#2052 hotspot class:** a single-slot `Mutex` on a concurrent point-read hot path degrades under
  concurrent same-target reads. A global cache concentrates ALL reader traffic onto one instance, so the
  shard count must be high enough that the hit path never serializes (design §F).
- **Cassandra reference:** `org.apache.cassandra.cache.AutoSavingCache` / `KeyCacheKey` — global capacity
  in bytes, entries keyed on `(sstable descriptor, key)`, invalidated on sstable removal;
  `docs/sstables-definitive-guide/chapters/06-index-and-summary.md`.

## A — Scope + key (global, `(generation identity, raw partition key)`)
**Chosen: a single process-global cache, keyed on `(generation identity, raw partition-key bytes) → PartitionLoc`.**

- **Global, not per-table.** One instance shared by every reader (like `DecompressedChunkCache`). The
  generation identity in the key namespaces per-generation entries, so one global byte budget bounds the
  whole process — Cassandra's single global key cache, not N per-table caches. A per-table cache would
  reintroduce the `N × cap` unbounded-aggregate hazard this change exists to kill.
- **Key = `(generation identity, raw partition key)`.** The generation identity is the authoritative
  inode-stable identity (#2345: device+inode+size+generation), NOT a path hash — paths rebind under
  snapshots (#2383), and the offsets are identical across a rebind, so the identity must be rebind-stable.
  The partition key is the FULL raw key bytes (collision-free by construction, mirroring #1570 D1 — hashing
  to a `u64` would admit a cross-key alias). Two generations that share a partition key never collide
  because the identity differs.
- **Value = `PartitionLoc`** (unchanged from #1570): `{ data_offset, data_size }`. BIG resolves both; BTI
  (out of scope here) would use the offset-only form. Format-agnostic so a future BTI extension is additive.
- **BTI scoped out (justified).** BTI open is O(1) and its point lookup is an in-memory trie descent
  (bounded CPU, already memoized by the #1574 single-walk memo); a key cache in front of BTI skips CPU,
  not I/O — a weaker win overlapping #1574/#2052. Wiring BTI now would be unwired-surface risk without the
  interval-I/O payoff BIG gets from #2412. The key/value shape leaves BTI trivially addable later.

## B — Capacity model (single global byte budget + eviction)
- **One fixed named global byte budget inside `<128MB`** (no new user knob — mirroring the #2343 WS4
  owner decision and the `DecompressedChunkCache` default). Byte-bounded, NOT entry-count: partition keys
  are variable-length (composite/text to ~64 KB), so only a byte budget bounds resident memory
  independent of key size — the same reasoning that drove #1570's byte-budget fix, now applied globally.
- **Eviction: LRU, sharded (recommended over CLOCK).** Reuse the proven `DecompressedChunkCache`/#1570
  discipline: each shard is an unbounded-by-count `LruCache` with running byte accounting; on insert the
  owning shard `pop_lru`s until within its per-shard byte budget, never evicting the just-inserted MRU
  entry (a single oversized entry is retained). LRU is chosen over CLOCK for parity with the two existing
  caches (one eviction mental model, proven under the concurrency tests) and because the recency signal
  matters for a point-read working set; CLOCK's only advantage (no per-get write) is moot here since LRU
  recency is already the accepted cost on the sibling caches. Documented approximate-byte accounting
  (per-entry overhead constant + key length), conservative over-estimate so the budget is never silently
  exceeded.

### One budget or two? (interaction with the #2343 warm budget) — RECOMMENDED: two caps, one envelope
The #2343 warm budget bounds **reader retention** (`Arc<SSTableReader>` per warm generation); the key
cache bounds **locations**. Recommendation: **two separately-enforced byte caps, both inside the single
`<128MB` envelope, coupled only by invalidation** — NOT one merged LRU. Rationale:
- **Incomparable weights.** A retained reader (post-#2412: summary-only, still KBs–MBs) and a 24-byte
  location have wildly different sizes; a single LRU mixing them lets reader churn evict hot locations, or
  a flood of locations evict a hot reader — neither is the intended policy.
- **Different eviction keys.** The warm budget evicts by `(table, generation)` recency (WS4); the key
  cache evicts by per-key recency. Merging forces one policy onto both.
- **Coupling is invalidation, not capacity.** When the warm registry removes/replaces a generation
  (§C), that generation's key-cache entries are invalidated — a targeted drop, not an LRU interaction.
- **Reporting:** both caps report into ONE consolidated `Database::stats().memory_stats` view (the
  process aggregate stays observable as a single envelope), even though they are enforced independently.

Post-#2412 the warm reader is cheap (summary-only), which makes spending the memory budget on locations
(this cache) the higher-value allocation — a further reason to keep the key cache's cap independent and
first-class rather than subordinate to reader retention.

## C — Invalidation (generation removal / compaction; #2383 rebind; fail-closed)
Cassandra invalidates key-cache entries on sstable removal. Here:
- **On generation removal / compaction / warm-registry evict** (the #2310 diff/swap, #2343
  removed-on-disk-evicts-immediately): drop ALL entries for that generation identity. A distinct
  `invalidations` counter records dropped entries (separate from budget `evictions`).
- **On #2383 rebind-by-inode:** the generation identity `(dev, ino, size, generation)` is STABLE across a
  path swap over a byte-identical generation, so entries REMAIN valid across a rebind — the offsets are
  byte-transparent (the rebind swaps only the backing path). No invalidation on rebind; this is the
  correctness-preserving behavior #2383 established.
- **Fail-closed on identity mismatch (the correctness guardrail).** A `get` supplies the querying
  reader's CURRENT generation identity; an entry keyed on a different identity is a MISS, never a hit.
  So a stale entry from a removed/replaced generation can never serve a location for a generation that no
  longer holds it — a hit returns the EXACT location a fresh lookup on THAT generation resolves, or nothing.
  This is parity-anchored: the invalidation-correctness scenario proves a removed generation's entry never
  serves rows.

## D — Sequencing vs #2412 (RECOMMENDED: land #2059 AFTER #2412)
The value analysis (proposal Problem) drives this:
- **Latency value is contingent on #2412.** Pre-#2412 the point lookup resolves from a resident O(1) map;
  a cache hit skips an O(1) map get + a probe counter — near-zero. Post-#2412 the point lookup reads+parses
  one `Index.db` interval (real disk I/O); a hit skips that interval read entirely. The cache only earns
  its latency keep once it fronts the interval parse.
- **Public-path reach comes from #2412.** #2412's §B rewrite replaces the whole-file `scan_for_key` with
  the Summary-guided interval resolution that is this cache's consult/populate site — resolving the #1818
  BIG-path dead-cache prerequisite (proposal "Relationship to #1818").
- **#2412 reshapes the memory target.** #2412 removes the ~500MB/generation resident index, leaving the
  key cache as the primary tunable resident structure — the aggregate-cap this change bounds lands on the
  post-#2412 memory picture, not the doomed resident-index one.

**Recommendation: sequence #2059 AFTER #2412** (depend on it). Building the global cache's point-lookup
wiring against the soon-removed resident-index path is throwaway; the interval-parse fast path is the
correct, durable consult site. The **memory-bounding requirement has standalone value** and could land
pre-#2412, but with near-zero latency value and a point-path wiring that #2412 rewrites — NOT recommended.
The owner confirms the order at Seam 1. The spec's work-probe scenario (§AC "hit skips the interval
parse") is written against the #2412 interval-parse counter (`cqlite.sstable.index_interval_parses_total`)
and is the pin the post-#2412 landing turns green.

## E — Relationship to #1818 (BIG point-read dead-cache site)
Disjoint mechanism, shared prerequisite (proposal). #2059 does not touch routing; #2412's point rewrite
provides the public-path reach #1818 flags for BIG. So #2059 **depends on #2412** (which resolves the BIG
portion of #1818 as a side effect); it neither subsumes #1818 nor depends on it narrowly. If #2412 is not
yet merged when #2059 starts, #2059 blocks on it (the wiring-evidence scenario cannot pass without the
public BIG point path reaching the cache).

## F — Concurrency (sharded; NOT the #2052 mutex hotspot)
- **Sharded `Mutex<Shard>`, poison-tolerant**, power-of-two shard count masked from a `DefaultHasher` of
  the key — the proven `DecompressedChunkCache`/#1570 pattern. The hit path locks exactly ONE shard, does
  a `LruCache::get` (recency bump ⇒ `Mutex`, not `RwLock`) + refcount-free `PartitionLoc` copy, unlocks.
  Never a process-wide lock.
- **Higher shard count than the per-reader default.** A single global cache concentrates ALL reader
  traffic onto one instance, so the shard count is raised (recommend 64 or 128, tuned so per-shard
  contention under the target concurrent-`do_get` fan-out is ≤ the per-reader baseline) — the explicit
  #2052-class mitigation. A concurrency-soundness scenario (N threads, overlapping + disjoint keys, under
  eviction pressure) proves correctness and no single-shard serialization.
- **Lock-free considered, deferred.** An atomic `(key-hash, offset)` slot (the #2052 direction) is
  rejected here: it cannot hold variable-length keys collision-free (the #1570 D1 guardrail), and the
  sharded-`Mutex` already keeps contention `1/shards`. Documented as the accepted tradeoff; if a bench
  shows per-shard contention regressing, raising the shard count is the first lever.

## G — Metrics (`cqlite.`-namespaced)
A single global snapshot replaces the per-reader `KeyCacheSnapshot` aggregation:
- `hits`, `misses`, `evictions` (budget-driven), **`invalidations`** (generation-removal drops, distinct
  from evictions), `resident_bytes`, `capacity_bytes` — all real counters/gauges, `cqlite.`-namespaced,
  catalog-registered, reported via `Database::stats().memory_stats`. No fabricated placeholders (a counter
  not observed is an error, never a 0). A disabled cache (honoring `block_cache.enabled == false`) is a
  genuine no-op reporting honest zeros, mirroring the sibling caches.

## Sequencing (one branch, staged; after #2412)
1. `GlobalKeyOffsetCache` (global, sharded, byte-bounded, generation-identity-keyed, invalidatable) +
   unit pins (eviction, byte-bound, no-alias-across-generations, invalidation, poison recovery,
   concurrency soundness). Retire the per-reader `KeyOffsetCache` construction.
2. Wire readers to consult/populate the global cache with their generation identity at the #2412
   interval-parse point-lookup site; work-probe pin (hit skips the interval parse).
3. Invalidation hooks on generation removal / warm-registry evict / compaction; rebind-stability pin.
4. Metrics: global snapshot into `Database::stats().memory_stats`; catalog registration/namespacing.
5. Flight `do_get` cold+warm e2e wiring evidence (cold miss populates, warm hit skips the interval parse,
   rows match the query-semantics oracle).

## Risks
- **Correctness across generations** — mitigated by the generation-identity key + fail-closed mismatch;
  parity-anchored invalidation scenario.
- **#2052-class contention on one global instance** — mitigated by high shard count + a concurrency
  scenario; lock-free deferred with a documented tradeoff.
- **Landing before #2412 wastes point-path wiring** — mitigated by the §D recommendation (sequence after)
  and the owner's Seam-1 confirmation.
