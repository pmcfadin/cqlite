# global-key-offset-cache

## ADDED Requirements

### Requirement: A single process-global cache bounds aggregate key-offset memory independent of reader count

The per-reader `KeyOffsetCache` (#1570) SHALL be replaced by ONE process-global, byte-bounded cache
shared by every open `SSTableReader`. Aggregate resident key-offset memory SHALL be bounded by a single
fixed global byte budget (inside the `<128MB` envelope, no new user knob), independent of how many
readers are concurrently open — NOT by `N_readers × per_reader_cap`. Each cache entry SHALL be weighed by
its approximate resident byte cost (key length + a documented per-entry overhead), and after an insert the
owning shard SHALL evict least-recently-used entries until it is within its per-shard byte budget, never
evicting the just-inserted entry.

#### Scenario: Aggregate footprint stays bounded as reader count grows

- **GIVEN** the global key cache configured with a fixed byte budget `B`
- **WHEN** entries are populated from many (`N`) distinct open readers/generations, far more than `B` can
  hold
- **THEN** the cache's total resident bytes stay `<= B` after every insert (measured via the cache's
  resident-bytes accessor), regardless of `N`
- **AND** the aggregate footprint does NOT scale with the number of open readers.

#### Scenario: Byte budget is key-size aware, not entry-count

- **GIVEN** the global cache sized for a small number of large-key entries
- **WHEN** more large-key entries are inserted than the byte budget holds
- **THEN** the least-recently-used large-key entries are evicted so resident bytes stay within budget
- **AND** a workload of small keys retains proportionally more entries under the same budget (the bound is
  bytes, not a fixed entry count).

### Requirement: Entries are keyed on authoritative generation identity plus the raw partition key (no-heuristics, collision-free)

Each entry SHALL be keyed on the tuple `(generation identity, raw partition-key bytes)`, where the
generation identity is the authoritative inode-stable identity (device + inode + size + generation number,
#2345) — never a path hash — and the partition key is the FULL raw key bytes (never a lossy hash). A hit
SHALL return the EXACT `PartitionLoc` a fresh index lookup on that generation resolves. No key component
SHALL be inferred from partition or cell byte content.

#### Scenario: The same partition key in two generations does not alias

- **GIVEN** two distinct generations (distinct generation identities) that both contain a partition key
  `K` resolving to different `Data.db` offsets
- **WHEN** `K` is looked up against each generation identity in turn
- **THEN** each lookup returns that generation's own offset (the identity component prevents a cross-
  generation alias)
- **AND** a lookup of `K` against a third, never-inserted generation identity misses.

#### Scenario: No key component is inferred from value bytes

- **WHEN** an entry is inserted for a resolved partition
- **THEN** the generation identity comes from the reader's authoritative `(device, inode, size,
  generation)` and the key from the raw partition-key bytes the index is itself keyed on — with no
  component derived from partition or cell byte patterns.

### Requirement: A cache hit skips the Summary-guided Index.db interval parse (post-#2412 work-probe)

Once BIG open is Summary-guided-lazy (#2412), a point lookup that HITS the global cache SHALL resolve the
partition location without reading or parsing any `Index.db` interval; a MISS SHALL read/parse exactly one
`Index.db` interval and then populate the cache. The number of `Index.db` interval parses attributable to
a lookup SHALL be 0 on a hit and exactly 1 on a miss (scale-free: independent of partition count). This
requirement is sequenced to land after #2412 (design §D); its work-probe is written against the #2412
interval-parse counter.

#### Scenario: A repeated present-key point read hits and touches zero interval parses

- **GIVEN** a BIG SSTable served through the lazy Summary-guided path (#2412), a partition key known
  present, and the interval-parse counter (`cqlite.sstable.index_interval_parses_total`) reset
- **WHEN** the same partition is fetched twice via the public read path
- **THEN** the first fetch increments the interval-parse counter by exactly 1 (miss → one interval parse →
  populate) and the second increments it by 0 (hit → interval parse skipped)
- **AND** both fetches return the byte-identical partition matching the physical-dump golden.

### Requirement: Entries are invalidated on generation removal and fail closed on identity mismatch

All cache entries for a generation identity SHALL be invalidated when that generation is removed,
compacted away, or evicted from the flight `WarmTableRegistry` (recorded by a distinct invalidations
counter, separate from budget evictions). A `get` SHALL supply the querying reader's CURRENT generation identity;
an entry keyed on a different identity SHALL be treated as a MISS (fail-closed), so a stale entry can never
serve a location for a generation that no longer holds it. Entries SHALL remain valid across a #2383
rebind-by-inode (a path swap over a byte-identical generation whose identity is unchanged).

#### Scenario: A removed generation's entry never serves rows

- **GIVEN** a generation with cached partition locations, then removed/compacted away
- **WHEN** the generation is invalidated and a subsequent lookup is made
- **THEN** no cached entry for the removed generation identity is served (the lookup misses or resolves
  against the surviving generations only), so the post-reconciliation `SELECT` result at a pinned `now`
  matches the query-semantics oracle (no rows from the removed generation)
- **AND** the invalidations counter reflects the entries dropped for that generation.

#### Scenario: A rebind-by-inode over a byte-identical generation keeps entries valid

- **GIVEN** cached locations for a generation whose backing path is then swapped by a #2383 rebind, with
  the generation identity `(device, inode, size, generation)` unchanged
- **WHEN** the same key is looked up after the rebind
- **THEN** the cached location is still served (a hit), returning the byte-identical partition (the offsets
  are transparent across the path swap; no invalidation occurs on a rebind).

#### Scenario: A mismatched generation identity is a miss, not a stale hit

- **GIVEN** an entry keyed on generation identity `G1`
- **WHEN** a lookup supplies a different current identity `G2` for the same raw key
- **THEN** the lookup misses (fail-closed) rather than returning `G1`'s offset.

### Requirement: The global cache is sharded and does not become a process-wide lock hotspot

The cache SHALL be hand-sharded (power-of-two shard count, selected by masking a hash of the key) so a
single hit/insert locks exactly ONE shard, never a process-wide lock, even though all readers share one
instance. The shard count SHALL be sized for the concurrent-`do_get` fan-out so per-shard contention does
not regress the per-reader baseline (the #2052-class mitigation). Locks SHALL be poison-tolerant so one
panicking thread cannot wedge the cache. Under concurrent access the cache SHALL never return a torn or
another key's location.

#### Scenario: Concurrent readers under eviction pressure stay correct with no process-wide lock

- **GIVEN** the global cache with a byte budget small relative to the working set and multiple shards
- **WHEN** many threads concurrently look up and populate overlapping and disjoint `(generation, key)`
  pairs under continuous eviction
- **THEN** every returned location is the exact one inserted for that `(generation, key)` (never another
  key's offset, never torn), resident bytes stay within budget, and no thread panics
- **AND** the hit path acquires only a single shard lock (no process-wide lock is taken).

#### Scenario: A poisoned shard lock recovers

- **GIVEN** a shard whose mutex was poisoned by a panicking thread
- **WHEN** a subsequent get/insert targets that shard
- **THEN** the operation recovers the guard and completes (the cache is not wedged).

### Requirement: Real, cqlite-namespaced observability counters

The global cache SHALL expose real hits, misses, evictions (budget-driven), invalidations
(generation-removal drops, distinct from evictions), resident bytes, and capacity bytes — all
`cqlite.`-namespaced, catalog-registered, and reported through `Database::stats().memory_stats` as a single
consolidated envelope. Every counter SHALL be a real observed value (never a fabricated placeholder). A
disabled cache (honoring `block_cache.enabled == false`) SHALL be a genuine no-op reporting honest zeros.

#### Scenario: Counters reflect real activity and are catalog-registered

- **GIVEN** the global cache with counters reset
- **WHEN** a sequence of inserts, hits, misses, budget evictions, and a generation invalidation occur
- **THEN** the hits/misses/evictions/invalidations/resident-bytes/capacity-bytes snapshot reflects the
  real activity (evictions and invalidations counted separately)
- **AND** every counter name is registered in the observability catalog and starts with `cqlite.`
- **AND** a disabled cache reports honest zeros for every counter and holds nothing.

### Requirement: Exercised end-to-end through the flight do_get path (cold and warm)

The global key cache SHALL be exercised end-to-end through the flight `do_get` read path on both a cold
first request (populate) and a warm repeat request (hit that skips the interval parse), proving the wiring
(a named surface + call chain + an end-to-end test), not helper-only unit coverage. The returned rows SHALL
match the query-semantics oracle on both requests.

#### Scenario: Cold do_get populates, warm do_get hits and skips the interval parse

- **GIVEN** a BIG-backed table served over flight through the lazy Summary-guided path (#2412), with the
  cache and interval-parse counter reset
- **WHEN** a cold `do_get` (`SELECT` returning known rows) is served, then the same `do_get` is repeated
  over the unchanged generation set
- **THEN** both responses return rows matching the query-semantics oracle
- **AND** the cold request populates the global cache (miss → one interval parse per resolved partition)
  while the warm request serves those partitions from the cache with 0 additional interval parses over the
  unchanged generations.
