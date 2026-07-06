# key-offset-cache Specification

## Purpose

Define a bounded key→partition-offset cache (the Cassandra key-cache analogue) that lets a repeated
point read skip the `Index.db`/`Partitions.db`-trie descent while returning the exact location a
fresh resolution would.

## ADDED Requirements

### Requirement: A bounded, sharded key→partition-offset cache exists

The system SHALL provide a key→partition-offset cache that stores the resolved location
`(data_offset, data_size)` for a partition key. The cache key SHALL be the **full raw
partition-key bytes** (never a lossy hash/digest of them), so a cache hit can never return a
location belonging to a different key. The cache SHALL be bounded by an approximate **per-reader
byte budget** (`DEFAULT_KEY_CACHE_BYTES`, a small default; overridable via the `with_budget_bytes`
constructor), accounting each entry's approximate footprint (key bytes plus the stored location),
and SHALL be internally **sharded** so that a lookup ("hit path") acquires only a per-shard lock and
never a single process-wide lock; the total budget is divided across shards. Recency SHALL be
updated on access and eviction SHALL be **byte-based least-recently-used** — the least-recently-used
entries in a shard are evicted until the shard is back within its byte share. (For fixed-width
partition keys the resident entry count is exactly proportional to the byte footprint, so a
byte-budget bound behaves as an entry-count bound.) Locking SHALL be poison-tolerant (a thread that
panics while holding a shard lock SHALL NOT wedge the cache), and the cache SHALL contain no
`unwrap()`/`expect()`.

#### Scenario: A hit returns the stored location

- **WHEN** a location is inserted under a partition key and then fetched with the same key bytes
- **THEN** the fetch returns exactly the inserted `(data_offset, data_size)`

#### Scenario: A different key never aliases

- **WHEN** two distinct partition keys are inserted with distinct locations
- **THEN** fetching each key returns only that key's location, and fetching a third never-inserted key returns nothing

#### Scenario: LRU eviction is deterministic under a single shard

- **WHEN** a single-shard cache with a byte budget sized to hold exactly two fixed-width entries receives inserts and accesses in the order insert A, insert B, access A, insert C
- **THEN** B (least recently used) is evicted and A and C remain resident

#### Scenario: Resident footprint stays within the byte budget

- **WHEN** more distinct keys are inserted than the byte budget holds
- **THEN** the resident byte footprint never exceeds the configured budget after each insert (and, for fixed-width keys, the resident entry count never exceeds the equivalent entry count)

### Requirement: The cache honors the read-cache enabled toggle

The cache SHALL be built honoring `config.memory.block_cache.enabled` (the read-cache toggle). When
that toggle is `false` the cache SHALL be a genuine no-op: a fetch SHALL always miss, an insert SHALL
NOT retain, and occupancy/counter accessors SHALL report zero — so the point-read path bypasses
caching entirely rather than the toggle being decorative. No new configuration field SHALL be
introduced for the cache capacity.

#### Scenario: A disabled cache retains nothing

- **WHEN** the cache is constructed in its disabled (no-op) form and a location is inserted then fetched
- **THEN** the fetch misses, the resident entry count is zero, and the hit/miss counters report zero

### Requirement: A repeated point read skips the index/trie descent

The point-read partition-resolution path SHALL consult the cache before resolving (BIG `Index.db`
lookup and BTI `Partitions.db` trie descent): on a hit it SHALL return the cached location
without probing `Index.db` and without descending the trie; on a miss it SHALL resolve
authoritatively and, for a *present* key, insert the resolved location. Absent keys SHALL NOT be
stored, so the cache SHALL NOT fabricate a hit for a key the SSTable does not contain. A
process-observable index-probe counter (`INDEX_PROBES`) SHALL increment once per real `Index.db`
probe, complementing the existing `TRIE_WALKS` trie-descent counter, so a test can prove a repeated
read performed zero index probes / zero trie walks.

#### Scenario: A repeated BTI point read performs zero trie walks on the hit

- **WHEN** a present partition key is point-read against a BTI SSTable, then a second distinct key is read, then the first key is read again with the trie-walk counter reset before it
- **THEN** the second read of the first key returns rows and the trie-walk counter is zero (the cache hit skipped the descent), proving the multi-key cache — not a single-entry memo — served it

#### Scenario: A repeated BIG index resolution performs zero index probes and returns the same location

- **WHEN** `lookup_partition_with_index` is called twice for the same present key against a BIG SSTable, with the index-probe counter reset before the second call
- **THEN** the second call's `(data_offset, data_size)` equals the first call's, and the index-probe counter is zero

#### Scenario: An absent key is never cached as a hit

- **WHEN** a partition key that the SSTable does not contain is looked up twice
- **THEN** both lookups resolve to authoritative absence and the cache holds no entry for that key
