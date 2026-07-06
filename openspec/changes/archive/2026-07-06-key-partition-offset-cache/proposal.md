## Why

Even with B1's decompressed-chunk cache (#1567) resident, every hot point read still pays the
full index/trie descent to re-resolve the SAME partition key to the SAME location: a BIG read
probes `Index.db` (`lookup_partition_with_index`) and a BTI read descends the `Partitions.db`
trie (`lookup_partition_via_bti_trie`, `TRIE_WALKS`) on every call. Apache Cassandra solves this
with its **key cache** (partition key → data position); CQLite has no analogue. The July 2026
read-path audit (`docs/reports/read-path-performance-audit-2026-07-01.md` §Epic B, child **B4**,
#1570) calls for a small, bounded LRU keyed by the partition key that returns the resolved
location and lets a repeated point read skip the index/trie descent entirely.

This is **Epic B / child B4 (#1570)**, capstone Wave 3 of the read-path audit. The audit is the
**standing owner Seam-1 approval** for its children (2026-07-06 drain directive); this change does
not re-open the design decision. It is **design-driven** (a new cache subsystem with real latitude
in structure, key domain, and eviction — no external oracle dictates the cache shape), so it goes
through OpenSpec. Its *correctness guardrail* is oracle-driven: the cache MUST NOT change any read
result — a cache hit must return the SAME location a fresh index/trie resolution would, and the
33-table `sstabledump` parity harness stays green.

The A-series measurement counters are on `main` and validate this change: `TRIE_WALKS` (A5, #1566)
already gauges BTI trie descents; this change adds the analogous `INDEX_PROBES` gauge for BIG
`Index.db` probes so the same wiring-evidence assertion (`== 0` on a cache hit) applies to both
formats.

Milestone: **M7 (perf validation)** — v0.14 perf wave. Target: repeated point reads skip the
index/trie descent, within the <128MB memory budget (entries are tiny).

## What Changes

- **New per-reader, bounded, sharded key→partition-offset cache** (`cqlite-core/src/storage/cache/`,
  new `key_offset` module). Key = the **full raw partition-key bytes** (no lossy hashing → no
  collision → never a wrong offset); value = the resolved location `(data_offset, data_size)` the
  index/trie descent produces. Entry-count bounded (entries are tiny — key bytes + 12 value bytes);
  capacity is a constructor parameter with a small default; internally sharded, each shard a
  `Mutex<LruCache<…>>`, so the hit path takes only a per-shard lock, mirroring B1's concurrency rule
  (no `RwLock<LruCache>` — recency mutates on `get`). Poison-tolerant locking; no `unwrap()`/`expect()`.
- **Wired in front of the index/trie resolution in the point-read path (BIG and BTI):**
  1. BIG — `SSTableReader::lookup_partition_with_index`: `cache get → on miss: probe Index.db →
     insert (offset,size) → return`. A hit returns the cached `(offset,size)` without an `Index.db`
     probe.
  2. BTI — `SSTableReader::lookup_partition_via_bti_trie`: `cache get → on miss: descend the trie →
     insert offset → return`. A hit returns the cached offset without a `TRIE_WALKS` descent.
- **Positive-only cache.** Only *present*-key resolutions are stored. An absent key is never stored,
  so the cache can never fabricate a hit for a key the SSTable does not contain; it simply misses and
  falls through to a fresh (authoritative-absence) resolution.
- **A new `INDEX_PROBES` read-work counter** (extending the cfg-gated `read_work_counters` pattern,
  zero-overhead in release) incremented once per real `Index.db` probe, so a test proves a BIG cache
  hit performed **zero** index probes — the exact analogue of the existing `TRIE_WALKS == 0`
  assertion for BTI.
- **Honors the B2 config toggle.** The cache is built honoring `config.memory.block_cache.enabled`
  (the read-cache toggle B2 established): when disabled it is a genuine no-op cache so the point-read
  path bypasses it entirely, rather than the toggle being decorative. No new decorative config knob.

## Non-goals

- **No change to any read RESULT.** A cache hit returns the SAME `(offset,size)`/offset a fresh
  resolution returns; 33-table parity is a hard guardrail, not a tradeoff.
- **Not caching row data.** The cache stores *locations* (immutable facts about an immutable SSTable),
  never decoded rows (which change shape under projection). Row/chunk caching is B1.
- **Not a negative cache.** Absent keys are not stored (keeps "must not fabricate hits" trivially
  true and avoids negative-cache invalidation subtleties).
- **Not the honest cache-observability surface (B5)** — this change adds only the minimal hit/miss
  counters its own tests need; the full `DatabaseStats` surface is B5.
- **No new external crate dependency** (reuses the existing `lru` crate, hand-sharded like B1).
- **No change to the no-heuristics posture** — keying is the full authoritative partition-key bytes,
  never inferred from byte patterns; the digest-domain schema-context lookup path is unchanged.
- **Not the C1 BIG `get()` fallback fix** (#1571) — B4 caches whatever `lookup_partition_with_index`
  resolves; it neither introduces nor depends on C1.

## Impact

- **Memory budget (<128MB):** entry-count bounded; each entry is a small key + 12 value bytes.
  The default capacity is a small constant, sized conservatively.
- **No-heuristics mandate:** unaffected — full-key authoritative keying only.
- **Public binding surfaces (Python/Node/CLI):** unchanged behavior; repeated point reads get
  faster. No API signature changes (the cache is internal, per-reader).
- **Concurrency:** the cache is a sharded `Mutex<LruCache>`; the design forbids poisoned-lock
  propagation, a global hit-path lock, and unbounded growth.
