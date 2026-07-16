# Global bounded key→partition-offset cache (Cassandra key-cache model) (#2059)

## Milestone
0.15 — the cqlite-trino latency/throughput/operations theme (epic #2403, **memory lane**).
**Design-driven — OpenSpec + Seam 1 required before any implementation.** Follow-up from #1570 (B4):
#1570 shipped a **per-reader** sharded key→partition-offset cache (`KeyOffsetCache`, byte-bounded per
reader). This change replaces the per-reader form with a **single process-global bounded cache** keyed on
`(generation identity, raw partition key)` — Cassandra's actual key-cache model — so aggregate resident
memory is bounded by ONE global cap regardless of how many SSTable readers are concurrently open, rather
than relying on `N_readers × per_reader_cap` staying within budget.

## Problem
Two independent problems, one structural fix:

- **Aggregate reader memory is unbounded across the open reader set.** Each open `SSTableReader` owns a
  512 KiB `KeyOffsetCache` (#1570). The `<128MB` guarantee currently rests on `N_readers × 512 KiB`
  staying small, but the flight `WarmTableRegistry` (#2310) pins one `Arc<SSTableReader>` per warm
  generation, so the aggregate key-cache footprint grows with the warm generation count — exactly the
  unbounded-aggregate hazard #1570's roborev flagged. A single global cap bounds it independent of reader
  count.
- **Point-read latency floor (value materializes with #2412).** Today a BIG point lookup resolves its
  offset from a **resident** raw-key `Index.db` map (O(1)); a cache in front of an O(1) map lookup is
  near-zero value. After #2412 (lazy Summary-guided BIG index, epic #2403 Lane 1) BIG open holds only
  `Summary.db` and each point lookup reads+parses **one `Index.db` interval** (a real disk read + parse).
  A key→offset cache then sits IN FRONT of that interval I/O: a hit skips the interval read entirely; a
  miss pays one interval parse then populates — precisely where Cassandra's key cache earns its keep
  (in front of `getPosition`'s summary-binary-search + index-interval walk). **The latency value of this
  change is therefore contingent on #2412; the memory-bounding value is standalone.** See design.md §D
  for the sequencing recommendation (land AFTER #2412).

## Goal
A single global, byte-bounded, sharded cache mapping `(generation identity, raw partition key) → offset`
(the location a fresh index lookup resolves), that:

- **Bounds aggregate resident memory** by one fixed global byte cap inside `<128MB`, independent of the
  number of concurrently-open readers.
- **Preserves the #1570 correctness guardrail** — a hit returns the EXACT location a fresh lookup on that
  generation resolves; a different generation or a different key never aliases.
- **Invalidates on generation removal / compaction** and **survives a #2383 rebind-by-inode** (a path
  swap over a byte-identical generation keeps entries valid); **fails closed on identity mismatch**.
- **Skips the Summary-guided `Index.db` interval parse on a hit** once #2412 lands (the load-bearing
  latency win).
- **Does not become the #2052-class mutex hotspot** under concurrent `do_get`s (sharded, one shard per
  hit, never a process-wide lock).

## Non-goals
- **No row cache.** This caches immutable partition *locations*, never row/value data (rows change shape
  under projection; values are already served by the B1 `DecompressedChunkCache`).
- **No cross-restart persistence / daemon posture.** Serializing the cache to disk to survive restarts is
  **consciously rejected** (recorded on epic #2403, mirroring #2412's rejection): the chosen "no state
  recreation on startup" strategy is to make *open cheap* (#2412), not to persist caches.
- **No BTI (`da`/`oa`) changes.** BTI open is already O(1) (three longs) and its point lookup walks an
  in-memory trie (a bounded CPU descent, already memoized by the #1574 single-walk memo), so a key cache
  in front of BTI skips CPU, not I/O — a materially weaker win that overlaps #1574/#2052 and is scoped
  out. The cache key/value stays format-agnostic (`PartitionLoc` already carries the BTI offset-only
  form) so a future BTI extension is additive; this change wires BIG only. Justified in design.md §A.
- **No change to the public `Database`/`QueryRow`/flight `do_get` result contract** — same rows, same
  bytes; only where locations are cached changes.
- **No `Value` decode / comparator / ordering change** — byte-parity is inviolable.
- **No pre-`na` format support** introduced or revisited (version floor unchanged).

## Relationship to #1818 (BIG point-read cache site dead on the public path)
#1818 observes the public BIG `get()` path routes to the whole-file `scan_for_key` fallback and never
reaches the cached `lookup_partition_with_index` site — so today the key cache is not even consulted on
the real public BIG point path. #2059 is **disjoint in mechanism** (cache shape + global bounding vs.
routing), but shares one prerequisite with #1818: the public BIG point path MUST reach the cache for the
wiring-evidence to hold. **#2412's §B point-lookup rewrite provides that reach** (it replaces the
whole-file `scan_for_key` with a Summary-guided interval resolution that is the cache's populate/consult
site), which is exactly the routing fix #1818 asks for. So #2059 neither subsumes #1818 nor depends on it
narrowly — it **depends on #2412**, whose point-lookup rewrite resolves the shared prerequisite (and the
BIG portion of #1818) as a side effect. Design.md §E states this precisely.

## Doctrine impact
- **No-heuristics (#28) reinforced:** the cache key is the authoritative generation identity (#2345
  device+inode+size+generation) plus the raw partition-key bytes the index is itself keyed on — never
  inferred from byte content; a hit returns the exact fresh-lookup location.
- CLAUDE.md / website `agents-developing/`: no doctrine text change; add a one-line note to the
  source-map / format-debugging page describing the global key cache once implemented (in-change, per the
  keep-doctrine-current rule).

## Definition of done
`scripts/agent-gate.sh` full PASS (SUMMARY recorded) + spec-auditor **C** PASS (every requirement
`satisfied` with a public-surface test) + roborev clean; `RUSTFLAGS="-D warnings"` clean; no
`unwrap()`/`expect()` in library code; physical-dump + query-semantics parity + flight `do_get`
cold+warm e2e green (cold miss populates, warm hit skips the interval parse). Then `openspec archive`.
