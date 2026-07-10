# cqlite-flight do_get: partition point-read for pushed PK-equality (issue #2207)

## Milestone
0.14 (Flight field-readiness). Design-driven — Seam-1 owner approval of this spec + design
precedes any implementation. Phase 1 of the ms-point-read program (research:
`docs/architecture/issue-2310-ms-point-reads-research.md`, epic #2310).

## Why (measured problem)
`WHERE pk = 'x'` pushed into the Flight ticket (#2164/PR #2166) reaches the server, but `do_get`
uses it **only as a per-row egress filter over a full k-way merge scan of every SSTable**. In
`MergeProducer::drive_merge` the producer builds a `KWayMerger` over all (token-pruned) paths
(`cqlite-flight/src/producer.rs:597`), walks every partition, and applies the pushed predicate at
`producer.rs:784` (`filter.keeps(&row)`). The only pruning today is token-range
(`prune_paths_cancellable` reads `Summary.db` for a token-span check, `producer.rs:618`) — there is
**no partition point-read and no predicate-driven SSTable prune**.

Consequence at field scale: a single-partition PK equality on a 2.16M-partition table reads and
merges every SSTable server-side. Measured field point read: **190–433s** (#2157 round-3); the
pushdown wins 1 row of egress while doing O(table) I/O + decode. The research names this the
dominant point-read cost by 3–4 orders of magnitude.

The machinery for a real point read already exists in core but is **never called from Flight**:
per-SSTable `might_contain_partition` (bloom presence oracle,
`cqlite-core/src/storage/sstable/reader/partition_lookup.rs:416`),
`lookup_partition_via_bti_trie` (`partition_lookup.rs:136`), `lookup_partition_with_index`
(Summary/Index seek, `partition_lookup.rs:25`), and the generation-reconciling
`TombstoneMerger` used by `storage.get` (`cqlite-core/src/storage/sstable/mod.rs:1006-1028`). The
Flight producer only knows the compaction-merge scan.

## What changes
When the pushed predicate set contains a **full partition-key equality** (every partition-key
component bound to a single value — or an `IN` list over the full PK, treated as N such lookups),
`do_get` SHALL route to a **partition point-read path** instead of the full scan:

1. **Prune** candidate SSTables using authoritative presence metadata only — bloom
   `might_contain_partition` plus Summary/Index (BIG `nb`) or BTI trie (`da`) resolution — never a
   heuristic (no-heuristics mandate, #28).
2. **Seek** each surviving candidate to just the target partition (its clustering rows), not the
   whole Data.db.
3. **Reconcile** those single-partition fragments through the **same merge/reconciliation** the
   scan path already drives (`drive_merge`), preserving LWW/tombstone/multi-generation semantics —
   a naive "first SSTable hit wins" is explicitly rejected.
4. **Stream** the reconciled rows through the existing budget/cancellation/Arrow-encode egress.

The result set is **byte-identical** to the current scan+filter path for the same pushed predicate.

## Non-goals
- **No generalization beyond full-PK equality.** Any non-full-PK, non-equality, or partial-PK
  predicate (clustering-only, range, secondary-column, `IS NULL`) keeps the unchanged full-scan +
  per-row filter path. Clustering-slice narrowing within a partition is out of scope (reserved
  #954); this change targets `WHERE pk = ?` / `WHERE pk IN (...)` only.
- **No warm-reader cache.** Per-request reader open / index parse is Phase 2 (#2310); this change
  does the probe cold each request.
- **No snapshot-completeness or component-resolution fixes.** Complete snapshots (#2295) and
  present-pair resolution (#2302) are merged prerequisites; this change consumes them and, when the
  index components are absent or ambiguous, **falls back to the scan path** (never a wrong answer).
- **No new config knob and no new user-facing library/CLI/binding method** beyond the internal
  core seek primitive the Flight producer calls and the existing observability surface.
- **No change to write, compaction, or the KWayMerger reconciliation logic itself** (#2230/#1668
  are adjacent merge-layer work, not touched here).

## Cross-links
Prereqs (merged): #2295 (snapshot completeness), #2302 (Summary/Index resolution). Program: epic
#2310 (Phase 2 warm readers), research doc above. Adjacent merge-layer: #2230, #1668. Observability:
#2163 (`cqlite.read.sstables_pruned`). Field verdict tracker: #2157. Epic: #2103 / AM #2226.
