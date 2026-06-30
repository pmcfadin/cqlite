# Wire BIG promoted-index decode into the wide-partition read/seek path + reverse iterator

## Why

Milestone: M7 (read-path parity + perf). Routing: **design-driven** (read/seek strategy + a
new reverse-iteration surface have real design latitude), grounded by an **oracle** (Cassandra
`AbstractSSTableIterator` / `SSTableReversedIterator` block-seek semantics + the byte-for-byte
`test_big.wide_partition` golden).

`PromotedIndexData::decode` / `decode_promoted_index` are implemented and proven byte-for-byte against a
real Cassandra wide-partition fixture (#993), but **no production read/seek path consumes the decoded
IndexInfo blocks** — only `block_count()` is wired (into stats at `index_reader.rs:218`). Per CQLite's
wiring-evidence rule, the decode surface is exercised only by a parity test, not a real query call chain.

Two concrete gaps follow:

1. **Forward seek (BIG).** A `WHERE ck > ? AND ck < ?` ranged read on a BIG (`nb`) wide partition
   **full-scans the partition then filters in memory**. BTI (`da`) already narrows via the Rows.db row
   index (`bti_clustering_row_window`); BIG has no promoted-index equivalent, so large wide-partition
   ranged reads pay a full-partition decode they should not.
2. **Reverse iteration.** `ORDER BY ck DESC` is served by a **post-fetch in-memory sort** (read whole
   partition forward, then `sort_by`) in both the CLI engine and the core executor — there is no true
   reverse partition iterator. It never drives the promoted index to seek blocks back-to-front, so the
   `cass.sstable_scan.wide_partition.forward_reverse_bounds` parity scenario is stuck at `partial`.

## What changes

- Add a **BIG promoted-index clustering block selector** that decodes the IndexInfo blocks for a
  targeted partition and selects the minimal contiguous block range covering a clustering predicate,
  mirroring the existing BTI Rows.db selector. Wire it into the `AccessPath::ClusteringSlice` ->
  `scan_partition_clustering` seam so a ranged read on a BIG wide partition seeks instead of full-scans
  (wiring-evidence: named surface + production call chain + e2e test).
- Add a **BIG reverse partition iterator** that walks the selected promoted-index blocks back-to-front,
  decoding each block's rows and emitting them in descending clustering order, mirroring Cassandra
  `SSTableReversedIterator`. Route `ORDER BY ck DESC` on a BIG wide partition through it instead of the
  post-fetch in-memory sort.
- Promote the `forward_reverse_bounds` manifest scenario from `partial` to mirrored once reverse
  iteration is real, with the pinned forward==reverse 290-row assertion.

## Non-goals

- BTI (`da`) reverse iteration. This change covers the **BIG (`nb`)** promoted-index path; BTI already
  has forward clustering narrowing and is out of scope here.
- Cross-partition / multi-partition reverse ordering, partition-key ordering, or token-range reverse
  scans — single-partition clustering order only.
- Changing the on-disk promoted-index decode (already authoritative + tested in #993); this only
  *consumes* it.
- Removing the in-memory sort fallback for formats/paths not covered here (it remains the correct
  fallback for non-wide partitions and BTI).
- `end_open_marker` range-tombstone-at-block-boundary handling beyond what the existing decoder already
  surfaces (writer emits `0x00` today; faithful pass-through only).

## Doctrine impact

- Reinforces the **wiring-evidence** rule (a decoded surface must be consumed by a real call chain with
  an e2e test) — this change is the canonical example of closing a decoded-but-unwired gap.
- Updates `test-data/cassandra-parity-manifest.yml` (`forward_reverse_bounds`: `partial` -> mirrored,
  tier promotion) and the parity report. No public CLI/binding surface change, so no
  `agents-developing/` site change required.
