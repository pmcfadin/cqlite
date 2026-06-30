# Design — BIG promoted-index read/seek + reverse iterator

## Context

- **Decoder (done, #993):** `decode_promoted_index(payload, prefix_len) -> DecodedPromotedIndex`
  (`promoted_index_reader.rs:253`) yields `entries: Vec<DecodedIndexInfo>`, each with
  `first_name`/`last_name` (serialized ClusteringPrefix min/max bound bytes), `offset` (bytes from
  partition start), `width`, `end_open_marker`. `PromotedIndexData { raw_payload }`
  (`index_reader.rs:70`) holds the raw bytes; `.decode(prefix_len)` is the schema-driven entry.
- **BTI precedent (the pattern to mirror):** `data_access/bti.rs::bti_clustering_row_window` (`:287`)
  uses the BTI Rows.db index to pick the block range covering a `ClusteringSlice`, returning
  `(rows, clustering_seek_engaged)`.
- **Seam:** `AccessPath::ClusteringSlice` (`access_path.rs:69`) is already the reserved variant;
  `classify_clustering_slice` (`select_executor/lookup.rs:265`) builds the slice;
  `SSTableReader::scan_partition_clustering` (`storage/mod.rs:305`) is the storage entry;
  `data_access/mod.rs:67+` branches BTI vs sequential.
- **Reverse today:** in-memory `sort_by` post-fetch (`executor.rs:578` core; `select_query_engine.rs:438`
  CLI). No reverse-iterator scaffolding exists.
- **Oracle:** Cassandra `AbstractSSTableIterator` (forward block seek via `RowIndexEntry`) and
  `SSTableReversedIterator` (back-to-front block walk). Local `~/local_projects/cassandra` has no `.java`
  today — use the remote `apache/cassandra` 5.0 tree for reference.

## Goals / constraints

- Wiring-evidence: a production read/seek call chain must consume the decoded blocks (e2e test, not a
  helper unit test).
- No-heuristics (#28): block selection compares **serialized clustering bound bytes** using the
  schema-derived comparator already used by the slice path — no guessing block boundaries.
- No `unwrap()`/`expect()` in library code; `-D warnings` clean.
- Forward and reverse of the same partition MUST return the identical clustering set (byte-parity truth:
  the 290-row pk=1 set in `test_big.wide_partition`, with the deleted ck 30..39 straddling a block
  boundary).
- Campsite: `data_access/` files are near threshold — add the BIG selector as its own submodule
  (`data_access/big_promoted.rs` or similar), do not grow `sequential.rs`/`bti.rs` past the ratchet.

## Decision 1 — BIG forward block selector (mirror BTI, not a new abstraction)

Add `big_clustering_row_window(reader, partition, slice, schema) -> (rows, clustering_seek_engaged)`
parallel to `bti_clustering_row_window`:
1. Resolve the partition's promoted-index payload (already parsed at `index_reader.rs:451`; today only
   `block_count()` is read — add an accessor that returns the `DecodedPromotedIndex`).
2. Decode blocks with the schema `PrefixLen`. Select the **minimal contiguous block range** whose
   `[first_name, last_name]` envelopes intersect the slice bounds (binary search on `first_name`).
3. Seek the Data.db reader to the first selected block's `offset` (relative to partition start), decode
   forward across the selected blocks only, stop past the slice's upper bound.
4. Return `clustering_seek_engaged = true` so the executor records `AccessPath::ClusteringSlice` (honest
   path labeling) instead of `FallbackFullScan`.

Dispatch: extend `data_access/mod.rs` BIG branch — when a `ClusteringSlice` is present and the partition
has a promoted index, call the new selector; otherwise fall back to today's sequential decode.

**Alternative considered — generalize BTI's selector to be index-source-agnostic.** Rejected for this
change: the two index encodings (Rows.db trie vs promoted IndexInfo array) decode differently enough
that a premature shared abstraction would obscure both; mirror first, unify later if a third consumer
appears (cheaper to merge two concrete impls than to unpick a wrong abstraction).

## Decision 2 — BIG reverse iterator (block walk back-to-front)

Add a reverse partition iterator that, given the selected (or full) block list, walks blocks
**last-to-first**; within each block decodes rows forward into a small buffer, then emits that buffer in
reverse. This mirrors `SSTableReversedIterator` (Cassandra also decodes each index block forward then
reverses within the block — rows are not individually back-seekable). Memory stays bounded to one block
(~64 KiB), not the whole partition.

Routing: when the plan has a single-partition target + `ORDER BY <ck> DESC` matching the clustering
order, the executor requests reverse iteration from storage instead of appending the in-memory Sort
step. The in-memory `sort_by` remains the fallback for non-wide / non-BIG / multi-partition cases.

**Alternative considered — keep the in-memory DESC sort, only add forward seek.** Rejected as the
primary deliverable because it leaves `forward_reverse_bounds` at `partial` and never exercises reverse
promoted-index decoding (the explicit acceptance criterion). **But see the phasing question below** —
the owner may elect to ship forward-seek first and reverse as a fast-follow.

## Phasing (owner decision at approval)

Forward-seek (Decision 1) and reverse-iterator (Decision 2) share the block-selector infrastructure but
are independently testable. Recommended: **deliver both in this change** (the issue scopes both, and the
forward==reverse parity assertion needs both). Acceptable alternative if the owner prefers a smaller
first increment: ship Decision 1 here, split Decision 2 into a fast-follow issue (manifest stays
`partial` until it lands). Flagged for approval — not decided unilaterally.

## Risks

- **Block-boundary correctness around tombstones:** the deleted ck 30..39 straddles a block boundary; the
  selector must include the boundary blocks so no live row adjacent to the deletion is dropped. The e2e
  test pins exactly this (forward==reverse==290 rows, none lost adjacent to the deleted block).
- **Prefix-len correctness:** wrong `PrefixLen` mis-parses `first_name`/`last_name`; reuse the
  schema-derived callback the decoder already requires (no heuristic).
- **Path-labeling honesty:** must record `ClusteringSlice` only when seek truly engaged, else parity/perf
  claims are overstated.

## Validation

`scripts/agent-gate.sh` PASS + spec-auditor **C** PASS against `specs/wide-partition-read/spec.md` +
roborev clean. E2e tests run against `test_big.wide_partition` with `CQLITE_DATASETS_ROOT` set.
