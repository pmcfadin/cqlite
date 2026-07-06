# Design — bti-first-last-key-short-circuit (C5, issue #1576)

## Context

Source of truth: `docs/reports/read-path-performance-audit-2026-07-01.md` §Epic C, finding
C5. Builds on C3 (#1574, zero-copy BTI slice walk) and C4 (#1575, hoisted per-candidate key
hash). The point-read entry is `SSTableReader::get_with_resolution`
(`reader/data_access/mod.rs`), which the multi-generation candidate prune calls once per
candidate reader (`storage/sstable/mod.rs`).

## Decision 1 — Range short-circuit at the point-read entry, in token order

`SSTableReader::partition_key_out_of_range(key: &[u8]) -> bool` (new module
`reader/data_access/range_short_circuit.rs`):

1. If the reader has no `summary_reader` (BTI `da`, or a BIG reader whose Summary failed to
   load), return `false` — no authoritative bound, cannot rule out.
2. Read `first_key`/`last_key` from `SummaryReader`. If either is empty, return `false`.
3. Compute the Cassandra order key `(cassandra_murmur3_token(k), k)` for the query key and
   both endpoints, and return `key_pos < first_pos || key_pos > last_pos`.

`get_with_resolution` calls it FIRST (before the BTI/BIG branch); an out-of-range key returns
`Ok(None)` after recording one `RANGE_SHORT_CIRCUITS`.

**Why token order, not raw bytes.** The on-disk partition order is ascending Murmur3 token
with an unsigned-byte tiebreak — identical to `sort_by_token_order`
(`reader/data_access/model.rs`) and the write engine's `PartitionPosition::cmp`. Comparing
raw bytes would misorder partitions and could rule out a present key (false miss). The
`(i64, &[u8])` tuple comparison reproduces `DecoratedKey.compareTo` exactly (`RowKey`'s
derived `Ord` over `Vec<u8>` is unsigned-lexicographic, matching `ByteBufferUtil.compareUnsigned`).

**Why inclusive.** `first_key`/`last_key` are real present partitions, so `== first`/`== last`
must stay in range. The strict `<`/`>` comparison keeps both endpoints in range.

**Why fail-open on a missing bound.** The check can only turn a would-be miss into a cheaper
miss; when it cannot prove out-of-range it defers to the unchanged presence path, so it can
never manufacture a miss. Combined with the inclusive bound and token-order comparison, no
present partition is ever dropped.

## Decision 2 — `RANGE_SHORT_CIRCUITS` counter (A5 pattern)

Add a cfg-gated counter to `storage/sstable/read_work_counters.rs`
(`record_range_short_circuit` / `range_short_circuits`), zero-overhead in release (empty body
under no `work-counters`/`cfg(test)`), following #1566. It makes the behavior verifiable: an
out-of-range read records exactly 1 with `INDEX_PROBES == 0`; an in-range read records 0 and
reaches the real `Index.db` probe. This is the no-heuristics "observe the work, not just the
result" property.

## Decision 3 — Delete dead/wrong BTI scaffolding (dead-code proof first)

`rg` across the workspace (excluding the file itself and doc comments referencing Cassandra's
`TrieNode.java`) confirms zero live references to:

- `bti/nodes.rs` — `NodeParser`, `TrieNode`, `NodeType`, `NodeRef`, `select_optimal_node_type`,
  `get_character_range` (all used only by the file's own `#[cfg(test)]` module). The whole
  file is deleted and `pub mod nodes;` removed from `bti/mod.rs`.
- `BtiNode::get_transitions` (`bti/node.rs`) — zero callers anywhere (the only
  `.get_transitions()` calls were in `nodes.rs`'s tests on the unrelated `TrieNode`); its
  `Dense`-returns-empty behavior is a silent-wrong-answer footgun, so it is deleted rather
  than "fixed" (no consumer needs it). The live `BtiNode::find_child` (used by the real trie
  walk) is untouched.

Minimal-features build (`--no-default-features --features all-compression`) stays green,
proving no gated code depended on the removed symbols.

## Deferred / out of scope

- BTI range short-circuit: BTI has no `Summary.db`; a bound would require a trie walk (the
  work being avoided) or an open-time precompute with its own blast radius. Not done.
- Multi-candidate prune sites that call `might_contain_partition_encoded` directly (C4) are
  not re-plumbed; the short-circuit lives at `get_with_resolution`, the shared per-candidate
  point-read entry.

## Risks

- **False miss from a wrong bound (primary risk).** Mitigated by: authoritative source
  (`Summary.db`), token-order comparison matching the on-disk order, inclusive boundaries,
  fail-open on a missing bound, and a test that (a) validates `Summary` `first`/`last` equal
  the min/max-token present keys byte-for-byte and (b) asserts EVERY present key (incl. both
  boundaries) and an in-range-absent key are NOT ruled out.
