## Why

A point read against a candidate SSTable pays bloom / `Index.db` / BTI-trie presence
work even when the queried partition key falls entirely OUTSIDE the SSTable's key range,
documented in the July 2026 read-path performance audit
(`docs/reports/read-path-performance-audit-2026-07-01.md` §Epic C, finding **C5**; child
of Epic C #1515, issue #1576):

- `SummaryReader` already parses the SSTable's `first_key`/`last_key`
  (`storage/sstable/summary_reader.rs`), but nothing consulted them — so a query key that
  provably cannot live in `[first_key, last_key]` still ran the full presence path.
- Dead + wrong BTI scaffolding sits in the tree as a loaded gun: `bti/nodes.rs`
  (`NodeParser` / `TrieNode` and their helpers) has NO non-test callers and decodes node
  pointers incorrectly; `BtiNode::get_transitions` (`bti/node.rs`) returns an EMPTY vec
  for `Dense` nodes — a silent-wrong-answer footgun — and also has zero callers.

This is design-driven read-path performance + hygiene work under the standing owner Seam-1
approval (2026-07-06 read-path-audit drain directive); routing = **design**. Target
milestone: **v0.14 performance wave** (Epic C, Wave 3 capstone).

Audit facts that constrain the design (no-heuristics, no-false-miss):

- The authoritative on-disk partition order is **ascending Cassandra Murmur3 token, ties
  broken by unsigned-lexicographic key bytes** — the exact order
  `sort_by_token_order` (`reader/data_access/model.rs`) and the write engine's
  `PartitionPosition::cmp` use (spec §5, Appendix B §313). The range comparison MUST be in
  THAT domain; a raw-byte comparison would be WRONG (physical order is token order, not
  byte order) and could drop a present partition — the false-miss footgun the audit warns
  against.
- The bound MUST come only from authoritative metadata (`Summary.db`), never a guess. The
  bound is **inclusive** at both ends: a key equal to `first_key` or `last_key` is IN range.
- BTI (`da`) SSTables have no `Summary.db` and no `Index.db`; their `Partitions.db` trie is
  itself the authoritative presence oracle. When no authoritative bound is available the
  short-circuit conservatively reports "cannot rule out" so the normal path runs unchanged.

## What Changes

- **Add an O(1) first/last-key range short-circuit to the point-read entry.** Before the
  BTI/BIG branch in `SSTableReader::get_with_resolution`
  (`reader/data_access/mod.rs`), consult a new
  `SSTableReader::partition_key_out_of_range` (`reader/data_access/range_short_circuit.rs`):
  when the query key sorts outside the SSTable's `Summary.db` `[first_key, last_key]` bound
  in Cassandra token order, return `Ok(None)` immediately — before any bloom check,
  `Index.db` probe, or BTI trie descent. `get_with_resolution` is the per-candidate entry
  every multi-generation point read funnels through, so the check covers the fan-out.
- **Measure it.** Add a cfg-gated `RANGE_SHORT_CIRCUITS` read-work counter (issue #1566 / A5
  pattern, zero-overhead in release) incremented at the single short-circuit site, so the
  behavior is provable: an out-of-range read records exactly 1 and performs 0 `Index.db`
  probes; an in-range read records 0.
- **Delete the dead/wrong BTI scaffolding.** Remove `bti/nodes.rs` entirely (`NodeParser`,
  `TrieNode`, `NodeType`, `NodeRef`, `select_optimal_node_type` — all with zero non-test
  callers) and the `pub mod nodes;` declaration; remove `BtiNode::get_transitions`
  (zero callers, `Dense`-returns-empty footgun). Dead-code proof: a workspace `rg` for each
  symbol confirms zero live references before deletion.

## Non-goals

- **No on-disk format change; no writer change.** Read-path only; `da` BTI and `nb`/`na` BIG
  byte layouts untouched. In-range reads resolve byte-identically (the 33-table parity is
  the correctness oracle).
- **No BTI range short-circuit from a trie walk.** BTI has no `Summary.db`; deriving a
  first/last bound by walking the trie would defeat the point (a trie descent is the very
  work the short-circuit avoids). BTI readers simply fall through to the trie oracle, which
  is already authoritative and fast. The short-circuit fires where an authoritative bound
  exists (BIG, via `Summary.db`).
- **No partitioner negotiation.** CQLite targets Cassandra's default Murmur3Partitioner (the
  whole codebase computes tokens with `cassandra_murmur3_token`); the comparison is
  consistent with that standing assumption.
- **No pre-`na` support**; Cassandra 5.0 `da`/`nb`/`na` only.
