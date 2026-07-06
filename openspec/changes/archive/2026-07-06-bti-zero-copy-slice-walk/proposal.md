## Why

Every Cassandra 5.0 BTI (`da`) partition point lookup pays three compounding, avoidable
wastes on the hot read path, documented in the July 2026 read-path performance audit
(`docs/reports/read-path-performance-audit-2026-07-01.md` §Epic C, finding **C3**;
child of Epic C #1515, issue #1574):

1. **Whole-file copy per lookup.** `lookup_partition_in_bti_file`
   (`bti/parser/partitions.rs:480-517`) allocates a fresh `Vec<u8>` and `read_exact`s the
   ENTIRE `Partitions.db` trie on every call — even though the file is already resident in
   the reader's `Arc<Vec<u8>>` (`reader/types.rs:339`). The production callers wrap that
   resident buffer in a `std::io::Cursor` only to have it copied straight back out.
2. **Double trie walk per point read.** A single-candidate `WHERE pk = ?` descends the SAME
   trie for the SAME key twice: once for the candidate prune
   (`might_contain_partition` → `lookup_partition_via_bti_trie`, `mod.rs:1243`) and again
   for the seek (`scan_single_partition_clustering` → `lookup_partition_via_bti_trie`,
   `data_access/bti.rs:112`). The `TRIE_WALKS` A5 counter reads 2 where it should read 1.
3. **Per-node heap allocation on descent.** Following one byte down one trie node
   (`find_next_child_offset` → `parse_bti_node_for_traversal` → `parse_bti_node`)
   materializes the node's ENTIRE child table (`Vec<Transition>` / `Vec<Option<SizedPointer>>`,
   `node_decode.rs`) just to read a single child pointer.

This is design-driven read-path performance work under the standing owner Seam-1 approval
(2026-07-06 drain directive) for the read-path audit; routing = **design**. Target milestone:
**v0.14 performance wave** (Epic C, Wave 3 capstone).

Audit facts that constrain the design (no-heuristics, byte-faithful):

- The `Partitions.db` trie is the **authoritative** presence oracle for BTI (BTI has no bloom
  filter); a trie miss is definitive absence. This ordering decision
  (`partition_lookup.rs:648-706`) MUST be preserved.
- Resolved offsets are pinned: the `test_da/simple_table` fixture leaves at trie offsets 0/3/6
  resolve to Data.db offsets **0 / 63 / 125**. These, and the full 33-table `da` parity, are the
  correctness oracle — output MUST be byte-identical before/after.
- Node-type decoding stays exactly per the `TrieNode.java` format spec; an ambiguous node decode
  is an error, never a guess.

## What Changes

- **Zero-copy slice walk.** Add slice-based lookup entry points that operate directly on a
  `&[u8]` view of the resident `Arc<Vec<u8>>` buffer (parse the 8-byte root-offset footer and
  walk in place), eliminating the per-lookup whole-file copy. Switch the two production callers
  (`lookup_partition_via_bti_trie`, `bti_clustering_row_window`) to the slice API. The existing
  `Read + Seek` entry point is retained for stream callers/tests.
- **Zero-alloc child descent.** Rewrite the per-node child-pointer resolution to binary/linear
  search and decode the target child pointer **in place** from the node's byte slice, for all 16
  `TrieNode` ordinals (PayloadOnly / Single{4,8,12,16} / Sparse{8,12,16,24,40} / Dense{12,16,24,32,40}
  / LongDense), with no `Vec` allocation. Node bounds/structure errors stay errors (not `None`).
- **Single walk per point read.** Reuse the prune's resolved location for the seek so a
  single-candidate BTI point read descends the trie exactly once (`TRIE_WALKS == 1`, down from 2),
  via a reader-local same-key memo of the resolved offset (the resolution is a pure function of the
  immutable trie + key). The presence-oracle ordering and observability semantics are unchanged.

## Non-goals

- **No on-disk format change; no writer change.** Read-path only; `da` BTI byte layout untouched.
- **No general key/offset cache (that is B4, #issue-B4).** The single-walk memo is a bounded
  same-key coalescing slot for one point read, not a cross-lookup LRU.
- **No change to the wide-partition `Rows.db` row-index trie walk** (`bti/parser/reader.rs`,
  `bti/parser/traversal.rs`) — C3 scopes the `Partitions.db` partition trie descent.
- **No pre-`na` support**; Cassandra 5.0 `da` BTI only.
- **No successor-offset / per-candidate rehash changes** (C4/C5, separate children).
