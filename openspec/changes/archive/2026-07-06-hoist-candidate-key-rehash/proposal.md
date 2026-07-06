## Why

A multi-generation `WHERE pk = ?` point read against Cassandra 5.0 BTI (`da`) SSTables
re-hashes the SAME query partition key once per candidate SSTable, documented in the July
2026 read-path performance audit (`docs/reports/read-path-performance-audit-2026-07-01.md`
§Epic C, finding **C4**; child of Epic C #1515, issue #1575):

- The candidate-prune loop calls `might_contain_partition` per candidate reader
  (`storage/sstable/mod.rs`, `storage/sstable/reverse_scan.rs`). For a BTI reader that
  routes to `lookup_partition_via_bti_trie` → `encode_partition_key_for_bti_trie`
  (`bti/parser/partitions.rs`), which computes a **Murmur3 token + byte-comparable
  encoding** of the raw key. That encoding is a pure function of the key ALONE — it does not
  depend on which SSTable is being pruned — yet it is recomputed once per candidate. With N
  generations that is N identical Murmur3 hashes per read where 1 suffices.

This is design-driven read-path performance work under the standing owner Seam-1 approval
(2026-07-06 read-path-audit drain directive); routing = **design**. Target milestone:
**v0.14 performance wave** (Epic C, Wave 3 capstone).

Audit facts that constrain the design (no-heuristics, byte-faithful):

- The `Partitions.db` trie is the **authoritative** presence oracle for BTI (no bloom
  filter); a trie miss is definitive absence. The prune decision (the admitted candidate
  set) and the resolved offsets MUST be byte-identical before/after — the pinned
  `test_da/simple_table` leaves at trie offsets 0/3/6 resolve to Data.db offsets 0/63/125,
  and the full 33-table `da` parity is the correctness oracle.
- The per-SSTable trie WALK is legitimately per-candidate (each generation has its own
  trie); only the key **hash+encoding** is redundant across candidates. This change hoists
  the hash, NOT the walk.

## What Changes

- **Hoist the BTI key hash+encoding out of the candidate-prune loop.** Compute the
  byte-comparable trie key ONCE per read (only when a BTI candidate is present) and reuse it
  for every candidate's prune, via a new pre-encoded lookup entry point
  (`SSTableReader::might_contain_partition_encoded` →
  `lookup_partition_via_bti_trie_encoded`, backed by the existing zero-copy
  `lookup_partition_in_bti_slice`). The three candidate-prune sites
  (`scan_partition_with_cell_metadata`, `scan_partition_clustering`,
  `scan_partition_clustering_reverse`) route through one `SSTableManager::prune_candidates`
  helper. A BIG (`nb`) candidate has no BTI encoding to hoist — its raw-key bloom check runs
  unchanged — so a non-BTI or mixed candidate set stays correct.
- **Measure it.** Add a cfg-gated `KEY_HASH_CALLS` read-work counter (issue #1566 / A5
  pattern, zero-overhead in release) incremented at the single BTI key-encoding site, so the
  hoist is provable: a multi-generation fan-out records exactly 1, not N.

## Non-goals

- **No on-disk format change; no writer change.** Read-path only; `da` BTI byte layout
  untouched. The prune decision and resolved offsets are byte-identical.
- **Local successor / no-whole-table-DFS partition-bound resolution is DEFERRED** (a
  distinct follow-up). C4's audit line pairs the hash-hoist with replacing the first-read
  whole-trie DFS successor enumeration (`partition_lookup.rs::bti_partition_offsets`) with a
  local next-greater trie walk (and single-DFS concurrency hardening). That is
  BTI-oracle-sensitive (a wrong seek bound silently truncates a partition read) and requires
  either a substantial new next-greater traversal over all 6 node-type families or an
  open-time precompute with its own blast radius — beyond this MINIMAL change. It is carried
  as remaining C4 work (see design.md "Deferred") so the risky rewrite gets its own focused
  review; the current enumeration is already memoized and correct.
- **No general key/offset cache (that is Epic B/B4).** The hoist reuses ONE encoding within a
  single read; it is not a cross-lookup cache.
- **No pre-`na` support**; Cassandra 5.0 `da` BTI only.
