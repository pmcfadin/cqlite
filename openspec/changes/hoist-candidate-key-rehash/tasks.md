# Tasks — hoist-candidate-key-rehash (C4, issue #1575)

## 1. Measurement first (A5 counter)
- [x] 1.1 Add the `KEY_HASH_CALLS` read-work counter (`record_key_hash` / `key_hash_calls`)
      to `storage/sstable/read_work_counters.rs`, following the issue #1566 zero-overhead
      pattern; extend the local-`Counters` round-trip unit test with a distinct multiplicity.
- [x] 1.2 Increment it at the single BTI key-encoding site
      (`bti/parser/partitions.rs::encode_partition_key_for_bti_trie`, the Murmur3 call).

## 2. TDD tests (RED without the hoist, GREEN with it)
- [x] 2.1 Reader-level fan-out (`tests/issue_1575_candidate_key_hash_hoist.rs`,
      `cli-helpers,work-counters`, fixture-gated): N INDEPENDENT `SSTableReader`s on
      `test_da/simple_table`; the retained per-candidate `might_contain_partition` records
      `KEY_HASH_CALLS == N`, the hoisted `might_contain_partition_encoded` fed one
      precomputed key records 1, with an identical admitted set. Present + absent scenarios.
- [x] 2.2 Manager path: a real `WHERE id = <uuid>` point read through the public `Database`
      API records `KEY_HASH_CALLS == 1` and returns the expected rows.

## 3. Implement the hoist
- [x] 3.1 Re-export the pre-encoded zero-copy walker `lookup_partition_in_bti_slice`
      (`bti/parser/mod.rs`, `bti/mod.rs`, `pub(crate)`).
- [x] 3.2 Refactor `lookup_partition_via_bti_trie` into: raw-key entry (memo-check, encode
      once, delegate), pre-encoded entry `lookup_partition_via_bti_trie_encoded`, and shared
      private `bti_trie_resolve(raw_key, encoded)`; add `is_bti` +
      `might_contain_partition_encoded` (`reader/partition_lookup.rs`).
- [x] 3.3 Add `SSTableManager::prune_candidates` (encode once iff any BTI candidate; reuse
      across all) and route the three prune sites through it
      (`scan_partition_with_cell_metadata`, `scan_partition_clustering`,
      `scan_partition_clustering_reverse`); drop the now-unused `Arc`/`reader` imports in
      `reverse_scan.rs`.

## 4. Gate + parity
- [x] 4.1 Add `--test issue_1575_candidate_key_hash_hoist` to the `work-counters-guard`
      gate component (`scripts/agent-gate.sh`) + its comment.
- [x] 4.2 `cargo +1.88.0 fmt` clean; C3 (`issue_1574`) and counter (`issue_1566`) tests still
      green; `scripts/agent-gate.sh --lite` PASS.

## 5. Deferred (see design.md "Deferred")
- [ ] 5.1 Local successor / no-whole-table-DFS partition-bound resolution (next-greater trie
      walk + single-DFS concurrency hardening) — carried as remaining C4 work; BTI
      oracle-sensitive, warrants its own focused review.
