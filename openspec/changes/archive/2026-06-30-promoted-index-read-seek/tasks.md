# Tasks — BIG promoted-index read/seek + reverse iterator

## 1. Decode accessor (consume, don't re-decode)
- [ ] 1.1 Add a `PromotedIndexData` accessor returning the full `DecodedPromotedIndex` (schema
  `PrefixLen`), beyond today's `block_count()` (`index_reader.rs`). Surface: `PromotedIndexData::decode`
  consumed by storage, not just tests.
- [ ] 1.2 Plumb the parsed promoted-index payload to the BIG read path (it is already parsed at
  `index_reader.rs:451`; expose it to `scan_partition_clustering`).

## 2. Forward block selector (BIG) — Requirement: forward clustering-range seek
- [ ] 2.1 Add `big_clustering_row_window(...)` in a new `data_access/big_promoted.rs` submodule
  (campsite: do not grow `sequential.rs`/`bti.rs`), mirroring `bti_clustering_row_window`. Binary-search
  block `first_name`/`last_name` bounds against the `ClusteringSlice` using the schema comparator
  (no-heuristics). Surface exercised: `scan_partition_clustering` (BIG branch in `data_access/mod.rs`).
- [ ] 2.2 Seek Data.db to the first selected block `offset`, decode only the selected blocks, stop past
  the upper bound; return `(rows, clustering_seek_engaged=true)`.
- [ ] 2.3 Record `AccessPath::ClusteringSlice` only when seek engaged (honest path labeling in
  `select_executor/execute.rs`).
- [ ] 2.4 e2e test: `SELECT ... WHERE pk=1 AND ck>100 AND ck<140` on `test_big.wide_partition` takes the
  seek path (assert access path + exact rows). Boundary test: ranged read across the deleted ck 30..39
  returns ck 29 and ck 40, omits 30..39.

## 3. Reverse partition iterator (BIG) — Requirement: reverse partition iteration
- [ ] 3.1 Implement a BIG reverse iterator that walks selected/all promoted-index blocks last-to-first,
  decoding each block forward into a bounded buffer then emitting reversed (mirror
  `SSTableReversedIterator`). Memory bounded to one block.
- [ ] 3.2 Route single-partition `ORDER BY <ck> DESC` through the reverse iterator instead of appending
  the in-memory Sort step, when the target is a BIG wide partition; keep the in-memory `sort_by` as the
  fallback for all other cases (`executor.rs`/`planner.rs`).
- [ ] 3.3 e2e test: forward (ASC) and reverse (DESC) scans of pk=1 return the identical 290-row set;
  reverse is the exact reverse ordering; no row lost adjacent to the deleted block.
- [ ] 3.4 Regression test: `ORDER BY ck DESC` on a small / BTI / multi-partition case still served by the
  in-memory sort (no behavior change).

## 4. Parity manifest — Requirement: manifest reflects real reverse coverage
- [ ] 4.1 Promote `cass.sstable_scan.wide_partition.forward_reverse_bounds` from `partial` to mirrored
  in `test-data/cassandra-parity-manifest.yml`; update `docs/reports/cassandra-test-parity.md`.
- [ ] 4.2 Pin the forward==reverse 290-row assertion in the parity test (extend
  `tests/issue_993_wide_partition_promoted_index_parity.rs` or a sibling).

## 5. Quality gates (definition of done)
- [ ] 5.1 `scripts/agent-gate.sh` PASS (run from the worktree with
  `CQLITE_DATASETS_ROOT=/Users/patrickmcfadin/local_projects/cqlite/test-data/datasets`); paste the
  SUMMARY block.
- [ ] 5.2 spec-auditor **C** PASS against `openspec/changes/promoted-index-read-seek/specs/**` (every
  requirement `satisfied` with a public-surface/e2e test as evidence).
- [ ] 5.3 roborev clean (`--agent claude-code --model opus`).
- [ ] 5.4 No `unwrap()`/`expect()` in library code; `-D warnings` clean; campsite ratchet respected.
