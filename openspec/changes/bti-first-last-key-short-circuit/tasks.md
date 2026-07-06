# Tasks — bti-first-last-key-short-circuit (C5, issue #1576)

## 1. Measurement first (A5 counter)
- [x] 1.1 Add the `RANGE_SHORT_CIRCUITS` read-work counter (`record_range_short_circuit` /
      `range_short_circuits`) to `storage/sstable/read_work_counters.rs`, following the issue
      #1566 zero-overhead pattern (struct field + `new`/`reset` + record/getter + module-doc
      entry).

## 2. TDD tests (RED without the short-circuit / bound validation, GREEN with it)
- [x] 2.1 Bound correctness (`tests/issue_1576_range_short_circuit.rs`,
      `cli-helpers,work-counters`, fixture-gated): `Summary.db` `first`/`last` equal the
      min/max-token `Index.db` keys; `partition_key_out_of_range` returns `false` for every
      present key + both boundaries + an in-range-absent key, and `true` for an out-of-range
      key.
- [x] 2.2 Counter wiring: an out-of-range `get_with_resolution` records
      `RANGE_SHORT_CIRCUITS == 1`, `INDEX_PROBES == 0`, `Ok(None)`; an in-range present key
      records `RANGE_SHORT_CIRCUITS == 0` and `INDEX_PROBES >= 1`.

## 3. Implement the short-circuit
- [x] 3.1 Add `reader/data_access/range_short_circuit.rs` with
      `SSTableReader::partition_key_out_of_range` (token-order, inclusive, fail-open on a
      missing bound); register the module in `reader/data_access/mod.rs`.
- [x] 3.2 Call it at the top of `SSTableReader::get_with_resolution` (before the BTI/BIG
      branch); record `RANGE_SHORT_CIRCUITS` and return `Ok(None)` on out-of-range.

## 4. Delete dead/wrong BTI scaffolding (dead-code proof first)
- [x] 4.1 `rg` the workspace for `NodeParser`, `TrieNode`, `NodeType`, `NodeRef`,
      `select_optimal_node_type`, `.get_transitions()` — confirm zero live (non-test,
      non-doc-comment) references.
- [x] 4.2 Delete `bti/nodes.rs`; remove `pub mod nodes;` from `bti/mod.rs`.
- [x] 4.3 Remove `BtiNode::get_transitions` from `bti/node.rs` (zero callers,
      `Dense`-returns-empty footgun); leave `BtiNode::find_child` untouched.

## 5. Gate + parity
- [x] 5.1 Add `--test issue_1576_range_short_circuit` to the `work-counters-guard` gate
      component (`scripts/agent-gate.sh`) + its comment.
- [x] 5.2 `cargo +1.88.0 fmt` clean; minimal-features + `cli-helpers[,work-counters]` builds
      green; `RUSTFLAGS="-D warnings"` clippy clean; `scripts/agent-gate.sh --lite` PASS;
      33-table parity unchanged.
