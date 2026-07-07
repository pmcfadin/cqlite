## 1. Parity test FIRST (TDD red, against legacy paths)

- [x] 1.1 Add `cqlite-core/tests/issue_1599_locate_parity.rs`: for every partition key (present, absent,
      boundary `first_key`/`last_key`) in the BIG (`nb` + uncompressed) and BTI (`da`, narrow + wide)
      fixtures, capture the result of the legacy `lookup_partition_with_index` /
      `lookup_partition_via_bti_trie` and assert it as the oracle. (Reds until `locate` exists.)
- [x] 1.2 Add counter-delta assertions: BIG definite-bloom-miss ⇒ 0 `INDEX_PROBES`; BTI ⇒ exactly one
      `READ_BLOOM_CHECKS` from the trie; B4 repeat present-key ⇒ 0 new probes/walks; C5 out-of-range ⇒
      1 short-circuit + 0 downstream work.

## 2. Build the façade

- [x] 2.1 Add `reader/partition_locator.rs` with `SSTableReader::locate` (C5 short-circuit once, then
      BIG Summary→Index.db / BTI trie dispatch) and `locate_encoded` (C4 hoist for prune).
- [x] 2.2 Wire the module in `reader/mod.rs`; make 1.1/1.2 pass against `locate` (byte-identical offsets,
      identical negatives, identical error classification).

## 3. Migrate the point path + candidate pruning

- [x] 3.1 `big_get_with_resolution` (`big_point.rs`): resolve the offset via `locate`, KEEPING the bloom
      pre-check first and the `Index.db`-miss ⇒ `scan_for_key` fallback unchanged.
- [x] 3.2 `bti_point_lookup` (`bti.rs`): resolve via `locate`, KEEPING bloom-skip + trie-authoritative
      absent semantics and the single `READ_BLOOM_CHECKS` emission.
- [x] 3.3 `prune_candidates` (`storage/sstable/mod.rs`): resolve via `locate_encoded` /
      `might_contain_partition` semantics through the façade.

## 4. Delete the now-unreachable entry points

- [x] 4.1 Prove zero production callers of `get_with_spec_readers`, `get_with_schema_context`,
      `lookup_partition_with_schema_context` (workspace `rg`, excluding `tests/` + in-crate).
- [x] 4.2 Delete them; re-point or remove the tests that referenced them onto `locate` / `get`.
- [x] 4.3 Delete `compute_partition_key_digest_with_schema` if it becomes orphaned by 4.2.

## 5. Split the over-threshold files (campsite #1116)

- [x] 5.1 Split `index_reader.rs` → `index_reader/{mod,parse}.rs` (struct+API vs nom parse tree); keep
      the future bounded-mode seam documented at the materialization sites.
- [x] 5.2 Split `reader/data_access/bti.rs` → move point-lookup decoders into `bti_point.rs`; confirm
      both touched primaries end smaller than pre-change.

## 6. Validate

- [ ] 6.1 `scripts/agent-gate.sh` PASS (paste SUMMARY verbatim).
- [x] 6.2 `RUSTFLAGS="-D warnings"` clean; no `unwrap()`/`expect()` in library code.
- [ ] 6.3 33-table golden parity green.
- [x] 6.4 `openspec validate g3-one-partition-locator --strict` clean.
