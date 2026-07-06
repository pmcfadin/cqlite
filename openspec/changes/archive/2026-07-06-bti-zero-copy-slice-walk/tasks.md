# Tasks — bti-zero-copy-slice-walk (C3, issue #1574)

## 1. TDD tests first (red on current main)
- [ ] 1.1 Slice-vs-stream parity unit tests in `bti/parser/slice_walk.rs`: `lookup_partition_in_bti_slice`
      and `lookup_raw_key_in_bti_partitions_slice` return the identical `BtiPartitionLocation` as
      `lookup_partition_in_bti_file` for the synthetic 2-partition trie and (fixture-gated) the real
      `test_da/simple_table` file; a miss returns `None`.
- [ ] 1.2 `find_child_offset` agreement unit tests: for each ordinal (PayloadOnly / Single 4,8,12,16 /
      Sparse 8,12,16,24,40 / Dense 12,16,24,32,40 / LongDense) assert `find_child_offset` equals
      `parse_bti_node(...).find_child(...)`, including the Dense offset-0 + gap (delta-0) cases and the
      truncated-node error case.
- [ ] 1.3 Single-walk integration test (`work-counters` feature, fixture-gated): a single-candidate BTI
      point read through the public `Database` API records `TRIE_WALKS == 1` (extend/add alongside
      `tests/issue_1566_read_work_counters.rs`, which currently asserts only `>= 1`).

## 2. Implement — zero-copy slice walk (Decision 1)
- [ ] 2.1 New `cqlite-core/src/storage/sstable/bti/parser/slice_walk.rs`: `lookup_partition_in_bti_slice`
      + `lookup_raw_key_in_bti_partitions_slice` (footer parse + in-place `walk_bti_trie`).
- [ ] 2.2 Register `mod slice_walk;` and re-export the slice fns in `bti/parser/mod.rs` and `bti/mod.rs`.
- [ ] 2.3 Switch `partition_lookup.rs::lookup_partition_via_bti_trie` and
      `data_access/bti.rs::bti_clustering_row_window` to the slice API on `partitions_db.as_slice()`
      (drop the `Cursor` wrap).

## 3. Implement — zero-alloc child descent (Decision 2)
- [ ] 3.1 `slice_walk::find_child_offset(trie_data, node_offset, search_byte)`: in-place per-ordinal
      child-pointer decode, reusing `node_decode::{read_be_unsigned, read_12bit_packed}` and the
      `saturating_sub` / Dense delta-0 sentinel arithmetic; structural failure → `Err`.
- [ ] 3.2 Replace `partitions.rs::find_next_child_offset` body with a delegate to
      `find_child_offset` (both the slice and stream walks now descend allocation-free).

## 4. Implement — single walk per point read (Decision 3)
- [ ] 4.1 Add a reader-local same-key memo field (`std::sync::Mutex<Option<(Box<[u8]>, Option<u64>)>>`)
      in `reader/types.rs` and initialize it at every reader construction site.
- [ ] 4.2 In `lookup_partition_via_bti_trie`: check the memo first; on a same-key hit return the cached
      resolution without re-walking or bumping `TRIE_WALKS`; otherwise walk, store, return. Preserve the
      presence-oracle ordering and the `READ_BLOOM_CHECKS` / `READ_PARTITION_LOOKUP` emissions.

## 5. Validate — gate + C + roborev
- [ ] 5.1 Byte-parity: full 33-table parity green (all `da` tables); `test_da` offsets 0/63/125 unchanged.
- [ ] 5.2 `RUSTFLAGS="-D warnings"` clean; no `unwrap()`/`expect()` in library code; minimal-features build
      if feature-gated modules were touched.
- [ ] 5.3 `scripts/agent-gate.sh --lite` PASS on each fix round; full `scripts/agent-gate.sh` PASS once
      before merge (SUMMARY pasted in the PR).
- [ ] 5.4 spec-auditor (C) PASS — every requirement satisfied with a public-surface test as evidence.
- [ ] 5.5 roborev clean.
