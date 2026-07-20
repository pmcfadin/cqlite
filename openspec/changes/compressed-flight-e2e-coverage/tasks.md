# Tasks — compressed-flight-e2e-coverage (#2373)

## 1. Make the corpus CI-real

- [ ] 1.1 `git add -f` the components the tests read for `test_comp`: the four codec tables
      (`lz4_table`, `snappy_table`, `deflate_table`, `zstd_table`), the two edge tables
      (`short_final_chunk`, `incompressible_uncompressed_chunk`), and the `uncompressed_table`
      control — `nb-1-big-{Data,CompressionInfo,Statistics,Index,Summary,Filter}.db` + `TOC.txt`.
      (~336 KB; `.jsonl` goldens + decoded sidecars are already tracked. `uncompressed_table` has no
      `CompressionInfo.db` by design.) Surface: the tests below run on a stock checkout.
- [ ] 1.2 Confirm nothing else in the corpus was swept in and the pin is untouched
      (`dataset-pin.env` unchanged; no regeneration — #2222/#1935 stand).

## 2. Fixture helper

- [ ] 2.1 Add `table_dir_by_prefix(keyspace, table_prefix, component_prefix) -> Option<PathBuf>` to
      `cqlite-flight/tests/fixture_support/mod.rs` (glob `<root>/sstables/<ks>/<table>-*`, require
      `<component_prefix>-Data.db` is a file), mirroring `issue_1082_deflate_zlib.rs:63-83`. Keep
      `table_dir_if_present` working (additive). Surface: used by every test below.
- [ ] 2.2 SKIP-aware gating with `CQLITE_REQUIRE_FIXTURES=1` → hard failure
      (pattern `chunk_cache_wiring_tests.rs:26,214`).

## 3. Transport-level per-codec coverage

- [ ] 3.1 New `cqlite-flight/tests/compressed_do_get_transport_test.rs`, copying the transport harness
      from `bti_do_get_transport_test.rs:68-125` (`do_get_batches_over_transport` / `run_do_get`).
- [ ] 3.2 Full-scan-vs-golden case per codec (LZ4, Snappy, Deflate, Zstd): row count AND
      `(pk, ck, body)` equality against `nb-1-big-Data.db.jsonl`; present-but-empty = FAILURE.
- [ ] 3.3 Edge-table full scans: `short_final_chunk`, `incompressible_uncompressed_chunk`.
- [ ] 3.4 LIMIT-k case bounding the result and matching a golden prefix.
- [ ] 3.5 Midstream-drop case on LZ4 reusing `do_get_drop_after` + `await_in_flight_settled`
      (`do_get_transport_test.rs:595-670`, batch_size=1): in-flight level settles to baseline.

## 4. Routing evidence

- [ ] 4.1 Bracket each compressed scan with `reset_decompress_calls()` /
      `decompress_call_count() >= 1` (`data_access/mod.rs:146,151`).
- [ ] 4.2 Uncompressed control (`test_comp.uncompressed_table`): same scan shape, counter **== 0**.
- [ ] 4.3 Serialize counter-reading tests against each other (`#[serial]` or single-test binary);
      header comment explaining the counter is process-global and why the control makes it evidence.

## 5. Docs

- [ ] 5.1 Mark FMT-1 closed for the in-repo Flight axis in
      `docs/architecture/issue-2363-coverage-matrix-audit.md`; note the live-testbed half remains open
      as the separate follow-up.
- [ ] 5.2 Test header notes the goldens/bytes are pinned to datasets-v3 v3.5 and must be refreshed
      together on any future regen (#2222).

## 6. Certification

- [ ] 6.1 `scripts/agent-gate.sh --lite` each fix round (summary-file redirect, unique path);
      plus the new test target explicitly.
- [ ] 6.2 **Verify against the COMMITTED tree**, not the dirty worktree — `git worktree add --detach HEAD`
      and run the new tests there (a gitignored-but-present local binary is the classic false green;
      #2372 `tasks.md:35-37`).
- [ ] 6.3 rust-reviewer + roborev on the lite-green diff (review-first); blockers fixed pre-PR.
- [ ] 6.4 flow-closer endgame: ONE full gate → C intent audit vs this spec → final roborev →
      merge-on-green → finalize.
