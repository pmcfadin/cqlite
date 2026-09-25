# Tasks — sstable-rebuild (issue #4197)

Ordered. Group 0 is premises, 1–3 the library, 4 the CLI, 5 the endgame. Commit after every group
(#3042). `--lite` after every fix round; ONE full gate in `flow-closer`.

## 0. Premises — re-confirm in the worktree, STOP and report if false

- [ ] 0.1 Confirm `write_partition_with_index_blocks` (`writer/data_writer/partition.rs:328`) has
      no existing decoupled "compute promoted-index blocks against pre-existing offsets" entry
      point — i.e. design.md §D1's claim holds. If one already exists, task 1.2/1.3's byte-extent
      walk can call it directly instead of reimplementing block-boundary logic; note which.
- [ ] 0.2 Confirm every committed schema under `test-data/schemas/*.cql` either states
      `bloom_filter_fp_chance` explicitly or is fine defaulting to `recomputed` for that field, and
      confirm none states a non-default `min_index_interval` (design.md §D6 assumes this for the
      "all 7 components" byte-parity scenario R1–R2; R3.2's non-default case is necessarily
      synthetic because of this).
- [ ] 0.3 Confirm `iterate_all_partitions_for_compaction` / `stream_all_partitions_for_compaction`
      (`reader/data_access/compaction.rs:160,589`) resolve LOGICAL (decompressed-stream) offsets
      transparently for compressed input, and determine whether they already expose per-row byte
      extents or only decoded `Mutation`s — this decides whether task 1.2's byte-extent walk is a
      thin wrapper or a new low-level parser.
- [ ] 0.4 Confirm `VerifyMode` is still `Quick`/`Full` only (no `Audit` variant) on `origin/main` at
      claim time, confirming `--in-place` must ship as a refusal in this PR (design.md §D3/R8.1).
- [ ] 0.5 Corruption corpus present under the fetched root (reuse #4196's confirmed list:
      `test_comp_corrupt/data_db_bit_flip` et al.) for R5/R7.2's refusal fixtures.

## 1. Byte-extent walk + boundary enumeration — surface: `write_engine::rebuild`

- [ ] 1.1 `rebuild/mod.rs`: `RebuildOptions`, `RebuildReport`, `Component` enum
      (`Index|Summary|Filter|Digest|Toc|Crc|Statistics`), `FieldProvenance`
      (`Recovered|Recomputed|Lost`), `Refusal` (D5 shape, serde). `pub mod rebuild;` declared in
      `write_engine/mod.rs` alongside `salvage`, same feature gating discipline.
- [ ] 1.2 `rebuild/boundaries.rs`: partition + intra-partition row/unfiltered-marker byte-extent
      walk over Data.db, built on whatever task 0.3 found (extend the compaction reader if it only
      exposes decoded mutations; wrap it directly if it already exposes extents). Chunk-CRC
      pre-flight identical in spirit to salvage's `chunks.rs` — a bad chunk is a `data-corrupt`
      refusal (R5), never a partial component set.
- [ ] 1.3 `rebuild/components.rs`: drives `IndexWriter`/`SummaryWriter`/`FilterWriter` from the
      boundary walk's output (R1–R3); `DigestWriter`/`crc_writer::StreamingCrc`/`TocWriter` driven
      directly from Data.db bytes (no walk needed, R1).
- [ ] 1.4 Close the `min_index_interval` gap (design.md §D2/R3.2): extend `cql_parser.rs`'s
      WITH-clause option parsing to capture `min_index_interval` into `schema.comments` the same
      way `bloom_filter_fp_chance` already is, and thread it into the Summary rebuild path (NOT
      into `SSTableWriter`'s general constructor — that is out of scope; only the rebuild-local
      Summary regeneration needs it). If this proves larger than a comments-map addition, STOP and
      report — R3.2 is written to pass EITHER way (asserting `recomputed` today, or `recovered`
      once this lands), so descoping this task to a declared gap is acceptable but must be flagged
      explicitly in the PR, not silently dropped.
- [ ] 1.5 `--lite`; commit.

## 2. Statistics rebuild (opt-in, full decode) — surface: `rebuild_components`

- [ ] 2.1 `rebuild/statistics.rs`: full per-partition decode (reuse salvage's
      `decode_partition_at_offset_for_salvage` / compaction-row path, read-only) folding through
      `stats_fold::fold_mutation_stats` — the SAME fold production flush/compaction uses, so
      recomputed aggregates cannot drift from what a real flush would have produced (R4.1).
- [ ] 2.2 Repair-field recovery: attempt to parse the original Statistics.db (present at its
      expected path, or an explicit recovery-source path) for `repaired_at`/`pending_repair`/
      `is_transient`; `recovered` when it parses, `lost` (default-valued) otherwise (R4.2/R4.3).
      Origin-host/compaction-ancestry unconditionally `lost` — no field exists to populate (R4.4's
      classification map states this without attempting anything).
- [ ] 2.3 Opt-in gate: `statistics` never implied by a bare `--components` omission; must be named
      explicitly (R4.4).
- [ ] 2.4 Tests: `issue_4197_rebuild_byte_parity.rs` (R1), `issue_4197_rebuild_index_parity.rs` and
      `issue_4197_rebuild_bti_index_parity.rs` (R2), `issue_4197_rebuild_summary_classification.rs`
      (R3), `issue_4197_rebuild_statistics_recompute.rs` (R4), `issue_4197_rebuild_refusal.rs` (R5).
      Expected values computed independently from the fixture's `*-Data.db.jsonl` golden or the
      clean source's own components — never from `rebuild_components`'s own output.
- [ ] 2.5 `scripts/tests/test_rebuild_no_resync_scan.sh` (R2.3), registered in `tooling-tests`.
- [ ] 2.6 Name the new targets in the gate's `core-tests`/`write-tests` list (#3522); confirm both
      new integration targets execute to completion against the real dataset corpus.
- [ ] 2.7 `--lite`; commit; push.

## 3. Review-first (library half)

- [ ] 3.1 rust-reviewer + roborev on the lite-green library diff BEFORE the CLI half starts, unless
      the CLI half is small enough to land in the same session — if so, ONE combined round covers
      both (flag explicitly in the PR per salvage's precedent, don't silently skip review-first).

## 4. `cqlite rebuild` (R7–R10) — surface: the built binary

- [ ] 4.1 `Commands::Rebuild(RebuildArgs { input, components, out, in_place, manifest, out_format })`;
      help per R8.2.
- [ ] 4.2 `commands/rebuild.rs`: schema via the GLOBAL `--schema` flag (reusing
      `write.rs::load_compaction_table_schema`); table-dir generation discovery reusing salvage's
      format-aware walker (BIG + BTI); `--in-place` refuses immediately with the #4195-dependency
      message (R8.1) — no attempt to implement the protocol's mechanics beyond the refusal path in
      this PR; exit codes per D3 via direct `std::process::exit` (mirroring `verify`'s pattern).
- [ ] 4.3 `cqlite-cli/tests/rebuild_cli_tests.rs` (R7.1–R7.3, R8.1) against the compiled binary via
      `CARGO_BIN_EXE_cqlite`. Target registered with `required-features = ["write-support"]`
      (auto-included in `cli-tests`, #3522).
- [ ] 4.4 R9.1 committed expected-manifest fixtures under `cqlite-cli/tests/fixtures/rebuild/`.
- [ ] 4.5 R10.1 verify + read-back parity test (rebuilt output vs. the ORIGINAL pre-deletion
      component set's read-sstable output, not merely "parses").
- [ ] 4.6 R6.1 memory-budget lane entry for `test_wide_rows` — implement alongside, not deferred
      (salvage deferred its equivalent; do not repeat that gap here without flagging it the same
      explicit way if time-boxing forces it).
- [ ] 4.7 Docs: `dev-cookbook.md` "Rebuild derived SSTable components" entry. Website CLI page /
      epic #4192 checklist tick: deferred to the closer/finalize step.
- [ ] 4.8 `--lite`; commit; push.

## 5. Endgame — `flow-closer`

- [ ] 5.1 Rebase; ONE full gate (`AGENT_GATE_SUMMARY_FILE` redirect); `RESULT: PASS`, tree
      integrity clean, own run-id.
- [ ] 5.2 C: `spec-auditor` vs `openspec/changes/sstable-rebuild/specs/**`.
- [ ] 5.3 Roborev LAST via `scripts/flow/roborev-review.sh --agent codex --model gpt-5.6-sol`; PASS.
- [ ] 5.4 `premerge-assert`; `HOLD:` re-read; `gh pr merge --auto --squash --delete-branch`.
- [ ] 5.5 `flow-finalize`: archive, telemetry via PR-in-worktree, close #4197, tick #4192.
