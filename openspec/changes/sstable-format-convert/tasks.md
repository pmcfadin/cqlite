# Tasks — sstable-format-convert (issue #4202)

Ordered. Group 0 is premises + the write-guard ordering check, 1 the library, 2 the CLI, 3 the
endgame. Commit after every group (#3042). `--lite` after every fix round; ONE full gate in
`flow-closer`.

## 0. Premises + write-guard ordering — re-confirm in the worktree, STOP and report if false

- [ ] 0.1 Confirm the merge/reconcile pipeline's row representation (`Mutation`/`CompactionRow`) is
      genuinely format-independent up to `SSTableWriter::write_partition` — no call site upstream of
      the writer inspects or branches on the INPUT's format. If this premise is false, STOP and
      report; it changes the design, it is not a detail to route around silently (design.md D1).
- [ ] 0.2 Confirm `has_partition_level_deletions` is computed by `SSTableWriter::write_partition`
      itself from the mutation stream (not read from source Statistics) when writing `da` — i.e. a
      BIG→BTI conversion gets a CORRECT value for this field for free, not a value that needs
      independent derivation by `convert`.
- [ ] 0.3 Confirm `estimated_partition_size`/`estimated_cell_count` use the IDENTICAL seed shapes
      (EH(155)/EH(118), #1327) in both `nb` and `da` STATS bodies — re-verify from
      `writer/stats_writer/metadata.rs`, don't assume from design.md's table.
- [ ] 0.4 **Write-guard ordering check.** `git log origin/main..origin/issue-4199-extract-split` (or
      the current state of that branch): if `cqlite-cli/src/commands/write_guard.rs` already exists
      on `origin/main` (meaning #4199 merged the promotion first), task 1.6 below is "adopt it,"
      not "perform it." If it does not exist yet, this issue performs the SAME relocation #4199's
      own design.md D7 describes (`commands/salvage/write_guard.rs` → `commands/write_guard.rs`,
      `pub(crate)`, salvage's `use` updated, its existing guard tests unchanged) as task 1.6,
      FIRST, before any convert-specific guard code — mirroring #4199's own task-0.4 ordering so the
      guard used from day one is the hardened one, matching design.md D4.
- [ ] 0.5 Fixture check: `test_basic` (BIG, incl. `uncompressed_table` — zero clustering columns,
      the salvage round-4 re-encoding fixture) and `test_da` (`multiclustering_table`,
      `simple_table`, `collection_table`, `wide_table`, `wide_multiclustering_small`, `ttl_table`)
      present under the fetched `CQLITE_DATASETS_ROOT`, each with its `*.jsonl` golden.

## 1. `format_convert::convert_sstable_generation` — surface: `write_engine::format_convert`

- [ ] 1.1 `format_convert/mod.rs`: `ConvertOptions`, `ConvertReport`,
      `StatisticsAccounting{preserved: Vec<&'static str>, not_representable: Vec<(&'static str,
      &'static str)>}` (field name, reason — design.md §D2's table, serde).
- [ ] 1.2 `format_convert/convert.rs`: single-input `KWayMerger` construction (design.md D1),
      `purge_safe(false)`, `gc_before_secs: None`, `now_secs: None`; feed the row stream to
      `SSTableWriter::with_format(output_dir, generation, schema, capacity, target_format)`.
      Compressed target refuses `Error::UnsupportedFormat` BEFORE any read/decode work starts (R4.1).
- [ ] 1.3 Statistics accounting per §D2: `repaired_at`/`pending_repair`/`is_transient` threaded via
      the SAME `set_repair_state` call `salvage`'s D4 uses; `has_partition_level_deletions` recorded
      in `statistics_not_representable` on a BTI→BIG conversion when the source carried it (task 0.2
      confirms the writer itself handles the BIG→BTI direction).
- [ ] 1.4 Library tests (`issue_4202_convert_roundtrip.rs`): R1.1 (every committed `test_basic`/
      `test_da` table, both directions), R1.2 (the declared zero-clustering-column content-parity
      fallback), R2.1/R2.2 (structural BTI assertions in the `issue_3002_bti_rows_root_base.rs`
      style, run against `convert`'s actual output), R3.1–R3.3 (Statistics field accounting,
      preserved and not-representable sets), R4.1.
- [ ] 1.5 Name the new test target in the gate's `core-tests`/`bti-multiclustering` lists (#3522).
- [ ] 1.6 Write-guard promotion OR adoption per task 0.4's finding.
- [ ] 1.7 `--lite`; commit.

## 2. `cqlite convert` (R5–R8) — surface: the built binary

- [ ] 2.1 `Commands::Convert(ConvertArgs { input, out, format, compression })` in `cli_types.rs`;
      `--format` has NO `default_value` (R5.2); `--compression` defaults `none`, `ValueEnum` with
      only `None` accepted today (documented as such, not merely undocumented-but-unreachable).
- [ ] 2.2 `commands/convert.rs`: schema via the global `--schema` flag
      (`load_compaction_table_schema_for_table`); table-dir generations discovered the same way
      `salvage`'s local walker does (BIG AND BTI generation filenames); `WriteGuard` instances for
      the input dir and the run's own `--out/<ks>/<tbl>/` (R7).
- [ ] 2.3 `cqlite-cli/tests/convert_cli_tests.rs`: R5.1 (both directions), R5.2, R5.3, R6.1, R7.1,
      R7.2 (both the success and refusal legs), R8.1. Target registered with `required-features =
      ["write-support"]` (auto-included in the gate's `cli-tests` via `required-features`
      derivation, #3522).
- [ ] 2.4 Docs: `dev-cookbook.md` "Convert an SSTable between BIG and BTI" entry. Website CLI page /
      epic #4192 checklist tick: deferred to the closer/finalize step.
- [ ] 2.5 `--lite`; commit; push.

## 3. Endgame

- [ ] 3.1 Review-first: `rust-reviewer` + `bash scripts/flow/roborev-review.sh --agent claude-code
      --model claude-opus-5` on the lite-green diff, BEFORE the first full gate.
- [ ] 3.2 Fix rounds: `--lite` re-cert + diff-scoped targets after each fix; never a full gate per
      round. Batch nits into one follow-up issue at merge time; blockers trigger
      `fix → --lite → re-review`. Expect the SAME class of destructive-path findings salvage's
      round-23 review surfaced if the write guard is re-derived rather than reused — a strong reason
      task 0.4/1.6 comes first, not late.
- [ ] 3.3 Open the PR once review-first is clean.
- [ ] 3.4 Hand to `flow-closer`: rebase → ONE full gate → C (spec-auditor intent audit against
      `specs/sstable-format-convert/spec.md` + `specs/cli-convert/spec.md`) → roborev LAST →
      `premerge-assert` → arm `gh pr merge --auto` → finalize.
