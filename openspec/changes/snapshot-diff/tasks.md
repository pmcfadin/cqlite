# Tasks — snapshot-diff (issue #4201)

Ordered. Group 0 is the blocking dependency + premises, 1 the library, 2 the CLI, 3 the endgame.
Commit after every group (#3042). `--lite` after every fix round; ONE full gate in `flow-closer`.

## 0. Blocking dependency + premises — re-confirm in the worktree, STOP and report if false

- [x] 0.0 **#4193 merged to `origin/main`** (#4546, `7dd80495a`; OpenSpec change archived
      2026-11-20). This branch is rebased on it; premises 0.1-0.4 below were re-checked against the
      merged code, not the draft branch.
- [x] 0.1 `merge::trace::{TraceSink, RecordingSink, CellDecision, TombstoneRecord, TombstoneKind,
      DecidedBy, Verdict, ProbeOutcome}` are `pub` (`merge/trace.rs`, an unconditional module);
      nothing to widen.
- [x] 0.2 The traced builder is `merge::build_traced_merger_with_registry(paths, keys, schema,
      udt_registry, scan_cancel, sink)`; `explain` then applies `with_now_secs`,
      `with_gc_before_secs(compute_gc_before(..))`, `with_purge_safe(true)`, loops `step()` to
      `MergeStep::Complete` and takes the sink back with `into_trace_sink()`. `diff` runs that
      sequence once per side, never merging the two sides' generation lists.
- [x] 0.3 Partition enumeration for the un-scoped case: `SSTableReader::open` +
      `distinct_partition_keys_with_positions(Some(schema))` (the structural walk `rebuild` uses),
      read-only, once per side; the key union is the scan set. (The draft named Index.db / Partitions.db
      boundary sources; those walkers are `pub(super)` inside salvage.)
- [x] 0.4 `merge::{compute_gc_before, effective_compaction_schema}` are `pub`.
- [x] 0.5 Fixture check: `test_tomb/resurrection_gc_positive` (2 generations) present under the
      `datasets-v4` (Cassandra 5.0.8) root with its `*.jsonl` goldens; design.md §D6's literals were
      re-derived (`local_delete_time` 1794358424, boundary 1795222424). R5.1 uses the committed
      2-generation `test_explain/trace_decisions` (design.md §D6b) because
      `test_tomb/wide_range_tombstone` is single-generation.
- [x] 0.6 (Moot: owner ruled R5 IN at Seam 1, 2026-09-23.) If proposal.md decision 3 (range-tombstone resurrection-risk scope) is ruled OUT at
      activation, drop R5/R5.1 from `specs/snapshot-diff-scan/spec.md` and re-run
      `openspec validate --strict` before implementing; do not silently under-implement an unrevised
      spec.

## 1. Diff scan — surface: `write_engine::snapshot_diff::{diff_table, diff_partition}`

- [ ] 1.1 `snapshot_diff/mod.rs`: `DiffOptions`, `DiffReport`, `PartitionDiff`, `CellDiffEntry`,
      `TombstoneDiffEntry`, `ResurrectionRisk` (design.md §D4 shape, serde).
- [ ] 1.2 `snapshot_diff/scan.rs`: drive both sides' `build_traced_merger_with_registry` +
      `RecordingSink` independently (R1); fold into winner sets; cell-status classification (R2);
      tombstone-status classification with same-both-sides omission (R3).
- [ ] 1.3 `snapshot_diff/resurrection.rs`: the shadow-check predicate (R4) — partition/row/cell/
      collection scope match plus writetime `<=` deletion_time — and the range-tombstone span variant
      (R5, if 0.6 keeps it in scope), both citing `DeletionPurger#shouldPurge` /
      `ColumnFamilyStore#gcBefore` in a doc comment (design.md §D2).
- [ ] 1.4 Partition enumeration for the un-scoped path: union of both sides' enumerated keys,
      fold-and-drop per partition (R8); `diff_partition` for the `--partition` path bypasses
      enumeration entirely.
- [ ] 1.5 `tests/issue_4201_diff_resurrection.rs` (R1.1, R2.1, R2.2, R3.1, R4.1, R4.2, R4.3, and R5.1
      if in scope) — expected values derived independently from the JSONL goldens and re-derived
      `compute_gc_before`/shadow arithmetic per design.md §D6, never from `diff_table`'s own output.
- [ ] 1.6 `tests/issue_4201_diff_symmetry.rs` (R6.1) — the generic mirror-property test, run over
      every fixture the other tests use.
- [ ] 1.7 `tests/issue_4201_diff_unreadable_generation.rs` (R7.1).
- [ ] 1.8 R8.1 memory-budget lane entry for `test_wide_rows`, matching salvage's/#4200's established
      pattern.
- [ ] 1.9 Name the new test targets in the gate's `core-tests` list (#3522).
- [ ] 1.10 `--lite`; commit.

## 2. `cqlite diff` (R9–R13) — surface: the built binary

- [ ] 2.1 `cli_types.rs`: `Commands::Diff(DiffArgs { dir_a, dir_b, table, schema, partition:
      Vec<String>, now: Option<String>, out: Option<OutputFormat> })`; help text states the read-only
      guarantee and that `--partition` restricts the point-read path (R9, R11).
- [ ] 2.2 `commands/diff.rs`: resolve each side's table dir independently via
      `explain_resolve::{resolve_table_dir, discover_live_generations}` (per proposal.md decision 1's flat-dir adoption); render the
      first line before anything else (R9.1/R9.2); call `diff_table`/`diff_partition`; render
      `text`/`json` per design.md §D4; exit codes per R12.
- [ ] 2.3 Wire in `main.rs`; `commands/mod.rs`.
- [ ] 2.4 `cqlite-cli/tests/diff_cli_tests.rs`: R9.1, R9.2, R10.1, R10.2, R11.1, R11.2, R12.1, R12.2,
      R12.3, R13.1, with committed expected-json files under `cqlite-cli/tests/fixtures/diff/`.
      **Add the target to the gate's `cli-tests` list** (#3522) and confirm it runs.
- [ ] 2.5 Docs: `docs/development/dev-cookbook.md` CLI section (one entry); epic #4192 checklist tick
      deferred to the closer/finalize step.
- [ ] 2.6 `--lite` green; commit; push.

## 3. Endgame

- [ ] 3.1 Review-first: `rust-reviewer` + `bash scripts/flow/roborev-review.sh --agent claude-code
      --model claude-opus-5` on the lite-green diff, BEFORE the first full gate.
- [ ] 3.2 Fix rounds: `--lite` re-cert + diff-scoped targets after each fix; never a full gate per
      round. Batch nits into one follow-up issue at merge time; blockers trigger
      `fix → --lite → re-review`.
- [ ] 3.3 Open the PR once review-first is clean.
- [ ] 3.4 Hand to `flow-closer`: rebase → ONE full gate → C (spec-auditor intent audit against
      `specs/snapshot-diff-scan/spec.md` + `specs/cli-diff/spec.md`) → roborev LAST →
      `premerge-assert` → arm `gh pr merge --auto` → finalize.
