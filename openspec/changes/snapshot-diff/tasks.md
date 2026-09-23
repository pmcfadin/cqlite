# Tasks — snapshot-diff (issue #4201)

Ordered. Group 0 is the blocking dependency + premises, 1 the library, 2 the CLI, 3 the endgame.
Commit after every group (#3042). `--lite` after every fix round; ONE full gate in `flow-closer`.

## 0. Blocking dependency + premises — re-confirm in the worktree, STOP and report if false

- [ ] 0.0 **BLOCKING: #4193 must be Seam-1 approved AND merged to `origin/main` before any task
      below starts.** At spec time #4193's OpenSpec change is drafted and `openspec validate --strict`
      clean on branch `issue-4193-forensics-explain`, but epic #4192's own tracking line reads
      "parked in Backlog, re-activate by adopting the branch" — **do not treat #4193 as approved.**
      Confirm current status (`gh issue view 4193 --json labels,state`) before doing anything else;
      if still unapproved/unmerged, stop and report back rather than implementing against a moving
      branch. Once merged, rebase this branch onto `origin/main` and re-run 0.1–0.4 against the
      merged code, not the branch snapshot this spec was written against.
- [ ] 0.1 Confirm `merge::trace::{TraceSink, RecordingSink, CellDecision, TombstoneRecord,
      TombstoneKind, DecidedBy}` visibility from a new sibling module under `write_engine/` — if
      private to `merge`, the first library task is a minimal `pub(crate)` widening, not a design
      change (design.md §D1).
- [ ] 0.2 Confirm `build_single_partition_merger`'s exact signature and how #4193's `explain` drives
      it over ALL of one table's generations — `diff` calls it twice (once per side), never merging
      the two sides' generation lists together.
- [ ] 0.3 Confirm the authoritative partition enumeration primitive `salvage`/#4200's `tombstones`
      use (BIG `Index.db` entries; BTI `Partitions.db` trie walk) is reusable read-only, over TWO
      independent table directories, for the un-scoped (no `--partition`) full-table case.
- [ ] 0.4 Confirm `compute_gc_before(schema, now)` visibility from the new module (same check #4200's
      Group 0 makes; do not duplicate its confirmation if #4200 has already landed and settled it —
      cite that instead of re-deriving).
- [ ] 0.5 Fixture check: `test_tomb/resurrection_gc_positive` (2 generations) present under the
      fetched `CQLITE_DATASETS_ROOT` with its `*.jsonl` goldens for both generations; confirm the
      exact `local_delete_time`/`marked_deleted` values design.md §D6 cites still match the committed
      fixture (re-derive the epoch arithmetic if the fixture has changed since this spec was written).
- [ ] 0.6 If proposal.md open decision 3 (range-tombstone resurrection-risk scope) is ruled OUT at
      activation, drop R5/R5.1 from `specs/snapshot-diff-scan/spec.md` and re-run
      `openspec validate --strict` before implementing; do not silently under-implement an unrevised
      spec.

## 1. Diff scan — surface: `write_engine::snapshot_diff::{diff_table, diff_partition}`

- [ ] 1.1 `snapshot_diff/mod.rs`: `DiffOptions`, `DiffReport`, `PartitionDiff`, `CellDiffEntry`,
      `TombstoneDiffEntry`, `ResurrectionRisk` (design.md §D4 shape, serde).
- [ ] 1.2 `snapshot_diff/scan.rs`: drive both sides' `build_single_partition_merger` +
      `RecordingSink` independently (R1); fold into winner sets; cell-status classification (R2);
      tombstone-status classification with same-both-sides omission (R3).
- [ ] 1.3 `snapshot_diff/resurrection.rs`: the shadow-check predicate (R4) — partition/row/cell/
      collection scope match plus writetime `<=` deletion_time — and the range-tombstone span variant
      (R5, if 0.6 keeps it in scope), both citing `DeletionPurger#shouldPurge` /
      `ColumnFamilyStore#gcBefore` in a doc comment (design.md §D2).
- [ ] 1.4 Partition enumeration for the un-scoped path: union of both sides' boundary-source keys,
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
- [ ] 2.2 `commands/diff.rs`: resolve each side's table dir independently via the same helpers
      `query`/`explain`/`tombstones` use (per proposal.md decision 1's flat-dir adoption); render the
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
- [ ] 3.3 Open the PR once review-first is clean; the PR body states the #4193 dependency (merged
      SHA) explicitly.
- [ ] 3.4 Hand to `flow-closer`: rebase → ONE full gate → C (spec-auditor intent audit against
      `specs/snapshot-diff-scan/spec.md` + `specs/cli-diff/spec.md`) → roborev LAST →
      `premerge-assert` → arm `gh pr merge --auto` → finalize.
