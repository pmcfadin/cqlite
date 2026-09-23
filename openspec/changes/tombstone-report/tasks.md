# Tasks — tombstone-report (issue #4200, slice 1)

Ordered. Group 0 is the blocking dependency + premises, 1 the library, 2 the CLI, 3 the endgame.
Commit after every group (#3042). `--lite` after every fix round; ONE full gate in `flow-closer`.

## 0. Blocking dependency + premises — re-confirm in the worktree, STOP and report if false

- [ ] 0.0 **BLOCKING: #4193 must be merged to `origin/main` before any task below starts.** At spec
      time it is Seam-1 approved and implementing on branch `issue-4193-forensics-explain`, not on
      `main`. Rebase this branch onto `origin/main` once #4193 lands; re-run 0.1–0.4 against the
      merged code, not the branch snapshot this spec was written against.
- [ ] 0.1 Confirm `merge::trace::{TraceSink, RecordingSink, CellDecision, TombstoneRecord,
      TombstoneKind, DecidedBy}` visibility from a new sibling module under `write_engine/` — if
      private to `merge`, the first library task is a minimal `pub(crate)` widening, not a design
      change (design.md D1).
- [ ] 0.2 Confirm `build_single_partition_merger`'s full-table (not single-key) entry point, or the
      lowest-friction way to drive it over every partition of a table — check whether `explain`'s own
      CLI/library split already exposes a per-partition-across-generations helper this scan can call
      once per enumerated partition, vs. needing its own thin wrapper.
- [ ] 0.3 Confirm the authoritative partition enumeration primitive `salvage`'s boundaries.rs
      established (BIG `Index.db` entries via `parse_big_index_entry`; BTI `Partitions.db` trie walk
      via `iterate_partitions_in_bti_file`) is reusable read-only from this new module without pulling
      in salvage's write-recovery machinery.
- [ ] 0.4 Confirm `compute_gc_before(schema, now)` visibility (`pub(crate)` in `merge/mod.rs` today —
      confirm still true post-#4193-merge) from the new module.
- [ ] 0.5 Fixture check: `test_tomb/wide_range_tombstone`, `test_compaction_tombstone_ttl/rt_cross_gen`,
      `test_tomb/gc_before_boundary`, and every other `test_tomb`/`test_compaction_tombstone_ttl`
      subdirectory present under the fetched `CQLITE_DATASETS_ROOT`, each with its `*.jsonl` golden.

## 1. Aggregation scan — surface: `write_engine::tombstone_density::scan_tombstone_density`

- [ ] 1.1 `tombstone_density/mod.rs`: `DensityOptions`, `DensityReport`, `TombstoneKindTotals`,
      `RangeTombstoneEntry` (design.md §D3 shape, serde).
- [ ] 1.2 `tombstone_density/scan.rs`: partition enumeration → per-partition
      `build_single_partition_merger` + `RecordingSink`, full-compaction config
      (`purge_safe(true)`, `gc_before_secs = compute_gc_before(schema, now)`, `now_secs = Some(now)`)
      → fold into `DensityReport`, drop the partition's trail before the next.
- [ ] 1.3 Range-tombstone shadow-count fold: for each `TombstoneRecord{kind: Range, ..}`, count
      `CellDecision`s whose `decided_by` names that marker with verdict `ShadowedByTombstone(Range)`.
- [ ] 1.4 `--top N` truncation with affirmative `range_tombstones_truncated`, widest-shadow-first
      ordering.
- [ ] 1.5 R4 library tests (`issue_4200_tombstone_report.rs`): R1.1, R1.2, R2.1, R2.2, R3.1 — expected
      values computed independently from JSONL goldens / re-derived `compute_gc_before`, per §D4.
      Never assert against `scan_tombstone_density`'s own output as the oracle.
- [ ] 1.6 R4.1 memory-budget lane entry for `test_wide_rows`, matching salvage's established pattern.
- [ ] 1.7 Name the new test target in the gate's `core-tests` list (#3522).
- [ ] 1.8 `--lite`; commit.

## 2. `cqlite tombstones` (R5–R8) — surface: the built binary

- [ ] 2.1 `Commands::Tombstones(TombstonesArgs { input, schema, now, top, out_format })` in
      `cli_types.rs`; `--out` here is the RENDER format (`text|json`), never a write path — name the
      distinction explicitly in `--help` (R7.1) since `salvage`/`compact` both use `--out` for a write
      destination and this verb deliberately does not.
- [ ] 2.2 `commands/tombstones.rs`: resolve schema/table via the same helpers `query`/`explain` use;
      render the first line (`now=…`) before anything else, matching `explain` R5.1/R5.2 verbatim.
- [ ] 2.3 `cqlite-cli/tests/tombstones_cli_tests.rs`: R5.1, R5.2, R6.1 (committed expected JSON,
      volatile `now` normalised), R6.2, R7.1, R7.2 (byte-identical input before/after), R8.1. Target
      registered with `required-features = ["write-support"]` (matching `explain`/`salvage`'s gate
      auto-inclusion via `required-features` derivation, #3522).
- [ ] 2.4 Docs: `dev-cookbook.md` "Report tombstone density and range coverage" entry. Website CLI
      page / epic #4192 checklist tick: deferred to the closer/finalize step.
- [ ] 2.5 `--lite`; commit; push.

## 3. Endgame

- [ ] 3.1 Review-first: `rust-reviewer` + `bash scripts/flow/roborev-review.sh --agent claude-code
      --model claude-opus-5` on the lite-green diff, BEFORE the first full gate.
- [ ] 3.2 Fix rounds: `--lite` re-cert + diff-scoped targets after each fix; never a full gate per
      round. Batch nits into one follow-up issue at merge time per the nit-batching doctrine; blockers
      trigger `fix → --lite → re-review`.
- [ ] 3.3 Open the PR once review-first is clean.
- [ ] 3.4 Hand to `flow-closer`: rebase → ONE full gate → C (spec-auditor intent audit against
      `specs/tombstone-density-scan/spec.md` + `specs/cli-tombstones/spec.md`) → roborev LAST →
      `premerge-assert` → arm `gh pr merge --auto` → finalize.
- [ ] 3.5 At finalize, file the slice-2 follow-up issue for `cqlite purge` (design.md §D5), gated on
      the owner's rulings on the three open product decisions in proposal.md — do not silently fold
      purge into this issue's closure.
