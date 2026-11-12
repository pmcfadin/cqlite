# Tasks — sstable-diagnose (issue #4204)

Ordered. Group 0 is premises, 1–3 the library, 4 the CLI, 5 the endgame. Commit after every group
(#3042 work-loss insurance). `--lite` after every fix round; ONE full gate in `flow-closer`.

## 0. Premises — re-confirm in the worktree, STOP and report if false

- [ ] 0.1 Confirm `StatisticsReader`/`SSTableStatistics` (`statistics_reader.rs`, `parser/
      statistics.rs`) already exposes, per generation: `timestamp_stats` (min/max timestamp, min/max
      LDT), `row_stats.partition_count`, `compression_stats`, `tombstone_drop_times`, and the
      `Option`-honest #1653 fields. Note any field the design needs that is NOT yet read-side
      decoded (the estimated histograms themselves — `estimatedCellPerPartitionCount`,
      `estimatedPartitionSize` — are confirmed WRITE-side ported in `estimated_histogram.rs`; confirm
      whether the READ side already decodes them back out, or whether this change's first library
      task is that read-side decode).
- [ ] 0.2 Confirm `write_engine::merge::fully_expired::is_fully_expired`'s visibility allows a
      read-only caller outside `write_engine` (it is `pub(super)` today per its module doc) — if not,
      the minimal visibility change (`pub(super)` → `pub(crate)`), not a reimplementation.
- [ ] 0.3 Confirm the existing full-scan/streaming-iterator primitive `compact_sstables`'s producer
      thread uses is reachable read-only (no writer attached) for `--deep`'s single pass — name the
      exact function `diagnose` will call.
- [ ] 0.4 Confirm `reader::types`' `first_key`/`last_key` are populated for both BIG and BTI readers
      (R4's token-overlap primitive needs both formats).
- [ ] 0.5 Generate the `sstablemetadata` text-oracle captures (R2.1) at the pinned `cassandra-5.0.8`
      tool build against the fixtures this change uses; commit them under
      `test-data/` with the tool version recorded, per the issue's AC 3.
- [ ] 0.6 Confirm the `MergeStats`/purge tally (D1's "reclaim_at_now" field) can be computed via a
      dry invocation of the existing compaction pre-pass without writing — name the function or
      report the smallest addition needed.

## 1. Core: cheap tier

- [ ] 1.1 `cqlite-core/src/storage/sstable/diagnose/mod.rs` (new file): `DiagnoseOptions`,
      `DiagnoseReport`, `diagnose_table(table_dir, options) -> Result<DiagnoseReport>` skeleton —
      per-generation cheap-tier fields via `StatisticsReader` — R1, R2.1, R5.1.
- [ ] 1.2 `diagnose/droppable_ratio.rs`: port `getEstimatedDroppableTombstoneRatio` exactly
      (`design.md` D3), taking `gc_before_secs` from the shared `compute_gc_before` helper — R2.1.
- [ ] 1.3 Wire `is_fully_expired` read-only for the fully-expired-at-now prediction — R2.2.
- [ ] 1.4 Provenance tagging (`source`/`cause` on every leaf) — R1.1.

## 2. Core: `--deep` scan + token overlap

- [ ] 2.1 `diagnose/deep_scan.rs`: one streaming pass producing partition-size/clustering-width
      histograms and `--top N` rankings (largest, most-tombstoned — RAW count, D2) — R3.1, R3.2.
- [ ] 2.2 `diagnose/reclaim_prediction.rs`: dry full-compaction-at-`--now` tally reusing the existing
      `MergeStats`/purge counters, no write — D1's `reclaim_at_now`.
- [ ] 2.3 `diagnose/token_overlap.rs`: fixed-`K`-bucket generation-overlap computation from
      `first_key`/`last_key` alone — R4.1.
- [ ] 2.4 Memory-budget lane entry for `test_wide_rows` under `--deep` — R3.3. Name the new target in
      the gate's memory-budget list (#3522 discipline).

## 3. Core tests

- [ ] 3.1 `cqlite-core/tests/issue_4204_diagnose_provenance.rs` — R1.1.
- [ ] 3.2 `cqlite-core/tests/issue_4204_diagnose_cheap_tier.rs` — R2.1, R2.2, R5.1.
- [ ] 3.3 `cqlite-core/tests/issue_4204_diagnose_deep_scan.rs` — R3.1, R3.2, R3.3.
- [ ] 3.4 `cqlite-core/tests/issue_4204_diagnose_token_overlap.rs` — R4.1.

## 4. CLI

- [ ] 4.1 `cqlite-cli/src/commands/diagnose.rs` (new file): arg parsing, no `--schema` requirement
      for the cheap tier, `--out text|json` as a RENDER format (reuse `VerifyOutputArg`'s enum
      shape, not `SalvageOutFormatArg`'s) — D7.
- [ ] 4.2 `cli_types.rs`: `Commands::Diagnose(DiagnoseArgs)`, one variant; `--help` states plainly
      that `--out` never writes a file and that this verb never modifies anything.
- [ ] 4.3 Text rendering: every `estimated_droppable_tombstone_ratio` /
      `top_tombstone_heaviest_partitions` field's description names `cqlite tombstones` (#4200) —
      R7.2.
- [ ] 4.4 `cqlite-cli/tests/diagnose_cli_tests.rs` — R6.1–R6.4, R7.1, R7.2 (name every target in
      `cli-tests`).

## 5. Endgame

- [ ] 5.1 `--lite` after every fix round (summary-file redirect); fix rounds re-cert `--lite`, never
      a repeat full gate.
- [ ] 5.2 Review-first: `rust-reviewer` + `roborev` (`bash scripts/flow/roborev-review.sh --agent
      claude-code --model <accessible-model>`) on the lite-green diff BEFORE the first full gate.
- [ ] 5.3 Open PR.
- [ ] 5.4 Hand off to `flow-closer`: rebase → ONE full gate (`AGENT_GATE_SUMMARY_FILE=...`,
      summary-file redirect, never raw stdout) → `spec-auditor` C intent audit against this change's
      `specs/**` → roborev LAST (a rebase voids an earlier round) → `premerge-assert` → arm
      `gh pr merge --auto --squash --delete-branch`.
- [ ] 5.5 `flow-finalize`: archive this change (sync delta specs into `openspec/specs/`), remove the
      worktree + branch, close #4204 with a traceable comment, record delivery telemetry.
