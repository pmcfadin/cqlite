# Tasks — sstable-scrub (issue #4198)

Ordered. Group 0 is premises, 1–3 the library, 4 the CLI, 5 the endgame. Commit after every group
(#3042 work-loss insurance). `--lite` after every fix round; ONE full gate in `flow-closer`.

## 0. Premises — re-confirm in the worktree, STOP and report if false

- [ ] 0.1 `compact_sstables`/`KWayMerger`'s existing streaming producer (`merge/mod.rs`,
      `producer_iter`) exposes enough of a seam to intercept per-partition/per-row DECODED SOURCE
      order before the heap re-routes it (D1's "order gate" needs the source's own physical order,
      not the merge's chosen output order). Confirm the seam or the smallest addition needed; do not
      widen scope beyond what D1 requires.
- [ ] 0.2 `salvage::decode_partition_at_offset_for_salvage` and `LossClass` are `pub(crate)` or can be
      made reachable from a sibling `write_engine::scrub` module without duplicating logic (R3).
      Confirm the actual visibility and the minimal export needed.
- [ ] 0.3 Confirm `cqlite-cli/src/commands/salvage/write_guard.rs`'s guard helper is reusable as-is
      by a second command module (R7.5) — a free function or a struct scrub can call directly, not
      something entangled with `SalvageArgs`.
- [ ] 0.4 Confirm test-data availability for R2.1's fixture: is a fixed-offset Index.db entry-swap
      mutation already scripted anywhere (salvage's `corrupt_byte_fixture.rs` family), or does this
      change need to add the mutation script itself? Either is in scope; report which.
- [ ] 0.5 Confirm `issue_1011_ttl_local_deletion_parity`'s existing fixtures include a BIG `na`/`nb`
      generation with a genuinely overflowed local-deletion-time row (R5.1), and a `test_da` table
      to use as R5.2's no-op control.
- [ ] 0.6 Surface the D4 counter-table/default-abort decision (Non-goals + design.md D4) to the owner
      at Seam 1 render — do not resolve it unilaterally in code before approval.

## 1. Core: the order gate + `ScrubReport` skeleton

- [ ] 1.1 `cqlite-core/src/storage/write_engine/scrub/mod.rs` (new file): `ScrubOptions`,
      `ScrubReport`, `scrub_table(generations, out_dir, schema, options) -> Result<ScrubReport>`
      skeleton wired to `compact_sstables` with `purge_safe = options.purge` — R1.
- [ ] 1.2 `scrub/order_gate.rs`: the per-source physical-order check from D1; diverts out-of-order
      partitions/rows into a sorted accumulator — R2.1.
- [ ] 1.3 `scrub/sidecar.rs`: writes the accumulator to `<out>/<generation>-outoforder` via the
      production `SSTableWriter` — R2.1, R6.2 (bounded).
- [ ] 1.4 Name R2.2 (intra-partition row order) explicitly in the module doc as covered-by-design,
      fixture-deferred-or-not per 0.4's finding — R2.2.

## 2. Core: `--skip-corrupted`, counter refusal, `--reinsert-overflowed-ttl`

- [ ] 2.1 Wire `--skip-corrupted` to call `decode_partition_at_offset_for_salvage` directly; assert
      loss-set equality against salvage in the SAME test binary run (R3.1).
- [ ] 2.2 Default (no `--skip-corrupted`): refuse the whole run on the first corrupted partition per
      D3/D4's recommended posture — implement the OWNER-APPROVED posture from 0.6, not a guess made
      ahead of approval (R4.1).
- [ ] 2.3 Counter-column schema check before any generation is opened for writing; refuse citing the
      write engine's existing restriction (R6.1).
- [ ] 2.4 `--reinsert-overflowed-ttl`: port `FixNegativeLocalDeletionTimeIterator`'s rule
      (`MAX_DELETION_TIME_2038_LEGACY_CAP`, `timestamp + 1`), gated the same way
      `hasUIntDeletionTime()` gates it per-format (R5.1, R5.2).

## 3. Core tests

- [ ] 3.1 `cqlite-core/tests/issue_4198_scrub_parity.rs` — R1.1, R1.2.
- [ ] 3.2 `cqlite-core/tests/issue_4198_scrub_out_of_order.rs` — R2.1 (+ R2.2's named-gap assertion).
- [ ] 3.3 `cqlite-core/tests/issue_4198_scrub_skip_corrupted_parity.rs` — R3.1, R4.1.
- [ ] 3.4 `cqlite-core/tests/issue_4198_scrub_reinsert_overflowed_ttl.rs` — R5.1, R5.2.
- [ ] 3.5 `cqlite-core/tests/issue_4198_scrub_counter_refusal.rs` — R6.1.
- [ ] 3.6 Memory-budget lane entry for `test_wide_rows` under scrub — R6.2. Name the new target in
      the gate's memory-budget list (#3522 discipline: a target unnamed there runs nowhere).

## 4. CLI

- [ ] 4.1 `cqlite-cli/src/commands/scrub/mod.rs` (new dir, campsite-clean): arg parsing, schema
      resolution reused from `compact`/`salvage`, exit-code mapping per D3.
- [ ] 4.2 `--dry-run`: compute the full report via the same library call, skip every write —
      structured so the write path is the ONLY thing `--dry-run` skips, never a second code path
      that could drift from the wet run (R8.1).
- [ ] 4.3 Wire `--manifest`/`--out` through the salvage `write_guard` helper directly (0.3's premise)
      — R7.5.
- [ ] 4.4 `cli_types.rs`: `Commands::Scrub(ScrubArgs)`, one variant, `--help` states the UNCOMPRESSED
      claim boundary (#1406) same as `salvage`'s.
- [ ] 4.5 `cqlite-cli/tests/scrub_cli_tests.rs` — R7.1–R7.4, R9.1 (name every target in `cli-tests`).
- [ ] 4.6 `cqlite-cli/tests/issue_4198_scrub_write_guard.rs` — R7.5.
- [ ] 4.7 `cqlite-cli/tests/issue_4198_scrub_dry_run.rs` — R8.1.

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
      worktree + branch, close #4198 with a traceable comment, record delivery telemetry.
