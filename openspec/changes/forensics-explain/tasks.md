# Tasks — forensics-explain (issue #4193)

Ordered. Groups 1–3 are F0 (engine), 4–5 are F1 (CLI), 6 is the endgame. Commit after every group
(work-loss insurance, #3042). `--lite` after every fix round; ONE full gate, in `flow-closer`.

## 0. Premises — re-confirm in the worktree, STOP if false

- [ ] 0.1 `merge/reconcile.rs::ReconcileState` still exposes the eight step methods design.md §D1
      names, and `mod.rs::apply_range_shadowing` is the only range-shadow site.
- [ ] 0.2 `build_single_partition_merger` returns a merger the caller drives with `step()` and its
      `paths` order is the run-index order (point_read.rs doc).
- [ ] 0.3 Every §D6 fixture dir exists under the root `fetch-datasets.sh --verify-only` names, with
      the generation counts listed. Name the `collection` fixture or generate + commit its JSONL.

## 1. Campsite relocation (D5) — surface: byte-parity suites

- [ ] 1.1 Move `reconcile_cluster*` + `apply_range_shadowing` + coalesce helpers from `merge/mod.rs`
      into `merge/reconcile_cluster.rs`, pure relocation. `--lite`; `compaction-byte-parity` green.
- [ ] 1.2 Commit.

## 2. Trace sink (R1, R2, R3, R4) — surface: `cqlite_core::storage::write_engine::merge::trace`

- [ ] 2.1 `merge/trace.rs`: `TraceSink`, `NoTrace`, `RecordingSink`, `Verdict`, `TombstoneKind`,
      `CellDecision`, `DecidedBy`, `ProbeOutcome`. `pub mod trace;` unconditional (pub-surface guard).
- [ ] 2.2 `KWayMerger<S: TraceSink = NoTrace>`; `with_trace_sink`; thread `&mut S` through
      `reconcile_cluster_*` into `ReconcileState` steps; emit at each §D1 site; range site in 1.1's
      file; probe outcomes from the point-read builder's `PathProbe`.
- [ ] 2.3 `tests/issue_4193_trace_sink_zero_cost.rs` (R1.2).
- [ ] 2.4 `tests/issue_4193_verdict_fixtures.rs` (R2.2, R2.3) — one test per verdict, literals
      cite `cassandra-5.0.8` source, per-table root resolution, fail-closed on absent fixture.
- [ ] 2.5 `tests/issue_4193_traced_equals_untraced.rs` (R3.1) + truncated-Statistics case (R4.1).
- [ ] 2.6 `scripts/tests/test_trace_verdict_sites.sh` (R2.1), registered in `tooling-tests`; confirm
      the census line shows it EXECUTED.
- [ ] 2.7 `--lite` green; commit. Run `cargo bench -p cqlite-core --bench compaction` locally as an
      early read on R1.1 (advisory; the CI workflow is the assertion).

## 3. Review-first (engine half)

- [ ] 3.1 Push; `rust-reviewer` on the diff; `bash scripts/flow/roborev-review.sh --agent <agent>
      --model <model>`; fix blockers → `--lite` → re-review. Nits batched to the follow-up.

## 4. `cqlite explain` (R5, R7, R8, R9) — surface: the built binary

- [ ] 4.1 `cli_types.rs`: `Commands::Explain { table, partition_key, clustering: Vec<String>,
      now: Option<String>, out: Option<OutputFormat> }`; help text per D7.
- [ ] 4.2 `commands/explain.rs`: resolve dir + schema as `query` does; parse key literals with the
      query literal parser; build via `build_single_partition_merger` over ALL generations; configure
      per D3 (`now`, `compute_gc_before`, `purge_safe=true`, `effective_compaction_schema`); drive
      with `RecordingSink`; render `table`/`json`/`csv`; exit codes per D4; first line per R5.
- [ ] 4.3 Wire in `main.rs`; `commands/mod.rs`.
- [ ] 4.4 `cqlite-cli/tests/explain_cli_tests.rs` (R5.1–R5.3, R7.1–R7.3, R8.1–R8.3, R9.1) with
      committed expected-json files per fixture under `cqlite-cli/tests/fixtures/explain/`.
      **Add the target to the gate's `cli-tests` list** (#3522) and confirm it runs.
- [ ] 4.5 `cqlite-core/tests/issue_4193_explain_vs_select.rs` (R6.1) — both `CQLITE_READ_PATH`
      values, both directions, column named on failure.
- [ ] 4.6 Docs: `docs/development/dev-cookbook.md` CLI section (one entry); website CLI page if one
      lists verbs; `docs/architecture/forensics-surface-2026-09.md` status line → "F0+F1 shipped #4193".
- [ ] 4.7 `--lite` green; commit; push.

## 5. Review-first (CLI half)

- [ ] 5.1 `rust-reviewer` + roborev (sanctioned script) on the lite-green diff; fix → `--lite` →
      re-review. Open the PR (body carries the LITE summaries so far).

## 6. Endgame — `flow-closer`

- [ ] 6.1 Rebase on `origin/main`; `AGENT_GATE_SUMMARY_FILE=… bash scripts/agent-gate.sh` ONCE;
      `RESULT: PASS`, `tree-integrity` clean, `dirty: no`, own `run-id`. If `file-size` shows
      `OPT-OUT`, the PR body says which file and links #1116.
- [ ] 6.2 C: `spec-auditor` against `openspec/changes/forensics-explain/specs/**` — every
      requirement `satisfied` with a public-surface test as evidence.
- [ ] 6.3 Roborev LAST via `bash scripts/flow/roborev-review.sh --agent <agent> --model <model>`;
      `RESULT: PASS`. Any rebase after this voids it.
- [ ] 6.4 `bash scripts/flow/premerge-assert.sh <pr> <sha>`; re-read for `HOLD:`;
      `gh pr merge --auto --squash --delete-branch`.
- [ ] 6.5 `flow-finalize`: archive the change, telemetry record via PR-in-worktree, close #4193 with
      a traceable comment, tick F0+F1 on epic #4192.
