# Tasks: roborev-vacuous-review-guard (issue #2964)

> Design decided in `design.md`: a fail-closed CQLite-side wrapper (`roborev` is an external binary we
> cannot patch), with a locally-computed `git` diff census as the oracle, a deterministic
> verdict-text-vs-census check as the primary vacuity assert, and bounded token accounting as a
> corroborating fail-closed-only tier. AC→requirement map is at the top of
> `specs/roborev-review-guard/spec.md`.

## 1. The sanctioned wrapper (surface: `scripts/flow/roborev-review.sh`)
- [x] Create the wrapper skeleton: flag parsing (`--repo`, `--base`, `--agent`, `--model`, passthrough),
      absolute repo-root resolution, branch + HEAD resolution, and the three-way exit-code contract
      (0 = PASS, 1 = FAIL, 3 = NOTHING-TO-REVIEW).
- [x] Declare the named vacuity threshold constants at the top of the script with the measured evidence
      table (jobs 4651/4652/4654/4656/4658/4659) cited in a comment.
- [x] Step 2 — push assert (AC3): `origin/<branch>` exists and equals HEAD, else FAIL naming the
      unpushed commits, before any review is enqueued.
- [x] Step 3 — local diff census oracle: `git diff --numstat <base>...HEAD` → files/+/−; a genuinely
      empty census exits `NOTHING-TO-REVIEW` without enqueuing a review.
- [x] Step 4 — invocation (AC2): explicit HEAD sha + explicit absolute `--repo` + `--wait`; refuse when
      only one of `--agent`/`--model` is supplied; never bare `--branch`, never the two-positional range form.
- [x] Step 5 — reviewed-SHA assert (AC2): parse `Enqueued job <N> for <sha>`, require a prefix match with
      HEAD; FAIL on mismatch, naming the base ref explicitly when the mismatched sha equals it; FAIL on an
      absent/unparseable enqueue line.
- [x] Step 6 tier 1 (AC1, primary): a "contains no code changes to review" / "no code changes" verdict
      against a non-empty census is a HARD FAIL.
- [x] Step 6 tier 2 (AC1, corroborating): read token accounting via `roborev show <N> --json` (fallback
      `roborev list --json`); FAIL on input below threshold OR zero cached input OR output below threshold;
      print observed-vs-threshold in the message; stamp an explicit degraded-signal notice (never a silent
      skip) when accounting is unavailable, with tier 1 still governing.
- [x] Step 7 — emit the `==== ROBOREV REVIEW SUMMARY ====` block (repo, branch, head-sha, reviewed-sha,
      job, base, census, tokens, per-check verdicts, terminal `RESULT:`), write the raw transcript to a log
      path named in the block, and exit non-zero on any non-PASS outcome.
- [ ] Step 7b — surface the reviewer's own exit status under its own greppable key `roborev-exit:`
      (`PASS` when the `roborev` process exited zero, else a `FAIL` carrying the observed code), placed
      with the other per-check keys ahead of the terminal `RESULT:` and included in the per-check scan.
      The fail-closed BEHAVIOUR already exists (a non-zero reviewer exit forces `RESULT: FAIL` plus an
      `ERROR:` detail line); only the greppable key is missing, so this is a one-line addition to
      `emit_summary` plus its verdict-scan entry — and a matching hermetic case.
- [x] Hygiene pass: keep the script small (campsite spirit), quote every interpolation of external
      output, and walk CLAUDE.md's pre-roborev self-check list over the diff.

## 2. Hermetic regression check (surface: `scripts/tests/test_roborev_review_guard.sh`)
- [x] Build the stub `roborev` fixture (first on `PATH`) that replays the recorded enqueue lines, verdict
      text, and `show --json` token payloads from the evidence table, plus throwaway `git init` fixtures
      with a synthetic `origin` remote.
- [x] Case (a): enqueued sha == base ref → FAIL, message names the base ref.
- [x] Case (b): enqueued sha is neither endpoint → FAIL.
- [x] Case (c): "contains no code changes to review" against a non-empty census → FAIL.
- [x] Case (d): vacuous token signature (≈18k input / 0 cached / <60 output) → FAIL.
- [x] Case (e): unpushed branch → FAIL, no review enqueued.
- [x] Case (f): genuine review (matching sha, ~500k/387k/6.3k accounting) → PASS.
- [x] Case (g): genuinely empty census → `NOTHING-TO-REVIEW` with its own non-zero exit code, never PASS.
- [x] Case: unavailable token accounting → degraded-signal notice stamped, tier 1 still applied.
- [x] Assert the summary block's header is distinct from all three agent-gate summary headers.
- [x] Confirm hermeticity: no network, no real roborev, no dataset corpus; and no wall-clock threshold
      assert in the correctness path (#2642).

## 3. Gate wiring (surface: `scripts/agent-gate.sh`)
- [x] Register the check in `run_roborev_lints_cmd()` (component `roborev-lints`, present in BOTH
      `LITE_COMPONENTS` and `COMPONENTS`) so a regression FAILs the fast `--lite` loop — the stated
      acceptance goal — following the existing `check-workflow-injection.sh` / `check-no-wallclock-asserts.sh`
      chaining pattern.
- [x] Append the check to `run_tooling_tests()` (full-gate `tooling-tests`) following the
      `test_check_dockerfile_rust_pin.sh` / `test_check_skill_flag_tables.sh` guard pattern (FAIL the
      component on non-zero, with a named failure line).
- [x] Verify runtime is fast enough for `--lite` and that `scripts/agent-gate.sh --list` /
      `--lite-list` still reflect the intended component set.

## 4. Call-site migration (surface: `.claude/skills/**`, `.claude/agents/**`)
- [x] `.claude/skills/flow-implement/SKILL.md` — the review-first step (the primary call site; replaces
      the documented bare `--branch` command).
- [x] `.claude/agents/flow-closer.md` — the final confirmation pass (the merge-gating call site; also
      removes the non-existent `/roborev-review-branch` form) and treat a non-PASS terminal `RESULT`,
      including `NOTHING-TO-REVIEW`, as a blocked merge.
- [x] `.claude/agents/flow-lead.md` — the stage table's roborev row.
- [x] `.claude/skills/{flow-activate,flow-address,flow-finalize,ci-cd-validation}/SKILL.md` — every
      roborev reference routed through the wrapper.
- [x] `.claude/agents/{rust-reviewer,sstable-developer,test-validator}.md` — every roborev reference
      routed through the wrapper.
- [x] Sweep for any remaining bare `--branch` roborev instruction across `.claude/**` and mark the form
      non-sanctioned; preserve the both-`--agent`-and-`--model` requirement at every call site.

## 5. Doctrine — ships in this change (AC4)
- [x] CLAUDE.md — update the roborev-invocation bullet in *Agent-Team Conventions*: the wrapper is the
      only sanctioned invocation; verify the reviewed SHA; "contains no code changes to review" on a
      non-empty diff is a HARD FAIL; docs-only diffs cannot be roborev-certified.
- [x] `website/src/content/docs/agents-developing/roborev-findings.md` — same four rules, plus a row in
      the "mechanized in `--lite`" table for the new `roborev-lints` guard.
- [ ] Verify publication by grepping the SERVED page for a distinctive new phrase (an HTTP 200 is not
      proof; CDN staleness ≈3 min) and re-check after a wait if absent.

## 6. Live worktree probe (AC5)
- [x] Document the probe (wrapper usage text + the doctrine page): from a real `issue-<N>-*` worktree with
      a pushed commit and the root checkout on `main`, run the wrapper and confirm the summary block's
      `reviewed-sha` equals the worktree HEAD and does NOT equal the base ref.
- [ ] Run the probe once against the real binary and record the observed summary-block values (head-sha,
      reviewed-sha, job, census, tokens) in the PR body as the AC5 live evidence.

## 7. Gate + review + sign-off
- [x] `scripts/agent-gate.sh --lite` green each fix round (summary-file redirect; anchor the poll on
      `RESULT: (PASS|FAIL)`).
- [ ] `rust-reviewer` (no Rust here — skip if it has nothing to review) + roborev on the lite-green diff
      via the NEW wrapper (review-first). Note: this change's own diff is largely docs + shell; if the
      wrapper FAILs it as code-free, record primary-source/self-test evidence per the docs-only rule
      instead of "roborev clean".
- [ ] Full gate ONCE via `flow-closer`; verify `tree-integrity:` alongside `RESULT:`.
- [ ] **C (spec-auditor)** anchored to `openspec/changes/roborev-vacuous-review-guard/specs/**`: every
      requirement `satisfied` with public-surface (wrapper + regression-check + doctrine) evidence.
- [ ] roborev clean or a recorded docs-only substitute; blockers fixed pre-merge, nits batched to ONE
      linked follow-up issue.
- [ ] File the upstream follow-up noted in `design.md` (worktree-aware `--branch` resolution; non-zero
      exit on a discarded code-free diff) so the external-binary gap is tracked.
- [ ] `openspec validate roborev-vacuous-review-guard --strict` clean; `openspec archive` after merge.
