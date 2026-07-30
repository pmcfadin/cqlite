# Tasks: roborev-vacuous-review-guard (issue #2964)

> Design decided in `design.md`: a fail-closed CQLite-side wrapper (`roborev` is an external binary we
> cannot patch) whose **DETERMINISTIC** checks carry the verdict — the remote (`git ls-remote`), a local
> `git` diff census, that census's code-free classification, the job record's structured
> `git_ref`/`status`, and the census's own paths inside the prompt actually sent — with reviewer prose
> and token accounting CORROBORATING and fail-closed-only. AC→requirement map is at the top of
> `specs/roborev-review-guard/spec.md`.

## 1. The sanctioned wrapper (surfaces: `scripts/flow/roborev-review.sh`, `roborev-review-oracles.sh`, `roborev-job-facts.py`)
- [x] Create the wrapper skeleton: flag parsing (`--agent`, `--model`, `--repo`, `--base`, `--log`),
      absolute repo-root resolution, branch + full-40-char HEAD resolution, and the four-way exit-code
      contract (0 = PASS, 1 = FAIL, 3 = NOTHING-TO-REVIEW, 2 = usage error).
- [x] Reject an option supplied with an EMPTY value as a usage error (never a silent fallback to the
      default — `--repo ""` falling back to `$PWD` reviews a repo the caller did not name).
- [x] Declare the named vacuity threshold constants at the top of the script with the measured evidence
      (jobs 4651/4652/4654/4656/4658/4659 + the small-genuine 67,387/43,520/2,232 run) cited in a comment.
- [x] Split the local oracles into a sourced `scripts/flow/roborev-review-oracles.sh` (push assert +
      census + code-free) resolved via `BASH_SOURCE`, and FAIL CLOSED when that file is missing OR
      truncated (a silently absent oracles file would turn both checks into no-ops).
- [x] Split JSON/token decoding into `scripts/flow/roborev-job-facts.py` (`git_ref`, `status`,
      `model`/`requested_model`, prompt, and the three-state token extraction).
- [x] Step 2 — push assert (AC3) with **`git ls-remote` as the authoritative oracle**: distinct
      fail-closed causes for detached HEAD, an `ls-remote` failure (infra/auth — NOT "never pushed"), a
      branch absent on the remote, and a remote tip behind/diverged from HEAD (naming the unpushed
      commits). Remote taken from the branch upstream, else `origin`.
- [x] Remove the local mirror-ref fast path entirely: a cached `refs/remotes/<remote>/<branch>` survives
      a force-push or a branch deletion, so it can equal HEAD while the remote lacks the commit.
- [x] Step 3 — local diff census oracle: `git diff --numstat --no-renames <base>...HEAD` → files/+/−
      plus the path list; a genuinely empty census exits `NOTHING-TO-REVIEW` without enqueuing a review;
      an unresolvable base and a FAILED `git diff` are DISTINCT fail-closed causes, never aliased to
      "genuinely empty".
- [x] Step 3b — `code-free:` as a DETERMINISTIC FAIL evaluated pre-enqueue from our own census
      classification (extension-based, with an extensionless-path assist), so `docs/foo.py` and
      `.github/**/*.yml` count as CODE and cannot produce a false code-free FAIL.
- [x] Step 4 — invocation (AC2): explicit HEAD sha + explicit absolute `--repo` + `--wait`; refuse when
      only one of `--agent`/`--model` is supplied; never bare `--branch`, never the two-positional range form.
- [x] Step 5 — reviewed-SHA assert (AC2) with the job record's full-40-char `git_ref` as the oracle and
      the stdout `Enqueued job <N> for <sha>` parse demoted to a cross-check (disagreement = NOTICE,
      absent/unparseable announcement = FAIL, ≥7-hex-char floor, case-normalised, last announcement
      wins with the multiplicity recorded); attribute a mismatch to the base ref or to "neither endpoint".
- [x] Step 5b — `model:` key surfacing `requested_model` != `model` as a loud NOTICE (not a FAIL: an
      alias resolution is legitimate and an always-red guard gets bypassed), and marking an absent model
      field UNCONFIRMED.
- [x] Step 6a — `review-completed:`: POSITIVE evidence required before PASS is reachable (job status
      `done` AND a terminal verdict marker from an allow-list); FAIL closed on an unreadable transcript,
      a non-`done` status, or no marker; NOTICE when the status is unavailable and completion rests on
      the marker alone.
- [x] Step 6b — `prompt-content:`: assert the census's own paths appear in the prompt ACTUALLY SENT
      (job-record `prompt`, else `roborev show <job> --prompt`), threshold-free, bounded by a named
      constant with even sampling; an unretrievable prompt is `UNAVAILABLE`, never a FAIL or a silent skip.
- [x] Step 6c — `findings:` key plus `roborev-exit:` splitting `FINDINGS (exit N)` from `ERROR (exit N)`
      with the structured `status` as authority, so a normal findings outcome is never misreported as a
      reviewer malfunction.
- [x] Step 6d — tier 1 (AUTHORITATIVE, gated): anchor the match to the verdict/`Summary:` region and gate
      it on `findings:` — `NONE`/`UNKNOWN` ⇒ HARD FAIL, `PRESENT*` ⇒ advisory NOTICE.
- [x] Step 6e — tier 2 (corroborating, fail-closed only): decode the doubly-encoded `token_usage` string
      and `total_output_tokens`; three-state extraction with `present-but-unparseable` ⇒ FAIL (drift);
      input floor 25,000 + `cached == 0` as the FAIL conditions with observed-vs-threshold printed; the
      output floor ADVISORY only; a degraded-signal notice (never a silent skip) when accounting is absent.
- [x] Step 7 — emit the `==== ROBOREV REVIEW SUMMARY ====` block in its contracted key order (repo,
      branch, base, head-sha, reviewed-sha, job, model, census, tokens, push-assert, census-check,
      code-free, sha-assert, review-completed, prompt-content, vacuity-tier1, vacuity-tier2, findings,
      roborev-exit, log, terminal `RESULT:`), write the raw transcript to the log path named in the block,
      and exit non-zero on any non-PASS outcome.
- [x] Step 7b — one verdict scan over every per-check key: `FAIL*`/`FINDINGS*`/`ERROR*` fail the run;
      `PASS`/`NOTICE`/`UNAVAILABLE`/`SKIP` never do. Unreached checks read `SKIP`, never blank.
- [x] Emit the block on EVERY verdict path including an unexpected mid-run abort (`trap … EXIT`), while
      the usage-error and `--help` paths emit NO block at all.
- [x] Hygiene pass: keep each file small (campsite spirit), quote every interpolation of external
      output, `shellcheck -x` clean at info level across all three shell files, and walk CLAUDE.md's
      pre-roborev self-check list over the diff.

## 2. Hermetic regression check (surface: `scripts/tests/test_roborev_review_guard.sh`)
- [x] Build the stub `roborev` fixture (first on `PATH`) replaying the recorded enqueue lines, verdict
      text, and `show --json` payloads — including `token_usage` as a JSON-ENCODED STRING carrying
      `total_output_tokens`, the exact shape whose mis-read left tier 2 permanently UNAVAILABLE — plus
      throwaway `git init` fixtures with their own local bare `origin`.
- [x] Fixture topologies: wide + **narrow** fetch refspec (the fleet's real configuration), behind,
      upstream-named remote, unreachable remote, missing base, deleted remote branch with a stale mirror
      ref, docs-only, mixed, workflow-yaml; plus a fixture-integrity guard so a narrow fixture that grew
      a mirror ref cannot silently stop testing its condition.
- [x] Case (a): reviewed sha == base ref → FAIL, message names the base ref.
- [x] Case (b): reviewed sha is neither endpoint → FAIL.
- [x] Case (c): a cleanliness vacuity claim against a non-empty CODE census → FAIL; plus the gated and
      anchored complements (findings UNKNOWN → FAIL; a findings-bearing review quoting the phrase →
      NOTICE/PASS; the phrase outside the summary region → PASS).
- [x] Case (d): vacuous token signature → FAIL, and the input floor pinned at exactly 25000.
- [x] Case (e): unpushed branch → FAIL, no review enqueued; remote behind HEAD names the unpushed commits.
- [x] Case (f): genuine review (matching sha, healthy accounting) → PASS, with the sanctioned argv
      asserted (explicit sha, absolute `--repo`, both flags, no `--branch`, no two positionals).
- [x] Case (g): genuinely empty census → `NOTHING-TO-REVIEW` with its own non-zero exit code, never PASS;
      an unresolvable base and a failed `git diff` → FAIL, never `NOTHING-TO-REVIEW`.
- [x] Code-free cases: a docs-only census FAILs deterministically (even with a clean verdict and healthy
      tokens), while a mixed census and a `.github/workflows/*.yml` census are NOT code-free.
- [x] Push-assert cases: narrow refspec pushed → PASS; narrow behind → FAIL; upstream-named remote
      honoured; `ls-remote` failure → infra/auth FAIL; deleted remote branch with a stale mirror ref → FAIL.
- [x] `review-completed` cases: a job that never finished, the #2433/#3037 model-mismatch 400, and a
      `failed` job status each must NOT reach PASS.
- [x] `prompt-content` cases: a prompt without the census paths → FAIL; an unretrievable prompt →
      UNAVAILABLE (visible, not a skip); a PASS reports the coverage it checked.
- [x] `sha-assert` cases: the structured `git_ref` wins over a disagreeing stdout announcement; a 9-char
      real-shape announcement still verifies; an uppercase announcement is normalised; a 4-hex-char
      announcement is too short; two announcements → the last is effective.
- [x] `findings`/`roborev-exit` cases: non-zero exit with a completed review → FINDINGS; with a failed
      job → ERROR; zero exit → PASS; a pre-invocation failure → SKIP; key ORDER pinned.
- [x] Token cases: the real payload shape evaluates; the legacy `output_tokens` alias still resolves;
      renamed fields resolve via the alias sets; present-but-unparseable → FAIL (drift);
      `has_token_data=false` beside real counts → drift NOTICE, not a bypass; a low output count never
      fails a genuine clean review; absent accounting degrades visibly and does not fail an otherwise
      clean review.
- [x] Robustness cases: detached HEAD; a repository with no commits; `roborev` absent from PATH; an abort
      before a verdict still emits a block; option-value/option-name validation; a MISSING and a
      TRUNCATED oracles file both FAIL closed.
- [x] Assert the summary block's header is distinct from all three agent-gate summary headers, and that
      `--help` documents the exit codes + the live worktree probe and emits no block.
- [x] Confirm hermeticity: no network, no real roborev, no dataset corpus, no cargo; a loud SKIP (never a
      silent pass) when python3 is unavailable; and no wall-clock threshold assert in the correctness
      path (#2642).
- [x] Verify the suite's own strength: 258 assertions green, and 27/27 deliberate wrapper mutations killed.

## 3. Gate wiring (surface: `scripts/agent-gate.sh`)
- [x] Register the check in `run_roborev_lints_cmd()` (component `roborev-lints`, present in BOTH
      `LITE_COMPONENTS` and `COMPONENTS`) so a regression FAILs the fast `--lite` loop — the stated
      acceptance goal — following the existing `check-workflow-injection.sh` / `check-no-wallclock-asserts.sh`
      chaining pattern.
- [x] Append the check to `run_tooling_tests()` (full-gate `tooling-tests`) following the
      `test_check_dockerfile_rust_pin.sh` / `test_check_skill_flag_tables.sh` guard pattern (FAIL the
      component on non-zero, with a named failure line).
- [x] Verify runtime is fast enough for `--lite` (~0.5s) and that `scripts/agent-gate.sh --list` /
      `--lite-list` still reflect the intended component set.

## 4. Call-site migration — 16 surfaces (`.claude/**` ×13 + fleet doctrine ×3)
- [x] `.claude/skills/flow-implement/SKILL.md` — the review-first step (the primary call site; replaces
      the documented bare `--branch` command).
- [x] `.claude/agents/flow-closer.md` — the final confirmation pass (the merge-gating call site; also
      removes the non-existent `/roborev-review-branch` form) and treat a non-PASS terminal `RESULT`,
      including `NOTHING-TO-REVIEW`, as a blocked merge.
- [x] `.claude/skills/flow-address/SKILL.md` — the post-comment re-review (push first, then re-run).
- [x] `.claude/commands/worker.md` — the fleet's UNATTENDED entry point, which runs the implement loop's
      review-first step itself (missed by the first migration sweep).
- [x] `.claude/agents/flow-lead.md` — the stage table's roborev row + the roborev doctrine bullet.
- [x] `.claude/skills/{flow-activate,flow-finalize,ci-cd-validation}/SKILL.md` +
      `.claude/skills/ci-cd-validation/merge-process.md` — every roborev reference routed through the
      wrapper; merge-readiness requires a terminal `RESULT: PASS`.
- [x] `.claude/commands/manager.md` — define "roborev clean" for dispatched workers as the wrapper's
      terminal `RESULT: PASS` (missed by the first migration sweep).
- [x] `.claude/agents/{rust-reviewer,sstable-developer,test-validator}.md` — each states it never invokes
      roborev directly and points at the wrapper; `rust-reviewer` flags a reintroduced bare `--branch` or
      two-positional range form as a **BLOCKER**.
- [x] `website/src/content/docs/agents-developing/delivery-pipeline.md`,
      `docs/development/pm-operating-loop.md`, `docs/development/agent-machine-setup.md` — replace the
      INVERSE instruction ("this machine's configured agent … no `--agent`/`--model` flags"; explicit
      agent/model called "never doctrine") with the wrapper, both flags, push-first, and the non-PASS rule.
- [x] Sweep for any remaining bare `--branch` roborev instruction across `.claude/**`, `docs/**` and
      `website/**` and mark the form non-sanctioned; preserve the both-`--agent`-and-`--model` requirement
      at every call site.

## 5. Doctrine — ships in this change (AC4)
- [x] CLAUDE.md — update the roborev-invocation bullet in *Agent-Team Conventions*: the wrapper is the
      only sanctioned invocation; verify the reviewed SHA; "contains no code changes to review" on a
      non-empty diff is a HARD FAIL; docs-only diffs cannot be roborev-certified; plus the exit-code
      contract and "any non-PASS `RESULT` is a blocked merge".
- [x] `website/src/content/docs/agents-developing/roborev-findings.md` — the same four rules, the
      evidence (T1/T2/T3 + token tells), the live-probe procedure, and a row in the "mechanized in
      `--lite`" table for the new guard.
- [ ] Verify publication by grepping the SERVED page for a distinctive new phrase (an HTTP 200 is not
      proof; CDN staleness ≈3 min) and re-check after a wait if absent. **Open: post-merge, after the
      site deploys.**

## 6. Live worktree probe (AC5)
- [x] Document the probe (wrapper `--help` text + the doctrine page): from a real `issue-<N>-*` worktree
      with a pushed commit and the root checkout on `main`, run the wrapper and confirm the summary
      block's `reviewed-sha` equals the worktree HEAD and does NOT equal the base ref; re-run after any
      roborev version bump.
- [ ] Run the probe once against the real binary and record the observed summary-block values (head-sha,
      reviewed-sha, job, census, tokens) in the PR body as the AC5 live evidence. **Open: needs a live
      reviewer; the branch's own review rounds are the natural vehicle.**

## 7. Gate + review + sign-off
- [x] `scripts/agent-gate.sh --lite` green each fix round (summary-file redirect; anchor the poll on
      `RESULT: (PASS|FAIL)`).
- [x] Reconcile the OpenSpec change with the as-built deterministic-primary architecture (this pass), so
      the intent audit anchors on what was actually built: every new key specified, the two evidenced
      relaxations (input floor 25,000; the dropped output floor) recorded as decisions, and the migrated
      surface set enumerated count-accurately (16, not the stale 10).
- [ ] `rust-reviewer` (no Rust here — skip if it has nothing to review) + roborev on the lite-green diff
      via the NEW wrapper (review-first). Note: this change's own diff mixes shell/python with docs, so
      it is NOT code-free; four roborev rounds have run against it and their blockers are folded in.
- [ ] Full gate ONCE via `flow-closer`; verify `tree-integrity:` alongside `RESULT:`.
- [ ] **C (spec-auditor)** anchored to `openspec/changes/roborev-vacuous-review-guard/specs/**`: every
      requirement `satisfied` with public-surface (wrapper + regression-check + doctrine) evidence.
- [ ] roborev clean; blockers fixed pre-merge, nits batched to ONE linked follow-up issue.
- [ ] File the upstream follow-up noted in `design.md` (worktree-aware `--branch` resolution; non-zero
      exit on a discarded code-free diff) so the external-binary gap is tracked.
- [x] `openspec validate roborev-vacuous-review-guard --strict` clean.
- [ ] `openspec archive` after merge.
