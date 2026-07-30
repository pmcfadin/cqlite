# Tasks: roborev-vacuous-review-guard (issue #2964)

> Design decided in `design.md`: a fail-closed CQLite-side wrapper (`roborev` is an external binary we
> cannot patch) whose **DETERMINISTIC** checks carry the verdict — the remote (`git ls-remote`), a local
> `git` diff census, that census's code-free classification, the job record's structured
> `git_ref`/`status`, and the census's own paths inside the prompt actually sent — with reviewer prose
> and token accounting CORROBORATING and fail-closed-only. AC→requirement map is at the top of
> `specs/roborev-review-guard/spec.md`.

## 1. The sanctioned wrapper (surfaces: `scripts/flow/roborev-review.sh`, `roborev-review-oracles.sh`, `roborev-review-checks.sh`, `roborev-job-facts.py`)
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
- [x] Split the five per-review checks into a sourced `scripts/flow/roborev-review-checks.sh`
      (review-completed, prompt-content, findings/roborev-exit, tier 1, tier 2) when the wrapper reached
      998 lines, resolved via `BASH_SOURCE`, FAIL CLOSED on a missing OR truncated file (each required
      function checked), and VALIDATED BEFORE the invocation so a broken install costs no review.
- [x] Split JSON/token decoding into `scripts/flow/roborev-job-facts.py` (`git_ref`, `status`,
      `model`/`requested_model`, `verdict`, prompt, and the three-state token extraction).
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
- [x] Measure the invocation matrix against the real daemon (17-commit branch, 27-file census) and pick the
      form empirically: `--branch --base <base> --repo <abs>` and `--since <base> --repo <abs>` both enqueue
      `<base40>..<head40>` with 5/5 code files in the prompt; two positionals anchor at git's EMPTY TREE
      (3/5); a single sha covers ONE COMMIT (3/5). Record the AC2 divergence (we implement its INTENT).
- [x] Step 4 — invocation (AC2's intent): `--branch --base <base> --repo <abs-repo> --wait` over the census
      RANGE; refuse when only one of `--agent`/`--model` is supplied; never `--branch` WITHOUT `--repo`,
      never the two-positional range form, never a single sha.
- [x] Step 5 — reviewed-RANGE assert with the job record's `git_ref` as the ONLY oracle, asserting BOTH
      endpoints against the census range; FAIL a single-commit record even when it equals HEAD; FAIL
      (never fall back to prose) when the record is unavailable, since a range review announces only the
      range BASE. The stdout `Enqueued job <N> for <sha>` parse is demoted to the JOB-ID carrier
      (absent/unparseable announcement = FAIL, ≥7-hex-char floor, case-normalised, last announcement
      wins with the multiplicity recorded); attribute a mismatch to the base ref, the empty-tree base, or a
      head short of the tip.
- [x] Step 5a — `job-record:` key (`PASS` / `PASS (no token accounting in the record)` /
      `DEGRADED (incomplete after <n>s: <fields>)` / `SKIP`), consulting BOTH payload shapes and keeping
      only a source that yields the required fields: `show --json` returns the REVIEW row and NESTS the job
      row under a `job` key, so prefer an id match that actually carries `git_ref`. Retract the earlier
      async-write misdiagnosis: the record is complete in ONE read, so the poll is a 5×1s sanity retry.
- [x] Step 5b — `model:` key surfacing `requested_model` != `model` as a loud NOTICE (not a FAIL: an
      alias resolution is legitimate and an always-red guard gets bypassed), and marking an absent model
      field UNCONFIRMED.
- [x] Step 6a — `review-completed:`: POSITIVE evidence required before PASS is reachable (job status
      `done` AND a terminal verdict marker from an allow-list); FAIL closed on an unreadable transcript,
      a non-`done` status, or no marker; NOTICE when the status is unavailable and completion rests on
      the marker alone.
- [x] Step 6b — `prompt-content:`: assert the CODE SUBSET of the census appears in the prompt ACTUALLY SENT
      (job-record `prompt`, else `roborev show <job> --prompt`), threshold-free, with EVERY code path checked
      (the sampling cap removed — a partial prompt naming the sampled files passed) against exact
      `diff --git` headers, collecting BOTH header sides so a detected rename is not a false rejection; an
      unretrievable prompt is `FAIL (prompt unretrievable — no evidence any diff was delivered)`, with NO
      non-failing `UNAVAILABLE` value for this key.
- [x] Step 6c — `findings:` key plus `roborev-exit:` splitting `FINDINGS (exit N)` from `ERROR (exit N)`
      with the structured `status` as authority, so a normal findings outcome is never misreported as a
      reviewer malfunction; derive PRESENT/NONE from the structured `verdict` field, scope prose to the
      findings BLOCK (line-initial `Summary` terminator), keep the count best-effort, and FAIL a
      contradiction as `INCONSISTENT (verdict clean|exit 0, <n> findings marker(s))`.
- [x] Step 6d — tier 1 (AUTHORITATIVE, gated): anchor the match to the whole SUMMARY BLOCK (heading or
      label → next heading/EOF — a region of only the lines CONTAINING `Summary:` missed the `## Summary`
      heading form and passed a vacuous review) and gate it on `findings:` — `NONE`/`UNKNOWN` ⇒ HARD FAIL,
      `PRESENT*` ⇒ advisory NOTICE, `INCONSISTENT` ⇒ no exemption.
- [x] Step 6e — tier 2 (corroborating, fail-closed only): decode the doubly-encoded `token_usage` string
      and `total_output_tokens`; three-state extraction with `present-but-unparseable` ⇒ FAIL (drift);
      input floor 25,000 + `cached == 0` as the FAIL conditions with observed-vs-threshold printed; the
      output floor ADVISORY only; a degraded-signal notice (never a silent skip) when accounting is absent.
- [x] Step 7 — emit the `==== ROBOREV REVIEW SUMMARY ====` block in its contracted key order (repo,
      branch, base, head-sha, reviewed-sha, job, model, census, tokens, push-assert, census-check,
      code-free, job-record, sha-assert, review-completed, prompt-content, vacuity-tier1, vacuity-tier2,
      findings, roborev-exit, log, terminal `RESULT:`), with `reviewed-sha:` carrying the
      `<base40>..<head40>` RANGE; write the raw transcript to the log path named in the block,
      and exit non-zero on any non-PASS outcome.
- [x] Step 7b — one verdict scan over every per-check key: `FAIL*`/`FINDINGS*`/`ERROR*`/`INCONSISTENT*`
      fail the run; `PASS*`/`NOTICE*`/`DEGRADED*`/`UNAVAILABLE`/`SKIP` never do. Unreached checks read
      `SKIP`, never blank.
- [x] Emit the block on EVERY verdict path including an unexpected mid-run abort (`trap … EXIT`), while
      the usage-error and `--help` paths emit NO block at all.
- [x] Hygiene pass: keep each file small (campsite spirit), quote every interpolation of external
      output, `shellcheck -x` clean at info level across all three shell files, and walk CLAUDE.md's
      pre-roborev self-check list over the diff.
- [x] Round-6 blocker fixes (each now a spec obligation, so a regression is a spec violation): the tier-1
      region is the whole summary BLOCK; an unretrievable prompt FAILs; rename headers are matched on BOTH
      sides. Plus the nested-job-row read and the checks-file split.

## 2. Hermetic regression check (surface: `scripts/tests/test_roborev_review_guard.sh`)
- [x] Build the stub `roborev` fixture (first on `PATH`) replaying the recorded enqueue lines, verdict
      text, and `show --json` payloads — including `token_usage` as a JSON-ENCODED STRING carrying
      `total_output_tokens`, the exact shape whose mis-read left tier 2 permanently UNAVAILABLE — plus
      throwaway `git init` fixtures with their own local bare `origin`.
- [x] Fixture topologies: wide + **narrow** fetch refspec (the fleet's real configuration), behind,
      upstream-named remote, unreachable remote, missing base, deleted remote branch with a stale mirror
      ref, docs-only, mixed, workflow-yaml, renamed; plus a fixture-integrity guard so a narrow fixture
      that grew a mirror ref cannot silently stop testing its condition.
- [x] Case (a): reviewed sha == base ref → FAIL, message names the base ref.
- [x] Case (b): the reviewed RANGE does not match the census range at either endpoint → FAIL; a
      SINGLE-COMMIT record equal to branch HEAD → FAIL (single-commit record, not the census range).
- [x] Case (c): a cleanliness vacuity claim against a non-empty CODE census → FAIL, INCLUDING one whose
      sentence sits under a `## Summary` heading; plus the gated and
      anchored complements (findings UNKNOWN → FAIL; a findings-bearing review quoting the phrase →
      NOTICE/PASS; the phrase outside the summary region → PASS).
- [x] Case (d): vacuous token signature → FAIL, and the input floor pinned at exactly 25000.
- [x] Case (e): unpushed branch → FAIL, no review enqueued; remote behind HEAD names the unpushed commits.
- [x] Case (f): genuine review (matching RANGE, healthy accounting) → PASS, with the sanctioned argv
      asserted (`--branch` PAIRED with an absolute `--repo`, both flags, no two positionals, no single sha).
- [x] Case (g): genuinely empty census → `NOTHING-TO-REVIEW` with its own non-zero exit code, never PASS;
      an unresolvable base and a failed `git diff` → FAIL, never `NOTHING-TO-REVIEW`.
- [x] Code-free cases: a docs-only census FAILs deterministically (even with a clean verdict and healthy
      tokens), while a mixed census and a `.github/workflows/*.yml` census are NOT code-free.
- [x] Push-assert cases: narrow refspec pushed → PASS; narrow behind → FAIL; upstream-named remote
      honoured; `ls-remote` failure → infra/auth FAIL; deleted remote branch with a stale mirror ref → FAIL.
- [x] `review-completed` cases: a job that never finished, the #2433/#3037 model-mismatch 400, and a
      `failed` job status each must NOT reach PASS.
- [x] `prompt-content` cases: a prompt without the census code paths → FAIL; an unretrievable prompt →
      FAIL (prompt unretrievable); a PASS reports the coverage it checked; a two-sided rename header covers
      both `--no-renames` census paths (`PASS (2/2 …)`).
- [x] `sha-assert` cases: the structured `git_ref` is the only scope oracle (the stdout announcement carries
      the job id only); the real abbreviated range-base announcement parses; an uppercase announcement is
      normalised; a 4-hex-char announcement is too short; two announcements → the last is effective.
- [x] `job-record` cases: a complete record → `PASS`; the NESTED job row of a `show --json` payload is read
      as a first-class source → `PASS`; an unreadable/mismatched record → `DEGRADED` plus
      `sha-assert: FAIL (job record unavailable — reviewed range unverifiable)`.
- [x] `findings`/`roborev-exit` cases: non-zero exit with a completed review → FINDINGS; with a failed
      job → ERROR; zero exit → PASS; a pre-invocation failure → SKIP; a clean verdict beside in-block
      markers → `INCONSISTENT (verdict clean, …)`; exit 0 beside in-block markers → `INCONSISTENT (exit 0,
      …)`; key ORDER pinned (including `job-record:`).
- [x] Token cases: the real payload shape evaluates; the legacy `output_tokens` alias still resolves;
      renamed fields resolve via the alias sets; present-but-unparseable → FAIL (drift);
      `has_token_data=false` beside real counts → drift NOTICE, not a bypass; a low output count never
      fails a genuine clean review; absent accounting degrades visibly and does not fail an otherwise
      clean review.
- [x] Robustness cases: detached HEAD; a repository with no commits; `roborev` absent from PATH; an abort
      before a verdict still emits a block; option-value/option-name validation; a MISSING and a
      TRUNCATED oracles file AND a missing/truncated CHECKS file all FAIL closed with no review enqueued.
- [x] Assert the summary block's header is distinct from all three agent-gate summary headers, and that
      `--help` documents the exit codes + the live worktree probe and emits no block.
- [x] Confirm hermeticity: no network, no real roborev, no dataset corpus, no cargo; a loud SKIP (never a
      silent pass) when python3 is unavailable; and no wall-clock threshold assert in the correctness
      path (#2642).
- [x] Verify the suite's own strength: 329 assertions green; deliberate wrapper mutations killed 27/27
      (round-5 batch) and 15/15 (round-6 batch), the single round-6 survivor — the nested-job-row read — now
      pinned by its own case.

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
      only sanctioned invocation; verify the reviewed SCOPE against the census range; "contains no code
      changes to review" on a non-empty diff is a HARD FAIL; docs-only diffs cannot be roborev-certified;
      plus the exit-code contract and "any non-PASS `RESULT` is a blocked merge".
- [x] `website/src/content/docs/agents-developing/roborev-findings.md` — the same four rules, the
      evidence (T1/T2/T3/T4 + token tells), the live-probe procedure, and a row in the "mechanized in
      `--lite`" table for the new guard.
- [x] Propagate the THREE measured corrections to every surface that states the rule (CLAUDE.md,
      `roborev-findings.md`, `delivery-pipeline.md`, `pm-operating-loop.md`, `agent-machine-setup.md`):
      the non-sanctioned form is `--branch` WITHOUT an explicit `--repo` (the old absolute ban forbade the
      form we now use); the single-SHA form reviews ONE COMMIT (a fourth vacuity class, and AC2's letter);
      roborev EXCLUDES non-code paths from the diff it builds. Add the `job-record:` key and the corrected
      `prompt-content:` values wherever the block is documented, and restate the live probe in the RANGE
      form.
- [ ] Verify publication by grepping the SERVED page for a distinctive new phrase (an HTTP 200 is not
      proof; CDN staleness ≈3 min) and re-check after a wait if absent. **Open: post-merge, after the
      site deploys.** Checked by the closer at merge+2min — the `Docs Site` workflow was still
      `in_progress` on `main` and both greps returned `0` (NOT yet published, which is expected, not a
      failure). Re-check once the deploy completes:
      `curl -sS https://pmcfadin.github.io/cqlite/agents-developing/roborev-findings/ | grep -c 'single-SHA review covers ONE COMMIT'`
      and `.../delivery-pipeline/ | grep -c 'one commit, not the branch'` — each must return non-zero.

## 6. Live worktree probe (AC5)
- [x] Document the probe (wrapper `--help` text + the doctrine page): from a real `issue-<N>-*` worktree
      with a pushed commit and the root checkout on `main`, run the wrapper and confirm the summary
      block's `reviewed-sha` RANGE has its HEAD endpoint at the worktree HEAD and is not the base ref
      alone; re-run after any roborev version bump. **Residual: the `--help` text still phrases step 3 as
      `reviewed-sha == head-sha (prefix match)`, wording that predates the range form (the doctrine page
      carries the corrected range phrasing) — a docs-only staleness in a frozen script, named in
      `design.md`.**
- [x] Run the probe against the real binary: round 5 executed it from this issue's own worktree and
      produced the invocation matrix (17-commit branch, 27-file census, enqueued `git_ref` + prompt code-file
      coverage per form), recorded in `design.md`/`proposal.md` and the spec delta. The closer mirrors the
      observed block values into the PR body as the AC5 evidence.

## 7. Gate + review + sign-off
- [x] `scripts/agent-gate.sh --lite` green each fix round (summary-file redirect; anchor the poll on
      `RESULT: (PASS|FAIL)`).
- [x] Reconcile the OpenSpec change with the as-built deterministic-primary architecture (final pass), so
      the intent audit anchors on what was actually built: the RANGE invocation and the recorded AC2
      divergence, every new key specified (`job-record:`, the range `sha-assert:` values, the
      `prompt-content:` FAIL values, the `INCONSISTENT` findings states), the five-file layout, the three
      round-6 blocker fixes as spec obligations, the evidenced relaxations (input floor 25,000; the dropped
      output floor; tier 1 gated + `UNAVAILABLE` on an absent region) recorded as decisions, and the
      migrated surface set enumerated count-accurately (16).
- [x] `rust-reviewer` (no Rust here — skip if it has nothing to review) + roborev on the lite-green diff
      via the NEW wrapper (review-first). Note: this change's own diff mixes shell/python with docs, so
      it is NOT code-free; six roborev rounds have run against it and their blockers are folded in
      (round-6 blockers in `21bba65`/`83129d5`). Closed: the post-round-6 re-review ran as the closer's
      final roborev pass below (job 10, dogfooded through the new wrapper).
- [x] Full gate ONCE via `flow-closer`; verify `tree-integrity:` alongside `RESULT:`.
      Gate of record `run-id: /tmp/agent-gate.xmN2Ij` at `39ee5b3`: `RESULT: PASS`,
      `tree-integrity: PASS` (`tree-start` == `tree-end`), 29/29 components, `datasets: 144 Data.db`.
- [x] **C (spec-auditor)** anchored to `openspec/changes/roborev-vacuous-review-guard/specs/**`: every
      requirement `satisfied` with public-surface (wrapper + regression-check + doctrine) evidence.
      Verdict PASS — 19/19 met (16 `satisfied`, 3 justified `partial`), no blockers, no permissive counts.
- [x] roborev clean; blockers fixed pre-merge, nits batched to ONE linked follow-up issue.
      Final pass: all guard keys PASS, `prompt-content: PASS (6/6)`, full range `f603c7f..39ee5b3`;
      4 findings, **0 blockers**, 4 nits batched to #3133.
- [x] File the upstream follow-up noted in `design.md` (worktree-aware `--branch` resolution; non-zero
      exit on a discarded code-free diff) so the external-binary gap is tracked. Filed as #3126 +
      kenn-io/roborev#1011.
- [x] `openspec validate roborev-vacuous-review-guard --strict` clean.
- [x] `openspec archive` after merge (archived as `2026-07-30-roborev-vacuous-review-guard`; the 19
      requirements promoted to `openspec/specs/roborev-review-guard/spec.md`).
