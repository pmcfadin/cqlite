# Tasks: absent-review-must-not-read-clean (issue #3751)

## 1. Mechanism
- [x] 1.1 `scripts/flow/review-stage.sh` — `open` (sentinel pre-stamp, `git check-ignore` fail-closed path
      verification, re-open refusal, prints the paste-ready spawn clause)
- [x] 1.2 `status` — elapsed / deadline / sentinel-only, advisory only
- [x] 1.3 `verdict` — closed grammar, string-equality token match, six named `NOT-RUN` causes, exit codes
- [x] 1.4 `record-author-performed` — required working, placeholder refusal, `AUTHOR-PERFORMED` token
- [x] 1.5 `.gitignore` entry for `.review-stage/`

## 2. Consumer (merge point)
- [x] 2.1 `premerge-assert.sh --c-verdict <path|AUTO>`; omission ⇒ exit 3 usage
- [x] 2.2 `AUTO` measures routing from `openspec/changes/<slug>/` presence on the branch
- [x] 2.3 refuse on absent/`NOT-RUN` C for a design-routed branch; report `NOT-APPLICABLE` affirmatively
- [x] 2.4 report `AUTHOR-PERFORMED` under its own token on the `PREMERGE:` line, never folded into `OK`

## 3. Agent + skill side
- [x] 3.1 report-of-record clause in ALL SIX pipeline-gating agent definitions (Q1=(a) as ruled;
      spec-auditor, rust-reviewer, coverage-reviewer, compaction-parity-auditor, flow-closer,
      sstable-developer) — owner ratifies the widening at Seam 1
- [x] 3.2 `flow-implement` / `flow-closer` skills: `open` before spawn, `verdict` after
- [x] 3.3 `flow-closer`'s `NEEDS-SPAWN` handshake carries the report path — BOTH packet templates
      (`role: spec-auditor` and `role: sstable-developer`), since `report:` is declared REQUIRED

## 4. Tests
- [x] 4.1 `scripts/tests/test_review_stage.sh` — AC1 case, positive control, per-cause naming,
      author-performed accept/refuse matrix, case floor
- [x] 4.2 enrol in the `tooling-tests` roster (no new gate component)
- [x] 4.3 premerge-assert cases: absent C refuses, `NOT-APPLICABLE` measured, missing flag exit 3

## 5. Doctrine + root cause
- [x] 5.1 `docs/development/review-stage-reporting.md` — AC5 record with the source census and the limits
- [x] 5.2 CLAUDE.md: the report-of-record contract in the implement-loop + flow-closer sections
- [x] 5.3 website `agents-developing/` page update — `delivery-pipeline.md` (the report-of-record bullet in
      the implement loop + the `--c-verdict` paragraph in the closer merge protocol) and
      `gate-contract.md` (the invocation signature + the C-verdict half). **The publish-verification
      half is POST-MERGE by construction**: the site deploys from `main`, so a `curl … | grep '<new
      phrase>'` before the merge can only ever return 0. Run it after the merge and expect a ~3-minute
      CDN stale window; a `0` then means not-yet-published, not published-and-wrong.

## 6. Landing coordination
- [x] 6.1 Coordinate the `premerge-assert.sh` arity change with #3752's assert so in-flight lanes pay ONE
      re-certification visit, not two (lead ruling 2026-09-01T18:19:24Z). **MEASURED against PR #3842
      (#3752) 2026-09-01T21:1xZ, not assumed: #3752 adds NO caller-facing flag** — it calls a new
      `scripts/flow/premerge-review-binding.sh` from INSIDE `premerge-assert.sh` and derives the
      roborev/head binding itself. So the arity change is `--c-verdict` ALONE, and the two asserts compose
      in EITHER landing order with no second arity visit: this change assumes neither order, and its
      missing-flag census names each absent flag independently (item 2.1) so a later required flag does not
      make its exit 3 order-dependent. Residual, disclosed rather than resolved here: both branches touch
      `premerge-assert.sh`, `CLAUDE.md`, `website/.../delivery-pipeline.md`, `.claude/agents/flow-closer.md`,
      `.claude/skills/flow-implement/SKILL.md` and `scripts/tests/test_premerge_assert.sh`, so whichever
      merges SECOND rebases and resolves text conflicts — a merge-order cost, not an interface cost.
- [~] 6.2 Owner rollout condition (Seam-1 approval 2026-09-01T18:36:42Z): whichever of #3751/#3752
      merges SECOND must not force a second sweep of open PRs, and the merge-time notice posted to
      open PRs must name the remedy commands for BOTH new asserts, so an in-flight lane pays ONE
      re-certification visit covering both.
      **Prepared, posted at merge time (the only moment it can be true):** ONE notice per open PR naming
      BOTH remedies — (a) #3751: add `--c-verdict AUTO` to the `premerge-assert.sh` call (or
      `--c-verdict <path>` naming a captured `review-stage.sh verdict` line); (b) #3752: ensure the PR
      records a roborev round covering the certified head, which needs NO argument change because that
      assert derives the binding internally. Since #3752 adds no arity, an in-flight lane's ONE visit is
      the `--c-verdict` addition regardless of which lands first.
