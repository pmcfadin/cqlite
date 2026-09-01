# Tasks: absent-review-must-not-read-clean (issue #3751)

## 1. Mechanism
- [x] 1.1 `scripts/flow/review-stage.sh` — `open` (sentinel pre-stamp, `git check-ignore` fail-closed path
      verification, re-open refusal, prints the paste-ready spawn clause)
- [x] 1.2 `status` — elapsed / deadline / sentinel-only, advisory only
- [x] 1.3 `verdict` — closed grammar, string-equality token match, five named `NOT-RUN` causes, exit codes
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
- [x] 3.3 `flow-closer`'s `NEEDS-SPAWN {role: spec-auditor}` handshake carries the report path

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
- [ ] 6.1 Coordinate the `premerge-assert.sh` arity change with #3752's `--roborev-block`-shaped flag so
      in-flight lanes pay ONE re-certification visit, not two (lead ruling 2026-09-01T18:19:24Z).
      Landing ORDER is the lead's call; this change assumes neither.
- [ ] 6.2 Owner rollout condition (Seam-1 approval 2026-09-01T18:36:42Z): whichever of #3751/#3752
      merges SECOND must not force a second sweep of open PRs, and the merge-time notice posted to
      open PRs must name the remedy commands for BOTH new asserts, so an in-flight lane pays ONE
      re-certification visit covering both.
