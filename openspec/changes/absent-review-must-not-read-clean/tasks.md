# Tasks: absent-review-must-not-read-clean (issue #3751)

## 1. Mechanism
- [ ] 1.1 `scripts/flow/review-stage.sh` — `open` (sentinel pre-stamp, `git check-ignore` fail-closed path
      verification, re-open refusal, prints the paste-ready spawn clause)
- [ ] 1.2 `status` — elapsed / deadline / sentinel-only, advisory only
- [ ] 1.3 `verdict` — closed grammar, string-equality token match, five named `NOT-RUN` causes, exit codes
- [ ] 1.4 `record-author-performed` — required working, placeholder refusal, `AUTHOR-PERFORMED` token
- [ ] 1.5 `.gitignore` entry for `.review-stage/`

## 2. Consumer (merge point)
- [ ] 2.1 `premerge-assert.sh --c-verdict <path|AUTO>`; omission ⇒ exit 3 usage
- [ ] 2.2 `AUTO` measures routing from `openspec/changes/<slug>/` presence on the branch
- [ ] 2.3 refuse on absent/`NOT-RUN` C for a design-routed branch; report `NOT-APPLICABLE` affirmatively
- [ ] 2.4 report `AUTHOR-PERFORMED` under its own token on the `PREMERGE:` line, never folded into `OK`

## 3. Agent + skill side
- [ ] 3.1 report-of-record clause in each pipeline-gating agent definition (scope pending Seam 1)
- [ ] 3.2 `flow-implement` / `flow-closer` skills: `open` before spawn, `verdict` after
- [ ] 3.3 `flow-closer`'s `NEEDS-SPAWN {role: spec-auditor}` handshake carries the report path

## 4. Tests
- [ ] 4.1 `scripts/tests/test_review_stage.sh` — AC1 case, positive control, per-cause naming,
      author-performed accept/refuse matrix, case floor
- [ ] 4.2 enrol in the `tooling-tests` roster (no new gate component)
- [ ] 4.3 premerge-assert cases: absent C refuses, `NOT-APPLICABLE` measured, missing flag exit 3

## 5. Doctrine + root cause
- [ ] 5.1 `docs/development/review-stage-reporting.md` — AC5 record with the source census and the limits
- [ ] 5.2 CLAUDE.md: the report-of-record contract in the implement-loop + flow-closer sections
- [ ] 5.3 website `agents-developing/` page update + publish verified by NEW CONTENT served, not HTTP 200
