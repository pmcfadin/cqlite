# Tasks — enforced board→label mirror

## 1. Mirror logic in project-board-sync.yml
- [ ] 1.1 In the `sweep` job's reconcile loop (which already reads each item's `fieldValueByName
      (name:"Status")`), add: compute desired label from Status (Ready→`status:ready`, In Progress→
      `status:in-progress`, In Review→`status:in-review`, Backlog/Done→none), then idempotent
      `gh issue edit <n> --add-label <desired>` + `--remove-label` the other board-derived status
      labels. Behind the existing "Guard token" step. (surface: sweep job)
- [ ] 1.2 Add `issues: [edited, labeled, unlabeled, reopened]` to the `on:` triggers and a job that
      re-asserts the single edited issue's label from its board Status (low-latency correction).
      Reuse the same compute-desired helper as 1.1 (single source).
- [ ] 1.3 Keep the existing duties intact: null-status→Backlog grace sweep, closed-PR→Done safety
      net, claim reaping. The mirror is additive.

## 2. Drift detector
- [ ] 2.1 Final step of the sweep job (after the mirror pass): re-read (Status, labels) for every
      OPEN issue; `::error::` naming each violator and exit non-zero on any label≠Status mismatch
      (respecting the auto-add grace window to avoid flap). Exit 0 when all consistent.

## 3. Rollout / reconcile current drift
- [ ] 3.1 After merge, `workflow_dispatch` an immediate run so the mirror reconciles all open issues
      (fixes the measured 19-issue drift: board Ready=1 vs label status:ready=20). Confirm the
      detector passes. (Documented in the PR; the mirror pass itself performs the reconcile.)

## 4. Flow skills stop writing status labels + use cheap discovery
- [ ] 4.1 Remove every `--add-label status:*` / `--remove-label status:*` from
      `.claude/skills/flow-{activate,implement,address,finalize}/SKILL.md`; they set board Status
      only (the mirror follows).
- [ ] 4.2 flow-board (and any dispatch discovery) uses `gh issue list --state open --label
      status:ready --json number,title` for candidate discovery instead of a broad `gh issue list
      --limit N --json …body…`.
- [ ] 4.3 Keep the claim protocol unchanged: claim ref + fresh board read remain the authority.

## 5. Doctrine (same change)
- [ ] 5.1 `CLAUDE.md`: update the Path A / label wording — `status:*` is now an ENFORCED read-mirror
      of board Status for discovery; claim ref + fresh board read remain the dispatch/claim
      authority; skills no longer write labels.
- [ ] 5.2 `docs/development/pm-operating-loop.md`: same reframing + the cheap-discovery query.
- [ ] 5.3 Website `agents-developing/delivery-pipeline` page: same.

## 6. Tests (gate of record: tooling-tests)
- [ ] 6.1 `scripts/tests/test_board_label_mirror.sh` (stubbed gh/GraphQL, like test_worker_supervisor.sh):
      Ready→ready set + others removed; Backlog→none; idempotent second run = no change; seeded
      mismatch → detector exit non-zero; consistent → exit 0; missing-token → fail loud.
- [ ] 6.2 Doctrine grep test: no flow-* skill contains `add-label status:` / `remove-label status:`.
- [ ] 6.3 Workflow injection lint (`scripts/ci/check-workflow-injection.sh`) clean — no
      `${{ }}`→`run:` interpolation in the new steps (pass event data via env).

## 7. Gate + audit + review (endgame via flow-closer)
- [ ] 7.1 `--lite` each round (summary-file redirect).
- [ ] 7.2 roborev review-first on the lite-green diff (workflow-injection + logic review).
- [ ] 7.3 flow-closer: ONE full `agent-gate.sh` → spec-auditor (C) anchored to
      `openspec/changes/board-label-mirror/specs/**` → final roborev → merge-on-green → finalize +
      the workflow_dispatch reconcile (task 3.1).

## Notes
- GitHub Actions + shell + docs + skills only — no Rust, no no-heuristics/memory-budget impact.
- One-way only (Status→label); writing a label never changes Status.
- Do NOT weaken the claim authority: label is discovery narrowing, claim ref decides.
