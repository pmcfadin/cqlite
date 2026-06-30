## 1. Merge-on-green mechanism

- [ ] 1.1 Define the worker terminal state: PR-open + `agent-gate.sh` PASS + roborev clean (+ C PASS for
      design-driven) → arm merge-on-green → end turn. No CI yield-poll.
- [ ] 1.2 Detect branch-protection auto-merge availability; arm `gh pr merge --auto --squash
      --delete-branch` when available, else hand off to the manager poller/merge-engine. Log which path.
- [ ] 1.3 Guard the green signal (D3): do not auto-land against an empty required-check set; require
      configured required checks or the poller's explicit lane set.

## 2. Doctrine + skills

- [ ] 2.1 Update `docs/development/pm-operating-loop.md`: worker terminal state + merge-on-green + explicit
      no-CI-busy-wait prohibition.
- [ ] 2.2 Update the `agents-developing/delivery-pipeline` website page to match.
- [ ] 2.3 Align the `worker` and `flow-implement` skill text (`.claude/skills/...`) with the new terminal
      state + merge-on-green mechanism.

## 3. Quality gates (definition of done)

- [ ] 3.1 `scripts/agent-gate.sh` PASS — paste the AGENT-GATE SUMMARY block verbatim. (Doc/skill-only
      change; gate still runs.)
- [ ] 3.2 Intent audit **C** (`spec-auditor` anchored to
      `openspec/changes/issue-1176-codify-merge-on-green/specs/**`) PASS.
- [ ] 3.3 roborev (`--agent codex --base origin/main`) clean.
- [ ] 3.4 PR opened; **dogfood the new doctrine** — arm merge-on-green and stop (do not busy-poll CI);
      then `flow-finalize` (archive change, cleanup, close #1176).
