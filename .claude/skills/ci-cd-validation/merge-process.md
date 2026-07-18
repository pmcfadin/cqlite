# Merge Process Workflow

> **Superseded (issue #1855).** The old manual merge steps (and the nonexistent "Test (Windows)" required
> check) are gone — they encoded a human-merge model the pipeline no longer uses.

Merge is **autonomous on green**: the moment **local certification** is met —
`scripts/agent-gate.sh` PASS + (design-driven) spec-auditor **C** PASS + roborev clean — a worker (or the
lead), after the pre-merge SHA assert + `HOLD` re-read, **arms `gh pr merge --auto --squash --delete-branch`**
and stops. GitHub lands the PR when the #2433 `required` check goes green (#2667), then `flow-finalize <N>`.
**Never `ScheduleWakeup`-poll a PR's own CI** — arming `--auto` replaces the busy-wait.

- Merge is **not** a human gate; the owner's spec approval (Seam 1) is the only standing human seam.
- **Hold the merge** only for: a genuine design-call roborev finding, a scope/product question, an
  unmet/uncovered requirement, work outside the issue, or a manager `HOLD: merge after #N` order.
- If `gh pr merge --auto` (GraphQL) is throttled, retry; `--auto` is set-once and idempotent, so GitHub
  still lands the PR when the required check passes.

Full doctrine: `docs/development/pm-operating-loop.md`; pipeline stages: the `flow-*` skills.
