# Merge Process Workflow

> **Superseded (issue #1855).** The old manual merge steps (and the nonexistent "Test (Windows)" required
> check) are gone — they encoded a human-merge model the pipeline no longer uses.

Merge is **autonomous on green**: the moment **local certification** is met —
`scripts/agent-gate.sh` PASS + (design-driven) spec-auditor **C** PASS + roborev clean (a terminal
`RESULT: PASS` from `scripts/flow/roborev-review.sh`, the only sanctioned roborev invocation — #2964;
`NOTHING-TO-REVIEW` and FAIL are both blocked merges) — a worker (or the
lead), after the pre-merge SHA assert + `HOLD` re-read, **arms `gh pr merge --auto --squash --delete-branch`**
and stops. GitHub lands the PR when the #2433 `required` check goes green (#2667), then `flow-finalize <N>`.
**Never `ScheduleWakeup`-poll a PR's own CI** — arming `--auto` replaces the busy-wait.

- Merge is **not** a human gate; the owner's spec approval (Seam 1) is the only standing human seam.
- **Hold the merge** only for: a genuine design-call roborev finding, a scope/product question, an
  unmet/uncovered requirement, work outside the issue, or a manager `HOLD: merge after #N` order.
- If `gh pr merge --auto` (GraphQL) is throttled, retry; `--auto` is set-once and idempotent, so GitHub
  still lands the PR when the required check passes.
- **Reading the outcome (full rules in `.claude/agents/flow-closer.md` step 5(d), #3042):** the merge
  timestamp (`mergedAt` / REST `merged_at`) is the ONLY reliable merged-probe (an OPEN PR's
  `merge_commit_sha` is populated speculatively and is NOT a merge); a nonzero `--delete-branch` exit
  often accompanies a SUCCESSFUL merge; and `--auto` on an already-green PR may be rejected with
  `Pull request is in clean status`, whose fallback is a GraphQL `mergePullRequest` with
  `expectedHeadOid` — never a REST merge.

Full doctrine: `docs/development/pm-operating-loop.md`; pipeline stages: the `flow-*` skills.
