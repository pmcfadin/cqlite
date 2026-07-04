# Merge Process Workflow

> **Superseded (issue #1855).** The old manual merge steps (and the nonexistent "Test (Windows)" required
> check) are gone — they encoded a human-merge model the pipeline no longer uses.

Merge is **autonomous on green**: a worker (or the lead) merges its own PR the moment the quality bar is met
— `scripts/agent-gate.sh` PASS + (design-driven) spec-auditor **C** PASS + roborev clean —
via `gh pr merge --squash --delete-branch`, then `flow-finalize <N>`.

- Merge is **not** a human gate; the owner's spec approval (Seam 1) is the only standing human seam.
- **Hold the merge** only for: a genuine design-call roborev finding, a scope/product question, an
  unmet/uncovered requirement, work outside the issue, or a manager `HOLD: merge after #N` order.
- If `gh pr merge` (GraphQL) is throttled, fall back to `gh api repos/OWNER/REPO/pulls/N/merge` (REST).

Full doctrine: `docs/development/pm-operating-loop.md`; pipeline stages: the `flow-*` skills.
