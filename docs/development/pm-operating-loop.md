# PM Operating Loop (flow-lead as continuous manager)

flow-lead runs as a **persistent loop on the lead machine** — one machine is the PM, the
others are workers. The PM writes no production code; it grooms, prioritizes, **sequences
merges**, reconciles the board, and surfaces decisions. Owner keeps the two seams (spec
approval, and merges outside the auto-merge class).

## Each tick (self-paced — faster when PRs are landing, slower when the fleet is heads-down)

1. **Auth**: `gh auth switch --user pmcfadin && gh auth setup-git` (EMU flip guard).
2. **Reconcile** (the `flow-board` sweep): board Status vs origin `issue-*` locks vs open PRs
   vs worktrees. Every In Progress/In Review item must have **exactly one** `issue-N-*` lock.
   Flag any issue with >1 lock (1:1:1:1 violation) — do NOT auto-clean it.
3. **Reap** abandoned claims (In Progress, lock branch stale > stale-window) → surface, don't steal.
4. **Run the merge queue** (see below).
5. **Board hygiene**: null-status issues → Backlog; epics out of `Ready`; (optionally) keep PRs
   off the board.
6. **Surface NEEDS-YOU**: spec approvals, scope/epic decisions, non-auto-merge green PRs, drift
   that needs a human call. Post as a single status comment / message.

## Auto-merge class (whitelisted — merge without a per-PR ask)

A PR qualifies for **autonomous merge** ONLY if ALL hold:
- It's a **behavior-preserving refactor** (epic #1116 child: file/module split, no API/behavior delta), OR another class the owner later whitelists here.
- `scripts/agent-gate.sh` **PASS** (fmt, clippy -D warnings, tests, file-size ratchet).
- **roborev clean** (no open findings). roborev in this env: `--agent claude-code --model opus`.
- CI required checks green.
- It is the **only** merge in flight (one at a time — see sequencing).

Everything else (design-driven changes, anything touching behavior/format/parity, anything
with an unresolved roborev finding, non-#1116 work) → **open the PR and surface it**; the owner merges.

## Merge sequencing (the gap the 18-agent fleet exposed)

The claim-lock prevents two agents on one file; it does **nothing** for cross-cutting merge
conflicts. 18 splits in cqlite-core/cqlite-cli all edit `mod.rs`/`lib.rs` re-export points.
So merges are **serialized**:
1. Pick the **oldest green** auto-merge-class PR.
2. Ensure it's rebased on current `origin/main`; if it conflicts, hand it back to its worker
   to rebase (don't rebase another worktree's branch from the lead).
3. Re-confirm gate + roborev on the rebased tip, then squash-merge.
4. The merge moves main → the remaining PRs are now behind. Notify their workers to rebase
   (or let the next tick re-check each for conflicts).
5. **WIP cap**: keep at most a small number of #1116 children at `In Review` simultaneously;
   if more than that pile up, pause new activations until the queue drains.

Native GitHub **merge queue** on `main` (with required checks) is the eventual upgrade — it
serializes + auto-rebases server-side. It needs branch-protection required checks configured
(owner/repo-admin). Until then the PM drives the queue manually as above.

## Hard rules (unchanged)
- The gate is the only run that counts; paste its summary block.
- Never close an epic, change scope/title, or make a product decision without the owner.
- Work from a worktree; stage explicit paths; the branch push is the cross-machine lock.
- Every GitHub write gets a short traceable comment.
