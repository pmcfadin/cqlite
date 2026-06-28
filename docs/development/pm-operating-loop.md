# Delivery operating model — manager + flow-lead workers

Two roles. One board. The manager orchestrates; the workers do everything else.

## Roles

| | **Manager** (one window, `/manager`) | **flow-lead workers** (N windows / machines) |
|---|---|---|
| Writes code / claims / merges? | **Never** | Yes — owns the issue end-to-end |
| Board | Controls **Ready** (what + order); reconciles; reaps | Reads Ready; claims the oldest unlocked item |
| Lifecycle | none | full **1:1:1:1**: claim → implement → gate → C → roborev → PR → **merge → cleanup** |
| Communication | signed **issue comments** (work orders) + Ready ordering | reads manager comments before acting; obeys the latest order |
| Tempo | sets it via Ready throughput, WIP cap, and ordering | runs flat-out on its claimed issue |

**Ready = the dispatch queue. A signed comment = a work order.** Those are the manager's only channels.

## Manager → worker comment protocol

Every manager order begins with a marker so workers parse orders, not human chatter:

```
🧭 **MANAGER** <!-- MGR:<id> -->
GO                      # cleared to run to completion
HOLD: merge after #N    # build + reach green, then block the merge until #N is merged
ORDER: k                # queue rank when several are Ready at once
<free-text / dependency notes>
```

`<id>` = a stable manager-session tag (host + short id). Workers obey the **latest** manager order.

## Worker lifecycle (flow-lead)

1. **Pick up**: take the oldest `Ready` issue with **no** `issue-N-*` lock on origin. Claim it
   (branch push = the cross-machine lock); first push wins, losers take the next item.
2. **Read orders**: read the issue's manager comments. Note any `HOLD` / `ORDER` / instructions.
3. **Run to completion** (`flow-implement`): design-driven pauses at Seam 1 for owner spec approval, then
   resumes; oracle/refactor runs straight through.
4. **Before merging**: re-check for an open `HOLD`. If `HOLD: merge after #N`, block until #N is merged.
   Merge only on `agent-gate.sh` PASS + spec-auditor C PASS (design) + roborev clean + HOLD cleared.
5. **Merge + clean up** (`flow-finalize`): squash-merge, archive any OpenSpec change, remove the worktree,
   delete the origin claim branch, close the issue with a traceable comment. Board → Done (built-in).

## Merge sequencing (why HOLD exists)

The claim-lock prevents two agents on one file; it does nothing for cross-cutting `mod.rs`/`lib.rs`
re-export conflicts (e.g. 18 concurrent #1116 splits). The manager sequences by **Ready ordering** and
**`HOLD: merge after #N`** so dependent or conflict-prone work lands in a safe order. Workers rebase on
the current `origin/main` before merging; if a rebase conflicts, the worker resolves it in its own
worktree (the manager never rebases someone else's branch).

## Human seams (unchanged)
- **Seam 1 — spec approval**: design-driven issues stop after `flow-activate` for owner approval.
- **Exceptions / product calls**: scope, epic close, conflicting requirements → manager surfaces a
  **NEEDS-YOU** list; never decided autonomously.
- Workers otherwise merge autonomously on green. There is no human merge click for worker-owned issues.

## Hard rules
- The gate is the only run that counts; paste its summary block.
- Worktrees only; the branch push is the lock; stage explicit paths.
- EMU guard every board op: `gh auth switch --user pmcfadin && gh auth setup-git`.
- roborev in this env: `--agent claude-code --model opus`.
- Every GitHub write gets a short traceable comment.
