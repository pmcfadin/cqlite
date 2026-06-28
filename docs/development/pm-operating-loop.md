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
3. **Route — spec-first for new work**: design-driven / any new feature → run **`flow-activate` FIRST**
   (produces the OpenSpec proposal/design/specs/tasks, STOPS at Seam 1 for owner spec approval); no code
   until the spec is approved. Oracle-driven bug (Cassandra/sstabledump truth + pinned test) → straight to implement.
4. **Run to completion** (`flow-implement`) via subagents (worker orchestrates; `sstable-developer` model:opus
   implements + runs the gate). **Out-of-scope bug found** → a subagent files a new detailed issue (never fix
   it inline / never grow the diff); if it **blocks** completion, comment "blocked on #<new>" on your issue,
   pause, and surface to the manager (it sequences via `HOLD`/Ready) — fix it only as its own 1:1:1:1 claim.
5. **Before merging**: re-check for an open `HOLD`. If `HOLD: merge after #N`, block until #N is merged.
   Merge only on `agent-gate.sh` PASS + spec-auditor C PASS (design) + roborev clean + HOLD cleared.
6. **Merge + clean up** (`flow-finalize`): squash-merge, archive any OpenSpec change, **stamp the
   telemetry ledger**, remove the worktree, delete the origin claim branch, close the issue with a
   traceable comment. Board → Done (built-in).

## Self-improvement loop (telemetry + retro)

The pipeline measures itself so improvement is data-driven, not anecdotal:
- **Sense** — at finalize, the worker stamps one record per completed issue into the append-only ledger
  `docs/reports/delivery-telemetry.jsonl` (schema: `docs/reports/delivery-telemetry.schema.json`) via
  `scripts/delivery-telemetry.py record`. Records hold authoritative data only — GitHub-derived
  timestamps (cycle time + coarse phase durations) plus run-observed counters (claim collisions, rebase
  events, agent-gate pass/fail + run count, roborev findings, rework). A counter that was not observed is
  an error, never a fabricated `0`.
- **Diagnose** — on a cadence (per-epic or weekly) the **manager** runs `delivery-telemetry.py retro`,
  which ranks the recorded failure categories by a documented weighted tally (deterministic, not an
  inferred model) and reports the single highest-cost recurring failure. `--file` files a `flow-meta`
  improvement issue, deduped against open `flow-meta` issues by a stable category marker.
- **Improve** — that `flow-meta` issue enters Ready and runs through the normal pipeline like any other.

The `delivery-telemetry` agent-gate component (SKIP-aware on `python3`) covers the tool: schema
round-trip, lint-rejects-malformed, fixture-ledger → expected top failure, and dedupe.

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
