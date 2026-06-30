## Context

The busy-wait is structural: a worker that, after opening its PR, calls `ScheduleWakeup` (or any
yield-and-poll) to watch the cross-platform CI matrix re-loads its full context (~55–60k tokens) on each
wake with no work to do — the implementation, gate, and review are already complete. The only missing
signal is external CI, which the harness does not track. The fix decouples "work done" (worker's job)
from "landed on green" (a merge-on-green mechanism's job).

## Decisions

### D1 — Worker terminates at the green-quality-bar; landing is delegated
**Chosen:** the worker's terminal state for an issue is **PR-open + `agent-gate.sh` PASS + roborev clean
(+ spec-auditor C PASS for design-driven)**, with the merge-on-green mechanism armed. It does NOT poll CI.
**Beat:** the status-quo "open PR → ScheduleWakeup every ~270s until CI green → merge" loop (the token
bleed this issue exists to kill).

### D2 — `gh pr merge --auto` preferred, manager-poller fallback
**Chosen:** arm `gh pr merge --auto --squash --delete-branch` when the repo supports branch-protection
auto-merge; otherwise hand off to a manager-owned poller/merge-engine that lands the PR on green. The
choice is detectable (auto-merge enabled?) and the worker picks accordingly, logging which path it armed.
**Beat:** (a) worker-polls-CI (the bug); (b) `--auto`-only (silently never lands if auto-merge is
disabled — exactly the kind of silent degrade we've been bitten by); (c) manager-poller-only (works, but
foregoes the zero-token native path when it's available).

### D3 — Guard the green signal so `--auto` can't land on an empty check set
**Chosen:** the merge-on-green path requires a *defined* green signal. `main` currently has **no required
status checks** (`contexts=[]`), so a naive `--auto` would merge the instant it's set — defeating the
"on green" intent. The spec requires either real required checks be configured for the PR, or the
manager-poller gate on an explicit lane set, before merge-on-green is considered satisfied. **Beat:**
arming `--auto` against an empty required-check set (would merge before CI even starts).

### D4 — Doctrine + skills are the deliverable surface
**Chosen:** the change lands as edits to `docs/development/pm-operating-loop.md`, the
`agents-developing/delivery-pipeline` page, and the `worker`/`flow-implement` skill text — the authoritative
operating docs the agents read. **Beat:** a code-only lint (there's no code path to lint — this is agent
behavior governed by skill/doctrine text).

## Risks / Trade-offs

- **`--auto` requires repo config** (auto-merge enabled + ideally required checks). If neither is set, the
  manager-poller is the operative path; the spec must not assume `--auto` always works (D2/D3).
- **Manager-poller is itself a session** — it still polls CI, but ONCE at the manager level for the whole
  fleet, not N times per worker, which is the efficiency win.
- **Verification of "no busy-wait"** is behavioral (observe a worker run open a PR and end without repeated
  CI-poll wake-ups), encoded as a scenario rather than a unit test.

## Migration / Rollout

Doctrine + skill text edits; immediately effective for new worker runs. No code, no data, no API change.
The fallback path means it ships safe regardless of branch-protection configuration.
