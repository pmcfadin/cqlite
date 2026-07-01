## Why

A flow-lead **worker** that waits on the cross-platform CI matrix *after opening its PR* spawns a CI
poller, yields, gets marked "completed", and re-orients at **~55–60k tokens per cycle**. Issue #1086's
worker did this ~10×; a single unattended session this week (the one that filed this) incurred the same
busy-wait **~6×** while polling `ScheduleWakeup` against the parity-CI matrix — pure token bleed when the
work was already done (PR open, gate PASS, roborev clean) and only the external CI matrix remained.

The fix is already proven in practice: the worker reaches **PR-open + agent-gate PASS + roborev clean
(+ spec-auditor C PASS for design-driven)** and then **STOPS**, leaving the final land to a
merge-on-green mechanism — GitHub's native `gh pr merge --auto --squash`, or a manager-owned poller as
the fallback when branch-protection auto-merge isn't enabled. This change codifies that in the delivery
doctrine + the worker/flow-implement skills so the busy-wait can't recur.

- **Milestone:** maintenance / delivery-pipeline process. **Design-driven** — process + doctrine + skill,
  with real latitude in *how* merge-on-green is wired (native `--auto` vs manager merge-engine vs both).
  Hence OpenSpec + Seam 1 (per the manager's routing on the issue).
- Modifies the existing `delivery-pipeline` capability (adds a merge-on-green / no-busy-wait requirement).
- No code/library impact; this is agent-operating doctrine + the `flow-*` skill text + the
  `pm-operating-loop.md` / delivery-pipeline website page.

## What Changes

- **Worker stops at PR-open + green-quality-bar; never polls external CI in a yield loop.** After a
  worker reaches PR-open with `agent-gate.sh` PASS + roborev clean (+ C PASS for design-driven), it sets
  the merge-on-green mechanism and **terminates its turn** rather than scheduling repeated CI-poll
  wake-ups. Observable: a worker run on a simple issue opens the PR and ends without N CI-poll
  stop/resume cycles.
- **Merge-on-green mechanism.** Preferred: `gh pr merge --auto --squash --delete-branch` (GitHub lands it
  when required checks pass; requires repo auto-merge enabled). Fallback when `--auto` is unavailable: a
  manager-owned poller / merge-engine lands it on green. Either way the issue auto-closes via the PR's
  `Closes #N`.
- **Doctrine updated:** `docs/development/pm-operating-loop.md` + the `agents-developing/delivery-pipeline`
  page describe merge-on-green and **explicitly forbid worker CI busy-waiting**; the worker/flow-implement
  skill text is aligned.

## Non-goals

- Not changing the quality bar (gate PASS + C + roborev) or the two human seams (spec approval; merge
  remains the same authority model — this only changes the *mechanism* by which a green PR lands, not who
  authorizes it).
- Not building a new bespoke CI service; the manager-poller fallback reuses the existing manager pattern.
- Not removing `ScheduleWakeup` as a tool — it remains valid for genuinely external, harness-untracked
  state; what's forbidden is using it to busy-poll a PR's own CI after the work is done.

## Decisions for the owner (recommended defaults — confirm/adjust at approval)

1. **Mechanism** → prefer `gh pr merge --auto --squash --delete-branch`; fall back to the manager poller
   when branch-protection auto-merge is not enabled on the repo. *Recommended.* (Sub-question: do you want
   auto-merge enabled on the repo so `--auto` is the primary path? If not, the manager-poller is primary.)
2. **Where the worker stops** → immediately after PR-open + gate PASS + roborev clean (+ C for
   design-driven), having set the merge mechanism; it does NOT verify CI itself. *Recommended.*
3. **Required-checks definition for `--auto`** → since `main` currently has no required status checks,
   `--auto` would merge immediately; the spec requires that the merge-on-green path only auto-lands once a
   defined green signal exists (either real required checks are configured, or the manager-poller gates on
   the chosen lane set). *Recommended — surfaced because it interacts with branch protection config.*

## Impact

- Modifies `delivery-pipeline` capability spec; edits `docs/development/pm-operating-loop.md`, the
  delivery-pipeline website page, and the `worker`/`flow-implement` skill text. No production code.
