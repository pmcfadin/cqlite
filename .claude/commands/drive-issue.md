---
description: Use when told to drive, run, or own ONE named cqlite issue to done/merged in this session (e.g. "drive issue 3272"), including unattended runs where lead/owner answers arrive asynchronously on the issue thread. Requires an issue number argument.
---

# /drive-issue <N> — drive one named issue to green and merged, with coord comms + cron re-checks

**Issue:** `$ARGUMENTS` — required, exactly one issue number. No argument, or not a number → say so
and stop; never guess an issue or fall back to the Ready column.

You are a **flow-lead worker bound to exactly ONE issue**. Your baseline operating rules are
`.claude/commands/worker.md` — read it and obey ALL of it: context discipline (you orchestrate;
subagents implement, gate, and review — explicit `model:` on every spawn), worktree isolation
(never touch the root checkout), the claim protocol (`claim.sh` ref FIRST), the implement loop
(TDD → `--lite` each round → review-first → PR → `flow-closer` endgame), finalize + telemetry, and
every hard rule (gate summary-file contract with the `PASS|FAIL` probe, roborev ONLY via
`scripts/flow/roborev-review.sh --agent … --model …`, merge ONLY via `gh pr merge --auto`, never
poll your own PR's CI, `CQLITE_DATASETS_ROOT` from the fetch script's printed export, commit early
and often).

The FOUR deltas below OVERRIDE worker.md wherever they conflict. Everything else: worker.md wins.

## Delta 1 — Issue selection is fixed, not board-picked

You take **issue #$ARGUMENTS only**. Never a second issue, never a substitute.

- Board `Status=Done` or issue closed → report "already done" and stop.
- `claim.sh claim <N>` returns `CLAIM LOST` → another machine owns it; report the holder and stop.
  Adoption of a reaped/legacy claim follows ONLY the documented `adopt` procedure (CLAUDE.md) after
  confirming abandonment — never race a live lane.
- Board `Status=Backlog` → the issue is deliberately not dispatchable (usually gated on another
  issue — read its thread). Report why and stop; do not "helpfully" promote it yourself.

## Delta 2 — Comms are github-coord, not manager-signed comments

At session start, run the **`github-coord:github-coord-worker`** skill and announce it on your
first issue comment. ALL upward communication goes through its protocol on issue #N's thread:

- One request marker per question; `coord:needs-attention` for decisions/questions,
  `coord:blocked` when you cannot make dependency-safe progress, `coord:follow-up-proposed` for
  scope you want split out. Those three labels are the ONLY tracker metadata you touch.
- **Never `AskUserQuestion`.** A prompt in an unattended session is a hang (#2666); the coord
  request + Delta 3's cron replaces it entirely.
- The LEAD clears coord labels, never you. You may proceed the moment a response comment
  **pairing your request ID, strictly newer than your request**, exists — even if the label is
  still on.
- Do not decide spec-ambiguous questions alone; resolve from (in order) approved spec, issue
  acceptance criteria, CLAUDE.md doctrine, repo state — and escalate only what those cannot decide.

## Delta 3 — Waiting = ONE cron re-check, never park-and-exit, never an in-session poll

When you are blocked on a lead/owner response (Seam-1 spec approval, a decision request, a HOLD):

1. Post the coord request (marker + label) with your recommendation and a default.
2. Record durable state in the worktree **through the script, never by hand** (#3822):
   `bash scripts/flow/drive-issue-state.sh write <N> --stage <stage> --request-id <id> [--pr <n>]
   [--branch <b>] [--body-file <notes.md>]`. It stamps the marker with this lane's ownership
   identity (issue, machine, worktree, session, session pid + start window, actor) inside a bounded
   prologue and REFUSES to overwrite a marker that is not yours — a hand-written marker carries no
   stamp and Delta 4 will refuse to rehydrate from it.
3. **Arm the cron**: `CronList` first — if a job named `drive-issue-<N>` already exists, do NOT
   create another. Else `CronCreate` name `drive-issue-<N>`, interval ~15 minutes, prompt exactly:
   `/drive-issue <N>`. The command is resume-safe (Delta 4), so each firing rehydrates, checks for
   an answer, and either continues the pipeline or goes back to sleep.
4. Refresh the claim heartbeat (`claim-heartbeat.sh beat <N>`), then **end the turn cleanly**. The
   cron does the waiting — no `sleep`, no in-session polling loop, no exiting-and-abandoning.

**Cron hygiene (non-negotiable):** exactly one cron per issue, named `drive-issue-<N>`.
`CronDelete` it the moment you are unblocked, and ALWAYS at merge/finalize/stop/kill — a leaked
cron re-invokes forever. Deleting the cron is part of "done"; a report that omits it is incomplete.

`resume-dont-ask` label on the issue = standing Seam-1 seal: proceed without asking or waiting.

## Delta 4 — Every invocation is a RESUME first

On start, BEFORE anything else: `git fetch origin`, then check whether this machine already holds
`refs/claims/issue-<N>` (`claim.sh verify <N>`) or has the issue worktree.

- **Held → resume**: gate the rehydrate on
  `bash scripts/flow/drive-issue-state.sh verify <N>` (#3822) — never read the marker's prose
  first. `--help` is the authoritative contract; act on the `verdict` token:
  - `OWNED` (0) → read the marker + the issue thread and resume from the recorded stage. If a
    pairing response newer than your open request exists → `CronDelete drive-issue-<N>`, `write`
    the marker again with the cleared request, and continue. If not → beat the heartbeat, post
    nothing, end the turn (the cron persists). Never re-ask an unanswered question.
  - `ABSENT` (3) → no durable state; treat as a fresh start of this stage and `write` one.
  - `ADOPTABLE` (5) → the recorded writer is provably gone (the normal cron re-invoke on a new
    session id). Take it EXPLICITLY: `drive-issue-state.sh adopt <N> --reason <what the resume is>`,
    which rewrites the stamp and records the prior session. Then resume.
  - `LIVE-PEER` (6) → a live peer session owns this lane. STOP: post nothing, adopt nothing, end
    the turn. Do not "fix" it by rewriting the marker.
  - `LIVENESS-UNKNOWN` (7) → liveness could not be measured. STOP the same way and say so in your
    report; never adopt on unproven information.
  - `FOREIGN-ISSUE`/`FOREIGN-MACHINE`/`FOREIGN-WORKTREE` (4) → this marker is not about this
    lane's work (a reused lane, a copied tree, a marker that travelled with a branch). Escalate to
    the lead rather than adopting or deleting it.
  - `UNSTAMPED`/`MALFORMED`/`DUPLICATE-SENTINEL` (8) → the marker predates #3822 or was hand-edited,
    so its plan could belong to any session. Do NOT adopt its contents; re-`write` a stamped marker
    only if this lane's work genuinely IS the issue it describes.
- **Not held → fresh start**: claim per worker.md step 3, then route (oracle-driven → straight to
  implement; design-driven → `flow-activate` to Seam 1, render the spec INLINE in an issue comment
  as the approval request, then Delta 3). The spec render and the coord request are **ONE combined
  comment** — spec + marker + recommendation + default together — never two posts.

All state is durable outside your context (claim ref, worktree commits, issue thread, OpenSpec
files, state marker, board) — a fresh session must be able to pick up mid-pipeline from those
alone. If something exists only in your window, write it down before ending the turn.

## The pipeline (worker.md's, restated in one line)

claim → route (Seam 1 if design-driven, via Delta 3) → `flow-implement` (subagent TDD, `--lite`
each round, rust-reviewer + sanctioned roborev on the lite-green diff BEFORE any full gate) → open
PR → `flow-closer` endgame (ONE full gate of record → C intent audit → final roborev →
`premerge-assert` → `gh pr merge --auto --squash --delete-branch`) → `flow-finalize` (archive,
telemetry PR, worktree/branch/claim cleanup) → `CronDelete` → closing comment on the issue with the
terminal packet (verdicts + PR + residuals).

## Red flags — STOP and re-read the matching delta

- About to `AskUserQuestion` → Delta 2: coord request + cron.
- About to `sleep`, loop-poll the thread, or watch your own PR's CI → Delta 3 / worker.md: the
  cron (or GitHub's `--auto`) does the waiting; end the turn.
- About to claim a second issue, or substitute a "better" one → Delta 1: one issue, fixed.
- About to create a cron without `CronList`, or report done with the cron alive → Delta 3 hygiene.
- Re-posting a question that already has an open marker → Delta 4: one marker, one wait.
- Editing labels/milestones/assignees beyond the three `coord:*` labels → Delta 2: not yours.
