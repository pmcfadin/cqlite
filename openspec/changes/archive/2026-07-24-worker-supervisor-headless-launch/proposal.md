# Fix worker-supervisor headless launch (issue #2841)

## Why

The unattended fleet runner `scripts/local/worker-supervisor.sh` cannot spawn a working
worker with its documented default. Validated live on 2026-07-24, the default
`WORKER_CMD` fails in **three independent ways**, each masking the next:

1. **Wrong agent** — the default is `claude --agent worker '…'`, but `worker` is a
   slash-command/skill (`.claude/commands/worker.md`), NOT a registered agent type. Every
   spawn exits 1 with `--agent 'worker' not found`. Three abnormal iterations trip the
   crash-loop breaker in ~50s (`issues_done=0`).
2. **No headless permission grant** — with `--agent flow-lead`, the worker spawns but every
   `gh project` / `gh auth` / `gh api` / `git worktree` / `git -C` command returns
   "requires approval" and is auto-denied (no human in a supervisor-spawned session). The
   worker correctly refuses to label-dispatch (Path A #1886) and prints a "please approve
   these tools" message to a human instead of writing a `blocked` marker → `marker_present=no`
   → abnormal → breaker.
3. **Interactive TUI by default** — `claude` starts an interactive session unless given
   `-p/--print`. Without it the worker opens the chat UI and blocks on keyboard input
   forever (0.1% CPU, 0-byte log, no children) — alive to the supervisor's exit-code check
   but doing nothing.

The validated working invocation is:
`claude -p --dangerously-skip-permissions --agent flow-lead '/worker'`
(confirmed live: claimed #1883, created its worktree, set board In Progress, dispatched an
Explore subagent, began authoring its OpenSpec change).

A **monitoring gap** falls out of fix #3: `-p` streams worker activity to the session
transcript (`~/.claude/projects/<proj>/<session-id>.jsonl`), NOT stdout, so
`logs/worker-supervisor/iter-N.log` stays 0 bytes for a healthy `-p` worker. The
supervisor's mid-iteration stuck-on-question watchdog (`detect_prompt_signature`/`log_size`
on `iter-N.log`) can therefore never fire under `-p`.

A **coupled probe** must stay consistent: `PROC_MATCH_WORKER='[c]laude.*--agent worker'`
(the leftover-worker orphan-detection preflight probe) keys on the literal `--agent worker`.
The corrected spawn uses `--agent flow-lead`, so this probe must be updated or the
leftover-worker guard silently stops detecting orphans.

## What Changes

- Correct the baked-in default `WORKER_CMD` to the validated headless invocation.
- Update `PROC_MATCH_WORKER` to match the corrected spawn shape while still excluding a
  plain interactive `claude` REPL / a different-agent session.
- Resolve the `-p` watchdog gap: capture `-p` output to `iter-N.log` so the existing
  stuck-on-question watchdog keeps working (chosen over pointing the watchdog at the
  transcript, or documenting it as `-p`-incompatible — see design.md).
- Update doctrine that names `claude --agent worker`: `docs/development/fleet-runbook.md`,
  `CLAUDE.md`, and the issue #2090 references.
- Update `scripts/tests/test_worker_supervisor.sh`: the orphan-detection pattern test and a
  new assertion that the default invocation is headless-shaped.

## Non-goals

- **No change to the worker/flow-lead behavior itself** — this is purely the launch
  mechanism and its monitoring/probe wiring.
- **No new `worker` agent** — reusing the registered `flow-lead` agent + the `/worker` skill
  is deliberate (the skill already IS the flow-lead-worker persona; a new agent is another
  artifact to maintain and keep in sync).
- **No cmux integration work.** This box wraps `claude` in cmux (cmux hooks appear in the
  spawned worker's `--settings`), but the plain-CLI `WORKER_CMD` is confirmed working; whether
  a cmux-native worker entrypoint is preferable is out of scope and, if wanted, a separate issue.
- **No change to the breaker / wall-clock / GitHub-merge-verification backstops.**

## Doctrine impact

User-facing launch instructions change, so `CLAUDE.md`, `docs/development/fleet-runbook.md`,
and the website `agents-developing/` delivery-pipeline page are updated in this same change.

## Routing

Design-driven (process/tooling; no SSTable/parity oracle). There is a genuine design choice
(how to close the `-p` watchdog gap) plus a coupled probe to keep consistent, so it goes
through OpenSpec rather than straight to implement.
