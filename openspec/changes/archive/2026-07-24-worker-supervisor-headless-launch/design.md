# Design — worker-supervisor headless launch

## The corrected default invocation

```sh
WORKER_CMD="claude -p --dangerously-skip-permissions --agent flow-lead '/worker'"
```

Each token fixes one validated failure:
- `-p` (`--print`) — non-interactive; runs the prompt to completion and exits, matching the
  supervisor's one-shot-per-iteration model. Without it, `claude` blocks on an interactive TUI.
- `--dangerously-skip-permissions` — a supervisor-spawned session has no human to approve
  per-command permission prompts, so `gh project`/`gh auth`/`git worktree` etc. are auto-denied.
  Skip-permissions is the standard headless posture; the safety net is the gate + roborev +
  spec-audit + GitHub branch-protection + `agent-notify` pages + the breaker/wall-clock budgets,
  NOT per-command keyboard approval.
- `--agent flow-lead` — the registered orchestrator agent. `worker` is a `/`-command, not an agent.
- `'/worker'` — the prompt: invoke the worker skill (single-issue session mode, #2090).

The literal prompt text is kept minimal (`/worker`) because the skill body carries the full
contract; the previous long inline prompt is redundant with the skill.

## Decision: how to close the `-p` watchdog gap

`-p` writes activity to the session transcript, not stdout, so the supervisor's
`bash -c "$WORKER_CMD" >"$logfile" 2>&1` captures an empty `iter-N.log`. The
stuck-on-question watchdog (`detect_prompt_signature`/`log_size` reading `iter-N.log`) then
never fires.

Three options considered:

| Option | Approach | Verdict |
|--------|----------|---------|
| **A. Capture `-p` output to iter-N.log** (CHOSEN) | Add `--output-format stream-json` (or `--verbose`) so `-p` emits a live event stream to stdout, which the existing `>"$logfile"` redirect captures. The watchdog's tail-scan + no-growth logic then works unchanged. | Smallest, most local change. Keeps the watchdog contract intact. Log also becomes useful for humans tailing it (fixes the "tail is empty" confusion we hit). |
| B. Point watchdog at the transcript | Resolve `~/.claude/projects/<proj>/<session-id>.jsonl` and scan it instead. | Rejected: the supervisor doesn't know the worker's session-id (claude self-assigns it); mapping is fragile and cmux-specific. |
| C. Document watchdog as `-p`-incompatible | Leave the log empty; rely solely on breaker + wall-clock. | Rejected: silently drops a safety feature. Acceptable only as a fallback if A proves unreliable. |

**Chosen: A**, with C as the documented fallback if the stream format is unavailable. The
watchdog's positive-wedge-evidence logic (signature in tail AND log not growing across two
scans) still holds against a streamed log: a wedged interactive prompt emits no further stream
events, so the byte size freezes exactly as the watchdog expects.

If `-p`'s stream output proves too chatty/noisy for the prompt-signature regex, the fallback
is C — flip a documented flag and lean on the breaker/wall-clock backstops — but A is tried first.

## Decision: the coupled orphan-detection probe

`PROC_MATCH_WORKER='[c]laude.*--agent worker'` must change to `'[c]laude.*--agent flow-lead'`
so it matches the corrected spawn. Analysis of the exclusion property:
- The bracket trick (`[c]laude`) still prevents the probe's own `bash -c` wrapper from
  self-matching.
- **Widened match risk:** `--agent flow-lead` also matches an interactive `claude --agent
  flow-lead` session an operator runs by hand (e.g. this very lead session). Per the
  one-worker-per-machine rule (#1930) that is arguably correct to flag, but to avoid a
  false leftover-hold against a deliberate interactive lead, the pattern is tightened to the
  full spawn shape including `-p`: `'[c]laude.*-p.*--agent flow-lead'` (an interactive lead
  has no `-p`). This preserves "detect orphaned unattended workers" while excluding an
  interactive REPL and an interactive lead session.

## Alternatives for the agent (why not create a `worker` agent)

Creating `.claude/agents/worker.md` would make `--agent worker` valid and keep the historical
command literally correct. Rejected: the `/worker` skill already encodes the flow-lead-worker
persona; a separate agent duplicates that and adds a second artifact to keep synchronized with
the skill. Reusing `flow-lead` + `/worker` is one source of truth.

## Test strategy

- `scripts/tests/test_worker_supervisor.sh`: update the orphan-detection pattern case to the
  new `-p.*--agent flow-lead` shape; assert an interactive `claude --agent flow-lead` (no `-p`)
  is NOT matched; assert a plain `claude` REPL is NOT matched.
- Add a case asserting the default `WORKER_CMD` (when unset by the caller) contains `-p`,
  `--dangerously-skip-permissions`, and `--agent flow-lead` — so a future edit that drops one
  fails the test rather than shipping a silently-broken default.
- The gate `tooling-tests` component runs this suite; it stays the gate of record.
