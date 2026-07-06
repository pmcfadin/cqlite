# Doc deltas for issue #2090 — worker-supervisor script (phase 1 output)

This file is the handoff from the SCRIPT half of #2090 (`scripts/local/worker-supervisor.sh`
+ `scripts/tests/test_worker_supervisor.sh`, built in this pass) to the DOCTRINE half
(`.claude/skills/worker/SKILL.md`, `docs/development/fleet-runbook.md`, the #2085
board-only-rehydration text, the website delivery-pipeline page). The supervisor is
already built and tested against the contract below — the worker skill must implement
this contract **exactly** (field names, values, file path) or the supervisor will judge
every real iteration as abnormal and trip the crash-loop breaker.

## ITERATION-MARKER CONTRACT (authoritative)

**Path**: `.worker-last-iteration.json` at the repo root (override via `MARKER_FILE` env
— the supervisor and the worker MUST agree on the same path; the supervisor passes
`MARKER_FILE` through its own environment when it invokes `WORKER_CMD`, so the worker
skill should read `${MARKER_FILE:-<repo-root>/.worker-last-iteration.json}` rather than
hardcoding the path).

**When written**: as the worker's LAST act before it exits the session — after any
merge/finalize/telemetry-stamp work is complete, whatever the outcome. The write must
be the final thing the worker does; anything after it (that could itself fail) would
undermine the guarantee the supervisor relies on.

**Shape** (strict JSON object, single line or pretty — the supervisor parses with `jq`
if present, else a `python3 -c 'json.load(...)'` fallback, so either is fine):

```json
{
  "outcome": "finalized",
  "issue": 1234,
  "pr": "https://github.com/pmcfadin/cqlite/pull/1235",
  "duration_s": 842,
  "reason": null
}
```

**`outcome` — exactly one of**:

| value | meaning | `issue`/`pr` | counts vs `MAX_ISSUES` | breaker | supervisor follow-up |
|---|---|---|---|---|---|
| `finalized` | claimed an issue, drove it through gate → C/review → merge-on-green → `flow-finalize` → telemetry stamp | MUST be set | **yes** | resets to 0 | info notify, journal `finalized` |
| `no-work` | rehydrated from the board (or checked for an own resumable claim branch) and found nothing to do | may be `null` | no | resets to 0 | `BACKOFF_NOWORK_SECS` sleep before next iteration (default 900s) — prevents hot-polling an empty Ready column |
| `blocked` | made real progress but stopped short of merge for a reason that needs the owner (design-call roborev finding, scope/product question, unmet acceptance criterion, an explicit `HOLD: merge after #N` order) | `issue` MUST be set; `reason` MUST be set | no | resets to 0 | info notify with `reason`; supervisor remembers the issue (does not auto-retry it) |

Any **other** value in `outcome`, a marker **missing required fields** for its outcome, a
**nonzero worker exit code**, or **no marker file present at all** when the worker
process exits => the supervisor judges the iteration **abnormal**. `BREAKER_N`
consecutive abnormal iterations (default 3) stop the supervisor entirely with a
high-priority notification — it never hot-respawns past that point.

**Stale-marker rule**: the supervisor runs `rm -f "$MARKER_FILE"` immediately before
every `WORKER_CMD` invocation. A marker left over from a previous (possibly crashed)
invocation must never be re-judged as the current iteration's result — so the worker
skill does not need to clean up its own marker on entry; the supervisor already
guarantees a clean slate. Conversely, the worker MUST NOT rely on a marker from a prior
run still being present (e.g. to detect "did I crash last time") — that state lives on
the board / claim branch, not in this file.

**Single-issue rule this marker enforces**: because the supervisor removes the marker
before spawning and treats "no marker at process exit" as abnormal, a worker that claims
a **second** issue in one session and then exits without writing (or writes prematurely
and keeps going) risks a false abnormal judgment on next inspection. The worker skill's
existing "never claim a second issue in the same session" contract is what keeps the
write timing simple (one claim → one outcome → one write → exit).

## Doctrine notes for the worker-skill edit (phase 2)

1. **Resume-own-claim-first**: before checking the board's Ready column, the worker
   must check for an existing `issue-<N>-*` branch pushed to origin under this
   machine's own prior claim (crash recovery — issue #1930's "any `issue-<N>-*`
   branch" pre-claim check is the board-authority side of this; the marker contract
   above is the supervisor-visible side). If a resumable claim exists, resume it and
   do not touch the Ready column at all for that iteration.
2. **Exit is mandatory, not advisory**: the skill's contract is not "should exit" but
   "MUST exit the process after writing the marker" — the supervisor has no way to
   force-kill a hung worker gracefully mid-write (it relies on `MAX_ITER_SECS` /
   `run_with_timeout`, which is a blunt `SIGTERM`, not a clean shutdown), so a worker
   that lingers after its marker write risks a `timeout`-killed abnormal judgment on an
   otherwise-successful iteration.
3. **`reason` field for `blocked`**: keep it short (one line) — it flows verbatim into
   an ntfy notification body (`agent-notify --category completion "<title>" "<reason>"`).
   Put the actionable ask in it (e.g. "roborev flagged a design call on #1234: needs
   owner decision on X"), not a restatement of the whole finding.
4. **`duration_s`**: worker-measured wall clock for its own claim→finalize/block/no-work
   span. This is independent of (and expected to be slightly less than) the
   supervisor's own `t1 - t0` measurement recorded in the journal line — the supervisor
   number includes process startup/teardown overhead the worker can't see.
5. **Heartbeat composition (#2089)**: each iteration should refresh the machine
   heartbeat as its own step, independent of the marker write — a stale heartbeat with
   no supervisor alert is meant to be the one unambiguous "the supervisor itself died"
   signal, so the two mechanisms must not be merged into one write.

## What the supervisor already provides (do not re-implement in the skill)

- Single-instance enforcement, budgets (`MAX_ISSUES`/`MAX_HOURS`), preflight
  (load/leftover-process/disk/stop-file), the crash-loop breaker, and all
  journaling/notification. The worker skill's only obligations are: single-issue-per-
  session, resume-own-claim-first, and the marker contract above.
- Config knobs the runbook should document as supervisor-level (not skill-level):
  `MAX_ISSUES`, `MAX_HOURS`, `LOAD_MAX`, `DISK_FLOOR_GB`, `BREAKER_N`,
  `BACKOFF_NOWORK_SECS`, `HOLD_POLL_SECS`, `MAX_ITER_SECS`, `SUPERVISOR_LOCK`,
  `STOP_FILE`, `MARKER_FILE`, `LOG_DIR`, `WORKER_CMD`, `LOAD_PROBE_CMD`,
  `DISK_PROBE_CMD`, `PROC_PROBE_CMD`, `NOTIFY_CMD`.
- Stop-file: create `.worker-stop` at the repo root (or `$STOP_FILE`) to request a
  clean stop before the next iteration; the runbook's "morning check" section should
  mention removing it before the next overnight run.
