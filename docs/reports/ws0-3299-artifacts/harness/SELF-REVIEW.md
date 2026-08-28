# Hostile self-review of the #3299 harness

Read as a reviewer would, on the delivery lead's three specific questions plus
what I found myself. Findings are recorded with a **fix-now / fix-after**
disposition, and the reason for deferral is stated — a deferred fix with no
reason is just an unfixed defect.

**Deferral rule used throughout: no measurement-path code changes mid-campaign.**
The same discipline that forbids changing the perf event set between reps applies
to the orchestration around them. Where a fix is deferred it is because a
measurement run was live, never because it is hard.

---

## Q1 — does any teardown kill by NAME? **No. Both traps are absent.**

The two failure modes are real and worth stating, because both *report success
having killed nothing*:

- **`pkill -x ws0-3299-scan-worker` can never match.** The kernel's `comm` is 15
  characters; the name is longer, so the exact-match form has no possible
  subject. `pkill` even warns about it, and the exit status still looks fine.
- **`pkill -f <pattern>` matches the killer's own wrapper shell**, whose command
  line contains the pattern. It kills the caller and leaves the targets.

**This harness uses neither.** The only kill is `rep.py:281 p.kill()` on a
`subprocess.Popen` object — a **PID** obtained by having spawned the process, so
it cannot mismatch a name it never consults. `sweep.sh` contains no kill at all.
Verified by grep over both files: no `pkill`, no `killall`, no `kill -x`.

The 34 orphans observed on this box came from **signalling `sweep.sh`/`rep.py`
from outside**, which is Q3, not from the harness's own teardown.

## Q2 — can the ACK loop consume two coalesced ACKs? **No.**

`command()` writes one control word and then blocks until it reads a non-empty
ack. Three properties close it:

1. **Only one command is ever outstanding.** Every `command()` call writes *then*
   reads before returning, and the two call sites (`enable`, `disable`) are
   sequential. So at most one ack can be in flight, and there is never a second
   to coalesce with.
2. **The read is larger than the ack.** `os.read(ack_fd, 64)` takes the whole
   pending payload in one call, so a multi-byte ack cannot be split across two
   reads and leave a remainder to satisfy the *next* command spuriously.
3. **FIFO writes under `PIPE_BUF` (4096) are atomic**, so a partial ack cannot
   appear in the pipe in the first place.

The dangerous shape here would be a *stale* ack satisfying a later command, not
two acks satisfying one — and (1) rules it out. **No change made**, and
deliberately no defensive `sleep`: a sleep here would be a race with a longer
fuse.

## Q3 — do all `die()` paths leave workers behind? **One real defect. Deferred.**

- **Inside the `try`** (`rep.py:266`) — safe. `die()` raises `SystemExit`, the
  `finally` writes the `stop` barrier file, workers exit through their own
  documented path, and `p.wait(timeout=180)` reaps them.
- **`verify_clock_source()`** — safe: it runs before any worker exists.
- **⚠ `launch_workers()` is called at line 265, one line BEFORE the `try` at
  266.** If it raises partway through spawning (a bad `taskset`, an exec
  failure), the workers already started are not covered by the `finally` and are
  **orphaned**. Narrow — it needs a failure *during* spawn — but real.
- **⚠ SIGKILL of `rep.py`** skips `finally` entirely, which is what happened on
  this box.

**Both leaks are BOUNDED, by design rather than by luck**: every worker carries
its own `--max-secs` deadline (900 s default) checked on each progress sample, so
an orphan self-terminates within ~15 minutes with no supervisor. That is the
backstop that made the observed orphan episode recoverable.

### The fix, and why the OBVIOUS version of it is insufficient

Moving the call inside the `try` is **not enough on its own.**
`launch_workers()` builds a **local** `procs` list and returns it, so if it
raises mid-loop the return never happens and the caller's name is never bound.
With `procs = []` initialised before the `try`, the `finally` would then write the
`stop` barrier (so the already-spawned workers *do* exit through their own path —
the important half) but its reaping loop would iterate an **empty list**, never
reaping the children or closing their log file descriptors.

The complete fix is therefore to have the caller own the list and
`launch_workers` **append into it as each worker starts**:

```python
procs = []
try:
    launch_workers(args, rundir, worker_cpus, procs)   # appends in place
    wait_ready(procs, ...)
    ...
finally:
    # writes `stop`, then reaps whatever was actually started — including a
    # PARTIAL spawn, which the return-a-local-list shape made invisible
```

**Success-path neutrality (the licence for fixing this after the measurements).**
On a rep that does not raise, the loop runs to completion exactly as before: the
same processes with the same arguments in the same order, `procs` ending with
identical contents, and every later statement — `wait_ready`, the barrier, the
window, the reap, the return-code check — reading it identically. The change is
observable **only** on an exception between the first `Popen` and the guarded
block. No counted interval, no counter, no attribution rule and no recorded value
can differ.

**Disposition: apply once the campaign is fully done** (after phase 2 and the
frequency calibration), never mid-campaign, and record the provenance in the
report — the numbers were produced by the pre-fix harness, the harness shipped is
the post-fix one, and the delta touches only the exception path.

## Q4 (mine) — `--rep` and `--round` are the same value under two names

`sweep.sh` passes `--rep "$round" --round "$round"`; `rep.py` declares both as
separate required arguments and writes both into `window.json`. Only `round` is
ever consumed (`derive.py` groups by it); `rep` is **dead and duplicative**.

By construction they *are* identical — this grid measures each point exactly once
per round — so nothing is currently wrong. But two names for one quantity is
precisely what a later reader mistakes for two independent dimensions.

**Fix (deferred): drop `--rep` from both files in one commit** — they must change
together, since `sweep.sh` passing an argument `rep.py` no longer accepts would
abort every rep. Deferred for the same reason as Q3, plus one specific to
`sweep.sh`: **bash reads a script incrementally as it executes**, so editing a
live `sweep.sh` can corrupt the running parse. It must be edited only when no
sweep is running.

## Q6 — `phase2.sh` / `phase2-compare.py`: DELETED, not fixed

I flagged the `--port` error here (the server takes `--listen`, so no invocation
could start a server) and queued a fix. Roborev then found two more in the same
pair: perf wrapped the loadgen's **whole process lifetime** while `rows_total`
covered only the timed step — the exact windowing mismatch this issue's aligned
window exists to prevent, reintroduced in the phase-2 script — and the
comparison tool printed client-bound verdicts **without validating** that the two
runs shared a server set, a shape or a corpus.

**Disposition: deleted, not fixed.** Neither script produced a published number
— `do_get` was measured by invoking `flight-loadgen` directly — so their only
value was reproducibility, and three defects of that kind make them worse than
absent. The reproduction value is replaced by the **exact commands that were
run**, recorded verbatim in the report.

**The lesson worth keeping**: I wrote a phase-2 harness that reintroduced the
windowing sin the phase-1 harness was built to avoid. Familiarity with the
principle did not transfer to new code written under time pressure, which is why
the review layer exists and why "I already know that one" is not a defence.

## Q7 — the fail-open guard fields (roborev finding 4)

`guard_window` treated `worker_cpus`, `perf_csv`, `perf_cpus` and `task-clock` as
OPTIONAL: omitting any skipped the check it feeds and the rep still returned
success. Same shape as an LLC counter that programs cleanly and returns a hard
zero — a check reporting success having measured nothing — this time in the guard
layer itself.

**Fixed**: all four required, with `WINDOW_FIELD_MISSING`,
`WINDOW_FIELD_MALFORMED` and `WINDOW_NO_TASK_CLOCK`, each observed to fire in
`selftest.sh`. **Verified not exercised**: of the 91 committed reps carrying a
`window.json`, **0** would have taken a fail-open path. A latent hole, not one
that fired.

## Q5 (mine) — exclusive-create must not break resume

`rep.py` now refuses an existing rundir (correctly: a shared rundir lets a peer's
`stop` file end this run's window). Two things checked so the guard cannot
self-inflict a failure:

- **Round names are distinct** — `RD="$RESULTS/s${S}-n${N}-round${round}"`
  includes the round, so rounds 2 and 3 cannot collide with round 1. A 2.5 h grid
  aborting at round 2 on its own new guard would have been the expensive version
  of this bug.
- **The refusal is diagnostic, not bare** — it names the path, states the
  mechanism (the rundir is the barrier channel its workers poll) and tells the
  operator to point `--rundir` elsewhere or delete it deliberately. A re-run of a
  single failed point into an existing tree therefore fails with an actionable
  message rather than a bare `FileExistsError`.
