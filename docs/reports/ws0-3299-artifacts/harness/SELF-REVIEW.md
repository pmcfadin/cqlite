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

**Fix (deferred): move `launch_workers` inside the `try`.** One line. Deferred
because extension runs were live at the time and rep.py is measurement-path code;
it will be applied when the box is quiet.

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
