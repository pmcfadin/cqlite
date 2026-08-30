# Running the gate from a lane session (issue #3473)

**Short version.** A gate launched from inside an agent session dies if that session's
**cgroup** is torn down. `nohup`, `setsid`, closing fds and being reparented to init do
**not** protect it, because cgroup membership is inherited across `fork` and cannot be
shed by detaching from the terminal, the process group or the session. (An agent merely
*finishing* does **not** tear the cgroup down — see AC2 below; the exposure is to pane and
session teardowns.) Use
`scripts/flow/gate-detached.sh`, which puts the gate in a cgroup of its own, and poll it
with `scripts/gate-liveness.sh`, which tells "reaped" from "still going".

```bash
# launch (returns immediately; the gate keeps running)
bash scripts/flow/gate-detached.sh --summary /tmp/gate-summary.txt --log /tmp/gate.log

# poll (NEVER read the gate log)
bash scripts/gate-liveness.sh /tmp/gate-summary.txt
#   COMPLETE (exit 0) — a terminal verdict is in the summary file
#   RUNNING  (exit 2) — beat within the freshness window (includes queued on the #1825 slot)
#   STALLED  (exit 3) — no liveness published for a while. NOT a claim the process is dead
#   UNKNOWN  (exit 4) — cannot tell; the printed cause names what was unmeasurable
```

---

## What was measured

#3473 reported a hard **~10 minute ceiling** on background work launched from a lane
pane, said to apply "regardless of what that work is". Measured with an instrument
rather than a gate (AC1): six tickers, each appending a timestamped line every 10 s,
each trapping every catchable signal, each self-terminating at 2400 s with an explicit
`COMPLETE … never reaped` line so that "reaped" and "still going" are distinguishable in
the artifact itself. They differed only in launch context.

| probe | launched by | detach idiom | outcome |
|---|---|---|---|
| A | lane top-level | plain `nohup … &` | ran past 2400 s, 0 signals |
| B | lane top-level | `setsid` (own sid+pgid) | ran past 2400 s, 0 signals |
| C | lane top-level | `setsid`, `cwd=/`, argv renamed | ran past 2400 s, 0 signals |
| D | lane top-level | the harness's own background-task mechanism | ran past 2400 s, 0 signals |
| E | **subagent** | plain `nohup … &` | ran past 2400 s, 0 signals |
| F | **subagent** | `setsid` | ran past 2400 s, 0 signals |

**There is no ~10 minute ceiling.** Every variant ran the full 2400 s and self-terminated
with its own `COMPLETE … never reaped` line, with no signal of any kind. The ~10 minute
figure was real as an observation but was not a property of elapsed time.

**The 600 s stall-watchdog hypothesis is UNTESTED, not falsified.** E and F were launched
by a subagent that was then given a deliberately silent 700 s foreground block, the
intention being to induce a stall and see whether the watchdog took the tickers with it.
**No stall occurred.** On this harness version an over-timeout foreground call is
**converted into a background task** and control returns to the agent immediately; the
blocker later completed on its own, exit 0, after its full 700 s, and the subagent
continued working normally throughout. So the experiment sidestepped the very condition it
was meant to create, and nothing here bears on what the watchdog does to a genuinely
stalled agent. (An earlier revision of this document claimed it did. It did not.)

That harness behaviour is itself worth recording against the original report, which noted
that *"the passive `sleep`-loop waiter was also killed"*: a long silent call being
backgrounded rather than killed means a waiter of that shape is not, on this version,
something the harness terminates.

Along the way the first datum already contradicted the framing: probe A's parent Bash
tool shell exited seconds after launch, A was reparented to init, and it kept running.
A tool call returning does not reap what it launched.

## The mechanism (AC2)

Every process in a lane — the agent process and every descendant — lives in one systemd
scope per tmux pane:

```
$ cat /proc/self/cgroup
0::/user.slice/user-1000.slice/user@1000.service/tmux-spawn-<uuid>.scope
$ systemctl --user show tmux-spawn-<uuid>.scope -p KillMode -p SendSIGKILL -p TimeoutStopUSec -p Delegate
KillMode=control-group
SendSIGKILL=yes
TimeoutStopUSec=1min 30s
Delegate=no
```

`KillMode=control-group` means that when the scope is stopped, systemd signals **every
task in the cgroup** — SIGTERM, then SIGKILL after `TimeoutStopUSec`. It does not care
what the work is, which is exactly why the issue's decisive clue (a passive `sleep` loop
dying alongside the gate) correctly eliminated CPU contention, memory pressure, disk and
the #1825 slot cap all at once. Those all *starve* work; a cgroup kill *signals* it.

**A subagent gets its own pane scope.** Probes E and F landed in a different
`tmux-spawn-*.scope` from A–D, alongside the subagent's own process.

**But a subagent ending does NOT tear its scope down — this was tested and ruled out.**
The obvious next inference was that `flow-closer`, a subagent, loses its gate when its
context ends. That is **false**, and the falsifying measurement is recorded here because it
is the most tempting wrong conclusion in this issue. Two tickers were launched from a
subagent, and the subagent was then killed outright. Its `claude` process disappeared; the
scope stayed `active`; **both tickers kept running**, orphaned. systemd releases a scope
only when the **last** process in it exits, so a long-running gate *holds its own scope
open* and survives the agent that launched it. An agent finishing is not a teardown.

So a teardown needs something that stops the scope explicitly — tmux killing the pane, a
supervisor recycling it, `systemctl stop`, session/logout teardown. Those do happen on this
fleet, which is why the detached launch below is still the right posture; but "your turn
ended" is not one of them.

### Demonstration

Detachment does not help; a separate cgroup does. Two identical tickers, one difference:

| | cgroup | detach idiom | after `systemctl --user stop` on the cage |
|---|---|---|---|
| IN | the cage's | `setsid` + `nohup` + closed fds, own sid **and** pgid, ppid 1 | **dead**, last tick 1 s before the stop |
| OUT | its own scope | none | **alive**, still ticking 11 s later |

Run twice. In the first trial IN recorded `SIGNAL TERM` at exactly the stop instant; in
the second it recorded **nothing at all** — the handler was still deferred behind its
`sleep` when the process died. Both are consistent with a cgroup-wide kill, and the
second reproduces the field symptom precisely: **a gate killed this way leaves no trace**,
which is why the summary file keeps its launch sentinel and nothing says why.

This is pinned as a standing test, not left as prose:
`scripts/tests/test_gate_detached.sh` section 3 creates a `KillMode=control-group`
cgroup, demonstrates both halves and destroys it. If a future systemd or tmux
configuration changes `KillMode`, or gives panes a delegated cgroup, the fix would be
addressing a problem that no longer has this shape — and we should be told by a red test
rather than during an incident.

### It also explains the control

The issue records that the same gate, same sha, same box, same slot cap, **completed**
when the coordination lead launched it over `ssh` + `nohup`. An ssh login gets its own
`session-N.scope` — a different cgroup, not a descendant of any pane's — so a lane pane's
teardown never reaches it. The variable was never the work, the box or the load. It was
the cgroup.

### What is established, and what is NOT — AC2 is a PARTIAL

**Established by measurement:**

1. There is **no time-based ceiling**: six launch variants ran past 2400 s with zero signals.
2. Two of those tickers were launched by a **subagent** and survived its entire lifetime.
   This does **not** clear the 600 s stall watchdog: the attempt to induce a stall failed
   (see above), so that hypothesis is **untested, not falsified**.
3. A lane pane's scope carries `KillMode=control-group` / `SendSIGKILL=yes` (read from
   `systemctl --user show` on the live scope).
4. Stopping such a cgroup **kills fully-detached work** (`setsid`+`nohup`+closed fds, own
   sid and pgid, ppid 1) and **spares identical work in its own cgroup** — demonstrated in
   both directions, twice, and in one trial the victim died leaving **no signal record at
   all**, reproducing the field symptom of a traceless kill.
5. Subagents get their own pane scope.
6. **An agent's process exiting does NOT tear down its scope**; work it launched keeps
   running, orphaned.

**NOT established:** that any of this is what killed the gates on lane-3393. Those deaths
were not observed, this host's journal retains no `tmux-spawn` scope-stop records, and (6)
rules out the most natural trigger. So AC2 is answered as *"here is a mechanism that
demonstrably produces exactly this symptom, and here are the alternatives ruled out"* —
**not** as *"this was the reaper"*. Calling it identified would be dressing an inference as
a measurement, which is the failure mode this issue was itself filed against.

What remains unexplained is the correlation with ~10 minutes, since nothing measured here
is time-based. Candidates not yet tested: a pane or supervisor recycle on a cadence, and a
harness-level kill of a *tracked* background task (distinct from the process-level probes
used here, all of which survived).

**The next occurrence is now diagnosable rather than arguable**, which is the durable part:
the heartbeat below records liveness at the moment it stops, so a reaped gate is
distinguishable from a slow one without a human on the box, and `journalctl --user -u
<scope>` can be read against the beat's last timestamp.

## The fix (AC3)

`scripts/flow/gate-detached.sh` starts the gate as a transient systemd unit under
`app.slice`, i.e. in a cgroup parented by the user manager rather than by the session.
The lane can then exit, crash, be recycled or have its pane killed, and the gate runs to
its verdict.

Two things about it that are easy to get wrong:

- **A transient unit does not inherit the caller's environment.** It starts from the
  systemd user manager's. Silently dropping the caller's environment would hand the gate
  a different `PATH` (no cargo/rustup), no `CQLITE_DATASETS_ROOT` and no sccache wiring,
  and the resulting failures would look like a real red on the branch. So the launcher
  forwards **every** exported variable minus a small deny-list, rather than an allowlist
  of the ones someone remembered — an allowlist fails silently and asymmetrically when a
  new gate-relevant variable appears. Variables it cannot forward (a non-identifier name,
  a value containing a newline) are **reported by name**, never dropped quietly, and the
  block states how many were forwarded.
- **It refuses rather than falling back.** On a host with no `systemd-run`, or where
  `systemd-run --user` does not work, the launcher exits 69 and says the gate would die
  with the session. A caller who asked for a detached gate and silently got a
  session-scoped one would believe it was protected when it was not.

It does **not** bypass the #1825 slot cap — a detached gate still queues, and a queued
gate correctly reads `RUNNING`.

## The launcher's threat model, declared

`gate-detached.sh` hardens against **accident and drift** and against exposure that needs no
write access anywhere. It does **not** try to defend a caller-supplied path against a hostile
local user, and that boundary is deliberate.

CLAUDE.md's existing triage rule for the roborev wrapper applies unchanged: *"the INVOKER can
bypass this ⇒ out of model — record it, do not patch it; a NON-INVOKER can bypass this, or it can
be bypassed BY ACCIDENT ⇒ defect. Same-host actors able to write these scripts are
invoker-class."* On this fleet lanes run as one user on dedicated boxes, so anyone who can plant
a symlink in a lane's directory can equally edit this script, shadow `systemd-run` on `PATH`, or
run their own gate.

| class | verdict | why |
|---|---|---|
| an attacker who can **write** the directory of a caller-supplied path | **out of model** | invoker-class on this fleet; the default paths live in a 0700 `mktemp -d` and are unguessable |
| exposure needing **no** write access — `/proc/<pid>/cmdline` is world-readable | **defect, fixed** | forwarding the environment via `--setenv` handed every token to any user on the box |
| **accident and drift** — stale artifacts, leftover beats, colliding temp names, two gates on one path | **defect, fixed** | the larger category in practice, and the source of most real findings here |

The cheap hardening already in place (`mktemp` everywhere, symlink and non-regular-file refusal,
non-destructive probes) **stays** — the same ruling says cheap hardening is worth keeping even
where an invoker could reach the same end another way. What this section licenses is declining to
*add* more: a further "a local user could plant X" finding should be recorded against this model
rather than fixed. Written down because the review rounds showed that list does not close by
itself — 25 findings over seven rounds, launcher-side rising 2 → 2 → 3 while the reader settled
at one per round.

## Telling reaped from running (AC4)

`RESULT: INCOMPLETE (gate did not finish)` is written into the summary file **once**, at
launch, before the #1825 slot is even granted (#3041). It is therefore the artifact of
three states at once — queued, running, killed — and the correct completion probe
(`grep -qE 'RESULT: (PASS|FAIL)'`) reports "not finished" for all three. Resolving them
needed a human running `ps` on the box, which is what made one actor the fleet's only
gate-runner.

A one-shot placeholder cannot express liveness: nothing about it decays. So the gate now
also publishes a signal that does. `scripts/lib/gate-heartbeat.sh` rewrites
`<summary-file>.heartbeat` every 20 s for as long as the gate process lives, and
`scripts/gate-liveness.sh` reads the two artifacts together.

- The path is a fixed suffix on the summary path the caller chose **in advance**, so the
  heartbeat is as discoverable as the summary itself (#1175). The startup sentinel now
  names it, because the sentinel is exactly what a lane finds when its gate was reaped.
- Every beat carries `run-id:`, and the reader refuses to answer about a run-id other
  than the one it was asked for — the #2874 reader contract, which holds for a `PASS`
  block just as much as for a beat. **Pass `--run-id` whenever you know it.**
- **Process identity is tiered and portable, and "cannot tell" is a THIRD state.** The beater
  verifies the gate pid before every beat, and the gate applies the same check in the other
  direction to its view of the beater. Identity is `/proc/<pid>/stat` field 22 where available,
  else `ps -o lstart=` — which exists on macOS, is stable, and is empty for a dead pid; its
  one-second granularity is immaterial for detecting pid *reuse*, which requires cycling the whole
  pid space. The beat declares which tier it used (`starttime` / `lstart` / `kill0`), and a
  `kill0`-only beat — no identity at all — cannot earn **any** `RUNNING` verdict, not merely the
  epoch-based one: counter progression would prove the *beater* is alive, not its *gate*, so after
  a pid recycle it could be beating happily for a stranger. There is no evidence in the artifact
  that rescues a `RUNNING` claim there, so the honest verdict is `UNKNOWN`. With the tiered
  identity in place `kill0` needs a host with neither `/proc` nor a working `ps -o lstart=`, so
  this costs almost nothing. The *comparison*
  covers every tier that has an identity: an earlier revision added the `lstart` tier, labelled it
  in the beat, and then never compared it — it fell through to a bare `kill -0`, so a recycled pid
  satisfied it while the beat still advertised `parent-check: lstart` and a reader trusted it.
  Adding a tier without wiring its comparison buys only the appearance of a guarantee.

  The gate's check is **three-valued** (`ours` / `gone` / `unverifiable`) because its two callers
  want *opposite* defaults for "cannot tell": respawning on it duplicates the beater at every
  component boundary, while signalling on it can SIGTERM an unrelated process. A two-valued
  predicate must be wrong about one of them — and it was: an earlier revision with no portable
  identity made every check fail on macOS, so a full gate would have accumulated ~30 concurrent
  beaters that nothing could stop. Respawn happens only on `gone`; signalling only on `ours`. That check is **local by
  construction** — the beater always runs on the gate's own host — which is why it survived
  the descope below. A beater that kept beating after its gate died would report a dead gate
  as `RUNNING` forever: this issue's own defect, one level down.
- The staleness window is `3 × interval` with a 90 s floor, read from the beat's **own**
  `interval:` line, so the reader holds no duplicate of the gate's beat period and cannot
  drift from it. There is deliberately **no env var** that widens the window or disables
  the beat — that hatch could only buy a vacuous `RUNNING` for a dead gate.
- **A timestamp is only trusted inside a proven shared clock domain; otherwise both answers
  come from counter progression.** `beat-epoch` is the writer's self-reported time, and nothing
  guarantees the writer's clock matches the reader's. Both directions bite: a writer running
  *behind* makes every fresh beat look `STALLED` (and since the response to a persistent
  `STALLED` is "relaunch", that risks a **duplicate gate launch**), while a dead beat from a
  writer that ran *ahead* later falls inside the freshness window and would look `RUNNING`
  forever. So the beat names its `host:`; when that is this host the timestamps are
  commensurable and a fresh beat returns `RUNNING` immediately. Otherwise the reader waits one
  interval (bounded, capped at 65 s) and checks whether `beat-seq` advanced — only the reader's
  clock times the wait, only the writer's counter shows progress, and the two are never
  compared. The same progression check settles a stale-looking beat in the shared-clock case.
  **A changed `beater-pid` counts as progress too:** every replacement beater restarts its counter
  at 1, and the gate respawns the beater at component boundaries — so a restart inside the window
  produces a *lower* second sequence, and requiring "strictly greater" alone would report a live
  gate as `STALLED`. A new incarnation under the *same* run-id is itself affirmative evidence,
  because the only thing that starts a new beater for a run is that run's own live gate.
  *Residual:* two boxes sharing a hostname and a filesystem would be treated as one clock
  domain; the consequence is a possibly-wrong `RUNNING`/`STALLED`, never a claim that a process
  is dead.

### The verdict set, and the death claim that was descoped

| observation | verdict | exit |
|---|---|---|
| terminal `RESULT:` in a complete, run-id-matching block | `COMPLETE` | 0 |
| beat present, run-id matches, **fresh** | `RUNNING` | 2 |
| beat present, run-id matches, **stale** | `STALLED` | 3 |
| anything unmeasurable, with a named cause | `UNKNOWN` | 4 |

**`STALLED` is not "the gate is dead".** It says exactly what two local files can establish:
*this run has published no liveness for N seconds.*

It started life as `REAPED` — a positive claim that the process was gone — and **four review
rounds each found another way that claim was unsound**:

1. a stale beat alone does not imply death: the beater is supervised only at component
   boundaries, and components run for minutes, so a beater can die under a perfectly live gate;
2. corroborating against the **reader's** `/proc` does not prove anything about the **gate's**
   host — across shared storage a live remote gate reads as dead;
3. matching **hostnames** do not prove machine identity either: two boxes can share a
   hostname, so a differing `boot-id` was misread as a reboot;
4. and the machinery itself was unportable — there is no `/proc` boot id on the macOS/BSD gate
   hosts, so the death cases failed there deterministically.

Each fix was correct about the case in front of it and the list did not close, because proving
a process is dead means **proving a negative about a machine you may not be on**. So the claim
was removed rather than defended a fifth time. What remains needs no pid, no `/proc`, no host
identity and no boot identity — and is therefore correct on every host.

**Nothing actionable is lost.** The lane's real question is *"should I keep waiting?"*, and
`STALLED` answers it. The rule that replaces "definitely dead, re-run now" needs no process
inspection: the gate relaunches its beater at every component boundary, so a live gate whose
beater alone died **recovers to `RUNNING` within one component**. Re-read before acting; if it
is still `STALLED` after a component's worth of time (~850 s at the longest), treat the gate as
gone and relaunch. The verdict text says all of this, so a reader acting on it needs no
memory of this document.

`STALLED` requires **two valid samples of the same run** showing no progress. If the confirmation
sample cannot be copied, holds NUL bytes, fails validation, or belongs to another run, the verdict is
`UNKNOWN` — and its text says explicitly that it is *not* a stall, so nobody re-runs a gate on an
unmeasurable read. An earlier revision left the "no progress" flag unset in all of those cases and
reported `STALLED`, which collapsed *"I could not measure"* into *"I measured no progress"* — the one
thing this reader's own contract forbids, in the function that contract describes.

The launcher owns **no** copy of this grammar either. It checks the nonce (which the reader has no
notion of) and then asks `gate-liveness.sh --run-id`, accepting only `COMPLETE` or `RUNNING`. Grepping
the beat itself had let it accept beats the reader rejects — a `parent-check: kill0` beat, or one with
invalid framing — returning success while every advertised poll answered `UNKNOWN`.

`RUNNING` and `STALLED` both remain **affirmative measurements** — each requires a beat that is
present, run-id-matching, and respectively fresh or stale. A **missing** heartbeat is `UNKNOWN`,
never `STALLED`: a gate predating this mechanism, or one whose summary path is unwritable,
produces the same absence, and reporting those as a stall would be the fail-open shape one
level down.

The beat is validated like the summary: exactly one ordered opener and closer, and **every field
that decides a verdict** present exactly once and inside the framing. The line between *required*
and *optional-but-unique* is whether absence makes a verdict **unsound** or merely **narrower** —
`run-id`, `beat-seq`, `beat-epoch`, `interval` and `parent-check` are required; `host` and
`beater-pid` are optional, because their absence degrades safely (no host means the clock domain is
unproven so progression decides; no beater-pid forfeits restart detection). A *duplicate* of any of
them is fatal either way, since the first occurrence would be trusted. `parent-check` comes from a
**closed** set — an unrecognised value
is `UNKNOWN`, never assumed benign. And a beat declaring an `interval` above 60 s is `UNKNOWN`
rather than `STALLED`: the confirmation window is capped at 65 s to bound a hostile artifact, so
such a beat might legitimately not advance inside it, and guessing `STALLED` would send a lane to
re-run a healthy gate.

**The launcher and the reader agree about startup.** `gate-detached.sh` accepts a gate on the
strength of its *beat* — the beater starts before the tree capture — and then prints a run-bound poll
command. So when `--run-id` names a run and a valid matching beat exists, the reader answers
`RUNNING` even if the summary is **absent** or still belongs to the **previous** run. Without that,
the launcher's own advertised command reported `UNKNOWN` for a healthy, accepted, actively-beating
gate for the whole duration of the capture — the two halves disagreeing about what "accepted" means,
which is worse than either being wrong alone. The rescue applies only to a *named* run with a beat that is valid, matching **and fresh in a proven
shared clock domain**. Freshness is not optional there: without it, a gate that died after its first
beat but before writing its summary would report `RUNNING` forever from that one stale beat — a false
`RUNNING` makes the caller wait indefinitely on a gate that is gone. A stale startup beat simply does
not take the shortcut, and the normal summary handling answers `UNKNOWN`. Unnamed callers have nothing
to match against, so for them the summary remains the only anchor.

A **terminal** verdict is reconciled with the beat before it is believed. During startup a new run
publishes its beat *before* replacing the previous run's summary — a window the early beater start
deliberately widened — so an unbound reader could otherwise report the **previous** run's `PASS` as
the completion of the run starting right now. A beat naming a different run makes the summary
`summary-superseded`; passing `--run-id` says which run you mean and is answered directly.

Numeric fields are length-bounded and normalised **base 10** before any arithmetic. A digit string
is not yet a number: bash reads a leading zero as octal, so `interval: 08` was a syntax error that
*aborted the reader* rather than returning its documented `UNKNOWN`.

### What the reader does NOT guarantee

The summary is **not** published atomically: `agent-gate.sh` writes it in place with `>`, so a
reader can observe a **prefix** of a block being written — and, contrary to what an earlier
revision of this document claimed, a **blend of two writes** as well. Two writers hold
independent file offsets, so if B truncates while A is mid-block the file becomes B's opener, a
sparse hole, then A's tail; a reader could pair one run's `run-id:` with another run's `RESULT:`
and end marker. That is a false `COMPLETE`, the worst verdict this script can give. It was
verified by performing the interleaving, not reasoned about.

Five things keep it out: the mandatory **end-marker** check rejects a truncated prefix; a
**NUL byte** anywhere is rejected, which is the fingerprint of the sparse hole a blend leaves;
**more than one** opener / `run-id:` / `RESULT:` / closer is rejected before any field is read;
the closer must **match the opener's dialect** (a LITE opener closed by a DELTA marker is two
fragments, and the three dialects are kept distinct precisely so no block can pass as another);
and the elements must be **ordered** — opener, then `run-id:` and `RESULT:`, then closer.

A valid opener, a matching dialect, correct ordering **and exactly one `run-id:`** are required on
**every** path. The run-id requirement is unconditional, and getting there corrected an
overstatement in this document's own audit: `COMPLETE` was described as "run-id bound", which was
true only when the caller passed `--run-id`. Without it, a block carrying **no** `run-id:` at all
was accepted and its verdict reported — a verdict attributable to no run, from a file every real
gate stamps with one. Absence was reading as "nothing to disagree with" instead of as missing
evidence, which is the permissive-branch shape the rest of this file is built to refuse. Only the
**closer** is specific to believing a terminal verdict — and that is the precise shape of the
legitimate exception, because a mid-write read is missing its *tail*. So a truncated
`INCOMPLETE` block still falls through to the heartbeat (the conservative direction, and the
common case), while a mismatched dialect or an out-of-order field is refused everywhere.

That distinction is not cosmetic. When the caller omits `--run-id`, the reader takes the run-id
**from the summary** and uses it to decide whether the heartbeat is ours — so an interleaved
summary could hand over a *foreign fragment's* run-id and the reader would validate a peer's
beat and report `RUNNING` about somebody else's gate. Enforcing the framing before any field is
trusted is what closes that. The reader also re-reads **once** when framing is incomplete, resolving the common
"caught mid-write" case; a permanently truncated artifact still reports `UNKNOWN`.

**Residual, stated rather than papered over:** a blend that lands on no hole *and* produces a
structurally well-formed single block is indistinguishable from a genuine one by any reader of
the file alone. Nothing here closes that. What closes it is the single-writer discipline #2874
already mandates — concurrent gates in one checkout MUST use distinct summary paths, and the
gate de-exports its summary path so no child can inherit it — plus making the write atomic at
the source.

Making that write atomic (sibling temp + rename) is the root fix and was deliberately left out
of #3473: `emit_summary` is load-bearing for #1175's write-failure detection and #2874's
no-clobber contract, so changing how the **gate of record** publishes its verdict deserves its
own issue rather than a ride-along.

### The launcher refuses an unmonitorable gate — verified by outcome, not by permission model

A bad summary location would start a gate that publishes neither verdict nor liveness: 30–50
minutes burned certifying nothing, with every poll answering `UNKNOWN` and no way to tell that
from a slow queue. `gate-detached.sh` prevents that in two layers.

**Cheap pre-checks, for better messages:** the summary directory must exist and support creating
and renaming a file; neither the summary nor the heartbeat destination may be a symlink,
directory, fifo or device; and the **log may not alias** the summary or the heartbeat. That last
one has two silent failure modes — the gate rewrites its summary with `>`, which would truncate
an accumulated log, and the beater publishes by rename, which would unlink the log's open inode
so the advertised log ends up holding heartbeat data. Aliasing is checked by name *and* by
device+inode (`-ef`), because two different spellings can be the same file. These catch obvious misconfiguration before anything starts.

**The beater starts before anything expensive.** It used to start after the tree-identity capture
and the sentinel, which meant the first seconds of every gate published no liveness at all — the
blind spot the heartbeat exists to close, open at the moment a reader is most likely to look. It
also made the launcher's monitorability check racy: a slow capture (~150 ms on a 6114-file checkout,
but unbounded in principle) could have made the launcher stop a *healthy* gate. The sentinel stays
after the capture, because it carries `tree-start:` and cannot precede what computes it.

**Ownership is proved from whichever artifact carries the nonce FIRST — the heartbeat, normally.**
The beat appears in ~0.4 s (before the tree capture), while the summary is written after it, so
requiring the *summary* to prove ownership would have reintroduced the very defect that moving the
beater fixed: a slow capture stopping a healthy, actively-beating gate. The summary remains a second
source, because a very short run can reach its verdict before any beat exists.

**The real guarantee is post-launch, and it is bound to a LAUNCHER NONCE:** the launcher generates
an opaque token, forwards it to the gate, and requires it in *both* artifacts before trusting them.
Binding to "the first run-id that differs from the pre-launch value" was not enough — a concurrent
gate on the same summary path can publish first, and the launcher would then report success and
print a poll command bound to the **peer's** run. A run-id it cannot predict is no basis for the
claim; a token it generates is. Additionally: the gate starts its
beater *before* it queues for the #1825 slot, so a first beat lands within a second or two even
when the gate will then sit in the queue for 20 minutes. The launcher snapshots the run-ids
already present, then requires the summary to publish a *different* run-id **and** the heartbeat
to carry *that* run-id — so a stale or foreign beat already sitting at the path cannot stand in
for a real one (which it could, in the first version of this check: exactly the
sticky-directory case the check exists to catch). The same binding applies to the
early-terminal-verdict shortcut. If no such beat arrives the launcher **stops the unit** and
refuses. That is an end-to-end proof covering every reason publication could fail —
ownership, sticky directories, ACLs, mount flags, SELinux, a full filesystem — without this
script modelling any of them. A gate that already reached a terminal verdict is accepted
without a beat, since there is nothing left to monitor.

Permission *modelling* was tried and removed, because it was wrong in **both** directions: a
zero-byte append proves write access to a file but not permission to **replace** it, so it
passed a sticky directory whose heartbeat is owned by someone else (where the beater fails
forever) — and it *refused* a mode-400 heartbeat that works perfectly well, since POSIX takes
rename permission from the **directory**, not the file. Both are pinned as tests.

**The summary path is reserved for the gate's lifetime.** The nonce proves ownership of the
artifacts the launcher *reads*; it does nothing to stop two launchers pointing at one summary path,
where each would prove ownership of its own artifacts while their heartbeat renames and summary
rewrites destroyed each other. A **symlink** beside the summary reserves the path, and a second
launcher is refused while its owner is alive. It is deliberately **self-healing rather than released**:
the gate outlives its launcher, so no process could reliably remove the lock, and a lock nobody can
release is worse than no lock.

The mechanism is `ln -s`, and the choice is the whole design. Creating a symlink **fails if the path
exists** — that is the mutual exclusion — and its target is **arbitrary text**, so the owner record
*is* the lock: `unit=<unit>|pid=<launcher>|start=<identity>` is published by the very syscall that
acquires it. Liveness counts the **launcher pid**, not just the unit, which closes the window between
reserving the path and the unit becoming active — during it the launcher is by definition alive. That
pid is **pinned by a start identity**, because a short-lived pid can be reused and would otherwise
make a finished gate's reservation look live forever. Reclamation of a provably-dead owner is claimed
by an atomic **rename** into an `mktemp` scratch directory, so two reclaimers cannot delete each
other's replacement locks: only one `mv` can succeed and the loser refuses rather than racing.

**A two-operation lock, and its deadline, were both tried and removed — this is the substantive
finding of the round.** `mkdir` is also atomic, but a directory cannot carry its owner atomically, so
acquisition was `mkdir` *then* write an `owner` file, leaving a window in which the lock existed and
its owner was unknown. Both readings of that window were wrong, in opposite directions. Refusing it
unconditionally meant a launcher killed mid-acquisition left a lock that could **never** self-heal,
permanently refusing every later launch on that path. Adding a grace period to fix that introduced a
worse failure: a launcher merely **paused** — SIGSTOP, or just descheduled under the heavy contention
these boxes run — could have its **live** lock reclaimed, after which two gates launch on one summary
path, which is the exact outcome the lock exists to prevent. No value of the deadline is safe, because
the deadline is trying to distinguish *slow* from *dead* using elapsed time, and elapsed time cannot
tell them apart.

So the deadline was not tuned; the window it policed was **eliminated**. With ownership published
atomically there is no incomplete state, no age probe (and no GNU-vs-BSD `stat` spelling to try), and
no timer anywhere in the reservation path. Reclamation rests on **affirmative proof** the owner is
gone — a dead pid whose start identity matches, or an inactive unit — and an owner that cannot be
read or parsed is **refused**, since unreadability is not evidence of death. Seven test cases were
deleted along with the states they covered, which is the honest measure of the simplification.
(#2874 already forbids two gates on one path; the launcher now
detects it rather than walking into it.)

Every probe is **non-destructive**, because under #2874 these paths may hold a live peer's
artifacts — including the **log**, which is probed with a zero-byte append (or created and removed
if absent) and only truncated immediately before launch, so a refusal can never destroy a previous
log for a launch that never happened: the directory is probed with `mktemp`-created siblings, and no existing summary or
heartbeat is written, truncated or replaced by any check. A caller-supplied **log** path is
refused if it is a symlink or non-regular file, since the log is truncated with `>` (residual: it
is a check-then-create, so a symlink planted in the microsecond window is not caught — the
default log path is unguessable inside a 0700 mkdtemp, so this only concerns a caller-chosen
path in a shared directory).

**A CHECK'S ANSWER DESCRIBES THE TREE IT RAN ON, AND TWO FINDINGS HERE WERE THAT SHAPE.** The log
path is refused if it is a symlink, because the log is truncated with `>` and `>` follows links. That
check ran early — and this script CREATES a symlink at the reservation path later, so
`--log <summary>.launch-lock` passed the symlink refusal (the launch-lock did not exist yet), the
reservation link was then created at exactly that path, and the pre-launch truncate followed it,
writing the gate's log into a file named after the link's own owner text. The instance is fixed by
putting the reservation path in the alias set; the CLASS is fixed by re-checking at the **point of
use**, immediately before the truncate, because any symlink appearing in that gap — ours or a
concurrent peer's — defeats a check made only at the start. Refusing there removes the reservation it
owns rather than leaking it.

**AND `kill -0` IS NOT A LIVENESS TEST FOR A PROCESS THAT MIGHT BE A ZOMBIE.** A zombie's pid entry
survives until its parent reaps it, so `kill -0` succeeds on a process that has already exited and
can never start a unit. Its reservation could therefore never self-heal — the same permanent-block
failure the incomplete-owner window caused, resurfacing at a different site, which is the argument
for stating the *shape* and not just fixing instances. Zombie state is read affirmatively, tiered
`/proc/<pid>/stat` (parsed after the LAST `)`, since `comm` may contain spaces and parens) then
`ps -o state=`; an **unmeasurable** state reads as NOT-a-zombie, keeping the caller refusing, because
"I could not tell" may never license reclaiming a lock that may be live. The test asserts the
**premise** too — that `kill -0` really does report a live-looking zombie — so it cannot pass for the
wrong reason once the platform changes.

### The launcher owns no second copy of the verdict grammar

The launcher needs to know whether a gate already reached a terminal verdict (a preflight
refusal or a very short `--only` run finishes before any beat). It asks `gate-liveness.sh
--run-id <this run>` and accepts only exit 0. Its own earlier version grepped
`^RESULT: (PASS|FAIL|…)` with no end anchor and no framing validation, so `RESULT: PASSENGER` or
a truncated block made the **launcher** report success while the reader would answer `UNKNOWN` —
the prefix-matching defect from the first review round, reproduced in a second implementation of
the same grammar. One implementation, one grammar; the run-binding comes along for free.

The command the launcher advertises for polling is shell-escaped and carries `--run-id`, since
the launcher knows it and this document tells everyone else to pass it whenever they do.

### The forwarded environment never rides in `argv`, and does not outlive the launch

A transient unit inherits none of the caller's environment, so it has to be carried across — but
`systemd-run --setenv=NAME=VALUE` puts every value on a command line, and `/proc/<pid>/cmdline`
is **world-readable** while `/proc/<pid>/environ` is owner-only. This fleet's environment
routinely holds `GH_TOKEN`, `PROJECTS_TOKEN` and `PARITY_HEAL_TOKEN`, so that is a real
downgrade. The launcher instead writes a mode-**0600** wrapper script inside its private
directory, quoted with `printf %q` (shell-exact, so there are no systemd quoting semantics to get
wrong — an `EnvironmentFile` approach was measured returning *empty* values and abandoned), and
only the script path appears in `argv`. Verified end to end: a probe variable reaches the unit's
environment and appears in no process command line.

That file **unlinks itself** from inside the generated wrapper, immediately before `exec` — the
launcher's `EXIT` trap remains as a fallback for paths where the wrapper never runs, but a trap
cannot fire if the launcher is SIGKILLed after the unit started, and then the secrets file would
survive indefinitely. Tying its lifetime to the process that consumed it closes that.
The first version never deleted it, so each launch left a persistent copy of the session's
credentials in an undisclosed directory — 51 had accumulated in `/tmp` during development of this
change. The private directory is removed with `rmdir`, which succeeds only when empty, so a
default-path launch keeps the summary and log the caller still needs.

Every SUMMARY block now carries a `heartbeat:` line, so a pasted block shows the
mechanism ran (same reason #3148 stamps a positive `schemas:` line).

Self-tests: `scripts/tests/test_gate_liveness.sh` (189 cases) and
`scripts/tests/test_gate_detached.sh` (130 cases), both in the full gate's
`tooling-tests` component.

## Doctrine

- A lane **may** run its own full gate, via `gate-detached.sh`. The claim "lanes cannot
  run a full gate" was true of the naive launch and is not true of the detached one.
- **`flow-closer` runs its gate detached** — not because its own completion kills the gate
  (measured: it does not), but because a detached gate is independent of *every* pane and
  session teardown, which is a class of failure the closer cannot see coming and cannot
  distinguish from a slow gate. It costs one wrapper call.
- Never conclude "my gate is still running" from `RESULT: INCOMPLETE` alone. Ask
  `gate-liveness.sh`. A `STALLED` verdict means stop waiting open-endedly: re-read once, and
  relaunch if it persists past a long component (~850 s). It is not proof of death.
- On a host where `gate-detached.sh` refuses (no working user systemd manager), the gate
  of record must be launched from a separate login (`ssh` + `nohup`), which gets its own
  scope. Do not launch it in-session and hope.
