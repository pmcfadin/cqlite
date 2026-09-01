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

**Caveat on `logout`, added after the fact:** a detached launch survives a logout only when user
lingering is enabled, because the user manager — and therefore every unit it holds — is stopped when
the last session ends. See "A separate cgroup is not enough" below; the launcher now refuses rather
than pretend otherwise.

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
hard-link refusal, non-destructive probes) **stays** — the same ruling says cheap hardening is
worth keeping even where an invoker could reach the same end another way. What this section
licenses is declining to *add* more: a further "a local user could plant X" finding should be
recorded against this model rather than fixed.

**The census, kept current because a stale count is the defect this document twice had to fix in
itself.** Written down because the review rounds showed the list does not close by itself: 25
findings over the first seven rounds (launcher-side rising 2 → 2 → 3 while the reader settled at
one per round), then **jobs 319/320/321 added 4 + 1 + 2**. Of those seven, **one (job 320) was in
the fix job 319 had just made**, and **five separate rounds have now landed in this file's
reservation/locking subsystem alone** (183, 203, 261, 269, 321). That subsystem, not the file, is
where the variant list refuses to close.

**Declared host precondition: cgroup v2 unified (job 322).** `_unit_runs_a_gate` composes
`/sys/fs/cgroup${ControlGroup}/cgroup.procs` directly. On a cgroup-v1 host `systemd-run --user`
still works but that file lives under a controller-specific mount, so the helper answers `unknown`
and every caller refuses. **That degrades SAFE** — a contended reservation is permanently refused
rather than wrongly reclaimed, so two gates still cannot land on one summary path; the cost is that
a stale reservation cannot self-heal and the operator must use a fresh path. Declared rather than
fixed because every box on this fleet is measured `cgroup2fs` unified, and a mount-resolution guard
for a host class we cannot test on is new executable code on a path nothing here takes.

**Where the boundary now sits for output-path aliasing, stated so the next finding is triageable
rather than automatically fixed.** Destinations are checked for **symlinks** (job 169) and for
**multiple hard links** (job 321) — both cheap, both kept. Aliasing routes beyond those two —
bind mounts, `/proc/self/fd` paths, a mount namespace, a directory replaced between check and
open — are **NOT closed**, are **invoker-class** under the table above, and should be **recorded
against this model rather than fixed**. Job 321's hard-link finding was itself within what this
section licensed declining; it was fixed because it is the same spelling of an axis already
closed for symlinks and cost ~35 lines, not because the model required it. A *third* spelling of
the same axis is where that stops being true.

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
| terminal `RESULT:`, unbound read, heartbeat names a **different** run | `UNKNOWN` | 4 |
| beat present, run-id matches, **fresh** | `RUNNING` | 2 |
| beat present, run-id matches, **stale** | `STALLED` | 3 |
| anything unmeasurable, with a named cause | `UNKNOWN` | 4 |

**There is deliberately NO AGE BRANCH in that row, and the two-round detour that established it is
the most useful thing in this section.** A heartbeat outlives the run that wrote it, so an unbound
reader finding a foreign `run-id` beside a terminal verdict cannot assume the beat is newer. Job 206
was right that the old diagnostic overclaimed — it called any foreign beat "a live heartbeat … a
NEWER run is starting", asserting a liveness it never measured. The fix attempted for that added a
permissive branch: a **provably stale** foreign beat was ignored as an older leftover, reporting the
summary's verdict.

Job 208 (High) showed that is unsound, because **staleness establishes no ordering**. These two
shapes are indistinguishable:

| | summary | heartbeat | truth |
|---|---|---|---|
| a run that refused before its first beat | `B` terminal | `A` stale | beat is OLDER |
| a run that beat, then **died** | `A` terminal | `B` stale | beat is NEWER |

In the second, ignoring the beat reports A's old `PASS` as the current run's outcome — a false
`COMPLETE`, certifying a gate that never finished. "Stale" was being read as "predates the summary",
which is this file's own rule broken in its own favour: a positive verdict derived from a reading
that does not support it. So every differing unbound `run-id` gets one verdict, and `--run-id` is how
you ask a question that HAS an answer. An ordering field could rescue the permissive branch, but that
is a new artifact contract, not a bug fix.

**Worth noting where this claim lived:** the fresh/stale split was documented in this table for about
twenty minutes before being removed. Three of this change's false claims landed in PROSE rather than
code — code is reviewed against reality, prose against plausibility — which is why each row above was
re-verified by running the reader against a constructed artifact rather than by reading the source.

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
is still `STALLED` after **longer than the longest component of your own run**, treat the gate as
gone and relaunch. The verdict text says all of this, so a reader acting on it needs no
memory of this document.

**Do not take that bound from a number written here.** This document said "~850 s at the longest"
in four places, and gate of record #4 on this very branch measured **`tooling-tests` at 2073 s** —
2.4x the stated ceiling. Acting on the understated figure is not a small error: it tells a closer
its live gate is gone less than half way through the longest component, so the closer relaunches
and **two gates write one summary path**, which is the outcome this whole issue exists to prevent.
The figure decayed because `tooling-tests` executes ~69 nested scripts and things keep being added
to it — including, in this very change, cases in the two suites it runs. So **derive the bound from
the component table in your own SUMMARY block** (`<name>: PASS (<n>s)`), where it is a measurement
rather than a remembered constant, and treat any number in prose here as illustrative only.

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
launcher is refused while its owner is alive; contended reclamation is serialised by `flock`. It is deliberately **self-healing rather than released**:
the gate outlives its launcher, so no process could reliably remove the lock, and a lock nobody can
release is worse than no lock.

The mechanism is `ln -s`, and the choice is the whole design. Creating a symlink **fails if the path
exists** — that is the mutual exclusion — and its target is **arbitrary text**, so the owner record
*is* the lock: `unit=<unit>|pid=<launcher>|start=<identity>` is published by the very syscall that
acquires it. Liveness counts the **launcher pid**, not just the unit, which closes the window between
reserving the path and the unit becoming active — during it the launcher is by definition alive. That
pid is **pinned by a start identity**, because a short-lived pid can be reused and would otherwise
make a finished gate's reservation look live forever. Reclamation of a provably-dead owner is serialised by
**`flock`**, with the owner re-read INSIDE the mutex.

**THE PREVIOUS SENTENCE HERE WAS FALSE, AND IT IS WORTH RECORDING WHY IT SURVIVED SO LONG.** It read:
*"reclamation is claimed by an atomic rename into an mktemp scratch directory, so two reclaimers
cannot delete each other's replacement locks: only one `mv` can succeed and the loser refuses rather
than racing."* **`mv` is not a compare-and-swap.** It moves whatever occupies the path and compares
nothing against an expected value; `rename()` offers no such semantics. Two launchers that both
classified the old owner as dead could therefore both succeed — the first replaced the link and
launched, and the second's *delayed* `mv` moved the first's **LIVE** reservation away and installed
its own, putting two gates on one summary path. That is the exact outcome this lock exists to
prevent, and the claim to the contrary was asserted in a code comment, a commit message, a test's
NAME, and this document. The interleaving is now demonstrated rather than argued (roborev job 203),
which is what should have happened when the claim was first made: **"atomic" describes the operation,
not the transaction**, and a sequence of individually-atomic steps is not itself atomic.

`flock` is used rather than a `mkdir` mutex because the kernel releases it when the fd closes, so a
reclaimer that dies mid-sequence leaves **nothing to time out** — reintroducing a stale-lock window
here would have undone the very simplification above. The classification is re-read inside the mutex,
because anything learned before acquiring it describes a tree that may already have changed. Two
smaller traps found in the same place: `exec` with no command applies its redirections to the
**current shell permanently**, so an `exec 9>… 2>/dev/null` silences every later refusal (the
launcher then exits non-zero in total silence — a test asserting only the exit code cannot see it);
and the mutex fd is opened for **append**, since another launcher may hold a lock on that inode.

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

### A SEPARATE CGROUP IS NOT ENOUGH: LINGERING IS A SECOND PRECONDITION

**And this corrects a claim made earlier in this document.** The text above lists "session/logout
teardown" among the things the detached launch protects against. **It does not, unless user lingering
is enabled** — and on the box where all of this was measured, `loginctl show-user … -p Linger`
returned **`no`**.

The mechanism is separate from the cgroup one and equally decisive. Escaping the pane's scope puts
the gate under `user@<uid>.service/app.slice`, which survives the pane. But **the user manager itself
is stopped when the user's last session ends** unless lingering is on — systemd's own documentation
is the authority: lingering is what keeps a user manager *"around after logouts"* — and stopping
`user@<uid>.service` stops the units it manages, including the gate's transient unit. A successful
`systemd-run --user` proves the manager is running **now**; it says nothing about whether it survives
a logout. `KillUserProcesses=no` does **not** substitute: that governs whether a *session's*
processes are killed at session end, not whether the user manager and its units are stopped.

**The AC1 measurements do not contradict this, and it is important to say why rather than let the
2400 s figures imply more than they showed.** Those six variants ran in a session that never reached
zero — this box carries sessions in `closing` state with the manager alive throughout — so **no
trial ever faced a real logout**. The measurements establish what they establish: nothing kills a
detached process at ~10 minutes, and a scope outlives the agent that created it. They never tested
the logout path, so they never validated the property the prose above claimed for them. Asserting a
mechanism's behaviour without exercising the case that matters is the same error as the
compare-and-swap claim below, and it is worth noticing that the *documentation* was where it landed
both times.

`gate-detached.sh` therefore refuses with **exit 69** when lingering is off, naming the one-command
remedy (`loginctl enable-linger <user>`), and refuses equally when the answer is **unmeasurable** —
a claim that a 30–50 minute gate survives session teardown requires an affirmative measurement, and
"I could not ask" is not one. This is the same posture as the cgroup refusal: a caller who believes a
gate is protected when it is not is the exact false assurance this script exists to remove.

**Fleet consequence:** lingering is a PREREQUISITE for a lane to run a detached gate, so it belongs
in `bootstrap-agent-machine.sh` alongside the other worker-environment guarantees. Until it is there,
a freshly-provisioned box will refuse (loudly, with the remedy) rather than run an unprotected gate.

### What the audit questions DO and DO NOT catch — measured, one finding on each side

Job 266 returned two findings, and they fell either side of the line cleanly enough to record as the
boundary of this whole apparatus:

| finding | severity | found by |
|---|---|---|
| a provably-stale extra marker was left in place, so a LIVE run stayed represented by a DEAD owner | **High** | review |
| `<summary>.heartbeat.launch-lock` was not gitignored | Low | **the sixth audit question, ~20 minutes earlier** |

**The six questions clear MECHANICAL debris and do not touch SEMANTIC judgement.** The gitignore gap is
mechanical: a fix created a new artifact name and a guard written against the old set did not cover it —
exactly what "what does this fix create?" asks. The stale-marker defect is semantic: *what should happen
when a path is already marked but its owner is dead?* No question about artifacts, orderings, sleeps or
weakened guarantees asks that. So run the audits to clear the cheap debris, and keep review rounds for
the judgement calls; expecting the audits to replace review would be the same overreach as expecting a
green suite to prove semantics.

**And the gitignore gap was a REPEAT.** Commit `82a878d02` in this same change exists solely to remove a
runtime heartbeat artifact committed by accident. A `git add -A` would have done it again. Prevention here
came from asking what the fix created, not from remembering the earlier mistake.

### A confident comment can license the defect beneath it — twice now

The stale-marker branch carried the comment `# a stale marker of our own shape; harmless`. That claim was
**false**: leaving the marker means this live run's heartbeat or log is represented by a dead owner, so a
later launch reads the path as reclaimable, takes it as its own summary, and two writers land on one file.
Reproduced — a launch succeeded with its heartbeat lock still naming pid 999999999.

This is the second instance in this change. The first was `host` described as "a DIAGNOSTIC ... not an
input to any verdict" sitting directly above `|| echo unknown`. **In both cases the comment's damage was
that it told the next reader the branch had been considered**, so the branch was not looked at again — by
me, on my own re-read. A wrong comment is worse than no comment, and the two here cost a High severity
finding each.

The reclamation itself is one line of `rm` plus one `ln -s`, and that simplicity is what the global launch
lock buys: no other launcher can interleave, so the summary lock's compare-and-swap machinery is not
needed for the extra markers. `4b.153` is the control that keeps reclamation honest — a LIVE foreign
marker must still refuse, or "reclaim the stale one" has quietly become "take everything".

### The sixth audit question: WHAT DOES THIS FIX CREATE, AND WHICH GUARD IS NOW BLIND TO IT?

Job 261 was one finding and produced SIX fixes, five of them consequences of the first. That ratio is the
lesson: **a fix that adds artifacts, states or orderings silently moves the target of every guard written
against the old shape.**

The finding: the artifact check was **asymmetric**. A launch asked *is any path I will write already
another run's reserved SUMMARY?* — while only summaries were ever RESERVED. So the question was
unanswerable by construction, and the global lock added earlier the same round, which correctly
serialises check-and-acquire, was powerless. **Serialisation cannot help when the thing you are looking
for was never recorded.** Measured: A with `--summary /t/a --log /t/b` reserved one lock; B with
`--summary /t/b` was accepted while A wrote its log into B's summary.

Reserving every write destination makes the check symmetric for free — no reverse lookups, just record
what was missing. And then:

| consequence | which guard was blind, and why |
|---|---|
| `$SUMMARY.heartbeat.launch-lock` exists | the gate's carve-out excused exact names that did not include it |
| the same name is **composed at runtime** (`"$_art.launch-lock"`) | `4b.105` derives shapes by grepping literal `$SUMMARY.x`, so it cannot see composed names — **it would have passed with the carve-out missing** |
| `<log>.launch-lock` exists, outside the summary prefix | nothing summary-anchored could see it; an in-repo `--log` would make the gate fail ITSELF |
| the summary-first ordering changed | the generic message claimed a path was "reserved as ITS summary" when it was reserved as another run's HEARTBEAT — the target records the owner, not the role |
| a test harness sourced the extracted predicate AFTER setting its variables | the sourced file's first line reset `TREE_STDOUT_REL=""`, silently clobbering them |

**The fix for the blind derivation is a different KIND of derivation: OBSERVE, do not parse.** `4b.105b`
performs a real launch and checks every file that appears beside the summary. Observation cannot miss a
composed name, a runtime-generated name, or one added later — and it found the `<log>.launch-lock` gap
within seconds of being written. That is the third time in this change that moving from parsing to
observing closed a class, after `4b.140` (stub `systemctl` rather than grep the case statement) and
`4b.142` (measure elapsed time rather than trust an exit code).

### The fifth audit question: DOES THIS FIX ASSUME SEQUENTIAL EXECUTION?

Job 256 returned a High that none of the four existing audit questions could have found. The job-251
artifact-set check was **check-then-lock**: two concurrent launches with `--summary x` and
`--summary x.heartbeat` could both observe no foreign reservation, both acquire their DISTINCT locks, and
then overwrite each other's files. The sequential case was closed; the concurrent one was not.

| audit | asks | would it have caught this? |
|---|---|---|
| sibling | where else does this rule apply? | no — the rule was applied everywhere it belonged |
| property | what property do these N instances violate? | no — the property was right |
| time | what changes while this code SLEEPS? | no — the race is not during a sleep |
| precondition | which guarantee did an earlier fix weaken? | no — none was weakened |
| **concurrency** | **what if two of these run at once?** | **yes** |

This is not a theoretical addition. CLAUDE.md's standing model is **multiple lanes per machine**, and
#3393's entire subject was per-machine refs colliding across lanes. A launcher that is only correct
one-at-a-time is wrong by design here.

**And the answer must be tested CONCURRENTLY.** The sequential case (`4b.144`) passed throughout; only
`4b.150`, which launches both at once and requires exactly one to win, can see the race. Its failure
message names both directions on purpose — *both accepted* means the serialisation is broken, *neither*
means the lock deadlocks — because those need opposite fixes and "expected 1, got 0" would send the
reader hunting the wrong one.

The fix keys the lock on what the colliding launches SHARE. Every per-summary name differs by
construction (`x.launch-lock` vs `x.heartbeat.launch-lock`, likewise their mutexes), so the key is the
**directory** — and a launch's artifacts all live in it. One lock means no acquisition order and no
deadlock; it is held only across check-and-acquire, and released AFTER the reservation exists (`4b.149`),
since releasing early reopens the race and holding it for the gate's lifetime would block every later
launch in that directory for 30-50 minutes.

### A fix can be INERT, and a passing test can be measuring a different path

Job 256's second finding is the first case here where one of my fixes **did nothing at all**. Job 251 made
the fallback blocking so an unproven-clock gate could be accepted — and left `if bash ...; then`, which
succeeds only on exit 0, while that case returns **2 (RUNNING)**. The verdict was received and discarded.

My verification had reported success, and it was invalid: `4b.142` used `--only fmt`, which finishes in
about a second, so the **terminal-summary** path answered `COMPLETE=0` and the fallback never ran. Two
component guesses were wrong — `--only scoped-tests` is also ~5s on a warm cache.

**What made the verification sound was not a better component but a DISCRIMINATOR.** The fast loop runs
20s before the fallback is reachable, so an acceptance in >20s can only have come from the fallback:
`--only roborev-lints` (~41s) gives exit 0 at 45s. The rule generalises — **when several paths can
produce the same observable, the test must measure something that DIFFERS between them.** `4b.142` now
asserts the premise (`host` absent from the beat), the outcome (exit 0), and the discriminator (>20s).

A postscript on that premise check, which failed against correct code before it worked: `grep -c` **exits
1 when the count is zero**, so `$(grep -c ... || echo 0)` yields `"0"` from grep AND `"0"` from the
fallback — a two-line value that compares unequal to `0`. The guard misread its own measurement, in the
failing direction. Fourth instance tonight of treating an exit code as a proxy for an answer when it
encodes something else (after `head` swallowing a status, `pgrep` matching its own ancestor, and
`ls-remote` proving nothing about a push).

### Scope narrower than the thing it protects — and a bound that made a verdict unreachable

Job 251's two findings share a shape distinct from the earlier ones: each control was correct *locally*
and too narrow *globally*.

**A per-call bound made an entire verdict unreachable, and it took two of my own fixes to do it.** Job
221 made an unverifiable hostname ABSENT, so the clock domain reads unproven. Job 231 put `--no-wait` on
every launcher call, so the reader cannot take a second sample. But with an unproven clock the reader
*cannot* judge freshness from the epoch — it needs PROGRESSION, which needs two samples — so every
stateless call returned `UNKNOWN`, the launcher's loop accepts only `0|2`, and after 20s it **stopped a
healthy, monitorable gate**. On any host where `uname -n` fails, that is every detached launch. Three
components each correct by their own contract; the defect lived only in the composition.

The fix is asymmetric rather than uniform: the FAST loop stays bounded and non-blocking, and the SINGLE
post-loop fallback is allowed its confirmation wait. The alternative — tracking beat-seq progression
inside the launcher — would have been a second implementation of the reader's progression grammar, which
jobs 172 and 198 exist to prevent. **And the resulting test pair is STRICTER than the blanket it
replaced**: `4b.141` forbids zero blocking calls (job 251's bug) *and* more than one (unbounded), where a
count of `--no-wait` occurrences permitted either.

**A reservation named after one path cannot protect an artifact SET.** With `--summary x` and
`--summary x.heartbeat`, launch A locks `x.launch-lock` and B locks `x.heartbeat.launch-lock` — both
acquire — and A's BEATER then overwrites B's SUMMARY every interval, destroying its terminal verdict.
Neither launch can see the other. Measured before the fix: both returned 0. The launcher now asks one
question of every path it will write — *is this already another live run's reserved summary?* — reusing
the same liveness primitives rather than a second copy of the classification. `4b.145` is the control
that stops "refuse everything" from passing as a fix.

**Housekeeping worth keeping.** `flock` decouples launch time from run time, so a run can acquire the
lock after edits that landed while it waited — three times this session I attributed a result to the
wrong file version. Each suite run now prints the md5 of every subject file at ACQUISITION, so its output
says what it tested. Same principle as the heartbeat carrying its own `run-id`: **make the artifact
self-describing rather than inferring from context.**

### A safety argument can be TRUE and INCOMPLETE — and the suite's own cost went unmeasured for 20 rounds

Two lessons from one episode, and the second is about noticing a cost I had been adding to all evening.

**The suite had grown to 696s, and it runs in `tooling-tests` on EVERY full gate.** Nobody asked for
that; it accumulated one case at a time, each of which looked cheap. What surfaced it was a harness
timeout, not judgement — the run tipped over a 700s ceiling. The driver is that ~22 cases expect
`STALLED`, and reaching that verdict means reaching the reader's confirmation sleep, which is
`interval + 5` taken from the BEAT'S OWN interval field: 25s per case with the standard fixture.
Lowering incidental fixtures to `interval: 1` cut the suite to **429s — a 267s saving on every full gate
in the repo**, verdict-neutral.

**The safety argument for that sweep was true and incomplete, which is the transferable part.** I
reasoned: `window = max(3 × interval, 90s)`, so any `interval ≤ 30` yields the same 90s floor, so
lowering an interval changes only the sleep and never a verdict. Both halves are correct. But the reader
*also* scales **future-clock tolerance** by interval, which the argument never considered — so case 7.6
("a beat up to one INTERVAL ahead is tolerated") flipped from `RUNNING` to `heartbeat-in-the-future`,
because 5s ahead is inside one 20s interval and outside a 1s one.

I had enumerated the interval-sensitive sections as 5 and 11g, protected them by line range, and
verified them byte-identical by md5. **There were three.** The enumeration was the weak step, not the
verification.

So the fix was scoped by that demonstrated unreliability rather than by patching the one case that
failed: the DEFAULT interval stays 20, and the saving is taken only at the 24 call sites that were
individually inspected. Patching 7.6 and re-running for green would have proved less than it appears —
with 21 defaulted call sites, a case can exercise a different path and still produce the same verdict
(the `11v.3` hazard), so **the suite is an oracle for VERDICTS, not for SEMANTICS**. And there is a
useful asymmetry in which sites are safe: a DEFAULTED call is precisely the one whose author did not
think about the interval, which makes it the likeliest to depend on it by accident.

### Two of my own fixes, each correct alone, jointly broken — the audit question none of the others ask

Job 238 found an ABORT, not a wrong verdict, and it was produced by two earlier fixes interacting:

- **Job 218** routed the missing-summary and unsnapshotable paths through the deferral funnel, so they
  now `break` out of the summary section **before** the initial snapshot is taken.
- **Job 231** added a post-wait comparison that expanded `_SUM_SNAP`, assuming the summary section had
  run to completion.

Neither is wrong in isolation. Together, under `set -u`, expanding the unset variable aborts:
`line 908: _SUM_SNAP: unbound variable`, exit 1. **A gate that completed during the confirmation wait
produced NO verdict at all** — worse than a wrong one, because the launcher treats any non-0/2 exit as
unmonitorable and stops a healthy gate.

The sibling audit asks *where else does this rule apply?* The time audit asks *what changes while this
code sleeps?* **Neither asks the question that would have caught this: which of my EARLIER fixes changed
a precondition this new code relies on?** Job 218 quietly removed a guarantee — that the summary section
always runs to completion — and job 231 then depended on it. Nothing recorded that the guarantee had
been weakened, so nothing could flag the dependency.

Two narrower lessons from the same finding, both worth keeping:

- **A comparison between two existing states cannot see CREATION.** The condition required the initial
  snapshot to be non-empty before comparing, so it was structurally blind to **absent-then-present** —
  which is the single most important transition here, because that is the gate finishing. `11x.1` pins
  it; `11x.3` is the control proving an absent summary with no completion still yields a verdict rather
  than a crash exit.
- **`exec` replaces the process, so an EXIT trap never runs.** The re-decision leaked its private
  snapshot directory on every fire — a regression of the class that leaked 868 of them earlier in this
  change. `11y.2` measures the directory count across a real re-decision rather than trusting the code.

And a log-integrity one: the `pub-surface` banner sat ~30 lines from its own invocation because these
suites were inserted between them, so the log announced pub-surface and then showed a different suite's
output — and a run dying in the gap left a log asserting a suite had run when it never did. **A banner is
a claim about what happens next; it must be adjacent to what it claims.**

### A justification for a shortcut is the signal to stop and apply the discipline

Two consecutive rounds (228, 231) found defects in the PREVIOUS round's fixes, and both times the cause
was the same: I wrote a reasoned argument for why a shortcut was acceptable instead of applying a
discipline this codebase already had.

- *"Being promote-only makes a second summary parser safe."* **False.** It counted openers and closers
  but never checked dialect match, ordering or duplicate fields — and **promoting on a malformed
  artifact IS a false certification**, the worst verdict here. The direction of the error (promote, not
  refuse) is not what bounds the harm. Fixed by the refactor I had declined: re-exec through the real
  grammar, so there is exactly one implementation.
- *"A wall-clock deadline checked per iteration bounds the loop."* **False.** The reader called inside
  the loop sleeps up to 65s, so 40 iterations is ~17 minutes against an advertised 20s. **A count
  bounds work only when each unit of work is bounded.**

Both were plausible, both were written down as justifications, and each took a review round to falsify.
**The justification is the tell**: when the argument being constructed is "a duplicate / a partial bound
is acceptable *in this case*", that is the moment to do the refactor instead.

The same shape then appeared twice more within the hour, outside the product, which is why it is
recorded as a rule rather than an anecdote:

- **In tooling.** I asserted in writing that editing a script would not affect an in-flight run because
  the interpreter "keeps its own copy". Bash reads scripts INCREMENTALLY; the edit injected a syntax
  error into a live run. Four edit-during-run incidents in one session, the last one corrupting the
  very script written to prevent them.
- **In advice to a peer.** I offered "only a push exercises the credential path, so a green `ls-remote`
  proves nothing", it was credited as load-bearing in a filed issue, and my own isolated repro
  falsified it: `push`, `ls-remote` and `fetch` all exit 128 with `credential url cannot be parsed`,
  with a clean-URL control failing differently (`Could not resolve host`). Corrected within the hour.

**Every instance was reasoned, plausible, and UNEXERCISED at the moment of assertion.** None was a
knowledge gap. Which is why the fixes that stuck are mechanisms, not intentions: `flock` (no process
names to match), copy-then-run (the template is never the executing file), truncate-both (a marker's
presence means *this* run). And note the tooling lesson in its own right — **every `pgrep -f` gate I
wrote deadlocked**, because the Bash-tool wrapper shell carries the whole command text in its argv and
is the runner's ancestor: a check whose subject includes the checker cannot work, however the pattern is
spelled. Narrowing the pattern three times never addressed that. The redundant guard was also the only
failure source: `flock` alone had always been sufficient.

### Audit the TIME axis too: where does this code sleep, and what can change underneath it?

The sibling audits above enumerate **space** — which files, which paths, which fields. Job 228 found two
defects neither of those could have surfaced, because both live in **time**:

- the reader sleeps up to 65s to confirm whether a non-advancing beat is stalled, and **the summary can
  become terminal while it sleeps**. If the gate finished and stopped its beater, the counter cannot
  advance — so a completed gate was reported `STALLED`, inviting a relaunch of a finished run. That is
  job 220's rule ("termination outranks a stale beat") on a new axis.
- the launcher's verification loop advertised **"within 20s"** and bounded itself by an ITERATION COUNT
  of 40 — while each iteration could block for `interval + 5`. Roughly seventeen minutes. **A count
  bounds work only when each unit of work is bounded**, and a diagnostic stating a limit the code does
  not enforce is the same defect class as a comment asserting a property the code lacks.

So the question is worth asking deliberately, and it is enumerable — these scripts sleep in exactly
three places:

| where time passes | what can change during it | status |
|---|---|---|
| reader's confirmation wait (≤65s) | the summary becomes terminal | **was the defect** (job 228) |
| reader's confirmation wait | the beat is replaced by a peer's | handled: the second snapshot clears the same bar, run-id checked |
| reader's settle-retry re-read | framing completes mid-write | handled: that is what the retry exists for |
| launcher's verification loop (≤20s) | the unit dies | handled: `is-active` break + settled re-derivation (job 213) |
| launcher's verification loop | artifacts replaced by a peer's | handled: single-snapshot nonce+run-id pairing (job 223) |
| beater's inter-beat sleep | the destination becomes a directory | handled: checked before EVERY publish (job 213) |
| beater's inter-beat sleep | the gate dies | handled: identity verified before every beat |

One of seven was unhandled, and it was the one review found. The table is recorded so the next person
re-runs the question instead of rediscovering the axis.

Two scoping notes worth keeping. The post-wait re-check may only **PROMOTE to `COMPLETE`, never refuse** —
that is what makes its small overlap with the main summary grammar safe, because the divergence risk jobs
172 and 198 removed is a second implementation that can produce a false REFUSAL; one that can only
recognise an unambiguous completion either fires or leaves the existing verdict untouched. And the
deadline's own tests **state what they cannot prove**: the pathological path it guards (a gate that starts
but never publishes a beat carrying our nonce) could not be constructed, because preflight already refuses
every heartbeat destination that would produce it. So the deadline is defence in depth, the tests assert
that the bound exists and that the advertised number EQUALS the enforced one, and they do not claim to
have observed it firing.

### When a finding names N instances, find the property and enforce it where all N pass through

Three times in this change an enumeration was fixed and the class stayed open, and each time the
enumeration was *complete for the sites it named*:

| the finding named | the enumeration missed | what closed it |
|---|---|---|
| the launcher's probe shapes (2) | there were **5** `$SUMMARY.`-anchored artifacts | a test that DERIVES the shapes from source |
| four summary refusals guarded | **nine** existed; one had no `\|\| break` at all | one funnel every refusal must pass through |
| three `beater-pid` branches missing `BEAT_ERR` | **four** other cases asserted `UNKNOWN 4 ""` and could hide the same defect elsewhere | the invariant enforced inside `expect_reader`, for all 234 cases |

The pattern is not that the enumerations were careless — it is that **a list is a snapshot of what someone
could see, and the property is what they were trying to express.** So the question to ask on receiving a
finding of the form "these N places are wrong" is *what property do they violate, and is there a point
every instance passes through where it can be enforced?* If yes, enforce it there; the list then becomes
a set of test cases rather than the fix.

The `beater-pid` instance is the sharpest, because both halves were mine one round apart. `4b.76` in the
detached suite had already taught that **asserting an exit code cannot see a SILENT refusal** — it caught
a launcher that exited non-zero while printing nothing. One round later I wrote a liveness case asserting
only `RC = 4`, and it passed green while the reader emitted the literal `gate-liveness: UNKNOWN ()`, in
the file whose whole doctrine is that every refusal names its cause. Knowing the lesson, writing it in a
commit message, and then not applying it to the next test is the same propagation failure as the
component-sibling table above — one level down, in the tests.

### The recurring shape in the LATE rounds: discipline present in one component, absent in its sibling

The early findings here were genuine design mistakes — a lock that could not tell *slow* from *dead*,
an "atomic rename" that was not a compare-and-swap. The late ones are a different animal, and naming
the difference is what makes them predictable:

| the discipline | where it already existed | where it was missing |
|---|---|---|
| decide from an immutable SNAPSHOT, never two reads of a live file | `gate-liveness.sh`, for all its own reads | the launcher, pairing its nonce with a run-id (job 223) |
| a terminal verdict needs the CLOSING marker | the recognised-`RESULT` path, from the start | the unrecognised-token branch added one round earlier (job 221) |
| exclude the exact artifact, never a subtree | `_tree_excluded`, narrowed twice | `.gitignore`, excluding the same paths (job 209) |
| gate-control variables must not reach the gate | the caller-side deny-list | the user manager's environment block (job 211) |

**Every one of these disciplines was already written down and demonstrated elsewhere in this same
change.** The failure was never ignorance of the rule — it was not asking *where else does this
apply?* That question is cheap to answer deliberately and expensive to have answered for you one
review round at a time, so it is worth running as an explicit audit after any fix that establishes a
rule: enumerate the sibling sites, and record the ones that are NOT instances along with why.

Two results from running that audit here are worth keeping, because both are cases where the rule does
**not** apply and the reasoning is what distinguishes them:

- **`gate-pid` is consumed by the reader but deliberately unvalidated.** Unlike `host` and
  `beater-pid`, it reaches exactly one place — a human-readable diagnostic string — and never a
  comparison. It even renders as `${HB_PID:-unknown}`, and that is fine: **a placeholder in DISPLAY
  text is harmless; the `host` defect was a placeholder entering a COMPARISON.** The distinction is
  the whole content of the rule, and "it's only a diagnostic" is trustworthy only when checked, since
  that exact claim about `host` was false.
- **The launcher's one remaining read of a live artifact is not an instance.** It establishes ONE fact
  from ONE read (does this beat carry our nonce). Job 223's defect was pairing TWO facts from TWO
  reads, where a peer could write between them. A single read cannot be internally inconsistent.

### A false COMMENT licensed a real defect — and prose has been the weak layer throughout

**THREE instances now, and the third makes the pattern the point rather than the anecdote.** (1) the
`host` comment below; (2) `# a stale marker of our own shape; harmless`, which licensed the job-266
High; (3) job 269's launch-lock placement, where the paragraph immediately above the code stated the
requirement — *"a lock every launch must take is a denial-of-service surface if any local user can
hold it, and XDG_RUNTIME_DIR is mode 0700"* — and the very next line read
`${XDG_RUNTIME_DIR:-${TMPDIR:-/tmp}}`, i.e. fell back to exactly the shared location the sentence
forbids. All three share one shape: **the comment asserts the safety property, and the reader (a
later author, or a reviewer, or me) checks the comment instead of the expression.** A comment that
states an invariant is therefore a *liability* unless something enforces it — which is why the
job-269 fix does not merely correct the path but makes the check AFFIRMATIVE (own it, and be 0700)
and REFUSE on an unmeasurable answer, so the invariant is executable rather than narrated.

The beater read `HOST_NAME=$(uname -n 2>/dev/null || echo unknown)`, and the comment directly above it
said `host` is *"a DIAGNOSTIC for whoever reads this file, not an input to any verdict."* **If that were
true the placeholder would be harmless.** It is not true: the reader compares that field against its
own hostname to decide whether the two share a **CLOCK DOMAIN**, and that decision gates whether
freshness may be judged from `beat-epoch` at all. Since the reader used the *same* `|| echo unknown`
fallback, two FAILED lookups compared equal, "proved" a shared clock, and a dead cross-host beat could
report `RUNNING` on incomparable timestamps — absence of measurement read as a positive match, in the
one field that licenses the epoch comparison.

The fix is in three places, and the shape matters: the beater now **omits** the field when it cannot
determine a host (so "absent" and "unverified" are ONE state rather than two spellings), the reader
treats an empty or literal-`unknown` value as unproven on both its comparison sites, and the comment
is corrected. **The comment was the most valuable of the three**, because it is what would have
reassured the next reader that the placeholder was safe.

**Prose has been the weak layer in this entire change**, consistently enough to be worth stating as a
rule. Four false statements were found in comments and docs — "atomic rename is a compare-and-swap",
"the detached launch survives logout", the fresh/stale verdict rows, and this one — against zero false
statements in code that the tests did not also catch. The asymmetry has a cause: **code is checked
against reality, prose against plausibility.** A confident explanatory comment sitting next to a
suspicious line is therefore a LEAD, not reassurance — and the more authoritative it sounds, the more
it is worth checking, because that is exactly what stops anyone checking it.

A companion finding in the same round is the smaller half of the same lesson: an unrecognised `RESULT`
token was treated as proof of termination **without requiring the closing marker**, so a truncated
write bypassed heartbeat confirmation. The recognised-terminal path had always required the closer; the
branch added one round earlier simply did not inherit it. Sibling paths through the same decision must
share their preconditions explicitly, or the newer one silently omits what the older one learned.

### A correct rule, over-applied: uniformity is not a substitute for asking what an artifact SAYS

Job 218's rule was right and hard-won: **do not make per-site judgements about which paths may skip
the funnel** — four "obviously safe to leave" sites had just turned out to be four real bypasses. So
every summary-side path was routed, including `unrecognised-result`, on the reasoning "same class: an
unusable summary must not pre-empt a matching beat."

It is **not** the same class, and job 220 caught it. The other paths — absent, unreadable,
unsnapshotable, no `RESULT` line — say *nothing* about whether the run terminated. A well-formed
summary that names the requested run and carries `RESULT: <something this reader does not know>` says
the gate **terminated**; only the verdict's NAME is unavailable. Deferring to the beat converts *"I
cannot name this verdict"* into *"the gate seems dead"*, and the beat's staleness there is a
**consequence** of termination, not evidence of a stall. A caller told `STALLED` may relaunch a run
that already finished.

The domain boundary is crisp once stated: **does this artifact carry information about termination?**
If no, the beat is the better authority. If yes, no heartbeat can answer for it. Two rules, each right
in its own domain — and the failure was applying the newer one past its edge because it had just been
expensively learned.

**How the fix avoided regressing the previous one:** not by adding an exception to the property guard,
which would have reintroduced job 218's exact defect (a name-based carve-out in a check that must stay
property-shaped). Instead there are now two **named** policies — `_summary_refusal_or_defer` and
`_summary_terminal_unknown` — each documenting its own deferral stance, so the guard still reads *zero
ad-hoc bare `verdict UNKNOWN` in the summary region* and still holds at zero. A control case
(`11q.5c`) fails if the two ever collapse into one and silently undo job 218.

### The guard that missed four paths because it checked a NAME, not a PROPERTY

Job 209 built `_summary_refusal` as *"one decision point for every summary-side refusal"* and asserted
it structurally — with a grep for `verdict UNKNOWN 4 "summary-` outside the funnel. **That checks a
name prefix, not a property.** Four paths emitted a bare `UNKNOWN` and none of them is spelled
`summary-*`: `no-summary-artifact`, `no-snapshot-dir`, `no-result-line`, `unrecognised-result`. They
bypassed the funnel for three more rounds while the guard reported clean, so a stale matching beat
never reached the two-sample confirmation and the pre-sentinel reap stayed unclassifiable. One of them
also called the helper with **no `|| break`**, so even its deferral fell through into the next check
instead of reaching the heartbeat side — it sat above the wrapper entirely.

This is the mistake this document describes elsewhere, committed *in the guard written to prevent it*.
Fluency with the lesson did not prevent it; only a check phrased as a property does. The replacement
states the property positively — **between the summary section's opening and the heartbeat side, zero
`verdict UNKNOWN` may appear** — and derives the region from the file's own structure at run time, so
a path added later is covered without editing the test. A companion case requires every routed call to
actually `break`.

The same lesson arrived from the opposite direction in the same round. `11b.17e` protects a real,
load-bearing invariant (`_ensure_snap_dir` must run in the CALLING shell; a subshell's `SNAP_DIR`
assignment is discarded, which is what leaked 868 snapshot directories) — but it was written as a grep
for `^_ensure_snap_dir || verdict UNKNOWN`, binding it to *the text that happened to follow*. Routing
that path through the funnel broke the assertion while the invariant was untouched. So a name-shaped
check produced a **false clean** in one place and a **false alarm** in the other. It now asserts a
column-zero call and no occurrence inside `$( )`, RED-checked by planting `x=$(_ensure_snap_dir)` —
because a guard that has never rejected anything is indistinguishable from one that cannot.

Throughout, the BEHAVIOURAL test carried the load correctly: `11b.17d` (15 reader invocations, 0
leaked directories) passed the whole time. **When a structural grep and a behavioural measurement
disagree, the measurement is the one describing reality.**

### Fixing one direction and leaving its mirror — twice

Job 209 made a summary refusal defer to a **fresh** matching beat, so an unreadable summary could no
longer make the launcher kill a healthy gate. Job 216 found the other half of the same door: a valid
matching beat that had gone **stale** still hit `UNKNOWN` before reaching the two-sample confirmation,
so `STALLED` was **unreachable** for a gate reaped during the pre-sentinel tree capture — the very
interval that moving the beater *before* the tree capture exists to cover. The ordering change and the
verdict it was meant to enable had been decoupled without anyone noticing.

That is the second instance of this shape here. The first was the age branch: job 206's fix treated a
provably stale beat as an older leftover, and job 208 showed staleness establishes no ordering. Both
times the reviewer named one direction, the fix addressed exactly that direction, and the symmetric
case stayed broken.

**The habit that follows is specific, not a resolution to be careful:** when a change makes condition
X defer to evidence Y, ask immediately what happens when Y is *present but degraded* — stale, foreign,
malformed, unmeasurable — and pin every one of those in the same commit. Section 11p does this with
five beat shapes plus a control that a valid terminal summary still wins; without that control the
deferral could quietly become "the heartbeat always decides" and `COMPLETE` would be unreachable.

Two things about the implementation are worth keeping. The refusal **defers** to the existing
heartbeat section via a `while :; do … break; done` forward jump (not a subshell, so every variable
the summary section sets stays visible) rather than duplicating the confirmation logic into the
refusal helper — jobs 172 and 198 both existed to *remove* duplicated grammars from this codebase, and
a third would have been the same mistake with a new number. And three existing cases legitimately
changed verdict from `UNKNOWN` to `STALLED`; what settled that was not the new code's own reasoning
but **`11g.7`, which already pinned** a fresh epoch from an unproven clock domain with a static counter
as `STALLED`. The heartbeat side's answer was this suite's considered position all along; the old
`UNKNOWN` was purely the pre-emption. When your change makes existing tests fail, look for a test that
independently corroborates the new behaviour before concluding the old ones were wrong.

### One shape, three times: a value I controlled on one path arrived by another

Worth stating as a family rather than three anecdotes, because each fix looked complete on its own
and the next instance appeared somewhere the previous fix could not see.

| what was controlled | what supplied it anyway |
|---|---|
| the gate's `_tree_excluded` carve-out, narrowed twice | `.gitignore`, via `--exclude-standard` |
| four guarded summary-refusal sites | a fifth refusal added in an earlier round of this same change |
| the launcher's caller-side environment deny-list | the **user manager's** environment block, which every `--user` unit inherits |

None of the controls was wrong. Each was simply not the only door, and in every case the second door
was invisible from the first one's code. The fixes that held were the ones that moved up a level: a
property test over the enumeration rather than the predicate; one funnel every refusal must pass
through; an empty environment the wrapper then fills, rather than a list of names to exclude.
**`env -i` is the environment-shaped version of the same move** — instead of enumerating what must not
get through, start from nothing and add only what is intended.

Measured both directions, since the mechanism is not obvious: with a variable in the manager's
environment the gate read it (63 vars in its `/proc/<pid>/environ`); started under `env -i`, absent
(53 vars). The concrete danger is an opt-out arriving unasked — a manager-set
`AGENT_GATE_ALLOW_MISSING_FIXTURES` or `CQLITE_ALLOW_FILE_GROWTH` relaxes the gate's own
validation, which is the one thing a certification run must not do quietly. Both are now at least
VISIBLE in the emitted block — `missing-fixtures: OPT-OUT (...)` (#2078) and, since #3402,
`file-size: OPT-OUT (...)` as the component's own status token, naming the variable, the count and
the grown files. Visibility is not a defence, though: an inherited opt-out is still an opt-out
nobody asked for, and the block only shows it to a reader who looks. Start the gate with a
deliberate environment.

**Two of this change's own verification attempts failed in ways that looked like passes**, and the
lesson generalises past this file. Checking the unit's `Environment=` property returned `0` — but we
never set that property, so it reads empty with or without the fix: a clean number that discriminates
nothing. And the first `/proc/environ` probe used `AGENT_GATE_WRAPPED`, which **the gate sets itself**
when it re-execs under `nice`, so it was present in both arms; a short `--only fmt` unit also finished
and was `--collect`ed before it could be sampled, yielding a `0` that meant *never measured*. Hence
the regression case treats an unsamplable probe as a **FAILURE, not a skip**: "could not look" must
never read as "absent" — the same rule this reader applies to its own verdicts, turned on the test.

### Two channels hid the same subtree, and only a PROPERTY test found the third set of paths

`_tree_excluded` was narrowed twice — jobs 203 and 204 — so the gate's carve-out excuses exact
artifacts instead of whole subtrees, because a `case` glob matches `/`. Both fixes were correct and
neither was sufficient, because **`.gitignore` was excluding the same paths**, and tree-integrity
enumerates untracked files with `git ls-files --others --exclude-standard`, which honours it. A
gitignore pattern matches a file OR a directory of that name, and git does not descend into an
ignored directory. Measured: source under `.agent-gate-summary.txt.launch-lock/` was visible to
**zero** of the enumerations that matter. The blindness the narrowings existed to remove was intact
the whole time, through a channel neither narrowing touched.

The negation form is counterintuitive and the obvious spelling fails silently:

| pattern added after the ignore rule | descendants visible |
|---|---|
| `!<path>/**` | **0** — git never descends into an ignored directory, so `**` is never consulted |
| `!<path>/` | **1** — the trailing slash matches the DIRECTORY, re-including it |

Glob entries take the same `!<glob>/` form. Both rows were measured in a throwaway repo, not reasoned
about.

**The durable lesson is about what the test asserts.** Two mechanism-level fixes left the property
broken. The test that closed it (`4b.119`) asserts the PROPERTY — *is planted source visible to the
enumeration tree-integrity actually uses* — so it covers any channel that could hide a subtree,
including ones nobody has thought of yet. It also DERIVES its subject paths from the committed
`.gitignore`, and that is what caught the rest: it found **15** subjects where the fix had addressed
12, and the three it flagged were the pre-existing `.integrity-fail.*` rules from #2874 that no
hand-written list of "artifacts this change adds" would ever have included. Twice in this change a
hand-enumerated inventory of artifact paths was wrong — the launcher's probe shapes (2 assumed, 5
real) and these (12 assumed, 15 real). Derive the set; do not list it.

### The reservation lock: five consecutive rounds, and why it was kept anyway

Recorded because the next person to touch this should start with the history rather than rediscover
it. The summary-path reservation produced a defect in **five consecutive review rounds**: an
incomplete-owner window whose unconditional refusal blocked the path permanently (196); the age
deadline that fixed it, which could steal a merely **paused** launcher's live lock (199); "atomic
rename is a compare-and-swap", which was simply **false** (203); a zombie launcher reading as live,
so its reservation could never self-heal (204); and `is-active --quiet` treating `activating` and
query failures as death (205). Every fix was correct; the concentration is the point.

Two properties make this more than an ordinary buggy component. **Its failure mode IS the harm it
prevents** — three of the five (the deadline, the false CAS, the unit-state read) put *two gates on
one summary path*, exactly what the lock exists to stop, so a defect here actively produces the
damage rather than merely failing to prevent it. And **it enforces no new invariant**: #2874 already
forbids two gates on one summary path and the gate de-exports its summary path so no child inherits
it, so this lock DETECTS a violation of an existing rule. Its value is a better diagnostic than
silent mutual corruption, not a new guarantee.

It was kept, deliberately, on one distinction: the last round's fixes close **classes**, where the
earlier ones closed **instances**. An affirmative `ActiveState` reading covers every state
systemd can report including the unmeasurable one, and the launcher/carve-out contract is now
composed by a derived test rather than asserted in prose. If a further defect of the
"next unnamed variant" kind appears here, the correct response is to remove the lock and let #2874
carry the invariant — not to write patch six.

**The transferable lesson is about CONTRACTS BETWEEN FILES.** Round 205's two findings were both of
that shape — the launcher's probe names against the gate's carve-out shape, and the reclaim decision
against systemd's state vocabulary. In each, both files were internally correct and their AGREEMENT
was wrong; in each, the dependency had been written down in a comment at the very site that broke.
**A stated dependency is not a protected one**, because prose is read by whoever happens to look. The
carve-out contract is now DERIVED at run time — every `$SUMMARY.`-anchored artifact shape is
extracted from the launcher's source and put through the gate's real predicate — which immediately
found **five** such shapes where the fix had considered two. A hand-written list would have encoded
the same incomplete mental model that caused the break.

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

### Round 47 (roborev job 269): three Mediums, and what each one generalises to

| Fix | Defect | Why it mattered |
|---|---|---|
| F1 | `_extra_locks` was a space-joined string iterated **unquoted** (`for _l in $_extra_locks`) | Word-splitting AND glob expansion. A space-bearing `$SUMMARY` made rollback remove the wrong words, so the real lock **survived** and every later launch on that path refused forever; a glob character could expand onto a **live peer's** reservation and delete it. This repository tracks 40 space-bearing paths under `docs/`, so neither shape is hypothetical. |
| F2 | the global launch lock fell back to `${TMPDIR:-/tmp}` | A **fixed-name** path in a shared directory: any local user can pre-create and hold it, refusing every detached launch on the box. Note what the defect is NOT — the two `PRIVDIR=$(mktemp -d "${TMPDIR:-/tmp}/cqlite-gate-XXXXXX")` calls are fine, because `mktemp` yields a fresh unguessable 0700 name. **The defect is the fixed NAME, not `/tmp`.** |
| F3 | the wrapper's `rm` (self-unlink) and `bash` (exec) were unqualified | The wrapper exports the **caller's** `PATH` before those lines run, so both resolved through a `PATH` this script does not control. A `PATH` without `rm` makes the self-unlink fail **silently**, leaving the 0600 file holding every forwarded secret on disk indefinitely; a `PATH` that shadows `bash` decides what we exec. |

Two things worth carrying forward. **F1 and F3 are the same class**: a value this script controls
(`$SUMMARY`, `$PATH`) is handed to a shell construct that reinterprets it — word-splitting/globbing in
one case, `PATH` resolution in the other. The fix in both is to remove the reinterpretation (an array;
absolute paths resolved in our own `PATH`), not to sanitise the value. **F2 is the third instance of a
false comment licensing the defect beneath it** — see that section above.

And the coverage that pins them is deliberately mixed. `4b.154`/`4b.158` are **structural** (the array
exists, iterated quoted; the emission is absolute), which is what a reviewer can check by reading.
`4b.155` was **written as the behavioural discriminator and is not one** — recorded here because the
correction is the useful part. The intent was "force the rollback with a space-bearing path, require the
lock to be gone"; measured against the pre-fix script it **passed there too**, i.e. it asserted an
absence that holds trivially. Three probes explain why: a live foreign reservation on the log, an
uncreatable log (read-only parent), and a lock path that is a **directory** are each refused by the
artifact-set **pre-check** (or the log-writability check) **before any lock is created**. The pre-check
classifies every artifact as free / live / owner-unestablished under the global launch lock, and the
lock stops a peer interleaving between check and `ln -s`. **So the `_extra_ok=0` rollback is not
reachable from outside**; only an I/O-level `ln -s` failure (ENOSPC/EROFS after the writability check
passed) reaches it. F1 is therefore a real latent defect whose exploit path — roborev's "remove
unrelated/live reservation files" — **requires reaching a rollback external input cannot reach**. The
fix stands as defence in depth, covered **structurally** by `4b.154`; `4b.155` was repointed at the
property it actually verifies, so that weakening the pre-check later makes the rollback reachable and
trips it. The general rule this cost: **a test that passes on the unfixed code is not coverage, and a
vacuous test wearing behavioural clothing is worse than no test** — so RED-run every new case against
the pre-fix artifact before believing it, which is how this one was caught. `4b.159` captures the **generated** wrapper by stubbing
`systemd-run` so it is never executed and therefore never self-deletes — because a structural grep
cannot see what the launcher actually wrote, only what its source says it would write. `4b.157` is
`4b.156`'s control, so a launcher that refused *everything* could not read as a pass.

### Round 48: the class audit, and why the independent enumerator found 1 of 4

The lead's ruling after round 47: three Mediums in one new file, all one class (unsafe shell handling of
untrusted input), means **rounds SAMPLE a class, they do not ENUMERATE one** — so audit the file against
a closed five-item list, fix everything in one series, RED-verify, ship.

| Class | Finding | Disposition |
|---|---|---|
| 4 — two-valued file predicates | `_foreign_reservation` opened `[ -L $lk ] || [ -e $lk ] \|\| { printf free; }`. Both predicates collapse *no such path* and *not permitted to look* onto the same FALSE, and the branch answered **`free`** — the permissive value — for both. | **FIXED.** `free` now needs an affirmative measurement (containing directory is a directory **and** searchable); otherwise `unknown`, which every caller refuses. Composition degrades safely: an unsearchable grandparent makes `-d` false and lands in the same branch. |
| 1 — unquoted expansion | `set -- $_state` and `set -- $rest`, **in both `gate-detached.sh` and `gate-heartbeat.sh`** — 4 instances. | **FIXED.** Two became pure parameter expansion; two keep the split (deliberate — `/proc/<pid>/stat` field 20) under `set -f`, prior `-f` state saved and restored. |
| 1 — the **generated** wrapper | shellcheck run on the captured artifact, not the source. | **0 findings**; `%q` encodes a space+glob value as `a\ b\*c`. |
| 2 — command qualification | The launcher runs under the caller's `PATH` by definition, so "qualify everything" is neither achievable nor meaningful against a hostile invoker (out of model per #3312). The boundary that matters: a command whose failure is **silent** and **security-consequential**, or whose output feeds a security decision. | Audited each: `stat` → refuse, `readlink` → `unknown`, `systemctl` → live, `flock` → refuse, `ln -s` → refuse. Already fail-closed. The **wrapper** was the real gap (F3, round 47). |
| 3 — shared-dir fixed names | `PRIVDIR`, the heartbeat probes and the launch snapshot are all `mktemp`-unpredictable; the global lock moved to a verified 0700 per-user dir (F2). | Clean. Residual recorded: the `$SUMMARY.launch-lock` family are fixed names in a **caller-chosen** directory, but a planted lock makes `ln -s` fail and the pre-check refuse — a DoS, never a false pass. |
| 5 — symlink / TOCTOU | `ln -s` is itself an atomic create, so losing the check→create race fails closed; the global lock removes the launcher-vs-launcher window. | One residual, unchanged and still recorded: the log truncation is check-then-create and bash cannot open `O_NOFOLLOW`. |
| dead code | `_has_line` and `_START_RE` — defined, never called, while five sites spell that regex out inline. | **REMOVED.** Dead code shaped like a check is worse than none: an unused `_START_RE` implies a single source of truth that does not exist. |

**THE METHOD LESSON, which outlives every finding above.** I pulled in shellcheck 0.11.0 specifically so
the audit would not rest on my reading of my own file, and it reported **1 of the class's 4 instances**.
Two of the misses were not tool limitations: those sites carried an explicit
`# shellcheck disable=SC2086` added in an earlier round, so **the enumerator was silent because it had
been told to be — at exactly the sites that needed it.** A blanket disable is not a narrowing; it
suppresses every reason the check exists, and here it suppressed the *globbing* half that nobody meant
to waive while licensing the *splitting* half that was deliberate. What actually closed the gap was
**my own structural test failing on my own fix**: `4b.162` went red after class 1 was "finished".

Three transferable rules:
1. **An independent enumerator can be muzzled by your own repository.** Before trusting a clean scan,
   grep for its suppression directives — a `disable=` line is evidence *about the scan*, not about the code.
2. **Where a lint's subject is legitimate, express the intent instead of muting the lint** (`set -f`
   around a deliberate split), and scope the directive to one command — shellcheck attaches a directive
   to the **next command**, so on `set -f; set -- $rest; …` it covered only the `set -f`. That is how the
   first attempt was caught.
3. **A tool covering one of five classes must not be reported as covering the audit.** "0 findings" is a
   class-1 statement here; classes 2–5 were enumerated by hand (21 file predicates individually).

RED-verified both directions against `fb97362eb`: `4b.161` (the false `free`) and `4b.162` (unguarded
splits, 2 per file) each fail on the pre-fix tree and pass after; `4b.160` is the control and passes on
both, so `4b.161` cannot be satisfied by a function that answers `unknown` for everything. Suites:
detached **195/0**, liveness **251/0**.

### Round 49-51: four lessons, three of them about my own instruments

**1. AUDIT BY CALL SITE, NOT BY PRIMITIVE** (lead's corrected rule, adopted). Round 48's class audit
enumerated predicates and paths — the *definitions* of the safe primitives — and never their **call
sites**. Both round-49 defects were sites that BYPASSED a primitive already present and correct a few
lines away: `_unit_is_live` had the closed grammar the unit-death check needed, and `_snap_pair` existed
for exactly the two-read race and was called twice immediately above. **An audit organised around
definitions is structurally blind to omission at the point of use, and the helper's existence makes the
omission feel handled.**

Corollary, seen twice: a structural test anchored on implementation *spelling* both fails on a correct
refactor and passes on the defect it was written to catch. `4b.124` bounded its range with the literal
`is-active --quiet` line and accepted any `_new_rid=` inside it — so it broke when that line was fixed,
**and** it had been passing on the unsafe two-grep form it nominally guarded.

**2. `RUNNING` MEANS ALIVE, NOT PROGRESSING — and the disclosure must be in the EMITTED LINE.**
`STALLED`'s emitted text already carried its own limitation ("NOT a claim that the process is dead…");
`RUNNING` carried none. **The asymmetry was the defect**: the verdict that could over-claim DEATH was
disclosed carefully in the artifact, and the one that over-claims PROGRESS was not disclosed at all. A
queued gate beats identically to a compiling one, so this reader answered `RUNNING` for **4.9 hours**
about a run with 61 CPU-seconds, and both a lane and its coordination lead read that as progress. The
note now lands at the single point in `verdict()`, so all five `verdict RUNNING` sites and any sixth
inherit it.

The measurement hierarchy behind that note, after three instruments each failed in a different direction:

| instrument | verdict |
|---|---|
| `cum-cpu ÷ elapsed` (cores sustained) | **THE discriminator** — cumulative *and* normalised |
| descendant PID-set turnover | rules out *wedged*, but a poll loop fakes it — corroborator only |
| any CPU **delta** | noise both ways: deltas false-negative on a bursty producer, turnover false-positives on a busy waiter |

Cumulative can **refute** a hang claim (a total is a lower bound on work done); no delta can **establish**
one. Measured: a working gate 1.32 cores, two queued gates 0.010 and 0.011 — and the same gate swung
105 → 0 → 103 CPU-sec/min *inside one component*, so no window length repairs a delta.

**3. A GREEN CASE IS NOT EVIDENCE THAT ITS PROBE RAN.** Narrowing `11c.8` from a spelling test to its
stated property introduced an awk regex that was a **syntax error**, so the scan returned zero hits on
*every* input including the reader — and the case reported `ok` while testing nothing. **The only thing
that caught it was `11c.8b`, the control added in the same commit**, which plants a genuine `/proc` read
and requires a hit. Nothing else in 251 cases noticed. An erroring probe and a clean subject produce
identical output, so any structural check whose scan can fail needs a control that fails when the scan
breaks — written at the same time, because a silently-erroring scan looks exactly like a clean subject.
Each discriminating behaviour is now measured individually (catches a real read; ignores a pure `echo`
mention; still catches `echo "$(cat /proc/…)"`), and the verdict messages carry their counts so a future
zero is distinguishable from a future silence.

**4. THIS LAUNCHER WAS A CONTAMINATION VECTOR, and forwarding-everything is why.** `agent-gate.sh` says
*"never export global RUSTFLAGS on a worker"* — a non-empty `RUSTFLAGS` suppresses cargo's managed block
and the gate then appends its own, producing a doubled `-D warnings` applied to components the gate
deliberately scopes it away from. That contamination made `binding-rust-tests` fail on a **clean** tree,
was diagnosed as a source defect, and halted the fleet for about an hour on a P0 that did not exist.
`gate-detached.sh` forwarded the caller's ENTIRE environment, so it propagated the poison **invisibly** —
through `systemd-run`, where no command line shows it. `RUSTFLAGS`, `CARGO_ENCODED_RUSTFLAGS` and
`RUSTDOCFLAGS` are now dropped and **named in `SKIPPED`**, which the banner emits; a silent drop of a
caller's variable would be this same defect in miniature. Verified on the generated wrapper, both
directions: before, `RUSTFLAGS` ×2 and `CARGO_ENCODED_RUSTFLAGS` ×1; after, zero, with 50 other exports
still present so the zero is not an empty capture.

**The through-line of all four, and of this issue: a check that can only see a TOKEN cannot decide a
STATE.** `PASS*` accepting `PASSthisNeverRan`; a flag enumeration treating any unlisted flag as a full
gate; an empty `ControlGroup` read as unmeasurable rather than as an answer; a name-grep counting a prose
mention as a live reference; a guard whose comment said *depend on* while it grepped for a substring. The
fix is never a rarer delimiter — it is to strip the channel that carries non-semantic occurrences, then
require the shape that means what you are claiming, and to prove the requirement fires.

## Four review findings the launcher's own doctrine did not prevent (roborev job 319)

Recorded because the *pattern* outlived every individual fix, and three of the four are the same
shape this document spends its length arguing against.

**F1 — an uninspectable pid was scored as an affirmative "no gate."** `_unit_runs_a_gate` defaults
`found=1`, so `continue`-ing past a pid whose `/proc/<pid>/cmdline` could not be read *returned*
"affirmatively not a gate", and the caller reclaims a live gate's summary path on that answer. The
skipped line's own comment said "not evidence either way" — true of the line, false of the function
it sat in. Refusing on every unreadable pid was not the fix either: a genuinely dead owner's pids are
unreadable too, and that is the job-196 permanent block. The distinction is `_pid_state`'s —
**affirmatively GONE** (and a zombie, which has already exited and can run nothing) stays
reclaimable; **PRESENT but unreadable** is the third value, and the caller refuses.

There was a second instance the review did not name: **`-r` succeeds on an exiting or mid-exec
process and the file then reads EMPTY**, so "no argv at all" was scored identically to "argv read,
no gate in it". Not theoretical — two daemons read this way while instrumenting this issue, and a
`/proc` sweep taken during the fix had ~10 pids vanish mid-loop. Inside this function the benign
explanation does not apply: a kernel thread also has an empty `cmdline` but can never be in a
`systemd-run --user` unit's cgroup.

**F2 — one function was answering two questions with opposite safe answers.** `_unit_is_live`
collapses *live* and *unmeasurable* onto `0`. At the reservation-reclamation site `0` means REFUSE,
so lumping them is conservative and correct. At the two heartbeat-acceptance sites `0` means
**ACCEPT** — and those sites exist specifically to reject a gate that published one beat and then
died. Keying them on a predicate that says "live" when it cannot measure re-admitted exactly that
case. The comment written at both sites — *"an unmeasurable unit still accepts (it can only weaken,
never invent, liveness)"* — stated the permissive rationale as though it were the safe one.

This was the **third round in this one function**: job 205 fixed its exit-code form, job 241 fixed
its state-name form, job 318 introduced the acceptance-site use, job 319 found the polarity. When a
claim fails at N stages, the claim's *form* is wrong — so the state was made explicit and **each
caller names the polarity it needs**, rather than a fourth patch at a call site. `_unit_is_live`
remains as the refuse-polarity wrapper over the one implementation, so the two cannot drift into two
opinions about one unit.

**And that fix was itself wrong, which is the fourth round and the actual lesson (job 320).** Making
the state three-valued was necessary and not sufficient: I then handed **both** callers the **same
partition** — `live | terminal | unknown` — and that partition is articulated for the REFUSE side,
where lumping every non-terminal state under `live` is conservative and right. On the ACCEPT side it
is not. `deactivating` is a unit *definitively shutting down*, so accepting it is precisely the
one-beat-then-dead case the acceptance gate exists to reject; `maintenance` and any state systemd
invents later are not evidence of anything either. **Naming the polarity is not enough if the state
space is only articulated for one of them.**

The state is therefore **four-valued** (`running | stopping | terminal | unknown`) and the two sides
take **opposite closures**, which is the point rather than an implementation detail:

- the **refuse** side (reclamation) stays closed on the **terminal** side — only `inactive|failed`
  are affirmative deaths, so an unrecognised state refuses. Job 241's rule, unchanged.
- the **accept** side is an **allowlist** of genuinely-running states (`active`, `activating`,
  `reloading`, `refreshing`); anything not named cannot accept.

That inversion is the standing rule that **an excusal is a positive verdict**. Blocklisting the
dangerous states admits the next unnamed one by default, and this one function produced that same
defect at **four consecutive rounds in a different spelling each time** (205 exit-code, 241
state-name, 319 polarity, 320 partition). A fifth named state would not have closed it; the
inversion does. The tests carry a non-vacuity control (`4b.192`) because an allowlist that accepts
*nothing* satisfies every refusal case above it.

**F3 — the advertised default invocation was the broken one.** The script runs under `set -uo
pipefail`, and this repository supports stock macOS `/bin/bash` 3.2, where an empty array expanded as
`"${ARRAY[@]}"` counts as unbound and **aborts**. A bare `gate-detached.sh` — the documented
full-gate form — leaves `GATE_ARGS` empty. Local bash is 5.2 and accepts it, which is why it
survived review and testing; `agent-gate.sh` already carries the identical fix and citation
(job-2108). **Limit stated: the abort itself was not reproduced here** — no bash < 4.4 on the host —
so that half rests on the repo's precedent and documented bash semantics, not on a measurement.

**F4 — a missing tool reported as a busy peer.** The global launch lock called `flock` with no
availability check; the only `command -v flock` guard sits in the reclamation path, which a normal
launch never reaches. On a host without `flock` the failure was reported as *"another launch holds
the global launch lock"* — naming a peer that does not exist, with remedies (retry, use a distinct
directory) that can never work. Demonstrated, then fixed as a named capability refusal.

**The transferable part.** F1, F2 and F4 are one family: a **two-valued test standing in for a
three-valued fact**, resolving the unmeasured case to whichever answer the surrounding control flow
made convenient. F1 resolved it to "no gate" because `found` defaulted that way; F2 resolved it to
"live" because that was conservative *at the other call site*; F4 resolved it to "contended" because
that was the only other branch. In each case the code carried a correct-sounding comment. So the
question to ask of a predicate is not *"is this the safe answer?"* but **"safe at WHICH call site,
and does 0 here mean refuse or accept?"** — a predicate reused across an inverted polarity is not
reused, it is reversed.

## Doctrine

- A lane **may** run its own full gate, via `gate-detached.sh`. The claim "lanes cannot
  run a full gate" was true of the naive launch and is not true of the detached one.
- **`flow-closer` runs its gate detached** — not because its own completion kills the gate
  (measured: it does not), but because a detached gate is independent of *every* pane and
  session teardown, which is a class of failure the closer cannot see coming and cannot
  distinguish from a slow gate. It costs one wrapper call.
- Never conclude "my gate is still running" from `RESULT: INCOMPLETE` alone. Ask
  `gate-liveness.sh`. A `STALLED` verdict means stop waiting open-endedly: re-read once, and
  relaunch only after waiting longer than the LONGEST COMPONENT OF YOUR OWN RUN, read off your
  own SUMMARY's component table rather than from a constant (the "~850 s" once written here was
  understated 2.4x; `tooling-tests` measured 2073 s). It is not proof of death.
- On a host where `gate-detached.sh` refuses (no working user systemd manager), the gate
  of record must be launched from a separate login (`ssh` + `nohup`), which gets its own
  scope. Do not launch it in-session and hope.
