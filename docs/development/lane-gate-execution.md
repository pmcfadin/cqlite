# Running the gate from a lane session (issue #3473)

**Short version.** A gate launched from inside an agent session dies when that session's
**cgroup** is torn down. `nohup`, `setsid`, closing fds and being reparented to init do
**not** protect it, because cgroup membership is inherited across `fork` and cannot be
shed by detaching from the terminal, the process group or the session. Use
`scripts/flow/gate-detached.sh`, which puts the gate in a cgroup of its own, and poll it
with `scripts/gate-liveness.sh`, which tells "reaped" from "still going".

```bash
# launch (returns immediately; the gate keeps running)
bash scripts/flow/gate-detached.sh --summary /tmp/gate-summary.txt --log /tmp/gate.log

# poll (NEVER read the gate log)
bash scripts/gate-liveness.sh /tmp/gate-summary.txt
#   COMPLETE (exit 0) — a terminal verdict is in the summary file
#   RUNNING  (exit 2) — alive, no verdict yet (includes queued on the #1825 slot)
#   REAPED   (exit 3) — killed; it will never write a verdict, re-run it
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
| E | **subagent** | plain `nohup … &` | ran past 1150 s, 0 signals |
| F | **subagent** | `setsid` | ran past 1150 s, 0 signals |

**There is no ~10 minute ceiling.** Every variant ran to at least twice the reported
figure with no signal of any kind, including two launched by a subagent that was
deliberately stalled silently past 600 s — which also rules out the harness's 600 s
stall watchdog as a *direct* cause. The ~10 minute figure was real as an observation but
was not a property of elapsed time; it was how long the launching context happened to
live.

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
`tmux-spawn-*.scope` from A–D, alongside the subagent's own process. This is the link to
the delivery pipeline: `flow-closer` — the agent that by design runs the gate of record —
is a subagent, so the gate it launches lives in the closer's scope and shares the
closer's fate.

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

### What is inference, and what would settle it

Measured: there is no time-based ceiling; the pane scope has `KillMode=control-group`;
a teardown of such a cgroup kills fully-detached work and spares work in its own cgroup;
subagents get their own scope. **Not** measured: the specific deaths on lane-3393 were
not observed, and this host's journal retains no `tmux-spawn` scope stop records, so the
claim that *those* deaths were scope teardowns is the best explanation of the evidence
rather than a direct observation. It is the only mechanism found that accounts for all
four of the issue's findings at once (passive `sleep` killed, no OOM, no ENOSPC, ssh
control surviving). The next occurrence can now be settled rather than argued: the
liveness heartbeat below distinguishes reaped from running at the time it happens.

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
- The beater verifies the gate pid before **every** beat, pinning `/proc/<pid>/stat`
  field 22 (start time) where available so a recycled pid reads as dead. A beater that
  kept beating after its gate died would report a dead gate as `RUNNING` forever — this
  issue's own defect, one level down.
- Every positive verdict is an **affirmative measurement**. `RUNNING` requires a present,
  run-id-matching, fresh beat; `REAPED` requires a present, run-id-matching, **stale**
  one. A **missing** heartbeat is `UNKNOWN`, never `REAPED`: a gate predating this
  mechanism, or one whose summary path is unwritable, produces the same absence, and
  declaring those dead would be the fail-open shape one level down.
- The staleness window is `3 × interval` with a 90 s floor, read from the beat's **own**
  `interval:` line, so the reader holds no duplicate of the gate's beat period and cannot
  drift from it. There is deliberately **no env var** that widens the window or disables
  the beat — that hatch could only buy a vacuous `RUNNING` for a dead gate.

Every SUMMARY block now carries a `heartbeat:` line, so a pasted block shows the
mechanism ran (same reason #3148 stamps a positive `schemas:` line).

Self-tests: `scripts/tests/test_gate_liveness.sh` (66 cases) and
`scripts/tests/test_gate_detached.sh` (23 cases), both in the full gate's
`tooling-tests` component.

## Doctrine

- A lane **may** run its own full gate, via `gate-detached.sh`. The claim "lanes cannot
  run a full gate" was true of the naive launch and is not true of the detached one.
- **`flow-closer` runs its gate detached.** It is a subagent, so its pane scope is torn
  down when its context ends, and an in-session gate would be killed by its own
  completion.
- Never conclude "my gate is still running" from `RESULT: INCOMPLETE` alone. Ask
  `gate-liveness.sh`. A `REAPED` verdict means re-run; it will never produce a verdict.
- On a host where `gate-detached.sh` refuses (no working user systemd manager), the gate
  of record must be launched from a separate login (`ssh` + `nohup`), which gets its own
  scope. Do not launch it in-session and hope.
