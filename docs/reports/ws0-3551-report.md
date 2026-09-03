# WS0 #3551 — SMT-unpin + allocator trial on the `do_get` consumer

**Both mechanisms this issue proposed are FALSIFIED IN THEIR STATED DIRECTION, and the arm added
to control for one of them is the result.** Separating the Flight server's two hot threads onto
distinct physical cores costs throughput; capping malloc arenas costs more. Swapping the
allocator wins large, and the pin change that *loses* 16% under glibc *gains* 26% under
jemalloc — a sign-flipping interaction that is the actual mechanism.

This issue also had to build the instrument before it could measure: arm B was not reachable
through the committed rig at all, and the committed quiescence gate could not certify a window
because its own sampler and judge did not compose.

Parent: epic #2817. Evidence base: #3248 / PR #3455. Rig: #3096. Predecessor stopped by the same
window problem: #3552.

Artifacts: [`ws0-3551-artifacts/`](ws0-3551-artifacts/). Read
[`set*/window-census.md`](ws0-3551-artifacts/) **before** any figure — which sessions were
contaminated is a precondition for reading them, not a footnote.

---

## 0. What this claims, and what it does not

**Claims.** Within-session, per-round PAIRED differences between arms that differ in one axis at
a time, on one host, on the canonical WS0 corpus, with each pair's own drift control stated
beside it.

**Does not claim a cross-session absolute.** Method §3b: an untouched warm bare scan drifted
~10% in one hour on this box. The absolutes are reported in a separate layer and are not
reusable.

**Does not claim a production change.** #3551's kill criterion defers it (*"file the production
change as its own priced issue"*), and `ws0-3217-artifacts/partC/PROPOSED-FOLLOWUPS.md` F1 routes
it: a remedy that swaps the allocator or pins arena count *"is design-driven, OpenSpec: it adds a
dependency, changes the memory-behaviour profile, and affects every binding and downstream
embedder, which is a product decision and not a tuning knob."* Nothing here changes a shipped
code path — the arms differ by a `taskset` list and one or two environment variables on the
Flight server process.

**Does not claim a quiet box.** It claims a per-session census verdict, which is weaker and is
stated per session.

---

## 1. Arm B was not reachable, and why that is the first finding

`ws0-baseline.sh` pins the measured process with `taskset -c "$SERVER_CPUS"` to a **verified
physical-core sibling pair** (default `2,10`), so both of the Flight server's hot threads — the
`spawn_blocking` encode thread and the async gRPC framing thread — share the two hyperthreads of
ONE physical core. That is exactly the mechanism #3248 named as a candidate, and it was baked in
as a fail-closed guarantee: `:695` calls `verify_sibling_pair`, and `:682-687` records that the
topology override exists only for the guard's own tests.

So the SMT-unpin arm is a **second, equally affirmative assertion**, never a relaxation:

| flag | asserts, from the real `thread_siblings_list` |
|---|---|
| `--flight-pin-mode siblings` | one physical core's hyperthreads (the #3096 default; REFUSES a distinct-core set) |
| `--flight-pin-mode distinct-cores` | pairwise DISTINCT physical cores (REFUSES a sibling pair, and REFUSES a 1-CPU list, over which "pairwise distinct" compares nothing) |

An unknown value is a usage error, never a default. `2,10` under `distinct-cores` is refused and
`2,3` under `siblings` is refused — pinned in both directions.

## 2. The arms: a 2×2 of pin × allocator, plus the pre-registered arena probe

| | glibc | jemalloc |
|---|---|---|
| **1 physical core** `2,10` | **A** (control) | **D** |
| **2 physical cores** `2,3` | **B** | **C** |

plus **C0** = B + `MALLOC_ARENA_MAX=2`.

Every arm is **two logical CPUs**; only the number of physical cores behind them changes, and
**cpu 2 is common to every arm**, so exactly one CPU moves. The client stays on the rig default
`4,12,5,13,6,14,7,15` throughout, and the bare-scan leg stays on `--server-cpus 2,10` in every
arm — that last fact is what makes it a control rather than a second treatment.

**Arm D exists because the first set measured a confound in its own design.** A four-arm set
(A/B/C0/C) was run first and is reported below as corroboration; its arm C differs from arm A in
TWO properties at once, so its +64% could not distinguish *"jemalloc is worth that much"* from
*"jemalloc is what unlocks the second physical core glibc's malloc was preventing the server from
using"*. A→B being **negative** is what made the second reading live. With D, (D−A) prices the
allocator at a fixed pin, (B−A) prices the pin at a fixed allocator, and the interaction is what
the SMT question is actually about.

**C0 is the pre-registered form of the allocator hypothesis and it cost nothing.** F1 (strength
*"STRONGEST"*) reasoned that *"glibc creates per-thread arenas up to `8 x ncores`, so a runtime
sized to 6 cores spreads one stream's allocations across more arenas with more cross-arena lock
traffic"*, and its AC2 asked for exactly `MALLOC_ARENA_MAX` = 1/2/4/default, adding that *"if
capping arenas does not move the -24%, the allocator hypothesis is falsified and that is a
passing outcome to be reported as such."*

**`cycles/row` is the right instrument for the SMT question, not a companion figure.** The
counter sums hardware-thread cycles over the counted CPUs and both arms count two hardware
threads — but in arm A those two threads contend for one core's execution resources and in arm B
they do not. If SMT contention were costing throughput, arm A would burn more hardware-thread
cycles for identical work.

---

## 3. Four ways this measurement could have lied

### 3a. `perf` counted the WRONG CPUs — a fabricated win, found by smoke-running before changing anything

The counting list was shared by both arms and hard-wired to `$SERVER_CPUS`: `ws0-baseline.sh:1308`
(the single `perf stat` wrapper), `:1181` (the `--profile-out` sampler), `:1427` (the printed
claim). A naive arm B — server on `2,3`, `perf` still counting `2,10` — counts cpu 10's **idle**
and misses cpu 3's **work**. Fewer cycles over the same rows reads as a large arm-B win, so the
error is in the flattering direction.

The counted list is now per-arm (`PERF_COUNT_CPUS`), set explicitly at each `measure_*` call site
and **refused** when unset, when it is not a list this session verified, or when it disagrees with
the `taskset` list of the process about to be measured. It never defaults back to `$SERVER_CPUS`:
a silent default is how this defect would have survived its own fix. Recorded per session:
`counted_cpus_by_arm = {"scan": "2,10", "flight": "2,3"}`.

It also **falsified an existing NOTES bullet** — *"Both arms are counted identically, so the ratio
and the arm-to-arm delta are unaffected"* — true only while the pins agree. Rewritten to be true
in both configurations rather than deleted.

### 3b. Arm C could have been a byte-identical duplicate of arm B under arm C's label

`LD_PRELOAD` **fails open**: glibc prints `object ... cannot be preloaded ...: ignored` and
continues with system malloc. So arm C is verified per rep, after `await_server_ready`, from the
server's own `/proc/<pid>/maps` **and** `/proc/<pid>/environ`. Neither half suffices: `maps`
cannot see an arena cap at all (a cap leaves no mapping) and `environ` cannot see the silent
fallback. `environ` is NUL-separated and matched per whole entry — a substring match confuses
`MALLOC_ARENA_MAX=1` with `=16`. An unreadable `maps` or `environ` is a REFUSAL, never
"verified". Recorded per rep, e.g.

```
jemalloc VERIFIED for flight-bypass-warm-1: RECEIVED LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2
(59 environ entries) and libjemalloc.so.2 is MAPPED in the server process (62 mappings)
```

The `system` arm asserts the **negative** too, and the launch **empties** any inherited
`LD_PRELOAD` rather than trusting it unset: a control arm quietly running jemalloc inverts the
entire result.

**The injection is server-process-only, and that is load-bearing.** `lib-measure.sh` launches both
arms from one shell with the environment inherited, so an exported `LD_PRELOAD` would reach
`ws0-scan-bench` too — putting the **drift control** on a different allocator in arm C than in
arms A and B, and breaking method §3b step 3.

A shell *shim* named `cqlite-flight` that `exec env LD_PRELOAD=… cqlite-flight.real` was
considered and **rejected**: it passes the `-x` check at `lib-binaries.sh:170` and then makes
`binary-provenance.json` record the **shim's** digest instead of the server's — an artifact
asserting something false about what ran.

### 3c. Arms A and C would have been INDISTINGUISHABLE in every recorded field

`lib-binaries.sh` digests three binaries; the session manifest captured **no environment at all**.
One binary set is measured by every arm — deliberately, because #3248 withdrew a machine-code
claim precisely because *"the arms are different binaries"* — so "glibc" and "jemalloc" would have
produced artifact sets differing in nothing written down. Ambient and injected `LD_PRELOAD`,
`LD_LIBRARY_PATH`, `MALLOC_*`, `RUSTFLAGS` and `CARGO_ENCODED_RUSTFLAGS` are now captured as
**separate fields**: an operator's stray value and a deliberate injection are different facts and
only the first is a defect.

### 3d. The admission ceiling moves with the affinity mask

`cqlite-flight/src/main.rs:53` → `resolve_max_concurrent_scans` defaults to
`clamp(2 x available_parallelism, 2, 64)`, and `available_parallelism` respects the CPU mask — so a
pin change can silently move the ceiling and make an arm differ in **two** properties. These arms
hold the logical-CPU count constant so it should not move; *should not* is not a measurement, and
the server logs the answer. The triple (`max_concurrent_scans`, its source,
`available_parallelism`) is parsed from each Flight rep's own `*.server.log` (ANSI-escaped, so
escapes are stripped first — #3400's class), recorded per rep, and the report is **REFUSED if it
is not identical across every Flight rep**. Measured identical at **4 / derived / 2** in every
session of every set. `--max-concurrent-scans` is deliberately NOT pinned to force agreement:
pinning changes the configuration #3248 measured and hides the drift the check exists to catch.

---

## 4. The interleaving, and the control that makes a delta readable

`ws0-baseline.sh` measures ONE configuration per invocation, so an A/B/C comparison is a SET of
its sessions and the interleaving is a property of how they are ordered. Method §3b requires one
rep at a time, the arm order rotated every round, the control carried in every run, differences
taken WITHIN a round with the direction count reported, and rows/s AND cycles/row AND IPC per run
— and §3b.1 states plainly that the committed rig implements **none** of it and makes no
interleaving claim.

`scripts/perf/ws0-3551-abc.sh` is that operator obligation made runnable. It claims the order it
EXECUTED, because it executed it, and records each session's `round`, `position_in_round`,
`arms_in_round` and the round's `order`. **Nothing verifies that ordering** — no artifact-side
check establishes that a recorded order is the rotation step 2 asks for — which is the OBSERVED
control #3287/#3299 own and is unchanged here. `scripts/perf/README.md` now states those three
states separately (guaranteed / executed-and-recorded / unverified) rather than collapsing them.

**The control.** Only the flight pin and the allocator knobs vary; `--server-cpus` is identical in
every arm, so the bare-scan leg is code-identical **and** pin-identical everywhere and its
movement across arms is drift plus contamination and nothing else. `ws0_abc_aggregate.py` prints
it FIRST and declares any smaller treatment delta **NOT READABLE**. Vary `--server-cpus` per arm
and the bare scan becomes a second treatment, leaving nothing to read the first one against.

---

## 5. Results

Figures below are refreshed from the committed artifacts; the tables in
[`ws0-3551-artifacts/`](ws0-3551-artifacts/) are authoritative.

### 5a. The primary estimate: pooled CLEAN within-round pairs

See [`clean-pairs.md`](ws0-3551-artifacts/clean-pairs.md), produced by
[`clean-pairs.py`](ws0-3551-artifacts/clean-pairs.py).

<!-- CLEAN-PAIRS-TABLE -->

### 5b. Set-level corroboration

[`set1/AGGREGATE.md`](ws0-3551-artifacts/set1/AGGREGATE.md) is a fully clean four-arm set (12 of
12 sessions census-0). [`set2/AGGREGATE.md`](ws0-3551-artifacts/set2/AGGREGATE.md) and
[`set3/AGGREGATE.md`](ws0-3551-artifacts/set3/AGGREGATE.md) add arm D but were partly
contaminated; their set-level medians are reported for corroboration and their per-session
verdicts are in the adjacent `window-census.md`.

<!-- SET-TABLES -->

### 5c. What the numbers say

<!-- INTERPRETATION -->

---

## 6. Against the pre-registered kill criterion

<!-- KILL-CRITERION -->

---

## 7. The quiescence instrument: three defects, one new, and a corrected remedy

`docs/reports/ws0-3552-report.md` §2a records two defects here. Both are real, both **fail
closed** (false refusals, never false certifications), and that report's description of each was
**incomplete**; it is corrected in place as part of this change. Live reproduction:
[`quiescence/live-reproduction.md`](ws0-3551-artifacts/quiescence/live-reproduction.md).

**D1 — the sampler and the judge do not compose, in THREE layers.** `sample()` emits
`{load:{load1..}, competing_count, competing}`; `judge --timeseries` requires a parseable `ts`,
the census fields `rustc`/`cargo`/`gate` as non-negative ints, **and** a flat `load1`. #3552 says
supplying `ts` *"advances the judge to its coverage check, which is sound"*. It does not: it
advances to the census-field check, then refuses a third time on `load1`. Fixed by a committed
`ws0_quiescence.py sample-loop` subcommand whose records the committed judge accepts unedited,
every field derived from the SAME `census()` the boundary sampler uses so the two halves cannot
disagree about what "competing" means. Its `--out` is REQUIRED and refused inside any git worktree
— a file appended every 10 s trips `tree-integrity` (#2926) and a worktree is deleted at finalize.

**D2 — `COMPETING_CMDLINE` matched a MENTION, not an execution, and #3552's remedy cannot fix it.**
Reproduced live: samples were classified CONTAMINATED by `cmdline~agent-gate.sh` matching
`/bin/bash -c source <claude shell snapshot> …` processes — agent tool-call shells that merely
name the string. The file's own comment two lines above documents this family for `cargo`, says it
*"caused a FALSE REFUSAL of a quiet box"*, and removed `cargo` for that reason.

Both #3552 and the in-file comment prescribe *"exclude by identity — self PID plus an ancestor
walk, which this file already does elsewhere"*. **`census()` already does the ancestor walk, and
it cannot help**: the offending shells belong to *other* agent sessions, and a `setsid`-detached
sampler's ancestor chain is `init`, so every peer lane's shell is a legitimate non-ancestor.
Identity answers "is this me?"; the question is "is this process EXECUTING the gate, or talking
about it?"

The fix matches an argv **element** whose basename names the script. A first version using
basename equality alone was falsified by its own RED arm:
`basename('--flag=/path/agent-gate.sh')` **is** `agent-gate.sh`, and so is the basename of a `-c`
script text ending at the needle — so an element is additionally rejected if it starts with `-`,
contains `=`, or contains whitespace. **Declared residual, in the false-negative direction:** an
executed path containing whitespace is not recognised; no fleet lane path has one, and the
alternative admits every `-c` text ending at the needle.

A compounding detail #3552 did not record: the record kept `cmdline[:160]` while matching the FULL
cmdline, so every contaminated record carried the verdict `cmdline~agent-gate.sh` with **no
occurrence of that string in its own recorded text** — the false positive was undiagnosable from
the artifact. Records now name the matched element.

**Live before/after, same box, same needle.** The committed sampler counts a genuine competitor
with its own evidence — `"why": "argv=agent-gate.sh", "evidence":
"argv[1]=/data/lanes/lane-3749/scripts/agent-gate.sh"` — while the agent shells that produced
every earlier false positive are no longer counted. It also caught a real peer gate mid-set at
`argv[11]`, i.e. one launched through a wrapper.

**D3 (NEW here) — a zero census is not a quiet box, and it is DECLARED rather than closed.**
Measured: 91 consecutive samples read `competing_count=0` while `load1` reached 6.39 with 9
runnable tasks, and the four CPUs this issue pins measured **median 8%, max 86% busy** with
foreign work under a zero census. `COMPETING_COMMS` is compilers and linkers plus one needle, so a
peer lane running node, jest, python, git or a shell suite is invisible, and in-window `load1` is
explicitly *"recorded as context, not a gate"*.

**`COMPETING_COMMS` was deliberately NOT widened.** The file records that including `sccache`
*"refused a perfectly quiet box"* and that *"a guard that cries wolf on the normal state of every
box in the fleet is the guard people learn to delete"* — still right, doubly so on a ten-lane box.
So the verdict DECLARES what a zero census bounds, in the same idiom the file already uses for
`census_breadth`, and carries a per-CPU utilisation snapshot as the diagnostic. The snapshot is
cumulative, not a percentage: a derived percentage in the record is one step from becoming a
threshold nobody chose. Also closes #3469 family 5 (the census self-exclusion swallow) at source.

**And the per-CPU column is not a contamination bound DURING a window**, which a first draft of
`window-census.py` implied. `/proc/stat` reports TOTAL busy, and during a session the pinned CPUs
are busy BY DESIGN — measured 42–46% in-session against a median 8% idle — so the column is
dominated by the measurement itself and cannot separate a peer's cycles from ours. It is kept
because an UNDER-loaded session is a real failure it makes visible, and because the PRE-window
baseline does bound foreign load; the tool now says which of those it is.

---

## 8. Measured on, stated as measured

<!-- PROVENANCE -->

---

## 9. Residuals

<!-- RESIDUALS -->

---

## 10. Related

#2817 (epic), #3248 (the pricing this arm set came from), #3096 (the rig), #3552 (the predecessor
stopped by the same window problem), #3217 partC F1 (the pre-registered arena experiment), #3469
family 5 (the census self-exclusion swallow, closed here), #3287/#3299 (an OBSERVED interleaving
control), #2877 (never a CPU-share), #3400 (colour-immune log parses), #2926 (tree integrity vs a
sampler in the worktree), #3272 (the rig's instrument-integrity contract).
