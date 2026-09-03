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

**31 of 42 sessions across three sets ran under a zero census.** Every direction count is
unanimous and every pair's own control is an order of magnitude below its treatment.

| arm | clean pairs | median Δcycles/row | median Δrows/s | direction (rows/s) | worst pair-control | median IPC |
|---|--:|--:|--:|--:|--:|--:|
| **B** `2,3` glibc | 6 | +17.85% | **−19.25%** | 0/6 up | 1.92% | 1.3903 |
| **C0** `2,3` arena=2 | 4 | +22.11% | **−22.71%** | 0/4 up | 1.80% | 1.3590 |
| **D** `2,10` jemalloc | 3 | −21.71% | **+29.21%** | 3/3 up | 0.70% | 1.5581 |
| **C** `2,3` jemalloc | 6 | −42.37% | **+61.17%** | 6/6 up | 2.41% | 2.1206 |

`worst pair-control` is the largest bare-scan disagreement inside any counted pair — identical
code on identical CPUs, so it is that pair's own drift bound. Nothing here is closer to its
control than 8x.

### 5b. Set-level corroboration

[`set1/AGGREGATE.md`](ws0-3551-artifacts/set1/AGGREGATE.md) is a fully clean four-arm set (12 of
12 sessions census-0). [`set2/AGGREGATE.md`](ws0-3551-artifacts/set2/AGGREGATE.md) and
[`set3/AGGREGATE.md`](ws0-3551-artifacts/set3/AGGREGATE.md) add arm D but were partly
contaminated; their set-level medians are reported for corroboration and their per-session
verdicts are in the adjacent `window-census.md`.

Set-level medians, three independent sets, Layer 2 (rows/s, paired vs A):

| arm | set 1 (12/12 clean) | set 2 (6/15 clean) | set 3 (12/15 clean) | pooled clean pairs |
|---|--:|--:|--:|--:|
| B | −14.16% | −18.48% | −20.01% | −19.25% |
| C0 | −22.29% | −24.60% | −19.93% | −22.71% |
| C | +64.07% | +61.28% | +57.44% | +61.17% |
| D | (not in set) | +29.21% | +30.31% | +29.21% |

Cross-arm control movement on cycles/row: **1.12%** (set 1), **0.63%** (set 2), **1.21%**
(set 3). The four estimates of each arm agree far more closely than any of them approaches zero,
and set 1 — which is fully clean and contains no arm D — reproduces the same three signs and
roughly the same magnitudes as the two partly-contaminated sets. Admission triple identical at
**4 / derived / 2** in all 42 sessions.

### 5c. What the numbers say

**1. The SMT-contention hypothesis is falsified in its stated direction.** #3551 proposed that
co-locating the `spawn_blocking` encode thread and the gRPC framing thread on one physical core's
two hyperthreads was *costing* throughput. Measured, separating them onto two physical cores
costs **−19.25% rows/s** and **+17.85% cycles/row**, 0 of 6 clean pairs positive. The inherited
#3096 pinning is not a confound to be removed; it is a **locality benefit**.

That is consistent with #3248's own §3 result — the Flight arm touches **5.19x the bytes for
1.22x the accesses**, which is a locality finding — and with the mechanism: the encode thread
hands buffers to the framing thread, so on one core the handoff is L1/L2-local and splitting them
across cores turns it into cross-core coherence traffic. `cycles/row` rising while rows/s falls is
the signature of the same work costing more, not of less work being done.

**2. F1's arena mechanism is falsified in its stated direction, which is exactly what it was
pre-registered to test.** F1 reasoned that *more* arenas meant more cross-arena lock traffic, so
capping should help. Capping to 2 measures **−22.71% rows/s** (0 of 4 pairs positive) — worse than
the uncapped two-core arm — with IPC falling to 1.3590. Fewer arenas is worse, so the cost is
arena *contention relieved by having enough of them*, not allocation spread across too many.
F1-AC2 said a null result would be *"a passing outcome to be reported as such"*; this is stronger
than null and in the opposite direction.

**3. The allocator is the lever, and it is worth +29% on its own.** At a FIXED pin — arm D, the
`2,10` sibling pin the rig has always used — jemalloc alone measures **+29.21% rows/s** and
**−21.71% cycles/row**, 3 of 3 pairs positive with a worst pair-control of 0.70%. No pin change,
no code change, no dependency: one `LD_PRELOAD` on the server process.

**4. The real mechanism is an INTERACTION, and it flips sign.** This is the finding arm D was
added to expose, and it is not visible in any single comparison:

| pin change `2,10` → `2,3` | under glibc | under jemalloc |
|---|--:|--:|
| Δrows/s | **−19.25%** (B vs A) | **+24.74%** (C vs D) |

The second physical core is not unusable — it is unusable **while glibc's malloc serializes
access to it**. Under glibc, adding a core loses 19%; under jemalloc, the same change gains 25%.
An interaction of ~44 percentage points that reverses direction cannot be read off either main
effect, which is why arm C alone (+61%) would have been attributable to whichever variable the
reader preferred.

This also supplies a mechanism for #3248's unexplained **+49% allocator term** under Flight, and
it is consistent with that report's other unexplained figure: the *same shared code* costing
+21.5% more under Flight is what allocator-lock stall inside shared call paths would look like.

**5. IPC is the corroborating signal, and it moves as the mechanism predicts.** 1.4645 (A) →
1.3903 (B, more stall from cross-core traffic) → 1.3590 (C0, more stall again from a tighter
arena cap) → 1.5581 (D, less stall) → 2.1206 (C, least stall). IPC was the *tightest* quantity
#3248 recorded (spreads 0.31%–1.72%), so a move from 1.46 to 2.12 is far outside drift.

**What was verified, not assumed, about the biggest number.** Arm C's flight leg is faster than
the bare scan (ratio 0.82x), which is legitimate — the server is multithreaded across two CPUs
while `execute_streaming` is essentially single-threaded — but it is also what a short payload
looks like. So: arm C completed **5 full scans of exactly 4,000,000 rows** (the pinned corpus
count) per rep against 3 for arm A, the rig's own `rows_per_scan_observed ==
rows_per_scan_expected` check passed on every rep of every session, and the jemalloc mapping was
verified per rep from both `/proc/<pid>/environ` and `/proc/<pid>/maps`.

---

## 6. Against the pre-registered kill criterion

> *"If B and C combined move served throughput < 3%, record the result and CLOSE — no tuning
> spiral. If either moves >= 3%, file the production change as its own priced issue."*

**Cleared decisively, and the criterion's own wording does not fit what was measured — so both
halves are reported rather than the nearest branch being chosen.**

* **B moves −19.25%**: ≥ 3% in magnitude, in the *losing* direction. There is no production
  change to file for arm B; the finding is that the current pinning is already the better of the
  two and should not be changed. The criterion did not anticipate a treatment that clears the
  threshold downward.
* **C moves +61.17% and D moves +29.21%**: ≥ 3% in the winning direction, so the second branch
  applies and a production issue **is** owed.

The follow-up is filed at **Backlog**, not Ready, carrying this measurement. Two reasons, both
external to this lane: F1 already routes an allocator change as **design-driven / OpenSpec**
because *"it adds a dependency … and affects every binding and downstream embedder, which is a
product decision and not a tuning knob"*; and the 2026-09-01 product-first ruling reserves Ready
for release-milestoned work. Promoting it is the lead's call, not this worker's.

**What the follow-up should carry, because the attribution matters to its scope:** the win is
available WITHOUT any pin change (arm D, +29.21%), and roughly doubles if the pin is changed TOO
(arm C, +61.17%) — but the pin change is actively harmful on its own. So "adopt jemalloc" and
"adopt jemalloc and re-pin" are both live options with different sizes, and "re-pin" alone is
refuted. A linked-jemalloc build (`tikv-jemallocator` behind a non-default feature) would also
need measuring in its own right: everything here is an `LD_PRELOAD`, which keeps one binary
across arms but is not how a shipped artifact would be built.

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

**Host.** `ip-172-31-7-163` — the same box #3096 and #3248 measured on, which is what makes
their figures comparable at all (#3552 ran on `ip-172-31-6-169` and correctly declined to compare
absolutes). Intel Xeon Platinum 8488C, 1 socket / 8 physical cores / 16 threads; physical core *k*
has siblings `{k, k+8}`, read from `thread_siblings_list` and recorded per session
(`cpu2=(2 10) cpu3=(3 11)`).

**Shared with nine other delivery lanes**, which is the single biggest fact about this
measurement and the reason for §5a's method.

**Corpus.** `/data/ws0-3096`, generated by `ws0-corpus-gen`: 4,000,000 rows / 40,000 partitions,
12 cells/row, `Data.db` 2,774,760,422 B, sha256
`4a903f6fa27c04dbf87a44fddf78615aed73fcd379ecaee6669f6b0d9bbae269` — byte-equal to the canonical
pin in `tools/ws0-corpus-gen/src/measurement_corpus.rs`, re-hashed from disk at every measurement
boundary, so no session is stamped `NOT A WS0 BASELINE`.

**Binaries.** ONE frozen set measured by every arm, `--bin-dir /data/ws0-3551/bins`:

```
fff86410764fe463d7f829522c29bc51  cqlite-flight
628f9bf9638dfaec2ca9b519503e0499  flight-loadgen
bf1595e042867ba4f8a9c20b907fe415  ws0-scan-bench
```

`mold 2.30.0`, `rustc 1.97.1 (8bab26f4f 2026-07-14)`, verified by `readelf -p .comment`. Built
under `env -u RUSTFLAGS -u CARGO_ENCODED_RUSTFLAGS -u RUSTDOCFLAGS` — **necessary, not
decorative**: this fleet exports `RUSTFLAGS=-D warnings` in every lane session, cargo prefers
`RUSTFLAGS` over `[target.*] rustflags`, and the managed `cqlite-mold` block is therefore silently
dropped (#3740). The first build of this set had no mold in `.comment`; the rebuild did.

**Environment, as measured** (recorded per session, ambient separated from injected):

```
ambient : LD_PRELOAD=<unset>; LD_LIBRARY_PATH=<unset>; RUSTFLAGS=-D warnings;
          CARGO_ENCODED_RUSTFLAGS=<unset>; MALLOC_VARS=<none>
injected: flight server process ONLY — LD_PRELOAD=<libjemalloc.so.2> (arm C, D) /
          MALLOC_ARENA_MAX=2 (arm C0); bare scan: NOTHING, asserted per rep
```

The ambient `RUSTFLAGS` is recorded for completeness and is **inert here**: `--bin-dir` implies
`--no-build`, so no compilation happens during a measurement session and the binaries' own
provenance is the `.comment` above.

**Sets were measured from a DETACHED worktree** at a pinned sha, so a teammate's commits to the
live tree could not disturb a run in flight, and so the rig's binary-staleness guard stayed
satisfied for the whole set.

---

## 9. Residuals

1. **No session carries an IN-RUN quiescence verdict.** `--quiescence-timeseries` refuses a
   contaminated session and leaves a non-empty `--out` the driver can never retry into, so one
   peer gate strands a round permanently. The sets were run without it — recorded honestly by the
   rig as `quiescence: NOT VERIFIED` — and judged post hoc by `window-census.py` from the same
   committed census. That is a real weakening: nothing *stopped* a contaminated session being
   measured. It is why the per-session verdicts must be read first, and why §5a counts only pairs
   whose both sessions were clean.

2. **`competing_count == 0` does not bound total foreign load** (D3, above). The clean pairs are
   clean by the in-run gate's own definition and no stronger.

3. **Nothing verifies the recorded arm ordering** — the driver claims the order it executed, and
   no artifact-side check establishes that a recorded order is method §3b step 2's rotation.
   #3287/#3299 own that.

4. **Arm D rests on 3 clean pairs, B and C on 6, C0 on 4.** Fewer pairs than a quiet box would
   have given. The three set-level medians corroborate each arm independently, but the pooled
   figure is not a substitute for a fully clean five-arm set, and no confidence interval is
   claimed from 3–6 pairs.

5. **`MALLOC_ARENA_MAX` was measured at 2 only.** F1-AC2 asked for 1, 2, 4 and default. 2 was
   chosen because the server's affinity mask is 2 CPUs; 1 and 4 are unmeasured, so "capping
   arenas does not help" is established at 2 and inferred elsewhere.

6. **`LD_PRELOAD` is not how a shipped artifact would be built.** It keeps one binary across all
   arms, which is the whole reason it was chosen (#3248's withdrawn machine-code claim), but a
   linked `#[global_allocator]` build could differ and is unmeasured.

7. **The corpus is CQLite-written and CQLite-read — a PERFORMANCE FIXTURE only** (#3042), never a
   correctness oracle. Nothing here says anything about output correctness.

8. **The rig's binary-staleness guard reds on correct input, and its printed remedy is a no-op.**
   It compares binary mtime against the HEAD *commit* time, which is unsatisfiable for a branch
   whose HEAD advances with non-Rust commits. `cargo clean -p` + rebuild finished in 1.01s without
   relinking, and removing `target/release/<bin>` restored it at the *original* mtime, because
   that path is a hardlink to `target/release/deps/<bin>-<hash>`. Only removing the deps artifact
   forces a real link step (76s). The relinked binaries were **byte-identical** (md5s above,
   reproduced three times), so the staleness was purely an mtime artifact. Recorded rather than
   filed: it fails closed and cost ~10 minutes, which does not meet the bar for a tooling issue
   under the 2026-09-01 ruling. The cheap fix, if wanted, is to compare against the last commit
   touching a **compiled input** rather than against HEAD.

---

## 10. Related

#2817 (epic), #3248 (the pricing this arm set came from), #3096 (the rig), #3552 (the predecessor
stopped by the same window problem), #3217 partC F1 (the pre-registered arena experiment), #3469
family 5 (the census self-exclusion swallow, closed here), #3287/#3299 (an OBSERVED interleaving
control), #2877 (never a CPU-share), #3400 (colour-immune log parses), #2926 (tree integrity vs a
sampler in the worktree), #3272 (the rig's instrument-integrity contract).
