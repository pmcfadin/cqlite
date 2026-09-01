# Negative control — this lane's `c7i`-class guest cannot answer #3287, and the reason is NEW

**This is NOT an acceptance artefact for #3287.** #3287's ACs belong to a *target* host that does
not exist yet. This file is a **negative control**, following the precedent the owner explicitly
approved on #3224 (`ws0-3224-artifacts/negative-control-c7i.md`): evidence that substituting the
lane's own box is barred by a **capability fact**, recorded so a future run under time pressure does
not re-derive it — or worse, "characterize the residual in prose" and ship #3217's outcome a third
time.

| | |
|---|---|
| Host | `ip-172-31-6-169`, lane box for issue #3287 |
| CPU | Intel Xeon Platinum 8488C (Sapphire Rapids), **8 physical / 16 threads, 1 socket, 1 NUMA node** |
| Caches as the guest sees them | L1d 384 KiB (8×), L2 16 MiB (8×), **L3 105 MiB (1×)** |
| Virtualization | `Hypervisor vendor: KVM`, full |
| Sysctls at probe time | `kernel.perf_event_paranoid=-1`, `kernel.kptr_restrict=0` — permissive |
| `perf` | version 7.0.12 |
| Probed | 2026-09-01 |

Reproduce: `bash docs/reports/ws0-3287-artifacts/capability-probe.sh <outdir>`.
Raw captures: [`host/`](host/) — `capability-probe.txt`, `tma-probe.txt`,
`event-disposition.txt`, `counter-semantics-verification.txt`, `gate-probe.{csv,txt}`,
`arm-{friendly,hostile-512m,hostile-2g,hostile-2g-nomux}.{csv,txt}`, `differential.txt` (the
run's verdict). The probe **exits non-zero and stamps `VERDICT: UNMEASURED`** if any step failed, so
a `VERDICT: COMPLETE` is an affirmative statement that every capture was taken.

---

## Summary — all three of #3287's method requirements are unreachable here

| #3287 method requirement | disposition on this host | how established |
|---|---|---|
| (1) TMA **level-2** breakdown | **unreachable** — `topdown-{retiring,fe-bound,be-bound,bad-spec}` and `slots`/`topdown.slots` are absent from the PMU; `perf stat -M TopdownL1`/`-M TopdownL2` cannot resolve | direct probe |
| (2) **offcore / prefetch-stall** term | **unreachable, and it LIES** — `offcore_requests_outstanding.*` programs cleanly and returns a hard `0`; `offcore_requests.all_data_rd` and `offcore_requests_buffer.sq_full` are absent | differential |
| (3) endpoints comparable to #3224 §5.3 | **unreachable** — no `uncore_*` PMU at all, and #3224's absolutes are 2-socket bare-metal Ice Lake-SP; its RUNBOOK forbids cross-host absolute comparison | probe + #3224 RUNBOOK |

## Finding 1 (NEW) — `cycle_activity.stalls_l3_miss`, the counter #3224's 67.76% headline rests on, is a SILENT ZERO here

#3224's negative control established that `LLC-load-misses` is `<not supported>` and
`cache-references` returns a silent `0` on this instance class. **Nobody had probed the stall
counter the attribution itself is built from.** It is worse than the events already known bad,
because a run that reached for "just re-measure *both* endpoints on the cheap box, self-consistently"
would be using it.

The differential is in `host/arm-hostile-2g-nomux.csv` — **4-event group, `enabled%=100.00` on every
row, asserted by the probe itself** (it FAILs rather than publish a multiplexed value where it claims
an unmultiplexed one), window gated to the chase, counted user-only.

**Deliberately stated as rep-invariants, not as absolutes.** These counts vary run to run — the box is
shared, the 12-event group time-shares, and the friendly arm's small counts swing by orders of
magnitude between identical invocations. Anything quoted below held in **every** rep; for exact
figures read the committed CSVs, which are the authority. (An earlier draft of this file quoted precise
absolutes and had to be re-derived twice; a number in prose rots the moment the artifact is
regenerated.)

| property | every rep | authority |
|---|---|---|
| `cycle_activity.stalls_l3_miss` | **exactly 0**, all arms, mux and no-mux | `arm-*.csv` |
| `offcore_requests_outstanding.all_data_rd` / `.cycles_with_data_rd` | **exactly 0**, all arms | `arm-*.csv` |
| `cache-misses` | **exactly 0**, all arms | `arm-*.csv` |
| `cycle_activity.stalls_l2_miss` on the 2 GiB arm | **> 85% of all cycles** (billions of cycles) | `arm-hostile-2g-nomux.csv` |
| `instructions` friendly vs hostile-512m | **ratio 1.00** (0.999 and 1.002 observed) | `arm-*.csv` |
| `ns_per_access` | **~5–6 ns** L2-resident vs **~180–270 ns** DRAM | `arm-*.txt` |

**The prediction was written before the measurement**: a 2 GiB random chase over 64 B nodes through a
serial data dependency cannot be L3-resident and the prefetcher cannot help it, so an honest
L3-miss-stall counter must be large. Instead, **>85% of all cycles are stalled with an L2-miss demand
load outstanding and exactly zero of them are attributed to an L3 miss**, on a working set 20× the
L3. That is not a small number; it is physically impossible.

**The workload's behaviour is established by WALL CLOCK, not by the PMU, so it is not in doubt — only
the counter is.** `ns_per_access` runs ~5–6 ns in the L2-resident arm against ~180–270 ns in the DRAM
arms — a 30–50× access-latency spread produced purely by changing the working-set extent — while
`instructions` differs by **≤0.2%** between those arms: identical work, identical code path. An L3 hit
is ~15–20 ns; ~200 ns is DRAM. No counter is needed to know these loads left the cache.

**It is not a `perf` event-table mis-encoding.** The raw programming was probed with #3224's own
verbatim verified encoding, and the *same event select* with a *different umask* works:

| programmed | meaning | hostile-2g, every rep |
|---|---|--:|
| `cpu/event=0xa3,umask=0x6,cmask=0x6/u` | #3224 §5.2's verified `stalls_l3_miss` encoding | **exactly 0** |
| `cpu/event=0xa3,umask=0x5,cmask=0x5/u` | `stalls_l2_miss` | **billions** (`raw_stalls_l2_miss` in `arm-hostile-2g.csv`) |

Same PMU, same event select, adjacent umask. The `umask=0x6` sub-event is unimplemented in this guest
and reports a measurement-shaped zero.

## Finding 2 — the offcore term #3287 exists to add is the same silent zero

`offcore_requests_outstanding.all_data_rd` and `.cycles_with_data_rd` both program cleanly and return
`0` in all three arms, including one where **billions of cycles — over 85% of the total — are stalled
with an L2-miss demand load outstanding.**
`offcore_requests.all_data_rd` and `offcore_requests_buffer.sq_full` are absent from the PMU outright.

This is the load-bearing one for #3287's *purpose*. The issue's whole point is that
`cycle_activity.stalls_l3_miss` is demand-load scoped, so prefetch-induced stalls hide in
`other execution stalls`, and an offcore term is needed to move them out. On this host that term
cannot be measured — so the TMA split alone would mis-attribute prefetch stalls as core-bound, which
the issue names in advance as "has not answered the question".

## Finding 3 — TMA level-2 is absent, not degraded

```
perf stat -M TopdownL1  ->  Unable to find PMU or event on a PMU of 'topdown-retiring'
perf stat -M TopdownL2  ->  Unable to find PMU or event on a PMU of 'topdown-retiring'
perf stat -e topdown.slots -> sys_perf_event_open() failed: Invalid argument
topdown-retiring / topdown-fe-bound / topdown-be-bound / topdown-bad-spec -> ABSENT-FROM-PMU
```

On Ice Lake and later, TMA is served by `PERF_METRICS` through those pseudo-events plus `slots`. With
`slots` returning `EINVAL` there is no level-1 breakdown, so level-2 is unreachable by construction.

**A hand-rolled substitute is not a lesser version of this study.** Some raw components *are* present
(`idq_uops_not_delivered.core`, `int_misc.recovery_cycles`, `exe_activity.bound_on_stores`,
`l1d_pend_miss.*`), so a pre-Ice-Lake-style TMA approximation could be assembled and would produce
percentages. It would be wrong twice over: the Sapphire Rapids TMA formulas are defined on `slots`
and `PERF_METRICS`, not on those events; and its memory terms would be built on the two counters
Findings 1 and 2 just showed are stuck at zero. It would be a confident, wrong report — the exact
outcome #3224's positive-control discipline exists to prevent.

## Finding 4 — no uncore PMU at all, so #3224's comparability requirement fails independently

```
/sys/bus/event_source/devices/  =  breakpoint cpu kprobe msr power software tracepoint uprobe
/sys/bus/event_source/devices/uncore*  =  No such file or directory
```

Consistent with #3224's negative control (which recorded 88 uncore devices on the `i4i.metal` target).
Independently of the counter findings, #3287 AC5 requires the result be *reconciled against #3224
§5.3's buckets*, and #3224's RUNBOOK states the rule flatly: "Any sentence comparing a #3224 absolute
to a #3217 absolute is a defect." #3224's absolutes are 2-socket bare-metal Ice Lake-SP. Reconciling
a single-socket virtualized Sapphire Rapids capture against them would be that same defect.

Note also that #3224's endpoint geometry needs 6 complete physical cores for the server plus 2 for
the client. This box has exactly 8 physical cores and is a **shared lane box** running up to 4
concurrent lanes and one full `agent-gate.sh` — zero headroom, and no isolation guarantee.

## What is confirmed working — the PMU is not dead, it is SELECTIVELY lying

That is what makes this a trap rather than an obstacle. In the same 12-event capture the counters that
do work move by orders of magnitude between the arms, exactly as the differential predicts, while three
sit at zero throughout.

**Two honesty caveats on the numbers below.** (1) The 12-event group **time-shares** on this host
(~65–75% enabled), so each value is a scaled estimate; #3224 §3.3 forbids publishing multiplexed values
as counts, and the verdict above rests on the unmultiplexed 4-event group instead. (2) The
**friendly arm's small counts are NOISY between reps** — `cycle_activity.stalls_l2_miss:u` was observed
at 2,047 and at 489,789 on two runs of the identical command — so **no precise ratio is quoted from a
single rep**. What is stable across every run is the *direction and order of magnitude*, and the zeros.
A stuck-at-zero counter is immune to both caveats: 0 × any scale factor is 0, and it was 0 in every rep.

| counter (`:u`, gated) | friendly → hostile-512m | verdict |
|---|---|---|
| `instructions` | **flat, ratio 1.00** | the control property — same work both arms |
| `cycles` | up ~30–40× | moves |
| `cycle_activity.stalls_total` | up ~40× | moves |
| `cycle_activity.stalls_l2_miss` | up **4–5 orders of magnitude** | moves |
| `l1d_pend_miss.pending` | up ~70–90× | moves |
| `l1d_pend_miss.fb_full` | up ~6–90× (rep-noisy) | moves |
| `cycle_activity.stalls_l3_miss` | **0 → 0** | **STUCK** |
| `offcore_requests_outstanding.all_data_rd` | **0 → 0** | **STUCK** |
| `cache-misses` | **0 → 0** | **STUCK** |

`instructions` being flat while `cycles` moves ~40× is the property that makes this a *control*: the
two arms run the same code over the same allocation and differ only in working-set extent, so the
entire cycle difference is memory-hierarchy. Any counter claiming to see the memory hierarchy must
move with it. Three do not.

A smoke test ("is the counter non-zero?") passes on the working ones and cannot distinguish the stuck
ones from a genuinely memory-clean workload. Only a differential against a predicted behaviour can.

## Carried forward

- #3287 needs a **bare-metal host** with core-PMU offcore support, `PERF_METRICS`/`slots`, and
  `uncore_*` devices. Preferably the same class as #3224's `i4i.metal` (Xeon 8375C, Ice Lake-SP), so
  AC5's reconciliation against #3224 §5.3 is a same-host-class comparison rather than the
  cross-microarchitecture comparison the RUNBOOK forbids.
- `perf_event_paranoid` was already permissive here, as on #3224's probe: **#3249's fix is confirmed
  working and confirmed insufficient a second time.** It removes the permission blocker and leaves
  the capability blocker untouched.
- The event-disposition triage is **three-valued on purpose** (`ABSENT-FROM-PMU` / `NOT-SUPPORTED` /
  `PROGRAMS`), and `PROGRAMS` is deliberately *not* called `SUPPORTED`: this host's whole lesson is
  that programming a counter and measuring with it are different facts.

## Measurement integrity — three decisions, all three forced by review

Recorded because the first revision of this probe got two of them wrong, and one of those wrong
answers would have *survived* into a metered bare-metal run had it not been caught here
(#3287 roborev job 305).

**(i) The perf window is gated exactly around the chase**, via perf's control FIFO
(`perf stat -D -1 --control fifo:<ctl>,<ack>`; `cache-hostile.c` drives the handshake). The first
revision passed only the benchmark's `--delay-ms` and **no** `perf -D`, so it counted buffer init AND
address-space teardown. Both neighbours are large and *asymmetric* between the arms — #3224 measured
teardown at 192M instructions (hostile) vs 80M (friendly) on a 512 MiB buffer, i.e. **larger than the
chase itself, and it does not cancel**. There is deliberately **no `-D <ms>` fallback**: that window
excludes init but not teardown, so an unavailable FIFO is a FAIL, never a quiet downgrade. A
`gate-probe` arm of 1,000 accesses runs first and must complete the handshake; it reads 986,903
cycles against 13.9e9 for a real arm — four orders of magnitude — which is the observable that the
window really is closed around the chase.

**How much it mattered, and the guard has a POSITIVE CONTROL** — full record in
[`host/gate-guard-positive-control.txt`](host/gate-guard-positive-control.txt). A guard that has never
been shown to fire is not evidence, so the ungated window was re-measured at the guard's own two probe
sizes:

| window | 1e3 accesses | 1e5 accesses | scaling | guard (≥10×) |
|---|--:|--:|--:|---|
| **UNGATED** (`--delay-ms` only) | 243,571,204 instr | 244,165,415 instr | **1.00×** | **FAILS — caught** |
| **GATED** (control FIFO) | 6,297 instr | 600,113 instr | **95.33×** | PASSES |

The ungated count barely moves when the workload does 100× more work, because a **constant ~244
million instructions** of buffer init and address-space teardown sits inside the window against roughly
6,300 instructions of actual chase — the signal buried under a factor of ~38,000 of *asymmetric* noise.
That is what the first revision published ratios from, and it is why the friendly→hostile instruction
ratio read **9.4** there and reads **1.00** gated. Since that ratio is the control's own validity
property (#3224 requires it near 1.0), the ungated capture had not merely inflated numbers — it had
**broken the control while still reporting success**.

Two properties of the guard, both deliberate. It is a **scaling** test, not a tuned ceiling, so it is
**host-independent**: #3224's absolute ceiling of 1e6 instructions per 1e3 accesses was correct for its
own machine and importing it would risk a false FAIL elsewhere, so the absolute is *reported* and only
the scaling property can fail the run. And the discrimination is enormous — 1.00× versus 95.33× against
a 10× threshold — so the bound needs no tuning and is insensitive to host and load.

**What did NOT change, and why that was predictable:** every capability verdict. Contamination can only
**add** counts, and no amount of extra work turns a nonzero counter into a zero. The zeros were zero
ungated, gated, multiplexed, unmultiplexed, at 512 MiB and at 2 GiB, in every rep. That invariance is
why the finding survived its own instrument being wrong — and it is why the capability claims rest on
the zeros rather than on the ratios.

**(ii) Events are counted user-only (`:u`).** The hostile arm runs ~40× longer in wall clock at equal
access count, so it absorbs proportionally more timer/IRQ kernel work; #3224 measured that alone
putting the instruction ratio at 1.22 with kernel counting on and 1.00002 with `:u`. Note the
modifier has **two non-interchangeable spellings**: a symbolic event takes a `:u` suffix, while a raw
`cpu/.../` event takes it *inside* the trailing slash (`/u`). The `/...:u` form is rejected outright
("Unrecognized input") and, because a perf event group is all-or-nothing, that one character silently
took down three of four arms while a 4-event group of symbolic events still succeeded.

**(iii) A positive verdict requires an affirmative measurement.** The first revision ran under
`set +e` with each arm ending in a successful `echo`, so a failed `perf`/`taskset` was swallowed and
the script still printed "capability probe written" over incomplete evidence — the fail-open class
this repository's doctrine exists to remove. Now every capture's exit status is recorded
(`capture-rc:` / `arm-rc:` lines in the artefacts), any failure stamps `VERDICT: UNMEASURED`, and the
script exits non-zero. **The checks earned their keep on their first runs**, catching the `/...:u`
breakage, the fresh-directory bug below, and — once CSV parsing was added — the fact that the earlier
gate probe omitted `instructions` and never read its own CSV, so it could not have detected the
contamination it advertised.

**Validity is measured, not inferred from an exit status.** A zero exit from `perf` is not evidence
that the numbers exist, so every arm's CSV is parsed: each requested event must have a row, a numeric
count (not `<not counted>`/`<not supported>`) and nonzero enabled time — and the group this file relies
on as *unmultiplexed* must read `enabled%=100.00` or the run FAILs rather than quietly publishing a
scaled estimate as a count. Three further masked-failure paths went with it: the inventory block
propagated only its LAST command's status; the event triage funnelled any *unrecognised* `perf` failure
into `PROGRAMS` with an empty value (a two-valued read of a multi-valued signal, taking the permissive
answer — now its own `ERROR` disposition that fails the run, because an operational error is not a
capability answer); and the counter-semantics size check was **vacuous**, since the loop wrote a heading
per event whether or not any definition was found (now each event is explicitly `FOUND` or
`NOT-LISTED`). (#3287 roborev job 308, findings 1 and 2.)

**One place where a non-zero exit is an ANSWER, not a failure**, and the distinction is deliberate:
`perf stat` exits non-zero when an event is absent from the PMU, and in the TMA probe and the
event-disposition sweep *that is precisely what is being measured*. Treating it as a step failure
would stamp `UNMEASURED` on a run that measured exactly what was asked. Those two blocks therefore
check only that they produced output at all; every other step checks its status.

**And a fresh output directory now works.** The first revision created `$OUT` but wrote to
`$OUT/host/*`, so the documented reproduction command failed on any new directory — and this
script's own run **masked it**, because that run's `$OUT/host` already existed. It is now
`mkdir -p "$OUT/host"`, and the fix was verified by running the documented invocation into a
directory that did not exist.

---

# RESUME NOTE — the exact capability gate a metal box must pass before #3287 is attempted

**Park record.** #3287 was parked 2026-09-01 by lead ruling (option (b)) awaiting a bare-metal
window. **That window is FUNDED (owner, 2026-09-01): [#3855](https://github.com/pmcfadin/cqlite/issues/3855)
is the co-scheduling bundle — this issue plus #3305/#3306/#3288 in ONE pass, and it carries a
counter-verification AC derived from the finding above.** So the question for whoever picks this up
is not *"can we afford a metal box"* but *"does the box #3855 provisions actually expose the
counters"* — which is what the gates below answer, before any metered hour is spent. Run
`bash docs/reports/ws0-3287-artifacts/capability-probe.sh <outdir>` on the candidate host FIRST and
check it against the table below — before any corpus staging, and certainly before any metered hour
is spent on a capture.

**The pass criterion is never "it programs without error".** That is the whole lesson of this file:
three of the counters below program cleanly on a `c7i` guest and return measurement-shaped zeros.
Every requirement is therefore stated as **"NONZERO and moving on the differential"** — the
`hostile` arm versus the `friendly` arm of `cache-hostile.c`, whose behaviour is known before it is
measured. A counter that does not MOVE has not been validated, whatever it printed.

## Gate A — TMA level-2 (#3287 AC1)

| probe | pass criterion |
|---|---|
| `perf stat -e slots -- true` and `-e topdown.slots` | resolves and counts; **`EINVAL` here** |
| `topdown-retiring`, `topdown-fe-bound`, `topdown-be-bound`, `topdown-bad-spec` | all four present; **all four ABSENT-FROM-PMU here** |
| `perf stat -M TopdownL1 -- true` | resolves and prints four level-1 shares summing to ~100% |
| `perf stat -M TopdownL2 -- true` | **resolves** — this is the AC itself. Level-2 is unreachable if level-1 is |

Level-2 is served by `PERF_METRICS` on Ice Lake and later. If `slots` returns `EINVAL`, stop: there
is no level-1 breakdown to subdivide, and the raw-event substitute is barred (see Finding 3).

## Gate B — the offcore / prefetch-stall term (#3287 AC2) — the one the issue exists for

| probe | pass criterion |
|---|---|
| `offcore_requests_outstanding.all_data_rd` | **NONZERO on the hostile arm and >> the friendly arm**; `0` in both here |
| `offcore_requests_outstanding.cycles_with_data_rd` | same; `0` in both here |
| `offcore_requests.all_data_rd` | present; **ABSENT-FROM-PMU here** |
| `offcore_requests_buffer.sq_full` | present; **ABSENT-FROM-PMU here** — this is the super-queue-full term that makes prefetch pressure visible |
| `l1d_pend_miss.fb_full` | NONZERO and moving (**already true here** — 1,557,955 → 9,932,430 friendly→hostile-512m; keep it, it is the fill-buffer half of the prefetch-pressure story) |

Gate B is the gate that decides whether the study is worth running at all. Without a working offcore
term the TMA split alone **mis-attributes prefetch-induced memory stalls as core-bound**, which
#3287's own text names in advance as not having answered the question. A host that passes Gate A and
fails Gate B is **not** a partial win — publish nothing from it.

## Gate C — reproduce #3224's own baseline buckets (#3287 AC5's reconciliation)

Non-negotiable, because AC5 requires the result be reconciled against #3224 §5.3, and that
reconciliation is only meaningful if this host can reproduce the buckets at all.

| probe | pass criterion |
|---|---|
| `cycle_activity.stalls_l3_miss` | **NONZERO and moving** — `0` in every arm here, the finding above |
| `cycle_activity.stalls_l2_miss` | NONZERO and moving (already true here) |
| `cycle_activity.stalls_total` | NONZERO, and the three must **NEST**: `stalls_l3_miss <= stalls_l2_miss <= stalls_total` in every arm. #3224's partition is differencing, so a nesting violation invalidates it |
| `LLC-loads`, `LLC-load-misses` | present and moving; `ABSENT-FROM-PMU` here |
| `cache-references`, `cache-misses` | NONZERO and moving; both silent `0` here and on #3224's own c7i probe |
| `ls /sys/bus/event_source/devices/uncore*` | at least one `uncore_imc*`; **none here**. AC3's bandwidth source and AC5's saturation verdict both need it. Never gate on `perf list \| grep uncore` — it lists per-model JSON entries on hosts with no uncore PMU at all |

## Gate D — topology and exclusivity (not a PMU question, and it fails on a lane box too)

- **≥ 8 complete physical cores available exclusively**: #3224's geometry is 6 for the server
  (`llc-s6-N16`) plus 2 constant for the client. A lane box has exactly 8 and shares them with up to
  4 concurrent lanes and a full `agent-gate.sh` — zero headroom and no isolation.
- **SMT sibling map read from `/sys`, never assumed.** Use `ws0_assert_full_physical_cores` from
  #3224's harness: it requires the pinned set to be an exact union of *complete* sibling groups with
  group count equal to the requested S, so it is correct on any topology. #3217's hardcoded core
  table silently measured a different machine than it labelled.
- **Single NUMA node**, server under `numactl --cpunodebind=<n> --membind=<n>`, so a mixed-NUMA
  allocation is excluded by construction rather than by argument.
- **Prefer the same class as #3224's host** (`i4i.metal`, Xeon 8375C, Ice Lake-SP). AC5 asks for
  reconciliation against #3224 §5.3's absolutes, and #3224's RUNBOOK states the rule flatly: a
  sentence comparing absolutes across microarchitectures is a defect. A different metal class means
  re-measuring both endpoints there and reconciling *ratios*, not absolutes — state which you did.
- **Re-apply and RE-VERIFY the sysctls, then re-verify them again mid-session.**
  `kernel.perf_event_paranoid` was found at **4** on #3224's fresh metal box (#3249's fix is not in
  the golden AMI and does not survive a reboot), and #3217 records both values reverting on their own
  schedule mid-session — surfacing later as unsymbolized frames, i.e. a failure that looks like a
  different problem. Note that on this guest both were already permissive and it changed nothing:
  **the permission layer and the capability layer are independent.**

## Also carried, from #3224's own park state

`docs/architecture/0.17-throughput-mission.md` records that #3224's pulled `/data` volume — the only
inputs for the `4.6×` AC5 re-derivation and two integrity checks — is **preserved and billing** until
#3287 consumes it or the owner deletes it. Check whether it still exists before assuming a
re-derivation is possible; if it is gone, say so rather than re-deriving from a different corpus.
