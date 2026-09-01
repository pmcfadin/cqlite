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
`event-disposition.txt`, `counter-semantics-verification.txt`, `differential.txt`,
`differential-unmultiplexed.txt`.

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

The differential (`host/differential-unmultiplexed.txt`), **4-event group, `enabled%=100.00` on every
row — no multiplexing, so these are counts and not scaled estimates**:

```
arm hostile-2g: 2 GiB random single-dependency pointer chase (working set ~20x the 105 MiB L3)
  ns_per_access = 253.038          <-- DRAM latency, measured by WALL CLOCK, not by any PMU
  11793534349,,cycles,3655135860,100.00
   8228167094,,cycle_activity.stalls_l2_miss,3655135860,100.00
            0,,cycle_activity.stalls_l3_miss,3655135860,100.00
            0,,offcore_requests_outstanding.all_data_rd,3655135860,100.00
```

**The prediction was written before the measurement**: a 2 GiB random chase over 64 B nodes through a
serial data dependency cannot be L3-resident and the prefetcher cannot help it, so an honest
L3-miss-stall counter must be large. 8.23 billion cycles — 70% of all cycles — are stalled with an
**L2**-miss demand load outstanding, and exactly zero of them are attributed to an L3 miss. With a
working set 20× the L3 that is not a small number; it is physically impossible. `ns_per_access=253`
independently confirms the loads are reaching DRAM (an L3 hit is ~15–20 ns), so the workload's
behaviour is not in doubt — only the counter is.

**It is not a `perf` event-table mis-encoding.** The raw programming was probed with #3224's own
verbatim verified encoding, and the *same event select* with a *different umask* works:

| programmed | meaning | hostile-2g count |
|---|---|--:|
| `cpu/event=0xa3,umask=0x6,cmask=0x6/` | #3224 §5.2's verified `stalls_l3_miss` encoding | **0** |
| `cpu/event=0xa3,umask=0x5,cmask=0x5/` | `stalls_l2_miss` | 8,395,798,758 |

Same PMU, same event select, adjacent umask. The `umask=0x6` sub-event is unimplemented in this guest
and reports a measurement-shaped zero.

## Finding 2 — the offcore term #3287 exists to add is the same silent zero

`offcore_requests_outstanding.all_data_rd` and `.cycles_with_data_rd` both program cleanly and return
`0` in all three arms, including one with 8.23 billion cycles of outstanding L2-miss demand loads.
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
do work move exactly as the differential predicts, friendly → hostile-512m. **These are the 12-event
group from `host/differential.txt`, which time-shares on this host (~65–76% enabled), so each value is
a scaled estimate and is shown here for its RATIO only** — #3224 §3.3 forbids publishing multiplexed
values as counts, and the verdict above rests on the unmultiplexed 4-event group instead. A stuck-at-zero
counter is immune to the distinction: 0 × any scale factor is 0.

| counter | friendly (L2-resident) | hostile-512m | ratio |
|---|--:|--:|--:|
| `cycles` | 344,219,141 | 12,350,848,493 | ×35.9 |
| `cycle_activity.stalls_l2_miss` | 2,522,213 | 10,240,540,985 | ×4060 |
| `l1d_pend_miss.fb_full` | 101,110 | 264,040,869 | ×2611 |
| `l1d_pend_miss.pending` | 142,537,385 | 14,566,541,893 | ×102 |
| `cycle_activity.stalls_l3_miss` | 0 | **0** | — |
| `offcore_requests_outstanding.all_data_rd` | 0 | **0** | — |
| `cache-misses` | 0 | **0** | — |

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

---

# RESUME NOTE — the exact capability gate a metal box must pass before #3287 is attempted

**Park record.** #3287 was parked 2026-09-01 by lead ruling (option (b)) awaiting a bare-metal
window. This section is the pre-flight gate for whoever picks it up. Run
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
| `l1d_pend_miss.fb_full` | NONZERO and moving (**already true here**, ×2611 — keep it; it is the fill-buffer half of the same story) |

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
