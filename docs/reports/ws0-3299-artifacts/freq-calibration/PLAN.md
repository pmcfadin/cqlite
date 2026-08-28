# Frequency calibration plan — WRITTEN, NOT RUN

Purpose: measure **true core frequency f(S)** so the S=1→S=6 marginal-efficiency
loss can be split into the part the package clock accounts for and an
unattributed residual. Without it the whole discount reads as contention, which
would overstate what a footprint lever (#3288) could recover.

**This plan is not the measurement.** The lead launches; nothing here runs until
then. It is written now so no planning round sits between the grid finishing and
the number existing.

## Why `cycles / task-clock` is NOT the instrument (retracted formula)

Under CPU-wide `perf stat -C`, `task-clock` accrues elapsed × nCPUs **including
idle CPUs**, while `cycles` accrue only where a core is unhalted. The quotient is
therefore **occupancy × frequency**, not frequency. The grid's own data makes the
error unmissable — at N=1 the quotient reads 3.268 (S=1), 2.486 (S=2), 1.673
(S=3), 1.271 (S=4). A "1.27 GHz" at S=4/N=1 is not a downclock; it is one busy
core diluted across eight pinned logical CPUs. Published as a frequency it would
propagate straight into the turbo decomposition and corrupt the one number that
bounds #3288's ceiling. The existing `unhalted Gcyc/CPU·s` column keeps its
original caption and is NOT renamed GHz.

## The instrument

`msr/aperf/` and `msr/mperf/` — **confirmed present in sysfs on this box**
(`/sys/bus/event_source/devices/msr/events/` holds `aperf mperf smi tsc`;
`aperf` = `event=0x01`, `mperf` = `event=0x02`). Frequency = `TSC_freq ×
aperf/mperf`, the canonical method.

**Sysfs presence is NOT counting.** The Step 1 census exists because on this host
several counters programmed cleanly at `100.00% pct_running` and returned a hard
zero. So this calibration carries the same positive control discipline:

1. **Positive control first.** Run `aperf`/`mperf` over a single-threaded busy
   loop pinned to one core with every other core idle. The package must sit near
   max turbo, so the computed frequency must land in a plausible band
   (~3.0–3.9 GHz for a Xeon Platinum 8488C; the grid's own clean S=1/N=1 probe
   read 3.29 by the occupancy×frequency proxy, and at full occupancy on one core
   the proxy IS the frequency, so ~3.3 GHz is the expected answer).
2. **Degenerate ⇒ UNAVAILABLE.** If either counter returns 0, `<not supported>`,
   `<not counted>`, below 100.00% `pct_running`, or a ratio outside a physically
   possible band, it is an **unavailable instrument** — reported as such, exactly
   like the LLC counters. **No derived quotient is substituted**, the turbo
   decomposition is DROPPED, and the report states that separating turbo from
   contention needs a host where either the frequency MSRs or the LLC counters
   are readable. A weaker, honest result.
3. **Corroboration, not primary.** `cycles / ref-cycles × base` is an acceptable
   cross-check (`ref-cycles` ticks at a fixed reference rate while unhalted). Use
   it to corroborate `aperf/mperf`; use it as primary only if `aperf/mperf` is
   unavailable, and say which was used.

## Protocol

- One point per S in 1..6. **Load the pinned set FULLY (N = 2S)** so the
  measurement is taken at the occupancy the S=6 grid point actually ran at — a
  diluted set measures nothing useful, which is the retracted formula's failure.
- `perf stat -C <the same union of sibling groups the grid pinned> -e
  msr/aperf/,msr/mperf/,cycles,ref-cycles,task-clock` over a short window
  (~10 s is ample; this is a frequency, not a throughput).
- Contained via `test-data/scripts/perf-run-contained.sh`, like every other run.
- 3 reps per S, medians, spread printed — same discipline as the grid.

## What it produces

| output | use |
|---|---|
| f(S) for S=1..6, its own small table | published as the measured clock |
| `f(S=6) / f(S=1)` | the clock ratio |
| split of the S=1→S=6 marginal-efficiency loss | the fraction the clock accounts for, and the **residual** — printed ONLY with `--main-grid <C(S,N) tree>`, from which the marginal efficiency and the per-row endpoint ratios are DERIVED. Without one the section is withheld rather than printed from constants, which would have combined another tree's clock ratio with this campaign's grid |

**The residual stays UNATTRIBUTED.** There is no LLC counter on this box, so
nothing here can say the residual is cache contention — AC3's deferral binds this
section too. The claim is a **bound on what #3288 could possibly recover**, never
an attribution. `instructions/row` measured flat (×0.984 S=1→S=6) already
establishes the residual is not extra work; the clock ratio then says how much of
the `cycles/row` rise (×1.041) is frequency rather than stalling.

Note the shape of the expected answer: `cycles/row` rises only **4.1%** across
the whole S=1→S=6 range. Whatever the split, **the total available there is
small** — which is itself the material finding for #3288's funding case.
