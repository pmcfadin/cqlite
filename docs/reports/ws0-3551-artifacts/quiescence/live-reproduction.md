# #3551 — live reproduction of the two committed quiescence defects #3552 recorded

Sampler window: 2026-09-02T23:27:07Z .. 2026-09-02T23:44:06Z (106 samples, 10s cadence).

## D1 — the committed sampler and the committed judge DO NOT COMPOSE (two layers, not one)

`ws0_quiescence.sample()` returns `{load:{load1..}, competing_count, competing}`.
`judge --timeseries` requires, per record: a parseable `ts`; the narrow census fields
`rustc`/`cargo`/`gate` as non-negative ints; and a FLAT `load1`. Reproduced in order:

    QUIESCENCE_TIMESERIES_MALFORMED: record has no usable ts field
    QUIESCENCE_TIMESERIES_SCHEMA: the sample at '...' carries no 'rustc' field

#3552 recorded only the first layer. Supplying `ts` alone does NOT advance the judge to its
coverage check as that report states -- it advances to the census-field check and refuses
again. With the full flat schema supplied, the judge returns QUIESCENT and records
`census_breadth: FULL (competing_count present on every in-window record)`.

## D2 — COMPETING_CMDLINE substring match, reproduced live, on MY OWN AGENT SHELLS

3 of 106 samples were classified CONTAMINATED, every one of them by
`cmdline~agent-gate.sh` matched against a `/bin/bash -c source <claude shell snapshot> ...`
process -- an agent tool-call shell that merely MENTIONS the string, exactly the family the
file's own comment two lines above documents for `cargo` and removed it for. Records:

  2026-09-02T23:42:48Z  load1=4.4  comm=bash why=cmdline~agent-gate.sh; comm=bash why=cmdline~agent-gate.sh
  2026-09-02T23:42:58Z  load1=3.87  comm=bash why=cmdline~agent-gate.sh; comm=bash why=cmdline~agent-gate.sh
  2026-09-02T23:43:08Z  load1=3.35  comm=bash why=cmdline~agent-gate.sh

### A compounding detail #3552 did not record: the false positive is UNDIAGNOSABLE from the artifact

`census()` matches the needle against the FULL `/proc/<pid>/cmdline` but records only
`cmdline[:160]`. Every record above therefore carries the verdict `cmdline~agent-gate.sh`
with NO occurrence of `agent-gate.sh` anywhere in its own recorded text -- so a reader of the
artifact cannot tell a genuine gate from a shell that mentioned one.

## D3 (NEW here, not in #3552) — a zero census is NOT a quiet box

32 samples recorded `competing_count=0` while `load1` exceeded the judge's own
boundary bound of 2.0:

  worst: 2026-09-02T23:41:58Z  load1=6.39  runnable=9/1300  competing_count=0

`COMPETING_COMMS` is `rustc,cargo,cc1,cc1plus,ld,lld,mold` plus one cmdline needle, so a peer
lane running node, jest, python, git or a shell suite is INVISIBLE to the census. In-window
`load1` is explicitly "recorded as context, not a gate", so a window like the one above is
CERTIFIABLE. The boundary `load1 <= 2.0` bound is the only thing standing against it, and it
only samples two instants. This is a residual of the committed gate, stated rather than fixed
here; the mitigation used for this issue's own measurement is a per-CPU utilisation record
for the PINNED cpus, so contamination of the two CPUs that matter is visible rather than
inferred from a box-wide average.
