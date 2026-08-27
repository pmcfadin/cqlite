# `freq-run/` — the true-frequency measurement (the turbo/contention split)

Two endpoints, `msr/aperf/` + `msr/mperf/`, **both at 100.00% `pct_running`**. Taken by
calling `../harness/rep.py` directly with an MSR event list; the committed runner
`../freq-calibration/run.sh` was DELETED after it failed on invocation, and the
verified commands are in the report's reproduction section.

Frequency = TSC × aperf/mperf. `mperf` ticks at the TSC rate and measured
2.401 / 2.400 G/sec at the two points, confirming TSC ≈ 2.4 GHz.

| | S=1 (N=2, 2 logical CPUs) | S=6 (N=24, 12 logical CPUs) |
|---|--:|--:|
| aperf/mperf | 1.4621 | 1.4256 |
| **true frequency** | **3.509 GHz** | **3.421 GHz** |
| occupancy (task-clock ÷ nCPUs) | 1.600/2 = **80%** | 9.602/12 = **80%** |

**clock ratio f(6)/f(1) = 0.9750 (−2.50%)**, cross-checked independently by
`cycles`/`task-clock` = 0.9732 (−2.68%) — **agreeing to 0.18 pp**.

**Why the `cycles`/`task-clock` cross-check is legitimate HERE and nowhere else in
this campaign.** As a general frequency formula it is WRONG: under CPU-wide
`perf stat -C`, `task-clock` accrues elapsed × nCPUs **including idle CPUs**, so the
quotient is occupancy × frequency. It happens to be valid at these two points
*only because occupancy is matched at 80% on both*. An earlier revision of this
work published it as a frequency across the whole grid and it read 1.271 "GHz" at
S=4/N=1 — one busy core diluted across eight pinned logical CPUs. That is why the
primary instrument here is `aperf`/`mperf` and the quotient is a cross-check only.
