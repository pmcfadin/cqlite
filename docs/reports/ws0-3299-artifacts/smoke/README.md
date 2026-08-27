# #3299 smoke + budget runs — plumbing proof, NOT the deliverable

Reduced grids run to prove the harness end to end and to calibrate the sweep
budget. **These are not AC1/AC2 numbers**: one rep per point carries no
dispersion. The deliverable is 25 points × ≥3 reps at 60 s.

| file | what |
|---|---|
| `CS-grid.md` | the two-dimensional table `derive.py` produces (grid + both denominators + endpoints) |
| `equivalence.md` | this harness's worker vs the rig's `ws0-scan-bench`, one core, one session |
| `s{S}-n{N}-round1/` | 20 s smoke reps: `window.json`, `perf.csv`, `attribution.json` |
| `budget-s{S}-n{N}-round1/` | 60 s reps at the two most expensive points, used for the budget |
| `selftest-output.txt` | all 41 guard cases, each observed to fire or pass |
| `siblings.map`, `manifest.jsonl` | the topology pinned from, and the reps run |

## What these establish

- Every counter reads **100.00% `pct_running`** at every point run, including
  S=6/N=24 (32 CPU-wide counting over 12 logical CPUs, 24 worker processes).
- Counter-window agreement **6e-06 … 6e-05** — perf's enabled interval versus the
  driver's `[T0, T1]`, i.e. measured proof the counters and rows share an interval.
- Attribution shortfall **0.070%** (S=1/N=8) and **0.086%** (S=6/N=24) at 60 s,
  comfortably inside the 0.5% bound after the switch to time-based sampling.
- The N dimension behaves as #3217 found: S=1 peaks at **N=2** (492,360 rows/s),
  not at N=1 (360,807) and not at N=8 (463,842).
- Budget: **238 s for two reps at the two most expensive points** at 60 s
  windows → the full grid is ≈2.0–2.5 h.

## A preliminary signal worth noting — ONE REP, NOT A CLAIM

At the endpoints, from the 20 s smoke (S=1/N=2 vs S=6/N=16):
instructions/row ×1.008 and L1-dcache-loads/row ×1.007 are **flat**, matching
#3224's shape — but **L1-dcache-load-misses/row is ×1.123**, and cycles/row
moves almost exactly with it (×1.121). #3224 measured its L1d misses/row as flat
(×0.987) on its host/corpus/arm. If the full sweep reproduces this, the private
cache hierarchy is *not* untouched here, which is materially different from the
assumption #3224's mechanism story rests on. **One rep, no dispersion, and
cross-host + cross-corpus + cross-arm against #3224** — recorded so the dense
endpoint sampling in the real sweep is aimed at it, not as a result.

## What these do not establish

Nothing about drift, dispersion, or the shape of C(S,N) between the endpoints.
One rep per point is exactly #3217's gap 1, and `derive.py` prints an
under-replication warning on these trees for that reason.
