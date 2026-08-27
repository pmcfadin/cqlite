# #3299 smoke run — plumbing proof, NOT the deliverable

Reduced size (`--s-list 1,2 --reps 1 --duration-s 15`) to prove the harness end
to end before the full sweep. **These are not AC1/AC2 numbers**: one rep per
point carries no dispersion, and the full sweep is S = 1..6 × ≥3 reps at 60 s.

| file | what |
|---|---|
| `CS-table.md` | the C(S) table `derive.py` produced from the smoke reps |
| `equivalence.md` | this harness's worker vs the rig's `ws0-scan-bench`, one core, one session |
| `s{1,2}-round1/` | the raw per-rep evidence: `window.json`, `perf.csv`, `attribution.json` |
| `selftest-output.txt` | all 39 guard cases, each observed to fire or pass |
| `siblings.map`, `manifest.jsonl` | the topology the run pinned from, and the reps it ran |

## What the smoke establishes

- All five counters read **100.00% `pct_running`** on a real S=1 and S=2 rep.
- Perf's enabled interval agrees with the driver's `[T0, T1]` to **2e-05** —
  the measured proof that counters and rows share one interval.
- Attribution shortfall **0.39%** at a 15 s window, inside the 0.5% bound (a 60 s
  window quarters it).
- The worker matches the rig's `ws0-scan-bench` to **−1.39%**, of which the
  bench's own three passes span 2.1% within a single run — see `equivalence.md`
  for the decomposition.
- S=2 marginal efficiency **0.983** on one rep. Directionally sane; not a claim.

## What it does not establish

Nothing about drift, dispersion, or the shape of C(S) beyond S=2. One rep per
point is exactly #3217's gap 1, and `derive.py` prints an under-replication
warning on this tree for that reason.
