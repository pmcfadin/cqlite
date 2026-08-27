## C(S, N) — bare-scan scaling grid, aligned window

Corpus: #3096 'Corpus B' (4,000,000 rows, 693.69 B/row, UNCOMPRESSED). Medians over reps; spread = (max-min)/median.

### C(N) per S, with dispersion

Aggregate rows/s (median), min-max spread as % of median in parentheses. Blank = not measured at that point.

| N | S=1 | S=2 | S=3 | S=4 | S=5 | S=6 |
|--:|---|---|---|---|---|---|
| 1 | 358,869 (5.1%) | 265,471 (3.4%) | 254,991 (8.2%) | 249,531 (5.0%) | 231,424 (2.3%) | 239,223 (2.0%) |
| 2 | **487,213** (4.3%) | 483,373 (4.7%) |  |  |  | 471,508 (1.9%) |
| 4 | 478,073 (3.4%) | 901,775 (5.3%) | 882,395 (1.0%) | 841,941 (1.9%) | 838,814 (3.3%) | 829,711 (3.4%) |
| 8 | 467,976 (2.2%) | **933,197** (2.9%) | **1,290,610** (1.7%) | 1,710,701 (2.3%) | 1,658,028 (1.6%) | 1,670,414 (0.7%) |
| 16 |  |  |  | **1,826,004** (0.4%) | **2,177,475** (1.7%) | 2,477,956 (0.4%) |
| 24 |  |  |  |  |  | **2,732,817** (0.7%) |

**bold** = that S's best-N point.

### Cross-S marginal efficiency — BOTH denominators

| S | best aggregate rows/s | **spread at that point** | N@peak | per-scan p50 rows/s | own N=1 | speedup vs **1-core peak** | **marg. eff. vs 1-core peak** | speedup vs 1-core N=1 | marg. eff. vs 1-core N=1 | cycles/row † | instr/row † | IPC | L1d loads/row † | L1d miss/row † | peak status |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 | 487,213 | 4.3% | 2 | 243,729 | 358,869 | 1.000 | **1.000** | 1.358 | 1.358 | 14,229.4 | 23,143.3 | 1.626 | 5,796.9 | 109.72 | **plateau** |
| 2 | 933,197 | 2.9% | 8 | 112,403 | 265,471 | 1.915 | **0.958** | 2.600 | 1.300 | 14,863.6 | 22,810.5 | 1.535 | 5,713.8 | 107.60 | **edge-truncated** |
| 3 | 1,290,610 | 1.7% | 8 | 162,619 | 254,991 | 2.649 | **0.883** | 3.596 | 1.199 | 15,821.9 | 23,441.8 | 1.482 | 5,866.5 | 124.16 | **edge-truncated** |
| 4 | 1,826,004 | 0.4% | 16 | 115,262 | 249,531 | 3.748 | **0.937** | 5.088 | 1.272 | 15,055.1 | 22,911.4 | 1.522 | 5,736.8 | 110.18 | **edge-truncated** |
| 5 | 2,177,475 | 1.7% | 16 | 133,267 | 231,424 | 4.469 | **0.894** | 6.068 | 1.214 | 15,572.9 | 23,262.5 | 1.494 | 5,822.2 | 120.41 | **edge-truncated** |
| 6 | 2,732,817 | 0.7% | 24 | 113,913 | 239,223 | 5.609 | **0.935** | 7.615 | 1.269 | 14,819.6 | 22,769.4 | 1.536 | 5,702.8 | 107.47 | **bracketed** |

### Is each best-N a real peak? (pre-registered bracketing rule)

- **S=1, N@peak=2 — PLATEAU**: N=4 is within 1.88% of N=2, inside that point's own spread (3.41%) — a flat top; the LOWER N is reported (same throughput, cheaper).
- **S=2, N@peak=8 — EDGE-TRUNCATED**: N=8 is the largest N tested at S=2; nothing above it was measured, so this is a LOWER BOUND on S=2's best, not a measured peak.
- **S=3, N@peak=8 — EDGE-TRUNCATED**: N=8 is the largest N tested at S=3; nothing above it was measured, so this is a LOWER BOUND on S=3's best, not a measured peak.
- **S=4, N@peak=16 — EDGE-TRUNCATED**: N=16 is the largest N tested at S=4; nothing above it was measured, so this is a LOWER BOUND on S=4's best, not a measured peak.
- **S=5, N@peak=16 — EDGE-TRUNCATED**: N=16 is the largest N tested at S=5; nothing above it was measured, so this is a LOWER BOUND on S=5's best, not a measured peak.
- **S=6, N@peak=24 — BRACKETED**: N=32 (2,652,863, spread 0.67%) is 1.95% BELOW N=24 (2,705,485, spread 0.64%), exceeding the larger of the two spreads, so the curve has turned over; N=16 is below N=24 as well, making this a clean INTERIOR MAXIMUM (rises 16->24, falls 24->32) — SOURCE: extension B (grid 6:24,32, 3 reps, incumbent re-measured INTERLEAVED with the candidate in every round, so the comparison is contemporaneous).

**An `edge-truncated` row is a LOWER BOUND, not a measured peak**, and any figure derived from it (including AC2's target) inherits that status. It is not smoothed, interpolated, or quoted as a result.

**† BASIS — every per-row counter is summed over ALL PINNED HARDWARE THREADS** (2S logical CPUs, both SMT siblings of each of the S cores), which is the set `perf stat -C` counted and is the same set at every N for a given S. It is NOT a per-hardware-thread figure: dividing by 2 would give the per-thread average only if both siblings were equally loaded, which is exactly what varies across the N ladder. IPC is basis-invariant (a ratio of two sums over the same set). Per mission section 1, no figure here is quoted without its basis.

`marg. eff.` = speedup / S; 1.000 would be perfect scaling. **Reference B (S=1's own peak) is PRIMARY**: it is the most the engine achieves on one physical core, so it is the fair 'perfect scaling' unit, and it is the CONSERVATIVE choice — it yields lower efficiencies than A. Reference A (S=1 at N=1) is published alongside because it is the naive baseline. The `own N=1` column is why a self-normalised speedup is NOT published: each arm's own N=1 moves with S, so dividing by it would flatter the wide arms.

### Rig resolution — per point, because spread is N-dependent

| N | median spread over the S values measured at that N | points |
|--:|--:|--:|
| 1 | 4.20% | 6 |
| 2 | 4.27% | 3 |
| 4 | 3.36% | 6 |
| 8 | 1.98% | 6 |
| 16 | 0.45% | 3 |
| 24 | 0.74% | 1 |

Grid-wide: median **2.27%**, max **8.20%** over 25 points. **That grid-wide pair is a summary across heterogeneous points — useful for judging the rig, NOT an error bar for any single figure.** The deliverable (S=6 at best-N) sits in the high-N regime, so its own spread, printed in the table above, is the number that bounds it.

**Round-over-round direction: 35 rose, 15 fell** across consecutive rounds at the same point. This is **INERT DATA, EXPLICITLY UNCONTROLLED FOR DRIFT** (`scripts/perf/README.md`): this rig does not control drift, nothing here establishes the session ran without it, and no round-major claim is made. A directional imbalance is consistent with page-cache warming or thermal settling. The S-order ROTATION is what distributes such a drift across points rather than concentrating it in one S — which is why the curve's SHAPE survives a drifting session even though no absolute number does. The rotation is a reasonable ordering, NOT a verified control. Note also that a median of 3 draws from a drifting distribution, so 'median of 3' reduces but does not remove this — it is not a drift-free figure.

### Endpoints S=1 and S=6 — the L1d partial of the DEFERRED AC3

AC3 (LLC-load-misses/row, S=1 vs S=6) is **DEFERRED**: every LLC instrument on this box is unavailable (`../host/README.md`), and nothing below discharges it. But `L1-dcache-loads` and `L1-dcache-load-misses` ARE real here, and they are exactly the counters #3224 reported as flat across its endpoints — so the private-cache half of the question is answerable.

All per-row counters below are summed over ALL PINNED HARDWARE THREADS (2S logical CPUs) — the same basis as the table above, and the same set `perf stat -C` counted.

| per-row counter | S=1, N=2 | S=6, N=24 | ratio |
|---|--:|--:|--:|
| instructions/row | 23,143.3 | 22,769.4 | x0.984 |
| L1-dcache-loads/row | 5,796.9 | 5,702.8 | x0.984 |
| L1-dcache-load-misses/row | 109.72 | 107.47 | x0.979 |
| cycles/row | 14,229.4 | 14,819.6 | x1.041 |
| IPC | 1.6264 | 1.5363 | x0.945 |

**Read this as CONDITIONAL and CROSS-EVERYTHING.** #3224's endpoint figures (instructions 38,856.8 -> 38,685.6; L1d loads 9,157.7 -> 9,140.8; L1d misses 586.7 -> 578.9; cycles 31,316.4 -> 37,284.9 = x1.191; IPC 1.2376 -> 1.0384) were measured on a DIFFERENT host (`i4i.metal`), a DIFFERENT corpus (Corpus A, LZ4-compressed, 196.09 B/row) and a DIFFERENT arm (`do_get`, not bare scan). The two sets are not divided into each other and no ratio between them is computed. What IS comparable is the SHAPE: if the L1d figures here are also flat S=1->S=6, that is consistent with #3224's private-caches-untouched finding and locates whatever decay appears in rows/s away from the private hierarchy — narrowing it to the shared level this box cannot instrument. If they are NOT flat, that is a new result, and a more interesting one, because #3224's mechanism story assumed that flatness.

### Instrument provenance

**No LLC column exists anywhere above.** Every LLC instrument on this host is unavailable, so AC3 is DEFERRED, not approximated: a hard 0 from a dead counter would read as 'no misses'.

`unhalted Gcyc/CPU·s` is deliberately absent from the deliverable table for the same reason a CPU-utilisation column is: under CPU-wide counting `task-clock` is elapsed x ncpus by construction, so a utilisation derived from it cannot vary.

Counter-window agreement (max over reps): 4.13e-05 — perf's enabled interval versus the driver's [T0, T1]. The measured proof that counters and rows were taken over the SAME interval.

Max attribution shortfall over all reps: 0.0909% of the window (bound 0.50%). Rows are counted only between progress records the workers actually emitted, so this biases every rows/s figure DOWNWARD and every per-row counter UPWARD, by at most that fraction.

