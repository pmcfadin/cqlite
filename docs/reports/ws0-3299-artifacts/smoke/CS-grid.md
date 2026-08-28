## C(S, N) — bare-scan scaling grid, aligned window

Corpus: #3096 'Corpus B' (4,000,000 rows, 693.69 B/row, UNCOMPRESSED). Medians over reps; spread = (max-min)/median.

### C(N) per S, with dispersion

Aggregate rows/s (median), min-max spread as % of median in parentheses. Blank = not measured at that point.

| N | S=1 | S=6 |
|--:|---|---|
| 1 | 360,807 (0.0%) |  |
| 2 | **492,360** (0.0%) | 469,784 (0.0%) |
| 8 | 463,842 (0.0%) |  |
| 16 |  | **2,537,627** (0.0%) |

**bold** = that S's best-N point.

### Cross-S marginal efficiency — BOTH denominators

| S | best aggregate rows/s | N@peak | per-scan p50 rows/s | own N=1 | speedup vs **1-core peak** | **marg. eff. vs 1-core peak** | speedup vs 1-core N=1 | marg. eff. vs 1-core N=1 | cycles/row | instr/row | IPC | L1d loads/row | L1d miss/row |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 | 492,360 | 2 | 246,528 | 360,807 | 1.000 | **1.000** | 1.365 | 1.365 | 14,297.0 | 23,214.9 | 1.624 | 5,815.4 | 110.99 |
| 6 | 2,537,627 | 16 | 156,714 | n/m | 5.154 | **0.859** | 7.033 | 1.172 | 16,022.9 | 23,405.1 | 1.461 | 5,857.5 | 124.62 |

`marg. eff.` = speedup / S; 1.000 would be perfect scaling. **Reference B (S=1's own peak) is PRIMARY**: it is the most the engine achieves on one physical core, so it is the fair 'perfect scaling' unit, and it is the CONSERVATIVE choice — it yields lower efficiencies than A. Reference A (S=1 at N=1) is published alongside because it is the naive baseline. The `own N=1` column is why a self-normalised speedup is NOT published: each arm's own N=1 moves with S, so dividing by it would flatter the wide arms.

### Endpoints S=1 and S=6 — the L1d partial of the DEFERRED AC3

AC3 (LLC-load-misses/row, S=1 vs S=6) is **DEFERRED**: every LLC instrument on this box is unavailable (`../host/README.md`), and nothing below discharges it. But `L1-dcache-loads` and `L1-dcache-load-misses` ARE real here, and they are exactly the counters #3224 reported as flat across its endpoints — so the private-cache half of the question is answerable.

| per-row counter | S=1, N=2 | S=6, N=16 | ratio |
|---|--:|--:|--:|
| instructions/row | 23,214.9 | 23,405.1 | x1.008 |
| L1-dcache-loads/row | 5,815.4 | 5,857.5 | x1.007 |
| L1-dcache-load-misses/row | 110.99 | 124.62 | x1.123 |
| cycles/row | 14,297.0 | 16,022.9 | x1.121 |
| IPC | 1.6238 | 1.4607 | x0.900 |

**Read this as CONDITIONAL and CROSS-EVERYTHING.** #3224's endpoint figures (instructions 38,856.8 -> 38,685.6; L1d loads 9,157.7 -> 9,140.8; L1d misses 586.7 -> 578.9; cycles 31,316.4 -> 37,284.9 = x1.191; IPC 1.2376 -> 1.0384) were measured on a DIFFERENT host (`i4i.metal`), a DIFFERENT corpus (Corpus A, LZ4-compressed, 196.09 B/row) and a DIFFERENT arm (`do_get`, not bare scan). The two sets are not divided into each other and no ratio between them is computed. What IS comparable is the SHAPE: if the L1d figures here are also flat S=1->S=6, that is consistent with #3224's private-caches-untouched finding and locates whatever decay appears in rows/s away from the private hierarchy — narrowing it to the shared level this box cannot instrument. If they are NOT flat, that is a new result, and a more interesting one, because #3224's mechanism story assumed that flatness.

### Instrument provenance

**No LLC column exists anywhere above.** Every LLC instrument on this host is unavailable, so AC3 is DEFERRED, not approximated: a hard 0 from a dead counter would read as 'no misses'.

`unhalted Gcyc/CPU·s` is deliberately absent from the deliverable table for the same reason a CPU-utilisation column is: under CPU-wide counting `task-clock` is elapsed x ncpus by construction, so a utilisation derived from it cannot vary.

Counter-window agreement (max over reps): 1.69e-04 — perf's enabled interval versus the driver's [T0, T1]. The measured proof that counters and rows were taken over the SAME interval.

Max attribution shortfall over all reps: 0.2306% of the window (bound 0.50%). Rows are counted only between progress records the workers actually emitted, so this biases every rows/s figure DOWNWARD and every per-row counter UPWARD, by at most that fraction.

**WARNING — under-replicated points:** {'S=1,N=1': 1, 'S=1,N=2': 1, 'S=1,N=8': 1, 'S=6,N=2': 1, 'S=6,N=16': 1} have fewer than 3 reps. Their medians carry no usable dispersion (this is #3217's gap 1).

