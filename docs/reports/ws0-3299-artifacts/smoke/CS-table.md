## C(S) — bare-scan scaling curve, aligned window

Corpus: #3096 'Corpus B' (4,000,000 rows, 693.69 B/row, UNCOMPRESSED). Reps per point: [(1, 1), (2, 1)]. Medians; spread = (max-min)/median.

| S | aggregate rows/s (median) | spread | per-scan p50 rows/s | marg. eff. vs S=1 | cycles/row | instr/row | IPC | L1d miss/row | unhalted Gcyc/CPU·s |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 | 360,570 | 0.0% | 361,998 | **1.000** | 18,949.5 | 27,079.2 | 1.429 | 182.17 | 3.416 |
| 2 | 709,123 | 0.0% | 355,697 | **0.983** | 19,197.4 | 27,338.8 | 1.424 | 187.28 | 3.403 |

`marg. eff. vs S=1` = (aggregate rows/s at S ÷ S) ÷ (aggregate rows/s at S=1). 1.000 would be perfect scaling.

**No LLC column exists.** Every LLC instrument on this host is unavailable (`../host/README.md`), so AC3 is DEFERRED, not approximated: a hard 0 from a dead counter would read as 'no misses'.

`unhalted Gcyc/CPU·s` is unhalted cycles per pinned logical-CPU-second. It is NOT a utilisation percentage and NOT a reported clock: it conflates occupancy with frequency, and is shown so a collapse in either is visible.

Counter-window agreement (max over reps): 1.96e-05 — perf's enabled interval versus the driver's [T0, T1]. This is the measured proof that the counters and the rows were taken over the SAME interval.

Max attribution shortfall over all reps: 0.3945% of the window (bound 0.50%). Rows are only counted between progress records the workers actually emitted, so this biases every rows/s figure DOWNWARD by at most that fraction.

**WARNING — under-replicated points:** {1: 1, 2: 1} have fewer than 3 reps. Their medians carry no usable dispersion (this is #3217's gap 1).

