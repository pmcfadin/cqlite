# Extension B — the run that RESOLVED S=6's peak

**Valid data.** Grid `6:24,32`, 3 reps each, 60 s aligned window, same guards and
containment as the main grid.

**Why it exists and why its design matters.** The incumbent **N=24 is
re-measured INTERLEAVED with the candidate N=32 in every round**, so the
bracketing comparison is between points measured minutes apart rather than hours.
That is not a nicety: this session drifts (§6 of the report), and comparing a
late candidate against an early incumbent would let drift decide the very
question the run exists to answer — in the direction that inflates the peak and
therefore AC2's target.

| N | reps | median rows/s | spread |
|--:|--:|--:|--:|
| 24 | 3 | 2,705,485 | 0.64% |
| 32 | 3 | 2,652,863 | 0.67% |

**Verdict: BRACKETED at N=24.** N=32 is 1.95% below N=24, exceeding the larger
spread, so the curve has turned over. With the main grid's N=16 below it too,
S=6 is a clean interior maximum.

**Its medians are NOT pooled with the main grid's.** They are a different
session; pooling would average across a drift epoch. The main grid remains the
campaign of record and the source of the headline figure; this run supplies the
bracketing verdict (which requires contemporaneity) and an independent
confirmation at the AC2 configuration (−1.0%, ~2 h later).
