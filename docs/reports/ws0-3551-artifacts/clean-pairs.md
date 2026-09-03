Sessions examined: 42 across 3 set(s); **31 clean**, 11 contaminated, 0 UNDERCOVERED, 0 UNOBSERVED, 0 CENSUS-UNUSABLE (an unobserved window is could-not-measure, never clean: the bound is the judge's own MAX_SAMPLE_GAP_S = 30s at a 10s cadence; a CENSUS-UNUSABLE one carries a sample whose census fields cannot be read at all, which is a third fact again and is never counted clean).

A pair is (baseline `A`, treatment) inside ONE round, with BOTH sessions clean. Method §3b step 4 differences within a round, so such a pair is valid regardless of any other round or arm.

| arm | clean pairs | median Δcycles/row | median Δrows/s | direction (rows/s) | worst pair-control | median IPC |
|---|--:|--:|--:|--:|--:|--:|
| B | 6 | +17.85% | -19.25% | 0/6 up | 1.92% | 1.3903 |
| C | 6 | -42.37% | +61.17% | 6/6 up | 2.41% | 2.1206 |
| C0 | 4 | +22.11% | -22.71% | 0/4 up | 1.80% | 1.3590 |
| D | 3 | -21.71% | +29.21% | 3/3 up | 0.70% | 1.5581 |

`worst pair-control` is the largest bare-scan disagreement inside any counted pair — identical code on identical CPUs, so it is that pair's own drift bound.

### Every counted pair, individually

| set | round | arm | Δcycles/row | Δrows/s | pair-control |
|---|--:|---|--:|--:|--:|
| set1 | 1 | B | +7.76% | -12.60% | 1.92% |
| set1 | 2 | B | +19.84% | -21.48% | 1.41% |
| set1 | 3 | B | +9.74% | -14.16% | 0.12% |
| set2 | 1 | B | +17.23% | -18.48% | 0.30% |
| set3 | 1 | B | +20.43% | -20.01% | 0.87% |
| set3 | 3 | B | +18.48% | -20.29% | 0.72% |
| set1 | 1 | C | -42.83% | +65.40% | 2.41% |
| set1 | 2 | C | -41.77% | +61.06% | 0.92% |
| set1 | 3 | C | -42.42% | +64.07% | 1.17% |
| set2 | 2 | C | -42.57% | +61.28% | 0.10% |
| set3 | 1 | C | -42.32% | +56.96% | 1.75% |
| set3 | 3 | C | -40.88% | +57.44% | 1.70% |
| set1 | 1 | C0 | +18.75% | -22.06% | 1.38% |
| set1 | 2 | C0 | +23.94% | -22.29% | 1.80% |
| set1 | 3 | C0 | +20.27% | -23.69% | 0.48% |
| set2 | 1 | C0 | +25.65% | -23.14% | 0.45% |
| set2 | 2 | D | -21.80% | +29.21% | 0.27% |
| set3 | 1 | D | -21.71% | +30.31% | 0.33% |
| set3 | 3 | D | -20.28% | +28.44% | 0.70% |
