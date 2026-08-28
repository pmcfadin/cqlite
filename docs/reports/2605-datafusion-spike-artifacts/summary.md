## Per-cell results (median of 3 iterations; [min, max])

| scenario | arm | wall s (median) | [min, max] | rows emitted/s | batches | encode ms | merge ms | decompress ms | cold-fault ms (sum over 2 producer threads) | peak RSS MiB | rows result |
|---|---|---|---|---|---|---|---|---|---|---|---|
| full_scan_count | floor | 124.1 | [99.9, 169.4] | 15308 | 1908 | 6118 | 30604 | 1722 | 156830 | 37.0 | 0 |
| full_scan_count | row_engine | 99.8 | [92.7, 140.3] | 19026 | 1908 | 5802 | 27648 | 1733 | 130130 | 36.8 | 1899750 |
| full_scan_count | datafusion@tp1 | 91.8 | [78.1, 95.9] | 20693 | 1908 | 5649 | 28087 | 1760 | 114299 | 48.3 | 1899750 |
| full_scan_count | datafusion@tp16 | 73.0 | [71.5, 119.0] | 26034 | 1908 | 5645 | 27977 | 1769 | 83578 | 48.8 | 1899750 |
| full_scan_count | row_pushdown | 88.6 | [59.2, 111.4] | 21451 | 1908 | 5809 | 29476 | 1831 | 97639 | 37.0 | 1899750 |
| projected_scan | floor | 81.0 | [66.7, 99.8] | 23458 | 1908 | 5484 | 27975 | 1806 | 97297 | 36.8 | 0 |
| projected_scan | row_engine | 92.1 | [81.9, 118.9] | 20621 | 1908 | 5734 | 28018 | 1752 | 120167 | 36.8 | 1899750 |
| projected_scan | datafusion@tp1 | 89.7 | [71.3, 102.9] | 21185 | 1908 | 5769 | 27762 | 1744 | 111877 | 47.2 | 1899750 |
| projected_scan | datafusion@tp16 | 61.8 | [61.0, 100.9] | 30749 | 1908 | 5578 | 28386 | 1824 | 64774 | 46.9 | 1899750 |
| projected_scan | row_pushdown | 56.7 | [48.3, 76.1] | 33494 | 232 | 788 | 22112 | 1469 | 67378 | 29.4 | 1899750 |
| filtered_scan | floor | 120.6 | [80.1, 129.7] | 15748 | 1908 | 5864 | 29786 | 1732 | 155382 | 36.8 | 0 |
| filtered_scan | row_engine | 93.2 | [72.3, 116.4] | 20375 | 1908 | 5772 | 27387 | 1744 | 122390 | 36.9 | 937602 |
| filtered_scan | datafusion@tp1 | 101.5 | [80.9, 108.9] | 18722 | 1908 | 5820 | 27332 | 1736 | 129061 | 50.1 | 937602 |
| filtered_scan | datafusion@tp16 | 98.9 | [72.4, 100.6] | 19213 | 1908 | 5837 | 27709 | 1777 | 128138 | 51.2 | 937602 |
| filtered_scan | row_pushdown | 84.4 | [57.4, 138.8] | 11113 | 942 | 3228 | 28566 | 1730 | 105482 | 36.7 | 937602 |

## Derived deltas

| scenario | floor s | row s | DF@tp1 s | DF@default s | pushdown s | vectorized-exec (row/DF@tp1) | concurrency (DF@tp1/DF@default) | pushdown vs floor | decode-to-column share of floor wall |
|---|---|---|---|---|---|---|---|---|
| full_scan_count | 124.1 | 99.8 | 91.8 | 73.0 | 88.6 | 1.09x | 1.26x | 1.40x | 4.9% |
| projected_scan | 81.0 | 92.1 | 89.7 | 61.8 | 56.7 | 1.03x | 1.45x | 1.43x | 6.8% |
| filtered_scan | 120.6 | 93.2 | 101.5 | 98.9 | 84.4 | 0.92x | 1.03x | 1.43x | 4.9% |

## Engine comparison with I/O controlled

- wall = 13.40 s + 0.696 x cold_fault_s  (R^2 = 0.980 over 45 runs)
- i.e. 98% of the wall-time variance across every run in this matrix is explained by page-in time ALONE

| scenario | arm | mean residual s (+ = slower than I/O predicts) | residual [min, max] |
|---|---|---|---|
| full_scan_count | floor | +2.1 | [-2.3, +7.0] |
| full_scan_count | row_engine | +0.2 | [-4.1, +4.5] |
| full_scan_count | datafusion@tp1 | -0.1 | [-2.3, +3.2] |
| full_scan_count | datafusion@tp16 | +0.4 | [-1.5, +1.4] |
| full_scan_count | row_pushdown | +2.7 | [-2.8, +7.2] |
| projected_scan | floor | -0.6 | [-2.8, +1.2] |
| projected_scan | row_engine | -2.5 | [-4.9, -0.4] |
| projected_scan | datafusion@tp1 | -0.6 | [-1.6, +1.2] |
| projected_scan | datafusion@tp16 | +1.0 | [-2.9, +3.3] |
| projected_scan | row_pushdown | -4.6 | [-8.5, -1.7] |
| filtered_scan | floor | +2.5 | [-0.9, +6.4] |
| filtered_scan | row_engine | +1.5 | [-5.3, +9.9] |
| filtered_scan | datafusion@tp1 | -0.9 | [-2.4, +1.4] |
| filtered_scan | datafusion@tp16 | -1.9 | [-3.7, +0.6] |
| filtered_scan | row_pushdown | +0.8 | [-2.4, +3.0] |

## Producer CPU sub-phases (the stable signal)

| bucket | median ms over all 45 runs | [min, max] | us/row at 1,899,750 rows |
|---|---|---|---|
| stream_encode (row->column transpose) | 5686 | [750, 6355] | 2.99 |
| stream_merge (merge + reconcile + row materialize) | 27971 | [21350, 33673] | 14.72 |
| stream_decompress (LZ4) | 1752 | [1460, 1891] | 0.92 |
| stream_cold_fault (page-in, 2 threads summed) | 114299 | [52612, 214181] | 60.17 |

## Preconditions (every run)

- runs: 45
- post-prune sources < 2: NONE
- merge arm NOT observed: NONE
- rows_scanned across the COMPARABLE arms (floor/row_engine/datafusion): [1899750]
- rows_scanned for row_pushdown (narrowed scan, expected to differ): [937602, 1899750]
- reconcile_entries range: 1899750..1899750
- peak RSS across ALL runs: 51.2 MiB max (B4 budget 512 MiB)
