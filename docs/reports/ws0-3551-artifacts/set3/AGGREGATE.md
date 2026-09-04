Rounds pairable: [1, 2, 3]

Configuration VALIDATED over every aggregated (round, arm): 15 session(s) = 3 pairable round(s) x 5 arm(s).

* per-arm TREATMENT stability, all of flight_server_cpus, flight_pin_mode, flight_allocator, flight_malloc_arena_max, counter_mode: identical in every round of each arm.
* CROSS-ARM invariants, all 7 of them (server_cpus, client_cpus, the corpus identity and every measured binary's sha256): identical in every session.
* the ADMISSION TRIPLE (max_concurrent_scans, max_concurrent_scans_source, available_parallelism): identical in every session.
* SCOPE: the aggregated sessions only. A round dropped as incomplete contributes to no figure below and is not examined.

## The drift control (bare scan — code-identical AND pin-identical in every arm)

Every figure is the MEDIAN over the pairable rounds; each `spread` is (max-min)/median over those same rounds, so it is a BETWEEN-ROUND spread and not a within-session one.

| arm | rows/s (median) | rows/s spread | cycles/row (median) | cycles/row spread | IPC (median) |
|-----|-----------------|---------------|---------------------|-------------------|--------------|
| A   | 351,599         | 6.05%         | 19,369              | 1.13%             | 1.4542       |
| B   | 350,805         | 2.91%         | 19,508              | 0.70%             | 1.4497       |
| C0  | 347,684         | 0.84%         | 19,477              | 0.80%             | 1.4562       |
| C   | 352,684         | 7.62%         | 19,348              | 2.63%             | 1.4542       |
| D   | 351,917         | 0.99%         | 19,274              | 1.32%             | 1.4514       |

**Control movement across arms: 1.21% on cycles/row.** The control is identical code on identical CPUs in every arm, so this is drift plus contamination and nothing else. Any treatment delta smaller than it is NOT READABLE.

## Layer 1 — the INVARIANT layer (cycles/row, IPC, ratio, cycles/row delta)

`ratio bare/flight` is rows/s(bare) / rows/s(flight) and `cycles/row delta` is flight - bare, both the rig's own definitions (`ws0_report.py`), both taken WITHIN a round and then medianed. A ratio above 1 means the bare scan is faster.

| arm | cycles/row (median) | cycles/row spread | IPC (median) | ratio bare/flight (median) | cycles/row delta (median) | paired Δcycles/row vs A | direction (cycles/row vs A) |
|-----|---------------------|-------------------|--------------|----------------------------|---------------------------|-------------------------|-----------------------------|
| A   | 23,417              | 1.48%             | 1.4707       | 1.3007x                    | +3,987 (+20.5%)           | baseline                | —                           |
| B   | 27,617              | 9.59%             | 1.3749       | 1.6351x                    | +8,110 (+41.6%)           | +18.48%                 | 3/3 up                      |
| C0  | 27,045              | 7.98%             | 1.4177       | 1.6444x                    | +7,525 (+38.6%)           | +16.02%                 | 3/3 up                      |
| C   | 13,739              | 0.98%             | 2.1055       | 0.8260x                    | -5,609 (-29.0%)           | -41.33%                 | 0/3 up                      |
| D   | 18,553              | 0.34%             | 1.5618       | 1.0227x                    | -753 (-3.9%)              | -20.77%                 | 0/3 up                      |

## Layer 2 — the ABSOLUTE layer (rows/s; no cross-session absolute is reusable)

| arm | rows/s (median) | rows/s spread | paired Δrows/s vs A | direction (rows/s vs A) | row denominator (median) |
|-----|-----------------|---------------|---------------------|-------------------------|--------------------------|
| A   | 264,059         | 2.82%         | baseline            | —                       | 12,000,000               |
| B   | 215,479         | 8.20%         | -20.01%             | 0/3 up                  | 12,000,000               |
| C0  | 211,439         | 6.32%         | -19.93%             | 0/3 up                  | 12,000,000               |
| C   | 425,589         | 2.94%         | +57.44%             | 3/3 up                  | 20,000,000               |
| D   | 345,023         | 0.89%         | +30.31%             | 3/3 up                  | 16,000,000               |

## Configuration, read back from each session's own recorded pinning

Read from round 1 of each arm and VERIFIED IDENTICAL in every aggregated round of that arm (see the validation above) — so this table describes the whole set and not just one round of it.

| arm | scan pin | flight pin | pin mode       | allocator | arena max                                                                                                                    | admission ceiling | counter mode                                                                                |
|-----|----------|------------|----------------|-----------|------------------------------------------------------------------------------------------------------------------------------|-------------------|---------------------------------------------------------------------------------------------|
| A   | 2,10     | 2,10       | siblings       | system    | not injected (MALLOC_ARENA_MAX is ABSENT from the server environment, which is deliberately NOT the same as setting it to 0) | 4                 | perf stat -C 2,10 for the bare-scan arm and -C 2,10 for the Flight arm (CPU-WIDE; never -p) |
| B   | 2,10     | 2,3        | distinct-cores | system    | not injected (MALLOC_ARENA_MAX is ABSENT from the server environment, which is deliberately NOT the same as setting it to 0) | 4                 | perf stat -C 2,10 for the bare-scan arm and -C 2,3 for the Flight arm (CPU-WIDE; never -p)  |
| C0  | 2,10     | 2,3        | distinct-cores | system    | MALLOC_ARENA_MAX=2 (injected into the flight SERVER process only)                                                            | 4                 | perf stat -C 2,10 for the bare-scan arm and -C 2,3 for the Flight arm (CPU-WIDE; never -p)  |
| C   | 2,10     | 2,3        | distinct-cores | jemalloc  | not injected (MALLOC_ARENA_MAX is ABSENT from the server environment, which is deliberately NOT the same as setting it to 0) | 4                 | perf stat -C 2,10 for the bare-scan arm and -C 2,3 for the Flight arm (CPU-WIDE; never -p)  |
| D   | 2,10     | 2,10       | siblings       | jemalloc  | not injected (MALLOC_ARENA_MAX is ABSENT from the server environment, which is deliberately NOT the same as setting it to 0) | 4                 | perf stat -C 2,10 for the bare-scan arm and -C 2,10 for the Flight arm (CPU-WIDE; never -p) |

Every figure above is rows/s AND cycles/row; **no CPU-share is reported** (#2877: a share shift with rows/s unmoved is a FAIL, not a win).
