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
| A   | 347,783         | 29.85%        | 19,406              | 36.75%            | 1.4533       |
| B   | 345,790         | 4.24%         | 19,297              | 1.99%             | 1.4543       |
| C0  | 346,314         | 2.59%         | 19,420              | 0.40%             | 1.4529       |
| C   | 344,664         | 3.50%         | 19,387              | 0.60%             | 1.4552       |
| D   | 353,194         | 6.90%         | 19,353              | 4.33%             | 1.4456       |

**Control movement across arms: 0.63% on cycles/row.** The control is identical code on identical CPUs in every arm, so this is drift plus contamination and nothing else. Any treatment delta smaller than it is NOT READABLE.

## Layer 1 — the INVARIANT layer (cycles/row, IPC, ratio, cycles/row delta)

`ratio bare/flight` is rows/s(bare) / rows/s(flight) and `cycles/row delta` is flight - bare, both the rig's own definitions (`ws0_report.py`), both taken WITHIN a round and then medianed. A ratio above 1 means the bare scan is faster.

| arm | cycles/row (median) | cycles/row spread | IPC (median) | ratio bare/flight (median) | cycles/row delta (median) | paired Δcycles/row vs A | direction (cycles/row vs A) |
|-----|---------------------|-------------------|--------------|----------------------------|---------------------------|-------------------------|-----------------------------|
| A   | 23,560              | 1.56%             | 1.4645       | 1.3193x                    | +4,295 (+22.3%)           | baseline                | —                           |
| B   | 27,618              | 6.39%             | 1.3994       | 1.5610x                    | +8,411 (+43.8%)           | +17.23%                 | 3/3 up                      |
| C0  | 29,602              | 3.21%             | 1.3339       | 1.7383x                    | +10,251 (+53.0%)          | +25.65%                 | 3/3 up                      |
| C   | 13,728              | 0.61%             | 2.1096       | 0.8193x                    | -5,678 (-29.2%)           | -41.67%                 | 0/3 up                      |
| D   | 18,595              | 2.42%             | 1.5581       | 1.0231x                    | -758 (-3.9%)              | -21.33%                 | 0/3 up                      |

## Layer 2 — the ABSOLUTE layer (rows/s; no cross-session absolute is reusable)

| arm | rows/s (median) | rows/s spread | paired Δrows/s vs A | direction (rows/s vs A) | row denominator (median) |
|-----|-----------------|---------------|---------------------|-------------------------|--------------------------|
| A   | 262,180         | 0.55%         | baseline            | —                       | 12,000,000               |
| B   | 213,726         | 6.28%         | -18.48%             | 0/3 up                  | 12,000,000               |
| C0  | 197,741         | 1.93%         | -24.60%             | 0/3 up                  | 12,000,000               |
| C   | 424,436         | 1.96%         | +61.28%             | 3/3 up                  | 20,000,000               |
| D   | 340,621         | 3.83%         | +29.21%             | 3/3 up                  | 16,000,000               |

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
