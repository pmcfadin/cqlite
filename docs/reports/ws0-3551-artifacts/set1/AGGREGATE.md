Rounds pairable: [1, 2, 3]

Configuration VALIDATED over every aggregated (round, arm): 12 session(s) = 3 pairable round(s) x 4 arm(s).

* per-arm TREATMENT stability, all of flight_server_cpus, flight_pin_mode, flight_allocator, flight_malloc_arena_max, counter_mode: identical in every round of each arm.
* CROSS-ARM invariants, all 7 of them (server_cpus, client_cpus, the corpus identity and every measured binary's sha256): identical in every session.
* the ADMISSION TRIPLE (max_concurrent_scans, max_concurrent_scans_source, available_parallelism): identical in every session.
* SCOPE: the aggregated sessions only. A round dropped as incomplete contributes to no figure below and is not examined.

## The drift control (bare scan — code-identical AND pin-identical in every arm)

Every figure is the MEDIAN over the pairable rounds; each `spread` is (max-min)/median over those same rounds, so it is a BETWEEN-ROUND spread and not a within-session one.

| arm | rows/s (median) | rows/s spread | cycles/row (median) | cycles/row spread | IPC (median) |
|-----|-----------------|---------------|---------------------|-------------------|--------------|
| A   | 351,808         | 0.56%         | 19,442              | 3.06%             | 1.4508       |
| B   | 353,011         | 0.89%         | 19,328              | 0.70%             | 1.4512       |
| C0  | 353,413         | 1.14%         | 19,225              | 0.74%             | 1.4493       |
| C   | 342,913         | 2.01%         | 19,379              | 1.07%             | 1.4570       |

**Control movement across arms: 1.12% on cycles/row.** The control is identical code on identical CPUs in every arm, so this is drift plus contamination and nothing else. Any treatment delta smaller than it is NOT READABLE.

## Layer 1 — the INVARIANT layer (cycles/row, IPC, ratio, cycles/row delta)

`ratio bare/flight` is rows/s(bare) / rows/s(flight) and `cycles/row delta` is flight - bare, both the rig's own definitions (`ws0_report.py`), both taken WITHIN a round and then medianed. A ratio above 1 means the bare scan is faster.

| arm | cycles/row (median) | cycles/row spread | IPC (median) | ratio bare/flight (median) | cycles/row delta (median) | paired Δcycles/row vs A | direction (cycles/row vs A) |
|-----|---------------------|-------------------|--------------|----------------------------|---------------------------|-------------------------|-----------------------------|
| A   | 23,476              | 1.93%             | 1.4721       | 1.3149x                    | +4,035 (+20.8%)           | baseline                | —                           |
| B   | 25,764              | 9.07%             | 1.4577       | 1.5529x                    | +6,370 (+33.0%)           | +9.74%                  | 3/3 up                      |
| C0  | 28,318              | 2.68%             | 1.3664       | 1.7221x                    | +9,093 (+47.3%)           | +20.27%                 | 3/3 up                      |
| C   | 13,621              | 0.85%             | 2.1208       | 0.7947x                    | -5,758 (-29.7%)           | -42.42%                 | 0/3 up                      |

## Layer 2 — the ABSOLUTE layer (rows/s; no cross-session absolute is reusable)

| arm | rows/s (median) | rows/s spread | paired Δrows/s vs A | direction (rows/s vs A) | row denominator (median) |
|-----|-----------------|---------------|---------------------|-------------------------|--------------------------|
| A   | 266,415         | 2.97%         | baseline            | —                       | 12,000,000               |
| B   | 227,824         | 7.81%         | -14.16%             | 0/3 up                  | 12,000,000               |
| C0  | 203,302         | 2.73%         | -22.29%             | 0/3 up                  | 12,000,000               |
| C   | 432,580         | 1.37%         | +64.07%             | 3/3 up                  | 20,000,000               |

## Configuration, read back from each session's own recorded pinning

Read from round 1 of each arm and VERIFIED IDENTICAL in every aggregated round of that arm (see the validation above) — so this table describes the whole set and not just one round of it.

| arm | scan pin | flight pin | pin mode       | allocator | arena max                                                                                                                    | admission ceiling | counter mode                                                                                |
|-----|----------|------------|----------------|-----------|------------------------------------------------------------------------------------------------------------------------------|-------------------|---------------------------------------------------------------------------------------------|
| A   | 2,10     | 2,10       | siblings       | system    | not injected (MALLOC_ARENA_MAX is ABSENT from the server environment, which is deliberately NOT the same as setting it to 0) | 4                 | perf stat -C 2,10 for the bare-scan arm and -C 2,10 for the Flight arm (CPU-WIDE; never -p) |
| B   | 2,10     | 2,3        | distinct-cores | system    | not injected (MALLOC_ARENA_MAX is ABSENT from the server environment, which is deliberately NOT the same as setting it to 0) | 4                 | perf stat -C 2,10 for the bare-scan arm and -C 2,3 for the Flight arm (CPU-WIDE; never -p)  |
| C0  | 2,10     | 2,3        | distinct-cores | system    | MALLOC_ARENA_MAX=2 (injected into the flight SERVER process only)                                                            | 4                 | perf stat -C 2,10 for the bare-scan arm and -C 2,3 for the Flight arm (CPU-WIDE; never -p)  |
| C   | 2,10     | 2,3        | distinct-cores | jemalloc  | not injected (MALLOC_ARENA_MAX is ABSENT from the server environment, which is deliberately NOT the same as setting it to 0) | 4                 | perf stat -C 2,10 for the bare-scan arm and -C 2,3 for the Flight arm (CPU-WIDE; never -p)  |

Every figure above is rows/s AND cycles/row; **no CPU-share is reported** (#2877: a share shift with rows/s unmoved is a FAIL, not a win).
