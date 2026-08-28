# #3299 host census — what this box can and cannot measure

Everything here was measured **on this box** (`i-04ac0a860eef7f241`, `c7i.4xlarge`) on
2026-08-27. A prior session's census ran on a *different* instance; that is not
transferable evidence, so it was re-run here. Reproduce with `bash census.sh`.

| file | what |
|---|---|
| `pmu-census.txt` | every event spelling the issue names, with value + `pct_running` + verbatim perf row |
| `census.sh` | the census, including its positive control |
| `classify-event.sh` | the verdict rule (`REAL` / `HARD-ZERO` / …) census.sh sources, so `../harness/selftest.sh` can drive every branch hermetically |
| `lscpu.txt` | CPU model / cache sizes |
| `thread-siblings.txt` | `thread_siblings_list` read from sysfs — the pinning ground truth |
| `sysfs-pmus.txt` | `/sys/bus/event_source/devices` — which PMUs exist at all |
| `resctrl-availability.txt` | `/sys/fs/resctrl` probe (E1 `llc_occupancy`) |

## Topology (read from sysfs, never assumed)

16 logical / **8 physical** cores, one socket, **1 NUMA node** (`node0: 0-15`), SMT
sibling rule **`(c, c+8)`** — pairs `0,8 1,9 2,10 3,11 4,12 5,13 6,14 7,15`. So the 8
physical cores are addressed by logical CPUs 0..7, each pinning taking BOTH siblings.
`nproc`=16 is the LOGICAL count; pinning one logical CPU per pair would half-populate
a core and silently halve the per-core figure.

## The verdict: AC3 is NOT ANSWERABLE on this box

AC3 wants LLC-load-misses/row at S=1 vs S=6. **No LLC instrument on this host
produces a count.** The decisive lines from `pmu-census.txt`:

```
instructions                     REAL                     4137979348     100.00
cycles                           REAL                    22227766667     100.00
L1-dcache-loads                  REAL                      786172946     100.00
L1-dcache-load-misses            REAL                      120303664     100.00
LLC-loads                        NOT-SUPPORTED       <not supported>     100.00
LLC-load-misses                  NOT-SUPPORTED       <not supported>     100.00
cache-references                 HARD-ZERO                         0     100.00
cache-misses                     HARD-ZERO                         0     100.00
mem_load_retired.l3_miss         HARD-ZERO                         0     100.00
mem_load_retired.l3_hit          HARD-ZERO                         0     100.00
longest_lat_cache.miss           HARD-ZERO                         0     100.00
longest_lat_cache.reference      HARD-ZERO                         0     100.00
r4f2e                            HARD-ZERO                         0     100.00
r412e                            HARD-ZERO                         0     100.00
```

**Why a `0` here is UNAVAILABLE and not a measurement.** The census workload is
#3224's `cache-hostile`: a serial-dependency pointer chase over a **2 GiB** buffer,
randomly permuted at 64 B granularity, on a host whose LLC is far smaller. Essentially
every load must reach DRAM. In the very same runs the core PMU counted **120,303,664
L1-dcache-load-misses** and 4.1 G instructions at `100.00%` enabled — so the PMU works,
the workload genuinely misses cache, and "zero L3 references" is physically impossible.
A counter that programs cleanly, reports `100.00% pct_running`, and returns a hard `0`
under those conditions is an **unavailable instrument**. Publishing it as `0 misses/row`
is precisely the silent-instrument failure #3217 shipped and #3224 catalogued, so this
census reports it as unavailable and the S-sweep harness refuses to record it at all.

**No second route exists on this host either:**

- **Uncore/IMC**: `/sys/bus/event_source/devices` holds only `breakpoint, cpu, kprobe,
  msr, software, tracepoint, uprobe` — there is **no `uncore_imc_*`, no `uncore_cha_*`**.
  This is a virtualized guest; the uncore PMUs are not exposed. So the DRAM-traffic
  substitute #3224 used on bare metal is not available.
- **resctrl / E1 `llc_occupancy`**: `/sys/fs/resctrl` absent, `resctrl` not in
  `/proc/filesystems`, mount refused, no RDT flags in `/proc/cpuinfo`. **UNAVAILABLE.**

## Consequence, per the issue's own pre-registered AC5

AC1 (the C(S) curve) and AC2 (marginal efficiency) rest on `instructions`, `cycles`,
`L1-dcache-loads`, `L1-dcache-load-misses` and wall-clock rows/s — **all REAL at
100.00%** — so they proceed unchanged. **AC3 is explicitly DEFERRED as unmeasurable on
this host**, which is the sanctioned path AC5 pre-registers, not a discretionary call.
It is deferred, never approximated: no L1-miss figure is offered as a stand-in for an
L3 figure, and no `0` from a dead counter appears anywhere in the results.
