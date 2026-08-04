# #3224 harness — what changed from #3217's, and why

This is #3217's harness with the host-specific parts rewritten. Every change below
was forced by a measurement on this box, not by preference.

## Run it

```bash
source docs/reports/ws0-3224-artifacts/harness/ws0env.sh
bash   docs/reports/ws0-3224-artifacts/run/run-all.sh          # both endpoints, 3 reps
python3 docs/reports/ws0-3224-artifacts/results/derive.py /data/ws0/results
```

`ws0env.sh` is **committed** — see gap 4 below.

## The four #3217 reproducibility gaps, and how each is closed

| # | #3217 gap | closed here by |
|---|---|---|
| 1 | `llc-*` counter captures were **reps=1**, so the headline IPC figures carry no dispersion | `run-all.sh` runs **3 reps** per endpoint; `derive.py` publishes min/median/max/spread and stamps a `WARNING` key if any endpoint has <3 |
| 2 | `rows/s` came from the **whole loadgen step** while counters came from a 20 s interior slice | both conventions measured per rep (ALIGNED + INTERIOR) and compared numerically — see below |
| 3 | driver `rc=` was **fabricated** (`echo "$(date) END rc=$?"` reports the rc of `date`) | every `rc` captured into a variable **immediately** after its command, before any other substitution; `capture-endpoint.sh` fails closed on any non-zero arm |
| 4 | `llc-run.sh` sourced `/data/ws0/ws0env.sh`, **never committed** — the exact staged-corpus path was unrecoverable | `harness/ws0env.sh` is committed, and derives `WT` rather than hardcoding it |

## Host-specific rewrites

### The core-set table — #3217's is wrong here, in two independent ways

| | #3217's host | this host |
|---|---|---|
| logical CPUs | 16 | 128 |
| physical cores | 8 | 64 |
| sockets / NUMA nodes | 1 / 1 | **2 / 2** |
| SMT sibling rule | `(c, c+8)` | **`(c, c+64)`** |

```
node0 = CPUs 0-31,64-95        node1 = CPUs 32-63,96-127
cpu0 -> {0,64}   cpu1 -> {1,65}   cpu2 -> {2,66}   ...
```

New table (physical-core indices mirror #3217's so the comparison keeps its shape):

| S | server CPUs | physical cores |
|--:|---|--:|
| 1 | `2,66` | 1 |
| 2 | `0,2,64,66` | 2 |
| 4 | `0-3,64-67` | 4 |
| 6 | `0-5,64-69` | 6 |
| — | client `8,9,72,73` (constant) | 2 |

### Why a NUMA check is NOT sufficient, and what actually catches the bug

`ws0_assert_cpuset_on_node` would **pass** #3217's `S=1 → "2,10"`: both CPUs are
inside node 0. But sysfs says `cpu2 → {2,66}` and `cpu10 → {10,74}`, so `"2,10"`
is **one thread of each of two different physical cores** — it would have measured
S=2 on half-populated pairs while labelling the point S=1. Its `S=6 → "0-5,8-13"`
is worse: one thread of each of **twelve** cores.

So the load-bearing guard is `ws0_assert_full_physical_cores`, which requires the
pinned set to be an exact union of **complete** SMT sibling groups and the group
count to equal the requested S. Negative-tested on this host — it rejects both of
#3217's sets with the diagnosis above, and accepts all four of the new ones.

`selftest.sh` cannot catch any of this: it verifies the topology *derivation*, not
the hardcoded table.

Also fixed: `ws0_verify_topology` hardcoded the string `"(c, c+8)"` and only tested
offset 8, so here it would have reported `NON-UNIFORM` — true but useless, and it
hid that the offset is uniform at a *different* value. It now derives the offset
and records per-node cpulists.

### NUMA binding (new — #3217's host had one node)

The server runs under `numactl --cpunodebind=0 --membind=0` inside `taskset`, so
page-cache and heap allocations cannot land on the far node. A mixed-NUMA
allocation is a plausible alternative explanation for an IPC delta and is excluded
**by construction**, not by argument. The uncore capture reports the per-socket
split so a far-socket component would be visible if one appeared anyway.

### The event set had to be SPLIT — the RUNBOOK's single group multiplexes

Measured (`perf stat -x,` field 5, enabled %) with all 11 events in one group:

```
cycles 79 | instructions 89 | LLC-loads 90 | LLC-load-misses 70
cache-references 80 | cache-misses 90 | L1-dcache-loads 90
L1-dcache-load-misses 90 | dTLB-load-misses 59 | branch-misses 69 | task-clock 100
```

Every count except `task-clock` was a **scaled estimate** — the headline IPC and
cycles/row included. Split into two 7-event groups, each verified at **100.00**:

- **A** `cycles, instructions, task-clock, LLC-loads, LLC-load-misses, cache-references, cache-misses`
- **B** `cycles, instructions, task-clock, L1-dcache-loads, L1-dcache-load-misses, dTLB-load-misses, branch-misses`

`cycles`/`instructions`/`task-clock` appear in **both** on purpose: the two groups
run over different loadgen steps, so requiring their IPC to agree is a symmetry
control across groups — the same role P2 plays in the positive control.
`derive.py` refuses any event under 99% enabled rather than publishing an estimate.

### Uncore: `--per-socket`, and do NOT re-apply the ×64

All 12 `uncore_imc_N` devices carry `cpumask=0,32` (CPU 0 is socket 0's proxy, CPU
32 is socket 1's), so they are **not** 12 per-socket instances — each counts on
both sockets and `-a` sums them. The split therefore needs `--per-socket`, which
inserts **two** leading fields, moving the enabled-% column from field 5 to
**field 7**. Reading field 5 there would silently parse `run_time` as a percentage.

perf reports `cas_count_*` already scaled to **MiB** — the ×64 B/cacheline
conversion the RUNBOOK specifies is applied *by perf*. Multiplying the MiB figure
by 64 again would overcount by 64×.

## The four arms per rep

| arm | what | why |
|---|---|---|
| a1 | ALIGNED, group A | **the primary numbers.** perf runs the loadgen as its own child, so the counted interval *is* the row-producing interval — numerator and denominator share one window by construction, no rate assumption |
| a2 | ALIGNED, group B | the attribution counters (L1d, dTLB, branch) |
| b | INTERIOR, group A | reproduces #3217's convention exactly (interior window, rate from the step) so the two can be compared |
| c | UNCORE | `cas_count_{read,write}` for DRAM bandwidth, separate invocation, `--per-socket` |

## Validity gates — all fail closed

A capture exits non-zero (and `run-all.sh` will redo it) unless **every** one holds:

- **occupancy**, per arm: `rows_total > 0`, an exact multiple of the corpus row
  count (whole scans only), `requests_error == 0`, `requests_unavailable == 0`,
  `requests_ok > 0`. The first smoke run returned **rc=0 on zero rows** because
  the old check asked `rows_total % corpus == 0` and `0 % 3999890 == 0`; the corpus
  had been staged flat and the server logged `discovered 0 tables across 0
  keyspaces` behind 2,258,606 `NotFound`s. That is the vacuous green CLAUDE.md
  forbids, and it is now impossible.
- **warmth verified, not assumed**: `/proc/<pid>/io` `read_bytes` delta over the
  whole capture must be **0**. `rchar`, `read_bytes` and `syscr` are three
  different layers, reported side by side and **never divided by one another**.
- **client saturation**: client-pinned-set utilisation ≤ 0.70. Above that the
  point measured the loadgen and must not be quoted as a server number.
- **sysctls re-asserted per capture**, not once per session — `perf_event_paranoid`
  and `kptr_restrict` revert on their own schedule, and a stale value surfaces
  later as a different-looking failure.
- **stage layout**: `common.sh` now searches for `*-Data.db` at any depth and dies
  if there is none. #3217's check accepted the flat layout that breaks discovery
  while warning on the correct `<keyspace>/<table>-<uuid>/` one.

## What is deliberately NOT here

`sweep.sh`, `profile-oncpu.sh`, `profile-offcpu.sh` and the off-CPU classifiers are
#3217's and are not re-run: this issue is **two endpoint points**, no new (S,N)
matrix and no re-litigation of the two verdicts #3217 closed. Widening the sweep
is a different issue.
