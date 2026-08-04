# The microarchitectural cause of the full-box IPC decay — CQLite #3224

**Measured 2026-08-04 on one AWS `i4i.metal`** (Intel Xeon Platinum 8375C @ 2.90 GHz, Ice Lake-SP,
**2 sockets, 64 physical / 128 logical cores, SMT on, 2 NUMA nodes**, 1,007 GiB RAM, 295 GB
instance-store NVMe at `/data`), Ubuntu kernel `6.17.0-1019-aws`, `perf` 6.17.13. CQLite built from
this branch, descending from `main` @ **`7e47282`**.

This issue exists because #3217 established that CQLite's full-box scaling shortfall **is** IPC
decay — instructions/row flat, cycles/row +34% between the 1-core and 6-core peaks — and then could
not name the cause: `LLC-loads`, `LLC-load-misses` and `cache-references` all read
`<not supported>` on its virtualized host. This run re-measures **both** of #3217's endpoint points
on a host where those counters actually program, and produces a cycles-per-row accounting that
either attributes the decay or **states plainly what fraction remains unattributed**.

Raw artefacts: `docs/reports/ws0-3224-artifacts/`. **Measurement only — no production code was
changed and no fix is proposed here.** Anything the data indicts is groomed as a separate follow-up,
as #3217 did.

> **Cross-host comparison rule.** #3217's host was a single-socket *virtualized* Sapphire Rapids
> guest; this is a two-socket *bare-metal* Ice Lake-SP. Different microarchitecture, different memory
> topology, and NUMA is new. **No absolute measured here is compared against a #3217 absolute.** The
> design is that *both* endpoints are re-measured on this box, and that self-consistent pair carries
> the mechanism claim. Where a #3217 figure appears below it is labelled **context only**.

---

## 1. AC1 — host capability, VERIFIED before the run

The three counters that read `<not supported>` on #3217's host all program here. From
`ws0-3224-artifacts/host/ac1-capability-probe.txt`, committed **before** any measurement (commit
`4b2bc33`) so AC1 is discharged independently of everything that follows:

| event | count on `true` |
|---|--:|
| `LLC-load-misses` | 104 (7.69% of LL-cache accesses) |
| `LLC-loads` | 1,352 |
| `cache-references` | 14,451 |
| `cycles` | 620,942 |
| `instructions` | 689,021 (IPC 1.11) |

`ls /sys/bus/event_source/devices/` — the authoritative test, never `perf list`, which enumerates
per-model JSON event-table entries and returns non-zero counts on hosts with no uncore PMU at all —
shows **88 uncore devices**, including `uncore_imc_0..11`, `uncore_imc_free_running_0..3`,
`uncore_cha_0..35`, `uncore_m2m_*` and `uncore_upi_*`.

**`perf stat -M MemoryBandwidth` is NOT available on this perf build** (`Cannot find metric or group
'MemoryBandwidth'`). Recorded as a capability fact rather than silently omitted. AC3 requires the
bandwidth figure's source to be named explicitly; it comes from
`uncore_imc_*/cas_count_{read,write}/` throughout, and the `-M` cross-check the RUNBOOK offers as an
option is unavailable here.

---

## 2. The positive control — PASS, on the second run

Owner condition 3 required the LLC counters to be programmed against a known-cache-hostile
microbenchmark and shown to move in the predicted direction before any real measurement, because the
dominant failure mode of this program is **a broken instrument that does not error but emits
plausible output** — on #3217's host `cache-references` programmed cleanly and returned a hard 0 over
40–240 CPU-seconds of memory-heavy work.

**Final verdict: `RESULT: PASS`**, exit 0
(`ws0-3224-artifacts/positive-control-run/{summary.txt,verdict.json}`):

| check | threshold | measured | |
|---|---|---|---|
| P1 hostility (cycles/access, hostile:friendly) | ≥ 5× | **29.209×** | PASS |
| P2 symmetry (instructions/access) | within ±10% | **1.000×** | PASS |
| P4 LLC miss-rate rise (misses/loads) | ≥ 1.5× | **9.818×** (0.055 → 0.540) | PASS |
| P3 `LLC-loads` movement, either direction | ≥ 2× | 18 → 36,986,968 | OK |
| P5 `cache-references` movement, either direction | ≥ 2× | 46 → 36,987,457 | OK |
| `LLC-load-misses` movement | ≥ 2× | 1 → 20,000,591 | OK |
| gate integrity (control-FIFO window really closed) | ≤ 1e6 instr / 1000 accesses | 6,298 | PASS |

### 2.1 The first run FAILed, and the instrument was at fault — not the host

Run 1 reported `RESULT: FAIL — 2 of 3 required counters unusable`, diagnosing `LLC-loads` and
`LLC-load-misses` as `ABSENT_EVENT_NAME`. **That diagnosis was false**, and it was falsifiable
against evidence committed *before* the control ran.

**Root cause.** `perf` 6.17.13 echoes the `-x,` event-name field **with** the `:u` modifier for some
events and **without** it for others:

| requested | field-3 name printed back | modifier |
|---|---|---|
| `cycles:u`, `instructions:u`, `cache-references:u`, `cache-misses:u` | unchanged | retained |
| `LLC-loads:u` | `LLC-loads` | **stripped** |
| `LLC-load-misses:u` | `LLC-load-misses` | **stripped** |

The script matched only the requested form, so for exactly the two counters this issue exists to read
the match failed and it declared the event absent — while field 1 held a valid count
(`455,,LLC-loads,218387,100.00`). `cell()`/`mux()` carried the same bug on the measurement path, so a
corrected probe alone would have produced a second, differently-shaped false failure downstream.

**Why proceeding was correct rather than a workaround.** The RUNBOOK's FAIL semantics concern *host
capability*. This FAIL was contradicted by a pre-registered, independently committed artefact (§1),
and the fix changed **no threshold and no gate**. Had the AC1 probe agreed with the control, the
correct action would have been to stop and close the issue BLOCKED. It did not agree, so the
instrument was at fault. The gate's teeth were re-verified on this host afterwards:

| condition | probe result | diagnosis still fires? |
|---|---|---|
| bogus name (`not_a_real_event_xyz:u`) | no CSV row at all | yes → `ABSENT_EVENT_NAME` |
| unsupported event (`LLC-prefetches:u`) | `<not supported>` in field 1 | yes → `NOT_SUPPORTED` |
| working counter | real count | `PROGRAMS` |

`NOT_SUPPORTED` is in fact now *reachable* for the two LLC events, where before it was unreachable —
they short-circuited to `ABSENT_EVENT_NAME` first.

**This is the second way this control reds a healthy box.** Its first draft asserted that raw
`LLC-loads` must *rise* in the hostile arm, which would have FAILed all three counters on a good host
(the prefetcher stops issuing loads it cannot predict, so raw load count *falls* while the miss rate
rises); that was caught in pre-run review. This one is a string-matching bug. Two independent
instances in one script argue the lesson is structural: **a control must be validated against a
known-good host, not only reasoned about.** Cost here: one wasted 3-rep gate run, ~4 metered minutes.
The FAILed run is committed as evidence in
`positive-control-run-FAILED-instrument-bug/` rather than deleted.

---

## 3. Method

### 3.1 Corpus — regenerated here, geometry matched exactly

Generated from the committed recipe `ws0-3026-artifacts/ws0-corpus/gen-corpus.sh`, invoked exactly as
`rerun.sh` does: `gen-corpus.sh 200000 375 6 96 3 96 16 2 10 8 50000`, on Cassandra 5.0.8
(`archive.apache.org`, sha256 `1579d7d3…b9a8f`) under OpenJDK 17.0.19 with `MAX_HEAP_SIZE=8G`.

The `cassandra.yaml` delta was applied with `patch(1)` and is **provably** #3026's: `diff <(diff
stock patched) cassandra.yaml.diff` is empty. Nothing performance-relevant changed — demonstrated,
not asserted.

| metric | RUNBOOK target | #3026 committed | **measured here** | Δ vs #3026 | oracle |
|---|--:|--:|--:|--:|---|
| rows | 3,999,890 | 3,999,890 | **3,999,890** | **exact** | `sstablemetadata totalRows` |
| rows (independent) | — | 3,999,890 | **3,999,890** | **exact** | `fullscan.py 512` |
| `totalColumnsSet` | 35,999,010 | 35,999,010 | **35,999,010** | **exact** | `sstablemetadata` |
| logical B/row | 693.29 | 692.70 | **692.58** | −0.017% | `dataLength` ÷ rows |
| on-disk B/row | 196.09 | 195.96 | **195.94** | −0.010% | `Data.db` ÷ rows |
| compression ratio | — | 3.5350× | **3.5346×** | −0.011% | derived |
| SSTables / format | 1 / `nb-16-big` | 1 / `nb-16-big` | **1 / `nb-16-big`** | match | `ls` + `sstablemetadata` |
| droppable tombstones | 0.0 | 0.0 | **0.0** | match | `sstablemetadata` |

**New `sha256(Data.db)` = `b1656ae8c0e45feb30f3da641b8a23c4969d1be43e5f341ef0af6bb3a9b41042`.**
`cassandra-stress` is not byte-deterministic, so the accepted bar — the one #3100 and #3217 both used
— is matched geometry **plus** a documented new sha256. Both halves are discharged.

**`now`-pinning: N/A, and recorded as such** (AC6). `sstablemetadata` reports no tombstones (min/max
local deletion time `9223372036854775807`) and TTL 0/0, so no read-time reconciliation depends on
`now` and there is nothing to pin.

*Documentation note, not a defect in this run:* the RUNBOOK's prose table gives 693.29 / 196.09 while
#3026's own committed `corpus-geometry.txt` records 692.70 / 195.96 for the same corpus — the two
source documents disagreed by ~0.09% before this run measured anything. The values here match the
committed artefact, which is the better authority.

### 3.2 Core allocation — physical-core basis, node-0-confined, and #3217's table is wrong here

| | #3217's host | this host |
|---|---|---|
| logical / physical | 16 / 8 | 128 / 64 |
| sockets / NUMA nodes | 1 / 1 | **2 / 2** |
| SMT sibling rule | `(c, c+8)` | **`(c, c+64)`** |

```
node0 = CPUs 0-31,64-95        node1 = CPUs 32-63,96-127
cpu0 -> {0,64}    cpu1 -> {1,65}    cpu2 -> {2,66}   ...
```

| point | S | server CPUs | physical cores | hw threads |
|---|--:|---|--:|--:|
| `llc-s1-N2` | 1 | `2,66` | 1 | 2 |
| `llc-s6-N16` | 6 | `0-5,64-69` | 6 | 12 |
| client (constant, both points) | — | `8,9,72,73` | 2 | 4 |

Every set is inside **NUMA node 0**, and the server additionally runs under `numactl
--cpunodebind=0 --membind=0` so page-cache and heap allocations cannot land on the far node. A
mixed-NUMA allocation is a plausible alternative explanation for an IPC delta and is excluded **by
construction**, not by argument; the uncore capture reports the per-socket split so a far-socket
component would still be visible if one appeared.

**#3217's hardcoded table would have silently measured a different machine than it labelled.** Its
`S=1 → "2,10"` is entirely inside node 0 and passes a NUMA check, but sysfs says `cpu2 → {2,66}` and
`cpu10 → {10,74}`: `"2,10"` is **one thread of each of two different physical cores**, i.e. S=2 on
half-populated pairs labelled S=1. Its `S=6 → "0-5,8-13"` is one thread of each of **twelve** cores.
`selftest.sh` cannot catch this — it verifies the topology *derivation*, not the table. The guard
that does catch it, `ws0_assert_full_physical_cores`, requires the pinned set to be an exact union of
**complete** SMT sibling groups with group count equal to the requested S, and is negative-tested
against both of #3217's sets.

### 3.3 The core event set had to be SPLIT — the RUNBOOK's single group multiplexes here

With all 11 events in one group, `perf stat -x,` field 5 (enabled %) read:

```
cycles 79 | instructions 89 | LLC-loads 90 | LLC-load-misses 70 | cache-references 80
cache-misses 90 | L1-dcache-loads 90 | L1-dcache-load-misses 90 | dTLB-load-misses 59
branch-misses 69 | task-clock 100
```

Every count except `task-clock` was a **multiplexed scaled estimate** — the headline IPC and
cycles/row included. RUNBOOK step 6 requires exactly this response: split rather than publish scaled
values. Two 7-event groups, each verified at **100.00** enabled:

- **A** `cycles, instructions, task-clock, LLC-loads, LLC-load-misses, cache-references, cache-misses`
- **B** `cycles, instructions, task-clock, L1-dcache-loads, L1-dcache-load-misses, dTLB-load-misses, branch-misses`

`cycles`/`instructions`/`task-clock` appear in both deliberately: the groups run over different
loadgen steps, so requiring their IPC to agree is a cross-group symmetry control — the role P2 plays
inside the positive control. `derive.py` **refuses** any event under 99% enabled rather than
publishing an estimate. Before/after CSVs are committed as
`run/multiplexing-evidence-{before,after}-split.csv`.

### 3.4 Uncore: `--per-socket`, and the ×64 must NOT be re-applied

All 12 `uncore_imc_N` devices carry `cpumask=0,32` — CPU 0 is socket 0's proxy, CPU 32 is socket 1's.
They are therefore **not** 12 per-socket instances: each counts on both sockets and `perf stat -a`
sums them, hiding the split. The per-socket split needs `--per-socket`, which inserts **two** leading
fields and moves the enabled-% column from field 5 to **field 7**; reading field 5 there would
silently parse `run_time` as a percentage.

perf reports `cas_count_*` **already scaled to MiB** — the ×64 B/cacheline conversion the RUNBOOK
specifies is applied by perf. Multiplying the MiB figure by 64 again would overcount by 64×.

### 3.5 The denominator convention — #3217's open method question, settled with data

#3217 computed `cycles/row = counter ÷ (rows_per_s × window_secs)` where `rows_per_s` came from the
**whole loadgen step** but the counters came from a **20 s interior slice**. At `llc-s1-N2` that step
held 4 completed requests over 63.99 s. The concern is real and it lands on the **baseline** of the
+8,593 cycles/row delta.

Calibration on this host surfaced the property that decides it, and which #3217's report never
stated: **the loadgen holds a step open until in-flight requests drain.** A 120 s requested step
returned `duration_s = 144.205 s` with `requests_ok = 8` and `rows_total = 31,999,120` — *exactly*
8 × 3,999,890. Consequences:

1. **A step contains only WHOLE scans.** No partial-scan row credit, so no truncation bias.
2. **Occupancy is ~99.5%** (8 × 35.88 s ÷ 2 workers = 143.5 s against a 144.2 s step). The workers
   are never idle, so the step-average rate *is* the steady-state rate — which is precisely the
   condition #3217's interior-window convention needs in order to be valid.

Note also that counting "rows completed inside the window" — the naive fix — would be **worse**, not
better: rows are credited in lumps when a request completes while cycles accrue continuously, so at
S=1/N=2 a 20 s window can contain 0, 1 or 2 completions and imply a rate off by 2× either way.

So rather than assume, both conventions are measured **per rep** and compared:

| convention | counters over | rows denominator |
|---|---|---|
| **ALIGNED** (primary) | the **whole** loadgen step — perf runs the loadgen as its own child | that same step's `rows_total`; numerator and denominator share one interval **by construction** |
| **INTERIOR** (#3217's) | an interior `window_secs` slice, starting `settle_secs` in | `rows_per_s` (whole step) × `window_secs` |

§4.3 reports whether they agree. If they diverge, the +8,593 target moves and that is a finding.

### 3.6 Validity gates — all fail closed

Per capture, per arm; a failure exits non-zero and `run-all.sh` redoes the rep:

- **occupancy**: `rows_total > 0`, an exact multiple of 3,999,890 (whole scans only),
  `requests_error == 0`, `requests_unavailable == 0`, `requests_ok > 0`.
- **warmth verified, not assumed**: `/proc/<pid>/io` `read_bytes` delta over the capture must be
  **0**. `rchar`, `read_bytes` and `syscr` are three different layers, reported side by side and
  **never divided by one another**.
- **client saturation**: client-pinned-set utilisation ≤ 0.70; above that the point measured the
  loadgen, not the engine.
- **sysctls re-asserted per capture**, not once per session — `perf_event_paranoid` and
  `kptr_restrict` were found at 4/1 on this fresh box (#3249's fix is **not** in the golden AMI and
  does not survive a reboot) and revert on their own schedule.
- **multiplexing**: any event under 99% enabled is refused by the derivation.

**One vacuous-pass bug was caught by the smoke test and fixed before any real capture.** The first
smoke run returned **rc=0 while measuring nothing**: the corpus had been staged flat, the server
logged `discovered 0 tables across 0 keyspaces`, every request returned `NotFound` (2,258,606 of
them), and the occupancy check passed because it asked `rows_total % corpus == 0` and
`0 % 3999890 == 0`. That is exactly the empty-dataset green CLAUDE.md forbids. Both the staging and
the check were fixed; the corpus now lives at
`sstables/ws0/events-52ff1a008fa211f1ac2485829b296e3f/` with `sha256(Data.db)` re-verified unchanged
after the move.

### 3.7 Binaries, and #3217's fabricated-`rc` bug

```
CARGO_PROFILE_RELEASE_STRIP=none CARGO_PROFILE_RELEASE_DEBUG=true \
  RUSTFLAGS="-C force-frame-pointers=yes" \
  cargo build --release -p cqlite-flight -p flight-loadgen
```

(`[profile.release]` hardcodes `strip = true`, hence the env overrides.) Server flags are #3100's
recorded invocation verbatim, `--batch-size 8192` among them as AC2 requires:
`--batch-size 8192 --max-batch-bytes 4194304 --max-inflight-egress-bytes 12582912
--max-concurrent-scans 16 --admission-wait-timeout-ms 30000`, with
`CQLITE_FLIGHT_MERGE_PATH=bypass` (warm).

Every `rc` in every driver is captured into a variable **immediately** after its command, before any
other command substitution. #3217's drivers did `echo "$(date) END rc=$?"`, where the substitution
overwrites `$?`, so a failed step logged `rc=0`. This report's captures fail closed on any non-zero
arm.

---

<!-- RESULTS SECTIONS 4-7 APPENDED AFTER THE CAPTURES COMPLETE -->
