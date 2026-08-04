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
`selftest.sh` does **not** silently accept this on a foreign host, and an earlier draft of this
section wrongly said it could not catch it. It hard-asserts `logical_cpus == 16`,
`physical_cores == 8`, `sibling_pair_rule_observed == "(c, c+8)"` and
`[2,10] ∈ smt_sibling_pairs`, every one read live from `/sys` — so on this box all four fail and
#3217's harness refuses to run rather than mislabelling. What it does not do is *derive* the sets:
the table is correct for its own host and only for that host. The guard used here,
`ws0_assert_full_physical_cores`, is topology-derived instead — it requires the pinned set to be an
exact union of **complete** SMT sibling groups with group count equal to the requested S, so it is
correct on any topology, and is negative-tested against both of #3217's sets *as they would be
interpreted on this host*. That is a portability improvement over a pinned-host selftest, not a
defect found in #3217.

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

### 3.4 Uncore: three ways to be wrong by a large integer factor

**(i) `--per-socket`, and the moving enabled-% column.** Each `uncore_imc_N` device carries
`cpumask=0,32` — CPU 0 is socket 0's proxy for it, CPU 32 is socket 1's — so a plain
`perf stat -a` aggregates the device across **both** sockets and hides the split this
report needs. `--per-socket` splits it, but it also inserts **two** leading CSV fields,
moving the enabled-% column from field 5 to **field 7**. Reading field 5 there would
silently parse `run_time` as a percentage; `derive.py` takes a `per_socket` flag for
exactly this reason.

**(ii) The ×64 is already applied.** perf reports `cas_count_*` **already scaled to MiB**
(its unit field says so). The ×64 B/cacheline conversion the RUNBOOK specifies is perf's,
not ours — multiplying the MiB figure by 64 again would overcount by 64×.

**(iii) The 8× that summing could have been — settled two independent ways.** perf exposes
`uncore_imc_0..11`; per socket **eight report a near-identical non-zero value and four read
exactly `0.0`**. Two readings fit that equally well:

- **distinct channels**, near-identical because DRAM interleaving is uniform → the
  per-instance values **must be summed**;
- **duplicate reports** of one socket-level aggregate → summing overcounts by **8×**.

`sum ÷ max = 7.996` is consistent with **both**, so it cannot decide — and every GB/s
figure in this report differs by 8× on the answer. What prompted checking rather than
assuming: the summed figure implies **22.2 KB of DRAM traffic per 692 B logical row**, a
32× amplification, which is large enough to demand proof.

Both checks say *distinct channels*:

1. **DIMM topology**, independent of perf entirely
   (`host/memory-channel-topology.txt`): `dmidecode -t memory` reports `CPU0 Channel0..7`
   and `CPU1 Channel0..7` populated at 3200 MT/s — **8 channels per socket**, exactly
   matching the 8 non-zero read/write instance pairs per socket.
2. **Byte accounting under the triad** (§6): the triad moves a *known* number of bytes, so
   `IMC_measured ÷ expected` is ~1× under the channel reading and ~8× under the duplicate
   reading. `run/ac5-peak.sh` states the verdict and **refuses to bless a bandwidth figure
   when the ratio matches neither**.

Topology also fixes the theoretical ceiling: 8 × 3200 MT/s × 8 B = **204.8 GB/s per
socket** (409.6 GB/s for the box). §6 measures the *achievable* peak at the engine's own
binding, which is the ceiling the engine actually faces and is far below the socket
figure — six cores cannot saturate eight channels.

**The amplification is real, not an artefact.** Demand `LLC-load-misses` × 64 B accounts
for only ~2.4 KB of the ~22 KB per row, because on a streaming scan the hardware
prefetcher does most of the fetching, and a prefetched line that a later demand load hits
is **not** counted as an `LLC-load-miss`. Demand miss counters therefore systematically
**undercount** DRAM traffic on this workload — which is why AC3 reports the IMC figure and
the miss counters as separate measured facts rather than deriving either from the other.
Internal consistency: DRAM bytes/row rises **×2.98** between the endpoints while
`LLC-load-misses`/row rises **×3.19**.

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

### 3.8 Arm (d): the memory-stall cycles are MEASURED, not modelled

AC4 asks what fraction of the cycles/row delta is attributed, and #3217 answered it
the classical way — charge each miss counter at a penalty, `attributed = Σ Δ(misses/row)
× penalty_cycles` — and landed **~87% unattributed**. That product is only ever as good
as the penalty, and the penalty is the weakest link in the chain:

- an **unloaded** serial-chase latency assumes **zero memory-level parallelism**, so it
  overcharges every miss that actually overlapped another;
- dividing by an **assumed** MLP undercharges if the guess is high;
- a **vendor** figure is not this silicon under this load at all.

This host removes the need to model it. `cycle_activity.stalls_l3_miss` counts
**execution-stall cycles while an L3 miss is outstanding**, in hardware, per pinned CPU
— the very quantity the penalty product estimates, and it is inherently **MLP-correct**,
because two overlapping misses that stall the same cycle are *one* stalled cycle, which
is what a cycles/row accounting must add up. So arm (d) charges the measured stall
cycles and keeps the modelled product as a **cross-check** (§5.4).

Group C, verified at **100.00% enabled under load before use**:

```
cycles, instructions, task-clock,
cycle_activity.stalls_l3_miss     <- the attribution term
cycle_activity.stalls_l2_miss     <- superset: includes L2 misses that HIT in L3
cycle_activity.stalls_total       <- superset: all execution stalls
l1d_pend_miss.pending             <- Σ outstanding L1D misses per cycle
l1d_pend_miss.pending_cycles      <- cycles with >=1 outstanding  => MEASURED MLP
```

The three stall counters **nest**, which is what makes §5.3's decomposition additive
rather than an estimate with a residual.

Two properties of arm (d) worth stating because they are deliberate:

1. **It is a separate script** (`run/capture-stalls.sh`), added after the primary arms
   were already green and writing separate files into the same rep directories, rather
   than an edit to an already-validated capture path. `derive.py` treats it as optional.
2. **Its heredoc is quoted.** `capture-endpoint.sh` interpolates shell variables into an
   *unquoted* heredoc, so bash performs command substitution inside the Python source —
   and its own docstring warning against writing backticks there *contains backticks*,
   so every run emitted `line 182: ok: command not found`. Harmless (the mangling is
   confined to that docstring; the whole heredoc was audited for other `` ` `` and `$(`
   and has none) but self-defeating. Arm (d) passes values as `argv` instead, so the
   hazard does not exist.

It reuses `harness/common.sh` unchanged, so every §3.6 guard governs it identically.

### 3.9 On-host latency calibration — and the probe defect it exposed

`run/penalty-probe.sh` measures the cache hierarchy's access latency on this exact
silicon with a **serial dependent pointer chase** over a random permutation of 64 B
lines: each access depends on the previous one, so there is no MLP to hide the latency
and cycles/access *is* the access latency in cycles.

**The first version of this probe was wrong, and it is worth recording how it announced
itself.** In `cache-hostile.c`, `--working-kib` *confines* the chase (`--working-kib 0`
means "the whole buffer"). The probe's "hostile" branch passed `--buffer-mib 2048
--working-kib 256`, which chases an **L2-resident 256 KiB** no matter how large the
buffer is. Its DRAM row therefore reported **15.12 cycles/access — below the L2 row's
18.64**. A DRAM latency cheaper than L2 is impossible, which is the *only* reason the
bug was caught: had the number merely been plausible it would have been published, and
every penalty charged from it would have been wrong. That is the same failure class as
the flat-staging capture that returned `rc=0` on zero rows (§3.6) and the positive
control's two false FAILs (§2.1) — **an instrument that does not error but emits
plausible output**, three times in one study.

The fixed probe uses one code path, brackets the LLC→DRAM transition across
256 MiB/1 GiB/2 GiB so the plateau is *observed* rather than assumed from a single
point, and adds `dTLB-load-misses:u` so the page-walk bundling is a **measured number
rather than a prose caveat**:

| level | working set | cycles/access | ns/access | LLC-loads/acc | LLC-miss/acc | dTLB-miss/acc |
|---|--:|--:|--:|--:|--:|--:|
| L1d | 32 KiB | **6.11** | 2.11 | 0.0000 | 0.0000 | 0.0000 |
| L2 | 512 KiB | **18.57** | 6.40 | 0.0005 | 0.0001 | 0.0000 |
| LLC | 8 MiB | **90.44** | 31.19 | 1.0093 | 0.0063 | 0.0154 |
| LLC | 32 MiB | **114.78** | 39.58 | 1.0299 | 0.0515 | 0.7634 |
| DRAM | 256 MiB | **393.50** | 135.69 | 1.4214 | 1.1129 | 1.1176 |
| DRAM | 1 GiB | **499.78** | 172.34 | 3.1550 | 1.9047 | 1.6606 |
| DRAM | 2 GiB | **581.76** | 200.61 | 5.0429 | 2.9016 | 2.3757 |

The DRAM rows do **not** plateau — they climb 393 → 500 → 582 — and the measured
`dTLB-miss/access` (1.12 → 2.38) and `LLC-loads/access` (1.42 → 5.04) say why: with
4 KiB pages a random stride over gigabytes misses the TLB on essentially every access,
and each page-table walk is itself extra memory traffic. So the larger the working set,
the more page-walk cost the figure bundles. **The smallest DRAM-resident point
(256 MiB, 393.50 cycles) is therefore the least-contaminated DRAM latency available
here**, and it is still an over-estimate. §5.4 charges from it and says so.

**Direction of conservatism, stated because it is easy to get backwards.** An earlier
draft of this probe asserted that an upper-bound penalty is "the conservative direction
for a claim of *attributed*". It is the **opposite**: a larger penalty inflates
`attributed` and *shrinks* the residual, which flatters the hypothesis that the decay is
explained. AC7 and RUNBOOK step 7 forbid rounding toward the hypothesis, so the
**headline attribution below is the measured stall term**, and the modelled zero-MLP
charge is reported as the upper bound it is — never as the attribution.

#### 3.9.1 A SECOND defect in the same probe, found by review, and its measured size

The table above carries a second contamination, found by the roborev round on
PR #3286 (finding ②) rather than by reading the numbers — because this one *was*
plausible. **perf ran unfenced.** The probe invoked `perf stat` as a plain wrapper with
neither `-D` nor a control FIFO, while `cache-hostile` defaults to `delay_s = 10.0` and
calls `wait_for_window()`. Counting therefore began at process start, so the
identity-fill and Sattolo permutation build were **inside the measured interval** — and
that build walks the **working set**, so its cost scales with exactly the variable the
probe exists to hold constant.

It is measurable from the committed CSVs, and the signature is unambiguous. Subtracting
the chase-only instruction count (the `L1d_32K` row's 120,174,195 = **6.009
instr/access**, where `nodes=512` makes init negligible):

| row | nodes | extra instructions | **instr/node** | instr/access |
|---|--:|--:|--:|--:|
| L1d_32K | 512 | 0 | — | 6.009 |
| L2_512K | 8,192 | 223,053 | 27.23 | 6.020 |
| LLC_8M | 131,072 | 3,789,715 | **28.91** | 6.198 |
| LLC_32M | 524,288 | 15,199,301 | **28.99** | 6.769 |
| DRAM_256M | 4,194,304 | 121,688,792 | **29.01** | 12.093 |
| DRAM_1G | 16,777,216 | 486,790,479 | **29.01** | 30.348 |
| DRAM_2G | 33,554,432 | 973,592,180 | **29.02** | 54.688 |

**29.0 instructions per node, constant across five orders of magnitude.** That is the
init loop, in the window. (The 10-second delay itself costs nothing: `wait_for_window`
`nanosleep`s, and `cycles:u` counts no user cycles while descheduled. The contaminant is
init *work*.)

**Size of the error, bounded from the artefacts.** Fitting
`cycles = 20M·L + nodes·k` across the three DRAM points gives `k` = 97.7–168.9
init-cycles/node, so:

| row | init share of counted cycles | reported | corrected |
|---|--:|--:|--:|
| LLC_8M | 0.7%–1.3% | 90.44 | ≈ 89.3–89.8 |
| DRAM_256M | 5.2%–9.0% | 393.50 | ≈ 358–373 |
| **penalty (DRAM − LLC-hit)** | | **303.06** | **≈ 269–284** |

So the penalty is **~7–12% high**, in the anti-conservative direction named above.
**Every probe row nevertheless ran to completion**: `cache-hostile` `exit(4)`s on
`init_overrun` *before* the chase, so the chase's 120,174,195 instructions would be
absent — they are present in all seven rows, which is positive evidence that no probe
exited early even though the script ignored its return codes.

**What this does and does not touch.** It does **not** touch §5.3 or the AC4 verdict:
those consume `cycle_activity.stalls_l3_miss` from the two-endpoint capture and consume
nothing from this probe (`results/derive.py` reads `penalty_table` at exactly one site,
the modelled cross-check). It **does** bound §5.4, which is corrected there.

Both defects are now mechanically refused rather than described. The window is gated by
perf's control FIFO — which also excludes exit-time teardown, itself
working-set-dependent — and `run/penalty-window-check.py` **verifies the gate held, per
row**, so a silently-failed handshake cannot publish a latency either. Run against the
contaminated CSVs still committed here, it rejects them; see §7.1.

---

## 4. Results — the two endpoints

**3 reps per endpoint**, every counter **100.00% enabled** in every rep, every validity gate
PASS in every rep. Raw: `results/<label>/rep<N>/`; derivation `results/derive.py` →
`results/derived.json` + `results/derived-summary.txt`; drivers `results/run-all.log` and
`results/run-stalls.log`.

`#3217's method gap 1 is closed`: its `llc-*` captures were **reps=1**, so its headline
delta carried no dispersion. Here every figure is a median of 3 with min/max published.

### 4.1 Headline — the decay reproduces, and instructions/row is flat

ALIGNED convention (primary), medians of 3 reps:

| per row | S=1 / N=2 | S=6 / N=16 | Δ | ratio |
|---|--:|--:|--:|--:|
| **instructions** | 38,856.8 | 38,685.6 | **−171.3 (−0.44%)** | 0.996 |
| **cycles** | 31,316.4 | 37,284.9 | **+5,968.6 (+19.06%)** | 1.191 |
| **IPC** | **1.2376** | **1.0384** | **−16.09%** | 0.839 |
| rows/s (aggregate) | 222,329 | 1,117,642 | — | 5.027 |

Dispersion across the 3 reps — cycles/row spread **1.52%** at S=1 and **0.56%** at S=6:

| | min | median | max | spread |
|---|--:|--:|--:|--:|
| cycles/row S=1/N=2 | 31,117.9 | 31,316.4 | 31,593.6 | 1.52% |
| cycles/row S=6/N=16 | 37,147.5 | 37,284.9 | 37,356.5 | 0.56% |

**The same work, executed more slowly.** Instructions/row is flat to −0.44%; cycles/row
rises 19%. This is #3217's finding, reproduced on a host where the mechanism counters work.

### 4.2 AC2 — the endpoints are the same points, and rows/s lands in band

| | #3217 (context only) | this host | Δ |
|---|--:|--:|--:|
| best aggregate at S=6/N=16 | 1,076,917 rows/s | **1,117,642 rows/s** | **+3.8%** |

**+3.8% is the band, and it is a band check rather than a comparison of absolutes** — a
two-socket bare-metal Ice Lake-SP is not #3217's single-socket virtualized Sapphire Rapids
guest. Corpus geometry is exact on both row oracles (§3.1) and `sha256(Data.db)` was
**re-verified unchanged after the last capture**:
`b1656ae8c0e45feb30f3da641b8a23c4969d1be43e5f341ef0af6bb3a9b41042`. `--batch-size 8192` as
AC2 requires; both SMT siblings of every pinned core; both endpoints inside NUMA node 0.

Where this host **differs** from #3217's, stated rather than absorbed: its decay is
**smaller** (cycles/row +19.06% vs +34.1%, IPC −16.09% vs −25.4%). That is the expected
direction for a bare-metal host with a larger shared LLC (54 MiB/socket) and no hypervisor,
and it does not weaken the mechanism claim, which rests on *this* box's self-consistent pair.

### 4.3 #3217's open method question, SETTLED: the two conventions agree

| | ALIGNED (counters and rows share one interval **by construction**) | INTERIOR (#3217's) | difference |
|---|--:|--:|--:|
| cycles/row S=1/N=2 | 31,316.4 | 31,457.0 | **+0.45%** |
| cycles/row S=6/N=16 | 37,284.9 | 37,585.8 | +0.81% |
| **Δ cycles/row** | **5,968.6** | **6,128.8** | **+2.68%** |
| IPC S=1 → S=6 | 1.2376 → 1.0384 | 1.2305 → 1.0374 | — |

**#3217's most exposed number was sound.** Its `cycles/row = counter ÷ (rows_per_s ×
window_secs)` took counters from a 20 s interior slice and the rate from the whole step, and
at `llc-s1-N2` that step held only 4 completed requests in 63.99 s. Measured both ways on
one host from the same reps, the conventions agree to **0.45%** at the exposed point. The
reason is the property §3.5 established: the loadgen holds each step open until in-flight
requests drain, so a step contains only whole scans and occupancy is ~99.5% — precisely the
condition the interior convention needs in order to be valid.

So **#3217's +8,593 cycles/row is not a convention artefact.** For the record, against
#3217's own baseline of **25,200** cycles/row at S=1/N=2, this host measures **31,316**
(ALIGNED) / **31,457** (INTERIOR) — a **+24%** absolute difference that is a *cross-host*
difference, not a convention one, and is exactly why the cross-host rule in the preamble
bars comparing the two absolutes. **Verdict: the conventions AGREE (0.45% at the exposed
point, 2.68% on the delta); the absolutes DIVERGE across hosts by 24% and must not be
compared.**

### 4.4 AC3 — the counters, per point, per row

ALIGNED convention, medians of 3 reps. The three counters that read `<not supported>` on
#3217's host are in **bold**.

| event / row | S=1 / N=2 | S=6 / N=16 | ratio | reading |
|---|--:|--:|--:|---|
| **LLC-load-misses** | **11.935** | **38.081** | **×3.191** | **the signal** |
| **LLC-loads** | **74.073** | **88.167** | ×1.190 | more LLC traffic |
| **cache-references** | **372.954** | **407.340** | ×1.092 | slightly more |
| cache-misses | 56.920 | 130.128 | ×2.286 | corroborates |
| L1-dcache-loads | 9,157.744 | 9,140.759 | ×0.998 | **flat** |
| L1-dcache-load-misses | 586.662 | 578.921 | ×0.987 | **flat** |
| branch-misses | 63.050 | 62.949 | ×0.998 | **flat** |
| dTLB-load-misses | 7.044 | 8.558 | ×1.215 | small, +1.5/row |
| instructions | 38,856.8 | 38,685.6 | ×0.996 | **flat** |

**The mechanism is named by what does NOT move.** The private caches are untouched — L1
loads *and* L1 misses flat to ~1%, branch behaviour flat, the same instruction stream. LLC
misses per row **triple**. What fails between the 1-core and 6-core points is the **shared**
LLC: six cores contend for one 54 MiB last-level cache, each core's effective share falls
roughly six-fold, and the working set stops fitting. `LLC-load-misses` rising ×3.19 while
`LLC-loads` rises only ×1.19 means the **miss rate** rose from 16.1% to 43.2% — the same
accesses, far less often satisfied.

The `LLC-loads` counter also demonstrates why the positive control gates on the miss *rate*
rather than the raw load count (§2, RUNBOOK step 4): raw loads move only ×1.19 here, so a
control asserting a large rise in raw `LLC-loads` would red a host that is behaving exactly
as expected.

### 4.5 AC3 — memory bandwidth, with the source named explicitly

**Source: `uncore_imc_*/cas_count_{read,write}/`, `perf stat --per-socket`, summed over the
8 populated channels per socket.** `perf stat -M MemoryBandwidth` **does not exist on this
perf build** (§1) — recorded as a capability fact, not silently omitted; there is no `-M`
cross-check available here. **Byte basis: actual DRAM bytes moved**, `cas_count × 64 B`,
where perf applies the ×64 itself and reports MiB (§3.4-ii). Channel summing validated two
independent ways (§3.4-iii, ratio **1.008**).

| | S=1 / N=2 | S=6 / N=16 |
|---|--:|--:|
| socket 0 (the engine's node) | 1.58 GB/s | **24.43 GB/s** |
| socket 1 (far) | 0.07 GB/s | 0.28 GB/s |
| **total** | **1.65 GB/s** | **24.71 GB/s** |
| far-socket fraction | 3.9% | **1.1%** |
| DRAM bytes / row | 7,429 B | 22,161 B |

**The NUMA confinement is proven by measurement, not asserted**: 98.9% of DRAM traffic lands
on the engine's own socket at the 6-core point. A mixed-NUMA allocation is a plausible
alternative explanation for an IPC delta, and it is excluded by data as well as by
construction.

DRAM bytes/row rises **×2.98**, against `LLC-load-misses`/row's **×3.19** — internally
consistent. On the absolute magnitude (22 KB/row against a 692 B logical row) see §3.4: the
hardware prefetcher does most of the fetching on a streaming scan, so demand miss counters
systematically undercount DRAM traffic, and the two are reported as separate measured facts
rather than one being derived from the other.

---

## 5. AC4 — the cycles-per-row accounting. The residual is a number.

### 5.1 Basis, and the arms' agreement

The stall counters come from arm (d) (group C), which ran its **own** loadgen steps, so they
are charged against **group C's own** cycles/row delta — numerator and denominator from one
interval, the same discipline the ALIGNED convention exists to enforce. The primary arm's
delta is reported beside it.

| | S=1 / N=2 | S=6 / N=16 | Δ |
|---|--:|--:|--:|
| cycles/row, primary arm (a1) | 31,316.4 | 37,284.9 | **+5,968.6** |
| cycles/row, group C arm (d) | 31,622.5 | 37,261.9 | **+5,639.4** |
| agreement at the point | +0.98% | −0.06% | — |

**The two arms agree to within 1% at each endpoint, but their deltas differ by 5.5%** —
differencing two large, nearly-equal numbers amplifies a small relative difference. Stated
because it bounds the precision of everything below: **the attribution shares carry a ±~5%
basis uncertainty from the choice of arm**, and would read 67.8% (group C basis) or 64.0%
(primary-arm basis). That is a real limit of this measurement, not a rounding.

`instructions/row` is flat on both arms (−0.44% primary, and group C's IPC 1.2309 → 1.0379
tracks the primary arm's 1.2376 → 1.0384), so both arms observed the same workload.

### 5.2 The measured stall counters

| per row | S=1 / N=2 | S=6 / N=16 | Δ | % of cycles (S=1 → S=6) |
|---|--:|--:|--:|---|
| `cycle_activity.stalls_l3_miss` | 1,290.3 | 5,111.6 | **+3,821.3** | 4.08% → **13.72%** |
| `cycle_activity.stalls_l2_miss` | 2,752.6 | 7,231.3 | +4,478.6 | 8.71% → 19.41% |
| `cycle_activity.stalls_total` | 12,436.5 | 18,707.7 | +6,271.3 | 39.36% → 50.21% |
| MLP (`pending ÷ pending_cycles`) | **2.150** | **2.201** | — | — |

The core goes from spending **4.1%** of its cycles stalled on an L3 miss to **13.7%** — a
3.3× rise that tracks the ×3.19 rise in LLC misses per row.

**What these counters mean is verified from the event definitions on this host, not
assumed** (`host/counter-semantics-verification.txt`, `perf list --details`):

| event | definition, verbatim | encoding |
|---|---|---|
| `cycle_activity.stalls_l3_miss` | "Execution stalls while L3 cache miss **demand load** is outstanding" | `event=0xa3, umask=0x6, cmask=0x6` |
| `l1d_pend_miss.pending` | "Number of L1D misses that are outstanding" | `event=0x48, umask=0x1` |
| `l1d_pend_miss.pending_cycles` | "**Cycles with** L1D load Misses outstanding" | `event=0x48, umask=0x1, cmask=0x1` |

Two things follow. It confirms the term is exactly the quantity the penalty product
estimates, and that `pending ÷ pending_cycles` is the mean outstanding misses *across
cycles in which at least one is outstanding* — the correct MLP divisor.

> **And it surfaces a scope limit worth stating plainly: the stall term is DEMAND-LOAD
> scoped.** Stalls caused by **prefetch** traffic — prefetches saturating fill buffers or
> memory queues — are not in it, and land in "other execution stalls" instead. That matters
> *here specifically*, because §3.4 showed the hardware prefetcher does most of the fetching
> on this streaming scan (demand `LLC-load-misses` × 64 B covers only ~2.4 KB of the ~22 KB
> DRAM traffic per row). So **the 67.76% below is a demand-load attribution, and some part
> of the ~32% residual is plausibly prefetch-related memory cost this counter cannot see.**
> It should not be read as "32% of the decay is non-memory" — §5.3's `other execution
> stalls` bucket is an upper bound on the non-memory share, not a measurement of it.

### 5.3 The additive decomposition — it closes exactly

The three stall counters **nest** (`stalls_l3_miss` ⊂ `stalls_l2_miss` ⊂ `stalls_total`), so
differencing the nested pairs yields **disjoint** buckets, and the cycles that were not
stalled at all close the identity:

| bucket | cycles/row | % of Δ | what it is |
|---|--:|--:|---|
| L3-miss stalls | **+3,821.3** | **67.76%** | misses served from **DRAM** |
| L2-miss-but-L3-hit stalls | +657.4 | 11.66% | misses served from the **LLC** |
| other execution stalls | +1,792.6 | 31.79% | **non-memory** (front-end, ports, dependencies) |
| non-stalled cycles | **−631.9** | **−11.20%** | fewer un-stalled cycles per row |
| **SUM** | **+5,639.4** | **100.00%** | **closure error +0.000** |

This is a **partition of the measured delta, not an estimate with a slack term** — it sums
to the measurement by construction, and the closure error is printed as a float-rounding
check (`+0.000`).

Two defensible places to draw the memory boundary, both published rather than one quietly
preferred:

| boundary | attributed | residual | residual % |
|---|--:|--:|--:|
| **strict DRAM** (L3-miss stalls only) | **+3,821.3** | **+1,818.1** | **32.24%** |
| whole cache hierarchy (L3 miss + L3 hit) | +4,478.6 | +1,160.8 | **20.58%** |

> ### AC4 verdict
> **Δ = +5,639.4 cycles/row.** Charged at the **measured** L3-miss stall cycles,
> **attributed = +3,821.3 cycles/row (67.76%)** and the **residual = +1,818.1 cycles/row,
> which is 32.24% of the delta, UNATTRIBUTED.** Widening the boundary to the whole cache
> hierarchy attributes 79.42% and leaves a **20.58%** residual.
>
> As a fraction of **#3217's** +8,593 cycles/row — which AC4 names, and which is a
> *different host's* delta — this box's attributed 3,821.3 is 44.5%. **That ratio is not
> meaningful and is given only because AC4 names the figure**: the correct denominator is
> this box's own Δ, per RUNBOOK step 7.1.
>
> For comparison, #3217 attributed **~10–13%** and left **~87%** unattributed. **The
> unattributed fraction falls from ~87% to 32%,** and the named cause is contention for the
> shared last-level cache.

### 5.4 The modelled charge brackets the measurement — and shows why modelling was the weak link

The classical route, charged from the **on-host** latencies of §3.9
(penalty = DRAM 393.50 − LLC-hit 90.44 = **303.1 cycles per miss**, applied to
Δ`LLC-load-misses`/row = 26.15):

| charge | cycles/row | % of Δ | reading |
|---|--:|--:|---|
| zero-MLP (full unloaded latency per miss) | +7,923.9 | **140.51%** | **impossible** — exceeds the whole delta |
| ÷ **measured** MLP 2.20 | +3,600.4 | 63.84% | plausible |
| **measured `stalls_l3_miss`** | **+3,821.3** | **67.76%** | **the headline** |

Both modelled rows use the penalty **as reported by the probe**, and §3.9.1 measures that
penalty to be **~7–12% high**. Corrected, the zero-MLP charge is ≈125%–130% and the
MLP-corrected charge ≈56.6%–59.8%. The reported figures are kept in the table because
they are what the committed artefacts contain; the corrected ranges are what the
conclusions below are stated against.

Two things follow, and both matter beyond this report:

1. **A zero-MLP penalty charge is not conservative — it is wrong.** It accounts for 140% of
   a delta it is supposed to explain a fraction of — and **still >100% after the §3.9.1
   correction**, so this conclusion does not depend on the contaminated figure; if
   anything the correction strengthens it, since the impossibility survives removing
   the inflation that most flattered it. Any accounting that had charged the unloaded
   latency per miss would have "attributed" more than 100% and declared the mechanism
   fully explained. The earlier draft of the penalty probe asserted precisely that
   this direction was "the conservative direction for a claim of *attributed*" (§3.9); it is
   the opposite.
2. **Corrected by a *measured* MLP, the model lands in the same range as the direct
   measurement** (63.84% vs 67.76%). Two independent routes — a modelled charge from
   on-host latency and MLP, and a hardware stall counter — agree on the mechanism and
   on its rough magnitude. That mutual corroboration is the strongest statement this
   study makes about the mechanism, and neither route alone would license it.

   > **The tightness of that agreement is NOT claimable, and an earlier draft of this
   > paragraph claimed it.** It read "the model lands within 5.8% of the direct
   > measurement", quoting the 63.84%-vs-67.76% gap as if both figures were clean.
   > §3.9.1 measures that the penalty feeding 63.84% is **~7–12% high** from init
   > contamination, so the corrected modelled share is **≈56.6%–59.8%** and the honest
   > gap against 67.76% is **~12–16%, not 5.8%**. The direction of the correction is
   > *away* from the measurement, so the agreement is looser than the draft claimed —
   > and quoting the tighter number would have been rounding toward the hypothesis,
   > which is what §3.9 and AC7 forbid.
   >
   > **What survives is the part that was load-bearing anyway:** two methods that share
   > no counter and no assumption both put the DRAM-served share of the delta in the
   > mid-50s to high-60s percent, against #3217's ~10–13%. The corroboration is of the
   > *mechanism*, and it is not weakened. What does not survive is a precision claim,
   > and precision claims are exactly what a contaminated instrument cannot support.
   > Recorded rather than quietly restated, because the draft's number is the kind that
   > gets cited.

`dTLB-load-misses`/row rises only 7.04 → 8.56 (+1.5/row) and is **not** added to the
headline: any stall a page-table walk caused is already inside the measured stall counters,
so charging it separately would double-count. Listed in `derived.json` for completeness.

### 5.5 Cross-check on this report's own arithmetic

RUNBOOK step 7.5 asks where the accounting lands against measured marginal efficiency:

| | value |
|---|--:|
| throughput 222,329 → 1,117,642 rows/s over 6× cores | **0.8378 measured efficiency** |
| predicted from the cycles/row inflation alone | **0.8399** |
| **gap** | **−0.21 pp** |

Two routes to the same quantity — one from throughput, one from cycles/row — agree to
0.21 pp. (#3217's equivalent check had a 1.26 pp gap.) This is a check on this report's
arithmetic, not a target, and it passes.

---

## 6. AC5 — the saturation verdict

**Achievable peak measured at the engine's own binding**, as RUNBOOK step 8.2 requires:
`cache-hostile stream`, 12 threads, 4 GiB × 3 arrays, 10 iterations, under
`numactl --cpunodebind=0 --membind=0 taskset -c 0-5,64-69` — the S=6/N=16 server set
verbatim. This is a **STREAM-triad-class reference, not the vendor STREAM benchmark**.
Artefacts: `ac5-run/`.

| | GB/s | basis |
|---|--:|---|
| triad, best iteration | 84.405 | **24 B/element** (architectural: 2 reads + 1 write) |
| triad, best iteration | **112.540** | **32 B/element** (adds read-for-ownership of the written line) |
| IMC steady-state equivalent | 112.54 | `cas_count × 64 B`, same instant |

The **32 B basis is the one quoted below**, because that is what the DRAM controller
actually sees on a machine without non-temporal stores — and it is the basis that makes the
peak directly comparable to the engine's IMC-measured traffic. The IMC byte-accounting
cross-check over the same run agrees to **1.008×** (§3.4-iii), which simultaneously
validates the peak and the channel summing.

For context from topology (§3.4): the *theoretical* socket ceiling is 8 channels ×
3200 MT/s × 8 B = **204.8 GB/s**. The 12-thread pinned peak of 112.5 GB/s is 55% of that,
which is the expected shape — **six of thirty-two cores cannot saturate eight channels**,
and 112.5 GB/s, not 204.8, is the ceiling this engine faces.

> ### AC5 verdict
> **At S=6/N=16 the memory system is NOT saturated: measured 24.43 GB/s on the engine's
> socket (basis: actual DRAM bytes, `uncore_imc cas_count × 64 B`; 24.71 GB/s including the
> 1.1% far-socket component) against an achievable 112.54 GB/s measured on this host at the
> same 6-core/12-thread NUMA-bound binding (basis: 32 B/element, read-for-ownership
> included) = 21.7% of peak.**
>
> **Therefore the decay is NOT a bandwidth wall, and reducing per-row work moves the scaling
> SLOPE only insofar as it reduces the per-row LLC FOOTPRINT; a lever that cuts
> instructions per row without cutting bytes touched per row moves the LEVEL only.**

### 6.1 Why the verdict is conditional rather than a bare "slope" or "level"

The bare form of the question presupposes that the binding constraint is DRAM throughput. It
is not — there is **4.6× bandwidth headroom**. The measured constraint is **capacity
contention in the shared 54 MiB LLC**: six cores each get roughly a sixth of it, the working
set stops fitting, and misses per row triple. Bandwidth *rises* as a consequence of that,
and stops well short of the ceiling.

That distinction is exactly what #2817 and #3096 need, so the criterion is stated as a test
a candidate lever must pass:

| a lever that… | effect | why |
|---|---|---|
| reduces **bytes touched per row** / improves locality (smaller intermediates, fewer passes, better reuse) | moves the **SLOPE** | it relieves the LLC capacity contention that *is* the decay, and relieves it more at 6 cores than at 1 |
| reduces **instructions per row** at the same memory footprint | moves the **LEVEL** | it does not change LLC pressure, so the ×3.19 miss inflation and the resulting IPC decay survive it |

`instructions/row` being flat across the endpoints (−0.44%) is the direct evidence for the
second row: the decay is not made of extra instructions, so removing instructions cannot
remove the decay.

**Whether #3096 (Arrow encode) is a slope lever or a level lever is therefore an open
question this report deliberately does not answer** — it turns on whether that work reduces
the bytes touched per row or only the instructions spent on them, which is a measurement on
#3096's own change, not on this one. What this report supplies is the criterion and the
headroom figure.

---

## 7. What this indicts — as follow-ups, not fixes here

Per the issue's explicit scope, **no production code was changed and no fix is proposed.**
Three things the data indicts, all now filed:

1. **LLC capacity contention is the named cause of the full-box IPC decay.** A working-set /
   locality reduction in the Flight read path is the lever class that can move the slope.
   The measurement to demand of any candidate is Δ`LLC-load-misses`/row at S=6/N=16, not
   Δinstructions/row. → **#3288**, where that criterion is the gate any proposal must pass.
2. **~32% of the delta remains unattributed** (20.6% at the wider boundary), sitting in
   `other execution stalls` (+1,792.6 cycles/row) net of the −631.9 in non-stalled cycles.
   **That bucket is not established to be non-memory**: because `stalls_l3_miss` is
   demand-load scoped (§5.2), prefetch-induced memory stalls land there too, and on this
   streaming workload the prefetcher moves most of the bytes. Splitting it would need
   front-end / port-utilisation counters (a TMA level-2 breakdown) **plus** an
   offcore/prefetch-stall term — a different capture than this one, and out of this issue's
   two-endpoint scope. The honest statement is that 32% is *unattributed*, not that it is
   *non-memory*. → **#3287**, filed with that method stated.
3. **#3217's committed scripts do NOT carry these defects — verified against `origin/main`, not
   assumed.** An earlier draft of this report asserted they did; that was wrong, and the
   correction matters because the claim would have impugned published figures.
   - The **fabricated `rc`** pattern was fixed there before commit, in all six drivers with a
     `run()` wrapper (`run-partA.sh`, `run-partA-followon.sh`, `run-partB{,2,3,4}.sh`): `rc` is
     captured into a variable immediately after the measured command, and the only intervening
     lines are comments, which are not commands and cannot reset `$?`. This matches #3217's own
     report C7, which records the fix and a verification that it logs a real `rc=1`. The
     remaining scripts in those directories log no `rc` at all, so there is nothing to fabricate.
   - The **hardcoded core table** is *correct* for #3217's own 16-logical / 8-physical host under
     its `(c, c+8)` sibling rule: `2,10` is both siblings of one physical core, and `0-5,8-13` is
     six complete pairs. It is pinned to that host rather than derived, and `selftest.sh` asserts
     that exact topology from `/sys` (§3.2), so on a different box it fails closed.

   **Therefore no #3217 figure is impugned by anything found in this issue**, and nothing here
   calls for re-deriving its results. What remains is a genuine but much narrower portability
   limitation — a pinned table plus a host-pinned selftest, where a topology-derived guard would
   let the harness run correctly anywhere. → **#3289**, filed as a portability improvement that
   explicitly records that no #3217 figure is affected.

### 7.1 The harness's own six fail-open defects — found by review, fixed here, not deferred

The roborev round at `c27ca28..88f7ec9` was the **first** to actually receive this PR's ten
harness executables (`prompt-content: PASS (10/10)`; the preceding rounds reviewed nothing —
a code-free verdict on a diff carrying 8 `.sh`, 1 `.py` and 1 `.c`, which is the #3229
exclusion-scope hazard, recorded in PR #3286 §2). It immediately found **six defects, and every one is a fail-open in a measurement
instrument** — a condition under which a *failed* measurement would have been published as a
number. That is the same class this report indicts #3217's harness for in §3.7 and §3.9, so
they are fixed **in this PR** rather than filed: #3287, #3288 and #3289 all re-run this
harness, and the next operator inherits whatever ships here.

| | site | fail-open | direction |
|---|---|---|---|
| ① | `positive-control.sh` `evaluate()` | 2× movement gate ran before the `LLC-load-misses` miss-rate branch | false **FAIL** — rejects a healthy host |
| ② | `run/penalty-probe.sh` | perf unfenced, so init was counted; probe rc ignored | false **PASS** — see §3.9.1 |
| ③ | `positive-control.sh` `report_ev()` | sub-`MUX_MIN_PCT` counts warned but still read `OK` | false **PASS** — a multiplexed estimate certified sound |
| ④ | `run/capture-endpoint.sh` | validity expression omitted `RC_LG_A`/`RC_LG_C` while claiming to cover every arm | false **PASS** |
| ⑤ | `run/run-all.sh` + `results/derive.py` | resume ignored rc and counter files; absent uncore CSV derived as **0 GB/s** | false **PASS** |
| ⑥ | `run/ac5-peak.sh` | nonzero rc printed not fatal; INDETERMINATE and UNAVAILABLE both exited 0 | false **PASS** — against a discharged AC |

**Five of the six fail in the PASS direction, which is why all six are blockers here rather
than the four Mediums an ordinary severity rubric would give them.** In a deliverable whose
entire content is *numbers whose provenance is trustworthy*, a false-PASS is not a defect in
the software, it is a defect in the result.

**The published figures were verified BEFORE any guard was written**, because "latent hazard"
and "published error" are different findings with different remedies, and patching the guards
would have destroyed the evidence for telling them apart. From the committed artefacts:

- **No `penalty-probe.sh` figure reaches the headline.** `derive.py:548-552` computes
  `attributed` and `residual` from `cycle_activity.stalls_l3_miss` alone; `penalty_table` is
  read at exactly one site (the §5.4 modelled cross-check). The one real consequence is
  §5.4's precision claim, corrected there.
- **Every IMC row behind "98.9% on the engine's own socket" is present and non-empty**: 6/6
  reps, **288/288** `S<n>` rows (12 IMCs × {read,write} × {S0,S1} × 6), zero
  `<not counted>`/`<not supported>`, so the `0 GB/s` default was never taken. Recomputed
  independently from the raw CSVs, the S=6/N=16 own-socket share is 0.9881 / 0.9886 / 0.9886,
  median **98.86%**.
- **No counter was multiplexed and no recorded rc was nonzero**: **587** counter rows across
  all 45 perf CSVs in the PR at `enabled% = 100.00`, and **42** recorded `rc` values across
  all 12 `meta*.json` at 0 — *including* `loadgen_interior` and `loadgen_uncore`, the two arms
  ④ omitted. So every fail-open path was **un-taken**, and the figures stand on their own
  evidence rather than on a guard that would not have caught a failure.

**Each fix is demonstrated load-bearing rather than asserted.** `selftest-guards.sh` runs 37
cases in seconds with no perf, no root and no bare metal — each guard is handed the bad input
it now catches *and* the good input it must still accept, since a guard that rejects
everything is how ① got in. The bad input for ② is not simulated: it is the contaminated
`penalty/` CSVs still committed here, i.e. the defect's own output being refused. Each fix was
then **reverted in place and the selftest re-run**; every mutation is caught
(`guard-selftest/mutation-matrix.md`). Two results from that matrix are worth carrying
forward:

- The two halves of ②'s window check flip **disjoint** cases, which *measures* their
  complementarity instead of asserting it. The absolute ceiling alone misses a row
  contaminated by +3.2% (the real `LLC_8M` value); the cross-row uniformity check alone
  misses a **uniformly** inflated sweep, because its reference is derived from the very data
  it is checking — CLAUDE.md's vacuous-pass shape, reproduced and then closed.
- Reverting ④ broke a **fourth** case the finding never named: the guard's "no arms at all"
  subject test. A roster that quietly covers a subset is indistinguishable from one that
  covers everything, and the only defence is printing what was actually checked.

**The captured data is immutable, so no fix changes a number** — verified, not assumed:
re-deriving from the committed tree yields 1,109 leaf values of which **13 differ, all 13
being the invocation's own path strings**. `attributed = +3,821.3` and `residual = 32.24%`
reproduce exactly, and the selftest asserts that as a case.

## 8. Acceptance criteria — discharged or not, explicitly

| AC | verdict | evidence |
|---|---|---|
| **1** host capability VERIFIED before the run, probe committed | **PASS** | §1; `host/ac1-capability-probe.txt`, committed at `4b2bc33` *before* any measurement. All three counters program. `perf stat -M MemoryBandwidth` absent — recorded, not hidden. |
| **2** both #3217 endpoints reproduced, same geometry, rows/s in band or divergence explained | **PASS** | §3.1, §4.2. Geometry exact on both row oracles; sha256 re-verified after the last capture; `--batch-size 8192`; +3.8% on aggregate rows/s; the *smaller* decay on this host explained (§4.2) rather than absorbed. |
| **3** per point per row: LLC-load-misses, LLC-loads, cache-references + bandwidth figure, source **named** | **PASS** | §4.4, §4.5. Source named as `uncore_imc cas_count` with `--per-socket`; `-M MemoryBandwidth` explicitly unavailable; channel summing validated two ways. |
| **4** cycles-per-row accounting, penalties stated and sourced, **residual as a number** | **PASS** | §5. **Residual = +1,818.1 cycles/row = 32.24%** (strict DRAM boundary); 20.58% at the wider boundary. Penalties measured on this host (§3.9); the additive decomposition closes with 0.000 error. |
| **5** explicit saturation verdict, measured vs achievable peak on the same host | **PASS** | §6. 24.43 vs 112.54 GB/s = **21.7% — not saturated**; peak measured at the engine's own pinning and NUMA binding; both byte bases reported and the quoted one named. |
| **6** byte basis named on every throughput figure; geometry + sha256 recorded; `now`-pinning N/A recorded | **PASS** | §3.1 (geometry, sha256, `now` N/A with the reason: no TTL, no tombstones, min/max local deletion time `9223372036854775807`); §4.5 and §6 name the basis on every bandwidth figure. |
| **7** a well-measured negative is a pass; do not round toward the hypothesis | **PASS** | §6 reports **21.7% of peak — the memory system is NOT saturated**, which is a negative on the bandwidth hypothesis, stated plainly. §5.4 rejects the penalty charge that would have "attributed" 140% of the delta, and §5.1 publishes the ±5% basis uncertainty rather than picking the flattering arm. |

**Not discharged, and deliberately so:** the ~32% residual is *named as unattributed*, not
explained (§7.2); and whether #3096 moves slope or level is left open with a stated criterion
(§6.1) rather than guessed.

### 8.1 Reproducibility — every headline re-derives from committed artefacts alone (#3226)

`results/derive.py` reads **only** committed inputs; nothing in this report is a number
typed by hand. Verified by re-running it in a **clean detached checkout with no access to
`/data/ws0`**, where the captures were produced:

```bash
git worktree add --detach /tmp/clean-3224 HEAD
cd /tmp/clean-3224/docs/reports/ws0-3224-artifacts
python3 results/derive.py results --penalty-summary penalty/summary.txt
```

The output is **byte-for-byte identical** to the committed
`results/derived-summary.txt` — transcript in
`results/reproduce-from-clean-checkout.txt`, verified at commit `7cb478a`. That
transcript depends only on `results/` and `penalty/`; any later commit touches report
prose alone, and the transcript names the `git diff --stat` a reviewer can run to confirm
those inputs are unchanged.

The window, the row counts, the CPU sets and the corpus size are all **read from the
artefacts**, never hardcoded, and the script **refuses** (non-zero exit, named diagnosis)
on a multiplexed counter, a `<not supported>` counter, a failed occupancy gate, a warmth
violation or a saturated client. Those refusals were **negative-tested**, not assumed: each
of the five was induced against a real rep and confirmed to exit non-zero.
