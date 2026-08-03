# Full-box C(N) + `do_get` handoff attribution — CQLite #3217

**Measured 2026-08-02/03 on one AWS box** (Intel Xeon Platinum 8488C, 16 logical / **8 physical
cores**, SMT on, 1 NUMA node, 30 GB RAM, 295 GB NVMe at `/data`), Ubuntu kernel
`6.17.0-1019-aws`. CQLite built from `main` @ **`693ae41`** (post-#3058). Same machine class as
#3100's `c7i.4xlarge`, which is what makes the S=1 control a direct comparison rather than an
analogy.

This run extends #3100 (PR #3216), which measured the **single-pinned-core** C(N) curve and
acquitted the device, and which explicitly **declined** off-CPU profiling. Two things had never
been measured: the **full-box** C(N) curve (the marginal-efficiency discount every #2817 per-box
projection quotes), and **stack-level attribution** of the concurrency overhead.

Raw artefacts: `docs/reports/ws0-3217-artifacts/`. **Measurement only — no production code was
changed, and no fix is proposed here.** Anything the data indicts is groomed as a follow-up issue.

---

## 1. Executive summary — two verdicts

### Verdict 1 — the full-box-collapse hypothesis is **FALSIFIED**

The pessimistic scenario #3217 was opened to rule in or out was that the per-core handoff discount
**compounds** across cores, so a 16-vCPU box delivers only ~2–3 cores of work and a handoff fix is
a 2.5–4× box-level lever. It does not compound. Marginal efficiency against the best a single
physical core achieves (252,420 rows/s at N=2):

| S (physical cores) | best aggregate rows/s | marginal efficiency vs 1-core peak |
|---:|---:|---:|
| 1 | 252,420 | 1.000 |
| 2 | 440,677 | **0.873** |
| 4 | 818,747 | **0.811** |
| 6 | **1,076,917** | **0.711** |

Six cores retain **71%** marginal efficiency (83% against the naive N=1 denominator) at **96.7%
server utilisation**. The loss is ~29%, not the 60–75% the collapse scenario required. **A perfect
handoff fix is worth at most ~1.2–1.4× at box level, not 2.5–4×.**

### Verdict 2 — the `do_get` mpsc handoff is **ACQUITTED**

Off-CPU stacks and park-event counts both say the same thing, and they are independent instruments:

- **Zero** voluntary parks on the `do_get` mpsc handoff at every park-count point (s1-N1, s6-N1,
  s6-N16). Not "few" — measured zero.
- **≤1.46 s** of `do_get_batch` blocked time out of **1,963 s** total at the worst point
  (≤0.07% of total blocked time).
- `egress_credit_acquire` ≈ 0 s at every point (max 0.0014 s).

The parks are real — but they are **per-chunk and per-128/256-rows inside `cqlite-core`**, below
the Flight layer. The bypass read path stacks **four** bounded channels between SSTable and wire;
"the mpsc handoff" was never one thing, and the one the issue named is not the one that parks.

### What this means for fix ordering

The residual ~29% is **not extra work** and **not the handoff**: instructions/row is **flat
(+0.1%)** from the 1-core peak to the 6-core peak while cycles/row rises **+34.1%** and IPC falls
**−25.4%**. The scaling discount is a **cycles-per-instruction (memory-side) effect**.

Ordering follows directly: **a `do_get`-handoff fix is ordered LAST** — it optimises a site
measured at ~0 cost. #3096 (Arrow encode) reduces per-row *work* and should raise absolute
throughput at every core count, but flat instructions/row means it is **not** specifically a fix
for the *scaling discount*. Those are two different claims and this report keeps them separate
(§8).

**And the #1 next step is neither fix — it is a MEASUREMENT.** Both candidate levers are currently
unfounded with respect to the scaling discount (one costs nothing; the other's relevance is
unknown), so the highest-value action is establishing the microarchitectural cause of the IPC decay
on a host with working LLC / memory-bandwidth counters. That single result decides whether *any*
per-row-work lever moves the slope. It is two endpoint points and one afternoon, gated only on host
selection. **Fund it before either fix** (§8).

### What is NOT established

The **microarchitectural cause of the IPC decay**. `LLC-loads`, `LLC-load-misses` and
`cache-references` are `<not supported>` on this virtualized host. L1d (+7.5% relative) and dTLB
(+40% relative) together account for only ~10–13% of the +8,593 cycles/row; **~87% is
unattributed**. LLC / memory-bandwidth saturation is the natural hypothesis and it was **not
measured**. That is the one genuinely open question this run leaves (§7, §10).

---

## 2. Method

### 2.1 Environment and verified topology

| | |
|---|---|
| CPU | Intel(R) Xeon(R) Platinum 8488C — 8 physical / 16 logical, SMT on, 1 NUMA node (`node0`) |
| SMT siblings | `(c, c+8)` — **read** from `/sys/devices/system/cpu/cpu*/topology/thread_siblings_list`, never assumed; re-derived by every script and written to `cpu-topology.json` beside each result set |
| RAM / disk | 30 GB / 295 GB NVMe at `/data` |
| Kernel | `6.17.0-1019-aws` |
| Instruments | `perf`, `bpftrace`, `offcputime-bpfcc` (patched, §2.6), `runqlat-bpfcc`, FlameGraph; `perf_event_paranoid=-1`, `kptr_restrict=0`, **re-asserted before every capture** (they silently revert to `4`/`1`) |

Cassandra was **stopped** before any measurement — nothing JVM-side competed for CPU.

### 2.2 Core allocation — physical-core basis throughout

Both SMT siblings of a physical core are always pinned together. `sweep.sh` **refuses to run** if
the server and client sets overlap, because a shared CPU would make `perf stat -C <server-cpus>`
count client work as engine work.

| arm | server CPUs | physical cores |
|---|---|---:|
| S=1 (AC2 control — #3100's exact set) | `2,10` | 1 |
| S=2 | `0,2,8,10` | 2 |
| S=4 | `0-3,8-11` | 4 |
| S=6 (full box, AC1) | `0-5,8-13` | 6 |
| client (fixed, all arms) | `6,7,14,15` | 2 |

The client is a **constant** across the whole curve, so no arm buys throughput by taking client
cores. Client CPU% is published at **every** (S,N) — see §3.5.

### 2.3 Corpus — regenerated, geometry-matched, NEW sha

Regenerated on this box from the committed `ws0-3026-artifacts/ws0-corpus/gen-corpus.sh` recipe:
Apache Cassandra **5.0.8** tarball, `MAX_HEAP_SIZE=8G`, `cassandra-stress` user profile,
`nodetool flush` per 50k-partition batch then `flush` + `compact` → exactly **one `nb-16-big`
SSTable**, 8 components. `cassandra-stress` is not byte-deterministic, so the accepted bar — the
one #3100 itself set — is **matched geometry + a documented new sha**.

| metric | this run | #3100 | delta |
|---|--:|--:|--:|
| rows | 3,999,890 | 3,999,890 | **0** |
| `totalColumnsSet` | 35,999,010 | 35,999,010 | **0** |
| partitions (estimate) | 198,130 | 198,130 | 0 |
| uncompressed B/row | **693.29** | 692.70 | +0.085% |
| on-disk `Data.db` bytes | 784,334,710 | 784,116,369 | +0.028% |
| LZ4 ratio (header-derived) | 3.5356× | 3.5353× | +0.008% |
| chunk count (16 KiB chunks) | 169,257 | 169,194 | +63 |
| droppable tombstones | 0.0 | 0.0 | same |
| SSTable count / format | 1 / `nb-16-big` | 1 / `nb-16-big` | same |

**New sha256(Data.db) = `3a4ee5cd5ef5937ae52a703cca0ee0359df8ecb959915dea66b3b89f9a9c7c1e`**
(#3100's was `2c297a0c…` — different, as expected). Row count is **double-oracled**:
`sstablemetadata totalRows` **and** an independent `fullscan.py` over 512 token ranges both give
3,999,890. Never a CQL `count(*)` — that server-side-times-out past 4M rows.

Recipe deviations, all recorded in `ws0-3217-artifacts/corpus/corpus-provenance.txt`: JDK 11.0.31
instead of 17 (only JDK on the box; Cassandra 5.0 supports both, daemon and stress both clean);
root `/data/ws0` instead of `/home/ubuntu/ws0`; `cassandra.yaml` **is** patched here, so the
SSTables landed in the configured data dir rather than #3100's CASSANDRA_HOME-relative default
(location only, no format or geometry effect).

**Byte bases (AC6) — never a bare MB/s.** Three bases for the same rows, differing by ~3.5×:

| basis | value | derivation |
|---|--:|---|
| logical / uncompressed | **693.29 B/row** | `CompressionInfo.db` `dataLength` ÷ rows |
| on-disk / compressed | **196.09 B/row** | exact sum of `*-Data.db` ÷ rows |
| Arrow wire | **capacity, not wire bytes** | `flight-loadgen` reports Arrow buffer *capacity*; it is **not** gRPC-on-the-wire bytes and is labelled as such wherever it appears |

**`now`-pinning: N/A.** The `ws0.events` fixture carries **no TTL and no tombstones**
(`sstablemetadata`: min/max local deletion time = `9223372036854775807`, TTL 0/0, estimated
droppable tombstones 0.0), so no read-time reconciliation depends on `now`. Keep it so.

### 2.4 Binaries and build flags

`cqlite-flight` + `flight-loadgen`, built in this worktree only, from `main` @ **`693ae41`**:

```
CARGO_PROFILE_RELEASE_STRIP=none CARGO_PROFILE_RELEASE_DEBUG=true \
  RUSTFLAGS="-C force-frame-pointers=yes" \
  cargo build --release -p cqlite-flight -p flight-loadgen
```

`[profile.release]` hardcodes `strip = true`, hence the env overrides. Frame pointers because
`--call-graph=dwarf` **hangs** past 120 s on this binary (§2.6 trap 6).

Server flags fixed to #3100's recorded invocation: `--batch-size 8192 --max-batch-bytes 4194304
--max-inflight-egress-bytes 12582912 --max-concurrent-scans 16 --admission-wait-timeout-ms 30000`.

### 2.5 Harness provenance — built from scratch, because #3100's was never committed

#3100 committed its C(N) **logs but not its driver**, so the sweep driver had to be reconstructed
from the recorded invocation. That is a repeatability gap this run closes: the harness is now a
**retained, committed artefact**, so the next run inherits the instrument and not merely the
numbers. `ws0-3217-artifacts/harness/` (13 files incl. `selftest.sh`, 36 checks that need neither
corpus nor live server), `partA-run/`, `partB-run/`.

### 2.6 Traps — recorded as METHOD, so the next off-CPU run inherits the guard

The first three all produce an **empty or truncated off-CPU profile**, which reads **identically
to "the mpsc handoff is innocent."** That false negative is exactly what this issue exists to
prevent — which is why they are method, not incidents. An empty profile and an acquittal are the
same picture; only the guard tells you which one you are looking at.

1. **A permissive `kernel.perf_event_paranoid` does NOT cover BPF map creation.** It governs perf
   events, not BPF maps. Unprivileged, bcc fails with `could not open bpf map: warn_events, error:
   Operation not permitted` and bpftrace refuses outright. **BPF collectors must run under
   `sudo`**; `perf` itself does not, once the sysctls are set. *If you miss this, you get a
   zero-byte off-CPU profile that looks like "nothing blocks."*
2. **`offcputime` charges a blocked interval only on switch-IN.** A thread that never wakes inside
   the window contributes **zero**, so a single long-sleep probe records no off-CPU time at all.
   This bit the dry run. The window must be long enough to contain the wake-ups of the intervals
   you care about. *If you miss this, you again get an empty profile that looks like an
   acquittal.*
3. **bcc `offcputime`'s `counts` map silently truncates at 10,240 keys.** The `BPF_HASH` default;
   `--stack-storage-size` sizes a *different* map and does not fix it. Round-1 N≥8 captures are
   **invalid** — 10,240 keys captured against 108,475 real unique stacks at s6-N16. Patched
   collector (`partB-run/offcputime-bigmap`, 1e6-key map) used for every quoted figure; round-1
   captures are marked **superseded** in the inventory rather than deleted. Residual `stack traces
   lost` even at the larger size: 0/149/251 (s6) and 20/42/29 (s1) — small, non-zero, reported not
   hidden.
4. **Rust v0 mangling defeats a v1-mangling classifier.** bcc emits raw `_RNvNvMs0_…` symbols; the
   v1 match table held demangled spellings, so almost nothing matched and 76–83% of blocked time
   fell into `other`. Fixed by demangling first (`partB-run/demangle-folded.py`).
5. **bpftrace `ustack` left ~50% of user frames as raw hex** where `perf script` symbolized the
   same stacks completely. The bpftrace park-count captures are retained for provenance and
   **superseded** by the `perf sched:sched_switch` runs.
6. **`--call-graph=dwarf` hangs** past 120 s against this binary — frame pointers instead.
7. **sched tracepoints are `root:root 0640`** — `perf_event_paranoid=-1` does **not** cover
   tracefs, so `perf record -e sched:sched_switch` needs sudo independently of the sysctl.
8. **`kernel.perf_event_paranoid` silently reverts to 4** on its own schedule; re-asserted (and
   asserted to have taken) before every capture. A stale value surfaces as unsymbolized kernel
   frames — i.e. as an AC3 failure with a misleading cause.
9. **The Part B parsers depend on `rust_demangler`, and its absence degrades quietly.** bcc and
   bpftrace emit raw Rust v0 symbols; `demangle_helper.py` returns an undecodable frame *unchanged*
   rather than failing, so `classify-offcpu-v2.py` without the module still runs, still emits a
   plausible table, and **mis-buckets nearly everything** — measured while re-deriving these
   artefacts, `mpsc_send_park` at s6-N1 collapsed from 50.57 s to 2.89 s. Same family as traps 1–3:
   a broken instrument that produces output rather than an error. Run the Part B parsers with an
   interpreter that has the module and check that no `_RN`-prefixed symbol survives into the table.
10. **`pkill -f '<pattern>'` matches the launching shell.** Part A's runner killed its own sampler
   this way. Nothing here calls `pkill`; `ws0_stop_server` takes an explicit PID and `selftest.sh`
   asserts the launching shell survives.

### 2.7 Validity controls actually enforced

- **Client-saturation gate.** Client CPU% recorded at every (S,N); >70% of the 2-core client
  budget stamps the point `INVALID_CLIENT_SATURATED` and excludes it from the curve. Max observed
  **0.138**. **Zero points excluded.**
- **Warmth verified, not assumed.** `/proc/<pid>/io` `read_bytes` = **0** summed across all
  **83** recorded points — every read served from page cache. (`rchar`/`syscr` are small because
  `Data.db` is mmap'd, so cache hits are faults, not `read()` syscalls; the three are reported
  raw, side by side, and never divided by one another.)
- **Admission clean.** `requests_unavailable` = 0 and `requests_error` = 0 on all 83 points.
- **≥3 reps per curve point** with min/median/max dispersion. The two `calib-*` rows are reps=1 and
  show `spr% 0.0`, which is **absent** dispersion, not a measured zero — they are labelled and
  excluded from every curve.

---

## 3. Part A — the full-box C(N) curve

Medians of 3 reps, 120 s per step, warm, `CQLITE_FLIGHT_MERGE_PATH=bypass` unless stated.
Artefacts: `ws0-3217-artifacts/results/partA-analysis.{txt,json}` + per-arm dirs.

### 3.1 C(N) per S, with dispersion

Aggregate rows/s (median), with min–max spread as % of median in parentheses:

| N | S=1 | S=2 | S=4 | S=6 |
|--:|---|---|---|---|
| 1 | 216,229 (3.9%) | 205,129 (3.2%) | 175,872 (4.7%) | 163,510 (2.6%) |
| 2 | **252,420** (5.4%) | 325,364 (6.7%) | 340,878 (12.3%) | 332,165 (10.7%) |
| 4 | 240,361 (2.2%) | 421,621 (3.0%) | 565,409 (2.7%) | 601,074 (4.5%) |
| 8 | 220,865 (2.1%) | **440,677** (1.1%) | 721,434 (1.3%) | 916,066 (2.7%) |
| 16 | 211,010 (1.9%) | 417,424 (1.1%) | **818,747** (0.9%) | **1,076,917** (0.5%) |

Per-stream rows/s at each arm's peak: S=1 126,210 (N=2) · S=2 55,085 (N=8) · S=4 51,172 (N=16) ·
S=6 67,307 (N=16).

**Byte basis at the headline point** (S=6, N=16, 1,076,917 rows/s): **746.6 MB/s logical /
uncompressed** (× 693.29 B/row) · **211.2 MB/s on-disk compressed** (× 196.09 B/row) ·
13.13 GB/s Arrow buffer **capacity** (not gRPC wire bytes).

Two dispersion outliers — S=4/N=2 at 12.3% and S=6/N=2 at 10.7%. Low N on a wide core set is the
least stable region (few streams, many idle cores, work-stealing). The ≥3-rep requirement is what
caught them; at reps=1 they would have shipped invisibly.

### 3.2 Cross-S marginal efficiency — the deliverable table, BOTH denominators

Per-arm self-normalised speedup columns (each arm ÷ its own N=1) are **NOT cross-comparable**,
because each arm's own N=1 **declines** with core count — 216,229 (S=1) → 205,129 (S=2) → 175,872
(S=4) → 163,510 (S=6). A single stream spread across 12 hardware threads loses to work-stealing
and locality. Self-normalising would flatter the wide arms by dividing them by a worse baseline.
So both denominators are published and neither is silently chosen:

| S | best aggregate rows/s | N@peak | srv util | speedup vs **1-core peak** | **marg. eff. vs 1-core peak** | speedup vs 1-core N=1 | marg. eff. vs 1-core N=1 |
|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 | 252,420 | 2 | 0.996 | 1.000 | **1.000** | 1.167 | 1.167 |
| 1 (independent re-run) | 249,985 | 2 | 0.995 | 0.990 | 0.990 | 1.156 | 1.156 |
| 2 | 440,677 | 8 | 0.995 | 1.746 | **0.873** | 2.038 | 1.019 |
| 4 | 818,747 | 16 | 0.992 | 3.244 | **0.811** | 3.786 | 0.947 |
| 6 | **1,076,917** | 16 | 0.967 | 4.266 | **0.711** | 4.980 | 0.830 |

**Reference B (S=1's peak, 252,420 rows/s at N=2) is primary**: it is the most the engine achieves
on one physical core, so it is the fair "perfect scaling" unit, and it is the **conservative**
choice — it yields lower efficiencies than reference A. Reference A (S=1 at N=1, 216,229 rows/s) is
reported alongside because it is the naive baseline.

An independent S=1 re-run (`cn-s1-ac5`) landed within **1.7%** at every N, so the reference itself
is reproducible.

### 3.3 AC2 — the S=1 shape reproduces #3100

| N | measured | normalised | #3100 published | normalised | measured/published |
|--:|--:|--:|--:|--:|--:|
| 1 | 216,229 | 1.000 | 246,940 | 1.000 | 0.876 |
| 2 | **252,420** | **1.167** | **287,441** | **1.164** | 0.878 |
| 4 | 240,361 | 1.112 | 273,438 | 1.107 | 0.879 |
| 8 | 220,865 | 1.021 | 248,621 | 1.007 | 0.888 |
| 16 | 211,010 | 0.976 | 236,734 | 0.959 | 0.891 |

**Peak-at-N=2-then-decline reproduced, within ≤1.8 pp at every N.** The absolute ratio is a
near-constant **0.876–0.891**. That *uniformity* is the evidence: a methodology divergence would
bend the shape, whereas a constant multiplier is a **level shift** (different box instance,
regenerated corpus with a new sha). AC2 is about the shape, and the shape is the same curve.

### 3.4 merge vs bypass (N=1 reference points)

| S | bypass rows/s | merge rows/s | bypass advantage |
|--:|--:|--:|--:|
| 1 | 216,229 (3.9% spread) | 72,632 (0.14% spread) | **2.98×** |
| 6 | 163,510 (2.6% spread) | 63,762 (1.3% spread) | **2.56×** |

Slightly under #3058's published 3.06–3.26×, and — worth noting on its own — the bypass advantage
**shrinks** as core count grows. The `CQLITE_FLIGHT_MERGE_PATH` kill-switch remains a real,
selectable seam.

### 3.5 Client headroom — CPU% at EVERY (S,N), per the owner's validity gate

Server / client utilisation of each pinned set (median of reps). The client budget is 2 physical
cores at every arm; the gate is 70%.

| arm | N=1 | N=2 | N=4 | N=8 | N=16 |
|---|---|---|---|---|---|
| S=1 srv / **cli** | 78.2 / **2.2** | 99.6 / **2.6** | 99.8 / **2.9** | 100.0 / **2.7** | 100.0 / **2.6** |
| S=2 srv / **cli** | 42.4 / **2.7** | 77.1 / **3.8** | 95.7 / **4.8** | 99.5 / **5.0** | 99.9 / **4.9** |
| S=4 srv / **cli** | 18.1 / **2.7** | 40.5 / **4.3** | 76.6 / **6.7** | 94.7 / **8.3** | 99.2 / **9.6** |
| S=6 srv / **cli** | 11.5 / **2.8** | 25.0 / **4.7** | 53.4 / **7.4** | 88.9 / **11.5** | 96.7 / **13.8** |
| S=1 merge N=1 | 82.0 / **1.8** | — | — | — | — |
| S=6 merge N=1 | 12.9 / **2.0** | — | — | — | — |

Max client utilisation across the entire run: **0.138**, against a 0.70 gate — roughly **5× of
headroom** at the hardest point. **Zero points excluded on validity grounds**; no server core was
ever traded back, so every arm is honestly labelled at its stated core count.

A finding worth carrying forward: **#3100's 4-physical-core client was over-provisioned by about
an order of magnitude.** Two client cores drove 1.08M rows/s at 13.8% busy.

---

## 4. Part B — attribution

Artefacts: `ws0-3217-artifacts/partB-results/` (`partB-analysis.{txt,json}`, `oncpu/`, `offcpu/`,
`park-counts/`, `scheduler/`, `counters/`, `raw-capture-inventory.txt`).

### 4.1 AC3 — on-CPU flame graphs, unsymbolized-frame gate (<10%)

`perf record -F 999 --call-graph=fp -C <server-cpus>`, 30 s steady-state windows.

| profile | unsym (all) | unsym (server threads) | gate | server share of capture |
|---|--:|--:|:--:|--:|
| oncpu-s1-N1 | 0.0263% | 0.0269% | **PASS** | 97.6% |
| oncpu-s1-N8 | 0.0124% | 0.0124% | **PASS** | 100.0% |
| oncpu-s1-N16 | 0.0094% | 0.0094% | **PASS** | 100.0% |
| oncpu-s6-N1 | 0.0275% | 0.0269% | **PASS** | 79.2% |
| oncpu-s6-N8 | 0.0236% | 0.0239% | **PASS** | 95.0% |
| oncpu-s6-N16 | 0.0189% | 0.0189% | **PASS** | 98.8% |

All six pass by ~500×. The lower server share at s6-N1 is the box being 88% idle at that point
(`swapper` is 19.0% of the capture) — expected, not a defect.

**Caveat that qualifies the gate — this is NOT "fully symbolized."** **16.86–17.94%** of
*frame instances on server threads* resolve only to the DSO `[libc.so.6]` with no function symbol
— that is the glibc allocator, shipped without symbols. Those frames are *not* counted as
unsymbolized by the gate (they carry a DSO name), so the 0.009–0.028% figure understates how much
of the profile is opaque at function granularity.

| profile | DSO-only `[libc.so.6]`, **server threads** | same, all frames in capture |
|---|--:|--:|
| oncpu-s1-N1 | 17.41% | 17.07% |
| oncpu-s1-N8 | 17.63% | 17.62% |
| oncpu-s1-N16 | **17.94%** | 17.94% |
| oncpu-s6-N1 | 17.25% | 14.25% |
| oncpu-s6-N8 | **16.86%** | 16.20% |
| oncpu-s6-N16 | 17.15% | 16.99% |

**Definition and artefact, so the figure is checkable rather than asserted.** A *DSO-only* frame is
one `perf script` prints as a bare `[<dso>]`: the address mapped to a shared object but to no
function symbol. The metric is **weighted frame instances** — `sum(weight × matching frames) /
sum(weight × frames)` — excluding `[unknown]` (that is the gate's own metric) and pseudo-DSOs
`[[vdso]]` / `[[anon:*]]`. The **server-threads-only** basis is the quotable one, because the claim
is about the *server's* opacity; the all-frames basis is diluted by `swapper`/loadgen/`bash` stacks
that carry no libc frames at all (visible above at s6-N1, where the box is 88% idle: 14.25% vs
17.25%). Both bases, plus an any-DSO variant, are emitted per profile by
`partB-run/summarize-oncpu.py` into **`partB-results/oncpu/AC3-oncpu-summary.json`** (fields
`frame_weighted_dso_only_libc_server_threads_only` / `…_all`, band under
`dso_only_libc_band_server_threads_only_pct`) and printed as the `DSO-only srv%` column of
`AC3-oncpu-summary.txt`. The roll-up reads the **committed** `oncpu/*.folded.gz`, so the band is
reproducible from this repo alone after the measurement box is gone.

`[libc.so.6]` is also the single largest *leaf* in every profile (21.9–24.7% of server weight). The
allocator's *presence* is therefore well established; its internal breakdown is not.

Cost centres are stable across the matrix — at s6-N16: `cqlite_core` 59.5%, `alloc` 11.1%,
`cqlite_flight` 8.5%, `tokio` 7.4%, `std` 6.6%, `arrow` 5.7%, `tonic` 0.8%, `h2` 0.16%,
`hyper` 0.08%, `prost` 0.0003%. The gRPC stack is a rounding error on-CPU.

### 4.2 AC4 — off-CPU blocked-time attribution, all seven buckets

Collector: **patched** `offcputime-bpfcc` (1e6-key counts map — the stock 10,240-key map saturated
at N≥8 and silently dropped stacks, §2.6 trap 3). Classifier v2, **leaf-first**. Seconds blocked
over a 30 s window; **an explicit 0 means measured absent**, never omitted.

| capture | total | egress_credit | mpsc_send | mpsc_recv | tonic/socket | disk_io | tokio_sched | other |
|---|--:|--:|--:|--:|--:|--:|--:|--:|
| s1-N1 | 160.19 | 0.0001 | 49.12 | 5.97 | 1.43 | **0.0000** | 103.55 | 0.12 |
| s1-N8 | 1048.95 | **0.0000** | 528.28 | 147.86 | 10.36 | **0.0000** | 361.95 | 0.50 |
| s1-N16 | 2008.56 | 0.0009 | 1100.62 | 331.30 | 18.96 | **0.0000** | 557.45 | 0.22 |
| s6-N1 | 210.35 | **0.0000** | 50.57 | 9.12 | 1.08 | **0.0000** | 149.58 | **0.0000** |
| s6-N8 | 947.01 | 0.0005 | 468.22 | 36.03 | 6.60 | 0.0001 | 435.85 | 0.32 |
| s6-N16 | 1963.72 | 0.0014 | 990.42 | 190.95 | 19.35 | **0.0000** | 760.32 | 2.68 |

`disk_io` = 0 is correct and expected — the corpus is fully page-cached (§2.7). `other` is ≤0.14%
of total blocked time everywhere and is broken out by named cause, with whatever matches no named
cause shown as an explicit **`unclassified_residual`** line — **≤0.11% of total blocked time on
every capture, reported not hidden** (worst case s6-N16: 2.13 s of 1,963.72 s). `tokio_scheduler`
is **≥99.99% idle-runtime park** (at s6-N16: io-driver epoll 306.6 s, blocking-pool idle 233.6 s,
idle worker park 220.0 s, timer 0.05 s, `unclassified_residual` 0.06 s) — i.e. threads with nothing
to do, not overhead. Neither claim is "nothing is left unnamed": a bounded, labelled, quantified
residue exists in both, and saying otherwise while printing the residue line would be a
self-contradiction a reader is entitled to hold against every other number here.

**The channel-identity split — the load-bearing distinction.** The bypass read path stacks **four**
bounded channels between SSTable and wire; lumping them into one "mpsc" bucket would attribute
core-read-path parks to the Flight handoff, which is the exact wrong answer. Every send/recv park
is therefore also tagged with its channel, identified from the channel's **item type** in the
demangled symbol:

| channel | item type | capacity | granularity |
|---|---|--:|---|
| `do_get_batch` — **THE #3217 handoff** | `CreditedBatch` / `ChannelSink` | 4 | 1 per RecordBatch (8,192 rows) |
| `core_raw_chunk` | `Sender<bytes::Bytes>` | 8 | 1 per 16 KiB compression chunk |
| `core_query_rows` | `QueryRowMsg` (std `sync_channel`) | 4 | 1 per 128 rows |
| `core_windowed_batch` | `Vec<(RowKey, ScanRow)>` | 2 | 1 per 256 rows |

Capacities are the production constants: `DO_GET_CHANNEL_CAPACITY` = 4 and `IN_FLIGHT_ALLOWANCE` =
3 (`cqlite-flight/src/streaming.rs`); `RAW_CHUNK_CHANNEL_CAP` = 8, `BATCH_EMIT_ROWS` = 256,
`BATCH_CHANNEL_CAP` = 2 (`cqlite-core/src/storage/sstable/reader/scan_stream_windowed.rs`);
`QUERY_ROWS_PER_BATCH` = 128, `QUERY_ROWS_CHANNEL_BATCHES` = 4
(`…/summary_scan/query_rows.rs`).

Blocked seconds by channel:

| capture | **do_get_batch** | core_raw_chunk | core_query_rows | core_windowed_batch | unattributed |
|---|--:|--:|--:|--:|--:|
| s1-N1 | **0.0001** | 27.08 | 15.53 | 9.46 | 3.02 |
| s1-N8 | **0.4666** | 238.05 | 230.83 | 146.48 | 60.31 |
| s1-N16 | **0.5880** | 477.75 | 468.33 | 369.31 | 115.94 |
| s6-N1 | **0.0000** | 27.46 | 16.70 | 12.64 | 2.89 |
| s6-N8 | **0.2940** | 229.09 | 138.09 | 93.53 | 43.23 |
| s6-N16 | **1.4563** | 473.23 | 379.27 | 217.79 | 109.63 |

At the worst point the `do_get` handoff is **1.46 s of 1,963.72 s = 0.074%** of blocked time, and
**0.12%** of the `mpsc_send`+`mpsc_recv` total. That is the acquittal.

### 4.3 Blocked time is NOT lost throughput — read this before reading §4.2

This deserves its own subsection because the numbers above invite a specific misreading.

At s6-N16 the profile records **990 s of `mpsc_send` blocked time** over a 30 s window. That is
not a 990 s cost, and it is not 990 s of lost work. Two reasons:

1. **It is summed across ~32 server threads.** 30 s of wall clock across 32 threads is up to
   ~960 thread-seconds of *capacity*; blocked seconds are drawn from that pool, not from wall
   clock. A number larger than the window is arithmetically normal and means nothing on its own.
2. **The same run measures 96.7% server utilisation.** When one thread parks on a full bounded
   channel, another thread runs on that core. The blocked interval is **overlapped**, not lost.
   With only 3.3% of the pinned set idle, there is at most ~3.3% of throughput that *any* amount
   of blocking could be costing — and Part B's own closure model attributes exactly 2.1 pp of the
   29% shortfall to residual idle (§6).

The corroborating evidence that this is overlap and not cost: **instructions/row is flat (+0.1%)**
between the 1-core peak and the 6-core peak. If parking were destroying work, the engine would be
executing more instructions per row (retries, re-polls, spin) or fewer rows per instruction. It is
executing the *same* instructions, *slower per instruction*.

So the correct reading of §4.2 is **compositional, not quantitative**: it tells you *which* sites
the system waits on (and that the `do_get` handoff is not one of them), not *how many rows/s* the
waiting costs. At 96.7–99.6% utilisation, the answer to "how many rows/s" is: almost none.

### 4.4 Park COUNTS by site — the instrument `offcputime` cannot be

`offcputime` charges *duration*, so a **frequent-but-short** park is invisible to it. Park counts
come from `perf record -e sched:sched_switch` with stacks, fully symbolized (`perf script`
resolves what bpftrace's `ustack` left ~50% hex).

Aggregate, 10 s windows:

| capture | rows/s | voluntary/s | involuntary/s | voluntary parks per 8,192-row batch |
|---|--:|--:|--:|--:|
| sched2-s1-N1 | 206,861 | 25,486 | 8,775 | 1,009 |
| sched2-s6-N1 | 167,999 | 53,666 | 3 | **2,617** |
| sched2-s6-N16 | 1,086,267 | 134,736 | 59,383 | 1,016 |

Voluntary parks per 8,192-row Flight batch, **by site** (an explicit 0 is measured absent):

| site | s1-N1 | s6-N1 | s6-N16 | % of voluntary at s6-N16 |
|---|--:|--:|--:|--:|
| **do_get_mpsc_handoff** | **0** | **0** | **0** | **0.00%** |
| **egress_credit** | **0** | **0** | **0** | **0.00%** |
| core_raw_chunk_chan | 314 | 327 | 265 | 26.1% |
| core_query_rows_chan | 203 | 220 | 201 | 19.8% |
| core_windowed_batch_chan | 8 | 27 | 43 | 4.2% |
| glibc_malloc_arena_lock | 163 | **1,677** | 243 | 23.9% |
| tokio_runtime_idle | 320 | 360 | 263 | 25.9% |
| grpc_egress | 2 | 6 | 1 | 0.14% |
| other | 0 | 0 | 0 | 0.00% |

**Running only one of the two instruments would have given a confidently wrong answer in either
direction.** glibc allocator arena contention is **64% of parks by count** at s6-N1 but only
**~2.0% of blocked time by duration**: count-only says "the allocator is the problem", duration-only
says "the allocator is invisible". The truth is "very frequent, very short" — and it is only
visible because both ran.

### 4.5 AC5 — run-queue latency and context switches

**Coverage differs between the two halves of AC5, and the table headings say which.** The
context-switch half below is **complete across the whole Part A matrix** — all five N (1, 2, 4, 8,
16) at every S, from the per-TID sidecar. The run-queue-latency half covers the **Part B capture
matrix only — N ∈ {1, 8, 16} at S=1 and S=6** — because `runqlat` ran alongside the off-CPU
captures, which were taken at those six points. N = 2 and N = 4 have context-switch data but no
`runqlat` histogram; that is a coverage limit of the instrument's schedule, not a missing
measurement at a point that was profiled.

`runqlat-bpfcc`, log2 buckets, microseconds (bucket-bounded percentiles — the tool reports
buckets, so a point estimate would be fabricated precision). **Part B matrix: N ∈ {1, 8, 16}
only:**

| arm | N | p50 | p90 | p99 | wakeups |
|---|--:|---|---|---|--:|
| S=1 | 1 | [0,1] | [8,15] | [128,255] | 1,089,066 |
| S=1 | 8 | [8,15] | [512,1023] | [4096,8191] | 996,349 |
| S=1 | 16 | [64,127] | [1024,2047] | [8192,16383] | 783,361 |
| S=6 | 1 | [0,1] | [2,3] | [2,3] | 1,177,996 |
| S=6 | 8 | [0,1] | [32,63] | [256,511] | 5,896,098 |
| S=6 | 16 | [4,7] | [128,255] | [1024,2047] | 5,889,067 |

Context switches per N — **complete across all five N at every S** (extract below shows the
endpoints; the full five-N-by-four-S table is `results/partA-analysis.txt`). Cpu-wide rate and —
the load-bearing columns — **per 1,000 rows**,
since a raw rate necessarily rises with throughput):

| arm | N | cs/s cpu-wide | migrations/s | vol cs / 1k rows | nonvol cs / 1k rows |
|---|--:|--:|--:|--:|--:|
| S=1 | 1 | 47,626 | 270 | 120.4 | 42.9 |
| S=1 | 16 | 28,994 | 109 | 100.8 | 32.1 |
| S=2 | 1 | 101,245 | 550 | 225.3 | 0.8 |
| S=2 | 16 | 69,398 | 229 | 107.3 | 50.4 |
| S=4 | 1 | 87,156 | 26 | 233.2 | 0.02 |
| S=4 | 16 | 163,503 | 1,018 | 118.7 | 68.3 |
| S=6 | 1 | 83,835 | 33 | 239.2 | 0.02 |
| S=6 | 16 | 235,367 | 14,604 | 128.9 | 51.2 |

**This adjudicates Part A's counter signal.** Voluntary parks per 1,000 rows **fall** as N rises
(S=6: 239 → 129) while involuntary switches rise three orders of magnitude (0.02 → 51.2 per 1k
rows) and run-queue p99 grows. That is genuine **queueing at saturation** — the scheduler
preempting runnable threads because the cores are full — not growing per-unit channel parking. The
counter-based inference #3100 could only gesture at is now settled by stacks, and the composition
of those voluntary parks never includes the handoff.

Note the AC5 sidecar's known bias: per-TID delta accounting over `/proc/<pid>/task/*/status` at
0.5 Hz can slightly **under**-count (threads born and retired between samples), never over-count.

---

## 5. The ~1,960-parks-per-batch question — answered by SITE; predicted by geometry for `core_raw_chunk` ONLY

> **Read this before the tables.** Every park in these captures is attributed to a **named site**,
> and the site attribution is what acquits the handoff — that part is settled. But "geometry
> predicts the parks" holds **tightly for `core_raw_chunk` only** (347 predicted sends vs 265–327
> measured parks). `core_query_rows` records **3.1–3.4× MORE parks than it has sends** (64
> predicted, 201–220 measured) and `core_windowed_batch` exceeds its send count at the busiest
> point (32 predicted, 43 measured). The explanation offered below — that both endpoints of a
> channel park and that a std `sync_channel`'s blocking path can switch more than once per transfer
> — is a **HYPOTHESIS, and it is unmeasured**. A mid-run claim on the issue thread that "geometry
> predicts these" was overstated for two of the three channels and is corrected here (§ Corrections).

Part A produced one number that made the handoff hypothesis look alive: at **S=6/N=1**, ~39,115
voluntary switches/s against ~20 batches/s ≈ **~1,960 voluntary parks per 8,192-row batch**. A
per-batch mpsc handoff would produce **~1–2**. Something was parking at a far finer granularity
than the batch.

The dedicated `sched:sched_switch` capture on the same arm measures **2,617 parks/batch** (53,666
vol/s ÷ 20.5 batches/s). The gap between 1,960 and 2,617 is instrument, not physics: the Part A
figure comes from the 0.5 Hz per-TID `/proc` sidecar, which under-counts by construction, while
`perf` counts every switch on the server CPU set. Both say "low thousands"; the question is where
they come from.

**They are per-chunk and per-128/256-rows, in `cqlite-core`, below the Flight layer.** Geometry
predicts the dominant term:

| channel | granularity | predicted **sends** per 8,192-row batch | measured **parks** per batch |
|---|---|--:|--:|
| `do_get_batch` | 1 per 8,192-row RecordBatch | **1 reserve + 1 emit** | **0** |
| `core_raw_chunk` | 1 per 16 KiB chunk; corpus = 3,999,890 rows / 169,257 chunks = **23.63 rows/chunk** | **347** | 265–327 |
| `core_query_rows` | 1 per 128 rows | 64 | 201–220 |
| `core_windowed_batch` | 1 per 256 rows | 32 | 8–43 |

The **dominant** term closes tightly: 347 predicted sends against 265–327 measured parks means
76–94% of raw-chunk sends park — exactly what a capacity-8 channel feeding a slower consumer
does. And the `do_get` channel, which does 1 reserve + 1 emit per 8,192 rows, **never parked
once** at any point.

**Where the geometry does *not* close, stated plainly.** `core_query_rows` records **3.1–3.4×
more parks than it has sends**, and `core_windowed_batch` exceeds its send count at the busiest
point (43 vs 32). **The offered explanation is a hypothesis and was not measured**: parks are
*sched_switch events*, not sends, so **both endpoints** of a channel can park (producer on full,
consumer on empty), and `core_query_rows` is a **std `sync_channel`** whose blocking path may
produce more than one switch per transfer. Nothing in these captures distinguishes producer-side
from consumer-side parks, so that account is plausible and untested; confirming it needs a
per-endpoint park breakdown, which is a follow-up measurement, not a claim this run can make.

What the data *does* establish, independent of that hypothesis: the geometry predicts the *order*
of all three channels and the *magnitude* of the dominant one, and the residual is a property of
the counting rather than evidence of an unidentified site — **every park in the capture is
attributed to a named site, with `other` = 0**.

**The conclusion the numbers force:** "the mpsc handoff" was never one channel. Splitting by
channel identity is what turned a plausible story into a measurement, and the channel #3217 named
is the one channel that does not park.

---

## 6. The residual ~29% — IPC decay, not extra work

`perf stat` at the two curve endpoints plus the S=6/N=1 outlier (`counters/*.perf-stat.csv`):

| point | instr/row | cycles/row | IPC | L1d-miss/row | dTLB-miss/row | br-miss/row |
|---|--:|--:|--:|--:|--:|--:|
| S=1 N=2 (1-core peak) | 38,343 | 25,200 | **1.52** | 265.4 | 7.6 | 56.6 |
| S=6 N=16 (6-core peak) | 38,382 | 33,793 | **1.14** | 285.4 | 10.6 | 59.2 |
| S=6 N=1 | 43,241 | 32,322 | 1.34 | 289.4 | 14.4 | 95.0 |

S=6/N=16 vs S=1/N=2: **instructions/row +0.1% (flat)** · **cycles/row +34.1%** · **IPC −25.4%**.
The engine is not doing more work as cores are added. The **same work executes slower**.

**Closure model.** At fixed clock and fixed SMT threads per core (both arms run 2 SMT threads per
physical core), rows/s per physical core ≈ utilisation × IPC ÷ instructions-per-row:

```
predicted = IPC 0.7465  ×  instr/row 0.9990  ×  util 0.9719  =  0.7247
measured  = 0.7111 (vs cn-s1 N=2)   /   0.7180 (vs the independent cn-s1-ac5 re-run)
closure gap = +1.37 pp
```

The 29% shortfall therefore splits as:

| component | contribution |
|---|--:|
| IPC decay | **25.4 pp** |
| residual idle (3.3% of the pinned set) | 2.1 pp |
| extra instructions | 0.1 pp |
| unexplained | 1.4 pp |

**What is not established.** `LLC-loads`, `LLC-load-misses` and `cache-references` all read
`<not supported>` on this virtualized host, so the microarchitectural cause of the IPC decay is
only *partially* measured — reported, not inferred. The counters that do work: L1d miss/row
+7.5% relative (2.93% → 3.14% of loads), dTLB miss/row +40.9% (7.56 → 10.65). Charging those at
generous penalties accounts for roughly **10–13%** of the +8,593 cycles/row; **~87% is
unattributed**. LLC / memory-bandwidth saturation across 6 cores is the natural hypothesis. It
was **not measured**, and this report does not claim it.

The S=6/N=1 row is a separate finding, not part of the closure: at one stream on six cores the
engine executes **+12.8% more instructions per row** than at the 1-core peak — the only point in
the matrix where instructions/row is *not* flat. It is also the point with 1,677 allocator parks
per batch (§4.4). Together they are a candidate mechanism for "N=1 gets slower as cores are
added" (−24% from S=1 to S=6), which is a real effect worth knowing independently of this issue.

---

## 7. Verdict

**The `do_get` mpsc handoff is ACQUITTED.** Zero parks at every park-count point; ≤0.074% of
blocked time at the worst point; `egress_credit_acquire` ≈ 0. Two independent instruments
(duration-weighted off-CPU stacks and event-counted `sched_switch` stacks) agree, and the
instruments' three known false-negative modes (§2.6 traps 1–3) were each identified and closed
before the figures were taken — which matters, because all three would have produced an *empty*
profile that reads exactly like this acquittal.

**The full-box-collapse hypothesis is FALSIFIED.** 0.873 / 0.811 / 0.711 marginal efficiency at
2 / 4 / 6 cores against the conservative denominator. No compounding, no box-level ceiling at
2–3 cores of effective work. There is no 2.5–4× box-level handoff lever here.

**The residual attributes to IPC decay**, 25.4 of the 29 pp, with instructions/row flat to +0.1%.
The engine's *work* scales; the *memory system* does not keep up.

**What is NOT established, plainly:** the microarchitectural cause of that IPC decay. ~87% of the
+8,593 cycles/row is unattributed because the LLC and uncore counters do not exist on this host.
**The instrument that would settle it:** a bare-metal or LLC-counter-exposing host where
`LLC-load-misses`, `offcore_response`/`OFFCORE_RESPONSE` and memory-bandwidth counters
(`uncore_imc/*`, or `perf stat -M MemoryBandwidth` / Intel PCM / `toplev` level-2) actually
program. Re-run the two endpoint points (S=1/N=2 and S=6/N=16) with a top-down breakdown and the
question closes in one afternoon. Until then, "LLC/memory bandwidth" is a hypothesis with a
plausible shape and no measurement behind it.

**A secondary, weaker finding worth carrying:** `glibc` allocator arena contention is 64% of parks
by count at S=6/N=1 and coincides with that point's +12.8% instructions/row. It is 2.0% of blocked
time by duration, so it is not costing throughput at saturation — but it is the best available
mechanism for the "more cores makes N=1 slower" effect.

---

## 8. Ordering recommendation — the #1 next step is a MEASUREMENT, not either fix

> ### THE SINGLE MOST DECISION-RELEVANT SENTENCE IN THIS RUN
>
> **The #1 next step is not a fix. It is measuring the microarchitectural cause of the IPC decay on
> a host with working LLC / memory-bandwidth counters — because BOTH candidate fixes are currently
> unfounded with respect to the scaling discount.** The handoff is measured at ~0 cost, so a
> handoff fix cannot address the discount. #3096 (Arrow encode) reduces per-row work, but the
> discount is 25.4 of 29 pp of *IPC decay at flat instructions/row*, and this run **cannot tell**
> whether cutting per-row work flattens the curve or only raises it. ~87% of the +8,593 cycles/row
> is unattributed because the counters that would attribute it are `<not supported>` on this host.
> **That one measurement — two endpoint points, one afternoon, gated only on host selection —
> decides whether ANY per-row-work lever moves the slope. Fund it before either fix.**

Only after that measurement does the fix ordering below become a real ranking rather than a
provisional one.

**The call between the two fixes: neither is shown to attack the measured scaling discount, and of
the two, a `do_get` handoff fix is ordered LAST.**

The reasoning, kept honest about what each claim rests on:

1. **A handoff fix is ordered last because the site is measured at ~0 cost.** Not "small" —
   0 parks, 0.074% of blocked time, ≈0 egress-credit blocking. Optimising it cannot recover
   throughput that is not being lost there. This is the strongest, most direct conclusion in the
   report.
2. **#3096 (Arrow encode) reduces per-row work, so it should raise ABSOLUTE throughput at every
   core count.** `rows_to_record_batch` is 3.8–9.1% of on-CPU server weight across the matrix
   (6.6–9.1% at the saturated N=8/N=16 points) and `estimate_arrow_row_bytes` another 1.6–2.4%
   where it reaches the top-12; the `alloc` crate is 10.3–11.4%. That is real, removable work, and
   removing it lowers instructions/row.
3. **But #3096 is NOT obviously a fix for the SCALING DISCOUNT specifically.** The discount is
   an IPC effect at flat instructions/row: 25.4 of 29 pp. Cutting instructions/row raises the
   *level* of the whole curve; whether it also *flattens* the curve depends on whether the removed
   work is what is saturating the memory system. If the encode path's allocations and copies are a
   principal source of the memory traffic, #3096 would improve both level and slope. If the
   pressure comes mostly from the decode/materialize path, #3096 raises the level and leaves the
   slope alone. **This report cannot distinguish those two cases** — that is precisely the LLC
   measurement it could not make (§7). Anyone claiming #3096 fixes the scaling discount is
   claiming something this data does not support.
4. **Therefore the highest-value next step is not a fix at all** — it is the LLC / memory-bandwidth
   measurement in the box at the top of this section. Restated because it is the point most likely
   to be skimmed past: **both fixes are unfounded with respect to the scaling discount until that
   measurement exists.** One is unfounded because its site costs nothing; the other is unfounded
   because nobody yet knows whether per-row work is what the memory system is choking on.

The measured discount (0.711–0.873) lands **inside** #2817's assumed **0.6–0.75** band at the
6-core point and above it at 2 and 4 cores. The projections built on that assumption therefore
**do not need revision** — which is itself the answer to #2817's open question.

---

## 9. Artefact inventory (AC8)

**Committed** under `docs/reports/ws0-3217-artifacts/`:

| path | contents |
|---|---|
| `corpus/` | corpus provenance, geometry, sha (initial + staged), `sstablemetadata`/`tablestats`/`tablehistograms`, fullscan oracle, owner report requirements |
| `harness/` | 13 files — `sweep.sh`, `common.sh`, `profile-oncpu.sh`, `profile-offcpu.sh`, `classify-offcpu.py` (v1), `corpus-basis.py`, `emit-point.py`, `summarize-sweep.py`, `unsym-check.py`, `parse-runqlat.py`, `offcpu-fallback.bt`, `selftest.sh` (36 checks), `README.md` |
| `partA-run/` | `run-partA.sh`, `run-partA-followon.sh`, `analyze-partA.py`, `ctxt-sampler{,2}.sh` |
| `partB-run/` | `run-partB{,2,3,4}.sh`, `classify-offcpu-v2.py`, `demangle-folded.py`, `demangle_helper.py`, `park-count.bt`, `park-count-run.sh`, `parse-park-count.py`, `sched-switch-run.sh`, `summarize-oncpu.py`, `llc-run.sh`, `offcputime-bigmap` |
| `results/` | `partA-analysis.{txt,json}`, per-arm `cn-s{1,2,4,6}/`, `cn-s{1,6}-merge-n1/`, `cn-s1-ac5/`, `calib-s6-n{1,16}/` (each: `points.jsonl`, `summary.{json,txt}`, `corpus-basis.json`, `cpu-topology.json`, `run-config.json`), `raw-capture-inventory.txt` |
| `partB-results/oncpu/` | 6 × (`.svg`, `.folded.gz`, `.unsym-check.json`, `.run-config.json`) + `AC3-oncpu-summary.{json,txt}` |
| `partB-results/offcpu/` | 6 × (`.svg`, `.folded.gz`, `.attribution-v2.{json,txt}`) + 2 × `.run-config.json` |
| `partB-results/park-counts/` | 3 × `sched2-*.{folded.gz,park-sites.json,park-sites.txt}` + 3 × superseded bpftrace captures (`.txt.gz`, `.run-config.json`) |
| `partB-results/scheduler/` | 6 × `runqlat.{txt,json}` + 2 × `scheduler-cost.{jsonl,txt}` |
| `partB-results/counters/` | 3 × `perf-stat.csv` + `microarch-counters.json` |
| `partB-results/` | `partB-analysis.{txt,json}`, `cpu-topology.json`, `raw-capture-inventory.txt` |
| `partC/` | the Part C deliverables as **drafts, not posted/filed** — `POST-TO-2817.md` (marginal-efficiency table + the lever call), `POST-TO-3100.md` (AC2 shape verdict + the off-CPU answer to #3100's declined section), `PROPOSED-FOLLOWUPS.md` (5 proposed issues + the candidates dropped for lack of evidence). Posting and filing are the owner's. |

**Retained on the measurement box with paths recorded** (too bulky to commit; full listing in
`results/raw-capture-inventory.txt` and `partB-results/raw-capture-inventory.txt`, box
`ip-172-31-5-109`):

- `/data/ws0/profiles/oncpu-*/perf.data` (9–61 MB each) and `perf.script` (127–889 MB each)
- `/data/ws0/profiles/sched2-*/sched.data` (98–552 MB)
- `/data/ws0/profiles/offcpu2-s{1,6}/` raw folded (42 MB / 253 MB)
- `/data/ws0/logs/<label>/` per-point perf CSV, loadgen step JSONL, `/proc` deltas, server logs
- `/data/ws0/logs/ctxt/threads-pertid.jsonl` (4.6 MB, the AC5 sidecar)

**Explicitly labelled SUPERSEDED — retained for provenance, MUST NOT be quoted:**

- `/data/ws0/profiles/offcpu-s{1,6}/` — round-1 off-CPU, bcc's stock 10,240-key counts map
  saturated and silently dropped stacks. N≥8 data invalid.
- `/data/ws0/profiles/park-s{1,6}-N*/park-count.txt` + the committed
  `park-counts/bpftrace-park-*.txt.gz` — bpftrace `ustack` left ~50% of user frames as raw hex.
- `/data/ws0/logs/ctxt/threads.jsonl` — AC5 sidecar v1, non-monotone under thread churn (its
  deltas go negative).
- The v1 classifier columns inside `scheduler/offcpu2-s*.scheduler-cost.txt` (its
  `other` = 76–83% and its `vol cs/s (proc)` = 0.0 rows). Superseded by
  `offcpu/*.attribution-v2.*` and by Part A's per-TID sidecar respectively. The file is retained
  because its runqlat and migration columns are still the record.

Every figure in this report is backed by a committed artefact. No figure in this report is drawn
from a superseded capture.

---

## 10. What a future run should do differently

1. **Book a host with working LLC / uncore counters before planning a microarchitectural
   question.** `cache-references`, `LLC-loads` and `LLC-load-misses` are `<not supported>` on this
   virtualized instance. That was discovered *after* the curve was measured, and it is the single
   reason this report leaves an open question rather than a closed one. Probe with
   `perf stat -e LLC-load-misses,cache-references true` on the target instance type **first**;
   if it prints `<not supported>`, the memory-side question cannot be asked there at any budget.
2. **Size the bcc `counts` map before the first real capture, not after.** `offcputime`'s
   `BPF_HASH` defaults to 10,240 keys and truncates **silently**; `--stack-storage-size` sizes a
   different map and does not help. An entire round of N≥8 off-CPU data was invalidated (10,240
   captured against 108,475 real unique stacks). This is a **fleet-wide measurement hazard**, not
   a one-off — any future off-CPU work should start from the patched collector
   (`partB-run/offcputime-bigmap`) and should assert `stack traces lost` is small and reported.
3. **Run count-based and duration-based park instruments together, always.** The glibc allocator
   was 64% of parks by count and 2.0% by duration. Either instrument alone gives a confident,
   wrong picture — in opposite directions.
4. **Prefer `perf script` over bpftrace `ustack` for symbolized user stacks on this stack.**
   bpftrace left ~50% raw hex where perf resolved everything. Round-1 park counts had to be
   re-run.
5. **Provision the client from measurement, not from caution.** #3100 spent 4 physical cores on
   the client; 2 cores drove the entire curve at ≤13.8% busy — ~5× headroom at the hardest point.
   Two of those cores were pure loss. Measure the client's actual budget at the busiest point once,
   then allocate. (The gate must stay: an unmeasured client is not a cheaper client, it is an
   invalid measurement.)
6. **Pin the parser's own dependencies, and make a missing one FAIL rather than degrade.**
   `demangle_helper.py` returns an undecodable symbol unchanged, so running the attribution without
   `rust_demangler` silently produces a wrong table instead of an error (s6-N1 `mpsc_send_park`
   50.57 s vs 2.89 s). Any analysis tool whose degraded mode is "plausible output" needs a startup
   assertion, not a fallback.
7. **Write the classifier against real symbols, then keep the pre-symbol version as evidence.**
   v1 was written before any real stack existed and put 76–83% into `other`; v2 demangles first
   and matches leaf-first. The revision is expected and correct — but it must be *recorded*, since
   "we changed how we bucket after seeing the data" is exactly the shape of a result that got
   fitted rather than measured. Both classifiers are committed.
8. **Budget three reps everywhere from the start.** The two dispersion outliers (12.3%, 10.7%)
   both sit at low N on wide core sets. At reps=1 they would have shipped invisibly, and one of
   them sits next to a headline.

---

## Corrections — mid-run claims that were walked back

Recorded here because **this report is the durable artefact**: a reader should not have to
reconcile it against a comment thread to find out which mid-run statements did not survive. Walking
a claim back is part of the method, not an embarrassment — the alternative is a published number
nobody can trace.

| # | Mid-run claim (posted to the issue thread while Parts A/B were running) | Correction | Where the corrected figure lives |
|--:|---|---|---|
| C1 | "**90 points**" — quoted for warmth (`read_bytes` = 0) and admission (`requests_unavailable` = 0) | **83 points.** 5 curve arms × 5 N × 3 reps (75) + 2 merge reference arms × 3 reps (6) + 2 reps=1 calibration probes (2) = **83**. The verified properties are unchanged — `read_bytes` = 0 and `requests_unavailable` = 0 hold on **all 83**; only the count was wrong. | `results/partA-analysis.json` → `warmth.points` = 83, with a per-sweep `points_by_sweep` breakdown so the total is auditable rather than asserted; §2.7 |
| C2 | "**Geometry predicts these**" — said of all three `cqlite-core` channels' parks-per-batch | **Overstated for two of the three.** It holds tightly for **`core_raw_chunk` only** (347 predicted sends vs 265–327 measured parks). `core_query_rows` records **3.1–3.4× more parks than sends** (64 vs 201–220) and `core_windowed_batch` exceeds its send count at the busiest point (32 vs 43). The both-endpoints-park / std-`sync_channel` explanation is a **hypothesis and unmeasured**. The site attribution itself is unaffected: every park is attributed to a named site, `other` = 0, and the handoff records zero. | §5, including its opening callout |
| C3 | The `self_normalisation_warning` string in `partA-analysis.json` labelled the own-N=1 series "S=1 216229, S=2 212578, S=4 205129, S=6 175872" | **Off by one label.** It zipped the first four entries of a per-arm list that holds **two S=1 arms** (`cn-s1` and the independent `cn-s1-ac5` re-run) onto the labels 1/2/4/6. Correct series: **S=1 216229, S=2 205129, S=4 175872, S=6 163510 rows/s.** The `.txt` table and the `per_arm` JSON were always correct — only the prose warning was wrong. Fixed **in the generator** (`partA-run/analyze-partA.py`, now derived from each arm's own `S_physical_cores` and spelling out the arm label) and the artefact regenerated, so the next run cannot reproduce it. | `results/partA-analysis.json` → `cross_S_scaling.self_normalisation_warning`; §3.2 |

| C4 | "**17.2–17.8%** of frame instances resolve only to `[libc.so.6]`" — the AC3 opacity caveat | **Unsourced, and both ends were wrong.** No committed artefact produced that band. Recomputed from a stated definition (weighted frame instances of bare-`[<dso>]` frames, excluding `[unknown]` and pseudo-DSOs): **16.86–17.94% on server threads**, 14.25–17.94% on the all-frames basis. The figure is now **emitted by `partB-run/summarize-oncpu.py` into `partB-results/oncpu/AC3-oncpu-summary.json`** (+ a `DSO-only srv%` column in the `.txt`), computed from the **committed** `oncpu/*.folded.gz` so it is reproducible from this repo alone. Under this report's own AC8 standard an unsourced figure in a caveat is exactly what may not ship. | §4.1 (definition + per-profile table); AC3 row |
| C5 | "`other` is **fully broken out by named cause, never left as an unnamed residue**" and "`tokio_scheduler` is **100%** idle-runtime park" | **Self-contradictory as written** — the generator printed those sentences directly above an `unnamed` line (2.13 s = 0.11% of blocked time at s6-N16; 0.06 s in the tokio breakdown). Corrected in **both** the report and the generator: the bucket is renamed **`unclassified_residual`** and the claim is now the true one — a residue exists, is bounded at **≤0.11% of total blocked time**, and is shown. The six `attribution-v2.{json,txt}` artefacts were **re-emitted** from the committed `offcpu/*.folded.gz` and verified **bit-identical to the originals modulo the rename** (only `folded_file` changes, now naming the committed input). | §4.2; `partB-run/classify-offcpu-v2.py` |

A further item is a qualification rather than a correction: **AC3's PASS is not "fully symbolized"**
— **16.86–17.94%** of *server-thread* frame instances are DSO-only `[libc.so.6]` and escape the
gate's metric. An earlier in-flight note quoted a narrower **17.2–17.8%** band that no committed
artefact backed; the figure is now emitted from a stated definition into
`partB-results/oncpu/AC3-oncpu-summary.json` by `partB-run/summarize-oncpu.py`, computed from the
**committed** `oncpu/*.folded.gz`, and the band is whatever that artefact says. Carried on the AC
table row itself so it cannot be read in isolation (§4.1).

---

## Acceptance criteria

| AC | Status | Evidence |
|--:|:--|:--|
| 1 — full-box C(N) at N=1..16, warm, median of ≥3, dispersion, aggregate + per-stream + marginal efficiency, admission clean | ✅ | §3.1–3.2; `results/partA-analysis.*`; `requests_unavailable`=0 on all 83 points |
| 2 — pinned-core control reproduces #3100's shape (or divergence explained) | ✅ reproduced within ≤1.8 pp | §3.3 |
| 3 — on-CPU flame graphs, pinned + full-box, unsymbolized <10% | ✅ 6/6 PASS (0.009–0.028%) — **QUALIFIED: this is NOT "fully symbolized."** **16.86–17.94%** of *server-thread* frame instances resolve only to the DSO `[libc.so.6]` (the un-symbolized system allocator) and are **not counted by the gate**, because they carry a DSO name (all-frames basis 14.25–17.94%). The gate's metric is satisfied; ~17% of server frames remain opaque at function granularity. Figure emitted from a stated definition into `AC3-oncpu-summary.json`, recomputable from the committed `oncpu/*.folded.gz`. Do not read this row in isolation. | §4.1 (caveat); remedy proposed as F4 in `partC/PROPOSED-FOLLOWUPS.md`; `partB-results/oncpu/` |
| 4 — off-CPU flame graphs + ranked attribution, every class quantified or explicitly absent | ✅ all 7 buckets + 5-way channel split, explicit zeros | §4.2; `partB-results/offcpu/` |
| 5 — context switches + run-queue latency per N | ✅ | §4.5; `partB-results/scheduler/` |
| 6 — byte basis named on every throughput figure; geometry + sha recorded; `now`-pinning N/A | ✅ | §2.3; three bases carried throughout |
| 7 — marginal-efficiency table for #2817 + verdict and ordering recommendation | ✅ table + explicit lever call | §3.2, §7, §8 |
| 8 — report + artefacts committed; non-retained figures labelled | ✅ | §9 (committed vs path-recorded vs superseded) |

## Notes and deviations

- The verdict is a **negative result**, and it is the deliverable. The collapse scenario would have
  made a handoff fix a 2.5–4× box lever; the measurement says 1.2–1.4× at most, and the handoff
  itself is measured at ~0 cost. No figure here has been rounded toward the hypothesis.
- **#3100's still-open follow-up is NOT answered here** and remains open: publishable absolutes vs
  Cassandra needs AWS provision plus a Cassandra arm, both explicitly out of scope for #3217.
- Two independent Part A S=1 arms (`cn-s1`, `cn-s1-ac5`) are reported separately rather than
  pooled; they agree within 1.7% at every N.
- The `calib-s6-n{1,16}` rows are reps=1 probes. Their `spr% 0.0` is **absent** dispersion, not a
  measured zero; they are excluded from every curve.
- The AC5 per-TID sidecar can slightly **under**-count under tokio thread churn, never over-count.
  The bias direction is stated so the numbers can be read correctly.
- 83 measured points are recorded across all arms (5 sweep arms × 5 N × 3 reps, 2 merge reference
  arms × 3 reps, 2 reps=1 calibration probes) — see **Corrections C1**, and `points_by_sweep` in
  `results/partA-analysis.json` for the auditable breakdown. An earlier in-flight note quoted 90; 83 is the
  count in the committed artefacts and is the figure of record.
