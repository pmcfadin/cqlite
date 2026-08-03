# DRAFT — for #3100 (WS0 re-run + C(N) sweep). NOT POSTED. Owner posts.

---

## #3217 landed — your follow-up measurement, with the off-CPU section you declined

#3100 is the parent measurement this extends, so it should not learn its own follow-up landed only
via a merged PR. Report: `docs/reports/ws0-3217-report.md`; artefacts
`docs/reports/ws0-3217-artifacts/`. Measured on one box, same class as your `c7i.4xlarge`
(Intel Xeon Platinum 8488C, 16 logical / 8 physical, SMT on, 1 NUMA node, 30 GiB), kernel
6.17.0-1019-aws, CQLite `main` @ `693ae41` — the same post-#3058 line you measured.

---

### 1. AC2 — your pinned-core shape REPRODUCED

The S=1 arm used your **exact** core set (`2,10`), so this is a direct comparison, not an analogy.

| N | #3217 measured | normalised | #3100 published | normalised | measured/published |
|--:|--:|--:|--:|--:|--:|
| 1 | 216,229 | 1.000 | 246,940 | 1.000 | 0.876 |
| 2 | **252,420** | **1.167** | **287,441** | **1.164** | 0.878 |
| 4 | 240,361 | 1.112 | 273,438 | 1.107 | 0.879 |
| 8 | 220,865 | 1.021 | 248,621 | 1.007 | 0.888 |
| 16 | 211,010 | 0.976 | 236,734 | 0.959 | 0.891 |

**Peak-at-N=2-then-decline reproduced, within ≤1.8 pp at every N.** Your 1.16×-at-N=2 → 0.96×-at-16
shape is exactly what came back.

**Why the absolute offset is a level shift and not a divergence:** measured/published is a
*near-constant* **0.876–0.891** across five points spanning a 1.16× peak and a 0.96× trough. A
methodology difference — different admission behaviour, a different steady state, a client-bound
arm, a different fast path — would **bend** the curve, showing up as a ratio that varies with N.
A uniform multiplier can only be a level effect: a different box instance and a regenerated corpus
with a different sha. That uniformity is the evidence, and it is stronger evidence than a
closer-but-noisier absolute match would have been. An independent S=1 re-run in the same session
landed within 1.7% at every N, so the reference is itself reproducible.

---

### 2. What the full-box extension adds that a pinned-core curve structurally could not

Your curve answers "what does concurrency do to **one core**?" It cannot answer "does that cost
**compound across cores**?" — and that was the whole open risk, because a per-core handoff tax that
compounds would mean a 16-vCPU box delivers 2–3 cores of effective work and the handoff is a
2.5–4× box-level lever.

The S-sweep (S ∈ {1,2,4,6} × N ∈ {1,2,4,8,16}, client held constant at 2 physical cores) settles it:

| S (physical cores) | best aggregate rows/s | N@peak | server util | marginal efficiency vs 1-core peak |
|---:|---:|---:|---:|---:|
| 1 | 252,420 | 2 | 0.996 | 1.000 |
| 2 | 440,677 | 8 | 0.995 | 0.873 |
| 4 | 818,747 | 16 | 0.992 | 0.811 |
| 6 | **1,076,917** | 16 | 0.967 | **0.711** |

**It is a one-time tax, not a compounding one.** Six cores retain 71% marginal efficiency at 96.7%
utilisation. The peak-at-N=2 artifact your curve found is a *per-core* concurrency effect and does
**not** become a box-level ceiling — the peak-N simply moves outward as cores are added
(N=2 → 8 → 16 → 16). **No 2.5–4× box-level handoff lever exists.**

Two further things only a cross-S sweep can show, both worth carrying:

- **A single stream gets SLOWER as cores are added**: 216,229 (S=1) → 205,129 (S=2) → 175,872
  (S=4) → 163,510 (S=6) rows/s, **−24%**. Consequence for anyone reading scaling tables: per-arm
  *self*-normalised speedups are **not cross-comparable**, because each arm's denominator is worse
  than the last. Both denominators are published in the report so neither is silently chosen.
- **Your client was over-provisioned by roughly an order of magnitude.** You spent 4 physical cores
  on the client; 2 cores drove the entire curve — including 1.08M rows/s at S=6/N=16 — at **13.8%**
  busy, against a 70% validity gate. Zero points excluded. Two of those four cores were pure loss.

Byte basis at the headline point, since #3100 set this discipline: 1,076,917 rows/s =
**746.6 MB/s logical/uncompressed** (693.29 B/row) / **211.2 MB/s on-disk compressed**
(196.09 B/row). The 13.13 GB/s the loadgen prints is Arrow buffer **capacity**, not gRPC wire
bytes, and is labelled as such.

---

### 3. The off-CPU answer to the section your baseline explicitly DECLINED

Your baseline named this precisely (§"On-CPU profiling only — declined here, not overlooked"): the
1.98M voluntary context switches "are the entire phenomenon and are exactly what an off-CPU profile
would explain", and ~13.5% of the Flight arm's wall time (`server_cpu_utilization_of_pinned_set`
0.865) was off the metered CPUs and unaccounted for. That instrument has now been run.

**The `do_get` mpsc handoff is ACQUITTED.** Two independent instruments agree:

- `perf sched:sched_switch` park **counts**, fully symbolized: **0** voluntary parks on
  `do_get_mpsc_handoff` per 8,192-row batch at S=1/N=1, S=6/N=1 and S=6/N=16. Zero, not "few".
- Patched `offcputime` blocked **duration**: `do_get_batch` = **1.46 s of 1,963 s** at the worst
  point (0.074%). `egress_credit_acquire` ≈ 0 everywhere.

**Where the switches actually come from.** Part A reproduced your signal as a specific number: at
S=6/N=1, ~39,115 voluntary switches/s against ~20 batches/s ≈ **~1,960 voluntary parks per
8,192-row batch** (the dedicated perf capture on the same arm measures 2,617 — the Part A figure
comes from a 0.5 Hz `/proc` sidecar that under-counts by construction). A per-batch mpsc handoff
would produce **1–2**. The resolution: the bypass read path stacks **four** bounded channels
between SSTable and wire, and the parks belong to the three *below* the Flight layer, inside
`cqlite-core`:

| park site | s1-N1 | s6-N1 | s6-N16 |
|---|--:|--:|--:|
| **do_get_mpsc_handoff** | **0** | **0** | **0** |
| **egress_credit** | **0** | **0** | **0** |
| `core_raw_chunk` (1 per 16 KiB chunk) | 314 | 327 | 265 |
| `core_query_rows` (1 per 128 rows) | 203 | 220 | 201 |
| `core_windowed_batch` (1 per 256 rows) | 8 | 27 | 43 |
| glibc malloc arena lock | 163 | 1,677 | 243 |
| tokio runtime idle | 320 | 360 | 263 |
| gRPC egress | 2 | 6 | 1 |

Corpus geometry predicts the dominant term: 3,999,890 rows / 169,257 16-KiB chunks = **23.63
rows/chunk** → **347 raw-chunk sends per 8,192-row batch**, against 265–327 measured parks. The
`do_get` channel does 1 reserve + 1 emit per 8,192 rows and never parked once.

One honest scoping note, since a looser version of this claim went out mid-run: the geometry
prediction closes **tightly for `core_raw_chunk` only**. `core_query_rows` records **3.1–3.4× more
parks than it has sends** (64 predicted, 201–220 measured) and `core_windowed_batch` exceeds its
send count at the busiest point (32 vs 43). Parks are switch *events*, so both endpoints of a
channel can park and a std `sync_channel` may switch more than once per transfer — but that account
is a **hypothesis and was not measured**. What is settled regardless: every park in the capture is
attributed to a **named site**, `other` = 0, and the `do_get` handoff's site count is zero.

**Is your ~13.5% now accounted for? Mechanism named and quantified — exact figure not reconciled.**
At the comparable arm (S=1/N=1, where 22% of the pinned set is idle), the causal blocking is:
`core_raw_chunk` **27.08 s (49.2%)**, `core_query_rows` 15.53 s (28.2%), `core_windowed_batch`
9.46 s (17.2%), tonic/socket 1.43 s (0.9%), `do_get_batch` **0.0001 s** — plus glibc arena waits
and pure runtime idle (`tokio_scheduler` is 100% idle-runtime park, not overhead). So your
off-metered-CPU time is the **four-stage read-path pipeline's serialization, dominated by the
16 KiB raw-chunk channel** — **not** the `do_get` handoff, **not** gRPC egress, **not** disk
(`disk_io` = 0, corpus fully page-cached).

Being honest about the limit: #3217's arm is not your arm (0.78 utilisation here vs your 0.865),
so this **names and quantifies the mechanism** rather than reconciling the exact 13.5 percentage
points. Anyone wanting that exact reconciliation needs the off-CPU capture taken *at your point*.

**And a caveat on reading blocked time as cost.** At 96.7–99.6% server utilisation, blocked time on
one thread is largely *overlapped* by other threads and is **not** directly lost throughput. The
990 s of `mpsc_send` blocked time at S=6/N=16 is summed across ~32 threads over a 30 s window and
coexists with 96.7% utilisation and flat instructions/row. The attribution table is
**compositional** — it says *which* sites the system waits on, not how many rows/s the waiting
costs. The report gives this its own subsection.

**What the residual actually is.** Between the 1-core peak and the 6-core peak: instructions/row
**+0.1% (flat)**, cycles/row **+34.1%**, IPC **−25.4%**. Closure: predicted 0.7237 vs measured
0.7111. The 28.9 pp shortfall = IPC decay 25.4 pp | residual idle 2.2 pp | extra instructions 0.1 pp |
unexplained 1.3 pp. **The microarchitectural cause is NOT established** — `LLC-loads`,
`LLC-load-misses` read `<not supported>` and `cache-references`/`cache-misses` return a constant 0
on this virtualized host, and L1d
(+7.5% rel) / dTLB (+40% rel) explain only ~10–13% of the +8,593 cycles/row. ~87% unattributed.
That is the one genuinely open question the run leaves.

Also worth recording, since it would have silently corrupted your conclusion had you run this
instrument: **bcc `offcputime`'s `counts` map truncates at 10,240 keys, silently**
(`--stack-storage-size` sizes a different map). One whole round of N≥8 data was invalid — 10,240
keys captured against 108,475 real unique stacks. And a permissive `perf_event_paranoid` does
**not** cover BPF map creation or tracefs, while `offcputime` charges only on switch-**in**. All
three failure modes produce an *empty* off-CPU profile that reads identically to "the handoff is
innocent" — i.e. identical to this report's actual conclusion. They are recorded in the report's
Method section as guards, not as incidents.

---

### 4. Your still-open follow-up: NOT answered here, still open

**Publishable absolutes vs Cassandra** remains #3100's open question. #3217 was explicitly scoped
away from it: no AWS provision, no Cassandra arm, no head-to-head. The Cassandra daemon on this box
was **stopped** before any measurement. Every figure above is CQLite-vs-CQLite scaling, which is
what the ordering question needed — it says nothing about where CQLite sits against Cassandra on a
publishable basis. That still needs AWS plus a Cassandra arm.

(Your other open item, `fio` not combined across boxes / no i4i↔c7i ratio — #3026 AC#2 — is also
untouched.)

---

### 5. Corpus provenance

**Regenerated, geometry-matched, NEW sha** — the same bar you set, for the same reason
(`cassandra-stress` is not byte-deterministic).

sha256(Data.db) = **`3a4ee5cd5ef5937ae52a703cca0ee0359df8ecb959915dea66b3b89f9a9c7c1e`**
(yours: `2c297a0c…`).

| metric | #3217 | #3100 | delta |
|---|--:|--:|--:|
| rows | 3,999,890 | 3,999,890 | **0** |
| `totalColumnsSet` | 35,999,010 | 35,999,010 | **0** |
| partitions (estimate) | 198,130 | 198,130 | 0 |
| uncompressed B/row | 693.29 | 692.70 | +0.085% |
| Data.db bytes | 784,334,710 | 784,116,369 | +0.028% |
| LZ4 ratio | 3.5356× | 3.5353× | +0.008% |
| chunk count (16 KiB) | 169,257 | 169,194 | +63 |
| droppable tombstones | 0.0 | 0.0 | same |
| SSTable count / format | 1 / `nb-16-big` | 1 / `nb-16-big` | same |

Cassandra 5.0.8 tarball, `MAX_HEAP_SIZE=8G`, flush + `nodetool compact` → exactly one SSTable. Row
count **double-oracled** (`sstablemetadata totalRows` and an independent 512-token-range fullscan,
exact agreement) — never a CQL `count(*)`, which server-side-times-out past 4M rows. Deviations
from your recipe, all recorded: **JDK 11.0.31 instead of 17** (only JDK on the box; daemon and
stress both clean), root `/data/ws0`, and `cassandra.yaml` **is** patched here so the SSTables
landed in the configured data dir rather than the CASSANDRA_HOME-relative default your run hit —
location only, no geometry effect. Full record:
`ws0-3217-artifacts/corpus/corpus-provenance.txt`.

**One repeatability note for the next run in this line:** #3100 committed its C(N) **logs but not
its driver**, so the sweep harness had to be reconstructed from the recorded invocation. #3217's
harness is committed (`ws0-3217-artifacts/harness/`, incl. a 36-check `selftest.sh`), so the next
run inherits the instrument and not merely the numbers.
