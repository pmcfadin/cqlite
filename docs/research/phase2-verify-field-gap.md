# Phase 2 — Adversarial reconciliation of the field-gap decomposition

**Date:** 2026-07-21 · **Status:** research (uncommitted) · **Author:** Phase-2 adversarial reconciler
**Adjudicates:** `phase1-3-linux-io.md` (IO/decompress) vs `phase1-5-transport-ingest.md` (transport/ingest),
using `phase0-scan-cost-breakdown-2026-07.md` (server CPU), `phase1-6-parallelism.md` (concurrency
math), `phase1-7-trino-question.md` (the server-direct pivot), and primary code evidence.

This document exists to settle **one contradiction** and hand the program a single canonical gap
table plus the one field measurement that must come first in 0.17.

---

## 0. The contradiction, stated precisely

- **P1.5 §1 (transport):** attributes **30–45 % of the ~47× local-vs-field gap** to a single
  **"cold-IO + LZ4 decompression"** bucket, graded **LOW**, and separately **30–50 %** to
  "distribution" and **15–30 %** to "transport + connector ingest".
- **P1.3 (IO):** by arithmetic, **LZ4 decode ≈ 0.1–1 % of CPU** and **cold NVMe bandwidth has ~8×
  headroom** (needs 10–120 MB/s against ≥ 1 GB/s), so **"cold IO never binds"** in the 100 k–600 k
  rows/s envelope.

Both cannot hold at that magnitude. A 30–45 % bucket built from two sub-terms that a sound
arithmetic says are ~1 % (decompress-CPU) and non-binding (cold bandwidth) is over-sized **as
labeled**. The reconciliation below shows the honest resolution is **not** "one doc is simply right"
— it is that **P1.5 mislabeled and over-sized the bucket, P1.3 is arithmetically right but let a real
sub-term fall off its headline, and BOTH docs over-weighted the Trino/connector axis** because
neither fully used the one field number that reframes everything (P1.7 §0).

---

## 1. The pivot both P1.3 and P1.5 under-used (primary evidence)

**P1.7 §0/§1 [FIELD]:** field **server-direct** full-ring scan (R11b, #2367) ≈ **29 k rows/s/ring**;
field **through-Trino** full-ring scan (B3, R12) = 1.94 M rows / 61.1 s / 3 pods ≈ **31.8 k rows/s
aggregate** (~10.6 k/pod). **These two aggregates are equal within noise (29 k ≈ 32 k).**

> **Consequence that neither P1.3 nor P1.5 propagated:** *Trino, the connector page-build, the
> HTTP/2 window, and split distribution add ≈ 0 to the aggregate.* If they cost a large multiple,
> through-Trino would be far **below** server-direct; it is not — it is marginally **above**. So the
> ~47× gap is almost entirely a **server-side, per-node scan-feed** deficit measured **with Trino
> removed**. P1.5 built its whole decomposition on `500 k (1 local stream) ÷ 10.6 k (field pod)` and
> then attributed 45–80 % of it (across the distribution + transport + connector buckets) to the
> layer P1.7 proves is nearly free on aggregate.

**Corollary — the per-stream "189×" is not a derivable quantity.** P1.5 §1b divides 10.6 k/pod by
"~4 streams/pod" (from **admission 12/64**) to get ~2.6 k/stream, then 500 k ÷ 2.6 k = 189×. But
**admission 12/64 is the R12 *B2 saturation-probe* snapshot, not the B3 scan** — B3's actual per-pod
concurrency during the 61 s full scan is unrecorded. The 189× therefore rests on a concurrency number
borrowed from a different workload. **We do not know the field per-stream rate.** This is itself a
top-tier measurement gap (§5), and it means the honest decomposition must be anchored at the
**aggregate/per-node** level (~500 k single warm M1 stream → ~10 k/node = **~47–50×/node**), where
P1.7 gives solid ground, not at the per-stream level where the divisor is a guess.

---

## 2. Ground truth the field actually measures (why neither doc could settle this from telemetry)

The task's key pointer: read the in-code phase metric. The closed phase set
(`cqlite-flight/src/obs.rs:210–227`, `RPC_PHASES`) is exactly **five**:

`validate` → `admission` → `resolve` → `merge_setup` → `stream`

- `admission` = time parked on the `--max-concurrent-scans` semaphore (this is where **12/64** comes
  from).
- `resolve` = snapshot + reader open, Summary/Index load — **the only isolatable cold-open signal.**
- **`stream` = the ENTIRE data plane**: cold body-chunk faults, LZ4 decompress, decode, k-way merge,
  reconcile, per-row materialize, Arrow encode, **and** gRPC write, all folded into one histogram
  (`cqlite.rpc.phase.duration`, `PhaseTimer`, `obs.rs:270–290`; the scan loop runs entirely inside
  `PHASE_STREAM`, `streaming.rs:757`).

> **The field's standing instrumentation is structurally blind to the P1.3↔P1.5 split.** Cold-IO
> latency, decompress-CPU, channel park/wake, and reconcile are all inside `stream` and cannot be
> separated by any dashboard query. The ONLY cold signal the field can currently see is an elevated
> **`resolve`** phase (open/index faults) — not the in-`stream` body-scan faults where the disputed
> cost would actually live. **Neither P1.3 nor P1.5 could have settled the contradiction from field
> data because the field data cannot express it.** This is why the first 0.17 measurement must be an
> *in-`stream` profile*, not another dashboard read (§5).

**Two more primary confirmations that ground the mechanism:**

- **Field path = the profiled path.** The field `do_get` drives
  `produce_streaming_from_readers` (`streaming.rs:349`, `producer.rs`) — the **same** warm-reader
  thread-per-input k-way merge Phase 0 profiled. So Phase 0's CPU shape (55 % kernel `sync_channel`
  park/wake, 18 % alloc, 22 % own compute) is the field server's shape too, *plus* the field-only
  terms.
- **`Auto` → mmap, no `madvise`.** `resolve_disk_access_mode` (`reader/mod.rs:245–268`) returns
  **Buffered** below `mmap_min_size`, **Direct** only above `memory_fraction × RAM` (16 GiB on a
  32 GiB box) — so every field Data.db (≪ 16 GiB) resolves to **Mmap**. `mmap_advice_for`
  (`reader/mod.rs:316`) returns **`None`** for the scan (deliberately, to avoid the #1143
  `MADV_SEQUENTIAL` drop-behind tail). **Result: cold field scans fault synchronously on the reader
  thread with only the kernel-default ~128 KiB readahead and no hint.** This is the exact mechanism
  the cold-IO-latency bucket rides on — real, but latency-shaped, not bandwidth- or CPU-shaped.

---

## 3. Stress-testing each doc's magnitude claim

### 3a. P1.3's "IO never binds" — does cold *latency* (not bandwidth) survive?

P1.3's bandwidth arithmetic is **sound and survives** (100 k–600 k rows/s needs 10–120 MB/s vs
≥ 1 GB/s NVMe; LZ4 decode ~0.2–1 % CPU). But its headline collapses two different questions. On the
**synchronous-mmap-fault** path (§2), the binding question is not "is there enough bandwidth?" but
"does the faulting reader thread stall?" Mechanism check:

- **Sequential case (readahead effective):** Cassandra Data.db is stored in **partition-token order**;
  a #2412/#2413 Summary-guided, token-bounded split walks a **contiguous** token range → **sequential
  on disk**. A 64 KiB uncompressed chunk (~26 KiB compressed) covers ~800 narrow rows; a 128 KiB
  readahead window covers ~2 chunks. A cold fault every ~1,600 rows at ~100 µs NVMe latency ≈
  **0.06 µs/row** — negligible against the field's ~34 µs/row aggregate. **P1.3 wins here.**
- **Defeated-readahead case:** the warm path spawns **one reader thread per input SSTable**
  (`from_readers`). A real node carries **many** SSTables (not the local rig's 4) from ongoing
  flush/compaction; N threads faulting N files **interleave the device LBA stream**, degrading each
  toward random and stalling the 128 KiB window (P1.3 §2b flags this, then drops it). This is the
  ONLY regime where cold-IO latency climbs toward a double-digit share — and it is **testable** and
  **currently unmeasured**.

**Verdict on P1.3:** right on bandwidth and decompress-CPU (grade HIGH); **overreaches** only in
letting the cold-**latency**/readahead-starvation term — which it correctly identifies in §2b —
fall out of its "IO never binds" headline. On a thread blocked in a synchronous page fault, latency
*is* throughput for that stream.

### 3b. P1.5's buckets — decomposing the mislabel

- **"Cold-IO + decompression 30–45 % (LOW)":** the **decompression** half is ~1 % (P1.3, HIGH), and
  the **bandwidth** half is non-binding (P1.3, HIGH). What remains is the cold-IO **latency** term of
  §3a — real, but (i) latency-shaped, (ii) amortized over a 61 s sustained scan so cold-start is a
  small fraction, and (iii) only large in the defeated-readahead regime. **Reconciled magnitude:
  ~10–25 %, not 30–45 %**, grade LOW. P1.5's *instinct* ("likely the single biggest term, and
  invisible on the warm rig") was directionally right; its *label* (bundling ~0 % decompress) and its
  *ceiling* (45 %) were wrong.
- **"Distribution 30–50 % (MED)":** capped hard by P1.7 — through-Trino aggregate ≈ server-direct
  aggregate, so the connector/Trino distribution layer is **not** leaving a large multiple on the
  table. And P1.6 §4 shows the server's own consumer **drain-saturates at `N_drain_sat` ≈ 4–8
  streams**, at/below where admission already sits — so "run more streams" cannot be a big share of a
  server-direct gap. **Reconciled: ~5–15 %.**
- **"Transport + connector 15–30 % (MED)":** same P1.7 cap. Real for **point-read latency** (the
  0.2–2 s Trino floor, P1.7 §0) but small for **scan throughput** on aggregate. **Reconciled:
  ~7–18 % combined.**

### 3c. Reconciling P1.5's "~4 do_gets" with P1.6

P1.5 reads admission 12/64 → ~4 streams/pod as **under-fan-out** (idle parallelism). P1.6 §4 reads
the **same regime** as **drain-saturation**: throughput is flat from 8 to 80 concurrent streams
(195 → 190 qps); past ~8 streams you buy only latency and buffer depth. **These are the same fact seen
from two sides:** the pod runs ~4–8 useful streams **because that is where the single-threaded
coordinator + Arrow-encode + gRPC-write consumer saturates**, not because Trino forgot to schedule
more. So the "distribution deficit" is mostly a **per-stream drain ceiling**, i.e. a **server**
property (Phase 0's 55 % kernel tax + single-threaded merge), **not** a scheduling shortfall. This is
the second reason to move weight OUT of P1.5's distribution/transport axis and INTO the server-side
per-stream/per-core buckets.

---

## 4. The reconciled gap table (canonical)

Shares are of the **~47–50× per-node** multiplicative gap (`~500 k single warm M1 stream → ~10 k
rows/s/node`), stated as confidence-weighted shares of the log-magnitude summing to 100 %. Each row
names the **falsifying measurement** that would settle it.

| # | Bucket | Share | Grade | Mechanism / primary evidence | Falsifying measurement |
|---|--------|------:|:-----:|------------------------------|------------------------|
| 1 | **Hardware + core-contention** — i4i.xlarge vCPU (4 cores, all shared) vs M1 Pro P-core (1 of 10 busy, 9 idle absorbing park/wake); Phase 0's 55 % kernel `sync_channel` tax *contends* when streams stack on 4 cores (`C(N) < 1`, P1.6 §0) | **~25–35 %** | MED | Phase 0 §3a (kernel tax); P1.6 §4 saturation table (flat 8→80 streams); field path = profiled path (§2) | Warm server-direct scan **on the i4i pod** at concurrency 1 and at pod-native concurrency; compare to the 500 k M1 warm anchor → isolates hardware + `C(N)` |
| 2 | **Field data-plane heaviness** — many SSTables (not 4) → more reader threads → worse Stage-4b channel tax; genuine tombstone/TTL/LWW **reconciliation overlap** → heavier Stage-4a | **~18–28 %** | MED | Phase 0 §5.5 caveat (owned by neither P1.3 nor P1.5); `from_readers` one thread/SSTable | Record **SSTable-count/node** + reconcile-overlap ratio; re-profile Stage-4a/4b share in-`stream` on field data |
| 3 | **Cold-IO *latency*** — synchronous mmap faults on the reader thread, `Auto`→mmap, **no `madvise`**, 128 KiB readahead, multi-SSTable LBA interleave. **NOT bandwidth, NOT decompress-CPU.** (This is the *reclassified, halved* remnant of P1.5's 30–45 % bucket.) | **~10–22 %** | **LOW** | §2/§3a; `reader/mod.rs:245–268,316`; P1.3 §2b (names it, then drops it) | **Cold-vs-warm A/B of the same full-ring scan**; the cold−warm delta *is* this bucket. On-CPU vs off-CPU-in-fault split isolates fault-wait from `send` park/wake |
| 4 | **Distribution / concurrency shortfall** — fewer effective streams than cores, per-stream serial pull, under-fan-out | **~6–14 %** | MED | P1.5 §1a; **capped by P1.7** (through-Trino ≈ server-direct) and P1.6 §4 (`N_drain_sat`≈4–8) | B3 per-pod concurrency actually observed during the 61 s scan (currently unrecorded — §5) |
| 5 | **Transport — network + HTTP/2 flow-control window** (64 KiB default, unset in code) | **~5–10 %** | MED→LOW | P1.5 §5; **capped by P1.7** (aggregate parity) | Server-direct **over the real app-node network** vs loopback, same scan |
| 6 | **Connector page-build** — `ArrowToTrino` row-at-a-time, 3.9 M `byte[]`/scan | **~3–8 %** | MED | P1.5 §2; **capped by P1.7** | through-Trino **minus** server-direct at equal concurrency (P1.7 says this delta is ~0) |
| 7 | **LZ4 decompression CPU** | **~1–2 %** | **HIGH** | P1.3 §2c (~0.2–1 % @ 1.5 GB/s/core) | i4i cold-compressed decode bench (extends `decode_policy_bench.rs`) |

Buckets **1 + 2 + 3 ≈ 55–75 %** — the server-side per-node terms — and **4 + 5 + 6 ≈ 15–30 %** — the
Trino/connector/transport axis that P1.5 owned and over-weighted. **Decompress (7) is ~1 %.** The
reallocation vs P1.5 is the whole story: weight moves *out* of "cold-IO+decompression" (30–45 % → 3
reclassified to ~10–22 % + 7 at ~1 %) and *out* of distribution+transport+connector (45–80 % → 4+5+6
at ~15–30 %), and *into* hardware+contention (1) and field data-plane heaviness (2), which neither
transport-focused doc owned.

---

## 5. Who was wrong, and why

- **P1.3 was substantially RIGHT.** Its bandwidth and decompress-CPU arithmetic (grade HIGH) survives
  adversarial checking: cold NVMe bandwidth has ~8× headroom; LZ4 decode is ~1 % CPU. Its **only**
  overreach is the headline "IO never binds," which quietly drops the cold-**latency**/readahead-
  starvation term it itself raised in §2b — a term that, on a synchronous-mmap-fault reader thread
  with no `madvise`, is throughput-limiting for the blocked stream. Fix: keep P1.3's numbers, promote
  its own §2b latency caveat into a first-class (LOW-confidence) bucket (row 3).
- **P1.5 was WRONG in two specific, asymmetric ways.** (1) It **bundled** a ~1 % term (decompress-CPU)
  with a real-but-different, latency-shaped term (cold-IO fault serialization) into one oversized
  30–45 % LOW bucket — a category error P1.3's arithmetic exposes; the two halves have opposite
  magnitudes. (2) It **over-weighted its own owned axis** (distribution + transport + connector,
  45–80 % combined) because it never reconciled against **P1.7's pivot** — through-Trino aggregate ≈
  server-direct aggregate — which caps that entire axis, nor against **P1.6 §4** (drain-saturation),
  which reframes "under-fan-out" as a server drain ceiling. P1.5's *instinct* that a large, warm-rig-
  invisible data-plane term dominates was **right**; its label, magnitude, and layer were wrong.
- **Root cause of the contradiction:** neither doc could falsify itself from field telemetry, because
  the field's 5-phase metric **folds the entire data plane into `stream`** (§2). The disagreement was
  never resolvable by argument — it needs the one measurement below.

---

## 6. The single highest-value first 0.17 field measurement

**File it as a 0.17 issue (proposed):** *"Field cold-vs-warm server-direct full-ring scan profile on
i4i — first real measurement of Stage-1 (IO+decompress) and Stage-6 (transport), and an in-`stream`
decomposition."*

**What it is:** replay **Phase 0's exact `samply`/`perf` methodology on the field i4i.xlarge pod**,
server-direct (`flight-loadgen` → `cqlite-flight`, `--shape full`), **cold then warm**, capturing
**both on-CPU and off-CPU (blocked) time**, and **recording SSTable-count/node** and the resolved
`DiskAccessMode`.

**Why it is THE measurement — it settles every disputed row at once:**

1. **Cold − warm delta = bucket 3** (cold-IO latency) directly. Off-CPU-in-fault vs off-CPU-in-`send`
   separates IO fault-wait from channel park/wake — the exact P1.3↔P1.5 split.
2. **Warm i4i run vs the 500 k M1 warm anchor = bucket 1** (hardware + `C(N)` contention), the term
   both docs under-weighted.
3. **First-ever measurement of Stage 1 and Stage 6** — the two stages the local rig is *structurally*
   blind to (Phase 0 §5) and the two the whole contradiction lives in.
4. **Server-direct removes Trino/connector as confounders** — and if server-direct field ≈ 29 k while
   through-Trino ≈ 32 k both re-confirm, buckets 4–6 are confirmed small (validates P1.7's pivot on
   fresh data).
5. **It uses instrumentation the field lacks** — the standing 5-phase metric cannot resolve inside
   `stream`; only a profile can. So this is not a dashboard read, it is the missing instrument.

**Secondary (cheap, same trip):** record B3's **actual per-pod concurrency** during the 61 s scan (to
retire the borrowed "12/64 → ~4 streams" divisor, §1) and the SSTable-count/node (bucket 2 mechanism).

**Do NOT lead with:** an `io_uring`/`O_DIRECT` build, a DataFusion spike, or a connector rewrite —
every one of those is gated behind this measurement telling us which bucket is real. The arithmetic
says buckets 1+2+3 (server-side) dominate and 4–7 (Trino/transport/decompress) are small; **this A/B
is what converts that from a reconciled estimate into a measured fact.**

---

## 7. One-paragraph summary for the program

The ~47× local-vs-field gap is **almost entirely server-side and Trino-independent**: P1.7's pivot —
through-Trino aggregate (~32 k rows/s) ≈ server-direct ring (~29 k) — proves the connector, HTTP/2
window, and split distribution add ≈ 0 on aggregate, so the gap cannot be the transport axis P1.5
weighted at 45–80 %. Reconciling the headline contradiction: **P1.3 is right** (LZ4 ≈ 1 % CPU, cold
bandwidth has 8× headroom) and **P1.5's "cold-IO + decompression 30–45 %" is mislabeled and
oversized** — decompression is ~1 %, cold *bandwidth* is non-binding, and only a **latency-shaped**
cold-mmap-fault term (synchronous faults, `Auto`→mmap, no `madvise`, 128 KiB readahead) survives, at
a reclassified **~10–22 %, LOW confidence**. The gap reallocates to two buckets neither transport doc
owned: **hardware + core-contention (~25–35 %)** and **field data-plane heaviness — more SSTables +
real reconciliation overlap (~18–28 %)**. Crucially, the field's own 5-phase metric folds the entire
data plane into `stream`, so **no existing telemetry can adjudicate this** — the first 0.17 field
measurement must be a **cold-vs-warm server-direct `samply` profile on the i4i pod** (Phase 0's method,
field hardware), which isolates every disputed bucket in one run and makes the first-ever real
measurement of Stage-1 (IO+decompress) and Stage-6 (transport).

**File:** `docs/research/phase2-verify-field-gap.md` (uncommitted per instructions).
