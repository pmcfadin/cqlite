# CQLite throughput program — Phase 3 synthesis (2026-07)

**Date:** 2026-07-21 · **Status:** filed as **epic #2817** (0.17 scan-path throughput program); manifest
items M0–M17 filed as GitHub issues (numbers inline in §7) · **Author:** Phase-3 synthesizer

This document folds the throughput research program into one actionable plan: the ground truth
(Phase 0), the eight lever memos (Phase 1-1…1-8), and the seven adversarial adjudications
(Phase 2-verify-*). **Precedence rule applied throughout: where a Phase-2 doc overrules a Phase-1
doc, the Phase-2 ruling is final.** Every rows/s figure carries its shape qualifier
(narrow/wide, warm/cold, RF). No optimism theater — the graveyard (§4) is a first-class deliverable.

Source files (cite by section): `docs/research/phase0-scan-cost-breakdown-2026-07.md` (P0),
`phase1-{1..8}-*.md` (P1.n), `phase2-verify-{row-engine,stage2,field-gap,transport,parallelism,caching,io}.md`
(P2:name).

---

## 1. Executive summary + verdict table

The through-Trino field baseline is **~10.6k rows/s/pod** (R12/#2367: 1.94M rows / 61.1s / 3 pods,
RF=3, LZ4, ~1.9M partitions/node, 4-vCPU pods). Server-direct field is **~29k rows/s/ring** (R11b).
The local single-stream ceiling is **~500k rows/s/core** (P0: M1, RF=1, narrow `keyvalue`, warm,
**uncompressed**, loopback) — a server-direct ceiling for that shape, **not** a field prediction.

The program's central finding: **the gap is almost entirely server-side and Trino-independent.**
P1.7's pivot — through-Trino aggregate (~32k) ≈ server-direct ring (~29k) — proves the connector,
HTTP/2 window, and split distribution add ≈0 on aggregate (P2:field-gap §1). The row *feed* is the
throughput ceiling; Trino is a *latency floor* (0.2–2s), not a throughput tax.

**Verdict table** (substance verbatim from P2:stage2 §0 for the A4/B3 rungs):

| Target | Verdict | Limiting factor (named) |
|---|---|---|
| **A4 Stage-1** — 100k rows/s/pod (server-direct) | **REACHABLE** | none binding; L1 batch-channel + fan-out-to-cores clears it comfortably (P2:stage2 §0) |
| **A4 Stage-2** — 600k rows/s/pod = 150k/core | **NOT reliably reachable** on the pure row engine (optimistic-corner only; central **~400–450k/pod**) | **C(N) < 4**: a 4-vCPU pod is **2 physical cores + hyperthreads, not 4× parallelism** + reconcile machinery (~2µs/row measured, P2:stage2 §3/§4) |
| **B3 Stage-1** — ≤10s (≈65k rows/s/pod) | **REACHABLE, at the edge** | needs full-core parallelism (~4×) **and** constants (~1.5×) to both land (P1.7 §4) |
| **B3 Stage-2** — ≤3s (216–323k rows/s/pod) | **REACHABLE-AT-COST on a row feed** (overrules P1.7's "not credible") | **NOT the row engine** — the **Trino coordinator floor** eating the 3s budget + **cold-start IO latency**; needs floor ≤~1.2s AND warm/prefetched IO AND L1/L3 landed (P2:stage2 §2) |
| **B2** — ≥100 qps @32thr warm | **REACHABLE** (reachable-at-cost, stack: #2680 pod-skew balance + #2600/#2765 egress relief + warm p50 ≤~0.3s) | pod-skew (count-balanced floorMod) — the #2680 lever (P1.7 §4) |
| **A2** — ≥1000 qps/pod warm | **REACHABLE-AT-COST, gated on measurement** | **unknown keyed access skew** — the decoded-partition cache's ~1.5–3× rests on an **unmeasured** hot-set hit rate (field keyed loadtest on record is ~0.9 qps, no skew captured); must measure the access distribution first (P2:caching §4) |
| **Worker ingest** — ≥250–350 MB/s/worker | **REACHABLE-AT-COST (wide rows only), via the Arrow path** | **row width**: at 180B/narrow rows this needs ~1.4M rows/s/worker = not credible on the row engine; credible only for **wide rows (≥1KB)** or via columnar. **Not via JDBC** (~32 MB/s/conn — moot, the path is Arrow) (P1.8 §3c, P1.5 §8) |
| **B4** — cold ≤3s / peak ≤512Mi / ≤16Mi per-query working set | **MET / holdable** | ≤16Mi is the **per-query working-set** target (NOT idle), and a single stream ≈15MB already ≈ the whole ≤16Mi (satisfiable at concurrency 1 or smaller batch_size); cold ≤3s met post-#2412; peak held with admission-resize + chunk-cache retune (P2:caching §1, P2:parallelism §6) |
| **A5** — 0 restarts @80thr | **MET today** | field 0 OOMKills, peak 270–391Mi @80thr; the memory-derived admission resize is a **forward guard** bundled with #2680, not a live-OOM fix (P2:parallelism §1) |

**Honest A4 Stage-2 finding (verbatim substance, P2:stage2):** central ~420k/pod, 600k is the
optimistic corner; C(N) = 2.5–3.5× (central 3×) on 2 physical cores, not the flat 4× both P1.2 and
P1.7 assumed. **Ingest caveat:** 250–350 MB/s is **not credible on narrow rows via the row engine**;
the target implicitly assumes a wider row shape than the `keyvalue` benchmark — state the assumed
width on record.

---

## 2. Ground truth (Phase 0 + the canonical field-gap decomposition)

**Method (P0 §1):** CPU-sampling profile (`samply`, 1000 Hz, CPU-weighted by `threadCPUDelta`) of
the real `cqlite-flight` server under sustained single-stream `--shape full` load; 3M rows / 4
uncompressed SSTables / narrow `keyvalue`; two orthogonal attributions (self-by-library and
by-operation) that **agree**.

**Where the single-stream CPU goes (P0 §3):**

| Stage | % CPU | What it is |
|---|--:|---|
| 4b — merge fan-in channel park/wake | **49.9%** | one `sync_channel` `send` **per row** (cap 256), ~94% kernel park/wake — overhead of thread-per-input decode, not merge math |
| 4a — k-way merge / reconcile compute | **32.5%** | `reconcile_cluster_with_overlap_counted`, heap refill — the true ceiling once 4b is removed (~2µs/row, machinery-dominated on singletons) |
| malloc (by-library) | 17.6% | per-row `RowKey::new(pk.to_vec())`, `MergeEntry`, `QueryRow` allocs + SipHash key hash |
| 2 — parse/decode | 9.7% | cell/vint decode |
| 3 — row materialize | 4.5% | `entry_to_row` → `QueryRow` |
| 5 — Arrow encode | 1.0% | cheap for 2 columns |
| 1 / 6 — IO+decompress / transport | 0.0% / 0.2% | **warm + uncompressed + loopback artifacts — real in the field** (P0 §5 caveats 1–3) |

Blunt truth: **~55% of single-stream CPU is kernel syscalls, ~18% allocator, only ~22% CQLite's own
logic.** IO (0%) and transport (0.2%) are rig artifacts.

**The canonical field-gap decomposition (P2:field-gap §4)** — shares of the **~47–50× per-node**
gap (~500k single warm M1 stream → ~10k rows/s/node), each with a falsifying measurement:

| # | Bucket | Share | Grade | Falsifying measurement |
|---|---|--:|:--:|---|
| 1 | **Hardware + core-contention** (i4i vCPU vs M1 P-core; 55% kernel tax *contends* on 4 shared cores, C(N)<1) | **25–35%** | MED | warm server-direct on i4i at conc 1 and pod-native, vs the 500k M1 anchor |
| 2 | **Field data-plane heaviness** (many SSTables → more reader threads → worse 4b; real tombstone/TTL/LWW reconcile overlap → heavier 4a) | **18–28%** | MED | SSTable-count/node + reconcile-overlap ratio; re-profile 4a/4b in-`stream` on field data |
| 3 | **Cold-IO *latency*** (synchronous mmap faults, `Auto`→mmap, **no `madvise`**, 128KiB readahead, multi-SSTable LBA interleave) — **NOT bandwidth, NOT decompress-CPU** | **10–22%** | **LOW** | cold-vs-warm A/B of the same full-ring scan; the cold−warm delta *is* this bucket |
| 4 | Distribution / concurrency shortfall | 6–14% | MED | B3 per-pod concurrency during the 61s scan (currently unrecorded) |
| 5 | Transport — network + HTTP/2 window | 5–10% | MED→LOW | server-direct over the real app-node network vs loopback |
| 6 | Connector page-build (`ArrowToTrino` row-at-a-time) | 3–8% | MED | through-Trino minus server-direct at equal concurrency (≈0 per P1.7) |
| 7 | LZ4 decompression CPU | **1–2%** | **HIGH** | i4i cold-compressed decode bench |

Buckets **1+2+3 ≈ 55–75%** (server-side per-node) dominate; **4+5+6 ≈ 15–30%** (Trino/transport)
is the axis P1.5 over-weighted at 45–80%; decompress (7) ≈ 1%. **The field's standing telemetry
cannot adjudicate this**: the 5-phase RPC metric (`validate→admission→resolve→merge_setup→stream`,
`obs.rs:210`) **folds the entire data plane into `stream`** — cold faults, LZ4, decode, merge,
reconcile, materialize, Arrow encode, and gRPC write are all one histogram. Only an in-`stream`
profile can settle it. This is why the i4i cold-vs-warm profile is the program's #1 item (§5).

---

## 3. The multiplier stacks (post-skeptic only, obeying the composition rules)

**Composition rules (authoritative — P2:parallelism §5):**
1. **ONE utilization credit for the whole width stack.** Sub-splits (#2680), transport fan-out
   (T6), and producer-side parallelism are **substitutes competing for one per-pod width budget**,
   all bounded by `min(N_admitted, N_drain_sat_postfix, N_mem) × C(N)<1`. **Do NOT** multiply
   #2680 × fan-out × producer-parallel; the first lever to reach the ceiling caps the rest.
2. **`width_postfix ≤ 4` (vCPU) for narrow CPU-bound rows**, `≤ N_mem ~16–24` for wide/network-bound.
   Today's realized width is **drain-bound ~4–8** (N_drain_sat), *below* cores, and rises toward the
   vCPU wall only **post-L1**.
3. **Per-stream ceiling levers compound on the Amdahl residual** — L1/L3/ArrowToTrino/window raise
   `per_stream_rows_s` AND raise the width ceiling (they are the multiplicand; width is the multiplier).
   Field-shape numbers, not rig-narrow.
4. **No L1 × #2680 product** — on one pod #2680 = 1.0× (skew fix only); their gains overlap in
   aggregate qps (P2:row-engine §7).
5. **L1 = the #2765-adjacent same-channel co-design** — L1 (batch, syscall axis) and #2765 (adaptive
   depth, memory axis) hit the **same** fan-in `sync_channel` (P2:row-engine §3.1 overrules P1.1's
   "different channel/orthogonal"). No throughput double-count, but a **mandatory** co-design on the
   256-cap constant.

**The surviving bottom-up stack (P2:stage2 §0, the honest band):**

```
Fixed single-thread pipeline (per-row channel deleted, L1/L2)   285k rows/s/core  (narrow rig)
  ÷ field derate  1.5–3×  (central 2×)                        → 95–190k /core  (central 143k)
  × C(N) on 4 vCPU  2.5–3.5×  (central 3×)                    → 238–670k /pod   (central ~420k)
       ├─ A4 Stage-1 (100k):  cleared with wide margin  → REACHABLE
       ├─ B3 Stage-2 (216–323k/pod = 54–81k/core):  fixed pipeline does 95–190k/core → cleared at central
       └─ A4 Stage-2 (600k):  only the optimistic corner (derate ~1.5×, C(N) ~3.5×)  → NOT reliable
```

The derate is **1.5–3×, not P1.2's 3–5×** (RF=3 is NOT a per-node reconcile multiplier — it is
compaction generation-overlap ~1.1–1.5×, deduped by the connector's one-replica pin; LZ4 is
~1.01–1.05×, not 1.3–2×; cold cache is a **latency** term, not a throughput derate — P2:stage2 §1).
C(N) is **2.5–3.5×, not 4×** (P2:stage2 §3).

**Gen-overlap term — now MEASURED (#2043 / M9,
[`docs/research/issue-2043-reconcile-overlap-multiplier.md`](../research/issue-2043-reconcile-overlap-multiplier.md)).**
The ~1.1–1.5× above was the one factor in the chain with nothing behind it; the measured k-curve
(k ∈ {1,2,5,10,20} × 5 collision mixes through the public `KWayMerger` drain, at a pinned `now`)
replaces the band with a function of the **overlap factor `o`** = generations per delivered row:

```
D(o) = (q + p·o) / (q + p)      p = 1.689 µs/input-row,  q = 1.127 µs/delivered-row
                                 (the `disjoint`+`ttl_expiring` OLS fit — lowest residual of four —
                                  over the saturated k ≥ 5 arms;
                                  o=1 ⇒ 2.82 µs/row FITTED — the measured saturated control is 2.81)
  o   1.0   1.25   1.5   1.75   2.0   3.0   4.0
  D  1.00  1.15  1.30  1.45  1.60  2.20  2.80      ← computed from the p, q above
```

Three corrections to how the term must be used:
1. **It is a row-DUPLICATION term, not an SSTable-COUNT term.** Measured: the disjoint control is
   flat in k (2851/2738/2829 ns/row at k = 5/10/20, no monotone trend) — reading 20 generations
   instead of 5 costs nothing per delivered row when no cluster spans two of them. The whole
   multiplier comes from the overwrite/update rate relative to compaction cadence.
2. **The floor is exact, and it is 1.00× not 1.1×.** An insert-once table (time-series/append-only,
   a primary connector target) has `o = 1.0` ⇒ the term should be **dropped**, not carried at 1.1×.
3. **The ceiling is optimistic for update-bearing tables.** `1.1–1.5×` implies `o ∈ [1.17, 1.83]`;
   at an ordinary STCS SSTables-per-read p99 of 3–4 the term is **2.2–2.8×**, outside the band.

The `o` substituted below — **`o_field` = 1.25–1.5, central 1.35 ⇒ D ≈ 1.15–1.30, central ~1.21** — is
an **ASSUMPTION, NOT A MEASUREMENT** (STCS-derived expected-k band; the vendored corpus is
single-generation so field `o` is unmeasurable locally). **#2818 (M0) is the measurement that
replaces it**; because the model is closed-form in `o`, substituting a measured `o` needs no
re-derivation and no re-run. TTL expiry at a pinned `now` was measured **free** (the `ttl_expiring`
arm tracks `lww_overwrite` to 0.9 %, inside run-to-run spread), so no separate TTL derate term is
warranted; deletion load, by contrast, costs **+3.9 %** over plain overwrite (the `tombstone` arm's
marginal cost is 1580 vs 1507 ns per extra input row), so it is inside the `p` term, not free.

Per-drain SETUP is amortized, not caveated: the bench's timed region contains `new_from_readers`
(one producer-thread spawn + one adapter open per generation), so the arm width was raised 4× — on
the PARTITION count, because `MergeStep::Partition` materializes a whole partition at a time — and
each arm's setup share is MEASURED and printed: **0.20–0.24 % at k = 1, 0.37–0.85 % at k = 20**,
which moves any multiplier by ≤0.6 %. The figures above are therefore per-ROW costs, not per-scan
fixed cost smeared over a thousand rows (owner decision 2026-07-26).

**Which stack reaches what:**
- **A4 Stage-1 / B3 Stage-1 / B3 Stage-2 / B2:** the width credit (up to ~4 vCPU × C(N)) × the L1/L3
  per-stream residual clears all of these at the central band.
- **A4 Stage-2 (600k):** falls short at the center (~420k). The gap is **C(N)** (2 physical cores)
  + reconcile machinery — **not** Arrow encode (1%), **not** the container format, **not** the
  columnar option. No single lever closes it on the narrow corpus; the honest closers are wider pods
  (more physical cores) or the columnar increment **on wide rows** — accept that (P2:stage2 §8).
- **Worker ingest 250–350 MB/s:** narrow via row engine = NOT credible (~1.4M rows/s needed); wide
  (≥1KB) at ~100k rows/s = ~100 MB/s, 2–3KB = 200–300 MB/s → reachable **for wide rows**, via Arrow
  batches (the CQLite producer, not the JVM page builder, is the limiter — P1.8 §3c).

---

## 4. Surviving lever table + the graveyard

**Surviving levers** (post-skeptic multiplier = field shape; cost S/M/L):

| Lever | Post-skeptic multiplier (field) | Cost | Risk | Verdict provenance |
|---|---|:--:|---|---|
| **L1** batch fan-in `sync_channel` | util **1.5–1.9× rig-narrow ceiling**; single-stream ~1.05–1.15× field; **raises C(N) + N_drain_sat** | M | Med (co-design #2765 same channel; cut msg-capacity for B4; keep #2419 gauge/#2361 cancel-recv) | P2:row-engine (SURVIVES-weakened) — **#1 lever, prerequisite for C(N) and fan-out** |
| **L3** reconcile singleton fast-path | disjoint-narrow **~1.20× upper**; **field-w/-TTL/overlap ~1.03–1.08×** | M–L | High (byte-parity; query-semantics + point-vs-full oracles load-bearing) | **Disposition unresolved — see §4 tension flag** (P2:stage2 ranks #2 / P2:row-engine WEAKENED) |
| **L4** `RowKey` Arc hoist (#1883) | ~~1.05–1.09× multi-row-partition~~ → **MEASURED 1.0× no-op, see §7 M4** | S–M | Low | P2:row-engine (SURVIVES re-scoped) — win governed by clustering fan-out, unknown from profile |
| **L5** FxHash `row_values` map | ~~~1.04×~~ → **deferred #2901, unmeasured, see §7 M4** | S | Low | P2:row-engine (SURVIVES) — target confirmed still SipHash |
| **L2** inline/thread-less merge | ~1.4–1.8× narrow single-stream; **≤1.0× wide/field → not credited in field stack** | L | High (shape-fragile; A/B-gated) | P2:row-engine (SURVIVES narrow-only) |
| **#2680** weight-balanced sub-splits | util (skew fix) up to 2–4× on *lagging* pods; **0× on one pod** | M | Med (P0 #2782 hang; needs early-close drain fix) | P2:parallelism P-A (SOUND) — K=2 rotation carries flight-pod balance, NOT SplitWeight |
| **#2765** adaptive egress budget (+ fan-out T6) | stability enabler; bounds fan-in growth; unlocks higher useful concurrency | M | Med (bounds fan-in only, not the 57k-row Arrow egress) | P2:parallelism L4/L3 |
| **T4** byte-bounded batch | ~1.0–1.1× (robustness/B4, not throughput) | S | Low | P2:transport (SURVIVES minor) |
| **T1+T2** bulk per-column `ArrowToTrino` + async prefetch | **~1.0× today; ~1.1–1.3× combined only AFTER server ceiling raised** | M | Med (off-heap lifetime vs Trino page graph) | P2:transport (WEAKENED — reframed bulk copy, not zero-copy) |
| **madvise(WILLNEED)** under Auto-mmap | ~1.0× warm; marginal cold-p99 hedge (B4 justification **stale** post-#2412) | S | Low | P2:io (RESPEC — policy flip on built machinery) |
| **Chunk-cache retune** (256MiB→smaller) | 0× throughput; closes a B4 **peak** hazard | S (config) | Low | P2:caching §1 |
| **Decoded-partition cache** (K-A/K-D) | **~1.5–3× keyed IF the skew is real — UNMEASURED** | L | High (3.5× decoded size, B4; #2037 overlap) | P2:caching §4 — gate on measured access distribution |

**Sequence (P2:stage2 §8):** **L1 → L3 → fan-out-past-drain (gated #2765)**. L1 is both the biggest
single-stream lever and the enabler of C(N); nothing scales without it. L5+L4 were projected as a near-free warm-up (both since retired by measurement — §7 M4); a near-free warm-up
bundle. #2680 re-land runs in parallel on the connector tier.

**§4 tension flag (phase2-vs-phase2, L3 disposition — RESOLVED CONDITIONALLY, #2043 / M9):**
P2:stage2 §6 ranks L3 the **#2 highest-value ceiling lever** (it attacks the measured ~2µs/row
singleton machinery = the true ceiling once L1 lands; ~1.20× disjoint-narrow). P2:row-engine §4 rules
it **WEAKENED** (field data with TTL/overlap **never hits** the fast-path; ~1.03–1.08×, and pushed
toward the low end). The disagreement is entirely about **field cluster shape** (singleton vs
multi-generation overlap).

**The overlap data now exists** —
[`docs/research/issue-2043-reconcile-overlap-multiplier.md`](../research/issue-2043-reconcile-overlap-multiplier.md)
§6 — and it resolves the disposition **conditionally, with the arithmetic written out**. L3's saving
is a fixed ~0.47 µs/row (= `1 − 1/1.20` of the 2.81 µs/row at `o = 1`), and overlap attacks
it **twice**: it destroys fast-path eligibility `f(o) ≈ max(0, 2 − o)` AND it grows the denominator
the saving divides into (the overlap cost is entirely per-INPUT-row decode/heap/resolve, which a
singleton fast-path cannot touch). `S(o) = 1/(1 − 0.47·f(o)/(q + p·o))`:

| `o` | 1.0 | 1.1 | 1.25 | **1.35** | 1.5 | 1.75 | ≥2.0 (or ANY `o` with TTL/tombstones) |
|---|---|---|---|---|---|---|---|
| **L3** | **1.20×** | 1.16× | 1.12× | **1.10×** | 1.07× | 1.03× | **≈1.00×** |

⇒ **P2:stage2's 1.20× is correct only at `o = 1.0` on a TTL-free, tombstone-free table** (its rig
fixture exactly); **P2:row-engine's 1.03–1.08× is correct for `o ≳ 1.5`, or for ANY `o` once a queried
column carries TTL** (TTL was measured *free* in the merge yet still disqualifies the cluster — a pure
eligibility loss with no compensating saving). At §3's **assumed** central `o ≈ 1.35`, L3 is **~1.10×**.
**Resolution: L3 does NOT earn the #2 headline slot — keep it off the headline lever list and sequence
it after L1 and after M0 (#2818).** The final call needs exactly two field numbers from M0 — the
row-duplication distribution (`o`) and whether queried columns carry TTL — after which the table above
yields the disposition with no further measurement.

**Other phase2-vs-phase2 tensions flagged:**
- **C(N) magnitude.** P2:stage2 defends central **C(N) 3×** (pod-vs-core). P2:parallelism is more
  conservative — width `≤4` vCPU for CPU-bound rows, drain-bound **~4–6 today**, × C(N)<1, ⇒ realistic
  post-fix ~2.8×. They agree 600k needs the optimistic corner; stage2's 3× sits at the **top** of
  parallelism's realistic post-L1 band. Flagged, not silently picked.
- **Cold-IO denominator.** P2:field-gap sizes cold-IO **latency** at 10–22% of the *wall-clock*
  ~47× field-vs-local gap; P2:stage2 §1e **removes** cold cache from the *throughput* derate entirely
  (reclassed to a latency term against the B3 3s budget). Complementary (different denominators), not
  contradictory — but the two must not be summed.

**The graveyard (killed / demoted — honesty deliverable):**

| Item | Disposition | One-line reason |
|---|---|---|
| **T3** HTTP/2 window sizing | **KILLED** | client window is already **1MiB with BDP auto-tuning** (grpc-java 1.79.0), not 64KB; the tonic-server knob is the wrong flow-control direction for `do_get` egress (P2:transport §2). Close as "not applicable in this stack." |
| **Columnar scan producer** (P1.2 option a) | **DEFERRED past 0.17** | ~1.05× narrow / 1.5× wide; touches neither the 49.9% channel nor the ~2µs/row reconcile; Stage-3 prep. Revisit only on sharpened #2605 data >1.3× on wide/overlap corpus (P2:stage2 §6). |
| **`io_uring` / forced `O_DIRECT` scan** | **NOT filed** | IO is not throughput-binding below multi-GB/s row rates (cold bandwidth need 10–120 MB/s vs ≥1GB/s NVMe, ~8–30× headroom); gated behind the i4i measurement (P2:io §4, P1.3 §2b). |
| **Idle-drain sweeper (S-D)** | **DEMOTED to P3** | B4 has **no idle clause**; caches are lazy LRUs sitting near-zero at idle (a scan populates ~0 of the chunk cache); the field meets the ≤512Mi peak (0 OOMKills). Preventive hardening, gates nothing (P2:caching §1). |
| **#2561** BTI chunk-straddle | **already FIXED on main** (PR #2554) | only a BIG-path verify test remains; P3 traceability-close, ride #2565 nit batch (P2:caching §2). |
| **T5** Flight-body compression | **demoted → latent** | net-**negative** for narrow rows (steals server CPU from the bottleneck coordinator); no network-bound case today (T3 headroom analysis); latent WAN lever only (P2:transport). |
| **L5-intra-query parallel merge** (P1.6 L5) | **DO NOT pursue** | it is #2680 rebuilt inside the server at higher cost — same C(N) tax, new scheduler + cancellation + concat (P1.6 §2). |
| **T1 "near-zero-copy" naming** | **reframed** | it is a **bulk per-column on-heap copy** (Arrow buffer freed on stream advance), not zero-copy; win = alloc-count + dispatch elimination, not copy-bandwidth (P2:transport §3). |
| **RF=3 as a reconcile multiplier** | **killed misattribution** | reconcile is per-node-per-cell; the connector pins one replica; real term is compaction generation-overlap ~1.1–1.5× (P2:stage2 §1a). |
| **#2165 as a decode-speed lever** | **worthless** (keep for maintainability) | Stage 2 is 9.7% and it is decode-plane *consolidation*, perf-neutral; cache value is keyed/repeated-range only (P2:caching §3, P1.1). |
| **AE Arrow-encode series** (#1497/#1498/#1500) | **demoted for scan** | Stage 5 = 1.0% → ≤1.01× on narrow scan; remain valid for the export/wide-collection path they were filed under (P1.1 §3). |
| **#941 full DataFusion program** | **NOT a 0.17 throughput pull** | owner-gated MPP surface; DataFusion does not speed the reconcile; its A3/#1907 bounded-stream blocker overlaps the deferred columnar producer (P1.2 §3.1, P1.7 Option 2). |

---

## 5. Measurement first

**#1 — i4i cold-vs-warm server-direct profile (P1 — the program's first item).** Replay P0's exact
`samply`/`perf` methodology on the field i4i.xlarge pod, server-direct (`flight-loadgen` →
`cqlite-flight`, `--shape full`), **cold then warm**, capturing **both on-CPU and off-CPU (blocked)
time**, recording SSTable-count/node and the resolved `DiskAccessMode`. It **adjudicates the 55–75%
server-side bucket** in one run: cold−warm delta = bucket 3 (cold-IO latency); warm-i4i vs the 500k
M1 anchor = bucket 1 (hardware+C(N)); it is the first-ever measurement of Stage-1 (IO+decompress)
and Stage-6 (transport); server-direct removes Trino/connector as confounders (P2:field-gap §6).
**Everything IO-heavy is gated behind this** — do NOT lead with io_uring/O_DIRECT/DataFusion.

**#2 — the phase-metric data-plane split.** The field's 5-phase RPC metric folds the whole data plane
into `stream`, so no dashboard can express the P1.3↔P1.5 disagreement (P2:field-gap §2). Split
`stream` into sub-phases (cold-fault / decompress / merge / encode / gRPC-write) so the field can
attribute in-`stream` cost without a profiler on every run.

**#3 — the keyed access-distribution probe (gates A2 work).** The decoded-partition cache's ~1.5–3×
rests on an **unmeasured** hot-set skew; the only field keyed loadtest on record is ~0.9 qps with no
captured concentration (P2:caching §4). Instrument the field keyed partition access distribution
(Zipf/skew over `cqlite.read.partition_lookup.*`). This is the **gate** that decides whether the A2
decoded-cache lever is worth building.

**#4 — B3 per-pod concurrency capture.** The "12/64 → ~4 streams" divisor is borrowed from the R12
B2 saturation probe, not the B3 scan; the scan's actual per-pod concurrency during the 61s is
unrecorded (P2:field-gap §1). Capture it — it retires a load-bearing guessed divisor and confirms
bucket 4 is small.

---

## 6. NEEDS-OWNER list

1. **A4 Stage-2 rung** — revise the ~600k/pod goal to **~400–450k/pod** on the pure row engine, or
   hold it aspirational pending the i4i measurement? Central is ~420k; 600k is the optimistic corner
   (2 physical cores). (P2:stage2 §8.)
2. **Ingest target's implicit row-width assumption** — 250–350 MB/s is not credible on narrow rows
   via the row engine; state the assumed row width on record (it presumes ≥1KB rows or columnar).
   (P1.5 §8, P1.8 §3c.)
3. **Snapshot-reuse-window default** (freshness) — the 3s window (`CqliteFlightConfig.java:84`) sets
   max data staleness; lengthening it is the cheapest keyed lever in the program but is a
   product/data-semantics call (mirrors #2305). Knob exists; the *value* is the owner's. (P2:caching §5.)
4. **Decoded-cache (K-A) vs #2037 sequencing** — the measurement precursor (#3 above) proceeds
   standalone; the *cache build* overlaps the owner-gated #2037 WS6 per-generation Arrow cache.
   Unblock the measurement; keep the build reconciled with #2037 so it isn't built twice. (P2:caching §4.)
5. **B4 "≤16Mi" semantics, on record** — confirm the ratified reading is **per-query working set**,
   not idle-pod memory, so no future agent re-derives a phantom idle gate. (P2:caching §1/§6.)
6. **The Flight bulk-path API** (new public surface) — P1.7 Option 4 (Hybrid): add a ticket-level
   Flight-native bulk-export path so A4/A-lane consumers (pandas/Spark/ADBC) get server-direct rates
   and skip the coordinator floor. A4 is a server-direct goal already; this is *how* it reaches real
   consumers. New public surface → a product call on whether to pursue.
7. **B3 Stage-2 strategy** — the coordinator floor (0.2–2s) inside the 3s budget is the swing factor.
   Accept the floor (measure it ≤~1.2s for this scan shape), route the bulk-path around it, or defer
   Stage-2? This is a Trino/IO measurement + product call, not a row-vs-columnar decision. (P2:stage2 §2.)

---

## 7. The 0.17 backlog manifest

Derived from the surviving levers, deduped against `throughput-backlog-inventory-2026-07.md`.
**Sequenced by dependency;** items gated on the i4i measurement (M0) are marked. Acceptance criteria
are in flight-loadgen/perf terms with the number each must demonstrate.

**Compact index** (action | title | P | gated-on):

| # | Action | Title | P | Gated-on |
|---|---|---|:--:|---|
| M0 → #2818 | NEW | i4i cold-vs-warm server-direct `samply` profile | P1 | — (first) |
| M1 → #2819 | NEW / EXTEND #1686 | Split `stream` RPC phase into data-plane sub-phases | P2 | — |
| M2 → #2820 | NEW | L1 batch fan-in `sync_channel`, co-designed with #2765 | P1 | M0 (informs), #2765 co-design |
| M3 → #2765 | EXTEND #2765 | Productionize adaptive egress budget + fan-out-past-drain + L1 256-cap co-design | P2 | M2 |
| M4 → #1883 | EXTEND #1883 | per-row alloc ratchet DELIVERED; L4 measured 1.0× no-op; L5 deferred → #2901 | P2 | — |
| M5 → #2680 | EXTEND #2680 | Re-land: K=2 opt-in rotation + early-close cancel fix + admission-resize + #2792 required | P1 | #2782, #2792 |
| M6 → #2821 | NEW | Streaming `do_get` result-budget wiring gap | P2 | — |
| M7 → #2822 | NEW (investigate) | L3 reconcile singleton fast-path — **demoted off the headline list** (~1.10× at the assumed field `o`, §4) | P3 | M0 (M9 ✅ delivered) |
| M8 → #2823 | NEW | L2 inline/thread-less merge (A/B-gated, narrow-only) | P3 | M2 |
| M9 → #2043 | EXTEND #2043 | Repoint WS7 to pin the reconcile **overlap** multiplier — **✅ DELIVERED**, §3 term now `D(o)`; L3 resolved conditionally (§4) | P3 | — |
| M10 → #2824 | RESPEC #1518-adjacent (NEW) | `madvise(WILLNEED)` under Auto-mmap + `MADV_DONTNEED` post-scan | P3 | M0 (re-measure) |
| M11 → #2825 | NEW | T4 byte-bounded batch sizing | P2 | — |
| M12 → #2826 | NEW | T1+T2 bulk `ArrowToTrino` per-column copy + async prefetch | P3 | M2/M3 (post-server) |
| M13 → #2827 | NEW | Keyed access-distribution probe: instrument + decision procedure (re-scoped; verdict lands with a real keyed workload — scoped to BTI and BIG-with-resident-index, see §M13) | P2 | — |
| M14 → #2828 | NEW (config) | Chunk-cache `block_cache.max_size` retune for 512Mi pod | P2 | — |
| M15 → #2605 | EXTEND #2605 | Sharpen the DataFusion PoC measurement | P2 | — |
| M16 → #2165 | RE-SCOPE #2165 | Decode-plane consolidation only (not a throughput lever) | P3 | — |
| M17 → #2561 | CLOSE-WITH-VERIFY #2561 | BIG-path present-key no-scan verify test, then close (CLOSED) | P3 | — |

**Body drafts + acceptance:**

- **M0 (#2818) — i4i cold-vs-warm profile (NEW, P1).** Replay P0's `samply` method on the field i4i.xlarge
  pod server-direct, cold then warm, on-CPU + off-CPU, recording SSTable-count/node + `DiskAccessMode`.
  *Accept:* produces the first in-`stream` CPU decomposition on field hardware; reports cold−warm delta
  (bucket 3), warm-i4i-vs-500k-M1-anchor ratio (bucket 1), and Stage-1/Stage-6 shares; confirms or
  refutes buckets 1+2+3 ≈ 55–75%. **Dep:** none — first. **Dedup:** new; the field 5-phase metric
  cannot express this.

- **M1 (#2819) — phase-metric data-plane split (NEW / EXTEND epic AI #1686, P2).** Split the `stream` RPC
  phase into sub-phase timers (cold-fault / decompress / merge / encode / gRPC-write). *Accept:* a
  field dashboard attributes in-`stream` cost across ≥4 sub-phases; the cold-fault sub-phase is
  isolable from `send` park/wake. **Dep:** none. **Dedup:** extends observability epic AI #1686
  (#1701/#1705/#1707 why-slow phase timings) rather than a new metrics stack.

- **M2 (#2820) — L1 batch fan-in `sync_channel` (NEW, P1).** Accumulate `Vec<MergeEntry>` in `forward_row`,
  `send` one message per `BATCH_EMIT_ROWS` rows; consumer drains a held batch. Reuse F2/#1592's
  `BATCH_EMIT_ROWS` + send-count/parity-oracle pattern. **Co-design with #2765 on the SAME channel**
  (256-cap constant becomes batch-unit); **cut message-capacity** so resident rows stay bounded for
  B4; preserve the #2419 `egress_channel_depth` gauge accounting and the #2361 cancel-aware
  `recv_timeout`. *Accept:* per-row `send` count drops ~256× (send-count oracle); util throughput on
  the server-direct `flight-loadgen --shape full` narrow rig rises measurably toward the 1.5–1.9×
  ceiling; query-semantics + point-vs-full oracles green; peak RSS/stream unchanged. **Dep:** M0
  informs but does not block (49.9% is already measured). **Dedup:** NEW — #2765/#2600 bound the
  *outer* egress; F2/#1592 batched a *different* (public `scan_stream`) channel; the inner fan-in
  `sync_channel` has zero coverage.

- **M3 (#2765) — #2765 productionize + fan-out-past-drain + L1 co-design (EXTEND #2765, P2).** Land the
  `clamp(BUDGET/active_merges, MIN, 256)` impl; reconcile the budget's message-unit with L1's batch
  (else `clamp(…,256)` budgets 256 *batches* = 65k rows); then raise effective fan-out toward the
  memory-safe admission ceiling. *Accept:* egress depth stays bounded under 80-thread overload
  (no depth-8080 blowup) at <10% qps cost; fan-out raises realized per-pod concurrency only after L1
  lands (no #2600 re-fire). **Dep:** M2. **Dedup:** extends #2765 (inventory: "extend #2765, don't
  refile").

- **M4 (#1883) — per-row alloc ratchet DELIVERED; L4 measured no-op; L5 deferred.**

  - **Per-row allocation ratchet — DONE.** At the public `build_row_from_scan_cached`, using the in-crate
    `test_alloc_probe` counting allocator. **Measured**, with the one-time setup cost held SEPARATE from the
    per-row rate (they were initially conflated; separating them is what makes the budget valid at any row
    count): **9 allocations once** — the result-collector `Vec` plus the first row's `PartitionKeyCache` MISS,
    which pays the whole `decode_partition_key_columns` inside the measured region — plus a steady-state
    **4 allocations/row narrow (3 cols)** and **33/row wide (32 cols)**, i.e. `1 sized row map + 1 per cell`.
    Totals at 8 rows: **41 narrow, 273 wide**. Solved from two row counts (8 rows = 41, 4 rows = 25) and
    confirmed independently against the wide fixture; the budget holds at 4, 8 and 16 rows. The dominant
    per-row cost is the #1644 retention compaction (`Value::into_owned`'s TIER-1 copy of a small payload),
    NOT hashing and NOT key handling. Two differential controls, both verified RED-on-revert:
    dropping the per-cell intern (#1334) takes narrow 41 → **89** and wide 273 → **785** (exactly +2 per
    cell), and dropping the map's capacity hint (#1584) takes narrow 41 → **49** (+1 per row of rehash
    growth). Each property is gated by a strict `<` against a pre-fix reference, not by the absolute
    constant alone.
  - **Scope correction (measured).** #1883's premise — that this ratchet would gate #1447/#1445/#1446 — does
    not hold: those are **binding-layer** fixes (#1447 = `bindings/node` `ExecuteNativeTask::compute`; #1446 =
    Node JsString interning; #1445 = Python `Row` ordering). Reverting the clone→move *in this crate* is
    exactly allocation-neutral (41 vs 41, 273 vs 273) because `Value::Text` is `Bytes`-backed and TIER-1
    compaction copies small payloads either way. Binding-layer probe → **#2894**.
  - **L4 (RowKey `Arc` hoist) — measured 1.0× / NO-OP, not implemented.** The partition-key path costs **zero**
    per-row allocations: `RowKey` is `Arc<[u8]>` (clone = refcount bump) and `PartitionKeyCache` (#1817)
    already hoists the partition-constant decode. No hoistable per-row `Arc` allocation exists, so **no field
    win is claimed and no follow-up is filed** for L4.
  - **L5 (FxHash row map) — implemented, then REVERTED before merge; deferred to #2901.** Implementing it
    established two things the design had not anticipated: (a) it is a **public breaking API change** —
    `row_values` moves straight into `QueryRow.values`, so the hasher cannot change without changing that
    public field's type, rippling through `cqlite-core`/`cqlite-flight`/`cqlite-cli`; and (b) it **contradicts
    the `rustc-hash` invariant** in `cqlite-core/Cargo.toml` (#1590 E8 — reserved for integer/digest keys, NOT
    untrusted string keys), since on the default read path column names come from the file's `Statistics.db`
    serialization header and are attacker-controlled for a hostile SSTable, where FxHash's easy collisions give
    O(N²) per-row inserts. **No benchmark was run, so the projected ~1.04× stays a projection and no L5 win is
    claimed.** Revisit behind a measurement + a HashDoS answer + an API plan: **#2901**.

  **Dep:** none. **Dedup:** L4 lived under #1883; L5 split out to #2901.

- **M5 (#2680) — #2680 re-land (EXTEND #2680, P1).** Default **K=1** (byte-identical pre-#2680), **opt-in
  K=2** (the sub-split *rotation* carries flight-pod balance — NOT `getSplitWeight()`, which is
  Trino-worker accounting), **never K=4 default**. Fix the early-close drain (a
  `producer_thread_from_reader` blocked in `send` on a full fan-in channel must wake on `ScanCancel`).
  Bundle the **memory-derived admission resize** (`--max-concurrent-scans` 64 → ~16–24 from
  512Mi/per-stream-MB) as a forward guard. Require #2792 (Flight↔Trino E2E as a `required` check).
  *Accept:* `SELECT id … LIMIT 2` under K≥2 returns 2 rows in **<5s** (not the 180s #2782 hang) in the
  now-`required` E2E lane; server-side early-close drain unit test (no producer left blocked); busiest
  pod CPU ≤~1.3× median @32thr (report-only next round); #2679 point read = 1 DoGet at any K; K=1
  identity byte-for-byte. **Dep:** #2782 resolved, #2792. **Dedup:** re-land of reverted #2779; extends
  #2680. **Owner note:** #2782 has no milestone — flag to owner (likely 0.16).

- **M6 (#2821) — streaming result-budget wiring gap (NEW, P2).** The 64MiB `QueryConfig::n` result budget is
  enforced only on the materializing `CollectSink` path; `rg result_budget cqlite-flight/src` returns
  nothing — the streaming `do_get` path is bounded **only** structurally by the 4-deep batch channel
  + admission K (P2:parallelism §2). *Accept:* either a byte budget is enforced on the streaming egress
  (bounded per-stream residency independent of row width), or the gap is documented + admission is the
  sole documented governor with a test pinning the per-stream ceiling. **Dep:** none. **Dedup:** NEW —
  not covered by #2230/#2423 (which bound intra-partition materialization).

- **M7 (#2822) — L3 reconcile singleton fast-path (NEW, investigate, P3).** In
  `reconcile_cluster_with_overlap_counted`, when a cluster is a lone Live entry with no
  row/complex/range deletion, no TTL/expiring cell, no dropped columns, emit directly and skip
  `ReconcileState` construction + the winner map. *Accept:* the same PR extends
  `query_semantics_oracle_parity.rs` AND `point_vs_full_differential.rs` with a
  singleton/TTL-collision/tombstone case (so `--lite` exercises the fast-path vs the full path at a
  pinned `now`); demonstrates ~1.20× on the disjoint-narrow-no-TTL fixture. **Dep:** M0 only — **M9
  (#2043) is ✅ delivered** and its overlap data resolves L3's disposition CONDITIONALLY (§4): the
  1.20× holds only at overlap factor `o = 1.0` on a TTL-free, tombstone-free table, and L3 falls to
  **~1.10× at the assumed field `o ≈ 1.35`** and to ≈1.00× on any table with a TTL'd queried column
  — so **L3 is demoted off the headline lever list**, and M0's row-duplication + TTL-presence numbers
  are the only inputs still needed to settle it (see
  [the record](../research/issue-2043-reconcile-overlap-multiplier.md) §6). **Dedup:** NEW; #2213
  (Murmur3 per-comparison) is a minor complement.

- **M8 (#2823) — L2 inline/thread-less merge (NEW, A/B-gated, P3).** For `k` inputs ≤ threshold + single
  stream, pull each `RunReader` on the coordinator thread — no producer threads, no `sync_channel`.
  *Accept:* ships behind a `k`+stream-count A/B gate that fires **only** where it wins; ~1.4–1.8× on
  the narrow few-SSTable fixture; **≤1.0× wide → not credited in the field stack**; byte-parity
  oracles green. **Dep:** M2. **Dedup:** NEW — no prior inline-merge bypass in `git log`.

- **M9 (#2043) — #2043 repoint (EXTEND #2043, P3). ✅ DELIVERED (2026-07-26).** Repointed WS7 to pin
  the reconcile **overlap multiplier** at field compaction state (the base was already ~2µs/row
  measured, machinery-dominated — NOT the `[ASSUMED]` 10–500ns/row). *Delivered:*
  `cqlite-core/benches/reconcile_overlap.rs` (advisory instrument, 25 arms = k ∈ {1,2,5,10,20} × 5
  collision mixes through the public `KWayMerger` drain at a `now` pinned via `with_now_secs`, plus 2
  producer-count control arms; every arm asserts its collision-shape census before timing) +
  [`docs/research/issue-2043-reconcile-overlap-multiplier.md`](../research/issue-2043-reconcile-overlap-multiplier.md).
  *Outcome:* the §3 gen-overlap term is now a **function of the overlap factor `o`**,
  `D(o) = (q + p·o)/(q + p)` with p = 1.689 µs/input-row and q = 1.127 µs/delivered-row (§3) — SSTable
  count is free, row duplication is the whole term, the floor is 1.00× (insert-once) and the ceiling
  reaches 2.2–2.8× at an ordinary STCS p99. Field `o` remains an explicit **assumption** (1.25–1.5,
  central 1.35) pending M0. **L3's disposition is resolved conditionally** (§4): ~1.10× at the assumed
  central `o`, so it does **not** earn the #2 headline slot. **Dep:** none. **Unblocks:** M7 (#2822).

- **M10 (#2824) — madvise(WILLNEED) respec (NEW, P3).** *[Updated 2026-09-01: the `WILLNEED` half has
  SHIPPED as slice 1 — `Auto` no longer maps to `None`, so the "currently gated off" clause below is
  stale. The `MADV_DONTNEED` half is sliced out to a follow-up; see issue #2824 REQ-2824-02. Priority
  was raised P3 → P2 by the 2026-08-30 adjudication.]* Flip `Auto` to issue the already-built
  `madvise(MADV_WILLNEED)` at open (`reader/mod.rs:1052`, currently gated off by `PrefetchMode::Auto →
  None`) + `MADV_DONTNEED` post-scan-once; keep `posix_fadvise` only behind an explicit
  buffered/direct backend. **Do NOT reach for `MADV_SEQUENTIAL`** (the #1143 ~2× p99 drop-behind
  regression; re-run `issue_1143_mmap_prefetch_tail_guard.rs` if touched). *Accept:* cold-p99 on a
  cold i4i scan improves marginally with no warm regression and no #1143 tail reintroduction. **Dep:**
  M0 (the B4 justification is **stale** post-#2412 — reprice as a cold-p99 hedge, defer behind the
  i4i re-measurement). **Dedup:** NEW; respec of P1.3's fadvise lever (dead code on the Auto-mmap path).

- **M11 (#2825) — T4 byte-bounded batch (NEW, P2).** Cap MB/batch (finish on whichever of row-cap 8192 /
  byte-cap trips first). *Accept:* wide-row batches stay under a configured byte ceiling; the
  ~49,152-row Arrow egress buffer — `(DO_GET_CHANNEL_CAPACITY = 4 + ~2 in-flight) × 8192`; the
  earlier 57,344 figure over-counted by folding in the `#[cfg(test)]`-only `IN_FLIGHT_ALLOWANCE = 3`
  (`cqlite-flight/src/streaming.rs:86-87`, correction recorded at
  `docs/research/phase2-verify-parallelism.md:94-100`) — stops scaling to ~8MB/batch on wide rows
  (helps B4). ~1.0–1.1×
  throughput — filed as a **robustness/correctness** lever, not throughput. **Dep:** none. **Dedup:**
  NEW connector/server surface; #1476/#2230 are the read-path analogue.

- **M12 (#2826) — T1+T2 connector follow-on (NEW, P3, post-server).** Bulk per-column `ArrowToTrino` copy
  (varchar via `VariableWidthBlock(Slice, offsets, nulls)`, one `Slice.copyOf`/column; kill the 3.9M
  per-scan `byte[]` allocs) + async batch prefetch (double-buffer `FlightStream.next()`). *Accept:*
  page-build CPU drops ~10–20× isolated; combined pod effect ~1.1–1.3× **measured only after the
  server ceiling is raised** (M2/M3) — at field per-stream rates today it is ~1.0× (page-build is
  ~0.05% of the per-batch cycle). **Dep:** M2/M3 — sequenced LAST. **Dedup:** NEW connector surface
  (AE #1470 is the server-side sibling); it is a **bulk copy**, not zero-copy.

- **M13 (#2827) — keyed access-distribution probe: instrument + decision procedure (NEW, P2).**
  **RE-SCOPED (2026-08-06).** Delivers **the instrument and the procedure, not the field number.**
  A bounded, default-OFF partition repeat-access histogram (`cqlite.read.partition_access.*`, six
  buckets `1|2|3-4|5-8|9-16|17+`, fixed 3 MiB, no per-key attribute — the originally-planned
  skew-from-`partition_lookup.*` method is impossible, since that counter carries only bounded
  attributes and per-key labels are forbidden), plus MEASURED distinct-partition working-set bytes
  and a committed decision procedure at `docs/research/decoded-partition-cache-decision.md`.
  *Accept:* reports the hot-set concentration shape of whatever workload runs with the probe
  enabled — the **verdict lands with a real keyed workload**, with no further analysis round.
  **That is SCOPED, not universal:** it holds for BTI and for BIG whose `Index.db` is already
  resident. The probe will not materialize an index to answer (that would defeat #2412's lazy
  Summary-guided open and change the process memory profile), so a Summary-guided BIG window is
  REFUSED rather than priced — as are a non-census window and one with a non-zero `unavailable`
  fraction. All three fail SAFE (a refusal is never a false "go"), but the FIRST window may be
  refused. Separately, `H_max` is an ESTIMATE under a stated ranking heuristic, not a ceiling, and
  **#3340 must land before any go/no-go verdict is derived from a real production window.**
  **NOT delivered:** the field skew number and the 64–128 MiB go/no-go. That AC is **not satisfied
  and not waived** — it is blocked solely by the absence of a field keyed workload with captured
  concentration (`docs/research/phase2-verify-caching.md:214-216`); the only keyed loadtest on record
  is ~0.9 qps with no reported concentration. A synthetic Zipf sweep was rejected as a circular
  oracle. **Dep:** none — standalone, decoupled from #2037. **Follow-up:** #3330 (keyed loadgen mode,
  the natural driver). **Dedup:** NEW; it is an **input** to the K-A decoded-cache decision, not the
  gate for it (#2037 WS6 stays owner-gated).

- **M14 (#2828) — chunk-cache retune (NEW config, P2).** Retune `block_cache.max_size` (256MiB default =
  `max_memory/4`) down for the 512Mi Flight/Trino pod; confirm cqlite-flight currently inherits the
  library default (no override found). *Accept:* a filled chunk cache + concurrent stream working sets
  stay under ≤512Mi peak under sustained point-read load; no throughput regression. **Dep:** none.
  **Dedup:** config retune, not a refile; the real (peak) B4 item (idle-drain is demoted, §4).

- **M15 (#2605) — #2605 sharpen (EXTEND #2605, P2).** The DataFusion PoC must (1) isolate the
  **decode-to-column** delta from the **vectorized-exec** delta; (2) measure on **wide + RF=3/overlap**
  shape, not RF=1 narrow; (3) record peak memory under B4 512Mi; (4) confirm both arms consume
  **post-reconciliation** batches. *Accept:* reports the isolated decode-to-column delta on a
  wide/overlap corpus; feeds the columnar-producer slot trigger (>1.3× → revisit; else Stage-3 prep)
  and the #941 promotion decision. **Dep:** none. **Dedup:** sharpens existing 0.16 #2605.

- **M16 (#2165) — #2165 re-scope (RE-SCOPE #2165, P3).** Keep as **decode-plane consolidation**
  (`chunk_decode_single_plane.rs`), **not** a scan throughput lever. If wired-and-populated from
  scans, it MUST ship with scan-resistant admission (K-G) or a scan evicts the keyed hot set.
  *Accept:* the two query-reachable legacy-IO decode sites route through ChunkSource with no perf
  regression; no scan-throughput multiplier claimed. **Dep:** none. **Dedup:** re-scope existing #2165.

- **M17 (#2561) — #2561 close-with-verify (CLOSE-WITH-VERIFY #2561, P3).** The BTI bug is already fixed on
  main (PR #2554); add a one-shot BIG-path assertion (present-key `get()` across a straddling BIG
  partition asserts `SCAN_FOR_KEY_CALLS` delta == 0), then close. *Accept:* the verify test passes;
  #2561 closes. **Dep:** none — ride the #2565 nit batch. **Dedup:** close, not refile.

---

*Committed under epic #2817 (0.17 scan-path throughput program); manifest items M0–M17 filed as
issues #2818–#2828 (new) + #2765/#1883/#2680/#2043/#2605/#2165/#2561 (extended/closed) — see §7.*
