# Phase 1 — Dimension 6: Parallelism & Scheduling

**Date:** 2026-07-21 · **Status:** research (uncommitted) · **Agent:** Phase-1 6/8 (parallelism & scheduling)
**Inputs:** `docs/research/phase0-scan-cost-breakdown-2026-07.md`, `docs/research/throughput-backlog-inventory-2026-07.md`

---

## 0. The hard discipline for this dimension (read first)

Every lever in this document is a **UTILIZATION multiplier**: it spreads work over idle cores or
idle pods. **None of them raises the per-stream ceiling** — the rate at which one `do_get` coordinator
turns SSTable bytes into Arrow rows. That ceiling is owned by Phase-0's #1/#2/#3 findings (per-row
channel handoff, k-way reconcile compute, per-row alloc/hash) and by the other Phase-1 dimensions.
If the per-stream coordinator does *X* rows/s, four parallel streams do **at most** `4·X` and in
practice **less**, because Phase-0 measured that a single stream already burns **~55 % of its CPU in
kernel `sync_channel` park/wake** — and that syscall/context-switch tax *contends* when you stack
streams onto the same cores.

**Honest per-pod formula (used throughout):**

```
per_pod_rows_s  =  min( N_admitted , N_drain_sat , N_mem )  ×  per_stream_rows_s  ×  C(N)
                   \_______________  utilization width  ______________/           \_ contention _/

  N_admitted     = admission ceiling (--max-concurrent-scans, default 64)
  N_drain_sat    = concurrent streams past which the drain-bound consumer stops
                   scaling  (measured ≈ 8 on the local rig, see §4)
  N_mem          = concurrent streams that fit the pod memory budget (B4 → §6)
  per_stream_rows_s = the per-stream ceiling — NOT movable by anything here
  C(N)  ≤ 1      = contention factor, FALLS as N grows (kernel park/wake tax, Phase-0 §3a)
```

The width term `min(...)` is what parallelism levers move. The two right-hand terms are the honest
tax that stops `4 vCPU → 4× throughput` from being true. On a 4-vCPU pod the effective width is **not
4** — it is whichever of the three ceilings bites first, and on today's code the **drain-bound
saturation (~8 concurrent streams) and the single-threaded merge coordinator** dominate, not core
count.

---

## 1. Lever table (every lever labeled UTILIZATION)

| # | Lever | Layer | Class | What it buys (honest per-pod math) | Cost / hazard | Status |
|---|-------|-------|-------|-----|------|--------|
| L1 | **#2680 re-land — K-way token sub-splitting + `SplitWeight`** | Trino connector (plan-time) | UTILIZATION (pod-skew fix) | Field: busiest pod does 2–4× the CPU of the median (count-balanced floorMod). Weight-balancing flattens the skew so aggregate qps rises off the ~39 qps floor toward the pod-count limit. **Multiplier ≈ (skew ratio) → up to ~2–4× on the *lagging* pods, i.e. it recovers stranded utilization; it does NOT raise any single pod's ceiling.** | Caused P0 #2782 (LIMIT hang) via `K=4` default + Trino early-termination not draining sub-split DoGet streams. See §3. | Reverted (`0bd63148b`); re-land is the headline 0.17 lever |
| L2 | **Lower default `sub-splits-per-range` (K=1 or 2), let `SplitWeight` carry balance** | Trino connector | UTILIZATION | Same skew fix as L1 with far less split inflation. At ~48 ring ranges, K=2 → ~96 splits (~32/pod) already fills a 32-thread Trino worker; K=4 → ~192 is past the sweet spot (§5) and is what armed #2782. | None beyond L1; strictly safer | Part of L1 re-land spec |
| L3 | **Admission ceiling `--max-concurrent-scans` (currently 64)** | Flight server | UTILIZATION *cap* (also a stability guard) | Sets `N_admitted`. Today sized from blocking-pool/fd ceilings, **not memory or drain**. At 64 it is far above both the drain-saturation width (~8) and the memory-safe width (§6) → it does not add throughput, only admits streams that pile into buffers. | **Too high for a 512Mi/4-vCPU pod** — see §6, A5 restart risk | Shipped #2420; **re-sizing is a lever** |
| L4 | **#2765 process-global adaptive egress budget** (`cap_per_merge = clamp(BUDGET/active_merges, MIN, 256)`) | Flight/core merge | UTILIZATION-enabling (stability) | Bounds the **merge fan-in** working set (`active_merges × K × per_source`, where `per_source` is `STREAMING_CHANNEL_CAPACITY` here and `4 ×` that since the #2820 batched egress) to a fixed budget instead of growing with concurrency. Lets you raise concurrency without the depth-8080 blowup. Measured: CAP=32 cut egress depth 5–8× at <10 % qps cost. | Bounds only the fan-in channel, **not** the Arrow egress channel (the memory-dominant one — §6). | #2600 shipped (characterization); **#2765 impl open, unmilestoned** |
| L5 | **Intra-query parallel merge (range-shard one `do_get` over R coordinators)** | Flight server | UTILIZATION | Would let ONE stream use >1 core by running R independent coordinators over R token sub-ranges of the same M SSTables. **But this is L1 done inside the server** — same effect, more machinery. | New intra-server merge-sharding + concat + cancellation; duplicates Trino's scheduler. **Strictly more expensive than L1 for the same utilization** (§2). | Not filed — recommend NOT pursuing; prefer L1 |
| L6 | **Batch rows per fan-in channel message** (Phase-0 #1 mitigation) | core merge | *per-stream* (NOT utilization) | Cuts the per-row `send` count → attacks the 55 % kernel tax → raises `C(N)` and `per_stream_rows_s`. Listed here because it is the thing that makes L1/L3 *scale better*, but it is a **per-stream ceiling lever**, owned by another dimension. | Design work in the merge fan-in | Phase-0 finding #1; not this dimension's to spec |

**Reading the table:** L1/L2/L3 are the parallelism/scheduling levers proper. L4 is a stability
enabler that *unlocks* higher useful concurrency. L5 is the tempting-but-wrong intra-server variant of
L1. L6 is flagged only because the honest math above shows that **the contention term `C(N)` — a
per-stream property — is what caps how much any utilization lever can deliver**; without L6-class work
the per-pod curve flattens early (§4).

---

## 2. Intra-query parallelism vs more `do_get`s per pod — which is cheaper?

**Where parallelism already exists.** Per `do_get`, the warm path spawns **one reader thread per
input SSTable** (`from_readers.rs::open_from_reader` → `producer_thread_from_reader`), each decoding
its SSTable and pushing rows over a bounded `sync_channel(256)` to **one** k-way merge coordinator
(`KWayMerger::step` / `reconcile_cluster_with_overlap_counted`). So decode is already parallel across
SSTables; **the merge/reconcile coordinator is single-threaded and is the per-stream throughput
limiter** (Phase-0 §3b stage 4a = 32.5 % of CPU, all on the one coordinator thread).

**Option A — intra-query parallel merge (L5).** Shard the `do_get`'s token range into R sub-ranges,
run R coordinators (each merging the same M readers, scoped by `ScanTokenBound` — the machinery
already exists via #2413's `token_bound` on `new_from_readers`), concatenate. This makes one stream
use R cores.

**Option B — more `do_get`s per pod (L1/#2680).** Trino already emits one split per token range and
schedules them concurrently; sub-splitting a range into K slices emits K `do_get`s that the Trino
scheduler + the Flight admission semaphore already spread across the pod's cores. Each sub-split is a
token-bounded Summary-guided walk (#2412/#2413) — the *same* per-coordinator work Option A's shards
would do.

**Verdict: Option B (more `do_get`s via sub-splits) is cheaper for the same utilization.** Both
options spin up N independent single-threaded coordinators over token sub-ranges; the only difference
is *who schedules them*. Option B reuses three things already in production — Trino's split scheduler,
the #2420 admission semaphore, and the #2413 token pushdown — and needs **zero new server code**.
Option A rebuilds a scheduler inside the server, adds cross-shard cancellation and output
concatenation, and still hits the same `C(N)` contention tax. **#2680 *is* intra-pod parallelism,
expressed at the split layer.** Recommend not filing L5.

**What limits concurrent streams per pod** (the `min(...)` width): (1) **admission** `N_admitted`=64
(L3); (2) **drain saturation** `N_drain_sat`≈8 — the consumer (Arrow-encode + gRPC-write +
coordinator) is drain-bound, so past ~8 streams throughput is flat and only latency/buffers grow
(§4, and #2765's own baseline table); (3) **memory** `N_mem` (§6). Today `N_drain_sat` bites first for
throughput and `N_mem` bites first for stability — **admission at 64 is above both**, which is the
core mis-sizing.

---

## 3. #2680 re-land spec sketch + acceptance criteria

### 3.1 What shipped and was reverted

Reverted commit `f5dd215a7` (PR #2779), backed out by `0bd63148b`/`7fa3f2050`. It added:
- `TokenRangeSlicer` — overflow-safe BigInteger ring arithmetic expanding `(start,end]` into K equal
  token-span slices (wraparound-safe, exact coverage, never an empty `(x,x]` slice, K=1 = identity).
- `cqlite.sub-splits-per-range` config — **default 4**, min 1, max 64.
- `getSplitWeight()` on `CqliteFlightSplit` (proportional to slice token span, mean-span = `standard()`,
  clamped to Trino's valid proportion range) and `CqliteFlightAggregateSplit` (clamped sum over slices).
- Slicing at the single `sliceRanges` seam before every consumer: scan `buildSplits`, the snapshot host
  chooser (`distinctReplicaHosts`), and #2679 pruning. Slice *i*'s primary = `rotated(parent)[i % n]` so
  per-owner span converges to ~1/N; full owner set retained per slice (#2241); snapshot chooser covers
  every slice primary (#2227). Aggregate path exempted from sub-splitting.

### 3.2 The #2782 hazard class (what the re-land MUST fix)

**Symptom:** with `K=4`, `SELECT ... LIMIT 2` (and partial-predicate + LIMIT) **hang 180 s** through
Trino; `LIMIT 100` (> 5-row table, drains every split) and unbounded scans **pass**.

**Root-cause class — early-termination stream drain, not ring math:** Trino satisfies a small `LIMIT`
from the first split(s) and then **cancels / never schedules the remaining sub-splits**. K=4 quadruples
the in-flight split count, so far more sub-split `DoGet` streams exist that Trino closes early. The
hang points at a **sub-split's Flight `DoGet` stream not being closed/drained on early close** — the
server-side producer thread (`producer_thread_from_reader`) blocks in `send` on a full `sync_channel`
forever if the consumer disappears without the cancel propagating. (CQLite has cooperative cancel via
`ScanCancel` polled before each `step` — #1473/#2361 — so the fix is ensuring Trino's early close
*fires* that cancel on every not-yet-drained sub-split, and that a blocked producer observes it.)

This is a **utilization-lever safety bug**: sub-splitting multiplies the number of cancellable streams,
so any latent early-close/drain gap that a single split per range hid becomes a hang.

### 3.3 Re-land design sketch

1. **Default `K=1`, not 4.** Ship the mechanism inert-by-default (K=1 = byte-identical pre-#2680
   behavior, already proven). Let `getSplitWeight()` + a **conservative default K=2** (opt-in bump)
   carry the balance. The field skew fix does not need K=4; §5 shows K=4 overshoots the split-count
   sweet spot and is exactly what armed #2782.
2. **Fix the early-close drain** as a hard prerequisite, gated by the `Flight ↔ Trino E2E` lane as the
   oracle (#2792 makes that lane `required` for `trino-connector/**`). The cancel must reach every
   sub-split's producer thread; a producer blocked in `send` on a full fan-in channel must wake and
   exit on cancel (verify the `SyncSender::send` blocked path is cancel-observing, or bound the block).
3. **Keep `SplitWeight` as the balancing mechanism**, K as the *granularity* knob. Balance should come
   from weight-proportional scheduling, not from raw split multiplication.
4. **Compose with #2679 pruning safely:** a fully-bound point read must prune to the **single covering
   sub-slice → 1 DoGet** (the reverted commit's "K=4 pruning to one narrower slice" test), never
   re-fan-out. Preserve this — sub-splitting must not re-inflate what #2679 collapsed.

### 3.4 Acceptance criteria — proving the hang is dead

- [ ] **LIMIT-under-sub-splits, the direct #2782 oracle:** with `K≥2`, `SELECT id FROM t LIMIT 2`
      (small LIMIT, forcing Trino early termination while sub-splits remain in flight) returns exactly
      2 rows in **< 5 s**, not a 180 s hang — asserted in the `Flight ↔ Trino E2E` lane. Repeat for the
      partial-predicate + LIMIT shape (`WHERE score>15 AND ... LIMIT 2`).
- [ ] **Early-close drain unit test (server-side):** a `do_get` whose consumer drops mid-stream while
      the fan-in `sync_channel` is full must have every producer thread observe cancel and exit within
      a bounded time (no thread left blocked in `send`) — a `producer_thread_from_reader`-level test,
      since that is the thread that hangs.
- [ ] **`Flight ↔ Trino E2E` is a `required` check** for `trino-connector/**` and Flight changes
      (#2792) — the process gap that let #2779 auto-merge red must be closed before re-land.
- [ ] **Weight balance (carried over from #2680):** per-owner assigned weight (Σ partitions or Σ bytes)
      within ≤1.25× of the mean under an RF==N fixture with 8×-unequal-span ranges; `getSplitWeight()`
      span-proportional + clamped; determinism preserved (`selectionIsDeterministicAcrossInvocations`);
      full owner set retained (#2241); snapshot chooser covers every slice primary (#2227).
- [ ] **#2679 compose:** a fully-bound PK point read emits exactly **1 DoGet** at any K (prune to the
      single covering sub-slice), asserted on the public split-count / do_get counter surface.
- [ ] **K=1 identity:** ranges/hosts/owner-sets/count byte-identical to pre-#2680 at the default.
- [ ] **Field (report-only, next round):** busiest pod CPU ≤ ~1.3× median at 32 threads; aggregate qps
      ceiling rises off ~39.

---

## 4. Utilization saturates early — the drain-bound ceiling (`N_drain_sat`)

The single most important honest number for this dimension comes from #2765's measured baseline
(server-direct `flight-loadgen` vs `cqlite-flight`, `--max-concurrent-scans 128`, full scan; Apple
Silicon, trends machine-independent):

| threads | peak egress depth | qps | p50 ms |
|--------:|------------------:|----:|-------:|
| 1  | 3    | 38  | 26  |
| 8  | 1473 | 195 | 36  |
| 32 | 5397 | 187 | 178 |
| 80 | 8080 | 190 | 417 |

**Throughput saturates at ~8 concurrent streams (195 qps) and is flat to 80 (190 qps).** Beyond ~8,
extra concurrency buys only latency (36→417 ms) and buffer depth (1473→8080). The consumer — the
single-threaded coordinator + Arrow-encode + gRPC-write — is **drain-bound**, exactly Phase-0's
finding that ~55 % of a stream's CPU is kernel park/wake and the coordinator is the limiter. This is
the empirical value of `N_drain_sat ≈ 8` and the reason `C(N)` collapses past it.

**Consequence for the per-pod math:** on this rig one pod's useful width is ~8 streams, *not* the 64
admission slots and *not* the 4 vCPUs× anything. A 4-vCPU field pod will have a *lower* `N_drain_sat`
(fewer cores to absorb the park/wake tax) — plausibly ~4–6. **Adding splits/streams past that point is
pure loss** (latency + memory), which is *also* the mechanism behind #2782 (more early-cancelled
sub-splits) and the depth-8080 saturation (#2600/#2765). The lever that raises `N_drain_sat` is L6
(cut the per-row channel tax) — a **per-stream** lever, not a scheduling one.

---

## 5. Split sizing for a 4-vCPU pod / 32-thread Trino

**Today's split count.** One split per read-replica token range. Standing rig = 3 nodes × `num_tokens`
16 → ~48 ranges (RF=3 read-replica intervals). A full scan = ~48 splits, ~16 per pod across 3 pods. A
point read *without* #2679 = still ~48 DoGets to fetch 1 partition (the #2679 motivation).

**Per-split fixed cost is now LOW — this is why sub-splitting is finally viable.** The three big
per-split fixed costs were retired in 0.15:
- **Snapshot open** — amortized by #2356 snapshot reuse per `(keyspace,table)` (~0 after the first split
  of a query).
- **Index open** — #2412 lazy Summary-guided BIG index: O(summary) open, ~0 resident, no full Index.db
  slurp.
- **Full-ring body walk** — #2413 pushes the split's token range into the per-SSTable walk, so a split
  reads only in-range partition bodies (not from ring start).

So a split is now ≈ a token-bounded Summary-guided walk with negligible open cost. **That is the
precondition that makes K-way sub-splitting cheap** (the #941 council's "one split per vnode is
pathological until token seeks exist" caveat is now largely resolved by #2413).

**Optimal split count.** Two competing pressures:
- **Floor:** enough splits to (a) fill Trino's 32 worker threads and (b) give weight-balancing enough
  granularity to flatten the 2–4× pod skew — needs *several×* the pod count, so ≥ ~4× pods.
- **Ceiling:** past `N_drain_sat` (~8/pod, §4) more concurrent splits per pod add no throughput, and
  each extra split adds an early-cancellable DoGet stream (#2782 blast radius) + a fan-in channel
  working set.

For 3 pods × 4 vCPU and a 32-thread Trino: target **total splits ≈ 4–8× the pod count ≈ 48–96**. The
~48-range ring already sits at the low end; **K=2 → ~96 splits (~32/pod)** fills the Trino worker and
gives balancing granularity without overshoot. **K=4 → ~192 (~64/pod)** is well past `N_drain_sat` and
past the split-count sweet spot — it bought skew-balancing granularity at the cost of 4× the
early-termination surface, which is the #2782 trade that blew up. **Recommend default K=1, opt-in K=2,
never K=4 as a default.**

**#2679 interaction.** Split pruning collapses a fully-bound point read from ~48 DoGets to 1. Sub-
splitting must prune to the single covering sub-slice (1 DoGet) — the two compose, but only if the
re-land preserves that path (§3.4). For scans, #2679 does nothing (no bound PK) and sub-splitting is
the relevant lever.

---

## 6. Memory / stability coupling — the B4-implied concurrency ceiling

### 6.1 Per-stream buffered working set (from code)

Two distinct bounded buffers per `do_get` (do not conflate — #2765 touches only the first):

| Buffer | Constant | Rows resident / stream | Source |
|--------|----------|-----:|--------|
| **Merge fan-in** (per input SSTable) | `STREAMING_CHANNEL_CAPACITY = 256` × M SSTables | 256·M `MergeEntry` | `merge/mod.rs:537`, `from_readers.rs:186` |
| **Arrow egress** (do_get → gRPC) | `(DO_GET_CHANNEL_CAPACITY 4 + IN_FLIGHT 3) × batch_size 8192` | **57,344 Arrow rows** | `streaming.rs:65/86`, batch 8192 |
| Coordinator build buffer | 1 × `batch_size` | 8,192 rows | producer build |

The **Arrow egress buffer dominates**: 57,344 rows/stream vs 256·M (≈2,048 at M=8) fan-in +8,192
build. **#2765's adaptive budget bounds the fan-in term only** (the 256·M side, which is the
concurrency-*growing* one and the depth-8080 signal) — it does **not** bound the 57,344-row Arrow
egress buffer, which is per-stream fixed and is the memory-dominant term. **Flag:** to bound pod
memory under concurrency you also need either a memory-derived admission ceiling (below) or an
adaptive `DO_GET_CHANNEL_CAPACITY`/`batch_size`.

### 6.2 Bytes and the ceiling

B4 = ≤3 s / **≤512Mi pod** / ≤16Mi (per-query working set target). Per-stream Arrow-egress bytes at a
field-representative row width:

- Narrow (`keyvalue`, ~300 B/Arrow-row, Phase-0's 150 MB/s ÷ 500k rows/s): 57,344 × 300 B ≈ **17 MB/stream**.
- Moderate (~256 B/row): ≈ **14.7 MB/stream**.
- Add transient encode doubling + reader mmaps/decompress buffers/snapshot handles: budget **~20–30
  MB/stream** all-in.

**`N_mem` = 512Mi / per-stream-MB:**

| per-stream all-in | `N_mem` (streams in 512Mi) |
|------:|------:|
| 15 MB | ~34 |
| 20 MB | ~26 |
| 30 MB | ~17 |

**B4-implied concurrency ceiling: ~14–34 concurrent streams per 512Mi pod, ~20 as a working
midpoint**, with the `batch_size × channel-depth` Arrow-egress product as the dominant term (a single
stream already consumes ~15 MB, essentially the whole ≤16Mi per-query target on its own — the ≤16Mi
number is only satisfiable at concurrency 1 or with a smaller `batch_size`/channel depth).

### 6.3 The A5 (0-restart under 80-thread overload) finding

**Admission default 64 is memory-unsafe for a 512Mi/4-vCPU pod.** It was "sized from blocking-pool
(~256) / fd (~1024÷SSTables) ceilings, **not** core count" — and **not memory**. At the memory
midpoint, 64 admitted × ~20 MB ≈ **1.28 GB ≫ 512Mi → OOMKill**, exactly the A5 failure mode when 80
threads pile onto a pod. Three coupled conclusions:

1. **`N_drain_sat` (~8) < `N_mem` (~20) < `N_admitted` (64).** Throughput is capped by drain at ~8;
   memory is safe up to ~20; admission lets in 64. The **64 ceiling protects neither throughput nor
   memory** — it admits streams that only deepen buffers and threaten OOM.
2. **Re-size admission from memory:** on a 512Mi pod cap `--max-concurrent-scans ≈ 512Mi / per-stream-MB
   ≈ 16–24` (round to ~16 for headroom). This keeps A5 at 0 restarts by construction and costs no
   throughput (it is still ≥ `N_drain_sat`).
3. **#2765 is necessary but not sufficient for A5:** it bounds the fan-in growth (256·M·active_merges),
   removing the depth-8080 blowup, but the per-stream 57k-row Arrow egress buffer × concurrency is the
   larger memory term and is bounded only by admission. **A5 safety = memory-derived admission (L3) +
   #2765 (L4) together**, not either alone.

---

## 7. Summary packet

**Lever set (all UTILIZATION, none raise the per-stream ceiling):**
- **L1 #2680 re-land** — weight-balanced sub-splits; recovers 2–4× stranded utilization on lagging
  pods; **default K=1/opt-in K=2, never K=4**; gated on the early-close drain fix + `Flight↔Trino E2E`
  as `required` (#2792).
- **L3 admission re-sizing** — the current 64 is above both the useful (~8) and the memory-safe (~16–24)
  width; re-derive from pod memory.
- **L4 #2765 adaptive egress budget** — bounds fan-in growth; stability enabler; open/unmilestoned.
- **L5 intra-query parallel merge — DO NOT pursue**: it is L1 rebuilt inside the server at higher cost.
- **L6 per-row channel batching** — the only thing that raises the contention factor `C(N)`, but it is a
  *per-stream* lever, out of this dimension.

**Honest per-pod math:** `per_pod = min(N_admitted 64, N_drain_sat ≈8, N_mem ≈20) × per_stream × C(N)`.
On today's code the width is **~8 (drain-bound)**, *not* 4 vCPU × anything and *not* 64 admission slots.
4 streams ≠ 4× because Phase-0's 55 % kernel park/wake tax contends (`C(N)<1`, falling). Utilization
levers move the `min(...)`; they cannot move `per_stream_rows_s`.

**#2680 re-land acceptance (hang-is-dead):** small-`LIMIT`-under-sub-splits returns in <5 s (not 180 s)
in a now-`required` E2E lane; server-side early-close drain unit test (no producer left blocked in
`send`); weight ≤1.25× mean; #2679 point read = 1 DoGet at any K; K=1 identity.

**B4-implied concurrency ceiling:** **~14–34 concurrent streams per 512Mi pod (~20 midpoint)**,
Arrow-egress `batch_size×channel-depth` = 57,344 rows ≈ 15–20 MB/stream is the dominant term. A single
stream ≈ the whole ≤16Mi per-query target. **Admission at 64 → ~1.3 GB at ~20 MB/stream ≫ 512Mi → A5
OOM risk; re-size admission to ~16–24 from memory. #2765 bounds only the fan-in term, so A5 safety
needs memory-derived admission AND #2765.**

**File:** `docs/research/phase1-6-parallelism.md` (uncommitted per instructions).
