# Phase 1-1 — Row-engine cost levers (keep the row pipeline, make it faster)

**Date:** 2026-07-21 · **Status:** research (uncommitted) · Agent 1/8 of the throughput program

Anchored entirely to `docs/research/phase0-scan-cost-breakdown-2026-07.md` (the single-stream,
warm, uncompressed, narrow-`keyvalue`, 4-way-merge CPU profile: 154.1 CPU-s total) and deduped
against `docs/research/throughput-backlog-inventory-2026-07.md`. Every multiplier below shows its
Amdahl arithmetic against the Phase-0 stage %s. **Read §0 (framing) before quoting any number.**

---

## 0. Framing — three things that make or break every number here

1. **Amdahl ceiling.** Fixing a stage that is `X` of total CPU buys **at most `1/(1-X)` alone**;
   combined levers multiply on the **residual** (`1/(1-X₁) · 1/(1-X₂')` where `X₂'` is stage 2's
   share of what's *left*). None of these levers touch IO (0%) or transport (0.2%) — both are ~0 in
   this rig and real in the field (Phase-0 §5 caveats 1–3), so a field re-profile can only *shrink*
   these shares.

2. **Per-stream-ceiling vs utilization — they are NOT the same win.** The 154 CPU-s is spread across
   ~45 threads: one CPU-bound **coordinator** (~62 CPU-s of real reconcile/materialize/hash/heap
   work = the single-stream throughput limiter) and ~44 **reader threads** carrying ~72 CPU-s that
   is *almost entirely `send`-park* (blocked, not throughput-limiting).
   - A **utilization lever** removes total CPU so more concurrent streams fit on the box (raises
     aggregate rows/s under load; lifts B2/A2 qps). It may not move a single stream at all.
   - A **per-stream-ceiling lever** shortens the *coordinator's* critical path or the pipeline
     wall-time, raising the ~500 k rows/s single-stream number the brief asks to lift.
   - The channel park/wake (Stage 4b, 49.9%) is *mostly* utilization (producer-side park, off the
     critical path) with a *smaller* per-stream slice (the futex **wake** the coordinator's `recv`
     fires to unpark a producer, once per row, **is** on the critical path). Both are labelled per
     lever below.

3. **Shape bound (Phase-0 §5 caveat 4).** The profiled narrow 2-column, one-row-per-partition
   `keyvalue` shape is the *extreme* that **maximizes** the channel + alloc + hash shares (Stages
   4b/3/7) and **minimizes** decode/materialize/Arrow (Stages 2/3/5). So the channel/alloc levers
   are quoted at their **upper bound**; wide rows (clustering keys, collections) dilute them and
   shift weight to decode/reconcile. Every multiplier below is bounded for **narrow AND wide**.

**Constraints honored by all levers:** B4 peak ≤512 Mi (batching adds only bounded buffers;
inline-merge *reduces* memory), A5 stability, byte-for-byte reconcile parity, no-heuristics (#28)
untouched (no lever infers type/behavior from bytes).

---

## 1. Lever table

| # | Lever | Mechanism | Anchored multiplier (arithmetic) | Kind | Cost | Risk | Issue | Evidence (file:line) |
|---|-------|-----------|----------------------------------|------|------|------|-------|----------------------|
| L1 | **Batch the k-way merge fan-in `sync_channel`** | Accumulate `Vec<MergeEntry>` in `forward_row` and `send` one message per `BATCH_EMIT_ROWS`(256) rows instead of one `send`/row; consumer `next()` drains a locally-held batch, one `recv` per batch. Drops sends/wakes **256×**. | Stage 4b = 49.9%, ~94% of it (≈47% of total) is kernel park/wake removable by batching. **Utilization:** `1/(1-0.47) ≈ 1.9×` aggregate CPU headroom. **Single-stream:** only the coordinator-side wake-per-`recv` is on the critical path (a fraction of 4b) → bounded **~1.15–1.35×**. Wide rows: park/wake share shrinks, so utilization ~1.4–1.6×, single-stream ~1.1×. | Mostly utilization + modest per-stream | **M** | Med (must preserve #2419 egress-depth gauge accounting + #2361 cancel-aware recv; batch becomes the gauge unit) | **NEW** (biggest gap — see §3) | send: `from_readers.rs:137,150`; cap: `mod.rs:537` (`=256`); recv: `mod.rs:1235`; batch-size precedent: `scan_stream_windowed.rs` `BATCH_EMIT_ROWS=256` |
| L2 | **Inline (thread-less) merge for the few-SSTable single-stream case** | When `k` inputs ≤ small threshold and single stream, pull each `RunReader` iterator directly on the coordinator thread — no producer threads, no `sync_channel`. Eliminates **all** of Stage 4b. | Removes 49.9% entirely. **Utilization:** `1/(1-0.499) ≈ 2.0×`. **Single-stream narrow:** removes ~47% critical-path wake but *re-serializes* decode (Stage 2, 9.7%, was overlapped across ~4 threads) → adds back ~2–3×·0.097 ≈ +0.15–0.20 of critical path → net **~1.4–1.8×**. **Single-stream WIDE: NEUTRAL-to-NEGATIVE** (decode is large and its overlap is lost). | Highest single-stream ceiling (narrow only) | **L** | High (reconcile parity heart; shape-fragile; needs an A/B gate to prove it only fires where it wins) | **NEW** | architecture: `from_readers.rs:176` (`open_from_reader` spawns the thread); Phase-0 §4 "bypass the per-input thread/channel entirely for the single-stream / few-SSTable case and merge inline" |
| L3 | **Reconcile singleton/no-overlap fast-path** | In `reconcile_cluster_with_overlap_counted`, when a clustering group has exactly one `Live` entry with no row/complex/range deletion and no gc/TTL work pending, emit it directly, skipping `ReconcileState` construction + per-`(column,cell_path)` winner resolution. | Stage 4a = 32.5%. Fast-path skips most per-cluster machinery for **singleton clusters** (every cluster in the disjoint-key profile). Cutting 4a to ~15% saves ~17.5 pp → `1/(1-0.175) ≈ 1.20×` single-stream. **Overlapping/wide field data: far less** (real reconciliation collisions keep the full path) — bound ~1.03–1.08×. | Per-stream ceiling (the true limiter once L1/L2 land) | **M–L** | Med-High (must be byte-identical to full reconcile; parity oracle load-bearing) | **NEW** | `mod.rs:4117` (`reconcile_cluster_with_overlap_counted`), `mod.rs:4156–4180` (ReconcileState steps); heap refill `mod.rs:2941` |
| L4 | **`RowKey` Arc-per-partition hoist** | Build one `RowKey(Arc<[u8]>)` per partition (outside `for entry in rows`) and clone the `Arc` per row, instead of `RowKey::new(partition_key.to_vec())` (fresh alloc+memcpy) every row. | Part of malloc 17.6% + Stage 3 (4.5%). **WIDE rows** (many rows/partition): removes ~1 alloc+copy/row → bound `1/(1-0.05..0.08) ≈ 1.05–1.09×`. **NARROW (1 row/partition): NEUTRAL** — one Arc/partition == one Arc/row, only the `.to_vec()` copy saved. | Utilization + wide per-stream | **S–M** | Low | under **#1883** (0.17 per-row alloc budget — this is a concrete fix; #1883 is the ratchet) | alloc site: `producer.rs:1000` (`RowKey::new(partition_key.to_vec())`); loop: `producer.rs:812,817` (`key.key` constant across `rows`); `RowKey(Arc<[u8]>)` at `types.rs` |
| L5 | **FxHash the per-row `row_values` map** | Swap the default-`RandomState` (SipHash) `HashMap<Arc<str>,Value>` built once per row to `rustc_hash::FxHashMap`. Keys are **internal** interned column names — no HashDoS surface. | SipHash ≈ 4.5% of CPU. FxHash removes ~0.038 → `1/(1-0.038) ≈ 1.04×`. Nearly free; same on narrow & wide (per-row map is per-row regardless). | Utilization | **S** | Low (FxHash already adopted in read path #1590/#1817 — no revert) | **NEW** (trivial; fold under E-hygiene / #1883) | `row_build.rs:246` (`HashMap::with_capacity`, default hasher); precedent `#1817` FxHashMap row map, `#1590` E8 FxHash |

*Note:* Phase-0 attributes SipHash to "`PartitionKeyCache`", but that struct is a **single-entry
decode cache** (`row_build.rs:125`, no hashing). The real per-row SipHash is the `row_values`
`HashMap` at `row_build.rs:246`. L5 targets the actual site. (For a 2-column row a `SmallVec`
beats any HashMap; FxHash is the S-cost version, a Vec swap is the M-cost follow-on.)

---

## 2. Combined-lever arithmetic (multiply on the residual)

Cheap bundle then headline then limiter, aggregate-CPU / utilization view:

```
L5 (FxHash)          1.04×
L1 (batch channel)   1.89×   (park/wake removal, utilization)
L3 (reconcile fast)  1.20×   (residual, disjoint/narrow)
                     ------
aggregate util ≈ 1.04 × 1.89 × 1.20 ≈ 2.36×   (NARROW, disjoint — upper bound)
```

Single-stream (~500 k rows/s) view — only per-stream-ceiling slices count:

```
L5  1.04×  ·  L1 ~1.25×  ·  L3 ~1.20×  ≈ 1.56×  → ~780 k rows/s   (NARROW)
   + L2 instead of L1 (narrow, few-SSTable): L1's ~1.25 → L2's ~1.6 → ~1.9× total → ~950 k rows/s
```

**Wide-row single-stream** collapses toward ~1.05–1.15× total (L1 park/wake share shrinks, L3
singletons vanish, L4 replaces L5 as the live lever). This is expected and correct: the narrow shape
is where the row engine's *coordination/alloc tax* is worst, so it is where these levers pay most.

---

## 3. Existing-issue → Phase-0-stage map (what buys which slice)

| Phase-0 stage | % CPU | Existing groomed issue(s) | Verdict |
|---|---|---|---|
| **4b — fan-in channel park/wake** | **49.9%** | **NONE.** #2765 (adaptive egress budget) and #2600 (shipped) bound the **OUTER** merge→tonic `tokio::mpsc` egress depth (stability/B4/A5) — a *different channel*. F2 #1592/PR #2100 batched the **single-generation public `scan_stream`** forwarder — also a different channel. Neither touches the **inner per-SSTable→coordinator `sync_channel`** Phase-0 indicts. | **NEW filing required (L1/L2).** The single largest cost has zero existing coverage. Do NOT credit #2765 with this multiplier — it is orthogonal (depth cap, not per-row CPU). |
| **4a — reconcile/heap compute** | **32.5%** | NONE targets a reconcile fast-path. Epic Q #1610 (write/compaction alloc discipline) is adjacent but unstarted/off-target. #2213 (Murmur3 recomputed per heap comparison) is a real *small* 4a sub-cut, already filed. | **NEW filing (L3).** #2213 is a legit minor complement (heap comparator). |
| **3 + malloc — materialize/alloc** | 4.5% + 17.6% | **#1883** (0.17, per-row alloc budget/ratchet) — the *measurement*; L4 (`RowKey` hoist) is the concrete fix under it. #1817 (partition-key decode hoist) is precedent. | **#1883 keeps** (relevant). L4 is a new concrete fix beneath it. |
| **2 — parse/decode** | 9.7% | Epics I–M (#1602–#1606) **all CLOSED** — already optimized. #2165 (route sequential_scan decode through ChunkSource) is *consolidation*, not a speedup. | Stage largely mined out. See demotions. |
| **5 — Arrow encode** | 1.0% | AE-series #1497/#1498/#1500 (delta emit / Node parquet). | Buys ≤ `1/(1-0.01)=1.01×` on THIS shape. Real for wide/collection & export paths, **not scan throughput on narrow rows.** |
| **1 / 6 — IO / transport** | 0% / 0.2% | — | Invisible in-rig; field-real (Phase-0 §5). Out of this agent's row-engine scope. |

### Issues Phase-0 proves near-worthless *as single-stream throughput levers* (say it bluntly)

- **#2177** (stale `range()` comment nit) — a **comment fix**, 0% CPU. Not a perf lever.
- **#1655** (M2 campsite file-split tracking) — hygiene, 0% CPU.
- **#2165** (route `iterate_all_partitions`/`sequential_scan` through ChunkSource) — Stage 2 is only
  9.7% *and* this is decode-plane *consolidation*, not an optimization; expected perf-neutral.
  Worthless as a throughput lever (keep only for maintainability).
- **#2349** (UDT registry into flight reads, CLOSED) — a **correctness/decode** fix; the narrow
  `keyvalue` shape has no UDTs, so zero throughput bearing. Not a lever.
- **AE Arrow-encode series (#1497/#1498/#1500)** — Stage 5 = 1.0%; ≤1.01× on narrow scan. Demoted
  for *this* program (they remain valid for the export/wide-collection path they were filed under).
- **#2765** (adaptive egress budget) — **not demoted, but re-scoped:** it is a **stability/B4/A5**
  lever on the outer egress channel, NOT the Stage-4b per-row CPU lever. It must not be conflated
  with L1.

### Phase-0 levers with NO existing issue → new filings

1. **L1 — batch the k-way merge fan-in `sync_channel`** (headline; ~1.9× utilization). NEW.
2. **L2 — inline/thread-less merge for few-SSTable single-stream** (highest narrow single-stream
   ceiling; shape-fragile). NEW. Can be an A/B experiment gated on `k` and stream count.
3. **L3 — reconcile singleton/no-overlap fast-path** (true per-stream limiter once L1/L2 land). NEW.
4. **L5 — FxHash the per-row `row_values` map** (freebie). NEW/trivial (fold under E-hygiene/#1883).
   (**L4** already has a home under **#1883**.)

---

## 4. Prior-attempt check (don't re-propose reverted work)

- **F2 #1592 / PR #2100** "batch the public streaming channel (stop one async wake per row)":
  batched the **single-generation `scan_stream`** forwarder (the outer `Vec<Row>` surface into the
  query-engine consumers), reusing `BATCH_EMIT_ROWS`. **Did NOT touch the inner k-way-merge fan-in
  `sync_channel`** (that path re-batches via a per-run `RunReader` peek-buffer `refill_buffer`, but
  the channel itself is still one message/row — `from_readers.rs:150`). **Not merged, not reverted —
  simply a different channel.** L1 is the un-taken sibling and F2 is the *precedent* (technique,
  `BATCH_EMIT_ROWS` constant, parity/send-count oracle test pattern in
  `issue_1592_stream_channel_batch.rs`) — reuse it.
- **#1143 / PR #1265** "batched row emission" (windowed-scan p99) — another batching precedent, also
  a different (windowed-scan) path. Not reverted.
- **#1817 / PR #2221** "partition-key hoist + internal FxHashMap row map" and **#1590 / PR #1877**
  "E8 FxHash bundle" — FxHash **already adopted** in the read path and **not reverted**. L5 extends
  the same accepted pattern to the still-default-hashed `row_values` map. No re-litigation risk.
- **#1668 / PR #2304** within-partition streaming merge (Q5) — established `streaming.rs`
  `finalize_current_cluster`; L3 must slot its fast-path *inside* that reconcile call to stay parity-safe.
- No prior attempt at an inline/thread-less merge bypass (L2) or a reconcile singleton fast-path (L3)
  was found in `git log`.

---

## 5. Recommended sequence (multiplier-per-effort)

1. **L5 FxHash `row_values`** — S, low-risk, ~1.04×, accepted precedent. Bundle as the warm-up.
2. **L4 `RowKey` Arc hoist** — S–M, low-risk; wide-row alloc win, neutral on narrow. Land under #1883.
   *(1+2 are a cheap "free multiplier" bundle, ~1 PR.)*
3. **L1 batch the fan-in `sync_channel`** — M, the headline. Best multiplier-per-effort: ~1.9×
   utilization + ~1.15–1.35× single-stream, reusing the F2 batch machinery & oracle pattern. **NEW.**
4. **L3 reconcile singleton fast-path** — M–L; the true per-stream ceiling lever once L1 exposes the
   coordinator as the limiter. Parity-oracle-gated. **NEW.**
5. **L2 inline-merge bypass** — L, highest narrow single-stream ceiling (~1.6–1.8×) but shape-fragile
   (loses decode overlap → wide-neutral/negative). Ship **only** behind an A/B gate on `k`+stream
   count; it is the experiment, not the default. **NEW.**

**One-line for the program:** the single biggest single-stream lever the backlog does not cover is
**L1 — batching the inner k-way-merge fan-in channel** (Stage 4b, 49.9%); it needs a new filing,
reuses F2's proven batch machinery, and #2765 is NOT it (that's the outer egress-depth stability
cap). L5+L4 are near-free companions; L3 then L2 are the successive single-stream-ceiling pushes,
both parity-risky and both un-filed.
