# Phase 2 — Adversarial verification of the row-engine levers (L1–L5)

**Date:** 2026-07-21 · **Status:** research (uncommitted) · **Role:** Phase-2 adversarial verifier
**Target:** `docs/research/phase1-1-row-engine.md`
**Cross-checks:** `docs/research/phase0-scan-cost-breakdown-2026-07.md`,
`docs/research/phase1-6-parallelism.md`, `docs/research/phase1-8-prior-art.md`
**Method:** read the cited source at each line, ran `git log` for prior/reverted attempts, default
skeptical — a lever survives only if the best attack fails. READ-ONLY (no builds/commits).

---

## 0. Bottom line (verdicts first)

| Lever | Verdict | Revised multiplier | The attack that landed |
|---|---|---|---|
| **L1** batch fan-in `sync_channel` | **SURVIVES (weakened)** | util **1.5–1.9× rig-narrow upper bound**; single-stream **~1.1–1.25× narrow / ~1.05–1.1× wide** (was 1.15–1.35×) | P1.1's "#2765 is a different channel, orthogonal" is **factually wrong** — same channel, must co-design; single-stream slice is unmeasured and over-quoted |
| **L2** inline/thread-less merge | **SURVIVES (narrow-only, A/B-gated)** | ~1.4–1.8× narrow single-stream (unmeasured); **≤1.0× wide/field → do not credit in the field stack** | decode-overlap-loss logic holds; field-shape kills it, as the doc concedes |
| **L3** reconcile singleton fast-path | **WEAKENED** | disjoint-narrow-no-TTL **~1.20× upper bound**; **field-with-TTL/overlap ~1.03–1.08× or lower** | precondition (singleton + no del + no TTL + no dropped-cols) is narrow; checking it ≈ doing the work; field TTL/overlap rows never hit it |
| **L4** `RowKey` Arc hoist (#1883) | **SURVIVES (re-scoped)** | **1.05–1.09× ONLY for multi-row-partition tables; 1.0× for single-row-partition of ANY width** | doc conflates "wide (bytes)" with "many rows/partition"; the win is governed by clustering fan-out, unknown from the profile |
| **L5** FxHash `row_values` map | **SURVIVES** | ~1.04× narrow & wide | none — target confirmed still SipHash; #1817's "FxHashMap row map" was a *different* map (per-partition LIMIT counter) |

**Double-count ruling and field governing multiplier are in §7 and §8.**

---

## 1. Attack line 1 — do the code claims hold at the cited lines?

All four mechanical claims **verified true**:

- **`from_readers.rs:137` / `:150` — one `send` per row.** `forward_row` (fn opens at :137) calls
  `sender.send(msg)` at :150 exactly once per streamed row, inside the
  `stream_all_partitions_for_query` per-row callback. Confirmed one `MergeEntry` per `send`.
- **`mod.rs:537` — `STREAMING_CHANNEL_CAPACITY = 256`** (`#[cfg(feature = "write-support")] const … = 256`). Confirmed.
- **`mod.rs:1235` — recv site.** The consumer's `recv` is `receiver.recv_timeout(RECV_CANCEL_POLL)`
  in a cancel-polling loop (issue #2361), one recv per entry, with `channel_depth::received()`
  decrementing the #2419 gauge per DATA entry. Confirmed — and note it is a **cancel-aware timed
  recv**, not a plain `recv()`; L1 must preserve that.
- **`producer.rs:1000` — `RowKey::new(partition_key.to_vec())`.** Inside `entry_to_row`, called once
  per `entry in rows` (loop at `producer.rs:812`). `RowKey(pub Arc<[u8]>)` (`types.rs:1687`),
  `RowKey::new(bytes: Vec<u8>)` (`types.rs:1691`). Confirmed: `.to_vec()` (Vec alloc + memcpy) then
  `Arc::<[u8]>::from(vec)` (second alloc + copy) — **two allocations per row**.

**The P1.1 SipHash re-attribution is CORRECT.** Phase-0 §4 #3 charged the 6.9 CPU-s / 4.5 % SipHash to
"per-row partition-key lookups (`PartitionKeyCache`)". I read `PartitionKeyCache::columns_for`
(`row_build.rs:138`): it does **no hashing at all** — a raw-byte compare
(`cached.as_ref() == key_bytes.as_ref()`) plus a fingerprint equality check. There is **no
`hash_one`** anywhere on the row path (the only `hash_one_value` is in `aggregation/group_key_cmp.rs`,
off the scan path). The real per-row SipHash is the `HashMap<Arc<str>, Value>` at **`row_build.rs:246`**
(default `RandomState` hasher, `cells.len() + pk_hint` inserts per row). Phase-0 measured a real
`core::hash::sip` cost; it just named the wrong owning function. P1.1 fixes the attribution correctly.
**L5 targets the actual site.**

---

## 2. Attack line 3 — history: is F2/#1592 a different channel, any reverted precedent?

- **F2 #1592 / PR #2100 (`cb8241e92`)** = *"batch the public streaming channel (stop one async wake
  per row)"*. Verified it batched the **outer async `scan_stream` forwarder** (the `Vec<Row>` surface
  into query-engine consumers) — a **`tokio` async wake**, a different channel from the inner
  `std::sync::mpsc::sync_channel` L1 targets. Not reverted; archived (`0ae161bd4`). **Precedent
  transfers** (technique + `BATCH_EMIT_ROWS` + the send-count/parity oracle pattern), the channel does
  not. P1.1 is right here.
- **No reverted inner-channel batching or hasher swap exists.** `git log --grep` for revert×(batch|
  channel|merge|inline|hash) returns only unrelated reverts (`#844` multicell, `#441/#442` wide-row).
  FxHash was **adopted and kept** — `#1590` (E8, `59e3ec9ec`), `#1817` (`815690fec`). No re-litigation
  risk for L5.
- **`#1817` caveat that matters for L5 (see §6).** Its commit says "internal FxHashMap row map", which
  could read as "the per-row map is already FxHashed → L5 is dead." It is not — that FxHashMap is a
  *different* map. Reconciled in §6.

---

## 3. Attack line 2 / 4 / 6 findings that change the levers

### 3.1 The #2765 "different channel" claim is FALSE — L1 and #2765 are the SAME channel (attack line 2)

This is the single biggest error in P1.1. P1.1 §3 asserts: *"#2765 … bound the OUTER merge→tonic
`tokio::mpsc` egress depth — a different channel … Do NOT credit #2765 with this multiplier — it is
orthogonal."*

**Verified against code — the opposite is true.** There are exactly two channels:

| Channel | Type | Constant | What #2600/#2765 & L1 do |
|---|---|---|---|
| **Fan-in** producer→coordinator | `std::sync::mpsc::sync_channel` | `STREAMING_CHANNEL_CAPACITY = 256` (`mod.rs:537`, `from_readers.rs:186`) | **L1 batches it; #2765 adaptively sizes it** |
| **Outer Arrow egress** coordinator→tonic | `tokio::sync::mpsc::channel` | `DO_GET_CHANNEL_CAPACITY=4` + `IN_FLIGHT_ALLOWANCE=3`, ×8192 = 57,344 rows (`streaming.rs:65/86/299`) | neither touches it |

The #2419 gauge that #2600 characterizes is literally named `cqlite.merge.egress_channel_depth`
(`channel_depth.rs` header: *"live count of merged DATA entries … buffered in the bounded
producer→consumer `sync_channel` (capacity `STREAMING_CHANNEL_CAPACITY` = 256)"*). #2765's
`clamp(BUDGET/active_merges, MIN, **256**)` clamps to that 256 fan-in cap. So **#2765 and L1 attack the
SAME fan-in `sync_channel`** — #2765 on the **memory-depth axis** (adaptive capacity), L1 on the
**per-message syscall axis** (batching). The genuinely-different channel neither touches is the outer
57,344-row Arrow egress. **P1.6 §6.1 has this right; P1.1 §3 has it backwards.**

Consequence: P1.1 reaches the correct conclusion ("don't double-credit #2765 with L1's multiplier") for
the wrong reason. The right statement is: *they share a channel and must be **co-designed**, not that
they are orthogonal.* See the co-design requirement in §3.3.

### 3.2 L1's single-stream slice is over-quoted and unmeasured (attack line 2)

Phase-0 is explicit that Stage 4b's 49.9 % is *mostly producer-side park* (reader threads blocked in
`send`, off the critical path) with only a *smaller coordinator-side wake-per-`recv`* on the critical
path. P1.1 turns that into single-stream **1.15–1.35×**. But the coordinator is CPU-bound on ~62 CPU-s
of real reconcile/materialize/hash/heap work; the per-row `recv_timeout` syscall is *on top of* that
real work, not 15–25 % of it in any measured sense. The 1.15–1.35× is an **estimate with no
measurement behind it**, sitting at/above the plausible band. **Revised: ~1.1–1.25× narrow
single-stream, ~1.05–1.1× wide** — and flagged unmeasured; a `perf`/`dtrace` of the coordinator's recv
share is the honest way to firm it up.

The **utilization** ~1.9× is more defensible, but *only* as the mechanism P1.6 already names: cutting
the park/wake syscall tax raises `C(N)` and pushes `N_drain_sat` past the measured ~8-stream ceiling
(195 qps flat to 80 threads). It is an **aggregate-CPU-headroom upper bound** realized as throughput
**only** because that tax is what collapses `C(N)` — i.e. **L1 (P1.1) and L6 (P1.6) are the same
lever described twice.** Keep 1.5–1.9× as a **rig-narrow ceiling**, not a field figure (§8).

### 3.3 Memory / B4 / A5 (attack line 6) — real, conditional, not a kill

Current fan-in resident set = 256 msgs × 1 `MergeEntry` × M sources (≈1 K entries at M=4, 2 K at M=8).
A naive L1 with `BATCH_EMIT_ROWS=256` and **unchanged** 256-msg capacity would hold
256×256×M = **65 K×M `MergeEntry` per stream — a 256× blowup**, straight through B4. A sane L1 must
**cut the message-capacity** so resident rows stay bounded (e.g. 4 msgs × 256 rows = 4× current). At a
"few hundred bytes"/`MergeEntry` (the `mod.rs:531` comment) that is low single-digit MB/stream — well
under the 57,344-row / ~15–17 MB/stream Arrow egress term that P1.6 shows dominates. **So B4-safe iff
capacity is cut proportionally.**

Because #2765 sizes that **same** capacity, the two **must be co-designed**: #2765's budget is in
message/entry units; once a message is a 256-row batch, its `clamp(…, MIN, 256)` would budget 256
*batches* = 65 K rows unless reconciled. P1.1 misses this entirely because it believes they are
different channels. **This is a hard prerequisite, not a nicety.** Not a memory *kill* for L1, but the
"batching adds only bounded buffers" line in P1.1 §0 is under-specified and must carry the
capacity-reduction + #2765-reconciliation math.

### 3.4 Parity oracles for L2/L3 (attack line 4)

L2 and L3 alter merge structure/order, so the **physical-dump `*-Data.db.jsonl` goldens cannot catch a
regression** — they enumerate every on-disk cell incl. tombstones, so a read-time-reconciliation
divergence stays green on both sides (CLAUDE.md "two parity oracles"). The load-bearing gates are:
- **`query-semantics-oracle`** (`test-data/query-semantics-oracle.json`,
  `query_semantics_oracle_parity.rs`) — post-reconciliation result set at a **pinned `now`**.
- **point-vs-full differential** (`point_vs_full_differential.rs`, #1918) — for L3 especially, since a
  singleton fast-path could diverge point vs full read paths.

**`--lite`-provable acceptance is possible but requires deliberate wiring.** `--lite` runs the touched
package `--lib` **plus the diff's new `--test` targets**. The oracle parity files live in
`cqlite-core/tests/`, so they only enter the blast radius if the L2/L3 diff **adds cases to those exact
test files** (making them diff-touched). Acceptance criterion: *the same PR extends
`query_semantics_oracle_parity.rs` (and `point_vs_full_differential.rs` for L3) with a
singleton/no-overlap-and-a-TTL/tombstone-collision case, so `--lite` exercises the fast-path against
the full path.* Without that, only the once-per-issue full gate covers it.

---

## 4. Per-lever detail

### L1 — batch fan-in `sync_channel` → **SURVIVES (weakened)**
Mechanics verified (§1). Revised multipliers (§3.2): util **1.5–1.9× rig-narrow ceiling**,
single-stream **~1.1–1.25× narrow / ~1.05–1.1× wide (unmeasured)**. Costs/risks **larger than P1.1
states**: (a) co-design with **#2765 on the same channel** (§3.1); (b) cut message-capacity for B4
(§3.3); (c) preserve the #2419 `egress_channel_depth` gauge — the batch becomes the accounting unit;
(d) preserve #2361 cancel-aware `recv_timeout`. NEW filing still warranted; F2 machinery transfers.

### L2 — inline/thread-less merge → **SURVIVES (narrow-only, A/B-gated)**
The decode-re-serialization argument holds: Stage 2 decode (9.7 %, ~14.9 CPU-s) is currently overlapped
across ~4 reader threads; inlining serializes it onto the coordinator, adding it to the critical path
while removing the ~47 % producer park/wake. Net **~1.4–1.8× narrow** (unmeasured), **≤1.0× wide** as
decode grows. Correctly scoped as an experiment behind a `k`+stream-count A/B gate. **Do not credit L2
in the field stack** (§8). Parity-safe *if* survivor selection is byte-identical — same oracle burden
as L3 (§3.4).

### L3 — reconcile singleton fast-path → **WEAKENED**
Confirmed genuinely absent: `reconcile_cluster_with_overlap_counted` (`mod.rs:4117`) builds a full
`ReconcileState` and runs **eight** steps (`fold_row_deletions`, `resolve_cell_winners`,
`apply_complex_deletions`, `shadow_by_row_deletion`, `filter_dropped_columns`, `expire_ttl_cells`,
`purge_gc_grace`, `build`) for **every** cluster, including a lone Live row. So there is real machinery
to skip. **But the fast-path precondition is narrow and self-defeating:** to fire safely it must prove
*singleton Live AND no row/complex/range deletion AND no expiring/TTL cell (else `expire_ttl_cells`
must run) AND no dropped columns (else `filter_dropped_columns` must run) AND correct #2163/#1037
tallies*. Checking all of that **approaches doing the work**; the real saving is skipping
`ReconcileState` construction + the per-`(column,cell_path)` winner map. On field data the fixtures
**deliberately keep TTL seams** (`test_basic.ttl_test_table`, `test_da.ttl_table`, MEMORY #1935), and
real nodes have genuine overlap/tombstones — those clusters **never hit the fast-path**. Revised:
**~1.20× disjoint-narrow-no-TTL upper bound; ~1.03–1.08× or lower on field-shaped data** (the doc's own
concession, and I'd push it toward the low end). Parity risk **HIGH**; `--lite`-provable only via §3.4.

### L4 — `RowKey` Arc hoist (#1883) → **SURVIVES (re-scoped)**
Mechanics verified (§1). **The multiplier is governed by rows-per-partition (clustering fan-out), NOT
row byte-width** — P1.1 conflates the two. In the narrow keyvalue shape (1 row/partition) hoisting the
Arc build outside `for entry in rows` saves nothing on alloc *count* (one Arc/partition == one
Arc/row) — **NEUTRAL**, as P1.1 says. But the same is true of **any single-row-partition table
regardless of width**: a 180 B wide row that is the *only* row in its partition gets **1.0× from L4**.
L4 pays **only** when partitions carry many clustering rows (amortize the PK Arc over N rows).
**~1.05–1.09× for multi-row-partition tables only.** Whether the field is multi-row-per-partition is
**unknown from the Phase-0 profile** — do not assume it. Cheap, low-risk; keep under #1883, but do not
credit it in the narrow stack and do not assume a field win.

### L5 — FxHash `row_values` map → **SURVIVES**
Target confirmed genuinely still SipHash (§1, §6). ~1.04× narrow & wide, near-free, accepted precedent
(#1590/#1817). Note stands that a `SmallVec` beats any HashMap for a 2–3-column row (M-cost follow-on).

---

## 5. (folded into §4)

---

## 6. The #1817 "FxHashMap row map" reconciliation (why L5 is not already done)

`#1817`'s commit subject reads *"internal FxHashMap row map"*, which would kill L5 if it meant
`row_build.rs:246`. It does not. Reading the diff and the current tree: the FxHashMap `#1817` added is
the **per-partition LIMIT counter** — `type PartitionCounts = rustc_hash::FxHashMap<u128, …>`
(`select_executor/mod.rs:167-173`, *"FxHashMap on the hot PER PARTITION LIMIT counter map"*), keyed on
a `u128` partition token. The per-**row** `HashMap<Arc<str>, Value>` at `row_build.rs:246` is still
`std::collections::HashMap` (default SipHash), and `build_row_from_scan_cached` (`row_build.rs:227`,
containing :246) **is** the function `cqlite-flight/producer.rs:entry_to_row` calls on the hot scan
path. So L5's target is real and un-optimized; the precedent it cites is genuine; there is no overlap.

---

## 7. The double-count ruling (L1 vs parallelism/#2680 vs #2765)

**Ruling: no naive double-count, but three specific compositions are illegitimate.**

1. **L1 × #2680 as independent aggregate throughput multipliers — ILLEGITIMATE.** They move different
   terms of P1.6's `per_pod = min(N_admitted, N_drain_sat, N_mem) × per_stream × C(N)`:
   L1 raises `C(N)`/`N_drain_sat`/`per_stream`; **#2680 only rebalances the cross-pod width and
   contributes ZERO throughput on a single pod** (its 2–4× is *stranded-utilization recovery on lagging
   pods*, a skew fix, not a ceiling lift). So `1.9× (L1) × 3× (#2680) ≈ 5.7×` is wrong on two counts:
   on one pod #2680 = 1.0×, and across pods L1's ceiling-lift and #2680's laggard-recovery **overlap in
   the same aggregate qps** (once L1 lifts every pod's ceiling, there is less stranded work for #2680 to
   recover). Credit them as: `aggregate ≈ Σ_pods [ per_stream(with L1) × min(width, N_drain_sat↑L1) ]`,
   with #2680 flattening the per-pod `min(...)` spread — **not** a scalar product.

2. **L1's "1.9× utilization" AND "raise N_drain_sat" as two wins — ILLEGITIMATE.** They are the same
   lever (P1.1 L1 ≡ P1.6 L6). Count once.

3. **L1 vs #2765 — same channel, so NOT a throughput double-count but a mandatory co-design.** They hit
   the same fan-in `sync_channel` on orthogonal axes (syscall-count vs memory-depth); their *throughput*
   contributions don't overlap, but their *implementations collide* on the capacity constant (§3.3).
   P1.1's "orthogonal, different channel, don't conflate" is the right no-double-credit verdict via a
   wrong mechanism — restate it as "same channel, co-design required."

The **outer Arrow egress channel** (`DO_GET_CHANNEL_CAPACITY`, 57,344 rows) is the only genuinely
independent one, and it is the memory-dominant term P1.6 flags for admission re-sizing — untouched by
any L1–L5 lever.

---

## 8. The field-shape (wide-row) governing multiplier for the stack

**The field multiplier is the WIDE-ROW single-stream number ~1.05–1.15×, and honestly a touch below
it.** Reasoning, brutally:

- Phase-0's 49.9 % Stage-4b share is the **narrow + uncompressed + loopback + 1-row/partition extreme**
  (Phase-0 §5 caveats 1/3/4). Every field departure **shrinks the share L1/L2/L3 attack**:
  compression adds a real Stage-1 decompress-CPU stage (caveat 1), wider rows add Stage 2/3/5 (caveat
  4), real network adds Stage-6 transport CPU (caveat 3). All three dilute 4b/4a.
- **L3 singletons vanish** with clustering keys + real TTL/overlap; **L2 goes ≤1.0×** as decode grows;
  **L1's park/wake share shrinks** as the coordinator does more real per-row work. **L4 becomes the one
  lever that can *gain* on the field** — but only if partitions are multi-row (§4), which the profile
  cannot tell us. **L5 holds at ~1.04×** (per-row map is per-row regardless of shape).
- So the narrow-shape headline numbers — **2.36× aggregate / 1.56× (or 1.9× w/ L2) single-stream** —
  are **rig-shape upper bounds, not field predictions**, exactly consistent with phase1-8's envelope
  (row-pipeline 150–450 k rows/s/core; "600 k/pod is a post-fix target, gated on the merge-coordination
  fix, not a property of shipped code") and P1.6's "utilization levers cannot move `per_stream`."

**Governing field multiplier for the L-stack on the per-stream ceiling: ~1.05–1.15×**, with L1's
`C(N)`/`N_drain_sat` utilization win as the component that best survives to the field (park/wake
contention is genuinely worse under concurrent field load) — but that is a **per-pod utilization**
recovery, not a per-stream-ceiling gain, and it too is diluted by compression/transport CPU the rig
cannot see. Anyone quoting a field number north of ~1.15× on the per-stream ceiling from these five
levers is laundering the rig's narrow shape.

---

## 9. Summary packet

**Per-lever:** L1 **SURVIVES-weakened** (util 1.5–1.9× rig-narrow ceiling; single-stream ~1.1–1.25×
narrow / ~1.05–1.1× wide, unmeasured; cost = co-design with #2765 on the *same* fan-in channel + cut
message-capacity for B4 + keep #2419 gauge/#2361 cancel-recv). L2 **SURVIVES narrow-only, A/B-gated**
(~1.4–1.8× narrow, ≤1.0× wide — not credited in the field stack). L3 **WEAKENED** (~1.20× disjoint-
narrow-no-TTL upper bound, ~1.03–1.08× field; HIGH parity risk; query-semantics + point-vs-full oracles
load-bearing; `--lite`-provable only if the diff extends those test files). L4 **SURVIVES re-scoped**
(1.05–1.09× *only* for multi-row-partition tables; 1.0× for single-row-partition of any width; win
governed by clustering fan-out, unknown from the profile). L5 **SURVIVES** (~1.04×; target confirmed
still SipHash; #1817's FxHashMap was a different, per-partition map).

**Code-claim audit:** all four line-level claims true; the P1.1 SipHash re-attribution
(PartitionKeyCache does no hashing → `row_build.rs:246` is the real site) is **correct**.

**Double-count ruling:** L1 and #2680 attack different formula terms and are NOT a naive product — but
you may **not** multiply L1's 1.9× by #2680's 2–4× (on one pod #2680 = 1.0×; across pods their gains
overlap in aggregate qps), and L1's "1.9× util" is the same lever as P1.6's L6 (count once). L1 and
**#2765 hit the SAME fan-in channel** (P1.1's "different channel/orthogonal" is factually wrong) — no
throughput double-count but a mandatory co-design on the shared capacity constant. The outer 57,344-row
Arrow egress channel is the only independent one and is untouched by L1–L5.

**Field governing multiplier for the stack: ~1.05–1.15× on the per-stream ceiling** (wide-row number
governs, further diluted by compression + transport CPU the rig can't see). The narrow 1.56–2.36×
figures are rig-shape upper bounds, not field predictions.
