# The reconcile generation-overlap multiplier — measured k-curve and verdict

**Issue:** #2043 (WS7 repoint) · **Epic:** #2817 (0.17 throughput program), manifest item **M9** ·
**Consumers:** `docs/architecture/throughput-program-2026-07.md` §3 (field derate, gen-overlap term)
and §4 (the **L3** tension flag, which blocks M7 / #2822).

**Instrument:** `cqlite-core/benches/reconcile_overlap.rs` +
`cqlite-core/benches/fixtures/multigen.rs`. Run it with

```bash
cargo bench -p cqlite-core --features write-support --bench reconcile_overlap
```

## 0. What this measures, in one paragraph

Per-row `KWayMerger` drain cost for row clusters spanning **k** SSTable generations, k ∈ {1, 2, 5,
10, 20}, crossed with five collision mixes, at a `now` pinned through `KWayMerger::with_now_secs`,
plus a two-arm **producer-count control**. The timed region is the FULL public drain
(`new_from_readers` → `step()` to `Complete`): producer setup, `BinaryHeap` refill, cluster assembly,
`MergeEntry` construction AND reconciliation — because the §3 term being tightened is a *whole-scan*
derate, not the `ReconcileState` pipeline alone. Reader OPEN is hoisted out of the timed region
(`Arc`-cloned per iteration — the warm-handle shape). No new `pub` item was added and no production
behavior changed.

**The reconcile base was NOT re-measured here.** It is already pinned at ~2.0 µs/row on narrow
disjoint singleton clusters (`docs/research/phase2-verify-stage2.md:226-232`, machinery-dominated),
and the `[ASSUMED]` 10–500 ns/row framing is obsolete. This record supplies the *slope*.

**Every arm's collision shape is asserted before it is timed** (issue #2043 review round 2). Each arm
checks a full census — output rows/partitions, live cells, cell tombstones, row tombstones and
coexisting row deletions (`MergeEntry::row_deletion`, issue #932) — against the census implied by its
documented mix, and asserts that generation *g* contributes identically at every k. This exists
because the first version of the `tombstone` fixture stamped its row tombstone ABOVE the
generation's live cells, so the flush writer's own reconciliation collapsed every affected cluster to
a **cell-less** row tombstone: the documented live-cells-vs-row-tombstone collision never reached the
merge and the arm silently measured tombstone-vs-tombstone while still printing plausible ns/row. All
numbers below are from the **corrected** fixture and are not comparable to the pre-correction run.

## 1. Run metadata

| | |
|---|---|
| Machine | AWS `r7iz`-class, Intel Xeon Platinum 8488C, **16 vCPU**, 30 GiB RAM, Linux 6.17.0-1019-aws x86_64 |
| Commit | **`6f894d67`** (`issue-2043-reconcile-overlap-multiplier`), `bench` profile (release + debuginfo) |
| Toolchain | rustc 1.97.1 (pinned `rust-toolchain.toml`) |
| Criterion | 20 samples/arm, 1 s warm-up, ≥5 s measurement; medians reported |
| Run 1 | **2026-07-26T04:14:15Z → 04:18:11Z**, run-start load1m **0.73**, **max foreign CPU 0.14 cores** |
| Run 2 | **2026-07-26T04:21:20Z → 04:25:17Z**, run-start load1m **0.66**, **max foreign CPU 0.20 cores** |
| Validity guard | Two tiers, both fail closed. **(1)** run-start `load1m` ≤ **2.00**. **(2)** per arm, the **foreign** (not-this-process) CPU busy during that arm's TIMED region ≤ **1.00 core** of 16, computed as `/proc/stat` busy minus this process's own `utime+stime`. The `/proc/stat` capacity sum covers `user…steal` only — `guest`/`guest_nice` are excluded because the kernel already counts them inside `user`/`nice`, and double-counting them would inflate the foreign figure on a KVM host (both were `0` on this box, so the numbers above are unaffected). Tier 2 is not a per-arm loadavg check because `KWayMerger` runs one producer thread per generation, so the run's OWN parallelism drives `load1m` to 4.6–5.2 by k = 20 — a per-arm loadavg gate fails on the instrument, not on interference. `load1m` is still sampled per arm and reported: run 1 range 0.69–5.23, run 2 range 0.66–4.59, **almost entirely self-inflicted**. Both probes read `/proc` and are **Linux-only** (unavailable ⇒ fails closed), so these are Linux measurements by construction. Both ceilings are **derived from the host's core count** and printed in the run header (`0.125 × cores` for tier 1, floor 0.5; `0.0625 × cores` cores for tier 2, floor 0.25 core) rather than fixed, so a smaller host gets a proportionally stricter gate instead of 4× more slack. On this **16-vCPU** box they evaluate to exactly the **2.00 / 1.00 core** figures both runs above enforced — the banked numbers are unchanged by the derivation. |
| Both runs | **VALID.** Peak foreign CPU across all 54 timed arms was **0.20 of 16 cores (1.3 %)**. |
| Discarded runs | **one, and it produced no numbers:** a run started 04:18:26Z was refused at the run-start gate (`load1m` 2.99 — the decaying tail of run 1's own producer threads). Nothing from it is used. A back-to-back re-run must wait for the 1-minute average to decay; this is now noted in `benches/README.md`. |
| Corpus | synthesized by `WriteEngine` (k flushed, uncompacted generations, exactly k `Data.db` asserted per arm, compaction explicitly disabled). `CQLITE_DATASETS_ROOT` is never consulted — the vendored corpus is single-generation and cannot supply k > 1. |
| Reproducibility | run-2-vs-run-1 median spread: **≤2.2 % on all 20 matrix arms at k ≥ 2** (worst `tombstone/k10`, 2.1 %); the k = 1 arms are the noisiest (worst `lww_overwrite/k1`, 11.3 %) — see §3 for why k = 1 is structurally the least stable point. |

Fixture per generation: 16 partitions × 64 clustering rows = **1024 clusters**, table
`(pk INT, ck INT, v0 TEXT, v1 TEXT, v2 INT, PRIMARY KEY (pk, ck))`. A fully-live reconciled row
carries **4 materialized cells** (`ck` + `v0` + `v1` + `v2`); this is observed through the public
`MergeStep` stream, not assumed, and the shape assertions fail if it changes.

## 2. The measured k-curve

Per **delivered (output)** row, mean of the two runs' Criterion medians. `cost(k)/cost(1)` is the
raw multiplier the spec asks for; `D` is the **pipeline-matched** derate (§4) —
`cost_mix(k) / cost_disjoint(k)`, i.e. the same k, same producer count, differing only in whether
rows collide. `o` = observed input-rows/output-rows (the overlap factor).

| mix | k=1 | k=2 | k=5 | k=10 | k=20 |
|---|---|---|---|---|---|
| **disjoint** (control) | 5409 ns / 1.00 | 3389 ns / 0.63 | 2858 ns / 0.53 | 2782 ns / 0.51 | 2830 ns / 0.52 |
| **lww_overwrite** | 5265 ns / 1.00 / D 0.97 | 6725 ns / 1.28 / D 1.98 | 10732 ns / 2.04 / D 3.75 | 18339 ns / 3.48 / D 6.59 | 33704 ns / 6.40 / D 11.91 |
| **tombstone** | 5383 ns / 1.00 / D 1.00 | 6779 ns / 1.26 / D 2.00 | 11047 ns / 2.05 / D 3.87 | 19124 ns / 3.55 / D 6.87 | 34705 ns / 6.45 / D 12.26 |
| **ttl_expiring** | 5631 ns / 1.00 / D 1.04 | 6704 ns / 1.19 / D 1.98 | 10828 ns / 1.92 / D 3.79 | 18477 ns / 3.28 / D 6.64 | 33846 ns / 6.01 / D 11.96 |
| **field_blend** | 5237 ns / 1.00 / D 0.97 | 5818 ns / 1.11 / D 1.72 | 8605 ns / 1.64 / D 3.01 | 14073 ns / 2.69 / D 5.06 | 25997 ns / 4.96 / D 9.18 |

**Producer-count control** (identical 2048 output rows, 8192 live cells and `o = 1` on both sides;
only the producer-stream count differs): `producer_control/p1` (ONE double-width generation) =
**4891 ns/row**, `producer_control/p2` (TWO standard-width generations) = **3433 ns/row** ⇒
**1.42× for the 1→2 producer step alone.** (`p2` is fixture-identical to `disjoint/k2`, and lands
within 1.3 % of it — an internal reproducibility check.)

Overlap factor `o` per arm (observed, not assumed): `lww_overwrite` / `tombstone` / `ttl_expiring`
= exactly k; `field_blend` = 1.00, 1.75, 4.00, 7.75, 15.25 (its 25 % singleton population dilutes
`o`); `disjoint` = 1.00 at every k, by construction.

Collision/deletion observables printed by every arm (`live_cells`, `tombstone_cells`,
`row_tombstones`, `coexisting_row_deletions`, `collisions_per_row`, `load1m`, `foreign_cpu_cores`)
are in the bench's stdout. **Purge counts are zero by construction, not unmeasured:** this is a READ
merge, so `gc_before_secs = None` and `purge_safe = false`, which makes the gc-grace purge stage a
strict no-op. The observable deletion work is the tombstone/expiry counts, and those are reported.

### Three findings that fall straight out of the table

1. **SSTable COUNT is free; row-level DUPLICATION is what costs.** The `disjoint` control is flat in
   k once the pipeline saturates — 2858 / 2782 / 2830 ns per row at k = 5 / 10 / 20 (±1.3 %). Reading
   20 generations instead of 5 costs nothing per delivered row when no cluster spans two of them.
   Every bit of the multiplier comes from clusters that appear in more than one generation.
2. **TTL expiry at a pinned `now` is free.** `ttl_expiring` tracks `lww_overwrite` to within 0.4 %
   at k = 20 (33 846 vs 33 704 ns) with a marginal cost per extra input row of 1535 vs 1531 ns
   (+0.3 %), while converting exactly one expiring cell per row into a tombstone.
   `expire_ttl_cells` is not a measurable cost centre.
3. **Deletion collisions cost slightly MORE than plain overwrite, not less.** With the corrected
   fixture, `tombstone` is the most expensive mix at every k ≥ 5: 34 705 ns/row at k = 20, **+3.0 %**
   over `lww_overwrite`, with a marginal cost of 1577 vs 1531 ns per extra input row (+3.0 %).
   Resolving a row deletion against surviving newer cells and carrying it forward (issue #932), plus
   per-column tombstone-vs-live ties, is *additional* work on top of LWW — it does not replace it.
   (The pre-correction fixture reported the opposite, because its shadowed clusters had already
   collapsed to cell-less row tombstones at flush time and were therefore cheap to merge. That
   finding was an artifact and is withdrawn.)

## 3. Anchor validation — and the k = 1 deviation, decomposed

**Stated band: ±50 % of the published anchor, applied to the comparable quantity.** The anchor is
the **saturated `disjoint` control** (mean of k = 5/10/20), per the amended spec requirement
(`openspec/changes/reconcile-overlap-multiplier/specs/reconcile-overlap-measurement/spec.md`, owner
decision 2026-07-26): the anchor as originally written (`disjoint`/k = 1) is a whole-drain WALL time
produced by a SINGLE producer stream, which is not the quantity the published figure reports, so
comparing them measured two different things. **An out-of-band SATURATED anchor still voids a run.**

| Quantity | Measured | Published comparable | Ratio | In band? |
|---|---|---|---|---|
| **ANCHOR** — `disjoint` at pipeline saturation (mean of k = 5/10/20) | **2.82 µs/row** | ~2.0 µs/row (`phase2-verify-stage2.md:226-232`; equal to Phase-0's ~500–540 k rows/s single-stream WALL anchor, `phase0-scan-cost-breakdown-2026-07.md:66-72`) | **1.41×** | **YES** |
| `disjoint`/k = 1, reported as an explained deviation | **5.41 µs/row** | ~2.0 µs/row | **2.70×** | n/a — see below |

**The k = 1 deviation is now fully decomposed by measured arms, with no residual.** `disjoint`/k = 1
is **1.92×** the saturated value, and that factor factorizes exactly:

| Step | Arms compared | Factor | What changes |
|---|---|---|---|
| Scan width | `disjoint/k1` (1024 rows) → `producer_control/p1` (2048 rows) | **1.106×** | rows only — per-iteration fixed cost amortizes |
| Producer count 1 → 2 | `producer_control/p1` → `producer_control/p2` | **1.424×** | producer streams only (rows, cells, `o` held fixed) |
| Producer count 2 → 5+ | `producer_control/p2` → saturated `disjoint` | **1.216×** | producer streams (and width) only |
| **Product** | | **1.916×** | = the measured `disjoint/k1` ÷ saturated ratio (1.916×) |

- The **producer-count-only** component is `1.424 × 1.216 = 1.73×`. Phase 0's own stage split
  predicts **1.79×** for exactly this transition: producer-side work (stage 1 IO 0.0 % + stage 2
  decode 9.7 % + stage 3 materialize 4.5 % + stage 4b fan-in park/wake **49.9 %**) is **64.1 %** of
  scan CPU and coordinator-side work (4a reconcile 32.5 % + 5 Arrow 1.0 % + 6 Flight 0.2 % + 7 other
  2.2 %) is **35.9 %**, so a producer-bound single-stream drain should cost 64.1/35.9 = 1.79× a
  coordinator-bound one. **Measured 1.73× against a 1.79× prediction.**
- The saturated fit has **no per-iteration fixed cost left to amortize** (`disjoint` k ≥ 5:
  intercept 0.24 ms, 2823 ns/row), which is why only the k = 1 (and, weakly, k = 2) points carry a
  width term at all.

**Consequence for this record (stated rather than quietly chosen):** the harness is sound — the
saturated control lands at 1.41× the published anchor on a different CPU with 2× the cells per row —
but `cost(k)/cost(1)` uses a base inflated ~1.92× by single-producer handoff and narrow-scan fixed
cost, so it **understates** the overlap multiplier. Both forms are therefore published: the
spec-mandated `cost(k)/cost(1)` in §2, and the **pipeline-matched** `D` (same k on both sides of the
ratio) which is the form substituted into §3. The 2.0 µs/row stage-4a CPU attribution is **not**
directly comparable to a whole-drain wall measurement and is not used as if it were; the comparable
used is Phase-0's own single-stream wall rate, which happens to coincide numerically.

## 4. The derived model — cost as a function of the overlap factor

Fitting `t = p · input_rows + q · output_rows` (ordinary least squares) over the saturated (k ≥ 5)
arms separates the cost of *reading and resolving one more colliding copy* from the cost of
*delivering one row*:

| Fitted over | `p` (ns per input row) | `q` (ns per delivered row) | max residual |
|---|---|---|---|
| `disjoint` + `field_blend` — **the headline fit** (lowest residual) | **1644** | **1180** | **9.9 %** |
| `disjoint` + `lww_overwrite` | 1655 | 1170 | 12.0 % |
| `disjoint` + `ttl_expiring` | 1664 | 1161 | 12.4 % |
| `disjoint` + `lww` + `tombstone` | 1685 | 1143 | 13.4 % |

Per-mix marginal cost of one extra colliding generation-row (k = 5 → k = 20): **1577 ns**
(`tombstone`), **1546 ns** (`field_blend`), **1535 ns** (`ttl_expiring`), **1531 ns**
(`lww_overwrite`). Against a fresh delivered row at **2782–2858 ns**, an extra colliding copy costs
**0.54–0.56×** a fresh row.

**The gen-overlap derate, as a function of the overlap factor `o` = generations per delivered row.**
Every figure below uses the **headline `disjoint` + `field_blend` fit**, named explicitly so the
arithmetic is checkable:

```
D(o) = (q + p·o) / (q + p)     with p = 1.644 µs/input-row, q = 1.180 µs/delivered-row
                                (o=1 ⇒ 2.82 µs/row FITTED, vs 2.82 µs/row MEASURED saturated control)
```

| `o` | 1.0 | 1.25 | 1.5 | 1.75 | 2.0 | 3.0 | 4.0 |
|---|---|---|---|---|---|---|---|
| **D(o)** — headline fit | 1.00 | 1.15 | 1.29 | 1.44 | 1.58 | 2.16 | 2.75 |
| range across all four fits | 1.00 | 1.15 | 1.29–1.30 | 1.44–1.45 | 1.58–1.60 | 2.16–2.19 | 2.75–2.79 |

The closed form is the **saturated asymptote**, and it is validated against the arm it was not tuned
on point-by-point — `field_blend`, whose `o` differs from its `k`:

| `field_blend` arm | k=2 | k=5 | k=10 | k=20 |
|---|---|---|---|---|
| observed `o` | 1.75 | 4.00 | 7.75 | 15.25 |
| measured `D` | 1.72 | 3.01 | 5.06 | 9.18 |
| model `D(o)`, headline fit | 1.44 | 2.75 | 4.93 | 9.29 |
| model vs measured | −16.3 % | −8.8 % | −2.6 % | +1.2 % |

**Agreement is within ~3 % at k ≥ 10 and the model reads LOW at small k** (−8.8 % at k = 5, −16.3 %
at k = 2) — because at low k the drain is still producer-bound (§3), which the saturated fit excludes
by construction. This matters for how the model is applied: a field scan has a **high producer count
and a low `o`** (k ≈ 4–8 SSTables, most rows singleton), which is squarely the saturated regime; the
low-k arms conflate low producer count with low `o` in a way the field does not. The saturated form
is therefore the right one to substitute, and where it errs at small k it errs **low**
(conservative).

## 5. Verdict for §3 — the gen-overlap term

**The 1.1–1.5× band is confirmed as correctly sized *for its implied overlap*, and is now
expressible as a function instead of a guess: `D = 1.1×` ⇔ `o ≈ 1.17`, `D = 1.5×` ⇔ `o ≈ 1.86`, so
the existing band means `o ∈ [1.2, 1.9]`.** Three changes to how the term should be used:

1. **It is a duplication term, not an SSTable-count term.** Measured: SSTable count alone is free
   (§2 finding 1). The derate is driven purely by the fraction of delivered rows assembled from more
   than one generation — i.e. by the *overwrite/update rate relative to compaction cadence*. This
   sharpens (and is consistent with) the graveyard entry that killed "RF=3 as a reconcile
   multiplier": the surviving term is real, but it is narrower than "generation overlap" suggests.
2. **The band's floor is exact.** An insert-once table (time-series/append-only, a primary connector
   target) has `o = 1.0` and therefore **`D = 1.00`, not 1.1×** — for those tables the gen-overlap
   term should be dropped from the derate entirely, not carried at its floor.
3. **The band's ceiling is optimistic for update-bearing tables.** At `o = 3–4` — a normal STCS
   "SSTables per read" p99 for a table taking overwrites — `D` is **2.16–2.75×**, well outside
   1.1–1.5×. The derate should carry `o` explicitly rather than a single band.

### The field `o` this substitutes — **ASSUMPTION, NOT MEASUREMENT**

> **`o_field = 1.25–1.5` (central 1.35) ⇒ D = 1.15–1.29 (central ~1.21).**
> **This is an assumption, not a measurement.** Its basis is an **STCS-derived expected-k band**:
> with `min_threshold = 4`, a steady-state STCS table carries ~4–8 live SSTables, and a given
> `(pk, ck)` appears in as many of them as the distinct flush windows in which it was written — so
> `o` is bounded above by the live-SSTable count and is ~1.0 for insert-once data, rising with the
> overwrite rate; the customary production shape for an update-bearing STCS table is a
> SSTables-per-read p50 of 1–2 with a p99 of 3–5, which maps to `o ≈ 1.25–1.5` at the median.
> The vendored corpus is **single-generation**, so the field `o` distribution is **not measurable on
> this machine** — it lives on the i4i rig. **#2818 (M0) is the measurement that replaces this
> assumption**, and because §4's model is a closed-form function of `o`, substituting a measured `o`
> needs no re-derivation and no re-run of this bench.

## 6. Verdict for §4 — the L3 disposition, resolved **conditionally**

P2:stage2 ranks **L3** (reconcile singleton fast-path, M7/#2822) the #2 ceiling lever at ~1.20×
disjoint-narrow; P2:row-engine rules it WEAKENED at ~1.03–1.08×. The disagreement is entirely about
cluster shape. Here is the arithmetic that decides it, with this record's measured inputs (headline
fit throughout).

**Step 1 — what 1.20× implies.** P2:stage2's ~1.20× is measured on a fixture where *every* cluster
is fast-path-eligible (`f = 1`: a lone Live entry, no row/complex/range deletion, no expiring cell,
no dropped columns). So L3 removes `1 − 1/1.20 = 16.7 %` of the drain at `f = 1`, i.e.
**0.47 µs of the 2.82 µs/delivered row at `o = 1`**.

**Step 2 — speedup from eligibility alone** (holding the drain cost at its `o = 1` value, which is
the basis on which BOTH parties quoted their multipliers): `S_elig(f) = 1 / (1 − 0.167·f)`:

| `f` (eligible cluster fraction) | 1.00 | 0.50 | 0.45 | 0.25 | 0.17 | 0.10 |
|---|---|---|---|---|---|---|
| **L3 speedup** | **1.20×** | 1.09× | 1.08× | 1.04× | **1.03×** | 1.02× |

⇒ **P2:stage2's ~1.20× is correct iff `f ≳ 0.95`. P2:row-engine's 1.03–1.08× is correct for
`f ∈ [0.17, 0.45]`.**

**Step 3 — from cluster shape to `f`.**
`f = P(depth = 1) × P(no expiring cell) × P(no row/complex/range deletion) × P(no dropped column)`.
Taking **mean depth 2 among the overlapping clusters** — a **modelling assumption**, not a
measurement (see §7) — gives `P(depth = 1) = 1 − (o − 1)`, so `o = 1.0 → f ≤ 1.00`,
`o = 1.25 → f ≤ 0.75`, `o = 1.5 → f ≤ 0.50`, `o = 1.83 → f ≤ 0.17`, `o ≥ 2.0 → f ≈ 0`.

**Step 4 — the new datum this record contributes: overlap hurts L3 TWICE, multiplicatively.**

- It **destroys eligibility** (Step 3), and
- it **raises the denominator L3's fixed 0.47 µs saving is divided into**: the overlap cost is
  entirely on the `p` side (1.64 µs per extra input row — decode + heap + collision resolve), which
  a *singleton* fast-path cannot touch by construction. At `o = 2` the drain is **4.47 µs/delivered
  row** (headline fit), so even a hypothetically-eligible cluster would yield only
  `1/(1 − 0.47/4.47) = 1.12×`.

Combining both — `S(o) = 1 / (1 − 0.47·f(o) / (q + p·o))` with `f(o) = max(0, 2 − o)` — gives the
**k-band that makes each ruling correct:**

| Field cluster shape | `f` | **L3 speedup** | Whose ruling holds |
|---|---|---|---|
| `o = 1.0` (insert-once) AND no TTL, no tombstone load | 1.00 | **1.20×** | **P2:stage2** — headline lever |
| `o ≈ 1.1` | 0.90 | 1.17× | P2:stage2 |
| `o ≈ 1.25` | 0.75 | 1.12× | between the two |
| `o ≈ 1.35` (the §5 **assumed** central point) | 0.65 | **1.10×** | between the two |
| `o ≈ 1.5` | 0.50 | 1.07× | **P2:row-engine** — WEAKENED |
| `o ≈ 1.75` | 0.25 | 1.03× | **P2:row-engine** |
| `o ≳ 2.0`, **or any `o` on a table with a TTL'd queried column or a tombstone-bearing column set** | ≈ 0 | **≈ 1.00×** | L3 is worthless |

**Step 5 — the additional decisive gate, from §2 findings 2 and 3.** TTL costs the merge ~nothing
(`ttl_expiring` ≡ `lww_overwrite` to 0.4 %), yet a TTL-bearing cluster is *ineligible* for L3. So on
a table with a TTL'd queried column, L3 is a **pure eligibility loss with no compensating cost
saving** — `f → 0` regardless of `o`, and L3's value collapses independently of overlap. Deletion
load is worse than neutral: it *raises* merge cost 3.0 % over plain overwrite (finding 3) while also
disqualifying the cluster. Any field table carrying TTL or tombstones settles the disposition on its
own.

**Disposition (conditional, per design D5).** At the §5 **assumed** central `o ≈ 1.35`, L3 lands at
**~1.10×** (range 1.07–1.12× over the assumed `o ∈ [1.25, 1.5]`) — **materially below P2:stage2's
~1.20× headline claim**, above P2:row-engine's 1.03–1.08× floor, and **collapsing to ~1.00× the
moment the queried table carries TTL or a tombstone-bearing column set**. Neither party is simply
right: stage2's 1.20× requires `o = 1.0` *and* a TTL-free, tombstone-free table (its rig fixture
exactly), while row-engine's ≤1.08× requires `o ≳ 1.5` or any TTL. **Recommendation: keep L3 OFF the
headline lever list — its expected value at the assumed field shape (~1.10×) does not earn the #2
ceiling-lever slot ahead of L1; sequence it after L1 and after M0.** The final call needs exactly two
field numbers, both from **#2818 (M0)**: (a) the SSTables-per-read / row-duplication distribution
giving `o`, and (b) whether the queried columns carry TTL. Given those, Steps 3–4 yield the
disposition arithmetically — **this bench does not need to be re-run.**

## 7. Honesty boundary

- The field `o` in §5 is an **assumption** (STCS-derived), explicitly labelled, and #2818 replaces it.
- **§6 Step 3's "mean depth 2 among the overlapping clusters" is a second modelling assumption**,
  not a measurement. It is what turns `o` into `f(o) = max(0, 2 − o)` and therefore drives the whole
  §6 L3 table. A heavier-tailed depth distribution at the same `o` would leave MORE singleton
  clusters (higher `f`, more favourable to L3); a tighter one, fewer. #2818's duplication
  distribution replaces this assumption too — it yields `f` directly rather than via `o`.
- Every `D(o)`, `S(o)` and drain figure quoted outside §4's fit table uses the **`disjoint` +
  `field_blend`** fit (`p = 1644`, `q = 1180` ns), named at each use. The cross-fit spread is ≤1.5 %
  at `o ≤ 4` and is shown in §4's table.
- The instrument measures **wall-clock whole-drain** cost, not stage-attributed CPU. It cannot
  decompose the residual between its 2.82 µs/row saturated control and Phase-0's 2.0 µs/row stage-4a
  CPU figure (different corpus shape — 4 cells/row vs 2 columns — and different CPU). That
  decomposition belongs to a profiler run on the rig, not here.
- `field_blend`'s composition (25 % singleton, 25 % per-column blend, 25 % tombstone, 25 % expiring)
  is a **modelling choice**, not a measured field distribution. It is used as the headline fit and as
  a sanity point on the `D(o)` curve, never as the source of `o_field`.
- Every number in this record comes from the two runs in §1 at commit `6f894d67`, including the
  producer-count decomposition in §3 (`producer_control/{p1,p2}` are arms of the matrix run, not a
  side experiment). Nothing is extrapolated beyond the fitted curve. One run was refused by the
  run-start load gate and produced no numbers; it is listed in §1.
