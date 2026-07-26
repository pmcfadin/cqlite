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
10, 20}, crossed with five collision mixes, at a `now` pinned through `KWayMerger::with_now_secs`.
The timed region is the FULL public drain (`new_from_readers` → `step()` to `Complete`): producer
setup, `BinaryHeap` refill, cluster assembly, `MergeEntry` construction AND reconciliation — because
the §3 term being tightened is a *whole-scan* derate, not the `ReconcileState` pipeline alone.
Reader OPEN is hoisted out of the timed region (`Arc`-cloned per iteration — the warm-handle shape).
No new `pub` item was added and no production behavior changed.

**The reconcile base was NOT re-measured here.** It is already pinned at ~2.0 µs/row on narrow
disjoint singleton clusters (`docs/research/phase2-verify-stage2.md:226-232`, machinery-dominated),
and the `[ASSUMED]` 10–500 ns/row framing is obsolete. This record supplies the *slope*.

## 1. Run metadata

| | |
|---|---|
| Machine | AWS `r7iz`-class, Intel Xeon Platinum 8488C, **16 vCPU**, 30 GiB RAM, Linux 6.17.0-1019-aws x86_64 |
| Commit | **`df2dc857`** (`issue-2043-reconcile-overlap-multiplier`), `bench` profile (release + debuginfo) |
| Toolchain | rustc 1.97.1 (pinned `rust-toolchain.toml`) |
| Criterion | 20 samples/arm, 1 s warm-up, ≥5 s measurement; medians reported |
| Run 1 | start **2026-07-26T02:54:50Z**, bench-observed **load1m = 0.28** |
| Run 2 | start **2026-07-26T03:06:19Z**, bench-observed **load1m = 0.84** (post-compile) |
| Load ceiling | **2.00** (1-minute average, read from `/proc/loadavg` at run start). Both runs are **VALID**. The bench **fails closed** above the ceiling, so an over-load run produces no numbers at all; `CQLITE_BENCH_ALLOW_LOAD=1` opts out and self-labels the output as non-measurement. |
| Discarded runs | **none.** No run above the ceiling was used, or needed to be discarded, for this record. |
| Corpus | synthesized by `WriteEngine` (k flushed, uncompacted generations, exactly k `Data.db` asserted per arm). `CQLITE_DATASETS_ROOT` is never consulted — the vendored corpus is single-generation and cannot supply k > 1. |
| Reproducibility | run-2-vs-run-1 median delta: **within ±2.5 % for all 20 arms at k ≥ 2**; the k = 1 arms are the noisiest (−8.6 % worst case, `field_blend/k1`) — see §3 for why k = 1 is structurally the least stable point. |

Fixture per generation: 16 partitions × 64 clustering rows = **1024 clusters**, table
`(pk INT, ck INT, v0 TEXT, v1 TEXT, v2 INT, PRIMARY KEY (pk, ck))` — 4 materialized cells per row.

## 2. The measured k-curve

Per **delivered (output)** row, mean of the two runs' Criterion medians. `cost(k)/cost(1)` is the
raw multiplier the spec asks for; `D` is the **pipeline-matched** derate (§4) —
`cost_mix(k) / cost_disjoint(k)`, i.e. the same k, same producer count, differing only in whether
rows collide. `o` = observed input-rows/output-rows (the overlap factor).

| mix | k=1 | k=2 | k=5 | k=10 | k=20 |
|---|---|---|---|---|---|
| **disjoint** (control) | 5527 ns / 1.00 | 3419 ns / 0.62 | 2876 ns / 0.52 | 2753 ns / 0.50 | 2791 ns / 0.50 |
| **lww_overwrite** | 5386 ns / 1.00 / D 0.97 | 6791 ns / 1.26 / D 1.99 | 10809 ns / 2.01 / D 3.76 | 18323 ns / 3.40 / D 6.66 | 33772 ns / 6.27 / D 12.10 |
| **tombstone** | 3878 ns / 1.00 / D 0.70 | 4716 ns / 1.22 / D 1.38 | 7801 ns / 2.01 / D 2.71 | 13977 ns / 3.60 / D 5.08 | 26908 ns / 6.94 / D 9.64 |
| **ttl_expiring** | 5235 ns / 1.00 / D 0.95 | 6767 ns / 1.29 / D 1.98 | 10927 ns / 2.09 / D 3.80 | 18401 ns / 3.51 / D 6.68 | 33751 ns / 6.45 / D 12.09 |
| **field_blend** | 4538 ns / 1.00 / D 0.82 | 5806 ns / 1.28 / D 1.70 | 7766 ns / 1.71 / D 2.70 | 13109 ns / 2.89 / D 4.76 | 24474 ns / 5.39 / D 8.77 |

Overlap factor `o` per arm (observed, not assumed): `lww_overwrite` / `tombstone` / `ttl_expiring`
= exactly k; `field_blend` = 1.00, 1.75, 4.00, 7.75, 15.25 (its 25 % singleton population dilutes
`o`); `disjoint` = 1.00 at every k, by construction.

Collision/deletion observables printed by every arm (`live_cells`, `tombstone_cells`,
`row_tombstones`, `collisions_per_row`) are in the bench's stdout. **Purge counts are zero by
construction, not unmeasured:** this is a READ merge, so `gc_before_secs = None` and
`purge_safe = false`, which makes the gc-grace purge stage a strict no-op. The observable deletion
work is therefore the tombstone/expiry counts, and those are reported.

### Two findings that fall straight out of the table

1. **SSTable COUNT is free; row-level DUPLICATION is what costs.** The `disjoint` control is flat in
   k once the pipeline saturates — 2876 / 2753 / 2791 ns per row at k = 5 / 10 / 20 (±2 %). Reading
   20 generations instead of 5 costs nothing per delivered row when no cluster spans two of them.
   Every bit of the multiplier comes from clusters that appear in more than one generation.
2. **TTL expiry at a pinned `now` is free.** `ttl_expiring` tracks `lww_overwrite` to within 0.1 %
   at k = 20 (33 751 vs 33 772 ns) with an identical marginal cost (1524 vs 1533 ns/input-row),
   while converting exactly one expiring cell per row into a tombstone. `expire_ttl_cells` is not a
   measurable cost centre.

## 3. Anchor validation — and the k = 1 outlier, explained

The spec requires the `disjoint`/k = 1 arm to be validated against the published ~2.0 µs/row
narrow-disjoint-singleton figure BEFORE any multiplier is derived. **Stated band: ±50 % of the
published anchor, applied to the comparable quantity.** The result and its honest reading:

| Quantity | Measured | Published comparable | Ratio | In band? |
|---|---|---|---|---|
| `disjoint` at pipeline saturation (mean of k = 5/10/20) | **2.81 µs/row** | ~2.0 µs/row (`phase2-verify-stage2.md:226-232`; equal to Phase-0's ~500–540 k rows/s single-stream WALL anchor, `phase0-scan-cost-breakdown-2026-07.md:66-72`) | **1.41×** | **YES** |
| `disjoint`/k = 1 as literally specified | **5.53 µs/row** | ~2.0 µs/row | **2.76×** | **NO** |

**The k = 1 point is out of band, and the cause is measured, not guessed.** `disjoint`/k = 1 is
1.97× the saturated value, and the reason is the number of producer threads, not reconcile:

- The saturated fit has **no per-iteration fixed cost to amortize** (`disjoint` k ≥ 5:
  intercept 0.24 ms, 2772 ns/row), so k = 1's excess is **k-dependent**, not a small-scan artifact.
- Phase 0's own stage split predicts exactly this ratio. Producer-side work (stage 1 IO 0.0 % +
  stage 2 decode 9.7 % + stage 3 materialize 4.5 % + stage 4b fan-in park/wake **49.9 %**) is
  **64.1 %** of scan CPU; coordinator-side work (4a reconcile 32.5 % + 5 Arrow 1.0 % + 6 Flight
  0.2 % + 7 other 2.2 %) is **35.9 %**. With ONE producer the drain is producer-bound; with k
  producers the producer side splits k ways and the drain becomes coordinator-bound. Predicted
  transition: **64.1 / 35.9 = 1.79×**. Measured: **1.97×**.
- Independent cross-check: at an identical 2048 input rows and identical cell count, a 1-producer
  drain costs 4.75 µs/row while a 2-producer drain costs 3.43 µs/row — the row count is held fixed
  and only the producer count changes.

**Consequence for this record (stated rather than quietly chosen):** the harness is sound — the
saturated control lands at 1.41× the published anchor on a different CPU with 2× the cells per row
— but `cost(k)/cost(1)` uses a base that is inflated ~1.97× by single-producer handoff, so it
**understates** the overlap multiplier. Both forms are therefore published: the spec-mandated
`cost(k)/cost(1)` in §2, and the **pipeline-matched** `D` (same k on both sides of the ratio) which
is the form substituted into §3. The 2.0 µs/row stage-4a CPU attribution is **not** directly
comparable to a whole-drain wall measurement and is not used as if it were; the comparable used is
Phase-0's own single-stream wall rate, which happens to coincide numerically.

## 4. The derived model — cost as a function of the overlap factor

Fitting `t = p · input_rows + q · output_rows` over the saturated (k ≥ 5) arms separates the cost of
*reading and resolving one more colliding copy* from the cost of *delivering one row*:

| Fitted over | `p` (ns per input row) | `q` (ns per delivered row) | max residual |
|---|---|---|---|
| `disjoint` + `lww_overwrite` | 1660 | 1130 | 12.7 % |
| `disjoint` + `ttl_expiring` | 1662 | 1129 | 13.6 % |
| `disjoint` + `field_blend` | 1528 | 1261 | 5.1 % |
| `disjoint` + `lww` + `tombstone` | 1462 | 1328 | 20.1 % |

Per-mix marginal cost of one extra colliding generation-row: **1533 ns** (`lww_overwrite`),
**1524 ns** (`ttl_expiring`), **1489 ns** (`field_blend`), **1277 ns** (`tombstone` — a shadowed row
is *cheaper* than a reconciled live one). Against a fresh delivered row at **2772–2791 ns**, an
extra colliding copy costs **0.46–0.55×** a fresh row.

**The gen-overlap derate, as a function of the overlap factor `o` = generations per delivered row:**

```
D(o) = (q + p·o) / (q + p)          with p ≈ 1.53 µs, q ≈ 1.26 µs   (o=1 ⇒ 2.79 µs/row, measured)
```

| `o` | 1.0 | 1.25 | 1.5 | 1.75 | 2.0 | 3.0 | 4.0 |
|---|---|---|---|---|---|---|---|
| **D(o)** | 1.00 | 1.13–1.15 | 1.26–1.30 | 1.39–1.45 | 1.52–1.60 | 2.05–2.19 | 2.57–2.79 |

(Range across the four fits above.) The closed form is the **saturated asymptote**, and it is
validated against the arm it was not tuned on point-by-point — `field_blend`, whose `o` differs from
its `k`:

| `field_blend` arm | k=2 | k=5 | k=10 | k=20 |
|---|---|---|---|---|
| observed `o` | 1.75 | 4.00 | 7.75 | 15.25 |
| measured `D` | 1.70 | 2.70 | 4.76 | 8.77 |
| model `D(o)` | 1.41 | 2.64 | 4.70 | 8.81 |

**Agreement is within 2 % at k ≥ 5 and the model reads 17 % LOW at k = 2** — because at k = 2 the
drain is still producer-bound (§3), which the saturated fit excludes by construction. This matters
for how the model is applied: a field scan has a **high producer count and a low `o`**
(k ≈ 4–8 SSTables, most rows singleton), which is squarely the saturated regime; the k = 2 arm
conflates low producer count with low `o` in a way the field does not. The saturated form is
therefore the right one to substitute, and where it errs at small k it errs **low** (conservative).

## 5. Verdict for §3 — the gen-overlap term

**The 1.1–1.5× band is confirmed as correctly sized *for its implied overlap*, and is now
expressible as a function instead of a guess: `D = 1.1×` ⇔ `o ≈ 1.18`, `D = 1.5×` ⇔ `o ≈ 1.9`, so the
existing band means `o ∈ [1.2, 1.9]`.** Three changes to how the term should be used:

1. **It is a duplication term, not an SSTable-count term.** Measured: SSTable count alone is free
   (§2 finding 1). The derate is driven purely by the fraction of delivered rows assembled from more
   than one generation — i.e. by the *overwrite/update rate relative to compaction cadence*. This
   sharpens (and is consistent with) the graveyard entry that killed "RF=3 as a reconcile
   multiplier": the surviving term is real, but it is narrower than "generation overlap" suggests.
2. **The band's floor is exact.** An insert-once table (time-series/append-only, a primary connector
   target) has `o = 1.0` and therefore **`D = 1.00`, not 1.1×** — for those tables the gen-overlap
   term should be dropped from the derate entirely, not carried at its floor.
3. **The band's ceiling is optimistic for update-bearing tables.** At `o = 3–4` — a normal STCS
   "SSTables per read" p99 for a table taking overwrites — `D` is **2.05–2.79×**, well outside
   1.1–1.5×. The derate should carry `o` explicitly rather than a single band.

### The field `o` this substitutes — **ASSUMPTION, NOT MEASUREMENT**

> **`o_field = 1.25–1.5` (central 1.35) ⇒ D = 1.13–1.30 (central ~1.2).**
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
cluster shape. Here is the arithmetic that decides it, with this record's measured inputs.

**Step 1 — what 1.20× implies.** P2:stage2's ~1.20× is measured on a fixture where *every* cluster
is fast-path-eligible (`f = 1`: a lone Live entry, no row/complex/range deletion, no expiring cell,
no dropped columns). So L3 removes `1 − 1/1.20 = 16.7 %` of the drain at `f = 1`, i.e.
**0.47 µs of the measured 2.79 µs/delivered row**.

**Step 2 — speedup from eligibility alone** (holding the drain cost at its `o = 1` value, which is
the basis on which BOTH parties quoted their multipliers): `S_elig(f) = 1 / (1 − 0.167·f)`:

| `f` (eligible cluster fraction) | 1.00 | 0.50 | 0.45 | 0.25 | 0.17 | 0.10 |
|---|---|---|---|---|---|---|
| **L3 speedup** | **1.20×** | 1.09× | 1.08× | 1.04× | **1.03×** | 1.02× |

⇒ **P2:stage2's ~1.20× is correct iff `f ≳ 0.95`. P2:row-engine's 1.03–1.08× is correct for
`f ∈ [0.17, 0.45]`.**

**Step 3 — from cluster shape to `f`.**
`f = P(depth = 1) × P(no expiring cell) × P(no row/complex/range deletion) × P(no dropped column)`.
With mean depth 2 among the overlapping clusters, `P(depth = 1) = 1 − (o − 1)`, so
`o = 1.0 → f ≤ 1.00`, `o = 1.25 → f ≤ 0.75`, `o = 1.5 → f ≤ 0.50`, `o = 1.83 → f ≤ 0.17`,
`o ≥ 2.0 → f ≈ 0`.

**Step 4 — the new datum this record contributes: overlap hurts L3 TWICE, multiplicatively.**

- It **destroys eligibility** (Step 3), and
- it **raises the denominator L3's fixed 0.47 µs saving is divided into**: the overlap cost is
  entirely on the `p` side (~1.53 µs per extra input row — decode + heap + collision resolve), which
  a *singleton* fast-path cannot touch by construction. At `o = 2` the drain is 4.45 µs/delivered
  row, so even a hypothetically-eligible cluster would yield only `1/(1 − 0.47/4.45) = 1.12×`.

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

**Step 5 — the additional decisive gate, from §2 finding 2.** TTL costs the merge ~nothing
(`ttl_expiring` ≡ `lww_overwrite` to 0.1 %), yet a TTL-bearing cluster is *ineligible* for L3. So on
a table with a TTL'd queried column, L3 is a **pure eligibility loss with no compensating cost
saving** — `f → 0` regardless of `o`, and L3's value collapses independently of overlap. Any field
table carrying TTL settles the disposition on its own.

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
- The instrument measures **wall-clock whole-drain** cost, not stage-attributed CPU. It cannot
  decompose the residual between its 2.81 µs/row saturated control and Phase-0's 2.0 µs/row stage-4a
  CPU figure (different corpus shape — 4 cells/row vs 2 columns — and different CPU). That
  decomposition belongs to a profiler run on the rig, not here.
- `field_blend`'s composition (25 % singleton, 25 % per-column blend, 25 % tombstone, 25 % expiring)
  is a **modelling choice**, not a measured field distribution. It is used only as a sanity point on
  the `D(o)` curve, never as the source of `o_field`.
- Every number in this record comes from the two runs in §1 at commit `df2dc857`. Nothing is
  extrapolated beyond the fitted curve, and no run was discarded (none exceeded the load ceiling).
