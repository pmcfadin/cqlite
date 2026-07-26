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

**Per-drain setup is amortized and measured, and every published number is GATED.** The timed
region deliberately keeps `new_from_readers` (one producer-thread spawn + one adapter open per
generation) — but the arm width was raised 4× so per-row work dominates it, and each arm PRINTS its
measured setup share (0.20–0.24 % at k = 1, 0.37–0.85 % at k = 20), so §2 can publish a
setup-corrected multiplier beside the raw one instead of a caveat (§1). Validity is enforced per
timed INTERVAL and **fails closed on an unreadable probe as well as an over-ceiling one**, with the
run asserting that every arm was gated (§1).

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
| Commit | **`562f14aa`** (`issue-2043-reconcile-overlap-multiplier`), `bench` profile (release + debuginfo). **Not an ancestor of the merged commit** (the branch was rebased after this run, then squash-merged, so no branch SHA is reachable from `main`) — but the instrument is **byte-identical** at both: all seven bench files (`benches/reconcile_overlap.rs`, `benches/fixtures/{mod,multigen,validity_guard}.rs`, `benches/README.md`, `benches/perf-gate.json`, `cqlite-core/Cargo.toml`) have the same blob SHA at `562f14aa` and at the certified head. The commit is **preserved on origin** so this citation stays obtainable — fetch and check it out to build exactly the instrument that produced every number below:<br>`git fetch origin 'refs/measurements/*:refs/measurements/*'`<br>`git checkout refs/measurements/issue-2043-run-562f14aa` |
| Toolchain | rustc 1.97.1 (pinned `rust-toolchain.toml`) |
| Criterion | 20 samples/arm, 1 s warm-up, ≥5 s measurement, **`SamplingMode::Flat`**; medians reported |
| Run 1 | **2026-07-26T17:47:42Z → 17:51:37Z**, run-start load1m **0.14**, **max per-interval foreign CPU 0.726 cores**, 633 gated intervals, **27/27 arms gated** |
| Run 2 | **2026-07-26T17:54:07Z → 17:58:00Z**, run-start load1m **0.51**, **max per-interval foreign CPU 0.946 cores**, 632 gated intervals, **27/27 arms gated** |
| Validity guard | Two tiers, both fail closed — **including when the probe itself is unreadable**. **(1)** run-start `load1m` ≤ **2.00**. **(2)** per **timed INTERVAL** (one Criterion sample batch, via `iter_custom`), the **foreign** (not-this-process) CPU busy over that interval ≤ **1.00 core** of 16, computed as `/proc/stat` busy minus this process's own `utime+stime`; the gate is the per-interval **MAXIMUM** and the mean is reported as context. A per-arm mean would hide the events that void a sample (a 0.5 s four-core burst averages to ~0.33 cores over a ≥6 s arm); at 250 ms intervals the same burst reads ~4 cores in the intervals it lands in. An **unreadable** probe PANICS exactly like an over-ceiling one, and every arm must clear `end_arm` (enough gated intervals, ≤25 % of its timed wall clock in intervals too short to resolve, and this process's own ticks provably advancing) — then the run asserts `arms_gated == arms`, so a skipped or unavailable sample can never leave an ungated Criterion number in this record. An interval is gateable only above **160 `/proc/stat` ticks**, the count at which one stray tick is ≤10 % of the ceiling (`1/(0.10 × 0.0625)`, core-count-independent); under `Flat` sampling every PUBLISHED sample is ≥250 ms (~400 ticks) so only Criterion's discarded warm-up probes fall below it (43 per run, ≈2 % of timed wall clock). The `/proc/stat` capacity sum covers `user…steal` only — `guest`/`guest_nice` are excluded because the kernel already counts them inside `user`/`nice` (both `0` on this box). `cores` is the count of `cpuN` lines in `/proc/stat` — the **same source as the busy figure** — with `available_parallelism()` also printed and the ceilings scaled by the smaller of the two; here both are 16. The own-tick extraction is a pure `own_ticks_from(&str)` self-tested against captured `/proc/self/stat` samples before anything is measured. Tier 2 is not a per-arm loadavg check because `KWayMerger` runs one producer thread per generation, so the run's OWN parallelism drives `load1m` to 4.6–5.2 by k = 20 — a per-arm loadavg gate fails on the instrument, not on interference. `load1m` is still sampled per arm and reported: run 1 range 0.14–5.23, run 2 range 0.51–4.59, **almost entirely self-inflicted**. Both probes read `/proc` and are **Linux-only** (unavailable ⇒ fails closed). Both ceilings are **derived from the host's core count** and printed in the run header (`0.125 × cores` for tier 1, floor 0.5; `0.0625 × cores` cores for tier 2, floor 0.25 core). |
| Both runs | **VALID.** Peak per-interval foreign CPU across all 54 timed arms (1265 gated intervals) was **0.946 of 16 cores (5.9 %)**; the whole-run means were 0.166 and 0.162 cores. |
| Discarded runs | **four, and none of them contributed a number.** (a) A **16-partition × 256-`ck`** sizing variant (17:05:49Z–17:10:14Z) was VALID but its *sizing* was rejected — see the note on the width knob below; its saturated anchor (3.02 µs/row) is quoted only as the measured evidence for that rejection. (b) Three runs (17:14Z, 17:34Z, 17:45Z) were **VOIDED by the tier-2 gate** at `1.02` / `1.05` / `1.09` cores of foreign CPU over **149 ms / 54 ms / 121 ms** intervals. Those windows are below the resolution floor the gate now enforces: at 16 cores a 54 ms interval advances only ~86 `/proc/stat` ticks, so a single stray background tick reads as 0.19 cores. The fix was the derived 160-tick floor plus `Flat` sampling (so no published sample is ever that short), NOT a looser ceiling — the ceiling is unchanged at 1.00 core. Nothing from any of the four runs is used. |
| Corpus | synthesized by `WriteEngine` (k flushed, uncompacted generations, exactly k `Data.db` asserted per arm, compaction explicitly disabled). `CQLITE_DATASETS_ROOT` is never consulted — the vendored corpus is single-generation and cannot supply k > 1. |
| Reproducibility | run-2-vs-run-1 median spread: **≤2.1 % on all 20 matrix arms at k ≥ 2** (worst `field_blend/k20`, 2.09 %); the k = 1 arms are the noisiest (worst `lww_overwrite/k1`, 6.1 %) — see §3 for why k = 1 is structurally the least stable point. |

Fixture per generation: **64 partitions × 64 clustering rows = 4096 clusters**, table
`(pk INT, ck INT, v0 TEXT, v1 TEXT, v2 INT, PRIMARY KEY (pk, ck))`. A fully-live reconciled row
carries **4 materialized cells** (`ck` + `v0` + `v1` + `v2`); this is observed through the public
`MergeStep` stream, not assumed, and the shape assertions fail if it changes.

### The arm width, and why per-drain setup is amortized rather than caveated

`KWayMerger::new_from_readers` spawns **one OS producer thread + one adapter open per generation**,
all of it inside the timed region. That is a fixed per-scan cost that *grows with k*, so against a
small row denominator it lands in the numerator of `cost(k)/cost(1)` and biases the published
multiplier **upward with k** — precisely the number §3's derate and #2822 consume as a *per-row*
figure, while a real compaction over millions of rows amortizes it to nothing. Owner decision
2026-07-26: re-measure with the setup amortized rather than ship a caveat. Two things were done, and
both are enforced by the instrument:

1. **The arm width was quadrupled to 4096 clusters/generation — on the PARTITION count (16 → 64),
   not the clustering width.** `MergeStep::Partition` materializes one whole partition's reconciled
   rows at a time, so rows-per-partition is itself a first-order determinant of per-row cost:
   growing `ck` would have changed the quantity being measured at the same time as it amortized
   setup. That is measured, not asserted — the rejected 16 × 256 variant moved the saturated anchor
   from 2.81 to **3.02 µs/row (+7 %, and +12.6 % at k = 20 alone**, where a partition batch reaches
   20 × 256 rows), which would have confounded the fix and pushed the anchor out of the ±50 % band.
   Scaling partitions leaves rows-per-partition-per-generation at 64 — the shape the earlier k-curve
   was banked on — and quadruples the denominator anyway.
2. **The residual is MEASURED per arm, never assumed.** Every arm prints a `SetupCensus`:
   `new_from_readers` alone, construct-then-teardown, and the full drain. Measured residual setup
   share (mean of both runs): **0.20–0.24 % at k = 1** and **0.37–0.85 % at k = 20** (worst single
   arm/run 1.11 %). At the pre-fix width it was 0.65–0.86 % at k = 1 against 2.4–4.8 % at k = 20 —
   a ~2.3 % upward bias on the k = 20 multiplier. §2 therefore publishes the **setup-corrected**
   multiplier (each arm's measured `construct` subtracted) beside the raw one; they agree to
   **≤0.6 %** everywhere, which is what "amortized" means here quantitatively.

## 2. The measured k-curve

Per **delivered (output)** row, mean of the two runs' Criterion medians. Each cell is
`ns/row / cost(k)/cost(1) (setup-corrected) / D`. `cost(k)/cost(1)` is the raw multiplier the spec
asks for and the parenthesised figure is the same ratio with each arm's **measured** per-drain setup
subtracted (§1); `D` is the **pipeline-matched** derate (§4) — `cost_mix(k) / cost_disjoint(k)`, i.e.
the same k, same producer count, differing only in whether rows collide. `o` = observed
input-rows/output-rows (the overlap factor).

| mix | k=1 | k=2 | k=5 | k=10 | k=20 |
|---|---|---|---|---|---|
| **disjoint** (control) | 5435 / 1.00 (1.00) | 3363 / 0.62 (0.62) | 2851 / 0.52 (0.52) | 2738 / 0.50 (0.50) | 2829 / 0.52 (0.52) |
| **lww_overwrite** | 5191 / 1.00 (1.00) / D 0.96 | 6925 / 1.33 (1.33) / D 2.06 | 11350 / 2.19 (2.19) / D 3.98 | 18455 / 3.56 (3.55) / D 6.74 | 33953 / 6.54 (6.50) / D 12.00 |
| **tombstone** | 5392 / 1.00 (1.00) / D 0.99 | 6946 / 1.29 (1.29) / D 2.07 | 11564 / 2.14 (2.14) / D 4.06 | 18959 / 3.52 (3.51) / D 6.92 | 35260 / 6.54 (6.51) / D 12.46 |
| **ttl_expiring** | 5222 / 1.00 (1.00) / D 0.96 | 6795 / 1.30 (1.30) / D 2.02 | 11383 / 2.18 (2.18) / D 3.99 | 18631 / 3.57 (3.57) / D 6.80 | 34246 / 6.56 (6.52) / D 12.10 |
| **field_blend** | 5327 / 1.00 (1.00) / D 0.98 | 6128 / 1.15 (1.15) / D 1.82 | 9238 / 1.73 (1.73) / D 3.24 | 13300 / 2.50 (2.49) / D 4.86 | 24503 / 4.60 (4.57) / D 8.66 |

**The setup correction never moves a multiplier by more than 0.6 %** (largest gap: `lww_overwrite`/k
= 20, 6.54 → 6.50). The amortization concern is therefore *closed by measurement*, not by argument:
at this width the raw `cost(k)/cost(1)` IS the per-row multiplier §3 wants, to within the printed
residual.

**Producer-count control** (identical 8192 output rows, 32 768 live cells and `o = 1` on both sides;
only the producer-stream count differs): `producer_control/p1` (ONE double-width generation) =
**4862 ns/row**, `producer_control/p2` (TWO standard-width generations) = **3402 ns/row** ⇒
**1.43× for the 1→2 producer step alone.** (`p2` is fixture-identical to `disjoint/k2`, and lands
within 1.2 % of it — an internal reproducibility check.)

Overlap factor `o` per arm (observed, not assumed): `lww_overwrite` / `tombstone` / `ttl_expiring`
= exactly k; `field_blend` = 1.00, 1.75, 4.00, 7.75, 15.25 (its 25 % singleton population dilutes
`o`); `disjoint` = 1.00 at every k, by construction.

Collision/deletion observables printed by every arm (`live_cells`, `tombstone_cells`,
`row_tombstones`, `coexisting_row_deletions`, `collisions_per_row`, `load1m`, `setup_share_pct`,
`foreign_cpu_cores max/mean`) are in the bench's stdout. **Purge counts are zero by construction,
not unmeasured:** this is a READ merge, so `gc_before_secs = None` and `purge_safe = false`, which
makes the gc-grace purge stage a strict no-op. The observable deletion work is the tombstone/expiry
counts, and those are reported.

### Three findings that fall straight out of the table

1. **SSTable COUNT is free; row-level DUPLICATION is what costs.** The `disjoint` control is flat in
   k once the pipeline saturates — 2851 / 2738 / 2829 ns per row at k = 5 / 10 / 20 (spread 4.1 %,
   no monotone trend, and k = 20 is *below* k = 5). Reading 20 generations instead of 5 costs
   nothing per delivered row when no cluster spans two of them. Every bit of the multiplier comes
   from clusters that appear in more than one generation.
2. **TTL expiry at a pinned `now` is free.** `ttl_expiring` tracks `lww_overwrite` to within 0.9 %
   at k = 20 (34 246 vs 33 953 ns) with a marginal cost per extra input row of 1524 vs 1507 ns
   (+1.1 %), while converting exactly one expiring cell per row into a tombstone.
   `expire_ttl_cells` is not a measurable cost centre — both gaps are inside the k = 20 arms'
   run-to-run spread (≤1.2 %).
3. **Deletion collisions cost slightly MORE than plain overwrite, not less.** `tombstone` is the
   most expensive mix at every k ≥ 5: 35 260 ns/row at k = 20, **+3.9 %** over `lww_overwrite`, with
   a marginal cost of 1580 vs 1507 ns per extra input row (+4.8 %) — a gap ~3× the arms' run-to-run
   spread. Resolving a row deletion against surviving newer cells and carrying it forward (issue
   #932), plus per-column tombstone-vs-live ties, is *additional* work on top of LWW — it does not
   replace it. (The pre-correction fixture reported the opposite, because its shadowed clusters had
   already collapsed to cell-less row tombstones at flush time and were therefore cheap to merge.
   That finding was an artifact and is withdrawn.)

## 3. Anchor validation — and the k = 1 deviation, decomposed

**Stated band: ±50 % of the published anchor, applied to the comparable quantity.** The anchor is
the **saturated `disjoint` control** (mean of k = 5/10/20), per the amended spec requirement
(`openspec/changes/reconcile-overlap-multiplier/specs/reconcile-overlap-measurement/spec.md`, owner
decision 2026-07-26): the anchor as originally written (`disjoint`/k = 1) is a whole-drain WALL time
produced by a SINGLE producer stream, which is not the quantity the published figure reports, so
comparing them measured two different things. **An out-of-band SATURATED anchor still voids a run.**

| Quantity | Measured | Published comparable | Ratio | In band? |
|---|---|---|---|---|
| **ANCHOR** — `disjoint` at pipeline saturation (mean of k = 5/10/20) | **2.81 µs/row** (setup-corrected 2.80) | ~2.0 µs/row (`phase2-verify-stage2.md:226-232`; equal to Phase-0's ~500–540 k rows/s single-stream WALL anchor, `phase0-scan-cost-breakdown-2026-07.md:66-72`) | **1.40×** | **YES** |
| `disjoint`/k = 1, reported as an explained deviation | **5.44 µs/row** | ~2.0 µs/row | **2.72×** | n/a — see below |

The anchor is **unchanged by the 4× width increase** (2.82 → 2.81 µs/row), which is the point of
having scaled the partition count rather than the clustering width (§1): the amortization fix moved
the setup residual by 4× and the measured quantity by 0.4 %.

**The k = 1 deviation is now fully decomposed by measured arms, with no residual.** `disjoint`/k = 1
is **1.937×** the saturated value, and that factor factorizes exactly:

| Step | Arms compared | Factor | What changes |
|---|---|---|---|
| Scan width | `disjoint/k1` (4096 rows) → `producer_control/p1` (8192 rows) | **1.118×** | rows only — per-iteration fixed cost amortizes |
| Producer count 1 → 2 | `producer_control/p1` → `producer_control/p2` | **1.429×** | producer streams only (rows, cells, `o` held fixed) |
| Producer count 2 → 5+ | `producer_control/p2` → saturated `disjoint` | **1.212×** | producer streams (and width) only |
| **Product** | | **1.937×** | = the measured `disjoint/k1` ÷ saturated ratio (1.937×) |

- The **producer-count-only** component is `1.429 × 1.212 = 1.733×`. Phase 0's own stage split
  predicts **1.79×** for exactly this transition: producer-side work (stage 1 IO 0.0 % + stage 2
  decode 9.7 % + stage 3 materialize 4.5 % + stage 4b fan-in park/wake **49.9 %**) is **64.1 %** of
  scan CPU and coordinator-side work (4a reconcile 32.5 % + 5 Arrow 1.0 % + 6 Flight 0.2 % + 7 other
  2.2 %) is **35.9 %**, so a producer-bound single-stream drain should cost 64.1/35.9 = 1.79× a
  coordinator-bound one. **Measured 1.733× against a 1.79× prediction** (−3.2 %).
- The saturated fit has **no per-iteration fixed cost left to amortize** (measured setup residual
  0.19–0.37 % on the `disjoint` k ≥ 5 arms), which is why only the k = 1 (and, weakly, k = 2) points
  carry a width term at all.

**Consequence for this record (stated rather than quietly chosen):** the harness is sound — the
saturated control lands at 1.40× the published anchor on a different CPU with 2× the cells per row —
but `cost(k)/cost(1)` uses a base inflated ~1.94× by single-producer handoff and narrow-scan fixed
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
| `disjoint` + `ttl_expiring` — **the headline fit** (lowest residual) | **1689** | **1127** | **15.9 %** |
| `disjoint` + `lww_overwrite` | 1674 | 1143 | 16.2 % |
| `disjoint` + `lww` + `tombstone` | 1706 | 1113 | 16.6 % |
| `disjoint` + `field_blend` | 1549 | 1267 | 19.2 % |

**The headline fit changed from `disjoint` + `field_blend` to `disjoint` + `ttl_expiring`** on the
same stated rule (lowest max residual) applied to the new numbers. That is an improvement in method
as well: `field_blend` is the arm whose `o` differs from its `k`, so excluding it from the fit makes
the point-by-point check below a genuine **out-of-sample** validation rather than a partly circular
one. Re-fitting on the setup-corrected medians moves the coefficients by <1 % (`p` 1677, `q` 1131),
so the correction does not shift the model either.

Per-mix marginal cost of one extra colliding generation-row (k = 5 → k = 20): **1580 ns**
(`tombstone`), **1524 ns** (`ttl_expiring`), **1507 ns** (`lww_overwrite`), **1357 ns**
(`field_blend`). Against a fresh delivered row at **2738–2851 ns**, an extra colliding copy costs
**0.48–0.58×** a fresh row.

**The gen-overlap derate, as a function of the overlap factor `o` = generations per delivered row.**
Every figure below uses the **headline `disjoint` + `ttl_expiring` fit**, named explicitly so the
arithmetic is checkable:

```
D(o) = (q + p·o) / (q + p)     with p = 1.689 µs/input-row, q = 1.127 µs/delivered-row
                                (o=1 ⇒ 2.82 µs/row FITTED, vs 2.81 µs/row MEASURED saturated control)
```

| `o` | 1.0 | 1.25 | 1.5 | 1.75 | 2.0 | 3.0 | 4.0 |
|---|---|---|---|---|---|---|---|
| **D(o)** — headline fit | 1.00 | 1.15 | 1.30 | 1.45 | 1.60 | 2.20 | 2.80 |
| range across all four fits | 1.00 | 1.14–1.15 | 1.28–1.30 | 1.41–1.45 | 1.55–1.61 | 2.10–2.21 | 2.65–2.82 |

The closed form is the **saturated asymptote**, and it is validated **out of sample** against the arm
it was not fitted on — `field_blend`, whose `o` differs from its `k`:

| `field_blend` arm | k=2 | k=5 | k=10 | k=20 |
|---|---|---|---|---|
| observed `o` | 1.75 | 4.00 | 7.75 | 15.25 |
| measured `D` | 1.82 | 3.24 | 4.86 | 8.66 |
| model `D(o)`, headline fit | 1.45 | 2.80 | 5.05 | 9.55 |
| model vs measured | −20.4 % | −13.6 % | +4.0 % | +10.2 % |

**The model crosses over: it reads LOW at small k (−20.4 % at k = 2, −13.6 % at k = 5) and HIGH at
large k (+10.2 % at k = 20).** Low k because the drain is still producer-bound (§3), which the
saturated fit excludes by construction; high k because `field_blend`'s marginal cost per colliding
row (1357 ns) is the lowest of any mix, i.e. its deep-overlap clusters are cheaper than the fit's
`p`. This matters for how the model is applied: a field scan has a **high producer count and a low
`o`** (k ≈ 4–8 SSTables, most rows singleton), which is squarely the saturated, low-`o` regime — the
regime where the model errs **low** (conservative). The `o ≥ 4` overshoot is outside the band §5
substitutes and is disclosed rather than smoothed away.

## 5. Verdict for §3 — the gen-overlap term

**The 1.1–1.5× band is confirmed as correctly sized *for its implied overlap*, and is now
expressible as a function instead of a guess: `D = 1.1×` ⇔ `o ≈ 1.17`, `D = 1.5×` ⇔ `o ≈ 1.83`, so
the existing band means `o ∈ [1.17, 1.83]`.** Three changes to how the term should be used:

1. **It is a duplication term, not an SSTable-count term.** Measured: SSTable count alone is free
   (§2 finding 1). The derate is driven purely by the fraction of delivered rows assembled from more
   than one generation — i.e. by the *overwrite/update rate relative to compaction cadence*. This
   sharpens (and is consistent with) the graveyard entry that killed "RF=3 as a reconcile
   multiplier": the surviving term is real, but it is narrower than "generation overlap" suggests.
2. **The band's floor is exact.** An insert-once table (time-series/append-only, a primary connector
   target) has `o = 1.0` and therefore **`D = 1.00`, not 1.1×** — for those tables the gen-overlap
   term should be dropped from the derate entirely, not carried at its floor.
3. **The band's ceiling is optimistic for update-bearing tables.** At `o = 3–4` — a normal STCS
   "SSTables per read" p99 for a table taking overwrites — `D` is **2.20–2.80×**, well outside
   1.1–1.5×. The derate should carry `o` explicitly rather than a single band.

### The field `o` this substitutes — **ASSUMPTION, NOT MEASUREMENT**

> **`o_field = 1.25–1.5` (central 1.35) ⇒ D = 1.15–1.30 (central ~1.21).**
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
**0.47 µs of the 2.81 µs/delivered row at `o = 1`**.

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
  entirely on the `p` side (1.69 µs per extra input row — decode + heap + collision resolve), which
  a *singleton* fast-path cannot touch by construction. At `o = 2` the drain is **4.51 µs/delivered
  row** (headline fit), so even a hypothetically-eligible cluster would yield only
  `1/(1 − 0.47/4.51) = 1.12×`.

Combining both — `S(o) = 1 / (1 − 0.47·f(o) / (q + p·o))` with `f(o) = max(0, 2 − o)` — gives the
**k-band that makes each ruling correct:**

| Field cluster shape | `f` | **L3 speedup** | Whose ruling holds |
|---|---|---|---|
| `o = 1.0` (insert-once) AND no TTL, no tombstone load | 1.00 | **1.20×** | **P2:stage2** — headline lever |
| `o ≈ 1.1` | 0.90 | 1.16× | P2:stage2 |
| `o ≈ 1.25` | 0.75 | 1.12× | between the two |
| `o ≈ 1.35` (the §5 **assumed** central point) | 0.65 | **1.10×** | between the two |
| `o ≈ 1.5` | 0.50 | 1.07× | **P2:row-engine** — WEAKENED |
| `o ≈ 1.75` | 0.25 | 1.03× | **P2:row-engine** |
| `o ≳ 2.0`, **or any `o` on a table with a TTL'd queried column or a tombstone-bearing column set** | ≈ 0 | **≈ 1.00×** | L3 is worthless |

**Step 5 — the additional decisive gate, from §2 findings 2 and 3.** TTL costs the merge ~nothing
(`ttl_expiring` ≡ `lww_overwrite` to 0.9 %, inside run-to-run spread), yet a TTL-bearing cluster is
*ineligible* for L3. So on a table with a TTL'd queried column, L3 is a **pure eligibility loss with
no compensating cost saving** — `f → 0` regardless of `o`, and L3's value collapses independently of
overlap. Deletion load is worse than neutral: it *raises* merge cost 3.9 % over plain overwrite
(finding 3) while also disqualifying the cluster. Any field table carrying TTL or tombstones settles the disposition on its
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
  `ttl_expiring`** fit (`p = 1689`, `q = 1127` ns), named at each use. The cross-fit spread is
  ≤2.3 % at `o ≤ 2` and ≤6.4 % at `o = 4`, and is shown in §4's table.
- The four fits' max residual is **15.9–19.2 %**, higher than a 2-parameter model over a
  three-decade row range deserves credit for. The residual is dominated by the `disjoint` arms,
  whose output row count scales with k while the collapsing mixes' does not; the model is used as
  a *shape* (the `p`-vs-`q` split) and validated point-by-point in §4, never as a precision fit.
- The instrument measures **wall-clock whole-drain** cost, not stage-attributed CPU. It cannot
  decompose the residual between its 2.81 µs/row saturated control and Phase-0's 2.0 µs/row stage-4a
  CPU figure (different corpus shape — 4 cells/row vs 2 columns — and different CPU). That
  decomposition belongs to a profiler run on the rig, not here.
- `field_blend`'s composition (25 % singleton, 25 % per-column blend, 25 % tombstone, 25 % expiring)
  is a **modelling choice**, not a measured field distribution. It is now the **out-of-sample
  validation** arm rather than part of the headline fit, and is never the source of `o_field`.
- **Per-drain SETUP is amortized and measured, not assumed away.** The timed region still contains
  `new_from_readers` (k thread spawns + k adapter opens) deliberately — hoisting construction out
  would hand every generation a pre-buffered head start, because each producer starts filling its
  256-row bounded channel the moment it is spawned, which is a worse distortion than the one being
  removed. Instead the width was raised 4× (on the partition count, §1) and every arm prints its
  measured setup share: **0.20–0.24 % at k = 1, 0.37–0.85 % at k = 20**. §2 publishes the
  setup-corrected multiplier alongside the raw one; they differ by ≤0.6 %.
- Every number in this record comes from the two runs in §1 at commit **`562f14aa`**, including the
  producer-count decomposition in §3 (`producer_control/{p1,p2}` are arms of the matrix run, not a
  side experiment). Nothing is extrapolated beyond the fitted curve. **`562f14aa` is the exact
  instrument that ran** — no post-measurement instrument change is being papered over here; four
  earlier runs produced no numbers and are all itemised in §1, including the rejected 16 × 256
  sizing variant whose 3.02 µs/row anchor is the measured basis for scaling partitions instead of
  clustering width.
- **What changed versus the previous (1024-cluster) revision of this record, and what did not.** The
  saturated anchor moved 2.82 → **2.81 µs/row** and the k = 1 decomposition 1.916 → **1.937×**
  (product still exact, producer-only component 1.733× against Phase-0's 1.79× prediction). The
  headline fit changed from `disjoint + field_blend` (`p` 1644, `q` 1180) to
  `disjoint + ttl_expiring` (`p` **1689**, `q` **1127**) under the same lowest-residual rule, making
  `D(o)` marginally steeper (at `o` = 1.5/2/4: 1.29/1.58/2.75 → **1.30/1.60/2.80**) and the
  band-to-`o` mapping `o ∈ [1.2, 1.9] → [1.17, 1.83]`. **No verdict changed:** SSTable count is
  still free, TTL is still free, deletion still costs slightly more (+3.0 % → +3.9 %), the 1.1–1.5×
  band is still correctly sized for `o ≈ 1.2–1.8` with an exact `D = 1.00` floor at `o = 1`, and L3
  still lands at **~1.10×** at the assumed central `o ≈ 1.35` (range 1.07–1.12× over
  `o ∈ [1.25, 1.5]`) — so the recommendation to keep L3 off the headline lever list stands unchanged.
