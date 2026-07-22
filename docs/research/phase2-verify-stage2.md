# Phase 2 — Adversarial adjudication of the Stage-2 question (≥600k rows/s/pod)

**Date:** 2026-07-21 · **Status:** research (uncommitted) · **Author:** Phase-2 adversarial adjudicator
**Scope:** READ-ONLY. Anchored to Phase 0 (`phase0-scan-cost-breakdown-2026-07.md`) and the Phase-1
memos (`phase1-1`…`phase1-8`). The reconcile cost claims are read off the code
(`cqlite-core/src/storage/write_engine/merge/reconcile.rs`, `.../merge/mod.rs`).

> **The one-line verdict.** On a **fixed row pipeline** (the per-row channel deleted, alloc
> controlled, splits parallelized), a 4-vCPU pod lands **~360–500k rows/s/pod central, ~240–670k
> across the honest band**. That makes **B3 Stage 2 (≤3 s through Trino ≈ 216–323k rows/s/pod)
> REACHABLE-AT-COST on a row feed** — overruling Phase 1-7 — with the limiting factor moved off the
> row engine onto **the Trino coordinator floor + cold-start latency inside the 3 s budget**. It
> makes **A4 Stage 2 (600k rows/s/pod server-direct) NOT reliably reachable** on the pure row engine
> — reachable only at the optimistic corner — with the limiting factor being **C(N): a 4-vCPU pod is
> 2 physical cores + hyperthreads, not 4× parallelism**, plus reconcile machinery. **Columnar option
> (a) does NOT earn a 0.17 slot on throughput grounds** — it is Stage-3 prep behind the row levers.

---

## 0. Packet (read this, then the audit backs every number)

| Target | Per-pod requirement | Verdict | Limiting factor |
|---|---|---|---|
| **Stage 1** (A4 100k / B3 ≤10 s ≈ 65k) | 65–100k rows/s/pod | **REACHABLE** | none binding; L1 batch-channel + fan-out clears it comfortably |
| **B3 Stage 2** (≤3 s through Trino) | **216k** (no floor) → **323k** (1 s floor) → 647k (2 s floor) | **REACHABLE-AT-COST** (row feed) | **NOT the row engine** — the Trino coordinator floor eating the 3 s budget + cold-start IO latency; needs floor ≤ ~1.2 s AND warm/prefetched IO AND L1/L3 landed |
| **A4 Stage 2** (server-direct) | **600k rows/s/pod = 150k/core** | **NOT reliably reachable** on the pure row engine (optimistic-corner only) | **C(N) < 4** (4 vCPU = 2 physical + HT → ~2.5–3.5× effective) + reconcile machinery (measured ~2 µs/row) |

**Surviving multiplier stack (bottom-up from the fixed single-thread pipeline; the honest band):**

```
Fixed M1 single-thread pipeline (channel deleted)      285k rows/s/core   (P1.2, 3.5 µs/row)
  ÷ field derate  1.5–3×  (central 2×)                → 95–190k /core  (central 143k)
  × C(N) on 4 vCPU  2.5–3.5×  (central 3×)            → 238–670k /pod   (central ~420k)
                                                         ├─ A4 Stage 2 600k: only the optimistic corner
                                                         └─ B3 Stage 2 216–323k: cleared at central
```

**The derate I will defend: ~1.5–3× (central 2×)** — **NOT** Phase 1-2's 3–5×. Decomposed in §1.
**The C(N) I will defend: 2.5–3.5× (central 3×)** — **NOT** the flat 4× both Phase 1-2 and Phase 1-7
assumed. Decomposed in §3.

---

## 1. Audit of the field derate — is 3–5× evidenced or hand-waved?

Phase 1-2 §1.4 lists five derate factors and multiplies to **3–5×**. Auditing each against the code
and the dedicated Phase-1 IO/parallelism agents, **the two largest stated components are overstated,
and one is misattributed.** The defensible derate is **~1.5–3×.**

### 1a. RF=3 reconcile "2–3× more input cells" — **OVERRULED (misattribution).**

Phase 1-2 makes RF=3 the big term: *"reconcile ~2–3× more input cells to compare/shadow across
replicas×gens."* **This conflates a cross-node/connector concern with a per-node one.**

- **What the reconcile actually costs, from the code.** `resolve_cell_winners`
  (`reconcile.rs:191`) iterates **every `MergeEntry` in a clustering group and every cell in each**,
  doing one `HashMap::entry()` + one `cell_wins()` compare per cell. Cost is **O(total input cells
  across the entries that share a clustering key)** — i.e. it scales with **how many SSTable
  generations on this node contain the same partition/row**, not with RF.
- **RF=3 does not multiply per-node reconcile input.** A partition replicated to 3 nodes appears
  **once per generation on each node's disk**, not three times on one node. The k-way merge inputs
  are **this pod's SSTable readers** (`from_readers::drive_query_stream`), i.e. the local generations
  for the ranges this pod owns. RF governs *how many nodes hold a copy*, and the connector already
  **pins each token range to one replica** (`CqliteFlightSplitManager.buildSplits`), so RF=3 is
  deduplicated at plan time. Phase 1-5's factor table agrees: **RF=3 ≈ 1.0–1.2× (fan-out/coordination
  only, not row work).** Phase 1-2's own memo even writes "replicas×gens" — the **×gens** is the real
  term; the **replicas×** is the error.
- **The real 4a derate is generation-overlap, and it is compaction-state-driven, single-digit,
  bounded.** Phase 0 ran **4 SSTables with disjoint keys → every cluster is a singleton** (1 entry),
  so it measured the *floor* of reconcile. Field data has genuine overlap (updates, tombstones,
  TTL), so a hot partition may sit in `k` generations. But `k` is **bounded by the compaction
  strategy**, not RF: LCS ≈ 1 overlapping SSTable per level; STCS ≈ a handful of size tiers.
  Append-only/time-series (the R12 `easy_stress` shape) ≈ 1 (no overlap). So the 4a derate is
  **~1× (append-only) to ~1.5–2.5× (update-heavy) on the 32.5% reconcile slice ⇒ ~1.1–1.5× overall**,
  not a blanket 2–3×.

**Verdict:** RF=3 is **not** a reconcile multiplier. Replace Phase 1-2's "RF=3 → 2–3×" with
"generation-overlap → 1.1–1.5× overall, corpus-dependent, ~1× on the append-only R12 corpus."

### 1b. LZ4 decompress — **OVERRULED (overstated by both P1.2 and P1.5).**

- Phase 1-2: *"single-digit-to-low-double-digit %."* Phase 1-5: **"1.3–2×."** Both are wrong high.
- **Phase 1-3 — the agent who actually modeled it — says LZ4 decode adds `~0.1%–1%` CPU** and the
  engine "stays CPU-bound on the Phase-0 per-row coordination." Independent per-row check: ~40–50 B
  packed/row ÷ LZ4 decompress ~2–3 GB/s ≈ **16–25 ns/row** against a ~3.5 µs/row pipeline ≈ **~0.5–1%
  ⇒ ~1.01×.** Even a pessimistic 1 GB/s LZ4 is ~1.4%.
- **Verdict:** LZ4 derate ≈ **1.01–1.05×**, negligible. Phase 1-5's 1.3–2× must not enter the stack.

### 1c. Wide-row per-cell decode/materialize — **UPHELD, but do not double-count it.**

decode (9.7%) + materialize (4.5%) + arrow (1.0%) + per-cell alloc scale with **cells/row**, and so
does reconcile (`resolve_cell_winners` is per-cell). A field row with 4–8 cells vs the profiled 2
cells is genuinely ~1.5–2.5× heavier per row. **BUT:** the R12 field baseline (10.6k rows/s/pod) was
measured on the **narrow 2-column `easy_stress.keyvalue` corpus** — the *same shape* as Phase 0. So
for the corpus that actually anchors the goal, the wide-row derate is **~1×**. Wide-row derate only
bites if the target corpus is wider than R12; if you invoke it, you must also lower the rows/s target
proportionally (wide rows are fewer rows at the same MB/s). **In the bottom-up stack it is ~1× for
the R12 shape; carry it only as an explicit, corpus-named factor, never as a silent 2×.**

### 1d. Slower cores (M1 → i4i.xlarge vCPU) — **UPHELD (~1.3–1.5×), and it hides a bigger issue (see §3).**

An i4i.xlarge vCPU is **one hyperthread of an Ice Lake core**; M1 Pro is a wide ~3.2 GHz core. Single-
thread M1-vs-one-Ice-Lake-vCPU ≈ **1.3–1.5×**. Defensible as stated. The trap is not the per-core
speed — it is that "4 vCPU" is **2 physical cores** (§3).

### 1e. Cold cache — **RECLASSIFIED: a latency term, not a throughput derate.**

Phase 1-2 lists cold cache as "IO-wait (wall, not CPU)." Phase 1-3 (dedicated) is sharper: **cold
sequential bandwidth needed is ~10–350 MB/s, an order of magnitude under i4i NVMe; IO never becomes
the throughput binding constraint below row rates far above 600k.** Cold IO owns exactly **cold-start
*latency* (the B4 ≤3 s goal)**, which matters for the ≤3 s B3 Stage-2 budget (§2) but is **not a
sustained-throughput multiplier**. Remove it from the throughput derate; carry it as a latency risk
against the B3 3 s budget.

### 1f. Reconstructed derate

| Factor (P1.2) | P1.2 stated | Adjudicated | Basis |
|---|---|---|---|
| RF=3 reconcile | 2–3× | **1.1–1.5× (gen-overlap, not RF)** | reconcile.rs:191 is per-node-per-cell; connector pins 1 replica (P1.5) |
| LZ4 | single→low-double-digit % | **1.01–1.05×** | P1.3 (0.1–1%); per-row math |
| Wide rows | included | **~1× on R12 corpus** (else explicit) | R12 = narrow keyvalue = Phase-0 shape |
| Slower cores | 1.3–2× | **1.3–1.5×** | M1 vs 1 Ice-Lake HT |
| Cold cache | in derate | **removed** (→ latency term) | P1.3: IO not throughput-binding < ~600k |
| **Product** | **3–5×** | **~1.5–3× (central 2×)** | |

Phase 1-2 reached roughly the right *answer* (240–400k/pod) partly by canceling two errors: an
**inflated derate (3–5×)** against an **inflated C(N) (perfect 4×)**. Correcting both — derate down
to 1.5–3×, C(N) down to 2.5–3.5× — lands in the **same band (central ~420k)** with defensible parts.

---

## 2. B3 Stage-2 arithmetic — and why Phase 1-7's central verdict is OVERRULED

Phase 1-7 declared **"B3 Stage 2 ≤3 s NOT credible on a row feed."** Its reasoning was
"215k/pod = 54k rows/s/core = ~1.6 µs/row = vectorized territory." **Phase 1-2 already caught the
first half of the error** (1.6 µs/row conflates pod and core). Re-deriving with the correct per-core
framing **inverts the conclusion.**

- **B3 Stage 2's per-pod bar is LOWER than A4 Stage 2's.** ≤3 s for the R12 scan (1.94M rows, 3 pods)
  = **216k/pod** (no Trino floor) → **323k/pod** (1 s floor) → 647k/pod (2 s floor). Per core (÷4):
  **54–81k/core = 12.4–18.6 µs/row/core** at floors ≤1 s. That is **more generous** than A4 Stage 2's
  6.67 µs/row/core.
- **The fixed field-derated row pipeline does 95–190k rows/s/core (5.3–10.5 µs/row/core).** That
  **exceeds** B3 Stage 2's 54–81k/core requirement at floors ≤1 s. **The row engine per-core rate is
  not the binding constraint** — Phase 1-7 named the wrong limiter.
- **What actually gates B3 Stage 2 ≤3 s:**
  1. **The Trino coordinator floor (0.2–2 s) eating the 3 s budget.** At a ~2 s floor only 1 s of
     scan remains → 647k/pod needed → A4-Stage-2-hard. At ~0.5–1 s floor → 260–323k/pod → cleared by
     the central pipeline. **This is the swing factor, it is measurable, and it is a Trino/plan-shape
     property, not a row-engine property.**
  2. **Cold-start IO latency inside the 3 s budget** (§1e / P1.3: cold start is the one thing IO owns;
     the B4 ≤3 s cold-start goal exists for exactly this). A cold first scan can spend seconds before
     rows flow; that competes with the Trino floor for the budget. Sustained bandwidth is fine.
  3. **Achieving C(N) parallelism + fixed drain** (L1/L6 batch channel; §3).

**Adjudicated B3 Stage 2 verdict: REACHABLE-AT-COST on the row feed**, gated on (a) Trino floor
≤ ~1.2 s for this scan shape, (b) warm/prefetched IO so cold-start latency does not consume the
budget, (c) L1 batch-channel + fan-out-past-drain landed. **The limiting factor is the Trino floor +
cold-start latency, NOT "the row engine can't do 54k/core."** Phase 1-7's structural framing (Trino
is a *latency floor*, the row *feed* is the throughput ceiling — not Trino-the-tax) is **upheld and
is the best framing in the set**; only its Stage-2 feasibility call is overruled.

*(Stage-1 B3 ≤10 s ≈ 65k/pod is reachable with margin — uncontested across all agents.)*

---

## 3. C(N) on a 4-vCPU pod — the factor both P1.2 and P1.7 got wrong

Both memos wrote "4 vCPU → 4×." **Two independent reasons it is not 4×:**

1. **An i4i.xlarge is 2 physical cores.** AWS vCPU = one hyperthread; 4 vCPU = 2 Ice-Lake cores with
   SMT. Two physical cores give ~2×; the two extra hyperthreads add ~0.5–0.75× **only** because the
   reconcile is pointer-chasing (HashMap `winners`, BinaryHeap `refill_heap`) and thus
   latency-bound/SMT-friendly. Net **~2.5–3.5× effective, central ~3×** — never 4×.
2. **Phase 1-6's measured saturation.** P1.6's model is
   `per_pod = min(N_admitted, N_drain_sat≈8, N_mem≈20) × per_stream × C(N)`, with **C(N) ≤ 1 and
   falling**, and it states outright: *"the width is ~8 (drain-bound), not 4 vCPU × anything… 4
   streams ≠ 4× because Phase-0's 55% kernel park/wake tax contends."* On **today's** code the pod
   is **drain-capped well below core count** (and a 4-vCPU pod's `N_drain_sat` is *lower* than the
   10-core local rig's 8). The **only lever that raises both `N_drain_sat` and `C(N)` is L6/L1 —
   batching the per-row fan-in `sync_channel`** (Phase 0 finding #1). So the entire "fixed row
   pipeline parallelizes" premise **depends on L1 landing**; without it, naive 4× fan-out just
   deepens the #2600 egress queue.

**Reconciling Phase-0's 55% kernel with C(N):** the 55% kernel park/wake is a *single-stream artifact
of thread-per-input*. Delete the per-row channel (inline/batched merge, L1/L2) and it collapses, so
C(N) is **not** bounded by that 55% *once fixed* — it is then bounded by the 2 physical cores + SMT.
This is why the fix and the scaling are the same work: **L1 batch-channel is simultaneously the
biggest single-stream lever (49.9% stage 4b) and the thing that makes C(N) approach ~3× instead of
<1.**

**Defended C(N): 2.5–3.5×, central 3×, and only post-L1.** Pre-L1 the pod is drain-capped and worse.

---

## 4. Audit of Phase 1-8's "150k/core is ⅓-of-Scylla, credible" — UPHELD per-core, OVERRULED as a pod given

- **Per core, 150k/core is defensible and conservative.** It is ~⅓ of Scylla's ~428k/core LSM rate
  and ~⅓ of CQLite's own measured 500k single-stream. The fixed field-derated pipeline (§0) brackets
  it (95–190k/core). Phase 1-8's per-core *envelope* (150–450k/core narrow) is **upheld**.
- **But ⅓-of-Scylla is a *generous* comparison, and Phase 1-8 says so itself.** Scylla's 428k/core is
  **aggregation pushdown — it never materializes or ships a row.** CQLite materializes every cell,
  reconciles k-way with tombstones, Arrow-encodes, and ships every row. So Scylla is an **upper
  bound**, not a peer rate; "⅓ of an upper bound" is not the same as "⅓ of a comparable workload."
  The per-core number survives on CQLite's *own* Phase-0 evidence (500k measured, ~⅔ of it deletable
  waste), not on the Scylla analogy — the Scylla line is corroboration, not proof.
- **What Phase 1-8 under-weights: the pod product.** It flags "the risk is scaling to 4 concurrent
  cores" but does not quantify it. Quantified (§3): **150k/core × C(N) 2.5–3.5 = 375–525k/pod
  central**, i.e. **short of 600k at the center**, reaching it only at the optimistic corner. So
  Phase 1-8's "**600k/pod credible and conservative**" is downgraded to "**150k/core credible; 600k/pod
  is the optimistic corner, ~400–450k/pod central, because 4 vCPU is 2 physical cores.**"

**A4 Stage 2 verdict: NOT reliably reachable on the pure row engine.** Central ~420k/pod; 600k needs
the optimistic corner (derate ~1.5×: narrow + append-only + warm; C(N) ~3.5×: good SMT scaling; all
row levers landed). Limiting factors, in order: **C(N) < 4** (structural, 2 physical cores) and
**reconcile machinery** (§5). Columnar helps least here (§6).

---

## 5. The reconcile constant and the #2043 (WS7) harvest claim — PARTIALLY OVERRULED

Phase 1-2 §3.3 says the whole Stage-2 arithmetic hinges on #2043 pinning the k-way-merge ns/row,
today an **`[ASSUMED]` 10–500 ns/row** (25–50× blind spot). **Phase 0 already pins the base
constant** — and it is *higher* than the assumed range:

- reconcile 4a = **50.0 CPU-s / ~25M rows ≈ 2.0 µs/row**, on **narrow disjoint singleton clusters**
  (Phase 0's data has no overlap). The coordinator's whole critical path is ~2.0–2.5 µs/row (= the
  500k ceiling). For singletons, that 2 µs/row is **`ReconcileState` machinery**
  (`ReconcileState::new` per cluster, the `winners` HashMap, the `order` Vec — `reconcile.rs:156–180`)
  **not comparison work** — which is exactly what Phase 1-1's L3 singleton fast-path targets. The
  `[ASSUMED]` 10–500 ns/row was the *merge-algorithm* cost; the real per-row cost is
  machinery-dominated and an order of magnitude larger.
- **So #2043 does NOT gate the base arithmetic** — Phase 0 measured it. **What #2043 *does* pin, and
  what is genuinely unmeasured, is the OVERLAP multiplier on 4a** — reconcile ns/row when clusters
  span multiple generations with real LWW/tombstone collisions (Phase 0 §5 caveat 5 is explicitly
  blind to this). That overlap multiplier **is** §1a's residual uncertainty (the 1.1–1.5× I could
  only bound, not measure).

**Ruling:** #2043 is worth harvesting, but **reframe its deliverable**: it must pin **the overlap
multiplier on reconcile at field compaction state**, not "the 10–500 ns/row base" (already ~2 µs/row
measured, machinery-dominated). That single number would tighten §1a's 1.1–1.5× — the last soft spot
in the derate — and would tell you whether L3 (singleton fast-path) or an overlap-path optimization
is the higher-value reconcile lever. Route it into the #2605 report as Phase 1-2 recommends.

---

## 6. Ruling on the 0.17 columnar-producer increment (P1.2 option (a)) — DOES NOT earn the slot

**Ruling: the scoped columnar scan producer is NOT a 0.17 throughput lever. It is Stage-3 prep and
should wait behind the row-engine levers and behind sharpened #2605 data.**

Multiplier-per-effort, on the R12 (narrow) regime that anchors the goal:

| Lever | Multiplier | Cost | Attacks | Verdict |
|---|---|---|---|---|
| **L1 batch fan-in channel** (P1.1) | **1.9× util / 1.15–1.35× single**, and raises C(N) | **M** | 49.9% stage 4b — the biggest line | **#1 — do first**; it is the prerequisite for C(N) and for any fan-out |
| **L3 reconcile singleton fast-path** (P1.1) | 1.20× single | M–L | the **measured ~2 µs/row machinery** (§5) — the true ceiling once L1 lands | **#2** — highest-value ceiling lever |
| **L5 FxHash + L4 RowKey hoist** (P1.1) | ~1.1× bundle | S | SipHash 4.5% + alloc | near-free warm-up bundle |
| **Columnar option (a)** (P1.2) | **1.0–1.1× narrow / 1.5× wide** | **M** | materialize 4.5% + transpose + PK-copy alloc | **defer** — see below |

- **On the narrow R12 corpus columnar is ~1.05×** — worse multiplier-per-effort than L1 (1.9×/M) and
  than L3 (1.2×/M–L), for the same M cost. It reaches 1.5× only on **wide** rows, which are precisely
  the rows with a *lower* rows/s baseline — so it does not reliably close the **rows/s** A4 Stage-2
  gap.
- **It touches neither of the two dominant costs** — the 49.9% channel (L1's) nor the ~2 µs/row
  reconcile machinery (L3's/§5). It deletes materialize (4.5%) + transpose + one alloc. That is real
  but secondary, exactly as the ladder places full columnar at **Stage 3**, above Stage 2.
- **The one honest argument *for* pulling it early is architectural, not throughput:** it banks the
  bounded-stream + byte-cap plumbing (#1907) that #941-A3 needs. That is a #941-de-risking rationale,
  not a Stage-2 rationale, and should be scheduled as such if at all.

**Sharpened trigger (supersedes "just build it"):** give the increment a 0.17 slot **only if** the
sharpened #2605 PoC shows the **decode-to-column delta, isolated from the vectorized-exec delta, on a
wide-and-overlap-shaped corpus (not RF=1 narrow), exceeds ~1.3×** while staying under B4 512Mi
byte-bounded. Absent that data it is Stage-3 prep. This matches Phase 1-2's own §3.2 sharpening asks;
I am making the *slot* conditional on them, not just the measurement.

---

## 7. What I overruled in each of the three positions

**Phase 1-8 (prior art — "600k/pod credible, 150k/core ≈ ⅓ Scylla").**
- **Upheld:** the per-core envelope (150–450k/core narrow); that 150k/core is unexotic per core.
- **Overruled:** "600k/pod credible and conservative." Quantifying the 4-concurrent-core risk it only
  gestured at gives **~400–450k/pod central** (4 vCPU = 2 physical cores + SMT, C(N) ~3, not 4).
  600k/pod is the optimistic corner.
- **Tempered:** the ⅓-of-Scylla defense — Scylla is an *upper bound* (aggregation pushdown, no row
  ship), so it corroborates but cannot *prove* the per-core rate; CQLite's own 500k-with-⅔-waste is
  the real evidence.

**Phase 1-2 (columnar — "240–400k, likely short; columnar 1.2–1.5×").**
- **Upheld & credited:** the headline (Stage 1 easy, Stage 2 likely short; columnar is not the unlock;
  reconcile is the dominant field cost), and the catch that Phase 1-7 conflated pod and core.
- **Overruled — derate decomposition:** "RF=3 → reconcile 2–3× input cells" is a **misattribution**
  (reconcile.rs is per-node-per-cell; RF is deduped by the connector's one-replica pin; the real term
  is compaction generation-overlap, ~1.1–1.5×). "LZ4 single-to-low-double-digit %" is **overstated**
  (P1.3: 0.1–1% ≈ 1.01×). Net derate **1.5–3×, not 3–5×**.
- **Net:** same answer band (~240–400k → I get ~240–670k, central ~420k) via corrected parts — its
  3–5× derate was canceling its optimistic 4× C(N).

**Phase 1-7 (Trino — "B3 Stage 2 ≤3 s NOT credible on a row feed").**
- **Upheld:** the structural reframing (Trino = latency floor; the row *feed*, not Trino, is the
  throughput ceiling) — the best framing in the set — and B3 Stage 1 ≤10 s reachable.
- **Overruled — the B3 Stage-2 feasibility call and its named limiter.** 215–323k/pod is
  **54–81k rows/s/core = 12–19 µs/row/core**, which the fixed field-derated pipeline (95–190k/core,
  5–10 µs/row/core) **exceeds**. The row engine per-core rate is **not** the binding constraint; the
  real gates are the **Trino coordinator floor + cold-start latency inside the 3 s budget** + landing
  L1/parallelism. B3 Stage 2 is **reachable-at-cost on a row feed**, not "not credible."
- **Also overruled (shared with P1.2's catch):** the "1.6 µs/row = vectorized territory" framing —
  it silently assumed a single-thread pod.

---

## 8. Bottom line for the program

1. **A4 Stage 2 (600k rows/s/pod server-direct): plan for ~400–450k/pod on the pure row engine, not
   600k.** The gap is **C(N)** (2 physical cores) + reconcile, not Arrow encode and not the container.
   If 600k/pod is a hard requirement, the honest levers are (a) the row levers to the optimistic
   corner *and* (b) either wider pods (more physical cores) or the columnar increment **on wide rows**
   — accept that no single lever closes it on the narrow corpus.
2. **B3 Stage 2 (≤3 s through Trino): pursuable on the row feed** — but the deliverable is **measure
   the Trino coordinator floor and the cold-start latency for this scan shape first.** If the floor is
   ≤ ~1.2 s and IO is warm, the central pipeline (L1 + L3 + fan-out) clears 216–323k/pod. If the floor
   is ~2 s, only the optimistic corner clears it. **This is a Trino/IO measurement, not a
   row-vs-columnar decision.**
3. **Sequence: L1 (batch fan-in channel) → L3 (reconcile singleton fast-path) → fan-out-past-drain
   (gated on #2765).** L1 is both the biggest single-stream lever and the enabler of C(N); nothing
   scales without it.
4. **#2043/WS7: harvest, but repoint it** at the reconcile **overlap multiplier** (the base is already
   measured at ~2 µs/row, machinery-dominated).
5. **Columnar option (a): defer past 0.17 as a throughput lever**; revisit only as #941/#1907 plumbing
   or on sharpened #2605 data showing an isolated decode-to-column delta >1.3× on a wide/overlap
   corpus.

**File left uncommitted per instructions:**
`/Users/patrickmcfadin/local_projects/cqlite/docs/research/phase2-verify-stage2.md`
