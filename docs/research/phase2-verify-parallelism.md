# Phase 2 — Adversarial verification of the parallelism / scheduling claims

**Date:** 2026-07-21 · **Status:** research (uncommitted) · **Agent:** Phase-2 adversarial verifier
**Verified against:** `docs/research/phase1-6-parallelism.md` (target), `phase1-5-transport-ingest.md`,
`phase1-1-row-engine.md`, `phase0-scan-cost-breakdown-2026-07.md`
**Method:** READ-ONLY. Every ruling below is anchored to a `file:line`, an issue/PR body, a git
revert SHA, or a field round note. No builds.

---

## 0. Verdict table (per target)

| Target | Claim | Verdict | One-line |
|---|---|---|---|
| **P-A** | #2680 re-land: default K=1, opt-in K=2, never K=4; early-close cancel fix; #2792 E2E required | **SOUND, one rationale correction** | Root-cause reading matches #2782 exactly; shape is safe; but "SplitWeight carries the balance" is wrong — the **sub-split rotation** carries flight-pod balance, SplitWeight is Trino-worker accounting. |
| **P-B** | Resize admission 64→16–24 because 64×15–20MB ≈ 1.3GB ≫ 512Mi → OOMKill | **DIRECTION right, JUSTIFICATION overstated** | The 1.3GB→OOM did **not** happen in the field (R11b 80-thread overload = 0 OOMKills, peak 270–391Mi). Real-but-**unexercised**. Resize is a prudent P2 companion to #2680, not a live-OOM P1. |
| **P-C** | Per-stream buffer = (4+3)×8192 = 57,344 rows ≈ 15–20MB; B4 ceiling ~14–34 streams | **VERIFIED, two corrections** | Channel counts **batches** (cap 4), each ≤ `batch_size` 8192 **rows** — math is structurally right. But `IN_FLIGHT_ALLOWANCE=3` is `#[cfg(test)]` (prod peak ≈ 4–6 batches, ~33–49k rows), and the byte figure is row-width-dependent. The 64MiB result budget does **not** cap the streaming path. |
| **P-D** | Drain saturates at ~8 concurrent streams = the pod's useful width | **CORRECT and honestly bounded** | ~8 is a real measured number (local 10-core rig); the packet already says L6 raises it and a 4-vCPU pod is lower. It does **not** treat 8 as a constant. Recommend an explicit "(pre-L6, local rig)" tag. |

---

## 1. THE A5 CONTRADICTION — ruling: **REAL-BUT-UNEXERCISED (mechanism), OVERSTATED (as written)**

### The deciding field evidence (falsifies the "1.3GB → OOM" claim as stated)

#2367 R11b field round, **80-thread scan-heavy overload** (`count(*)` + `LIMIT 50000`):

- **"0 pod restarts, 0 OOMKills, pods stayed 1/1 Running, no hangs"** (#2367 comment, the R11b
  admission-control section).
- **Memory trajectory: idle 3–4Mi, peak 270–391Mi, 0 OOMKills, no growth trend** (#2367 R11b memory
  4-point + the round-11b-vs-expectation table: `memory | idle 3–4Mi, peak 270–391Mi, 0 OOMKills`;
  `80-thread overload | 0 restarts`).
- do_get whole-round **30,414 ok / 708 error (2.3% aborted/superseded splits)** — the server was
  processing tens of thousands of streams and still peaked at **391Mi**, well under the 512Mi B4 pod.

If P-B's arithmetic (64 admitted × ~20MB ≈ 1.28GB) described the realized state, the pod would have
OOMKilled under an 80-thread flood. It did not — it peaked at **~0.3× the 512Mi budget**. So the
literal claim *"admission 64 → 1.3GB → A5 OOMKill under overload"* is **not what the field shows.**

### Why it didn't OOM — the mechanism (this is the "real-but-unexercised" part)

Realized concurrent streams per pod **never approached 64**:

1. **Drain saturation caps useful concurrency at ~8** (#2765 baseline table, reproduced in
   phase1-6 §4): throughput is flat 8→80 threads; extra offered concurrency piles into buffers/latency,
   it does not create 64 simultaneously-producing streams.
2. **Fan-out skew pinned most load onto one pod** (#2367 R9/R11: *"only 1 of 3 flight pods did any
   work"*, *"Every concurrent do_get lands on one pod"* — the `pickReplica` first-replica bug, later
   the floorMod rotation). Even the busy pod stayed at 391Mi.
3. **Admission telemetry corroborates:** R12 saturation snapshot **admission 12/64** (phase1-5 §1);
   R11b `admission_in_use` read **0** at 20–80 threads (scrape-resolution caveat, #2367). Nothing in
   the field record shows the pod ever holding anywhere near 64 concurrent Arrow-egress buffers.

The per-stream buffer math (P-C) is correct; the OOM never fired because the **width term** (realized
concurrent streams) sat at ~8–12, not 64. The 1.3GB is the number for a *hypothetical* 64-wide pod
that the current code's drain-saturation + skew prevent from ever existing.

### The sharp coupling the packet under-states — and the corrected ruling

The reason 64 is unexercised (drain saturation + **fan-out skew**) is *itself* the thing #2680 sets
out to fix. **A successful #2680 re-land spreads load evenly AND multiplies split count → it pushes a
pod's realized concurrent-stream count UP, toward the admission ceiling.** So:

- **Today:** the A5 memory hazard is **real in principle, unexercised in practice** — P-B's OOM is a
  hazard of a state the current code does not reach.
- **After #2680 K=2 + drain-fix:** realized per-pod concurrency rises, so the 57k-row Arrow buffer ×
  concurrency starts to matter for the first time.

**Ruling:** the memory-derived admission resize is **a sensible companion to the #2680 re-land**, not
noise — but its correct justification is *"the re-land will raise realized concurrency, so cap it from
memory before it bites,"* **not** *"we are OOMing at 64 today"* (the field falsifies that). It is a
**P2 forward-looking stability guard bundled with #2680**, not a standalone P1 stability fix. The
packet's §6.3 should be reworded to lead with the field's 0-OOMKill/391Mi result and frame the resize
as a pre-emptive cap that becomes load-bearing only once #2680 unlocks higher concurrency.

---

## 2. P-C BUFFER MATH — verified at file:line, two corrections

### What the channel actually counts (verified)

- `cqlite-flight/src/streaming.rs:65` — `DO_GET_CHANNEL_CAPACITY: usize = 4`, documented (`:58`) as
  **"do_get channel capacity, in batches."** The channel payload is `RecordBatch`
  (`streaming.rs:140` `mpsc::Sender<Result<RecordBatch, ProducerError>>`, `:299`
  `mpsc::channel::<Result<RecordBatch, ProducerError>>(capacity.max(1))`).
- `cqlite-flight/src/producer.rs:401` — batches are built to `batch_size` **rows** (default 8192,
  `main.rs:35–36`) then emitted. So `8192` counts **rows per batch**, and the channel holds **4
  batches**. P-C's "channel = batches, 8192 = rows/batch" reading is **correct** — this directly
  answers attack-line-1's "is it rows or batches?": it is a **4-deep batch channel**, each batch a row
  vector of ≤8192.
- Peak resident rows/stream = (channel depth + in-flight slack) × batch_size. The doc comment
  (`streaming.rs:61`) states the bound as `(DO_GET_CHANNEL_CAPACITY + IN_FLIGHT_ALLOWANCE) · batch_size`.

**Correction 1 — the `+3` is a test bound, not a production one.** `IN_FLIGHT_ALLOWANCE = 3` is
`#[cfg(test)]` (`streaming.rs:85`); its own doc says it is a *"test-observation bound, not a value any
production code branches on."* Its three components are +1 send-in-flight, +1 encoder prefetch, +1
Tokio scheduling slack — of which only the first two are real production residency. So the **production**
peak is ≈ `(4 + ~2) × 8192 ≈ 49,152 rows`, not 57,344. P-C's 57,344 is a ~15% over-count (uses the
test bound). Order of magnitude and every downstream conclusion are unaffected, but the packet should
cite the production figure and not the `#[cfg(test)]` constant as if it were a runtime property.

**Correction 2 — the 15–20MB is row-width-dependent, state it as such.** 49k–57k rows × row-width.
At the narrow `keyvalue` ~300B/Arrow-row that is ~15–17MB; wider field rows raise it, narrower lower
it. The packet's §6.2 does band this (15/20/30MB), which is fine — just anchor the row count to the
production 49k, not 57k.

### Does the byte-bounded result budget already cap this? **No — not on the streaming path.**

- The result budget (`cqlite-core/src/query/result_budget.rs`, `QueryConfig::n` default **64MiB**,
  `config.rs`) is enforced by `enforce_result_budget` on the **materializing / collect-into-`Vec`**
  path (the byte-identity parity path, `producer.rs:369–375` `CollectSink`).
- **The streaming `do_get` path does not enforce it** — `rg result_budget cqlite-flight/src` returns
  **nothing**. The streaming path (`streaming.rs`) emits each batch as produced and is bounded
  **structurally** by the 4-deep batch channel, not by a byte budget.

So P-C's fear is **not** already handled by `--max-result-bytes`/`QueryConfig::n`. Per-stream residency
is capped *only* by the channel depth (structural, ~49k rows) and cross-stream residency is capped
*only* by admission `K`. This is exactly why a memory-derived admission ceiling (P-B) is the right knob
— there is no byte budget on the streaming egress to lean on. The B4-implied ~14–34 streams/512Mi
(§6.2) is arithmetically sound given the per-stream figure.

---

## 3. P-A #2680 RE-LAND — root cause verified; re-land shape ruled; **SplitWeight rationale corrected**

### 3.1 The #2782 root-cause reading is CORRECT and evidenced

#2782 body confirms the packet's reading verbatim:
- **Symptom:** `SELECT id ... LIMIT 2` (and partial-predicate + LIMIT) **hang 180s**; `LIMIT 100`
  (> 5-row table, full-drains every split) and unbounded scans **PASS**.
- **Discriminating evidence** (quoted in #2782): *"LIMIT 2 (small — Trino stops early) hangs; LIMIT 100
  … consumes every split to completion PASSES."* → early-termination-with-more-splits-in-flight, **not**
  ring/slice math.
- **Trigger:** `DEFAULT_SUB_SPLITS_PER_RANGE = 4` shipped as **production default** (K=4), quadrupling
  the in-flight cancellable split count.
- **Mechanism:** a sub-split's Flight `DoGet` producer blocks in `send` on a full fan-in channel and
  never observes the early close. The packet's §3.2 identification (`producer_thread_from_reader` blocked
  in `send` on the cap-256 `sync_channel`, `from_readers.rs:96/123` `forward_row` →
  `mod.rs:537` `STREAMING_CHANNEL_CAPACITY=256`) is the correct thread. **CQLite's cancel is cooperative
  (`ScanCancel` polled before each `step`)**, so the fix is ensuring early close *fires* that cancel on
  every not-yet-drained sub-split AND that a producer already blocked in `send` wakes on it — precisely
  the packet's §3.4 "early-close drain unit test." Verified sound.

Revert history confirms the timeline: `f5dd215a7` (PR #2779 feat) → `0bd63148b`/`7fa3f2050` reverts
(PR #2791). The re-land is greenfield off a known-good base.

### 3.2 #2792 is real and correctly scoped

#2792 body: promote `Flight ↔ Trino E2E` to a `required` check via the standard **path-conditional**
workaround (success-by-skip on non-connector paths, real run on `trino-connector/**`/`cqlite-flight/**`),
with `enforce_admins` preserved (#2433). AC includes the exact #2782 reproducer. This closes the
process gap that let #2779 auto-merge red. The packet's dependency on #2792 is correct.

### 3.3 RE-LAND SHAPE RULING — K=2 opt-in is right; **SplitWeight-only is NOT a substitute**

Attack-line-3 asks: is "default K=1, opt-in K=2" the right re-land, or does **SplitWeight-only (no
sub-splits, zero hang risk)** capture most of the 2–4× skew win?

**Ruling: SplitWeight-only does NOT capture the win. The flight-pod skew is fixed by the sub-split
rotation, not by SplitWeight.** Evidence:

- **Which flight pod does the CPU work is determined by `split.host()`**, surfaced as Trino soft-affinity
  locality hints in `CqliteFlightSplit.getAddresses()` (`CqliteFlightSplit.java:72–80`), and the primary
  is the deterministic per-range rotation `sorted[floorMod(rotationKey, size)]`
  (`CqliteFlightSplitManager.java:348–349,396–409`). This is confirmed causally by the field: the R9
  skew was *"pickReplica deterministic first-replica"* pinning **one pod** (#2367); the #2409 rotation
  fix spread *participation* but, per #2680's own triage, **count-balanced ≠ weight-balanced** — heavy
  ranges still cluster onto whichever pod floorMod hands them.
- **`SplitWeight` (the reverted `getSplitWeight()`) is Trino's scheduler cost accounting** — it bounds
  the summed weight of outstanding splits **per Trino worker**, i.e. it governs Trino-worker-side split
  concurrency, **not which flight pod a split's `do_get` connects to.** Trino workers and cqlite-flight
  pods are distinct tiers; the connector opens a `FlightStream` to `split.host()` regardless of which
  Trino worker runs the split. So overriding `getSplitWeight()` at **K=1** leaves every heavy range still
  wholly assigned to its single floorMod pod — **it cannot move flight-pod CPU load at all.**
- **To rebalance flight-pod load you must change the assignment granularity or the assignment rule:**
  (a) **sub-split each range into K slices and rotate the primary per slice**
  (`slice i primary = rotated(parent)[i % n]`, the reverted design) — refines granularity so heavy
  ranges' sub-slices spread across owners; or (b) **weight-aware bin-packing at K=1** (#2680 candidate
  direction 1) — assigns whole ranges to owners by size, zero split inflation, zero hang risk, **but
  requires a per-range size feed that does not exist.** The shipped/reverted implementation chose (a).

**Therefore:** the honest re-land is **default K=1 (byte-identical pre-#2680), opt-in K=2 (the minimum
granularity that delivers the rotation-based flight-pod balance), never K=4 default (the #2782 trigger),
gated on the early-close drain fix + #2792 required.** The packet lands on exactly this — but its §3.3/§7
wording *"let getSplitWeight() + a conservative default K=2 carry the balance"* is **imprecise and should
be corrected**: at K=2 it is the **sub-split ROTATION** that carries flight-pod balance; `getSplitWeight()`
only refines Trino-worker scheduling order and adds nothing to flight-pod CPU distribution. "SplitWeight-only,
no sub-splits" is a **rejected** option (captures ~0 of the flight-pod skew win), not the safe shortcut the
question probes for. The only zero-inflation alternative is weight-aware bin-packing (K=1), which is blocked
on a size feed and was not built — so K=2 sub-split rotation is the pragmatic re-land.

---

## 4. P-D DRAIN-SATURATION-AT-8 — correct, and NOT treated as a constant

Attack-line-4 asks whether the packet wrongly treats ~8 as a fixed pod width when row-engine L1 +
transport T-levers would raise the drain rate and move saturation.

**Ruling: the packet does NOT double-count; it already treats 8 as movable.** Evidence in the target:
- §4: *"The lever that raises `N_drain_sat` is L6 (cut the per-row channel tax) — a per-stream lever,
  not a scheduling one."*
- §4: *"A 4-vCPU field pod will have a lower `N_drain_sat` … plausibly ~4–6"* — it explicitly does not
  export the local 10-core `~8` as a field constant.
- §6.3 point 1: orders `N_drain_sat (~8) < N_mem (~20) < N_admitted (64)` as *current-code* values.
- phase1-5 §6 (the sibling packet) states the coupling directly: *"anything that speeds the Java drain …
  raises the real drain rate, which empties the egress channel … letting `active_merges` climb toward
  the 64 admission ceiling."*

So the "8" is honestly scoped as **today's drain-bound width on the local rig**, and both packets say
the row-engine (L6) and transport (ArrowToTrino/prefetch/window) levers **raise** it. The one gap: the
§7 summary formula pins `N_drain_sat ≈8` inline **without** a "(pre-L6, local 10-core)" tag, so a Phase-3
reader skimming only the summary could inherit 8 as a field constant. **Recommendation:** label it
`N_drain_sat ≈ 8 (pre-L6, local 10-core; ~4–6 on a 4-vCPU field pod, rising with L6/transport)` wherever
it appears standalone.

**Honest post-fix width model** (the "8 is not a constant" answer, stated for Phase 3):

```
width_postfix = min( N_admitted , N_drain_sat_postfix , N_mem )
  N_drain_sat_postfix  rises as L6 (channel batching) + ArrowToTrino zero-copy + prefetch + HTTP/2
                       window speed the drain — but is HARD-CAPPED at the vCPU count (4) for
                       CPU-bound narrow rows: you cannot run more truly-concurrent CPU-bound
                       streams than cores.
  N_mem  ≈ 512Mi / per-stream-MB ≈ 16–24 (memory-bound wide rows / slow link regime).
```

For narrow CPU-bound rows, `width_postfix → ~4` (vCPU-bound). For wide/compressed/network-bound rows,
`width_postfix → N_mem ~16–24` (memory/link-bound). Saturation "moves" from ~8 (today, drain-bound below
cores) toward the 4-vCPU wall — it does not grow without limit.

---

## 5. UTILIZATION STACKING — the credit cap for Phase 3

Attack-line-5: sub-splits (#2680/L1), row-engine producer-side utilization (phase1-1 L1/L6), and
transport fan-out (phase1-5 lever 6) all widen effective streams. How much **total** utilization credit
may the stack take against 4 vCPUs and the post-fix drain?

**Ruling — ONE width multiplier for the entire utilization stack, not three multiplied:**

1. **Sub-splits (#2680), transport fan-out (phase1-5 #6), and producer-side parallel decode all buy the
   SAME quantity — realized concurrent streams per pod — and all cash out against the SAME ceiling**
   `min(N_admitted, N_drain_sat_postfix, N_mem)`. They are **substitutes competing for one width budget,
   not independent multipliers.** #2680 spreads streams *across pods* (fixes skew); fan-out raises streams
   *per pod*; both are throttled by the same per-pod drain/vCPU/memory `min(...)`. Phase 3 must **NOT**
   multiply their separately-quoted "2–4× (skew)" × "2–5× (fan-out)" — the first lever to reach the
   ceiling caps the rest.

2. **The utilization ceiling on a 4-vCPU pod is ≈ the vCPU count for CPU-bound rows** (`width_postfix → 4`,
   §4). Today's effective width is already ~8 on a 10-core local rig and drain-bound *below* cores; on a
   4-vCPU field pod the realized width is smaller. So the utilization stack's total credit over a single
   stream is **bounded at ~4× (the vCPU envelope), × `C(N) < 1`** (Phase-0's 55% kernel park/wake tax that
   contends as streams stack) — **not** the ~2-to-many× product the three packets separately imply.

3. **Per-stream ceiling levers are a DIFFERENT axis and DO compound — on the Amdahl residual.** L6 (batch
   the fan-in channel, ~1.9× util / ~1.15–1.35× single-stream), L3 (reconcile fast-path), ArrowToTrino
   zero-copy, HTTP/2 window: these raise `per_stream_rows_s`, and by speeding the drain they **raise the
   width ceiling** (`N_drain_sat_postfix`). They are the **multiplicand**; width is the **multiplier**.

**The cap rule for Phase 3, stated as an equation:**

```
per_pod_gain  ≤  (width_postfix / width_now)  ×  (Π per-stream ceiling levers, Amdahl residual)  ×  C(N)
                 \___ ONE utilization factor ___/     \___ compounds on the residual ___/         \_<1_/

  width_postfix ≤ 4 (vCPU) for narrow CPU-bound rows; ≤ N_mem ~16–24 for wide/network-bound rows
  Take the utilization credit ONCE (the min-width move). Do NOT stack #2680 × fan-out × producer-parallel.
  Per-stream levers (L6/L3/ArrowToTrino/window) are the residual product AND the thing that raises width.
```

Concretely: Phase 3 may claim **one** width improvement (from the utilization stack collectively, bounded
by the 4-vCPU / `N_mem` envelope) **times** the Amdahl product of the per-stream ceiling levers **times**
`C(N)<1`. It may not present sub-splitting, transport fan-out, and producer-side parallelism as three
independent 2–5× factors — they are one lever expressed three ways, all bounded by the same per-pod
`min(...)`.

---

## 6. Summary packet

**Per-target verdicts:** P-A **sound** (root cause matches #2782; K=1/opt-in-K=2 shape correct; #2792
real) with the **SplitWeight rationale corrected**; P-B **direction right, OOM justification overstated**
(field 0-OOMKill / 391Mi peak falsifies the 1.3GB-today claim); P-C **verified** with the `#[cfg(test)]`
+3 over-count and the row-width caveat noted, and the confirmation that the 64MiB result budget does
**not** cap the streaming path; P-D **correct and honestly non-constant**.

**The A5 ruling — REAL-BUT-UNEXERCISED (mechanism) / OVERSTATED (as written).** Deciding evidence:
#2367 R11b **80-thread overload = 0 OOMKills, peak RSS 270–391Mi** against a 512Mi pod, with realized
concurrency ~≤12 (admission 12/64, `in_use` scrape-0, fan-out pinned to one pod, drain saturates ~8).
The 1.3GB is the number for a 64-wide pod the current code never reaches. The memory-derived admission
resize (64→16–24) is a **P2 forward guard to bundle with the #2680 re-land** — justified by *"the re-land
raises realized concurrency toward the ceiling,"* **not** by a live OOM. Downgrade its stated severity.

**The re-land shape ruling — K=2 opt-in, SplitWeight-only REJECTED.** Flight-pod CPU load is set by
`split.host()` rotation (`CqliteFlightSplit.getAddresses` + floorMod primary), not by `getSplitWeight()`
(Trino-worker split-cost accounting). SplitWeight-only at K=1 moves **zero** flight-pod load; the skew fix
requires the **sub-split rotation** (K≥2) or weight-aware bin-packing at K=1 (blocked on a nonexistent
size feed). So: **default K=1, opt-in K=2, never K=4 default**, gated on the early-close drain fix
(`producer_thread_from_reader` blocked-in-`send` must wake on cancel) + #2792 `required`. Correct the
packet's "SplitWeight carries the balance" to "the K=2 sub-split rotation carries flight-pod balance."

**The total utilization credit cap.** ONE width multiplier for the whole utilization stack (#2680 +
transport fan-out + producer-side parallelism — substitutes competing for one per-pod width budget),
bounded by `min(N_admitted, N_drain_sat_postfix ≈ 4-vCPU envelope, N_mem ~16–24) × C(N)<1`. Per-stream
ceiling levers (L6/L3/ArrowToTrino/window) compound on the Amdahl residual and are what raise the width
ceiling. Phase 3 must not multiply the three utilization quotes as independent factors.

**File:** `docs/research/phase2-verify-parallelism.md` (uncommitted per instructions).
