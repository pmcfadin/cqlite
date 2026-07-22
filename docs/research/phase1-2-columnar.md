# Phase 1 — Agent 2/8: Columnar / vectorized scan path

**Date:** 2026-07-21 · **Status:** research (uncommitted) · **Scope:** READ-ONLY analysis
**Anchor:** `docs/research/phase0-scan-cost-breakdown-2026-07.md` (the ground-truth CPU profile)
**Dedup base:** `docs/research/throughput-backlog-inventory-2026-07.md`

> **Headline (uncomfortable, on purpose).** For the Flight `do_get` full-scan throughput number,
> **columnar / DataFusion is oversold.** Phase-0 proves Arrow encode is **1.0 %** of CPU and the
> real cost is the k-way **reconcile (32.5 %)** plus per-row **channel coordination (49.9 %)** and
> **allocation (17.6 %)**. Columnar decode-to-Arrow-columns can delete **row materialization
> (4.5 %)** + the redundant transpose + per-row alloc — a **~1.2–1.5× multiplier that scales with
> row width** — but it **cannot touch the reconcile**, which is inherently row/cell-ordered and is
> the field's dominant per-row derate under RF=3. The Stage-2 levers are, in order: (1) delete the
> per-row channel handoff [threading fix, not columnar], (2) parallelize splits across the pod's 4
> vCPUs [#2680, not columnar], (3) control alloc [not columnar], (4) columnar decode [this memo] as
> a secondary multiplier that earns its keep on **wide rows and analytical (aggregate/filter) query
> shapes**. Recommend a **scoped columnar scan producer for 0.17 (M, low-risk)** — NOT the full
> #941 DataFusion program.

---

## 1. What Stage 2 actually requires — the arithmetic

### 1.1 The ladder and the two anchors

Ratified A4 ladder (`941-datafusion-decision-brief-2026-07.md` §"ladder math",
`performance-goals-2026-07.md`):

```
baseline 10k  →  Stage 1: 100k  →  Stage 2: 600k  →  Stage 3: millions   (rows/s/pod)
   R12 field       row-engine        "not row-at-a-time"     "columnar"
   (~10.6k)        territory         per the brief           per the ladder
```

- **Field baseline (R12, #2367):** ~10.6k rows/s/pod (1.94M rows / 61.1s / 3 pods), through Trino,
  RF=3, LZ4, ~1.9M partitions/node, 4-vCPU pods.
- **Local ceiling (Phase-0):** ~500k rows/s **single stream**, M1 core, RF=1, narrow 2-col rows,
  warm, **uncompressed**, server-direct. This is **≈2.0 µs/row of real coordinator work** (see
  below) — NOT a field prediction, ~50× the field number for known reasons.

The brief reads Stage 2 as "600k rows/s/pod = ~1.6 µs/row end-to-end … vectorized-execution
territory." **That reading conflates rows/s/pod with single-thread µs/row.** A pod has **4 vCPUs**.
600k rows/s/pod is only 1.6 µs/row *if the merge runs on one core*. With parallel splits across 4
vCPUs it is **6.4 µs/row/core** — squarely reachable by a row engine. So the real question is not
"can one coordinator hit 1.6 µs/row" but "can 4 parallel per-split merges hit 600k/pod aggregate,
on wide/RF=3/LZ4 data."

### 1.2 Where the single-stream ceiling really comes from (Phase-0 re-read)

Phase-0's critical finding, restated precisely: **single-stream throughput is limited by the one
k-way merge coordinator doing real reconcile+materialize+arrow work; the 49.9 % channel park/wake
is the reader threads idling *because the coordinator cannot drain them*** (report §3, thread
architecture: "coordinator carries ~62 CPU-s of real work — it is the throughput limiter"; "reader
threads … almost entirely blocked in `send` … starved by the coordinator"). So:

- **Coordinator real work ≈ 2.0–2.5 µs/row** (≈500k rows/s, one core). Composition (by-stage §3b,
  charged to the coordinator): reconcile **4a 32.5 %** + materialize **3 4.5 %** + arrow **5 1.0 %**
  + its slice of alloc (part of malloc **17.6 %**) + SipHash key-hash (**4.5 %**).
- **Channel 4b (49.9 %)** and most of **malloc (17.6 %)** are *overhead / symptom* of the
  thread-per-input design and per-row `QueryRow`/`RowKey`/`MergeEntry` churn — deletable, but
  deleting them frees CPU (helps multi-stream throughput/pod and total CPU-s), it does not by itself
  raise the coordinator's serial ceiling much.

### 1.3 Model: real single-threaded compute per row

Sum the genuine data-plane stages (by-stage §3b, which sums to 100 % over 154.1 CPU-s):

| Stage | % | Deletable by… |
|---|---|---|
| 2 decode | 9.7 | irreducible (needed to feed reconcile) |
| 3 row materialize (`entry_to_row`→`QueryRow`) | 4.5 | **columnar** (this memo) |
| 4a reconcile / LWW / tombstone | **32.5** | **irreducible** — not columnar, not threading |
| 5 arrow build | 1.0 | already cheap |
| 4b channel park/wake | 49.9 | **threading** (batch handoff / inline merge) |
| malloc (by-library, distributed into 3/4a/4b) | 17.6 | ~half **alloc discipline + columnar**, ~half irreducible output |
| SipHash key hash | 4.5 | faster hasher / fewer key copies |

**Real compute** = decode 9.7 + materialize 4.5 + reconcile 32.5 + arrow 1.0 ≈ **47.7 %**, plus
~half the malloc (~9 %) that is genuine output/Arrow ≈ **~57 % of 154.1 CPU-s ≈ 88 CPU-s**. Over
the profiled ~25M rows (≈500k/s × ~50s active), that is **≈3.5 µs/row of real single-threaded
compute** on an M1 core, RF=1, narrow, warm, uncompressed.

- One inline-merge core (channel deleted): **~3.5 µs/row → ~285k rows/s/core**.
- 4 parallel per-split merges on a pod (perfect parallelism, M1-class cores, RF=1 narrow warm):
  **~1.14M rows/s/pod** — *above* Stage 2.

### 1.4 The field derate — why the row pipeline lands *near but likely short of* 600k

The M1/RF=1/narrow/warm/uncompressed number must be derated to field (4-vCPU, RF=3, wide, LZ4):

| Factor | Effect on µs/row | Which stage |
|---|---|---|
| RF=3 fan-in | reconcile ~2–3× more input cells to compare/shadow across replicas×gens | **4a (the big one)** |
| LZ4 decompress | Stage 1 goes 0 %→ real single-digit-to-low-double-digit % | 1 (invisible in Phase-0) |
| Wide-ish rows (many cells/row) | decode + materialize + arrow + alloc scale with cell count | 2, **3**, 5, malloc |
| Slower cores (i4i.xlarge vCPU < M1) | ~1.3–2× | all |
| Cold-ish cache (1.9M part/node) | IO-wait (wall, not CPU) | 1 |

Net per-row derate ≈ **3–5×** → single-core field **~10–17 µs/row** → **4 vCPUs ≈ 240k–400k
rows/s/pod** on a *fixed row pipeline* (channel deleted, alloc controlled, splits parallelized).

**Conclusion (arithmetic):**

- A fixed row pipeline + agent-1 fixes credibly reaches **~250k–500k rows/s/pod** — a **25–50×**
  jump over R12's ~10.6k, i.e. **Stage 1 (100k) comfortably, and into the lower half of the gap to
  Stage 2**, but **likely short of the 600k bar** on wide/RF=3/LZ4 data.
- The residual gap to 600k is dominated by **RF=3 reconcile (4a)** and **wide-row per-cell
  decode/materialize** — NOT by Arrow encode and NOT by the container format.
- **Columnar decode is NOT structurally required to reach Stage 2, and it does not remove the
  dominant field cost (reconcile).** It removes the *materialize + transpose + alloc* slice, which
  grows with row width — a real but secondary multiplier. The ladder itself places full columnar at
  **Stage 3 (millions)**, above Stage 2 — consistent with this finding.

---

## 2. Decode-directly-to-Arrow-columns — where it attaches, and the correctness problem

### 2.1 The current path (two row materializations, then a transpose)

```
KWayMerger.step()  → MergeStep::Partition{ key, rows:[MergeEntry] }        (merge/mod.rs, streaming.rs)
  producer.drive_merge (producer.rs:785)  per entry:
    entry_to_row (producer.rs:967)
      assemble_read_cells (merge::assemble_read_cells)   → RowCells         [reassemble collections]
      RowKey::new(partition_key.to_vec())  (producer.rs:1000)               [per-row PK Vec COPY = malloc]
      build_row_from_scan_cached → QueryRow                                 [row-oriented carrier alloc]
    buffer.push(row)                        → Vec<QueryRow>                  [MATERIALIZATION #1]
    at batch_size: flush_buffer → rows_to_record_batch (arrow_convert.rs:197)
      convert_to_arrays (arrow_convert.rs:1296)
        transpose_columns(columns, rows)    → Cells per column              [MATERIALIZATION #2 / transpose]
        convert_column_to_array per column  → ArrayRef                      [typed builder append]
```

There are **two** materializations: the `Vec<QueryRow>` buffer, then `transpose_columns` re-scatters
those rows back into per-column slices (`arrow_convert.rs:1300`) before the typed builders run. The
typed column builders already exist and are CQL-type-driven (`convert_column_to_array`,
`arrow_convert.rs:1322` — no heuristics, authoritative `cql_type`).

### 2.2 The columnar attach point

A columnar scan producer **replaces the `Vec<QueryRow>` buffer with a set of per-column Arrow
builders** and makes `entry_to_row` append each surviving cell straight into its column's builder:

```
drive_merge per surviving entry:
  reconcile stays IDENTICAL (row-wise, in KWayMerger)                       [UNCHANGED]
  assemble_read_cells → RowCells                                           [UNCHANGED]
  → for each needed column: builder[col].append(cell_value_or_null)         [NEW: direct scatter]
  cell tombstone → append null;  row tombstone → skip row (append nothing)  [reconcile result]
  at byte/row cap: finish() all builders → RecordBatch                      [replaces flush_buffer]
```

Attach sites: **`producer.rs` `drive_merge`/`flush_buffer`** (buffer → builder set) and the
**`entry_to_row`→builder append** edge. Deletes: materialization #1 (`QueryRow` + `RowKey::new`
PK-copy), materialization #2 (`transpose_columns`), and the per-row row-carrier allocs. Reuses:
every typed builder in `arrow_convert.rs`.

### 2.3 The correctness problem — three options, only one is 0.17-shaped

The k-way merge/reconciliation is **inherently row/cell-ordered**: LWW is per-cell by writetime, and
tombstones **shadow across SSTables/generations** (a value in gen-47 can be killed by a tombstone or
a newer-timestamp cell in a *different* generation or the tail). #2037 §7b makes this explicit: a
per-SSTable columnar file is *wrong as a standalone answer*.

| Option | Description | Tombstone reconciliation | Query-semantics oracle | Verdict |
|---|---|---|---|---|
| **(a) merge keys row-wise, scatter values into column builders** | Reconcile exactly as today; a surviving row's cells append to column builders (cell tombstone→null, row tombstone→skip) | **Compatible** — reconcile is byte-for-byte unchanged; only the output *container* changes | **Unchanged by construction** — identical row set, values, order at pinned `now` | **RECOMMEND** |
| (b) merge-then-gather | Materialize `Vec<QueryRow>`, then transpose to columns | Compatible (this is essentially TODAY's path) | Unchanged | No net win — it *is* the redundant double-pass we want to delete |
| (c) per-SSTable columnar decode + merge-on-sorted-runs | Decode each SSTable to columns independently, merge columnar runs | **Broken cheaply** — cross-gen tombstone shadowing means a per-SSTable column is not a final answer; you must carry the full LWW envelope (writetime/TTL/deletion per cell) columnar and merge columnar | Would need the #2037 per-generation-cache design to stay correct | **Reject for 0.17** (this IS #2037, a large exploration) |

**Only option (a) is compatible with the parity oracles at low cost**, because reconciliation is
untouched and only the output container changes:

- **Physical-dump parity** (`*-Data.db.jsonl`): enumerates on-disk cells incl. tombstones — unaffected,
  it operates below the producer.
- **Query-semantics oracle** (`query-semantics-oracle.json`, gate component `query-semantics-oracle`):
  post-reconciliation `SELECT` result set at a pinned `now` — **identical by construction** under (a).
- **Point-vs-full differential** (`point_vs_full_differential.rs`): unaffected (routing knob, both
  paths reconcile identically).
- **No-heuristics**: builders are driven by authoritative `cql_type` (existing
  `convert_column_to_array` dispatch) — no byte-pattern inference introduced.

Wiring-evidence for done: route the **query-semantics oracle lane and the physical-dump smoke
through the columnar producer** (extend #2374's Flight-do_get oracle lane) so the surface is
exercised end-to-end, not just a helper unit test.

### 2.4 The B4 memory trap (≤512Mi peak)

Columnar builders hold a **whole batch in flight** (row cap 8192 × all columns × per-cell backing).
For wide rows this is exactly the hazard the #941 council flagged (`issue-941-design-a` §memory:
"byte_cap is the primary governor … 8 MiB/batch × 4-batch channel = 32 MiB/scan … the target is
plausibly blown under fan-out"). Under **B4 ≤512Mi peak** with the pod running ~4 concurrent
split-merges plus egress channels, the columnar batch **must be bounded by BYTES, not rows** — carry
the #1907 lesson forward: byte_cap primary (start ~4–8 MiB, drop to 2–4 MiB for wide tables), finish
the batch on whichever of row-cap / byte-cap trips first. A naive `8192-row` builder on wide rows can
blow the budget. **A5 stability** actually *improves*: deleting per-row `QueryRow`/`RowKey` malloc
removes allocator jitter that shows up in p99 (Phase-0 stage-3/malloc), provided batch memory is
byte-bounded so peak doesn't spike.

---

## 3. Disposition of existing work — #941 / #2605 / #2037

Treating **#941 and #2605 as ONE lineage** (the brief scopes #2605 as the de-risking spike for the
#941 promotion decision) and #2037 as a separate owner-gated exploration:

### 3.1 #941 Design-A epic (#1905–#1914) — keep Backlog, owner-gated; NOT a 0.17 throughput pull

The Design-A packet is an **MPP / analytical-execution surface** (a DataFusion `TableProvider` so
DataFusion can run leaf scans while Trino stays the MPP scheduler), gated behind Sidecar snapshot
manifests/leases, split planning, ring-coverage correctness, and 8 non-negotiable invariants. It is
**not a throughput fix for the existing do_get path**:

- Its value is **vectorized filter / project / aggregate / join over already-reconciled batches** —
  which helps *analytical query shapes*, not the full-scan `do_get` rows/s number Phase-0 measured
  (where Arrow encode is 1.0 % and the cost is the reconcile that happens *before* DataFusion sees a
  batch). DataFusion vectorization **does not speed up the k-way reconcile**.
- It is **L** (10 children, MPP integration, Sidecar leases, signed manifest, Java+Rust type
  contract, E2E-vs-Trino capstone #1912). The hard blocker #1907 (`Vec<RecordBatch>` → bounded
  cancellable stream) is real work the row path also wants.
- **Disposition:** leave at Backlog/P3 owner-gated. Its A3 streaming-producer blocker (#1907)
  partially overlaps the columnar scan producer below — sequence so the columnar producer lands the
  bounded-stream + byte-cap plumbing that #1907 needs, de-risking #941 as a side effect.

### 3.2 #2605 TableProvider PoC (0.16, P2) — keep, but SHARPEN what it must measure

As filed, #2605 benches DataFusion vs the row engine on three shapes (count(\*), projected scan,
filtered scan) on the R12 corpus, feature-gated, zero wiring. **The risk is it measures the wrong
delta** and over- or under-sells columnar. To de-risk the 0.17 call it must isolate:

1. **Decode-to-column delta vs vectorized-exec delta, separately.** How much of any DataFusion win
   comes from (i) not materializing `QueryRow`+transpose (which cqlite can capture *without*
   DataFusion, via §2 option (a)) vs (ii) vectorized filter/aggregate over batches (which *needs*
   DataFusion)? These are different levers with different costs — the PoC must attribute the delta,
   not just report a single ratio.
2. **On the field regime, not RF=1 narrow warm.** Measure on **wide-ish rows + a multi-generation /
   RF=3-shaped merge fan-in**, else it measures the regime where columnar helps *least* (narrow) or
   *most* (misleadingly) and mispredicts the field.
3. **Peak memory under B4 (≤512Mi).** Record builder/batch peak at the chosen byte_cap — a
   throughput win that blows 512Mi is not a win.
4. **The reconcile is unchanged in both arms.** Confirm the PoC feeds *post-reconciliation* batches
   to DataFusion (it must — reconcile can't move into DataFusion without breaking Cassandra
   semantics), so the measured delta is honestly "post-merge vectorization," not "faster merge."

Its result then feeds the brief's trigger rule (Stage-1 <~30k/pod → promote #941 for 0.16; else
#941→0.17 with the spike banked).

### 3.3 #2037 ArrowMemtable (#2043 WS7 only) — keep owner-gated; harvest #2043's constant

- The epic's **post-merge-columnarization principle** ("row format until data stops mutating; Arrow
  from the first immutable moment; the columnar copy never has to be independently query-answerable
  because the tombstone-aware merger runs first") is **exactly the design law that validates §2
  option (a)** — columnarize *after* reconcile, never per-SSTable. Good corroboration, but the epic
  itself is a **Cassandra-coordinator CEP-11 play** (in-JVM memtable plugin, freshness protocol),
  **out of scope** for the do_get throughput lever.
- **#2043 (WS7 bench spike) is directly useful and should be harvested regardless of #2037's fate:**
  it produces the **k-way merge ns/row** and **nb decode throughput** constants that the entire
  Stage-2 arithmetic hinges on. Today those are a `[ASSUMED]` **10–500 ns/row** blind spot (a
  25–50× unreconciled spread, per #2037 §12.1). Until #2043 pins the real reconcile ns/row, §1.4's
  derate is an estimate. **Recommend feeding #2043's measurement into the #2605 report.**
- **Disposition:** #2037 stays owner-gated exploration ("do not promote WS1–9 without owner");
  pull *only* #2043's constant into the throughput program.

---

## 4. Estimate — multiplier / cost / risk for a scoped columnar scan path

**Recommended 0.17 increment:** a **columnar scan producer** — §2 option (a): replace the
`Vec<QueryRow>` buffer + `transpose_columns` with direct decode-into-Arrow-column-builders inside the
existing flight producer, keeping the row-wise reconcile untouched, byte-cap bounded. This is the
scoped increment; it is **not** the #941 DataFusion program and does not need DataFusion at all.

### 4.1 Multiplier (arithmetic anchored to Phase-0 %s)

Applied **on top of** the fixed row pipeline (channel deleted, alloc controlled, splits
parallelized):

- Columnar deletes: **materialize (stage 3, 4.5 %)** + the redundant `transpose_columns` pass
  (part of the second `convert_to_arrays` traversal) + the per-row `QueryRow`/`RowKey::new` PK-copy
  alloc (a chunk of the 17.6 % malloc — call it ~8–10 pp on wide rows, less on narrow).
- On the **remaining coordinator cost** after row fixes (reconcile 32.5 % + decode 9.7 % + arrow
  1.0 % + necessary alloc), removing ~4.5 % + ~8–10 pp of alloc + the transpose is a **~20–40 %
  reduction of the remaining per-row cost → ~1.2–1.5×**.
- **Width-dependent:** materialize + transpose + alloc scale with **cells/row**, so the multiplier
  sits near **1.5×** on wide rows (many cells) and near **1.0–1.1×** on narrow 2-col rows. This is
  the opposite width-sensitivity from the channel overhead (worst on narrow rows), so columnar and
  the threading fix are complementary, not redundant.

**Net:** columnar is a **~1.2–1.5× multiplier**, best on wide rows — a real Stage-2 *contributor*,
not a Stage-2 *unlock*. It does **not** move reconcile (32.5 %, the field's dominant derate). Do not
sell it as "the 600k lever."

### 4.2 Engineering cost — **M (medium)**, ~3–5 issues

| # | Issue | Size |
|---|---|---|
| 1 | Column-builder sink replacing the `Vec<QueryRow>` buffer in `producer.rs` (`drive_merge`/`flush_buffer`) | S–M |
| 2 | `entry_to_row`→builder append edge; cell-tombstone→null, row-tombstone→skip; reuse `convert_column_to_array` builders | M |
| 3 | Collection / UDT / frozen columnar append (the `assemble_read_cells` reassembly must feed list/map/UDT builders — the fiddly part) | M |
| 4 | Byte-cap batch bound under B4 (≤512Mi); finish-on-first-of row/byte cap | S |
| 5 | Parity wiring-evidence: route query-semantics oracle + physical-dump smoke through the columnar producer (extend #2374) | S |

Full #941 DataFusion program by contrast = **L** (10 children + Sidecar + MPP + manifest + capstone).
The columnar scan producer also lands the bounded-stream/byte-cap plumbing #1907 (#941 A3) needs.

### 4.3 Risk — **LOW–MEDIUM (correctness), MEDIUM (memory)**

- **Correctness: LOW under option (a).** Reconcile is byte-for-byte unchanged; only the output
  container changes → physical-dump + query-semantics + point-vs-full oracles are **unchanged by
  construction**. No-heuristics preserved (type-driven builders). Risk rises to HIGH only if someone
  drifts toward option (c) per-SSTable columnar — which breaks cross-gen tombstone shadowing; the
  design must state option (a) as a hard boundary.
- **Memory: MEDIUM.** Columnar builders hold a whole batch; wide rows × 8192-row cap can blow B4
  512Mi under 4-way split fan-out. Mitigation is mandatory byte-cap governance (#1907/#941-council
  lesson). Manageable, but it is the real risk and must be a first-class acceptance criterion.
- **A5 stability:** net positive — deleting per-row alloc reduces p99 allocator jitter, provided
  peak is byte-bounded.
- **Collections/UDT:** the one genuinely fiddly area (issue #3) — the multi-cell collection
  reassembly (`assemble_read_cells`, #2324) must land in list/map/UDT builders without regressing
  the composite-keyed-collection fail-closed contract. Scope carefully; it is where a parity
  regression would hide.

---

## 5. Final packet

**Stage-2 requirement arithmetic.** 600k rows/s/pod on a 4-vCPU pod = **6.4 µs/row/core** with
parallel splits (not the brief's 1.6 µs/row single-thread reading). Phase-0's ~500k/s single stream
= **~2.0–2.5 µs/row real coordinator work** (M1, RF=1, narrow, warm, uncompressed); genuine
single-threaded compute ≈ **3.5 µs/row**. Field-derated **3–5×** (RF=3 reconcile, LZ4, wide rows,
slower cores) → single-core **~10–17 µs/row** → **4 vCPUs ≈ 240k–400k rows/s/pod** on a fixed row
pipeline: **Stage 1 (100k) comfortably, into the lower gap toward Stage 2, likely short of 600k.**
The residual gap is **RF=3 reconcile (32.5 %) + wide-row per-cell decode/materialize**, NOT Arrow
encode (1.0 %) and NOT the container format. **Columnar is not structurally required for Stage 2 and
does not remove the dominant cost; the ladder itself puts full columnar at Stage 3.**

**Recommended 0.17 columnar increment.** A **scoped columnar scan producer** (§2 option (a)):
reconcile row-wise exactly as today, scatter surviving cells straight into byte-cap-bounded Arrow
column builders in the flight producer, deleting the `Vec<QueryRow>` buffer + `transpose_columns` +
per-row PK-copy alloc. This is the **only** merge/columnar option compatible with tombstone
reconciliation and the query-semantics oracle at low cost (options (b) = today's redundant
double-pass, (c) per-SSTable columnar = breaks cross-gen shadowing = the large #2037 exploration).
**Not the full #941 DataFusion program.**

**Multiplier / cost / risk.** Multiplier **~1.2–1.5×** on top of the fixed row pipeline
(width-dependent: ~1.5× wide, ~1.0–1.1× narrow; deletes materialize 4.5 % + transpose + ~8–10 pp
alloc; **cannot** touch reconcile 32.5 %). Cost **M** (~3–5 issues, reuses existing typed builders;
the #941 program is **L**). Risk **LOW** correctness (oracles unchanged by construction under option
(a)), **MEDIUM** memory (B4 512Mi — builders must be byte-bounded, not row-bounded), with
collections/UDT the one fiddly area.

**#941 / #2605 / #2037 disposition.**
- **#941 (#1905–#1914):** keep Backlog/owner-gated — it is an MPP/analytical-execution surface, not
  a do_get throughput fix; DataFusion does not speed up the reconcile. Its A3 bounded-stream blocker
  (#1907) overlaps the columnar producer; sequence so the producer banks that plumbing.
- **#2605 (0.16 PoC):** keep, but **sharpen the measurement** — attribute decode-to-column vs
  vectorized-exec delta separately, measure on wide/RF=3 shape (not RF=1 narrow), record peak memory
  under 512Mi, and confirm both arms consume *post-reconciliation* batches. Feed the result to the
  #941 promotion trigger.
- **#2037 (+#2043):** keep owner-gated exploration; its post-merge-columnarization law **validates
  option (a)**. **Harvest #2043 (WS7)** — it pins the k-way-merge **ns/row** constant (today a
  `[ASSUMED]` 10–500 ns/row, 25–50× blind spot) that the whole Stage-2 arithmetic hinges on; route
  it into the #2605 report.

**File left uncommitted per instructions:**
`/Users/patrickmcfadin/local_projects/cqlite/docs/research/phase1-2-columnar.md`
