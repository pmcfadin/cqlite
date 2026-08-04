# 0.17 throughput — established-engineering survey: SYNTHESIS

**2026-08-04. Owner-commissioned, three parallel research lanes.** The owner's question: *"It sounds
like we aren't using cores efficiently — one thread per core, no context switches, no cache
invalidation. Research how these problems have been solved in Rust."* Companion lane reports (full
citations there; this file carries only the conclusions):

- `throughput-2026-08-tpc-runtime-survey.md` — thread-per-core runtimes (lane 1)
- `throughput-2026-08-cache-footprint-survey.md` — cache-footprint engine design (lane 2)
- `throughput-2026-08-arrow-build-path-survey.md` — arrow-rs production build paths (lane 3)

Ground truth throughout = the measured numbers in `docs/architecture/0.17-throughput-mission.md`
(§0, §6): per-core gap A = 1.52 µs/row (1.2 µs unattributed), scaling gap B = LLC capacity
contention, bytes touched ≈ 3.3 KB per 692.7 B logical row (#3027 (row-decode attribution)).

---

## 1. Verdicts

### Thread-per-core: NO (mechanism mismatch, with physics)

- Every documented TPC win (Seastar/ScyllaDB, Redpanda, monoio, Enberg ANCS'19) is a
  coherence/locking/scheduling/syscall win; **none is an LLC-capacity win**, and the public evidence
  base is network echo benchmarks, not CPU-bound columnar streaming.
- Our counters already acquit what TPC fixes: blocked time ≤0.07%, zero voluntary parks,
  instructions/row flat (#3217 (full-box C(N)), #3224 (LLC attribution)).
- **Pinning cannot partition the LLC**: Ice-Lake-class LLCs are hash-sliced *by address*, not
  core-owned (Maurice et al., RAID'15). A migrated task refetches through L1/L2, not LLC — and our
  L1d misses are **flat** while LLC misses triple, which affirmatively acquits work-stealing
  migration as the gap-B mechanism.
- The one TPC benefit that transfers — bounding concurrent live working sets — is available from
  admission control we already ship (`--max-concurrent-scans`), by making it footprint-aware.

### The convergent diagnosis: the owned `Value` row intermediate is BOTH gaps

All three lanes independently land on the same structure. We pay a **double pivot**
(SSTable bytes → owned `Value` row tree → Arrow builders) where one pivot is required; the
intermediate is why we touch 4.75× the logical bytes per row. The fat intermediate is
simultaneously the per-core tax (gap A) and the LLC footprint that stops fitting at 6 cores (gap B)
— which is why footprint levers are the rare class that moves both.

### The 1.2 µs/row mystery now has a NAMED SUSPECT with an external anchor

arrow-avro (in-tree, benchmarked) measured **exactly our architecture** — "deserialize one record
at a time into native Rust values, then build Arrow arrays from those values" — at **9.6× slower**
than its column-first decoder (267 ns/row vs 27.9 ns/row on its bench schema; ~67 ns/field for the
slow path, estimating **~1.07 µs/row at our column width**). Our unattributed 1.2 µs/row lands
almost exactly on the measured row-object path scaled to our width. Cross-anchors agree
(arrow-json/Arroyo 0.4–0.6 µs/record *including* JSON parsing; ConnectorX 2.6 µs/row for an entire
DB-client pipeline). **Judgement: our build region is ~4–10× off achievable (150–400 ns/row for
15–20 mixed columns), not 2× off.**

### Retro-validation of #3096's two zeros (mechanism-level, raises rig confidence)

- Flight framing lever = zero because `split_batch_for_grpc_response` at a 2 MiB target computes one
  size sum and emits a **single zero-copy slice** — there was no work to remove.
- Schema-cache lever = zero because the encoder emits the schema **once per stream** — nothing was
  recomputed.
- The remaining framing cost is tonic's whole-body memcpy (tonic #1558, open upstream) — our
  measured 313 ns/row IPC framing is near the practical floor from our side.

### The per-core arithmetic now closes

Achievable build (150–400 ns/row) + framing floor (~313 ns/row) ≈ **0.5–0.7 µs/row total Flight
overhead**, vs today's 1.52. On bare scan's 2.44 µs/row that projects `do_get` ≈ **318–340k rows/s
per core — above the old 1.3× per-core bar (315,730)**. The per-core target is reachable through
the build path alone, per external anchors. (Projection, not a measurement — #3248 must confirm the
cost is where the anchors say it is.)

### Batch-size math (estimate — VERIFY, this is D2/E2's job)

At the 2 MiB Flight target and ~675 B/row wire, a batch ≈ ~3,100 rows; × 3.3 KB touched/row ≈
**~10 MB working set per in-flight batch region; 6 cores ≈ 61 MB vs a 54 MiB LLC** — right at the
contention boundary, consistent with the measured miss tripling. X100 (CIDR'05, Fig. 10) is the
measured precedent that batch sizing alone moves this class by 1.4–3× when straddling a boundary;
DataFusion re-measured the same (batch 32k→4k: Q1 996→685 ms).

---

## 2. Diagnostics slate (hours each; run BEFORE any lever — #3096 discipline)

| ID | Probe | Cost | Validates/kills | Where |
|---|---|---|---|---|
| **D1** | `size_of::<Value>()` + per-variant sizes | 10 min | enum-shrink headroom (L4) | any box |
| **D2** | Batch-size sweep (rows/s AND LLC-misses/row, S=1 + S=6) | ~2 h | L2 batch sizing; = X100 Fig. 10 on our corpus | metal box (#3299 E2) |
| **D3** | Stub `estimate_value_size` → constant; re-profile | ~1 h | L1 (42%-of-bytes claim) | any box |
| **D4** | Allocations/row + reallocs/builder/batch (dhat / counting allocator) | ~2 h | L3b borrowed-bytes; L3c pre-sizing | any box |
| **D5** | perf self-time: builder vtable thunks / `field_builder` downcasts / `Value` match arms | ~2 h | L3a column-first aim check (<10% ⇒ mis-aimed) | perf-capable box |
| **E1** | `resctrl`/CMT `llc_occupancy` per scan, S=1 + S=6 | ~1 h | capacity hypothesis directly | metal box (#3299) |

**Pre-registered prediction (for #3248):** D5 finds per-cell dispatch + owned-row
materialization ≈ 0.8–1.0 µs/row of the 1.2; D4 finds allocations/row ≈ count of var-len columns.
If NOT, the arrow-avro analogy fails and L3 must not be funded.

## 3. Lever slate (ranked; fund only after the matching diagnostic)

| ID | Lever | Gap | Expected | Risk | Gate |
|---|---|---|---|---|---|
| **L1** | Fuse size accounting into decode; delete the standalone `estimate_value_size` walk (1,398 B/row = 42% of touched bytes) | BOTH | 15–30% scan path | LOW — but verify encoded-vs-decoded size semantics for the v0.13 byte budgets first | D3 |
| **L2** | Byte-budgeted batch sizing (Velox `preferred_output_batch_bytes` model; budget = LLC/concurrent_scans, not LLC/6) | B mostly | 1.4–3× on the scaling loss IF D2 shows a knee | LOW-MED — small batches raise per-batch + framing overhead; interacts with admission | D2/E2 |
| **L3a** | Column-first build: resolve type dispatch + builder downcast ONCE per column per batch; tight per-column loops; per-batch `with_capacity` (NOT builder-object reuse — `finish()` is `mem::take`, capacity is not retained) | A | 2–4× on the 1.2 µs/row region | MED — nested types (`StructBuilder` re-downcasts per call) are where naive ports lose the win | D5 |
| **L3b** | Append borrowed bytes for text/blob (`append_value(impl AsRef<[u8]>)`); conservative form only — `Utf8View`/`append_block` changes the WIRE schema (connector compatibility = owner decision) | A | 1.3–2× on string-heavy schemas | MED | D4 |
| **L3c (full)** | Retire the owned row on the scan→Flight path entirely (decode → builders) | BOTH | ~58% of touched bytes | **HIGH** — reconciliation (tombstones/TTL/merge) operates on owned rows; acceptance gates MUST be the query-semantics oracle + point-vs-full differential (#1918), never the JSONL goldens (symmetric-oracle blind spot) | prototype 1 fixed-width column, single-SSTable, no-merge path first |
| **L4** | Arena (bumpalo) per-batch scratch + box outsized `Value` variants — partial credit if L3c is too expensive (oxc analogue: ~20% + ~10%) | BOTH | ~20-30% | MED — bumpalo skips `Drop`; hazard for resource-owning variants | D1 + synthetic arena bench |
| **L5** | Footprint-aware `--max-concurrent-scans` (cap = effective_LLC × safety / bytes_per_scan) | B stability | small on peak, large on overload stability | LOW | E1 |

**Do NOT pursue:** runtime migration (glommio is seeking maintainers; monoio evidence is echo
benchmarks), further Flight framing tuning (mechanism-explained zero), morsel-driven scheduling
(a NUMA/load-balance tool; our box is one node — the cache-resident unit is the vector, which is L2).

## 4. Relationship to the mission doc

Slots directly into the two-gap frame (mission §6): L3a/b + L1 are the gap-A program #3248 must
license; L2 + L5 (+ L3c's byte cuts) are the gap-B program #3288 must license; #3299's E1/E2 arms
adjudicate the capacity mechanism directly. Nothing here is funded until its diagnostic passes —
the #3096 lesson, applied in advance.
