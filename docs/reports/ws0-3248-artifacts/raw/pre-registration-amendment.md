# Pre-registration amendment — three pre-registered probes name mechanisms that are NOT on the measured path

**Status: reported before measuring, as an amendment, not applied silently.** The predictions and
probes on this issue were pre-registered *deliberately* (mission rule 1: post before measuring), so
redefining one quietly would destroy the thing pre-registration buys. Each item below states what was
registered, what the code actually contains, how that was verified, and what this work will do instead.

Every claim here was **verified directly against the tree**, not taken from a summary.

---

## 1. Probe D3 targets a function that never executes on the measured path

**Registered:** *"probe D3 — stub `estimate_value_size` to a constant in a throwaway branch and
re-profile (validates lever L1, the 42%-of-touched-bytes deletion)."*

**Finding: `estimate_value_size` is not called by either arm.** Stubbing it would have measured
**exactly zero, by construction.**

Verified:

* `cqlite_core::memory::estimate_value_size` — `cqlite-core/src/memory/mod.rs:89`, `pub(crate)`. Its
  only non-recursive, non-test caller is `query::result_budget::estimate_query_row_bytes`
  (`result_budget.rs:36`).
* That is reached only from `QueryEngine::enforce_legacy_result_budget` (`query/engine.rs:186`), called
  at `engine.rs:253`, `:274`, `:336` — all on the **materializing `execute()`** path (legacy
  point-lookup, plan-cache hit, non-SELECT). The function's own doc comment says ad-hoc SELECTs route
  through `SelectExecutor`, "which budgets itself".
* Neither `execute_streaming` (the bare-scan arm) nor the Flight `do_get` path reaches it.
* Two same-named but unrelated functions exist on the **write** path and are not involved:
  `Memtable::estimate_value_size` (`storage/write_engine/memtable.rs:227`) and
  `merge/mod.rs:377`.

**Why this matters more than a naming slip.** Had D3 been run as written, it would have produced a
clean, plausible **zero** — and a zero here is not neutral. #3096 already reported two levers
"measuring at ZERO", so a third zero would have read as corroboration of a real pattern
("array-build costs are irreducible") when it was purely an artifact of stubbing dead code. **A
vacuous zero is indistinguishable from a genuine negative result**, which is this issue's whole
subject matter one level up.

**Amendment.** D3's stated *intent* is unambiguous — it exists to price **the sizing pre-pass**, and
the issue body's own lever list names "**lever 3 (fold the estimate pass)**". The sizing pre-pass
actually on the Flight path is:

`cqlite_core::export::arrow_size::estimate_arrow_row_bytes` — `cqlite-core/src/export/arrow_size.rs:251`

So D3 is redirected to that function. This serves the registered intent exactly; it is recorded as an
amendment rather than performed silently.

---

## 2. Prediction P1 names an Arrow API this tree does not use

**Registered:** *"P1: per-cell type dispatch + builder downcasts + owned-row materialization
(`Value` match arms, `field_builder::<T>` downcasts, `into_owned`, drop glue) account for 0.8–1.0
µs/row of the 1.2 unattributed."*

**Finding: `field_builder::<T>` does not exist in this codebase.** Verified: zero occurrences of
`field_builder`, `StructBuilder`, `ListBuilder` or `MapBuilder` across `cqlite-core/src` and
`cqlite-flight/src`.

The encode region does **not** use Arrow's generic builder-downcast API. It builds per-column
`Vec<Option<T>>` and hands them to `XArray::from(vec)`. Type dispatch is a plain `match` on
`ColumnInfo` in `convert_column_to_array` (`export/arrow_convert.rs:456`) — not a builder downcast.

**Consequence.** P1's other three components *do* exist and remain testable: the `Value` match arms
(`arrow_convert.rs:456` dispatch, `arrow_builders_scalar.rs:40-486`), `into_owned`
(`query/select_executor/row_build.rs:259` → `types.rs:1234`), and drop glue. The `field_builder`
component is **structurally absent — not measured at zero.** That distinction is kept explicit in
the report, because "we measured it and it was zero" and "the mechanism does not exist" license very
different conclusions about the arrow-avro analogy P1 was derived from.

---

## 3. Prediction P3's premise does not hold on the scalar path

**Registered:** *"P3: builder realloc count per batch is >2 (no per-batch `with_capacity`
pre-sizing; note `finish()` is `mem::take` — capacity is NOT retained, so any 'builder reuse' in our
code buys nothing)."*

**Finding: the scalar path uses no Arrow builder at all**, so there is no realloc-count-per-builder
and no `finish()`/`mem::take` question to ask of it. `XArray::from(Vec<Option<T>>)` has neither.

Only four `with_capacity` builder sites exist, and both types they cover are **already pre-sized**:

| site | builder | pre-sized? |
|---|---|---|
| `arrow_builders_scalar.rs:300`, `:469` | `FixedSizeBinaryBuilder::with_capacity(cells.len(), 16)` | **yes** |
| `arrow_builders_scalar.rs:366`, `:398` | `Decimal128Builder::with_capacity(cells.len())` | **yes** |

On `ws0.events` the only column reaching either is `device_id uuid` → `FixedSizeBinary`, which is
therefore **pre-sized**, contradicting P3 for the one column it can apply to. The `finish()` calls that
exist (`:313, :388, :440, :482`) are on locals dropped immediately, so no builder is reused across
batches and the `mem::take` observation — true of Arrow's own `GenericByteBuilder` upstream — has no
CQLite-side subject.

**P3 is reported as structurally inapplicable to this tree**, with the caveat that Arrow's *internal*
buffer growth inside `XArray::from` is a separate question that a per-function profile can still see.

---

## 4. Prediction P2 predicts a quantity that largely CANCELS in the differential

**Registered:** *"P2: allocations/row ≈ the count of var-len columns (owned String/Vec
materialization before append)."* On `ws0.events` that is **6** (`part_id`, `blob_a`, `blob_b`,
`payload`, `region`, `status`).

**Finding: the per-row owned materialization is on the SHARED path, so it cancels in AC2's
differential.** The single `into_owned` site is `row_build.rs:259`, inside
`build_row_from_scan_cached` — which **both** arms execute. The encode region's own allocations are
per **batch**, not per row: `transpose_columns` (`export/arrow_columnar.rs:59-102`) allocates
`n_cols` inner `vec![None; n_rows]` plus a name→index map, and each `build_*_array` collects a second
`Vec<Option<T>>` — roughly `2 × n_cols` vectors per batch, amortized over the batch's rows.

P2 therefore remains measurable but its reading changes: **a ~6 allocations/row figure would be a
shared cost, and per AC2 shared cost cannot close the ratio.** If a *Flight-marginal* allocation delta
of that order appears, the suspects are `estimate_arrow_row_bytes` and the transpose, **not**
`into_owned`.

---

## 5. Correction to an inherited corpus fact

A peer agent's map stated the corpus table is LZ4-compressed with `chunk_length_in_kb = 16`. **That
describes the #3026 corpus, not the #3096 corpus this issue measures.** Verified:

| | #3096 corpus (`/data/ws0-3096`, AC0's) | #3026 corpus |
|---|---|---|
| `CompressionInfo.db` | **absent** (0 files) | present |
| `compression_info_present` | **`False`** | — |
| DDL compression clause | **none** (`WITH CLUSTERING ORDER BY …` only) | `LZ4Compressor, chunk_length_in_kb 16` |

So on AC0's corpus **decompression cost is not merely "shared" — it is absent.** That matters for the
bytes-touched-per-row differential, whose reading would otherwise carry a decompression term that
does not exist here.

---

## What this section is NOT

None of the above is an argument that the levers are worthless or that the analogy behind P1 fails.
It is narrower and only about instrument aim: **three of the registered probes, run as written, would
have returned numbers that describe absent or off-path code**, and at least one of them (D3) would
have returned a *zero* that looked exactly like a finding. The predictions are re-aimed at the
mechanisms this tree actually contains, and every re-aim is recorded here with its verification so a
reader can judge whether the intent was preserved.
