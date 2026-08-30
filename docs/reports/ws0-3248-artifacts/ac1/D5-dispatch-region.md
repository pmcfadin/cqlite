# Probe D5 — self-time in the dispatch/downcast/match region

**D5's registered criterion:** perf self-time in the dispatch/downcast/match region; **<10% of the
build region ⇒ L3a is mis-aimed.**

## The answer, and why the threshold alone does not decide it

**The verdict FLIPS depending on which of two things "the dispatch region" means, and the two
CANNOT BE SEPARATED on a codegen-faithful binary.** Reporting a single number against the 10%
threshold would therefore be a choice disguised as a measurement.

The build region, measured: `rows_to_record_batch` 1,881 + builder closures 683 +
`convert_column_to_array` 51 + `arrow_array` builders 77 ≈ **2,692 cyc/row**.

| candidate reading of "dispatch region" | cyc/row | share of build region | verdict vs 10% |
|---|--:|--:|---|
| **COLUMN-level type dispatch** — `convert_column_to_array`, the `match` on `ColumnInfo` | 51 | **1.9%** | **mis-aimed** |
| **PER-CELL `Value` enum match** — fused inside the builder closures | ≤683 | **≤25.4%** | **not mis-aimed** |

**Why they cannot be separated.** The per-cell match is the closure passed to
`cells.iter().map(…).collect::<Result<Vec<Option<T>>,_>>()`, and the symbol perf attributes it to is
the *fused* iterator adapter — verified from the demangled name:

```
<GenericShunt<Map<slice::Iter<Option<&Value>>,
   arrow_builders_scalar::build_string_array::{closure#0}>, …> as Iterator>::next
```

That single symbol contains **both** the per-cell `Value` match **and** the intermediate
`Vec<Option<T>>` construction that **lever 2** exists to delete. Separating them needs
`#[inline(never)]` on the closure, which perturbs codegen — so on the codegen-faithful binary the
attribution floor sits above the distinction D5 asks about.

**Consequence for AC3 that is worth more than D5's own verdict: lever 2's 668 cyc/row is an UPPER
BOUND, not a price.** Some of it is the per-cell match, which you must still perform after deleting
the intermediate `Vec`. Anyone funding lever 2 on "668 cyc/row" would be funding a number that
includes work the lever does not remove.

## The substantive finding: L3a is mis-aimed, but not for the reason D5 tests for

D5's threshold implicitly assumes the dispatch region might be *large*. It is not, and the reason is
structural rather than quantitative:

1. **There are no builder downcasts at all.** P1 predicted `field_builder::<T>` downcasts; that API
   does not exist in this tree (verified: zero occurrences of `field_builder`, `StructBuilder`,
   `ListBuilder`, `MapBuilder` in `cqlite-core/src` or `cqlite-flight/src`).
2. **The dispatch is ALREADY column-major.** `convert_column_to_array` runs `n_cols` times per
   *batch* — not `n_rows × n_cols` times — which is why it measures 1.9%. At batch 8192 that is one
   dispatch per 8,192 rows per column.

**So L3a's premise is already satisfied.** "Column-first, to eliminate per-cell type dispatch" has
nothing left to eliminate at the dispatch level: the code is column-first there today. What remains
per-cell is reading a value **out of a `Value` enum**, and column-first cannot remove that — the
representation is what forces it.

**Which lands exactly on the shared-path inversion this issue exists to catch.** Removing the
per-cell `Value` read means changing the row representation, and the row representation is
**shared** — `build_row_from_scan_cached` builds it for both arms. Per the AC2 differential, a
shared-path change gains more in *absolute* throughput than any Flight-marginal lever while moving
the **ratio the wrong way**. So the honest reading of D5 is:

> The dispatch region is small because the column-first transformation L3a proposes is already
> done. The remaining per-cell cost is a property of the `Value` representation, and attacking it
> is a shared-path lever whose ratio delta is adverse.

## Per-column-builder attribution (a bonus AC1 result at column granularity)

Mean of 3 reps, Flight arm, converted at 26,854 cyc/row:

| cyc/row | share | builder | `ws0.events` columns served | per column |
|--:|--:|---|---|--:|
| 386 | 1.44% | `build_string_array` | `part_id`, `payload`, `region`, `status` (4 text, **var-len**) | ~96 |
| 98 | 0.36% | `build_binary_array` | `blob_a`, `blob_b` (2 blob, **var-len**) | ~49 |
| 72 | 0.27% | `build_int32_array` | `seq`, `metric_a` (2 int, fixed) | ~36 |
| 39 | 0.15% | `build_float64_array` | `metric_c` (1 double, fixed) | ~39 |
| 38 | 0.14% | `build_int64_array` | `metric_b` (1 bigint, fixed) | ~38 |
| 35 | 0.13% | `build_timestamp_array` | `event_time` (1 timestamp, fixed) | ~35 |
| 19 | 0.07% | `FixedSizeBinaryBuilder::append_value` | `device_id` (1 uuid, fixed) | ~19 |

**Var-len columns cost roughly 2–3x a fixed-width column each** (~96 cyc/row for text, ~49 for
blob, against 19–39 for the fixed types). With 6 var-len and 6 fixed columns, the var-len half
accounts for **484 of the 687 cyc/row** of per-column builder work — **70%**.

That is a shape result, not just a total: it says the encode cost of this corpus is dominated by its
**text and blob** columns, so a lever aimed at fixed-width column handling has little to win here,
while the corpus's own column mix largely determines the figure. A different table shape would move
this number substantially, which is a caveat on generalising **any** of this issue's absolute
per-row figures to other schemas.
