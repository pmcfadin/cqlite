## Why

The July 2026 read-path / parser performance audit
(`docs/reports/parser-performance-audit-2026-07-01.md` finding **J1**, and
`docs/reports/read-path-performance-audit-2026-07-01.md` §Epic J) named per-cell type
dispatch as **the single biggest hot-path lever in the parser**.

Type dispatch is resolved **per cell** for a type that is constant **per column**:

- `row_decoder/cell_value.rs` calls `column.data_type.to_lowercase()` on every
  non-tombstone cell, then walks a ~30-arm string-match ladder.
- `row_decoder/udt.rs::is_complex_column` calls a second `to_lowercase()` on the
  same string, once per column **per row** (`row_data.rs`).

A 1M-row × 10-col scan performs ~20M transient type-string allocations producing nothing
but a branch target. Two `to_lowercase` normalizations fire per non-key cell today; the
H5 work-counter `TYPE_NORMALIZE_CALLS` measures this and reads ≥2/cell on `main`.

The correct pattern already exists in-tree: `RowColumnResolution::build`
(`row_decoder/mod.rs`, issue #1046) resolves the header→schema column ordering
ONCE per block. It never absorbed dispatch. This change extends it.

**Routing: design-driven, owner-pre-decided.** The audit is the source of truth. This
change encodes J1's locked decision with **no new design latitude** (standing owner
Seam-1 approval, 2026-07-06 drain directive).

**No-heuristics guardrail (issue #28):** the per-column dispatch tag is derived ONLY from
authoritative column metadata (the on-disk SerializationHeader marshal type / supplied
schema type) — never inferred from value byte patterns. The type-string parse happens
once at bind time; it is exactly the metadata the per-cell `to_lowercase` was re-deriving.

Milestone: **v0.14 perf wave** (Epic J headline). No change to any decoded value.

## What Changes

- **Extend `ColumnToParse`** (the per-block resolution entry) with a precomputed
  dispatch tag `CellKind` and an `is_complex: bool`, both computed ONCE per column at
  `RowColumnResolution::build` time from the authoritative type string. `CellKind` maps
  1:1 onto the existing scalar decode arms; a `Complex` variant carries the
  already-lowercased type string for the frozen/tuple/collection/UDT slow paths (a thin
  adapter that Epic J2 later collapses — this issue makes *dispatch* per-column).
- **Dispatch the per-cell decode on the precomputed tag**: `parse_cell_value_schema_order`
  matches on `CellKind` instead of re-lowercasing and string-matching every cell; the row
  body (`row_data.rs`) reads `ctp.is_complex` instead of calling `is_complex_column` per
  row. The per-cell `to_lowercase()` calls are deleted from the hot loop.
- **Preserve exact decode output** (byte-for-byte parity across all CQL types), including
  the empty-value early-return (`text`/`varchar`/`ascii` → `Text("")`, `blob` →
  `Blob([])`, everything else → `Null`) and the complex/frozen slow path.

## Non-goals

- **Consolidating the ladder bodies** (the three `ComparatorType` decoder implementations
  and the 5×-copied string ladder in `raw_value.rs` et al.) — that is Epic **J2**. J1
  makes the *dispatch decision* per-column; the complex/unknown decode bodies stay behind
  the `CellKind::Complex` adapter.
- **Hoisting `value_parsing.rs`'s per-value `ComparatorType::from_data_type`** through the
  state-machine path — a separate producer, not the counted V5 block-emit cell loop; left
  to a J2/follow-up.
- Any change to write/compaction paths or to the `tombstones`-gated semantics.
