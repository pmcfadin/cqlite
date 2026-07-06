# Design — Per-column resolved dispatch (J1)

## Context

`RowColumnResolution::build` (issue #1046) already resolves the header→schema column
ordering ONCE per SSTable block into a `Vec<ColumnToParse>` reused across every row.
Each `ColumnToParse` already carries the interned emit `name: Arc<str>` and the
authoritative `header_type`. It never carried the *decode dispatch decision*, so the
per-row/per-cell decode re-derived it: `column.data_type.to_lowercase()` +
`is_complex_column(complex_type)` (a second `to_lowercase`) on every cell.

## Decision 1 — a purpose-built `CellKind` tag, not `ComparatorType`

The audit permits reusing `ComparatorType` if suitable. It is **not** a clean fit for the
decode dispatch:

- `ComparatorType::from_cql_type` maps `Time` and `Inet` to `Custom("time")` /
  `Custom("inet")`, collapsing two distinct decode arms into the `Custom` bucket. The V5
  cell decoder has dedicated `time` and `inet` arms; routing them through `Custom` would
  change decode behavior.
- `ComparatorType` is a 72-byte enum built for comparison semantics; the decode dispatch
  needs only a small jump-table tag.

So J1 introduces a small `CellKind` enum whose scalar variants map **1:1** onto the
existing `cell_value.rs` decode arms (`Boolean`, `Int`, `Text`, `Uuid`, `Decimal`,
`BigInt`, `Counter`, `Double`, `Timestamp`, `Date`, `Duration`, `Float`, `SmallInt`,
`TinyInt`, `Time`, `Inet`, `Blob`), plus one `Complex(Arc<str>)` variant that carries the
**already-lowercased** type string for the frozen / tuple / non-frozen-collection /
marshal-UDT / unknown-scalar slow paths. This is the "thin per-file adapter" the audit
says J2 collapses.

`CellKind::from_type(&str)` lowercases the type string **once** and matches — the same
normalization the per-cell path did, now paid once per column at bind time. It is
declared `#[must_use]`, pure, and never inspects value bytes (no-heuristics).

## Decision 2 — which authoritative type feeds each derived field (exact parity)

The pre-J1 code derives two different things from two different type sources, and J1
preserves that split exactly:

- **Complex-ness** used `complex_type = header_type.unwrap_or(&column.data_type)` — the
  on-disk marshal type preferred (it carries `UserType(...)` a bare CQL short form
  cannot). So `is_complex = is_complex_column(complex_type)`.
- **Scalar decode dispatch** used `column.data_type.to_lowercase()` — the supplied-schema
  type (or, for a DROPPED column, the synthetic header type). So `kind` is derived from
  `value_type = schema.map(|c| c.data_type).unwrap_or(header_type|"blob")`.

Both are computed inside `build()` per column, from the same borrowed strings already in
scope. `is_complex_column` is called at bind time (once per column), not per row.

## Decision 3 — the H5 counter flips to per-cell-loop == 0

`TYPE_NORMALIZE_CALLS` (issue #1618, `read_work_counters`) was documented as measuring the
**per-cell decode path** normalization, with consumer J1 flipping its assertion to `== 0`.
J1 removes both counted `record_type_normalize()` sites (the `cell_value.rs` per-cell
normalize and the now-bind-time `is_complex_column`). Bind-time normalization is a fixed
O(columns-per-block) cost measured by dhat, deliberately **not** counted by this per-cell
gauge. After J1 a full fixture scan records `TYPE_NORMALIZE_CALLS == 0`, and the same
assertion FAILs on `main` (≥2/cell). The counter stays wired so any reintroduced per-cell
normalization is caught.

## Decision 4 — signature threading

`parse_cell_value_schema_order` takes `kind: Option<&CellKind>`. The hot loop passes
`Some(&ctp.kind)` (precomputed). Recursive frozen-inner calls and in-crate test callers
pass `None`, for which the function computes `CellKind::from_type(&column.data_type)`
locally — a rare, bounded path (frozen inner primitives / tests), not the per-cell scan
loop, so it neither regresses the hot path nor the counter.

## Risks / parity net

The refactor is covered by the H4 lockstep suite (`decoder_lockstep_tests.rs`), the 33-
table JSONL parity suite, and the flipped H5 counter tests. The `CellKind::Complex` arm
retains the existing string ladder verbatim (only the match scrutinee changes), so complex
/ frozen / UDT decode is byte-identical.
