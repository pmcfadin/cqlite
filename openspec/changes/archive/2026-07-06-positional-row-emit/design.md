# Design — Positional row emit (K3)

## Context

`parse_row_data_with_offset_impl` decodes a clustering/static row's cells by
iterating `RowColumnResolution::columns_for(is_static)` — already in
serialization-header (schema) column order — and inserting each decoded value into
a per-row `HashMap<Arc<str>, Value>`. Because a `HashMap` has nondeterministic
iteration order, the shared `build_display_row` re-materializes the map into a
`Vec` and `sort_by`-s it alphabetically per row so that scan output is
deterministic. That is a per-row allocation + per-row sort on the hottest read
path, entirely to undo the nondeterminism the `HashMap` introduced.

The row carrier that crosses the storage → query boundary is already an ordered
`RowCells = Vec<(Arc<str>, Value)>` (issue #1334); only the row's INTERNAL assembly
used a `HashMap`.

## Decision 1 — Build the row positionally; delete the sort

The cell accumulator becomes a `RowCells` (`Vec`), pre-sized to the on-disk column
count, populated by `push` in the existing column-iteration order. Because each
column is visited exactly once and clustering pseudo-cells are pushed once before
the loop, the vector has no duplicate keys — the same invariant the `HashMap`
enforced implicitly. `build_display_row` then moves the already-ordered vector into
`ScanRow::Row` with no `sort_by` and no `record_row_sort()`.

**Determinism-by-construction:** two scans of the same fixture see the identical
column-iteration order (it is fixed by the serialization header, not by a hash
seed), so the emitted order is stable without any sort.

**Observability:** the public query result is `QueryRow.values: HashMap<..>`
(name-keyed) built in `build_row_from_scan`; the CLI display path
(`data_parser.rs::to_string_vec`) reorders by an explicit `column_order`. Neither
depends on `RowCells` order, so the change is byte-neutral. This is asserted by the
33-table parity/goldens harness and directly by a two-scan column-set equality
test.

## Decision 2 — Positional static-cell merge (`merge_static_cells`)

The former static-column merge used `HashMap::entry(k).or_insert_with(..)`
("clustering-row-wins"). The `Vec` analogue appends a static cell only when the
clustering row does not already carry that column (a linear membership check over a
handful of columns). Static and regular columns are disjoint in Cassandra, so this
is effectively a concatenation; the membership check preserves the exact
clustering-row-wins precedence defensively. The merged order (clustering-row cells,
then appended static cells) is never user-visible.

## Decision 3 — Keep the metadata maps keyed

`want_cell_metadata`'s `cell_meta`/`complex_col_meta` maps
(`HashMap<String, _>`) and the `static_cell_meta` merge keep their current keying —
they are cold, off the read hot path, and consumed by name. Only the value-cell
carrier goes positional (the audit says "keep their current keying unless trivially
unifiable"; unifying them is out of scope).

## Decision 4 — Retain `ROW_SORT_INVOCATIONS` as a tripwire

No production site calls `record_row_sort()` after this change. The counter is kept
(unconditional, zero-overhead in release) as a regression tripwire, mirroring the
J1 `TYPE_NORMALIZE_CALLS` treatment: any future per-row cell sort must record it,
which would flip the `== 0` scan assertions red.

## Risks & mitigations

- **Risk: a hidden consumer relies on alphabetical `RowCells` order.** Mitigation:
  audited every `ScanRow::Row`/`RowCells` consumer — `QueryRow.values` is a
  name-keyed map, the CLI reorders by explicit `column_order`, compaction
  re-sorts its own `SimpleCell`/`ComplexColumn` vectors by name, and the
  schema-discovery sampler is order-agnostic. The 33-table parity + compaction
  byte-parity suites are the end-to-end proof.
- **Risk: `extract_clustering_values` `HashMap::get` → linear scan.** Mitigation:
  clustering arity is tiny (single digits) and this runs only when a range
  tombstone is open, off the tombstone-free hot path.

## Alternatives considered

- **Keep the `HashMap` but sort with a stable key once** — still pays the per-row
  map allocation and a per-row sort; does not address the finding.
- **Emit a positional `Vec<Value>` + a shared `Arc<[Arc<str>]>` header** (the
  audit's "preferred" long-term shape) — a larger carrier-type change that ripples
  into every `RowCells` consumer and overlaps K4/E5. Deferred; this change keeps
  the existing `Vec<(Arc<str>, Value)>` carrier and only fixes its CONSTRUCTION,
  which removes the map + sort with a contained blast radius.
