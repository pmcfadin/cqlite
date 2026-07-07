## Why

The July 2026 parser performance audit
(`docs/reports/parser-performance-audit-2026-07-01.md`, Epic K finding **K3**, and
its joint read-path E2 finding in `docs/reports/read-path-performance-audit-2026-07-01.md`)
found the V5CompressedLegacy row-emit path builds each decoded row as a
`HashMap<Arc<str>, Value>` and then **alphabetically re-sorts it on EVERY row**
solely to hide `HashMap` iteration nondeterminism:

- The decoder (`row_decoder/row_data.rs`) inserts each cell into a per-row
  `HashMap` even though it already iterates columns in serialization-header order.
- The shared display-row builder (`row_decoder/mod.rs::build_display_row`,
  into which #1334 consolidated the former three `block_emit`/`block_emit_windowed`
  sort sites) then allocates a `Vec`, bumps the H5 `ROW_SORT_INVOCATIONS` gauge, and
  runs `sort_by(|a,b| a.name.cmp(b.name))` — once per returned live row.

Cost: one throwaway per-row `HashMap` allocation + one per-row `sort_by` in every
scan/compaction emit path. The schema already knows the column order
(`columns_in_order` in `RowColumnResolution`); determinism should come from
CONSTRUCTION, not from a per-row sort.

**Routing: design-driven.** This is a hot-path mechanics change (change the row
carrier's construction discipline), not an oracle-driven parse-correctness bug, so
it is captured as an OpenSpec change per the spec-driven doctrine. The design and
priority are owner-approved via the read-path/parser performance audit (standing
owner Seam-1 approval, v0.14 perf wave); this change encodes that decision rather
than re-litigating it.

Milestone: **v0.14 performance wave** (Epic #1604, row/cell hot-loop mechanics).
This is a **pure factoring** — identical observable output. The public query
result (`QueryRow.values`) is a name-keyed `HashMap`, so the INTERNAL emit order is
not user-visible. Parity is the proof: the 33-table sstabledump JSONL harness and
the compaction byte-parity suite must stay green.

## What Changes

- **Change** the row-assembly carrier in `parse_row_data_with_offset_impl` from a
  per-row `HashMap<Arc<str>, Value>` to a positional `RowCells`
  (`Vec<(Arc<str>, Value)>`), pre-sized to the on-disk column count. Clustering
  pseudo-cells (#229) and simple/complex data cells are `push`ed in
  serialization-header column order — the loop already iterates in that order.
- **Delete** the per-row `sort_by` and the `record_row_sort()` increment from
  `build_display_row`; it now moves the already-ordered `RowCells` straight into
  `ScanRow::Row`. `row_has_non_key_cell` and `extract_clustering_values` take
  slices.
- **Add** `merge_static_cells(&mut RowCells, &RowCells)` — the positional,
  clustering-row-wins analogue of the former
  `HashMap::entry(..).or_insert_with(..)` static-column merge — and switch the
  `static_cells` accumulators in `block_emit`/`block_emit_windowed` to `RowCells`.
- **Retain** the `ROW_SORT_INVOCATIONS` counter as a regression tripwire (no
  production caller remains; any reintroduced per-row sort must record it).
- **Add** a `work-counters` wiring test (`issue_1642_positional_row_emit.rs`)
  asserting `ROW_SORT_INVOCATIONS == 0` on a full scan across the four column
  shapes (simple / static / collections / wide) and two-scan
  determinism-by-construction; flip the former `>= rows` currency assertion in
  `issue_1618_parser_work_counters.rs` to `== 0`. Both FAIL on `main`.

## Non-goals

- **K4 (#1643) `Arc<RowKey>`/`Arc<TableId>` identity handles** — the next link in
  the K-emit chain. This change does NOT fold in that work; where a K4 seam appears
  it is left as a `TODO` referencing the owning issue.
- **No change to the public row representation.** `QueryRow.values` stays a
  name-keyed map; the observable query result (column values and the ordering the
  public API presents) is byte-identical. Only the INTERNAL emit path goes
  positional.
- **No change to column-name case semantics** — no lowercasing or re-keying of
  column names (the anti-pattern the audit explicitly forbids).
- **Not** re-litigating the pre-`na` version floor or the no-heuristics mandate.
