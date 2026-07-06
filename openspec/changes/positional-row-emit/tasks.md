# Tasks — Positional row emit (K3)

## 1. Work-counter wiring test (TDD, write first)

- [x] 1.1 Add `cqlite-core/tests/issue_1642_positional_row_emit.rs` (feature
      `work-counters`): a full scan asserting `ROW_SORT_INVOCATIONS == 0` on the
      simple table, across the static/collections/wide shapes, and a two-scan
      determinism check (identical per-row column set, zero sorts on both scans).
      FAILS on `main` (the shared builder sorts once per returned live row).
- [x] 1.2 Flip the former `>= rows` currency assertion in
      `issue_1618_parser_work_counters.rs` (`scan_sorts_cells_per_row`) to the K3
      `== 0` reality (renamed `scan_does_not_sort_cells_per_row`).

## 2. Positional construction

- [x] 2.1 Change the `cells` accumulator in `parse_row_data_with_offset_impl` from
      `HashMap<Arc<str>, Value>` to `RowCells` (pre-sized `Vec`); `push` clustering
      pseudo-cells and simple/complex data cells in column order.
- [x] 2.2 Change the `ParsedRow` tuple's first element to `RowCells`.

## 3. Drop the per-row sort

- [x] 3.1 `build_display_row` takes `RowCells`, moves it straight into
      `ScanRow::Row` — no `Vec` re-collect, no `record_row_sort()`, no `sort_by`.
- [x] 3.2 `row_has_non_key_cell`/`extract_clustering_values` take `&[(Arc<str>,
      Value)]`.

## 4. Positional static-cell merge

- [x] 4.1 Add `merge_static_cells(&mut RowCells, &RowCells)` (clustering-row-wins).
- [x] 4.2 Switch `static_cells` in `block_emit`/`block_emit_windowed` (locals +
      the windowed `TimestampPolicy` field) to `RowCells`; replace the
      `HashMap::entry(..).or_insert_with(..)` merges with `merge_static_cells`.
- [x] 4.3 Update `build_compaction_row_data`'s `cells` parameter to `RowCells`.

## 5. Counter + docs

- [x] 5.1 Retain `ROW_SORT_INVOCATIONS` as a regression tripwire; update its
      `read_work_counters.rs` doc to the J1-style "K3 removed the caller" note.
- [x] 5.2 Update the `RowCells`/`ScanRow::Row` doc comments in `types.rs` (order is
      now positional/serialization-header, not alphabetical; not user-visible).

## 6. Gate wiring + validation

- [x] 6.1 Add `--test issue_1642_positional_row_emit` to the `work-counters-guard`
      component in `scripts/agent-gate.sh`.
- [x] 6.2 `cargo +1.88.0 fmt --check` clean; `RUSTFLAGS="-D warnings" cargo clippy
      -p cqlite-core --features cli-helpers` clean.
- [x] 6.3 33-table sstabledump JSONL parity + compaction byte-parity green
      (byte-identical observable output).
- [x] 6.4 `openspec validate positional-row-emit --strict` clean.
