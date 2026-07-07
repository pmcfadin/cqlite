# Tasks — Per-column resolved dispatch (J1)

## 1. TDD: flip the H5 counter tests to the post-J1 contract (must fail on main)
- [ ] 1.1 In `cqlite-core/tests/issue_1618_parser_work_counters.rs`, flip
  `scan_normalizes_type_per_cell` from `assert!(normalizes >= rows)` to
  `assert_eq!(normalizes, 0)` (and rename to reflect zero per-cell normalization); flip
  `scan_counts_empty_cell_type_normalization` from `assert_eq!(normalizes, 29)` to
  `assert_eq!(normalizes, 0)`. Both assertions FAIL on `main` and PASS after the impl.
  *Surface exercised:* `Database::execute` full scan + `read_work_counters::type_normalize_calls`.

## 2. Add the per-column dispatch tag
- [ ] 2.1 Add `CellKind` (small enum, scalar variants 1:1 with the `cell_value.rs` decode
  arms + `Complex(Arc<str>)`), with `#[must_use] fn from_type(&str) -> CellKind` that
  lowercases once and maps from authoritative metadata (no byte inspection). New file
  `row_decoder/cell_kind.rs`; `pub(super) use` from the module.
  *Surface exercised:* `CellKind::from_type` (unit-tested for the full type map).

## 3. Extend the once-per-block resolution
- [ ] 3.1 Add `kind: CellKind` and `is_complex: bool` to `ColumnToParse`; compute both in
  `RowColumnResolution::build` per column (`kind` from the value-type = schema type or
  header type for dropped; `is_complex` from `header_type.unwrap_or(value_type)` via
  `is_complex_column`, at bind time — once per column, not per row).
  *Surface exercised:* `RowColumnResolution::build` (exercised by every V5 block scan).

## 4. Dispatch the per-cell decode on the tag (delete per-cell to_lowercase)
- [ ] 4.1 `parse_cell_value_schema_order`: add `kind: Option<&CellKind>`; match on it for
  the scalar arms and the empty-value early-return; keep the frozen/tuple/collection/
  marshal-UDT/default ladder verbatim inside `CellKind::Complex(lowered)`. Delete the
  per-cell `record_type_normalize()` + `to_lowercase()`. `None` (recursion/tests) computes
  `CellKind::from_type(&column.data_type)` locally.
  *Surface exercised:* `parse_cell_value_schema_order` (lockstep + parity + counter tests).
- [ ] 4.2 `row_data.rs`: branch on `ctp.is_complex` instead of `is_complex_column(...)`;
  pass `Some(&ctp.kind)` to `parse_cell_value_schema_order`.
  *Surface exercised:* `parse_row_data_with_offset` row loop.
- [ ] 4.3 Remove the `record_type_normalize()` call from `is_complex_column` (it is now a
  bind-time, not per-cell, call).
  *Surface exercised:* `is_complex_column` (still unit-tested for its boolean result).

## 5. Gate + reviews
- [ ] 5.1 `scripts/agent-gate.sh --lite` PASS each fix round; full `scripts/agent-gate.sh`
  once before merge (run by the orchestrator).
- [ ] 5.2 `openspec validate per-column-resolved-dispatch --strict` clean.
- [ ] 5.3 Intent audit **C** (`spec-auditor`) PASS; roborev clean.
