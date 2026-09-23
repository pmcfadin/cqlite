# Design — vortex-export (issue #4237)

This document carries the design decisions this OpenSpec change is responsible for. The
product-level decisions (role of Vortex, row semantics, version pins, Rust floor) were made by the
owner on 2026-09-18 and are recorded in `docs/plans/2026-09-18-vortex-export-arrow59-design.md`
(PR #4235) — **not re-opened here.** This document only resolves the implementation-level choices
the design doc left to the implementing change: where the shared Arrow producer lives, how the
gate exercises a test that needs two non-default features at once, and the test-file layout.

## D1 — The shared Arrow producer is a new adapter, not a refactor of the Parquet writer

The design doc requires Parquet and Vortex to consume "the SAME `RecordBatch` stream" with "no
second CQL→Arrow mapping, no bridge." Today's CLI export loop (`cqlite-cli/src/commands/export.rs`)
calls `ExportFormat::Parquet`'s arm directly with `&[QueryRow]` chunks; there is no existing
`RecordBatch`-stream abstraction shared across formats.

**Decision:** add one small adapter, `cqlite_core::export::batch_stream` (or a function on the
existing `arrow_convert` module — implementer's naming call, not a spec requirement), that wraps
`rows_to_record_batch`/`rows_to_record_batch_with_schema` and yields the same `RecordBatch` per
input chunk that the Parquet writer already builds internally. Both `ExportFormat::Parquet` and the
new `ExportFormat::Vortex` arm in `export.rs` and `export_sstable.rs` call this adapter and hand the
result to their respective format writer. This is additive: `ParquetExportOptions`/
`ParquetExportWriter`'s existing internal batch construction is left alone (no behavior change,
no byte drift risk to the golden-proven Parquet path) — the adapter is a new call site the Vortex
writer uses, and the Parquet call sites are refactored to source their batches from the same
adapter, but the *conversion logic itself* (already `arrow_convert::rows_to_record_batch`) is not
touched. **What beat what:** an alternative that gave Vortex its own copy of the row→batch mapping
was rejected — that is exactly the "second CQL→Arrow mapping" the design doc rules out, and would
reopen the type-mapping verification work the design doc already did against `vortex-arrow`'s
`convert.rs`.

## D2 — Chunk/row-group boundary normalization in the differential test

Parquet chunks at `row_group_size` (10,000 rows); Vortex's writer picks its own physical chunk
layout from its pinned session default. The proposal's item 1 requires comparing "batch-for-batch
... after normalizing physical chunk/row-group boundaries" — this means the test reads each format
back to Arrow with its own reader, concatenates all record batches per column into one logical
column vector per side (a `compute::concat_batches`-shaped comparison, or equivalent row-by-row
comparison after flattening), and compares values and schema — never asserting the two writers
produced the same number of physical chunks. This is the same normalization Parquet-vs-CQLite
row-group-size changes already tolerate; nothing new is invented, just made explicit so a reviewer
doesn't read a chunk-count assertion as intended coverage.

## D3 — Gate wiring: a new component runs the two-non-default-feature primary oracle

Surveyed `scripts/agent-gate.sh` (origin/main, `79db00db7`): every existing component enabling a
non-default feature does so alone or in an already-fixed pair —
`write-tests` hardcodes `--features write-support`; `feature-iso-parquet` and
`feature-iso-delta-scan` are each a SINGLE feature in MUTUAL isolation from the other by
construction (#1699); `all-features-check` runs `cargo check`/`clippy` (never `cargo test`) at
`-p cqlite-core --all-features`. **No existing component executes a test with `parquet` AND
`vortex` enabled together.** The primary cross-format-differential test (proposal item 1) needs
exactly that, so it cannot land in any existing lane unchanged.

**Decision:** two new, narrowly-scoped components, keeping the existing isolation lanes'
contracts untouched:

1. `feature-iso-vortex` — mirrors `feature-iso-delta-scan`'s *executing* form (not
   `feature-iso-parquet`'s compile-only form): `cargo test -p cqlite-core --no-default-features
   --features <defaults-minus-parquet-minus-delta-scan>,vortex --lib`, proving the `vortex` feature
   compiles and its own unit tests (type-mapping helpers, the UUID-extension pin at the unit level)
   pass with neither `parquet` nor `delta-scan` present. Compile-only would not be enough here
   because the UUID-extension-pin unit test (proposal item 2) needs to actually run somewhere
   `parquet` is absent, to prove Vortex's own importer path — not a shared helper only exercised
   under `parquet` — carries the extension.
2. `vortex-parquet-differential` — a new component, same `run_component` shape as `write-tests`:
   `cargo test -p cqlite-cli --features parquet,vortex --test issue_4237_vortex_parquet_differential`
   (plus any split files the campsite file-size rule forces), `CQLITE_REQUIRE_FIXTURES=1`, added to
   `DATASET_COMPONENTS` (it reads the 33 fixture tables) and to the `COMPONENTS=(...)` array in the
   same edit as the new test target, per the pattern every prior feature addition (#3453, #1699)
   used. It is CLI-level (not `cqlite-core`-level) because proposal item 5 (wiring evidence) requires
   the compiled binary, and the differential is most naturally driven at the `cqlite export`
   surface where both format flags already exist.

This is an implementation/gate-wiring decision, not a product one — no requirement's meaning
changes based on which component name runs it, and the choice follows the file's own established
per-feature-combination pattern (a new named component per distinct feature set, never widening an
existing one to change its meaning). Flagged in design rather than left implicit so a reviewer
doesn't read the design doc's one-line "Gate: named `--test` targets in a write-tests-style list"
as already covering the two-feature case.

## D4 — Test file layout

`cqlite-cli/tests/issue_4237_vortex_parquet_differential.rs` (proposal item 1, `required-features =
["parquet", "vortex"]`) drives the compiled binary once per fixture table (33 tables) plus the
declared per-CQL-type-family cases from `test-data/schemas`; if the campsite `~1500`-line test-file
threshold is at risk, the per-type-family cases split into a sibling
`issue_4237_vortex_type_coverage.rs`, same `required-features`. `cqlite-core/src/export/vortex.rs`
carries the writer plus its own `#[cfg(test)]` unit tests (options defaulting, error-shape
construction) — no fixture I/O at that layer, matching `parquet.rs`'s own split between library unit
tests and `cqlite-cli/tests/*parquet*` integration coverage.

## Alternatives considered and rejected (owner-settled, recorded for completeness)

- **Isolated Vortex-only Arrow crate with an IPC bridge** — rejected by the owner in favor of one
  arrow-59 tree (`docs/plans/2026-09-18-vortex-export-arrow59-design.md` decision 3).
  Not re-litigated here.
- **A Vortex-specific CQL→Arrow type map** — rejected; verified unnecessary against
  `vortex-arrow/src/convert.rs` (design doc, "Type map: NONE added").
