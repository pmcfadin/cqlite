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

**Owner Seam-1 ruling 2026-09-23 (slim gate wiring):** at spec approval the owner narrowed this
from my original two-executing-lane proposal to ONE executing new component. `feature-iso-vortex`
is **compile-only**, mirroring `feature-iso-parquet` exactly rather than `feature-iso-delta-scan`'s
executing form; `vortex-parquet-differential` is the ONLY new component that runs tests, and it
absorbs the unit-level coverage the original design would have run inside `feature-iso-vortex`.
Decision below reflects that ruling — not re-litigated.

**Decision:** two new, narrowly-scoped components, keeping the existing isolation lanes'
contracts untouched:

1. `feature-iso-vortex` — **compile-only**, mirroring `feature-iso-parquet`'s existing form
   exactly: `cargo test -p cqlite-core --no-default-features --features
   <defaults-minus-parquet-minus-delta-scan>,vortex --lib --no-run`, proving the `vortex` feature
   compiles cleanly with neither `parquet` nor `delta-scan` present (no accidental compile-time
   coupling to the Parquet crate). It does not execute anything, matching the owner's ruling and
   `feature-iso-parquet`'s own documented `PASS (0s)` legitimacy (gate-ops.md:10569).
2. `vortex-parquet-differential` — a new component, same `run_component` shape as `write-tests`,
   and the SOLE place `vortex`'s tests actually run. As landed it runs FOUR `&&`-chained passes
   (core `parquet,vortex`; cli `vortex`), each preceded by a `_fm_observe_child` record:
   - `cargo test -p cqlite-core --features parquet,vortex --lib export::vortex::` — the writer's own
     unit-level coverage (R1.2's no-second-mapping check, R2.1's writer-level UUID dtype pin, R3.2,
     R4.1). Name-filtered, not a bare `--lib`, so the lane does not pay for the ~4k-test core suite
     at a feature set no other component builds (roborev job 4372).
   - `cargo test -p cqlite-core --features parquet,vortex --lib export::arrow_schema::` — the
     shared Arrow schema's nested-UUID extension coverage (the fourth pass; the schema is what both
     writers consume, so its tests run under the pairing too).
   - `cargo test -p cqlite-cli --features vortex --test export_integration_tests
     test_export_sstable_to_vortex` — R7's library-level arm, otherwise executed by no lane.
   - `cargo test -p cqlite-cli --features vortex --test issue_4237_vortex_parquet_differential
     --test issue_4237_vortex_type_coverage` — the CLI-level, fixture-required cross-format oracle
     (R9) and the per-type-family cases (R2.2/R2.3), under the global
     `CQLITE_REQUIRE_FIXTURES=1`. `--features vortex` suffices on the CLI package because both test
     targets declare `required-features = ["state_machine", "vortex"]` and `state_machine` is a
     default feature that already forwards `cqlite-core/parquet`.
   `check_no_unexpected_zero_tests` (empty allowed-zero list) and `check_unittest_targets_ran`
   close the block. Added to `DATASET_COMPONENTS` (it reads the 33 fixture tables) and to the
   `COMPONENTS=(...)` array in the same edit as the new test targets, per the pattern every prior
   feature addition (#3453, #1699) used.

This is an implementation/gate-wiring decision, not a product one — no requirement's meaning
changes based on which component name runs it, and the choice follows the file's own established
per-feature-combination pattern (a new named component per distinct feature set, never widening an
existing one to change its meaning). Flagged in design rather than left implicit so a reviewer
doesn't read the design doc's one-line "Gate: named `--test` targets in a write-tests-style list"
as already covering the two-feature case.

## D4 — Test file layout

Two CLI test targets, both `required-features = ["state_machine", "vortex"]` (`state_machine`
already forwards `cqlite-core/parquet`, so the pair gives both writers):

- `cqlite-cli/tests/issue_4237_vortex_parquet_differential.rs` drives the compiled binary once per
  fixture table (33 tables, R9.1) plus the R2.1/R3.1/R8.1 cases.
- `cqlite-cli/tests/issue_4237_vortex_type_coverage.rs` carries the per-CQL-type-family cases
  (R2.2/R2.3: tuple, varint, nested UDT keys, duration; the top-level UDT-as-struct case is
  `#[ignore]`d on a defect shared with Parquet, see tasks.md 4.1).
- Both share the export/read-back/compare harness in `cqlite-cli/tests/support/vortex_differential.rs`
  (CLI invocation, the Parquet and Vortex readers, `logical_field` comparison, null-marked value
  comparison), so the type-coverage cases run exactly the `diff_one_table` logic.

`cqlite-core/src/export/vortex.rs` carries the writer; its `#[cfg(test)]` unit tests live in
`vortex_tests.rs` (options defaulting, error-shape construction, R3.2 order, R4.1 attribution) — no
fixture I/O at that layer, matching `parquet.rs`'s own split between library unit tests and
`cqlite-cli/tests/*parquet*` integration coverage.

## Alternatives considered and rejected (owner-settled, recorded for completeness)

- **Isolated Vortex-only Arrow crate with an IPC bridge** — rejected by the owner in favor of one
  arrow-59 tree (`docs/plans/2026-09-18-vortex-export-arrow59-design.md` decision 3).
  Not re-litigated here.
- **A Vortex-specific CQL→Arrow type map** — rejected; verified unnecessary against
  `vortex-arrow/src/convert.rs` (design doc, "Type map: NONE added").

## Nested UUID extension metadata (implementation note, #4237)

The nested-UUID `arrow.uuid` metadata fix (list/set/map/tuple/UDT element, key and field positions) is
requirement R2 ("UUID keeps its extension type") applied to nested positions, not a new requirement:
the shared Arrow schema is the single source of the extension metadata, so a nested UUID that lost it
would reach Vortex (and Arrow Flight) as a bare `FixedSizeBinary(16)`. No requirement or scenario
changes; the CHANGELOG records the user-visible effect on the other Arrow consumers.

## Editorial trims for `openspec validate vortex-export --strict` (2026-11-16)

`--strict` fails on the validator's >500-character requirement-text warning (R6, R7, R9, R11, R12 of
`cli-export-vortex`). The trims below are EDITORIAL: history and rationale moved here, the normative
behavior of every requirement and every scenario's Given/When/Then unchanged (apart from the R11.2
pass count, below). No requirement was split or renumbered. Removed text, verbatim:

- **R6** (implementation correction, history): "Corrected during implementation (the ORIGINAL R6.2 below described a non-existent code path): `export`'s destination is the MANDATORY positional `<FILE>` argument (`cli_types.rs`'s `Export` variant), not a separate `--out`/`--output` flag — that flag pattern belongs to the SEPARATE `query`/one-shot surface (`cqlite --query "..." --out <fmt> --output <file>`), which R6 does not touch (`query`'s own `OutputFormat::Vortex` arm always REJECTS, per the proposal's design choice — see `cqlite-cli/src/commands/query.rs`). Since `<FILE>` is mandatory for every `export` format alike, clap itself refuses `cqlite export --format vortex` with no `<FILE>` before any format-specific code runs — that is pre-existing, unconditional clap behavior, not something this change adds or could usefully re-test as Vortex-specific."
- **R7** (lead ruling, rationale): "Lead ruling 2026-09-24, correcting this requirement's original wording: `export_sstable` is NOT a CLI verb — the `export-sstable` subcommand documented in `cli-reference.md` is a DIFFERENT code path (the write engine's own SSTable-format exporter, `commands::write::handle_export`)."
- **R9**: the SHALL sentence kept its wording except that the parenthetical explaining #3890 was
  reduced to `(#3890)` (explanation: no column may be absent from one side and compared only on the
  columns the test happens to name), and the closing sentence "This test is gated
  `CQLITE_REQUIRE_FIXTURES=1` with no per-case skip, and runs in the mandatory full gate — not
  CI-only." moved here; R9.1 ("the test fails closed") and R11 (`CQLITE_REQUIRE_FIXTURES=1`) restate
  it, and the executing lane remains part of the mandatory full gate (D3).
- **R11**: the body originally read: "The full gate SHALL gain `feature-iso-vortex` as a **compile-only** lane (vortex enabled, parquet and delta-scan disabled, `--no-run`, mirroring `feature-iso-parquet` exactly — owner Seam-1 ruling 2026-09-23: slim gate wiring) and `vortex-parquet-differential` as the sole new EXECUTING component, enabling `parquet` AND `vortex` together to run both the writer's own unit-level coverage and R9's CLI-level differential target (`CQLITE_REQUIRE_FIXTURES=1`, added to `DATASET_COMPONENTS`; see design.md D3). `features-load-bearing` SHALL pass (the `vortex` feature has real `cfg` reference sites — the writer module itself) and `dep-duplicates` SHALL show no `ADVISORY-INCREASE` (Vortex's `parquet ^59.2`/`tokio` dependencies are already in the tree after #4236's arrow-59 rev)." The lane contracts
  it described live in R11.1/R11.2 and D3; the body now states the component names, the
  fixture-required executing lane, and quotes R11.3's own wording for `dep-duplicates` (`PASS` or,
  at worst, an `ADVISORY-INCREASE` block naming the duplicate, never a silent `FAIL`), resolving the
  earlier body-vs-R11.3 inconsistency ("SHALL show no `ADVISORY-INCREASE`").
- **R12** (rationale): "`CHANGELOG.md`, not the README formats table, is this change's record of an UNSHIPPED feature — README's "Features" section is a per-shipped-milestone changelog (`### ✅ M3 Complete`, `### ✅ v0.17.0`, …) and Vortex is not shipped as of this change; a README edit implying otherwise would be false. The eventual 0.18 release notes (owned by whichever issue assembles them, not this one) name Vortex export, the arrow 59 rev, and the Rust 1.95 floor."
- **R11.2** (scenario text, corrected not trimmed): it said "three passes"; the landed component
  runs four (`export::vortex::`, `export::arrow_schema::`, `test_export_sstable_to_vortex`, the CLI
  targets), now named in the scenario.
