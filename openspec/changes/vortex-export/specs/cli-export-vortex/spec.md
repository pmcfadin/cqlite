# cli-export-vortex — new capability (vortex-export, issue #4237)

`cqlite export` and `cqlite export-sstable` SHALL accept `--format vortex` as a sibling of
`--format parquet`, `read-sstable` SHALL reject it with the same message shape it already uses to
reject Parquet, and the primary cross-format-differential oracle SHALL run against the compiled
binary as a named, fixture-required gate target. All requirements are ADDED.

## ADDED Requirements

### Requirement: R6 — `cqlite export --format vortex` writes a `.vortex` file from a query

The CLI SHALL accept `--format vortex` (alias/spelling matching the existing `ExportFormat` value
enum casing, i.e. `vortex`) on `cqlite export`, requiring an `--output`/`-o` file destination
exactly as Parquet does (Vortex is a binary format; it cannot be written to stdout), and forwarding
through the existing `--schema` resolution and streaming chunk/backpressure path unchanged.

#### Scenario: R6.1 query export produces a file Vortex's own reader opens
- **Given** the built `cqlite` binary with the `vortex` feature enabled, a real fixture table, and
  a `SELECT * FROM <table>` query
- **When** `cqlite export --format vortex --out results.vortex 'SELECT * FROM <table>'` runs
- **Then** exit `0`, `results.vortex` exists, and Vortex's own session reader opens it and reports
  the expected row count.

#### Scenario: R6.2 `--format vortex` without a file destination is a usage error
- **Given** `cqlite export --format vortex 'SELECT ...'` with no `--out`/`--output`
- **When** the command runs
- **Then** it exits non-zero with an error naming that Vortex requires a file destination — the
  same shape as today's Parquet-without-`--output` error.

### Requirement: R7 — `cqlite export-sstable --format vortex` reads a table dir directly

The CLI SHALL accept `--format vortex` on `export-sstable`, following the same dispatch shape as
`ExportFormat::Parquet` in `export_sstable.rs` (schema resolution, progress bar, single-generation
export).

#### Scenario: R7.1 direct SSTable export to Vortex
- **Given** a real fixture table directory and its resolved schema
- **When** `cqlite export-sstable <table-dir> --format vortex --out results.vortex` runs
- **Then** exit `0` and Vortex's own reader opens `results.vortex` with the expected row count,
  matching the same table's `--format parquet` row count exactly.

### Requirement: R8 — `read-sstable` rejects Vortex like it rejects Parquet

`read-sstable`'s `--output`/format dispatch SHALL reject `vortex` with an error naming the
unsupported format and pointing at `--out json` or `--out csv`, using the same message shape as its
existing Parquet rejection (`cqlite-cli/src/commands/read_sstable.rs`).

#### Scenario: R8.1 `read-sstable --output vortex` is rejected
- **Given** `cqlite read-sstable <Data.db> --output vortex`
- **When** the command runs
- **Then** it exits non-zero with the message `"Vortex format is not supported for this command.
  Use --out json or --out csv instead."` (or the exact string the implementation settles on,
  matching Parquet's phrasing pattern) — never a panic or a silently-empty file.

### Requirement: R9 — Cross-format differential is the primary oracle, over every fixture table

The SAME `SELECT *` query SHALL be exported to Parquet and to Vortex from the compiled CLI, for
each of the 33 committed fixture tables (`test_basic`, `test_collections`, `test_timeseries`,
`test_wide_rows`), both read back to Arrow with their own readers, and compared equal on schema
(modulo extension-metadata spelling differences) and every value, asserting the full column set
present in BOTH directions (#3890: no column absent from one side compared only on the columns the
test happens to name). This test is gated `CQLITE_REQUIRE_FIXTURES=1` with no per-case skip, and
runs in the mandatory full gate — not CI-only.

#### Scenario: R9.1 all 33 fixture tables agree, full column set, both directions
- **Given** the fetched dataset root (`CQLITE_DATASETS_ROOT`) with all 33 fixture tables present
- **When** each table's `SELECT *` is exported to both formats and both are read back to Arrow
- **Then** for every table, every column present in the Parquet read-back is present with an equal
  value in the Vortex read-back and vice versa, and row order matches positionally (R3 in
  `export-vortex`); the test fails closed (not silently skips) if any fixture table is missing.

#### Scenario: R9.2 the differential is a named, required-features `--test` target
- **Given** `cqlite-cli/Cargo.toml`
- **When** the new differential test target is declared
- **Then** it carries `required-features = ["parquet", "vortex"]`, so a default-feature `cargo
  test` neither compiles nor silently skips it — it is absent from that build entirely, and a gate
  component that intends to run it must enable both features explicitly (see R11).

### Requirement: R10 — No byte-level `.vortex` golden is required or added

Because Vortex's sampling compressor is not byte-deterministic across runs, this change SHALL NOT
add a committed `.vortex` byte golden or assert exact output bytes; correctness is established
solely by the cross-format differential (R9) and the value/type-level requirements in
`export-vortex`.

#### Scenario: R10.1 repeated exports of the same query may differ byte-for-byte
- **Given** the same query exported to Vortex twice in separate runs
- **When** the two output files are compared byte-for-byte
- **Then** they are permitted to differ (compressor sampling), and no test in this change asserts
  byte equality between them or against a committed golden — only decoded-value equality (R9).

### Requirement: R11 — Gate wiring: mutual-isolation lane plus a two-feature differential lane

The full gate SHALL gain `feature-iso-vortex` (vortex enabled, parquet and delta-scan disabled,
executing the vortex module's own unit tests — see design.md D3) and a second component that
enables `parquet` AND `vortex` together to execute R9's differential target
(`CQLITE_REQUIRE_FIXTURES=1`, added to `DATASET_COMPONENTS`). `features-load-bearing` SHALL pass
(the `vortex` feature has real `cfg` reference sites — the writer module itself) and
`dep-duplicates` SHALL show no `ADVISORY-INCREASE` (Vortex's `parquet ^59.2`/`tokio` dependencies
are already in the tree after #4236's arrow-59 rev).

#### Scenario: R11.1 `feature-iso-vortex` compiles and runs without `parquet` or `delta-scan`
- **Given** the full gate
- **When** `feature-iso-vortex` runs
- **Then** `cqlite-core` builds and its `--lib` tests pass with `vortex` enabled and both `parquet`
  and `delta-scan` disabled — proving the Vortex writer has no accidental compile-time dependency on
  the Parquet crate itself (only on the shared `arrow_convert` producer, which requires `arrow`, not
  `parquet`).

#### Scenario: R11.2 the two-feature differential lane is named and fixture-required
- **Given** the full gate's `AGENT-GATE SUMMARY`
- **When** the new component's line is read
- **Then** it names the exact `--test` target(s) it ran, states `CQLITE_REQUIRE_FIXTURES=1`, and
  its pass/fail is not silently absorbed into an existing component's line (#3522's per-component
  naming discipline).

#### Scenario: R11.3 `features-load-bearing` and `dep-duplicates` stay green
- **Given** the full gate
- **When** `features-load-bearing` and `dep-duplicates` run after this change
- **Then** `features-load-bearing` reports the `vortex` feature as load-bearing (a real `cfg` site
  in its declaring package), and `dep-duplicates` reports `PASS` or, at worst, an `ADVISORY-INCREASE`
  block naming exactly which new duplicate Vortex's dependency tree introduced (never a silent
  `FAIL`).

### Requirement: R12 — Docs name Vortex as a new export target

Documentation SHALL name Vortex as a new export target: `output-formats.md`, `cli-reference.md`,
`docs/development/dev-cookbook.md` (a `--features vortex` build/test recipe mirroring the existing
`--features parquet` one), and the README formats table each gain a Vortex entry; release notes for
the shipping version name Vortex export, the arrow 59 rev, and the Rust 1.95 floor.

#### Scenario: R12.1 output-formats.md documents Vortex like it documents Parquet
- **Given** `website/src/content/docs/user-docs/output-formats.md`
- **When** this change lands
- **Then** it gains a `## \`vortex\` — ...` section in the same shape as the existing `## \`parquet\`
  — ...` section: what it is, the feature-gate/off-by-default note, an example command, and a note
  that it is export-only (no `read_vortex` binding method exists yet).

#### Scenario: R12.2 dev-cookbook gains the `--features vortex` recipe
- **Given** `docs/development/dev-cookbook.md`
- **When** this change lands
- **Then** it documents `cargo build --package cqlite-core --features vortex` and `cargo test
  --package cqlite-core --features vortex`, next to the existing `--features parquet` recipe lines.
