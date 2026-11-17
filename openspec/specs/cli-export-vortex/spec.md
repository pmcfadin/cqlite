# cli-export-vortex Specification

## Purpose
TBD - created by archiving change vortex-export. Update Purpose after archive.

## Requirements

### Requirement: R6 — `cqlite export --format vortex` writes a `.vortex` file from a query

The CLI SHALL accept `--format vortex` (alias/spelling matching the existing `ExportFormat` value
enum casing, i.e. `vortex`) on `cqlite export <FILE> --table <TABLE> --format vortex`, forwarding
through the existing `--schema` resolution and streaming chunk/backpressure path unchanged.

#### Scenario: R6.1 query export produces a file Vortex's own reader opens
- **Given** the built `cqlite` binary with the `vortex` feature enabled, a real fixture table, and
  its resolved schema
- **When** `cqlite export results.vortex --format vortex --table <keyspace>.<table>` runs
- **Then** exit `0`, `results.vortex` exists, and Vortex's own session reader opens it and reports
  the expected row count (verified for all 33 fixture tables by the R9 differential, which uses
  exactly this invocation shape).

### Requirement: R7 — the `export_sstable` library function reads a table dir directly into Vortex

The `export_sstable` LIBRARY function (`cqlite_cli::commands::export_sstable::export_sstable`) SHALL
accept `ExportFormat::Vortex`, following the same dispatch shape as `ExportFormat::Parquet` in
`export_sstable.rs` (schema resolution, progress bar, single-generation export), tested at the
library level exactly as the pre-existing Parquet arm already is (`test_export_sstable_to_parquet`)
— not through a CLI subprocess. 

#### Scenario: R7.1 direct SSTable export to Vortex via the library function
- **Given** a real fixture table directory (`test_basic.simple_table`) and its resolved schema
- **When** `export_sstable(&sstable_file, &schema_file, &output_file, ExportFormat::Vortex, true)`
  is called directly (`test_export_sstable_to_vortex`, mirroring `test_export_sstable_to_parquet`)
- **Then** it returns `Ok(())`, and reading the output back with Vortex's own reader
  (`read_vortex_row_count`) reports a non-zero row count — not merely a non-empty file, which a
  0-row Vortex export (magic bytes + footer) would also satisfy.

### Requirement: R8 — `read-sstable` rejects Vortex like it rejects Parquet

`read-sstable`'s `--format`/`-f` dispatch SHALL reject `vortex` with an error naming the
unsupported format and pointing at `--out json` or `--out csv`, using the same message shape as its
existing Parquet rejection (`cqlite-cli/src/commands/read_sstable.rs`). Correction: `read-sstable`'s
flag is `--format`/`-f` (`ReadSstable`'s `format: OutputFormat` field in `cli_types.rs`), not
`--output` — the original wording below conflated it with `export`'s `--output`/`-o` destination
flag.

#### Scenario: R8.1 `read-sstable --format vortex` is rejected
- **Given** `cqlite read-sstable <Data.db> --format vortex`
- **When** the command runs
- **Then** it exits non-zero with the message `"Vortex format is not supported for this command.
  Use --out json or --out csv instead."` (or the exact string the implementation settles on,
  matching Parquet's phrasing pattern) — never a panic or a silently-empty file.

### Requirement: R9 — Cross-format differential is the primary oracle, over every fixture table

The SAME `SELECT *` query SHALL be exported to Parquet and to Vortex from the compiled CLI, for
each of the 33 committed fixture tables (`test_basic`, `test_collections`, `test_timeseries`,
`test_wide_rows`), both read back to Arrow with their own readers, and compared equal on schema
(modulo extension-metadata spelling differences) and every value, asserting the full column set
present in BOTH directions (#3890).

#### Scenario: R9.1 all 33 fixture tables agree, full column set, both directions
- **Given** the fetched dataset root (`CQLITE_DATASETS_ROOT`) with all 33 fixture tables present
- **When** each table's `SELECT *` is exported to both formats and both are read back to Arrow
- **Then** for every table, every column present in the Parquet read-back is present with an equal
  value in the Vortex read-back and vice versa, and row order matches positionally (R3 in
  `export-vortex`); the test fails closed (not silently skips) if any fixture table is missing.
- **And** there is no quarantine and no exception: all 33 tables are swept, including
  `test_wide_rows.wide_partition_table` and `test_collections.collection_clustering_table`, whose
  clustering columns of every CQL type are decoded by schema since #4279 (PR #4558). A table that
  fails to export or disagrees is a failure, never a skip.

#### Scenario: R9.2 the differential is a named, required-features `--test` target
- **Given** `cqlite-cli/Cargo.toml`
- **When** the new differential test target is declared
- **Then** it carries `required-features = ["state_machine", "vortex"]` — `state_machine` already
  forwards `cqlite-core/parquet` (see that feature's own comment in `cqlite-cli/Cargo.toml`), so
  this pair gives both writers — so a default-feature `cargo test` neither compiles nor silently
  skips it — it is absent from that build entirely, and a gate component that intends to run it
  must enable both explicitly (see R11).

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

### Requirement: R11 — Gate wiring: a compile-only isolation lane plus one executing differential lane

The full gate SHALL gain `feature-iso-vortex` as a **compile-only** lane and
`vortex-parquet-differential` as the sole new EXECUTING component, which enables `parquet` AND
`vortex` together (`CQLITE_REQUIRE_FIXTURES=1`, added to `DATASET_COMPONENTS`; see design.md D3).
`features-load-bearing` SHALL pass and `dep-duplicates` SHALL report `PASS` or, at worst, an
`ADVISORY-INCREASE` block naming exactly which new duplicate Vortex's dependency tree introduced
(never a silent `FAIL`).

#### Scenario: R11.1 `feature-iso-vortex` compiles, without `parquet` or `delta-scan`, and runs nothing
- **Given** the full gate
- **When** `feature-iso-vortex` runs
- **Then** `cqlite-core` builds with `vortex` enabled and both `parquet` and `delta-scan` disabled
  (`--no-run`, no test execution) — proving the Vortex writer has no accidental compile-time
  dependency on the Parquet crate itself (only on the shared `arrow_convert` producer, which
  requires `arrow`, not `parquet`) — matching `feature-iso-parquet`'s own compile-only contract and
  its documented `PASS (0s)` legitimacy.

#### Scenario: R11.2 `vortex-parquet-differential` is the sole executing lane, named, fixture-required, and zero-tests-guarded
- **Given** the full gate's `AGENT-GATE SUMMARY`
- **When** the `vortex-parquet-differential` component's line is read
- **Then** it names all four passes it ran — a name-filtered `cargo test -p cqlite-core --features
  parquet,vortex --lib export::vortex::` (not a bare `--lib`, to bound gate cost to the writer's own
  tests), a second name-filtered `--lib export::arrow_schema::` pass (the shared Arrow schema's
  nested-UUID extension coverage), `--test export_integration_tests test_export_sstable_to_vortex`
  (R7's library-level coverage), and the CLI-level targets `--test
  issue_4237_vortex_parquet_differential --test issue_4237_vortex_type_coverage` — states
  `CQLITE_REQUIRE_FIXTURES=1` for the fixture-backed pass, runs `check_no_unexpected_zero_tests`
  (empty allowed-zero list) over the combined log so a target compiling out to 0 tests FAILs rather
  than PASSing having executed nothing (review finding, Medium — every sibling executing lane
  already has this guard), and its pass/fail is not silently absorbed into an existing component's
  line (#3522's per-component naming discipline); no other new component executes any
  `vortex`-gated test.

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
`--features parquet` one), and `CHANGELOG.md`'s `[Unreleased]` section each gain a Vortex entry.

#### Scenario: R12.0 CHANGELOG.md names Vortex under Unreleased, not as a shipped release
- **Given** `CHANGELOG.md`'s `## [Unreleased]` section (previously `_Nothing yet._`)
- **When** this change lands
- **Then** it gains an `### Added` entry naming Vortex export, the off-by-default `vortex` feature,
  and the nested-UUID extension-metadata fix this change made along the way (issue #4237).

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
