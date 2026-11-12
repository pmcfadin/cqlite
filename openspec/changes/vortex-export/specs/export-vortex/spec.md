# export-vortex — new capability (vortex-export, issue #4237)

`cqlite-core` SHALL provide a Vortex writer for reconciled query results, off by default, that
consumes the exact same Arrow `RecordBatch` stream the Parquet writer consumes — same values, same
order, same type mapping — so the two exports agree by construction rather than by a separate
verification path. All requirements are ADDED.

## ADDED Requirements

### Requirement: R1 — One shared Arrow batch producer, no second CQL→Arrow mapping

The crate SHALL expose a single Arrow `RecordBatch` producer, over the ordered `QueryMetadata`
schema, that both the Parquet writer and the Vortex writer consume for the same chunk of
`QueryRow`s. Adding the Vortex writer SHALL NOT introduce a second row→Arrow mapping, a CQL-type
translation independent of `arrow_convert`, or an Arrow-IPC bridge between two Arrow crate
versions.

#### Scenario: R1.1 identical RecordBatch feeds both writers
- **Given** a chunk of `QueryRow`s for a table exercising every CQL type family in
  `test-data/schemas` (collections, tuple/UDT-as-struct, decimal, varint, temporal, inet, counter,
  uuid/timeuuid)
- **When** the shared producer builds one `RecordBatch` from that chunk
- **Then** the Parquet writer and the Vortex writer are each handed that same batch object (or an
  equal clone) — never two independently-constructed batches — verified by a unit test asserting
  both call sites pass through the one producer function.

#### Scenario: R1.2 no Vortex-specific CQL→Arrow mapping exists
- **Given** the `cqlite-core/src/export/vortex.rs` module
- **When** its source is inspected
- **Then** it contains no `match` over `CqlType`/`ColumnInfo` that builds Arrow arrays — all such
  logic is reached only via the shared `arrow_convert` producer, pinned by a `grep`-style check in
  the test suite (or code review) that fails the requirement's intent if violated.

### Requirement: R2 — Type fidelity across the format boundary, UUID extension pinned

Every Arrow type the shared producer emits SHALL survive Vortex's official Arrow-extension importer
unchanged in logical value, including the `arrow.uuid` extension metadata on UUID/TimeUUID columns,
which SHALL arrive as Vortex's UUID extension type, never as a bare `FixedSizeBinary(16)` /
`FixedSizeList<u8,16>` without that metadata.

#### Scenario: R2.1 UUID/TimeUUID column keeps its extension identity
- **Given** a table with a `uuid` and a `timeuuid` column, real fixture data
- **When** the Vortex writer exports it and the file is read back with Vortex's own reader
- **Then** both columns' Vortex dtype is the UUID extension type (asserted by inspecting the
  read-back schema/dtype, not by re-deriving values), and every value round-trips byte-identical to
  the source UUID bytes.

#### Scenario: R2.2 every CQL type family exports without error
- **Given** one fixture table per CQL type family in `test-data/schemas` (collections, tuple,
  UDT-as-struct, `decimal`, `varint`, temporal (`date`/`time`/`timestamp`), `inet`, `counter`,
  `duration`)
- **When** each is exported to Vortex
- **Then** the export succeeds and every value, decoded back through Vortex's reader, equals the
  same table's Parquet-exported value for that column (cross-referenced against R-series in
  `cli-export-vortex`'s differential requirement, which is the primary oracle for value equality;
  this scenario pins that no type family is silently rejected at the writer level).

#### Scenario: R2.3 `duration` takes the same text fallback as Parquet
- **Given** a table with a CQL `duration` column
- **When** it is exported to both Parquet and Vortex
- **Then** both encode the value as `Utf8` in the same CQL duration text form (e.g. `"1mo2d3ns"`) —
  Vortex never attempts to represent it as Arrow `Interval(MonthDayNano)` or the unimplemented
  Arrow `Duration`, matching the design doc's documented Vortex refusal of `Duration`.

### Requirement: R3 — Row and field order are part of the export contract

The Vortex writer's output order SHALL match the reconciled query stream's order exactly: Cassandra
token-ring order across partitions, then the schema-aware clustering comparator within a partition
(composite clustering columns, per-column `DESC`, null/absent-value rules), with Arrow/Vortex field
order taken from the query schema's column order — never derived from hash-map iteration.

#### Scenario: R3.1 clustering order preserved end-to-end
- **Given** a wide-partition fixture table (`test_wide_rows`) with a multi-column clustering key
  including at least one `DESC` column
- **When** the same `SELECT` is exported to Parquet and to Vortex
- **Then** both outputs, read back to Arrow, yield rows in the identical order, row-for-row —
  compared positionally, not by re-sorting either side.

#### Scenario: R3.2 field order matches the query schema, not map iteration
- **Given** a table with more than eight columns (large enough that a `HashMap`-ordered field list
  would almost certainly disagree with declaration order across two runs)
- **When** exported to Vortex twice in the same process
- **Then** the Arrow/Vortex struct field order is identical both times and matches
  `QueryMetadata.columns` order exactly.

### Requirement: R4 — Fail-closed on any conversion or write error, no partial file

A conversion or write error at any point in the batch stream SHALL abort the export, name the
failing batch index and column, and leave no readable `.vortex` file at the destination path — an
in-progress file is either never created at the final path (write-to-temp-then-rename) or is
deleted on the error path.

#### Scenario: R4.1 induced mid-stream error leaves no file
- **Given** an export of a large enough result set to span multiple batches, with a fault injected
  into the conversion of a specific later batch (e.g. an unsupported value shape reachable only in
  that batch)
- **When** the export runs
- **Then** it exits non-zero (library: returns `Err`), the error names the batch index and the
  offending column, and the destination path has no file (or a stat of it fails / a reader rejects
  it as unreadable — never a truncated file callers could mistake for complete output).

#### Scenario: R4.2 error type is a dedicated enum, not a stringly-typed failure
- **Given** the public error type returned by the Vortex writer
- **When** inspected
- **Then** it is a `thiserror`-derived enum (matching `ParquetExportError`'s shape) with a variant
  carrying batch index + column context, not a bare `String`/`anyhow::Error` at the library
  boundary.

### Requirement: R5 — Feature gating matches the Parquet precedent exactly

The `vortex` feature SHALL be `vortex = ["arrow", "dep:vortex"]`, absent from
`cqlite-core`'s default feature set, and its module SHALL be declared as an unconditional
`#[cfg(feature = "vortex")] pub mod vortex;` at the crate root — never gated a second time inside
the module (the pub-surface guard's actual concern, #1712). The Vortex crate dependency SHALL
enable `vortex-file` and SHALL NOT enable `vortex-cloud` or `vortex-tensor`.

#### Scenario: R5.1 default build has no Vortex dependency
- **Given** `cargo build -p cqlite-core` with default features
- **When** the dependency tree is inspected
- **Then** the `vortex` crate is absent (not compiled), matching `parquet`'s absence from a default
  build today.

#### Scenario: R5.2 `pub mod vortex` is unconditional at the declaration site
- **Given** `cqlite-core/src/lib.rs`
- **When** the `pub-surface` gate component runs
- **Then** it finds `pub mod vortex;` gated only by the top-level `#[cfg(feature = "vortex")]`
  attribute at the declaration site, with no inner `#![cfg(...)]` inside `vortex.rs` — passing the
  same check that caught the `benchmarks` module drift (#1712).
