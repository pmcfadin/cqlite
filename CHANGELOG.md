# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v0.9.1] — Reader correctness fixes

Reader correctness and test/CI follow-ups to v0.9.0. No writer changes and no
public API changes.

### Fixed

- **Set-element tombstones** were surfaced as live values by the V5CompressedLegacy
  parser because the cell `is_deleted` flag was discarded. `parse_complex_cell_value`
  now returns the deletion flag, and the set (and list) branch skips tombstoned
  elements (#493).
- **Schema-aware tuple decoding** for arbitrary arity: tuples with more than two
  elements (e.g. `tuple<int, text, uuid>`) previously read back as `Null` or `Blob`.
  The reader now decodes each element using the element types from the schema's
  type string, with bounds-checked parsing and no heuristics (#501).
- **Frozen UDT field decoding**: `frozen<NAME>` columns previously read back as
  `Frozen(Null)`. The reader now resolves the concrete UDT through the UDT registry
  and decodes fields by name and type, and returns an actionable error when the
  referenced UDT is not registered (#502).

### Testing & CI

- Revived the orphan root-package integration tests: hardcoded SSTable directory
  UUIDs (from the retired dataset version) were replaced with dynamic table
  discovery, and the suite is now wired into CI so it cannot rot again (#514).
- Fixed the `aarch64-apple-darwin` CI runner where `cargo` was routed to
  `rustup-init` (the `cargo metadata` / `cargo +1.88.0` failures). The real cargo
  is now prepended to `PATH` in the Node and Python build workflows, with a
  toolchain verification step (#512).

### Known Issues

- Reviving the orphan integration tests surfaced three pre-existing reader bugs,
  now tracked separately and marked `#[ignore]` in the revived tests:
  `scan()` result ordering is not guaranteed (#516), `get()` misses partitions that
  `scan()` returns (#517), and `SSTableReader::stats().block_count` always reports
  0 (#518).
- The v0.9.0 known issues for counter writes, the BIG-format BTI writer, and the
  Python concurrent-query race (#311) are unchanged.

## [v0.9.0] — M5 Write Support

### Added

- **WriteEngine** in `cqlite-core/src/storage/write_engine/`: WAL-backed memtable,
  STCS compaction, and flush to portable Cassandra 5.0 SSTables. Public methods:
  `write(mutation)`, `write_async(mutation)`, `flush()`, `maintenance_step(budget)`,
  `maintenance_stats()`, and `export_sstable(path)`.
- **Mutation API** (parser-independent): `Mutation { table, partition_key,
  clustering_key, operations, timestamp_micros, ttl_seconds }` with
  `CellOperation::Write | WriteWithTtl | Delete | DeleteRow`.
- **CQL text write path**: `db.execute("INSERT/UPDATE/DELETE …")` as a convenience
  layer on top of the mutation API (PR #487).
- **Type coverage** for write roundtrips: Inet, Varint, Duration, Tuple, and
  Frozen all roundtrip through write→flush→read (Issue #477, #478).
- **Counter guard**: `WriteEngine::write()` and `write_async()` return
  `Error::InvalidOperation` immediately when a mutation targets a counter column,
  preventing silent data corruption (Issue #479, PR #489).
- **Python bindings write support** (PR #488): `db.execute(INSERT/UPDATE/DELETE)`,
  `db.flush_run()`, `db.maintenance_step(budget_ms)`, and `db.write_stats` property.
  Open database with `writable=True, write_dir=path` to enable writes.
- **Node.js bindings write support** (PR #494): `await db.execute(INSERT/UPDATE/DELETE)`,
  `await db.flushRun()`, `await db.maintenanceStep({ budgetMs })`, and
  `db.writeStats` getter. Open with `{ writable: true, writeDir: path }`.
- **CLI write flags**: `--writable`, `--write-dir`, `--mutation`, `--mutations-file`,
  `--flush`. Subcommands: `maintenance --budget-ms`, `write-stats`, `export-sstable`.
- **E2E readback gate** (`test-data/scripts/e2e-cassandra-readback.sh`, PR #508):
  exercises 5 tables (basic-primitives, collections, udt, static-columns, ttl)
  through write → flush → Docker copy → `nodetool refresh` → `cqlsh` verify.
- Write→flush→read roundtrip tests for `Inet`, `Varint`, and `Duration` types
  (Issue #477).
- Write→flush→read roundtrip tests for `Tuple<int, text, uuid>` and
  `Frozen<udt>` types (Issue #478).

### Changed

- M5 milestone closed; v0.9.0 marks the first release with full write support.
- CHANGELOG promoted from `[Unreleased]` to `[v0.9.0]`.

### Fixed

- Static columns could be duplicated in query results; fixed in PR #490
  (Issue #480, `static_columns_table` xfail removed).
- `typed_collections_table` V5CompressedLegacy cell extraction returned 1 row
  instead of 50; reader fallback added in PR #506 (Issue #481).
- Static-row write path emitted incorrect flags; fixed in PR #509.

### Known Issues

- **Counter writes**: Counter columns cannot be written via CQLite. The `write()`
  call returns `Error::InvalidOperation` with a descriptive message. Cassandra
  requires distributed CAS semantics for counter increments.
- **BTI writer**: The SSTable writer emits BIG format index files. BTI (trie)
  format indexes are read-only for now.
- **Python concurrent-query race** (Issue #311): Concurrent queries on the same
  database handle may see a race in schema metadata access. Run one warm-up query
  before spawning parallel threads.
- **Open reader follow-ups**: set-element tombstone decoding (#493), schema-aware
  tuple decoding (#501), frozen<udt> field decoding (#502). _(Resolved in v0.9.1.)_

## [0.4.0] - 2026-01-27 (M4 Complete)

### Added
- Python bindings via PyO3 with sync-first API (Issue #289)
- Node.js bindings via napi-rs with Promise-based API (Issue #290)
- Streaming API for memory-efficient large result sets (Issue #305)
- Complete CQL type coverage in bindings (20+ types including collections, UDTs)
- Type stubs for IDE support (Python mypy, TypeScript definitions)
- Thread-safe database handles with idempotent close
- pip/npm installable packages (5 platform builds each)
- 500+ tests across Python and Node.js bindings

### Python Bindings
- `cqlite.open()` context manager API
- `Database.execute()` for query execution
- `Row.to_dict()` for dictionary conversion
- `StreamingIterator` for large result sets
- Native Python types (datetime, UUID, bytes, Decimal)

### Node.js Bindings
- `Database.open()` with async/await pattern
- `Database.executeNative()` for native JS types (BigInt, Date, Buffer, Set, Map)
- `Database.executeStreaming()` for async iteration
- Complete TypeScript definitions with no `any` types
- Error properties: `code`, `category`, `isRecoverable`

## [0.3.0] - 2026-01-20 (M3 Complete)

### Added
- Parquet output format with Snappy compression (Issue #277)
- `cqlite export` command for file-based data export (Issue #278)
- Streaming export infrastructure for memory-efficient large dataset handling (Issue #280)
- Export formats: CSV, JSON, Parquet, CQL (INSERT statements)
- Progress bar with statistics for exports
- Atomic file writes to prevent partial output files (Issue #279)

### Changed
- Removed YAML from output format options (Issue #283)

## [0.2.0] - 2026-01-08 (M2 Complete)

### Added
- CLI one-shot query mode with `--schema`, `--data-dir`, `--query`, `--out` flags
- REPL mode with history, completion, and status display
- TUI mode (experimental)
- SELECT query support with WHERE clause (partition/clustering key equality)
- Output formats: Table, JSON, CSV
- M2SelectValidator for query validation

### Changed
- Query engine enabled by default (`state_machine` feature)
- Documentation updated for M2 completion

## [0.1.0] - 2025-12-18 (M1 Complete)

### Added
- Initial release of CQLite core library
- Cassandra 5.0 SSTable format support ('oa' format with BTI indexes)
- SSTable component parsing:
  - Data.db (row and cell data)
  - Index.db (partition index)
  - Summary.db (index summary)
  - Statistics.db (SSTable metadata)
  - TOC.txt (table of contents)
- Compression codec support:
  - LZ4
  - Snappy
  - Deflate
  - Zstd
- CQL type system implementation:
  - Primitive types (int, bigint, text, blob, uuid, timestamp, etc.)
  - Collection types (list, set, map)
  - User-defined types (UDT)
  - Frozen types
- Schema-aware decoding
- CLI tool with basic parsing commands
- Workspace structure:
  - `cqlite-core`: Core parsing library
  - `cqlite-cli`: Command-line interface
- 33/33 test tables passing (100% validation)

### Technical Details
- Zero-copy parsing where possible
- Memory-efficient design targeting <128MB for large files
- No external cluster dependencies required
- Real Cassandra SSTable test data validation

[v0.9.0]: https://github.com/pmcfadin/cqlite/compare/v0.4.0...v0.9.0
[0.4.0]: https://github.com/pmcfadin/cqlite/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/pmcfadin/cqlite/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/pmcfadin/cqlite/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/pmcfadin/cqlite/releases/tag/v0.1.0
