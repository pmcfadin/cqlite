# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- **UUID/TIMEUUID WHERE clause returned 0 rows** (Issue #548) — `WHERE id = <uuid-literal>`
  now correctly returns the matching partition. Four bugs were fixed together:
  1. `QueryParser::parse_value` now recognises bare UUID literals (`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`)
     and produces `Value::Uuid([u8; 16])` instead of `Value::Text`.
  2. `QueryExecutor::value_to_row_key` now handles `Value::Uuid` (produces 16 raw bytes)
     and `Value::Tuple` (composite-PK framing `[len u16 BE][value][0x00]` per component,
     matching `PartitionKey::to_bytes`). Also adds `Value::BigInt` support.
  3. `QueryExecutor::compare_values` now has `(Value::Uuid, Value::Uuid)` arms for
     WHERE-clause filter evaluation in table-scan paths.
  4. `SSTableManager::get` now routes lookups through `table_readers` (keyed by
     unqualified table name) instead of `readers` (keyed by filename), which caused
     all SSTables to share a single HashMap entry and only the last-loaded one to be
     searched. This also fixes point lookups for all other types.

  Additionally, `SSTableReader::scan_for_key` now passes the reader's own schema to
  `stitch_and_parse_all_chunks` so V5CompressedLegacy rows parse during the scan
  fallback without an external schema.

### Added

- **Performance methodology doc** — new `docs/performance.md` (Issue #575)
  explains what the CI perf gate enforces (strict: `read/*`, `write/ingest_wal_off`,
  `write/flush`) versus what it tracks as advisory (`write/ingest_wal_on`), why
  CI absolute numbers are not authoritative for fsync-bound work, how to
  reproduce benchmarks locally with exact `cargo bench` invocations and the
  `Durability` knob, the effect of tmpfs vs disk on `ingest_wal_on` throughput,
  and a direct answer to "is ~282 ops/sec expected?" (yes — it is disk-bounded
  by per-write fsync latency at ~1 000 / fsync_ms ops/sec, not a cqlite
  regression). Linked from README Resources section.

- **WAL durability toggle on `WriteEngine`** — `WriteEngineConfig` now has a
  `durability` field (default `Durability::SyncEachWrite`) and a matching builder
  method `with_durability(Durability)`. When set to `Durability::Disabled`,
  `write()` and `write_async()` skip WAL append and fsync entirely, buffering
  mutations in the memtable only; data becomes durable only after a successful
  `flush()` or `close()`. Default behavior (`SyncEachWrite`) is **unchanged**: a
  successful `write` call still guarantees the mutation is durable on disk (#547).

  ```toml
  # Public API additions (cqlite-core::storage::write_engine)
  pub enum Durability { SyncEachWrite, Disabled }
  impl WriteEngineConfig { pub fn with_durability(self, Durability) -> Self }
  ```

  **Hazard note**: `ingest_wal_on` benchmarks may show fsync-latency noise on
  shared CI runners; this is expected and does not indicate a regression. Only
  `Durability::Disabled` paths are CPU-bound and gate-able.

- **`write/ingest_wal_off` benchmark** — new Criterion bench in
  `cqlite-core/benches/write.rs` that runs the same 256-row ingest loop as
  `ingest_wal_on` but with `Durability::Disabled` (#574). The measured path
  performs no `wal.append()` or `wal.sync()`, isolating pure CPU + memtable
  cost. This bench is strictly gated in the CI perf regression gate;
  `ingest_wal_on` is now classified as advisory (reported, never fails CI on
  its own). A new `open_write_engine_wal_off` fixture helper in
  `benches/fixtures/mod.rs` constructs the WAL-disabled engine.

- **Perf-gate redesign — strict vs advisory benches** (Issue #572). The CI
  performance regression gate now distinguishes two bench classes, driven
  entirely by `cqlite-core/benches/perf-gate.json`:

  - **Strict** (`read/*`, `write/ingest_wal_off`, `write/flush`): non-zero exit
    on regression beyond per-bench `threshold_pct` — these are CPU-bound with
    stable timings suitable for reliable regression detection.
  - **Advisory** (`write/ingest_wal_on`): delta reported in every CI run but
    **never causes a non-zero exit**, regardless of magnitude. `ingest_wal_on`
    is I/O-dominated by `fsync`; its wall-clock time varies well beyond 10% on
    shared GitHub-hosted runners, producing false-positive failures on PRs that
    cannot affect performance.

  Configuration: `perf-gate.json` now uses per-bench objects (`id`,
  `threshold_pct`) and an `advisory_benches` string list. The gate script
  (`scripts/ci/check_perf_regression.py`) is fully data-driven from this file —
  no bench names are hardcoded in the script. A suite of pytest fixtures in
  `scripts/ci/tests/` validates the strict-fail / advisory-pass behavior.

- **Gate workflow path filter** (Issue #572, Phase A). The
  `perf-regression.yml` workflow now uses a `paths` allowlist that excludes
  `docs/**`, `**/*.md`, `examples/**`, and other non-runtime `.github/**`
  files. Docs-only / examples-only PRs no longer trigger the benchmark gate,
  eliminating false-positive regression alerts from fsync noise on those PRs.

### Fixed

- **`LIMIT` ignored on streaming `SELECT`** (#581): `Database::execute_streaming`
  yielded the entire result set regardless of `LIMIT`. The streaming producer
  (`execute_streaming_background`) only logged the `LIMIT` step and relied on a
  consumer that never enforced it, so `SELECT … LIMIT N` streamed every row — a
  silent wrong-result bug. The producer now enforces `LIMIT`/`OFFSET` inline
  during the scan (skip `OFFSET` matches, stop sending once `count` rows are
  emitted, and return so the scan stops early), matching the non-streaming
  `execute_limit` semantics. Regression test:
  `tests/test_issue_581_streaming_limit.rs`.

- **Provenance gate false-positives on branch names**: `scripts/ci/ensure_real_dataset.sh`
  now restricts its environment-variable scan to dataset-relevant names (`*_ROOT`,
  `*_PATH`, `DATASET*`) instead of scanning every env var. GitHub CI vars such as
  `GITHUB_HEAD_REF`, `GITHUB_REF*`, and `GITHUB_BASE_REF` are no longer inspected,
  so branch names containing words like "fixture" or "mock" no longer cause spurious
  gate failures. The `DATASET_SHA256` checksum check and CLI-argument scan are
  unchanged (#545).

### Changed

- **`write-support` is now a default feature** of `cqlite-core`. The write path
  (`WriteEngine`, `Mutation`) is available out of the box; downstream consumers no
  longer need to opt in to enable it. This adds **no new dependencies** —
  `write-support` gates only first-party code, so the dependency surface for
  read-only consumers is unchanged. `flush`/`compact` on the high-level `Database`
  type remain behind the separate `experimental` feature (#558).

## [v0.9.2] — Correctness fixes

Reader and compaction correctness follow-ups to v0.9.1, plus a compaction memory
fix and a multi-partition Index.db reader fix. No new features and no public API
changes.

### Fixed

- **`scan()` result ordering** is now guaranteed to be ascending Murmur3 token
  order (with raw key bytes as the equal-token tiebreaker), matching the on-disk
  SSTable layout and the write engine. Previously rows could come back out of
  order; `LIMIT` is now applied after ordering (#516).
- **`get()` / `scan()` partition consistency**: `get()` no longer returns `None`
  for partition keys that `scan()` returns. An Index.db digest-lookup miss now
  falls back to a key scan, and the V5CompressedLegacy chunk-stitching parse path
  is used so partitions spanning chunk boundaries are found (#517).
- **`SSTableReader::stats().block_count`** is now populated from the authoritative
  `CompressionInfo.db` chunk count instead of always reporting `0` (#518).
- **Compaction dropped input tombstones**: the k-way merger now surfaces row and
  cell tombstones from input SSTables with their authoritative `markedForDeleteAt`
  timestamps, so a higher-timestamp tombstone in a later SSTable correctly shadows
  a live row from an earlier one (#505).
- **Equal-timestamp Delete-vs-Live reconcile** now follows Cassandra
  `Cells#reconcile`: at equal timestamp the tombstone wins, independent of input
  file recency (previously the newer file won regardless of liveness) (#498).
- **Compaction dropped disjoint columns**: the k-way merger now reconciles cells
  per column (Cassandra `Cells#reconcile`) instead of selecting one whole winning
  row per clustering key, so rows updated across SSTables on different columns keep
  all their cells after compaction (#533).
- **Compaction memory**: the SSTable writer now streams `Data.db` to disk per
  partition instead of buffering the entire component in memory, bounding peak heap
  to roughly the largest single partition (was O(whole file), exceeding the 128 MB
  target on large compactions). Output is byte-identical (#492).
- **Multi-partition Index.db reader**: the reader mis-parsed Index.db entries whose
  leading `u16` key length was not `0x0010`, treating it as a digest marker and
  dropping most partitions (e.g. 100 partitions read back as 2). It now parses the
  real Cassandra BIG format `[key_len][raw key][offset][promoted]` for any key
  length; the project guide's Index.db documentation was corrected to match, and
  the `write-support` test targets are now wired into CI so this class of failure
  can't rot again (#552). Restoring O(1) raw-key point lookup is tracked in #553.

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

- Reviving the orphan integration tests surfaced three pre-existing reader bugs
  (`scan()` ordering #516, `get()`/`scan()` consistency #517, and
  `stats().block_count` #518). All three are **resolved in v0.9.2**.
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
