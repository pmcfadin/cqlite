# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Typed inner-UDT decode for frozen-UDT elements inside frozen collections (#1340, PR #1960)
- RF-correct logical-optimizer row-count estimate for the Flight/Trino connector (#1336, PR #1954)
- Byte-bounded result budget: `Error::ResultTooLarge` and `QueryConfig.max_result_bytes` (default 64 MiB) (#1582, PR #1890)
- Per-surface SSTable freshness contract plus explicit `Database::refresh()` (#1749, PR #1761)
- Compaction drops fully-expired SSTables whole via metadata only, overlap-safe (#1388, PR #1740)
- fsync the data directory before WAL truncate for SSTable durability (#1392, PR #1421)
- Intern per-cell column names as `Arc<str>` to cut row-decode allocations (#1334, PR #1533)
- Recursive comparator for composite collection element/key ordering (#1296, PR #1317)
- Preserve repaired metadata through compaction (Cassandra 5.0) (#1021, PR #1250)

### Changed

- **BREAKING (Python bindings):** CQL `duration` and `time` now decode to exact,
  lossless Python types, matching the Node binding (#1450). See the
  [v0.13 Migration Guide](docs/development/v0.13-migration-guide.md) for
  before/after examples. The previous mapping (M4 §5.2) was lossy and disagreed
  with Node, the CLI, and Cassandra:
  - `time` → `int` (nanoseconds since midnight), was `datetime.time` (which
    truncated sub-microsecond nanoseconds).
  - `duration` → `cqlite.Duration(months, days, nanos)`, was `datetime.timedelta`
    (which approximated months as 30 days and truncated nanoseconds to
    microseconds).

  Migration for code that relied on the old types (the
  [v0.13 Migration Guide](docs/development/v0.13-migration-guide.md) expands on
  this):
  ```python
  # OLD: t was a datetime.time; d was a datetime.timedelta
  # NEW: t is an int (nanoseconds); d is a cqlite.Duration
  import datetime
  t_ns = row["work_time"]                       # e.g. 3723123456789
  t = datetime.time(                            # reconstruct a datetime.time (µs, lossy)
      t_ns // 3_600_000_000_000,
      (t_ns // 60_000_000_000) % 60,
      (t_ns // 1_000_000_000) % 60,
      (t_ns // 1000) % 1_000_000,
  )
  d = row["duration_val"]                        # cqlite.Duration
  d.months, d.days, d.nanos                      # exact components
  ```
- **BREAKING (discovery-driven flows):** loading the schema for a table that was
  never registered or discovered now returns an error instead of a fabricated
  `uuid id` default schema. `SchemaManager::load_schema` previously invented a
  hardcoded schema for unknown tables, so queries against an undefined table
  returned fabricated-shape rows rather than failing — a no-heuristics violation.
  Unknown tables now fail honestly with `Table schema not found: {table_name}`,
  mirroring the I3 hard-fail precedent
  (#1626). Tables with real registered/discovered schemas (including all corpus
  parity tables) are unaffected (#1710). See the
  [v0.13 Migration Guide](docs/development/v0.13-migration-guide.md) for the
  per-surface error (Rust/Python/Node/CLI) and how to register a schema.

### Removed

- **BREAKING (CLI):** dropped YAML as a query output format. `--out yaml` and
  `--format yaml` are no longer accepted — both are now rejected at parse time
  with a clear error listing the supported values (`table`, `json`, `csv`,
  `parquet` for `--out`). YAML output was never implemented (it was only an
  unused `OutputMode`/`OutputFormat` variant), so this removes a dead surface
  rather than working behavior; anyone scripting `--out yaml`/`--format yaml`
  must switch to a supported format. A parse-rejection regression test guards
  against the variant being silently re-added (#283). See the
  [v0.13 Migration Guide](docs/development/v0.13-migration-guide.md).

### Fixed

<!-- DRAFT (release-prep): curated from the merge log since v0.12.0; owner to
     verify/trim wording before the release commit. See
     docs/development/release-0.13-checklist.md. -->

- **No-heuristics: removed blob-decode byte-pattern guessing.** The raw-decode
  path no longer infers a value's type from its byte pattern; blobs that happened
  to look like other types are returned faithfully as blobs, per the no-heuristics
  mandate (#1630).
- **Unknown-table reads fail honestly** rather than returning a fabricated
  `uuid id` default schema (also listed under Changed as a breaking behavior;
  #1710).
- **BTI `export-sstable` partition count** is now read from the authoritative
  `Statistics.db` reader instead of a derived estimate (#1622).
- **Deterministic raw-only Snappy decode** with transient-only retry — removes a
  nondeterministic decode fallback on the raw path (#1588).
- **Full-scan no longer issues duplicate scans.** Retired
  `execute_parallel_table_scan` in favor of bounded streaming, eliminating the
  4× duplicate table scan on the full-scan path (#1691).
- Pre-admission memtable size check with a bounded iterative estimator (#1625, PR #1957)
- Grouped/filtered non-star aggregates now read the input columns they were missing (#1952, PR #1971)
- Parser hardening bundle: recursion-depth guards, duration i32 bounds, collection capacity limits (#1632, PR #1970)
- Aggregate/grouped result metadata names now match row value keys (#1763, PR #1953)
- Data-safe logging: log shapes not values — removed eprintln/WHERE-literal logging that could leak data (#1694, PR #1958)
- Surface chunk-CRC corruption on point lookups instead of silently proceeding (#1411, PR #1777)

### Performance

<!-- DRAFT (release-prep): this release's headline theme is read-path + bindings
     performance. Owner to confirm framing before the release commit. -->

- **Read-path constant-factor bundle (Epic E):** query-engine hot-path cleanups
  (schema `Arc`, single projection, cached sort keys, plan cache, GROUP BY hash;
  #1587, PR #1867), read-path idiom bundle (de-async, `FxHash`, OFFSET skip/take,
  per-partition digest, IN-expansion allocation cuts; #1590, PR #1877), and dropping the
  `table_readers` read guard before scanning (#1591, PR #1882).
- **Point-read I/O (C2):** a `ReadAt` positional-read trait removes the point-read
  cursor convoy and the per-lookup `open(2)` (#1573).
- **Node bindings throughput:** batch-fetch streaming rows per async task (#1443),
  move (not deep-clone) row values in `executeNative` (#1447), and cache `Set`/`Map`
  constructors per result conversion (#1448).
- Cache derived comparators on `SchemaEntry` (#1709, PR #1963)
- One payload+CRC read per compressed chunk (E3 A5 read-path bundle) (#1585, PR #1955)

## [v0.12.0] - 2026-06-22

The compaction release. CQLite now rewrites and compacts Cassandra 5.0 SSTables
with **byte-for-byte parity against Apache Cassandra**, verified by a differential
harness that compacts the same inputs with both engines and compares the output
bytes plus an end-to-end Cassandra readback (Epic #842 → #921 → #938). Reaching
parity required modelling compaction the way Cassandra does — per-element/per-cell
merge reconciliation (#886, #899) rather than whole-cell/row-timestamp granularity —
and then implementing the full reconciliation rule set: complex-deletion
strict-supersede and shadow-before-purge (#887), tombstone-vs-expiring tie-breaks
(#848), `gc_grace`/`gcBefore` purging with overlap-aware partial-compaction safety
(#845, #935), range-tombstone shadowing (#846), per-cell and dropped-column purging
(#922, #847), row-deletion/live-cell coexistence (#932), row-tombstone
`localDeletionTime` preservation (#873), clustering identity through row tombstones
(#912), non-frozen UDT multi-cell read+write (#927, #929), and static-row presence
read from input headers (#850).

Beyond compaction, this release adds an **Arrow Flight server + Trino connector**
for querying SSTables as a federated source with predicate, token-range, and
aggregation pushdown (Epics #874, #918); **canonical BTI (`da`) write support** that
emits Cassandra-format trie-indexed SSTables and end-to-end BTI read (Epics #872,
#835); a **CDC-style delta-scan / delta-export** path that projects SSTable
generations to Parquet envelopes with full tombstone fidelity (Epic #696);
**`WRITETIME()` / `TTL()` in `SELECT`** (Epic #689); broad **query-engine
completeness** (PER PARTITION LIMIT, static columns, clustering order, clustering-key
bounds, cross-generation LWW merge, partition-targeted lookups); and **read-path
performance** work (parallel single-reader scans, a size-aware direct-I/O disk
backend with configurable prefetch, streamed writers, promoted index, BTI seeks).
Plus crates.io OIDC trusted publishing, a Homebrew tap, and a hardened CI/validation
pipeline.

### Added

- **Byte-for-byte compaction parity vs Apache Cassandra** (Epics #842, #921, #938) —
  a `cqlite compact` command and a differential harness that compacts identical
  inputs with CQLite and Cassandra and diffs the resulting `Data.db` bytes, wired
  into CI alongside an E2E Cassandra readback (#854, #858, #936). The merge path was
  re-modelled to per-element/per-cell granularity (#886, #899), enabling the full
  reconciliation rule set: complex-deletion strict-supersede + shadow-before-purge
  (#887), tombstone-vs-expiring(TTL) tie-break (#848), `gc_grace`/`gcBefore` purging
  (#845) with overlap-aware partial-compaction purging via per-key
  `maxPurgeableTimestamp` (#935), range-tombstone merge shadowing (#846), per-cell
  dropped-column purging (#922, #847), row-deletion/live-cell coexistence (#932),
  multi-cell collection/UDT merge per cell-path (#844), non-frozen UDT multi-cell
  support (#927, #929), single schema-ordered emission for mixed complex writes
  (#930), row-tombstone `localDeletionTime` preservation (#873), clustering identity
  through row tombstones (#912), and static-row presence from input headers (#850).
  Rules are documented in `docs/compaction/byte-parity-rules.md`.

- **Arrow Flight server + Trino connector** (Epics #874, #918) — query Cassandra
  SSTables as a federated Trino source over Arrow Flight, with predicate pushdown,
  arbitrary nested predicates (OR/NOT), token-range SSTable pruning, and aggregation
  pushdown (count/sum/min/max/avg + GROUP BY) including integer `avg` and float
  min/max with Trino-exact NaN ordering (#836, #898, #919). GROUP BY pushdown has an
  operator-configurable gate for high-cardinality cases (#937).

- **Canonical BTI (`da`) write + read** (Epics #872, #835) — emit Cassandra-format
  `da` (BTI) SSTables with a Partitions.db/Rows.db trie, drop legacy
  Index.db/Summary.db, and validate against `sstabledump`/Cassandra readback (#914);
  end-to-end BTI full-scan read support (#897); Data.db offset extraction from BTI
  trie payloads for O(log n) seeks (#833); read-path completion for wide partitions
  and BTI (#867).

- **CDC-style delta-scan and `delta-export`** (Epic #696) — a `scan_delta` streaming
  API emitting `DeltaRecord`s (upserts, static upserts, row/range/partition
  tombstones) with an Arrow envelope schema derived from the table schema, a
  `DeltaParquetWriter`, and a CLI `delta-export` subcommand for projecting an SSTable
  generation to a CDC Parquet file (#869, #871, #870, #876, #877, #880, #879). Ships
  with a DuckDB reference-merge reconciliation guide (#878) and a parity harness
  (#881, #882).

- **`WRITETIME()` and `TTL()` in `SELECT`** (Epic #689) — parse, validate, thread
  per-cell writetime/TTL metadata from reader to query rows (opt-in), and evaluate
  the functions in projections, with coverage across JSON/CSV/Parquet and the
  bindings (#837, #838, #840, #855, #856).

- **Query-engine completeness** (Epic #756) — `PER PARTITION LIMIT` (#859),
  `indexes_used` in query-plan results (#861), static-column tracking in discovered
  schemas (#862), clustering-order (ASC/DESC) discovery from the Statistics.db header
  (#863), UDT-reference validation against the registry at load (#860), clustering-key
  inequality bounds (`>=`, `>`, `<`, `<=`) (#791), and a partition-targeted lookup
  for fully-constrained `WHERE pk = ?` instead of scanning every SSTable (#949).

- **Size-aware disk-access backend with direct I/O and configurable prefetch**
  (#964) — a new `Auto` selector sizes each `Data.db` file against system RAM:
  small files are memory-mapped (resident in the page cache for repeated scans),
  while files larger than a configurable fraction of RAM (default half) use **true
  direct I/O** (`O_DIRECT` on Linux, `F_NOCACHE` on macOS) so a single huge scan does
  not thrash the page cache. New `StorageConfig` fields — `disk_access_mode`
  (`auto`/`buffered`/`mmap`/`direct`), `direct_io_memory_fraction`, `prefetch`
  (`off`/`sequential`/`willneed`/`auto`), and `direct_io_prefetch_bytes` — plus
  `CQLITE_DISK_ACCESS_MODE` / `CQLITE_PREFETCH` env overrides. Prefetch is wired to
  both paths (`madvise(SEQUENTIAL/WILLNEED)` on mmap, an aligned read-ahead window on
  the direct path), and every non-buffered backend degrades gracefully to buffered
  I/O when the OS/filesystem refuses it (network mounts, `O_DIRECT`-hostile
  filesystems). All fields are serde-default for backward compatibility; the legacy
  `use_mmap` flag still forces mmap.

- **Read-path performance** (Epics #906, #751) — restored parallel reads on a single
  `SSTableReader` via per-scan positioned reads (#815) with a concurrent-scan scaling
  benchmark (#917); cursor/channel groundwork for a streaming K-way merge (#754); a
  Cassandra-correct streamed promoted index for wide partitions (#752); and Index.db
  entries streamed to disk instead of buffered (#753).

- **Distribution** — crates.io publishing for `cqlite-core` + `cqlite-cli` (#782)
  migrated to OIDC trusted publishing (#786), and a Homebrew tap for the CLI (#781).

### Changed

- Incremental streaming of the read/scan path instead of full materialization (#790).
- crates.io release publishing now runs on tag via OIDC rather than a stored token
  (#895); linux-gnu CLI binaries build under `cross` to lower the glibc floor (#864).
- Cross-generation `SELECT *` now performs an LWW merge with tombstone suppression
  across SSTable generations (#883), and `WRITETIME`/`TTL` metadata reconciles across
  generations (#885).

### Fixed

- **Write-path / Statistics.db fidelity** (Epic #796) — populate the
  `estimatedTombstoneDropTime` histogram (#797), compute final Statistics.db delta
  baselines before the write loop (#799), wire real `l0_count`/`total_written` from
  the WriteEngine (#800), and a `write_dir` file lock to prevent concurrent-write
  corruption (#798).
- **Bindings** — CLI emits `null` for tombstoned cells in JSON (#806); Python provides
  a hashable representation for `SET<FROZEN<UDT>>` (#804) and thread-safe schema init
  for concurrent queries (#805).
- **Test & CI trustworthiness** (Epic #795) — resolve `CQLITE_DATASETS_ROOT` to
  `sstables/` and unmask 7 silently-skipped Python failures (#773), harden
  `agent-gate.sh` to cover the Python bindings and all integration targets (#865),
  retire a TTL byte-scan flake for a structural assertion (#774), and build DuckDB
  from source so the DS11 reconciliation job links on ubuntu (#916).

### Contributors

Thanks to everyone who contributed to this release:

- **Patrick McFadin** ([@pmcfadin](https://github.com/pmcfadin)) — maintainer; the
  bulk of compaction parity, BTI write/read, delta-scan, query engine, read-path
  performance, write-path fidelity, CI, and release infrastructure.
- **Jon Haddad** ([@rustyrazorblade](https://github.com/rustyrazorblade)) — the Arrow
  Flight server + Trino connector (#836) and the compaction byte-parity foundations:
  the rule spec + parity-auditor agent (#854) and the `cqlite compact` differential
  harness vs Apache Cassandra (#858).

Development was AI-assisted: substantial portions were implemented and reviewed with
Claude Code under human direction and review. Reconciliation edge cases were validated
against the `rustyrazorblade/cassandra` compaction reference.

## [v0.11.0] - 2026-06-15

Minor release bundling everything merged since v0.10.0. New capability: a
first-class Parquet writer lifted into `cqlite-core` behind a `parquet` feature,
with `export_parquet` methods on the Python and Node bindings (Epic #682).
New read coverage: version-gated read behavior for the Cassandra 5.0 `oa` format
and graceful handling of the `da` (BTI) format (VG1/VG3/VG5/VG6/VG7), real BTI
node-type dispatch (#651), schema-typed query result columns (#770), and
higher-fidelity Parquet/Arrow type mapping (#771). Plus opt-in memory-mapped
reads (#589, **off by default**) with their follow-up hardening (#591) and a
bounded uncompressed-read allocation (#592). Correctness:
TEXT/composite partition-key reconstruction on the scan path (#586), a safe
compaction async-to-sync bridge (#587), writer temporal-delta and tombstone
serialization fixes (#645, #723), Summary.db offset-table encoding (#718), and
removal of the last parser heuristic (#650). Plus removal of three dead mmap
readers (#590) and a new documentation site.

(The `0.10.1` version that briefly appeared in the manifests was never tagged or
published; its prepared notes are folded into this release.)

### Added

- **Parquet writer lifted into `cqlite-core` behind a `parquet` feature**
  (Epic #682) — the Parquet/Arrow export engine now lives in
  `cqlite-core/src/export/parquet.rs` behind an optional `parquet` cargo feature,
  with the CLI's `cqlite-cli/src/output/parquet.rs` reduced to a thin wrapper over
  it (#685). The Python and Node bindings gain `export_parquet` /
  `exportParquet(query, path, { rowGroupSize, compression })` methods so callers
  can stream query results straight to a Parquet file. Golden-file coverage lives
  in `cqlite-cli/tests/parquet_golden_tests.rs`.

- **Version-gated read support for the Cassandra 5.0 `oa` format** (Issues
  #653, #655, #672) — `VersionGate`s are threaded through the read path (VG1)
  so format-specific behavior is selected from the SSTable version rather than
  guessed. The `oa` (5.0 BIG) read behavior lives behind five `oa`-only gates
  (VG3) and the query path is gated end-to-end (VG6, including a range-tombstone
  marker-skip fix); all six `oa` fixture tables pass `sstabledump` parity. The
  `da` (BTI) format has a routing foundation that returns a graceful
  *unsupported* error instead of misreading (VG5). Table identity in discovery
  is now keyed by `(keyspace, table)` (VG7, #680). New `oa`/`da` fixtures and
  goldens ship in the `datasets-v3` test set (#654).

- **Real BTI node-type dispatch in `RowsParser`** (Issue #647) — `parse_node_data`
  was a stub that always returned `PayloadOnly`, mislabeling Single/Sparse/Dense
  nodes as leaves and returning wrong results for any multi-node trie. A shared
  `parse_bti_node` now dispatches all 16 `TrieNode` ordinals (including the
  packed 12-bit pointer variants), used by both `PartitionsParser` and
  `RowsParser`.

- **Schema `CqlType` threaded into query-result `ColumnInfo`** (Issue #674) —
  result columns now carry their declared CQL type from the schema rather than
  an inferred one, enabling type-correct downstream conversions in bindings and
  exporters.

- **Higher-fidelity Parquet/Arrow type mapping** (Epic #673) — nested and
  high-precision CQL types are preserved on export instead of being flattened:
  collections map to Arrow `List`/`Map`, and high-precision types keep their
  precision rather than degrading to strings.

- **Opt-in memory-mapped I/O on the SSTable read path** (Issue #589) — the
  reader now sits on a `BlockSource` abstraction with two interchangeable
  backends: a portable `BufReader<File>` (default) and a read-only `memmap2`
  mapping. When enabled, files at or above `mmap_min_size_bytes` (4096) are
  served from the OS page cache with no per-block read syscall, mirroring
  Cassandra's `disk_access_mode: mmap`. **Opt-in and off by default**
  (`use_mmap: false`); buffered I/O remains the portable, safe default. Map
  failures degrade gracefully to buffered I/O. Enable only for immutable local
  SSTables — external mutation/truncation of a mapped file or some network
  filesystems can `SIGBUS`; the write-while-mapped guard and Windows
  delete/replace policy from #591 make this safe for the supported use case.

### Removed

- **Removed three dead mmap-based SSTable readers** (Issue #590) — deleted
  `SchemaAwareSSTableReader` (`storage/reader.rs`), `OptimizedSSTableReader`
  (`storage/sstable/optimized_reader.rs`), and `StreamingSSTableReader`
  (`storage/sstable/streaming_reader.rs`). They were never constructed outside
  benchmarks and carried divergent, misleading mmap/threshold logic. The single
  real read path is `SSTableReader` with the opt-in `BlockSource::Mapped` mapping
  (#589). Benchmark coverage was retained on the real reader.

### Fixed

- **Bounded the uncompressed/headerless read allocation** (Issue #592) — the
  uncompressed read path (`read_uncompressed_data_block` in
  `storage/sstable/reader/block_io.rs`) read the entire current-position-to-EOF
  range with a single `vec![0u8; remaining]`, zero-initializing and copying the
  whole data section into one heap `Vec`. With the opt-in memory map (#589) the
  bytes could be resident twice, breaking the <128MB memory target on large
  uncompressed SSTables. The read now streams through a reusable scratch buffer
  capped at `read_buffer_size` (shared helper `read_into_vec_capped`, the same
  shape the compressed large-block path already used), so the transient working
  set no longer scales with file size and the redundant zeroing is gone.
  Behavior is byte-identical and the `estimated_memory_usage` health metric is
  unaffected (it accounts for the block cache, not transient read buffers).
  Regression coverage: an instrumented-reader unit test asserts the scratch
  buffer stays capped for a block 64× its size, plus an end-to-end test over the
  `uncompressed_table` fixture (`issue_592_bounded_uncompressed_read.rs`).

- **mmap write-while-mapped guard + delete/publication policy** (Issue #591) —
  hardens the opt-in memory-mapped read path (#589, default OFF) against the
  compaction delete path. A memory map aliases a Data.db file's bytes for the
  reader's lifetime; deleting or truncating a mapped file can fault with `SIGBUS`
  on Unix or block deletion on Windows. The invariant is now enforced and tested:
  1. Compaction reads its inputs through **buffered I/O**, never a memory map
     (pinned explicitly in `KWayMerger`, independent of the global `use_mmap`
     setting), and drains them into memory before any delete — so the merger
     never holds a mapping over a file it removes.
  2. SSTable deletion removes **`TOC.txt` first** (the publication barrier), then
     the data components best-effort. The compaction candidate scan
     (`scan_data_files`) now skips any Data.db lacking a sibling TOC.txt, matching
     the read path. A component still pinned by a mapped reader on Windows
     therefore becomes an invisible orphan (reclaimed by the startup sweep)
     rather than a failed delete or a duplicate-row source.

  Regression coverage: an end-to-end test opens the inputs through a mmap-enabled
  `SSTableManager` and then compacts/deletes them
  (`issue_591_mmap_compaction_delete.rs`), plus unit tests for TOC-first deletion
  and the publication-barrier candidate scan. Constraints documented on
  `StorageConfig::use_mmap` and the write engine.

- **Compaction panicked when triggered from an async context** (Issue #587) —
  a high-severity panic shipped in v0.10.0. `WriteEngine::maintenance_step()` is
  synchronous but bridges to async I/O to read a merge's input SSTables. The
  bridge used `tokio::runtime::Handle::current().block_on(future)` whenever a
  runtime was already running on the calling thread, which panics with *"Cannot
  start a runtime from within a runtime"*. Because the bridge is only reached once
  a merge has input SSTables to read, STCS compaction worked in isolation but was
  **unreachable from any `#[tokio::main]`/async caller** — including the CLI's
  `maintenance` and `export-sstable --compact` subcommands (both run under
  `#[tokio::main]`).

  Fix: the shared async-to-sync bridge (`merge::block_on_async`, now also used by
  `flush_internal` and `finalize_merge_blocking`) detects an already-running
  runtime and offloads the future to a dedicated scoped thread with its own
  runtime, joining before returning. This is runtime-flavor-agnostic (works for
  both multi-thread and current-thread runtimes, unlike `block_in_place`) and
  preserves the synchronous public signature of `maintenance_step` that the CLI
  and Python bindings depend on. The Node binding already wrapped the call in
  `spawn_blocking` and was unaffected; the Python binding calls from outside any
  runtime and was likewise unaffected. Regression coverage drives
  `maintenance_step()` from inside both runtime flavors
  (`cqlite-core/tests/issue_587_compaction_async_bridge.rs`).

- **Partition-key column dropped / `WHERE` on TEXT PK returned 0 rows** (Issue #586) —
  a correctness regression shipped in v0.10.0. On the scan + residual-filter path
  (used for `WHERE` on a TEXT partition key, unlike the Index.db point-lookup path
  for UUID keys, #548/#553), partition-key columns are reconstructed from the raw
  row key. The reconstructor assumed a `u16` length prefix for *every* TEXT key,
  which is the composite-component framing, not the single-component layout (raw
  bytes). Consequences, both now fixed:
  1. A **single-component TEXT partition key** (`id text PRIMARY KEY`) failed to
     decode; the error was silently swallowed, so `SELECT *` was missing the PK
     column and `WHERE id = '<literal>'` returned 0 rows.
  2. A **composite partition key** decoded every column from the first component,
     so second+ PK columns got the wrong value and non-text components (e.g. a
     `date`) became debug strings.

  The scan path now decodes through the canonical, always-compiled
  `storage::partition_key_codec`, the exact codec the write engine's
  `PartitionKey::from_bytes` uses (single source of truth for both paths). A failed
  reconstruction is now logged via `log::warn!` instead of being swallowed, so this
  class of bug cannot ship invisibly again.

- **Writer temporal deltas now use unsigned VInt, not ZigZag** (Issue #644) —
  per Cassandra's `SerializationHeader`, every row-header temporal delta
  (timestamp, TTL, local-deletion-time) is written with unsigned VInt. The
  writer previously ZigZag-encoded these fields while the reader (fixed in #629)
  expected unsigned VInt, so every positive timestamp delta read back as roughly
  2× its real value. Corrected across all `data_writer.rs` row/cell/complex/range
  paths.

- **Correct tombstone serialization in the Data.db writer** (Issues #716, #717) —
  fixes four tombstone shapes that Cassandra 5.0.2 rejected or misread on
  `nodetool refresh` readback. Tombstone cells now set `HAS_EMPTY_VALUE` (without
  it the reader consumed a phantom value and desynced the row stream), and row
  tombstones now write the columns subset after the deletion times as
  `UnfilteredSerializer` requires (omitting it made Cassandra read the next row's
  flags byte as the subset bitmask). Pure row tombstones no longer carry
  primary-key liveness, matching Cassandra's serializer.

- **Correct Summary.db offset-table encoding and first/last key tracking**
  (Issue #666) — offset-table entries are now biased by the offset-table size so
  `offset[0]` equals the table size (absolute layout Cassandra's
  `IndexSummary.deserialize` asserts), and first/last key plus partition count
  are tracked for every partition via a new `note_partition()` rather than only
  at sampling boundaries — so tables with fewer than `min_index_interval`
  partitions no longer collapse the range filter to a single key.

- **Removed the last parser heuristic from `parse_cql_value`** (Issue #648) — the
  Ascii/Varchar arm carried three heuristic fallbacks (4-byte length prefix,
  null-terminated, raw UTF-8) "for test compatibility," violating the
  no-heuristics mandate (#28). The caller already extracts exactly the value
  bytes, so the entire slice is the text; the default path now treats it as UTF-8
  and errors on invalid input instead of silently accepting garbled data. The old
  paths remain behind the opt-in `legacy-heuristics` feature flag.

### Documentation

- **New documentation site** — a Starlight-based site (with rustdoc published to
  `/api/`) consolidates the user, CLI, bindings, use-case, and agent-developer
  docs, replacing the scattered in-repo guides as the source of truth.

- **SSTable definitive guide audited against Cassandra 5.0.8** — the Data.db,
  Index/Summary, Statistics.db, compression, bloom-filter/checksum, BTI, SAI,
  and version-matrix chapters were verified field-by-field against the
  cassandra-5.0.8 source and corrected.

## [v0.10.0] - 2026-06-02

Minor release. Three query-engine correctness/performance fixes (#548, #553,
#581), the new `Durability` write API (#547), `write-support` enabled by
default (#558), and a batch of developer-experience, CI, and documentation
improvements. 14 PRs since v0.9.2.

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

- **Index.db point-lookup performance cliff** (Issue #553) — `lookup_partition_with_index`
  previously computed a Murmur3 digest of the raw partition key and looked that digest up in
  the Index.db `key_lookup` map, which is keyed on **raw** partition key bytes (since #552).
  The digest never matched, so every `get()` call fell back to an O(n) sequential scan of
  Data.db. Results were always correct but at O(file-size) cost per lookup.

  Fix: the digest computation (`compute_partition_key_digest`) has been removed from the
  hot path. `lookup_partition_with_index` now passes the raw `partition_key: &[u8]` bytes
  directly to `index_reader.lookup_partition`, restoring the O(1) HashMap lookup that was
  present before #552 changed the key representation. Callers already pass raw bytes
  (single = raw value bytes, composite = `[len u16 BE][value][0x00]` per component).

  `lookup_partition_with_schema_context` (the schema-driven variant) is unchanged.

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

- **Linux x86_64 musl release target + SHA-256 checksums** — release binaries now
  include a statically-linked `x86_64-unknown-linux-musl` artifact plus a SHA-256
  checksum per asset, and the README gained an install section (#561, #568).

### Changed

- **`write-support` is now a default feature** of `cqlite-core`. The write path
  (`WriteEngine`, `Mutation`) is available out of the box; downstream consumers no
  longer need to opt in to enable it. This adds **no new dependencies** —
  `write-support` gates only first-party code, so the dependency surface for
  read-only consumers is unchanged. `flush`/`compact` on the high-level `Database`
  type remain behind the separate `experimental` feature (#558).

### Documentation

- **README feature → public-API table** mapping each Cargo feature to the API it
  gates (#557).
- **"Using cqlite-core as a dependency" guide** plus a compiling write-path example
  (#559).
- **Write-path concurrency & durability model** documented end to end (#560).
- **Per-tag rustdoc published to GitHub Pages** with a discoverable changelog link
  (#563).

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
