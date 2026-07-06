# Design — delete-dead-reader-stacks (issue #1597, Epic G / G1)

## Context

Anchors verified on the worktree base (`main` @ `7af5ebb9`) via workspace grep:

- **`SchemaAwareReader`**: defined in `cqlite-core/src/storage/sstable/schema_aware_reader.rs`;
  unit tests in `schema_aware_reader_test.rs`; re-exported at
  `storage/sstable/mod.rs:36` (`pub use schema_aware_reader::SchemaAwareReader;`). Zero `src/`
  (non-test) references — the hits in `reader/parsing/*.rs` and `data_access/mod.rs` are doc/log
  strings that name the type, not uses. Test-only consumers:
  `cqlite-core/tests/{schema_aware_reader_integration_test,counter_type_integration_test,type_invariant_tests}.rs`
  and `tests/{golden_path_get_operations_tests.rs,golden_path/harness.rs,golden_path/integration_tests.rs}`.
- **`ChunkedDataReader`**: defined in `chunked_data_reader.rs`; `mod` at `mod.rs:8`. Zero `src/`
  consumers. Test-only: `tests/chunked_data_reader_{integration,direct}_test.rs`.
- **`StreamingDecompressor`** + `ChunkedDecompressionConfig`: defined in `compression.rs`; zero
  consumers outside `compression.rs` (own tests only). `CompressionReader` is used only via `new()`,
  `algorithm()`, and `.is_some()` across `reader/data_access/*`, `bti.rs`, `scan_stream_windowed.rs`,
  `parsing/*` — `read_streaming`/`read`/`with_block_size`/`block_size` have zero external callers.
- **Duplicate CompressionInfo parse on open** (`reader/mod.rs::open`):
  - line ~605 `detect_and_initialize_compression(&header, path)` → `reader/compression.rs`
    `load_compression_info` → `compression::CompressionInfo::parse_binary` (legacy), only to learn
    the algorithm; the `get_standard_compression_patterns` generation loop issues ~25 `exists()`.
  - line ~608 `Self::load_compression_info_metadata` → `compression_info::CompressionInfo::parse`
    (modern), returning `Option<Arc<CompressionInfo>>` for chunk decode. This path already derives
    the file name deterministically via `SsTableDescriptor::parse(path)` and does ONE `exists()`.
- The legacy `compression::CompressionInfo`'s only compiled consumers are `reader/compression.rs`
  (the second open-path parse) and the **unwired** `cqlite-cli/src/commands/test_compression.rs`
  (no `mod test_compression;` in `commands/mod.rs` — dead file).
- Work-counter idiom to copy: `read_work_counters.rs` — unconditional `record_*()` free fns whose
  body is `#[cfg(any(test, feature = "work-counters"))]` (zero overhead in release), getters/`reset`
  behind the same cfg. Integration tests enable `--features work-counters`.

## Decisions

### Decision 1 — Derive `CompressionReader` from the single modern parse
Reorder `open()` so `compression_info` (modern parse) is loaded first, then build the
`CompressionReader` from it:
```rust
let compression_info = Self::load_compression_info_metadata(path, &platform).await?;
let compression_reader = match &compression_info {
    Some(info) => Some(CompressionReader::new(info.algorithm_enum()?)),
    None => None,
};
```
`algorithm_enum()` returns `Result` and `parse()` already rejects unknown names, so no
`unwrap()`/`expect()` is introduced. This eliminates the second parse AND the ~25-probe legacy
detection path in one move. `detect_and_initialize_compression` and its private helpers
(`discover_compression_info`, `get_standard_compression_patterns`,
`scan_directory_for_compression_files`, `score_compression_file_match`,
`extract_generation_number`, `load_compression_info`, and the `legacy-heuristics`-gated
`detect_compression_heuristic`/`detect_compression_from_filename`) are deleted.
`extract_sstable_base_name` (used by `load_crc_reader`) and its tests are **kept**.

**Behavior equivalence:** for Cassandra 5.0 `na+`/`nb`/`da`, a compressed SSTable always ships a
`CompressionInfo.db`, so `Some(info)` ⇔ compressed and `info.algorithm_enum()` is the same algorithm
the legacy path returned. The pre-existing header-only Strategy-1 branch (algorithm from the header
with no `CompressionInfo.db`) produced a functionally-dead `Some(reader)`/`None(info)` state — you
cannot locate chunks without the metadata — so dropping it changes no working read. The 33-table +
compressed-fixture parity suite is the net.

### Decision 2 — Collapse `CompressionReader` to `{ algorithm }`, keep the accessor
Keep the `CompressionReader` type as a thin `{ algorithm: CompressionAlgorithm }` wrapper with
`new()` + `algorithm()` (its two live methods) rather than replacing the field type at ~10 call
sites. This satisfies "reduces to a plain algorithm field" with minimal call-site churn and lowest
regression risk. The dead `buffer`/`block_size` fields and `read`/`read_streaming`/`with_block_size`/
`block_size` methods are removed.

### Decision 3 — One-parse-per-open counter (wiring evidence)
Add a `compression_info_parses` counter to `read_work_counters` (same zero-in-release pattern).
Record it at the top of `compression_info::CompressionInfo::parse`. For the RED demonstration the
counter is also recorded (temporarily) at the top of the legacy `compression::CompressionInfo::
parse_binary` so a reader-open on the pre-consolidation tree counts 2; after the legacy parser is
deleted, only the modern site remains and an open counts exactly 1. The durable guard is the
surviving `compression_info::parse` instrumentation: any re-introduced second parse on the open path
trips the `== 1` assertion.

### Decision 4 — Delete dead test surface, preserve independent coverage
Dedicated dead-symbol test files are deleted whole. In shared test files, only the tests that
construct the dead symbol are removed; independent tests (type invariants, counter value parsing,
golden-path get/scan through `SSTableReader`) are preserved. Coverage of dead production code is not
meaningful coverage; the real read path keeps its parity + golden-path coverage.

## Risks

- **Feature-gated / test-crate dangling refs** (the #1 deletion risk): the default build and `--lite`
  gate do not compile `tombstones`/`write-support`/minimal-features/`cqlite-integration-tests`. The
  self-check compiles all of these explicitly.
- **Parity divergence** from the compression-reader derivation change: mitigated by the byte-for-byte
  33-table parity suite and compressed fixtures staying green.
