# Tasks — delete-dead-reader-stacks (issue #1597, Epic G / G1)

## 1. TDD: one-parse-per-open counter + test (RED first)
- [x] 1.1 Add a `compression_info_parses` counter to `read_work_counters` (unconditional
      `record_compression_info_parse()` + cfg-gated getter + `reset`), mirroring the existing
      zero-in-release pattern; extend the round-trip unit test with a distinct multiplicity.
- [x] 1.2 Record the counter at the top of `compression_info::CompressionInfo::parse` (durable) and,
      temporarily, at the legacy `compression::CompressionInfo::parse_binary` for the RED demo.
- [x] 1.3 New integration test (`--features work-counters`, `CQLITE_DATASETS_ROOT`): reset, open a
      compressed fixture reader, assert parse count == 1. Skip-not-fail when the fixture is absent;
      never a silent pass. Confirm RED (== 2) against the pre-consolidation tree.

## 2. Consolidate to one CompressionInfo parser
- [x] 2.1 In `reader/mod.rs::open`, load `compression_info` first and derive `compression_reader`
      from `info.algorithm_enum()?` (no `unwrap`/`expect`); drop the `detect_and_initialize_compression`
      call.
- [x] 2.2 Delete the dead detection helpers in `reader/compression.rs`
      (`detect_and_initialize_compression`, `discover_compression_info`,
      `get_standard_compression_patterns`, `scan_directory_for_compression_files`,
      `score_compression_file_match`, `extract_generation_number`, `load_compression_info`, and the
      `legacy-heuristics` helpers) and their tests; keep `extract_sstable_base_name` + its tests.
- [x] 2.3 Delete the legacy `compression::CompressionInfo` (+ `ChunkInfo`, `parse`, `parse_binary`,
      `normalize_algorithm_name`) and their tests; delete the unwired
      `cqlite-cli/src/commands/test_compression.rs`. Remove the temporary RED-only counter site.

## 3. Collapse CompressionReader; delete StreamingDecompressor
- [x] 3.1 Delete `StreamingDecompressor` + `ChunkedDecompressionConfig` and their tests.
- [x] 3.2 Collapse `CompressionReader` to `{ algorithm }` with `new()` + `algorithm()`; delete
      `read`, `read_streaming`, `with_block_size`, `block_size`, and the dead fields/tests.

## 4. Delete SchemaAwareReader and ChunkedDataReader
- [x] 4.1 Delete `schema_aware_reader.rs`, `schema_aware_reader_test.rs`, and their `mod`/`pub use`
      wiring in `storage/sstable/mod.rs`.
- [x] 4.2 Delete `chunked_data_reader.rs` and its `mod` wiring; delete
      `tests/chunked_data_reader_{integration,direct}_test.rs`.
- [x] 4.3 Delete `cqlite-core/tests/schema_aware_reader_integration_test.rs`; remove the
      SchemaAwareReader-constructing tests + imports from `type_invariant_tests.rs`,
      `counter_type_integration_test.rs`, `tests/golden_path_get_operations_tests.rs`, and the unused
      imports in `golden_path/{harness,integration_tests}.rs`.

## 5. Validation
- [x] 5.1 `cargo +1.88.0 fmt` clean; lite gate PASS each fix round.
- [x] 5.2 Self-check compiles: default build, minimal-features
      (`--no-default-features --features all-compression`), `tombstones` clippy, `write-support`
      clippy, and `cargo test -p cqlite-integration-tests --no-run`.
- [x] 5.3 33-table parity green; one-parse-per-open test GREEN (== 1); changelog records the
      semver-visible public removals.
