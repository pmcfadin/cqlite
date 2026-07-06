## Why

The July 2026 parser performance audit (`docs/reports/parser-performance-audit-2026-07-01.md`,
finding **J3**, audit block 2) quantified "built but unwired" dead parser generations sitting
beside the live decode code — misleading readers, auditors, and future wiring. Owner decision #9
(2026-07-01, Wave 2.5 dead-code purge) is: **delete the batch, behind a reachability guard.** This
is design-driven cleanup within the read-path/parser performance program and is **Seam-1
pre-approved** for the batch (standing owner approval, part of epic #1603 "one decoder").

Proving each symbol dead (zero non-test, non-bench callers across the whole workspace) then deleting
it removes ~1,200 LOC of confusing parallel code and, critically, installs an **unwired-symbol
guard** so the class cannot silently return: the guard fails if any non-test parser module loses all
callers.

Facts that constrain the scope (established by whole-workspace reference proof, not the audit's
estimate):

- `cqlite-core/src/parser/optimized_complex_types.rs` (633 LOC) and
  `cqlite-core/src/parser/zero_copy_parser.rs` (309 LOC) have **zero** non-test, non-bench callers.
  Their only references are the `mod`/`pub use` declarations in `parser/mod.rs`, a
  `#[cfg(feature = "benchmarks")]`-gated re-export of `OptimizedComplexTypeParser`, doc comments, and
  a `tests/src/m3_performance_validator.rs.disabled` file (not compiled). They are fully dead.
- The **legacy statistics parse/serialize subtree** in `cqlite-core/src/parser/statistics.rs`
  (`parse_statistics_file` + its ten section sub-parsers + `serialize_statistics`) has **zero
  production callers** (the live path is `enhanced_statistics_parser`); its only callers are its own
  in-module self-tests. It is production-dead.
- **`parse_unsigned_vint32`** (`vint.rs`) and **`parse_vint_binary`** (`binary.rs`) have zero callers
  anywhere. J4 (#1638, one-VInt-decoder) has already substantially landed (`decode_unsigned`/
  `decode_signed` are the canonical pair and the surviving `vint.rs` functions are thin adapters over
  them), so J3's vint scope shrinks exactly as the issue's sequencing note predicted — to these two
  orphaned adapters.
- `parser/mod.rs:120` declares `collection_benchmarks` **without** the `#[cfg(feature = "benchmarks")]`
  gate that every other benchmark module carries (the module's own inner `#![cfg(feature = ...)]`
  masks it) — the audit's flagged cfg inconsistency.

## What Changes

- **Delete** the two fully-dead whole modules `parser/optimized_complex_types.rs` and
  `parser/zero_copy_parser.rs`, their `pub mod` declarations, the `benchmarks`-gated
  `OptimizedComplexTypeParser` re-export, and the doc-comment references to them.
- **Delete** the production-dead legacy statistics parse/serialize subtree from `statistics.rs`
  (`parse_statistics_file`, `parse_row_statistics`, `parse_column_statistics`,
  `parse_single_column_statistics`, `parse_table_statistics`, `parse_partition_statistics`,
  `parse_compression_statistics`, `parse_metadata_section`, `parse_row_size_bucket`,
  `parse_partition_size_bucket`, `parse_value_frequency`, `serialize_statistics`) together with the
  in-module self-tests that exercise only those functions, and prune the now-unused imports.
- **Delete** the two orphaned VInt adapters `parse_unsigned_vint32` (`vint.rs`) and `parse_vint_binary`
  (`binary.rs`) plus `parse_vint_binary`'s self-tests.
- **Fix** the `collection_benchmarks` cfg inconsistency by gating its `mod.rs` declaration behind
  `#[cfg(feature = "benchmarks")]`.
- **Add the unwired-symbol guard**: a test asserting every non-test, non-benchmark module declared
  under `cqlite-core/src/parser/` has at least one non-test, non-bench caller (a `<mod>::` path use, or
  a non-gated facade re-export). Run against pre-delete `main` the guard reds on exactly
  `optimized_complex_types` and `zero_copy_parser`; after the deletion it passes.

## Non-goals

- **Do NOT touch the live decoder `v5_compressed_legacy`** — it is THE live decode engine for BIG and
  BTI (#1656, M3) and is entirely out of scope.
- **Keep shared type definitions.** The `SSTableStatistics`/`StatisticsHeader`/`RowStatistics`/
  `TimestampStatistics`/`ColumnStatistics`/`TableStatistics`/`PartitionStatistics`/
  `CompressionStatistics` structs (and the histogram bucket types they contain) are reused by
  `enhanced_statistics_parser` and `statistics_reader`; they stay.
- **Keep `parse_statistics_header` and `parse_timestamp_statistics`** — both have live test callers
  (`statistics_parser_no_heuristics_tests.rs` pins no-heuristics header rejection; `statistics_test.rs`
  pins legacy timestamp decode), so they are not orphaned.
- **Keep `StatisticsAnalyzer` / `StatisticsSummary`.** The audit listed `StatisticsAnalyzer` as dead,
  but whole-workspace proof shows a compiled caller (`StatisticsReader::analyze`/`compact_summary`/
  `generate_report` in `statistics_reader.rs`) and #1325 regression tests pinning the sentinel-vs-real
  reporting behavior. Per the dead-proof discipline "anything with a live caller stays and is flagged,"
  it is retained and the audit discrepancy is recorded on the issue.
- **Do NOT delete the `benchmarks` harness** (owned by platform AK1, block 6). This change only removes
  its dead `optimized_complex_types`/`zero_copy_parser` re-exports so the crate compiles; re-pointing
  the harness itself is AK1's work (cross-comment on landing).
- No production decode-path behavior change; no schema/format change; no new public API beyond the
  guard test.
