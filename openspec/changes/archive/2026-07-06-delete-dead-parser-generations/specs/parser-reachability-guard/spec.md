## ADDED Requirements

### Requirement: Dead parser generations are removed
The dead, unwired parser generations identified by parser-audit finding J3 SHALL be removed from
`cqlite-core`: the whole modules `parser/optimized_complex_types.rs` and `parser/zero_copy_parser.rs`,
the legacy `Statistics.db` parse/serialize subtree in `parser/statistics.rs` (`parse_statistics_file`
and its section sub-parsers, and `serialize_statistics`), and the orphaned VInt adapters
`parse_unsigned_vint32` (`parser/vint.rs`) and `parse_vint_binary` (`parser/binary.rs`). Each removed
symbol SHALL have had zero non-test, non-benchmark callers in the workspace prior to removal.

#### Scenario: Deleted modules and symbols have no remaining references
- **WHEN** the workspace is searched for `optimized_complex_types`, `OptimizedComplexTypeParser`,
  `zero_copy_parser`, `serialize_statistics`, `parse_unsigned_vint32`, or `parse_vint_binary`
- **THEN** no compiled (non-comment, non-`.disabled`) Rust source references any of them

#### Scenario: The crate builds on all feature combinations after removal
- **WHEN** `cqlite-core` is built with default features, with `--no-default-features --features
  all-compression`, and with the `benchmarks` feature enabled
- **THEN** every build succeeds with no unused-import or dead-code warnings under `-D warnings`

### Requirement: Shared statistics types and the live report path are preserved
The removal SHALL NOT delete statistics type definitions reused by the live parser, nor any statistics
function or analyzer that retains a compiled (non-test) caller. Specifically `SSTableStatistics`,
`StatisticsHeader`, `RowStatistics`, `TimestampStatistics`, `ColumnStatistics`, `TableStatistics`,
`PartitionStatistics`, `CompressionStatistics` and their histogram bucket types SHALL remain, as SHALL
`parse_statistics_header`, `parse_timestamp_statistics`, `StatisticsAnalyzer`, and `StatisticsSummary`.

#### Scenario: Enhanced statistics parser and reader still compile against retained types
- **WHEN** `enhanced_statistics_parser` and `storage::sstable::statistics_reader` are compiled after the removal
- **THEN** they resolve every statistics type and `StatisticsAnalyzer`/`StatisticsSummary` symbol they import, and their tests pass

#### Scenario: Retained parsers keep their pinning tests
- **WHEN** the statistics no-heuristics regression tests and the legacy timestamp-decode tests run
- **THEN** `parse_statistics_header` and `parse_timestamp_statistics` are still present and the tests pass

### Requirement: The live decode path is unchanged
The removal SHALL NOT alter the live decode engine `row_decoder` or any production
read/decode behavior; the 33-table golden parity harness SHALL remain green.

#### Scenario: row_decoder is untouched
- **WHEN** the change's diff is inspected
- **THEN** no file under `storage/sstable/reader/parsing/row_decoder` is modified

#### Scenario: Golden parity is preserved
- **WHEN** the SSTable read/parity tests run against the real test datasets after the removal
- **THEN** every previously passing table still parses to the same rows

### Requirement: An unwired-symbol guard prevents re-introduction
A test SHALL assert that every module declared under `cqlite-core/src/parser/` that is neither
`#[cfg(test)]`-gated nor `#[cfg(feature = "benchmarks")]`-gated has at least one non-test, non-benchmark
caller — either a `<module>::` path reference in a non-test/non-bench source file other than the module's
own source and `parser/mod.rs`, or a non-`cfg`-gated facade re-export in `parser/mod.rs`. The guard SHALL
fail with the list of orphaned modules when any qualifying module loses all callers.

#### Scenario: Guard passes when every parser module is wired
- **WHEN** the guard runs on the post-deletion tree
- **THEN** it finds a caller for every non-test, non-benchmark parser module and passes

#### Scenario: Guard reds on a newly-orphaned module
- **WHEN** a non-test, non-benchmark parser module has no `<module>::` caller and no non-gated facade re-export (as `optimized_complex_types` and `zero_copy_parser` were on pre-delete main)
- **THEN** the guard fails and names that module as unwired

#### Scenario: Benchmark-gated re-exports do not count as wiring
- **WHEN** a module's only re-export in `parser/mod.rs` is `#[cfg(feature = "benchmarks")]`-gated
- **THEN** the guard does not treat that re-export as a caller, so a module wired only through a benchmark-gated re-export is reported as unwired
