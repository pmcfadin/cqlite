## 1. Dead-code proof

- [x] 1.1 Prove `optimized_complex_types` / `OptimizedComplexTypeParser` has zero non-test, non-bench callers
- [x] 1.2 Prove `zero_copy_parser` has zero non-test, non-bench callers
- [x] 1.3 Prove the legacy statistics parse tree + `serialize_statistics` have zero production callers
- [x] 1.4 Prove `parse_unsigned_vint32` and `parse_vint_binary` have zero callers
- [x] 1.5 Confirm `StatisticsAnalyzer`/`StatisticsSummary`, the statistics types, and
      `parse_statistics_header`/`parse_timestamp_statistics` DO have callers → retain

## 2. Unwired-symbol guard (TDD red first)

- [x] 2.1 Add `cqlite-core/tests/parser_no_unwired_modules.rs` implementing the reachability guard
- [x] 2.2 Verify it reds on pre-delete state for exactly `optimized_complex_types` + `zero_copy_parser`

## 3. Delete the dead generations

- [x] 3.1 Delete `parser/optimized_complex_types.rs` and `parser/zero_copy_parser.rs`
- [x] 3.2 Remove their `pub mod` declarations, the `benchmarks`-gated re-export, and doc-comment references in `parser/mod.rs`
- [x] 3.3 Gate `collection_benchmarks` behind `#[cfg(feature = "benchmarks")]` in `parser/mod.rs`
- [x] 3.4 Delete the legacy statistics parse tree + `serialize_statistics` + their self-tests; prune unused imports
- [x] 3.5 Delete `parse_unsigned_vint32` (`vint.rs`) and `parse_vint_binary` + tests (`binary.rs`); fix the vint doc-comment reference
- [x] 3.6 Grep `scripts/agent-gate.sh` for every deleted symbol/target/feature — confirm no dangling reference

## 4. Validate

- [x] 4.1 `scripts/agent-gate.sh --lite` PASS
- [x] 4.2 `cargo +1.88.0 fmt --check` clean; `RUSTFLAGS="-D warnings" cargo clippy -p cqlite-core --features cli-helpers`
- [x] 4.3 Minimal-features + `tombstones` + `write-support` builds compile; `cqlite-integration-tests --no-run` compiles
- [x] 4.4 `openspec validate delete-dead-parser-generations --strict` clean
