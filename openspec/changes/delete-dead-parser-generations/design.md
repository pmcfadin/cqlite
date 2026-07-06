## Context

Finding J3 of the parser performance audit identified parallel dead parser generations. This change
deletes only the provably-dead ones and installs a guard against re-introduction. The risk profile of a
deletion is dominated by (a) deleting something actually live, and (b) leaving a dangling reference in
feature-gated or test code that the default build does not compile. The design therefore centers on
per-symbol dead-code proof and a reachability guard.

## Dead-code proof method

For every symbol/file proposed for deletion, the whole workspace (`cqlite-core/src`, `cqlite-cli/src`,
`cqlite-flight/src`, `bindings/**`, `tests/**`, `cqlite-core/tests/**`, `benches/**`) was searched for
references. A symbol is deletable only when it has **zero non-test, non-comment, non-bench** references
(self-tests that exist solely to exercise a to-be-deleted function are deleted with it). Symbols the
audit named but that proof shows have a live caller are **retained and flagged** (`StatisticsAnalyzer`).

Proof summary:

| Symbol / file | Live (non-test) callers | Disposition |
|---|---|---|
| `optimized_complex_types.rs` / `OptimizedComplexTypeParser` | 0 (only `mod`/gated re-export, docs, `.disabled`) | delete |
| `zero_copy_parser.rs` | 0 (only `mod`, docs) | delete |
| `serialize_statistics` | 0 | delete |
| `parse_statistics_file` + 10 section sub-parsers | 0 production; only in-module self-tests | delete + self-tests |
| `parse_unsigned_vint32` | 0 anywhere | delete |
| `parse_vint_binary` (`binary.rs`) | 0 production; only own self-tests | delete + self-tests |
| `parse_statistics_header` | test callers (no-heuristics regression) | **keep** |
| `parse_timestamp_statistics` | test callers (legacy sentinel decode) | **keep** |
| `StatisticsAnalyzer` / `StatisticsSummary` | `StatisticsReader::analyze` (compiled) + #1325 tests | **keep + flag** |
| statistics type structs + bucket types | `enhanced_statistics_parser`, `statistics_reader` | **keep** |

## Unwired-symbol guard design

The guard is an integration test (`cqlite-core/tests/parser_no_unwired_modules.rs`). It:

1. Parses `cqlite-core/src/parser/mod.rs` and collects every module declared with `mod`/`pub mod`/
   `pub(crate) mod` that is **not** gated by `#[cfg(test)]` and **not** gated by
   `#[cfg(feature = "benchmarks")]`.
2. For each such module `M`, decides it is **wired** if EITHER:
   - **(a)** some non-test, non-bench `.rs` file in the crate/workspace source tree — other than `M`'s
     own file/directory and other than `parser/mod.rs` — contains a `M::` path reference; OR
   - **(b)** `parser/mod.rs` contains a **non-`cfg`-gated** `pub use M::` facade re-export (the
     immediately-preceding attribute line is not `#[cfg(...)]`). This covers `binary`, whose items are
     reached only through the facade.
3. Asserts every collected module is wired, failing with the list of orphaned modules.

This is deterministic (static source scan, no runtime reflection) and low-false-positive (path-segment
`M::` match, not a bare-word match that common English words like "header"/"types" would satisfy). Run
against pre-delete `main` it reds on exactly `optimized_complex_types` and `zero_copy_parser` (neither
has a `M::` use nor a non-gated re-export); after the deletion those modules are gone and all remaining
non-test/non-bench modules are wired, so it passes. Excluding benchmark-gated re-exports from mode (b)
is what makes the pre-delete red fire on `optimized_complex_types` (its only re-export is
`benchmarks`-gated).

## Import-pruning

Deleting the legacy statistics parse tree removes the last users of the `nom` combinators `count` and
`be_f64`, the `parse_vint_length_signed` import from `super::vint`, and the `crate::error::{Error,
Result}` import (only `serialize_statistics` used them). These are pruned; the retained
`parse_statistics_header`/`parse_timestamp_statistics`/`parse_vint_as_u64` keep `take`, `be_u8`,
`be_i64`, `be_u32`, `be_u64`, and `parse_vint`. `-D warnings` (unused-import lint) is the backstop.

## Gate-script sync

No integration **test target** is deleted (the deleted statistics/vint self-tests are in-module unit
tests, not `tests/<name>.rs` targets), and no Cargo **feature** is removed, so `scripts/agent-gate.sh`
needs no edit for those. The new guard is a `cqlite-core` default-feature integration test discovered
automatically by `cargo test`, so it needs no explicit `--test` wiring in the gate. `scripts/agent-gate.sh`
is grepped for every deleted symbol/target/feature name to confirm no dangling reference.
