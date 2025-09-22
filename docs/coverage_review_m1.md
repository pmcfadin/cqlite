# M1 Coverage Review – cqlite-core

## Current Snapshot
- Tarpaulin run shows 30.23% line coverage; CI requires 35%, leaving ≈1,100 lines uncovered.
- Large swaths of the core read path, schema plumbing, and validation framework execute zero lines during tests, leaving major M1 functionality unverified.
- Data gathered from `target/tarpaulin/cqlite-core-coverage.json` (latest run in this workspace).

## Highest-Risk Gaps (Core Read Path)
1. `cqlite-core/src/storage/sstable/reader.rs:232` — 1,247 of 1,269 traced lines missed. Key blind spots:
   - Header heuristics (`detect_ascii_header_corruption`, `parse_header_with_version_detection`) never exercised.
   - Hot paths `open`, `get`, `scan`, and compressed block handling run only in production.
   - Suggestion: create focused async tests that instantiate `SSTableReader` against small fixtures from `tests/fixtures`, covering bloom filter/index fallbacks, corrupted header fallback, and `scan` with limits.
2. `cqlite-core/src/storage/sstable/row_cell_state_machine.rs:245` — 538/552 missed. The Cassandra 5 row parsing FSM has no test harness.
   - Add unit tests feeding synthetic buffers for each state transition (`parse_header`, `parse_partition_key`, tombstone handling) using small schemas from `tests/src/schema`. Property-based tests can validate error transitions for malformed inputs.
3. `cqlite-core/src/storage/sstable/streaming_reader.rs:44` and `.../bulletproof_reader.rs:32` — 287/329 and 255/283 lines missed respectively.
   - Build regression tests that stream known BTI tables and assert chunk boundaries, checksum detection, and retry logic.
   - These tests can reuse `tests/fixtures/bti` to avoid large downloads.

## Parser & Type System Coverage
- `cqlite-core/src/parser/visitor.rs:90` (593/769 missed) and `.../types.rs:70` (514/789 missed) drive CQL deserialization yet run untested in CI.
  - Introduce table-driven unit tests covering each branch in `parse_cql_value` (numeric widths, ASCII fallback, length-prefixed blobs) and tuple/list parsing using `UdtRegistry` stubs.
  - `visitor.rs` can be covered by round-tripping available `tests/parser_abstraction_tests.rs` inputs through the visitor to verify callbacks fire per node.
- `cqlite-core/src/parser/vint.rs:40` and `.../optimized_complex_types.rs:28` also show >70% miss; extend existing `tests/debug_vint.rs` style fixtures to assert overflow and negative-case handling.

## Schema & Query Surfaces
- `cqlite-core/src/schema/registry.rs:61` (405/493 missed) leaves auto-discovery, caching, and validation logic unverified.
  - Add async tests using `tempfile::TempDir` to simulate discovery and ensure version history updates when registering schemas; verify eviction via `cache_ttl_seconds`.
- `cqlite-core/src/query/executor.rs:50` (302/389 missed) and `.../planner.rs:40` (251/344 missed) lack point/range lookup coverage.
  - Use in-memory `StorageEngine` backed by `MemTable` and mock plans to assert selection, insert routing, and failure paths (`condition_to_row_key`).
  - Snapshot results from `QueryResult` to verify metadata population and plan info.

## Validation Framework Weight
- Validation modules (`validation/hardened_validator_parser.rs:20`, `.../error_handling.rs:14`, `.../data_integrity.rs:16`, `.../format_compatibility.rs:21`) contribute ~2,300 untouched lines.
  - If required for M1 readiness, prioritize smoke tests that instantiate each validator with canned configs and assert basic success/failure paths (e.g., corrupted SSTable detection).
  - Otherwise, consider gating under optional features or moving long-lived demos (`src/bin/*`) to `examples/` so Tarpaulin can exclude them without affecting core coverage.

## Quick Wins & Next Steps
1. **Targeted SSTableReader suite** — Hitting 200–300 critical lines here should add ~1.5% coverage alone.
2. **RowCellStateMachine table tests** — Probing transition matrix adds another ~1% (low setup cost, pure Rust).
3. **Parser branch tests** — Covering ASCII/UTF8 fallbacks, varint edges, and collection parsing yields ~0.8%.
4. **Schema registry smoke tests** — Register/discover schema and validate TTL/cache behaviour (~0.5%).
5. **Trim or gate validation binaries** — Moving `src/bin/sstable_data_demo.rs` and peers to `examples/` removes >800 uncovered lines from metrics if they are not part of M1.

Delivering the first four items should comfortably exceed the 1,100-line delta while also proving the M1 read-path quality bars. EOF
