# Tasks — dhat alloc/memory-budget lane + `size_of::<Value>` pin (A4)

## 1. Measure today's numbers (measure-first)
- [ ] 1.1 Measure `size_of::<Value>()` on current `main` (record the exact byte value).
- [ ] 1.2 Measure total bytes allocated for a full-scan `SELECT *` workload over the largest real
      fixture (via a scratch dhat run), and peak heap bytes for the type-heavy materializing `SELECT *`.
      Record all three numbers.

## 2. dhat budget test target (TDD)
- [ ] 2.1 Add `cqlite-core/tests/memory_budget.rs`, `#![cfg(feature = "dhat-heap")]`, installing
      `#[global_allocator] static ALLOC: dhat::Alloc`. Include `#[path = "../benches/fixtures/mod.rs"] mod fixtures;`.
      Public surface exercised: `cqlite_core::Database::execute` over `benches/fixtures/mod.rs::open_read_db`.
- [ ] 2.2 Write `select_full_scan_alloc_budget` (`#[serial_test::serial]`): build a testing-mode
      `dhat::Profiler`, run the full-scan workload (assert ≥1 row first), assert total bytes ≤ pinned
      ceiling. Demonstrate red with a too-tight ceiling, then land the honest ceiling + comment
      (measured value + E2/E3 target).
- [ ] 2.3 Write `materialized_select_byte_ceiling` (`#[serial_test::serial]`): run the type-heavy
      materializing workload (assert ≥1 row first), assert `max_bytes` ≤ pinned ceiling AND ≤ 128 MiB.
      Demonstrate red with a too-tight ceiling, then land the honest ceiling + comment.

## 3. `size_of::<Value>` compile-time pin
- [ ] 3.1 Add `const _: () = assert!(std::mem::size_of::<Value>() <= N);` beside `Value` in
      `cqlite-core/src/types.rs`, `N` = measured value from 1.1, with a comment naming the measured
      number and Epic E #1517 E1's smaller target.

## 4. Gate wiring
- [ ] 4.1 Add `memory-budget` to the `COMPONENTS` array and `DATASET_COMPONENTS` in
      `scripts/agent-gate.sh`; run it as
      `cargo test --package cqlite-core --features cli-helpers,dhat-heap --test memory_budget -- --test-threads=1`.
- [ ] 4.2 Document the component + its budgets in the agent-gate header comment block.
- [ ] 4.3 (If touched in passing) fix the pre-existing partial-feature dead-code warning in
      `cqlite-core/examples/heap_profile.rs`; otherwise leave it (gate uses `--all-features` where it is
      green).

## 5. Verify
- [ ] 5.1 `RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets --all-features` clean
      (the `dhat-heap` file compiles under `--all-features`).
- [ ] 5.2 No `unwrap()`/`expect()` in library code (test code may use them).
- [ ] 5.3 Run `scripts/agent-gate.sh` — PASS; paste the AGENT-GATE SUMMARY block verbatim.
- [ ] 5.4 spec-auditor C verdict PASS; roborev `--base origin/main --agent codex` clean.
