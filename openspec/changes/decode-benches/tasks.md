## 1. Bench-internals shim (production, additive, opt-in)
- [ ] 1.1 Add empty `bench-internals` feature to `cqlite-core/Cargo.toml` `[features]`.
- [ ] 1.2 Add `#[cfg(feature = "bench-internals")] #[doc(hidden)] pub fn decode_value_for_bench(&self, value_data: &[u8], data_type: &str) -> Result<Value>` on `SSTableReader`, forwarding verbatim to `parse_value_with_schema_type`. No `unwrap`/`expect`.

## 2. Per-type + throughput criterion benches
- [ ] 2.1 Add `cqlite-core/benches/decode_bench.rs` (`harness = false`) + `[[bench]] name = "decode"` in `Cargo.toml`.
- [ ] 2.2 `decode/type_<name>` group: open one real `SIMPLE` fixture reader once (outside the loop); decode a fixed representative buffer for each CQL type (all scalars + list/set/map/tuple/UDT/frozen) through `decode_value_for_bench`. Assert each decode yields the expected `Value` variant at setup (wiring-evidence — proves the live entry ran, not a no-op).
- [ ] 2.3 `decode/wide_row_primitives` group: assemble a fixed ~20-primitive-column row buffer set; decode all columns in a loop; `Throughput::Elements(rows)` → rows/sec.
- [ ] 2.4 `decode/text_heavy` group: text/blob-dominated block; `Throughput::Elements(rows)` → rows/sec.
- [ ] 2.5 Append per-run metrics via `crate::bench_ledger` (best-effort; ledger failure logs to stderr, never fails the bench).

## 3. Perf-gate wiring
- [ ] 3.1 Add `decode/wide_row_primitives` and `decode/text_heavy` to `cqlite-core/benches/perf-gate.json` `benches` (STRICT, `threshold_pct: 10`).
- [ ] 3.2 Add `--bench decode` to `.github/workflows/perf-regression.yml`: unconditionally on the PR run; guarded by `[[ -f cqlite-core/benches/decode_bench.rs ]]` on the base (main) run (compaction pattern).

## 4. allocs/row + allocs/cell dhat budget (A4 lane)
- [ ] 4.1 Add allocs/row + allocs/cell budget tests to `cqlite-core/tests/memory_budget.rs` (or a sibling `#[cfg(all(feature = "dhat-heap", feature = "cli-helpers"))]` target): drive real `Database::execute("SELECT * …")` full scan over the wide real fixture; profiler starts AFTER `open_read_db`.
- [ ] 4.2 Compute allocs/row = `total_blocks / rows`, allocs/cell = `total_blocks / (rows * cols)`; assert each ≤ pinned current-main ceiling; fail closed (panic) on 0 rows for a present fixture; SKIP only when the fixture is entirely absent.
- [ ] 4.3 Pin ceilings to measured current-main values + documented variance slack; document the measured numbers in code comments.

## 5. Baseline + red-run + validation
- [ ] 5.1 Record measured baseline numbers (per-type ns/op, wide-row & text-heavy rows/sec, allocs/row, allocs/cell) as a comment on issue #1615.
- [ ] 5.2 Demonstrate the allocs/row budget REDs when the ceiling is set below measured (paste the red output), then restore to the ratchet value.
- [ ] 5.3 `scripts/agent-gate.sh` PASS — paste the AGENT-GATE SUMMARY block verbatim.
