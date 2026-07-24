# Tasks — Rust per-row allocation-budget ratchet + L5 FxHash row map

## 1. Baseline measurement (informs the assert, no code change yet)
- [ ] 1.1 Write a throwaway harness driving `build_row_from_scan_cached` (surface: `select_executor/mod.rs`)
      inside `test_alloc_probe::measure` over a narrow + a wide synthesized result; record allocations/row.
- [ ] 1.2 Record the negative-control deltas: allocations added by (a) reverting #1447 into_iter→clone,
      (b) allocating a fresh key string per cell. Confirm each pushes over the observed baseline.

## 2. The alloc-budget ratchet test (surface: public `build_row_from_scan_cached`)
- [ ] 2.1 Add `#[cfg(all(test, feature = "state_machine", not(feature = "dhat-heap")))]` test(s) in
      `cqlite-core/src/query/select_executor/` (near `row_build.rs` / mirroring `lookup.rs:822`) asserting
      `allocs/row <= MEASURED_BASELINE` for narrow + wide fixtures.
- [ ] 2.2 Document in-test: the measured baseline numbers, the negative-control deltas, and why the assert is
      tolerance-free (deterministic counting allocator) — or the documented slack + reason if not.
- [ ] 2.3 Confirm the test compiles + runs under the default feature set (`cargo test -p cqlite-core`),
      i.e. it is not silently skipped.

## 3. L5 — FxHashMap for row_values (surface: `row_build.rs:246`)
- [ ] 3.1 Swap `HashMap<Arc<str>, Value>` → `rustc_hash::FxHashMap<Arc<str>, Value>` (dep already in
      `cqlite-core/Cargo.toml:76`), preserving the capacity hint (`with_capacity_and_hasher` /
      `default()`+`reserve`).
- [ ] 3.2 Confirm output-equivalence (same keys/values/shape) and that the alloc-budget ratchet still passes
      at/below baseline (alloc-neutral hasher swap).
- [ ] 3.3 Check no downstream code depends on `HashMap` iteration order (it is already non-deterministic).

## 4. L4 verdict + docs (surface: throughput-program doc)
- [ ] 4.1 From the measured per-row allocation count, decide the L4 verdict: file a concrete follow-up issue
      (exact hoistable site) OR credit L4 as a measured 1.0×/no-op.
- [ ] 4.2 Update `docs/architecture/throughput-program-2026-07.md` §7 M4 with the measured allocations/row +
      the L4 verdict + the L5 result. Update CLAUDE.md only if a user-facing surface changed (it does not).

## 5. Gate / review / audit (endgame — via flow-implement → flow-closer)
- [ ] 5.1 `scripts/agent-gate.sh --lite` green each fix round (summary-file redirect).
- [ ] 5.2 Review-first on the lite-green diff: `rust-reviewer` + roborev; triage blockers/nits per
      `docs/development/roborev-severity.md`.
- [ ] 5.3 Open PR (`Closes #1883`).
- [ ] 5.4 flow-closer: ONE full `scripts/agent-gate.sh` → C (`spec-auditor` anchored to
      `openspec/changes/rust-per-row-alloc-budget/specs/**`) → final roborev → merge-on-green → finalize.
