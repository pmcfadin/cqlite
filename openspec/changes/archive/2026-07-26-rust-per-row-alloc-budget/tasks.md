# Tasks — Rust per-row allocation-budget ratchet + L5 FxHash row map

## 1. Baseline measurement (informs the assert, no code change yet)
- [x] 1.1 Write a throwaway harness driving `build_row_from_scan_cached` (surface: `select_executor/mod.rs`)
      inside `test_alloc_probe::measure` over a narrow + a wide synthesized result; record allocations/row.
- [x] 1.2 Record the negative-control deltas: allocations added by (a) reverting #1447 into_iter→clone,
      (b) allocating a fresh key string per cell. Confirm each pushes over the observed baseline.

## 2. The alloc-budget ratchet test (surface: public `build_row_from_scan_cached`)
- [x] 2.1 Add `#[cfg(all(test, feature = "state_machine", not(feature = "dhat-heap")))]` test(s) in
      `cqlite-core/src/query/select_executor/` (near `row_build.rs` / mirroring `lookup.rs:822`) asserting
      `allocs/row <= MEASURED_BASELINE` for narrow + wide fixtures.
- [x] 2.2 Document in-test: the measured baseline numbers, the negative-control deltas, and why the assert is
      tolerance-free (deterministic counting allocator) — or the documented slack + reason if not.
- [x] 2.3 Confirm the test compiles + runs under the default feature set (`cargo test -p cqlite-core`),
      i.e. it is not silently skipped.

## 3. L5 — FxHashMap for row_values (surface: `row_build.rs:246`)
- [~] 3.1 (REVERTED — see notes) Swap `HashMap<Arc<str>, Value>` → `rustc_hash::FxHashMap<Arc<str>, Value>` (dep already in
      `cqlite-core/Cargo.toml:76`), preserving the capacity hint (`with_capacity_and_hasher` /
      `default()`+`reserve`).
- [~] 3.2 (REVERTED) Confirm output-equivalence (same keys/values/shape) and that the alloc-budget ratchet still passes
      at/below baseline (alloc-neutral hasher swap).
- [~] 3.3 (REVERTED) Check no downstream code depends on `HashMap` iteration order (it is already non-deterministic).

## 4. L4 verdict + docs (surface: throughput-program doc)
- [x] 4.1 From the measured per-row allocation count, decide the L4 verdict: file a concrete follow-up issue
      (exact hoistable site) OR credit L4 as a measured 1.0×/no-op.
- [x] 4.2 Update `docs/architecture/throughput-program-2026-07.md` §7 M4 with the measured allocations/row +
      the L4 verdict + the L5 result. Update CLAUDE.md only if a user-facing surface changed (it does not).

## 5. Gate / review / audit (endgame — via flow-implement → flow-closer)
- [x] 5.1 `scripts/agent-gate.sh --lite` green each fix round (summary-file redirect).
- [x] 5.2 Review-first on the lite-green diff: `rust-reviewer` + roborev; triage blockers/nits per
      `docs/development/roborev-severity.md`.
- [x] 5.3 Open PR (`Closes #1883`) — PR #2904, squashed to `4c299d14`.
- [x] 5.4 flow-closer: ONE full `scripts/agent-gate.sh` → C (`spec-auditor` anchored to
      `openspec/changes/rust-per-row-alloc-budget/specs/**`) → final roborev → merge-on-green → finalize.
      Gate of record: full PASS at anchor `5cd27667` (29 components, 0 FAIL), re-certified for the
      markdown-only tail by `--delta` at `483be63d` and `5ba89e2c`. C: PASS (all 4 requirements
      `satisfied`). roborev: clean of blockers (1 Medium + 3 Low all fixed pre-merge; 4 nits
      deferred to #2928).

## 6. Implementation notes (measured outcomes)

- **1.2 scope correction (owner-ratified).** #1447/#1445/#1446 are BINDING-layer fixes (Node
  `ExecuteNativeTask::compute`, Node JsString interning, Python `Row` ordering), so no `cqlite-core` test can
  gate them. Reverting clone→move in this crate is exactly alloc-neutral (41 vs 41 narrow, 273 vs 273 wide) —
  `Value::Text` is `Bytes`-backed and TIER-1 compaction (#1644) copies small payloads either way. The ratchet
  is anchored instead to per-cell name interning (#1334) + the sized map (#1584), verified RED-on-revert
  (narrow 41 → 89, wide 273 → 785). Binding-layer probe filed as **#2894**.
- **Section 3 (L5) REVERTED before merge (owner decision).** Implementing it proved it is a PUBLIC breaking
  API change (`row_values` moves into `QueryRow.values`; needed a `RowValues` alias rippling through
  cqlite-core/cqlite-flight/cqlite-cli) AND that it contradicts the `rustc-hash` invariant in
  `cqlite-core/Cargo.toml` (#1590 E8: not for untrusted string keys — column names come from the file's
  serialization header on the default path). With no benchmark run, the win stayed unquantified. Deferred to
  **#2901** behind a measurement + a HashDoS answer + an API plan.
- **4.1 L4 verdict: measured 1.0× / no-op.** The partition-key path costs ZERO per-row allocations (`RowKey`
  is `Arc<[u8]>`; `PartitionKeyCache` #1817 already hoists the decode). No follow-up filed; no win claimed.
- **Baselines are hasher-independent**: 41/273 measured identically with and without FxHashMap, which is also
  why the ratchet alone could never have served as L5's wiring evidence.
