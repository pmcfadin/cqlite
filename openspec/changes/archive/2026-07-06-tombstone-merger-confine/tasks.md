# Tasks — Confine the legacy `TombstoneMerger` (G4)

## 1. Verify hazard 2 is already retired (no code change)
- [x] 1.1 Confirm `execute_parallel_table_scan` is retired and replaced by a single
  `scan_stream` pass (`cqlite-core/src/query/executor.rs`); confirm the work-counter proof
  `table_scan_parallel_branch_issues_one_whole_table_pass` exists and asserts `== 1`.
  *Surface exercised:* `SelectExecutor::execute` (TableScan plan) + `storage::table_scan_call_count`.

## 2. Delete the named quadratic path (TDD: prove deadness first)
- [ ] 2.1 `rg apply_range_tombstones` / `range_tombstone_applies` across
  `cqlite-core/src`, `cqlite-cli/src`, `bindings`, `cqlite-core/tests`; paste the
  zero-production-call-site result into the PR (deadness verification).
- [ ] 2.2 Delete `TombstoneMerger::apply_range_tombstones` and
  `TombstoneMerger::range_tombstone_applies` and their `test_range_tombstone_application`
  unit test from `cqlite-core/src/storage/sstable/tombstone_merger.rs`.
  *Surface exercised:* the (now absent) `TombstoneMerger` public API under `--features tombstones`.

## 3. Fix the `new()` unwrap (no-unwrap hard rule)
- [ ] 3.1 Replace `SystemTime::now().duration_since(UNIX_EPOCH).unwrap()` in
  `TombstoneMerger::new()` with a graceful `unwrap_or_default()` fallback (epoch 0).
  *Surface exercised:* `TombstoneMerger::new()`.
- [ ] 3.2 Add a unit test that builds via the production `TombstoneMerger::new()` (not
  `with_time`) and runs `merge_generations`, proving the production constructor works and
  is exercised (today all tests use `with_time`).
  *Surface exercised:* `TombstoneMerger::new()` + `merge_generations`.

## 4. Doc-scope the confined module
- [ ] 4.1 Add/extend the module `//!` header on `tombstone_merger.rs`: `tombstones`-only,
  off the default C1/C4 fast path, per-method complexity of the retained live surface
  (`merge_generations` O(n log n), `fast_tombstone_check` O(1),
  `batch_merge_with_tombstones` O(Σ gens · log)), rationale for accepting the cost, and the
  future KWay-point-path consolidation direction.
  *Surface exercised:* module documentation (read by reviewers / rustdoc).

## 5. Feature-matrix hygiene
- [ ] 5.1 Build+lint clean WITHOUT `tombstones` (default) and WITH `--features tombstones`
  under `RUSTFLAGS="-D warnings"`.
- [ ] 5.2 Confirm the existing `--all-features` clustered full-scan parity test
  (`cqlite-core/tests/issue_1085_tombstones_full_scan_parity.rs`) still passes with a real
  fixture and SKIPs cleanly without one.
  *Surface exercised:* `Database::execute` full-scan under `tombstones`.

## 6. Gate + review
- [ ] 6.1 Run the FAST iteration gate on each fix round:
  `CQLITE_ALLOW_FILE_GROWTH=1 CQLITE_DATASETS_ROOT=<main-checkout>/test-data/datasets bash scripts/agent-gate.sh --lite` → RESULT: PASS.
- [ ] 6.2 Pre-roborev self-check: scan the diff for `manual_range_contains`, integer
  overflow/saturation, float ordering vs Java, wall-clock test races, no-heuristics
  violations, gitignored reference binaries.
- [ ] 6.3 Full `scripts/agent-gate.sh` (run by the orchestrator, serialized) → PASS.
- [ ] 6.4 C (spec-auditor) intent audit anchored to
  `openspec/changes/tombstone-merger-confine/specs/**` → PASS.
- [ ] 6.5 roborev (`--branch --base origin/main --agent codex`) clean.
