# Tasks — flight-merge-runtime-amplification

## 1. Pin the defect with a failing regression test (TDD first)
- [x] Add a regression test that drives a **real** multi-SSTable merge (real SSTable inputs — never an
      empty dataset) and observes the process's peak OS thread count via `/proc/self/task` (Linux) /
      `proc_pidinfo(PROC_PIDTASKINFO)` (macOS).
      Done: `cqlite-core/tests/issue_2316_merge_thread_budget.rs` builds 4 real `nb` inputs
      (WriteEngine flush, 400 live rows each) and drives `KWayMerger::new(...).merge(...)` end-to-end.
- [x] Assert the peak thread delta over baseline is within `O(M)` (`PER_INPUT·M + slack`, coefficient
      independent of `num_cpus`); guard on `num_cpus >= 2` + platforms without a direct thread-count
      API. Confirmed FAILS on pre-change (delta=48 on a 10-core box, bound=15) and PASSES after the fix
      (delta 9–10).

## 2. Replace the per-producer multi-core runtime (recommended design (b))
- [x] In `producer_thread` replace `tokio::runtime::Runtime::new()` with
      `tokio::runtime::Builder::new_current_thread().enable_all().build()` (zero extra worker threads).
      Scan, emit callback, `SyncSender` backpressure, k-way heap, and `ScanCancel` wiring untouched.
- [x] Update the surrounding doc comments (`Issue #587` / "owns a fresh Tokio runtime" notes) to state
      the O(M) bound and the current_thread rationale, referencing #2316.
- [x] Confirmed the regression test from step 1 now PASSES (delta 9–10 ≤ bound 15).

## 3. Land the producer-thread gauge (coordinates with #2313 WS2)
- [x] Add the gauge to the observability catalog + otel instrument registry:
      `cqlite.merge.producer_threads`, unit `{thread}`. Live count incremented at producer spawn
      (`SSTableRowIteratorAdapter::open`) and decremented via `ProducerThreadGuard` at producer-thread
      exit; gauge re-recorded on each change.
- [x] Metric name `cqlite.merge.producer_threads` = the #2313 WS2-coordinated name from design.md
      (WS2 = the thread/blocking-pool metrics surface); recorded in catalog doc + this change.
- [x] Added a sibling gauge test `cqlite-core/tests/issue_2316_producer_gauge.rs` (under
      `observability-testing`) corroborating the bound via the gauge (reads `M` mid-merge with the
      `{thread}` unit, returns to baseline after drain), plus a catalog registration/unit unit-test.

## 4. Byte-parity + cancellation verification
- [x] Ran the compaction byte-parity tests over the present real corpus (issue_819 differential 7/7,
      issue_1020 UDT-frozen 3/3, issue_1021 repaired-metadata 6/6, issue_1234 frozen-UDT 3/3) — all
      green, merged output byte-identical (the builder swap does not touch merge/reconcile/write).
- [x] Ran the #2264 cancellation tests (`compaction_cancel_tests` 5/5) — prompt abandonment +
      `Cancelled`-vs-error distinction unchanged (the `ScanCancel` wiring is untouched).

## 5. Benches (#1494) — no wall-clock regression
- [ ] The #1494 merge bench suite has NOT landed yet. The change is a runtime-flavor swap with no
      change to the sequential scan/emit path, so no throughput regression is expected; a
      representative before/after timing to be recorded in the PR, noting the #1494 dependency.

## 6. Review + gate + close
- [ ] `scripts/agent-gate.sh --lite` green on each fix round (summary-file redirect).
- [ ] `rust-reviewer` + roborev (`/roborev-review-branch --base origin/main`) on the lite-green diff
      (review-first, before the full gate).
- [ ] Pre-roborev self-check: no `unwrap()`/`expect()` in library code; no wall-clock races in the
      thread-count test (capture the full merge window); no no-heuristics violation (thread count is a
      direct `/proc` observation).
- [ ] Open the PR; record the full `AGENT-GATE SUMMARY` + the roborev outcome.
- [ ] **C intent audit** (`spec-auditor` anchored to
      `openspec/changes/flight-merge-runtime-amplification/specs/**`) at closer time — this is a
      design-driven merge-architecture change, so it SHALL get a C audit: every requirement
      `satisfied` with a public-surface test as evidence before merge.
- [ ] Full `scripts/agent-gate.sh` PASS + C PASS + roborev clean → merge → `openspec archive`.

## Non-goals (do not do here)
- No task-based (candidate c) rearchitecture of the merge to O(1) threads.
- No change to merge reconciliation / tombstone / GC semantics.
- No streaming-channel-capacity or heap tuning.
