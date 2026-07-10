# Tasks — flight-merge-runtime-amplification

## 1. Pin the defect with a failing regression test (TDD first)
- [ ] Add a regression test that drives a **real** multi-SSTable merge (real SSTable inputs;
      `CQLITE_DATASETS_ROOT` corpus — never an empty dataset) and observes the process's peak OS
      thread count via `/proc/self/task` entry count.
      **Surface:** `KwayMerge::new_with_gc_and_registry_cancellable`
      (`cqlite-core/src/storage/write_engine/merge/mod.rs`) driven end-to-end; new test file under
      `cqlite-core/tests/`.
- [ ] Assert the peak thread delta over baseline is within `O(M)` (`M + small_constant`); guard on
      `num_cpus >= 2` so it FAILS on the pre-change code (multi-threaded per-producer runtime) and is
      deterministic on single-core hosts. Confirm it FAILS on `main` before implementing the fix.

## 2. Replace the per-producer multi-core runtime (recommended design (b))
- [ ] In `producer_thread` (`.../write_engine/merge/mod.rs` ~line 519) replace
      `tokio::runtime::Runtime::new()` with
      `tokio::runtime::Builder::new_current_thread().enable_all().build()` (zero extra worker
      threads).
      **Surface:** `SSTableRowIteratorAdapter::producer_thread`; the scan, emit callback,
      `SyncSender` backpressure, k-way heap, and `ScanCancel` wiring are untouched.
- [ ] Update the surrounding doc comments (the `Issue #587` / "owns a fresh Tokio runtime" notes at
      ~lines 374/454/493) to state the O(M) bound and the current_thread rationale, referencing
      #2316.
- [ ] Confirm the regression test from step 1 now PASSES.

## 3. Land the producer-thread gauge (coordinates with #2313 WS2)
- [ ] Add the gauge to the observability catalog.
      **Surface:** `cqlite-core/src/observability/catalog.rs` (proposed
      `cqlite.merge.producer_threads`, unit `{thread}`) + the counter increment/decrement at producer
      spawn (`SSTableRowIteratorAdapter::open`) and join/drop.
- [ ] Confirm the metric name with epic #2313 WS2 before finalizing (naming-collision guard); note
      the agreed name in the change.
- [ ] Extend the regression test (or add a sibling) to corroborate the bound via the gauge, not only
      `/proc/self/task`.

## 4. Byte-parity + cancellation verification
- [ ] Run the compaction-byte-parity harness + sstabledump JSONL golden comparison over the present
      real corpus; confirm merged output is byte-identical to pre-change.
      **Surface:** existing parity harness / `test-validator`.
- [ ] Exercise the #2264 cancellation path (Flight `do_get` drop mid-merge over an index-less input)
      and confirm prompt abandonment + `Cancelled`-vs-error distinction unchanged.

## 5. Benches (#1494) — no wall-clock regression
- [ ] If the #1494 merge bench suite has landed, run it and confirm no wall-clock regression on merge
      throughput. If not yet landed, record a manual before/after timing of a representative
      multi-SSTable merge in the PR and note the #1494 dependency.

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
