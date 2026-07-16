# Tasks — eager multi-generation merge admission

## 1. Wire admission into ALL THREE eager merge helpers
- [x] 1.1 In `cqlite-core/src/storage/sstable/generation_merge.rs`, acquire
      `scan_admission::admit().await` at the top of `merge_generations_for_read` (before the
      `spawn_blocking`) and MOVE the `OwnedSemaphorePermit` INTO the `spawn_blocking` closure so it is
      held until the detached blocking work terminates (cancellation-safe). Reach the module via
      `super::reader::scan_stream_windowed::scan_admission::admit()`.
      *Surface exercised:* `merge_generations_for_read` (the eager branch at `mod.rs:1135`, `1646`).
- [x] 1.2 Apply the identical acquire (permit moved into the closure) at the top of
      `seek_merge_generations_for_read`, the partition-seeking point-read merge. Verify its sole call
      site (`scan_partition_clustering`, `mod.rs:1650`) is a top-level manager operation holding no
      outer permit, so admission introduces no cross-path hold-and-wait.
      *Surface exercised:* the seeking merge branch at `mod.rs:1650`.
- [x] 1.3 Apply the acquire at the top of `merge_generations_for_read_with_metadata`. This helper's
      permit must span the async per-reader `scan_with_cell_metadata` loop OUTSIDE `spawn_blocking`, so
      it stays an OUTER future guard; document the consequently weaker cancellation residual honestly.
      *Surface exercised:* the metadata eager branch at `mod.rs:1482`, `1854`.
- [x] 1.4 Confirm no nested `admit()` is introduced (the KWayMerger producer threads must not acquire
      permits) and no `unwrap`/`expect` added to library code.

## 2. Update the scope documentation
- [x] 2.1 Rewrite the `# Scope` doc comment in `scan_admission.rs`: state the eager path is now
      admitted, NAME all three helpers, remove the out-of-scope claim, fix the file reference to
      `generation_merge.rs`, and document the known limitations (shared bound = eager *operation*
      concurrency, not the per-op producer-thread footprint; metadata helper's weaker cancellation).

## 3. Regression guard (admission bound + deadlock-freedom for the eager path)
- [x] 3.1 Add `cqlite-core/tests/issue_2063_eager_merge_admission_bound.rs`, `scan-offload-probe`-gated,
      `#[serial]` (process-global probe atomics). Drive `N > LIMIT` concurrent reads against a
      multi-generation fixture WITH a schema present (so the eager `merge_generations_for_read` branch
      is taken — verify the branch, contrast the #1594 test's `schema=None` lazy-forcing).
      *Surface exercised:* eager merge admission via the probe `max_in_flight`/`current_in_flight`.
- [x] 3.2 Assert `max_in_flight <= LIMIT` (bound covers eager), `max_in_flight >= 1` (acquire wired,
      non-vacuous), `current_in_flight == 0` after (RAII release). Level snapshots, never wall-clock.
- [x] 3.3 Add the deadlock-freedom flavor: `N > CAP` concurrent eager scans all complete within a
      bounded timeout AND total rows `> 0` AND `max_in_flight <= CAP`.
- [x] 3.4 Add an end-to-end metadata-sibling case driving `scan_with_cell_metadata` (WRITETIME/TTL) on
      the multi-gen + schema fixture (bound + wired + release), plus a seek-helper case driving a
      multi-candidate point read (`scan_partition`) asserting the bound + release.
- [x] 3.5 Wire the new test into the `scan-offload-guard` gate component
      (`--test issue_2063_eager_merge_admission_bound`) so it executes in the canonical gate.

## 4. Verify (gate + C + roborev)
- [ ] 4.1 `--lite` green each fix round (summary-file redirect).
- [ ] 4.2 Review-first on the lite-green diff: `rust-reviewer` + roborev
      (`--agent codex --model gpt-5.6-sol`). Focus: deadlock-freedom of the shared-semaphore acquire,
      RAII release on error/cancel, non-vacuous guard.
- [ ] 4.3 ONE full `agent-gate.sh` of record (via flow-closer) — the new `scan-offload-probe` guard
      must PASS in its lane; confirm no bound/deadlock regression.
- [ ] 4.4 C (spec-auditor) anchored to `openspec/changes/eager-merge-admission/specs/**`: every
      requirement `satisfied` with a public-surface test as evidence.
- [ ] 4.5 Merge on green (gate PASS + C PASS + roborev clean + required CI green), then archive.
