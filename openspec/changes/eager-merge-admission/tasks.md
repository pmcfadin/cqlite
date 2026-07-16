# Tasks — eager multi-generation merge admission

## 1. Wire admission into the eager merge path
- [ ] 1.1 In `cqlite-core/src/storage/sstable/generation_merge.rs`, acquire
      `scan_admission::admit().await` at the top of `merge_generations_for_read` (before the
      `spawn_blocking` at ~line 255), binding the RAII `ScanAdmissionPermit` (`let _admission = …`) so
      it is held across the join `.await` and released on every exit path. Reach the module via
      `super::reader::scan_stream_windowed::scan_admission::admit()`.
      *Surface exercised:* `merge_generations_for_read` (the eager branch at `mod.rs:1135`, `1646`).
- [ ] 1.2 Apply the identical acquire at the top of `merge_generations_for_read_with_metadata`
      (~line 319, before its `spawn_blocking`).
      *Surface exercised:* the metadata eager branch at `mod.rs:1482`, `1853`.
- [ ] 1.3 Confirm no nested `admit()` is introduced (the KWayMerger producer threads must not acquire
      permits) and no `unwrap`/`expect` added to library code.

## 2. Update the scope documentation
- [ ] 2.1 Rewrite the `# Scope` doc comment in `scan_admission.rs:51-58`: state the eager path is now
      admitted, remove the out-of-scope claim, fix the file reference to `generation_merge.rs`, and
      document the known limitation (shared bound = eager *operation* concurrency, not the eager path's
      per-op producer-thread footprint).

## 3. Regression guard (admission bound + deadlock-freedom for the eager path)
- [ ] 3.1 Add `cqlite-core/tests/issue_2063_eager_merge_admission_bound.rs`, `scan-offload-probe`-gated,
      `#[serial]` (process-global probe atomics). Drive `N > LIMIT` concurrent reads against a
      multi-generation fixture WITH a schema present (so the eager `merge_generations_for_read` branch
      is taken — verify the branch, contrast the #1594 test's `schema=None` lazy-forcing).
      *Surface exercised:* eager merge admission via the probe `max_in_flight`/`current_in_flight`.
- [ ] 3.2 Assert `max_in_flight <= LIMIT` (bound covers eager), `max_in_flight >= 1` (acquire wired,
      non-vacuous), `current_in_flight == 0` after (RAII release). Level snapshots, never wall-clock.
- [ ] 3.3 Add the deadlock-freedom flavor: `N > CAP` concurrent eager scans all complete within a
      bounded timeout AND total rows `> 0` AND `max_in_flight <= CAP`.
- [ ] 3.4 Check whether the existing `issue_1594_scan_admission_bound.rs` fixture now also hits the
      eager branch once wired; ensure the new guard adds distinct (multi-gen + eager) coverage, not a
      duplicate.

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
