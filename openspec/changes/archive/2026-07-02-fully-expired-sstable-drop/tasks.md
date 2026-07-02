# Tasks: fully-expired-sstable-drop

> Implementation tasks for the LATER implement phase. Each task names the public surface it exercises.
> Do NOT start these during the proposal/design phase. Owner OPEN QUESTIONS in `design.md` (OQ-1 CLI
> drop under `--major`; OQ-2 coarse vs key-range overlap) MUST be resolved before tasks 3–4 are
> finalized.

## 1. Metadata-only fully-expired detection

- [x] 1.1 Add `fn is_fully_expired(stats: &TimestampStatistics, gc_before_secs: i64) -> bool` (or a
      per-path variant reading `Statistics.db` via `stats_path_for` + `parse_statistics_with_fallback`)
      to the merge module. Predicate: `max_deletion_time < gc_before_secs`. Surface:
      `cqlite_core::storage::write_engine::merge` public/pub(crate) fn + a unit test that passes for an
      all-expired stats block and fails for a LIVE-sentinel / >= gcBefore block, asserting NO Data.db
      read occurs.

## 2. Overlap-safety gate + drop-set computation

- [x] 2.1 Add `fn fully_expired_sstables(input_paths, outside_paths, gc_before_secs: Option<i64>) ->
      Vec<PathBuf>` composing 1.1 with the overlap bound. Reuse `compute_max_purgeable_timestamp` for the
      outside `min_timestamp` bound; drop a candidate iff fully expired AND
      `candidate.max_timestamp < outside_bound` (with `+inf` for an empty outside set / full compaction,
      and NO drop when `gc_before_secs == None` or the bound is UNKNOWN in a partial compaction). Uses the
      authoritative #1729 `max_timestamp` and fails closed (retains) when it is unavailable (`i64::MIN`
      sentinel). Surface: `merge::fully_expired_sstables` + unit tests covering: shadowing-retained,
      older-than-outside-dropped, major-empty-outside-dropped, unknown-bound-retained,
      invalid-gcBefore-retained, min<bound<max-retained, unavailable-max-fails-closed.

## 3. Wire drop-set into the WriteEngine background path

- [x] 3.1 In `maintenance_step_inner` compute the drop-set from `selected` (inputs) + `non_included`
      (outside) + `gc_before_secs`, subtract it from `selected` before `start_merge`, and thread the
      dropped paths into `start_merge` so finalize deletes them after publish. Surface:
      `WriteEngine::maintenance_step` (via `start_merge`) + an integration test asserting a fully-expired
      candidate is excluded and reclaimed.

## 4. Wire drop-set into the CLI one-shot path (gated per OQ-1)

- [x] 4.1 Per OQ-1 resolution: in `compact_sstables_with_registry`, when `purge_safe == true` (`--major`),
      compute the drop-set with an empty outside set, subtract it from `input_paths` before building the
      `KWayMerger`, and delete the dropped files after `writer.finish()`. No drop when `purge_safe`
      is false. Surface: `merge::compact_sstables_with_registry` + `cqlite compact --major` integration
      test (acceptance-criterion 1).

## 5. Report/stats surface for the drop decision

- [x] 5.1 Add `dropped_whole: Vec<PathBuf>` (and count) to `MergeStats` / `CompactReport` and populate it
      from the drop-set on both surfaces. Surface: `CompactReport.stats.dropped_whole` +
      `CompactResult` (CLI) fields + tests asserting the plan decision (acceptance-criterion 1's
      "assert via plan/stats, not just output") and an empty set when nothing is dropped.

## 6. Read-parity + regression tests

- [x] 6.1 Add an integration test: build live + fully-expired SSTables, run the query before and after a
      drop-whole compaction, assert identical result sets per partition/generation (acceptance-criterion
      3), and assert the drop-whole result equals a merged-purge result. Surface: query engine over the
      compaction output.
- [x] 6.2 Add an overlap-safety regression test: a fully-expired SSTable that shadows data in an EXCLUDED
      overlapping SSTable is NOT dropped and the shadowed data stays shadowed on read (acceptance-criterion
      2). Surface: `WriteEngine::maintenance_step` + query engine.
- [x] 6.3 Roborev High F1/F2 regression tests (prerequisites #1728/#1729 merged make the metadata
      authoritative). F1 (data-loss): a MIXED SSTable (old tombstone LDT < gcBefore + a live non-TTL cell)
      is NOT classified fully expired and is NOT dropped by `compact_sstables` — the live cell survives
      (`mixed_tombstone_and_live_sstable_not_dropped`). F2 (resurrection): the overlap gate compares the
      authoritative `max_timestamp` — a candidate with `min_timestamp < bound < max_timestamp` is RETAINED
      (`overlap_gate_uses_max_timestamp_not_min_retains_when_max_above_bound`), and an UNAVAILABLE
      (`i64::MIN`) max_timestamp fails closed / retains
      (`overlap_gate_fails_closed_on_unavailable_max_timestamp`). Surface: `compact_sstables` + the
      metadata classifier.

## 7. Documentation

- [x] 7.1 Add a short note to `docs/sstables-definitive-guide/chapters/15-compaction-strategies.md` that
      fully-expired SSTables are dropped whole (metadata-only detection + overlap gate). No doctrine change.

## 8. Gate + audit + review (pipeline)

- [x] 8.1 Run `scripts/agent-gate.sh` (fmt, clippy `-D warnings`, core/integration/write-support/CLI
      tests, minimal build, smoke) and paste the SUMMARY block. Run with `CQLITE_DATASETS_ROOT` pointed
      at the main repo's `test-data/datasets` (worktrees have no Data.db binaries).
- [ ] 8.2 Spec-auditor (C) review anchored to `openspec/changes/fully-expired-sstable-drop/specs/**`:
      every requirement `satisfied` with a public-surface test as evidence.
- [ ] 8.3 roborev review (`--branch --base origin/main --agent codex --wait`); clear all findings.
- [ ] 8.4 `openspec archive fully-expired-sstable-drop` after gate PASS + C PASS + roborev clean + merge.
