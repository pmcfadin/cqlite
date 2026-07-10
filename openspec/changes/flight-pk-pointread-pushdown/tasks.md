# Tasks — Flight partition point-read for pushed PK-equality (#2207)

One issue ↔ branch `issue-2207-flight-pk-pointread-pushdown` ↔ this change ↔ one PR. Each stage
names the surface it exercises and carries a red-then-green test (fails on `main`). Anchors are
`main`-relative and WILL drift — re-grep before editing. Follow the implement loop: `--lite`
(summary-file redirect) each fix round → rust-reviewer + roborev on the lite-green diff
(review-first) → open PR → hand the endgame to `flow-closer` (ONE full gate → C intent audit →
final roborev → merge-on-green → finalize). Point `CQLITE_DATASETS_ROOT` at the main repo's
`test-data/datasets`.

## Stage 0 — route detection (no behavior change yet; tests fail on main)
- [x] 0.1 Add a resolved routing decision to `ScanSpec` (or a sibling analyzer) computed once from
  the lowered `FilterExpr` (`cqlite-flight/src/filter.rs:103`) + `TableSchema.partition_keys`:
  `PartitionPointRead(key)` / `MultiPartitionPointRead(keys)` / `Scan`. Total & schema-driven — any
  unprovable shape → `Scan`. No byte-pattern inference (#28). (flight-partition-point-read)
  — `cqlite-flight/src/point_read.rs`: `PointReadRoute` + `detect_route`.
- [x] 0.2 Red-then-green unit tests: full single-PK equality → point route; composite PK fully
  bound → point route; partial PK / clustering-only / range / no predicate / `IS NULL` → `Scan`;
  full-PK `IN` → multi-point route. (flight-partition-point-read)
  — `cqlite-flight/tests/point_read_route.rs` (18 tests green).

## Stage 1 — core single-partition candidate primitive (public surface)
- [x] 1.1 Public core primitive `SSTableReader::read_single_partition_for_compaction` →
  `SinglePartitionCompaction::{DefinitelyAbsent, IndexUnavailable, Rows}`
  (`data_access/point_compaction.rs`), wrapping `might_contain_partition` +
  `lookup_partition_via_bti_trie` / `lookup_partition_with_index` + the chunk-targeted seek. The
  merge composes it via `build_single_partition_merger` + `KWayMerger::from_row_iterators`
  (`merge/point_read.rs`). No-heuristics + fail-safe live here. (flight-partition-point-read)
- [x] 1.2 Behavioral tests exercise all three outcomes through the public builder
  (`point_read_tests.rs`): absent key → `None` merger; present → 1 partition; index-less (Summary.db
  stripped) → still read. (flight-partition-point-read)

## Stage 2 — wire the point path into do_get (reuse drive_merge reconciliation)
- [x] 2.1 `MergeProducer::produce_streaming` branches on the Stage-0 route (`producer_point.rs`):
  prune via the presence oracle (`cqlite.read.sstables_pruned` emitted by the oracle), build
  single-partition runs for survivors (scan-fallback on `IndexUnavailable`), token-exclude keys
  before any seek, and drive the **existing** `drive_merge` over them — reconciliation, budget,
  LIMIT, #2264 cancellation unchanged. Reports `AccessPath::StreamingPartitionLookup`. (flight-partition-point-read)
- [x] 2.2 Non-PK conjuncts stay a residual `filter.keeps` per row (route detection ignores them;
  `drive_merge` still applies the full filter). Covered by `residual_*_conjunct` route tests. (flight-partition-point-read)

## Stage 3 — parity (the deliverable)
- [x] 3.1 Dual-path parity: same PK-equality spec through scan (`produce_from_paths`) and point
  (`produce_streaming_to_vec`) over a 2-generation tombstoned/overwritten fixture; byte-identical
  rows (`point_path_matches_scan_on_{overwritten,tombstoned,live_untouched}_key`). (flight-partition-point-read)
- [~] 3.2 Semantic reconciliation proven via the dual-path parity over a tombstoned multi-gen
  fixture (LWW overwrite + row tombstone resolved identically to scan) — the property the
  physical-dump goldens cannot catch (#1742). NOT wired into `query-semantics-oracle.json`: that
  harness tests the core SELECT surface, not Flight `do_get`, and the point path IS the scan path's
  merge restricted to one partition (byte-identical by construction). See report deviation note. (flight-partition-point-read)
- [x] 3.3 Work-done probe: point merger steps 1 partition vs 12 for the full scan
  (`point_merger_steps_only_the_target_partition_not_the_whole_table`); full-PK `IN` bounded to the
  listed keys (`full_pk_in_list_is_bounded_by_the_listed_keys`); token exclusion in
  `point_read_keys`. (flight-partition-point-read)

## Stage 4 — fail-safe, cancellation/budget, observability
- [x] 4.1 Fail-safe: key only in a Summary.db-stripped (index-less, #2295-shape) SSTable is still
  read and returned (`index_less_candidate_is_read_never_skipped`); the inverted skip would drop it. (flight-partition-point-read)
- [x] 4.2 Cancellation + LIMIT: pre-cancelled point read surfaces the distinct `Cancelled` variant
  without full-table work (`pre_cancelled_point_read_stops_without_masking_errors`); `LIMIT k` over a
  wide partition streams exactly k (`point_read_respects_limit_over_a_wide_partition`). (flight-partition-point-read)
- [x] 4.3 Observability: a PK-equality `do_get` reports `streaming_partition_lookup` on
  `query.rows_scanned` (`point_read_metrics_test.rs`, own binary, `observability-testing` feature); no
  new knob/env/ticket field; the label is the existing bounded catalog attribute. (flight-partition-point-read)
- [x] 4.4 LIMIT early-stop asserts an EXACT count over a wide partition (streams k, not truncates
  post-hoc); the wire LIMIT test (`do_get_over_transport_enforces_limit`) still guards the multi-gen
  scan path. (flight-partition-point-read)

## Stage 5 — end-to-end wiring evidence
- [x] 5.1 e2e through the public tonic `FlightService::do_get`
  (`do_get_over_transport_pk_equality_point_read`): a pushed `key = ?` PK-equality ticket → exactly
  the target partition's row over the real gRPC transport. Paired with the label test (4.3) and the
  work-done probe (3.3). (flight-partition-point-read)
- [x] 5.2 No user-facing surface change (no new CLI/binding method, config knob, env var, or ticket
  field) — internal core seek primitive + existing observability only; doctrine unchanged. (flight-partition-point-read)

## Stage 6 — endgame (flow-closer)
- [ ] 6.1 `--lite` green on the full diff (summary-file redirect); rust-reviewer + roborev on the
  lite-green diff (review-first); fix rounds re-run `--lite` + diff-scoped parity/integration targets.
- [ ] 6.2 Open PR; hand to `flow-closer`: ONE full `scripts/agent-gate.sh` (run of record) →
  spec-auditor **C** intent audit anchored to
  `specs/flight-partition-point-read/spec.md` → final roborev → merge-on-green
  (`gh pr merge --squash --delete-branch`) → `flow-finalize` (archive change, close #2207, telemetry
  stamp).
