# Tasks — Flight partition point-read for pushed PK-equality (#2207)

One issue ↔ branch `issue-2207-flight-pk-pointread-pushdown` ↔ this change ↔ one PR. Each stage
names the surface it exercises and carries a red-then-green test (fails on `main`). Anchors are
`main`-relative and WILL drift — re-grep before editing. Follow the implement loop: `--lite`
(summary-file redirect) each fix round → rust-reviewer + roborev on the lite-green diff
(review-first) → open PR → hand the endgame to `flow-closer` (ONE full gate → C intent audit →
final roborev → merge-on-green → finalize). Point `CQLITE_DATASETS_ROOT` at the main repo's
`test-data/datasets`.

## Stage 0 — route detection (no behavior change yet; tests fail on main)
- [ ] 0.1 Add a resolved routing decision to `ScanSpec` (or a sibling analyzer) computed once from
  the lowered `FilterExpr` (`cqlite-flight/src/filter.rs:103`) + `TableSchema.partition_keys`:
  `PartitionPointRead(key)` / `MultiPartitionPointRead(keys)` / `Scan`. Total & schema-driven — any
  unprovable shape → `Scan`. No byte-pattern inference (#28). (flight-partition-point-read)
- [ ] 0.2 Red-then-green unit tests: full single-PK equality → point route; composite PK fully
  bound → point route; partial PK / clustering-only / range / no predicate / `IS NULL` → `Scan`;
  full-PK `IN` → multi-point route. (flight-partition-point-read)

## Stage 1 — core single-partition candidate primitive (public surface)
- [ ] 1.1 Add the public core primitive (recommended: a `SinglePartitionSource` that `KWayMerger`
  accepts alongside `DirSource`, wrapping `might_contain_partition`
  (`partition_lookup.rs:416`) + `lookup_partition_via_bti_trie` (`:136`) /
  `lookup_partition_with_index` (`:25`)): given a reader + partition key it returns
  `DefinitelyAbsent` (prune), a single-partition `PartitionStepper` (seek hit), or
  `IndexUnavailable` (scan-fallback signal). No-heuristics + fail-safe live here. (flight-partition-point-read)
- [ ] 1.2 Red-then-green core tests over BIG (`nb`) and BTI (`da`) fixtures: bloom-negative →
  `DefinitelyAbsent`; present key → stepper yields exactly that partition's fragments; index-less
  input → `IndexUnavailable` (never a wrong/empty seek). (flight-partition-point-read)

## Stage 2 — wire the point path into do_get (reuse drive_merge reconciliation)
- [ ] 2.1 In `MergeProducer::produce_streaming` (`producer.rs:582`) branch on the Stage-0 route:
  for a point read, prune via the presence oracle (increment `cqlite.read.sstables_pruned`, #2163),
  build single-partition steppers for surviving candidates (fall back to the SSTable's scan on
  `IndexUnavailable`), apply the token guard (`producer.rs:763`), and drive the **existing**
  `drive_merge` loop over those steppers — reconciliation, budget, LIMIT, `#2264` cancellation
  unchanged. Report `AccessPath::StreamingPartitionLookup` (replaces the hard-coded `FullScan` at
  `producer.rs:736`) on the point path. (flight-partition-point-read)
- [ ] 2.2 Retain non-PK conjuncts on the point path (apply the residual `filter.keeps` per row so
  `pk = ? AND col = ?` still narrows). (flight-partition-point-read)

## Stage 3 — parity (the deliverable)
- [ ] 3.1 Dual-path parity harness: same PK-equality ticket through scan (route forced off) and
  point path over a real multi-SSTable, multi-generation, tombstoned fixture; assert byte-identical
  batch streams. (flight-partition-point-read)
- [ ] 3.2 Query-semantics-oracle test: point-read result for a shadowed/tombstoned key matches
  `test-data/query-semantics-oracle.json` at the pinned `now`. (flight-partition-point-read)
- [ ] 3.3 Work-done probe (`CountingStepper`-style): partitions examined ≈ candidate lookups, NOT
  the table's partition count — fails on `main`. Includes the full-PK `IN` bounded-lookup case and
  the token-range interplay (point read within a split's token range only). (flight-partition-point-read)

## Stage 4 — fail-safe, cancellation/budget, observability
- [ ] 4.1 Fail-safe test: key only in a Data.db-only (index-less, #2295-shape) SSTable is still
  read and returned; a "skip on missing index" variant MUST fail. (flight-partition-point-read)
- [ ] 4.2 Cancellation + budget tests: pre-cancelled point read stops without full-table work and
  does not mask a real I/O error; `LIMIT k` over a wide partition streams ≤ k and respects the
  result-byte budget. (flight-partition-point-read)
- [ ] 4.3 Observability test: point path reports `streaming_partition_lookup`, scan path reports a
  full-scan label (`main` reports `full_scan` for the PK query — fails before the change); assert no
  new config knob / env var / ticket field and only bounded attributes. (flight-partition-point-read)
- [ ] 4.4 Test-strength hardening (from #2157): add a step-count assertion to the LIMIT early-stop
  tests so a regression to post-hoc stream truncation fails. (flight-partition-point-read)

## Stage 5 — end-to-end wiring evidence
- [ ] 5.1 e2e test through the public Flight `do_get` surface: real PK-equality ticket → correct
  reconciled rows + `streaming_partition_lookup` signal + work-done probe. Helper-only unit tests do
  NOT satisfy this. (flight-partition-point-read)
- [ ] 5.2 If any behavior is user-facing, update CLAUDE.md + the `agents-developing/` note in the
  same change (keep doctrine current).

## Stage 6 — endgame (flow-closer)
- [ ] 6.1 `--lite` green on the full diff (summary-file redirect); rust-reviewer + roborev on the
  lite-green diff (review-first); fix rounds re-run `--lite` + diff-scoped parity/integration targets.
- [ ] 6.2 Open PR; hand to `flow-closer`: ONE full `scripts/agent-gate.sh` (run of record) →
  spec-auditor **C** intent audit anchored to
  `specs/flight-partition-point-read/spec.md` → final roborev → merge-on-green
  (`gh pr merge --squash --delete-branch`) → `flow-finalize` (archive change, close #2207, telemetry
  stamp).
