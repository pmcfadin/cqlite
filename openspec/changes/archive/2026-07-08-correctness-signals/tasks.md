# Tasks — correctness-signals (#2163)

> Implementer note: every counter goes through `observability::add_counter`, which is a
> no-op with no OTel linkage when the `observability` feature is off. Aggregate per-row
> work into stack-local integers and emit ONCE per merge / query / prune — never per row or
> cell. Tests assert via the `observability-testing` `InMemoryMetricExporter` driven by a
> real read/merge through a public surface (wiring evidence).

## 1. Catalog + instrument plumbing
- [x] Add constants `MERGE_ROWS_IN`, `MERGE_ROWS_OUT`, `COMPACTION_TOMBSTONES_SUPPRESSED`,
      `COMPACTION_TOMBSTONES_EMITTED`, `READ_SSTABLES_PRUNED`, `READ_BLOOM_FALSE_NEGATIVES`,
      `QUERY_DEGRADED_PATH` and attr key `FALLBACK_REASON` to
      `cqlite-core/src/observability/catalog.rs`; extend `ALL_METRICS` and the attr list.
- [x] Register each new counter as an `Instruments` field with its unit in
      `observability/otel.rs` and add its `add_counter` dispatch arm.
- [x] **Surface/test**: catalog unit tests `metric_names_are_namespaced_and_unique` +
      `attribute_keys_are_namespaced` cover the additions (Requirement: Catalog integrity).

## 2. Merge row-count reconciliation (Requirement: Merge row-count reconciliation counters)
- [x] Aggregate rows-in / rows-out into the merge's per-merge tally alongside `PurgeCounts`
      (`storage/write_engine/merge/mod.rs`), scoped to the reconcile boundary; emit both
      once per merge next to the existing `tombstones_purged` emission.
- [x] **Surface/test**: compaction over two overlapping generations asserting
      `cqlite.merge.rows_in`/`rows_out` equal input/output row counts and their delta equals
      reconciliation drops, via the in-memory exporter (spec scenario's "compaction OR Flight
      do_get" — compaction chosen; both drive the SAME `KWayMerger` instrumentation site, so
      the Flight `do_get` variant is covered by the same wiring and left as optional).

## 3. Tombstone suppression / emission (Requirement: Tombstone suppression-vs-emission)
- [x] Tally shadowed live cells/rows (suppressed) and retained tombstone markers (emitted) in
      the same reconcile struct (`merge/reconcile.rs` + `merge/mod.rs`); emit once per merge.
      Leave `tombstones_purged` untouched.
- [x] **Surface/test**: a merge with (a) a row-tombstone shadowing an older live cell and
      (b) a retained non-purgeable marker, asserting `tombstones_suppressed` and
      `tombstones_emitted` move independently of `tombstones_purged`.

## 4. SSTable-pruned counter (Requirement: SSTable-pruned-by-presence-oracle counter)
- [x] Increment `cqlite.read.sstables_pruned{format}` at the presence-oracle-negative /
      candidate-prune site in the reader (`storage/sstable/reader/partition_lookup.rs` +
      candidate selection), once per skipped SSTable.
- [x] **Surface/test**: a partition point read through the public read surface over a
      multi-SSTable table where the key is absent from some SSTables; assert the counter
      increments per skipped SSTable with the right `cqlite.sstable.format`.

## 5. Opt-in false-negative verification (Requirement: Opt-in presence-oracle false-negative)
- [x] Add the default-off runtime switch (`ObservabilityConfig` field +
      `CQLITE_VERIFY_PRESENCE_ORACLE`); when on, run an authoritative confirmation scan on a
      presence-oracle miss and increment `cqlite.read.bloom.false_negatives{format}` only on
      a contradiction. Authoritative scan only — no byte-pattern inference.
- [x] **Surface/test**: (a) default-off read performs no confirmation scan and never emits;
      (b) switch-on true-negative read keeps the counter at 0; (c) a fault-injected/synthetic
      false-negative oracle increments it by 1 with the SSTable's format.

## 6. Degraded read-path counter (Requirement: Degraded read-path counter with bounded reason)
- [x] Emit `cqlite.query.degraded_path.total{fallback_reason}` from
      `query/access_path::record(FallbackFullScan{reason})` (or sibling) using
      `FallbackReason::label()`; fire at the existing honest-fallback decision sites in
      `query/select_executor/execute.rs` + `lookup.rs`.
- [x] **Surface/test**: a SELECT through the public query surface that triggers a known
      fallback (e.g. `NoSchema`) increments the counter with the matching reason; a targeted
      query does not; the reason value is always a bounded `label()`.

## 7. Docs + doctrine (same change)
- [x] Add the new metric names + the opt-in verify switch to the observability catalog
      reference in `docs/` and the website `agents-developing/` observability material.

## 8. Quality gates (delivery pipeline)
- [x] `--lite` gate (summary-file redirect) each fix round + diff-scoped targets.
- [ ] Review-first: `rust-reviewer` + roborev on the lite-green diff BEFORE any full gate.
- [ ] Fix roborev **blockers** pre-merge (each re-triggers fix → `--lite` + relevant
      integration/parity target → re-review); batch nits into one follow-up issue.
- [ ] Endgame via `flow-closer`: ONE full `scripts/agent-gate.sh` (AGENT-GATE SUMMARY) →
      **C** intent audit (`spec-auditor` anchored to
      `openspec/changes/correctness-signals/specs/**`) → final roborev → merge-on-green →
      `flow-finalize` (archive the change, close #2163, telemetry stamp).
- [ ] Done = gate PASS + **C PASS** (every requirement `satisfied` with public-surface test
      evidence) + roborev clean.
