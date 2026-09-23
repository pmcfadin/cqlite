# tombstone-report — issue #4200, slice 1 of 2 (epic #4192)

**Milestone:** unmilestoned (owner convention; board `Backlog` pending #4193, see Scheduling below).
**Priority:** P2. **Routing:** design-driven (OpenSpec + Seam 1) — a new read-only verb whose output
shape, aggregation granularity and per-generation attribution have real latitude, built entirely on
top of #4193's reconciliation trail.

## Scheduling note (read before approving)

Issue #4200 depends on #4193 (F0 decision trail + `cqlite explain`, epic #4192's own dependency
note). At spec time: #4193 is Seam-1 approved (`status:spec-review` + `resume-dont-ask`) and
**implementing on branch `issue-4193-forensics-explain`, not yet merged to `origin/main`**. This
worktree branches from `origin/main` and therefore does not yet contain `merge/trace.rs`
(`TraceSink`/`RecordingSink`/`CellDecision`/`TombstoneRecord`) or `explain`'s CLI scaffolding. This
spec is written against #4193's committed design (`openspec/changes/forensics-explain/design.md` on
that branch) so review and Seam-1 approval can proceed in parallel, but `tasks.md` Group 0 makes
landing #4193 first an explicit, re-checked premise — **implementation cannot start until #4193
merges and this branch rebases onto it.**

## Why

Epic #4192, FIX slice: operators need to know how much droppable tombstone weight a table is
carrying and, for range tombstones specifically, how many live cells a given range currently shadows
— the density/coverage half of "tombstone surgery." The companion write verb, `purge`, is scoped out
of this change (see Non-goals) because it is materially riskier (a write verb whose failure mode is
silent data resurrection) and its safety contract is a product decision, not an implementation
detail — see "Open product decisions" below and in the lead's report.

CQLite already has every primitive slice 1 needs, all from #4193: the closed verdict vocabulary
(`winner` / `shadowed-by-timestamp` / `shadowed-by-tombstone{kind}` / `expired` / `purgeable` /
`dropped-column`), the `RecordingSink` that captures one `CellDecision`/`TombstoneRecord` per
decision at zero cost on the production path (`NoTrace` monomorphization), and
`compute_gc_before(schema, now)` for the purgeable annotation. Slice 1 adds nothing new to the
reconciliation engine — it drives the SAME per-partition merger #4193's `explain` drives, once per
partition across a whole table, and aggregates the trail instead of rendering it per-partition.

## What changes (slice 1 — specced in this change)

**Library (`cqlite-core`).** A `tombstone_density` module under `storage/write_engine/` (or
sibling to `merge/trace.rs` — see design.md D1 for the exact placement question) exposing
`scan_tombstone_density(table_dir, schema, options) -> Result<DensityReport>`. It drives
`build_single_partition_merger` with a `RecordingSink` for **every partition** in the table (the
authoritative partition enumeration `salvage` already established: BIG `Index.db` entries / BTI
`Partitions.db` trie leaves — read-only here, no chunk pre-flight needed since this is a reporting
verb over presumed-healthy input, not a recovery tool), in FULL-compaction configuration
(`purge_safe(true)`, `gc_before_secs = compute_gc_before(schema, now)`, `now_secs = Some(now)`) so
`purgeable` is computed exactly as `explain`/a real compaction would. It aggregates: tombstone count
by kind × per-generation, droppable-at-`now` count, and for every RANGE tombstone the clustering span
plus the count of `CellDecision`s whose `decided_by` names that specific range marker as the reason
(the "how many live cells does this shadow" question).

**CLI (`cqlite-cli`).** `cqlite tombstones <keyspace.table | table-dir> [--schema <path>] [--now
<ts>] [--top N] [--out text|json]`, resolving the table exactly as `explain`/`query` do. Never opens
its input for writing (R-series below).

## What this change must establish

1. **Report counts equal counts derived by the test from the JSONL goldens** (kind, per generation)
   — never from CQLite's own prior behavior (#3042).
2. **Range-tombstone shadow counts equal the golden-derived live-cell count** inside the span with
   writetime ≤ deletion time, on `wide_range_tombstone` (`test_tomb`) and `rt_cross_gen`
   (`test_compaction_tombstone_ttl`).
3. **Trail agreement**: every count in the report is a straight aggregation of #4193's own verdict
   vocabulary, so `tombstones`' `purgeable` figure and `explain`'s per-partition `purgeable` verdicts
   never disagree for the same partition at the same `now` (both read `compute_gc_before` the same
   way).
4. **`--now` stated first**, same convention as `explain` R5.1; a report without a pinned `now` is
   wall-clock and says so.
5. **Read-only, always** — no `--out`/write path exists on this verb at all.

## Non-goals

- **`cqlite purge` (slice 2, NOT specced here).** Design-sketched in design.md D5 for the owner's
  context, but its requirements, scenarios and safety contract (dry-run posture, required flags,
  refusals) are deliberately left to a follow-up OpenSpec change once the owner rules on the open
  product decisions below — purging live-replica-shadowing data is a resurrection risk and the scope
  is a genuinely separate, larger, higher-stakes PR (its own writer wiring, byte-parity oracle vs
  `compact_sstables`, manifest contract, and CLI safety gates). Recommend filing it as a distinct
  child issue once slice 1 merges, rather than treating #4200 as closed by slice 1 alone.
- Editing data (report is read-only by construction; not merely by convention).
- A data-dir-wide sweep across tables (#4194's territory); this verb takes one table.
- Anything the `purgeable` annotation does today for `explain` R6 that this change does not
  independently re-derive — slice 1 reuses #4193's trail verbatim rather than re-implementing gc_grace
  math.

## Doctrine impact

- **No-heuristics (#28):** every count comes from the reconciliation trail's own decisions, driven by
  the SAME merger `compact_sstables`/`explain` use; no independent tombstone-counting heuristic.
- **Uncompressed-write claim boundary (#1406):** not engaged — slice 1 writes nothing. (Slice 2
  `purge` will need this note; see design.md D5.)
- **Cassandra 5.0 only:** `na`/`nb` BIG, `da` BTI; the trail's partition enumeration is the same one
  `salvage`/`explain` use, which is already format-gated.
- **Oracles:** every acceptance number is derived from Cassandra-written JSONL goldens
  (`test_tomb`, `test_compaction_tombstone_ttl`) or from `cassandra-5.0.8`'s own gc_grace/purge
  evaluator source (`CompactionController`/tombstone purge logic), never from CQLite's own prior
  output (#3041/#3042).

## Open product decisions for the owner (do not decide here)

1. **Purge's dry-run posture.** Should `cqlite purge` require a separate `--dry-run` invocation
   (manifest-only, no bytes) before a real run is accepted, or is `tombstones` itself the de facto
   dry-run (an operator runs `tombstones` to see the density/shadow picture, then `purge` for real),
   matching `salvage`'s report-then-recover shape rather than a stateful two-step gate?
2. **Purge's completeness assertion.** `compact --major` today treats "the input dir holds every
   overlapping SSTable" as the *operator's* unverified assertion. Should `purge` inherit that same
   unverified-assertion contract, or require an explicit flag naming that assertion out loud
   (`--i-have-every-generation`-style), given the epic's own framing of this as a resurrection risk?
3. **Purge's required-flag bar.** Is mandatory `--now` (already an issue acceptance criterion)
   sufficient friction, or should `purge` additionally require a distinct confirmation flag beyond
   `--now` before it writes?
4. **Slice split itself.** Confirm the report/purge split above, or direct that both ship in one
   (larger, ~5–6k line) change — sizing detail in design.md D6.
