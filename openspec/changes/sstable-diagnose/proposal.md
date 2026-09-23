# sstable-diagnose — issue #4204 (epic #4192, DIAGNOSE)

**Milestone:** 0.18. **Priority:** P2. **Routing:** design-driven (OpenSpec + Seam 1) — `diagnose`
composes existing, authoritative metadata (Statistics.db's `EstimatedHistogram`s, min/max
timestamp/LDT, compression ratio, `fully_expired_sstables`'s classification) into a NEW report shape
with real latitude (what's cheap vs `--deep`, what's top-N, how token overlap is bucketed). Every
individual NUMBER is oracle-bound to Cassandra's own `StatsMetadata`/`EstimatedHistogram` formulas
and to `sstablemetadata`'s printed output, read directly at `cassandra-5.0.8` for this proposal
(never a CQLite `file:line`, #3041).

## Why

An operator with a damaged or slow table and no cluster has no way to see WHY a table is expensive
to compact or query, offline. Cassandra's own answer to this — `sstablemetadata` (per-file, fast +
`-s` scan mode) and `nodetool tablehistograms` (live-node, cross-sstable + read/write latency) — is
either per-file only or requires a running node. `diagnose` is CQLite's offline answer: everything
`sstablemetadata` can say about ONE file, extended to a whole TABLE (every generation), from files
alone. It is the DIAGNOSE-tier sibling of #4205 `find`.

## What changes

**Library (`cqlite-core`).** A `diagnose` module under `storage/sstable/` (read-only; no write
engine dependency), in new files (#1116), exposing `diagnose_table(table_dir, options) ->
Result<DiagnoseReport>`. Two cost tiers, matching `sstablemetadata`'s own `-s`/no-`-s` split
exactly:

- **Cheap (default): `Statistics.db` alone**, per generation — reusing CQLite's existing
  `StatisticsReader`/`SSTableStatistics` (already Cassandra-parity for the two `EstimatedHistogram`s,
  issue #1327) and `fully_expired.rs`'s `TimestampStatistics`. No component other than
  Statistics.db/Index.db/Summary.db/CompressionInfo.db/TOC.txt is opened; Data.db is never read.
- **`--deep`: one full streaming scan per table** (the existing point-compaction/full-scan iterator,
  NOT a new decode path), computing partition-size and clustering-width histograms, top-N largest/
  most-tombstoned partitions (mirrors `sstablemetadata -s`'s `widestPartitions`/`largestPartitions`/
  `mostTombstones` `MinMaxPriorityQueue`s byte-for-byte in shape), and a full-compaction-at-`--now`
  reclaim prediction reusing the existing `MergeStats`/purge tally — no write, `--dry-run`-shaped by
  construction (this verb never writes at all).

**CLI (`cqlite-cli`).** `cqlite diagnose <table-dir> [--now <ts>] [--deep] [--top N] [--out
text|json]`. Read-only; no `--out <dir>` (there is no output SSTable — `--out` here selects the
RENDERING format, not a write destination, an intentional naming divergence from every writing verb
in this epic, called out explicitly so it is never mistaken for one).

## Relationship to sibling DETECT/FIX verbs — where the line is, precisely

The team lead flagged two specific boundaries to hold; both are drawn from source read, not
guesswork.

### vs. #4200 `cqlite tombstones` (branch `issue-4200-tombstone-surgery`)

**Different oracle, not overlapping work.** #4200's design (`tombstone-report`, already drafted)
drives `merge::trace`'s `RecordingSink` through a FULL per-partition reconciliation across EVERY
generation (`build_single_partition_merger`, `purge_safe(true)`) to produce an AUTHORITATIVE,
kind-classified (`partition|range|row|cell|collection`), cross-generation, droppable-at-`now`,
shadow-counted tombstone report — the *query-semantics* oracle (#1742), because "is this tombstone
droppable and what does it shadow" is a reconciliation question. `diagnose --deep`'s tombstone
signal is a completely different, MUCH CHEAPER thing: `sstablemetadata -s`'s `mostTombstones`
counter is a **raw physical marker count per partition, from ONE generation's own scan, with no
reconciliation at all** (`cell.isTombstone()` / `RANGE_TOMBSTONE_MARKER` counted as the scanner
walks — see `design.md` D2) — the *physical-dump* oracle (#1742), for ranking ("which partitions are
tombstone-heavy") not for correctness. `diagnose` therefore:
- reports the CHEAP, non-deep `Estimated droppable tombstones` RATIO straight from
  `StatsMetadata.getEstimatedDroppableTombstoneRatio` (an ESTIMATE Cassandra itself ships, matching
  `sstablemetadata`'s own fast-path field, per generation);
- under `--deep`, ranks top-N tombstone-heaviest partitions by the CHEAP raw per-generation count
  (matching `sstablemetadata -s`'s "Tombstone Leaders");
- NEVER computes a kind breakdown, a droppable-at-now verdict per tombstone, or a shadow count —
  every field in that shape is `cqlite tombstones`'s job, and `diagnose`'s report text points there
  by name rather than approximating it.

### vs. #4195 `verify --mode audit` (branch `issue-4195-consistency-audit`)

**Reporting vs. validating — diagnose never cross-checks.** #4195's invariant 8 is exactly
"Statistics min/max timestamp, min/max LDT, estimated partition count consistent with a full scan" —
a CONSISTENCY check (does the metadata agree with reality), with a PASS/FAIL/SKIP verdict. `diagnose`
reads the SAME Statistics.db fields for its cheap tier but never asks whether they are trustworthy —
per no-heuristics doctrine, authoritative metadata is TRUSTED, not re-derived, by every consumer
that isn't itself an audit. `diagnose --deep`'s independently-computed histograms are not a
cross-check either: they report what the scan found, on their own terms, for operator triage — they
never compare against Statistics.db's declared values or emit a verdict. An operator who wants to
know "can I trust this table's Statistics.db" runs `verify --mode audit`; `diagnose` assumes yes,
same as `compact`/`salvage`/`scrub` already do.

## What this change must establish

1. Every number states its source (`statistics | index | scan`) — no metric renders as a bare `0`
   when its source is absent; matches the epic's #4159 "unmeasured, never 0" class exactly.
2. Cheap-tier numbers match `sstablemetadata`'s own printed fields, captured as a text oracle at the
   pinned `cassandra-5.0.8` tool version (`sstablemetadata`'s formulas, not CQLite's).
3. `--deep` histograms and top-N rankings match values computed independently by the test from the
   Cassandra-written JSONL goldens — never from CQLite's own scan output compared to itself (#3042).
4. `--deep` is bounded to one partition resident at a time (the existing streaming scan's memory
   contract, `test_wide_rows` under the memory-budget lane) — no write path at all, so no #1406
   claim-boundary question arises (this verb writes nothing, ever).

## Non-goals

- Fixing anything (`scrub` #4198, `tombstones`/`purge` #4200 own remedies).
- Per-cell explain / decision trail (#4193 — **not Seam-1 approved yet**; `diagnose` has no
  dependency on it and must not gain one implicitly through a shared module that only exists on
  #4193's branch).
- The authoritative, kind-classified, cross-generation tombstone report (#4200's job — see above).
- Cross-checking Statistics.db against a scan (#4195's job — see above).
- `nodetool tablehistograms`'s READ/WRITE LATENCY percentiles and cross-sstable live-reservoir
  merge: these come from a running node's query metrics, which do not exist offline, ever. `diagnose`
  matches `tablehistograms`' PARTITION-SIZE/CELL-COUNT histogram shape (both tools ultimately read
  the same `EstimatedHistogram`s) but has and can have no read/write-latency equivalent — stated
  explicitly in `--help` and the report, not silently absent.
- Writing an output SSTable of any kind; `--out` selects text/json rendering only.

## Doctrine impact

- **No-heuristics (#28):** every cheap-tier field is read, not inferred, from Statistics.db; `--deep`
  fields are computed by an actual scan, never estimated from byte patterns.
- **#4159 class (unmeasured, never 0):** a generation whose Statistics.db lacks a field this report
  wants (e.g. an older/legacy layout that didn't decode `max_timestamp`, issue #1653's `Option`
  fields) renders `unmeasured: <cause>`, never a silent `0` that reads as "empty/healthy."
- **Cassandra 5.0 only:** `na`+/`nb` BIG, `da` BTI — reuses the existing reader's version gates.
- **Oracles (#3042):** `sstablemetadata` text captures + independently-derived JSONL-golden
  histogram/ranking checks; no CQLite-round-trip-alone claim anywhere in this change.

## Size

Medium (~1.6–2.2k lines incl. tests) — one PR. **Depends on.** Nothing (epic's own listed
dependency: none). **Deliberately independent of.** #4193 (not approved), #4200, #4195 (both
in-flight specs on sibling branches) — `diagnose` reads only components every prior-merged verb
already reads (Statistics.db, Index/Summary, CompressionInfo, the existing full-scan iterator).
