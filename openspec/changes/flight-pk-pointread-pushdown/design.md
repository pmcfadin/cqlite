# Design — Flight partition point-read for pushed PK-equality (#2207)

## Context

Static analysis (anchors `main`-relative, WILL drift — re-grep before editing):

- `do_get` full-scans: `MergeProducer::produce_streaming` builds `KWayMerger::new_cancellable` over
  all token-pruned paths (`cqlite-flight/src/producer.rs:597`), `drive_merge` walks every partition
  and applies the pushed predicate per-row at `producer.rs:784` (`filter.keeps(&row)`). LIMIT and
  token filter are the only narrowing.
- Only prune today is token-span via `Summary.db` (`prune_paths_cancellable`, `producer.rs:618`).
- The pushed predicate is a typed `FilterExpr` tree (`And/Or/Not/Leaf/IsNull`,
  `cqlite-flight/src/filter.rs:103`), lowered once in `ScanSpec::from_ticket` (`filter.rs:220`)
  against the `TableSchema` (which knows `partition_keys`).
- Per-SSTable point-read primitives already exist in core, unwired from Flight:
  `might_contain_partition` (bloom, `partition_lookup.rs:416`), `lookup_partition_via_bti_trie`
  (BTI `da`, `partition_lookup.rs:136`), `lookup_partition_with_index` (Summary/Index `nb`,
  `partition_lookup.rs:25`).
- Generation-reconciling merge already exists: `storage.get` collects each generation's value and
  resolves via `TombstoneMerger::merge_generations` (`storage/sstable/mod.rs:1006-1028`) — **but it
  returns a single `ScanRow` per full key**, not every clustering row of a partition, so it is
  insufficient for a partition point-read (which must return all clustering rows for the pk).
- The observable seam is ready: core `AccessPath` (`query/access_path.rs:55`) already defines
  `PartitionLookup` / `StreamingPartitionLookup` / `FallbackFullScan { reason }`; the Flight
  producer currently hard-codes `AccessPath::FullScan.label()` at `producer.rs:736`. The
  `cqlite.read.sstables_pruned` counter exists (`catalog.rs:208`, #2163).

## Detecting the route

`ScanSpec` gains a resolved routing decision computed once from the lowered `FilterExpr` + the
schema's `partition_keys`:

- **PartitionPointRead(key)** iff the filter is (a conjunction of) equality leaves that together
  bind **every** partition-key component to exactly one value, with no other partition-key
  constraint. Non-PK conjuncts (clustering/regular equality, ranges) are retained and still applied
  per-row on the point path — they narrow, never widen.
- **MultiPartitionPointRead(keys)** iff the PK is bound by an `IN` list (or an `Or` of full-PK
  equalities) — treated as N bounded point reads.
- **Scan** (unchanged) for anything else, including a partial PK.

Detection is schema-driven and total: any shape the analyzer cannot prove is a full-PK equality
falls through to Scan. No byte-pattern guessing (#28).

## Candidate designs

### (a) Route entirely inside `producer.rs` at plan time
Branch in `produce_streaming`: when the route is a point read, the producer itself opens each
candidate SSTable, calls the core presence-oracle + seek primitives, and builds single-partition
iterators, then drives the existing `drive_merge`.
- **Pro:** no new core public surface; cancellation/budget/merge stay where they are.
- **Con:** pushes SSTable index/bloom/BTI seek orchestration — format-version-specific logic that
  belongs to core's storage layer — into the Flight crate. Weak wiring evidence (the point-read is
  a private producer branch, not a named, testable core surface). Duplicates seek sequencing that
  core already owns and would drift from it.

### (b) New end-to-end core API: `Database::point_read_partition`
Core exposes a fully-reconciled partition stream (`point_read_partition(table, pk) -> stream of
reconciled rows`); Flight calls it and just Arrow-encodes.
- **Pro:** strongest public-surface wiring; reusable by CLI/bindings later.
- **Con:** re-implements reconciliation that the Flight producer *already drives* over `KWayMerger`,
  and forces the token-range / budget / `#2264` cancellation / LIMIT semantics that live in the
  Flight `ScanSpec` to be re-plumbed through a second core path — two reconciliation code paths to
  keep byte-identical. Highest risk of scan-vs-point divergence, which is exactly the property the
  spec must guarantee.

### (c) Hybrid — core exposes the per-SSTable *single-partition candidate* primitive; Flight composes it into its existing merge  **(RECOMMENDED)**
Core adds ONE public, named surface: a **single-partition candidate stepper** — given an open
SSTable reader and a partition key, it (i) consults the presence oracle (`might_contain_partition`)
and returns *definitely-absent* so Flight can prune, or (ii) seeks via BTI trie (`da`) / Summary+Index
(`nb`) to that partition and yields **only that partition's fragments** as a `PartitionStepper`
(the same trait `KWayMerger` feeds today), or (iii) reports *index-unavailable* so Flight falls back
to scanning that SSTable. Flight's producer detects the route, prunes/seeks each candidate through
this primitive, and drives its **existing `drive_merge` reconciliation loop unchanged** over the
resulting single-partition steppers.

- **Pro — byte-identical by construction:** the point path feeds the *same* fragments into the
  *same* merge/reconciliation as the scan path, only fewer partitions. LWW/tombstone/multi-gen
  correctness is inherited, not re-derived. Cancellation (#2264), byte/row budgets, token filter,
  and LIMIT all stay in the one Flight producer path.
- **Pro — wiring evidence:** the new core primitive is a named public surface with its own call
  chain (`do_get` → detect route → core single-partition stepper → `drive_merge`) and is exercised
  end-to-end through the public Flight `do_get` ticket.
- **Pro — no-heuristics & fail-safe live in core:** presence/seek decisions use only bloom +
  index/trie + Statistics/schema metadata; "index unavailable / ambiguous" is an explicit
  return that Flight maps to scan-fallback, never a silent skip.
- **Beats (a)** by keeping format-version seek logic in core (single source of truth, testable
  surface) rather than smearing it into the Flight crate.
- **Beats (b)** by not duplicating reconciliation/budget/cancellation — one merge path, so
  scan-vs-point parity is structural rather than a thing two code paths must agree on.

**Recommendation: (c).** It is the minimal wiring that makes parity a property of construction.

## Fail-safe pruning (the correctness spine)

Pruning is **fail-open toward reading**: a candidate SSTable is skipped only when the presence
oracle says the key is *definitely absent* (bloom negative is exact). Any of {bloom positive,
bloom unavailable, Summary/Index/BTI absent or unreadable, ambiguous FQ-table resolution} →
the SSTable is a candidate and is **read** (seek if the index resolves, else full-scan that one
SSTable's partitions and filter). Never skip an SSTable that might contain the key. This is what
makes #2295 (snapshots sometimes ship Data.db only) safe: index-less input degrades to the scan
path for that SSTable, preserving correctness while losing the speedup for it.

## Reconciliation across candidates

Every candidate SSTable that is *not* pruned contributes its fragment of the partition to the
merge. `drive_merge` already reconciles across generations (the merge is what `TombstoneMerger`
backs). A key present in 3 generations with a tombstone in one is resolved exactly as the scan
path resolves it — because it *is* the scan path's merge, restricted to one partition per input.

## Modes, budgets, cancellation

- **Snapshot vs live:** unchanged. `DirSource::resolve` (`producer.rs:146`) still lists the
  per-request snapshot/live dir; the route decision and prune run on that resolved path set. No
  warm state (that is #2310).
- **Token range:** the point read stays *within* the split's token range — a partition whose token
  is outside `spec.token` is dropped before any seek (the point path applies the same token guard
  `drive_merge` applies at `producer.rs:763`).
- **#2264 cancellation:** the point path polls the cancel flag before each candidate seek and each
  `step`, mapping `Error::Cancelled` by variant (not by racing a flag) exactly as `drive_merge`
  does today (`producer.rs:755`).
- **Budgets:** the byte/row result budget and LIMIT are enforced by the same `drive_merge` sink —
  a wide-partition point read is still bounded (the multi-GB-partition bound itself is #2230, not
  regressed here).

## Observability

The producer reports `AccessPath::StreamingPartitionLookup` (label `streaming_partition_lookup`)
when the point path runs and `FullScan` / `FallbackFullScan { reason }` otherwise — the label the
`ScanProgressMeter` already carries (`producer.rs:736`), so the field harness reads the taken path
from existing metrics. Pruned candidates increment `cqlite.read.sstables_pruned` (#2163). No new
config knob; the signal rides the existing observability contract.

## Test strategy (parity is the deliverable)

- **Dual-path parity harness:** run the *same* pushed PK-equality ticket through the scan path
  (route forced off) and the point path (route on) over a real multi-SSTable, multi-generation,
  tombstoned corpus fixture; assert the two `RecordBatch` streams are byte-identical.
- **Query-semantics oracle:** the point-read result must match
  `test-data/query-semantics-oracle.json` at the pinned `now` (post-reconciliation truth), not only
  the physical-dump goldens (which cannot catch a reconciliation bug — both paths keep shadowed
  rows). This is the oracle that proves LWW/tombstone correctness.
- **Work-done probe (issue AC):** a `CountingStepper`-style probe asserts partitions examined ≈
  candidate-SSTable point lookups, NOT the table's partition count — the scan-vs-probe distinction
  that makes the speedup real, failing on `main`.
- **Fail-safe:** a candidate with a stripped/absent index must be *read* (fall back), never skipped
  — asserted by a fixture whose key lives only in the index-less SSTable.
- **e2e wiring:** a real Flight `do_get` with a PK-equality ticket end-to-end reports
  `streaming_partition_lookup` and returns the correct rows.

## Open questions for Seam 1
1. Core primitive shape: a `PartitionStepper`-returning constructor on the SSTable reader vs a
   thin `SinglePartitionSource` that `KWayMerger` accepts alongside `DirSource`. (Recommendation:
   the latter — mirrors the existing source abstraction.)
2. `IN`-list bound: cap N (config vs schema-derived) before falling back to scan, or always N
   point reads? (Recommendation: fixed named cap, fall back to scan above it.)
3. Whether to also route the **aggregate** path (`aggregate_paths`, `producer.rs:816`) for
   `count(*) WHERE pk = ?`, or leave aggregates on scan for this change. (Recommendation: defer;
   row path only.)
