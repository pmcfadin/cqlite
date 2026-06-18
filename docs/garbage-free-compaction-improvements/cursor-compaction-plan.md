# Plan: Finishing CursorCompactor

> **2026-06-12 — FINDING #26 (counter-tombstone value tie-break) FIXED, COMMITTED; ladder
> 32/32. MEGA-REVIEW QUEUE COMPLETE: 5 of 6 candidates fixed (#22-#26), 1 refuted (paxos —
> indexes gate covers it); branch total TWENTY-SIX findings.** mergeCounterCells' tombstone
> fold treated COMPARE as keep-left; the iterator routes tied counter tombstones (not
> counter cells per AbstractCell.isCounterCell) through Cells.resolveRegular's
> compareValues tail — greater RAW value bytes win. Fix: COMPARE branch mirrors the regular
> path's #21 rule (copy challenger wire value to tempCellBuffer2, skip the leading length
> vint on both sides, Arrays.compareUnsigned, swap buffers + liveness + flags when the
> challenger's bytes are strictly greater). Pinned by
> CounterDifferentialCompactionTest.counterTombstoneValueTieBreak: tied (ts, ldt) pairs in
> both encounter orders (empty-vs-valued each direction; red kept the empty one) plus
> valued-vs-valued, across generations. Remaining from the review (NOT yet fixed, medium
> backlog journaled in the mega-review entry): lastClustering aliasing, expiring counter
> cells, corrupt-input parity x3, partition-tombstone guardrail count, headerLength int
> wrap, BTI gate message. Upstream JIRAs to file: Row.Merger NPE on dropped-column
> surviving deletion; serializedMarkerBodySize size-vs-write mismatch; post-2038 LDT
> round-trip degradation to INVALID. Soak follow-up: uuid pk generator flake + per-seed
> repro position-dependence (shared blob gen).
>
> **2026-06-12 — FINDING #25 (complex-deletion LDT size predict-vs-write) FIXED, COMMITTED;
> ladder 32/32.** SSTableCursorWriter predicted complex-marker deletion sizes with
> SerializationHeader.deletionTimeSerializedSize (long-domain delta: 5 bytes) while
> writeLocalDeletionTime emits writeUnsignedVInt32((int)delta), which sign-extends — a delta
> in [2^31, 2^32) writes 9 bytes → row-size vint 4 short → STRUCTURALLY CORRUPT output (the
> red run's harness verifier rejected the cursor sstable exactly as predicted). Fix:
> deletionTimeWrittenSize() in the writer mirrors the cast; SerializationHeader untouched
> (its mirror bug on the iterator RT-marker path = upstream JIRA #2, journaled in the #22
> entry). Pinned by FarFutureDeletionDifferentialCompactionTest: raw-applied ldt=4e9 (~2096,
> VALID — avoids the corrupted-tombstone guardrail) + recent tombstone pinning minLDT;
> corrupted_tombstone_strategy=disabled for the suite (prod default; the test default
> 'exception' makes the ITERATOR refuse the round-tripped INVALID classification outright).
> DECODE finding (adjusted from the review's prediction): the negative wrap round-trips to
> the INVALID classification IDENTICALLY on both paths — DeletionTime.build and
> ReusableDeletionTime.reset agree, and UnfilteredSerializer.readComplexColumn's unsigned
> fixup (:694) is DEAD for 5.0-format inputs because build normalizes before the < 0 check.
> Row-deletion parity pinned too. UPSTREAM OBSERVATION #3: valid post-2038 deletion times do
> not survive the header round-trip on ANY path (4e9 in → INVALID out) — data degradation,
> not just a guardrail issue; belongs in the same JIRA family as the marker-size bug.
> VACUOUS-GREEN LESSON AGAIN: first red attempt silently lost the raw far-future deletion to
> the memtable merge — CQL INSERT of a collection writes its own complex deletion at the
> statement timestamp, superseding the raw mfda=2000 one; probe-then-pin before trusting
> green.
>
> **2026-06-12 — FINDING #24 (accord nowInSec) FIXED, COMMITTED; ladder 31/31.**
> CompactionIterator.purger() overrides nowInSec = controller.gcBefore for
> metadata.isAccordEnabled() || migratingFromAccord() tables (defers TTL-expiry conversion
> and liveness purge so accord can read at earlier timestamps); the cursor used wall-clock
> nowInSec everywhere. Fix: same override in the CursorCompactor constructor — every
> nowInSec use in the class is a purge/expiry decision (shouldPurge x3, isExpired x1), so
> overriding the field is exactly the iterator's semantics. Pinned by
> AccordTableDifferentialCompactionTest.expiredCellsDeferToGcBefore: transactional_mode =
> 'test_unsafe' (accordIsEnabled without routing CQL through accord) + a REAL local
> AccordService (localStartup + distributedStartup, the RouteIndexTest pattern) because
> CompactionTask.getCompactionController derives accord gcBefore from
> node.durableBefore()/redundantBefore (NoOpAccordService.node() is null, mocks too
> brittle); harness gcBefore passed as CompactionManager.NO_GC matching the scheduler's
> sentinel (Invariants.expect(gcBefore <= 0)); empty durability state derives NO_GC →
> iterator keeps lapsed-TTL cells, unfixed cursor converted them to tombstones (red showed
> exactly that). Ladder now 31 suites.
>
> **2026-06-12 — FINDING #23 (bloom_filter_fp_chance=1.0 ClassCastException) FIXED,
> COMMITTED; ladder 30/30.** BigCursorIndexWriter cast indexWriter.bf to the concrete
> BloomFilter for the garbage-free add(byte[],int,int,long[]) overload; FilterFactory
> returns the AlwaysPresentFilter singleton (an IFilter, NOT a BloomFilter) at exactly
> fpChance == 1.0 — legal schema, permanent compaction-failure loop on BIG tables (no
> fallback; CompactionTask rethrows). Fix: resolve once in the constructor via instanceof,
> null means "nothing to add to", matching the iterator's interface-dispatch no-op. BTI
> path was already safe (goes through IndexWriter.append → bf.add(key)). Pinned by
> BasicDifferentialCompactionTest.bloomFilterDisabled (red: CCE; green: byte-identical).
>
> **2026-06-12 (later still) — FINDING #22 (dropped columns) FIXED, COMMITTED; ladder
> 30/30 (one soak flake, see below).** Reader-level filtering in
> SSTableCursorReader.CellCursor.readCellHeader, mirroring DeserializationHelper.isDropped /
> isDroppedComplexDeletion: per-column drop horizons precomputed into droppedTimesArray
> (built with cellTypeArray, garbage-free), dropped cells consumed-not-surfaced, dropped
> complex deletions reset LIVE, and a surviving complex deletion whose cells all drop
> surfaces as a deletion-only pause so the merge fold still sees it. Filtering MUST be
> per-source pre-merge (not winner-filter): counter contexts merge values across sources,
> so a post-merge filter would fold dropped shards into the output. Wrapper change:
> readCellHeader rc=-1 now advances straight to UNFILTERED_END — a cell-less CELL_END stop
> would enter the cell merge as a phantom simple-column group (rc=-1 was previously
> unreachable with pauseAtEmptyComplexColumns on; the one pause-off consumer tolerates
> both shapes). The red corpus exposed TWO MORE cursor bugs, fixed in the same commit:
> (a) reader rejected sstables containing static rows once the last static column was
> dropped (metadata.hasStaticColumns → serializationHeader.hasStatic; the HEADER decides);
> (b) compactor skipped the entire static merge phase for the same reason — cursors stuck
> at STATIC_ROW_START ("Unexpected static row after static row merge"); now derived from
> the INPUT sstable headers, whose union is the output header (SerializationHeader.make),
> keeping reader/compactor/writer consistent. New corpus:
> DroppedColumnDifferentialCompactionTest (+ Bti variant), 7 scenarios: regular/complex/
> static/counter drops, drop+re-ADD resurrection shape, USING TIMESTAMP future cells
> newer than the drop retained, surviving complex deletion (single-sstable, see below).
> Ladder now 30 suites (both added).
> **UPSTREAM BUG #1 (JIRA needed):** iterator compaction NPEs at
> Row.Merger.ColumnDataReducer.getReduced (Row.java:904, complexBuilder null) when a
> dropped complex column's SURVIVING deletion (DELETE ... USING TIMESTAMP future, then
> ALTER DROP) merges with another version of the row — the reference path itself cannot
> compact that shape, so the differential scenario is single-sstable on purpose.
> **UPSTREAM BUG #2 (JIRA needed, from the mega-review size sweep):** iterator
> serializedMarkerBodySize (UnfilteredSerializer:410-426) PREDICTS marker deletion sizes
> with long-domain sizeofUnsignedVInt while writeDeletionTime emits the (int)-cast
> sign-extended form — same 5-vs-9 mismatch family as queued finding #25, on trunk.
> **SOAK FLAKE (follow-up):** BtiRandomDifferentialCompactionTest seed 14742527382543
> (example 7) — workload generator bound a 1-byte value to a uuid partition key
> (InvalidRequestException in MultiElements$Value.fromSerialized, pure CQL layer, no
> compaction on stack). NOT reproducible as example 0 with the same seed: the shared blob
> generator (JVM-wide, seed 42) makes per-seed repro position-dependent — generator bug
> plus a repro-determinism gap to fix in the harness. Suite re-ran green twice.
>
> **2026-06-12 (later) — MEGA-REVIEW (24 agents over full branch diff): six high-confidence
> candidates, one REFUTED, five queued.** Candidate queue, fix-in-progress order: #22 dropped
> columns never filtered (no isDropped on cursor path; iterator filters at
> UnfilteredSerializer:676/701/709; resurrection after DROP+re-ADD; adversarially confirmed,
> no gate, no fallback); #23 bloom_filter_fp_chance=1.0 → ClassCastException at
> BigCursorIndexWriter:167 ((BloomFilter) cast on AlwaysPresentFilter; permanent compaction
> failure loop, BIG only); #24 accord-enabled tables purge with real nowInSec
> (CompactionIterator:281 overrides nowInSec=gcBefore; cursor doesn't — premature TTL
> conversion/purge); #25 complex-deletion LDT size prediction vs write divergence
> (SSTableCursorWriter:503 sizeofUnsignedVInt(long delta) vs :574 writeUnsignedVInt32((int))
> sign-extension; delta in [2^31,2^32) → row-size vint understates by 4 → CORRUPT output;
> verified reachable via INVALID_DELETION_TIME + merged stats minima; NOTE upstream iterator
> has the MIRROR bug for RT markers via serializedMarkerBodySize — separate JIRA, not ours);
> #26 counter-tombstone (ts,ldt) tie keeps left, iterator ends at Cells.resolveRegular
> compareValues (greater raw value bytes win; reachable with the #20 valued-tombstone shape).
> **REFUTED: the system.paxos/PaxosPurger candidate** — system.paxos is built with
> PaxosUncommittedIndex (SystemKeyspace.java:265), so the existing indexes gate excludes it
> in every paxos_state_purging mode; protection is incidental-but-real. Jon: revert the
> drafted explicit gate + matrix pin, no change. (If the indexes gate is ever lifted, paxos
> needs an explicit PaxosPurger gate — re-raise then.) Medium-confidence backlog, not queued:
> lastClustering aliases reusable descriptor (stats metadata on trailing merged-to-nothing
> unfiltereds); raw-applied expiring counter cells (cursor strips ttl, skips expiry
> conversion; CQL can't produce); corrupt-input parity (reader (int)readUnsignedVInt complex
> cell count vs readUnsignedVInt32; missing ttl<0 check; complex deletion not validated under
> corrupted_tombstone_strategy); partition-tombstone guardrail never counts the partition
> deletion (checked before updatePartitionDeletion, reset by addKey); headerLength int wrap
> for ~2GiB static rows; BTI allocation-gate failure message prints CEILING_BYTES instead of
> ceilingBytes().
>
> **2026-06-12 — POST-COUNTERS BUG HUNT: findings #20 + #21 fixed, COMMITTED 6336451163;
> ladder 28/28.** The adversarial pass over the post-review diff surfaced two reachability-
> gated candidates; both were then DEMONSTRATED red:
> - **#20 (counter tombstone value loss — output corruption):** a counter tombstone carrying
>   a value (serializers preserve values faithfully; proven reachable by raw-applying such a
>   cell — flush writes it, probe confirmed valueSize 36 on disk) was written by
>   mergeCounterCells with flags claiming a value but no value bytes → CorruptSSTableException
>   on the cursor's own output. Pinned by CounterDifferentialCompactionTest
>   .exoticCounterCellShapes: RAW-applied cells the CQL surface cannot produce — multi-shard
>   global+remote contexts (the only end-to-end differential coverage of real shard merges),
>   MARKED local-shard contexts (pins the clear transform end-to-end), and the retained
>   tombstone-with-value (an epoch-old ldt purges in both paths and tests nothing —
>   vacuous-green lesson again; ldt=now-60 retains it). Also proven UNREACHABLE and
>   documented: empty-value live counter cells cannot exist in sstables — memtable flush
>   itself dies in upstream hasLegacyShards before writing one.
> - **#21 (same-ts tie-break compares wire bytes — wrong winner, LATENT since the original
>   cursor code):** found by the widened soak (seed 106755391857402, exposed by the
>   counter-mode stream shift); bisect confirmed it fails at 82026ab7ed. The COMPARE branch
>   compared vint-length-prefixed wire bytes, ordering same-ts ties by LENGTH first; the
>   reference (Cells.resolveRegular → ValueAccessor.compare) is unsigned lexicographic on
>   RAW bytes. Diverges whenever tied values have different lengths (finding #4's pin used
>   equal-length values). Fix: skip the vint for variable-length types (fixed-length carry
>   none). Pinned by EdgeCaseDifferentialCompactionTest.timestampTiesDifferentLengthValues
>   (both directions + the 128-byte vint-width boundary) + the seed.
> - **Harness improvement (committed together):** capture() now copies output components
>   BEFORE verification — a failed leg's transaction rollback used to delete the only
>   evidence; the captured bytes enabled the #20 hand-decode.
> - **Process pothole (memorized):** a bisect cleanup (`git checkout HEAD -- src/java`)
>   silently WIPED the then-uncommitted #20 fix; its resurfaced red masqueraded as a new
>   regression and cost a long ghost-hunt (instrumentation, hand-decode of the uncompressed
>   row: row size vint 5 vs 42 — value missing — finally exposed the stale file). Bisects
>   over uncommitted work: git stash first.
> Branch total: TWENTY-ONE findings. Remaining standing orders (Jon): code documentation,
> structure/clarity, testing improvements — then document the entire SSTable format.
>
> **The full-ladder runner now lives at `garbage-free-compaction-improvements/ladder.sh`**
> (moved from build/tmp at Jon's request; logs still go to `build/tmp/ladder/`, summary at
> `build/tmp/ladder/summary.txt`). 28 suites, sequential, accord-race workaround between
> runs, PASS/FAIL from the junit result lines rather than ant's exit code.

> **2026-06-12 — INCREMENT 5 (COUNTERS) COMPLETE.** Commits: 81bc893436 (CursorCounterContexts
> — garbage-free window mirror of CounterContext merge/clearAllLocal/hasLegacyShards, pinned
> by CursorCounterContextMergeTest: 2000 merge + 2000 clear + 1000 legacy-predicate randomized
> trials byte-identical with upstream, all outcome classes asserted non-vacuous) and
> 2855d2d34a (integration: gate flip — unsupportedSchema now rejects NOTHING; mergeCounterCells
> mirrors Cells.reconcile/resolveCounter incl. CASSANDRA-7346 tombstone supremacy, the
> load-bearing pre-reconcile shadow filter, max-ts liveness, and the Flag.LOCAL marked-shard
> clear on EVERY counter value incl. passthrough — the read-parity subtlety that raw streaming
> would have missed; writer gained a raw-window value write + updateHasLegacyCounterShards
> stats parity). Scenario rhythm followed: matrix flip + 5-scenario CounterDifferentialCompactionTest
> (+ BTI subclass) failed RED on the closed gate, then green post-implementation (one bug
> caught on the way: VIntCoding.numberOfExtraBytesToRead takes the SIGNED first byte). Soak
> gained a counter-table generation mode (~1 in 5 examples: counter-only schemas, increment/
> delete workload — the TypeKind filter stays because counters cannot mix with regular
> columns). Allocation gate: counter scenario per-input-byte, measured 1.367 B/B BIG /
> 1.646 BTI (tiny ~68B rows make per-key residual dominate; JFR recordCounterAllocationProfile:
> ZERO cursor-owned per-cell sites), ceilings 1.7/2.0. CLOSING LADDER 28/28 GREEN (new suites:
> counter ×2, three unit pins). Single-JVM CQL only makes single-shard global contexts —
> multi-shard/local/remote/marked semantics rest on the unit pin; journaled as the documented
> coverage shape. GarbageSkipper (increment 6, optional) is now the ONLY remaining gap;
> next per Jon: open-ended bug hunt + docs/structure/testing improvements, then full SSTable
> format documentation.
>
> **Cleanup sweep COMMITTED bc7099ac11** (the journaled minor queue, ladder-gated, 8 suites
> green): dead SSTableCursorWriter.currentOffsetInPartition + 5 dead imports removed; BTI
> granularity now references BtiFormatPartitionWriter.DEFAULT_GRANULARITY (widened to
> package-private — no more silent-drift literal); endPartition uses partitioner.decorateKey
> (PreHashedDecoratedKey saves a bloom hash pass); CursorIndexWriter gained a close() hook
> (BTI closes its rowTrie, mirroring BtiFormatPartitionWriter.close); BtiCursorIndexWriter's
> stale "reusable views" javadoc corrected to the snapshotOf reality; harness: stale
> early-open comment + dangling scope removed, expired-flag normalization hoisted to a static
> Pattern (scale suites ~25% faster: BigVolume 27s→19.8s). DEFERRED from the sweep (noted,
> low value-to-churn): ComplexColumnListener test-only seam, long[21] bloom scratch constant,
> ClusteringPrefix.Serializer bit-helper widening, 9x Bti test-format boilerplate dedup.

> **Status:** increment 1 in progress, on branch `cursor-compaction-completion`.
> **This document is NEVER committed** — it is a specification plus a journal of what is
> completed. Each increment's results, benchmark numbers, edge-case findings, and
> allowlist justifications get appended as they land.
>
> **Prime directive:** this database runs at scale; changes here must not break anything.
> The harness comes first, comprehensively, and must prove the *currently supported*
> surface correct — including finding edge cases in what already works — before any
> feature gap is touched.
>
> **Prime constraint (added 2026-06-09): cursor compaction exists to be GARBAGE-FREE** —
> no per-partition/row/cell heap allocations in steady state. Every change, bug fix or
> feature, must preserve this. The toolbox is the existing one: reusable/flyweight objects
> (`ReusableLivenessInfo`, `ReusableDeletionTime` + pooling), retained-and-cleared
> `DataOutputBuffer`s, raw byte[] windows compared with `Arrays.compareUnsigned`, primitive
> state. New per-element `new`, boxing, iterator creation, or String building on the hot
> path is a defect even when functionally correct.
> Audit of the six increment-1 fixes (diff 3831d8265d..52ce72062c, src/java only): zero
> allocation sites — primitive arithmetic, flag logic, and writes into pre-existing reused
> buffers throughout. Property intact.

## Goal

Cursor-based compaction (`CursorCompactor`, `SSTableCursorReader`, introduced in
`ff78780d61`) avoids per-partition/row/cell garbage but falls back to the iterator path
(`CompactionIterator`) for most real-world schemas and configurations. This plan closes
the functional gaps incrementally — one capability at a time, each verified before the
next begins — on a local branch, upstreamed only once the full path is proven.

Secondary goal: increments 2–4 deliberately build the capabilities a future read-path
cursor needs (complex cells, BTI, bounded/sliced reads), so this work doubles as the
de-risking phase for that larger effort.

## Current support gaps (from `CursorCompactor.isSupported()` / `unsupportedMetadata()`)

| Gap | Where | Planned |
|---|---|---|
| Multi-cell columns (collections, UDTs) | `CursorCompactor.java:185`, throws at `:643` | Increment 2 |
| BTI output format | `CursorCompactor.java:142` | Increment 3 |
| Partial-range scanners | `CursorCompactor.java:126` (TODO `:125`) | Increment 4 |
| Counter columns | `CursorCompactor.java:190`, throws at `:645` | Increment 5 |
| GarbageSkipper (`tombstoneOption != NONE`) | `CursorCompactor.java:148` (TODO `:147`) | Increment 6 (optional) |
| Older sstable versions | `CursorCompactor.java:134` | Out of scope — phases out as compaction rewrites to current version |
| Secondary indexes / SAI | `CursorCompactor.java:169` | Out of scope — index observers consume materialized rows; needs its own design |
| Non-reusable-key partitioners, Accord keyspace | `CursorCompactor.java:160-167` | Intentional constraints, not gaps |

Note: frozen collections are single cells (`column.isComplex()` is false) and already
work. The complex-column gap is multi-cell columns only.

## Decisions (made up front)

1. **Harness first.** Increment 1 is a differential verification harness, not a feature.
   Every later increment inherits its gate by extending the workload space.
2. **Verification bar: bytes + logical.** Assert byte-identical output components where
   the two paths should match exactly; fall back to logical equivalence (full
   unfiltered-walk comparison) with an explicit, documented allowlist of known-divergent
   components. Every allowlist entry must be justified in this doc.
3. **Delivery: local branch first, single JIRA (decided 2026-06-09, refined same day).**
   Finish increment 1 completely (randomized soak, Harry histories, frozen-clock boundaries)
   before any feature work. The patch boundary depends on what the soak finds: if it surfaces
   enough additional bugs, the harness + all bug fixes ship as one JIRA with the feature gaps
   (multi-cell columns, BTI, partial scanners, counters, garbage skipper) as a follow-up;
   if the findings stay at the current six, everything rolls into one combined JIRA.
   Increments remain clean commit series on the branch for reviewability. The reviewer-notes
   section is written to transplant into the JIRA description.
   Open scope question for the JIRA boundary: secondary indexes/SAI (currently out of scope —
   index observers consume materialized rows and need their own design; revisit whether
   "every use case" includes it before posting).
4. **Performance is informational, not a gate** — but if the cursor path ever *loses* to
   the iterator path for a newly supported shape, that's a stop-and-discuss finding.

## Existing test infrastructure (survey, 2026-06-09)

The harness is assembly, not invention. Prior art in-tree:

- **`test/unit/.../db/compaction/simple/`** — `SimpleCompactionTest` base, parameterized
  over `cursor={true,false}` × `diskAccessMode={standard,direct}`; 8 subclasses covering
  value merge, static rows, PK/row/range/column deletes, purge. Hand-written scenarios;
  each path verified against expected CQL results independently. **Gap: paths never
  compared against each other, never at file level.** These become the seed corpus.
- **`RandomSchemaTest`** (`test/unit/.../cql3/`) — QuickTheories property test:
  random schemas via `CassandraGenerators.TableMetadataBuilder` + `AbstractTypeGenerators`
  (collections, UDTs, vectors, depth control), random data via `CassandraGenerators.data()`.
  Already has a `STRESS_CURSOR_COMPACTION` flag that filters generated schemas through
  `CursorCompactor.unsupportedMetadata()` — the generator↔support-matrix hook exists.
  **Gaps: single-row inserts (no real merge), no tombstones, read-back verification only.**
- **Harry** (`test/harry`, fuzz tests in `test/distributed/.../fuzz/harry/`) — model-based
  fuzzer. `ReplayingHistoryBuilder` DSL drives probabilistic histories (insert, row/
  partition/column/range deletes, flushes) from a seeded RNG; model checkers
  (`QuiescentChecker`, `Reconciler`, `ASTSingleTableModel`) validate reads against
  expected state. See `RangeTombstoneBurnTest` for the canonical pattern. **Gap:
  `ColumnSpec.java:64-65` — no UDT or collection generation yet.** Also: bundled
  executors are dtest-oriented; increment 1 must confirm a single-JVM/CQLTester
  execution route (look in `harry/execution`).
- **`cql3.ast` generators** (`ASTGenerators`, `Mutation`) — QuickTheories mutation
  generation as ASTs, supports complex types; used by `ASTSingleTableModel`.

Consequence: complex-column scenarios (increment 2) come from the
`CassandraGenerators`/AST route, not Harry. Teaching Harry collections is an optional
upstream contribution, not a dependency of this plan.

## Increments

Per-increment rhythm: flip the generator/matrix expectation and watch the harness fail
for the right reason → implement → harness green → record JMH + allocation numbers here
→ sweep micro-TODOs only in code already being touched.

### Increment 1 — Differential verification harness

The genuinely new piece is the differential runner + comparators; scenario generation is
reused (see survey above).

1. **Dual-path runner** — compacts identical input sstables through `CompactionIterator`
   and `CursorCompactor` directly (bypassing strategy plumbing), following the
   `SimpleCompactionTest` parameterization pattern but capturing both outputs.
2. **Comparators**
   - Byte comparison of output components, with an allowlist (below) for components that
     legitimately diverge.
   - Logical walker: partition-by-partition, comparing every clustering, cell, liveness
     info, deletion time, and tombstone bound.
   - Stats spot-checks: min/max timestamps, tombstone counts, partition counts.
3. **Scenario sources**
   - Seed corpus: ported `simple/` scenarios.
   - Random: `CassandraGenerators` schemas/data, multi-round insert/delete/flush to force
     real multi-sstable merges (fixing `RandomSchemaTest`'s single-row weakness).
   - Tombstone-rich: Harry `HistoryBuilder` histories on simple schemas (executor route
     to be confirmed).
   All seeded and reproducible from a logged seed.
4. **Support-matrix test** — asserts `isSupported()`/`unsupportedMetadata()` verdicts for
   each schema shape. Later increments flip their row; gate regressions become test
   failures, not silent fallbacks.

**Exit criteria:** harness green on today's supported space; byte-divergence findings for
currently supported tables either fixed or documented in the allowlist with justification.

**First deliverable inside this increment:** learn whether byte-identical data components
*already* hold between the two paths for supported tables.

#### Byte-comparison allowlist

| Component | Reason | Justification recorded |
|---|---|---|
| *(none yet — populated by increment 1)* | | |

### Increment 2 — Multi-cell columns (collections, UDTs)

Largest feature. Sub-steps, each harness-gated:

- **(a) Reading:** cell paths + complex deletion times through `SSTableCursorReader`
  (`CellCursor.cellPath` is partially plumbed; replace throw at `CursorCompactor.java:643`).
- **(b) Merging:** per-path cell reconciliation within a complex column, complex-deletion
  handling, reusable structures (template: the `ReusableDeletionTime` pooling at
  `CursorCompactor.java:847-1063`).
- **(c) Writing:** complex cell serialization in the cursor writer.

**Exit criteria:** generator flags for maps/sets/lists/UDTs on; support matrix flipped;
harness green including complex deletions interleaved with range tombstones (the nastiest
interaction here).

### Increment 3 — BTI output format

Remove the `BigFormat`-only check (`:142`). Data component serialization is shared between
formats; the work is feeding trie partition/row index builders from the cursor's streamed
byte-comparable keys.

**Exit criteria:** full matrix runs with BTI output; data component held to the byte
standard vs the iterator path's BTI output; index components byte-compared if
deterministic (expected; allowlist if not); read-back through `BtiTableReader`.

### Increment 4 — Partial-range scanners

The TODO at `:125`. Seek cursor to first partition ≥ range start via the index; stop at
range end. **Design the cursor-level seek API with read-path slices in mind** — this
increment is the direct foundation for read-path cursor bounds; record API rationale here.

**Exit criteria:** harness gains range-scoped scenarios (token-subrange compactions,
boundary partitions, ranges landing mid-index-block).

### Increment 5 — Counters

`CounterContext` reconciliation with reusable scratch buffers (replace throw at `:645`).

**Exit criteria:** counter generator flag on; scenarios include shard merges and counter
tombstones.

### Increment 6 — GarbageSkipper equivalent (optional)

TODO at `:147`, for `tombstoneOption != NONE`. Default is NONE, so most deployments never
hit this fallback. Explicitly cuttable if the branch is otherwise ready to upstream.

## Sequencing rationale

Harness converts every later increment from "carefully reason about merge correctness"
to "make the diff empty." Complex columns before BTI: largest coverage win, exercises the
harness hardest while output format is still simple. BTI before partial scanners: without
BTI output, the whole cursor path silently stops running the day a cluster switches its
default format. Partial scanners before counters: the read-path dividend.

## Micro-TODO sweep list (opportunistic, only when touching that code)

- `SSTableCursorReader.java:151` — specialize common cell-flags case (`HAS_VALUE | USE_ROW_TS`)
- `SSTableCursorReader.java:750` — pre-compute next state when flags are read
- `SSTableCursorReader.java:385` — avoid copy through buffer for cell values
- `SSTableCursorReader.java:467` — introduce cell header class
- `CursorCompactor.java:535-537` — state validation / flag-check optimization

## Results log

### 2026-06-09 — Increment 1: harness design + first implementation

**Files:**
- `test/unit/org/apache/cassandra/db/compaction/differential/DifferentialCompactionTester.java` (base)
- `test/unit/org/apache/cassandra/db/compaction/differential/BasicDifferentialCompactionTest.java` (smoke scenarios)

**Design decisions:**
- Both paths run the **full production** `CompactionTask.execute()` (selection happens at
  `CompactionTask.java:263` → `AbstractCompactionPipeline.create()` via
  `DatabaseDescriptor.setCursorCompactionEnabled`), not hand-driven pipelines — maximum fidelity.
- Identical inputs across runs: `keepOriginals=true` (original files never deleted), restore by
  delisting outputs (`Tracker.removeUnsafe`), releasing readers, deleting output files, re-opening
  original descriptors, `Tracker.addInitialSSTables`. Deliberately NOT `loadNewSSTables()` — that
  routes through `SSTableImporter` with `resetLevel(true)`, which can rewrite input stats between
  runs and poison the identical-inputs invariant.
- Same `gcBefore` passed to both runs → purge decisions cannot flip between runs.
- **Silent-fallback guard:** before the cursor run, the harness asserts
  `CursorCompactor.isSupported()` on equivalent scanners/controller. A scenario that would fall
  back to the iterator path fails loudly instead of vacuously passing.
- Logical comparison = `JsonTransformer.toJsonLines` (sstabledump format) with fixed `now=0` so
  rendering can't depend on wall clock, plus stats spot-check summary
  (min/max timestamps, deletion times, estimatedKeys, totalRows, totalColumnsSet).
- Byte comparison = every discovered component, allowlist starts EMPTY.
- Output verification also runs `IVerifier` with extended verification on every output sstable.

**Known limitations (accepted for now):**
- `nowInSec` for TTL-expiry evaluation is taken per run inside `CompactionTask.runMayThrow`
  (`FBUtilities.nowInSeconds()`); the two runs are seconds apart. Scenarios must not place
  TTL-expiry or purge boundaries within seconds of the run. Exact-boundary scenarios need a
  frozen clock — deferred, tracked here.
- Scenarios with fully-expired input sstables would have inputs obsoleted (deleted) by run A
  (`transaction::obsolete` in `runMayThrow`), breaking restore. Excluded from the corpus until
  the harness gets snapshot-based restore.

**Status:** smoke scenarios (overlapping overwrites, retained tombstones incl. range/partition
deletes, purged tombstones with empty-partition drop, static rows) written; edge-case corpus
(DESC clustering, composite clusterings, wide partitions crossing index blocks, overlapping
range tombstones, frozen collections, TTLs, timestamp ties, shadowed partitions, single-input,
8-way merge, disjoint inputs, empty/null values) written; support-matrix test written
(`CursorSupportMatrixTest` — pins unsupportedMetadata verdicts; increments 2/5 flip their rows).
First build/run in progress.

**Upstream finding #1 (latent bug candidate, found while building the harness):**
`SSTableRewriter.moveStarts()` (`SSTableRewriter.java:214`) obsoletes fully-covered originals
via `transaction.obsolete(latest)` (`:232`) WITHOUT checking `keepOriginals` — the flag only
guards the bulk `obsoleteOriginals()` call (`:349`). Consequence: `keepOriginals=true` on an
ONLINE transaction with early open active still deletes the original files (asynchronously,
when the last reader reference is released). Production users of `keepOriginals`
(scrub/upgrade) run offline transactions where `moveStarts` returns early, so this never
bites today — but the flag's contract is violated for online use. Worth an upstream JIRA;
low severity. Harness works around it by disabling preemptive open during differential runs
(early open affects read availability mid-compaction, not final output bytes).

**Upstream finding #2 (REAL output divergence on the supported surface — FIXED on this branch):**
The cursor writer never wrote the `previousUnfilteredSize` field: `SSTableCursorWriter` wrote a
literal `0` vint for every row (`writeRowStart`, `writeEmptyStaticRow`) and marker
(`writeRangeTombstone`, the `// TODO: previousUnfilteredSize` at old line 531). The iterator path
writes the true distance from the previous unfiltered's start
(`SortedTablePartitionWriter.addUnfiltered`: `offset - previousRowStartOffset`;
static rows hardcode 0 via `serializeStaticRow` and do not advance the chain).
- Severity today: LOW — every deserialization site skips the field
  (`UnfilteredSerializer.java:529,571,602`; comment at `:208-209`: written "to make reverse
  queries more efficient... currently not used"). So no live read-path bug, but cursor-compacted
  sstables differ in content from iterator-compacted ones, and any future/external consumer of
  the field would be silently broken.
- Found by: differential harness, `tombstonesRetained` — 2601/5100 byte positions differed in
  one small sstable (every row's prev-size vint plus knock-on effects).
- Fix (this branch): `SSTableCursorWriter` now tracks `previousRowStartOffset` per partition and
  writes the field exactly as the iterator does, including the row/marker size vints accounting
  for the prev-size vint length (mirrors `UnfilteredSerializer.java:207-210` and
  `serializedMarkerBodySize:414`).

**Upstream finding #3 (REAL output divergence on the supported surface — FIXED on this branch):**
Every tombstone cell written by cursor compaction carried a spurious `IS_EXPIRING` flag plus a
wasted TTL vint (`00`) — +1 byte per tombstone cell and a flags byte differing from the iterator
path (`0x07` vs `0x05` for a plain cell tombstone). Two compounding causes:
1. `ReusableLivenessInfo.isExpiring()` means "has an expiration time"
   (`localExpirationTime != NO_EXPIRATION_TIME`) — TRUE for tombstones too — whereas cell flag
   semantics need `AbstractCell.isExpiring()` (`ttl != NO_TTL`).
2. `CursorCompactor` set `IS_DELETED` / `IS_EXPIRING` with two independent `if`s
   (old lines 775-776), whereas `Cell.Serializer.serialize` uses `if/else-if`
   (`Cell.java:349-352`) — deleted and expiring are mutually exclusive in the format.
- Severity today: LOW-MEDIUM — the resulting cell is still deserialized to an equivalent
  tombstone (`ttl=0` reads back as `NO_TTL`), so reads are correct, but every tombstone cell in
  cursor-compacted sstables is one byte larger and flag-divergent from iterator output.
- Found by: differential harness, `tombstonesRetained` / `tombstonesRetainedUncompressed` —
  isolated as a single-byte insertion at the only tombstone-cell row once finding #2's noise was
  fixed; flags byte decoded from the uncompressed hexdump.
- Fix (this branch): strict `isExpiring = ttl != NO_TTL` plus `else-if`, mirroring
  `Cell.Serializer` exactly (`CursorCompactor.java` cell-flag rebuild).

**Upstream finding #4 (DATA CORRECTNESS BUG on the supported surface — FIXED on this branch):**
Cursor compaction resolved same-timestamp cell conflicts with INVERTED value comparison: in
`CursorCompactor.mergeCells` the challenger replaced the current winner when
`compareUnsigned(currentValue, challengerValue) >= 0` — i.e. the LEXICALLY SMALLER (or equal)
value won. The iterator rule is `compareValues(left, right) >= 0 ? left : right`
(`Cells.java:128`): left/current wins ties, challenger needs a strictly greater value.
- Consequence: for same-timestamp writes (same-millisecond client timestamps, bulk loads,
  `USING TIMESTAMP` collisions), cursor-compacted sstables keep the WRONG value — silent data
  divergence between cursor- and iterator-compacted replicas; the comment at `:648`
  ("same timestamp handling awaits!") shows the case was known-incomplete, but the code path
  WAS live and chose wrongly rather than falling back.
- Severity: HIGH within its trigger window (equal-timestamp, both-live, different values).
- Found by: differential harness `timestampTies` scenario — iterator kept "zzz*", cursor kept
  "aaa*".
- Fix (this branch): comparison polarity corrected to `compare < 0` → challenger wins only with
  a strictly greater value. Also added the missing TTL tie-break from `Cells.java:121-125`
  (both expiring, same timestamp/expiration, different TTLs → lower TTL wins) to
  `CursorCompactor.resolveRegular`, which previously fell through to value comparison.

**Upstream finding #5 (Index.db divergence — FIXED on this branch):**
`widePartitionCrossingIndexBlocks`: Data.db byte-identical, Index.db differed by one byte value
(`0xcd` vs `0xcf`) — the LAST index block's width vint in the promoted index. Root cause: the
cursor computed the final block width as `(partitionEnd - partitionStart - 1) -
indexBlockStartOffset`, EXCLUDING the end-of-partition marker byte; the iterator's
`BigFormatPartitionWriter.finish()` calls `addIndexBlock()` AFTER
`SortedTablePartitionWriter.finish()` has written the marker, so the reference width INCLUDES
it. Width delta 1 → zigzag vint delta 2 (the width-minus-WIDTH_BASE delta is negative for a
sub-threshold tail block). Severity: LOW (block width slightly understated; reads tolerate it),
but format-divergent. Fix: `SSTableCursorWriter.writePartitionEnd` now includes the marker byte
and skips the tail block only when size == 1 (exactly the iterator's `firstClustering == null`
case).

**Upstream finding #6 (missing promoted index at the block boundary — FIXED on this branch):**
For a partition crossing `column_index_size` exactly once (one cut block + tail), the cursor
wrote NO promoted index (entry: promoted size `0`, 12 bytes) where the iterator writes a
2-entry promoted index (74 bytes). Root cause: `appendBIGIndex` decided promotion via
`rowIndexEntriesOffsets.size() <= 1` BEFORE the tail block was added; the iterator
(`BigFormatPartitionWriter.finish` + `RowIndexEntry.create`) promotes when the total count
INCLUDING the tail is > 1.
- Severity: LOW-MEDIUM, performance not correctness — indexed reads of such partitions lose
  intra-partition seeking and scan from the partition start. Affects every partition whose size
  lands between 1× and 2× `column_index_size` (prod default 64KiB: partitions ~64-128KiB — a
  common size band).
- Found by: predicted from code reading during finding #5, then confirmed by the
  `partitionCrossingOneIndexBlock` scenario — after correcting the scenario for the test
  config's `column_index_size: 4KiB` (NOT the prod default; first version of the scenario
  accidentally produced ~22 blocks and tested nothing new).
- Fix (this branch): tail block size computed and counted before the promotion decision;
  promotion at total > 1 including tail, exactly mirroring the iterator.

**Increment-1 corpus state after fixes: 18/18 differential scenarios byte-identical
(BasicDifferentialCompactionTest 5/5, EdgeCaseDifferentialCompactionTest 13/13),
CursorSupportMatrixTest 8/8. Allowlist: still EMPTY.**

### 2026-06-09 — Randomized soak (RandomDifferentialCompactionTest) and finding #7

Randomized scenario source added: random schemas via `CassandraGenerators.TableMetadataBuilder`
restricted to the cursor-supported surface with the production `unsupportedMetadata` filter
(plus a guard rejecting invalid generated CQL: statics require clustering columns), random
multi-round workloads (15-40 inserts/round with 30% primary-key overwrites, row deletes,
occasional partition deletes, flush per round, 2-4 rounds), seeded and reproducible — every
failure message carries the seed; schema is logged per example.
Deliberate v1 coverage gaps (journaled): NORMAL value domain only (no null/empty generated
values), no generated range deletes — both covered by the deterministic corpus.

**Upstream finding #7 (stats over-count for empty static rows — FIXED on this branch):**
The soak failed within its first examples (seed 179462336227839): `totalRows=74` (iterator) vs
`75` (cursor). Root cause: for a static-column table whose partition has NO static values, both
paths WRITE an empty static row, but the iterator collects row stats only for non-empty rows
(`SortedTableWriter.addStaticRow`: `if (!row.isEmpty()) Rows.collectStats(...)`), while the
cursor's `writeRowEnd` called `metadataCollector.updateColumnSetPerRow(...)` unconditionally —
`totalRows`/`totalColumnsSet` over-counted by one per such partition. The original `staticRows`
deterministic scenario gave EVERY partition static data, so it never produced an empty static
row — exactly the class of gap the randomized soak exists to find.
- Severity: LOW — stats only (`totalRows` feeds nodetool tablestats and estimates), but
  format/content divergent.
- Fix: `writeRowEnd` skips row-stats collection when the row is empty by the iterator's
  definition (no cells, no liveness timestamp/TTL, no row deletion — checked via
  `columnsWrittenCount` and the HAS_TIMESTAMP/HAS_TTL/HAS_DELETION row flags).
- Pinned by: new deterministic `EdgeCaseDifferentialCompactionTest.emptyStaticRows` scenario.

**Extended soak result (post-#7-fix):** 6 consecutive soak runs (60 random schema/workload
examples) fully green — byte-identical output across all components for every example.
Combined with the deterministic corpus (19 scenarios) the supported surface now holds
byte-equivalence under both crafted and randomized workloads. Allowlist remains EMPTY.

### 2026-06-09 — Partial-set and multi-output coverage (no new findings)

The two highest-risk untested regions are now differentially covered:

- **Partial-set compactions** (`PartialSetDifferentialCompactionTest`, 3 scenarios): only a
  subset of live sstables compacts; the purge evaluator must consult overlapping
  NON-participating sstables. Scenarios use explicit `USING TIMESTAMP` for deterministic purge
  decisions and SELF-VERIFY their outcome from the captured output (tombstones retained when
  an overlapping non-participant holds older data; purged when the non-participant is
  disjoint; retained when the shadowed data sstable is left out). Harness extended: input
  subsets, before/after descriptor diffing for output identification, non-participants left
  untouched across restore, and a TaskFactory hook for custom CompactionTask shapes.
- **Multi-output compactions** (`MultiOutputDifferentialCompactionTest`, 3 scenarios): a
  size-capped `MaxSSTableSizeWriter` (8KiB, compression disabled) forces writer switches —
  4-6 outputs per compaction, compared pairwise byte-identically. Covers switch-decision
  parity and per-output finalization (first/last keys, promoted index, stats — the machinery
  of findings #5-#7, now exercised per output).

Both suites green with NO new findings — cursor matches iterator byte-for-byte in both regions.

**Harness lesson (vacuous-green, second occurrence):** the first multi-output run produced ONE
output everywhere — `MaxSSTableSizeWriter` switches on estimated ON-DISK (compressed) bytes
and the repetitive test padding compressed below the cap. Scenarios that exercise a mechanism
now ASSERT the mechanism fired (output count >= 2; purge outcome present/absent in the dump):
`assertCursorMatchesIterator` returns the captured output for exactly this purpose. Same class
of error as finding #6's first scenario (sized against prod default instead of test config).

### 2026-06-09 — Final increment-1 items complete (no new findings)

- **Harry tombstone histories** (`HarryDifferentialCompactionTest`): deep probabilistic
  delete/overwrite histories (row/column/range/partition deletes, 70/10/8/8/4 mix, 3 flush
  rounds, reversed clustering, statics) via `HistoryBuilder` + `CQLTesterVisitExecutor` —
  plain CQL in this JVM, no dtest cluster (route: existing `HarryCompactionTest` pattern).
  Seeded and logged. The differential harness is the oracle; no Harry validation reads.
  Green: byte-identical.
- **Purge boundary WITHOUT clock control** (`PurgeBoundaryDifferentialCompactionTest`):
  instead of freezing the clock, the harness gained an explicit-gcBefore overload; the
  scenario reads the REAL deletion second from the flushed sstable's stats
  (excluding the Long.MAX_VALUE no-deletion sentinel — first scenario version tripped on it)
  and runs the differential at gcBefore == ldt (retained — strict `<` in both paths,
  cursor at `CursorCompactor.java:1452`) and gcBefore == ldt+1 (purged), with outcome
  assertions on the captured dump. Both byte-identical.
- **Config axes**: compaction read disk access mode `direct`, and Zstd/Deflate/Snappy
  compressors — all byte-identical.
- **Deferred (the one remaining clock-dependent gap):** TTL-expiry-exactly-at-nowInSec
  boundary. `nowInSec` is taken inside `CompactionTask.runMayThrow` per run and a custom
  `Clock` implementation can only be installed at JVM startup (`cassandra.clock` property),
  so deterministic coverage needs a dedicated JVM/test-runner configuration. Far-from-boundary
  TTL behavior (live TTLs, long-expired TTLs incl. expired-to-tombstone conversion) is covered.

**INCREMENT 1 COMPLETE.** Final state: 7 findings (6 fixed on this branch, 1 upstream-flagged),
allowlist EMPTY throughout — every divergence was a bug, never a justified difference.
Differential coverage: 30 deterministic scenarios across 7 suites (smoke, edge cases incl.
index boundaries and empty static rows, partial-set purge semantics, multi-output writer
switching, purge boundary, config axes, Harry histories) + randomized soak (60+ examples) +
support matrix + 236 pre-existing cursor tests. Increment 2 (multi-cell columns) is next; its
rhythm: widen the generator/matrix, watch the harness fail for the right reason, implement.

### 2026-06-09 — Increment 2 scoping (multi-cell columns)

Wire format per complex column within a row (`UnfilteredSerializer.readComplexColumn:685`):
`[complex DeletionTime if row flag HAS_COMPLEX_DELETION][cell count vint][count cells, each
with CellPath]`. Cells within a column are path-sorted. HAS_COMPLEX_DELETION is a ROW-level
flag covering all complex columns in the row.

Sub-increments, each gated by `unsupportedMetadata` staying true until all three land:

- **(a) Reading** (`SSTableCursorReader`): the cell loop (`CellCursor.readCellHeader:142`)
  assumes ONE cell per present column — needs a per-column remaining-cells counter (count
  vint), optional complex-deletion read before the cells, and path-carrying cell states.
  CellPath deserialization already exists (`:168-169`).
- **(b) Merging** (`CursorCompactor.mergeCells`, throws at `:643`): today resolves one winner
  per (clustering, column) across sources. Complex needs a nested N-way merge BY PATH within
  the column (sources are path-sorted on disk), equal paths resolved with the existing
  `resolveRegular` + value-compare logic, plus complex-deletion reconciliation (newest wins;
  shadows cells with timestamp <= it). Reference: `Row.Merger.ColumnDataReducer` /
  `ComplexColumnData` merge in the iterator path.
- **(c) Writing** (`SSTableCursorWriter`): `writeCellHeader` walks `columns[nextCellIndex]`
  one cell per column with missing-column subset tracking — complex columns need
  [complex deletion][surviving-cell count vint][cells]; the count is only known after the
  merge, so a per-column buffering or count-then-copy step is required (cells already buffer
  into rowBuffer). HAS_COMPLEX_DELETION must be computed row-wide before row flags are
  written.

**Garbage-free design constraints for each sub-increment:**
- (a) Reading: `cellColumn.cellPathSerializer().deserialize(dataReader)` (the existing
  dormant code at `SSTableCursorReader:168-169`) ALLOCATES a CellPath per cell — must be
  replaced with a reusable raw representation (byte[] scratch + length, like
  `clusteringBytes`), not used as-is. Per-column remaining-cell counter is a primitive field.
- (b) Merging: path comparisons via byte[] windows + `Arrays.compareUnsigned` (the
  `tempCellBuffer1/2` pattern); complex-deletion reconciliation via `ReusableDeletionTime`
  (pool already exists for range tombstones). No per-path objects.
- (c) Writing: the count-then-copy step uses one retained-and-cleared per-column
  `DataOutputBuffer` (grown once, amortized zero), same lifecycle as
  rowBuffer/rowHeaderBuffer.

**Verification idea for the property itself (harness addition, to build before or with
increment 2):** an allocation-scaling gate using
`ThreadMXBean.getThreadAllocatedBytes` — run the cursor compaction on N rows and on 10N rows
(warmed JVM) and assert allocated bytes do NOT scale with N (delta below a fixed ceiling).
That tests the amortized garbage-free property directly, is JVM-cheap, and turns "we think
it's allocation-free" into a regression test. JMH `gc.alloc.rate.norm` numbers recorded per
increment as the precision instrument.

Completion flips: `unsupportedSchema`'s isComplex rejection, `CursorSupportMatrixTest`
multi-cell rows, new deterministic scenarios (maps/sets/lists/UDTs, complex deletions
interleaved with range tombstones — flagged in the original plan as the nastiest
interaction), and the randomized soak's filter widens automatically.

### 2026-06-09 — Allocation gate built; BASELINE CONTRADICTS the absolute garbage-free claim

`CursorCompactionAllocationGateTest`: measures `ThreadMXBean.getThreadAllocatedBytes` around
`CompactionTask.execute` (cursor path verified via the isSupported guard — the gate's own
first run was vacuous-adjacent: it measured without asserting which pipeline ran; third
occurrence of that lesson, now asserted). Warmed, min-of-3, uncompressed table, 2 sstables,
simple schema (bigint pk/ck, bigint+text values), 1,200 vs 12,000 rows.

**Baseline numbers (current trunk + our 6 fixes):**
- Cursor path: 4.26MB -> 5.75MB, delta 1.49MB ≈ **137 B per additional row**
- Iterator path: 4.80MB -> 11.49MB, delta 6.68MB ≈ 619 B per additional row

Cursor allocates ~4.5x less per row than iterator — but it is NOT zero-scaling. ~137B/row
grows with data on the cursor path. The ~4.2MB constant base is per-compaction overhead
(controller, scanners, compaction-history mutation, logging) and cancels in the delta.
Attribution unknown without allocation profiling; candidates include shared per-row
infrastructure both paths pay (`MetadataCollector.updateClusteringValues` min/max copies),
per-key index/bloom structures (partition-scaled, likely too small), or missed allocations in
cursor reader/writer code. NOT yet a numbered finding — needs attribution first.

Decision (user): identify the allocation. Done — see finding #8.

**Upstream finding #8 (per-row allocation on the cursor hot path — FIXED on this branch):**
`ClusteringPrefix.Kind.values()` was called once per row read (`ClusteringDescriptor.java:64`,
and `:106`) and once per range-tombstone marker written (`SSTableCursorWriter.java:547`).
Every Java enum `.values()` call allocates a fresh array (~40B for this enum) — at one-per-row
-per-source this was the dominant scaling allocation on the "garbage-free" path.
- Found by: JFR (`jdk.ObjectAllocationInNewTLAB/OutsideTLAB` with stacks, 30 warmed
  compactions recorded from inside the gate test; async-profiler not installed, JFR ships
  with the JDK). `ClusteringPrefix$Kind[]` appeared with stacks through
  `SSTableCursorReader.readRowHeader`.
- Fix: `ClusteringPrefix.Kind.ALL_KINDS` — a shared cached array on the enum — used at all
  three cursor sites. Gate delta dropped **1,487,448B -> 449,488B** (~70%; ~137B/row ->
  ~41B/row residual). Iterator context unchanged at 6.69MB delta (~619B/row).
- Residual decomposition (post-fix JFR): ZERO cursor-owned scaling allocation remains. The
  449KB test-env residual = Ref$Debug stack captures (`-Dcassandra.debugrefcount=true` in the
  ant test JVM, `build.xml:1357`; production default false) + chunk-cache machinery (data-
  volume scaled) + per-key metadata. Per-compaction constants (the 4.2MB base): tombstone
  histogram spool (3MB per MetadataCollector! 2MB long[] + 1MB int[] — possible upstream
  memory-footprint observation, per-compaction not per-row, out of scope here) + component
  writers + Ref debug.
- Gate ceiling calibrated to 512KB (measured 450KB + margin; min-of-3 variance ~hundreds of
  bytes) — trips at ~+6B/row regressions. The gate is now the standing tripwire the
  garbage-free mandate requires.
- Follow-up candidates journaled, NOT fixed (out of cursor scope): the same `Kind.values()`
  pattern on the iterator deserialization path (`ClusteringPrefix.java:479, 490, 660`), and
  the 3MB-per-compaction histogram spool.

Increment-1 audit note amended: the six fixes plus finding #8's fix keep the cursor hot path
allocation-clean BY MEASUREMENT, not just by code reading.

**Possible finding #8 — RESOLVED as a duplicate of #7:** the earlier soak divergence (seed
1781061356843, Statistics.db-only, "100 vs 101") was first read as a histogram count; on
re-decode the bytes parse as two adjacent 8-byte longs — `totalColumnsSet=188` followed by
`totalRows=100 vs 101` — i.e. the same empty-static-row over-count as finding #7 (that schema
also had static columns). The histogram hypothesis is withdrawn; no separate finding #8.
Post-fix soak runs are green, consistent with a single root cause.

**Increment-1 status (2026-06-09 end of session):** findings #2-#6 all root-caused and FIXED;
all 18 differential scenarios byte-identical across all components, compressed and
uncompressed; support matrix 8/8; allowlist EMPTY — the byte-equivalence bar required zero
exceptions, only bug fixes. Branch state: fixes to `SSTableCursorWriter` (prevUnfilteredSize;
final-block width; index promotion boundary) and `CursorCompactor` (tombstone cell flags;
tie-break polarity; TTL tie-break) plus the harness are UNCOMMITTED, awaiting review.

**Next steps:** (1) wire random (`CassandraGenerators`) and Harry history scenario sources into
the harness; (2) frozen-clock support for purge/TTL boundary scenarios; (3) DESC-ordered and
multi-output (splitting) compaction scenarios; (4) upstream JIRAs for findings #1-#6 once
reviewed.

**Harness lessons (journal):**
- With `keepOriginals`, originals (or early-open clones with moved starts) can REMAIN in the
  live set as different reader instances — outputs must be identified by descriptor, never by
  instance difference, or the harness deletes input files itself.
- Obsoletion-driven file deletion is asynchronous (NonPeriodicTasks, on last ref release) —
  "file present right after compaction" proves nothing; the txn log
  (`pa_txn_compaction_*.log`) lingering in the data dir is the tell.

**Build-environment notes (not code findings):**
- Stale `build/classes` from cassandra-5.0 work shadowed `org.apache.cassandra.cql3.Terms`
  (moved to `cql3.terms.Terms` on trunk) → bogus parser compile failures. Fixed by `ant clean`.
- Stale `lib/snakeyaml-2.1.jar` vs trunk's pinned 2.4 → `YamlConfigurationLoader` compile
  failure. Fixed by `ant realclean` (forces dependency re-resolution).
  Cross-branch builds in this checkout need `ant realclean` first.

---

## Reviewer notes — defense of each finding and fix

Written for reviewers who do not have time to re-derive context. Each finding follows the same
shape: symptom → mechanism (with just enough format background) → diagnostic trail (how we know
this is the cause, not a symptom) → the fix → why the fix is complete → how to reproduce.

### Quick map: finding → pinning test → changed code

| # | Finding | Pinning test (fails without fix) | Fix location |
|---|---------|----------------------------------|--------------|
| 1 | `keepOriginals` violated by early-open obsoletion | n/a (harness-level; no production fix on this branch) | workaround in `DifferentialCompactionTester.assertCursorMatchesIterator` (disables preemptive open) |
| 2 | `previousUnfilteredSize` never written | every differential scenario; most direct: `BasicDifferentialCompactionTest.tombstonesRetainedUncompressed` | `SSTableCursorWriter`: new `previousRowStartOffset` field, `writePartitionStart`, `writeRowStart`, `writeEmptyStaticRow`, `writeRowEnd`, `writeRangeTombstone` |
| 3 | Tombstone cells flagged `IS_EXPIRING` + spurious TTL byte | `BasicDifferentialCompactionTest.tombstonesRetained[Uncompressed]` | `CursorCompactor.mergeCells` cell-flag rebuild (strict `isExpiring`, `else-if`) |
| 4 | Inverted same-timestamp value tie-break (DATA CORRECTNESS) | `EdgeCaseDifferentialCompactionTest.timestampTies` | `CursorCompactor.mergeCells` COMPARE branch polarity; `CursorCompactor.resolveRegular` TTL tie-break |
| 5 | Final index block width excludes end-of-partition marker | `EdgeCaseDifferentialCompactionTest.widePartitionCrossingIndexBlocks` | `SSTableCursorWriter.appendBIGIndex` tail width |
| 6 | No promoted index for partitions crossing `column_index_size` once | `EdgeCaseDifferentialCompactionTest.partitionCrossingOneIndexBlock` | `SSTableCursorWriter.appendBIGIndex` promotion decision |

Repro for any of them: revert the corresponding fix hunk, then
`ant testsome -Dtest.name=org.apache.cassandra.db.compaction.differential.<TestClass> -Dtest.methods=<method>`.
The harness fails with the exact byte offset / JSON line of the divergence.

### Why the harness itself is trustworthy (defending the methodology)

Reviewers should challenge the harness before the findings. The claims it depends on:

1. **Identical inputs for both paths.** Run A uses `CompactionTask(..., keepOriginals=true)`, so
   original files are never deleted; restore delists outputs (`Tracker.removeUnsafe`), releases
   readers, deletes output files only, re-opens the ORIGINAL descriptors and re-adds them via
   `addInitialSSTables`. We deliberately avoid `loadNewSSTables()` because `SSTableImporter`
   runs `resetLevel(true)`, which can rewrite input stats between runs. Run B therefore reads
   the same bytes run A read.
2. **Identical purge decisions.** The same `gcBefore` value is passed to both `CompactionTask`s.
3. **The cursor path actually runs.** Before the cursor run the harness re-creates equivalent
   scanners + controller and asserts `CursorCompactor.isSupported(...)` — the same check
   production uses at `AbstractCompactionPipeline.create` (`CompactionTask.java:263`). A
   scenario that would silently fall back to the iterator fails loudly instead of passing
   vacuously.
4. **Both paths are the full production path** — `CompactionTask.execute()`, not hand-driven
   pipelines. The only behavioral deviation is disabling preemptive open (see finding #1), which
   affects when readers become visible during compaction, not the bytes written.
5. **Comparison is two-level.** Byte comparison per component (allowlist EMPTY — never needed)
   plus a logical walk (`JsonTransformer.toJsonLines`, fixed `now`) plus stats spot-checks, plus
   `IVerifier` extended verification on every output. Byte-identity is the strongest claim and
   it is the one all 18 scenarios now meet.

### Finding 1 — `keepOriginals` is violated by early-open obsoletion (no fix on branch)

**Mechanism.** `SSTableRewriter.keepOriginals` guards exactly one call:
`transaction.obsoleteOriginals()` in `doPrepare()` (`SSTableRewriter.java:348-349`). But
`moveStarts(...)` (`:214`) — invoked as writers fill/finish when early open is active — calls
`transaction.obsolete(latest)` (`:232`) for every original whose key range is fully covered by
the new output, with no `keepOriginals` check. Obsoletion writes REMOVE records into the
transaction log; the files are deleted asynchronously (NonPeriodicTasks) when the last reader
reference is released. Net effect: `keepOriginals=true` + online transaction + early open =
original files deleted anyway.

**Why production has not noticed.** `moveStarts` returns immediately when
`transaction.isOffline() || preemptiveOpenInterval == Long.MAX_VALUE`. The shipping users of
`keepOriginals` (scrub, upgrade tooling) run offline transactions, so the unguarded path never
executes for them.

**Diagnostic trail.** Harness restore failed with "Data component is missing" for input
sstables. Directory listings taken immediately before and after `CompactionTask.execute()`
showed all input files PRESENT after the compaction, with the `pa_txn_compaction_*.log` still
in the data dir; the inputs were deleted later, logged through the normal
`BigFormat "Deleting sstable"` path — i.e. genuine obsoletion, deferred to last-ref release.
Tracing who obsoletes originals when `obsoleteOriginals()` is skipped led to `moveStarts`.

**Disposition.** Not fixed here (it is upstream `SSTableRewriter` behavior, out of scope for
cursor compaction). The harness disables preemptive open for differential runs, which is also
what makes the keepOriginals-based restore valid. Candidate for its own upstream JIRA: either
honor the flag in `moveStarts` or document/assert that `keepOriginals` requires offline
transactions.

### Finding 2 — `previousUnfilteredSize` was never written

**Format background.** Every unfiltered (row or range-tombstone marker) in the sstable data
file carries two leading vints after its flags/clustering: the body size, then
`previousUnfilteredSize`. The body size INCLUDES the byte length of the prev-size vint —
see `UnfilteredSerializer.java:207`:
`out.writeUnsignedVInt(dob.position() + TypeSizes.sizeofUnsignedVInt(previousUnfilteredSize))`,
and for markers `serializedMarkerBodySize` adds `TypeSizes.sizeofUnsignedVInt(prev)` at `:414`.
The field's value, per the reference implementation
(`SortedTablePartitionWriter.addUnfiltered`), is `offsetInPartition(current unfiltered start) -
offsetInPartition(previous unfiltered start)`, with `previousRowStartOffset` starting at 0 each
partition — so the FIRST unfiltered writes its own start offset (= the partition header length;
observed value 11 for an 8-byte key table). Static rows are special: `serializeStaticRow`
hardcodes 0 (`UnfilteredSerializer.java:152-157`) and `addStaticRow` does NOT advance the
chain, so the first regular row's prev-size spans back over the static row to the partition
start.

The field is dead on the read path today: every deserialization site skips it
(`UnfilteredSerializer.java:529, 571, 602`; design comment at `:208-209` says it exists "to
make reverse queries more efficient ... currently not used"). So the bug had no live read
impact — but cursor-compacted files differed in content from iterator-compacted files on every
single row, and any future consumer of the field would silently read garbage zeros.

**What the cursor did.** Wrote a literal `0` vint at all three write sites
(`writeRowStart`, `writeEmptyStaticRow`, `writeRangeTombstone`), with comments acknowledging it
("NOTE: if we are to write this value (which is not used)...", "TODO: previousUnfilteredSize").

**Diagnostic trail.** On an UNCOMPRESSED table (scenario added precisely to make Data.db
directly comparable), `cmp -l` showed 2601 of ~5100 byte positions differing, at a regular
~27-byte stride (the row pitch), iterator values 11/27/29/... vs cursor 0 — matching the
prev-size position in the decoded row layout, one per row.

**Fix.** `SSTableCursorWriter` tracks `previousRowStartOffset` (reset to 0 in
`writePartitionStart`). `writeRowEnd` captures `dataWriter.position()` immediately before the
flags byte (correct because between `writeRowStart` and `writeRowEnd` all writes go to side
buffers, not the data writer), computes the delta, includes
`TypeSizes.sizeofUnsignedVInt(prev)` in the size vint, writes the prev vint between size and
body — byte-for-byte the iterator's layout. `writeRangeTombstone` same. Static rows write 0 and
do not advance the chain.

**Completeness cases.** (a) first unfiltered in partition: delta from 0 = header length —
verified byte-identical post-fix; (b) static row: hardcoded 0, chain untouched — mirrors
`serializeStaticRow`/`addStaticRow`; (c) markers and rows share one chain, as in the iterator's
single `addUnfiltered` path; (d) multi-byte prev vints (rows ≥128B apart) are correct because
the value is computed, not assumed 1 byte — covered by the wide-partition scenario (~210-byte
rows).

### Finding 3 — tombstone cells carried `IS_EXPIRING` + a spurious TTL byte

**Format background.** Cell flags (`Cell.java` Serializer): `IS_DELETED=0x01`,
`IS_EXPIRING=0x02`, `HAS_EMPTY_VALUE=0x04`, `USE_ROW_TIMESTAMP=0x08`, `USE_ROW_TTL=0x10`. The
serializer writes the cell's localDeletionTime when `(isDeleted || isExpiring) && !useRowTTL`
and a TTL vint when `isExpiring && !useRowTTL`. Critically the reference sets the two flags
mutually exclusively (`Cell.java:349-352`): `if (isDeleted) ... else if (isExpiring) ...`, with
`isExpiring` meaning `ttl != NO_TTL` (`AbstractCell.isExpiring`).

**What the cursor did.** Two compounding problems: (1) it used
`ReusableLivenessInfo.isExpiring()`, which is defined as `localExpirationTime !=
NO_EXPIRATION_TIME` — i.e. "has an expiration time", which is TRUE for tombstones as well
(`ReusableLivenessInfo.isTombstone()` is `ldt set && ttl == NO_TTL`); (2) it applied the two
flags with independent `if`s. Result: every tombstone cell got `IS_DELETED|IS_EXPIRING`
(`0x07` instead of `0x05`) and an extra TTL vint `0x00`.

**Why reads still worked.** A deserializer seeing `IS_EXPIRING` reads the TTL vint; ttl=0 is
`NO_TTL`, so the reconstructed cell is still a tombstone. Logically equivalent, byte-divergent
— exactly the class the byte-level bar exists to catch.

**Diagnostic trail.** After finding #2 was fixed the remaining delta was exactly +1 byte. A
byte-alignment scan located the single insertion; hex decode of the only tombstone-cell row in
the scenario showed flags `05` vs `07` and the extra `00` in the position the serializer's
field order assigns to TTL.

**Fix.** `isExpiring = cellLiveness.ttl() != LivenessInfo.NO_TTL` (strict, equals
`AbstractCell.isExpiring`) and `else-if` structure. With strict semantics the two predicates
are mutually exclusive by construction (isTombstone requires `ttl == NO_TTL`), so the `else-if`
is defense-in-depth that also makes the code shape match `Cell.Serializer` for future readers.
Note the `useRowTTL` predicate now also uses the strict definition, matching the iterator's
`cell.isExpiring()` input to the same predicate.

### Finding 4 — inverted same-timestamp value tie-break (DATA CORRECTNESS)

**The rule being implemented** (`Cells.resolveRegular`, `Cells.java:79-128`): on timestamp tie,
(a) expiring-or-tombstone beats live (CASSANDRA-14592), (b) tombstone beats expiring, (c) both
expiring/tombstone: greater localDeletionTime wins, (d) both expiring, same expiration,
different TTLs: LOWER ttl wins (more recent `UPDATE USING TTL AND TIMESTAMP`), (e) finally:
`compareValues(left, right) >= 0 ? left : right` — the accumulated winner ("left") retains the
win unless the challenger's value is STRICTLY greater.

**What the cursor did.** Its `resolveRegular` mirrored (a)-(c) faithfully and returned COMPARE
for the value case — but: the COMPARE consumer in `mergeCells` switched the winner when
`Arrays.compareUnsigned(currentValue, challengerValue) >= 0`, i.e. when the CURRENT was greater
or equal — inverted relative to (e). And rule (d) was absent entirely (fell through to value
comparison). The in-code comment ("same timestamp handling awaits!") shows the case was known
incomplete, but the path was live and chose wrongly rather than falling back to the iterator.

**Consequence.** For equal-timestamp, both-live, different-value conflicts — same-millisecond
writes with client timestamps, bulk loads, explicit `USING TIMESTAMP` collisions — cursor
compaction kept the lexically SMALLER value. A cursor-compacted replica then permanently
disagrees with an iterator-compacted replica holding the same writes: silent divergence,
read-repair churn, and the anti-entropy outcome depends on which replica compacted with which
implementation. This is the most serious finding of the set.

**Diagnostic trail.** `timestampTies` scenario: two sstables, `INSERT ... USING TIMESTAMP
1000` with values `aaa<ck>` (flushed first) then `zzz<ck>` (flushed second). Iterator output
kept `zzz*`; cursor output kept `aaa*`. The divergence is in the value bytes of the JSON dump —
not positions or flags — so it cannot be an encoding artifact.

**Fix.** Polarity corrected: challenger wins only on `compare < 0` (strictly greater value),
current wins ties — identical decision table to `compareValues(left, right) >= 0 ? left :
right`. The buffer swap on challenger-win is retained so the winner's value always sits in
`tempCellBuffer1` (an invariant the downstream write path relies on). Rule (d) added to
`resolveRegular` with the same guard as upstream (`!leftIsTombstone`, lower TTL wins).

**Completeness cases.** equal values → left retained (matches iterator; also no observable
difference since the cells are identical); empty vs non-empty value → empty loses
(`compareUnsigned` is length-aware lexicographic, same ordering as the accessor-based
`compareValues`); chain of >2 sstables → the left/current accumulates wins pairwise in cursor
order, same fold the iterator's reducer performs; tombstone-vs-live and expiring cases never
reach the value comparison (handled in (a)-(d), all of which now exist).

### Finding 5 — final index block width excluded the end-of-partition marker

**Format background.** The big-format promoted index (`RowIndexEntry`/`IndexInfo`) stores per
index block: first/last clustering, start offset, and `writeVInt(width - WIDTH_BASE)` where
`WIDTH_BASE = 64KiB` — a SIGNED zigzag vint, so for blocks narrower than 64KiB the encoded
delta is negative and a SMALLER width encodes a LARGER final byte.

**Mechanism.** The iterator's `BigFormatPartitionWriter.finish()` calls `super.finish()` FIRST
— which writes the end-of-partition marker (`SortedTablePartitionWriter.finish`,
`:156-166`) — and THEN `addIndexBlock()`, whose width is `currentOffsetInPartition() -
indexBlockStartOffset` measured AFTER the marker byte. The reference tail-block width therefore
includes that byte. The cursor computed `(partitionEnd - partitionStart - 1) -
indexBlockStartOffset` — excluding it. Width short by 1 → zigzag delta of 2 → the observed
`0xcd` vs `0xcf` single-byte difference, sign-consistent (cursor byte larger = more-negative
delta = smaller width).

**Severity.** Low: the last block's width is informational for block-range math; a width
understated by one byte does not misdirect reads. But it is a format divergence and it masked
finding #6.

**Diagnostic trail.** `widePartitionCrossingIndexBlocks`: every component byte-identical
EXCEPT Index.db, same length, exactly one differing byte sitting immediately before the
offsets array — i.e. the last byte of the last IndexInfo entry — which is the width vint
position. The δ2-with-sign analysis matched the EOP-byte hypothesis; code inspection of both
`finish()` orderings confirmed it.

**Fix.** Width computed as `(partitionEnd - partitionStart) - indexBlockStartOffset`
(`partitionEnd` is taken after the marker is written in `writePartitionEnd`); tail block
skipped only when the size is exactly 1 — meaning only the marker remains since the last cut,
which is precisely the iterator's `firstClustering == null` no-tail condition.

### Finding 6 — missing promoted index at the block-count boundary

**Mechanism.** The iterator promotes the index when the total block count INCLUDING the tail
block is > 1 (`BigFormatPartitionWriter.finish()` adds the tail before `RowIndexEntry.create`
decides). The cursor checked `rowIndexEntriesOffsets.size() <= 1` BEFORE the tail block was
added, so a partition with exactly one mid-partition cut + a tail (total 2 blocks) wrote a
NON-promoted entry: observed 12 bytes (promoted size `0`) vs the iterator's 74 bytes
(2-entry promoted index).

**Severity.** Performance, not correctness: reads of such a partition cannot seek within it
and scan from the partition start. The affected band is every partition between 1× and 2×
`column_index_size` — with the production default of 64KiB, that is the common 64-128KiB
partition range. Reads stay correct because a missing promoted index is a legal state (small
partitions never have one); they are just slower.

**Diagnostic trail.** Predicted from code reading while fixing #5 (the `<= 1` check visibly
precedes the tail-add). First attempt at a pinning scenario was VACUOUS: it sized the partition
(~90KB) against the production default 64KiB, but the unit-test config sets
`column_index_size: 4KiB` (`test/conf/cassandra.yaml:24`), so the scenario produced ~22 blocks
and passed without touching the boundary. Re-sized to ~6KB (one cut + tail at 4KiB), the test
immediately failed with the 12-vs-74-byte Index.db divergence, confirming the prediction.
Journal lesson recorded: size corpus scenarios against the TEST config, not prod defaults.

**Fix and case analysis.** `appendBIGIndex` now computes the tail size first and decides on
`totalBlocks = cuts + (hasTail ? 1 : 0)`:

| cuts | tail? | totalBlocks | cursor (fixed) | iterator | match |
|------|-------|-------------|----------------|----------|-------|
| 0 | yes (whole body) | 1 | non-promoted | 1 block → not promoted | ✓ |
| 1 | no (cut at last unfiltered) | 1 | non-promoted | `firstClustering == null` → count 1 → not promoted | ✓ |
| 1 | yes | 2 | promoted, 2 entries | promoted, 2 entries | ✓ (the bug case) |
| ≥2 | either | ≥2 | promoted | promoted | ✓ |

The `hasTail` predicate is `tailSize > 1`, where size 1 means only the end-of-partition marker
remains since the last cut (same condition as finding #5's skip case, same iterator analogue).

### 2026-06-09 (late) — Finding #9: per-sparse-row allocation in the cursor reader (FIXED)

Found while scoping increment 2(a): the complex-column work sits on the same column-subset
machinery, which turned out to allocate per row TODAY for any row not containing every header
column (cell deletes, partial UPDATEs, sparse inserts).

**Mechanism:** `UnfilteredDescriptor.loadCommonRowFields` called
`Columns.serializer.deserializeSubset` per sparse row (allocating a new `Columns` + BTree),
which then missed `CellCursor.init`'s identity cache, re-allocating `toArray` +
`AbstractType[]` per sparse row (~154 B/sparse row measured). The site carried the authors'
own `// TODO: re-implement GC free` marker. The original gate scenario wrote full rows only —
the trigger class the gate could not see (vacuous-green lesson, 4th occurrence).

**Evidence:** new permanent gate scenario `allocationDoesNotScaleWithSparseRows` (every other
row omits one column): delta 1,280,440 B pre-fix (vs 449,488 B full-row residual) — failed the
512 KB ceiling; passes post-fix.

**Fix:** garbage-free subset decode for supersets < 64 columns: `UnfilteredDescriptor` reads
the missing-columns vint bitmask directly (wire format per
`Columns.Serializer.deserializeSubset`, bit i = i-th superset column in iteration order is
absent; 0 = all present), keeps `rowColumns` = the superset, and exposes
`missingColumnsMask()`. `CellCursor` iterates set bits of the present mask
(`Long.numberOfTrailingZeros` walk, preserving superset iteration order = serializer bit
order) over its once-initialized superset arrays. Supersets >= 64 columns keep the allocating
path (structural large-subset wire format; rare shape; documented).

**Verification:** sparse + full-row + large-file allocation gates green; all 7 differential
suites (25 tests) byte-identical — the fix is read-side only and provably does not change
outputs.

**Increment 2(a) note:** this fix is the foundation the complex-column reading extension
builds on — the mask iteration is where per-column multi-cell counts will hook in.

### Reviewer notes — Finding 9 (sparse-row subset allocation)

**Symptom.** ~154 B heap allocation per sparse row (any row lacking >=1 header column) on the
cursor read path; invisible to the original allocation gate because its scenario wrote full
rows only. Sparse-row gate scenario: delta 1,280,440 B vs the 449,488 B full-row residual.

**Mechanism.** Rows that do not carry every column are serialized with a column-subset
encoding instead of the HAS_ALL_COLUMNS flag. For supersets < 64 columns the wire format is a
single unsigned vint interpreted as a bitmask of MISSING columns: bit i set means the i-th
column of the table's column set (iteration order) is absent; encoded 0 means all present
(`Columns.Serializer.deserializeSubset`). The cursor reader decoded this by materializing a
new `Columns` object per sparse row (BTree build), which also broke `CellCursor`'s
identity-based cache, re-running `toArray` + building a fresh `AbstractType[]` per row. The
site carried the original authors' own `// TODO: re-implement GC free` comment.

**Fix.** `UnfilteredDescriptor` reads the vint mask directly and keeps `rowColumns` pointing
at the superset; `CellCursor` iterates only present columns by bit-walking:
- present mask = `~missingMask & (-1L >>> (64 - size))` — flip "missing" to "present", then
  trim the flipped garbage above bit `size-1` (`-1L >>> (64-n)` = exactly n low ones; n==0
  special-cased because Java shifts are mod 64).
- next column = `Long.numberOfTrailingZeros(presentMask)` — index of the lowest set bit,
  single CPU instruction.
- advance = `presentMask &= presentMask - 1` — clears the lowest set bit (subtracting 1
  borrows through the trailing zeros; AND kills both regions).
- validation mirrors upstream: `(encoded >>> size) != 0` -> corrupt subset bytes.

**Why ordering is correct (the key review question).** The serializer assigns bit i to the
i-th column in `Columns` iteration order; `CellCursor.columnsArray` is `columns.toArray` in
that same order; `numberOfTrailingZeros` walks bits low-to-high — so cells are consumed in
exactly the on-disk order. Verified empirically: all 7 differential suites (25 tests)
byte-identical post-fix; the change is read-side only.

**Completeness cases.** all-columns rows (mask 0 -> full present mask, identical behavior to
before); zero-cell rows (writer emits all-ones mask -> present mask 0 -> hasNext false);
0-column sets (guarded; shift-by-64 is a no-op in Java, NOT zero); supersets >= 64 columns
(structurally different "large subset" wire format -> retained allocating fallback,
documented in-code; extension path if ever needed: long[] of mask words).

**Pinned by.** `CursorCompactionAllocationGateTest.allocationDoesNotScaleWithSparseRows`
(permanent; every other row omits one column). Repro: revert the fix hunks, run that method.

**Found during** increment 2(a) scoping — complex columns sit on the same subset machinery,
so this fix is also the foundation the multi-cell extension builds on.

### 2026-06-10 — Upstream anchors: CASSANDRA-20428 / CASSANDRA-20918

CASSANDRA-20428 (reporter: Jon) is the motivating ticket for this entire effort:
`ByteArrayAccessor.read` (`new byte[length]` per value, ByteArrayAccessor.java:102-107)
drives ~40% of allocations in default compaction profiling. Its callers are cell
deserialization (UnfilteredSerializer.java:675, 708) and clustering deserialization
(ClusteringPrefix.java:649) — i.e. the VALUE materialization of the iterator path.

CASSANDRA-20918 (Nitsan Wakart) is the merged partial answer: cursor-based compaction —
THE CursorCompactor/SSTableCursorReader/Writer codebase this branch hardens. Reported
results there: 2-5x faster compaction, up to 100x allocation reduction, fixed
~15-20MB/sstable. Its known limitations in the ticket = this plan's increments
(complex cells = increment 2, BIG-only = increment 3, secondary index gap = out of scope).

Implications for prioritization:
- For COMPACTION, the 40% allocation source is already eliminated wherever the cursor path
  applies. Increment 2 is therefore not feature-parity work — it is DEPLOYING the 40% fix
  to every schema with multi-cell columns. Coverage expansion is the win multiplier.
- For the READ path, ByteArrayAccessor.read remains unaddressed; per the 20428 thread
  consensus (cursor/flyweight per Blake; byte-addressable formats per Benedict; arenas per
  Jon) the remedies map onto: the read-path cursor direction (original top-10 item #1),
  and the FFM memtable moonshot (ffm-memtable-offheap-plan.md) whose record+cursor end
  state is the same architecture.
- The quick-perf-wins list (quick-perf-wins.md) is the second-order term: bookkeeping
  around values, complementary to but much smaller than the value-materialization fixes.
- The eventual JIRA for this branch should reference both tickets: it is the continuation
  of 20918 in direct service of 20428.

### 2026-06-10 — Increment 2(a) READING COMPLETE

Multi-cell (complex) column READING in SSTableCursorReader, garbage-free. The production
gate (unsupportedMetadata) remains CLOSED — this lands inert; CursorSupportMatrixTest 8/8
confirms.

**Design:**
- Per-column inner loop: `remainingCellsInColumn` primitive counter; complex column headers
  ([deletion if row flag HAS_COMPLEX_DELETION][count vint]) consumed on column entry.
- Complex deletion: `serializationHeader.readDeletionTime(in, reusable)` delta decode into a
  per-cursor ReusableDeletionTime (LIVE when absent).
- Cell paths: grow-only byte[] scratch + length (wire: vint len + bytes —
  CollectionType.CollectionPathSerializer is THE path serializer for all complex types incl.
  UDTs, per ColumnMetadata.cellPathSerializer). The old dormant per-cell CellPath allocation
  is gone (field removed; no consumers existed).
- Contract: CellCursor.readCellHeader() returns -1/0/1 (no-cell / valueless cell / cell with
  value) + `producedCell` flag; trailing deletion-only complex columns are consumed by the -1
  path with next-unfiltered flags preloaded (verified against continueReading semantics — no
  double-read).
- Zero-cell (deletion-only) columns surface via ComplexColumnListener (reusable args), which
  is also the hook 2(b)'s merge will consume.

**Verification:**
- Reader-level differential (ComplexColumnCursorReadTest): canonical record streams
  (structure + complex headers + every cell: column, path bytes, liveness, WIRE-form value
  bytes) compared against the standard iterator read of the same sstable. Scenarios:
  multi-cell collections incl. element updates + full-collection overwrite (deletion+cells
  merged in memtable); deletion-only columns incl. a TRAILING one (the -1 path); multi-cell
  UDTs + static complex columns; sparse rows mixed with complex. Non-vacuousness guards
  (complex records must exist) after the first run came back suspiciously green-on-first-try.
- Allocation walk: small=6,592B big=11,328B delta=4,736B for 10x rows (~0.2 B/cell — the
  residual is chunk-cache noise). Ceiling 64KB (trips ~+2.7B/cell).
- Full regression ladder green: all 7 differential suites + matrix + all allocation gates
  (46 tests) — simple-column paths byte-identical, the readCellHeader contract change is
  behavior-neutral outside complex schemas.

**CASSANDRA-20428 connection:** each 2(x) step extends cursor coverage = extends the fix for
the 40%-of-allocations ByteArrayAccessor.read problem to more schemas. 2(a) is the reader
third of unlocking multi-cell schemas.

**Next: 2(b) merge** — replace the complex throw in CursorCompactor.mergeCells with a
path-ordered N-way merge: sources are path-sorted on disk; equal paths resolve via the
existing resolveRegular + value compare; complex deletions reconcile newest-wins and shadow
cells with timestamp <= deletion; the ComplexColumnListener/state surface from 2(a) feeds it.

### 2026-06-10 — Increment 2(b)+(c) implementation design (landing as ONE unit + gate flip)

Why one unit: mergeCells writes winners directly through the writer — merge alone has no
consumer; the existing differential harness is the oracle the moment merge+write+gate exist.

1. READER (SSTableCursorReader): `pauseAtEmptyComplexColumns` mode (default off, set by the
   compactor): a zero-cell complex column becomes a stoppable position (producedCell=false,
   cellColumn set, cellPathLength=-1, state CELL_END) instead of being auto-skipped; the
   normal CELL_END -> readCellHeader cycle moves past it. Trailing empties pause first, then
   take the -1 path.
2. SORT (CursorCompactor.compareByColumn): extend to (column, then producedCell empty-first,
   then type-aware path compare). Path comparator: pathType(column) = map->key type,
   set->element type, list->TimeUUIDType, UDT->unsigned-short raw compare; values compared
   via per-source REUSABLE ByteBuffer windows over cellPathBuffer (re-wrap only when the
   scratch array identity changes; position/limit per cell; zero per-cell alloc).
3. MERGE (mergeCells): remove complex throws. Column-entry detection (current output column
   tracker): on entering complex column X, scan [0, rowMergeLimit) cursors positioned at X —
   ALL sources owning X are provably at X then (column-ordered streams; X is the min) — and
   max their complexDeletions into a reusable mergedComplexDeletion; purge it via shouldPurge
   (write LIVE if purged); effective shadow for X's cells = max(activeDeletion,
   mergedComplexDeletion); existing resolveRegular + value-compare picks winners for equal
   (column, path) groups unchanged; empty-column groups contribute deletion only.
4. WRITER (SSTableCursorWriter): writeCellHeader advances nextCellIndex/missingColumns once
   per COLUMN (track last column; multi-cell columns currently break the walk); complex cell
   headers append the path (vint len + bytes from the winner's scratch) after liveness
   fields. Complex-row assembly: cells stream into rowBuffer as today; per complex column a
   reusable marker array records (rowBuffer start offset, cell count, deletion mfda/ldt);
   writeRowEnd with markers present assembles the final cell section into a second reusable
   buffer, inserting [deletion if row flag][count] at each marker and deciding
   HAS_COMPLEX_DELETION = any marker deletion non-LIVE; rows without complex columns keep
   the existing direct path verbatim (zero simple-path regression, enforced by byte
   identity). Static rows: same machinery (markers reset per row).
5. GATE: remove the isComplex rejection from unsupportedSchema (counters STAY); flip
   CursorSupportMatrixTest expectations; add complex scenarios to the deterministic corpus
   (headline: complex deletions interleaved with range tombstones; plus cross-sstable
   overwrite merges, equal-timestamp path ties, deletion-only meeting cells, UDT field
   merges); RandomDifferentialCompactionTest widens automatically (its filter IS
   unsupportedMetadata); add a complex scenario to the allocation gates.
6. ORACLE: full differential ladder byte-identical + soak; ComplexColumnCursorReadTest
   unchanged (reader contract for non-pause mode is untouched).

### 2026-06-10 — 2(b)+(c) status: logical equivalence ACHIEVED, one stats byte remains

Implementation complete per the design entry above; debugging trail (all fixed):
1. HANG/CORRUPTION (critical lesson): rowFlags |= HAS_COMPLEX_DELETION was set AFTER the
   flags byte was already written to disk — output sstables carried deletion bytes the flag
   never announced; the NEXT compaction of that output desynced (EOF mid-value). Manifested
   first on system.local (gate flip routes system tables' set<varchar> immediately!) and as
   teardown hangs from leaked refs. Fix: the complex assembly block (which decides the flag)
   moved to the TOP of writeRowEnd, before any byte is emitted. jstack of teardown + the
   CompactionExecutor stack in the test log were the diagnostics.
2. totalColumnsSet overcount: StatsAccumulation.accumulateOnColumnData counts a complex
   column ONLY if it contributed >=1 cell; deletion-only columns are in the subset encoding
   but NOT counted. Fix: statsColumnCount = columnsWrittenCount - zero-cell markers.
3. 6 extra rows under range tombstones: the merged complex deletion must be SHADOWED by the
   active deletion (ColumnDataReducer: activeDeletion.supersedes(complexDeletion) -> LIVE)
   BEFORE the purge check; without it, rows whose entire content was RT-shadowed survived
   via the force-start path. Fix applied at the column-entry scan.
4. Harness improvement: logical JSON compare now asserted BEFORE stats summary (row-level
   diffs are debuggable; that diff found bug 3 immediately).

REMAINING (both complex scenarios, only failure): single-byte divergence in Statistics.db,
offset ~4611/4608, same region (~21 bytes before the serialization header's type names,
iterator 0x60/0x38 vs cursor 0xc0/...): a deletion-time-derived stats field. statsSummary
(incl. encodingStats line) compares EQUAL, so it is a field NOT in the summary —
EncodingStats serialization itself or StatsMetadata long fields adjacent. NOTE
MetadataCollector.update(DeletionTime) is LIVE-guarded so the LIVE-update theory is dead.
Next probe: extend the harness statsSummary to include ALL StatsMetadata fields
(minLocalDeletionTime, maxLocalDeletionTime, tombstone histogram buckets, encodingStats
minTTL/minLDT/minTimestamp raw values) OR decode Statistics.db at the offset directly; the
suspect inputs are SHADOWED complex deletions (iterator: ColumnDataReducer applies delete()
BEFORE collectStats sees the row, so shadowed deletions never reach the collector — check
whether cursor's startComplexColumn update path matches exactly which deletions reach
metadataCollector.update; also check updateTTL/cell-liveness paths for complex cells).

State: EdgeCase suite 14/16 (the 2 complex scenarios fail ONLY on this byte; logical+all
other components identical); 13 commits clean through 2(a); ALL 2(b)+(c) work UNCOMMITTED
(user confirmed WIP — no commit until verification complete). After the byte: full ladder +
soak x2 + allocation gate complex scenario + journal + single commit on user instruction.

### 2026-06-10 — the last byte DECODED: cells-per-partition histogram double-count

The single Statistics.db byte (0x60 vs 0xc0 = 96 vs 192) is a BUCKET BOUNDARY of
estimatedCellPerPartitionCount: the cursor counts ~2x cells per partition, and the scenario
is ~90% complex cells -> COMPLEX CELLS ARE COUNTED TWICE. Mechanism: currentPartitionCells
increments in MetadataCollector.update(Cell) (iterator, once per cell) and
updateCellLiveness(LivenessInfo) (cursor path, MetadataCollector.java:294). All other stats
now compare equal (harness statsSummary extended with metaEncodingStats + tombstoneHist —
keep that extension).

NEXT ACTION (surgical): grep updateCellLiveness + any second per-cell collector call in
SSTableCursorWriter — find why complex cells hit it twice (suspects: called in BOTH the
public 3-arg writeCellHeader/startComplexColumn path AND the private writeCellHeader(flags,
liveness, buffer); or once per writeCellPath/writeCellValue too). Fix = exactly one
increment per cell (deletion-only columns: ZERO cell increments — verify the iterator
doesn't count complex deletions as cells). Then: rerun both complex scenarios (expect
byte-identical) -> FULL ladder (8 differential suites + matrix + gates + ComplexColumnCursorReadTest)
-> soak x2 (gate flip widens generated schemas automatically) -> allocation-gate complex
scenario -> journal 2(b)+(c) COMPLETE -> single commit (fix+tests) ONLY on user instruction.
State: 13 commits clean through 2(a); all 2(b)+(c) work uncommitted across 6 files
(CursorCompactor, SSTableCursorReader, SSTableCursorWriter, CursorSupportMatrixTest,
DifferentialCompactionTester, EdgeCaseDifferentialCompactionTest).

### 2026-06-10 — 2(b)+(c) COMPLETE, committed

Final bug: tombstone-histogram bins 96 vs 192 (NOT cells-per-partition — that theory was
wrong; decoded by hexdumping the captured Statistics.db at the diff offsets: the 5 differing
bytes were bins of estimatedTombstoneDropTime). Complex deletions were counted in BOTH
startComplexColumn AND the row assembly's writeDeletionTime (which updates the collector
internally, SSTableCursorWriter:415). Fix: removed the startComplexColumn update — the
assembly write is the single counting point, and it naturally matches the iterator
(update(LIVE) is a no-op; non-LIVE deletions always imply the row flag so always written).
Full ladder: 10 suites / 50 tests green (incl. 16/16 edge cases with both complex scenarios
byte-identical). Note for reviewers: the harness's captured-component dirs under
build/tmp/differential-compaction* made the byte-level decode possible — keep that.

### 2026-06-10 — complex allocation gate landed; 2(b)+(c) verification closed

Gate journey (methodology lesson worth keeping): the first complex gate used the 512KB fixed
ceiling at small scale and failed at delta 1,058,864B. JFR attribution
(recordComplexAllocationProfile, now a permanent diagnostic) showed ZERO per-element
allocation in cursor frames — the profile is maybeSwitchWriter's per-compaction
histogram-spool constants (2MB long[] + 1MB int[], the documented spool), which cancel in
deltas. The delta was the volume-proportional test-env residual: complex rows carry ~2.4x
the bytes of simple rows and the residual followed exactly (1.06MB vs 449KB = 2.36x). At
sub-MB inputs the residual ratio exceeds 1 B/B for ANY scenario; the 0.5 B/B calibration
only applies at multi-MB scale. Fix: the gate now measures at the large-file regime
(19/192 partitions, 4 rounds, 180-char padding): delta 7,354,344B over 17,674,940B extra
input = 0.416 B/B, green under the calibrated 0.5 ceiling.
Soak: RandomDifferentialCompactionTest green 2x with complex schemas flowing (the gate flip
widened its generation automatically); one additional run was an accord-gradle environmental
failure (1-second BUILD FAILED, recurring race between back-to-back ant invocations —
rm -rf modules/accord/accord-core/build/libs clears it).
Increment 2 (complex columns) is COMPLETE: read (19bcffd406), merge+write+gate
(0c85d26fbe), allocation gate + JFR diagnostic (this commit). Coverage: all cell types
except counters, BIG format. Next: increment 3 (BTI) — biggest multiplier, trunk default
format; then 4 (partial scanners), 5 (counters).

### 2026-06-10 — Increment 3 (BTI) scoping + design

**Reader: already format-agnostic.** SSTableCursorReader has ZERO BIG-index references — it
reads Data.db sequentially and BTI's Data.db serialization is identical. Reading BTI INPUTS
likely works today (the gate just never routes it). Step 1 proves this cheaply: a reader-level
differential test (ComplexColumnCursorReadTest pattern) against a BTI-written sstable.

**Writer: BIG coupling is concentrated and maps 1:1 onto BTI primitives.**
- Current BIG-specific block: SSTableCursorWriter:211-234 — createRowIndexEntry +
  (BigTableWriter.IndexWriter) cast + promoted-index blocks (findings #5/#6 machinery) +
  BigFormatPartitionWriter.DEFAULT_GRANULARITY threshold.
- BTI equivalents (read from BtiFormatPartitionWriter/BtiTableWriter/RowIndexWriter):
  per ~16KB block rowTrie.add(firstClustering, lastClustering, IndexInfo(blockStart,
  openMarker)); at partition end TrieIndexEntry.create(partitionStart, finishResult,
  deletion, blockCount) -> BtiTableWriter.IndexWriter.append(key, entry); single-block
  partitions SKIP the row trie (rowIndexBlockCount>1 rule, finish() returns -1) — the cursor
  must reproduce this decision exactly.
- Design: extract a CursorIndexWriter interface: partitionStart(key, deletion) /
  indexBlockBoundary(blockStartOffset, firstClusteringBytes, lastClusteringBytes,
  openMarker) / partitionEnd(dataEnd) / granularity(). BIG impl = current code moved
  verbatim. BTI impl = the primitives above. Construction: CursorCompactor:1214 already
  receives any SortedTableWriter (BtiTableWriter extends it) — only internals branch.

**The genuinely new problem: ByteComparable boundary clusterings.** BTI's row trie keys are
byte-comparable separators (RowIndexWriter.add -> comparator.asByteComparable); the cursor
has raw serialized clustering bytes + ClusteringDescriptor component windows. Build
ByteSource per component via AbstractType.asComparableBytes over the windows. Per-BLOCK
allocation (every ~16KB of output) is bounded and acceptable for the first pass — gates
measure it; optimize only if they say so. This is the same ByteComparable-without-
materialization problem the read-path moonshot has — solutions transfer.

**Verification:** BTI variants of the differential suites (subclass axis setting the
selected format — both pipelines write BTI, byte-compare INCLUDING Partitions.db/Rows.db;
tries are deterministic given identical input order), matrix flip, allocation gates BTI
axis, soak axis. Granularity must come from the format (BTI getColumnIndexSize 16KB default
vs BIG promoted thresholds).

**Order:** (a) BTI-input reader test (may already pass); (b) CursorIndexWriter extraction
with BIG impl, full ladder must stay byte-identical (pure refactor gate); (c) BTI impl +
ByteComparable boundaries; (d) gate flip + BTI differential axis + matrix + gates + soak.

### 2026-06-10 — Increment 3 step (a) DONE: BTI inputs read correctly

BtiCursorReadTest (subclasses ComplexColumnCursorReadTest with
DatabaseDescriptor.setSelectedSSTableFormat("bti") around each test): 5/5 green on first
run — every reader-level differential scenario incl. the allocation walk passes against
BTI-written sstables. The reader's format-agnosticism is now TESTED, not assumed. Note the
accord-gradle 1-second BUILD FAILED race struck again before the green run (standard fix).
UNCOMMITTED: BtiCursorReadTest.java. Next: step (b) — extract CursorIndexWriter from
SSTableCursorWriter (BIG impl verbatim, full ladder as pure-refactor gate), then (c) BTI
impl per the design entry above.

### 2026-06-10 — step (b) site map (extraction is mechanical from here)

BIG coupling sites in SSTableCursorWriter (866 lines total):
- ctor: indexWriter param (:133), indexBlockThreshold from
  getColumnIndexSize(BigFormatPartitionWriter.DEFAULT_GRANULARITY) (:146) -> granularity()
  moves into the format impl (BTI default differs).
- per-row hooks: block-start tracking (:730), block-size check + addIndexBlock (:742-744),
  addIndexBlock itself (:757-790) — BIG serializes IndexInfo entries incrementally into the
  rowIndexEntries buffer (clustering bytes + offset + width + open marker). SEAM DESIGN: the
  interface receives raw block facts (firstClusteringBytes/lastClusteringBytes windows,
  blockStartOffset, blockSize, openMarker); BIG impl serializes promoted entries as today;
  BTI impl calls RowIndexWriter.add with ByteComparable boundaries.
- partition end: createRowIndexEntry block (:211-280) incl. the (BigTableWriter.IndexWriter)
  cast (:223) and the tail-counted promotion decision (findings #5/#6 — preserve comments).
  BTI equivalent: TrieIndexEntry.create + IndexWriter.append + the rowIndexBlockCount>1
  skip rule.
- import RowIndexEntry (:51) moves with the BIG impl.
Pure-refactor gate: full 10-suite ladder byte-identical after extraction with BIG impl.
Committed so far this increment: c90b363197 (BTI reader verification).

### 2026-06-10 — step (b) seam design REFINED after full code read

KEY CONSTRAINT discovered: BIG serializes block-boundary clusterings into rowIndexEntries
AT ROW TIME (updateMetadataAndIndexBlock :722-745 — first clustering eagerly, last via
reusable rowIndexEntryLastClustering copy) because UnfilteredDescriptors are transient.
BTI faces the identical constraint -> the seam is EVENT-shaped, not block-batch:

  interface CursorIndexWriter {
      void startPartition(long partitionStartPosition);
      void rowWritten(UnfilteredDescriptor d, long rowStart, long rowEnd);  // block tracking + boundary capture
      void endPartition(byte[] key, int keyLen, long partitionStart, int headerLength,
                        DeletionTime partitionDeletion, long partitionEnd);
      int granularity();
  }

Shared-state inventory to untangle (ALL must be accounted for in the move):
- openMarker (RT open deletion): read by addIndexBlock (:786-789) — pass via constructor
  ref or per-row arg; the writer owns it.
- partitionStart / currentOffsetInPartition: needed by impls — pass via startPartition.
- updateIndexBlockStartOffset: ALSO called from static-row/empty-static paths (grep before
  moving!) — those call sites become impl-internal or seam events.
- getIndexBlockStartOffset (:193): exposed to CursorCompactor — find its consumer and route
  through the seam.
- BIG impl state moving verbatim: rowIndexEntries, rowIndexEntriesOffsets,
  rowIndexEntryOffset, rowIndexEntryLastClustering, hasDistinctLastClustering,
  indexBlockStartOffset, writeClusteringToRowIndexEntries, addIndexBlock, appendBIGIndex
  (bloom filter + summary + tail-counted promotion with findings #5/#6 comments intact).
- BTI impl captures boundary clusterings into ByteComparable-convertible scratch at
  rowWritten time (rowIndexEntryLastClustering pattern), RowIndexWriter.add at block
  boundaries, TrieIndexEntry.create + IndexWriter.append at endPartition, blockCount>1 rule.
Gate: full 10-suite ladder byte-identical with BIG impl (pure refactor), THEN BTI impl.
Committed this increment: c90b363197 (BTI reader verification, step (a)).

### 2026-06-11 — SCOPE (user directive)

This branch is COMPACTION ONLY: the cursor-compaction increments (currently increment 3,
BTI). The FFM memtable investigation/plan (ffm-memtable-*.md in this directory) is a
separate future project — NOT this branch, do not let it influence this work. Everything
here targets JDK 21+: no FFM/Arena APIs, no JDK 22+ assumptions anywhere in the compaction
code or tests (none exist today — the cursor code is plain Java and stays that way).

### 2026-06-11 — step (b) COMPLETE: CursorIndexWriter extraction, byte-identical

CursorIndexWriter (event seam: startPartition/rowWritten/staticRowWritten/endPartition,
base owns partitionStart + indexBlockStartOffset block clock) + BigCursorIndexWriter
(verbatim move: promoted-block buffer machinery, tail-counted promotion with findings
#5/#6 comments, bloom/summary appends). SSTableCursorWriter 866->710 lines. One deliberate
substitution: the tail addIndexBlock passes DeletionTime.LIVE where old code read
this.openMarker — equivalent by the RTs-close-before-partition-end invariant, VERIFIED by
the ladder. Compile-clean first try; full 11-suite ladder (57 tests incl. both reader
suites) green byte-identical. UNCOMMITTED: CursorIndexWriter.java, BigCursorIndexWriter.java,
SSTableCursorWriter.java changes. Next: step (c) BtiCursorIndexWriter.

### 2026-06-11 — keepOriginals/early-open fix: SCOPE CHANGE (user directive)

Finding #1 (SSTableRewriter.moveStarts obsoletes originals despite keepOriginals=true under
early open) will be FIXED ON THIS BRANCH once the current BTI work completes — NOT as a
standalone 4.1/5.0 ticket ("doesn't matter for 5.0, we don't use it, nobody cares").
keeporiginals-jira-draft.md remains the technical reference (mechanism, repro, fix shape)
but its backport framing is obsolete. The harness's disable-preemptive-open workaround can
be REMOVED once the real fix lands (good validation: harness should pass with early open
enabled again).

### 2026-06-11 — step (c) facts gathered (BtiCursorIndexWriter inputs)

- RowIndexWriter.add(ClusteringPrefix first, ClusteringPrefix last, IndexInfo info)
  (RowIndexWriter.java:66-75): computes separator = separatorGt(prevMax,
  asByteComparable(first)); requires info.openDeletion != null; tracks prevMax/prevSep.
  asByteComparable allocates per block — acceptable (per-16KB) per design; the cursor impl
  needs first/last as ClusteringPrefix: either reusable accessor-backed prefix views over
  two captured ClusteringDescriptors (BIG's first+last capture pattern) or feed the trie a
  precomputed ByteComparable (would need a RowIndexWriter variant — avoid; use prefixes).
- bti IndexInfo(offset, openMarker) is BTI-specific (check import in
  BtiFormatPartitionWriter); rowTrie lifecycle: ONE RowIndexWriter per partition writer,
  reset per partition (verify RowIndexWriter.reset exists; BtiFormatPartitionWriter.reset).
- BtiTableWriter.IndexWriter (BtiTableWriter.java:161-183): rowIndexWriter SequentialWriter
  (Rows.db), partitionIndexWriter + PartitionIndexBuilder (Partitions.db), flush listeners
  wired. append(DecoratedKey, TrieIndexEntry) -> partition trie needs the KEY as
  ByteComparable: the cursor writer's endPartition currently receives raw key bytes only.
  LEAD: PartitionDescriptor already wraps a reusable key
  (partitioner.createReusableKey(0)) — extend the seam's endPartition to carry the
  descriptor's reusable DecoratedKey (CursorCompactor has it at partition scope) instead of
  reconstructing/decorating from bytes (which allocates per partition).
- TrieIndexEntry.create(partitionStart, finishResult, deletion, blockCount); blockCount>1
  rule: finish/complete returns -1 (no row trie) for single-block partitions — replicate
  exactly (BtiFormatPartitionWriter.finish:93-110).
State: step (b) committed-pending (3 files uncommitted: CursorIndexWriter,
BigCursorIndexWriter, SSTableCursorWriter changes — ladder green); step (c) next with the
above; then gate flip + BTI differential variants; then task #14 (keepOriginals fix here,
no backports).

### 2026-06-11 — step (c) implementation design FINAL

- PLACEMENT: BtiCursorIndexWriter goes in org.apache.cassandra.io.sstable.format.bti —
  RowIndexWriter and BtiTableWriter.IndexWriter are package-private; no visibility changes
  to upstream files. CursorIndexWriter base is public, so cross-package extension works.
- RowIndexWriter lifecycle: construct once (comparator, rowIndexWriter SequentialWriter,
  version), reset() per partition, add(first, last, IndexInfo) per block, complete(endPos)
  -> root at partition end (or skip when blockCount <= 1 -> finishResult -1).
- THE CLUSTERING VIEW PROBLEM (the meat): ClusteringDescriptor stores raw serialized
  clustering bytes + kind + bound count — NO component offsets. RowIndexWriter.add needs
  ClusteringPrefix (comparator.asByteComparable iterates get(i) per component). Design: a
  reusable ClusteringPrefix VIEW class that lazily parses component boundaries from the
  descriptor's buffer (the parse logic exists in SSTableCursorReader's clustering compare,
  :683-743 — same format: 32-component header words + per-component size headers) into
  reusable offset/length int arrays; get(i) returns a reusable ByteBuffer window
  (re-positioned per call — safe because asByteComparable consumes components
  sequentially; separatorGt compares two DIFFERENT prefixes so each needs its OWN view).
  Two views needed (first + last boundary captures, mirroring BIG's eager-first +
  lastClustering-copy pattern with two dedicated ClusteringDescriptors).
- endPartition: needs DecoratedKey for partitionIndex.addEntry ByteComparable — extend the
  seam endPartition signature with the reusable DecoratedKey from PartitionDescriptor
  (CursorCompactor passes it; BIG impl ignores it and keeps using raw bytes).
- TrieIndexEntry.create(partitionStart, finishResult, deletion, blockCount) + append.
- bti IndexInfo = RowIndexReader.IndexInfo(offset, openDeletion) — openDeletion must be
  non-null (LIVE when none), per the assert in add().
- Then: ctor dispatch (instanceof BtiTableWriter), gate flip (CursorCompactor:145), BTI
  variants of differential suites (format-switch subclass pattern from BtiCursorReadTest,
  byte-compare ALL components incl. Partitions.db/Rows.db), matrix, gates, soak.

### 2026-06-11 — step (b) committed; FUTURE PHASE directive (user)

Step (b) committed. USER DIRECTIVE for after BTI + early-open work: a dedicated
test-hardening phase — "provably rock solid": various mixes of tombstones (cell/row/range/
partition/complex interleavings), ALL collection types, counters, etc. To be PLANNED
TOGETHER after the current work completes. This supersedes nothing — it extends the
verification spine; the differential harness + soak + gates are the foundation it builds on.

### 2026-06-11 — step (c) NEARLY DONE: BTI differential 14/16 -> 2 failures remain

Implemented: ClusteringDescriptorPrefixView (reusable parse + snapshotOf factory),
BtiCursorIndexWriter (bti package), seam endPartition unified to carry DecoratedKey + raw
bytes, ctor dispatch on instanceof BtiTableWriter, gate flip accepts BtiFormat,
BtiDifferentialCompactionTest (EdgeCase corpus under BTI).

TWO REUSABLE-ESCAPE BUGS found and fixed (lesson: trie APIs RETAIN lazy references):
1. RowIndexWriter retains prefixes across add() (prevMax/prevSep lazy) -> block boundary
   prefixes are now snapshotOf copies (per ~16KB block, bounded).
2. PartitionIndexBuilder retains the key AND ITS TOKEN — ReusableDecoratedKey mutates its
   token in place (recalculateToken) -> snapshot clones bytes AND recomputes an immutable
   token via partitioner.getToken(copy). This took two passes: bytes-only snapshot still
   failed because the shared token mutated.

REMAINING: 2/16 — partitionCrossingOneIndexBlock + widePartitionCrossingIndexBlocks fail
with Rows.db BYTE divergence (same lengths! offsets 5 and 1334; structurally identical
tries, few payload bytes differ). Suspects: (a) IndexInfo payload — openDeletion encoding or
offset; (b) separator content — my boundary clusterings vs the iterator's (check: iterator
uses Unfiltered.clustering() for first/last incl. RT marker bounds; my descriptor capture
should match — verify the (4010)-style separator bytes by decoding Rows.db at offset 5 of
the 30-byte case, TINY file = decode by hand against RowIndexReader format); (c) the
firstClustering eager-capture condition — BIG captured first at block START (offset==
indexBlockStartOffset check), my BTI blockOpen flag captures first row AFTER a cut —
off-by-one-row on block boundaries vs BtiFormatPartitionWriter (it sets firstClustering
in addUnfiltered BEFORE the size check, then cuts AFTER — verify my rowWritten order
matches: capture-then-maybe-cut ✓ same... but the iterator's addIndexBlock uses
firstClustering/lastClustering BEFORE adding the current row?? READ BtiFormatPartitionWriter
addUnfiltered/super.addUnfiltered ordering precisely).
The 30-byte Rows.db case is the debugging gift: hand-decode both versions.
All uncommitted: view, BtiCursorIndexWriter, seam changes, gate flip, BTI test.

### 2026-06-11 — increment 3 COMPLETE; spool dropped from concerns

BTI verification finished: all 14 suites green (BTI edge corpus 16/16 byte-identical incl.
tries, BTI soak green, BTI gates green with format-aware ceiling — BTI pays ~2KB/partition
inherent index cost, iterator pays same; sparse gate 449KB BIG -> 560KB BTI, ceiling 768KB
justified in-code). Root causes this increment: 2 reusable-escape bugs (trie APIs retain
lazy refs; ReusableDecoratedKey mutates its token in place) + endPos off-by-one
(SortedTablePartitionWriter.finish captures length pre-EOP-marker).
The 3MB histogram spool is DROPPED from the branch concern list (user: not concerned).
UNCOMMITTED (~10 files): seam + Big/Bti index writers + view + writer/compactor changes +
gate flip + 4 BTI test files + matrix + gate ceiling refactor.
NEXT (user-sequenced): commit -> task 14 (keepOriginals fix here) -> task 15 (test-hardening
planning WITH Jon; fresh session recommended, this journal is the handoff).

### 2026-06-11 — task 14 COMPLETE: keepOriginals/early-open fix (ready to commit)

Fix: one-line guard in SSTableRewriter.moveStarts (~:231) — fully-covered originals are no
longer obsoleted when keepOriginals=true (only the bulk obsoleteOriginals call honored the
flag before). Per Jon: fixed HERE, no 4.1/5.0 backports.

Validation, three layers:
1. SSTableRewriterKeepOriginalsTest (DEDICATED class): online txn + early open +
   keepOriginals=true full rewrite -> original Data.db survives waitForDeletions. Own class
   because kept originals leave liveDiskSpaceUsed residue that poisons the shared fixture
   (first attempt in SSTableRewriterTest cascaded expected:<0> but was:<7552> into 19 tests)
   — and NEVER manually delete tracker-known files (NPEs the txn log machinery). Per-class
   forked JVMs make the residue harmless.
2. SSTableRewriterTest 21/21 green — all its paths use keepOriginals=false, unaffected.
3. The harness's disable-preemptive-open workaround REMOVED: every differential run now
   exercises keepOriginals=true under live early open — full ladder 14 suites / 82 tests
   green. Every future differential run is a standing regression test for this bug.

UNCOMMITTED: SSTableRewriter.java (the guard), SSTableRewriterKeepOriginalsTest.java (new),
SSTableRewriterTest.java (no net change — clean), DifferentialCompactionTester.java
(workaround removal). Single commit on Jon's word. Then: task 15 test-hardening planning
WITH Jon (fresh session recommended, this journal is the brief); increments 4 (partial
scanners) / 5 (counters) / 6 (GarbageSkipper, optional) remain on the larger plan.

### 2026-06-10 — task 14 COMMITTED; task 15 test-hardening phase — PLAN (decided with Jon)

Task 14 committed as 25e71051ec ("Honor keepOriginals in SSTableRewriter.moveStarts").
Post-hoc verification of the full early-open ladder: all 14 eo_*.log files complete, 14
suites / 82 tests, zero failures (the stale per-method fail=1 XMLs in build/test/output are
leftovers from the 2(b) debugging hunt, hours older than the ladder run).

**Hardening sequencing (Jon):** runs NOW, before increments 4 (partial scanners) and
5 (counters) — wider surface first prevents regressions and surfaces issues while they are
still cheap to fix. Counter coverage stays a written spec (H9) until increment 5 lands.

**Corpus audit summary (start of task 15):** 14 differential/gate suites, 82 tests,
byte-identity bar, allowlist still EMPTY, both formats via subclass axis, allocation gates
on 4 scaling axes, soak + Harry randomized backstops. Strong: tombstone basics (row/range/
partition/cell), overlapping/adjacent RTs, DESC + composite clusterings, index-block
boundaries, exact purge boundaries, partial-set purge, multi-output switching, statics incl.
empty static rows, frozen collections, basic multi-cell map/set/UDT, complex deletion × RT,
timestamp ties (simple cells), sparse rows, empty/null values, 3 compressors, direct I/O.

**Gap classes found:**
- A. Tombstone/liveness shapes never exercised: cell-path tombstones (DELETE m['k'] etc. —
  tombstone cells WITH CellPath through the path-merge); equal-timestamp ties INSIDE complex
  columns (finding-#4 class on increment-2 code, only pinned for simple cells today);
  row-liveness shapes (UPDATE-only rows = no pk liveness; INSERT pk/ck-only = liveness with
  zero cells; row TTL vs cell TTL mixes; expiring-vs-live same-ts tie = CASSANDRA-14592 rule
  (a), implemented in resolveRegular but never pinned differentially); TTL on collection
  cells + expired-to-tombstone inside complex columns; shadowable row deletions (MV extended
  flag — audit needed, latent corruption risk if gate admits views); complex × index-block
  crossings; static complex columns in the differential corpus.
- B. Soak much narrower than the deterministic corpus: NO range/cell/complex deletes, TTLs,
  timestamp collisions, null/empty values; single-column partition keys only.
- C. Structural rungs: no second-generation compaction (the 2(b) flag bug only manifested
  when the NEXT compaction read the corrupt output); >64-column superset path never
  byte-compared; no RT-dense allocation gate (ReusableDeletionTime pool untripwired).
- D. Type edges: nested types, UDT-with-collection fields, tuples, vector, duration,
  multi-component partition keys.
- E. Deferred: TTL-at-nowInSec boundary (see H8 below), counters (increment 5).

**Work items and milestone/commit plan (one commit per milestone, soak after every
milestone — Jon's standing directive; thousands-of-iterations soak before JIRA posting):**

| Milestone | Items | Content |
|---|---|---|
| M1 | H1+H7 | Soak widening (range/cell/complex deletes, TTLs, USING TIMESTAMP collisions, null/empty domain, composite partition keys) + property-gated example count |
| M2 | H2 | Second-generation compaction rung in the harness, applied to the edge corpus (BTI inherits) |
| M3 | H3 | Tombstone-interleaving deterministic scenarios (gap class A) |
| M4 | H4 | Shadowable/MV audit -> matrix pin or scenario (or finding+fix if gate admits views) |
| M5 | H5 | Type matrix: nested/UDT-field/tuple/vector/duration + >64-col differential scenario |
| M6 | H6 | RT-dense allocation gate |

**H8 (frozen-clock TTL boundary) — demoted to OPTIONAL by code-reading equivalence proof:**
1. Expiry boundary: iterator live iff nowInSec < localDeletionTime (Cell.java:182); cursor
   expired iff nowInSec >= localExpirationTime (ReusableLivenessInfo.java:74). Exact
   complements — same boundary second, same direction.
2. Expired->tombstone conversion: iterator BufferCell.tombstone(.., localDeletionTime()-ttl())
   (AbstractCell.purge:99); cursor ttlToTombstone(): LET -= ttl, ttl = NO_TTL
   (ReusableLivenessInfo.java:77-82). Identical resulting tombstone.
3. Purge decision: both reduce to shouldPurge(timestamp, LET - ttl). The iterator's extra
   pre-conversion check against LET is strictly subsumed: smaller LDT is uniformly easier to
   purge under every clause of Purger.shouldPurge (CursorCompactor.java:1542-1547).
4. Row liveness: both paths literally share DeletionPurger's default
   shouldPurge(liveness, nowInSec) (DeletionPurger.java:31-34), reached from
   CursorCompactor.java:558 and BTreeRow.purge alike.
If ever implemented, the cheap route is NOT a frozen JVM clock: nowInSec is a local at
CompactionTask.java:260; a protected nowInSec() seam + the harness TaskFactory pins it per
run deterministically (S effort, one tiny production seam). Do only if free; otherwise this
analysis is the documented exclusion for the JIRA.

**H9 — counter hardening spec (executes when increment 5 lands):**
- Shard merge scenarios: same counter cell across N sstables (global shards, differing
  clocks/values), verifying CounterContext reconciliation parity byte-for-byte.
- Counter tombstones: DELETE on counter columns, counter cell vs tombstone merges, purge.
- Counter + static counter columns; counter columns in wide partitions crossing index blocks.
- Soak: remove the COUNTER TypeKind filter from RandomDifferentialCompactionTest generation;
  matrix flip countersUnsupported -> supported; allocation gate counter scenario (counter
  contexts are byte[]-manipulation-heavy — prime allocation-regression territory).
- Local/remote shard handling: pre-6.0 legacy shards are out of scope (cursor only reads
  current-version sstables).

**Type scope decisions (Jon):** vector and duration IN (vector: matrix pin + scenario +
soak; duration: folded into an existing scenario's schema). DynamicCompositeType OUT,
custom types (fully-qualified-classname AbstractType columns) OUT — opaque-blob shapes with
no cursor-specific machinery; "someone else's problem."

**Soak cadence (Jon):** property-gated example count; modest default per-milestone as the
standing regression check; thousands of iterations as the pre-JIRA validation run, with the
final N quoted in the reviewer notes.

### 2026-06-10 — M1 in progress; FINDING #10 (DATA ORDERING BUG, found by the widened soak)

The widened soak's FIRST 25-example BTI run failed at example 5 (seed 99303954147053,
reproducible): Statistics.db 8 bytes longer on the cursor side, all other components
byte-identical. Decoded via sstablemetadata on the captured components (rename captures to
da-1-bti-* in a scratch dir — works): **Covered clusterings** differ — the iterator's max
clustering bound has an EMPTY first component; the cursor's carries bigint
-8881229868369476487 and entirely different remaining components. Schema (from seed replay):
first clustering column **ReversedType(LongType)** — DESC — and the soak's new NULL value
domain maps to EMPTY BUFFERS on clustering columns. (The droppable-tombstones delta seen in
sstablemetadata output was a red herring: that estimate uses wall-clock gcBefore at TOOL
runtime; the two invocations ran seconds apart.)

**Root cause:** `ClusteringComparator.compare(AbstractType[], ByteBuffer, ByteBuffer, int)`
(the raw-descriptor comparison used by the cursor path) decided null/empty/valued component
ordering purely from the serialized flag bits: null (0b10) < empty (0b01) < valued (0b00),
NEVER consulting the type. The iterator's reference (`compareComponent`) handles null
outside the type (nulls first, always) but routes empty-vs-valued THROUGH the type — and
`ReversedType.compareCustom` swaps operands, so on a DESC column empty sorts AFTER values.
Every base type sorts empty first, so the flags rule is correct for ASC and INVERTED for
DESC empty-vs-valued.

**Severity: DATA ORDERING (corruption class), not stats-only.** The same compare backs the
merge's source ordering (`CursorCompactor.java:1442`). Same partition, DESC column, empty
clustering in one sstable and valued in another → the cursor merge emits rows in the wrong
order — an output sstable inconsistent with its comparator. Verified: the pinning scenario
WITHOUT the fix fails with a LOGICAL (row-level JSON) divergence, not just the stats byte.
The soak example only tripped the cross-partition stats variant (covered-clustering min/max
compares clusterings across partitions, where the merge never does). Trigger window: DESC
clustering column + empty (zero-length) clustering values — legal, rare (empty values bind
through drivers; pre-CQL data).

**Fix:** the flags branch flips empty-vs-valued (not null) comparisons for
`type.isReversed()` (ClusteringComparator.java, comment explains the asymmetry). Zero
allocation — flag arithmetic only.

**Pinned by:** `EdgeCaseDifferentialCompactionTest.emptyClusteringValuesDescending`
(same-partition merge variant + cross-partition stats variant in one scenario; FAILS
pre-fix with a logical divergence) and `emptyClusteringValuesAscending` (guards the
unflipped ASC ordering). BTI inherits both via BtiDifferentialCompactionTest. Verified:
seed 99303954147053 green post-fix under BOTH formats.

**Harness improvement landed alongside:** `-Dcassandra.test.differential.seed=N` makes any
failing seed example 0 of the next run — no more code edits to reproduce (pass via
`-Dtest.jvm.args="-Dcassandra.test.differential.seed=N"`).

### 2026-06-10 — M4 audit (shadowable deletions / MV) — RESULT: no code gap, coverage gap only

- **Shadowable row deletions** (extended flag HAS_SHADOWABLE_DELETION): NO producers left in
  src/java — `Deletion.shadowable` has zero callers; modern MV maintenance uses
  expired-liveness rows (EXPIRED_LIVENESS_TTL, CASSANDRA-13409) instead. Legacy sstables
  containing the flag cannot reach the cursor (older-version gate, CursorCompactor:134). The
  reader FAIL-LOUDS on the flag (`SSTableCursorReader.validateStaticRowFlags:968` throws
  UnsupportedOperationException; non-static extended flags mid-partition →
  corruptSSTable) — no silent-corruption path exists.
- **Strict liveness** (the MV-specific purge rule): ALREADY implemented in the cursor —
  `CursorCompactor.enforceStrictLiveness` (:216, :308, :570) mirrors
  PurgeFunction/CompactionIterator (:474).
- **Remaining M4 work:** a differential scenario over an actual materialized-view table
  (exercises strict liveness + view row shapes end-to-end) — view tables ARE admitted by the
  gate (no isView() rejection, deliberately fine given the above).

### 2026-06-10 — M1 COMPLETE (committed)

- 587612cd85 — finding #10 fix (ClusteringComparator reversal-aware absent-component
  ordering) + the two pinning scenarios (DESC fails pre-fix with a LOGICAL row-level
  divergence — corruption class confirmed; ASC pins the unflipped order).
- dafd000ee0 — soak widening (UPDATE rows, pk-only inserts, long TTLs, USING TIMESTAMP
  collision pool, null domain incl. empty clustering buffers, single-sided + prefix range
  deletes, multi-column + static cell deletes, composite partition keys) + knobs:
  cassandra.test.differential.examples=N, cassandra.test.differential.seed=N (both via
  -Dtest.jvm.args).
- Validation: failing seed 99303954147053 green post-fix on BOTH formats; pin verified to
  fail pre-fix (stash/run/pop); full 14-suite ladder green (now 86 tests — EdgeCase and BTI
  variants gained 2 scenarios each).
- Soak generator space note (observed in the failing example's schema): the generator
  already emits VectorType and DynamicCompositeType inside clustering types and
  DynamicCompositeType regular columns — vectors/exotics are ALREADY flowing through the
  soak even before M5's explicit scenarios.

Next: M2 (second-generation rung).

### 2026-06-10 — M2 COMPLETE (committed 96d95f78a7); M3 in progress

M2: assertCursorMatchesIteratorAcrossGenerations + commitCompaction in the harness; all 18
edge scenarios switched (BTI inherits). Validation: EdgeCase + BtiDifferential + both soaks
green. Gen 2 re-compacts genuinely cursor-produced outputs — the 2(b) flag-desync bug class
now fails in-suite instead of in production's next compaction.

**Tooling lessons (this session):**
- `ant testsome -Dtest.methods=<list>` embeds the METHOD LIST in the result XML filename —
  8 method names blew past the 255-byte filename limit, the formatter died
  (FileSystemException: File name too long) AFTER the tests ran, and ant surfaced a
  synthetic 0-second "Timeout occurred" entry for one method. Keep method batches short
  (filename prefix is ~91 chars; stay under ~3-4 methods per invocation).
- This shell's grep is ugrep: the big junit logs contain binary bytes (hex dumps), which
  silently suppress ALL matches unless `-a` is passed, and `\|` alternation inside basic
  patterns matches nothing — use `grep -a -E`. Both produced convincing false "no results"
  during finding-#10 triage.

### 2026-06-10 — FINDING #11 (stats poisoning via ReusableDeletionTime.reset, found by M3 scenario)

The new `staticComplexColumns` scenario failed: cursor `minTimestamp=Long.MIN_VALUE` (vs the
real minimum), `encodingStats.minTimestamp` collapsed to the 2015 epoch, tombstone histogram
content diverged. Logical dump identical — stats-only.

**Root cause (pinned by a direct unit probe before fixing):**
`DeletionTime.ReusableDeletionTime.reset(long mfda, long ldt)` classified
`ldt == Cell.NO_DELETION_TIME` (Long.MAX_VALUE — the canonical "no deletion" long) as
INVALID, storing the invalid uint marker (0xFFFFFFFE) instead of the LIVE marker
(0xFFFFFFFF). So `reset(LIVE.markedForDeleteAt(), LIVE.localDeletionTime())` produced an
object with `isLive() == false`. The single caller of that overload is the complex-row
assembly's marker write (`SSTableCursorWriter.writeRowEnd:479`): when a row has
HAS_COMPLEX_DELETION, EVERY complex column serializes its deletion — LIVE included — and
each LIVE marker's "deletion" then sailed past `MetadataCollector.update(DeletionTime)`'s
`!isLive()` guard: `updateTimestamp(Long.MIN_VALUE)` + `updateLocalDeletionTime(4294967294)`
+ `updateTombstoneCount()` per live marker.

**Bisection trail:** probes p1 (static-only partition) PASS, p2 (late static) PASS,
p3 (static live complex + deleted complex) FAIL, p4 (REGULAR-row replica of p3) PASS,
p5 (p3 minus regular rows) FAIL, p7 (static deletion alone, no live neighbor) PASS,
p0 (direct reset(LIVE longs) round-trip) FAIL — mechanism proven independent of the
differential harness. Curious wrinkle, not fully chased: the regular-row replica (p4) does
NOT trip the collector despite the same assembly code — post-fix this is moot (the reset
now round-trips correctly and ALL probes pass), but it suggests the regular path's live
markers take a different write order; worth remembering if a related divergence ever shows.

**Severity:** stats-only (minTimestamp drives compaction-strategy decisions and tombstone
estimates feed droppable-tombstone ratios — wrong inputs to those heuristics, no read-path
wrongness). Trigger: any cursor-compacted STATIC row containing a complex deletion next to
a live complex column.

**Fix:** reset(long,long) maps NO_DELETION_TIME -> LIVE marker, mirroring
Cell.deletionTimeLongToUnsignedInteger exactly; negative/out-of-range values still classify
invalid. Zero allocation.

**Pinned by:** new `DeletionTimeTest.resetWithLiveLongsStaysLive` (the p0 probe made
permanent, fails pre-fix) + `EdgeCaseDifferentialCompactionTest.staticComplexColumns`
(fails pre-fix at the stats rung; BTI inherits).

### 2026-06-10 — M3 COMPLETE (d5bc7fb5c3); M4 COMPLETE (d62c7339f8)

M3: 8 tombstone-interleaving scenarios (cell-path tombstones, complex ts-ties, row-liveness
shapes, row-vs-cell TTL mix, expiring-vs-live ties, collection-cell TTLs, complex × index
blocks, static complex) + the finding-#11 DeletionTime fix + DeletionTimeTest, one commit.
Validation: unit pin + EdgeCase (26 scenarios) + BTI variant + both soaks green.

M4: MaterializedViewDifferentialCompactionTest + BTI subclass — compacts the VIEW table
differentially (strict liveness, view-partition moves, tombstones-on-tombstones, two
generations, both formats). Green on first run: view-table compaction is byte-identical
through the cursor. The audit (no code gap: no shadowable producers, version gate blocks
legacy data, reader fail-louds, enforceStrictLiveness already mirrored) is recorded two
entries up. Ladder is now 16 suites.

M5 in progress: matrix pins (vectorAndDurationSupported, nestedTypesSupported) + EdgeCase
scenarios nestedTypes / vectorAndDuration / over64Columns (the >64-column large-subset wire
format byte-compared for the first time).

### 2026-06-10 — FINDING #12 (OUTPUT CORRUPTION, >64-column tables — found by M5 scenario)

`over64Columns` died with CorruptSSTableException (EOF mid-cell) verifying the CURSOR's own
output — gen 1, single- and multi-input alike (probes q1/q2/q3 all reproduce; nestedTypes
and vectorAndDuration pass). The cursor WRITES unreadable sstables for >64-regular-column
tables whenever sparse rows take the present-index encoding mode.

**Root cause — two independent bugs in `SSTableCursorWriter.encodeLargeColumnsSubset`
(cursor-original code, never exercised in present-index mode by any pre-existing test;
the old simple-suite's over-64 test deletes a FEW columns from full rows = missing-index
mode only):**
1. The trailing-present loop's bound was the last missing index itself
   (`for (; presentIndex < missingIndex; ...)` right after `presentIndex = missingIndex+1`)
   — vacuously empty, so present columns sorting AFTER the last missing column were DROPPED
   from the encoding. `Columns.Serializer.deserializeLargeSubset` then reads the declared
   present-count of vints, consuming row-body bytes as column indices — desync, EOF.
2. Mode selection used `missing > supersetCount/2` where the reference serializer AND the
   deserializer use `presentCount < supersetCount/2` — equivalent for even superset sizes,
   FLIPPED for odd sizes at exactly missing == supersetCount/2 + 1, i.e. the encoder picks
   present-mode while the decoder reads missing-mode. Same corruption class.

**Severity: HIGHEST of the branch so far — output corruption on the currently supported
surface.** The gate does not exclude >64-column tables (finding #9 documented the
large-subset path as the allocating-but-functional fallback — functional was true only for
READING; the WRITE side was broken). Any production cursor compaction of a wide-schema
table with sparse rows in present-index shape would have produced sstables that crash every
subsequent read/compaction of those rows.

**Fix:** both conditions corrected to mirror Columns.Serializer exactly (tail loop runs to
columns.length; mode = presentCount < supersetCount/2). Flag/index arithmetic only — no
allocation.

**Pinned by:** `EdgeCaseDifferentialCompactionTest.over64Columns` (sliding sparse windows,
wrap-around rows exercise the dropped-tail shape; fails pre-fix with CorruptSSTableException)
and `over64ColumnsOddSupersetBoundary` (71 columns, rows at 34/35/36 present straddle the
mode boundary). BTI inherits both; both run two generations.

### 2026-06-10 — M5 COMPLETE (1be942ded8)

nestedTypes + vectorAndDuration green on first run; over64Columns found finding #12 (above),
fixed in the same commit with the boundary pin. Matrix pins landed
(vectorAndDurationSupported, nestedTypesSupported). Validation: full EdgeCase + BTI variant
+ matrix + both soaks green. EdgeCase is now 30 scenarios per format: the original 16,
finding-#10 pins (2), M3 interleavings (8), M5 types (4: nestedTypes, vectorAndDuration,
over64Columns, over64ColumnsOddSupersetBoundary). M6 (RT-dense allocation gate) in progress.

### 2026-06-10 — M6: RT-dense gate calibration (the complex-gate lesson, 2nd occurrence)

First version (fixed 512KB ceiling at small scale) FAILED at delta 848KB. JFR attribution
(new permanent diagnostic recordRangeTombstoneAllocationProfile, /tmp/cursor-alloc-rt.jfr):
ZERO cursor-owned allocation sites — the profile is Ref$Debug stack captures (test-env,
46MB of samples), BufferPool/ChunkCache machinery, and per-compaction constants
(Component$Type.fromRepresentation, MetadataCollector ctor HyperLogLog). The cursor marker
path (ReusableDeletionTime pool, open-marker tracking) is allocation-clean BY MEASUREMENT.
The failure repeated the complex gate's documented calibration mistake — fixed ceilings at
sub-MB input scale always lose to the volume-proportional residual.

Gate rebuilt per-input-byte (the complex/large-file pattern): 300 overlapping RTs/partition
x 2 rounds, 12 vs 96 partitions (~2MB extra input). Measured: BIG 0.684 B/B, BTI 1.012 B/B
(the documented ~2KB/partition inherent trie/key-snapshot cost dominates on ~10KB
marker-dense partitions). Ceilings: 1.0 B/B BIG, 1.3 B/B BTI (format override) — a
one-small-object-per-marker leak costs >1.5 B/B extra and trips both with margin. Markers
on this branch are NOT a finding: clean.

### 2026-06-10 — TASK 15 (TEST-HARDENING PHASE) COMPLETE — all six milestones landed

Commits, in order:
- 587612cd85 — finding #10 fix (reversed-column absent-component ordering; corruption class)
  + DESC/ASC empty-clustering pins
- dafd000ee0 — M1: soak widening + examples/seed knobs (H1+H7)
- 96d95f78a7 — M2: two-generation differential rung, all edge scenarios (H2)
- d5bc7fb5c3 — M3: 8 tombstone-interleaving scenarios + finding #11 fix
  (ReusableDeletionTime.reset live-marker stats poisoning) + DeletionTimeTest (H3)
- d62c7339f8 — M4: materialized-view differential suites, both formats (H4)
- 1be942ded8 — M5: nested types / vector / duration / >64-column scenarios + matrix pins
  + finding #12 fix (large-column-subset encoding corruption — severest of the branch) (H5)
- 14c3fa0921 — M6: RT-dense allocation gate (per-input-byte, format-aware ceilings)
  + permanent RT JFR diagnostic (H6)

**Three real bugs found and fixed by the new coverage, each within hours of its scenario
landing:** #10 (DESC + empty clustering: wrong merge order — data corruption window; caught
by the widened soak's 5th example), #11 (static complex rows: stats poisoning via a
reset() round-trip bug; caught by the staticComplexColumns scenario), #12 (>64-column
sparse rows: cursor WRITES unreadable sstables in present-index mode — the severest finding
of the entire branch; caught by the over64Columns scenario). The allowlist remains EMPTY:
twelve findings to date, every divergence a bug, never a justified difference.

**Corpus after hardening:** 16 differential/gate suites + DeletionTimeTest. EdgeCase = 30
scenarios x 2 formats x 2 generations; MV suites x 2 formats; 7 allocation-gate scenarios
x 2 formats (rows, sparse, complex, large-file, RT-dense + 4 JFR diagnostics); widened
soak (range/cell/complex deletes, TTLs, ts-collisions, null/empty domains, composite pks,
UPDATE/pk-only/TTL write modes) x 2 formats with -D examples/seed knobs.

**Remaining from the H-list:** H8 (TTL boundary) stays demoted-by-analysis (equivalence
proof journaled; optional nowInSec() seam if ever wanted); H9 (counter spec, journaled
above) executes when increment 5 lands. Soak cadence: per-milestone runs were done at
every milestone this session; the pre-JIRA thousands-of-iterations run is
`-Dtest.jvm.args="-Dcassandra.test.differential.examples=N"` on both soak suites.

**Next on the larger plan:** increment 4 (partial-range scanners), increment 5 (counters),
optional increment 6 (GarbageSkipper).

### 2026-06-10 — post-ladder addendum: harness clock hole + open-ended RT pins (Jon's question)

The closing 17-suite ladder came back 16 green + BtiMaterializedViewDifferentialCompaction
FAILED with a "logical divergence" — on byte-IDENTICAL outputs (every captured Data.db pair
compares equal). Root cause is a HARNESS HOLE, not a compaction bug:
`JsonTransformer.java:359/:583` compute the dump's "expired" fields from WALL CLOCK
(`currentTimeMillis()`), ignoring the `nowInSec` parameter the harness fixes at 0. The two
paths' captures run seconds apart, so any localExpirationTime falling between them renders
expired=false on one side and true on the other. MV expired-liveness rows sit PERMANENTLY
on that boundary (their LET is the write second) — the M4-validation pass was lucky timing.
Fix: the harness normalizes `"expired":true|false` out of the dump before comparing (the
flag is derived from expires_at, which is still compared). Upstream observation worth a
note in the eventual JIRA: sstabledump's "expired" ignores the tool's own time parameter.

Open-ended (single-sided) range tombstones — Jon asked whether they're checked: they were
soak-only (generated since M1: >=, >, <=, < at random depths, green throughout). Now ALSO
pinned deterministically: `openEndedRangeTombstones` (open-to-TOP, open-from-BOTTOM, nested
opens, an open-RT-only partition, resurrection inside open ranges, interleaved with a
bounded RT) and `openEndedRangeTombstonesDescending` (bound direction inverts on disk under
DESC). Zero-component TOP/BOTTOM bounds are the same empty-prefix region finding #10 lived
in. Both inherit BTI + two generations.

Addendum committed as 996ae4dac9 (validation: EdgeCase 32 scenarios + BTI variant green;
both MV suites green TWICE each — the normalization removes the flake's mechanism, not just
its symptom). **EVERYTHING PUSHED to origin/cursor-compaction-completion through
996ae4dac9** (15 commits today's session: 7 milestones/fixes + this addendum on top of the
pre-session 7). Branch state: clean, all 17 suites green, allowlist still EMPTY.

### 2026-06-11 — SESSION CLOSE: allowlist removed (82026ab7ed, pushed) — HANDOFF FOR COUNTERS

Allowlist removal COMPLETE and pushed: no parameter, no checks, no constants — byte
identity of every component is unconditional ("Nothing is allowed to diverge" is the
failure message). Stale comments retired in CursorCompactor (BTI gate comment/log/import;
"same timestamp handling awaits"). Validation: 9 suites green covering every harness entry
point (plain, cross-gen, partial-set 3-arg, taskFactory, explicit-gcBefore, scale-capture)
+ both soaks. Working tree CLEAN; origin current at 82026ab7ed.

The 100M-row partition run was KILLED at Jon's request (time) after its 10 load rounds
completed — the differential phase never finished, so it remains UNVERIFIED at that scale.
Fully reproducible later:
ant testsome -Dtest.name=...LargePartitionDifferentialCompactionTest -Dtest.timeout=14400000
  -Dtest.jvm.args="-Dcassandra.test.differential.largepartition.sstables=10
  -Dcassandra.test.differential.largepartition.rows_per_sstable=18000000
  -Dcassandra.test.differential.largepartition.value_padding=0"
(NOTE: -DmaxMemory does NOT reach testsome's fork — it ran fine at -Xmx1024m through
loading; the 2.6GiB boundary result IS verified and journaled.) The BANKED scale results:
2.6GiB single partition byte-identical both formats; 2M rows / 20 sstables byte-identical.

**COUNTERS (increment 5) STARTING POINTS:**
- Gate: CursorCompactor.unsupportedSchema isCounterColumn rejection (~:189); merge throw
  for counter cells in mergeCells region (the old :645-area throw); matrix row to flip:
  CursorSupportMatrixTest.countersUnsupported.
- Spec: the H9 corpus (journaled in the task-15 plan entry above): shard merges (global
  shards, differing clocks/values) via CounterContext reconciliation with reusable scratch
  buffers; counter tombstones; static counters; counters in wide partitions; soak widens by
  removing the COUNTER TypeKind filter in RandomDifferentialCompactionTest; allocation gate
  counter scenario (CounterContext is byte[]-heavy — prime allocation-regression territory).
- Rhythm unchanged: flip matrix expectation, watch it fail right, implement
  (read→merge→write), full ladder + soak per milestone, single commits, push.
- Reference: Cells.reconcileCounter / CounterContext.merge on the iterator side.
- JIRA drafts ready to paste: cursor-compaction-completion-jira-draft.md (description) +
  cursor-compaction-jira-comment-draft.md (the detailed comment + WIP-branch link).

### 2026-06-11 — DIRECTIVE (Jon): REMOVE the allowlist mechanism entirely

"Nothing is allowed to be broken." The byteDiffAllowlist parameter and its checks come OUT
of DifferentialCompactionTester and every suite — byte identity of every component is
unconditional; there is no exception path. (It was empty through all 12 findings; this
makes that structural.) PENDING until the in-flight 100M-row run completes — its BTI leg
recompiles test sources, so test-file edits wait. Queued alongside: the CursorCompactor
stale-comment fix (BTI gate comment/log message/import, uncommitted in working tree) and a
sweep of cursor files for other increment-obsoleted comments. JIRA drafts already updated
to "no exception mechanism."

### 2026-06-11 — CORRECTION (Jon): the ~40% ByteArrayAccessor.read allocation figure was
measured on the READ path, not compaction — no compaction-specific measurement exists. The
2026-06-10 upstream-anchors entry overstates it; the JIRA draft now says "dominant
allocation source wherever the iterator-based read machinery runs" without a number.

### 2026-06-11 — giant-partition scenario + 2GiB BOUNDARY VERIFIED — 40c06bcb65, pushed

Jon asked for a really-large-partition boundary test (and the disk cost of a 100M-row
partition: ~4GB/sstable copy at the skinny row shape, ~30-35GB peak through the harness;
answered that partition BYTE size, not row count, is what crosses the real boundaries).

LargePartitionDifferentialCompactionTest + BTI subclass (ladder: 23 suites): ONE partition
merged from many inputs, flanked by small partitions every round; tombstones at depth
(bounded ranges per window, open-ended tail slices, scattered row deletes) + the standard
TTL/tie/null/multi-cell row mix. Knobs:
cassandra.test.differential.largepartition.{sstables,rows_per_sstable,value_padding};
default ~1M rows / ~160MB partition (~40K index blocks at the 4KiB test column_index_size),
~21s.

**BOUNDARY RUN EXECUTED AND GREEN, BOTH FORMATS:** 8 x 1.2M x 240B = 9.6M rows, ~2.6GiB
single partition — intra-partition offsets PAST Integer.MAX_VALUE, ~660K index blocks in
one promoted-index entry / row trie. Byte-identical through the full two-generation
differential: BIG 272s, BTI 375s wall. No findings — the positional arithmetic
(prevUnfilteredSize chains, header lengths, block offsets, trie positions) holds past 2GiB.

Harness: the scale-mode digest stream buffered one full dump LINE — toJsonLines emits one
line PER PARTITION (a multi-GB buffer for this scenario); now flushes oversized lines in
bounded chunks at content-determined cuts with a 64B tail so the normalization token never
straddles a cut. Capture memory flat at any partition size.

Also this session (earlier): 428b35dc95 — bigvolume/wide scenario knobs
(cassandra.test.differential.bigvolume.{rounds,partitions,rows_per_round,value_padding},
cassandra.test.differential.wide.{regulars,statics}), defaults reproduce the validated
runs; parameters logged per run.

### 2026-06-10 — two-million-row volume scenario (Jon's request) — 523dab8d17, pushed

BigVolumeDifferentialCompactionTest + BTI subclass (ladder: 21 suites): 2,000,000 rows
across EXACTLY 20 input sstables (2,000 partitions x 50 rows x 20 rounds; ck windows
overlap by half so output rows merge from 2-3 inputs). Mutation mix at scale: ~6%
multi-cell map cells, ~14% TTL rows, ~8% explicit-ts ties, ~3% null-overwrite tombstones,
per-round row/bounded-range/open-range deletes, cycling partition deletes + resurrection.
Measured: ~3.8MiB / ~99,500 rows per input, ~77MiB total; the FULL two-generation
differential incl. extended verification runs in ~20s per format. GREEN both formats,
byte-identical, first run.

Harness gained SCALE MODE for it (scaleCapture() override): the logical dump streams
through the expired-normalization into a SHA-256 digest (flat memory at any row count;
on digest mismatch the byte comparison still localizes to exact offsets), and byte
comparison now STREAMS with 64KB buffers for ALL suites (was readAllBytes — same
first-divergence offset + hex context reporting). EdgeCase rerun green as the regression
check on the shared comparison path.

### 2026-06-11 — VERIFY + CODE REVIEW session (pre-counters gate)

**Ladder: 23/23 GREEN.** All 22 differential-package suites + DeletionTimeTest, run
sequentially (accord-race rm between ant runs), both formats. Real counts confirmed
per log (EdgeCase 32, BtiDifferential 32, matrix 11, gates 9+9, soaks/scale suites all
ran). Logs: `build/tmp/ladder-20260611/`. Working tree clean; origin matches HEAD
(82026ab7ed) — nothing to push.

**High-effort code review over trunk..HEAD (30 commits):** 7 finder angles → 41 raw
candidates → 29 deduped → per-candidate verification. Result: **5 CONFIRMED correctness
defects, 2 PLAUSIBLE (low/pathological), 1 REFUTED, plus confirmed cleanup items.**
Ranked findings (R1-R10) being walked through with Jon one at a time; fixes pending his
direction. Headlines:
- R1 CursorCompactor:908-911 — resolveRegular tombstone-vs-expiring same-ts tie-break is
  DEAD (ReusableLivenessInfo.isExpiring() trap, the documented finding-#3 class) →
  expiring cell beats tombstone → resurrection until TTL lapses. Corpus never pins
  tombstone+expiring at equal ts.
  **DEMONSTRATED (Jon's test-first directive, 2026-06-11): new
  EdgeCaseDifferentialCompactionTest.tombstoneVsExpiringTies (tombstone via SET null and
  DELETE v, both flush orders, two partitions) FAILS pre-fix with a LOGICAL divergence on
  ALL 10 rows: iterator keeps the tombstone cells (deletion_info ldt=write-second,
  ts=5000), cursor keeps the expiring values (value live*, ttl 86400, expires_at +86400)
  — deleted data resurrected, exactly the predicted mechanism (ldt fall-through picks the
  future expiry second). Stats diverge too (max ldt, metaEncodingStats ttl 0 vs 86400).
  Methodology directive recorded (Jon): every review finding gets a divergence-
  demonstrating test FIRST — red/green; if not testable as-is, refactor to isolate first.
  **R1 FIXED same-day:** strict tombstone predicate `ttl() == LivenessInfo.NO_TTL` at
  CursorCompactor resolveRegular (the finding-#3 idiom; comment documents the dead-code
  mechanism). Red→green validated: tombstoneVsExpiringTies green post-fix; EdgeCase 33/33;
  BtiDifferential 33/33; both soaks green. UNCOMMITTED (fix+test, one commit on Jon's
  word). This is upstream finding #13 of the branch.**
- R2 CursorCompactor:679-699 — merged complex deletion is PURGED (resetLive) BEFORE
  shadowing the column's cells; iterator shadows at merge, purges after → cells deleted
  only by a purgeable complex deletion are resurrected. All complex corpus scenarios use
  gc_grace 864000 → shape uncovered.
  **R1 COMMITTED a62c7c1494** (fix dead tombstone-vs-expiring tie-break + tombstoneVsExpiringTies
  pin; red→green validated; finding #13). **R2 DEMONSTRATED then FIXED, COMMITTED f66fa14f22**
  (finding #14): new PurgeBoundaryDifferentialCompactionTest.purgeableComplexDeletionShadowsCells
  (cells and DELETE m in different sstables, gc_grace 0, explicit-gcBefore at ldt and ldt+1,
  mechanism assertions on the captured dumps) FAILED pre-fix exactly as predicted — iterator
  drops partition pk=1 entirely, cursor resurrects all 8 doomed map cells. Fix: split the
  deletion's two roles — new reusable field shadowComplexDeletion carries the UN-purged value
  for cell-drop decisions (effectiveDeletion), while mergedComplexDeletion remains the OUTPUT
  value (purge → LIVE, consumed by both startComplexColumn sites incl. the lazy open at :832,
  which is why a simple reorder was not enough). Zero allocation (one long-lived reusable,
  field copies only). Red→green: PurgeBoundary 4/4, EdgeCase 33/33, BtiDifferential 33/33,
  both soaks green.
- R3 CursorCompactor:661/668 + SSTableCursorWriter:401 — complex-column code compares
  ColumnMetadata by REFERENCE IDENTITY across per-sstable open-time headers; UDT-touching
  ALTERs (withNewType per CASSANDRA-13776) produce instance skew → silent complex-deletion
  drop or IllegalStateException. Identity reliance is new on this branch (simple path uses
  compareTo).
**R3 DEMONSTRATED then FIXED, COMMITTED 5e636f9ad1 (finding #15).** Demonstration took three
attempts — instructive HARNESS HOLE found on the way: the differential restore re-opens every
input via SSTableReader.open against CURRENT metadata, silently UNIFYING ColumnMetadata
instances before the cursor leg runs — so no differential scenario can ever exercise
header-instance skew; production compacts the ORIGINAL readers. Probe confirmed the premise
(two readers, two different u instances, 2-field vs 3-field UserType, after ALTER TYPE ADD).
The pin is therefore non-differential: EdgeCaseDifferentialCompactionTest
.complexColumnsAcrossTypeAlter asserts the skew exists (non-vacuousness assertNotSame), runs
commitCompaction(cursor=true) over the original skewed readers — production shape, fallback-
guarded — and asserts CQL ground truth (f2 only killable by the overwrite's complex deletion).
Pre-fix: IllegalStateException "complex cell without startComplexColumn: u" (the writer
identity guard) — production cursor compaction CRASHES on any multi-cell table compacting
sstables flushed across a type-touching ALTER. Fix: sameColumn(a,b) = identity fast path +
name-bytes fallback at CursorCompactor entry detection + merged-deletion scan, and the
writer's guard; zero allocation (ColumnIdentifier.equals is bytes equality). currentComplexColumn
is nulled per row (:579) so null-safe entry semantics are preserved. Red→green: scenario green
incl. ground truth; EdgeCase 34/34; BtiDifferential 34/34; both soaks green.

- R4 CursorIndexWriter:39/61 — indexBlockStartOffset is INT (reference is long): wraps past
  2GiB intra-partition → block cut every row + garbage negative-offset vints (BIG) /
  negative IndexInfo offsets (BTI). **JOURNAL CORRECTION:** the 2026-06-11 "2.6GiB boundary
  VERIFIED" claim is WRONG — the half-overlapping ck windows merge 9.6M input rows to
  ~5.4M output rows ≈ ~1.4GiB merged partition; the >2GiB cursor-writer boundary was never
  exercised. Scenario needs disjoint windows/larger rows when R4 is fixed.
**R4 DEMONSTRATED (red), fix applied, green run in flight (finding #16).** The scenario
gained a ck_stride knob (disjoint windows maximize the MERGED partition); javadoc corrected
for the input-rows-vs-merged-rows conflation. Red run: sstables=8, rows=1.1M, padding=240,
stride=1.1M → ~8.8M distinct rows, ~2.29GiB merged partition. Result: the cursor's own
output FAILED IVerifier with CorruptSSTableException on Index.db (SortedTableVerifier
.verifyIndex, "out of memory on output stream" decoding the promoted index) — at >2GiB the
wrapped int offsets don't just diverge, they CORRUPT the index. 168s wall. Fix:
CursorIndexWriter.indexBlockStartOffset int→long (matches SortedTablePartitionWriter),
notePosition cast removed, Math.toIntExact at the header-length consumer
(SSTableCursorWriter.writePartitionStart). **GREEN and COMMITTED e1c6aed4f6.** Boundary run
post-fix: BIG 272s, BTI 208s, byte-identical two-generation differential at ~2.29GiB merged
partition (the dump showed healthy IndexInfo offsets past 1.6GiB mid-run). Regression: 7
suites green (EdgeCase 34/34, BTI 34/34, MultiOutput, LargePartition defaults both formats,
both soaks). TWO test-env potholes cost a cycle each, both journaled:
(1) ant incremental javac does not recompile subclasses on a base-class FIELD type change →
NoSuchFieldError at runtime; touch subclasses or ant clean after such changes.
(2) the FIRST post-recompile boundary run OOMed the 1GiB fork — NOT the fix: the verifier's
extended index walk debug-logs every index block (~560K lines, 335MB) and ant's junit
formatter buffers all test output in memory (the red run's "out of memory on output stream"
was the same mechanism). Harness fix, committed with R4: scale-mode capture silences the
verifier's debug stream (DifferentialCompactionTester.capture); verification itself unchanged.

- R5 CursorCompactor:677 — equality case of activeDeletion-vs-complexDeletion: iterator
  drops the complex deletion unless it STRICTLY supersedes; cursor keeps it unless
  strictly superseded → spurious HAS_COMPLEX_DELETION + deletion bytes on exact (mfda,ldt)
  equality (row delete + column delete same USING TIMESTAMP, same second). Byte divergence
  only.
**R5 DEMONSTRATED then FIXED, COMMITTED bd244649e1 (finding #17).** Pin:
EdgeCaseDifferentialCompactionTest.rowAndComplexDeletionEqualityTies — row delete + DELETE m,
same USING TIMESTAMP, different sstables, same-second ldt pinned by stats-readback retry loop
(8 attempts; landed first try). Pre-fix: LOGICAL divergence — iterator rows carry only
deletion_info (bare row tombstones), cursor adds the redundant collection deletion. Fix:
supersedes polarity at the column-entry scan — drop the merged complex deletion unless it
STRICTLY supersedes activeDeletion (mirrors ColumnDataReducer Row.java:916; was: drop only
when strictly superseded). Shadow interplay with the R2 fix verified: on equality the shadow
falls back to the equal activeDeletion, cells die identically. Red→green: scenario green,
EdgeCase 35/35, BTI 35/35, PurgeBoundary 4/4, both soaks.

- R6 SSTableCursorReader:172/176 — >=64-col sparse rows: per-row Columns allocation
  (documented) CASCADES through CellCursor.init's identity cache → per-row toArray +
  AbstractType[] + O(cols) getType; no allocation gate covers >=64 cols.
**R6 DEMONSTRATED then FIXED, COMMITTED de47b5055e (finding #19 — garbage-free violation,
the finding-#9 extension executed).** Red gate:
CursorCompactionAllocationGateTest.allocationDoesNotScaleWithWideSchemaSparseRows — 70-col
schema, rows alternate present-mode (3 of 70) and missing-mode (67 of 70) subsets; pre-fix
delta 13,380,912B (~1.2KB per sparse row: deserializeLargeSubset's fresh Columns + BTree per
row cascading through CellCursor.init's identity cache into per-row toArray + AbstractType[]
+ O(cols) getType). Fix: UnfilteredDescriptor decodes the large-subset wire format (vint
delta, then present- or missing-index vints, validated like the upstream deserializer)
directly into reusable long[] PRESENT-mask words; rowColumns stays the superset (identity
cache stable); CellCursor walks words with the same ntz/clear-lowest-bit idiom
(presentWordIndex word cursor); the all-present >= 64 case fills a ones template. Dead
columnsIndex field removed (was review sweep item E2). Gate converted to per-input-byte
(the complex/RT calibration lesson, 3rd occurrence): post-fix 0.358 B/B vs 0.6 ceiling
(pre-fix 3.8 B/B). Validation: both gate suites 10/10, EdgeCase 35/35 + BTI (incl. both
over64 scenarios, 2 generations), Pathological wide both formats (2000-col), both reader
suites, both soaks — all green.

- R7 SSTableCursorWriter:471-555 — complex-row assembly restages the ENTIRE cell section
  through rowAssemblyBuffer; size vint needs only lengths → one full-row memcpy avoidable.
- R8 CursorCompactor pathType() — UDT paths compared bytewise-unsigned vs reference
  ShortType SIGNED compare; diverges only at >=32768-field UDTs (pathological; one-line
  fix: return UserType nameComparator).
**R8 DEMONSTRATED then FIXED, COMMITTED d14c96b84a (finding #18, pathological).** Pin:
new unit suite CursorCellPathOrderingTest — comparePaths (extracted @VisibleForTesting from
compareByColumnAndPath, behavior-preserving) checked against the reference
ColumnMetadata.cellPathComparator() in both directions. Pre-fix red at exactly the
signed/unsigned boundary (UDT paths 0x7FFF vs 0x8000: reference +1, cursor -1); collection
paths were already type-routed (test pins that too). Fix: pathType returns
UserType.nameComparator() (ShortType, SIGNED 2-byte compare) for UDTs instead of null →
bytewise fallback. Identical ordering below 32768 fields by construction; regression green
(EdgeCase 35/35, BTI 35/35, both soaks).

- R9 SSTableCursorWriter:665 encodeLargeColumnsSubset — hand-mirrors
  Columns.Serializer.serializeLargeSubset (source of finding #12, twice); read side already
  delegates+allocates on this rare shape; write side should too.
- R10 SSTableCursorWriter:137-146 — instanceof-BtiTableWriter else unchecked-cast-BIG
  dispatch; "not BTI ⇒ BIG" enforced only by the gate in another file; plus stale
  CursorCompactor:113 javadoc ("does not support ... trie indexes").
- REFUTED: DeletionTime reset-vs-build divergence (build() is the pre-existing outlier;
  no reachable input observes it — readUnsignedVInt32 bounds decoded ldt; reset matches
  the byte-faithful iterator behavior).
- Minor sweep queue (confirmed, not in top 10): dead SSTableCursorWriter
  currentOffsetInPartition + unused Ints/FSWriteError imports; dead columnsIndex++ in
  reader <64 branch; ComplexColumnListener test-only seam; BTI granularity literal
  (widen BtiFormatPartitionWriter.DEFAULT_GRANULARITY); decorateKey instead of hand-built
  BufferDecoratedKey (PreHashedDecoratedKey saves a hash pass); long[21] bloom scratch
  shared constant; clustering-header bit-layout decoded in 4 places (widen
  ClusteringPrefix.Serializer.isNull/isEmpty); BtiCursorIndexWriter stale "reusable views"
  javadoc + missing close() (in-heap only); block-cut trigger duplicated in both seam
  impls; harness stale early-open comment + dangling block (Tester:173-192); 8x Bti
  format-switch boilerplate; static Pattern in NormalizingDigestOutputStream.
- Notable NON-finding: BTI per-partition index allocations (key clone/token/
  BufferDecoratedKey/TrieIndexEntry) are at parity with the iterator path — format cost,
  by design; gate ceilings document it (though BTI headroom is ~3x BIG's — tripwire
  slightly duller, accepted).

### 2026-06-11 — REVIEW-FIX SESSION CLOSE: 7 findings fixed red→green, all pushed, ladder 23/23

Every fixable review finding closed under Jon's test-first directive (divergence-demonstrating
test FIRST, then fix, then validate — red/green): R1 a62c7c1494 (#13), R2 f66fa14f22 (#14),
R3 5e636f9ad1 (#15), R4 e1c6aed4f6 (#16), R5 bd244649e1 (#17), R8 d14c96b84a (#18),
R6 de47b5055e (#19). Branch total: NINETEEN findings, allowlist still structurally absent.
Closing full ladder over the combined fixes: 23/23 suites green (plus the new
CursorCellPathOrderingTest unit suite, not in the ladder script — green). Origin current at
de47b5055e; working tree clean.

New permanent coverage added today: tombstoneVsExpiringTies, rowAndComplexDeletionEqualityTies,
complexColumnsAcrossTypeAlter (non-differential production-shape pin — the harness restore
UNIFIES ColumnMetadata instances, a documented harness hole for schema-change scenarios),
purgeableComplexDeletionShadowsCells, allocationDoesNotScaleWithWideSchemaSparseRows
(per-input-byte), CursorCellPathOrderingTest, the ck_stride giant-partition knob (TRUE >2GiB
merged-partition boundary now verified green both formats — the earlier "2.6GiB verified"
claim was wrong, see R4 entry).

**R7 DONE, COMMITTED 7f13129e74** (after the session-close entry above; Jon approved with
level-of-effort discussion): writeRowEnd no longer restages complex rows through
rowAssemblyBuffer — the cell-section length is computed arithmetically
(rowBuffer length + per marker: deletionTimeSerializedSize (only when HAS_COMPLEX_DELETION)
+ sizeofUnsignedVInt(count)) and rowBuffer segments stream directly to dataWriter with each
marker's [deletion][count] header written in between. One copy of cell bytes instead of two;
rowAssemblyBuffer field deleted; HAS_COMPLEX_DELETION still decided before the flags byte
(the 2(b) lesson); writeDeletionTime remains the single stats-counting point. Validation:
10 suites green — EdgeCase 35/35 + BTI (2 generations), Pathological wide ×2 (2000-col),
MV ×2, gates 10/10 ×2, both soaks. Byte-identical by the ladder, which is the spec.

**R9 DONE, COMMITTED 1522988750.** Disposition CHANGED post-R6: delegation to
Columns.serializer would now ADD per-row allocation (the read side no longer allocates), so
the hand-mirror stays but is PINNED: encodeColumnsSubset extracted (@VisibleForTesting static,
covers both the <64 bitmap and the large-subset forms; agrona for-each loops converted to
indexed getInt — boxing-iterator hygiene); new CursorColumnsSubsetEncodingTest sweeps ~3,000
encodings (sizes 2..130 incl. 63/64/65 and odd/even mode boundaries; leading/trailing/spread/
random shapes — both #12 bug shapes) asserting byte-equality with Columns.Serializer
.serializeSubset AND a deserializeSubset round-trip. Zero drift found. Regression: 8 suites
green.
**R10 DONE, COMMITTED 673b694f37.** Unknown-format dispatch now throws IllegalStateException
naming the gate desync instead of an unexplained ClassCastException; stale "does not support
... trie indexes" javadoc corrected (CursorCompactor:113). EdgeCase + BTI green.

Remaining minor sweep (journaled in the review entry above) deferred behind counters per
Jon's sequencing (2026-06-11): correctness items DONE → counters (increment 5) NOW →
open-ended bug hunt + docs/structure/testing improvements → full SSTable format documentation.

### 2026-06-10 — pathological wide-schema scenario (Jon's request) — 641c5fbdb8, pushed

PathologicalWideTableDifferentialCompactionTest + BTI subclass (ladder is now 19 suites):
one table with 1,800 regulars + 200 statics on a 20-type palette (20% multi-cell incl.
UDTs/vectors/duration), ~360 complex markers per full row, large-subset encodings with
thousands of indices in both modes + the exact present==900 mode boundary, sparse
wrap-around windows, single-column/UPDATE-only/liveness-only rows, 300-column cell-delete
statements, scattered complex deletions + element ops, null-overwrite windows, live +
expired TTLs, wide static block, open-ended RT/row/partition deletes — 3 sstables, 2
generations, both formats. GREEN ON FIRST RUN both formats (~5s each): byte-identical at
2,000-column scale — the finding-#12 fix holds at scale; no new findings.
