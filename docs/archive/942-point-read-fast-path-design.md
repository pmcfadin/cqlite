# [RETIRED] Design Doc: Route Single-Partition Reads to a Point-Read Fast Path

> **RETIRED 2026-07-04 — do NOT implement from this document.**
> Its premise predates epic [#951](https://github.com/pmcfadin/cqlite/issues/951), which shipped the routing this doc proposes (`AccessPath` plan enum, `classify_partition_lookup`, `scan_partition*` with bloom + BTI-trie pruning, CLI `--explain`, the #958 work-bound CI guard). The Summary.db binary-search plan in §5 describes code that is dead on `main`; the real lookup path is the Index.db raw-key map (bounding it is [#1599](https://github.com/pmcfadin/cqlite/issues/1599)).
> Validation record: the full archaeology of this doc vs `main` is in [#942 (comment)](https://github.com/pmcfadin/cqlite/issues/942#issuecomment-4871162090). Issue #942 is closed as superseded. The verified residue is tracked by epic [#1915](https://github.com/pmcfadin/cqlite/issues/1915) (children #1916 metadata-IN fan-out, #1917 concat-fallback token ordering, #1918 `CQLITE_READ_PATH` knob + differential lane), whose issue bodies are self-contained — they do not depend on this doc.
> The correctness landmine surfaced during that validation (single-generation reads skipped reconciliation) was fixed as [#1741](https://github.com/pmcfadin/cqlite/issues/1741).

---

**Feeds:** GitHub issue [#942](https://github.com/pmcfadin/cqlite/issues/942) — "Use the read path for partition reads" (P2, enhancement, filed by @rustyrazorblade)
**Intended use:** Input to OpenSpec workflow (`flow-groom` → proposal/design/tasks → epics + issues)
**Suggested OpenSpec change name:** `point-read-fast-path`
**Status of this doc:** Draft for grooming. Contains `VERIFY:` items that MUST be resolved against the code before spec approval (Seam 1). Authored from external inspection of the docs site, CLAUDE.md, and the SSTable format guide — not from reading `cqlite-core` source.

---

## 1. Problem Statement

Every `SELECT` currently executes via the "compact to memory" path: all SSTable
generations for the table are merged (compaction-style) into memory, and the query is
evaluated against the merged result.

This is the right semantics for full scans and exports. For a single-partition query —
`SELECT ... WHERE <full partition key bound by equality>` — it is strictly wasteful:

- **Latency:** cost is O(table size), not O(partition size). The project's stated
  target of sub-millisecond partition lookups is unreachable through this path on
  non-trivial tables.
- **Memory:** the <128 MB budget for large files is spent materializing data the query
  never touches.
- **Scaling with M5:** now that flush + STCS compaction produce multiple SSTable
  generations per table, the compact-to-memory cost grows with generation count even
  when the target partition lives in one file.

Issue #942 asks the engine to *intelligently route* single-partition queries to a
normal read path: index lookup → seek → decompress only what's needed → stream one
partition — i.e., what Cassandra itself does, and what the repo's own format guide
documents in Ch.10 (Point Reads and Slices) and Ch.21 (flow card).

`VERIFY:` locate the actual compact-to-memory entry point before speccing.
Start: `rg -n "compact" cqlite-core/src/query/` and trace how
`cqlite-core/src/query/` obtains rows from `storage/`. Confirm whether it invokes
`storage/write_engine/merge.rs` machinery or a separate scan/merge in the reader.

## 2. Goals

- G1: Queries that fully bind the partition key by equality (or `IN`) execute without
  materializing the whole table.
- G2: Results are **bit-identical** to the compact-to-memory path for every query, on
  every table in the corpus. Routing is a pure performance optimization; it must never
  change semantics.
- G3: The two paths share one merge/reconciliation implementation (no forked
  tombstone/timestamp logic).
- G4: Routing decisions come only from authoritative metadata (schema partition-key
  definition, TOC component presence, Statistics.db) — no byte sniffing
  (no-heuristics mandate).
- G5: The chosen path is observable (EXPLAIN-style output and/or `metrics` feature
  counters) and forcible (env/config override) for testing and support.
- G6: Demonstrated wins: point-read latency and peak RSS benchmarks, fast path vs
  compact path, captured with the existing `scripts/profile.sh` harness.

## 3. Non-Goals (this change)

- NG1: BTI (`da`) fast path. Ships as a follow-up epic; BTI tables fall back to the
  compact path in this change. (See Open Question OQ-2.)
- NG2: Secondary index (2i) or SAI acceleration.
- NG3: Token-range routing, partition-range scans, or general predicate pushdown
  beyond clustering bounds within the located partition.
- NG4: Bloom-filter (Filter.db) parsing, **if** it does not already exist (OQ-1).
  Designed-for but delivered as its own issue.
- NG5: Any change to write/compaction output. This is read-side only.

## 4. Current State (with verification checklist)

Believed true from external inspection; each item needs code confirmation:

| # | Claim | Basis | VERIFY command / question |
|---|-------|-------|---------------------------|
| C1 | Summary→Index partition lookup exists and is CI-tested | `golden_path_partition_lookup_tests.rs`, `golden_path_summary_index_integration_tests.rs`, `golden_path_get_operations_tests.rs` in `cqlite-integration-tests/tests/` | Which library API do these tests exercise? Is it callable from `query/`? |
| C2 | K-way merge with byte-for-byte Cassandra compaction parity exists | `storage/write_engine/merge.rs` (M5.2), v0.12.0 release notes | Is `merge.rs` input an iterator abstraction or whole-SSTable readers? Refactor cost? |
| C3 | Main V5 parser is `storage/sstable/reader/parsing/v5_compressed_legacy.rs` (~2000 lines) | source-map page | Does it expose (or can it expose) a "parse one partition starting at offset X" entry, or only whole-file iteration? |
| C4 | BTI reader is incomplete; `da` excluded from default smoke | source-map page (may be stale vs v0.12.0 "canonical BTI read/write") | Actual state of `storage/sstable/bti/` read path: exact-key `Partitions.db` lookup usable? |
| C5 | Filter.db (bloom) parsing status unknown | format guide Ch.7 exists; no source-map entry | `rg -ni "filter.db|bloom" cqlite-core/src/` |
| C6 | CQLite-written SSTables are uncompressed, never emit CompressionInfo.db | CLAUDE.md issue #1406 claim boundary | Confirm reader handles absent CompressionInfo.db via direct offsets today |
| C7 | Writable mode has a memtable that reads must consult | M5 write support | Does the current read path merge memtable contents? Where? |
| C8 | Query classification data available: schema gives partition/clustering columns; `cql/` gives AST | source-map | Confirm AST shape for WHERE conjuncts; existing predicate model in `query/` |

## 5. Proposed Design

### 5.1 Plan layer (new: `cqlite-core/src/query/plan.rs`)

Introduce a small physical-plan enum between CQL AST resolution and execution:

```rust
pub enum ReadPlan {
    /// All partition key columns bound by `=` (or expanded from `IN`).
    PointRead {
        keys: Vec<DecoratedKey>,          // one per IN element; deduplicated
        clustering: ClusteringBounds,      // full/prefix equality + range on clustering cols
        residual: Option<Predicate>,       // non-key predicates, applied as post-filter
        limit: Option<u64>,
        reversed: bool,                    // ORDER BY ... DESC
    },
    /// Everything else — existing compact-to-memory execution, unchanged.
    FullScan {
        predicate: Option<Predicate>,
        limit: Option<u64>,
    },
}
```

Classification rules (routing MUST be decidable from schema + AST alone — G4):

- R1: `PointRead` iff every partition key column appears in exactly-one equality
  conjunct (or one `IN` list). Any partition key column unbound, bound by range, or
  bound through `OR` → `FullScan`.
- R2: `IN (k1..kn)` → `PointRead` with n keys, executed as sequential lookups,
  results concatenated in key order consistent with the compact path's ordering.
  `VERIFY:` what ordering does the compact path produce for multi-partition results?
  Match it exactly (G2).
- R3: Clustering column predicates with a full partition key stay on the fast path:
  equality/range prefixes become `ClusteringBounds` (seek/stop conditions); anything
  not expressible as bounds goes to `residual` post-filter.
- R4: Predicates on regular columns → `residual` post-filter (fast path still valid).
- R5: Aggregations/`COUNT(*)`/`WRITETIME`/`TTL` projections do not affect routing —
  they consume the row stream identically on either path. `VERIFY:` confirm
  projection layer is path-agnostic.
- R6: Any feature the fast path does not support (discovered at plan or exec time) →
  fall back to `FullScan`. Fallback is silent for the user, but counted (5.5).

### 5.2 Per-SSTable partition locator (new: `storage/sstable/reader/point_read.rs`)

For each live SSTable generation of the table, locate the partition (or prove
absence) using TOC component presence to dispatch (G4):

**BIG family (`nb`/`oa`; has `Index.db` + `Summary.db`)** — per format guide Ch.21:
1. *(Phase: bloom issue)* If `Filter.db` parsed and negative → absent; skip file.
2. `Summary.db` binary search over decorated keys → `index_offset`.
3. Sequential `Index.db` entry scan from `index_offset` (u16 key length + raw key +
   vint data_offset + vint promoted_index_len); compare raw key bytes; skip on
   mismatch; not found within window → absent (bloom false positive path).
4. Map `data_offset` to the data stream:
   - `CompressionInfo.db` present → chunk lookup, decompress only covering chunk(s).
   - Absent (CQLite-flushed, C6) → direct seek.
5. Parse exactly one partition via `SerializationHeader`, yielding a
   **`PartitionRowIterator`** honoring `ClusteringBounds` and `reversed`.

**BTI family (`da`; has `Partitions.db`)** — out of scope here (NG1); locator returns
`Unsupported` → whole-query fallback to `FullScan` (R6). Follow-up epic implements
trie `exactCandidate`, sign-bit routing to `Rows.db` vs direct `Data.db`, per Ch.21.

Promoted index / `Rows.db` intra-partition seeking for wide partitions is an
optimization issue, not required for correctness: v1 may scan the partition from its
start applying bounds.

### 5.3 Partition-scoped merge (refactor: `storage/write_engine/merge.rs`)

- Extract the reconciliation core (timestamp resolution; partition/row/range
  tombstone shadowing) so it operates over **N ordered row iterators** rather than
  whole-SSTable inputs. Compaction becomes "merge over full-table iterators"; point
  read becomes "merge over per-partition iterators (+ memtable iterator when
  writable, C7)". One implementation, two callers (G3).
- Byte-for-byte compaction parity tests MUST still pass after the refactor — this is
  the highest-risk step and should be its own issue with the parity suite as its
  gate.
- Range tombstones spanning clustering bounds are the known bug farm: opening
  markers before the seek point must still shadow rows inside the bound window.
  `VERIFY:` how does current merge represent RT bounds? Property tests required
  (7.3).

### 5.4 Fallback + forcing

- Config/env `CQLITE_READ_PATH=auto|point|compact` (default `auto`).
  `point` = error if a query can't take the fast path (test mode);
  `compact` = force legacy path (support/diagnosis mode);
  `auto` = route per §5.1 with silent fallback.
- Fast-path execution errors that indicate *unsupported*, not *corrupt* (missing
  component, unexpected format feature) → transparent per-query fallback to compact.
  Corruption errors surface as errors on both paths identically.

### 5.5 Observability

- `metrics` feature counters: `read_path.point`, `read_path.full_scan`,
  `read_path.fallback{reason}`, per-generation `bloom_skip` (later).
- CLI: `EXPLAIN SELECT ...` (or minimal: `--explain` flag) printing chosen plan,
  per-SSTable locator outcome, and fallback reason. Even a debug-log line is
  acceptable for v1; the requirement is that routing is observable (G5).

### 5.6 Latency budget investigation (EXPLORE — feeds A1)

**Observed problem (motivating data point):** single-partition point lookups are
measured at **150–500 ms per read**. This design assumes the dominant term is
compact-to-memory scan/merge cost, but that MUST be verified — if a large share is
per-query setup or bindings overhead, routing alone will not reach the sub-ms target.

A1 SHALL produce an end-to-end latency budget for one representative point read,
decomposed into at least:

1. **Session/setup:** file open, TOC/Statistics.db/Summary parse, schema resolution,
   reader-state construction. Is any of this repeated per `execute()` within an open
   Database/REPL session? (CLI one-shot mode pays full process + setup cost every
   invocation and must be excluded from — or reported separately in — the budget.)
2. **Scan/merge:** decompression + partition iteration + k-way merge (the cost this
   design eliminates for point reads).
3. **Projection/conversion:** row materialization, value formatting, and (when
   measured through bindings) CQL→Python/JS conversion.

Diagnostics (record method + numbers in the A1 findings on #942):

- **Size-scaling check:** identical point lookup against a small table vs a large
  one. Latency proportional to table size ⇒ scan-dominated ⇒ this design is the fix.
  Flat-but-slow ⇒ setup-dominated ⇒ see contingency below.
- **Flamegraph:** `./scripts/profile.sh flame` under a repeated single-partition
  query; attribute samples to the three buckets above (expect
  `v5_compressed_legacy.rs` iteration + merge frames if scan-dominated).
- **Generation sensitivity:** same lookup at 1 vs N SSTable generations.

**Contingency (in scope to spec, out of scope to build until data says so):** if
setup dominates, add an issue for cached reader/index state across queries within an
open session (Summary/index metadata, open file handles, decompression scratch) —
complementary to routing, and likely required to hit sub-ms even with the fast path.
Benchmarks in D2 SHALL report warm-session latency (excluding one-time open cost)
as the primary metric, with cold/one-shot reported separately.

## 6. Requirements (OpenSpec spec-delta form)

### Requirement: Single-partition query routing
The query engine SHALL classify each SELECT into `PointRead` or `FullScan` using only
the resolved schema and query AST, per rules R1–R6.

#### Scenario: Full partition key equality
- **WHEN** a SELECT binds every partition key column with `=`
- **THEN** the engine executes it via the point-read path
- **AND** the result is byte-identical to the compact-to-memory path's result

#### Scenario: Partial partition key
- **WHEN** a SELECT binds only a proper subset of partition key columns
- **THEN** the engine executes it via the compact-to-memory path unchanged

#### Scenario: IN on partition key
- **WHEN** a SELECT binds the partition key via `IN` with n values
- **THEN** the engine executes n point lookups and returns results in the same order
  the compact path would produce

### Requirement: Result equivalence
For every query in the validation corpus, output via the point-read path SHALL be
identical to output via the compact path (rows, values, order, WRITETIME/TTL).

#### Scenario: Differential corpus run
- **WHEN** the full 33-table corpus query suite runs under `CQLITE_READ_PATH=point`
  and `CQLITE_READ_PATH=compact`
- **THEN** normalized outputs are equal for every table and query

### Requirement: Multi-generation reconciliation
A point read over a table with multiple SSTable generations (and a memtable when
writable) SHALL reconcile rows and tombstones through the same merge implementation
used by compaction.

#### Scenario: Shadowed row across generations
- **WHEN** generation 2 contains a row tombstone shadowing a row in generation 1 for
  the queried partition
- **THEN** the point read returns no such row, matching the compact path

#### Scenario: Range tombstone spanning clustering bounds
- **WHEN** a range tombstone opens before the queried clustering bound and closes
  inside it
- **THEN** rows within the shadowed range are absent from point-read results

### Requirement: Authoritative-metadata routing (no heuristics)
Index-family dispatch SHALL be driven by TOC component presence and version gates
(`BigVersionGates`/`BtiVersionGates`); the engine SHALL NOT infer format or routing
from data-byte patterns.

#### Scenario: BTI table encountered (this change)
- **WHEN** a point-read-eligible query targets a `da` (BTI) table
- **THEN** the engine falls back to the compact path and increments
  `read_path.fallback{reason="bti_unsupported"}`

### Requirement: Uncompressed-generation support
The point-read path SHALL read partitions from generations lacking
`CompressionInfo.db` (CQLite-flushed) and generations with it (Cassandra-written)
within the same query.

#### Scenario: Mixed generations
- **WHEN** a queried partition has fragments in one compressed and one uncompressed
  generation
- **THEN** the point read merges both fragments correctly

### Requirement: Fallback safety
Unsupported constructs discovered at plan or execution time SHALL cause transparent
fallback to the compact path with an observable reason; correctness SHALL never
depend on the fast path.

### Requirement: Observability and forcing
The engine SHALL expose the chosen path (metrics and/or EXPLAIN output) and honor
`CQLITE_READ_PATH=auto|point|compact`.

### Requirement: Performance evidence
The change SHALL include criterion benchmarks demonstrating, on a large corpus table:
point-read latency reduction and peak-RSS reduction vs the compact path, recorded via
`scripts/profile.sh` baseline/compare.

## 7. Validation Plan

1. **Differential corpus test (primary gate):** run the corpus (per
   `CQLITE_DATASETS_ROOT`) through both forced paths; assert normalized equality.
   Wire as an integration test target alongside the golden-path suites; agent-gate
   must include it.
2. **Golden-path extension:** new `golden_path_point_read_tests.rs` covering: found /
   not-found / bloom-false-positive-analog (key between index entries), first/last
   partition in file, single-row and wide partitions, reversed reads, LIMIT.
3. **Merge property tests:** randomized generations (timestamps, row/range
   tombstones, static rows, clustering orders); invariant: partition-scoped merge
   result == compact-path result for that partition. This is the guard for the §5.3
   refactor.
4. **Compaction parity regression:** the existing byte-for-byte compaction parity
   suite must pass unchanged after the merge refactor (proves G3 didn't break the
   writer side).
5. **Bindings parity:** existing Python/CLI parity tests rerun with
   `CQLITE_READ_PATH=point` in a slow-test lane (`RUN_SLOW_TESTS=1`).
6. **Benchmarks:** criterion point-read benches (small partition, wide partition,
   multi-generation) + dhat heap check vs the <128 MB budget; store baseline via
   `./scripts/profile.sh baseline`.

## 8. Epic / Issue Breakdown

Sized for the repo's 1:1:1:1 model (one issue ↔ one branch ↔ one OpenSpec change ↔
one PR). Suggested labels: `enhancement`, inherit P2; epics ordered by dependency.

### Epic A — Verification & plan layer (unblocks everything)
- **A1. Code archaeology spike (timeboxed):** resolve C1–C8 in §4 **and produce the
  §5.6 latency budget for the observed 150–500 ms point reads**; post findings on
  #942; amend this design. *Deliverable: updated design doc + measured latency
  decomposition; no production code.*
- **A2. `ReadPlan` classification:** implement §5.1 with exhaustive unit tests over
  AST shapes (R1–R6). No execution changes; `PointRead` temporarily executes by
  delegating to compact path (wiring proof).
- **A3. Path forcing + observability:** `CQLITE_READ_PATH`, metrics counters,
  minimal EXPLAIN/debug output. *Depends: A2.*

### Epic B — BIG point-read execution
- **B1. Partition locator API:** wrap/extract existing Summary→Index lookup (C1)
  behind `locate_partition(&self, key) -> Located | Absent | Unsupported` in new
  `point_read.rs`. *Depends: A1.*
- **B2. Single-partition iterator:** "parse one partition at offset" entry into the
  V5 parser as a new module (respect file-size ratchet on
  `v5_compressed_legacy.rs`), honoring clustering bounds + reversed; handles
  compressed and uncompressed generations (C6). *Depends: B1. Likely the largest
  issue — split bounds/reversed into B2b if needed.*
- **B3. Wire `PointRead` single-generation:** planner → locator → iterator →
  projection for tables with exactly one generation and no memtable; differential
  test on corpus (most corpus tables are single-generation). *Depends: A2, A3, B2.*

### Epic C — Multi-source reconciliation
- **C1. Merge refactor to iterator inputs:** §5.3; compaction parity suite is the
  gate; zero behavior change intended. *Depends: A1. Can proceed parallel to Epic B.*
- **C2. Partition-scoped merge in point path:** N generations + memtable (C7);
  property tests §7.3; tombstone scenarios §6. *Depends: B3, C1.*
- **C3. LIMIT early termination + IN execution order parity.** *Depends: C2.*

### Epic D — Proof & release
- **D1. Differential corpus test in agent-gate + CI.** *Depends: B3 (runs single-gen
  subset), extended after C2.*
- **D2. Benchmarks + profile baselines + report** (numbers for the PR/release
  notes). *Depends: C2.*
- **D3. Docs:** user-docs page on read paths + EXPLAIN; format-guide cross-links;
  limitations page updated (BTI fallback). *Depends: C2.*

### Epic E — Follow-ups (separate from #942 closure)
- **E1. BTI (`da`) fast path** per Ch.21 (trie exactCandidate, Rows.db routing) —
  gated on C4 findings.
- **E2. Filter.db bloom check** with per-generation skip metrics — gated on C5.
- **E3. Promoted-index / intra-partition seek for wide partitions.**
- **E4. Retire compact-to-memory for point reads entirely?** Only after E1/E2 mature
  (keep as forced fallback regardless).

**Definition of done for #942:** Epics A–D merged; differential corpus green in CI;
benchmarks show latency and RSS wins; BTI + bloom follow-ups filed under Epic E.

## 9. Risks

| Risk | Impact | Mitigation |
|------|--------|------------|
| Merge refactor (C1) breaks byte-for-byte compaction parity | Release-blocking regression | Parity suite as the issue's gate; zero-behavior-change PR isolated from feature PRs |
| V5 parser not factorable for single-partition entry without large diff | Schedule; file-size ratchet friction | A1 spike answers early; new module + minimal seams into existing parser |
| Range-tombstone edge cases under clustering bounds | Silent wrong results | Property tests before wiring bounds; residual-filter-only v1 if bounds seek slips |
| Ordering mismatch on IN / multi-key results | Differential test failures late | Pin compact-path ordering in A1; encode in R2 test first |
| Source-map staleness (BTI actually complete?) | Wasted fallback work or wrong scoping | A1 resolves C4 before Epic E scoping |

## 10. Open Questions (resolve during A1 / grooming)

- **OQ-1:** Does Filter.db parsing exist anywhere in `cqlite-core`? (C5)
- **OQ-2:** True state of BTI read path in v0.12.0 — is E1 small or large? (C4)
- **OQ-3:** Where exactly does compact-to-memory live, and is it also the path the
  bindings use? (Bindings must route identically.)
- **OQ-4:** Memtable read semantics in `--writable` mode today. (C7)
- **OQ-5:** Should EXPLAIN be real CQL-surface syntax (design-driven → its own
  OpenSpec change?) or a CLI flag for now? Recommend CLI flag in A3, EXPLAIN as
  possible E-epic.
- **OQ-6:** Does the `state_machine` feature gate the query engine such that the
  plan layer must live behind it? (Feature-flag placement for minimal builds.)
- **OQ-7:** What fraction of the observed 150–500 ms point-read latency is per-query
  setup vs scan/merge vs conversion (§5.6)? If setup-dominated, does a
  session-cached reader-state issue need to join Epic B as a peer deliverable for
  the sub-ms target?

## 11. References

- Issue #942; issue #1406 (uncompressed write claim boundary); epic #1116 (file-split
  doctrine)
- Format guide: Ch.6 (Index/Summary), Ch.7 (Filter.db), Ch.9 (CompressionInfo),
  Ch.10 (Point Reads and Slices), Ch.11 (Merging/Tombstones/Shadowing), Ch.17 (BTI),
  Ch.21 (row-by-key flow card), Appendix F (known limitations)
- Source map: `cqlite-core/src/query/`, `storage/sstable/reader/parsing/
  v5_compressed_legacy.rs`, `storage/sstable/bti/`, `storage/write_engine/merge.rs`
- Existing tests: `cqlite-integration-tests/tests/golden_path_*.rs`
- Cassandra 5.0 source: `BigTableReader.java` (#L298–325 index entry scan),
  `IndexSummary.java` (#L127–152 binary search), `BtiTableReader.java`,
  `TrieIndexEntry.java`, `UnfilteredSerializer.java`
- Doctrine: no-heuristics mandate, gate contract, file-size ratchet, validation
  playbook (agents-developing site)
