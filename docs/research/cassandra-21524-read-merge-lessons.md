# CASSANDRA-21524 "Row Merging Logic Optimizations" — lessons for CQLite's read-path merge

**Date**: 2026-07-30. **Method**: three parallel research lanes — (1) JIRA ticket + PR review
archaeology, (2) commit-level diff analysis of `apache/cassandra@1e3d43b2` (PR #4941), (3) a map of
CQLite's own merge machinery — then adjudication against CQLite's field measurements (#2818).
**Outcome**: issues #3174 (reconcile allocation cut) and #3176 (empty-source scan skip) filed at
Backlog, contingent on #3023 profiling; the singleton-fast-path idea was filed (#3175) and closed
same-day as a duplicate of the owner-dispositioned #2822.

## 1. What Cassandra did and what it bought

CASSANDRA-21524 (Dmitry Konstantinov, reviewed by Francisco Guerrero; 6.0-alpha2/7.0) is a
CPU-tuning pass over `Row.Merger` / `MergeIterator` / `ColumnDataReducer` — one child of the
read-path CPU family under epic C-21287 (siblings: C-21359 merge-alloc reduction, C-21526
megamorphic `Cell.timestamp`, C-21536 `AbstractType.writeValue` profile pollution, C-21363 skip
cell iteration when no tombstones).

Measured (cassandra-stress, 10-row partition reads, warm page cache, CPU-bound, m8i.4xlarge,
JDK 21, async-profiler): **170,119 → 183,139 op/s (+7.7%)** over two rounds; `Row$Merger` CPU
flamegraph share **11.13% → 6.66%**. The win was pure CPU — GC totals were identical. Caveats:
single-run pairs, no variance reported, p99/p99.9 marginally worse and undiscussed.

The seven techniques, with portability verdicts for a Rust consumer:

| # | Technique | Class | Rust portability |
|---|---|---|---|
| 1 | Skip empty iterators for null rows + loop fusion — merge heap sized to *live* inputs (10-way → 2-way when only 2 sources hold the row) | algorithmic | **High** — the heap-arity shrink is the biggest real win in the patch |
| 2 | Build-time textual class duplication of `MergeIterator` (`RowMergeIterator` / `UnfilteredMergeIterator` / `ComplexCellMergeIterator`) to de-pollute JIT call-site profiles | **JVM-only** | **N/A** — Rust monomorphization gives this free; the transferable lesson is *avoid `dyn` on the per-cell hot path* |
| 3 | Fold `min(minLocalDeletionTime)` across inputs during the merge loop; if it stays `MAX`, skip the whole-B-tree rescan in `BTreeRow.create` (conservative one-sided fast path; any deletion/expiry falls back to the full scan) | algorithmic | **High** in principle — but CQLite has no per-row B-tree rescan to skip (§3) |
| 4 | Reference-equality short-circuit in `useColumnMetadata` before the version comparator | algorithmic | High — `Arc::ptr_eq` / id compare before structural compare |
| 5 | `DeletionTime.deletes(cell)`: test `markedForDeleteAt != LIVE` (cheap scalar) before the megamorphic `cell.timestamp()` | hybrid | Medium-high — cheap-scalar-first shape survives; the megamorphic motivation does not |
| 6 | `ColumnDataReducer.versions` List → pre-sized array + explicit size, `Arrays.fill` on key change (allocate once per reducer, clear per key) | algorithmic | Low as written (`Vec` already is this) — the **reusable-scratch-buffer discipline** is the takeaway |
| 7 | `replaceAndSink` hot/cold split with `@DontInline` on the binary-heap sink (plus a genuinely new no-children early exit) | hybrid | Medium — `#[cold]` / `#[inline(never)]` is the analogue |

Only opt-3 got a dedicated test (`testMergerMinLocalDeletionTime`); the rest ride the existing
merge suite. No JMH; justification was mechanism + flamegraph throughout. Nothing was rejected in
review; the sharpest review question was cold-path coverage of the new sink split.

## 2. CQLite's merge machinery (as mapped 2026-07-30)

One kernel, many drivers: `KWayMerger`
(`cqlite-core/src/storage/write_engine/merge/mod.rs`) with buffered (`step()`) and row-granular
streaming (`streaming.rs`, #1668/#2230) drivers; point-read builders in `point_read.rs` (#2207);
warm-reader construction in `from_readers.rs` (#2346); read-path entry via
`generation_merge.rs`; Flight producers in `cqlite-flight/src/producer_*.rs` (#2423/#2230);
producer-thread/channel adapter in `producer_iter.rs`/`producer_msg.rs` (#3120), egress budget in
`egress_budget.rs` (#2765). Reconciliation is the 7-step `ReconcileState` pipeline
(`merge/reconcile.rs`), orchestrated from `reconcile_cluster_with_overlap_counted`.

Dispatch: dynamic at the run boundary (`Box<dyn SSTableRowIterator>`, Flight's
`&mut dyn RowStepper`/`&mut dyn BatchSink`) but amortized over an ~8 KB `VecDeque` refill;
everything below the run boundary (heap comparator, reconcile) is monomorphic. So the opt-2
anti-pattern is already mostly avoided.

Findings against the seven techniques:

- **Opt-1 (empty sources)**: point-read already skips (`PathProbe::Empty` → no run;
  `runs.is_empty()` → `Ok(None)`). Full-scan/warm construction does **not** — `new_from_readers`
  and `new_cancellable` spawn an OS producer thread + bounded channel + `RunReader` per input
  unconditionally, even for a generation contributing zero rows to the scan range. → **#3176**.
- **Opt-3 (min-LDT caching)**: no analogue. CQLite stores rows in flat `Vec`/`HashMap`s; there is
  no per-merged-row tree rescan. The only mild cousin: `effective_gc_settings()` is documented
  merger-lifetime-constant but re-derived per partition (trivial; a no-op on the read path).
- **Opt-4/5 (fast paths)**: range-tombstone shadowing already has an empty fast path + binary
  search (#1669); partition shadowing has an early `None` return. The *cluster-level* singleton /
  all-live bypass is absent — but see §3: it was measured and dispositioned.
- **Opt-6 (allocation discipline)**: the strongest mismatch in CQLite's favor of action.
  `resolve_cell_winners` clones a `CellKey = (String, Option<Vec<u8>>)` **per cell per source
  before any winner test**, clones the key again into the ordering `Vec`, and clones the full
  `CellData` on insert/replace; 4 fresh `Vec`s + a `HashMap` per clustering group with no reuse;
  the clustering-key-name `HashSet` is rebuilt **three times per clustering group** from
  schema-constant data (the scan path builds its equivalent once per scan);
  `complex_deletions.contains` is a linear scan per carried deletion. → **#3174**.
- **Opt-7 (hot/cold)**: already structurally done — 7 named steps, `#[inline]` predicates in
  `reconcile_rules.rs`, cold work behind `is_empty()`/`is_some()` guards. Low headroom.

Existing oracles ready for any of this work: `benches/reconcile_overlap.rs` (#2043, k-curve
record in `issue-2043-reconcile-overlap-multiplier.md`), `merge/clone_regression_tests.rs`,
`merge/reconcile_microalloc_tests.rs`, `MergeEntryCloneScope` work counters.

## 3. The adjudication that matters: our field data already priced this lever class

Cassandra's flamegraph had row merging at **11.1%** of CPU. CQLite's field decomposition (#2818,
`perf` on-CPU, i4i.xlarge, compressed corpus, server-direct) has the k-way merge + reconcile
bucket at **3.2% (v0.16.0) – 6.4% (0.17-dev)** — the CPU lives in **row decode (26.7%)** and the
**allocator (19.6%)**. That is why #2822 (L3 reconcile singleton fast-path — precisely
Cassandra's opts 1/4/5 at cluster level, independently proposed here before this research) was
closed on owner direction: a generous 2× on a ~6% bucket returns ~3% end-to-end, below harness
noise. The 21524 idea set is *correct*; our workload shape just doesn't have the same hot spot.

Consequences:

- **#3175 (single-source/all-live fast paths) — filed from this research, then closed same-day**
  as a duplicate of the dispositioned #2822. The standing disposition holds.
- **#3174 (reconcile allocation cut) — kept, Backlog**: it targets the **allocator bucket
  (19.6%, WS2 of umbrella #3023)**, not merge CPU. Its value is contingent on #3023's local
  profiling attributing a material slice of allocator time to reconcile-path allocations rather
  than decode-side ones. Adjudicate there before promoting.
- **#3176 (empty-source scan skip) — kept, Backlog**: its value is **per-scan setup cost**
  (thread + channel + open + egress slot per zero-contribution generation) rather than per-row
  CPU — most relevant for many-generation tables under scan concurrency (#2420). Mandatory
  co-design with #2820 (batch fan-in) / #2765 (egress budget), same channel machinery.

## 4. Transferable lessons regardless of scheduling

1. **Measure before monomorphizing.** Cassandra's headline technique (build-time class
   duplication) is a JVM workaround for what Rust generics already provide. When reviewing our
   code, the equivalent smell is `dyn` dispatch inside a per-cell loop — we currently only have
   `dyn` at the per-run boundary, amortized by buffering. Keep it that way.
2. **Conservative one-sided fast paths** (opt-3's design: use the cached aggregate only where it
   is provably exact, fall back otherwise) is the correct safety shape for any future reconcile
   bypass — and is exactly what #2822's acceptance criteria demanded (differential parity at a
   pinned `now`, fast path forced off in one arm).
3. **Scratch-buffer discipline** (allocate once, clear per key) is Cassandra's opt-6 and our
   biggest concrete gap (`reconcile.rs`). Whatever #3023/WS2 decides, new merge-path code should
   not add per-cell heap allocations.
4. **Their benchmark discipline was thinner than ours** — single runs, no variance, tail
   regressions unremarked. Our criterion + `perf-gate.json` + validity-guard practice is the
   stronger standard; hold any future merge-path change to it.
5. **Sibling tickets worth a later look**: C-21363 (skip cell iteration when no tombstones — the
   `BTreeRow#hasLiveData` shape), C-21359 (Candidate footprint shrink), C-21526/21536 (profile
   pollution — JVM-specific, low relevance).

## 5. Primary sources

- JIRA: https://issues.apache.org/jira/browse/CASSANDRA-21524 (benchmarks in comments, Jul 15–16
  2026 runs; attachments `jul15_merge_check_*_cpu.html`, `cpu_merger_{before,after}.png`).
- PR: https://github.com/apache/cassandra/pull/4941 (review threads: sink cold-path coverage,
  `gen-java-copies` IDE wiring, `useColumnMetadata` javadoc).
- Commit: `apache/cassandra@1e3d43b2ddb22ebcdd5a835a55b2a58e8977d9db` — 9 files:
  `build.xml`, `DeletionTime.java`, `BTreeRow.java`, `Row.java`, `UnfilteredRowIterators.java`,
  `index/sai/utils/RowWithSource.java`, `utils/MergeIterator.java`, `RowsTest.java`,
  `CHANGES.txt`.
- CQLite adjudication: #2818 (M0 CPU decomposition), #2822 (closed disposition), #3023
  (umbrella), #2043 record `docs/research/issue-2043-reconcile-overlap-multiplier.md`.
