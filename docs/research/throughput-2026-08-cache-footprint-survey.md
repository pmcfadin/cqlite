# Lane 2 — Minimizing per-row memory footprint in analytical engines

**2026-08-04, owner-commissioned research survey (lane 2 of 3).** Synthesis + verdicts:
`throughput-2026-08-research-synthesis.md`. Claims tagged MEASURED vs FOLKLORE/inference; our own
numbers (mission doc §0/§6, #3027 (row-decode attribution)) treated as ground truth.

**Bottom line:** our measured 4.75× amplification (~3.3 KB touched per 692.7 B row) and the
LLC-capacity scaling mechanism are the exact symptom pair the vectorized-execution literature was
built to fix — and the canonical fix is not "add threads" or "tune allocators": it is **(a) never
build the intermediate at all, and (b) size the batch so its working set fits a private cache, not
the shared one.** The measured origin for (b) is MonetDB/X100, not DuckDB folklore.

## 1. Morsel-driven parallelism — and why it is NOT our lever

Leis et al. (SIGMOD 2014) schedules ~100K-tuple morsels for **NUMA locality and load balance**
(measured >30× average speedup on 32 cores; "morsel size of about 100,000 tuples yields good
tradeoff") ([paper](https://db.in.tum.de/~leis/papers/morsels.pdf)). A morsel is a *scheduling*
unit, **not a cache-resident unit** — the paper outsources cache-sized processing to the vector
engine. Adopters: DuckDB ([discussion #6632](https://github.com/duckdb/duckdb/discussions/6632)),
Umbra ([SIGMOD 2021](https://db.in.tum.de/~kohn/papers/query-scheduling-sigmod21.pdf)), Velox
([dbdb.io](https://dbdb.io/db/velox)). Our box is 6 cores on effectively one NUMA node — morsel
scheduling buys nothing here. Do not spend effort on it.

## 2. Cache-conscious batch sizing — the measured rule

**MEASURED** ([Boncz/Zukowski/Nes, CIDR 2005](https://www.cidrdb.org/cidr2005/papers/P19.pdf),
§5.1.1 + Fig. 10): X100 default vector 1024; TPC-H Q1 optimum ~1000, "all values between 128 and 8K
work well"; performance deteriorates "when intermediate results do not fit in the cache anymore"
(40 B/row × 8K crossing the 320 KB combined L1+L2 of the test box). Order-of-magnitude swing from
batch sizing alone (~10 s at size 1, ~0.2–0.3 s at 1K, back toward ~1 s at 4M).

**The rule: `batch_rows × bytes_touched_per_row` should fit the PRIVATE L1+L2, not the LLC.**

Engine settings, with provenance:
- **DuckDB** `STANDARD_VECTOR_SIZE` = 2048 ([docs](https://duckdb.org/docs/current/internals/vector)).
  FOLKLORE FLAG: the "chosen to fit L1/L2" rationale appears only in secondary blogs; DuckDB's own
  docs state no cache-fit rationale. The measured justification traces to X100.
- **Velox**: `preferred_output_batch_rows` = 1024, `preferred_output_batch_bytes` = 10 MB,
  `max_output_batch_rows` = 10000 — **sizes by BYTES when row size is known**
  ([configs](https://facebookincubator.github.io/velox/configs.html)). This is the discipline to
  adopt; we know our row size.
- **DataFusion**: default `batch_size` 8192, reduced from 65536 with measured TPC-H SF=10 numbers
  (Q1: 32000→996 ms, 16000→829, 8000→715, 4000→685 — ~1.45× from batch size alone)
  ([PR #9834](https://github.com/apache/arrow/pull/9834)).
- **Photon** (§4.1): batches "to bound memory usage and exploit cache locality"
  ([SIGMOD 2022](https://people.eecs.berkeley.edu/~matei/papers/2022/sigmod_photon.pdf)).

**Applied to CQLite (INFERENCE — verify actual batch size, D2):** at 3.3 KB touched/row, an
8192-row batch ≈ 27 MB/core working set; ×6 cores = 162 MB vs 54 MiB LLC — 3× oversubscribed.
Per-core LLC budget 54/6 = 9 MiB permits ~2,700 rows at 3.3 KB/row, or ~13,600 at the 693 B logical
width. **The amplification factor is what pushes us over the line — which is why footprint levers
fix scaling AND base cost.**

## 3. Eliminating intermediate materialization

Where our 2,343 B/row of `estimate_value_size` + `into_owned` lives.

- **arrow-rs Parquet** decodes column-at-a-time straight into Arrow arrays, never building rows:
  amortized type dispatch, sequential access, "avoid many small heap allocations with a single
  large allocation"; streams in bounded batches; dictionary preservation measured >60× in cases
  ([Arrow blog 2022](https://arrow.apache.org/blog/2022/12/26/querying-parquet-with-millisecond-latency/)).
- **arrow-rs late materialization**: `RowSelection`, `ReadPlanBuilder` (one predicate column at a
  time, tightening selection), `CachedArrayReader` — "the ultimate performance win is not doing I/O
  or decoding at all"
  ([Arrow blog 2025](https://arrow.apache.org/blog/2025/12/11/parquet-late-materialization-deep-dive/)).
- **Velox Lazy Vectors** (§4.2): populate on first use; selective materialization; computation
  pushdown "without having to materialize an intermediate Vector"
  ([VLDB 2022](https://www.vldb.org/pvldb/vol15/p3372-pedreira.pdf)).
- **Photon** (§3.4): columnar-native scan "can skip a possibly expensive column-to-row pivoting
  step." **The inverse is our situation**: SSTables are row-oriented, so one pivot is mandatory —
  but we currently pay it TWICE (bytes → owned row → Arrow) where one (bytes → Arrow builders) is
  required.

**Equivalent restructuring for a row-oriented format:** decode a bounded row window; within it,
write each cell directly into the per-column Arrow builder as parsed — no per-row `Value` tree ever
exists. Column-at-a-time *output* with row-at-a-time *input*. Late materialization applies for
predicate-bearing scans (decode key/predicate columns first, then survivors' projected columns).

## 4. Allocation strategy: arenas, buffer reuse, drop glue

- **Photon buffer pool** (§4.5): transient column batches from an internal MRU pool — "keeps hot
  memory in use for repeated allocations for each input batch"; per-batch allocation count is fixed
  once operators are fixed.
- **Velox Buffers** (§4.2): contiguous, memory-pooled, refcounted, copy-on-write.
- **MEASURED Rust analogue** ([oxc, Rust Magazine](https://rustmagazine.org/issue-3/javascript-compiler/))
  — deep enum trees + allocation churn, the closest published shape to ours:
  - arena for the AST: **~20% improvement** (motivated by profiler showing heavy `drop` time — our
    exact 949 B/row drop-glue symptom);
  - boxing enum variants (to 16 B): **~10%**;
  - `usize`→`u32` span fields: up to 5%.
- **bumpalo** ([docs](https://docs.rs/bumpalo/latest/bumpalo/)): mass deallocation ≈ pointer reset
  — collapses drop glue by construction. Caveat: `Drop` is skipped unless `bumpalo::boxed::Box`;
  resource-owning values need explicit handling.

## 5. SoA vs AoS and enum-heavy `Value` types

- Rust enum size = largest variant + tag; types >128 B trigger `memcpy`; box outsized variants
  ("more likely a net win if the outsized variant is rare")
  ([Rust Performance Book](https://nnethercote.github.io/perf-book/type-sizes.html) — guidance, no
  percentages; the measured numbers are oxc's above).
- AoS with a tagged union pays max_variant_size per element regardless of stored variant; a
  `Value`-shaped enum reaches ~48–56 B ([dense enums](https://alic.dev/blog/dense-enums)); at ~12
  columns that is ~670 B/row of enum scaffolding before payload — nearly doubling a 693 B row.
- **How engines avoid it: no per-row tagged union at all.** Photon stores column values
  contiguously with a null byte-vector + position list; type known once per *column vector*, not
  per *value* (and it tested + rejected the bitmask alternative). Velox: size + type + nullability
  bitmap per Vector. **The tag moves from per-value to per-column — that is the whole trick.**

## Ranked candidates (confidence stated for magnitude, not direction)

1. **Fuse size accounting into decode; delete the standalone `estimate_value_size` traversal.**
   Gap: BOTH. Removes up to 1,398 B/row = 42% of touched bytes; expected 15–30% scan path
   (confidence: high on bytes, medium on wall-clock). Risk LOW — but the v0.13 byte-bounded result
   budgets must keep identical semantics (encoded vs decoded size definitions must agree first).
   Pre-validate: stub it to a constant in a throwaway branch; re-run the #3027 profile (<1 h).
2. **Byte-budgeted batch sizing (adopt Velox's `preferred_output_batch_bytes`).** Gap: 6-core
   scaling primarily. X100 Fig. 10 + DataFusion PR show 1.4–3× when straddling a boundary — which
   the 3× oversubscription estimate says we are (confidence medium; near-zero if current batch is
   already ~1K rows — VERIFY). Risks: per-batch + framing overhead regression (#3096's measured
   framing zero is adjacent); LLC budget is 54 MiB / concurrent_scans, not /6 (admission
   interaction). Pre-validate: sweep 256/1K/2K/4K/8K/16K rows; plot rows/s AND LLC-misses/row at 1
   and 6 cores (~2 h; diagnostic even if nothing changes).
3. **Decode straight into Arrow builders — retire the owned row on the scan→Flight path.** Gap:
   BOTH — the structural fix. Eliminates `into_owned` (945 B/row) + most drop glue (949 B/row) ≈
   58% of touched bytes; composes with #1 to ~99% of measured intermediate traffic. Confidence
   medium-high on bytes, medium end-to-end (Amdahl). Risk HIGH: read-time reconciliation
   (tombstones, TTL, multi-SSTable merge) operates on owned rows — either builders accept
   out-of-order/retracted rows or reconciliation moves upstream. Acceptance gates MUST be the
   query-semantics oracle + point-vs-full differential (#1918) — the JSONL physical goldens are
   blind to reconciliation bugs, and a CQLite-written round-trip is invariant to symmetric errors.
   Pre-validate: prototype ONE fixed-width column on a single-SSTable no-merge path; measure
   bytes-touched/row vs current (~1 day). Do after #1/#2 reveal remaining headroom.
4. **Arena-allocate per-batch decode scratch + shrink the `Value` enum** — the partial-credit
   version of #3. oxc analogue: ~20% (arena, attacking the 949 B/row drop glue) + ~10% (boxing).
   Confidence medium (someone else's numbers, compiler AST not scan pipeline). Risks: arena
   lifetimes at API boundaries; bumpalo skipping `Drop`; boxing costs indirection on hot common
   variants. Pre-validate: (a) `size_of::<Value>()` + per-variant sizes (10 min); (b) synthetic
   100K-row build-and-drop bench, current allocator vs bumpalo (no engine changes).

**Sequencing:** run #4(a)'s size_of probe and #2's batch sweep FIRST (hours, diagnostic regardless
of outcome). Then #1 (cheapest real win, best byte-per-effort). Use results to decide #3 vs #4.
Given #3096 (two levers measured at exactly zero), **a day of measurement before a week of
refactoring is the pattern our own history endorses.**
