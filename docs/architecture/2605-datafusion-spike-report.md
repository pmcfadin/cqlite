# DataFusion `TableProvider` spike — measurement report (issue #2605)

**Status:** spike complete, throwaway by construction. Feature `datafusion-spike`, non-default.
**Sharpened by:** `docs/architecture/throughput-program-2026-07.md` M15 (#2605 sharpen).
**Feeds:** the #941 promotion decision (`docs/architecture/941-datafusion-decision-brief-2026-07.md`)
and the columnar-producer slot trigger (M15: `>1.3x` on wide/overlap → revisit; else Stage-3 prep).

---

## 0. Read this first — corpus provenance

**This is NOT the Round-12 (R12) dataset.** R12's `~10.6k rows/s/pod` full-scan figure
(`941-datafusion-decision-brief-2026-07.md` L13) was measured on a field cluster this box does not
have, and a worktree ships no `Data.db` binaries at all (the largest locally-available fixture tops
out at ~0.6 MB, which would produce a number worse than no number).

What was measured instead is a **shape-matched corpus generated on this box**: real Apache Cassandra
5.0, `cassandra-stress` user profile, `>= 4 KB` wide rows, **multiple overlapping SSTable
generations**, LZ4-compressed, field widths matched to `gen-perf-corpus-3068.sh`'s `wide_4kb` so it is
comparable to the other local perf corpora. Generation script:
`test-data/scripts/gen-df-spike-corpus-2605.sh`.

Consequences, stated plainly:

* **Absolute `rows/s` here are NOT comparable to R12's `10.6k rows/s/pod`.** Different hardware,
  different page-cache state, different node count, no network, no gRPC egress.
* **The load-bearing number is the RELATIVE engine delta** — row engine vs DataFusion over identical
  batches, and both against the shared batch-production floor. Those ratios are properties of the
  code, measured on one box, and they are what the M15 trigger rule is written against.
* Nothing in this report licenses a `rows/s/pod` claim.

---

## 1. What was built

A **thin**, feature-gated DataFusion `TableProvider` over the **existing** Flight scan path, plus a
bench harness. **Zero production wiring** and **no new decode work**: every byte read goes through
`MergeProducer::produce_streaming` — the same call the streaming `do_get` route makes.

| Path | Role |
|---|---|
| `cqlite-flight/src/df_spike/scan.rs` | The ONE batch-production seam both arms consume; sub-phase timing readback; read-arm evidence |
| `cqlite-flight/src/df_spike/pushdown.rs` | DataFusion `Expr` → CQLite predicate translation, fail-closed |
| `cqlite-flight/src/df_spike/provider.rs` | `TableProvider` (`schema`, `scan`, `supports_filters_pushdown`) |
| `cqlite-flight/src/df_spike/exec.rs` | Single-partition `ExecutionPlan` → `SendableRecordBatchStream` |
| `cqlite-flight/src/df_spike/rowwise.rs` | Row-at-a-time arm (the row-engine analogue) |
| `cqlite-flight/src/df_spike/rss.rs` | Per-run peak-RSS sampling |
| `cqlite-flight/src/df_spike/bench.rs` | Scenario/arm matrix + the JSON result record |
| `cqlite-flight/src/bin/df_spike_bench.rs` | The harness binary (`required-features = ["datafusion-spike"]`) |
| `cqlite-flight/src/df_spike/tests.rs` | 11 tests: pushdown classification + the arm-equivalence oracle |

**Why it lives inside `cqlite-flight` and not in a new crate:** the streaming seam it drives is
`pub(crate)` — `MergeProducer::produce_streaming`, the `BatchSink` trait, `ScanProgress`,
`CreditedBatch`. A separate crate could only have reached `produce()`, which materializes the whole
result set into a `Vec<RecordBatch>` and is unusable at corpus scale under a 512Mi budget.

**Why `produce_streaming` and not `produce()`:** `produce()` is fully materializing. The spike's
resident payload is bounded structurally instead: at most `CHANNEL_CAPACITY (2) x max_batch_bytes
(4 MiB default)` in flight plus the one batch the consumer holds, independent of result size. The
peak-RSS column in §4 measures that claim rather than asserting it.

---

## 2. Method

### 2.1 Arms

| Arm | What it does | Batches |
|---|---|---|
| `floor` | Stream batches, discard. The **shared batch-production floor** — the ceiling ANY execution engine is capped by | identical |
| `row_engine` | Row-at-a-time evaluation over the produced batches | identical |
| `datafusion` | DataFusion SQL through the `TableProvider`, **pushdown OFF** | identical |
| `row_pushdown` | Reference arm: the real production `do_get` shape, projection + predicate pushed into `ScanSpec` | **narrowed on purpose** |

`datafusion` runs with **pushdown disabled** for the headline comparison. That is deliberate: with
pushdown on, the DataFusion arm narrows the scan itself and would look faster **because it did less
work**, not because vectorized execution is faster. The `vectorized-exec` delta M15 asks for is only
meaningful over identical batches. `row_pushdown` reports separately what pushdown buys, and its
batches are never compared row-for-row against the other arms'.

### 2.2 The row-engine arm understates the row engine — deliberately

`rowwise.rs` downcasts each column **once per batch** and then indexes it. CQLite's production row
engine evaluates predicates against a `QueryRow` (`HashMap<String, Value>`), so per cell it
additionally pays a string hash lookup and a `Value` enum construction that this arm does not. Any
vectorized advantage reported below is therefore a **lower bound** on the advantage over the
production row path — the direction an honest measurement should err in.

### 2.3 Both arms consume post-reconciliation batches (M15 item 4)

`row_source.rs` documents two arms: the k-way merge (`StreamingMerger` → `RowStepper`) and the
single-generation `bypass::ScanRowSource`. If the bypass arm served the scan, the comparison would be
measuring a **correctness** difference, not an engine difference. Three independent facts pin the
merge arm:

1. `produce_streaming`'s **path-based (cold) route builds a `KWayMerger` unconditionally** — the
   bypass exists only on the warm reader-based route (`produce_streaming_from_readers`).
2. The corpus presents **>= 2 post-prune `*-Data.db` sources**, asserted from the authoritative
   listing, so there is genuinely something to reconcile across.
3. `cqlite_core::storage::read_path_probe` counters (`reconcile_entries`, `cell_metadata_maps`) are
   incremented **only on the merge arm**; the harness records the delta per run and **fails closed**
   when it is zero. That is a direct observation of the work, not a timing correlation.

The harness additionally rejects any scenario whose comparable arms did not scan the same number of
rows, and rejects a 0-row scan outright (an empty corpus is a failure, never a fast result).

### 2.4 Instrumentation: the existing sub-phase counters, not new timing

Decode-to-column is read from the always-compiled `#2819` instrument
(`cqlite_core::observability::stream_subphase`), the same accumulator the
`cqlite.rpc.phase.duration` sub-phase histograms are emitted from:

| Counter | Meaning |
|---|---|
| `stream_decompress` | LZ4 chunk decompression, per-SSTable producer thread(s) |
| `stream_merge` | k-way merge + reconcile + per-row materialize, merge-consumer thread |
| `stream_encode` | **Arrow array build — the row→column transpose.** This is the decode-to-column figure |
| `stream_cold_fault` | Cold body-chunk page-in |
| `stream_grpc_write` | Egress channel send incl. backpressure park |

The sub-phases run on concurrent pipeline threads and **overlap in wall clock**, so they are not
expected to sum to elapsed time.

### 2.5 Correctness guard: `Exact` pushdown is never claimed for a predicate the scan does not apply

A provider that reports `TableProviderFilterPushDown::Exact` tells DataFusion "do not re-check this".
If it then fails to apply the predicate, rows survive that should not — and in a **benchmark** that
shows up as the DataFusion arm being *faster because it is wrong*. `pushdown.rs` is fail-closed:

* Translation targets the **public ticket `PredicateExpr`** and is then validated through
  **production's own** `filter::lower_predicate_expr`, so operand coercion and Kleene semantics are
  inherited rather than re-derived. A lowering failure ⇒ `Unsupported`.
* `supports_filters_pushdown` and `scan` call **one** translation function, so the verdict can never
  disagree with what is actually pushed.
* Casts, column-vs-column comparisons, `NULL` literals, timestamps/dates/decimals and every other
  operand whose CQLite↔DataFusion coercion is unproven are `Unsupported` by construction.
* `<>` becomes `NOT (col = v)` — a negation, never a silent substitution of `=`.

Tests (`df_spike/tests.rs`, 11 passing) pin this: `Exact`/`Unsupported` classification incl. mirrored
literal-first operands; and, over a two-generation fixture with an LWW overwrite and a row tombstone
at a **pinned `now`**, (a) the DataFusion arm returns the row engine's rows, values and order, and
(b) an `Exact` pushdown selects **exactly** the rows DataFusion's own `FilterExec` selects.

---

## 3. Results

### 3.0 Corpus and matrix

| Property | Value |
|---|---|
| Written by | Apache Cassandra **5.0.2** (`cassandra-stress` in the official image) — real `nb`/BIG bytes, not CQLite's |
| Table | `perf_2605.wide_4kb`, `PRIMARY KEY (pk, ck)`, 12 columns, `>= 4 KB` rows |
| Compression | LZ4, 16 KiB chunks — **measured** from each `CompressionInfo.db`, not read off the DDL |
| `*-Data.db` | **2** (merge depth k=2), 10.35 GB total |
| Generations | gen1 190k partitions / 1.9M rows, then gen2 57k partitions / 570k rows at newer timestamps |
| Overlap | **28.8 %** measured (`writetime` over a fixed 1 % token slice, before/after gen2) |
| Rows scanned per run | **1,899,750** — identical across every comparable arm, asserted |
| Matrix | 3 scenarios x 5 arm configs x 3 iterations = **45 runs**, one PROCESS per run |
| Ordering | iteration outermost, arm order **rotated** per iteration |

The 1,899,750 rows against the manifest's nominal 1,900,000 is not a loss: `cassandra-stress`
draws `pk` from `uniform(1..1e9)` over 190,000 seeds, so a handful of seeds collide onto the same
partition key. The count is identical in all 45 runs.

**RF is nominal only.** The DDL records `SimpleStrategy RF=3`, but a single-node container stores
exactly one replica. This corpus delivers M15's **wide** and **overlap** halves; the property
actually exercised is cross-SSTable overlap (read-time reconciliation), **not** replication. Do not
cite it as an RF=3 measurement.

### 3.1 Two methodology corrections, made before the numbers were believed

**(a) The first run reported a 1.6x DataFusion win that was not real.** With the harness's own
scenario-then-arm-then-iteration loop in a single process, the DataFusion arm measured 64.6 s against
the floor arm's 101-108 s over *identical* batches. Re-run with one process per cell and the arm
order rotated, the same cell measured 119.0 s — **slower** than the floor. The 1.6x was
ordering-plus-page-cache artefact. Both properties were fixed
(`docs/reports/2605-datafusion-spike-artifacts/run-matrix.sh`): peak RSS is now genuinely per-arm
(process RSS never returns to its start, so a second arm in one process inherited the first's
high-water mark), and no arm is systematically favoured by cache warming.

**(b) Raw wall time on this box cannot resolve the engine delta, and the data proves it rather than
merely suggesting it.** Per-iteration wall time for one cell swings up to **2.4x** (e.g.
`row_pushdown/filtered_scan` `[57.4 s, 138.8 s]`), and the **`floor` arm — which by construction does
strictly LESS work than every other arm — measures SLOWER than `row_engine` and both DataFusion arms
on two of three scenarios.** That is impossible in expectation, so the wall-clock channel is
noise-dominated, and quoting a wall-time ratio as a "vectorized-exec delta" would be reporting cache
luck as an engine property.

The confounder is identified, not guessed: the corpus is 10.35 GB, the box has 30 GB with the 7 GB
Cassandra container that produced it still resident, and `stream_cold_fault` measures exactly the
synchronous page-in this causes. §3.3 therefore controls for it.

### 3.2 Raw per-cell results (median of 3 iterations, with the range)

| scenario | arm | wall s (median) | [min, max] | rows emitted/s | batches | encode ms | merge ms | decompress ms | cold-fault ms (sum over 2 producer threads) | peak RSS MiB | rows result |
|---|---|---|---|---|---|---|---|---|---|---|---|
| full_scan_count | floor | 124.1 | [99.9, 169.4] | 15308 | 1908 | 6118 | 30604 | 1722 | 156830 | 37.0 | 0 |
| full_scan_count | row_engine | 99.8 | [92.7, 140.3] | 19026 | 1908 | 5802 | 27648 | 1733 | 130130 | 36.8 | 1899750 |
| full_scan_count | datafusion@tp1 | 91.8 | [78.1, 95.9] | 20693 | 1908 | 5649 | 28087 | 1760 | 114299 | 48.3 | 1899750 |
| full_scan_count | datafusion@tp16 | 73.0 | [71.5, 119.0] | 26034 | 1908 | 5645 | 27977 | 1769 | 83578 | 48.8 | 1899750 |
| full_scan_count | row_pushdown | 88.6 | [59.2, 111.4] | 21451 | 1908 | 5809 | 29476 | 1831 | 97639 | 37.0 | 1899750 |
| projected_scan | floor | 81.0 | [66.7, 99.8] | 23458 | 1908 | 5484 | 27975 | 1806 | 97297 | 36.8 | 0 |
| projected_scan | row_engine | 92.1 | [81.9, 118.9] | 20621 | 1908 | 5734 | 28018 | 1752 | 120167 | 36.8 | 1899750 |
| projected_scan | datafusion@tp1 | 89.7 | [71.3, 102.9] | 21185 | 1908 | 5769 | 27762 | 1744 | 111877 | 47.2 | 1899750 |
| projected_scan | datafusion@tp16 | 61.8 | [61.0, 100.9] | 30749 | 1908 | 5578 | 28386 | 1824 | 64774 | 46.9 | 1899750 |
| projected_scan | row_pushdown | 56.7 | [48.3, 76.1] | 33494 | 232 | 788 | 22112 | 1469 | 67378 | 29.4 | 1899750 |
| filtered_scan | floor | 120.6 | [80.1, 129.7] | 15748 | 1908 | 5864 | 29786 | 1732 | 155382 | 36.8 | 0 |
| filtered_scan | row_engine | 93.2 | [72.3, 116.4] | 20375 | 1908 | 5772 | 27387 | 1744 | 122390 | 36.9 | 937602 |
| filtered_scan | datafusion@tp1 | 101.5 | [80.9, 108.9] | 18722 | 1908 | 5820 | 27332 | 1736 | 129061 | 50.1 | 937602 |
| filtered_scan | datafusion@tp16 | 98.9 | [72.4, 100.6] | 19213 | 1908 | 5837 | 27709 | 1777 | 128138 | 51.2 | 937602 |
| filtered_scan | row_pushdown | 84.4 | [57.4, 138.8] | 11113 | 942 | 3228 | 28566 | 1730 | 105482 | 36.7 | 937602 |

`datafusion@tp1` pins DataFusion's `target_partitions` to 1 so its thread count matches the
single-threaded direct arms; `datafusion@tp16` is DataFusion's default (one per core on this
16-core box). `row_pushdown`'s scan is narrowed on purpose, so its `rows emitted/s` denominator is
post-filter and its batch count differs — it is never compared row-for-row against the others.

No `grpc-write` column: that sub-phase counter is fed by production's `ChannelSink`, not the spike's
sink, so it reads 0 because it was never instrumented — not because the send was free.

### 3.3 The answer: with I/O controlled, all four engines are indistinguishable

- wall = 13.40 s + 0.696 x cold_fault_s  (R^2 = 0.980 over 45 runs)
- i.e. 98% of the wall-time variance across every run in this matrix is explained by page-in time ALONE

| scenario | arm | mean residual s (+ = slower than I/O predicts) | residual [min, max] |
|---|---|---|---|
| full_scan_count | floor | +2.1 | [-2.3, +7.0] |
| full_scan_count | row_engine | +0.2 | [-4.1, +4.5] |
| full_scan_count | datafusion@tp1 | -0.1 | [-2.3, +3.2] |
| full_scan_count | datafusion@tp16 | +0.4 | [-1.5, +1.4] |
| full_scan_count | row_pushdown | +2.7 | [-2.8, +7.2] |
| projected_scan | floor | -0.6 | [-2.8, +1.2] |
| projected_scan | row_engine | -2.5 | [-4.9, -0.4] |
| projected_scan | datafusion@tp1 | -0.6 | [-1.6, +1.2] |
| projected_scan | datafusion@tp16 | +1.0 | [-2.9, +3.3] |
| projected_scan | row_pushdown | -4.6 | [-8.5, -1.7] |
| filtered_scan | floor | +2.5 | [-0.9, +6.4] |
| filtered_scan | row_engine | +1.5 | [-5.3, +9.9] |
| filtered_scan | datafusion@tp1 | -0.9 | [-2.4, +1.4] |
| filtered_scan | datafusion@tp16 | -1.9 | [-3.7, +0.6] |
| filtered_scan | row_pushdown | +0.8 | [-2.4, +3.0] |

**`R^2 = 0.980`. Ninety-eight percent of the wall-time variance across all 45 runs is explained by
page-in time alone.** With that covariate removed, every arm's mean residual sits within
**+/-2.7 s on a ~100 s scan — under +/-3 %**, and the residual ranges overlap zero everywhere. There
is no vectorized-execution advantage to find on this shape: the pipeline is not
execution-bound, it is I/O-bound and then producer-CPU-bound, and DataFusion changes neither.

Two further readings of the same table:

* `datafusion@tp16` (DataFusion's default parallelism) is **not** better than `tp1` once I/O is
  controlled. Its apparent raw-wall lead (73.0 s vs 91.8 s on `full_scan_count`) tracks its lower
  cold-fault (83.6 s vs 114.3 s) exactly. Over a **single-partition** scan DataFusion's extra
  workers have nothing to parallelise except draining and dropping batches.
* `row_pushdown` on `projected_scan` is the one arm with a consistently NEGATIVE residual
  (**-4.6 s**, range `[-8.5, -1.7]`) — the only engine-attributable win in the matrix, and it comes
  from **narrowing the scan**, not from vectorizing execution (see §3.5).

### 3.4 The stable signal: producer CPU sub-phases

| bucket | median ms over all 45 runs | [min, max] | us/row at 1,899,750 rows |
|---|---|---|---|
| stream_encode (row->column transpose) | 5686 | [750, 6355] | 2.99 |
| stream_merge (merge + reconcile + row materialize) | 27971 | [21350, 33673] | 14.72 |
| stream_decompress (LZ4) | 1752 | [1460, 1891] | 0.92 |
| stream_cold_fault (page-in, 2 threads summed) | 114299 | [52612, 214181] | 60.17 |

These are stable to a few percent across all 45 runs (`stream_encode` `[5.48, 6.36] s` excluding the
projection-pushdown cells), which is why they — and not wall time — carry the load-bearing figures.

**M15 item 1, the two deltas, reported separately:**

* **decode-to-column delta = `stream_encode` = 5.69 s per 1.9M-row scan = 2.99 us/row.** This is the
  Arrow array build: the row -> column transpose, and precisely what a columnar producer would
  eliminate. `stream_merge` (**27.97 s, 14.72 us/row**) is the k-way merge + reconcile + per-row
  materialize that produces the rows being transposed — **4.9x the transpose cost**. Both run on the
  same merge-consumer thread, so they add: **33.66 s of consumer CPU per scan**, of which the
  transpose is **16.9 %**.
* **vectorized-exec delta = none measurable.** `<= +/-3 %` with I/O controlled (§3.3); the sign is not
  even consistent across scenarios.
* **shared batch-production floor.** The `floor` arm's medians are 124.1 / 81.0 / 120.6 s, but its
  honest expression is the CPU ceiling it implies rather than its cache-dependent wall time:
  33.66 s of merge-consumer CPU for 1,899,750 rows = **~56.4k rows/s** as this pipeline's
  single-consumer-thread CPU ceiling on this box. Every execution engine is capped by that number,
  and all four measured within noise of it once I/O was removed.

**The consequence for a columnar producer, quantitatively.** Removing the transpose ENTIRELY — the
absolute best case, assuming a columnar producer costs nothing and the merge/materialize side is
unchanged — moves the consumer-CPU ceiling from 33.66 s to 27.97 s: **56.4k -> 67.9k rows/s, i.e.
1.20x**. That is *below* M15's 1.3x revisit trigger, and it is an upper bound, not an estimate.

### 3.5 The lever the matrix did find: projection pushdown into the scan

`row_pushdown` on `projected_scan` pushes `pk, ck, v_int` (3 of 12 columns) into `ScanSpec.projection`,
so the producer never materializes the wide `body`/`payload`/`note` columns:

| | floor (all 12 columns) | row_pushdown (3 columns) | change |
|---|---:|---:|---:|
| `stream_encode` | 5484 ms | **788 ms** | **-86 %** |
| `stream_merge` | 27975 ms | **22112 ms** | -21 % |
| batches emitted | 1908 | **232** | -88 % (the 4 MiB byte cap stops tripping) |
| peak RSS | 36.8 MiB | **29.4 MiB** | -20 % |
| I/O-controlled residual | -0.6 s | **-4.6 s** | the matrix's only consistent engine win |

**Predicate pushdown, by contrast, does not pay on this shape.** `row_pushdown` on `filtered_scan`
still reads every byte (the predicate is on a clustering column, so nothing narrows I/O) and its
residual is `+0.8 s` — it adds per-row filter work without removing any. That is a useful negative
result for the connector: push *projections* eagerly; pushing a non-partition-key *predicate* buys
nothing until it can prune SSTables or partitions.


---

## 4. Peak memory vs the B4 512Mi pod budget (M15 item 3)

Peak RSS is sampled per run from `/proc/self/statm` at 20 ms, in the run's OWN process, and is
reported as `unmeasured` (never `0`) if it cannot be read.

| scenario | floor | row_engine | datafusion@tp1 | datafusion@tp16 | row_pushdown |
|---|---:|---:|---:|---:|---:|
| `full_scan_count` | 37.0 MiB | 36.8 MiB | 48.3 MiB | 48.8 MiB | 37.0 MiB |
| `projected_scan` | 36.8 MiB | 36.8 MiB | 47.2 MiB | 46.9 MiB | **29.4 MiB** |
| `filtered_scan` | 36.8 MiB | 36.9 MiB | 50.1 MiB | 51.2 MiB | 36.7 MiB |

**Maximum across all 45 runs: 51.2 MiB, i.e. 10.0 % of the B4 512 MiB pod budget.** No arm came near
it, and nothing here exceeds it — so there is no finding to report in that direction.

Reading the table:

* The **direct arms sit at ~37 MiB** over a 10.35 GB corpus, which is the structural bound working as
  designed: at most `CHANNEL_CAPACITY (2) x max_batch_bytes (4 MiB)` in flight plus the consumer's
  batch, independent of result size. `MergeProducer::produce()` was never called precisely because it
  would have made this figure result-size-proportional (several GB) and unusable.
* **DataFusion adds ~11-14 MiB** — its `SessionContext`, plan and per-partition operator state. Modest,
  but it is the arm that would grow with a real multi-operator plan (joins, sorts, spill buffers), so a
  production integration would need its own budget analysis rather than inheriting this number.
* **Projection pushdown REDUCES peak RSS by 20 %** (36.8 -> 29.4 MiB), for the same reason it reduces
  encode time: the wide columns are never materialized.

Caveat: `/proc/self/statm` resident pages include file-backed pages, and the page size is assumed to
be 4 KiB (stated in `rss.rs`, not silently assumed). These are reporting figures, not an allocator
audit; the dhat-based `memory-budget` gate component remains the authority for the production
producer's allocation bound.


---

## 5. Dependency and build-time impact (AC4)

### 5.1 Pin, and why it is not upgradeable here

`datafusion = "44.0.0"`, optional, `default-features = false`. **DF 44 is the last line that resolves
`arrow` 53.x** — verified by real resolution, not by reading a changelog: DF 44.0.0 pulls
`arrow 53.4.1`, the **exact** version `cqlite-flight` and `arrow-flight 53` already use, so a
`RecordBatch` produced by the Flight producer is handed to DataFusion with **zero conversion**. DF >= 45
moves to `arrow` 54 and forks the Arrow type graph — two structurally identical but mutually
incompatible `RecordBatch`/`Schema` types in one binary — which destroys the entire reuse premise of
this spike. Do not bump the major.

It compiles clean on the pinned `rustc 1.97.1` with `RUSTFLAGS="-D warnings"`.

(The `arrow 54.2.1` already in `Cargo.lock` comes from `duckdb 1.2.2` behind an optional `cqlite-cli`
feature and is unrelated.)

### 5.2 Measured impact

| Measure | Feature OFF | Feature ON | Delta |
|---|---:|---:|---:|
| Workspace `Cargo.lock` packages | 658 | 695 | **+37** |
| `cqlite-flight` resolved dep graph (unique crates compiled) | 199 | 277 | **+78** |
| Clean `cargo build -p cqlite-flight --release` (sccache disabled, 16 cores) | 142 s | 373 s | **+231 s (2.6x)** |
| `target/release` size | 599 MiB | 1021 MiB | **+422 MiB** |

Method: `CARGO_TARGET_DIR` pointed at a fresh directory per arm with `RUSTC_WRAPPER=` (sccache
disabled) so both arms are genuinely cold; dep counts from `cargo tree --edges normal`.

The `+78`-vs-`+37` gap is not a contradiction: `+37` is the number of packages **new to the lock**,
while `+78` is the number of crates **newly reachable from `cqlite-flight`** — 41 of them (`chrono`,
`half`, `object_store`, `petgraph`, `sqlparser`, the `parquet`/`arrow` leaves DataFusion needs, ...)
were already in the lock for other workspace members and are now compiled for this crate too. For a
promotion decision the honest cost is the **`+231 s` build time and `+422 MiB` of build output**,
because that is what every CI run and every developer would pay.

### 5.3 With the feature OFF, nothing changes (AC1)

Verified two ways:

* `RUSTFLAGS="-D warnings" cargo check -p cqlite-flight --all-targets` (default features) is clean and
  compiles **no DataFusion crate** — the module, the harness binary
  (`required-features = ["datafusion-spike"]`) and every DataFusion/`async-trait` dependency are gated.
* The `--lite` gate (`file-size`, `fmt`, workspace-scoped `clippy`, `roborev-lints`, `scoped-tests`)
  PASSes with default features.

The only change to non-spike production code is one visibility widening:
`cqlite-flight/src/filter.rs`'s `lower_predicate_expr` becomes `pub(crate)` so the spike validates a
translated DataFusion filter through **production's** lowering instead of re-deriving operand
coercion. `filter.rs`'s inline tests were split into `filter_tests.rs` (campsite rule, epic
#1116/#1135) because that file was already over the 800-line source target; all 29 tests are
unchanged and still run.


---

## 6. Recommendation

### 6.1 For #941 (`docs/architecture/941-datafusion-decision-brief-2026-07.md`)

**Recommendation: #941 targets 0.17 with the spike banked. Do NOT promote it for 0.16.**

The brief's trigger rule is about Stage-1 throughput, and this spike cannot measure `rows/s/pod` (§0).
What it *can* do — and did — is answer the question that made the trigger interesting: **would a
DataFusion execution layer help?** On a wide, overlapping, LZ4-compressed, Cassandra-5.0-written
corpus, with I/O controlled, **DataFusion is within +/-3 % of the row engine over identical batches**,
and its default parallelism is not better than a single partition because the scan *is* a single
partition. The DataFusion arm has nothing to accelerate: 98 % of wall-time variance is page-in, and
the residual CPU is the merge/materialize/transpose pipeline, all of it upstream of execution.

So promoting #941 for 0.16 would land a heavy dependency (**+231 s of cold build time, +422 MiB of
build output, +37 lock packages**) against a measured **~0 %** execution gain. The de-risking that
option C in the brief asked for is done, and it de-risked *downward*.

Two conditions would change this recommendation, and neither is measurable here:

1. **A field round measuring Stage-1 short of ~30k rows/s/pod** — the brief's actual trigger. That is a
   cluster measurement; this box cannot produce it. Note the ceiling the sub-phases imply is
   **~56.4k rows/s of merge-consumer CPU** with I/O free, so on this shape Stage-1's ceiling is set by
   the producer, and moving it means moving the producer.
2. **A workload with real execution to do** — multi-way joins, large sorts/aggregations,
   many-partition parallel scans. Every scenario here is a scan plus a trivial count/projection, which
   is the shape where a vectorized engine has least to offer. The spike's `TableProvider` is the right
   thing to reuse when that workload exists; it is `~1.1k` lines behind one feature flag and costs
   nothing while switched off.

### 6.2 For the columnar-producer slot (M15, `throughput-program-2026-07.md` L534)

**Recommendation: Stage-3 prep. The `>1.3x` revisit trigger is NOT met.**

The trigger asks for `>1.3x` on wide/overlap. The measured decode-to-column cost is
**`stream_encode` = 2.99 us/row = 16.9 % of merge-consumer CPU**, so eliminating the transpose
*entirely* — a columnar producer that costs nothing, with the merge/materialize side unchanged — is a
**1.20x** ceiling. That is an upper bound derived from a stable, directly-instrumented counter, and it
is below 1.3x. The bigger term is `stream_merge` at **14.72 us/row (4.9x the transpose)**: the merge +
reconcile + per-row materialize, which a columnar *producer* does not remove — removing that means a
columnar *merge*, a much larger change than the slot describes.

### 6.3 The change worth making instead, already visible in the data

**Projection pushdown into `ScanSpec.projection` is worth more than either of the above and is already
implemented.** Pushing 3 of 12 columns cut `stream_encode` by **86 %**, `stream_merge` by **21 %**,
batches by **88 %** and peak RSS by **20 %**, and produced the matrix's only consistent
engine-attributable win (I/O-controlled residual **-4.6 s**). The connector should push projections
eagerly and unconditionally.

Conversely, **pushing a non-partition-key predicate is not worth it on this shape**: `filtered_scan`'s
pushdown arm still reads every byte and its residual is `+0.8 s`. Predicate pushdown only pays once it
can prune SSTables or partitions.

### 6.4 Disposition of the spike code

Keep it behind the flag; do not wire it. It is `datafusion-spike`-gated, has no service route, ticket
field or CLI flag reaching it, adds no decode logic, and would be deleted by deleting the feature. Its
durable value is the **measurement rig**: the sub-phase readback, the read-arm evidence, the arm
equivalence oracle, the I/O-controlled driver and aggregator. The one production change is
`filter::lower_predicate_expr` widening to `pub(crate)`.


---

## 7. Reproducing

```bash
# Generate the corpus (real Cassandra 5.0, wide + overlapping generations)
bash test-data/scripts/gen-df-spike-corpus-2605.sh

# Build the harness (non-default feature; the gate never compiles it)
cargo build --release -p cqlite-flight --features datafusion-spike --bin df_spike_bench

# Run the matrix. Use the DRIVER, not the harness's own loop — see §3.1(a) for why
# one process per cell and a rotated arm order are load-bearing, not cosmetic.
bash docs/reports/2605-datafusion-spike-artifacts/run-matrix.sh \
  /data/corpus-2605/sstables/perf_2605/wide_4kb-<uuid> \
  docs/reports/2605-datafusion-spike-artifacts/wide_4kb.cql \
  docs/reports/2605-datafusion-spike-artifacts \
  3

# Aggregate
python3 docs/reports/2605-datafusion-spike-artifacts/summarize.py \
  docs/reports/2605-datafusion-spike-artifacts/cells
```

A single cell, for debugging:

```bash
./target/release/df_spike_bench \
  --dir <table-dir> --ddl-file docs/reports/2605-datafusion-spike-artifacts/wide_4kb.cql \
  --projection pk,ck,v_int --filter-column ck --filter-op lt --filter-value 5 \
  --scenario filtered_scan --arm datafusion --df-target-partitions 1 --iterations 1
```

### Artifacts

| File | What |
|---|---|
| `cells/*.json` | one JSON document per (scenario, arm, iteration) run — 45 files, every field this report quotes |
| `summary.md` | the aggregated tables in §3, regenerated by `summarize.py` |
| `run-matrix.sh` | the driver (one process per cell, iteration outermost, arm order rotated) |
| `summarize.py` | the aggregator (median + range, the I/O-controlled regression, the comparability assert) |
| `wide_4kb.cql` | the corpus DDL (column/key structure verbatim from Cassandra's own `schema.cql`) |
| `corpus-manifest-2605.json` | the generator's provenance manifest, copied in so this report is self-contained (overlap measurement, per-SSTable timestamps, compression verification, the RF caveat) |

Regenerate the tables from the committed cells without re-running the bench:

```bash
python3 docs/reports/2605-datafusion-spike-artifacts/summarize.py \
  docs/reports/2605-datafusion-spike-artifacts/cells
```

The manifest is copied in as `corpus-manifest-2605.json` (produced by
`test-data/scripts/gen-df-spike-corpus-2605.sh` at `/data/corpus-2605/manifest-2605.json`, which is a
machine-local path and not in the repository).
