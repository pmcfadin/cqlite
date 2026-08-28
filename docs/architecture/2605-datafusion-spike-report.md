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

<!-- RESULTS -->

---

## 4. Peak memory vs the B4 512Mi pod budget (M15 item 3)

<!-- MEMORY -->

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

<!-- RECOMMENDATION -->

---

## 7. Reproducing

```bash
# Generate the corpus (real Cassandra 5.0, wide + overlapping generations)
bash test-data/scripts/gen-df-spike-corpus-2605.sh

# Run the matrix
cargo run --release -p cqlite-flight --features datafusion-spike --bin df_spike_bench -- \
  --dir <corpus>/cassandra-data/data/perf_2605/wide_4kb-<uuid> \
  --ddl-file docs/reports/2605-datafusion-spike-artifacts/wide_4kb.cql \
  --projection pk,ck,v_int \
  --filter-column v_int --filter-op lt --filter-value <median> \
  --iterations 3 \
  --out docs/reports/2605-datafusion-spike-artifacts/results.json
```

Raw results: `docs/reports/2605-datafusion-spike-artifacts/`.
