# Flight merge-egress channel backpressure — characterization & lever

**Issue:** #2600 (oracle-driven exploration) · **Field signal:** #2367 round 12 ·
**Date:** 2026-07-19 · **Status:** characterization complete; follow-up filed.

## TL;DR

- **Attribution: (a) consumer-side drain latency**, amplified into an unbounded
  process-global buffer by concurrency. Under overload the SSTable-decode
  *producer* outruns the Arrow-encode + gRPC-write *consumer*; each admitted scan's
  bounded 256-slot `sync_channel` pins near-full, and the process-global gauge is
  the **sum** across all concurrent starved scans.
- **NOT (b) producer burstiness** — at 1 thread depth peaks at **3** (consumer keeps
  up); under load the depth holds a **sustained plateau** for the whole window, not a
  spike-and-drain.
- Throughput **saturates at 8 threads** (~190 qps full-scan) and does **not** improve
  through 80 threads; the extra 72 threads' work only queues in the egress channel
  (depth 1473 → 8080) and inflates latency (p50 36 → 417 ms). The server is
  **drain-bound**, not buffer-bound.
- **Chosen lever: a process-global egress memory budget that adaptively sizes the
  per-merge channel capacity** (concurrency-aware cap = `clamp(BUDGET / active_merges,
  min, 256)`). Measured basis: a temporary fixed cap of 32 (8× smaller) cut peak depth
  **8× at 8 threads (1473→184)** and **5× at 80 threads (8080→1619)** with **throughput
  and p99 flat** (qps 195→189 / 190→170; p99 99→101 / 814→915). Predicted for an
  adaptive budget B=2048: 80-thread peak depth **8080 → ≈2048 (≈4× relief), bounded
  regardless of concurrent-scan count**, at **< 10 % throughput cost**.

## Background: where the gauge comes from

`cqlite.merge.egress_channel_depth` (#2419, WS2) is a process-global live count of
merged DATA entries buffered in the k-way merge's bounded producer→consumer
`sync_channel` (`cqlite-core/.../write_engine/merge/mod.rs`,
`STREAMING_CHANNEL_CAPACITY = 256`). The producer thread decodes rows from `Data.db`
and `send()`s them (blocking at 256); the consumer
(`SSTableRowIteratorAdapter::next`) pulls them, the `StreamingMerger` batches them
into Arrow `RecordBatch`es (`--batch-size`, default 8192) and `sink.emit()`s each
batch to the Flight `do_get` gРПС stream (`cqlite-flight/src/producer_stream.rs`).

The cap of 256 bounds **one** merge. The gauge sums over **all** concurrent merges,
so its ceiling is `256 × (concurrent scans whose producer is outrunning its
consumer)` — there is **no process-global bound** on total buffered rows.

## Methodology

- Server: `cqlite-flight` release build, `--data-dir <datasets>/sstables`,
  `--max-concurrent-scans 128` (high, so admission is **not** the limiter — matching
  the field's admission = 12/64 observation), default `--batch-size 8192`.
- Client: `tools/flight-loadgen` (#2418), one ramp step per cell, `--step-duration 8s`.
- Data: `test_basic.simple_table` (1 000 rows, 1 `nb` SSTable, 19 wide columns,
  Snappy) and `test_timeseries.sensor_data` (2 000 rows, clustered). Ticket templates
  + `run-sweep.sh` under `test-data/scripts/egress-backpressure-2600/`.
- **Depth capture:** the depth gauge has no public in-process accessor (it is exported
  only via OTLP). To sample it at 15 ms resolution without an OTLP collector, a **small
  NON-COMMITTED instrumentation patch** was applied for the measurement run and
  **reverted before commit**:
  - `merge/channel_depth.rs`: `pub(super) fn depth_snapshot() -> i64 { DEPTH.load(Relaxed) }`
  - `merge/mod.rs`: `pub fn egress_channel_depth_snapshot()` re-export
  - `cqlite-flight/src/main.rs`: a thread appending `elapsed_ms,depth,peak` to
    `$CQLITE_EGRESS_SAMPLE_FILE` every 15 ms.
  Throughput/latency columns need **no** patch and reproduce against a stock build.
- Machine: Darwin 25.5.0, Apple Silicon. Runs were serialized (one sweep at a time,
  machine otherwise idle). Absolute qps is machine-specific; the **trends** (flat
  throughput, linear depth-vs-concurrency, cap-vs-depth proportionality) are the
  evidence and are machine-independent.

## Results

### Concurrency sweep (KLIMIT=128, 8 s/cell)

| cell            | threads | shape   | peak depth | qps    | p50 ms | p99 ms |
|-----------------|---------|---------|-----------:|-------:|-------:|-------:|
| simple_full_8   | 8       | full    |   **1473** | 195.0  | 36.2   | 98.7   |
| simple_full_32  | 32      | full    |   **5397** | 186.8  | 178.0  | 378.4  |
| simple_full_80  | 80      | full    |   **8080** | 190.4  | 416.5  | 814.1  |
| simple_lim_8    | 8       | limit-k |     1310   | 1091.1 | 6.0    | 24.0   |
| simple_lim_32   | 32      | limit-k |     5021   | 1002.9 | 21.9   | 217.9  |
| simple_lim_80   | 80      | limit-k |  **13452** | 1015.7 | 64.1   | 315.6  |
| sensor_full_80  | 80      | full    |  **15239** | 358.5  | 97.9   | 851.5  |

(1-thread baseline, not tabled: peak depth **3**, qps 38, p50 26 ms.)

### Diagnostic cells

| cell               | note                              | peak depth | qps   |
|--------------------|-----------------------------------|-----------:|------:|
| simple_full_80_K64 | admission K=64 (default)           |   **9014** | 188.5 |
| two_table_80       | simple×40 + sensor×40 concurrent   |   **9891** |  —    |

### CAP=32 experiment (temporary `STREAMING_CHANNEL_CAPACITY=32`, reverted)

| cell         | threads | peak depth | qps   | p50 ms | p99 ms |
|--------------|---------|-----------:|------:|-------:|-------:|
| cap32_full_8 | 8       |    **184** | 188.8 | 36.7   | 100.7  |
| cap32_full_80| 80      |   **1619** | 170.4 | 468.0  | 915.5  |

vs the CAP=256 baseline (simple_full_8 / simple_full_80): depth **1473→184 (8×)** and
**8080→1619 (5×)**; qps **195→189** and **190→170**; p99 **99→101** and **814→915**.

## Attribution — (a) consumer-side drain latency

Every claim below traces to a measured cell (no-heuristics applies to perf attribution):

1. **A single consumer is slower than a single producer under CPU contention.** At
   1 thread depth = 3 (consumer keeps up). At 8 threads per-merge depth is
   `1473 / 8 ≈ 184` of 256 — each channel is near-pinned. The only variable between
   these is CPU contention among the decode threads and the Arrow-encode/gRPC-write
   consumers, so the consumer is falling behind = **(a)**.
2. **Throughput is drain-bound, not concurrency-bound.** Full-scan qps is flat
   (195 → 187 → 190) from 8 → 80 threads while p50 grows 36 → 178 → 417 ms. Adding
   producers past the drain ceiling adds **zero** throughput — pure queueing.
3. **Depth is a sustained plateau, not a burst.** The 80-thread depth timeseries
   (`data/simple_full_80.depth.downsampled.csv`) holds ~2 700–5 400 for the entire
   8 s load window and only decays after load stops — the opposite of the transient
   spike-and-drain that would indicate **(b) producer burstiness**.
4. **The aggregate scales with concurrent-merge count, not table.** depth grows
   linearly with threads (1473 → 5397 → 8080) and the 2-table cell (9891) sits between
   the single-table 80-thread points — consistent with `depth ≈ per_merge_cap ×
   active_merges`, i.e. the 256-cap is correct per-merge but the **sum is unbounded**
   in concurrency = the **(c)-flavoured** consequence of the **(a)** root cause.
5. **Admission-K does not relieve it.** At K=64, 80 threads still buffer 9014 (64
   concurrent starved merges ≈ 80) with **zero** shedding (each waits < the 30 s
   budget). Bounding memory via K alone would require K ≈ the drain ceiling (~8 for
   full-scan here), which trades the memory alarm for heavy latency/shedding.

## Lever — process-global adaptive egress budget (chosen; exactly one)

Replace the fixed per-merge `STREAMING_CHANNEL_CAPACITY = 256` with a
**concurrency-aware capacity** derived from a single process-global row budget:

```
cap_per_merge = clamp(EGRESS_ROW_BUDGET / active_merge_count, MIN_CAP, 256)
```

`active_merge_count` is already trackable (the #2316 producer-thread gauge / the same
place `channel_depth` is wired). This applies producer backpressure sooner as
concurrency rises so **total** buffered rows track a fixed budget instead of
`256 × N`.

**Why this and not the alternatives:**

- *consumer-side batch drain / drain-path syscall reduction* — aim at throughput, but
  I have no profile isolating channel-sync cost from Arrow-encode/gRPC cost, so any
  predicted throughput win would be a guess (batch is already 8192; the per-row
  channel sync is not shown to be the drain bottleneck). Rejected under no-guessing.
- *fixed smaller cap* — the CAP=32 experiment shows it works but stays **linear in
  concurrency** (80-thread depth still 1619); it under-buffers at low concurrency and
  still grows unbounded at high concurrency. The adaptive form removes both.
- *admission-K sizing* — measured to not relieve the buffer without heavy shedding
  (K=64 cell).

**Predicted effect (derived from the CAP=32 measurement, not guessed):**

- Peak depth at 80-thread full-scan: **8080 → ≈ EGRESS_ROW_BUDGET** (e.g. 2048 ⇒ ≈4×
  relief), and — unlike any fixed cap — **bounded regardless of concurrent-scan
  count** (the field's 3 505 → capped at the budget).
- Throughput: **within ±10 %** of baseline (CAP 256→32, an 8× cut, moved full-scan qps
  195→189 at 8 threads and 190→170 at 80 threads; the consumer, not the buffer, sets
  throughput).
- p99: unchanged (queueing latency is set by the shared drain rate; buffer size only
  sets how many rows sit in RAM — 99→101 / 814→915 in the experiment).
- Memory: bounds the merge-egress working set to `EGRESS_ROW_BUDGET` rows total
  instead of `256 × active_merges` (the field OOM-adjacent concern the 3 505 signal
  raised).

## Follow-up

Implementation issue filed (see below) with the measured baseline, the predicted win,
the affected modules (`cqlite-core/.../write_engine/merge/mod.rs` +
`channel_depth.rs`), oracle-vs-design routing, and acceptance criteria.

## Reproduce

```bash
cargo build --release -p cqlite-flight -p flight-loadgen
DATASETS_ROOT=$HOME/local_projects/cqlite/test-data/datasets \
  test-data/scripts/egress-backpressure-2600/run-sweep.sh
```

Throughput/latency reproduce on a stock build; the depth column requires the
NON-COMMITTED instrumentation patch documented in **Methodology**. Raw data:
`test-data/scripts/egress-backpressure-2600/data/`.
