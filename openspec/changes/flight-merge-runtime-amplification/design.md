# Design — bounding per-merge producer-thread cost

## Problem restated
`SSTableRowIteratorAdapter::open()` spawns one OS producer thread per input SSTable, and each
`producer_thread` builds a full multi-threaded `tokio::runtime::Runtime` (`num_cpus` workers). Over
M inputs the merge costs ~`M + M·num_cpus` threads. The producer's async work is a single sequential
scan (`stream_all_partitions_for_compaction(...).await` under one `block_on`) with no internal
`tokio::spawn`, so the worker pool is never exercised for concurrency — it is pure overhead.

## Constraints (from the issue)
1. **Merge output byte-parity unchanged** — compaction-byte-parity + sstabledump JSONL goldens are
   the oracle; the merge/reconciliation logic must not move.
2. **Cancellation discipline (#2264) preserved** — the `ScanCancel` token wired onto every per-run
   reader, the `MergeProducerError::Cancelled` channel signal, and the Flight `do_get` abort path
   must keep working identically.
3. **No wall-clock regression** on the merge benches (#1494 suite, once landed).
4. Applies uniformly to **both** callers — the Flight `do_get` read path and write-engine
   compaction/maintenance — since they share this merge.

## Candidates

### (a) Shared runtime `Handle` passed in — producers construct no runtime
Thread a `tokio::runtime::Handle` (the server's, already live under `#[tokio::main]`) down through
`open()` into `producer_thread`; the producer calls `handle.block_on(scan)` instead of building a
runtime.
- **Thread bound:** M producer threads + 0 new runtime workers (reuses the server pool). Meets O(M).
- **Byte-parity:** merge logic untouched → preserved.
- **Cancellation:** unchanged in principle.
- **Cost/risk:** the **compaction/maintenance callers have no ambient runtime** (they build one via
  `Runtime::new()` at `maintenance.rs:671`, `mod.rs:1866/2336` specifically to drive the merge), so
  the merge would need an `Option<Handle>` parameter and a fallback path when absent — new plumbing
  through every `KwayMerge::new*` constructor, and it couples the merge's lifetime to an
  externally-owned runtime. Driving a producer's blocking `SyncSender::send` via a *shared* server
  runtime handle also risks tying up that runtime if misused. More surface, more subtlety, for the
  same O(M) that (b) reaches with none of it.

### (b) `current_thread` runtime per producer — RECOMMENDED
Keep the exact per-producer ownership model, but replace
`tokio::runtime::Runtime::new()` with
`tokio::runtime::Builder::new_current_thread().enable_all().build()`. A `current_thread` runtime
drives futures **on the producer thread itself** and starts **zero** extra worker threads.
- **Thread bound:** M producer threads, each with a current_thread runtime that adds 0 workers →
  total extra ≈ **M**. Meets the "O(M) or better, no per-producer multi-core runtimes" target and
  eliminates the `M·num_cpus` term outright.
- **Byte-parity:** the change is one builder call inside `producer_thread`; the scan, emit callback,
  channel, heap, and reconciliation are byte-for-byte unchanged → parity trivially preserved.
- **Cancellation (#2264):** the `ScanCancel` wiring, the `MergeProducerError` signal, and the
  drop-join lifecycle are all untouched — a current_thread runtime honors the same cooperative
  `.await`-point cancellation the scan already polls.
- **Callers:** none change — works identically for Flight and compaction, because the producer still
  owns its runtime exactly as today.
- **Validity:** justified precisely because the scan is sequential (no internal `tokio::spawn`); the
  regression test + parity + benches confirm nothing relied on multiple workers.
- **Cost/risk:** minimal — a localized diff, no new parameters, no lifecycle coupling.

### (c) Producers as tasks on the server's existing runtime
Replace the OS producer threads with `tokio::spawn`ed tasks feeding an async channel.
- **Thread bound:** could reach O(1) (no per-producer OS threads at all).
- **Cost/risk:** the merge uses a **blocking `std::sync::mpsc::sync_channel`** for backpressure and a
  **blocking pull** consumer (`SSTableRowIterator::next` driving the k-way heap). Converting to tasks
  means an async channel, a redesigned backpressure model, and re-plumbing #2264 cancellation across
  a task boundary — the largest blast radius of the three, directly over the byte-parity-critical
  path, for no thread savings the issue actually requires. Rejected for 0.14 (see Non-goals).

## Recommendation: (b)
**(b) `current_thread` runtime per producer.** It achieves the required O(M) bound and kills the
`M·num_cpus` amplification with the smallest possible diff, no new plumbing, and no lifecycle
coupling — so byte-parity and #2264 cancellation are preserved essentially by construction, uniformly
for both the Flight read path and compaction.

**What it beats:**
- vs **(a)**: (a) reaches the same O(M) but forces an `Option<Handle>` parameter + a runtime-less
  fallback through every merge constructor and couples the merge to an externally-owned runtime — more
  surface and subtlety for zero additional thread savings.
- vs **(c)**: (c) could go below O(M) but only by reworking the blocking backpressure into an async
  channel and re-plumbing #2264 across a task boundary — a large, parity-endangering change the issue
  does not require. O(M) is the accepted bar; if field load later proves it insufficient, (c) is the
  documented follow-up.

## Observability (the gauge requirement, #2313 WS2 coordination)
Add a gauge to `cqlite-core/src/observability/catalog.rs` reflecting the count of **live merge
producer threads** (and, if cheap, the blocking-pool occupancy) — incremented when a producer is
spawned, decremented when it is joined/dropped. This makes the amplification (and its fix) visible on
a loaded node and gives the pinned regression test an always-on signal to corroborate the direct
OS-thread-count observation. The metric name is agreed with epic #2313 WS2 to avoid a naming
collision; proposed `cqlite.merge.producer_threads` (gauge, unit `{thread}`).

## Regression test (the pinned-bound requirement)
A test drives a **real multi-SSTable merge** over M present inputs and observes the process's peak OS
thread count (Linux `/proc/self/task` entry count — a direct, no-heuristics observation) across the
merge. It asserts the peak delta over a pre-merge baseline stays within an O(M) bound
(`baseline + M + small_constant`). On **today's code** the same observation exceeds the bound (each
producer's multi-threaded runtime adds `num_cpus` workers), so the test **FAILS on `main`** and passes
after (b). The assertion is meaningful only where `num_cpus >= 2` (where the amplification is
observable); the test guards on that so it is deterministic on the multi-core gate/CI box and never
flakes on a single-core host.
