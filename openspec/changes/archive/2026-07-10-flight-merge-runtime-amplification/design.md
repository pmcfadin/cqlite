# Design — bounding per-merge producer-thread cost

## Problem restated
`SSTableRowIteratorAdapter::open()` spawns one OS producer thread per input SSTable, and each
`producer_thread` builds a full multi-threaded `tokio::runtime::Runtime` (`num_cpus` workers). Over
M inputs the merge costs ~`M + M·num_cpus` threads. The producer drives
`stream_all_partitions_for_compaction(...).await` under one `block_on` on that runtime.

`stream_all_partitions_for_compaction`'s OWN implementation
(`sstable/reader/data_access/compaction.rs`) is a self-contained sequential window-drain loop with
no internal `tokio::spawn` — it reads raw chunks and drains its own `WindowCursor` inline, never
delegating to the reader crate's OTHER streaming machinery. **But that other machinery genuinely
IS concurrent**: `run_scan_stream_windowed` (`scan_stream_windowed.rs:543`), which backs the public
`scan_stream`/`scan_stream_batched` SELECT-scan surface for chunk-stitching formats, spawns a
`tokio::spawn` forwarder task PLUS a `spawn_blocking` decompress/parse task that run concurrently
with its own async I/O feed loop. Any premise here needs to hold for a `current_thread` runtime
under BOTH shapes — the producer's actual today's self-contained loop, and the shared windowed-scan
pattern it does not currently call but sits alongside in the same crate — so the recommendation
below is justified against the real (not overstated) concurrency model. See "Why `current_thread`
stays sound under real concurrency" below the candidate table.

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
- **Validity:** the producer's actual scan loop is self-contained and single-.await (no internal
  `tokio::spawn`), so it plainly never needed extra workers. Even under the reader crate's OTHER,
  genuinely concurrent windowed-scan pattern (forwarder task + `spawn_blocking`, not currently called
  by this producer but present in the same crate), a `current_thread` runtime remains sound for the
  reasons in "Why `current_thread` stays sound under real concurrency" below — so the choice does not
  silently depend on the producer never adopting that pattern. The regression test + parity + benches
  confirm nothing relied on multiple runtime workers.
- **Cost/risk:** minimal — a localized diff, no new parameters, no lifecycle coupling.

### (c) Producers as tasks on the server's existing runtime
Replace the OS producer threads with `tokio::spawn`ed tasks feeding an async channel.
- **Thread bound:** could reach O(1) (no per-producer OS threads at all).
- **Cost/risk:** the merge uses a **blocking `std::sync::mpsc::sync_channel`** for backpressure and a
  **blocking pull** consumer (`SSTableRowIterator::next` driving the k-way heap). Converting to tasks
  means an async channel, a redesigned backpressure model, and re-plumbing #2264 cancellation across
  a task boundary — the largest blast radius of the three, directly over the byte-parity-critical
  path, for no thread savings the issue actually requires. Rejected for 0.14 (see Non-goals).

## Why `current_thread` stays sound under real concurrency

The producer's actual scan (`stream_all_partitions_for_compaction`) is a self-contained sequential
window-drain loop with no internal `tokio::spawn` — verified by tracing its full call graph
(`compaction.rs`'s own chunk-read + `drain_compaction_window` loop, and its `iterate_all_partitions`
→ `sequential_scan` fallback, neither of which calls the reader crate's shared
`run_scan_stream_windowed`). But the reader crate's OTHER streaming machinery is genuinely
concurrent: `run_scan_stream_windowed` (`scan_stream_windowed.rs:543`), which backs the public
`scan_stream`/`scan_stream_batched` surface for chunk-stitching formats, spawns a `tokio::spawn`
forwarder task that drains a batched-row channel PLUS a `spawn_blocking` task that owns the
decompress+parse CPU work — both running concurrently with the caller's own async I/O feed loop. A
`current_thread` runtime remains sound hosting that pattern, for three independent reasons:

1. **The forwarder is cooperatively scheduled, not concurrently executed.** A `current_thread`
   runtime multiplexes every task it owns (the driving future plus any `tokio::spawn`ed task like the
   forwarder) on its single OS thread, polling whichever task is ready at each other task's `.await`
   yield point. Neither the feed loop nor the forwarder busy-loops without yielding, so the runtime
   interleaves them exactly as a multi-threaded runtime would interleave concurrent polls — just
   serially instead of in parallel. No deadlock: progress on one never *requires* the other to run on
   a different thread.
2. **`spawn_blocking` work runs on Tokio's separate blocking-thread pool, independent of runtime
   flavor.** `tokio::task::spawn_blocking` always hands its closure to Tokio's dedicated blocking
   pool (on-demand OS threads), never the runtime's own worker set — a `current_thread` runtime has
   zero async workers but the blocking pool exists regardless. So the parse task is never starved by
   the runtime being single-threaded; it makes independent progress on its own OS thread.
3. **The producer's own blocking channel send is released by an EXTERNAL thread, not a co-scheduled
   task.** `producer_thread`'s emit callback calls the k-way merge's bounded `SyncSender::send`
   synchronously (a genuine blocking call inside the polled future). If it blocks (channel full), the
   `current_thread` runtime's one OS thread stalls — but what unblocks it is the merge's OWN consumer
   thread (a completely different OS thread `KWayMerger::step` runs on) draining the receiving end.
   Progress does not depend on any task queued on THIS producer's own runtime, so a synchronous block
   inside the polled future cannot self-deadlock the runtime the way it would if the unblocking event
   were another task on the same single thread.

Additionally, `producer_thread` forces `config.storage.use_mmap = false` and
`config.storage.disk_access_mode = DiskAccessMode::Buffered` (issue #591 safety), so even if a future
refactor routed the producer through `run_scan_stream_windowed`, its synchronously-faulting-backend
branch (`feed_raw_chunks_blocking`, a THIRD `spawn_blocking` task used only for mmap/`O_DIRECT`
backends) would never be taken — the producer's backend is never the faulting kind.

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
