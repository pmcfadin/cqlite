# Design: in-`stream` data-plane sub-phase timers (issue #2819 / M1)

## Context

`cqlite-flight/src/obs.rs` already carries the per-RPC phase instrument (epic AI #1686, #1701/#1705/
#1707): a `PhaseTimer` single-cursor state machine over a closed, ordered five-value set
(`RPC_PHASES`, obs.rs:226) — `validate → admission → resolve → merge_setup → stream` — each
`transition()` closing the open phase with one `cqlite.rpc.phase.duration` histogram sample tagged
`cqlite.rpc.phase = <value>` plus a `cqlite.rpc.phase.active` up/down gauge. The `merge_setup →
stream` boundary is the `on_merger_built` hook fired right after the merger is built
(`producer.rs:776`, `streaming.rs:344-346`); from there the entire scan loop
(`producer_stream.rs::drive_merge_streaming`) runs inside `PHASE_STREAM`.

Everything the data plane does is inside that one phase — but it is a CONCURRENT PIPELINE, not a
serial loop (this is the correction over the change's first-draft design). The threads are:

1. **Merge consumer thread** — the flight `spawn_blocking` task (`streaming.rs:323`) runs
   `drive_merge_streaming`: per reconciled batch it calls `stepper.step_row()` (k-way merge +
   LWW/tombstone/TTL reconcile + `entry_to_row` materialize, pulling already-decoded rows from the
   per-input channels) → `flush()` (Arrow `RecordBatch` encode) → `sink.emit()`.
2. **Per-SSTable producer thread(s)** — `KWayMerger::new_from_readers` → `open_from_reader`
   (`merge/from_readers.rs:201`, `merge/mod.rs:672`) spawns one `std::thread` per input SSTable,
   each driving `stream_all_partitions_for_query` over a bounded `sync_channel` into the merge
   consumer (O(M) thread-per-input backpressure, issues #827/#2316/#2346).
3. **Feed thread(s)** — inside each producer thread's windowed scan, a `spawn_blocking` feed task
   (`scan_stream_windowed.rs:489`) performs the cold body-chunk **page-in**
   (`read_compressed_chunk_sync`, `scan_stream_windowed_read.rs`) and **LZ4 decompress**
   (`decode_scan_chunk`, `scan_stream_windowed_decode.rs:107`), shipping decoded chunks to the parse
   half over another bounded channel.

So page-in + decompress run on the feed thread(s), CONCURRENTLY with merge/encode on the merge
consumer thread and the channel send on the egress side. A dashboard reading `stream` cannot tell a
slow cold disk from a slow client, because all of it collapses into one wall-clock phase.

## Recommended design

### Sub-phase decomposition (5, meeting the ≥4 acceptance bar)

Within `stream`, accumulate wall time into five buckets, tagged with new bounded **values** of the
**existing** `cqlite.rpc.phase` attribute and emitted on the **existing** `cqlite.rpc.phase.duration`
histogram:

| Sub-phase value | What it wraps | Where |
|---|---|---|
| `stream_cold_fault` | the synchronous SSTable **body-chunk page-in** (mmap fault / disk read) — cold-IO *latency*, the throughput-program bucket-3 signal | core SSTable read path (chunk fetch) |
| `stream_decompress` | LZ4 chunk decompression of the read body chunk (when `CompressionInfo.db` present) | core chunk-decode path |
| `stream_merge` | k-way merge + LWW/tombstone/TTL reconcile + per-row materialize (`step_row` reconcile → `entry_to_row`) | merger + `producer.rs` |
| `stream_encode` | Arrow `RecordBatch` conversion (`flush` → `arrow_convert`) | `producer_stream.rs` flush |
| `stream_grpc_write` | `sink.emit` channel `reserve()`/send **including the backpressure park/wake** | `streaming.rs::ChannelSink::emit` |

Because these stages run on CONCURRENT pipeline threads (see Context), the sub-phases OVERLAP in
wall-clock and DO NOT sum to the `stream` phase's duration — their sum may exceed it. `stream` keeps
its exact meaning as the whole data-plane wall-clock total; the sub-phases attribute WHERE that time
is spent across concurrent stages, and each recorded sub-phase is a positive share bounded by the RPC
wall time (Non-goal: `stream` semantics unchanged).

### Cold-fault isolation from send park/wake (the crux acceptance criterion)

`stream_cold_fault` is measured ONLY around the reader's body-chunk page-in, on the feed thread; the
send-side backpressure park is captured in the **disjoint** `stream_grpc_write` scope
(`ChannelSink::emit`'s `reserve()`/send) on the merge/egress thread. The two scopes share no code
interval and run on distinct threads, so a client that stops draining the channel inflates
`stream_grpc_write` but can never inflate `stream_cold_fault` — an elevated `stream_cold_fault` is
provably cold-IO. This is exactly what the program's #1 measurement needs: the **cold−warm delta on
`stream_cold_fault`** *is* the disputed cold-IO-latency bucket (throughput-program §5 #1, field-gap
§2), readable off the standing dashboard. The signal is the delta, NOT a sum.

### Recording mechanism: a per-request Arc-atomic accumulator, emitted once per RPC

Because the sub-phases accrue on SEPARATE concurrent threads, a thread-local accumulator on the merge
thread could never see the feed thread's page-in/decompress time. The mechanism is instead a
**per-request `Arc<StreamSubPhaseTimings>` holding five per-sub-phase `AtomicU64` nanos counters**.
Lightweight RAII `SubPhaseScope` guards, on whichever thread the work runs, add their elapsed nanos
into the right atomic (`fetch_add`, `Relaxed`). The `Arc` is created by the flight closure and
propagated to the three scan-thread spawn sites so each thread adds into the SAME per-request
accumulator:

- **feed thread** (`scan_stream_windowed.rs:489`) → `stream_cold_fault` (page-in) + `stream_decompress`,
- **merge consumer thread** (`streaming.rs:323`) → `stream_merge` (`step_row`) + `stream_encode` (`flush`),
- **egress** (`ChannelSink::emit`) → `stream_grpc_write`.

The core-side seam (cold-fault + decompress) is a lock-free thread-local `Option<Arc<...>>` sink in
`cqlite-core::observability`: the flight closure installs the request's `Arc` and it is captured +
re-installed at each scan-thread spawn (via a `current()`/`install()` RAII pair, mirroring the
`BlockingTaskGuard` pattern), so the feed thread's chunk-fetch and decompressor push into it and it is
a no-op (unset) for every non-flight core caller. At stream teardown (the existing `PhaseTimer`'s
`stream` close / Drop) the flight side reads each atomic and emits **exactly one**
`cqlite.rpc.phase.duration` sample per sub-phase that accumulated any time (never a fabricated zero
for a sub-phase never entered — matching `PhaseTimer`'s "a phase never entered records none"
invariant). Sample count is ≤5 per RPC, not per-row/per-chunk; label cardinality is the closed value
set.

## Alternatives considered (and why the recommendation beat them)

1. **A single-thread wall-time `Duration`-bucket accumulator threaded by a per-row loop cursor** (the
   change's first-draft design). *Rejected:* it assumed the data plane was a serial
   `fault→decompress→merge→encode→write` loop on one `spawn_blocking` thread and that the five buckets
   summed to `stream`. The read path is actually a concurrent pipeline (per-SSTable producer threads +
   feed `spawn_blocking` threads, see Context), so page-in/decompress run on threads a thread-local on
   the merge thread never reaches, and the buckets overlap in wall-clock rather than summing. The
   Arc-atomic accumulator propagated to the real scan-thread spawn sites is what actually captures the
   feed-thread cost; the "sum ≈ stream" invariant is dropped in favour of the cold−warm delta signal.
2. **Flat extended phase enum driven by per-chunk/per-row `PhaseTimer::transition()`** — reuse the
   single-cursor timer, adding the sub-phase values to `RPC_PHASES` and calling `transition()` around
   each region. *Rejected:* the stages recur per chunk/row across threads, so this emits one histogram
   **sample per chunk per sub-phase** (sample-count blowup on a large scan), makes
   `cqlite.rpc.phase.active` flap meaninglessly, and a single ordered cursor cannot model concurrent
   cross-thread stages. The Arc-atomic accumulator keeps ≤5 samples/RPC with the same label cardinality.
3. **A distinct new sub-phase metric** (`cqlite.rpc.stream.subphase.duration`) + its own active
   gauge. *Rejected:* a new metric name + catalog entry + a parallel gauge is more surface and closer
   in spirit to "a new stack" (Non-goal); a dashboard would have to join two metrics. The existing
   histogram already carries the `cqlite.rpc.phase` attribute — new bounded values reuse it for free
   and any existing `phase.duration` panel picks the sub-phases up automatically.
4. **Per-sub-phase monotonic counters** (accumulated nanos, no histogram). *Rejected:* loses the
   distributional shape (per-sub-phase p99 tail) — and the cold-IO-*latency* investigation is
   explicitly about the tail, not the mean. The histogram keeps percentiles.

## Wiring evidence pattern (for the spec scenarios)

The existing e2e proof is `cqlite-flight/tests/metrics_capture_test.rs::
do_get_emits_bounded_phase_and_incremental_metrics`: it runs a full `do_get` over a real multi-row
fixture, drains the whole stream, captures emitted metrics, `find(RPC_PHASE_DURATION)`, and asserts a
sample count ≥1 per bounded phase value plus "every phase value is in the closed set / no unbounded
value leaks." The sub-phase scenarios require the equivalent: over a real drained `do_get`, the
sub-phase-tagged samples exist (≥4 distinct), each is a positive share ≤ the RPC wall time,
cold-fault is a distinct sample from grpc-write, a slow-client run inflates `stream_grpc_write` but
not `stream_cold_fault`, the cold−warm delta on `stream_cold_fault` is observable, an uncompressed
fixture records no `stream_decompress` sample, the sample count is bounded per-RPC (independent of
row/batch count), and every emitted `cqlite.rpc.phase` value stays in the (now-extended) closed set.

## Owner decisions (resolved at Seam-1)

1. **Sub-phase count: 5** (cold-fault, decompress, merge, encode, gRPC-write). The decompress split is
   worth the core seam — it directly settles the P1.3↔P1.5 contradiction (decompress-CPU vs cold-IO
   latency).
2. **Cardinality: sub-phase values are gated to the `do_get` method and `cqlite.rpc.phase.duration`
   only** — they are NOT added to `cqlite.rpc.phase.active`. Other methods see only the 5 top-level
   phases; `phase.active` stays the 5-value set × methods.
3. **`stream_grpc_write` is a first-class sub-phase**, carrying a catalog annotation flag noting it is
   client-paced (not server cost), so a "server data-plane" panel reader is not misled.
4. **The thread-local core seam is IN SCOPE** for #2819 (both ≥4 sub-phases and cold-fault isolation
   need it), realised as the per-request Arc-atomic sink propagated to the scan-thread spawn sites
   (see "Recording mechanism").
5. **Dashboard: this issue ships the metric emission + operator-doc catalog annotation only.** The
   Grafana panel JSON is ops-owned / delivered separately, out of #2819's code scope.

**Accounting-model amendment (post-activation, owner-approved):** the first-draft "the five sub-phases
sum within slack to the `stream` phase duration" invariant is DROPPED. The read path is a concurrent
pipeline (per-SSTable producer threads + feed `spawn_blocking` threads), so the sub-phases overlap in
wall-clock and their sum can exceed `stream`. `stream` keeps its meaning as the whole data-plane
wall-clock total; the load-bearing cold-IO signal is the **cold−warm delta on `stream_cold_fault`**,
not a sum. This is why the recording mechanism is a per-request Arc-atomic accumulator propagated to
the three scan-thread spawn sites, not a single-thread `Duration`-bucket loop cursor.
