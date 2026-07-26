# Design: in-`stream` data-plane sub-phase timers (issue #2819 / M1)

## Context

`cqlite-flight/src/obs.rs` already carries the per-RPC phase instrument (epic AI #1686, #1701/#1705/
#1707): a `PhaseTimer` single-cursor state machine over a closed, ordered five-value set
(`RPC_PHASES`, obs.rs:226) — `validate → admission → resolve → merge_setup → stream` — each
`transition()` closing the open phase with one `cqlite.rpc.phase.duration` histogram sample tagged
`cqlite.rpc.phase = <value>` plus a `cqlite.rpc.phase.active` up/down gauge. The `merge_setup →
stream` boundary is the `on_merger_built` hook fired right after `KWayMerger::new`
(`producer.rs:776`, `streaming.rs:344-346`); from there the entire scan loop
(`producer_stream.rs::drive_merge_streaming`) runs inside `PHASE_STREAM`.

Everything the data plane does is inside that one phase. Per reconciled batch the loop:
`stepper.step_row()` (→ core reader: cold body-chunk page-in + LZ4 decompress + k-way merge +
reconcile + row materialize) → `flush()` (→ Arrow `RecordBatch` encode) → `sink.emit()` (→ bounded
channel `reserve()`/send, which **parks** under backpressure from a slow client, `streaming.rs:149-
183`). A dashboard reading `stream` cannot tell a slow cold disk from a slow client.

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

The five sub-phases sum (within measurement slack) to the `stream` phase's own duration, so
`stream` retains its meaning as the total and the sub-phases explain its composition (Non-goal:
`stream` semantics unchanged).

### Cold-fault isolation from send park/wake (the crux acceptance criterion)

`stream_cold_fault` wraps ONLY the reader's body-chunk page-in; the send-side backpressure park is
captured in the **disjoint** `stream_grpc_write` scope (`ChannelSink::emit`'s `reserve()`/send). The
two scopes never overlap in wall time — cold-fault is entered/exited entirely inside `step_row`
before any batch reaches `sink.emit`. So an elevated `stream_cold_fault` is provably cold-IO, never
inflated by a client that stopped draining the channel. This is exactly what the program's #1
measurement needs: the **cold−warm delta on `stream_cold_fault`** *is* the disputed cold-IO-latency
bucket (throughput-program §5 #1, field-gap §2), readable off the standing dashboard.

### Recording mechanism: a bounded wall-time accumulator (NOT per-row transitions)

The sub-phases are **interleaved per row/batch** in a tight loop (fault → decompress → merge → encode
→ write → fault …), not a once-through ordered sequence like the top-level phases. So a small
`StreamSubPhaseTimings` struct holds five `Duration` buckets; lightweight RAII `SubPhaseScope`
guards add elapsed nanos to the right bucket as the loop runs. At stream teardown (the existing
`PhaseTimer`'s `stream` close / Drop) it emits **exactly one** `cqlite.rpc.phase.duration` sample per
sub-phase that accumulated any time (never a fabricated zero for a sub-phase never entered — matching
`PhaseTimer`'s "a phase never entered records none" invariant). Sample count is ≤5 per RPC, not
per-row; label cardinality is the closed value set.

### Threading core-side timings (cold-fault + decompress live in `cqlite-core`)

Cold-fault and decompress happen inside the core SSTable reader, not the flight loop. Because the
whole merge runs on **one** `spawn_blocking` thread (`streaming.rs:323`), the recommended seam is a
**thread-local sub-phase accumulator**: the flight closure installs it around the merge (set on
entry, drained + cleared on exit — matching the existing `BlockingTaskGuard` RAII pattern), the core
reader's chunk-fetch and the decompressor push their elapsed time into it, and the flight loop wraps
`flush`/`sink.emit` directly. This is lock-free (single thread), adds no parameter to the hot
`step_row`/reader signatures, and is a no-op when unset (so non-flight core callers pay nothing). See
Open Question 4 on whether the core seam is in-scope for this issue.

## Alternatives considered (and why the recommendation beat them)

1. **Flat extended phase enum driven by per-row `PhaseTimer::transition()`** — reuse the existing
   single-cursor timer, adding the sub-phase values to `RPC_PHASES` and calling `transition()` around
   each region. *Rejected:* the sub-phases interleave per row, so this emits one histogram **sample
   per row per sub-phase** (sample-count blowup on a million-row scan) and makes `cqlite.rpc.phase.
   active` flap meaninglessly; the single ordered cursor cannot model a fault→decompress→merge cycle
   repeated per row. The accumulator keeps ≤5 samples/RPC with the same label cardinality.
2. **A distinct new sub-phase metric** (`cqlite.rpc.stream.subphase.duration`) + its own active
   gauge. *Rejected:* a new metric name + catalog entry + a parallel gauge is more surface and closer
   in spirit to "a new stack" (Non-goal); a dashboard would have to join two metrics. The existing
   histogram already carries the `cqlite.rpc.phase` attribute — new bounded values reuse it for free
   and any existing `phase.duration` panel picks the sub-phases up automatically.
3. **Per-sub-phase monotonic counters** (accumulated nanos, no histogram). *Rejected:* loses the
   distributional shape (per-sub-phase p99 tail) — and the cold-IO-*latency* investigation is
   explicitly about the tail, not the mean. The histogram keeps percentiles.

## Wiring evidence pattern (for the spec scenarios)

The existing e2e proof is `cqlite-flight/tests/metrics_capture_test.rs::
do_get_emits_bounded_phase_and_incremental_metrics`: it runs a full `do_get` over a real multi-row
fixture, drains the whole stream, captures emitted metrics, `find(RPC_PHASE_DURATION)`, and asserts a
sample count ≥1 per bounded phase value plus "every phase value is in the closed set / no unbounded
value leaks." The sub-phase scenarios below require the equivalent: over a real drained `do_get`, the
sub-phase-tagged samples exist, cold-fault is a distinct sample from grpc-write, and every emitted
`cqlite.rpc.phase` value stays in the (now-extended) closed set.

## Open questions (OWNER DECISION — not decided here)

1. **Sub-phase count: 5 vs fold decompress into cold-fault (4).** The ≥4 bar is met either way.
   Keeping `stream_decompress` separate from `stream_cold_fault` is the split that *directly* settles
   the P1.3↔P1.5 contradiction (decompress-CPU ~1-2% vs cold-IO latency) — recommended — but it
   requires the decompress timing seam in the core reader. Fold-to-4 avoids that seam. **Recommend 5;
   owner confirms the decompress split is worth the core seam.**
2. **Cardinality budget / scope of the new phase values.** Adding 5 values grows the shared
   `cqlite.rpc.phase` set from 5 → 10 and `cqlite.rpc.phase.active` combinations from methods×5 →
   methods×10. Sub-phases only ever occur on `do_get`. **Recommend gating the sub-phase values to the
   `do_get` method** (other methods still see only the 5 top-level phases). Owner confirms the
   cardinality budget and whether `phase.active` should carry sub-phases at all (vs `phase.duration`
   only).
3. **Is `stream_grpc_write` (send-park) a first-class sub-phase or the residual remainder?** Isolating
   cold-fault only *requires* send-park be separable. Making it its own sub-phase is the cleanest
   (cold-fault provably disjoint) but send-park is client-speed, not server cost, and could mislead a
   reader of a "server data-plane" panel. **Recommend a first-class `stream_grpc_write`** with an
   annotation flagging it as client-paced. Owner confirms.
4. **Core-instrumentation depth (crate seam).** Cold-fault + decompress need a lightweight (thread-
   local) observability seam in the `cqlite-core` read path. Is adding that seam in-scope for #2819,
   or should this issue ship only the flight-loop-visible boundaries (`step_row` combined /
   `stream_encode` / `stream_grpc_write` = 3 sub-phases) and defer the cold-fault/decompress split to
   a follow-up? **This gates whether ≥4 AND cold-fault isolation are met in one issue.** **Recommend
   the thread-local core seam in this issue** (both acceptance criteria need it). Owner confirms scope.
5. **Dashboard scope.** AC#1 says "a field dashboard attributes in-`stream` cost." **Recommend this
   issue ships the metric emission + the operator-doc catalog annotation that makes the panel
   expressible**, and the actual Grafana panel JSON is ops-owned / delivered separately. Owner
   confirms the dashboard artifact is out of this issue's code scope.
