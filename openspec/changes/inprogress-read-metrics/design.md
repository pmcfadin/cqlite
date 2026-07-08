# Design — In-progress read-path metrics (#2162)

## Recommended design (≈10 lines)
- **Cadence: per-record-batch increment (Flight) + per-bounded-row-threshold delta (core).**
  Flight already accumulates rows/bytes per batch in `MeteredDoGetStream::poll_next`; move the
  `add_counter(RPC_ROWS/RPC_BYTES, delta)` from `finalize` into the per-batch arm. Core's row-at-a-time
  scan loop accumulates into `context.scan_rows` and flushes a delta each time it crosses a threshold
  (a named `SCAN_PROGRESS_ROWS` const, aligned to the batch size, e.g. 8192), plus a final flush of the
  remainder. Counters stay monotonic and their totals are byte-identical to today's single emission.
- **Phase breakdown: bounded enum `resolve | merge_setup | stream`.** `do_get` records
  `cqlite.rpc.phase.duration` (new histogram, attrs `rpc.method` + new bounded `cqlite.rpc.phase`) and a
  `tracing` span event at each transition. Three phases, closed set — never per-query cardinality.
- **Overhead posture: per-batch/threshold, never per-row; relaxed atomics; zero-cost when off.** The
  accumulators are the existing plain/`Relaxed`-atomic counters; `obs::add_counter` is `#[inline]` and a
  genuine no-op that links no OTel when `observability` is off. Phase timing reads `Instant` at the ~3
  transitions only — no clock read in the per-row loop, no background thread, no new task.
- **What it beat:** a dedicated time-based periodic flush (a timer/interval that samples the counter
  every N seconds).
- **Progress signal is a moving counter, not a gauge** — composes under concurrent `do_get`s; the gauge
  is a documented deferred alternative.

## Alternative considered and rejected: time-based periodic flush
A background timer (or an `Instant`-check inside the scan loop) that emits the accumulated counter every
N seconds regardless of batch boundaries.

Rejected because:
1. **No added signal over per-batch + phase.** The failure it targets — "a query producing no batches
   for minutes" — is exactly the case a periodic *counter* flush cannot help: there are no new rows to
   report, so it emits the same flat value a per-batch counter already shows as flat. What *localizes*
   such a stall is the **phase** signal (time piling up in `merge_setup`), which this design provides
   directly and cheaply. The timer would duplicate the "counter is flat ⇒ stalled" reading without
   adding the "where" the phase marker already gives.
2. **Cost + complexity on the hot path.** Either a per-query background task/timer (a thread or spawned
   future per in-flight query — real overhead under the fan-out the harness runs) or a wall-clock read
   inside the per-row scan loop (a syscall-adjacent cost the no-per-row-work rule forbids).
3. **Nondeterministic to test.** Time-based emission makes the wiring test depend on wall-clock timing;
   per-batch/threshold emission is deterministic (K batches ⇒ K increments), so the scenarios below
   assert an exact, race-free property.

## Alternative considered and deferred: rows-examined-so-far gauge
The issue lists an optional in-flight progress *gauge*. Deferred, not adopted:
- A gauge is a **last-value** instrument. Under concurrent `do_get`s (the harness fan-out), a single
  `cqlite.rpc.rows_examined` gauge is written by every in-flight query and reads as an arbitrary
  interleaving — it cannot attribute progress to one query without per-query (unbounded) labels.
- A **moving monotonic counter** already answers the operative question ("is this making progress?"):
  rate > 0 ⇒ forward progress, rate == 0 while `in_flight > 0` ⇒ stall. It is bounded and concurrency-
  safe. If a per-query gauge is ever wanted it belongs with span-scoped attributes, which is out of
  this change's telemetry-only surface. Recorded here so the owner can green-light it later.

## Emit-site map (surveyed, `main`-relative — implementer re-greps)
| Signal | Emit-at-tail site today | New incremental site |
|---|---|---|
| `cqlite.rpc.rows` / `cqlite.rpc.bytes` | `obs.rs:145` (`RpcMetrics::finish`, via `finalize`) | per-batch arm of `MeteredDoGetStream::poll_next` (`streaming.rs:370`) |
| `cqlite.query.rows_scanned` | `execute.rs:343` (single `add_counter`) | threshold flush in the scan loop (`select_executor/mod.rs:702`, `stream_agg.rs:170`) |
| `cqlite.read.rows` / `cqlite.read.partitions` | end-of-read | same scan/merge loop, threshold delta |
| phase timing | *(does not exist)* | `do_get` / `do_get_setup` (`service.rs:391/468`) + `producer.rs` `drive_merge` boundary |

## Bounded phase enum
`resolve` (path discovery + token prune, `do_get_setup` → `resolve_paths_cancellable`),
`merge_setup` (`KWayMerger::new` — opens every input SSTable, the #2157 suspect), `stream`
(`drive_merge` stepping partitions + batches flowing to the client; scan and emit interleave once the
merger is built). Closed set; the attribute value is a `&'static str` from a fixed table so cardinality
is capped exactly like the existing `RPC_METHODS` slot table in `obs.rs`.

## Catalog additions (telemetry surface, not public API)
- `cqlite.rpc.phase.duration` — histogram `s`; attrs `cqlite.rpc.method`, `cqlite.rpc.phase`.
- `attr::RPC_PHASE = "cqlite.rpc.phase"` — bounded to the three phase strings above.
- `cqlite.rpc.rows` / `cqlite.rpc.bytes` / `cqlite.query.rows_scanned` / `cqlite.read.rows` /
  `cqlite.read.partitions` — **names unchanged**; only their emission cadence changes. Catalog doc
  comments are updated to state "emitted incrementally during a long-running scan."

## Doctrine
- **No-heuristics:** cadence and phase are structural (batch boundaries, code phases), never inferred
  from byte patterns or values. No value/key ever becomes an attribute.
- **Wiring evidence:** the primary proof is a long-running streaming `do_get` through the public Flight
  surface showing `cqlite.rpc.rows` move (and a `merge_setup` phase sample recorded) **before** the
  stream is drained — asserted via the `observability-testing` `MetricsCapture` / `capture_spans`
  harness and the existing `StreamProbe`. CLAUDE.md is updated in the same change if user-facing.
