# Saturation instrumentation — process fd/thread/RSS + blocking-pool + egress-channel gauges

## Milestone
0.15. Workstream **WS2 of epic #2313** (flight read-throughput saturation program). Design-driven —
the saturation ramp (#2313 §C3/C4) can only attribute a throughput plateau to a resource when that
resource is *observed*, and today the OS-level resources that saturate first under concurrent readers
are all invisible in the server's own metric surface. This change is the instrumentation seam the
ramp depends on; it ships before WS8 (the ramp experiment) can attribute anything.

## Why (measured problem)
Source of truth: `docs/architecture/issue-throughput-saturation-research.md` §C0/§C1/§C6-WS2 and
issue #2419. The research ranks the expected order-of-failure as **thread/scheduler collapse →
no-admission-control queueing → fd exhaustion → memory**, and round-10/10b field observations (#2367)
saw per-pod core imbalance and RSS pressure that were only legible through out-of-band `kubectl top`.
The `cqlite-flight` server exposes a good RPC-level surface (`cqlite.rpc.*`, the #2361 `phase.active`
gauge, `query.rows_scanned`) and the #2420 admission gauges (`cqlite.flight.admission.*`), but it has
**no gauges for the OS-level resources that bind first**:

1. **Thread count** — a wide `do_get` merge over-subscribed the box (#2316 added a *per-merge*
   `cqlite.merge.producer_threads`, but there is no *process-wide* thread gauge to see the aggregate
   across N concurrent queries).
2. **fd count** — a fresh `File::open` per SSTable per scan (no reader pool by #815 design) → N×M fds
   vs a container ulimit ~1024 → `EMFILE`. No fd gauge exists.
3. **RSS** — the flight path bypasses the query engine's result-byte budget, so RSS ≈ N × per-scan
   peak; no process-memory gauge.
4. **Egress channel depth** — the producer→consumer bounded `sync_channel`
   (`STREAMING_CHANNEL_CAPACITY = 256`, `merge/mod.rs:515`) occupancy is unsampled, so "stuck in
   `do_get`" (in_flight>0, rows_scanned flat) can't be distinguished from disk-bound.
5. **Blocking-task depth** — streaming + merge run on `spawn_blocking` threads
   (`streaming.rs`); the number the flight path currently has outstanding is unsampled.

Without these gauges the ramp can measure that throughput plateaus but not *why*, defeating the
epic's purpose (prove the only ceiling is disk bandwidth, or file the resource that binds first).

## What changes
Add five saturation gauges, sampled by a lightweight background task, following the existing
`cqlite.*` catalog conventions (fixed-cardinality names, no per-request labels):

- `cqlite.proc.threads` — process thread count (`/proc/self/task` on Linux).
- `cqlite.proc.fds` — open fd count (`/proc/self/fd`).
- `cqlite.proc.rss_bytes` — resident set size (`/proc/self/statm` / `status`).
- `cqlite.merge.egress_channel_depth` — live occupancy of the bounded producer→consumer
  `sync_channel` (cap 256), tracked by an atomic on send/recv (the #2316 producer-thread-gauge
  pattern), in `cqlite.merge.*` alongside `producer_threads`.
- `cqlite.flight.blocking_tasks_in_use` — flight-managed `spawn_blocking` tasks currently
  outstanding (an honest, dependency-free proxy for blocking-pool pressure; the true global
  `tokio` pool queue depth needs `tokio_unstable` — an open fork, see design).

Each gauge is registered in the core catalog (`ALL_METRICS`) and the `otel::Instruments` struct with
a `record_gauge` arm (no per-call instrument rebuild — the #2412 lesson). Where a source is
unreadable on the running OS (a non-`/proc` platform), the gauge is **absent** (no sample), never a
fabricated `0` — the telemetry authoritative-data rule (#2314). The names/units feed #2426's
generated operator metrics reference.

## Non-goals
- NOT a new admission/backpressure mechanism (that is #2420, WS4) — this observes, it does not throttle.
- NOT the runtime de-amplification fix (WS3) or the ramp experiment itself (WS8).
- NOT the true global `tokio` blocking-pool queue depth (needs a build-wide `tokio_unstable` cfg —
  deferred as an open fork; we ship the honest flight-scoped in-use proxy instead of a fabricated number).
- NOT Windows support — `/proc`-derived gauges are Linux-only (the field target); other platforms
  report absence, not zero, and the server still starts and serves.
- NOT a change to any existing metric's meaning, cardinality, or emission cadence.
