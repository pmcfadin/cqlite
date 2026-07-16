# Design — Saturation instrumentation (WS2, epic #2313 / issue #2419)

Five saturation gauges that make the research's Rank 1–4 order-of-failure legible inside the flight
server's own metric surface. Two axes drove every decision: (a) the no-fabrication rule — an
unreadable resource reports *absence*, never `0`; and (b) the #2412 lesson — every catalog name needs
an `otel::Instruments` arm or it silently rebuilds its instrument per call.

## Context / anchors (main-relative; re-grep before editing)
- `cqlite-flight/src/obs.rs` — the existing RPC metric surface (`RpcMetrics`, `PhaseTimer`); **870
  lines, already over the ~800 campsite target**, so the sampler + `/proc` readers go in a NEW file
  `cqlite-flight/src/saturation.rs`, not into `obs.rs`.
- `cqlite-core/src/observability/catalog.rs` — metric-name constants + `ALL_METRICS`; `unit` module
  (has `THREADS = "{thread}"`, `BYTES = "By"`).
- `cqlite-core/src/observability/otel.rs` — `struct Instruments { … }` built once in `instruments()`;
  `record_gauge(name, value, attrs)` matches the catalog name to a pre-built `Gauge<i64>` field, else
  falls back to a per-call-rebuilt ad-hoc gauge (the arm to avoid).
- `cqlite-core/src/observability/mod.rs` — the always-compiled `record_gauge` facade (no-op when the
  `observability` feature is off); call sites are identical in every build.
- `cqlite-core/src/storage/write_engine/merge/mod.rs:515,557` — `STREAMING_CHANNEL_CAPACITY = 256`;
  `sync_channel(STREAMING_CHANNEL_CAPACITY)` — the egress channel (shared by compaction + flight).
- `cqlite-flight/src/streaming.rs` — the `spawn_blocking` sites for the merge/producer path.
- `cqlite-flight/src/main.rs` — server startup (owns the tokio runtime + `shutdown_signal()`); the
  sampler task spawns here. `#2316` already added the per-merge `cqlite.merge.producer_threads` gauge
  with the atomic-on-spawn/join pattern this change reuses for the channel + blocking gauges.

## Decisions

### D1 — Collection: a background sampler task, ~2s cadence (chosen over on-demand)
The `/proc`-derived gauges (`threads`, `fds`, `rss_bytes`) are sampled by ONE background tokio task
spawned in `main.rs`, ticking every ~2s and calling `obs::record_gauge` for each reader that returns
`Some`. **On-demand collection is rejected**: the whole point of these gauges is to see a *wedged*
`do_get` — one that emits no RPC completion and no batch — whose thread/fd/RSS footprint must stay
visible while it hangs (the #2361/#2157 hang class). An on-RPC-completion sample would go dark exactly
when it matters. Cost bound: 3 small `/proc` reads per tick (a directory entry count for `task`/`fd`,
one line-read for RSS) — O(1), no per-request/per-row work. The task takes the server's shutdown
token and returns when signaled (no leaked task, no busy-spin; a `tokio::select!` on the interval vs.
shutdown).

### D2 — `/proc` readers as pure `Option`-returning functions (deterministic, no-fabrication)
`read_proc_threads() -> Option<u64>`, `read_proc_fds() -> Option<u64>`, `read_proc_rss_bytes() ->
Option<u64>` are pure functions over `/proc/self/*`, unit-testable without a running server:
- threads = count of entries in `/proc/self/task`.
- fds = count of entries in `/proc/self/fd`.
- rss = `VmRSS` from `/proc/self/status` (kB → bytes) — **dependency-free pure `std::fs`**, chosen
  over `/proc/self/statm` × page-size because the latter needs `libc::sysconf(_SC_PAGESIZE)` (a new
  `libc` dep in `cqlite-flight`); `VmRSS` is a plain text field, no page-size math (open fork O2).

On Linux each returns `Some(v)` (the calling process always has ≥1 thread, several fds, non-zero RSS)
— a deterministic work-probe, no wall-clock. On any non-`/proc` platform each returns `None` (a
`#[cfg(target_os = "linux")]` / else branch), the sampler skips the `record_gauge`, and the gauge is
absent from the exposition — never `0`. The unsupported state logs ONCE at startup.

### D3 — Channel depth + blocking-task in-use via atomics (the #2316 pattern)
`std::sync::mpsc::sync_channel` exposes no `len()`, so occupancy is tracked explicitly:
- **`egress_channel_depth`**: a process-wide `AtomicI64` incremented on a successful `send`,
  decremented on a successful `recv`, at the merge/mod.rs send/recv sites; the resulting level is
  recorded to `cqlite.merge.egress_channel_depth`. Lives in `cqlite.merge.*` next to
  `producer_threads` (both merge-scoped, shared by compaction + flight). A `max(0)` floor guards
  against an unexpected imbalance (matching `RpcMetrics::finish`).
- **`blocking_tasks_in_use`**: a process-wide `AtomicI64` incremented on entry to a flight
  `spawn_blocking` closure and decremented on exit via an RAII guard (so a panic/cancel/early-return
  still decrements). Recorded to `cqlite.flight.blocking_tasks_in_use`.

Both update at their call sites independent of the sampler cadence, so the level is current between
ticks. Both are readable through a test accessor (like `obs::in_flight_level`) for the work-probe
tests — no wall-clock asserts.

### D4 — Registration: catalog constant + `ALL_METRICS` + `Instruments` field + `record_gauge` arm
Each gauge is added as an `i64_gauge` field in `otel::Instruments`, built once in `instruments()`,
matched in `record_gauge`. A test asserts each name resolves to a pre-built field (not the ad-hoc
fallback) so no saturation gauge rebuilds its instrument per sample (#2412). New unit constant
`unit::FDS = "{fd}"` and `unit::ENTRIES = "{entry}"` (threads reuse `unit::THREADS`; rss uses
`unit::BYTES`). Names/units are exactly what #2426's catalog-derived operator reference will render, so
they follow the `cqlite.<subsystem>.<signal>` shape already in use.

## Naming table (feeds #2426)
| Metric | Type | Unit | Source | Absence rule |
|---|---|---|---|---|
| `cqlite.proc.threads` | gauge | `{thread}` | `/proc/self/task` count | None off-Linux → no sample |
| `cqlite.proc.fds` | gauge | `{fd}` | `/proc/self/fd` count | None off-Linux → no sample |
| `cqlite.proc.rss_bytes` | gauge | `By` | `/proc/self/status` `VmRSS` | None off-Linux → no sample |
| `cqlite.merge.egress_channel_depth` | gauge | `{entry}` | atomic on send/recv | OS-independent (always emits) |
| `cqlite.flight.blocking_tasks_in_use` | gauge | `{thread}` | atomic on spawn_blocking enter/exit | OS-independent (always emits) |

## Open forks (Seam-1 owner calls)
- **O1 — the global `tokio` blocking-pool queue depth.** The true global pool `blocking_queue_depth`
  / `num_blocking_threads` are only exposed via `tokio::runtime::RuntimeMetrics` under a build-wide
  `RUSTFLAGS=--cfg tokio_unstable`, which affects the whole workspace build and pins tokio internals.
  This change ships the honest flight-scoped `blocking_tasks_in_use` proxy instead and does **not**
  set `tokio_unstable`. Fork: accept the flight-scoped proxy for 0.15 (recommended), or take on
  `tokio_unstable` in a follow-up to add a true-global-pool gauge.
- **O2 — RSS source.** Recommended: dependency-free `/proc/self/status` `VmRSS`. Alternative:
  `/proc/self/statm` resident-pages × `libc::sysconf(_SC_PAGESIZE)` (adds a `libc` dep to
  `cqlite-flight`; issue #2419 text mentions `statm`). Recommend `VmRSS` to keep the diff pure-`std`.
- **O3 — optional `dhat`-heap merge lane.** Issue #2419 lists an optional feature-flagged `dhat` heap
  lane (research §C1). `cqlite-flight` already has a `dhat-heap` feature (issue #1494). Fork: include
  a merge-path `dhat` lane in this change, or defer it to a separate profiling task (recommend defer —
  it is a dev/profiling lane, not a production gauge, and would widen this change's scope).
- **O4 — sampler cadence configurability.** Recommend a fixed ~2s constant for 0.15 (one fewer flag);
  fork: expose `--saturation-sample-interval-ms` if the ramp (WS8) needs finer resolution.
