# flight-saturation-instrumentation

## ADDED Requirements

### Requirement: Process resource gauges are sampled and exported under load

The server SHALL expose three process-wide OS-resource gauges — `cqlite.proc.threads`
(unit `{thread}`), `cqlite.proc.fds` (unit `{fd}`), and `cqlite.proc.rss_bytes` (unit `By`) — sampled
from the running process on Linux via `/proc/self/{task,fd,statm|status}`, so that under a
multi-reader `do_get` load the aggregate thread count, open-fd count, and resident-set size are each
visible on the server's own metric surface (not only through out-of-band `kubectl top`). The sampled
values SHALL move with offered load (rise as concurrent scans open SSTables and spawn producer
threads, fall back toward baseline as scans complete). Each gauge SHALL carry no high-cardinality
attributes (no ticket, key, query, or per-request label).

#### Scenario: The proc readers return a live, non-zero reading on Linux

- **WHEN** the process-resource readers are invoked on a Linux host (`/proc` present)
- **THEN** each of `read_proc_threads()`, `read_proc_fds()`, `read_proc_rss_bytes()` returns
  `Some(v)` with `v > 0` — a live reading of the current process, deterministically (no wall-clock
  wait), because the calling process itself has at least one thread, several open fds, and a non-zero
  resident set
- **AND** the readings are wired to `cqlite.proc.threads` / `cqlite.proc.fds` / `cqlite.proc.rss_bytes`
  via `obs::record_gauge` with the corresponding catalog names.

#### Scenario: Thread and fd gauges rise with concurrent scans and settle after

- **WHEN** N concurrent `do_get` scans are admitted against the flight service and then all complete
- **THEN** a work-probe read of the process thread count taken while the scans are in flight is
  strictly greater than the pre-load baseline read (concurrent scans open producer threads and fds)
- **AND** a read taken after every scan has completed returns to within a bounded delta of the
  pre-load baseline (the resources are released), asserted by comparing captured level snapshots, not
  by asserting an elapsed duration.

### Requirement: An unreadable resource reports absence, never a fabricated value

A gauge whose OS source is unavailable on the running platform SHALL report absence — the sampler
emits no sample for that gauge — rather than recording `0` or any synthesized value, honoring the
telemetry authoritative-data rule (a counter/gauge not observed is never a fabricated `0`). On a
non-`/proc` platform (e.g. macOS, Windows) the `/proc`-derived readers SHALL return `None`, the
sampler SHALL skip the corresponding `record_gauge` calls, and the server SHALL still start and serve
`do_get` normally. The platform's unsupported state SHALL be logged exactly once at startup, not
per sample.

#### Scenario: Non-/proc platform yields None and the server still serves

- **WHEN** the process-resource readers are invoked on a platform without `/proc` (the reader is
  compiled/exercised in its non-Linux branch)
- **THEN** `read_proc_threads()`, `read_proc_fds()`, and `read_proc_rss_bytes()` each return `None`
- **AND** the sampler records no sample for `cqlite.proc.threads` / `cqlite.proc.fds` /
  `cqlite.proc.rss_bytes` for that tick (the gauge is absent from the exposition, not `0`)
- **AND** the server startup path completes and a `do_get` is served, proving the sampler's absence
  does not gate serving.

### Requirement: The egress channel-depth gauge reflects real bounded-channel occupancy

The server SHALL expose `cqlite.merge.egress_channel_depth` (unit `{entry}`), tracking the live
occupancy of the bounded producer→consumer `sync_channel` (capacity `STREAMING_CHANNEL_CAPACITY`,
merge/mod.rs) that carries merged entries toward the `do_get` client. Because `std::sync::mpsc`'s
`sync_channel` exposes no length, occupancy SHALL be tracked by a process-wide atomic incremented on a
successful send and decremented on a successful receive (mirroring the #2316 producer-thread-gauge
pattern), and the resulting level SHALL be recorded to the gauge. A rising depth (channel near
capacity) SHALL indicate the producer outrunning a slower consumer; a depth near zero indicates the
consumer keeping up (or a stalled producer). The gauge SHALL carry no high-cardinality attributes and
SHALL live in the `cqlite.merge.*` namespace alongside `cqlite.merge.producer_threads`.

#### Scenario: Depth rises when the consumer is slower than the producer

- **WHEN** a producer fills the bounded channel faster than the consumer drains it (a producer-fast /
  consumer-slow harness against the tracked send/recv wrappers)
- **THEN** a work-probe read of the channel-depth atomic taken while the channel is backed up is
  greater than zero and bounded by the channel capacity
- **AND** after the consumer has drained every entry the depth atomic returns to zero (every tracked
  send is balanced by exactly one tracked receive), asserted on the level, not on timing.

### Requirement: A flight-managed blocking-task gauge proxies blocking-pool pressure without tokio_unstable

The server SHALL expose `cqlite.flight.blocking_tasks_in_use` (unit `{thread}`), counting the
`spawn_blocking` tasks the flight streaming/merge path currently has outstanding, tracked by a
process-wide atomic incremented when the flight path enters a `spawn_blocking` closure and decremented
when it exits (including on panic/cancel/early-return). This is an honest, dependency-free proxy for
blocking-pool pressure; it SHALL be documented as flight-managed-tasks-in-flight, NOT the global
`tokio` blocking-pool queue depth (which requires a build-wide `tokio_unstable` cfg and is out of
scope — see design open fork). The gauge SHALL never record a fabricated global-pool number and SHALL
carry no high-cardinality attributes.

#### Scenario: The in-use count rises with concurrent blocking tasks and balances to baseline

- **WHEN** the flight path enters and exits its `spawn_blocking` closures under N concurrent `do_get`
  scans, tracked through the increment/decrement wrapper
- **THEN** a work-probe read of the blocking-task atomic while tasks are outstanding exceeds the
  pre-load baseline
- **AND** after every tracked task has exited (normal completion, cancel, or panic) the atomic
  returns to its pre-load baseline — the increment and decrement are balanced on every exit path,
  asserted on the level.

### Requirement: Every saturation gauge is registered in the catalog and the OTel instrument struct

Each of the five saturation gauges SHALL be defined as a `cqlite.*` name constant in
`cqlite-core/src/observability/catalog.rs`, listed in `ALL_METRICS`, and built ONCE into the
`otel::Instruments` struct with a matching arm in `record_gauge` — never rebuilt per call (the #2412
lesson: a catalog name without an instrument-struct arm silently falls back to an ad-hoc
per-call-rebuilt instrument). The registration/uniqueness sanity checks SHALL cover the new names, and
each SHALL be rooted under `cqlite.` with a documented unit from `catalog::unit`.

#### Scenario: Registration and uniqueness checks cover the new gauges

- **WHEN** the catalog registration/uniqueness test runs
- **THEN** `ALL_METRICS` contains `cqlite.proc.threads`, `cqlite.proc.fds`, `cqlite.proc.rss_bytes`,
  `cqlite.merge.egress_channel_depth`, and `cqlite.flight.blocking_tasks_in_use`, each rooted under
  `cqlite.` and each appearing exactly once
- **AND** a test asserts each name resolves in `otel::record_gauge` to a pre-built `Instruments` field
  (not the ad-hoc fallback arm), so no saturation gauge rebuilds its instrument on every sample.

### Requirement: A bounded background sampler drives the process gauges and stops on shutdown

The process-resource gauges SHALL be driven by a single lightweight background task, on a fixed
cadence (~2s), whose per-tick cost is bounded (a small fixed number of `/proc` reads — no per-request
or per-row work), chosen over on-demand collection because a wedged `do_get` emits no RPC completion
yet its resource footprint must remain visible while it hangs. The sampler SHALL be spawned at server
startup and SHALL terminate on the server shutdown signal (no leaked task, no busy-spin). The
atomic-backed gauges (`egress_channel_depth`, `blocking_tasks_in_use`) SHALL update at their
send/recv/spawn sites independently of the sampler cadence, so their level is current between ticks.

#### Scenario: The sampler runs at least once and terminates on shutdown

- **WHEN** the sampler task is started with a short test cadence and then signaled to stop
- **THEN** a work-probe (a sample-count atomic or a captured gauge value) confirms the sampler
  performed at least one collection tick
- **AND** after the stop signal the task future resolves/returns (it does not run forever and does not
  busy-spin), asserted by the task handle completing, not by a wall-clock sleep.

### Requirement: The saturation gauges are a distinct family from the admission gauges

The saturation gauges SHALL NOT duplicate or overlap the #2420 admission gauges
(`cqlite.flight.admission.limit` / `.in_use` / `.waiting` / `.rejected_total` / `.wait_seconds`):
the admission family measures the `do_get` admission-queue state (a configured ceiling and permits),
whereas the saturation family measures OS-resource occupancy (threads, fds, RSS, channel depth,
blocking tasks). No saturation gauge SHALL re-count an admission signal, and the two families SHALL
remain independently named so #2426's generated operator reference can present them as separate
sections without double-counting.

#### Scenario: Saturation names are disjoint from the admission set

- **WHEN** the catalog is enumerated
- **THEN** the five saturation gauge names are pairwise distinct from the five
  `cqlite.flight.admission.*` names
- **AND** `cqlite.flight.blocking_tasks_in_use` (blocking-pool pressure) is a distinct metric from
  `cqlite.flight.admission.in_use` (held admission permits) — the two measure different resources and
  are never conflated.
