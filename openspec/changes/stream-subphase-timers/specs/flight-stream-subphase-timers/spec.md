# flight-stream-subphase-timers — delta for stream-subphase-timers (issue #2819)

## ADDED Requirements

### Requirement: The in-`stream` data plane is attributed across at least four bounded sub-phases
The Flight `do_get` `stream` phase SHALL be decomposed into at least four data-plane sub-phases —
cold-fault, decompress, merge, encode, and gRPC-write — each recorded as a `cqlite.rpc.phase.duration`
histogram sample tagged with a distinct, closed-set value of the `cqlite.rpc.phase` attribute
(`stream_cold_fault`, `stream_decompress`, `stream_merge`, `stream_encode`, `stream_grpc_write`). A
sub-phase that a given `do_get` never entered SHALL record no sample for that value (never a
fabricated zero). The sub-phases are measured on the CONCURRENT threads of the streaming read pipeline
(page-in + decompress on the per-SSTable feed thread; merge + encode on the merge consumer thread;
gRPC-write on the egress thread), so they OVERLAP in wall-clock time and SHALL NOT be expected to sum
to the `stream` phase's duration. The top-level `stream` phase SHALL retain its exact meaning as the
whole data-plane wall-clock total, unchanged by this decomposition.

#### Scenario: A completed do_get over a real fixture records at least four distinct sub-phase samples
- **GIVEN** a `cqlite-flight` `do_get` run over a real multi-row, compressed SSTable fixture that faults in cold body chunks, reconciles, encodes, and streams batches to the client
- **WHEN** the whole response stream is drained and the emitted metrics are captured
- **THEN** `cqlite.rpc.phase.duration` carries at least one sample for at least four of the bounded sub-phase values (`stream_cold_fault`, `stream_decompress`, `stream_merge`, `stream_encode`, `stream_grpc_write`), each tagged with the `do_get` method

#### Scenario: Each recorded sub-phase is a positive share of the RPC wall time
- **WHEN** the captured `cqlite.rpc.phase.duration` samples for a completed `do_get` are grouped by the `cqlite.rpc.phase` attribute
- **THEN** each recorded sub-phase's duration is greater than zero and no greater than the RPC's total wall time (`cqlite.rpc.duration`), and the top-level `stream` sample continues to represent the whole data-plane wall-clock total — the sub-phases attribute the in-`stream` cost across concurrent pipeline stages WITHOUT summing to it (they overlap in wall-clock, so their sum may exceed `stream`)

#### Scenario: A sub-phase never entered records no sample
- **GIVEN** a `do_get` whose plan performs no work in one specific sub-phase (e.g. an uncompressed-fixture run that never invokes decompression)
- **WHEN** the emitted metrics are captured
- **THEN** no `cqlite.rpc.phase.duration` sample tagged with that sub-phase's value is present, and the other sub-phases still record their samples

### Requirement: The cold-fault sub-phase is isolable from send park/wake
The `stream_cold_fault` sub-phase SHALL measure only the synchronous SSTable body page-in (cold-IO
latency) on the read feed thread, and SHALL NOT include the channel send/backpressure park time, which
SHALL be attributed to a disjoint `stream_grpc_write` sub-phase measured on the egress thread. The two
scopes SHALL share no code interval and run on distinct threads, so a send-side stall cannot inflate
`stream_cold_fault` — cold-IO latency is readable off the dashboard independently of client-drain speed
(without a profiler).

#### Scenario: A slow client inflates gRPC-write but not cold-fault
- **GIVEN** two `do_get` runs over the same cold fixture: one drained promptly and one whose client deliberately stalls draining the channel so the producer parks in `sink.emit`
- **WHEN** the emitted `cqlite.rpc.phase.duration` samples are captured for both
- **THEN** the stalled run's `stream_grpc_write` duration is materially larger than the prompt run's, while its `stream_cold_fault` duration is not inflated by the client stall (cold-fault reflects page-in latency only)

#### Scenario: The cold-fault and gRPC-write timing scopes are measured disjointly
- **WHEN** the stream-loop instrumentation is inspected
- **THEN** `stream_cold_fault` is measured only around the reader body-chunk page-in on the feed thread, and `stream_grpc_write` only around the egress channel `reserve()`/send on the merge/egress thread — the two scopes share no code interval and run on distinct threads, so a send-side park/wake interval is never counted under `stream_cold_fault`

#### Scenario: The cold-warm delta on cold-fault is readable as the cold-IO bucket
- **GIVEN** the same full-scan `do_get` run cold (first touch) and again warm (pages resident)
- **WHEN** the `stream_cold_fault` samples from both runs are compared
- **THEN** the cold run's `stream_cold_fault` exceeds the warm run's by the cold-IO page-in latency, and this delta is obtainable from the standing metric alone (no profiler attached)

### Requirement: The sub-phase timers add no new metrics stack
The sub-phase timers SHALL hang off the existing observability surface (epic AI #1686): the same
`cqlite.rpc.phase.duration` histogram and the same `cqlite.rpc.phase` bounded attribute key. No new
metric name, OTel meter, exporter, or attribute key SHALL be introduced, and the meaning of the five
top-level RPC phases (`validate`, `admission`, `resolve`, `merge_setup`, `stream`) SHALL be unchanged.
The `cqlite.rpc.phase` attribute SHALL remain a closed, static, low-cardinality value set — never a
ticket, key, query, or payload value.

#### Scenario: No new metric name or attribute key is registered
- **WHEN** the observability catalog (`cqlite_core::observability::catalog::ALL_METRICS`) and the metrics emitted over a `do_get` are inspected
- **THEN** the only histogram carrying sub-phase samples is the pre-existing `cqlite.rpc.phase.duration`, the only attribute key distinguishing them is the pre-existing `cqlite.rpc.phase`, and no new metric name or attribute key has been added to the catalog

#### Scenario: Every emitted phase value stays within the closed set
- **WHEN** the `cqlite.rpc.phase` attribute values emitted over a completed `do_get` are collected
- **THEN** each value is a member of the closed set (the five top-level phases plus the bounded sub-phase values), and no unexpected or unbounded value ever appears

#### Scenario: The top-level phases keep their meaning
- **WHEN** a `do_get` records its phase samples
- **THEN** the `validate`, `admission`, `resolve`, and `merge_setup` samples are recorded exactly as before this change, and `stream` still represents the whole data-plane total

### Requirement: The sub-phase timers are wired and exercised end-to-end on a real stream
The sub-phase timers SHALL be emitted by the production `do_get` streaming path (not only a helper
unit), and this SHALL be proven by an end-to-end test that drives a real `do_get`, drains the stream,
and asserts the sub-phase samples over the captured metrics — mirroring the existing phase-timing
wiring proof (`cqlite-flight/tests/metrics_capture_test.rs`). Sample count per sub-phase SHALL be
bounded (emitted once per sub-phase at stream teardown, never once per row).

#### Scenario: The production streaming path emits the sub-phase samples
- **GIVEN** the real `CqliteFlightService::do_get` streaming handler (the same path that drives `PhaseTimer` from `merge_setup` into `stream`)
- **WHEN** an end-to-end test runs a `do_get` over a real fixture and drains the whole stream
- **THEN** the captured `cqlite.rpc.phase.duration` metric carries the sub-phase samples, proving the timers are wired into the production egress path and not merely a stand-alone helper

#### Scenario: Sub-phase emission is bounded per RPC, not per row
- **GIVEN** a `do_get` that streams many rows across many batches
- **WHEN** the emitted `cqlite.rpc.phase.duration` samples are counted
- **THEN** the number of sub-phase samples is bounded by the number of sub-phases (one per sub-phase that recorded time), independent of the row or batch count
