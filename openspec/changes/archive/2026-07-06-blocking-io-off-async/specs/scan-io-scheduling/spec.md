## ADDED Requirements

### Requirement: Blocking scan I/O does not execute on an async worker thread

The windowed streaming scan SHALL NOT run a synchronously-faulting backend's raw chunk read
on any tokio async worker thread. When the scan reads from a backend that faults
synchronously (a memory-mapped file, whose first access to a cold page is a blocking disk
read, or an `O_DIRECT`/`F_NOCACHE` cursor, whose `pread` is a blocking uncached disk read),
the raw chunk read SHALL execute on a `spawn_blocking` thread. The buffered backend's
genuinely-async `tokio::fs` read SHALL continue to run inline on the async runtime (it is
reactor-driven and non-blocking). The offload decision SHALL key on the scan cursor's ACTUAL
backend, so a non-buffered request that gracefully degrades to buffered I/O at open is read
inline and is never driven under a non-tokio executor.

#### Scenario: mmap-backed scan reads off the async worker pool
- **WHEN** a full streaming scan runs over a real multi-chunk SSTable fixture opened with `use_mmap = true`, on a fixed-size async worker runtime with the `scan-offload-probe` instrumentation armed
- **THEN** the fixture returns a non-zero row count AND the `ThreadId` recorded for the scan's raw chunk read is not any of the enumerated async worker thread ids (it is a `spawn_blocking` thread)

#### Scenario: buffered-backed scan read stays on the async runtime
- **WHEN** a full streaming scan runs over the same fixture opened with the default buffered backend
- **THEN** the scan completes and returns the identical row set, and the buffered read path is not offloaded (its genuinely-async `tokio::fs` read remains driven by the runtime reactor)

### Requirement: The scheduling change preserves scan results exactly

Moving the blocking read off the async workers SHALL NOT change any scan output. A windowed
streaming scan over an mmap-backed reader SHALL emit the byte-identical row set, order, and
tombstone filtering as the same scan over a buffered-backed reader, and the scan's error
propagation (a mid-stream read error surfaces as a terminal stream item and suppresses the
partial trailing-window emit) SHALL be unchanged.

#### Scenario: mmap and buffered scans return identical rows
- **WHEN** the same table is scanned once via a reader opened with `use_mmap = true` and once via a buffered reader
- **THEN** the two scans return the same number of rows and the same row keys in the same order

### Requirement: The mixed-load tail latency stays bounded by a ratio, not a wall clock

The cold-mixed-load tail-latency gate SHALL bound the point-read p99 under a concurrent mmap
scan by a MULTIPLE `K` of the measured scan-free baseline p99 captured in the same run, never
by an absolute wall-clock threshold, so shared-runner / oversubscribed-CPU noise cannot flap
it. The gate SHALL capture the measurement window such that the background scan is
demonstrably live across all sampled point reads, and SHALL skip (not fail) when the fixture
binary is absent while failing loudly if a present fixture yields zero rows.

#### Scenario: cold mmap mixed-load p99 is within K× the scan-free baseline
- **WHEN** the tail-latency harness runs point reads against an mmap-backed fixture both scan-free and under a continuous background full-table scan
- **THEN** the mixed-load point-read p99 is at most `K ×` the scan-free baseline p99 measured in the same run, and the assertion is expressed as that ratio rather than any absolute nanosecond bound

#### Scenario: the gate is deterministic about fixture presence
- **WHEN** the fixture binary is not present in the datasets root
- **THEN** the gate skips with a message rather than failing, and when the fixture IS present but returns zero rows the harness panics loudly rather than reporting a vacuous pass

### Requirement: The offload gate is wired into the agent gate

The deterministic worker-starvation / I/O-offload guard SHALL be executed by
`scripts/agent-gate.sh` (added as a component or as a `--test` target of an existing
component that already enables the required features), following the existing scan-offload
guard's pattern, so the guard actually runs in the pre-merge gate rather than existing only
as an un-invoked test.

#### Scenario: the guard runs under the gate
- **WHEN** `scripts/agent-gate.sh` executes
- **THEN** the F3 I/O-offload guard test is among the tests it runs (built with the `scan-offload-probe` + `cli-helpers` features it requires), and a regression that runs the blocking read back on an async worker fails the gate
