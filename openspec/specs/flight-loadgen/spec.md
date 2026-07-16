# flight-loadgen Specification

## Purpose
TBD - created by archiving change flight-loadgen. Update Purpose after archive.
## Requirements
### Requirement: drives do_get directly over a raw FlightServiceClient

The `flight-loadgen` tool SHALL issue `do_get` requests to a `cqlite-flight`
endpoint through a raw `arrow_flight::FlightServiceClient<Channel>` (tonic) over a
real gRPC transport, and MUST NOT route through Trino, the JDBC connector, or the
`cqlite-core` query engine, so that the measurement isolates server-side behavior.

#### Scenario: a request reaches the server as a real do_get over the wire

- **GIVEN** a `cqlite-flight` server serving a fixture table over a loopback
  gRPC endpoint
- **WHEN** `flight-loadgen` runs a one-request ramp against that endpoint with a
  ticket for the fixture table
- **THEN** the request is delivered via `FlightServiceClient::do_get` over the
  transport and its Arrow `RecordBatch` stream is decoded by the client
- **AND** no Trino/JDBC/`cqlite-core`-query-engine component participates in the
  client path

### Requirement: parameterized deterministic concurrency ramp

The tool SHALL execute an ordered sequence of ramp steps, each step holding a
configured target concurrency of in-flight `do_get`s for a configured step
duration, and its ticket selection SHALL be reproducible from a configured seed so
two runs with the same seed and ramp produce the same request sequence.

#### Scenario: each step maintains its target concurrency for its duration

- **GIVEN** a ramp `1,2,4` with a fixed per-step duration
- **WHEN** the tool runs the ramp against a live endpoint
- **THEN** it emits exactly one step record per configured level in order, each
  reporting its `target_concurrency`, and during a step at most that many
  `do_get`s are in flight at once

#### Scenario: identical seed reproduces the ticket sequence

- **GIVEN** two runs with the same `--seed`, `--ramp`, `--shape`, and template
- **WHEN** the deterministic ticket sequence for a fixed step and worker is
  generated in each run
- **THEN** the two sequences are byte-identical (token ranges, limits, and shape
  choices match), so wall-clock timing never perturbs which data is requested

### Requirement: four workload shapes synthesized from a base ticket template

The tool SHALL support the workload shapes `point`, `limit-k`, `full`, and
`mixed`, each derived by transforming a clone of an operator-supplied base ticket
template, and MUST NOT fabricate DDL or partition keys the operator did not
provide.

#### Scenario: each shape produces the expected ticket transform

- **GIVEN** a base ticket template for `keyspace.table` describing the full ring
  with no limit
- **WHEN** each shape builds its ticket
- **THEN** `full` yields the template with `limit = None` over the full ring;
  `limit-k` yields the template with `limit = Some(k)`; `point` yields the
  template narrowed to a seeded token sub-range `[t, t + width)`; and `mixed`
  yields a seeded weighted draw across those three
- **AND** every produced ticket carries the template's original `keyspace`,
  `table`, `ddl`, and `snapshot` unchanged

### Requirement: outcome classification distinguishes admission shedding from errors

Each `do_get` outcome SHALL be classified as exactly one of `ok`, `unavailable`,
or `error`; a gRPC `UNAVAILABLE` status SHALL be counted as `unavailable` (the
retry-safe admission-shed signal per issue #2420) and every other non-success
status or transport/decode failure SHALL be counted as `error` and recorded with
its status code.

#### Scenario: an admission shed is counted as unavailable, not error

- **GIVEN** a request whose `do_get` returns gRPC status `UNAVAILABLE` before any
  batch is delivered
- **WHEN** the tool classifies the outcome
- **THEN** the step's `requests_unavailable` increments by one and
  `requests_error` does not

#### Scenario: any other failure status is counted as error with its code

- **GIVEN** a request whose `do_get` fails with a status other than `UNAVAILABLE`
  (for example `Internal` or `InvalidArgument`) or with a transport/decode error
- **WHEN** the tool classifies the outcome
- **THEN** the step's `requests_error` increments by one and the status code is
  recorded under `error_codes`, while `requests_unavailable` is unchanged

### Requirement: memory-bounded response consumption

The tool SHALL drain each `do_get` response stream by consuming and immediately
dropping each `RecordBatch` while accumulating only running row and byte counters,
and MUST NOT retain the decoded result set, so peak client memory is bounded by
concurrency and one in-flight batch rather than by result-set size.

#### Scenario: a large result does not accumulate in memory

- **GIVEN** a `do_get` that streams many record batches
- **WHEN** the worker consumes the stream
- **THEN** each batch's row count and memory size are added to the running
  counters and the batch is dropped before the next is polled
- **AND** no `Vec<RecordBatch>` (or equivalent) retaining the full result is held

### Requirement: JSONL per-step output consumable by the round-N metrics template

The tool SHALL emit one JSON object per step, one per line (JSONL), each carrying
at minimum the step's `target_concurrency`, `shape`, `duration_s`, per-class
counts (`requests_ok`, `requests_unavailable`, `requests_error`), `qps`,
`rows_per_s`, `bytes_per_s`, `rows_total`, `bytes_total`, and latency percentiles
`p50`/`p95`/`p99`/`max`, so the records feed the #2399 C-throughput block and diff
cleanly between rounds.

#### Scenario: a step record carries the required fields and is valid JSONL

- **GIVEN** a completed ramp step
- **WHEN** the tool writes its record
- **THEN** the line parses as a single JSON object containing every required
  field with the correct types
- **AND** `qps` equals `requests_ok / duration_s` and the latency percentiles are
  computed over the `ok` requests of that step

### Requirement: cheap in-process self-test, not a gate component

The tool SHALL provide a self-test that serves `cqlite-flight` in-process on an
ephemeral loopback port and runs a fixed, request-count-bounded (non-wall-clock)
ramp end-to-end, asserting a well-formed JSONL record is produced; this self-test
MUST be a normal workspace test and SHALL NOT be registered as an
`scripts/agent-gate.sh` component nor contact any real cluster.

#### Scenario: the self-test exercises the client to server to JSONL pipeline

- **GIVEN** a tiny in-process fixture table served over `127.0.0.1:0`
- **WHEN** the self-test runs a concurrency-1, fixed-request-count ramp against it
- **THEN** it emits at least one JSONL step record with `requests_ok >= 1` and all
  required fields present and parseable
- **AND** the test uses no fixed port and no wall-clock-duration step, so it is
  deterministic and free of port/timing flake

