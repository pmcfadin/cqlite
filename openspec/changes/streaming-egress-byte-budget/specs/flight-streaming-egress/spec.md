# flight-streaming-egress — delta for streaming-egress-byte-budget (issue #2821)

## MODIFIED Requirements

### Requirement: peak resident payload is bounded, independent of result size

The `do_get` streaming path SHALL bound peak resident record-batch payload **in bytes**, independent
of the total result size **and independent of row width**, applying backpressure to the merge when
the consumer is slow.

Each batch's `RecordBatch::get_array_memory_size()` SHALL be charged against a per-stream in-flight
byte ceiling before the batch enters the egress channel, and that charge SHALL be released when the
batch leaves the stream toward the client. The guaranteed contract is:

> peak in-flight egress payload ≤ **`ceiling + one maximum batch`**

and NOT `ceiling` — see the deadlock-avoidance requirement below for why the one-batch residual is
structural. The residual term is capped by issue #2825 (T4 byte-bounded batch sizing), the named
follow-on.

This byte ceiling SHALL compose with, and SHALL NOT replace, the existing `DO_GET_CHANNEL_CAPACITY`
batch-count channel; whichever bound is reached first governs.

#### Scenario: slow consumer bounds in-flight egress bytes on a wide-row fixture

- **GIVEN** the synthetic wide-row fixture, whose per-batch payload is large enough that the byte
  ceiling binds before the 4-deep batch-count channel does
- **WHEN** a client reads ONE batch and then stops polling
- **THEN** the observed peak in-flight egress bytes never exceeds the configured ceiling plus the
  largest single batch observed on that stream
- **AND** the assertion is on measured BYTES (and batch counts), never on a wall-clock threshold
- **AND** this test FAILS on pre-change `main`, where residency is bounded only by a batch count and
  grows with row width

#### Scenario: narrow-row streams are not regressed

- **GIVEN** the existing narrow `keyvalue` fixture shape and the default byte ceiling
- **WHEN** a slow consumer reads one batch and pauses
- **THEN** the batch-count channel is still the binding governor and the produced-batch bound
  matches the pre-change structural bound (`DO_GET_CHANNEL_CAPACITY` + the in-flight allowance)
- **AND** the byte ceiling does not reduce the number of batches the producer may run ahead by at
  this row width

#### Scenario: the bound holds independent of total result size

- **GIVEN** a stream over a fixture producing many times the ceiling's worth of payload
- **WHEN** the stream is fully consumed
- **THEN** peak in-flight egress bytes at every point remain within the stated contract, and the
  total streamed content is byte-identical to the collect path for the same ticket

## ADDED Requirements

### Requirement: the byte ceiling never deadlocks, and the honest bound is stated

A single record batch MAY be larger than the entire configured ceiling. The egress governor SHALL
therefore always admit at least one batch when zero bytes are in flight, by clamping a batch's
credit request to the pool total. No batch of any size SHALL be able to wedge a stream.

Because an oversized batch is charged at most the whole ceiling while resident, the guaranteed bound
is `ceiling + one maximum batch`. The code and its doc comments SHALL state this bound honestly and
SHALL NOT claim a bound of `ceiling`.

#### Scenario: a batch larger than the whole ceiling is still delivered

- **GIVEN** a stream configured with a byte ceiling smaller than a single batch of the wide-row
  fixture
- **WHEN** the client consumes the stream
- **THEN** every batch is delivered and the stream terminates normally
- **AND** a naive non-clamping implementation hangs on this scenario (the test is the guard)

#### Scenario: the stated bound matches the enforced bound

- **WHEN** the wide-row ceiling test measures peak in-flight bytes against
  `ceiling + max observed batch bytes`
- **THEN** the assertion passes, and the same derivation appears in the governor's doc comment so
  the documented contract and the tested bound cannot drift apart

### Requirement: egress credit is released on every stream-termination path

Credit SHALL be released on normal drain, on stream drop (client disconnect), on cancellation, and
on a producer error or panic. A terminated stream SHALL NOT leak credit; after termination the full
per-stream pool SHALL be available again. Release SHALL be structural (ownership-based) rather than
dependent on re-measuring a batch at the drain side, so no measurement asymmetry can drift the pool.

#### Scenario: dropping the stream mid-flight releases all charged credit

- **GIVEN** an in-progress `do_get` over the wide-row fixture with batches queued in the channel
- **WHEN** the client drops the response stream after the first batch
- **THEN** all charged credit is returned (the pool is fully available), the merge stops, and the
  blocking task exits without wedging

#### Scenario: a producer parked on credit wakes on cancellation

- **GIVEN** a producer blocked waiting for egress credit because the consumer has stopped reading
- **WHEN** the shared cancel flag trips (client disconnect)
- **THEN** the producer stops promptly instead of remaining parked, exactly as it does today when
  parked on a full channel, and it pins no blocking-pool thread

#### Scenario: a mid-stream producer error does not strand credit

- **GIVEN** a stream whose merge raises a terminal `ProducerError` with batches still in flight
- **WHEN** the error is surfaced and the stream finalizes
- **THEN** no credit remains charged and the error reaches the same error-observability hook it does
  today

### Requirement: the egress byte ceiling is configurable from the server CLI

The `cqlite-flight` server SHALL expose the per-stream egress byte ceiling as a
`--max-inflight-egress-bytes` argument backed by the `CQLITE_MAX_INFLIGHT_EGRESS_BYTES` environment
variable and a `DEFAULT_MAX_INFLIGHT_EGRESS_BYTES` constant of **8 MiB**, mirroring the
`--max-concurrent-scans` / `CQLITE_MAX_CONCURRENT_SCANS` / `DEFAULT_MAX_CONCURRENT_SCANS` precedent.

The value SHALL be plumbed const → clap `Args` → a `CqliteFlightService` field set by a builder
mirroring `with_admission` → the `do_get` spawn site → `spawn_streaming` → the egress sink. Every
hop SHALL be a real call chain; a value that stops short of the sink is not wired.

`CqliteFlightService::new` SHALL apply the default ceiling (a byte credit can only delay a producer,
never turn a working query into an error), and an embedder SHALL be able to opt out explicitly to an
unbounded budget through the builder.

#### Scenario: the CLI-configured ceiling governs a real streamed do_get

- **GIVEN** a `CqliteFlightService` constructed the way `main` constructs it, with an explicitly
  configured small egress ceiling
- **WHEN** a client runs an end-to-end streaming `do_get` against it with a slow consumer
- **THEN** the observed peak in-flight egress bytes respect the configured ceiling (plus the
  one-batch residual), proving the value reached the governor through the whole chain
- **AND** this is an end-to-end test through the service surface, not a unit test on the credit
  helper alone

#### Scenario: the environment variable backs the flag

- **GIVEN** `CQLITE_MAX_INFLIGHT_EGRESS_BYTES` set in the environment and no explicit flag
- **WHEN** the server parses its arguments
- **THEN** the parsed ceiling equals the environment value, and an explicit flag overrides it

#### Scenario: the default is 8 MiB and fits the B4 reading

- **WHEN** neither the flag nor the environment variable is set
- **THEN** the ceiling is `DEFAULT_MAX_INFLIGHT_EGRESS_BYTES` = 8 MiB
- **AND** `ceiling + one maximum batch` at concurrency 1 sits within the ratified B4 ≤16Mi
  per-query working-set reading for any batch under ~8 MiB

#### Scenario: an embedder can opt out to an unbounded budget

- **GIVEN** a library caller that constructs the service with an explicitly unbounded egress budget
- **WHEN** it runs a streaming `do_get`
- **THEN** no byte ceiling is applied and residency reverts to the pre-change structural bound

### Requirement: the byte ceiling composes with admission K

The per-stream byte ceiling SHALL NOT change admission `K`, its default, or its shedding policy. The
two governors SHALL remain independent: `K` bounds concurrently admitted scans server-wide, the byte
ceiling bounds payload in flight within one stream. The documented server-wide worst case SHALL be
`K × (per-stream ceiling + one maximum batch)`.

#### Scenario: admission behaviour is unchanged

- **WHEN** the existing admission tests run after this change
- **THEN** permit acquisition, the wait budget, and `UNAVAILABLE` shedding behave exactly as before,
  and no admission default is altered

#### Scenario: whichever governor binds first wins

- **GIVEN** a single stream at a narrow row width and the same stream at a wide row width
- **WHEN** each runs with a slow consumer
- **THEN** the narrow case is governed by the batch-count channel and the wide case by the byte
  ceiling, with no interaction that relaxes either bound

### Requirement: the channel-depth doc comment states real production residency

The `DO_GET_CHANNEL_CAPACITY` doc comment SHALL state production residency as approximately
`(DO_GET_CHANNEL_CAPACITY + 2) × batch_size` ≈ 49,152 rows at the default `batch_size`, SHALL flag
that figure as **row-width dependent**, and SHALL NOT cite the `#[cfg(test)]`-only
`IN_FLIGHT_ALLOWANCE` as a production quantity or propagate the stale 57,344-row over-count. Its
current claim that the depth is "deliberately not a config knob" SHALL be replaced by a pointer to
the byte ceiling as the configurable governor.

This correction is scoped to the source doc comment. The historical phase-research documents are
dated analysis snapshots and SHALL NOT be rewritten by this change, and
`docs/architecture/throughput-program-2026-07.md` manifest item M11 SHALL be left to issue #2825.

#### Scenario: the revised comment carries the production figure and the knob

- **WHEN** a reader consults the `DO_GET_CHANNEL_CAPACITY` doc comment after this change
- **THEN** it states the ~49,152-row production residency as row-width dependent, does not present
  the test-only allowance as production, contains no 57,344 figure, and names
  `--max-inflight-egress-bytes` as the byte governor

### Requirement: the wide-row fixture is synthetic, deterministic, and self-contained

The wide-row fixture backing the ceiling tests SHALL live in `cqlite-flight/src/test_fixtures.rs`,
SHALL be built from in-process mutations with pinned content and a fixed (non-wall-clock) write
timestamp, and SHALL NOT depend on the fetched `test_wide_rows` dataset or any other external
dataset — a dataset-dependent test that passes vacuously on an absent dataset is not acceptance
evidence.

#### Scenario: the ceiling tests run with no fetched dataset present

- **GIVEN** a checkout with no `CQLITE_DATASETS_ROOT` datasets fetched
- **WHEN** the wide-row egress tests run
- **THEN** they build their own SSTables, stream a non-zero number of rows, and assert a real
  ceiling — they neither skip nor pass on an empty result

#### Scenario: the fixture is deterministic across runs

- **WHEN** the wide-row fixture is materialized twice in the same process or across runs
- **THEN** the row content, row count, and schema are identical, so a byte-based ceiling assertion
  is stable and not timing- or content-dependent
