# flight-streaming-egress Specification

## Purpose
TBD - created by archiving change streaming-do-get. Update Purpose after archive.
## Requirements
### Requirement: do_get emits batches incrementally

`FlightService::do_get` SHALL make the first `RecordBatch` available to the
client before the underlying merge has run to completion, for any result
spanning more than one batch.

#### Scenario: first batch arrives while the merge is still running

- **GIVEN** a table whose SSTables merge into at least 3 record batches
- **WHEN** a client opens `do_get` and reads the first record batch
- **THEN** the merge has NOT yet performed the steps for the remaining
  partitions (observed via a merge-step/work counter), and the remaining
  batches subsequently arrive with byte-identical total content
- **AND** this test FAILS on pre-change `main` (where the first batch is only
  available after all merge steps complete)

### Requirement: peak resident payload is bounded, independent of result size

The `do_get` path SHALL bound resident record-batch payload to a fixed number
of batches (channel capacity + in-flight allowance), independent of the total
result size, applying backpressure to the merge when the consumer is slow.

#### Scenario: slow consumer does not grow server memory

- **GIVEN** a table producing at least 4× the channel capacity in batches
- **WHEN** a client reads ONE batch and then pauses
- **THEN** the producer blocks after at most (channel capacity + in-flight
  allowance) further batches — verified by a batch-count/work-counter budget
  (or alloc-budget) assertion that FAILS on pre-change `main`, where all
  batches materialize regardless of consumer progress

### Requirement: consumer disconnect stops the merge

A dropped `do_get` client SHALL stop the underlying merge within a bounded
number of merge steps, via send-failure and/or the #1473 cancel flag.

#### Scenario: dropping the stream cancels the merge

- **GIVEN** an in-progress `do_get` over a multi-batch table
- **WHEN** the client drops the response stream after the first batch
- **THEN** the merge terminates without completing the remaining partitions
  (work counter strictly below the full-scan step count) and the blocking task
  exits; no batch is produced after cancellation beyond the bounded allowance

### Requirement: streamed output is byte-identical to collected output

The concatenated streamed batches SHALL be byte-identical to the batches
returned by the retained collect path (`produce`/`produce_cancellable`) for
any ticket, including limit, predicate-filter, and token-range cases.

#### Scenario: stream/collect parity across ticket shapes

- **GIVEN** fixtures exercising: no constraints, `limit=N` mid-batch,
  a predicate filter, and a token-range restriction
- **WHEN** the same ticket is executed through the streaming path and the
  collect path
- **THEN** row content, row order, schema, and batch boundaries are identical

### Requirement: rows/bytes metrics reflect what was emitted

RPC rows/bytes attribution for `do_get` SHALL equal the collected-path totals
for a fully consumed stream, and SHALL equal the actually-emitted subset for a
cancelled stream.

#### Scenario: metrics parity on full consumption

- **GIVEN** a multi-batch `do_get` fully consumed by the client
- **WHEN** the stream completes
- **THEN** recorded rows/bytes equal the pre-change materialized accounting for
  the same ticket

#### Scenario: cancelled stream attributes the emitted prefix

- **GIVEN** a client that reads one batch and disconnects
- **WHEN** the merge stops
- **THEN** recorded rows/bytes cover exactly the batches handed to the encoder,
  not the full table

### Requirement: aggregate path keeps materializing

The aggregate (`aggregate_paths`) route SHALL continue to materialize its
bounded per-group output and serve it as a stream, unchanged in content.

#### Scenario: aggregation results unchanged

- **GIVEN** an aggregation ticket over a multi-SSTable table
- **WHEN** `do_get` executes it before and after this change
- **THEN** the emitted partial-aggregate batches are identical

