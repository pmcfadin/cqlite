# flight-streaming-egress — delta for streaming-egress-byte-budget (issue #2821)

## MODIFIED Requirements

### Requirement: peak resident payload is bounded, independent of result size

The `do_get` streaming path SHALL bound peak resident record-batch residency **in bytes**,
independent of the total result size **and independent of row width**, applying backpressure to the
merge when the consumer is slow.

The governed quantity is each batch's `RecordBatch::get_array_memory_size()` — Arrow buffer
**CAPACITY** bytes, the quantity `MeteredDoGetStream::poll_next` already meters for metrics. That
capacity SHALL be charged against a per-stream in-flight byte ceiling before the batch enters the
egress channel, and the charge SHALL be released when the batch has left the stream toward the
client. The guaranteed contract is:

> peak charged in-flight egress **capacity** ≤ **`ceiling + one maximum batch`**

and NOT `ceiling` — see the deadlock-avoidance requirement below for why the one-batch residual is
structural, and for the terms that sit outside the governed set.

This byte ceiling SHALL compose with, and SHALL NOT replace, the existing `DO_GET_CHANNEL_CAPACITY`
batch-count channel; whichever bound is reached first governs.

#### Scenario: slow consumer bounds in-flight egress bytes on a wide-row fixture

- **GIVEN** the synthetic wide-row fixture (`cqlite-flight/src/wide_row_fixture.rs`, merged with
  issue #2825), whose per-batch capacity is large enough that the byte ceiling binds before the
  4-deep batch-count channel does
- **WHEN** a client reads ONE batch and then stops polling
- **THEN** the observed peak charged in-flight capacity bytes never exceed the configured ceiling
  plus the largest single batch capacity observed on that stream
- **AND** the assertion is on measured `get_array_memory_size()` BYTES (and batch counts), never on
  a wall-clock threshold
- **AND** this test FAILS on pre-change `main`, where egress residency is bounded only by a batch
  count and grows with row width

#### Scenario: narrow-row streams are not regressed

- **GIVEN** the existing narrow `keyvalue` / `narrow_rows` fixture shapes and the default ceiling
- **WHEN** a slow consumer reads one batch and pauses
- **THEN** the batch-count channel is still the binding governor and the produced-batch bound
  matches the pre-change structural bound (`DO_GET_CHANNEL_CAPACITY` + the in-flight allowance)
- **AND** the byte ceiling does not reduce the number of batches the producer may run ahead by at
  this row width

#### Scenario: the bound holds independent of total result size

- **GIVEN** a stream over a fixture producing many times the ceiling's worth of capacity
- **WHEN** the stream is fully consumed
- **THEN** peak charged in-flight capacity at every point remains within the stated contract, and
  the total streamed content is byte-identical to the collect path for the same ticket

## ADDED Requirements

### Requirement: the ceiling is denominated in capacity bytes and consumes #2825's published conversion

Two byte currencies meet at this boundary and SHALL be named explicitly wherever they meet:

- **Payload bytes** (sum of Arrow buffer *lengths*, `cqlite_core::export::arrow_payload_bytes`) —
  the currency of issue #2825's per-batch cap `DEFAULT_MAX_BATCH_BYTES` (4 MiB). Estimable before a
  batch exists, monotonic in row count, which is why it can be a *trigger*.
- **Capacity bytes** (`RecordBatch::get_array_memory_size()`) — the currency of THIS change's
  per-stream ceiling. Readable only after a batch exists, up to ~2× payload because `MutableBuffer`
  doubles from zero, which is why it cannot be a trigger but IS the right unit for a residency
  ceiling.

The per-stream ceiling SHALL be denominated in capacity bytes. Any composition of the per-batch cap
with the per-stream ceiling SHALL convert through the published constant
`cqlite_flight::batch_bytes::BATCH_BYTES_CAPACITY_FACTOR` (and `worst_case_batch_capacity_bytes`),
NEVER through a locally re-derived factor and NEVER by comparing a payload figure with a capacity
figure directly. Code and docs SHALL NOT state a composition that adds `DEFAULT_MAX_BATCH_BYTES`
(payload) to the ceiling (capacity) as if the two were the same unit.

#### Scenario: the composition is asserted from the published constants, not hard-coded

- **GIVEN** `DEFAULT_MAX_INFLIGHT_EGRESS_BYTES` and `cqlite_flight::batch_bytes`'s
  `DEFAULT_MAX_BATCH_BYTES` / `BATCH_BYTES_CAPACITY_FACTOR` / `worst_case_batch_capacity_bytes`
- **WHEN** a test computes `DEFAULT_MAX_INFLIGHT_EGRESS_BYTES + worst_case_batch_capacity_bytes(
  DEFAULT_MAX_BATCH_BYTES, n_array_nodes, 0)` for a small flat schema
- **THEN** the result is ≤ 16 MiB (the ratified B4 per-query working set at concurrency 1), so a
  later change to EITHER constant that breaks the composition fails this test rather than silently
  invalidating the doctrine

#### Scenario: no locally re-derived payload→capacity factor exists

- **WHEN** the change's code and doc comments are read
- **THEN** the payload→capacity conversion appears only as a use of
  `BATCH_BYTES_CAPACITY_FACTOR` / `worst_case_batch_capacity_bytes`, with no second definition of
  the factor `2` and no undocumented fudge

### Requirement: the byte ceiling never deadlocks, and the honest bound is stated

A single record batch MAY be larger than the entire configured ceiling. The egress governor SHALL
therefore always admit at least one batch when zero bytes are in flight, by clamping a batch's
credit request to the pool total. No batch of any size SHALL be able to wedge a stream.

Because a clamped batch is charged at most the whole ceiling while resident, the guaranteed bound is
`ceiling + one maximum batch`. The code and its doc comments SHALL state this bound honestly, SHALL
NOT claim a bound of `ceiling`, and SHALL name the terms that sit OUTSIDE the governed set rather
than let a reader assume the governor covers them:

1. **The producer's pre-credit batch.** `emit` receives an already-materialized batch and only then
   acquires credit, so one batch can be resident while parked and uncharged. This is the
   pre-existing "+1 send-in-flight" residency term (it exists today against the channel), unchanged
   by this change — but it is not covered by the ceiling and SHALL be documented as such.
2. **A single row wider than #2825's per-batch cap**, which leaves as a one-row batch at its own
   natural width (`worst_case_batch_capacity_bytes`'s `max(cap, widest_row_payload)` term).
3. **`BATCH_BYTES_PER_COLUMN_SLACK × n_array_nodes`** of fixed per-array-node allocation.

#### Scenario: a batch larger than the whole ceiling is still delivered

- **GIVEN** a stream configured with a byte ceiling smaller than a single batch of the wide-row
  fixture
- **WHEN** the client consumes the stream
- **THEN** every batch is delivered and the stream terminates normally
- **AND** a naive non-clamping implementation hangs on this scenario (the test is the guard)

#### Scenario: the stated bound matches the enforced bound

- **WHEN** the wide-row ceiling test measures peak charged in-flight capacity against
  `ceiling + max observed batch capacity`
- **THEN** the assertion passes, and the same derivation appears in the governor's doc comment so
  the documented contract and the tested bound cannot drift apart

#### Scenario: the un-governed residency terms are named, not hidden

- **WHEN** a reader consults the governor's doc comment
- **THEN** it states the three terms above explicitly (the parked pre-credit batch, an over-cap
  single row, the per-node slack), so nobody reads `ceiling + one maximum batch` as covering the
  producer's own hand

### Requirement: credit release is deferred by one batch so the encoder prefetch stays inside the bound

`MeteredDoGetStream` is constructed UPSTREAM of the Flight encoder (`encode_do_get(metered, …)`),
and `FlightDataEncoderBuilder`'s stream can pull one batch ahead of yielding it. Releasing a batch's
credit at the instant `poll_next` yields it would therefore leave that batch resident with its
credit already returned, making the true bound `ceiling + 2 × one maximum batch`.

The stream SHALL therefore hold the yielded batch's permit in a single deferred slot and release it
only when the NEXT batch is yielded (and on `Drop`). At most one batch is downstream of the credit
boundary at any time, which is what makes `ceiling + one maximum batch` true as stated.

#### Scenario: the yielded batch's credit is still charged while the encoder holds it

- **GIVEN** a stream whose consumer has taken exactly one batch
- **WHEN** the charged in-flight total is observed
- **THEN** it still includes the just-yielded batch's capacity — the permit has not been released —
  and it is released only once the following batch is yielded

#### Scenario: dropping the stream releases the deferred permit

- **GIVEN** a stream holding a deferred permit for the last yielded batch
- **WHEN** the stream is dropped
- **THEN** the deferred permit is released and the full pool is available again

### Requirement: egress credit is released on every stream-termination path

Credit SHALL be released on normal drain, on stream drop (client disconnect), on cancellation, and
on a producer error or panic. A terminated stream SHALL NOT leak credit; after termination the full
per-stream pool SHALL be available again. Release SHALL be structural (ownership-based: the permit
rides with the batch through the channel) rather than dependent on re-measuring a batch at the drain
side, so no measurement asymmetry can drift the pool.

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
variable and a `DEFAULT_MAX_INFLIGHT_EGRESS_BYTES` constant of **6 MiB of CAPACITY bytes**,
mirroring the merged `--max-batch-bytes` / `CQLITE_MAX_BATCH_BYTES` / `DEFAULT_MAX_BATCH_BYTES`
plumbing precedent from issue #2825 (itself modelled on `--max-concurrent-scans`).

The value SHALL be plumbed const → clap `Args` → a `CqliteFlightService` field set by a builder
mirroring `with_max_batch_bytes` → the `do_get` spawn site (`spawn_streaming_from_readers`, the sole
production spawn site) → `spawn_streaming` → the egress sink. Every hop SHALL be a real call chain;
a value that stops short of the sink is not wired.

Like #2825's byte-cap and unlike admission `K`, the ceiling SHALL be ON by default on EVERY
construction path including `CqliteFlightService::new` (a byte credit can only delay a producer,
never turn a working query into an error), with an explicit opt-out to an unbounded budget for an
embedder.

#### Scenario: the CLI-configured ceiling governs a real streamed do_get

- **GIVEN** a `CqliteFlightService` constructed the way `main` constructs it, with an explicitly
  configured small egress ceiling
- **WHEN** a client runs an end-to-end streaming `do_get` against it with a slow consumer
- **THEN** the observed peak charged in-flight capacity respects the configured ceiling (plus the
  one-batch residual), proving the value reached the governor through the whole chain
- **AND** this is an end-to-end test through the service surface, not a unit test on the credit
  helper alone

#### Scenario: the environment variable backs the flag

- **GIVEN** `CQLITE_MAX_INFLIGHT_EGRESS_BYTES` set in the environment and no explicit flag
- **WHEN** the server parses its arguments
- **THEN** the parsed ceiling equals the environment value, and an explicit flag overrides it

#### Scenario: the default is 6 MiB and the composition fits B4

- **WHEN** neither the flag nor the environment variable is set
- **THEN** the ceiling is `DEFAULT_MAX_INFLIGHT_EGRESS_BYTES` = 6 MiB of capacity
- **AND** composing it with #2825's merged 4 MiB payload cap through
  `BATCH_BYTES_CAPACITY_FACTOR = 2` gives `6 MiB + (2 × 4 MiB) = 14 MiB` of capacity for
  `ceiling + one maximum batch`, inside the ratified **B4 ≤16Mi per-query working set at
  concurrency 1** with ~2 MiB of headroom (less `BATCH_BYTES_PER_COLUMN_SLACK × n_array_nodes`)

#### Scenario: an embedder can opt out to an unbounded budget

- **GIVEN** a library caller that constructs the service with an explicitly unbounded egress budget
- **WHEN** it runs a streaming `do_get`
- **THEN** no byte ceiling is applied and residency reverts to the pre-change structural bound

### Requirement: the byte ceiling composes with admission K

The per-stream byte ceiling SHALL NOT change admission `K`, its default, or its shedding policy. The
two governors SHALL remain independent: `K` bounds concurrently admitted scans server-wide, the byte
ceiling bounds capacity in flight within one stream. The documented server-wide worst case SHALL be
`K × (per-stream ceiling + one maximum batch)` — `K × 14 MiB` at the defaults.

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
the byte ceiling as the configurable governor of the same residency.

This correction is scoped to the source doc comment. The historical phase-research documents are
dated analysis snapshots and SHALL NOT be rewritten by this change.

#### Scenario: the revised comment carries the production figure and the knob

- **WHEN** a reader consults the `DO_GET_CHANNEL_CAPACITY` doc comment after this change
- **THEN** it states the ~49,152-row production residency as row-width dependent, does not present
  the test-only allowance as production, contains no 57,344 figure, and names
  `--max-inflight-egress-bytes` as the byte governor

### Requirement: the #2825 byte-cap documentation is corrected to state the now-enforced composition

Issue #2825 shipped documentation that scoped its own guarantee honestly by describing per-stream
egress residency as still count-bounded and the 14 MiB composition as a TARGET for this issue. This
change makes those statements false, and SHALL update them in the same change that makes them false:

- `cqlite-flight/src/batch_bytes.rs`'s module documentation SHALL no longer state that `do_get` is
  count-bounded at `~7 × 8 MiB ≈ 56 MiB` per stream, nor that the 14 MiB composition "becomes true
  only once #2821 lands" / "is a TARGET for the dependent issue". It SHALL instead state that the
  per-stream capacity ceiling is enforced here, name the default, and RETAIN the payload-vs-capacity
  currency explanation and the published-constant conversion, which remain correct.
- `worst_case_batch_capacity_bytes`'s doc comment SHALL likewise drop "Until that ceiling lands,
  egress residency is count-bounded, not byte-bounded".
- `docs/flight-trino/JOURNAL.md`'s "B4 composition for issue #2821" bullet SHALL be corrected from a
  prospective statement to the enforced one, naming this issue as delivered.

No other historical document SHALL be rewritten.

#### Scenario: the merged byte-cap module docs no longer describe egress as count-bounded

- **WHEN** `cqlite-flight/src/batch_bytes.rs` is read after this change
- **THEN** it contains no `~56 MiB` count-bounded claim and no "TARGET for #2821" framing, and it
  states the enforced per-stream ceiling with its default and its currency

#### Scenario: the Flight/Trino journal states the composition as enforced

- **WHEN** the B4-composition entry in `docs/flight-trino/JOURNAL.md` is read after this change
- **THEN** it states `6 + 8 = 14 MiB` as the enforced composition (not a prospective one), keeps the
  payload-vs-capacity correction that motivates it, and points at this issue as the delivery

### Requirement: the wide-row fixture is the merged synthetic one, reused not duplicated

The wide-row fixture backing the ceiling tests SHALL be the existing
`cqlite-flight/src/wide_row_fixture.rs` (merged with issue #2825): a `test-util`-gated module of
deterministic, in-process shapes (`wide_row_schema`, `wide_row_mutations(n_rows, payload_len)`,
`narrow_row_schema`, `narrow_row_mutations`) with a fixed non-wall-clock `FIXTURE_TIMESTAMP`. This
change SHALL NOT add a second wide-row fixture (in `test_fixtures.rs` or elsewhere); if the ceiling
tests need a shape the module does not have, it SHALL be added there.

The fixture SHALL NOT depend on the fetched `test_wide_rows` dataset or any other external dataset —
a dataset-dependent test that passes vacuously on an absent dataset is not acceptance evidence.

#### Scenario: the ceiling tests run with no fetched dataset present

- **GIVEN** a checkout with no `CQLITE_DATASETS_ROOT` datasets fetched
- **WHEN** the wide-row egress tests run
- **THEN** they build their own SSTables from `wide_row_fixture` mutations, stream a non-zero number
  of rows, and assert a real ceiling — they neither skip nor pass on an empty result

#### Scenario: the fixture is deterministic across runs

- **WHEN** the wide-row fixture is materialized twice in the same process or across runs
- **THEN** the row content, row count, and schema are identical (fixed payload fill, fixed
  timestamp), so a byte-based ceiling assertion is stable and not timing- or content-dependent
