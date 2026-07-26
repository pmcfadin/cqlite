# flight-streaming-egress — delta for streaming-egress-byte-budget (issue #2821)

## MODIFIED Requirements

### Requirement: peak resident payload is bounded, independent of result size

The `do_get` streaming path SHALL bound peak resident record-batch residency **in bytes**,
independent of the total result size **and independent of row width**, applying backpressure to the
merge when the consumer is slow.

The governed quantity is each batch's `RecordBatch::get_array_memory_size()` — Arrow buffer
**CAPACITY** bytes, the quantity `MeteredDoGetStream::poll_next` already meters for metrics. Credit
for that capacity SHALL be **reserved before the batch is materialized** (see the
reserve-before-materialize requirement), trued up to the realized capacity once it exists, and
released only when the batch has left the stream toward the client. No materialized-but-uncharged
`RecordBatch` SHALL exist on the egress path. The guaranteed contract is:

> peak charged in-flight egress **capacity** ≤ **`max(ceiling, one maximum batch)`**

The `+ one maximum batch` additive term of the pre-reservation design is GONE: it existed only
because a producer could hold a materialized, uncharged batch while parked. The remaining
`max(...)` is the deadlock-avoidance clamp — an oversized batch may take the whole pool and is
resident at its own size — see the deadlock-avoidance requirement, which also names every residency
term that remains outside the governed set.

This byte ceiling SHALL compose with, and SHALL NOT replace, the existing `DO_GET_CHANNEL_CAPACITY`
batch-count channel; whichever bound is reached first governs.

#### Scenario: slow consumer bounds in-flight egress bytes on a wide-row fixture

- **GIVEN** the synthetic wide-row fixture (`cqlite-flight/src/wide_row_fixture.rs`, merged with
  issue #2825), whose per-batch capacity is large enough that the byte ceiling binds before the
  4-deep batch-count channel does
- **WHEN** a client reads ONE batch and then stops polling
- **THEN** the observed peak charged in-flight capacity bytes never exceed
  `max(configured ceiling, largest single batch capacity observed on that stream)`
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

The per-stream ceiling SHALL be denominated in capacity bytes. Any conversion of a payload figure
into capacity — the reservation amount, the composition against B4, any documented bound — SHALL go
through `cqlite_flight::batch_bytes::worst_case_batch_capacity_bytes` /
`BATCH_BYTES_CAPACITY_FACTOR`, NEVER through a locally re-derived factor and NEVER by comparing a
payload figure with a capacity figure directly. Code and docs SHALL NOT state a composition that
adds `DEFAULT_MAX_BATCH_BYTES` (payload) to the ceiling (capacity) as if the two were the same unit.

A conversion SHALL NOT be written as a bare `payload × BATCH_BYTES_CAPACITY_FACTOR`: the published
worst case also carries `BATCH_BYTES_PER_COLUMN_SLACK × n_array_nodes` of fixed per-array-node
allocation, so the factor alone UNDER-states capacity. `n_array_nodes` SHALL be counted as Arrow
array NODES over the projected output schema (a `list<text>` column is two, a `map<text,text>`
column is four), computed once per merge, not as a column count.

#### Scenario: the composition is asserted from the published constants, not hard-coded

- **GIVEN** `DEFAULT_MAX_INFLIGHT_EGRESS_BYTES` and `cqlite_flight::batch_bytes`'s
  `DEFAULT_MAX_BATCH_BYTES` / `BATCH_BYTES_CAPACITY_FACTOR` / `worst_case_batch_capacity_bytes`
- **WHEN** a test computes `max(DEFAULT_MAX_INFLIGHT_EGRESS_BYTES, worst_case_batch_capacity_bytes(
  DEFAULT_MAX_BATCH_BYTES, n_array_nodes, 0))` for a small flat schema
- **THEN** the result is ≤ 16 MiB (the ratified B4 per-query working set at concurrency 1), so a
  later change to EITHER constant that breaks the composition fails this test rather than silently
  invalidating the doctrine

#### Scenario: array nodes are counted, not columns

- **GIVEN** a projected output schema containing a `map<text,text>` column
- **WHEN** the reservation's slack term is computed
- **THEN** that column contributes four array nodes, not one, so the reservation is not
  under-stated for nested schemas

#### Scenario: no locally re-derived payload→capacity factor exists

- **WHEN** the change's code and doc comments are read
- **THEN** the payload→capacity conversion appears only as a use of
  `BATCH_BYTES_CAPACITY_FACTOR` / `worst_case_batch_capacity_bytes`, with no second definition of
  the factor `2` and no undocumented fudge

### Requirement: the published payload→capacity conversion holds for TINY batches

Making the conversion an ENFORCED, fail-closed reservation means it SHALL be a true upper bound in
the regime where the fixed per-array-node allocations — not the `BATCH_BYTES_CAPACITY_FACTOR`
growth term — dominate `get_array_memory_size()`. `BATCH_BYTES_PER_COLUMN_SLACK` SHALL therefore
cover the largest fixed allocation an Arrow array built by `export::arrow_convert` carries at ANY
length (measured 1208 B for a `Utf8`/`Binary` node against arrow 53: the string builder's 1 KiB
default values buffer plus offsets and struct overhead). A conversion that holds only for
payload-dominated batches is not a bound: under this change it turns every narrow-table `do_get`
into a terminal internal error.

The correction SHALL be made to the published constant, never as a local allowance at the
reservation site (see the no-second-definition requirement above).

#### Scenario: the bound holds where the fixed per-node cost dominates

- **GIVEN** batches whose payload is small relative to their array count — two `text` columns of
  three short rows, a single empty string, `text`+`blob`+`int`, `list<text>`, `map<text,text>`, and
  an all-null row over a variable-width schema
- **WHEN** each is materialized through the real converter and compared with
  `worst_case_batch_capacity_bytes(Σ estimate_arrow_row_bytes, n_array_nodes, 0)` — exactly the
  quantity the reservation computes
- **THEN** the realized `get_array_memory_size()` is within the bound for every shape
- **AND** each shape is asserted to be fixed-cost-dominated (capacity > 2 × estimate), so the test
  cannot go vacuous by drifting into the payload-dominated regime

### Requirement: egress credit is reserved BEFORE a batch is materialized

The producer SHALL acquire the batch's capacity credit at the batch boundary, BEFORE
`rows_to_record_batch` allocates anything, and SHALL park awaiting credit with only the row buffer
resident. A `RecordBatch` SHALL NEVER exist on the egress path without credit already held for it.

The reservation amount SHALL be derived from the payload estimate the producer already maintains
for the rows about to be materialized (#2825's `BatchByteCap` running accumulator, itself the sum of
`estimate_arrow_row_bytes`), converted to capacity with `worst_case_batch_capacity_bytes(estimate,
n_array_nodes, 0)`. No second estimator SHALL be introduced.

This SHALL apply at EVERY streaming build site — both the partition-at-a-time merge loop
(`drive_merge`) and the row-granular loop (`drive_merge_streaming`), at all three of their flush
points (the byte-cap cut, the row-cap cut, and the end-of-merge tail). The reserve → build →
true-up → emit sequence SHALL be structured so a build site cannot materialize without a
reservation (a single owning helper, or a reservation value `emit` consumes) rather than left as an
unenforced calling convention. The collect/parity sink SHALL be unaffected (its reservation is a
no-op), so the collect path stays byte-identical.

Parking on credit SHALL race the shared cancel flag in the same biased `select!` that the channel
reservation already uses, so a client disconnect wakes a producer parked on credit exactly as it
wakes one parked on a full channel, and pins no blocking-pool thread.

#### Scenario: no materialized batch is ever uncharged

- **GIVEN** a stream whose pool is exhausted by batches already in flight
- **WHEN** the producer reaches the next batch boundary
- **THEN** it parks BEFORE building the batch, with only the row buffer resident, and the observed
  charged in-flight capacity plus zero uncharged batches accounts for every `RecordBatch` alive on
  the egress path

#### Scenario: both producer loops reserve

- **WHEN** the same ceiling test is driven through the partition-at-a-time merge loop and through
  the row-granular streaming loop
- **THEN** both observe the bound, because both reserve at all of their flush points — a governor
  wired into only one loop would leave the other unbounded

#### Scenario: a producer parked on a pre-materialization reservation wakes on cancellation

- **GIVEN** a producer parked awaiting credit before building a batch
- **WHEN** the shared cancel flag trips
- **THEN** it stops promptly, materializes nothing, and pins no blocking-pool thread

### Requirement: the reservation is trued up DOWNWARD after materialization, never upward

Because the estimator is deliberately conservative, the reservation will usually exceed the realized
`get_array_memory_size()`. Immediately after materialization the governor SHALL measure the real
capacity and RELEASE the excess (`reserved − actual`), so over-reservation lasts only for the
materialization window and does not starve the pool or serialize the stream.

The governor SHALL NEVER true up upward. If the realized capacity exceeds the reservation, a bound
this change guarantees has been violated: the governor SHALL fail closed — surface a terminal
internal error naming the violated invariant, drop the permit on the normal path, and NOT emit the
batch while silently exceeding the pool. Overshoot SHALL NOT be absorbed by quietly acquiring more
credit (which can deadlock) or by ignoring it.

#### Scenario: an over-reserved batch returns its excess immediately

- **GIVEN** a shape whose payload estimate materially over-states the realized capacity
- **WHEN** a batch is built under a reservation
- **THEN** the charged credit after materialization equals the realized capacity (within the
  KiB rounding quantum), not the reservation, and the pool admits further batches accordingly

#### Scenario: an under-reservation fails closed rather than exceeding the pool

- **GIVEN** an injected/simulated batch whose realized capacity exceeds its reservation
- **WHEN** the true-up runs
- **THEN** the stream terminates with an internal error identifying the violated
  estimate-conservatism invariant, no credit is leaked, and the pool is fully released
- **AND** this is proven through the RESPONSE STREAM (the error raised at a real producer batch
  boundary reaches the encoded `do_get` stream as `Status::internal`), not only on the credit
  helper

#### Scenario: a credit pool that cannot charge fails closed rather than reserving uncharged

- **GIVEN** a per-stream credit pool that cannot grant a reservation (a closed semaphore)
- **WHEN** the producer reserves before materializing
- **THEN** the reservation surfaces a terminal internal error, no reservation is recorded as
  granted, and no batch is placed on the egress path under an UNCHARGED reservation — the memory
  bound is never degraded silently in exchange for continuing to stream

### Requirement: the bound depends on #2825's estimator-conservatism contract

This change's memory bound now rests on a contract owned by another module, and that dependency
SHALL be named as a cross-issue invariant at both ends rather than left implicit:

> `Σ estimate_arrow_row_bytes(columns, row) >= arrow_payload_bytes(batch)` (issue #2825,
> `cqlite-core/src/export/arrow_size.rs`, property-tested) **and** realized capacity
> `<= worst_case_batch_capacity_bytes(payload, n_array_nodes, 0)`.

If either leg is weakened, this change's ceiling silently under-reserves. The reservation site SHALL
carry a comment naming the invariant and pointing at the property test that enforces it, and
`arrow_size.rs`'s conservatism section SHALL name this ceiling as a dependent consumer, so a future
change to the estimator cannot be made without meeting the dependency.

#### Scenario: the invariant is documented at both ends

- **WHEN** the reservation site and `arrow_size.rs`'s conservatism section are read
- **THEN** each names the other, and the reservation site points at the property test that enforces
  `Σ estimate >= payload`

### Requirement: the byte ceiling never deadlocks, and the honest bound is stated

A single record batch MAY be larger than the entire configured ceiling. The egress governor SHALL
therefore always admit at least one batch when zero bytes are in flight, by clamping a reservation
to the pool total. No batch of any size SHALL be able to wedge a stream.

Because a clamped batch is charged at most the whole ceiling while resident at its own size, the
guaranteed bound is `max(ceiling, one maximum batch)`. The code and its doc comments SHALL state
this bound honestly. In particular they SHALL state WHEN the clamp still engages at the shipped
default rather than implying it is unreachable: `permits_for(2 × cap + 2 KiB × n_array_nodes)
= 8192 + 2 × n_array_nodes` against a 12288-permit pool, so a projection of **2049 or more Arrow
array nodes** (or a row wider than the ceiling, or an operator-configured small ceiling) still clamps —
and the documented lock-step behaviour then applies. They SHALL likewise NOT claim a guaranteed
number of concurrently admitted batches: whether a second full-size reservation fits after the
true-down depends on the shape's realized capacity/payload factor.

The doc comments SHALL also name the residency that remains OUTSIDE the governed set rather than
let a reader assume the ceiling covers everything:

1. **The producer's row buffer** (`Vec<QueryRow>`, up to `batch_size` rows or one byte-cap's worth of
   payload, plus per-value Rust overhead) is resident while rows accumulate, while the producer is
   parked on a reservation, and during materialization (buffer and batch overlap until the buffer is
   cleared). It is not a `RecordBatch` and is not metered by `get_array_memory_size()`. This term is
   PRE-EXISTING and unchanged by this change, but it is un-governed and SHALL be documented.
2. **A single row wider than #2825's per-batch cap**, which leaves as a one-row batch at its own
   natural width (`worst_case_batch_capacity_bytes`'s `max(cap, widest_row_payload)` term) — a
   property of the data, not slack in the mechanism.
3. **The aggregate route** (`aggregate_paths`), which materializes its partial batches into a `Vec`
   and does not pass through the credit governor at all. Bounded by group count by construction; an
   explicit non-goal of this change and SHALL be stated as such, not implied to be covered.

#### Scenario: a batch larger than the whole ceiling is still delivered

- **GIVEN** a stream configured with a byte ceiling smaller than a single batch of the wide-row
  fixture
- **WHEN** the client consumes the stream
- **THEN** every batch is delivered and the stream terminates normally
- **AND** a naive non-clamping implementation hangs on this scenario (the test is the guard)

#### Scenario: the shipped default admits one worst-case reservation, and the clamp boundary is pinned

- **GIVEN** a credit pool at `DEFAULT_MAX_INFLIGHT_EGRESS_BYTES` and a reservation of
  `worst_case_batch_capacity_bytes(DEFAULT_MAX_BATCH_BYTES, n_array_nodes, 0)`
- **WHEN** the reservation is taken at the documented no-clamp width and one array node past it
- **THEN** no clamp occurs at or below the documented width, the clamp DOES occur one node past it,
  and at that point the documented behaviour holds — the batch is still admitted, it holds the
  ENTIRE pool, and nothing else can be admitted beside it

#### Scenario: the stated bound matches the enforced bound

- **WHEN** the wide-row ceiling test measures peak charged in-flight capacity against
  `max(ceiling, max observed batch capacity)`
- **THEN** the assertion passes, and the same derivation appears in the governor's doc comment so
  the documented contract and the tested bound cannot drift apart

#### Scenario: the un-governed residency terms are named, not hidden

- **WHEN** a reader consults the governor's doc comment
- **THEN** it states the three terms above explicitly (the row buffer, an over-cap single row, the
  aggregate route), and does NOT list a parked pre-credit batch — reserve-before-materialize
  eliminated that term rather than documenting it

### Requirement: credit release is deferred by one batch so the encoder prefetch stays inside the bound

`MeteredDoGetStream` is constructed UPSTREAM of the Flight encoder (`encode_do_get(metered, …)`),
and `FlightDataEncoderBuilder`'s stream can pull one batch ahead of yielding it. Releasing a batch's
credit at the instant `poll_next` yields it would therefore leave that batch resident with its
credit already returned — reintroducing exactly the class of un-charged resident batch that
reserve-before-materialize eliminated on the producer side, and making the true bound
`max(ceiling, one maximum batch) + one maximum batch`.

The stream SHALL therefore hold each yielded batch's permit in a deferred slot and release it only
once the batch's Arrow data is no longer referenced downstream, observed at the TOP of a later
`poll_next` (before the inner stream is polled) and unconditionally on `Drop` and on every terminal
arm. At most one batch is downstream of the credit boundary at a time in production, and it is still
charged, which is what makes `max(ceiling, one maximum batch)` true as stated.

**The release point is constrained from BOTH sides, and SHALL NOT rest on the consumer's polling
discipline:**

- *Correctness.* `FlightDataEncoder::poll_next` (arrow-flight 53.4.1, `encode.rs:400-436`) polls its
  inner stream ONLY when its `FlightData` queue is empty — that is, after `encode_batch` has
  consumed and dropped the previous `RecordBatch`. Releasing at the top of the next poll therefore
  releases at the first instant the previous batch is provably gone, strictly TIGHTER than releasing
  when the next batch is yielded. But `MeteredDoGetStream` is `pub(crate)` and polled directly, and
  an unconditional release at the top of `poll_next` would return credit for a batch a speculative
  poller (a `select!` arm, `futures::poll!`) is still holding. The release SHALL therefore be keyed
  on the batch data's own liveness, so a consumer that still holds a yielded batch keeps paying for
  it — including across a poll that returns `Pending`.
- *Liveness.* Releasing only when the next batch is YIELDED deadlocks any stream whose pool is one
  batch deep: the deferred permit holds the whole pool, the producer parks reserving the next batch,
  and the consumer waits for that batch. At the merged defaults a resident batch plus a worst-case
  reservation already exceed the ceiling, so that cycle is reachable in the DEFAULT configuration,
  not merely a corner case. A test SHALL cover this (a ceiling smaller than one batch, driven end to
  end): the release-on-yield variant hangs on it. Keying the release on data liveness preserves
  liveness for the production encoder, which has dropped the batch before it re-polls.

#### Scenario: the yielded batch's credit is still charged while the encoder holds it

- **GIVEN** a stream whose consumer has taken exactly one batch and has not polled again
- **WHEN** the charged in-flight total is observed
- **THEN** it still includes the just-yielded batch's capacity — the permit has not been released —
  and it is released on a later poll, once the consumer has dropped the batch

#### Scenario: a speculative Pending poll does not release a held batch's credit

- **GIVEN** a consumer that still holds a yielded batch and polls the stream again while the inner
  stream has nothing ready (the poll returns `Pending`)
- **THEN** that batch's credit remains charged across the `Pending` return
- **AND** once the consumer drops the batch, the next poll returns the credit, so a producer parked
  on the pool is not wedged

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
variable and a `DEFAULT_MAX_INFLIGHT_EGRESS_BYTES` constant denominated in **CAPACITY bytes**, set
to **12 MiB** (design A / D4a as corrected in review). The default SHALL be at least one worst-case
RESERVATION — `worst_case_batch_capacity_bytes(DEFAULT_MAX_BATCH_BYTES, n_array_nodes, 0)` — not
merely at or below one maximum batch: admission is gated on the pre-materialization reservation, so
a smaller ceiling makes the deadlock clamp fire on EVERY byte-cap-cut batch and runs the stream
lock-step on exactly the wide-row workloads this change exists for (at 8 MiB: 8198 permits wanted
against 8192 held). The composition test above SHALL hold at that value. Plumbing mirrors the merged `--max-batch-bytes` / `CQLITE_MAX_BATCH_BYTES` / `DEFAULT_MAX_BATCH_BYTES`
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

#### Scenario: the default ceiling composes inside B4

- **WHEN** neither the flag nor the environment variable is set
- **THEN** the ceiling is `DEFAULT_MAX_INFLIGHT_EGRESS_BYTES`, denominated in capacity bytes
- **AND** that constant is **12 MiB**, so composing it with #2825's merged 4 MiB payload cap through
  `worst_case_batch_capacity_bytes` gives `max(12 MiB, 2 × 4 MiB + slack) = 12 MiB` of capacity for
  the guaranteed bound — inside the ratified **B4 ≤16Mi per-query working set at concurrency 1**
  with 4 MiB of headroom
- **AND** a worst-case reservation at that default is admitted WITHOUT clamping (a test asserts the
  clamp counter is zero for `worst_case_batch_capacity_bytes(DEFAULT_MAX_BATCH_BYTES, 3, 0)`), so
  the deadlock clamp is not the normal case

#### Scenario: an embedder can opt out to an unbounded budget

- **GIVEN** a library caller that constructs the service with an explicitly unbounded egress budget
- **WHEN** it runs a streaming `do_get`
- **THEN** no byte ceiling is applied and residency reverts to the pre-change structural bound

### Requirement: the byte ceiling composes with admission K

The per-stream byte ceiling SHALL NOT change admission `K`, its default, or its shedding policy. The
two governors SHALL remain independent: `K` bounds concurrently admitted scans server-wide, the byte
ceiling bounds capacity in flight within one stream. The documented server-wide worst case SHALL be
`K × max(per-stream ceiling, one maximum batch)` — `K × 8 MiB` at the merged batch cap for any
ceiling ≤ 8 MiB.

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
egress residency as still count-bounded and the 14 MiB additive composition as a TARGET for this
issue. This
change makes those statements false, and SHALL update them in the same change that makes them false:

- `cqlite-flight/src/batch_bytes.rs`'s module documentation SHALL no longer state that `do_get` is
  count-bounded at `~7 × 8 MiB ≈ 56 MiB` per stream, nor that the composition "becomes true only
  once #2821 lands" / "is a TARGET for the dependent issue". It SHALL instead state the enforced
  per-stream capacity ceiling and its bound `max(ceiling, one maximum batch)`, name the default, and
  RETAIN the payload-vs-capacity currency explanation and the published-constant conversion, which
  remain correct. Its `6 + 8 = 14 MiB` sketch SHALL be replaced by the delivered arithmetic (the
  additive term does not exist under reserve-before-materialize), and
  `DEFAULT_MAX_BATCH_BYTES` SHALL NOT change.
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
- **THEN** it states the enforced composition `max(ceiling, one maximum batch) ≈ 8 MiB` (not the
  prospective `6 + 8 = 14 MiB` additive sketch), records that reserve-before-materialize is what
  removed the additive term, keeps the payload-vs-capacity correction that motivates the currency,
  and points at this issue as the delivery

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
