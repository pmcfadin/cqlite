# flight-admission-control

## ADDED Requirements

### Requirement: do_get concurrency is bounded by a configured admission limit

`FlightService::do_get` SHALL bound the number of concurrently admitted scans to a
configured limit `K` by acquiring an admission permit before opening any SSTable
or producing any batch, and MUST NOT admit more than `K` scans at once regardless
of offered concurrency.

#### Scenario: offering more than K concurrent do_gets holds in-flight at K

- **GIVEN** an admission limit `K` and a deterministic test that holds all `K`
  permits via pre-acquired guards (no wall-clock; concurrency injected, not timed)
- **WHEN** `K + M` `do_get` requests are offered against a live table
- **THEN** the admission in-use gauge never exceeds `K` while the barrier is held
- **AND** every one of the `M` excess requests is blocked from opening an SSTable
  (observed via an SSTable-open / permit-acquire work counter that stays at the
  `K`-admitted level)
- **AND** this test FAILS on pre-change `main`, where offering `K + M` admits all
  `K + M` (no permit exists to bound them)

#### Scenario: releasing a permit admits exactly one waiting request

- **GIVEN** `K` permits held and `M ≥ 1` requests waiting on `acquire`
- **WHEN** exactly one held permit is released
- **THEN** exactly one waiting request is admitted (in-use returns to `K`) and the
  remaining `M − 1` stay waiting — no over-admission, no lost wakeup

### Requirement: sustained overload sheds with a retry-safe status, never RESOURCE_EXHAUSTED

An admission rejection SHALL be returned only after a bounded permit-wait timeout
elapses with no permit available, MUST use gRPC status `UNAVAILABLE` (so the
connector's #2241 replica-failover treats it as retry-safe), MUST NOT use
`RESOURCE_EXHAUSTED`, and MUST occur before any record batch has been delivered
for that request.

#### Scenario: a request that cannot get a permit within the timeout is rejected UNAVAILABLE

- **GIVEN** an admission limit `K`, all `K` permits held by a test barrier, and a
  configured (injectable, non-wall-clock) permit-wait timeout
- **WHEN** one more `do_get` is offered and the injected timeout is advanced past
  the wait deadline while the barrier is still held
- **THEN** that request completes with gRPC status `UNAVAILABLE` (not
  `RESOURCE_EXHAUSTED`, not `OK`) having delivered zero record batches
- **AND** the admission `rejected_total` counter increments by exactly one

#### Scenario: a short burst is absorbed by the wait without rejection

- **GIVEN** an admission limit `K` and all `K` permits held only transiently (a
  single held permit released before the wait deadline)
- **WHEN** one excess `do_get` is offered and waits, then a permit frees before
  the timeout
- **THEN** the request is admitted and returns its complete result with status
  `OK` — no rejection, no error status — and `rejected_total` is unchanged

### Requirement: a cancelled or disconnected do_get releases its permit promptly

A `do_get` that is cancelled, superseded, or whose client disconnects SHALL
release its admission permit within a bounded number of steps via the same
RAII/cancel-aware teardown as the #2361/#2383 machinery, leaving no permit leaked.

#### Scenario: dropping every admitted stream returns the admission gauge to baseline

- **GIVEN** `K` admitted, in-flight `do_get` scans over a multi-batch table
- **WHEN** all `K` client streams are dropped (disconnect) after the first batch
- **THEN** the admission in-use gauge returns to its pre-scan baseline (zero
  leaked permits) and a subsequently offered scan is admitted immediately
- **AND** this is asserted via the gauge/level, not a timed sleep

#### Scenario: a request cancelled while waiting for a permit never holds one

- **GIVEN** all `K` permits held and one request waiting on `acquire`
- **WHEN** that waiting request is cancelled (client disconnect before admission)
- **THEN** it completes with a cancellation/`Aborted`-class outcome, the waiting
  gauge returns to zero, and the in-use gauge is unchanged (it never acquired)

### Requirement: the admission limit is a real, wired configuration knob

The admission limit SHALL be settable via a `--max-concurrent-scans` CLI flag and
an environment variable with a documented conservative default, and setting it to
`K` MUST bound admitted concurrency to `K` end-to-end (the knob is functional, not
decorative).

#### Scenario: a configured limit K bounds admitted concurrency to K

- **GIVEN** a flight server configured with `--max-concurrent-scans = K` for a
  small deterministic `K`
- **WHEN** `K + M` concurrent `do_get` requests are offered
- **THEN** the admission in-use gauge is bounded by exactly the configured `K`
  (not a hard-coded constant), demonstrated for at least two distinct `K` values
  so the value is proven to flow through to the ceiling

#### Scenario: the permit-wait timeout is honoured as configured

- **GIVEN** a configured (injectable) permit-wait timeout value
- **WHEN** a request waits for a permit that never frees
- **THEN** it is rejected after the configured wait budget is exhausted, and a
  different configured budget yields a correspondingly different reject point —
  proving the timeout knob is wired (asserted deterministically, no real sleep)

### Requirement: admission state is exported as observability instruments

The server SHALL export admission observability distinct from the RPC in-flight
gauge: the configured limit, permits in use, requests waiting, a monotonic
rejection counter, and a permit-wait latency histogram — all via the metric
catalog and all scale-free (independent of fixture size).

#### Scenario: admission gauges and counters track engagement deterministically

- **GIVEN** the in-process metric recorder installed and an admission limit `K`
- **WHEN** `K` scans are admitted, `M` are made to wait, and one wait times out
- **THEN** the in-use gauge reads `K`, the waiting gauge reads the current wait
  count, the rejection counter increments by one on the timeout, and the
  permit-wait histogram records a sample — each read as a level/total, not derived
  from fixture row or SSTable counts
- **AND** these instruments are distinct names from `cqlite.rpc.in_flight`
  (no double-counting of the WS2 gauges)

### Requirement: admission does not alter the result of an admitted scan

Admission control SHALL be transparent to result content: a scan that is admitted
(immediately or after waiting) MUST return the byte-identical rows, order, schema,
and batch boundaries it would return with admission disabled/unbounded.

#### Scenario: admitted-after-wait result equals the unbounded result

- **GIVEN** a table and a ticket exercising rows, a limit, and a predicate filter
- **WHEN** the same ticket is executed once through an unbounded path and once
  through the admission path after waiting for a permit
- **THEN** row content, row order, schema, and batch boundaries are identical —
  admission changes *when* a scan runs, never *what* it returns
