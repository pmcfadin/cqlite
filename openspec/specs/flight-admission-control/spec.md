# flight-admission-control Specification

## Purpose
TBD - created by archiving change flight-admission-control. Update Purpose after archive.
## Requirements
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

The admission limit SHALL be settable via a `--max-concurrent-scans` CLI flag and an environment
variable, and setting it to `K` MUST bound admitted concurrency to `K` end-to-end (the knob is
functional, not decorative). **When no explicit value is supplied, the limit SHALL be a value derived
at startup from the parallelism available to the process** (see *The default admission ceiling is
derived from the parallelism available to this process*) rather than a compile-time constant — but
the derived value SHALL NEVER exceed the constant ceiling `DEFAULT_MAX_CONCURRENT_SCANS`, so no
configuration derives a wider ceiling than the pre-#3225 release used.

#### Scenario: a configured limit K bounds admitted concurrency to K

- **GIVEN** a flight server configured with `--max-concurrent-scans = K` for a small deterministic `K`
- **WHEN** `K + M` concurrent `do_get` requests are offered
- **THEN** the admission in-use gauge is bounded by exactly the configured `K` (not a hard-coded
  constant, and not the derived default), demonstrated for at least two distinct `K` values so the
  value is proven to flow through to the ceiling

#### Scenario: the permit-wait timeout is honoured as configured

- **GIVEN** a configured (injectable) permit-wait timeout value
- **WHEN** a request waits for a permit that never frees
- **THEN** it is rejected after the configured wait budget is exhausted, and a different configured
  budget yields a correspondingly different reject point — proving the timeout knob is wired
  (asserted deterministically, no real sleep)

#### Scenario: the derived default never exceeds the pre-existing ceiling

- **GIVEN** any value of available parallelism, including one larger than any real machine
- **WHEN** the default is derived with no explicit configuration
- **THEN** the result is less than or equal to `DEFAULT_MAX_CONCURRENT_SCANS` (64), so the change is
  one-directional and no host is admitted more widely than it is on the current release

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

### Requirement: The default admission ceiling is derived from the parallelism available to this process

With no explicit `--max-concurrent-scans` and no `CQLITE_MAX_CONCURRENT_SCANS`, the effective
admission ceiling SHALL be `clamp(DERIVED_SCANS_PER_HARDWARE_THREAD × P, MIN, MAX)` where `P` is the
number of hardware threads available to this process, `DERIVED_SCANS_PER_HARDWARE_THREAD = 2`,
`MIN = 2` and `MAX = DEFAULT_MAX_CONCURRENT_SCANS = 64`. The derivation SHALL be a pure function of
`P`, separable from the act of probing `P`, so it is exhaustively testable without a machine of the
width under test. The formula SHALL be monotone non-decreasing in `P`.

The formula's warrant is #3217's measured peak-throughput concurrency at four server widths
(`docs/reports/ws0-3217-artifacts/results/partA-analysis.json`, report §3.1), converted to hardware
threads on the SMT-on measurement rig (`P = 2 × physical cores`, report §2.2): P=2 → measured peak 2,
P=4 → 8, P=8 → 16 (censored at the top of the ramp), P=12 → ≥16 (censored). The formula's deviation
at each measured width, including the −4.8% at P=2 and the unmeasured P=12 extrapolation, is recorded
in `design.md` D2 and is not to be presented as a clean four-point fit.

#### Scenario: the derived value is computed at documented widths

- **GIVEN** the pure derivation function
- **WHEN** it is evaluated at `P ∈ {1, 2, 3, 4, 6, 8, 12, 16, 24, 31, 32, 33, 64, 1024}`
- **THEN** it returns `{2, 4, 6, 8, 12, 16, 24, 32, 48, 62, 64, 64, 64, 64}` respectively — the floor
  binds at `P = 1`, the ceiling binds from `P = 32` upward, and the sequence is monotone
  non-decreasing

#### Scenario: the derived default reproduces the measured peak at the two uncensored widths

- **GIVEN** the measured peak concurrency at 2 physical cores (N=8) and 4 physical cores (N=16) from
  #3217, expressed in hardware threads as P=4 and P=8
- **WHEN** the derivation is evaluated at those `P`
- **THEN** it returns exactly 8 and exactly 16 — the formula is pinned to the measurement, so a
  future coefficient change that breaks the fit fails a test rather than a review

#### Scenario: an unavailable parallelism oracle yields the previous constant, distinguishably

- **GIVEN** a platform on which `std::thread::available_parallelism()` returns `Err`
- **WHEN** no explicit configuration is supplied
- **THEN** the effective ceiling is `DEFAULT_MAX_CONCURRENT_SCANS` (64), the pre-#3225 behaviour, and
  the provenance is reported as `derived-fallback` — never as `derived`, because the oracle that
  would justify a derived value could not be consulted

### Requirement: The derived default never reads host topology

The probe for `P` SHALL report the parallelism available to **this process**, honouring the CPU
affinity mask (`sched_getaffinity`) and the cgroup v1/v2 CPU quota. A container or a
`taskset`-restricted process MUST NOT derive its ceiling from the host's CPU count. The chosen API is
`std::thread::available_parallelism()`, matching the existing repo precedent at
`cqlite-core/src/storage/sstable/reader/scan_admission.rs:169`.

`num_cpus::get_physical()` SHALL NOT be used: it parses `/proc/cpuinfo` and sums `cpu cores` per
`physical id` (`num_cpus-1.17.0/src/linux.rs:59-97`), applying neither the cgroup quota nor the
affinity mask, so inside a container it returns the host's core count — the exact failure this
requirement exists to prevent. Neither `/proc/cpuinfo` nor `/sys/devices/system/cpu/**` SHALL be read
on the derivation path.

#### Scenario: a restricted affinity mask lowers the derived ceiling

- **GIVEN** a host with more hardware threads than the process is permitted to run on
- **WHEN** the server is started under a restricted CPU affinity mask and no explicit ceiling
- **THEN** the logged `available_parallelism` equals the number of CPUs in the mask, not the host's
  count, and the effective ceiling is the formula applied to that number

#### Scenario: the derivation path cannot be re-pointed at host topology unnoticed

- **GIVEN** the source of the admission-default derivation path
- **WHEN** it is inspected structurally
- **THEN** it contains no `/proc/cpuinfo` read, no `/sys/devices/system/cpu` read and no
  `num_cpus::get_physical()` call — asserted in-repo, because the failure mode is a future edit on a
  machine where no container is available to catch it behaviourally

#### Scenario: a sub-CPU cgroup quota still yields a usable ceiling

- **GIVEN** a process whose available parallelism resolves to 1 (a hard one-CPU quota or mask)
- **WHEN** the default is derived
- **THEN** the effective ceiling is 2, never 1 and never 0 — a single-permit server serialises every
  scan, and #3217 measured N=1 as the worst point at every width

### Requirement: An explicit admission ceiling always overrides the derived default

An explicitly configured `--max-concurrent-scans` (flag) or `CQLITE_MAX_CONCURRENT_SCANS`
(environment) SHALL take precedence over the derived default, in that order, and SHALL NOT be clamped
toward it. The only bound applied to an explicit value is #2420's pre-existing
`[1, Semaphore::MAX_PERMITS]` clamp, unchanged. Configuring the previous constant explicitly SHALL
reproduce the previous behaviour exactly on any host.

#### Scenario: the flag wins over both the environment and the derived value

- **GIVEN** a host whose derived default is some value `D`, and `CQLITE_MAX_CONCURRENT_SCANS = E`
  with `E ≠ D`
- **WHEN** the server is started with `--max-concurrent-scans = F`, `F ∉ {D, E}`
- **THEN** the effective ceiling is `F`, exercised through the real CLI parser rather than a
  hand-constructed config

#### Scenario: the environment wins over the derived value

- **GIVEN** a host whose derived default is `D` and no `--max-concurrent-scans` flag
- **WHEN** `CQLITE_MAX_CONCURRENT_SCANS = E` with `E ≠ D` is set
- **THEN** the effective ceiling is `E`

#### Scenario: an explicit value above the derived default is honoured, not clamped down

- **GIVEN** a narrow host whose derived default is small
- **WHEN** an explicit ceiling well above the derived value (but within the #2420 clamp) is
  configured
- **THEN** the effective ceiling is the explicit value — the derived default is a default, not a cap

#### Scenario: the previous default is restorable with one setting

- **GIVEN** any host
- **WHEN** the server is started with `--max-concurrent-scans 64`
- **THEN** the effective admission ceiling is 64, identical to the pre-#3225 release on that host

### Requirement: The effective ceiling and its provenance are logged at startup

The startup log event that already reports `max_concurrent_scans` SHALL additionally report the
provenance of that value and the parallelism reading it was derived from, so an operator can
distinguish a derived ceiling from a configured one from the log alone. Provenance SHALL be one of
exactly four values — `flag`, `env`, `derived`, `derived-fallback` — with `derived-fallback` reserved
for the case where the parallelism oracle returned no answer. No new log event is added; the fields
join the existing `cqlite-flight starting` event.

#### Scenario: a derived ceiling is labelled as derived, with its input

- **GIVEN** a server started with neither the flag nor the environment variable
- **WHEN** it logs its startup event
- **THEN** the event carries the effective `max_concurrent_scans`, `max_concurrent_scans_source =
  "derived"`, and `available_parallelism` equal to the `P` actually read

#### Scenario: each provenance value is produced by its own input

- **GIVEN** four startups — flag set; environment set with no flag; neither set with the oracle
  answering; neither set with the oracle returning `Err`
- **WHEN** each logs its startup event
- **THEN** the reported sources are `flag`, `env`, `derived`, `derived-fallback` respectively, each
  distinguishable from the others, so a configured value that coincidentally equals the derived one
  is still identifiable as configured

#### Scenario: the logged value is the effective post-clamp ceiling

- **GIVEN** an explicit ceiling that the #2420 `[1, Semaphore::MAX_PERMITS]` clamp adjusts
- **WHEN** the startup event is logged
- **THEN** `max_concurrent_scans` reports the value the semaphore was actually constructed with, not
  the requested value — preserving the existing `main.rs:162` behaviour of logging
  `admission.limit()`

