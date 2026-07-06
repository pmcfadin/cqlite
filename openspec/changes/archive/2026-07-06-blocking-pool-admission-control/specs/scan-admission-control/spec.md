# scan-admission-control

## ADDED Requirements

### Requirement: Concurrent windowed scans admitted to the blocking pool are bounded

The windowed streaming scan SHALL bound the number of scans admitted to tokio's
blocking pool concurrently by a process-wide admission limit. Each scan SHALL
acquire exactly one admission permit BEFORE spawning any `spawn_blocking` work
(its parse task and, for a synchronously-faulting backend, its raw-chunk feed
task), so at most `limit` scans hold blocking-pool threads at once. Because a
faulting-backend scan holds two blocking threads per admitted permit, `limit`
admitted scans SHALL use at most `2 × limit` blocking threads, leaving headroom in
the shared pool for latency-critical fs/point-read operations. The default limit
SHALL be derived from `available_parallelism` (documented), not a hard-coded
magic number, and SHALL be at least 1.

#### Scenario: Concurrent scans never exceed the admission limit

- **GIVEN** the admission limit is set to a small value `L` and a present
  multi-chunk fixture that returns live rows
- **WHEN** `N > L` full windowed scans run concurrently over the fixture with the
  `scan-offload-probe` in-flight instrumentation armed
- **THEN** the recorded maximum number of concurrently-admitted scans is at most
  `L` (never `N`), at least one scan was admitted (the admission path is wired and
  non-vacuous), and every scan returns its rows unchanged.

### Requirement: A scan over the admission limit waits and then proceeds

When the admission limit is reached, an additional windowed scan SHALL WAIT on the
admission semaphore (natural backpressure) rather than return an error, and SHALL
proceed as soon as an admitted scan releases its permit. A scan awaiting admission
SHALL NOT hold any admission permit or lock while waiting (there is a single
admission point and each scan takes exactly one permit once), so admission cannot
deadlock.

#### Scenario: The limit+1-th acquisition blocks until a permit frees

- **GIVEN** an admission semaphore with `L` permits and `L` permits already held
- **WHEN** an additional admission is attempted
- **THEN** the additional admission does not complete while all `L` permits are
  held, and it completes as soon as one held permit is released — the scan queues
  rather than erroring.

### Requirement: Admission permits release on every scan exit path

The admission permit SHALL be released — returning the slot to the semaphore — when
the scan completes successfully, completes with an error, OR is cancelled/dropped,
via RAII `Drop`. Releasing SHALL NOT panic. Over any sequence of admissions and
releases (including drop-before-completion), the number of available permits SHALL
return to the full limit, so a scan can never leak an admission slot.

#### Scenario: Repeated acquire-and-drop cycles leak no permits

- **GIVEN** an admission semaphore with `L` permits
- **WHEN** a permit is acquired and then dropped many times in succession
  (including dropping before any scan work would complete)
- **THEN** after each drop the permit returns to the semaphore, and after all
  cycles the full `L` permits are again available — no slot is leaked.

### Requirement: Admission control preserves scan results exactly

Bounding concurrent scan admission SHALL NOT change any scan output. A windowed
streaming scan run under the admission limit SHALL emit the byte-identical row set,
order, and tombstone filtering as the same scan run without contention; admission
is a scheduling change only. Admission control SHALL NOT introduce a shipped
production configuration knob (the only limit override is a test-only surface
compiled under the non-default `scan-offload-probe` feature), and SHALL NOT panic
or make a scan un-runnable if the admission mechanism is unavailable (fail-open:
proceed without a permit).

#### Scenario: Results are unchanged under admission

- **WHEN** a full windowed scan runs while other scans contend for admission
  permits
- **THEN** it returns the same rows in the same order as an uncontended scan of the
  same table, and no admission-related error is surfaced to the caller.

### Requirement: The admission wiring is exercised by the agent gate

The deterministic admission-bound guard SHALL be executed by
`scripts/agent-gate.sh` (added as a `--test` target of the existing scan-offload
guard component, which already enables the `scan-offload-probe` + `cli-helpers`
features), so the wiring guard actually runs pre-merge rather than existing only as
an un-invoked test.

#### Scenario: The admission guard runs under the gate

- **WHEN** `scripts/agent-gate.sh` executes
- **THEN** the F4 admission-bound guard test is among the tests it runs (built with
  the `scan-offload-probe` + `cli-helpers` features it requires), and a regression
  that removes the permit acquisition (so concurrent scans exceed the limit) fails
  the gate.
