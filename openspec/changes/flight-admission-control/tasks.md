# Tasks — flight-admission-control (issue #2420, WS4)

## 1. TDD guards (fail on main first)

- [x] 1.1 Bounded-admission test: offer `K + M` concurrent `do_get`s with all `K`
      permits held by a test barrier (injected concurrency, no wall-clock); assert
      in-use gauge ≤ `K` and the `M` excess never open an SSTable. FAILS on main
      (no permit exists).
- [x] 1.2 Timeout-reject test: with the barrier held and an injectable permit-wait
      timeout advanced past the deadline, assert the excess request returns
      `UNAVAILABLE` (never `RESOURCE_EXHAUSTED`), zero batches delivered,
      `rejected_total += 1`. FAILS on main.
- [x] 1.3 Burst-absorb test: release a held permit before the timeout; assert the
      waiter is admitted with `OK` and `rejected_total` unchanged. FAILS on main.
- [x] 1.4 Permit-leak test: admit `K`, drop all `K` streams; assert the in-use
      gauge returns to baseline and a new scan is admitted. FAILS on main.
- [x] 1.5 Knob test: configure two distinct `K` values via `--max-concurrent-scans`;
      assert the ceiling equals each configured `K` (proves non-decorative). FAILS
      on main.

## 2. Admission primitive (`cqlite-flight`)

- [x] 2.1 Add an `Admission` type wrapping an `Arc<Semaphore>` (capacity `K`) and
      the configured permit-wait timeout; `try_acquire_with_timeout` returns an
      `OwnedSemaphorePermit`-holding guard or an `UNAVAILABLE` `Status` on timeout.
- [x] 2.2 Increment/decrement the admission gauges (in-use, waiting) around
      acquire/release; increment `rejected_total` + record the permit-wait
      histogram on the reject and admit paths. Use catalog constants.

## 3. Service wiring (`cqlite-flight/src/service.rs`, `streaming.rs`)

- [x] 3.1 Acquire the admission permit at the top of `do_get_inner`, before
      `do_get_setup` opens anything; on timeout return the `UNAVAILABLE` status
      through the existing error path (records the RPC error + closes the span).
- [x] 3.2 Move the `OwnedSemaphorePermit` guard into the metered stream / merge
      task alongside the existing `CancelGuard`, so every exit path (completion,
      setup error, disconnect, cancel) drops it — one lifetime, zero leak.
- [x] 3.3 Aggregate and row paths both gated by the same permit.

## 4. Configuration (`cqlite-flight/src/main.rs`)

- [x] 4.1 Add `--max-concurrent-scans` (CLI + env) with a documented conservative
      default and a `--admission-wait-timeout` (or equivalent) knob; thread them
      into `CqliteFlightService::new`.
- [x] 4.2 Add a coarse tonic `max_concurrent_streams` transport backstop (well
      above `K`) on `Server::builder()`.

## 5. Observability (`cqlite-flight/src/obs.rs`, `cqlite-core` catalog)

- [x] 5.1 Add the five catalog instruments (limit, in_use, waiting,
      rejected_total, wait_seconds); confirm distinct names from
      `cqlite.rpc.in_flight`.

## 6. Docs

- [x] 6.1 Document `--max-concurrent-scans` + the overload contract (queue-then-
      `UNAVAILABLE`, connector fails over) in the flight/ops docs and update the
      WS4 research-section pointer.

## 7. Endgame

- [x] 7.1 `--lite` green each fix round (summary-file redirect); blast-radius
      tests for `cqlite-flight`.
- [ ] 7.2 rust-reviewer + roborev on the lite-green diff; address blockers.
- [ ] 7.3 Open PR; flow-closer runs the FULL gate ONCE → spec-auditor (C) → final
      roborev → merge-on-green → finalize.
- [ ] 7.4 Default `K` validated against the WS1 ramp (WS8) before the default is
      locked; record the evidence on #2420.
- [ ] 7.5 `openspec archive flight-admission-control` after merge.
