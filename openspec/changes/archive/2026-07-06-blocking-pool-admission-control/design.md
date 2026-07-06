# Design — Blocking-pool admission control for windowed scans (F4)

## Context

`run_scan_stream_windowed` (`scan_stream_windowed.rs`) spawns:

- a PARSE task (`drain_scan_window_blocking`) on `spawn_blocking` — always, one per scan (#1143);
- for a synchronously-faulting backend (mmap / `O_DIRECT`), a FEED task
  (`feed_raw_chunks_blocking`) ALSO on `spawn_blocking` — one per scan (#1593, F3).

So a faulting-backend scan holds TWO long-lived blocking-pool threads for its full
duration; `K` concurrent cold scans → `~2K` blocking threads. The pool (default
512) is shared with tokio-fs, so at scale latency-critical point-read fs ops queue
behind these throughput tasks (priority inversion, audit F4).

## Decision

A process-wide `tokio::sync::Semaphore` (the `scan_admission` module) caps the
number of windowed scans admitted concurrently. `run_scan_stream_windowed` acquires
ONE owned permit before spawning ANY blocking task and holds it (RAII) for the whole
scan.

### Why one permit per scan (not per blocking thread)

The permit gates the SCAN, which owns both its blocking threads. Admitting `cap`
scans therefore bounds faulting-backend blocking threads to `2 × cap` and
parse-only (buffered-backend) threads to `cap`. Sizing the cap accounts for the
doubled (`×2`) faulting-backend footprint by leaving the pool's remaining
`512 − 2·cap` threads for fs/point ops. Gating per-thread would need the scan to
hold two permits and re-acquire mid-scan — extra complexity and a deadlock risk
for no benefit, since the two threads are always co-resident for one scan.

### Cap sizing

`default_limit = max(1, available_parallelism)`. The parse half is CPU-bound;
admitting more concurrent scans than cores yields no throughput, only pool
pressure. On a typical box (`ncpu` = 8–16) that is 8–16 admitted scans → at most
16–32 blocking threads worst-case (faulting) — a small fraction of the 512-thread
pool, leaving ample fs/point-read headroom while still allowing full read
throughput for any realistic `cap ≥ workload`. This is intentionally a bound on
runaway `K`, not a throttle on normal use.

### Acquire-before-spawn, RAII release (deadlock freedom)

- The permit is acquired at the TOP of `run_scan_stream_windowed`, before `ctx` is
  built and before either `spawn_blocking`. A scan awaiting a permit holds NO other
  permit and NO lock — so "never hold a permit while awaiting another permit" holds
  trivially (there is exactly one admission point and a scan takes exactly one
  permit, once).
- The permit lives in a local RAII guard (`ScanAdmissionPermit`) that drops when
  `run_scan_stream_windowed` returns (success OR error) or when its future is
  dropped (cancellation — the scan runs inside `scan_stream`'s `tokio::spawn`,
  which is dropped on consumer cancel). `Drop` returns the owned semaphore permit,
  so the slot is released on EVERY exit path including panic/unwind. No slot leak.
- `Drop` performs only atomic decrements + the owned-permit drop; it never panics
  (no-panic-in-Drop).

### Queue-full = wait, never error

`admit()` is `semaphore.acquire_owned().await`. When `cap` scans are admitted, the
`cap + 1`-th scan's spawned task simply blocks at the `.await` until a permit frees,
then proceeds. This is natural backpressure; no scan returns an admission error. The
public `scan_stream` contract is unchanged (the caller still gets its `rx`
immediately; rows begin once the scan is admitted).

### Fail-open, no unwrap/expect

`acquire_owned()` errors only if the semaphore is CLOSED, which never happens (the
process-wide semaphore is never closed). To honor the no-`unwrap`/`expect` rule and
guarantee admission control can never make a scan un-runnable, `admit()` treats an
(impossible) closed-semaphore error as fail-open: it proceeds WITHOUT a permit
(`Option<OwnedSemaphorePermit>` = `None`) rather than panicking. This is a safety
valve, not a normal path.

### Testability without a production knob

Production uses a `OnceLock<Arc<Semaphore>>` (no per-scan lock). The
`scan-offload-probe`-gated test surface provides `set_test_limit(n)` /
`clear_test_limit()` (replaces the shared semaphore with a low-cap one) plus
in-flight/max-in-flight atomic counters incremented/decremented by the RAII guard.
This is test instrumentation compiled ONLY under the non-default feature — it is
NOT a shipped config knob, so the "no decorative knob" guardrail holds; and the
wiring test IS the set-knob-assert-behavior test the audit requires (set a low
limit, assert the observed max admitted count respects it).

Pure semaphore-behavior unit tests use an `admit_with(&Arc<Semaphore>)` internal
that takes an EXPLICIT local semaphore (production `admit()` calls it with the
global), so the bound / queue / no-leak properties are verified deterministically
against an isolated semaphore with no global-state interference and no wall clock.

## Alternatives considered

- **Per-thread semaphore (2 permits/scan):** rejected — needs mid-scan re-acquire
  (deadlock risk) for no benefit; the two threads are always co-resident.
- **Resurrect `platform/threading.rs`:** rejected by audit capstone §3 / AK2 — as
  built it cannot bound async scans; admission is designed fresh at the scan layer.
- **Error on queue-full:** rejected — the audit mandates WAIT (backpressure), and
  erroring would break the `scan_stream` contract for a scheduling concern.
- **Absolute wall-clock priority-inversion assertion:** rejected as the wiring
  proof — flaky on shared/oversubscribed CI. The bound is proven by a deterministic
  in-flight-counter assertion + isolated-semaphore unit tests instead.
