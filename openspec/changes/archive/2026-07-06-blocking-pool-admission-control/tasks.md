# Tasks — Blocking-pool admission control for windowed scans (F4)

## 1. TDD tests (write first, must fail on `main`)

- [ ] 1.1 Add `scan_admission` unit tests (`#[cfg(test)]` in `scan_admission.rs`):
      against an isolated local `Arc<Semaphore>` — the bound (at most `L`
      permits outstanding), wait-then-proceed queueing (`L+1`-th blocks until a
      permit frees), and no-leak across 100 acquire/drop cycles (permits return to
      `L`), including drop-before-completion. Deterministic, no wall clock.
- [ ] 1.2 Add `cqlite-core/tests/issue_1594_scan_admission_bound.rs` (features
      `cli-helpers,scan-offload-probe`): set a low admission limit `L`, run `N > L`
      concurrent full scans over the real multi-chunk fixture with the in-flight
      probe armed, assert recorded max-admitted `<= L`, `>= 1` (non-vacuous), and
      rows unchanged. FAILS on `main` (no admission surface / counter).

## 2. Admission mechanism

- [ ] 2.1 Add `scan_admission.rs` sibling module: `default_limit()`
      (`max(1, available_parallelism)`, documented), a `OnceLock<Arc<Semaphore>>`
      production semaphore, `admit()` / `admit_with(&Arc<Semaphore>)` (fail-open,
      no `unwrap`/`expect`), and the `ScanAdmissionPermit` RAII guard (holds
      `Option<OwnedSemaphorePermit>`, releases on `Drop`, no panic in `Drop`).
- [ ] 2.2 Add the `scan-offload-probe`-gated test surface: `set_test_limit` /
      `clear_test_limit` (replace the shared semaphore) + in-flight/max-in-flight
      atomic counters incremented on guard create, decremented on `Drop`, with
      `reset` / `current` / `max` readers. Zero surface in default/release builds.

## 3. Wire admission into the scan driver

- [ ] 3.1 In `run_scan_stream_windowed`, acquire one admission permit at the TOP
      (before `ctx` is built and before either `spawn_blocking`), bind it to a
      local RAII guard held to function end. No other permit/lock is held while
      awaiting admission (deadlock-free by construction).
- [ ] 3.2 Expose `scan_stream_windowed::scan_admission` as `pub mod` ONLY under the
      `scan-offload-probe` feature (matching the `probe` pattern) so the guard test
      reaches the test surface; `admit()` stays crate-internal for production.

## 4. Gate wiring + validation

- [ ] 4.1 Add `--test issue_1594_scan_admission_bound` to the `scan-offload-guard`
      component in `scripts/agent-gate.sh` (and its header comment list).
- [ ] 4.2 `cargo +1.88.0 fmt --check` clean; `RUSTFLAGS="-D warnings" cargo clippy
      -p cqlite-core --features cli-helpers` clean; minimal-feature build clean.
- [ ] 4.3 Scan parity unchanged (admission is scheduling-only); lite gate PASS.
- [ ] 4.4 `openspec validate blocking-pool-admission-control --strict` clean.
