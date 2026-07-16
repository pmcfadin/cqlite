# Tasks — Saturation instrumentation (WS2, issue #2419)

Sequenced on branch `issue-2419-saturation-gauges`. Each stage names the surface it exercises and
carries a red-then-green, work-probe/deterministic test (no wall-clock asserts). Anchors are
`main`-relative and will drift; re-grep before editing.

## Stage 0 — catalog + instruments (write registration tests FIRST, must fail on main)
- [ ] 0.1 Add the five gauge name constants to `cqlite-core/src/observability/catalog.rs`:
  `PROC_THREADS`, `PROC_FDS`, `PROC_RSS_BYTES`, `MERGE_EGRESS_CHANNEL_DEPTH`,
  `FLIGHT_BLOCKING_TASKS_IN_USE`, each documented with its bounded (empty) attribute set. Add
  `unit::FDS = "{fd}"` and `unit::ENTRIES = "{entry}"`. (flight-saturation-instrumentation)
- [ ] 0.2 Add all five to `ALL_METRICS`; extend the namespaced-and-unique test to cover them (fails
  on main until 0.1 lands). (flight-saturation-instrumentation)
- [ ] 0.3 Add an `i64_gauge` field per metric to `otel::Instruments`, build each once in
  `instruments()`, add a matching arm in `otel::record_gauge`. Add a test asserting each name resolves
  to a pre-built field (not the ad-hoc fallback), so no saturation gauge rebuilds per sample (#2412).
  (flight-saturation-instrumentation)

## Stage 1 — /proc readers (deterministic, no-fabrication)
- [ ] 1.1 New file `cqlite-flight/src/saturation.rs`: `read_proc_threads()`, `read_proc_fds()`,
  `read_proc_rss_bytes()` → `Option<u64>` over `/proc/self/{task,fd,status:VmRSS}`, Linux-only branch;
  `None` on non-`/proc` platforms. Pure `std::fs`, no new deps. (flight-saturation-instrumentation)
- [ ] 1.2 Tests: on Linux each returns `Some(v)`, `v > 0` (live self-read, deterministic); the
  non-Linux branch returns `None` (compile/exercise the else arm). Assert absence → the sampler skips
  `record_gauge` (no `0`). (flight-saturation-instrumentation)

## Stage 2 — atomic-backed gauges (egress channel + blocking tasks)
- [ ] 2.1 Egress channel depth: an `AtomicI64` incremented on `send`, decremented on `recv` at the
  `merge/mod.rs` `sync_channel` sites; `max(0)` floor; record to `MERGE_EGRESS_CHANNEL_DEPTH`; add a
  test accessor. Producer-fast/consumer-slow harness test: depth > 0 while backed up, returns to 0
  after drain (level assert, not timing). (flight-saturation-instrumentation)
- [ ] 2.2 Blocking-task in-use: an `AtomicI64` + RAII guard incremented on entry / decremented on exit
  (incl. panic/cancel) at the flight `streaming.rs` `spawn_blocking` sites; record to
  `FLIGHT_BLOCKING_TASKS_IN_USE`; test accessor. Balance test: rises under concurrent tasks, returns
  to baseline on every exit path. (flight-saturation-instrumentation)

## Stage 3 — background sampler + wiring
- [ ] 3.1 Sampler task in `saturation.rs`: `tokio::select!` interval (~2s const) vs. shutdown token;
  each tick calls `obs::record_gauge` for every reader that returns `Some`; skips `None`. Log the
  unsupported-platform state ONCE at startup. (flight-saturation-instrumentation)
- [ ] 3.2 Spawn the sampler in `cqlite-flight/src/main.rs` at startup, wired to `shutdown_signal()`.
  Test: sampler performs ≥1 tick (sample-count probe) and its handle resolves after the stop signal
  (no forever-run, no busy-spin) — assert on completion, not a sleep. (flight-saturation-instrumentation)

## Stage 4 — distinctness + docs
- [ ] 4.1 Test: the five saturation names are pairwise distinct from the five
  `cqlite.flight.admission.*` names; `blocking_tasks_in_use` ≠ `admission.in_use`.
  (flight-saturation-instrumentation)
- [ ] 4.2 Catalog doc-comments for the five gauges written in the #2426 operator-sentence shape
  (name, meaning, unit, healthy-vs-alarming, absence rule), so the generated reference picks them up.
  (flight-saturation-instrumentation)

## Out of scope (open forks — do not implement without owner call)
- Global `tokio` blocking-pool queue depth (`tokio_unstable`) — O1.
- `/proc/self/statm` × page-size RSS (`libc` dep) — O2 (default is dep-free `VmRSS`).
- Feature-flagged `dhat`-heap merge lane — O3.
- Configurable sampler cadence flag — O4.
