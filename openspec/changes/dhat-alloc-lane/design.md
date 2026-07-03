# Design — dhat alloc/memory-budget lane + `size_of::<Value>` pin (A4)

## Context

Epic A is "measurement first": land honest regression nets pinned to today's numbers, then let the
optimization epics tighten them. A4 is the memory axis. dhat is already a `cqlite-core` dependency
(`cqlite-core/Cargo.toml`), installed only under the opt-in `dhat-heap` feature, and already drives
`cqlite-core/examples/heap_profile.rs` via `./scripts/profile.sh heap`. This change turns that
manual profiling into an automated, ratchet-able gate lane plus a compile-time layout pin.

## Decisions

### D1 — dhat in a `cargo test` target, not just an example
`dhat::Alloc` must be the `#[global_allocator]` to observe every allocation, which affects the whole
binary. Putting the budget tests in a dedicated integration test file
(`cqlite-core/tests/memory_budget.rs`) isolates the dhat allocator to that one test binary; the
default `core-tests` run (no `dhat-heap`) never compiles or runs it. The entire file is gated
`#![cfg(feature = "dhat-heap")]` so it exists only when the feature is on.

### D2 — single-threaded test execution (`--test-threads=1`)
`dhat::Profiler` is a process-global singleton; building a second one while one is live panics.
Each budget test builds its own `dhat::Profiler::builder().testing().build()`, reads
`dhat::HeapStats::get()`, and drops it. Cargo runs tests in a binary concurrently by default, so the
gate invokes the lane with `-- --test-threads=1`. Belt-and-suspenders: the tests also carry
`#[serial_test::serial]` (already a dev-dep, used by A2) so intra-binary ordering is enforced even if
someone runs the target without the flag.

### D3 — real fixtures, measured-first ceilings (ratchet semantics)
Workloads use the vendored real SSTables through `benches/fixtures/mod.rs` (`open_read_db`), the same
loader A1/A2 use — no synthetic data, honoring parity-is-truth. Ceilings are set from the first
measured run on `main`: `ceiling = measured + slack`, with the measured number and the Epic E target
written in a comment beside each constant. The issue's "10k-row" intent is met by driving the largest
real fixture over a fixed repeat count (real data, meaningful allocation volume) rather than
fabricating a 10k-row synthetic table.

### D4 — `size_of::<Value>` compile-time pin
A `const _: () = assert!(std::mem::size_of::<Value>() <= N);` beside the `Value` enum in
`cqlite-core/src/types.rs`. `N` is today's measured size; the comment records the measured value and
Epic E #1517 E1's smaller target (E1 tightens `N` when it lands). This is a zero-cost compile-time
check that fires under any feature set (including the default build and clippy `--all-features`), so a
`Value` that grows a hot per-cell type can never merge green. It is the one code edit outside
test/gate machinery and changes no runtime behavior.

### D5 — dataset-dependent, fail-closed on empty
The lane reads real Data.db, so `memory-budget` joins `DATASET_COMPONENTS`; the existing agent-gate
preflight already FAILs loudly when no Data.db is present (the #646/#1175 hazard). Present-but-empty
must fail, not skip: the tests assert the query returns ≥1 row before reading dhat stats, so a
zero-row fixture is a hard failure, never a green pass.

### D6 — honest partial: peak-byte ceiling vs the 128 MiB budget
`materialized_select_byte_ceiling` asserts two things: peak ≤ the pinned measured ceiling (the
ratchet) AND peak ≤ 128 MiB (the CLAUDE.md project budget). Today's small real fixtures sit far below
128 MiB, so the pinned ceiling is the meaningful regression net; the 128 MiB assertion documents the
project ceiling the lane ultimately defends as fixtures/workloads grow.

## Alternatives considered

- **Reuse `examples/heap_profile.rs` as the gate step.** Rejected: an example's process exit code and
  fixed workload are awkward to assert per-budget and it is not a `cargo test` target the gate
  component model expects; a test target gives per-budget red/green and `#[serial]` control.
- **A synthetic 10k-row generated fixture.** Rejected: violates parity-is-truth (real binaries only)
  and would fold write-path allocations into a read-path budget. Repeating a real scan is honest.
- **Runtime `assert!` on `size_of` in a test.** Rejected: a compile-time `const _` pin catches growth
  at build time under every feature set with zero runtime cost and no dataset dependency.

## Risks

- **dhat overhead / flakiness.** dhat counts allocations deterministically (not wall-clock), so byte
  and allocation totals are stable across runs on a given target; ceilings use documented slack for
  allocator/toolchain variance. Mitigated by `--test-threads=1` + `#[serial]`.
- **`--all-features` clippy compiles the file.** The `dhat-heap` + `cli-helpers` file must be
  warning-clean under `-D warnings`; ensured as part of the gate's clippy component.

## Wiring evidence

Public surface: `cqlite_core::Database::execute` (the real query path) driven from the test over a
real fixture loaded by `benches/fixtures/mod.rs`; dhat observes the allocations of that path. The gate
`memory-budget` component runs the target so the net actually executes in CI/gate (a net that does not
run is not a net). The `size_of` pin fires from `cqlite-core/src/types.rs` in every build.
