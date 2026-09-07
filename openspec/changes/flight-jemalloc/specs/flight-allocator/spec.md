# flight-allocator — new capability for flight-jemalloc (issue #3997)

The `cqlite-flight` server binary MAY link a non-glibc global allocator. This spec fixes where it
lives, what it must not touch, how the choice is observable, and what has to be measured before the
default changes. All requirements are ADDED.

## Requirement R1 — Linked allocator behind a feature, confined to the binary target

`cqlite-flight` SHALL expose a Cargo feature `jemalloc` that, on `target_os = "linux"`, installs
`tikv_jemallocator::Jemalloc` as `#[global_allocator]` in `cqlite-flight/src/main.rs` and nowhere
else. With the feature off, or off-Linux, the binary SHALL use the system allocator.

- **Scenario R1.1** — Given a Linux host, When `cargo build -p cqlite-flight --features jemalloc`
  is run, Then `nm`/`readelf` on the binary resolves `malloc` to jemalloc's symbol (`je_malloc` or
  the `_rjem_` prefix) — asserted by `scripts/tests/test_flight_allocator_link.sh`.
- **Scenario R1.2** — Given the same host, When `cargo build -p cqlite-flight --no-default-features`
  is run, Then no jemalloc symbol is present in the binary (same test, negative arm).
- **Scenario R1.3** — Given any host, When `cargo test -p cqlite-flight` and
  `cargo test -p cqlite-flight --features dhat-heap` run, Then both build and pass — the allocator
  is not in any test binary (gate components `flight-tests`, `memory-budget`).

## Requirement R2 — The allocator in use is observable from outside the process

- **Scenario R2.1** — When `cqlite-flight --version` runs, Then stdout contains exactly one line
  matching `^allocator: (jemalloc|system)$`, and the value matches R1's build (integration test
  `cqlite-flight/tests/issue_3997_allocator_surface.rs`, run under both feature states in the gate).
- **Scenario R2.2** — When the server starts, Then the first startup `info` log line contains
  `allocator=<same value>`.

## Requirement R3 — The default is decided by a linked-build measurement, not inferred from #3551

`default = ["jemalloc"]` SHALL be written only after arm E (linked jemalloc, pin `2,10`) is measured
against arm A on the #3551 rig with #3551's paired interleaved method, and the pre-registered kill
criterion (proposal) is applied.

- **Scenario R3.1** — Given `scripts/perf/ws0-3551-abc.sh` with arm `E`, When ≥3 clean within-round
  A/E pairs are collected, Then `docs/reports/ws0-3997-report.md` records median Δrows/s, Δcycles/row,
  IPC, `VmHWM` and `VmRSS` per arm at N=1 **and** at the admission ceiling, with the byte basis and
  fixture sha256 named, and states which of SHIP-default / SHIP-opt-in / DO-NOT-SHIP applies.
- **Scenario R3.2** — Given the report says SHIP-default, Then the `Cargo.toml` default-feature
  commit cites the report path and the median figure in its message; Given it says otherwise, Then
  `default` stays `[]` and the report is still committed (a null is a deliverable).
- **Scenario R3.3** — Given arm E's binary sha256 differs from arm A's, Then the aggregate's
  cross-arm invariant check names E as the one permitted exception and still FAILs on any other
  cross-arm binary difference (`scripts/tests/test_ws0_abc_driver_guards.sh` gains this case).

## Requirement R4 — Structural confinement is gate-enforced

- **Scenario R4.1** — When `scripts/tests/test_flight_allocator_confinement.sh` runs (a `tooling-tests`
  member), Then it asserts that `global_allocator` occurs in exactly one non-test production file in
  the workspace, `cqlite-flight/src/main.rs`, guarded by the `jemalloc` feature; any `src/lib.rs`
  or `cqlite-core` occurrence outside `cfg(test)` FAILs.

## Requirement R5 — No library consumer, binding, or other crate inherits the allocator

- **Scenario R5.1** — When the confinement test runs, Then it also asserts no `Cargo.toml` under
  `bindings/`, `cqlite-core/`, `cqlite-cli/` names `tikv-jemallocator`, and that every dependent of
  `cqlite-flight` (`tools/flight-loadgen`, the crate's own dev-dependency) links the library target.

## Requirement R6 — Memory budget is preserved

- **Scenario R6.1** — Given arm E, When R3.1's measurement runs, Then `VmHWM` ≤ 1.10× arm A for
  SHIP-default (≤1.25× for opt-in), at both N=1 and peak N.
- **Scenario R6.2** — When the gate's `memory-budget` component runs, Then
  `issue_1494_producer_mem_budget` still executes and passes unchanged.

## Acceptance

Gate PASS (full, one run of record) + C intent audit against this spec + roborev clean, and R3.1's
report committed with an explicit verdict. Wiring evidence for the user-facing surface is R2.1's
end-to-end test against the built binary.
