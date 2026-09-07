# Design — flight-jemalloc (issue #3997)

## The one constraint that dominates: bin-target only

A `#[global_allocator]` is process-wide and there can be exactly one per binary. That fact decides
where the code goes and dissolves two of #3997's five open questions:

- **Where:** `cqlite-flight/src/main.rs`, guarded by
  `#[cfg(all(feature = "jemalloc", target_os = "linux"))]`. Rust compiles `main.rs` into the **bin**
  target only. The **lib** target (`src/lib.rs`, what `flight-loadgen`, the integration tests and the
  benches link) never sees it.
- **Collision with the ratchets — none, by construction.** `issue_1494_producer_mem_budget` installs
  `dhat::Alloc` in its own **test** binary; `cqlite-core`'s `CountingAllocator` is `cfg(test)` in
  core's own test binary. Neither is the Flight **bin** target, so no two `#[global_allocator]`s ever
  meet. Requirement R4 pins this structurally (a grep-lint that the token appears in exactly one
  production file) so a later "move it to lib.rs for convenience" fails the gate.
- **Bindings/embedders — unaffected, by construction.** Python/Node depend on `cqlite-core`, not
  `cqlite-flight` (verified: no `Cargo.toml` outside `cqlite-flight/`, `tools/flight-loadgen/` names
  it, and both link the library target). R5 states this as a requirement with a manifest test.

## Dependency

`tikv-jemallocator` (jemalloc 5.3, the version #3551 measured via `LD_PRELOAD`), `optional = true`,
enabled by feature `jemalloc`. Chosen over mimalloc because mimalloc has **no measurement** on this
workload and #3551's kill criterion says every multiplier is measured, not modeled. Swapping to a
different allocator later is a one-line change behind the same feature and needs its own arm.

Default posture is decided by the measurement (proposal §Kill criterion), not by this document:
`default = ["jemalloc"]` is written into `Cargo.toml` **only** in the task that records a SHIP-as-default
verdict. Until then the feature exists and is off.

## Observability of the choice

A compile-time allocator is invisible at runtime unless the binary says so. Two surfaces, both
testable from outside the process:

1. `cqlite-flight --version` prints an extra line `allocator: jemalloc` / `allocator: system`.
2. The startup `tracing::info!` line that already logs observability status gains `allocator=<name>`.

Both are derived from the same `cfg` the installation uses (one `const ALLOCATOR: &str`), so they
cannot disagree with what was linked.

## Measurement design (reuses #3551's rig verbatim)

`scripts/perf/ws0-3551-abc.sh` gains arm **E**: same flags as arm A (`--flight-server-cpus 2,10
--flight-pin-mode siblings --flight-allocator system`) but pointing at a **second binary** built with
`--features jemalloc`. This is the one place the "one binary across all arms" invariant is
deliberately broken, and the aggregate must say so: the per-session sha256 of the flight binary
already recorded by the rig will differ between A and E, and `ws0_abc_aggregate.py`'s cross-arm
invariant check must be told E is an allowed exception rather than silently loosened for all arms.

Memory: the rig samples `/proc/<pid>/status` `VmHWM` and `VmRSS` of the flight server at scan end for
every arm (new in this change; it is the same `ws0_flight_arm.py` that already owns the server pid).
Peak N is measured too: one paired A/E at the admission ceiling (`--max-concurrent-scans` default),
because a lock-contention win at N=1 can be a fragmentation loss at N=peak.

Reporting contract (inherited, non-negotiable): rows/s per physical core warm, cycles/row, IPC, RSS
peak; **never** CPU-share-in-malloc — a share drop with flat rows/s is a FAIL (#3096 kill criterion).

## Deployment

The Trino/Helm deployment runs the release binary; no `LD_PRELOAD` was ever deployed, so nothing is
removed. `[profile.release]` is untouched. The connector needs no change.

## Alternatives rejected

- **Runtime switch (`CQLITE_FLIGHT_ALLOCATOR=…`)** — impossible for a linked global allocator; an
  `LD_PRELOAD`-based switch was rejected because it is deployment configuration outside the binary
  and is exactly what #3551 could not certify as the shipped form.
- **Workspace-wide allocator** — imposes on embedders; violates the scope decision F1 asked for.
- **Arena-count tuning (`MALLOC_ARENA_MAX`)** — measured worse (arm C0, −22.71%).
- **Bundling the pin change** — arm B: pin alone is −19.25%; needs its own topology design.
