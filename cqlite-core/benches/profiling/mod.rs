//! Shared Criterion configuration that attaches an in-process sampling CPU
//! profiler to every bench target (docs/profiling.md).
//!
//! Profiling is **opt-in per run**: a plain `cargo bench` measures exactly as
//! before (criterion only activates the attached profiler when the binary is
//! invoked with `--profile-time <seconds>`), so the CI perf-regression gate is
//! unaffected by this module. When `--profile-time` is passed, each selected
//! bench runs for the given duration under the [`pprof`] sampler and writes a
//! flamegraph to:
//!
//! ```text
//! target/criterion/<group>/<bench>/profile/flamegraph.svg
//! ```
//!
//! Typical invocation (or use `scripts/profile.sh flame`):
//!
//! ```text
//! cargo bench --package cqlite-core --features cli-helpers \
//!     --bench read -- --profile-time 10
//! ```
//!
//! pprof samples via `SIGPROF`/`setitimer`, so it needs neither the `perf`
//! binary nor `perf_event_open` access — it works inside unprivileged
//! containers and CI runners. On non-unix targets (where pprof does not
//! build) this degrades to the plain criterion default config.
//!
//! Like `fixtures/mod.rs`, this file is included into each bench target via
//! `#[path = "profiling/mod.rs"] mod profiling;`.

#![allow(dead_code)]

use criterion::Criterion;

/// Sampling frequency in Hz. A prime (997 ≈ 1 kHz) avoids lockstep with
/// periodic work in the benched code, which would bias the samples.
#[cfg(unix)]
const SAMPLE_HZ: i32 = 997;

/// The standard criterion config for cqlite benches, with the pprof
/// flamegraph profiler attached on unix.
pub fn configure() -> Criterion {
    let criterion = Criterion::default();
    #[cfg(unix)]
    let criterion = criterion.with_profiler(pprof::criterion::PProfProfiler::new(
        SAMPLE_HZ,
        pprof::criterion::Output::Flamegraph(None),
    ));
    criterion
}
