//! Saturation instrumentation for the Flight server (issue #2419, WS2 of epic
//! #2313).
//!
//! Makes the OS-level resources that bind first under concurrent readers legible
//! on the server's own metric surface, so the read-throughput saturation ramp
//! can attribute a plateau to the resource that saturates (the research ranks the
//! order-of-failure as thread/scheduler collapse → queueing → fd exhaustion →
//! memory). Two mechanisms:
//!
//! * **`/proc`-derived process gauges** — [`read_proc_threads`], [`read_proc_fds`],
//!   [`read_proc_rss_bytes`] are pure `Option`-returning `std::fs` reads over
//!   `/proc/self/*` (Linux only; `None` on any non-`/proc` platform), driven by a
//!   single background [`run_sampler`] task on a ~2s cadence. A reader that
//!   returns `None` means the sampler emits NO sample for that gauge — the gauge
//!   is ABSENT from the exposition, never a fabricated `0` (the telemetry
//!   authoritative-data rule, #2314).
//! * **A flight blocking-task gauge** — [`BlockingTaskGuard`] is an RAII guard
//!   incremented on entry to a flight `spawn_blocking` closure and decremented on
//!   exit (incl. panic/cancel), backing `cqlite.flight.blocking_tasks_in_use`. An
//!   honest, dependency-free proxy for blocking-pool pressure — FLIGHT-managed
//!   tasks in flight, NOT the global `tokio` blocking-pool queue depth (which
//!   needs `tokio_unstable`; out of scope, design open fork O1).
//!
//! No new dependencies: RSS is read from the `VmRSS` text field of
//! `/proc/self/status` (no page-size math), and thread/fd counts are directory
//! entry counts. All gauges route through `cqlite_core::observability`, a no-op
//! when its `observability` feature is off, so this compiles and runs identically
//! in every build.

use std::future::Future;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Once;
use std::time::Duration;

use cqlite_core::observability::{self as obs, catalog};

/// Background saturation sampler cadence. A fixed ~2s constant (design fork O4:
/// one fewer tunable for 0.15) — bounded per-tick cost (three small `/proc`
/// reads), chosen over on-demand collection so a wedged `do_get` (no RPC
/// completion, no batch) keeps its thread/fd/RSS footprint visible while it hangs.
pub const DEFAULT_SAMPLE_INTERVAL: Duration = Duration::from_secs(2);

// --- /proc-derived process-resource readers --------------------------------
//
// Each is a pure function over `/proc/self/*`, returning `Some(v)` on Linux and
// `None` on any platform without `/proc`. Deterministic (no wall-clock wait): the
// calling process always has ≥1 thread, several open fds, and a non-zero RSS.

/// Count the entries in a `/proc/self/*` directory (`task` or `fd`), returning
/// `None` if the directory cannot be read. `std::fs::read_dir` excludes `.`/`..`,
/// so the count is the true number of tasks / descriptors. Reading `/proc/self/fd`
/// itself holds one transient descriptor, which is legitimately included in the
/// live reading.
#[cfg(target_os = "linux")]
fn count_dir_entries(path: &str) -> Option<u64> {
    let mut n: u64 = 0;
    for entry in std::fs::read_dir(path).ok()? {
        if entry.is_ok() {
            n = n.saturating_add(1);
        }
    }
    Some(n)
}

/// Process thread count from `/proc/self/task` (Linux). `None` off-`/proc`.
#[cfg(target_os = "linux")]
pub fn read_proc_threads() -> Option<u64> {
    count_dir_entries("/proc/self/task")
}

/// Off-`/proc` platforms: report absence, never a fabricated `0`.
#[cfg(not(target_os = "linux"))]
pub fn read_proc_threads() -> Option<u64> {
    None
}

/// Open file-descriptor count from `/proc/self/fd` (Linux). `None` off-`/proc`.
#[cfg(target_os = "linux")]
pub fn read_proc_fds() -> Option<u64> {
    count_dir_entries("/proc/self/fd")
}

/// Off-`/proc` platforms: report absence, never a fabricated `0`.
#[cfg(not(target_os = "linux"))]
pub fn read_proc_fds() -> Option<u64> {
    None
}

/// Resident set size in BYTES from the `VmRSS` field of `/proc/self/status`
/// (Linux) — a plain-text `kB` field scaled to bytes, dependency-free (no
/// `sysconf` page-size math). `None` off-`/proc` or if the field is absent.
#[cfg(target_os = "linux")]
pub fn read_proc_rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            // Format: `VmRSS:\t     1234 kB` — first whitespace-separated token
            // after the label is the value in kB.
            let kb: u64 = rest.split_whitespace().next()?.parse().ok()?;
            return Some(kb.saturating_mul(1024));
        }
    }
    None
}

/// Off-`/proc` platforms: report absence, never a fabricated `0`.
#[cfg(not(target_os = "linux"))]
pub fn read_proc_rss_bytes() -> Option<u64> {
    None
}

/// Clamp a `u64` reading into the gauge's `i64` domain without a panicking cast
/// (saturates at `i64::MAX`, unreachable for a real thread/fd/RSS reading).
fn as_gauge(v: u64) -> i64 {
    i64::try_from(v).unwrap_or(i64::MAX)
}

// --- Flight blocking-task gauge --------------------------------------------

/// Process-wide count of flight-managed `spawn_blocking` tasks currently
/// outstanding, backing `cqlite.flight.blocking_tasks_in_use`. A single shared
/// atomic (not thread-local) so the increment (closure entry) and decrement
/// (closure exit, on any path) stay correct even though the closure runs on a
/// blocking-pool thread distinct from the spawner.
static BLOCKING_TASKS: AtomicI64 = AtomicI64::new(0);

fn record_blocking(level: i64) {
    // Floor at 0 so an unexpected imbalance never records a negative gauge
    // (matches `RpcMetrics::finish`).
    obs::record_gauge(catalog::FLIGHT_BLOCKING_TASKS_IN_USE, level.max(0), &[]);
}

/// RAII guard that accounts one flight `spawn_blocking` task as in-use for its
/// lifetime. Constructed as the FIRST act inside a flight `spawn_blocking`
/// closure ([`crate::streaming`]); its `Drop` decrements on EVERY exit path —
/// normal return, early `?`, cancel, or panic — so the increment/decrement are
/// balanced by construction (mirroring the #2316 `ProducerThreadGuard`).
pub(crate) struct BlockingTaskGuard {
    _private: (),
}

impl BlockingTaskGuard {
    /// Enter a flight blocking task: increment the in-use gauge and return the
    /// guard whose drop decrements it.
    pub(crate) fn enter() -> Self {
        record_blocking(BLOCKING_TASKS.fetch_add(1, Ordering::SeqCst) + 1);
        Self { _private: () }
    }
}

impl Drop for BlockingTaskGuard {
    fn drop(&mut self) {
        record_blocking(BLOCKING_TASKS.fetch_sub(1, Ordering::SeqCst) - 1);
    }
}

/// Read the current process-wide flight blocking-task in-use level (issue #2419).
///
/// Exposes the same atomic that drives `cqlite.flight.blocking_tasks_in_use`, so
/// an end-to-end streaming test can assert the level rises while blocking tasks
/// are outstanding and returns to its pre-load baseline after every task exits
/// (asserting on the LEVEL, never on timing). Feature-independent (the atomic is
/// maintained regardless of the `observability` OTel feature; only the emission
/// is gated), mirroring [`crate::obs::in_flight_level`].
pub fn blocking_tasks_in_use_level() -> i64 {
    BLOCKING_TASKS.load(Ordering::SeqCst)
}

// --- Background sampler -----------------------------------------------------

/// Total collection ticks the sampler has performed (a `do_get`-independent
/// work-probe): incremented once per `sample_once`, whether or not any `/proc`
/// reader returned `Some`, so it confirms the sampler ran even on a non-`/proc`
/// platform.
static SAMPLE_TICKS: AtomicU64 = AtomicU64::new(0);

/// Read the total number of sampler collection ticks performed (issue #2419) —
/// the sampler's work-probe. `#[cfg(test)]`-only: consumed solely by the
/// in-crate sampler tests (no production reader), so it would otherwise be
/// flagged dead code under `-D warnings`.
#[cfg(test)]
pub(crate) fn sample_ticks() -> u64 {
    SAMPLE_TICKS.load(Ordering::SeqCst)
}

/// Perform one collection tick: read each `/proc` gauge and record ONLY the ones
/// that returned `Some` (a `None` reader emits no sample — absence, never `0`).
fn sample_once() {
    if let Some(threads) = read_proc_threads() {
        obs::record_gauge(catalog::PROC_THREADS, as_gauge(threads), &[]);
    }
    if let Some(fds) = read_proc_fds() {
        obs::record_gauge(catalog::PROC_FDS, as_gauge(fds), &[]);
    }
    if let Some(rss) = read_proc_rss_bytes() {
        obs::record_gauge(catalog::PROC_RSS_BYTES, as_gauge(rss), &[]);
    }
    SAMPLE_TICKS.fetch_add(1, Ordering::SeqCst);
}

/// Log the platform's `/proc` support state EXACTLY ONCE (design D2), so an
/// operator on a non-`/proc` platform learns the `cqlite.proc.*` gauges will be
/// absent (never per-sample spam).
fn log_platform_support_once() {
    static LOGGED: Once = Once::new();
    LOGGED.call_once(|| {
        if read_proc_threads().is_none() {
            tracing::info!(
                "saturation sampler: /proc is unavailable on this platform; the \
                 cqlite.proc.threads/fds/rss_bytes gauges will be ABSENT (no \
                 fabricated zero). The server starts and serves normally."
            );
        } else {
            tracing::debug!("saturation sampler started; cqlite.proc.* gauges active");
        }
    });
}

/// Run the background saturation sampler until `shutdown` resolves.
///
/// Takes one immediate startup sample (so the `cqlite.proc.*` gauges are visible
/// the moment the server starts, before any load) and then ticks every
/// `interval` (production: [`DEFAULT_SAMPLE_INTERVAL`]), calling [`sample_once`]
/// on each tick. Returns promptly when `shutdown` resolves — no leaked task, no
/// busy-spin (a `tokio::select!` between the interval and the shutdown future).
/// Spawned at server startup and wired to the same shutdown source as the tonic
/// server. Because the initial sample runs before the select loop, the sampler
/// always performs at least one collection tick even if shutdown is already
/// pending (deterministic, no reliance on interval first-tick timing).
pub async fn run_sampler<S>(interval: Duration, shutdown: S)
where
    S: Future<Output = ()>,
{
    log_platform_support_once();
    tokio::pin!(shutdown);
    // Immediate startup sample; the periodic ticker then fires after each full
    // `interval` (never an extra immediate tick — `interval_at` from `now +
    // interval` — so the cadence stays regular).
    sample_once();
    let start = tokio::time::Instant::now() + interval;
    let mut ticker = tokio::time::interval_at(start, interval);
    loop {
        tokio::select! {
            _ = ticker.tick() => sample_once(),
            _ = &mut shutdown => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Stage 1.2: on Linux each `/proc` reader returns `Some(v)` with `v > 0` (a
    /// live, deterministic self-read); on a non-`/proc` platform each returns
    /// `None` (the absence branch — no fabricated `0`). Whichever branch this
    /// build compiles is exercised.
    #[test]
    fn proc_readers_match_platform() {
        #[cfg(target_os = "linux")]
        {
            assert!(
                read_proc_threads().is_some_and(|v| v > 0),
                "Linux: the calling process has ≥1 thread"
            );
            assert!(
                read_proc_fds().is_some_and(|v| v > 0),
                "Linux: the calling process has ≥1 open fd"
            );
            assert!(
                read_proc_rss_bytes().is_some_and(|v| v > 0),
                "Linux: the calling process has a non-zero resident set"
            );
        }
        #[cfg(not(target_os = "linux"))]
        {
            assert_eq!(read_proc_threads(), None, "off-/proc: absence, not 0");
            assert_eq!(read_proc_fds(), None, "off-/proc: absence, not 0");
            assert_eq!(read_proc_rss_bytes(), None, "off-/proc: absence, not 0");
        }
    }

    /// Stage 1.2 corollary: a `None` reader contributes NO sample to a tick, so
    /// the gauge is absent rather than `0`. Exercised by driving `sample_once`
    /// and confirming it never panics and always advances the tick probe,
    /// regardless of platform (on non-`/proc` platforms all three readers are
    /// `None` and no gauge is recorded, yet the tick still counts).
    #[test]
    fn sample_once_advances_tick_and_skips_none_readers() {
        let before = sample_ticks();
        sample_once();
        assert!(
            sample_ticks() > before,
            "a collection tick is counted even when every /proc reader is None"
        );
    }

    /// Stage 2.2: the blocking-task gauge RISES with concurrent guards and
    /// balances back to baseline on every exit path (RAII drop), asserted on the
    /// LEVEL, not on timing. Uses a captured pre/post baseline so it is robust
    /// under the parallel test runner.
    #[test]
    fn blocking_task_guard_rises_and_balances() {
        let base = blocking_tasks_in_use_level();
        {
            let _g1 = BlockingTaskGuard::enter();
            assert_eq!(blocking_tasks_in_use_level(), base + 1);
            let _g2 = BlockingTaskGuard::enter();
            assert_eq!(
                blocking_tasks_in_use_level(),
                base + 2,
                "a second concurrent blocking task must ADD to the in-use count"
            );
        }
        assert_eq!(
            blocking_tasks_in_use_level(),
            base,
            "every guard's drop decrements — the level returns to its baseline"
        );
    }

    /// A guard dropped on the panic-unwind path still decrements (RAII), so a
    /// panicking blocking closure never leaks in-use count. Asserted on the level.
    #[test]
    fn blocking_task_guard_decrements_on_panic() {
        let base = blocking_tasks_in_use_level();
        let result = std::panic::catch_unwind(|| {
            let _g = BlockingTaskGuard::enter();
            panic!("simulated blocking-closure panic");
        });
        assert!(result.is_err(), "the closure panicked as set up");
        assert_eq!(
            blocking_tasks_in_use_level(),
            base,
            "the guard's Drop ran during unwind, restoring the baseline"
        );
    }

    /// Stage 3.2: the sampler performs ≥1 collection tick and its handle RESOLVES
    /// after the shutdown signal (it does not run forever, does not busy-spin) —
    /// asserted on task completion, never a wall-clock sleep.
    #[tokio::test]
    async fn sampler_ticks_at_least_once_then_stops_on_shutdown() {
        let base = sample_ticks();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let handle = tokio::spawn(run_sampler(Duration::from_millis(5), async move {
            let _ = rx.await;
        }));

        // Signal shutdown; the immediate first interval tick (polled first via
        // `biased`) guarantees ≥1 collection before the loop breaks.
        let _ = tx.send(());

        // Assert on COMPLETION (the handle resolving), with a generous safety
        // timeout — not a fixed sleep. A sampler that ran forever would time out.
        let joined = tokio::time::timeout(Duration::from_secs(5), handle).await;
        let task_result =
            joined.expect("the sampler handle must resolve after shutdown (no forever-run)");
        task_result.expect("the sampler task completed without panicking");
        assert!(
            sample_ticks() > base,
            "the sampler performed at least one collection tick before stopping"
        );
    }
}
