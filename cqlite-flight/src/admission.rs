//! `do_get` admission control (issue #2420, WS4).
//!
//! The Flight server has no application-level ceiling on concurrent `do_get`
//! scans: past ~256 concurrent scans the Tokio blocking pool (512 threads, ~2 per
//! scan) queues silently, each scan opens a fresh fd per SSTable toward the
//! container `ulimit`, and peak RSS grows with offered concurrency — every
//! in-flight request degrades together rather than the server queueing or
//! shedding load gracefully.
//!
//! [`Admission`] bounds concurrent admitted scans to a configured `K` via an
//! owned [`tokio::sync::Semaphore`]. A `do_get` acquires a permit BEFORE opening
//! any SSTable (see [`crate::service`]), holds it — through the RAII
//! [`AdmissionPermit`] moved into the response stream — for the scan's lifetime,
//! and releases it on completion, client disconnect, or cancellation. On
//! saturation a request waits a bounded [`AdmissionConfig::wait_timeout`] for a
//! permit; if none frees it is rejected with gRPC **`UNAVAILABLE`** (never
//! `RESOURCE_EXHAUSTED`, which the connector's #2241 replica-failover would treat
//! as a hard query failure) BEFORE any record batch is delivered, so failover to
//! another replica is correctness-safe.
//!
//! All admission state is mirrored to the `cqlite.flight.admission.*` metric
//! catalog AND held in feature-independent atomics (readable via [`Admission::snapshot`]),
//! so the deterministic tests observe engagement without depending on the
//! `observability` OTel feature — mirroring the [`crate::obs::RpcMetrics`] pattern.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use cqlite_core::observability::{self as obs, catalog};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::Status;

/// Default `do_get` admission ceiling.
///
/// Sized from the binding constraints (NOT `num_cpus` — CPU is not the limit):
/// the Tokio blocking pool caps at 512 threads and each scan consumes ~2 (a setup
/// `spawn_blocking` + the merge `spawn_blocking`), a ~256 ceiling; each scan also
/// opens one fd per input SSTable against the ~1024 container `ulimit`, a
/// `1024 / M`-SSTable ceiling. 64 sits well below both (≥4× blocking-pool
/// headroom, ~64 fds at M≈16) while still absorbing bursty offered concurrency.
/// Conservative pending WS1-ramp (WS8) validation before the default is locked.
pub const DEFAULT_MAX_CONCURRENT_SCANS: usize = 64;

/// Default permit-wait timeout: how long a saturated `do_get` parks for a permit
/// before it is shed with `UNAVAILABLE`. Long enough to absorb short bursts
/// transparently, bounded so sustained overload sheds rather than hangs.
pub const DEFAULT_WAIT_TIMEOUT_MS: u64 = 30_000;

/// Environment variable backing `--max-concurrent-scans`.
pub const ENV_MAX_CONCURRENT_SCANS: &str = "CQLITE_MAX_CONCURRENT_SCANS";

/// Environment variable backing `--admission-wait-timeout-ms`.
pub const ENV_WAIT_TIMEOUT_MS: &str = "CQLITE_ADMISSION_WAIT_TIMEOUT_MS";

/// The permit-wait budget: how long a saturated `do_get` may wait for an
/// admission permit before it is rejected with `UNAVAILABLE`.
///
/// Deliberately NOT represented as a bare `Duration` with a `Duration::MAX`
/// sentinel for "unbounded" (roborev-1703): `tokio::time::timeout` computes an
/// absolute deadline (`Instant::now() + duration`) internally, and
/// `Instant::now() + Duration::MAX` is an overflow/panic hazard that must never
/// depend on how far in the future "now" happens to be — library code must
/// never carry that hazard, even latently. [`WaitBudget::Unbounded`] instead
/// skips the `timeout()` wrapper entirely in [`Admission::acquire`], awaiting
/// `acquire_owned()` directly — no deadline math at all, so there is nothing to
/// overflow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitBudget {
    /// Wait as long as it takes for a permit to free (or the semaphore to
    /// close, on shutdown). Used by [`Admission::unconstrained`] — a caller
    /// that never opted into a configured ceiling gets a `do_get` that never
    /// sheds on a wait timeout (the semaphore is effectively uncontended at
    /// [`Semaphore::MAX_PERMITS`] anyway).
    Unbounded,
    /// Wait up to `Duration`, then reject with `UNAVAILABLE`.
    Timeout(Duration),
}

impl From<Duration> for WaitBudget {
    fn from(d: Duration) -> Self {
        WaitBudget::Timeout(d)
    }
}

/// Configuration for [`Admission`]: the ceiling and the permit-wait budget. Both
/// are real, wired knobs (CLI flag + env; see [`crate`]'s `main`).
#[derive(Debug, Clone, Copy)]
pub struct AdmissionConfig {
    /// The admission ceiling `K` — the maximum number of concurrently admitted
    /// `do_get` scans. Clamped to a minimum of 1 by [`Admission::new`].
    pub max_concurrent_scans: usize,
    /// How long a request waits on `acquire` for a permit before it is rejected
    /// with `UNAVAILABLE` — see [`WaitBudget`] for why this is an enum, not a
    /// bare `Duration`. Injectable so tests drive it deterministically under a
    /// paused Tokio clock (no wall-clock sleep).
    pub wait_budget: WaitBudget,
}

impl Default for AdmissionConfig {
    fn default() -> Self {
        Self {
            max_concurrent_scans: DEFAULT_MAX_CONCURRENT_SCANS,
            wait_budget: WaitBudget::Timeout(Duration::from_millis(DEFAULT_WAIT_TIMEOUT_MS)),
        }
    }
}

impl AdmissionConfig {
    /// Build from the environment, falling back to the documented defaults. A
    /// present-but-unparseable value (or a zero ceiling) falls back to the default
    /// rather than failing startup. `max_concurrent_scans` is clamped to ≥1 by
    /// [`Admission::new`].
    pub fn from_env() -> Self {
        let mut cfg = Self::default();
        if let Some(k) = std::env::var(ENV_MAX_CONCURRENT_SCANS)
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .filter(|k| *k >= 1)
        {
            cfg.max_concurrent_scans = k;
        }
        if let Some(ms) = std::env::var(ENV_WAIT_TIMEOUT_MS)
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
        {
            cfg.wait_budget = WaitBudget::Timeout(Duration::from_millis(ms));
        }
        cfg
    }
}

/// A point-in-time read of the admission counters, feature-independent (does not
/// require the `observability` OTel feature). All fields are levels or monotonic
/// totals — scale-free, independent of fixture row/SSTable count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdmissionSnapshot {
    /// The configured ceiling `K`.
    pub limit: usize,
    /// Permits currently held (admitted, in-flight scans).
    pub in_use: i64,
    /// Requests currently parked on `acquire`.
    pub waiting: i64,
    /// Monotonic count of timeout rejections (each returned `UNAVAILABLE`).
    pub rejected_total: u64,
    /// Monotonic count of permit-wait histogram samples recorded (one per
    /// acquire outcome — admit or reject). The feature-independent evidence that
    /// the wait-latency instrument engaged.
    pub wait_samples: u64,
}

/// Shared admission state behind [`Admission`]'s `Arc`.
#[derive(Debug)]
struct AdmissionInner {
    /// The permit pool. Held in its own `Arc` so [`Semaphore::acquire_owned`] can
    /// hand out `'static` [`OwnedSemaphorePermit`]s moved into response streams.
    sem: Arc<Semaphore>,
    limit: usize,
    wait_budget: WaitBudget,
    in_use: AtomicI64,
    waiting: AtomicI64,
    rejected: AtomicU64,
    wait_samples: AtomicU64,
}

impl AdmissionInner {
    /// Record a permit-wait sample (both the admit and reject paths): bump the
    /// feature-independent sample counter and mirror the duration to the catalog
    /// histogram.
    fn record_wait(&self, waited: Duration) {
        self.wait_samples.fetch_add(1, Ordering::Relaxed);
        obs::record_histogram(
            catalog::FLIGHT_ADMISSION_WAIT_SECONDS,
            waited.as_secs_f64(),
            &[],
        );
    }
}

/// A cloneable `do_get` admission ceiling. Clones share one semaphore + counter
/// set (the `Arc`), so every per-RPC [`crate::service::CqliteFlightService`] clone
/// throttles against the SAME `K`.
#[derive(Clone)]
pub struct Admission {
    inner: Arc<AdmissionInner>,
}

impl Admission {
    /// Build an admission ceiling from `config`. The ceiling is clamped
    /// symmetrically at both ends (roborev-1697): a minimum of 1 (a zero-permit
    /// semaphore would reject every request) and a maximum of
    /// [`Semaphore::MAX_PERMITS`] — `Semaphore::new` PANICS above that bound, so
    /// an absurd `--max-concurrent-scans`/`CQLITE_MAX_CONCURRENT_SCANS` value
    /// must be capped, never allowed to crash startup. A clamp (at either end) is
    /// logged so an operator setting an out-of-range value learns it was capped,
    /// not silently honoured. Records the (post-clamp) configured limit gauge.
    pub fn new(config: AdmissionConfig) -> Self {
        let requested = config.max_concurrent_scans;
        let limit = requested.clamp(1, Semaphore::MAX_PERMITS);
        if limit != requested {
            tracing::warn!(
                requested,
                clamped_to = limit,
                "max-concurrent-scans out of range [1, Semaphore::MAX_PERMITS]; clamped"
            );
        }
        obs::record_gauge(catalog::FLIGHT_ADMISSION_LIMIT, limit as i64, &[]);
        Self {
            inner: Arc::new(AdmissionInner {
                sem: Arc::new(Semaphore::new(limit)),
                limit,
                wait_budget: config.wait_budget,
                in_use: AtomicI64::new(0),
                waiting: AtomicI64::new(0),
                rejected: AtomicU64::new(0),
                wait_samples: AtomicU64::new(0),
            }),
        }
    }

    /// An admission ceiling that is, for all practical purposes, unconstrained
    /// (issue #2420, roborev-1699): `Semaphore::MAX_PERMITS` permits and
    /// [`WaitBudget::Unbounded`] (never a `Duration::MAX` sentinel — see
    /// [`WaitBudget`]'s doc for why that would be an overflow hazard), so this
    /// can never meaningfully gate a `do_get`. This is what
    /// [`crate::service::CqliteFlightService::new`] uses so a library caller
    /// embedding `cqlite-flight` keeps EXACTLY today's (pre-#2420) behavior — no
    /// environment read, no ceiling on concurrent scans. Admission becomes a
    /// real gate only when a caller explicitly opts in via
    /// [`crate::service::CqliteFlightService::with_admission`] (the `cqlite-flight`
    /// SERVER BINARY, `main`, does this with a CLI/env-configured `K`).
    pub fn unconstrained() -> Self {
        Self::new(AdmissionConfig {
            max_concurrent_scans: Semaphore::MAX_PERMITS,
            wait_budget: WaitBudget::Unbounded,
        })
    }

    /// The configured ceiling `K`.
    pub fn limit(&self) -> usize {
        self.inner.limit
    }

    /// The configured permit-wait budget.
    pub fn wait_budget(&self) -> WaitBudget {
        self.inner.wait_budget
    }

    /// A point-in-time read of the admission counters.
    pub fn snapshot(&self) -> AdmissionSnapshot {
        AdmissionSnapshot {
            limit: self.inner.limit,
            in_use: self.inner.in_use.load(Ordering::Relaxed),
            waiting: self.inner.waiting.load(Ordering::Relaxed),
            rejected_total: self.inner.rejected.load(Ordering::Relaxed),
            wait_samples: self.inner.wait_samples.load(Ordering::Relaxed),
        }
    }

    /// Acquire an admission permit, waiting up to the configured
    /// [`WaitBudget`] only if the ceiling is currently saturated.
    ///
    /// **Fast path (roborev-1696):** an UNCONTENDED acquire (a permit is
    /// immediately available) is served by [`Semaphore::try_acquire_owned`] and
    /// never touches the `waiting` gauge or records a permit-wait histogram
    /// sample — the gauge is a genuine backpressure signal (requests parked in
    /// the wait queue), so an instant admit must not transiently over-report
    /// queue depth. An instant admit is deliberately zero-sample in the
    /// histogram too (not a `0.0` sample): `cqlite.flight.admission.wait_seconds`
    /// measures how long a request that DID contend waited, so mixing in a flood
    /// of zero-duration instant admits would dilute that distribution with
    /// non-events.
    ///
    /// **Slow path:** only entered on [`tokio::sync::TryAcquireError::NoPermits`]
    /// — the ceiling is saturated. Bumps `waiting`, then waits per the configured
    /// [`WaitBudget`] — [`WaitBudget::Timeout`] wraps the acquire in
    /// `tokio::time::timeout`; [`WaitBudget::Unbounded`] (roborev-1703) awaits
    /// `acquire_owned` DIRECTLY, with no `timeout()` wrapper and therefore no
    /// deadline computation to overflow. On success returns an
    /// [`AdmissionPermit`] RAII guard that holds the semaphore permit AND the
    /// `in_use` gauge until dropped (moved into the response stream so every
    /// exit path — completion, disconnect, cancel — releases it) and records the
    /// genuine wait sample. On a `Timeout` budget's deadline elapsing, returns a
    /// gRPC **`UNAVAILABLE`** [`Status`] (never `RESOURCE_EXHAUSTED`) and
    /// increments `rejected_total`; `Unbounded` can never take this arm.
    ///
    /// A request whose future is dropped while WAITING (client disconnect before
    /// admission) never acquires a permit and its `waiting` count is released by
    /// the [`WaitGuard`] drop — no leak on either the wait or the hold path. The
    /// semaphore is only ever closed on shutdown; both budgets map a closed
    /// semaphore to the same retry-safe `UNAVAILABLE` shed.
    pub async fn acquire(&self) -> Result<AdmissionPermit, Status> {
        let inner = Arc::clone(&self.inner);

        // Fast path: try to admit without ever registering as "waiting". Covers
        // the common uncontended case with zero gauge/histogram noise.
        match inner.sem.clone().try_acquire_owned() {
            Ok(permit) => return Ok(AdmissionPermit::new(permit, inner)),
            Err(tokio::sync::TryAcquireError::Closed) => return Err(reject_status()),
            Err(tokio::sync::TryAcquireError::NoPermits) => {
                // Fall through to the slow, timed wait path below.
            }
        }

        // Slow path: the ceiling is saturated. RAII: the `waiting` gauge is
        // released on EVERY exit, including a cancellation that drops this
        // future mid-`.await` (a request cancelled while waiting never held a
        // permit and must not leak the waiting count).
        let waiter = WaitGuard::enter(Arc::clone(&inner));
        let started = tokio::time::Instant::now();
        let sem = Arc::clone(&inner.sem);
        // roborev-1703: NO `Duration::MAX` sentinel anywhere. `Unbounded` awaits
        // `acquire_owned` with no `timeout()` wrapper at all — no deadline
        // computed, so nothing can overflow. Only `Timeout(d)` computes a
        // (bounded, caller-supplied) deadline.
        let outcome = match inner.wait_budget {
            WaitBudget::Unbounded => match sem.acquire_owned().await {
                Ok(permit) => SlowAcquireOutcome::Admitted(permit),
                Err(_closed) => SlowAcquireOutcome::Closed,
            },
            WaitBudget::Timeout(d) => match tokio::time::timeout(d, sem.acquire_owned()).await {
                Ok(Ok(permit)) => SlowAcquireOutcome::Admitted(permit),
                Ok(Err(_closed)) => SlowAcquireOutcome::Closed,
                Err(_elapsed) => SlowAcquireOutcome::TimedOut,
            },
        };
        // roborev-1700 (same gauge-accuracy class as roborev-1696): drop the
        // waiter THE INSTANT the future resolves — before recording the wait
        // sample or constructing the `AdmissionPermit` (which bumps `in_use`) —
        // so an admitted (or rejected) request is never simultaneously counted
        // both waiting AND in_use. `waiter`'s scope-end drop would otherwise
        // outlive `AdmissionPermit::new` below, overlapping both gauges for the
        // span of that call.
        drop(waiter);
        match outcome {
            SlowAcquireOutcome::Admitted(permit) => {
                inner.record_wait(started.elapsed());
                Ok(AdmissionPermit::new(permit, inner))
            }
            SlowAcquireOutcome::Closed => Err(reject_status()),
            SlowAcquireOutcome::TimedOut => {
                inner.record_wait(started.elapsed());
                inner.rejected.fetch_add(1, Ordering::Relaxed);
                obs::add_counter(catalog::FLIGHT_ADMISSION_REJECTED_TOTAL, 1, &[]);
                Err(reject_status())
            }
        }
    }
}

/// The three outcomes of the slow-path acquire (roborev-1703): unifies the
/// `WaitBudget::Unbounded` (no timeout wrapper — cannot time out) and
/// `WaitBudget::Timeout` (may time out) arms into one `match` below, so the
/// gauge/counter bookkeeping after the await is written exactly once.
enum SlowAcquireOutcome {
    Admitted(OwnedSemaphorePermit),
    Closed,
    TimedOut,
}

/// The rejection status returned when no permit frees within the wait timeout.
///
/// MUST be `UNAVAILABLE`: the connector's #2241 `ReplicaFailoverStream` fails over
/// to the next replica ONLY on `UNAVAILABLE` (and only pre-first-batch), so a
/// sustained-overload reject sheds to a less-loaded replica rather than failing
/// the query. `RESOURCE_EXHAUSTED` (or any other code) would be rethrown and fail
/// the split.
fn reject_status() -> Status {
    Status::unavailable(
        "cqlite-flight: server at max-concurrent-scans capacity; \
         retry (failover-safe: no batch delivered)",
    )
}

/// RAII guard for the `waiting` gauge: bumped on `enter`, released on `Drop` —
/// including the future-drop (client-disconnect-while-waiting) path.
struct WaitGuard {
    inner: Arc<AdmissionInner>,
}

impl WaitGuard {
    fn enter(inner: Arc<AdmissionInner>) -> Self {
        let level = inner.waiting.fetch_add(1, Ordering::Relaxed) + 1;
        obs::record_gauge(catalog::FLIGHT_ADMISSION_WAITING, level, &[]);
        Self { inner }
    }
}

impl Drop for WaitGuard {
    fn drop(&mut self) {
        let prev = self.inner.waiting.fetch_sub(1, Ordering::Relaxed);
        let level = (prev - 1).max(0);
        obs::record_gauge(catalog::FLIGHT_ADMISSION_WAITING, level, &[]);
    }
}

/// An held admission permit. Owns the [`OwnedSemaphorePermit`] AND the `in_use`
/// gauge: constructing it increments `in_use`, dropping it releases the semaphore
/// permit and decrements `in_use`. Moved into the `do_get` response stream so
/// every stream-exit path (completion, client disconnect, cancellation) drops it —
/// one lifetime, structurally leak-free.
///
/// The permit field is `Option` (roborev-1702), NOT a bare `OwnedSemaphorePermit`:
/// Rust drops a struct's fields AFTER `Drop::drop` returns, so a bare field would
/// release the semaphore permit (making capacity available to a waiter) ONLY
/// AFTER the `in_use` gauge decrement below had already executed — a transient
/// window where the gauge undercounts real capacity relative to what a waiter
/// can actually observe becoming free. `Drop` below explicitly `take()`s and
/// drops the permit FIRST, so the semaphore's availability and the `in_use`
/// gauge move in the same order every time.
#[derive(Debug)]
pub struct AdmissionPermit {
    permit: Option<OwnedSemaphorePermit>,
    inner: Arc<AdmissionInner>,
}

impl AdmissionPermit {
    fn new(permit: OwnedSemaphorePermit, inner: Arc<AdmissionInner>) -> Self {
        let level = inner.in_use.fetch_add(1, Ordering::Relaxed) + 1;
        obs::record_gauge(catalog::FLIGHT_ADMISSION_IN_USE, level, &[]);
        Self {
            permit: Some(permit),
            inner,
        }
    }
}

impl Drop for AdmissionPermit {
    fn drop(&mut self) {
        // Release the semaphore permit FIRST (roborev-1702) — before the `in_use`
        // gauge decrement, so a waiter that wakes on this release never observes
        // a gauge still counting this permit as held.
        drop(self.permit.take());
        let prev = self.inner.in_use.fetch_sub(1, Ordering::Relaxed);
        let level = (prev - 1).max(0);
        obs::record_gauge(catalog::FLIGHT_ADMISSION_IN_USE, level, &[]);
    }
}

/// Wrap a `do_get` response stream so it holds `permit` for the stream's whole
/// lifetime. The wrapper is a transparent pass-through — it forwards every item
/// and the terminal `None` unchanged (byte-identical rows/order/schema/batch
/// boundaries; admission changes *when* a scan runs, never *what* it returns) —
/// and drops `permit` (releasing admission) when the stream completes or is
/// dropped (client disconnect).
pub(crate) fn admitted_stream(
    inner: crate::streaming::DoGetStream,
    permit: AdmissionPermit,
) -> crate::streaming::DoGetStream {
    Box::pin(AdmittedStream {
        inner,
        _permit: permit,
    })
}

/// The pass-through stream produced by [`admitted_stream`]. `AdmittedStream` is
/// `Unpin` (both fields are), so `poll_next` projects through `get_mut`.
struct AdmittedStream {
    inner: crate::streaming::DoGetStream,
    _permit: AdmissionPermit,
}

impl futures::Stream for AdmittedStream {
    type Item = Result<arrow_flight::FlightData, Status>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.get_mut().inner.as_mut().poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

#[cfg(test)]
#[path = "admission_tests.rs"]
mod tests;
