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

/// Configuration for [`Admission`]: the ceiling and the permit-wait timeout. Both
/// are real, wired knobs (CLI flag + env; see [`crate`]'s `main`).
#[derive(Debug, Clone, Copy)]
pub struct AdmissionConfig {
    /// The admission ceiling `K` — the maximum number of concurrently admitted
    /// `do_get` scans. Clamped to a minimum of 1 by [`Admission::new`].
    pub max_concurrent_scans: usize,
    /// How long a request waits on `acquire` for a permit before it is rejected
    /// with `UNAVAILABLE`. Injectable so tests drive it deterministically under a
    /// paused Tokio clock (no wall-clock sleep).
    pub wait_timeout: Duration,
}

impl Default for AdmissionConfig {
    fn default() -> Self {
        Self {
            max_concurrent_scans: DEFAULT_MAX_CONCURRENT_SCANS,
            wait_timeout: Duration::from_millis(DEFAULT_WAIT_TIMEOUT_MS),
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
            cfg.wait_timeout = Duration::from_millis(ms);
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
    wait_timeout: Duration,
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
    /// Build an admission ceiling from `config`. The ceiling is clamped to a
    /// minimum of 1 (a zero-permit semaphore would reject every request). Records
    /// the configured limit gauge.
    pub fn new(config: AdmissionConfig) -> Self {
        let limit = config.max_concurrent_scans.max(1);
        obs::record_gauge(catalog::FLIGHT_ADMISSION_LIMIT, limit as i64, &[]);
        Self {
            inner: Arc::new(AdmissionInner {
                sem: Arc::new(Semaphore::new(limit)),
                limit,
                wait_timeout: config.wait_timeout,
                in_use: AtomicI64::new(0),
                waiting: AtomicI64::new(0),
                rejected: AtomicU64::new(0),
                wait_samples: AtomicU64::new(0),
            }),
        }
    }

    /// The configured ceiling `K`.
    pub fn limit(&self) -> usize {
        self.inner.limit
    }

    /// The configured permit-wait timeout.
    pub fn wait_timeout(&self) -> Duration {
        self.inner.wait_timeout
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

    /// Acquire an admission permit, waiting up to [`AdmissionConfig::wait_timeout`].
    ///
    /// On success returns an [`AdmissionPermit`] RAII guard that holds the
    /// semaphore permit AND the `in_use` gauge until dropped (moved into the
    /// response stream so every exit path — completion, disconnect, cancel —
    /// releases it). On timeout returns a gRPC **`UNAVAILABLE`** [`Status`] (never
    /// `RESOURCE_EXHAUSTED`) and increments `rejected_total`.
    ///
    /// A request whose future is dropped while WAITING (client disconnect before
    /// admission) never acquires a permit and its `waiting` count is released by
    /// the [`WaitGuard`] drop — no leak on either the wait or the hold path.
    pub async fn acquire(&self) -> Result<AdmissionPermit, Status> {
        let inner = Arc::clone(&self.inner);
        // RAII: the `waiting` gauge is released on EVERY exit, including a
        // cancellation that drops this future mid-`.await` (a request cancelled
        // while waiting never held a permit and must not leak the waiting count).
        let _waiter = WaitGuard::enter(Arc::clone(&inner));
        let started = tokio::time::Instant::now();
        let sem = Arc::clone(&inner.sem);
        match tokio::time::timeout(inner.wait_timeout, sem.acquire_owned()).await {
            Ok(Ok(permit)) => {
                inner.record_wait(started.elapsed());
                Ok(AdmissionPermit::new(permit, inner))
            }
            // The semaphore is only ever closed on shutdown; surface it as the same
            // retry-safe status so an in-flight acquire during drain sheds cleanly.
            Ok(Err(_closed)) => Err(reject_status()),
            Err(_elapsed) => {
                inner.record_wait(started.elapsed());
                inner.rejected.fetch_add(1, Ordering::Relaxed);
                obs::add_counter(catalog::FLIGHT_ADMISSION_REJECTED_TOTAL, 1, &[]);
                Err(reject_status())
            }
        }
    }
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
#[derive(Debug)]
pub struct AdmissionPermit {
    _permit: OwnedSemaphorePermit,
    inner: Arc<AdmissionInner>,
}

impl AdmissionPermit {
    fn new(permit: OwnedSemaphorePermit, inner: Arc<AdmissionInner>) -> Self {
        let level = inner.in_use.fetch_add(1, Ordering::Relaxed) + 1;
        obs::record_gauge(catalog::FLIGHT_ADMISSION_IN_USE, level, &[]);
        Self {
            _permit: permit,
            inner,
        }
    }
}

impl Drop for AdmissionPermit {
    fn drop(&mut self) {
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
