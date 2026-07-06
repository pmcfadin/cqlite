//! Blocking-pool admission control for windowed streaming scans (issue #1594,
//! Epic F finding F4).
//!
//! # Why this exists
//!
//! `run_scan_stream_windowed` spawns a long-lived `spawn_blocking` PARSE task per
//! scan (issue #1143), and — for a synchronously-faulting backend (mmap page
//! fault / `O_DIRECT` `pread`) — a SECOND long-lived `spawn_blocking` FEED task
//! per scan (issue #1593, F3). So a faulting-backend scan pins TWO blocking-pool
//! threads for its full duration; `K` concurrent cold scans pin `~2K` threads.
//! tokio's blocking pool defaults to 512 threads and is SHARED with tokio-fs
//! internals, so at high scan concurrency latency-critical point-read file ops
//! queue behind these long-lived throughput tasks — the priority inversion the
//! July 2026 read-path audit named (§Epic F / F4).
//!
//! # The mechanism
//!
//! A process-wide [`tokio::sync::Semaphore`] caps the number of windowed scans
//! admitted to the blocking pool concurrently. [`run_scan_stream_windowed`]
//! acquires exactly ONE permit ([`admit`]) at the TOP of the scan — BEFORE any
//! `spawn_blocking` — and holds it via the RAII [`ScanAdmissionPermit`] guard for
//! the whole scan. The permit (and therefore the admission slot) is returned on
//! EVERY exit path — success, error, or cancellation/drop — because the guard's
//! `Drop` returns the owned permit. A scan can never leak a slot.
//!
//! One permit per SCAN (not per blocking thread): the scan owns both its blocking
//! threads, so admitting `cap` scans bounds faulting-backend blocking threads to
//! `2 × cap` and buffered-backend threads to `cap`. Sizing `cap` from the CPU
//! count therefore leaves the pool's remaining `512 − 2·cap` threads free for
//! fs/point ops.
//!
//! Queue-full behavior is WAIT, not error: when `cap` scans are admitted the
//! `cap + 1`-th scan's spawned task simply blocks at `admit().await` until a
//! permit frees, then proceeds (natural backpressure). There is exactly one
//! admission point and each scan takes exactly one permit once, holding no other
//! permit/lock while awaiting — so admission is deadlock-free by construction.
//!
//! [`run_scan_stream_windowed`]: super::SSTableReader::run_scan_stream_windowed

use std::sync::{Arc, OnceLock};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Default cap on concurrently-admitted windowed scans.
///
/// Derived from `available_parallelism`: the windowed parse half is CPU-bound, so
/// admitting more concurrent scans than cores yields no throughput, only pressure
/// on the shared blocking pool. Because a faulting-backend scan holds TWO blocking
/// threads per admitted permit (issue #1593, F3's doubled footprint), a cap of
/// `ncpu` bounds the worst-case blocking-thread footprint to `2 × ncpu` — a small
/// fraction of tokio's 512-thread default pool — leaving ample headroom for
/// latency-critical fs/point-read operations regardless of scan concurrency `K`.
/// Never below 1 (a zero-permit semaphore would deadlock every scan).
fn default_limit() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .max(1)
}

/// The process-wide admission semaphore used in production. Lazily initialized
/// once from [`default_limit`]; no per-scan lock on the hot path.
fn production_semaphore() -> Arc<Semaphore> {
    static PROD: OnceLock<Arc<Semaphore>> = OnceLock::new();
    Arc::clone(PROD.get_or_init(|| Arc::new(Semaphore::new(default_limit()))))
}

/// The admission semaphore this process should use right now. In default/release
/// builds this is always the production semaphore; under the non-default
/// `scan-offload-probe` feature a test may install a low-cap override.
fn semaphore() -> Arc<Semaphore> {
    #[cfg(feature = "scan-offload-probe")]
    {
        if let Some(s) = probe::test_override() {
            return s;
        }
    }
    production_semaphore()
}

/// Acquire one admission permit for a windowed scan against the process-wide
/// semaphore, waiting if the admission limit is currently reached. See
/// [`admit_with`] for the fail-open / no-panic contract.
pub(super) async fn admit() -> ScanAdmissionPermit {
    admit_with(&semaphore()).await
}

/// Acquire one admission permit against an EXPLICIT semaphore (production
/// [`admit`] passes the process-wide one; unit tests pass an isolated local one).
///
/// Waits when no permit is available (natural backpressure — never errors from
/// queue-full). `acquire_owned` errors only if the semaphore is CLOSED, which
/// never happens for our never-closed semaphore; to honor the no-`unwrap`/`expect`
/// rule AND guarantee admission control can never make a scan un-runnable, that
/// impossible case is FAIL-OPEN: proceed without a permit rather than panic.
async fn admit_with(sem: &Arc<Semaphore>) -> ScanAdmissionPermit {
    let permit = Arc::clone(sem).acquire_owned().await.ok();
    ScanAdmissionPermit::new(permit)
}

/// RAII admission slot for one windowed scan.
///
/// Holds the owned semaphore permit (when one was granted) for the scan's whole
/// duration. Dropping this guard — on success, error, or cancellation/drop —
/// returns the permit to the semaphore, releasing the admission slot. `Drop`
/// performs only atomic bookkeeping + the owned-permit drop and never panics
/// (no-panic-in-`Drop`).
pub(super) struct ScanAdmissionPermit {
    /// `None` only on the impossible fail-open path (closed semaphore); a real
    /// admission always carries `Some`.
    _permit: Option<OwnedSemaphorePermit>,
}

impl ScanAdmissionPermit {
    fn new(permit: Option<OwnedSemaphorePermit>) -> Self {
        #[cfg(feature = "scan-offload-probe")]
        probe::on_admit();
        Self { _permit: permit }
    }
}

impl Drop for ScanAdmissionPermit {
    fn drop(&mut self) {
        #[cfg(feature = "scan-offload-probe")]
        probe::on_release();
        // `_permit` drops here, returning the owned permit to the semaphore.
    }
}

/// Test-only admission instrumentation (issue #1594 wiring guard). Compiled ONLY
/// under the non-default `scan-offload-probe` feature: in a normal/default/release
/// build this module, its statics, and its call-sites do not exist, so admission
/// control adds zero test surface and no public API. Exposed as `pub` (via the
/// parent's feature-gated `pub mod scan_admission`) so the integration guard can
/// install a low cap and read the in-flight counters.
#[cfg(feature = "scan-offload-probe")]
pub mod probe {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    /// A low-cap semaphore installed by [`set_test_limit`] to replace the
    /// process-wide production semaphore for the duration of a guard test.
    static TEST_OVERRIDE: Mutex<Option<Arc<Semaphore>>> = Mutex::new(None);
    /// Scans currently holding an admission permit.
    static IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);
    /// High-water mark of [`IN_FLIGHT`] since the last [`reset`].
    static MAX_IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);

    /// The installed test-override semaphore, if any. A poisoned lock falls back
    /// to the production semaphore (returns `None`) rather than panicking.
    pub(super) fn test_override() -> Option<Arc<Semaphore>> {
        TEST_OVERRIDE.lock().ok().and_then(|g| g.clone())
    }

    /// Install a fresh admission semaphore with `limit` permits, replacing the
    /// production one for subsequent [`super::admit`] calls, and reset the
    /// in-flight counters. Call from a test before driving concurrent scans.
    pub fn set_test_limit(limit: usize) {
        reset();
        if let Ok(mut g) = TEST_OVERRIDE.lock() {
            *g = Some(Arc::new(Semaphore::new(limit.max(1))));
        }
    }

    /// Remove any installed test-override semaphore, restoring the production
    /// semaphore for subsequent [`super::admit`] calls.
    pub fn clear_test_limit() {
        if let Ok(mut g) = TEST_OVERRIDE.lock() {
            *g = None;
        }
    }

    /// Zero the in-flight / max-in-flight counters.
    pub fn reset() {
        IN_FLIGHT.store(0, Ordering::SeqCst);
        MAX_IN_FLIGHT.store(0, Ordering::SeqCst);
    }

    /// Record that a scan just acquired an admission permit, updating the
    /// high-water mark. Called from [`ScanAdmissionPermit::new`].
    pub(super) fn on_admit() {
        let now = IN_FLIGHT.fetch_add(1, Ordering::SeqCst) + 1;
        MAX_IN_FLIGHT.fetch_max(now, Ordering::SeqCst);
    }

    /// Record that a scan just released its admission permit. Called from
    /// [`ScanAdmissionPermit`]'s `Drop`.
    pub(super) fn on_release() {
        IN_FLIGHT.fetch_sub(1, Ordering::SeqCst);
    }

    /// Scans currently admitted (holding a permit).
    pub fn current_in_flight() -> usize {
        IN_FLIGHT.load(Ordering::SeqCst)
    }

    /// High-water mark of concurrently-admitted scans since the last [`reset`].
    /// The F4 wiring guard asserts this never exceeds the installed limit.
    pub fn max_in_flight() -> usize {
        MAX_IN_FLIGHT.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;
    use tokio::sync::Semaphore;

    /// The bound: against an isolated `L`-permit semaphore, at most `L` permits
    /// are outstanding at once and the `L+1`-th `admit_with` WAITS until one frees
    /// (queue-full = wait, not error), then proceeds.
    #[tokio::test]
    async fn admission_bounds_outstanding_permits_and_queues_the_overflow() {
        const L: usize = 3;
        let sem = Arc::new(Semaphore::new(L));

        // Take all L permits.
        let mut held = Vec::new();
        for _ in 0..L {
            held.push(admit_with(&sem).await);
        }
        assert_eq!(sem.available_permits(), 0, "all {L} permits taken");

        // The (L+1)-th admission must WAIT: it cannot complete while all permits
        // are held. Deterministic (current-thread runtime): the spawned task is
        // driven only by our `yield_now`s and parks on the pending `acquire_owned`,
        // so `admitted` stays false — no wall-clock assertion.
        let admitted = Arc::new(AtomicBool::new(false));
        let admitted_task = Arc::clone(&admitted);
        let sem2 = Arc::clone(&sem);
        let overflow = tokio::spawn(async move {
            let permit = admit_with(&sem2).await;
            admitted_task.store(true, Ordering::SeqCst);
            permit
        });
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(
            !admitted.load(Ordering::SeqCst),
            "the L+1-th admission must queue while all permits are held"
        );

        // Release one permit; the queued admission now proceeds.
        held.pop();
        let permit = tokio::time::timeout(Duration::from_secs(5), overflow)
            .await
            .expect("queued admission proceeds once a permit frees")
            .expect("overflow task joins");
        assert!(
            admitted.load(Ordering::SeqCst),
            "the queued admission completed after a permit was freed"
        );
        drop(permit);
        drop(held);
        assert_eq!(sem.available_permits(), L, "permits fully restored");
    }

    /// No leak: acquiring and dropping a permit many times (including dropping
    /// before any scan work would complete) returns every permit to the
    /// semaphore, so the full limit is always eventually available.
    #[tokio::test]
    async fn repeated_acquire_and_drop_leaks_no_permits() {
        const L: usize = 2;
        let sem = Arc::new(Semaphore::new(L));
        for _ in 0..100 {
            let a = admit_with(&sem).await;
            let b = admit_with(&sem).await;
            assert_eq!(sem.available_permits(), 0, "both permits held mid-cycle");
            // Drop before "completion" — RAII must still return the permits.
            drop(a);
            drop(b);
            assert_eq!(sem.available_permits(), L, "permits returned after drop");
        }
    }

    /// The default limit is a sane, documented, non-zero value.
    #[test]
    fn default_limit_is_at_least_one() {
        assert!(default_limit() >= 1);
    }
}
