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
//! A process-wide [`tokio::sync::Semaphore`] caps the number of scan OPERATIONS
//! admitted to the blocking pool concurrently. Admission is per top-level scan
//! OPERATION — NOT per blocking thread and NOT per fan-out sub-scan (issue #1594
//! deadlock fix):
//!
//! - A direct single-generation scan
//!   ([`run_scan_stream`](super::SSTableReader::run_scan_stream) invoked with
//!   [`ScanAdmission::Acquire`]) acquires exactly ONE permit ([`admit`]) at the
//!   TOP of the scan — BEFORE any `spawn_blocking` — and holds it via the RAII
//!   [`ScanAdmissionPermit`] guard for the whole scan.
//! - A cross-generation fan-out merge (`SSTableManager::scan_stream`) is ONE
//!   operation that legitimately opens N per-generation sub-scans and primes a
//!   head from EVERY sub-scan before draining any. It acquires exactly ONE permit
//!   for the whole merge and opens each sub-scan with [`ScanAdmission::Exempt`], so
//!   the sub-scans do NOT independently admit. A single query therefore needs
//!   exactly ONE permit, never N.
//!
//! The permit (and therefore the admission slot) is returned on EVERY exit path —
//! success, error, or cancellation/drop — because the guard's `Drop` returns the
//! owned permit. A scan can never leak a slot.
//!
//! One permit per scan OPERATION (not per blocking thread, not per sub-scan): the
//! bound limits concurrent INDEPENDENT scan operations (the K-concurrent-cold-scans
//! case the July 2026 audit named). For the common single-generation table each
//! admitted operation owns its (up to two, faulting-backend) blocking threads, so
//! `cap` operations bound the footprint to `~2 × cap` threads — a small fraction of
//! tokio's 512-thread default, leaving ample headroom for latency-critical fs/point
//! ops. A cross-generation operation's fan-out inherently needs its N sub-scans
//! live AT ONCE (it primes every head before draining), so that intra-operation
//! fan-out is deliberately NOT throttled: throttling it would deadlock (see below).
//! The admission bound is therefore on operation concurrency, not on the total
//! blocking threads a single multi-generation operation may transiently use.
//!
//! # Scope
//!
//! Admission covers the LAZY/WINDOWED scan path only. The write-support
//! multi-generation EAGER-materialize branch (`merge_generations_for_read` in
//! `storage/sstable/mod.rs`, which drains a `KWayMerger` inside a single
//! `spawn_blocking`) does NOT pass through admission — it predates F4 and is not
//! the `spawn_blocking`-windowed target. Do not assume ALL reads are throttled by
//! this semaphore; wiring that eager path in would be a separate change.
//!
//! # Deadlock-freedom
//!
//! Admission is per top-level scan OPERATION; a fan-out merge's sub-scans are
//! EXEMPT and never independently admit. No code path blocks on [`admit`] while
//! already holding a permit, and no permit-holder's progress depends on another
//! permit being granted: an operation acquires AT MOST ONE permit, once, at its
//! top, holding no other permit/lock while awaiting, and its internal fan-out
//! sub-scans run permit-free. Independent operations that queue at [`admit`] hold
//! nothing, so they cannot block a permit-holder. Hence there is no hold-and-wait
//! cycle — the mechanism stays deadlock-free even when a single query fans out to
//! `N > cap` generations.
//!
//! This is the exact bug an earlier per-SUB-SCAN design introduced: with each
//! sub-scan admitting independently, a fan-out to `N > cap` generations let `cap`
//! sub-scans win permits and park in consumer backpressure while the remaining
//! sub-scans blocked forever at `admit`; the merge — stuck priming a head from
//! EVERY sub-scan — never drained the permit-holders, so no permit ever freed
//! (permanent hang). Admitting at operation granularity removes the hold-and-wait.
//!
//! Queue-full behavior is WAIT, not error: when `cap` operations are admitted the
//! `cap + 1`-th operation simply blocks at `admit().await` until a permit frees,
//! then proceeds (natural backpressure).

use std::sync::{Arc, OnceLock};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Whether a windowed scan must acquire its own admission permit, or is already
/// covered by a caller's permit.
///
/// Admission is per top-level scan OPERATION (issue #1594). A cross-generation
/// fan-out merge is ONE operation that opens N sub-scans and primes a head from
/// every one before draining any; if each sub-scan admitted independently, a
/// fan-out to `N > cap` generations would deadlock (`cap` sub-scans hold permits
/// and park in backpressure while the rest block forever at [`admit`], and the
/// priming merge never drains the holders). So the fan-out acquires ONE permit for
/// the whole operation and opens each sub-scan [`Exempt`](ScanAdmission::Exempt).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ScanAdmission {
    /// Top-level scan operation: acquire exactly one permit for the whole scan.
    Acquire,
    /// Sub-scan of a fan-out merge that already holds the operation's single
    /// permit: do NOT admit (independent admission here would hold-and-wait on the
    /// same operation and deadlock).
    ///
    /// The SOLE constructor of this variant is the lazy cross-generation fan-out
    /// merge in `SSTableManager::scan_stream`, which is `#[cfg(not(feature =
    /// "tombstones"))]` (under `tombstones` that method instead delegates wholesale
    /// to the materializing `scan`, so no fan-out sub-scan is ever opened). The
    /// enum, the `run_scan_stream` match arm that consumes this variant, and the
    /// intra-doc links to it are all compiled UNCONDITIONALLY, so the variant must
    /// keep existing under every feature set — it cannot itself be `#[cfg]`-gated
    /// out without breaking that always-compiled match/doc surface. Under
    /// `tombstones` alone it is therefore declared-but-never-constructed, which
    /// `-D warnings` dead-code would reject; allow it under exactly the feature
    /// where its constructor is compiled out (mirroring the constructor's cfg
    /// rather than blanket-allowing). Issue #1594 / dead-variant clippy fix.
    #[cfg_attr(feature = "tombstones", allow(dead_code))]
    Exempt,
}

/// Default cap on concurrently-admitted windowed scans.
///
/// Derived from `available_parallelism`: the windowed parse half is CPU-bound, so
/// admitting more concurrent scan OPERATIONS than cores yields no throughput, only
/// pressure on the shared blocking pool. For the common single-generation table a
/// faulting-backend operation holds TWO blocking threads per admitted permit (issue
/// #1593, F3's doubled footprint), so a cap of `ncpu` bounds that footprint to
/// `~2 × ncpu` — a small fraction of tokio's 512-thread default pool — leaving
/// ample headroom for latency-critical fs/point-read operations regardless of
/// operation concurrency `K`. (A cross-generation operation's fan-out inherently
/// uses more, one pair per sub-scan, because it needs all N sub-scans live at once;
/// that intra-operation fan-out is deliberately not throttled — see the module
/// docs — so the cap bounds operation concurrency, not one operation's fan-out.)
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

/// Acquire one admission permit for a scan OPERATION against the process-wide
/// semaphore, waiting if the admission limit is currently reached. See
/// [`admit_with`] for the fail-open / no-panic contract. Callers acquire at most
/// ONE permit per top-level operation (a fan-out merge's sub-scans are
/// [`ScanAdmission::Exempt`] and do not call this) — see the module docs.
pub(crate) async fn admit() -> ScanAdmissionPermit {
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
pub(crate) struct ScanAdmissionPermit {
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
    ///
    /// Defense-in-depth (issue #1594 roborev Low): saturating, never wrapping.
    /// Real admission is balanced — every [`on_admit`] is paired with exactly one
    /// `on_release` — so `IN_FLIGHT` never legitimately underflows. But once the
    /// probe lib-tests run in-process together (the new gate lib run), a test's
    /// [`reset`] could zero the counter while another test/scan still holds a
    /// permit; a plain `fetch_sub(1)` from 0 would then wrap `usize` and corrupt
    /// [`current_in_flight`]/[`max_in_flight`] for unrelated scans in the same
    /// binary. `saturating_sub` clamps at 0 instead. Production admission is
    /// unaffected (it never underflows).
    pub(super) fn on_release() {
        let _ = IN_FLIGHT.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| {
            Some(v.saturating_sub(1))
        });
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
    use serial_test::serial;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;
    use tokio::sync::Semaphore;

    // These tests drive `admit_with`, which under the `scan-offload-probe` feature
    // mutates the process-global `IN_FLIGHT`/`MAX_IN_FLIGHT` counters via
    // `probe::on_admit`/`on_release`. `#[serial]` (serial_test) serializes them
    // against each other AND the fan-out deadlock guard (also `#[serial]`) so
    // cargo's parallel intra-binary threads cannot race a permit-holder against a
    // `set_test_limit`/`reset` that zeros those globals (issue #1594 roborev Low).

    /// The bound: against an isolated `L`-permit semaphore, at most `L` permits
    /// are outstanding at once and the `L+1`-th `admit_with` WAITS until one frees
    /// (queue-full = wait, not error), then proceeds.
    #[tokio::test]
    #[serial]
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
    #[serial]
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
