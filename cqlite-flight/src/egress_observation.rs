//! Feature-independent observation seam for the per-stream egress credit
//! governor (issue #2821).
//!
//! Split out of `egress_credit.rs` (campsite rule, epic #1116): the governor owns
//! the credit *mechanism*, this module owns the counters that make the mechanism
//! observable — charged/resident bytes and their high-water marks, plus the
//! reservation lifecycle events (granted / parked / clamped / materialized) that
//! turn "the bound holds" and "the clamp is not the normal case" into assertions
//! rather than prose.
//!
//! Test-only in intent (issue #2821 adds no OTel metric), but always compiled:
//! the writes are cheap `Relaxed` atomics, exactly like `StreamProbe`'s, so
//! production simply carries a throwaway [`Default`] instance and never reads it.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Feature-independent observation of the credit governor, maintained with cheap
/// `Relaxed` atomics exactly like `StreamProbe::produced_batches`.
///
/// Test-only in intent (no new OTel metric — issue #2821 non-goal), but always
/// compiled so production simply carries a throwaway [`Default`] instance.
#[derive(Clone, Default)]
pub(crate) struct EgressObservation {
    inner: Arc<EgressCounters>,
}

#[derive(Default)]
struct EgressCounters {
    /// Permit bytes currently held (reservations AND charged batches).
    charged: AtomicU64,
    peak_charged: AtomicU64,
    /// REALIZED `get_array_memory_size()` of every materialized batch still on
    /// the egress path. This is the quantity the published bound is about.
    resident: AtomicU64,
    peak_resident: AtomicU64,
    /// Reservations granted (a batch may be materialized only under one).
    reservations_granted: AtomicU64,
    /// Reservations that could not be taken immediately and PARKED on the
    /// exhausted pool — the deterministic "the ceiling is now binding" event.
    reservations_parked: AtomicU64,
    /// Reservations parked on the pool RIGHT NOW — a gauge, not a counter.
    /// Maintained by the RAII [`ParkGuard`], so a reservation future dropped
    /// while parked (a cancelled stream) decrements it too. Read by
    /// `MeteredDoGetStream`'s safety valve as the "the producer cannot proceed"
    /// half of its wedge predicate (issue #2821 review R1).
    ///
    /// Written with `Release` and read with `Acquire` (the only non-`Relaxed`
    /// accesses here) so that a reader observing `> 0` is guaranteed to also see
    /// the matching [`Self::parked_want`] contribution. The safety valve sizes
    /// its release from that pairing, so a torn read of the two gauges would let
    /// it under-release and re-open the wedge it exists to close.
    parked_now: AtomicU64,
    /// Capacity bytes the currently-parked reservation(s) are waiting to acquire
    /// — a gauge paired with [`Self::parked_now`] and maintained by the same
    /// RAII [`ParkGuard`].
    ///
    /// This is what makes the safety valve's release MINIMAL rather than a
    /// blanket drain: the valve releases deferred permits oldest-first only until
    /// the pool's free credit reaches this figure, and stops. A sum (not a max)
    /// because it is a gauge; there is at most one reserver per stream (see
    /// `EgressCredit::acquire`), so the two coincide in practice and the sum is
    /// the conservative direction when they would not.
    parked_want: AtomicU64,
    /// Total capacity bytes the pool can hand out, published once by
    /// `EgressCredit::new` (zero for an unbounded/inert budget).
    ///
    /// Free credit is `pool_total - charged`, which is what lets the safety valve
    /// compute how much it must release instead of releasing one permit and
    /// hoping that was enough.
    pool_total: AtomicU64,
    /// Deferred permits force-released by the safety valve because the stream
    /// was wedged: the producer parked, the channel empty, and every charged
    /// byte held by a batch the CONSUMER is still retaining. Zero on every
    /// normal path — a non-zero count means the governor stopped charging for
    /// consumer-held bytes so the stream could make progress.
    safety_valve_releases: AtomicU64,
    /// Reservations clamped to the whole pool by the deadlock-avoidance clamp
    /// (see [`EgressCredit::reserve`]). A non-zero count means the stream is
    /// running lock-step: the clamped batch is charged for the ENTIRE pool.
    reservations_clamped: AtomicU64,
    /// Batches materialized under a reservation.
    batches_materialized: AtomicU64,
    /// Largest single-batch realized capacity observed on this stream.
    largest_batch: AtomicU64,
    /// Signalled whenever a reservation parks. Lets a test wait for pool
    /// saturation on the EVENT rather than on elapsed time (#2642); `notify_one`
    /// stores a permit when no waiter is registered, so the signal cannot be lost
    /// in the gap between a check and an await.
    parked_signal: tokio::sync::Notify,
    /// The SAME event, on a SEPARATE `Notify` for `MeteredDoGetStream`'s safety
    /// valve. Deliberately not shared with [`Self::parked_signal`]: `notify_one`
    /// wakes exactly one waiter, so a single `Notify` with both a test observer
    /// and the stream registered would have each of them randomly stealing the
    /// other's wakeup — silently converting the valve into a best-effort
    /// mechanism and the saturation helper into a flaky one.
    parked_valve_signal: tokio::sync::Notify,
}

impl EgressObservation {
    pub(crate) fn charge(&self, bytes: u64) {
        let now = self.inner.charged.fetch_add(bytes, Ordering::Relaxed) + bytes;
        self.inner.peak_charged.fetch_max(now, Ordering::Relaxed);
    }

    pub(crate) fn uncharge(&self, bytes: u64) {
        // Saturating by construction: `fetch_update` cannot wrap below zero.
        let _ = self
            .inner
            .charged
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(bytes))
            });
    }

    pub(crate) fn record_reservation(&self) {
        self.inner
            .reservations_granted
            .fetch_add(1, Ordering::Relaxed);
    }

    /// One reservation of `want_bytes` could not be taken immediately and is
    /// about to park.
    ///
    /// Returns an RAII [`ParkGuard`] that must be held for exactly as long as
    /// the reservation is parked: it maintains the `parked_now` and
    /// `parked_want` GAUGES the safety valve reads, and releasing it on `Drop`
    /// is what keeps them correct when a parked reservation future is dropped
    /// rather than completed (a cancelled stream), instead of leaving them stuck
    /// high and making the valve fire on a stream that is not wedged.
    #[must_use = "the park gauges are only correct while the guard is held"]
    pub(crate) fn park(&self, want_bytes: u64) -> ParkGuard {
        self.inner
            .reservations_parked
            .fetch_add(1, Ordering::Relaxed);
        // `parked_want` is raised BEFORE `parked_now`, and `parked_now`'s
        // `Release` pairs with the valve's `Acquire` load: a valve that sees the
        // park at all also sees the amount it must free.
        self.inner
            .parked_want
            .fetch_add(want_bytes, Ordering::Relaxed);
        self.inner.parked_now.fetch_add(1, Ordering::Release);
        // Notify AFTER the gauges are raised, so a waiter woken by either signal
        // observes `parked_now > 0` (the safety valve's wedge predicate).
        self.inner.parked_signal.notify_one();
        self.inner.parked_valve_signal.notify_one();
        ParkGuard {
            obs: self.clone(),
            want_bytes,
        }
    }

    /// Publish the pool's total capacity, once, at pool construction.
    pub(crate) fn set_pool_total_bytes(&self, bytes: u64) {
        self.inner.pool_total.store(bytes, Ordering::Relaxed);
    }

    /// Count one deferred permit force-released by the stream's safety valve.
    pub(crate) fn record_safety_valve_release(&self) {
        self.inner
            .safety_valve_releases
            .fetch_add(1, Ordering::Relaxed);
    }

    /// One reservation was clamped to the pool total (deadlock avoidance).
    pub(crate) fn record_clamped(&self) {
        self.inner
            .reservations_clamped
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_materialized(&self, actual: u64) {
        self.inner
            .batches_materialized
            .fetch_add(1, Ordering::Relaxed);
        self.inner
            .largest_batch
            .fetch_max(actual, Ordering::Relaxed);
        let now = self.inner.resident.fetch_add(actual, Ordering::Relaxed) + actual;
        self.inner.peak_resident.fetch_max(now, Ordering::Relaxed);
    }

    pub(crate) fn release_resident(&self, actual: u64) {
        let _ = self
            .inner
            .resident
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(actual))
            });
    }
}

/// Read side of the observation seam. Like `StreamProbe`'s counters these are
/// maintained unconditionally (the writes are cheap `Relaxed` atomics) but only
/// READ by tests — production carries a throwaway instance and never inspects it,
/// so the readers are `allow(dead_code)` outside `cfg(test)` rather than
/// `cfg(test)`-gated (which would make the seam itself conditional).
#[cfg_attr(not(test), allow(dead_code))]
impl EgressObservation {
    /// Permit bytes currently held by live reservations/permits.
    pub(crate) fn charged_bytes(&self) -> u64 {
        self.inner.charged.load(Ordering::Relaxed)
    }

    /// High-water mark of [`Self::charged_bytes`].
    pub(crate) fn peak_charged_bytes(&self) -> u64 {
        self.inner.peak_charged.load(Ordering::Relaxed)
    }

    /// Realized capacity bytes of materialized batches currently on the egress
    /// path (producer → channel → the stream's deferred slot).
    pub(crate) fn resident_capacity_bytes(&self) -> u64 {
        self.inner.resident.load(Ordering::Relaxed)
    }

    /// High-water mark of [`Self::resident_capacity_bytes`] — the quantity the
    /// `max(ceiling, one maximum batch)` contract bounds.
    pub(crate) fn peak_resident_capacity_bytes(&self) -> u64 {
        self.inner.peak_resident.load(Ordering::Relaxed)
    }

    /// Reservations granted over the stream's lifetime.
    pub(crate) fn reservations_granted(&self) -> u64 {
        self.inner.reservations_granted.load(Ordering::Relaxed)
    }

    /// Reservations that parked on an exhausted pool — i.e. the number of times
    /// the byte ceiling actually applied backpressure to the producer.
    pub(crate) fn reservations_parked(&self) -> u64 {
        self.inner.reservations_parked.load(Ordering::Relaxed)
    }

    /// Reservations parked on the pool AT THIS INSTANT (gauge). The "producer
    /// cannot proceed" half of the safety valve's wedge predicate.
    ///
    /// `Acquire`, pairing with [`Self::park`]'s `Release`: a caller that sees a
    /// park must also see that park's [`Self::parked_want_bytes`] contribution.
    pub(crate) fn parked_now(&self) -> u64 {
        self.inner.parked_now.load(Ordering::Acquire)
    }

    /// Capacity bytes the parked reservation(s) are waiting for (gauge). Zero
    /// when nothing is parked. The safety valve releases only enough deferred
    /// credit to reach this figure.
    pub(crate) fn parked_want_bytes(&self) -> u64 {
        self.inner.parked_want.load(Ordering::Relaxed)
    }

    /// Total capacity bytes the pool can hand out; zero for an unbounded budget.
    pub(crate) fn pool_total_bytes(&self) -> u64 {
        self.inner.pool_total.load(Ordering::Relaxed)
    }

    /// Deferred permits force-released by the stream's safety valve. ZERO is the
    /// healthy state: every normal consumer (including `FlightDataEncoder`)
    /// drops batch N before batch N+1 exists, so the valve never fires.
    pub(crate) fn safety_valve_releases(&self) -> u64 {
        self.inner.safety_valve_releases.load(Ordering::Relaxed)
    }

    /// Reservations clamped to the whole pool (deadlock avoidance). ZERO is the
    /// healthy state at the shipped defaults: a clamped batch runs the stream
    /// lock-step, so this counter is the regression guard for a ceiling that is
    /// too small for one worst-case reservation.
    pub(crate) fn reservations_clamped(&self) -> u64 {
        self.inner.reservations_clamped.load(Ordering::Relaxed)
    }

    /// Resolves the next time a reservation parks on the exhausted pool.
    ///
    /// The deterministic saturation signal for tests: waiting for THIS instead of
    /// for a duration is what keeps the slow-consumer assertions free of a
    /// wall-clock threshold (#2642) and non-vacuous (the sample is taken with the
    /// producer provably pressed against the ceiling, not merely "after a yield").
    pub(crate) async fn parked(&self) {
        self.inner.parked_signal.notified().await
    }

    /// The same signal as [`Self::parked`], as an OWNED future.
    ///
    /// `MeteredDoGetStream` must register for this wakeup *before* it returns
    /// `Pending`, or the safety valve loses a race it cannot recover from: if
    /// the producer parks in the window between the stream's wedge check and
    /// its `Pending` return, nothing would ever poll the stream again. `Notify`
    /// stores a permit when no waiter is registered, so a park on either side of
    /// the window wakes the stream exactly once either way.
    pub(crate) fn parked_owned(&self) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move { inner.parked_valve_signal.notified().await })
    }

    /// Batches materialized over the stream's lifetime. Can never exceed
    /// [`Self::reservations_granted`] — that is the reserve-before-materialize
    /// property, observable rather than merely asserted in prose.
    pub(crate) fn batches_materialized(&self) -> u64 {
        self.inner.batches_materialized.load(Ordering::Relaxed)
    }

    /// Largest single-batch realized capacity observed on this stream.
    pub(crate) fn largest_batch_capacity_bytes(&self) -> u64 {
        self.inner.largest_batch.load(Ordering::Relaxed)
    }
}

/// RAII marker that one reservation is parked on the exhausted credit pool.
///
/// Held for exactly the duration of the semaphore await in
/// [`crate::egress_credit::EgressCredit::reserve`], so the `parked_now` gauge is
/// correct on BOTH exits: a reservation that eventually acquires its permits,
/// and one whose future is dropped mid-park because the stream was cancelled.
/// A gauge maintained by paired increment/decrement calls would leak on the
/// second path and leave the stream's safety valve believing a departed producer
/// is still parked.
pub(crate) struct ParkGuard {
    obs: EgressObservation,
    /// The amount this park contributed to `parked_want`, returned on `Drop` so
    /// the two gauges fall together.
    want_bytes: u64,
}

impl Drop for ParkGuard {
    fn drop(&mut self) {
        // Lowered in the MIRROR of `park`'s order (`parked_now` first): a valve
        // that races this drop then reads a stale `parked_now > 0` alongside an
        // already-cleared `parked_want`, which makes it release NOTHING — the
        // safe direction, since a departed producer needs no unwedging.
        //
        // Saturating by construction: `fetch_update` cannot wrap below zero.
        let _ = self
            .obs
            .inner
            .parked_now
            .fetch_update(Ordering::Release, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(1))
            });
        let _ =
            self.obs
                .inner
                .parked_want
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                    Some(v.saturating_sub(self.want_bytes))
                });
    }
}
