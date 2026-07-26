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

    /// One reservation could not be taken immediately and is about to park.
    pub(crate) fn record_parked(&self) {
        self.inner
            .reservations_parked
            .fetch_add(1, Ordering::Relaxed);
        self.inner.parked_signal.notify_one();
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
