//! Per-stream in-flight egress **capacity**-byte credit governor (issue #2821).
//!
//! # What this bounds
//!
//! Before this module, a streaming `do_get` was bounded only by a batch COUNT:
//! `DO_GET_CHANNEL_CAPACITY` (4) plus ~2 more in flight, each of `batch_size`
//! rows — a row count multiplied by an unbounded row width. Issue #2825 made ONE
//! batch finite; this module makes the number of resident BYTES finite.
//!
//! ```text
//! Guaranteed contract:
//!     peak SERVER-SIDE in-flight egress CAPACITY <= max(ceiling, one maximum batch)
//! ```
//!
//! At the merged defaults, in capacity currency:
//!
//! ```text
//! one maximum batch = worst_case_batch_capacity_bytes(DEFAULT_MAX_BATCH_BYTES, nodes, 0)
//!                   = 2 * 4 MiB + 2 KiB * nodes  =  8 MiB + ~2n KiB
//! contract          = max(12 MiB, 8 MiB + ~2n KiB) = 12 MiB  <=  16 MiB (B4)
//! ```
//!
//! # WHAT the bound is over: SERVER-SIDE residency
//!
//! The governed quantity is the Arrow capacity bytes **the server holds** on the
//! egress path: rows being materialized, batches queued in the `do_get` channel,
//! and batches yielded downstream that the consumer has **not yet dropped**.
//!
//! It is **NOT** a bound on total resident bytes including consumer-held
//! batches. Once a consumer takes a batch and keeps it, those bytes are the
//! CONSUMER's memory: the server cannot free them, cannot reuse them, and cannot
//! make any decision about them — so the governor stops charging for them rather
//! than metering something it no longer controls. Concretely, a consumer that
//! retains every batch it is handed will accumulate arbitrarily much Arrow data
//! in its own heap; that is its own budget, and this ceiling neither claims nor
//! could enforce anything about it.
//!
//! That is not a weakening of the guarantee — it is the guarantee's actual
//! subject, and it is what makes the stream unwedgeable. Charging a
//! consumer-retained batch against the server's pool would let a consumer that
//! holds batch N while awaiting N+1 deadlock the stream; releasing it is exactly
//! `MeteredDoGetStream::open_safety_valve`, which fires ONLY in that state (the
//! producer parked, the channel empty, the whole charge held by retained
//! batches) and never on the ordinary path, where `FlightDataEncoder` drops each
//! batch before asking for the next.
//!
//! The 12 MiB ceiling and the B4 ≤16Mi composition below are stated over exactly
//! this quantity.
//!
//! It is a `max`, not a sum: under reserve-before-materialize every resident
//! `RecordBatch` on the egress path holds credit, so their summed charged
//! capacity cannot exceed the pool; the only way realized bytes exceed the pool
//! is [`the deadlock-avoidance clamp`](EgressCredit::reserve), and a clamped
//! batch holds the ENTIRE pool, so nothing else is resident beside it.
//!
//! # TWO CURRENCIES — never mix them
//!
//! | Currency | Definition | Owner |
//! |---|---|---|
//! | **payload** | sum of Arrow buffer *lengths* (`cqlite_core::export::arrow_payload_bytes`) | issue #2825's per-batch cap, [`crate::batch_bytes::DEFAULT_MAX_BATCH_BYTES`] (4 MiB) |
//! | **capacity** | `RecordBatch::get_array_memory_size()` — buffer *capacities* | THIS ceiling, and the metering `MeteredDoGetStream` already does |
//!
//! Adding a payload figure to a capacity figure is not a bound. Every conversion
//! here goes through [`crate::batch_bytes::worst_case_batch_capacity_bytes`] —
//! never a locally re-derived factor, and never a bare
//! `payload × BATCH_BYTES_CAPACITY_FACTOR`, which UNDER-states capacity by
//! `BATCH_BYTES_PER_COLUMN_SLACK × n_array_nodes` (see
//! [`count_arrow_array_nodes`]).
//!
//! # Residency OUTSIDE the governed set (named, not implied away)
//!
//! 1. **The producer's row buffer.** Since issue #3552 this is an
//!    `ArrowRowAccumulator`: the rows' projected cells, held COLUMN-MAJOR as
//!    `n_cols` `Vec<Option<Value>>` stores (plus one staging row), rather than a
//!    `Vec<QueryRow>` of per-row hash maps. The bound is unchanged in kind — ≤
//!    `batch_size` rows or one byte-cap's worth of payload, plus per-value Rust
//!    overhead — and the accumulator holds only the PROJECTED cells, dropping each
//!    row's map, key and metadata at push time. The stores are NOT pre-sized: they
//!    GROW to the batch's row count and then keep that capacity across batches
//!    (`clear` retains it), so the steady-state high-water mark is one batch's
//!    worth of cells and is reached by growth, never reserved up front (issue
//!    #3552 review B1). Resident while rows accumulate, while the producer is
//!    parked on a reservation, and during materialization (buffer and batch
//!    overlap until the accumulator is cleared). Not a `RecordBatch`, not visible
//!    to `get_array_memory_size()`. PRE-EXISTING and outside the governed set.
//! 2. **A single row wider than #2825's per-batch cap**, delivered as a one-row
//!    batch at its own natural width (`worst_case_batch_capacity_bytes`'s
//!    `max(cap, widest_row_payload)` term). A property of the data.
//! 3. **The aggregate route** (`MergeProducer::aggregate_paths`), which
//!    materializes its bounded per-group output into a `Vec` and reaches the wire
//!    through `futures::stream::iter` — it never touches a
//!    [`crate::producer::BatchSink`], so no reservation applies. Bounded by GROUP
//!    count by construction; an explicit non-goal of issue #2821.
//! 4. **The Flight encoder's `FlightData` queue**, downstream of the credit
//!    boundary: `FlightDataEncoder` encodes one `RecordBatch` into queued
//!    protobuf messages and drops the batch. PRE-EXISTING and unchanged.
//! 5. **Batches a consumer chooses to RETAIN** after they have been yielded —
//!    consumer-side residency by definition, and released from the pool by the
//!    safety valve when retaining them would otherwise wedge the stream. See
//!    "WHAT the bound is over" above.
//!
//! Note what is NOT on that list: a parked producer holding a materialized but
//! uncharged batch. Reserving before materializing eliminated that term rather
//! than documenting it.

use std::sync::Arc;

use arrow::datatypes::{DataType, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub(crate) use crate::egress_observation::EgressObservation;

/// Default per-stream in-flight egress ceiling: **12 MiB of CAPACITY bytes**.
///
/// # What it bounds: SERVER-SIDE residency
///
/// 12 MiB is the ceiling on the Arrow capacity bytes **the server holds** on one
/// stream's egress path — rows being materialized, batches queued in the `do_get`
/// channel, and yielded batches the consumer has not yet dropped. It is **not** a
/// bound on total resident bytes including consumer-held batches: a batch a
/// consumer takes and retains is the consumer's memory, so the governor stops
/// charging for bytes it no longer controls (see the module documentation's
/// "WHAT the bound is over", and `MeteredDoGetStream::open_safety_valve`, which
/// is what makes retaining safe rather than a hang). Every figure below is
/// stated over exactly that quantity.
///
/// # Why not 8 MiB — admission is gated by the RESERVATION, not the realized size
///
/// A reservation is taken BEFORE the batch exists, so it is the full published
/// worst case for the buffered payload — and it is that figure, not the
/// trued-down realized one, that must fit in the pool or the deadlock-avoidance
/// clamp ([`EgressCredit::reserve`]) engages. At the merged defaults over a small
/// flat three-node schema:
///
/// ```text
/// one worst-case reservation
///   = worst_case_batch_capacity_bytes(DEFAULT_MAX_BATCH_BYTES, 3, 0)
///   = 2 × 4 MiB + 2 KiB × 3 = 8,394,752 B   -> permits_for(..) = 8198
/// an 8 MiB pool                             ->  8 MiB / 1 KiB  = 8192 permits
/// 8198 > 8192  =>  EVERY byte-cap-cut batch would clamp to the WHOLE pool
/// ```
///
/// A clamped reservation holds the entire pool, so the producer cannot begin
/// materializing batch N+1 until batch N has completely left the stream: strict
/// lock-step with the 4-deep batch-count channel as dead weight — the exact
/// outcome design D4a rejected 6 MiB to avoid, on exactly the wide-row workload
/// this ceiling exists for. The 8 MiB default missed it by precisely the
/// `BATCH_BYTES_PER_COLUMN_SLACK × n_array_nodes` term (6 KiB at three nodes).
///
/// # What 12 MiB actually buys — stated honestly
///
/// ```text
/// 12 MiB = 12,582,912 B -> 12288 permits  >=  8198  =>  no clamp
/// contract = max(12 MiB, 8,394,752 B) = 12 MiB  <=  16 MiB (B4)
/// ```
///
/// **What the `<= 16 MiB` comparison does and does NOT say.** Both sides are
/// read in GOVERNED EGRESS CAPACITY only — the quantity this pool meters. Two
/// server-side terms on the same query are real but sit OUTSIDE this accounting
/// and are not deducted above: the producer's row buffer — since issue #3552 an
/// `ArrowRowAccumulator` holding one batch's PROJECTED cells column-major, grown
/// to the batch's row count rather than pre-reserved, and resident alongside the
/// batch until it is cleared — and the encoder's
/// queued `FlightData` (an encoded copy of up to one batch's payload, ~4 MiB at
/// defaults). So the headroom between 12 MiB and B4's 16 MiB is NOT free space
/// to spend — it is where those terms live. Do not restate this line as a total
/// per-query working-set bound (roborev job 12 F3).
///
/// * **Guaranteed**: a single worst-case reservation fits without clamping for
///   every schema of at most **2048 Arrow array nodes** — `permits_for` of a
///   full-cap reservation is `8192 + 2 × n_array_nodes`, so the clamp engages
///   from `n_array_nodes ≥ 2049` (a very wide or deeply nested projection; a
///   `map<text,text>` column is four nodes). At that width the documented clamp
///   behaviour takes over: the batch acquires the whole pool, is delivered, and
///   is the only thing resident — correct, just lock-step.
/// * **Workload-dependent, NOT guaranteed**: after the true-down to the realized
///   `get_array_memory_size()` (measured capacity/payload factor 1.0–1.8 ⇒
///   4–7.2 MiB for a full 4 MiB payload batch), the residual pool can often admit
///   a second reservation, so the stream typically overlaps two batches. Whether
///   it does depends on the shape's realized factor: at 1.8× one resident batch
///   leaves 4.8 MiB, under the 8.2 MiB a second full reservation asks for, and
///   the producer parks until the first batch drains. No claim is made that two
///   full-size batches are always in flight.
/// * Narrow shapes are unaffected either way — the 4-deep batch-count channel
///   binds first there (a 192 KiB narrow batch is ~64 to the pool).
///
/// `max(12 MiB, 8 MiB + ~2n KiB) ≤ 16 MiB` — the GOVERNED EGRESS CAPACITY is
/// inside the ratified **B4 ≤16Mi per-query working set at concurrency 1**
/// (the row buffer and the encoder's queued `FlightData` are additional
/// server-side terms outside this figure, per the note above), asserted from the imported
/// constants by `egress_credit_tests::composition_stays_inside_b4`, with the
/// no-clamp property pinned by
/// `egress_credit_tests::a_worst_case_default_reservation_does_not_clamp`.
pub const DEFAULT_MAX_INFLIGHT_EGRESS_BYTES: usize = 12 * 1024 * 1024;

/// Environment variable backing `--max-inflight-egress-bytes`.
pub const ENV_MAX_INFLIGHT_EGRESS_BYTES: &str = "CQLITE_MAX_INFLIGHT_EGRESS_BYTES";

/// Accounting quantum for one semaphore permit, in capacity bytes.
///
/// A `tokio::sync::Semaphore` counts permits, so the ceiling is expressed as
/// `ceil(ceiling / QUANTUM)` permits — 12 MiB is 12288 permits, comfortably inside
/// `Semaphore::MAX_PERMITS`. Every conversion rounds **UP**, so the quantisation
/// is always conservative (a stream is charged at least what it holds).
pub const EGRESS_CREDIT_QUANTUM_BYTES: usize = 1024;

/// The per-stream egress ceiling, or an explicit opt-out.
///
/// Like #2825's byte-cap and unlike admission `K`, this is ON by default on every
/// service construction path: a byte credit can only ever *delay* a producer,
/// never turn a working query into an error, so an unbounded egress stream is a
/// memory hazard rather than a policy choice. An embedder that wants the
/// pre-#2821 structural (batch-count) bound opts out explicitly with
/// [`EgressBudget::unbounded`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EgressBudget {
    /// Bound in-flight egress to this many **capacity** bytes per stream.
    Bounded(usize),
    /// No byte ceiling — residency reverts to the batch-count channel bound.
    Unbounded,
}

impl EgressBudget {
    /// A ceiling of `bytes` capacity bytes.
    pub fn bytes(bytes: usize) -> Self {
        Self::Bounded(bytes)
    }

    /// No byte ceiling (embedder opt-out).
    pub fn unbounded() -> Self {
        Self::Unbounded
    }

    /// The configured ceiling, or `None` when unbounded.
    pub fn ceiling_bytes(self) -> Option<usize> {
        match self {
            Self::Bounded(bytes) => Some(bytes),
            Self::Unbounded => None,
        }
    }
}

impl Default for EgressBudget {
    fn default() -> Self {
        Self::Bounded(DEFAULT_MAX_INFLIGHT_EGRESS_BYTES)
    }
}

/// The realized capacity of a materialized batch exceeded the credit reserved
/// for it before it was built.
///
/// This is a VIOLATED INVARIANT, not a soft accounting event — see
/// [`EgressReservation::materialize`]. Acquiring the difference could block
/// behind the pool and deadlock, and ignoring it would silently break the memory
/// bound this module publishes, so the stream fails closed instead.
#[derive(Debug, thiserror::Error)]
#[error(
    "egress credit invariant violated: realized batch capacity {actual} B exceeds the \
     pre-materialization reservation {reserved} B — the estimator-conservatism contract \
     (Sum estimate_arrow_row_bytes >= arrow_payload_bytes, cqlite-core/src/export/arrow_size.rs) \
     or the published payload->capacity conversion no longer holds"
)]
pub struct EgressCreditInvariant {
    /// Capacity bytes reserved before materialization.
    pub reserved: usize,
    /// Capacity bytes the materialized batch actually reports.
    pub actual: usize,
}

/// The credit pool could not charge a reservation, so no batch may be built
/// under it.
///
/// A `tokio::sync::Semaphore` acquire fails ONLY on a closed semaphore, and this
/// pool is never closed today (it lives exactly as long as its stream). It is
/// surfaced as a terminal error anyway, in the same shape as
/// [`EgressCreditInvariant`]: degrading to an UNCHARGED reservation would let a
/// batch reach the egress path with zero credit — silently voiding the memory
/// bound this module publishes — which is the opposite of the fail-closed posture
/// every other branch here takes. Failing the stream is recoverable and visible;
/// an unbounded stream is neither.
#[derive(Debug, thiserror::Error)]
#[error(
    "egress credit pool unavailable: a reservation of {requested} B ({permits} permits) could not \
     be charged because the per-stream credit pool is closed — the stream fails closed rather than \
     putting an uncharged batch on the egress path (issue #2821)"
)]
pub struct EgressCreditUnavailable {
    /// Capacity bytes the reservation asked for.
    pub requested: usize,
    /// Permits the (possibly clamped) acquire attempted to take.
    pub permits: u32,
}

/// Permits needed for `bytes`, rounding UP (conservative) and saturating.
///
/// The `u32` narrowing saturates rather than wrapping: on a 64-bit target a
/// `usize::MAX` reservation would need more permits than `u32` can hold, and
/// `u32::MAX` is far above `Semaphore::MAX_PERMITS`, so such a request is clamped
/// to the pool total by the caller — never silently reduced by a wrap.
fn permits_for(bytes: usize) -> u32 {
    let permits = bytes.div_ceil(EGRESS_CREDIT_QUANTUM_BYTES);
    u32::try_from(permits).unwrap_or(u32::MAX)
}

/// Bytes accounted for `permits` permits, saturating.
fn bytes_for(permits: u32) -> u64 {
    u64::from(permits).saturating_mul(EGRESS_CREDIT_QUANTUM_BYTES as u64)
}

// ---------------------------------------------------------------------------
// The credit pool
// ---------------------------------------------------------------------------

/// A per-stream pool of egress capacity credit, cheap to clone (the pool is
/// shared behind an `Arc`).
#[derive(Clone)]
pub(crate) struct EgressCredit {
    /// `None` when the budget is unbounded — reservations are then inert.
    sem: Option<Arc<Semaphore>>,
    /// Total permits in the pool; the deadlock-avoidance clamp ceiling.
    total_permits: u32,
    obs: EgressObservation,
}

impl EgressCredit {
    /// Build a pool for `budget`, publishing its accounting through `obs`.
    ///
    /// A bounded pool always holds at least ONE permit: a configured ceiling of
    /// `0` (or anything under one quantum) degrades to strict one-batch-at-a-time
    /// egress rather than either wedging or silently becoming unbounded.
    pub(crate) fn new(budget: EgressBudget, obs: EgressObservation) -> Self {
        match budget.ceiling_bytes() {
            Some(ceiling) => {
                let total_permits = permits_for(ceiling).max(1);
                // Publish the pool size so the stream's safety valve can compute
                // FREE credit (`pool_total - charged`) and release the minimum
                // that unblocks a parked producer, instead of guessing.
                obs.set_pool_total_bytes(bytes_for(total_permits));
                Self {
                    sem: Some(Arc::new(Semaphore::new(total_permits as usize))),
                    total_permits,
                    obs,
                }
            }
            None => {
                obs.set_pool_total_bytes(0);
                Self {
                    sem: None,
                    total_permits: 0,
                    obs,
                }
            }
        }
    }

    /// Reserve credit for a batch that will report at most `capacity_bytes` of
    /// `get_array_memory_size()`, BEFORE it is materialized.
    ///
    /// **Deadlock-avoidance clamp.** A single reservation may be larger than the
    /// entire configured ceiling — an operator may configure a small ceiling, a
    /// row may be wider than the whole per-batch cap, or a projection may carry
    /// enough array nodes that the slack term alone pushes past the pool (from
    /// `n_array_nodes ≥ 2049` at the shipped defaults — `8192 + 2 × nodes`
    /// permits wanted against a 12288-permit pool; see
    /// [`DEFAULT_MAX_INFLIGHT_EGRESS_BYTES`]). Acquiring `n` permits from a pool
    /// of `N < n` would block forever, wedging the stream and hanging the client.
    /// The reservation is therefore clamped to the pool total: when everything
    /// else has drained, an oversized batch acquires the WHOLE pool and proceeds.
    /// Progress is guaranteed for a batch of any size — which is precisely why
    /// the honest contract is `max(ceiling, one maximum batch)` and not
    /// `ceiling`.
    ///
    /// A clamp is not free: the clamped batch holds the entire pool, so the
    /// stream runs strictly lock-step while it is resident. Every clamp is
    /// counted ([`EgressObservation::reservations_clamped`]) precisely so
    /// "the default ceiling makes the clamp routine" is a test-detectable
    /// regression rather than an invisible throughput cliff.
    ///
    /// Parking here is safe on the caller's `spawn_blocking` thread; the caller
    /// races this future against the shared cancel flag (see
    /// `ChannelSink::reserve`) so a client disconnect wakes a producer parked on
    /// credit exactly as it wakes one parked on a full channel.
    pub(crate) async fn reserve(
        &self,
        capacity_bytes: usize,
    ) -> Result<EgressReservation, EgressCreditUnavailable> {
        let want = permits_for(capacity_bytes);
        let take = want.min(self.total_permits);
        let permit = match &self.sem {
            Some(sem) => {
                if take < want {
                    self.obs.record_clamped();
                }
                Some(self.acquire(sem, take, capacity_bytes).await?)
            }
            None => None,
        };
        let charged = match &permit {
            Some(_) => bytes_for(take),
            None => 0,
        };
        self.obs.charge(charged);
        self.obs.record_reservation();
        Ok(EgressReservation {
            permit: EgressPermit {
                permit,
                charged_bytes: charged,
                resident_bytes: 0,
                obs: Some(self.obs.clone()),
            },
            reserved_bytes: capacity_bytes,
        })
    }

    /// Take `take` permits, recording the PARK when they are not immediately
    /// available and failing closed when the pool is closed.
    ///
    /// The `try_acquire_many_owned` probe first is not an optimisation: it is what
    /// makes "the producer is now pressed against the ceiling" an observable event
    /// rather than an inference from elapsed time. There is at most one reserver
    /// per stream, so `try_acquire`'s ability to barge ahead of a queued waiter
    /// cannot starve anyone here.
    async fn acquire(
        &self,
        sem: &Arc<Semaphore>,
        take: u32,
        capacity_bytes: usize,
    ) -> Result<OwnedSemaphorePermit, EgressCreditUnavailable> {
        let closed = || EgressCreditUnavailable {
            requested: capacity_bytes,
            permits: take,
        };
        match Arc::clone(sem).try_acquire_many_owned(take) {
            Ok(permit) => Ok(permit),
            Err(tokio::sync::TryAcquireError::NoPermits) => {
                // RAII: the `parked_now`/`parked_want` gauges the stream's safety
                // valve reads must fall again whether this future completes OR is
                // dropped mid-park by a cancelled stream. The want is the CLAMPED
                // permit count (what this acquire actually asks the pool for), so
                // the valve sizes its release against a figure the pool can
                // always satisfy.
                let _park = self.obs.park(bytes_for(take));
                Arc::clone(sem)
                    .acquire_many_owned(take)
                    .await
                    .map_err(|_| closed())
            }
            // Fail CLOSED (not an uncharged reservation): see
            // [`EgressCreditUnavailable`].
            Err(tokio::sync::TryAcquireError::Closed) => Err(closed()),
        }
    }

    /// Close the pool, so every subsequent reservation fails closed. Test-only:
    /// production never closes a live stream's pool, which is exactly why the
    /// closed branch needs an explicit test rather than an assumption.
    #[cfg(test)]
    pub(crate) fn close_for_test(&self) {
        if let Some(sem) = &self.sem {
            sem.close();
        }
    }
}

// ---------------------------------------------------------------------------
// Reservation → permit
// ---------------------------------------------------------------------------

/// Credit held for a batch that has NOT been materialized yet.
///
/// Consumed by [`Self::materialize`] the instant the batch exists, which trues
/// the reservation DOWN to the realized capacity and hands back the
/// [`EgressPermit`] that then rides with the batch.
pub(crate) struct EgressReservation {
    permit: EgressPermit,
    /// The UNCLAMPED reservation — the invariant reference for the fail-closed
    /// check. Deliberately not the clamped permit count: the invariant under test
    /// is the estimator's conservatism, not the deadlock clamp.
    reserved_bytes: usize,
}

impl EgressReservation {
    /// A no-op reservation for a sink with no egress residency to govern (the
    /// collect/parity path). Requires no Tokio runtime, holds no permit, and can
    /// never fail closed — so the collect path stays byte-identical.
    pub(crate) fn inert() -> Self {
        Self {
            permit: EgressPermit::inert(),
            reserved_bytes: usize::MAX,
        }
    }

    /// Account the batch that was just built under this reservation and hand
    /// back the permit that will ride with it.
    ///
    /// **Trues up DOWNWARD, never upward.** `estimate_arrow_row_bytes` is
    /// deliberately conservative (measured over-shoot 1.18–3× depending on shape)
    /// and the payload→capacity conversion doubles that again, so holding the
    /// full reservation for the batch's whole channel residency would let one
    /// batch pin the pool and collapse the stream to lock-step. Releasing
    /// `reserved − actual` the instant the batch exists confines the
    /// over-reservation to the materialization window.
    ///
    /// **Fails closed when `actual > reserved`.** That is a violated invariant,
    /// not an accounting rounding: quietly acquiring the difference could block
    /// behind the pool and deadlock, and ignoring it would silently break the
    /// published bound. The permit drops on the normal path (releasing its
    /// credit) and no batch is emitted on a false account.
    pub(crate) fn materialize(
        mut self,
        actual_capacity_bytes: usize,
    ) -> Result<EgressPermit, EgressCreditInvariant> {
        if actual_capacity_bytes > self.reserved_bytes {
            // Loud, but NOT a `debug_assert!`: the fail-closed path is itself
            // covered by a test, and a debug-only abort would turn that coverage
            // into a panic while leaving release builds silent.
            tracing::error!(
                reserved = self.reserved_bytes,
                actual = actual_capacity_bytes,
                "egress credit invariant violated: realized batch capacity exceeds its \
                 pre-materialization reservation (issue #2821 / #2825 estimator conservatism)"
            );
            return Err(EgressCreditInvariant {
                reserved: self.reserved_bytes,
                actual: actual_capacity_bytes,
            });
        }
        self.permit.true_up_down(actual_capacity_bytes);
        Ok(self.permit)
    }
}

/// An owned unit of egress credit that travels WITH its batch.
///
/// Release is `Drop`, so every termination path is covered by construction —
/// normal drain, a dropped receiver with batches still queued, a cancellation
/// while parked, a producer error, a panic unwind. There is no re-measurement at
/// the drain side that could drift the pool: the permit carries exactly the
/// amount that was charged.
pub(crate) struct EgressPermit {
    permit: Option<OwnedSemaphorePermit>,
    charged_bytes: u64,
    resident_bytes: u64,
    /// The observation seam this permit reports through, or `None` for an INERT
    /// permit — one issued outside the governed set (the collect sink and the
    /// aggregate route). `None`, not a throwaway [`EgressObservation`]: an inert
    /// permit must account for nothing on BOTH inert routes, and an observation
    /// nobody can read is indistinguishable from a real one that was silently
    /// dropped. It also keeps the inert path allocation-free (no per-batch
    /// `Arc<EgressCounters>`).
    ///
    /// Note this is NOT the same as `permit: None`: an explicitly UNBOUNDED
    /// budget (`EgressBudget::unbounded`) holds no semaphore permit but is still
    /// observed, so residency stays visible where it is merely un-governed.
    obs: Option<EgressObservation>,
}

impl EgressPermit {
    /// A permit charging nothing and observing nothing — the aggregate route and
    /// the collect sink, which are outside the governed set by construction.
    pub(crate) fn inert() -> Self {
        Self {
            permit: None,
            charged_bytes: 0,
            resident_bytes: 0,
            obs: None,
        }
    }

    /// Release the difference between the reservation and the realized capacity.
    fn true_up_down(&mut self, actual: usize) {
        let Some(obs) = self.obs.as_ref() else {
            // Inert: no credit to return, and nothing to record. Leaving
            // `resident_bytes` at zero keeps the two inert routes
            // (`EgressReservation::inert().materialize(..)` and
            // `CreditedBatch::uncredited`, which never materializes at all)
            // byte-identical in what they account for.
            return;
        };
        if let Some(permit) = self.permit.as_mut() {
            let held = u32::try_from(permit.num_permits()).unwrap_or(u32::MAX);
            // Never below what the batch actually occupies, never above what is
            // held (a clamped reservation may hold less than the batch needs).
            let keep = permits_for(actual).min(held);
            let release = held - keep;
            if release > 0 {
                if let Some(excess) = permit.split(release as usize) {
                    drop(excess);
                    let released = bytes_for(release);
                    self.charged_bytes = self.charged_bytes.saturating_sub(released);
                    obs.uncharge(released);
                }
            }
        }
        self.resident_bytes = actual as u64;
        obs.record_materialized(self.resident_bytes);
    }

    /// Capacity bytes this permit currently charges against the pool.
    ///
    /// Read by `MeteredDoGetStream`'s safety valve to decide whether the pool is
    /// held ENTIRELY by deferred (consumer-retained) batches — the half of the
    /// wedge predicate that distinguishes "the producer is waiting for the
    /// consumer to drop data" from "the producer is waiting for the channel".
    pub(crate) fn charged_bytes(&self) -> u64 {
        self.charged_bytes
    }

    /// Realized capacity bytes this permit accounts as resident. Zero for an
    /// inert permit on BOTH inert routes — see [`Self::obs`].
    #[cfg(test)]
    pub(crate) fn resident_bytes(&self) -> u64 {
        self.resident_bytes
    }
}

impl Drop for EgressPermit {
    fn drop(&mut self) {
        if let Some(obs) = self.obs.as_ref() {
            obs.uncharge(self.charged_bytes);
            obs.release_resident(self.resident_bytes);
        }
        // The inner `OwnedSemaphorePermit`'s own `Drop` returns the permits to
        // the pool; nothing else is needed for release.
    }
}

// ---------------------------------------------------------------------------
// The channel element
// ---------------------------------------------------------------------------

/// A materialized batch and the egress credit charged for it.
///
/// Making this the `do_get` channel element is what makes release structural: a
/// queued batch dropped because the receiver went away drops its permit with it,
/// with no hand-audited cleanup path.
pub(crate) struct CreditedBatch {
    batch: RecordBatch,
    permit: EgressPermit,
}

impl CreditedBatch {
    /// Pair a batch with the permit reserved for it before it was built.
    pub(crate) fn new(batch: RecordBatch, permit: EgressPermit) -> Self {
        Self { batch, permit }
    }

    /// A batch that is outside the governed set (the aggregate route, and the
    /// collect sink's already-materialized output).
    pub(crate) fn uncredited(batch: RecordBatch) -> Self {
        Self {
            batch,
            permit: EgressPermit::inert(),
        }
    }

    /// Take the batch, releasing its credit immediately (collect path).
    pub(crate) fn into_batch(self) -> RecordBatch {
        self.batch
    }

    /// Split into the batch and its still-live permit, so the caller can hold
    /// the credit for as long as the batch is resident downstream.
    pub(crate) fn split(self) -> (RecordBatch, EgressPermit) {
        (self.batch, self.permit)
    }
}

// ---------------------------------------------------------------------------
// Arrow array-node counting
// ---------------------------------------------------------------------------

/// Count Arrow array **NODES** over a projected output schema.
///
/// [`crate::batch_bytes::BATCH_BYTES_PER_COLUMN_SLACK`] is denominated per array
/// node, not per output column: a flat scalar column is one node, a `list<text>`
/// is two (the `ListArray` plus its `Utf8` child) and a `map<text,text>` is four
/// (map, entries struct, key `Utf8`, value `Utf8`). Passing a COLUMN count would
/// under-state the fixed per-node allocations on a nested schema and therefore
/// under-reserve, tripping the fail-closed path.
///
/// The walk is iterative (an explicit worklist, no recursion) so a deeply nested
/// type cannot overflow the stack, and the accumulation saturates.
pub(crate) fn count_arrow_array_nodes(schema: &ArrowSchema) -> usize {
    let mut count = 0usize;
    let mut work: Vec<&DataType> = schema.fields().iter().map(|f| f.data_type()).collect();
    while let Some(data_type) = work.pop() {
        count = count.saturating_add(1);
        match data_type {
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::ListView(field)
            | DataType::LargeListView(field)
            | DataType::FixedSizeList(field, _)
            | DataType::Map(field, _) => work.push(field.data_type()),
            DataType::Struct(fields) => work.extend(fields.iter().map(|f| f.data_type())),
            DataType::Union(fields, _) => {
                work.extend(fields.iter().map(|(_, f)| f.data_type()));
            }
            DataType::Dictionary(_, value) => work.push(value.as_ref()),
            DataType::RunEndEncoded(run_ends, values) => {
                work.push(run_ends.data_type());
                work.push(values.data_type());
            }
            _ => {}
        }
    }
    count
}

#[cfg(test)]
#[path = "egress_credit_tests.rs"]
mod egress_credit_tests;
