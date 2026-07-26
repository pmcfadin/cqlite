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
//!     peak charged in-flight egress CAPACITY <= max(ceiling, one maximum batch)
//! ```
//!
//! At the merged defaults, in capacity currency:
//!
//! ```text
//! one maximum batch = worst_case_batch_capacity_bytes(DEFAULT_MAX_BATCH_BYTES, nodes, 0)
//!                   = 2 * 4 MiB + 1 KiB * nodes  =  8 MiB + ~n KiB
//! contract          = max(8 MiB, 8 MiB + ~n KiB) =  8 MiB + ~n KiB  <<  16 MiB (B4)
//! ```
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
//! 1. **The producer's row buffer** (`Vec<QueryRow>`, ≤ `batch_size` rows or one
//!    byte-cap's worth of payload, plus per-value Rust overhead). Resident while
//!    rows accumulate, while the producer is parked on a reservation, and during
//!    materialization (buffer and batch overlap until `buffer.clear()`). Not a
//!    `RecordBatch`, not visible to `get_array_memory_size()`. PRE-EXISTING and
//!    unchanged by this change.
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
//!
//! Note what is NOT on that list: a parked producer holding a materialized but
//! uncharged batch. Reserving before materializing eliminated that term rather
//! than documenting it.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arrow::datatypes::{DataType, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Default per-stream in-flight egress ceiling: **8 MiB of CAPACITY bytes**.
///
/// Derived against the merged #2825 per-batch PAYLOAD cap, in capacity currency:
/// one maximum batch is
/// `worst_case_batch_capacity_bytes(DEFAULT_MAX_BATCH_BYTES, nodes, 0)
/// = BATCH_BYTES_CAPACITY_FACTOR × 4 MiB + 1 KiB × nodes ≈ 8 MiB`. Because the
/// guaranteed bound is `max(ceiling, one maximum batch)`, EVERY ceiling ≤ 8 MiB
/// yields the identical worst case — so a smaller default buys nothing in B4
/// terms while making the deadlock-avoidance clamp the normal case (a worst-case
/// full batch would always take the whole pool and run the wide-row path
/// lock-step with the batch-count channel dead behind it). 8 MiB is therefore the
/// largest value that does not widen the worst case, and it admits ~2 typical
/// batches (measured capacity/payload factor 1.0–1.8) or ~42 narrow-shape ones.
///
/// `max(8 MiB, 8 MiB + ~n KiB) ≤ 16 MiB` — inside the ratified **B4 ≤16Mi
/// per-query working set at concurrency 1**, asserted from the imported
/// constants by `egress_credit_tests::composition_stays_inside_b4`.
pub const DEFAULT_MAX_INFLIGHT_EGRESS_BYTES: usize = 8 * 1024 * 1024;

/// Environment variable backing `--max-inflight-egress-bytes`.
pub const ENV_MAX_INFLIGHT_EGRESS_BYTES: &str = "CQLITE_MAX_INFLIGHT_EGRESS_BYTES";

/// Accounting quantum for one semaphore permit, in capacity bytes.
///
/// A `tokio::sync::Semaphore` counts permits, so the ceiling is expressed as
/// `ceil(ceiling / QUANTUM)` permits — 8 MiB is 8192 permits, comfortably inside
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

/// Permits needed for `bytes`, rounding UP (conservative) and saturating.
fn permits_for(bytes: usize) -> u32 {
    let permits = bytes.div_ceil(EGRESS_CREDIT_QUANTUM_BYTES);
    u32::try_from(permits).unwrap_or(u32::MAX)
}

/// Bytes accounted for `permits` permits, saturating.
fn bytes_for(permits: u32) -> u64 {
    u64::from(permits).saturating_mul(EGRESS_CREDIT_QUANTUM_BYTES as u64)
}

// ---------------------------------------------------------------------------
// Observation seam
// ---------------------------------------------------------------------------

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
    /// Batches materialized under a reservation.
    batches_materialized: AtomicU64,
    /// Largest single-batch realized capacity observed on this stream.
    largest_batch: AtomicU64,
}

impl EgressObservation {
    fn charge(&self, bytes: u64) {
        let now = self.inner.charged.fetch_add(bytes, Ordering::Relaxed) + bytes;
        self.inner.peak_charged.fetch_max(now, Ordering::Relaxed);
    }

    fn uncharge(&self, bytes: u64) {
        // Saturating by construction: `fetch_update` cannot wrap below zero.
        let _ = self
            .inner
            .charged
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(bytes))
            });
    }

    fn record_reservation(&self) {
        self.inner
            .reservations_granted
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_materialized(&self, actual: u64) {
        self.inner
            .batches_materialized
            .fetch_add(1, Ordering::Relaxed);
        self.inner
            .largest_batch
            .fetch_max(actual, Ordering::Relaxed);
        let now = self.inner.resident.fetch_add(actual, Ordering::Relaxed) + actual;
        self.inner.peak_resident.fetch_max(now, Ordering::Relaxed);
    }

    fn release_resident(&self, actual: u64) {
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
                Self {
                    sem: Some(Arc::new(Semaphore::new(total_permits as usize))),
                    total_permits,
                    obs,
                }
            }
            None => Self {
                sem: None,
                total_permits: 0,
                obs,
            },
        }
    }

    /// Reserve credit for a batch that will report at most `capacity_bytes` of
    /// `get_array_memory_size()`, BEFORE it is materialized.
    ///
    /// **Deadlock-avoidance clamp.** A single `RecordBatch` may be larger than
    /// the entire configured ceiling (at the merged defaults a worst-case full
    /// batch is ~8 MiB of capacity, and an operator may configure a far smaller
    /// ceiling). Acquiring `n` permits from a pool of `N < n` would block
    /// forever, wedging the stream and hanging the client. The reservation is
    /// therefore clamped to the pool total: when everything else has drained, an
    /// oversized batch acquires the WHOLE pool and proceeds. Progress is
    /// guaranteed for a batch of any size — which is precisely why the honest
    /// contract is `max(ceiling, one maximum batch)` and not `ceiling`.
    ///
    /// Parking here is safe on the caller's `spawn_blocking` thread; the caller
    /// races this future against the shared cancel flag (see
    /// `ChannelSink::reserve`) so a client disconnect wakes a producer parked on
    /// credit exactly as it wakes one parked on a full channel.
    pub(crate) async fn reserve(&self, capacity_bytes: usize) -> EgressReservation {
        let want = permits_for(capacity_bytes);
        let take = want.min(self.total_permits);
        let permit = match &self.sem {
            Some(sem) => {
                // `acquire_many_owned` errors ONLY on a closed semaphore, and this
                // pool is never closed (it lives as long as the stream). Degrade
                // to an inert permit rather than `unwrap` in library code.
                Arc::clone(sem).acquire_many_owned(take).await.ok()
            }
            None => None,
        };
        let charged = match &permit {
            Some(_) => bytes_for(take),
            None => 0,
        };
        self.obs.charge(charged);
        self.obs.record_reservation();
        EgressReservation {
            permit: EgressPermit {
                permit,
                charged_bytes: charged,
                resident_bytes: 0,
                obs: self.obs.clone(),
            },
            reserved_bytes: capacity_bytes,
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
            permit: EgressPermit {
                permit: None,
                charged_bytes: 0,
                resident_bytes: 0,
                obs: EgressObservation::default(),
            },
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
    obs: EgressObservation,
}

impl EgressPermit {
    /// A permit charging nothing — the aggregate route and the collect sink,
    /// which are outside the governed set by construction.
    pub(crate) fn inert() -> Self {
        Self {
            permit: None,
            charged_bytes: 0,
            resident_bytes: 0,
            obs: EgressObservation::default(),
        }
    }

    /// Release the difference between the reservation and the realized capacity.
    fn true_up_down(&mut self, actual: usize) {
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
                    self.obs.uncharge(released);
                }
            }
        }
        self.resident_bytes = actual as u64;
        self.obs.record_materialized(self.resident_bytes);
    }

    /// Capacity bytes this permit currently charges against the pool.
    #[cfg(test)]
    pub(crate) fn charged_bytes(&self) -> u64 {
        self.charged_bytes
    }
}

impl Drop for EgressPermit {
    fn drop(&mut self) {
        self.obs.uncharge(self.charged_bytes);
        self.obs.release_resident(self.resident_bytes);
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
