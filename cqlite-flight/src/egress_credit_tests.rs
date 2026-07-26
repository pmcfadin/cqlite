//! Unit tests for the egress credit governor (issue #2821).
//!
//! Split out of `egress_credit.rs` (loaded via `#[path]`) to keep the production
//! module under the campsite source threshold — epic #1116/#1135.
//!
//! Every assertion here is on measured BYTES, permit counts or booleans. Nothing
//! compares an elapsed duration against a threshold (#2642): a producer parked on
//! credit is observed by polling its future to `Pending`, never by sleeping.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;

use super::*;
use crate::batch_bytes::{
    worst_case_batch_capacity_bytes, BATCH_BYTES_CAPACITY_FACTOR, BATCH_BYTES_PER_COLUMN_SLACK,
    DEFAULT_MAX_BATCH_BYTES,
};

/// The ratified B4 per-query working set at concurrency 1.
const B4_WORKING_SET_BYTES: usize = 16 * 1024 * 1024;

/// A live pool never refuses a reservation; only a CLOSED one does, and that
/// path has its own dedicated test.
const POOL_OPEN: &str = "a live credit pool must grant the reservation";

fn credit(ceiling: usize) -> (EgressCredit, EgressObservation) {
    let obs = EgressObservation::default();
    (
        EgressCredit::new(EgressBudget::bytes(ceiling), obs.clone()),
        obs,
    )
}

// ---------------------------------------------------------------------------
// Requirement: capacity denomination + the published composition
// ---------------------------------------------------------------------------

/// The guaranteed bound `max(ceiling, one maximum batch)` composes inside B4,
/// computed from the IMPORTED constants — no hard-coded `2`, no hard-coded
/// 8 MiB. A later change to either constant that breaks the composition fails
/// here rather than silently invalidating the doctrine.
#[test]
fn composition_stays_inside_b4() {
    // A small flat schema: `id int, payload blob, label text` is three nodes.
    let n_array_nodes = 3;
    let one_max_batch = worst_case_batch_capacity_bytes(DEFAULT_MAX_BATCH_BYTES, n_array_nodes, 0);
    let contract = DEFAULT_MAX_INFLIGHT_EGRESS_BYTES.max(one_max_batch);
    assert!(
        contract <= B4_WORKING_SET_BYTES,
        "max(ceiling={DEFAULT_MAX_INFLIGHT_EGRESS_BYTES}, one_max_batch={one_max_batch}) \
         = {contract} exceeds the ratified B4 working set {B4_WORKING_SET_BYTES}"
    );
    // Non-vacuity: the derivation must actually be the published one.
    assert_eq!(
        one_max_batch,
        DEFAULT_MAX_BATCH_BYTES * BATCH_BYTES_CAPACITY_FACTOR
            + BATCH_BYTES_PER_COLUMN_SLACK * n_array_nodes
    );
}

/// Every ceiling at or below one maximum batch yields the IDENTICAL worst case
/// (design D4a) — a regression that made the bound additive again
/// (`ceiling + one maximum batch`) would break this equality.
///
/// And the corrected half of D4a: the SHIPPED default is deliberately ABOVE one
/// maximum batch. Sitting at or below it is what made the deadlock clamp the
/// normal case, because admission is gated on the pre-materialization
/// RESERVATION, not on the trued-down realized size.
#[test]
fn a_ceiling_below_one_max_batch_does_not_widen_the_worst_case() {
    let one_max_batch = worst_case_batch_capacity_bytes(DEFAULT_MAX_BATCH_BYTES, 3, 0);
    for ceiling in [1024, 6 * 1024 * 1024, 8 * 1024 * 1024] {
        assert_eq!(
            ceiling.max(one_max_batch),
            one_max_batch,
            "ceiling {ceiling} should not widen the worst case"
        );
    }
    assert!(
        DEFAULT_MAX_INFLIGHT_EGRESS_BYTES > one_max_batch,
        "the shipped default ({DEFAULT_MAX_INFLIGHT_EGRESS_BYTES} B) must clear ONE worst-case \
         reservation ({one_max_batch} B), or every byte-cap-cut batch clamps to the whole pool \
         and the stream runs lock-step"
    );
}

// ---------------------------------------------------------------------------
// Requirement: the shipped default does not make the clamp the normal case
// ---------------------------------------------------------------------------

/// Permits in a pool of `ceiling` bytes — the quantity the clamp compares
/// against, computed the way the governor computes it.
fn pool_permits(ceiling: usize) -> u64 {
    (ceiling.div_ceil(EGRESS_CREDIT_QUANTUM_BYTES)) as u64
}

/// A worst-case reservation over the merged wide-row fixture's schema shape
/// (`id int, payload blob, label text` — three array nodes) is admitted at the
/// SHIPPED defaults WITHOUT tripping the deadlock clamp.
///
/// This is the regression guard for the 8 MiB default, which missed by exactly
/// the per-node slack term:
///
/// ```text
/// want = worst_case_batch_capacity_bytes(4 MiB, 3, 0) = 8,394,752 B = 8198 permits
/// 8 MiB pool  = 8192 permits  ->  8198 > 8192  -> CLAMP on every byte-cap cut
/// 12 MiB pool = 12288 permits ->  8198 <= 12288 -> admitted, 4090 permits spare
/// ```
#[tokio::test]
async fn a_worst_case_default_reservation_does_not_clamp() {
    let nodes = 3;
    let want = worst_case_batch_capacity_bytes(DEFAULT_MAX_BATCH_BYTES, nodes, 0);
    let (credit, obs) = credit(DEFAULT_MAX_INFLIGHT_EGRESS_BYTES);

    let reservation = credit.reserve(want).await.expect(POOL_OPEN);

    assert_eq!(
        obs.reservations_clamped(),
        0,
        "a full byte-cap-cut batch ({want} B, {} permits) clamped against the shipped {} B pool \
         ({} permits): every such batch would then hold the ENTIRE pool and the stream would run \
         lock-step with the batch-count channel dead behind it",
        want.div_ceil(EGRESS_CREDIT_QUANTUM_BYTES),
        DEFAULT_MAX_INFLIGHT_EGRESS_BYTES,
        pool_permits(DEFAULT_MAX_INFLIGHT_EGRESS_BYTES)
    );
    assert_eq!(
        obs.charged_bytes(),
        want as u64,
        "an unclamped reservation charges exactly what it asked for"
    );
    assert!(
        obs.charged_bytes() < DEFAULT_MAX_INFLIGHT_EGRESS_BYTES as u64,
        "a clamped reservation would hold the whole pool"
    );
    drop(reservation);
}

/// PAST the break point: a schema wide enough that the per-node slack alone
/// pushes one reservation past the ceiling DOES clamp — and the documented
/// behaviour holds there (the batch is still admitted, holding the entire pool,
/// with nothing else admissible beside it).
///
/// `permits_for(2 × 4 MiB + 2 KiB × nodes) = 8192 + 2 × nodes`, so against the
/// 12288-permit default pool the boundary is exactly 2048 nodes.
#[tokio::test]
async fn the_clamp_engages_only_past_the_documented_schema_width() {
    // Spare permits after one full-cap batch, converted back into array NODES at
    // the published per-node slack — derived from the constants, never hard-coded.
    let spare_permits = pool_permits(DEFAULT_MAX_INFLIGHT_EGRESS_BYTES) as usize
        - DEFAULT_MAX_BATCH_BYTES * BATCH_BYTES_CAPACITY_FACTOR / EGRESS_CREDIT_QUANTUM_BYTES;
    let boundary = spare_permits * EGRESS_CREDIT_QUANTUM_BYTES / BATCH_BYTES_PER_COLUMN_SLACK;
    assert_eq!(boundary, 2048, "the documented no-clamp schema width");

    for (nodes, expect_clamp) in [(boundary, false), (boundary + 1, true)] {
        let want = worst_case_batch_capacity_bytes(DEFAULT_MAX_BATCH_BYTES, nodes, 0);
        let (credit, obs) = credit(DEFAULT_MAX_INFLIGHT_EGRESS_BYTES);
        let reservation = credit.reserve(want).await.expect(POOL_OPEN);

        assert_eq!(
            obs.reservations_clamped(),
            u64::from(expect_clamp),
            "at {nodes} array nodes the clamp should{} engage",
            if expect_clamp { "" } else { " NOT" }
        );
        if expect_clamp {
            // The documented clamp behaviour: charged at most the whole pool,
            // still admitted, and the only thing that can be resident.
            assert_eq!(
                obs.charged_bytes(),
                DEFAULT_MAX_INFLIGHT_EGRESS_BYTES as u64,
                "a clamped reservation holds the ENTIRE pool"
            );
            let mut beside = Box::pin(credit.reserve(EGRESS_CREDIT_QUANTUM_BYTES));
            assert!(
                futures::poll!(&mut beside).is_pending(),
                "nothing may be admitted beside a clamped batch"
            );
        } else {
            assert_eq!(obs.charged_bytes(), want as u64);
        }
        drop(reservation);
    }
}

// ---------------------------------------------------------------------------
// Requirement: a pool that cannot charge fails CLOSED
// ---------------------------------------------------------------------------

/// A closed credit pool surfaces a terminal error instead of degrading to an
/// UNCHARGED reservation. Proceeding uncharged would put a batch on the egress
/// path outside the published bound — a silently voided memory bound, where every
/// other branch of this module fails closed.
#[tokio::test]
async fn a_closed_pool_fails_closed_rather_than_reserving_uncharged() {
    let (credit, obs) = credit(64 * 1024);
    credit.close_for_test();

    let Err(err) = credit.reserve(8 * 1024).await else {
        panic!("a closed pool must fail closed, not yield an uncharged reservation");
    };
    assert_eq!(err.requested, 8 * 1024);
    assert_eq!(err.permits, 8);
    assert!(
        err.to_string().contains("fails closed"),
        "the error must state the posture, got: {err}"
    );
    assert_eq!(
        obs.reservations_granted(),
        0,
        "no reservation may be granted on a closed pool"
    );
    assert_eq!(obs.charged_bytes(), 0);

    // It is a terminal INTERNAL fault on the wire, like the invariant violation.
    let status = tonic::Status::from(crate::producer::ProducerError::from(err));
    assert_eq!(status.code(), tonic::Code::Internal);
}

// ---------------------------------------------------------------------------
// Requirement: array NODES are counted, not columns
// ---------------------------------------------------------------------------

fn schema_of(fields: Vec<Field>) -> ArrowSchema {
    ArrowSchema::new(fields)
}

/// A `map<text,text>` column contributes FOUR array nodes (map, entries struct,
/// key `Utf8`, value `Utf8`), not one — so the reservation's slack term is not
/// under-stated for nested schemas.
#[test]
fn a_map_column_is_four_array_nodes() {
    let entries = Field::new(
        "entries",
        DataType::Struct(Fields::from(vec![
            Field::new("keys", DataType::Utf8, false),
            Field::new("values", DataType::Utf8, true),
        ])),
        false,
    );
    let schema = schema_of(vec![Field::new(
        "m",
        DataType::Map(Arc::new(entries), false),
        true,
    )]);
    assert_eq!(count_arrow_array_nodes(&schema), 4);
}

/// A `list<text>` column is two nodes; a flat scalar column is one.
#[test]
fn list_is_two_nodes_and_a_flat_column_is_one() {
    let list = schema_of(vec![Field::new(
        "l",
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        true,
    )]);
    assert_eq!(count_arrow_array_nodes(&list), 2);

    let flat = schema_of(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("payload", DataType::Binary, true),
        Field::new("label", DataType::Utf8, true),
    ]);
    assert_eq!(count_arrow_array_nodes(&flat), 3);
}

/// The count is the sum over the whole projected schema, and it feeds the
/// reservation through `worst_case_batch_capacity_bytes` — a bare
/// `payload × BATCH_BYTES_CAPACITY_FACTOR` would under-reserve by exactly
/// `BATCH_BYTES_PER_COLUMN_SLACK × nodes`.
#[test]
fn the_slack_term_is_what_the_bare_factor_would_miss() {
    let nodes = 7usize;
    let payload = 4096usize;
    let full = worst_case_batch_capacity_bytes(payload, nodes, 0);
    let bare_factor_only = payload * BATCH_BYTES_CAPACITY_FACTOR;
    assert_eq!(
        full - bare_factor_only,
        BATCH_BYTES_PER_COLUMN_SLACK * nodes
    );
}

// ---------------------------------------------------------------------------
// Requirement: the byte ceiling never deadlocks (the clamp)
// ---------------------------------------------------------------------------

/// A reservation LARGER than the whole pool is still granted when nothing else
/// is in flight — clamped to the pool total. A naive non-clamping implementation
/// parks forever here.
#[tokio::test]
async fn a_reservation_larger_than_the_pool_is_still_granted() {
    let (credit, obs) = credit(4 * 1024);
    let reservation = credit.reserve(64 * 1024).await.expect(POOL_OPEN);
    let permit = reservation
        .materialize(60 * 1024)
        .expect("under reservation");
    // Charged at most the whole pool; resident at its own (larger) size.
    assert_eq!(permit.charged_bytes(), 4 * 1024);
    assert_eq!(obs.resident_capacity_bytes(), 60 * 1024);
}

/// Degenerate ceilings of `0` and `1` byte degrade to strict one-batch-at-a-time
/// egress rather than either wedging or silently becoming unbounded.
#[tokio::test]
async fn degenerate_ceilings_still_admit_one_batch() {
    for ceiling in [0usize, 1usize] {
        let (credit, _obs) = credit(ceiling);
        let first = credit.reserve(1024 * 1024).await.expect(POOL_OPEN);
        let permit = first.materialize(512 * 1024).expect("granted");
        // One quantum is the floor of a bounded pool.
        assert_eq!(permit.charged_bytes(), EGRESS_CREDIT_QUANTUM_BYTES as u64);

        // The pool is now empty, so a second reservation parks — and is released
        // the moment the first permit drops. No spin, no drop, no deadlock.
        let mut second = Box::pin(credit.reserve(1024));
        assert!(futures::poll!(&mut second).is_pending());
        drop(permit);
        let _ = second.await.expect(POOL_OPEN);
    }
}

// ---------------------------------------------------------------------------
// Requirement: reserve BEFORE materialize, and park on an exhausted pool
// ---------------------------------------------------------------------------

/// With the pool exhausted, a further reservation is PENDING — the producer
/// parks with nothing materialized. `batches_materialized` can never exceed
/// `reservations_granted`, which is the reserve-before-materialize property made
/// observable (a charge-at-emit implementation inverts that inequality).
#[tokio::test]
async fn an_exhausted_pool_parks_the_next_reservation() {
    let (credit, obs) = credit(8 * 1024);
    let held = credit
        .reserve(8 * 1024)
        .await
        .expect(POOL_OPEN)
        .materialize(8 * 1024)
        .expect("granted");
    assert_eq!(obs.charged_bytes(), 8 * 1024);

    let mut parked = Box::pin(credit.reserve(4 * 1024));
    assert!(futures::poll!(&mut parked).is_pending());
    // Parked BEFORE building: exactly one materialization per granted reservation.
    assert_eq!(obs.reservations_granted(), 1);
    assert_eq!(obs.batches_materialized(), 1);

    drop(held);
    assert_eq!(obs.charged_bytes(), 0);
    let granted = parked.await.expect(POOL_OPEN);
    assert_eq!(obs.reservations_granted(), 2);
    assert_eq!(obs.batches_materialized(), 1, "nothing built yet");
    drop(granted);
}

// ---------------------------------------------------------------------------
// Requirement: true up DOWNWARD, never upward
// ---------------------------------------------------------------------------

/// An over-reserved batch returns its excess IMMEDIATELY, so the charged credit
/// tracks the realized capacity (within the KiB quantum) rather than the
/// reservation — without which one conservative estimate would pin the pool and
/// serialize the stream.
#[tokio::test]
async fn an_over_reserved_batch_returns_its_excess() {
    let (credit, obs) = credit(64 * 1024);
    let reservation = credit.reserve(48 * 1024).await.expect(POOL_OPEN);
    assert_eq!(obs.charged_bytes(), 48 * 1024, "full reservation held");

    let permit = reservation
        .materialize(6 * 1024)
        .expect("under reservation");
    assert_eq!(
        permit.charged_bytes(),
        6 * 1024,
        "trued up DOWN to realized"
    );
    assert_eq!(obs.charged_bytes(), 6 * 1024);
    assert_eq!(obs.resident_capacity_bytes(), 6 * 1024);

    // The returned credit is genuinely available again: a second reservation
    // that would NOT have fitted under the original one now resolves.
    let second = credit.reserve(56 * 1024).await.expect(POOL_OPEN);
    assert_eq!(obs.charged_bytes(), 62 * 1024);
    drop(second);
    drop(permit);
    assert_eq!(obs.charged_bytes(), 0);
}

/// A realized capacity ABOVE the reservation is a violated invariant: it fails
/// closed with the named error, releases every byte of credit, and yields no
/// permit that could carry a batch onto the egress path on a false account.
#[tokio::test]
async fn an_under_reservation_fails_closed() {
    let (credit, obs) = credit(64 * 1024);
    let reservation = credit.reserve(8 * 1024).await.expect(POOL_OPEN);
    let Err(err) = reservation.materialize(8 * 1024 + 1) else {
        panic!("an under-reservation must fail closed, not yield a permit");
    };
    assert_eq!(err.reserved, 8 * 1024);
    assert_eq!(err.actual, 8 * 1024 + 1);
    assert!(
        err.to_string().contains("estimator-conservatism")
            || err.to_string().contains("arrow_size.rs"),
        "the error must name the violated invariant, got: {err}"
    );
    assert_eq!(obs.charged_bytes(), 0, "no credit leaked");
    assert_eq!(obs.resident_capacity_bytes(), 0);
    assert_eq!(
        obs.batches_materialized(),
        0,
        "nothing emitted on a bad account"
    );
}

// ---------------------------------------------------------------------------
// Requirement: release is structural (RAII) on every path
// ---------------------------------------------------------------------------

/// Dropping a permit — for ANY reason: normal drain, a dropped receiver with
/// batches still queued, an unwind — returns its credit, and the whole pool is
/// available again afterwards.
#[tokio::test]
async fn dropping_a_permit_returns_the_whole_pool() {
    let (credit, obs) = credit(16 * 1024);
    let a = credit
        .reserve(8 * 1024)
        .await
        .expect(POOL_OPEN)
        .materialize(8 * 1024)
        .expect("a");
    let b = credit
        .reserve(8 * 1024)
        .await
        .expect(POOL_OPEN)
        .materialize(8 * 1024)
        .expect("b");
    assert_eq!(obs.charged_bytes(), 16 * 1024);
    assert_eq!(obs.peak_resident_capacity_bytes(), 16 * 1024);

    drop(a);
    drop(b);
    assert_eq!(obs.charged_bytes(), 0);
    assert_eq!(obs.resident_capacity_bytes(), 0);
    // Proof the pool really refilled: a full-pool reservation resolves at once.
    let mut full = Box::pin(credit.reserve(16 * 1024));
    assert!(futures::poll!(&mut full).is_ready());
}

/// Dropping a reservation that was never materialized (cancel while parked)
/// releases its credit too, and records no materialization.
#[tokio::test]
async fn an_abandoned_reservation_releases_its_credit() {
    let (credit, obs) = credit(16 * 1024);
    let reservation = credit.reserve(16 * 1024).await.expect(POOL_OPEN);
    assert_eq!(obs.charged_bytes(), 16 * 1024);
    drop(reservation);
    assert_eq!(obs.charged_bytes(), 0);
    assert_eq!(obs.batches_materialized(), 0);
}

// ---------------------------------------------------------------------------
// The collect/parity path stays byte-identical and runtime-free
// ---------------------------------------------------------------------------

/// `EgressReservation::inert` needs NO Tokio runtime (the collect path runs on a
/// plain thread) and can never fail closed, whatever the batch turns out to be.
#[test]
fn the_inert_reservation_needs_no_runtime_and_never_fails_closed() {
    let permit = EgressReservation::inert()
        .materialize(usize::MAX)
        .expect("inert reservations never fail closed");
    assert_eq!(permit.charged_bytes(), 0);
}

/// The TWO inert routes account identically — for nothing.
///
/// The collect path reaches an inert permit through
/// `EgressReservation::inert().materialize(..)`; the aggregate path builds one
/// directly via `CreditedBatch::uncredited`, never materializing. Before this was
/// made explicit the first recorded residency into a throwaway observation nobody
/// can read while the second recorded nothing at all — two different answers for
/// the same "outside the governed set" state, either of which could mislead a
/// reader of the seam.
#[test]
fn both_inert_routes_account_for_nothing() {
    let collect = EgressReservation::inert()
        .materialize(64 * 1024)
        .expect("inert reservations never fail closed");
    let empty = RecordBatch::new_empty(Arc::new(ArrowSchema::empty()));
    let (_batch, aggregate) = CreditedBatch::uncredited(empty).split();

    assert_eq!(collect.charged_bytes(), aggregate.charged_bytes());
    assert_eq!(collect.resident_bytes(), aggregate.resident_bytes());
    assert_eq!(collect.charged_bytes(), 0);
    assert_eq!(
        collect.resident_bytes(),
        0,
        "an inert permit must not publish residency it cannot report"
    );
}

/// An unbounded budget applies no ceiling: reservations resolve immediately
/// however large, and nothing is charged (the embedder opt-out).
#[tokio::test]
async fn an_unbounded_budget_charges_nothing() {
    let obs = EgressObservation::default();
    let credit = EgressCredit::new(EgressBudget::unbounded(), obs.clone());
    let reservation = futures::FutureExt::now_or_never(credit.reserve(usize::MAX))
        .expect("an unbounded budget resolves a reservation of any size immediately")
        .expect(POOL_OPEN);
    let permit = reservation
        .materialize(1024)
        .expect("no ceiling to violate");
    assert_eq!(permit.charged_bytes(), 0);
    assert_eq!(obs.charged_bytes(), 0);
    // Residency is still OBSERVED (the seam works), it is just not governed.
    assert_eq!(obs.resident_capacity_bytes(), 1024);
}

/// The quantum rounds UP, always: a sub-quantum batch still charges one permit,
/// so the pool is never over-committed by rounding.
#[tokio::test]
async fn quantisation_always_rounds_up() {
    let (credit, _obs) = credit(4 * 1024);
    let permit = credit
        .reserve(1)
        .await
        .expect(POOL_OPEN)
        .materialize(1)
        .expect("granted");
    assert_eq!(permit.charged_bytes(), EGRESS_CREDIT_QUANTUM_BYTES as u64);
}
