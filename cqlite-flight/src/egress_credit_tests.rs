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

use super::*;
use crate::batch_bytes::{
    worst_case_batch_capacity_bytes, BATCH_BYTES_CAPACITY_FACTOR, BATCH_BYTES_PER_COLUMN_SLACK,
    DEFAULT_MAX_BATCH_BYTES,
};

/// The ratified B4 per-query working set at concurrency 1.
const B4_WORKING_SET_BYTES: usize = 16 * 1024 * 1024;

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

/// Every ceiling at or below one maximum batch yields the IDENTICAL worst case —
/// the reason the default is the largest such value (design D4a). A regression
/// that made the bound additive again (`ceiling + one maximum batch`) would break
/// this equality.
#[test]
fn any_ceiling_below_one_max_batch_has_the_same_worst_case() {
    let one_max_batch = worst_case_batch_capacity_bytes(DEFAULT_MAX_BATCH_BYTES, 3, 0);
    for ceiling in [1024, 6 * 1024 * 1024, DEFAULT_MAX_INFLIGHT_EGRESS_BYTES] {
        assert_eq!(
            ceiling.max(one_max_batch),
            one_max_batch,
            "ceiling {ceiling} should not widen the worst case"
        );
    }
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
    let reservation = credit.reserve(64 * 1024).await;
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
        let first = credit.reserve(1024 * 1024).await;
        let permit = first.materialize(512 * 1024).expect("granted");
        // One quantum is the floor of a bounded pool.
        assert_eq!(permit.charged_bytes(), EGRESS_CREDIT_QUANTUM_BYTES as u64);

        // The pool is now empty, so a second reservation parks — and is released
        // the moment the first permit drops. No spin, no drop, no deadlock.
        let mut second = Box::pin(credit.reserve(1024));
        assert!(futures::poll!(&mut second).is_pending());
        drop(permit);
        let _ = second.await;
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
    let granted = parked.await;
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
    let reservation = credit.reserve(48 * 1024).await;
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
    let second = credit.reserve(56 * 1024).await;
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
    let reservation = credit.reserve(8 * 1024).await;
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
        .materialize(8 * 1024)
        .expect("a");
    let b = credit
        .reserve(8 * 1024)
        .await
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
    let reservation = credit.reserve(16 * 1024).await;
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

/// An unbounded budget applies no ceiling: reservations resolve immediately
/// however large, and nothing is charged (the embedder opt-out).
#[tokio::test]
async fn an_unbounded_budget_charges_nothing() {
    let obs = EgressObservation::default();
    let credit = EgressCredit::new(EgressBudget::unbounded(), obs.clone());
    let reservation = futures::FutureExt::now_or_never(credit.reserve(usize::MAX))
        .expect("an unbounded budget resolves a reservation of any size immediately");
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
    let permit = credit.reserve(1).await.materialize(1).expect("granted");
    assert_eq!(permit.charged_bytes(), EGRESS_CREDIT_QUANTUM_BYTES as u64);
}
