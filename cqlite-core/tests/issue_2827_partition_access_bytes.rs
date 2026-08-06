//! Byte weighting and the BTI fail-closed path for the bounded partition
//! access-distribution probe (issue #2827).
//!
//! # Scope statement
//!
//! This change delivers **the instrument and the procedure, not the field number**.
//! Issue #2827's original AC2 — "decides whether a 64–128 MiB decoded-partition
//! cache clears a useful hit ratio" — is **NOT satisfied** by it. Not waived, not
//! deferred to another issue: satisfiable on the first real keyed workload run with
//! the probe enabled, and blocked only by the absence of such a workload
//! (`docs/research/phase2-verify-caching.md:214-216`). Nothing here is a measured
//! field skew, a go/no-go, or a gate.
//!
//! # What is asserted
//!
//! - **Distinct-partition semantics**: a partition accessed ten times contributes
//!   its on-disk bytes ONCE, because the working set is defined over distinct
//!   partitions.
//! - **An unpriceable access fails closed**: a resolution that yields no size is
//!   still counted as a partition, marked `size_source = unavailable`, and
//!   contributes ZERO bytes. No size is estimated, interpolated from a successor
//!   offset, or defaulted.
//! - **Incompleteness is visible**: a mixed window carries both `size_source`
//!   values, and the decision procedure REFUSES it by naming the unpriceable
//!   fraction rather than pricing a partial byte total.
//!
//! # Where the bytes come from: the successor gap, not an index-recorded size
//!
//! Neither Cassandra 5.0 index format records a per-partition size. A BIG index
//! entry is `[key][data_offset vint][promoted_index_len vint][promoted_index]`
//! (`docs/sstables-definitive-guide/chapters/06-index-and-summary.md`, "Index.db
//! Entry Format"; written by `BigTableWriter.createRowIndexEntry` at
//! `cassandra-5.0.8`) and the BTI `Partitions.db` trie resolves an offset only.
//!
//! So a partition's on-disk extent is MEASURED as `[data_offset,
//! successor_offset)` — the same authoritative index-layout bound the
//! single-partition seek already uses to size its decompression window — reported
//! under the distinct `size_source = successor_gap` so a reader can always tell a
//! measured extent from an index-supplied one, and from a genuinely unknown one.
//!
//! # Fixture contract (#3220)
//!
//! The two fixtures exercised end-to-end are COMMITTED to git — a BIG (`nb`) table
//! and a BTI (`da`) table — so neither can be legitimately absent in any checkout.
//! Resolution failure is a hard FAILURE, per case, unconditionally: never a
//! suite-wide `assert!(ran > 0)`, which cannot see one case skipping behind its
//! siblings.

use cqlite_core::observability::partition_access::{
    decision, AccessWeight, PartitionAccessRecorder, Refusal, RepeatBucket, TableScope, Verdict,
    WindowConfig,
};
use std::time::Duration;

/// One table for every recorder-level case here; the cross-table identity property
/// has its own case below.
const SCOPE: TableScope<'static> = TableScope {
    keyspace: "ks",
    table: "t",
};

fn deterministic_recorder() -> PartitionAccessRecorder {
    PartitionAccessRecorder::new(WindowConfig {
        duration: Duration::from_secs(86_400),
        max_accesses: u64::MAX,
        ..WindowConfig::default()
    })
}

#[test]
fn repeated_accesses_to_one_partition_count_its_bytes_once() {
    let r = deterministic_recorder();
    // One partition of known on-disk size, accessed ten times in the window.
    for _ in 0..10 {
        r.record(
            SCOPE,
            b"partition-of-known-size",
            AccessWeight::SuccessorGap(65_536),
        );
    }
    let s = r.close_window().expect("accesses were recorded");

    let bucket = s.bucket(RepeatBucket::NineToSixteen);
    assert_eq!(bucket.distinct_successor_gap, 1, "one distinct partition");
    assert_eq!(bucket.distinct_index, 0);
    assert_eq!(bucket.accesses, 10);
    assert_eq!(
        bucket.bytes, 65_536,
        "distinct-partition bytes count ONCE, not once per access (10 x 65_536 \
         would be 655_360)"
    );
    assert_eq!(s.total_bytes(), 65_536);
}

#[test]
fn an_entry_retains_the_maximum_weight_never_a_running_sum() {
    // A later access that resolved MORE generations reports a larger weight; the
    // entry keeps the maximum, which is exact because partition sizes are immutable
    // within a generation set.
    let r = deterministic_recorder();
    r.record(SCOPE, b"p", AccessWeight::SuccessorGap(1_000));
    r.record(SCOPE, b"p", AccessWeight::SuccessorGap(3_000));
    r.record(SCOPE, b"p", AccessWeight::SuccessorGap(2_000));
    let s = r.close_window().expect("accesses were recorded");
    assert_eq!(s.bucket(RepeatBucket::ThreeToFour).bytes, 3_000);
    assert_eq!(s.total_bytes(), 3_000);
}

#[test]
fn an_unavailable_access_is_counted_as_a_partition_and_contributes_no_bytes() {
    let r = deterministic_recorder();
    r.record(SCOPE, b"bti-resolved", AccessWeight::Unavailable);
    let s = r.close_window().expect("an access was recorded");

    let bucket = s.bucket(RepeatBucket::One);
    assert_eq!(
        bucket.distinct_unavailable, 1,
        "a BTI access is a real access — dropping it would make the histogram wrong"
    );
    assert_eq!(bucket.distinct_index, 0);
    assert_eq!(bucket.bytes, 0, "no size is estimated for it, ever");
    assert_eq!(s.total_bytes(), 0);
    assert_eq!(s.unavailable_partitions(), 1);
    assert!((s.unavailable_fraction() - 1.0).abs() < f64::EPSILON);
}

#[test]
fn unavailability_is_sticky_for_the_window() {
    // One unpriced access poisons the partition's byte weight for the window: a
    // partial size is not a size, and reporting the priced subset would understate
    // the working set — the direction that flatters the cache.
    let r = deterministic_recorder();
    r.record(SCOPE, b"p", AccessWeight::SuccessorGap(4_096));
    r.record(SCOPE, b"p", AccessWeight::Unavailable);
    r.record(SCOPE, b"p", AccessWeight::SuccessorGap(4_096));
    let s = r.close_window().expect("accesses were recorded");
    assert_eq!(s.bucket(RepeatBucket::ThreeToFour).distinct_unavailable, 1);
    assert_eq!(
        s.bucket(RepeatBucket::ThreeToFour).distinct_successor_gap,
        0
    );
    assert_eq!(s.bucket(RepeatBucket::ThreeToFour).bytes, 0);
}

#[test]
fn a_mixed_window_makes_its_incompleteness_visible_and_the_procedure_refuses_it() {
    let r = deterministic_recorder();
    // Enough accesses to clear the procedure's minimum, so the refusal below is
    // about the unpriceable fraction and nothing else.
    for i in 0..12_000u64 {
        r.record(SCOPE, &i.to_be_bytes(), AccessWeight::SuccessorGap(2_048));
    }
    for i in 0..50u64 {
        r.record(
            SCOPE,
            &(i | 1 << 40).to_be_bytes(),
            AccessWeight::Unavailable,
        );
    }
    let s = r.close_window().expect("accesses were recorded");

    // Both arms present with non-zero values.
    let priced: u64 = RepeatBucket::ALL
        .iter()
        .map(|b| s.bucket(*b).distinct_priced())
        .sum();
    assert!(priced > 0, "the measured partitions must be priced");
    assert_eq!(s.unavailable_partitions(), 50);
    assert!(s.unavailable_fraction() > 0.0);
    assert!(s.total_bytes() > 0);

    match decision::evaluate(
        &s,
        decision::WindowSource::Field,
        128 * 1024 * 1024,
        decision::ASSUMED_DECODE_MULTIPLIER,
    ) {
        Verdict::Refused(Refusal::UnpriceableFraction {
            partitions,
            fraction,
        }) => {
            assert_eq!(partitions, 50);
            assert!(fraction > 0.0);
            // The refusal NAMES the unpriceable fraction rather than pricing the
            // partial byte total.
            let text = Refusal::UnpriceableFraction {
                partitions,
                fraction,
            }
            .to_string();
            assert!(text.contains("unavailable"), "{text}");
        }
        other => panic!("a partial byte total must never be priced: got {other:?}"),
    }
}

#[test]
fn a_window_at_the_sampling_floor_is_non_census_and_refused() {
    // Drive past the sampling-prefix cap. The PRODUCTION cap is 20 (a 1-in-1,048,576
    // sample), which no realistic corpus reaches — so the cap is a config knob
    // purely to make this state reachable here. Everything else about the recorder
    // is the production configuration.
    let r = PartitionAccessRecorder::new(WindowConfig {
        duration: Duration::from_secs(86_400),
        max_accesses: u64::MAX,
        max_prefix_bits: 1,
    });
    for i in 0..400_000u64 {
        r.record(
            SCOPE,
            &i.wrapping_mul(0x9e37_79b9_7f4a_7c15).to_be_bytes(),
            AccessWeight::SuccessorGap(64),
        );
    }
    let s = r.close_window().expect("accesses were recorded");
    assert!(!s.is_census(), "400k distinct keys cannot be a census here");
    assert!(
        s.at_sampling_floor,
        "the prefix width cap must be reached and reported"
    );
    // The requirement is "returns a refusal, not a hit-ratio number". A window
    // driven this hard also loses input it could not seat, and that refusal is
    // checked first — either name is a correct diagnosis of the same unusable
    // window, so the assertion is on the refusal, not on which one won the race.
    let verdict = decision::evaluate(
        &s,
        decision::WindowSource::Field,
        128 * 1024 * 1024,
        decision::ASSUMED_DECODE_MULTIPLIER,
    );
    assert!(
        verdict.is_refusal(),
        "a floored window must be refused, not priced: got {verdict:?}"
    );
    assert!(matches!(
        verdict,
        Verdict::Refused(Refusal::SamplingFloor { .. } | Refusal::DroppedAccesses { .. })
    ));
}

#[test]
fn a_downsampled_but_unfloored_window_is_refused_as_a_sample() {
    // C2: the budget arithmetic compares REAL bytes (`C / m`) against the window's
    // per-bucket bytes. In a 1-in-2^k sampled window those bytes cover only the
    // admitted share, so pricing the full budget against them makes everything look
    // like it fits — a FALSE "go". The committed note is explicit that absolute
    // `n_b`/`B_b` are meaningless until scaled; the procedure refuses instead.
    let r = deterministic_recorder();
    for i in 0..220_000u64 {
        r.record(
            SCOPE,
            &i.wrapping_mul(0x9e37_79b9_7f4a_7c15).to_be_bytes(),
            AccessWeight::SuccessorGap(1_024),
        );
    }
    let s = r.close_window().expect("accesses were recorded");
    assert!(!s.is_census(), "220k distinct keys must force a downsample");
    assert!(!s.at_sampling_floor, "but nowhere near the prefix cap");
    assert_eq!(s.dropped_accesses, 0, "and nothing was lost");

    match decision::evaluate(
        &s,
        decision::WindowSource::Field,
        128 * 1024 * 1024,
        decision::ASSUMED_DECODE_MULTIPLIER,
    ) {
        Verdict::Refused(Refusal::NonCensusSample { sample_denominator }) => {
            assert!(sample_denominator > 1);
        }
        other => panic!("a sample must never be filled against a real budget: {other:?}"),
    }
}

#[test]
fn a_partial_extent_is_never_published_as_a_complete_one() {
    // E1, at the level the bug lived: the accumulator must not turn "one generation
    // priced, one generation unpriceable" into a fully-measured total.
    //
    // The defect was upstream of this type — the resolver read a key-cache MISS as
    // "this generation did not hold the key", so an evicted-but-held generation
    // contributed NOTHING instead of poisoning the access, and the surviving
    // generation's partial sum shipped as `SuccessorGap`. That under-prices the
    // working set, i.e. flatters the cache. The resolver now resolves residency
    // authoritatively and calls `note_unsized` for anything indeterminate; this pins
    // the contract it relies on.
    let mut b = cqlite_core::observability::partition_access::AccessWeightBuilder::new();
    b.note_measured(4_096); // generation A: measured
    b.note_unsized(); // generation B: held, but not priceable
    assert_eq!(
        b.finish(),
        AccessWeight::Unavailable,
        "a partial sum must never be published as a measured extent"
    );

    // And the same through a window: the access is counted, contributes no bytes,
    // and makes the window unpriceable.
    let r = deterministic_recorder();
    r.record(SCOPE, b"p", AccessWeight::Unavailable);
    let s = r.close_window().expect("the access was recorded");
    assert_eq!(s.distinct_partitions(), 1);
    assert_eq!(s.total_bytes(), 0);
    assert!(decision::evaluate(
        &s,
        decision::WindowSource::Field,
        128 * 1024 * 1024,
        decision::ASSUMED_DECODE_MULTIPLIER,
    )
    .is_refusal());
}

#[test]
fn a_mixed_provenance_total_reports_the_weakest_provenance() {
    // An access whose bytes came partly from an index-recorded size and partly from
    // a measured gap is reported MEASURED: a total is only as well-founded as its
    // weakest component, and collapsing it to `index` would overstate how the number
    // was obtained.
    let mut b = cqlite_core::observability::partition_access::AccessWeightBuilder::new();
    b.note_sized(100);
    b.note_measured(400);
    assert_eq!(b.finish(), AccessWeight::SuccessorGap(500));

    // A zero-length gap is not an extent.
    let mut b = cqlite_core::observability::partition_access::AccessWeightBuilder::new();
    b.note_measured(0);
    assert_eq!(b.finish(), AccessWeight::Unavailable);
}

#[test]
fn a_gap_measured_census_window_is_priced_not_refused() {
    // The point of the successor-gap mechanism: a window whose bytes were MEASURED
    // is priceable. Refusal condition 1 exists to reject an INCOMPLETE byte total,
    // and a fully gap-measured window is complete.
    let r = deterministic_recorder();
    for i in 0..600u64 {
        for _ in 0..20 {
            r.record(SCOPE, &i.to_be_bytes(), AccessWeight::SuccessorGap(1_024));
        }
    }
    for i in 0..10_000u64 {
        r.record(
            SCOPE,
            &(i | 1 << 40).to_be_bytes(),
            AccessWeight::SuccessorGap(1_024),
        );
    }
    let s = r.close_window().expect("accesses were recorded");
    assert_eq!(s.unavailable_partitions(), 0);
    assert!(s.total_bytes() > 0, "measured bytes must be reported");

    match decision::evaluate(
        &s,
        decision::WindowSource::Field,
        128 * 1024 * 1024,
        decision::ASSUMED_DECODE_MULTIPLIER,
    ) {
        Verdict::Priced(c) => {
            // Hand-computed: A = 600*20 + 10_000 = 22_000; the hot bucket can serve
            // 12_000 - 600 = 11_400 and the singleton bucket 0; both fit the
            // 38.34 MiB on-disk budget, so H_max = 11_400 / 22_000 = 0.51818...
            assert!(
                (c.h_max - 11_400.0 / 22_000.0).abs() < 1e-9,
                "unexpected ceiling {}",
                c.h_max
            );
        }
        other => panic!("a fully measured window must be priceable: got {other:?}"),
    }
}

/// The worked example in `docs/research/decoded-partition-cache-decision.md` must be
/// reproducible by the shipped evaluator — otherwise the committed note documents
/// arithmetic the code does not perform.
///
/// It is an INSTRUMENT SELF-CHECK and never a field result: the note labels it so,
/// and refusal condition 4 rejects the same window when its source is declared
/// synthetic (asserted below).
#[test]
fn the_committed_notes_worked_example_matches_the_shipped_evaluator() {
    let r = deterministic_recorder();
    for i in 0..600u64 {
        for _ in 0..20 {
            r.record(SCOPE, &i.to_be_bytes(), AccessWeight::SuccessorGap(1_024));
        }
    }
    for i in 0..10_000u64 {
        r.record(
            SCOPE,
            &(i | 1 << 40).to_be_bytes(),
            AccessWeight::SuccessorGap(1_024),
        );
    }
    let s = r.close_window().expect("accesses were recorded");
    assert_eq!(s.total_accesses(), 22_000, "A in the note");
    assert_eq!(s.bucket(RepeatBucket::SeventeenPlus).accesses, 12_000);
    assert_eq!(s.bucket(RepeatBucket::SeventeenPlus).distinct(), 600);
    assert_eq!(s.bucket(RepeatBucket::SeventeenPlus).bytes, 614_400);
    assert_eq!(s.bucket(RepeatBucket::One).bytes, 10_240_000);

    match decision::evaluate(
        &s,
        decision::WindowSource::Field,
        128 * 1024 * 1024,
        decision::ASSUMED_DECODE_MULTIPLIER,
    ) {
        Verdict::Priced(c) => {
            assert!(
                (c.h_max - 0.518).abs() < 0.001,
                "the note states H_max(128 MiB) = 0.518; evaluator says {}",
                c.h_max
            );
            assert!(c.clears_threshold, "0.518 >= the recorded 0.50 threshold");
        }
        other => panic!("the note's example window must be priceable: got {other:?}"),
    }

    // The same window, declared for what it is, is refused.
    assert_eq!(
        decision::evaluate(
            &s,
            decision::WindowSource::Synthetic,
            128 * 1024 * 1024,
            decision::ASSUMED_DECODE_MULTIPLIER,
        ),
        Verdict::Refused(Refusal::SyntheticWorkload)
    );
}

#[test]
fn reaching_the_prefix_cap_marks_the_window_non_census_even_when_it_fits_afterwards() {
    // The B1 boundary the sibling floor test cannot see. That one drives 400k keys
    // against `max_prefix_bits: 1`, so the table is STILL over its load factor when
    // the widen loop gives up — both arms of the old `prefix_bits >= cap &&
    // occupancy >= limit` condition hold and the flag is set for the wrong reason.
    //
    // Here the key count is chosen so the last permitted widen brings occupancy
    // BELOW the load factor: the window ends at the cap with a comfortable table.
    // Under the old condition it reported `at_sampling_floor = false` beside
    // `sample_denominator = 2^cap`, and `decision::evaluate` then PRICED a
    // 1-in-2^cap sample. The floor is a property of the scale reached, full stop.
    let r = PartitionAccessRecorder::new(WindowConfig {
        duration: Duration::from_secs(86_400),
        max_accesses: u64::MAX,
        max_prefix_bits: 2,
    });
    // 250k distinct keys against a cap of 2. The admitted share halves per widen:
    // k=1 leaves ~125k, still over the 98,304 load factor, so the recorder widens
    // again and REACHES the cap; k=2 leaves ~62.5k, comfortably under it. That is
    // exactly the state the old condition mis-classified as a census.
    for i in 0..250_000u64 {
        r.record(
            SCOPE,
            &i.wrapping_mul(0x9e37_79b9_7f4a_7c15).to_be_bytes(),
            AccessWeight::SuccessorGap(64),
        );
    }
    let s = r.close_window().expect("accesses were recorded");
    assert_eq!(s.sample_denominator, 4, "the prefix reached its cap of 2");
    assert!(
        s.distinct_partitions() < 98_304,
        "the table must have ended UNDER its load factor — otherwise this test is \
         re-testing the sibling case, not the boundary ({} entries)",
        s.distinct_partitions()
    );
    assert!(
        s.at_sampling_floor,
        "a window that reached the prefix cap is non-census whatever its final \
         occupancy"
    );
    assert!(
        decision::evaluate(
            &s,
            decision::WindowSource::Field,
            128 * 1024 * 1024,
            decision::ASSUMED_DECODE_MULTIPLIER,
        )
        .is_refusal(),
        "a capped sample must never be priced"
    );
}

#[test]
fn accesses_dropped_at_the_sampling_floor_are_reported_not_lost() {
    // B8: an access the table could not seat is INPUT LOST, distinct from a key the
    // sampling predicate declined to admit (which is the sample working as designed).
    // A measurement instrument must never lose input silently.
    let r = PartitionAccessRecorder::new(WindowConfig {
        duration: Duration::from_secs(86_400),
        max_accesses: u64::MAX,
        max_prefix_bits: 0,
    });
    // With no widening permitted the table saturates and further keys cannot be
    // seated at all.
    for i in 0..400_000u64 {
        r.record(
            SCOPE,
            &i.wrapping_mul(0x9e37_79b9_7f4a_7c15).to_be_bytes(),
            AccessWeight::SuccessorGap(64),
        );
    }
    let s = r.close_window().expect("accesses were recorded");
    assert_eq!(s.recorded_accesses, 400_000);
    assert!(
        s.dropped_accesses > 0,
        "a saturated table at the prefix cap must REPORT the accesses it could not \
         seat, not absorb them"
    );
    assert!(
        s.dropped_accesses <= s.recorded_accesses,
        "a drop is a subset of what was offered"
    );
    assert!(s.at_sampling_floor);
}

#[test]
fn the_access_that_crosses_a_window_boundary_opens_the_next_window() {
    // E2, tested WITHOUT sleeping on a clock. A zero-length window means every
    // access finds the window already expired, so the ordering of "close" and "seat"
    // is observable directly: with the close BEFORE the seat, each access lands in a
    // fresh window and the same key can never accumulate a repeat count.
    //
    // Under the old ordering (seat, then close) the second access was folded into
    // the window it had already left, so one nominal window reported count 2 — a
    // manufactured repeat. The design accepts the OPPOSITE bias: a partition split
    // across a boundary becomes two lower-repeat entries, understating
    // concentration, because understating is the safe direction for a go/no-go.
    let r = PartitionAccessRecorder::new(WindowConfig {
        duration: Duration::ZERO,
        max_accesses: u64::MAX,
        ..WindowConfig::default()
    });

    // First access: nothing to close (no window yet), so it is seated.
    assert!(r
        .record(SCOPE, b"hot", AccessWeight::SuccessorGap(100))
        .is_empty());
    // Second access to the SAME key: the expired window closes FIRST, carrying only
    // access #1, and access #2 opens the next window.
    let closed = r.record(SCOPE, b"hot", AccessWeight::SuccessorGap(100));
    assert_eq!(
        closed.len(),
        1,
        "the expired window must close before the new access is seated"
    );
    let first = closed[0];
    assert_eq!(
        first.total_accesses(),
        1,
        "the boundary-crossing access must NOT be banked in the window it left"
    );
    assert_eq!(first.bucket(RepeatBucket::One).distinct(), 1);
    assert_eq!(
        first.bucket(RepeatBucket::Two).distinct(),
        0,
        "a repeat count of 2 here would be the manufactured concentration E2 removes"
    );

    // And access #2 really is in the next window.
    let second = r.close_window().expect("access #2 opened a window");
    assert_eq!(second.total_accesses(), 1);
    assert_eq!(second.bucket(RepeatBucket::One).distinct(), 1);
}

#[test]
fn both_window_triggers_firing_on_one_access_lose_neither_window() {
    // Nit from round 5: the duration bound closes BEFORE the access is seated and
    // the access-count bound AFTER it, so with an access bound of 1 EVERY access
    // fires both. Returning only one of the two summaries would drop a closed
    // window's whole measurement on the floor.
    let r = PartitionAccessRecorder::new(WindowConfig {
        duration: Duration::ZERO,
        max_accesses: 1,
        ..WindowConfig::default()
    });

    // First access: no window exists to expire, but it fills its own immediately.
    let first = r.record(SCOPE, b"a", AccessWeight::SuccessorGap(10));
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].total_accesses(), 1);

    // Second access: the (already expired, already emptied) window closes with
    // nothing — silent — and the access then fills its own.
    let second = r.record(SCOPE, b"b", AccessWeight::SuccessorGap(10));
    assert_eq!(
        second.len(),
        1,
        "an empty expired window is silent, so only the filled one is reported"
    );
    assert_eq!(second[0].total_accesses(), 1);
    assert_eq!(second[0].distinct_partitions(), 1);
}

#[test]
fn a_concurrent_close_never_splits_an_access_from_its_count() {
    // C3: the access count and the table entry must be banked in the SAME window
    // generation. When they were not, a close landing between the two put the count
    // in window N and the entry in window N+1 — so a window could hold entries with
    // `recorded_accesses` short of them, and a window holding entries but a count of
    // zero returned `None`, losing them outright.
    //
    // Drive records from several threads while a closer runs continuously, then
    // check the documented invariant on EVERY window and the conservation of the
    // total across all of them.
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::Arc;

    let recorder = Arc::new(deterministic_recorder());
    let stop = Arc::new(AtomicBool::new(false));
    let closed_accesses = Arc::new(AtomicU64::new(0));
    let violations = Arc::new(AtomicU64::new(0));

    let closer = {
        let recorder = Arc::clone(&recorder);
        let stop = Arc::clone(&stop);
        let closed_accesses = Arc::clone(&closed_accesses);
        let violations = Arc::clone(&violations);
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                // Yield between closes. A hot spin here does not sharpen the race —
                // it just makes every writer contend with a full 3 MiB table reset,
                // which dominated the runtime without exercising anything more.
                std::thread::yield_now();
                if let Some(s) = recorder.close_window() {
                    // The invariant `WindowSummary` documents.
                    if s.recorded_accesses < s.total_accesses() {
                        violations.fetch_add(1, Ordering::Relaxed);
                    }
                    closed_accesses.fetch_add(s.recorded_accesses, Ordering::Relaxed);
                }
            }
        })
    };

    const THREADS: u64 = 4;
    const PER_THREAD: u64 = 5_000;
    let writers: Vec<_> = (0..THREADS)
        .map(|t| {
            let recorder = Arc::clone(&recorder);
            std::thread::spawn(move || {
                for i in 0..PER_THREAD {
                    let key = (t << 32) | i;
                    recorder.record(SCOPE, &key.to_be_bytes(), AccessWeight::SuccessorGap(512));
                }
            })
        })
        .collect();
    for w in writers {
        w.join().expect("writer thread");
    }
    stop.store(true, Ordering::Relaxed);
    closer.join().expect("closer thread");
    if let Some(s) = recorder.close_window() {
        assert!(s.recorded_accesses >= s.total_accesses());
        closed_accesses.fetch_add(s.recorded_accesses, Ordering::Relaxed);
    }

    assert_eq!(
        violations.load(Ordering::Relaxed),
        0,
        "no closed window may report fewer recorded accesses than its own table holds"
    );
    assert_eq!(
        closed_accesses.load(Ordering::Relaxed),
        THREADS * PER_THREAD,
        "every access must be banked in exactly one closed window — none lost to a \
         window that closed with a zero count, none double-counted"
    );
}

#[test]
fn a_probe_cluster_below_the_load_factor_widens_instead_of_dropping() {
    // C4: `Insert::Full` is reachable far below the load factor — the probe bound is
    // 64 slots while the expected longest cluster near a 0.75 load factor is several
    // hundred. Dropping there is biased in the dangerous direction: an existing entry
    // is always within 64 probes of home, so only NEW keys are lost, which suppresses
    // the singleton bucket and OVERSTATES concentration.
    //
    // Fill to just under the load factor with the production prefix cap and assert
    // nothing was lost: any probe-cluster `Full` must have widened (unbiased) rather
    // than dropped.
    //
    // That a `Full` is genuinely reachable here — rather than this test passing
    // because the path was never taken — is demonstrated directly at the table
    // level by `a_probe_cluster_reports_full_well_below_the_load_factor`, which
    // constructs a cluster longer than the probe bound on an almost-empty table.
    let r = deterministic_recorder();
    let n = 95_000u64; // just under LOAD_FACTOR_LIMIT (98,304)
    for i in 0..n {
        r.record(
            SCOPE,
            &i.wrapping_mul(0x9e37_79b9_7f4a_7c15).to_be_bytes(),
            AccessWeight::SuccessorGap(256),
        );
    }
    let s = r.close_window().expect("accesses were recorded");
    assert_eq!(s.recorded_accesses, n);
    assert_eq!(
        s.dropped_accesses, 0,
        "a probe cluster must widen the sample, never silently drop a new key"
    );
    // Whatever happened, the two documented invariants hold together: a drop implies
    // the floor was reached, and the count covers the table.
    assert!(s.recorded_accesses >= s.total_accesses());
    assert!(s.dropped_accesses == 0 || s.at_sampling_floor);
}

#[test]
fn the_same_key_in_two_tables_is_two_partitions_not_one_repeat() {
    // F1. ONE recorder serves every table, so an entry identity over raw key bytes
    // alone merges a key shared by two tables — and a tenant/user id shared across
    // tables is ordinary, not a rare collision.
    //
    // The merge is biased twice, both toward "build the cache": two singletons
    // become a `count = 2` entry, so `hittable = accesses - distinct` reports 1
    // where the truth is 0; and the entry keeps the MAX byte weight rather than the
    // sum, so it is under-priced and ranks earlier by access density.
    let r = deterministic_recorder();
    let users = TableScope::new("ks", "users");
    let orders = TableScope::new("ks", "orders");
    let shared_key = b"tenant-42";

    r.record(users, shared_key, AccessWeight::SuccessorGap(1_000));
    r.record(orders, shared_key, AccessWeight::SuccessorGap(3_000));

    let s = r.close_window().expect("accesses were recorded");
    assert_eq!(
        s.distinct_partitions(),
        2,
        "one key in two tables is TWO partitions"
    );
    assert_eq!(
        s.bucket(RepeatBucket::One).distinct(),
        2,
        "both are singletons — a merged entry would report one partition in bucket 2"
    );
    assert_eq!(s.bucket(RepeatBucket::Two).distinct(), 0);
    assert_eq!(
        s.bucket(RepeatBucket::One).accesses,
        2,
        "accesses - distinct = 0: nothing here is cacheable, and a merge would have \
         manufactured one hit"
    );
    assert_eq!(
        s.total_bytes(),
        4_000,
        "each partition contributes its OWN extent; a merged entry would keep only \
         the maximum (3_000) and under-price the working set"
    );

    // Same table + same key is still ONE partition — the scope must not fragment a
    // genuine repeat.
    let r = deterministic_recorder();
    r.record(users, shared_key, AccessWeight::SuccessorGap(1_000));
    r.record(users, shared_key, AccessWeight::SuccessorGap(1_000));
    let s = r.close_window().expect("accesses were recorded");
    assert_eq!(s.distinct_partitions(), 1);
    assert_eq!(s.bucket(RepeatBucket::Two).distinct(), 1);

    // And an unqualified name is a distinct scope from a qualified one rather than
    // silently aliasing onto it.
    assert_ne!(
        TableScope::from_qualified("ks.users"),
        TableScope::from_qualified("users")
    );
    assert_eq!(TableScope::from_qualified("ks.users"), users);
}

// ---------------------------------------------------------------------------
// End-to-end: real committed fixtures through the real read path.
// ---------------------------------------------------------------------------

// TABLE-granular fixture-root resolution shared with the sibling dataset lanes
// (#3220). Declared at file scope because a `#[path]` module inside an inline
// `mod` resolves relative to that inline module's directory.
#[cfg(all(feature = "state_machine", feature = "cli-helpers"))]
#[path = "support/datasets_root.rs"]
mod datasets_root;

#[cfg(all(feature = "state_machine", feature = "cli-helpers"))]
mod end_to_end {
    use cqlite_core::ingestion::{ingest, IngestionConfig};
    use cqlite_core::observability::partition_access::{self, RepeatBucket, WindowSummary};
    use cqlite_core::{Config, Database};
    use std::path::{Path, PathBuf};
    use std::time::Duration;
    use tokio::sync::Mutex;

    use super::datasets_root;
    use datasets_root::{describe_search, sstables_root_for_table};

    /// The probe's enable flag and its window are process-global, so every case
    /// that drives them runs under this lock. (Serialising is not a wall-clock
    /// dependency: each case still closes its window explicitly.)
    static PROBE: Mutex<()> = Mutex::const_new(());

    /// A BIG (`nb`) fixture. Its `Index.db` records NO partition size (no Cassandra
    /// 5.0 index format does), so the extent is measured as the successor gap.
    const BIG: (&str, &str, &str, &str, &str) = (
        "test_compaction_tombstone_ttl",
        "shadow_row_delete",
        "compaction-tombstone-ttl-parity.cql",
        "id",
        "1",
    );
    /// A BTI (`da`) fixture: the `Partitions.db` trie resolves an offset and no
    /// size, so the extent is measured by a strict-ceiling trie successor walk.
    /// Its partition key `pk = 1` is the SAME logical value as the BIG fixture's
    /// `id = 1`, which is what makes the cross-table case below real.
    const BTI: (&str, &str, &str, &str, &str) = (
        "test_da",
        "multiclustering_table",
        "multiclustering-table-bti.cql",
        "pk",
        "1",
    );

    fn resolve(keyspace: &str, table: &str, schema: &str) -> (PathBuf, PathBuf) {
        let root = sstables_root_for_table(keyspace, table).unwrap_or_else(|| {
            panic!(
                "{keyspace}.{table} is COMMITTED to git and must resolve in every \
                 checkout, unconditionally (#3220) — {}",
                describe_search(keyspace, table)
            )
        });
        let schema_path = datasets_root::schema_path(schema).unwrap_or_else(|| {
            panic!(
                "committed schema {schema} is unreadable — it is checkout-relative \
                 source (#3148), so this is a resolution defect, never an absence"
            )
        });
        (root, schema_path)
    }

    async fn open_db(root: &Path, schema: &Path, keyspace: &str) -> Database {
        let cfg = IngestionConfig {
            schema_paths: vec![schema.to_path_buf()],
            data_dir: root.to_path_buf(),
            version_hint: None,
            core_config: Config::default(),
            table_directory_filter: Some(format!("/{keyspace}/")),
        };
        let result = ingest(cfg).await.expect("ingestion succeeds");
        assert!(
            result.schema_load_result.schemas_loaded > 0,
            "schema must load"
        );
        result.database
    }

    /// Drive `repeats` point reads of one existing partition with the probe on, and
    /// return the closed window.
    async fn window_for(
        fixture: (&str, &str, &str, &str, &str),
        repeats: usize,
    ) -> (WindowSummary, usize) {
        let (keyspace, table, schema, pk_column, pk_value) = fixture;
        let (root, schema_path) = resolve(keyspace, table, schema);
        let db = open_db(&root, &schema_path, keyspace).await;
        let sql = format!("SELECT * FROM {keyspace}.{table} WHERE {pk_column} = {pk_value}");

        // Pin the PROCESS-GLOBAL window open (B7). Its default is 60 s, and
        // `close_if_triggered` consults `Instant::elapsed()` on every record — so on
        // a contended gate a >60 s gap between these reads would auto-close the
        // window mid-flow and the assertions below would evaporate. Every close in
        // this file is explicit; none depends on elapsed time.
        partition_access::global().set_window_config(partition_access::WindowConfig {
            duration: Duration::from_secs(86_400),
            max_accesses: u64::MAX,
            ..partition_access::WindowConfig::default()
        });
        // Start from a clean window: close (and discard) anything a sibling left.
        let _ = partition_access::close_window();
        partition_access::set_probe_enabled(Some(true));

        let mut rows = 0usize;
        for _ in 0..repeats {
            let result = db.execute(&sql).await.expect("the point read succeeds");
            rows += result.rows.len();
        }
        let summary = partition_access::close_window();
        partition_access::set_probe_enabled(Some(false));
        (
            summary.expect("the probe must have recorded the point reads"),
            rows,
        )
    }

    /// A BIG point read is counted once and PRICED from its measured successor gap.
    ///
    /// The provenance label matters and is asserted: `successor_gap`, never `index`
    /// — Cassandra 5.0's BIG `Index.db` records no partition size, so a weight
    /// claiming `index` here would be a lie about how the number was obtained.
    #[tokio::test]
    async fn a_big_access_is_counted_once_and_priced_from_its_measured_gap() {
        let _guard = PROBE.lock().await;
        let (summary, rows) = window_for(BIG, 5).await;
        assert!(rows > 0, "the fixture partition must return rows");

        assert_eq!(
            summary.distinct_partitions(),
            1,
            "five reads of ONE partition are one distinct partition, not five — and \
             not one per SSTable generation probed"
        );
        let bucket = summary.bucket(RepeatBucket::FiveToEight);
        assert_eq!(bucket.accesses, 5, "the repeat count is exact");
        assert_eq!(
            bucket.distinct_successor_gap, 1,
            "the extent is MEASURED as the successor gap and labelled as such"
        );
        assert_eq!(
            bucket.distinct_index, 0,
            "Cassandra 5.0's BIG Index.db records no size, so no access may claim \
             an index-supplied weight"
        );
        assert_eq!(bucket.distinct_unavailable, 0);
        assert!(
            bucket.bytes > 0,
            "a measured extent must contribute real on-disk bytes"
        );
        assert_eq!(summary.unavailable_partitions(), 0);
    }

    /// A BTI point read is priced from its measured gap too — via the trie's
    /// strict-ceiling successor walk rather than an `Index.db` scan.
    #[tokio::test]
    async fn a_bti_access_is_priced_from_its_measured_trie_successor_gap() {
        let _guard = PROBE.lock().await;
        let (summary, rows) = window_for(BTI, 3).await;
        assert!(rows > 0, "the fixture partition must return rows");

        assert_eq!(summary.distinct_partitions(), 1);
        let bucket = summary.bucket(RepeatBucket::ThreeToFour);
        assert_eq!(bucket.accesses, 3);
        assert_eq!(
            bucket.distinct_successor_gap, 1,
            "the BTI trie resolves no size, so the extent is measured as the gap"
        );
        assert_eq!(bucket.distinct_index, 0);
        assert_eq!(bucket.distinct_unavailable, 0);
        assert!(bucket.bytes > 0);
        assert_eq!(
            summary.unavailable_fraction(),
            0.0,
            "a measurable extent is not an unavailable one"
        );
    }

    /// The F1 aliasing case through the REAL read path, on real fixtures.
    ///
    /// `test_compaction_tombstone_ttl.shadow_row_delete.id = 1` (BIG) and
    /// `test_da.multiclustering_table.pk = 1` (BTI) are both `int` partition keys
    /// with the value 1, so their raw on-disk key bytes are IDENTICAL. Before the
    /// table became part of the entry identity these two reads of two different
    /// partitions in two different keyspaces merged into a single entry with
    /// `count = 2` — a manufactured repeat, priced at the larger of the two extents.
    #[tokio::test]
    async fn the_same_key_bytes_in_two_real_tables_stay_two_partitions() {
        let _guard = PROBE.lock().await;

        let (big_root, big_schema) = resolve(BIG.0, BIG.1, BIG.2);
        let big_db = open_db(&big_root, &big_schema, BIG.0).await;
        let (bti_root, bti_schema) = resolve(BTI.0, BTI.1, BTI.2);
        let bti_db = open_db(&bti_root, &bti_schema, BTI.0).await;

        partition_access::global().set_window_config(partition_access::WindowConfig {
            duration: Duration::from_secs(86_400),
            max_accesses: u64::MAX,
            ..partition_access::WindowConfig::default()
        });
        let _ = partition_access::close_window();
        partition_access::set_probe_enabled(Some(true));

        let big_rows = big_db
            .execute(&format!(
                "SELECT * FROM {}.{} WHERE {} = {}",
                BIG.0, BIG.1, BIG.3, BIG.4
            ))
            .await
            .expect("BIG point read")
            .rows
            .len();
        let bti_rows = bti_db
            .execute(&format!(
                "SELECT * FROM {}.{} WHERE {} = {}",
                BTI.0, BTI.1, BTI.3, BTI.4
            ))
            .await
            .expect("BTI point read")
            .rows
            .len();

        let summary = partition_access::close_window();
        partition_access::set_probe_enabled(Some(false));
        let summary = summary.expect("both point reads must have been recorded");

        assert!(
            big_rows > 0 && bti_rows > 0,
            "both fixtures must return rows"
        );
        assert_eq!(
            summary.distinct_partitions(),
            2,
            "two tables, two partitions — even though the key BYTES are identical"
        );
        assert_eq!(
            summary.bucket(RepeatBucket::One).distinct(),
            2,
            "both are singletons; a merged identity would report one partition in \
             bucket 2 and invent a cacheable repeat"
        );
        assert_eq!(summary.bucket(RepeatBucket::Two).distinct(), 0);
    }

    /// E1 end-to-end: pricing must not depend on the key cache retaining anything.
    ///
    /// The resolver used to read a key-cache MISS as "this generation did not hold
    /// the key". Invalidating every cached location for the table between the read
    /// and the window close reproduces exactly the eviction the cache does on its
    /// own — and the access must STILL be priced from the authoritative index, not
    /// silently reported as a complete measurement over the generations that
    /// happened to survive.
    #[tokio::test]
    async fn pricing_survives_an_emptied_key_cache() {
        let _guard = PROBE.lock().await;
        let (root, schema) = resolve(BIG.0, BIG.1, BIG.2);
        let db = open_db(&root, &schema, BIG.0).await;
        let sql = format!(
            "SELECT * FROM {}.{} WHERE {} = {}",
            BIG.0, BIG.1, BIG.3, BIG.4
        );

        partition_access::global().set_window_config(partition_access::WindowConfig {
            duration: Duration::from_secs(86_400),
            max_accesses: u64::MAX,
            ..partition_access::WindowConfig::default()
        });
        let _ = partition_access::close_window();
        partition_access::set_probe_enabled(Some(true));

        let rows = db.execute(&sql).await.expect("point read").rows.len();

        // Evict everything the read memoised, process-wide.
        cqlite_core::storage::cache::GlobalKeyOffsetCache::global().invalidate_all();

        let rows2 = db
            .execute(&sql)
            .await
            .expect("second point read")
            .rows
            .len();
        let summary = partition_access::close_window();
        partition_access::set_probe_enabled(Some(false));
        let summary = summary.expect("the reads were recorded");

        assert!(rows > 0 && rows2 > 0);
        assert_eq!(summary.distinct_partitions(), 1);
        assert_eq!(
            summary.unavailable_partitions(),
            0,
            "an emptied key cache must not make a resolvable partition unpriceable"
        );
        assert!(
            summary.total_bytes() > 0,
            "the extent comes from the index, not from cache retention"
        );
    }
}
