//! Bucket-histogram recovery for the bounded partition access-distribution probe
//! (issue #2827).
//!
//! # What this change delivers, and what it does not
//!
//! Issue #2827 as re-scoped delivers **the instrument and the procedure, not the
//! field number**. Its original AC2 — "decides whether a 64–128 MiB
//! decoded-partition cache clears a useful hit ratio" — is **NOT satisfied** by
//! this change. Not waived, not deferred to another issue: it becomes satisfiable
//! on the first real keyed workload run with the probe enabled, and the reason it
//! cannot be satisfied here is that **no field keyed workload with captured
//! concentration exists** (`docs/research/phase2-verify-caching.md:214-216`).
//!
//! # Why a synthetic input is a LEGITIMATE oracle here — and where its licence ends
//!
//! Every access sequence in this file is one I constructed, and every expected
//! value is computed from that sequence by hand — arithmetic that does not pass
//! through the instrument — so these tests are capable of failing.
//!
//! That makes a synthetic input a legitimate oracle for a claim about the
//! **instrument** ("it recovers a distribution I control") and an **illegitimate**
//! oracle for a claim about the **world** ("the field workload is skewed"). The
//! asymmetry is the same one CLAUDE.md records twice: a CQLite-written +
//! CQLite-read round trip is invariant to a uniform error because both sides share
//! the assumption (#3042), and a physical-dump oracle cannot see a
//! read-reconciliation bug because it is blind to the property under test (#1742).
//! A hit-ratio-vs-skew curve over a distribution I chose is the same object — my
//! assumption in, my conclusion out, nothing in it can fail.
//!
//! So: **nothing in this file asserts a hit ratio, a cache size, or a skew
//! parameter**, and no output of these tests may be cited as evidence about a real
//! workload. The executable decision procedure refuses a synthetic window by
//! construction; see `issue_2827_partition_access_bytes.rs` and
//! `docs/research/decoded-partition-cache-decision.md`.

use cqlite_core::observability::partition_access::{
    AccessWeight, PartitionAccessRecorder, RepeatBucket, TableScope, WindowConfig, WindowSummary,
};
use std::time::Duration;

/// A recorder whose window NEVER closes on its own: the duration bound is a day
/// out and the access bound is unreachable, so every window in this file is closed
/// by an explicit `close_window()`.
///
/// This is deliberate, not incidental. A correctness assertion that depends on
/// elapsed wall time is the mechanized `roborev-lints` failure class (#2642); the
/// deterministic close hook exists precisely so no test here has to sleep.
/// One table for every recorder-level case here.
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

/// Drive `count` accesses to the partition identified by `id`.
fn access(r: &PartitionAccessRecorder, id: u64, count: u32, bytes: u64) {
    for _ in 0..count {
        r.record(SCOPE, &id.to_be_bytes(), AccessWeight::SuccessorGap(bytes));
    }
}

/// Distinct partitions reported in a bucket, priced or not.
fn distinct(s: &WindowSummary, b: RepeatBucket) -> u64 {
    s.bucket(b).distinct()
}

#[test]
fn access_counts_land_in_the_correct_bucket_at_every_boundary() {
    let r = deterministic_recorder();

    // Eight distinct partitions, accessed 1, 2, 3, 4, 8, 9, 16 and 17 times.
    // Expected placement, computed from the input by hand against the six declared
    // ranges (1 | 2 | 3-4 | 5-8 | 9-16 | 17+) — NOT by asking the instrument:
    //
    //   count  1 →  "1"      count  4 → "3-4"     count  9 → "9-16"
    //   count  2 →  "2"      count  8 → "5-8"     count 16 → "9-16"
    //   count  3 →  "3-4"                         count 17 → "17+"
    //
    // so distinct = { "1":1, "2":1, "3-4":2, "5-8":1, "9-16":2, "17+":1 }
    // and accesses = { "1":1, "2":2, "3-4":3+4=7, "5-8":8, "9-16":9+16=25,
    //                  "17+":17 }.
    //
    // (The spec scenario's prose transposes the `5-8` and `9-16` distinct counts;
    // the arithmetic above is what the stated input actually produces, and the
    // scenario's own worked figures — `3-4` reporting 7 accesses and `17+`
    // reporting 17 — agree with it.)
    let counts: [(u64, u32); 8] = [
        (1, 1),
        (2, 2),
        (3, 3),
        (4, 4),
        (5, 8),
        (6, 9),
        (7, 16),
        (8, 17),
    ];
    for (id, times) in counts {
        access(&r, id, times, 1_024);
    }

    let s = r.close_window().expect("the window recorded accesses");

    assert_eq!(distinct(&s, RepeatBucket::One), 1);
    assert_eq!(distinct(&s, RepeatBucket::Two), 1);
    assert_eq!(distinct(&s, RepeatBucket::ThreeToFour), 2);
    assert_eq!(distinct(&s, RepeatBucket::FiveToEight), 1);
    assert_eq!(distinct(&s, RepeatBucket::NineToSixteen), 2);
    assert_eq!(distinct(&s, RepeatBucket::SeventeenPlus), 1);
    assert_eq!(s.distinct_partitions(), 8);

    assert_eq!(s.bucket(RepeatBucket::One).accesses, 1);
    assert_eq!(s.bucket(RepeatBucket::Two).accesses, 2);
    assert_eq!(s.bucket(RepeatBucket::ThreeToFour).accesses, 7);
    assert_eq!(s.bucket(RepeatBucket::FiveToEight).accesses, 8);
    assert_eq!(s.bucket(RepeatBucket::NineToSixteen).accesses, 25);
    assert_eq!(s.bucket(RepeatBucket::SeventeenPlus).accesses, 17);
    assert_eq!(s.total_accesses(), 1 + 2 + 3 + 4 + 8 + 9 + 16 + 17);
}

#[test]
fn a_known_skewed_distribution_is_recovered_exactly() {
    let r = deterministic_recorder();

    // 10 partitions × 20 accesses, 100 × 3, 1,000 × 1. Hand-computed expectation:
    //   "17+"  : 10 distinct,   10 × 20 =   200 accesses
    //   "3-4"  : 100 distinct,  100 × 3 =   300 accesses
    //   "1"    : 1,000 distinct, 1,000 × 1 = 1,000 accesses
    //   everything else: zero.
    let mut id = 0u64;
    for _ in 0..10 {
        id += 1;
        access(&r, id, 20, 2_048);
    }
    for _ in 0..100 {
        id += 1;
        access(&r, id, 3, 2_048);
    }
    for _ in 0..1_000 {
        id += 1;
        access(&r, id, 1, 2_048);
    }

    let s = r.close_window().expect("the window recorded accesses");
    assert!(
        s.is_census(),
        "1,110 distinct keys must not force a downsample"
    );

    assert_eq!(distinct(&s, RepeatBucket::One), 1_000);
    assert_eq!(distinct(&s, RepeatBucket::ThreeToFour), 100);
    assert_eq!(distinct(&s, RepeatBucket::SeventeenPlus), 10);
    assert_eq!(distinct(&s, RepeatBucket::Two), 0);
    assert_eq!(distinct(&s, RepeatBucket::FiveToEight), 0);
    assert_eq!(distinct(&s, RepeatBucket::NineToSixteen), 0);

    assert_eq!(s.bucket(RepeatBucket::One).accesses, 1_000);
    assert_eq!(s.bucket(RepeatBucket::ThreeToFour).accesses, 300);
    assert_eq!(s.bucket(RepeatBucket::SeventeenPlus).accesses, 200);
    assert_eq!(s.total_accesses(), 1_500);
}

#[test]
fn a_uniform_distribution_is_not_reported_as_concentrated() {
    let r = deterministic_recorder();
    for id in 0..5_000u64 {
        access(&r, id, 1, 512);
    }

    let s = r.close_window().expect("the window recorded accesses");
    assert!(s.is_census());
    assert_eq!(distinct(&s, RepeatBucket::One), 5_000);
    for b in RepeatBucket::ALL.iter().skip(1) {
        assert_eq!(distinct(&s, *b), 0, "bucket {} must be empty", b.label());
    }
    assert_eq!(s.total_accesses(), 5_000);
    // Deliberately NO assertion about a hit ratio or a cache size: this test says
    // what the instrument recovered, never what a cache would do.
}

#[test]
fn closing_a_window_emits_once_and_resets() {
    let r = deterministic_recorder();
    access(&r, 1, 5, 100);
    access(&r, 2, 1, 100);

    let first = r
        .close_window()
        .expect("the first window recorded accesses");
    assert_eq!(distinct(&first, RepeatBucket::FiveToEight), 1);
    assert_eq!(distinct(&first, RepeatBucket::One), 1);

    // A second close with no intervening accesses is silent — the window is
    // tumbling, so the first close already consumed and reset everything.
    assert_eq!(
        r.close_window(),
        None,
        "a re-close with no accesses must emit nothing"
    );

    // The next window starts empty: a partition from the previous window is absent
    // unless it is accessed again.
    access(&r, 3, 1, 100);
    let second = r
        .close_window()
        .expect("the second window recorded an access");
    assert_eq!(second.distinct_partitions(), 1);
    assert_eq!(distinct(&second, RepeatBucket::FiveToEight), 0);
    assert_eq!(second.total_accesses(), 1);
}

#[test]
fn an_empty_window_is_silent() {
    let r = deterministic_recorder();
    assert_eq!(
        r.close_window(),
        None,
        "a window with no subject has no measurement to report"
    );
    assert_eq!(
        r.footprint_bytes(),
        0,
        "closing an untouched window must not allocate the table"
    );
}

#[test]
fn memory_does_not_grow_with_the_number_of_distinct_partitions() {
    // A disabled/untouched recorder holds nothing at all.
    let small = deterministic_recorder();
    assert_eq!(small.footprint_bytes(), 0);

    for id in 0..1_000u64 {
        access(&small, id, 1, 64);
    }
    let small_footprint = small.footprint_bytes();
    assert_eq!(
        small_footprint,
        PartitionAccessRecorder::declared_footprint_bytes()
    );

    let large = deterministic_recorder();
    for id in 0..500_000u64 {
        access(&large, id, 1, 64);
    }
    assert_eq!(
        large.footprint_bytes(),
        small_footprint,
        "500x the distinct partitions must occupy the identical fixed footprint"
    );
    assert_eq!(small_footprint, 3 * 1024 * 1024);

    // And the large window really did overflow into sampling rather than growing.
    let s = large.close_window().expect("accesses were recorded");
    assert!(
        s.sample_denominator > 1,
        "500,000 distinct partitions must not fit a 131,072-slot table as a census"
    );
}

#[test]
fn downsampling_preserves_the_bucket_fractions_and_declares_its_scale() {
    // A known two-population distribution over enough distinct keys to force at
    // least one downsample: 200,000 singletons and 20,000 partitions accessed 10
    // times each. Hand-computed shares of the DISTINCT population:
    //   bucket "1"    : 200,000 / 220,000 = 0.909090…
    //   bucket "9-16" :  20,000 / 220,000 = 0.090909…
    // The admission predicate is a function of the key hash alone, so it is
    // independent of a key's access frequency and these SHARES survive sampling
    // (the absolute counts do not — that is what `sample_denominator` publishes).
    let r = deterministic_recorder();
    let mut id = 0u64;
    for _ in 0..200_000u64 {
        id += 1;
        access(&r, id, 1, 128);
    }
    for _ in 0..20_000u64 {
        id += 1;
        access(&r, id, 10, 128);
    }

    let s = r.close_window().expect("accesses were recorded");
    assert!(
        s.sample_denominator > 1,
        "220,000 distinct keys must force at least one downsample"
    );
    assert!(!s.is_census());
    assert!(
        !s.at_sampling_floor,
        "220,000 keys is far from the sampling floor"
    );

    let total = s.distinct_partitions() as f64;
    assert!(total > 1_000.0, "the surviving sample must not be tiny");
    let ones = distinct(&s, RepeatBucket::One) as f64 / total;
    let tens = distinct(&s, RepeatBucket::NineToSixteen) as f64 / total;

    // Tolerance: a 1-in-2^k uniform sample of 220,000 keys leaves tens of thousands
    // of survivors, so a 2-percentage-point band is generous but still capable of
    // catching a frequency-correlated (i.e. biased) admission rule, which would
    // move these shares by tens of points.
    assert!(
        (ones - 0.909_090).abs() < 0.02,
        "singleton share {ones} must track the known 0.909 within tolerance"
    );
    assert!(
        (tens - 0.090_909).abs() < 0.02,
        "repeat share {tens} must track the known 0.0909 within tolerance"
    );
    assert_eq!(
        distinct(&s, RepeatBucket::Two)
            + distinct(&s, RepeatBucket::ThreeToFour)
            + distinct(&s, RepeatBucket::FiveToEight)
            + distinct(&s, RepeatBucket::SeventeenPlus),
        0,
        "the input has no partition outside the 1 and 9-16 buckets"
    );

    // No survivor's count may be UNDER-recorded: every admitted 10-access
    // partition must still report 10 accesses, so its bucket's accesses/distinct
    // ratio is exactly 10.
    let nine_to_sixteen = s.bucket(RepeatBucket::NineToSixteen);
    assert_eq!(
        nine_to_sixteen.accesses,
        nine_to_sixteen.distinct() * 10,
        "a survivor's count must stay exact across a downsample"
    );
    let ones_bucket = s.bucket(RepeatBucket::One);
    assert_eq!(ones_bucket.accesses, ones_bucket.distinct());
}

#[test]
fn recorded_accesses_counts_every_access_including_unadmitted_keys() {
    let r = deterministic_recorder();
    for id in 0..300_000u64 {
        access(&r, id, 1, 32);
    }
    let s = r.close_window().expect("accesses were recorded");
    assert_eq!(
        s.recorded_accesses, 300_000,
        "every access the recorder was asked to record is counted, whether or not \
         its key survived the sampling predicate"
    );
    assert!(
        s.total_accesses() < s.recorded_accesses,
        "the admitted sample is a subset once downsampling has kicked in"
    );
}

/// The corrected documentation of `cqlite.read.partition_lookup.total` matches what
/// the code emits (issue #2827's bundled correction).
///
/// This change's central factual claim is about that counter's attribute set, and
/// the tree stated it three ways: `docs/observability/configuration.md` and the
/// `otel.rs` instrument description both said `cqlite.query.access_path`, while the
/// catalog doc and every emission site attach `cqlite.read.lookup_route`. The code
/// is authoritative and the docs were corrected to match — so this asserts the
/// corrected fact at its source, against the constants the emission sites use,
/// rather than leaving the correction unpinned.
#[test]
fn the_corrected_partition_lookup_attribute_documentation_matches_the_code() {
    use cqlite_core::observability::catalog;

    // The emission sites are the authority for the attribute SET. Assert against
    // the module source so this holds with or without an OTel exporter installed,
    // and so a future site that attaches `access_path` to this counter fails here.
    //
    // ALL THREE emission sites, not a hand-picked pair: a census that misses one is
    // a guard with a hole, and `bti_lookup_memo.rs` (the C3 memo-hit path, which
    // re-emits the presence decision a skipped descent would have) was exactly that.
    //
    // `data_access/full_index_stream.rs` is deliberately NOT here: it only NAMES the
    // counter in a comment explaining that the path it describes is accounted for
    // elsewhere — it emits nothing. Including it would put a non-emitting file in a
    // census of emitters, which is how a list stops meaning what it says.
    //
    // The list is hand-maintained (a source scan cannot enumerate modules), so the
    // completeness assertion below fails loudly if an entry stops emitting.
    let sites = [
        include_str!("../src/storage/sstable/reader/partition_lookup.rs"),
        include_str!("../src/storage/sstable/reader/summary_point.rs"),
        include_str!("../src/storage/sstable/reader/bti_lookup_memo.rs"),
    ];
    let emitting = sites
        .iter()
        .filter(|src| src.contains("catalog::READ_PARTITION_LOOKUP"))
        .count();
    assert_eq!(
        emitting,
        sites.len(),
        "every file in this census must actually emit the counter — a stale entry \
         hides the fact that a real site went uncovered"
    );
    for src in sites {
        for chunk in src.split("catalog::READ_PARTITION_LOOKUP,").skip(1) {
            // The attribute list follows immediately; bound the scan to it.
            let attrs = chunk.split(");").next().unwrap_or("");
            assert!(
                attrs.contains("attr::RESULT")
                    && attrs.contains("attr::LOOKUP_ROUTE")
                    && attrs.contains("attr::SSTABLE_FORMAT"),
                "every cqlite.read.partition_lookup.total emission must carry exactly \
                 {{result, lookup_route, sstable_format}}; saw: {attrs}"
            );
            assert!(
                !attrs.contains("attr::ACCESS_PATH"),
                "cqlite.read.partition_lookup.total must NOT carry \
                 cqlite.query.access_path — that was the stale documentation this \
                 change corrects; saw: {attrs}"
            );
        }
    }

    // And the corrected published row names the same three keys.
    let published = include_str!("../../docs/observability/configuration.md");
    let row = published
        .lines()
        .find(|l| l.contains("| `cqlite.read.partition_lookup.total` |"))
        .expect("the published row must exist");
    assert!(row.contains(catalog::attr::RESULT), "{row}");
    assert!(row.contains(catalog::attr::LOOKUP_ROUTE), "{row}");
    assert!(row.contains(catalog::attr::SSTABLE_FORMAT), "{row}");
    assert!(
        !row.contains(catalog::attr::ACCESS_PATH),
        "the published row must no longer claim access_path: {row}"
    );

    // The documented access_path value set must contain every label the enum can
    // return — the omission of `streaming_partition_lookup` is the documented source
    // of the "assert on bare partition_lookup" mistake.
    // The value-set row, not the metric row that merely CARRIES the attribute: it
    // is the one whose FIRST column is the attribute key.
    let value_row = published
        .lines()
        .find(|l| l.trim_start().starts_with("| `cqlite.query.access_path` |"))
        .expect("the access_path value-set row must exist");
    for label in [
        "full_scan",
        "partition_lookup",
        "multi_partition_lookup",
        "streaming_partition_lookup",
        "metadata_partition_lookup",
        "clustering_slice",
        "fallback_full_scan",
    ] {
        assert!(
            value_row.contains(label),
            "the documented access_path value set omits {label}: {value_row}"
        );
    }
}

/// The emitted-series shape, read back through the process-global capture harness.
///
/// One test, deliberately: the production metric helpers bind a single global
/// `Meter` and the probe's enable flag is process-global, so every assertion that
/// needs both lives here rather than racing sibling tests.
#[cfg(feature = "observability-testing")]
#[test]
fn emitted_series_carry_only_the_two_declared_bounded_attribute_keys() {
    use cqlite_core::observability::partition_access::SizeSource;
    use cqlite_core::observability::{catalog, partition_access, testing};

    let capture = testing::metrics_capture();
    capture.reset();

    // Pin the PROCESS-GLOBAL window open (C6): its default is 60 s and
    // `close_if_triggered` reads `Instant::elapsed()` on every record, so a stalled
    // runner would auto-close mid-flow and break the counts asserted below.
    partition_access::global().set_window_config(partition_access::WindowConfig {
        duration: std::time::Duration::from_secs(86_400),
        max_accesses: u64::MAX,
        ..partition_access::WindowConfig::default()
    });
    partition_access::set_probe_enabled(Some(true));
    assert!(partition_access::enabled());

    // Distinct, long, high-entropy keys: if anything key-derived ever leaked into
    // an attribute, the series count would explode with the key count.
    let keys: Vec<Vec<u8>> = (0..64u64)
        .map(|i| {
            let mut k = Vec::with_capacity(96);
            for j in 0..12u64 {
                k.extend_from_slice(&(i.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ j).to_be_bytes());
            }
            k
        })
        .collect();
    for (i, k) in keys.iter().enumerate() {
        // A spread of repeat counts so several buckets are populated, plus a
        // couple of unpriced accesses so both `size_source` values appear.
        let times = 1 + (i % 20);
        for _ in 0..times {
            let weight = if i % 8 == 0 {
                AccessWeight::Unavailable
            } else {
                AccessWeight::SuccessorGap(4_096)
            };
            partition_access::record_partition_access(SCOPE, k, weight);
        }
    }
    let summary = partition_access::close_window().expect("accesses were recorded");
    partition_access::set_probe_enabled(Some(false));

    assert_eq!(summary.distinct_partitions(), 64);

    let metrics = capture.flush_and_collect();
    let names = [
        catalog::READ_PARTITION_ACCESS_DISTINCT_PARTITIONS,
        catalog::READ_PARTITION_ACCESS_ACCESSES,
        catalog::READ_PARTITION_ACCESS_BYTES,
        catalog::READ_PARTITION_ACCESS_SAMPLE_DENOMINATOR,
    ];

    // The trustworthiness signals must be EXPORTED, not merely returned from
    // `close_window` — an operator reading dashboards alone has to be able to tell a
    // clean window from a lossy or floored one (C5). This window is clean, so the
    // floor gauge must be present and zero and the drop counter must be absent.
    assert!(
        metrics.contains(catalog::READ_PARTITION_ACCESS_SAMPLING_FLOOR),
        "the sampling-floor gauge must be exported on every closed window"
    );
    assert_eq!(
        metrics.counter_sum(catalog::READ_PARTITION_ACCESS_SAMPLING_FLOOR),
        0.0,
        "this window is nowhere near the sampling cap"
    );
    assert_eq!(
        summary.dropped_accesses, 0,
        "and it lost nothing, so the drop counter stays silent"
    );
    assert_eq!(
        metrics.counter_sum(catalog::READ_PARTITION_ACCESS_DROPPED),
        0.0
    );

    let mut series = 0usize;
    for name in names {
        let Some(entry) = metrics.find(name) else {
            panic!("{name} must be emitted for a window with accesses");
        };
        for point in &entry.points {
            series += 1;
            for (key, value) in &point.attributes {
                assert!(
                    key == catalog::attr::REPEAT_BUCKET || key == catalog::attr::SIZE_SOURCE,
                    "{name} carries an undeclared attribute key {key} (= {value})"
                );
                if key == catalog::attr::REPEAT_BUCKET {
                    assert!(
                        RepeatBucket::ALL.iter().any(|b| b.label() == value),
                        "{value} is not one of the six declared repeat-bucket labels"
                    );
                }
                if key == catalog::attr::SIZE_SOURCE {
                    assert!(
                        SizeSource::ALL.iter().any(|s| s.label() == value),
                        "{value} is not one of the three declared size-source labels"
                    );
                }
            }
        }
    }
    assert!(
        series <= 31,
        "the four metrics may carry at most 6x3 + 6 + 6 + 1 = 31 series regardless \
         of how many distinct partitions were accessed; saw {series}"
    );
    assert!(
        series > 0,
        "the window must actually have emitted something"
    );

    // Both `size_source` arms must be present: the window deliberately mixed
    // priced and unpriced accesses, and the unavailable arm is what makes an
    // incomplete byte total visible instead of silently absorbed.
    assert!(
        metrics.sum_where(
            catalog::READ_PARTITION_ACCESS_DISTINCT_PARTITIONS,
            &[(catalog::attr::SIZE_SOURCE, "unavailable")]
        ) > 0.0
    );
    assert!(
        metrics.sum_where(
            catalog::READ_PARTITION_ACCESS_DISTINCT_PARTITIONS,
            &[(catalog::attr::SIZE_SOURCE, "successor_gap")]
        ) > 0.0
    );
}
