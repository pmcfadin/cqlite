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
    decision, AccessWeight, PartitionAccessRecorder, Refusal, RepeatBucket, Verdict, WindowConfig,
};
use std::time::Duration;

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
    r.record(b"p", AccessWeight::SuccessorGap(1_000));
    r.record(b"p", AccessWeight::SuccessorGap(3_000));
    r.record(b"p", AccessWeight::SuccessorGap(2_000));
    let s = r.close_window().expect("accesses were recorded");
    assert_eq!(s.bucket(RepeatBucket::ThreeToFour).bytes, 3_000);
    assert_eq!(s.total_bytes(), 3_000);
}

#[test]
fn an_unavailable_access_is_counted_as_a_partition_and_contributes_no_bytes() {
    let r = deterministic_recorder();
    r.record(b"bti-resolved", AccessWeight::Unavailable);
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
    r.record(b"p", AccessWeight::SuccessorGap(4_096));
    r.record(b"p", AccessWeight::Unavailable);
    r.record(b"p", AccessWeight::SuccessorGap(4_096));
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
        r.record(&i.to_be_bytes(), AccessWeight::SuccessorGap(2_048));
    }
    for i in 0..50u64 {
        r.record(&(i | 1 << 40).to_be_bytes(), AccessWeight::Unavailable);
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
    match decision::evaluate(
        &s,
        decision::WindowSource::Field,
        128 * 1024 * 1024,
        decision::ASSUMED_DECODE_MULTIPLIER,
    ) {
        Verdict::Refused(Refusal::SamplingFloor { sample_denominator }) => {
            assert!(sample_denominator > 1);
        }
        other => panic!("a floored window must be refused, not priced: got {other:?}"),
    }
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
            r.record(&i.to_be_bytes(), AccessWeight::SuccessorGap(1_024));
        }
    }
    for i in 0..10_000u64 {
        r.record(
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
            r.record(&i.to_be_bytes(), AccessWeight::SuccessorGap(1_024));
        }
    }
    for i in 0..10_000u64 {
        r.record(
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

    /// A BIG (`nb`) fixture: `Index.db` resolves an authoritative partition size.
    const BIG: (&str, &str, &str, &str, &str) = (
        "test_compaction_tombstone_ttl",
        "shadow_row_delete",
        "compaction-tombstone-ttl-parity.cql",
        "id",
        "1",
    );
    /// A BTI (`da`) fixture: the `Partitions.db` trie resolves an offset and no
    /// size, so the read path records `data_size = 0`.
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
}
