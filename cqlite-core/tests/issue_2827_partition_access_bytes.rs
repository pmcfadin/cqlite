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
//! # Open finding: neither Cassandra 5.0 index format records a partition size
//!
//! The approved design expected BIG (`Index.db`) to supply an authoritative
//! per-partition size and only BTI to fail closed. It does not. A Cassandra 5.0
//! BIG index entry is
//! `[key][data_offset vint][promoted_index_len vint][promoted_index]`
//! (`docs/sstables-definitive-guide/chapters/06-index-and-summary.md`) with no size
//! field — which is why the reader's own seek path bounds a partition by the
//! SUCCESSOR offset. So `PartitionLoc.data_size` is `0` on both formats, every
//! access reports `unavailable`, and the decision procedure refuses every real
//! window on its unpriceable-fraction condition. The end-to-end cases below pin
//! that ACTUAL behaviour; the recorder-level cases pin the byte semantics the
//! weighting will have once an authoritative extent is available.
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
        r.record(b"partition-of-known-size", AccessWeight::Index(65_536));
    }
    let s = r.close_window().expect("accesses were recorded");

    let bucket = s.bucket(RepeatBucket::NineToSixteen);
    assert_eq!(bucket.distinct_index, 1, "one distinct partition");
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
    r.record(b"p", AccessWeight::Index(1_000));
    r.record(b"p", AccessWeight::Index(3_000));
    r.record(b"p", AccessWeight::Index(2_000));
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
    r.record(b"p", AccessWeight::Index(4_096));
    r.record(b"p", AccessWeight::Unavailable);
    r.record(b"p", AccessWeight::Index(4_096));
    let s = r.close_window().expect("accesses were recorded");
    assert_eq!(s.bucket(RepeatBucket::ThreeToFour).distinct_unavailable, 1);
    assert_eq!(s.bucket(RepeatBucket::ThreeToFour).bytes, 0);
}

#[test]
fn a_mixed_window_makes_its_incompleteness_visible_and_the_procedure_refuses_it() {
    let r = deterministic_recorder();
    // Enough accesses to clear the procedure's minimum, so the refusal below is
    // about the unpriceable fraction and nothing else.
    for i in 0..12_000u64 {
        r.record(&i.to_be_bytes(), AccessWeight::Index(2_048));
    }
    for i in 0..50u64 {
        r.record(&(i | 1 << 40).to_be_bytes(), AccessWeight::Unavailable);
    }
    let s = r.close_window().expect("accesses were recorded");

    // Both arms present with non-zero values.
    let priced: u64 = RepeatBucket::ALL
        .iter()
        .map(|b| s.bucket(*b).distinct_index)
        .sum();
    assert!(priced > 0, "the BIG-resolved partitions must be priced");
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
            AccessWeight::Index(64),
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

    /// A BIG point read is COUNTED, and — today — is not priceable either.
    ///
    /// # An open finding against the approved design (issue #2827)
    ///
    /// The design expected this case to report `size_source = "index"` with the
    /// partition's on-disk bytes, on the premise that "BIG (`Index.db`) resolves
    /// both fields". **That premise is false for Cassandra 5.0.** A BIG index entry
    /// is `[key][data_offset vint][promoted_index_len vint][promoted_index]`
    /// (`docs/sstables-definitive-guide/chapters/06-index-and-summary.md`) — there
    /// is no per-partition size field, which is exactly why the reader's own seek
    /// path bounds a partition with the SUCCESSOR offset and calls that its
    /// authoritative end bound.
    ///
    /// So `PartitionLoc.data_size` is `0` for BIG as well as BTI, and this test
    /// pins what the shipped instrument ACTUALLY does rather than what the design
    /// hoped for: the access is counted exactly once and reported unpriceable. The
    /// histogram — the instrument's primary deliverable — is unaffected. Closing
    /// the byte gap means sourcing the extent from the successor offset, which the
    /// approved design deferred; it is not taken unilaterally here.
    #[tokio::test]
    async fn a_big_resolved_access_is_counted_once_and_reports_its_price_honestly() {
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
            bucket.distinct_index, 0,
            "Cassandra 5.0's BIG Index.db carries no partition size, so no access \
             through it can claim an authoritative weight"
        );
        assert_eq!(bucket.distinct_unavailable, 1);
        assert_eq!(
            summary.total_bytes(),
            0,
            "a size that the format does not record is reported missing, never \
             estimated or defaulted"
        );
    }

    /// A BTI point read is marked unavailable and priced at nothing.
    #[tokio::test]
    async fn a_bti_resolved_access_is_unavailable_and_contributes_no_bytes() {
        let _guard = PROBE.lock().await;
        let (summary, rows) = window_for(BTI, 3).await;
        assert!(rows > 0, "the fixture partition must return rows");

        assert_eq!(summary.distinct_partitions(), 1);
        let bucket = summary.bucket(RepeatBucket::ThreeToFour);
        assert_eq!(
            bucket.distinct_unavailable, 1,
            "the BTI trie resolves an offset with no size, so the access is counted \
             but not priced"
        );
        assert_eq!(bucket.distinct_index, 0);
        assert_eq!(bucket.accesses, 3);
        assert_eq!(
            summary.total_bytes(),
            0,
            "no non-zero size may be recorded for a BTI partition from any source"
        );
        assert!(summary.unavailable_fraction() > 0.0);
    }
}
