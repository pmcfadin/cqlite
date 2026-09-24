//! Issue #4204 (`cqlite diagnose`), spec R1: every reported number states its
//! source; nothing unmeasured renders as a bare zero (the #4159 class).
//!
//! Two layers, deliberately:
//! - A UNIT-LEVEL check against the EXACT `SourcedField` mapping functions
//!   `diagnose_generation` calls (`max_timestamp_field`/`compression_ratio_field`/
//!   `pending_repair_field`), driven with a SYNTHETIC `Option::None` — issue
//!   #1653's documented "not authoritatively decoded" state — because no fixture
//!   in the committed corpus carries that legacy layout, and fabricating a whole
//!   on-disk SSTable generation just to reach one `None` branch would test the
//!   file-discovery/reader-open plumbing, not the provenance mapping (R1.1's
//!   actual subject). This calls PRODUCTION code, not a test-local duplicate.
//! - An END-TO-END check against a real, committed `test_basic/simple_table`
//!   generation, proving the MEASURED path is wired through `diagnose_table`
//!   itself (wiring evidence, not just unit coverage of the mapping helper).

#[path = "support/datasets_root.rs"]
mod datasets_root;

use cqlite_core::parser::repair_metadata::{RepairField, RepairMetadata};
use cqlite_core::parser::statistics::{RowStatistics, SSTableStatistics, StatisticsHeader, TimestampStatistics};
use cqlite_core::storage::sstable::diagnose::{
    compression_ratio_field, diagnose_table, max_timestamp_field, pending_repair_field,
    DiagnoseOptions, FieldSource, SourcedField,
};
use std::collections::HashMap;

/// Minimal, otherwise-benign `SSTableStatistics` for exercising ONE field's
/// mapping at a time — mirrors the pattern already used by
/// `parser::statistics::tests::create_test_statistics`.
fn minimal_statistics() -> SSTableStatistics {
    SSTableStatistics {
        header: StatisticsHeader {
            version: 4,
            statistics_kind: 0,
            data_length: 0,
            metadata1: 0,
            metadata2: 0,
            metadata3: 0,
            checksum: 0,
            table_id: None,
        },
        row_stats: RowStatistics {
            total_rows: 0,
            live_rows: 0,
            tombstone_count: 0,
            partition_count: 42,
            avg_rows_per_partition: 0.0,
            row_size_histogram: vec![],
        },
        timestamp_stats: TimestampStatistics {
            min_timestamp: 1_000_000,
            max_timestamp: Some(2_000_000),
            min_deletion_time: 0,
            max_deletion_time: i64::MAX,
            min_ttl: None,
            max_ttl: None,
            rows_with_ttl: None,
        },
        column_stats: vec![],
        table_stats: None,
        partition_stats: None,
        compression_stats: None,
        metadata: HashMap::new(),
        serialization_header_columns: vec![],
        serialization_header_partition_keys: vec![],
        serialization_header_clustering_keys: vec![],
        tombstone_drop_times: vec![],
    }
}

/// R1.1: a generation whose Statistics.db layout does not carry `max_timestamp`
/// (issue #1653's `None` case) renders `{"value": null, "source": "unmeasured",
/// "cause": "..."}`, never a bare `0`/`null`.
#[test]
fn max_timestamp_none_renders_unmeasured_with_cause() {
    let mut stats = minimal_statistics();
    stats.timestamp_stats.max_timestamp = None;

    match max_timestamp_field(&stats) {
        SourcedField::Unmeasured { cause } => {
            assert!(!cause.is_empty(), "cause must be a non-empty explanation");
        }
        other => panic!("expected Unmeasured for max_timestamp == None, got {other:?}"),
    }
}

/// Non-vacuous positive path: a REAL max_timestamp renders Measured with source
/// `statistics`, never Unmeasured.
#[test]
fn max_timestamp_some_renders_measured_statistics() {
    let stats = minimal_statistics();
    match max_timestamp_field(&stats) {
        SourcedField::Measured { value, source } => {
            assert_eq!(value, Some(2_000_000));
            assert_eq!(source, FieldSource::Statistics);
        }
        other => panic!("expected Measured, got {other:?}"),
    }
}

/// R1: compression_ratio must render Unmeasured (never a fabricated 0.0/1.0)
/// when the STATS component itself is absent.
#[test]
fn compression_ratio_none_renders_unmeasured() {
    match compression_ratio_field(None) {
        SourcedField::Unmeasured { cause } => assert!(!cause.is_empty()),
        other => panic!("expected Unmeasured for a missing STATS component, got {other:?}"),
    }
}

/// Non-vacuous positive path: a decoded ratio renders Measured/statistics.
#[test]
fn compression_ratio_some_renders_measured() {
    match compression_ratio_field(Some(0.5)) {
        SourcedField::Measured { value, source } => {
            assert_eq!(value, Some(0.5));
            assert_eq!(source, FieldSource::Statistics);
        }
        other => panic!("expected Measured, got {other:?}"),
    }
}

/// R1's THIRD state: a genuinely decoded ABSENCE ("Pending repair: --") must be
/// distinguishable from "we don't know" — both are `value: None`-shaped, but
/// only one is `Unmeasured`. Conflating them was exactly the #4159 class this
/// spec closes.
#[test]
fn pending_repair_decoded_none_is_measured_absent_not_unmeasured() {
    let repair = RepairMetadata {
        repaired_at: 0,
        pending_repair: RepairField::Decoded(None),
        is_transient: RepairField::Decoded(false),
        repaired_at_decoded: true,
    };
    match pending_repair_field(&repair) {
        SourcedField::Measured { value, source } => {
            assert_eq!(value, None, "a decoded absence carries value=None");
            assert_eq!(source, FieldSource::Statistics);
        }
        other => panic!(
            "a genuinely DECODED absence must be Measured{{value:None}}, not {other:?} \
             (conflating decoded-absent with unmeasured is the #4159 class)"
        ),
    }
}

/// The Unparsed case (genuinely unreachable by the version-gated walk) IS
/// Unmeasured — the mirror-image check proving the two states are not
/// accidentally swapped.
#[test]
fn pending_repair_unparsed_is_unmeasured() {
    let repair = RepairMetadata {
        repaired_at: 0,
        pending_repair: RepairField::Unparsed,
        is_transient: RepairField::Unparsed,
        repaired_at_decoded: true,
    };
    match pending_repair_field(&repair) {
        SourcedField::Unmeasured { cause } => assert!(!cause.is_empty()),
        other => panic!("expected Unmeasured for RepairField::Unparsed, got {other:?}"),
    }
}

/// End-to-end wiring evidence: `diagnose_table` over a real, committed
/// `test_basic/simple_table` generation populates every cheap-tier field with a
/// `source`, and the always-decoded fields (min/max timestamp, LDTs) are never
/// unmeasured for a healthy fixture.
#[tokio::test]
async fn diagnose_table_end_to_end_every_field_carries_a_source() {
    let Some(dir) = datasets_root::table_generation_dirs(
        &match datasets_root::sstables_root_for_table("test_basic", "simple_table") {
            Some(root) => root,
            None => {
                eprintln!(
                    "skip: {}",
                    datasets_root::describe_search("test_basic", "simple_table")
                );
                return;
            }
        },
        "test_basic",
        "simple_table",
    )
    .into_iter()
    .next() else {
        panic!("test_basic/simple_table root resolved but no generation dir found");
    };

    let options = DiagnoseOptions {
        now_secs: 1_700_000_000,
        deep: false,
        top_n: 5,
        gc_grace_seconds: None,
        schema: None,
    };
    let report = diagnose_table(&dir, &options)
        .await
        .expect("diagnose_table must succeed for a healthy committed fixture");

    assert!(!report.generations.is_empty(), "must_run: fixture is committed");
    for gen in &report.generations {
        assert!(
            gen.max_timestamp.is_measured(),
            "a healthy nb fixture must have a measured max_timestamp"
        );
        assert!(
            gen.estimated_partition_count.is_measured(),
            "estimated_partition_count must always be measured (Statistics.db's own histogram sum)"
        );
        // `compression_ratio` decodes the STATS component's own
        // `compressionRatio` f64 field directly (issue #4204), which every
        // Cassandra 5.0 format writes unconditionally, so a healthy fixture's
        // ratio is always Measured — matching `sstablemetadata`'s own printed
        // "Compression ratio: …" line (never a fabricated 0.0/1.0 either way).
        assert!(
            gen.compression_ratio.is_measured(),
            "a healthy fixture's STATS component always carries compressionRatio"
        );
        // Never a bare unexplained zero for the ratio: it must be EITHER a real
        // Measured value OR Unmeasured WITH a cause.
        match &gen.estimated_droppable_tombstone_ratio {
            SourcedField::Measured { .. } => {}
            SourcedField::Unmeasured { cause } => assert!(!cause.is_empty()),
        }
    }
}
