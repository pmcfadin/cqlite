//! Issue #4222 — raw SSTable view over an uncompressed BIG SSTable: real
//! per-cell metadata, not the fabricated-metadata fallback.
//!
//! Roborev finding (High), investigated: `SSTableReader::stream_all_partitions_for_compaction`
//! has a NON-stitching fallback branch (`!requires_chunk_stitching() &&
//! bti_partitions_db.is_none()`) that collapses every row through
//! `CompactionRow::from_legacy_value`, zeroing every live cell's write
//! timestamp and dropping TTL/local-deletion-time — which would be a
//! FABRICATED value (no-heuristics, issue #28) if this view rendered it as
//! an authoritative on-disk fact. The finding worried this triggers for
//! "every uncompressed BIG SSTable, including every SSTable CQLite's own
//! write path emits".
//!
//! **Investigated and empirically NOT reproducible on this real fixture**:
//! `CassandraVersion::data_format()` maps EVERY 'nb'-format variant
//! (`parser/header.rs:221-254`) — including `V5_0Uncompressed` — to
//! `DataFormat::V5CompressedLegacy`, and this real Cassandra 5.0
//! `compression = {'enabled': false}` fixture is classified by its on-disk
//! magic bytes as an `is_nb_format()`-TRUE variant (confirmed empirically
//! below: `requires_chunk_stitching()` is TRUE for it, so the fabricating
//! fallback branch is NOT taken). The theoretical branch remains a real,
//! defensive fail-closed guard
//! (`SSTableReader::compaction_stream_loses_cell_metadata`, wired into both
//! `raw_view` producers) for whatever DOES classify as `is_nb_format() ==
//! false` (a non-`nb` BIG sub-format) — this file documents and pins the
//! ACTUAL behavior on the one real "uncompressed" fixture this repo has,
//! rather than asserting an untriggerable expectation.
//!
//! Fixture: `test_comp.uncompressed_table` (`test-data/schemas/compression-parity.cql`
//! Table 5) — git-committed, `must_run`.

#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

#[path = "support/datasets_root.rs"]
mod datasets_root;

use chrono::DateTime;
use cqlite_core::query::result::QueryRow;
use cqlite_core::types::Value;
use cqlite_core::{ingestion::ingest, ingestion::IngestionConfig, Config, Database};
use datasets_root::{describe_search, schema_path, sstables_root_for_table};

const KEYSPACE: &str = "test_comp";
const TABLE: &str = "uncompressed_table";

async fn open_fixture_db() -> Database {
    let root = sstables_root_for_table(KEYSPACE, TABLE).unwrap_or_else(|| {
        panic!(
            "committed fixture must resolve (issue #3220, fail-closed): {}",
            describe_search(KEYSPACE, TABLE)
        )
    });
    let schema = schema_path("compression-parity.cql")
        .expect("committed schema compression-parity.cql must be readable (#3148)");
    let cfg = IngestionConfig {
        schema_paths: vec![schema],
        data_dir: root,
        version_hint: None,
        core_config: Config::default(),
        table_directory_filter: Some(format!("/{KEYSPACE}/")),
    };
    let result = ingest(cfg).await.expect("ingestion of the fixture");
    assert!(
        result.schema_load_result.schemas_loaded > 0,
        "the committed schema must load"
    );
    result.database
}

fn get<'a>(row: &'a QueryRow, col: &str) -> Option<&'a Value> {
    row.values.get(col)
}

fn bigint_of(row: &QueryRow, col: &str) -> Option<i64> {
    match get(row, col) {
        Some(Value::BigInt(i)) => Some(*i),
        _ => None,
    }
}

/// Both the point-key AND the full-scan raw-view producers must succeed
/// over this real uncompressed fixture (neither takes the fabricating
/// fallback), and the per-cell write timestamp must be the REAL golden
/// value — never zero (the fabricated sentinel `from_legacy_value` would
/// use) and never absent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_scan_and_point_key_surface_real_non_fabricated_metadata() {
    let db = open_fixture_db().await;

    let full_scan = db
        .execute(&format!(
            "SELECT pk, ck, body_timestamp FROM {KEYSPACE}.{TABLE}_raw_sstable_data"
        ))
        .await
        .expect("full scan over this real uncompressed fixture must succeed");
    assert!(
        !full_scan.rows.is_empty(),
        "the fixture must have at least one row"
    );

    let golden_micros = DateTime::parse_from_rfc3339("2021-01-01T00:00:00Z")
        .unwrap()
        .timestamp_micros();

    let pk1_ck1 = full_scan
        .rows
        .iter()
        .find(|r| {
            matches!(get(r, "pk"), Some(Value::Integer(1)))
                && matches!(get(r, "ck"), Some(Value::Integer(1)))
        })
        .expect("pk=1 ck=1 row must be present (golden's first row)");
    let ts = bigint_of(pk1_ck1, "body_timestamp")
        .expect("body_timestamp must be present — never dropped");
    assert_ne!(
        ts, 0,
        "a zero write timestamp would be the FABRICATED sentinel \
         `CompactionRow::from_legacy_value` uses — this real fixture must report the \
         genuine on-disk write time"
    );
    assert_eq!(
        ts, golden_micros,
        "body_timestamp must match the sstabledump golden byte-exact (physical-dump parity, #1742)"
    );

    let point_key = db
        .execute(&format!(
            "SELECT pk, ck, body_timestamp FROM {KEYSPACE}.{TABLE}_raw_sstable_data WHERE pk = 1"
        ))
        .await
        .expect("point-key query must succeed via the real seek path");
    assert!(
        !point_key.rows.is_empty(),
        "pk=1 must resolve to at least one physical row"
    );
    for row in &point_key.rows {
        let ts = bigint_of(row, "body_timestamp");
        assert!(
            ts.is_some() && ts != Some(0),
            "every point-key row's body_timestamp must be real (present, non-zero)"
        );
    }
}
