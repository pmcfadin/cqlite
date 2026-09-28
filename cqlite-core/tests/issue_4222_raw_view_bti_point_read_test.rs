//! Issue #4222 — raw SSTable view over a **BTI (`da`)** SSTable
//! (C intent-audit item R6, scenario B; issue AC1's "one `da` (BTI) table").
//!
//! # The gap this closes
//!
//! Every other #4222 lane reads a BIG (`nb`) SSTable. `raw_view/point.rs`'s
//! `resolve_position` branches on `reader.is_bti()` →
//! `lookup_partition_via_bti_trie(...)`, else `lookup_partition_with_index(...)`;
//! before this file the BTI arm was COMPILED and never EXECUTED by any test,
//! so the spec's "resolve the partition through the existing per-generation
//! point-read primitives … for both BIG (`nb`) and BTI (`da`)" had coverage
//! for exactly one of the two formats.
//!
//! # Why `position` is the path evidence here
//!
//! `position` is populated ONLY on the point-read path (the full-scan
//! producer leaves it `None`), and on a BTI reader it can only come from
//! `lookup_partition_via_bti_trie` — the trie descent itself. So asserting
//! `position` equals the golden's partition offset is a positive,
//! value-level demonstration that the BTI trie descent ran, not merely that
//! the right rows came back. That matters because `point.rs` documents an
//! `IndexUnavailable` degrade that scans one candidate's compaction stream
//! and returns IDENTICAL rows — a row-set assertion alone could not tell the
//! two apart, but `position` can (that path yields no offset).
//!
//! # Fixture: `test_da.wide_table` (`test-data/schemas/wide-table-bti.cql`)
//!
//! Cassandra 5.0 BTI (`da-2-bti`), LZ4, 3 partitions (pk = 1, 2, 3) of 300
//! clustering rows each (ck = 0..299, ~2 KiB `payload`). It is the same
//! fixture issue #953's within-partition traversal guarantee uses — the
//! original BTI decoder returned only the FIRST row of a partition — so a
//! "returns every clustering row" assertion here is a regression test for
//! that class as well as wiring evidence for this view.
//!
//! # Fixture discipline (#3220/#3121)
//!
//! Unlike the `test_tomb`/`test_deltas` lanes, `test_da/wide_table`'s
//! `Data.db` **is git-committed**, so its absence is never a legitimate
//! "corpus not fetched" state: this lane is `must_run` and PANICS rather
//! than skipping (the fail-closed direction #3220 requires for a committed
//! fixture).
//!
//! # Oracle (#1742/#3041/#3042)
//!
//! Row counts, clustering values, write timestamps and partition offsets are
//! all parsed at runtime from the committed `da-2-bti-Data.db.jsonl`
//! sstabledump golden beside the data — never transcribed.

#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

#[path = "support/datasets_root.rs"]
mod datasets_root;

use chrono::DateTime;
use cqlite_core::query::result::QueryRow;
use cqlite_core::types::Value;
use cqlite_core::{ingestion::ingest, ingestion::IngestionConfig, Config, Database};
use datasets_root::{resolve_table_generation_dir, schema_path, sstables_root_for_table};
use serde_json::Value as Json;

const KEYSPACE: &str = "test_da";
const TABLE: &str = "wide_table";
const SSTABLE: &str = "da-2-bti-Data.db";

/// Open the BTI fixture. `must_run`: `test_da/wide_table`'s `Data.db` is
/// git-committed, so "not found" means a broken checkout, never an unfetched
/// corpus — it panics with the search diagnostic instead of skipping.
async fn open_bti_fixture() -> (Database, Vec<Json>) {
    let root = sstables_root_for_table(KEYSPACE, TABLE).unwrap_or_else(|| {
        panic!(
            "'{KEYSPACE}.{TABLE}' is a GIT-COMMITTED fixture, so its absence is a broken \
             checkout, not an unfetched corpus — this lane must_run: {}",
            datasets_root::describe_search(KEYSPACE, TABLE)
        )
    });
    let schema = schema_path("wide-table-bti.cql")
        .expect("committed schema wide-table-bti.cql must be readable (#3148)");
    let cfg = IngestionConfig {
        schema_paths: vec![schema],
        data_dir: root,
        version_hint: None,
        core_config: Config::default(),
        table_directory_filter: Some(format!("/{KEYSPACE}/{TABLE}-")),
    };
    let result = ingest(cfg).await.expect("ingestion of the BTI fixture");
    assert!(
        result.schema_load_result.schemas_loaded > 0,
        "the committed schema must load, else the raw view would refuse with Error::Schema"
    );
    (result.database, golden_partitions())
}

/// Every partition object of the committed sstabledump golden beside the
/// very `Data.db` the query reads.
fn golden_partitions() -> Vec<Json> {
    let dir = resolve_table_generation_dir(KEYSPACE, TABLE)
        .unwrap_or_else(|e| panic!("committed BTI fixture must resolve: {e}"));
    let path = dir.join(format!("{SSTABLE}.jsonl"));
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "the sstabledump golden {} must be readable — it is THE oracle for this lane: {e}",
            path.display()
        )
    });
    let parts: Vec<Json> = text
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("each golden line must be a JSON object"))
        .collect();
    assert_eq!(
        parts.len(),
        3,
        "golden precondition: test_da.wide_table has 3 partitions"
    );
    parts
}

fn golden_partition<'a>(parts: &'a [Json], pk: i32) -> &'a Json {
    let wanted = pk.to_string();
    parts
        .iter()
        .find(|p| p["partition"]["key"][0].as_str() == Some(wanted.as_str()))
        .unwrap_or_else(|| panic!("golden must carry partition pk={pk}"))
}

fn iso_to_micros(iso: &str) -> i64 {
    DateTime::parse_from_rfc3339(iso)
        .unwrap_or_else(|e| panic!("golden timestamp '{iso}' must parse as RFC3339: {e}"))
        .timestamp_micros()
}

fn get<'a>(row: &'a QueryRow, col: &str) -> Option<&'a Value> {
    row.values.get(col)
}

fn text_of(row: &QueryRow, col: &str) -> Option<String> {
    match get(row, col) {
        Some(Value::Text(b)) => Some(String::from_utf8_lossy(b).to_string()),
        _ => None,
    }
}

fn int_of(row: &QueryRow, col: &str) -> Option<i32> {
    match get(row, col) {
        Some(Value::Integer(i)) => Some(*i),
        _ => None,
    }
}

fn bigint_of(row: &QueryRow, col: &str) -> Option<i64> {
    match get(row, col) {
        Some(Value::BigInt(i)) => Some(*i),
        _ => None,
    }
}

/// Spec R6, scenario B: "a partition-key predicate over a BTI (`da`) table is
/// resolved by trie descent, never a full-table scan" — and #953's
/// within-partition traversal guarantee: EVERY clustering row of the
/// partition comes back, not just the first.
///
/// Run over all three partitions so a single lucky offset cannot carry the
/// case, and so `position` is compared against three DIFFERENT golden values.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bti_point_read_returns_every_clustering_row_with_the_trie_resolved_offset() {
    let (db, golden) = open_bti_fixture().await;

    for pk in [1, 2, 3] {
        let g = golden_partition(&golden, pk);
        let g_rows = g["rows"].as_array().expect("golden rows array");
        let g_position = g["partition"]["position"]
            .as_i64()
            .unwrap_or_else(|| panic!("golden partition pk={pk} must carry a byte position"));

        let result = db
            .execute(&format!(
                "SELECT * FROM {KEYSPACE}.{TABLE}_raw_sstable_data WHERE pk = {pk}"
            ))
            .await
            .unwrap_or_else(|e| {
                panic!("BTI raw-view point-key query for pk={pk} must succeed: {e}")
            });

        // #953: every clustering row, not just the partition's first.
        assert_eq!(
            result.rows.len(),
            g_rows.len(),
            "pk={pk} must yield one raw-view row per golden physical row (the #953 decoder \
             returned only the FIRST row of a BTI partition)"
        );
        let mut cks: Vec<i32> = result.rows.iter().filter_map(|r| int_of(r, "ck")).collect();
        cks.sort_unstable();
        let mut expected_cks: Vec<i32> = g_rows
            .iter()
            .map(|r| {
                r["clustering"][0]
                    .as_i64()
                    .expect("golden clustering component") as i32
            })
            .collect();
        expected_cks.sort_unstable();
        assert_eq!(
            cks, expected_cks,
            "pk={pk}'s clustering keys must match the golden's exactly — no gaps, no dupes"
        );

        for row in &result.rows {
            assert_eq!(
                int_of(row, "pk"),
                Some(pk),
                "every row must be tagged with the queried partition key"
            );
            assert_eq!(
                text_of(row, "format").as_deref(),
                Some("bti"),
                "the source-format column must report BTI for a `da` SSTable"
            );
            assert_eq!(text_of(row, "sstable").as_deref(), Some(SSTABLE));
            assert_eq!(
                bigint_of(row, "generation"),
                Some(2),
                "the fixture's only generation is `da-2`"
            );
            assert_eq!(text_of(row, "row_kind").as_deref(), Some("row"));
            // THE path evidence: a non-`None` `position` on a BTI reader can
            // only have come from `lookup_partition_via_bti_trie`, and it
            // matches the golden's partition offset exactly.
            assert_eq!(
                bigint_of(row, "position"),
                Some(g_position),
                "pk={pk}'s `position` must be the BTI trie descent's resolved partition \
                 offset, matching sstabledump's reported offset byte-exact — an \
                 `IndexUnavailable` scan-and-filter degrade would return the same rows with \
                 NO position at all"
            );
        }
    }
}

/// The per-row facts of the BTI partition, value-asserted against the golden:
/// `row_timestamp` (R3, never value-asserted anywhere before) and the
/// `payload` cell's own write time, on the FIRST and LAST clustering row of
/// the partition — the two positions a truncated traversal would drop.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bti_row_and_cell_write_times_match_the_golden() {
    let (db, golden) = open_bti_fixture().await;
    let g = golden_partition(&golden, 2);
    let g_rows = g["rows"].as_array().expect("golden rows array");

    let result = db
        .execute(&format!(
            "SELECT * FROM {KEYSPACE}.{TABLE}_raw_sstable_data WHERE pk = 2"
        ))
        .await
        .expect("BTI raw-view point-key query must succeed");

    for g_row in [
        g_rows.first().expect("golden first row"),
        g_rows.last().expect("golden last row"),
    ] {
        let ck = g_row["clustering"][0].as_i64().expect("golden clustering") as i32;
        let tstamp = iso_to_micros(
            g_row["liveness_info"]["tstamp"]
                .as_str()
                .unwrap_or_else(|| panic!("golden ck={ck} must carry liveness_info.tstamp")),
        );
        let row = result
            .rows
            .iter()
            .find(|r| int_of(r, "ck") == Some(ck))
            .unwrap_or_else(|| panic!("the raw view must return ck={ck}"));

        assert_eq!(
            bigint_of(row, "row_timestamp"),
            Some(tstamp),
            "row_timestamp must match the golden's liveness tstamp byte-exact for ck={ck}"
        );
        assert_eq!(
            bigint_of(row, "payload_timestamp"),
            Some(tstamp),
            "payload_timestamp must match the golden's write time for ck={ck}"
        );
        // Written with no TTL and never deleted — so every expiry/tombstone
        // column is ABSENT, not a fabricated zero.
        for absent in [
            "payload_ttl",
            "payload_local_deletion_time",
            "payload_tombstone",
            "row_ttl",
            "row_local_deletion_time",
            "row_tombstone",
        ] {
            assert_eq!(
                get(row, absent),
                None,
                "{absent} must be ABSENT for a live, non-expiring BTI row (ck={ck})"
            );
        }
    }
}
