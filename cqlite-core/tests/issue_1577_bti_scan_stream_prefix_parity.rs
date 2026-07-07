//! Issue #1577 (D1), owner-chosen fix (2026-07-06): `scan_stream` must be
//! PREFIX-AUTHORITATIVE with `scan` for ALL formats, including BTI (`da`).
//!
//! ## The bug this closes (BTI coverage hole)
//!
//! D1's LIMIT/OFFSET pushdown (`capped_fallback_scan`) returns the first `cap`
//! rows the LAZY `scan_stream` yields WITHOUT reconciling against the
//! materializing `scan` (the decode-stop perf win). That is only sound if
//! `scan_stream`'s first `cap` rows are byte-identical, in the same order, to
//! `scan`'s first `cap` rows.
//!
//! For BTI (`da` → `V5UncompressedOA`, `requires_chunk_stitching() == false`),
//! `SSTableReader::scan` routes through the trie-walk decoder
//! `bti_scan_with_metadata`, but `run_scan_stream` had NO BTI branch — it fell
//! into the block-by-block `read_next_block` + `parse_block_entries_with_schema`
//! decoder, a DIFFERENT per-reader decode path that can diverge in content /
//! count / order while still yielding >= `cap` rows, bypassing both the release
//! guard and the `< cap` reconciliation net.
//!
//! ## What this test proves
//!
//! Drives BOTH `SSTableReader::scan` and `SSTableReader::scan_stream` DIRECTLY
//! (bypassing the ingest/executor layer) against the REAL single-generation BTI
//! `test_da/simple_table` fixture and asserts, for several `cap` values (both
//! `cap < total` and `cap >= total`), that the first-`cap` `(RowKey, ScanRow)`
//! entries are IDENTICAL in CONTENT and ORDER. Before the BTI branch was added
//! to `run_scan_stream`, the streamed prefix came from the wrong decoder and
//! could diverge from `scan`'s trie-walk output.
//!
//! ## Non-vacuous / skip vs fail-closed (issue #1856 pattern)
//!
//! The fixture is asserted to actually hold BTI rows (a 0-row scan is a hard
//! failure, not a pass). Absent the `-Data.db` binary the test SKIPS with an
//! honest `eprintln` — EXCEPT under `CQLITE_PARITY_REQUIRE_DATASETS=1` (the
//! required parity gate), where absence is a fail-closed panic.

#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

use std::path::PathBuf;
use std::sync::Arc;

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::SSTableReader;
use cqlite_core::types::ScanRow;
use cqlite_core::{Config, Database, RowKey, TableId};

/// The real, committed single-generation BTI (`da`) fixture: 3 partitions, one
/// row each (`id` UUID PK, no clustering), LZ4-chunked Data.db + Partitions.db.
const BTI_DIR: &str = "sstables/test_da/simple_table-de1be8b064e711f19ad401a8c8227b11";
const DATA_DB: &str = "da-2-bti-Data.db";
const KS_TABLE: &str = "test_da.simple_table";

fn datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

fn bti_data_db() -> Option<PathBuf> {
    let data_db = datasets_root()?.join(BTI_DIR).join(DATA_DB);
    data_db.exists().then_some(data_db)
}

/// CI fail-closed switch (issue #1856): locally an absent fixture skips; the
/// required parity gate sets this so absence hard-fails instead of green-passing.
fn parity_datasets_required() -> bool {
    std::env::var("CQLITE_PARITY_REQUIRE_DATASETS")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

fn skip_or_fail_closed(reason: &str) {
    if parity_datasets_required() {
        panic!(
            "issue_1577_bti_scan_stream_prefix_parity: CQLITE_PARITY_REQUIRE_DATASETS=1 \
             but {reason} — required parity gate cannot green-pass without running \
             fail-closed (issue #1856)"
        );
    }
    eprintln!("Skipping (#1577 BTI scan_stream prefix parity): {reason}");
}

async fn open_reader(data_db: &std::path::Path) -> Arc<SSTableReader> {
    let cfg = Config::default();
    let platform = Arc::new(Platform::new(&cfg).await.expect("platform"));
    let reader = SSTableReader::open(data_db, &cfg, platform)
        .await
        .expect("open BTI reader");
    // PROOF this is genuinely the BTI (`da`) format — otherwise the test would
    // exercise the BIG path and be vacuous for the bug under repair.
    assert_eq!(
        reader.format_version().expect("format version"),
        "da",
        "fixture must be the BTI (`da`) format to exercise the trie-walk decoder"
    );
    Arc::new(reader)
}

/// Drain `scan_stream` fully into a `Vec`, preserving order.
async fn drain_stream(
    reader: Arc<SSTableReader>,
    table_id: &TableId,
    schema: Option<&cqlite_core::schema::TableSchema>,
) -> Vec<(RowKey, ScanRow)> {
    let mut rx = reader.scan_stream(table_id.clone(), None, None, schema.cloned(), 64);
    let mut out = Vec::new();
    while let Some(item) = rx.recv().await {
        out.push(item.expect("scan_stream item"));
    }
    out
}

/// THE parity assertion: for a real single-generation BTI fixture, the first
/// `cap` rows of `scan_stream` equal the first `cap` rows of `scan` in content
/// AND order, for `cap` both below and at/above the total row count.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bti_scan_stream_prefix_matches_scan() {
    let Some(data_db) = bti_data_db() else {
        skip_or_fail_closed("test_da/simple_table da-2-bti-Data.db not present");
        return;
    };

    let reader = open_reader(&data_db).await;
    let table_id = TableId::new(KS_TABLE);
    // Resolve the schema the SAME way both decode paths do: the reader's
    // header-embedded schema (BTI `da` fixtures carry it in Statistics.db), so
    // both `scan` and `scan_stream` decode schema-aware and identically. Passing
    // it explicitly (rather than `None`) makes the two paths use the byte-for-byte
    // same `TableSchema`, isolating any divergence to the decoder itself.
    let schema = reader.schema().cloned();

    // Authoritative full scan (the trie-walk `bti_scan_with_metadata` decoder).
    let full = reader
        .scan(&table_id, None, None, None, schema.as_ref())
        .await
        .expect("authoritative BTI scan");

    // Non-vacuous: a present fixture MUST return its rows (0 rows = read
    // regression, not a pass). This BTI fixture holds 3 partitions/rows.
    assert!(
        full.len() >= 3,
        "present BTI fixture must return its full row set (got {}) — 0/low rows means \
         the BTI decode path returned nothing (read regression), which would make this \
         parity test vacuous",
        full.len()
    );

    let streamed = drain_stream(reader, &table_id, schema.as_ref()).await;

    // The FULL streamed scan must equal the full materializing scan (content +
    // order) — the invariant `run_scan_stream`'s BTI branch now enforces by
    // construction (it drives the SAME trie-walk decoder as `scan`).
    assert_eq!(
        streamed, full,
        "#1577: streamed BTI scan diverged from authoritative BTI scan — the two \
         per-reader decode paths must be byte-identical in content and order"
    );

    // Prefix parity for several caps, both below and at/above the total.
    for cap in [1usize, 2, 3, full.len(), full.len() + 2] {
        let scan_prefix: Vec<_> = full.iter().take(cap).cloned().collect();
        let stream_prefix: Vec<_> = streamed.iter().take(cap).cloned().collect();
        assert_eq!(
            stream_prefix, scan_prefix,
            "#1577: first-{cap} scan_stream rows must equal first-{cap} scan rows \
             (content + order) for the BTI (`da`) fixture"
        );
    }

    eprintln!(
        "#1577 BTI scan_stream prefix parity: {} rows, stream==scan for caps \
         [1,2,3,{},{}]",
        full.len(),
        full.len(),
        full.len() + 2
    );
}

fn schemas_dir() -> Option<PathBuf> {
    // <workspace>/test-data/schemas — the datasets root is
    // <workspace>/test-data/datasets, so its parent holds `schemas/`.
    let root = datasets_root()?;
    let dir = root.parent()?.join("schemas");
    dir.exists().then_some(dir)
}

/// END-TO-END wiring evidence: the D1 LIMIT pushdown path (`capped_fallback_scan`
/// → `scan_stream_materializes` → the streamed BTI decode) must produce the
/// SAME first-`n` rows for a `LIMIT n` query as the unbounded full scan, for a
/// real single-generation BTI (`da`) table driven through the query executor.
/// Before the BTI branch, a `LIMIT` over this table decode-diverged (the stream
/// took the wrong per-reader decoder).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bti_executor_limit_matches_full_scan_prefix() {
    if bti_data_db().is_none() {
        skip_or_fail_closed("test_da/simple_table da-2-bti-Data.db not present");
        return;
    }
    let Some(schemas) = schemas_dir() else {
        skip_or_fail_closed("test-data/schemas dir not resolvable from CQLITE_DATASETS_ROOT");
        return;
    };
    let schema_path = schemas.join("da-test.cql");
    if !schema_path.exists() {
        skip_or_fail_closed("test-data/schemas/da-test.cql not present");
        return;
    }
    let root = datasets_root().expect("datasets root (checked above)");

    let result = ingest(IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir: root.join("sstables"),
        version_hint: Some("5.0".to_string()),
        core_config: Config::default(),
        table_directory_filter: Some("/test_da/".to_string()),
    })
    .await
    .expect("ingest test_da");
    assert!(
        result.schema_load_result.schemas_loaded > 0,
        "test_da schema must load"
    );
    let db: Database = result.database;

    // Oracle: the full unbounded scan (materializing `scan`, BTI trie-walk).
    let full = db
        .execute("SELECT id FROM test_da.simple_table")
        .await
        .expect("BTI full scan");
    assert!(
        full.rows.len() >= 3,
        "present BTI fixture must return its full row set (got {}) — 0/low rows = read \
         regression (would make this test vacuous)",
        full.rows.len()
    );
    let full_ids = row_ids(&full.rows);

    // For each cap (below and at/above total), the LIMIT result must equal the
    // full scan's first-`cap` rows — the D1 pushdown invariant, now sound for BTI.
    let total = full.rows.len();
    for cap in [1usize, 2, total, total + 2] {
        let limited = db
            .execute(&format!("SELECT id FROM test_da.simple_table LIMIT {cap}"))
            .await
            .expect("BTI LIMIT scan");
        let expected = &full_ids[..cap.min(total)];
        assert_eq!(
            row_ids(&limited.rows),
            expected,
            "#1577: LIMIT {cap} over the BTI table must equal the full scan's first-{cap} \
             rows (content + order)"
        );
    }
    eprintln!("#1577 BTI executor LIMIT parity: {total} rows, LIMIT==prefix for all caps");
}

/// Project each row's `id` UUID (the unique partition key of `simple_table`) in
/// result order — a stable, comparable identity for oracle-vs-bounded checks.
fn row_ids(rows: &[cqlite_core::QueryRow]) -> Vec<[u8; 16]> {
    rows.iter()
        .filter_map(|r| match r.values.get("id") {
            Some(cqlite_core::types::Value::Uuid(b)) => Some(*b),
            _ => None,
        })
        .collect()
}
