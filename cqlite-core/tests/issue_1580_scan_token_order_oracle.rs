//! Issue #1580 (Epic D4): `SSTableManager` cross-SSTable scan ordering must match
//! Cassandra's authoritative order — ascending Murmur3 **token**, NOT raw
//! partition-key byte order.
//!
//! ## Oracle (determined FIRST, before the fix — decision #10)
//!
//! Cassandra's default `Murmur3Partitioner` orders partitions by their token (the
//! Murmur3 hash of the partition-key bytes). A full scan / `sstabledump` therefore
//! emits partitions in **token order**, which in general is NOT the lexicographic
//! order of the raw key bytes.
//!
//! The `test_da/simple_table` BTI fixture (authored by Cassandra 5.0.2) pins this
//! divergence exactly. Its `da-2-bti-Data.db.jsonl` golden — the sstabledump
//! reference, i.e. the on-disk physical order — lists the three UUID partitions in
//! this order:
//!
//! ```text
//!   22222222-…   (token 1213057064512856170)
//!   11111111-…   (token 4360155383588533346)
//!   33333333-…   (token 8780122315263850168)
//! ```
//!
//! Ascending token → `[0x22, 0x11, 0x33]`. Ascending raw bytes → `[0x11, 0x22,
//! 0x33]`. They disagree, so this fixture is a clean oracle. (The token values are
//! independently pinned in `util::cassandra_murmur3` unit tests.)
//!
//! ## What this asserts
//!
//! `SSTableManager::scan` over the fixture returns the partitions in ascending
//! Murmur3-token order — equivalently, exactly the physical/`sstabledump` order
//! `[0x22, 0x11, 0x33]`. A guard confirms the fixture actually exercises the
//! divergence (token order ≠ raw-byte order), so the test can never silently
//! degenerate into asserting the wrong (raw-byte) order.
//!
//! ## Regression it guards
//!
//! Pre-fix, the manager concatenated each reader's (already token-ordered) rows
//! and RE-SORTED the whole concatenation by raw `RowKey` bytes
//! (`all_results.sort_by(|a, b| a.0.cmp(&b.0))`). That both (1) produced the wrong
//! order `[0x11, 0x22, 0x33]` and (2) cost a full O(n log n) sort on the async
//! worker. This test FAILS on that code and PASSES once the manager preserves
//! token order via a k-way merge over the per-reader token-ordered streams.

#![cfg(feature = "state_machine")]

use std::path::PathBuf;
use std::sync::Arc;

use cqlite_core::platform::Platform;
use cqlite_core::schema::{parse_cql_schema, TableSchema};
use cqlite_core::storage::sstable::SSTableManager;
use cqlite_core::types::TableId as CqlTableId;
use cqlite_core::util::cassandra_murmur3::cassandra_murmur3_token;
use cqlite_core::{Config, RowKey};

const KEYSPACE: &str = "test_da";
const TABLE: &str = "simple_table";

/// Locate the `test_da` keyspace directory under `CQLITE_DATASETS_ROOT`, if the
/// binary fixtures are present. Returns `None` (skip) when they are not — the
/// gitignored `.db` binaries are absent in a clean checkout.
fn test_da_dir() -> Option<PathBuf> {
    let root = std::env::var("CQLITE_DATASETS_ROOT").ok().map(PathBuf::from)?;
    let dir = root.join("sstables").join(KEYSPACE);
    // Require an actual Data.db under the simple_table generation; a bare dir
    // (JSONL-only) must skip, not spuriously pass with zero rows.
    let has_data = std::fs::read_dir(&dir)
        .ok()?
        .filter_map(|e| e.ok())
        .any(|e| {
            let name = e.file_name();
            let name = name.to_string_lossy();
            name.starts_with(&format!("{TABLE}-"))
                && std::fs::read_dir(e.path())
                    .map(|inner| {
                        inner.filter_map(|x| x.ok()).any(|x| {
                            x.file_name().to_string_lossy().ends_with("-Data.db")
                        })
                    })
                    .unwrap_or(false)
        });
    has_data.then_some(dir)
}

/// Schema for `test_da.simple_table` (matches `test-data/schemas/da-test.cql`).
fn simple_table_schema() -> TableSchema {
    let cql = format!(
        "CREATE TABLE {KEYSPACE}.{TABLE} (\
             id UUID PRIMARY KEY, \
             name TEXT, \
             age INT, \
             salary BIGINT, \
             active BOOLEAN, \
             created TIMESTAMP\
         );"
    );
    parse_cql_schema(&cql).expect("parse simple_table schema")
}

/// Distinct partition keys in first-seen order (rows for one partition are
/// contiguous; this collapses clustering duplicates while preserving order).
fn distinct_keys_in_order(rows: &[(RowKey, impl Sized)]) -> Vec<Vec<u8>> {
    let mut out: Vec<Vec<u8>> = Vec::new();
    for (k, _) in rows {
        let bytes = k.as_bytes().to_vec();
        if out.last().map(|last| last != &bytes).unwrap_or(true) {
            out.push(bytes);
        }
    }
    out
}

#[tokio::test]
async fn manager_scan_returns_cassandra_token_order_not_rawbyte_order() {
    let Some(data_dir) = test_da_dir() else {
        eprintln!(
            "Skipping issue #1580 oracle test: test_da/simple_table Data.db not present \
             (set CQLITE_DATASETS_ROOT and fetch datasets)"
        );
        return;
    };

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    let manager = SSTableManager::new(
        &data_dir,
        &config,
        platform,
        #[cfg(feature = "state_machine")]
        None,
    )
    .await
    .expect("open SSTableManager over test_da");

    let schema = simple_table_schema();
    let table_id = CqlTableId::from(format!("{KEYSPACE}.{TABLE}").as_str());
    let rows = manager
        .scan(&table_id, None, None, None, Some(&schema))
        .await
        .expect("scan must not error");

    assert!(
        !rows.is_empty(),
        "fixture present but scan returned 0 rows — fixture/registration problem"
    );

    let got = distinct_keys_in_order(&rows);
    assert_eq!(
        got.len(),
        3,
        "test_da/simple_table has exactly 3 partitions; got {} distinct keys",
        got.len()
    );

    // The oracle order: sort the observed keys by ascending Murmur3 token.
    let mut token_sorted = got.clone();
    token_sorted.sort_by_key(|k| cassandra_murmur3_token(k));

    // Guard: this fixture must actually exercise the token≠rawbyte divergence,
    // otherwise the assertion below would be vacuous.
    let mut rawbyte_sorted = got.clone();
    rawbyte_sorted.sort();
    assert_ne!(
        token_sorted, rawbyte_sorted,
        "fixture no longer exercises token≠raw-byte divergence — oracle is vacuous"
    );

    // THE oracle assertion: the manager's scan output is in ascending token order.
    assert_eq!(
        got, token_sorted,
        "Issue #1580: SSTableManager::scan must return partitions in ascending \
         Murmur3-token order (Cassandra's cross-SSTable scan order), not raw \
         partition-key byte order.\n  got (first byte of each key): {:?}\n  token order:  {:?}\n  raw-byte order: {:?}",
        got.iter().map(|k| k.first().copied()).collect::<Vec<_>>(),
        token_sorted.iter().map(|k| k.first().copied()).collect::<Vec<_>>(),
        rawbyte_sorted.iter().map(|k| k.first().copied()).collect::<Vec<_>>(),
    );

    // Pin the exact physical/sstabledump order for this fixture: 0x22, 0x11, 0x33.
    let first_bytes: Vec<u8> = got.iter().filter_map(|k| k.first().copied()).collect();
    assert_eq!(
        first_bytes,
        vec![0x22, 0x11, 0x33],
        "Issue #1580: expected the Cassandra physical order [0x22, 0x11, 0x33]"
    );
}
