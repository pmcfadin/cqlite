//! Issue #1570 (Epic B / B4): the key→partition-offset cache lets a repeated hot
//! point read skip the index/trie descent entirely (the Cassandra key-cache
//! analogue).
//!
//! Wiring evidence, both formats:
//!
//! - **BTI** (`test_da/simple_table`): read present key A, then present key B, then
//!   A again with `TRIE_WALKS` reset. A's second read hits the B4 LRU and descends
//!   the `Partitions.db` trie ZERO times. The interleave with B is deliberate — it
//!   proves the *multi-key* B4 cache served the hit, not the single-entry C3 memo
//!   (which after reading B holds B, so it could not serve A).
//! - **BIG** (`test_basic/simple_table`): call `lookup_partition_with_index` twice
//!   for the same present key at the reader level. The second call hits the B4 cache
//!   and probes `Index.db` ZERO times (`INDEX_PROBES == 0`), returning the SAME
//!   `(offset,size)` the first probe resolved (parity on a hit).
//!
//! Compiled only with `--features work-counters` (the counter getters/`reset` live
//! behind it). Requires `CQLITE_DATASETS_ROOT`; each test self-skips (never fails)
//! when its optional fixture is absent. The BTI test is excluded under `tombstones`
//! (that build serves point reads by a full-scan filter, not the prune+seek path).

#![cfg(all(
    feature = "state_machine",
    feature = "cli-helpers",
    feature = "work-counters"
))]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use cqlite_core::storage::sstable::read_work_counters as rwc;

fn datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

/// Locate the first `*-Data.db` under `<datasets>/sstables/<keyspace>/<table>-*/`.
fn find_data_db(keyspace: &str, table: &str) -> Option<PathBuf> {
    let root = datasets_root()?;
    let entries = std::fs::read_dir(root.join("sstables").join(keyspace)).ok()?;
    let prefix = format!("{table}-");
    for e in entries.flatten() {
        if !e.file_name().to_string_lossy().starts_with(&prefix) {
            continue;
        }
        if let Ok(files) = std::fs::read_dir(e.path()) {
            for f in files.flatten() {
                if f.file_name().to_string_lossy().ends_with("-Data.db") {
                    return Some(f.path());
                }
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// BIG wiring: INDEX_PROBES == 0 on a repeated Index.db resolution.
// ---------------------------------------------------------------------------

mod big {
    use super::*;
    use cqlite_core::storage::sstable::index_reader::IndexReader;
    use cqlite_core::storage::sstable::reader::SSTableReader;
    use serial_test::serial;

    /// Learn a known-present raw partition key from the Index.db (its entries are
    /// keyed on the raw partition-key bytes since #552).
    async fn learn_present_raw_key(
        data_db: &Path,
        platform: Arc<cqlite_core::platform::Platform>,
    ) -> Option<Vec<u8>> {
        let index_path = data_db.with_file_name("nb-1-big-Index.db");
        if !index_path.exists() {
            return None;
        }
        let index_reader = IndexReader::open(&index_path, platform).await.ok()?;
        let entry = index_reader.get_partition_entries().first()?;
        Some(entry.key_digest.to_vec())
    }

    #[tokio::test]
    #[serial]
    async fn big_repeated_index_lookup_skips_probe_and_matches() {
        let Some(data_db) = find_data_db("test_basic", "simple_table") else {
            eprintln!("Skipping (B4/BIG): test_basic/simple_table Data.db not present");
            return;
        };
        let config = cqlite_core::Config::default();
        let platform = Arc::new(
            cqlite_core::platform::Platform::new(&config)
                .await
                .expect("platform"),
        );
        let Some(raw_key) = learn_present_raw_key(&data_db, platform.clone()).await else {
            eprintln!("Skipping (B4/BIG): could not learn a present raw key (no Index.db)");
            return;
        };

        let reader = SSTableReader::open(&data_db, &config, platform)
            .await
            .expect("open BIG reader");

        // First resolution: a real Index.db probe (cache miss).
        rwc::reset();
        let first = reader
            .lookup_partition_with_index(&raw_key)
            .await
            .expect("first index lookup");
        let Some(first) = first else {
            eprintln!("Skipping (B4/BIG): raw-key Index.db lookup did not resolve on this fixture");
            return;
        };
        assert_eq!(
            rwc::index_probes(),
            1,
            "B4/BIG: the first (cold) resolution must perform exactly one Index.db probe"
        );

        // Second resolution: served from the B4 cache — zero Index.db probes.
        rwc::reset();
        let second = reader
            .lookup_partition_with_index(&raw_key)
            .await
            .expect("second index lookup")
            .expect("cached hit must resolve");
        assert_eq!(
            rwc::index_probes(),
            0,
            "B4/BIG: a repeated point read must skip the Index.db probe (key-cache hit); got {}",
            rwc::index_probes()
        );
        assert_eq!(
            first, second,
            "B4/BIG correctness: a cache hit must return the SAME (offset,size) a fresh probe does"
        );
    }
}

// ---------------------------------------------------------------------------
// BTI wiring: TRIE_WALKS == 0 on a repeated (interleaved) point read.
// ---------------------------------------------------------------------------

#[cfg(not(feature = "tombstones"))]
mod bti {
    use super::*;
    use cqlite_core::ingestion::{ingest, IngestionConfig};
    use cqlite_core::{Database, Value};
    use serial_test::serial;

    fn schemas_dir() -> Option<PathBuf> {
        if let Some(root) = datasets_root() {
            if let Some(dir) = root.parent().and_then(|p| {
                let d = p.join("schemas");
                d.exists().then_some(d)
            }) {
                return Some(dir);
            }
        }
        let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        let dir = manifest_dir.parent()?.join("test-data").join("schemas");
        dir.exists().then_some(dir)
    }

    async fn setup(keyspace: &str, schema_file: &str) -> Option<Database> {
        let root = datasets_root()?;
        let schema_path = schemas_dir()?.join(schema_file);
        if !schema_path.exists() {
            return None;
        }
        let data_dir = root.join("sstables");
        if !data_dir.exists() {
            return None;
        }
        let config = IngestionConfig {
            schema_paths: vec![schema_path],
            data_dir,
            version_hint: Some("5.0".to_string()),
            core_config: cqlite_core::Config::default(),
            table_directory_filter: Some(format!("/{keyspace}/")),
        };
        let result = ingest(config).await.ok()?;
        if result.schema_load_result.schemas_loaded == 0 {
            return None;
        }
        Some(result.database)
    }

    fn uuid_to_literal(bytes: &[u8; 16]) -> String {
        let h = |range: std::ops::Range<usize>| -> String {
            bytes[range].iter().map(|b| format!("{b:02x}")).collect()
        };
        format!(
            "{}-{}-{}-{}-{}",
            h(0..4),
            h(4..6),
            h(6..8),
            h(8..10),
            h(10..16)
        )
    }

    /// Learn two distinct present `id` UUIDs and build projected point-read SQL for
    /// each (>8 tokens → routes through the modern partition-targeted path).
    async fn learn_two_point_sqls(db: &Database, table: &str) -> Option<(String, String)> {
        let scan = db.execute(&format!("SELECT id FROM {table}")).await.ok()?;
        let mut ids: Vec<[u8; 16]> = Vec::new();
        for row in &scan.rows {
            if let Some(Value::Uuid(b)) = row.values.get("id") {
                if !ids.contains(b) {
                    ids.push(*b);
                }
            }
            if ids.len() == 2 {
                break;
            }
        }
        if ids.len() < 2 {
            return None;
        }
        let sql = |id: &[u8; 16]| {
            format!(
                "SELECT id, name FROM {table} WHERE id = {}",
                uuid_to_literal(id)
            )
        };
        Some((sql(&ids[0]), sql(&ids[1])))
    }

    /// Scenario: a repeated (interleaved) BTI point read performs ZERO trie walks on
    /// the hit — proving the multi-key B4 cache, not the single-entry C3 memo.
    #[tokio::test]
    #[serial]
    async fn bti_repeated_interleaved_point_read_skips_trie_walk() {
        if find_data_db("test_da", "simple_table").is_none() {
            eprintln!("Skipping (B4/BTI): optional test_da/simple_table not present");
            return;
        }
        let Some(db) = setup("test_da", "da-test.cql").await else {
            eprintln!("Skipping (B4/BTI): could not ingest test_da");
            return;
        };
        let Some((sql_a, sql_b)) = learn_two_point_sqls(&db, "test_da.simple_table").await else {
            eprintln!("Skipping (B4/BTI): fixture has fewer than two distinct keys");
            return;
        };

        // Read A (populates the B4 cache for A), then B (evicts A from the SINGLE
        // C3 memo, but not from the multi-key B4 LRU).
        let ra = db.execute(&sql_a).await.expect("BTI point read A");
        assert!(!ra.rows.is_empty(), "B4/BTI: key A must be present");
        let _ = db.execute(&sql_b).await.expect("BTI point read B");

        // Now re-read A. The single-entry memo holds B, so only the B4 cache can
        // serve A without a trie descent.
        rwc::reset();
        assert_eq!(rwc::trie_walks(), 0, "reset must zero TRIE_WALKS");
        let ra2 = db.execute(&sql_a).await.expect("BTI repeated point read A");
        assert!(
            !ra2.rows.is_empty(),
            "B4/BTI: repeated read of present key A returned zero rows"
        );
        assert_eq!(
            rwc::trie_walks(),
            0,
            "B4/BTI: a repeated point read of a cached key must descend the trie ZERO times \
             (key-cache hit, beyond the single-entry memo); got {}",
            rwc::trie_walks()
        );
    }
}
