//! Issue #1576 (Epic C / C5): first/last-key range short-circuit.
//!
//! An SSTable's partitions occupy a contiguous token-ring slice bounded by
//! `first_key`/`last_key` (parsed from `Summary.db`). When a point read's key sorts
//! outside that bound the partition is definitely absent, so the reader can answer
//! `Ok(None)` BEFORE any bloom check, `Index.db` probe, or BTI trie descent.
//!
//! Wiring evidence on the BIG (`test_basic/simple_table`, single-UUID partition key)
//! format, whose `Summary.db` carries the authoritative bound:
//!
//! - **Bound correctness (no false miss — the load-bearing property):** the
//!   `Summary.db` `first_key`/`last_key` equal the min-token / max-token present
//!   partition keys (validated against the raw keys in `Index.db`), so the bound is
//!   the true token-order extent. Then [`SSTableReader::partition_key_out_of_range`]
//!   returns `false` for EVERY present key — including the two boundary keys equal to
//!   `first_key`/`last_key` (inclusive bound) — and for an in-range-but-absent key
//!   (the short-circuit must never over-fire), and `true` only for a key whose token
//!   sorts strictly outside the bound.
//! - **Counter wiring:** an out-of-range point read via
//!   [`SSTableReader::get_with_resolution`] returns `Ok(None)`, records exactly one
//!   `RANGE_SHORT_CIRCUITS`, and performs ZERO `Index.db` probes. An in-range present
//!   key records ZERO short-circuits and reaches the real presence path
//!   (`INDEX_PROBES >= 1`) — proving in-range reads are unchanged.
//!
//! Compiled only with `--features work-counters` (the counter getters/`reset` live
//! behind it). Requires `CQLITE_DATASETS_ROOT`; each test self-skips (never fails)
//! when its fixture is absent.

#![cfg(all(
    feature = "state_machine",
    feature = "cli-helpers",
    feature = "work-counters"
))]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use cqlite_core::storage::sstable::index_reader::IndexReader;
use cqlite_core::storage::sstable::read_work_counters as rwc;
use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::storage::sstable::summary_reader::SummaryReader;
use cqlite_core::types::TableId;
use cqlite_core::util::cassandra_murmur3::cassandra_murmur3_token;
use cqlite_core::RowKey;
use serial_test::serial;

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

/// Cassandra on-disk partition order key: ascending Murmur3 token, ties broken by
/// unsigned-lexicographic key bytes (mirrors `sort_by_token_order`).
fn order_key(k: &[u8]) -> (i64, Vec<u8>) {
    (cassandra_murmur3_token(k), k.to_vec())
}

/// Learn every raw partition key from `Index.db` (raw bytes since #552).
async fn learn_all_raw_keys(
    data_db: &Path,
    platform: Arc<cqlite_core::platform::Platform>,
) -> Option<Vec<Vec<u8>>> {
    let data_name = data_db.file_name()?.to_string_lossy();
    let index_name = format!("{}-Index.db", data_name.strip_suffix("-Data.db")?);
    let index_path = data_db.with_file_name(index_name);
    if !index_path.exists() {
        return None;
    }
    let index_reader = IndexReader::open(&index_path, platform).await.ok()?;
    let keys: Vec<Vec<u8>> = index_reader
        .get_partition_entries()
        .iter()
        .map(|e| e.key_digest.to_vec())
        .collect();
    if keys.is_empty() {
        None
    } else {
        Some(keys)
    }
}

/// Open the reader's `Summary.db` directly to read the authoritative bound.
async fn open_summary(
    data_db: &Path,
    platform: Arc<cqlite_core::platform::Platform>,
) -> Option<SummaryReader> {
    let data_name = data_db.file_name()?.to_string_lossy();
    let summary_name = format!("{}-Summary.db", data_name.strip_suffix("-Data.db")?);
    let summary_path = data_db.with_file_name(summary_name);
    if !summary_path.exists() {
        return None;
    }
    SummaryReader::open(&summary_path, platform).await.ok()
}

/// Deterministically find a 16-byte key whose token sorts strictly OUTSIDE
/// `[min_token, max_token]` (guaranteed absent + out of range). Returns `None` only
/// if the fixture's token range covers essentially the whole ring (not the case for
/// the small test fixtures). Spread across the ring via a golden-ratio multiplier so
/// a match is found in few iterations.
fn find_out_of_range_key(min_token: i64, max_token: i64) -> Option<Vec<u8>> {
    for i in 0u64..1_000_000 {
        let mut k = [0u8; 16];
        k[..8].copy_from_slice(&i.to_le_bytes());
        k[8..].copy_from_slice(&i.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_le_bytes());
        let t = cassandra_murmur3_token(&k);
        if t < min_token || t > max_token {
            return Some(k.to_vec());
        }
    }
    None
}

/// Find a 16-byte key whose token sorts strictly INSIDE `(min_token, max_token)` —
/// an in-range key the SSTable does not contain (used to prove the short-circuit
/// does not over-fire on in-range absent keys).
fn find_in_range_absent_key(
    min_token: i64,
    max_token: i64,
    present: &[Vec<u8>],
) -> Option<Vec<u8>> {
    for i in 0u64..1_000_000 {
        let mut k = [0u8; 16];
        k[..8].copy_from_slice(&i.wrapping_mul(0xD1B5_4A32_D192_ED03).to_le_bytes());
        k[8..].copy_from_slice(&i.to_le_bytes());
        let t = cassandra_murmur3_token(&k);
        if t > min_token && t < max_token && !present.iter().any(|p| p.as_slice() == k) {
            return Some(k.to_vec());
        }
    }
    None
}

async fn platform() -> Arc<cqlite_core::platform::Platform> {
    let config = cqlite_core::Config::default();
    Arc::new(
        cqlite_core::platform::Platform::new(&config)
            .await
            .expect("platform"),
    )
}

/// Bound correctness: the Summary bound is the true token extent, and the predicate
/// never rules out a present or boundary or in-range key, and rules out only a
/// genuinely out-of-range key.
#[tokio::test]
#[serial]
async fn big_range_predicate_no_false_miss() {
    let Some(data_db) = find_data_db("test_basic", "simple_table") else {
        eprintln!("Skipping (C5/BIG predicate): test_basic/simple_table Data.db not present");
        return;
    };
    let platform = platform().await;
    let Some(keys) = learn_all_raw_keys(&data_db, platform.clone()).await else {
        eprintln!("Skipping (C5/BIG predicate): could not learn raw keys (no Index.db)");
        return;
    };
    let Some(summary) = open_summary(&data_db, platform.clone()).await else {
        eprintln!("Skipping (C5/BIG predicate): no Summary.db");
        return;
    };

    // Min/max present partition keys in token order.
    let min_key = keys
        .iter()
        .min_by(|a, b| order_key(a).cmp(&order_key(b)))
        .expect("min key")
        .clone();
    let max_key = keys
        .iter()
        .max_by(|a, b| order_key(a).cmp(&order_key(b)))
        .expect("max key")
        .clone();

    // The Summary bound MUST equal the true token extent (validates the bound source
    // is authoritative, token-ordered, and byte-exact — a wrong bound = false miss).
    assert_eq!(
        summary.get_first_key(),
        min_key.as_slice(),
        "C5: Summary first_key must equal the min-token present partition key"
    );
    assert_eq!(
        summary.get_last_key(),
        max_key.as_slice(),
        "C5: Summary last_key must equal the max-token present partition key"
    );

    let config = cqlite_core::Config::default();
    let reader = SSTableReader::open(&data_db, &config, platform)
        .await
        .expect("open BIG reader");

    // No present key is ever ruled out — including the two boundary keys (inclusive).
    for k in &keys {
        assert!(
            !reader.partition_key_out_of_range(k),
            "C5: a present partition key must NEVER be reported out of range (false miss)"
        );
    }
    assert!(
        !reader.partition_key_out_of_range(&min_key),
        "C5: the first_key boundary is IN range (inclusive)"
    );
    assert!(
        !reader.partition_key_out_of_range(&max_key),
        "C5: the last_key boundary is IN range (inclusive)"
    );

    let min_token = cassandra_murmur3_token(&min_key);
    let max_token = cassandra_murmur3_token(&max_key);

    // An in-range-but-absent key must NOT be short-circuited (no over-fire).
    if let Some(in_range_absent) = find_in_range_absent_key(min_token, max_token, &keys) {
        assert!(
            !reader.partition_key_out_of_range(&in_range_absent),
            "C5: an in-range (absent) key must NOT be ruled out by the range check"
        );
    }

    // A genuinely out-of-range key IS ruled out.
    let Some(oor) = find_out_of_range_key(min_token, max_token) else {
        eprintln!("Skipping out-of-range assertion: fixture token range covers the ring");
        return;
    };
    assert!(
        reader.partition_key_out_of_range(&oor),
        "C5: a key whose token sorts outside [first,last] must be ruled out of range"
    );
}

/// Counter wiring: out-of-range short-circuits with zero presence work; in-range
/// reads run the normal path unchanged.
#[tokio::test]
#[serial]
async fn big_out_of_range_short_circuits_zero_presence_work() {
    let Some(data_db) = find_data_db("test_basic", "simple_table") else {
        eprintln!("Skipping (C5/BIG counter): test_basic/simple_table Data.db not present");
        return;
    };
    let platform = platform().await;
    let Some(keys) = learn_all_raw_keys(&data_db, platform.clone()).await else {
        eprintln!("Skipping (C5/BIG counter): could not learn raw keys (no Index.db)");
        return;
    };
    let min_token = keys
        .iter()
        .map(|k| cassandra_murmur3_token(k))
        .min()
        .unwrap();
    let max_token = keys
        .iter()
        .map(|k| cassandra_murmur3_token(k))
        .max()
        .unwrap();
    let Some(oor) = find_out_of_range_key(min_token, max_token) else {
        eprintln!("Skipping (C5/BIG counter): fixture token range covers the ring");
        return;
    };
    let present = keys
        .iter()
        .min_by(|a, b| order_key(a).cmp(&order_key(b)))
        .unwrap()
        .clone();

    let config = cqlite_core::Config::default();
    let reader = SSTableReader::open(&data_db, &config, platform)
        .await
        .expect("open BIG reader");
    let table_id = TableId::new("test_basic.simple_table".to_string());

    // Out-of-range: exactly one short-circuit, ZERO Index.db probes, Ok(None).
    rwc::reset();
    let got = reader
        .get_with_resolution(&table_id, &RowKey::new(oor.clone()), false)
        .await
        .expect("out-of-range get");
    assert!(
        got.is_none(),
        "C5: an out-of-range key must resolve to authoritative absence"
    );
    assert_eq!(
        rwc::range_short_circuits(),
        1,
        "C5: an out-of-range point read must record exactly one RANGE_SHORT_CIRCUITS; got {}",
        rwc::range_short_circuits()
    );
    assert_eq!(
        rwc::index_probes(),
        0,
        "C5: an out-of-range point read must perform ZERO Index.db probes (short-circuited before \
         the presence path); got {}",
        rwc::index_probes()
    );

    // In-range present key: NO short-circuit, and the normal presence path runs
    // (Index.db probe), proving in-range reads are unchanged.
    rwc::reset();
    let _ = reader
        .get_with_resolution(&table_id, &RowKey::new(present.clone()), false)
        .await
        .expect("in-range get");
    assert_eq!(
        rwc::range_short_circuits(),
        0,
        "C5: an in-range key must NOT short-circuit; got {}",
        rwc::range_short_circuits()
    );
    assert!(
        rwc::index_probes() >= 1,
        "C5: an in-range present key must reach the real Index.db probe (normal path unchanged); \
         got {}",
        rwc::index_probes()
    );
}
