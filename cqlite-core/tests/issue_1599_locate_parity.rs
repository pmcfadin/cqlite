//! Issue #1599 (G3): one format-tagged `SSTableReader::locate` façade.
//!
//! `locate(key) -> Result<Option<(u64, u32)>>` resolves a partition's uncompressed
//! `Data.db` offset through ONE façade: it composes the C5 range short-circuit
//! (step 1), then dispatches to the BIG `Index.db` raw-key map
//! (`lookup_partition_with_index`) or the BTI `Partitions.db` trie
//! (`lookup_partition_via_bti_trie`). This test pins the façade to the LEGACY paths
//! as the oracle — byte-identical offsets, identical negatives — and asserts the
//! read-work counter deltas the two ordering carve-outs require.
//!
//! Carve-outs verified here (see the amended spec):
//! - C5 stays a pre-dispatch guard in `get_with_resolution` (bloom-first preserved
//!   for BIG); an out-of-range `locate` records exactly one `RANGE_SHORT_CIRCUITS`
//!   and performs zero downstream work.
//! - The B4 key→offset cache serves a repeated present-key `locate` with zero new
//!   `INDEX_PROBES` / `TRIE_WALKS`.
//!
//! Compiled only with `--features work-counters` (the counter getters/`reset` live
//! behind it). Requires `CQLITE_DATASETS_ROOT`; each test self-skips (never fails)
//! when its fixture is absent. The BTI (`da`) scenarios self-skip when the optional
//! `test_da` binaries (with `Partitions.db`) are absent.

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
use cqlite_core::util::cassandra_murmur3::cassandra_murmur3_token;
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

/// True iff the fixture dir also holds a BTI `Partitions.db` (required for the trie).
fn has_partitions_db(keyspace: &str, table: &str) -> bool {
    let Some(root) = datasets_root() else {
        return false;
    };
    let Ok(entries) = std::fs::read_dir(root.join("sstables").join(keyspace)) else {
        return false;
    };
    let prefix = format!("{table}-");
    for e in entries.flatten() {
        if !e.file_name().to_string_lossy().starts_with(&prefix) {
            continue;
        }
        if let Ok(files) = std::fs::read_dir(e.path()) {
            for f in files.flatten() {
                if f.file_name().to_string_lossy().ends_with("-Partitions.db") {
                    return true;
                }
            }
        }
    }
    false
}

/// Cassandra on-disk partition order key: ascending Murmur3 token, ties broken by
/// unsigned-lexicographic key bytes.
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

/// Deterministically find a 16-byte key whose token sorts strictly OUTSIDE
/// `[min_token, max_token]` (guaranteed absent + out of range).
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

async fn platform() -> Arc<cqlite_core::platform::Platform> {
    let config = cqlite_core::Config::default();
    Arc::new(
        cqlite_core::platform::Platform::new(&config)
            .await
            .expect("platform"),
    )
}

// ---------------------------------------------------------------------------
// BIG (`nb` + uncompressed) parity + counter-delta scenarios
// ---------------------------------------------------------------------------

/// Scenario: `locate` parity with the legacy BIG path for present / absent /
/// boundary keys, on a compressed (`nb`) and an uncompressed BIG fixture.
async fn big_locate_matches_legacy(keyspace: &str, table: &str) {
    let Some(data_db) = find_data_db(keyspace, table) else {
        eprintln!("Skipping (G3 BIG parity {keyspace}/{table}): Data.db not present");
        return;
    };
    let platform = platform().await;
    let Some(keys) = learn_all_raw_keys(&data_db, platform.clone()).await else {
        eprintln!("Skipping (G3 BIG parity {keyspace}/{table}): no Index.db raw keys");
        return;
    };
    let config = cqlite_core::Config::default();
    let reader = SSTableReader::open(&data_db, &config, platform)
        .await
        .expect("open BIG reader");

    // Present keys: locate() offset is byte-identical to the legacy Index.db resolve.
    for k in &keys {
        let legacy = reader
            .lookup_partition_with_index(k)
            .await
            .expect("legacy lookup_partition_with_index");
        let via_locate = reader.locate(k).await.expect("locate");
        assert_eq!(
            via_locate, legacy,
            "G3: locate() must byte-match the legacy BIG offset for a present key \
             ({keyspace}/{table})"
        );
        assert!(
            via_locate.is_some(),
            "G3: a present partition key must resolve to Some via locate()"
        );
    }

    // Boundary keys (physically-first / physically-last in token order) resolve to
    // the same offsets and are never short-circuited as out of range.
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
    for boundary in [&min_key, &max_key] {
        let legacy = reader
            .lookup_partition_with_index(boundary)
            .await
            .expect("legacy boundary lookup");
        let via_locate = reader.locate(boundary).await.expect("locate boundary");
        assert_eq!(
            via_locate, legacy,
            "G3: a boundary partition must resolve to the same offset via locate()"
        );
        assert!(
            via_locate.is_some(),
            "G3: a boundary partition (first/last key) is IN range (inclusive) and must resolve"
        );
    }

    // Absent (but in-range) key: locate() returns exactly what the legacy path did.
    let min_token = cassandra_murmur3_token(&min_key);
    let max_token = cassandra_murmur3_token(&max_key);
    for i in 0u64..100_000 {
        let mut k = [0u8; 16];
        k[..8].copy_from_slice(&i.wrapping_mul(0xD1B5_4A32_D192_ED03).to_le_bytes());
        k[8..].copy_from_slice(&i.to_le_bytes());
        let t = cassandra_murmur3_token(&k);
        if t > min_token && t < max_token && !keys.iter().any(|p| p.as_slice() == k) {
            let legacy = reader
                .lookup_partition_with_index(&k)
                .await
                .expect("legacy absent lookup");
            let via_locate = reader.locate(&k).await.expect("locate absent");
            assert_eq!(
                via_locate, legacy,
                "G3: an in-range absent key must resolve identically via locate() (legacy \
                 None-vs-fallthrough branch preserved)"
            );
            break;
        }
    }
}

#[tokio::test]
#[serial]
async fn big_nb_locate_parity() {
    big_locate_matches_legacy("test_basic", "simple_table").await;
}

#[tokio::test]
#[serial]
async fn big_uncompressed_locate_parity() {
    big_locate_matches_legacy("test_basic", "uncompressed_table").await;
}

/// Counter carve-out: an out-of-range `locate` records exactly one
/// `RANGE_SHORT_CIRCUITS` and performs ZERO `Index.db` probes / trie walks.
#[tokio::test]
#[serial]
async fn big_locate_out_of_range_short_circuits_zero_downstream() {
    let Some(data_db) = find_data_db("test_basic", "simple_table") else {
        eprintln!("Skipping (G3 C5 counter): test_basic/simple_table Data.db not present");
        return;
    };
    let platform = platform().await;
    let Some(keys) = learn_all_raw_keys(&data_db, platform.clone()).await else {
        eprintln!("Skipping (G3 C5 counter): no Index.db raw keys");
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
        eprintln!("Skipping (G3 C5 counter): fixture token range covers the ring");
        return;
    };
    let config = cqlite_core::Config::default();
    let reader = SSTableReader::open(&data_db, &config, platform)
        .await
        .expect("open BIG reader");

    rwc::reset();
    let got = reader.locate(&oor).await.expect("locate out-of-range");
    assert!(
        got.is_none(),
        "G3/C5: an out-of-range key must resolve to authoritative absence via locate()"
    );
    assert_eq!(
        rwc::range_short_circuits(),
        1,
        "G3/C5: an out-of-range locate() must record exactly one RANGE_SHORT_CIRCUITS; got {}",
        rwc::range_short_circuits()
    );
    assert_eq!(
        rwc::index_probes(),
        0,
        "G3/C5: an out-of-range locate() must perform ZERO Index.db probes; got {}",
        rwc::index_probes()
    );
    assert_eq!(
        rwc::trie_walks(),
        0,
        "G3/C5: an out-of-range locate() must perform ZERO trie walks; got {}",
        rwc::trie_walks()
    );
}

/// Counter carve-out: a repeated present-key `locate` is served by the B4 cache
/// with zero new `Index.db` probes.
#[tokio::test]
#[serial]
async fn big_locate_b4_repeat_zero_reprobe() {
    let Some(data_db) = find_data_db("test_basic", "simple_table") else {
        eprintln!("Skipping (G3 B4): test_basic/simple_table Data.db not present");
        return;
    };
    let platform = platform().await;
    let Some(keys) = learn_all_raw_keys(&data_db, platform.clone()).await else {
        eprintln!("Skipping (G3 B4): no Index.db raw keys");
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

    // First locate warms the B4 cache with a real probe.
    rwc::reset();
    let first = reader.locate(&present).await.expect("first locate");
    assert!(first.is_some(), "G3/B4: present key must resolve");
    assert!(
        rwc::index_probes() >= 1,
        "G3/B4: the first present-key locate must perform a real Index.db probe; got {}",
        rwc::index_probes()
    );

    // Second locate of the same present key is served from the cache: zero new probes.
    rwc::reset();
    let second = reader.locate(&present).await.expect("second locate");
    assert_eq!(
        second, first,
        "G3/B4: the cached locate must return the identical (offset, size)"
    );
    assert_eq!(
        rwc::index_probes(),
        0,
        "G3/B4: a repeated present-key locate must re-probe zero times; got {}",
        rwc::index_probes()
    );
}

// ---------------------------------------------------------------------------
// BTI (`da`) parity + counter-delta scenarios (self-skip when binaries absent)
//
// BTI (`da`) SSTables have NO `Index.db`, so `learn_all_raw_keys` cannot source
// keys for them. Instead we use the SAME schema-free VERIFIED GOLDEN oracle
// `issue_831_bti_reader_point_lookup.rs` pins for `test_da/simple_table`: three
// raw 16-byte partition keys `[uuid_byte; 16]` and their trie-resolved
// `Data.db` offsets (0 / 63 / 125), plus an absent key. When the fixture is
// present these MUST resolve (a `None` where `Some(offset)` is expected is a
// FAILURE, not a skip), so the BTI façade parity runs non-vacuously.
// ---------------------------------------------------------------------------

/// Verified golden `test_da/simple_table` partition keys: `(uuid_byte, expected
/// uncompressed Data.db offset)`. Identical to the issue_755 / issue_831 oracle.
const DA_SIMPLE_GOLDEN: &[(u8, u64)] = &[(0x22, 0), (0x11, 63), (0x33, 125)];

/// A raw 16-byte key guaranteed ABSENT from the golden set (used for the trie-miss
/// / authoritative-absence branch).
const DA_ABSENT_KEY: [u8; 16] = [0xEE; 16];

#[tokio::test]
#[serial]
async fn bti_narrow_locate_parity() {
    let keyspace = "test_da";
    let table = "simple_table";
    if !has_partitions_db(keyspace, table) {
        eprintln!("Skipping (G3 BTI parity {keyspace}/{table}): optional Partitions.db absent");
        return;
    }
    let Some(data_db) = find_data_db(keyspace, table) else {
        eprintln!("Skipping (G3 BTI parity {keyspace}/{table}): Data.db not present");
        return;
    };
    let platform = platform().await;
    let config = cqlite_core::Config::default();
    let reader = SSTableReader::open(&data_db, &config, platform)
        .await
        .expect("open BTI reader");

    // Counters FIRST, before the parity loop warms the B4 cache for these keys.
    // A single present-key locate() descends the trie exactly once...
    let present = [DA_SIMPLE_GOLDEN[0].0; 16];
    rwc::reset();
    let _ = reader.locate(&present).await.expect("locate present");
    assert_eq!(
        rwc::trie_walks(),
        1,
        "G3: a BTI locate() must descend the Partitions.db trie exactly once; got {}",
        rwc::trie_walks()
    );
    // ...and the B4 repeat is cache-served with zero new trie walks.
    rwc::reset();
    let _ = reader
        .locate(&present)
        .await
        .expect("locate present repeat");
    assert_eq!(
        rwc::trie_walks(),
        0,
        "G3/B4: a repeated present-key BTI locate must re-walk the trie zero times; got {}",
        rwc::trie_walks()
    );

    // Present golden keys: locate() byte-matches BOTH the golden offset AND the
    // legacy trie resolve, with size == 0 (the trie records none). A None here is
    // a FAILURE (the fixture is present), never a skip.
    for &(uuid_byte, expected_offset) in DA_SIMPLE_GOLDEN {
        let raw = [uuid_byte; 16];
        let legacy = reader
            .lookup_partition_via_bti_trie(&raw)
            .expect("legacy trie lookup");
        assert_eq!(
            legacy,
            Some(expected_offset),
            "oracle: legacy trie must resolve uuid 0x{uuid_byte:02x} to golden offset \
             {expected_offset}"
        );
        let via_locate = reader.locate(&raw).await.expect("locate");
        assert_eq!(
            via_locate,
            Some((expected_offset, 0u32)),
            "G3: locate() must resolve uuid 0x{uuid_byte:02x} to the golden BTI shape \
             (offset {expected_offset}, size 0)"
        );
        // Façade == legacy trie path, byte-identical offset.
        assert_eq!(
            via_locate.map(|(off, _)| off),
            legacy,
            "G3: locate() must byte-match the legacy BTI trie offset for a present key"
        );
    }

    // Absent key: a trie miss is authoritative absence — identical None both ways.
    let legacy_absent = reader
        .lookup_partition_via_bti_trie(&DA_ABSENT_KEY)
        .expect("legacy trie absent");
    assert_eq!(
        legacy_absent, None,
        "oracle: an absent key must miss the legacy trie"
    );
    let via_locate = reader.locate(&DA_ABSENT_KEY).await.expect("locate absent");
    assert_eq!(
        via_locate, None,
        "G3: an absent BTI key must resolve to the same None the legacy trie returned"
    );

    eprintln!(
        "bti_narrow_locate_parity PASSED: golden offsets 0/63/125 resolve via locate() == \
         legacy trie (size 0), absent key None, 1 trie walk then 0 on B4 repeat"
    );
}

// Note: `test_da/wide_table` has no published verified-golden offsets, so its BTI
// locate() parity is intentionally NOT asserted here (guessing offsets would
// fabricate the oracle). The `simple_table` scenario above is sufficient proof of
// the BTI façade == legacy-trie equivalence.
