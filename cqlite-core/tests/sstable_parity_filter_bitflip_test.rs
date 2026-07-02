//! Filter.db bit-flip corruption exposure pin (issue #1398, epic #1380).
//!
//! Cassandra's `Filter.db` carries NO checksum, and CQLite's read path is
//! fail-open only for an UNPARSEABLE filter (`component_loading.rs`). A PARSEABLE
//! filter with a bit flipped 1→0 inside the bit array is therefore NOT caught on
//! load, yet it produces Bloom-filter FALSE NEGATIVES: `might_contain == false`
//! for a partition key Cassandra actually wrote. On the BIG point-lookup path
//! that makes `partition_lookup.rs` return `Ok(None)` (bloom "miss" ⇒ skip) — a
//! live partition is silently invisible.
//!
//! POSTURE (a) — accept Cassandra-equivalent exposure (issue #1398):
//!   * Cassandra's `sstableverify` does NOT read/validate Filter.db bit-array
//!     contents, so it reports this fixture CLEAN and its READ path has the SAME
//!     false-negative exposure. This is recorded as `verdict_parity: divergent`
//!     in `corruption-manifest.yml` (`filter_db_bit_flip`).
//!   * CQLite adds a detection tool Cassandra LACKS: `cqlite verify --mode full`
//!     re-probes every present `Index.db` key against the decoded filter and
//!     reports `FilterFalseNegative`. This test pins that detection.
//!   * FULL SCANS and BTI (`da`) point lookups are UNAFFECTED (they never gate on
//!     this bloom). This test asserts both.
//!
//! The fixture is materialized into a tempdir from the COMMITTED clean
//! `test_comp/lz4_table` (BIG) source — the SAME clean source and mutation the
//! `filter_db_bit_flip` corpus fixture records (byte 8, the first bitset byte past
//! the valid 8-byte header, XOR 0x10). Being self-contained, this lane runs
//! whenever the clean dataset is present (fail-closed under
//! `CQLITE_REQUIRE_FIXTURES=1`) and never depends on the gitignored corrupt corpus
//! binaries being regenerated.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::bloom::BloomFilter;
use cqlite_core::storage::sstable::index_reader::IndexReader;
use cqlite_core::storage::sstable::verify::{verify_sstable, VerifyErrorClass, VerifyMode};
use cqlite_core::Config;

/// The single mutation the `filter_db_bit_flip` corpus fixture records: clear the
/// first bit-array byte's low nibble bit (byte 8, XOR 0x10) — a SET bit in the
/// lz4_table filter — leaving the 8-byte header valid.
const BITSET_FLIP_OFFSET: usize = 8;
const BITSET_FLIP_MASK: u8 = 0x10;

fn datasets_sstables_root() -> PathBuf {
    let root = if let Ok(root) = std::env::var("CQLITE_DATASETS_ROOT") {
        PathBuf::from(root)
    } else {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .map(|w| w.join("test-data/datasets"))
            .unwrap_or_else(|| PathBuf::from("test-data/datasets"))
    };
    root.join("sstables")
}

fn require_fixtures_strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

fn skip_or_require(what: &str, reason: &str) -> bool {
    if require_fixtures_strict() {
        panic!("CQLITE_REQUIRE_FIXTURES=1 but {what} unavailable: {reason}");
    }
    eprintln!("[SKIP] {what}: {reason}");
    false
}

/// Resolve a clean, materialized SSTable generation directory (has a `*-Data.db`)
/// whose table dir starts with `prefix` under `<sstables>/<keyspace>`.
fn resolve_clean_generation(keyspace: &str, prefix: &str) -> Option<PathBuf> {
    let ks_dir = datasets_sstables_root().join(keyspace);
    let entries = std::fs::read_dir(&ks_dir).ok()?;
    for entry in entries.flatten() {
        let p = entry.path();
        if !p.is_dir() {
            continue;
        }
        let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if !name.starts_with(prefix) {
            continue;
        }
        let has_data = std::fs::read_dir(&p)
            .map(|rd| {
                rd.flatten().any(|e| {
                    e.file_name()
                        .to_str()
                        .map(|n| n.ends_with("-Data.db"))
                        .unwrap_or(false)
                })
            })
            .unwrap_or(false);
        if has_data {
            return Some(p);
        }
    }
    None
}

/// Find the single `<base>-Filter.db` in a generation dir.
fn find_filter(dir: &Path) -> Option<PathBuf> {
    std::fs::read_dir(dir).ok()?.flatten().find_map(|e| {
        let p = e.path();
        let n = p.file_name().and_then(|s| s.to_str()).unwrap_or("");
        if n.ends_with("-Filter.db") && !n.starts_with("._") {
            Some(p)
        } else {
            None
        }
    })
}

/// Copy a whole SSTable generation dir into `dst`, then flip one bit in the
/// `Filter.db` bit array (byte `BITSET_FLIP_OFFSET`, XOR `BITSET_FLIP_MASK`),
/// leaving the 8-byte header valid. Returns the destination dir.
fn materialize_corrupt_filter(src: &Path, dst: &Path) -> std::io::Result<PathBuf> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let p = entry.path();
        if !p.is_file() {
            continue;
        }
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        // Skip regeneratable sidecars so the fixture dir is a clean component set.
        if name_str.starts_with("._")
            || name_str == ".DS_Store"
            || name_str.ends_with(".db.jsonl")
            || name_str.ends_with(".db.txt")
        {
            continue;
        }
        std::fs::copy(&p, dst.join(&name))?;
    }
    let filter = find_filter(dst)
        .unwrap_or_else(|| panic!("no Filter.db in copied generation {}", dst.display()));
    let mut bytes = std::fs::read(&filter)?;
    assert!(
        bytes.len() > BITSET_FLIP_OFFSET,
        "Filter.db too small ({} bytes) to flip a bit-array bit",
        bytes.len()
    );
    let before = bytes[BITSET_FLIP_OFFSET];
    bytes[BITSET_FLIP_OFFSET] ^= BITSET_FLIP_MASK;
    let after = bytes[BITSET_FLIP_OFFSET];
    assert_ne!(before, after, "bit flip must change the byte");
    std::fs::write(&filter, &bytes)?;
    Ok(dst.to_path_buf())
}

async fn make_platform() -> Arc<Platform> {
    let config = Config::default();
    Arc::new(
        Platform::new(&config)
            .await
            .expect("Platform::new must succeed"),
    )
}

/// Count Bloom-filter false negatives over the authoritative present keys
/// (Index.db `key_digest`). `filter_bytes` may be clean or corrupt.
async fn false_negatives(
    filter_bytes: &[u8],
    index_path: &Path,
    platform: Arc<Platform>,
) -> (usize, usize) {
    let bloom = BloomFilter::deserialize(filter_bytes)
        .unwrap_or_else(|e| panic!("decode filter {}: {e:?}", index_path.display()));
    let reader = IndexReader::open(index_path, platform)
        .await
        .unwrap_or_else(|e| panic!("open {}: {e:?}", index_path.display()));
    let mut present = 0usize;
    let mut fneg = 0usize;
    for entry in reader.get_partition_entries() {
        present += 1;
        if !bloom.might_contain(&entry.key_digest) {
            fneg += 1;
        }
    }
    (present, fneg)
}

// ---------------------------------------------------------------------------
// Exposure pin (BIG): the false negative is real, verify --mode full DETECTS it,
// and the full scan is UNAFFECTED.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn filter_db_bit_flip_exposure_and_verify_detection_big() {
    let Some(clean) = resolve_clean_generation("test_comp", "lz4_table") else {
        skip_or_require(
            "filter_db_bit_flip BIG exposure",
            "clean test_comp/lz4_table generation not materialized",
        );
        return;
    };
    let clean_filter = find_filter(&clean).expect("clean lz4_table has a Filter.db");
    let base = clean_filter
        .file_name()
        .and_then(|n| n.to_str())
        .and_then(|n| n.strip_suffix("-Filter.db"))
        .expect("filter base name");
    let clean_index = clean.join(format!("{base}-Index.db"));
    assert!(
        clean_index.exists(),
        "BIG lz4_table must ship an Index.db (authoritative present keys): {}",
        clean_index.display()
    );

    let platform = make_platform().await;
    let clean_filter_bytes = std::fs::read(&clean_filter).expect("read clean Filter.db");

    // (0) Clean baseline: the healthy filter has ZERO false negatives. Anchors the
    // corruption assertion — if the clean filter already had a false negative the
    // exposure pin would be meaningless.
    let (present, clean_fn) =
        false_negatives(&clean_filter_bytes, &clean_index, platform.clone()).await;
    assert!(
        present > 0,
        "lz4_table Index.db yielded zero present keys — cannot exercise the exposure"
    );
    assert_eq!(
        clean_fn, 0,
        "clean Filter.db must have no false negatives over {present} present keys"
    );

    // Materialize the corrupt-filter fixture (byte 8 XOR 0x10) into a tempdir.
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path().join("filter_db_bit_flip");
    materialize_corrupt_filter(&clean, &dir).expect("materialize corrupt filter");
    let corrupt_filter_bytes =
        std::fs::read(dir.join(format!("{base}-Filter.db"))).expect("read corrupt Filter.db");

    // Header (bytes 0..8) unchanged; exactly the flipped bit-array byte differs.
    assert_eq!(
        &corrupt_filter_bytes[..BITSET_FLIP_OFFSET],
        &clean_filter_bytes[..BITSET_FLIP_OFFSET],
        "the 8-byte Bloom header must be left valid"
    );
    assert_eq!(
        corrupt_filter_bytes[BITSET_FLIP_OFFSET],
        clean_filter_bytes[BITSET_FLIP_OFFSET] ^ BITSET_FLIP_MASK,
        "exactly the bit-array byte must be flipped"
    );

    // (1) THE EXPOSURE: the corrupt-but-parseable filter reports a present key
    // ABSENT (false negative). This is exactly the predicate partition_lookup.rs
    // evaluates before returning Ok(None) on the BIG point-lookup path — the live
    // partition would be silently invisible. FIXTURE-ROT GUARD: a "corrupt"
    // fixture that produced NO false negative (e.g. the flip hit an unset bit)
    // would fail here, so this can never vacuously pass on a healthy filter.
    let corrupt_index = dir.join(format!("{base}-Index.db"));
    let (present_c, corrupt_fn) =
        false_negatives(&corrupt_filter_bytes, &corrupt_index, platform.clone()).await;
    assert_eq!(
        present_c, present,
        "present-key set must be identical (only Filter.db changed)"
    );
    assert!(
        corrupt_fn > 0,
        "corrupt Filter.db MUST produce at least one false negative over {present} present keys \
         (fixture rot: the bit flip did not turn off a bit a present key relies on)"
    );

    // (2) DETECTION: cqlite verify --mode full reports FilterFalseNegative for the
    // corrupt fixture, attributed to Filter.db (a detection tool Cassandra lacks).
    let config = Config::default();
    let report = verify_sstable(&dir, VerifyMode::Full, &config, platform.clone())
        .await
        .unwrap_or_else(|e| panic!("verify_sstable(corrupt) Err: {e}"));
    let has_fn_finding = report
        .findings
        .iter()
        .any(|f| f.class == VerifyErrorClass::FilterFalseNegative && f.component == "Filter.db");
    assert!(
        has_fn_finding,
        "verify --mode full must report a FilterFalseNegative finding on Filter.db, got: {:?}",
        report
            .findings
            .iter()
            .map(|f| f.to_string())
            .collect::<Vec<_>>()
    );

    // (4) CLEAN BASELINE through the same verify surface: no FilterFalseNegative,
    // and the full scan reports a non-zero row count we can compare against.
    let clean_report = verify_sstable(&clean, VerifyMode::Full, &config, platform)
        .await
        .unwrap_or_else(|e| panic!("verify_sstable(clean) Err: {e}"));
    assert!(
        clean_report
            .findings
            .iter()
            .all(|f| f.class != VerifyErrorClass::FilterFalseNegative),
        "clean lz4_table must NOT report FilterFalseNegative, got: {:?}",
        clean_report
            .findings
            .iter()
            .map(|f| f.to_string())
            .collect::<Vec<_>>()
    );
    let clean_rows = clean_report.rows_scanned;
    assert!(
        clean_rows.map(|n| n > 0).unwrap_or(false),
        "clean baseline FULL scan must read rows; rows_scanned={clean_rows:?}"
    );

    // (3) FULL SCAN UNAFFECTED: the scan never consults the bloom, so every row is
    // still read despite the corrupt filter — the corrupt-fixture scan reads the
    // SAME row count as the clean baseline (rows_scanned counts rows, and the
    // single wide partition holds many clustering rows).
    assert_eq!(
        report.rows_scanned, clean_rows,
        "FULL scan must read the same row count with the corrupt filter as clean \
         (full scans do not gate on the Bloom filter); corrupt={:?} clean={clean_rows:?}",
        report.rows_scanned
    );

    eprintln!(
        "filter_db_bit_flip BIG exposure: present_keys={present} clean_fn={clean_fn} \
         corrupt_fn={corrupt_fn} verify=FilterFalseNegative rows_scanned={:?} (clean {clean_rows:?})",
        report.rows_scanned
    );
}

// ---------------------------------------------------------------------------
// BTI immunity: the SAME bit-array flip on a BTI (`da`) Filter.db does NOT yield
// a FilterFalseNegative, because BTI resolves partitions via the Partitions.db
// trie (the authoritative oracle) and bypasses the bloom entirely
// (partition_lookup.rs:640-650). The full scan is likewise unaffected.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn filter_db_bit_flip_bti_paths_unaffected() {
    let Some(clean) = resolve_clean_generation("test_da", "wide_table") else {
        skip_or_require(
            "filter_db_bit_flip BTI immunity",
            "clean test_da/wide_table (da/BTI) generation not materialized",
        );
        return;
    };
    let Some(clean_filter) = find_filter(&clean) else {
        skip_or_require(
            "filter_db_bit_flip BTI immunity",
            "BTI wide_table has no Filter.db",
        );
        return;
    };
    // Ensure the filter is big enough to flip a bit-array byte; otherwise skip.
    let clean_bytes = std::fs::read(&clean_filter).expect("read BTI Filter.db");
    if clean_bytes.len() <= BITSET_FLIP_OFFSET {
        skip_or_require(
            "filter_db_bit_flip BTI immunity",
            "BTI Filter.db too small to flip a bit-array bit",
        );
        return;
    }

    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path().join("bti_filter_flip");
    materialize_corrupt_filter(&clean, &dir).expect("materialize corrupt BTI filter");

    let platform = make_platform().await;
    let config = Config::default();
    let report = verify_sstable(&dir, VerifyMode::Full, &config, platform)
        .await
        .unwrap_or_else(|e| panic!("verify_sstable(BTI corrupt filter) Err: {e}"));

    // BTI is IMMUNE: no FilterFalseNegative regardless of the bit-array flip.
    assert!(
        report
            .findings
            .iter()
            .all(|f| f.class != VerifyErrorClass::FilterFalseNegative),
        "BTI must be immune to Filter.db bit flips (bloom bypassed for the trie); \
         got FilterFalseNegative: {:?}",
        report
            .findings
            .iter()
            .map(|f| f.to_string())
            .collect::<Vec<_>>()
    );
    // Full scan unaffected.
    assert!(
        report.rows_scanned.map(|n| n > 0).unwrap_or(false),
        "BTI FULL scan must still read rows despite the corrupt filter; rows_scanned={:?}",
        report.rows_scanned
    );

    eprintln!(
        "filter_db_bit_flip BTI immunity: no FilterFalseNegative, rows_scanned={:?}",
        report.rows_scanned
    );
}
