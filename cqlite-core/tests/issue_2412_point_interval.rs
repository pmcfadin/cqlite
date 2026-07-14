//! Issue #2412 (Stage 3, spec `lazy-big-partition-index` Requirement 2) — a BIG
//! point lookup resolves through ONE `Summary.db`-bounded `Index.db` interval, never
//! by materializing the whole partition map.
//!
//! Public-surface work-probe pins (design §B/§F), driven through
//! `SSTableReader::get` on real Cassandra fixtures:
//!
//! - **Present key**: the point read resolves the partition, records exactly ONE
//!   bounded interval parse (`cqlite.sstable.index_interval_parses_total`) and ZERO
//!   full parses (`cqlite.sstable.index_parses_total`).
//! - **Within-range absent key** (bloom removed so the absence must come from the
//!   interval, not a bloom short-circuit): the read returns "not found" as an
//!   authoritative absence from ONE interval WITHOUT a whole-file `scan_for_key`.
//! - **Interval-boundary key** (a `Summary.db` sample key, which sits exactly at an
//!   interval boundary): resolves correctly via one interval.
//!
//! The byte-identical-golden property (Requirement 2) is covered by the repository's
//! sstabledump / query-semantics parity oracles, which exercise `get()`/`scan()` on
//! these same fixtures; these pins add the scale-free WORK bounds those oracles cannot
//! observe.
//!
//! Separate integration-test process: the OTel capture harness installs a
//! PROCESS-GLOBAL meter provider, so this must not share cqlite-core's parallel
//! `--lib` unit-test binary (roborev #2163 / #2385 precedent).
//!
//! Run with:
//! ```text
//! cargo test -p cqlite-core --features observability-testing \
//!   --test issue_2412_point_interval
//! ```

#![cfg(feature = "observability-testing")]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use cqlite_core::observability::{catalog, testing};
use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::index_reader::IndexReader;
use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::storage::sstable::summary_reader::SummaryReader;
use cqlite_core::types::{RowKey, TableId};
use cqlite_core::util::cassandra_murmur3::cassandra_murmur3_token;
use cqlite_core::Config;
use serial_test::serial;

fn datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

/// Locate a `*-Data.db` in `<datasets>/sstables/<keyspace>/<table>-*/` that has a
/// sibling `*-Index.db` AND `*-Summary.db` (a BIG SSTable with a usable summary).
fn find_big_data_file_with_summary(keyspace: &str, table: &str) -> Option<PathBuf> {
    let root = datasets_root()?;
    let entries = std::fs::read_dir(root.join("sstables").join(keyspace)).ok()?;
    let prefix = format!("{table}-");
    for e in entries.flatten() {
        if !e.file_name().to_string_lossy().starts_with(&prefix) {
            continue;
        }
        let dir = e.path();
        let files = std::fs::read_dir(&dir).ok()?;
        let mut data_file: Option<PathBuf> = None;
        let mut has_index = false;
        let mut has_summary = false;
        for f in files.flatten() {
            let name = f.file_name().to_string_lossy().into_owned();
            if name.ends_with("-Data.db") {
                data_file = Some(f.path());
            } else if name.ends_with("-Index.db") {
                has_index = true;
            } else if name.ends_with("-Summary.db") {
                has_summary = true;
            }
        }
        if has_index && has_summary {
            if let Some(df) = data_file {
                return Some(df);
            }
        }
    }
    None
}

/// The sibling component path for a `-Data.db` file (e.g. `-Index.db`, `-Summary.db`).
fn sibling(data_file: &Path, suffix: &str) -> PathBuf {
    let name = data_file.file_name().unwrap().to_string_lossy();
    let base = name.strip_suffix("-Data.db").unwrap();
    data_file.with_file_name(format!("{base}{suffix}"))
}

/// Eagerly parse the fixture's sibling `Index.db` to recover its present raw
/// partition keys — a HELPER reader, distinct from the (lazy) reader under test.
fn present_raw_keys(
    data_file: &Path,
    platform: Arc<Platform>,
    rt: &tokio::runtime::Runtime,
) -> Vec<Vec<u8>> {
    let index_path = sibling(data_file, "-Index.db");
    let ir = rt
        .block_on(IndexReader::open(&index_path, platform))
        .expect("eager Index.db open for present-key harvest");
    ir.get_partition_entries()
        .iter()
        .map(|e| e.key_digest.to_vec())
        .collect()
}

/// Present-key point read: exactly one bounded interval parse, zero full parses.
/// Spec Requirement 2 scenario "A present-key point read touches at most one summary
/// interval" + Requirement 5 (interval work counted separately from full parses).
#[test]
#[serial]
fn present_key_point_read_touches_one_interval_zero_full_parses() {
    let mc = testing::metrics_capture();

    let Some(data_file) = find_big_data_file_with_summary("test_basic", "simple_table") else {
        eprintln!("Skipping (#2412 present-key interval): BIG test_basic/simple_table absent");
        return;
    };

    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let config = Config::default();
    let platform = Arc::new(
        rt.block_on(Platform::new(&config))
            .expect("platform must initialize"),
    );

    let keys = present_raw_keys(&data_file, platform.clone(), &rt);
    assert!(
        !keys.is_empty(),
        "fixture must expose present Index.db entries (0-rows-when-present is a failure)"
    );
    let present = keys[0].clone();

    let reader = rt
        .block_on(SSTableReader::open(&data_file, &config, platform))
        .expect("lazy BIG open");
    let table_id = TableId::from("test_basic.simple_table");

    mc.reset();
    let got = rt
        .block_on(reader.get(&table_id, &RowKey::new(present)))
        .expect("point read must not error");
    let m = mc.flush_and_collect();

    assert!(
        got.is_some(),
        "a known-present partition key must resolve to a row via the interval path"
    );
    assert_eq!(
        m.counter_sum(catalog::INDEX_PARSES_TOTAL),
        0.0,
        "a lazy point read must perform ZERO full Index.db parses (design §B/§F)"
    );
    assert_eq!(
        m.counter_sum(catalog::INDEX_INTERVAL_PARSES_TOTAL),
        1.0,
        "a present-key point read reads EXACTLY ONE bounded Summary-guided interval"
    );
}

/// Interval-boundary key (a `Summary.db` sample, which sits exactly at an interval
/// boundary) resolves correctly via one interval. Spec Requirement 2 scenario "A key
/// at an interval boundary resolves correctly".
#[test]
#[serial]
fn interval_boundary_sample_key_resolves_via_one_interval() {
    let mc = testing::metrics_capture();

    let Some(data_file) = find_big_data_file_with_summary("test_basic", "simple_table") else {
        eprintln!("Skipping (#2412 boundary key): BIG test_basic/simple_table absent");
        return;
    };

    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let config = Config::default();
    let platform = Arc::new(
        rt.block_on(Platform::new(&config))
            .expect("platform must initialize"),
    );

    let summary_path = sibling(&data_file, "-Summary.db");
    let summary = rt
        .block_on(SummaryReader::open(&summary_path, platform.clone()))
        .expect("Summary.db open");
    let samples = summary.get_entries();
    assert!(
        !samples.is_empty(),
        "a usable Summary.db must expose at least one sample"
    );
    // Prefer an INTERIOR boundary (a sample after the first) when present; the first
    // sample sits at position 0 (index start). Either way the key is a boundary key.
    let boundary_key = samples[samples.len().saturating_sub(1)]
        .partition_key
        .to_vec();

    let reader = rt
        .block_on(SSTableReader::open(&data_file, &config, platform))
        .expect("lazy BIG open");
    let table_id = TableId::from("test_basic.simple_table");

    mc.reset();
    let got = rt
        .block_on(reader.get(&table_id, &RowKey::new(boundary_key)))
        .expect("boundary point read must not error");
    let m = mc.flush_and_collect();

    assert!(
        got.is_some(),
        "a Summary.db sample key (interval boundary) must resolve to its present partition"
    );
    assert_eq!(
        m.counter_sum(catalog::INDEX_PARSES_TOTAL),
        0.0,
        "a boundary point read must perform ZERO full Index.db parses"
    );
    assert_eq!(
        m.counter_sum(catalog::INDEX_INTERVAL_PARSES_TOTAL),
        1.0,
        "a boundary key resolves through EXACTLY ONE bounded interval"
    );
}

/// Copy every component of `data_file`'s SSTable into a fresh temp dir EXCEPT the
/// `-Filter.db` bloom filter, returning the copied `-Data.db` path. Without a bloom
/// filter the point read cannot short-circuit an absent key at the presence oracle,
/// so a within-range absent key must reach — and be answered by — the Summary-guided
/// interval path.
fn copy_fixture_without_filter(data_file: &Path) -> (PathBuf, PathBuf) {
    let src_dir = data_file.parent().expect("fixture parent");
    let tmp = std::env::temp_dir().join(format!(
        "cqlite-2412-nofilter-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&tmp).expect("mkdir temp fixture copy");
    let mut copied_data: Option<PathBuf> = None;
    for entry in std::fs::read_dir(src_dir)
        .expect("read fixture dir")
        .flatten()
    {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.ends_with("-Filter.db") {
            continue; // deliberately omitted: disables the bloom pre-check
        }
        let dest = tmp.join(&name);
        std::fs::copy(entry.path(), &dest).expect("copy fixture component");
        if name.ends_with("-Data.db") {
            copied_data = Some(dest);
        }
    }
    (tmp, copied_data.expect("fixture must include a Data.db"))
}

/// Within-range absent key: authoritative absence from ONE interval, NO whole-file
/// `scan_for_key`. Spec Requirement 2 scenario "An absent-key point read within range
/// is authoritative from one interval".
#[test]
#[serial]
fn within_range_absent_key_is_authoritative_without_scan_for_key() {
    let mc = testing::metrics_capture();

    let Some(data_file) = find_big_data_file_with_summary("test_basic", "simple_table") else {
        eprintln!("Skipping (#2412 absent-in-interval): BIG test_basic/simple_table absent");
        return;
    };

    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let config = Config::default();
    let platform = Arc::new(
        rt.block_on(Platform::new(&config))
            .expect("platform must initialize"),
    );

    // Present set (for absence) + the token bound for a NON-LAST (end-bounded)
    // interval: the constructed key's token must sort strictly between the FIRST and
    // the LAST `Summary.db` SAMPLE, so it (a) passes the C5 range check (in
    // `[first_key, last_key]`) and (b) floors to an interval delimited above by a
    // next sample — the authoritative-absence path (`covering_interval_is_end_bounded`).
    // The last (read-to-EOF) interval is intentionally avoided; a miss there keeps the
    // #1572 scan fallback and would not exercise the no-scan property.
    let present: std::collections::HashSet<Vec<u8>> =
        present_raw_keys(&data_file, platform.clone(), &rt)
            .into_iter()
            .collect();
    let summary_path = sibling(&data_file, "-Summary.db");
    let summary = rt
        .block_on(SummaryReader::open(&summary_path, platform.clone()))
        .expect("Summary.db open");
    let samples = summary.get_entries();
    if samples.len() < 2 {
        eprintln!(
            "Skipping (#2412 absent-in-interval): fixture has < 2 summary samples, no \
             end-bounded interval to exercise"
        );
        return;
    }
    let first_sample_token = cassandra_murmur3_token(&samples[0].partition_key);
    let last_sample_token = cassandra_murmur3_token(&samples[samples.len() - 1].partition_key);
    let (lo, hi) = (
        first_sample_token.min(last_sample_token),
        first_sample_token.max(last_sample_token),
    );

    // Deterministic search for a 16-byte key whose Murmur3 token sorts strictly
    // inside (lo, hi) and is absent. UUID keys spread tokens across the ring, so an
    // in-range absent candidate is found within a few probes.
    let mut absent: Option<Vec<u8>> = None;
    for i in 0u64..200_000 {
        let a = i.to_be_bytes();
        let b = i.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_be_bytes();
        let cand = [a, b].concat();
        let t = cassandra_murmur3_token(&cand);
        if t > lo && t < hi && !present.contains(&cand) {
            absent = Some(cand);
            break;
        }
    }
    let Some(absent) = absent else {
        eprintln!("Skipping (#2412 absent-in-interval): no in-range absent key constructed");
        return;
    };

    let (tmp, copied_data) = copy_fixture_without_filter(&data_file);
    let reader = rt
        .block_on(SSTableReader::open(&copied_data, &config, platform))
        .expect("lazy BIG open without Filter.db");
    let table_id = TableId::from("test_basic.simple_table");

    let scan_before = SSTableReader::scan_for_key_call_count();
    mc.reset();
    let got = rt
        .block_on(reader.get(&table_id, &RowKey::new(absent)))
        .expect("absent point read must not error");
    let m = mc.flush_and_collect();
    let scan_after = SSTableReader::scan_for_key_call_count();

    assert!(
        got.is_none(),
        "an in-range absent key must resolve to 'not found'"
    );
    assert_eq!(
        scan_after, scan_before,
        "an in-range absent key answered from the bounded interval must NOT fall back to a \
         whole-file scan_for_key (design §B)"
    );
    assert_eq!(
        m.counter_sum(catalog::INDEX_INTERVAL_PARSES_TOTAL),
        1.0,
        "the absence must be resolved by reading EXACTLY ONE bounded interval (not a bloom \
         short-circuit — Filter.db was removed)"
    );
    assert_eq!(
        m.counter_sum(catalog::INDEX_PARSES_TOTAL),
        0.0,
        "the absent read must perform ZERO full Index.db parses"
    );

    let _ = std::fs::remove_dir_all(&tmp);
}
