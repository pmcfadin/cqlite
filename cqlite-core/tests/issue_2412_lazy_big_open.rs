//! Issue #2412 (Stage 2, spec `lazy-big-partition-index` Requirement 1) — cold BIG
//! `SSTableReader::open` with a usable `Summary.db` performs ZERO full `Index.db`
//! parses, scale-free (independent of partition count).
//!
//! Distinguishes the two scenarios the spec names:
//! - A BIG SSTable WITH `Summary.db`: lazy open, `index_parses_total += 0`.
//! - A BIG SSTable WITHOUT `Summary.db` (synthesized by copying a fixture and
//!   removing its `Summary.db` sibling): the §A1 counted FellBack full parse,
//!   `index_parses_total += 1` — never silent, never a regression for shapes that
//!   legitimately ship without a usable Summary.db.
//!
//! Separate integration-test process: the OTel capture harness installs a
//! PROCESS-GLOBAL meter provider, so this must not share cqlite-core's parallel
//! `--lib` unit-test binary (roborev #2163 / #2385 precedent).
//!
//! Run with:
//! ```text
//! cargo test -p cqlite-core --features observability-testing \
//!   --test issue_2412_lazy_big_open
//! ```

#![cfg(feature = "observability-testing")]

use std::path::PathBuf;
use std::sync::Arc;

use cqlite_core::observability::{catalog, testing};
use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::Config;

fn datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

/// Locate a `*-Data.db` in `<datasets>/sstables/<keyspace>/<table>-*/` that has a
/// sibling `*-Index.db` AND `*-Summary.db` (a BIG-format SSTable with a usable
/// summary). Skip keys off fixture presence.
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

/// Requirement 1, Scenario "Cold BIG open touches zero Index.db entries and
/// performs zero full parses": a BIG SSTable with a usable `Summary.db` opens
/// without a single full `Index.db` parse.
#[test]
fn cold_open_with_summary_performs_zero_full_parses() {
    let mc = testing::metrics_capture();

    let Some(data_file) = find_big_data_file_with_summary("test_basic", "simple_table") else {
        eprintln!(
            "Skipping (#2412 lazy BIG open): BIG test_basic/simple_table fixture (with \
             Summary.db) absent"
        );
        return;
    };

    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let config = Config::default();
    let platform = Arc::new(
        rt.block_on(Platform::new(&config))
            .expect("platform must initialize"),
    );

    mc.reset();
    let reader = rt
        .block_on(SSTableReader::open(&data_file, &config, platform))
        .expect("BIG fixture with Summary.db must open");
    // Keep the reader alive across the collection so the open work is attributed.
    let _ = &reader;

    let full_parses = mc
        .flush_and_collect()
        .counter_sum(catalog::INDEX_PARSES_TOTAL);
    assert_eq!(
        full_parses, 0.0,
        "a BIG open with a usable Summary.db must perform ZERO full Index.db \
         parses (lazy Summary-guided open, issue #2412); got {full_parses}"
    );
}

/// Requirement 6, Scenario "Absent Summary.db falls back to a counted full parse,
/// not a guess": copy a real fixture into a temp dir, delete its `Summary.db`
/// sibling, and confirm opening it performs EXACTLY ONE full parse (the §A1
/// FellBack), never silent and never zero (which would silently under-read).
#[test]
fn absent_summary_falls_back_to_one_counted_full_parse() {
    let mc = testing::metrics_capture();

    let Some(data_file) = find_big_data_file_with_summary("test_basic", "simple_table") else {
        eprintln!(
            "Skipping (#2412 FellBack): BIG test_basic/simple_table fixture (with Summary.db) \
             absent"
        );
        return;
    };
    let src_dir = data_file.parent().expect("fixture has a parent dir");

    let tmp = std::env::temp_dir().join(format!(
        "cqlite-2412-no-summary-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&tmp).expect("mkdir temp fixture copy");
    let mut copied_data_path: Option<PathBuf> = None;
    for entry in std::fs::read_dir(src_dir)
        .expect("read fixture dir")
        .flatten()
    {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.ends_with("-Summary.db") {
            // Deliberately NOT copied: this synthesizes the absent-Summary.db shape.
            continue;
        }
        let dest = tmp.join(&name);
        std::fs::copy(entry.path(), &dest).expect("copy fixture component");
        if name.ends_with("-Data.db") {
            copied_data_path = Some(dest);
        }
    }
    let copied_data_path = copied_data_path.expect("fixture must include a Data.db");

    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let config = Config::default();
    let platform = Arc::new(
        rt.block_on(Platform::new(&config))
            .expect("platform must initialize"),
    );

    mc.reset();
    let reader = rt
        .block_on(SSTableReader::open(&copied_data_path, &config, platform))
        .expect("BIG fixture without Summary.db must still open (FellBack, not a hard error)");
    let _ = &reader;

    let full_parses = mc
        .flush_and_collect()
        .counter_sum(catalog::INDEX_PARSES_TOTAL);
    assert_eq!(
        full_parses, 1.0,
        "an absent Summary.db must FellBack to exactly ONE counted full Index.db parse \
         (issue #2412 design §A1), never silent and never zero; got {full_parses}"
    );

    let _ = std::fs::remove_dir_all(&tmp);
}
