//! Issue #2385 / #2395 — opening a BIG (`nb`) SSTable parses its `Index.db`
//! EXACTLY ONCE.
//!
//! Before this fix the reader open path parsed the same `Index.db` twice: once via
//! `load_index` (Strategy 2 → `convert_index_reader_to_sstable_index`, only to
//! build the now-retired digest-keyed `SSTableIndex`) and once via
//! `load_index_reader` (the raw-key `IndexReader` that actually serves lookups).
//! At 1.42M partitions that doubled a multi-minute cold parse (#2385) — the #2395
//! double-parse. This pins the win via the authoritative
//! `cqlite.sstable.index_parses_total` counter (#2383): the whole file is full-parsed
//! AT MOST ONCE (RED = 2 on the pre-fix tree).
//!
//! Issue #2412 (lazy Summary-guided open) SUPERSEDES the earlier "exactly once at
//! OPEN" phrasing: a BIG open with a usable `Summary.db` now full-parses `Index.db`
//! ZERO times at open (deferred), and the single full parse happens on the first
//! materializing use (here, a full enumeration). The anti-double-parse guard is
//! preserved as "open + one full enumeration parses EXACTLY ONCE total, never twice".
//! The pure open-time laziness is pinned separately by `issue_2412_lazy_big_open`.
//!
//! Separate integration-test process: the OTel capture harness installs a
//! PROCESS-GLOBAL meter provider, so this must not share cqlite-core's parallel
//! `--lib` unit-test binary (roborev #2163 precedent). Requires
//! `CQLITE_DATASETS_ROOT` + fetched binaries; skips (never fails) when the fixture
//! is absent — a present fixture that fails to open stays a hard failure.
//!
//! Run with:
//! ```text
//! cargo test -p cqlite-core --features observability-testing \
//!   --test issue_2385_index_single_parse
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
/// sibling `*-Index.db` (a BIG-format SSTable). Skip keys off fixture presence.
fn find_big_data_file(keyspace: &str, table: &str) -> Option<PathBuf> {
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
        for f in files.flatten() {
            let name = f.file_name().to_string_lossy().into_owned();
            if name.ends_with("-Data.db") {
                data_file = Some(f.path());
            } else if name.ends_with("-Index.db") {
                has_index = true;
            }
        }
        if has_index {
            if let Some(df) = data_file {
                return Some(df);
            }
        }
    }
    None
}

/// Opening a BIG SSTable plus one full enumeration must full-parse `Index.db` at
/// most once (never the pre-#2395 twice).
///
/// RED on the pre-fix tree: `load_index` (Strategy 2 convert) + `load_index_reader`
/// each ran a full parse → counter == 2. After retiring the Strategy 2 `SSTableIndex`
/// build AND the #2412 lazy open, a cold open with a usable `Summary.db` parses ZERO
/// times, and the first materializing use (the full enumeration here) parses exactly
/// once → total == 1 (never 2).
#[test]
fn open_plus_enumeration_parses_index_exactly_once() {
    // Install the process-global in-memory meter BEFORE any parse in this process.
    let mc = testing::metrics_capture();

    let Some(data_file) = find_big_data_file("test_basic", "simple_table") else {
        eprintln!(
            "Skipping (#2385 one-parse-per-open): BIG test_basic/simple_table fixture absent"
        );
        return;
    };

    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let config = Config::default();
    let platform = Arc::new(
        rt.block_on(Platform::new(&config))
            .expect("platform must initialize"),
    );

    // Reset BEFORE the open so the entire open + enumeration path is measured from zero.
    mc.reset();
    let reader = rt
        .block_on(SSTableReader::open(&data_file, &config, platform))
        .expect("BIG fixture must open");

    // Open alone (with a usable Summary.db) full-parses ZERO times — the #2412 lazy win.
    let parses_after_open = mc
        .flush_and_collect()
        .counter_sum(catalog::INDEX_PARSES_TOTAL);
    assert_eq!(
        parses_after_open, 0.0,
        "a BIG open with a usable Summary.db must full-parse Index.db ZERO times (lazy, \
         issue #2412); got {parses_after_open}"
    );

    // The first materializing use (a full enumeration) full-parses EXACTLY ONCE — never
    // the pre-#2395 twice. `flush_and_collect` is delta temporality, so this is the
    // enumeration's own parse count.
    let _ = rt
        .block_on(reader.iterate_all_partitions())
        .expect("full enumeration must succeed");
    let parses_after_enum = mc
        .flush_and_collect()
        .counter_sum(catalog::INDEX_PARSES_TOTAL);
    assert_eq!(
        parses_after_enum, 1.0,
        "the first full enumeration must full-parse Index.db EXACTLY ONCE (got \
         {parses_after_enum}); pre-#2395 this whole path parsed twice"
    );
}
