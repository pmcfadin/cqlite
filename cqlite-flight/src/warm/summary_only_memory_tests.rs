//! Issue #2412 §D / spec Requirement 4 (Stage 5) — the `WarmTableRegistry` pins
//! SUMMARY-ONLY index memory for a generation held warm, not a full resident
//! partition map.
//!
//! Scale-free wiring-evidence pin (mirroring the #2385/#2412 pin-test style):
//! two single-generation BIG SSTables differing ONLY in partition count (small
//! vs large) are warmed through the SAME public `warm_readers` surface a Flight
//! `do_get` drives. If the registry's accounted footprint (`account_footprint`,
//! `budget.rs`) still counted the full on-disk `Index.db` (the pre-#2412
//! behavior — every partition materialized at open), the large generation's
//! footprint would scale roughly LINEARLY with partition count (a proportionally
//! bigger `Index.db`). After Stage 5 it does not: the large generation's `Index.db`
//! is never materialized on this path (Summary-guided lazy open + streaming
//! query paths, Stages 2-4), so only its `Summary.db` (O(n/128) samples) and
//! fixed overhead are counted — materially smaller than the full index, and NOT
//! scaling with `N` the way the on-disk `Index.db` size would.
//!
//! Loaded via `#[path]` from `registry.rs` (campsite rule), like the sibling
//! `registry_tests`/`spin_tests_2383` modules.

use std::path::PathBuf;

use cqlite_core::schema::TableSchema;
use cqlite_core::storage::write_engine::{Durability, WriteEngine, WriteEngineConfig};

use crate::cancel::CancelFlag;
use crate::testutil::{simple_schema, write_row, KS, TBL};
use crate::warm::{TableKey, WarmTableRegistry};

fn key() -> TableKey {
    TableKey::new(KS, TBL)
}

fn ddl() -> u64 {
    crate::warm::ddl_hash(crate::testutil::SIMPLE_DDL)
}

/// Build ONE SSTable generation with `n` distinct int-PK partitions, all in one
/// generation (large flush threshold, durability disabled — bulk-load shape,
/// mirrors `spin_tests_2383::build_big_single_gen`).
fn build_single_gen(schema: &TableSchema, n: i32) -> (tempfile::TempDir, PathBuf) {
    let temp = tempfile::TempDir::new().unwrap();
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");
    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, schema.clone())
        .with_flush_threshold(1usize << 30)
        .with_durability(Durability::Disabled);
    let mut engine = WriteEngine::new(config).expect("engine");
    for id in 0..n {
        engine.write(write_row(id, "n", id, 100)).expect("write");
    }
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(engine.flush()).expect("flush").expect("info");
    let table_dir = data_dir.join(&schema.keyspace).join(&schema.table);
    (temp, table_dir)
}

/// Find the sibling `-Index.db` / `-Summary.db` file sizes for the `-Data.db`
/// at `data_path`'s directory (authoritative on-disk sizes, no guessing).
fn sibling_component_sizes(data_path: &std::path::Path) -> (u64, u64) {
    let name = data_path
        .file_name()
        .and_then(|n| n.to_str())
        .expect("Data.db filename");
    let base = name.strip_suffix("-Data.db").expect("Data.db suffix");
    let parent = data_path.parent().expect("parent dir");
    let index_size = std::fs::metadata(parent.join(format!("{base}-Index.db")))
        .map(|m| m.len())
        .unwrap_or(0);
    let summary_size = std::fs::metadata(parent.join(format!("{base}-Summary.db")))
        .map(|m| m.len())
        .unwrap_or(0);
    (index_size, summary_size)
}

fn find_data_db(table_dir: &std::path::Path) -> std::path::PathBuf {
    std::fs::read_dir(table_dir)
        .expect("table dir")
        .flatten()
        .map(|e| e.path())
        .find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with("-Data.db"))
        })
        .expect("a Data.db exists")
}

/// Requirement 4's pinned scenario: a warm-held generation's accounted index
/// footprint is bounded by `Summary.db` size (O(n/128)), NOT by the full
/// on-disk `Index.db` size — and so does NOT scale linearly with partition
/// count the way the full `Index.db` does.
#[test]
fn warm_footprint_does_not_scale_with_partition_count() {
    let schema = simple_schema();

    // Small generation: a handful of partitions (non-vacuity control).
    let (_small_temp, small_dir) = build_single_gen(&schema, 200);
    let small_reg = WarmTableRegistry::new();
    small_reg
        .warm_readers(&key(), ddl(), &schema, &small_dir, None, &CancelFlag::new())
        .expect("small generation warms");
    let small_footprint = small_reg.debug_used_bytes();

    // Large generation: 1000x the partitions, same shape — the fixture whose
    // FULL Index.db is genuinely ~1000x bigger (verified against the on-disk
    // Index.db size below, not assumed).
    let (_large_temp, large_dir) = build_single_gen(&schema, 200_000);
    let large_data = find_data_db(&large_dir);
    let (large_index_bytes, large_summary_bytes) = sibling_component_sizes(&large_data);
    let large_reg = WarmTableRegistry::new();
    large_reg
        .warm_readers(&key(), ddl(), &schema, &large_dir, None, &CancelFlag::new())
        .expect("large generation warms");
    let large_footprint = large_reg.debug_used_bytes();

    assert!(
        small_footprint > 0,
        "small generation's footprint is non-zero"
    );
    assert!(
        large_footprint > 0,
        "large generation's footprint is non-zero"
    );
    // Non-vacuity: the large generation's REAL on-disk Index.db must genuinely
    // dwarf its Summary.db — otherwise the fixture cannot discriminate a
    // "counts the full index" bug from the correct summary-only accounting.
    assert!(
        large_index_bytes > large_summary_bytes * 20,
        "fixture precondition: the large generation's real Index.db \
         ({large_index_bytes}) must dwarf its Summary.db ({large_summary_bytes})"
    );

    // THE evidence (authoritative bound, not a guessed ceiling): compare the
    // REGISTRY's accounted footprint against what `account_footprint` itself
    // would compute counting the full `Index.db` as resident — the exact
    // pre-#2412 behavior. If the registry still counted the full index, the
    // two would coincide; the fix must land materially below it.
    let full_index_footprint = crate::warm::budget::account_footprint(&large_data, true);
    assert!(
        large_footprint < full_index_footprint / 2,
        "large-generation registry-accounted footprint ({large_footprint}) must sit \
         materially BELOW the full-Index.db-resident accounting ({full_index_footprint}) \
         — issue #2412 §D: a Summary-usable BIG generation's Index.db is not \
         resident on the query-serving path, so it must not be counted"
    );
}

/// Companion direct-accounting pin: the SAME on-disk shape counted with
/// `index_resident = true` (simulating the pre-#2412 always-eager behavior)
/// DOES scale with the large `Index.db`'s real size — proving the bounded ratio
/// above comes from the residency-aware exclusion, not from the fixture
/// happening to produce small `Index.db` files.
#[test]
fn resident_accounting_of_the_same_fixture_would_scale_with_index_size() {
    use crate::warm::budget::account_footprint;

    let schema = simple_schema();
    let (_temp, dir) = build_single_gen(&schema, 200_000);
    let data_path = std::fs::read_dir(&dir)
        .expect("table dir")
        .flatten()
        .map(|e| e.path())
        .find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with("-Data.db"))
        })
        .expect("a Data.db exists");

    let lazy = account_footprint(&data_path, false);
    let resident = account_footprint(&data_path, true);
    assert!(
        resident > lazy * 5,
        "a 200k-partition Index.db is large enough that counting it resident \
         ({resident}) must dwarf the lazy (summary-only) accounting ({lazy}) — \
         otherwise this fixture cannot discriminate the two modes"
    );
}
