//! #4626 phase 5, review fix: an input the generation listing does not return (hidden by a
//! transaction log, listing refused, a non-`*-Data.db` name) still has its `Statistics.db` read
//! through the trusted path. A damaged one fails closed (#4612); it is never skipped.

#![cfg(all(feature = "write-support", not(feature = "tombstones")))]

#[path = "support/datasets_root.rs"]
mod datasets_root;
#[path = "support/generation_fixtures.rs"]
mod generation_fixtures;

use std::path::{Path, PathBuf};

use cqlite_core::schema::TableSchema;
use cqlite_core::storage::write_engine::merge::{
    all_input_stats_readable, compact_sstables, compute_baseline_min,
};
use generation_fixtures::*;

fn schema() -> TableSchema {
    cqlite_core::schema::cql_parser::parse_cql_schema(
        "CREATE TABLE gd4626.txn_compaction (id int PRIMARY KEY, v text)",
    )
    .expect("schema")
}

async fn compact_err(inputs: Vec<PathBuf>) -> String {
    let out = tempfile::tempdir().unwrap();
    compact_sstables(inputs, out.path(), &schema(), 9, None, None, false)
        .await
        .err()
        .expect("a damaged input must be refused, compaction returned Ok")
        .to_string()
}

fn damage(stats: &Path) {
    let mut b = std::fs::read(stats).unwrap();
    let n = b.len();
    b[n / 2] ^= 0xff; // a component checksum no longer matches
    b.truncate(n - 1);
    std::fs::write(stats, b).unwrap();
}

/// A txn-hidden input (committed REMOVE) with a damaged Statistics.db: refused, naming the file.
#[tokio::test]
async fn a_txn_hidden_input_with_a_corrupt_statistics_db_fails_closed() {
    let (_root, dir) = stamped_fixture_copy("txn_committed");
    let stats = dir.join("nb-1-big-Statistics.db");
    damage(&stats);
    let err = compact_err(vec![dir.join("nb-1-big-Data.db")]).await;
    assert!(err.contains("nb-1-big-Statistics.db"), "{err}");
}

/// A directory whose listing is refused (inconsistent transaction log): the same.
#[tokio::test]
async fn an_input_in_an_unlistable_directory_with_a_corrupt_statistics_db_fails_closed() {
    let (_root, dir) = stamped_fixture_copy("txn_inconsistent");
    let stats = dir.join("nb-4-big-Statistics.db");
    damage(&stats);
    let err = compact_err(vec![dir.join("nb-4-big-Data.db")]).await;
    assert!(err.contains("nb-4-big-Statistics.db"), "{err}");
}

/// A non-standard name (`input-Data.db`, not an SSTable component name): the same.
#[tokio::test]
async fn a_non_standard_named_input_with_a_corrupt_statistics_db_fails_closed() {
    let src = fixture_table("txn_committed", "txn_compaction");
    let dir = tempfile::tempdir().unwrap();
    std::fs::copy(
        src.join("nb-4-big-Data.db"),
        dir.path().join("input-Data.db"),
    )
    .unwrap();
    std::fs::copy(
        src.join("nb-4-big-Statistics.db"),
        dir.path().join("input-Statistics.db"),
    )
    .unwrap();
    damage(&dir.path().join("input-Statistics.db"));
    let err = compact_err(vec![dir.path().join("input-Data.db")]).await;
    assert!(err.contains("input-Statistics.db"), "{err}");
}

/// The public wrappers read "the sibling Statistics.db" for a path outside the listing too.
#[test]
fn wrappers_read_the_sibling_statistics_db_of_a_non_standard_name() {
    let src = fixture_table("txn_committed", "txn_compaction");
    let dir = tempfile::tempdir().unwrap();
    for (from, to) in [
        ("nb-4-big-Data.db", "input-Data.db"),
        ("nb-4-big-Statistics.db", "input-Statistics.db"),
    ] {
        std::fs::copy(src.join(from), dir.path().join(to)).unwrap();
    }
    let standard = compute_baseline_min(&[src.join("nb-4-big-Data.db")]);
    let odd = compute_baseline_min(&[dir.path().join("input-Data.db")]);
    assert_eq!(
        odd, standard,
        "the baseline must not depend on the file name"
    );
    assert_ne!(odd.0, i64::MAX, "the baseline was read, not skipped");
    assert!(all_input_stats_readable(&[dir
        .path()
        .join("input-Data.db")]));
    assert!(!all_input_stats_readable(&[dir
        .path()
        .join("none-Data.db")]));
}
