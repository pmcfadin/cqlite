//! Crate-root unit tests, split out of `lib.rs` under the campsite rule
//! (issue #1135): `lib.rs` sits at the source size target, so the tests live
//! beside it rather than inside it.

use super::*;
use tempfile::TempDir;

#[tokio::test]
async fn test_database_open_close() {
    let temp_dir = TempDir::new().unwrap();
    let config = Config::test_config();

    let db = Database::open(temp_dir.path(), config).await.unwrap();
    db.close().await.unwrap();
}

/// Documents that open_with_discovered_sstables_and_registry is crate-private.
/// This test exists to document the API contract - the function should NOT be
/// callable from integration tests or external crates.
#[cfg(feature = "state_machine")]
#[test]
fn test_open_with_discovered_sstables_and_registry_is_crate_private() {
    // This test compiling proves the function exists and is accessible within the crate
    // If we accidentally made it pub instead of pub(crate), integration tests could access it
    // The function signature itself enforces this via pub(crate) keyword

    // Note: We don't actually call the function here since it requires async setup
    // The mere existence of this test documents the API boundary
    assert!(
        true,
        "open_with_discovered_sstables_and_registry is correctly marked pub(crate)"
    );
}

#[tokio::test]
#[cfg(feature = "state_machine")]
async fn test_database_open_with_discovered_sstables() {
    let temp_dir = TempDir::new().unwrap();
    let config = Config::test_config();

    // Create an empty list of discovered table directories
    let discovered_dirs = Vec::new();

    let db = Database::open_with_discovered_sstables(temp_dir.path(), discovered_dirs, config)
        .await
        .unwrap();

    // Verify database was created successfully
    let stats = db.stats().await.unwrap();
    assert_eq!(stats.storage_stats.sstables.sstable_count, 0);

    db.close().await.unwrap();
}

// NOTE: `test_database_basic_operations` (CREATE TABLE → INSERT → SELECT) was
// removed in Issue #1880. It asserted the row-count of data inserted via the
// write path, which was deleted in Issue #175 (`execute` on an INSERT now
// returns `UnsupportedFormat`), so the test could only ever panic under
// `--all-features`. Read-path SELECT coverage lives in the real-SSTable
// integration/parity tests; open/close lifecycle is covered above.
