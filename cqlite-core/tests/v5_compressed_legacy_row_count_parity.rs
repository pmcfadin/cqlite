/// Regression test for Issue #196: Ensure parser reads ALL partitions
///
/// This test validates that the V5CompressedLegacy parser continues reading
/// partitions even when encountering malformed data, rather than silently
/// stopping after ~3 partitions.
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::testing::resolve_table_to_sstable_path;
use cqlite_core::{Config, Platform};

// ---------------------------------------------------------------------------
// Fail-closed gate (issue #1242)
// ---------------------------------------------------------------------------

/// CI fail-closed switch. The `sstabledump-parity-gate.yml` workflow sets
/// `CQLITE_PARITY_REQUIRE_DATASETS=1` and treats this test's step as a REQUIRED
/// gate. In that mode a missing dataset / missing golden / zero matched rows
/// must PANIC (the gate enforces real coverage) rather than silently skip and
/// green-pass. Locally (env unset) the test keeps its skip-on-absence behavior.
fn parity_datasets_required() -> bool {
    std::env::var("CQLITE_PARITY_REQUIRE_DATASETS")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// Skip when local (flag unset), but FAIL-CLOSED (panic) when
/// `CQLITE_PARITY_REQUIRE_DATASETS=1` is set.
fn skip_or_fail_closed(test_name: &str, reason: &str) {
    if parity_datasets_required() {
        panic!(
            "{test_name}: CQLITE_PARITY_REQUIRE_DATASETS=1 but {reason} — \
             required parity gate cannot green-pass without running fail-closed (issue #1242)"
        );
    }
    eprintln!("{test_name}: SKIPPED ({reason})");
}

fn count_jsonl_rows(path: &Path) -> std::io::Result<usize> {
    let file = File::open(path)?;
    Ok(BufReader::new(file).lines().count())
}

/// Find the Data.db file in the SSTable directory
fn find_data_db(dir: &Path) -> Option<PathBuf> {
    if dir.is_file() {
        // Already a file, return it if it's Data.db
        if dir.file_name()?.to_str()?.ends_with("-Data.db") {
            return Some(dir.to_path_buf());
        }
        return None;
    }

    // Search directory for Data.db file
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            if let Some(name) = entry.file_name().to_str() {
                if name.ends_with("-Data.db") {
                    return Some(entry.path());
                }
            }
        }
    }
    None
}

#[tokio::test]
async fn test_v5_multi_partition_parity_simple_table() {
    let test_name = "test_v5_multi_partition_parity_simple_table";
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    // Use test_basic/simple_table - should have multiple partitions
    let sstable_dir = match resolve_table_to_sstable_path("test_basic", "simple_table") {
        Ok(dir) => dir,
        Err(e) => {
            skip_or_fail_closed(test_name, &format!("dataset not resolvable: {e}"));
            return;
        }
    };

    let sstable_path = match find_data_db(&sstable_dir) {
        Some(p) => p,
        None => {
            skip_or_fail_closed(test_name, "Data.db not found in SSTable directory");
            return;
        }
    };

    let reader = SSTableReader::open(&sstable_path, &config, platform)
        .await
        .expect("Failed to open SSTable");

    let entries = reader
        .get_all_entries()
        .await
        .expect("Failed to read entries");

    // Load expected count from JSONL reference file (same basename as Data.db with .jsonl suffix)
    let jsonl_filename = sstable_path
        .file_name()
        .and_then(|n| n.to_str())
        .map(|n| n.replace("-Data.db", "-Data.db.jsonl"))
        .expect("Failed to construct JSONL filename");
    let jsonl_path = sstable_dir.join(jsonl_filename);

    let expected_rows = match count_jsonl_rows(&jsonl_path) {
        Ok(n) => n,
        Err(_) => {
            skip_or_fail_closed(
                test_name,
                &format!("JSONL golden not readable at {:?}", jsonl_path),
            );
            return;
        }
    };

    // A MISSING or genuinely EMPTY golden is an absence signal: skip locally,
    // fail-closed under CQLITE_PARITY_REQUIRE_DATASETS=1. But a PRESENT golden
    // with `expected_rows > 0` while the parser returned zero entries is a real
    // parser regression (issue #196 / #1242 finding) — NEVER skippable, assert
    // failure UNCONDITIONALLY regardless of the fail-closed flag.
    if expected_rows == 0 {
        skip_or_fail_closed(
            test_name,
            &format!(
                "golden {:?} is empty (0 rows) — Data.db/golden absent?",
                jsonl_path
            ),
        );
        return;
    }

    assert!(
        !entries.is_empty(),
        "Parser returned 0 entries but golden {:?} has {} rows — V5 parser regression \
         (issue #196): a present, non-empty golden with zero parsed rows is a real defect, \
         not a skip (issue #1242)",
        jsonl_path,
        expected_rows
    );

    assert_eq!(
        entries.len(),
        expected_rows,
        "Parser stopped early (Issue #196): got {} rows, expected {} from Data.db.jsonl",
        entries.len(),
        expected_rows
    );
}

#[tokio::test]
async fn test_v5_multi_partition_parity_collection_table() {
    let test_name = "test_v5_multi_partition_parity_collection_table";
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    // Use test_collections/collection_table - should have multiple partitions with complex data
    let sstable_dir = match resolve_table_to_sstable_path("test_collections", "collection_table") {
        Ok(dir) => dir,
        Err(e) => {
            skip_or_fail_closed(test_name, &format!("dataset not resolvable: {e}"));
            return;
        }
    };

    let sstable_path = match find_data_db(&sstable_dir) {
        Some(p) => p,
        None => {
            skip_or_fail_closed(test_name, "Data.db not found in SSTable directory");
            return;
        }
    };

    let reader = SSTableReader::open(&sstable_path, &config, platform)
        .await
        .expect("Failed to open SSTable");

    let entries = reader
        .get_all_entries()
        .await
        .expect("Failed to read entries");

    // Load expected count from JSONL reference file (same basename as Data.db with .jsonl suffix)
    let jsonl_filename = sstable_path
        .file_name()
        .and_then(|n| n.to_str())
        .map(|n| n.replace("-Data.db", "-Data.db.jsonl"))
        .expect("Failed to construct JSONL filename");
    let jsonl_path = sstable_dir.join(jsonl_filename);

    let expected_rows = match count_jsonl_rows(&jsonl_path) {
        Ok(n) => n,
        Err(_) => {
            skip_or_fail_closed(
                test_name,
                &format!("JSONL golden not readable at {:?}", jsonl_path),
            );
            return;
        }
    };

    // MISSING / EMPTY golden = absence (skip or fail-closed); a PRESENT non-empty
    // golden with zero parsed entries is a real parser regression (issue #1242).
    if expected_rows == 0 {
        skip_or_fail_closed(
            test_name,
            &format!(
                "golden {:?} is empty (0 rows) — Data.db/golden absent?",
                jsonl_path
            ),
        );
        return;
    }

    assert!(
        !entries.is_empty(),
        "Parser returned 0 entries but golden {:?} has {} rows — V5 parser regression \
         (issue #196): a present, non-empty golden with zero parsed rows is a real defect, \
         not a skip (issue #1242)",
        jsonl_path,
        expected_rows
    );

    assert_eq!(
        entries.len(),
        expected_rows,
        "Parser stopped early on collection table (Issue #196): got {} rows, expected {} from Data.db.jsonl",
        entries.len(),
        expected_rows
    );
}

#[tokio::test]
async fn test_v5_no_early_termination_on_parse_errors() {
    // This test ensures that parse errors don't cause early termination
    // The parser should log warnings and continue to subsequent partitions

    let test_name = "test_v5_no_early_termination_on_parse_errors";
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    let sstable_dir = match resolve_table_to_sstable_path("test_basic", "simple_table") {
        Ok(dir) => dir,
        Err(e) => {
            skip_or_fail_closed(test_name, &format!("dataset not resolvable: {e}"));
            return;
        }
    };

    let sstable_path = match find_data_db(&sstable_dir) {
        Some(p) => p,
        None => {
            skip_or_fail_closed(test_name, "Data.db not found in SSTable directory");
            return;
        }
    };

    let reader = SSTableReader::open(&sstable_path, &config, platform)
        .await
        .expect("Failed to open SSTable");

    let entries = reader
        .get_all_entries()
        .await
        .expect("Failed to read entries");

    // At minimum, we should get some entries - the bug would cause 0 or very few
    assert!(
        !entries.is_empty(),
        "Parser should read at least some entries, but got 0 (possible early termination)"
    );
}
