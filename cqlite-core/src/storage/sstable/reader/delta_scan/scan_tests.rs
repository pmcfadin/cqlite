//! `scan_delta` corpus tests: upserts, writetimes, static columns and cell
//! tombstones (Issues #698/#699).
//!
//! Split out of `scan.rs` per the campsite rule (#1116/#1135) — that source file
//! was 1922 lines, 1494 of them this one inline `mod tests`, so any addition to
//! the driver had to move the tests out first. This is a VERBATIM move: no
//! assertion changed. The sibling half (`scan_tombstone_tests.rs`) carries the
//! row/range/partition-tombstone + collection tests and the shared
//! `find_test_deltas_table_dir` helper they alone use.
//!
//! Included from [`super`] via `#[path = "scan_tests.rs"]`, so `use super::*`
//! reaches the `scan_delta` driver and its parse helpers.

use super::*;

// -----------------------------------------------------------------------
// Integration spot-check: scan_delta on corpus SSTable directories
// -----------------------------------------------------------------------

/// Integration test: scan_delta yields at least one Upsert record from
/// `test_basic/simple_table`.  Validates that the streaming API works
/// end-to-end with a real SSTable.
///
/// Skipped automatically when CQLITE_DATASETS_ROOT is not set or the
/// Data.db file is not present (fetch with `bash test-data/scripts/fetch-datasets.sh`).
#[tokio::test]
async fn scan_delta_yields_upserts_from_simple_table() {
    let root = match std::env::var("CQLITE_DATASETS_ROOT") {
        Ok(r) => std::path::PathBuf::from(r),
        Err(_) => {
            eprintln!("CQLITE_DATASETS_ROOT not set — skipping scan_delta integration test");
            return;
        }
    };

    let base = root.join("sstables/test_basic");
    if !base.exists() {
        eprintln!("test_basic not found — skipping");
        return;
    }

    // Find the simple_table directory.
    let table_dir = std::fs::read_dir(&base).ok().and_then(|mut it| {
        it.find_map(|e| {
            e.ok()
                .filter(|e| {
                    e.file_name()
                        .to_str()
                        .map(|n| n.starts_with("simple_table"))
                        .unwrap_or(false)
                })
                .map(|e| e.path())
        })
    });

    let Some(table_dir) = table_dir else {
        eprintln!("simple_table dir not found — skipping");
        return;
    };

    // Check that a Data.db actually exists; skip gracefully if not.
    let has_data_db = std::fs::read_dir(&table_dir)
        .ok()
        .map(|it| {
            it.filter_map(|e| e.ok()).any(|e| {
                e.file_name()
                    .to_str()
                    .map(|n| n.ends_with("-Data.db"))
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false);
    if !has_data_db {
        eprintln!("No Data.db in simple_table — skipping (run fetch-datasets.sh)");
        return;
    }

    // Build a minimal schema for test_basic.simple_table.
    let schema = crate::schema::TableSchema {
        keyspace: "test_basic".to_string(),
        table: "simple_table".to_string(),
        partition_keys: vec![crate::schema::KeyColumn {
            name: "id".to_string(),
            data_type: "uuid".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            crate::schema::Column {
                name: "name".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            crate::schema::Column {
                name: "value".to_string(),
                data_type: "int".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: std::collections::HashMap::new(),
        dropped_columns: std::collections::HashMap::new(),
    };

    let (mut rx, _scan_summary) = scan_delta(table_dir, schema, 64);
    let mut upsert_count = 0_usize;
    let mut total = 0_usize;

    while let Some(result) = rx.recv().await {
        total += 1;
        match result {
            Ok(DeltaRecord::Upsert { .. }) => upsert_count += 1,
            Ok(DeltaRecord::StaticUpsert { .. }) => {}
            Ok(other) => {
                panic!(
                    "simple_table should have no tombstones; got {:?}",
                    other.op_name()
                );
            }
            Err(e) => panic!("scan_delta error: {e}"),
        }
    }

    eprintln!(
        "scan_delta simple_table: {} total records, {} upserts",
        total, upsert_count
    );
    assert!(
        upsert_count > 0,
        "expected at least one Upsert from simple_table"
    );
}

/// Integration spot-check: each Upsert from `test_basic/simple_table`
/// has a non-zero writetime on at least one cell.
#[tokio::test]
async fn scan_delta_cells_have_nonzero_writetime() {
    let root = match std::env::var("CQLITE_DATASETS_ROOT") {
        Ok(r) => std::path::PathBuf::from(r),
        Err(_) => return,
    };

    let base = root.join("sstables/test_basic");
    if !base.exists() {
        return;
    }

    let table_dir = std::fs::read_dir(&base).ok().and_then(|mut it| {
        it.find_map(|e| {
            e.ok()
                .filter(|e| {
                    e.file_name()
                        .to_str()
                        .map(|n| n.starts_with("simple_table"))
                        .unwrap_or(false)
                })
                .map(|e| e.path())
        })
    });

    let Some(table_dir) = table_dir else {
        return;
    };

    let has_data_db = std::fs::read_dir(&table_dir)
        .ok()
        .map(|it| {
            it.filter_map(|e| e.ok()).any(|e| {
                e.file_name()
                    .to_str()
                    .map(|n| n.ends_with("-Data.db"))
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false);
    if !has_data_db {
        return;
    }

    let schema = crate::schema::TableSchema {
        keyspace: "test_basic".to_string(),
        table: "simple_table".to_string(),
        partition_keys: vec![crate::schema::KeyColumn {
            name: "id".to_string(),
            data_type: "uuid".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            crate::schema::Column {
                name: "name".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            crate::schema::Column {
                name: "value".to_string(),
                data_type: "int".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: std::collections::HashMap::new(),
        dropped_columns: std::collections::HashMap::new(),
    };

    let (mut rx, _scan_summary) = scan_delta(table_dir, schema, 64);
    let mut checked = 0_usize;

    while let Some(result) = rx.recv().await {
        if let Ok(DeltaRecord::Upsert { cells, .. }) = result {
            for (_, cell) in &cells {
                // writetime must be a plausible Cassandra µs timestamp
                // (after 2010-01-01, i.e. > 1_262_304_000_000_000 µs).
                assert!(
                    cell.writetime > 1_262_304_000_000_000,
                    "writetime {} is suspiciously small (cell {:?})",
                    cell.writetime,
                    cell.value
                );
                checked += 1;
            }
        }
    }

    if checked > 0 {
        eprintln!("scan_delta writetime check: verified {} cells", checked);
    }
}

// -----------------------------------------------------------------------
// E2E: StaticUpsert path — real SSTable (test_basic/static_columns_table)
// -----------------------------------------------------------------------

/// Integration test: scan_delta emits at least one `StaticUpsert` record
/// from `test_basic/static_columns_table`, which has a STATIC TEXT column
/// (`static_data`) alongside clustered rows.
///
/// Skipped automatically when CQLITE_DATASETS_ROOT is unset or the
/// Data.db file is absent (run `bash test-data/scripts/fetch-datasets.sh`).
#[tokio::test]
async fn scan_delta_emits_static_upsert_from_static_columns_table() {
    let root = match std::env::var("CQLITE_DATASETS_ROOT") {
        Ok(r) => std::path::PathBuf::from(r),
        Err(_) => {
            eprintln!("CQLITE_DATASETS_ROOT not set — skipping StaticUpsert e2e test");
            return;
        }
    };

    let base = root.join("sstables/test_basic");
    if !base.exists() {
        eprintln!("test_basic not found — skipping StaticUpsert e2e test");
        return;
    }

    // Find the static_columns_table directory (prefix match).
    let table_dir = std::fs::read_dir(&base).ok().and_then(|mut it| {
        it.find_map(|e| {
            e.ok()
                .filter(|e| {
                    e.file_name()
                        .to_str()
                        .map(|n| n.starts_with("static_columns_table"))
                        .unwrap_or(false)
                })
                .map(|e| e.path())
        })
    });

    let Some(table_dir) = table_dir else {
        eprintln!("static_columns_table dir not found — skipping StaticUpsert e2e test");
        return;
    };

    // Skip gracefully if Data.db is not present.
    let has_data_db = std::fs::read_dir(&table_dir)
        .ok()
        .map(|it| {
            it.filter_map(|e| e.ok()).any(|e| {
                e.file_name()
                    .to_str()
                    .map(|n| n.ends_with("-Data.db"))
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false);
    if !has_data_db {
        eprintln!("No Data.db in static_columns_table — skipping (run fetch-datasets.sh)");
        return;
    }

    // Schema for test_basic.static_columns_table:
    //   PRIMARY KEY (partition_key UUID, clustering_key TIMESTAMP)
    //   static_data TEXT STATIC
    //   row_data    TEXT
    //   row_value   INT
    let schema = crate::schema::TableSchema {
        keyspace: "test_basic".to_string(),
        table: "static_columns_table".to_string(),
        partition_keys: vec![crate::schema::KeyColumn {
            name: "partition_key".to_string(),
            data_type: "uuid".to_string(),
            position: 0,
        }],
        clustering_keys: vec![crate::schema::ClusteringColumn {
            name: "clustering_key".to_string(),
            data_type: "timestamp".to_string(),
            position: 0,
            order: crate::schema::ClusteringOrder::Asc,
        }],
        columns: vec![
            crate::schema::Column {
                name: "static_data".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: true,
            },
            crate::schema::Column {
                name: "row_data".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            crate::schema::Column {
                name: "row_value".to_string(),
                data_type: "int".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: std::collections::HashMap::new(),
        dropped_columns: std::collections::HashMap::new(),
    };

    let (mut rx, _scan_summary) = scan_delta(table_dir, schema, 64);
    let mut static_upsert_count = 0_usize;
    let mut upsert_count = 0_usize;

    while let Some(result) = rx.recv().await {
        match result {
            Ok(DeltaRecord::StaticUpsert { ref cells, .. }) => {
                static_upsert_count += 1;
                // Each StaticUpsert must have at least one cell.
                assert!(
                    !cells.is_empty(),
                    "StaticUpsert must have at least one cell delta"
                );
            }
            Ok(DeltaRecord::Upsert { .. }) => {
                upsert_count += 1;
            }
            Ok(other) => {
                // Row/range/partition tombstones are not expected here and
                // are out of scope for Issue #698, but we don't panic —
                // the test_basic corpus should not contain tombstones.
                eprintln!(
                    "scan_delta static_columns_table: unexpected record: {}",
                    other.op_name()
                );
            }
            Err(e) => panic!("scan_delta error on static_columns_table: {e}"),
        }
    }

    eprintln!(
        "scan_delta static_columns_table: {} StaticUpserts, {} Upserts",
        static_upsert_count, upsert_count
    );
    assert!(
        static_upsert_count > 0,
        "expected at least one StaticUpsert from static_columns_table; \
         got {} StaticUpserts and {} Upserts",
        static_upsert_count,
        upsert_count
    );
}

// -----------------------------------------------------------------------
// E2E: cell-tombstone path — real SSTable (test_deltas/cell_tombstones)
// -----------------------------------------------------------------------

/// Integration test: scan_delta emits at least one `CellDelta { value: None }`
/// from `test_deltas/cell_tombstones`, which was written by issuing
/// `UPDATE … SET col_b = null …` against rows that had `col_b` set.
///
/// This test is **gated** on the presence of the `test_deltas` binary Data.db
/// files, which are not committed to git (they are regenerated locally via
/// `bash test-data/scripts/generate-deltas.sh`).  The test skips cleanly
/// with a message if the binary is absent, matching the project convention for
/// dataset-gated tests.  It will skip in CI until the test_deltas dataset
/// asset is published.
#[tokio::test]
async fn scan_delta_emits_cell_tombstone_from_cell_tombstones_table() {
    let root = match std::env::var("CQLITE_DATASETS_ROOT") {
        Ok(r) => std::path::PathBuf::from(r),
        Err(_) => {
            eprintln!("CQLITE_DATASETS_ROOT not set — skipping cell-tombstone e2e test");
            return;
        }
    };

    let deltas_dir = root.join("sstables/test_deltas");
    if !deltas_dir.exists() {
        eprintln!(
            "test_deltas not found at {:?} — skipping cell-tombstone e2e test \
             (run `bash test-data/scripts/generate-deltas.sh` to regenerate)",
            deltas_dir
        );
        return;
    }

    // Find the cell_tombstones directory (prefix match).
    let table_dir = std::fs::read_dir(&deltas_dir).ok().and_then(|mut it| {
        it.find_map(|e| {
            e.ok()
                .filter(|e| {
                    e.file_name()
                        .to_str()
                        .map(|n| n.starts_with("cell_tombstones"))
                        .unwrap_or(false)
                })
                .map(|e| e.path())
        })
    });

    let Some(table_dir) = table_dir else {
        eprintln!("cell_tombstones dir not found — skipping cell-tombstone e2e test");
        return;
    };

    // Skip gracefully if the binary Data.db is absent (only JSONL present).
    let has_data_db = std::fs::read_dir(&table_dir)
        .ok()
        .map(|it| {
            it.filter_map(|e| e.ok()).any(|e| {
                let name = e.file_name();
                let n = name.to_string_lossy();
                // Must end with -Data.db but NOT be the .jsonl reference file.
                n.ends_with("-Data.db") && !n.ends_with(".db.jsonl")
            })
        })
        .unwrap_or(false);
    if !has_data_db {
        eprintln!(
            "No binary Data.db in cell_tombstones — skipping cell-tombstone e2e test \
             (run `bash test-data/scripts/generate-deltas.sh` to regenerate binaries; \
             test_deltas binaries are not in the published dataset asset)"
        );
        return;
    }

    // Schema for test_deltas.cell_tombstones:
    //   PRIMARY KEY (pk INT, ck INT)
    //   col_a TEXT
    //   col_b TEXT   ← this column has cell tombstones after UPDATE … SET col_b = null
    let schema = crate::schema::TableSchema {
        keyspace: "test_deltas".to_string(),
        table: "cell_tombstones".to_string(),
        partition_keys: vec![crate::schema::KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![crate::schema::ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: crate::schema::ClusteringOrder::Asc,
        }],
        columns: vec![
            crate::schema::Column {
                name: "col_a".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            crate::schema::Column {
                name: "col_b".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: std::collections::HashMap::new(),
        dropped_columns: std::collections::HashMap::new(),
    };

    let (mut rx, _scan_summary) = scan_delta(table_dir, schema, 64);
    let mut cell_tombstone_count = 0_usize;
    let mut total_cells = 0_usize;

    while let Some(result) = rx.recv().await {
        match result {
            Ok(DeltaRecord::Upsert { cells, .. }) => {
                for (col_id, cell) in &cells {
                    total_cells += 1;
                    if cell.value.is_none() {
                        // This is a cell tombstone flowing through the real path.
                        cell_tombstone_count += 1;
                        eprintln!(
                            "cell-tombstone e2e: column {:?} has CellDelta {{ value: None, writetime: {} }}",
                            col_id.0, cell.writetime
                        );
                    }
                }
            }
            Ok(DeltaRecord::StaticUpsert { .. }) => {}
            Ok(other) => {
                // Row/partition tombstones may appear in cell_tombstones too;
                // they are out of scope for #698 but we don't fail the test.
                eprintln!(
                    "scan_delta cell_tombstones: got {} (out of #698 scope)",
                    other.op_name()
                );
            }
            Err(e) => panic!("scan_delta error on cell_tombstones: {e}"),
        }
    }

    eprintln!(
        "scan_delta cell_tombstones e2e: {} cell tombstones out of {} total cells",
        cell_tombstone_count, total_cells
    );
    assert!(
        cell_tombstone_count > 0,
        "expected at least one CellDelta {{ value: None }} from cell_tombstones; \
         got {} total cells with 0 tombstones",
        total_cells
    );
}
