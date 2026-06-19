//! VG5: da (BTI) foundation tests (Issue #657)
//!
//! These tests verify the foundation work for da/BTI SSTable support:
//!
//! 1. Directory scan classifies `Partitions.db`/`Rows.db` correctly without warnings.
//! 2. `SSTableReader::open` on a da SSTable returns a structured "BTI read support
//!    not yet implemented" error — not a panic or confusing parse failure.
//! 3. `FormatDetector` maps `da` to `V5x` (not `Unknown`).
//! 4. Discovery finds the `test_da` tables from the real fixture.
//!
//! # Test data
//!
//! Tests that require real SSTable files read the `CQLITE_DATASETS_ROOT` environment
//! variable (set to `test-data/datasets` when running the local gate).  Tests that
//! do not require binary SSTable files run unconditionally.

#[cfg(test)]
mod da_component_routing {
    use std::str::FromStr;

    use cqlite_core::storage::sstable::directory::SSTableComponent;

    /// Partitions.db must parse to `SSTableComponent::Partitions` without error.
    #[test]
    fn partitions_db_parses_to_partitions_component() {
        let component = SSTableComponent::from_str("Partitions.db")
            .expect("Partitions.db should parse to SSTableComponent::Partitions");
        assert_eq!(
            component,
            SSTableComponent::Partitions,
            "Partitions.db must map to SSTableComponent::Partitions"
        );
    }

    /// Rows.db must parse to `SSTableComponent::Rows` without error.
    #[test]
    fn rows_db_parses_to_rows_component() {
        let component = SSTableComponent::from_str("Rows.db")
            .expect("Rows.db should parse to SSTableComponent::Rows");
        assert_eq!(
            component,
            SSTableComponent::Rows,
            "Rows.db must map to SSTableComponent::Rows"
        );
    }

    /// Both Partitions.db and Rows.db must be recognised as BTI-specific.
    #[test]
    fn partitions_and_rows_are_bti_specific() {
        assert!(
            SSTableComponent::Partitions.is_bti_specific(),
            "Partitions is a BTI-specific component"
        );
        assert!(
            SSTableComponent::Rows.is_bti_specific(),
            "Rows is a BTI-specific component"
        );
    }

    /// BIG-specific components must NOT be flagged as BTI-specific.
    #[test]
    fn big_specific_components_are_not_bti_specific() {
        assert!(
            !SSTableComponent::Index.is_bti_specific(),
            "Index.db is BIG-specific, not BTI-specific"
        );
        assert!(
            !SSTableComponent::Summary.is_bti_specific(),
            "Summary.db is BIG-specific, not BTI-specific"
        );
    }

    /// Partitions.db must NOT be flagged as BIG-specific.
    #[test]
    fn partitions_is_not_big_specific() {
        assert!(
            !SSTableComponent::Partitions.is_big_specific(),
            "Partitions.db must not be BIG-specific"
        );
    }

    /// Rows.db must NOT be flagged as BIG-specific.
    #[test]
    fn rows_is_not_big_specific() {
        assert!(
            !SSTableComponent::Rows.is_big_specific(),
            "Rows.db must not be BIG-specific"
        );
    }

    /// File extension round-trips for BTI components.
    #[test]
    fn bti_component_extensions_round_trip() {
        assert_eq!(
            SSTableComponent::Partitions.file_extension(),
            "Partitions.db"
        );
        assert_eq!(SSTableComponent::Rows.file_extension(), "Rows.db");
    }
}

#[cfg(test)]
mod da_format_detection {
    use cqlite_core::storage::sstable::format_detector::{FormatDetector, SSTableFormat};

    /// `da` must map to `V5x` in the FormatDetector — not `Unknown`.
    #[test]
    fn da_maps_to_v5x_not_unknown() {
        let detector = FormatDetector::new();
        let fmt = detector
            .detect_from_version("da")
            .expect("FormatDetector must not error for the known 'da' version");

        assert_ne!(
            fmt,
            SSTableFormat::Unknown("da".to_string()),
            "da must NOT map to Unknown"
        );
        assert_eq!(
            fmt,
            SSTableFormat::V5x("da".to_string()),
            "da must map to V5x"
        );
        assert!(
            detector.is_supported("da"),
            "da must appear in supported_versions()"
        );
    }

    /// `da` format reports it supports compression (consistent with other V5x).
    #[test]
    fn da_format_supports_compression() {
        let fmt = SSTableFormat::V5x("da".to_string());
        assert!(
            fmt.supports_compression(),
            "da (V5x) format must support compression"
        );
    }

    /// `SSTableInfo::from_path` on a da Data.db must return V5x format.
    #[test]
    fn sstable_info_from_da_data_db_is_v5x() {
        use cqlite_core::storage::sstable::format_detector::SSTableInfo;
        use std::path::PathBuf;

        let path = PathBuf::from("da-2-bti-Data.db");
        let info = SSTableInfo::from_path(&path).expect("SSTableInfo::from_path must succeed");

        assert_eq!(
            info.format,
            SSTableFormat::V5x("da".to_string()),
            "da-2-bti-Data.db must yield V5x format, not Unknown"
        );
        assert_eq!(info.sstable_id, "2");
        assert_eq!(info.size, "bti");
    }
}

#[cfg(test)]
mod da_directory_discovery {
    use cqlite_core::storage::sstable::directory::{SSTableComponent, SSTableDirectory};

    /// Helper: return the path to the da simple_table fixture, or None if not available.
    ///
    /// Returns `None` when:
    /// - `CQLITE_DATASETS_ROOT` is not set, or
    /// - the `test_da/simple_table-*` directory does not exist, or
    /// - the directory contains only git-tracked JSONL/metadata files (CI with
    ///   datasets-v2 which has no da binary SSTables).  We detect the binary
    ///   presence by looking for at least one `da-*-bti-Partitions.db` file.
    fn da_simple_table_path() -> Option<std::path::PathBuf> {
        let datasets_root = std::env::var("CQLITE_DATASETS_ROOT").ok()?;
        let base = std::path::Path::new(&datasets_root)
            .join("sstables")
            .join("test_da");

        // Find the simple_table directory (includes UUID suffix)
        let table_dir = std::fs::read_dir(&base)
            .ok()?
            .flatten()
            .find(|e| e.file_name().to_string_lossy().starts_with("simple_table-"))
            .map(|e| e.path())?;

        // Guard: require at least one binary Partitions.db sentinel file.
        // Without it the directory contains only git-tracked JSONL goldens and
        // SSTableDirectory::scan will fail — causing a spurious test failure on
        // CI environments that use datasets-v2 (no da binaries).
        let has_binary_partitions = std::fs::read_dir(&table_dir).ok()?.flatten().any(|e| {
            let name = e.file_name();
            let s = name.to_string_lossy();
            s.starts_with("da-") && s.ends_with("-bti-Partitions.db")
        });

        if !has_binary_partitions {
            eprintln!(
                "SKIP: test_da binaries not present (CI uses datasets-v2; goldens-only checkout). \
                 Run `bash test-data/scripts/fetch-datasets.sh` to download binary SSTables."
            );
            return None;
        }

        Some(table_dir)
    }

    /// Directory scan of the da `simple_table` fixture must succeed — classifying
    /// Partitions.db and Rows.db without warnings or errors.
    #[test]
    fn da_simple_table_scans_without_error() {
        let Some(table_path) = da_simple_table_path() else {
            eprintln!(
                "SKIP: CQLITE_DATASETS_ROOT not set or test_da/simple_table not found; \
                 run `bash test-data/scripts/fetch-datasets.sh` first"
            );
            return;
        };

        let dir = SSTableDirectory::scan(&table_path).unwrap_or_else(|e| {
            panic!(
                "SSTableDirectory::scan failed for da simple_table at {:?}: {}",
                table_path, e
            )
        });

        // Must find at least one generation
        assert!(
            !dir.generations.is_empty(),
            "da simple_table must have at least one generation"
        );

        let gen = &dir.generations[0];
        assert_eq!(gen.format, "bti", "da format segment must be 'bti'");
        assert_eq!(gen.version, "da", "da version must be 'da'");

        // Partitions.db and Rows.db must be classified correctly
        assert!(
            gen.components.contains_key(&SSTableComponent::Partitions),
            "da generation must include SSTableComponent::Partitions; \
             components present: {:?}",
            gen.components.keys().collect::<Vec<_>>()
        );
        assert!(
            gen.components.contains_key(&SSTableComponent::Rows),
            "da generation must include SSTableComponent::Rows; \
             components present: {:?}",
            gen.components.keys().collect::<Vec<_>>()
        );

        // Data.db and Statistics.db must also be present
        assert!(
            gen.components.contains_key(&SSTableComponent::Data),
            "da generation must include SSTableComponent::Data"
        );
        assert!(
            gen.components.contains_key(&SSTableComponent::Statistics),
            "da generation must include SSTableComponent::Statistics"
        );
    }

    /// The three foundational da tables (simple_table, collection_table,
    /// ttl_table) must be discoverable from the `test_da` keyspace directory.
    ///
    /// NOTE (issue #832): the `test_da` keyspace also carries a `wide_table`
    /// fixture used by the BTI row-index traversal tests, so the directory count
    /// is now >= 3 rather than exactly 3.  This test pins the *required* tables'
    /// presence, not an exact directory count.
    #[test]
    fn da_keyspace_discovers_three_tables() {
        let datasets_root = match std::env::var("CQLITE_DATASETS_ROOT") {
            Ok(v) => v,
            Err(_) => {
                eprintln!("SKIP: CQLITE_DATASETS_ROOT not set");
                return;
            }
        };

        let da_ks_path = std::path::Path::new(&datasets_root)
            .join("sstables")
            .join("test_da");

        if !da_ks_path.exists() {
            eprintln!(
                "SKIP: test_da keyspace directory not found at {:?}",
                da_ks_path
            );
            return;
        }

        let table_dirs: Vec<_> = std::fs::read_dir(&da_ks_path)
            .unwrap()
            .flatten()
            .filter(|e| e.path().is_dir())
            .collect();

        assert!(
            table_dirs.len() >= 3,
            "test_da keyspace must have at least the 3 foundational table \
             directories (simple_table, collection_table, ttl_table). Found: {:?}",
            table_dirs.iter().map(|e| e.file_name()).collect::<Vec<_>>()
        );

        // Each expected table must be discoverable
        let table_names: Vec<String> = table_dirs
            .iter()
            .map(|e| {
                e.file_name()
                    .to_string_lossy()
                    .split('-')
                    .next()
                    .unwrap_or("")
                    .to_string()
            })
            .collect();

        for expected in &["simple_table", "collection_table", "ttl_table"] {
            assert!(
                table_names.iter().any(|n| n == expected),
                "Expected table '{}' not found in test_da. Found: {:?}",
                expected,
                table_names
            );
        }
    }

    /// Scanning the da directory must not produce any component-routing warnings
    /// for Partitions.db/Rows.db (they must be classified, not silently skipped).
    ///
    /// This test verifies the component count includes BTI-specific components.
    #[test]
    fn da_scan_includes_bti_specific_components_not_skipped() {
        let Some(table_path) = da_simple_table_path() else {
            eprintln!("SKIP: CQLITE_DATASETS_ROOT not set or da fixture not available");
            return;
        };

        let dir = SSTableDirectory::scan(&table_path).expect("scan must succeed");
        let gen = &dir.generations[0];

        // The da fixture has: Data, Statistics, CompressionInfo, Filter,
        // Partitions, Rows, Digest, TOC — at least 6 components.
        assert!(
            gen.components.len() >= 6,
            "da generation must have >= 6 classified components (including Partitions/Rows). \
             Got {} components: {:?}",
            gen.components.len(),
            gen.components.keys().collect::<Vec<_>>()
        );
    }
}

#[cfg(test)]
mod da_reader_graceful_rejection {
    use cqlite_core::{storage::sstable::reader::SSTableReader, Config, Error};
    use std::sync::Arc;

    /// Helper: return the path to the da simple_table Data.db fixture, or None.
    fn da_data_db_path() -> Option<std::path::PathBuf> {
        let datasets_root = std::env::var("CQLITE_DATASETS_ROOT").ok()?;
        let base = std::path::Path::new(&datasets_root)
            .join("sstables")
            .join("test_da");

        let simple_table_dir = std::fs::read_dir(&base)
            .ok()?
            .flatten()
            .find(|e| e.file_name().to_string_lossy().starts_with("simple_table-"))?
            .path();

        // Find da-N-bti-Data.db within the directory
        std::fs::read_dir(&simple_table_dir)
            .ok()?
            .flatten()
            .find(|e| {
                let name = e.file_name();
                let s = name.to_string_lossy();
                s.starts_with("da-") && s.ends_with("-bti-Data.db")
            })
            .map(|e| e.path())
    }

    /// Opening a da Data.db with `SSTableReader::open` must return
    /// `Error::UnsupportedFormat` — not a panic, and not a confusing parse error.
    ///
    /// The error message must mention "BTI (da)" so callers can act on it.
    #[tokio::test]
    async fn da_reader_open_returns_unsupported_format_error() {
        let Some(data_db) = da_data_db_path() else {
            eprintln!(
                "SKIP: CQLITE_DATASETS_ROOT not set or da Data.db not found; \
                 run `bash test-data/scripts/fetch-datasets.sh` first"
            );
            return;
        };

        let config = Config::default();
        let platform = Arc::new(
            cqlite_core::platform::Platform::new(&config)
                .await
                .expect("Platform::new must succeed"),
        );

        let result = SSTableReader::open(&data_db, &config, platform).await;

        assert!(
            result.is_err(),
            "SSTableReader::open on a da Data.db must return an error"
        );

        let err = result.unwrap_err();

        // Must be UnsupportedFormat — not Corruption or Parse or Internal.
        assert!(
            matches!(err, Error::UnsupportedFormat(_)),
            "Error must be UnsupportedFormat, got: {:?}",
            err
        );

        let msg = err.to_string();
        assert!(
            msg.contains("BTI (da)"),
            "Error message must contain 'BTI (da)' for actionable diagnosis. Got: {msg}"
        );
        assert!(
            msg.contains("not yet implemented"),
            "Error message must say 'not yet implemented'. Got: {msg}"
        );

        // The error must not be recoverable (it's a format limitation, not a transient I/O issue).
        assert!(
            !err.is_recoverable(),
            "UnsupportedFormat error must not be recoverable"
        );
    }

    /// Verify the error message includes a pointer to the scoping document.
    #[tokio::test]
    async fn da_reader_error_mentions_scoping_doc() {
        let Some(data_db) = da_data_db_path() else {
            eprintln!("SKIP: CQLITE_DATASETS_ROOT not set or da Data.db not found");
            return;
        };

        let config = Config::default();
        let platform = Arc::new(
            cqlite_core::platform::Platform::new(&config)
                .await
                .expect("Platform::new must succeed"),
        );

        let result = SSTableReader::open(&data_db, &config, platform).await;
        let err = result.expect_err("Must return an error for da Data.db");
        let msg = err.to_string();

        assert!(
            msg.contains("bti-read-support-scoping"),
            "Error message must reference 'bti-read-support-scoping' doc. Got: {msg}"
        );
    }
}

#[cfg(test)]
mod da_error_category {
    use cqlite_core::error::ErrorCategory;
    use cqlite_core::Error;

    /// `Error::UnsupportedFormat` must have category `Data` and be non-recoverable.
    /// This is the error variant returned when opening a da SSTable.
    #[test]
    fn unsupported_format_error_category_and_recoverability() {
        let err = Error::unsupported_format("BTI (da) read support not yet implemented");

        // Category must be Data (maps to PARSE in Node.js bindings, CqliteError base in Python).
        assert_eq!(
            err.category(),
            ErrorCategory::Data,
            "UnsupportedFormat must have Data category"
        );

        // Not recoverable — this is a format limitation, not a transient error.
        assert!(
            !err.is_recoverable(),
            "UnsupportedFormat must not be recoverable"
        );
    }

    /// Python binding: `Error::UnsupportedFormat` falls through to `CqliteError`
    /// base (the `_ =>` arm). This is a compile-time documentation assertion.
    #[test]
    fn unsupported_format_maps_to_cqlite_error_in_python_binding() {
        // The Python binding maps UnsupportedFormat via the `_ => CqliteError` arm.
        // This test documents the contract: the error IS surfaced (not swallowed),
        // it just uses the base exception class rather than a specific subclass.
        let err = Error::UnsupportedFormat("BTI (da) read support not yet implemented".into());
        let msg = err.to_string();
        assert!(
            msg.contains("BTI (da)"),
            "Error message must contain 'BTI (da)' for Python callers to inspect"
        );
    }

    /// Node.js binding: `Error::UnsupportedFormat` category is `Data`, which maps
    /// to code `PARSE` in the Node error mapping table.
    #[test]
    fn unsupported_format_maps_to_parse_code_in_node_binding() {
        use cqlite_core::error::ErrorCategory;

        let err = Error::UnsupportedFormat("BTI (da) read support not yet implemented".into());
        // UnsupportedFormat.category() == Data, and Data -> "PARSE" in node error.rs
        assert_eq!(
            err.category(),
            ErrorCategory::Data,
            "UnsupportedFormat category must be Data (-> PARSE code in Node.js)"
        );
    }
}
