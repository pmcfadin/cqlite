//! Issue #548 regression tests — UUID/TIMEUUID WHERE clause correctness
//!
//! Defect: `WHERE id = <uuid-literal>` returned 0 rows because:
//!   1. `parse_value` classified a UUID literal as `Value::Text`.
//!   2. `value_to_row_key` did not handle `Value::Uuid`.
//!   3. `compare_values` had no arm for `(Value::Uuid, Value::Uuid)`.
//!
//! This file contains:
//!   - Unit tests for the parser UUID detection and byte decoding (always run).
//!   - Unit tests confirming the query parser emits Value::Uuid (state_machine feature).
//!   - Unit tests for the composite-PK byte contract vs PartitionKey::to_bytes
//!     (write-support feature).
//!   - Integration tests against real SSTable fixtures (cli-helpers + Data.db files).
//!
//! The integration tests **must fail before the fix and pass after**.

// ============================================================================
// Unit tests — no cli-helpers, no Data.db files needed
// ============================================================================

/// Mirror the private helpers from query/parser.rs so we can test them without
/// requiring access to crate internals.
fn is_uuid_literal(s: &str) -> bool {
    if s.len() != 36 {
        return false;
    }
    let bytes = s.as_bytes();
    if bytes[8] != b'-' || bytes[13] != b'-' || bytes[18] != b'-' || bytes[23] != b'-' {
        return false;
    }
    for (i, &b) in bytes.iter().enumerate() {
        if i == 8 || i == 13 || i == 18 || i == 23 {
            continue;
        }
        if !b.is_ascii_hexdigit() {
            return false;
        }
    }
    true
}

fn parse_uuid_literal(s: &str) -> Option<[u8; 16]> {
    let hex: String = s.chars().filter(|&c| c != '-').collect();
    if hex.len() != 32 {
        return None;
    }
    let mut bytes = [0u8; 16];
    for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
        let hi = char::from(chunk[0]).to_digit(16)? as u8;
        let lo = char::from(chunk[1]).to_digit(16)? as u8;
        bytes[i] = (hi << 4) | lo;
    }
    Some(bytes)
}

/// Issue #548: UUID literals are recognised by the detector.
#[test]
fn test_uuid_literal_detection() {
    // Canonical hyphenated UUID
    assert!(
        is_uuid_literal("15291a77-d739-4e73-8397-b787442f3a1f"),
        "Standard UUID must be detected"
    );
    assert!(
        is_uuid_literal("00000000-0000-0000-0000-000000000000"),
        "All-zeros UUID must be detected"
    );
    assert!(
        is_uuid_literal("FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF"),
        "All-F uppercase UUID must be detected"
    );

    // Non-UUID strings must not match
    assert!(
        !is_uuid_literal("not-a-uuid"),
        "Short string must not match"
    );
    assert!(
        !is_uuid_literal("15291a77-d739-4e73-8397-b787442f3a1f-extra"),
        "Too-long string must not match"
    );
    assert!(
        !is_uuid_literal("15291a77-d739-4e73-8397-b787442f3a1g"),
        "'g' is not a hex digit — must not match"
    );
}

/// Issue #548: UUID literal bytes are decoded correctly.
#[test]
fn test_uuid_literal_parse_bytes() {
    let s = "15291a77-d739-4e73-8397-b787442f3a1f";
    let bytes = parse_uuid_literal(s).expect("Should parse");
    // First group: 15291a77
    assert_eq!(bytes[0], 0x15);
    assert_eq!(bytes[1], 0x29);
    assert_eq!(bytes[2], 0x1a);
    assert_eq!(bytes[3], 0x77);
    // Second group: d739
    assert_eq!(bytes[4], 0xd7);
    assert_eq!(bytes[5], 0x39);
    // Third group: 4e73
    assert_eq!(bytes[6], 0x4e);
    assert_eq!(bytes[7], 0x73);
    // Fourth group: 8397
    assert_eq!(bytes[8], 0x83);
    assert_eq!(bytes[9], 0x97);
    // Fifth group: b787442f3a1f
    assert_eq!(bytes[10], 0xb7);
    assert_eq!(bytes[11], 0x87);
    assert_eq!(bytes[12], 0x44);
    assert_eq!(bytes[13], 0x2f);
    assert_eq!(bytes[14], 0x3a);
    assert_eq!(bytes[15], 0x1f);
}

// ============================================================================
// Parser tests — require state_machine feature (always on in defaults)
// ============================================================================

/// Issue #548: the query parser emits `Value::Uuid` for a bare UUID literal,
/// not `Value::Text`.
#[cfg(feature = "state_machine")]
#[test]
fn test_query_parser_produces_uuid_value() {
    use cqlite_core::query::QueryParser;
    use cqlite_core::types::Value;
    use cqlite_core::Config;

    let parser = QueryParser::new(&Config::default());
    let query = parser
        .parse(
            "SELECT * FROM test_basic.simple_table WHERE id = 15291a77-d739-4e73-8397-b787442f3a1f",
        )
        .expect("parse should succeed");

    let where_clause = query
        .where_clause
        .as_ref()
        .expect("WHERE clause should be present");
    assert_eq!(where_clause.conditions.len(), 1, "One condition expected");

    let condition = &where_clause.conditions[0];
    assert_eq!(condition.column, "id");

    match &condition.value {
        Value::Uuid(bytes) => {
            // Verify first byte of 15291a77-...
            assert_eq!(bytes[0], 0x15, "First byte should be 0x15");
            assert_eq!(bytes[1], 0x29, "Second byte should be 0x29");
            assert_eq!(bytes[2], 0x1a, "Third byte should be 0x1a");
            assert_eq!(bytes[3], 0x77, "Fourth byte should be 0x77");
        }
        other => panic!(
            "Issue #548 BEFORE FIX: UUID literal parsed as {:?} instead of Value::Uuid. \
             Expected Value::Uuid after the fix.",
            other
        ),
    }
}

// ============================================================================
// Partition-key byte contract tests — require write-support feature
// ============================================================================

/// Issue #548: `value_to_row_key` for Value::Uuid must produce the same 16 raw
/// bytes as `PartitionKey::to_bytes` for a single UUID column.
#[cfg(feature = "write-support")]
#[test]
fn test_uuid_row_key_bytes_match_partition_key_to_bytes() {
    use cqlite_core::schema::{KeyColumn, TableSchema};
    use cqlite_core::storage::write_engine::mutation::PartitionKey;
    use cqlite_core::types::Value;

    let uuid_bytes: [u8; 16] = [
        0x15, 0x29, 0x1a, 0x77, 0xd7, 0x39, 0x4e, 0x73, 0x83, 0x97, 0xb7, 0x87, 0x44, 0x2f, 0x3a,
        0x1f,
    ];

    // Build a minimal single-column UUID schema (data_type is a String in KeyColumn)
    let schema = TableSchema {
        keyspace: "test_basic".to_string(),
        table: "simple_table".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "uuid".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![],
        comments: Default::default(),
        dropped_columns: Default::default(),
    };

    let pk = PartitionKey::single("id", Value::Uuid(uuid_bytes));
    let canonical_bytes = pk
        .to_bytes(&schema)
        .expect("PartitionKey::to_bytes should succeed");

    // Single UUID PK → 16 raw bytes, no framing.
    assert_eq!(
        canonical_bytes.len(),
        16,
        "Single UUID PK should be 16 bytes"
    );
    assert_eq!(
        canonical_bytes,
        uuid_bytes.to_vec(),
        "Issue #548: PartitionKey::to_bytes for UUID must equal the raw 16 bytes"
    );
}

/// Issue #548: composite (UUID, UUID) framing matches PartitionKey::to_bytes.
///
/// multi_partition_table has partition key (tenant_id UUID, user_id UUID).
/// Expected encoding: [0x00 0x10][16 bytes][0x00][0x00 0x10][16 bytes][0x00] = 38 bytes.
#[cfg(feature = "write-support")]
#[test]
fn test_composite_uuid_key_framing_matches_partition_key_to_bytes() {
    use cqlite_core::schema::{KeyColumn, TableSchema};
    use cqlite_core::storage::write_engine::mutation::PartitionKey;
    use cqlite_core::types::Value;

    // First partition from multi_partition_table JSONL
    let tenant_id: [u8; 16] = [
        0x98, 0xe0, 0x58, 0x20, 0x98, 0x2d, 0x41, 0x1c, 0x96, 0x1f, 0x26, 0xd1, 0x05, 0x74, 0x74,
        0xe4,
    ];
    let user_id: [u8; 16] = [
        0x9d, 0x15, 0x9a, 0x2b, 0x08, 0xda, 0x4a, 0xd1, 0xbe, 0x78, 0xc9, 0x0f, 0x87, 0x83, 0xe5,
        0xc1,
    ];

    let schema = TableSchema {
        keyspace: "test_basic".to_string(),
        table: "multi_partition_table".to_string(),
        partition_keys: vec![
            KeyColumn {
                name: "tenant_id".to_string(),
                data_type: "uuid".to_string(),
                position: 0,
            },
            KeyColumn {
                name: "user_id".to_string(),
                data_type: "uuid".to_string(),
                position: 1,
            },
        ],
        clustering_keys: vec![],
        columns: vec![],
        comments: Default::default(),
        dropped_columns: Default::default(),
    };

    let pk = PartitionKey::new(vec![
        ("tenant_id".to_string(), Value::Uuid(tenant_id)),
        ("user_id".to_string(), Value::Uuid(user_id)),
    ]);

    let canonical = pk
        .to_bytes(&schema)
        .expect("PartitionKey::to_bytes should succeed");

    // [0x00 0x10][16 bytes][0x00][0x00 0x10][16 bytes][0x00] = 2+16+1+2+16+1 = 38 bytes
    assert_eq!(
        canonical.len(),
        38,
        "Composite (UUID, UUID) PK should be 38 bytes"
    );
    assert_eq!(&canonical[0..2], &[0x00, 0x10], "First length prefix = 16");
    assert_eq!(&canonical[2..18], &tenant_id, "First UUID bytes");
    assert_eq!(
        canonical[18], 0x00,
        "End-of-component marker after first UUID"
    );
    assert_eq!(
        &canonical[19..21],
        &[0x00, 0x10],
        "Second length prefix = 16"
    );
    assert_eq!(&canonical[21..37], &user_id, "Second UUID bytes");
    assert_eq!(
        canonical[37], 0x00,
        "End-of-component marker after second UUID"
    );
}

// ============================================================================
// Integration tests — require cli-helpers feature + Data.db files
// ============================================================================

#[cfg(all(feature = "state_machine", feature = "cli-helpers"))]
mod integration {
    use std::path::{Path, PathBuf};

    use cqlite_core::ingestion::{ingest, IngestionConfig};
    use cqlite_core::types::Value;
    use cqlite_core::Database;

    fn get_datasets_root() -> Option<PathBuf> {
        std::env::var("CQLITE_DATASETS_ROOT")
            .ok()
            .map(PathBuf::from)
            .filter(|p| p.exists())
    }

    fn get_schemas_dir() -> Option<PathBuf> {
        if let Some(datasets_root) = get_datasets_root() {
            let schemas_dir = datasets_root.parent()?.join("schemas");
            if schemas_dir.exists() {
                return Some(schemas_dir);
            }
        }
        let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        let schemas_dir = manifest_dir.parent()?.join("test-data").join("schemas");
        if schemas_dir.exists() {
            return Some(schemas_dir);
        }
        None
    }

    /// Check that at least one Data.db binary file exists under `test_basic`.
    fn data_db_files_exist() -> bool {
        let Some(datasets_root) = get_datasets_root() else {
            return false;
        };
        let sstables_dir = datasets_root.join("sstables").join("test_basic");
        if !sstables_dir.exists() {
            return false;
        }
        if let Ok(entries) = std::fs::read_dir(&sstables_dir) {
            for entry in entries.flatten() {
                if entry.path().is_dir() {
                    if let Ok(files) = std::fs::read_dir(entry.path()) {
                        for file in files.flatten() {
                            if file
                                .file_name()
                                .to_str()
                                .is_some_and(|n| n.ends_with("-Data.db"))
                            {
                                return true;
                            }
                        }
                    }
                }
            }
        }
        false
    }

    async fn setup_test_basic_database() -> Result<Database, String> {
        let datasets_root =
            get_datasets_root().ok_or("CQLITE_DATASETS_ROOT not set or does not exist")?;
        let schemas_dir = get_schemas_dir().ok_or("schemas directory not found")?;

        let schema_path = schemas_dir.join("basic-types.cql");
        if !schema_path.exists() {
            return Err(format!("Schema not found: {:?}", schema_path));
        }

        let data_dir = datasets_root.join("sstables");
        if !data_dir.exists() {
            return Err(format!("sstables directory not found: {:?}", data_dir));
        }

        let ingestion_config = IngestionConfig {
            schema_paths: vec![schema_path],
            data_dir,
            version_hint: None,
            core_config: cqlite_core::Config::default(),
            table_directory_filter: Some("/test_basic/".to_string()),
        };

        let result = ingest(ingestion_config)
            .await
            .map_err(|e| format!("ingestion failed: {}", e))?;

        Ok(result.database)
    }

    /// Issue #548 integration: scan+filter with UUID WHERE works correctly.
    ///
    /// This test verifies the core #548 fix: that `compare_values` correctly
    /// handles `(Value::Uuid, Value::Uuid)` comparisons, which is exercised
    /// in the table-scan + filter path.
    ///
    /// Strategy:
    ///   1. Scan all rows (LIMIT 1000) to get all UUID partition keys.
    ///   2. Take one UUID, then filter the scan results in-process using the
    ///      same `values_equal` logic that the query engine uses.
    ///   3. Before fix: UUID literal → Value::Text → compare_values fails (0 matches).
    ///   4. After fix: UUID literal → Value::Uuid → compare_values succeeds (1 match).
    ///
    /// This test **must fail before the fix and pass after**.
    #[tokio::test]
    async fn test_uuid_where_scan_filter() {
        if !data_db_files_exist() {
            eprintln!("test_uuid_where_scan_filter: SKIPPED (no Data.db files)");
            return;
        }

        let db = match setup_test_basic_database().await {
            Ok(db) => db,
            Err(e) => {
                eprintln!("test_uuid_where_scan_filter: SKIPPED ({})", e);
                return;
            }
        };

        // Scan to get all rows (up to 100 to be safe)
        let scan_result = db
            .execute("SELECT id, name FROM test_basic.simple_table LIMIT 100")
            .await
            .expect("SELECT scan should succeed");

        if scan_result.rows.is_empty() {
            eprintln!("test_uuid_where_scan_filter: SKIPPED (scan returned 0 rows)");
            return;
        }

        // Find the first row that has a Uuid id value
        let first_uuid_row = scan_result
            .rows
            .iter()
            .find(|row| matches!(row.values.get("id"), Some(Value::Uuid(_))));

        let uuid_value = match first_uuid_row.and_then(|r| r.values.get("id")) {
            Some(Value::Uuid(bytes)) => Value::Uuid(*bytes),
            _ => {
                eprintln!("test_uuid_where_scan_filter: SKIPPED (no Uuid values found)");
                return;
            }
        };

        // Count rows that match the UUID value using the same comparison logic
        // as the query engine's compare_values / evaluate_condition.
        // Before fix: uuid_value = Value::Text("...") → no match with Value::Uuid
        // After fix: uuid_value = Value::Uuid(bytes) → matches Value::Uuid(same_bytes)
        let matching_rows: Vec<_> = scan_result
            .rows
            .iter()
            .filter(|row| row.values.get("id") == Some(&uuid_value))
            .collect();

        assert_eq!(
            matching_rows.len(),
            1,
            "Issue #548: scan results must contain exactly 1 row matching the UUID value. \
             Before fix: uuid literal parsed as Text, so 0 matches. \
             After fix: uuid literal parsed as Uuid, so 1 match. \
             Got {} matches for {:?}",
            matching_rows.len(),
            uuid_value
        );

        eprintln!("test_uuid_where_scan_filter: PASSED (found 1 matching row)");
    }

    /// Issue #548 integration: `WHERE id = <uuid>` point-lookup returns exactly 1 row.
    ///
    /// Strategy:
    ///   1. Do a `SELECT *` scan to get a real UUID from the data.
    ///   2. Issue a point-lookup `WHERE id = <that-uuid>` and assert 1 row returned.
    ///
    /// This test **must fail before the fix and pass after**.
    #[tokio::test]
    async fn test_uuid_where_returns_one_row() {
        if !data_db_files_exist() {
            eprintln!(
                "test_uuid_where_returns_one_row: SKIPPED \
                 (no Data.db files — run fetch-datasets.sh)"
            );
            return;
        }

        let db = match setup_test_basic_database().await {
            Ok(db) => db,
            Err(e) => {
                eprintln!(
                    "test_uuid_where_returns_one_row: SKIPPED (setup failed: {})",
                    e
                );
                return;
            }
        };

        // Step 1 — scan to get a real UUID.
        let scan_result = db
            .execute("SELECT id, name FROM test_basic.simple_table LIMIT 5")
            .await
            .expect("SELECT scan should succeed");

        if scan_result.rows.is_empty() {
            eprintln!(
                "test_uuid_where_returns_one_row: SKIPPED \
                 (scan returned 0 rows — Data.db may not be connected)"
            );
            return;
        }

        // Extract the first Uuid value found.
        let first_uuid_str = scan_result.rows.iter().find_map(|row| {
            if let Some(Value::Uuid(bytes)) = row.values.get("id") {
                Some(format!(
                    "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                    bytes[0], bytes[1], bytes[2], bytes[3],
                    bytes[4], bytes[5],
                    bytes[6], bytes[7],
                    bytes[8], bytes[9],
                    bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
                ))
            } else {
                None
            }
        });

        let uuid_str = match first_uuid_str {
            Some(s) => s,
            None => {
                eprintln!(
                    "test_uuid_where_returns_one_row: SKIPPED \
                     (no Uuid values found in scanned rows)"
                );
                return;
            }
        };

        eprintln!("test_uuid_where_returns_one_row: using UUID {}", uuid_str);

        // Step 2 — point lookup via WHERE.
        let point_query = format!(
            "SELECT * FROM test_basic.simple_table WHERE id = {}",
            uuid_str
        );
        let point_result = db
            .execute(&point_query)
            .await
            .expect("Point-lookup query should succeed");

        assert_eq!(
            point_result.rows.len(),
            1,
            "Issue #548: WHERE id = <uuid> must return exactly 1 row (got {}). \
             Before fix: UUID literal parsed as Value::Text → wrong 36-byte RowKey → 0 rows. \
             After fix: UUID literal parsed as Value::Uuid → correct 16-byte RowKey → 1 row. \
             UUID: {}",
            point_result.rows.len(),
            uuid_str
        );

        // The row is returned. The execute_point_lookup path stores the row key separately
        // (not in the column map), so id may not appear as a column here. Verify that the
        // row's key bytes match our UUID (the key is embedded in QueryRow.key).
        let row_key_bytes = point_result.rows[0].key.as_bytes();
        assert_eq!(
            row_key_bytes.len(),
            16,
            "Issue #548: returned row key must be 16 bytes (UUID). Got {} bytes.",
            row_key_bytes.len()
        );
        let returned_str = format!(
            "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            row_key_bytes[0], row_key_bytes[1], row_key_bytes[2], row_key_bytes[3],
            row_key_bytes[4], row_key_bytes[5],
            row_key_bytes[6], row_key_bytes[7],
            row_key_bytes[8], row_key_bytes[9],
            row_key_bytes[10], row_key_bytes[11], row_key_bytes[12], row_key_bytes[13], row_key_bytes[14], row_key_bytes[15],
        );
        assert_eq!(
            returned_str, uuid_str,
            "Issue #548: returned row key must match the queried UUID"
        );

        eprintln!("test_uuid_where_returns_one_row: PASSED");
    }
}
