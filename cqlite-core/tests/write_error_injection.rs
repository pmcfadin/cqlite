//! Error injection tests for the write engine (#460)
//!
//! Tests WAL corruption recovery scenarios and engine lifecycle error handling.
//! Validates that the WAL replay correctly handles:
//! - Truncated entries (EOF mid-read)
//! - Corrupted CRC fields
//! - Corrupted mutation bytes
//! - Completely corrupted WAL data
//! - Empty WAL files

#![cfg(feature = "write-support")]

use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use std::collections::HashMap;
use tempfile::TempDir;

// ───────────────────────────── helpers ──────────────────────────────────────

fn create_test_schema() -> TableSchema {
    TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "id".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "name".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

fn create_mutation(id: i32, name: &str, timestamp: i64) -> Mutation {
    let table_id = TableId::new("test_ks", "test_table");
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ops = vec![CellOperation::Write {
        column: "name".to_string(),
        value: Value::Text(name.to_string()),
    }];
    Mutation::new(table_id, pk, None, ops, timestamp, None)
}

/// Parse entry boundaries from WAL bytes.
///
/// Returns a Vec of `(header_offset, entry_length)` for each entry found.
/// The header is 8 bytes: `[length: u32 LE][crc32: u32 LE]`.
fn parse_wal_entries(bytes: &[u8]) -> Vec<(usize, u32)> {
    let mut entries = Vec::new();
    let mut offset = 0usize;

    while offset + 8 <= bytes.len() {
        let entry_length = u32::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]);

        // Sanity check: stop on unreasonable lengths
        if entry_length > 16 * 1024 * 1024 {
            break;
        }

        entries.push((offset, entry_length));
        offset += 8 + entry_length as usize;
    }

    entries
}

// ───────────────────────── WAL corruption tests ─────────────────────────────

/// Truncate the last 10 bytes — the final entry becomes incomplete.
/// Replay should stop at the truncated entry and return the 2 intact entries.
#[tokio::test]
async fn test_wal_truncated_mid_entry() -> cqlite_core::error::Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_test_schema();
    let wal_path = temp_dir.path().join("wal").join("commitlog.wal");

    // Session 1: write 3 mutations and crash (drop without flush)
    {
        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema.clone(),
        );
        let mut engine = WriteEngine::new(config)?;
        for i in 0..3 {
            engine
                .write_async(create_mutation(
                    i,
                    &format!("User{}", i),
                    1_000_000 + i as i64,
                ))
                .await?;
        }
        assert_eq!(engine.memtable_row_count(), 3);
        // engine is dropped here without flushing — WAL is preserved
    }

    // Corrupt: truncate last 10 bytes (cuts into the third entry's payload)
    let mut wal_bytes = std::fs::read(&wal_path).unwrap();
    assert!(wal_bytes.len() > 10, "WAL unexpectedly small");
    let new_len = wal_bytes.len() - 10;
    wal_bytes.truncate(new_len);
    std::fs::write(&wal_path, &wal_bytes).unwrap();

    // Session 2: reopen and replay
    {
        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );
        let engine = WriteEngine::new(config)?;
        // Third entry was truncated — only 2 should be recovered
        assert_eq!(
            engine.memtable_row_count(),
            2,
            "expected 2 recovered entries after truncation"
        );
    }

    Ok(())
}

/// Flip one byte of the CRC field of the second entry.
/// Replay should skip entry #2 (CRC mismatch) but recover entries #1 and #3.
#[tokio::test]
async fn test_wal_corrupted_crc_skips_entry() -> cqlite_core::error::Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_test_schema();
    let wal_path = temp_dir.path().join("wal").join("commitlog.wal");

    // Session 1: write 3 mutations and crash
    {
        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema.clone(),
        );
        let mut engine = WriteEngine::new(config)?;
        for i in 0..3 {
            engine
                .write_async(create_mutation(
                    i,
                    &format!("User{}", i),
                    1_000_000 + i as i64,
                ))
                .await?;
        }
        assert_eq!(engine.memtable_row_count(), 3);
    }

    // Corrupt: flip the first byte of entry #2's CRC field (offset = header_offset + 4)
    let mut wal_bytes = std::fs::read(&wal_path).unwrap();
    let entries = parse_wal_entries(&wal_bytes);
    assert_eq!(entries.len(), 3, "expected 3 WAL entries before corruption");

    let (entry2_offset, _) = entries[1];
    let crc_offset = entry2_offset + 4; // length field is 4 bytes; CRC starts at offset+4
    wal_bytes[crc_offset] ^= 0xFF; // flip all bits in first CRC byte
    std::fs::write(&wal_path, &wal_bytes).unwrap();

    // Session 2: replay
    {
        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );
        let engine = WriteEngine::new(config)?;
        // Entry #2 skipped due to CRC mismatch; entries #1 and #3 recovered
        assert_eq!(
            engine.memtable_row_count(),
            2,
            "expected 2 recovered entries after CRC corruption of entry #2"
        );
    }

    Ok(())
}

/// Flip one byte inside the mutation payload of entry #2.
/// The CRC will not match, so the entry must be skipped.
#[tokio::test]
async fn test_wal_corrupted_mutation_bytes() -> cqlite_core::error::Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_test_schema();
    let wal_path = temp_dir.path().join("wal").join("commitlog.wal");

    // Session 1: write 3 mutations and crash
    {
        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema.clone(),
        );
        let mut engine = WriteEngine::new(config)?;
        for i in 0..3 {
            engine
                .write_async(create_mutation(
                    i,
                    &format!("User{}", i),
                    1_000_000 + i as i64,
                ))
                .await?;
        }
        assert_eq!(engine.memtable_row_count(), 3);
    }

    // Corrupt: flip a byte in entry #2's payload (after the 8-byte header)
    let mut wal_bytes = std::fs::read(&wal_path).unwrap();
    let entries = parse_wal_entries(&wal_bytes);
    assert_eq!(entries.len(), 3, "expected 3 WAL entries before corruption");

    let (entry2_offset, entry2_len) = entries[1];
    assert!(entry2_len > 0, "entry #2 payload is empty");
    let payload_byte_offset = entry2_offset + 8; // skip 8-byte header
    wal_bytes[payload_byte_offset] ^= 0xFF;
    std::fs::write(&wal_path, &wal_bytes).unwrap();

    // Session 2: replay
    {
        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );
        let engine = WriteEngine::new(config)?;
        // CRC over mutated payload will mismatch — entry #2 skipped
        assert_eq!(
            engine.memtable_row_count(),
            2,
            "expected 2 recovered entries after payload corruption of entry #2"
        );
    }

    Ok(())
}

/// Overwrite the WAL with random garbage bytes.
/// Replay should recover 0 entries.
#[tokio::test]
async fn test_wal_completely_corrupted() -> cqlite_core::error::Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_test_schema();
    let wal_path = temp_dir.path().join("wal").join("commitlog.wal");

    // Session 1: write 3 mutations and crash
    {
        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema.clone(),
        );
        let mut engine = WriteEngine::new(config)?;
        for i in 0..3 {
            engine
                .write_async(create_mutation(
                    i,
                    &format!("User{}", i),
                    1_000_000 + i as i64,
                ))
                .await?;
        }
    }

    // Overwrite with garbage
    std::fs::write(&wal_path, vec![0xAA_u8; 100]).unwrap();

    // Session 2: replay
    {
        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );
        let engine = WriteEngine::new(config)?;
        assert_eq!(
            engine.memtable_row_count(),
            0,
            "expected 0 recovered entries from completely corrupted WAL"
        );
    }

    Ok(())
}

/// Truncate the WAL to 0 bytes (empty file).
/// Replay should recover 0 entries.
#[tokio::test]
async fn test_wal_empty_file() -> cqlite_core::error::Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_test_schema();
    let wal_path = temp_dir.path().join("wal").join("commitlog.wal");

    // Session 1: write 3 mutations and crash
    {
        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema.clone(),
        );
        let mut engine = WriteEngine::new(config)?;
        for i in 0..3 {
            engine
                .write_async(create_mutation(
                    i,
                    &format!("User{}", i),
                    1_000_000 + i as i64,
                ))
                .await?;
        }
    }

    // Truncate WAL to 0 bytes
    std::fs::write(&wal_path, b"").unwrap();

    // Session 2: replay
    {
        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );
        let engine = WriteEngine::new(config)?;
        assert_eq!(
            engine.memtable_row_count(),
            0,
            "expected 0 recovered entries from empty WAL"
        );
    }

    Ok(())
}

/// Write exactly 4 bytes to the WAL (incomplete 8-byte header).
/// Replay should stop immediately and recover 0 entries.
#[tokio::test]
async fn test_wal_truncated_header_only() -> cqlite_core::error::Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_test_schema();
    let wal_path = temp_dir.path().join("wal").join("commitlog.wal");

    // Session 1: write 3 mutations and crash
    {
        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema.clone(),
        );
        let mut engine = WriteEngine::new(config)?;
        for i in 0..3 {
            engine
                .write_async(create_mutation(
                    i,
                    &format!("User{}", i),
                    1_000_000 + i as i64,
                ))
                .await?;
        }
    }

    // Write only 4 bytes (half a header)
    std::fs::write(&wal_path, [0x10_u8, 0x00, 0x00, 0x00]).unwrap();

    // Session 2: replay
    {
        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );
        let engine = WriteEngine::new(config)?;
        assert_eq!(
            engine.memtable_row_count(),
            0,
            "expected 0 recovered entries from incomplete header WAL"
        );
    }

    Ok(())
}

// ────────────────────────── lifecycle tests ─────────────────────────────────

/// Flushing an empty memtable should succeed and return None.
#[tokio::test]
async fn test_flush_empty_memtable() -> cqlite_core::error::Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_test_schema();
    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema,
    );

    let mut engine = WriteEngine::new(config)?;
    let result = engine.flush().await?;
    assert!(
        result.is_none(),
        "flush of empty memtable should return None"
    );

    Ok(())
}

/// After calling close(), write_async() must return an error.
#[tokio::test]
async fn test_write_after_close() -> cqlite_core::error::Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_test_schema();
    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema,
    );

    let mut engine = WriteEngine::new(config)?;
    engine.close().await?;

    // Writing to a closed engine should fail
    let mutation = create_mutation(1, "Alice", 1_000_000);
    let result = engine.write_async(mutation).await;
    assert!(result.is_err(), "write after close should return an error");

    Ok(())
}

/// On Unix, make the data directory read-only; flush should fail because the
/// SSTable writer cannot create new files there.
#[cfg(unix)]
#[tokio::test]
async fn test_readonly_data_dir_flush_fails() -> cqlite_core::error::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let temp_dir = TempDir::new().unwrap();
    let schema = create_test_schema();
    let data_dir = temp_dir.path().join("data");
    let wal_dir = temp_dir.path().join("wal");

    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, schema);
    let mut engine = WriteEngine::new(config)?;

    // Write one mutation so the memtable is non-empty
    engine
        .write_async(create_mutation(1, "Alice", 1_000_000))
        .await?;
    assert_eq!(engine.memtable_row_count(), 1);

    // Make the SSTable subdirectory inside data_dir read-only.
    // The writer will try to create files under data_dir/test_ks/test_table/.
    // Making data_dir itself read-only is sufficient to block directory creation.
    std::fs::set_permissions(&data_dir, std::fs::Permissions::from_mode(0o444)).unwrap();

    // Privileged users (e.g. uid 0 in containerized CI) bypass directory
    // permissions, so the read-only precondition cannot be created.
    let probe = data_dir.join(".permission-probe");
    if std::fs::create_dir(&probe).is_ok() {
        let _ = std::fs::remove_dir(&probe);
        std::fs::set_permissions(&data_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        return Ok(());
    }

    let flush_result = engine.flush().await;

    // Restore permissions so TempDir cleanup can succeed
    std::fs::set_permissions(&data_dir, std::fs::Permissions::from_mode(0o755)).unwrap();

    assert!(
        flush_result.is_err(),
        "flush to a read-only data directory should fail"
    );

    Ok(())
}

/// On Unix, make the WAL directory read-only after engine creation; a
/// subsequent write_async should fail when the WAL tries to sync.
///
/// Note: The WAL file is already open when we make the directory read-only,
/// so the write to the buffered file itself succeeds. However, fsync on
/// a file inside a read-only directory will fail on most systems.
/// If the OS allows fsync despite directory permissions, we verify that
/// at minimum no data corruption occurs by asserting the call either
/// succeeds silently or returns an error.
#[cfg(unix)]
#[tokio::test]
async fn test_readonly_wal_dir_write_fails() -> cqlite_core::error::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let temp_dir = TempDir::new().unwrap();
    let schema = create_test_schema();
    let wal_dir = temp_dir.path().join("wal");

    let config = WriteEngineConfig::new(temp_dir.path().join("data"), wal_dir.clone(), schema);
    let mut engine = WriteEngine::new(config)?;

    // Make the WAL directory read-only to block future WAL file operations
    std::fs::set_permissions(&wal_dir, std::fs::Permissions::from_mode(0o444)).unwrap();

    let mutation = create_mutation(1, "Alice", 1_000_000);
    let write_result = engine.write_async(mutation).await;

    // Restore permissions so TempDir cleanup can succeed
    std::fs::set_permissions(&wal_dir, std::fs::Permissions::from_mode(0o755)).unwrap();

    // The write may fail (WAL sync blocked) or succeed (OS allows fsync on
    // an already-open file handle). Either outcome is acceptable — the key
    // invariant is no panic and no data corruption.
    match write_result {
        Ok(_) => {
            // OS allowed the write despite read-only dir — engine is still usable
            assert!(engine.memtable_row_count() > 0);
        }
        Err(e) => {
            // Expected: WAL sync failed due to permissions
            let msg = format!("{}", e);
            assert!(
                msg.contains("WAL")
                    || msg.contains("sync")
                    || msg.contains("permission")
                    || msg.contains("Permission")
                    || msg.contains("Storage"),
                "Unexpected error message: {}",
                msg
            );
        }
    }

    Ok(())
}
