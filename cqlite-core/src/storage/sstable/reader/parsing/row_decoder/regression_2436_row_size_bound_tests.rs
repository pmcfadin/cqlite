//! Issue #2436 negative pin (roborev job 1729, Low but load-bearing).
//!
//! The `row_size > available` structural guard added to
//! `parse_row_data_with_offset_impl` (`row_data.rs`) is now the ONLY protection
//! between a corrupt `row_size` VInt and the `after_row_offset` slice math. This
//! pins that a buffer whose `row_size` claims MORE bytes than remain fails
//! CLOSED — a typed [`Error::corruption`], never a panic, never a mis-parse, and
//! critically never `Ok` with a truncated/garbage row at the parser level.

use super::V5CompressedLegacyParser as V5;
use crate::schema::{Column, KeyColumn, TableSchema};
use crate::storage::sstable::writer::{SSTableFormat, SSTableWriter};
use crate::storage::write_engine::mutation::{CellOperation, Mutation, PartitionKey, TableId};
use crate::types::Value;

/// `t(id int, name text)`, no clustering keys — the row_size VInt for a tiny
/// `name` value fits in ONE byte (values 0-127), so corrupting it in place (no
/// length change) keeps every other offset in the buffer untouched.
fn schema() -> TableSchema {
    TableSchema {
        keyspace: "ks".to_string(),
        table: "t".to_string(),
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
        comments: std::collections::HashMap::new(),
        dropped_columns: std::collections::HashMap::new(),
    }
}

/// A corrupt `row_size` VInt claiming more bytes than remain in the buffer must
/// make [`V5CompressedLegacyParser::parse_row_data_with_offset`] return `Err`,
/// never panic and never `Ok` with a shortened/garbage row.
#[tokio::test]
async fn corrupt_row_size_exceeding_available_bytes_fails_closed() {
    let schema = schema();
    let dir = tempfile::TempDir::new().unwrap();
    let mut writer =
        SSTableWriter::with_format(dir.path().to_path_buf(), 1, &schema, 16, SSTableFormat::Big)
            .unwrap();

    let m = Mutation::new(
        TableId::new("ks", "t"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text("hi".to_string()),
        }],
        1_000_000,
        None,
    );
    let key = m.decorated_key(&schema).unwrap();
    writer.write_partition(key, vec![m]).unwrap();
    let info = writer.finish().await.unwrap();

    let config = crate::Config::default();
    let platform = std::sync::Arc::new(crate::platform::Platform::new(&config).await.unwrap());
    let reader =
        crate::storage::sstable::reader::SSTableReader::open(&info.data_path, &config, platform)
            .await
            .unwrap();
    let cursor = reader.new_scan_cursor().await.unwrap();
    let mut buf = reader.stitch_all_chunks(&cursor).await.unwrap();

    let parser = reader.build_v5_parser(false);
    let resolution = super::RowColumnResolution::build(&schema, &reader);

    // Walk to the row_size VInt exactly as the real decoder does: partition
    // header -> row flags -> clustering prefix (no-op: no clustering keys).
    let (_pk, offset, _del) = parser.parse_partition_header_full(&buf, 0).unwrap();
    let (_row_flags, _ext, flags_size) = parser.parse_row_flags(&buf, offset).unwrap();
    let (_clustering, row_metadata_offset) = parser
        .parse_clustering_prefix(&buf, offset + flags_size, &schema)
        .unwrap();

    // Sanity: for this tiny payload the row_size VInt is a single byte (top bit
    // clear, value 0-127) — corrupting it in place changes no other offset.
    let vint_byte = buf[row_metadata_offset];
    assert!(
        vint_byte < 0x80,
        "precondition: row_size VInt must be single-byte for this tiny fixture \
         (byte=0x{vint_byte:02x}) — otherwise the in-place corruption below would \
         shift downstream offsets"
    );
    let available = buf.len() - (row_metadata_offset + 1);
    assert!(
        available < 0x7F,
        "precondition: remaining bytes ({available}) must be less than the max \
         single-byte VInt value (127) for the corruption to exceed `available`"
    );

    // Corrupt: claim row_size = 127 (the max single-byte VInt value), which
    // exceeds the actual remaining bytes in this tiny fixture.
    buf[row_metadata_offset] = 0x7F;

    let result = parser.parse_row_data_with_offset(
        &buf,
        offset,
        Some(&schema),
        &reader,
        false,
        &resolution,
        None,
    );

    match result {
        Err(crate::Error::Corruption(msg)) => {
            assert!(
                msg.contains("row_size") && msg.contains("exceeds available data"),
                "corruption error must name the row_size/available-bytes mismatch, got: {msg}"
            );
        }
        Err(other) => panic!(
            "expected a typed Error::Corruption for a row_size exceeding available bytes, \
             got a different Err variant: {other:?}"
        ),
        Ok(_) => panic!(
            "a corrupt row_size VInt claiming more bytes than remain must fail CLOSED \
             (Err), never Ok — a silent truncated/garbage-row parse at the parser level \
             is exactly the #2436 failure mode this bound exists to prevent"
        ),
    }
}

/// Sibling sanity: the SAME buffer, uncorrupted, parses successfully — proving
/// the negative test above fails specifically because of the corruption, not
/// because of an unrelated harness bug (anti-empty-pass).
#[tokio::test]
async fn uncorrupted_row_parses_successfully_as_control() {
    let schema = schema();
    let dir = tempfile::TempDir::new().unwrap();
    let mut writer =
        SSTableWriter::with_format(dir.path().to_path_buf(), 1, &schema, 16, SSTableFormat::Big)
            .unwrap();

    let m = Mutation::new(
        TableId::new("ks", "t"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text("hi".to_string()),
        }],
        1_000_000,
        None,
    );
    let key = m.decorated_key(&schema).unwrap();
    writer.write_partition(key, vec![m]).unwrap();
    let info = writer.finish().await.unwrap();

    let config = crate::Config::default();
    let platform = std::sync::Arc::new(crate::platform::Platform::new(&config).await.unwrap());
    let reader =
        crate::storage::sstable::reader::SSTableReader::open(&info.data_path, &config, platform)
            .await
            .unwrap();
    let cursor = reader.new_scan_cursor().await.unwrap();
    let buf = reader.stitch_all_chunks(&cursor).await.unwrap();

    let parser: V5 = reader.build_v5_parser(false);
    let resolution = super::RowColumnResolution::build(&schema, &reader);
    let (_pk, offset, _del) = parser.parse_partition_header_full(&buf, 0).unwrap();

    let result = parser.parse_row_data_with_offset(
        &buf,
        offset,
        Some(&schema),
        &reader,
        false,
        &resolution,
        None,
    );
    assert!(
        result.is_ok(),
        "control: the uncorrupted buffer must parse successfully, got {result:?}"
    );
}
