//! Issue #1663: `SSTableWriter::finish()` must compute `Digest.crc32` and
//! `CRC.db` from checksums accumulated during the streaming write — NOT by
//! re-reading the finished `Data.db`.
//!
//! WORK GUARD: this test drives a full non-empty flush through the public
//! `SSTableWriter::finish()` and asserts the process-global
//! `data_db_checksum_full_reads` work counter stays at 0. On the pre-#1663 path
//! `finish()` re-read the whole `Data.db` twice (once for `Digest.crc32`, once
//! for `CRC.db`), so the counter would read 2. Accumulating both checksums as
//! the data streams makes finalization perform ZERO extra full-file reads.
//!
//! This lives in its own integration test binary so the global counter it
//! asserts on has a FRESH per-process instance — no in-crate `crc_writer` /
//! reader test (which exercise the re-read oracle and DO bump the counter) can
//! race it. Byte-parity of the incrementally-produced checksums is covered by
//! the in-crate `crc_writer` golden tests; here we additionally reconcile the
//! written components against the on-disk `Data.db` as a sanity check.

#![cfg(feature = "write-support")]

use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::work_counters;
use cqlite_core::storage::sstable::writer::{SSTableFormat, SSTableWriter};
use cqlite_core::storage::write_engine::mutation::{
    CellOperation, DecoratedKey, Mutation, PartitionKey, TableId,
};
use cqlite_core::types::Value;
use std::collections::HashMap;
use std::path::PathBuf;
use tempfile::TempDir;

const KEYSPACE: &str = "test_incr_checksum";
const TABLE: &str = "rows";
const T_WRITE: i64 = 1_700_000_000_000_000;

/// Cassandra's default uncompressed CRC chunk size.
const CRC_CHUNK_SIZE: usize = 64 * 1024;

fn schema() -> TableSchema {
    TableSchema {
        keyspace: KEYSPACE.into(),
        table: TABLE.into(),
        partition_keys: vec![KeyColumn {
            name: "id".into(),
            data_type: "int".into(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "id".into(),
                data_type: "int".into(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "name".into(),
                data_type: "text".into(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

fn mutations(schema: &TableSchema) -> Vec<(DecoratedKey, Vec<Mutation>)> {
    let mut keyed: Vec<(DecoratedKey, Mutation)> = (0..12)
        .map(|i| {
            let m = Mutation::new(
                TableId::new(KEYSPACE, TABLE),
                PartitionKey::single("id", Value::Integer(i)),
                None,
                vec![CellOperation::Write {
                    column: "name".into(),
                    value: Value::Text(format!("name{i}")),
                }],
                T_WRITE,
                None,
            );
            (m.decorated_key(schema).expect("decorated key"), m)
        })
        .collect();
    keyed.sort_by_key(|(k, _)| k.token);

    let mut grouped: Vec<(DecoratedKey, Vec<Mutation>)> = Vec::new();
    for (k, m) in keyed {
        match grouped.last_mut() {
            Some((lk, ms)) if lk.token == k.token && lk.key == k.key => ms.push(m),
            _ => grouped.push((k, vec![m])),
        }
    }
    grouped
}

async fn flush() -> (TempDir, PathBuf) {
    let dir = TempDir::new().expect("tempdir");
    let s = schema();
    let mut writer =
        SSTableWriter::with_format(dir.path().to_path_buf(), 1, &s, 16, SSTableFormat::Big)
            .expect("writer");
    for (k, ms) in mutations(&s) {
        writer.write_partition(k, ms).expect("write_partition");
    }
    let info = writer.finish().await.expect("finish");
    let table_dir = info.data_path.parent().expect("data parent").to_path_buf();
    (dir, table_dir)
}

/// The whole-file CRC32 of a raw `Data.db` payload (the `Digest.crc32` value).
fn crc32(bytes: &[u8]) -> u32 {
    let mut h = crc32fast::Hasher::new();
    h.update(bytes);
    h.finalize()
}

/// Expected `CRC.db` for a raw `Data.db` payload on the flush path (no trailer):
/// big-endian i32 chunk-size header + one big-endian u32 CRC32 per chunk.
fn expected_flush_crc_db(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(CRC_CHUNK_SIZE as i32).to_be_bytes());
    for chunk in data.chunks(CRC_CHUNK_SIZE) {
        out.extend_from_slice(&crc32(chunk).to_be_bytes());
    }
    out
}

/// WORK GUARD (issue #1663): a non-empty `finish()` performs ZERO full re-reads
/// of `Data.db` for checksums. On the pre-#1663 double-re-read path this counter
/// would read 2.
#[tokio::test]
async fn finish_does_not_reread_data_db_for_checksums() {
    work_counters::reset();
    let (_guard, out_dir) = flush().await;

    assert_eq!(
        work_counters::data_db_checksum_full_reads(),
        0,
        "finish() must not re-read Data.db for checksums (pre-#1663 path re-read it twice)"
    );

    // Sanity: the components written from the accumulated checksums reconcile
    // against the on-disk Data.db (byte-parity is proven in the crc_writer
    // golden tests; this guards the end-to-end wiring).
    let data = std::fs::read(out_dir.join("nb-1-big-Data.db")).expect("Data.db");
    assert!(!data.is_empty(), "flush must be non-empty for this guard");

    let digest_text =
        std::fs::read_to_string(out_dir.join("nb-1-big-Digest.crc32")).expect("Digest.crc32");
    let digest: u32 = digest_text.trim().parse().expect("digest u32");
    assert_eq!(
        digest,
        crc32(&data),
        "Digest.crc32 must equal CRC32(Data.db)"
    );

    let crc_db = std::fs::read(out_dir.join("nb-1-big-CRC.db")).expect("CRC.db");
    assert_eq!(
        crc_db,
        expected_flush_crc_db(&data),
        "CRC.db must reconcile against the on-disk Data.db (flush path, no trailer)"
    );
}
