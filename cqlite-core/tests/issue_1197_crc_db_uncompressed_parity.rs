//! Issue #1197: `CRC.db` per-chunk checksum parity for uncompressed BIG SSTables.
//!
//! Cassandra 5.0 emits a `CRC.db` component for every UNCOMPRESSED BIG (`nb`)
//! SSTable (`ChecksummedSequentialWriter` → `ChecksumWriter`), alongside (not
//! instead of) `Digest.crc32`. CQLite's writer historically emitted only
//! `Digest.crc32` — a component-completeness gap pinned during issue #1190.
//!
//! This test is the FORMAT ORACLE for that component. It does two things:
//!
//!   1. ORACLE (reference-only): against the committed Cassandra-written
//!      `test_writeparity` references, assert the on-disk `CRC.db` layout is
//!      exactly what the Cassandra source produces:
//!        * 4-byte big-endian signed `int` header = the data writer's
//!          `buffer.capacity()`, which defaults to `64 * 1024` (65536). The real
//!          fixtures store `0x00010000`.
//!        * one big-endian `u32` CRC32 per chunk of the RAW (uncompressed)
//!          Data.db bytes (`java.util.zip.CRC32` == `crc32fast`).
//!        * total length = `4 + 4 * ceil(data_len / chunk_size)` (and exactly
//!          `4 + 4` for a single-chunk file).
//!
//!      Each stored CRC is recomputed from the reference Data.db and compared.
//!      For a single-chunk file the lone CRC equals the `Digest.crc32` value
//!      (the per-chunk CRCs are NOT folded into the data digest for the
//!      uncompressed path), which is also asserted.
//!
//!   2. WRITER PARITY: re-emit the same logical table through CQLite's
//!      `SSTableWriter` and assert it now (a) emits a `CRC.db` component, (b)
//!      lists `CRC.db` in `TOC.txt`, and (c) produces a byte-identical `CRC.db`
//!      vs the Cassandra reference (Data.db is byte-identical and the chunk size
//!      matches, so CRC.db must match too).
//!
//! Dataset-dependency doctrine (issue #719 / #1208): the `test_writeparity`
//! reference binaries are committed (pinned bundle), but this test still uses
//! the skip-on-presence idiom so a clean checkout WITHOUT the binaries SKIPS
//! rather than fails. A reference that is PRESENT but malformed (wrong header,
//! wrong CRC, missing CRC.db) is a FAILURE.

#![cfg(feature = "write-support")]

use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::writer::{SSTableFormat, SSTableWriter};
use cqlite_core::storage::write_engine::mutation::{
    CellOperation, DecoratedKey, Mutation, PartitionKey, TableId,
};
use cqlite_core::types::Value;
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use tempfile::TempDir;

const KEYSPACE: &str = "test_writeparity";

/// Cassandra's default uncompressed CRC chunk size
/// (`SequentialWriterOption.Builder.bufferSize = 64 * 1024`).
const CRC_CHUNK_SIZE: usize = 64 * 1024;

/// The single fixed writetime (micros) used by the #1190 generator.
const T_WRITE: i64 = 1_700_000_000_000_000;

// ---------------------------------------------------------------------------
// Fixture resolution (skip-on-absence; present-but-malformed is a failure)
// ---------------------------------------------------------------------------

/// Resolve the committed Cassandra reference directory for `table` under
/// `test_writeparity`. Returns `None` when the dataset root is unset or the
/// reference Data.db has not been fetched (genuine absence → skip).
fn reference_dir(table: &str) -> Option<PathBuf> {
    let root = std::env::var("CQLITE_DATASETS_ROOT").ok()?;
    let base = Path::new(&root).join("sstables").join(KEYSPACE);
    for e in std::fs::read_dir(&base).ok()?.flatten() {
        let name = e.file_name().to_string_lossy().to_string();
        if name.starts_with(&format!("{table}-")) {
            let dir = e.path();
            if dir.join("nb-1-big-Data.db").is_file() {
                return Some(dir);
            }
            return None;
        }
    }
    None
}

/// Compute the expected `CRC.db` bytes for a raw `Data.db` payload, matching the
/// Cassandra layout: big-endian i32 chunk-size header + one big-endian u32 CRC32
/// per `CRC_CHUNK_SIZE` chunk.
fn expected_crc_bytes(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(CRC_CHUNK_SIZE as i32).to_be_bytes());
    for chunk in data.chunks(CRC_CHUNK_SIZE) {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(chunk);
        out.extend_from_slice(&hasher.finalize().to_be_bytes());
    }
    out
}

/// Assert the Cassandra-written reference `CRC.db` matches the format derived
/// from the Cassandra source, reconciled against the reference `Data.db`.
fn assert_reference_crc_format(table: &str, dir: &Path) {
    let crc_path = dir.join("nb-1-big-CRC.db");
    let crc = std::fs::read(&crc_path)
        .unwrap_or_else(|e| panic!("{table}: reference CRC.db {crc_path:?} unreadable: {e}"));
    assert!(
        crc.len() >= 4,
        "{table}: reference CRC.db too short ({} bytes) — must hold at least the 4-byte header",
        crc.len()
    );

    // Header: big-endian signed int == chunk size (Cassandra default 64 KiB).
    let header = i32::from_be_bytes([crc[0], crc[1], crc[2], crc[3]]);
    assert_eq!(
        header, CRC_CHUNK_SIZE as i32,
        "{table}: reference CRC.db header {header} != expected chunk size {CRC_CHUNK_SIZE}"
    );

    let data = std::fs::read(dir.join("nb-1-big-Data.db"))
        .unwrap_or_else(|e| panic!("{table}: reference Data.db unreadable: {e}"));
    assert!(
        !data.is_empty(),
        "{table}: reference Data.db present-but-empty — parity failure"
    );

    // Length contract: 4-byte header + 4 bytes per chunk.
    let expected_chunks = data.len().div_ceil(CRC_CHUNK_SIZE);
    assert_eq!(
        crc.len(),
        4 + 4 * expected_chunks,
        "{table}: reference CRC.db length {} != 4 + 4*{expected_chunks}",
        crc.len()
    );

    // Each stored CRC must equal CRC32 over the corresponding raw-data chunk.
    let expected = expected_crc_bytes(&data);
    assert_eq!(
        crc, expected,
        "{table}: reference CRC.db bytes do not reconcile against the reference Data.db \
         (per-chunk CRC32 mismatch)"
    );

    // Single-chunk invariant: the lone CRC == Digest.crc32 (per-chunk CRCs are
    // NOT folded into the data digest on the uncompressed path).
    if expected_chunks == 1 {
        let digest_text = std::fs::read_to_string(dir.join("nb-1-big-Digest.crc32"))
            .unwrap_or_else(|e| panic!("{table}: reference Digest.crc32 unreadable: {e}"));
        let digest: u32 = digest_text
            .trim()
            .parse()
            .unwrap_or_else(|e| panic!("{table}: Digest.crc32 not a decimal u32: {e}"));
        let lone = u32::from_be_bytes([crc[4], crc[5], crc[6], crc[7]]);
        assert_eq!(
            lone, digest,
            "{table}: single-chunk CRC.db value {lone} != Digest.crc32 {digest}"
        );
    }
}

// ---------------------------------------------------------------------------
// CQLite writer harness (re-emit the same logical table)
// ---------------------------------------------------------------------------

async fn cqlite_write(schema: &TableSchema, mutations: Vec<Mutation>) -> (TempDir, PathBuf) {
    let dir = TempDir::new().expect("tempdir");
    let mut writer =
        SSTableWriter::with_format(dir.path().to_path_buf(), 1, schema, 16, SSTableFormat::Big)
            .expect("writer");

    let mut keyed: Vec<(DecoratedKey, Mutation)> = mutations
        .into_iter()
        .map(|m| (m.decorated_key(schema).expect("decorated key"), m))
        .collect();
    keyed.sort_by_key(|(k, _)| k.token);

    let mut grouped: Vec<(DecoratedKey, Vec<Mutation>)> = Vec::new();
    for (k, m) in keyed {
        match grouped.last_mut() {
            Some((lk, ms)) if lk.token == k.token && lk.key == k.key => ms.push(m),
            _ => grouped.push((k, vec![m])),
        }
    }
    for (k, ms) in grouped {
        writer.write_partition(k, ms).expect("write_partition");
    }
    let info = writer.finish().await.expect("finish");
    // The writer must now report a CRC.db path for the uncompressed BIG output.
    assert!(
        info.crc_path.as_ref().is_some_and(|p| p.is_file()),
        "writer did not emit a CRC.db component (info.crc_path={:?})",
        info.crc_path
    );
    let table_dir = info.data_path.parent().expect("data parent").to_path_buf();
    (dir, table_dir)
}

/// `finished_data` schema/data — must match the #1190 generator exactly so the
/// re-emitted Data.db (and therefore CRC.db) is byte-identical.
fn finished_schema() -> TableSchema {
    TableSchema {
        keyspace: KEYSPACE.into(),
        table: "finished_data".into(),
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

fn finished_mutations() -> Vec<Mutation> {
    (0..6)
        .map(|i| {
            Mutation::new(
                TableId::new(KEYSPACE, "finished_data"),
                PartitionKey::single("id", Value::Integer(i)),
                None,
                vec![CellOperation::Write {
                    column: "name".into(),
                    value: Value::Text(format!("name{i}")),
                }],
                T_WRITE,
                None,
            )
        })
        .collect()
}

fn toc_set(toc_bytes: &[u8]) -> BTreeSet<String> {
    String::from_utf8_lossy(toc_bytes)
        .lines()
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty())
        .collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// ORACLE: the on-disk `CRC.db` of every committed Cassandra `test_writeparity`
/// reference matches the format derived from the Cassandra 5.0 source.
#[test]
fn cassandra_reference_crc_db_format_oracle() {
    let mut checked = 0usize;
    for table in [
        "finished_data",
        "partition_boundary",
        "static_clustering_shape",
    ] {
        let Some(dir) = reference_dir(table) else {
            eprintln!("[issue_1197] reference {KEYSPACE}.{table} absent; skipping");
            continue;
        };
        assert!(
            dir.join("nb-1-big-CRC.db").is_file(),
            "{table}: present reference is missing CRC.db (broken fixture)"
        );
        assert_reference_crc_format(table, &dir);
        checked += 1;
    }
    if checked == 0 {
        eprintln!("[issue_1197] no references present; oracle skipped");
    }
}

/// WRITER PARITY: CQLite re-emits `finished_data` and now produces a `CRC.db`
/// component that is byte-identical to the Cassandra reference, and lists it in
/// `TOC.txt`.
#[tokio::test]
async fn cqlite_emits_byte_identical_crc_db() {
    let Some(ref_dir) = reference_dir("finished_data") else {
        eprintln!("[issue_1197] reference finished_data absent; skipping writer parity");
        return;
    };

    let (_guard, out_dir) = cqlite_write(&finished_schema(), finished_mutations()).await;

    // (a) writer emitted the component file.
    let our_crc_path = out_dir.join("nb-1-big-CRC.db");
    assert!(
        our_crc_path.is_file(),
        "CQLite did not write CRC.db to {our_crc_path:?}"
    );

    // (b) TOC.txt lists CRC.db.
    let our_toc = toc_set(&std::fs::read(out_dir.join("nb-1-big-TOC.txt")).expect("our TOC"));
    assert!(
        our_toc.contains("CRC.db"),
        "CQLite TOC.txt does not list CRC.db (toc={our_toc:?})"
    );

    // (c) byte-for-byte vs the Cassandra reference.
    let reference = std::fs::read(ref_dir.join("nb-1-big-CRC.db")).expect("reference CRC.db");
    let ours = std::fs::read(&our_crc_path).expect("our CRC.db");
    assert_eq!(
        ours,
        reference,
        "CRC.db byte mismatch\n  cass={}\n  ours={}",
        reference
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>(),
        ours.iter().map(|b| format!("{b:02x}")).collect::<String>(),
    );

    // And it reconciles against our own Data.db.
    let our_data = std::fs::read(out_dir.join("nb-1-big-Data.db")).expect("our Data.db");
    assert_eq!(
        ours,
        expected_crc_bytes(&our_data),
        "CQLite CRC.db does not reconcile against its own Data.db"
    );
}
