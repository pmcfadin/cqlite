//! Issue #1406 — CompressionInfo.db write path: fail-closed guard + claim boundary.
//!
//! Posture (b), owner-approved: CQLite's production write surface emits
//! UNCOMPRESSED SSTables only. The compressed-write building blocks
//! (`CompressedDataWriter` / `CompressionInfoWriter`) are built but UNWIRED — they
//! exist for read-path fixtures, not production emission, and carry zero
//! Cassandra-side byte-parity coverage. These tests exercise the PUBLIC write
//! surface to prove that any attempt to configure compression fails closed with a
//! clear error instead of silently emitting an uncompressed (falsely-claimed) or
//! partial/unvalidated artifact.

#![cfg(feature = "write-support")]

use cqlite_core::error::Error;
use cqlite_core::schema::cql_parser::parse_cql_schema;
use cqlite_core::schema::TableSchema;
use cqlite_core::storage::sstable::writer::{CompressionAlgorithm, SSTableWriter};

fn minimal_schema() -> TableSchema {
    parse_cql_schema("CREATE TABLE test_ks.guard_tbl (id int PRIMARY KEY, val text);")
        .expect("CQL schema should parse")
}

/// The public write surface refuses every real compression algorithm.
#[test]
fn with_compression_fails_closed_for_real_algorithms() {
    let dir = tempfile::tempdir().expect("tempdir");
    let schema = minimal_schema();

    for algo in [
        CompressionAlgorithm::Lz4,
        CompressionAlgorithm::Snappy,
        CompressionAlgorithm::Deflate,
        CompressionAlgorithm::Zstd,
    ] {
        let result = SSTableWriter::with_compression(dir.path().to_path_buf(), 1, &schema, algo);
        let err = result.err().unwrap_or_else(|| {
            panic!(
                "SSTableWriter::with_compression({algo:?}) MUST fail closed while the \
                 compressed write path is unwired (issue #1406)"
            )
        });
        match err {
            Error::UnsupportedFormat(msg) => {
                assert!(
                    msg.contains(algo.cassandra_name()),
                    "error must name the requested algorithm, got: {msg}"
                );
                assert!(
                    msg.contains("1406"),
                    "error must cite the claim-boundary issue #1406, got: {msg}"
                );
            }
            other => panic!("expected Error::UnsupportedFormat, got {other:?}"),
        }
    }
}

/// The uncompressed case (the only supported production write) is accepted.
#[test]
fn with_compression_none_builds_an_uncompressed_writer() {
    let dir = tempfile::tempdir().expect("tempdir");
    let schema = minimal_schema();

    let writer = SSTableWriter::with_compression(
        dir.path().to_path_buf(),
        1,
        &schema,
        CompressionAlgorithm::None,
    )
    .expect("uncompressed (None) production writes must be permitted");
    // Constructing the writer is sufficient evidence the None path is live; drop it.
    drop(writer);
}
