//! `CRC.db` writer — per-chunk CRC32 for uncompressed BIG SSTables.
//!
//! Cassandra 5.0 writes a `CRC.db` component for every **uncompressed** BIG
//! (`nb`) SSTable, alongside (not instead of) `Digest.crc32`. Compressed tables
//! carry per-chunk CRCs inline after each compressed chunk and describe them in
//! `CompressionInfo.db`, so they have no `CRC.db`. BTI (`da`) tables likewise do
//! not emit `CRC.db` (verified against the Cassandra-written `da` fixtures).
//!
//! # On-disk format (oracle: Cassandra 5.0)
//!
//! Produced by `ChecksummedSequentialWriter` via `ChecksumWriter`
//! (`org.apache.cassandra.io.util.ChecksumWriter` /
//! `ChecksummedSequentialWriter`):
//!
//! ```text
//! [chunk size : 4 bytes, signed int, big-endian]   <- data writer buffer.capacity()
//! [CRC32 chunk 0 : 4 bytes, big-endian]
//! [CRC32 chunk 1 : 4 bytes, big-endian]
//! ...
//! ```
//!
//! - **Chunk size** is the data `SequentialWriter`'s `buffer.capacity()`, which
//!   defaults to `64 * 1024` (`SequentialWriterOption.Builder.bufferSize`). The
//!   real Cassandra fixtures store `0x00010000` (65536). `ChecksumWriter`
//!   serializes it with `DataOutput.writeInt` (big-endian).
//! - Each **CRC32** covers exactly one chunk of the **raw (uncompressed)**
//!   Data.db bytes — `flushData()` checksums `buffer.position()` bytes per buffer
//!   flush. The algorithm is `java.util.zip.CRC32` (IEEE / `crc32fast`); the
//!   value is written as a big-endian `i32`/`u32` via `DataOutput.writeInt`.
//! - The per-chunk CRC values are **not** folded into the data file's
//!   `Digest.crc32` (`checksumIncrementalResult = false` for the uncompressed
//!   path), so `Digest.crc32` remains the CRC32 over the raw data bytes only.
//!   For a single-chunk file the lone `CRC.db` entry therefore equals the
//!   `Digest.crc32` value.
//!
//! # Seek formula (read side)
//!
//! To locate the CRC for byte `offset` in Data.db:
//! `crc_file_pos = (offset / chunk_size) * 4 + 4`
//! (`DataIntegrityMetadata.ChecksumValidator`).

use std::path::{Path, PathBuf};

use crate::error::Result;

/// Default uncompressed CRC chunk size, matching Cassandra's
/// `SequentialWriterOption.Builder` default `bufferSize` of `64 * 1024`.
///
/// This is the value stored in the 4-byte `CRC.db` header and the block size
/// over which each per-chunk CRC32 is computed.
pub const CRC_CHUNK_SIZE: usize = 64 * 1024;

/// Compute the `CRC.db` bytes for an uncompressed `Data.db`.
///
/// Streams `data_path` in `CRC_CHUNK_SIZE` blocks (bounded memory, independent
/// of the total Data.db size — see the digest-streaming rationale in
/// `finish.rs`) and emits the Cassandra `CRC.db` layout: a big-endian `i32`
/// chunk-size header followed by one big-endian `u32` CRC32 per chunk.
///
/// An empty `Data.db` yields just the 4-byte header (zero chunks), matching
/// Cassandra's behaviour when no data buffer is ever flushed.
pub(super) async fn build_crc_bytes(data_path: &Path) -> Result<Vec<u8>> {
    use tokio::io::AsyncReadExt;

    let mut out = Vec::new();
    // Header: chunk size as a big-endian signed 32-bit int (DataOutput.writeInt).
    out.extend_from_slice(&(CRC_CHUNK_SIZE as i32).to_be_bytes());

    let mut file = tokio::fs::File::open(data_path).await?;
    let mut buffer = vec![0u8; CRC_CHUNK_SIZE];
    let mut filled = 0usize;
    loop {
        // Fill up to a full chunk before checksumming so each CRC covers a
        // complete CRC_CHUNK_SIZE block (except the final, short chunk), exactly
        // as Cassandra's per-buffer flushData() does. A single short read does
        // not imply EOF, so accumulate until the chunk is full or EOF is hit.
        let n = file.read(&mut buffer[filled..]).await?;
        if n == 0 {
            if filled > 0 {
                let mut hasher = crc32fast::Hasher::new();
                hasher.update(&buffer[..filled]);
                out.extend_from_slice(&hasher.finalize().to_be_bytes());
            }
            break;
        }
        filled += n;
        if filled == CRC_CHUNK_SIZE {
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&buffer[..filled]);
            out.extend_from_slice(&hasher.finalize().to_be_bytes());
            filled = 0;
        }
    }

    Ok(out)
}

/// Write the `CRC.db` component for an uncompressed `Data.db`, returning the
/// component path.
///
/// Computes the per-chunk CRC bytes (see [`build_crc_bytes`]) and writes them to
/// `crc_path`. Callers add the returned path to `TOC.txt` and `SSTableInfo`.
pub(super) async fn write_crc_db(data_path: &Path, crc_path: PathBuf) -> Result<PathBuf> {
    let bytes = build_crc_bytes(data_path).await?;
    tokio::fs::write(&crc_path, bytes).await?;
    Ok(crc_path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn empty_data_yields_header_only() {
        let dir = tempfile::tempdir().expect("tempdir");
        let data = dir.path().join("Data.db");
        tokio::fs::write(&data, b"").await.expect("write data");

        let bytes = build_crc_bytes(&data).await.expect("crc bytes");
        assert_eq!(bytes, (CRC_CHUNK_SIZE as i32).to_be_bytes().to_vec());
    }

    #[tokio::test]
    async fn single_chunk_crc_matches_whole_file_crc32() {
        // For data <= one chunk, the single CRC.db entry equals the whole-file
        // CRC32 (the Digest.crc32 value) — Cassandra-observed invariant.
        let dir = tempfile::tempdir().expect("tempdir");
        let data = dir.path().join("Data.db");
        let payload = b"hello cqlite crc.db parity";
        tokio::fs::write(&data, payload).await.expect("write data");

        let bytes = build_crc_bytes(&data).await.expect("crc bytes");
        assert_eq!(bytes.len(), 8, "header + 1 crc");

        let header = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        assert_eq!(header, CRC_CHUNK_SIZE as i32);

        let stored = u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(payload);
        assert_eq!(stored, hasher.finalize());
    }

    #[tokio::test]
    async fn multi_chunk_emits_one_crc_per_chunk() {
        let dir = tempfile::tempdir().expect("tempdir");
        let data = dir.path().join("Data.db");
        // 2.5 chunks of deterministic bytes.
        let len = CRC_CHUNK_SIZE * 2 + CRC_CHUNK_SIZE / 2;
        let payload: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
        tokio::fs::write(&data, &payload).await.expect("write data");

        let bytes = build_crc_bytes(&data).await.expect("crc bytes");
        // header(4) + 3 CRCs(4 each)
        assert_eq!(bytes.len(), 4 + 3 * 4);

        // Verify each chunk CRC independently.
        let mut pos = 4;
        for chunk in payload.chunks(CRC_CHUNK_SIZE) {
            let stored =
                u32::from_be_bytes([bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3]]);
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(chunk);
            assert_eq!(stored, hasher.finalize());
            pos += 4;
        }
    }
}
