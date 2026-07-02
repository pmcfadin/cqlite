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
//! # Flush vs compaction: the trailing empty-chunk CRC32 (issue #1222)
//!
//! Cassandra's **compaction** write path (`CompactionAwareWriter` →
//! `SSTableRewriter` → `BigTableWriter`) flushes the data `SequentialWriter` once
//! more at close after the last real chunk, checksumming a ZERO-length buffer.
//! `java.util.zip.CRC32` over zero bytes is `0`, so the compacted `CRC.db` carries
//! one extra trailing `00000000` group after the last real per-chunk CRC32. The
//! **flush** path does NOT do this (verified against the #1190 flush goldens,
//! whose `CRC.db` ends exactly on the last real chunk CRC). The writer therefore
//! takes an explicit [`CrcTrailer`] so the compaction path can request the
//! trailing empty-chunk CRC32 without changing the byte-identical flush output.
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

/// Whether the `CRC.db` carries Cassandra's compaction-only trailing
/// empty-final-chunk CRC32 (issue #1222).
///
/// The two SSTable write paths produce byte-different `CRC.db` tails:
/// - [`CrcTrailer::None`] — the **flush** path: the file ends on the last real
///   per-chunk CRC32. This is the byte-identical match for the #1190 flush
///   goldens and MUST stay unchanged.
/// - [`CrcTrailer::EmptyFinalChunk`] — the **compaction** path: Cassandra's
///   `CompactionAwareWriter` flushes the data writer once more at close over a
///   zero-length buffer, appending one trailing `00000000` (`CRC32` of zero
///   bytes). This is the byte-identical match for the #1017 compacted goldens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CrcTrailer {
    /// No trailing chunk — the flush path (default).
    None,
    /// Append one trailing empty-final-chunk `CRC32 = 0` — the compaction path.
    EmptyFinalChunk,
}

/// Compute the `CRC.db` bytes for an uncompressed `Data.db`.
///
/// Streams `data_path` in `CRC_CHUNK_SIZE` blocks (bounded memory, independent
/// of the total Data.db size — see the digest-streaming rationale in
/// `finish.rs`) and emits the Cassandra `CRC.db` layout: a big-endian `i32`
/// chunk-size header followed by one big-endian `u32` CRC32 per chunk.
///
/// When `trailer` is [`CrcTrailer::EmptyFinalChunk`] (the compaction path,
/// issue #1222), one extra big-endian `u32` `CRC32 = 0` is appended after the
/// last real chunk, matching Cassandra's compaction close-time empty-buffer
/// flush. [`CrcTrailer::None`] (the flush path) appends nothing.
///
/// An empty `Data.db` yields just the 4-byte header (zero chunks), matching
/// Cassandra's behaviour when no data buffer is ever flushed. This empty-file
/// behaviour is preserved for BOTH trailers: the compaction trailing
/// empty-final-chunk `CRC32 = 0` is appended only when at least one real data
/// chunk was checksummed (non-empty `Data.db`). A compaction that purges all
/// partitions and finalizes an empty BIG SSTable therefore still gets the
/// documented header-only `CRC.db`, never a lone trailing `00000000` after the
/// header — the trailing zero only makes sense after a real chunk, and we have
/// no Cassandra golden for the empty-compaction case (issue #1222 roborev).
pub(super) async fn build_crc_bytes(data_path: &Path, trailer: CrcTrailer) -> Result<Vec<u8>> {
    use tokio::io::AsyncReadExt;

    let mut out = Vec::new();
    // Header: chunk size as a big-endian signed 32-bit int (DataOutput.writeInt).
    out.extend_from_slice(&(CRC_CHUNK_SIZE as i32).to_be_bytes());

    let mut file = tokio::fs::File::open(data_path).await?;
    let mut buffer = vec![0u8; CRC_CHUNK_SIZE];
    let mut filled = 0usize;
    // Track whether any real data-chunk CRC was emitted (non-empty Data.db). The
    // compaction trailer is gated on this so an empty Data.db keeps the
    // documented header-only output for both trailers (issue #1222).
    let mut emitted_real_chunk = false;
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
                emitted_real_chunk = true;
            }
            break;
        }
        filled += n;
        if filled == CRC_CHUNK_SIZE {
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&buffer[..filled]);
            out.extend_from_slice(&hasher.finalize().to_be_bytes());
            emitted_real_chunk = true;
            filled = 0;
        }
    }

    // Compaction-only trailing empty-final-chunk CRC32 (issue #1222). Cassandra's
    // compaction close flushes the data writer once more over a zero-length
    // buffer; CRC32 of zero bytes is 0, so emit one trailing `00000000`. The
    // flush path (CrcTrailer::None) never reaches this. Gated on a real chunk
    // having been emitted: an empty Data.db keeps the documented header-only
    // output even on the compaction path (the trailing zero only makes sense
    // after at least one real data chunk, and there is no empty-compaction
    // golden to match).
    if matches!(trailer, CrcTrailer::EmptyFinalChunk) && emitted_real_chunk {
        let empty_crc = crc32fast::Hasher::new().finalize();
        out.extend_from_slice(&empty_crc.to_be_bytes());
    }

    Ok(out)
}

/// Write the `CRC.db` component for an uncompressed `Data.db`, returning the
/// component path.
///
/// Computes the per-chunk CRC bytes (see [`build_crc_bytes`]) and writes them to
/// `crc_path`. Callers add the returned path to `TOC.txt` and `SSTableInfo`.
///
/// `trailer` selects the flush vs compaction tail (issue #1222): flush callers
/// pass [`CrcTrailer::None`]; compaction callers pass
/// [`CrcTrailer::EmptyFinalChunk`].
pub(crate) async fn write_crc_db(
    data_path: &Path,
    crc_path: PathBuf,
    trailer: CrcTrailer,
) -> Result<PathBuf> {
    let bytes = build_crc_bytes(data_path, trailer).await?;
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

        let bytes = build_crc_bytes(&data, CrcTrailer::None)
            .await
            .expect("crc bytes");
        assert_eq!(bytes, (CRC_CHUNK_SIZE as i32).to_be_bytes().to_vec());
    }

    #[tokio::test]
    async fn empty_data_with_compaction_trailer_stays_header_only() {
        // Issue #1222 (roborev): the compaction trailing empty-final-chunk
        // CRC32 = 0 must be gated on a real data chunk having been emitted. An
        // empty Data.db (e.g. a compaction that purges all partitions and
        // finalizes an empty BIG SSTable) keeps the documented header-only
        // CRC.db — NO trailing 00000000 — for BOTH trailers.
        let dir = tempfile::tempdir().expect("tempdir");
        let data = dir.path().join("Data.db");
        tokio::fs::write(&data, b"").await.expect("write data");

        let header_only = (CRC_CHUNK_SIZE as i32).to_be_bytes().to_vec();

        let flush = build_crc_bytes(&data, CrcTrailer::None)
            .await
            .expect("flush crc bytes");
        let compaction = build_crc_bytes(&data, CrcTrailer::EmptyFinalChunk)
            .await
            .expect("compaction crc bytes");

        assert_eq!(
            flush, header_only,
            "empty Data.db on the flush path is header-only"
        );
        assert_eq!(
            compaction, header_only,
            "empty Data.db on the compaction path stays header-only (no trailing 00000000)"
        );
    }

    #[tokio::test]
    async fn compaction_trailer_appends_one_empty_chunk_crc32_zero() {
        // Issue #1222: the compaction path appends one trailing empty-final-chunk
        // CRC32 = 0 (00000000) after the last real chunk; the flush path does not.
        let dir = tempfile::tempdir().expect("tempdir");
        let data = dir.path().join("Data.db");
        let payload = b"compaction crc.db trailer parity";
        tokio::fs::write(&data, payload).await.expect("write data");

        let flush = build_crc_bytes(&data, CrcTrailer::None)
            .await
            .expect("flush crc bytes");
        let compaction = build_crc_bytes(&data, CrcTrailer::EmptyFinalChunk)
            .await
            .expect("compaction crc bytes");

        // Flush: header + 1 real chunk CRC.
        assert_eq!(flush.len(), 8, "flush = header + 1 chunk crc");
        // Compaction: flush bytes + a trailing 00000000.
        assert_eq!(
            compaction.len(),
            flush.len() + 4,
            "compaction appends one trailing chunk crc"
        );
        assert_eq!(
            &compaction[..flush.len()],
            &flush[..],
            "compaction must be flush bytes plus the trailer (flush output unchanged)"
        );
        assert_eq!(
            &compaction[flush.len()..],
            &[0u8, 0, 0, 0],
            "trailing chunk crc32 of an empty buffer is 0"
        );
    }

    #[tokio::test]
    async fn single_chunk_crc_matches_whole_file_crc32() {
        // For data <= one chunk, the single CRC.db entry equals the whole-file
        // CRC32 (the Digest.crc32 value) — Cassandra-observed invariant.
        let dir = tempfile::tempdir().expect("tempdir");
        let data = dir.path().join("Data.db");
        let payload = b"hello cqlite crc.db parity";
        tokio::fs::write(&data, payload).await.expect("write data");

        let bytes = build_crc_bytes(&data, CrcTrailer::None)
            .await
            .expect("crc bytes");
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

        let bytes = build_crc_bytes(&data, CrcTrailer::None)
            .await
            .expect("crc bytes");
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
