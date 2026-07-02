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

// `Path`/`PathBuf`/`Result` are used only by the re-read oracle helpers
// (`build_crc_bytes`/`write_crc_db`), which are test-only since the production
// write path assembles `CRC.db` from streaming-accumulated chunk CRCs (#1663).
#[cfg(test)]
use crate::error::Result;
#[cfg(test)]
use std::path::{Path, PathBuf};

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

/// Assemble the on-disk `CRC.db` bytes from a slice of per-chunk CRC32 values.
///
/// This is the single source of truth for the `CRC.db` byte layout (issue
/// #1663): a big-endian `i32` chunk-size header followed by one big-endian `u32`
/// CRC32 per chunk, plus the issue-#1222 compaction trailer rule. Both the
/// streaming write path (checksums accumulated during the write — see
/// [`StreamingCrc`]) and the re-read [`build_crc_bytes`] oracle route their
/// chunk CRCs through here, so the two paths are provably byte-identical.
///
/// Trailer rule (issue #1222):
/// - [`CrcTrailer::None`] (flush path) appends nothing after the last chunk CRC.
/// - [`CrcTrailer::EmptyFinalChunk`] (compaction path) appends ONE trailing
///   `CRC32 = 0` (`00000000`) — Cassandra's compaction close-time zero-length
///   buffer flush — but ONLY when at least one real chunk CRC was produced
///   (non-empty `Data.db`). An empty `Data.db` stays header-only for BOTH
///   trailers (there is no Cassandra golden for the empty-compaction case).
pub(crate) fn assemble_crc_bytes(chunk_crcs: &[u32], trailer: CrcTrailer) -> Vec<u8> {
    // header (4) + one u32 per chunk + optional trailing empty-chunk CRC (4).
    let mut out = Vec::with_capacity(4 + chunk_crcs.len() * 4 + 4);
    // Header: chunk size as a big-endian signed 32-bit int (DataOutput.writeInt).
    out.extend_from_slice(&(CRC_CHUNK_SIZE as i32).to_be_bytes());
    for crc in chunk_crcs {
        out.extend_from_slice(&crc.to_be_bytes());
    }
    // Compaction-only trailing empty-final-chunk CRC32 (issue #1222): CRC32 of a
    // zero-length buffer is 0. Gated on a real chunk having been produced so an
    // empty Data.db keeps the documented header-only output for both trailers.
    if matches!(trailer, CrcTrailer::EmptyFinalChunk) && !chunk_crcs.is_empty() {
        out.extend_from_slice(&0u32.to_be_bytes());
    }
    out
}

/// Incremental `CRC.db` + `Digest.crc32` accumulator for the streaming write
/// path (issue #1663).
///
/// The streaming Data.db writer feeds every byte it writes to disk through
/// [`update`](Self::update) exactly once, in write order. This computes the
/// whole-file CRC32 (the `Digest.crc32` value) and the per-chunk CRC32s
/// (`CRC.db`) as the data streams by, so `finish()` never has to re-read the
/// finished `Data.db` to checksum it. The chunk boundaries are fixed
/// `CRC_CHUNK_SIZE` blocks of the RAW Data.db bytes and span partition
/// boundaries — identical to what the re-read [`build_crc_bytes`] oracle
/// produces — because `update` carries `chunk_filled` across calls.
#[derive(Debug, Default)]
pub(crate) struct StreamingCrc {
    /// Whole-file CRC32 hasher (the `Digest.crc32` value).
    whole: crc32fast::Hasher,
    /// CRC32 hasher for the chunk currently being filled.
    chunk: crc32fast::Hasher,
    /// Bytes already fed into `chunk` toward the current `CRC_CHUNK_SIZE` block.
    chunk_filled: usize,
    /// Finalized per-chunk CRC32s, in order.
    chunk_crcs: Vec<u32>,
}

impl StreamingCrc {
    /// Create an empty accumulator.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Feed the next run of just-written Data.db bytes, in write order.
    ///
    /// Updates the whole-file hasher and slices `bytes` into the fixed
    /// `CRC_CHUNK_SIZE` chunk grid, finalizing a chunk CRC every time a full
    /// block is completed. `chunk_filled` carries across calls so a chunk that
    /// straddles two `update` calls (i.e. two partition flushes) is checksummed
    /// as one block — matching `build_crc_bytes`'s chunk boundaries exactly.
    pub(crate) fn update(&mut self, bytes: &[u8]) {
        self.whole.update(bytes);
        let mut remaining = bytes;
        while !remaining.is_empty() {
            let take = (CRC_CHUNK_SIZE - self.chunk_filled).min(remaining.len());
            self.chunk.update(&remaining[..take]);
            self.chunk_filled += take;
            remaining = &remaining[take..];
            if self.chunk_filled == CRC_CHUNK_SIZE {
                let done = std::mem::replace(&mut self.chunk, crc32fast::Hasher::new());
                self.chunk_crcs.push(done.finalize());
                self.chunk_filled = 0;
            }
        }
    }

    /// Finalize the accumulator, returning `(digest_crc32, chunk_crcs)`.
    ///
    /// `digest_crc32` is the whole-file CRC32 over every fed byte (the
    /// `Digest.crc32` value). `chunk_crcs` is one CRC32 per `CRC_CHUNK_SIZE`
    /// block, including a final short chunk if `chunk_filled > 0`. Pass
    /// `chunk_crcs` to [`assemble_crc_bytes`] to obtain the `CRC.db` bytes.
    pub(crate) fn finalize(mut self) -> (u32, Vec<u32>) {
        if self.chunk_filled > 0 {
            self.chunk_crcs.push(self.chunk.finalize());
        }
        (self.whole.finalize(), self.chunk_crcs)
    }
}

/// Compute the `CRC.db` bytes for an uncompressed `Data.db` by RE-READING it.
///
/// Streams `data_path` in `CRC_CHUNK_SIZE` blocks (bounded memory, independent
/// of the total Data.db size) to build the per-chunk CRC32s, then delegates the
/// byte layout to [`assemble_crc_bytes`] so this re-read oracle and the
/// streaming [`StreamingCrc`] path are provably byte-identical.
///
/// Retained (issue #1663) as the golden oracle for the incremental path's
/// byte-parity tests and to build a `CRC.db` for the reader-side tests; the
/// production write path no longer re-reads Data.db and does NOT call this.
/// Each invocation bumps the [`data_db_checksum_full_reads`] work counter so a
/// regression that reintroduces the full-file re-read is caught.
///
/// [`data_db_checksum_full_reads`]: crate::storage::sstable::work_counters::data_db_checksum_full_reads
#[cfg(test)]
pub(super) async fn build_crc_bytes(data_path: &Path, trailer: CrcTrailer) -> Result<Vec<u8>> {
    use tokio::io::AsyncReadExt;

    let mut file = tokio::fs::File::open(data_path).await?;
    // Full re-read of Data.db for checksums — count it (issue #1663).
    crate::storage::sstable::work_counters::add_data_db_checksum_full_read();
    let mut chunk_crcs = Vec::new();
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
                chunk_crcs.push(hasher.finalize());
            }
            break;
        }
        filled += n;
        if filled == CRC_CHUNK_SIZE {
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&buffer[..filled]);
            chunk_crcs.push(hasher.finalize());
            filled = 0;
        }
    }

    Ok(assemble_crc_bytes(&chunk_crcs, trailer))
}

/// Write a `CRC.db` component by re-reading `data_path` (test-only oracle,
/// issue #1663).
///
/// The production write path assembles `CRC.db` from streaming-accumulated chunk
/// CRCs (see [`assemble_crc_bytes`] / [`StreamingCrc`]) and no longer calls this;
/// it is retained to build `CRC.db` fixtures for the reader-side tests.
///
/// `trailer` selects the flush vs compaction tail (issue #1222): flush callers
/// pass [`CrcTrailer::None`]; compaction callers pass
/// [`CrcTrailer::EmptyFinalChunk`].
#[cfg(test)]
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

    // ---------------------------------------------------------------------
    // Issue #1663: the incremental (streaming-accumulated) checksums must be
    // byte-identical to the re-read oracle for BOTH the flush (`CrcTrailer::None`)
    // and compaction (`CrcTrailer::EmptyFinalChunk`) tails, across single-chunk,
    // multi-chunk, and short-final-chunk sizes — regardless of how the byte
    // stream is split across `update` calls (partition-flush boundaries), which
    // proves chunks straddle partition boundaries exactly like `build_crc_bytes`.
    // ---------------------------------------------------------------------

    /// Feed `payload` through a `StreamingCrc` in `split`-sized runs (simulating
    /// per-partition flushes), then assert the assembled `CRC.db` bytes and the
    /// whole-file digest equal the re-read oracle (`build_crc_bytes` /
    /// `SSTableWriter::compute_crc32`) over a file holding the same bytes.
    async fn assert_incremental_matches_reread(payload: &[u8], trailer: CrcTrailer, split: usize) {
        use crate::storage::sstable::writer::SSTableWriter;

        // Incremental path: accumulate as the bytes "stream" by in `split` runs.
        let mut acc = StreamingCrc::new();
        for run in payload.chunks(split.max(1)) {
            acc.update(run);
        }
        let (inc_digest, inc_chunk_crcs) = acc.finalize();
        let inc_crc_db = assemble_crc_bytes(&inc_chunk_crcs, trailer);

        // Re-read oracle over a file holding the identical bytes.
        let dir = tempfile::tempdir().expect("tempdir");
        let data = dir.path().join("Data.db");
        tokio::fs::write(&data, payload).await.expect("write data");
        let reread_crc_db = build_crc_bytes(&data, trailer)
            .await
            .expect("build_crc_bytes");
        let reread_digest = SSTableWriter::compute_crc32(&data)
            .await
            .expect("compute_crc32");

        assert_eq!(
            inc_crc_db, reread_crc_db,
            "incremental CRC.db must equal re-read oracle (len={}, trailer={trailer:?}, split={split})",
            payload.len()
        );
        assert_eq!(
            inc_digest,
            reread_digest,
            "incremental Digest.crc32 must equal re-read oracle (len={}, split={split})",
            payload.len()
        );
    }

    #[tokio::test]
    async fn incremental_checksums_match_reread_oracle_all_sizes() {
        // single-chunk short, exactly one full chunk, multi + short final,
        // exactly two full chunks, multi + short final again.
        let sizes = [
            100usize,
            CRC_CHUNK_SIZE,
            CRC_CHUNK_SIZE + 100,
            CRC_CHUNK_SIZE * 2,
            CRC_CHUNK_SIZE * 2 + 100,
        ];
        // A split that is NOT a multiple of CRC_CHUNK_SIZE forces chunks to
        // straddle `update` (partition-flush) boundaries.
        let splits = [1usize, 7_000, CRC_CHUNK_SIZE, usize::MAX];
        for &len in &sizes {
            let payload: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
            for &trailer in &[CrcTrailer::None, CrcTrailer::EmptyFinalChunk] {
                for &split in &splits {
                    assert_incremental_matches_reread(&payload, trailer, split).await;
                }
            }
        }
    }

    #[tokio::test]
    async fn incremental_empty_matches_reread_oracle() {
        // Empty Data.db: header-only for both trailers, digest = CRC32 of zero
        // bytes = 0 — must match the re-read oracle.
        assert_incremental_matches_reread(&[], CrcTrailer::None, 1).await;
        assert_incremental_matches_reread(&[], CrcTrailer::EmptyFinalChunk, 1).await;
    }
}
