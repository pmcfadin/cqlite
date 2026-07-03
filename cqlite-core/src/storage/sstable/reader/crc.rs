//! `CRC.db` reader — per-chunk CRC32 for **uncompressed** BIG (`nb`) SSTables.
//!
//! Cassandra 5.0 writes a `CRC.db` sidecar for every uncompressed BIG SSTable
//! (`ChecksummedSequentialWriter` / `DataIntegrityMetadata.ChecksumValidator`).
//! CQLite writes it byte-for-byte on the write side (`writer/crc_writer.rs`,
//! issue #1197) but had **no reader** — the uncompressed read path returned raw
//! bytes unchecked. This module is the missing consumer: it parses the
//! authoritative `CRC.db` bytes and exposes the stored CRC32 for any Data.db
//! offset so the read path can fail-fast on a bit flip (issue #1396), matching
//! the compressed path's unconditional per-chunk CRC posture.
//!
//! # On-disk format (oracle: Cassandra 5.0 `ChecksumWriter`, mirrored by #1197)
//!
//! ```text
//! [chunk size : 4 bytes, i32, big-endian]   <- SequentialWriter buffer.capacity(), default 65536
//! [CRC32 chunk 0 : 4 bytes, u32, big-endian]
//! [CRC32 chunk 1 : 4 bytes, u32, big-endian]
//! ...
//! ```
//!
//! Each CRC32 covers exactly one `chunk_size` block of the **raw uncompressed**
//! Data.db bytes (the final chunk is short). The algorithm is `java.util.zip.CRC32`
//! (IEEE), i.e. `crc32fast` — identical to the compressed path's `crc32fast::hash`.
//!
//! # Seek formula (`DataIntegrityMetadata.ChecksumValidator`)
//!
//! For a Data.db byte `offset`: `chunk_index = offset / chunk_size`,
//! `crc_file_pos = chunk_index * 4 + 4` (the `+ 4` skips the header).
//!
//! # Trailing empty-final-chunk CRC (compaction, issue #1222)
//!
//! Cassandra's compaction write path appends one extra trailing `CRC32 = 0` after
//! the last real chunk (the flush path does not). That trailing entry maps to a
//! Data.db offset AT OR BEYOND EOF and is therefore never dereferenced by a real
//! read — [`CrcDb::crc_for_chunk`] is only ever called for a chunk that covers
//! actual returned Data.db bytes, so the extra entry is a harmless no-op, never a
//! truncation error.
//!
//! # Memory
//!
//! The `CRC.db` is a tiny sidecar (`4 + 4 * ceil(data_len / chunk_size)` bytes —
//! for a multi-GiB Data.db still only a few hundred KiB, `data_len / 16384`), so
//! like the other resident sidecars (`Index.db`, `Filter.db`, BTI `Partitions.db`)
//! it is parsed once into memory. This adds **no** Data.db-file-sized buffer; a
//! per-offset lookup returns exactly one stored CRC32 without touching the
//! Data.db-sized read buffer, keeping the read well within the <128 MB budget.

use std::path::Path;

use crate::{Error, Result};

/// Upper bound accepted for the `CRC.db` chunk-size header (issue #1396).
///
/// Cassandra writes uncompressed BIG SSTables with a 64 KiB (`65536`) chunk and
/// there is no realistic reason for an uncompressed CRC chunk to be larger. A
/// malformed / hostile sidecar could advertise an arbitrary positive `i32`
/// (up to ~2 GiB) which downstream verification turns into a `vec![0u8; n]`
/// scratch allocation — an OOM / DoS vector. We cap at 16 MiB: generously above
/// any plausible real chunk length yet a bounded allocation, so an absurd
/// header is rejected as typed corruption at parse time rather than exhausting
/// memory. Any legitimate value stays far below this.
pub(crate) const MAX_CRC_CHUNK_SIZE: u32 = 16 * 1024 * 1024;

/// Lower bound accepted for the `CRC.db` chunk-size header (issue #1396).
///
/// Cassandra's uncompressed `ChecksummedRandomAccessReader` uses a fixed 64 KiB
/// (`65536`) chunk. A hostile sidecar advertising a TINY `chunk_size` (e.g. `1`)
/// implies `~data_len` CRC entries, so the derived "maximum plausible CRC.db
/// length" in [`CrcDb::open`] balloons to `~4 * data_len` — a multi-GiB
/// `tokio::fs::read` for a large Data.db, defeating the max-chunk guard. We floor
/// at 4096: well below Cassandra's 64 KiB (so every real sidecar passes), yet far
/// above any plausible real value, which caps the per-Data.db entry count and
/// kills the tiny-chunk entry-count explosion. The accepted header range is
/// therefore `MIN_CRC_CHUNK_SIZE ..= MAX_CRC_CHUNK_SIZE`.
pub(crate) const MIN_CRC_CHUNK_SIZE: u32 = 4096;

/// Absolute ceiling on the number of `CRC.db` bytes ever read into memory
/// (issue #1396 Fix 2), independent of the header `chunk_size` and the declared
/// Data.db length. Derivation: at Cassandra's 64 KiB chunk size, one 4-byte CRC
/// covers 64 KiB of Data.db, so 64 MiB of CRC entries (`64 MiB / 4 = 16M`
/// entries) describes a 1 TiB Data.db — larger than any uncompressed BIG SSTable
/// we target. Capping the sidecar read at 64 MiB is thus generous for real data
/// yet a hard bound: a `CRC.db` larger than this is rejected as typed corruption
/// before its body is read, regardless of what its header or the Data.db size
/// claim.
pub(crate) const ABSOLUTE_CRC_DB_MAX: u64 = 64 * 1024 * 1024;

/// Validate a raw `CRC.db` chunk-size header value, returning the accepted `u32`
/// or a typed [`Error::Corruption`] naming the violated bound. Enforced
/// identically at parse time and at open time so read-time verification uses the
/// same bounds as open-time (issue #1396).
fn validate_chunk_size(chunk_size_raw: i32) -> Result<u32> {
    if chunk_size_raw <= 0 {
        return Err(Error::corruption(format!(
            "CRC.db chunk-size header is non-positive ({chunk_size_raw}); expected a positive block size (Cassandra default 65536)"
        )));
    }
    let chunk_size = chunk_size_raw as u32;
    if chunk_size < MIN_CRC_CHUNK_SIZE {
        // Reject a tiny chunk size before it can inflate the per-Data.db entry
        // count / derived sidecar-length bound (issue #1396: OOM guard).
        return Err(Error::corruption(format!(
            "CRC.db chunk-size header is {chunk_size} bytes — below the {MIN_CRC_CHUNK_SIZE}-byte minimum (Cassandra uses 65536); a chunk size this small implies an unbounded CRC entry count"
        )));
    }
    if chunk_size > MAX_CRC_CHUNK_SIZE {
        // Bound the header before any downstream `vec![0u8; chunk_size]`
        // verification buffer is sized from it (issue #1396: OOM guard).
        return Err(Error::corruption(format!(
            "CRC.db chunk-size header is {chunk_size} bytes — exceeds the {MAX_CRC_CHUNK_SIZE}-byte maximum (Cassandra default 65536); refusing to allocate an unbounded verification buffer"
        )));
    }
    Ok(chunk_size)
}

/// Parsed `CRC.db`: the chunk-size header plus one CRC32 per Data.db chunk.
#[derive(Debug, Clone)]
pub(crate) struct CrcDb {
    /// Chunk size from the 4-byte big-endian header (Cassandra default 65536).
    chunk_size: u32,
    /// One big-endian-decoded CRC32 per chunk, in Data.db chunk order. May carry
    /// one trailing compaction empty-final-chunk `0` (issue #1222) that is never
    /// dereferenced by an in-bounds read.
    crcs: Vec<u32>,
}

impl CrcDb {
    /// Parse the raw `CRC.db` bytes. Never panics; a missing header, a negative /
    /// zero chunk size, or a partial trailing CRC entry is a typed
    /// [`Error::Corruption`].
    pub(crate) fn parse(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 4 {
            return Err(Error::corruption(format!(
                "CRC.db is {} bytes — too short for the 4-byte chunk-size header",
                bytes.len()
            )));
        }
        let chunk_size_raw = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        let chunk_size = validate_chunk_size(chunk_size_raw)?;

        let body = &bytes[4..];
        if body.len() % 4 != 0 {
            return Err(Error::corruption(format!(
                "CRC.db body is {} bytes — not a whole number of 4-byte CRC32 entries (truncated)",
                body.len()
            )));
        }
        let crcs = body
            .chunks_exact(4)
            .map(|c| u32::from_be_bytes([c[0], c[1], c[2], c[3]]))
            .collect();

        Ok(Self { chunk_size, crcs })
    }

    /// Read and parse a `CRC.db` from disk, bounded by the Data.db size.
    ///
    /// `data_len` is the length in bytes of the associated `Data.db`. Before
    /// reading the sidecar body into memory this reads only the 4-byte
    /// chunk-size header, derives the MAXIMUM plausible `CRC.db` length for a
    /// `data_len`-byte Data.db, and rejects an oversized sidecar as typed
    /// [`Error::Corruption`] (issue #1396 Fix 2). This closes an unbounded-alloc
    /// vector at open: a malformed / hostile `CRC.db` far larger than the
    /// Data.db can possibly imply would otherwise force `tokio::fs::read` to
    /// buffer the whole (arbitrarily large) file before any size sanity check.
    pub(crate) async fn open(path: &Path, data_len: u64) -> Result<Self> {
        use tokio::io::AsyncReadExt;

        // Read ONLY the 4-byte chunk-size header first (bounded) so a hostile
        // sidecar cannot drive an unbounded whole-file read here.
        let mut file = tokio::fs::File::open(path).await.map_err(|e| {
            Error::corruption(format!("cannot open CRC.db at {}: {}", path.display(), e))
        })?;
        let mut header = [0u8; 4];
        file.read_exact(&mut header).await.map_err(|e| {
            Error::corruption(format!(
                "CRC.db at {} is too short for the 4-byte chunk-size header: {}",
                path.display(),
                e
            ))
        })?;
        let chunk_size_raw = i32::from_be_bytes(header);
        // Enforce the SAME chunk-size bounds as parse (min floor + max cap) so
        // open-time and read-time verification agree (issue #1396). The min floor
        // is what caps the derived `max_len` below — a tiny chunk size can no
        // longer inflate the per-Data.db entry count.
        let chunk_size = validate_chunk_size(chunk_size_raw)?;

        let actual_len = tokio::fs::metadata(path)
            .await
            .map(|m| m.len())
            .map_err(|e| {
                Error::corruption(format!("cannot stat CRC.db at {}: {}", path.display(), e))
            })?;

        // Absolute ceiling FIRST — independent of the (attacker-controlled)
        // header chunk_size and the declared Data.db length (issue #1396 Fix 2).
        // Even a well-formed header + honest Data.db size cannot authorize an
        // arbitrarily large sidecar read: 64 MiB of CRC entries already describes
        // a 1 TiB Data.db at Cassandra's 64 KiB chunk size.
        if actual_len > ABSOLUTE_CRC_DB_MAX {
            return Err(Error::corruption(format!(
                "CRC.db at {} is {actual_len} bytes — exceeds the absolute {ABSOLUTE_CRC_DB_MAX}-byte ceiling (enough CRC entries for a 1 TiB Data.db at a 64 KiB chunk size); refusing to read an oversized sidecar",
                path.display()
            )));
        }

        // Maximum plausible CRC.db length for a `data_len`-byte Data.db: the
        // 4-byte header + one 4-byte CRC per chunk + at most ONE trailing
        // compaction empty-final-chunk entry (issue #1222). All arithmetic is
        // saturating so an absurd `data_len`/`chunk_size` can never overflow.
        let n_chunks = data_len.div_ceil(chunk_size as u64);
        let max_len = 4u64.saturating_add(n_chunks.saturating_add(1).saturating_mul(4));
        if actual_len > max_len {
            return Err(Error::corruption(format!(
                "CRC.db at {} is {actual_len} bytes — exceeds the {max_len}-byte maximum implied by a {data_len}-byte Data.db (chunk_size={chunk_size}, {n_chunks} chunks + optional trailer); refusing to read an oversized sidecar",
                path.display()
            )));
        }

        // Size-validated: read the whole (now bounded) sidecar and parse.
        let bytes = tokio::fs::read(path).await.map_err(|e| {
            Error::corruption(format!("cannot read CRC.db at {}: {}", path.display(), e))
        })?;
        Self::parse(&bytes)
    }

    /// Chunk size recorded in the header (bytes per CRC-covered block).
    pub(crate) fn chunk_size(&self) -> u32 {
        self.chunk_size
    }

    /// Number of per-chunk CRC entries present (including any trailing compaction
    /// empty-final-chunk entry).
    pub(crate) fn chunk_count(&self) -> usize {
        self.crcs.len()
    }

    /// Stored CRC32 for chunk `chunk_index`.
    ///
    /// Returns a typed [`Error::Corruption`] when the entry is absent — i.e. the
    /// `CRC.db` is truncated / has fewer entries than the Data.db has chunks. This
    /// is only ever called for a chunk that covers real Data.db bytes, so a
    /// missing entry is genuine truncation, never the harmless trailing
    /// empty-final-chunk (issue #1222).
    pub(crate) fn crc_for_chunk(&self, chunk_index: usize) -> Result<u32> {
        self.crcs.get(chunk_index).copied().ok_or_else(|| {
            Error::corruption(format!(
                "CRC.db is truncated: no CRC32 entry for chunk {} (file position {}); it has only {} entries",
                chunk_index,
                chunk_index * 4 + 4,
                self.crcs.len()
            ))
        })
    }

    /// Stored CRC32 for the chunk containing Data.db byte `offset`
    /// (`chunk_index = offset / chunk_size`).
    #[cfg(test)]
    pub(crate) fn crc_for_offset(&self, offset: u64) -> Result<u32> {
        let chunk_index = (offset / self.chunk_size as u64) as usize;
        self.crc_for_chunk(chunk_index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // Writer round-trip helpers live in the write-support-gated `writer` module.
    #[cfg(feature = "write-support")]
    use crate::storage::sstable::writer::crc_writer::{write_crc_db, CrcTrailer, CRC_CHUNK_SIZE};

    fn synth_crc_db(chunk_size: u32, crcs: &[u32]) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(&(chunk_size as i32).to_be_bytes());
        for c in crcs {
            v.extend_from_slice(&c.to_be_bytes());
        }
        v
    }

    #[test]
    fn parses_header_and_entries() {
        let bytes = synth_crc_db(65536, &[0xf9e0_9e7f, 0x0000_0001, 0xdead_beef]);
        let crc = CrcDb::parse(&bytes).expect("parse");
        assert_eq!(crc.chunk_size(), 65536);
        assert_eq!(crc.chunk_count(), 3);
        assert_eq!(crc.crc_for_chunk(0).unwrap(), 0xf9e0_9e7f);
        assert_eq!(crc.crc_for_chunk(2).unwrap(), 0xdead_beef);
    }

    #[test]
    fn offset_maps_to_correct_chunk() {
        let cs = 64u32 * 1024;
        let bytes = synth_crc_db(cs, &[10, 20, 30]);
        let crc = CrcDb::parse(&bytes).expect("parse");
        // offset 0 -> chunk 0; offset cs -> chunk 1; offset 2*cs+5 -> chunk 2.
        assert_eq!(crc.crc_for_offset(0).unwrap(), 10);
        assert_eq!(crc.crc_for_offset(cs as u64 - 1).unwrap(), 10);
        assert_eq!(crc.crc_for_offset(cs as u64).unwrap(), 20);
        assert_eq!(crc.crc_for_offset(2 * cs as u64 + 5).unwrap(), 30);
    }

    #[test]
    fn missing_header_is_typed_error_not_panic() {
        for len in 0..4 {
            let err = CrcDb::parse(&vec![0u8; len]).unwrap_err();
            assert!(matches!(err, Error::Corruption(_)), "len {len}: {err}");
        }
    }

    #[test]
    fn non_positive_chunk_size_is_typed_error() {
        let bytes = synth_crc_db(0, &[]);
        assert!(matches!(CrcDb::parse(&bytes), Err(Error::Corruption(_))));
        // Negative (high bit set) chunk-size header.
        let neg = [0x80u8, 0, 0, 0];
        assert!(matches!(CrcDb::parse(&neg), Err(Error::Corruption(_))));
    }

    #[test]
    fn absurd_chunk_size_is_typed_error_not_oom() {
        // A sidecar advertising a chunk size far above MAX_CRC_CHUNK_SIZE (here
        // near i32::MAX) must be rejected as typed corruption at parse time —
        // BEFORE any `vec![0u8; chunk_size]` verification buffer is sized from
        // it — rather than attempting a multi-gigabyte allocation (issue #1396).
        let bytes = synth_crc_db(i32::MAX as u32, &[]);
        let err = CrcDb::parse(&bytes).expect_err("absurd chunk size must error");
        assert!(
            matches!(err, Error::Corruption(_)),
            "typed corruption: {err}"
        );
        assert!(
            err.to_string().contains("maximum"),
            "message names the maximum bound: {err}"
        );
        // Exactly at the cap parses; one byte over is rejected.
        assert!(CrcDb::parse(&synth_crc_db(MAX_CRC_CHUNK_SIZE, &[])).is_ok());
        assert!(matches!(
            CrcDb::parse(&synth_crc_db(MAX_CRC_CHUNK_SIZE + 1, &[])),
            Err(Error::Corruption(_))
        ));
    }

    /// Fix 2 (issue #1396): `CrcDb::open` must reject a `CRC.db` far larger than
    /// the Data.db size implies BEFORE reading its body into memory — otherwise a
    /// malformed / hostile sidecar forces an unbounded allocation at open. Here a
    /// well-formed-header CRC.db carrying vastly more per-chunk entries than a
    /// tiny Data.db can have is rejected as typed corruption naming the maximum.
    #[tokio::test]
    async fn oversized_crc_db_vs_data_len_is_typed_error_before_body_read() {
        let dir = tempfile::tempdir().expect("tempdir");
        let data = dir.path().join("Data.db");
        // A tiny Data.db: one short chunk at 64 KiB chunk size → at most 1 real
        // CRC entry (+ optional trailer) → max CRC.db length 12 bytes.
        tokio::fs::write(&data, b"tiny").await.expect("write data");
        let data_len = 4u64;

        // Craft a CRC.db with a valid 64 KiB header but 10_000 bogus CRC entries
        // (~40 KB) — far beyond the 12-byte maximum a 4-byte Data.db implies.
        let bogus: Vec<u32> = (0..10_000u32).collect();
        let crc_path = dir.path().join("CRC.db");
        tokio::fs::write(&crc_path, synth_crc_db(65536, &bogus))
            .await
            .expect("write oversized crc.db");

        let err = CrcDb::open(&crc_path, data_len)
            .await
            .expect_err("oversized CRC.db must be rejected");
        assert!(
            matches!(err, Error::Corruption(_)),
            "typed corruption: {err}"
        );
        assert!(
            err.to_string().contains("maximum") && err.to_string().contains("exceeds"),
            "error must name the derived maximum bound: {err}"
        );

        // A correctly-sized CRC.db for the same Data.db still opens fine.
        let ok_path = dir.path().join("CRC-ok.db");
        tokio::fs::write(&ok_path, synth_crc_db(65536, &[0xdead_beef]))
            .await
            .expect("write ok crc.db");
        CrcDb::open(&ok_path, data_len)
            .await
            .expect("correctly-sized CRC.db must open");
    }

    /// Fix 1 (issue #1396): a TINY chunk-size header (the residual OOM vector —
    /// e.g. `chunk_size = 1` makes the derived `max_len ≈ 4 * data_len`) is
    /// rejected as typed corruption BEFORE any large allocation, at BOTH parse
    /// and open, with the error naming the minimum-chunk-size bound.
    #[tokio::test]
    async fn tiny_chunk_size_is_rejected_before_large_alloc() {
        // parse: chunk_size = 1 is below the floor.
        let bytes = synth_crc_db(1, &[0xdead_beef]);
        let err = CrcDb::parse(&bytes).expect_err("chunk_size=1 must be rejected");
        assert!(
            matches!(err, Error::Corruption(_)),
            "typed corruption: {err}"
        );
        assert!(
            err.to_string().contains("minimum"),
            "message names the minimum-chunk-size bound: {err}"
        );

        // open: a header advertising chunk_size = 1 against a large declared
        // Data.db length must be rejected right after the 4-byte header read —
        // i.e. before `tokio::fs::read` buffers the (would-be multi-GiB) body.
        let dir = tempfile::tempdir().expect("tempdir");
        let crc_path = dir.path().join("CRC.db");
        tokio::fs::write(&crc_path, synth_crc_db(1, &[0, 1, 2]))
            .await
            .expect("write tiny-chunk crc.db");
        // ~4 GiB declared Data.db: with the old chunk_size=1 the derived bound
        // (~16 GiB) would have permitted an unbounded whole-file read.
        let huge_data_len = 4u64 * 1024 * 1024 * 1024;
        let err = CrcDb::open(&crc_path, huge_data_len)
            .await
            .expect_err("tiny chunk size must be rejected at open");
        assert!(
            matches!(err, Error::Corruption(_)),
            "typed corruption: {err}"
        );
        assert!(
            err.to_string().contains("minimum"),
            "open error must name the min-chunk-size bound: {err}"
        );

        // The floor value itself (4096) is accepted at parse.
        assert!(CrcDb::parse(&synth_crc_db(MIN_CRC_CHUNK_SIZE, &[])).is_ok());
        assert!(matches!(
            CrcDb::parse(&synth_crc_db(MIN_CRC_CHUNK_SIZE - 1, &[])),
            Err(Error::Corruption(_))
        ));
    }

    /// Fix 2 (issue #1396): a `CRC.db` whose on-disk length exceeds the ABSOLUTE
    /// 64 MiB ceiling is rejected as typed corruption before its body is read,
    /// independent of the (valid) header chunk_size and the declared Data.db size.
    /// Uses a sparse `set_len` so the oversized length is reported by metadata
    /// without allocating real disk blocks.
    #[tokio::test]
    async fn crc_db_over_absolute_ceiling_is_rejected_before_body_read() {
        use tokio::io::AsyncWriteExt;

        let dir = tempfile::tempdir().expect("tempdir");
        let crc_path = dir.path().join("CRC.db");
        {
            let mut f = tokio::fs::File::create(&crc_path).await.expect("create");
            // Valid 64 KiB-chunk header so validate_chunk_size passes.
            f.write_all(&65536i32.to_be_bytes())
                .await
                .expect("write header");
            f.flush().await.expect("flush");
            // Extend (sparsely) to just over the absolute ceiling.
            f.set_len(ABSOLUTE_CRC_DB_MAX + 4)
                .await
                .expect("set_len over ceiling");
        }
        // A huge declared Data.db so the DERIVED max_len does not fire first —
        // only the absolute ceiling should reject this.
        let huge_data_len = 100u64 * 1024 * 1024 * 1024 * 1024;
        let err = CrcDb::open(&crc_path, huge_data_len)
            .await
            .expect_err("over-ceiling CRC.db must be rejected");
        assert!(
            matches!(err, Error::Corruption(_)),
            "typed corruption: {err}"
        );
        assert!(
            err.to_string().contains("absolute"),
            "error must name the absolute ceiling: {err}"
        );
    }

    #[test]
    fn partial_trailing_crc_is_typed_error() {
        // header + 1 full CRC + 2 stray bytes.
        let mut bytes = synth_crc_db(65536, &[42]);
        bytes.extend_from_slice(&[0x01, 0x02]);
        assert!(matches!(CrcDb::parse(&bytes), Err(Error::Corruption(_))));
    }

    #[test]
    fn truncated_missing_entry_errors_for_covered_chunk() {
        // A CRC.db with only 1 entry queried for chunk 2 -> typed truncation error.
        let bytes = synth_crc_db(65536, &[7]);
        let crc = CrcDb::parse(&bytes).expect("parse");
        assert!(matches!(crc.crc_for_chunk(2), Err(Error::Corruption(_))));
    }

    /// Round-trip: the #1197 writer output parses back to identical values, and
    /// each recovered CRC32 equals `crc32fast` over the corresponding raw chunk.
    #[cfg(feature = "write-support")]
    #[tokio::test]
    async fn round_trips_the_writer_output_multi_chunk() {
        let dir = tempfile::tempdir().expect("tempdir");
        let data = dir.path().join("Data.db");
        // 2.5 chunks of deterministic bytes so there are 3 per-chunk CRCs.
        let len = CRC_CHUNK_SIZE * 2 + CRC_CHUNK_SIZE / 2;
        let payload: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
        tokio::fs::write(&data, &payload).await.expect("write data");

        let crc_path = dir.path().join("CRC.db");
        write_crc_db(&data, crc_path.clone(), CrcTrailer::None)
            .await
            .expect("write crc.db");

        let crc = CrcDb::open(&crc_path, len as u64)
            .await
            .expect("open crc.db");
        assert_eq!(crc.chunk_size() as usize, CRC_CHUNK_SIZE);
        assert_eq!(crc.chunk_count(), 3, "2.5 chunks -> 3 CRC entries");

        for (i, chunk) in payload.chunks(CRC_CHUNK_SIZE).enumerate() {
            let expected = crc32fast::hash(chunk);
            assert_eq!(
                crc.crc_for_chunk(i).unwrap(),
                expected,
                "chunk {i} CRC must match crc32fast over its raw bytes"
            );
        }
    }

    /// R1 (issue #1396): parse a REAL Cassandra-written `CRC.db` from an
    /// uncompressed BIG fixture and assert the parsed chunk-size header is
    /// Cassandra's 65536 AND every per-chunk CRC32 byte-agrees with the CRC32
    /// recomputed over the corresponding raw `chunk_size` block of that fixture's
    /// `Data.db`. This is the no-heuristics anchor — verification consumes the
    /// authoritative `CRC.db` bytes, not inferred-from-content values.
    ///
    /// Fixture-gated: skip-clean when the dataset binaries are absent.
    #[tokio::test]
    async fn parses_real_cassandra_crc_db_and_byte_agrees_with_data_db() {
        use std::path::PathBuf;

        // Locate the datasets root (CQLITE_DATASETS_ROOT or the repo's test-data).
        let root = std::env::var("CQLITE_DATASETS_ROOT")
            .ok()
            .map(PathBuf::from)
            .filter(|p| p.is_dir())
            .or_else(|| {
                let p = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .parent()?
                    .join("test-data/datasets");
                p.is_dir().then_some(p)
            });
        let Some(root) = root else {
            eprintln!("SKIP: no datasets root for the real CRC.db byte-agreement test.");
            return;
        };

        // Find any uncompressed_table-*/nb-1-big-CRC.db with its Data.db present
        // (Cassandra writes CRC.db for every uncompressed BIG SSTable).
        let mut found: Option<(PathBuf, PathBuf)> = None;
        for ks in ["test_basic", "test_comp"] {
            let base = root.join("sstables").join(ks);
            let Ok(rd) = std::fs::read_dir(&base) else {
                continue;
            };
            for entry in rd.flatten() {
                let name = entry.file_name();
                let Some(name) = name.to_str() else { continue };
                if !name.starts_with("uncompressed_table-") {
                    continue;
                }
                let crc = entry.path().join("nb-1-big-CRC.db");
                let data = entry.path().join("nb-1-big-Data.db");
                if crc.is_file() && data.is_file() {
                    found = Some((crc, data));
                    break;
                }
            }
            if found.is_some() {
                break;
            }
        }
        let Some((crc_path, data_path)) = found else {
            eprintln!(
                "SKIP: no Cassandra-written uncompressed_table CRC.db+Data.db fixture found."
            );
            return;
        };

        let data_len = std::fs::metadata(&data_path)
            .map(|m| m.len())
            .expect("stat Data.db");
        let crc = CrcDb::open(&crc_path, data_len)
            .await
            .expect("parse real CRC.db");
        assert_eq!(
            crc.chunk_size(),
            65536,
            "Cassandra uncompressed CRC.db chunk size must be 65536 (0x00010000)"
        );

        let data = tokio::fs::read(&data_path).await.expect("read Data.db");
        let cs = crc.chunk_size() as usize;
        let expected_chunks = data.len().div_ceil(cs);
        assert!(
            crc.chunk_count() >= expected_chunks,
            "CRC.db must have at least one entry per Data.db chunk ({} >= {})",
            crc.chunk_count(),
            expected_chunks
        );
        for (i, block) in data.chunks(cs).enumerate() {
            let recomputed = crc32fast::hash(block);
            assert_eq!(
                crc.crc_for_chunk(i).unwrap(),
                recomputed,
                "chunk {i}: stored CRC.db value must byte-agree with CRC32 over the raw Data.db chunk"
            );
        }
    }

    /// Compaction trailer (issue #1222) appends one trailing `0` entry; the reader
    /// exposes it as an extra entry that a real read never dereferences.
    #[cfg(feature = "write-support")]
    #[tokio::test]
    async fn compaction_trailer_is_extra_harmless_entry() {
        let dir = tempfile::tempdir().expect("tempdir");
        let data = dir.path().join("Data.db");
        let payload: &[u8] = b"one short chunk";
        tokio::fs::write(&data, payload).await.expect("write");
        let crc_path = dir.path().join("CRC.db");
        write_crc_db(&data, crc_path.clone(), CrcTrailer::EmptyFinalChunk)
            .await
            .expect("write");
        let crc = CrcDb::open(&crc_path, payload.len() as u64)
            .await
            .expect("open");
        // 1 real chunk + 1 trailing empty-final-chunk 0.
        assert_eq!(crc.chunk_count(), 2);
        assert_eq!(crc.crc_for_chunk(1).unwrap(), 0);
    }
}
