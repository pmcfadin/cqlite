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
        if chunk_size_raw <= 0 {
            return Err(Error::corruption(format!(
                "CRC.db chunk-size header is non-positive ({chunk_size_raw}); expected a positive block size (Cassandra default 65536)"
            )));
        }
        let chunk_size = chunk_size_raw as u32;
        if chunk_size > MAX_CRC_CHUNK_SIZE {
            // Bound the header before any downstream `vec![0u8; chunk_size]`
            // verification buffer is sized from it (issue #1396: OOM guard).
            return Err(Error::corruption(format!(
                "CRC.db chunk-size header is {chunk_size} bytes — exceeds the {MAX_CRC_CHUNK_SIZE}-byte maximum (Cassandra default 65536); refusing to allocate an unbounded verification buffer"
            )));
        }

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

    /// Read and parse a `CRC.db` from disk.
    pub(crate) async fn open(path: &Path) -> Result<Self> {
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

        let crc = CrcDb::open(&crc_path).await.expect("open crc.db");
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

        let crc = CrcDb::open(&crc_path).await.expect("parse real CRC.db");
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
    #[tokio::test]
    async fn compaction_trailer_is_extra_harmless_entry() {
        let dir = tempfile::tempdir().expect("tempdir");
        let data = dir.path().join("Data.db");
        tokio::fs::write(&data, b"one short chunk")
            .await
            .expect("write");
        let crc_path = dir.path().join("CRC.db");
        write_crc_db(&data, crc_path.clone(), CrcTrailer::EmptyFinalChunk)
            .await
            .expect("write");
        let crc = CrcDb::open(&crc_path).await.expect("open");
        // 1 real chunk + 1 trailing empty-final-chunk 0.
        assert_eq!(crc.chunk_count(), 2);
        assert_eq!(crc.crc_for_chunk(1).unwrap(), 0);
    }
}
