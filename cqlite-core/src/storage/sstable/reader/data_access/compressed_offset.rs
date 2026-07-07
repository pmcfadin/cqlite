//! CRC-validated compressed offset-read window (issue #1773).
//!
//! The compressed offset-read point-lookup path
//! ([`read_value_at_offset`](super::super::SSTableReader::read_value_at_offset) →
//! `get_cached_data`) used to read `size` raw bytes at an offset and LZ4-decompress
//! them WITHOUT validating the trailing 4-byte inline per-chunk CRC32 — re-introducing
//! the #1411 CRC bypass on a latent path (unreachable today via `get`, but live the
//! moment `find_entry` hits for a compressed table). This module carries the helper
//! that routes that case through the shared CRC-enforcing chunk reader.

use std::sync::atomic::Ordering;

use crate::storage::sstable::reader::SSTableReader;
use crate::{Error, Result};

impl SSTableReader {
    /// Decompress the uncompressed byte window `[block_offset, block_offset + size)`
    /// out of a COMPRESSED Data.db, validating each covering chunk's authoritative
    /// inline per-chunk CRC32 BEFORE decompression (issue #1773).
    ///
    /// Reuses [`read_compressed_chunk_at`] — the positional, CRC-enforcing chunk
    /// reader also used by the BTI point-read path — so the CRC-then-decompress
    /// ordering (guardrail #1411) is honored on this path too: a bit-flipped chunk
    /// surfaces the SAME typed `Error::InvalidFormat` (naming the chunk index +
    /// offset) that scan / `scan_for_key` return, never garbage. No heuristics: the
    /// authoritative CRC trailer is checked, never inferred.
    ///
    /// [`read_compressed_chunk_at`]: super::super::block_io::read_compressed_chunk_at
    pub(super) async fn read_compressed_offset_window(
        &self,
        comp_info: &crate::storage::sstable::compression_info::CompressionInfo,
        block_offset: u64,
        size: u32,
    ) -> Result<Vec<u8>> {
        use super::super::block_io;
        use crate::storage::sstable::compression::Compression;

        let chunk_length = comp_info.chunk_length as usize;
        if chunk_length == 0 {
            return Err(Error::corruption(
                "CompressionInfo chunk_length is zero; cannot map a Data.db offset to a \
                 compressed chunk"
                    .to_string(),
            ));
        }

        let start = block_offset as usize;
        let end = start.checked_add(size as usize).ok_or_else(|| {
            Error::corruption(format!(
                "compressed offset read range [{start}, +{size}) overflows"
            ))
        })?;
        // `size >= 1` (callers reject size == 0), so `end > start` and `end - 1`
        // never underflows; `first_chunk <= last_chunk`.
        let first_chunk = start / chunk_length;
        let last_chunk = (end - 1) / chunk_length;

        let max_compressed_length = comp_info.max_compressed_length as usize;
        let compression = self
            .compression_reader
            .as_ref()
            .map(|r| Compression::new(*r.algorithm()))
            .transpose()?;

        let mut assembled = Vec::with_capacity(
            last_chunk.saturating_sub(first_chunk).saturating_add(1) * chunk_length,
        );
        for chunk_idx in first_chunk..=last_chunk {
            // CRC-validated compressed bytes (guardrail #1411): the trailing inline
            // per-chunk CRC32 is checked HERE, before decompression. header_offset is
            // 0 for nb/BTI (CompressionInfo chunk offsets are absolute from Data.db
            // byte 0), matching the cursor and BTI point-read paths (issue #1573 C2).
            let Some(compressed) = block_io::read_compressed_chunk_at(
                self.point_source.as_ref(),
                comp_info,
                chunk_idx,
                self.stats.file_size,
                0,
            )?
            else {
                break; // EOF — no further chunks to cover the requested window.
            };

            // Incompressible-chunk fallback (Bug #639, epic #970): Cassandra stores a
            // chunk RAW (not compressed) when its compressed length would meet or
            // exceed `max_compressed_length`; those bytes are already plaintext.
            // Authority: CompressedSequentialWriter.java:160-177.
            let decompressed = if compressed.len() >= max_compressed_length {
                compressed
            } else if let Some(compression) = &compression {
                let out = compression.decompress(&compressed)?;
                super::model::DECOMPRESS_CALLS.fetch_add(1, Ordering::Relaxed);
                out
            } else {
                compressed
            };
            assembled.extend_from_slice(&decompressed);
        }

        // Slice the requested window out of the assembled uncompressed chunk bytes,
        // clamped to what actually decoded (a short final chunk yields fewer bytes).
        let window_base = first_chunk * chunk_length;
        let within_start = start - window_base;
        if within_start >= assembled.len() {
            return Ok(Vec::new());
        }
        let within_end = (end - window_base).min(assembled.len());
        Ok(assembled[within_start..within_end].to_vec())
    }
}
