//! CRC-validated compressed offset-read window (issue #1773).
//!
//! The compressed offset-read path
//! ([`read_value_at_offset`](super::super::SSTableReader::read_value_at_offset) →
//! `get_cached_data`, and the Summary-guided compressed scan walk in
//! `summary_scan.rs`) used to read `size` raw bytes at an offset and LZ4-decompress
//! them WITHOUT validating the trailing 4-byte inline per-chunk CRC32 — re-introducing
//! the #1411 CRC bypass on a latent path (unreachable today via `get`, but live the
//! moment `find_entry` hits for a compressed table). This module carries the helper
//! that routes that case through the shared CRC-enforcing chunk reader.
//!
//! The positional plane is the CALLER's, never hardcoded here (issue #2876): this
//! helper serves BOTH read intents, so each caller passes the source its intent
//! selects. The scan-shaped walks (`summary_scan.rs`'s Summary-guided partition
//! walk, `full_index_scan.rs` / `full_index_stream.rs`'s full-`Index.db`
//! enumeration, and the windowed scan) pass the reader's UNADVISED
//! `scan_positional_source`, because they read Data.db largely sequentially and the
//! advised mapping's readahead suppression is exactly backwards for them (the
//! #2210 × #1940 cross-path regression). A genuine point lookup — `get_cached_data`
//! reached from `read_value_at_offset` — passes the dedicated `MADV_RANDOM`
//! `point_source`, keeping the advice issue #2210 gave it. (`bti_point.rs` /
//! `big_promoted.rs` read `point_source` directly and do not route through here.)

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
    /// `source` is the positional plane the CALLER's read intent selects (issue
    /// #2876) — see the module doc. It is a parameter rather than a field read so
    /// that this one helper can serve the scan walks and the point offset read
    /// without either losing its intended mapping advice.
    ///
    /// [`read_compressed_chunk_at`]: super::super::block_io::read_compressed_chunk_at
    pub(in crate::storage::sstable::reader) async fn read_compressed_offset_window(
        &self,
        source: &dyn super::super::read_at::ReadAt,
        comp_info: &crate::storage::sstable::compression_info::CompressionInfo,
        block_offset: u64,
        size: u32,
    ) -> Result<Vec<u8>> {
        use crate::storage::sstable::compression::Compression;

        let compression = self
            .compression_reader
            .as_ref()
            .map(|r| Compression::new(*r.algorithm()))
            .transpose()?;

        read_compressed_offset_window_impl(
            source,
            comp_info,
            compression.as_ref(),
            self.stats.file_size,
            block_offset,
            size,
        )
    }
}

/// Decompression core of [`SSTableReader::read_compressed_offset_window`], factored
/// out of the reader method so it can be exercised directly against a hand-built
/// [`CompressionInfo`] + `ReadAt` source (no reader/SQL roundtrip) — see the
/// `max_compressed_length == 0` regression test below.
///
/// [`CompressionInfo`]: crate::storage::sstable::compression_info::CompressionInfo
pub(super) fn read_compressed_offset_window_impl(
    positional_source: &dyn super::super::read_at::ReadAt,
    comp_info: &crate::storage::sstable::compression_info::CompressionInfo,
    compression: Option<&crate::storage::sstable::compression::Compression>,
    file_size: u64,
    block_offset: u64,
    size: u32,
) -> Result<Vec<u8>> {
    use super::super::block_io;

    let chunk_length = comp_info.chunk_length as usize;
    if chunk_length == 0 {
        return Err(Error::corruption(
            "CompressionInfo chunk_length is zero; cannot map a Data.db offset to a \
             compressed chunk"
                .to_string(),
        ));
    }

    // Fail CLOSED on a corrupt `CompressionInfo` whose `max_compressed_length == 0`
    // (issue #2524, mirroring the #1869 fix in the sibling `compressed_partition_window`):
    // otherwise the raw-chunk fallback below (`compressed.len() >= max_compressed_length`)
    // is ALWAYS true, so EVERY still-LZ4-compressed chunk would be returned verbatim as
    // "raw/incompressible" plaintext — never decompressed — and the inline CRC32 (computed
    // over the genuine on-disk bytes) would still pass, silently handing back garbage rows.
    // A valid Cassandra `CompressionInfo` never records a zero `max_compressed_length`
    // (it is `i32::MAX` when minCompressRatio=0, the default; CompressionParams.java:186-189).
    let max_compressed_length = comp_info.max_compressed_length as usize;
    if max_compressed_length == 0 {
        return Err(Error::corruption(
            "compressed offset read: CompressionInfo max_compressed_length is zero; \
             cannot distinguish compressed chunks from raw/incompressible ones"
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

    let mut assembled =
        Vec::with_capacity(last_chunk.saturating_sub(first_chunk).saturating_add(1) * chunk_length);
    for chunk_idx in first_chunk..=last_chunk {
        // CRC-validated compressed bytes (guardrail #1411): the trailing inline
        // per-chunk CRC32 is checked HERE, before decompression. header_offset is
        // 0 for nb/BTI (CompressionInfo chunk offsets are absolute from Data.db
        // byte 0), matching the cursor and BTI point-read paths (issue #1573 C2).
        //
        // Fail CLOSED (issue #1773 roborev): for a VALID offset+size every chunk
        // in `first_chunk..=last_chunk` exists, so a `None` (EOF) here means the
        // requested range does not actually fit in this compressed Data.db — a
        // corrupt/out-of-range offset or size. Returning the partial `assembled`
        // bytes as `Ok` would silently truncate; this must surface as a typed,
        // non-recoverable corruption error instead.
        // Issue #2819: attribute the synchronous body-chunk page-in (positional
        // read + CRC) to the `stream_cold_fault` sub-phase on the flight
        // per-request sink (no-op when no sink is installed — every non-flight
        // caller). This wraps ONLY the read, on the per-SSTable scan (producer)
        // thread; it shares no code interval with the egress `stream_grpc_write`
        // scope, which is measured on the merge/egress thread in
        // `cqlite-flight`'s `ChannelSink::emit` — so a slow client's send-park can
        // never inflate cold-fault.
        let chunk = crate::observability::stream_subphase::timed(
            crate::observability::StreamSubPhase::ColdFault,
            || {
                block_io::read_compressed_chunk_at(
                    positional_source,
                    comp_info,
                    chunk_idx,
                    file_size,
                    0,
                )
            },
        )?;
        let Some(compressed) = chunk else {
            return Err(Error::corruption(format!(
                "compressed offset read requires chunk {chunk_idx} past EOF for range \
                 [{start}, {end}) — corrupt or out-of-range offset/size"
            )));
        };

        // Incompressible-chunk fallback (Bug #639, epic #970): Cassandra stores a
        // chunk RAW (not compressed) when its compressed length would meet or
        // exceed `max_compressed_length`; those bytes are already plaintext.
        // Authority: CompressedSequentialWriter.java:160-177.
        let decompressed = if compressed.len() >= max_compressed_length {
            compressed
        } else if let Some(compression) = compression {
            // Single decode plane (issue #1598, G2): the actual decompress call
            // resolves inside `ChunkSource::decompress_only`, so this module holds
            // zero query-path decompress call sites (the architecture test proves
            // exactly one such module). CRC is already validated above by
            // `read_compressed_chunk_at` (guardrail #1411), so we never decode bytes
            // that failed their inline CRC32.
            // Issue #2819: attribute the LZ4 decompress to the `stream_decompress`
            // sub-phase (no-op when no sink is installed). Reached only for a
            // genuinely compressed chunk (past the incompressible-raw fallback
            // above), so an uncompressed table records NO `stream_decompress`.
            let out = crate::observability::stream_subphase::timed(
                crate::observability::StreamSubPhase::Decompress,
                || {
                    super::super::chunk_source::ChunkSource::decompress_only(
                        Some(compression),
                        compressed,
                    )
                },
            )?;
            super::model::DECOMPRESS_CALLS.fetch_add(1, Ordering::Relaxed);
            out
        } else {
            // No compression reader (should not happen: this path is only reached
            // for a compressed Data.db, which always carries one). Mirror
            // `stitch_all_chunks`' warn! (issue #2167) so an unexpected reach —
            // returning still-compressed bytes as if plaintext — is visible in logs.
            tracing::warn!(
                "read_compressed_offset_window: No compression reader, using raw chunk data"
            );
            compressed
        };
        assembled.extend_from_slice(&decompressed);
    }

    // Slice the requested window out of the assembled uncompressed chunk bytes.
    //
    // Fail CLOSED (issue #1773 roborev): for a VALID offset+size the assembled
    // chunks fully cover `[start, end)`. If they do not — a short final chunk, or
    // `within_start` itself past what decoded — the offset/size is corrupt or
    // out-of-range; return a typed corruption error rather than truncating to
    // `Ok(partial)` / `Ok(empty)`.
    let window_base = first_chunk * chunk_length;
    let within_start = start - window_base;
    let requested_end = end - window_base;
    if requested_end > assembled.len() {
        return Err(Error::corruption(format!(
            "compressed offset read range [{start}, {end}) is short by {} bytes after \
             decompressing chunks {first_chunk}..={last_chunk} (assembled {} bytes) — \
             corrupt or out-of-range offset/size",
            requested_end - assembled.len(),
            assembled.len()
        )));
    }
    Ok(assembled[within_start..requested_end].to_vec())
}

#[cfg(test)]
mod tests {
    use crate::storage::sstable::compression_info::CompressionInfo;
    use crate::storage::sstable::reader::read_at::ReadAt;
    use crate::Error;

    /// Minimal `ReadAt` double. The `max_compressed_length == 0` guard fires BEFORE
    /// any chunk read, so `read_at` is never reached — it exists only to satisfy the
    /// signature (and panics if the guard ever regresses to touch the source).
    struct NeverReadAt;
    impl ReadAt for NeverReadAt {
        fn read_at(&self, _offset: u64, _buf: &mut [u8]) -> crate::Result<usize> {
            panic!(
                "read_at must not be reached: the max_compressed_length==0 guard fails closed \
                    before any chunk I/O"
            );
        }
        fn len(&self) -> u64 {
            0
        }
    }

    /// A hand-built `CompressionInfo` with `max_compressed_length` corrupted to `0`.
    /// One 64-byte chunk; the chunk_offsets/data_length are immaterial because the
    /// guard rejects the metadata before mapping any offset to a chunk.
    fn comp_info_zero_max() -> CompressionInfo {
        CompressionInfo {
            algorithm: "LZ4Compressor".to_string(),
            option_pairs: Vec::new(),
            chunk_length: 64,
            max_compressed_length: 0, // corrupt: a valid CompressionInfo never records 0
            data_length: 64,
            chunk_offsets: vec![0],
        }
    }

    /// Issue #2524 — a corrupt/malformed `CompressionInfo` whose `max_compressed_length
    /// == 0` MUST fail closed with a typed `Error::Corruption`, never silently return
    /// still-compressed bytes as plaintext. Pre-fix, the raw-chunk fallback test
    /// `compressed.len() >= max_compressed_length` was `>= 0` — ALWAYS true — so every
    /// still-LZ4-compressed chunk was returned verbatim as "raw" plaintext, and the
    /// inline CRC32 (over the genuine on-disk bytes) still matched, failing OPEN.
    /// Mirrors the #1869 fix + test for the sibling `compressed_partition_window`.
    #[test]
    fn zero_max_compressed_length_fails_closed() {
        let ci = comp_info_zero_max();
        let src = NeverReadAt;

        let err = super::read_compressed_offset_window_impl(&src, &ci, None, 0, 0, 16).expect_err(
            "CompressionInfo.max_compressed_length == 0 must fail closed with a typed \
                 corruption error, not silently return still-compressed bytes as plaintext",
        );
        match err {
            Error::Corruption(m) => assert!(
                m.contains("max_compressed_length is zero"),
                "unexpected corruption text: {m}"
            ),
            other => panic!("expected Corruption(max_compressed_length zero), got {other:?}"),
        }
    }
}
