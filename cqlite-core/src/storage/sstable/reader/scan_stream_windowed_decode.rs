//! IO-half chunk decode for the windowed streaming scan (issue #1940, D2).
//!
//! Split out of `scan_stream_windowed.rs` to keep that source file under the
//! campsite-rule size limit (epic #1116). `decode_scan_chunk` is the one place the
//! windowed-scan IO half turns a freshly-read, CRC-verified compressed chunk into
//! the refcounted decompressed `Bytes` substrate shipped on the chunk channel,
//! while handing the compressed buffer back so the feed loop recycles it as the
//! next read's scratch (≤1 alloc/chunk). Included via
//! `#[path = "scan_stream_windowed_decode.rs"] impl`-carrying module in the parent.

use super::SSTableReader;
use crate::Result;

impl SSTableReader {
    /// Decode ONE compressed chunk on the IO half (issue #1940, D2): CRC was
    /// already verified inside the read path, so this does cache-lookup-or-decompress
    /// and returns the refcounted decompressed `Bytes` substrate shipped on the
    /// channel — while HANDING BACK the compressed buffer (`compressed`) so the feed
    /// loop can RECYCLE it as the next read's scratch (no per-chunk compressed-buffer
    /// allocation). The single decode plane (`chunk_source`) still owns the one
    /// decompress call. Two cases consume the buffer instead of handing it back
    /// (returning an empty scratch, so a fresh buffer is minted next read): an
    /// incompressible-raw chunk (stored uncompressed by Cassandra, compressed len
    /// ≥ `max_compressed_length`) is passed through zero-copy `Vec`→`Bytes`; and the
    /// no-compressor case (raw/uncompressed NB scan with no `CompressionInfo` — the
    /// read buffer already holds finished bytes) is MOVED into the B1 cache zero-copy
    /// via `Bytes::from(Vec)`, never `to_vec()`-copied (issue #1940 BLOCKER-1;
    /// uncompressed is CQLite's own write-surface format, a first-class path).
    /// `chunk_index` is the ABSOLUTE chunk index (the feed reads from data-section
    /// start in order), matching the prior parse-half keying.
    pub(super) fn decode_scan_chunk(
        &self,
        chunk_index: usize,
        max_compressed_length: usize,
        compressed: Vec<u8>,
    ) -> Result<(bytes::Bytes, Vec<u8>)> {
        use crate::storage::sstable::compression::Compression;

        // Incompressible-raw chunks (stored uncompressed by Cassandra) skip the
        // cache and are passed through zero-copy; the buffer is consumed.
        if compressed.len() >= max_compressed_length {
            return Ok((bytes::Bytes::from(compressed), Vec::new()));
        }
        let key = crate::storage::cache::ChunkKey::new(
            self.chunk_cache_id ^ super::super::data_access::NS_WINDOWED_CHUNK,
            chunk_index as u64,
        );
        // Cache hit → refcount-bump clone (B1 contract), never a memcpy; the
        // compressed buffer is recycled unused. Warm scans must take the hit rather
        // than re-decompress and overwrite (issue #1598 roborev Medium).
        if let Some(hit) = self.chunk_cache.get(&key) {
            return Ok((hit, compressed));
        }
        // No compressor (raw/uncompressed NB scan, no CompressionInfo): the read
        // buffer already holds the finished, uncompressed chunk bytes. MOVE it into
        // the B1 cache as zero-copy `Bytes` (`Bytes::from(Vec)` reuses the read
        // buffer's heap allocation) rather than `to_vec()`-copying it — this is
        // CQLite's own uncompressed write-surface format, a first-class path (issue
        // #1940 BLOCKER-1). Nothing to recycle on the raw path (the buffer became
        // the cached substrate), so the returned scratch is empty; a fresh read
        // buffer is minted for the next chunk. No decompress here, so the
        // decode-thread probe does NOT fire (it pins where decompression runs).
        if self.compression_reader.is_none() {
            return Ok((self.chunk_cache.insert(key, compressed), Vec::new()));
        }
        // Miss → decompress from the BORROWED slice (so we keep `compressed` to
        // recycle) into the single decode plane, cache the resident `Bytes`.
        let compression = self
            .compression_reader
            .as_ref()
            .map(|cr| Compression::new(*cr.algorithm()))
            .transpose()?;
        // Runtime-placement guard (issue #1940): record the thread this decode
        // actually decompresses on — placed at the REAL decompress site (past the
        // cache-hit / incompressible-raw / no-compressor early exits, none of which
        // decompress) so a guard test can prove decompression runs on a
        // spawn_blocking thread, NOT an async worker (the D2 substrate moved
        // decompression into the IO-half feed loop, which must stay off the reactor
        // for EVERY backend). Compiled only under `scan-offload-probe`.
        #[cfg(feature = "scan-offload-probe")]
        super::probe::record_decode_thread();
        let comp_info_dummy = crate::storage::sstable::compression_info::CompressionInfo {
            algorithm: String::new(),
            option_pairs: vec![],
            chunk_length: 0,
            max_compressed_length: max_compressed_length as u32,
            data_length: 0,
            chunk_offsets: vec![],
        };
        let chunk_source = super::super::chunk_source::ChunkSource::new(
            self.point_source.as_ref(), // unused by decode_borrowed
            &comp_info_dummy,
            compression.as_ref(),
            &self.chunk_cache,
            0, // unused
            0, // unused
            super::super::data_access::NS_WINDOWED_CHUNK,
            self.chunk_cache_id,
        );
        let decoded = chunk_source.decode_borrowed(key, &compressed)?;
        Ok((decoded, compressed))
    }
}
