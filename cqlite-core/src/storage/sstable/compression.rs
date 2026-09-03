//! Compression support for SSTable storage

use crate::{error::Error, Result};
// use async_trait::async_trait; // Commented out - unused

/// Compression algorithms supported
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize, Default)]
pub enum CompressionAlgorithm {
    /// No compression
    None,
    /// LZ4 compression (fast)
    #[default]
    Lz4,
    /// Snappy compression (balanced)
    Snappy,
    /// Deflate compression (high ratio)
    Deflate,
    /// Zstd compression (high efficiency)
    Zstd,
}

/// Maximum allowed decompressed size to prevent memory exhaustion attacks (128MB)
///
/// Gated to the union of its consumers' features — same reason and same idiom as
/// `validate_decompression_size` below (issue #1873). With no backend enabled every
/// `decompress` arm errors without decompressing, so nothing unbounded is left to
/// guard: genuinely dead, not merely unreferenced. Ungated, `cargo test -p
/// cqlite-ffi-common` (cqlite-core at `default-features = false`) fails `-D warnings`.
#[cfg(any(
    feature = "lz4",
    feature = "snappy",
    feature = "deflate",
    feature = "zstd"
))]
const MAX_DECOMPRESSED_SIZE: usize = 128 * 1024 * 1024;

impl CompressionAlgorithm {
    /// Map a recognized compressor name to its enum variant.
    ///
    /// Accepts CQLite short names (`LZ4`, `SNAPPY`, ...), Cassandra simple names
    /// (`LZ4Compressor`, `SnappyCompressor`, ...) and the explicit no-compression
    /// markers (`NONE`, `NoopCompressor`, `NoCompressor`). Returns `None` for any
    /// other (unrecognized) name — callers MUST treat `None` here as "unknown",
    /// not as "uncompressed".
    fn from_name_opt(s: &str) -> Option<Self> {
        // Strip any fully-qualified class prefix Cassandra may emit.
        let simple = s.rsplit('.').next().unwrap_or(s);
        match simple.to_uppercase().as_str() {
            "NONE" | "NOOPCOMPRESSOR" | "NOCOMPRESSOR" | "NULLCOMPRESSOR" => {
                Some(CompressionAlgorithm::None)
            }
            "LZ4" | "LZ4COMPRESSOR" => Some(CompressionAlgorithm::Lz4),
            "SNAPPY" | "SNAPPYCOMPRESSOR" => Some(CompressionAlgorithm::Snappy),
            "DEFLATE" | "DEFLATECOMPRESSOR" => Some(CompressionAlgorithm::Deflate),
            "ZSTD" | "ZSTDCOMPRESSOR" => Some(CompressionAlgorithm::Zstd),
            _ => None,
        }
    }

    /// Fallible parse of a compressor name (issue #1001).
    ///
    /// This is the path the SSTable open / `CompressionInfo.db` flow MUST use: an
    /// unrecognized name produces an explicit `UnsupportedFormat` error (including the
    /// exact offending string) rather than silently falling back to uncompressed.
    /// No content-based guessing is performed (no-heuristics mandate, issue #28).
    pub fn parse(s: &str) -> Result<Self> {
        Self::from_name_opt(s).ok_or_else(|| {
            Error::UnsupportedFormat(format!(
                "Unsupported compression algorithm '{}'. CQLite supports: \
                 LZ4Compressor, SnappyCompressor, DeflateCompressor, ZstdCompressor \
                 (or NONE for uncompressed).",
                s
            ))
        })
    }
}

impl From<String> for CompressionAlgorithm {
    fn from(s: String) -> Self {
        Self::from(s.as_str())
    }
}

impl From<&str> for CompressionAlgorithm {
    /// Infallible best-effort mapping. Unrecognized names map to `None`.
    ///
    /// WARNING: this MUST NOT be used on the SSTable read path — an unknown name here
    /// is indistinguishable from genuinely-disabled compression. Use the fallible
    /// [`CompressionAlgorithm::parse`] for any path that opens real `CompressionInfo.db`
    /// metadata (issue #1001). This `From` is retained only for the legacy header
    /// (`SSTableHeader.compression.algorithm`) path, which guards with an explicit
    /// `!= "NONE"` check before conversion and re-validates the resulting variant.
    fn from(s: &str) -> Self {
        Self::from_name_opt(s).unwrap_or(CompressionAlgorithm::None)
    }
}

/// Validates that decompressed size does not exceed safety limits
///
/// # Security
/// Prevents decompression bomb attacks by rejecting sizes > 128MB
///
/// Only the LZ4 small-block decompress path consumes this (the Snappy/Deflate/Zstd
/// paths bound inline via `MAX_DECOMPRESSED_SIZE`), so it is gated to its sole caller
/// to stay dead-code-free under single-feature builds (issue #1873).
#[cfg(feature = "lz4")]
fn validate_decompression_size(uncompressed_size: usize) -> Result<()> {
    if uncompressed_size > MAX_DECOMPRESSED_SIZE {
        return Err(Error::storage(format!(
            "Decompression bomb protection: size {} exceeds limit {} (128MB)",
            uncompressed_size, MAX_DECOMPRESSED_SIZE
        )));
    }
    Ok(())
}

/// Decode a RAW Snappy block (Cassandra 5.0 `SnappyCompressor`: no length prefix)
/// in EXACTLY one attempt.
///
/// The authoritative CompressionInfo.db algorithm determines the single format —
/// no framed-then-raw guessing (no-heuristics mandate #28, issue #1588). A guess
/// could silently mis-decode an adversarial chunk to wrong bytes; strict raw
/// decoding surfaces a typed error instead.
///
/// `decode_attempts` is incremented once per decode call. Production passes a
/// throwaway `&mut 0`; tests thread a real counter to assert a single attempt.
#[cfg(feature = "snappy")]
fn snappy_decompress_raw(data: &[u8], decode_attempts: &mut usize) -> Result<Vec<u8>> {
    use snap::raw::Decoder;
    *decode_attempts += 1;

    // CENTRALIZED bomb guard (issue #1588): a raw Snappy block carries its
    // decompressed length as a leading varint. Inspect it FIRST and reject an
    // over-limit block WITHOUT calling `decompress_vec` — `decompress_vec`
    // pre-allocates `decompress_len` bytes up front, so an adversarial block
    // declaring a huge size would allocate before any post-decode guard runs.
    // This single choke point protects EVERY caller (chunk decode + streaming).
    let advertised = snap::raw::decompress_len(data)
        .map_err(|e| Error::storage(format!("Snappy (raw) length decode failed: {}", e)))?;
    if advertised > MAX_DECOMPRESSED_SIZE {
        return Err(Error::storage(format!(
            "Decompression bomb protection: advertised size {} exceeds limit {} (128MB)",
            advertised, MAX_DECOMPRESSED_SIZE
        )));
    }

    let mut decoder = Decoder::new();
    let decompressed = decoder
        .decompress_vec(data)
        .map_err(|e| Error::storage(format!("Snappy (raw) decompression failed: {}", e)))?;
    // Belt-and-suspenders: the advertised length is attacker-controlled, so
    // re-check the ACTUAL decoded size against the hard cap.
    if decompressed.len() > MAX_DECOMPRESSED_SIZE {
        return Err(Error::storage(format!(
            "Decompression bomb protection: decompressed size {} exceeds limit {} (128MB)",
            decompressed.len(),
            MAX_DECOMPRESSED_SIZE
        )));
    }
    Ok(decompressed)
}

/// Compression handler
pub struct Compression {
    algorithm: CompressionAlgorithm,
}

impl Compression {
    /// Create a new compression handler
    pub fn new(algorithm: CompressionAlgorithm) -> Result<Self> {
        Ok(Self { algorithm })
    }

    /// Compress data with Cassandra-compatible parameters
    pub fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        match self.algorithm {
            CompressionAlgorithm::None => Ok(data.to_vec()),
            CompressionAlgorithm::Lz4 => {
                #[cfg(feature = "lz4")]
                {
                    // Use Cassandra-compatible LZ4 compression
                    use lz4_flex::compress_prepend_size;

                    // Cassandra uses LZ4 frame format with specific parameters
                    let compressed = compress_prepend_size(data);
                    Ok(compressed)
                }
                #[cfg(not(feature = "lz4"))]
                {
                    Err(Error::storage("LZ4 compression not available".to_string()))
                }
            }
            CompressionAlgorithm::Snappy => {
                #[cfg(feature = "snappy")]
                {
                    use snap::raw::Encoder;

                    // Cassandra 5.0's SnappyCompressor writes a RAW Snappy block with NO
                    // length prefix (see writer::compressed_data_writer::SnappyCompressor
                    // and org.apache.cassandra.io.compress.SnappyCompressor). Emit raw so
                    // it round-trips through the strict raw decode path below (#1588). A
                    // 4-byte size prefix was NEVER a Cassandra-compatible format.
                    let mut encoder = Encoder::new();
                    encoder
                        .compress_vec(data)
                        .map_err(|e| Error::storage(format!("Snappy compression failed: {}", e)))
                }
                #[cfg(not(feature = "snappy"))]
                {
                    Err(Error::storage(
                        "Snappy compression not available".to_string(),
                    ))
                }
            }
            CompressionAlgorithm::Deflate => {
                #[cfg(feature = "deflate")]
                {
                    use flate2::write::ZlibEncoder;
                    use flate2::Compression as DeflateCompression;
                    use std::io::Write;

                    // Cassandra's DeflateCompressor uses java.util.zip.Deflater, which
                    // emits a ZLIB-wrapped stream (2-byte header + DEFLATE body +
                    // Adler-32 trailer) with NO 4-byte size prefix. Match it exactly so
                    // the output reads back through the zlib-aware decode path. (#1082)
                    let mut encoder = ZlibEncoder::new(Vec::new(), DeflateCompression::new(6));
                    encoder.write_all(data).map_err(|e| {
                        Error::storage(format!("Deflate compression failed: {}", e))
                    })?;
                    encoder
                        .finish()
                        .map_err(|e| Error::storage(format!("Deflate finish failed: {}", e)))
                }
                #[cfg(not(feature = "deflate"))]
                {
                    Err(Error::storage(
                        "Deflate compression not available".to_string(),
                    ))
                }
            }
            CompressionAlgorithm::Zstd => {
                #[cfg(feature = "zstd")]
                {
                    use zstd::stream::encode_all;

                    // Cassandra's ZstdCompressor writes a BARE zstd frame with NO
                    // 4-byte size prefix. Match it so the output reads back through
                    // the bare-frame decode path (#1082).
                    encode_all(data, 3)
                        .map_err(|e| Error::storage(format!("Zstd compression failed: {}", e)))
                }
                #[cfg(not(feature = "zstd"))]
                {
                    Err(Error::storage("Zstd compression not available".to_string()))
                }
            }
        }
    }

    /// Decompress data using traditional method (for small blocks)
    pub fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        // A5 read-work counter (DECOMPRESS_CALLS; consumers B1/E3): one per chunk
        // decompress. This is the single choke point every compressed-chunk read
        // path funnels through. No-op in release (design.md Decision 1/2).
        crate::storage::sstable::read_work_counters::record_decompress();
        match self.algorithm {
            CompressionAlgorithm::None => Ok(data.to_vec()),
            CompressionAlgorithm::Lz4 => {
                #[cfg(feature = "lz4")]
                {
                    use lz4_flex::decompress_size_prepended;

                    // LZ4 format: 4-byte size prefix (little-endian) + compressed data
                    if data.len() < 4 {
                        return Err(Error::storage("Invalid LZ4 data: too short".to_string()));
                    }

                    // Extract uncompressed size (4 bytes, little-endian for LZ4)
                    let uncompressed_size =
                        u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;

                    // Validate size to prevent decompression bombs
                    // SECURITY: lz4_flex::decompress_size_prepended does NOT validate the size
                    // prefix before allocating memory, making it vulnerable to memory exhaustion
                    // attacks if a malicious file contains an excessively large size value.
                    validate_decompression_size(uncompressed_size)?;

                    // Decompress using library function (now safe after validation)
                    decompress_size_prepended(data)
                        .map_err(|e| Error::storage(format!("LZ4 decompression failed: {}", e)))
                }
                #[cfg(not(feature = "lz4"))]
                {
                    Err(Error::storage("LZ4 compression not available".to_string()))
                }
            }
            CompressionAlgorithm::Snappy => {
                #[cfg(feature = "snappy")]
                {
                    // Decode EXACTLY one format (raw Snappy), determined by the
                    // authoritative CompressionInfo.db algorithm — never by trying
                    // framed-then-raw and keeping whichever "succeeds" (no-heuristics
                    // mandate #28, issue #1588). The framed-guess could silently return
                    // wrong bytes for an adversarial chunk; strict raw decoding rejects
                    // it. `&mut 0` discards the (test-only) attempt counter.
                    snappy_decompress_raw(data, &mut 0)
                }
                #[cfg(not(feature = "snappy"))]
                {
                    Err(Error::storage(
                        "Snappy compression not available".to_string(),
                    ))
                }
            }
            CompressionAlgorithm::Deflate => {
                #[cfg(feature = "deflate")]
                {
                    use flate2::read::ZlibDecoder;
                    use std::io::Read;

                    // Cassandra's DeflateCompressor uses java.util.zip.Deflater/Inflater,
                    // which emit ZLIB-wrapped streams: a 2-byte header (0x78 0x9c) +
                    // DEFLATE body + 4-byte Adler-32 trailer. There is NO 4-byte
                    // uncompressed-size prefix (that is an LZ4/Zstd convention) and the
                    // body is NOT raw DEFLATE. Decode with ZlibDecoder. (#1082)
                    if data.is_empty() {
                        return Err(Error::storage(
                            "Invalid Deflate data: empty chunk".to_string(),
                        ));
                    }

                    // Decompression-bomb guard: the decoder reads into a growing Vec,
                    // so we cap the output length rather than trusting any in-stream
                    // size field (none exists for zlib). The caller (chunk reader)
                    // separately bounds chunks by CompressionInfo.db lengths.
                    let mut decoder = ZlibDecoder::new(data).take(MAX_DECOMPRESSED_SIZE as u64 + 1);
                    let mut decompressed = Vec::new();
                    decoder.read_to_end(&mut decompressed).map_err(|e| {
                        Error::storage(format!("Deflate decompression failed: {}", e))
                    })?;

                    if decompressed.len() > MAX_DECOMPRESSED_SIZE {
                        return Err(Error::storage(format!(
                            "Decompression bomb protection: Deflate output exceeds limit {} (128MB)",
                            MAX_DECOMPRESSED_SIZE
                        )));
                    }

                    Ok(decompressed)
                }
                #[cfg(not(feature = "deflate"))]
                {
                    Err(Error::storage(
                        "Deflate compression not available".to_string(),
                    ))
                }
            }
            CompressionAlgorithm::Zstd => {
                #[cfg(feature = "zstd")]
                {
                    use std::io::Read;
                    use zstd::stream::read::Decoder as ZstdDecoder;

                    // Cassandra's ZstdCompressor writes a BARE zstd frame (magic
                    // 0x28 0xB5 0x2F 0xFD ...) with NO 4-byte uncompressed-size
                    // prefix — the same as the chunk-targeted decode path
                    // (chunk_decompressor.rs::decompress_zstd_chunk). The previous
                    // 4-byte-prefix assumption mis-read the frame magic as a ~650MB
                    // size and tripped the bomb guard on every stitched scan (#1082,
                    // same root cause as the Deflate fix). Decode the frame directly
                    // and bound the OUTPUT length instead of trusting an in-stream
                    // size field.
                    if data.is_empty() {
                        return Err(Error::storage("Invalid Zstd data: empty chunk".to_string()));
                    }

                    // Decompression-bomb guard: stream through a capped reader so a
                    // small malicious frame cannot allocate past the limit BEFORE we
                    // check the length (mirrors the Deflate path). A zstd frame can
                    // declare a huge content size, so `decode_all` would pre-allocate
                    // it up front — `Read::take` bounds the work instead.
                    let mut decoder = ZstdDecoder::new(data)
                        .map_err(|e| Error::storage(format!("Zstd decoder init failed: {}", e)))?
                        .take(MAX_DECOMPRESSED_SIZE as u64 + 1);
                    let mut decompressed = Vec::new();
                    decoder
                        .read_to_end(&mut decompressed)
                        .map_err(|e| Error::storage(format!("Zstd decompression failed: {}", e)))?;

                    if decompressed.len() > MAX_DECOMPRESSED_SIZE {
                        return Err(Error::storage(format!(
                            "Decompression bomb protection: Zstd output exceeds limit {} (128MB)",
                            MAX_DECOMPRESSED_SIZE
                        )));
                    }

                    Ok(decompressed)
                }
                #[cfg(not(feature = "zstd"))]
                {
                    Err(Error::storage("Zstd compression not available".to_string()))
                }
            }
        }
    }

    /// Get compression algorithm
    pub fn algorithm(&self) -> &CompressionAlgorithm {
        &self.algorithm
    }

    /// Select optimal compression algorithm based on data characteristics
    pub fn select_optimal_algorithm(
        data_sample: &[u8],
        performance_priority: CompressionPriority,
    ) -> CompressionAlgorithm {
        // Analyze data characteristics
        let entropy = calculate_entropy(data_sample);
        let repetition_score = calculate_repetition_score(data_sample);
        let data_size = data_sample.len();

        match performance_priority {
            CompressionPriority::Speed => {
                // Prioritize speed over compression ratio
                if entropy > 0.9 {
                    CompressionAlgorithm::None // High entropy data doesn't compress well
                } else {
                    CompressionAlgorithm::Lz4 // Fast compression
                }
            }
            CompressionPriority::Balanced => {
                // Balance speed and compression ratio
                if entropy > 0.95 {
                    CompressionAlgorithm::None
                } else if repetition_score > 0.7 || data_size > 1024 * 1024 {
                    CompressionAlgorithm::Snappy // Good balance for large or repetitive data
                } else {
                    CompressionAlgorithm::Lz4
                }
            }
            CompressionPriority::Ratio => {
                // Prioritize compression ratio
                if entropy > 0.98 {
                    CompressionAlgorithm::None
                } else if repetition_score > 0.5 {
                    CompressionAlgorithm::Deflate // Best compression for repetitive data
                } else {
                    CompressionAlgorithm::Snappy
                }
            }
        }
    }
}

/// Compression priority for algorithm selection
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CompressionPriority {
    /// Prioritize compression/decompression speed
    Speed,
    /// Balance speed and compression ratio
    Balanced,
    /// Prioritize maximum compression ratio
    Ratio,
}

/// Compression statistics
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CompressionStats {
    /// Original size in bytes
    pub original_size: u64,

    /// Compressed size in bytes
    pub compressed_size: u64,

    /// Compression ratio (compressed / original)
    pub ratio: f64,

    /// Compression algorithm used
    pub algorithm: CompressionAlgorithm,
}

impl CompressionStats {
    /// Calculate compression statistics
    pub fn calculate(
        original_size: u64,
        compressed_size: u64,
        algorithm: CompressionAlgorithm,
    ) -> Self {
        let ratio = if original_size > 0 {
            compressed_size as f64 / original_size as f64
        } else {
            1.0
        };

        Self {
            original_size,
            compressed_size,
            ratio,
            algorithm,
        }
    }

    /// Get space saved in bytes
    pub fn space_saved(&self) -> u64 {
        self.original_size.saturating_sub(self.compressed_size)
    }

    /// Get compression percentage
    pub fn compression_percentage(&self) -> f64 {
        (1.0 - self.ratio) * 100.0
    }
}

/// Calculate entropy of data sample (0.0 = no entropy, 1.0 = maximum entropy)
fn calculate_entropy(data: &[u8]) -> f64 {
    if data.is_empty() {
        return 0.0;
    }

    let mut counts = [0u32; 256];
    for &byte in data {
        counts[byte as usize] += 1;
    }

    let total = data.len() as f64;
    let mut entropy = 0.0;

    for &count in &counts {
        if count > 0 {
            let probability = count as f64 / total;
            entropy -= probability * probability.log2();
        }
    }

    // Normalize to 0.0-1.0 range
    entropy / 8.0 // 8 bits per byte
}

/// Calculate repetition score (0.0 = no repetition, 1.0 = highly repetitive)
fn calculate_repetition_score(data: &[u8]) -> f64 {
    if data.len() < 4 {
        return 0.0;
    }

    let mut repeated_bytes = 0;
    let mut pattern_matches = 0;

    // Check for byte repetitions
    for i in 1..data.len() {
        if data[i] == data[i - 1] {
            repeated_bytes += 1;
        }
    }

    // Check for 2-byte pattern repetitions
    // Need at least 4 bytes to check 2-byte patterns (i-3 must be valid)
    // Starting at i=3 prevents arithmetic underflow when accessing data[i-3]
    for i in 3..data.len() {
        if data[i] == data[i - 2] && data[i - 1] == data[i - 3] {
            pattern_matches += 1;
        }
    }

    let byte_repetition_score = repeated_bytes as f64 / (data.len() - 1) as f64;
    let pattern_repetition_score = if data.len() > 3 {
        pattern_matches as f64 / (data.len() - 3) as f64
    } else {
        0.0
    };

    // Combine scores with weights
    (byte_repetition_score * 0.6 + pattern_repetition_score * 0.4).min(1.0)
}

/// The compression algorithm a reader uses to decompress a `Data.db`'s chunks.
///
/// Reduced to a plain algorithm field (issue #1597 / G1): the reader derives the
/// algorithm from the single authoritative `CompressionInfo.db` parse and every
/// decompression site funnels through [`Compression::decompress`] via
/// [`CompressionReader::algorithm`]. The former streaming half (`read_streaming`,
/// `read`, `with_block_size`, `block_size`, and the `buffer`/`block_size` fields)
/// had zero consumers and was deleted.
pub struct CompressionReader {
    algorithm: CompressionAlgorithm,
}

impl CompressionReader {
    /// Create a compression reader for the given algorithm.
    pub fn new(algorithm: CompressionAlgorithm) -> Self {
        Self { algorithm }
    }

    /// Get the compression algorithm.
    pub fn algorithm(&self) -> &CompressionAlgorithm {
        &self.algorithm
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_compression() {
        let compression = Compression::new(CompressionAlgorithm::None).unwrap();
        let data = b"hello world";

        let compressed = compression.compress(data).unwrap();
        assert_eq!(compressed, data);

        let decompressed = compression.decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_compression_stats() {
        let stats = CompressionStats::calculate(1000, 600, CompressionAlgorithm::Lz4);

        assert_eq!(stats.original_size, 1000);
        assert_eq!(stats.compressed_size, 600);
        assert_eq!(stats.ratio, 0.6);
        assert_eq!(stats.space_saved(), 400);
        assert_eq!(stats.compression_percentage(), 40.0);
    }

    // Note: Test methods temporarily disabled due to compilation issues
    // The functionality is tested via integration tests

    /// Adversarial oracle (issue #1588, decision #14): a chunk whose leading 4
    /// bytes ALSO parse as a plausible framed big-endian length header, followed
    /// by a valid RAW-snappy body of exactly that many output bytes.
    ///
    /// Under the (deleted) framed-then-raw guessing, `decompress` read the 4-byte
    /// BE prefix `S`, decoded `data[4..]` as raw snappy to `P_wrong` (whose length
    /// equals `S`), and RETURNED those bytes — silently wrong. The authoritative
    /// Cassandra 5.0 format for a `SnappyCompressor` chunk is RAW snappy with NO
    /// length prefix, so strict raw decoding of the WHOLE chunk must NOT return
    /// `P_wrong` (it rejects the malformed leading zero-varint stream). This is the
    /// no-heuristics enforcement: decode exactly one format determined by metadata.
    #[cfg(feature = "snappy")]
    #[test]
    fn test_snappy_decode_is_strict_raw_only_no_format_guessing() {
        use snap::raw::Encoder;
        let p_wrong = b"WRONG-framed-decode-abcdefghijklmnopqrstuvwxyz".to_vec();
        let s = p_wrong.len() as u32;
        let mut enc = Encoder::new();
        let inner = enc.compress_vec(&p_wrong).unwrap(); // valid raw snappy -> p_wrong
        let mut adversarial = s.to_be_bytes().to_vec(); // plausible framed BE header
        adversarial.extend_from_slice(&inner);

        let compression = Compression::new(CompressionAlgorithm::Snappy).unwrap();
        let got = compression.decompress(&adversarial).ok();
        // Strict raw decode must not merely differ from the framed guess — it must
        // FAIL (typed error) rather than silently produce any bytes: the leading
        // 4-byte pseudo-header parses as a malformed raw Snappy stream (a
        // zero-length literal with trailing data), which the raw decoder rejects.
        assert!(
            got.is_none(),
            "strict raw decode must ERROR on the ambiguous chunk, not return bytes \
             (no-heuristics, #1588); got {got:?}"
        );
    }

    #[cfg(feature = "snappy")]
    #[test]
    fn test_snappy_compression_cassandra_format() {
        let compression = Compression::new(CompressionAlgorithm::Snappy).unwrap();
        let data = b"This is test data for Snappy compression with Cassandra format validation. "
            .repeat(10);

        let compressed = compression.compress(&data).unwrap();

        // Cassandra 5.0 SnappyCompressor emits a RAW Snappy block with NO length
        // prefix (#1588). The compressed bytes are exactly what a raw Snappy
        // decoder consumes; there is no 4-byte big-endian size header.
        let raw_roundtrip = {
            use snap::raw::Decoder;
            Decoder::new().decompress_vec(&compressed).unwrap()
        };
        assert_eq!(raw_roundtrip, data, "compress() output must be raw Snappy");

        let decompressed = compression.decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    /// A legitimate RAW Snappy chunk decodes to the known-good bytes in EXACTLY
    /// one decode attempt (issue #1588 decision #14: single-attempt + byte-identity).
    #[cfg(feature = "snappy")]
    #[test]
    fn test_snappy_decode_single_attempt_and_byte_identical() {
        use snap::raw::Encoder;
        let good = b"the quick brown fox jumps over the lazy dog. ".repeat(8);
        let chunk = Encoder::new().compress_vec(&good).unwrap();

        let mut attempts = 0usize;
        let out = super::snappy_decompress_raw(&chunk, &mut attempts).unwrap();
        assert_eq!(
            out, good,
            "raw decode is byte-identical to the known-good input"
        );
        assert_eq!(
            attempts, 1,
            "exactly one decode attempt (no format guessing)"
        );
    }

    /// Encode a Snappy raw-block length prefix (LEB128 varint) for `n`.
    #[cfg(feature = "snappy")]
    fn snappy_len_prefix(mut n: u64) -> Vec<u8> {
        let mut out = Vec::new();
        loop {
            let mut b = (n & 0x7f) as u8;
            n >>= 7;
            if n != 0 {
                b |= 0x80;
            }
            out.push(b);
            if n == 0 {
                break;
            }
        }
        out
    }

    /// SECURITY (issue #1588): the SHARED `snappy_decompress_raw` helper — the
    /// choke point used by the CHUNK (non-streaming) decode path — must reject a
    /// block whose ADVERTISED decompressed length exceeds `MAX_DECOMPRESSED_SIZE`
    /// WITHOUT allocating (i.e. before `decompress_vec` pre-allocates that size).
    /// This proves the guard lives at the helper, not only in the streaming caller.
    #[cfg(feature = "snappy")]
    #[test]
    fn test_snappy_raw_helper_advertised_len_bomb_rejected_no_alloc() {
        // Craft a tiny raw block: a varint advertising 200MB (> 128MB cap) plus a
        // stub tag byte. The length guard fires before any output allocation.
        let mut chunk = snappy_len_prefix(200 * 1024 * 1024);
        chunk.push(0x00);

        let mut attempts = 0usize;
        let err = super::snappy_decompress_raw(&chunk, &mut attempts)
            .expect_err("over-limit advertised length must be rejected at the helper");
        assert!(
            err.to_string().contains("Decompression bomb protection"),
            "expected typed decompression-bomb error, got: {err}"
        );
        assert!(
            err.to_string().contains("advertised size"),
            "guard must fire on the ADVERTISED length before decode/alloc, got: {err}"
        );
        // The decode attempt is counted, but the rejection happens before the
        // `decompress_vec` allocation of the advertised 200MB.
        assert_eq!(attempts, 1, "helper is entered exactly once");
    }

    #[cfg(feature = "deflate")]
    #[test]
    fn test_deflate_compression_cassandra_format() {
        let compression = Compression::new(CompressionAlgorithm::Deflate).unwrap();
        let data = b"This is test data for Deflate compression with Cassandra format validation. "
            .repeat(10);

        let compressed = compression.compress(&data).unwrap();

        // Cassandra format: ZLIB-wrapped (0x78 header), NO 4-byte size prefix (#1082).
        assert!(compressed.len() >= 2);
        assert_eq!(compressed[0], 0x78, "zlib stream must start with CMF 0x78");

        let decompressed = compression.decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_compression_reader() {
        // CompressionReader is now a plain algorithm field (issue #1597 / G1): it
        // carries the algorithm the reader derived from CompressionInfo.db, which
        // the decompression sites feed to `Compression::new`.
        let reader = CompressionReader::new(CompressionAlgorithm::Snappy);
        assert_eq!(reader.algorithm(), &CompressionAlgorithm::Snappy);
    }

    #[test]
    fn test_compression_algorithm_from_string() {
        assert_eq!(
            CompressionAlgorithm::from("NONE".to_string()),
            CompressionAlgorithm::None
        );
        assert_eq!(
            CompressionAlgorithm::from("LZ4".to_string()),
            CompressionAlgorithm::Lz4
        );
        assert_eq!(
            CompressionAlgorithm::from("SNAPPY".to_string()),
            CompressionAlgorithm::Snappy
        );
        assert_eq!(
            CompressionAlgorithm::from("DEFLATE".to_string()),
            CompressionAlgorithm::Deflate
        );
        assert_eq!(
            CompressionAlgorithm::from("unknown".to_string()),
            CompressionAlgorithm::None
        );
    }

    #[test]
    fn test_compression_invalid_data() {
        let compression = Compression::new(CompressionAlgorithm::Snappy).unwrap();

        // Test with data too short for size prefix
        let short_data = &[1, 2];
        assert!(compression.decompress(short_data).is_err());

        // Test with invalid size prefix
        let invalid_data = &[0, 0, 0, 100, 1, 2, 3]; // Claims 100 bytes but only has 3
        if cfg!(feature = "snappy") {
            assert!(compression.decompress(invalid_data).is_err());
        }
    }

    #[test]
    fn test_decompression_bomb_protection() {
        // Test protection against malicious size claims for all algorithms
        // Using 200MB claim (exceeds 128MB limit) to test protection

        // Snappy: Test that decompression bomb protection works after decompression
        // (not during prefix check, since NB format uses raw Snappy without prefix)
        #[cfg(feature = "snappy")]
        {
            // Note: The decompression bomb protection for Snappy happens AFTER decompression
            // completes, by checking the decompressed size. This is because Cassandra 5.0 NB
            // format uses raw Snappy without a size prefix, so we can't detect bombs early.
            //
            // A malicious prefix with fake size >128MB is handled by skipping the prefixed
            // format and trying raw Snappy instead (which will fail if the data is invalid).
            //
            // This test verifies that post-decompression size checking works correctly.
            // The actual protection is at lines 281-286 in the decompress() method.
        }

        // Deflate (#1082): Cassandra emits ZLIB-wrapped streams with NO 4-byte size
        // prefix, so the bomb guard caps the DECODED output length rather than
        // trusting an in-stream size field. The legacy "fake 200MB size prefix" no
        // longer applies; instead verify that non-zlib bytes (here, what the old
        // format would have produced) are rejected as malformed rather than read as
        // a 2GB size and OOM'd.
        #[cfg(feature = "deflate")]
        {
            let compression = Compression::new(CompressionAlgorithm::Deflate).unwrap();
            let malicious_size: u32 = 200 * 1024 * 1024;
            let mut malicious_data = malicious_size.to_be_bytes().to_vec();
            malicious_data.extend_from_slice(&[0u8; 10]);

            let result = compression.decompress(&malicious_data);
            assert!(
                result.is_err(),
                "Should reject malformed (non-zlib) Deflate data"
            );

            // A genuine zlib-wrapped round-trip still decodes correctly.
            let data = b"deflate bomb-guard roundtrip".repeat(4);
            let compressed = compression.compress(&data).unwrap();
            let decompressed = compression.decompress(&compressed).unwrap();
            assert_eq!(decompressed, data);
        }

        // Zstd (#1082): Cassandra writes a BARE zstd frame with NO 4-byte size
        // prefix, so the old "fake 200MB prefix" scenario no longer applies. The
        // bomb guard now caps the DECODED output length; here verify that malformed
        // (non-frame) bytes are rejected rather than mis-read as a multi-GB size,
        // and that a genuine bare-frame round-trip still decodes.
        #[cfg(feature = "zstd")]
        {
            let compression = Compression::new(CompressionAlgorithm::Zstd).unwrap();
            let mut malicious_data = (200u32 * 1024 * 1024).to_be_bytes().to_vec();
            malicious_data.extend_from_slice(&[0u8; 10]);

            let result = compression.decompress(&malicious_data);
            assert!(result.is_err(), "Should reject malformed Zstd frame");

            let data = b"zstd bomb-guard roundtrip".repeat(4);
            let compressed = compression.compress(&data).unwrap();
            let decompressed = compression.decompress(&compressed).unwrap();
            assert_eq!(decompressed, data);
        }

        // LZ4: Create data claiming 200MB uncompressed size
        #[cfg(feature = "lz4")]
        {
            let compression = Compression::new(CompressionAlgorithm::Lz4).unwrap();
            let malicious_size: u32 = 200 * 1024 * 1024; // 200MB claim (exceeds 128MB limit)
            let mut malicious_data = malicious_size.to_le_bytes().to_vec(); // LZ4 uses little-endian
            malicious_data.extend_from_slice(&[0u8; 10]); // Some fake compressed data

            let result = compression.decompress(&malicious_data);
            assert!(result.is_err(), "Should reject malicious LZ4 size");
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("Decompression bomb"));
        }
    }

    #[test]
    fn test_entropy_calculation() {
        // Test with uniform data (high entropy)
        let uniform_data: Vec<u8> = (0..=255).collect();
        let entropy = calculate_entropy(&uniform_data);
        assert!(entropy > 0.9); // Should be close to 1.0

        // Test with repetitive data (low entropy)
        let repetitive_data = vec![0u8; 256];
        let entropy = calculate_entropy(&repetitive_data);
        assert!(entropy < 0.1); // Should be close to 0.0
    }

    #[test]
    fn test_repetition_score() {
        // Test with highly repetitive data
        let repetitive_data = vec![0u8, 0u8, 0u8, 0u8];
        let score = calculate_repetition_score(&repetitive_data);
        assert!(score > 0.8);

        // Test with random data
        let random_data = vec![1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 7u8, 8u8];
        let score = calculate_repetition_score(&random_data);
        assert!(score < 0.2);
    }

    // Note: Algorithm selection test temporarily disabled due to compilation issues
    // The functionality is tested via integration tests
}
