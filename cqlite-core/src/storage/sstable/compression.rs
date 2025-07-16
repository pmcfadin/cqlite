//! Compression support for SSTable storage

use crate::{error::Error, Result};

/// Compression algorithms supported
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum CompressionAlgorithm {
    /// No compression
    None,
    /// LZ4 compression (fast)
    Lz4,
    /// Snappy compression (balanced)
    Snappy,
    /// Deflate compression (high ratio)
    Deflate,
}

impl Default for CompressionAlgorithm {
    fn default() -> Self {
        CompressionAlgorithm::Lz4
    }
}

impl From<String> for CompressionAlgorithm {
    fn from(s: String) -> Self {
        match s.to_uppercase().as_str() {
            "NONE" => CompressionAlgorithm::None,
            "LZ4" => CompressionAlgorithm::Lz4,
            "SNAPPY" => CompressionAlgorithm::Snappy,
            "DEFLATE" => CompressionAlgorithm::Deflate,
            _ => CompressionAlgorithm::None, // Default to None for unknown algorithms
        }
    }
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

    /// Compress data
    pub fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        match self.algorithm {
            CompressionAlgorithm::None => Ok(data.to_vec()),
            CompressionAlgorithm::Lz4 => {
                #[cfg(feature = "lz4")]
                {
                    use lz4_flex::compress_prepend_size;
                    Ok(compress_prepend_size(data))
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
                    let mut encoder = Encoder::new();
                    encoder
                        .compress_vec(data)
                        .map_err(|e| Error::storage(e.to_string()))
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
                    use flate2::write::DeflateEncoder;
                    use flate2::Compression as DeflateCompression;
                    use std::io::Write;

                    let mut encoder =
                        DeflateEncoder::new(Vec::new(), DeflateCompression::default());
                    encoder
                        .write_all(data)
                        .map_err(|e| Error::storage(e.to_string()))?;
                    encoder.finish().map_err(|e| Error::storage(e.to_string()))
                }
                #[cfg(not(feature = "deflate"))]
                {
                    Err(Error::storage(
                        "Deflate compression not available".to_string(),
                    ))
                }
            }
        }
    }

    /// Decompress data
    pub fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        match self.algorithm {
            CompressionAlgorithm::None => Ok(data.to_vec()),
            CompressionAlgorithm::Lz4 => {
                #[cfg(feature = "lz4")]
                {
                    use lz4_flex::decompress_size_prepended;
                    decompress_size_prepended(data).map_err(|e| Error::storage(e.to_string()))
                }
                #[cfg(not(feature = "lz4"))]
                {
                    Err(Error::storage("LZ4 compression not available".to_string()))
                }
            }
            CompressionAlgorithm::Snappy => {
                #[cfg(feature = "snappy")]
                {
                    use snap::raw::Decoder;
                    let mut decoder = Decoder::new();
                    decoder
                        .decompress_vec(data)
                        .map_err(|e| Error::storage(e.to_string()))
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
                    use flate2::read::DeflateDecoder;
                    use std::io::Read;

                    let mut decoder = DeflateDecoder::new(data);
                    let mut decompressed = Vec::new();
                    decoder
                        .read_to_end(&mut decompressed)
                        .map_err(|e| Error::storage(e.to_string()))?;
                    Ok(decompressed)
                }
                #[cfg(not(feature = "deflate"))]
                {
                    Err(Error::storage(
                        "Deflate compression not available".to_string(),
                    ))
                }
            }
        }
    }

    /// Get compression algorithm
    pub fn algorithm(&self) -> &CompressionAlgorithm {
        &self.algorithm
    }

    /// Get compression ratio estimate
    pub fn estimated_ratio(&self) -> f64 {
        match self.algorithm {
            CompressionAlgorithm::None => 1.0,
            CompressionAlgorithm::Lz4 => 0.6,    // ~40% compression
            CompressionAlgorithm::Snappy => 0.5, // ~50% compression
            CompressionAlgorithm::Deflate => 0.3, // ~70% compression
        }
    }
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

    #[test]
    fn test_compression_ratio_estimates() {
        let none = Compression::new(CompressionAlgorithm::None).unwrap();
        let lz4 = Compression::new(CompressionAlgorithm::Lz4).unwrap();
        let snappy = Compression::new(CompressionAlgorithm::Snappy).unwrap();
        let deflate = Compression::new(CompressionAlgorithm::Deflate).unwrap();

        assert_eq!(none.estimated_ratio(), 1.0);
        assert_eq!(lz4.estimated_ratio(), 0.6);
        assert_eq!(snappy.estimated_ratio(), 0.5);
        assert_eq!(deflate.estimated_ratio(), 0.3);
    }
}

/// Compression reader for streaming decompression
pub struct CompressionReader {
    algorithm: CompressionAlgorithm,
    buffer: Vec<u8>,
}

impl CompressionReader {
    /// Create a new compression reader
    pub fn new(algorithm: CompressionAlgorithm) -> Self {
        Self {
            algorithm,
            buffer: Vec::new(),
        }
    }

    /// Read and decompress data
    pub fn read(&mut self, compressed_data: &[u8]) -> Result<Vec<u8>> {
        let compression = Compression::new(self.algorithm.clone())?;
        compression.decompress(compressed_data)
    }

    /// Get the compression algorithm
    pub fn algorithm(&self) -> &CompressionAlgorithm {
        &self.algorithm
    }
}
