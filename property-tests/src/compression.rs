//! Mock compression algorithms for property testing

use thiserror::Error;

/// Compression algorithm types
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CompressionType {
    None,
    Lz4Mock,
    SnappyMock,
    DeflateMock,
    ZstdMock,
}

/// Compression errors
#[derive(Error, Debug)]
pub enum CompressionError {
    #[error("Invalid compression format")]
    InvalidFormat,
    #[error("Decompression failed: {0}")]
    DecompressionFailed(String),
    #[error("Compression failed: {0}")]
    CompressionFailed(String),
}

/// Mock compression codec for testing
pub struct CompressionCodec {
    algorithm: CompressionType,
}

impl CompressionCodec {
    /// Create a new compression codec
    pub fn new(algorithm: CompressionType) -> Result<Self, CompressionError> {
        Ok(Self { algorithm })
    }

    /// Compress data using the specified algorithm
    pub fn compress(&self, data: &[u8]) -> Result<Vec<u8>, CompressionError> {
        match self.algorithm {
            CompressionType::None => Ok(data.to_vec()),
            CompressionType::Lz4Mock => self.lz4_compress(data),
            CompressionType::SnappyMock => self.snappy_compress(data),
            CompressionType::DeflateMock => self.deflate_compress(data),
            CompressionType::ZstdMock => self.zstd_compress(data),
        }
    }

    /// Decompress data using the specified algorithm
    pub fn decompress(&self, data: &[u8], expected_size: usize) -> Result<Vec<u8>, CompressionError> {
        if data.is_empty() {
            return Ok(vec![]);
        }

        match self.algorithm {
            CompressionType::None => Ok(data.to_vec()),
            CompressionType::Lz4Mock => self.lz4_decompress(data, expected_size),
            CompressionType::SnappyMock => self.snappy_decompress(data, expected_size),
            CompressionType::DeflateMock => self.deflate_decompress(data, expected_size),
            CompressionType::ZstdMock => self.zstd_decompress(data, expected_size),
        }
    }

    // Mock LZ4 compression - simple run-length encoding for testing
    fn lz4_compress(&self, data: &[u8]) -> Result<Vec<u8>, CompressionError> {
        if data.is_empty() {
            return Ok(vec![0xF4]); // LZ4 empty marker
        }

        let mut result = vec![0xF4]; // LZ4 magic marker

        let mut i = 0;
        while i < data.len() {
            let byte = data[i];
            let mut count = 1;

            // Count consecutive identical bytes
            while i + count < data.len() && data[i + count] == byte && count < 255 {
                count += 1;
            }

            if count >= 4 {
                // Use run-length encoding for 4+ consecutive bytes
                result.push(0xFF); // RLE marker
                result.push(byte);
                result.push(count as u8);
            } else {
                // Store bytes literally
                for _ in 0..count {
                    result.push(byte);
                }
            }

            i += count;
        }

        Ok(result)
    }

    fn lz4_decompress(&self, data: &[u8], _expected_size: usize) -> Result<Vec<u8>, CompressionError> {
        if data.is_empty() || data[0] != 0xF4 {
            return Err(CompressionError::InvalidFormat);
        }

        if data.len() == 1 {
            return Ok(vec![]); // Empty data
        }

        let mut result = Vec::new();
        let mut i = 1; // Skip magic marker

        while i < data.len() {
            if data[i] == 0xFF && i + 2 < data.len() {
                // RLE sequence
                let byte = data[i + 1];
                let count = data[i + 2] as usize;
                result.extend(vec![byte; count]);
                i += 3;
            } else {
                // Literal byte
                result.push(data[i]);
                i += 1;
            }
        }

        Ok(result)
    }

    // Mock Snappy compression - simple dictionary-based approach
    fn snappy_compress(&self, data: &[u8]) -> Result<Vec<u8>, CompressionError> {
        let mut result = vec![0x53]; // Snappy magic marker

        if data.is_empty() {
            return Ok(result);
        }

        // Simple pattern matching for common sequences
        let mut i = 0;
        while i < data.len() {
            let mut best_match_len = 0;
            let mut best_match_offset = 0;

            // Look for matches in previous 1024 bytes
            let start = i.saturating_sub(1024);
            for j in start..i {
                let mut match_len = 0;
                while i + match_len < data.len() &&
                      j + match_len < i &&
                      data[j + match_len] == data[i + match_len] &&
                      match_len < 255 {
                    match_len += 1;
                }

                if match_len > best_match_len && match_len >= 4 {
                    best_match_len = match_len;
                    best_match_offset = i - j;
                }
            }

            if best_match_len >= 4 {
                // Encode as (offset, length) pair
                result.push(0xFE); // Match marker
                result.extend_from_slice(&(best_match_offset as u16).to_le_bytes());
                result.push(best_match_len as u8);
                i += best_match_len;
            } else {
                // Literal byte
                result.push(data[i]);
                i += 1;
            }
        }

        Ok(result)
    }

    fn snappy_decompress(&self, data: &[u8], _expected_size: usize) -> Result<Vec<u8>, CompressionError> {
        if data.is_empty() || data[0] != 0x53 {
            return Err(CompressionError::InvalidFormat);
        }

        let mut result = Vec::new();
        let mut i = 1; // Skip magic marker

        while i < data.len() {
            if data[i] == 0xFE && i + 3 < data.len() {
                // Match sequence
                let offset = u16::from_le_bytes([data[i + 1], data[i + 2]]) as usize;
                let length = data[i + 3] as usize;

                if result.len() < offset {
                    return Err(CompressionError::DecompressionFailed(
                        "Invalid offset in match".to_string()
                    ));
                }

                let start_pos = result.len() - offset;
                for j in 0..length {
                    let byte = result[start_pos + j];
                    result.push(byte);
                }

                i += 4;
            } else {
                // Literal byte
                result.push(data[i]);
                i += 1;
            }
        }

        Ok(result)
    }

    // Mock Deflate compression - simple Huffman-like approach
    fn deflate_compress(&self, data: &[u8]) -> Result<Vec<u8>, CompressionError> {
        let mut result = vec![0xDE, 0xF1]; // Deflate magic marker

        if data.is_empty() {
            return Ok(result);
        }

        // Count byte frequencies
        let mut freq = [0u32; 256];
        for &byte in data {
            freq[byte as usize] += 1;
        }

        // Simple frequency-based encoding
        let mut byte_codes = [0u8; 256];
        let mut sorted_bytes: Vec<(u32, u8)> = freq.iter()
            .enumerate()
            .map(|(i, &f)| (f, i as u8))
            .filter(|(f, _)| *f > 0)
            .collect();
        sorted_bytes.sort_by(|a, b| b.0.cmp(&a.0));

        // Assign shorter codes to more frequent bytes
        for (i, (_, byte)) in sorted_bytes.iter().enumerate() {
            byte_codes[*byte as usize] = i as u8;
        }

        // Encode frequency table
        result.push(sorted_bytes.len() as u8);
        for (_, byte) in &sorted_bytes {
            result.push(*byte);
        }

        // Encode data
        for &byte in data {
            result.push(byte_codes[byte as usize]);
        }

        Ok(result)
    }

    fn deflate_decompress(&self, data: &[u8], _expected_size: usize) -> Result<Vec<u8>, CompressionError> {
        if data.len() < 2 || data[0] != 0xDE || data[1] != 0xF1 {
            return Err(CompressionError::InvalidFormat);
        }

        if data.len() == 2 {
            return Ok(vec![]); // Empty data
        }

        if data.len() < 3 {
            return Err(CompressionError::InvalidFormat);
        }

        let table_size = data[2] as usize;
        if data.len() < 3 + table_size {
            return Err(CompressionError::InvalidFormat);
        }

        // Read frequency table
        let mut code_to_byte = Vec::new();
        for i in 0..table_size {
            code_to_byte.push(data[3 + i]);
        }

        // Decode data
        let mut result = Vec::new();
        for i in (3 + table_size)..data.len() {
            let code = data[i] as usize;
            if code >= code_to_byte.len() {
                return Err(CompressionError::DecompressionFailed(
                    "Invalid code in compressed data".to_string()
                ));
            }
            result.push(code_to_byte[code]);
        }

        Ok(result)
    }

    // Mock Zstd compression - hybrid approach
    fn zstd_compress(&self, data: &[u8]) -> Result<Vec<u8>, CompressionError> {
        let mut result = vec![0x28, 0xB5, 0x2F, 0xFD]; // Zstd magic number

        if data.is_empty() {
            return Ok(result);
        }

        // Combine run-length encoding and dictionary matching
        let mut i = 0;
        while i < data.len() {
            let byte = data[i];
            let mut count = 1;

            // Check for runs
            while i + count < data.len() && data[i + count] == byte && count < 255 {
                count += 1;
            }

            if count >= 3 {
                // Use RLE for runs of 3+
                result.push(0xF0); // RLE marker
                result.push(byte);
                result.push(count as u8);
                i += count;
            } else {
                // Check for dictionary matches
                let mut best_match_len = 0;
                let mut best_match_offset = 0;

                let start = i.saturating_sub(2048);
                for j in start..i {
                    let mut match_len = 0;
                    while i + match_len < data.len() &&
                          j + match_len < i &&
                          data[j + match_len] == data[i + match_len] &&
                          match_len < 255 {
                        match_len += 1;
                    }

                    if match_len > best_match_len && match_len >= 3 {
                        best_match_len = match_len;
                        best_match_offset = i - j;
                    }
                }

                if best_match_len >= 3 {
                    // Dictionary match
                    result.push(0xF1); // Dict marker
                    result.extend_from_slice(&(best_match_offset as u16).to_le_bytes());
                    result.push(best_match_len as u8);
                    i += best_match_len;
                } else {
                    // Literal
                    result.push(data[i]);
                    i += 1;
                }
            }
        }

        Ok(result)
    }

    fn zstd_decompress(&self, data: &[u8], _expected_size: usize) -> Result<Vec<u8>, CompressionError> {
        if data.len() < 4 || &data[0..4] != [0x28, 0xB5, 0x2F, 0xFD] {
            return Err(CompressionError::InvalidFormat);
        }

        let mut result = Vec::new();
        let mut i = 4; // Skip magic number

        while i < data.len() {
            if data[i] == 0xF0 && i + 2 < data.len() {
                // RLE sequence
                let byte = data[i + 1];
                let count = data[i + 2] as usize;
                result.extend(vec![byte; count]);
                i += 3;
            } else if data[i] == 0xF1 && i + 3 < data.len() {
                // Dictionary match
                let offset = u16::from_le_bytes([data[i + 1], data[i + 2]]) as usize;
                let length = data[i + 3] as usize;

                if result.len() < offset {
                    return Err(CompressionError::DecompressionFailed(
                        "Invalid offset in dictionary match".to_string()
                    ));
                }

                let start_pos = result.len() - offset;
                for j in 0..length {
                    let byte = result[start_pos + j];
                    result.push(byte);
                }

                i += 4;
            } else {
                // Literal byte
                result.push(data[i]);
                i += 1;
            }
        }

        Ok(result)
    }

    /// Get the expected compression ratio for this algorithm and data
    pub fn expected_compression_ratio(&self, data: &[u8]) -> f64 {
        if data.is_empty() {
            return 1.0;
        }

        match self.algorithm {
            CompressionType::None => 1.0,
            CompressionType::Lz4Mock => {
                // LZ4 is fast but not great compression
                self.estimate_rle_ratio(data, 0.9) // Modest compression
            },
            CompressionType::SnappyMock => {
                // Snappy balances speed and compression
                self.estimate_dict_ratio(data, 0.8)
            },
            CompressionType::DeflateMock => {
                // Deflate achieves better compression
                self.estimate_huffman_ratio(data, 0.6)
            },
            CompressionType::ZstdMock => {
                // Zstd combines multiple techniques
                let rle_ratio = self.estimate_rle_ratio(data, 0.7);
                let dict_ratio = self.estimate_dict_ratio(data, 0.8);
                rle_ratio.min(dict_ratio)
            },
        }
    }

    fn estimate_rle_ratio(&self, data: &[u8], base_ratio: f64) -> f64 {
        if data.is_empty() {
            return 1.0;
        }

        let mut _runs = 0;
        let mut total_run_length = 0;
        let mut i = 0;

        while i < data.len() {
            let byte = data[i];
            let mut count = 1;

            while i + count < data.len() && data[i + count] == byte {
                count += 1;
            }

            if count >= 4 {
                _runs += 1;
                total_run_length += count;
            }

            i += count;
        }

        if total_run_length > data.len() / 2 {
            base_ratio * 0.3 // Good compression for repetitive data
        } else {
            base_ratio
        }
    }

    fn estimate_dict_ratio(&self, data: &[u8], base_ratio: f64) -> f64 {
        // Simplified estimate based on data patterns
        let unique_bytes = data.iter().collect::<std::collections::HashSet<_>>().len();
        let diversity_ratio = unique_bytes as f64 / 256.0;

        base_ratio * (0.5 + diversity_ratio * 0.5)
    }

    fn estimate_huffman_ratio(&self, data: &[u8], base_ratio: f64) -> f64 {
        if data.is_empty() {
            return 1.0;
        }

        // Calculate entropy
        let mut freq = [0u32; 256];
        for &byte in data {
            freq[byte as usize] += 1;
        }

        let len = data.len() as f64;
        let entropy: f64 = freq.iter()
            .filter(|&&count| count > 0)
            .map(|&count| {
                let p = count as f64 / len;
                -p * p.log2()
            })
            .sum();

        let max_entropy = 8.0; // 8 bits per byte
        let compression_potential = 1.0 - (entropy / max_entropy);

        base_ratio * (1.0 - compression_potential * 0.7)
    }
}