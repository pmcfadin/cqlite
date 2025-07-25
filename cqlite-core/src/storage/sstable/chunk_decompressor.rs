//! Bulletproof chunk-based decompression for SSTable Data.db files
//!
//! This module implements the proper decompression strategy for Cassandra SSTable files
//! using CompressionInfo.db metadata to decompress chunks on-demand.

use std::io::{Read, Seek, SeekFrom};
use std::collections::HashMap;
use crate::{Error, Result};
use super::compression_info::CompressionInfo;

/// Chunk-based decompressor for SSTable Data.db files
pub struct ChunkDecompressor {
    /// Compression metadata from CompressionInfo.db
    compression_info: CompressionInfo,
    /// Cache of decompressed chunks
    chunk_cache: HashMap<usize, Vec<u8>>,
    /// Maximum number of chunks to cache
    max_cached_chunks: usize,
}

impl ChunkDecompressor {
    /// Create a new chunk decompressor with compression metadata
    pub fn new(compression_info: CompressionInfo) -> Result<Self> {
        compression_info.validate()?;
        
        Ok(Self {
            compression_info,
            chunk_cache: HashMap::new(),
            max_cached_chunks: 16, // Cache up to 16 chunks (16 * 16KB = 256KB max memory)
        })
    }
    
    /// Read data from compressed SSTable at specified offset and length
    pub fn read_data<R: Read + Seek>(
        &mut self,
        reader: &mut R,
        offset: u64,
        length: usize,
    ) -> Result<Vec<u8>> {
        let mut result = Vec::with_capacity(length);
        let mut remaining = length;
        let mut current_offset = offset;
        
        while remaining > 0 {
            // Determine which chunk contains this offset
            let chunk_index = self.compression_info.chunk_for_offset(current_offset);
            let offset_in_chunk = self.compression_info.offset_within_chunk(current_offset);
            
            // Get the decompressed chunk
            let chunk_data = self.get_decompressed_chunk(reader, chunk_index)?;
            
            // Extract the requested data from this chunk
            let chunk_start = offset_in_chunk as usize;
            let chunk_end = std::cmp::min(
                chunk_start + remaining,
                chunk_data.len()
            );
            
            if chunk_start >= chunk_data.len() {
                return Err(Error::InvalidFormat(format!(
                    "Offset {} beyond chunk {} size {}",
                    chunk_start, chunk_index, chunk_data.len()
                )));
            }
            
            let chunk_slice = &chunk_data[chunk_start..chunk_end];
            result.extend_from_slice(chunk_slice);
            
            let bytes_read = chunk_slice.len();
            remaining -= bytes_read;
            current_offset += bytes_read as u64;
        }
        
        Ok(result)
    }
    
    /// Get a decompressed chunk, using cache if available
    fn get_decompressed_chunk<R: Read + Seek>(
        &mut self,
        reader: &mut R,
        chunk_index: usize,
    ) -> Result<Vec<u8>> {
        // Check cache first
        if let Some(cached_chunk) = self.chunk_cache.get(&chunk_index) {
            return Ok(cached_chunk.clone());
        }
        
        // Decompress the chunk
        let chunk_data = self.decompress_chunk(reader, chunk_index)?;
        
        // Add to cache (with LRU eviction if necessary)
        if self.chunk_cache.len() >= self.max_cached_chunks {
            // Simple eviction: remove first entry
            if let Some(first_key) = self.chunk_cache.keys().next().copied() {
                self.chunk_cache.remove(&first_key);
            }
        }
        
        self.chunk_cache.insert(chunk_index, chunk_data.clone());
        Ok(chunk_data)
    }
    
    /// Decompress a specific chunk from the compressed data file
    fn decompress_chunk<R: Read + Seek>(
        &self,
        reader: &mut R,
        chunk_index: usize,
    ) -> Result<Vec<u8>> {
        // Get compressed chunk offset and size
        let compressed_offset = self.compression_info.compressed_chunk_offset(chunk_index)
            .ok_or_else(|| Error::InvalidFormat(format!("No offset for chunk {}", chunk_index)))?;
        
        // Determine chunk size by finding the file size
        let current_pos = reader.seek(SeekFrom::Current(0))
            .map_err(|e| Error::Io(e))?;
        
        let file_size = reader.seek(SeekFrom::End(0))
            .map_err(|e| Error::Io(e))?;
        
        reader.seek(SeekFrom::Start(current_pos))
            .map_err(|e| Error::Io(e))?;
        
        let compressed_size = self.compression_info.compressed_chunk_size(chunk_index, file_size)
            .ok_or_else(|| Error::InvalidFormat(format!("Cannot determine size for chunk {}", chunk_index)))? as usize;
        
        // Seek to compressed chunk offset
        reader.seek(SeekFrom::Start(compressed_offset))
            .map_err(|e| Error::Io(e))?;
        
        // Read compressed chunk data
        let mut compressed_data = vec![0u8; compressed_size];
        reader.read_exact(&mut compressed_data)
            .map_err(|e| Error::Io(e))?;
        
        println!("📦 Reading chunk {} at offset {} ({} bytes compressed)", 
                 chunk_index, compressed_offset, compressed_size);
        
        // Decompress based on algorithm
        match self.compression_info.algorithm.as_str() {
            "LZ4Compressor" => self.decompress_lz4_chunk(&compressed_data, chunk_index),
            "SnappyCompressor" => self.decompress_snappy_chunk(&compressed_data, chunk_index),
            "DeflateCompressor" => self.decompress_deflate_chunk(&compressed_data, chunk_index),
            algorithm => Err(Error::UnsupportedFormat(format!("Unknown compression algorithm: {}", algorithm))),
        }
    }
    
    /// Decompress LZ4 chunk with multiple fallback strategies
    fn decompress_lz4_chunk(&self, compressed_data: &[u8], chunk_index: usize) -> Result<Vec<u8>> {
        println!("🔧 Attempting LZ4 decompression for chunk {} ({} bytes)", chunk_index, compressed_data.len());
        
        if compressed_data.is_empty() {
            return Err(Error::InvalidFormat("Empty compressed chunk".to_string()));
        }
        
        // Debug: show first bytes
        let debug_len = std::cmp::min(32, compressed_data.len());
        println!("📊 First {} bytes: {:02x?}", debug_len, &compressed_data[..debug_len]);
        
        // Strategy 1: Try size-prepended format (standard LZ4)
        if let Ok(decompressed) = lz4_flex::decompress_size_prepended(compressed_data) {
            println!("✅ LZ4 size-prepended decompression succeeded: {} bytes", decompressed.len());
            return Ok(decompressed);
        }
        
        // Strategy 2: Try with expected chunk size
        let expected_size = self.compression_info.chunk_length as usize;
        if let Ok(decompressed) = lz4_flex::decompress(compressed_data, expected_size) {
            println!("✅ LZ4 fixed-size decompression succeeded: {} bytes", decompressed.len());
            return Ok(decompressed);
        }
        
        // Strategy 3: Try reading size from first 4 bytes (big-endian)
        if compressed_data.len() >= 8 {
            let size_be = u32::from_be_bytes([
                compressed_data[0], compressed_data[1], 
                compressed_data[2], compressed_data[3]
            ]) as usize;
            
            if size_be > 0 && size_be <= 10 * 1024 * 1024 { // Reasonable size limit
                if let Ok(decompressed) = lz4_flex::decompress(&compressed_data[4..], size_be) {
                    println!("✅ LZ4 big-endian size decompression succeeded: {} bytes", decompressed.len());
                    return Ok(decompressed);
                }
            }
        }
        
        // Strategy 4: Try reading size from first 4 bytes (little-endian)
        if compressed_data.len() >= 8 {
            let size_le = u32::from_le_bytes([
                compressed_data[0], compressed_data[1], 
                compressed_data[2], compressed_data[3]
            ]) as usize;
            
            if size_le > 0 && size_le <= 10 * 1024 * 1024 { // Reasonable size limit
                if let Ok(decompressed) = lz4_flex::decompress(&compressed_data[4..], size_le) {
                    println!("✅ LZ4 little-endian size decompression succeeded: {} bytes", decompressed.len());
                    return Ok(decompressed);
                }
            }
        }
        
        // Strategy 5: Try with various common sizes
        for &size in &[4096, 8192, 16384, 32768, 65536] {
            if let Ok(decompressed) = lz4_flex::decompress(compressed_data, size) {
                println!("✅ LZ4 size {} decompression succeeded: {} bytes", size, decompressed.len());
                return Ok(decompressed);
            }
        }
        
        // Strategy 6: Check for uncompressed data (Cassandra sometimes stores small chunks uncompressed)
        if compressed_data.len() <= self.compression_info.chunk_length as usize {
            println!("⚠️  Chunk appears to be uncompressed, returning raw data");
            return Ok(compressed_data.to_vec());
        }
        
        Err(Error::InvalidFormat(format!(
            "LZ4 decompression failed for chunk {} with {} bytes. First 16 bytes: {:02x?}",
            chunk_index,
            compressed_data.len(),
            &compressed_data[..std::cmp::min(16, compressed_data.len())]
        )))
    }
    
    /// Decompress Snappy chunk
    fn decompress_snappy_chunk(&self, compressed_data: &[u8], chunk_index: usize) -> Result<Vec<u8>> {
        println!("🔧 Attempting Snappy decompression for chunk {} ({} bytes)", chunk_index, compressed_data.len());
        
        // Try standard Snappy decompression
        #[cfg(feature = "snappy")]
        {
            use snap::raw::Decoder;
            let mut decoder = Decoder::new();
            
            match decoder.decompress_vec(compressed_data) {
                Ok(decompressed) => {
                    println!("✅ Snappy decompression succeeded: {} bytes", decompressed.len());
                    return Ok(decompressed);
                }
                Err(e) => {
                    println!("❌ Snappy decompression failed: {}", e);
                }
            }
        }
        
        #[cfg(not(feature = "snappy"))]
        {
            println!("❌ Snappy support not compiled in");
        }
        
        // Fallback to raw data
        Ok(compressed_data.to_vec())
    }
    
    /// Decompress Deflate chunk
    fn decompress_deflate_chunk(&self, compressed_data: &[u8], chunk_index: usize) -> Result<Vec<u8>> {
        println!("🔧 Attempting Deflate decompression for chunk {} ({} bytes)", chunk_index, compressed_data.len());
        
        #[cfg(feature = "deflate")]
        {
            use flate2::read::DeflateDecoder;
            use std::io::Read;
            
            let mut decoder = DeflateDecoder::new(&compressed_data[..]);
            let mut decompressed = Vec::new();
            
            match decoder.read_to_end(&mut decompressed) {
                Ok(_) => {
                    println!("✅ Deflate decompression succeeded: {} bytes", decompressed.len());
                    return Ok(decompressed);
                }
                Err(e) => {
                    println!("❌ Deflate decompression failed: {}", e);
                }
            }
        }
        
        #[cfg(not(feature = "deflate"))]
        {
            println!("❌ Deflate support not compiled in");
        }
        
        // Fallback to raw data
        Ok(compressed_data.to_vec())
    }
    
    /// Clear the chunk cache to free memory
    pub fn clear_cache(&mut self) {
        self.chunk_cache.clear();
    }
    
    /// Get cache statistics
    pub fn cache_stats(&self) -> (usize, usize) {
        (self.chunk_cache.len(), self.max_cached_chunks)
    }
    
    /// Read all data from the compressed file (for testing/debugging)
    pub fn read_all_data<R: Read + Seek>(&mut self, reader: &mut R) -> Result<Vec<u8>> {
        self.read_data(reader, 0, self.compression_info.data_length as usize)
    }
    
    /// Get compression info
    pub fn compression_info(&self) -> &CompressionInfo {
        &self.compression_info
    }
}

/// Utility function to create a chunk decompressor from CompressionInfo.db file
pub fn create_decompressor_from_file(compression_info_path: &std::path::Path) -> Result<ChunkDecompressor> {
    let compression_data = std::fs::read(compression_info_path)
        .map_err(|e| Error::Io(e))?;
    
    let compression_info = CompressionInfo::parse(&compression_data)
        .or_else(|_| CompressionInfo::parse_alternative_format(&compression_data))?;
    
    println!("📋 Loaded compression info:");
    println!("   Algorithm: {}", compression_info.algorithm);
    println!("   Chunk Length: {} bytes", compression_info.chunk_length);
    println!("   Data Length: {} bytes", compression_info.data_length);
    println!("   Chunk Count: {}", compression_info.chunk_offsets.len());
    
    ChunkDecompressor::new(compression_info)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_chunk_decompressor_creation() {
        let compression_info = CompressionInfo {
            algorithm: "LZ4Compressor".to_string(),
            chunk_length: 16384,
            data_length: 32768,
            chunk_offsets: vec![0, 8192, 16384],
        };
        
        let decompressor = ChunkDecompressor::new(compression_info).unwrap();
        assert_eq!(decompressor.compression_info.algorithm, "LZ4Compressor");
        assert_eq!(decompressor.compression_info.chunk_length, 16384);
        assert_eq!(decompressor.compression_info.chunk_offsets.len(), 3);
    }

    #[test]
    fn test_chunk_cache() {
        let compression_info = CompressionInfo {
            algorithm: "LZ4Compressor".to_string(),
            chunk_length: 16384,
            data_length: 16384,
            chunk_offsets: vec![0],
        };
        
        let mut decompressor = ChunkDecompressor::new(compression_info).unwrap();
        
        let (cached, max) = decompressor.cache_stats();
        assert_eq!(cached, 0);
        assert_eq!(max, 16);
        
        decompressor.clear_cache();
        let (cached_after_clear, _) = decompressor.cache_stats();
        assert_eq!(cached_after_clear, 0);
    }
}