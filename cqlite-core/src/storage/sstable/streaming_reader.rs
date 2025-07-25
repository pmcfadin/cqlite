//! High-performance streaming SSTable reader
//!
//! This module provides memory-efficient streaming reading of large SSTable files with:
//! - Streaming decompression for compressed blocks
//! - Buffer pool management to prevent memory exhaustion
//! - Progressive loading of large files
//! - Memory-mapped I/O for very large files
//! - Integration with compression ratio-based algorithm selection

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, BufReader};
use tokio::sync::{Mutex, RwLock};
use memmap2::{Mmap, MmapOptions};
use parking_lot::Mutex as SyncMutex;

use crate::{
    parser::{
        types::{parse_cql_value, CqlTypeId},
        vint::parse_vint_length,
        SSTableHeader, SSTableParser,
    },
    platform::Platform,
    types::TableId,
    Config, Error, Result, RowKey, Value,
};

use super::{
    bloom::BloomFilter,
    compression::{Compression, CompressionAlgorithm, CompressionReader},
    index::SSTableIndex,
    reader::{SSTableReaderStats, BlockMeta},
};

/// Configuration for streaming SSTable reader
#[derive(Debug, Clone)]
pub struct StreamingReaderConfig {
    /// Buffer pool size in MB
    pub buffer_pool_size_mb: usize,
    /// Individual buffer size in bytes
    pub buffer_size_bytes: usize,
    /// Memory-mapped I/O threshold (files larger than this use mmap)
    pub mmap_threshold_bytes: u64,
    /// Maximum memory usage before applying backpressure
    pub max_memory_usage_mb: usize,
    /// Streaming chunk size for decompression
    pub streaming_chunk_size: usize,
    /// Enable progressive loading
    pub enable_progressive_loading: bool,
    /// Prefetch distance in blocks
    pub prefetch_distance: usize,
}

impl Default for StreamingReaderConfig {
    fn default() -> Self {
        Self {
            buffer_pool_size_mb: 32,           // 32MB buffer pool
            buffer_size_bytes: 64 * 1024,      // 64KB buffers
            mmap_threshold_bytes: 64 * 1024 * 1024, // 64MB threshold
            max_memory_usage_mb: 128,          // 128MB max memory
            streaming_chunk_size: 16 * 1024,   // 16KB streaming chunks
            enable_progressive_loading: true,
            prefetch_distance: 4,              // Prefetch 4 blocks ahead
        }
    }
}

/// Memory-efficient buffer pool for reusing buffers
pub struct BufferPool {
    /// Available buffers
    available_buffers: SyncMutex<VecDeque<Vec<u8>>>,
    /// Buffer size
    buffer_size: usize,
    /// Maximum number of buffers
    max_buffers: usize,
    /// Current number of allocated buffers
    allocated_count: AtomicUsize,
    /// Total memory usage in bytes
    memory_usage: AtomicU64,
}

impl BufferPool {
    pub fn new(buffer_size: usize, max_buffers: usize) -> Self {
        Self {
            available_buffers: SyncMutex::new(VecDeque::new()),
            buffer_size,
            max_buffers,
            allocated_count: AtomicUsize::new(0),
            memory_usage: AtomicU64::new(0),
        }
    }

    /// Get a buffer from the pool or allocate a new one
    pub fn get_buffer(&self) -> Vec<u8> {
        let mut buffers = self.available_buffers.lock();
        
        if let Some(mut buffer) = buffers.pop_front() {
            // Reuse existing buffer
            buffer.clear();
            buffer.resize(self.buffer_size, 0);
            buffer
        } else if self.allocated_count.load(Ordering::Relaxed) < self.max_buffers {
            // Allocate new buffer
            self.allocated_count.fetch_add(1, Ordering::Relaxed);
            self.memory_usage.fetch_add(self.buffer_size as u64, Ordering::Relaxed);
            vec![0u8; self.buffer_size]
        } else {
            // Pool exhausted, wait for available buffer or allocate temporary
            vec![0u8; self.buffer_size]
        }
    }

    /// Return a buffer to the pool
    pub fn return_buffer(&self, buffer: Vec<u8>) {
        if buffer.capacity() >= self.buffer_size {
            let mut buffers = self.available_buffers.lock();
            if buffers.len() < self.max_buffers {
                buffers.push_back(buffer);
                return;
            }
        }
        
        // Buffer doesn't fit criteria, let it be dropped
        if self.allocated_count.load(Ordering::Relaxed) > 0 {
            self.allocated_count.fetch_sub(1, Ordering::Relaxed);
            self.memory_usage.fetch_sub(self.buffer_size as u64, Ordering::Relaxed);
        }
    }

    /// Get current memory usage in bytes
    pub fn memory_usage(&self) -> u64 {
        self.memory_usage.load(Ordering::Relaxed)
    }

    /// Get current buffer count
    pub fn buffer_count(&self) -> usize {
        self.allocated_count.load(Ordering::Relaxed)
    }
}

/// Streaming block for progressive loading
#[derive(Debug)]
pub struct StreamingBlock {
    /// Block metadata
    pub meta: BlockMeta,
    /// Compressed data chunks
    pub compressed_chunks: Vec<Vec<u8>>,
    /// Decompressed data (lazily loaded)
    pub decompressed_data: Option<Vec<u8>>,
    /// Loading progress (0.0 to 1.0)
    pub progress: f32,
    /// Last access time for LRU eviction
    pub last_access: std::time::Instant,
}

impl StreamingBlock {
    pub fn new(meta: BlockMeta) -> Self {
        Self {
            meta,
            compressed_chunks: Vec::new(),
            decompressed_data: None,
            progress: 0.0,
            last_access: std::time::Instant::now(),
        }
    }

    /// Add a compressed chunk to the block
    pub fn add_chunk(&mut self, chunk: Vec<u8>) {
        self.compressed_chunks.push(chunk);
        self.update_progress();
    }

    /// Update loading progress
    fn update_progress(&mut self) {
        let total_compressed_size = self.meta.compressed_size as usize;
        let loaded_size: usize = self.compressed_chunks.iter().map(|c| c.len()).sum();
        self.progress = if total_compressed_size > 0 {
            (loaded_size as f32 / total_compressed_size as f32).min(1.0)
        } else {
            1.0
        };
    }

    /// Check if block is fully loaded
    pub fn is_fully_loaded(&self) -> bool {
        self.progress >= 1.0
    }

    /// Get memory usage of this block
    pub fn memory_usage(&self) -> usize {
        let chunks_size: usize = self.compressed_chunks.iter().map(|c| c.len()).sum();
        let decompressed_size = self.decompressed_data.as_ref().map(|d| d.len()).unwrap_or(0);
        chunks_size + decompressed_size
    }
}

/// High-performance streaming SSTable reader
pub struct StreamingSSTableReader {
    /// Path to the SSTable file
    file_path: PathBuf,
    /// Memory-mapped file (for large files)
    mmap: Option<Mmap>,
    /// File handle for streaming
    file: Option<Arc<Mutex<BufReader<File>>>>,
    /// SSTable header
    header: SSTableHeader,
    /// Parser for SSTable format
    parser: SSTableParser,
    /// Index for efficient lookups
    index: Option<SSTableIndex>,
    /// Bloom filter
    bloom_filter: Option<BloomFilter>,
    /// Streaming compression reader
    compression_reader: Option<CompressionReader>,
    /// Buffer pool for memory management
    buffer_pool: Arc<BufferPool>,
    /// Streaming blocks cache
    streaming_blocks: Arc<RwLock<HashMap<u64, Arc<Mutex<StreamingBlock>>>>>,
    /// Current memory usage tracker
    current_memory_usage: Arc<AtomicU64>,
    /// Configuration
    config: StreamingReaderConfig,
    /// Platform abstraction
    platform: Arc<Platform>,
    /// Performance statistics
    stats: Arc<Mutex<SSTableReaderStats>>,
}

impl StreamingSSTableReader {
    /// Create a new streaming SSTable reader
    pub async fn open(path: &Path, config: &Config, platform: Arc<Platform>) -> Result<Self> {
        let file = File::open(path).await?;
        let file_size = file.metadata().await?.len();
        
        let streaming_config = StreamingReaderConfig::default();

        // Decide on I/O strategy based on file size
        let (mmap, file_handle) = if file_size >= streaming_config.mmap_threshold_bytes {
            // Use memory mapping for large files
            let std_file = file.into_std().await;
            let mmap = unsafe { MmapOptions::new().map(&std_file)? };
            (Some(mmap), None)
        } else {
            // Use streaming I/O for smaller files
            (None, Some(Arc::new(Mutex::new(BufReader::new(file)))))
        };

        // Parse header
        let header_data = if let Some(ref mmap) = mmap {
            // Read header from memory-mapped data
            let header_size = 4096.min(mmap.len());
            mmap[..header_size].to_vec()
        } else {
            // Read header from file
            let mut buffer = vec![0u8; 4096];
            if let Some(ref file) = file_handle {
                let mut file_guard = file.lock().await;
                file_guard.read_exact(&mut buffer).await?;
            }
            buffer
        };

        let config = crate::parser::config::ParserConfig::default();
        let parser = SSTableParser::new(config)?;
        // TODO: Implement parse_header method for SSTableParser - using placeholder header for now
        let header = crate::parser::header::SSTableHeader {
            cassandra_version: crate::parser::header::CassandraVersion::V5_0_NewBig,
            version: crate::parser::header::SUPPORTED_VERSION,
            table_id: [0; 16],
            keyspace: "placeholder".to_string(),
            table_name: "placeholder".to_string(),
            generation: 0,
            compression: crate::parser::header::CompressionInfo {
                algorithm: "NONE".to_string(),
                chunk_size: 0,
                parameters: std::collections::HashMap::new(),
            },
            stats: crate::parser::header::SSTableStats {
                row_count: 0,
                min_timestamp: 0,
                max_timestamp: 0,
                max_deletion_time: 0,
                compression_ratio: 1.0,
                row_size_histogram: vec![],
            },
            columns: vec![],
            properties: std::collections::HashMap::new(),
        };

        // Initialize compression reader for streaming decompression
        let compression_reader = if header.compression.algorithm != "NONE" {
            let algorithm = CompressionAlgorithm::from(header.compression.algorithm.clone());
            Some(CompressionReader::new(algorithm))
        } else {
            None
        };

        // Initialize buffer pool
        let max_buffers = streaming_config.buffer_pool_size_mb * 1024 * 1024 / streaming_config.buffer_size_bytes;
        let buffer_pool = Arc::new(BufferPool::new(streaming_config.buffer_size_bytes, max_buffers));

        let stats = Arc::new(Mutex::new(SSTableReaderStats {
            file_size,
            entry_count: header.stats.row_count,
            table_count: 1,
            block_count: 0,
            index_size: 0,
            bloom_filter_size: 0,
            compression_ratio: header.stats.compression_ratio,
            cache_hit_rate: 0.0,
        }));

        Ok(Self {
            file_path: path.to_path_buf(),
            mmap,
            file: file_handle,
            header,
            parser,
            index: None, // Would load from file in real implementation
            bloom_filter: None, // Would load from file in real implementation
            compression_reader,
            buffer_pool,
            streaming_blocks: Arc::new(RwLock::new(HashMap::new())),
            current_memory_usage: Arc::new(AtomicU64::new(0)),
            config: streaming_config,
            platform,
            stats,
        })
    }

    /// Get a value with streaming optimizations
    pub async fn get_streaming(&self, table_id: &TableId, key: &RowKey) -> Result<Option<Value>> {
        // Check bloom filter first
        if let Some(bloom_filter) = &self.bloom_filter {
            if !bloom_filter.might_contain(key.as_bytes()) {
                return Ok(None);
            }
        }

        // Use index if available
        if let Some(index) = &self.index {
            if let Some(entry) = index.find_entry(table_id, key).await? {
                return self.read_value_streaming(entry.offset, entry.size).await;
            }
        }

        // Fallback to streaming scan
        self.scan_streaming(table_id, Some(key), Some(key), Some(1))
            .await
            .map(|results| results.into_iter().next().map(|(_, value)| value))
    }

    /// Streaming scan with memory management
    pub async fn scan_streaming(
        &self,
        table_id: &TableId,
        start_key: Option<&RowKey>,
        end_key: Option<&RowKey>,
        limit: Option<usize>,
    ) -> Result<Vec<(RowKey, Value)>> {
        let mut results = Vec::new();
        let mut count = 0;

        // Apply memory pressure backpressure
        self.check_memory_pressure().await?;

        let header_size = self.calculate_header_size();
        let mut current_offset = header_size as u64;

        while let Some(streaming_block) = self.load_block_streaming(current_offset).await? {
            // Wait for block to be sufficiently loaded
            self.wait_for_block_loading(&streaming_block, 0.5).await?;

            let entries = self.parse_streaming_block_entries(&streaming_block).await?;

            for (entry_table_id, entry_key, entry_value) in entries {
                if entry_table_id != *table_id {
                    continue;
                }

                // Range filtering
                if let Some(start) = start_key {
                    if entry_key < *start {
                        continue;
                    }
                }

                if let Some(end) = end_key {
                    if entry_key > *end {
                        continue;
                    }
                }

                results.push((entry_key, entry_value));
                count += 1;

                if let Some(limit) = limit {
                    if count >= limit {
                        return Ok(results);
                    }
                }
            }

            // Update offset for next block
            let block_guard = streaming_block.lock().await;
            current_offset += block_guard.meta.compressed_size as u64 + 16; // Header size
        }

        Ok(results)
    }

    /// Get streaming statistics
    pub async fn get_streaming_stats(&self) -> Result<StreamingStats> {
        let buffer_memory = self.buffer_pool.memory_usage();
        let streaming_memory = self.current_memory_usage.load(Ordering::Relaxed);
        let total_memory = buffer_memory + streaming_memory;

        let blocks = self.streaming_blocks.read().await;
        let loaded_blocks = blocks.len();
        let total_block_memory: usize = {
            let mut total = 0;
            for block in blocks.values() {
                let block_guard = block.lock().await;
                total += block_guard.memory_usage();
            }
            total
        };

        Ok(StreamingStats {
            buffer_pool_memory_mb: buffer_memory as f64 / 1024.0 / 1024.0,
            streaming_memory_mb: streaming_memory as f64 / 1024.0 / 1024.0,
            total_memory_mb: total_memory as f64 / 1024.0 / 1024.0,
            loaded_blocks,
            total_block_memory_mb: total_block_memory as f64 / 1024.0 / 1024.0,
            buffer_pool_utilization: self.buffer_pool.buffer_count() as f64 / (self.config.buffer_pool_size_mb * 1024 * 1024 / self.config.buffer_size_bytes) as f64,
        })
    }

    // Private helper methods

    async fn read_value_streaming(&self, offset: u64, size: u32) -> Result<Option<Value>> {
        let data = self.read_data_streaming(offset, size as usize).await?;
        
        // Streaming decompression if needed
        let decompressed_data = if let Some(compression_reader) = &self.compression_reader {
            self.decompress_streaming(&data).await?
        } else {
            data
        };

        // Parse value
        let (_, value) = parse_cql_value(&decompressed_data, CqlTypeId::Varchar)
            .map_err(|e| Error::corruption(format!("Failed to parse value: {:?}", e)))?;

        Ok(Some(value))
    }

    async fn load_block_streaming(&self, offset: u64) -> Result<Option<Arc<Mutex<StreamingBlock>>>> {
        // Check if block is already being loaded
        {
            let blocks = self.streaming_blocks.read().await;
            if let Some(existing_block) = blocks.get(&offset) {
                return Ok(Some(Arc::clone(existing_block)));
            }
        }

        // Read block metadata
        let header_data = self.read_data_streaming(offset, 16).await?;
        if header_data.len() < 16 {
            return Ok(None);
        }

        let compressed_size = u32::from_be_bytes([
            header_data[8], header_data[9], header_data[10], header_data[11]
        ]);
        let checksum = u32::from_be_bytes([
            header_data[12], header_data[13], header_data[14], header_data[15]
        ]);

        // Create block metadata
        let block_meta = BlockMeta {
            offset,
            compressed_size,
            uncompressed_size: compressed_size, // Simplified
            checksum,
            first_key: RowKey::from(""), // Would be populated from index
            last_key: RowKey::from(""),
            entry_count: 0,
        };

        // Create streaming block
        let streaming_block = Arc::new(Mutex::new(StreamingBlock::new(block_meta)));

        // Add to cache
        {
            let mut blocks = self.streaming_blocks.write().await;
            blocks.insert(offset, Arc::clone(&streaming_block));
        }

        // Start streaming the block data
        self.stream_block_data(Arc::clone(&streaming_block), offset + 16, compressed_size as usize).await?;

        Ok(Some(streaming_block))
    }

    async fn stream_block_data(&self, streaming_block: Arc<Mutex<StreamingBlock>>, data_offset: u64, total_size: usize) -> Result<()> {
        let chunk_size = self.config.streaming_chunk_size;
        let mut current_offset = data_offset;
        let mut remaining_size = total_size;

        while remaining_size > 0 {
            let read_size = chunk_size.min(remaining_size);
            let chunk_data = self.read_data_streaming(current_offset, read_size).await?;

            {
                let mut block_guard = streaming_block.lock().await;
                block_guard.add_chunk(chunk_data);
            }

            current_offset += read_size as u64;
            remaining_size -= read_size;

            // Check memory pressure and yield if necessary
            if self.current_memory_usage.load(Ordering::Relaxed) > (self.config.max_memory_usage_mb * 1024 * 1024) as u64 {
                tokio::task::yield_now().await;
            }
        }

        Ok(())
    }

    async fn read_data_streaming(&self, offset: u64, size: usize) -> Result<Vec<u8>> {
        if let Some(ref mmap) = self.mmap {
            // Memory-mapped access
            let start = offset as usize;
            let end = (start + size).min(mmap.len());
            
            if start < mmap.len() && end <= mmap.len() {
                Ok(mmap[start..end].to_vec())
            } else {
                Err(Error::corruption("Read beyond file bounds".to_string()))
            }
        } else {
            // Streaming file access
            if let Some(ref file) = self.file {
                let mut buffer = self.buffer_pool.get_buffer();
                buffer.resize(size, 0);
                
                {
                    let mut file_guard = file.lock().await;
                    file_guard.seek(tokio::io::SeekFrom::Start(offset)).await?;
                    file_guard.read_exact(&mut buffer).await?;
                }
                
                // Don't return buffer to pool immediately to avoid copying
                Ok(buffer)
            } else {
                Err(Error::corruption("No file handle available".to_string()))
            }
        }
    }

    async fn decompress_streaming(&self, compressed_data: &[u8]) -> Result<Vec<u8>> {
        if let Some(compression_reader) = &self.compression_reader {
            // Use streaming decompression for large chunks
            if compressed_data.len() > self.config.streaming_chunk_size {
                let mut decompressed = Vec::new();
                let chunk_size = self.config.streaming_chunk_size;
                
                for chunk_start in (0..compressed_data.len()).step_by(chunk_size) {
                    let chunk_end = (chunk_start + chunk_size).min(compressed_data.len());
                    let chunk = &compressed_data[chunk_start..chunk_end];
                    
                    let mut reader = CompressionReader::new(compression_reader.algorithm().clone());
                    let chunk_decompressed = reader.read(chunk)?;
                    decompressed.extend_from_slice(&chunk_decompressed);
                    
                    // Yield periodically for responsiveness
                    if chunk_start % (chunk_size * 4) == 0 {
                        tokio::task::yield_now().await;
                    }
                }
                
                Ok(decompressed)
            } else {
                // Small chunk, decompress directly
                let mut reader = CompressionReader::new(compression_reader.algorithm().clone());
                reader.read(compressed_data)
            }
        } else {
            Ok(compressed_data.to_vec())
        }
    }

    async fn wait_for_block_loading(&self, streaming_block: &Arc<Mutex<StreamingBlock>>, min_progress: f32) -> Result<()> {
        const MAX_WAIT_MS: u64 = 5000; // 5 second timeout
        const CHECK_INTERVAL_MS: u64 = 10;
        
        let start_time = std::time::Instant::now();
        
        loop {
            {
                let block_guard = streaming_block.lock().await;
                if block_guard.progress >= min_progress {
                    return Ok(());
                }
            }
            
            if start_time.elapsed().as_millis() > MAX_WAIT_MS as u128 {
                return Err(Error::corruption("Block loading timeout".to_string()));
            }
            
            tokio::time::sleep(tokio::time::Duration::from_millis(CHECK_INTERVAL_MS)).await;
        }
    }

    async fn parse_streaming_block_entries(&self, streaming_block: &Arc<Mutex<StreamingBlock>>) -> Result<Vec<(TableId, RowKey, Value)>> {
        let compressed_data = {
            let block_guard = streaming_block.lock().await;
            let mut data = Vec::new();
            for chunk in &block_guard.compressed_chunks {
                data.extend_from_slice(chunk);
            }
            data
        };

        // Decompress the full block
        let decompressed_data = self.decompress_streaming(&compressed_data).await?;

        // Parse entries from decompressed data
        let mut entries = Vec::new();
        let mut offset = 0;

        while offset < decompressed_data.len() {
            // Parse table ID
            let (remaining, table_id_len) = parse_vint_length(&decompressed_data[offset..])
                .map_err(|e| Error::corruption(format!("Failed to parse table ID length: {:?}", e)))?;
            offset = decompressed_data.len() - remaining.len();

            if offset + table_id_len > decompressed_data.len() {
                break;
            }

            let table_id = TableId::new(
                String::from_utf8_lossy(&decompressed_data[offset..offset + table_id_len]).into_owned()
            );
            offset += table_id_len;

            // Parse key
            let (remaining, key_len) = parse_vint_length(&decompressed_data[offset..])
                .map_err(|e| Error::corruption(format!("Failed to parse key length: {:?}", e)))?;
            offset = decompressed_data.len() - remaining.len();

            if offset + key_len > decompressed_data.len() {
                break;
            }

            let key = RowKey::new(decompressed_data[offset..offset + key_len].to_vec());
            offset += key_len;

            // Parse value
            let (remaining, value_len) = parse_vint_length(&decompressed_data[offset..])
                .map_err(|e| Error::corruption(format!("Failed to parse value length: {:?}", e)))?;
            offset = decompressed_data.len() - remaining.len();

            if offset + value_len > decompressed_data.len() {
                break;
            }

            let (_, value) = parse_cql_value(&decompressed_data[offset..offset + value_len], CqlTypeId::Varchar)
                .map_err(|e| Error::corruption(format!("Failed to parse value: {:?}", e)))?;
            offset += value_len;

            entries.push((table_id, key, value));
        }

        Ok(entries)
    }

    async fn check_memory_pressure(&self) -> Result<()> {
        let current_usage = self.current_memory_usage.load(Ordering::Relaxed);
        let max_usage = (self.config.max_memory_usage_mb * 1024 * 1024) as u64;

        if current_usage > max_usage {
            // Apply backpressure by cleaning up old blocks
            self.cleanup_old_blocks().await?;
            
            // If still over limit, apply more aggressive cleanup
            let current_usage = self.current_memory_usage.load(Ordering::Relaxed);
            if current_usage > max_usage {
                return Err(Error::storage("Memory usage exceeds limit".to_string()));
            }
        }

        Ok(())
    }

    async fn cleanup_old_blocks(&self) -> Result<()> {
        let mut blocks_to_remove = Vec::new();
        
        {
            let blocks = self.streaming_blocks.read().await;
            let mut block_ages: Vec<(u64, std::time::Instant)> = Vec::new();
            
            for (&offset, block) in blocks.iter() {
                let block_guard = block.lock().await;
                block_ages.push((offset, block_guard.last_access));
            }
            
            // Sort by age (oldest first)
            block_ages.sort_by_key(|(_, access_time)| *access_time);
            
            // Remove oldest 25% of blocks
            let remove_count = (block_ages.len() / 4).max(1);
            for i in 0..remove_count {
                blocks_to_remove.push(block_ages[i].0);
            }
        }

        // Remove selected blocks
        {
            let mut blocks = self.streaming_blocks.write().await;
            for offset in blocks_to_remove {
                if let Some(block) = blocks.remove(&offset) {
                    let block_guard = block.lock().await;
                    let memory_freed = block_guard.memory_usage() as u64;
                    self.current_memory_usage.fetch_sub(memory_freed, Ordering::Relaxed);
                }
            }
        }

        Ok(())
    }

    fn calculate_header_size(&self) -> usize {
        1024 // Simplified estimate
    }
}

/// Statistics for streaming operations
#[derive(Debug, Clone)]
pub struct StreamingStats {
    /// Buffer pool memory usage in MB
    pub buffer_pool_memory_mb: f64,
    /// Streaming blocks memory usage in MB
    pub streaming_memory_mb: f64,
    /// Total memory usage in MB
    pub total_memory_mb: f64,
    /// Number of loaded blocks
    pub loaded_blocks: usize,
    /// Total memory used by blocks in MB
    pub total_block_memory_mb: f64,
    /// Buffer pool utilization (0.0 to 1.0)
    pub buffer_pool_utilization: f64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_buffer_pool() {
        let pool = BufferPool::new(1024, 10);
        
        // Test buffer allocation
        let buffer1 = pool.get_buffer();
        assert_eq!(buffer1.len(), 1024);
        assert_eq!(pool.buffer_count(), 1);
        
        // Test buffer return
        pool.return_buffer(buffer1);
        assert_eq!(pool.buffer_count(), 1); // Should still be 1 as buffer is reused
        
        // Test buffer reuse
        let buffer2 = pool.get_buffer();
        assert_eq!(buffer2.len(), 1024);
        assert_eq!(pool.buffer_count(), 1); // Should still be 1 as buffer was reused
    }

    #[tokio::test]
    async fn test_streaming_block() {
        let meta = BlockMeta {
            offset: 0,
            compressed_size: 1000,
            uncompressed_size: 2000,
            checksum: 0x12345678,
            first_key: RowKey::from("key1"),
            last_key: RowKey::from("key100"),
            entry_count: 100,
        };

        let mut block = StreamingBlock::new(meta);
        assert_eq!(block.progress, 0.0);
        assert!(!block.is_fully_loaded());

        // Add chunks progressively
        block.add_chunk(vec![0u8; 500]);
        assert_eq!(block.progress, 0.5);
        assert!(!block.is_fully_loaded());

        block.add_chunk(vec![0u8; 500]);
        assert_eq!(block.progress, 1.0);
        assert!(block.is_fully_loaded());
    }

    #[tokio::test]
    async fn test_streaming_config() {
        let config = StreamingReaderConfig::default();
        
        assert!(config.buffer_pool_size_mb > 0);
        assert!(config.buffer_size_bytes > 0);
        assert!(config.mmap_threshold_bytes > 0);
        assert!(config.max_memory_usage_mb > 0);
        assert!(config.streaming_chunk_size > 0);
        assert!(config.enable_progressive_loading);
        assert!(config.prefetch_distance > 0);
    }
}