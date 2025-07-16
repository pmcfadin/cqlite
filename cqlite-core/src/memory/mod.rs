//! Memory management for CQLite

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

use crate::{types::TableId, Config, Result, Value};

/// Memory manager for caching and buffer management
#[derive(Debug)]
pub struct MemoryManager {
    /// Block cache for storage blocks
    block_cache: Arc<RwLock<BlockCache>>,

    /// Row cache for frequently accessed rows
    row_cache: Arc<RwLock<RowCache>>,

    /// Buffer pool for memory allocation
    buffer_pool: Arc<RwLock<BufferPool>>,

    /// Configuration
    config: Config,

    /// Memory statistics
    stats: Arc<RwLock<MemoryStats>>,
}

/// Block cache for storage blocks
#[derive(Debug)]
struct BlockCache {
    /// Cached blocks
    blocks: HashMap<BlockKey, Arc<Block>>,

    /// Maximum cache size
    max_size: usize,

    /// Current size
    current_size: usize,

    /// LRU ordering
    lru_order: Vec<BlockKey>,
}

/// Row cache for frequently accessed rows
#[derive(Debug)]
struct RowCache {
    /// Cached rows
    rows: HashMap<RowKey, Arc<CachedRow>>,

    /// Maximum cache size
    max_size: usize,

    /// Current size
    current_size: usize,

    /// LRU ordering
    lru_order: Vec<RowKey>,
}

/// Buffer pool for memory allocation
#[derive(Debug)]
struct BufferPool {
    /// Free buffers by size
    free_buffers: HashMap<usize, Vec<Vec<u8>>>,

    /// Allocated buffers
    allocated_count: usize,

    /// Total memory used
    total_memory: usize,

    /// Maximum memory limit
    max_memory: usize,
}

/// Block key for cache lookup
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct BlockKey {
    table_id: TableId,
    block_id: u64,
}

/// Row key for cache lookup
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct RowKey {
    table_id: TableId,
    row_key: String,
}

/// Cached block
#[derive(Debug)]
struct Block {
    /// Block data
    data: Vec<u8>,

    /// Block size
    size: usize,

    /// Last access time
    last_access: std::time::Instant,
}

/// Cached row
#[derive(Debug)]
struct CachedRow {
    /// Row data
    data: Vec<Value>,

    /// Row size estimate
    size: usize,

    /// Last access time
    last_access: std::time::Instant,
}

impl MemoryManager {
    /// Create a new memory manager
    pub fn new(config: &Config) -> Result<Self> {
        let block_cache = Arc::new(RwLock::new(BlockCache::new(config.memory.block_cache.max_size as usize)));
        let row_cache = Arc::new(RwLock::new(RowCache::new(config.memory.row_cache.max_size as usize)));
        let buffer_pool = Arc::new(RwLock::new(BufferPool::new(config.memory.max_memory as usize)));

        Ok(Self {
            block_cache,
            row_cache,
            buffer_pool,
            config: config.clone(),
            stats: Arc::new(RwLock::new(MemoryStats::default())),
        })
    }

    /// Get a block from cache
    pub fn get_block(&self, table_id: &TableId, block_id: u64) -> Option<Arc<Block>> {
        let key = BlockKey {
            table_id: table_id.clone(),
            block_id,
        };

        let mut cache = self.block_cache.write();
        
        // Check if block exists and clone it before any mutations
        let block_option = cache.blocks.get(&key).map(|block| Arc::clone(block));
        
        if let Some(block) = block_option {
            // Update LRU order (now safe since we don't hold immutable borrow)
            if let Some(pos) = cache.lru_order.iter().position(|k| k == &key) {
                cache.lru_order.remove(pos);
            }
            cache.lru_order.push(key);

            // Update access time
            let mut block_clone = block.clone();
            if let Some(block_mut) = Arc::get_mut(&mut block_clone) {
                block_mut.last_access = std::time::Instant::now();
            }

            // Update stats
            {
                let mut stats = self.stats.write();
                stats.block_cache_hits += 1;
            }

            Some(block)
        } else {
            // Update stats
            {
                let mut stats = self.stats.write();
                stats.block_cache_misses += 1;
            }

            None
        }
    }

    /// Put a block in cache
    pub fn put_block(&self, table_id: &TableId, block_id: u64, data: Vec<u8>) {
        let key = BlockKey {
            table_id: table_id.clone(),
            block_id,
        };

        let block = Arc::new(Block {
            size: data.len(),
            data,
            last_access: std::time::Instant::now(),
        });

        let mut cache = self.block_cache.write();

        // Check if we need to evict
        while cache.current_size + block.size > cache.max_size && !cache.blocks.is_empty() {
            if let Some(evict_key) = cache.lru_order.first().cloned() {
                cache.lru_order.remove(0);
                if let Some(evicted_block) = cache.blocks.remove(&evict_key) {
                    cache.current_size -= evicted_block.size;
                }
            } else {
                break;
            }
        }

        // Add new block
        cache.current_size += block.size;
        cache.blocks.insert(key.clone(), block);
        cache.lru_order.push(key);
    }

    /// Get a row from cache
    pub fn get_row(&self, table_id: &TableId, row_key: &str) -> Option<Arc<CachedRow>> {
        let key = RowKey {
            table_id: table_id.clone(),
            row_key: row_key.to_string(),
        };

        let mut cache = self.row_cache.write();
        
        // Check if row exists and clone it before any mutations
        let row_option = cache.rows.get(&key).map(|row| Arc::clone(row));
        
        if let Some(row) = row_option {
            // Update LRU order (now safe since we don't hold immutable borrow)
            if let Some(pos) = cache.lru_order.iter().position(|k| k == &key) {
                cache.lru_order.remove(pos);
            }
            cache.lru_order.push(key);

            // Update stats
            {
                let mut stats = self.stats.write();
                stats.row_cache_hits += 1;
            }

            Some(row)
        } else {
            // Update stats
            {
                let mut stats = self.stats.write();
                stats.row_cache_misses += 1;
            }

            None
        }
    }

    /// Put a row in cache
    pub fn put_row(&self, table_id: &TableId, row_key: &str, data: Vec<Value>) {
        let key = RowKey {
            table_id: table_id.clone(),
            row_key: row_key.to_string(),
        };

        let size = self.estimate_row_size(&data);
        let row = Arc::new(CachedRow {
            data,
            size,
            last_access: std::time::Instant::now(),
        });

        let mut cache = self.row_cache.write();

        // Check if we need to evict
        while cache.current_size + row.size > cache.max_size && !cache.rows.is_empty() {
            if let Some(evict_key) = cache.lru_order.first().cloned() {
                cache.lru_order.remove(0);
                if let Some(evicted_row) = cache.rows.remove(&evict_key) {
                    cache.current_size -= evicted_row.size;
                }
            } else {
                break;
            }
        }

        // Add new row
        cache.current_size += row.size;
        cache.rows.insert(key.clone(), row);
        cache.lru_order.push(key);
    }

    /// Allocate buffer from pool
    pub fn allocate_buffer(&self, size: usize) -> Vec<u8> {
        let mut pool = self.buffer_pool.write();

        if let Some(buffers) = pool.free_buffers.get_mut(&size) {
            if let Some(buffer) = buffers.pop() {
                pool.allocated_count += 1;
                return buffer;
            }
        }

        // Allocate new buffer
        pool.allocated_count += 1;
        pool.total_memory += size;
        vec![0u8; size]
    }

    /// Return buffer to pool
    pub fn deallocate_buffer(&self, mut buffer: Vec<u8>) {
        let size = buffer.len();
        buffer.clear();
        buffer.shrink_to_fit();

        let mut pool = self.buffer_pool.write();
        pool.free_buffers
            .entry(size)
            .or_insert_with(Vec::new)
            .push(buffer);
        pool.allocated_count -= 1;
    }

    /// Get memory statistics
    pub fn stats(&self) -> Result<MemoryStats> {
        let stats = self.stats.read();
        Ok(stats.clone())
    }

    /// Clear all caches
    pub fn clear_caches(&self) {
        {
            let mut cache = self.block_cache.write();
            cache.blocks.clear();
            cache.lru_order.clear();
            cache.current_size = 0;
        }

        {
            let mut cache = self.row_cache.write();
            cache.rows.clear();
            cache.lru_order.clear();
            cache.current_size = 0;
        }
    }

    /// Estimate row size
    fn estimate_row_size(&self, data: &[Value]) -> usize {
        data.iter().map(|v| self.estimate_value_size(v)).sum()
    }

    /// Estimate value size
    fn estimate_value_size(&self, value: &Value) -> usize {
        match value {
            Value::Null => 1,
            Value::Boolean(_) => 1,
            Value::Integer(_) => 8,
            Value::BigInt(_) => 8,
            Value::Float(_) => 8,
            Value::Double(_) => 8,
            Value::Text(s) => s.len(),
            Value::Blob(b) => b.len(),
            Value::Timestamp(_) => 8,
            Value::Date(_) => 4,
            Value::Time(_) => 8,
            Value::Uuid(_) => 16,
            Value::List(items) => items.iter().map(|v| self.estimate_value_size(v)).sum(),
            Value::Map(map) => map
                .iter()
                .map(|(k, v)| k.len() + self.estimate_value_size(v))
                .sum(),
            Value::Set(items) => items.iter().map(|v| self.estimate_value_size(v)).sum(),
            Value::VarInt(_) => 16,
            Value::Decimal(_) => 16,
            Value::Duration(_) => 8,
            Value::Inet(_) => 16,
        }
    }
}

impl BlockCache {
    fn new(max_size: usize) -> Self {
        Self {
            blocks: HashMap::new(),
            max_size,
            current_size: 0,
            lru_order: Vec::new(),
        }
    }
}

impl RowCache {
    fn new(max_size: usize) -> Self {
        Self {
            rows: HashMap::new(),
            max_size,
            current_size: 0,
            lru_order: Vec::new(),
        }
    }
}

impl BufferPool {
    fn new(max_memory: usize) -> Self {
        Self {
            free_buffers: HashMap::new(),
            allocated_count: 0,
            total_memory: 0,
            max_memory,
        }
    }
}

/// Memory statistics
#[derive(Debug, Clone, Default)]
pub struct MemoryStats {
    /// Block cache hits
    pub block_cache_hits: u64,

    /// Block cache misses
    pub block_cache_misses: u64,

    /// Row cache hits
    pub row_cache_hits: u64,

    /// Row cache misses
    pub row_cache_misses: u64,

    /// Total memory used
    pub total_memory_used: usize,

    /// Buffer pool allocations
    pub buffer_allocations: u64,

    /// Buffer pool deallocations
    pub buffer_deallocations: u64,
}

impl MemoryStats {
    /// Calculate block cache hit rate
    pub fn block_cache_hit_rate(&self) -> f64 {
        let total = self.block_cache_hits + self.block_cache_misses;
        if total > 0 {
            self.block_cache_hits as f64 / total as f64
        } else {
            0.0
        }
    }

    /// Calculate row cache hit rate
    pub fn row_cache_hit_rate(&self) -> f64 {
        let total = self.row_cache_hits + self.row_cache_misses;
        if total > 0 {
            self.row_cache_hits as f64 / total as f64
        } else {
            0.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::TableId;

    #[test]
    fn test_memory_manager_creation() {
        let config = Config::default();
        let manager = MemoryManager::new(&config).unwrap();

        let stats = manager.stats().unwrap();
        assert_eq!(stats.block_cache_hits, 0);
        assert_eq!(stats.block_cache_misses, 0);
    }

    #[test]
    fn test_block_cache() {
        let config = Config::default();
        let manager = MemoryManager::new(&config).unwrap();

        let table_id = TableId::new("test_table");
        let block_id = 1;
        let data = vec![1, 2, 3, 4, 5];

        // Cache miss
        let result = manager.get_block(&table_id, block_id);
        assert!(result.is_none());

        // Put block
        manager.put_block(&table_id, block_id, data.clone());

        // Cache hit
        let result = manager.get_block(&table_id, block_id);
        assert!(result.is_some());
        assert_eq!(result.unwrap().data, data);
    }

    #[test]
    fn test_row_cache() {
        let config = Config::default();
        let manager = MemoryManager::new(&config).unwrap();

        let table_id = TableId::new("test_table");
        let row_key = "test_key";
        let data = vec![Value::Integer(42), Value::Text("hello".to_string())];

        // Cache miss
        let result = manager.get_row(&table_id, row_key);
        assert!(result.is_none());

        // Put row
        manager.put_row(&table_id, row_key, data.clone());

        // Cache hit
        let result = manager.get_row(&table_id, row_key);
        assert!(result.is_some());
        assert_eq!(result.unwrap().data, data);
    }

    #[test]
    fn test_buffer_pool() {
        let config = Config::default();
        let manager = MemoryManager::new(&config).unwrap();

        let size = 1024;
        let buffer = manager.allocate_buffer(size);
        assert_eq!(buffer.len(), size);

        manager.deallocate_buffer(buffer);

        // Should reuse buffer
        let buffer2 = manager.allocate_buffer(size);
        assert_eq!(buffer2.len(), size);
    }
}
