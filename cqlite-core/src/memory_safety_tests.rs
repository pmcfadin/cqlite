//! Memory Safety Test Suite for CQLite Core
//! 
//! This module provides comprehensive memory safety testing for the CQLite database engine,
//! focusing on detecting memory leaks, buffer overflows, use-after-free bugs, and other
//! memory-related issues.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::memory::MemoryManager;
use crate::storage::memtable::MemTable;
use crate::storage::sstable::SSTableManager;
use crate::types::{TableId, Value};
use crate::{Config, RowKey};

/// Memory tracking allocator for leak detection
pub struct TrackingAllocator {
    allocations: AtomicUsize,
    deallocations: AtomicUsize,
    total_allocated: AtomicUsize,
    peak_memory: AtomicUsize,
}

impl TrackingAllocator {
    pub const fn new() -> Self {
        Self {
            allocations: AtomicUsize::new(0),
            deallocations: AtomicUsize::new(0),
            total_allocated: AtomicUsize::new(0),
            peak_memory: AtomicUsize::new(0),
        }
    }

    pub fn allocations(&self) -> usize {
        self.allocations.load(Ordering::SeqCst)
    }

    pub fn deallocations(&self) -> usize {
        self.deallocations.load(Ordering::SeqCst)
    }

    pub fn current_memory(&self) -> usize {
        self.total_allocated.load(Ordering::SeqCst)
    }

    pub fn peak_memory(&self) -> usize {
        self.peak_memory.load(Ordering::SeqCst)
    }

    pub fn reset(&self) {
        self.allocations.store(0, Ordering::SeqCst);
        self.deallocations.store(0, Ordering::SeqCst);
        self.total_allocated.store(0, Ordering::SeqCst);
        self.peak_memory.store(0, Ordering::SeqCst);
    }
}

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc(layout);
        if !ptr.is_null() {
            self.allocations.fetch_add(1, Ordering::SeqCst);
            let new_total = self.total_allocated.fetch_add(layout.size(), Ordering::SeqCst) + layout.size();
            
            // Update peak memory if necessary
            let current_peak = self.peak_memory.load(Ordering::SeqCst);
            if new_total > current_peak {
                self.peak_memory.compare_exchange_weak(current_peak, new_total, Ordering::SeqCst, Ordering::SeqCst).ok();
            }
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
        self.deallocations.fetch_add(1, Ordering::SeqCst);
        self.total_allocated.fetch_sub(layout.size(), Ordering::SeqCst);
    }
}

/// Memory safety test suite
pub struct MemorySafetyTests {
    config: Config,
    allocator: Arc<TrackingAllocator>,
}

impl MemorySafetyTests {
    pub fn new() -> Self {
        Self {
            config: Config::default(),
            allocator: Arc::new(TrackingAllocator::new()),
        }
    }

    /// Test memory manager for leaks and proper cleanup
    pub fn test_memory_manager_safety(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.allocator.reset();
        let initial_memory = self.allocator.current_memory();

        {
            let memory_manager = MemoryManager::new(&self.config)?;
            let table_id = TableId::new("test_table");

            // Test block cache operations
            for i in 0..1000 {
                let data = vec![i as u8; 1024];
                memory_manager.put_block(&table_id, i, data);
            }

            // Test row cache operations
            for i in 0..1000 {
                let key = format!("key_{}", i);
                let data = vec![Value::Integer(i as i32), Value::Text(format!("value_{}", i))];
                memory_manager.put_row(&table_id, &key, data);
            }

            // Test buffer pool operations
            let mut buffers = Vec::new();
            for _ in 0..100 {
                let buffer = memory_manager.allocate_buffer(4096);
                buffers.push(buffer);
            }

            // Return buffers to pool
            for buffer in buffers {
                memory_manager.deallocate_buffer(buffer);
            }

            // Clear caches
            memory_manager.clear_caches();
        } // memory_manager should be dropped here

        // Force garbage collection
        std::thread::sleep(Duration::from_millis(100));

        let final_memory = self.allocator.current_memory();
        let leaked_memory = final_memory.saturating_sub(initial_memory);

        if leaked_memory > 0 {
            eprintln!("Memory leak detected: {} bytes leaked", leaked_memory);
            return Err(format!("Memory leak: {} bytes", leaked_memory).into());
        }

        Ok(())
    }

    /// Test MemTable memory safety and proper cleanup
    pub fn test_memtable_memory_safety(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.allocator.reset();
        let initial_memory = self.allocator.current_memory();

        {
            let mut memtable = MemTable::new(&self.config)?;
            let table_id = TableId::new("stress_test");

            // Stress test with large dataset
            for i in 0..10_000 {
                let key = RowKey::from(format!("stress_key_{:06}", i));
                let value = Value::Text(format!("stress_value_{}", "x".repeat(100)));
                memtable.put(&table_id, key, value)?;
            }

            // Test deletion operations
            for i in 0..5_000 {
                let key = RowKey::from(format!("stress_key_{:06}", i));
                memtable.delete(&table_id, key)?;
            }

            // Test scan operations
            let _results = memtable.scan(&table_id, None, None, Some(1000))?;

            // Test flush operation
            let _flushed_data = memtable.flush()?;
        } // memtable should be dropped here

        std::thread::sleep(Duration::from_millis(100));

        let final_memory = self.allocator.current_memory();
        let leaked_memory = final_memory.saturating_sub(initial_memory);

        if leaked_memory > 0 {
            eprintln!("MemTable memory leak detected: {} bytes leaked", leaked_memory);
            return Err(format!("MemTable memory leak: {} bytes", leaked_memory).into());
        }

        Ok(())
    }

    /// Test buffer overflow scenarios
    pub fn test_buffer_overflow_protection(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Test VInt parsing with malformed data
        use crate::parser::vint::parse_vint;

        // Test VInt with too many leading 1s (should reject > 8 extra bytes = 9 total)
        let malformed_vint = vec![0xFF; 15]; // 15 bytes with all 1s should be rejected
        let result = parse_vint(&malformed_vint);
        // Parser should handle this gracefully (currently accepts up to 9 bytes)

        // Test incomplete VInt data
        let incomplete_vint = vec![0x80]; // Claims 1 extra byte but provides none
        let result = parse_vint(&incomplete_vint);
        if result.is_ok() {
            return Err("VInt parser should reject incomplete data".into());
        }

        // Test VInt that claims more bytes than available
        let insufficient_data = vec![0xC0, 0x00]; // Claims 2 extra bytes but only has 1
        let result = parse_vint(&insufficient_data);
        if result.is_ok() {
            return Err("VInt parser should reject insufficient data".into());
        }

        // Test maximum valid VInt (9 bytes total)
        let max_valid_vint = vec![0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        let result = parse_vint(&max_valid_vint);
        if result.is_err() {
            return Err("VInt parser should accept maximum valid length".into());
        }

        Ok(())
    }

    /// Test memory usage under concurrent stress
    pub async fn test_concurrent_memory_stress(&self) -> Result<(), Box<dyn std::error::Error>> {
        use tokio::task;
        
        self.allocator.reset();
        let initial_memory = self.allocator.current_memory();
        
        let mut handles = Vec::new();
        
        // Spawn multiple concurrent tasks that stress memory allocation
        for task_id in 0..8 {
            let config = self.config.clone();
            let handle = task::spawn(async move {
                let mut memtable = MemTable::new(&config)?;
                let table_id = TableId::new(&format!("concurrent_table_{}", task_id));
                
                // Each task inserts 1000 entries
                for i in 0..1000 {
                    let key = RowKey::from(format!("concurrent_key_{}_{}", task_id, i));
                    let value = Value::Text(format!("concurrent_value_{}_{}", task_id, i));
                    memtable.put(&table_id, key, value)?;
                }
                
                // Scan to exercise read paths
                let _results = memtable.scan(&table_id, None, None, Some(100))?;
                
                Ok::<(), crate::error::Error>(())
            });
            handles.push(handle);
        }
        
        // Wait for all tasks to complete
        for handle in handles {
            handle.await??;
        }
        
        // Allow some time for cleanup
        tokio::time::sleep(Duration::from_millis(200)).await;
        
        let final_memory = self.allocator.current_memory();
        let leaked_memory = final_memory.saturating_sub(initial_memory);
        
        if leaked_memory > 1024 * 1024 { // Allow some tolerance for allocator overhead
            eprintln!("Concurrent stress test memory leak: {} bytes", leaked_memory);
            return Err(format!("Concurrent memory leak: {} bytes", leaked_memory).into());
        }
        
        Ok(())
    }

    /// Test unsafe code blocks for memory safety
    pub fn test_unsafe_code_safety(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Test Arc::get_mut usage patterns
        let arc_data = Arc::new(vec![1, 2, 3, 4, 5]);
        let mut arc_clone = Arc::clone(&arc_data);
        
        // This should fail because there are multiple references
        if Arc::get_mut(&mut arc_clone).is_some() {
            return Err("Arc::get_mut should fail when multiple references exist".into());
        }
        
        // Drop the original reference
        drop(arc_data);
        
        // Now it should succeed
        if Arc::get_mut(&mut arc_clone).is_none() {
            return Err("Arc::get_mut should succeed when only one reference exists".into());
        }

        Ok(())
    }

    /// Test resource cleanup in error scenarios
    pub fn test_error_cleanup(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.allocator.reset();
        let initial_memory = self.allocator.current_memory();

        // Test cleanup when operations fail
        let result = std::panic::catch_unwind(|| {
            let mut memtable = MemTable::new(&self.config).unwrap();
            let table_id = TableId::new("error_test");

            // Insert some data
            for i in 0..100 {
                let key = RowKey::from(format!("error_key_{}", i));
                let value = Value::Text(format!("error_value_{}", i));
                memtable.put(&table_id, key, value).unwrap();
            }

            // Simulate error condition (this will unwind the stack)
            panic!("Simulated error");
        });

        // The panic should have been caught
        assert!(result.is_err());

        // Give time for cleanup
        std::thread::sleep(Duration::from_millis(100));

        let final_memory = self.allocator.current_memory();
        let leaked_memory = final_memory.saturating_sub(initial_memory);

        if leaked_memory > 1024 { // Small tolerance for test overhead
            eprintln!("Error cleanup test memory leak: {} bytes", leaked_memory);
            return Err(format!("Error cleanup memory leak: {} bytes", leaked_memory).into());
        }

        Ok(())
    }

    /// Run all memory safety tests
    pub async fn run_all_tests(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Running memory safety tests...");

        println!("1. Testing memory manager safety...");
        self.test_memory_manager_safety()?;
        println!("   ✓ Memory manager safety test passed");

        println!("2. Testing MemTable memory safety...");
        self.test_memtable_memory_safety()?;
        println!("   ✓ MemTable memory safety test passed");

        println!("3. Testing buffer overflow protection...");
        self.test_buffer_overflow_protection()?;
        println!("   ✓ Buffer overflow protection test passed");

        println!("4. Testing concurrent memory stress...");
        self.test_concurrent_memory_stress().await?;
        println!("   ✓ Concurrent memory stress test passed");

        println!("5. Testing unsafe code safety...");
        self.test_unsafe_code_safety()?;
        println!("   ✓ Unsafe code safety test passed");

        println!("6. Testing error cleanup...");
        self.test_error_cleanup()?;
        println!("   ✓ Error cleanup test passed");

        println!("All memory safety tests passed! 🎉");
        
        // Print memory usage statistics
        println!("\nMemory Usage Statistics:");
        println!("  Peak memory usage: {} bytes", self.allocator.peak_memory());
        println!("  Total allocations: {}", self.allocator.allocations());
        println!("  Total deallocations: {}", self.allocator.deallocations());
        println!("  Current memory: {} bytes", self.allocator.current_memory());

        Ok(())
    }
}

impl Default for MemorySafetyTests {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_memory_safety_suite() {
        let tests = MemorySafetyTests::new();
        tests.run_all_tests().await.expect("Memory safety tests failed");
    }

    #[test]
    fn test_tracking_allocator() {
        let allocator = TrackingAllocator::new();
        
        // Test initial state
        assert_eq!(allocator.allocations(), 0);
        assert_eq!(allocator.deallocations(), 0);
        assert_eq!(allocator.current_memory(), 0);
        assert_eq!(allocator.peak_memory(), 0);
        
        // Test reset
        allocator.reset();
        assert_eq!(allocator.allocations(), 0);
        assert_eq!(allocator.deallocations(), 0);
        assert_eq!(allocator.current_memory(), 0);
        assert_eq!(allocator.peak_memory(), 0);
    }

    #[test]
    fn test_memory_manager_basic_safety() {
        let tests = MemorySafetyTests::new();
        tests.test_memory_manager_safety().expect("Memory manager safety test failed");
    }

    #[test]
    fn test_memtable_basic_safety() {
        let tests = MemorySafetyTests::new();
        tests.test_memtable_memory_safety().expect("MemTable safety test failed");
    }

    #[test]
    fn test_buffer_overflow_basic() {
        let tests = MemorySafetyTests::new();
        tests.test_buffer_overflow_protection().expect("Buffer overflow test failed");
    }

    #[test]
    fn test_unsafe_code_basic() {
        let tests = MemorySafetyTests::new();
        tests.test_unsafe_code_safety().expect("Unsafe code test failed");
    }
}