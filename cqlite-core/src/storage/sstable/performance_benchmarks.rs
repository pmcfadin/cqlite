//! Performance benchmarks for SSTable readers using real test data
//!
//! This module provides comprehensive benchmarking of SSTable reader performance
//! using actual Cassandra 5 SSTable files from the test environment.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::fs;

use crate::{
    platform::Platform,
    types::TableId,
    Config, Result, RowKey,
};

use super::{
    reader::SSTableReader,
    // optimized_reader::OptimizedSSTableReader, // TODO: Re-enable when optimized_reader module is available
    streaming_reader::{StreamingSSTableReader, StreamingStats},
};

/// Performance benchmark results
#[derive(Debug, Clone)]
pub struct BenchmarkResults {
    /// Reader type
    pub reader_type: String,
    /// File size in bytes
    pub file_size: u64,
    /// Total benchmark duration
    pub total_duration: Duration,
    /// Operations per second
    pub ops_per_second: f64,
    /// Memory usage statistics
    pub memory_stats: MemoryStats,
    /// I/O statistics
    pub io_stats: IoStats,
    /// Error count
    pub error_count: usize,
}

/// Memory usage statistics
#[derive(Debug, Clone)]
pub struct MemoryStats {
    /// Peak memory usage in MB
    pub peak_memory_mb: f64,
    /// Average memory usage in MB
    pub average_memory_mb: f64,
    /// Memory efficiency (data processed / memory used)
    pub efficiency_ratio: f64,
}

/// I/O statistics
#[derive(Debug, Clone)]
pub struct IoStats {
    /// Total bytes read
    pub bytes_read: u64,
    /// Read operations count
    pub read_operations: u64,
    /// Average read latency
    pub avg_read_latency_ms: f64,
    /// Cache hit rate
    pub cache_hit_rate: f64,
}

/// Benchmark suite for SSTable readers
pub struct PerformanceBenchmarks {
    /// Test data directory
    test_data_dir: PathBuf,
    /// Platform abstraction
    platform: Arc<Platform>,
    /// Configuration
    config: Config,
}

impl PerformanceBenchmarks {
    /// Create a new benchmark suite
    pub async fn new(test_data_dir: &Path) -> Result<Self> {
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await?);
        
        Ok(Self {
            test_data_dir: test_data_dir.to_path_buf(),
            platform,
            config,
        })
    }

    /// Run comprehensive benchmarks on all available test files
    pub async fn run_comprehensive_benchmarks(&self) -> Result<Vec<BenchmarkResults>> {
        let mut results = Vec::new();
        
        // Discover test SSTable files
        let test_files = self.discover_test_files().await?;
        
        println!("🚀 Running comprehensive SSTable performance benchmarks");
        println!("📁 Test data directory: {}", self.test_data_dir.display());
        println!("📊 Found {} test files", test_files.len());
        
        for (table_name, file_path) in test_files {
            println!("\n📋 Benchmarking table: {}", table_name);
            println!("📄 File: {}", file_path.display());
            
            let file_size = fs::metadata(&file_path).await?.len();
            println!("📏 File size: {:.2} MB", file_size as f64 / 1024.0 / 1024.0);
            
            // Benchmark standard reader
            if let Ok(standard_result) = self.benchmark_standard_reader(&file_path, &table_name).await {
                results.push(standard_result);
            }
            
            // Benchmark optimized reader
            if let Ok(optimized_result) = self.benchmark_optimized_reader(&file_path, &table_name).await {
                results.push(optimized_result);
            }
            
            // Benchmark streaming reader
            if let Ok(streaming_result) = self.benchmark_streaming_reader(&file_path, &table_name).await {
                results.push(streaming_result);
            }
        }
        
        // Print summary
        self.print_benchmark_summary(&results);
        
        Ok(results)
    }

    /// Benchmark standard SSTable reader
    pub async fn benchmark_standard_reader(&self, file_path: &Path, table_name: &str) -> Result<BenchmarkResults> {
        println!("  🔍 Testing standard reader...");
        
        let start_time = Instant::now();
        let reader = SSTableReader::open(file_path, &self.config, Arc::clone(&self.platform)).await?;
        
        let table_id = TableId::new(table_name.to_string());
        
        // Perform various read operations
        let mut ops_count = 0;
        let mut error_count = 0;
        let memory_start = get_memory_usage();
        
        // Test sequential scan
        match reader.scan(&table_id, None, None, Some(100)).await {
            Ok(results) => {
                ops_count += results.len();
                println!("    ✅ Sequential scan: {} results", results.len());
            }
            Err(_) => {
                error_count += 1;
                println!("    ❌ Sequential scan failed");
            }
        }
        
        // Test random access (if we have results from scan)
        if ops_count > 0 {
            // Simulate some random key accesses
            for i in 0..10 {
                let test_key = RowKey::from(format!("test_key_{}", i));
                match reader.get(&table_id, &test_key).await {
                    Ok(_) => ops_count += 1,
                    Err(_) => error_count += 1,
                }
            }
        }
        
        let memory_end = get_memory_usage();
        let total_duration = start_time.elapsed();
        let file_size = fs::metadata(file_path).await?.len();
        
        let stats = reader.stats().await?;
        
        Ok(BenchmarkResults {
            reader_type: format!("Standard ({})", table_name),
            file_size,
            total_duration,
            ops_per_second: ops_count as f64 / total_duration.as_secs_f64(),
            memory_stats: MemoryStats {
                peak_memory_mb: memory_end,
                average_memory_mb: (memory_start + memory_end) / 2.0,
                efficiency_ratio: file_size as f64 / (memory_end * 1024.0 * 1024.0),
            },
            io_stats: IoStats {
                bytes_read: file_size,
                read_operations: ops_count as u64,
                avg_read_latency_ms: total_duration.as_millis() as f64 / ops_count as f64,
                cache_hit_rate: stats.cache_hit_rate,
            },
            error_count,
        })
    }

    /// Benchmark optimized SSTable reader (TODO: Re-enable when OptimizedSSTableReader is available)
    pub async fn benchmark_optimized_reader(&self, _file_path: &Path, _table_name: &str) -> Result<BenchmarkResults> {
        // TODO: Re-enable when OptimizedSSTableReader is available
        Err(crate::Error::not_found("OptimizedSSTableReader not available".to_string()))
    }

    /// Benchmark streaming SSTable reader
    pub async fn benchmark_streaming_reader(&self, file_path: &Path, table_name: &str) -> Result<BenchmarkResults> {
        println!("  🌊 Testing streaming reader...");
        
        let start_time = Instant::now();
        let reader = StreamingSSTableReader::open(file_path, &self.config, Arc::clone(&self.platform)).await?;
        
        let table_id = TableId::new(table_name.to_string());
        
        let mut ops_count = 0;
        let mut error_count = 0;
        let memory_start = get_memory_usage();
        
        // Test streaming scan
        match reader.scan_streaming(&table_id, None, None, Some(100)).await {
            Ok(results) => {
                ops_count += results.len();
                println!("    ✅ Streaming scan: {} results", results.len());
            }
            Err(_) => {
                error_count += 1;
                println!("    ❌ Streaming scan failed");
            }
        }
        
        // Test streaming get operations
        for i in 0..10 {
            let test_key = RowKey::from(format!("test_key_{}", i));
            match reader.get_streaming(&table_id, &test_key).await {
                Ok(_) => ops_count += 1,
                Err(_) => error_count += 1,
            }
        }
        
        let memory_end = get_memory_usage();
        let total_duration = start_time.elapsed();
        let file_size = fs::metadata(file_path).await?.len();
        
        let streaming_stats = reader.get_streaming_stats().await?;
        
        Ok(BenchmarkResults {
            reader_type: format!("Streaming ({})", table_name),
            file_size,
            total_duration,
            ops_per_second: ops_count as f64 / total_duration.as_secs_f64(),
            memory_stats: MemoryStats {
                peak_memory_mb: streaming_stats.total_memory_mb,
                average_memory_mb: streaming_stats.total_memory_mb,
                efficiency_ratio: file_size as f64 / (streaming_stats.total_memory_mb * 1024.0 * 1024.0),
            },
            io_stats: IoStats {
                bytes_read: file_size,
                read_operations: ops_count as u64,
                avg_read_latency_ms: total_duration.as_millis() as f64 / ops_count as f64,
                cache_hit_rate: streaming_stats.buffer_pool_utilization,
            },
            error_count,
        })
    }

    /// Discover available test SSTable files
    async fn discover_test_files(&self) -> Result<Vec<(String, PathBuf)>> {
        let mut test_files = Vec::new();
        
        // Look for SSTable directories
        let sstables_dir = self.test_data_dir.join("sstables");
        if !sstables_dir.exists() {
            return Ok(test_files);
        }
        
        let mut dir_entries = fs::read_dir(&sstables_dir).await?;
        while let Some(entry) = dir_entries.next_entry().await? {
            let path = entry.path();
            if path.is_dir() {
                // Look for Data.db files
                let data_file = path.join("nb-1-big-Data.db");
                if data_file.exists() {
                    if let Some(table_name) = path.file_name().and_then(|n| n.to_str()) {
                        // Extract table name from directory name (format: tablename-uuid)
                        let clean_name = table_name.split('-').next().unwrap_or(table_name);
                        test_files.push((clean_name.to_string(), data_file));
                    }
                }
            }
        }
        
        Ok(test_files)
    }

    /// Print benchmark summary
    fn print_benchmark_summary(&self, results: &[BenchmarkResults]) {
        println!("\n📊 BENCHMARK SUMMARY");
        println!("═══════════════════════════════════════════════════════════════");
        
        for result in results {
            println!("\n📋 {}", result.reader_type);
            println!("   📏 File size: {:.2} MB", result.file_size as f64 / 1024.0 / 1024.0);
            println!("   ⏱️  Duration: {:.2}ms", result.total_duration.as_millis());
            println!("   🚀 Ops/sec: {:.2}", result.ops_per_second);
            println!("   💾 Peak memory: {:.2} MB", result.memory_stats.peak_memory_mb);
            println!("   📈 Efficiency: {:.2}", result.memory_stats.efficiency_ratio);
            println!("   📡 Cache hit rate: {:.2}%", result.io_stats.cache_hit_rate * 100.0);
            if result.error_count > 0 {
                println!("   ❌ Errors: {}", result.error_count);
            }
        }
        
        // Find best performers
        if !results.is_empty() {
            let fastest = results.iter().max_by(|a, b| a.ops_per_second.partial_cmp(&b.ops_per_second).unwrap()).unwrap();
            let most_efficient = results.iter().max_by(|a, b| a.memory_stats.efficiency_ratio.partial_cmp(&b.memory_stats.efficiency_ratio).unwrap()).unwrap();
            
            println!("\n🏆 PERFORMANCE WINNERS");
            println!("   🚀 Fastest: {} ({:.2} ops/sec)", fastest.reader_type, fastest.ops_per_second);
            println!("   💾 Most efficient: {} (ratio: {:.2})", most_efficient.reader_type, most_efficient.memory_stats.efficiency_ratio);
        }
    }
}

/// Memory usage monitoring helper
pub struct MemoryMonitor {
    start_time: Instant,
    samples: Vec<(Instant, f64)>,
}

impl MemoryMonitor {
    pub fn new() -> Self {
        Self {
            start_time: Instant::now(),
            samples: Vec::new(),
        }
    }

    pub fn sample(&mut self) {
        let now = Instant::now();
        let memory_mb = self.get_current_memory_mb();
        self.samples.push((now, memory_mb));
    }

    pub fn peak_memory(&self) -> f64 {
        self.samples.iter().map(|(_, mem)| *mem).fold(0.0, f64::max)
    }

    pub fn average_memory(&self) -> f64 {
        if self.samples.is_empty() {
            return 0.0;
        }
        let sum: f64 = self.samples.iter().map(|(_, mem)| *mem).sum();
        sum / self.samples.len() as f64
    }

    fn get_current_memory_mb(&self) -> f64 {
        // Platform-specific memory monitoring would go here
        // For now, return a placeholder
        0.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_benchmark_creation() {
        let temp_dir = TempDir::new().unwrap();
        let benchmarks = PerformanceBenchmarks::new(temp_dir.path()).await;
        assert!(benchmarks.is_ok());
    }

    #[tokio::test]
    async fn test_memory_monitor() {
        let mut monitor = MemoryMonitor::new();
        monitor.sample();
        
        assert!(monitor.peak_memory() >= 0.0);
        assert!(monitor.average_memory() >= 0.0);
    }

    #[test]
    fn test_benchmark_results() {
        let results = BenchmarkResults {
            reader_type: "Test".to_string(),
            file_size: 1024 * 1024, // 1MB
            total_duration: Duration::from_millis(1000),
            ops_per_second: 100.0,
            memory_stats: MemoryStats {
                peak_memory_mb: 10.0,
                average_memory_mb: 8.0,
                efficiency_ratio: 0.1,
            },
            io_stats: IoStats {
                bytes_read: 1024 * 1024,
                read_operations: 100,
                avg_read_latency_ms: 10.0,
                cache_hit_rate: 0.8,
            },
            error_count: 0,
        };

        assert_eq!(results.reader_type, "Test");
        assert_eq!(results.file_size, 1024 * 1024);
        assert_eq!(results.ops_per_second, 100.0);
        assert_eq!(results.error_count, 0);
    }
}

/// Get current memory usage (simplified)
fn get_memory_usage() -> f64 {
    // In a real implementation, this would use platform-specific memory monitoring
    // For now, return a placeholder value
    0.0
}