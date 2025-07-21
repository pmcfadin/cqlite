//! SSTable performance benchmark suite
//! Tests write/read performance, compression efficiency, and scalability

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cqlite_core::{
    config::CompressionAlgorithm,
    platform::Platform,
    storage::sstable::{
        reader::SSTableReader,
        writer::SSTableWriter,
    },
    types::TableId,
    Config, Result, RowKey, Value,
};

use tempfile::TempDir;

/// Benchmark configuration
#[derive(Debug, Clone)]
pub struct BenchmarkConfig {
    /// Number of entries to write in performance tests
    pub entry_count: usize,
    /// Value size for synthetic data
    pub value_size: usize,
    /// Number of random reads to perform
    pub random_read_count: usize,
    /// Whether to enable compression
    pub enable_compression: bool,
    /// Compression algorithm to test
    pub compression_algorithm: CompressionAlgorithm,
    /// Whether to enable bloom filters
    pub enable_bloom_filters: bool,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            entry_count: 10_000,
            value_size: 1024,
            random_read_count: 1_000,
            enable_compression: true,
            compression_algorithm: CompressionAlgorithm::Lz4,
            enable_bloom_filters: true,
        }
    }
}

/// Benchmark results
#[derive(Debug, Clone)]
pub struct BenchmarkResults {
    pub write_performance: WritePerformance,
    pub read_performance: ReadPerformance,
    pub compression_stats: CompressionStats,
    pub file_stats: FileStats,
    pub memory_usage: MemoryUsage,
}

#[derive(Debug, Clone)]
pub struct WritePerformance {
    pub duration: Duration,
    pub entries_per_second: f64,
    pub bytes_per_second: f64,
    pub average_latency_micros: f64,
}

#[derive(Debug, Clone)]
pub struct ReadPerformance {
    pub sequential_duration: Duration,
    pub random_duration: Duration,
    pub sequential_ops_per_sec: f64,
    pub random_ops_per_sec: f64,
    pub cache_hit_rate: f64,
}

#[derive(Debug, Clone)]
pub struct CompressionStats {
    pub enabled: bool,
    pub algorithm: String,
    pub original_size: u64,
    pub compressed_size: u64,
    pub ratio: f64,
    pub space_saved_mb: f64,
}

#[derive(Debug, Clone)]
pub struct FileStats {
    pub total_size_bytes: u64,
    pub header_size: u64,
    pub data_size: u64,
    pub index_size: u64,
    pub bloom_filter_size: u64,
    pub metadata_overhead_percent: f64,
}

#[derive(Debug, Clone)]
pub struct MemoryUsage {
    pub peak_writer_memory_mb: f64,
    pub peak_reader_memory_mb: f64,
    pub index_memory_mb: f64,
    pub bloom_filter_memory_mb: f64,
}

/// SSTable performance benchmark suite
pub struct SSTableBenchmark {
    platform: Arc<Platform>,
    test_dir: TempDir,
}

impl SSTableBenchmark {
    /// Create a new benchmark suite
    pub async fn new() -> Result<Self> {
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await?);
        let test_dir = TempDir::new().map_err(|e| {
            cqlite_core::error::Error::storage(format!("Failed to create temp dir: {}", e))
        })?;

        Ok(Self {
            platform,
            test_dir,
        })
    }

    /// Run comprehensive benchmark suite
    pub async fn run_benchmark(&self, config: BenchmarkConfig) -> Result<BenchmarkResults> {
        println!("🚀 Starting SSTable benchmark suite");
        println!("Configuration: {:?}", config);

        // Setup test configuration
        let mut test_config = Config::default();
        test_config.storage.compression.enabled = config.enable_compression;
        test_config.storage.compression.algorithm = config.compression_algorithm.clone();
        test_config.storage.enable_bloom_filters = config.enable_bloom_filters;
        test_config.storage.bloom_filter_fp_rate = 0.01;

        let test_path = self.test_dir.path().join("benchmark.sst");

        // Benchmark write performance
        println!("📝 Benchmarking write performance...");
        let write_perf = self.benchmark_write_performance(&test_path, &test_config, &config).await?;

        // Benchmark read performance  
        println!("📖 Benchmarking read performance...");
        let read_perf = self.benchmark_read_performance(&test_path, &test_config, &config).await?;

        // Gather compression statistics
        let compression_stats = self.analyze_compression(&test_path, &config).await?;

        // Analyze file structure
        let file_stats = self.analyze_file_structure(&test_path).await?;

        // Estimate memory usage
        let memory_usage = self.estimate_memory_usage(&config);

        Ok(BenchmarkResults {
            write_performance: write_perf,
            read_performance: read_perf,
            compression_stats,
            file_stats,
            memory_usage,
        })
    }

    /// Benchmark write performance
    async fn benchmark_write_performance(
        &self,
        path: &Path,
        config: &Config,
        bench_config: &BenchmarkConfig,
    ) -> Result<WritePerformance> {
        let start_time = SystemTime::now();
        let mut writer = SSTableWriter::create(path, config, self.platform.clone()).await?;

        let table_id = TableId::new("benchmark_table");
        
        // Generate test data
        let test_value = "x".repeat(bench_config.value_size);
        
        for i in 0..bench_config.entry_count {
            let key = RowKey::from(format!("key_{:08}", i));
            let value = Value::Text(format!("{}{}", test_value, i));
            writer.add_entry(&table_id, key, value).await?;
        }

        writer.finish().await?;
        let duration = start_time.elapsed().unwrap();

        let entries_per_second = bench_config.entry_count as f64 / duration.as_secs_f64();
        let total_bytes = bench_config.entry_count * (bench_config.value_size + 20); // Estimate
        let bytes_per_second = total_bytes as f64 / duration.as_secs_f64();
        let average_latency_micros = duration.as_micros() as f64 / bench_config.entry_count as f64;

        Ok(WritePerformance {
            duration,
            entries_per_second,
            bytes_per_second,
            average_latency_micros,
        })
    }

    /// Benchmark read performance
    async fn benchmark_read_performance(
        &self,
        path: &Path,
        config: &Config,
        bench_config: &BenchmarkConfig,
    ) -> Result<ReadPerformance> {
        let reader = SSTableReader::open(path, config, self.platform.clone()).await?;
        let table_id = TableId::new("benchmark_table");

        // Sequential read test
        let seq_start = SystemTime::now();
        let _results = reader.scan(&table_id, None, None, Some(bench_config.entry_count)).await?;
        let sequential_duration = seq_start.elapsed().unwrap();

        // Random read test
        let rand_start = SystemTime::now();
        let mut cache_hits = 0;
        
        for i in 0..bench_config.random_read_count {
            let key_index = i % bench_config.entry_count;
            let key = RowKey::from(format!("key_{:08}", key_index));
            
            if let Some(_) = reader.get(&table_id, &key).await? {
                cache_hits += 1;
            }
        }
        
        let random_duration = rand_start.elapsed().unwrap();

        let sequential_ops_per_sec = bench_config.entry_count as f64 / sequential_duration.as_secs_f64();
        let random_ops_per_sec = bench_config.random_read_count as f64 / random_duration.as_secs_f64();
        let cache_hit_rate = cache_hits as f64 / bench_config.random_read_count as f64;

        Ok(ReadPerformance {
            sequential_duration,
            random_duration,
            sequential_ops_per_sec,
            random_ops_per_sec,
            cache_hit_rate,
        })
    }

    /// Analyze compression effectiveness
    async fn analyze_compression(&self, path: &Path, config: &BenchmarkConfig) -> Result<CompressionStats> {
        let file_size = std::fs::metadata(path)
            .map_err(|e| cqlite_core::error::Error::storage(format!("Failed to get file metadata: {}", e)))?
            .len();

        if !config.enable_compression {
            return Ok(CompressionStats {
                enabled: false,
                algorithm: "None".to_string(),
                original_size: file_size,
                compressed_size: file_size,
                ratio: 1.0,
                space_saved_mb: 0.0,
            });
        }

        // Estimate original size (rough calculation)
        let estimated_original = config.entry_count * (config.value_size + 50); // Value + key + metadata
        let ratio = file_size as f64 / estimated_original as f64;
        let space_saved = (estimated_original as i64 - file_size as i64).max(0) as f64 / 1024.0 / 1024.0;

        Ok(CompressionStats {
            enabled: true,
            algorithm: format!("{:?}", config.compression_algorithm),
            original_size: estimated_original as u64,
            compressed_size: file_size,
            ratio,
            space_saved_mb: space_saved,
        })
    }

    /// Analyze file structure and overhead
    async fn analyze_file_structure(&self, path: &Path) -> Result<FileStats> {
        let total_size = std::fs::metadata(path)
            .map_err(|e| cqlite_core::error::Error::storage(format!("Failed to get file metadata: {}", e)))?
            .len();

        // Estimates based on SSTable format
        let header_size = 32; // Fixed header size
        let footer_size = 16; // Fixed footer size
        let estimated_index_size = total_size / 20; // Rough estimate: 5% for index
        let estimated_bloom_size = total_size / 50; // Rough estimate: 2% for bloom filter
        let data_size = total_size - header_size - footer_size - estimated_index_size - estimated_bloom_size;
        
        let metadata_overhead = (total_size - data_size) as f64 / total_size as f64 * 100.0;

        Ok(FileStats {
            total_size_bytes: total_size,
            header_size,
            data_size,
            index_size: estimated_index_size,
            bloom_filter_size: estimated_bloom_size,
            metadata_overhead_percent: metadata_overhead,
        })
    }

    /// Estimate memory usage
    fn estimate_memory_usage(&self, config: &BenchmarkConfig) -> MemoryUsage {
        // Rough estimates based on typical SSTable implementations
        let entry_overhead = 50; // bytes per entry for index/structures
        let total_data_size = config.entry_count * (config.value_size + 20);
        
        let peak_writer_memory = (total_data_size / 10) as f64 / 1024.0 / 1024.0; // 10% for buffers
        let peak_reader_memory = (total_data_size / 20) as f64 / 1024.0 / 1024.0; // 5% for caches
        let index_memory = (config.entry_count * entry_overhead) as f64 / 1024.0 / 1024.0;
        let bloom_memory = if config.enable_bloom_filters {
            (config.entry_count * 2) as f64 / 1024.0 / 1024.0 // ~2 bytes per entry
        } else {
            0.0
        };

        MemoryUsage {
            peak_writer_memory_mb: peak_writer_memory,
            peak_reader_memory_mb: peak_reader_memory,
            index_memory_mb: index_memory,
            bloom_filter_memory_mb: bloom_memory,
        }
    }

    /// Run scalability test with increasing data sizes
    pub async fn run_scalability_test(&self) -> Result<Vec<BenchmarkResults>> {
        println!("📈 Running scalability test...");
        
        let sizes = vec![1_000, 5_000, 10_000, 50_000, 100_000];
        let mut results = Vec::new();

        for size in sizes {
            println!("Testing with {} entries...", size);
            
            let config = BenchmarkConfig {
                entry_count: size,
                ..Default::default()
            };
            
            let result = self.run_benchmark(config).await?;
            results.push(result);
        }

        Ok(results)
    }

    /// Compare different compression algorithms
    pub async fn compare_compression_algorithms(&self) -> Result<HashMap<String, BenchmarkResults>> {
        println!("🗜️ Comparing compression algorithms...");
        
        let algorithms = vec![
            ("None", CompressionAlgorithm::None),
            ("LZ4", CompressionAlgorithm::Lz4),
            ("Snappy", CompressionAlgorithm::Snappy),
            ("Deflate", CompressionAlgorithm::Deflate),
        ];
        
        let mut results = HashMap::new();
        
        for (name, algorithm) in algorithms {
            println!("Testing {} compression...", name);
            
            let config = BenchmarkConfig {
                enable_compression: algorithm != CompressionAlgorithm::None,
                compression_algorithm: algorithm,
                ..Default::default()
            };
            
            let result = self.run_benchmark(config).await?;
            results.insert(name.to_string(), result);
        }

        Ok(results)
    }

    /// Print benchmark results
    pub fn print_results(&self, results: &BenchmarkResults) {
        println!("\n📊 Benchmark Results");
        println!("==================");
        
        println!("\n✍️ Write Performance:");
        println!("  Duration: {:.2}s", results.write_performance.duration.as_secs_f64());
        println!("  Throughput: {:.0} entries/sec", results.write_performance.entries_per_second);
        println!("  Bandwidth: {:.2} MB/sec", results.write_performance.bytes_per_second / 1024.0 / 1024.0);
        println!("  Avg Latency: {:.2} μs", results.write_performance.average_latency_micros);
        
        println!("\n📖 Read Performance:");
        println!("  Sequential: {:.0} ops/sec", results.read_performance.sequential_ops_per_sec);
        println!("  Random: {:.0} ops/sec", results.read_performance.random_ops_per_sec);
        println!("  Cache Hit Rate: {:.1}%", results.read_performance.cache_hit_rate * 100.0);
        
        println!("\n🗜️ Compression:");
        if results.compression_stats.enabled {
            println!("  Algorithm: {}", results.compression_stats.algorithm);
            println!("  Ratio: {:.3}", results.compression_stats.ratio);
            println!("  Space Saved: {:.2} MB", results.compression_stats.space_saved_mb);
            println!("  Efficiency: {:.1}%", (1.0 - results.compression_stats.ratio) * 100.0);
        } else {
            println!("  Disabled");
        }
        
        println!("\n📁 File Structure:");
        println!("  Total Size: {:.2} MB", results.file_stats.total_size_bytes as f64 / 1024.0 / 1024.0);
        println!("  Data: {:.2} MB", results.file_stats.data_size as f64 / 1024.0 / 1024.0);
        println!("  Index: {:.2} MB", results.file_stats.index_size as f64 / 1024.0 / 1024.0);
        println!("  Bloom Filter: {:.2} MB", results.file_stats.bloom_filter_size as f64 / 1024.0 / 1024.0);
        println!("  Metadata Overhead: {:.1}%", results.file_stats.metadata_overhead_percent);
        
        println!("\n💾 Memory Usage:");
        println!("  Peak Writer: {:.2} MB", results.memory_usage.peak_writer_memory_mb);
        println!("  Peak Reader: {:.2} MB", results.memory_usage.peak_reader_memory_mb);
        println!("  Index: {:.2} MB", results.memory_usage.index_memory_mb);
        println!("  Bloom Filter: {:.2} MB", results.memory_usage.bloom_filter_memory_mb);
    }

    /// Print scalability test results
    pub fn print_scalability_results(&self, results: &[BenchmarkResults]) {
        println!("\n📈 Scalability Test Results");
        println!("===========================");
        println!("{:>10} {:>12} {:>12} {:>10} {:>10}", "Entries", "Write/sec", "Read/sec", "Size(MB)", "Latency(μs)");
        println!("{}", "-".repeat(60));
        
        for (i, result) in results.iter().enumerate() {
            let entry_count = match i {
                0 => 1_000,
                1 => 5_000,
                2 => 10_000,
                3 => 50_000,
                4 => 100_000,
                _ => 0,
            };
            
            println!("{:>10} {:>12.0} {:>12.0} {:>10.2} {:>10.2}",
                entry_count,
                result.write_performance.entries_per_second,
                result.read_performance.random_ops_per_sec,
                result.file_stats.total_size_bytes as f64 / 1024.0 / 1024.0,
                result.write_performance.average_latency_micros
            );
        }
    }

    /// Print compression comparison results
    pub fn print_compression_comparison(&self, results: &HashMap<String, BenchmarkResults>) {
        println!("\n🗜️ Compression Algorithm Comparison");
        println!("===================================");
        println!("{:>10} {:>12} {:>10} {:>12} {:>10}", "Algorithm", "Write/sec", "Ratio", "Size(MB)", "Saved(MB)");
        println!("{}", "-".repeat(65));
        
        let order = vec!["None", "LZ4", "Snappy", "Deflate"];
        for algorithm in order {
            if let Some(result) = results.get(algorithm) {
                println!("{:>10} {:>12.0} {:>10.3} {:>12.2} {:>10.2}",
                    algorithm,
                    result.write_performance.entries_per_second,
                    result.compression_stats.ratio,
                    result.file_stats.total_size_bytes as f64 / 1024.0 / 1024.0,
                    result.compression_stats.space_saved_mb
                );
            }
        }
    }
}

/// Run comprehensive SSTable benchmarks
pub async fn run_comprehensive_benchmark() -> Result<()> {
    let benchmark = SSTableBenchmark::new().await?;
    
    // Run standard benchmark
    println!("🚀 Running standard benchmark...");
    let standard_result = benchmark.run_benchmark(BenchmarkConfig::default()).await?;
    benchmark.print_results(&standard_result);
    
    // Run scalability test
    let scalability_results = benchmark.run_scalability_test().await?;
    benchmark.print_scalability_results(&scalability_results);
    
    // Compare compression algorithms
    let compression_results = benchmark.compare_compression_algorithms().await?;
    benchmark.print_compression_comparison(&compression_results);
    
    println!("\n✅ Benchmark suite completed!");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_benchmark_creation() {
        let benchmark = SSTableBenchmark::new().await.unwrap();
        assert!(benchmark.test_dir.path().exists());
    }

    #[tokio::test]
    async fn test_small_benchmark() {
        let benchmark = SSTableBenchmark::new().await.unwrap();
        
        let config = BenchmarkConfig {
            entry_count: 100,
            value_size: 64,
            random_read_count: 50,
            ..Default::default()
        };
        
        let result = benchmark.run_benchmark(config).await.unwrap();
        assert!(result.write_performance.entries_per_second > 0.0);
        assert!(result.read_performance.random_ops_per_sec > 0.0);
    }
}