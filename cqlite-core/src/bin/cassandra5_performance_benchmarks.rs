//! Cassandra 5+ Performance Benchmarking Runner
//!
//! This binary runs comprehensive performance benchmarks for Cassandra 5+ SSTable
//! format, validating all PRD requirements including:
//! - Memory usage ≤128MB for large files
//! - Parse speed ≥100 MB/s
//! - Throughput ≥100K ops/sec
//! - Zero-copy deserialization efficiency

use std::env;
use std::path::PathBuf;

use cqlite_core::{
    benchmarks::cassandra5::{Cassandra5PerformanceSuite, PRDTargets},
    performance_monitor::PerformanceMonitor,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Cassandra 5+ Performance Benchmarking Suite");
    println!("═══════════════════════════════════════════════");

    // Parse command line arguments
    let args: Vec<String> = env::args().collect();
    let test_data_dir = if args.len() > 1 {
        PathBuf::from(&args[1])
    } else {
        // Default to current directory
        env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
    };

    println!("📁 Test data directory: {}", test_data_dir.display());

    // Initialize performance monitoring
    let performance_monitor = PerformanceMonitor::default();
    println!("📊 Performance monitoring initialized with PRD baselines");

    // Create benchmarking suite
    let mut benchmark_suite = Cassandra5PerformanceSuite::new().await?;

    // Define PRD targets
    let prd_targets = PRDTargets {
        parse_speed_mb_per_sec: 100.0,
        memory_limit_mb: 128.0,
        throughput_ops_per_sec: 100_000.0,
        max_file_size_gb: 1.0,
    };

    println!("\n🎯 PRD Performance Targets:");
    println!(
        "   • Parse Speed: ≥ {} MB/s",
        prd_targets.parse_speed_mb_per_sec
    );
    println!("   • Memory Limit: ≤ {} MB", prd_targets.memory_limit_mb);
    println!(
        "   • Throughput: ≥ {} ops/sec",
        prd_targets.throughput_ops_per_sec
    );
    println!("   • Max File Size: {} GB", prd_targets.max_file_size_gb);

    // Run comprehensive benchmarks
    println!("\n🏁 Starting comprehensive benchmarks...");
    let start_time = std::time::Instant::now();

    let results = benchmark_suite
        .run_comprehensive_benchmarks(&test_data_dir)
        .await?;

    let total_duration = start_time.elapsed();

    // Record benchmark results in performance monitor
    for result in &results {
        performance_monitor.record_measurement(
            &format!("benchmark_{}", result.benchmark_name),
            result.throughput_mb_per_sec,
            "MB/s",
        );

        performance_monitor.record_measurement(
            &format!("memory_{}", result.benchmark_name),
            result.memory_usage_mb,
            "MB",
        );

        if result.operations_per_second > 0.0 {
            performance_monitor.record_measurement(
                &format!("ops_{}", result.benchmark_name),
                result.operations_per_second,
                "ops/sec",
            );
        }
    }

    // Generate comprehensive report
    println!("\n📊 FINAL BENCHMARK REPORT");
    println!("═════════════════════════");

    let passed_benchmarks = results.iter().filter(|r| r.meets_prd_target).count();
    let total_benchmarks = results.len();
    let overall_pass_rate = if total_benchmarks > 0 {
        (passed_benchmarks as f64 / total_benchmarks as f64) * 100.0
    } else {
        0.0
    };

    println!("📈 Overall Results:");
    println!("   • Benchmarks Run: {}", total_benchmarks);
    println!(
        "   • Benchmarks Passed: {} ({:.1}%)",
        passed_benchmarks, overall_pass_rate
    );
    println!(
        "   • Total Duration: {:.2} seconds",
        total_duration.as_secs_f64()
    );

    // Categorize results
    categorize_and_display_results(&results);

    // Performance monitoring report
    println!("\n📊 Performance Monitoring Report:");
    println!("{}", performance_monitor.generate_performance_report());

    // Export results for further analysis
    let export_results = benchmark_suite.export_results();
    println!(
        "\n💾 Benchmark data exported: {} result sets available",
        export_results.len()
    );

    // Summary and recommendations
    println!("\n💡 SUMMARY & RECOMMENDATIONS");
    println!("══════════════════════════════");

    if overall_pass_rate >= 80.0 {
        println!(
            "✅ EXCELLENT: {:.1}% of benchmarks passed PRD targets",
            overall_pass_rate
        );
        println!("   The implementation meets or exceeds Cassandra 5+ performance requirements.");
    } else if overall_pass_rate >= 60.0 {
        println!(
            "⚠️  GOOD: {:.1}% of benchmarks passed, some optimizations needed",
            overall_pass_rate
        );
        generate_optimization_recommendations(&results);
    } else {
        println!(
            "❌ ATTENTION NEEDED: Only {:.1}% of benchmarks passed",
            overall_pass_rate
        );
        println!("   Significant performance optimizations required to meet PRD targets.");
        generate_optimization_recommendations(&results);
    }

    // Exit code based on results
    let exit_code = if overall_pass_rate >= 80.0 { 0 } else { 1 };

    println!(
        "\n🏁 Benchmarking completed in {:.2} seconds",
        total_duration.as_secs_f64()
    );
    std::process::exit(exit_code);
}

/// Categorize and display benchmark results by type
fn categorize_and_display_results(
    results: &[cqlite_core::benchmarks::cassandra5::BenchmarkResult],
) {
    let categories = vec![
        ("Memory", "💾"),
        ("Compression", "🗜️"),
        ("ZeroCopy", "⚡"),
        ("Throughput", "🏃"),
        ("CQLite", "🚀"),
        ("Comparison", "🔄"),
    ];

    for (category, emoji) in categories {
        let category_results: Vec<_> = results
            .iter()
            .filter(|r| r.benchmark_name.contains(category))
            .collect();

        if !category_results.is_empty() {
            let passed = category_results
                .iter()
                .filter(|r| r.meets_prd_target)
                .count();
            let total = category_results.len();
            let pass_rate = (passed as f64 / total as f64) * 100.0;

            println!(
                "\n{} {} Benchmarks: {}/{} passed ({:.1}%)",
                emoji, category, passed, total, pass_rate
            );

            // Show best and worst performers
            if let Some(best) = category_results.iter().max_by(|a, b| {
                a.throughput_mb_per_sec
                    .partial_cmp(&b.throughput_mb_per_sec)
                    .unwrap()
            }) {
                println!(
                    "   🏆 Best: {} ({:.2} MB/s, {:.1} MB memory)",
                    best.benchmark_name, best.throughput_mb_per_sec, best.memory_usage_mb
                );
            }

            // Show failures if any
            let failures: Vec<_> = category_results
                .iter()
                .filter(|r| !r.meets_prd_target)
                .collect();
            if !failures.is_empty() {
                println!("   ❌ Failed ({}):", failures.len());
                for failure in failures.iter().take(3) {
                    // Show up to 3 failures
                    println!(
                        "      • {}: {}",
                        failure.benchmark_name, failure.target_comparison
                    );
                }
                if failures.len() > 3 {
                    println!("      • ... and {} more", failures.len() - 3);
                }
            }
        }
    }
}

/// Generate optimization recommendations based on failed benchmarks
fn generate_optimization_recommendations(
    results: &[cqlite_core::benchmarks::cassandra5::BenchmarkResult],
) {
    let failures: Vec<_> = results.iter().filter(|r| !r.meets_prd_target).collect();

    println!("\n🔧 OPTIMIZATION RECOMMENDATIONS:");

    // Memory-related failures
    let memory_failures: Vec<_> = failures
        .iter()
        .filter(|r| r.memory_usage_mb > 128.0)
        .collect();

    if !memory_failures.is_empty() {
        println!("   💾 Memory Usage Optimizations:");
        println!("      • Implement streaming readers for large files");
        println!("      • Optimize buffer pool management");
        println!("      • Consider memory-mapped file access");
        println!("      • Implement progressive loading for large SSTables");
    }

    // Throughput-related failures
    let throughput_failures: Vec<_> = failures
        .iter()
        .filter(|r| r.throughput_mb_per_sec < 100.0)
        .collect();

    if !throughput_failures.is_empty() {
        println!("   🏃 Throughput Optimizations:");
        println!("      • Profile I/O bottlenecks and optimize read patterns");
        println!("      • Implement parallel processing for large files");
        println!("      • Optimize compression/decompression algorithms");
        println!("      • Consider SIMD optimizations for data processing");
    }

    // Compression-related failures
    let compression_failures: Vec<_> = failures
        .iter()
        .filter(|r| r.benchmark_name.contains("Compression"))
        .collect();

    if !compression_failures.is_empty() {
        println!("   🗜️ Compression Optimizations:");
        println!("      • Benchmark and select optimal compression algorithms");
        println!("      • Implement adaptive compression based on data characteristics");
        println!("      • Optimize decompression buffer management");
        println!("      • Consider hardware-accelerated compression");
    }

    // Zero-copy related failures
    let zerocopy_failures: Vec<_> = failures
        .iter()
        .filter(|r| r.benchmark_name.contains("ZeroCopy"))
        .collect();

    if !zerocopy_failures.is_empty() {
        println!("   ⚡ Zero-Copy Optimizations:");
        println!("      • Minimize data copying in deserialization paths");
        println!("      • Implement view-based data structures");
        println!("      • Optimize memory allocation patterns");
        println!("      • Use borrowed data structures where possible");
    }

    if failures.is_empty() {
        println!("   🎉 All benchmarks passed! No optimizations needed.");
    }
}

/// Display usage information
#[allow(dead_code)]
fn print_usage() {
    println!("Usage: cassandra5_performance_benchmarks [TEST_DATA_DIR]");
    println!();
    println!("Arguments:");
    println!("  TEST_DATA_DIR    Directory containing test SSTable files (optional)");
    println!();
    println!("Environment Variables:");
    println!("  RUST_LOG         Set logging level (debug, info, warn, error)");
    println!();
    println!("Examples:");
    println!("  cassandra5_performance_benchmarks");
    println!("  cassandra5_performance_benchmarks /path/to/test/data");
    println!("  RUST_LOG=info cassandra5_performance_benchmarks ./test-data");
}
