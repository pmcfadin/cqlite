//! Streaming performance benchmark executable
//!
//! This binary tests the streaming SSTable reader performance optimizations
//! using real Cassandra 5 SSTable files from the test environment.

#[cfg(feature = "benchmarks")]
use std::env;
#[cfg(feature = "benchmarks")]
use std::path::PathBuf;
#[cfg(feature = "benchmarks")]
use std::process;

#[cfg(feature = "benchmarks")]
use cqlite_core::storage::sstable::performance_benchmarks::PerformanceBenchmarks;

#[cfg(feature = "benchmarks")]
#[tokio::main]
async fn main() {
    // Parse command line arguments
    let args: Vec<String> = env::args().collect();
    let test_data_dir = if args.len() > 1 {
        PathBuf::from(&args[1])
    } else {
        // Default to test-env directory
        let mut current_dir = env::current_dir().expect("Failed to get current directory");
        current_dir.push("test-env");
        current_dir.push("cassandra5");
        current_dir
    };

    if !test_data_dir.exists() {
        eprintln!(
            "❌ Test data directory not found: {}",
            test_data_dir.display()
        );
        eprintln!("💡 Usage: {} [test-data-directory]", args[0]);
        eprintln!(
            "💡 Expected structure: test-data-directory/sstables/table-uuid/nb-1-big-Data.db"
        );
        process::exit(1);
    }

    println!("🚀 CQLite Streaming Performance Benchmark");
    println!("═══════════════════════════════════════════");
    println!("📁 Test data directory: {}", test_data_dir.display());

    // Create benchmark suite
    let benchmarks = match PerformanceBenchmarks::new(&test_data_dir).await {
        Ok(benchmarks) => benchmarks,
        Err(e) => {
            eprintln!("❌ Failed to initialize benchmarks: {}", e);
            process::exit(1);
        }
    };

    // Run comprehensive benchmarks
    match benchmarks.run_comprehensive_benchmarks().await {
        Ok(results) => {
            if results.is_empty() {
                println!("⚠️  No test files found. Make sure SSTable files exist in:");
                println!(
                    "   📂 {}/sstables/*/nb-1-big-Data.db",
                    test_data_dir.display()
                );
                println!("\n💡 To generate test data:");
                println!("   cd test-env/cassandra5");
                println!("   ./manage.sh all");
                println!("   ./manage.sh extract-sstables");
            } else {
                println!("\n✅ Benchmark completed successfully!");
                println!("📊 Processed {} test results", results.len());

                // Export results to JSON for further analysis
                export_results_to_json(&results).await;
            }
        }
        Err(e) => {
            eprintln!("❌ Benchmark failed: {}", e);
            process::exit(1);
        }
    }
}

#[cfg(feature = "benchmarks")]
async fn export_results_to_json(
    results: &[cqlite_core::storage::sstable::performance_benchmarks::BenchmarkResults],
) {
    use serde_json;
    use tokio::fs;

    // Create a simple JSON export of results
    let mut json_data = serde_json::Map::new();

    for (i, result) in results.iter().enumerate() {
        let mut result_data = serde_json::Map::new();
        result_data.insert(
            "reader_type".to_string(),
            serde_json::Value::String(result.reader_type.clone()),
        );
        result_data.insert(
            "file_size_mb".to_string(),
            serde_json::Value::Number(
                serde_json::Number::from_f64(result.file_size as f64 / 1024.0 / 1024.0).unwrap(),
            ),
        );
        result_data.insert(
            "duration_ms".to_string(),
            serde_json::Value::Number(
                serde_json::Number::from_f64(result.total_duration.as_millis() as f64).unwrap(),
            ),
        );
        result_data.insert(
            "ops_per_second".to_string(),
            serde_json::Value::Number(serde_json::Number::from_f64(result.ops_per_second).unwrap()),
        );
        result_data.insert(
            "peak_memory_mb".to_string(),
            serde_json::Value::Number(
                serde_json::Number::from_f64(result.memory_stats.peak_memory_mb).unwrap(),
            ),
        );
        result_data.insert(
            "efficiency_ratio".to_string(),
            serde_json::Value::Number(
                serde_json::Number::from_f64(result.memory_stats.efficiency_ratio).unwrap(),
            ),
        );
        result_data.insert(
            "cache_hit_rate".to_string(),
            serde_json::Value::Number(
                serde_json::Number::from_f64(result.io_stats.cache_hit_rate).unwrap(),
            ),
        );
        result_data.insert(
            "error_count".to_string(),
            serde_json::Value::Number(serde_json::Number::from(result.error_count)),
        );

        json_data.insert(
            format!("result_{}", i),
            serde_json::Value::Object(result_data),
        );
    }

    let json_str = serde_json::to_string_pretty(&json_data).unwrap_or_else(|_| "{}".to_string());

    if let Ok(()) = fs::write("benchmark_results.json", &json_str).await {
        println!("📊 Results exported to: benchmark_results.json");
    } else {
        println!("⚠️  Failed to export results to JSON");
    }
}

#[cfg(not(feature = "benchmarks"))]
fn main() {
    eprintln!("This binary requires the 'benchmarks' feature to be enabled.");
    eprintln!("Run with: cargo run --features benchmarks --bin benchmark_streaming");
    std::process::exit(1);
}
