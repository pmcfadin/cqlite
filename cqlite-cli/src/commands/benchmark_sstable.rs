//! SSTable read-performance benchmark command handler.
//!
//! Extracted verbatim from `commands/mod.rs` during the module split (issue #1126).

#![allow(dead_code)]
// Allow deprecated BulletproofReader usage (Issue #190 - experimental reader)
#![allow(deprecated)]

use super::schema_load::load_schema_file;
use super::support::{resolve_sstable_path, RealDataParser};
use anyhow::{Context, Result};
use cqlite_core::storage::sstable::bulletproof_reader::BulletproofReader;
use std::path::Path;

/// Benchmark SSTable read performance with various operations
pub async fn benchmark_sstable(
    sstable_path: &Path,
    schema_path: Option<&Path>,
    iterations: u32,
    operations: &str,
    report_path: Option<&Path>,
    memory_profile: bool,
) -> Result<()> {
    println!("🏁 SSTable Performance Benchmark");
    println!("📂 SSTable: {}", sstable_path.display());

    if let Some(schema) = schema_path {
        println!("📋 Schema: {}", schema.display());
    }

    println!("🔄 Iterations: {iterations}");
    println!("🎯 Operations: {operations}");

    if memory_profile {
        println!("📊 Memory profiling enabled");
    }

    if let Some(report) = report_path {
        println!("📋 Report will be saved to: {}", report.display());
    }

    // Smart path resolution
    let actual_sstable_path = resolve_sstable_path(sstable_path)?;
    println!("📄 Data file: {}", actual_sstable_path.display());

    let mut benchmark_results = Vec::new();

    // Parse operations list
    let ops: Vec<&str> = if operations == "all" {
        vec!["read", "scan", "query"]
    } else {
        operations.split(',').map(|s| s.trim()).collect()
    };

    println!("\n🚀 Starting benchmarks...");

    for op in &ops {
        println!("\n📊 Benchmarking operation: {op}");

        let mut times = Vec::new();
        let mut memory_usage = Vec::new();

        for i in 1..=iterations {
            print!("   Iteration {i}/{iterations}: ");

            let start_time = std::time::Instant::now();
            let initial_memory = if memory_profile {
                // TODO: Implement memory measurement
                0u64
            } else {
                0u64
            };

            // Perform the operation
            let result = match *op {
                "read" => benchmark_read_operation(&actual_sstable_path).await,
                "scan" => benchmark_scan_operation(&actual_sstable_path).await,
                "query" => benchmark_query_operation(&actual_sstable_path, schema_path).await,
                _ => {
                    println!("❌ Unknown operation: {op}");
                    continue;
                }
            };

            let elapsed = start_time.elapsed();
            let final_memory = if memory_profile {
                // TODO: Implement memory measurement
                0u64
            } else {
                0u64
            };

            match result {
                Ok(entries_processed) => {
                    println!(
                        "✅ {}ms ({} entries)",
                        elapsed.as_millis(),
                        entries_processed
                    );
                    times.push(elapsed.as_millis() as f64);
                    if memory_profile {
                        memory_usage.push(final_memory.saturating_sub(initial_memory));
                    }
                }
                Err(e) => {
                    println!("❌ Failed: {e}");
                }
            }
        }

        // Calculate statistics
        if !times.is_empty() {
            times.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let min_time = times[0];
            let max_time = times[times.len() - 1];
            let avg_time = times.iter().sum::<f64>() / times.len() as f64;
            let median_time = times[times.len() / 2];

            println!("\n   📊 {op} Statistics:");
            println!("      Min time: {min_time:.2}ms");
            println!("      Max time: {max_time:.2}ms");
            println!("      Avg time: {avg_time:.2}ms");
            println!("      Median time: {median_time:.2}ms");

            benchmark_results.push(format!(
                "{op}: min={min_time:.2}ms, max={max_time:.2}ms, avg={avg_time:.2}ms, median={median_time:.2}ms"
            ));

            if memory_profile && !memory_usage.is_empty() {
                let avg_memory = memory_usage.iter().sum::<u64>() / memory_usage.len() as u64;
                println!("      Avg memory: {avg_memory} bytes");
                benchmark_results.push(format!("{op}: avg_memory={avg_memory}bytes"));
            }
        }
    }

    // Generate report
    if let Some(report_path) = report_path {
        let mut report_content = format!(
            "# SSTable Benchmark Report\n\n\
            **File:** {}\n\
            **Benchmark Time:** {}\n\
            **Iterations:** {}\n\
            **Operations:** {}\n\
            **Memory Profiling:** {}\n\n\
            ## Results\n",
            sstable_path.display(),
            chrono::Utc::now().to_rfc3339(),
            iterations,
            operations,
            memory_profile
        );

        for result in &benchmark_results {
            report_content.push_str(&format!("- {result}\n"));
        }

        std::fs::write(report_path, report_content)
            .with_context(|| format!("Failed to write report to {}", report_path.display()))?;

        println!("\n📋 Benchmark report saved to: {}", report_path.display());
    }

    println!("\n🏆 Benchmark completed!");

    Ok(())
}

/// Benchmark read operation (open and basic info)
async fn benchmark_read_operation(sstable_path: &Path) -> Result<usize> {
    let reader = BulletproofReader::open(sstable_path).with_context(|| "Failed to open SSTable")?;

    let _info = reader.info();
    Ok(1) // Return 1 as we processed the file info
}

/// Benchmark scan operation (iterate through all entries)
async fn benchmark_scan_operation(sstable_path: &Path) -> Result<usize> {
    let mut reader =
        BulletproofReader::open(sstable_path).with_context(|| "Failed to open SSTable")?;

    match reader.parse_sstable_data() {
        Ok(entries) => Ok(entries.len()),
        Err(_) => {
            // Fallback to basic read
            let _info = reader.info();
            Ok(0)
        }
    }
}

/// Benchmark query operation (with schema parsing if available)
async fn benchmark_query_operation(
    sstable_path: &Path,
    schema_path: Option<&Path>,
) -> Result<usize> {
    let mut reader =
        BulletproofReader::open(sstable_path).with_context(|| "Failed to open SSTable")?;

    match reader.parse_sstable_data() {
        Ok(entries) => {
            if let Some(schema_path) = schema_path {
                match load_schema_file(schema_path, true, None) {
                    Ok(schema) => {
                        let parser = RealDataParser::new(schema);
                        let mut parsed_count = 0;

                        for entry in &entries {
                            let key = entry.key.clone();
                            // Issue #1334: parse_entry consumes the `ScanRow` carrier;
                            // this mock path has no decoded row, so wrap as a marker.
                            let value = cqlite_core::types::ScanRow::Marker(
                                cqlite_core::Value::Text(format!("{:?}", entry.key)),
                            );

                            if parser.parse_entry(&key, &value).is_ok() {
                                parsed_count += 1;
                            }
                        }

                        Ok(parsed_count)
                    }
                    Err(_) => Ok(entries.len()), // Fallback to just entry count
                }
            } else {
                Ok(entries.len())
            }
        }
        Err(_) => Ok(0),
    }
}
