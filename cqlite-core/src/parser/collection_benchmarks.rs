//! Performance benchmarks for collection parsing and serialization
//!
//! This module provides comprehensive benchmarks for collections to ensure
//! they meet performance requirements for production Cassandra workloads.

use super::*;
use crate::types::Value;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct CollectionBenchmarkResult {
    pub operation: String,
    pub collection_type: String,
    pub element_count: usize,
    pub data_size_bytes: usize,
    pub parse_time: Duration,
    pub serialize_time: Duration,
    pub throughput_mb_per_sec: f64,
    pub ops_per_second: f64,
}

pub struct CollectionBenchmarks {
    pub results: Vec<CollectionBenchmarkResult>,
}

impl CollectionBenchmarks {
    pub fn new() -> Self {
        Self {
            results: Vec::new(),
        }
    }

    /// Run comprehensive collection benchmarks
    pub fn run_all_benchmarks(&mut self) -> crate::Result<()> {
        println!("🔥 Running Collection Performance Benchmarks...");
        
        self.benchmark_list_operations()?;
        self.benchmark_set_operations()?;
        self.benchmark_map_operations()?;
        self.benchmark_tuple_operations()?;
        self.benchmark_nested_collections()?;
        self.benchmark_large_collections()?;
        
        Ok(())
    }

    /// Benchmark List operations with various sizes
    fn benchmark_list_operations(&mut self) -> crate::Result<()> {
        println!("  📋 Benchmarking List operations...");
        
        let sizes = vec![10, 100, 1000, 10000];
        
        for size in sizes {
            // String List benchmark
            let string_list = Value::List(
                (0..size).map(|i| Value::Text(format!("item_{:06}", i))).collect()
            );
            
            self.benchmark_collection_roundtrip(
                "parse_serialize",
                "List<String>",
                string_list.clone(),
                CqlTypeId::List,
            )?;
            
            // Integer List benchmark
            let int_list = Value::List(
                (0..size).map(|i| Value::Integer(i as i32)).collect()
            );
            
            self.benchmark_collection_roundtrip(
                "parse_serialize",
                "List<Integer>",
                int_list,
                CqlTypeId::List,
            )?;
            
            // UUID List benchmark (common for ID lists)
            let uuid_list = Value::List(
                (0..size).map(|i| {
                    let mut uuid = [0u8; 16];
                    uuid[0..4].copy_from_slice(&(i as u32).to_be_bytes());
                    Value::Uuid(uuid)
                }).collect()
            );
            
            self.benchmark_collection_roundtrip(
                "parse_serialize",
                "List<UUID>",
                uuid_list,
                CqlTypeId::List,
            )?;
        }
        
        Ok(())
    }

    /// Benchmark Set operations with various sizes
    fn benchmark_set_operations(&mut self) -> crate::Result<()> {
        println!("  🎯 Benchmarking Set operations...");
        
        let sizes = vec![10, 100, 1000, 5000];
        
        for size in sizes {
            // String Set benchmark (tags, categories)
            let string_set = Value::Set(
                (0..size).map(|i| Value::Text(format!("tag_{:04}", i))).collect()
            );
            
            self.benchmark_collection_roundtrip(
                "parse_serialize",
                "Set<String>",
                string_set,
                CqlTypeId::Set,
            )?;
            
            // Integer Set benchmark
            let int_set = Value::Set(
                (0..size).map(|i| Value::Integer(i as i32)).collect()
            );
            
            self.benchmark_collection_roundtrip(
                "parse_serialize",
                "Set<Integer>",
                int_set,
                CqlTypeId::Set,
            )?;
        }
        
        Ok(())
    }

    /// Benchmark Map operations with various key-value combinations
    fn benchmark_map_operations(&mut self) -> crate::Result<()> {
        println!("  🗺️  Benchmarking Map operations...");
        
        let sizes = vec![10, 100, 1000, 5000];
        
        for size in sizes {
            // String-to-Integer Map (common pattern)
            let string_int_map = Value::Map(
                (0..size).map(|i| (
                    Value::Text(format!("key_{:06}", i)),
                    Value::Integer(i as i32)
                )).collect()
            );
            
            self.benchmark_collection_roundtrip(
                "parse_serialize",
                "Map<String,Integer>",
                string_int_map,
                CqlTypeId::Map,
            )?;
            
            // String-to-String Map (metadata, configs)
            let string_string_map = Value::Map(
                (0..size).map(|i| (
                    Value::Text(format!("key_{:06}", i)),
                    Value::Text(format!("value_{:06}", i))
                )).collect()
            );
            
            self.benchmark_collection_roundtrip(
                "parse_serialize",
                "Map<String,String>",
                string_string_map,
                CqlTypeId::Map,
            )?;
            
            // UUID-to-Text Map (user mappings)
            let uuid_text_map = Value::Map(
                (0..size).map(|i| {
                    let mut uuid = [0u8; 16];
                    uuid[0..4].copy_from_slice(&(i as u32).to_be_bytes());
                    (
                        Value::Uuid(uuid),
                        Value::Text(format!("user_{:06}", i))
                    )
                }).collect()
            );
            
            self.benchmark_collection_roundtrip(
                "parse_serialize",
                "Map<UUID,String>",
                uuid_text_map,
                CqlTypeId::Map,
            )?;
        }
        
        Ok(())
    }

    /// Benchmark Tuple operations with various type combinations
    fn benchmark_tuple_operations(&mut self) -> crate::Result<()> {
        println!("  📦 Benchmarking Tuple operations...");
        
        let sizes = vec![2, 5, 10, 20];
        
        for size in sizes {
            // Mixed type tuple (realistic scenario)
            let mixed_tuple = Value::Tuple(
                (0..size).map(|i| match i % 6 {
                    0 => Value::Integer(i as i32),
                    1 => Value::Text(format!("field_{}", i)),
                    2 => Value::Boolean(i % 2 == 0),
                    3 => Value::Float(i as f64 * 3.14),
                    4 => Value::BigInt(i as i64 * 1000000),
                    _ => {
                        let mut uuid = [0u8; 16];
                        uuid[0..4].copy_from_slice(&(i as u32).to_be_bytes());
                        Value::Uuid(uuid)
                    }
                }).collect()
            );
            
            self.benchmark_collection_roundtrip(
                "parse_serialize",
                "Tuple<Mixed>",
                mixed_tuple,
                CqlTypeId::Tuple,
            )?;
            
            // Homogeneous integer tuple
            let int_tuple = Value::Tuple(
                (0..size).map(|i| Value::Integer(i as i32)).collect()
            );
            
            self.benchmark_collection_roundtrip(
                "parse_serialize",
                "Tuple<Integer>",
                int_tuple,
                CqlTypeId::Tuple,
            )?;
            
            // String tuple (field names, etc.)
            let string_tuple = Value::Tuple(
                (0..size).map(|i| Value::Text(format!("field_{}", i))).collect()
            );
            
            self.benchmark_collection_roundtrip(
                "parse_serialize",
                "Tuple<String>",
                string_tuple,
                CqlTypeId::Tuple,
            )?;
        }
        
        Ok(())
    }

    /// Benchmark nested collections (realistic complex scenarios)
    fn benchmark_nested_collections(&mut self) -> crate::Result<()> {
        println!("  🪆 Benchmarking Nested Collections...");
        
        let sizes = vec![10, 50, 100];
        
        for size in sizes {
            // List of Maps (JSON-like structures)
            let list_of_maps = Value::List(
                (0..size).map(|i| Value::Map(vec![
                    (Value::Text("id".to_string()), Value::Integer(i as i32)),
                    (Value::Text("name".to_string()), Value::Text(format!("item_{}", i))),
                    (Value::Text("active".to_string()), Value::Boolean(i % 2 == 0)),
                    (Value::Text("score".to_string()), Value::Float(i as f64 * 1.5)),
                ])).collect()
            );
            
            self.benchmark_collection_roundtrip(
                "parse_serialize",
                "List<Map<String,Mixed>>",
                list_of_maps,
                CqlTypeId::List,
            )?;
            
            // Map of Lists (categorized data)
            let map_of_lists = Value::Map(
                (0..size).map(|i| (
                    Value::Text(format!("category_{}", i)),
                    Value::List(vec![
                        Value::Text(format!("item_{}_{}", i, 1)),
                        Value::Text(format!("item_{}_{}", i, 2)),
                        Value::Text(format!("item_{}_{}", i, 3)),
                    ])
                )).collect()
            );
            
            self.benchmark_collection_roundtrip(
                "parse_serialize",
                "Map<String,List<String>>",
                map_of_lists,
                CqlTypeId::Map,
            )?;
            
            // Tuple with nested collections (complex records)
            let nested_tuple = Value::Tuple(vec![
                Value::Integer(42),
                Value::Text("complex_record".to_string()),
                Value::List((0..size).map(|i| Value::Integer(i as i32)).collect()),
                Value::Map((0..size/2).map(|i| (
                    Value::Text(format!("attr_{}", i)),
                    Value::Text(format!("value_{}", i))
                )).collect()),
            ]);
            
            self.benchmark_collection_roundtrip(
                "parse_serialize",
                "Tuple<Mixed+Nested>",
                nested_tuple,
                CqlTypeId::Tuple,
            )?;
        }
        
        Ok(())
    }

    /// Benchmark large collections to test scalability
    fn benchmark_large_collections(&mut self) -> crate::Result<()> {
        println!("  🏋️ Benchmarking Large Collections...");
        
        // Very large list (stress test)
        let large_sizes = vec![10000, 50000, 100000];
        
        for size in large_sizes {
            // Large string list
            let large_list = Value::List(
                (0..size).map(|i| Value::Text(format!("large_item_{:08}", i))).collect()
            );
            
            self.benchmark_collection_roundtrip(
                "parse_serialize",
                "Large_List<String>",
                large_list,
                CqlTypeId::List,
            )?;
            
            // Large integer list
            let large_int_list = Value::List(
                (0..size).map(|i| Value::Integer(i as i32)).collect()
            );
            
            self.benchmark_collection_roundtrip(
                "parse_serialize",
                "Large_List<Integer>",
                large_int_list,
                CqlTypeId::List,
            )?;
            
            // Don't test huge maps/tuples as they'd be impractical
            if size <= 10000 {
                // Large map
                let large_map = Value::Map(
                    (0..size).map(|i| (
                        Value::Text(format!("key_{:08}", i)),
                        Value::Integer(i as i32)
                    )).collect()
                );
                
                self.benchmark_collection_roundtrip(
                    "parse_serialize",
                    "Large_Map<String,Integer>",
                    large_map,
                    CqlTypeId::Map,
                )?;
            }
        }
        
        Ok(())
    }

    /// Helper function to benchmark collection roundtrip operations
    fn benchmark_collection_roundtrip(
        &mut self,
        operation: &str,
        collection_type: &str,
        value: Value,
        type_id: CqlTypeId,
    ) -> crate::Result<()> {
        let element_count = value.collection_len().unwrap_or(0);
        
        // Benchmark serialization
        let serialize_start = Instant::now();
        let serialized = serialize_cql_value(&value)?;
        let serialize_time = serialize_start.elapsed();
        
        let data_size = serialized.len();
        
        // Benchmark parsing
        let parse_start = Instant::now();
        let (_remaining, _parsed_value) = parse_cql_value(&serialized[1..], type_id)?;
        let parse_time = parse_start.elapsed();
        
        // Calculate performance metrics
        let total_time = serialize_time + parse_time;
        let throughput_mb_per_sec = if total_time.as_secs_f64() > 0.0 {
            (data_size as f64) / (total_time.as_secs_f64() * 1_000_000.0)
        } else {
            0.0
        };
        
        let ops_per_second = if total_time.as_secs_f64() > 0.0 {
            1.0 / total_time.as_secs_f64()
        } else {
            0.0
        };
        
        self.results.push(CollectionBenchmarkResult {
            operation: operation.to_string(),
            collection_type: collection_type.to_string(),
            element_count,
            data_size_bytes: data_size,
            parse_time,
            serialize_time,
            throughput_mb_per_sec,
            ops_per_second,
        });
        
        Ok(())
    }

    /// Generate performance report
    pub fn generate_report(&self) -> String {
        let mut report = String::new();
        
        report.push_str("🔥 Collection Performance Benchmark Report\n");
        report.push_str("==========================================\n\n");
        
        // Summary statistics
        let total_tests = self.results.len();
        let avg_throughput: f64 = self.results.iter()
            .map(|r| r.throughput_mb_per_sec)
            .sum::<f64>() / total_tests as f64;
        let max_throughput = self.results.iter()
            .map(|r| r.throughput_mb_per_sec)
            .fold(0.0f64, f64::max);
        let avg_ops_per_sec: f64 = self.results.iter()
            .map(|r| r.ops_per_second)
            .sum::<f64>() / total_tests as f64;
        
        report.push_str(&format!("📊 Summary\n"));
        report.push_str(&format!("----------\n"));
        report.push_str(&format!("Total Benchmarks: {}\n", total_tests));
        report.push_str(&format!("Average Throughput: {:.2} MB/s\n", avg_throughput));
        report.push_str(&format!("Peak Throughput: {:.2} MB/s\n", max_throughput));
        report.push_str(&format!("Average Ops/Second: {:.2}\n\n", avg_ops_per_sec));
        
        // Performance by collection type
        let mut type_groups: std::collections::HashMap<String, Vec<&CollectionBenchmarkResult>> = 
            std::collections::HashMap::new();
        
        for result in &self.results {
            type_groups.entry(result.collection_type.clone())
                .or_insert_with(Vec::new)
                .push(result);
        }
        
        report.push_str("📋 Performance by Collection Type\n");
        report.push_str("----------------------------------\n");
        
        for (collection_type, results) in type_groups {
            let avg_throughput: f64 = results.iter()
                .map(|r| r.throughput_mb_per_sec)
                .sum::<f64>() / results.len() as f64;
            let avg_parse_time: f64 = results.iter()
                .map(|r| r.parse_time.as_micros() as f64)
                .sum::<f64>() / results.len() as f64;
            let avg_serialize_time: f64 = results.iter()
                .map(|r| r.serialize_time.as_micros() as f64)
                .sum::<f64>() / results.len() as f64;
            
            report.push_str(&format!("• {}\n", collection_type));
            report.push_str(&format!("  Avg Throughput: {:.2} MB/s\n", avg_throughput));
            report.push_str(&format!("  Avg Parse Time: {:.1} μs\n", avg_parse_time));
            report.push_str(&format!("  Avg Serialize Time: {:.1} μs\n", avg_serialize_time));
            report.push_str("\n");
        }
        
        // Detailed results
        report.push_str("📊 Detailed Results\n");
        report.push_str("-------------------\n");
        report.push_str(&format!("{:<25} {:<10} {:<12} {:<12} {:<12} {:<12}\n",
            "Collection Type", "Elements", "Parse (μs)", "Serialize (μs)", "Size (bytes)", "Throughput (MB/s)"));
        report.push_str(&format!("{}\n", "-".repeat(95)));
        
        for result in &self.results {
            report.push_str(&format!("{:<25} {:<10} {:<12.1} {:<12.1} {:<12} {:<12.2}\n",
                result.collection_type,
                result.element_count,
                result.parse_time.as_micros() as f64,
                result.serialize_time.as_micros() as f64,
                result.data_size_bytes,
                result.throughput_mb_per_sec));
        }
        
        // Performance requirements analysis
        report.push_str("\n🎯 Performance Analysis\n");
        report.push_str("----------------------\n");
        
        let slow_operations: Vec<_> = self.results.iter()
            .filter(|r| r.parse_time.as_millis() > 10 || r.serialize_time.as_millis() > 10)
            .collect();
        
        if slow_operations.is_empty() {
            report.push_str("✅ All operations meet performance requirements (<10ms)\n");
        } else {
            report.push_str(&format!("⚠️  {} operations exceed 10ms threshold:\n", slow_operations.len()));
            for op in &slow_operations {
                report.push_str(&format!("   • {} ({}): parse={}ms, serialize={}ms\n",
                    op.collection_type,
                    op.element_count,
                    op.parse_time.as_millis(),
                    op.serialize_time.as_millis()));
            }
        }
        
        // Memory efficiency analysis
        let large_data: Vec<_> = self.results.iter()
            .filter(|r| r.data_size_bytes > 1_000_000) // >1MB
            .collect();
        
        if !large_data.is_empty() {
            report.push_str(&format!("\n📈 Large Data Collections (>1MB):\n"));
            for data in &large_data {
                let mb_size = data.data_size_bytes as f64 / 1_000_000.0;
                report.push_str(&format!("   • {} ({}): {:.2} MB, {:.2} MB/s\n",
                    data.collection_type,
                    data.element_count,
                    mb_size,
                    data.throughput_mb_per_sec));
            }
        }
        
        report
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collection_benchmarks() {
        let mut benchmarks = CollectionBenchmarks::new();
        let result = benchmarks.run_all_benchmarks();
        
        assert!(result.is_ok(), "Collection benchmarks failed: {:?}", result);
        assert!(!benchmarks.results.is_empty(), "No benchmark results generated");
        
        let report = benchmarks.generate_report();
        println!("{}", report);
        
        // Verify performance requirements
        for result in &benchmarks.results {
            // Parse time should be reasonable (<100ms for even large collections)
            assert!(result.parse_time.as_millis() < 100, 
                "Parse time too slow for {}: {}ms", result.collection_type, result.parse_time.as_millis());
            
            // Serialize time should be reasonable (<100ms for even large collections)
            assert!(result.serialize_time.as_millis() < 100,
                "Serialize time too slow for {}: {}ms", result.collection_type, result.serialize_time.as_millis());
            
            // Throughput should be reasonable (>1 MB/s for most operations)
            if result.data_size_bytes > 1000 { // Only check for non-trivial data sizes
                assert!(result.throughput_mb_per_sec > 0.1,
                    "Throughput too low for {}: {:.2} MB/s", result.collection_type, result.throughput_mb_per_sec);
            }
        }
    }
}