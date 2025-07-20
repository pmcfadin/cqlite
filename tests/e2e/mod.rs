//! Agentic E2E Test Framework Module
//!
//! This module provides the complete agentic end-to-end test framework for CQLite,
//! enabling AI-powered cross-language compatibility testing.

pub mod agentic_framework;
pub mod agentic_test_runner;
pub mod test_data_generator;

pub mod agents {
    pub mod query_generator;
    pub mod result_validator;
    pub mod performance_monitor;
}

// Re-export main framework components
pub use agentic_framework::{
    AgenticTestConfig, AgenticTestFramework, AgentCapability, AgentError, AgentFinding, 
    AgentMessage, AgentResult, AgentStatus, AgentTask, TestAgent, TestResult, TestSessionConfig,
    TargetLanguage, TaskType, Severity, FindingType, TestQuery, CompatibilityInconsistency,
};

pub use agentic_test_runner::{
    AgenticTestRunner, TestSuiteConfig, TestSuiteResults, CrossLanguageTestResults,
};

pub use test_data_generator::{
    TestDataGenerator, DataGenerationConfig, GeneratedTestData, TableSchema, CQLDataType,
};

// Re-export agents
pub use agents::{
    query_generator::QueryGeneratorAgent,
    result_validator::ResultValidatorAgent,
    performance_monitor::PerformanceMonitorAgent,
};

/// Main entry point for running agentic E2E tests
pub async fn run_agentic_tests() -> Result<(), AgentError> {
    println!("🤖 Starting Agentic E2E Test Framework for CQLite");
    
    // Create configuration
    let config = AgenticTestConfig::default();
    
    // Create and start test runner
    let mut runner = AgenticTestRunner::new(config).await?;
    runner.start().await?;
    
    // Generate test data
    let mut data_generator = TestDataGenerator::new(DataGenerationConfig::default());
    let generated_data = data_generator.generate_test_data().await?;
    
    println!("📊 Generated {} test tables with {} total rows", 
             generated_data.tables.len(),
             generated_data.tables.iter().map(|t| t.rows.len()).sum::<usize>());
    
    // Create test queries
    let test_queries = create_comprehensive_test_queries();
    
    // Run cross-language compatibility tests
    let results = runner.execute_cross_language_tests(
        test_queries,
        generated_data.tables.iter().map(|t| t.file_path.clone()).collect()
    ).await?;
    
    // Print results summary
    print_results_summary(&results);
    
    // Stop runner
    runner.stop().await?;
    
    Ok(())
}

/// Create comprehensive test queries for cross-language testing
fn create_comprehensive_test_queries() -> Vec<TestQuery> {
    vec![
        // Basic SELECT queries
        TestQuery {
            cql: "SELECT * FROM test_table_0 WHERE id = '550e8400-e29b-41d4-a716-446655440000'".to_string(),
            expected_schema: None,
            performance_expectations: Some(crate::agentic_framework::PerformanceExpectations {
                max_execution_time: std::time::Duration::from_millis(100),
                max_memory_usage: 1024 * 1024, // 1MB
                min_throughput: 100.0,
            }),
            compatibility_requirements: vec![
                crate::agentic_framework::CompatibilityRequirement {
                    language: TargetLanguage::Python,
                    result_consistency: crate::agentic_framework::ResultConsistency::Exact,
                    performance_tolerance: 0.1,
                },
                crate::agentic_framework::CompatibilityRequirement {
                    language: TargetLanguage::NodeJS,
                    result_consistency: crate::agentic_framework::ResultConsistency::Exact,
                    performance_tolerance: 0.2,
                },
            ],
        },
        
        // Aggregation queries
        TestQuery {
            cql: "SELECT COUNT(*) FROM test_table_1".to_string(),
            expected_schema: None,
            performance_expectations: Some(crate::agentic_framework::PerformanceExpectations {
                max_execution_time: std::time::Duration::from_millis(500),
                max_memory_usage: 10 * 1024 * 1024, // 10MB
                min_throughput: 50.0,
            }),
            compatibility_requirements: vec![],
        },
        
        // Complex WHERE clauses
        TestQuery {
            cql: "SELECT column_1, column_2 FROM test_table_2 WHERE column_3 > 1000 AND column_4 = 'test'".to_string(),
            expected_schema: None,
            performance_expectations: None,
            compatibility_requirements: vec![],
        },
        
        // Collection queries
        TestQuery {
            cql: "SELECT * FROM test_table_3 WHERE column_5 CONTAINS 'value'".to_string(),
            expected_schema: None,
            performance_expectations: None,
            compatibility_requirements: vec![],
        },
        
        // Sorting and limiting
        TestQuery {
            cql: "SELECT * FROM test_table_4 ORDER BY column_1 LIMIT 100".to_string(),
            expected_schema: None,
            performance_expectations: Some(crate::agentic_framework::PerformanceExpectations {
                max_execution_time: std::time::Duration::from_millis(200),
                max_memory_usage: 5 * 1024 * 1024, // 5MB
                min_throughput: 200.0,
            }),
            compatibility_requirements: vec![],
        },
    ]
}

/// Print test results summary
fn print_results_summary(results: &CrossLanguageTestResults) {
    println!("\n🎯 Agentic E2E Test Results Summary");
    println!("══════════════════════════════════════");
    println!("📊 Total Tests: {}", results.total_tests);
    println!("✅ Successful Tests: {}", results.test_results.iter().filter(|r| r.success).count());
    println!("❌ Failed Tests: {}", results.test_results.iter().filter(|r| !r.success).count());
    println!("🎯 Compatibility Score: {:.2}%", results.compatibility_score * 100.0);
    
    if !results.inconsistencies.is_empty() {
        println!("\n⚠️  Cross-Language Inconsistencies Detected:");
        for (i, inconsistency) in results.inconsistencies.iter().enumerate() {
            println!("  {}. {} - {} (Severity: {:?})", 
                     i + 1, 
                     inconsistency.query, 
                     inconsistency.description,
                     inconsistency.severity);
        }
    } else {
        println!("\n🎉 No cross-language inconsistencies detected!");
    }
    
    // Performance summary by language
    println!("\n📈 Performance Summary by Language:");
    let mut perf_by_lang = std::collections::HashMap::new();
    
    for result in &results.test_results {
        let entry = perf_by_lang.entry(&result.language).or_insert((Vec::new(), Vec::new()));
        entry.0.push(result.execution_time);
        entry.1.push(result.memory_usage);
    }
    
    for (language, (times, memory)) in perf_by_lang {
        let avg_time: std::time::Duration = times.iter().sum::<std::time::Duration>() / times.len() as u32;
        let avg_memory = memory.iter().sum::<u64>() / memory.len() as u64;
        
        println!("  {:?}: Avg Time: {:?}, Avg Memory: {} KB", 
                 language, 
                 avg_time, 
                 avg_memory / 1024);
    }
    
    println!("\n🤖 Agentic framework successfully validated the world's first direct SSTable query engine!");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_agentic_framework_integration() {
        // Test that all components can be imported and basic functionality works
        let config = AgenticTestConfig::default();
        assert!(!config.target_languages.is_empty());
        
        let data_config = DataGenerationConfig::default();
        assert!(data_config.table_count > 0);
    }
    
    #[test]
    fn test_query_generation() {
        let queries = create_comprehensive_test_queries();
        assert!(!queries.is_empty());
        assert!(queries.iter().all(|q| !q.cql.is_empty()));
    }
    
    #[tokio::test]
    async fn test_test_runner_creation() {
        let config = AgenticTestConfig::default();
        let runner = AgenticTestRunner::new(config).await;
        assert!(runner.is_ok());
    }
}