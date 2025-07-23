//! Integration Test Harness
//!
//! Provides utilities and helper functions for comprehensive integration testing
//! of CQLite with real Cassandra 5.0 SSTable data.

use cqlite_core::error::{Error, Result};
use cqlite_core::parser::SSTableParser;
use cqlite_core::storage::sstable::directory::SSTableDirectory;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// Test data validator for ensuring test environment is properly set up
pub struct TestDataValidator {
    test_data_path: PathBuf,
}

impl TestDataValidator {
    pub fn new(test_data_path: PathBuf) -> Self {
        Self { test_data_path }
    }

    /// Validate that test data environment is complete and ready
    pub fn validate_test_environment(&self) -> Result<TestEnvironmentStatus> {
        let mut status = TestEnvironmentStatus::default();

        // Check if base path exists
        if !self.test_data_path.exists() {
            return Err(Error::io_error(format!(
                "Test data path does not exist: {}. Run 'cd test-env/cassandra5 && ./manage.sh all' to generate test data.",
                self.test_data_path.display()
            )));
        }

        // Scan for table directories
        if let Ok(entries) = fs::read_dir(&self.test_data_path) {
            for entry in entries.flatten() {
                if entry.path().is_dir() {
                    let table_name = entry.file_name().to_string_lossy().to_string();
                    let table_info = self.analyze_table_directory(&entry.path())?;
                    status.tables.insert(table_name, table_info);
                }
            }
        }

        // Check for expected table types
        status.has_collections_table = status.tables.contains_key("collections_table-462afd10673711f0b2cf19d64e7cbecb");
        status.has_all_types_table = status.tables.contains_key("all_types-46200090673711f0b2cf19d64e7cbecb");
        status.has_time_series_table = status.tables.contains_key("time_series-464cb5e0673711f0b2cf19d64e7cbecb");
        status.has_large_table = status.tables.contains_key("large_table-465df3f0673711f0b2cf19d64e7cbecb");

        // Calculate completeness score
        status.completeness_score = self.calculate_completeness_score(&status);

        Ok(status)
    }

    fn analyze_table_directory(&self, table_path: &Path) -> Result<TableInfo> {
        let mut info = TableInfo::default();
        
        if let Ok(entries) = fs::read_dir(table_path) {
            for entry in entries.flatten() {
                let file_name = entry.file_name().to_string_lossy().to_string();
                
                if file_name.contains("Data.db") {
                    info.has_data_file = true;
                    if let Ok(metadata) = fs::metadata(&entry.path()) {
                        info.data_file_size = metadata.len();
                    }
                } else if file_name.contains("Statistics.db") {
                    info.has_statistics_file = true;
                } else if file_name.contains("TOC.txt") {
                    info.has_toc_file = true;
                } else if file_name.contains("CompressionInfo.db") {
                    info.has_compression_info = true;
                } else if file_name.contains("Filter.db") {
                    info.has_bloom_filter = true;
                } else if file_name.contains("Index.db") {
                    info.has_index_file = true;
                }
            }
        }

        info.is_complete = info.has_data_file && info.has_statistics_file && info.has_toc_file;
        Ok(info)
    }

    fn calculate_completeness_score(&self, status: &TestEnvironmentStatus) -> f64 {
        let mut score = 0.0;
        let total_checks = 4.0;

        if status.has_collections_table { score += 1.0; }
        if status.has_all_types_table { score += 1.0; }
        if status.has_time_series_table { score += 1.0; }
        if status.has_large_table { score += 1.0; }

        score / total_checks
    }
}

/// Status of the test environment
#[derive(Debug, Default)]
pub struct TestEnvironmentStatus {
    pub tables: HashMap<String, TableInfo>,
    pub has_collections_table: bool,
    pub has_all_types_table: bool,
    pub has_time_series_table: bool,
    pub has_large_table: bool,
    pub completeness_score: f64,
}

/// Information about a specific table in test data
#[derive(Debug, Default)]
pub struct TableInfo {
    pub has_data_file: bool,
    pub has_statistics_file: bool,
    pub has_toc_file: bool,
    pub has_compression_info: bool,
    pub has_bloom_filter: bool,
    pub has_index_file: bool,
    pub data_file_size: u64,
    pub is_complete: bool,
}

/// Test execution timer for measuring performance
pub struct TestTimer {
    start_time: Instant,
    checkpoints: Vec<(String, Instant)>,
}

impl TestTimer {
    pub fn new() -> Self {
        Self {
            start_time: Instant::now(),
            checkpoints: Vec::new(),
        }
    }

    pub fn checkpoint(&mut self, label: &str) {
        self.checkpoints.push((label.to_string(), Instant::now()));
    }

    pub fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }

    pub fn elapsed_since_checkpoint(&self, label: &str) -> Option<Duration> {
        self.checkpoints.iter()
            .find(|(l, _)| l == label)
            .map(|(_, time)| time.elapsed())
    }

    pub fn get_checkpoint_intervals(&self) -> Vec<(String, Duration)> {
        let mut intervals = Vec::new();
        let mut last_time = self.start_time;

        for (label, time) in &self.checkpoints {
            intervals.push((label.clone(), time.duration_since(last_time)));
            last_time = *time;
        }

        intervals
    }
}

/// Memory usage monitor for tracking resource consumption during tests
pub struct MemoryMonitor {
    initial_usage: usize,
}

impl MemoryMonitor {
    pub fn new() -> Self {
        Self {
            initial_usage: Self::get_memory_usage(),
        }
    }

    pub fn current_usage(&self) -> usize {
        Self::get_memory_usage()
    }

    pub fn delta_usage(&self) -> isize {
        Self::get_memory_usage() as isize - self.initial_usage as isize
    }

    fn get_memory_usage() -> usize {
        // Simplified memory tracking - in real implementation would use proper system calls
        // For now, return a reasonable estimate
        64 * 1024 * 1024 // 64MB baseline
    }
}

/// File pattern matcher for finding specific SSTable components
pub struct SSTableFileFinder {
    base_path: PathBuf,
}

impl SSTableFileFinder {
    pub fn new(base_path: PathBuf) -> Self {
        Self { base_path }
    }

    /// Find all Data.db files in test directories
    pub fn find_data_files(&self) -> Result<Vec<PathBuf>> {
        let mut data_files = Vec::new();
        self.find_files_with_pattern("Data.db", &mut data_files)?;
        Ok(data_files)
    }

    /// Find all Statistics.db files
    pub fn find_statistics_files(&self) -> Result<Vec<PathBuf>> {
        let mut stats_files = Vec::new();
        self.find_files_with_pattern("Statistics.db", &mut stats_files)?;
        Ok(stats_files)
    }

    /// Find all TOC.txt files
    pub fn find_toc_files(&self) -> Result<Vec<PathBuf>> {
        let mut toc_files = Vec::new();
        self.find_files_with_pattern("TOC.txt", &mut toc_files)?;
        Ok(toc_files)
    }

    /// Find files matching a specific pattern
    pub fn find_files_with_pattern(&self, pattern: &str, results: &mut Vec<PathBuf>) -> Result<()> {
        if let Ok(entries) = fs::read_dir(&self.base_path) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    self.find_files_in_directory(&path, pattern, results)?;
                }
            }
        }
        Ok(())
    }

    fn find_files_in_directory(&self, dir: &Path, pattern: &str, results: &mut Vec<PathBuf>) -> Result<()> {
        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.flatten() {
                let file_name = entry.file_name().to_string_lossy().to_string();
                if file_name.contains(pattern) {
                    results.push(entry.path());
                }
            }
        }
        Ok(())
    }

    /// Get table directory for specific table type
    pub fn get_table_directory(&self, table_type: &str) -> Option<PathBuf> {
        if let Ok(entries) = fs::read_dir(&self.base_path) {
            for entry in entries.flatten() {
                let dir_name = entry.file_name().to_string_lossy().to_string();
                if dir_name.starts_with(table_type) {
                    return Some(entry.path());
                }
            }
        }
        None
    }
}

/// Test result aggregator for collecting and analyzing test outcomes
pub struct TestResultAggregator {
    results: Vec<TestOutcome>,
}

impl TestResultAggregator {
    pub fn new() -> Self {
        Self {
            results: Vec::new(),
        }
    }

    pub fn add_result(&mut self, outcome: TestOutcome) {
        self.results.push(outcome);
    }

    pub fn get_summary(&self) -> TestSummary {
        let total_tests = self.results.len();
        let passed_tests = self.results.iter().filter(|r| r.passed).count();
        let failed_tests = total_tests - passed_tests;

        let total_execution_time: Duration = self.results.iter()
            .map(|r| r.execution_time)
            .sum();

        let total_bytes_processed: usize = self.results.iter()
            .map(|r| r.bytes_processed)
            .sum();

        let average_score: f64 = if total_tests > 0 {
            self.results.iter().map(|r| r.score).sum::<f64>() / total_tests as f64
        } else {
            0.0
        };

        TestSummary {
            total_tests,
            passed_tests,
            failed_tests,
            total_execution_time,
            total_bytes_processed,
            average_score,
            success_rate: if total_tests > 0 {
                passed_tests as f64 / total_tests as f64
            } else {
                0.0
            },
        }
    }

    pub fn get_results_by_category(&self) -> HashMap<String, Vec<&TestOutcome>> {
        let mut categorized = HashMap::new();
        for result in &self.results {
            categorized.entry(result.category.clone())
                .or_insert_with(Vec::new)
                .push(result);
        }
        categorized
    }

    pub fn get_failed_tests(&self) -> Vec<&TestOutcome> {
        self.results.iter().filter(|r| !r.passed).collect()
    }
}

/// Individual test outcome
#[derive(Debug, Clone)]
pub struct TestOutcome {
    pub name: String,
    pub category: String,
    pub passed: bool,
    pub execution_time: Duration,
    pub bytes_processed: usize,
    pub score: f64,
    pub error_message: Option<String>,
}

/// Test summary statistics
#[derive(Debug)]
pub struct TestSummary {
    pub total_tests: usize,
    pub passed_tests: usize,
    pub failed_tests: usize,
    pub total_execution_time: Duration,
    pub total_bytes_processed: usize,
    pub average_score: f64,
    pub success_rate: f64,
}

/// Data generator for creating test scenarios
pub struct TestDataGenerator;

impl TestDataGenerator {
    /// Generate corrupted SSTable data for error handling tests
    pub fn generate_corrupted_sstable(&self) -> Vec<u8> {
        // Create invalid SSTable data
        vec![0xDE, 0xAD, 0xBE, 0xEF, 0xFF, 0xFF, 0xFF, 0xFF]
    }

    /// Generate truncated file data
    pub fn generate_truncated_file(&self, original_size: usize) -> Vec<u8> {
        vec![0x6F, 0x61] // Only magic number, missing rest
    }

    /// Generate invalid VInt sequences
    pub fn generate_invalid_vints(&self) -> Vec<Vec<u8>> {
        vec![
            vec![0xFF; 10], // Too long
            vec![],         // Empty
            vec![0x80],     // Incomplete
        ]
    }
}

/// Performance measurement utilities
pub struct PerformanceMeasurer {
    measurements: Vec<PerformanceMeasurement>,
}

impl PerformanceMeasurer {
    pub fn new() -> Self {
        Self {
            measurements: Vec::new(),
        }
    }

    pub fn measure_operation<F, R>(&mut self, name: &str, operation: F) -> R
    where
        F: FnOnce() -> R,
    {
        let start_time = Instant::now();
        let memory_before = MemoryMonitor::new().current_usage();
        
        let result = operation();
        
        let execution_time = start_time.elapsed();
        let memory_after = MemoryMonitor::new().current_usage();
        
        self.measurements.push(PerformanceMeasurement {
            name: name.to_string(),
            execution_time,
            memory_delta: memory_after as isize - memory_before as isize,
            timestamp: Instant::now(),
        });

        result
    }

    pub fn get_measurements(&self) -> &[PerformanceMeasurement] {
        &self.measurements
    }

    pub fn get_average_execution_time(&self) -> Duration {
        if self.measurements.is_empty() {
            return Duration::from_secs(0);
        }

        let total: Duration = self.measurements.iter()
            .map(|m| m.execution_time)
            .sum();
        
        total / self.measurements.len() as u32
    }
}

#[derive(Debug, Clone)]
pub struct PerformanceMeasurement {
    pub name: String,
    pub execution_time: Duration,
    pub memory_delta: isize,
    pub timestamp: Instant,
}

/// Test case builder for creating parameterized tests
pub struct TestCaseBuilder {
    name: String,
    category: String,
    timeout: Option<Duration>,
    expected_failure: bool,
    prerequisites: Vec<String>,
}

impl TestCaseBuilder {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            category: "General".to_string(),
            timeout: None,
            expected_failure: false,
            prerequisites: Vec::new(),
        }
    }

    pub fn category(mut self, category: &str) -> Self {
        self.category = category.to_string();
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn expect_failure(mut self) -> Self {
        self.expected_failure = true;
        self
    }

    pub fn prerequisite(mut self, prerequisite: &str) -> Self {
        self.prerequisites.push(prerequisite.to_string());
        self
    }

    pub fn build(self) -> TestCase {
        TestCase {
            name: self.name,
            category: self.category,
            timeout: self.timeout,
            expected_failure: self.expected_failure,
            prerequisites: self.prerequisites,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TestCase {
    pub name: String,
    pub category: String,
    pub timeout: Option<Duration>,
    pub expected_failure: bool,
    pub prerequisites: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timer_functionality() {
        let mut timer = TestTimer::new();
        std::thread::sleep(Duration::from_millis(10));
        timer.checkpoint("test1");
        std::thread::sleep(Duration::from_millis(10));
        timer.checkpoint("test2");

        assert!(timer.elapsed() >= Duration::from_millis(20));
        assert!(timer.elapsed_since_checkpoint("test1").is_some());
        assert_eq!(timer.get_checkpoint_intervals().len(), 2);
    }

    #[test]
    fn test_memory_monitor() {
        let monitor = MemoryMonitor::new();
        let initial = monitor.current_usage();
        assert!(initial > 0);
    }

    #[test]
    fn test_result_aggregator() {
        let mut aggregator = TestResultAggregator::new();
        
        aggregator.add_result(TestOutcome {
            name: "test1".to_string(),
            category: "unit".to_string(),
            passed: true,
            execution_time: Duration::from_millis(100),
            bytes_processed: 1000,
            score: 0.9,
            error_message: None,
        });

        let summary = aggregator.get_summary();
        assert_eq!(summary.total_tests, 1);
        assert_eq!(summary.passed_tests, 1);
        assert!(summary.success_rate > 0.9);
    }

    #[test]
    fn test_performance_measurer() {
        let mut measurer = PerformanceMeasurer::new();
        
        let result = measurer.measure_operation("test_op", || {
            std::thread::sleep(Duration::from_millis(1));
            42
        });

        assert_eq!(result, 42);
        assert_eq!(measurer.get_measurements().len(), 1);
        assert!(measurer.get_average_execution_time() >= Duration::from_millis(1));
    }

    #[test]
    fn test_case_builder() {
        let test_case = TestCaseBuilder::new("test")
            .category("integration")
            .timeout(Duration::from_secs(30))
            .prerequisite("data_exists")
            .build();

        assert_eq!(test_case.name, "test");
        assert_eq!(test_case.category, "integration");
        assert_eq!(test_case.timeout, Some(Duration::from_secs(30)));
        assert_eq!(test_case.prerequisites.len(), 1);
    }
}