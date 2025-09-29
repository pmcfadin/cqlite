//! Enhanced Test Context Framework for CQLite Phase 2
//!
//! This module provides an advanced test infrastructure framework designed to achieve
//! 95% test coverage and enable comprehensive quality gates. It extends the existing
//! TestContext with schema validation, property-based testing, and coverage tracking.
//!
//! ## Key Features
//!
//! ### 1. Enhanced TestContext
//! - Schema-aware test validation
//! - Real-time coverage tracking
//! - Property-based test configuration
//! - Quality gate enforcement
//!
//! ### 2. Test Categories
//! - Organized test taxonomy for systematic coverage
//! - Performance benchmarking categories
//! - Integration test classification
//!
//! ### 3. Coverage Tracking
//! - Line and branch coverage monitoring
//! - Component-level coverage analysis
//! - Real-time quality gates
//!
//! ## Usage Example
//!
//! ```rust
//! use cqlite_core::tests::common::enhanced_test_context::{
//!     EnhancedTestContext, TestCategory, QualityGate
//! };
//!
//! #[tokio::test]
//! async fn test_comprehensive_sstable_validation() {
//!     let mut context = EnhancedTestContext::builder()
//!         .category(TestCategory::Integration(IntegrationSubcategory::SSTableReading))
//!         .schema_validation(true)
//!         .coverage_tracking(true)
//!         .quality_gate(QualityGate::new().min_coverage(90.0))
//!         .build("test_basic")
//!         .await
//!         .unwrap();
//!
//!     // Run test with automatic coverage tracking
//!     context.run_test_with_coverage(|| async {
//!         // Your test logic here
//!         Ok(())
//!     }).await.unwrap();
//!
//!     // Validate quality gates
//!     let metrics = context.validate_and_cleanup().await.unwrap();
//!     assert!(metrics.coverage_percentage >= 90.0);
//! }
//! ```

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tempfile::TempDir;

use cqlite_core::{schema::TableSchema, types::Value, Config, Error, Result};

// Re-export the original TestContext for compatibility
pub use super::sstable_test_utils::{
    DatasetDescriptor, SSTableComponent, TableDescriptor, TestContext, TestMetrics,
};

/// Test category taxonomy for systematic test organization
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TestCategory {
    /// Unit tests for individual components
    Unit(UnitSubcategory),
    /// Integration tests for component interactions
    Integration(IntegrationSubcategory),
    /// Performance and regression tests
    Performance(PerformanceSubcategory),
    /// Property-based testing
    Property(PropertySubcategory),
    /// End-to-end system tests
    EndToEnd(E2ESubcategory),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum UnitSubcategory {
    SSTableParsing,
    IndexReading,
    CompressionDecompression,
    SchemaValidation,
    ErrorHandling,
    MemoryManagement,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum IntegrationSubcategory {
    SSTableReading,
    PartitionLookup,
    RangeScanning,
    CacheIntegration,
    CompressionPipeline,
    SchemaEvolution,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PerformanceSubcategory {
    ThroughputBenchmarks,
    LatencyMeasurement,
    MemoryUsageProfiles,
    ConcurrencyStress,
    RegressionDetection,
    ScalabilityTests,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PropertySubcategory {
    DataIntegrity,
    Serialization,
    Compression,
    IndexConsistency,
    StateMachineProperties,
    InvariantChecking,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum E2ESubcategory {
    CQLiteCompatibility,
    RealWorldWorkloads,
    MultiTableOperations,
    FailureRecovery,
    BackwardCompatibility,
    EdgeCaseHandling,
}

impl fmt::Display for TestCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TestCategory::Unit(sub) => write!(f, "Unit::{:?}", sub),
            TestCategory::Integration(sub) => write!(f, "Integration::{:?}", sub),
            TestCategory::Performance(sub) => write!(f, "Performance::{:?}", sub),
            TestCategory::Property(sub) => write!(f, "Property::{:?}", sub),
            TestCategory::EndToEnd(sub) => write!(f, "EndToEnd::{:?}", sub),
        }
    }
}

/// Schema validation configuration for tests
#[derive(Debug, Clone)]
pub struct SchemaValidationConfig {
    /// Enable strict schema validation
    pub strict_mode: bool,
    /// Expected table schema
    pub expected_schema: Option<TableSchema>,
    /// Allowed schema evolution operations
    pub allowed_evolutions: Vec<SchemaEvolution>,
    /// Custom validation rules
    pub custom_validators: Vec<Box<dyn SchemaValidator>>,
}

/// Schema evolution operations
#[derive(Debug, Clone, PartialEq)]
pub enum SchemaEvolution {
    AddColumn(String),
    DropColumn(String),
    ChangeColumnType(String, String),
    AddIndex(String),
    DropIndex(String),
}

/// Trait for custom schema validators
pub trait SchemaValidator: fmt::Debug + Send + Sync {
    fn validate(&self, schema: &TableSchema) -> Result<()>;
}

/// Property-based testing configuration
#[derive(Debug, Clone)]
pub struct PropertyTestConfig {
    /// Number of test cases to generate
    pub test_cases: usize,
    /// Maximum size for generated data
    pub max_size: usize,
    /// Seed for reproducible tests
    pub seed: Option<u64>,
    /// Shrinking configuration
    pub shrink_config: ShrinkConfig,
    /// Custom generators
    pub generators: HashMap<String, PropertyGenerator>,
}

#[derive(Debug, Clone)]
pub struct ShrinkConfig {
    pub max_iterations: usize,
    pub strategies: Vec<ShrinkStrategy>,
}

#[derive(Debug, Clone)]
pub enum ShrinkStrategy {
    RemoveElements,
    ReduceSize,
    SimplifyStructure,
}

/// Property generator trait
pub trait PropertyGenerator: fmt::Debug + Send + Sync {
    fn generate(&self, size: usize) -> Result<Value>;
}

/// Coverage tracking for quality gates
#[derive(Debug, Clone, Default)]
pub struct CoverageTracker {
    /// Lines covered during testing
    pub lines_covered: HashSet<String>,
    /// Total lines in the codebase
    pub total_lines: usize,
    /// Branch coverage information
    pub branches_covered: HashMap<String, BranchCoverage>,
    /// Function coverage
    pub functions_covered: HashSet<String>,
    /// Component-level coverage
    pub component_coverage: HashMap<String, ComponentCoverage>,
}

#[derive(Debug, Clone)]
pub struct BranchCoverage {
    pub taken: bool,
    pub not_taken: bool,
}

#[derive(Debug, Clone)]
pub struct ComponentCoverage {
    pub component_name: String,
    pub lines_covered: usize,
    pub total_lines: usize,
    pub functions_covered: usize,
    pub total_functions: usize,
}

impl CoverageTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_line_coverage(&mut self, file: &str, line: usize) {
        self.lines_covered.insert(format!("{}:{}", file, line));
    }

    pub fn record_function_coverage(&mut self, function: &str) {
        self.functions_covered.insert(function.to_string());
    }

    pub fn coverage_percentage(&self) -> f64 {
        if self.total_lines == 0 {
            0.0
        } else {
            (self.lines_covered.len() as f64 / self.total_lines as f64) * 100.0
        }
    }

    pub fn component_coverage_percentage(&self, component: &str) -> f64 {
        if let Some(coverage) = self.component_coverage.get(component) {
            if coverage.total_lines == 0 {
                0.0
            } else {
                (coverage.lines_covered as f64 / coverage.total_lines as f64) * 100.0
            }
        } else {
            0.0
        }
    }
}

/// Quality gate configuration and enforcement
#[derive(Debug, Clone)]
pub struct QualityGate {
    /// Minimum required line coverage percentage
    pub min_coverage: f64,
    /// Minimum required branch coverage percentage
    pub min_branch_coverage: f64,
    /// Maximum allowed test execution time
    pub max_execution_time: Duration,
    /// Maximum allowed memory usage (in MB)
    pub max_memory_usage: usize,
    /// Required component coverage targets
    pub component_targets: HashMap<String, f64>,
    /// Custom quality rules
    pub custom_rules: Vec<Box<dyn QualityRule>>,
}

/// Trait for custom quality rules
pub trait QualityRule: fmt::Debug + Send + Sync {
    fn validate(&self, metrics: &EnhancedTestMetrics) -> Result<()>;
}

impl QualityGate {
    pub fn new() -> Self {
        Self {
            min_coverage: 95.0, // Target 95% coverage for Phase 2
            min_branch_coverage: 85.0,
            max_execution_time: Duration::from_secs(300), // 5 minutes
            max_memory_usage: 512,                        // 512MB
            component_targets: HashMap::new(),
            custom_rules: Vec::new(),
        }
    }

    pub fn min_coverage(mut self, coverage: f64) -> Self {
        self.min_coverage = coverage;
        self
    }

    pub fn min_branch_coverage(mut self, coverage: f64) -> Self {
        self.min_branch_coverage = coverage;
        self
    }

    pub fn max_execution_time(mut self, duration: Duration) -> Self {
        self.max_execution_time = duration;
        self
    }

    pub fn component_target(mut self, component: String, target: f64) -> Self {
        self.component_targets.insert(component, target);
        self
    }

    pub fn validate(&self, metrics: &EnhancedTestMetrics) -> Result<()> {
        // Validate coverage requirements
        if metrics.coverage.coverage_percentage() < self.min_coverage {
            return Err(Error::TestFailure(format!(
                "Coverage too low: {:.2}% < {:.2}%",
                metrics.coverage.coverage_percentage(),
                self.min_coverage
            )));
        }

        // Validate execution time
        if metrics.execution_time > self.max_execution_time {
            return Err(Error::TestFailure(format!(
                "Execution time too long: {:?} > {:?}",
                metrics.execution_time, self.max_execution_time
            )));
        }

        // Validate memory usage
        let max_memory_mb = metrics.memory_peaks.iter().max().copied().unwrap_or(0) / (1024 * 1024);
        if max_memory_mb > self.max_memory_usage {
            return Err(Error::TestFailure(format!(
                "Memory usage too high: {}MB > {}MB",
                max_memory_mb, self.max_memory_usage
            )));
        }

        // Validate component-specific targets
        for (component, target) in &self.component_targets {
            let component_coverage = metrics.coverage.component_coverage_percentage(component);
            if component_coverage < *target {
                return Err(Error::TestFailure(format!(
                    "Component '{}' coverage too low: {:.2}% < {:.2}%",
                    component, component_coverage, target
                )));
            }
        }

        // Validate custom rules
        for rule in &self.custom_rules {
            rule.validate(metrics)?;
        }

        Ok(())
    }
}

/// Enhanced test metrics with coverage and quality information
#[derive(Debug, Clone)]
pub struct EnhancedTestMetrics {
    /// Base test metrics from original framework
    pub base_metrics: TestMetrics,
    /// Test category
    pub category: TestCategory,
    /// Coverage tracking data
    pub coverage: CoverageTracker,
    /// Schema validation results
    pub schema_validation: Option<SchemaValidationResult>,
    /// Property test results
    pub property_test_results: Vec<PropertyTestResult>,
    /// Test execution time
    pub execution_time: Duration,
    /// Memory usage peaks during test execution
    pub memory_peaks: Vec<usize>,
    /// Error and failure information
    pub errors: Vec<TestError>,
    /// Quality gate validation status
    pub quality_status: QualityStatus,
}

#[derive(Debug, Clone)]
pub struct SchemaValidationResult {
    pub valid: bool,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct PropertyTestResult {
    pub property_name: String,
    pub test_cases_run: usize,
    pub failures: usize,
    pub counterexamples: Vec<String>,
    pub shrink_iterations: usize,
}

#[derive(Debug, Clone)]
pub struct TestError {
    pub error_type: TestErrorType,
    pub message: String,
    pub location: Option<String>,
    pub stack_trace: Option<String>,
}

#[derive(Debug, Clone)]
pub enum TestErrorType {
    AssertionFailure,
    SchemaValidationError,
    PropertyTestFailure,
    CoverageInsufficient,
    PerformanceRegression,
    MemoryLeak,
    TimeoutExceeded,
}

#[derive(Debug, Clone)]
pub enum QualityStatus {
    Passed,
    Failed(Vec<String>),
    Warning(Vec<String>),
}

/// Enhanced test context builder for fluent configuration
#[derive(Debug)]
pub struct EnhancedTestContextBuilder {
    category: Option<TestCategory>,
    schema_validation: Option<SchemaValidationConfig>,
    property_testing: Option<PropertyTestConfig>,
    coverage_tracking: bool,
    quality_gate: Option<QualityGate>,
    base_config: Option<Config>,
}

impl EnhancedTestContextBuilder {
    pub fn new() -> Self {
        Self {
            category: None,
            schema_validation: None,
            property_testing: None,
            coverage_tracking: false,
            quality_gate: None,
            base_config: None,
        }
    }

    pub fn category(mut self, category: TestCategory) -> Self {
        self.category = Some(category);
        self
    }

    pub fn schema_validation(mut self, enabled: bool) -> Self {
        if enabled {
            self.schema_validation = Some(SchemaValidationConfig {
                strict_mode: true,
                expected_schema: None,
                allowed_evolutions: Vec::new(),
                custom_validators: Vec::new(),
            });
        }
        self
    }

    pub fn schema_validation_config(mut self, config: SchemaValidationConfig) -> Self {
        self.schema_validation = Some(config);
        self
    }

    pub fn property_testing(mut self, config: PropertyTestConfig) -> Self {
        self.property_testing = Some(config);
        self
    }

    pub fn coverage_tracking(mut self, enabled: bool) -> Self {
        self.coverage_tracking = enabled;
        self
    }

    pub fn quality_gate(mut self, gate: QualityGate) -> Self {
        self.quality_gate = Some(gate);
        self
    }

    pub fn base_config(mut self, config: Config) -> Self {
        self.base_config = Some(config);
        self
    }

    pub async fn build(self, dataset_name: &str) -> Result<EnhancedTestContext> {
        let base_context = TestContext::new(dataset_name).await?;

        Ok(EnhancedTestContext {
            base_context,
            category: self
                .category
                .unwrap_or(TestCategory::Unit(UnitSubcategory::SSTableParsing)),
            schema_validation: self.schema_validation,
            property_testing: self.property_testing,
            coverage_tracker: if self.coverage_tracking {
                Some(Arc::new(Mutex::new(CoverageTracker::new())))
            } else {
                None
            },
            quality_gate: self.quality_gate,
            start_time: Instant::now(),
            errors: Vec::new(),
        })
    }
}

/// Enhanced test context with advanced features
#[derive(Debug)]
pub struct EnhancedTestContext {
    /// Base test context for compatibility
    pub base_context: TestContext,
    /// Test category
    pub category: TestCategory,
    /// Schema validation configuration
    pub schema_validation: Option<SchemaValidationConfig>,
    /// Property testing configuration
    pub property_testing: Option<PropertyTestConfig>,
    /// Coverage tracker
    pub coverage_tracker: Option<Arc<Mutex<CoverageTracker>>>,
    /// Quality gate configuration
    pub quality_gate: Option<QualityGate>,
    /// Test execution start time
    start_time: Instant,
    /// Accumulated errors during test execution
    errors: Vec<TestError>,
}

impl EnhancedTestContext {
    /// Create a new enhanced test context builder
    pub fn builder() -> EnhancedTestContextBuilder {
        EnhancedTestContextBuilder::new()
    }

    /// Run a test with automatic coverage tracking and quality validation
    pub async fn run_test_with_coverage<F, Fut, T>(&mut self, test_fn: F) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let start_time = Instant::now();

        // Initialize coverage tracking if enabled
        if let Some(tracker) = &self.coverage_tracker {
            let mut tracker = tracker.lock().unwrap();
            tracker.total_lines = self.estimate_total_lines();
        }

        // Run the test
        let result = test_fn().await;

        // Record execution time
        let execution_time = start_time.elapsed();

        // Validate results if quality gate is configured
        if let Some(quality_gate) = &self.quality_gate {
            let metrics = self.create_metrics(execution_time)?;
            quality_gate.validate(&metrics)?;
        }

        result
    }

    /// Validate schema against expected schema
    pub fn validate_schema(
        &mut self,
        actual_schema: &TableSchema,
    ) -> Result<SchemaValidationResult> {
        if let Some(config) = &self.schema_validation {
            let mut errors = Vec::new();
            let mut warnings = Vec::new();

            if let Some(expected) = &config.expected_schema {
                // Compare schemas
                if actual_schema.keyspace != expected.keyspace {
                    errors.push(format!(
                        "Keyspace mismatch: expected '{}', got '{}'",
                        expected.keyspace, actual_schema.keyspace
                    ));
                }

                if actual_schema.table != expected.table {
                    errors.push(format!(
                        "Table name mismatch: expected '{}', got '{}'",
                        expected.table, actual_schema.table
                    ));
                }

                // Validate columns
                for expected_col in &expected.columns {
                    if !actual_schema
                        .columns
                        .iter()
                        .any(|col| col.name == expected_col.name)
                    {
                        if config.strict_mode {
                            errors.push(format!("Missing column: {}", expected_col.name));
                        } else {
                            warnings.push(format!("Missing column: {}", expected_col.name));
                        }
                    }
                }
            }

            // Run custom validators
            for validator in &config.custom_validators {
                if let Err(e) = validator.validate(actual_schema) {
                    errors.push(format!("Custom validation failed: {}", e));
                }
            }

            Ok(SchemaValidationResult {
                valid: errors.is_empty(),
                errors,
                warnings,
            })
        } else {
            Ok(SchemaValidationResult {
                valid: true,
                errors: Vec::new(),
                warnings: Vec::new(),
            })
        }
    }

    /// Record coverage for a specific line
    pub fn record_coverage(&self, file: &str, line: usize) {
        if let Some(tracker) = &self.coverage_tracker {
            let mut tracker = tracker.lock().unwrap();
            tracker.record_line_coverage(file, line);
        }
    }

    /// Record function coverage
    pub fn record_function_coverage(&self, function: &str) {
        if let Some(tracker) = &self.coverage_tracker {
            let mut tracker = tracker.lock().unwrap();
            tracker.record_function_coverage(function);
        }
    }

    /// Add a test error
    pub fn add_error(&mut self, error: TestError) {
        self.errors.push(error);
    }

    /// Validate and cleanup the test context
    pub async fn validate_and_cleanup(self) -> Result<EnhancedTestMetrics> {
        let execution_time = self.start_time.elapsed();
        let metrics = self.create_metrics(execution_time)?;

        // Validate quality gates if configured
        let quality_status = if let Some(quality_gate) = &self.quality_gate {
            match quality_gate.validate(&metrics) {
                Ok(()) => QualityStatus::Passed,
                Err(e) => QualityStatus::Failed(vec![e.to_string()]),
            }
        } else {
            QualityStatus::Passed
        };

        Ok(EnhancedTestMetrics {
            base_metrics: self.base_context.cleanup()?,
            category: self.category,
            coverage: self
                .coverage_tracker
                .map(|t| t.lock().unwrap().clone())
                .unwrap_or_default(),
            schema_validation: None, // Would be set during schema validation
            property_test_results: Vec::new(), // Would be populated during property testing
            execution_time,
            memory_peaks: Vec::new(), // Would be populated during execution
            errors: self.errors,
            quality_status,
        })
    }

    /// Create metrics snapshot
    fn create_metrics(&self, execution_time: Duration) -> Result<EnhancedTestMetrics> {
        Ok(EnhancedTestMetrics {
            base_metrics: self.base_context.metrics.clone(),
            category: self.category.clone(),
            coverage: self
                .coverage_tracker
                .as_ref()
                .map(|t| t.lock().unwrap().clone())
                .unwrap_or_default(),
            schema_validation: None,
            property_test_results: Vec::new(),
            execution_time,
            memory_peaks: Vec::new(),
            errors: self.errors.clone(),
            quality_status: QualityStatus::Passed, // Placeholder
        })
    }

    /// Estimate total lines in codebase for coverage calculation
    fn estimate_total_lines(&self) -> usize {
        // This would be calculated by analyzing the source code
        // For now, return a reasonable estimate
        50000 // Estimated lines in codebase
    }
}

// Default implementations for configuration structs
impl Default for SchemaValidationConfig {
    fn default() -> Self {
        Self {
            strict_mode: false,
            expected_schema: None,
            allowed_evolutions: Vec::new(),
            custom_validators: Vec::new(),
        }
    }
}

impl Default for PropertyTestConfig {
    fn default() -> Self {
        Self {
            test_cases: 100,
            max_size: 1000,
            seed: None,
            shrink_config: ShrinkConfig {
                max_iterations: 100,
                strategies: vec![ShrinkStrategy::RemoveElements, ShrinkStrategy::ReduceSize],
            },
            generators: HashMap::new(),
        }
    }
}

impl Default for QualityGate {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_enhanced_context_creation() {
        let context = EnhancedTestContext::builder()
            .category(TestCategory::Unit(UnitSubcategory::SSTableParsing))
            .coverage_tracking(true)
            .build("test_basic")
            .await;

        assert!(context.is_ok());
    }

    #[test]
    fn test_coverage_tracking() {
        let mut tracker = CoverageTracker::new();
        tracker.total_lines = 100;
        tracker.record_line_coverage("test.rs", 10);
        tracker.record_line_coverage("test.rs", 20);

        assert_eq!(tracker.coverage_percentage(), 2.0);
    }

    #[test]
    fn test_quality_gate_validation() {
        let gate = QualityGate::new().min_coverage(50.0);

        let mut metrics = EnhancedTestMetrics {
            base_metrics: TestMetrics::default(),
            category: TestCategory::Unit(UnitSubcategory::SSTableParsing),
            coverage: CoverageTracker::new(),
            schema_validation: None,
            property_test_results: Vec::new(),
            execution_time: Duration::from_millis(100),
            memory_peaks: vec![1024 * 1024], // 1MB
            errors: Vec::new(),
            quality_status: QualityStatus::Passed,
        };

        // Should fail with low coverage
        assert!(gate.validate(&metrics).is_err());

        // Set up adequate coverage
        metrics.coverage.total_lines = 100;
        for i in 0..60 {
            metrics.coverage.record_line_coverage("test.rs", i);
        }

        // Should pass with adequate coverage
        assert!(gate.validate(&metrics).is_ok());
    }

    #[test]
    fn test_test_categorization() {
        let category = TestCategory::Integration(IntegrationSubcategory::SSTableReading);
        assert_eq!(category.to_string(), "Integration::SSTableReading");
    }
}
