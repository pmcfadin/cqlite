//! Core Validation Framework
//!
//! This module provides the core validation framework that coordinates all validation activities.

use crate::error::{Error, Result};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use serde::{Deserialize, Serialize};

/// Core validation framework
#[derive(Debug)]
pub struct ValidationFramework {
    /// Framework configuration
    config: ValidationConfig,
    /// Validation context
    context: ValidationContext,
}

/// Configuration for the validation framework
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationConfig {
    /// Enable comprehensive validation
    pub enable_comprehensive_validation: bool,
    /// Enable performance benchmarking
    pub enable_performance_benchmarks: bool,
    /// Enable real-time monitoring
    pub enable_realtime_monitoring: bool,
    /// Maximum validation timeout (seconds)
    pub max_validation_timeout: u64,
    /// Test data directories
    pub test_data_directories: Vec<PathBuf>,
    /// Validation log level
    pub log_level: String,
    /// Parallel validation threads
    pub parallel_threads: usize,
}

impl Default for ValidationConfig {
    fn default() -> Self {
        Self {
            enable_comprehensive_validation: true,
            enable_performance_benchmarks: true,
            enable_realtime_monitoring: true,
            max_validation_timeout: 300, // 5 minutes
            test_data_directories: vec![
                PathBuf::from("test-data"),
                PathBuf::from("test-env/cassandra5/data"),
            ],
            log_level: "info".to_string(),
            parallel_threads: 4,
        }
    }
}

/// Validation context shared across all validators
#[derive(Debug, Clone)]
pub struct ValidationContext {
    /// Session ID for tracking
    pub session_id: String,
    /// Start time of validation
    pub start_time: Instant,
    /// Current validation phase
    pub current_phase: ValidationPhase,
    /// Accumulated metrics
    pub metrics: ValidationMetrics,
    /// Shared state between validators
    pub shared_state: HashMap<String, String>,
}

/// Validation phase enumeration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ValidationPhase {
    Initialization,
    DataIntegrity,
    ErrorHandling,
    FormatCompatibility,
    Performance,
    RealtimeMonitoring,
    ReportGeneration,
    Cleanup,
    Completed,
}

/// Validation metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationMetrics {
    /// Total tests executed
    pub total_tests: usize,
    /// Total validation time
    pub total_duration_ms: u64,
    /// Bytes processed
    pub bytes_processed: u64,
    /// Files validated
    pub files_validated: usize,
    /// Memory peak usage
    pub peak_memory_mb: f64,
}

impl Default for ValidationMetrics {
    fn default() -> Self {
        Self {
            total_tests: 0,
            total_duration_ms: 0,
            bytes_processed: 0,
            files_validated: 0,
            peak_memory_mb: 0.0,
        }
    }
}

impl ValidationFramework {
    /// Create a new validation framework
    pub fn new(config: ValidationConfig) -> Result<Self> {
        let context = ValidationContext {
            session_id: uuid::Uuid::new_v4().to_string(),
            start_time: Instant::now(),
            current_phase: ValidationPhase::Initialization,
            metrics: ValidationMetrics::default(),
            shared_state: HashMap::new(),
        };

        Ok(Self {
            config,
            context,
        })
    }

    /// Get framework configuration
    pub fn config(&self) -> &ValidationConfig {
        &self.config
    }

    /// Get validation context
    pub fn context(&self) -> &ValidationContext {
        &self.context
    }

    /// Update validation phase
    pub fn set_phase(&mut self, phase: ValidationPhase) {
        self.context.current_phase = phase;
        log::info!("Validation phase changed to: {:?}", phase);
    }

    /// Update metrics
    pub fn update_metrics(&mut self, update: ValidationMetricsUpdate) {
        self.context.metrics.total_tests += update.tests_added;
        self.context.metrics.total_duration_ms += update.duration_ms;
        self.context.metrics.bytes_processed += update.bytes_processed;
        self.context.metrics.files_validated += update.files_validated;
        self.context.metrics.peak_memory_mb = self.context.metrics.peak_memory_mb.max(update.memory_mb);
    }

    /// Get current session metrics
    pub fn get_current_metrics(&self) -> ValidationMetrics {
        let mut metrics = self.context.metrics.clone();
        metrics.total_duration_ms = self.context.start_time.elapsed().as_millis() as u64;
        metrics
    }

    /// Set shared state value
    pub fn set_shared_state(&mut self, key: String, value: String) {
        self.context.shared_state.insert(key, value);
    }

    /// Get shared state value
    pub fn get_shared_state(&self, key: &str) -> Option<&String> {
        self.context.shared_state.get(key)
    }

    /// Check if framework is configured for comprehensive validation
    pub fn is_comprehensive_enabled(&self) -> bool {
        self.config.enable_comprehensive_validation
    }

    /// Check if performance benchmarks are enabled
    pub fn is_performance_enabled(&self) -> bool {
        self.config.enable_performance_benchmarks
    }

    /// Check if real-time monitoring is enabled
    pub fn is_realtime_monitoring_enabled(&self) -> bool {
        self.config.enable_realtime_monitoring
    }

    /// Get test data directories
    pub fn test_data_directories(&self) -> &[PathBuf] {
        &self.config.test_data_directories
    }

    /// Get maximum validation timeout
    pub fn max_timeout(&self) -> Duration {
        Duration::from_secs(self.config.max_validation_timeout)
    }

    /// Get parallel thread count
    pub fn parallel_threads(&self) -> usize {
        self.config.parallel_threads
    }

    /// Cleanup resources
    pub fn cleanup(&mut self) -> Result<()> {
        self.set_phase(ValidationPhase::Cleanup);
        
        // Cleanup operations
        self.context.shared_state.clear();
        
        self.set_phase(ValidationPhase::Completed);
        log::info!("Validation framework cleanup completed");
        
        Ok(())
    }
}

/// Metrics update structure
#[derive(Debug, Clone, Default)]
pub struct ValidationMetricsUpdate {
    pub tests_added: usize,
    pub duration_ms: u64,
    pub bytes_processed: u64,
    pub files_validated: usize,
    pub memory_mb: f64,
}

impl ValidationMetricsUpdate {
    /// Create a new metrics update
    pub fn new() -> Self {
        Default::default()
    }

    /// Add test count
    pub fn with_tests(mut self, count: usize) -> Self {
        self.tests_added = count;
        self
    }

    /// Add duration
    pub fn with_duration(mut self, duration: Duration) -> Self {
        self.duration_ms = duration.as_millis() as u64;
        self
    }

    /// Add bytes processed
    pub fn with_bytes(mut self, bytes: u64) -> Self {
        self.bytes_processed = bytes;
        self
    }

    /// Add files validated
    pub fn with_files(mut self, files: usize) -> Self {
        self.files_validated = files;
        self
    }

    /// Add memory usage
    pub fn with_memory(mut self, memory_mb: f64) -> Self {
        self.memory_mb = memory_mb;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validation_framework_creation() {
        let config = ValidationConfig::default();
        let framework = ValidationFramework::new(config);
        assert!(framework.is_ok());
        
        let framework = framework.unwrap();
        assert_eq!(framework.context().current_phase, ValidationPhase::Initialization);
    }

    #[test]
    fn test_validation_phase_update() {
        let config = ValidationConfig::default();
        let mut framework = ValidationFramework::new(config).unwrap();
        
        framework.set_phase(ValidationPhase::DataIntegrity);
        assert_eq!(framework.context().current_phase, ValidationPhase::DataIntegrity);
    }

    #[test]
    fn test_metrics_update() {
        let config = ValidationConfig::default();
        let mut framework = ValidationFramework::new(config).unwrap();
        
        let update = ValidationMetricsUpdate::new()
            .with_tests(5)
            .with_bytes(1024)
            .with_files(2);
        
        framework.update_metrics(update);
        
        let metrics = framework.get_current_metrics();
        assert_eq!(metrics.total_tests, 5);
        assert_eq!(metrics.bytes_processed, 1024);
        assert_eq!(metrics.files_validated, 2);
    }

    #[test]
    fn test_shared_state() {
        let config = ValidationConfig::default();
        let mut framework = ValidationFramework::new(config).unwrap();
        
        framework.set_shared_state("test_key".to_string(), "test_value".to_string());
        assert_eq!(framework.get_shared_state("test_key"), Some(&"test_value".to_string()));
        assert_eq!(framework.get_shared_state("missing_key"), None);
    }

    #[test]
    fn test_configuration_flags() {
        let config = ValidationConfig::default();
        let framework = ValidationFramework::new(config).unwrap();
        
        assert!(framework.is_comprehensive_enabled());
        assert!(framework.is_performance_enabled());
        assert!(framework.is_realtime_monitoring_enabled());
    }
}