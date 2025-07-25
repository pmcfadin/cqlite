//! Parser configuration and settings
//!
//! This module defines configuration options for the parser subsystem,
//! allowing fine-tuning of parser behavior and backend selection.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

/// Parser configuration
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ParserConfig {
    /// Backend to use for parsing
    pub backend: ParserBackend,
    
    /// Timeout for parsing operations
    pub timeout: Duration,
    
    /// Maximum depth for nested expressions
    pub max_expression_depth: u32,
    
    /// Maximum number of items in collections
    pub max_collection_size: u32,
    
    /// Maximum length for string literals
    pub max_string_length: u32,
    
    /// Maximum number of parameters in a statement
    pub max_parameters: u32,
    
    /// Whether to enable strict validation
    pub strict_validation: bool,
    
    /// Whether to allow experimental features
    pub allow_experimental: bool,
    
    /// Backend-specific options
    pub backend_options: HashMap<String, serde_json::Value>,
    
    /// Features to enable
    pub features: Vec<ParserFeature>,
    
    /// Memory limits
    pub memory_limits: MemoryLimits,
    
    /// Performance settings
    pub performance: PerformanceSettings,
    
    /// Error handling settings
    pub error_handling: ErrorHandlingSettings,
    
    /// Memory settings
    pub memory_settings: MemorySettings,
    
    /// Security settings
    pub security_settings: SecuritySettings,
}

/// Parser backend selection
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ParserBackend {
    /// Use nom parser (fast, streaming)
    Nom,
    
    /// Use ANTLR parser (full-featured, better error recovery)
    Antlr,
    
    /// Auto-select best backend based on input characteristics
    Auto,
    
    /// Custom backend (for extensions)
    Custom(String),
}

/// Parser features that can be enabled/disabled
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ParserFeature {
    /// Support for streaming/incremental parsing
    Streaming,
    
    /// Enhanced error recovery
    ErrorRecovery,
    
    /// Syntax highlighting support
    SyntaxHighlighting,
    
    /// Code completion support
    CodeCompletion,
    
    /// AST transformation support
    AstTransformation,
    
    /// Custom operator support
    CustomOperators,
    
    /// Parallel parsing support
    Parallel,
    
    /// Caching of parse results
    Caching,
    
    /// Validation during parsing
    OnlineValidation,
    
    /// Performance profiling
    Profiling,
}

/// Memory limit settings
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemoryLimits {
    /// Maximum memory usage for AST construction (bytes)
    pub max_ast_size: u64,
    
    /// Maximum memory usage for temporary parsing data (bytes)
    pub max_temp_memory: u64,
    
    /// Maximum call stack depth
    pub max_stack_depth: u32,
    
    /// Maximum number of cached parse results
    pub max_cache_entries: u32,
}

/// Performance-related settings
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PerformanceSettings {
    /// Number of worker threads for parallel parsing
    pub worker_threads: u32,
    
    /// Buffer size for streaming parsing (bytes)
    pub stream_buffer_size: u32,
    
    /// Whether to enable parse result caching
    pub enable_caching: bool,
    
    /// Cache TTL for parse results
    pub cache_ttl: Duration,
    
    /// Whether to enable JIT compilation (if supported)
    pub enable_jit: bool,
    
    /// Optimization level (0-3)
    pub optimization_level: u8,
}

/// Error handling settings
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ErrorHandlingSettings {
    /// Maximum number of errors to collect before stopping
    pub max_errors: u32,
    
    /// Whether to continue parsing after recoverable errors
    pub continue_on_error: bool,
    
    /// Number of context lines to include in error messages
    pub error_context_lines: u32,
    
    /// Whether to include suggestions in error messages
    pub include_suggestions: bool,
    
    /// Whether to collect detailed error statistics
    pub collect_error_stats: bool,
}

/// Memory-specific settings for parser
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemorySettings {
    /// Maximum memory usage for parser operations (bytes)
    pub max_parser_memory: u64,
    
    /// Memory allocation strategy
    pub allocation_strategy: MemoryAllocationStrategy,
    
    /// Whether to enable memory pooling
    pub enable_memory_pooling: bool,
    
    /// Pool size for memory allocations
    pub memory_pool_size: usize,
}

/// Memory allocation strategies
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MemoryAllocationStrategy {
    /// Standard allocation
    Standard,
    /// Pool-based allocation
    Pooled,
    /// Arena-based allocation
    Arena,
}

/// Security settings for parser
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SecuritySettings {
    /// Maximum query depth to prevent stack overflow
    pub max_query_depth: u32,
    
    /// Maximum number of tokens in a query
    pub max_token_count: u32,
    
    /// Whether to enable input sanitization
    pub enable_input_sanitization: bool,
    
    /// Whether to restrict dangerous operations
    pub restrict_dangerous_operations: bool,
    
    /// List of blocked keywords
    pub blocked_keywords: Vec<String>,
}

impl Default for ParserConfig {
    fn default() -> Self {
        Self {
            backend: ParserBackend::Auto,
            timeout: Duration::from_secs(30),
            max_expression_depth: 100,
            max_collection_size: 10_000,
            max_string_length: 1_000_000,
            max_parameters: 1000,
            strict_validation: true,
            allow_experimental: false,
            backend_options: HashMap::new(),
            features: vec![
                ParserFeature::ErrorRecovery,
                ParserFeature::OnlineValidation,
            ],
            memory_limits: MemoryLimits::default(),
            performance: PerformanceSettings::default(),
            error_handling: ErrorHandlingSettings::default(),
            memory_settings: MemorySettings::default(),
            security_settings: SecuritySettings::default(),
        }
    }
}

impl Default for MemoryLimits {
    fn default() -> Self {
        Self {
            max_ast_size: 100 * 1024 * 1024, // 100 MB
            max_temp_memory: 50 * 1024 * 1024, // 50 MB
            max_stack_depth: 1000,
            max_cache_entries: 10_000,
        }
    }
}

impl Default for PerformanceSettings {
    fn default() -> Self {
        Self {
            worker_threads: num_cpus::get() as u32,
            stream_buffer_size: 64 * 1024, // 64 KB
            enable_caching: true,
            cache_ttl: Duration::from_secs(300), // 5 minutes
            enable_jit: false,
            optimization_level: 2,
        }
    }
}

impl Default for ErrorHandlingSettings {
    fn default() -> Self {
        Self {
            max_errors: 100,
            continue_on_error: true,
            error_context_lines: 3,
            include_suggestions: true,
            collect_error_stats: false,
        }
    }
}

impl Default for MemorySettings {
    fn default() -> Self {
        Self {
            max_parser_memory: 50 * 1024 * 1024, // 50 MB
            allocation_strategy: MemoryAllocationStrategy::Standard,
            enable_memory_pooling: false,
            memory_pool_size: 1024,
        }
    }
}

impl Default for SecuritySettings {
    fn default() -> Self {
        Self {
            max_query_depth: 100,
            max_token_count: 10_000,
            enable_input_sanitization: true,
            restrict_dangerous_operations: true,
            blocked_keywords: vec![],
        }
    }
}

impl ParserConfig {
    /// Create a new configuration with default values
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Create a fast configuration optimized for performance
    pub fn fast() -> Self {
        Self {
            backend: ParserBackend::Nom,
            timeout: Duration::from_secs(10),
            strict_validation: false,
            features: vec![ParserFeature::Parallel, ParserFeature::Caching],
            performance: PerformanceSettings {
                optimization_level: 3,
                enable_jit: true,
                ..Default::default()
            },
            ..Default::default()
        }
    }
    
    /// Create a strict configuration with maximum validation
    pub fn strict() -> Self {
        Self {
            backend: ParserBackend::Antlr,
            strict_validation: true,
            allow_experimental: false,
            features: vec![
                ParserFeature::ErrorRecovery,
                ParserFeature::OnlineValidation,
                ParserFeature::Profiling,
            ],
            error_handling: ErrorHandlingSettings {
                max_errors: 1,
                continue_on_error: false,
                include_suggestions: true,
                collect_error_stats: true,
                ..Default::default()
            },
            ..Default::default()
        }
    }
    
    /// Create a development configuration with debugging features
    pub fn development() -> Self {
        Self {
            backend: ParserBackend::Auto,
            allow_experimental: true,
            features: vec![
                ParserFeature::ErrorRecovery,
                ParserFeature::SyntaxHighlighting,
                ParserFeature::CodeCompletion,
                ParserFeature::AstTransformation,
                ParserFeature::OnlineValidation,
                ParserFeature::Profiling,
            ],
            error_handling: ErrorHandlingSettings {
                continue_on_error: true,
                include_suggestions: true,
                collect_error_stats: true,
                ..Default::default()
            },
            ..Default::default()
        }
    }
    
    /// Create a minimal configuration for embedded use
    pub fn minimal() -> Self {
        Self {
            backend: ParserBackend::Nom,
            timeout: Duration::from_secs(5),
            max_expression_depth: 50,
            max_collection_size: 1000,
            max_string_length: 10_000,
            max_parameters: 100,
            strict_validation: false,
            allow_experimental: false,
            backend_options: HashMap::new(),
            features: vec![],
            memory_limits: MemoryLimits {
                max_ast_size: 10 * 1024 * 1024, // 10 MB
                max_temp_memory: 5 * 1024 * 1024, // 5 MB
                max_stack_depth: 100,
                max_cache_entries: 100,
            },
            performance: PerformanceSettings {
                worker_threads: 1,
                enable_caching: false,
                optimization_level: 1,
                ..Default::default()
            },
            error_handling: ErrorHandlingSettings {
                max_errors: 10,
                error_context_lines: 1,
                include_suggestions: false,
                collect_error_stats: false,
                ..Default::default()
            },
            memory_settings: MemorySettings::default(),
            security_settings: SecuritySettings::default(),
        }
    }
    
    /// Set the parser backend
    pub fn with_backend(mut self, backend: ParserBackend) -> Self {
        self.backend = backend;
        self
    }
    
    /// Set the timeout
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
    
    /// Enable strict validation
    pub fn with_strict_validation(mut self, strict: bool) -> Self {
        self.strict_validation = strict;
        self
    }
    
    /// Add a feature
    pub fn with_feature(mut self, feature: ParserFeature) -> Self {
        if !self.features.contains(&feature) {
            self.features.push(feature);
        }
        self
    }
    
    /// Add multiple features
    pub fn with_features(mut self, features: Vec<ParserFeature>) -> Self {
        for feature in features {
            self = self.with_feature(feature);
        }
        self
    }
    
    /// Set a backend-specific option
    pub fn with_backend_option(mut self, key: String, value: serde_json::Value) -> Self {
        self.backend_options.insert(key, value);
        self
    }
    
    /// Set memory limits
    pub fn with_memory_limits(mut self, limits: MemoryLimits) -> Self {
        self.memory_limits = limits;
        self
    }
    
    /// Set performance settings
    pub fn with_performance(mut self, performance: PerformanceSettings) -> Self {
        self.performance = performance;
        self
    }
    
    /// Set error handling settings
    pub fn with_error_handling(mut self, error_handling: ErrorHandlingSettings) -> Self {
        self.error_handling = error_handling;
        self
    }
    
    /// Check if a feature is enabled
    pub fn has_feature(&self, feature: &ParserFeature) -> bool {
        self.features.contains(feature)
    }
    
    /// Get a backend-specific option
    pub fn get_backend_option(&self, key: &str) -> Option<&serde_json::Value> {
        self.backend_options.get(key)
    }
    
    /// Validate the configuration
    pub fn validate(&self) -> Result<(), String> {
        // Validate timeout
        if self.timeout.as_secs() == 0 {
            return Err("Timeout must be greater than 0".to_string());
        }
        
        // Validate expression depth
        if self.max_expression_depth == 0 {
            return Err("Max expression depth must be greater than 0".to_string());
        }
        
        // Validate collection size
        if self.max_collection_size == 0 {
            return Err("Max collection size must be greater than 0".to_string());
        }
        
        // Validate memory limits
        if self.memory_limits.max_ast_size == 0 {
            return Err("Max AST size must be greater than 0".to_string());
        }
        
        if self.memory_limits.max_stack_depth == 0 {
            return Err("Max stack depth must be greater than 0".to_string());
        }
        
        // Validate performance settings
        if self.performance.worker_threads == 0 {
            return Err("Worker threads must be greater than 0".to_string());
        }
        
        if self.performance.optimization_level > 3 {
            return Err("Optimization level must be 0-3".to_string());
        }
        
        // Validate error handling settings
        if self.error_handling.max_errors == 0 {
            return Err("Max errors must be greater than 0".to_string());
        }
        
        // Check feature compatibility
        if self.has_feature(&ParserFeature::Parallel) && self.performance.worker_threads == 1 {
            return Err("Parallel parsing requires more than 1 worker thread".to_string());
        }
        
        if self.has_feature(&ParserFeature::Streaming) && matches!(self.backend, ParserBackend::Antlr) {
            return Err("Streaming is not supported with ANTLR backend".to_string());
        }
        
        Ok(())
    }
    
    /// Create a configuration suitable for the given input characteristics
    pub fn for_input(input_size: usize, complexity: InputComplexity) -> Self {
        match complexity {
            InputComplexity::Simple => {
                if input_size < 1000 {
                    Self::minimal()
                } else {
                    Self::fast()
                }
            }
            InputComplexity::Medium => {
                Self::default()
            }
            InputComplexity::Complex => {
                Self::strict().with_timeout(Duration::from_secs(60))
            }
        }
    }
}

/// Input complexity classification
#[derive(Debug, Clone, PartialEq)]
pub enum InputComplexity {
    /// Simple queries with basic operations
    Simple,
    /// Medium complexity with joins, subqueries
    Medium,
    /// Complex queries with deep nesting, many conditions
    Complex,
}

/// Configuration builder for fluent API
#[derive(Debug, Default)]
pub struct ParserConfigBuilder {
    config: ParserConfig,
}

impl ParserConfigBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            config: ParserConfig::default(),
        }
    }
    
    /// Set backend
    pub fn backend(mut self, backend: ParserBackend) -> Self {
        self.config.backend = backend;
        self
    }
    
    /// Set timeout
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.config.timeout = timeout;
        self
    }
    
    /// Enable feature
    pub fn feature(mut self, feature: ParserFeature) -> Self {
        self.config = self.config.with_feature(feature);
        self
    }
    
    /// Set strict validation
    pub fn strict(mut self, strict: bool) -> Self {
        self.config.strict_validation = strict;
        self
    }
    
    /// Build the configuration
    pub fn build(self) -> Result<ParserConfig, String> {
        self.config.validate()?;
        Ok(self.config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_default_config() {
        let config = ParserConfig::default();
        assert!(matches!(config.backend, ParserBackend::Auto));
        assert!(config.strict_validation);
        assert!(!config.allow_experimental);
        assert!(config.validate().is_ok());
    }
    
    #[test]
    fn test_preset_configs() {
        let fast = ParserConfig::fast();
        assert!(matches!(fast.backend, ParserBackend::Nom));
        assert!(!fast.strict_validation);
        assert!(fast.validate().is_ok());
        
        let strict = ParserConfig::strict();
        assert!(matches!(strict.backend, ParserBackend::Antlr));
        assert!(strict.strict_validation);
        assert!(strict.validate().is_ok());
        
        let minimal = ParserConfig::minimal();
        assert_eq!(minimal.memory_limits.max_ast_size, 10 * 1024 * 1024);
        assert!(minimal.validate().is_ok());
    }
    
    #[test]
    fn test_config_validation() {
        let mut config = ParserConfig::default();
        assert!(config.validate().is_ok());
        
        // Invalid timeout
        config.timeout = Duration::from_secs(0);
        assert!(config.validate().is_err());
        
        // Reset and test invalid optimization level
        config = ParserConfig::default();
        config.performance.optimization_level = 5;
        assert!(config.validate().is_err());
    }
    
    #[test]
    fn test_config_builder() {
        let config = ParserConfigBuilder::new()
            .backend(ParserBackend::Nom)
            .timeout(Duration::from_secs(10))
            .feature(ParserFeature::Caching)
            .strict(false)
            .build()
            .unwrap();
        
        assert!(matches!(config.backend, ParserBackend::Nom));
        assert!(!config.strict_validation);
        assert!(config.has_feature(&ParserFeature::Caching));
    }
    
    #[test]
    fn test_feature_management() {
        let mut config = ParserConfig::default();
        assert!(!config.has_feature(&ParserFeature::Streaming));
        
        config = config.with_feature(ParserFeature::Streaming);
        assert!(config.has_feature(&ParserFeature::Streaming));
    }
    
    #[test]
    fn test_input_based_config() {
        let simple_config = ParserConfig::for_input(100, InputComplexity::Simple);
        assert!(matches!(simple_config.backend, ParserBackend::Nom));
        
        let complex_config = ParserConfig::for_input(10000, InputComplexity::Complex);
        assert!(matches!(complex_config.backend, ParserBackend::Antlr));
        assert_eq!(complex_config.timeout, Duration::from_secs(60));
    }
}