//! Parser factory for creating parser instances
//!
//! This module provides factory functions for creating parser instances
//! with different backends and configurations.

use crate::error::{Error, Result};
use std::sync::Arc;

use super::{
    config::{ParserConfig, ParserBackend},
    traits::{CqlParser, CqlParserFactory, FactoryInfo, ParserBackendInfo},
    nom_backend::NomParser,
    antlr_backend::AntlrParser,
};

/// Main parser factory
#[derive(Debug, Default)]
pub struct ParserFactory;

impl ParserFactory {
    /// Create a parser with the default configuration
    pub fn create_default() -> Result<Arc<dyn CqlParser + Send + Sync>> {
        let config = ParserConfig::default();
        Self::create(config)
    }
    
    /// Create a parser with the specified configuration
    pub fn create(config: ParserConfig) -> Result<Arc<dyn CqlParser + Send + Sync>> {
        // Validate configuration
        config.validate().map_err(|e| Error::configuration(e))?;
        
        let backend = match config.backend.clone() {
            ParserBackend::Nom => Self::create_nom_parser(config)?,
            ParserBackend::Antlr => Self::create_antlr_parser(config)?,
            ParserBackend::Auto => Self::create_auto_parser(config)?,
            ParserBackend::Custom(name) => Self::create_custom_parser(&name, config)?,
        };
        
        Ok(backend)
    }
    
    /// Create a nom-based parser
    fn create_nom_parser(config: ParserConfig) -> Result<Arc<dyn CqlParser + Send + Sync>> {
        let parser = NomParser::new(config)?;
        Ok(Arc::new(parser))
    }
    
    /// Create an ANTLR-based parser
    fn create_antlr_parser(config: ParserConfig) -> Result<Arc<dyn CqlParser + Send + Sync>> {
        let parser = AntlrParser::new(config)?;
        Ok(Arc::new(parser))
    }
    
    /// Auto-select the best parser based on configuration and input characteristics
    fn create_auto_parser(config: ParserConfig) -> Result<Arc<dyn CqlParser + Send + Sync>> {
        // Decision logic for auto-selection
        let backend = Self::select_optimal_backend(&config);
        
        let mut config_with_backend = config;
        config_with_backend.backend = backend;
        
        Self::create(config_with_backend)
    }
    
    /// Create a custom parser (for extensions)
    fn create_custom_parser(
        name: &str,
        _config: ParserConfig,
    ) -> Result<Arc<dyn CqlParser + Send + Sync>> {
        Err(Error::configuration(format!(
            "Custom parser '{}' not available",
            name
        )))
    }
    
    /// Select optimal backend based on configuration
    fn select_optimal_backend(config: &ParserConfig) -> ParserBackend {
        use super::config::ParserFeature;
        
        // Prefer ANTLR for features that require advanced parsing
        if config.has_feature(&ParserFeature::ErrorRecovery) ||
           config.has_feature(&ParserFeature::SyntaxHighlighting) ||
           config.has_feature(&ParserFeature::CodeCompletion) ||
           config.strict_validation
        {
            return ParserBackend::Antlr;
        }
        
        // Prefer nom for performance-oriented features
        if config.has_feature(&ParserFeature::Streaming) ||
           config.has_feature(&ParserFeature::Parallel) ||
           config.performance.optimization_level >= 2
        {
            return ParserBackend::Nom;
        }
        
        // Default to nom for general use
        ParserBackend::Nom
    }
    
    /// Get information about available backends
    pub fn get_available_backends() -> Vec<ParserBackendInfo> {
        vec![
            NomParser::backend_info(),
            AntlrParser::backend_info(),
        ]
    }
    
    /// Check if a specific backend is available
    pub fn is_backend_available(backend: &ParserBackend) -> bool {
        match backend {
            ParserBackend::Nom => true,
            ParserBackend::Antlr => true, // Will be true once implemented
            ParserBackend::Auto => true,
            ParserBackend::Custom(_) => false, // Would need registration system
        }
    }
    
    /// Get the recommended backend for specific use cases
    pub fn recommend_backend(use_case: UseCase) -> ParserBackend {
        match use_case {
            UseCase::HighPerformance => ParserBackend::Nom,
            UseCase::Development => ParserBackend::Antlr,
            UseCase::Production => ParserBackend::Auto,
            UseCase::Embedded => ParserBackend::Nom,
            UseCase::Interactive => ParserBackend::Antlr,
            UseCase::Batch => ParserBackend::Nom,
        }
    }
    
    /// Create a parser optimized for a specific use case
    pub fn create_for_use_case(use_case: UseCase) -> Result<Arc<dyn CqlParser + Send + Sync>> {
        let backend = Self::recommend_backend(use_case.clone());
        let config = Self::create_config_for_use_case(use_case, backend);
        Self::create(config)
    }
    
    /// Create configuration for a specific use case
    fn create_config_for_use_case(use_case: UseCase, backend: ParserBackend) -> ParserConfig {
        use super::config::ParserFeature;
        use std::time::Duration;
        
        match use_case {
            UseCase::HighPerformance => ParserConfig::fast().with_backend(backend),
            UseCase::Development => ParserConfig::development().with_backend(backend),
            UseCase::Production => ParserConfig::default().with_backend(backend),
            UseCase::Embedded => ParserConfig::minimal().with_backend(backend),
            UseCase::Interactive => {
                ParserConfig::development()
                    .with_backend(backend)
                    .with_feature(ParserFeature::CodeCompletion)
                    .with_feature(ParserFeature::SyntaxHighlighting)
                    .with_timeout(Duration::from_millis(100))
            }
            UseCase::Batch => {
                ParserConfig::fast()
                    .with_backend(backend)
                    .with_feature(ParserFeature::Parallel)
                    .with_timeout(Duration::from_secs(300))
            }
        }
    }
}

impl CqlParserFactory for ParserFactory {
    fn create_parser(&self) -> Result<Box<dyn CqlParser>> {
        let config = ParserConfig::default();
        let parser = Self::create(config)?;
        
        // We need to convert from Arc<dyn CqlParser + Send + Sync> to Box<dyn CqlParser>
        // This is a bit tricky, but we can work around it by creating a wrapper
        Ok(Box::new(ParserWrapper { inner: parser }))
    }
    
    fn create_parser_with_config(&self, config: ParserConfig) -> Result<Box<dyn CqlParser>> {
        let parser = Self::create(config)?;
        Ok(Box::new(ParserWrapper { inner: parser }))
    }
    
    fn factory_info(&self) -> FactoryInfo {
        FactoryInfo {
            name: "DefaultParserFactory".to_string(),
            supported_backends: vec!["nom".to_string(), "antlr".to_string()],
            default_backend: "auto".to_string(),
        }
    }
}

/// Wrapper to convert Arc<dyn CqlParser + Send + Sync> to Box<dyn CqlParser>
#[derive(Debug)]
struct ParserWrapper {
    inner: Arc<dyn CqlParser + Send + Sync>,
}

#[async_trait::async_trait]
impl CqlParser for ParserWrapper {
    async fn parse(&self, input: &str) -> Result<super::ast::CqlStatement> {
        self.inner.parse(input).await
    }
    
    async fn parse_type(&self, input: &str) -> Result<super::ast::CqlDataType> {
        self.inner.parse_type(input).await
    }
    
    async fn parse_expression(&self, input: &str) -> Result<super::ast::CqlExpression> {
        self.inner.parse_expression(input).await
    }
    
    async fn parse_identifier(&self, input: &str) -> Result<super::ast::CqlIdentifier> {
        self.inner.parse_identifier(input).await
    }
    
    async fn parse_literal(&self, input: &str) -> Result<super::ast::CqlLiteral> {
        self.inner.parse_literal(input).await
    }
    
    async fn parse_column_definitions(&self, input: &str) -> Result<Vec<super::ast::CqlColumnDef>> {
        self.inner.parse_column_definitions(input).await
    }
    
    async fn parse_table_options(&self, input: &str) -> Result<super::ast::CqlTableOptions> {
        self.inner.parse_table_options(input).await
    }
    
    fn validate_syntax(&self, input: &str) -> bool {
        self.inner.validate_syntax(input)
    }
    
    fn backend_info(&self) -> super::traits::ParserBackendInfo {
        self.inner.backend_info()
    }
}

/// Use case categories for parser optimization
#[derive(Debug, Clone, PartialEq)]
pub enum UseCase {
    /// High-performance parsing with minimal overhead
    HighPerformance,
    
    /// Development environment with rich error messages and debugging
    Development,
    
    /// Production environment with balanced performance and reliability
    Production,
    
    /// Embedded systems with strict resource constraints
    Embedded,
    
    /// Interactive environments requiring fast response times
    Interactive,
    
    /// Batch processing with large volumes of queries
    Batch,
}

/// Registry for custom parser backends
#[derive(Debug, Default)]
pub struct ParserRegistry {
    custom_factories: std::collections::HashMap<String, Box<dyn CqlParserFactory + Send + Sync>>,
}

impl ParserRegistry {
    /// Create a new parser registry
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Register a custom parser factory
    pub fn register_factory(
        &mut self,
        name: String,
        factory: Box<dyn CqlParserFactory + Send + Sync>,
    ) {
        self.custom_factories.insert(name, factory);
    }
    
    /// Get a registered factory
    pub fn get_factory(&self, name: &str) -> Option<&(dyn CqlParserFactory + Send + Sync)> {
        self.custom_factories.get(name).map(|f| f.as_ref())
    }
    
    /// List all registered factories
    pub fn list_factories(&self) -> Vec<&str> {
        self.custom_factories.keys().map(|s| s.as_str()).collect()
    }
    
    /// Create a parser using a registered factory
    pub fn create_with_factory(
        &self,
        factory_name: &str,
        config: ParserConfig,
    ) -> Result<Box<dyn CqlParser>> {
        let factory = self.get_factory(factory_name)
            .ok_or_else(|| Error::configuration(format!("Factory '{}' not found", factory_name)))?;
        
        factory.create_parser_with_config(config)
    }
}

/// Global parser registry instance
static mut GLOBAL_REGISTRY: Option<ParserRegistry> = None;
static REGISTRY_INIT: std::sync::Once = std::sync::Once::new();

/// Get the global parser registry
pub fn global_registry() -> &'static mut ParserRegistry {
    unsafe {
        REGISTRY_INIT.call_once(|| {
            GLOBAL_REGISTRY = Some(ParserRegistry::new());
        });
        GLOBAL_REGISTRY.as_mut().unwrap()
    }
}

/// Register a global parser factory
pub fn register_global_factory(
    name: String,
    factory: Box<dyn CqlParserFactory + Send + Sync>,
) {
    global_registry().register_factory(name, factory);
}

/// Benchmark different parser backends
pub mod benchmarks {
    use super::*;
    use std::time::{Duration, Instant};
    
    /// Benchmark result for a parser backend
    #[derive(Debug, Clone)]
    pub struct BenchmarkResult {
        pub backend: String,
        pub avg_parse_time: Duration,
        pub min_parse_time: Duration,
        pub max_parse_time: Duration,
        pub success_rate: f64,
        pub errors: Vec<String>,
    }
    
    /// Benchmark configuration
    #[derive(Debug, Clone)]
    pub struct BenchmarkConfig {
        pub iterations: u32,
        pub timeout: Duration,
        pub test_cases: Vec<String>,
    }
    
    impl Default for BenchmarkConfig {
        fn default() -> Self {
            Self {
                iterations: 100,
                timeout: Duration::from_secs(1),
                test_cases: vec![
                    "SELECT * FROM users".to_string(),
                    "INSERT INTO users (id, name) VALUES (?, ?)".to_string(),
                    "UPDATE users SET name = ? WHERE id = ?".to_string(),
                    "DELETE FROM users WHERE id = ?".to_string(),
                    "CREATE TABLE test (id UUID PRIMARY KEY, data TEXT)".to_string(),
                ],
            }
        }
    }
    
    /// Run benchmarks on available parser backends
    pub async fn benchmark_parsers(config: BenchmarkConfig) -> Vec<BenchmarkResult> {
        let backends = vec![ParserBackend::Nom, ParserBackend::Antlr];
        let mut results = Vec::new();
        
        for backend in backends {
            if ParserFactory::is_backend_available(&backend) {
                let result = benchmark_backend(backend, &config).await;
                results.push(result);
            }
        }
        
        results
    }
    
    /// Benchmark a specific parser backend
    async fn benchmark_backend(
        backend: ParserBackend,
        config: &BenchmarkConfig,
    ) -> BenchmarkResult {
        let parser_config = ParserConfig::default().with_backend(backend.clone());
        let parser = match ParserFactory::create(parser_config) {
            Ok(p) => p,
            Err(e) => {
                return BenchmarkResult {
                    backend: format!("{:?}", backend),
                    avg_parse_time: Duration::from_secs(0),
                    min_parse_time: Duration::from_secs(0),
                    max_parse_time: Duration::from_secs(0),
                    success_rate: 0.0,
                    errors: vec![format!("Failed to create parser: {}", e)],
                };
            }
        };
        
        let mut times = Vec::new();
        let mut errors = Vec::new();
        let mut successes = 0;
        
        for _ in 0..config.iterations {
            for test_case in &config.test_cases {
                let start = Instant::now();
                
                match tokio::time::timeout(config.timeout, parser.parse(test_case)).await {
                    Ok(Ok(_)) => {
                        successes += 1;
                        times.push(start.elapsed());
                    }
                    Ok(Err(e)) => {
                        errors.push(format!("Parse error: {}", e));
                    }
                    Err(_) => {
                        errors.push("Timeout".to_string());
                    }
                }
            }
        }
        
        let total_attempts = config.iterations * config.test_cases.len() as u32;
        let success_rate = successes as f64 / total_attempts as f64;
        
        let avg_time = if !times.is_empty() {
            times.iter().sum::<Duration>() / times.len() as u32
        } else {
            Duration::from_secs(0)
        };
        
        let min_time = times.iter().min().copied().unwrap_or_default();
        let max_time = times.iter().max().copied().unwrap_or_default();
        
        BenchmarkResult {
            backend: format!("{:?}", backend),
            avg_parse_time: avg_time,
            min_parse_time: min_time,
            max_parse_time: max_time,
            success_rate,
            errors,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_factory_creation() {
        let factory = ParserFactory;
        let info = factory.factory_info();
        assert_eq!(info.name, "DefaultParserFactory");
        assert!(!info.supported_backends.is_empty());
    }
    
    #[test]
    fn test_backend_availability() {
        assert!(ParserFactory::is_backend_available(&ParserBackend::Nom));
        assert!(ParserFactory::is_backend_available(&ParserBackend::Auto));
        assert!(!ParserFactory::is_backend_available(&ParserBackend::Custom("unknown".to_string())));
    }
    
    #[test]
    fn test_backend_recommendation() {
        assert_eq!(
            ParserFactory::recommend_backend(UseCase::HighPerformance),
            ParserBackend::Nom
        );
        assert_eq!(
            ParserFactory::recommend_backend(UseCase::Development),
            ParserBackend::Antlr
        );
    }
    
    #[test]
    fn test_auto_backend_selection() {
        let config = ParserConfig::fast();
        let backend = ParserFactory::select_optimal_backend(&config);
        assert_eq!(backend, ParserBackend::Nom);
        
        let config = ParserConfig::strict();
        let backend = ParserFactory::select_optimal_backend(&config);
        assert_eq!(backend, ParserBackend::Antlr);
    }
    
    #[test]
    fn test_parser_registry() {
        let registry = ParserRegistry::new();
        assert!(registry.list_factories().is_empty());
        
        // Can't easily test factory registration without implementing a mock factory
        // but the structure is in place
    }
}