//! Parser factory for creating parser instances
//!
//! This module provides factory functions for creating parser instances
//! with different backends and configurations.

use crate::error::{Error, Result};
use std::sync::{Arc, Mutex, OnceLock};

use super::{
    config::{ParserBackend, ParserConfig},
    nom_backend::NomParser,
    traits::{CqlParser, CqlParserFactory, FactoryInfo, ParserBackendInfo},
};

/// Main parser factory
#[derive(Debug, Default)]
pub struct ParserFactory;

impl ParserFactory {
    /// Create a parser with the default configuration.
    pub fn create_default() -> Result<Arc<dyn CqlParser + Send + Sync>> {
        Self::create(ParserConfig::default())
    }

    /// Create a parser with the specified configuration.
    pub fn create(mut config: ParserConfig) -> Result<Arc<dyn CqlParser + Send + Sync>> {
        config.validate().map_err(Error::configuration)?;

        if matches!(config.backend, ParserBackend::Auto) {
            config.backend = Self::select_optimal_backend(&config);
        }

        match config.backend.clone() {
            ParserBackend::Nom => Ok(Arc::new(NomParser::new(config)?)),
            ParserBackend::Auto => unreachable!("Auto resolved above"),
            ParserBackend::Custom(name) => Err(Error::configuration(format!(
                "Custom parser '{}' not available",
                name
            ))),
        }
    }

    /// Select the backend for an `Auto` configuration.
    ///
    /// nom is the only built-in backend, so `Auto` always resolves to it.
    fn select_optimal_backend(_config: &ParserConfig) -> ParserBackend {
        ParserBackend::Nom
    }

    /// Get information about available backends.
    pub fn get_available_backends() -> Vec<ParserBackendInfo> {
        vec![NomParser::backend_info()]
    }

    /// Check whether the given backend can be instantiated.
    ///
    /// nom is the only built-in backend; `Auto` resolves to it. Custom backends
    /// are never available through this factory.
    pub fn is_backend_available(backend: &ParserBackend) -> bool {
        match backend {
            ParserBackend::Nom | ParserBackend::Auto => true,
            ParserBackend::Custom(_) => false,
        }
    }

    /// Recommend a backend for a high-level use case.
    ///
    /// Every use case maps to the single built-in nom backend.
    pub fn recommend_backend(use_case: UseCase) -> ParserBackend {
        match use_case {
            UseCase::HighPerformance
            | UseCase::Embedded
            | UseCase::Batch
            | UseCase::Development
            | UseCase::Interactive
            | UseCase::Production => ParserBackend::Nom,
        }
    }

    /// Create a parser optimized for a specific use case.
    pub fn create_for_use_case(use_case: UseCase) -> Result<Arc<dyn CqlParser + Send + Sync>> {
        let backend = Self::recommend_backend(use_case.clone());
        Self::create(Self::create_config_for_use_case(use_case, backend))
    }

    fn create_config_for_use_case(use_case: UseCase, backend: ParserBackend) -> ParserConfig {
        use super::config::ParserFeature;
        use std::time::Duration;

        match use_case {
            UseCase::HighPerformance => ParserConfig::fast().with_backend(backend),
            UseCase::Development => ParserConfig::development().with_backend(backend),
            UseCase::Production => ParserConfig::default().with_backend(backend),
            UseCase::Embedded => ParserConfig::minimal().with_backend(backend),
            UseCase::Interactive => ParserConfig::development()
                .with_backend(backend)
                .with_timeout(Duration::from_millis(100)),
            UseCase::Batch => ParserConfig::fast()
                .with_backend(backend)
                .with_feature(ParserFeature::Parallel)
                .with_timeout(Duration::from_secs(300)),
        }
    }
}

impl CqlParserFactory for ParserFactory {
    fn create_parser(&self) -> Result<Box<dyn CqlParser>> {
        self.create_parser_with_config(ParserConfig::default())
    }

    fn create_parser_with_config(&self, config: ParserConfig) -> Result<Box<dyn CqlParser>> {
        Ok(Box::new(ParserWrapper {
            inner: Self::create(config)?,
        }))
    }

    fn factory_info(&self) -> FactoryInfo {
        FactoryInfo {
            name: "DefaultParserFactory".to_string(),
            supported_backends: vec!["nom".to_string()],
            default_backend: "auto".to_string(),
        }
    }
}

/// Adapter turning the shared `Arc<dyn CqlParser + Send + Sync>` returned by
/// [`ParserFactory::create`] into the `Box<dyn CqlParser>` required by
/// [`CqlParserFactory`].
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

/// Use case categories for parser optimization.
#[derive(Debug, Clone, PartialEq)]
pub enum UseCase {
    /// High-performance parsing with minimal overhead.
    HighPerformance,
    /// Development environment with rich error messages and debugging.
    Development,
    /// Production environment with balanced performance and reliability.
    Production,
    /// Embedded systems with strict resource constraints.
    Embedded,
    /// Interactive environments requiring fast response times.
    Interactive,
    /// Batch processing with large volumes of queries.
    Batch,
}

/// Registry for custom parser backends.
#[derive(Debug, Default)]
pub struct ParserRegistry {
    custom_factories: std::collections::HashMap<String, Box<dyn CqlParserFactory + Send + Sync>>,
}

impl ParserRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_factory(
        &mut self,
        name: String,
        factory: Box<dyn CqlParserFactory + Send + Sync>,
    ) {
        self.custom_factories.insert(name, factory);
    }

    pub fn get_factory(&self, name: &str) -> Option<&(dyn CqlParserFactory + Send + Sync)> {
        self.custom_factories.get(name).map(|f| f.as_ref())
    }

    pub fn list_factories(&self) -> Vec<&str> {
        self.custom_factories.keys().map(|s| s.as_str()).collect()
    }

    /// Create a parser using a registered factory.
    pub fn create_with_factory(
        &self,
        factory_name: &str,
        config: ParserConfig,
    ) -> Result<Box<dyn CqlParser>> {
        self.get_factory(factory_name)
            .ok_or_else(|| Error::configuration(format!("Factory '{}' not found", factory_name)))?
            .create_parser_with_config(config)
    }
}

static GLOBAL_REGISTRY: OnceLock<Mutex<ParserRegistry>> = OnceLock::new();

/// Register a factory in the process-wide registry.
pub fn register_global_factory(name: String, factory: Box<dyn CqlParserFactory + Send + Sync>) {
    let registry = GLOBAL_REGISTRY.get_or_init(|| Mutex::new(ParserRegistry::new()));
    let mut guard = registry.lock().unwrap_or_else(|e| e.into_inner());
    guard.register_factory(name, factory);
}

/// Benchmark helpers for comparing parser backends.
pub mod benchmarks {
    use super::*;
    use std::time::{Duration, Instant};

    #[derive(Debug, Clone)]
    pub struct BenchmarkResult {
        pub backend: String,
        pub avg_parse_time: Duration,
        pub min_parse_time: Duration,
        pub max_parse_time: Duration,
        pub success_rate: f64,
        pub errors: Vec<String>,
    }

    impl BenchmarkResult {
        fn failed(backend: &ParserBackend, error: String) -> Self {
            Self {
                backend: format!("{:?}", backend),
                avg_parse_time: Duration::ZERO,
                min_parse_time: Duration::ZERO,
                max_parse_time: Duration::ZERO,
                success_rate: 0.0,
                errors: vec![error],
            }
        }
    }

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

    /// Run benchmarks on all available parser backends.
    pub async fn benchmark_parsers(config: BenchmarkConfig) -> Vec<BenchmarkResult> {
        let mut results = Vec::new();
        for backend in [ParserBackend::Nom] {
            if ParserFactory::is_backend_available(&backend) {
                results.push(benchmark_backend(backend, &config).await);
            }
        }
        results
    }

    async fn benchmark_backend(
        backend: ParserBackend,
        config: &BenchmarkConfig,
    ) -> BenchmarkResult {
        let parser_config = ParserConfig::default().with_backend(backend.clone());
        let parser = match ParserFactory::create(parser_config) {
            Ok(p) => p,
            Err(e) => {
                return BenchmarkResult::failed(&backend, format!("Failed to create parser: {}", e))
            }
        };

        let mut times = Vec::new();
        let mut errors = Vec::new();
        let mut successes: u32 = 0;

        for _ in 0..config.iterations {
            for test_case in &config.test_cases {
                let start = Instant::now();
                match tokio::time::timeout(config.timeout, parser.parse(test_case)).await {
                    Ok(Ok(_)) => {
                        successes += 1;
                        times.push(start.elapsed());
                    }
                    Ok(Err(e)) => errors.push(format!("Parse error: {}", e)),
                    Err(_) => errors.push("Timeout".to_string()),
                }
            }
        }

        let total_attempts = config.iterations * config.test_cases.len() as u32;
        let success_rate = f64::from(successes) / f64::from(total_attempts);
        let avg_parse_time = if times.is_empty() {
            Duration::ZERO
        } else {
            times.iter().sum::<Duration>() / times.len() as u32
        };

        BenchmarkResult {
            backend: format!("{:?}", backend),
            avg_parse_time,
            min_parse_time: times.iter().min().copied().unwrap_or_default(),
            max_parse_time: times.iter().max().copied().unwrap_or_default(),
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
        assert!(!ParserFactory::is_backend_available(
            &ParserBackend::Custom("unknown".to_string())
        ));
    }

    #[test]
    fn test_backend_recommendation() {
        // Every use case now maps to the single built-in nom backend.
        for use_case in [
            UseCase::HighPerformance,
            UseCase::Development,
            UseCase::Production,
            UseCase::Embedded,
            UseCase::Interactive,
            UseCase::Batch,
        ] {
            assert_eq!(
                ParserFactory::recommend_backend(use_case),
                ParserBackend::Nom
            );
        }
    }

    #[test]
    fn test_auto_backend_selection() {
        // `Auto` always resolves to nom, regardless of features or strictness.
        let config = ParserConfig::fast();
        assert_eq!(
            ParserFactory::select_optimal_backend(&config),
            ParserBackend::Nom
        );

        let config = ParserConfig::strict();
        assert_eq!(
            ParserFactory::select_optimal_backend(&config),
            ParserBackend::Nom
        );
    }

    #[test]
    fn test_create_for_every_use_case_succeeds() {
        // Regression for issue #1639: a non-functional stub backend used to be
        // handed out for Development/Interactive and whenever strict_validation
        // was set, making every parse fail. Every use case must now yield a
        // working nom parser.
        for use_case in [
            UseCase::HighPerformance,
            UseCase::Development,
            UseCase::Production,
            UseCase::Embedded,
            UseCase::Interactive,
            UseCase::Batch,
        ] {
            let parser = ParserFactory::create_for_use_case(use_case.clone())
                .unwrap_or_else(|e| panic!("create failed for {:?}: {}", use_case, e));
            assert_eq!(parser.backend_info().name, "nom", "use case {:?}", use_case);
        }
    }

    #[test]
    fn test_parser_registry() {
        let registry = ParserRegistry::new();
        assert!(registry.list_factories().is_empty());
    }
}
