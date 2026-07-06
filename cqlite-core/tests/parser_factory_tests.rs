use cqlite_core::cql::config::{ParserBackend, ParserConfig, ParserFeature};
use cqlite_core::cql::factory::{ParserFactory, ParserRegistry, UseCase};
use cqlite_core::cql::traits::CqlParserFactory;
use std::time::Duration;

#[test]
fn test_parser_factory_public_api() {
    // Test public factory methods and configuration

    // Test default parser creation
    let result = ParserFactory::create_default();
    assert!(result.is_ok());

    // Test factory trait implementation
    let factory = ParserFactory;
    let parser_result = factory.create_parser();
    assert!(parser_result.is_ok());

    let config = ParserConfig::default();
    let parser_with_config = factory.create_parser_with_config(config);
    assert!(parser_with_config.is_ok());

    // Test factory info
    let info = factory.factory_info();
    assert_eq!(info.name, "DefaultParserFactory");
    assert!(!info.supported_backends.is_empty());
    assert_eq!(info.default_backend, "auto");
    assert!(info.supported_backends.contains(&"nom".to_string()));
    // nom is the only built-in backend after the ANTLR stub removal (#1639).
    assert!(!info.supported_backends.contains(&"antlr".to_string()));
}

#[test]
fn test_parser_factory_error_conditions() {
    // Test factory error handling for invalid configurations

    // Test invalid configuration timeout
    let mut invalid_config = ParserConfig::default();
    invalid_config = invalid_config.with_timeout(Duration::ZERO);
    let result = ParserFactory::create(invalid_config);
    // Should fail due to invalid timeout
    assert!(result.is_err());

    // Test custom parser creation failure
    let custom_config =
        ParserConfig::default().with_backend(ParserBackend::Custom("unknown".to_string()));
    let result = ParserFactory::create(custom_config);
    assert!(result.is_err());

    // Test backend availability checking
    assert!(ParserFactory::is_backend_available(&ParserBackend::Nom));
    assert!(ParserFactory::is_backend_available(&ParserBackend::Auto));
    assert!(!ParserFactory::is_backend_available(
        &ParserBackend::Custom("nonexistent".to_string())
    ));
}

#[test]
fn test_parser_factory_configuration_validation() {
    // Test configuration validation

    // Test valid configuration
    let valid_config = ParserConfig::default();
    let result = ParserFactory::create(valid_config);
    assert!(result.is_ok());

    // Test configuration with valid timeout
    let timeout_config = ParserConfig::default().with_timeout(Duration::from_secs(10));
    let result = ParserFactory::create(timeout_config);
    assert!(result.is_ok());

    // Test configuration with nom-supported features
    let feature_config = ParserConfig::default()
        .with_feature(ParserFeature::ErrorRecovery)
        .with_feature(ParserFeature::Caching);
    let result = ParserFactory::create(feature_config);
    assert!(result.is_ok());

    // Test configuration validation for the available backends
    let nom_config = ParserConfig::default().with_backend(ParserBackend::Nom);
    let result = ParserFactory::create(nom_config);
    assert!(result.is_ok());

    let auto_config = ParserConfig::default().with_backend(ParserBackend::Auto);
    let result = ParserFactory::create(auto_config);
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_use_case_recommendations() {
    // After the ANTLR stub removal (#1639) every use case maps to nom, and
    // every use case must produce a parser that actually parses valid CQL.
    let use_cases = [
        UseCase::HighPerformance,
        UseCase::Development,
        UseCase::Production,
        UseCase::Embedded,
        UseCase::Interactive,
        UseCase::Batch,
    ];

    for use_case in use_cases {
        assert_eq!(
            ParserFactory::recommend_backend(use_case.clone()),
            ParserBackend::Nom,
            "use case {:?} should recommend nom",
            use_case
        );

        let parser = ParserFactory::create_for_use_case(use_case.clone())
            .unwrap_or_else(|e| panic!("create_for_use_case({:?}) failed: {}", use_case, e));
        assert_eq!(parser.backend_info().name, "nom", "use case {:?}", use_case);

        let statement = parser
            .parse("CREATE TABLE ks.t (id uuid PRIMARY KEY)")
            .await
            .unwrap_or_else(|e| panic!("parse failed for {:?}: {}", use_case, e));
        assert!(matches!(
            statement,
            cqlite_core::cql::ast::CqlStatement::CreateTable(_)
        ));
    }
}

#[tokio::test]
async fn test_strict_validation_config_parses() {
    // Regression for #1639: strict_validation used to route to the ANTLR stub,
    // failing every parse. It must now succeed with the nom backend.
    let config = ParserConfig::default().with_strict_validation(true);
    let parser = ParserFactory::create(config).expect("strict config should create a parser");
    assert_eq!(parser.backend_info().name, "nom");
    let statement = parser
        .parse("CREATE TABLE ks.t (id uuid PRIMARY KEY)")
        .await
        .expect("strict-validation parse should succeed");
    assert!(matches!(
        statement,
        cqlite_core::cql::ast::CqlStatement::CreateTable(_)
    ));
}

#[test]
fn test_parser_registry_functionality() {
    // Test parser registry creation and management

    let registry = ParserRegistry::new();
    assert!(registry.list_factories().is_empty());

    // Test getting non-existent factory
    let result = registry.get_factory("nonexistent");
    assert!(result.is_none());

    // Test creating parser with non-existent factory
    let config = ParserConfig::default();
    let result = registry.create_with_factory("nonexistent", config);
    assert!(result.is_err());
}

#[test]
fn test_available_backends_info() {
    // Test backend information retrieval

    let backends = ParserFactory::get_available_backends();
    assert!(!backends.is_empty());

    // nom is the only built-in backend after the ANTLR stub removal (#1639).
    let backend_names: Vec<String> = backends.iter().map(|b| b.name.clone()).collect();
    assert!(backend_names.contains(&"nom".to_string()));
    assert!(!backend_names.contains(&"antlr".to_string()));

    // Each backend should have proper info
    for backend_info in backends {
        assert!(!backend_info.name.is_empty());
        assert!(!backend_info.version.is_empty());
        assert!(!backend_info.features.is_empty());
    }
}

#[test]
fn test_auto_parser_creation_flow() {
    // Test the complete auto parser creation flow

    // Create auto config
    let auto_config = ParserConfig::default().with_backend(ParserBackend::Auto);

    // Should successfully create parser
    let result = ParserFactory::create(auto_config);
    assert!(result.is_ok());

    // Test with different feature combinations
    // Use compatible feature combinations - avoid Streaming which may be incompatible with some auto-selected backends
    let error_recovery_auto = ParserConfig::default()
        .with_backend(ParserBackend::Auto)
        .with_feature(ParserFeature::ErrorRecovery);
    let result = ParserFactory::create(error_recovery_auto);
    assert!(result.is_ok());

    let cache_auto = ParserConfig::default()
        .with_backend(ParserBackend::Auto)
        .with_feature(ParserFeature::Caching);
    let result = ParserFactory::create(cache_auto);
    assert!(result.is_ok());
}

#[test]
fn test_parser_wrapper_functionality() {
    // Test parser wrapper that converts Arc<dyn CqlParser + Send + Sync> to Box<dyn CqlParser>

    let factory = ParserFactory;
    let parser = factory.create_parser().expect("Should create parser");

    // Test that all parser methods are properly delegated
    let backend_info = parser.backend_info();
    assert!(!backend_info.name.is_empty());

    // Test syntax validation
    let valid_sql = "SELECT * FROM users";
    let _is_valid = parser.validate_syntax(valid_sql);
    // Should not crash, return value depends on implementation

    let invalid_sql = "INVALID SQL SYNTAX HERE";
    let _is_invalid = parser.validate_syntax(invalid_sql);
    // Should not crash, return value depends on implementation
}

#[test]
fn test_complex_configuration_scenarios() {
    // Test complex configuration scenarios

    // Test configuration with multiple nom-supported features
    let complex_config = ParserConfig::default()
        .with_backend(ParserBackend::Auto)
        .with_feature(ParserFeature::ErrorRecovery)
        .with_feature(ParserFeature::Caching)
        .with_feature(ParserFeature::Streaming)
        .with_timeout(Duration::from_secs(30));

    let result = ParserFactory::create(complex_config);
    assert!(result.is_ok());

    // The ANTLR-only tooling features are now truthfully rejected: nom (the only
    // built-in backend after #1639) does not implement them.
    let unsupported = ParserConfig::default()
        .with_backend(ParserBackend::Auto)
        .with_feature(ParserFeature::SyntaxHighlighting);
    assert!(ParserFactory::create(unsupported).is_err());

    // Test configuration with performance settings
    let mut perf_config = ParserConfig::default();
    perf_config.performance.optimization_level = 3;
    perf_config.performance.enable_caching = true;

    let result = ParserFactory::create(perf_config);
    assert!(result.is_ok());

    // Test configuration with strict validation
    let strict_config = ParserConfig::strict().with_timeout(Duration::from_secs(120));

    let result = ParserFactory::create(strict_config);
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_benchmark_functionality() {
    use cqlite_core::cql::factory::benchmarks::{benchmark_parsers, BenchmarkConfig};

    // Test benchmark configuration
    let mut config = BenchmarkConfig::default();
    assert_eq!(config.iterations, 100);
    assert_eq!(config.timeout, Duration::from_secs(1));
    assert!(!config.test_cases.is_empty());

    // Modify config for faster test
    config.iterations = 1;
    config.timeout = Duration::from_millis(100);
    config.test_cases = vec!["SELECT 1".to_string()];

    // Run benchmarks
    let results = benchmark_parsers(config).await;

    // Should have results for available backends
    assert!(!results.is_empty());

    // Check result structure
    for result in results {
        assert!(!result.backend.is_empty());
        assert!(result.success_rate >= 0.0 && result.success_rate <= 1.0);
        // Other timing fields depend on actual execution
    }
}
