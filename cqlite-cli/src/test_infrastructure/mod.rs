//! Testing infrastructure for CQLite CLI
//!
//! This module provides comprehensive testing utilities including:
//! - TestContainer pattern for dependency injection
//! - CLI test utilities and helpers
//! - Test data fixtures and builders
//! - Integration test framework
//! - Performance testing utilities

pub mod container;
pub mod cli_helpers;
pub mod fixtures;
pub mod integration;
pub mod performance;
pub mod assertions;

// Re-export commonly used types
pub use container::{TestContainer, TestDatabase, TestEnvironment};
pub use cli_helpers::{CliTestRunner, CliTestBuilder, CommandAssertion};
pub use fixtures::{TestDataBuilder, SSTableFixture, SchemaFixture};
pub use integration::{IntegrationTestSuite, E2ETestRunner};
pub use performance::{PerformanceTestRunner, BenchmarkSuite};
pub use assertions::{ResultAssertion, OutputAssertion, ErrorAssertion};

/// Common test result type
pub type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Test configuration builder
#[derive(Debug, Clone)]
pub struct TestConfig {
    pub timeout: std::time::Duration,
    pub parallel: bool,
    pub verbose: bool,
    pub temp_dir: Option<std::path::PathBuf>,
    pub cleanup: bool,
}

impl Default for TestConfig {
    fn default() -> Self {
        Self {
            timeout: std::time::Duration::from_secs(30),
            parallel: false,
            verbose: false,
            temp_dir: None,
            cleanup: true,
        }
    }
}

impl TestConfig {
    pub fn new() -> Self {
        Self::default()
    }
    
    pub fn with_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.timeout = timeout;
        self
    }
    
    pub fn with_parallel(mut self) -> Self {
        self.parallel = true;
        self
    }
    
    pub fn with_verbose(mut self) -> Self {
        self.verbose = true;
        self
    }
    
    pub fn with_temp_dir<P: Into<std::path::PathBuf>>(mut self, dir: P) -> Self {
        self.temp_dir = Some(dir.into());
        self
    }
    
    pub fn no_cleanup(mut self) -> Self {
        self.cleanup = false;
        self
    }
}

/// Macro for creating test containers with automatic cleanup
#[macro_export]
macro_rules! test_container {
    ($name:ident) => {
        let $name = $crate::test_infrastructure::TestContainer::new()?;
    };
    ($name:ident, $config:expr) => {
        let $name = $crate::test_infrastructure::TestContainer::with_config($config)?;
    };
}

/// Macro for CLI test assertions
#[macro_export]
macro_rules! assert_cli_success {
    ($cmd:expr) => {
        $cmd.assert().success();
    };
    ($cmd:expr, $expected:expr) => {
        $cmd.assert().success().stdout(predicates::str::contains($expected));
    };
}

/// Macro for CLI error assertions
#[macro_export]
macro_rules! assert_cli_error {
    ($cmd:expr) => {
        $cmd.assert().failure();
    };
    ($cmd:expr, $expected:expr) => {
        $cmd.assert().failure().stderr(predicates::str::contains($expected));
    };
}