//! TestContainer pattern implementation for CQLite CLI testing
//!
//! This module provides a dependency injection container for testing,
//! allowing for easy setup and teardown of test environments.

use super::{TestConfig, TestResult};
use crate::config::Config;
use cqlite_core::{Config as CoreConfig, Database};
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::sync::Mutex;

/// Test container that manages test dependencies and lifecycle
#[derive(Debug)]
pub struct TestContainer {
    config: TestConfig,
    temp_dir: Arc<TempDir>,
    database: Arc<Mutex<Option<TestDatabase>>>,
    cleanup_handlers: Vec<Box<dyn Fn() -> TestResult + Send + Sync>>,
}

/// Test database wrapper with enhanced testing capabilities
#[derive(Debug)]
pub struct TestDatabase {
    pub database: Database,
    pub db_path: PathBuf,
    pub schema_path: Option<PathBuf>,
    pub data_fixtures: Vec<PathBuf>,
}

/// Test environment configuration
#[derive(Debug, Clone)]
pub struct TestEnvironment {
    pub temp_dir: PathBuf,
    pub db_path: PathBuf,
    pub config_path: PathBuf,
    pub fixtures_dir: PathBuf,
    pub output_dir: PathBuf,
}

impl TestContainer {
    /// Create a new test container with default configuration
    pub fn new() -> TestResult<Self> {
        Self::with_config(TestConfig::default())
    }

    /// Create a new test container with custom configuration
    pub fn with_config(config: TestConfig) -> TestResult<Self> {
        let temp_dir = if let Some(ref dir) = config.temp_dir {
            TempDir::new_in(dir)?
        } else {
            TempDir::new()?
        };

        let container = Self {
            config,
            temp_dir: Arc::new(temp_dir),
            database: Arc::new(Mutex::new(None)),
            cleanup_handlers: Vec::new(),
        };

        container.setup_environment()?;
        Ok(container)
    }

    /// Get the test environment configuration
    pub fn environment(&self) -> TestEnvironment {
        let temp_path = self.temp_dir.path();
        TestEnvironment {
            temp_dir: temp_path.to_path_buf(),
            db_path: temp_path.join("test.db"),
            config_path: temp_path.join("config.toml"),
            fixtures_dir: temp_path.join("fixtures"),
            output_dir: temp_path.join("output"),
        }
    }

    /// Initialize a test database
    pub async fn init_database(&self) -> TestResult<Arc<Mutex<TestDatabase>>> {
        let env = self.environment();

        // Create core config
        let core_config = CoreConfig::default();

        // Initialize database
        let database = Database::open(&env.db_path, core_config)
            .await
            .map_err(|e| format!("Failed to initialize test database: {}", e))?;

        let test_db = TestDatabase {
            database,
            db_path: env.db_path,
            schema_path: None,
            data_fixtures: Vec::new(),
        };

        let test_db_arc = Arc::new(Mutex::new(test_db));
        *self.database.lock().await = Some(test_db_arc.lock().await.clone());

        Ok(test_db_arc)
    }

    /// Get or initialize the test database
    pub async fn database(&self) -> TestResult<Arc<Mutex<TestDatabase>>> {
        let mut db_guard = self.database.lock().await;
        if db_guard.is_none() {
            drop(db_guard);
            return self.init_database().await;
        }

        // Return a clone of the existing database
        let existing_db = db_guard.as_ref().unwrap();
        Ok(Arc::new(Mutex::new(existing_db.lock().await.clone())))
    }

    /// Create CLI configuration for testing
    pub fn create_cli_config(&self) -> TestResult<Config> {
        let env = self.environment();

        let mut config = Config::default();
        config.default_database = Some(env.db_path);

        // Write config to file
        config.save_to_file(&env.config_path)?;

        Ok(config)
    }

    /// Add cleanup handler
    pub fn add_cleanup_handler<F>(&mut self, handler: F)
    where
        F: Fn() -> TestResult + Send + Sync + 'static,
    {
        self.cleanup_handlers.push(Box::new(handler));
    }

    /// Setup test environment directories
    fn setup_environment(&self) -> TestResult<()> {
        let env = self.environment();

        // Create necessary directories
        std::fs::create_dir_all(&env.fixtures_dir)?;
        std::fs::create_dir_all(&env.output_dir)?;

        // Create basic configuration file
        let config = Config::default();
        config.save_to_file(&env.config_path)?;

        Ok(())
    }

    /// Execute cleanup handlers
    pub fn cleanup(&self) -> TestResult<()> {
        if !self.config.cleanup {
            return Ok(());
        }

        for handler in &self.cleanup_handlers {
            if let Err(e) = handler() {
                eprintln!("Cleanup handler failed: {}", e);
            }
        }

        Ok(())
    }
}

impl Drop for TestContainer {
    fn drop(&mut self) {
        if let Err(e) = self.cleanup() {
            eprintln!("Failed to cleanup test container: {}", e);
        }
    }
}

impl Clone for TestDatabase {
    fn clone(&self) -> Self {
        Self {
            database: self.database.clone(),
            db_path: self.db_path.clone(),
            schema_path: self.schema_path.clone(),
            data_fixtures: self.data_fixtures.clone(),
        }
    }
}

impl TestDatabase {
    /// Add schema file to the test database
    pub fn with_schema<P: Into<PathBuf>>(mut self, schema_path: P) -> Self {
        self.schema_path = Some(schema_path.into());
        self
    }

    /// Add data fixture to the test database
    pub fn with_fixture<P: Into<PathBuf>>(mut self, fixture_path: P) -> Self {
        self.data_fixtures.push(fixture_path.into());
        self
    }

    /// Execute a query against the test database
    pub async fn execute_query(&self, query: &str) -> TestResult<Vec<String>> {
        // This is a placeholder - implement actual query execution
        // based on your database API
        println!("Executing query: {}", query);
        Ok(vec!["query_result".to_string()])
    }

    /// Load schema from file
    pub async fn load_schema(&self) -> TestResult<()> {
        if let Some(ref schema_path) = self.schema_path {
            let schema_content = std::fs::read_to_string(schema_path)?;
            println!("Loading schema: {}", schema_content);
            // Implement actual schema loading
        }
        Ok(())
    }

    /// Load test fixtures
    pub async fn load_fixtures(&self) -> TestResult<()> {
        for fixture_path in &self.data_fixtures {
            let fixture_content = std::fs::read_to_string(fixture_path)?;
            println!("Loading fixture: {}", fixture_content);
            // Implement actual fixture loading
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_container_creation() {
        let container = TestContainer::new().unwrap();
        let env = container.environment();

        assert!(env.temp_dir.exists());
        assert!(env.fixtures_dir.exists());
        assert!(env.output_dir.exists());
        assert!(env.config_path.exists());
    }

    #[tokio::test]
    async fn test_database_initialization() {
        let container = TestContainer::new().unwrap();
        let _db = container.init_database().await.unwrap();

        // Verify database was created
        let env = container.environment();
        assert!(env.db_path.exists());
    }

    #[tokio::test]
    async fn test_cli_config_creation() {
        let container = TestContainer::new().unwrap();
        let config = container.create_cli_config().unwrap();

        let env = container.environment();
        assert_eq!(config.default_database, Some(env.db_path));
    }

    #[test]
    fn test_test_config_builder() {
        let config = TestConfig::new()
            .with_timeout(std::time::Duration::from_secs(60))
            .with_parallel()
            .with_verbose()
            .no_cleanup();

        assert_eq!(config.timeout, std::time::Duration::from_secs(60));
        assert!(config.parallel);
        assert!(config.verbose);
        assert!(!config.cleanup);
    }
}
