# CQLite Testing Architecture - Implementation Guide

## Quick Start Implementation

This guide provides concrete code examples and step-by-step instructions for implementing the comprehensive testing architecture designed for CQLite.

## 1. Core Infrastructure Setup

### 1.1 Test Container and Dependency Injection

Create `/tests/infrastructure/test_container.rs`:

```rust
//! Test Container for Dependency Injection
//! Provides mock and real implementations for testing

use std::collections::HashMap;
use std::sync::Arc;
use anyhow::Result;
use tokio::sync::RwLock;

/// Main dependency injection container for tests
pub struct TestContainer {
    services: HashMap<String, Arc<dyn std::any::Any + Send + Sync>>,
    config: TestContainerConfig,
}

#[derive(Clone)]
pub struct TestContainerConfig {
    pub use_mocks: bool,
    pub enable_logging: bool,
    pub temp_dir: Option<std::path::PathBuf>,
}

impl Default for TestContainerConfig {
    fn default() -> Self {
        Self {
            use_mocks: true,
            enable_logging: false,
            temp_dir: None,
        }
    }
}

impl TestContainer {
    pub fn new(config: TestContainerConfig) -> Self {
        let mut container = Self {
            services: HashMap::new(),
            config,
        };
        
        container.register_default_services();
        container
    }
    
    /// Register a service in the container
    pub fn register<T: 'static + Send + Sync>(&mut self, name: &str, service: T) {
        self.services.insert(name.to_string(), Arc::new(service));
    }
    
    /// Get a service from the container
    pub fn get<T: 'static + Send + Sync>(&self, name: &str) -> Option<Arc<T>> {
        self.services.get(name)?
            .clone()
            .downcast::<T>()
            .ok()
    }
    
    /// Register default services based on configuration
    fn register_default_services(&mut self) {
        if self.config.use_mocks {
            self.register("filesystem", MockFileSystem::new());
            self.register("compression", MockCompressionProvider::new());
            self.register("timer", MockTimeProvider::new());
        } else {
            self.register("filesystem", RealFileSystem::new());
            self.register("compression", RealCompressionProvider::new());
            self.register("timer", SystemTimeProvider::new());
        }
        
        if self.config.enable_logging {
            self.register("logger", TestLogger::new());
        }
    }
    
    /// Create a configured SSTable reader
    pub fn create_sstable_reader(&self) -> Result<cqlite_core::storage::sstable::reader::SSTableReader> {
        let fs = self.get::<dyn FileSystemTrait>("filesystem")
            .ok_or_else(|| anyhow::anyhow!("FileSystem not registered"))?;
        let compression = self.get::<dyn CompressionTrait>("compression")
            .ok_or_else(|| anyhow::anyhow!("Compression provider not registered"))?;
        
        Ok(cqlite_core::storage::sstable::reader::SSTableReader::new_with_deps(
            fs,
            compression,
        ))
    }
}

// Mock implementations for testing
pub struct MockFileSystem {
    files: Arc<RwLock<HashMap<std::path::PathBuf, Vec<u8>>>>,
}

impl MockFileSystem {
    pub fn new() -> Self {
        Self {
            files: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    pub async fn add_file(&self, path: std::path::PathBuf, content: Vec<u8>) {
        let mut files = self.files.write().await;
        files.insert(path, content);
    }
}

#[async_trait::async_trait]
impl FileSystemTrait for MockFileSystem {
    async fn read_file(&self, path: &std::path::Path) -> Result<Vec<u8>> {
        let files = self.files.read().await;
        files.get(path)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("File not found: {:?}", path))
    }
    
    async fn file_exists(&self, path: &std::path::Path) -> bool {
        let files = self.files.read().await;
        files.contains_key(path)
    }
    
    async fn file_size(&self, path: &std::path::Path) -> Result<u64> {
        let files = self.files.read().await;
        Ok(files.get(path)
            .map(|content| content.len() as u64)
            .unwrap_or(0))
    }
}

// Define traits for testable components
#[async_trait::async_trait]
pub trait FileSystemTrait: Send + Sync {
    async fn read_file(&self, path: &std::path::Path) -> Result<Vec<u8>>;
    async fn file_exists(&self, path: &std::path::Path) -> bool;
    async fn file_size(&self, path: &std::path::Path) -> Result<u64>;
}

#[async_trait::async_trait]
pub trait CompressionTrait: Send + Sync {
    async fn decompress(&self, data: &[u8], algorithm: CompressionAlgorithm) -> Result<Vec<u8>>;
    async fn compress(&self, data: &[u8], algorithm: CompressionAlgorithm) -> Result<Vec<u8>>;
}

pub trait TimeProviderTrait: Send + Sync {
    fn now(&self) -> std::time::SystemTime;
    fn elapsed_since(&self, start: std::time::SystemTime) -> std::time::Duration;
}

#[derive(Clone, Copy)]
pub enum CompressionAlgorithm {
    LZ4,
    Snappy,
    ZSTD,
}

// Real implementations
pub struct RealFileSystem;
pub struct RealCompressionProvider;
pub struct SystemTimeProvider;
pub struct MockCompressionProvider;
pub struct MockTimeProvider;
pub struct TestLogger;

// Implementation details would continue...
```

### 1.2 Async Test Executor

Create `/tests/infrastructure/async_executor.rs`:

```rust
//! Async Test Execution Framework
//! Handles timeout, parallel execution, and resource management

use std::time::Duration;
use std::collections::HashMap;
use tokio::process::Command;
use anyhow::Result;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone)]
pub struct AsyncTestExecutor {
    config: ExecutorConfig,
    runtime_handle: tokio::runtime::Handle,
}

#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    pub default_timeout: Duration,
    pub max_parallel_tests: usize,
    pub cli_binary_path: String,
    pub enable_tracing: bool,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            default_timeout: Duration::from_secs(30),
            max_parallel_tests: 4,
            cli_binary_path: "target/debug/cqlite".to_string(),
            enable_tracing: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CliCommand {
    pub args: Vec<String>,
    pub env: HashMap<String, String>,
    pub working_dir: Option<String>,
    pub stdin: Option<String>,
    pub timeout: Option<Duration>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandOutput {
    pub status_code: i32,
    pub stdout: String,
    pub stderr: String,
    pub execution_time: Duration,
    pub memory_peak_kb: Option<u64>,
}

impl AsyncTestExecutor {
    pub fn new(config: ExecutorConfig) -> Self {
        Self {
            config,
            runtime_handle: tokio::runtime::Handle::current(),
        }
    }
    
    /// Execute a single CLI command with timeout
    pub async fn execute_command(&self, command: CliCommand) -> Result<CommandOutput> {
        let timeout = command.timeout.unwrap_or(self.config.default_timeout);
        
        let execution_future = self.execute_command_inner(command);
        
        match tokio::time::timeout(timeout, execution_future).await {
            Ok(result) => result,
            Err(_) => Err(anyhow::anyhow!("Command timed out after {:?}", timeout)),
        }
    }
    
    async fn execute_command_inner(&self, command: CliCommand) -> Result<CommandOutput> {
        let start_time = std::time::Instant::now();
        
        let mut cmd = Command::new(&self.config.cli_binary_path);
        cmd.args(&command.args);
        
        // Set environment variables
        for (key, value) in &command.env {
            cmd.env(key, value);
        }
        
        // Set working directory
        if let Some(working_dir) = &command.working_dir {
            cmd.current_dir(working_dir);
        }
        
        // Configure stdio
        cmd.stdin(std::process::Stdio::piped());
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());
        
        let mut child = cmd.spawn()?;
        
        // Write stdin if provided
        if let Some(stdin_data) = &command.stdin {
            if let Some(stdin) = child.stdin.take() {
                use tokio::io::AsyncWriteExt;
                let mut stdin = stdin;
                stdin.write_all(stdin_data.as_bytes()).await?;
                stdin.shutdown().await?;
            }
        }
        
        let output = child.wait_with_output().await?;
        let execution_time = start_time.elapsed();
        
        Ok(CommandOutput {
            status_code: output.status.code().unwrap_or(-1),
            stdout: String::from_utf8_lossy(&output.stdout).to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
            execution_time,
            memory_peak_kb: None, // Could be implemented with process monitoring
        })
    }
    
    /// Execute multiple commands in parallel
    pub async fn execute_parallel(&self, commands: Vec<CliCommand>) -> Vec<Result<CommandOutput>> {
        use futures::stream::{FuturesUnordered, StreamExt};
        
        let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(self.config.max_parallel_tests));
        let mut futures = FuturesUnordered::new();
        
        for command in commands {
            let semaphore = semaphore.clone();
            let executor = self.clone();
            
            futures.push(async move {
                let _permit = semaphore.acquire().await.unwrap();
                executor.execute_command(command).await
            });
        }
        
        let mut results = Vec::new();
        while let Some(result) = futures.next().await {
            results.push(result);
        }
        
        results
    }
    
    /// Execute a test scenario (multiple related commands)
    pub async fn execute_scenario(&self, scenario: TestScenario) -> Result<ScenarioResult> {
        let mut command_results = Vec::new();
        let scenario_start = std::time::Instant::now();
        
        for command in scenario.commands {
            let result = self.execute_command(command.clone()).await;
            
            let success = result.as_ref().map(|r| r.status_code == 0).unwrap_or(false);
            command_results.push(result);
            
            // Stop scenario on first failure if configured
            if !success && scenario.stop_on_failure {
                break;
            }
        }
        
        Ok(ScenarioResult {
            scenario_name: scenario.name,
            total_time: scenario_start.elapsed(),
            command_results,
            success: command_results.iter().all(|r| r.as_ref().map(|cmd| cmd.status_code == 0).unwrap_or(false)),
        })
    }
}

#[derive(Debug, Clone)]
pub struct TestScenario {
    pub name: String,
    pub commands: Vec<CliCommand>,
    pub stop_on_failure: bool,
    pub setup_commands: Vec<CliCommand>,
    pub cleanup_commands: Vec<CliCommand>,
}

#[derive(Debug)]
pub struct ScenarioResult {
    pub scenario_name: String,
    pub total_time: Duration,
    pub command_results: Vec<Result<CommandOutput>>,
    pub success: bool,
}

// Helper functions for common CLI operations
impl CliCommand {
    pub fn new(args: Vec<String>) -> Self {
        Self {
            args,
            env: HashMap::new(),
            working_dir: None,
            stdin: None,
            timeout: None,
        }
    }
    
    pub fn with_env(mut self, key: &str, value: &str) -> Self {
        self.env.insert(key.to_string(), value.to_string());
        self
    }
    
    pub fn with_working_dir(mut self, dir: &str) -> Self {
        self.working_dir = Some(dir.to_string());
        self
    }
    
    pub fn with_stdin(mut self, input: &str) -> Self {
        self.stdin = Some(input.to_string());
        self
    }
    
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
    
    // Common CLI command builders
    pub fn query(database: &str, query: &str) -> Self {
        Self::new(vec![
            "--database".to_string(),
            database.to_string(),
            "query".to_string(),
            query.to_string(),
        ])
    }
    
    pub fn info(database: &str) -> Self {
        Self::new(vec![
            "--database".to_string(),
            database.to_string(),
            "info".to_string(),
        ])
    }
    
    pub fn export(database: &str, format: &str, output: &str) -> Self {
        Self::new(vec![
            "--database".to_string(),
            database.to_string(),
            "export".to_string(),
            "--format".to_string(),
            format.to_string(),
            "--output".to_string(),
            output.to_string(),
        ])
    }
    
    pub fn repl(database: &str) -> Self {
        Self::new(vec![
            "--database".to_string(),
            database.to_string(),
            "repl".to_string(),
        ])
    }
}
```

### 1.3 Test Data Management

Create `/tests/infrastructure/test_data_manager.rs`:

```rust
//! Test Data Management System
//! Handles fixtures, generators, and real data sets

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use anyhow::Result;
use serde::{Serialize, Deserialize};
use tokio::sync::RwLock;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct TestDataKey {
    pub data_type: TestDataType,
    pub size: DataSize,
    pub complexity: Complexity,
    pub format_version: FormatVersion,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum TestDataType {
    SyntheticSSTable,
    RealCassandraData { version: String, dataset: String },
    EdgeCaseData { scenario: EdgeCaseType },
    PerformanceData { workload: WorkloadType },
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum DataSize {
    Tiny,     // < 1MB
    Small,    // 1-10MB
    Medium,   // 10-100MB  
    Large,    // 100MB-1GB
    XLarge,   // > 1GB
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum Complexity {
    Simple,      // Basic types only
    Moderate,    // Collections, UDTs
    Complex,     // Nested collections, complex UDTs
    Extreme,     // Edge cases, deeply nested
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum FormatVersion {
    Cassandra3x,
    Cassandra40,
    Cassandra41,
    Cassandra50,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum EdgeCaseType {
    CorruptedData,
    TruncatedFiles,
    InvalidHeaders,
    ExtremeValues,
    UnicodeEdgeCases,
    CompressionFailures,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum WorkloadType {
    ReadHeavy,
    WriteHeavy,
    Mixed,
    AnalyticalQueries,
    TransactionalQueries,
}

pub struct TestDataManager {
    cache: Arc<RwLock<HashMap<TestDataKey, Arc<TestDataSet>>>>,
    generators: HashMap<TestDataType, Box<dyn TestDataGenerator + Send + Sync>>,
    base_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct TestDataSet {
    pub key: TestDataKey,
    pub files: Vec<TestDataFile>,
    pub metadata: TestDataMetadata,
    pub verification_hash: String,
}

#[derive(Debug, Clone)]
pub struct TestDataFile {
    pub path: PathBuf,
    pub file_type: FileType,
    pub size_bytes: u64,
    pub content_hash: String,
}

#[derive(Debug, Clone)]
pub enum FileType {
    SSTableData,
    SSTableIndex,
    SSTableStatistics,
    SSTableFilter,
    SSTableTOC,
    Schema,
    Other(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestDataMetadata {
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub generator_version: String,
    pub schema_info: SchemaInfo,
    pub row_count: Option<u64>,
    pub expected_errors: Vec<String>,
    pub performance_baseline: Option<PerformanceBaseline>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaInfo {
    pub keyspace: String,
    pub table: String,
    pub partition_keys: Vec<ColumnInfo>,
    pub clustering_keys: Vec<ColumnInfo>,
    pub regular_columns: Vec<ColumnInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceBaseline {
    pub expected_parse_time_ms: u64,
    pub expected_memory_mb: u64,
    pub expected_throughput_rows_per_sec: u64,
}

impl TestDataManager {
    pub fn new(base_path: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&base_path)?;
        
        let mut generators: HashMap<TestDataType, Box<dyn TestDataGenerator + Send + Sync>> = HashMap::new();
        
        // Register default generators
        generators.insert(
            TestDataType::SyntheticSSTable,
            Box::new(SyntheticSSTableGenerator::new()),
        );
        
        Ok(Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
            generators,
            base_path,
        })
    }
    
    /// Get or generate test data
    pub async fn get_test_data(&self, key: TestDataKey) -> Result<Arc<TestDataSet>> {
        // Check cache first
        {
            let cache = self.cache.read().await;
            if let Some(cached) = cache.get(&key) {
                // Verify data still exists on disk
                if self.verify_data_exists(&cached).await {
                    return Ok(cached.clone());
                }
            }
        }
        
        // Generate new data
        let data_set = self.generate_test_data(&key).await?;
        let arc_data = Arc::new(data_set);
        
        // Cache the result
        {
            let mut cache = self.cache.write().await;
            cache.insert(key, arc_data.clone());
        }
        
        Ok(arc_data)
    }
    
    async fn generate_test_data(&self, key: &TestDataKey) -> Result<TestDataSet> {
        let generator = self.generators.get(&key.data_type)
            .ok_or_else(|| anyhow::anyhow!("No generator for data type: {:?}", key.data_type))?;
        
        let output_dir = self.base_path.join(format!("data_{}", self.generate_key_hash(key)));
        std::fs::create_dir_all(&output_dir)?;
        
        generator.generate(key, &output_dir).await
    }
    
    async fn verify_data_exists(&self, data_set: &TestDataSet) -> bool {
        for file in &data_set.files {
            if !tokio::fs::metadata(&file.path).await.is_ok() {
                return false;
            }
        }
        true
    }
    
    fn generate_key_hash(&self, key: &TestDataKey) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        format!("{:x}", hasher.finish())
    }
    
    /// Clear cache and remove generated data
    pub async fn cleanup(&self) -> Result<()> {
        let mut cache = self.cache.write().await;
        cache.clear();
        
        if self.base_path.exists() {
            tokio::fs::remove_dir_all(&self.base_path).await?;
        }
        
        Ok(())
    }
    
    /// Generate a complete test suite data set
    pub async fn generate_test_suite_data(&self, suite_name: &str) -> Result<TestSuiteDataSet> {
        let suite_keys = self.get_standard_test_keys();
        let mut data_sets = Vec::new();
        
        for key in suite_keys {
            let data_set = self.get_test_data(key).await?;
            data_sets.push(data_set);
        }
        
        Ok(TestSuiteDataSet {
            name: suite_name.to_string(),
            data_sets,
            created_at: chrono::Utc::now(),
        })
    }
    
    fn get_standard_test_keys(&self) -> Vec<TestDataKey> {
        vec![
            // Basic synthetic data
            TestDataKey {
                data_type: TestDataType::SyntheticSSTable,
                size: DataSize::Small,
                complexity: Complexity::Simple,
                format_version: FormatVersion::Cassandra50,
            },
            TestDataKey {
                data_type: TestDataType::SyntheticSSTable,
                size: DataSize::Medium,
                complexity: Complexity::Moderate,
                format_version: FormatVersion::Cassandra50,
            },
            // Edge cases
            TestDataKey {
                data_type: TestDataType::EdgeCaseData { scenario: EdgeCaseType::CorruptedData },
                size: DataSize::Small,
                complexity: Complexity::Simple,
                format_version: FormatVersion::Cassandra50,
            },
            TestDataKey {
                data_type: TestDataType::EdgeCaseData { scenario: EdgeCaseType::TruncatedFiles },
                size: DataSize::Small,
                complexity: Complexity::Simple,
                format_version: FormatVersion::Cassandra50,
            },
        ]
    }
}

pub struct TestSuiteDataSet {
    pub name: String,
    pub data_sets: Vec<Arc<TestDataSet>>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[async_trait::async_trait]
pub trait TestDataGenerator: Send + Sync {
    async fn generate(&self, key: &TestDataKey, output_dir: &Path) -> Result<TestDataSet>;
}

pub struct SyntheticSSTableGenerator {
    config: GeneratorConfig,
}

#[derive(Debug, Clone)]
pub struct GeneratorConfig {
    pub include_bloom_filter: bool,
    pub include_statistics: bool,
    pub compression_enabled: bool,
}

impl Default for GeneratorConfig {
    fn default() -> Self {
        Self {
            include_bloom_filter: true,
            include_statistics: true,
            compression_enabled: true,
        }
    }
}

impl SyntheticSSTableGenerator {
    pub fn new() -> Self {
        Self {
            config: GeneratorConfig::default(),
        }
    }
}

#[async_trait::async_trait]
impl TestDataGenerator for SyntheticSSTableGenerator {
    async fn generate(&self, key: &TestDataKey, output_dir: &Path) -> Result<TestDataSet> {
        let row_count = match key.size {
            DataSize::Tiny => 100,
            DataSize::Small => 1_000,
            DataSize::Medium => 10_000,
            DataSize::Large => 100_000,
            DataSize::XLarge => 1_000_000,
        };
        
        let schema = self.generate_schema(key)?;
        let files = self.generate_sstable_files(output_dir, &schema, row_count).await?;
        
        let metadata = TestDataMetadata {
            created_at: chrono::Utc::now(),
            generator_version: "1.0.0".to_string(),
            schema_info: schema,
            row_count: Some(row_count),
            expected_errors: Vec::new(),
            performance_baseline: Some(PerformanceBaseline {
                expected_parse_time_ms: row_count / 1000,
                expected_memory_mb: (row_count * 100) / (1024 * 1024),
                expected_throughput_rows_per_sec: 10_000,
            }),
        };
        
        Ok(TestDataSet {
            key: key.clone(),
            files,
            metadata,
            verification_hash: "synthetic".to_string(),
        })
    }
}

impl SyntheticSSTableGenerator {
    fn generate_schema(&self, key: &TestDataKey) -> Result<SchemaInfo> {
        let schema = match key.complexity {
            Complexity::Simple => SchemaInfo {
                keyspace: "test_ks".to_string(),
                table: "simple_table".to_string(),
                partition_keys: vec![
                    ColumnInfo {
                        name: "id".to_string(),
                        data_type: "uuid".to_string(),
                        nullable: false,
                    },
                ],
                clustering_keys: vec![],
                regular_columns: vec![
                    ColumnInfo {
                        name: "name".to_string(),
                        data_type: "text".to_string(),
                        nullable: true,
                    },
                    ColumnInfo {
                        name: "age".to_string(),
                        data_type: "int".to_string(),
                        nullable: true,
                    },
                ],
            },
            Complexity::Moderate => SchemaInfo {
                keyspace: "test_ks".to_string(),
                table: "moderate_table".to_string(),
                partition_keys: vec![
                    ColumnInfo {
                        name: "id".to_string(),
                        data_type: "uuid".to_string(),
                        nullable: false,
                    },
                ],
                clustering_keys: vec![
                    ColumnInfo {
                        name: "timestamp".to_string(),
                        data_type: "timestamp".to_string(),
                        nullable: false,
                    },
                ],
                regular_columns: vec![
                    ColumnInfo {
                        name: "tags".to_string(),
                        data_type: "set<text>".to_string(),
                        nullable: true,
                    },
                    ColumnInfo {
                        name: "metadata".to_string(),
                        data_type: "map<text, text>".to_string(),
                        nullable: true,
                    },
                ],
            },
            _ => return Err(anyhow::anyhow!("Complex schemas not yet implemented")),
        };
        
        Ok(schema)
    }
    
    async fn generate_sstable_files(&self, output_dir: &Path, schema: &SchemaInfo, row_count: u64) -> Result<Vec<TestDataFile>> {
        let mut files = Vec::new();
        
        // Generate Data.db file
        let data_file = self.generate_data_file(output_dir, schema, row_count).await?;
        files.push(data_file);
        
        // Generate Index.db file
        let index_file = self.generate_index_file(output_dir).await?;
        files.push(index_file);
        
        // Generate Statistics.db file
        if self.config.include_statistics {
            let stats_file = self.generate_statistics_file(output_dir, row_count).await?;
            files.push(stats_file);
        }
        
        // Generate Filter.db file
        if self.config.include_bloom_filter {
            let filter_file = self.generate_filter_file(output_dir).await?;
            files.push(filter_file);
        }
        
        // Generate TOC.txt file
        let toc_file = self.generate_toc_file(output_dir, &files).await?;
        files.push(toc_file);
        
        Ok(files)
    }
    
    async fn generate_data_file(&self, output_dir: &Path, schema: &SchemaInfo, row_count: u64) -> Result<TestDataFile> {
        let file_path = output_dir.join("nb-1-big-Data.db");
        
        // Generate synthetic SSTable data based on schema
        let data = self.create_synthetic_sstable_data(schema, row_count)?;
        
        tokio::fs::write(&file_path, &data).await?;
        
        Ok(TestDataFile {
            path: file_path,
            file_type: FileType::SSTableData,
            size_bytes: data.len() as u64,
            content_hash: self.calculate_hash(&data),
        })
    }
    
    async fn generate_index_file(&self, output_dir: &Path) -> Result<TestDataFile> {
        let file_path = output_dir.join("nb-1-big-Index.db");
        let data = b"synthetic index data"; // Simplified for example
        
        tokio::fs::write(&file_path, data).await?;
        
        Ok(TestDataFile {
            path: file_path,
            file_type: FileType::SSTableIndex,
            size_bytes: data.len() as u64,
            content_hash: self.calculate_hash(data),
        })
    }
    
    async fn generate_statistics_file(&self, output_dir: &Path, row_count: u64) -> Result<TestDataFile> {
        let file_path = output_dir.join("nb-1-big-Statistics.db");
        
        // Generate realistic statistics
        let stats = self.create_statistics_data(row_count)?;
        
        tokio::fs::write(&file_path, &stats).await?;
        
        Ok(TestDataFile {
            path: file_path,
            file_type: FileType::SSTableStatistics,
            size_bytes: stats.len() as u64,
            content_hash: self.calculate_hash(&stats),
        })
    }
    
    async fn generate_filter_file(&self, output_dir: &Path) -> Result<TestDataFile> {
        let file_path = output_dir.join("nb-1-big-Filter.db");
        let data = b"synthetic bloom filter data"; // Simplified for example
        
        tokio::fs::write(&file_path, data).await?;
        
        Ok(TestDataFile {
            path: file_path,
            file_type: FileType::SSTableFilter,
            size_bytes: data.len() as u64,
            content_hash: self.calculate_hash(data),
        })
    }
    
    async fn generate_toc_file(&self, output_dir: &Path, files: &[TestDataFile]) -> Result<TestDataFile> {
        let file_path = output_dir.join("nb-1-big-TOC.txt");
        
        let mut toc_content = String::new();
        for file in files {
            if let Some(filename) = file.path.file_name() {
                toc_content.push_str(&filename.to_string_lossy());
                toc_content.push('\n');
            }
        }
        
        tokio::fs::write(&file_path, &toc_content).await?;
        
        Ok(TestDataFile {
            path: file_path,
            file_type: FileType::SSTableTOC,
            size_bytes: toc_content.len() as u64,
            content_hash: self.calculate_hash(toc_content.as_bytes()),
        })
    }
    
    fn create_synthetic_sstable_data(&self, schema: &SchemaInfo, row_count: u64) -> Result<Vec<u8>> {
        // This would contain the actual SSTable format generation logic
        // For now, return a simple placeholder
        let mut data = Vec::new();
        
        // SSTable header
        data.extend_from_slice(b"CASSANDRA_SSTABLE_5.0");
        
        // Simplified row data generation
        for i in 0..row_count {
            let row_data = format!("row_{}_data", i);
            data.extend_from_slice(row_data.as_bytes());
        }
        
        Ok(data)
    }
    
    fn create_statistics_data(&self, row_count: u64) -> Result<Vec<u8>> {
        // Generate realistic statistics data
        let stats = format!(
            "estimated_row_size_histogram: [{}]\nestimated_column_count_histogram: [{}]\nrow_count: {}\n",
            "100,200,300,400,500",
            "1,2,3,4,5",
            row_count
        );
        
        Ok(stats.into_bytes())
    }
    
    fn calculate_hash(&self, data: &[u8]) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        data.hash(&mut hasher);
        format!("{:x}", hasher.finish())
    }
}
```

This implementation guide provides the foundational infrastructure for the testing architecture. The next sections would cover specific test implementations, performance benchmarking, and CI/CD integration patterns.

Key features implemented:

1. **Dependency Injection**: TestContainer with mock/real implementations
2. **Async Execution**: Timeout handling, parallel execution, CLI command abstraction
3. **Test Data Management**: Generators, caching, fixture management
4. **Type Safety**: Strong typing for test data classification
5. **Extensibility**: Plugin architecture for generators and custom data types

The architecture supports both synthetic test data generation and real Cassandra data handling, with proper cleanup and resource management.