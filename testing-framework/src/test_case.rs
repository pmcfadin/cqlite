//! Test case definitions and management

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use uuid::Uuid;

/// Represents a single test case
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestCase {
    /// Unique identifier for the test case
    pub id: Uuid,
    
    /// Human-readable name for the test case
    pub name: String,
    
    /// Description of what this test case validates
    pub description: String,
    
    /// The CQL query to execute
    pub query: String,
    
    /// Expected result type
    pub expected_result: ExpectedResult,
    
    /// Setup queries to run before the main query
    pub setup_queries: Vec<String>,
    
    /// Cleanup queries to run after the test
    pub cleanup_queries: Vec<String>,
    
    /// Test case category
    pub category: TestCategory,
    
    /// Priority level
    pub priority: TestPriority,
    
    /// Custom metadata for the test case
    pub metadata: HashMap<String, String>,
    
    /// Whether this test is expected to fail
    pub should_fail: bool,
    
    /// Timeout for this specific test case
    pub timeout_seconds: Option<u64>,
    
    /// Tags for grouping and filtering
    pub tags: Vec<String>,
}

/// Expected result types for test cases
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExpectedResult {
    /// Expect successful execution with data
    Success {
        /// Expected number of rows (None for any number)
        row_count: Option<usize>,
        /// Expected column names
        columns: Option<Vec<String>>,
    },
    /// Expect an error
    Error {
        /// Expected error message pattern
        error_pattern: Option<String>,
    },
    /// Expect empty result set
    Empty,
    /// Custom validation logic identifier
    Custom(String),
}

/// Test case categories
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TestCategory {
    /// Basic DDL operations (CREATE, ALTER, DROP)
    DDL,
    /// Data manipulation (INSERT, UPDATE, DELETE)
    DML,
    /// Query operations (SELECT)
    Query,
    /// Schema operations
    Schema,
    /// Index operations
    Index,
    /// Type system tests
    Types,
    /// Collection operations
    Collections,
    /// User-defined functions
    UDF,
    /// Materialized views
    MaterializedViews,
    /// Secondary indexes
    SecondaryIndexes,
    /// Batch operations
    Batch,
    /// Transaction-like operations
    Atomic,
    /// Performance tests
    Performance,
    /// Edge cases and error conditions
    EdgeCases,
    /// Custom category
    Custom(String),
}

/// Test priority levels
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum TestPriority {
    /// Low priority - nice to have
    Low,
    /// Medium priority - important for functionality
    Medium,
    /// High priority - critical for basic operations
    High,
    /// Critical priority - must pass for release
    Critical,
}

impl TestCase {
    /// Create a new test case
    pub fn new(
        name: String,
        description: String,
        query: String,
        expected_result: ExpectedResult,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            name,
            description,
            query,
            expected_result,
            setup_queries: Vec::new(),
            cleanup_queries: Vec::new(),
            category: TestCategory::Query,
            priority: TestPriority::Medium,
            metadata: HashMap::new(),
            should_fail: false,
            timeout_seconds: None,
            tags: Vec::new(),
        }
    }

    /// Create a simple success test case
    pub fn simple_success(name: &str, query: &str) -> Self {
        Self::new(
            name.to_string(),
            format!("Test case: {}", name),
            query.to_string(),
            ExpectedResult::Success {
                row_count: None,
                columns: None,
            },
        )
    }

    /// Create a simple error test case
    pub fn simple_error(name: &str, query: &str, error_pattern: Option<&str>) -> Self {
        Self::new(
            name.to_string(),
            format!("Error test case: {}", name),
            query.to_string(),
            ExpectedResult::Error {
                error_pattern: error_pattern.map(|s| s.to_string()),
            },
        )
    }

    /// Add setup query
    pub fn with_setup(mut self, setup_query: String) -> Self {
        self.setup_queries.push(setup_query);
        self
    }

    /// Add cleanup query
    pub fn with_cleanup(mut self, cleanup_query: String) -> Self {
        self.cleanup_queries.push(cleanup_query);
        self
    }

    /// Set category
    pub fn with_category(mut self, category: TestCategory) -> Self {
        self.category = category;
        self
    }

    /// Set priority
    pub fn with_priority(mut self, priority: TestPriority) -> Self {
        self.priority = priority;
        self
    }

    /// Add tag
    pub fn with_tag(mut self, tag: String) -> Self {
        self.tags.push(tag);
        self
    }

    /// Add metadata
    pub fn with_metadata(mut self, key: String, value: String) -> Self {
        self.metadata.insert(key, value);
        self
    }

    /// Set timeout
    pub fn with_timeout(mut self, timeout_seconds: u64) -> Self {
        self.timeout_seconds = Some(timeout_seconds);
        self
    }

    /// Mark as should fail
    pub fn should_fail(mut self) -> Self {
        self.should_fail = true;
        self
    }

    /// Check if test case matches filter criteria
    pub fn matches_filter(&self, filter: &TestFilter) -> bool {
        // Category filter
        if let Some(ref categories) = filter.categories {
            if !categories.contains(&self.category) {
                return false;
            }
        }

        // Priority filter
        if let Some(min_priority) = filter.min_priority {
            if self.priority < min_priority {
                return false;
            }
        }

        // Tag filter
        if let Some(ref required_tags) = filter.required_tags {
            if !required_tags.iter().all(|tag| self.tags.contains(tag)) {
                return false;
            }
        }

        // Name pattern filter
        if let Some(ref pattern) = filter.name_pattern {
            if !self.name.contains(pattern) {
                return false;
            }
        }

        true
    }

    /// Get timeout for this test case
    pub fn get_timeout(&self, default_timeout: u64) -> u64 {
        self.timeout_seconds.unwrap_or(default_timeout)
    }
}

/// Filter for selecting test cases
#[derive(Debug, Clone, Default)]
pub struct TestFilter {
    /// Only include these categories
    pub categories: Option<Vec<TestCategory>>,
    /// Minimum priority level
    pub min_priority: Option<TestPriority>,
    /// Required tags (all must be present)
    pub required_tags: Option<Vec<String>>,
    /// Name must contain this pattern
    pub name_pattern: Option<String>,
}

/// Test case loader for loading test cases from files
pub struct TestCaseLoader;

impl TestCaseLoader {
    /// Load test cases from a directory
    pub fn load_from_directory(dir: &PathBuf) -> anyhow::Result<Vec<TestCase>> {
        let mut test_cases = Vec::new();
        
        if !dir.exists() {
            return Ok(test_cases);
        }

        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            
            if path.is_file() {
                if let Some(extension) = path.extension() {
                    match extension.to_str() {
                        Some("json") => {
                            let mut cases = Self::load_from_json(&path)?;
                            test_cases.append(&mut cases);
                        }
                        Some("cql") => {
                            let mut cases = Self::load_from_cql(&path)?;
                            test_cases.append(&mut cases);
                        }
                        _ => {} // Ignore other file types
                    }
                }
            }
        }

        Ok(test_cases)
    }

    /// Load test cases from JSON file
    pub fn load_from_json(path: &PathBuf) -> anyhow::Result<Vec<TestCase>> {
        let content = std::fs::read_to_string(path)?;
        let test_cases: Vec<TestCase> = serde_json::from_str(&content)?;
        Ok(test_cases)
    }

    /// Load test cases from CQL file (simple format)
    pub fn load_from_cql(path: &PathBuf) -> anyhow::Result<Vec<TestCase>> {
        let content = std::fs::read_to_string(path)?;
        let mut test_cases = Vec::new();
        
        let file_name = path.file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown");

        // Simple parser: split by semicolons and create test cases
        let queries: Vec<&str> = content
            .split(';')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty() && !s.starts_with("--") && !s.starts_with("//"))
            .collect();

        for (i, query) in queries.iter().enumerate() {
            let test_case = TestCase::simple_success(
                &format!("{}_query_{}", file_name, i + 1),
                query,
            );
            test_cases.push(test_case);
        }

        Ok(test_cases)
    }

    /// Create default test cases for basic functionality
    pub fn create_default_test_cases() -> Vec<TestCase> {
        vec![
            TestCase::simple_success(
                "create_keyspace",
                "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}"
            ).with_category(TestCategory::DDL)
             .with_priority(TestPriority::High),
             
            TestCase::simple_success(
                "use_keyspace",
                "USE test"
            ).with_category(TestCategory::DDL)
             .with_priority(TestPriority::High),
             
            TestCase::simple_success(
                "create_table",
                "CREATE TABLE IF NOT EXISTS test.users (id UUID PRIMARY KEY, name TEXT, age INT)"
            ).with_category(TestCategory::DDL)
             .with_priority(TestPriority::High),
             
            TestCase::simple_success(
                "insert_data",
                "INSERT INTO test.users (id, name, age) VALUES (uuid(), 'John Doe', 25)"
            ).with_category(TestCategory::DML)
             .with_priority(TestPriority::High),
             
            TestCase::simple_success(
                "select_data",
                "SELECT * FROM test.users"
            ).with_category(TestCategory::Query)
             .with_priority(TestPriority::High),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_test_case_creation() {
        let test_case = TestCase::simple_success("test_select", "SELECT * FROM users");
        assert_eq!(test_case.name, "test_select");
        assert_eq!(test_case.query, "SELECT * FROM users");
        assert_eq!(test_case.category, TestCategory::Query);
        assert_eq!(test_case.priority, TestPriority::Medium);
    }

    #[test]
    fn test_test_case_builder() {
        let test_case = TestCase::simple_success("test", "SELECT 1")
            .with_category(TestCategory::DDL)
            .with_priority(TestPriority::High)
            .with_tag("basic".to_string())
            .with_timeout(30);
        
        assert_eq!(test_case.category, TestCategory::DDL);
        assert_eq!(test_case.priority, TestPriority::High);
        assert!(test_case.tags.contains(&"basic".to_string()));
        assert_eq!(test_case.timeout_seconds, Some(30));
    }

    #[test]
    fn test_filter_matching() {
        let test_case = TestCase::simple_success("ddl_test", "CREATE TABLE test (id INT)")
            .with_category(TestCategory::DDL)
            .with_priority(TestPriority::High)
            .with_tag("create".to_string());

        let filter = TestFilter {
            categories: Some(vec![TestCategory::DDL]),
            min_priority: Some(TestPriority::Medium),
            required_tags: Some(vec!["create".to_string()]),
            name_pattern: Some("ddl".to_string()),
        };

        assert!(test_case.matches_filter(&filter));
    }

    #[test]
    fn test_default_test_cases() {
        let test_cases = TestCaseLoader::create_default_test_cases();
        assert!(!test_cases.is_empty());
        assert!(test_cases.iter().any(|tc| tc.name == "create_keyspace"));
        assert!(test_cases.iter().any(|tc| tc.name == "select_data"));
    }
}