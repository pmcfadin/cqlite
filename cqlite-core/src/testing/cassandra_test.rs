/// Cassandra testing utilities using Docker cqlsh integration
use crate::docker::{DockerCqlshClient, DockerContainer, CqlshOutput};
use std::io;

/// Cassandra test runner that uses Docker containers
pub struct CassandraTestRunner {
    client: DockerCqlshClient,
}

impl CassandraTestRunner {
    /// Create a new test runner with the running Cassandra container
    pub fn new() -> io::Result<Self> {
        let container = DockerCqlshClient::find_cassandra_container()?;
        let client = DockerCqlshClient::new(container);
        Ok(Self { client })
    }

    /// Initialize the test environment
    pub fn setup(&self) -> io::Result<()> {
        // Wait for Cassandra to be ready
        self.client.wait_until_ready(30)?;
        
        // Create test keyspace
        let create_keyspace = r#"
            CREATE KEYSPACE IF NOT EXISTS cqlite_test 
            WITH REPLICATION = {
                'class': 'SimpleStrategy',
                'replication_factor': 1
            };
        "#;
        
        self.client.execute_cql(create_keyspace)?;
        
        Ok(())
    }

    /// Execute a CQL query and return parsed results
    pub fn execute_query(&self, query: &str) -> io::Result<CqlshOutput> {
        let output = self.client.execute_cql(query)?;
        Ok(DockerCqlshClient::parse_cqlsh_output(&output))
    }

    /// Execute a CQL query in the test keyspace
    pub fn execute_test_query(&self, query: &str) -> io::Result<CqlshOutput> {
        let use_keyspace = "USE cqlite_test;";
        let combined_query = format!("{}\n{}", use_keyspace, query);
        self.execute_query(&combined_query)
    }

    /// Create a test table with sample data
    pub fn create_test_table(&self, table_name: &str) -> io::Result<()> {
        let create_table = format!(r#"
            USE cqlite_test;
            CREATE TABLE IF NOT EXISTS {} (
                id UUID PRIMARY KEY,
                name TEXT,
                email TEXT,
                age INT,
                created_at TIMESTAMP
            );
        "#, table_name);

        self.client.execute_cql(&create_table)?;

        // Insert sample data
        let insert_data = format!(r#"
            USE cqlite_test;
            INSERT INTO {} (id, name, email, age, created_at) 
            VALUES (uuid(), 'Alice Johnson', 'alice@example.com', 30, toTimestamp(now()));
            
            INSERT INTO {} (id, name, email, age, created_at) 
            VALUES (uuid(), 'Bob Smith', 'bob@example.com', 25, toTimestamp(now()));
            
            INSERT INTO {} (id, name, email, age, created_at) 
            VALUES (uuid(), 'Charlie Brown', 'charlie@example.com', 35, toTimestamp(now()));
            
            INSERT INTO {} (id, name, email, age, created_at) 
            VALUES (uuid(), 'Diana Prince', 'diana@example.com', 28, toTimestamp(now()));
        "#, table_name, table_name, table_name, table_name);

        self.client.execute_cql(&insert_data)?;
        Ok(())
    }

    /// Clean up test data
    pub fn cleanup(&self) -> io::Result<()> {
        let drop_keyspace = "DROP KEYSPACE IF EXISTS cqlite_test;";
        self.client.execute_cql(drop_keyspace)?;
        Ok(())
    }

    /// Compare query results between CQLite and Cassandra
    pub fn compare_results(&self, query: &str, cqlite_result: &str) -> io::Result<ComparisonResult> {
        let cassandra_output = self.execute_test_query(query)?;
        
        // Parse CQLite result (assuming it's in a similar format)
        let cqlite_output = DockerCqlshClient::parse_cqlsh_output(cqlite_result);
        
        let matches = compare_outputs(&cassandra_output, &cqlite_output);
        
        Ok(ComparisonResult {
            query: query.to_string(),
            cassandra_result: cassandra_output,
            cqlite_result: cqlite_output,
            matches,
        })
    }

    /// Run a comprehensive test suite
    pub fn run_test_suite(&self, test_queries: Vec<&str>) -> io::Result<TestSuiteResult> {
        let mut results = Vec::new();
        let mut passed = 0;
        let mut failed = 0;

        // Setup test environment
        self.setup()?;
        self.create_test_table("users")?;

        for query in test_queries {
            match self.execute_test_query(query) {
                Ok(output) => {
                    results.push(TestResult {
                        query: query.to_string(),
                        success: true,
                        output: Some(output),
                        error: None,
                    });
                    passed += 1;
                }
                Err(e) => {
                    results.push(TestResult {
                        query: query.to_string(),
                        success: false,
                        output: None,
                        error: Some(e.to_string()),
                    });
                    failed += 1;
                }
            }
        }

        // Cleanup
        let _ = self.cleanup();

        Ok(TestSuiteResult {
            total_tests: results.len(),
            passed,
            failed,
            results,
        })
    }
}

/// Result of comparing CQLite and Cassandra outputs
#[derive(Debug)]
pub struct ComparisonResult {
    pub query: String,
    pub cassandra_result: CqlshOutput,
    pub cqlite_result: CqlshOutput,
    pub matches: bool,
}

/// Result of a single test
#[derive(Debug)]
pub struct TestResult {
    pub query: String,
    pub success: bool,
    pub output: Option<CqlshOutput>,
    pub error: Option<String>,
}

/// Result of a test suite run
#[derive(Debug)]
pub struct TestSuiteResult {
    pub total_tests: usize,
    pub passed: usize,
    pub failed: usize,
    pub results: Vec<TestResult>,
}

/// Compare two CQL outputs for equality
fn compare_outputs(cassandra: &CqlshOutput, cqlite: &CqlshOutput) -> bool {
    // Compare headers
    if cassandra.headers != cqlite.headers {
        return false;
    }
    
    // Compare number of rows
    if cassandra.rows.len() != cqlite.rows.len() {
        return false;
    }
    
    // Compare each row (allowing for different ordering)
    let mut cassandra_rows = cassandra.rows.clone();
    let mut cqlite_rows = cqlite.rows.clone();
    
    cassandra_rows.sort();
    cqlite_rows.sort();
    
    cassandra_rows == cqlite_rows
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_comparison_result() {
        let cassandra = CqlshOutput {
            headers: vec!["id".to_string(), "name".to_string()],
            rows: vec![
                vec!["1".to_string(), "Alice".to_string()],
                vec!["2".to_string(), "Bob".to_string()],
            ],
            raw_output: "".to_string(),
        };

        let cqlite = CqlshOutput {
            headers: vec!["id".to_string(), "name".to_string()],
            rows: vec![
                vec!["2".to_string(), "Bob".to_string()],
                vec!["1".to_string(), "Alice".to_string()],
            ],
            raw_output: "".to_string(),
        };

        assert!(compare_outputs(&cassandra, &cqlite));
    }
}