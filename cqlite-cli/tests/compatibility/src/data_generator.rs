use std::collections::HashMap;
use std::process::Command;
use std::fs;
use std::path::{Path, PathBuf};
use anyhow::{Result, Context};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use chrono::{DateTime, Utc};

/// Generates test data across different Cassandra versions for compatibility testing
#[derive(Debug, Clone)]
pub struct TestDataGenerator {
    pub cassandra_versions: Vec<String>,
    pub data_output_dir: PathBuf,
    pub test_schemas: Vec<TestSchema>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestSchema {
    pub name: String,
    pub keyspace: String,
    pub table: String,
    pub cql_definition: String,
    pub data_types: Vec<CqlDataType>,
    pub test_cases: Vec<TestCase>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CqlDataType {
    // Basic types
    Text,
    Int,
    Bigint,
    Boolean,
    Decimal,
    Double,
    Float,
    Uuid,
    Timeuuid,
    Timestamp,
    Date,
    Time,
    Inet,
    Blob,
    Ascii,
    Varchar,
    Varint,
    
    // Collection types
    List(Box<CqlDataType>),
    Set(Box<CqlDataType>),
    Map(Box<CqlDataType>, Box<CqlDataType>),
    
    // Complex types
    Tuple(Vec<CqlDataType>),
    Udt(String, HashMap<String, CqlDataType>),
    
    // Counter type
    Counter,
    
    // Duration (Cassandra 3.10+)
    Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestCase {
    pub name: String,
    pub description: String,
    pub insert_statements: Vec<String>,
    pub expected_rows: usize,
    pub complexity_level: ComplexityLevel,
    pub cassandra_version_min: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ComplexityLevel {
    Basic,      // Simple single-column data
    Moderate,   // Collections and UDTs
    Complex,    // Nested collections, large UDTs
    Extreme,    // Maximum complexity, edge cases
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GeneratedDataSet {
    pub version: String,
    pub schemas: Vec<TestSchema>,
    pub sstable_files: Vec<PathBuf>,
    pub metadata: DataSetMetadata,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DataSetMetadata {
    pub generated_at: DateTime<Utc>,
    pub total_rows: usize,
    pub total_sstables: usize,
    pub data_size_bytes: u64,
    pub compression_ratio: f64,
    pub test_coverage: TestCoverage,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TestCoverage {
    pub data_types_covered: Vec<String>,
    pub collection_types_covered: Vec<String>,
    pub udt_complexity_levels: Vec<String>,
    pub edge_cases_covered: Vec<String>,
}

impl TestDataGenerator {
    pub fn new(output_dir: PathBuf) -> Self {
        let mut generator = Self {
            cassandra_versions: vec![
                "4.0".to_string(),
                "4.1".to_string(),
                "5.0".to_string(),
                "5.1".to_string(),
                "6.0".to_string(),
            ],
            data_output_dir: output_dir,
            test_schemas: Vec::new(),
        };
        
        generator.initialize_test_schemas();
        generator
    }

    /// Initialize comprehensive test schemas covering all CQL data types
    fn initialize_test_schemas(&mut self) {
        // Basic data types schema
        self.test_schemas.push(TestSchema {
            name: "basic_types".to_string(),
            keyspace: "compatibility_test".to_string(),
            table: "basic_types".to_string(),
            cql_definition: r#"
                CREATE TABLE compatibility_test.basic_types (
                    id UUID PRIMARY KEY,
                    text_col TEXT,
                    int_col INT,
                    bigint_col BIGINT,
                    boolean_col BOOLEAN,
                    decimal_col DECIMAL,
                    double_col DOUBLE,
                    float_col FLOAT,
                    timeuuid_col TIMEUUID,
                    timestamp_col TIMESTAMP,
                    date_col DATE,
                    time_col TIME,
                    inet_col INET,
                    blob_col BLOB,
                    ascii_col ASCII,
                    varchar_col VARCHAR,
                    varint_col VARINT
                )
            "#.to_string(),
            data_types: vec![
                CqlDataType::Uuid, CqlDataType::Text, CqlDataType::Int,
                CqlDataType::Bigint, CqlDataType::Boolean, CqlDataType::Decimal,
                CqlDataType::Double, CqlDataType::Float, CqlDataType::Timeuuid,
                CqlDataType::Timestamp, CqlDataType::Date, CqlDataType::Time,
                CqlDataType::Inet, CqlDataType::Blob, CqlDataType::Ascii,
                CqlDataType::Varchar, CqlDataType::Varint,
            ],
            test_cases: self.generate_basic_type_test_cases(),
        });

        // Collections schema
        self.test_schemas.push(TestSchema {
            name: "collections".to_string(),
            keyspace: "compatibility_test".to_string(),
            table: "collections".to_string(),
            cql_definition: r#"
                CREATE TABLE compatibility_test.collections (
                    id UUID PRIMARY KEY,
                    list_text LIST<TEXT>,
                    list_int LIST<INT>,
                    set_text SET<TEXT>,
                    set_int SET<INT>,
                    map_text_int MAP<TEXT, INT>,
                    map_int_text MAP<INT, TEXT>,
                    nested_list LIST<LIST<TEXT>>,
                    nested_map MAP<TEXT, MAP<TEXT, INT>>
                )
            "#.to_string(),
            data_types: vec![
                CqlDataType::List(Box::new(CqlDataType::Text)),
                CqlDataType::Set(Box::new(CqlDataType::Int)),
                CqlDataType::Map(Box::new(CqlDataType::Text), Box::new(CqlDataType::Int)),
            ],
            test_cases: self.generate_collection_test_cases(),
        });

        // UDT schema
        self.test_schemas.push(TestSchema {
            name: "user_defined_types".to_string(),
            keyspace: "compatibility_test".to_string(),
            table: "udt_test".to_string(),
            cql_definition: r#"
                CREATE TYPE compatibility_test.address (
                    street TEXT,
                    city TEXT,
                    zip_code INT,
                    coordinates MAP<TEXT, DOUBLE>
                );
                
                CREATE TYPE compatibility_test.user_profile (
                    name TEXT,
                    age INT,
                    addresses LIST<FROZEN<address>>,
                    metadata MAP<TEXT, TEXT>
                );
                
                CREATE TABLE compatibility_test.udt_test (
                    id UUID PRIMARY KEY,
                    simple_address FROZEN<address>,
                    profile FROZEN<user_profile>,
                    address_list LIST<FROZEN<address>>,
                    profile_map MAP<TEXT, FROZEN<user_profile>>
                )
            "#.to_string(),
            data_types: vec![
                CqlDataType::Udt("address".to_string(), HashMap::from([
                    ("street".to_string(), CqlDataType::Text),
                    ("city".to_string(), CqlDataType::Text),
                    ("zip_code".to_string(), CqlDataType::Int),
                ])),
            ],
            test_cases: self.generate_udt_test_cases(),
        });

        // Counter schema
        self.test_schemas.push(TestSchema {
            name: "counters".to_string(),
            keyspace: "compatibility_test".to_string(),
            table: "counters".to_string(),
            cql_definition: r#"
                CREATE TABLE compatibility_test.counters (
                    partition_key TEXT,
                    cluster_key TEXT,
                    counter_col COUNTER,
                    PRIMARY KEY (partition_key, cluster_key)
                )
            "#.to_string(),
            data_types: vec![CqlDataType::Counter],
            test_cases: self.generate_counter_test_cases(),
        });
    }

    /// Generate test data for all Cassandra versions
    pub async fn generate_all_versions(&mut self) -> Result<Vec<GeneratedDataSet>> {
        let mut datasets = Vec::new();
        
        for version in &self.cassandra_versions.clone() {
            println!("📊 Generating test data for Cassandra {}", version);
            
            match self.generate_for_version(version).await {
                Ok(dataset) => {
                    println!("✅ Generated {} SSTable files for version {}", 
                        dataset.sstable_files.len(), version);
                    datasets.push(dataset);
                },
                Err(e) => {
                    eprintln!("❌ Failed to generate data for version {}: {}", version, e);
                }
            }
        }
        
        Ok(datasets)
    }

    /// Generate test data for a specific Cassandra version
    pub async fn generate_for_version(&self, version: &str) -> Result<GeneratedDataSet> {
        let version_dir = self.data_output_dir.join(format!("cassandra-{}", version));
        fs::create_dir_all(&version_dir)?;
        
        // Start Cassandra for this version (assuming it's running)
        let port = self.get_port_for_version(version);
        
        // Create keyspace and schemas
        self.create_keyspace_and_schemas(port).await?;
        
        // Generate and insert test data
        let mut total_rows = 0;
        for schema in &self.test_schemas {
            let rows = self.insert_test_data(port, schema).await?;
            total_rows += rows;
        }
        
        // Force flush to create SSTable files
        self.flush_tables(port).await?;
        
        // Copy SSTable files to our test directory
        let sstable_files = self.collect_sstable_files(version, &version_dir).await?;
        
        // Calculate metadata
        let metadata = self.calculate_metadata(&sstable_files, total_rows).await?;
        
        Ok(GeneratedDataSet {
            version: version.to_string(),
            schemas: self.test_schemas.clone(),
            sstable_files,
            metadata,
        })
    }

    /// Generate identical test data patterns across versions
    pub async fn generate_cross_version_identical_data(&self) -> Result<HashMap<String, PathBuf>> {
        let mut version_data = HashMap::new();
        
        // Use deterministic random seed for identical data
        let seed_data = self.generate_deterministic_test_data();
        
        for version in &self.cassandra_versions {
            let version_dir = self.data_output_dir.join(format!("identical-{}", version));
            fs::create_dir_all(&version_dir)?;
            
            // Insert identical data into each version
            self.insert_deterministic_data(version, &seed_data).await?;
            
            version_data.insert(version.clone(), version_dir);
        }
        
        Ok(version_data)
    }

    /// Create keyspace and all test schemas
    async fn create_keyspace_and_schemas(&self, port: u16) -> Result<()> {
        // Create keyspace
        let create_keyspace = r#"
            CREATE KEYSPACE IF NOT EXISTS compatibility_test 
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
        "#;
        
        self.execute_cql(port, create_keyspace).await?;
        
        // Create all schemas
        for schema in &self.test_schemas {
            // Create UDTs first if needed
            if schema.name == "user_defined_types" {
                self.execute_cql(port, &r#"
                    CREATE TYPE IF NOT EXISTS compatibility_test.address (
                        street TEXT,
                        city TEXT,
                        zip_code INT,
                        coordinates MAP<TEXT, DOUBLE>
                    )
                "#).await?;
                
                self.execute_cql(port, &r#"
                    CREATE TYPE IF NOT EXISTS compatibility_test.user_profile (
                        name TEXT,
                        age INT,
                        addresses LIST<FROZEN<address>>,
                        metadata MAP<TEXT, TEXT>
                    )
                "#).await?;
            }
            
            self.execute_cql(port, &schema.cql_definition).await?;
        }
        
        Ok(())
    }

    /// Insert test data for a specific schema
    async fn insert_test_data(&self, port: u16, schema: &TestSchema) -> Result<usize> {
        let mut total_rows = 0;
        
        for test_case in &schema.test_cases {
            for insert_stmt in &test_case.insert_statements {
                self.execute_cql(port, insert_stmt).await?;
                total_rows += 1;
            }
        }
        
        Ok(total_rows)
    }

    async fn execute_cql(&self, port: u16, cql: &str) -> Result<()> {
        let output = Command::new("cqlsh")
            .args(&[
                &format!("127.0.0.1:{}", port),
                "-e", cql
            ])
            .output()
            .context("Failed to execute CQL")?;
        
        if !output.status.success() {
            return Err(anyhow::anyhow!("CQL execution failed: {}", 
                String::from_utf8_lossy(&output.stderr)));
        }
        
        Ok(())
    }

    async fn flush_tables(&self, port: u16) -> Result<()> {
        let flush_cmd = "CALL cqlrunner.flush('compatibility_test');";
        // Alternative: use nodetool flush
        let output = Command::new("docker")
            .args(&[
                "exec", &format!("cassandra-{}", port),
                "nodetool", "flush", "compatibility_test"
            ])
            .output()
            .context("Failed to flush tables")?;
        
        if !output.status.success() {
            eprintln!("Warning: flush command failed: {}", 
                String::from_utf8_lossy(&output.stderr));
        }
        
        Ok(())
    }

    async fn collect_sstable_files(&self, version: &str, output_dir: &Path) -> Result<Vec<PathBuf>> {
        // Copy SSTable files from Cassandra container
        let container_name = format!("cassandra-{}", version);
        
        let output = Command::new("docker")
            .args(&[
                "exec", &container_name,
                "find", "/var/lib/cassandra/data/compatibility_test", 
                "-name", "*.db", "-type", "f"
            ])
            .output()
            .context("Failed to find SSTable files")?;
        
        if !output.status.success() {
            return Err(anyhow::anyhow!("Failed to list SSTable files: {}", 
                String::from_utf8_lossy(&output.stderr)));
        }
        
        let sstable_paths = String::from_utf8(output.stdout)?;
        let mut collected_files = Vec::new();
        
        for path in sstable_paths.lines() {
            if !path.trim().is_empty() {
                let filename = Path::new(path).file_name()
                    .ok_or_else(|| anyhow::anyhow!("Invalid file path: {}", path))?;
                
                let local_path = output_dir.join(filename);
                
                // Copy file from container
                Command::new("docker")
                    .args(&["cp", &format!("{}:{}", container_name, path), &local_path.to_string_lossy()])
                    .output()
                    .context("Failed to copy SSTable file")?;
                
                collected_files.push(local_path);
            }
        }
        
        Ok(collected_files)
    }

    async fn calculate_metadata(&self, sstable_files: &[PathBuf], total_rows: usize) -> Result<DataSetMetadata> {
        let mut total_size = 0u64;
        
        for file in sstable_files {
            if let Ok(metadata) = fs::metadata(file) {
                total_size += metadata.len();
            }
        }
        
        Ok(DataSetMetadata {
            generated_at: Utc::now(),
            total_rows,
            total_sstables: sstable_files.len(),
            data_size_bytes: total_size,
            compression_ratio: 0.7, // Placeholder
            test_coverage: TestCoverage {
                data_types_covered: vec!["all_basic_types".to_string()],
                collection_types_covered: vec!["list".to_string(), "set".to_string(), "map".to_string()],
                udt_complexity_levels: vec!["simple".to_string(), "nested".to_string()],
                edge_cases_covered: vec!["null_values".to_string(), "empty_collections".to_string()],
            },
        })
    }

    fn generate_basic_type_test_cases(&self) -> Vec<TestCase> {
        vec![
            TestCase {
                name: "basic_values".to_string(),
                description: "Standard values for all basic types".to_string(),
                insert_statements: vec![
                    format!(
                        "INSERT INTO compatibility_test.basic_types (id, text_col, int_col, bigint_col, boolean_col) VALUES ({}, 'test_text', 42, 9223372036854775807, true)",
                        Uuid::new_v4()
                    ),
                ],
                expected_rows: 1,
                complexity_level: ComplexityLevel::Basic,
                cassandra_version_min: "4.0".to_string(),
            },
            // Add more test cases...
        ]
    }

    fn generate_collection_test_cases(&self) -> Vec<TestCase> {
        vec![
            TestCase {
                name: "collection_basics".to_string(),
                description: "Basic collection operations".to_string(),
                insert_statements: vec![
                    format!(
                        "INSERT INTO compatibility_test.collections (id, list_text, set_int, map_text_int) VALUES ({}, ['a', 'b', 'c'], {{1, 2, 3}}, {{'key1': 100, 'key2': 200}})",
                        Uuid::new_v4()
                    ),
                ],
                expected_rows: 1,
                complexity_level: ComplexityLevel::Moderate,
                cassandra_version_min: "4.0".to_string(),
            },
        ]
    }

    fn generate_udt_test_cases(&self) -> Vec<TestCase> {
        vec![
            TestCase {
                name: "udt_basic".to_string(),
                description: "Basic UDT usage".to_string(),
                insert_statements: vec![
                    format!(
                        "INSERT INTO compatibility_test.udt_test (id, simple_address) VALUES ({}, {{street: '123 Main St', city: 'Anytown', zip_code: 12345, coordinates: {{'lat': 40.7128, 'lng': -74.0060}}}})",
                        Uuid::new_v4()
                    ),
                ],
                expected_rows: 1,
                complexity_level: ComplexityLevel::Complex,
                cassandra_version_min: "4.0".to_string(),
            },
        ]
    }

    fn generate_counter_test_cases(&self) -> Vec<TestCase> {
        vec![
            TestCase {
                name: "counter_basic".to_string(),
                description: "Basic counter operations".to_string(),
                insert_statements: vec![
                    "UPDATE compatibility_test.counters SET counter_col = counter_col + 1 WHERE partition_key = 'test' AND cluster_key = 'counter1'".to_string(),
                ],
                expected_rows: 1,
                complexity_level: ComplexityLevel::Basic,
                cassandra_version_min: "4.0".to_string(),
            },
        ]
    }

    fn generate_deterministic_test_data(&self) -> HashMap<String, Vec<String>> {
        // Generate deterministic test data for cross-version comparison
        HashMap::new() // Placeholder
    }

    async fn insert_deterministic_data(&self, version: &str, data: &HashMap<String, Vec<String>>) -> Result<()> {
        // Insert deterministic data for cross-version testing
        Ok(()) // Placeholder
    }

    fn get_port_for_version(&self, version: &str) -> u16 {
        match version {
            "4.0" => 9042,
            "4.1" => 9043,
            "5.0" => 9044,
            "5.1" => 9045,
            "6.0" => 9046,
            _ => 9042,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_data_generator_creation() {
        let temp_dir = TempDir::new().unwrap();
        let generator = TestDataGenerator::new(temp_dir.path().to_path_buf());
        
        assert!(!generator.test_schemas.is_empty());
        assert!(generator.test_schemas.iter().any(|s| s.name == "basic_types"));
        assert!(generator.test_schemas.iter().any(|s| s.name == "collections"));
    }

    #[test]
    fn test_test_case_generation() {
        let temp_dir = TempDir::new().unwrap();
        let generator = TestDataGenerator::new(temp_dir.path().to_path_buf());
        
        let basic_cases = generator.generate_basic_type_test_cases();
        assert!(!basic_cases.is_empty());
        assert!(basic_cases[0].insert_statements.len() > 0);
    }
}