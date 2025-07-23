//! Test Data Fixtures for CQL Schema Validation
//!
//! Provides comprehensive test data including CQL samples, expected schemas,
//! error cases, and performance test data for thorough validation testing.

use std::collections::HashMap;

/// Collection of CQL test cases with expected outcomes
#[derive(Debug, Clone)]
pub struct CqlTestCase {
    pub name: &'static str,
    pub cql: &'static str,
    pub expected_table_name: &'static str,
    pub expected_columns: usize,
    pub should_succeed: bool,
    pub expected_features: Vec<&'static str>,
}

/// Collection of type conversion test cases
#[derive(Debug, Clone)]
pub struct TypeTestCase {
    pub cql_type: &'static str,
    pub test_values: Vec<&'static str>,
    pub expected_internal_type: &'static str,
    pub should_succeed: bool,
}

/// Error test cases for malformed CQL
#[derive(Debug, Clone)]
pub struct ErrorTestCase {
    pub name: &'static str,
    pub malformed_cql: &'static str,
    pub expected_error_pattern: &'static str,
}

/// Performance test data generator
pub struct PerformanceTestData;

impl PerformanceTestData {
    /// Generate basic CQL statements for performance testing
    pub fn basic_cql_statements() -> Vec<CqlTestCase> {
        vec![
            CqlTestCase {
                name: "simple_table",
                cql: "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT, email TEXT);",
                expected_table_name: "users",
                expected_columns: 3,
                should_succeed: true,
                expected_features: vec!["primary_key", "text_columns"],
            },
            CqlTestCase {
                name: "clustering_table",
                cql: r#"CREATE TABLE events (
                    user_id UUID,
                    event_time TIMESTAMP,
                    event_type TEXT,
                    data BLOB,
                    PRIMARY KEY (user_id, event_time)
                ) WITH CLUSTERING ORDER BY (event_time DESC);"#,
                expected_table_name: "events",
                expected_columns: 4,
                should_succeed: true,
                expected_features: vec!["composite_primary_key", "clustering_key", "clustering_order"],
            },
            CqlTestCase {
                name: "collections_table",
                cql: r#"CREATE TABLE posts (
                    id UUID PRIMARY KEY,
                    title TEXT,
                    tags SET<TEXT>,
                    metadata MAP<TEXT, TEXT>,
                    comments LIST<TEXT>
                );"#,
                expected_table_name: "posts",
                expected_columns: 5,
                should_succeed: true,
                expected_features: vec!["collections", "set_type", "map_type", "list_type"],
            },
        ]
    }

    /// Generate complex CQL statements for advanced testing
    pub fn complex_cql_statements() -> Vec<CqlTestCase> {
        vec![
            CqlTestCase {
                name: "nested_collections",
                cql: r#"CREATE TABLE complex_data (
                    id UUID PRIMARY KEY,
                    nested_list LIST<FROZEN<MAP<TEXT, SET<INT>>>>,
                    tuple_data TUPLE<UUID, TIMESTAMP, TEXT>,
                    frozen_collection FROZEN<LIST<TEXT>>
                );"#,
                expected_table_name: "complex_data",
                expected_columns: 4,
                should_succeed: true,
                expected_features: vec!["nested_collections", "tuples", "frozen_types"],
            },
            CqlTestCase {
                name: "all_primitive_types",
                cql: r#"CREATE TABLE all_types (
                    id UUID PRIMARY KEY,
                    col_boolean BOOLEAN,
                    col_tinyint TINYINT,
                    col_smallint SMALLINT,
                    col_int INT,
                    col_bigint BIGINT,
                    col_float FLOAT,
                    col_double DOUBLE,
                    col_decimal DECIMAL,
                    col_text TEXT,
                    col_ascii ASCII,
                    col_varchar VARCHAR,
                    col_blob BLOB,
                    col_timestamp TIMESTAMP,
                    col_date DATE,
                    col_time TIME,
                    col_uuid UUID,
                    col_timeuuid TIMEUUID,
                    col_inet INET,
                    col_duration DURATION
                );"#,
                expected_table_name: "all_types",
                expected_columns: 20,
                should_succeed: true,
                expected_features: vec!["all_primitive_types"],
            },
            CqlTestCase {
                name: "complex_primary_key",
                cql: r#"CREATE TABLE time_series (
                    sensor_id UUID,
                    location TEXT,
                    year INT,
                    month INT,
                    day INT,
                    timestamp TIMESTAMP,
                    value DOUBLE,
                    metadata MAP<TEXT, TEXT>,
                    PRIMARY KEY ((sensor_id, location, year), month, day, timestamp)
                ) WITH CLUSTERING ORDER BY (month ASC, day ASC, timestamp DESC);"#,
                expected_table_name: "time_series",
                expected_columns: 8,
                should_succeed: true,
                expected_features: vec!["composite_partition_key", "multiple_clustering_keys", "clustering_order"],
            },
        ]
    }

    /// Generate UDT test cases
    pub fn udt_test_cases() -> Vec<CqlTestCase> {
        vec![
            CqlTestCase {
                name: "simple_udt",
                cql: r#"CREATE TABLE users (
                    id UUID PRIMARY KEY,
                    name TEXT,
                    address FROZEN<address>
                );"#,
                expected_table_name: "users",
                expected_columns: 3,
                should_succeed: true,
                expected_features: vec!["udt_usage", "frozen_udt"],
            },
            CqlTestCase {
                name: "nested_udt",
                cql: r#"CREATE TABLE employees (
                    id UUID PRIMARY KEY,
                    employee_data FROZEN<person>,
                    emergency_contact FROZEN<person>
                );"#,
                expected_table_name: "employees",
                expected_columns: 3,
                should_succeed: true,
                expected_features: vec!["udt_usage", "multiple_udts"],
            },
            CqlTestCase {
                name: "udt_in_collections",
                cql: r#"CREATE TABLE posts (
                    id UUID PRIMARY KEY,
                    title TEXT,
                    tags LIST<FROZEN<tag>>,
                    tag_map MAP<TEXT, FROZEN<tag>>
                );"#,
                expected_table_name: "posts",
                expected_columns: 4,
                should_succeed: true,
                expected_features: vec!["udt_in_collections", "list_of_udt", "map_with_udt"],
            },
        ]
    }

    /// Generate error test cases
    pub fn error_test_cases() -> Vec<ErrorTestCase> {
        vec![
            ErrorTestCase {
                name: "missing_semicolon",
                malformed_cql: "CREATE TABLE users (id UUID PRIMARY KEY)",
                expected_error_pattern: "semicolon",
            },
            ErrorTestCase {
                name: "missing_table_name",
                malformed_cql: "CREATE TABLE (id UUID PRIMARY KEY);",
                expected_error_pattern: "table name",
            },
            ErrorTestCase {
                name: "missing_primary_key",
                malformed_cql: "CREATE TABLE users (id UUID, name TEXT);",
                expected_error_pattern: "primary key",
            },
            ErrorTestCase {
                name: "invalid_data_type",
                malformed_cql: "CREATE TABLE users (id INVALID_TYPE PRIMARY KEY);",
                expected_error_pattern: "invalid.*type",
            },
            ErrorTestCase {
                name: "unclosed_parenthesis",
                malformed_cql: "CREATE TABLE users (id UUID PRIMARY KEY;",
                expected_error_pattern: "parenthesis",
            },
            ErrorTestCase {
                name: "empty_list_type",
                malformed_cql: "CREATE TABLE users (id UUID PRIMARY KEY, tags LIST<>);",
                expected_error_pattern: "empty.*list",
            },
            ErrorTestCase {
                name: "incomplete_map_type",
                malformed_cql: "CREATE TABLE users (id UUID PRIMARY KEY, data MAP<TEXT>);",
                expected_error_pattern: "incomplete.*map",
            },
            ErrorTestCase {
                name: "reserved_keyword",
                malformed_cql: "CREATE TABLE users (select TEXT PRIMARY KEY);",
                expected_error_pattern: "reserved.*keyword",
            },
            ErrorTestCase {
                name: "invalid_clustering_order",
                malformed_cql: r#"CREATE TABLE events (
                    id UUID, 
                    ts TIMESTAMP, 
                    PRIMARY KEY (id, ts)
                ) WITH CLUSTERING ORDER BY (ts INVALID);"#,
                expected_error_pattern: "invalid.*clustering.*order",
            },
        ]
    }

    /// Generate type conversion test cases
    pub fn type_conversion_test_cases() -> Vec<TypeTestCase> {
        vec![
            TypeTestCase {
                cql_type: "BOOLEAN",
                test_values: vec!["true", "false"],
                expected_internal_type: "Boolean",
                should_succeed: true,
            },
            TypeTestCase {
                cql_type: "TINYINT",
                test_values: vec!["42", "-128", "127"],
                expected_internal_type: "TinyInt",
                should_succeed: true,
            },
            TypeTestCase {
                cql_type: "SMALLINT",
                test_values: vec!["1000", "-32768", "32767"],
                expected_internal_type: "SmallInt",
                should_succeed: true,
            },
            TypeTestCase {
                cql_type: "INT",
                test_values: vec!["1000000", "-2147483648", "2147483647"],
                expected_internal_type: "Int",
                should_succeed: true,
            },
            TypeTestCase {
                cql_type: "BIGINT",
                test_values: vec!["1000000000000", "-9223372036854775808", "9223372036854775807"],
                expected_internal_type: "BigInt",
                should_succeed: true,
            },
            TypeTestCase {
                cql_type: "FLOAT",
                test_values: vec!["3.14", "-1.0", "0.0"],
                expected_internal_type: "Float",
                should_succeed: true,
            },
            TypeTestCase {
                cql_type: "DOUBLE",
                test_values: vec!["3.14159265", "-1.0", "0.0"],
                expected_internal_type: "Double",
                should_succeed: true,
            },
            TypeTestCase {
                cql_type: "TEXT",
                test_values: vec!["'hello'", "'world'", "''"],
                expected_internal_type: "Text",
                should_succeed: true,
            },
            TypeTestCase {
                cql_type: "VARCHAR",
                test_values: vec!["'varchar'", "'test'"],
                expected_internal_type: "Text", // VARCHAR maps to Text internally
                should_succeed: true,
            },
            TypeTestCase {
                cql_type: "ASCII",
                test_values: vec!["'ascii'", "'test'"],
                expected_internal_type: "Ascii",
                should_succeed: true,
            },
            TypeTestCase {
                cql_type: "BLOB",
                test_values: vec!["0x010203", "0xdeadbeef"],
                expected_internal_type: "Blob",
                should_succeed: true,
            },
            TypeTestCase {
                cql_type: "UUID",
                test_values: vec!["550e8400-e29b-41d4-a716-446655440000"],
                expected_internal_type: "Uuid",
                should_succeed: true,
            },
            TypeTestCase {
                cql_type: "TIMEUUID",
                test_values: vec!["550e8400-e29b-41d4-a716-446655440000"],
                expected_internal_type: "TimeUuid",
                should_succeed: true,
            },
            TypeTestCase {
                cql_type: "TIMESTAMP",
                test_values: vec!["'2023-01-01 00:00:00'", "'2023-12-31 23:59:59'"],
                expected_internal_type: "Timestamp",
                should_succeed: true,
            },
            TypeTestCase {
                cql_type: "DATE",
                test_values: vec!["'2023-01-01'", "'2023-12-31'"],
                expected_internal_type: "Date",
                should_succeed: true,
            },
            TypeTestCase {
                cql_type: "TIME",
                test_values: vec!["'12:30:45'", "'00:00:00'", "'23:59:59'"],
                expected_internal_type: "Time",
                should_succeed: true,
            },
            TypeTestCase {
                cql_type: "INET",
                test_values: vec!["'192.168.1.1'", "'::1'", "'2001:db8::1'"],
                expected_internal_type: "Inet",
                should_succeed: true,
            },
            TypeTestCase {
                cql_type: "DURATION",
                test_values: vec!["1y2mo3w4d5h6m7s8ms", "P1Y2M3DT4H5M6.007S"],
                expected_internal_type: "Duration",
                should_succeed: true,
            },
            // Collection types
            TypeTestCase {
                cql_type: "LIST<TEXT>",
                test_values: vec!["['item1', 'item2', 'item3']", "[]"],
                expected_internal_type: "List<Text>",
                should_succeed: true,
            },
            TypeTestCase {
                cql_type: "SET<INT>",
                test_values: vec!["{1, 2, 3, 4, 5}", "{}"],
                expected_internal_type: "Set<Int>",
                should_succeed: true,
            },
            TypeTestCase {
                cql_type: "MAP<TEXT, BIGINT>",
                test_values: vec!["{'key1': 100, 'key2': 200}", "{}"],
                expected_internal_type: "Map<Text, BigInt>",
                should_succeed: true,
            },
            // Complex nested types
            TypeTestCase {
                cql_type: "LIST<SET<TEXT>>",
                test_values: vec!["[{'a', 'b'}, {'c', 'd'}]", "[]"],
                expected_internal_type: "List<Set<Text>>",
                should_succeed: true,
            },
            TypeTestCase {
                cql_type: "MAP<TEXT, LIST<INT>>",
                test_values: vec!["{'key1': [1, 2, 3], 'key2': [4, 5, 6]}", "{}"],
                expected_internal_type: "Map<Text, List<Int>>",
                should_succeed: true,
            },
            // Tuple types
            TypeTestCase {
                cql_type: "TUPLE<TEXT, INT, BOOLEAN>",
                test_values: vec!["('hello', 42, true)", "('world', 0, false)"],
                expected_internal_type: "Tuple<Text, Int, Boolean>",
                should_succeed: true,
            },
            // Frozen types
            TypeTestCase {
                cql_type: "FROZEN<LIST<TEXT>>",
                test_values: vec!["['item1', 'item2']"],
                expected_internal_type: "Frozen<List<Text>>",
                should_succeed: true,
            },
        ]
    }

    /// Generate performance test schemas of various sizes
    pub fn performance_test_schemas() -> Vec<(String, String)> {
        let mut schemas = Vec::new();
        
        // Small schema (10 columns)
        schemas.push((
            "small_schema".to_string(),
            Self::generate_schema_with_columns(10),
        ));
        
        // Medium schema (50 columns)
        schemas.push((
            "medium_schema".to_string(),
            Self::generate_schema_with_columns(50),
        ));
        
        // Large schema (200 columns)
        schemas.push((
            "large_schema".to_string(),
            Self::generate_schema_with_columns(200),
        ));
        
        // Extra large schema (500 columns)
        schemas.push((
            "extra_large_schema".to_string(),
            Self::generate_schema_with_columns(500),
        ));
        
        // Complex nested schema
        schemas.push((
            "complex_nested_schema".to_string(),
            r#"CREATE TABLE complex_nested (
                id UUID PRIMARY KEY,
                level1 MAP<TEXT, FROZEN<MAP<TEXT, LIST<TUPLE<UUID, TEXT, BIGINT>>>>>,
                level2 LIST<FROZEN<LIST<FROZEN<SET<TEXT>>>>>,
                level3 TUPLE<UUID, FROZEN<TUPLE<TEXT, FROZEN<TUPLE<BIGINT, TIMESTAMP>>>>>,
                collections_array LIST<FROZEN<MAP<TEXT, SET<FROZEN<TUPLE<UUID, TIMESTAMP, TEXT>>>>>>,
                deep_map MAP<TEXT, FROZEN<MAP<TEXT, FROZEN<MAP<TEXT, LIST<INT>>>>>>
            );"#.to_string(),
        ));
        
        schemas
    }

    /// Generate a schema with specified number of columns
    fn generate_schema_with_columns(num_columns: usize) -> String {
        let mut cql = String::from("CREATE TABLE generated_table (\n");
        
        for i in 0..num_columns {
            let col_type = match i % 20 {
                0 => "UUID",
                1 => "TEXT",
                2 => "BIGINT",
                3 => "TIMESTAMP",
                4 => "BOOLEAN",
                5 => "DOUBLE",
                6 => "INT",
                7 => "FLOAT",
                8 => "SMALLINT",
                9 => "TINYINT",
                10 => "BLOB",
                11 => "DECIMAL",
                12 => "DATE",
                13 => "TIME",
                14 => "INET",
                15 => "DURATION",
                16 => "LIST<TEXT>",
                17 => "SET<INT>",
                18 => "MAP<TEXT, BIGINT>",
                _ => "VARCHAR",
            };
            
            if i == 0 {
                cql.push_str(&format!("    col_{} {} PRIMARY KEY", i, col_type));
            } else {
                cql.push_str(&format!(",\n    col_{} {}", i, col_type));
            }
        }
        
        cql.push_str("\n);");
        cql
    }

    /// Generate real-world-like schemas for integration testing
    pub fn real_world_schemas() -> Vec<(String, String)> {
        vec![
            (
                "ecommerce_orders".to_string(),
                r#"CREATE TABLE orders (
                    order_id UUID,
                    customer_id UUID,
                    order_date DATE,
                    order_timestamp TIMESTAMP,
                    status TEXT,
                    total_amount DECIMAL,
                    currency TEXT,
                    items LIST<FROZEN<MAP<TEXT, TEXT>>>,
                    shipping_address FROZEN<MAP<TEXT, TEXT>>,
                    billing_address FROZEN<MAP<TEXT, TEXT>>,
                    payment_method TEXT,
                    tracking_number TEXT,
                    notes TEXT,
                    metadata MAP<TEXT, TEXT>,
                    tags SET<TEXT>,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP,
                    PRIMARY KEY ((customer_id, order_date), order_timestamp, order_id)
                ) WITH CLUSTERING ORDER BY (order_timestamp DESC, order_id ASC);"#,
            ),
            (
                "user_profiles".to_string(),
                r#"CREATE TABLE user_profiles (
                    user_id UUID,
                    profile_type TEXT,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP,
                    username TEXT,
                    email TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    date_of_birth DATE,
                    profile_data MAP<TEXT, TEXT>,
                    preferences MAP<TEXT, BOOLEAN>,
                    social_links LIST<TEXT>,
                    tags SET<TEXT>,
                    avatar_url TEXT,
                    bio TEXT,
                    location TUPLE<DOUBLE, DOUBLE, TEXT>,
                    verification_status TEXT,
                    privacy_settings FROZEN<MAP<TEXT, BOOLEAN>>,
                    PRIMARY KEY (user_id, profile_type)
                ) WITH CLUSTERING ORDER BY (profile_type ASC);"#,
            ),
            (
                "iot_sensor_data".to_string(),
                r#"CREATE TABLE sensor_readings (
                    sensor_id UUID,
                    location TEXT,
                    year INT,
                    month INT,
                    day INT,
                    hour INT,
                    timestamp TIMESTAMP,
                    sensor_type TEXT,
                    readings MAP<TEXT, DOUBLE>,
                    status_flags SET<TEXT>,
                    raw_data BLOB,
                    calibration_data MAP<TEXT, DOUBLE>,
                    metadata TUPLE<UUID, TIMESTAMP, TEXT, MAP<TEXT, TEXT>>,
                    alert_thresholds MAP<TEXT, TUPLE<DOUBLE, DOUBLE>>,
                    last_maintenance TIMESTAMP,
                    firmware_version TEXT,
                    battery_level FLOAT,
                    signal_strength INT,
                    PRIMARY KEY ((sensor_id, location, year), month, day, hour, timestamp)
                ) WITH CLUSTERING ORDER BY (month ASC, day ASC, hour ASC, timestamp DESC);"#,
            ),
            (
                "social_media_posts".to_string(),
                r#"CREATE TABLE social_posts (
                    user_id UUID,
                    post_type TEXT,
                    created_at TIMESTAMP,
                    post_id TIMEUUID,
                    content TEXT,
                    hashtags SET<TEXT>,
                    mentions LIST<UUID>,
                    media_urls LIST<TEXT>,
                    media_metadata LIST<FROZEN<MAP<TEXT, TEXT>>>,
                    reactions MAP<TEXT, BIGINT>,
                    location TUPLE<DOUBLE, DOUBLE, TEXT>,
                    visibility_settings FROZEN<MAP<TEXT, BOOLEAN>>,
                    edit_history LIST<FROZEN<TUPLE<TIMESTAMP, TEXT>>>,
                    reply_to_post UUID,
                    share_count BIGINT,
                    view_count BIGINT,
                    engagement_score DOUBLE,
                    language TEXT,
                    content_warning SET<TEXT>,
                    PRIMARY KEY ((user_id, post_type), created_at, post_id)
                ) WITH CLUSTERING ORDER BY (created_at DESC, post_id DESC);"#,
            ),
            (
                "financial_transactions".to_string(),
                r#"CREATE TABLE transactions (
                    account_id UUID,
                    transaction_date DATE,
                    transaction_id TIMEUUID,
                    timestamp TIMESTAMP,
                    transaction_type TEXT,
                    amount DECIMAL,
                    currency TEXT,
                    from_account UUID,
                    to_account UUID,
                    reference_number TEXT,
                    description TEXT,
                    category TEXT,
                    subcategory TEXT,
                    merchant_info FROZEN<MAP<TEXT, TEXT>>,
                    location TUPLE<DOUBLE, DOUBLE, TEXT>,
                    payment_method TEXT,
                    status TEXT,
                    fees MAP<TEXT, DECIMAL>,
                    exchange_rate DECIMAL,
                    original_amount DECIMAL,
                    original_currency TEXT,
                    reconciliation_status TEXT,
                    metadata MAP<TEXT, TEXT>,
                    tags SET<TEXT>,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP,
                    PRIMARY KEY ((account_id, transaction_date), timestamp, transaction_id)
                ) WITH CLUSTERING ORDER BY (timestamp DESC, transaction_id DESC);"#,
            ),
        ]
    }
}

/// JSON schema examples for comparison testing
pub struct JsonSchemaFixtures;

impl JsonSchemaFixtures {
    /// Get sample JSON schema for users table
    pub fn users_table_schema() -> &'static str {
        r#"{
            "keyspace": "test",
            "table": "users",
            "partition_keys": [
                {"name": "id", "type": "uuid", "position": 0}
            ],
            "clustering_keys": [],
            "columns": [
                {"name": "id", "type": "uuid", "nullable": false},
                {"name": "name", "type": "text", "nullable": true},
                {"name": "email", "type": "text", "nullable": true}
            ]
        }"#
    }

    /// Get sample JSON schema for events table with clustering
    pub fn events_table_schema() -> &'static str {
        r#"{
            "keyspace": "test",
            "table": "events",
            "partition_keys": [
                {"name": "user_id", "type": "uuid", "position": 0}
            ],
            "clustering_keys": [
                {"name": "event_time", "type": "timestamp", "position": 0, "order": "DESC"}
            ],
            "columns": [
                {"name": "user_id", "type": "uuid", "nullable": false},
                {"name": "event_time", "type": "timestamp", "nullable": false},
                {"name": "event_type", "type": "text", "nullable": true},
                {"name": "data", "type": "blob", "nullable": true}
            ]
        }"#
    }

    /// Get sample JSON schema for posts table with collections
    pub fn posts_table_schema() -> &'static str {
        r#"{
            "keyspace": "test",
            "table": "posts",
            "partition_keys": [
                {"name": "id", "type": "uuid", "position": 0}
            ],
            "clustering_keys": [],
            "columns": [
                {"name": "id", "type": "uuid", "nullable": false},
                {"name": "title", "type": "text", "nullable": true},
                {"name": "tags", "type": "set<text>", "nullable": true},
                {"name": "metadata", "type": "map<text, text>", "nullable": true},
                {"name": "comments", "type": "list<text>", "nullable": true}
            ]
        }"#
    }

    /// Get sample JSON schema with complex types
    pub fn complex_types_schema() -> &'static str {
        r#"{
            "keyspace": "test",
            "table": "complex_data",
            "partition_keys": [
                {"name": "id", "type": "uuid", "position": 0}
            ],
            "clustering_keys": [],
            "columns": [
                {"name": "id", "type": "uuid", "nullable": false},
                {"name": "nested_list", "type": "list<frozen<map<text, set<int>>>>", "nullable": true},
                {"name": "tuple_data", "type": "tuple<uuid, timestamp, text>", "nullable": true},
                {"name": "frozen_collection", "type": "frozen<list<text>>", "nullable": true}
            ]
        }"#
    }

    /// Get all sample JSON schemas
    pub fn all_schemas() -> HashMap<&'static str, &'static str> {
        let mut schemas = HashMap::new();
        schemas.insert("users", Self::users_table_schema());
        schemas.insert("events", Self::events_table_schema());
        schemas.insert("posts", Self::posts_table_schema());
        schemas.insert("complex_data", Self::complex_types_schema());
        schemas
    }
}

/// Test fixtures for compatibility testing
pub struct CompatibilityTestFixtures;

impl CompatibilityTestFixtures {
    /// Get CQL statements that should work across different Cassandra versions
    pub fn cross_version_compatible_cql() -> Vec<CqlTestCase> {
        vec![
            CqlTestCase {
                name: "basic_table_v3_compatible",
                cql: "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT, created_at TIMESTAMP);",
                expected_table_name: "users",
                expected_columns: 3,
                should_succeed: true,
                expected_features: vec!["cassandra_3_compatible"],
            },
            CqlTestCase {
                name: "clustering_table_v3_compatible",
                cql: r#"CREATE TABLE logs (
                    app_id TEXT,
                    timestamp TIMESTAMP,
                    level TEXT,
                    message TEXT,
                    PRIMARY KEY (app_id, timestamp)
                ) WITH CLUSTERING ORDER BY (timestamp DESC);"#,
                expected_table_name: "logs",
                expected_columns: 4,
                should_succeed: true,
                expected_features: vec!["cassandra_3_compatible", "clustering"],
            },
        ]
    }

    /// Get CQL statements that require Cassandra 4.0+
    pub fn cassandra_4_plus_cql() -> Vec<CqlTestCase> {
        vec![
            CqlTestCase {
                name: "duration_type_v4",
                cql: "CREATE TABLE events (id UUID PRIMARY KEY, name TEXT, duration DURATION);",
                expected_table_name: "events",
                expected_columns: 3,
                should_succeed: true,
                expected_features: vec!["cassandra_4_plus", "duration_type"],
            },
        ]
    }

    /// Get CQL statements that require Cassandra 5.0+
    pub fn cassandra_5_plus_cql() -> Vec<CqlTestCase> {
        vec![
            CqlTestCase {
                name: "enhanced_collections_v5",
                cql: r#"CREATE TABLE enhanced_data (
                    id UUID PRIMARY KEY,
                    vector_data LIST<DOUBLE>,
                    json_column JSON
                );"#,
                expected_table_name: "enhanced_data",
                expected_columns: 3,
                should_succeed: true,
                expected_features: vec!["cassandra_5_plus", "vector_support", "json_type"],
            },
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_cql_statements() {
        let statements = PerformanceTestData::basic_cql_statements();
        assert!(!statements.is_empty());
        
        for statement in &statements {
            assert!(!statement.cql.is_empty());
            assert!(!statement.expected_table_name.is_empty());
            assert!(statement.expected_columns > 0);
        }
    }

    #[test]
    fn test_error_test_cases() {
        let error_cases = PerformanceTestData::error_test_cases();
        assert!(!error_cases.is_empty());
        
        for case in &error_cases {
            assert!(!case.name.is_empty());
            assert!(!case.malformed_cql.is_empty());
            assert!(!case.expected_error_pattern.is_empty());
        }
    }

    #[test]
    fn test_type_conversion_cases() {
        let type_cases = PerformanceTestData::type_conversion_test_cases();
        assert!(!type_cases.is_empty());
        
        for case in &type_cases {
            assert!(!case.cql_type.is_empty());
            assert!(!case.test_values.is_empty());
            assert!(!case.expected_internal_type.is_empty());
        }
    }

    #[test]
    fn test_json_schema_fixtures() {
        let schemas = JsonSchemaFixtures::all_schemas();
        assert!(!schemas.is_empty());
        
        for (name, schema_json) in &schemas {
            assert!(!name.is_empty());
            assert!(!schema_json.is_empty());
            assert!(schema_json.contains("keyspace"));
            assert!(schema_json.contains("table"));
            assert!(schema_json.contains("columns"));
        }
    }

    #[test]
    fn test_performance_schemas() {
        let schemas = PerformanceTestData::performance_test_schemas();
        assert!(!schemas.is_empty());
        
        for (name, cql) in &schemas {
            assert!(!name.is_empty());
            assert!(!cql.is_empty());
            assert!(cql.contains("CREATE TABLE"));
        }
    }

    #[test]
    fn test_real_world_schemas() {
        let schemas = PerformanceTestData::real_world_schemas();
        assert!(!schemas.is_empty());
        
        for (name, cql) in &schemas {
            assert!(!name.is_empty());
            assert!(!cql.is_empty());
            assert!(cql.contains("CREATE TABLE"));
            assert!(cql.contains("PRIMARY KEY"));
        }
    }
}