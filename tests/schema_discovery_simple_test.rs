//! Simple synchronous test for schema discovery logic
//! Tests the core algorithms without async/external dependencies

use std::collections::HashMap;

#[cfg(test)]
mod schema_discovery_tests {
    use super::*;

    #[test]
    fn test_type_normalization() {
        // Test type name normalization logic from schema discovery
        fn normalize_type_name(type_name: &str) -> String {
            match type_name.to_lowercase().as_str() {
                "int" | "integer" => "int".to_string(),
                "bigint" | "biginteger" => "bigint".to_string(),
                "double" | "float64" => "double".to_string(),
                "float" | "float32" => "float".to_string(),
                "text" | "varchar" | "string" => "text".to_string(),
                "bool" | "boolean" => "boolean".to_string(),
                "timestamp" | "datetime" => "timestamp".to_string(),
                "blob" | "bytes" => "blob".to_string(),
                "uuid" => "uuid".to_string(),
                "decimal" => "decimal".to_string(),
                "varint" => "varint".to_string(),
                "tinyint" => "tinyint".to_string(),
                "smallint" => "smallint".to_string(),
                "duration" => "duration".to_string(),
                _ => type_name.to_string(),
            }
        }

        let test_cases = vec![
            ("INT", "int"),
            ("Integer", "int"),
            ("BIGINT", "bigint"),
            ("varchar", "text"),
            ("STRING", "text"),
            ("BOOL", "boolean"),
            ("custom_type", "custom_type"),
        ];

        for (input, expected) in test_cases {
            assert_eq!(normalize_type_name(input), expected);
        }

        println!("✅ Type normalization tests passed");
    }

    #[test]
    fn test_udt_detection() {
        // Test UDT detection logic
        fn is_udt_type(type_str: &str) -> bool {
            !matches!(
                type_str.to_lowercase().as_str(),
                "text"
                    | "varchar"
                    | "ascii"
                    | "int"
                    | "bigint"
                    | "smallint"
                    | "tinyint"
                    | "float"
                    | "double"
                    | "boolean"
                    | "timestamp"
                    | "date"
                    | "time"
                    | "uuid"
                    | "timeuuid"
                    | "blob"
                    | "varint"
                    | "decimal"
                    | "duration"
                    | "inet"
                    | "counter"
            ) && !type_str.starts_with("list<")
                && !type_str.starts_with("set<")
                && !type_str.starts_with("map<")
                && !type_str.starts_with("tuple<")
                && !type_str.starts_with("frozen<")
        }

        let test_cases = vec![
            ("text", false),
            ("int", false),
            ("list<text>", false),
            ("map<text,int>", false),
            ("address_type", true),
            ("user_profile", true),
            ("my_custom_udt", true),
        ];

        for (type_str, expected) in test_cases {
            assert_eq!(is_udt_type(type_str), expected, "Failed for: {}", type_str);
        }

        println!("✅ UDT detection tests passed");
    }

    #[test]
    fn test_collection_parsing() {
        // Test collection type parsing
        fn parse_collection_kind(type_str: &str) -> Option<&'static str> {
            let lower_type = type_str.to_lowercase();
            if lower_type.starts_with("list<") {
                Some("list")
            } else if lower_type.starts_with("set<") {
                Some("set")
            } else if lower_type.starts_with("map<") {
                Some("map")
            } else if lower_type.starts_with("tuple<") {
                Some("tuple")
            } else if lower_type.starts_with("frozen<") {
                // Parse inner type
                if let Some(start) = type_str.find('<') {
                    if let Some(end) = type_str.rfind('>') {
                        let inner = &type_str[start + 1..end];
                        return parse_collection_kind(inner);
                    }
                }
                None
            } else {
                None
            }
        }

        let test_cases = vec![
            ("list<text>", Some("list")),
            ("set<int>", Some("set")),
            ("map<text,int>", Some("map")),
            ("tuple<text,int,double>", Some("tuple")),
            ("frozen<list<text>>", Some("list")),
            ("frozen<map<text,int>>", Some("map")),
            ("text", None),
            ("int", None),
        ];

        for (type_str, expected) in test_cases {
            assert_eq!(
                parse_collection_kind(type_str),
                expected,
                "Failed for: {}",
                type_str
            );
        }

        println!("✅ Collection parsing tests passed");
    }

    #[test]
    fn test_map_type_extraction() {
        // Test map type extraction
        fn extract_map_types(type_str: &str) -> (String, String) {
            if let Some(start) = type_str.find('<') {
                if let Some(end) = type_str.rfind('>') {
                    let inner = &type_str[start + 1..end];
                    if let Some(comma_pos) = inner.find(',') {
                        let key_type = inner[..comma_pos].trim().to_string();
                        let value_type = inner[comma_pos + 1..].trim().to_string();
                        return (key_type, value_type);
                    }
                }
            }
            ("text".to_string(), "text".to_string())
        }

        let test_cases = vec![
            ("map<text,int>", ("text".to_string(), "int".to_string())),
            (
                "map<uuid, list<text>>",
                ("uuid".to_string(), "list<text>".to_string()),
            ),
            (
                "map<text, map<text,int>>",
                ("text".to_string(), "map<text,int>".to_string()),
            ),
            ("invalid", ("text".to_string(), "text".to_string())),
        ];

        for (type_str, expected) in test_cases {
            assert_eq!(
                extract_map_types(type_str),
                expected,
                "Failed for: {}",
                type_str
            );
        }

        println!("✅ Map type extraction tests passed");
    }

    #[test]
    fn test_index_suggestion_heuristics() {
        // Test index suggestion logic
        fn should_suggest_index(column_name: &str) -> bool {
            column_name.to_lowercase().contains("id")
                || column_name.to_lowercase().ends_with("_id")
                || column_name.to_lowercase().ends_with("_ref")
                || column_name.to_lowercase().contains("email")
                || column_name.to_lowercase().contains("username")
        }

        let test_cases = vec![
            ("id", true),
            ("user_id", true),
            ("customer_ref", true),
            ("email", true),
            ("username", true),
            ("name", false),
            ("description", false),
            ("data", false),
            ("created_at", false),
        ];

        for (column_name, expected) in test_cases {
            assert_eq!(
                should_suggest_index(column_name),
                expected,
                "Failed for: {}",
                column_name
            );
        }

        println!("✅ Index suggestion tests passed");
    }

    #[test]
    fn test_column_classification() {
        // Test column classification logic
        fn is_partition_key_column(column_name: &str, position: usize) -> bool {
            position == 0
                || column_name.to_lowercase().contains("key")
                || column_name.to_lowercase() == "id"
                || column_name.to_lowercase().ends_with("_id")
        }

        fn is_clustering_column(column_name: &str, position: usize) -> bool {
            (position == 1 && !is_partition_key_column(column_name, position))
                || column_name.to_lowercase().contains("time")
                || column_name.to_lowercase().contains("date")
                || column_name.to_lowercase().contains("order")
        }

        let test_cases = vec![
            ("id", 0, true, false),         // partition key
            ("user_id", 1, true, false),    // partition key (has _id)
            ("created_at", 1, false, true), // clustering key (has time)
            ("timestamp", 2, false, true),  // clustering key (has time)
            ("name", 2, false, false),      // regular column
            ("data", 3, false, false),      // regular column
        ];

        for (column_name, position, expected_partition, expected_clustering) in test_cases {
            let is_partition = is_partition_key_column(column_name, position);
            let is_clustering = is_clustering_column(column_name, position);

            assert_eq!(
                is_partition, expected_partition,
                "Partition key failed for: {} at position {}",
                column_name, position
            );
            assert_eq!(
                is_clustering, expected_clustering,
                "Clustering key failed for: {} at position {}",
                column_name, position
            );
        }

        println!("✅ Column classification tests passed");
    }

    #[test]
    fn test_cql_generation_logic() {
        // Test CQL generation core logic
        fn generate_simple_cql(
            keyspace: &str,
            table: &str,
            columns: &[(String, String)],
        ) -> String {
            let mut cql = String::new();
            cql.push_str(&format!("CREATE TABLE {}.{} (\\n", keyspace, table));

            for (name, data_type) in columns {
                cql.push_str(&format!("    {} {},\\n", name, data_type));
            }

            // Simple primary key (first column)
            if !columns.is_empty() {
                cql.push_str(&format!("    PRIMARY KEY ({})", columns[0].0));
            }

            cql.push_str("\\n);");
            cql
        }

        let columns = vec![
            ("id".to_string(), "uuid".to_string()),
            ("name".to_string(), "text".to_string()),
            ("age".to_string(), "int".to_string()),
        ];

        let cql = generate_simple_cql("test_ks", "users", &columns);

        assert!(cql.contains("CREATE TABLE test_ks.users"));
        assert!(cql.contains("id uuid"));
        assert!(cql.contains("name text"));
        assert!(cql.contains("age int"));
        assert!(cql.contains("PRIMARY KEY (id)"));

        println!("✅ CQL generation logic tests passed");
        println!("Generated CQL:\\n{}", cql);
    }

    #[test]
    fn test_type_confidence_calculation() {
        // Test confidence calculation logic
        fn calculate_type_confidence(type_counts: &HashMap<String, usize>) -> f64 {
            if type_counts.is_empty() {
                return 0.0;
            }

            let total_samples: usize = type_counts.values().sum();
            let max_frequency = *type_counts.values().max().unwrap_or(&0);

            max_frequency as f64 / total_samples as f64
        }

        let test_cases = vec![
            (vec![("text", 10)], 1.0),                           // Perfect confidence
            (vec![("text", 8), ("int", 2)], 0.8),                // Good confidence
            (vec![("text", 5), ("int", 5)], 0.5),                // Poor confidence
            (vec![("text", 3), ("int", 3), ("double", 4)], 0.4), // Multiple types
        ];

        for (type_data, expected_confidence) in test_cases {
            let mut type_counts = HashMap::new();
            for (type_name, count) in type_data {
                type_counts.insert(type_name.to_string(), count);
            }

            let confidence = calculate_type_confidence(&type_counts);
            assert!(
                (confidence - expected_confidence).abs() < 0.001,
                "Confidence calculation failed: expected {}, got {}",
                expected_confidence,
                confidence
            );
        }

        println!("✅ Type confidence calculation tests passed");
    }

    #[test]
    fn test_integration_schema_discovery_workflow() {
        println!("🔄 Testing integrated schema discovery workflow...");

        // Step 1: Mock column data
        let column_samples = vec![
            ("id", vec!["uuid", "uuid", "uuid"]),
            ("name", vec!["text", "text", "text"]),
            ("age", vec!["int", "int", "text"]), // Mixed types
            ("email", vec!["text", "text", "text"]),
        ];

        // Step 2: Type inference for each column
        let mut inferred_columns = Vec::new();
        for (column_name, type_samples) in column_samples {
            let mut type_counts = HashMap::new();
            for type_sample in type_samples {
                *type_counts.entry(type_sample.to_string()).or_insert(0) += 1;
            }

            let most_common_type = type_counts
                .iter()
                .max_by_key(|(_, count)| *count)
                .map(|(type_name, _)| type_name.clone())
                .unwrap_or_else(|| "text".to_string());

            let confidence = {
                let total_samples: usize = type_counts.values().sum();
                let max_frequency = *type_counts.values().max().unwrap_or(&0);
                max_frequency as f64 / total_samples as f64
            };

            inferred_columns.push((column_name.to_string(), most_common_type, confidence));
        }

        // Step 3: Column classification
        let mut partition_keys = Vec::new();
        let mut regular_columns = Vec::new();
        let mut suggested_indexes = Vec::new();

        for (i, (column_name, data_type, confidence)) in inferred_columns.iter().enumerate() {
            let is_partition_key = i == 0
                || column_name.to_lowercase().contains("key")
                || column_name == "id"
                || column_name.ends_with("_id");

            if is_partition_key {
                partition_keys.push((column_name.clone(), data_type.clone()));
            } else {
                regular_columns.push((column_name.clone(), data_type.clone()));
            }

            // Index suggestions
            let should_index = column_name.to_lowercase().contains("id")
                || column_name.to_lowercase().contains("email")
                || column_name.to_lowercase().contains("username");

            if should_index && !is_partition_key {
                suggested_indexes.push(format!("{}_idx", column_name));
            }

            println!(
                "Column: {}, Type: {}, Confidence: {:.2}",
                column_name, data_type, confidence
            );
        }

        // Step 4: Generate CQL
        let mut cql = String::new();
        cql.push_str("CREATE TABLE test_ks.discovered_table (\\n");

        for (name, data_type) in &partition_keys {
            cql.push_str(&format!("    {} {},\\n", name, data_type));
        }

        for (name, data_type) in &regular_columns {
            cql.push_str(&format!("    {} {},\\n", name, data_type));
        }

        if !partition_keys.is_empty() {
            let pk_names: Vec<String> = partition_keys
                .iter()
                .map(|(name, _)| name.clone())
                .collect();
            cql.push_str(&format!("    PRIMARY KEY ({})", pk_names.join(", ")));
        }

        cql.push_str("\\n);");

        // Verification
        assert!(
            !partition_keys.is_empty(),
            "Should have detected at least one partition key"
        );
        assert!(cql.contains("CREATE TABLE test_ks.discovered_table"));
        assert!(cql.contains("PRIMARY KEY"));
        assert!(
            !suggested_indexes.is_empty(),
            "Should have suggested some indexes"
        );

        println!("✅ Integration workflow test passed");
        println!("Generated CQL:\\n{}", cql);
        println!("Suggested indexes: {:?}", suggested_indexes);
        println!("Partition keys: {:?}", partition_keys);
        println!("Regular columns: {:?}", regular_columns);
    }
}
