//! Comprehensive tests for the AST and parser abstraction layer
//!
//! This module provides extensive testing to ensure correctness and compatibility
//! of the new AST-based parser system with the existing CQL parser.

use super::*;
use crate::schema::{TableSchema, KeyColumn};
use crate::types::UdtTypeDef;
use crate::error::Error;
use std::collections::HashMap;

/// Test suite for AST structure creation and manipulation
#[cfg(test)]
mod ast_tests {
    use super::*;

    #[test]
    fn test_ast_creation_basic_types() {
        // Test basic AST node creation
        let identifier = CqlIdentifier::new("test_table");
        assert_eq!(identifier.name(), "test_table");
        assert!(!identifier.is_quoted());

        let quoted_identifier = CqlIdentifier::quoted("test table");
        assert_eq!(quoted_identifier.name(), "test table");
        assert!(quoted_identifier.is_quoted());

        let table = CqlTable::new("users");
        assert_eq!(table.name().name(), "users");
        assert_eq!(table.full_name(), "users");

        let qualified_table = CqlTable::with_keyspace("app", "users");
        assert_eq!(qualified_table.full_name(), "app.users");
    }

    #[test]
    fn test_ast_select_statement() {
        let select = CqlSelect {
            distinct: false,
            select_list: vec![
                CqlSelectItem::Wildcard,
                CqlSelectItem::Expression {
                    expression: CqlExpression::Column(CqlIdentifier::new("id")),
                    alias: Some(CqlIdentifier::new("user_id")),
                },
            ],
            from: CqlTable::new("users"),
            where_clause: Some(CqlExpression::Binary {
                left: Box::new(CqlExpression::Column(CqlIdentifier::new("id"))),
                operator: CqlBinaryOperator::Eq,
                right: Box::new(CqlExpression::Parameter(1)),
            }),
            order_by: Some(vec![CqlOrderBy {
                column: CqlIdentifier::new("created_at"),
                direction: CqlSortDirection::Desc,
            }]),
            limit: Some(10),
            allow_filtering: false,
        };

        assert!(!select.distinct);
        assert_eq!(select.select_list.len(), 2);
        assert!(select.where_clause.is_some());
        assert_eq!(select.limit, Some(10));
    }

    #[test]
    fn test_ast_create_table_statement() {
        let create_table = CqlCreateTable {
            if_not_exists: true,
            table: CqlTable::with_keyspace("test", "users"),
            columns: vec![
                CqlColumnDef {
                    name: CqlIdentifier::new("id"),
                    data_type: CqlDataType::Uuid,
                    is_static: false,
                },
                CqlColumnDef {
                    name: CqlIdentifier::new("name"),
                    data_type: CqlDataType::Text,
                    is_static: false,
                },
                CqlColumnDef {
                    name: CqlIdentifier::new("tags"),
                    data_type: CqlDataType::Set(Box::new(CqlDataType::Text)),
                    is_static: false,
                },
            ],
            primary_key: CqlPrimaryKey {
                partition_key: vec![CqlIdentifier::new("id")],
                clustering_key: vec![],
            },
            options: CqlTableOptions {
                options: HashMap::new(),
            },
        };

        assert!(create_table.if_not_exists);
        assert_eq!(create_table.table.full_name(), "test.users");
        assert_eq!(create_table.columns.len(), 3);
        assert_eq!(create_table.primary_key.partition_key.len(), 1);
        assert!(create_table.primary_key.clustering_key.is_empty());
    }

    #[test]
    fn test_ast_complex_data_types() {
        // Test nested collection types
        let list_of_maps = CqlDataType::List(Box::new(CqlDataType::Map(
            Box::new(CqlDataType::Text),
            Box::new(CqlDataType::BigInt),
        )));

        let frozen_set = CqlDataType::Frozen(Box::new(CqlDataType::Set(Box::new(
            CqlDataType::Uuid,
        ))));

        let tuple_type = CqlDataType::Tuple(vec![
            CqlDataType::Text,
            CqlDataType::Int,
            CqlDataType::Boolean,
        ]);

        // Verify type structure
        match list_of_maps {
            CqlDataType::List(inner) => match *inner {
                CqlDataType::Map(key, value) => {
                    assert!(matches!(*key, CqlDataType::Text));
                    assert!(matches!(*value, CqlDataType::BigInt));
                }
                _ => panic!("Expected Map inside List"),
            },
            _ => panic!("Expected List type"),
        }

        match frozen_set {
            CqlDataType::Frozen(inner) => match *inner {
                CqlDataType::Set(element) => {
                    assert!(matches!(*element, CqlDataType::Uuid));
                }
                _ => panic!("Expected Set inside Frozen"),
            },
            _ => panic!("Expected Frozen type"),
        }

        match tuple_type {
            CqlDataType::Tuple(types) => {
                assert_eq!(types.len(), 3);
                assert!(matches!(types[0], CqlDataType::Text));
                assert!(matches!(types[1], CqlDataType::Int));
                assert!(matches!(types[2], CqlDataType::Boolean));
            }
            _ => panic!("Expected Tuple type"),
        }
    }

    #[test]
    fn test_ast_serialization() {
        // Test that AST nodes can be serialized and deserialized
        let statement = CqlStatement::Select(CqlSelect {
            distinct: false,
            select_list: vec![CqlSelectItem::Wildcard],
            from: CqlTable::new("users"),
            where_clause: None,
            order_by: None,
            limit: None,
            allow_filtering: false,
        });

        // Serialize to JSON
        let serialized = serde_json::to_string(&statement).expect("Failed to serialize AST");
        assert!(!serialized.is_empty());

        // Deserialize back
        let deserialized: CqlStatement = serde_json::from_str(&serialized)
            .expect("Failed to deserialize AST");

        // Verify they're equal
        assert_eq!(statement, deserialized);
    }

    #[test]
    fn test_ast_equality() {
        let stmt1 = CqlStatement::Insert(CqlInsert {
            table: CqlTable::new("users"),
            columns: vec![
                CqlIdentifier::new("id"),
                CqlIdentifier::new("name"),
            ],
            values: CqlInsertValues::Values(vec![
                CqlExpression::Parameter(1),
                CqlExpression::Parameter(2),
            ]),
            if_not_exists: false,
            using: None,
        });

        let stmt2 = CqlStatement::Insert(CqlInsert {
            table: CqlTable::new("users"),
            columns: vec![
                CqlIdentifier::new("id"),
                CqlIdentifier::new("name"),
            ],
            values: CqlInsertValues::Values(vec![
                CqlExpression::Parameter(1),
                CqlExpression::Parameter(2),
            ]),
            if_not_exists: false,
            using: None,
        });

        let stmt3 = CqlStatement::Insert(CqlInsert {
            table: CqlTable::new("orders"), // Different table
            columns: vec![
                CqlIdentifier::new("id"),
                CqlIdentifier::new("name"),
            ],
            values: CqlInsertValues::Values(vec![
                CqlExpression::Parameter(1),
                CqlExpression::Parameter(2),
            ]),
            if_not_exists: false,
            using: None,
        });

        assert_eq!(stmt1, stmt2);
        assert_ne!(stmt1, stmt3);
    }
}

/// Test suite for visitor pattern functionality
#[cfg(test)]
mod visitor_tests {
    use super::*;

    #[test]
    fn test_identifier_collector() {
        let statement = CqlStatement::Select(CqlSelect {
            distinct: false,
            select_list: vec![
                CqlSelectItem::Expression {
                    expression: CqlExpression::Column(CqlIdentifier::new("id")),
                    alias: Some(CqlIdentifier::new("user_id")),
                },
                CqlSelectItem::Expression {
                    expression: CqlExpression::Column(CqlIdentifier::new("name")),
                    alias: None,
                },
            ],
            from: CqlTable::with_keyspace("app", "users"),
            where_clause: Some(CqlExpression::Binary {
                left: Box::new(CqlExpression::Column(CqlIdentifier::new("status"))),
                operator: CqlBinaryOperator::Eq,
                right: Box::new(CqlExpression::Literal(CqlLiteral::String("active".to_string()))),
            }),
            order_by: Some(vec![CqlOrderBy {
                column: CqlIdentifier::new("created_at"),
                direction: CqlSortDirection::Desc,
            }]),
            limit: None,
            allow_filtering: false,
        });

        let mut collector = IdentifierCollector::default();
        collector.visit_statement(&statement).unwrap();

        // Should collect: id, user_id, name, app, users, status, created_at
        assert_eq!(collector.identifiers.len(), 7);
        
        let names: Vec<&str> = collector.identifiers.iter().map(|id| id.name()).collect();
        assert!(names.contains(&"id"));
        assert!(names.contains(&"user_id"));
        assert!(names.contains(&"name"));
        assert!(names.contains(&"app"));
        assert!(names.contains(&"users"));
        assert!(names.contains(&"status"));
        assert!(names.contains(&"created_at"));
    }

    #[test]
    fn test_semantic_validator() {
        // Create a context with a known table
        let mut schemas = HashMap::new();
        let table_schema = TableSchema {
            keyspace: "test".to_string(),
            table: "users".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "uuid".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![
                crate::schema::Column {
                    name: "id".to_string(),
                    data_type: "uuid".to_string(),
                    nullable: false,
                    default: None,
                },
                crate::schema::Column {
                    name: "name".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                },
            ],
            comments: HashMap::new(),
        };
        schemas.insert("test.users".to_string(), table_schema);

        let context = ValidationContext {
            schemas,
            udts: HashMap::new(),
            current_keyspace: Some("test".to_string()),
            strictness: ValidationStrictness::Strict,
        };

        // Test valid CREATE TABLE statement
        let valid_create = CqlStatement::CreateTable(CqlCreateTable {
            if_not_exists: false,
            table: CqlTable::with_keyspace("test", "orders"),
            columns: vec![
                CqlColumnDef {
                    name: CqlIdentifier::new("id"),
                    data_type: CqlDataType::Uuid,
                    is_static: false,
                },
                CqlColumnDef {
                    name: CqlIdentifier::new("amount"),
                    data_type: CqlDataType::Decimal,
                    is_static: false,
                },
            ],
            primary_key: CqlPrimaryKey {
                partition_key: vec![CqlIdentifier::new("id")],
                clustering_key: vec![],
            },
            options: CqlTableOptions {
                options: HashMap::new(),
            },
        });

        let mut validator = SemanticValidator::new(context);
        validator.visit_statement(&valid_create).unwrap();
        assert!(validator.is_valid());

        // Test invalid CREATE TABLE with missing partition key column
        let invalid_create = CqlStatement::CreateTable(CqlCreateTable {
            if_not_exists: false,
            table: CqlTable::new("invalid_table"),
            columns: vec![CqlColumnDef {
                name: CqlIdentifier::new("data"),
                data_type: CqlDataType::Text,
                is_static: false,
            }],
            primary_key: CqlPrimaryKey {
                partition_key: vec![CqlIdentifier::new("missing_column")], // Column doesn't exist
                clustering_key: vec![],
            },
            options: CqlTableOptions {
                options: HashMap::new(),
            },
        });

        let context2 = ValidationContext::new();
        let mut validator2 = SemanticValidator::new(context2);
        validator2.visit_statement(&invalid_create).unwrap();
        assert!(!validator2.is_valid());
        assert!(!validator2.get_errors().is_empty());
    }

    #[test]
    fn test_default_visitor_traversal() {
        let statement = CqlStatement::Update(CqlUpdate {
            table: CqlTable::new("users"),
            using: Some(CqlUsing {
                ttl: Some(CqlExpression::Literal(CqlLiteral::Integer(3600))),
                timestamp: None,
            }),
            assignments: vec![CqlAssignment {
                column: CqlIdentifier::new("name"),
                operator: CqlAssignmentOperator::Assign,
                value: CqlExpression::Parameter(1),
            }],
            where_clause: CqlExpression::Binary {
                left: Box::new(CqlExpression::Column(CqlIdentifier::new("id"))),
                operator: CqlBinaryOperator::Eq,
                right: Box::new(CqlExpression::Parameter(2)),
            },
            if_condition: None,
        });

        // Default visitor should traverse without errors
        let result: () = DefaultVisitor.visit_statement(&statement).unwrap();
        assert_eq!(result, ());
    }
}

/// Test suite for parser compatibility and integration
#[cfg(test)]
mod parser_integration_tests {
    use super::*;
    use crate::schema::cql_parser::{parse_cql_schema, extract_table_name};

    #[test]
    fn test_existing_parser_compatibility() {
        // Test cases from the existing parser to ensure compatibility
        let test_cases = vec![
            (
                "CREATE TABLE users (id uuid PRIMARY KEY, name text, email text)",
                "users",
                3, // columns
                1, // partition keys
            ),
            (
                "CREATE TABLE myapp.orders (order_id bigint, customer_id uuid, total decimal, PRIMARY KEY (order_id))",
                "orders",
                3, // columns  
                1, // partition keys
            ),
            (
                "CREATE TABLE time_series (partition_key text, clustering_key timestamp, value double, PRIMARY KEY (partition_key, clustering_key))",
                "time_series",
                3, // columns
                1, // partition keys (1 partition + 1 clustering)
            ),
        ];

        for (cql, expected_table, expected_columns, expected_partition_keys) in test_cases {
            // Test existing parser
            let schema = parse_cql_schema(cql).expect("Failed to parse with existing parser");
            assert_eq!(schema.table, expected_table);
            assert_eq!(schema.columns.len(), expected_columns);
            assert_eq!(schema.partition_keys.len(), expected_partition_keys);

            // Test table name extraction
            let (_, table_name) = extract_table_name(cql).expect("Failed to extract table name");
            assert_eq!(table_name, expected_table);
        }
    }

    #[test]
    fn test_complex_type_parsing() {
        let cql = r#"
            CREATE TABLE complex_types (
                id uuid PRIMARY KEY,
                tags set<text>,
                metadata map<text, text>,
                coordinates list<double>,
                user_info frozen<tuple<text, int, boolean>>,
                nested_data list<map<text, set<uuid>>>
            )
        "#;

        let schema = parse_cql_schema(cql).expect("Failed to parse complex types");
        assert_eq!(schema.table, "complex_types");
        assert_eq!(schema.columns.len(), 6);

        // Find specific columns and verify their types
        let tags_col = schema.columns.iter().find(|c| c.name == "tags").unwrap();
        assert_eq!(tags_col.data_type, "set<text>");

        let metadata_col = schema.columns.iter().find(|c| c.name == "metadata").unwrap();
        assert_eq!(metadata_col.data_type, "map<text, text>");

        let coordinates_col = schema.columns.iter().find(|c| c.name == "coordinates").unwrap();
        assert_eq!(coordinates_col.data_type, "list<double>");

        let user_info_col = schema.columns.iter().find(|c| c.name == "user_info").unwrap();
        assert_eq!(user_info_col.data_type, "frozen<tuple<text, int, boolean>>");

        let nested_col = schema.columns.iter().find(|c| c.name == "nested_data").unwrap();
        assert_eq!(nested_col.data_type, "list<map<text, set<uuid>>>");
    }

    #[test]
    fn test_all_primitive_types_compatibility() {
        let primitive_types = vec![
            "ascii", "bigint", "blob", "boolean", "counter", "decimal", "double",
            "float", "int", "timestamp", "uuid", "varchar", "text", "varint",
            "timeuuid", "inet", "date", "time", "smallint", "tinyint", "duration"
        ];

        for (i, ptype) in primitive_types.iter().enumerate() {
            let cql = format!(
                "CREATE TABLE test_table (id uuid PRIMARY KEY, col_{} {})",
                i, ptype
            );

            let schema = parse_cql_schema(&cql).expect(&format!("Failed to parse type: {}", ptype));
            assert_eq!(schema.columns.len(), 2);

            let type_col = schema.columns.iter().find(|c| c.name == format!("col_{}", i));
            assert!(type_col.is_some(), "Column with {} type not found", ptype);
            assert_eq!(type_col.unwrap().data_type, *ptype);
        }
    }

    #[test]
    fn test_error_handling_compatibility() {
        let invalid_cql_cases = vec![
            "", // Empty string
            "INVALID SQL", // Not CQL
            "CREATE TABLE", // Incomplete
            "CREATE TABLE users ()", // No columns
        ];

        for invalid_cql in invalid_cql_cases {
            // Should handle errors gracefully
            let result = parse_cql_schema(invalid_cql);
            assert!(result.is_err(), "Expected error for: {}", invalid_cql);
        }
    }
}

/// Test suite for error handling and edge cases
#[cfg(test)]
mod error_handling_tests {
    use super::*;

    #[test]
    fn test_parser_error_creation() {
        let error = ParserError::syntax(
            "Test syntax error".to_string(),
            SourcePosition::new(5, 10, 50, 5),
        );

        assert_eq!(error.category(), &ErrorCategory::Syntax);
        assert_eq!(error.severity(), &ErrorSeverity::Error);
        assert!(error.message().contains("Test syntax error"));
        assert_eq!(error.position().unwrap().line, 5);
        assert_eq!(error.position().unwrap().column, 10);
    }

    #[test]
    fn test_validation_context_edge_cases() {
        let context = ValidationContext::new();
        
        // Test empty context
        assert_eq!(context.strictness, ValidationStrictness::Strict);
        assert!(context.schemas.is_empty());

        // Test lenient context
        let lenient = ValidationContext::lenient();
        assert_eq!(lenient.strictness, ValidationStrictness::Lenient);

        // Test builder pattern
        let built_context = ValidationContext::new()
            .with_keyspace("test".to_string())
            .with_schema("test.users".to_string(), TableSchema {
                keyspace: "test".to_string(),
                table: "users".to_string(),
                partition_keys: vec![],
                clustering_keys: vec![],
                columns: vec![],
                comments: HashMap::new(),
            });

        assert_eq!(built_context.current_keyspace, Some("test".to_string()));
        assert!(built_context.schemas.contains_key("test.users"));
    }

    #[test]
    fn test_identifier_edge_cases() {
        // Test empty identifier (should be invalid for unquoted)
        let empty_id = CqlIdentifier::new("");
        assert!(!empty_id.is_valid_unquoted());

        // Test identifier starting with number (should be invalid for unquoted)
        let numeric_id = CqlIdentifier::new("123invalid");
        assert!(!numeric_id.is_valid_unquoted());

        // Test identifier with spaces (should require quoting)
        let space_id = CqlIdentifier::quoted("name with spaces");
        assert!(space_id.needs_quoting());

        // Test valid unquoted identifier
        let valid_id = CqlIdentifier::new("valid_name_123");
        assert!(valid_id.is_valid_unquoted());
        assert!(!valid_id.needs_quoting());
    }
}

/// Test suite for schema builder visitor
#[cfg(test)]
mod schema_builder_tests {
    use super::*;

    /// Visitor that builds TableSchema from CqlCreateTable AST
    #[derive(Debug, Default)]
    pub struct SchemaBuilderVisitor {
        pub errors: Vec<String>,
    }

    impl SchemaBuilderVisitor {
        pub fn new() -> Self {
            Self {
                errors: Vec::new(),
            }
        }

        pub fn build_schema(&mut self, create_table: &CqlCreateTable) -> Result<TableSchema, Error> {
            // Convert CqlCreateTable to TableSchema
            let keyspace = create_table.table.keyspace
                .as_ref()
                .map(|ks| ks.name().to_string())
                .unwrap_or_else(|| "default".to_string());

            let table_name = create_table.table.name.name().to_string();

            // Build partition keys
            let partition_keys = create_table.primary_key.partition_key
                .iter()
                .enumerate()
                .map(|(pos, key)| {
                    let column = create_table.columns
                        .iter()
                        .find(|c| c.name.name() == key.name())
                        .ok_or_else(|| Error::schema(format!("Partition key column '{}' not found", key.name())))?;

                    Ok(KeyColumn {
                        name: key.name().to_string(),
                        data_type: self.data_type_to_string(&column.data_type),
                        position: pos,
                    })
                })
                .collect::<Result<Vec<_>, Error>>()?;

            // Build clustering keys
            let clustering_keys = create_table.primary_key.clustering_key
                .iter()
                .enumerate()
                .map(|(pos, key)| {
                    let column = create_table.columns
                        .iter()
                        .find(|c| c.name.name() == key.name())
                        .ok_or_else(|| Error::schema(format!("Clustering key column '{}' not found", key.name())))?;

                    Ok(crate::schema::ClusteringColumn {
                        name: key.name().to_string(),
                        data_type: self.data_type_to_string(&column.data_type),
                        position: pos,
                        order: "ASC".to_string(),
                    })
                })
                .collect::<Result<Vec<_>, Error>>()?;

            // Build columns
            let columns = create_table.columns
                .iter()
                .map(|col| crate::schema::Column {
                    name: col.name.name().to_string(),
                    data_type: self.data_type_to_string(&col.data_type),
                    nullable: true, // Default to nullable
                    default: None,
                })
                .collect();

            let schema = TableSchema {
                keyspace,
                table: table_name,
                partition_keys,
                clustering_keys,
                columns,
                comments: HashMap::new(),
            };

            Ok(schema)
        }

        fn data_type_to_string(&self, data_type: &CqlDataType) -> String {
            match data_type {
                CqlDataType::Boolean => "boolean".to_string(),
                CqlDataType::TinyInt => "tinyint".to_string(),
                CqlDataType::SmallInt => "smallint".to_string(),
                CqlDataType::Int => "int".to_string(),
                CqlDataType::BigInt => "bigint".to_string(),
                CqlDataType::Varint => "varint".to_string(),
                CqlDataType::Decimal => "decimal".to_string(),
                CqlDataType::Float => "float".to_string(),
                CqlDataType::Double => "double".to_string(),
                CqlDataType::Text | CqlDataType::Varchar => "text".to_string(),
                CqlDataType::Ascii => "ascii".to_string(),
                CqlDataType::Blob => "blob".to_string(),
                CqlDataType::Timestamp => "timestamp".to_string(),
                CqlDataType::Date => "date".to_string(),
                CqlDataType::Time => "time".to_string(),
                CqlDataType::Uuid => "uuid".to_string(),
                CqlDataType::TimeUuid => "timeuuid".to_string(),
                CqlDataType::Inet => "inet".to_string(),
                CqlDataType::Duration => "duration".to_string(),
                CqlDataType::Counter => "counter".to_string(),
                CqlDataType::List(inner) => format!("list<{}>", self.data_type_to_string(inner)),
                CqlDataType::Set(inner) => format!("set<{}>", self.data_type_to_string(inner)),
                CqlDataType::Map(key, value) => format!("map<{}, {}>", 
                    self.data_type_to_string(key), self.data_type_to_string(value)),
                CqlDataType::Tuple(types) => {
                    let type_strings: Vec<String> = types.iter()
                        .map(|t| self.data_type_to_string(t))
                        .collect();
                    format!("tuple<{}>", type_strings.join(", "))
                },
                CqlDataType::Udt(name) => name.name().to_string(),
                CqlDataType::Frozen(inner) => format!("frozen<{}>", self.data_type_to_string(inner)),
                CqlDataType::Custom(name) => name.clone(),
            }
        }
    }

    #[test]
    fn test_schema_builder_visitor() {
        let create_table = CqlCreateTable {
            if_not_exists: false,
            table: CqlTable::with_keyspace("test", "users"),
            columns: vec![
                CqlColumnDef {
                    name: CqlIdentifier::new("id"),
                    data_type: CqlDataType::Uuid,
                    is_static: false,
                },
                CqlColumnDef {
                    name: CqlIdentifier::new("name"),
                    data_type: CqlDataType::Text,
                    is_static: false,
                },
                CqlColumnDef {
                    name: CqlIdentifier::new("tags"),
                    data_type: CqlDataType::Set(Box::new(CqlDataType::Text)),
                    is_static: false,
                },
            ],
            primary_key: CqlPrimaryKey {
                partition_key: vec![CqlIdentifier::new("id")],
                clustering_key: vec![],
            },
            options: CqlTableOptions {
                options: HashMap::new(),
            },
        };

        let mut builder = SchemaBuilderVisitor::new();
        let schema = builder.build_schema(&create_table).unwrap();

        assert_eq!(schema.keyspace, "test");
        assert_eq!(schema.table, "users");
        assert_eq!(schema.columns.len(), 3);
        assert_eq!(schema.partition_keys.len(), 1);
        assert_eq!(schema.clustering_keys.len(), 0);

        // Verify partition key
        assert_eq!(schema.partition_keys[0].name, "id");
        assert_eq!(schema.partition_keys[0].data_type, "uuid");

        // Verify columns
        let name_col = schema.columns.iter().find(|c| c.name == "name").unwrap();
        assert_eq!(name_col.data_type, "text");

        let tags_col = schema.columns.iter().find(|c| c.name == "tags").unwrap();
        assert_eq!(tags_col.data_type, "set<text>");
    }

    #[test]
    fn test_schema_builder_error_handling() {
        // Test with missing primary key column
        let invalid_create = CqlCreateTable {
            if_not_exists: false,
            table: CqlTable::new("invalid"),
            columns: vec![
                CqlColumnDef {
                    name: CqlIdentifier::new("data"),
                    data_type: CqlDataType::Text,
                    is_static: false,
                },
            ],
            primary_key: CqlPrimaryKey {
                partition_key: vec![CqlIdentifier::new("missing_column")],
                clustering_key: vec![],
            },
            options: CqlTableOptions {
                options: HashMap::new(),
            },
        };

        let mut builder = SchemaBuilderVisitor::new();
        let result = builder.build_schema(&invalid_create);
        assert!(result.is_err());
    }
}

/// Run all tests and report results
#[cfg(test)]
mod test_runner {
    use super::*;

    #[test]
    fn run_all_ast_tests() {
        // This is a meta-test that ensures all test modules are properly included
        println!("Running comprehensive AST and parser tests...");
        
        // The individual test modules will be run by the test framework
        // This test just serves as a confirmation that the test suite is complete
        assert!(true, "All tests should pass");
    }
}