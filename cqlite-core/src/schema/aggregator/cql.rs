//! CQL schema handling for the schema aggregator.
//!
//! Parses CQL files containing `CREATE TYPE` and `CREATE TABLE` statements
//! (plus `USE` / `CREATE KEYSPACE` for keyspace context) into the
//! intermediate [`ParsedSchema`] format.

use std::collections::HashMap;
use std::path::Path;

use crate::error::{Error, Result};
use crate::schema::{
    cql_parser::{classify_statement, parse_create_type, split_cql_statements, StatementType},
    parse_cql_schema,
};
use crate::types::UdtTypeDef;

use super::{ParsedSchema, SchemaAggregator};

/// Extract keyspace name from USE statement
/// Example: "USE test_basic;" -> Some("test_basic")
/// Example: "USE \"test-basic\";" -> Some("test-basic")
pub(super) fn extract_use_keyspace(statement: &str) -> Option<String> {
    let normalized = statement.trim().to_lowercase();
    if !normalized.starts_with("use ") {
        return None;
    }

    // Extract keyspace name after "USE "
    let after_use = statement.trim()[4..].trim();
    let mut ks_name = after_use.trim_end_matches(';').trim();

    // Strip quotes from keyspace name if present
    if ks_name.starts_with('"') && ks_name.ends_with('"') && ks_name.len() > 1 {
        ks_name = &ks_name[1..ks_name.len() - 1];
    }

    if ks_name.is_empty() {
        None
    } else {
        Some(ks_name.to_string())
    }
}

/// Extract keyspace name from CREATE KEYSPACE statement
/// Example: "CREATE KEYSPACE IF NOT EXISTS test_basic WITH ..." -> Some("test_basic")
/// Example: "CREATE KEYSPACE \"test-basic\" WITH ..." -> Some("test-basic")
pub(super) fn extract_create_keyspace_name(statement: &str) -> Option<String> {
    let normalized = statement.trim().to_lowercase();
    if !normalized.starts_with("create keyspace") {
        return None;
    }

    // Split by whitespace and find the keyspace name
    let words: Vec<&str> = statement.split_whitespace().collect();

    // Pattern: CREATE KEYSPACE [IF NOT EXISTS] <name> ...
    let start_idx = if words.len() > 2 && words[2].eq_ignore_ascii_case("if") {
        5 // Skip "CREATE KEYSPACE IF NOT EXISTS"
    } else {
        2 // Skip "CREATE KEYSPACE"
    };

    if words.len() > start_idx {
        let mut ks_name = words[start_idx].trim();

        // Strip quotes from keyspace name if present
        if ks_name.starts_with('"') && ks_name.ends_with('"') && ks_name.len() > 1 {
            ks_name = &ks_name[1..ks_name.len() - 1];
        }

        Some(ks_name.to_string())
    } else {
        None
    }
}

impl SchemaAggregator {
    /// Parse a CQL file (supports multiple statements: CREATE TYPE and CREATE TABLE)
    pub(super) async fn parse_cql_file(&self, path: &Path) -> Result<Option<ParsedSchema>> {
        let content = std::fs::read_to_string(path)?;

        // Split file content into individual statements
        let statements = split_cql_statements(&content);

        if statements.is_empty() {
            return Ok(None);
        }

        let mut keyspace: Option<String> = None;
        let mut tables = HashMap::new();
        let mut udts = HashMap::new();
        let mut errors = Vec::new();

        // Separate CREATE TYPE from CREATE TABLE statements
        let mut create_type_stmts = Vec::new();
        let mut create_table_stmts = Vec::new();

        for statement in &statements {
            match classify_statement(statement) {
                StatementType::CreateType => create_type_stmts.push(statement.as_str()),
                StatementType::CreateTable => create_table_stmts.push(statement.as_str()),
                StatementType::Other(ref kind) if kind == "use" => {
                    // Extract keyspace name from USE statement
                    if let Some(ks_name) = extract_use_keyspace(statement) {
                        keyspace = Some(ks_name);
                    }
                }
                StatementType::Other(ref kind) if kind == "create" => {
                    // Handle CREATE KEYSPACE statements - extract keyspace name
                    if let Some(ks_name) = extract_create_keyspace_name(statement) {
                        // Only set keyspace if not already set by USE statement
                        if keyspace.is_none() {
                            keyspace = Some(ks_name);
                        }
                    }
                }
                StatementType::Other(_kind) => {
                    // Skip other statement types silently (e.g., ALTER, DROP, comments)
                }
            }
        }

        // Parse CREATE TYPE statements first (UDTs must be registered before tables)
        for stmt in create_type_stmts {
            match parse_create_type(stmt) {
                Ok((_, (type_name, type_keyspace, fields))) => {
                    // Determine keyspace (use from statement or inherit from file context)
                    let udt_keyspace = type_keyspace.unwrap_or_else(|| {
                        keyspace.clone().unwrap_or_else(|| "default".to_string())
                    });

                    // Update keyspace if not set
                    if keyspace.is_none() {
                        keyspace = Some(udt_keyspace.clone());
                    }

                    // Build UdtTypeDef
                    let mut udt_def = UdtTypeDef::new(udt_keyspace.clone(), type_name.clone());
                    for (field_name, field_type_str) in fields {
                        // Parse field type
                        let field_type = crate::schema::CqlType::parse(&field_type_str)?;
                        udt_def = udt_def.with_field(field_name, field_type, true);
                    }

                    // Key by fully-qualified name (keyspace.typename) to avoid multi-keyspace collisions
                    let qualified_name = format!("{}.{}", udt_keyspace, type_name);
                    udts.insert(qualified_name, udt_def);
                }
                Err(e) => {
                    errors.push(format!(
                        "Failed to parse CREATE TYPE in {}: {:?}",
                        path.display(),
                        e
                    ));
                }
            }
        }

        // Parse CREATE TABLE statements
        for stmt in create_table_stmts {
            match parse_cql_schema(stmt) {
                Ok(mut table_schema) => {
                    // Override keyspace with the one from USE statement or CREATE KEYSPACE
                    // Only override if the table doesn't have an explicit qualified name
                    if table_schema.keyspace == "default" {
                        if let Some(ref active_keyspace) = keyspace {
                            table_schema.keyspace = active_keyspace.clone();
                        }
                    }

                    // Update keyspace if not set (from first table's explicit keyspace)
                    if keyspace.is_none() {
                        keyspace = Some(table_schema.keyspace.clone());
                    }

                    // Key by fully-qualified name (keyspace.table) to avoid multi-keyspace collisions
                    let qualified_name =
                        format!("{}.{}", table_schema.keyspace, table_schema.table);
                    tables.insert(qualified_name, table_schema);
                }
                Err(e) => {
                    errors.push(format!(
                        "Failed to parse CREATE TABLE in {}: {}",
                        path.display(),
                        e
                    ));
                }
            }
        }

        // If there were errors and no successful parses, return error
        if !errors.is_empty() && tables.is_empty() && udts.is_empty() {
            return Err(Error::CqlParse(format!(
                "Failed to parse CQL file {}: {}",
                path.display(),
                errors.join("; ")
            )));
        }

        // If no tables or UDTs were parsed, and statements exist, treat as error
        // This catches truly invalid CQL that doesn't match any expected pattern
        if tables.is_empty() && udts.is_empty() && !statements.is_empty() {
            // Check if all statements are legitimate "other" types (USE, CREATE KEYSPACE, etc.)
            // or if there are truly unrecognized/invalid statements
            let legitimate_keywords = [
                "use", "create", "alter", "drop", "grant", "revoke", "truncate",
            ];
            let has_invalid_statement = statements.iter().any(|stmt| {
                let normalized = stmt.trim().to_lowercase();
                let first_word = normalized.split_whitespace().next().unwrap_or("");

                // If it's a CREATE statement that wasn't successfully parsed, it's invalid
                if normalized.starts_with("create ") {
                    return true;
                }

                // If it's not a legitimate keyword, it's invalid
                !legitimate_keywords.contains(&first_word)
            });

            // If there are invalid statements, return an error
            if has_invalid_statement {
                return Err(Error::CqlParse(format!(
                    "Failed to parse CQL file {}: No valid CREATE TABLE or CREATE TYPE statements found",
                    path.display()
                )));
            }
        }

        // Determine final keyspace (use first discovered or default)
        let final_keyspace = keyspace.unwrap_or_else(|| "default".to_string());

        Ok(Some(ParsedSchema {
            keyspace: final_keyspace,
            tables,
            udts,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::*;
    use super::super::{AggregatorConfig, SchemaAggregator};
    use crate::platform::Platform;
    use crate::schema::registry::{SchemaRegistry, SchemaRegistryConfig};
    use crate::schema::UdtRegistry;
    use crate::Config;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    #[tokio::test]
    async fn test_load_single_cql_file() {
        let (mut aggregator, temp_dir) = setup_test_aggregator().await;

        let cql_content = r#"
        CREATE TABLE test_ks.products (
            id uuid PRIMARY KEY,
            name text,
            price decimal
        );
        "#;

        let cql_path = write_file(temp_dir.path(), "products.cql", cql_content);
        let result = aggregator.load_from_paths(&[cql_path]).await.unwrap();

        assert_eq!(result.schemas_loaded, 1);
        assert_eq!(result.udts_loaded, 0);
        assert!(result.errors.is_empty());
    }

    #[tokio::test]
    async fn test_multi_statement_cql_file_with_create_type_and_create_table() {
        // Create aggregator without UDT validation to avoid dependency ordering issues
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        let registry_config = SchemaRegistryConfig::default();
        let registry = Arc::new(RwLock::new(
            SchemaRegistry::new(registry_config, platform, config)
                .await
                .unwrap(),
        ));
        let udt_registry = Arc::new(RwLock::new(UdtRegistry::new()));

        let mut aggregator = SchemaAggregator::new(
            registry,
            udt_registry,
            AggregatorConfig {
                graceful_degradation: true,
                validate_udt_dependencies: false, // Disable to avoid HashMap ordering issues
            },
        );

        // Multi-statement CQL file with CREATE TYPE and CREATE TABLE
        let cql_content = r#"
        -- Test schema with UDTs
        CREATE TYPE test_ks.address (
            street text,
            city text,
            zip_code int
        );

        CREATE TYPE test_ks.contact_info (
            email text,
            phone text,
            address address
        );

        CREATE TABLE test_ks.users (
            id uuid PRIMARY KEY,
            name text,
            contact contact_info
        );
        "#;

        let cql_path = write_file(temp_dir.path(), "schema.cql", cql_content);
        let result = aggregator.load_from_paths(&[cql_path]).await.unwrap();

        // Verify both UDTs and table were loaded
        assert_eq!(result.udts_loaded, 2, "Expected 2 UDTs to be loaded");
        assert_eq!(result.schemas_loaded, 1, "Expected 1 table to be loaded");
        assert!(
            result.errors.is_empty(),
            "Expected no errors, got: {:?}",
            result.errors
        );

        // Verify UDTs were registered
        let udt_registry = aggregator.udt_registry.read().await;
        assert!(
            udt_registry.contains_udt("test_ks", "address"),
            "address UDT should be registered"
        );
        assert!(
            udt_registry.contains_udt("test_ks", "contact_info"),
            "contact_info UDT should be registered"
        );

        // Verify table was registered
        let registry = aggregator.registry.read().await;
        let schema = registry.get_schema("test_ks", "users").await.unwrap();
        assert_eq!(schema.table, "users");
        assert_eq!(schema.columns.len(), 3);
    }

    /// Issue #761: a table column referencing an undefined UDT must fail to load
    /// with an error naming the missing UDT (top-level reference).
    #[tokio::test]
    async fn test_table_referencing_undefined_udt_top_level_fails() {
        let (mut aggregator, temp_dir) = setup_test_aggregator().await;

        let cql_content = r#"
        CREATE TABLE test_ks.users (
            id uuid PRIMARY KEY,
            name text,
            contact ContactInfo
        );
        "#;

        let cql_path = write_file(temp_dir.path(), "schema.cql", cql_content);
        let result = aggregator.load_from_paths(&[cql_path]).await.unwrap();

        assert_eq!(
            result.schemas_loaded, 0,
            "table referencing undefined UDT must not load"
        );
        assert!(
            result
                .errors
                .iter()
                .any(|e| e.message.contains("ContactInfo")),
            "an error must name the missing UDT, got: {:?}",
            result.errors
        );
    }

    /// Issue #761: nested UDT references (UDT inside a collection/frozen) must be
    /// validated, not just top-level columns.
    #[tokio::test]
    async fn test_table_referencing_undefined_udt_nested_fails() {
        let (mut aggregator, temp_dir) = setup_test_aggregator().await;

        let cql_content = r#"
        CREATE TABLE test_ks.users (
            id uuid PRIMARY KEY,
            contacts list<frozen<ContactInfo>>
        );
        "#;

        let cql_path = write_file(temp_dir.path(), "schema.cql", cql_content);
        let result = aggregator.load_from_paths(&[cql_path]).await.unwrap();

        assert_eq!(
            result.schemas_loaded, 0,
            "table referencing undefined nested UDT must not load"
        );
        assert!(
            result
                .errors
                .iter()
                .any(|e| e.message.contains("ContactInfo")),
            "an error must name the missing nested UDT, got: {:?}",
            result.errors
        );
    }

    /// Issue #761: a table whose UDT reference IS defined still loads cleanly.
    #[tokio::test]
    async fn test_table_referencing_defined_udt_loads() {
        let (mut aggregator, temp_dir) = setup_test_aggregator().await;

        let cql_content = r#"
        CREATE TYPE test_ks.contact_info (
            email text,
            phone text
        );

        CREATE TABLE test_ks.users (
            id uuid PRIMARY KEY,
            contact contact_info
        );
        "#;

        let cql_path = write_file(temp_dir.path(), "schema.cql", cql_content);
        let result = aggregator.load_from_paths(&[cql_path]).await.unwrap();

        assert!(
            result.errors.is_empty(),
            "defined UDT reference should load without errors, got: {:?}",
            result.errors
        );
        assert_eq!(result.udts_loaded, 1);
        assert_eq!(result.schemas_loaded, 1);
    }

    #[tokio::test]
    #[ignore = "Test fails due to UDT dependency validation not implemented - see Issue #117 review"]
    async fn test_cql_file_with_comments_and_semicolons() {
        let (mut aggregator, temp_dir) = setup_test_aggregator().await;

        // Test edge cases: comments with semicolons
        let cql_content = r#"
        -- This is a comment with ; semicolon
        CREATE TYPE test_ks.metadata (
            key text,
            value text
        );

        /* Multi-line comment
           with ; semicolon */
        CREATE TABLE test_ks.data (
            id uuid PRIMARY KEY,
            info metadata
        );
        "#;

        let cql_path = write_file(temp_dir.path(), "edge_cases.cql", cql_content);
        let result = aggregator.load_from_paths(&[cql_path]).await.unwrap();

        assert_eq!(result.udts_loaded, 1);
        assert_eq!(result.schemas_loaded, 1);
        assert!(result.errors.is_empty());
    }

    #[tokio::test]
    async fn test_backward_compat_single_create_table() {
        let (mut aggregator, temp_dir) = setup_test_aggregator().await;

        // Ensure backward compatibility with single CREATE TABLE files
        let cql_content = r#"
        CREATE TABLE test_ks.simple (
            id uuid PRIMARY KEY,
            data text
        );
        "#;

        let cql_path = write_file(temp_dir.path(), "simple.cql", cql_content);
        let result = aggregator.load_from_paths(&[cql_path]).await.unwrap();

        assert_eq!(result.schemas_loaded, 1);
        assert_eq!(result.udts_loaded, 0);
        assert!(result.errors.is_empty());
    }

    #[tokio::test]
    async fn test_multi_keyspace_cql_file_no_collision() {
        let (mut aggregator, temp_dir) = setup_test_aggregator().await;

        // CQL file with UDTs and tables from TWO different keyspaces
        let cql_content = r#"
        CREATE TYPE ks_a.address (
            street text,
            city text
        );

        CREATE TYPE ks_b.address (
            country text,
            postal_code text
        );

        CREATE TABLE ks_a.users (
            id uuid PRIMARY KEY,
            addr frozen<address>
        );

        CREATE TABLE ks_b.customers (
            id uuid PRIMARY KEY,
            location frozen<address>
        );
        "#;

        let cql_path = write_file(temp_dir.path(), "multi_ks.cql", cql_content);
        let result = aggregator.load_from_paths(&[cql_path]).await.unwrap();

        // Both UDTs should be loaded (no collision)
        assert_eq!(
            result.udts_loaded, 2,
            "Expected 2 UDTs from different keyspaces"
        );
        assert_eq!(
            result.schemas_loaded, 2,
            "Expected 2 tables from different keyspaces"
        );
        assert!(
            result.errors.is_empty(),
            "Expected no errors, got: {:?}",
            result.errors
        );

        // Verify both UDTs are registered with correct keyspaces
        let udt_registry = aggregator.udt_registry.read().await;
        assert!(
            udt_registry.contains_udt("ks_a", "address"),
            "ks_a.address should be registered"
        );
        assert!(
            udt_registry.contains_udt("ks_b", "address"),
            "ks_b.address should be registered"
        );

        // Verify both tables are registered with correct keyspaces
        let registry = aggregator.registry.read().await;
        let schema_a = registry.get_schema("ks_a", "users").await.unwrap();
        assert_eq!(schema_a.keyspace, "ks_a");
        assert_eq!(schema_a.table, "users");

        let schema_b = registry.get_schema("ks_b", "customers").await.unwrap();
        assert_eq!(schema_b.keyspace, "ks_b");
        assert_eq!(schema_b.table, "customers");
    }
}
