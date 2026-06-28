//! CQL AST to Mutation conversion.
//!
//! Converts parsed CQL literal values into `Value` types that the WriteEngine
//! can persist. The primary entry point is `literal_to_value`, which performs
//! schema-aware coercion from the CQL parser's AST types to the internal
//! `Value` enum.
//!
//! The `insert_to_mutation` function converts a parsed `CqlInsert` AST node into
//! a `Mutation` using schema information to identify partition/clustering keys and
//! regular columns.
//!
//! This module is split by responsibility:
//! - [`builders`] — INSERT/UPDATE/DELETE/BATCH → `Mutation` builders
//! - [`codec`] — CQL literal/expression/JSON → `Value` conversion + scalar parsers
//! - [`delta_helpers`] — WHERE-clause bindings, predicates, tombstones, USING clauses
//!
//! `mod.rs` owns the string-dispatch entry points (`convert_cql_to_mutation`,
//! `convert_cql_to_mutations`) and re-exports the public surface so callers see an
//! unchanged API.

#[cfg(feature = "write-support")]
mod builders;
#[cfg(feature = "write-support")]
mod codec;
#[cfg(feature = "write-support")]
mod delta_helpers;

#[cfg(feature = "write-support")]
pub(crate) use builders::{
    batch_to_mutations, delete_to_mutation, insert_to_mutation, update_to_mutation,
};
// `literal_to_value` is the documented schema-aware coercion entry point. It is
// re-exported to preserve the module's public surface even though current
// in-crate callers reach value coercion via the higher-level builders.
#[cfg(feature = "write-support")]
#[allow(unused_imports)]
pub(crate) use codec::literal_to_value;

#[cfg(feature = "write-support")]
use crate::schema::TableSchema;
#[cfg(feature = "write-support")]
use crate::storage::write_engine::mutation::Mutation;
#[cfg(feature = "write-support")]
use crate::Error;

/// Convert a CQL mutation statement string to a Mutation struct.
///
/// Supports INSERT, UPDATE, and DELETE statements.
/// The statement is parsed using the existing CQL parser, then converted
/// to a Mutation using the provided schema for type resolution.
#[cfg(feature = "write-support")]
pub(crate) fn convert_cql_to_mutation(
    statement: &str,
    schema: &TableSchema,
) -> Result<Mutation, Error> {
    let trimmed = statement.trim();

    if trimmed.len() >= 6 && trimmed.as_bytes()[..6].eq_ignore_ascii_case(b"INSERT") {
        let insert = crate::cql::mutation_parser::parse_insert_statement(trimmed)?;
        insert_to_mutation(&insert, schema)
    } else if trimmed.len() >= 6 && trimmed.as_bytes()[..6].eq_ignore_ascii_case(b"UPDATE") {
        let update = crate::cql::mutation_parser::parse_update_statement(trimmed)?;
        update_to_mutation(&update, schema)
    } else if trimmed.len() >= 6 && trimmed.as_bytes()[..6].eq_ignore_ascii_case(b"DELETE") {
        let delete = crate::cql::mutation_parser::parse_delete_statement(trimmed)?;
        delete_to_mutation(&delete, schema)
    } else {
        Err(Error::InvalidInput(format!(
            "Unsupported mutation statement. Expected INSERT, UPDATE, or DELETE: {}",
            &trimmed[..trimmed.len().min(50)]
        )))
    }
}

/// Convert a CQL statement string to one or more `Mutation` structs.
///
/// Supports INSERT, UPDATE, DELETE, and BATCH statements. BATCH statements
/// produce multiple mutations (one per inner statement). All other statements
/// produce a single-element vector.
#[cfg(feature = "write-support")]
pub(crate) fn convert_cql_to_mutations(
    statement: &str,
    schema: &TableSchema,
) -> Result<Vec<Mutation>, Error> {
    let trimmed = statement.trim();

    if trimmed.len() >= 5 && trimmed.as_bytes()[..5].eq_ignore_ascii_case(b"BEGIN") {
        let batch = crate::cql::mutation_parser::parse_batch_statement(trimmed)?;
        batch_to_mutations(&batch, schema)
    } else {
        let mutation = convert_cql_to_mutation(trimmed, schema)?;
        Ok(vec![mutation])
    }
}

#[cfg(all(test, feature = "write-support"))]
mod tests {
    use super::*;
    use crate::storage::write_engine::mutation::CellOperation;
    use crate::types::Value;
    use std::collections::HashMap;

    // ── Test schema helpers ────────────────────────────────────────────────────

    fn test_schema() -> crate::schema::TableSchema {
        crate::schema::TableSchema {
            keyspace: "test_ks".into(),
            table: "test_tbl".into(),
            partition_keys: vec![crate::schema::KeyColumn {
                name: "id".into(),
                data_type: "uuid".into(),
                position: 0,
            }],
            clustering_keys: vec![crate::schema::ClusteringColumn {
                name: "ts".into(),
                data_type: "timestamp".into(),
                position: 0,
                order: crate::schema::ClusteringOrder::Asc,
            }],
            columns: vec![
                crate::schema::Column {
                    name: "id".into(),
                    data_type: "uuid".into(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                crate::schema::Column {
                    name: "ts".into(),
                    data_type: "timestamp".into(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                crate::schema::Column {
                    name: "name".into(),
                    data_type: "text".into(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                crate::schema::Column {
                    name: "age".into(),
                    data_type: "int".into(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        }
    }

    fn simple_json_schema() -> crate::schema::TableSchema {
        crate::schema::TableSchema {
            keyspace: "test_ks".into(),
            table: "test_tbl".into(),
            partition_keys: vec![crate::schema::KeyColumn {
                name: "id".into(),
                data_type: "int".into(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![
                crate::schema::Column {
                    name: "id".into(),
                    data_type: "int".into(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                crate::schema::Column {
                    name: "name".into(),
                    data_type: "text".into(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                crate::schema::Column {
                    name: "value".into(),
                    data_type: "int".into(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                crate::schema::Column {
                    name: "flag".into(),
                    data_type: "boolean".into(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                crate::schema::Column {
                    name: "tags".into(),
                    data_type: "list<text>".into(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        }
    }

    fn test_clustering_schema() -> crate::schema::TableSchema {
        crate::schema::TableSchema {
            keyspace: "test_ks".into(),
            table: "test_table".into(),
            partition_keys: vec![crate::schema::KeyColumn {
                name: "id".into(),
                data_type: "int".into(),
                position: 0,
            }],
            clustering_keys: vec![crate::schema::ClusteringColumn {
                name: "ck".into(),
                data_type: "text".into(),
                position: 0,
                order: crate::schema::ClusteringOrder::Asc,
            }],
            columns: vec![
                crate::schema::Column {
                    name: "id".into(),
                    data_type: "int".into(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                crate::schema::Column {
                    name: "ck".into(),
                    data_type: "text".into(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                crate::schema::Column {
                    name: "value".into(),
                    data_type: "text".into(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        }
    }

    // ── convert_cql_to_mutation integration tests ─────────────────────────────

    #[test]
    fn test_convert_cql_insert_string() {
        let schema = test_schema();
        let sql = "INSERT INTO test_ks.test_tbl (id, ts, name) VALUES (550e8400-e29b-41d4-a716-446655440000, 1704067200000, 'Alice')";
        let result = convert_cql_to_mutation(sql, &schema);
        assert!(result.is_ok(), "Failed: {:?}", result.err());
        let mutation = result.unwrap();
        assert_eq!(mutation.table.keyspace, "test_ks");
        assert_eq!(mutation.table.table, "test_tbl");
        assert_eq!(mutation.operations.len(), 1); // only 'name' is non-key
    }

    #[test]
    fn test_convert_cql_update_string() {
        let schema = test_schema();
        let sql = "UPDATE test_ks.test_tbl SET name = 'Bob' WHERE id = 550e8400-e29b-41d4-a716-446655440000 AND ts = 1704067200000";
        let result = convert_cql_to_mutation(sql, &schema);
        assert!(result.is_ok(), "Failed: {:?}", result.err());
        let mutation = result.unwrap();
        assert_eq!(mutation.operations.len(), 1);
        match &mutation.operations[0] {
            CellOperation::Write { column, value } => {
                assert_eq!(column, "name");
                assert_eq!(*value, Value::Text("Bob".into()));
            }
            _ => panic!("expected Write"),
        }
    }

    #[test]
    fn test_convert_cql_delete_string_partition_tombstone() {
        // DELETE with only PK on a table with clustering columns → partition tombstone
        let schema = test_schema();
        let sql = "DELETE FROM test_ks.test_tbl WHERE id = 550e8400-e29b-41d4-a716-446655440000";
        let result = convert_cql_to_mutation(sql, &schema);
        assert!(result.is_ok(), "Failed: {:?}", result.err());
        let mutation = result.unwrap();
        assert!(
            mutation.operations.is_empty(),
            "Partition tombstone should have no cell operations"
        );
        assert!(
            mutation.partition_tombstone.is_some(),
            "Should generate partition tombstone"
        );
    }

    #[test]
    fn test_convert_cql_delete_string_row_tombstone() {
        // DELETE with PK + CK → row tombstone
        let schema = test_schema();
        let sql = "DELETE FROM test_ks.test_tbl WHERE id = 550e8400-e29b-41d4-a716-446655440000 AND ts = 1704067200000";
        let result = convert_cql_to_mutation(sql, &schema);
        assert!(result.is_ok(), "Failed: {:?}", result.err());
        let mutation = result.unwrap();
        assert_eq!(mutation.operations.len(), 1);
        assert!(matches!(mutation.operations[0], CellOperation::DeleteRow));
        assert!(mutation.partition_tombstone.is_none());
    }

    #[test]
    fn test_convert_unsupported_statement() {
        let schema = test_schema();
        let sql = "SELECT * FROM test_ks.test_tbl";
        let result = convert_cql_to_mutation(sql, &schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_convert_cql_insert_with_using_timestamp() {
        let schema = test_schema();
        let sql = "INSERT INTO test_ks.test_tbl (id, ts, name) VALUES (550e8400-e29b-41d4-a716-446655440000, 1704067200000, 'Alice') USING TIMESTAMP 1704067200000000";
        let result = convert_cql_to_mutation(sql, &schema);
        assert!(result.is_ok(), "Failed: {:?}", result.err());
        assert_eq!(result.unwrap().timestamp_micros, 1704067200000000);
    }

    #[test]
    fn test_convert_cql_delete_range_string() {
        let schema = test_clustering_schema();
        let sql = "DELETE FROM test_ks.test_table WHERE id = 1 AND ck > 'a' AND ck < 'z'";
        let result = convert_cql_to_mutation(sql, &schema);
        assert!(result.is_ok(), "Failed: {:?}", result.err());
        let mutation = result.unwrap();
        assert_eq!(mutation.range_tombstones.len(), 1);
        assert!(mutation.operations.is_empty());
    }

    // ── BATCH statement tests ─────────────────────────────────────────────────

    #[test]
    fn test_batch_basic_two_inserts() {
        let schema = simple_json_schema();
        let sql = "BEGIN BATCH \
            INSERT INTO test_ks.test_tbl (id, name) VALUES (1, 'Alice'); \
            INSERT INTO test_ks.test_tbl (id, name) VALUES (2, 'Bob'); \
            APPLY BATCH";
        let result = convert_cql_to_mutations(sql, &schema);
        assert!(result.is_ok(), "Failed: {:?}", result.err());
        let mutations = result.unwrap();
        assert_eq!(mutations.len(), 2);
        assert_eq!(mutations[0].partition_key.columns[0].1, Value::Integer(1));
        assert_eq!(mutations[1].partition_key.columns[0].1, Value::Integer(2));
    }

    #[test]
    fn test_batch_mixed_statements() {
        let schema = simple_json_schema();
        let sql = "BEGIN BATCH \
            INSERT INTO test_ks.test_tbl (id, name) VALUES (1, 'Alice'); \
            UPDATE test_ks.test_tbl SET name = 'Updated' WHERE id = 1; \
            DELETE FROM test_ks.test_tbl WHERE id = 2; \
            APPLY BATCH";
        let result = convert_cql_to_mutations(sql, &schema);
        assert!(result.is_ok(), "Failed: {:?}", result.err());
        let mutations = result.unwrap();
        assert_eq!(mutations.len(), 3);
    }

    #[test]
    fn test_batch_unlogged() {
        let schema = simple_json_schema();
        let sql = "BEGIN UNLOGGED BATCH \
            INSERT INTO test_ks.test_tbl (id, name) VALUES (1, 'Alice'); \
            APPLY BATCH";
        let result = convert_cql_to_mutations(sql, &schema);
        assert!(result.is_ok(), "Failed: {:?}", result.err());
        assert_eq!(result.unwrap().len(), 1);
    }

    #[test]
    fn test_batch_with_timestamp() {
        let schema = simple_json_schema();
        let sql = "BEGIN BATCH USING TIMESTAMP 1704067200000000 \
            INSERT INTO test_ks.test_tbl (id, name) VALUES (1, 'Alice'); \
            INSERT INTO test_ks.test_tbl (id, name) VALUES (2, 'Bob'); \
            APPLY BATCH";
        let result = convert_cql_to_mutations(sql, &schema);
        assert!(result.is_ok(), "Failed: {:?}", result.err());
        let mutations = result.unwrap();
        assert_eq!(mutations.len(), 2);
        assert_eq!(mutations[0].timestamp_micros, 1704067200000000);
        assert_eq!(mutations[1].timestamp_micros, 1704067200000000);
    }

    #[test]
    fn test_batch_inner_timestamp_override() {
        let schema = simple_json_schema();
        let sql = "BEGIN BATCH USING TIMESTAMP 1000000 \
            INSERT INTO test_ks.test_tbl (id, name) VALUES (1, 'Alice') USING TIMESTAMP 2000000; \
            INSERT INTO test_ks.test_tbl (id, name) VALUES (2, 'Bob'); \
            APPLY BATCH";
        let result = convert_cql_to_mutations(sql, &schema);
        assert!(result.is_ok(), "Failed: {:?}", result.err());
        let mutations = result.unwrap();
        // First INSERT has its own timestamp — should be preserved
        assert_eq!(mutations[0].timestamp_micros, 2000000);
        // Second INSERT should use batch timestamp
        assert_eq!(mutations[1].timestamp_micros, 1000000);
    }

    #[test]
    fn test_batch_empty() {
        let schema = simple_json_schema();
        let sql = "BEGIN BATCH APPLY BATCH";
        let result = convert_cql_to_mutations(sql, &schema);
        assert!(result.is_ok(), "Failed: {:?}", result.err());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_batch_single_statement() {
        let schema = simple_json_schema();
        let sql = "BEGIN BATCH \
            INSERT INTO test_ks.test_tbl (id, name) VALUES (1, 'Alice'); \
            APPLY BATCH";
        let result = convert_cql_to_mutations(sql, &schema);
        assert!(result.is_ok(), "Failed: {:?}", result.err());
        assert_eq!(result.unwrap().len(), 1);
    }

    #[test]
    fn test_convert_cql_to_mutations_non_batch() {
        // Non-batch statement should return single mutation
        let schema = simple_json_schema();
        let sql = "INSERT INTO test_ks.test_tbl (id, name) VALUES (1, 'Alice')";
        let result = convert_cql_to_mutations(sql, &schema);
        assert!(result.is_ok(), "Failed: {:?}", result.err());
        assert_eq!(result.unwrap().len(), 1);
    }
}
