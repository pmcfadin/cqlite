//! Schema-file loading for the legacy SSTable command handlers.
//!
//! Extracted verbatim from `commands/mod.rs` during the module split (issue #1126).
//! Loads a `TableSchema` from either a `.cql`/`.sql` file or a JSON schema document.

use anyhow::{Context, Result};
use cqlite_core::schema::{
    parse_cql_schema, ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema,
};
use std::collections::HashMap;
use std::path::Path;

/// Load schema from JSON or CQL file
pub(crate) fn load_schema_file(
    schema_path: &Path,
    _auto_detect: bool,
    _cassandra_version: Option<&str>,
) -> Result<TableSchema> {
    let schema_content = std::fs::read_to_string(schema_path)
        .with_context(|| format!("Failed to read schema file: {}", schema_path.display()))?;

    println!("📋 Loading schema from: {}", schema_path.display());

    // Determine file type by extension
    let extension = schema_path
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("");

    match extension.to_lowercase().as_str() {
        "json" => {
            println!("📝 Parsing JSON schema format");
            // Parse JSON schema
            let json_schema: serde_json::Value = serde_json::from_str(&schema_content)
                .with_context(|| "Failed to parse JSON schema")?;

            // Convert JSON to TableSchema
            parse_json_schema(&json_schema)
        }
        "cql" | "sql" | "" => {
            println!("📝 Parsing CQL schema format");
            // Parse CQL schema
            parse_cql_schema(&schema_content).with_context(|| "Failed to parse CQL schema")
        }
        _ => Err(anyhow::anyhow!(
            "Unsupported schema file extension: .{}\nSupported formats: .json, .cql",
            extension
        )),
    }
}

/// Parse JSON schema format
fn parse_json_schema(json: &serde_json::Value) -> Result<TableSchema> {
    let keyspace = json["keyspace"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("Missing keyspace in schema"))?;
    let table = json["table"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("Missing table in schema"))?;

    let columns = json["columns"]
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("Missing columns in schema"))?;

    let mut schema_columns = Vec::new();
    let mut partition_keys = Vec::new();
    let mut clustering_columns = Vec::new();

    for (col_name, col_info) in columns {
        let col_obj = col_info
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("Invalid column definition for {}", col_name))?;

        let col_type = col_obj["type"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Missing type for column {}", col_name))?;
        let col_kind = col_obj["kind"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Missing kind for column {}", col_name))?;

        let column = Column {
            name: col_name.clone(),
            data_type: col_type.to_string(),
            nullable: true,   // Default to nullable
            default: None,    // No default value
            is_static: false, // SSTable metadata doesn't track static columns
        };

        match col_kind {
            "PartitionKey" => {
                partition_keys.push(KeyColumn {
                    name: col_name.clone(),
                    position: partition_keys.len(),
                    data_type: col_type.to_string(),
                });
            }
            "ClusteringColumn" => {
                clustering_columns.push(ClusteringColumn {
                    name: col_name.clone(),
                    position: clustering_columns.len(),
                    data_type: col_type.to_string(),
                    order: ClusteringOrder::Asc,
                });
            }
            "Regular" => {
                // Regular column - just add to columns list
            }
            _ => return Err(anyhow::anyhow!("Unknown column kind: {}", col_kind)),
        }

        schema_columns.push(column);
    }

    // Optional dropped-column drop times (column → drop_time_micros) used for
    // dropped-column filtering during compaction (#904/#847). Absent → empty; a
    // present-but-malformed field is a hard error (not silently ignored) so a
    // typo can't leave stale cells unpurged.
    let mut dropped_columns = HashMap::new();
    match json.get("dropped_columns") {
        None | Some(serde_json::Value::Null) => {}
        Some(serde_json::Value::Object(dropped)) => {
            for (name, ts) in dropped {
                let drop_time = ts.as_i64().ok_or_else(|| {
                    anyhow::anyhow!(
                        "dropped_columns['{}'] must be an integer drop time in microseconds",
                        name
                    )
                })?;
                dropped_columns.insert(name.clone(), drop_time);
            }
        }
        Some(_) => {
            return Err(anyhow::anyhow!(
                "schema field `dropped_columns` must be an object mapping column \
                 name → drop time in microseconds"
            ));
        }
    }

    Ok(TableSchema {
        keyspace: keyspace.to_string(),
        table: table.to_string(),
        columns: schema_columns,
        partition_keys,
        clustering_keys: clustering_columns,
        comments: HashMap::new(),
        dropped_columns,
    })
}

#[cfg(test)]
mod dropped_column_json_tests {
    use super::*;

    /// The CLI JSON schema loader must carry `dropped_columns` into the
    /// `TableSchema` so `cqlite compact --schema x.json` can supply drop times
    /// for dropped-column filtering (#904/#847). The dropped column stays in
    /// `columns` (decode contract).
    #[test]
    fn parse_json_schema_preserves_dropped_columns() {
        let json: serde_json::Value = serde_json::from_str(
            r#"{
                "keyspace": "ks",
                "table": "t",
                "columns": {
                    "id": {"type": "uuid", "kind": "PartitionKey"},
                    "legacy": {"type": "int", "kind": "Regular"}
                },
                "dropped_columns": {"legacy": 1700000000000000}
            }"#,
        )
        .expect("json parses");

        let schema = parse_json_schema(&json).expect("schema parses");
        assert_eq!(
            schema.dropped_columns.get("legacy"),
            Some(&1_700_000_000_000_000_i64),
            "CLI JSON loader must preserve dropped_columns drop time"
        );
    }

    /// A `dropped_columns` field of the wrong shape (not an object) is a clear
    /// error, not silently ignored — a typo must not leave stale cells unpurged.
    #[test]
    fn parse_json_schema_rejects_non_object_dropped_columns() {
        let json: serde_json::Value = serde_json::from_str(
            r#"{
                "keyspace": "ks",
                "table": "t",
                "columns": {"id": {"type": "uuid", "kind": "PartitionKey"}},
                "dropped_columns": ["legacy"]
            }"#,
        )
        .expect("json parses");

        let err = parse_json_schema(&json).expect_err("non-object dropped_columns must error");
        assert!(
            err.to_string().contains("dropped_columns"),
            "error must name the offending field, got: {err}"
        );
    }

    /// A non-integer drop time is a clear error rather than a silent drop.
    #[test]
    fn parse_json_schema_rejects_non_integer_drop_time() {
        let json: serde_json::Value = serde_json::from_str(
            r#"{
                "keyspace": "ks",
                "table": "t",
                "columns": {"id": {"type": "uuid", "kind": "PartitionKey"}},
                "dropped_columns": {"legacy": "not-a-number"}
            }"#,
        )
        .expect("json parses");

        assert!(
            parse_json_schema(&json).is_err(),
            "a non-integer drop time must be rejected"
        );
    }
}
