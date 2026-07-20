//! Minimal per-table schema for schema-aware CommitLog mutation decoding
//! (issue #2389).
//!
//! Cassandra's CommitLog mutation stream is only partially self-describing: the
//! per-partition messaging header carries column **names**, but clustering and
//! cell **values** are serialized by their CQL type's fixed/variable-length
//! rule (`AbstractType.writeValue`) with no on-wire type tag. Fixed-length types
//! (int, bigint, uuid, …) are written with **no length prefix**, so decoding
//! them requires knowing the type — exactly the schema-aware posture CQLite's
//! SSTable reader takes (no-heuristics, issue #28: types come from the schema,
//! never guessed from bytes).
//!
//! Without a schema, [`super::CommitLogReader`] still decodes every mutation's
//! table id, partition key, and column names structurally; a schema unlocks full
//! clustering/cell value decode.

use std::collections::HashMap;

/// A column's name and CQL type name (e.g. `("age", "int")`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSpec {
    /// Column name as it appears in the messaging header.
    pub name: String,
    /// CQL type name, lowercased (e.g. `int`, `text`, `uuid`).
    pub type_name: String,
}

impl ColumnSpec {
    /// Construct a spec, normalizing the type name to lowercase.
    pub fn new(name: impl Into<String>, type_name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            type_name: type_name.into().to_ascii_lowercase(),
        }
    }
}

/// A single table's schema, keyed into a decode by its 16-byte table id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitLogSchema {
    /// Keyspace name (informational).
    pub keyspace: String,
    /// Table name (informational).
    pub table: String,
    /// Partition-key columns in order.
    pub partition_key: Vec<ColumnSpec>,
    /// Clustering columns in order.
    pub clustering: Vec<ColumnSpec>,
    /// Regular + static columns, looked up by name during cell decode.
    pub columns: Vec<ColumnSpec>,
}

impl CommitLogSchema {
    /// Look up a regular/static column's CQL type by name.
    pub fn column_type(&self, name: &str) -> Option<&str> {
        self.columns
            .iter()
            .find(|c| c.name == name)
            .map(|c| c.type_name.as_str())
    }
}

/// A set of table schemas keyed by their 16-byte table id.
pub type SchemaSet = HashMap<[u8; 16], CommitLogSchema>;

/// Fixed serialized length of a CQL type's value, or `None` if variable-length.
///
/// Mirrors `AbstractType.valueLengthIfFixed()` for the Cassandra 5.0 types.
/// Variable-length types (text/blob/varint/decimal/inet/collections/…) return
/// `None` and are decoded with an unsigned-vint length prefix.
pub fn cql_fixed_len(type_name: &str) -> Option<usize> {
    match type_name.trim().to_ascii_lowercase().as_str() {
        "boolean" | "tinyint" => Some(1),
        "smallint" => Some(2),
        "int" | "date" | "float" => Some(4),
        "bigint" | "timestamp" | "time" | "double" | "counter" => Some(8),
        "uuid" | "timeuuid" => Some(16),
        _ => None,
    }
}

/// Format a 16-byte table id as a canonical UUID string
/// (`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`).
pub fn format_table_id(id: &[u8; 16]) -> String {
    let hex: String = id.iter().map(|b| format!("{b:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32],
    )
}

/// Parse a canonical UUID string into 16 bytes. Returns `None` on malformed
/// input.
pub fn parse_table_id(s: &str) -> Option<[u8; 16]> {
    let hex: String = s.chars().filter(|c| *c != '-').collect();
    if hex.len() != 32 {
        return None;
    }
    let mut out = [0u8; 16];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_lengths_match_cassandra() {
        assert_eq!(cql_fixed_len("int"), Some(4));
        assert_eq!(cql_fixed_len("bigint"), Some(8));
        assert_eq!(cql_fixed_len("uuid"), Some(16));
        assert_eq!(cql_fixed_len("boolean"), Some(1));
        assert_eq!(cql_fixed_len("text"), None);
        assert_eq!(cql_fixed_len("blob"), None);
        assert_eq!(cql_fixed_len("varint"), None);
    }

    #[test]
    fn table_id_round_trips() {
        let s = "d6de7150-8448-11f1-bf5a-bf4cbc47cb4a";
        let id = parse_table_id(s).expect("parse");
        assert_eq!(format_table_id(&id), s);
    }
}
