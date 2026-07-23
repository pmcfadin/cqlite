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
        "boolean" => Some(1),
        "int" | "float" => Some(4),
        "bigint" | "timestamp" | "double" => Some(8),
        "uuid" | "timeuuid" => Some(16),
        // `tinyint`/`smallint`/`date`/`time` are NOT fixed-length. Per Cassandra
        // 5.0.2 marshal source, `ByteType`(tinyint)/`ShortType`(smallint) extend
        // `NumberType`, and `SimpleDateType`(date)/`TimeType`(time) extend
        // `TemporalType`; none of them — nor their parents — override
        // `AbstractType.valueLengthIfFixed()`, so all four inherit
        // `VARIABLE_LENGTH == -1` and are written via `writeWithVIntLength`
        // (unsigned-vint length prefix + bytes), exactly like text/blob.
        // Classifying them as `Fixed(1/2/4/8)` would read the first value byte
        // as the vint length and misalign the cursor for every subsequent cell
        // (mirrors the AUTHORITY NOTE in parser/repair_clustering.rs). They fall
        // through to `None` below (the vint-length path) and stay simple scalars
        // on the `is_simple_scalar_type` allowlist.
        //
        // `counter` is NOT fixed-8 either: CounterColumnType extends NumberType (not
        // LongType) and does not override valueLengthIfFixed(), so a counter
        // cell's on-disk value is a vint-length-prefixed CounterContext blob
        // (header + shards), not a raw i64. Treating it as fixed-8 silently
        // misaligned every subsequent field in the partition (roborev
        // finding, review-first pass). Falls through to `None` below, which
        // reads it exactly like blob — the vint-length envelope is what
        // matters for staying aligned; this module doesn't further interpret
        // the CounterContext's internal shard structure.
        _ => None,
    }
}

/// Column types this module can decode without misaligning the cursor.
///
/// Cassandra's row format serializes a "complex" column (collection/tuple/
/// UDT/vector) as a categorically different wire structure — an optional
/// complex-deletion time, then a count-prefixed set of (path, cell) pairs —
/// not the single simple `Cell` this module's `read_cell` decodes. Treating a
/// complex column as simple doesn't just read the wrong length, it reads the
/// wrong SHAPE, silently misaligning every subsequent row/field while still
/// reporting `rows_decoded == true` (roborev finding, review-first pass: the
/// module doc already claimed this was surfaced honestly — it was not,
/// because nothing actually checked for it). Prose-matched, not a byte-level
/// verification against Cassandra 5.0 source (unavailable in this
/// environment) — kept conservative (allowlist, not denylist) so an unknown
/// type name bails rather than being assumed simple.
pub fn is_simple_scalar_type(type_name: &str) -> bool {
    let t = type_name.trim().to_ascii_lowercase();
    matches!(
        t.as_str(),
        "boolean"
            | "tinyint"
            | "smallint"
            | "int"
            | "date"
            | "float"
            | "bigint"
            | "timestamp"
            | "time"
            | "double"
            | "uuid"
            | "timeuuid"
            | "text"
            | "varchar"
            | "ascii"
            | "blob"
            | "varint"
            | "decimal"
            | "inet"
            | "counter"
    )
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
    // `hex.len()` is a BYTE length; a non-ASCII multi-byte char could pass a
    // `!= 32` byte-length guard while still slicing across a UTF-8 char
    // boundary below and panicking — a public API taking untrusted input
    // must return None, never panic (roborev finding, review-first pass).
    if !hex.is_ascii() || hex.len() != 32 {
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

    #[test]
    fn counter_is_not_fixed_length() {
        // Regression: CounterColumnType does not override valueLengthIfFixed()
        // — a counter cell is a vint-length-prefixed CounterContext blob, not
        // a raw fixed-8 i64 (roborev finding, review-first pass).
        assert_eq!(cql_fixed_len("counter"), None);
    }

    #[test]
    fn tinyint_smallint_date_time_are_not_fixed_length() {
        // Regression: none of ByteType(tinyint)/ShortType(smallint)/
        // SimpleDateType(date)/TimeType(time) override valueLengthIfFixed() in
        // Cassandra 5.0.2 — they inherit VARIABLE_LENGTH and are written with a
        // vint length prefix (writeWithVIntLength), exactly like text/blob.
        // Treating them as Fixed(1/2/4/8) reads the first value byte as the vint
        // length and misaligns every subsequent cell (PR #2797 blocker).
        for t in ["tinyint", "smallint", "date", "time"] {
            assert_eq!(cql_fixed_len(t), None, "{t} must be variable-length");
            // Still a simple scalar — just a variable-length one.
            assert!(is_simple_scalar_type(t), "{t} stays a simple scalar");
        }
    }

    #[test]
    fn simple_scalar_type_allowlist_excludes_complex_types() {
        for t in [
            "int", "bigint", "text", "blob", "uuid", "counter", "decimal", "varint", "inet",
        ] {
            assert!(is_simple_scalar_type(t), "{t} should be a simple scalar");
        }
        for t in [
            "list<text>",
            "set<int>",
            "map<text, int>",
            "frozen<list<int>>",
            "tuple<int, text>",
            "vector<float, 4>",
            "some_udt_name",
        ] {
            assert!(
                !is_simple_scalar_type(t),
                "{t} must bail, not be treated as a simple scalar"
            );
        }
    }

    #[test]
    fn parse_table_id_rejects_non_ascii_without_panicking() {
        // A multi-byte UTF-8 char can pass a byte-length `!= 32` guard while
        // still slicing across a char boundary — must return None, not panic
        // (roborev finding, review-first pass).
        assert_eq!(parse_table_id("d6de7150-8448-11f1-bf5a-bf4cbc47cb4€"), None);
    }
}
