//! `Selection` (design D1.1): what partitions an `extract` run targets, CQL-
//! literal key parsing, and resolving `Selection::TokenRange` to a concrete
//! raw-key list via the boundary-source walk.

use std::collections::HashSet;
use std::path::PathBuf;

use crate::cql::ast::CqlLiteral;
use crate::error::{Error, Result};
use crate::schema::{CqlType, TableSchema};
use crate::storage::partition_key_codec::encode_partition_key_columns;
use crate::storage::write_engine::cql_to_mutation::literal_to_value;
use crate::storage::write_engine::salvage::boundaries::enumerate_boundaries;
use crate::types::Value;

use super::{base_and_format, SelectionSummary};

/// What partitions an `extract` run is selecting (design D1.1). `Key`/
/// `KeySet` already carry RESOLVED raw partition-key bytes — [`parse_key_literal`]
/// performs the CQL-literal → raw-key-bytes step the CLI (or a library
/// caller) runs first.
#[derive(Debug, Clone)]
pub enum Selection {
    /// One partition.
    Key(Vec<u8>),
    /// `(a, b]` — left-exclusive, right-inclusive Murmur3 token range,
    /// matching Cassandra's own ring-boundary convention
    /// (`nodetool describering`/`ring`).
    TokenRange(i64, i64),
    /// A named set of partitions.
    KeySet(Vec<Vec<u8>>),
}

impl Selection {
    pub(crate) fn summarize(&self) -> SelectionSummary {
        match self {
            Selection::Key(k) => SelectionSummary {
                kind: "partition".to_string(),
                detail: hex::encode(k),
            },
            Selection::TokenRange(a, b) => SelectionSummary {
                kind: "token-range".to_string(),
                detail: format!("({a}, {b}]"),
            },
            Selection::KeySet(keys) => SelectionSummary {
                kind: "keys-file".to_string(),
                detail: format!("{} keys", keys.len()),
            },
        }
    }

    /// Resolve to a concrete, deduplicated raw-key list (design D1.1).
    /// `Key`/`KeySet` are already resolved; `TokenRange` walks every input
    /// generation's boundary source.
    pub(crate) fn resolve(&self, generation_paths: &[PathBuf]) -> Result<Vec<Vec<u8>>> {
        match self {
            Selection::Key(k) => Ok(vec![k.clone()]),
            Selection::KeySet(keys) => {
                let mut seen = HashSet::new();
                let mut out = Vec::new();
                for k in keys {
                    if seen.insert(k.clone()) {
                        out.push(k.clone());
                    }
                }
                Ok(out)
            }
            Selection::TokenRange(a, b) => resolve_token_range_to_keys(generation_paths, *a, *b),
        }
    }
}

/// Lex ONE CQL scalar literal's TEXT SHAPE into a [`CqlLiteral`] — grammar
/// dispatch only (a quoted string is always a string, a UUID's 8-4-4-4-12
/// hex-dash shape is always a UUID, per the CQL grammar itself), never a
/// guess about what the SCHEMA says the column should hold: [`literal_to_value`]
/// still does that schema-driven coercion from the returned [`CqlLiteral`].
///
/// `cql::factory::ParserFactory`'s generic `parse_literal` does not
/// recognise a UUID literal at all (its own doc calls it a "placeholder"),
/// so `--partition`/`--keys-file` need this narrower, purpose-built lexer
/// covering exactly the literal shapes a partition-key column can hold:
/// null, boolean, single-quoted string, UUID, integer, float.
fn lex_cql_literal(s: &str) -> Result<CqlLiteral> {
    let s = s.trim();
    if s.eq_ignore_ascii_case("null") {
        return Ok(CqlLiteral::Null);
    }
    if s.eq_ignore_ascii_case("true") || s.eq_ignore_ascii_case("false") {
        return Ok(CqlLiteral::Boolean(s.eq_ignore_ascii_case("true")));
    }
    if s.len() >= 2 && s.starts_with('\'') && s.ends_with('\'') {
        return Ok(CqlLiteral::String(s[1..s.len() - 1].to_string()));
    }
    if uuid::Uuid::parse_str(s).is_ok() {
        return Ok(CqlLiteral::Uuid(s.to_string()));
    }
    if let Ok(i) = s.parse::<i64>() {
        return Ok(CqlLiteral::Integer(i));
    }
    if let Ok(f) = s.parse::<f64>() {
        return Ok(CqlLiteral::Float(f));
    }
    Err(Error::InvalidInput(format!(
        "could not parse '{s}' as a CQL literal (expected null, true/false, 'quoted text', a \
         UUID, or a number)"
    )))
}

/// Parse ONE key literal (`--partition`'s argument, or one `--keys-file`
/// line) into raw partition-key bytes (design D1.1).
///
/// A single-column partition key accepts the bare literal (`42`,
/// `'us-east'`). A composite key requires the named-column form
/// (`tenant_id=42,region='us-east'` — comma-separated `column=literal`
/// pairs, one per partition-key column) so a schema column reorder, or a
/// typo, names the offending column rather than silently binding to the
/// wrong position.
pub fn parse_key_literal(literal: &str, schema: &TableSchema) -> Result<Vec<u8>> {
    let literal = literal.trim();
    if schema.partition_keys.is_empty() {
        return Err(Error::InvalidInput(format!(
            "{}.{} declares no partition-key columns",
            schema.keyspace, schema.table
        )));
    }
    let mut values: Vec<Option<Value>> = vec![None; schema.partition_keys.len()];

    if schema.partition_keys.len() == 1 && !literal.contains('=') {
        let column = &schema.partition_keys[0];
        let cql_literal = lex_cql_literal(literal)
            .map_err(|e| Error::InvalidInput(format!("key literal '{literal}': {e}")))?;
        let cql_type = CqlType::parse(&column.data_type)?;
        values[0] = Some(literal_to_value(&cql_literal, &cql_type)?);
    } else {
        for segment in literal.split(',') {
            let segment = segment.trim();
            let (name, value_literal) = segment.split_once('=').ok_or_else(|| {
                Error::InvalidInput(format!(
                    "composite key literal segment '{segment}' is not `column=literal` \
                     (key: '{literal}')"
                ))
            })?;
            let name = name.trim();
            let value_literal = value_literal.trim();
            let (position, column) = schema
                .partition_keys
                .iter()
                .enumerate()
                .find(|(_, c)| c.name == name)
                .ok_or_else(|| {
                    Error::InvalidInput(format!(
                        "'{name}' is not a partition-key column of {}.{} (key: '{literal}')",
                        schema.keyspace, schema.table
                    ))
                })?;
            let cql_literal = lex_cql_literal(value_literal).map_err(|e| {
                Error::InvalidInput(format!(
                    "key literal '{value_literal}' for column '{name}': {e}"
                ))
            })?;
            let cql_type = CqlType::parse(&column.data_type)?;
            values[position] = Some(literal_to_value(&cql_literal, &cql_type)?);
        }
    }

    let mut ordered = Vec::with_capacity(values.len());
    for (i, v) in values.into_iter().enumerate() {
        ordered.push(v.ok_or_else(|| {
            Error::InvalidInput(format!(
                "key literal '{literal}' is missing partition-key column '{}'",
                schema.partition_keys[i].name
            ))
        })?);
    }
    encode_partition_key_columns(&ordered, schema)
}

/// Resolve `(a, b]` to the deduplicated raw-key set of every partition
/// across `generation_paths` whose Murmur3 token qualifies (design D1.1). A
/// full boundary-source scan of every input generation, bounded by
/// partition count, not by the width of the range — documented as a known
/// cost (proposal non-goals), not optimized in this slice.
///
/// # Declared scope gap: BTI narrow (`DataOffset`) leaves
///
/// BIG `Index.db` entries always carry the raw key, so BIG coverage is
/// complete. A BTI `Partitions.db` narrow leaf's trie payload is a byte-
/// comparable PREFIX only (see `salvage::boundaries::BoundaryEntry`'s doc) —
/// reconstructing the true raw key from it needs a Data.db decode this
/// resolver does not perform, so a `--token-range` extract against a BTI
/// table with narrow leaves may under-match. This is a named, examined
/// trade (not a silent heuristic): closing it needs the same decode-and-
/// recover step `raw_copy`/`split` already pay per SELECTED partition,
/// applied here to every UNSELECTED one just to learn its key — tracked as
/// follow-up rather than paid in this slice. BTI wide (`RowsOffset`) leaves
/// carry `expected_key` and resolve correctly.
fn resolve_token_range_to_keys(
    generation_paths: &[PathBuf],
    a: i64,
    b: i64,
) -> Result<Vec<Vec<u8>>> {
    let mut seen: HashSet<Vec<u8>> = HashSet::new();
    let mut keys = Vec::new();
    for path in generation_paths {
        let (base, is_bti) = base_and_format(path)?;
        let dir = path.parent().ok_or_else(|| {
            Error::InvalidInput(format!(
                "input path has no parent directory: {}",
                path.display()
            ))
        })?;
        let boundaries = enumerate_boundaries(dir, &base, is_bti).map_err(|refusal| {
            Error::InvalidInput(format!(
                "boundary source for {} unreadable: {}",
                path.display(),
                refusal.remedy
            ))
        })?;
        for entry in &boundaries.entries {
            let Some(key) = entry.expected_key.as_ref() else {
                continue;
            };
            let token = crate::util::cassandra_murmur3::cassandra_murmur3_token(key);
            if token > a && token <= b && seen.insert(key.clone()) {
                keys.push(key.clone());
            }
        }
    }
    Ok(keys)
}
