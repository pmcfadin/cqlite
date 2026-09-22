//! Map a per-generation [`CompactionRow`] into the raw view's [`QueryRow`]
//! shape (design.md D7's column contract, issue #4222). No reconciliation:
//! one `CompactionRow` becomes one `QueryRow` (two for a `RangeMarker` — its
//! start and end bound), each carrying THIS generation's source identity
//! untouched — the deliberate absence of `KWayMerger` (design.md D5).

use crate::query::result::QueryRow;
use crate::schema::TableSchema;
use crate::storage::partition_key_codec::decode_partition_key_columns;
use crate::storage::sstable::reader::compaction_row::{
    CompactionBound, CompactionRow, CompactionRowData, ComplexColumn, SimpleCell,
};
use crate::storage::sstable::reader::SSTableReader;
use crate::types::{RowKey, TombstoneType, Value};
use crate::Result;
use std::collections::{HashMap, HashSet};

/// Source-SSTable identity attached to every row a generation contributes
/// (design.md D7's "source columns, present on every row").
#[derive(Clone)]
pub(in crate::query::select_executor) struct RawViewSource {
    pub sstable: String,
    pub generation: u64,
    pub format: &'static str,
    /// Byte offset of the partition in `Data.db`, when cheaply resolvable.
    /// The point-key producer resolves this from the SAME index lookup it
    /// uses to seek the partition (issue #4222); the full-scan producer
    /// currently leaves this `None` — exposing it there needs the streaming
    /// compaction walk to surface a per-partition offset, which nothing in
    /// this codebase does today (a documented scope limitation, not a
    /// heuristic substitute).
    pub position: Option<i64>,
}

impl RawViewSource {
    /// Build a source identity from an open reader (issue #4222). Shared by
    /// the point-key and full-scan producers so `sstable`/`generation`/
    /// `format` are derived identically on both paths.
    pub(in crate::query::select_executor) fn from_reader(
        reader: &SSTableReader,
        position: Option<i64>,
    ) -> Self {
        let sstable = reader
            .file_path()
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default()
            .to_string();
        Self {
            sstable,
            generation: reader.generation,
            format: reader.sstable_format_label(),
            position,
        }
    }
}

/// Narrow an `i64` to `i32` by SATURATION, never silent two's-complement
/// wraparound (roborev overflow-class self-check) — every field this touches
/// (TTL seconds, generation number, local-deletion-time) is authoritative
/// on-disk/reader data that is always small in practice, so saturation is a
/// safety net, not a correctness path any committed fixture exercises.
fn saturating_i32(v: i64) -> i32 {
    i32::try_from(v).unwrap_or(if v > 0 { i32::MAX } else { i32::MIN })
}

fn insert_source_values(values: &mut HashMap<String, Value>, source: &RawViewSource) {
    values.insert("sstable".to_string(), Value::text(source.sstable.clone()));
    values.insert(
        "generation".to_string(),
        Value::Integer(saturating_i32(source.generation as i64)),
    );
    values.insert("format".to_string(), Value::text(source.format));
    values.insert(
        "position".to_string(),
        source.position.map(Value::BigInt).unwrap_or(Value::Null),
    );
}

fn insert_pk_values(values: &mut HashMap<String, Value>, pk_values: &[(String, Value)]) {
    for (name, value) in pk_values {
        values.insert(name.clone(), value.clone());
    }
}

/// Map one [`CompactionRow`] into its raw-view [`QueryRow`]s — one for
/// `Live`/`Tombstone`/`PartitionDelete`, two (start + end bound) for
/// `RangeMarker`.
pub(in crate::query::select_executor) fn map_compaction_row(
    row: CompactionRow,
    schema: &TableSchema,
    source: &RawViewSource,
) -> Result<Vec<QueryRow>> {
    let clustering_names: HashSet<&str> = schema
        .clustering_keys
        .iter()
        .map(|k| k.name.as_str())
        .collect();
    let pk_values = decode_partition_key_columns(row.key.as_bytes(), schema)?;

    let rows = match row.row_data {
        CompactionRowData::Live {
            simple,
            complex,
            row_deletion,
            row_liveness,
        } => {
            let mut values: HashMap<String, Value> = HashMap::new();
            insert_pk_values(&mut values, &pk_values);
            for cell in &simple {
                insert_simple_cell(&mut values, cell, &clustering_names);
            }
            for col in &complex {
                insert_complex_column(&mut values, col);
            }
            values.insert("row_kind".to_string(), Value::text("row"));

            if row_liveness.has_marker {
                if let Some(ts) = row_liveness.marker_timestamp {
                    values.insert("row_timestamp".to_string(), Value::BigInt(ts));
                    // TTL seconds = expiry (epoch s) - write time (epoch s),
                    // derived from two authoritative on-disk fields — never a
                    // guess (issue #28).
                    if let Some(expires_at) = row_liveness.expires_at_seconds {
                        let ttl = expires_at.saturating_sub(ts / 1_000_000);
                        values.insert("row_ttl".to_string(), Value::Integer(saturating_i32(ttl)));
                    }
                }
                if let Some(expires_at) = row_liveness.expires_at_seconds {
                    values.insert(
                        "row_local_deletion_time".to_string(),
                        Value::Integer(saturating_i32(expires_at)),
                    );
                }
            }
            if let Some((_deletion_time, local_deletion_time)) = row_deletion {
                values.insert("row_tombstone".to_string(), Value::text("row"));
                values.insert(
                    "row_local_deletion_time".to_string(),
                    Value::Integer(local_deletion_time),
                );
            }

            insert_source_values(&mut values, source);
            vec![QueryRow::with_values(row.key.clone(), values)]
        }
        CompactionRowData::Tombstone {
            deletion_time: _,
            local_deletion_time,
            clustering,
        } => {
            let mut values: HashMap<String, Value> = HashMap::new();
            insert_pk_values(&mut values, &pk_values);
            for (name, value) in &clustering {
                values.insert(name.clone(), value.clone());
            }
            values.insert("row_kind".to_string(), Value::text("row"));
            values.insert("row_tombstone".to_string(), Value::text("row"));
            values.insert(
                "row_local_deletion_time".to_string(),
                Value::Integer(local_deletion_time),
            );
            insert_source_values(&mut values, source);
            vec![QueryRow::with_values(row.key.clone(), values)]
        }
        CompactionRowData::PartitionDelete {
            deletion_time,
            local_deletion_time,
        } => {
            let mut values: HashMap<String, Value> = HashMap::new();
            insert_pk_values(&mut values, &pk_values);
            values.insert("row_kind".to_string(), Value::text("partition_tombstone"));
            values.insert(
                "partition_deletion_timestamp".to_string(),
                Value::BigInt(deletion_time),
            );
            values.insert(
                "partition_deletion_time".to_string(),
                Value::BigInt(local_deletion_time as i64),
            );
            insert_source_values(&mut values, source);
            vec![QueryRow::with_values(row.key.clone(), values)]
        }
        CompactionRowData::RangeMarker {
            start,
            end,
            deletion_time,
            local_deletion_time,
        } => {
            vec![
                range_bound_row(
                    &row.key,
                    &pk_values,
                    &start,
                    "range_tombstone_start",
                    deletion_time,
                    local_deletion_time,
                    source,
                ),
                range_bound_row(
                    &row.key,
                    &pk_values,
                    &end,
                    "range_tombstone_end",
                    deletion_time,
                    local_deletion_time,
                    source,
                ),
            ]
        }
    };
    Ok(rows)
}

#[allow(clippy::too_many_arguments)]
fn range_bound_row(
    key: &RowKey,
    pk_values: &[(String, Value)],
    bound: &CompactionBound,
    row_kind: &'static str,
    deletion_time: i64,
    local_deletion_time: i32,
    source: &RawViewSource,
) -> QueryRow {
    let mut values: HashMap<String, Value> = HashMap::new();
    insert_pk_values(&mut values, pk_values);
    let bound_inclusive = match bound {
        CompactionBound::Inclusive(components) => {
            for (name, value) in components {
                values.insert(name.clone(), value.clone());
            }
            Some(true)
        }
        CompactionBound::Exclusive(components) => {
            for (name, value) in components {
                values.insert(name.clone(), value.clone());
            }
            Some(false)
        }
        // Open bounds (before/after every clustering key) carry no clustering
        // component and no literal inclusivity to report.
        CompactionBound::Bottom | CompactionBound::Top => None,
    };
    values.insert("row_kind".to_string(), Value::text(row_kind));
    if let Some(inclusive) = bound_inclusive {
        values.insert("bound_inclusive".to_string(), Value::Boolean(inclusive));
    }
    values.insert(
        "range_deletion_timestamp".to_string(),
        Value::BigInt(deletion_time),
    );
    values.insert(
        "range_deletion_time".to_string(),
        Value::BigInt(local_deletion_time as i64),
    );
    insert_source_values(&mut values, source);
    QueryRow::with_values(key.clone(), values)
}

fn insert_simple_cell(
    values: &mut HashMap<String, Value>,
    cell: &SimpleCell,
    clustering_names: &HashSet<&str>,
) {
    let live_value = match &cell.value {
        Value::Tombstone(_) => Value::Null,
        other => other.clone(),
    };

    if clustering_names.contains(cell.column.as_str()) {
        // Clustering columns are surfaced as plain data columns, not per-cell
        // metadata quads (design.md D7: the quad is for non-key columns only).
        values.insert(cell.column.clone(), live_value);
        return;
    }

    let tombstone_kind = match &cell.value {
        Value::Tombstone(info) => Some(match info.tombstone_type {
            TombstoneType::TtlExpiration => "expired",
            _ => "cell",
        }),
        _ => None,
    };
    values.insert(cell.column.clone(), live_value);
    values.insert(
        format!("{}_timestamp", cell.column),
        Value::BigInt(cell.timestamp),
    );
    if let Some(ttl) = cell.ttl {
        values.insert(
            format!("{}_ttl", cell.column),
            Value::Integer(saturating_i32(ttl as i64)),
        );
    }
    if let Some(ldt) = cell.local_deletion_time {
        values.insert(format!("{}_local_deletion_time", cell.column), Value::Integer(ldt));
    }
    if let Some(kind) = tombstone_kind {
        values.insert(format!("{}_tombstone", cell.column), Value::text(kind));
    }
}

fn insert_complex_column(values: &mut HashMap<String, Value>, col: &ComplexColumn) {
    values.insert(col.column.clone(), col.collapsed_value.clone());
    values.insert(
        format!("{}_complex_deletion", col.column),
        Value::Boolean(col.complex_deletion.is_some()),
    );
    if let Some((deletion_time, local_deletion_time)) = col.complex_deletion {
        values.insert(
            format!("{}_complex_deletion_timestamp", col.column),
            Value::BigInt(deletion_time),
        );
        values.insert(
            format!("{}_complex_deletion_time", col.column),
            Value::Integer(local_deletion_time),
        );
    }
}
