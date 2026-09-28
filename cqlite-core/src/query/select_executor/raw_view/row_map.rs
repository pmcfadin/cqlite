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
    // `bigint`, never a saturating `i32` narrow (roborev finding, issue
    // #4222): `SSTableReader::generation` is a `u64`, and this view's whole
    // purpose is authoritative per-generation source identity — collapsing
    // two distinct generations to the same saturated value would be exactly
    // the fabricated-fact class the rest of this module fails closed on.
    // `i64::MAX` remains a saturating fallback ONLY for a generation number
    // past `i64::MAX`, a value no real corpus can produce.
    values.insert(
        "generation".to_string(),
        Value::BigInt(i64::try_from(source.generation).unwrap_or(i64::MAX)),
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
///
/// KNOWN GAP, declared rather than left unexamined (roborev finding, issue
/// #4222 — round 8; spec.md's "A static row is distinguishable from a
/// clustering row" scenario, DEFERRED): `CompactionRow`/`CompactionRowData`
/// do not carry whether the decoder classified this row as a Cassandra
/// STATIC row (`compaction_row_build.rs`'s `is_static`, issue #3809), so a
/// static row reaches here as an ordinary `Live`/`Tombstone` with EMPTY
/// clustering and renders as the same `row_kind = 'row'` a genuine
/// clustering row with missing clustering components would — a silent
/// fidelity gap. Threading `is_static` onto `CompactionRow` (a shared
/// compaction-read data model, not just this view's) is future work.
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
                }
                // The AUTHORITATIVE on-disk row TTL, reported VERBATIM
                // (roborev finding, issue #4222 — round 11). This used to be
                // DERIVED as `expires_at_seconds - marker_timestamp/1_000_000`,
                // which is not a fact: Cassandra computes `localExpirationTime`
                // from the COORDINATOR's `nowInSec` at write time, never from
                // the mutation's write timestamp, so the subtraction diverges
                // by exactly the gap between "now" and an explicit
                // `USING TIMESTAMP`. `INSERT ... USING TIMESTAMP
                // 4102444800000000 AND TTL 60` (year 2100) left the expiry at
                // `now + 60`, so the subtraction underflowed to ≈ `-2.4e9` and
                // `saturating_i32` published `row_ttl = i32::MIN` — a
                // fabricated fact in the very branch whose comment claimed
                // "never a guess (issue #28)". `RowHeader::ttl` was decoded all
                // along and merely dropped when `RowLiveness` was built; it is
                // now threaded through as `ttl_seconds`, matching how the
                // per-cell `<col>_ttl` path has always read `cell.ttl`. A
                // marker with no on-disk TTL reports NO `row_ttl` at all.
                if let Some(ttl) = row_liveness.ttl_seconds {
                    values.insert("row_ttl".to_string(), Value::Integer(ttl));
                }
                if let Some(expires_at) = row_liveness.expires_at_seconds {
                    // ITS OWN COLUMN, never `row_local_deletion_time`
                    // (roborev finding, issue #4222 — round 11). A row
                    // deletion and a TTL'd liveness marker COEXIST on one
                    // physical row (`CompactionRowData::Live::row_deletion`,
                    // issue #932 — e.g. a `DELETE … USING TIMESTAMP 100`
                    // flushed together with a later
                    // `INSERT … USING TIMESTAMP 200 AND TTL 60`), and this
                    // value used to be written into `row_local_deletion_time`
                    // only to be UNCONDITIONALLY OVERWRITTEN by the
                    // tombstone's LDT below — silently discarding a physical
                    // fact in a view whose contract is to lose none. The two
                    // clocks are different facts and now have different
                    // columns: `row_local_deletion_time` is the row
                    // TOMBSTONE's GC clock and nothing else.
                    //
                    // `expires_at` is an HONEST `i64` (write-time + TTL
                    // seconds), never a wrapped on-disk LDT bit pattern — no
                    // cast needed for this `bigint` column (roborev finding,
                    // issue #4222 — round 8).
                    values.insert(
                        "row_liveness_expires_at".to_string(),
                        Value::BigInt(expires_at),
                    );
                }
            }
            if let Some((deletion_time, local_deletion_time)) = row_deletion {
                values.insert("row_tombstone".to_string(), Value::text("row"));
                values.insert(
                    "row_local_deletion_time".to_string(),
                    // `i64::from(x as u32)`, never a bare narrow-to-i32
                    // (roborev finding, issue #4222 — round 8): this
                    // `local_deletion_time` IS the wrapped on-disk bit
                    // pattern (module doc, `compaction_row.rs`'s header),
                    // matching `partition_deletion_time`/`range_deletion_time`'s
                    // round-7 fix.
                    Value::BigInt(i64::from(local_deletion_time as u32)),
                );
                values.insert(
                    "row_deletion_timestamp".to_string(),
                    Value::BigInt(deletion_time),
                );
            }

            insert_source_values(&mut values, source);
            vec![QueryRow::with_values(row.key.clone(), values)]
        }
        CompactionRowData::Tombstone {
            deletion_time,
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
                "row_deletion_timestamp".to_string(),
                Value::BigInt(deletion_time),
            );
            values.insert(
                "row_local_deletion_time".to_string(),
                // See the identical fix + rationale in the `Live` arm above
                // (roborev finding, issue #4222 — round 8).
                Value::BigInt(i64::from(local_deletion_time as u32)),
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
                // `i64::from(x as u32)`, NEVER a bare `as i64` (roborev
                // finding, issue #4222 — round 7): `local_deletion_time` is
                // an `i32` that deliberately carries a far-future LDT via
                // wrapping `as u32 as i32` (`compaction_row.rs`), matching
                // the SAME widening convention `write_engine/merge/{mod,
                // reconcile,streaming}.rs` already use. A bare `as i64`
                // SIGN-EXTENDS that wrapped bit pattern instead, rendering a
                // real far-future LDT as a fabricated large-negative epoch
                // second in this `bigint` column — precisely the
                // authoritative-facts violation this view exists to avoid.
                Value::BigInt(i64::from(local_deletion_time as u32)),
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
        // See the identical fix + rationale on `partition_deletion_time`
        // above (roborev finding, issue #4222 — round 7): `i64::from(x as
        // u32)`, never a bare `as i64` sign-extension of the wrapped bit
        // pattern.
        Value::BigInt(i64::from(local_deletion_time as u32)),
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

    // A tombstoned cell's authoritative local-deletion-time lives on
    // `TombstoneInfo` (`Value::Tombstone(info).local_deletion_time`), NOT on
    // `SimpleCell::local_deletion_time` (which the decoder leaves `None` for
    // a tombstone — that field is populated for a LIVE expiring cell). Prefer
    // the tombstone's own field when the cell is a tombstone; fall back to
    // the cell's own field otherwise (the live-TTL case).
    //
    // `TombstoneInfo::local_deletion_time` is an HONEST `i64` (never a
    // wrapped bit pattern — `types.rs`'s own doc), so no cast is needed
    // there. `SimpleCell::local_deletion_time: Option<i32>` DOES follow the
    // wrapped `as u32 as i32` convention for a far-future LDT
    // (`compaction_row.rs`'s module-header invariant) — widened via
    // `i64::from(x as u32)`, never a bare narrow-to-i32 (roborev finding,
    // issue #4222 — round 8, matching round 7's `partition_deletion_time`
    // fix for the same defect class).
    let (tombstone_kind, tombstone_ldt): (Option<&str>, Option<i64>) = match &cell.value {
        Value::Tombstone(info) => (
            Some(match info.tombstone_type {
                TombstoneType::TtlExpiration => "expired",
                _ => "cell",
            }),
            Some(info.local_deletion_time),
        ),
        _ => (None, None),
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
    let ldt = tombstone_ldt.or_else(|| cell.local_deletion_time.map(|x| i64::from(x as u32)));
    if let Some(ldt) = ldt {
        values.insert(
            format!("{}_local_deletion_time", cell.column),
            Value::BigInt(ldt),
        );
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
            // `ComplexColumn::complex_deletion`'s `i32` element follows the
            // SAME wrapped `as u32 as i32` convention as every other LDT
            // field in this module (`compaction_row.rs`'s header) — widen
            // via `i64::from(x as u32)`, never a bare narrow-to-i32
            // (roborev finding, issue #4222 — round 8).
            Value::BigInt(i64::from(local_deletion_time as u32)),
        );
    }
}

#[cfg(test)]
#[path = "row_map_tests.rs"]
mod tests;
