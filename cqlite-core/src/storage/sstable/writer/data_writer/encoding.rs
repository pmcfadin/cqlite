//! Value/element serialization and op-classification helpers used across the writer (serialize_value, collection elements, static-op classification, range-tombstone coverage).
//!
//! Part of the `data_writer` responsibility split (issue #1118). `use super::*`
//! provides the crate imports and sibling helpers re-exported from
//! `data_writer/mod.rs`. No emitted bytes change.

use super::*;

pub(crate) fn column_order_key(column: &Column) -> (bool, &str) {
    (is_complex_column(&column.data_type), column.name.as_str())
}

/// The column name targeted by a cell operation, if any (issue #927: used by
/// mixed-stream reconciliation). `DeleteRow` targets no specific column.
pub(crate) fn merged_op_column(
    op: &crate::storage::write_engine::mutation::CellOperation,
) -> Option<&str> {
    use crate::storage::write_engine::mutation::CellOperation;
    match op {
        CellOperation::Write { column, .. }
        | CellOperation::WriteWithTtl { column, .. }
        | CellOperation::Delete { column, .. }
        | CellOperation::WriteComplexElement { column, .. }
        | CellOperation::ComplexDeletion { column, .. } => Some(column.as_str()),
        CellOperation::DeleteRow => None,
    }
}

/// The `localDeletionTime` (seconds, GC clock) to stamp a cell tombstone with for
/// an op carried by `mutation` (#921 finding 2).
///
/// A `CellOperation::Delete` that carries its OWN `local_deletion_time` (set by
/// the compaction merge→rewrite path from the SOURCE cell tombstone's surfaced
/// LDT) is honored VERBATIM so a surviving within-grace cell tombstone keeps its
/// original GC clock across compactions. For every other op — and for a `Delete`
/// with no surfaced LDT (`None`) — fall back to the mutation's
/// [`effective_local_deletion_time`](crate::storage::write_engine::mutation::Mutation::effective_local_deletion_time),
/// preserving the historical behavior (WAL / CQL-DELETE paths, row-derived LDT).
/// Choose the `localDeletionTime` the cell/row tombstone of `op` is actually
/// emitted with.
///
/// A `CellOperation::Delete { local_deletion_time: Some(ldt), .. }` carries an
/// explicit per-cell LDT (#921 finding 2) that the DataWriter stamps VERBATIM;
/// every other op (a `Delete` without a per-cell LDT, or a `DeleteRow`) derives
/// the LDT from the enclosing mutation. The SSTable writer's STATS/baseline
/// collection MUST record this exact value (not just
/// `mutation.effective_local_deletion_time()`), otherwise a per-cell LDT below
/// the mutation-derived value underflows the Data.db delta encoding, and one
/// above it leaves Statistics.db min/max/histogram describing a tombstone that
/// was never written (#921 finding 2 — roborev Medium).
pub(crate) fn op_cell_local_deletion_time(
    op: &crate::storage::write_engine::mutation::CellOperation,
    mutation: &Mutation,
) -> i32 {
    use crate::storage::write_engine::mutation::CellOperation;
    match op {
        CellOperation::Delete {
            local_deletion_time: Some(ldt),
            ..
        } => *ldt,
        _ => mutation.effective_local_deletion_time(),
    }
}

/// The write timestamp (microseconds) to stamp on the static cell emitted for
/// `op` within `mutation` (issue #1018).
///
/// A `Write`/`WriteWithTtl`/`Delete` op carries its OWN per-cell write timestamp
/// when the compaction merge→mutation path recorded one in
/// [`Mutation::cell_write_timestamps`](crate::storage::write_engine::mutation::Mutation::cell_write_timestamps)
/// (a surviving live cell's writetime, or a static cell tombstone's
/// `markedForDeleteAt`). It is resolved via
/// [`Mutation::cell_write_timestamp`](crate::storage::write_engine::mutation::Mutation::cell_write_timestamp),
/// which falls back to the mutation's row `timestamp_micros` when no per-cell
/// override exists. Every other op (and the no-override case) resolves to exactly
/// `mutation.timestamp_micros`, so the single-writetime behavior is unchanged.
///
/// Shared by `collect_static_operations` (the compaction/partition path) and
/// `DataWriter::write_static_row` (the public single-mutation entry point) so both
/// resolve per-cell static writetimes IDENTICALLY — without it the public entry
/// point would rewrite older static cells (live OR cell tombstones) to the row max.
pub(crate) fn op_cell_write_timestamp(
    op: &crate::storage::write_engine::mutation::CellOperation,
    mutation: &Mutation,
) -> i64 {
    use crate::storage::write_engine::mutation::CellOperation;
    match op {
        CellOperation::Write { column, .. }
        | CellOperation::WriteWithTtl { column, .. }
        | CellOperation::Delete { column, .. } => mutation.cell_write_timestamp(column),
        _ => mutation.timestamp_micros,
    }
}

/// Generate a version-1 TimeUUID for use as a list cell path.
///
/// List elements in Cassandra use TimeUUIDs as cell paths to maintain insertion order.
/// Each call with a different `element_index` produces a monotonically increasing UUID.
///
/// # Arguments
/// * `timestamp_micros` - Mutation timestamp in microseconds since Unix epoch
/// * `element_index` - Index of the element within the list (for monotonic ordering)
pub(crate) fn generate_list_cell_path_timeuuid(
    timestamp_micros: i64,
    element_index: u64,
) -> [u8; 16] {
    // UUID v1 timestamp: 100-nanosecond intervals since UUID epoch (Oct 15, 1582)
    // Offset from Unix epoch to UUID epoch in 100-ns units
    const UUID_EPOCH_OFFSET: u64 = 0x01B2_1DD2_1381_4000;

    let ts_100ns = (timestamp_micros as u64) * 10 + element_index;
    let uuid_ts = ts_100ns + UUID_EPOCH_OFFSET;

    // Extract time fields per RFC 4122
    let time_low = (uuid_ts & 0xFFFF_FFFF) as u32;
    let time_mid = ((uuid_ts >> 32) & 0xFFFF) as u16;
    let time_hi = ((uuid_ts >> 48) & 0x0FFF) as u16 | 0x1000; // version 1

    // Fixed clock_seq and node for deterministic output
    let clock_seq: u16 = 0x80; // variant bits (10xx) + seq=0
    let node: [u8; 6] = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00];

    let mut uuid = [0u8; 16];
    uuid[0..4].copy_from_slice(&time_low.to_be_bytes());
    uuid[4..6].copy_from_slice(&time_mid.to_be_bytes());
    uuid[6..8].copy_from_slice(&time_hi.to_be_bytes());
    uuid[8] = (clock_seq >> 8) as u8;
    uuid[9] = (clock_seq & 0xFF) as u8;
    uuid[10..16].copy_from_slice(&node);

    uuid
}

/// Convert a usize length to i32 for Cassandra's collection wire format.
/// Returns an error if the length exceeds i32::MAX.
pub(crate) fn len_as_i32(len: usize) -> Result<i32> {
    i32::try_from(len).map_err(|_| {
        Error::InvalidInput(format!(
            "Length {} exceeds maximum i32 for collection encoding",
            len
        ))
    })
}

/// Serialize a collection element, rejecting null (CQL semantics: lists/sets cannot contain null).
pub(crate) fn serialize_collection_element(
    value: &Value,
    collection_kind: &str,
) -> Result<Vec<u8>> {
    if matches!(value, Value::Null) {
        return Err(Error::InvalidInput(format!(
            "{} elements cannot be null (CQL semantics)",
            collection_kind
        )));
    }
    serialize_value(value)
}

/// Serialize a Value to bytes for cell storage
///
/// This follows Cassandra's type-specific serialization rules.
pub(crate) fn serialize_value(value: &Value) -> Result<Vec<u8>> {
    match value {
        Value::Null => Ok(Vec::new()),
        Value::Boolean(b) => Ok(vec![if *b { 1 } else { 0 }]),
        Value::TinyInt(n) => Ok(vec![*n as u8]),
        Value::SmallInt(n) => Ok(n.to_be_bytes().to_vec()),
        Value::Integer(n) => Ok(n.to_be_bytes().to_vec()),
        Value::BigInt(n) => Ok(n.to_be_bytes().to_vec()),
        Value::Counter(n) => Ok(n.to_be_bytes().to_vec()),
        Value::Float32(f) => Ok(f.to_bits().to_be_bytes().to_vec()),
        Value::Float(f) => Ok(f.to_bits().to_be_bytes().to_vec()),
        Value::Text(s) => Ok(s.as_bytes().to_vec()),
        Value::Blob(bytes) => Ok(bytes.clone()),
        Value::Timestamp(millis) => Ok(millis.to_be_bytes().to_vec()),
        Value::Date(days) => {
            // Cassandra DATE: stored as unsigned int with Integer.MIN_VALUE offset
            let stored = days.wrapping_sub(i32::MIN) as u32;
            Ok(stored.to_be_bytes().to_vec())
        }
        Value::Time(nanos) => Ok(nanos.to_be_bytes().to_vec()),
        Value::Uuid(bytes) => Ok(bytes.to_vec()),
        Value::Inet(bytes) => Ok(bytes.clone()),
        Value::Varint(bytes) => Ok(bytes.clone()),
        Value::Decimal { scale, unscaled } => {
            let mut result = Vec::new();
            result.extend_from_slice(&scale.to_be_bytes());
            result.extend_from_slice(unscaled);
            Ok(result)
        }
        Value::Duration {
            months,
            days,
            nanos,
        } => {
            let mut result = Vec::new();
            // Cassandra DurationType stores three signed VInts, not fixed-width ints.
            encode_signed(*months as i64, &mut result);
            encode_signed(*days as i64, &mut result);
            encode_signed(*nanos, &mut result);
            Ok(result)
        }
        Value::Udt(udt_value) => {
            // Construct UdtTypeDef from UdtValue fields by inferring types
            let mut schema =
                UdtTypeDef::new(udt_value.keyspace.clone(), udt_value.type_name.clone());

            // Infer field types from values
            for field in &udt_value.fields {
                let field_type = infer_cql_type_from_value(field.value.as_ref());
                schema = schema.with_field(field.name.clone(), field_type, true);
            }

            let serializer = TypeSerializer::new();
            serializer.serialize_udt(value, &schema)
        }
        Value::List(elements) | Value::Set(elements) => {
            let mut buf = Vec::new();
            buf.extend_from_slice(&len_as_i32(elements.len())?.to_be_bytes());
            for elem in elements {
                let elem_bytes = serialize_collection_element(elem, "Collection")?;
                buf.extend_from_slice(&len_as_i32(elem_bytes.len())?.to_be_bytes());
                buf.extend_from_slice(&elem_bytes);
            }
            Ok(buf)
        }
        Value::Map(entries) => {
            let mut buf = Vec::new();
            buf.extend_from_slice(&len_as_i32(entries.len())?.to_be_bytes());
            for (key, val) in entries {
                if matches!(key, Value::Null) {
                    return Err(Error::InvalidInput(
                        "MAP keys cannot be null (CQL semantics)".to_string(),
                    ));
                }
                let key_bytes = serialize_value(key)?;
                buf.extend_from_slice(&len_as_i32(key_bytes.len())?.to_be_bytes());
                buf.extend_from_slice(&key_bytes);
                let val_bytes = serialize_value(val)?;
                buf.extend_from_slice(&len_as_i32(val_bytes.len())?.to_be_bytes());
                buf.extend_from_slice(&val_bytes);
            }
            Ok(buf)
        }
        Value::Tuple(fields) => {
            let mut buf = Vec::new();
            for field in fields {
                match field {
                    Value::Null => buf.extend_from_slice(&(-1i32).to_be_bytes()),
                    other => {
                        let field_bytes = serialize_value(other)?;
                        buf.extend_from_slice(&len_as_i32(field_bytes.len())?.to_be_bytes());
                        buf.extend_from_slice(&field_bytes);
                    }
                }
            }
            Ok(buf)
        }
        Value::Frozen(inner) => serialize_value(inner),
        _ => Err(Error::InvalidInput(format!(
            "Unsupported value type for serialization: {:?}",
            value
        ))),
    }
}

/// Infer CQL type from a Value instance
///
/// Used for UDT serialization when schema context is not available.
/// Empty collections still fall back to `text` because there is no element
/// value available to inspect.
pub(crate) fn infer_cql_type_from_value(value: Option<&Value>) -> CqlType {
    match value {
        None | Some(Value::Null) => CqlType::Text, // Default for NULL
        Some(Value::Boolean(_)) => CqlType::Boolean,
        Some(Value::TinyInt(_)) => CqlType::TinyInt,
        Some(Value::SmallInt(_)) => CqlType::SmallInt,
        Some(Value::Integer(_)) => CqlType::Int,
        Some(Value::BigInt(_)) => CqlType::BigInt,
        Some(Value::Float32(_)) => CqlType::Float,
        Some(Value::Float(_)) => CqlType::Double,
        Some(Value::Text(_)) => CqlType::Text,
        Some(Value::Blob(_)) => CqlType::Blob,
        Some(Value::Timestamp(_)) => CqlType::Timestamp,
        Some(Value::Date(_)) => CqlType::Date,
        Some(Value::Time(_)) => CqlType::Time,
        Some(Value::Uuid(_)) => CqlType::Uuid,
        Some(Value::Inet(_)) => CqlType::Inet,
        Some(Value::Varint(_)) => CqlType::Varint,
        Some(Value::Decimal { .. }) => CqlType::Decimal,
        Some(Value::Duration { .. }) => CqlType::Duration,
        Some(Value::Counter(_)) => CqlType::Counter,
        Some(Value::List(elements)) => CqlType::List(Box::new(
            elements
                .first()
                .map(|elem| infer_cql_type_from_value(Some(elem)))
                .unwrap_or(CqlType::Text),
        )),
        Some(Value::Set(elements)) => CqlType::Set(Box::new(
            elements
                .first()
                .map(|elem| infer_cql_type_from_value(Some(elem)))
                .unwrap_or(CqlType::Text),
        )),
        Some(Value::Map(entries)) => {
            let (key_type, value_type) = entries
                .first()
                .map(|(key, value)| {
                    (
                        infer_cql_type_from_value(Some(key)),
                        infer_cql_type_from_value(Some(value)),
                    )
                })
                .unwrap_or((CqlType::Text, CqlType::Text));
            CqlType::Map(Box::new(key_type), Box::new(value_type))
        }
        Some(Value::Tuple(fields)) => CqlType::Tuple(
            fields
                .iter()
                .map(|field| infer_cql_type_from_value(Some(field)))
                .collect(),
        ),
        Some(Value::Udt(udt)) => CqlType::Udt(
            udt.type_name.clone(),
            udt.fields
                .iter()
                .map(|field| {
                    (
                        field.name.clone(),
                        infer_cql_type_from_value(field.value.as_ref()),
                    )
                })
                .collect(),
        ),
        Some(Value::Frozen(inner)) => {
            CqlType::Frozen(Box::new(infer_cql_type_from_value(Some(inner))))
        }
        Some(Value::Tombstone(_)) => CqlType::Text, // Tombstones shouldn't appear in UDT fields
        Some(Value::Json(_)) => CqlType::Text,      // JSON is stored as text
    }
}

pub(crate) fn cell_value_uses_length_prefix(value: &Value) -> bool {
    !matches!(
        value,
        Value::Boolean(_)
            | Value::Integer(_)
            | Value::BigInt(_)
            | Value::Float32(_)
            | Value::Float(_)
            | Value::Timestamp(_)
            | Value::Uuid(_)
    )
}

pub(crate) fn is_static_row_mutation(mutation: &Mutation, schema: &TableSchema) -> bool {
    if mutation.clustering_key.is_some() || !schema.columns.iter().any(|column| column.is_static) {
        return false;
    }

    mutation.operations.iter().all(|operation| match operation {
        crate::storage::write_engine::mutation::CellOperation::Write { column, .. }
        | crate::storage::write_engine::mutation::CellOperation::WriteWithTtl { column, .. }
        | crate::storage::write_engine::mutation::CellOperation::Delete { column, .. }
        | crate::storage::write_engine::mutation::CellOperation::WriteComplexElement {
            column,
            ..
        }
        | crate::storage::write_engine::mutation::CellOperation::ComplexDeletion {
            column, ..
        } => schema
            .columns
            .iter()
            .find(|candidate| candidate.name == *column)
            .map(|candidate| candidate.is_static)
            .unwrap_or(false),
        crate::storage::write_engine::mutation::CellOperation::DeleteRow => true,
    })
}

/// Returns true if this single operation targets a static column.
pub(crate) fn is_static_operation(
    op: &crate::storage::write_engine::mutation::CellOperation,
    schema: &TableSchema,
) -> bool {
    match op {
        crate::storage::write_engine::mutation::CellOperation::Write { column, .. }
        | crate::storage::write_engine::mutation::CellOperation::WriteWithTtl { column, .. }
        | crate::storage::write_engine::mutation::CellOperation::Delete { column, .. }
        | crate::storage::write_engine::mutation::CellOperation::WriteComplexElement {
            column,
            ..
        }
        | crate::storage::write_engine::mutation::CellOperation::ComplexDeletion {
            column, ..
        } => schema
            .columns
            .iter()
            .find(|c| c.name == *column)
            .map(|c| c.is_static)
            .unwrap_or(false),
        crate::storage::write_engine::mutation::CellOperation::DeleteRow => false,
    }
}

/// Returns true if `column` is part of the primary key — a partition-key or
/// clustering-key column.
///
/// Primary-key columns are encoded positionally (the partition key and the row's
/// clustering prefix); they must NEVER be written as regular cells. The compaction
/// path can surface a clustering column as a `Write` op (the merger keeps the
/// clustering cell for its own read-back, and `merge_entry_to_mutation` turns it
/// into a `Write`); emitting it as a cell writes the value a second time and
/// corrupts the row body for strict readers (#857). The writer drops such ops.
pub(crate) fn is_primary_key_column(column: &str, schema: &TableSchema) -> bool {
    schema.partition_keys.iter().any(|k| k.name == column)
        || schema.clustering_keys.iter().any(|k| k.name == column)
}

/// Collect and merge static-column operations from all mutations in a partition.
///
/// Scans every mutation (regardless of whether it has a clustering key) and
/// collects operations that target static columns.  Last-write-wins by
/// `timestamp_micros` when the same column is written more than once.
///
/// Mutations at or before `shadow_floor` (the partition tombstone's deletion
/// timestamp) are skipped: their static cells are shadowed and an sstable
/// must be internally reconciled (see `DataWriter::write_partition`).
///
/// Returns the merged operations in an unspecified order (the writer will
/// sort them by schema column order when building the row body). Each op
/// carries the originating mutation's timestamp and explicit local deletion
/// time (Issue #764) so a surviving older static delete keeps its own LDT
/// instead of inheriting the newest static mutation's value.
pub(crate) fn collect_static_operations(
    mutations: &[Mutation],
    schema: &TableSchema,
    shadow_floor: Option<i64>,
) -> Vec<StaticMergedOp> {
    use std::collections::HashMap;

    // Map: column_name → winning StaticMergedOp (last-write-wins by timestamp).
    let mut best: HashMap<String, StaticMergedOp> = HashMap::new();

    for mutation in mutations {
        if shadow_floor.is_some_and(|floor| mutation.timestamp_micros <= floor) {
            continue;
        }
        for op in &mutation.operations {
            if !is_static_operation(op, schema) {
                continue;
            }
            let col_name = match op {
                crate::storage::write_engine::mutation::CellOperation::Write { column, .. }
                | crate::storage::write_engine::mutation::CellOperation::WriteWithTtl {
                    column,
                    ..
                }
                | crate::storage::write_engine::mutation::CellOperation::Delete {
                    column, ..
                } => column.clone(),
                // Per-element complex ops (epic #899) are not produced for STATIC
                // complex columns by the (Phase B) capability — they flow through
                // the regular-row per-element path. Skip them here defensively.
                crate::storage::write_engine::mutation::CellOperation::WriteComplexElement {
                    ..
                }
                | crate::storage::write_engine::mutation::CellOperation::ComplexDeletion {
                    ..
                } => continue,
                crate::storage::write_engine::mutation::CellOperation::DeleteRow => continue,
            };
            // Issue #1018: a static `Write`/`WriteWithTtl`/`Delete` cell carries
            // its OWN per-cell timestamp when the compaction merge→mutation path
            // recorded one in `Mutation::cell_write_timestamps` (it differs from
            // the row's `timestamp_micros`) — a live cell's writetime OR a static
            // cell tombstone's markedForDeleteAt. Use it for BOTH the stamped
            // candidate timestamp AND the last-write-wins comparison below,
            // mirroring the regular-row path in `rows.rs`. Otherwise a compacted
            // static row with surviving static siblings at differing timestamps
            // would rewrite older static cells (live OR tombstone) to the newest
            // static mutation's row max — re-introducing the over-deletion bug for
            // statics. For every other op (and for cells with no per-cell override)
            // this is exactly `mutation.timestamp_micros`, so the single-writetime
            // case is unchanged.
            let candidate_ts = op_cell_write_timestamp(op, mutation);
            // Issue #1018 (roborev HIGH): PER-CELL shadow filtering for statics. The
            // mutation-level `shadow_floor` skip above gates on the ROW MAX
            // (`mutation.timestamp_micros`), so a mutation that survives the floor
            // (because a recent static sibling keeps its row max high) can still
            // carry an individual static `Write`/`WriteWithTtl`/`Delete` whose OWN
            // per-cell timestamp is `<= shadow_floor`. That cell is covered by the
            // partition tombstone and MUST be shadowed exactly as it would have been
            // when every static cell used the row max. Apply the SAME boundary the
            // row-max skip uses (`<= floor`) to the resolved `candidate_ts`,
            // dropping the static op here so it never reaches the LWW map (and so
            // the static liveness/TTL the partition path derives from the SURVIVING
            // ops below cannot resurrect it). A static cell tombstone is itself
            // shadowed on the same floor using its OWN markedForDeleteAt. Every cell
            // with no override already has `candidate_ts == mutation.timestamp_micros
            // > floor`, leaving the single-writetime case unchanged.
            if shadow_floor.is_some_and(|floor| candidate_ts <= floor)
                && matches!(
                    op,
                    crate::storage::write_engine::mutation::CellOperation::Write { .. }
                        | crate::storage::write_engine::mutation::CellOperation::WriteWithTtl { .. }
                        | crate::storage::write_engine::mutation::CellOperation::Delete { .. }
                )
            {
                continue;
            }
            let candidate = StaticMergedOp {
                // #921 finding 2: preserve a `Delete` cell tombstone's own surfaced
                // LDT; other ops fall back to the mutation's effective LDT.
                cell_local_deletion_time: op_cell_local_deletion_time(op, mutation),
                op: op.clone(),
                timestamp_micros: candidate_ts,
                // #1196: carry statement-level TTL so a static `USING TTL` Write
                // is emitted as an expiring cell, not a non-expiring one.
                row_ttl_seconds: mutation.ttl_seconds,
            };
            match best.entry(col_name) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(candidate);
                }
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    if candidate.timestamp_micros >= entry.get().timestamp_micros {
                        entry.insert(candidate);
                    }
                }
            }
        }
    }

    best.into_values().collect()
}

/// Derive a static row's liveness timestamp + TTL from the SURVIVING merged
/// static ops (issue #1018, roborev HIGH).
///
/// The static row carries no row-level liveness in the emitted bytes (#1196 —
/// the writetime rides on each static CELL), but the partition path still
/// threads a `(latest_ts, ttl)` pair into `write_static_row_with_prev_size`.
/// That pair MUST be derived from the ops that actually SURVIVED
/// `collect_static_operations` — each carrying its own per-cell
/// `timestamp_micros` after the per-cell shadow floor — NOT from the set of
/// mutations that merely cleared the floor on their ROW MAX. A static `Write`
/// whose per-cell writetime is `<= shadow_floor` is already dropped from
/// `merged`, so it cannot contribute liveness/TTL here; deriving from the
/// mutations' row max could otherwise resurrect a shadowed static cell's
/// writetime. Returns the max per-cell timestamp and the TTL of the op holding
/// it (last-write-wins). `None` when there are no surviving ops.
pub(crate) fn static_liveness_from_ops(ops: &[StaticMergedOp]) -> Option<(i64, Option<u32>)> {
    ops.iter()
        .max_by_key(|mop| mop.timestamp_micros)
        .map(|mop| (mop.timestamp_micros, mop.row_ttl_seconds))
}

/// Whether a range tombstone's clustering range covers the given clustering key.
pub(crate) fn range_tombstone_covers(
    rt: &RangeTombstone,
    clustering_key: Option<&ClusteringKey>,
    schema: &TableSchema,
) -> bool {
    use std::cmp::Ordering;

    let Some(ck) = clustering_key else {
        return false;
    };
    let cmp = |bound: &ClusteringKey| ck.compare(bound, schema).unwrap_or_else(|_| ck.cmp(bound));

    let after_start = match &rt.start {
        ClusteringBound::Inclusive(b) => cmp(b) != Ordering::Less,
        ClusteringBound::Exclusive(b) => cmp(b) == Ordering::Greater,
        ClusteringBound::Bottom => true,
        ClusteringBound::Top => false,
    };
    let before_end = match &rt.end {
        ClusteringBound::Inclusive(b) => cmp(b) != Ordering::Greater,
        ClusteringBound::Exclusive(b) => cmp(b) == Ordering::Less,
        ClusteringBound::Top => true,
        ClusteringBound::Bottom => false,
    };
    after_start && before_end
}

/// Serialize value for clustering key (type-specific encoding)
///
/// Fixed-width types: raw bytes (no length prefix)
/// Variable-width types: VInt length + bytes
pub(crate) fn serialize_value_for_clustering(
    value: &Value,
    comparator: &ComparatorType,
) -> Result<Vec<u8>> {
    match (value, comparator) {
        // Fixed-width types (no length prefix)
        (Value::Boolean(b), ComparatorType::Boolean) => Ok(vec![if *b { 1 } else { 0 }]),
        (Value::TinyInt(n), ComparatorType::TinyInt) => Ok(n.to_be_bytes().to_vec()),
        (Value::SmallInt(n), ComparatorType::SmallInt) => Ok(n.to_be_bytes().to_vec()),
        (Value::Integer(n), ComparatorType::Int) => Ok(n.to_be_bytes().to_vec()),
        (Value::BigInt(n), ComparatorType::BigInt) => Ok(n.to_be_bytes().to_vec()),
        (Value::Float32(f), ComparatorType::Float32) => Ok(f.to_bits().to_be_bytes().to_vec()),
        (Value::Float(f), ComparatorType::Float) => Ok(f.to_bits().to_be_bytes().to_vec()),
        (Value::Timestamp(millis), ComparatorType::Timestamp) => Ok(millis.to_be_bytes().to_vec()),
        (Value::Date(days), ComparatorType::Date) => {
            // Cassandra DATE in clustering keys: stored as unsigned int with Integer.MIN_VALUE offset
            let stored = days.wrapping_sub(i32::MIN) as u32;
            let mut result = Vec::new();
            encode_unsigned(4, &mut result);
            result.extend_from_slice(&stored.to_be_bytes());
            Ok(result)
        }
        (Value::Uuid(bytes), ComparatorType::Uuid) => Ok(bytes.to_vec()),

        // Variable-width types (VInt length + bytes)
        (Value::Text(s), ComparatorType::Text) => {
            let bytes = s.as_bytes();
            let mut result = Vec::new();
            encode_unsigned(bytes.len() as u64, &mut result);
            result.extend_from_slice(bytes);
            Ok(result)
        }
        (Value::Blob(bytes), ComparatorType::Blob) => {
            let mut result = Vec::new();
            encode_unsigned(bytes.len() as u64, &mut result);
            result.extend_from_slice(bytes);
            Ok(result)
        }

        // Frozen collections as clustering keys: serialize the full collection bytes with VInt length prefix
        (Value::Frozen(inner), _) => {
            let bytes = serialize_value(inner)?;
            let mut result = Vec::new();
            encode_unsigned(bytes.len() as u64, &mut result);
            result.extend_from_slice(&bytes);
            Ok(result)
        }

        _ => Err(Error::InvalidInput(format!(
            "Type mismatch or unsupported clustering type: value={:?}, comparator={:?}",
            value, comparator
        ))),
    }
}

/// Serialize a `ClusteringKey` as a Cassandra `ClusteringPrefix` byte sequence.
///
/// Format (same as the clustering prefix written in Data.db rows):
/// ```text
/// [header: unsigned VInt]   ← 2 bits per column: 00=present, 10=null
/// [value bytes…]            ← type-specific bytes for each PRESENT column
/// ```
///
/// Returns `Err` if a clustering column type is unknown; the caller falls back
/// to `[0x00]` (empty header VInt, valid for "no columns") in that case.
pub(super) fn serialize_clustering_prefix_to_vec(
    clustering_key: &ClusteringKey,
    schema: &TableSchema,
) -> Result<Vec<u8>> {
    let mut header = 0u64;
    for (i, (_, value)) in clustering_key.columns.iter().enumerate() {
        let state: u64 = match value {
            Value::Null => 2, // NULL
            _ => 0,           // PRESENT
        };
        header |= state << (i * 2);
    }

    let mut buf: Vec<u8> = Vec::new();
    encode_unsigned(header, &mut buf);

    for (i, (_, value)) in clustering_key.columns.iter().enumerate() {
        if !matches!(value, Value::Null) {
            if i >= schema.clustering_keys.len() {
                return Err(crate::error::Error::Schema(format!(
                    "Clustering key index {} out of range (schema has {})",
                    i,
                    schema.clustering_keys.len()
                )));
            }
            let cluster_col = &schema.clustering_keys[i];
            let comparator = ComparatorType::from_data_type(&cluster_col.data_type)?;
            let value_bytes = serialize_value_for_clustering(value, &comparator)?;
            buf.extend_from_slice(&value_bytes);
        }
    }

    Ok(buf)
}

/// `ClusteringPrefix.Kind.CLUSTERING.ordinal()` in Cassandra 5.0
/// (`org.apache.cassandra.db.ClusteringPrefix.Kind`). A row's full clustering key
/// (a promoted-index `firstName`/`lastName`) is always kind `CLUSTERING`; range
/// bounds use the other ordinals (e.g. `EXCL_END_INCL_START_BOUNDARY = 2`,
/// `INCL_END_EXCL_START_BOUNDARY = 5`).
pub(crate) const CLUSTERING_PREFIX_KIND_CLUSTERING: u8 = 4;

/// Serialize a `ClusteringKey` as the promoted-index (`IndexInfo`) `ClusteringPrefix`
/// byte sequence (Issue #1186).
///
/// This is **NOT** the same as the Data.db row clustering prefix
/// ([`serialize_clustering_prefix_to_vec`]). Cassandra serializes a Data.db row's
/// clustering via the values-only `Clustering.serializer` (no kind byte), but it
/// serializes a promoted-index `firstName`/`lastName` via
/// `ClusteringPrefix.serializer.serialize`, which prepends a **leading kind byte**
/// (`Kind.ordinal()`). For a full clustering key that kind is always `CLUSTERING`
/// (`= 4`). Format:
///
/// ```text
/// [kind: 1 byte = 0x04 (CLUSTERING)]
/// [header: unsigned VInt]            ← 2 bits per column: 00=present, 10=null
/// [value bytes…]                     ← type-specific bytes for each PRESENT column
/// ```
///
/// For a single `int` clustering this is the Cassandra-exact 6 bytes
/// `04 00 <4-byte big-endian int>`, matching the real
/// `test_big.wide_partition` `Index.db` fixture (verified byte-for-byte).
///
/// Returns `Err` if a clustering column type is unknown (the caller falls back to
/// `[kind, 0x00]` — an empty `Clustering` — in that case).
pub(super) fn serialize_clustering_prefix_for_index(
    clustering_key: &ClusteringKey,
    schema: &TableSchema,
) -> Result<Vec<u8>> {
    let values = serialize_clustering_prefix_to_vec(clustering_key, schema)?;
    let mut buf = Vec::with_capacity(values.len() + 1);
    buf.push(CLUSTERING_PREFIX_KIND_CLUSTERING);
    buf.extend_from_slice(&values);
    Ok(buf)
}

/// The empty-clustering promoted-index `ClusteringPrefix`: a `Clustering` of kind
/// `CLUSTERING` with no columns (Issue #1186). Used for no-clustering rows and
/// range-bound fallbacks where no per-row clustering values are available. Equals
/// `[0x04 (CLUSTERING)][0x00 (empty values header)]`.
pub(super) fn empty_clustering_prefix_for_index() -> Vec<u8> {
    vec![CLUSTERING_PREFIX_KIND_CLUSTERING, 0x00]
}

/// Serialize a range-tombstone **marker** bound as its promoted-index
/// (`IndexInfo`) `ClusteringPrefix` byte sequence (Issue #1186 roborev MEDIUM).
///
/// A row clustering name is always kind `CLUSTERING` (`0x04`), but a marker is an
/// *unfiltered* too: when a range-tombstone marker becomes an IndexInfo block's
/// `firstName`/`lastName`, Cassandra serializes its **actual bound kind** ordinal,
/// NOT `CLUSTERING`. Cassandra's `ClusteringBoundOrBoundary.Serializer.serialize`
/// (the same writer behind on-disk markers) prepends `Kind.ordinal()`:
///
/// ```text
/// [kind: 1 byte]   ← INCL_START_BOUND=1 / EXCL_END_BOUND=0 / INCL_END_BOUND=6 / EXCL_START_BOUND=7
/// [header: VInt]   ← 2 bits per column: 00=present, 10=null
/// [value bytes…]   ← type-specific bytes for each PRESENT column
/// ```
///
/// The kind selection is **identical** to [`DataWriter::write_range_bound`]:
/// open/close × inclusive/exclusive, with the `Bottom`/`Top` sentinels mapping to
/// the open/close inclusive ordinal and carrying zero clustering values (an empty
/// `ClusteringBound`). This guarantees the promoted-index name's kind byte matches
/// the on-disk marker's kind byte byte-for-byte.
///
/// Returns `Err` only if a clustering column type is unknown (the caller falls back
/// to an empty prefix of the correct kind via [`marker_bound_prefix_for_index`]).
pub(super) fn serialize_marker_bound_prefix_for_index(
    bound: &ClusteringBound,
    is_open: bool,
    schema: &TableSchema,
) -> Result<Vec<u8>> {
    let (kind, clustering) = marker_bound_kind(bound, is_open);
    let mut buf = Vec::new();
    buf.push(kind);
    match clustering {
        Some(ck) => buf.extend_from_slice(&serialize_clustering_prefix_to_vec(ck, schema)?),
        None => buf.push(0x00), // empty values header (no clustering columns)
    }
    Ok(buf)
}

/// The bound-kind-aware empty-prefix fallback for a marker (Issue #1186): the
/// marker's correct `Kind.ordinal()` byte followed by an empty values header
/// (`0x00`). Used when the marker's clustering values cannot be encoded.
pub(super) fn marker_bound_prefix_for_index(bound: &ClusteringBound, is_open: bool) -> Vec<u8> {
    let (kind, _) = marker_bound_kind(bound, is_open);
    vec![kind, 0x00]
}

/// Select the `ClusteringPrefix.Kind` ordinal and clustering values for a marker
/// bound — the SINGLE source of truth shared by the serializer and the fallback.
///
/// Mirrors the `(is_open, bound)` match in [`DataWriter::write_range_bound`]
/// exactly so the promoted-index kind byte equals the on-disk marker kind byte.
fn marker_bound_kind(bound: &ClusteringBound, is_open: bool) -> (u8, Option<&ClusteringKey>) {
    match (is_open, bound) {
        (true, ClusteringBound::Inclusive(ck)) => (INCL_START_BOUND, Some(ck)),
        (true, ClusteringBound::Exclusive(ck)) => (EXCL_START_BOUND, Some(ck)),
        (false, ClusteringBound::Inclusive(ck)) => (INCL_END_BOUND, Some(ck)),
        (false, ClusteringBound::Exclusive(ck)) => (EXCL_END_BOUND, Some(ck)),
        (true, ClusteringBound::Bottom | ClusteringBound::Top) => (INCL_START_BOUND, None),
        (false, ClusteringBound::Bottom | ClusteringBound::Top) => (INCL_END_BOUND, None),
    }
}
