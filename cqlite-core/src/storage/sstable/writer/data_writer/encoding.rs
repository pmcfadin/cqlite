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
///
/// Thin wrapper over [`serialize_collection_element_into`] preserving the
/// owned-`Vec` signature that the comparator fallback (`collection_order`) and
/// tests depend on.
pub(crate) fn serialize_collection_element(
    value: &Value,
    collection_kind: &str,
) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    serialize_collection_element_into(value, collection_kind, &mut out)?;
    Ok(out)
}

/// Serialize a collection element directly into `out`, rejecting null (CQL
/// semantics: lists/sets cannot contain null). Byte-identical to
/// [`serialize_collection_element`] with zero throwaway allocation (issue #1672).
pub(crate) fn serialize_collection_element_into(
    value: &Value,
    collection_kind: &str,
    out: &mut Vec<u8>,
) -> Result<()> {
    if matches!(value, Value::Null) {
        return Err(Error::InvalidInput(format!(
            "{} elements cannot be null (CQL semantics)",
            collection_kind
        )));
    }
    serialize_value_into(value, out)
}

/// Serialize a Value to bytes for cell storage.
///
/// Thin wrapper over [`serialize_value_into`], preserving the owned-`Vec`
/// signature that the comparator, UDT canonicalization, clustering-key
/// serialization, and tests depend on. Callers on the hot write path should
/// prefer [`serialize_value_into`] to write straight into their destination
/// buffer (issue #1672).
pub(crate) fn serialize_value(value: &Value) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    serialize_value_into(value, &mut out)?;
    Ok(out)
}

/// Serialize a Value directly into `out`, following Cassandra's type-specific
/// serialization rules. Byte-identical to [`serialize_value`] but writes into a
/// caller-supplied buffer with no per-value throwaway `Vec` (issue #1672, R1).
///
/// The FIXED-WIDTH i32 length prefixes of List/Set/Map/Tuple are back-patched in
/// place: reserve 4 bytes, serialize the element into `out`, then overwrite the
/// reservation with the encoded length — so nested collections never build a
/// per-element temporary either.
pub(crate) fn serialize_value_into(value: &Value, out: &mut Vec<u8>) -> Result<()> {
    match value {
        Value::Null => {}
        Value::Boolean(b) => out.push(if *b { 1 } else { 0 }),
        Value::TinyInt(n) => out.push(*n as u8),
        Value::SmallInt(n) => out.extend_from_slice(&n.to_be_bytes()),
        Value::Integer(n) => out.extend_from_slice(&n.to_be_bytes()),
        Value::BigInt(n) => out.extend_from_slice(&n.to_be_bytes()),
        Value::Counter(n) => out.extend_from_slice(&n.to_be_bytes()),
        Value::Float32(f) => out.extend_from_slice(&f.to_bits().to_be_bytes()),
        Value::Float(f) => out.extend_from_slice(&f.to_bits().to_be_bytes()),
        // Issue #1644: Text/Blob hold `bytes::Bytes`; `&Bytes` deref-coerces to
        // `&[u8]` for `extend_from_slice` (same as the pre-split `&Vec<u8>`).
        Value::Text(s) => out.extend_from_slice(s),
        Value::Blob(bytes) => out.extend_from_slice(bytes),
        Value::Timestamp(millis) => out.extend_from_slice(&millis.to_be_bytes()),
        Value::Date(days) => {
            // Cassandra DATE: stored as unsigned int with Integer.MIN_VALUE offset
            let stored = days.wrapping_sub(i32::MIN) as u32;
            out.extend_from_slice(&stored.to_be_bytes());
        }
        Value::Time(nanos) => out.extend_from_slice(&nanos.to_be_bytes()),
        Value::Uuid(bytes) => out.extend_from_slice(bytes),
        Value::Inet(bytes) => out.extend_from_slice(bytes),
        Value::Varint(bytes) => out.extend_from_slice(bytes),
        Value::Decimal { scale, unscaled } => {
            out.extend_from_slice(&scale.to_be_bytes());
            out.extend_from_slice(unscaled);
        }
        Value::Duration {
            months,
            days,
            nanos,
        } => {
            // Cassandra DurationType stores three signed VInts, not fixed-width ints.
            encode_signed(*months as i64, out);
            encode_signed(*days as i64, out);
            encode_signed(*nanos, out);
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

            // `serialize_udt` returns an owned Vec (its signature is out of R1
            // scope); one temp per UDT cell is acceptable (R1 targets per-SCALAR
            // -cell allocations).
            let serializer = TypeSerializer::new();
            out.extend_from_slice(&serializer.serialize_udt(value, &schema)?);
        }
        Value::List(elements) => {
            // ListType preserves insertion order — do NOT sort.
            out.extend_from_slice(&len_as_i32(elements.len())?.to_be_bytes());
            for elem in elements {
                write_len_prefixed_i32(out, |o| {
                    serialize_collection_element_into(elem, "Collection", o)
                })?;
            }
        }
        Value::Set(elements) => {
            // Cassandra SetType is a sorted collection: a frozen set serializes its
            // elements in the SetType element-type comparator's order (issue #1254,
            // #1275). Order by `compare_collection_elements` — SIGNED for numeric
            // element types, unsigned-byte otherwise — decided from the element
            // `Value`s (authoritative type), the SAME single comparator the
            // non-frozen SET cell path uses (`write_set_complex_cells` in
            // complex.rs). A raw serialized-byte sort (the pre-#1275 behavior) put
            // negatives last, e.g. `frozen<set<int>>` of {-1,0,1} serialized in the
            // wrong order. Then serialize each element in that order
            // (`serialize_collection_element_into` rejects Value::Null). Sorting the
            // `&Value` refs needs no serialization.
            let mut ordered: Vec<&Value> = elements.iter().collect();
            ordered.sort_by(|a, b| compare_collection_elements(a, b));

            out.extend_from_slice(&len_as_i32(ordered.len())?.to_be_bytes());
            for elem in ordered {
                write_len_prefixed_i32(out, |o| {
                    serialize_collection_element_into(elem, "Collection", o)
                })?;
            }
        }
        Value::Map(entries) => {
            // Cassandra MapType is a sorted collection: a frozen map serializes its
            // entries in the KEY-type comparator's order (issue #1254, #1275). Order
            // by `compare_collection_elements` over the KEY `Value`s — SIGNED for
            // numeric key types, unsigned-byte otherwise — the SAME single
            // comparator the non-frozen MAP cell path uses (`write_map_complex_cells`
            // in complex.rs). A raw serialized-KEY-byte sort (the pre-#1275 behavior)
            // ordered a `map<int,…>` with negative keys wrong. Then serialize each
            // key/value in that order.
            let mut ordered: Vec<&(Value, Value)> = entries.iter().collect();
            ordered.sort_by(|a, b| compare_collection_elements(&a.0, &b.0));

            out.extend_from_slice(&len_as_i32(ordered.len())?.to_be_bytes());
            for (key, val) in ordered {
                if matches!(key, Value::Null) {
                    return Err(Error::InvalidInput(
                        "MAP keys cannot be null (CQL semantics)".to_string(),
                    ));
                }
                write_len_prefixed_i32(out, |o| serialize_value_into(key, o))?;
                write_len_prefixed_i32(out, |o| serialize_value_into(val, o))?;
            }
        }
        Value::Tuple(fields) => {
            for field in fields {
                match field {
                    Value::Null => out.extend_from_slice(&(-1i32).to_be_bytes()),
                    other => write_len_prefixed_i32(out, |o| serialize_value_into(other, o))?,
                }
            }
        }
        Value::Frozen(inner) => serialize_value_into(inner, out)?,
        _ => {
            return Err(Error::InvalidInput(format!(
                "Unsupported value type for serialization: {:?}",
                value
            )))
        }
    }
    Ok(())
}

/// Run `write_body`, framing the bytes it appends to `out` with a FIXED 4-byte
/// big-endian i32 length prefix — reserved before the body and back-patched
/// after — so a collection element/field never needs a per-element temporary
/// (issue #1672). Byte-identical to `extend(len_as_i32(bytes.len())); extend(bytes)`.
fn write_len_prefixed_i32(
    out: &mut Vec<u8>,
    write_body: impl FnOnce(&mut Vec<u8>) -> Result<()>,
) -> Result<()> {
    let len_pos = out.len();
    out.extend_from_slice(&[0u8; 4]);
    write_body(out)?;
    let body_len = out.len() - len_pos - 4;
    out[len_pos..len_pos + 4].copy_from_slice(&len_as_i32(body_len)?.to_be_bytes());
    Ok(())
}

/// Append a regular cell's value to `buf` with Cassandra's cell framing — a
/// VInt length prefix for variable-width types, raw bytes for fixed-width — and
/// the `i64::MAX` length bound check.
///
/// Fixed-width scalars (bool/int/bigint/float/timestamp/uuid — the types WITHOUT
/// a length prefix, per [`cell_value_uses_length_prefix`]) are written STRAIGHT
/// into `buf` via [`serialize_value_into`], eliminating the throwaway per-cell
/// `Vec` + second copy that dominated the int write hot path (issue #1672, R1).
/// Variable-width types still serialize once into an owned buffer because the
/// VInt length must precede the bytes. Byte-identical to the prior
/// `serialize_value` → optional length prefix → `extend_from_slice` sequence.
pub(crate) fn write_cell_value_into(buf: &mut Vec<u8>, column: &str, value: &Value) -> Result<()> {
    if cell_value_uses_length_prefix(value) {
        // Variable-width: the VInt length must precede the bytes, so serialize
        // once into an owned buffer, then length-prefix + copy.
        let value_bytes = serialize_value(value)?;
        if value_bytes.len() > i64::MAX as usize {
            return Err(Error::InvalidInput(format!(
                "Value too large for column '{}': {} bytes (max {})",
                column,
                value_bytes.len(),
                i64::MAX
            )));
        }
        encode_unsigned(value_bytes.len() as u64, buf);
        buf.extend_from_slice(&value_bytes);
    } else {
        // Fixed-width: no length prefix — write straight into `buf` (zero alloc).
        let start = buf.len();
        serialize_value_into(value, buf)?;
        let written = buf.len() - start;
        if written > i64::MAX as usize {
            return Err(Error::InvalidInput(format!(
                "Value too large for column '{}': {} bytes (max {})",
                column,
                written,
                i64::MAX
            )));
        }
    }
    Ok(())
}

/// Infer CQL type from a Value instance
///
/// Used for UDT serialization when schema context is not available.
/// Empty collections still fall back to `text` because there is no element
/// value available to inspect.
pub(crate) fn infer_cql_type_from_value(value: Option<&Value>) -> CqlType {
    match value {
        None | Some(Value::Null) => CqlType::Text, // Default for NULL
        // The sentinel CARRIES its declared type, so inference is exact here
        // rather than a `text` fallback (issue #3805).
        Some(Value::Empty(ty)) => ty.cql_type(),
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
///
/// Issue #1668, stage 5c-ii: a thin wrapper over [`StaticOpsTracker`] — the
/// SAME per-mutation last-write-wins fold, now factored into a running
/// tracker so a future incremental writer entry point (stage 5c-iv) can feed
/// it one cluster group at a time instead of requiring the whole `&[Mutation]`
/// slice upfront. Byte-identical to the prior whole-slice implementation.
pub(crate) fn collect_static_operations(
    mutations: &[Mutation],
    schema: &TableSchema,
    shadow_floor: Option<i64>,
) -> Vec<StaticMergedOp> {
    let mut tracker = StaticOpsTracker::new();
    for mutation in mutations {
        tracker.feed(mutation, schema, shadow_floor);
    }
    tracker.finish()
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
            let bytes = s.as_ref();
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

// Promoted-index (`IndexInfo`) `ClusteringPrefix` serialization lives in the
// `index_prefix` submodule (Issue #1186 / epic #1116 file-size split). The helpers
// are re-exported from `mod.rs` so existing `use super::*` callers (`partition.rs`)
// resolve `serialize_clustering_prefix_for_index`, `empty_clustering_prefix_for_index`,
// `serialize_marker_bound_prefix_for_index`, and `marker_bound_prefix_for_index`
// unchanged.
