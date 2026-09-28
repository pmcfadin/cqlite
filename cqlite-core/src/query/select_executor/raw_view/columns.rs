//! Table-name suffix detection + the raw view's column contract (issue #4222,
//! design.md D1/D6/D7).

use super::super::column_info_from_type_str;
use crate::query::result::ColumnInfo;
use crate::schema::{CqlType, TableSchema};
use crate::Error;
use std::collections::HashSet;

pub(in crate::query::select_executor) use crate::query::raw_view_naming::strip_raw_view_suffix;
/// Suffix that marks a table reference as the raw-SSTable-view name for its
/// base table (design.md D1 — a naming convention over the existing flat
/// `keyspace.table` namespace, never a new grammar). The single source of
/// truth lives in [`crate::query::raw_view_naming`] — shared with
/// `Database::has_schema_for_table`/`schema_status`, so the CLI's pre-flight
/// schema check (issue #199) recognizes a raw-view name too, instead of
/// rejecting every raw-view query before `SelectExecutor` can intercept it.
pub(in crate::query::select_executor) use crate::query::raw_view_naming::RAW_SSTABLE_VIEW_SUFFIX as RAW_VIEW_SUFFIX;

/// `Some(true)` for a CQL type whose cells are individually addressable (a
/// non-frozen list/set/map/UDT) — the column gets ONLY the
/// `_complex_deletion` trio (design.md D7), never the single-cell
/// `_timestamp`/`_ttl`/`_local_deletion_time`/`_tombstone` quad: a collection
/// has no ONE cell timestamp to report, and declaring those four columns for
/// a complex type would always render NULL — indistinguishable from "no
/// timestamp exists" (roborev finding, issue #4222). `Frozen(..)`
/// collections/UDTs are single-cell and excluded on purpose: Cassandra never
/// gives them a per-column complex-deletion marker, so they keep the plain
/// quad instead.
///
/// `None` — the caller must FAIL CLOSED, never default to "simple" — for
/// ANY `CqlType::Custom(_)`, not just a non-`udt:`-prefixed one (roborev
/// finding, issue #4222 — round 5, correcting the FIRST fix's own gap): a
/// bare (non-frozen) UDT name parses to `Custom("udt:<name>")` via
/// `cql_type_parser.rs`'s explicit UDT-shaped branch — matched below — but
/// an all-LOWERCASE, non-primitive type name ALSO reaches `Custom(_)`, via
/// that same parser's final fallback arm (`cql_type_parser.rs:274`), with
/// NO prefix at all. Cassandra identifiers default to lowercase unless
/// quoted, so a real UDT named e.g. `address` or `person` parses to exactly
/// `Custom("address")` — indistinguishable, at this call site with no UDT
/// registry to consult, from a genuinely unrecognized custom type.
/// Defaulting the non-`udt:`-prefixed case to "simple" (the FIRST fix's
/// remaining gap) therefore silently misclassified every real
/// lowercase-named UDT column as single-cell, assigning it the WRONG
/// metadata shape rather than refusing to guess (issue #28 no-heuristics).
fn try_is_complex_cql_type(t: &CqlType) -> Option<bool> {
    match t {
        CqlType::List(_) | CqlType::Set(_) | CqlType::Map(_, _) | CqlType::Udt(_, _) => Some(true),
        CqlType::Custom(_) => None,
        _ => Some(false),
    }
}

/// Build the raw view's full column contract (design.md D7) from the BASE
/// table's schema — a pinned public surface (spec's column-snapshot
/// requirement).
///
/// Order: partition-key columns, clustering-key columns (plain data columns:
/// every physical row, including a range-tombstone bound row, carries them),
/// then per-non-key-column metadata (the base value plus, for a simple
/// column, its `_timestamp`/`_ttl`/`_local_deletion_time`/`_tombstone` quad,
/// or for a collection/UDT column, its `_complex_deletion` trio only), then
/// the row-level, partition-level, row-kind/range-tombstone, and source
/// columns.
///
/// Fails closed (design.md D8) with `Error::Schema` when a synthesized
/// metadata-column name COLLIDES with a real base-table column name (e.g. a
/// base table with its own `generation`/`sstable`/`row_kind` column, or a
/// `val` column alongside a `val_timestamp` column) — silently overwriting
/// one of the two would be a no-heuristics-violating silent data loss
/// (roborev finding, issue #4222), never something this view may do quietly.
///
/// Returns the synthesized column set ALONGSIDE the set of names that are
/// METADATA DERIVATIVES (the per-cell `_timestamp`/`_ttl`/
/// `_local_deletion_time`/`_tombstone`/`_complex_deletion*` names and the
/// row-level `row_timestamp`/`row_ttl`/`row_local_deletion_time`/
/// `row_tombstone`/`row_deletion_timestamp` quintet) — roborev finding,
/// issue #4222 — round 9, correcting round 8's own gap: that fix classified
/// a predicate's column by NAME SUFFIX (`predicates.rs`'s
/// `is_metadata_derivative_column`), which misclassifies a REAL base-table
/// column that happens to be named e.g. `event_timestamp` with no sibling
/// `event` column to trip the collision guard (which only fires when a
/// SIBLING column's synthesized name collides, not when an UNRELATED base
/// column merely LOOKS like one). Returning the set THIS FUNCTION ACTUALLY
/// SYNTHESIZED — never re-derived from a name pattern — makes that
/// misclassification structurally impossible: a real base column can never
/// end up in this set, because it is populated only at the exact call
/// sites that push a synthesized quad/trio/quintet name.
pub(in crate::query::select_executor) fn raw_view_columns(
    base: &TableSchema,
) -> crate::Result<(Vec<ColumnInfo>, HashSet<String>)> {
    let key_names: HashSet<&str> = base
        .partition_keys
        .iter()
        .map(|k| k.name.as_str())
        .chain(base.clustering_keys.iter().map(|k| k.name.as_str()))
        .collect();

    let mut columns: Vec<ColumnInfo> = Vec::new();
    let mut metadata_names: HashSet<String> = HashSet::new();
    let mut seen: HashSet<String> = HashSet::new();
    let mut push = |columns: &mut Vec<ColumnInfo>,
                    metadata_names: &mut HashSet<String>,
                    name: String,
                    type_str: &str,
                    is_metadata: bool| {
        if !seen.insert(name.clone()) {
            return Err(Error::Schema(format!(
                "raw SSTable view: base table '{}.{}' has a column named '{name}' that \
                 collides with a synthesized metadata column name — the raw view's column \
                 contract (design.md D7) cannot be built without silently shadowing one of \
                 the two",
                base.keyspace, base.table
            )));
        }
        if is_metadata {
            metadata_names.insert(name.clone());
        }
        let position = columns.len();
        columns.push(column_info_from_type_str(name, type_str, position, None));
        Ok(())
    };

    for pk in &base.partition_keys {
        push(
            &mut columns,
            &mut metadata_names,
            pk.name.clone(),
            &pk.data_type,
            false,
        )?;
    }
    for ck in &base.clustering_keys {
        push(
            &mut columns,
            &mut metadata_names,
            ck.name.clone(),
            &ck.data_type,
            false,
        )?;
    }

    // `TableSchema::columns` carries EVERY declared column, key columns
    // included (issue #4222 research pass) — skip the ones already emitted
    // above so a key column is never duplicated as a "non-key" metadata quad.
    for col in base
        .columns
        .iter()
        .filter(|c| !key_names.contains(c.name.as_str()))
    {
        push(
            &mut columns,
            &mut metadata_names,
            col.name.clone(),
            &col.data_type,
            false,
        )?;

        // Fail closed (design.md D8) on an unparseable declared type rather
        // than defaulting to "simple" (roborev finding, issue #4222): a type
        // this parser cannot classify might be complex, and silently
        // declaring the wrong column shape is the same class of defect the
        // collision check above exists to prevent.
        // [`CqlType::parse`] — the parser the doc comments above cite
        // (`cql_type_parser.rs`) — and NEVER `row_build.rs`'s
        // `parse_cql_type_str` (roborev finding, issue #4222 — round 11):
        // the latter is `parser::complex_types::ComplexTypeParser`, which
        // is materially weaker. It has NO `varint` arm at all (so `varint`
        // fell through to `Custom("varint")` and was refused as an
        // "ambiguous UDT"), and its primitive alternation lists `time`
        // BEFORE `timeuuid`, so `"timeuuid"` matched `Time` with a
        // trailing `"uuid"` and was rejected outright as malformed. Both
        // outcomes are hard errors here, so the raw view was UNUSABLE for
        // any table declaring a `timeuuid`/`varint`/`vector<..>`/UDT
        // column — real committed fixtures do (`basic-types.cql`'s
        // `session_id TIMEUUID`, `issue-4114-vector-float.cql`'s
        // `vector<float, n>`) — and the message misdiagnosed a primitive
        // parser gap as UDT ambiguity. `CqlType::parse` resolves all of
        // those and reserves `Custom(_)` for the genuinely-ambiguous UDT
        // shape the fail-closed arm below is actually about.
        let cql_type = CqlType::parse(&col.data_type).map_err(|e| {
            Error::Schema(format!(
                "raw SSTable view: base table '{}.{}' column '{}' has a declared type \
                 ('{}') this parser cannot classify as simple or complex ({e}) — refusing \
                 to guess the column contract shape (issue #28 no-heuristics)",
                base.keyspace, base.table, col.name, col.data_type
            ))
        })?;
        let is_complex = try_is_complex_cql_type(&cql_type).ok_or_else(|| {
            Error::Schema(format!(
                "raw SSTable view: base table '{}.{}' column '{}' has declared type '{}', \
                 which parses to an unclassifiable custom type — this parser cannot tell \
                 whether it is a single-cell type or a multi-cell UDT/collection without a \
                 UDT registry (a bare lowercase UDT name parses to this exact shape) — \
                 refusing to guess the column contract shape (issue #28 no-heuristics)",
                base.keyspace, base.table, col.name, col.data_type
            ))
        })?;
        if is_complex {
            push(
                &mut columns,
                &mut metadata_names,
                format!("{}_complex_deletion", col.name),
                "boolean",
                true,
            )?;
            // `bigint`, never `int` (roborev finding, issue #4222 — round
            // 8, matching `partition_deletion_time`/`range_deletion_time`'s
            // round-7 fix for the same defect class): a far-future LDT is
            // carried as the wrapped `as u32 as i32` on-disk bit pattern
            // (`compaction_row.rs`'s module-header invariant on
            // `ComplexColumn::complex_deletion`'s `i32` element), and an
            // `int` column can only render its SIGN-EXTENDED (fabricated
            // negative) form.
            push(
                &mut columns,
                &mut metadata_names,
                format!("{}_complex_deletion_time", col.name),
                "bigint",
                true,
            )?;
            push(
                &mut columns,
                &mut metadata_names,
                format!("{}_complex_deletion_timestamp", col.name),
                "bigint",
                true,
            )?;
        } else {
            push(
                &mut columns,
                &mut metadata_names,
                format!("{}_timestamp", col.name),
                "bigint",
                true,
            )?;
            push(
                &mut columns,
                &mut metadata_names,
                format!("{}_ttl", col.name),
                "int",
                true,
            )?;
            // `bigint`, never `int` (roborev finding, issue #4222 — round
            // 8): see the identical `_complex_deletion_time` note above —
            // `SimpleCell::local_deletion_time`/`TombstoneInfo::local_deletion_time`
            // both feed this column and either can carry a value an `int`
            // cannot render honestly.
            push(
                &mut columns,
                &mut metadata_names,
                format!("{}_local_deletion_time", col.name),
                "bigint",
                true,
            )?;
            push(
                &mut columns,
                &mut metadata_names,
                format!("{}_tombstone", col.name),
                "text",
                true,
            )?;
        }
    }

    // The row-level metadata QUINTET (roborev finding, issue #4222 — round
    // 9): every one of these is a metadata derivative — populated ONLY on
    // a plain `row_kind = 'row'` row (`row_map.rs`'s `Live`/`Tombstone`
    // arms) — so `is_metadata = true` for each.
    push(
        &mut columns,
        &mut metadata_names,
        "row_timestamp".to_string(),
        "bigint",
        true,
    )?;
    push(
        &mut columns,
        &mut metadata_names,
        "row_ttl".to_string(),
        "int",
        true,
    )?;
    // `bigint`, never `int` (roborev finding, issue #4222 — round 8): see
    // the identical `<col>_local_deletion_time` note above.
    push(
        &mut columns,
        &mut metadata_names,
        "row_local_deletion_time".to_string(),
        "bigint",
        true,
    )?;
    push(
        &mut columns,
        &mut metadata_names,
        "row_tombstone".to_string(),
        "text",
        true,
    )?;
    // The row tombstone's own `markedForDeleteAt` — distinct from
    // `row_local_deletion_time` (the GC-clock seconds), mirroring the
    // partition/range pairs below (roborev finding, issue #4222: this was
    // previously discarded, making a row tombstone's writetime unrecoverable
    // from this view).
    push(
        &mut columns,
        &mut metadata_names,
        "row_deletion_timestamp".to_string(),
        "bigint",
        true,
    )?;

    // The remaining columns are all ALWAYS-APPLICABLE (source-identity/
    // row_kind/partition-and-range-deletion — `predicates.rs`'s
    // `always_applicable_column_names`), never a metadata DERIVATIVE of a
    // base column — `is_metadata = false` for each.
    push(
        &mut columns,
        &mut metadata_names,
        "partition_deletion_time".to_string(),
        "bigint",
        false,
    )?;
    push(
        &mut columns,
        &mut metadata_names,
        "partition_deletion_timestamp".to_string(),
        "bigint",
        false,
    )?;

    push(
        &mut columns,
        &mut metadata_names,
        "row_kind".to_string(),
        "text",
        false,
    )?;
    push(
        &mut columns,
        &mut metadata_names,
        "bound_inclusive".to_string(),
        "boolean",
        false,
    )?;
    push(
        &mut columns,
        &mut metadata_names,
        "range_deletion_time".to_string(),
        "bigint",
        false,
    )?;
    push(
        &mut columns,
        &mut metadata_names,
        "range_deletion_timestamp".to_string(),
        "bigint",
        false,
    )?;

    push(
        &mut columns,
        &mut metadata_names,
        "sstable".to_string(),
        "text",
        false,
    )?;
    // `bigint`, not `int` (roborev finding, issue #4222): `SSTableReader::generation`
    // is a `u64`; narrowing it to `i32` would SATURATE (and thus collapse
    // two distinct generations to the same fabricated value) for any real
    // corpus whose generation identifiers exceed `i32::MAX` — a
    // no-heuristics violation in a view whose whole purpose is authoritative
    // per-generation source identity.
    push(
        &mut columns,
        &mut metadata_names,
        "generation".to_string(),
        "bigint",
        false,
    )?;
    push(
        &mut columns,
        &mut metadata_names,
        "format".to_string(),
        "text",
        false,
    )?;
    // KNOWN, DELIBERATE scope limitation (roborev finding, issue #4222 —
    // round 8): `position`'s PROJECTED value diverges by internal access
    // path, and nothing distinguishes the two cases from the value alone.
    // `point.rs::resolve_position` resolves a real byte offset via a
    // second index lookup (best-effort — a lookup miss/error also yields
    // `Null`, per that function's own doc); `scan.rs`'s full-scan producer
    // NEVER consults an index per row and always reports `Value::Null`.
    // So `SELECT pk, position FROM t_raw_sstable_data WHERE pk = 1` (point
    // path) yields a real offset while the SAME projection with no WHERE
    // (full-scan path) yields `NULL` for every row — a caller cannot tell
    // "not measured on this access path" from "genuinely no offset" from
    // the value alone. A `position` PREDICATE is rejected outright for
    // exactly this reason (see the check in `execute_raw_sstable_view`);
    // the PROJECTED value is NOT rejected, because a `SELECT *` (this
    // view's primary, already-tested shape) must keep working over BOTH
    // access paths, and a bare `NULL` is at least an honest "not measured"
    // signal, never a fabricated one. Resolving `position` on the
    // full-scan path too (a per-row index lookup) would undermine that
    // producer's whole "bounded full scan" cost model; deferred rather
    // than fixed here.
    push(
        &mut columns,
        &mut metadata_names,
        "position".to_string(),
        "bigint",
        false,
    )?;

    Ok((columns, metadata_names))
}

#[cfg(test)]
#[path = "columns_tests.rs"]
mod tests;
