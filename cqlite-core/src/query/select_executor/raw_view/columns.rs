//! Table-name suffix detection + the raw view's column contract (issue #4222,
//! design.md D1/D6/D7).

use super::super::{column_info_from_type_str, parse_cql_type_str};
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
        let cql_type = parse_cql_type_str(&col.data_type).ok_or_else(|| {
            Error::Schema(format!(
                "raw SSTable view: base table '{}.{}' column '{}' has a declared type \
                 ('{}') this parser cannot classify as simple or complex — refusing to guess \
                 the column contract shape (issue #28 no-heuristics)",
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
mod tests {
    use super::*;
    use crate::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn};

    fn dropped_regular_col_schema() -> TableSchema {
        TableSchema {
            keyspace: "test_tomb".to_string(),
            table: "dropped_regular_col".to_string(),
            partition_keys: vec![KeyColumn {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![ClusteringColumn {
                name: "ck".to_string(),
                data_type: "int".to_string(),
                position: 0,
                order: ClusteringOrder::Asc,
            }],
            columns: vec![
                Column {
                    name: "pk".to_string(),
                    data_type: "int".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "ck".to_string(),
                    data_type: "int".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "keep_col".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "drop_col".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: Default::default(),
            dropped_columns: Default::default(),
        }
    }

    #[test]
    fn strip_raw_view_suffix_recognizes_and_rejects() {
        assert_eq!(
            strip_raw_view_suffix("dropped_regular_col_raw_sstable_data"),
            Some("dropped_regular_col")
        );
        assert_eq!(strip_raw_view_suffix("dropped_regular_col"), None);
        // Bare suffix with no base name is refused, not treated as a
        // zero-length base table.
        assert_eq!(strip_raw_view_suffix("_raw_sstable_data"), None);
    }

    /// Pinned column-contract snapshot (issue #4222, spec's public-surface
    /// requirement) for `test_tomb.dropped_regular_col_raw_sstable_data`. A
    /// column rename/add/remove in [`raw_view_columns`] must show up here as a
    /// diff, not be discovered downstream.
    #[test]
    fn dropped_regular_col_column_contract_snapshot() {
        let schema = dropped_regular_col_schema();
        let (columns, _metadata_names) =
            raw_view_columns(&schema).expect("no collision in this fixture");
        let names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "pk",
                "ck",
                "keep_col",
                "keep_col_timestamp",
                "keep_col_ttl",
                "keep_col_local_deletion_time",
                "keep_col_tombstone",
                "drop_col",
                "drop_col_timestamp",
                "drop_col_ttl",
                "drop_col_local_deletion_time",
                "drop_col_tombstone",
                "row_timestamp",
                "row_ttl",
                "row_local_deletion_time",
                "row_tombstone",
                "row_deletion_timestamp",
                "partition_deletion_time",
                "partition_deletion_timestamp",
                "row_kind",
                "bound_inclusive",
                "range_deletion_time",
                "range_deletion_timestamp",
                "sstable",
                "generation",
                "format",
                "position",
            ],
            "raw-view column contract changed unexpectedly — update this snapshot \
             deliberately if the change is intended (issue #4222 D7)"
        );
        // Positions must be dense/ordered (metadata.columns is positional).
        for (idx, col) in columns.iter().enumerate() {
            assert_eq!(col.position, idx);
        }
    }

    #[test]
    fn key_columns_are_never_duplicated_as_metadata_quads() {
        let schema = dropped_regular_col_schema();
        let (columns, _metadata_names) =
            raw_view_columns(&schema).expect("no collision in this fixture");
        let pk_timestamp_present = columns.iter().any(|c| c.name == "pk_timestamp");
        assert!(
            !pk_timestamp_present,
            "a partition-key column must not get a per-cell metadata quad"
        );
    }

    /// Roborev finding (issue #4222, round 9): the returned `metadata_names`
    /// set must contain EXACTLY the synthesized per-cell quad + row-level
    /// quintet names — never a structural column (`pk`/`ck`/`keep_col`/
    /// `drop_col` themselves), and never an ALWAYS-applicable column
    /// (`generation`/`sstable`/`row_kind`/`partition_deletion_time`/etc,
    /// `predicates.rs`'s separate `always_applicable_column_names` set).
    #[test]
    fn metadata_names_contains_exactly_the_synthesized_derivatives() {
        let schema = dropped_regular_col_schema();
        let (_columns, metadata_names) =
            raw_view_columns(&schema).expect("no collision in this fixture");
        let expected: std::collections::HashSet<String> = [
            "keep_col_timestamp",
            "keep_col_ttl",
            "keep_col_local_deletion_time",
            "keep_col_tombstone",
            "drop_col_timestamp",
            "drop_col_ttl",
            "drop_col_local_deletion_time",
            "drop_col_tombstone",
            "row_timestamp",
            "row_ttl",
            "row_local_deletion_time",
            "row_tombstone",
            "row_deletion_timestamp",
        ]
        .into_iter()
        .map(String::from)
        .collect();
        assert_eq!(metadata_names, expected);
        // Structural + always-applicable columns must NEVER appear here.
        for never in [
            "pk",
            "ck",
            "keep_col",
            "drop_col",
            "generation",
            "sstable",
            "format",
            "position",
            "row_kind",
            "partition_deletion_time",
            "partition_deletion_timestamp",
            "bound_inclusive",
            "range_deletion_time",
            "range_deletion_timestamp",
        ] {
            assert!(
                !metadata_names.contains(never),
                "'{never}' must never be classified as a metadata derivative"
            );
        }
    }

    /// Roborev finding (issue #4222, round 9) — the EXACT failure scenario
    /// the finding cited: a REAL base column named `event_timestamp` with
    /// NO sibling `event` column (so the collision guard never fires) must
    /// NOT end up in `metadata_names` — it is a genuine structural data
    /// column, never a synthesized derivative, no matter what its name
    /// LOOKS like.
    #[test]
    fn a_real_column_that_merely_looks_like_a_metadata_derivative_is_never_classified_as_one() {
        let mut schema = dropped_regular_col_schema();
        schema.columns.push(Column {
            name: "event_timestamp".to_string(),
            data_type: "text".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        });
        let (columns, metadata_names) =
            raw_view_columns(&schema).expect("no collision — 'event_timestamp' has no sibling");
        assert!(
            columns.iter().any(|c| c.name == "event_timestamp"),
            "the real base column must still be present in the contract"
        );
        assert!(
            !metadata_names.contains("event_timestamp"),
            "REGRESSION: a real base column merely named like a metadata derivative must \
             NEVER be classified as one — it is a genuine structural data column"
        );
    }

    /// A base table whose real column name collides with a synthesized
    /// metadata-column name must fail closed (design.md D8), never silently
    /// clobber one of the two (roborev finding, issue #4222).
    #[test]
    fn colliding_base_column_name_fails_closed() {
        let mut schema = dropped_regular_col_schema();
        schema.columns.push(Column {
            name: "generation".to_string(),
            data_type: "int".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        });
        let err = raw_view_columns(&schema)
            .expect_err("a base column literally named 'generation' must be refused");
        assert!(
            matches!(err, Error::Schema(_)),
            "collision must surface as Error::Schema, got: {err:?}"
        );
    }

    /// A base column named `<other>_timestamp` alongside a plain `<other>`
    /// column collides with that OTHER column's synthesized metadata quad.
    #[test]
    fn colliding_quad_suffix_fails_closed() {
        let mut schema = dropped_regular_col_schema();
        schema.columns.push(Column {
            name: "keep_col_timestamp".to_string(),
            data_type: "bigint".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        });
        let err = raw_view_columns(&schema)
            .expect_err("a base column literally named 'keep_col_timestamp' must be refused");
        assert!(matches!(err, Error::Schema(_)));
    }

    /// Roborev finding (issue #4222, round 5, correcting round 4's own
    /// gap): a BARE lowercase UDT name — e.g. `address` — parses to
    /// `CqlType::Custom("address")` with NO `udt:` prefix
    /// (`cql_type_parser.rs`'s all-lowercase fallback arm), indistinguishable
    /// at this call site from a genuinely unrecognized custom type. This
    /// must fail closed (never silently default to "simple"), or a real
    /// UDT column gets the WRONG metadata shape (the single-cell quad
    /// instead of the `_complex_deletion` trio).
    #[test]
    fn bare_lowercase_udt_type_name_fails_closed_rather_than_simple() {
        let mut schema = dropped_regular_col_schema();
        schema.columns.push(Column {
            name: "addr".to_string(),
            data_type: "address".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        });
        let err = raw_view_columns(&schema).expect_err(
            "a bare lowercase UDT-shaped type name must be refused, never silently \
             classified as a simple type",
        );
        assert!(
            matches!(err, Error::Schema(_)),
            "must surface as Error::Schema, got: {err:?}"
        );
    }

    /// Sanity: the explicitly `udt:`-prefixed shape (a MIXED-case or
    /// dotted UDT name, `cql_type_parser.rs`'s earlier branch) also fails
    /// closed here — this call site has no UDT registry to consult either
    /// way, so both `Custom(_)` shapes are refused identically.
    #[test]
    fn udt_prefixed_custom_type_also_fails_closed() {
        assert_eq!(
            try_is_complex_cql_type(&CqlType::Custom("udt:Address".to_string())),
            None
        );
        assert_eq!(
            try_is_complex_cql_type(&CqlType::Custom("address".to_string())),
            None
        );
        assert_eq!(try_is_complex_cql_type(&CqlType::Text), Some(false));
        assert_eq!(
            try_is_complex_cql_type(&CqlType::List(Box::new(CqlType::Int))),
            Some(true)
        );
    }
}
