//! Header-driven schema preparation for compaction (split out of `merge/mod.rs`, #4658): the
//! bare-name UDT normalisation plan and the effective compaction schema.

use super::*;

/// Build the effective compaction schema (#850): `schema` augmented with any
/// static columns that the input SSTables' SerializationHeaders declare but the
/// current schema no longer does (e.g. a static column dropped from the table).
///
/// Cassandra reads static-row PRESENCE from each input SSTable's
/// SerializationHeader, not from the current table metadata. After the last
/// static column is dropped, an older SSTable that still carries static rows must
/// keep its static-row prelude through compaction — otherwise the prelude (and
/// any surviving static data) is dropped, diverging from Cassandra. Re-adding the
/// dropped static column to the schema handed to the merger and writer restores
/// that: the reader decodes the static cells and the writer emits the static
/// prelude, exactly as a header-driven compaction would.
///
/// Returns `schema` unchanged when the inputs declare no static column absent
/// from `schema` (the overwhelmingly common case), so output stays byte-identical
/// for every table whose schema still matches its on-disk data.
#[cfg(feature = "write-support")]
/// How the input SSTable headers constrain bare-name UDT normalization for a
/// compaction (#929). The compaction reader decides complex-vs-simple from the
/// single decode schema, so each column's encoding must be consistent across
/// inputs.
#[derive(Debug, Default, Clone)]
pub struct UdtNormalizationPlan {
    /// Columns SAFE to decode/write as complex, mapped to the EXACT
    /// `UserType(...)` marshal string taken from the input headers. The header
    /// marshal is used verbatim (rather than re-rendered from the registry) so
    /// the decode schema is byte-exact and handles nested-UDT field types the
    /// flush-time renderer intentionally skips (roborev #1019).
    pub eligible_marshals: std::collections::HashMap<String, String>,
    /// Columns with an incompatible encoding across inputs — declared BOTH as a
    /// `UserType(...)` (complex) and as a simple form, OR as two DIFFERENT
    /// `UserType(...)` marshals (a UDT definition mismatch). No single decode
    /// schema is correct, so the caller must fail rather than corrupt
    /// (roborev #1017 / #1019).
    pub conflicts: std::collections::HashSet<String>,
    /// Every column name that appears in ANY input header (complex or simple).
    /// A schema column ABSENT from this set is in no input, so there are no cells
    /// to misdecode and it may be safely registry-normalized for the output
    /// (schema evolution: a UDT column added after the inputs were written —
    /// roborev #1023). Trust this set ONLY when `headers_verified` is true.
    pub observed: std::collections::HashSet<String>,
    /// True only when EVERY input header was successfully read and parsed. When
    /// false, header state is unknown: `observed`/`eligible_marshals` are empty
    /// and the caller MUST NOT treat a column's absence from `observed` as proof
    /// it is absent from the inputs (it could be an unreadable simple-cell input
    /// that registry-normalization would misdecode — roborev #1025).
    pub headers_verified: bool,
}

/// Inspect the input SSTable headers to plan bare-name UDT normalization.
///
/// - An input written without registry support stores the column as a simple
///   `BytesType` cell; that VETOES normalization, or the reader would misdecode
///   that input (roborev #1013).
/// - An older input that simply LACKS the column (schema evolution) must NOT
///   veto: absence is not a simple-cell declaration (roborev #1015).
/// - A column with incompatible encodings across inputs (complex vs simple, or
///   two different `UserType` marshals) is a `conflict`: no single decode schema
///   is correct, so the caller must fail rather than silently drop/corrupt the
///   complex values (roborev #1017 / #1019).
///
/// Conservative: if any input's header is missing or unparseable, returns an
/// empty plan (normalize nothing), since the inputs' forms cannot be confirmed.
pub fn udt_columns_eligible_for_normalization(input_paths: &[PathBuf]) -> UdtNormalizationPlan {
    udt_columns_eligible_for_normalization_in(&InputStats::resolve(input_paths))
}

/// [`udt_columns_eligible_for_normalization`] over inputs already resolved to generations (G11).
pub(crate) fn udt_columns_eligible_for_normalization_in(
    inputs: &InputStats,
) -> UdtNormalizationPlan {
    use std::collections::{HashMap, HashSet};
    // A column header is "UDT-bearing" when its marshal mentions `UserType(`
    // ANYWHERE — whether a bare top-level `UserType(...)` (a multicell complex
    // column) OR a `FrozenType(UserType(...))` / a frozen collection-of-UDT
    // like `FrozenType(ListType(...UserType(...)...))` (single-cell frozen UDT
    // collection-of-UDT). All of these MUST carry their exact header marshal to
    // the compaction output, else re-rendering the bare CQL form collapses the
    // nested UDT to `BytesType` and CQLite can no longer decode the cell from its
    // OWN output header (roborev #1020 Finding 3). `is_complex_column` still
    // decides complex-vs-simple correctly off the copied marshal (a
    // `FrozenType(...)` stays single-cell).
    const USERTYPE_MARKER: &str = "org.apache.cassandra.db.marshal.usertype(";
    // column -> distinct UDT-bearing marshal strings observed (exact, orig case)
    let mut usertype_marshals: HashMap<String, HashSet<String>> = HashMap::new();
    // columns declared as a non-UDT-bearing (plain simple / non-UDT) form
    let mut vetoed: HashSet<String> = HashSet::new();
    for input in inputs.iter() {
        match input.statistics() {
            Ok(sstable_stats) => {
                for c in &sstable_stats.serialization_header_columns {
                    if c.column_type.to_lowercase().contains(USERTYPE_MARKER) {
                        usertype_marshals
                            .entry(c.name.clone())
                            .or_default()
                            .insert(c.column_type.clone());
                    } else {
                        // Declared, but carries no UserType -> a simple/non-UDT cell.
                        vetoed.insert(c.name.clone());
                    }
                }
            }
            Err(_) => return UdtNormalizationPlan::default(),
        }
    }

    let mut plan = UdtNormalizationPlan {
        headers_verified: true,
        ..UdtNormalizationPlan::default()
    };
    plan.observed.extend(vetoed.iter().cloned());
    plan.observed.extend(usertype_marshals.keys().cloned());
    for (column, marshals) in usertype_marshals {
        if vetoed.contains(&column) || marshals.len() != 1 {
            // Mixed complex/simple, or disagreeing UDT definitions -> conflict.
            plan.conflicts.insert(column);
        } else if let Some(marshal) = marshals.into_iter().next() {
            plan.eligible_marshals.insert(column, marshal);
        }
    }
    plan
}

/// Apply the #929 compaction normalization to `schema` (the EFFECTIVE decode
/// schema): copy each eligible column's exact `UserType(...)` marshal from the
/// input headers so the compaction reader treats it as complex
/// (`is_complex_column`) and round-trips the per-field UDT cells. Errors when an
/// input set has an incompatible mixed encoding for a column (see
/// [`UdtNormalizationPlan::conflicts`]) rather than silently corrupting data.
#[cfg(any(test, not(feature = "tombstones")))]
pub(crate) fn apply_udt_marshals_from_inputs(
    schema: &mut TableSchema,
    input_paths: &[PathBuf],
    registry: Option<&crate::schema::UdtRegistry>,
) -> Result<()> {
    apply_udt_marshals_from_inputs_in(schema, &InputStats::resolve(input_paths), registry)
}

/// [`apply_udt_marshals_from_inputs`] over inputs already resolved to generations (G11).
pub(crate) fn apply_udt_marshals_from_inputs_in(
    schema: &mut TableSchema,
    inputs: &InputStats,
    registry: Option<&crate::schema::UdtRegistry>,
) -> Result<()> {
    let plan = udt_columns_eligible_for_normalization_in(inputs);
    if !plan.conflicts.is_empty() {
        let mut cols: Vec<&String> = plan.conflicts.iter().collect();
        cols.sort();
        return Err(Error::InvalidInput(format!(
            "compaction inputs disagree on the encoding of UDT column(s) {cols:?}: some store \
             complex UserType cells and others a simple cell (or a different UDT definition). \
             Refusing to compact to avoid data loss; rewrite the divergent SSTable(s) first."
        )));
    }
    let keyspace = schema.keyspace.clone();
    for column in &mut schema.columns {
        if let Some(marshal) = plan.eligible_marshals.get(&column.name) {
            // Observed as complex in the inputs: use the exact header marshal.
            column.data_type = marshal.clone();
        } else if plan.headers_verified && !plan.observed.contains(&column.name) {
            // Absent from ALL input headers (schema evolution: a UDT column added
            // after the inputs were written). No input cells to misdecode, so a
            // registry render is safe and keeps the output header consistent with
            // future flushes (roborev #1023). Columns OBSERVED as simple are left
            // bare on purpose (normalizing them would misdecode those inputs).
            // Requires `headers_verified`: if a header was unreadable we cannot
            // prove the column is truly absent, so we must not normalize it
            // (roborev #1025).
            if let Some(reg) = registry {
                if let Some(marshal) =
                    crate::storage::sstable::writer::data_writer::resolve_bare_udt_marshal(
                        &column.data_type,
                        &keyspace,
                        reg,
                    )
                {
                    column.data_type = marshal;
                }
            }
        }
    }
    Ok(())
}

pub fn effective_compaction_schema(schema: &TableSchema, input_paths: &[PathBuf]) -> TableSchema {
    effective_compaction_schema_in(schema, &InputStats::resolve(input_paths))
}

/// [`effective_compaction_schema`] over inputs already resolved to generations (G11).
pub(crate) fn effective_compaction_schema_in(
    schema: &TableSchema,
    inputs: &InputStats,
) -> TableSchema {
    use std::collections::HashSet;

    // Names already known to the current schema (regular + static columns and
    // both key kinds) must not be re-added.
    let mut known: HashSet<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
    for k in &schema.partition_keys {
        known.insert(k.name.clone());
    }
    for k in &schema.clustering_keys {
        known.insert(k.name.clone());
    }

    // (name, cql_type) for static columns present in the inputs but missing from
    // the current schema. Sorted by name for deterministic output.
    let mut added: Vec<(String, String)> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();

    for input in inputs.iter() {
        match input.statistics() {
            Err(u) if u.is_absent() => continue,
            Ok(sstable_stats) => {
                for col in &sstable_stats.serialization_header_columns {
                    if col.is_static && !known.contains(&col.name) && seen.insert(col.name.clone())
                    {
                        added.push((col.name.clone(), col.column_type.clone()));
                    }
                }
            }
            Err(u) => {
                tracing::warn!(
                    "effective_compaction_schema: cannot read Statistics.db for {:?}: {}",
                    input.path(),
                    u
                );
            }
        }
    }

    if added.is_empty() {
        return schema.clone();
    }

    added.sort_by(|a, b| a.0.cmp(&b.0));
    tracing::info!(
        "effective_compaction_schema: re-adding {} static column(s) dropped from schema \
         {}.{} but still present in input SSTable headers: {:?}",
        added.len(),
        schema.keyspace,
        schema.table,
        added.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>()
    );

    let mut effective = schema.clone();
    for (name, data_type) in added {
        effective.columns.push(crate::schema::Column {
            name,
            data_type,
            nullable: true,
            default: None,
            is_static: true,
        });
    }
    effective
}
