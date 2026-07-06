//! Delta-scan Arrow schema derivation (Epic #696, Issue #703).
//!
//! Derives the Arrow envelope schema from a [`TableSchema`] for CDC-style
//! Parquet projections of individual SSTable generations.
//!
//! ## Schema layout
//!
//! For a table `t (pk int, ck text, val text, st text STATIC, PRIMARY KEY (pk, ck))`:
//!
//! ```text
//! pk          : Int32               -- partition key, plain type
//! ck          : Utf8                -- clustering key, plain type (null on partition/static ops)
//! val         : Struct(nullable) {  -- regular column cell struct
//!                 value:      Utf8,
//!                 writetime:  Int64,
//!                 expires_at: Int64 (nullable),
//!               }
//! st          : Struct(nullable) {  -- static column cell struct (no `replaced`)
//!                 value:      Utf8,
//!                 writetime:  Int64,
//!                 expires_at: Int64 (nullable),
//!               }
//! __op        : Dictionary(Int8, Utf8)   -- op discriminator, dictionary-encoded
//! __ts        : Int64 (nullable)         -- deletion/liveness timestamp
//! __range_start : Struct(nullable) {    -- range-delete lower bound
//!                   ck:         Utf8,
//!                   inclusive:  Boolean,
//!                 }
//! __range_end   : Struct(nullable) {    -- range-delete upper bound
//!                   ck:         Utf8,
//!                   inclusive:  Boolean,
//!                 }
//! ```
//!
//! ## Feature gate
//!
//! This module is compiled only when **both** `delta-scan` and `arrow` features
//! are enabled.  It deliberately reuses [`cql_type_to_arrow_data_type`] from
//! `export::arrow_convert` (the #673 mapping) for the cell `value` field, so
//! there is no duplicated CQL → Arrow type logic.
//!
//! ## Fail-before-writing rules
//!
//! Both error conditions are raised at schema-derivation time, before any
//! output bytes are produced:
//!
//! 1. **Counter tables** — rejected with a descriptive error.
//! 2. **Column-name collisions** — a user column whose name matches an envelope
//!    reserved name (e.g. `__op`) causes a hard error.  The caller may provide
//!    a custom [`DeltaSchemaOpts::envelope_prefix`] (e.g. `"_cqlite_"`) to
//!    choose a different prefix for all reserved names; the error message names
//!    the option.

use arrow::datatypes::{DataType as ArrowDataType, Field, Fields, Schema};
use thiserror::Error;

use crate::export::arrow_convert::cql_type_to_arrow_data_type;
use crate::schema::{CqlType, TableSchema};

/// Name of the hardcoded boolean sub-field appended to the range-bound structs
/// (`{prefix}range_start` / `{prefix}range_end`).  Unlike the top-level
/// envelope columns, this sub-field name is NOT namespaced by
/// [`DeltaSchemaOpts::envelope_prefix`], so a clustering-key column with this
/// exact name collides regardless of the chosen prefix.
const RANGE_BOUND_INCLUSIVE_FIELD: &str = "inclusive";

// ============================================================================
// Error type
// ============================================================================

/// Errors produced by [`derive_delta_schema`] at schema-derivation time.
///
/// All errors are raised **before** any output bytes are written
/// (fail-before-writing guarantee, design §"Error handling").
#[derive(Debug, Error)]
pub enum DeltaSchemaError {
    /// A user column name collides with one of the envelope reserved names.
    ///
    /// The error message names the colliding column, the reserved name it
    /// conflicts with, and how to supply a different prefix via
    /// [`DeltaSchemaOpts::envelope_prefix`].
    #[error(
        "Column '{column}' collides with envelope reserved name '{reserved}'. \
         Use DeltaSchemaOpts::envelope_prefix to choose a different prefix \
         (e.g. envelope_prefix = \"_cqlite_\" gives \"_cqlite_op\", \"_cqlite_ts\", etc.)."
    )]
    ColumnCollision {
        /// The user column name that caused the collision.
        column: String,
        /// The reserved envelope name it collides with.
        reserved: String,
    },

    /// Counter tables cannot be projected to the delta envelope.
    ///
    /// Cassandra counter tables use a fundamentally different on-disk format
    /// (distributed counters) that cannot be represented as simple cell deltas.
    /// Reject at schema-derivation time rather than silently producing wrong output.
    #[error(
        "Counter tables cannot be projected to the delta envelope. \
         Table '{keyspace}.{table}' contains counter column(s): {columns}. \
         Counter semantics (distributed add/subtract) are incompatible with \
         the per-cell writetime delta model."
    )]
    CounterTable {
        /// Keyspace of the rejected table.
        keyspace: String,
        /// Table name of the rejected table.
        table: String,
        /// Comma-separated list of counter column names.
        columns: String,
    },

    /// CQL type parsing failed during schema derivation.
    #[error("CQL type parse error for column '{column}': {source}")]
    CqlTypeParse {
        /// The column whose type could not be parsed.
        column: String,
        /// The underlying error message.
        #[source]
        source: crate::error::Error,
    },
}

// ============================================================================
// Options
// ============================================================================

/// Options for [`derive_delta_schema`].
///
/// All fields have sensible defaults via [`Default`].
#[derive(Debug, Clone)]
pub struct DeltaSchemaOpts {
    /// Prefix used for the envelope's reserved column names.
    ///
    /// Defaults to `"__"`, yielding `__op`, `__ts`, `__range_start`,
    /// `__range_end`.  If a user column collides with one of these names,
    /// change this to a prefix that does not appear in the schema (e.g.
    /// `"_cqlite_"`).
    pub envelope_prefix: String,
}

impl Default for DeltaSchemaOpts {
    fn default() -> Self {
        Self {
            envelope_prefix: "__".to_string(),
        }
    }
}

impl DeltaSchemaOpts {
    /// Create options with a custom envelope prefix.
    pub fn with_prefix(prefix: impl Into<String>) -> Self {
        Self {
            envelope_prefix: prefix.into(),
        }
    }

    /// Return the name of the `__op` envelope column under the configured prefix.
    pub fn op_col(&self) -> String {
        format!("{}op", self.envelope_prefix)
    }

    /// Return the name of the `__ts` envelope column under the configured prefix.
    pub fn ts_col(&self) -> String {
        format!("{}ts", self.envelope_prefix)
    }

    /// Return the name of the `__range_start` envelope column under the configured prefix.
    pub fn range_start_col(&self) -> String {
        format!("{}range_start", self.envelope_prefix)
    }

    /// Return the name of the `__range_end` envelope column under the configured prefix.
    pub fn range_end_col(&self) -> String {
        format!("{}range_end", self.envelope_prefix)
    }

    /// Return all four reserved envelope names for collision checking.
    fn reserved_names(&self) -> [String; 4] {
        [
            self.op_col(),
            self.ts_col(),
            self.range_start_col(),
            self.range_end_col(),
        ]
    }
}

// ============================================================================
// Public API
// ============================================================================

/// Derive the Arrow envelope schema for a [`TableSchema`].
///
/// Produces the complete Arrow [`Schema`] for the delta-scan Parquet envelope,
/// including:
///
/// 1. **Key columns** — partition key and clustering key columns as plain Arrow
///    types (using the #673 mapping via [`cql_type_to_arrow_data_type`]).
/// 2. **Cell columns** — every non-key column becomes a nullable `Struct{
///    value: <Arrow type>, writetime: i64, expires_at: i64|null }`.  Collection
///    columns (`List`, `Set`, `Map`) additionally include `replaced: bool`.
/// 3. **`{prefix}op`** — dictionary-encoded `Utf8` (default `__op`).
/// 4. **`{prefix}ts`** — nullable `i64` (default `__ts`).
/// 5. **`{prefix}range_start`** / **`{prefix}range_end`** — nullable
///    `Struct{ <clustering columns...>, inclusive: bool }`.
///
/// # Errors
///
/// Returns [`DeltaSchemaError::CounterTable`] if any column has type `counter`.
///
/// Returns [`DeltaSchemaError::ColumnCollision`] if any user column name matches
/// one of the reserved envelope names (see [`DeltaSchemaOpts::envelope_prefix`]).
///
/// Returns [`DeltaSchemaError::CqlTypeParse`] if a column's `data_type` string
/// cannot be parsed into a [`CqlType`].
pub fn derive_delta_schema(
    table: &TableSchema,
    opts: &DeltaSchemaOpts,
) -> Result<Schema, DeltaSchemaError> {
    // ------------------------------------------------------------------
    // 1. Fail-before-writing: reject counter tables
    // ------------------------------------------------------------------
    let counter_cols: Vec<String> = table
        .columns
        .iter()
        .filter(|col| {
            // Parse the data type; on parse failure we'll catch it below.
            CqlType::parse(&col.data_type)
                .map(|t| is_counter_type(&t))
                .unwrap_or(false)
        })
        .map(|col| col.name.clone())
        .collect();

    if !counter_cols.is_empty() {
        return Err(DeltaSchemaError::CounterTable {
            keyspace: table.keyspace.clone(),
            table: table.table.clone(),
            columns: counter_cols.join(", "),
        });
    }

    // ------------------------------------------------------------------
    // 2. Fail-before-writing: check for column-name collisions
    //
    // The collision check must cover ALL user-visible column names, including
    // partition-key and clustering-key columns.  Key columns are emitted as
    // plain Arrow fields (steps 3a/3b) just like regular columns, so a key
    // column named e.g. `__op` would produce two Arrow fields with the same
    // name — silently malformed output rather than the intended error.
    // ------------------------------------------------------------------
    let reserved = opts.reserved_names();

    // Collect all column names: partition keys + clustering keys + regular columns.
    let all_column_names = table
        .partition_keys
        .iter()
        .map(|k| &k.name)
        .chain(table.clustering_keys.iter().map(|k| &k.name))
        .chain(table.columns.iter().map(|c| &c.name));

    for col_name in all_column_names {
        for res in &reserved {
            if col_name == res {
                return Err(DeltaSchemaError::ColumnCollision {
                    column: col_name.clone(),
                    reserved: res.clone(),
                });
            }
        }
    }

    // Additionally reject any CLUSTERING-key column named after the range-bound
    // struct's hardcoded `inclusive` sub-field.  Clustering columns are emitted
    // as sub-fields of `{prefix}range_start`/`{prefix}range_end` alongside a
    // hardcoded `inclusive: bool` (see `build_range_bound_field`), so a
    // clustering column literally named `inclusive` would produce two
    // `inclusive` fields in that struct — a malformed Arrow schema that
    // otherwise fails LATE at `StructArray::try_new` (after the output file is
    // created).  Fail here, at derivation time, before any bytes are written.
    //
    // The `inclusive` sub-field is hardcoded and NOT namespaced by
    // `envelope_prefix`, so this collision fires regardless of the prefix.
    // Only clustering keys enter the range-bound struct; partition-key and
    // regular columns named `inclusive` are harmless and remain allowed.
    for ck in &table.clustering_keys {
        if ck.name == RANGE_BOUND_INCLUSIVE_FIELD {
            return Err(DeltaSchemaError::ColumnCollision {
                column: ck.name.clone(),
                reserved: RANGE_BOUND_INCLUSIVE_FIELD.to_string(),
            });
        }
    }

    // ------------------------------------------------------------------
    // 3. Build Arrow fields
    // ------------------------------------------------------------------
    let mut fields: Vec<Field> = Vec::new();

    // 3a. Partition key columns — plain Arrow types, non-nullable.
    let ordered_pk = table.ordered_partition_keys();
    for key_col in &ordered_pk {
        let cql_type =
            CqlType::parse(&key_col.data_type).map_err(|e| DeltaSchemaError::CqlTypeParse {
                column: key_col.name.clone(),
                source: e,
            })?;
        let arrow_type = cql_type_to_arrow_data_type(&cql_type);
        fields.push(Field::new(&key_col.name, arrow_type, false));
    }

    // 3b. Clustering key columns — plain Arrow types, nullable (null for
    //     partition-scoped ops like partition_delete / static_upsert).
    let ordered_ck = table.ordered_clustering_keys();
    for ck_col in &ordered_ck {
        let cql_type =
            CqlType::parse(&ck_col.data_type).map_err(|e| DeltaSchemaError::CqlTypeParse {
                column: ck_col.name.clone(),
                source: e,
            })?;
        let arrow_type = cql_type_to_arrow_data_type(&cql_type);
        fields.push(Field::new(&ck_col.name, arrow_type, true));
    }

    // 3c. Non-key columns — cell structs.
    //
    // Key column names for quick membership check.
    let pk_names: std::collections::HashSet<&str> =
        ordered_pk.iter().map(|k| k.name.as_str()).collect();
    let ck_names: std::collections::HashSet<&str> =
        ordered_ck.iter().map(|k| k.name.as_str()).collect();

    for col in &table.columns {
        if pk_names.contains(col.name.as_str()) || ck_names.contains(col.name.as_str()) {
            // Already emitted as a plain key field above.
            continue;
        }

        let cql_type =
            CqlType::parse(&col.data_type).map_err(|e| DeltaSchemaError::CqlTypeParse {
                column: col.name.clone(),
                source: e,
            })?;

        let cell_field = build_cell_struct_field(&col.name, &cql_type);
        fields.push(cell_field);
    }

    // 3d. Envelope columns.
    fields.push(build_op_field(&opts.op_col()));
    fields.push(Field::new(opts.ts_col(), ArrowDataType::Int64, true));
    fields.push(build_range_bound_field(
        &opts.range_start_col(),
        &ordered_ck,
    )?);
    fields.push(build_range_bound_field(&opts.range_end_col(), &ordered_ck)?);

    Ok(Schema::new(fields))
}

// ============================================================================
// Internal helpers
// ============================================================================

/// Returns `true` if the CQL type is `Counter` (including through `Frozen`).
fn is_counter_type(cql_type: &CqlType) -> bool {
    match cql_type {
        CqlType::Counter => true,
        CqlType::Frozen(inner) => is_counter_type(inner),
        _ => false,
    }
}

/// Returns `true` if the CQL type is a non-frozen collection (`List`, `Set`, `Map`).
///
/// Frozen collections do NOT get the `replaced` field — they behave like scalars.
fn is_collection_type(cql_type: &CqlType) -> bool {
    match cql_type {
        CqlType::List(_) | CqlType::Set(_) | CqlType::Map(_, _) => true,
        // All other types, including Frozen<collection>, are treated as scalars.
        _ => false,
    }
}

/// Build the nullable cell `Struct` field for a non-key column.
///
/// ```text
/// Struct(nullable) {
///   value:      <Arrow type per #673>,
///   writetime:  Int64,
///   expires_at: Int64 (nullable),
///   replaced:   Boolean  -- collection columns ONLY
/// }
/// ```
///
/// Reuses [`cql_type_to_arrow_data_type`] (the #673 mapping) for `value`.
fn build_cell_struct_field(col_name: &str, cql_type: &CqlType) -> Field {
    let value_arrow_type = cql_type_to_arrow_data_type(cql_type);
    let is_collection = is_collection_type(cql_type);

    let mut struct_fields = vec![
        // value: nullable — `None` encodes a cell tombstone.
        Field::new("value", value_arrow_type, true),
        // writetime: always present (i64 µs since epoch).
        Field::new("writetime", ArrowDataType::Int64, false),
        // expires_at: nullable — `None` means no TTL.
        Field::new("expires_at", ArrowDataType::Int64, true),
    ];

    if is_collection {
        // replaced: present only for non-frozen collection columns (v1 design).
        struct_fields.push(Field::new("replaced", ArrowDataType::Boolean, false));
    }

    // The struct itself is nullable: null struct = column not present in this delta.
    Field::new(
        col_name,
        ArrowDataType::Struct(Fields::from(struct_fields)),
        true, // nullable struct
    )
}

/// Build the `__op` field: `Dictionary(Int8, Utf8)`.
///
/// Dictionary-encoded so that the five op strings (`upsert`, `static_upsert`,
/// `row_delete`, `range_delete`, `partition_delete`) are stored once in the
/// dictionary and referenced by small integer indices.
fn build_op_field(col_name: &str) -> Field {
    Field::new(
        col_name,
        ArrowDataType::Dictionary(Box::new(ArrowDataType::Int8), Box::new(ArrowDataType::Utf8)),
        false,
    )
}

/// Build a `__range_start` or `__range_end` field.
///
/// ```text
/// Struct(nullable) {
///   <ck_1>:    <Arrow type of first clustering column>,
///   <ck_2>:    <Arrow type of second clustering column>,
///   ...
///   inclusive: Boolean,
/// }
/// ```
///
/// The struct is nullable: null means "no range bound" (only non-null on
/// `range_delete` records).  Clustering-key columns within the struct are
/// individually nullable to support prefix bounds.
///
/// Tables with no clustering key produce an empty-struct with just `inclusive`
/// (degenerate but well-formed for the writer).
fn build_range_bound_field(
    col_name: &str,
    clustering_keys: &[&crate::schema::ClusteringColumn],
) -> Result<Field, DeltaSchemaError> {
    let mut struct_fields: Vec<Field> = Vec::with_capacity(clustering_keys.len() + 1);

    for ck_col in clustering_keys {
        let cql_type =
            CqlType::parse(&ck_col.data_type).map_err(|e| DeltaSchemaError::CqlTypeParse {
                column: ck_col.name.clone(),
                source: e,
            })?;
        let arrow_type = cql_type_to_arrow_data_type(&cql_type);
        // Nullable: trailing components absent in a prefix bound become null.
        struct_fields.push(Field::new(&ck_col.name, arrow_type, true));
    }

    struct_fields.push(Field::new(
        RANGE_BOUND_INCLUSIVE_FIELD,
        ArrowDataType::Boolean,
        false,
    ));

    Ok(Field::new(
        col_name,
        ArrowDataType::Struct(Fields::from(struct_fields)),
        true, // nullable — null except on range_delete records
    ))
}

// ============================================================================
// Tests
// ============================================================================

// Unit tests live in a sibling file (extracted per epic #1135 to keep this
// source file under the campsite-rule threshold).
#[cfg(test)]
#[path = "delta_schema_tests.rs"]
mod tests;
